package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/satori/go.uuid"
)

type Message struct {
	// core project message fields
	Source      string `json:"src"`
	Destination string `json:"dst"`
	Leader      string `json:"leader"`
	Type        string `json:"type"`
	ID          string `json:"MID"`
	Key         string `json:"key"`
	Value       string `json:"value"`
	// RequestVote RPC
	Term         int    `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
	VoteGranted  bool   `json:"voteGranted"`
	// AppendEntries RPC
	PrevLogIndex int       `json:"prevLogIndex"`
	PrevLogTerm  int       `json:"prevLogTerm"`
	Entries      []LogItem `json:"entries"`
	LeaderCommit int       `json:"leaderCommit"`
	Success      bool      `json:"success"`
	CommitIndex  int       `json:"currentCommit"`
}

type LogItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Term  int    `json:"term"`
}

var happened = true
var conn net.UnixConn

// constants
const LEADER = "leader"
const FOLLOWER = "follower"
const CANDIDATE = "candidate"
const LEADER_APPEND_ENTRIES = "leader_append_entries"
const RESPONSE_APPEND_ENTRIES = "response_append_entries"

// global state
var myID string
var leaderID string
var state = FOLLOWER
var currentTerm = 0
var votedFor = ""
var log []LogItem = []LogItem{}
var commitIndex = 0
var lastApplied = 0

var lastHeartbeatTime = time.Now()

var currentTermVotes = 0
var replicaIDs []string

// leader state
var nextIndexMutex sync.Mutex
var nextIndex = make(map[string]int)

func setNextIndex(s string, val int) {
	nextIndexMutex.Lock()
	defer nextIndexMutex.Unlock()
	nextIndex[s] = val
}

func getIndex(s string) int {
	nextIndexMutex.Lock()
	defer nextIndexMutex.Unlock()
	return nextIndex[s]
}

var matchIndex = make(map[string]int)

func getElectionTimeout() (timeout time.Duration) {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return time.Duration(300+r.Int31n(2000)) * time.Millisecond
}

func checkForReplication(logIndex int, message Message) {
	initialLeaderID := leaderID
	started := time.Now()
	for lastApplied < logIndex-1 {
		if state != LEADER || initialLeaderID != leaderID ||
			time.Since(started) > 75*time.Millisecond {

			failureMessage := Message{
				ID:          message.ID,
				Source:      myID,
				Destination: message.Source,
				Leader:      leaderID,
				Type:        "fail",
			}
			sendMessage(failureMessage)
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	okMessage := Message{
		ID:          message.ID,
		Source:      myID,
		Destination: message.Source,
		Leader:      leaderID,
		Type:        "ok",
		Key:         message.Key,
		Value:       log[logIndex-1].Value,
	}

	sendMessage(okMessage)
}

func main() {
	myID = os.Args[1]
	replicaIDs = os.Args[2:]

	var err error
	uconn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{myID, "unixpacket"})
	conn = *uconn

	if err != nil {
		panic(err)
	}

	go checkForElection()

	for {
		if !happened && len(log) > 30 {
			happened = true
		}
		var message Message

		var p []byte = make([]byte, 10000)
		_, _, err := conn.ReadFromUnix(p)
		if err != nil {
			panic(err)
		}

		for i, byt := range p {
			if byt == '\x00' {
				p = p[:i]
				break
			}
		}
		err = json.Unmarshal(p, &message)
		if err != nil {
			panic(err)
		}

		switch message.Type {
		case "get":
			if state == LEADER {
				// provide results
				found := false
				for i := max(lastApplied-1, 0); i >= 0; i-- {
					if log[i].Key == message.Key {
						okMessage := Message{
							ID:          message.ID,
							Source:      myID,
							Destination: message.Source,
							Leader:      leaderID,
							Type:        "ok",
							Key:         message.Key,
							Value:       log[i].Value,
						}
						found = true
						sendMessage(okMessage)
						break
					}
				}

				if !found {
					failureMessage := Message{
						ID:          message.ID,
						Source:      myID,
						Destination: message.Source,
						Leader:      leaderID,
						Type:        "ok",
					}
					sendMessage(failureMessage)
				}

			} else {
				sendRedirect(message.Source, message.ID)
			}
		case "put":
			if state == LEADER {
				// store results
				log = append(log, LogItem{
					Key:   message.Key,
					Value: message.Value,
					Term:  currentTerm,
				})
				commitIndex++
				for _, replica := range replicaIDs {
					go sendAppendEntriesRpc(replica, true)
				}
				go checkForReplication(commitIndex, message)

			} else {
				sendRedirect(message.Source, message.ID)
			}
		case "redirect":
			fmt.Println("THIS SHOULD NEVER HAPPEN")
			fmt.Println("SOMETHING WENT TERRIBLY WRONG")
		case "fail":
			break
		case "ok":
			break
		case LEADER_APPEND_ENTRIES:
			handleAppendEntries(message)
		case RESPONSE_APPEND_ENTRIES:
			if message.Success {
				if message.CommitIndex < getIndex(message.Source) {
				} else {
					setNextIndex(message.Source, message.CommitIndex+1)
					matchIndex[message.Source] = message.CommitIndex

					lastApplied = computeLastApplied()
				}
			}
		case "requestVote":
			if message.Term < currentTerm {
				// send false
				sendVote(message.Source, false)
			} else if (votedFor == "" || votedFor == message.CandidateID) && message.LastLogIndex >= lastApplied {
				// send vote
				votedFor = message.CandidateID
				sendVote(message.Source, true)
			} else {
				// vote false, we might be able to not vote for efficiency
				sendVote(message.Source, false)
			}
		case "vote":
			if message.VoteGranted {
				currentTermVotes++
			}

			if state == CANDIDATE && currentTermVotes > len(replicaIDs)/2 {
				// we are elected leader
				state = LEADER
				leaderID = myID
				// reinitialize matchIndex and nextIndex
				for _, replica := range replicaIDs {
					go sendAppendEntriesRpc(replica, false)
					setNextIndex(replica, commitIndex+1)
					matchIndex[replica] = 0
				}
				lastApplied = computeLastApplied()
			}
		}
	}
}

func checkForElection() {
	const ELECTION_SLEEP_TIME = 50 * time.Millisecond

	for {
		if state == FOLLOWER && time.Since(lastHeartbeatTime) > getElectionTimeout() {
			currentTerm++
			currentTermVotes++
			state = CANDIDATE
			votedFor = myID

			lastLogTerm := 0
			if len(log) > 0 {
				lastLogTerm = log[len(log)-1].Term
			}

			electMe := Message{
				ID:           uuid.NewV4().String(),
				Destination:  "FFFF",
				Source:       myID,
				Leader:       "FFFF",
				Type:         "requestVote",
				Term:         currentTerm,
				CandidateID:  myID,
				LastLogIndex: commitIndex,
				LastLogTerm:  lastLogTerm,
			}

			sendMessage(electMe)
		}

		time.Sleep(ELECTION_SLEEP_TIME)
	}
}

func sendAppendEntriesRpc(destination string, bypassTimeout bool) {
	const HEARTBEAT_TIMEOUT = 100 * time.Millisecond
	const HEARTBEAT_SLEEP_TIME = 25 * time.Millisecond
	leaderLastHeartbeatTime := time.Unix(0, 0)

	for state == LEADER {
		if bypassTimeout || time.Since(leaderLastHeartbeatTime) > HEARTBEAT_TIMEOUT {

			logOffset := min(getIndex(destination)-1, len(log)-1)
			if logOffset < 0 {
				logOffset = 0
			}

			lastLogTerm := 0
			if len(log) > 0 {
				lastLogTerm = log[max(getIndex(destination)-2, 0)].Term
			}
			var actualEntries = log[logOffset:min(len(log), logOffset+20)]

			appendEntriesMessage := Message{
				ID:           uuid.NewV4().String(),
				Type:         LEADER_APPEND_ENTRIES,
				Destination:  destination,
				Source:       myID,
				Leader:       leaderID,
				Term:         currentTerm,
				PrevLogIndex: max(getIndex(destination)-1, 0),
				PrevLogTerm:  lastLogTerm,
				Entries:      actualEntries,
				LeaderCommit: commitIndex,
			}

			sendMessage(appendEntriesMessage)
			leaderLastHeartbeatTime = time.Now()
		}

		if bypassTimeout {
			break
		}

		time.Sleep(HEARTBEAT_SLEEP_TIME)
	}
}

func handleAppendEntries(message Message) {
	if message.Term > currentTerm || message.Leader != leaderID {
		currentTerm = message.Term
		leaderID = message.Leader
		state = FOLLOWER
		votedFor = ""
	}

	lastHeartbeatTime = time.Now()

	responseMessage := Message{
		Source:      myID,
		Destination: leaderID,
		Leader:      leaderID,
		ID:          uuid.NewV4().String(),
		Type:        RESPONSE_APPEND_ENTRIES,
		Term:        currentTerm,
	}
	if message.Term < currentTerm {
		responseMessage.Success = false
	} else if message.PrevLogIndex > 0 &&
		len(log) >= message.PrevLogIndex &&
		log[max(0, message.PrevLogIndex-1)].Term != message.PrevLogTerm {
		// this is the problem
		responseMessage.Success = false
	} else {
		responseMessage.Success = true
		log = log[:max(0, message.PrevLogIndex)]
	}

	if responseMessage.Success {
		log = append(log, message.Entries...)
		if message.LeaderCommit > commitIndex {
			commitIndex = min(message.LeaderCommit, len(log))
		}
	}

	responseMessage.CommitIndex = commitIndex
	lastApplied = commitIndex

	sendMessage(responseMessage)
}

func sendRedirect(destination, messageID string) {
	redirectMessage := Message{
		ID:          messageID,
		Destination: destination,
		Source:      myID,
		Leader:      leaderID,
		Type:        "redirect",
	}

	sendMessage(redirectMessage)
}

func sendVote(destination string, vote bool) {
	voteMessage := Message{
		ID:          uuid.NewV4().String(),
		Destination: destination,
		Source:      myID,
		Leader:      "FFFF",
		Type:        "vote",
		Term:        currentTerm,
		VoteGranted: vote,
	}

	sendMessage(voteMessage)
}

func sendMessage(message Message) {
	bytes, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}

	conn.Write(bytes)
}

func computeLastApplied() int {
	var vals []int
	nextIndexMutex.Lock()
	defer nextIndexMutex.Unlock()
	for _, ind := range nextIndex {
		vals = append(vals, ind)
	}
	sort.Ints(vals)

	return vals[max(0, len(vals)/2)] - 1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
