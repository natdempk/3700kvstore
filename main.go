package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
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
var lastApplied = 0 // commit thing

var lastHeartbeatTime = time.Now()

var currentTermVotes = 0
var replicaIDs []string

var committedValues = make(map[string]string)

// leader state
var nextIndex = make(map[string]int)
var matchIndex = make(map[string]int)

func getElectionTimeout() (timeout time.Duration) {
	// change this later
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return time.Duration(300+r.Int31n(2000)) * time.Millisecond
}

func checkForReplication(logIndex int, message Message) {
	// TODO: weird stuff could happen, need a failure condition
	//startTime := time.Now()

	initialLeaderID := leaderID

	for lastApplied < logIndex {
		if state != LEADER || initialLeaderID != leaderID {
			fmt.Println("SOMETHING CHANGED")
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

		time.Sleep(2 * time.Millisecond)
	}

	okMessage := Message{
		ID:          message.ID,
		Source:      myID,
		Destination: message.Source,
		Leader:      leaderID,
		Type:        "ok",
		Value:       log[logIndex-1].Value,
	}

	sendMessage(okMessage)
	//fmt.Println(time.Since(startTime))
}

func main() {
	myID = os.Args[1]
	replicaIDs = os.Args[2:]

	var err error
	uconn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{myID, "unixpacket"})
	conn = *uconn

	if err != nil {
		fmt.Println("TERRIBLE ERROR")
		fmt.Println(err)
		panic(err)
	}

	go checkForElection()

	//d := json.NewDecoder(conn)

	for {
		var message Message

		var p []byte = make([]byte, 10000)
		_, _, err := conn.ReadFromUnix(p)
		if err != nil {
			fmt.Printf("conn error")
			fmt.Println(err)
			fmt.Println(string(p))
			panic(err)
		}

		for i, byt := range p {
			if byt == '\x00' {
				p = p[:i]
				break
			}
		}
		err = json.Unmarshal(p, &message)
		//debug.FreeOSMemory()
		if err != nil {
			fmt.Printf("horrible error")
			fmt.Println(err)
			fmt.Println(string(p))
			panic(err)
		}

		switch message.Type {
		case "get":
			//fmt.Println("get")
			if state == LEADER {
				// provide results
				found := false
				for i := max(lastApplied-1, 0); i >= 0; i-- {
					//for i := len(log) - 1; i >= 0; i-- {
					//fmt.Println(lastApplied-1, len(log))
					if log[i].Key == message.Key {
						okMessage := Message{
							ID:          message.ID,
							Source:      myID,
							Destination: message.Source,
							Leader:      leaderID,
							Type:        "ok",
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
						Type:        "fail",
					}
					sendMessage(failureMessage)
				}

			} else {
				sendRedirect(message.Source, message.ID)
			}
		case "put":
			//fmt.Println("put")
			if state == LEADER {
				//failed := false
				//for i, logItem := range log[lastApplied:] {
				//if logItem.Key == message.Key {
				//// fail, still waiting to  be applied
				//failed = true
				//break
				//}
				//}

				//if failed {
				//// send message and
				//sendMessage(failedMessage)
				//} else {
				//for i := lastApplied - 1; i >= 0; i-- {
				//if log[i].Key == message.Key {
				//okMessage := Message{
				//ID:          message.ID,
				//Source:      myID,
				//Destination: message.Source,
				//Leader:      leaderID,
				//Type:        "ok",
				//Value:       log[i].Value,
				//}
				//found = true
				//sendMessage(okMessage)
				//break
				//}
				//}
				//}

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

				//okMessage := Message{
				//ID:          message.ID,
				//Source:      myID,
				//Destination: message.Source,
				//Leader:      leaderID,
				//Type:        "ok",
				//}
				//sendMessage(okMessage)
			} else {
				//fmt.Println("REDIRECT")
				sendRedirect(message.Source, message.ID)
			}
		case "redirect":
			fmt.Println("THIS SHOULD NEVER HAPPEN")
			fmt.Println("SOMETHING WENT TERRIBLY WRONG")
		case "fail":
			break
			//fmt.Println("fail")
		case "ok":
			//fmt.Println("ok")
			break
			// more to come
		case LEADER_APPEND_ENTRIES:
			handleAppendEntries(message)
		case RESPONSE_APPEND_ENTRIES:
			if message.Success {
				if message.CommitIndex < nextIndex[message.Source] {
					//fmt.Println("SICK OUT OF ORDER MESSAGE, IGNORED")
				} else {
					nextIndex[message.Source] = message.CommitIndex + 1
					matchIndex[message.Source] = message.CommitIndex

					lastApplied = computeLastApplied()
				}
			} else { // we aren't the leader
				// decremented nextIndex and retry
				//fmt.Printf("ELSE STATEMENT mESSSAGE: %v\n", message)
				//nextIndex[message.Source] = message.CommitIndex + 1
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
				//fmt.Println("granted vote")
			}

			if state == CANDIDATE && currentTermVotes > len(replicaIDs)/2 {
				// we are elected leader
				fmt.Printf("LEADER ELECTED: %v\n", myID)
				state = LEADER
				leaderID = myID
				// reinitialize matchIndex and nextIndex
				for _, replica := range replicaIDs {
					go sendAppendEntriesRpc(replica, false)
					nextIndex[replica] = commitIndex + 1
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
	//const EMPTY_HEARTBEAT_TIMEOUT = 100 * time.Millisecond
	const HEARTBEAT_TIMEOUT = 100 * time.Millisecond
	const HEARTBEAT_SLEEP_TIME = 25 * time.Millisecond
	leaderLastHeartbeatTime := time.Unix(0, 0)

	for state == LEADER {
		if bypassTimeout || time.Since(leaderLastHeartbeatTime) > HEARTBEAT_TIMEOUT {

			//fmt.Println("Sending append")
			logOffset := min(nextIndex[destination], len(log)-1)
			if logOffset < 0 {
				logOffset = 0
			}

			lastLogTerm := 0
			if len(log) > 0 {
				//fmt.Println(len(log), nextIndex[destination])
				lastLogTerm = log[max(nextIndex[destination]-2, 0)].Term
			}
			var actualEntries = log[logOffset:min(len(log), logOffset+20)]
			//if len(actualEntries) == 0 {
			//if time.Since(leaderLastHeartbeatTime) < EMPTY_HEARTBEAT_TIMEOUT {
			//continue
			//}
			//}
			appendEntriesMessage := Message{
				ID:           uuid.NewV4().String(),
				Type:         LEADER_APPEND_ENTRIES,
				Destination:  destination,
				Source:       myID,
				Leader:       leaderID,
				Term:         currentTerm,
				PrevLogIndex: max(nextIndex[destination]-1, 0),
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

	// THIS MIGHT BE WRONG
	if (state == CANDIDATE || state == FOLLOWER) &&
		(message.Term > currentTerm || message.Leader != leaderID) {
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

	//fmt.Printf("max: %v, prevlogindex: %v\n", max(len(log), 0), message.PrevLogIndex)
	if message.Term < currentTerm {
		responseMessage.Success = false

	} else if message.PrevLogIndex > 0 &&
		len(log) >= message.PrevLogIndex &&
		log[max(0, message.PrevLogIndex-1)].Term != message.PrevLogTerm {
		// this is the problem
		responseMessage.Success = false
	} else if len(log)-1 > message.PrevLogIndex {
		log = log[:message.PrevLogIndex]
		responseMessage.Success = true
	} else {
		responseMessage.Success = true
	}

	if responseMessage.Success {
		log = append(log, message.Entries...)
		if message.LeaderCommit > commitIndex {
			commitIndex = min(message.LeaderCommit, len(log))
		}
	} else {
		//fmt.Println("Message not successful")
	}

	//fmt.Printf("SENDING US, logLen: %v, commitIndex: %v\n", len(log), commitIndex)

	responseMessage.CommitIndex = commitIndex

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
		fmt.Println(err)
		fmt.Printf("marshal error")
		panic(err)
	}

	conn.Write(bytes)
}

func computeLastApplied() int {
	var vals []int
	for _, ind := range nextIndex {
		vals = append(vals, ind)
	}
	sort.Ints(vals)

	return vals[max(0, (len(vals)/2)-1)] - 1
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
