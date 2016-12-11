package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
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
var lastApplied = 0
var lastLogTerm = 0

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

func main() {
	myID = os.Args[1]
	replicaIDs = os.Args[2:]

	var err error
	//uconn, err := net.DialUnix("unixpacket", &net.UnixAddr{myID, "unixpacket"}, &net.UnixAddr{myID, "unixpacket"})
	uconn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{myID, "unixpacket"})
	conn = *uconn
	//ulistener, err2 := net.ListenUnix("unixpacket", &net.UnixAddr{myID, "unixpacket"})
	//conn, err := ulistener.AcceptUnix()
	//fmt.Println(err2)

	if err != nil {
		fmt.Println("TERRIBLE ERROR")
		fmt.Println(err)
		panic(err)
	}

	go checkForElection()

	//d := json.NewDecoder(conn)

	for {
		var message Message

		// err := d.Decode(&message)

		var p []byte = make([]byte, 10000)
		//con, err := ulistener.AcceptUnix()
		//con.Read(p)
		//fmt.Println("NOT SO SICK")
		_, _, err := conn.ReadFromUnix(p)
		if err != nil {
			fmt.Println(string(p))
			fmt.Printf("conn error")
			fmt.Println(err)
			//var p []byte = make([]byte, 1000)

			//bufio.NewReader(conn).Read(p)
			fmt.Println(string(p))
			panic(err)
		}

		//fmt.Println("SICK")
		//fmt.Println([]byte(string(p)))
		for i, byt := range p {
			if byt == '\x00' {
				p = p[:i]
				break
			}
		}
		err = json.Unmarshal(p, &message)
		//fmt.Println(string(p))
		if err != nil {
			fmt.Println(string(p))
			fmt.Printf("horrible error")
			fmt.Println(err)
			//var p []byte = make([]byte, 1000)

			//bufio.NewReader(conn).Read(p)
			fmt.Println(string(p))
			panic(err)
		}

		switch message.Type {
		case "get":
			fmt.Println("get")
			if state == LEADER {
				// provide results
				found := false
				for i := len(log) - 1; i >= 0; i-- {
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
						Type:        "ok",
					}
					sendMessage(failureMessage)
				}

			} else {
				sendRedirect(message.Source, message.ID)
			}
		case "put":
			fmt.Println("put")
			if state == LEADER {
				// store results
				log = append(log, LogItem{
					Key:   message.Key,
					Value: message.Value,
					Term:  currentTerm,
				})
				okMessage := Message{
					ID:          message.ID,
					Source:      myID,
					Destination: message.Source,
					Leader:      leaderID,
					Type:        "ok",
				}
				commitIndex++
				sendMessage(okMessage)
			} else {
				fmt.Println("REDIRECT")
				sendRedirect(message.Source, message.ID)
			}
		case "redirect":
			fmt.Println("THIS SHOULD NEVER HAPPEN")
			fmt.Println("SOMETHING WENT TERRIBLY WRONG")
		case "fail":
			fmt.Println("fail")
		case "ok":
			fmt.Println("ok")
			// more to come
		case LEADER_APPEND_ENTRIES:
			handleAppendEntries(message)
		case RESPONSE_APPEND_ENTRIES:
			if message.Success {
				nextIndex[message.Source] = message.CommitIndex + 1
				matchIndex[message.Source] = message.CommitIndex
			} else { // we aren't the leader
				// decremented nextIndex and retry
				nextIndex[message.Source] = message.CommitIndex + 1
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
			fmt.Println("received vote")
			if message.VoteGranted {
				currentTermVotes++
				fmt.Println("granted vote")
			}

			if state == CANDIDATE && currentTermVotes > len(replicaIDs)/2 {
				// we are elected leader
				fmt.Printf("LEADER ELECTED: %v\n", myID)
				state = LEADER
				leaderID = myID
				// reinitialize matchIndex and nextIndex
				for _, replica := range replicaIDs {
					go sendAppendEntriesRpc(replica)
					nextIndex[replica] = commitIndex + 1
					matchIndex[replica] = 0
				}
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

func sendAppendEntriesRpc(destination string) {
	const HEARTBEAT_TIMEOUT = 200 * time.Millisecond
	const HEARTBEAT_SLEEP_TIME = 25 * time.Millisecond
	leaderLastHeartbeatTime := time.Unix(0, 0)

	for state == LEADER {
		if time.Since(leaderLastHeartbeatTime) > HEARTBEAT_TIMEOUT {
			logOffset := min(nextIndex[destination], len(log)-1)
			if logOffset < 0 {
				logOffset = 0
			}
			var actualEntries = log[logOffset:]

			appendEntriesMessage := Message{
				ID:           uuid.NewV4().String(),
				Type:         LEADER_APPEND_ENTRIES,
				Destination:  destination,
				Source:       myID,
				Leader:       leaderID,
				Term:         currentTerm,
				PrevLogIndex: lastApplied,
				PrevLogTerm:  lastLogTerm,
				Entries:      actualEntries,
				LeaderCommit: commitIndex,
			}

			sendMessage(appendEntriesMessage)
			leaderLastHeartbeatTime = time.Now()
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
	} else if max(len(log)-1, 0) != message.PrevLogIndex &&
		log[message.PrevLogIndex].Term != message.PrevLogTerm {
		responseMessage.Success = false
	} else if len(log)-1 > message.PrevLogIndex {
		for i := message.PrevLogIndex; i < len(log); i++ {
			if log[i].Term != message.Entries[i-message.PrevLogIndex].Term {
				log = log[:i]
				message.Entries = message.Entries[i-message.PrevLogIndex:]
				break
			}
		}
		responseMessage.Success = true
	} else {
		responseMessage.Success = true
	}

	if responseMessage.Success {
		fmt.Println("LOG11111111111111111111")
		fmt.Println(log)
		fmt.Println("ENTRIESSSSSSSSS1111111")
		fmt.Println(message.Entries)
		log = append(log, message.Entries...)
		fmt.Println("LOG222222222222222")
		fmt.Println(log)
		fmt.Println("LOGLENLOGLENLOGE")
		fmt.Println(len(log))
		fmt.Println("stop")
		if message.LeaderCommit > commitIndex {
			commitIndex = min(message.LeaderCommit, len(log)-1)
		}
	}

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

	//fmt.Fprintf(conn, string(bytes))
	//if len(log) != 0 {
	//fmt.Println(string(bytes))
	//}
	conn.Write(bytes)
	//conn.WriteToUnix(bytes, &net.UnixAddr{myID, "unixpacket"})
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
