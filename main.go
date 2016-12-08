package main

import (
	"encoding/json"
	"fmt"
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
}

type LogItem struct {
	Key   string
	Value string
	Term  int
}

var conn net.Conn

// constants
const LEADER = "leader"
const FOLLOWER = "follower"
const CANDIDATE = "candidate"
const LEADER_APPEND_ENTRIES = "leader_append_entries"
const RESPONSE_APPEND_ENTRIES = "response_append_entries"
const ENTRIES_BUFFER_SIZE = 100

// global state
var myID string
var leaderID string
var state = FOLLOWER
var currentTerm = 0
var votedFor = ""
var log []LogItem
var commitIndex = 0
var lastApplied = 0
var lastLogTerm = 0

var lastHeartbeatTime = time.Now()

var ELECTION_TIMEOUT = getElectionTimeout()
var currentTermVotes = 0
var replicaIDs []string

var entriesToAppend = make(chan LogItem, ENTRIES_BUFFER_SIZE)

var committedValues = make([string]string)

// change to maps
// leader state
var nextindex []int
var matchIndex []int

func getElectionTimeout() (timeout time.Duration) {
	// change this later
	return 250 * time.Millisecond
}

func main() {
	myID = os.Args[1]
	replicaIDs = os.Args[2:]

	var err error = nil
	conn, err = net.Dial("unixpacket", myID)

	if err != nil {
		fmt.Println("TERRIBLE ERROR")
		fmt.Println(err)
		panic(err)
	}

	go checkForElection()

	d := json.NewDecoder(conn)

	for {
		var message Message
		err := d.Decode(&message)
		if err != nil {
			fmt.Printf("horrible error")
			panic(err)
		}

		fmt.Println(message)

		switch message.Type {
		case "get":
			fmt.Println("get")
			if state == LEADER {
				// provide results
			} else {
				sendRedirect(message.Source)
			}
		case "put":
			fmt.Println("put")
			if state == LEADER {
				// store results
			} else {
				sendRedirect(message.Source)
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
			// TOOD: handle failures?
			fmt.Println("ok")
		case "requestVote":
			if message.Term < currentTerm {
				// send false
				sendVote(message.Source, false)
			} else if (votedFor == "" || votedFor == message.CandidateID) && message.LastLogIndex >= lastApplied {
				// send vote
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

			if currentTermVotes > len(replicaIDs)/2 {
				// we are elected leader
				fmt.Printf("LEADER ELECTED: %v\n", myID)
				state = LEADER
			}

		}
	}
}

func checkForElection() {
	const ELECTION_SLEEP_TIME = 50 * time.Millisecond

	for {
		if time.Since(lastHeartbeatTime) > ELECTION_TIMEOUT {
			currentTerm++
			currentTermVotes++
			state = CANDIDATE

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

func sendAppendEntriesRpc() {
	const HEARTBEAT_TIMEOUT = 50 * time.Millisecond
	const HEARTBEAT_SLEEP_TIME = 25 * time.Millisecond
	for {
		if len(entriesToAppend) == ENTRIES_BUFFER_SIZE || time.Since(lastHeartbeatTime) > HEARTBEAT_TIMEOUT {

			var actualEntries []LogItem

			entriesToConsume := len(entriesToAppend)

			for i := 0; i < entriesToConsume; i++ {
				actualEntries = append(actualEntries, <-entriesToAppend)
			}

			appendEntriesMessage := Message{
				ID:           uuid.NewV4().String(),
				Type:         LEADER_APPEND_ENTRIES,
				Destination:  "FFFF",
				Source:       myID,
				Leader:       leaderID,
				Term:         currentTerm,
				PrevLogIndex: lastApplied,
				PrevLogTerm:  lastLogTerm,
				Entries:      actualEntries,
				LeaderCommit: commitIndex,
			}

			sendMessage(appendEntriesMessage)
		}

		time.Sleep(HEARTBEAT_SLEEP_TIME)
	}
}

func handleAppendEntries(message Message) {
	responseMessage := Message{
		Source:      myID,
		Destination: leaderID,
		ID:          uuid.NewV4().String(),
		Type:        RESPONSE_APPEND_ENTRIES,
		Term:        currentTerm,
	}

	if message.Term < currentTerm {
		responseMessage.Success = false
	} else if len(log)-1 != message.PrevLogIndex &&
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
		log = append(log, message.Entries...)
		if message.LeaderCommit > commitIndex {
			if message.LeaderCommit < len(log)-1 {
				commitIndex = message.LeaderCommit
			} else {
				commitIndex = len(log) - 1
			}
		}
	}

	sendMessage(responseMessage)
}

func sendRedirect(destination string) {
	redirectMessage := Message{
		ID:          uuid.NewV4().String(),
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

	fmt.Fprintf(conn, string(bytes))
}
