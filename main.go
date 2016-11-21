package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
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
}

type LogItem struct {
	Foo string
}

var conn net.Conn

// global state
var myID string
var state = "follower"
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

	conn, err := net.Dial("unixpacket", myID)

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
		case "put":
			fmt.Println("put")
		case "redirect":
			fmt.Println("redirect")
		case "fail":
			fmt.Println("fail")
		case "ok":
			fmt.Println("ok")
			// more to come
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
			state = "candidate"

			electMe := Message{
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

func sendVote(destination string, vote bool) {
	voteMessage := Message{
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
	bytes, _ := json.Marshal(message)
	fmt.Fprintf(conn, string(bytes))
}
