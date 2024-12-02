package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"dissys/project/labrpc"
	"encoding/gob"

	//"fmt"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          int
	committedIndex    int
	lastApplied       int
	isleader          bool
	nextIndex         []int
	matchIndex        []int
	lastLogTerm       int
	lastLogIndex      int
	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	lastHeartbeat     time.Time
	log               []LogEntry
	applyCh           chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.isleader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.lastLogTerm)
	e.Encode(rf.committedIndex)
	e.Encode(rf.lastApplied)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	if err := d.Decode(&rf.currentTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.log); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.lastLogIndex); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.lastLogTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.committedIndex); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.lastApplied); err != nil {
		panic(err)
	}
	rf.isleader = false
	rf.heartbeatInterval = 50 * time.Millisecond
	rf.electionTimeout = time.Duration((300 + rand.Intn(300))) * time.Millisecond
	rf.lastHeartbeat = time.Now()
	DPrintf("Term %d, Server %d, commited index %d, last applied %d", rf.currentTerm, rf.me, rf.committedIndex, rf.lastApplied)
}

type AppendEntryArgs struct {
	Term           int
	LeaderId       int
	PrevLogIndex   int
	PrevLogTerm    int
	PrevLogCommand interface{}
	Entries        []LogEntry
	LeaderCommit   int
}

type AppendEntryReply struct {
	Term     int
	Accepted bool
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	UpdatedFlag bool

	VoteGranted bool
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Term %d, Server %d, get request from server %d! \n", args.Term, rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isleader = false
		go rf.persist()
	}
	reply.Term = rf.currentTerm

	var voteFlag bool
	var updateFlag bool
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		voteFlag = true
	}

	if (args.LastLogTerm > rf.lastLogTerm) ||
		(args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex >= rf.lastLogIndex) {
		updateFlag = true
	}

	if voteFlag && updateFlag {
		rf.votedFor = args.CandidateId
	}
	reply.UpdatedFlag = updateFlag

	reply.VoteGranted = voteFlag && updateFlag

	if reply.VoteGranted {
		rf.lastHeartbeat = time.Now()
		DPrintf("Term %d, Server %d agree to vote for server %d! \n", rf.currentTerm, rf.me, args.CandidateId)
	} else {
		DPrintf("Term %d, Server %d disagree to vote for server %d! \n", rf.currentTerm, rf.me, args.CandidateId)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Printf("Server %d, send request to server %d! \n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	if rf.isleader {
		ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
		return ok
	}
	return false
}

/*func (rf *Raft) sendHeartBeat(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	return ok
}*/

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.isleader

	if isLeader {
		if len(rf.log) == 0 {
			index = 1
		} else {
			index = rf.log[len(rf.log)-1].Index + 1
		}

		logEntry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		}
		term = rf.currentTerm
		rf.log = append(rf.log, logEntry)
		index = logEntry.Index
		rf.lastLogTerm = rf.currentTerm
		rf.lastLogIndex = index
		go rf.persist()
		DPrintf("Leader %d starts command %v at index %d, term %d", rf.me, command, index, term)
		go rf.replicateLogs()
	}

	return index, term, isLeader
}

func (rf *Raft) replicateLogs() {
	if rf.isleader {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.isleader {
				//DPrintf("Leader %d sends log to server %d", rf.me, i)
				go rf.sendLogsToFollower(i)
			}
		}
	}
}

func (rf *Raft) sendLogsToFollower(server int) {
	rf.mu.Lock()

	//DPrintf("Leader %d sends log to server %d , nextindex is %d", rf.me, server, rf.nextIndex[server])

	prevLogIndex := rf.nextIndex[server] - 2
	prevLogTerm := 0
	var prevLogCommand interface{}
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
		prevLogCommand = rf.log[prevLogIndex].Command
	}
	var entries []LogEntry

	if rf.nextIndex[server]-1 < len(rf.log) {
		entries = rf.log[rf.nextIndex[server]-1:]
	}

	args := AppendEntryArgs{
		Term:           rf.currentTerm,
		LeaderId:       rf.me,
		PrevLogIndex:   prevLogIndex,
		PrevLogTerm:    prevLogTerm,
		PrevLogCommand: prevLogCommand,
		Entries:        entries,
		LeaderCommit:   rf.committedIndex,
	}
	rf.mu.Unlock()

	//DPrintf("Leader %d sends log to server %d", rf.me, server)

	//fmt.Print(args)
	//fmt.Println()

	var reply AppendEntryReply

	go func() {
		ok := rf.sendAppendEntry(server, args, &reply)

		if ok {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.isleader = false
				rf.votedFor = -1
				rf.lastHeartbeat = time.Now()
				DPrintf("Leader %d lose leadership at Term %d", rf.me, rf.currentTerm)
				go rf.persist()
				return
			}
			rf.mu.Lock()
			if !rf.isleader {
				rf.mu.Unlock()
				return
			}
			if reply.Accepted {
				if rf.matchIndex[server]+len(args.Entries) <= len(rf.log) && len(args.Entries) != 0 {
					DPrintf("Term %d, Before Leader %d sends log to server %d , match index %d and next index %d", rf.currentTerm, rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
					rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
					rf.matchIndex[server] = rf.matchIndex[server] + len(args.Entries)
					DPrintf("Leader %d sends log to server %d , match index %d and next index %d is accepted", rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
					go rf.updateCommitIndex()
				}

			} else {
				DPrintf("Term %d, Before Leader %d sends log to server %d , match index %d and next index %d", rf.currentTerm, rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
				rf.nextIndex[server]--
				rf.matchIndex[server]--
				DPrintf("Leader %d sends log to server %d , match index %d and next index %d not accepted", rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
			}
			rf.mu.Unlock()
		}
	}()
}

func (rf *Raft) updateCommitIndex() {
	if !rf.isleader {
		return
	}
	for N := len(rf.log); N > rf.committedIndex; N-- {
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= N && rf.log[N-1].Term == rf.currentTerm {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			DPrintf("Leader %d update committedIndex %d", rf.me, N)
			rf.committedIndex = N
			go rf.persist()
			//DPrintf("Term %d Server %d update as a Leader.", rf.currentTerm, rf.me)
			go rf.applyLogs()
			break
		}
	}
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.committedIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.isleader = false
	rf.heartbeatInterval = 50 * time.Millisecond
	rf.electionTimeout = time.Duration((300 + rand.Intn(300))) * time.Millisecond
	rf.lastHeartbeat = time.Now()
	rf.applyCh = applyCh

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runElectionTimer()

	go rf.runHeartbeatSender()

	return rf
}

func (rf *Raft) runElectionTimer() {
	for {
		time.Sleep(rf.electionTimeout)
		rf.mu.Lock()
		if !rf.isleader && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			rf.startElection()
			rf.electionTimeout = time.Duration((300 + rand.Intn(300))) * time.Millisecond
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	votes := 1
	go rf.persist()

	DPrintf("Term %d Server %d, Start election! \n", rf.currentTerm, rf.me)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			var reply RequestVoteReply
			go func(server int) {
				if rf.sendRequestVote(server, args, &reply) {

					if reply.Term > rf.currentTerm || !reply.UpdatedFlag {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.isleader = false
						rf.lastHeartbeat = time.Now()
						go rf.persist()
						return
					} else if reply.VoteGranted {
						votes++
						if 2*votes > len(rf.peers) && !rf.isleader {
							rf.getElected()
						}
					}
				}
			}(i)
			if rf.votedFor == -1 {
				return
			}
		}
	}
}

func (rf *Raft) getElected() {
	rf.isleader = true
	DPrintf("Term %d Leader %d get elected as leader", rf.currentTerm, rf.me)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = rf.lastLogIndex
	}
}

func (rf *Raft) runHeartbeatSender() {
	for {
		if rf.isleader {
			rf.mu.Lock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.isleader {
					//DPrintf("Term %d Leader %d sending heartbeat to %d", rf.currentTerm, rf.me, i)
					go rf.sendLogsToFollower(i)
				}
			}
			rf.mu.Unlock()
		}
		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("Term %d Server %d gets log from Leader %d", args.Term, rf.me, args.LeaderId)

	reply.Accepted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.lastHeartbeat = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.isleader {
			DPrintf("Server %d lose leadership at Term %d after get an entry", rf.me, rf.currentTerm)
		}
		rf.isleader = false
		go rf.persist()
	}

	reply.Term = rf.currentTerm

	if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm && rf.log[args.PrevLogIndex].Command != args.PrevLogCommand) {
		//DPrintf("Log index %d unmatched!Server %d at Term %d", args.PrevLogIndex, rf.me, rf.currentTerm)
		reply.Accepted = false
		reply.Term = rf.currentTerm
		return
	}

	//DPrintf("Log index %d, Log matched!Server %d at Term %d", args.PrevLogIndex, rf.me, rf.currentTerm)

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	//DPrintf("Server %d update log to %d", rf.me, len(rf.log))

	reply.Accepted = true
	if len(rf.log) > 0 {
		if rf.lastLogIndex != rf.log[len(rf.log)-1].Index {
			rf.lastLogIndex = rf.log[len(rf.log)-1].Index
			DPrintf("Server %d update log to %d", rf.me, rf.lastLogIndex)
			rf.lastLogTerm = rf.currentTerm
			go rf.persist()
		}
	}

	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = min(args.LeaderCommit, rf.lastLogIndex)
		DPrintf("Server %d update committedIndex %d from log of %d, term %d", rf.me, rf.committedIndex, args.LeaderId, args.Term)
		DPrintf("%v", args.Entries)
		go rf.persist()
		go rf.applyLogs()
	}
}

/*func (rf *Raft) HeartBeat(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.isleader = false
	reply.Accepted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.lastHeartbeat = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isleader = false
		rf.persist()
	}
	reply.Term = rf.currentTerm
	reply.Accepted = true

	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = min(args.LeaderCommit, len(rf.log))
		go rf.applyLogs()
	}
}*/

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	go func() {
		for rf.lastApplied < rf.committedIndex {
			rf.lastApplied++
			go rf.persist()
			applyMsg := ApplyMsg{
				Index:   rf.lastApplied,
				Command: rf.log[rf.lastApplied-1].Command,
			}
			rf.applyCh <- applyMsg
			DPrintf("Term %d, Server %d apply msg %d, %v", rf.currentTerm, rf.me, applyMsg.Index, applyMsg.Command)
		}
	}()
}
