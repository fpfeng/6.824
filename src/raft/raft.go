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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器固定存在
	currentTerm int
	votedFor    int
	log         []LogEntry
	state       RaftState

	termVotedFor      map[int]int
	termGetVotedCount map[int]int

	applyCh chan ApplyMsg

	stopLogging bool
	// 所有服务器经常改变
	commitIndex int
	lastApplied int

	// 领导状态才改变的
	nextIndex  map[int]int
	matchIndex map[int]int

	followerTicker  *time.Ticker
	heartbeatTicker *time.Ticker

	heartbeatInProgress map[int]bool // pretty sure this is stupid
}

func (rf *Raft) debugLog(format string, a ...interface{}) (n int, err error) {
	if rf.stopLogging {
		return
	}
	format = fmt.Sprintf("[%d] ", rf.me) + format
	return DPrintf(format, a...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.isInState(Leader)
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) resetToFollower() {
	if !rf.isInState(Follower) {
		rf.debugLog("reset to follower")
		rf.state = Follower
		rf.votedFor = 0
		stopTickerIfIsValid(rf.heartbeatTicker)
	}
}

func (rf *Raft) checkTermSwitchFollower(term int) bool {
	/*
		If RPC request or response contains term T > currentTerm:
		set currentTerm = T, convert to follower (§5.1)
	*/
	isNotSwitch := true

	rf.mu.Lock()
	if term > rf.currentTerm {
		rf.debugLog("set term to %d", term)
		rf.currentTerm = term
		rf.resetToFollower()
		isNotSwitch = false
	}
	rf.mu.Unlock()

	rf.debugLog("done check term")
	return isNotSwitch
}

func (rf *Raft) initNextIndexAndMatchIndex() {
	/*
		(Reinitialized after election)
		nextIndex[] for each server, index of the next log entry
		to send to that server (initialized to leader
		last log index + 1)
		matchIndex[] for each server, index of highest log entry
		known to be replicated on server
		(initialized to 0, increases monotonically)
	*/
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.heartbeatInProgress = make(map[int]bool) // it is 23:28 right now, too lazy to move it another place

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
		rf.heartbeatInProgress[i] = false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	var currentTerm int
	var votedFor int
	var logLength int
	var lastLogTerm int
	isTermNotVoted := true

	rf.mu.Lock()
	currentTerm = rf.currentTerm
	votedFor = rf.votedFor
	logLength = len(rf.log)
	lastLogTerm = rf.log[logLength-1].Term

	if _, exists := rf.termVotedFor[args.Term]; exists {
		isTermNotVoted = false
	}

	/*
		1. Reply false if term < currentTerm (§5.1)
		2. If votedFor is null or candidateId, and candidate’s log is at
		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

		Raft determines which of two logs is more up-to-date
		by comparing the index and term of the last entries in the
		logs. If the logs have last entries with different terms, then
		the log with the later term is more up-to-date. If the logs
		end with the same term, then whichever log is longer is
		more up-to-date.
	*/
	voteGranted := false
	isLargeThanCurrentTerm := args.Term > currentTerm
	isLogUpToDate := args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= logLength-1
	rf.debugLog("candidate%d: (term: %d last index: %d last term: %d) current term: %d, last index:%d", args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm, currentTerm, logLength-1)
	rf.debugLog("candidate%d: isLargeThanCurrentTerm: %t, isLogUpToDate: %t, isTermNotVoted: %t, voteFor: %d", args.CandidateID, isLargeThanCurrentTerm, isLogUpToDate, isTermNotVoted, votedFor)
	voteGranted = (votedFor == 0 || votedFor == args.CandidateID) && isLargeThanCurrentTerm && isLogUpToDate && isTermNotVoted

	if voteGranted {
		reply.Term = currentTerm
		reply.VoteGranted = true

		rf.votedFor = args.CandidateID
		rf.termVotedFor[args.Term] = args.CandidateID
	}
	rf.mu.Unlock()

	rf.checkTermSwitchFollower(args.Term)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.checkTermSwitchFollower(reply.Term)
	return ok
}

func (rf *Raft) deleteConflictEntries(newLogIndex int, newEnties []LogEntry) {
	/*
		3. If an existing entry conflicts with a new one (same index
		but different terms), delete the existing entry and all that
		follow it (§5.3)
	*/
	if len(newEnties) == 0 || len(rf.log) <= newLogIndex {
		return
	}

	if rf.log[newLogIndex].Term != newEnties[0].Term {
		rf.debugLog("deleteConflictEntries logs after index:%d", newLogIndex)
		rf.log = rf.log[:newLogIndex]
	} else {
		rf.deleteConflictEntries(newLogIndex+1, newEnties)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
		1. Reply false if term < currentTerm (§5.1)
		2. Reply false if log doesn’t contain an entry at prevLogIndex
		whose term matches prevLogTerm (§5.3)
		3. If an existing entry conflicts with a new one (same index
		but different terms), delete the existing entry and all that
		follow it (§5.3)
		4. Append any new entries not already in the log
		5. If leaderCommit > commitIndex, set commitIndex =
		min(leaderCommit, index of last new entry)
	*/

	rf.mu.Lock()
	currentLogLength := len(rf.log)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.debugLog("s%d term less than me", args.LeaderID)
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	isPrevTermMatchs := false
	isLogLengthOk := false
	isLogLengthOk = currentLogLength > args.PrevLogIndex
	rf.debugLog("append params: [prevLogIndex:%d prevLogTerm:%d logLength:%d], current log length:%d", args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), currentLogLength)
	if isLogLengthOk {
		rf.debugLog("node prevLogTerm: %d", rf.log[args.PrevLogIndex].Term)
		isPrevTermMatchs = rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
	}

	if !isLogLengthOk || !isPrevTermMatchs {
		rf.debugLog("!!params check fail: log length ok:%t, prev term match:%t", isLogLengthOk, isPrevTermMatchs)
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	rf.resetToFollower()
	rf.mu.Unlock()
	newEntriesLength := len(args.Entries)

	rf.debugLog("pass append pre-check [payload log length:%d]", newEntriesLength)
	rf.checkTermSwitchFollower(args.Term)
	reply.Success = true

	rf.mu.Lock()
	rf.followerTicker.Stop()
	rf.deleteConflictEntries(args.PrevLogIndex+1, args.Entries)
	if newEntriesLength > 0 {
		// Append any new entries not already in the log 这里感觉不对
		for idx, newLog := range args.Entries {
			logIndex := args.PrevLogIndex + idx + 1

			if logIndex >= len(rf.log) {
				rf.log = append(rf.log, newLog)
			}

			localLog := rf.log[logIndex]
			cmd1, ok1 := localLog.Command.(int)
			cmd2, ok2 := newLog.Command.(int)

			if ok1 && ok2 {
				if cmd1 == cmd2 {
					continue
				} else {
					rf.log[logIndex] = newLog
				}
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.debugLog("leaderCommit %d > commitIndex %d", args.LeaderCommit, rf.commitIndex)
		/*
			5. If leaderCommit > commitIndex, set commitIndex =
			min(leaderCommit, index of last new entry)
		*/
		var min int
		lastNewEntryIndex := currentLogLength - 1
		rf.debugLog("lastNewEntryIndex: %d", lastNewEntryIndex)
		if args.LeaderCommit < lastNewEntryIndex {
			rf.debugLog("set commitIndex as LeaderCommit")
			min = args.LeaderCommit
		} else {
			rf.debugLog("set commitIndex as lastNewEntryIndex")
			min = lastNewEntryIndex
		}
		rf.commitIndex = min
	}
	if newEntriesLength > 0 {
		rf.debugLog("new log content:%d", args.Entries[0].Command)
		rf.debugLog("done AppendEntries with %d new log, current total log:%d", newEntriesLength, len(rf.log))
		for idx, item := range rf.log {
			rf.debugLog("log idx:%d, content:%d", idx, item.Command)
		}
	} else {
		rf.debugLog("done heartbeat AppendEntries")
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.checkTermSwitchFollower(reply.Term)
	}
	return ok
}

func (rf *Raft) sendHeartbeatToNode(nodeIdx int, replicateLogIfReplySuccess bool) {
	rf.mu.Lock()
	if !rf.isInState(Leader) || rf.heartbeatInProgress[nodeIdx] {
		rf.debugLog("skip heartbeat to node:%d current state:%d heartbeatInProgress:%t", nodeIdx, rf.state, rf.heartbeatInProgress[nodeIdx])
		rf.mu.Unlock()
		return
	}

	rf.heartbeatInProgress[nodeIdx] = true

	aea := AppendEntriesArgs{}
	aea.Term = rf.currentTerm
	aea.LeaderID = rf.me
	nodeNextIndex := rf.nextIndex[nodeIdx]
	logLength := len(rf.log)
	rf.debugLog("heartbeat to node:%d, nodeNextIndex:%d, log length:%d", nodeIdx, nodeNextIndex, logLength)

	aea.PrevLogTerm = rf.log[nodeNextIndex-1].Term
	aea.PrevLogIndex = nodeNextIndex - 1
	aea.Entries = nil
	aea.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()
	rf.debugLog("send heartbeat unlock, prevLogTerm:%d prevLogIndex:%d", aea.PrevLogTerm, aea.PrevLogIndex)

	aer := AppendEntriesReply{}
	isOk := rf.sendAppendEntries(nodeIdx, &aea, &aer)
	if isOk {
		if aer.Success {
			if replicateLogIfReplySuccess && logLength >= nodeNextIndex {
				rf.replicateLogToNode(nodeIdx)
			}
		} else {
			rf.mu.Lock()
			if rf.nextIndex[nodeIdx] > 1 {
				rf.nextIndex[nodeIdx]--
				rf.debugLog("node %d heartbeat fail, nextIndex--:%d", nodeIdx, rf.nextIndex[nodeIdx])
				rf.mu.Unlock()
				rf.sendHeartbeatToNode(nodeIdx, replicateLogIfReplySuccess)
			} else {
				rf.mu.Unlock()
				rf.debugLog("node %d heartbeat fail, nextIndex:%d", nodeIdx, rf.nextIndex[nodeIdx])
			}
		}
	}

	rf.mu.Lock()
	rf.heartbeatInProgress[nodeIdx] = false
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(replicateLogIfReplySuccess bool) {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go rf.sendHeartbeatToNode(idx, replicateLogIfReplySuccess)
		rf.debugLog("heartbeat send to node:%d replicateLogIfReplySuccess:%t", idx, replicateLogIfReplySuccess)
	}
}

func stopTickerIfIsValid(ticker *time.Ticker) {
	if ticker != nil {
		ticker.Stop()
	}
}

func (rf *Raft) intervalSendHeartbeat() {
	stopTickerIfIsValid(rf.heartbeatTicker)
	go rf.sendHeartbeat(false)
	ticker := time.NewTicker(150 * time.Millisecond)
	rf.mu.Lock()
	rf.heartbeatTicker = ticker
	rf.mu.Unlock()

	go func() {
		for range ticker.C {
			rf.sendHeartbeat(true)
		}
	}()
}

func (rf *Raft) checkIncreaseCommitIndex() {
	/*
		If there exists an N such that N > commitIndex, a majority
		of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		set commitIndex = N (§5.3, §5.4).
	*/
	rf.debugLog("checkIncreaseCommitIndex about to lock")
	rf.mu.Lock()
	if !rf.isInState(Leader) {
		rf.mu.Unlock()
		rf.debugLog("state changed: %d, skip checkIncreaseCommitIndex", rf.state)
		return
	}

	matchIndexCount := make(map[int]int)
	for idx, nodeMatchIndex := range rf.matchIndex {
		if idx == rf.me {
			continue
		}

		rf.debugLog("node%d matchIndex:%d", idx, nodeMatchIndex)
		matchIndexCount[nodeMatchIndex]++
	}
	matchIndexCount[len(rf.log)-1]++ // leader it self

	var isMajorityExists bool

	for {
		N := rf.commitIndex + 1
		rf.debugLog("N: %d", N)
		for matchIndex, count := range matchIndexCount {
			if (matchIndex >= N) && (count > len(rf.peers)/2) {
				isMajorityExists = true
				break
			}
		}

		rf.debugLog("isMajorityExists:%t", isMajorityExists)
		if !isMajorityExists {
			break
		}

		var isNExists bool
		if len(rf.log) > N {
			rf.debugLog("log N term:%d, current term:%d", rf.log[N].Term, rf.currentTerm)
			if rf.log[N].Term == rf.currentTerm {
				rf.commitIndex = N
				rf.debugLog("set commitIndex to %d", N)
				isNExists = true
			}
		}

		rf.debugLog("isNExists:%t", isNExists)
		if !isNExists {
			break
		}
	}
	rf.mu.Unlock()
	rf.debugLog("done checkIncreaseCommitIndex")
}

func (rf *Raft) checkApplyLog() {
	/*
		If commitIndex > lastApplied: increment lastApplied, apply
		log[lastApplied] to state machine (§5.3)
	*/
	rf.debugLog("start check apply log")
	for {
		rf.mu.Lock()
		rf.debugLog("commitIndex:%d, lastApplied:%d", rf.commitIndex, rf.lastApplied)
		if rf.commitIndex > rf.lastApplied {
			am := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied+1].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.lastApplied++

			rf.mu.Unlock()
			rf.debugLog("send applyCh CommandValid:%t CommandIndex:%d", am.CommandValid, am.CommandIndex)
			rf.applyCh <- am
			rf.debugLog("done send applyCh")
		} else {
			rf.mu.Unlock()
			break
		}
	}
}

func (rf *Raft) isInState(state RaftState) bool {
	return rf.state == state
}

func (rf *Raft) replicateLogToNode(nodeIndex int) {
	rf.debugLog("replicateLogToNode: %d", nodeIndex)
	rf.mu.Lock()

	if !rf.isInState(Leader) {
		rf.mu.Unlock()
		rf.debugLog("state changed: %d", rf.state)
		return
	}

	nextIndex := rf.nextIndex[nodeIndex]
	matchIndex := rf.matchIndex[nodeIndex]
	lastLogIndex := len(rf.log) - 1
	rf.mu.Unlock()

	rf.debugLog("node:%d nextIndex:%d matchIndex:%d lastLogIndex:%d", nodeIndex, nextIndex, matchIndex, lastLogIndex)
	if lastLogIndex < 1 || lastLogIndex < nextIndex {
		rf.debugLog("node:%d skip replicate log", nodeIndex)
		return
	}

	rf.mu.Lock()
	var aea AppendEntriesArgs
	aea.Term = rf.currentTerm
	aea.LeaderID = rf.me
	aea.LeaderCommit = rf.commitIndex
	aea.PrevLogIndex = nextIndex - 1
	aea.PrevLogTerm = rf.log[nextIndex-1].Term
	aea.Entries = make([]LogEntry, len(rf.log[nextIndex:]))
	copy(aea.Entries, rf.log[nextIndex:])

	rf.debugLog("send %d logs to node:%d from idx:%d, prevlogIndex:%d, prevLogTerm:%d", len(aea.Entries), nodeIndex, nextIndex, aea.PrevLogIndex, aea.PrevLogTerm)
	rf.mu.Unlock()

	go func(nodeIdx int) {
		rf.debugLog("rpc call node: %d", nodeIdx)
		var aer AppendEntriesReply
		isOk := rf.sendAppendEntries(nodeIdx, &aea, &aer)

		if isOk {
			rf.debugLog("node:%d reply success:%t term:%d", nodeIdx, aer.Success, aer.Term)
			if aer.Success {
				rf.mu.Lock()
				rf.nextIndex[nodeIdx] = nextIndex + len(aea.Entries)
				rf.matchIndex[nodeIdx] = matchIndex + len(aea.Entries)
				rf.mu.Unlock()
			} else {
				rf.debugLog("node: %d nextIndex: %d fail replicate log", nodeIdx, rf.nextIndex[nodeIdx])
			}
		} else {
			rf.debugLog("sendAppendEntries node:%d not ok", nodeIdx)
		}

	}(nodeIndex)

}

func (rf *Raft) doReplicateLog() {
	rf.debugLog("doReplicateLog...")
	/*
		If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		• If successful: update nextIndex and matchIndex for follower (§5.3)
		• If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	*/
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		rf.replicateLogToNode(idx)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	isLeader := rf.isInState(Leader)

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	le := LogEntry{Command: command, Term: rf.currentTerm, Index: len(rf.log)}
	rf.log = append(rf.log, le)
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.debugLog(">> append log, commitIndex: %d, length: %d, content: %d, prev content: %d", rf.commitIndex, len(rf.log), command, rf.log[index-1].Command)
	for idx, item := range rf.log {
		rf.debugLog("idx:%d, content:%d", idx, item.Command)
	}
	rf.mu.Unlock()

	go rf.doReplicateLog()
	return index, term, isLeader
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	if rf.isInState(Candidate) {
		rf.state = Leader
		rf.initNextIndexAndMatchIndex()
		rf.debugLog("******* become leader *******")
	} else {
		rf.debugLog("current state: %d, aint gonna do it", rf.state)
	}
	rf.mu.Unlock()
}

func (rf *Raft) startsElection() {
	/*
		• On conversion to candidate, start election:
		• Increment currentTerm
		• Vote for self
		• Reset election timer
		• Send RequestVote RPCs to all other servers
		• If votes received from majority of servers: become leader
		• If AppendEntries RPC received from new leader: convert to
		follower
		• If election timeout elapses: start new election
	*/
	rf.debugLog("start election")
	rva := RequestVoteArgs{CandidateID: rf.me}

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.termVotedFor[rf.currentTerm] = rf.me
	rva.Term = rf.currentTerm
	rva.LastLogIndex = len(rf.log) - 1
	rva.LastLogTerm = rf.log[rva.LastLogIndex].Term
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		rf.debugLog("send vote to follower%d", idx)
		go func(nodeIdx int) {
			rvr := RequestVoteReply{}
			isOk := rf.sendRequestVote(nodeIdx, &rva, &rvr)
			if !isOk {
				return
			}

			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			rf.debugLog("follower%d vote:[isOk:%t grant:%t term: %d], current term:%d", nodeIdx, isOk, rvr.VoteGranted, rvr.Term, currentTerm)
			if rvr.Term < currentTerm && rvr.VoteGranted {
				rf.mu.Lock()
				if rf.termGetVotedCount[currentTerm] == 0 {
					rf.termGetVotedCount[currentTerm] = 2 // leader vote itself + current follower vote
				} else {
					rf.termGetVotedCount[currentTerm]++
				}
				becomeLeaderVoteCount := rf.termGetVotedCount[currentTerm]
				rf.mu.Unlock()

				if becomeLeaderVoteCount > len(rf.peers)/2 {
					rf.becomeLeader()
					rf.intervalSendHeartbeat()
					return
				}
			}
		}(idx)
	}

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.debugLog("killed")
	rf.mu.Lock()
	rf.stopLogging = true
	rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func randomRangeMillisecond(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}

func sleepRandomRange(min, max int) int {
	t := randomRangeMillisecond(min, max)
	time.Sleep(time.Duration(t) * time.Millisecond)
	return t
}

func (rf *Raft) waitTimeoutTurnCandidate() {
	ms := randomRangeMillisecond(390, 440)

	t := time.Duration(ms)
	ticker := time.NewTicker(t * time.Millisecond)
	rf.mu.Lock()
	rf.state = Follower
	rf.followerTicker = ticker
	rf.mu.Unlock()

	go func() {
		for range ticker.C {
			rf.mu.Lock()
			rf.state = Candidate
			rf.mu.Unlock()
		}
	}()
	addFiveMS := time.Duration(ms + 5)
	time.Sleep(addFiveMS * time.Millisecond)
	ticker.Stop()
	rf.debugLog("follower awake %dms", ms+5)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{0, 0, 0})
	rf.termVotedFor = make(map[int]int)
	rf.termGetVotedCount = make(map[int]int)

	// Your initialization code here (2A, 2B, 2C).

	go func() {
		for {
			var currentState RaftState
			rf.mu.Lock()
			currentState = rf.state
			rf.mu.Unlock()

			rf.debugLog("current state: %d", currentState)
			switch currentState {
			case Follower:
				rf.waitTimeoutTurnCandidate()
			case Candidate:
				rf.startsElection()
				rf.debugLog("candidate awake %dms", sleepRandomRange(200, 450))
			case Leader:
				rf.intervalSendHeartbeat()
				rf.checkIncreaseCommitIndex()
				rf.debugLog("leader awake %dms", sleepRandomRange(60, 80))
			}
			rf.checkApplyLog()
			rf.debugLog("end loop")
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
