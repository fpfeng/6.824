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
	log         []*LogEntry
	state       RaftState

	termVotedFor      map[int]int
	termGetVotedCount map[int]int

	applyCh chan ApplyMsg

	stepAsCandidate bool // reset false when receive heartbeat rpc
	stopLogging     bool
	// 所有服务器经常改变
	commitIndex int
	lastApplied int

	// 领导状态才改变的
	nextIndex  map[int]int
	matchIndex map[int]int
}

func (rf *Raft) debugLog(format string, a ...interface{}) (n int, err error) {
	if rf.stopLogging {
		return
	}
	format = fmt.Sprintf("\033[38;5;%dmS%d \033[39;49m", rf.me+10, rf.me) + format
	return DPrintf(format, a...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
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
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) checkTermSwitchFollower(term int) bool {
	/*
		If RPC request or response contains term T > currentTerm:
		set currentTerm = T, convert to follower (§5.1)
	*/
	isNotSwitch := true

	rf.mu.Lock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.stepAsCandidate = false
		isNotSwitch = false
		rf.votedFor = 0
		rf.debugLog("reset to follower")
	}
	rf.mu.Unlock()

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
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
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
	if logLength > 0 {
		lastLogTerm = rf.log[logLength-1].Term
	} else {
		lastLogTerm = 0
	}

	if _, exists := rf.termVotedFor[args.Term]; exists {
		isTermNotVoted = false
	}

	rf.debugLog("recv request vote, term: %d", currentTerm)

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
	isLogUpToDate := args.LastLogTerm >= lastLogTerm && args.LastLogIndex > logLength-1
	rf.debugLog("candidate%d: [term: %d last index: %d last term: %d] current term: %d, last index:%d", args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm, currentTerm, logLength-1)
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

	return ok && rf.checkTermSwitchFollower(reply.Term)
}

func (rf *Raft) deleteConflictEntries(newEntryIndex int, newEnties []*LogEntry) {
	/*
		3. If an existing entry conflicts with a new one (same index
		but different terms), delete the existing entry and all that
		follow it (§5.3)
	*/
	if len(newEnties) == 0 {
		return
	}
	if rf.log[newEntryIndex].Term != newEnties[0].Term {
		rf.log = rf.log[:newEntryIndex]
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

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	isPrevTermMatchs := false
	isLogLengthOk := len(rf.log) >= args.PrevLogIndex
	if isLogLengthOk {
		if args.PrevLogIndex == -1 {
			isPrevTermMatchs = true
		} else {
			isPrevTermMatchs = rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
		}
	}

	if !isLogLengthOk || !isPrevTermMatchs {
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	rf.stepAsCandidate = false
	rf.mu.Unlock()
	rf.debugLog("pass append enties pre-check [log length: %d]", len(rf.log))
	rf.checkTermSwitchFollower(args.Term)
	reply.Success = true

	rf.mu.Lock()
	rf.deleteConflictEntries(args.PrevLogIndex+1, args.Entries)
	if len(args.Entries) > 0 {
		// Append any new entries not already in the log 这里感觉不对
		rf.log = append(rf.log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.debugLog("leaderCommit %d > commitIndex %d", args.LeaderCommit, rf.commitIndex)
		/*
			5. If leaderCommit > commitIndex, set commitIndex =
			min(leaderCommit, index of last new entry)
		*/
		var min int
		lastNewEntryIndex := len(rf.log) - 1
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
	rf.mu.Unlock()
	rf.debugLog("done append enties")
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.checkTermSwitchFollower(reply.Term)
	}
	return ok
}

func (rf *Raft) sendHeartbeat() {
	rf.debugLog("send heartbeat about to lock")
	rf.mu.Lock()
	aea := AppendEntriesArgs{}
	aea.Term = rf.currentTerm
	aea.LeaderID = rf.me
	if len(rf.log) > 0 {
		aea.PrevLogTerm = rf.log[len(rf.log)-1].Term
		aea.PrevLogIndex = len(rf.log) - 1
	} else {
		aea.PrevLogTerm = rf.currentTerm
		aea.PrevLogIndex = -1
	}
	aea.Entries = make([]*LogEntry, 0)
	aea.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()
	rf.debugLog("send heartbeat unlock")

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		aer := AppendEntriesReply{}
		go rf.sendAppendEntries(idx, &aea, &aer)
	}
}

func (rf *Raft) checkIncreaseCommitIndex() {
	/*
		If there exists an N such that N > commitIndex, a majority
		of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		set commitIndex = N (§5.3, §5.4).
	*/
	matchIndexCount := make(map[int]int)
	for idx, nodeMatchIndex := range rf.matchIndex {
		if idx == rf.me {
			continue
		}

		matchIndexCount[nodeMatchIndex]++
	}

	var isMajorityExists bool
	for {
		N := rf.commitIndex + 1
		for matchIndex, count := range matchIndexCount {
			if (matchIndex >= N) && (count > len(rf.peers)/2) {
				isMajorityExists = true
				break
			}
		}

		if !isMajorityExists {
			break
		}

		var isNExists bool
		if len(rf.log) > N && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			isNExists = true
		}

		if !isNExists {
			break
		}
	}
}

func (rf *Raft) checkApplyLog() {
	/*
		If commitIndex > lastApplied: increment lastApplied, apply
		log[lastApplied] to state machine (§5.3)
	*/
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			am := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.lastApplied++
			rf.mu.Unlock()
			rf.applyCh <- am
		} else {
			rf.mu.Unlock()
			break
		}
	}
}

func (rf *Raft) doReplicateLog() {
	/*
		If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		• If successful: update nextIndex and matchIndex for follower (§5.3)
		• If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	*/
	for idx := range rf.peers {
		nextIndex := rf.nextIndex[idx]
		if idx == rf.me || nextIndex == rf.matchIndex[idx] || len(rf.log)-1 < nextIndex {
			continue
		}

		prevLogIndex := len(rf.log) - 2

		var aea AppendEntriesArgs
		aea.LeaderID = rf.me
		aea.LeaderCommit = rf.commitIndex
		aea.PrevLogIndex = prevLogIndex
		aea.PrevLogTerm = rf.log[prevLogIndex].Term
		aea.Entries = rf.log[nextIndex:]

		go func(nodeIdx int) {
			var aer AppendEntriesReply
			isOk := rf.sendAppendEntries(nodeIdx, &aea, &aer)

			if isOk && aer.Success {
				rf.nextIndex[nodeIdx] += len(aea.Entries)
				rf.matchIndex[nodeIdx] += len(aea.Entries)
			} else {
				rf.nextIndex[nodeIdx]--
			}

		}(idx)
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
	isLeader := false
	rf.mu.Lock()
	isLeader = rf.state == Leader

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	le := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, &le)
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.mu.Unlock()

	go rf.doReplicateLog()

	return index, term, isLeader
}

func (rf *Raft) stepAsLeader() {
	rf.debugLog("******* step as leader *******")
	rf.mu.Lock()
	if rf.state != Leader {
		rf.state = Leader
		rf.initNextIndexAndMatchIndex()
	}
	rf.mu.Unlock()
	go rf.sendHeartbeat()
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
	if len(rf.log) > 0 {
		rva.LastLogIndex = len(rf.log) - 1
		rva.LastLogTerm = rf.log[rva.LastLogIndex].Term
	} else {
		rva.LastLogIndex = 0
		rva.LastLogTerm = 0
	}
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

			var currentTerm int
			rf.mu.Lock()
			currentTerm = rf.currentTerm
			rf.mu.Unlock()
			rf.debugLog("follower%d vote:[isOk: %t grant: %t @ term: %d], current term: %d", nodeIdx, isOk, rvr.VoteGranted, rvr.Term, currentTerm)
			if rvr.Term < currentTerm && rvr.VoteGranted {
				var getVotedCount int
				rf.mu.Lock()
				if rf.termGetVotedCount[currentTerm] == 0 {
					rf.termGetVotedCount[currentTerm] = 2 // leader vote itself + current follower vote
				} else {
					rf.termGetVotedCount[currentTerm]++
				}
				getVotedCount = rf.termGetVotedCount[currentTerm]
				rf.mu.Unlock()

				if getVotedCount > len(rf.peers)/2 {
					rf.stepAsLeader()
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

func sleepRandomRange(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	t := rand.Intn(max-min) + min
	time.Sleep(time.Duration(t) * time.Millisecond)
	return t
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.log = make([]*LogEntry, 0)
	rf.termVotedFor = make(map[int]int)
	rf.termGetVotedCount = make(map[int]int)

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			var currentState RaftState
			var stepAsCandidate bool
			rf.mu.Lock()
			currentState = rf.state
			stepAsCandidate = rf.stepAsCandidate
			rf.mu.Unlock()

			rf.debugLog("current state: %d stepAsCandidate: %t", currentState, stepAsCandidate)
			switch currentState {
			case Follower:
				if !stepAsCandidate {
					rf.mu.Lock()
					rf.stepAsCandidate = true
					rf.mu.Unlock()
					rf.debugLog("follower awake %dms", sleepRandomRange(400, 550))
				} else {
					rf.mu.Lock()
					rf.state = Candidate
					rf.mu.Unlock()
					rf.debugLog("step as candidate")
				}
			case Candidate:
				rf.startsElection()
				rf.debugLog("candidate awake %dms", sleepRandomRange(200, 450))
			case Leader:
				rf.sendHeartbeat()
				sleepRandomRange(100, 110)
				rf.debugLog("leader awake")
			}
			rf.debugLog("end loop")
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
