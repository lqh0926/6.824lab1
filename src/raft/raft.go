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
	//	"bytes"

	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term     int
	Logindex int
	Command  interface{}
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	electionTimeout time.Duration
	electionTimer   *time.Ticker
	logs            []LogEntry
	committedIndex  int
	state           string
	currentTerm     int
	votedCount      int
	votedFor        int
	leaderId        int
	nextIndex       []int
	matchIndex      []int
	waitapply       chan struct{}
	applyIndex      int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) runElectionTimer() {
	rf.mu.Lock()
	rf.electionTimer = time.NewTicker(rf.electionTimeout)
	rf.mu.Unlock()
	for range rf.electionTimer.C {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.state == "leader" {
			rf.mu.Unlock()
			continue
		}
		rf.startElection()
		rf.mu.Unlock()
	}
}
func (rf *Raft) startElection() {
	rf.state = "candidate"
	//fmt.Printf("server %d start election\n", rf.me)
	rf.currentTerm += 1
	rf.votedCount = 1
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs[len(rf.logs)-1].Logindex,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					rf.handlerRequestVoteReply(reply)
				}
			}(i)
		}
	}
}
func (rf *Raft) handlerRequestVoteReply(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.votedCount = 0
		rf.electionTimer.Reset(rf.electionTimeout)
		return
	}
	if reply.VoteGranted {
		rf.votedCount += 1
		if rf.votedCount == len(rf.peers)/2+1 {
			rf.state = "leader"
			rf.electionTimer.Stop()
			rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Logindex: len(rf.logs)})
		}
	}
}
func (rf *Raft) TikcerAppendEntries() {

	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		curstate := rf.state
		rf.mu.Unlock()
		if curstate == "leader" {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			currentCommit := rf.committedIndex
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.sendAndHandleAppendEntries(i, currentTerm, currentCommit)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (rf *Raft) sendAndHandleAppendEntries(server int, currentTerm int, currentCommit int) {
	rf.mu.Lock()

	// 如果当前不是领导者，直接返回
	if rf.state != "leader" {
		rf.mu.Unlock()
		return
	}

	// 构造 AppendEntriesArgs
	args := rf.buildAppendEntriesArgs(currentTerm, currentCommit)
	rf.mu.Unlock()

	// 发送 AppendEntries RPC 并处理回复
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.handleAppendEntriesReply(server, &args, &reply)
	}
}

// 构造 AppendEntriesArgs
func (rf *Raft) buildAppendEntriesArgs(currentTerm int, currentCommit int) AppendEntriesArgs {
	lastLogIndex := len(rf.logs) - 1

	if rf.logs[lastLogIndex].Logindex < currentCommit {
		return AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.logs[lastLogIndex].Logindex,
			PrevLogTerm:  rf.logs[lastLogIndex].Term,
			Entries:      []LogEntry{},
			LeaderCommit: currentCommit,
		}
	}
	if currentCommit == 0 {
		return AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			Entries:      rf.logs,
			LeaderCommit: currentCommit,
		}
	}
	return AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.logs[currentCommit-1].Logindex,
		PrevLogTerm:  rf.logs[currentCommit-1].Term,
		Entries:      rf.logs[currentCommit:],
		LeaderCommit: currentCommit,
	}
}

// 处理 AppendEntriesReply
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果回复的任期大于当前任期，切换为跟随者
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "follower"
		rf.resetElectionState()
		return
	}

	// 如果成功复制日志
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// 检查是否有多数 节点复制成功，更新 commitIndex
		rf.updateCommitIndex()
		return
	}

	// 如果失败，处理日志不一致问题
	if reply.ConflictIndex != -1 {
		rf.handleLogConflict(server, args, reply)
	}
}

// 更新 commitIndex
func (rf *Raft) updateCommitIndex() {
	// 统计 matchIndex，找到多数节点一致的日志索引
	matchIndexes := make([]int, len(rf.peers))
	copy(matchIndexes, rf.matchIndex)
	sort.Ints(matchIndexes)

	majorityIndex := matchIndexes[len(matchIndexes)/2]
	if majorityIndex > rf.committedIndex && rf.logs[majorityIndex].Term == rf.currentTerm {
		rf.committedIndex = majorityIndex
		select {
		case rf.waitapply <- struct{}{}:
		default:
		}
	}
}

// 处理日志冲突
func (rf *Raft) handleLogConflict(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.ConflictIndex == -1 {
		return
	}
	if reply.ConflictIndex != args.PrevLogIndex {
		rf.nextIndex[server] = reply.ConflictIndex
		return
	}
	for i := args.PrevLogIndex; i >= 0; i-- {
		if rf.logs[i].Term != args.PrevLogTerm {
			rf.nextIndex[server] = i
			break
		}
	}
}

func (rf *Raft) resetElectionState() {
	rf.votedFor = -1
	rf.votedCount = 0
	rf.electionTimer.Reset(rf.electionTimeout)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == "leader"

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.resetElectionState()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(rf.electionTimeout)
	} else {
		reply.VoteGranted = false
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.resetElectionState()
	}
	if args.PrevLogIndex > rf.logs[len(rf.logs)-1].Logindex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = len(rf.logs)
		rf.electionTimer.Reset(rf.electionTimeout)
		return
	}
	if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = args.PrevLogIndex
		rf.electionTimer.Reset(rf.electionTimeout)
		return
	}
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < rf.logs[len(rf.logs)-1].Logindex {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}
	rf.logs = append(rf.logs, args.Entries...)
	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = args.LeaderCommit
		select {
		case rf.waitapply <- struct{}{}:
		default:
		}

	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.nextIndex[args.LeaderId] = args.PrevLogIndex + len(args.Entries) + 1
	rf.matchIndex[args.LeaderId] = rf.nextIndex[args.LeaderId] - 1
	rf.electionTimer.Reset(rf.electionTimeout)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == "leader" {
		index = len(rf.logs)
		term = rf.currentTerm
		isLeader = true
		rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Logindex: index, Command: command})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1

	}

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	go rf.runElectionTimer()
	go rf.TikcerAppendEntries()

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
	rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
	rf.state = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votedCount = 0
	rf.leaderId = -1
	rf.logs = append(rf.logs, LogEntry{Term: 0, Logindex: 0})
	rf.committedIndex = 0
	rf.applyIndex = 0
	rf.waitapply = make(chan struct{}, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.ticker()
	go rf.applyLog(applyCh)
	go rf.printIndex()
	return rf
}

func (rf *Raft) applyLog(applyCh chan ApplyMsg) {
	for {
		<-rf.waitapply
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		commitindex := rf.committedIndex
		rf.mu.Unlock()
		for commitindex > rf.applyIndex {
			for i := rf.applyIndex + 1; i <= rf.committedIndex; i++ {
				applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
			}
			rf.applyIndex = rf.committedIndex
			rf.mu.Lock()
			commitindex = rf.committedIndex
			rf.mu.Unlock()
		}

	}

}

// 定时打印committedIndex和applyIndex
func (rf *Raft) printIndex() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		committedIndex := rf.committedIndex
		applyIndex := rf.applyIndex
		rf.mu.Unlock()
		fmt.Printf("server %d committedIndex %d applyIndex %d\n", rf.me, committedIndex, applyIndex)
		time.Sleep(1000 * time.Millisecond)
	}
}
