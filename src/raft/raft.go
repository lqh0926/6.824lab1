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

	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

// minInt returns the smaller of x or y.
func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
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
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
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
	snapshot        Snapshot

	applyIndex int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// type myticker struct {
// 	ticker *time.Ticker
// 	server int
// }

// func makeTicker(d time.Duration, s int) *myticker {
// 	return &myticker{time.NewTicker(d), s}
// }
// func (t *myticker) C() <-chan time.Time {
// 	fmt.Println("server ", t.server, " ticker")
// 	return t.ticker.C
// }
// func (t *myticker) Reset(d time.Duration) {
// 	fmt.Println("server ", t.server, " reset")
// 	t.ticker.Reset(d)
// }

func (rf *Raft) runElectionTimer() {
	rf.mu.Lock()

	electionTime := rf.electionTimer
	rf.mu.Unlock()
	for range electionTime.C {
		//fmt.Println("server ", electionTime.server, " ticker")
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.electionTimer.Reset(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
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
		rf.electionTimer.Reset(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
		rf.persist()
		return
	}
	if reply.Term < rf.currentTerm || rf.state != "candidate" {
		return
	}
	if reply.VoteGranted {
		rf.votedCount += 1
		if rf.votedCount == len(rf.peers)/2+1 {
			rf.state = "leader"
			rf.electionTimer.Reset(time.Minute)
			rf.leaderId = rf.me
			rf.nextIndex = make([]int, len(rf.peers))

			rf.matchIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Logindex + 1
				//fmt.Println("server ", rf.me, " nextIndex ", rf.nextIndex[i])
				rf.matchIndex[i] = 0
			}
			rf.persist()
			//rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Logindex: len(rf.logs)})
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
		if curstate == "leader" {

			currentTerm := rf.currentTerm
			currentCommit := rf.committedIndex

			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.sendAndHandleAppendEntries(i, currentTerm, currentCommit)
				}
			}
		}
		rf.mu.Unlock()
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
	args, err := rf.buildAppendEntriesArgs(server, currentTerm, currentCommit)
	if err != nil {
		go rf.sendAndHandleInstallSnapshot(server)
		rf.mu.Unlock()
		return
	}
	rf.persist()
	rf.mu.Unlock()

	// 发送 AppendEntries RPC 并处理回复
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.handleAppendEntriesReply(server, &args, &reply)
	}
}

type InstallSnapArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapArgs, reply *InstallSnapReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.electionTimer.Reset(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
		rf.persist()
	}
	rf.currentTerm = args.Term
	reply.Term = args.Term
	reply.Success = true

	rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
	rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
	rf.logs = []LogEntry{{Term: rf.snapshot.LastIncludedTerm, Logindex: rf.snapshot.LastIncludedIndex}}
	rf.committedIndex = rf.snapshot.LastIncludedIndex
	rf.snapshot.Data = args.Data

	select {
	case rf.waitapply <- struct{}{}:
	default:
	}
	rf.persist()
}

func (rf *Raft) sendAndHandleInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.state != "leader" {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshot.LastIncludedIndex,
		LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
		Data:              rf.snapshot.Data,
	}
	rf.mu.Unlock()

	reply := InstallSnapReply{}
	if rf.sendInstallSnapshot(server, &args, &reply) {
		rf.handleInstallSnapshotReply(server, &args, &reply)
	}
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapArgs, reply *InstallSnapReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) handleInstallSnapshotReply(server int, args *InstallSnapArgs, reply *InstallSnapReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "follower"
		rf.resetElectionState()
		rf.persist()
		return
	}
	if reply.Success {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		return
	}
}

// 构造 AppendEntriesArgs
func (rf *Raft) buildAppendEntriesArgs(server int, currentTerm int, currentCommit int) (AppendEntriesArgs, error) {
	if rf.nextIndex[server] <= rf.snapshot.LastIncludedIndex {
		return AppendEntriesArgs{}, errors.New("nextIndex < snapshot.LastIncludedIndex")
	}
	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.logs[rf.nextIndex[server]-1-rf.snapshot.LastIncludedIndex].Term,
		LeaderCommit: currentCommit,
		Entries:      rf.logs[rf.nextIndex[server]-rf.snapshot.LastIncludedIndex:],
	}
	return args, nil
}

// 处理 AppendEntriesReply
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return
	}
	// 如果回复的任期大于当前任期，切换为跟随者
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "follower"
		rf.resetElectionState()
		rf.persist()
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
	if majorityIndex > rf.committedIndex && rf.logs[majorityIndex-rf.snapshot.LastIncludedIndex].Term == rf.currentTerm {
		rf.committedIndex = majorityIndex
		//fmt.Println("server ", rf.me, " commit ", rf.committedIndex, "update")
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
	if rf.snapshot.LastIncludedIndex >= args.PrevLogIndex {
		return
	}
	rf.nextIndex[server] = rf.snapshot.LastIncludedIndex + 1
	for i := args.PrevLogIndex; i >= rf.snapshot.LastIncludedIndex+1; i-- {
		if rf.logs[i-rf.snapshot.LastIncludedIndex].Term != args.PrevLogTerm {
			rf.nextIndex[server] = i
			break
		}
	}
}

func (rf *Raft) resetElectionState() {
	rf.votedFor = -1
	rf.votedCount = 0
	rf.electionTimer.Reset(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot.Data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}
func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	rf.snapshot.Data = data
	rf.snapshot.LastIncludedIndex = rf.logs[0].Logindex
	rf.snapshot.LastIncludedTerm = rf.logs[0].Term

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.snapshot.LastIncludedIndex {
		return
	}
	lastIndex := rf.logs[len(rf.logs)-1].Logindex
	if index > lastIndex {
		return
	}
	rf.logs = rf.logs[index-rf.snapshot.LastIncludedIndex:]
	rf.snapshot.LastIncludedIndex = index
	rf.snapshot.Data = snapshot
	rf.snapshot.LastIncludedTerm = rf.logs[0].Term
	rf.persist()
	//fmt.Println("server ", rf.me, " snapshot ", snapshot, " index ", index)

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
	//打印args和自己和状态
	//fmt.Println("server ", rf.me, " receive requestvote from ", args.CandidateId, " term ", args.LastLogTerm, " logindex ", args.LastLogIndex)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}
	if args.Term > rf.currentTerm {
		//fmt.Print("server ", rf.me, " ", rf.currentTerm, " ", args.Term)
		rf.currentTerm = args.Term
		if rf.state != "follower" {
			rf.resetElectionState()
			rf.state = "follower"
		} else {
			rf.votedFor = -1
		}

	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := rf.logs[len(rf.logs)-1].Logindex
		lastLogTerm := rf.logs[len(rf.logs)-1].Term
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {

			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.votedCount = 0
			rf.electionTimer.Reset(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
		} else {
			reply.VoteGranted = false
			//fmt.Println("server ", rf.me, " receive requestvote from ", args.CandidateId, " term ", args.LastLogTerm, " logindex ", args.LastLogIndex)

			//fmt.Println("server ", rf.me, " vote for ", args.CandidateId, " false", "lastlogindex ", lastLogIndex, " lastlogterm ", lastLogTerm)
		}

	} else {
		reply.VoteGranted = false
		//fmt.Println("server ", rf.me, " vote for ", args.CandidateId, " false", "voted for ", rf.votedFor)
	}
	reply.Term = rf.currentTerm
	rf.persist()

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("server ", rf.me, " receive appendentries from ", args.LeaderId, " term ", args.Term, " logindex ", args.PrevLogIndex)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.resetElectionState()
		rf.persist()
	}
	if args.PrevLogIndex > rf.logs[len(rf.logs)-1].Logindex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.logs[len(rf.logs)-1].Logindex + 1
		rf.electionTimer.Reset(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
		return
	}
	if args.Entries != nil && args.Entries[len(args.Entries)-1].Logindex < rf.logs[len(rf.logs)-1].Logindex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.PrevLogIndex-rf.snapshot.LastIncludedIndex < 0 {
		args.PrevLogIndex = rf.snapshot.LastIncludedIndex
		args.PrevLogTerm = rf.snapshot.LastIncludedTerm
		args.Entries = args.Entries[rf.snapshot.LastIncludedIndex-args.PrevLogIndex:]
	}
	if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex-rf.snapshot.LastIncludedIndex].Term != args.PrevLogTerm {
		//fmt.Print("server ", rf.me, " ", rf.logs[args.PrevLogIndex-rf.snapshot.LastIncludedIndex].Term, " ", args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = args.PrevLogIndex
		rf.electionTimer.Reset(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
		return
	}
	if args.Entries == nil {
		//fmt.Println("server ", rf.me, " receive heartbeat from ", args.LeaderId, " term ", args.Term)
	}
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < rf.logs[len(rf.logs)-1].Logindex {
		rf.logs = rf.logs[:args.PrevLogIndex-rf.snapshot.LastIncludedIndex+1]
		//fmt.Println("server ", rf.me, " trim ", rf.committedIndex, len(rf.logs))

	}
	rf.logs = append(rf.logs, args.Entries...)
	//fmt.Println("server ", rf.me, " add ", rf.committedIndex, len(rf.logs))
	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = args.LeaderCommit
		//fmt.Println("server ", rf.me, " commit ", rf.committedIndex, len(rf.logs))
		select {
		case rf.waitapply <- struct{}{}:
		default:
		}

	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
	rf.persist()
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
		index = len(rf.logs) + rf.snapshot.LastIncludedIndex
		term = rf.currentTerm
		isLeader = true
		rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Logindex: index, Command: command})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
		currentTerm := rf.currentTerm
		currentCommit := rf.committedIndex
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.sendAndHandleAppendEntries(i, currentTerm, currentCommit)
			}
		}

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
	rf.electionTimeout = time.Duration(300) * time.Millisecond
	rf.electionTimer = time.NewTicker(rf.electionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
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
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	rf.persist()
	// start ticker goroutine to start elections
	rf.ticker()
	go rf.applyLog(applyCh)
	//go rf.printIndex()
	return rf
}

func (rf *Raft) applyLog(applyCh chan ApplyMsg) {
	for {
		<-rf.waitapply
		if rf.killed() {
			continue
		}

		// 获取锁并做一些基本操作
		rf.mu.Lock()
		applyIndex := rf.applyIndex
		committedIndex := rf.committedIndex
		snapshot := rf.snapshot
		//logs := rf.logs
		rf.mu.Unlock()

		// 如果当前没有日志需要应用，直接跳过
		if applyIndex >= committedIndex {
			continue
		}

		// 先应用 snapshot（如果有）
		if applyIndex < snapshot.LastIncludedIndex {
			applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot.Data,
				SnapshotIndex: snapshot.LastIncludedIndex,
				SnapshotTerm:  snapshot.LastIncludedTerm,
			}
			//fmt.Println("server ", rf.me, " apply snapshot ", snapshot.Data, " index ", snapshot.LastIncludedIndex)
			rf.mu.Lock()
			rf.applyIndex = snapshot.LastIncludedIndex
			rf.mu.Unlock()
			continue
		}

		// 应用日志条目
		for i := applyIndex + 1; i <= committedIndex; i++ {
			// 获取日志条目（这部分是在没有锁的情况下执行的）
			// if i-snapshot.LastIncludedIndex >= len(logs) {
			// 	willapplyedindex = i - 1
			// 	break
			// }
			logIndex := i - snapshot.LastIncludedIndex
			rf.mu.Lock()
			command := rf.logs[logIndex].Command
			rf.mu.Unlock()
			// 发送日志条目到 applyCh
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: i,
			}
			//fmt.Println("server ", rf.me, " apply log ", command, " index ", logs[logIndex].Logindex)
		}

		// 更新 applyIndex 为 committedIndex
		rf.mu.Lock()
		rf.applyIndex = committedIndex
		rf.mu.Unlock()
	}
}

// 定时打印term和state和投票情况和LOG内容
func (rf *Raft) printIndex() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		fmt.Printf("server %d term %d state %s votedFor %d votedCount %d\n", rf.me, rf.currentTerm, rf.state, rf.votedFor, rf.votedCount)
		//打印计时器
		//fmt.Println("server ", rf.me, " electiontimer ", rf.electionTimer)
		// //打印applyid和commitid
		//fmt.Printf("server %d applyIndex %d commitIndex %d\n", rf.me, rf.applyIndex, rf.committedIndex)
		// // for i := 0; i < len(rf.logs); i++ {
		// // 	fmt.Printf("server %d logindex %d logterm %d logcommand %v\n", rf.me, rf.logs[i].Logindex, rf.logs[i].Term, rf.logs[i].Command)
		// // }
		// //如果是leader打印nextindex和matchindex和log[0]的index
		// if rf.state == "leader" {
		// 	for i := 0; i < len(rf.peers); i++ {
		// 		fmt.Printf("server %d nextIndex %d matchIndex %d\n", i, rf.nextIndex[i], rf.matchIndex[i])
		// 	}
		// 	fmt.Println("server ", rf.me, " logindex ", rf.logs[0].Logindex)
		// }

		rf.mu.Unlock()
		time.Sleep(200 * time.Millisecond)

	}
}
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}
