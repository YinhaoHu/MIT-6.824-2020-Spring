package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/util"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type CommandType interface{}

type SnapshotData any

type LogEntry struct {
	Term    int
	Command CommandType
}

// ApplyMsg
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      CommandType
	CommandIndex int

	SnapshotValid bool
	SnapshotData  SnapshotData

	IsLastIndex   bool
	RaftStateSize int
}

const (
	NullVotedFor    = -1
	ElectionTimeMin = 350 * time.Millisecond
	ElectionTimeMax = 500 * time.Millisecond
	// HeartBeatRate is restricted that the leader send heartbeat RPCs no more than ten times per second.
	HeartBeatRate = 100 * time.Millisecond

	debugShowLog = true
)

type peerState int32

const (
	FollowerState peerState = iota
	CandidateState
	LeaderState
)

func (rf *Raft) generateRandomizedElectionTime() time.Duration {
	msCount := ElectionTimeMin.Milliseconds() + rand.Int63n(ElectionTimeMax.Milliseconds()-ElectionTimeMin.Milliseconds())
	result := time.Duration(msCount) * time.Millisecond
	return result
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              SnapshotData
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	leaderID  int                 // For client to redirect quickly

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states on all servers (Updated on stable Storage before responding to RPCs)
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry // Valid index starts with 1, Log[0] is invalid and should be initialized!
	// Volatile state on all servers:
	commitIndex int // Logical Index
	lastApplied int // Logical Index

	// Volatile state on leaders: (Reinitialized after election)
	nextIndex        []int // Logical Index
	matchIndex       []int // Logical Index
	snapshotPoint    []int // Logic indices of the lastIncludedIndex for each peer.
	snapshotArg      InstallSnapshotArgs
	snapshotChannel  chan Snapshot
	snapshot         Snapshot
	snapshotDoneCh   chan int // Notify the upper layer that the snapshot is done and broadcast-ed.
	snapshotDisabled bool     // Upper layer disabled the snapshot or not.

	applyChannel       chan ApplyMsg
	startNotifications []chan int
	//Leader status which tracks number of replications on the servers.
	//Access with physical index.
	replicaCount      [][]int
	state             peerState
	electionTimeout   time.Duration
	lastHeartBeatTime time.Time
	lastVoteTime      time.Time
}

// lock the internal mutex with ID returned for trace bug.
func (rf *Raft) lock() int64 {
	lockID := time.Now().UnixNano()
	if debugShowLog {
		_, _, line, _ := runtime.Caller(1)
		util.ShardKVDebugLog("raft server(%v) try to acquire lock %v in line %v.", rf.me, lockID, line)
		rf.mu.Lock()
		util.ShardKVDebugLog("raft server(%v) acquired the lock %v in line %v.", rf.me, lockID, line)
	} else {
		rf.mu.Lock()
	}
	return lockID
}

// unlock the internal mutex.
func (rf *Raft) unlock(lockID int64) {
	if debugShowLog {
		_, _, line, _ := runtime.Caller(1)
		rf.mu.Unlock()
		util.ShardKVDebugLog("raft server(%v) released the lock %v in line %v.", rf.me, lockID, line)
	} else {
		rf.mu.Unlock()
	}
}

// thread safety should be guaranteed by caller.
func (rf *Raft) physicalIndex(logicIndex int) int {
	returnedIndex := logicIndex - rf.snapshot.LastIncludedIndex
	if returnedIndex < 0 {
		_, path, line, _ := runtime.Caller(1)
		util.KVRaftDebugLog("Raft.physicalIndex - warning:[server %v] logicIndex=%v,returnedIndex=%v. Caller: %v %v",
			rf.me, logicIndex,
			returnedIndex, path, line)
	}
	return returnedIndex
}

// thread safety should be guaranteed by caller.
func (rf *Raft) logicalIndex(physicalIndex int) int {
	returnedIndex := physicalIndex + rf.snapshot.LastIncludedIndex
	if returnedIndex < 0 {
		_, path, line, _ := runtime.Caller(1)
		util.KVRaftDebugLog("Raft.logicalIndex - warning:[server %v] physicalIndex=%v,returnedIndex=%v. Caller: %v %v", rf.me,
			physicalIndex,
			returnedIndex, path, line)
	}
	return returnedIndex
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int // Logical Index
	LastIncludedTerm  int
	Data              SnapshotData
	// Fields `Done` and `Offset` in the paper are ignored.
}

type InstallSnapshotReply struct {
	Term int //currentTerm, for leader to update itself
}

// broadcasts the snapshot to all followers. Thread safety should be guaranteed by caller.
func (rf *Raft) broadCastSnapshot() {
	if rf.snapshotDisabled {
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.snapshotArg.Term,
		LeaderID:          rf.snapshotArg.LeaderID,
		LastIncludedIndex: rf.snapshotArg.LastIncludedIndex,
		LastIncludedTerm:  rf.snapshotArg.LastIncludedTerm,
		Data:              rf.snapshot.Data,
	}
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			go func(server int) {
				for !rf.killed() {
					var reply InstallSnapshotReply
					ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
					util.RaftDebugLog("Leader %v received %+v for message in broadcastSnapshot.", rf.me, reply)
					if ok {
						lockID := rf.lock()
						if args.Term > rf.CurrentTerm && rf.state == LeaderState {
							rf.convertToFollower()
							rf.persist()
							util.RaftDebugLog("leader %v convert to follower", rf.me)
						} else if rf.nextIndex[server] < args.LastIncludedIndex {
							rf.nextIndex[server] = args.LastIncludedIndex + 1
							rf.matchIndex[server] = args.LastIncludedIndex
						}
						if rf.snapshotPoint[server] < args.LastIncludedIndex {
							rf.snapshotPoint[server] = args.LastIncludedIndex
						}
						util.RaftDebugLog("Leader %v updated rf.snapshotPoint[%v]=%v", rf.me, server, args.LastIncludedIndex)
						rf.unlock(lockID)
						break
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.snapshotDisabled {
		return
	}
	lockID := rf.lock()
	util.RaftDebugLog("server %v received InstallSnapshot(args=%+v) \nCurrentLog[LLL=%v,LPL=%v]:%v", rf.me, *args,
		rf.logicalIndex(len(rf.Log)), len(rf.Log),
		rf.Log)
	reply.Term = rf.CurrentTerm
	if args.Term >= rf.CurrentTerm && rf.state == LeaderState {
		rf.convertToFollower()
		rf.unlock(lockID)
		return
	}
	if args.Term < rf.CurrentTerm {
		rf.unlock(lockID)
		return
	}
	rf.lastHeartBeatTime = time.Now()
	// If RPC request or response contains term T > CurrentTerm:
	// set CurrentTerm = T, convert to follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.persist()
		if rf.state != FollowerState {
			rf.convertToFollower()
		}
	}
	if rf.snapshot.LastIncludedIndex < args.LastIncludedIndex {
		if len(rf.Log) > rf.physicalIndex(args.LastIncludedIndex)+1 {
			rf.Log = append(rf.Log[:1], rf.Log[rf.physicalIndex(args.LastIncludedIndex)+1:]...)
		} else {
			rf.Log = rf.Log[:1]
		}
		rf.Log[0].Term = args.LastIncludedTerm
		rf.snapshot.Data = args.Data
		rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
		rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
		rf.snapshotArg = *args
		if rf.commitIndex < args.LastIncludedIndex {
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = rf.commitIndex
			rf.applyChannel <- ApplyMsg{
				SnapshotValid: true,
				SnapshotData:  args.Data,
			}
		}
		rf.persistStateAndSnapshot()
	}
	util.RaftDebugLog("server %v finished InstallSnapshot(snapshot=%+v,reply=%+v)\nCurrentLog[LLL=%v,LPL=%v]=%v,snapshotArg=%+v",
		rf.me, rf.snapshot,
		*reply, rf.logicalIndex(len(rf.Log)), len(rf.Log), rf.Log, rf.snapshotArg)
	rf.unlock(lockID)
}

// MakeSnapshot makes snapshot when server needs.
func (rf *Raft) makeSnapshot(snapshot *Snapshot) bool {
	util.RaftDebugLog("Raft.makeSnapshot: server %v try to make snapshot", rf.me)
	if rf.state != LeaderState {
		util.RaftDebugLog("Raft.MakeSnapshot: server %v did not make snapshot ",
			rf.me)
		return false
	}
	util.RaftDebugLog("Raft.makeSnapshot: before log[LLL=%v,LPL=%v,commitIndex=%v] = %v", rf.logicalIndex(len(rf.Log)),
		len(rf.Log), rf.commitIndex, rf.Log)
	snapshot.LastIncludedTerm = rf.Log[rf.physicalIndex(snapshot.LastIncludedIndex)].Term
	rf.Log = append(rf.Log[0:1], rf.Log[rf.physicalIndex(snapshot.LastIncludedIndex)+1:]...)
	rf.replicaCount = append(rf.replicaCount[0:1], rf.replicaCount[rf.physicalIndex(snapshot.LastIncludedIndex)+1:]...)
	rf.snapshot = *snapshot
	rf.Log[0].Term = rf.snapshot.LastIncludedTerm
	rf.persistStateAndSnapshot()
	util.RaftDebugLog("Raft.MakeSnapshot: server %v made snapshot[LLL=%v,LPL=%v](%+v)\n Current Log: %+v", rf.me,
		len(rf.Log)+rf.snapshot.LastIncludedIndex, len(rf.Log), rf.snapshot, rf.Log)
	rf.snapshotArg = InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.snapshot.LastIncludedIndex,
		LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
		Data:              snapshot.Data,
	}
	return true
}

// GetSnapshot sends Storage snapshot to the upper level when raft module starts.
func (rf *Raft) GetSnapshot() (SnapshotData, bool) {
	if rf.snapshot.LastIncludedIndex == 0 {
		var snapshotData SnapshotData
		return snapshotData, false
	}
	return rf.snapshot.Data, true
}

func (rf *Raft) readSnapshot() {
	buffer := bytes.NewBuffer(rf.persister.ReadSnapshot())
	decoder := labgob.NewDecoder(buffer)
	err := decoder.Decode(&rf.snapshot)
	if err != nil {
		if err.Error() != "EOF" {
			panic("Raft.GetSnapshot decode error")
		} else {
			return
		}
	}
	rf.Log[0].Term = rf.snapshot.LastIncludedTerm
	rf.commitIndex = rf.snapshot.LastIncludedIndex
	rf.lastApplied = rf.snapshot.LastIncludedIndex
	rf.snapshotArg = InstallSnapshotArgs{rf.CurrentTerm, rf.me, rf.snapshot.LastIncludedIndex,
		rf.snapshot.LastIncludedTerm, rf.snapshot.Data}
	util.KVRaftDebugLog("server %v reported snapshot. Snapshot:%v", rf.me, rf.snapshot)
}

func (rf *Raft) InitializeSnapshot(workCh chan Snapshot, doneCh chan int) {
	rf.snapshotDisabled = false
	rf.snapshotChannel = workCh
	rf.snapshotDoneCh = doneCh
	go func() {
		for !rf.killed() {
			snapshot := <-rf.snapshotChannel
			lockID := rf.lock()
			if rf.snapshot.LastIncludedIndex >= snapshot.LastIncludedIndex {
				rf.snapshotDoneCh <- 1
			} else {
				made := rf.makeSnapshot(&snapshot)
				rf.snapshotDoneCh <- 1
				if made {
					rf.broadCastSnapshot()
				}
			}
			rf.unlock(lockID)
		}
	}()
}

// GetState return CurrentTerm and whether this server
// believes it is the leader. THREAD-SAFE
func (rf *Raft) GetState() (int, bool) {
	lockID := rf.lock()
	defer rf.unlock(lockID)
	// util.RaftDebugLog("check server %v [CurrentTerm=%v,isLeader=%v]", rf.me, rf.CurrentTerm, rf.state == LeaderState)
	return rf.CurrentTerm, rf.state == LeaderState
}

// Kill
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
	util.RaftDebugLog("server %v is killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) persistStateAndSnapshot() {
	stateBuffer := new(bytes.Buffer)
	stateEncoder := labgob.NewEncoder(stateBuffer)
	err := stateEncoder.Encode(rf.CurrentTerm)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	err = stateEncoder.Encode(rf.VotedFor)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	err = stateEncoder.Encode(rf.Log)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	stateData := stateBuffer.Bytes()

	snapshotBuffer := new(bytes.Buffer)
	snapshotEncoder := labgob.NewEncoder(snapshotBuffer)
	err = snapshotEncoder.Encode(rf.snapshot)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	snapshotData := snapshotBuffer.Bytes()
	rf.persister.SaveStateAndSnapshot(stateData, snapshotData)
}

// save Raft's persistent state to stable Storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Store : 1)log, 2)current term and 3)voted for.
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	err := encoder.Encode(rf.CurrentTerm)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	err = encoder.Encode(rf.VotedFor)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	err = encoder.Encode(rf.Log)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int
	var votedFor int
	var logentries []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logentries) != nil {
		log.Fatalf("error in read from persister")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = logentries
	}
}

type AppendEntriesArgs struct {
	Term          int
	LeaderID      int
	PrevLogIndex  int // Logic Index
	PrevLogTerm   int
	Entries       []LogEntry
	LeaderCommit  int // Logic Index
	SnapshotPoint int // Logic index of the snapshot.lastIncludedIndex to protect stale AppendEntriesArgs
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	XTerm       int // the term of the conflicting entry
	XIndex      int // the first LOGICAL index it stores for that term
	XLen        int // the length of the log entries in term XTerm
	SnapshotLag bool
}

// The first return value indicates whether there is a conflict. The second return value
// indicates the number of common entries in `entries` and rf.Log. Note that two entries are
// common entries in the two log slice only if they have the same term. Command is not considered here.
func (rf *Raft) hasConflict(beginPhysicalIndex int, entries *[]LogEntry) (bool, int) {
	end := util.MinInt(beginPhysicalIndex+len(*entries), len(rf.Log))
	entryIndex := 0
	for index := beginPhysicalIndex; index < end; index++ {
		lhs := fmt.Sprintf("%v", (*entries)[entryIndex])
		rhs := fmt.Sprintf("%v", rf.Log[index])
		if lhs != rhs {
			return true, entryIndex
		}
		entryIndex++
	}
	return false, entryIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	lockID := rf.lock()
	util.RaftDebugLog("server %v receive entry from leader %v [current term %v] : %+v", rf.me,
		args.LeaderID, rf.CurrentTerm, *args)
	if !rf.snapshotDisabled && args.SnapshotPoint != rf.snapshot.LastIncludedIndex {
		// Leader can implicitly handle this case by compare two snapshot last included indices.
		reply.Success = false
		if args.SnapshotPoint > rf.snapshot.LastIncludedIndex {
			reply.SnapshotLag = true
		}
		rf.unlock(lockID)
		return
	}
	reply.Success = true
	reply.Term = rf.CurrentTerm
	// Avoid two leaders in the same term.
	// Think in a 3 peers cluster: s1 vote s2, s2 vote s3, s3 vote s1.
	if args.Term >= rf.CurrentTerm && rf.state == LeaderState {
		reply.Success = false
		rf.convertToFollower()
		util.RaftDebugLog("leader %v convert to follower", rf.me)
		util.RaftDebugLog("server %v reply %v with log(LLL = %v,LPL = %v) : %v \n for message %v", rf.me,
			*reply, rf.logicalIndex(len(rf.Log)), len(rf.Log), rf.Log, *args)
		rf.unlock(lockID)
		return
	}
	util.Assert(!(args.Term >= rf.CurrentTerm && rf.state == LeaderState), "why is ok")
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		util.RaftDebugLog("server %v reply %v with log(LLL = %v,LPL = %v) : %v \n for message %v", rf.me,
			*reply, rf.logicalIndex(len(rf.Log)), len(rf.Log), rf.Log, *args)
		rf.unlock(lockID)
		return
	}
	rf.lastHeartBeatTime = time.Now()
	// If RPC request or response contains term T > CurrentTerm:
	// set CurrentTerm = T, convert to follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.persist()
		if rf.state != FollowerState {
			rf.convertToFollower()
		}
		util.RaftDebugLog("server %v reply %v with log(LLL = %v,LPL = %v) : %v \n for message %v", rf.me,
			*reply, rf.logicalIndex(len(rf.Log)), len(rf.Log), rf.Log, *args)
		rf.unlock(lockID)
		return
	}
	// Now, we append entries to the log.
	lastLogPhysicalIndex := len(rf.Log) - 1
	if rf.physicalIndex(args.PrevLogIndex) > lastLogPhysicalIndex {
		// There is a gap between this Log and the leader's, return false.
		reply.Success = false
		// Fast backup[follower action - point 0]
		reply.XTerm = rf.Log[lastLogPhysicalIndex].Term
		reply.XIndex = rf.logicalIndex(rf.firstIndexOfTermByIndex(lastLogPhysicalIndex))
		reply.XLen = rf.logicalIndex(lastLogPhysicalIndex) - reply.XIndex + 1
		util.RaftDebugLog("server %v return false for AppendEntries", rf.me)
	} else if rf.Log[rf.physicalIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		// Fast backup[follower action - point 1]
		reply.XTerm = rf.Log[rf.physicalIndex(args.PrevLogIndex)].Term
		reply.XIndex = rf.logicalIndex(rf.firstIndexOfTermByIndex(rf.physicalIndex(args.PrevLogIndex)))
		reply.XLen = args.PrevLogIndex - reply.XIndex + 1
		util.RaftDebugLog("server %v return false for AppendEntries", rf.me)
	} else if len(args.Entries) == 0 {
		reply.Success = true
	} else if lastLogPhysicalIndex > rf.physicalIndex(args.PrevLogIndex) {
		conflict, nCommon := rf.hasConflict(rf.physicalIndex(args.PrevLogIndex+1), &args.Entries)
		if conflict {
			rf.Log = rf.Log[:rf.physicalIndex(args.PrevLogIndex+nCommon+1)]
			reply.Success = false
			// Fast backup[follower action - point 2]
			reply.XTerm = rf.Log[rf.physicalIndex(args.PrevLogIndex+nCommon)].Term
			reply.XIndex = rf.logicalIndex(rf.firstIndexOfTermByIndex(rf.physicalIndex(args.PrevLogIndex + nCommon)))
			reply.XLen = args.PrevLogIndex - reply.XIndex + 1
			util.RaftDebugLog("server %v return false for AppendEntries", rf.me)
		} else if nCommon == len(args.Entries) {
			util.Assert(nCommon > 0, "nCommon has to be greater than 0, but nCommon=%v", nCommon)
			// This Log entries is already appended. Received this due to redundant sending.
			/* TODO: make the log entry comparable in the future
			util.Assert(rf.Log[rf.physicalIndex(args.PrevLogIndex+1)] == args.Entries[0],
				"Log entry does not match (follower %v)%v ("+
					"leader %v)%v",
				rf.me, rf.Log[rf.physicalIndex(args.PrevLogIndex+1)], args.LeaderID, args.Entries[0])
			reply.Success = true*/
		} else {
			util.Assert(nCommon > 0, "nCommon has to be greater than 0, but nCommon=%v", nCommon)
			reply.Success = true
			rf.Log = append(rf.Log, args.Entries[nCommon:]...)
			rf.persist()
		}
	} else if lastLogPhysicalIndex == rf.physicalIndex(args.PrevLogIndex) {
		util.RaftDebugLog("server %v AppendEntries case: %v", rf.me, "lastLogPhysicalIndex == rf.physicalIndex(args.PrevLogIndex) ")
		rf.Log = append(rf.Log, args.Entries...)
		rf.persist()
		reply.Success = true
	} else {
		util.Assert(false, "unexpected region")
	}
	if args.LeaderCommit > rf.commitIndex && reply.Success {
		wantCommitLogicalIndex := util.MinInt(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if wantCommitLogicalIndex > rf.lastApplied {
			rf.commit(wantCommitLogicalIndex)
		}
	}
	util.RaftDebugLog("server %v reply %v with log(LLL=%v,LPL=%v) : %v \n for message %v", rf.me,
		*reply, rf.logicalIndex(len(rf.Log)), len(rf.Log), rf.Log, *args)
	rf.unlock(lockID)
	return
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	lockID := rf.lock()
	util.RaftDebugLog("server %v votes[current term %v, last Log index %v]. args %+v", rf.me, rf.CurrentTerm, len(rf.Log)-1, args)
	// If RPC request contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateID
		rf.persist()
		if rf.state != FollowerState {
			rf.convertToFollower()
		}
	}
	lastLogTerm := rf.Log[len(rf.Log)-1].Term
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		rf.unlock(lockID)
		return
	} else if (rf.VotedFor == args.CandidateID) &&
		((lastLogTerm < args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && rf.logicalIndex(len(rf.Log)-1) <= args.
			LastLogIndex)) {
		reply.VoteGranted = true
		rf.lastVoteTime = time.Now()
		rf.unlock(lockID)
		return
	}
	rf.unlock(lockID)
	return
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
// handler function on the server side does not return.  Thus, there
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

// ticker is a goroutine which starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(rf.electionTimeout)
		lockID := rf.lock()
		currentTime := time.Now()
		if ((currentTime.Sub(rf.lastHeartBeatTime)) > rf.electionTimeout &&
			(currentTime.Sub(rf.lastVoteTime)) > rf.electionTimeout) && rf.state != LeaderState {
			rf.electionTimeout = rf.generateRandomizedElectionTime()
			go rf.convertToCandidate()
		}
		rf.unlock(lockID)
	}
}

// generateVote generates a vote which is guaranteed to be other peer with the assumption
// that the total servers in the raft cluster are more than one.
func (rf *Raft) generateVote() int {
	voteFor := rand.Int() % len(rf.peers)
	if voteFor == rf.me {
		if rf.me == len(rf.peers)-1 {
			voteFor--
		} else if rf.me == 0 {
			voteFor++
		}
	}
	return 1
}

// convertToCandidate converts the follower to candidate state and start election.
func (rf *Raft) convertToCandidate() {
	startTime := time.Now()
	lockID := rf.lock()
	lastHeartBeatTime := rf.lastHeartBeatTime
	atomic.StoreInt32((*int32)(unsafe.Pointer(&rf.state)), int32(CandidateState))
	rf.CurrentTerm++
	rf.VotedFor = rf.generateVote()
	rf.persist()
	voteCount := int32(1)
	leastNeededVotes := (int32)((len(rf.peers) / 2) + 1)
	lastLogPhysicalIndex := len(rf.Log) - 1
	meetMaxTerm := int32(0)
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, rf.logicalIndex(lastLogPhysicalIndex), rf.Log[lastLogPhysicalIndex].Term}
	CurrentTerm := rf.CurrentTerm
	util.RaftDebugLog("server %v starts a election[current term %v]", rf.me, CurrentTerm)
	rf.unlock(lockID)
	condMutex := sync.Mutex{}
	cond := sync.NewCond(&condMutex)
	nFinishedGoRoutines := int32(0)
	// Phase 1 : Request vote.
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go func(thisServer int) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(thisServer, &args, &reply) {
					if reply.VoteGranted {
						atomic.AddInt32(&voteCount, 1)
					}
					if int32(reply.Term) > atomic.LoadInt32(&meetMaxTerm) {
						atomic.StoreInt32(&meetMaxTerm, int32(reply.Term))
					}
				}
				atomic.AddInt32(&nFinishedGoRoutines, 1)
				cond.Broadcast()
			}(server)
		}
	}
	// If this candidate did not get enough votes or there is one rpc is not done, we wait.
	condMutex.Lock()
	for atomic.LoadInt32(&voteCount) < leastNeededVotes && atomic.LoadInt32(&nFinishedGoRoutines) < int32(len(rf.
		peers)-1) {
		cond.Wait()
	}
	condMutex.Unlock()
	// Phase 2: Check the fate of this election.
	lockID = rf.lock()
	// If the peer is already in the election of a new term, this election should be invalidated.
	if rf.CurrentTerm > CurrentTerm {
		util.RaftDebugLog("server %v time out, new election started.[term %v,vote (%v/%v)] - elapsed %v ms", rf.me, CurrentTerm,
			atomic.LoadInt32(&voteCount), leastNeededVotes, time.Now().Sub(startTime).Milliseconds())
		rf.unlock(lockID)
		return
	}
	// Rule: All Servers - 2
	if int(atomic.LoadInt32(&meetMaxTerm)) > CurrentTerm {
		util.RaftDebugLog("server %v finished election: convert to follower because of greater term met", rf.me)
		rf.CurrentTerm = int(atomic.LoadInt32(&meetMaxTerm))
		rf.persist()
		rf.convertToFollower()
		rf.unlock(lockID)
		return
	}
	// Rule for candidate
	if rf.lastHeartBeatTime.After(lastHeartBeatTime) {
		// New leader occurs.
		util.RaftDebugLog("server %v finished election: convert to follower because of new leader[current term %v]", rf.me,
			rf.CurrentTerm)
		rf.convertToFollower()
		rf.unlock(lockID)
		return
	}
	// Rule for candidate
	if atomic.LoadInt32(&voteCount) >= leastNeededVotes {
		util.RaftDebugLog("server %v finished election: convert to leader[current term %v]", rf.me, CurrentTerm)
		rf.unlock(lockID)
		rf.convertToLeader()
		return
	}
	util.RaftDebugLog("server %v election time out, election again[current term %v | votesCount=%v,needVotes=%v]", rf.me,
		CurrentTerm, voteCount, leastNeededVotes)
	rf.unlock(lockID)
	// election time out, starts a new election
	return
}

// convertToLeader converts the candidate peer to leader state.
func (rf *Raft) convertToLeader() {
	lockID := rf.lock()
	atomic.StoreInt32((*int32)(unsafe.Pointer(&rf.state)), int32(LeaderState))
	nPeers := len(rf.peers)
	for peer := 0; peer < nPeers; peer++ {
		rf.nextIndex[peer] = rf.logicalIndex(len(rf.Log))
		rf.matchIndex[peer] = 0
		rf.replicaCount = make([][]int, len(rf.Log))
		for i := 0; i < rf.physicalIndex(rf.commitIndex+1); i++ {
			rf.replicaCount[i] = make([]int, nPeers)
		}
		for i := rf.physicalIndex(rf.commitIndex + 1); i < len(rf.replicaCount); i++ {
			rf.replicaCount[i] = make([]int, nPeers)
			rf.replicaCount[i][rf.me] = 1
		}
	}
	util.RaftDebugLog("server %v becomes leader in term %v with Log(LLL=%v,LPL=%v) %v and commit index %v", rf.me,
		rf.CurrentTerm, rf.logicalIndex(len(rf.Log)),
		len(rf.Log), rf.Log,
		rf.commitIndex)
	rf.snapshotArg.LeaderID = rf.me
	rf.snapshotArg.Term = rf.CurrentTerm
	rf.broadCastSnapshot()
	rf.unlock(lockID)
	done := int32(0)
	// Send initial heart beat message upon election is done immediate and later
	// send the heart beat message periodically.
	for server := 0; server < nPeers; server++ {
		if server != rf.me {
			go func(server int) {
				for !rf.killed() && atomic.LoadInt32(&done) == 0 {
					lockID := rf.lock()
					if rf.state != LeaderState {
						rf.unlock(lockID)
						break
					}
					if rf.snapshotPoint[server] < rf.snapshot.LastIncludedIndex && !rf.snapshotDisabled {
						util.RaftDebugLog("Leader %v: state=%v,snapshotPoint[%v]=%v,snapshot.LastIncludedIndex=%v", rf.me, rf.state, server, rf.snapshotPoint[server], rf.snapshot.LastIncludedIndex)
						rf.unlock(lockID)
						time.Sleep(HeartBeatRate)
						continue
					}
					args := AppendEntriesArgs{Term: rf.CurrentTerm, LeaderID: rf.me, PrevLogIndex: 0,
						PrevLogTerm: 0,
						Entries:     make([]LogEntry, 0), LeaderCommit: rf.commitIndex, SnapshotPoint: rf.snapshot.LastIncludedIndex}
					// Ensure the correctness of next index.
					if rf.nextIndex[server] > rf.logicalIndex(len(rf.Log)) {
						rf.nextIndex[server] = rf.logicalIndex(len(rf.Log))
					} else if rf.nextIndex[server] == rf.logicalIndex(0) {
						rf.nextIndex[server] = rf.logicalIndex(1)
					}
					// Append entries to the args and set some fields.
					sentBeginEntryPhysicalIndex := rf.physicalIndex(rf.nextIndex[server])
					if rf.logicalIndex(len(rf.Log)) > rf.nextIndex[server] {
						args.Entries = append(args.Entries, rf.Log[sentBeginEntryPhysicalIndex:len(rf.Log)]...)
					}
					args.PrevLogIndex = rf.logicalIndex(sentBeginEntryPhysicalIndex) - 1
					args.PrevLogTerm = rf.Log[rf.physicalIndex(args.PrevLogIndex)].Term
					reply := AppendEntriesReply{}
					firstRoundSnapshotLastIndex := rf.snapshot.LastIncludedIndex
					util.RaftDebugLog("leader made message %+v for server %v.", args, server)
					rf.unlock(lockID)
					// To avoid the blocked AppendEntries RPC affected the heart beat rate, I use the go routine.
					// It is safe to release the lock and re-acquire the lock here.
					go func() {
						util.RaftDebugLog("server %v sends AppendEntries to server %v. Message: %v",
							rf.me, server, args)
						rf.sendAppendEntries(server, &args, &reply)
						lockID := rf.lock()
						util.RaftDebugLog("leader %v received reply %+v in term %v", rf.me, reply, rf.CurrentTerm)
						// During the period of lock released, some inconsistency might occur, and we ignore to
						// handle them.
						// 1. snapshot was made.
						// 2. nextIndex[server] was modified.
						if reply.SnapshotLag || rf.state != LeaderState || rf.snapshot.LastIncludedIndex != firstRoundSnapshotLastIndex || rf.nextIndex[server] != args.PrevLogIndex+1 {
							rf.unlock(lockID)
							return
						}
						// Log size is modified by commitRangeCheck, stop.
						if reply.Success && len(args.Entries) > 0 {
							// This condition is used to ensure concurrency correct.
							for i := sentBeginEntryPhysicalIndex; i < (sentBeginEntryPhysicalIndex + len(args.Entries)); i++ {
								rf.replicaCount[i][server] = 1
							}
							rf.matchIndex[server] = rf.logicalIndex(sentBeginEntryPhysicalIndex + len(args.Entries) - 1)
							rf.nextIndex[server] = rf.logicalIndex(sentBeginEntryPhysicalIndex + len(args.Entries))
						} else if reply.Term > rf.CurrentTerm && rf.state == LeaderState {
							// If message is sent and this leader is stale, we convert it to follower. Multiple conversion
							// to follower is avoided by the condition 'state==LeaderState'.
							util.RaftDebugLog("leader %v convert to follower[current term %v]", rf.me, rf.CurrentTerm)
							atomic.StoreInt32(&done, 1)
							rf.CurrentTerm = reply.Term
							rf.persist()
							rf.convertToFollower()
							rf.unlock(lockID)
							return
						} else if reply.Term == rf.CurrentTerm && !reply.Success && rf.physicalIndex(rf.
							nextIndex[server]) == sentBeginEntryPhysicalIndex {
							// Synchronize the Log.
							util.RaftDebugLog("rf.snapshotPoint[%v]=%v,lastIncludedIndex=%v", server,
								rf.snapshotPoint[server], rf.snapshot.LastIncludedIndex)
							if rf.Log[rf.physicalIndex(reply.XIndex)].Term == reply.XTerm {
								var i int
								for i = rf.physicalIndex(reply.XIndex); i < len(rf.Log) && i < reply.XLen; i++ {
									if rf.Log[i].Term != reply.XTerm {
										break
									}
								}
								rf.nextIndex[server] = rf.logicalIndex(i)
								util.RaftDebugLog("leader %v set nextIndex[%v] = %v for case 1", rf.me, server, rf.nextIndex[server])
							} else {
								rf.nextIndex[server] = reply.XIndex
								util.RaftDebugLog("leader %v set nextIndex[%v] = %v for case 2", rf.me, server, rf.nextIndex[server])
							}
						}
						toCommitPhysicalIndex := sentBeginEntryPhysicalIndex + len(args.Entries) - 1
						if toCommitPhysicalIndex < len(rf.Log) && rf.Log[toCommitPhysicalIndex].Term == rf.CurrentTerm && rf.
							commitRangeCheck(toCommitPhysicalIndex) && toCommitPhysicalIndex > rf.physicalIndex(rf.
							commitIndex) {
							rf.commit(rf.logicalIndex(toCommitPhysicalIndex))
						}
						rf.unlock(lockID)
					}()
					select {
					case <-time.After(HeartBeatRate):
					case <-rf.startNotifications[server]:
					}
				}
			}(server)
		}
	}
}

// commitRangeCheck checks that if all Log entries whose index is in the range [commitIndex+1,toCommitIndex]
// can be committed. Return true if so. Otherwise, false.
func (rf *Raft) commitRangeCheck(toCommitPhysicalIndex int) bool {
	if toCommitPhysicalIndex == 0 || toCommitPhysicalIndex == len(rf.Log) {
		return false
	}
	util.Assert(toCommitPhysicalIndex > 0 && toCommitPhysicalIndex < len(rf.Log), "toCommitPhysicalIndex %v violates restriction(rf log length %v)",
		toCommitPhysicalIndex, len(rf.Log))
	majorityAllow := true
	for physicalIndex := rf.physicalIndex(rf.commitIndex + 1); physicalIndex <= toCommitPhysicalIndex; physicalIndex++ {
		if rf.Log[physicalIndex].Term < rf.CurrentTerm {
			continue
		}
		nReplicated := func() int {
			count := 0
			for _, v := range rf.replicaCount[physicalIndex] {
				count += v
			}
			return count
		}()
		if nReplicated < rf.getMajorityNumber() {
			majorityAllow = false
			break
		}
	}
	// If we want to commit a log entry whose term is less than current term, we delete it.
	return majorityAllow
}

// firstIndexOfTermByIndex returns the first `physical index` of the term 'log[index].Term'
// WARNING: holding rf.mu before calling this function.
func (rf *Raft) firstIndexOfTermByIndex(physicalIndex int) int {
	term := rf.Log[physicalIndex].Term
	for physicalIndex = physicalIndex - 1; physicalIndex > 0; physicalIndex-- {
		if rf.Log[physicalIndex].Term != term {
			break
		}
	}
	return physicalIndex + 1
}

// convertToFollower converts the peer to follower state and reset the election timeout.
// Note that this function does NOT acquire and release raft mutex.
func (rf *Raft) convertToFollower() {
	atomic.StoreInt32((*int32)(unsafe.Pointer(&rf.state)), int32(FollowerState))
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command CommandType) (int, int, bool) {
	if rf.killed() {
		util.RaftDebugLog("server %v is killed", rf.me)
		return -1, -1, false
	}
	lockID := rf.lock()
	index := len(rf.Log)
	term := rf.CurrentTerm
	if rf.state != LeaderState {
		rf.unlock(lockID)
		return -1, -1, false
	}
	newEntry := LogEntry{rf.CurrentTerm, command}
	logEntryNReplicated := make([]int, len(rf.peers))
	logEntryNReplicated[rf.me] = 1
	rf.replicaCount = append(rf.replicaCount, logEntryNReplicated)
	rf.Log = append(rf.Log, newEntry)
	rf.persist()
	util.RaftDebugLog("leader %v Log(LLL=%v,LPL=%v): %v", rf.me, rf.logicalIndex(len(rf.Log)), len(rf.Log), rf.Log)
	// Notify the leader goroutine to send AppendEntriesRPC immediately so that the agreement will be made
	// quickly.
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				select {
				case rf.startNotifications[server] <- 0:
				case <-time.After(time.Millisecond * 300):
				}
			}(i)
		}
	}
	rf.unlock(lockID)
	return index, term, true
}

func (rf *Raft) IsLeader() bool {
	return atomic.LoadInt32((*int32)(unsafe.Pointer(&rf.state))) == int32(LeaderState)
}

// Get the majority number in the raft cluster which is nPeers/2+1.
func (rf *Raft) getMajorityNumber() int {
	return len(rf.peers)/2 + 1
}

// commit the Log[index] entry. This function does NOT acquire and release the mutex internally!
func (rf *Raft) commit(logicalIndex int) {
	util.RaftDebugLog("server %v commit %v", rf.me, logicalIndex)
	if logicalIndex == 0 {
		return
	}
	rf.commitIndex = logicalIndex
	for rf.commitIndex > rf.lastApplied {
		commandIndex := rf.lastApplied + 1
		applyMsg := ApplyMsg{}
		applyMsg.Command = rf.Log[rf.physicalIndex(commandIndex)].Command
		applyMsg.CommandValid = true
		applyMsg.CommandIndex = commandIndex
		applyMsg.IsLastIndex = commandIndex == rf.logicalIndex(len(rf.Log)-1)
		applyMsg.RaftStateSize = rf.persister.RaftStateSize()
		rf.applyChannel <- applyMsg
		rf.lastApplied++
		util.RaftDebugLog("server %v apply %v", rf.me, applyMsg)
	}
	util.RaftDebugLog("server %v commits. Info: rf.commitIndex = %v, lastApply = %v", rf.me, rf.commitIndex,
		rf.lastApplied)
	util.Assert(rf.lastApplied == rf.commitIndex, "server %v last applied index(%v) != commit index(%v)",
		rf.me, rf.lastApplied, rf.commitIndex)
}

// Make
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
	rf := new(Raft)
	rf.snapshotDisabled = true
	rf.applyChannel = applyCh
	rf.peers = peers
	rf.startNotifications = make([]chan int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.startNotifications[i] = make(chan int)
	}
	rf.persister = persister
	rf.me = me
	rf.CurrentTerm = 1
	rf.VotedFor = NullVotedFor
	rf.Log = append(rf.Log, LogEntry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	nPeers := len(peers)
	rf.nextIndex = make([]int, nPeers)
	rf.matchIndex = make([]int, nPeers)
	rf.snapshotPoint = make([]int, nPeers)
	rf.replicaCount = make([][]int, 1)
	rf.state = FollowerState
	rf.electionTimeout = rf.generateRandomizedElectionTime()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot()
	util.RaftDebugLog("raft server %v is created(current term %v) with log %v", me, rf.CurrentTerm, rf.Log)
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
