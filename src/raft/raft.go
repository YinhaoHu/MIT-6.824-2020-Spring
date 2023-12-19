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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type LogEntry struct {
	Term    int
	Command interface{}
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
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	NullVotedFor    = -1
	ElectionTimeMin = 350 * time.Millisecond
	ElectionTimeMax = 500 * time.Millisecond
	// HeartBeatRate is restricted that the leader send heartbeat RPCs no more than ten times per second.
	HeartBeatRate = 100 * time.Millisecond
)

type peerState int

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

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states on all servers (Updated on stable storage before responding to RPCs)
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry // Valid index starts with 1, Log[0] is invalid and should be initialized!
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	applyChannel        chan ApplyMsg
	logEntryNReplicated []int // Leader status which track number of replications on the servers.
	state               peerState
	electionTimeout     time.Duration
	lastHeartBeatTime   time.Time
	lastVoteTime        time.Time
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DebugLog("check peer %v [CurrentTerm=%v,isLeader=%v]", rf.me, rf.CurrentTerm, rf.state == LeaderState)
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
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DebugLog("peer %v is killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	XTerm   int // the term of the conflicting entry
	XIndex  int // the first index it stores for that term
	XLen    int // the length of the log entries in term XTerm
}

// The first return value indicates whether there is a conflict. The second return value
// indicates the number of common entries in `entries` and rf.Log
func (rf *Raft) hasConflict(begin int, entries *[]LogEntry) (bool, int) {
	end := MinInt(begin+len(*entries), len(rf.Log))
	entryIndex := 0
	for index := begin; index < end; index++ {
		if rf.Log[index].Term != (*entries)[entryIndex].Term {
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
	rf.mu.Lock()
	DebugLog("peer %v receive entry from leader %v [current term %v] : %+v", rf.me,
		args.LeaderID, rf.CurrentTerm, *args)
	defer DebugLog("peer %v reply %v with log(%v entries) : %v", rf.me, reply, len(rf.Log), rf.Log)
	reply.Success = true
	reply.Term = rf.CurrentTerm
	// Avoid two leaders in the same term.
	// Think in a 3 peers cluster: s1 vote s2, s2 vote s3, s3 vote s1.
	if args.Term >= rf.CurrentTerm && rf.state == LeaderState {
		reply.Success = false
		rf.convertToFollower()
		DebugLog("leader %v convert to follower", rf.me)
		rf.mu.Unlock()
		return
	}
	Assert(!(args.Term >= rf.CurrentTerm && rf.state == LeaderState), "why is ok")
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		rf.mu.Unlock()
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
		rf.mu.Unlock()
		return
	}
	// Now, we append entries to the log.
	lastLogIndex := len(rf.Log) - 1
	if args.PrevLogIndex > lastLogIndex {
		// There is a gap between this Log and the leader's, return false.
		reply.Success = false
		// TODO(Hoo) : Fast backup[follower action - point 0]
		reply.XTerm = rf.Log[lastLogIndex].Term
		reply.XIndex = rf.firstIndexOfTermByIndex(lastLogIndex)
		reply.XLen = lastLogIndex - reply.XIndex + 1
		DebugLog("peer %v return false for AppendEntries", rf.me)
	} else if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		// TODO(Hoo) : Fast backup[follower action - point 1]
		reply.XTerm = rf.Log[args.PrevLogIndex].Term
		reply.XIndex = rf.firstIndexOfTermByIndex(args.PrevLogIndex)
		reply.XLen = args.PrevLogIndex - reply.XIndex + 1
		DebugLog("peer %v return false for AppendEntries", rf.me)
	} else if len(args.Entries) == 0 {
		reply.Success = true
	} else if lastLogIndex > args.PrevLogIndex {
		conflict, nCommon := rf.hasConflict(args.PrevLogIndex+1, &args.Entries)
		if conflict {
			rf.Log = rf.Log[:args.PrevLogIndex+nCommon+1]
			rf.persist()
			reply.Success = false
			// TODO(Hoo) : Fast backup[follower action - point 2]
			reply.XTerm = rf.Log[args.PrevLogIndex+nCommon].Term
			reply.XIndex = rf.firstIndexOfTermByIndex(args.PrevLogIndex + nCommon)
			reply.XLen = args.PrevLogIndex - reply.XIndex + 1
			DebugLog("peer %v return false for AppendEntries", rf.me)
		} else if nCommon == len(args.Entries) {
			Assert(nCommon > 0, "nCommon has to be greater than 0, but nCommon=%v", nCommon)
			// This Log entries is already appended. Received this due to redundant sending.
			Assert(rf.Log[args.PrevLogIndex+1] == args.Entries[0], "Log entry does not match (follower %v)%v ("+
				"leader %v)%v",
				rf.me, rf.Log[args.PrevLogIndex+1], args.LeaderID, args.Entries[0])
			reply.Success = true
		} else {
			Assert(nCommon > 0, "nCommon has to be greater than 0, but nCommon=%v", nCommon)
			reply.Success = true
			rf.Log = append(rf.Log, args.Entries[nCommon:]...)
			rf.persist()
		}
	} else if lastLogIndex == args.PrevLogIndex {
		rf.Log = append(rf.Log, args.Entries...)
		rf.persist()
		reply.Success = true
	} else {
		Assert(false, "unexpected region")
	}
	if args.LeaderCommit > rf.commitIndex && reply.Success {
		wantCommitIndex := MinInt(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if wantCommitIndex > rf.lastApplied {
			rf.commit(wantCommitIndex)
		}
	}
	rf.mu.Unlock()
	return
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// For lab 2A.
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
	rf.mu.Lock()
	DebugLog("peer %v votes[current term %v, last Log index %v]. args %+v", rf.me, rf.CurrentTerm, len(rf.Log)-1, args)
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
		rf.mu.Unlock()
		return
	} else if (rf.VotedFor == args.CandidateID) &&
		((lastLogTerm < args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && (len(rf.Log)-1) <= args.LastLogIndex)) {
		reply.VoteGranted = true
		rf.lastVoteTime = time.Now()
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
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
// handler function on the server side does not return.  Thus, there
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
		rf.mu.Lock()
		currentTime := time.Now()
		if ((currentTime.Sub(rf.lastHeartBeatTime)) > rf.electionTimeout &&
			(currentTime.Sub(rf.lastVoteTime)) > rf.electionTimeout) && rf.state != LeaderState {
			rf.electionTimeout = rf.generateRandomizedElectionTime()
			go rf.convertToCandidate()
		}
		rf.mu.Unlock()
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
	rf.mu.Lock()
	lastHeartBeatTime := rf.lastHeartBeatTime
	rf.state = CandidateState
	rf.CurrentTerm++
	rf.VotedFor = rf.generateVote()
	rf.persist()
	voteCount := int32(1)
	leastNeededVotes := (int32)((len(rf.peers) / 2) + 1)
	lastLogIndex := len(rf.Log) - 1
	meetMaxTerm := int32(0)
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, lastLogIndex, rf.Log[lastLogIndex].Term}
	CurrentTerm := rf.CurrentTerm
	DebugLog("peer %v starts a election[current term %v]", rf.me, CurrentTerm)
	rf.mu.Unlock()
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
	rf.mu.Lock()
	// If the peer is already in the election of a new term, this election should be invalidated.
	if rf.CurrentTerm > CurrentTerm {
		DebugLog("peer %v time out, new election started.[term %v,vote (%v/%v)] - elapsed %v ms", rf.me, CurrentTerm,
			atomic.LoadInt32(&voteCount), leastNeededVotes, time.Now().Sub(startTime).Milliseconds())
		rf.mu.Unlock()
		return
	}
	// Rule: All Servers - 2
	if int(atomic.LoadInt32(&meetMaxTerm)) > CurrentTerm {
		DebugLog("peer %v finished election: convert to follower because of greater term met", rf.me)
		rf.CurrentTerm = int(atomic.LoadInt32(&meetMaxTerm))
		rf.persist()
		rf.convertToFollower()
		rf.mu.Unlock()
		return
	}
	// Rule for candidate
	if rf.lastHeartBeatTime.After(lastHeartBeatTime) {
		// New leader occurs.
		DebugLog("peer %v finished election: convert to follower because of new leader[current term %v]", rf.me,
			rf.CurrentTerm)
		rf.convertToFollower()
		rf.mu.Unlock()
		return
	}
	// Rule for candidate
	if atomic.LoadInt32(&voteCount) >= leastNeededVotes {
		DebugLog("peer %v finished election: convert to leader[current term %v]", rf.me, CurrentTerm)
		rf.mu.Unlock()
		rf.convertToLeader()
		return
	}
	DebugLog("peer %v election time out, election again[current term %v | votesCount=%v,needVotes=%v]", rf.me,
		CurrentTerm, voteCount, leastNeededVotes)
	rf.mu.Unlock()
	// election time out, starts a new election
	return
}

// convertToLeader converts the candidate peer to leader state.
func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	rf.state = LeaderState
	nPeers := len(rf.peers)
	for peer := 0; peer < nPeers; peer++ {
		rf.nextIndex[peer] = len(rf.Log)
		rf.matchIndex[peer] = 0
		rf.logEntryNReplicated = make([]int, len(rf.Log))
		for i := rf.commitIndex + 1; i < len(rf.logEntryNReplicated); i++ {
			rf.logEntryNReplicated[i] = 1
		}
	}
	DebugLog("peer %v becomes leader with Log(%v entries) %v and commit index %v", rf.me, len(rf.Log), rf.Log,
		rf.commitIndex)
	rf.mu.Unlock()
	done := int32(0)
	// Send initial heart beat message upon election is done immediate and later
	// send the heart beat message periodically.
	nRunningGoroutines := int32(0)
	for server := 0; server < nPeers; server++ {
		if server != rf.me {
			go func(server int) {
				for !rf.killed() && atomic.LoadInt32(&done) == 0 {
					rf.mu.Lock()
					if rf.state != LeaderState {
						rf.mu.Unlock()
						break
					}
					args := AppendEntriesArgs{Term: rf.CurrentTerm, LeaderID: rf.me, PrevLogIndex: 0,
						PrevLogTerm: 0,
						Entries:     make([]LogEntry, 0), LeaderCommit: rf.commitIndex}
					if rf.nextIndex[server] > len(rf.Log) {
						rf.nextIndex[server] = len(rf.Log)
					} else if rf.nextIndex[server] == 0 {
						rf.nextIndex[server] = 1
					}
					sentBeginEntryIndex := rf.nextIndex[server]
					if len(rf.Log) > rf.nextIndex[server] {
						args.Entries = append(args.Entries, rf.Log[sentBeginEntryIndex:len(rf.Log)]...)
					}
					args.PrevLogIndex = sentBeginEntryIndex - 1
					args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					// To avoid the blocked AppendEntries RPC affected the heart beat rate, I use the go routine.
					// It is safe to release the lock and re-acquire the lock here.
					go func() {
						atomic.AddInt32(&nRunningGoroutines, 1)
						rf.sendAppendEntries(server, &args, &reply)
						rf.mu.Lock()
						// Log size is modified by commitRangeCheck, stop.
						if reply.Success && len(args.Entries) > 0 {
							// This condition is used to ensure concurrency correct.
							if rf.matchIndex[server] < sentBeginEntryIndex {
								for i := sentBeginEntryIndex; i < (sentBeginEntryIndex + len(args.Entries)); i++ {
									rf.logEntryNReplicated[i]++
								}
							}
							rf.matchIndex[server] = sentBeginEntryIndex + len(args.Entries) - 1
							rf.nextIndex[server] = sentBeginEntryIndex + len(args.Entries)
						} else if reply.Term > rf.CurrentTerm && rf.state == LeaderState {
							// If message is sent and this leader is stale, we convert it to follower. Multiple conversion
							// to follower is avoided by the condition 'state==LeaderState'.
							DebugLog("leader %v convert to follower[current term %v]", rf.me, rf.CurrentTerm)
							atomic.StoreInt32(&done, 1)
							rf.CurrentTerm = reply.Term
							rf.persist()
							rf.convertToFollower()
							rf.mu.Unlock()
							return
						} else if reply.Term == rf.CurrentTerm && !reply.Success && rf.nextIndex[server] == sentBeginEntryIndex {
							// Synchronize the Log.
							if rf.Log[reply.XIndex].Term == reply.XTerm {
								var i int
								for i = reply.XIndex; i < len(rf.Log) && i < reply.XLen; i++ {
									if rf.Log[i].Term != reply.XTerm {
										break
									}
								}
								rf.nextIndex[server] = i
								DebugLog("leader %v set nextIndex[%v] = %v for case 1", rf.me, server, rf.nextIndex[server])
							} else {
								rf.nextIndex[server] = reply.XIndex
								DebugLog("leader %v set nextIndex[%v] = %v for case 2", rf.me, server, rf.nextIndex[server])
							}
						}
						toCommitIndex := sentBeginEntryIndex + len(args.Entries) - 1
						if toCommitIndex < len(rf.Log) && rf.Log[toCommitIndex].Term == rf.CurrentTerm && rf.
							commitRangeCheck(toCommitIndex) && toCommitIndex > rf.commitIndex {
							rf.commit(toCommitIndex)
						}
						rf.mu.Unlock()
						atomic.AddInt32(&nRunningGoroutines, -1)
					}()
					rf.mu.Lock()
					isBusy := rf.nextIndex[server] < len(rf.Log)
					rf.mu.Unlock()
					// If there are a lot of Log entries to be synchronized, don't sleep so long.
					// But if there is a small period which the latency is a little high, we don't want
					// to have too many goroutines.
					if !isBusy || atomic.LoadInt32(&nRunningGoroutines) >= 2 {
						time.Sleep(HeartBeatRate)
					} else {
						// Wait for a while so that no many redundant Log entire to be sent.
						// Time is selected similarly randomly.
						time.Sleep(10 * time.Millisecond)
					}
				}
			}(server)
		}
	}
}

// commitRangeCheck checks that if all Log entries whose index is in the range [commitIndex+1,toCommitIndex]
// can be committed. Return true if so. Otherwise, false.
func (rf *Raft) commitRangeCheck(toCommitIndex int) bool {
	if toCommitIndex == 0 || toCommitIndex == len(rf.Log) {
		return false
	}
	Assert(toCommitIndex > 0 && toCommitIndex < len(rf.Log), "toCommitIndex %v violates restriction(rf log length %v)",
		toCommitIndex, len(rf.Log))
	majorityAllow := true
	for index := rf.commitIndex + 1; index <= toCommitIndex; index++ {
		if rf.Log[index].Term < rf.CurrentTerm {
			continue
		}
		if rf.logEntryNReplicated[index] < rf.getMajorityNumber() {
			majorityAllow = false
			break
		}
	}
	// If we want to commit a log entry whose term is less than current term, we delete it.
	return majorityAllow
}

// firstIndexOfTermByIndex returns the first index of the term 'log[index].Term'
// WARNING: holding rf.mu before calling this function.
func (rf *Raft) firstIndexOfTermByIndex(index int) int {
	term := rf.Log[index].Term
	for index = index - 1; index > 0; index-- {
		if rf.Log[index].Term != term {
			break
		}
	}
	return index + 1
}

// convertToFollower converts the peer to follower state and reset the election timeout.
// Note that this function does NOT acquire and release raft mutex.
func (rf *Raft) convertToFollower() {
	rf.state = FollowerState
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.Log)
	term := rf.CurrentTerm
	if rf.state != LeaderState {
		return -1, -1, false
	}
	newEntry := LogEntry{rf.CurrentTerm, command}
	rf.logEntryNReplicated = append(rf.logEntryNReplicated, 1)
	rf.Log = append(rf.Log, newEntry)
	rf.persist()
	DebugLog("leader %v Log(%v entries): %v", rf.me, len(rf.Log), rf.Log)
	return index, term, true
}

// Get the majority number in the raft cluster which is nPeers/2+1.
func (rf *Raft) getMajorityNumber() int {
	return len(rf.peers)/2 + 1
}

// commit the Log[index] entry. This function does NOT acquire and release the mutex internally!
func (rf *Raft) commit(index int) {
	if index == 0 {
		return
	}
	rf.commitIndex = index
	for rf.commitIndex > rf.lastApplied {
		commandIndex := rf.lastApplied + 1
		applyMsg := ApplyMsg{}
		applyMsg.Command = rf.Log[commandIndex].Command
		applyMsg.CommandValid = true
		applyMsg.CommandIndex = commandIndex
		rf.applyChannel <- applyMsg
		rf.lastApplied++
		DebugLog("server %v apply %v(%v)", rf.me, applyMsg.CommandIndex, applyMsg.Command)
	}
	Assert(rf.lastApplied == rf.commitIndex, "peer %v last applied index(%v) != commit index(%v)", rf.me, rf.lastApplied,
		rf.commitIndex)
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := new(Raft)
	rf.applyChannel = applyCh
	rf.peers = peers
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
	rf.logEntryNReplicated = make([]int, 1)
	rf.state = FollowerState
	rf.electionTimeout = rf.generateRandomizedElectionTime()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DebugLog("raft peer %v is created(current term %v) with log %v", me, rf.CurrentTerm, rf.Log)
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
