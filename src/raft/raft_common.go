package raft

import (
	"6.824/util"
	"runtime"
	"time"
)

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

// Get the majority number in the raft cluster which is nPeers/2+1.
func (rf *Raft) getMajorityNumber() int {
	return len(rf.peers)/2 + 1
}
