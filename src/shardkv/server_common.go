package shardkv

import (
	"6.824/util"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// lock the internal mutex with ID returned for trace bug.
func (kv *ShardKV) lock(locker *sync.Mutex) int64 {
	lockID := time.Now().UnixNano()
	if debugShowLog {
		_, _, line, _ := runtime.Caller(1)
		util.ShardKVDebugLog("server(%v,%v) try to acquire lock %v in line %v.", kv.gid, kv.me, lockID, line)
		locker.Lock()
		util.ShardKVDebugLog("server(%v,%v) acquired the lock %v in line %v.", kv.gid, kv.me, lockID, line)
	} else {
		locker.Lock()
	}
	return lockID
}

// unlock the internal mutex.
func (kv *ShardKV) unlock(locker *sync.Mutex, lockID int64) {
	if debugShowLog {
		_, _, line, _ := runtime.Caller(1)
		locker.Unlock()
		util.ShardKVDebugLog("server(%v,%v) released the lock %v in line %v.", kv.gid, kv.me, lockID, line)
	} else {
		locker.Unlock()
	}
}

func (kv *ShardKV) strState(state int32) string {
	res := ""
	switch state {
	case StateReady:
		res = "Ready"
	case StateUpdating:
		res = "Updating"
	case StateAborting:
		res = "Aborting"
	case StateRecovering:
		res = "Recovering"
	}
	return res
}

// setState atomically set the state of kv to 'state'.
func (kv *ShardKV) setState(state int32) {
	if debugShowLog {
		_, _, line, _ := runtime.Caller(1)
		util.ShardKVDebugLog("server(%v,%v) set state to be %v in line %v",
			kv.gid, kv.me, kv.strState(state), line)
	}
	atomic.StoreInt32(&kv.state, state)
}

// getState atomically get the state of kv.
func (kv *ShardKV) getState() int32 {
	return atomic.LoadInt32(&kv.state)
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	util.ShardKVDebugLog("server(%v,%v) is killed.", kv.gid, kv.me)
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

// killed return true if the kv server is killed.
func (kv *ShardKV) killed() bool { return atomic.LoadInt32(&kv.dead) == 1 }
