package kvraft

import (
	"6.824/labrpc"
	"6.824/util"
	"sync"
	"sync/atomic"
	"time"
)

var nClerks int32

type Clerk struct {
	servers []*labrpc.ClientEnd
	id      int32

	mu     sync.Mutex
	leader int // The server id of the leader
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.id = nClerks
	ck.mu = sync.Mutex{}

	nClerks++
	return ck
}

const waitingTimeout = 300 * time.Millisecond

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	arg := GetArgs{key, ck.id, time.Now().UnixNano()}
	nServers := len(ck.servers)
	value := ""
	mainLoopCh := make(chan int) // 1 : continue, 0: end
	for {
		reply := GetReply{}
		timeout := int32(1)
		go func() {
			ck.mu.Lock()
			leader := ck.leader
			ck.mu.Unlock()
			util.KVRaftDebugLog("client %v sent RPC Clerk.Get(%v) to server %v", ck.id, arg, leader)
			startTime := time.Now()
			ok := ck.servers[leader].Call("KVServer.Get", &arg, &reply)
			endTime := time.Now()
			if startTime.Add(waitingTimeout).Before(endTime) {
				return
			}
			atomic.StoreInt32(&timeout, 0)
			util.KVRaftDebugLog("client %v got response (ok=%v,%v)for RPC Clerk.Get(%v) from server %v", ck.id, ok, reply, arg, leader)
			if !ok {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % nServers
				ck.mu.Unlock()
				mainLoopCh <- 0
			} else if reply.Err == OK {
				value = reply.Value
				util.KVRaftDebugLog("OK")
				mainLoopCh <- 1
			} else if reply.Err == ErrNoKey {
				mainLoopCh <- 1
			} else if reply.Err == ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % nServers
				ck.mu.Unlock()
				mainLoopCh <- 0
			} else {
				util.Assert(false, "Clerk.Get reached unexpected code region.")
			}
		}()
		go func() {
			time.Sleep(waitingTimeout + 50*time.Millisecond)
			if atomic.LoadInt32(&timeout) == 1 {
				ck.mu.Lock()
				util.KVRaftDebugLog("client %v timeout in receiving response for RPC Clerk.PutAppend("+
					"%v) from server %v", ck.id, arg, ck.leader)
				ck.leader = (ck.leader + 1) % nServers
				ck.mu.Unlock()
				mainLoopCh <- 0
			}
		}()
		done := <-mainLoopCh
		if done != 0 {
			break
		}
	}
	return value
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	arg := PutAppendArgs{key, value, op, ck.id, time.Now().UnixNano()}
	nServers := len(ck.servers)
	mainLoopCh := make(chan int)
	for {
		reply := GetReply{}
		timeout := int32(1)
		go func() {
			ck.mu.Lock()
			leader := ck.leader
			ck.mu.Unlock()
			util.KVRaftDebugLog("client %v sent RPC Clerk.PutAppend(%v) to server %v", ck.id, arg, leader)
			startTime := time.Now()
			ok := ck.servers[leader].Call("KVServer.PutAppend", &arg, &reply)
			endTime := time.Now()
			if startTime.Add(waitingTimeout).Before(endTime) {
				return
			}
			atomic.StoreInt32(&timeout, 0)
			util.KVRaftDebugLog("client %v got response[ok=%v] (%v) for RPC Clerk.PutAppend(%v) from server %v", ck.id,
				ok, reply, arg, leader)
			if !ok {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % nServers
				ck.mu.Unlock()
				mainLoopCh <- 0
			} else if reply.Err == OK {
				util.KVRaftDebugLog("OK")
				mainLoopCh <- 1
			} else if reply.Err == ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % nServers
				ck.mu.Unlock()
				mainLoopCh <- 0
			} else {
				util.Assert(false, "Clerk.PutAppend reached unexpected code region %v.", reply.Err)
			}
		}()
		go func() {
			time.Sleep(waitingTimeout + 50*time.Millisecond)
			if atomic.LoadInt32(&timeout) == 1 {
				ck.mu.Lock()
				util.KVRaftDebugLog("client %v timeout in receiving response for RPC Clerk.PutAppend("+
					"%v) from server %v", ck.id, arg, ck.leader)
				ck.leader = (ck.leader + 1) % nServers
				ck.mu.Unlock()
				mainLoopCh <- 0
			}
		}()
		done := <-mainLoopCh
		if done != 0 {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
