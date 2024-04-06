package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/util"
	"maps"
	"sync"
	"sync/atomic"
)

type SnapshotData struct {
	Storage  map[string]string
	Versions map[int32]int64
}

type Op struct {
	Type      string // Types: `Get`, `Put`, `Append`
	Key       string
	Value     string
	ClientID  int32
	Timestamp int64
}

type KVServer struct {
	mu                     sync.Mutex
	me                     int
	rf                     *raft.Raft
	applyCh                chan raft.ApplyMsg
	snapshotCh             chan raft.Snapshot
	snapshotDoneReceiverCh chan int
	dead                   int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big

	versions map[int32]int64   // (clientID,timestamp) Update only if all RPCs are handled whose timestamp is less than #
	storage  map[string]string // stores the kv pairs in memory
	cond     sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	util.KVRaftDebugLog("server %+v received %v in KVServer.Get", kv.me, *args)
	var op interface{} = Op{Type: "Get", Key: args.Key, Value: "", ClientID: args.ClientID, Timestamp: args.Timestamp}
	_, _, isLeader := kv.rf.Start(op)
	value, exists := "", false
	if !isLeader {
		reply.Err = ErrWrongLeader
		util.KVRaftDebugLog("server %v is not a leader hinted by start().", kv.me)
	} else {
		kv.lock()
		for kv.versions[args.ClientID] < args.Timestamp {
			util.KVRaftDebugLog("server %v waits in handling %v in KVServer.Get", kv.me, *args)
			kv.cond.Wait()
			util.KVRaftDebugLog("server %v wakes in handling %v in KVServer.Get", kv.me, *args)
			if kv.killed() {
				reply.Err = ErrServerKilled
			}
			if !kv.rf.IsLeader() {
				isLeader = false
				break
			}
		}
		value, exists = kv.storage[args.Key]
		kv.unlock()
		if !isLeader {
			util.KVRaftDebugLog("server %v is not a leader hinted by GetState().", kv.me)
			// This server lost its leadership.
			reply.Err = ErrWrongLeader
		} else if exists {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
	util.KVRaftDebugLog("server %v returned %v in KVServer.Get(arg=%+v)", kv.me, *reply, *args)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	util.KVRaftDebugLog("server %v received %v in KVServer.PutAppend", kv.me, *args)
	var op interface{} = Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID,
		Timestamp: args.Timestamp}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		util.KVRaftDebugLog("server %v is not a leader hinted by start().", kv.me)
	} else {
		kv.lock()
		for kv.versions[args.ClientID] < args.Timestamp {
			util.KVRaftDebugLog("server %v waits in handling message %v", kv.me, *args)
			kv.cond.Wait()
			util.KVRaftDebugLog("server %v wakes in handling message %v", kv.me, *args)
			if kv.killed() {
				reply.Err = ErrServerKilled
			}
			if !kv.rf.IsLeader() {
				isLeader = false
				break
			}
		}
		kv.unlock()
		if !isLeader {
			util.KVRaftDebugLog("server %v is not a leader hinted by GetState().", kv.me)
			// This server lost its leadership.
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	}
	util.KVRaftDebugLog("server %v returned %v in KVServer.PutAppend(arg=%v)", kv.me, *reply, *args)
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.cond.Broadcast()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) lock() {
	//_, _, line, _ := runtime.Caller(1)
	//util.KVRaftDebugLog("KVServer %v try to acquire lock in line %v.", kv.me, line)
	kv.mu.Lock()
	//util.KVRaftDebugLog("KVServer %v acquired the lock in line %v.", kv.me, line)
}

func (kv *KVServer) unlock() {
	//_, _, line, _ := runtime.Caller(1)
	kv.mu.Unlock()
	//util.KVRaftDebugLog("KVServer %v released the lock in line %v.", kv.me, line)
}

func (kv *KVServer) worker() {
	snapshotIsMaking := int32(0)
	for !kv.killed() {
		applyItem := <-kv.applyCh
		util.KVRaftDebugLog("KVServer %v received %v th apply item: %+v", kv.me, applyItem.CommandIndex, applyItem)
		if applyItem.SnapshotValid {
			kv.lock()
			snapshot := applyItem.SnapshotData.(SnapshotData)
			kv.storage = maps.Clone(snapshot.Storage)
			kv.versions = maps.Clone(snapshot.Versions)
			kv.unlock()
		} else {
			operation := applyItem.Command.(Op)
			util.KVRaftDebugLog("server %v received operation %v", kv.me, operation)
			kv.lock()
			if operation.Type == "Get" {
			} else if kv.versions[operation.ClientID] < operation.Timestamp {
				if operation.Type == "Put" {
					kv.storage[operation.Key] = operation.Value
				} else if operation.Type == "Append" {
					kv.storage[operation.Key] += operation.Value
				}
				util.KVRaftDebugLog("server %v updated %v. Storage:\n%+v", kv.me, operation.Key, kv.storage)
			} else {
				// Temporarily nothing.
				util.KVRaftDebugLog("KVServer %v meets older version. Operation = %v", kv.me, operation)
			}
			if operation.Timestamp > kv.versions[operation.ClientID] {
				kv.versions[operation.ClientID] = operation.Timestamp
			}
			kv.unlock()
			kv.cond.Broadcast()
			util.KVRaftDebugLog("server %v broadcast operation %v", kv.me, operation)
		}
		if applyItem.RaftStateSize > kv.maxRaftState && atomic.CompareAndSwapInt32(&snapshotIsMaking, 0, 1) {
			go func(commandIndex int, storage map[string]string, versions map[int32]int64) {
				snapshot := raft.Snapshot{
					LastIncludedIndex: commandIndex,
					LastIncludedTerm:  0,
					Data: SnapshotData{
						Storage:  storage,
						Versions: versions,
					},
				}
				util.KVRaftDebugLog("KVServer %v sent snapshot %+v to snapshot channel.", kv.me, snapshot)
				kv.snapshotCh <- snapshot
				_ = <-kv.snapshotDoneReceiverCh
				atomic.StoreInt32(&snapshotIsMaking, 0)
			}(applyItem.CommandIndex, maps.Clone(kv.storage), maps.Clone(kv.versions))
		}
	}
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// `me` is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds `maxRaftState` bytes,
// in order to allow Raft to garbage-collect its log. if `maxRaftState` is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(SnapshotData{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.mu = sync.Mutex{}
	kv.cond = sync.Cond{L: &kv.mu}
	kv.storage = make(map[string]string)
	kv.versions = make(map[int32]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	if snapshot, hasSnapshot := kv.rf.GetSnapshot(); hasSnapshot {
		snapshotData := snapshot.(SnapshotData)
		kv.storage = maps.Clone(snapshotData.Storage)
		kv.versions = maps.Clone(snapshotData.Versions)
	}
	if maxRaftState > 0 {
		kv.snapshotCh = make(chan raft.Snapshot)
		kv.snapshotDoneReceiverCh = make(chan int)
		kv.rf.InitializeSnapshot(kv.snapshotCh, kv.snapshotDoneReceiverCh)
	}

	go kv.worker()
	return kv
}
