package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardmaster"
	"6.824/util"
	"fmt"
	"maps"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Op is the type used to interact with Raft.
//
// Supported type:  "Get", "Put", "Append","ShiftShardsTransaction",
//
//	"UpdateShards", "SyncLog", "HasPendingResult", "RequestSnapshot", "AddServe"
type Op struct {
	Type                         string
	ClientID                     int32
	Timestamp                    int64
	Key                          string
	Value                        string
	UpdateShardsInfo             UpdateShardsInfo
	ShiftShardsTransactionResult ShiftShardsTransactionResult
	UnaffectedSetAvailability    [shardmaster.NShards]bool
	NewService                   Service
}

// Service represents what the server can serve to the outside.
type Service struct {
	Shards  []int             // New available shards.
	Storage map[string]string // Keys for the new shards.
	Version map[int32]int64   // Probably used version
}

// SnapshotData represents the data structure used to make snapshot by Raft.
type SnapshotData struct {
	Timestamp                              int64 // The time when the snapshot is made.
	Storage                                map[string]string
	Versions                               map[int32]int64
	Config                                 shardmaster.Config
	AvailableShards                        [shardmaster.NShards]bool
	PendingShiftShardsTransactionResult    ShiftShardsTransactionResult
	HasPendingShiftShardsTransactionResult bool
	ShardsVersion                          [shardmaster.NShards]int
}

// Snapshot represents the state at a point of the shard kv server, used for
// isolating mainLoop and RPCs.
type Snapshot struct {
	Config       shardmaster.Config
	Availability [shardmaster.NShards]bool
	Storage      map[string]string
	Versions     map[int32]int64
}

// ShardKV represents the shardKV server.
type ShardKV struct {
	mu           sync.Mutex
	me           int
	dead         int32
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	masterClerk  *shardmaster.Clerk
	maxRaftState int                // snapshot if log grows this big
	config       shardmaster.Config // The current configuration of the KV Service system. Used for detection

	availableShards        [shardmaster.NShards]bool // Used for serving check.
	storage                map[string]string
	versions               map[int32]int64
	snapshotCh             chan raft.Snapshot
	snapshotDoneReceiverCh chan int

	shardsVersion            [shardmaster.NShards]int
	processorChannel         map[string]chan any
	processorChannelMapMutex sync.Mutex
	pendingResult            *ShiftShardsTransactionResult
	resultBuffer             *ShiftShardsTransactionResult
	snapshot                 Snapshot
	resultChannel            chan int
	nextConfigNum            int32 // Next expected configuration number from querying the master.
	updatingTo               int32

	runningGoroutineMutex    sync.Mutex
	runningGoroutineCond     sync.Cond
	runningGoroutineChannels map[int64]chan int
	state                    int32

	// rpcReply is used to solve the leader crash problem in the last challenge.
	// That challenge violated the assumption of 2PC and hench this field is needed.
	rpcReply map[int64]any
}

const (
	AbortingTimeout           = 2000 * time.Millisecond
	NonBlockingRPCTimeout     = 1000 * time.Millisecond
	NonBlockingChannelTimeout = 1250 * time.Millisecond
	StateReady                = int32(0) // Ready to be any state below
	StateRecovering           = int32(1)
	StateUpdating             = int32(2)
	StateAborting             = int32(3)

	debugShowLog = true
)

// kvOperationRPCInternalHandler is the internal handler of Get/PutAppend, returns anyReply when it received from
// mailLoop and in this case Err is OK. Otherwise, Err is only returned. This function needs lock and unlock.
func (kv *ShardKV) kvOperationRPCInternalHandler(op Op) (any, Err) {
	_, _, isLeader := kv.rf.Start(op)
	var err Err = OK
	var anyReply any
	if !isLeader {
		err = ErrWrongLeader
		util.ShardKVDebugLog("server(%v,%v) is not a leader hinted by start().", kv.gid, kv.me)
	} else {
		lockID := kv.lock(&kv.processorChannelMapMutex)
		chName := kv.generateProcessingDoneChannelName(op.ClientID, op.Timestamp)
		_, exist := kv.processorChannel[chName]
		var ch chan any
		if !exist {
			kv.processorChannel[chName] = make(chan any)
		}
		ch = kv.processorChannel[chName]
		kv.unlock(&kv.processorChannelMapMutex, lockID)
		// Consider the case, the server received the request when it is the leader. But it restarts later,
		// then this channel will never be notified. So we need the timeout check.
		select {
		case anyReply = <-ch:
		case <-time.After(2 * time.Second):
			err = ErrRPCInternal
		}
	}
	return anyReply, err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	util.ShardKVDebugLog("server(%v,%v) received %v in ShardKVServer.Get", kv.gid, kv.me, *args)
	var op interface{} = Op{Type: "Get", Key: args.Key, Value: "", ClientID: args.ClientID, Timestamp: args.Timestamp}
	anyReply, err := kv.kvOperationRPCInternalHandler(op.(Op))
	if err != OK {
		reply.Err = err
	} else {
		*reply = anyReply.(GetReply)
	}
	util.ShardKVDebugLog("server(%v,%v) returned %v in ShardKVServer.Get(arg=%+v)", kv.gid, kv.me, *reply, *args)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	util.ShardKVDebugLog("server(%v,%v) received %v in ShardKVServer.PutAppend", kv.gid, kv.me, *args)
	var op interface{} = Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID,
		Timestamp: args.Timestamp}
	anyReply, err := kv.kvOperationRPCInternalHandler(op.(Op))
	if err != OK {
		reply.Err = err
	} else {
		*reply = anyReply.(PutAppendReply)
	}
	util.ShardKVDebugLog("server(%v,%v) returned %v in ShardKVServer.PutAppend(arg=%v)", kv.gid, kv.me, *reply, *args)
}

// snapshotCheckAndMake checks the condition for snapshot making and make snapshot if the condition is satisfied.
// Call this function after lock kv and unlock after this function returns.
// Note that this function guarantees that the snapshot is made in the raft level when it returned.
// It does not guarantee that all the followers received the snapshot and made in that time.
// So, the snapshot in the receiver side might be stale and be used if you do not pay more attention to
// snapshot check.
func (kv *ShardKV) snapshotCheckAndMake(applyItem *raft.ApplyMsg, snapshotIsMaking *int32) {
	snapshotDataBuffer := SnapshotData{
		Timestamp:                              time.Now().UnixNano(),
		Storage:                                maps.Clone(kv.storage),
		Versions:                               maps.Clone(kv.versions),
		Config:                                 *kv.config.Clone(),
		AvailableShards:                        kv.availableShards,
		PendingShiftShardsTransactionResult:    ShiftShardsTransactionResult{},
		HasPendingShiftShardsTransactionResult: false,
		ShardsVersion:                          kv.shardsVersion,
	}
	if kv.pendingResult != nil {
		snapshotDataBuffer.HasPendingShiftShardsTransactionResult = true
		snapshotDataBuffer.PendingShiftShardsTransactionResult = *kv.pendingResult.Clone()
	}
	if applyItem.RaftStateSize > kv.maxRaftState && atomic.CompareAndSwapInt32(snapshotIsMaking, 0, 1) {
		go func(commandIndex int, snapshotData SnapshotData) {
			util.ShardKVDebugLog("server(%v,%v) try to make snapshot.", kv.gid, kv.me)
			snapshot := raft.Snapshot{
				LastIncludedIndex: commandIndex,
				LastIncludedTerm:  0,
				Data:              snapshotData,
			}
			kv.snapshotCh <- snapshot
			util.ShardKVDebugLog("server(%v,%v) sent snapshot %+v to snapshot channel.", kv.gid, kv.me, snapshot)
			_ = <-kv.snapshotDoneReceiverCh
			atomic.StoreInt32(snapshotIsMaking, 0)
			util.ShardKVDebugLog("server(%v,%v) finished snapshot.", kv.gid, kv.me)
		}(applyItem.CommandIndex, snapshotDataBuffer)
	}
}

func (kv *ShardKV) generateProcessingDoneChannelName(clientID int32, timestamp int64) string {
	return fmt.Sprintf("%v %v", clientID, timestamp)
}

// notifyProcessingDone tells the invoker of the kv operation from server RPCs for client requests that the
// operations are done.
func (kv *ShardKV) notifyProcessingDone(clientID int32, timestamp int64, reply any) {
	which := fmt.Sprintf("%v %v", clientID, timestamp)
	lockID := kv.lock(&kv.processorChannelMapMutex)
	select {
	case kv.processorChannel[which] <- reply:
	default:
	}
	kv.unlock(&kv.processorChannelMapMutex, lockID)
}

// mailLoop is the function interacting with the lower layer raft.
//
// Principle: mailLoop should not share any resource with other go routines. Channel is
// the only safe entity for the mailLoop and other goroutines to communicate.
func (kv *ShardKV) mailLoop() {
	snapshotIsMaking := int32(0)
	lastProcessTimestamp := int64(0)
	for !kv.killed() {
		applyItem := <-kv.applyCh
		util.ShardKVDebugLog("server(%v,%v) received %v th apply item: %+v", kv.gid, kv.me, applyItem.CommandIndex, applyItem)
		if applyItem.SnapshotValid {
			snapshotData := applyItem.SnapshotData.(SnapshotData)
			// If the snapshot is stale, we do not apply it.
			if snapshotData.Timestamp > lastProcessTimestamp {
				kv.storage = maps.Clone(snapshotData.Storage)
				kv.versions = maps.Clone(snapshotData.Versions)
				kv.availableShards = snapshotData.AvailableShards
				kv.config = shardmaster.CloneConfig(&snapshotData.Config)
				kv.shardsVersion = snapshotData.ShardsVersion
				atomic.StoreInt32(&kv.nextConfigNum, int32(kv.config.Num+1))
				if snapshotData.HasPendingShiftShardsTransactionResult {
					kv.pendingResult = new(ShiftShardsTransactionResult)
					*kv.pendingResult = snapshotData.PendingShiftShardsTransactionResult
				}
				util.ShardKVDebugLog("server(%v,%v) used snapshot %+v", kv.gid, kv.me, snapshotData)
			} else {
				util.ShardKVDebugLog("server(%v,%v) did not use snapshot %+v", kv.gid, kv.me, snapshotData)
			}
		} else {
			noVersionUpdate := false
			operation := applyItem.Command.(Op)
			lastProcessTimestamp = operation.Timestamp
			if operation.Type == "SyncLog" {
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, struct{}{})
			} else if operation.Type == "AddService" {
				// AddService is used for the optimization Challenge2Partial.
				util.ShardKVDebugLog("server(%v,%v) shardsVersion=%v in the begin of addService:%+v",
					kv.gid, kv.me, kv.shardsVersion, operation.NewService)
				isNewShard := make(map[int]bool)
				// Update availability.
				for _, newShard := range operation.NewService.Shards {
					isNewShard[newShard] = true
					kv.availableShards[newShard] = true
				}
				// Update storage.
				// If we do not use isNewShard, this would conflict with linearizability.
				// If we do not check the shard version, linearizability will be broken too.
				for k, v := range operation.NewService.Storage {
					shard := key2shard(k)
					if isNewShard[shard] && kv.shardsVersion[shard] <= kv.config.Num {
						util.ShardKVDebugLog("server(%v,%v) set key %v : %v -> %v in C#%v. ",
							kv.gid, kv.me, k, kv.storage[k], v, kv.config.Num)
						kv.storage[k] = v
					}
				}
				// Update the shard versions in the last because some keys might be in the same shard.
				for shard, v := range kv.shardsVersion {
					if isNewShard[shard] && v <= kv.config.Num {
						kv.shardsVersion[shard] = kv.config.Num + 1
					}
				}
				// Update version.
				for client, version := range operation.NewService.Version {
					kv.versions[client] = max(kv.versions[client], version)
				}
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, struct{}{})
				util.ShardKVDebugLog("server(%v,%v) shardsVersion=%v in the end of addService:%+v",
					kv.gid, kv.me, kv.shardsVersion, operation.NewService)
			} else if operation.Type == "RequestSnapshot" {
				// Consider the following log:
				// ----------------------------
				// | RequestSnapshot | Append |
				// ----------------------------
				// If we do not temporarily set availability to false, the log entries above will cause
				// inconsistency which is the server is updating while it is appending to the storage
				// so the snapshot sent to the ConfigMonitor goroutine is stale.
				reply := Snapshot{
					Config:       *kv.config.Clone(),
					Availability: kv.availableShards,
					Storage:      maps.Clone(kv.storage),
					Versions:     maps.Clone(kv.versions),
				}
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, reply)
				for i := 0; i < shardmaster.NShards; i++ {
					if kv.shardsVersion[i] != kv.config.Num+1 {
						kv.availableShards[i] = false
					}
				}
			} else if operation.Type == "HasPendingResult" {
				reply := struct {
					Valid  bool
					Result ShiftShardsTransactionResult
				}{}
				reply.Valid = kv.pendingResult != nil
				if reply.Valid {
					reply.Result = *kv.pendingResult.Clone()
				}
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, reply)
			} else if operation.Type == "SetUnaffectedShards" {
				for s, a := range operation.UnaffectedSetAvailability {
					if a {
						kv.shardsVersion[s] = kv.config.Num + 1
						kv.availableShards[s] = true
					} else if kv.shardsVersion[s] <= kv.config.Num {
						kv.availableShards[s] = false
					}
				}
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, struct{}{})
			} else if operation.Type == "ShiftShardsTransaction" {
				util.ShardKVDebugLog("server(%v,%v) meets 2PC log entry:RunningMode=%v,State=%v,ConfigNum:%v",
					kv.gid, kv.me, operation.ShiftShardsTransactionResult.RunningMode,
					operation.ShiftShardsTransactionResult.State, operation.ShiftShardsTransactionResult.NewConfig.Num)
				if operation.ShiftShardsTransactionResult.RunningMode == "Participant" {
					if operation.ShiftShardsTransactionResult.State == "Ready" {
						kv.pendingResult = operation.ShiftShardsTransactionResult.Clone()
					} else if operation.ShiftShardsTransactionResult.State == "Abort" {
						kv.pendingResult = nil
					} else if operation.ShiftShardsTransactionResult.State == "Commit" {
						kv.commitShiftShardsTransaction(&operation.ShiftShardsTransactionResult)
						kv.pendingResult = nil
					} else {
						util.Assert(false, "ShiftShardsTransactionResult.State %v is not implemented.",
							operation.ShiftShardsTransactionResult.State)
					}
				} else if operation.ShiftShardsTransactionResult.RunningMode == "Coordinator" {
					if operation.ShiftShardsTransactionResult.State == "Commit" {
						kv.commitShiftShardsTransaction(&operation.ShiftShardsTransactionResult)
					} else if operation.ShiftShardsTransactionResult.State == "Abort" {

					} else {
						util.Assert(false, "ShiftShardsTransactionResult.State %v is not implemented.",
							operation.ShiftShardsTransactionResult.State)
					}
				} else {
					util.Assert(false, "ShiftShardsTransactionResult.RunningMode %v is not implemented.",
						operation.ShiftShardsTransactionResult.RunningMode)
				}
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, struct{}{})
			} else if operation.Type == "UpdateShards" {
				kv.updateShards(&operation.UpdateShardsInfo)
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, struct{}{})
			} else if operation.Type == "Get" {
				reply := GetReply{}
				if kv.availableShards[key2shard(operation.Key)] {
					reply.Err = OK
					reply.Value = kv.storage[operation.Key]
				} else {
					noVersionUpdate = true
					util.ShardKVDebugLog("server(%v,%v) did not handle operation. Reason: not available. Config#%v, availability=%v",
						kv.gid, kv.me, kv.config.Num, kv.availableShards)
					reply.Err = ErrWrongGroup
				}
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, reply)
			} else if operation.Type == "Put" {
				keyShardIsAvailable := kv.availableShards[key2shard(operation.Key)]
				if kv.versions[operation.ClientID] < operation.Timestamp && keyShardIsAvailable {
					kv.storage[operation.Key] = operation.Value
				} else if kv.versions[operation.ClientID] >= operation.Timestamp {
					util.ShardKVDebugLog("server(%v,%v) did not handle operation. Reason: invalid timestamp %v, last timestamp %v",
						kv.gid, kv.me, operation.Timestamp, kv.versions[operation.ClientID])
				} else if !keyShardIsAvailable {
					util.ShardKVDebugLog("server(%v,%v) did not handle operation. Reason: not available",
						kv.gid, kv.me)
				}
				reply := PutAppendReply{}
				if keyShardIsAvailable {
					reply.Err = OK
				} else {
					noVersionUpdate = true
					reply.Err = ErrWrongGroup
				}
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, reply)
				util.ShardKVDebugLog("server(%v,%v) updated key %v:\n\tStorage:%+v\n\tCurrentConfig:%+v\n\tAvailability:%+v",
					kv.gid, kv.me, operation.Key, kv.storage, kv.config, kv.availableShards)
			} else if operation.Type == "Append" {
				keyShardIsAvailable := kv.availableShards[key2shard(operation.Key)]
				if kv.versions[operation.ClientID] < operation.Timestamp && keyShardIsAvailable {
					kv.storage[operation.Key] += operation.Value
				} else if kv.versions[operation.ClientID] >= operation.Timestamp {
					util.ShardKVDebugLog("server(%v,%v) did not handle operation. Reason: invalid timestamp %v, last timestamp %v",
						kv.gid, kv.me, operation.Timestamp, kv.versions[operation.ClientID])
				} else if !keyShardIsAvailable {
					util.ShardKVDebugLog("server(%v,%v) did not handle operation. Reason: not available",
						kv.gid, kv.me)
				}
				reply := PutAppendReply{}
				if keyShardIsAvailable {
					reply.Err = OK
				} else {
					noVersionUpdate = true
					reply.Err = ErrWrongGroup
				}
				kv.notifyProcessingDone(operation.ClientID, operation.Timestamp, reply)
				util.ShardKVDebugLog("server(%v,%v) updated key %v:\n\tStorage:%+v\n\tCurrentConfig:%+v\n\tAvailability:%+v",
					kv.gid, kv.me, operation.Key, kv.storage, kv.config, kv.availableShards)
			} else {
				util.Assert(false, "Op.Type %v is not implemented.", operation.Type)
			}
			if operation.Timestamp > kv.versions[operation.ClientID] && !noVersionUpdate {
				kv.versions[operation.ClientID] = operation.Timestamp
			}
		}
		/* Make snapshot or not. */
		kv.snapshotCheckAndMake(&applyItem, &snapshotIsMaking)
		if applyItem.CommandValid {
			util.ShardKVDebugLog("server(%v,%v) broadcast operation %+v", kv.gid, kv.me, applyItem.Command.(Op))
		}
	}
}

// startGoroutine returns channel used to quit the goroutine and its id.
func (kv *ShardKV) startGoroutine() (chan int, int64) {
	_, _, line, _ := runtime.Caller(1)
	kv.runningGoroutineMutex.Lock()
	myChanID := time.Now().UnixNano()
	myChan := make(chan int, 1)
	util.ShardKVDebugLog("server(%v,%v) starts a goroutine in line %v,id %v.", kv.gid, kv.me, line, myChanID)
	kv.runningGoroutineChannels[myChanID] = myChan
	kv.runningGoroutineMutex.Unlock()
	return myChan, myChanID
}

func (kv *ShardKV) endGoroutine(id int64) {
	kv.runningGoroutineMutex.Lock()
	delete(kv.runningGoroutineChannels, id)
	_, _, line, _ := runtime.Caller(1)
	util.ShardKVDebugLog("server(%v,%v) ends a goroutine, len(kv.runningGoroutineChannels)=%v in line %v,id %v.",
		kv.gid, kv.me, len(kv.runningGoroutineChannels), line, id)
	kv.runningGoroutineCond.Signal()
	kv.runningGoroutineMutex.Unlock()
}

func (kv *ShardKV) goroutineAbortDaemon(ch chan int, myID int64, aborted *int32) {
	val := <-ch
	_, _, line, _ := runtime.Caller(1)
	util.ShardKVDebugLog("server(%v,%v) received abort signal val %v in abort daemon in line %v",
		kv.gid, kv.me, val, line)
	atomic.StoreInt32(aborted, 1)
	kv.endGoroutine(myID)
}

// start starts an operation. Return the reply and ok after timeout or received from the channel.
func (kv *ShardKV) start(op Op, timeout time.Duration) (any, bool) {
	_, _, line, _ := runtime.Caller(1)
	util.ShardKVDebugLog("server(%v,%v) start(%+v) in line %v", kv.gid, kv.me, op, line)
	// generate channel
	ch := make(chan any, 1)
	lockID := kv.lock(&kv.processorChannelMapMutex)
	chName := kv.generateProcessingDoneChannelName(op.ClientID, op.Timestamp)
	kv.processorChannel[chName] = ch
	kv.unlock(&kv.processorChannelMapMutex, lockID)
	_, _, isLeader := kv.rf.Start(op)
	util.ShardKVDebugLog("server(%v,%v) returned from kv.rf.start(%+v) in line %v", kv.gid, kv.me, op, line)
	// Receive from channel
	var reply any
	ok := isLeader
	if isLeader {
		select {
		case reply = <-ch:
			ok = true
		case <-time.After(timeout):
			ok = false
		}
	}
	util.ShardKVDebugLog("server(%v,%v) returned from kv.start(%+v) with reply %+v in line %v",
		kv.gid, kv.me, op, reply, line)
	return reply, ok
}

func (kv *ShardKV) nonBlockingRPC(serverEnd *labrpc.ClientEnd, rpc string, timeout time.Duration, arg any, reply any) bool {
	ok := false
	rpcCh := make(chan bool)
	go func() {
		rpcCh <- serverEnd.Call(rpc, arg, reply)
	}()
	select {
	case <-time.After(timeout):
		ok = false
	case ok = <-rpcCh:
	}
	return ok
}

func (kv *ShardKV) nonBlockingReceive(ch chan any, timeout time.Duration) (any, bool) {
	var reply any
	var ok bool
	select {
	case reply = <-ch:
		ok = true
	case <-time.After(timeout):
		ok = false
	}
	util.ShardKVDebugLog("server(%v,%v) nonBlockingReceive: ok=%v,reply=%v",
		kv.gid, kv.me, ok, reply)
	return reply, ok
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// `me` is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(SnapshotData{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.masterClerk = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.storage = make(map[string]string)
	kv.versions = make(map[int32]int64)
	kv.resultBuffer = new(ShiftShardsTransactionResult)
	kv.resultChannel = make(chan int, 1)
	kv.processorChannel = make(map[string]chan any)
	kv.nextConfigNum = 1
	kv.runningGoroutineMutex = sync.Mutex{}
	kv.runningGoroutineCond = *sync.NewCond(&kv.runningGoroutineMutex)
	kv.runningGoroutineChannels = make(map[int64]chan int)
	kv.rpcReply = make(map[int64]any)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.config.Shards[i] = kv.gid
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	if maxRaftState > 0 {
		kv.snapshotCh = make(chan raft.Snapshot)
		kv.snapshotDoneReceiverCh = make(chan int)
		kv.rf.InitializeSnapshot(kv.snapshotCh, kv.snapshotDoneReceiverCh)
	}
	if snapshot, hasSnapshot := kv.rf.GetSnapshot(); hasSnapshot {
		data := snapshot.(SnapshotData)
		kv.storage = data.Storage
		kv.versions = data.Versions
		kv.config = shardmaster.CloneConfig(&data.Config)
		kv.nextConfigNum = int32(kv.config.Num + 1)
		kv.availableShards = data.AvailableShards
		kv.shardsVersion = data.ShardsVersion
		if data.HasPendingShiftShardsTransactionResult {
			kv.pendingResult = new(ShiftShardsTransactionResult)
			*kv.pendingResult = data.PendingShiftShardsTransactionResult
		}
	}

	util.ShardKVDebugLog("server(%v,%v) starts.", kv.gid, kv.me)
	go kv.mailLoop()
	go kv.configMonitor()
	return kv
}
