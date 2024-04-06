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
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type UpdateShardsInfo struct {
	NewStorage    map[string]string
	DesiredShards []int
	NewVersions   map[int32]int64
	// If Mode is 0, it will cause the configuration change, which means set kv.config = NewConfig.
	// If Mode is 1, it just updates the storage, availability and versions in the ShiftShardsTransactionResultBuffer.
	Mode int
	// NewConfig is only used when Mode is 0 for modifying the current configuration.
	NewConfig shardmaster.Config
}

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

type Service struct {
	Shards  []int             // New available shards.
	Storage map[string]string // Keys for the new shards.
	Version map[int32]int64   // Probably used version
}

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

type Snapshot struct {
	Config       shardmaster.Config
	Availability [shardmaster.NShards]bool
	Storage      map[string]string
	Versions     map[int32]int64
}

/*
ShardKV specification

-> resultBuffer is constrained by the state of ShardKV. If the kv is aborting,
no new goroutine can access it.
*/
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
// processor and in this case Err is OK. Otherwise, Err is only returned. This function needs lock and unlock.
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

// processor is the function interacting with the lower layer raft.
//
// Principle: processor should not share any resource with other go routines. Channel is
// the only safe entity for the processor and other goroutines to communicate.
func (kv *ShardKV) processor() {
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

// returns channel and id.
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

func (kv *ShardKV) endGoroutine(ch chan int, id int64) {
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
	kv.endGoroutine(ch, myID)
}

type ShiftShardsRPCArg struct {
	Storage      map[string]string
	Versions     map[int32]int64
	Type         string // "To","From","All"
	NewConfigNum int    // Used for consistency check.
	Shards       []int
	Group        int
	Timestamp    int64
}

type ShiftShardsRPCReply struct {
	Storage   map[string]string
	Versions  map[int32]int64
	NotLeader bool
	Retry     bool
}

type ShiftShardsTransactionResult struct {
	RunningMode        string       // "Participant" or "Coordinator"
	State              string       // "Commit", "Ready", "Abort"
	ContactState       map[int]bool // gid -> contacted. True: notContacted. False, contacted.
	NewConfig          shardmaster.Config
	NewAvailableShards [shardmaster.NShards]bool
	NewStorage         map[string]string
	NewVersions        map[int32]int64
}

func (res *ShiftShardsTransactionResult) Clone() *ShiftShardsTransactionResult {
	result := new(ShiftShardsTransactionResult)
	result.NewConfig = shardmaster.CloneConfig(&res.NewConfig)
	result.NewAvailableShards = res.NewAvailableShards
	result.NewStorage = maps.Clone(res.NewStorage)
	result.NewVersions = maps.Clone(res.NewVersions)
	result.RunningMode = res.RunningMode
	result.State = res.State
	return result
}

func (res *ShiftShardsTransactionResult) Reset() {
	*res = ShiftShardsTransactionResult{
		RunningMode:        "",
		State:              "",
		ContactState:       nil,
		NewConfig:          shardmaster.Config{},
		NewAvailableShards: [10]bool{},
		NewStorage:         nil,
		NewVersions:        nil,
	}
}

type ConfigDifference struct {
	ShardsToGroups   map[int][]int             // gid -> shards
	ShardsFromGroups map[int][]int             // gid -> shards
	InvolvedGroups   map[int][]string          // Involved group ids and this does not include the kv group.
	MaxGroupID       int                       // The max group id needed to be communicated with.
	UnaffectedShards [shardmaster.NShards]bool // The unaffected available shards.
}

func (kv *ShardKV) ShiftShards(args *ShiftShardsRPCArg, reply *ShiftShardsRPCReply) {
	util.ShardKVDebugLog("server(%v,%v) received %+v in ShardKV.ShiftShards", kv.gid, kv.me, *args)
	lockID := kv.lock(&kv.mu)
	if pastReply, exist := kv.rpcReply[args.Timestamp]; exist {
		kv.unlock(&kv.mu, lockID)
		*reply = (pastReply).(ShiftShardsRPCReply)
		return
	}
	isLeader := kv.rf.IsLeader()
	if !isLeader {
		reply.NotLeader = true
		kv.unlock(&kv.mu, lockID)
		util.ShardKVDebugLog("server(%v,%v) reply %+v in ShardKV.ShiftShards for %+v",
			kv.gid, kv.me, *reply, *args)
		return
	}
	if int(atomic.LoadInt32(&kv.nextConfigNum)) != args.NewConfigNum || kv.getState() != StateUpdating {
		reply.Retry = true
		util.ShardKVDebugLog("server(%v,%v) meets unmatched config numbers in ShiftShards. expected %v,meet %v. Its config: %+v, state=%v",
			kv.gid, kv.me, kv.nextConfigNum, args.NewConfigNum, kv.config, kv.strState(kv.getState()))
		kv.unlock(&kv.mu, lockID)
		util.ShardKVDebugLog("server(%v,%v) reply %+v in ShardKV.ShiftShards for %+v",
			kv.gid, kv.me, *reply, *args)
		return
	}

	myCh, myID := kv.startGoroutine()
	reply.Storage = nil
	if args.Type == "To" {
		info := UpdateShardsInfo{
			NewStorage:    maps.Clone(args.Storage),
			DesiredShards: slices.Clone(args.Shards),
			NewVersions:   maps.Clone(args.Versions),
			Mode:          1,
			NewConfig:     shardmaster.Config{},
		}
		kv.updateShards(&info)
	} else if args.Type == "From" {
		reply.Storage = maps.Clone(kv.snapshot.Storage)
		reply.Versions = maps.Clone(kv.snapshot.Versions)
	} else if args.Type == "All" {
		reply.Storage = maps.Clone(kv.snapshot.Storage)
		reply.Versions = maps.Clone(kv.snapshot.Versions)
		info := UpdateShardsInfo{
			NewStorage:    maps.Clone(args.Storage),
			DesiredShards: slices.Clone(args.Shards),
			NewVersions:   maps.Clone(args.Versions),
			Mode:          1,
			NewConfig:     shardmaster.Config{},
		}
		kv.updateShards(&info)
	} else {
		util.Assert(false, "ShiftShardsRPC reaches unexpected condition. arg=%+v", *args)
	}
	delete(kv.resultBuffer.ContactState, args.Group)
	if len(kv.resultBuffer.ContactState) == 0 {
		kv.rpcReply[args.Timestamp] = *reply
		kv.unlock(&kv.mu, lockID)
		func() {
			defer func() {
				if r := recover(); r != nil {
					util.ShardKVDebugLog("server(%v,%v) met an ignored panic %v", kv.gid, kv.me, r)
				}
			}()
			select {
			case kv.resultChannel <- 1:
			case <-myCh:
			}
		}()
	} else {
		kv.unlock(&kv.mu, lockID)
	}
	kv.endGoroutine(myCh, myID)
	util.ShardKVDebugLog("server(%v,%v) reply %+v in ShardKV.ShiftShards for %+v",
		kv.gid, kv.me, *reply, *args)
}

// updateShards will put the key in newStorage into the own storage if the key's shard is desired.
// concurrency control should be guaranteed by invoker.
// mode: "1" for initialization and "0" for data transfer.
func (kv *ShardKV) updateShards(info *UpdateShardsInfo) {
	var version *map[int32]int64
	var storage *map[string]string
	var availability *[shardmaster.NShards]bool
	if info.Mode == 1 {
		if kv.getState() == StateAborting {
			return
		}
		availability = &(kv.resultBuffer.NewAvailableShards)
		storage = &(kv.resultBuffer.NewStorage)
		version = &(kv.resultBuffer.NewVersions)
	} else if info.Mode == 0 {
		version = &kv.versions
		storage = &kv.storage
		availability = &kv.availableShards
	}
	// Update availability
	for _, shard := range info.DesiredShards {
		(*availability)[shard] = true
	}
	// Update version
	for c, t := range info.NewVersions {
		(*version)[c] = max(t, (*version)[c])
	}
	// Update storage
	for k, v := range info.NewStorage {
		for _, desiredShard := range info.DesiredShards {
			if key2shard(k) == desiredShard {
				(*storage)[k] = v
				break
			}
		}
	}
	// Cause configuration change by mode 0.
	if info.Mode == 0 {
		for s := range kv.shardsVersion {
			kv.shardsVersion[s] = 1
		}
		kv.config = *info.NewConfig.Clone()
		atomic.AddInt32(&kv.nextConfigNum, 1)
	} else if info.Mode == 1 {
		kv.start(Op{Type: "AddService", ClientID: -1, Timestamp: time.Now().UnixNano(),
			NewService: Service{Shards: info.DesiredShards, Storage: maps.Clone(*storage), Version: maps.Clone(*version)}},
			NonBlockingChannelTimeout)
	}
	util.ShardKVDebugLog("server(%v,%v) updated availableShards to: %+v", kv.gid, kv.me, *availability)
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

type QueryShiftShardsTransactionFateArg struct {
	NewConfigNum int
}

type QueryShiftShardsTransactionFateReply struct {
	Commit bool
	Error  Err
}

func (kv *ShardKV) QueryShiftShardsTransactionFate(arg *QueryShiftShardsTransactionFateArg, reply *QueryShiftShardsTransactionFateReply) {
	util.ShardKVDebugLog("server(%v,%v) received QueryShiftShardsTransactionFate arg = %+v", kv.gid, kv.me, *arg)
	if _, ok := kv.start(Op{Type: "SyncLog", ClientID: -1, Timestamp: time.Now().UnixNano()}, NonBlockingChannelTimeout); ok {
		reply.Commit = int32(arg.NewConfigNum) < atomic.LoadInt32(&kv.nextConfigNum)
		reply.Error = OK
	} else {
		reply.Error = ErrWrongLeader
	}
	util.ShardKVDebugLog("server(%v,%v) replied QueryShiftShardsTransactionFate reply = %+v", kv.gid, kv.me, *reply)
}

// handles the pending transaction result. Lock before calling.
func (kv *ShardKV) handlePendingShiftShardsTransactionResult(receivedConfig *shardmaster.Config, pendingResult *ShiftShardsTransactionResult) {
	commitFate := false
	fateMutex := sync.Mutex{}
	fateCond := sync.NewCond(&fateMutex)
	involvedGroups := make(map[int][]string) // gid -> groups, Note this contains the group of this `kv`
	for gid, grp := range kv.config.Groups {
		involvedGroups[gid] = grp
	}
	for gid, grp := range receivedConfig.Groups {
		involvedGroups[gid] = grp
	}
	fateMutex.Lock()
	nContactedGroups := 1
	for gid, grp := range involvedGroups {
		if gid != kv.gid {
			go func(group []string) {
				for serverID := 0; serverID < len(group); serverID = (serverID + 1) % len(group) {
					server := group[serverID]
					arg := QueryShiftShardsTransactionFateArg{receivedConfig.Num}
					reply := QueryShiftShardsTransactionFateReply{}
					serverEnd := kv.make_end(server)
					util.ShardKVDebugLog("server(%v,%v) sends QueryShiftShardsTransactionFate to %v, arg=%v",
						kv.gid, kv.me, server, arg)
					ok := serverEnd.Call("ShardKV.QueryShiftShardsTransactionFate", &arg, &reply)
					util.ShardKVDebugLog("server(%v,%v) got reply QueryShiftShardsTransactionFate from %v, reply=%v",
						kv.gid, kv.me, server, reply)
					if !ok || reply.Error == ErrWrongLeader {
						time.Sleep(33 * time.Millisecond)
						continue
					}
					fateMutex.Lock()
					if reply.Commit {
						commitFate = true
					}
					nContactedGroups += 1
					fateMutex.Unlock()
					fateCond.Signal()
					break
				}
			}(grp)
		}
	}
	// We know the fate of the transaction iff commitFate is COMMIT,
	// or we got answers from all the other involved groups.
	for !commitFate && nContactedGroups != len(involvedGroups) {
		fateCond.Wait()
		util.ShardKVDebugLog("server(%v,%v) wakes from waiting querying result. commitFate=%v,nContactedGroups=%v",
			kv.gid, kv.me, commitFate, nContactedGroups)
	}
	// The fate is known.
	if commitFate {
		pendingResult.State = "Commit"
	} else {
		pendingResult.State = "Abort"
	}
	fateMutex.Unlock()
	util.ShardKVDebugLog("server(%v,%v) finished recovering with fate %v", kv.gid, kv.me, pendingResult.State)
}

// configMonitor monitors the configuration and start a configuration change if needed.
func (kv *ShardKV) configMonitor() {
	retryTime := 43 * time.Millisecond
	for {
		if kv.rf.IsLeader() == true {
			// If this server is not the leader, we do not let it start SyncLog
			// otherwise, it will be a waste.
			// Recover.
			if _, ok := kv.start(Op{Type: "SyncLog", Timestamp: time.Now().UnixNano(), ClientID: -1}, NonBlockingChannelTimeout); ok {
				break
			}
		}
		time.Sleep(retryTime)
	}
	for !kv.killed() {
		if !kv.rf.IsLeader() || kv.getState() != StateReady {
			util.ShardKVDebugLog("server(%v,%v) reject to detect config. isLeader=%v,state=%v",
				kv.gid, kv.me, kv.rf.IsLeader(), kv.strState(kv.getState()))
			time.Sleep(retryTime)
			continue
		}
		nextConfigNum := int(atomic.LoadInt32(&kv.nextConfigNum))
		receivedConfig := kv.masterClerk.Query(nextConfigNum)
		// If receivedConfig.Num == nextConfigNum, it indicates that the received configuration should be
		// used.
		if receivedConfig.Num != nextConfigNum {
			time.Sleep(retryTime)
			continue
		}
		// Request a snapshot to start updating.
		reply, ok := kv.start(Op{Type: "RequestSnapshot", Timestamp: time.Now().UnixNano(), ClientID: -1}, NonBlockingChannelTimeout)
		if !ok {
			time.Sleep(retryTime)
			continue
		}
		lockID := kv.lock(&kv.mu)
		kv.snapshot = reply.(Snapshot)
		util.ShardKVDebugLog("server(%v,%v) received config %+v,current config=%+v,expected %v,received snapshot=%+v",
			kv.gid, kv.me, receivedConfig, kv.config, atomic.LoadInt32(&kv.nextConfigNum), kv.snapshot)
		reply, ok = kv.start(Op{ClientID: -1, Timestamp: time.Now().UnixNano(), Type: "HasPendingResult"}, NonBlockingChannelTimeout)
		if !ok {
			// Wait for the log is up-to-date and hence the recovery check is feasible.
			kv.unlock(&kv.mu, lockID)
			continue
		}
		pendingResult := reply.(struct {
			Valid  bool
			Result ShiftShardsTransactionResult
		})
		if kv.config.Num > receivedConfig.Num {
			// The server detected a configuration change in recovering, we discard it.
			kv.unlock(&kv.mu, lockID)
			continue
		}
		if pendingResult.Valid {
			// The server detected a pending result which means it is the participant and went through
			// the phase one, we need to ask for fate.
			diff := kv.analyzeDifference(&receivedConfig)
			kv.snapshot.Availability = diff.UnaffectedShards
			if _, ok = kv.start(Op{Type: "SetUnaffectedShards", UnaffectedSetAvailability: diff.UnaffectedShards,
				ClientID: -1, Timestamp: time.Now().UnixNano()}, NonBlockingChannelTimeout); !ok {
				kv.unlock(&kv.mu, lockID)
				continue
			}
			kv.setState(StateRecovering)
			kv.handlePendingShiftShardsTransactionResult(&receivedConfig, &pendingResult.Result)
			kv.start(Op{
				Type:                         "ShiftShardsTransaction",
				ClientID:                     -1,
				Timestamp:                    time.Now().UnixNano(),
				ShiftShardsTransactionResult: pendingResult.Result,
			}, NonBlockingChannelTimeout)
			kv.setState(StateReady)
			kv.unlock(&kv.mu, lockID)
			continue
		}
		// Initialize the configuration.
		atomic.StoreInt32(&kv.updatingTo, int32(receivedConfig.Num))
		if kv.config.Num == 0 {
			kv.setState(StateUpdating)
			shards := make([]int, 0)
			for shard, gid := range receivedConfig.Shards {
				if gid == kv.gid {
					shards = append(shards, shard)
				}
			}
			// No extra handler for not-leader case.
			kv.start(Op{ClientID: -1, Timestamp: time.Now().UnixNano(), Type: "UpdateShards",
				UpdateShardsInfo: UpdateShardsInfo{DesiredShards: slices.Clone(shards),
					Mode: 0, NewConfig: *receivedConfig.Clone()}},
				NonBlockingChannelTimeout)
			kv.setState(StateReady)
			kv.unlock(&kv.mu, lockID)
		} else {
			diff := kv.analyzeDifference(&receivedConfig)
			kv.snapshot.Availability = diff.UnaffectedShards
			if _, ok = kv.start(Op{Type: "SetUnaffectedShards", UnaffectedSetAvailability: diff.UnaffectedShards,
				ClientID: -1, Timestamp: time.Now().UnixNano()}, NonBlockingChannelTimeout); !ok {
				kv.unlock(&kv.mu, lockID)
				continue
			}
			kv.setState(StateUpdating)
			kv.resultBuffer.NewConfig = receivedConfig
			kv.resultBuffer.NewAvailableShards = kv.snapshot.Availability
			kv.resultBuffer.NewStorage = maps.Clone(kv.snapshot.Storage)
			kv.resultBuffer.NewVersions = maps.Clone(kv.snapshot.Versions)
			kv.resultBuffer.ContactState = make(map[int]bool)
			for gid := range diff.InvolvedGroups {
				kv.resultBuffer.ContactState[gid] = true
			}
			util.Assert(receivedConfig.Num >= kv.config.Num, "ShardKVServer's config num should not be greater than the master's")
			if len(diff.InvolvedGroups) == 0 {
				// Only the single group can move on. For example in this case:
				// GID=102. C#1:{100,101} -> C#2:{100}
				kv.start(Op{
					Type:      "ShiftShardsTransaction",
					ClientID:  -1,
					Timestamp: time.Now().UnixNano(),
					ShiftShardsTransactionResult: ShiftShardsTransactionResult{
						RunningMode:        "Coordinator",
						State:              "Commit",
						ContactState:       nil,
						NewConfig:          receivedConfig,
						NewAvailableShards: kv.availableShards,
						NewStorage:         maps.Clone(kv.storage),
						NewVersions:        maps.Clone(kv.versions),
					},
				}, NonBlockingChannelTimeout)
				kv.setState(StateReady)
			} else {
				close(kv.resultChannel)
				kv.resultChannel = make(chan int, 1)
				// Coordination among the groups is needed, 2PC starts.
				go kv.executeShiftShards(diff, &receivedConfig)
				go kv.startTransactionManager(diff, receivedConfig)
			}
			kv.unlock(&kv.mu, lockID)
		}
	}
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

// analyzeDifference computes the difference by analyzing the current configuration and received configuration.
// Also sets the unaffected between the current config and new config which
// is used to improve the performance which is required in challenge task 2.
func (kv *ShardKV) analyzeDifference(receivedConfig *shardmaster.Config) *ConfigDifference {
	// Phase #1: Analyze difference
	diff := new(ConfigDifference)
	diff.MaxGroupID = kv.gid // Used to know the need to send ShiftRPCs to higher GID groups.
	diff.ShardsToGroups = make(map[int][]int)
	diff.ShardsFromGroups = make(map[int][]int)
	diff.InvolvedGroups = make(map[int][]string)
	for shard := 0; shard < shardmaster.NShards; shard++ {
		kv.snapshot.Availability[shard] = false
		if receivedConfig.Shards[shard] != kv.snapshot.Config.Shards[shard] {
			contactGid := 0
			if kv.snapshot.Config.Shards[shard] == kv.gid {
				// I am the sender.
				contactGid = receivedConfig.Shards[shard]
				diff.ShardsToGroups[contactGid] = append(diff.ShardsToGroups[contactGid], shard)
				diff.InvolvedGroups[contactGid] = receivedConfig.Groups[contactGid]
			} else if receivedConfig.Shards[shard] == kv.gid {
				// I am the receiver.
				contactGid = kv.snapshot.Config.Shards[shard]
				diff.ShardsFromGroups[contactGid] = append(diff.ShardsFromGroups[contactGid], shard)
				diff.InvolvedGroups[contactGid] = kv.snapshot.Config.Groups[contactGid]
			} else {
				// I am not the receiver or sender. Ignore.
				continue
			}
			diff.MaxGroupID = max(diff.MaxGroupID, contactGid)
		} else if receivedConfig.Shards[shard] == kv.gid {
			diff.UnaffectedShards[shard] = true
		}
	}
	util.ShardKVDebugLog("server(%v,%v) analyzeDifference result:\n\tdiff=%+v\n\tCurrentConfig:%+v",
		kv.gid, kv.me, *diff, kv.snapshot.Config)
	return diff
}

// executeShiftShards shift shards among servers in a specific order and notify the `resultChannel`
// after working.
func (kv *ShardKV) executeShiftShards(diff *ConfigDifference, receivedConfig *shardmaster.Config) {
	for dest := kv.gid + 1; dest <= diff.MaxGroupID; dest++ {
		_, sendTo := diff.ShardsToGroups[dest]
		_, receiveFrom := diff.ShardsFromGroups[dest]
		if sendTo || receiveFrom {
			util.ShardKVDebugLog("server(%v,%v) handles ShiftShardsRPC preparation for group %v in config#%v",
				kv.gid, kv.me, dest, receivedConfig.Num-1)
			arg := ShiftShardsRPCArg{Storage: maps.Clone(kv.storage), NewConfigNum: receivedConfig.Num,
				Versions: maps.Clone(kv.versions), Group: kv.gid, Timestamp: time.Now().UnixNano()}
			if sendTo && !receiveFrom {
				arg.Type = "To"
				arg.Shards = diff.ShardsToGroups[dest]
			} else if !sendTo && receiveFrom {
				arg.Type = "From"
			} else {
				arg.Type = "All"
				arg.Shards = diff.ShardsToGroups[dest]
			}

			// The dest gid group information might be changed. But we still want to get the shards from them.
			servers := make([]string, 0)
			if receivedConfig.HasGroup(dest) {
				servers = slices.Clone(receivedConfig.Groups[dest])
			} else {
				servers = slices.Clone(kv.config.Groups[dest])
			}

			// Asynchronously send ShiftShardsRPCs for each server in the group.
			go func(arg ShiftShardsRPCArg, servers []string, gid int) {
				// If abort signal comes, notify the looping goroutine.
				myCh1, myID1 := kv.startGoroutine()
				myCh2, myID2 := kv.startGoroutine()
				aborted := int32(0)
				go kv.goroutineAbortDaemon(myCh1, myID1, &aborted)
				for i := 0; ; i = (i + 1) % len(servers) {
					server := servers[i]
					reply := ShiftShardsRPCReply{}
					serverEnd := kv.make_end(server)
					util.ShardKVDebugLog("server(%v,%v) send ShiftShardsRPC to %v in config#%v with arg %+v",
						kv.gid, kv.me, server, receivedConfig.Num-1, arg)
					ok := kv.nonBlockingRPC(serverEnd, "ShardKV.ShiftShards", NonBlockingRPCTimeout, &arg, &reply)

					if atomic.LoadInt32(&aborted) == 1 {
						break
					}
					if !ok {
						// This code snippet is used in debug for data race free. Thought this seems silly.
						util.ShardKVDebugLog("server(%v,%v) did not received ShiftShardsRPC from %v in config#%v",
							kv.gid, kv.me, server, receivedConfig.Num-1)
						time.Sleep(73 * time.Millisecond)
						continue
					}
					util.ShardKVDebugLog("server(%v,%v) received ShiftShardsRPC reply %+v from %v in config#%v",
						kv.gid, kv.me, reply, server, receivedConfig.Num-1)
					if reply.NotLeader || reply.Retry {
						time.Sleep(73 * time.Millisecond)
						continue
					}
					// Handle
					info := UpdateShardsInfo{
						NewStorage:    reply.Storage,
						DesiredShards: diff.ShardsFromGroups[gid],
						NewVersions:   reply.Versions,
						Mode:          1,
						NewConfig:     shardmaster.Config{},
					}
					lockID := kv.lock(&kv.mu)
					kv.updateShards(&info)
					delete(kv.resultBuffer.ContactState, gid)
					if len(kv.resultBuffer.ContactState) == 0 {
						kv.unlock(&kv.mu, lockID)
						select {
						case kv.resultChannel <- 1:
						case <-myCh2:
						}
					} else {
						kv.unlock(&kv.mu, lockID)
					}
					break
				}
				kv.endGoroutine(myCh1, myID1)
				kv.endGoroutine(myCh2, myID2)
			}(arg, servers, dest)
		}
	}
}

// startTransactionManager starts a transaction in the 2PC approach including:
// determine the coordinator and participants.
func (kv *ShardKV) startTransactionManager(diff *ConfigDifference, receivedConfig shardmaster.Config) {
	minGroupID := kv.gid
	for gid := range diff.InvolvedGroups {
		minGroupID = min(minGroupID, gid)
	}
	util.ShardKVDebugLog("server(%v,%v) startsTransactionManager in config#%v with minGroupId %v",
		kv.gid, kv.me, receivedConfig.Num-1, minGroupID)
	// Now starts transaction.
	if kv.gid == minGroupID {
		// Run as coordinator.
		wg := sync.WaitGroup{}
		wg.Add(len(diff.InvolvedGroups) + 1)
		result := ShiftShardsTransactionResult{}
		// Wait for the result of the transaction OR abort signal.
		go func() {
			myChannel, myID := kv.startGoroutine()
			select {
			case _ = <-kv.resultChannel:
				result = *kv.resultBuffer
			case _ = <-myChannel:
			}
			kv.endGoroutine(myChannel, myID)
			wg.Done()
		}()

		// Phase #1: Collect Votes
		abortWg := sync.WaitGroup{}
		util.ShardKVDebugLog("server(%v,%v) begins 2PC phase#1 in config#%v", kv.gid, kv.me, kv.config.Num)
		fate := "Commit"
		collectVotesArg := CollectVotesArg{receivedConfig.Num, time.Now().UnixNano()}
		for gid := range diff.InvolvedGroups {
			abortWg.Add(1)
			go func(groupID int) {
				aborted := int32(0)
				myChannel, myID := kv.startGoroutine()
				go kv.goroutineAbortDaemon(myChannel, myID, &aborted)
				for serverID := 0; ; serverID = (serverID + 1) % len(diff.InvolvedGroups[groupID]) {
					server := diff.InvolvedGroups[groupID][serverID]
					serverEnd := kv.make_end(server)
					reply := CollectVotesReply{}
					util.ShardKVDebugLog("server(%v,%v) send CollectVotes arg to %v with arg %+v",
						kv.gid, kv.me, server, collectVotesArg)
					ok := kv.nonBlockingRPC(serverEnd, "ShardKV.CollectVotes", NonBlockingRPCTimeout, &collectVotesArg, &reply)
					util.ShardKVDebugLog("server(%v,%v) received (ok=%v) CollectVotes reply from %v with reply %+v for %+v",
						kv.gid, kv.me, ok, server, reply, collectVotesArg)
					if atomic.LoadInt32(&aborted) == 1 {
						break
					}
					if !ok || reply.Error == ErrWrongLeader || reply.Error == ErrLag {
						time.Sleep(33 * time.Millisecond)
						continue
					}
					util.Assert(reply.Error != "", "Invalid reply error")
					if !reply.Commit {
						fate = "Abort"
					}
					break
				}
				wg.Done()
				abortWg.Done()
				kv.endGoroutine(myChannel, myID)
			}(gid)
		}
		waitDoneFunc := func() <-chan struct{} {
			doneChannel := make(chan struct{})
			go func() {
				wg.Wait()
				doneChannel <- struct{}{}
			}()
			return doneChannel
		}
		select {
		case <-waitDoneFunc():
		case <-time.After(AbortingTimeout):
			util.ShardKVDebugLog("server(%v,%v) abort transaction C%v because of waiting too long.",
				kv.gid, kv.me, receivedConfig.Num)
			kv.abortShiftShardsTransaction()
			abortWg.Wait()
			kv.resultBuffer.Reset()
			util.ShardKVDebugLog("server(%v,%v) returned from abortWg.Wait()", kv.gid, kv.me)
			fate = "Abort"
			result.NewConfig = receivedConfig
		}

		// Phase #2: Decide
		util.ShardKVDebugLog("server(%v,%v) begins 2PC phase#2 in config#%v", kv.gid, kv.me, kv.config.Num)
		result.RunningMode = "Coordinator"
		result.State = fate
		lockID := kv.lock(&kv.mu)
		if _, ok := kv.start(Op{Type: "ShiftShardsTransaction", ClientID: -1, Timestamp: time.Now().UnixNano(),
			ShiftShardsTransactionResult: result}, NonBlockingChannelTimeout); ok {
			localWg := sync.WaitGroup{}
			decideArg := DecideArg{fate == "Commit", receivedConfig.Num, time.Now().UnixNano()}
			for gid := range diff.InvolvedGroups {
				localWg.Add(1)
				go func(groupID int) {
					for serverID := 0; ; serverID = (serverID + 1) % len(diff.InvolvedGroups[groupID]) {
						server := diff.InvolvedGroups[groupID][serverID]
						serverEnd := kv.make_end(server)
						reply := DecideReply{}
						util.ShardKVDebugLog("server(%v,%v) send ShardKV.Decide to %v with arg %+v",
							kv.gid, kv.me, server, decideArg)
						ok := kv.nonBlockingRPC(serverEnd, "ShardKV.Decide", NonBlockingRPCTimeout, &decideArg, &reply)
						util.ShardKVDebugLog("server(%v,%v) received (ok=%v) ShardKV.Decide from %v with reply %+v for arg %+v",
							kv.gid, kv.me, ok, server, reply, decideArg)
						if !ok || reply.Error != OK {
							time.Sleep(33 * time.Millisecond)
							continue
						}
						break
					}
					localWg.Done()
				}(gid)
			}
			localWg.Wait()
		}
		kv.setState(StateReady)
		kv.unlock(&kv.mu, lockID)
	}
}

func (kv *ShardKV) commitShiftShardsTransaction(result *ShiftShardsTransactionResult) {
	util.ShardKVDebugLog("server(%v,%v) begin to commitShiftShardsTransaction. current availability=%+v, buffer availability=%+v",
		kv.gid, kv.me, kv.availableShards, kv.resultBuffer.NewAvailableShards)
	for k, _ := range kv.storage {
		if !kv.availableShards[key2shard(k)] {
			util.ShardKVDebugLog("server(%v,%v) deleted key %v from C#%v->C#%v",
				kv.gid, kv.me, k, kv.config.Num, result.NewConfig.Num)
			delete(kv.storage, k)
		}
	}
	kv.config = *result.NewConfig.Clone()
	atomic.StoreInt32(&kv.nextConfigNum, int32(kv.config.Num+1))
	util.ShardKVDebugLog("server(%v,%v) commitShiftShardsTransaction,current config num:%v,next config num:%v.CurrentConfig:%+v,availability=%+v,storage=%+v",
		kv.gid, kv.me, kv.config.Num, atomic.LoadInt32(&kv.nextConfigNum), kv.config, kv.availableShards, kv.storage)
}

// abortShiftShardsTransaction releases the resource and block service until the resource is
// guaranteed to be not disturbed by the older transaction. This function should be called
// after the kv is locked because of state set.
func (kv *ShardKV) abortShiftShardsTransaction() {
	kv.setState(StateAborting)
	go func() {
		kv.runningGoroutineMutex.Lock()
		util.ShardKVDebugLog("server(%v,%v) is aborting transaction.", kv.gid, kv.me)
		for _, channel := range kv.runningGoroutineChannels {
			nSent := cap(channel)
			for i := 0; i < nSent; i++ {
				channel <- 1
			}
		}
		for len(kv.runningGoroutineChannels) > 0 {
			util.ShardKVDebugLog("server(%v,%v) len(kv.runningGoroutineChannels)=%v,content=%+v",
				kv.gid, kv.me, len(kv.runningGoroutineChannels), kv.runningGoroutineChannels)
			kv.runningGoroutineCond.Wait()
		}
		util.ShardKVDebugLog("server(%v,%v) finished aborting transaction.", kv.gid, kv.me)
		kv.runningGoroutineMutex.Unlock()
	}()
}

/*========== 2PC Phase#1: Collect Votes Implementation ==========*/

type CollectVotesArg struct {
	NewConfigNum int
	Timestamp    int64 // Used to trace bug.
}

type CollectVotesReply struct {
	Error  Err
	Commit bool
}

// CollectVotes collects votes from the participants for the coordinator to determine
// the fate of the shiftShardsTransaction.
func (kv *ShardKV) CollectVotes(args *CollectVotesArg, reply *CollectVotesReply) {
	util.ShardKVDebugLog("server(%v,%v) received CollectVotes with arg %+v", kv.gid, kv.me, *args)
	// If this server is not leader, no result will be received from the channel.
	if !kv.rf.IsLeader() {
		reply.Error = ErrWrongLeader
		util.ShardKVDebugLog("server(%v,%v) replied CollectVotes with reply %+v for %+v",
			kv.gid, kv.me, *reply, *args)
		return
	}

	// Check whether this server would like to serve the RPC or not based on the progress between receiver and sender.
	lockID := kv.lock(&kv.mu)
	updatingTo := int(atomic.LoadInt32(&kv.updatingTo))
	if args.NewConfigNum > updatingTo || kv.getState() != StateUpdating {
		reply.Error = ErrLag
		util.ShardKVDebugLog("server(%v,%v) replied CollectVotes with reply %+v for %+v, state=%v",
			kv.gid, kv.me, *reply, *args, kv.strState(kv.getState()))
		kv.unlock(&kv.mu, lockID)
		return
	}
	kv.unlock(&kv.mu, lockID)

	// Monitor the abort signal.
	myChannel, myID := kv.startGoroutine()
	util.Assert(args.NewConfigNum == updatingTo,
		"CollectVotes config num: want %v, got %v. server(%v,%v)",
		updatingTo, args.NewConfigNum, kv.gid, kv.me)
	select {
	case _ = <-kv.resultChannel:
		util.ShardKVDebugLog("server(%v,%v) got result from resultChannel.",
			kv.gid, kv.me)
		kv.endGoroutine(myChannel, myID)
	case _ = <-myChannel:
		util.ShardKVDebugLog("server(%v,%v) received abort signal.", kv.gid, kv.me)
		reply.Error = OK
		reply.Commit = false
		kv.endGoroutine(myChannel, myID)
		return
	}

	lockID = kv.lock(&kv.mu)
	util.Assert(kv.getState() == StateUpdating, "Unexpected state change during CollectVotes. state=%v",
		kv.strState(kv.getState()))
	var result *ShiftShardsTransactionResult
	kv.resultBuffer.RunningMode = "Participant"
	kv.resultBuffer.State = "Ready"
	result = kv.resultBuffer.Clone()
	_, ok := kv.start(Op{Type: "ShiftShardsTransaction", ClientID: -1,
		Timestamp: time.Now().UnixNano(), ShiftShardsTransactionResult: *result}, NonBlockingChannelTimeout)
	kv.unlock(&kv.mu, lockID)
	// Lost the relationship after executing shiftShards in the term, and the result will be not used,
	// we commit it.
	reply.Commit = ok
	reply.Error = OK
	util.ShardKVDebugLog("server(%v,%v) replied CollectVotes with reply %+v for %+v",
		kv.gid, kv.me, *reply, *args)
}

/*========== 2PC Phase#2: Decide Implementation ==========*/

type DecideArg struct {
	Commit       bool
	NewConfigNum int
	Timestamp    int64 // Used to trace bug
}

type DecideReply struct {
	Error Err
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

// Decide tells the participants the fate of the shiftShardsTransaction.
// Because we just need the single-directional communication in the second phase,
// we don't need to handle the reply.
func (kv *ShardKV) Decide(arg *DecideArg, reply *DecideReply) {
	util.ShardKVDebugLog("server(%v,%v) received Decide: %+v", kv.gid, kv.me, *arg)
	lockID := kv.lock(&kv.mu)
	chReply, ok := kv.start(Op{Type: "HasPendingResult", Timestamp: time.Now().UnixNano(), ClientID: -1}, NonBlockingChannelTimeout)
	if !ok {
		reply.Error = ErrDeny
		util.ShardKVDebugLog("server(%v,%v) replied Decide: %+v for %+v",
			kv.gid, kv.me, *reply, *arg)
		kv.unlock(&kv.mu, lockID)
		return
	}

	pendingResult := chReply.(struct {
		Valid  bool
		Result ShiftShardsTransactionResult
	})

	if kv.getState() == StateAborting || atomic.LoadInt32(&kv.updatingTo) < int32(arg.NewConfigNum) {
		reply.Error = ErrDeny
		util.ShardKVDebugLog("server(%v,%v) is reject for arg %+v.state:isAborting=%v,expected=%v,got=%v",
			kv.gid, kv.me, *arg, kv.getState() == StateAborting, atomic.LoadInt32(&kv.updatingTo), arg.NewConfigNum)
		kv.unlock(&kv.mu, lockID)
		return
	}
	if !pendingResult.Valid {
		reply.Error = OK
		util.ShardKVDebugLog("server(%v,%v) replied Decide: %+v for %+v",
			kv.gid, kv.me, *reply, *arg)
		kv.unlock(&kv.mu, lockID)
		return
	}
	if arg.Commit {
		pendingResult.Result.State = "Commit"
	} else {
		pendingResult.Result.State = "Abort"
		kv.abortShiftShardsTransaction()
		kv.resultBuffer.Reset()
	}
	if _, ok = kv.start(Op{
		Type:                         "ShiftShardsTransaction",
		ClientID:                     -1,
		Timestamp:                    time.Now().UnixNano(),
		ShiftShardsTransactionResult: pendingResult.Result,
	}, NonBlockingChannelTimeout); !ok {
		reply.Error = ErrWrongLeader
	} else {
		reply.Error = OK
	}
	kv.setState(StateReady)
	util.ShardKVDebugLog("server(%v,%v) replied Decide: %+v for %+v",
		kv.gid, kv.me, *reply, *arg)
	kv.unlock(&kv.mu, lockID)
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
	go kv.processor()
	go kv.configMonitor()
	return kv
}
