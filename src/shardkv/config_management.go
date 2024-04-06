package shardkv

import (
	"6.824/shardmaster"
	"6.824/util"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// ShiftShardsTransactionResult represents the new shards configuration of the re-config transaction.
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
	kv.endGoroutine(myID)
	util.ShardKVDebugLog("server(%v,%v) reply %+v in ShardKV.ShiftShards for %+v",
		kv.gid, kv.me, *reply, *args)
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
				kv.endGoroutine(myID1)
				kv.endGoroutine(myID2)
			}(arg, servers, dest)
		}
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

type ConfigDifference struct {
	ShardsToGroups   map[int][]int             // gid -> shards
	ShardsFromGroups map[int][]int             // gid -> shards
	InvolvedGroups   map[int][]string          // Involved group ids and this does not include the kv group.
	MaxGroupID       int                       // The max group id needed to be communicated with.
	UnaffectedShards [shardmaster.NShards]bool // The unaffected available shards.
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

// UpdateShardsInfo represents the structure the server's shards update.
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
