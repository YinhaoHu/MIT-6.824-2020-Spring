package shardkv

import (
	"6.824/shardmaster"
	"6.824/util"
	"sync"
	"sync/atomic"
	"time"
)

type CollectVotesArg struct {
	NewConfigNum int
	Timestamp    int64 // Used to trace bug.
}

type CollectVotesReply struct {
	Error  Err
	Commit bool
}

// CollectVotes collects votes from the participants for the coordinator to determine
// the fate of the shiftShardsTransaction in 2PC phase1.
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
		kv.endGoroutine(myID)
	case _ = <-myChannel:
		util.ShardKVDebugLog("server(%v,%v) received abort signal.", kv.gid, kv.me)
		reply.Error = OK
		reply.Commit = false
		kv.endGoroutine(myID)
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

type DecideArg struct {
	Commit       bool
	NewConfigNum int
	Timestamp    int64 // Used to trace bug
}

type DecideReply struct {
	Error Err
}

// Decide tells the participants the fate of the shiftShardsTransaction.
// Because we just need the single-directional communication in the second phase,
// we don't need to handle the reply in 2PC phase2.
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

type QueryShiftShardsTransactionFateArg struct {
	NewConfigNum int
}

type QueryShiftShardsTransactionFateReply struct {
	Commit bool
	Error  Err
}

// QueryShiftShardsTransactionFate queries the pending re-config transaction result in recovering.
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
			kv.endGoroutine(myID)
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
				kv.endGoroutine(myID)
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
