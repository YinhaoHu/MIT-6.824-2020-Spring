package shardmaster

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/util"
	"maps"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

type ShardMaster struct {
	mu      sync.Mutex
	cond    sync.Cond
	me      int
	rf      *raft.Raft
	dead    int32
	applyCh chan raft.ApplyMsg

	versions map[int32]int64
	configs  []Config // indexed by config num
}

type Op struct {
	Type        string // Supported: "Join", "Leave", "Move", "Query"
	ClientID    int32
	Timestamp   int64
	JoinServers *map[int][]string
	LeaveGIDs   *[]int
	MoveShard   int
	MoveGID     int
}

// Join new replica groups to the system and therefore causes the configuration evolves.
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	util.SMDebugLog("ShardMaster %v received %v in Join.", sm.me, *args)
	var op interface{} = Op{
		Type:        "Join",
		ClientID:    args.ClientID,
		Timestamp:   args.Timestamp,
		JoinServers: &args.Servers,
		LeaveGIDs:   nil,
		MoveShard:   0,
		MoveGID:     0,
	}
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
	} else {
		sm.lock()
		for sm.versions[args.ClientID] < args.Timestamp {
			util.KVRaftDebugLog("server %v waits in handling %v in ShardMaster.Join", sm.me, *args)
			sm.cond.Wait()
			util.KVRaftDebugLog("server %v wakes in handling %v in ShardMaster.Join", sm.me, *args)
			if sm.killed() {
				reply.WrongLeader = true
			}
			if !sm.rf.IsLeader() {
				isLeader = false
				break
			}
		}
		// Handle data.
		sm.unlock()
		if !isLeader {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
		// Handle other cases.
	}
	util.SMDebugLog("ShardMaster %v returned %v for %v", sm.me, *reply, *args)
}

// Leave replica groups from the system and therefore causes the configuration evolves.
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	util.SMDebugLog("ShardMaster %v received %v in Leave.", sm.me, *args)
	var op interface{} = Op{
		Type:        "Leave",
		ClientID:    args.ClientID,
		Timestamp:   args.Timestamp,
		JoinServers: nil,
		LeaveGIDs:   &args.GIDs,
		MoveShard:   0,
		MoveGID:     0,
	}
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
	} else {
		sm.lock()
		for sm.versions[args.ClientID] < args.Timestamp {
			util.KVRaftDebugLog("server %v waits in handling %v in ShardMaster.Leave", sm.me, *args)
			sm.cond.Wait()
			util.KVRaftDebugLog("server %v wakes in handling %v in ShardMaster.Leave", sm.me, *args)
			if sm.killed() {
				reply.WrongLeader = true
			}
			if !sm.rf.IsLeader() {
				isLeader = false
				break
			}
		}
		sm.unlock()
		if !isLeader {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
		// Handle other cases.
	}
	util.SMDebugLog("ShardMaster %v returned %v for %v", sm.me, *reply, *args)
}

// Move the specified shard to the specified new group.
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	var op interface{} = Op{
		Type:        "Move",
		ClientID:    args.ClientID,
		Timestamp:   args.Timestamp,
		JoinServers: nil,
		LeaveGIDs:   nil,
		MoveShard:   args.Shard,
		MoveGID:     args.GID,
	}
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
	} else {
		sm.lock()
		for sm.versions[args.ClientID] < args.Timestamp {
			util.KVRaftDebugLog("server %v waits in handling %v in ShardMaster.Move", sm.me, *args)
			sm.cond.Wait()
			util.KVRaftDebugLog("server %v wakes in handling %v in ShardMaster.Move", sm.me, *args)
			if sm.killed() {
				reply.WrongLeader = true
			}
			if !sm.rf.IsLeader() {
				isLeader = false
				break
			}
		}
		// Handle data.
		sm.unlock()
		if !isLeader {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
		// Handle other cases.
	}
	util.SMDebugLog("ShardMaster %v returned %v for %v", sm.me, *reply, *args)
}

// Query returns the configuration for the specified config num. If config num is -1 or
// bigger than the max config num of the shard master, the latest config will be returned.
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	var op interface{} = Op{
		Type:        "Query",
		ClientID:    args.ClientID,
		Timestamp:   args.Timestamp,
		JoinServers: nil,
		LeaveGIDs:   nil,
		MoveShard:   0,
		MoveGID:     0,
	}
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
	} else {
		sm.lock()
		for sm.versions[args.ClientID] < args.Timestamp {
			util.KVRaftDebugLog("server %v waits in handling %v in ShardMaster.Query", sm.me, *args)
			sm.cond.Wait()
			util.KVRaftDebugLog("server %v wakes in handling %v in ShardMaster.Query", sm.me, *args)
			if sm.killed() {
				reply.WrongLeader = true
			}
			if !sm.rf.IsLeader() {
				isLeader = false
				break
			}
		}
		configNum := args.Num
		if configNum < 0 || configNum >= len(sm.configs) {
			configNum = len(sm.configs) - 1
		}
		reply.Config = sm.configs[configNum]
		// Handle data.
		sm.unlock()
		if !isLeader {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
		// Handle other cases.
	}
	util.SMDebugLog("ShardMaster %v returned %v for %v", sm.me, *reply, *args)
}

// Kill is called by tester when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
}

// Raft needed by shard kv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) lock() {
	_, _, line, _ := runtime.Caller(1)
	util.SMDebugLog("ShardMaster %v try to acquire lock in line %v.", sm.me, line)
	sm.mu.Lock()
	util.SMDebugLog("ShardMaster %v acquired the lock in line %v.", sm.me, line)
}

func (sm *ShardMaster) unlock() {
	_, _, line, _ := runtime.Caller(1)
	sm.mu.Unlock()
	util.SMDebugLog("ShardMaster %v released the lock in line %v.", sm.me, line)
}

// return true if ShardMaster is killed, false otherwise.
func (sm *ShardMaster) killed() bool {
	return atomic.LoadInt32(&sm.dead) == 1
}

func (sm *ShardMaster) reassignLatestConfig() {
	shard := 0
	latestConfigNum := len(sm.configs) - 1
	if len(sm.configs[latestConfigNum].Groups) == 0 {
		for ; shard < NShards; shard++ {
			sm.configs[latestConfigNum].Shards[shard] = 0
		}
	} else {
		currentGIDS := make([]int, 0)
		for gid, _ := range sm.configs[latestConfigNum].Groups {
			currentGIDS = append(currentGIDS, gid)
		}
		sort.Ints(currentGIDS)
		for shard != NShards {
			for _, gid := range currentGIDS {
				sm.configs[latestConfigNum].Shards[shard] = gid
				shard++
				if shard == NShards {
					break
				}
			}
		}
	}
}

// processor is a goroutine that waits from apply channel of Raft and broadcast the RPC handlers.
func (sm *ShardMaster) processor() {
	for !sm.killed() {
		applyMsg := <-sm.applyCh
		operation := applyMsg.Command.(Op)
		sm.lock()
		if operation.Type == "Join" {
			if operation.Timestamp > sm.versions[operation.ClientID] {
				util.SMDebugLog("Process message - Join: %v", *operation.JoinServers)
				newConfig := Config{
					Num:    len(sm.configs),
					Shards: sm.configs[len(sm.configs)-1].Shards,
					Groups: maps.Clone(sm.configs[len(sm.configs)-1].Groups),
				}
				for gid, servers := range *operation.JoinServers {
					newConfig.Groups[gid] = servers
				}
				sm.configs = append(sm.configs, newConfig)
				sm.reassignLatestConfig()
			}
		} else if operation.Type == "Leave" {
			if operation.Timestamp > sm.versions[operation.ClientID] {
				util.SMDebugLog("Shard master %v starts processing message - Leave: %v", sm.me, *operation.LeaveGIDs)
				newConfig := Config{
					Num:    len(sm.configs),
					Shards: sm.configs[len(sm.configs)-1].Shards,
					Groups: make(map[int][]string),
				}
				for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
					gidShouldBeExclusive := false
					for _, exclusiveGID := range *operation.LeaveGIDs {
						if exclusiveGID == gid {
							gidShouldBeExclusive = true
							break
						}
					}
					if !gidShouldBeExclusive {
						newConfig.Groups[gid] = servers
					}
				}
				sm.configs = append(sm.configs, newConfig)
				sm.reassignLatestConfig()
				util.SMDebugLog("Shard master %v finished processing message - Leave: %v", sm.me, *operation.LeaveGIDs)
			}
		} else if operation.Type == "Move" {
			if operation.Timestamp > sm.versions[operation.ClientID] {
				newConfig := Config{
					Num:    len(sm.configs),
					Shards: sm.configs[len(sm.configs)-1].Shards,
					Groups: maps.Clone(sm.configs[len(sm.configs)-1].Groups),
				}
				newConfig.Shards[operation.MoveShard] = operation.MoveGID
				sm.configs = append(sm.configs, newConfig)
				util.SMDebugLog("Process message - Move: gid=%v,shard=%v", operation.MoveGID, operation.MoveShard)
			}
		} else if operation.Type == "Query" {

		} else {
			util.Assert(false, "ShardMaster.rpcProcessor meets unexpected operation type.")
		}
		if operation.Timestamp > sm.versions[operation.ClientID] {
			sm.versions[operation.ClientID] = operation.Timestamp
		}
		util.SMDebugLog("ShardMaster state: \n->versions=%v\n->configs=%v \n latestConfig=%v", sm.versions, sm.configs, sm.configs[len(sm.configs)-1])
		sm.unlock()
		sm.cond.Broadcast()
	}
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shard-master service.
// 'me' is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.versions = make(map[int32]int64)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.cond = sync.Cond{L: &sm.mu}

	go sm.processor()

	return sm
}
