package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"6.824/util"
)
import "crypto/rand"
import "math/big"
import "6.824/shardmaster"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	me       int32
	// You will have to modify this struct.
}

// MakeClerk
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
var allocatedClient int32 = 0

func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.me = allocatedClient
	allocatedClient += 1
	// You'll have to add code here.
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientID = ck.me
	args.Timestamp = time.Now().UnixNano()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		util.ShardKVDebugLog("Client(%v) see shards availability %v in config#%v for request %+v",
			ck.me, ck.config.Shards, ck.config.Num, args)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si = (si + 1) % len(servers) {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				if ok && reply.Err == ErrRPCInternal {
					continue
				}
				if ok && reply.Err == ErrWrongLeader {
					continue
				}
				if !ok {
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// PutAppend
// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientID = ck.me
	args.Timestamp = time.Now().UnixNano()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		util.ShardKVDebugLog("Client(%v) see shards availability %v in config#%v for request %+v",
			ck.me, ck.config.Shards, ck.config.Num, args)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si = (si + 1) % len(servers) {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				util.ShardKVDebugLog("Client(%v) send %+v to server %v", ck.me, args, servers[si])
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				util.ShardKVDebugLog("Client(%v) get %+v from server %v for %+v", ck.me, reply, servers[si], args)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				if ok && reply.Err == ErrRPCInternal {
					continue
				}
				if ok && reply.Err == ErrWrongLeader {
					continue
				}
				if !ok {
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
