package shardmaster

import (
	"slices"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gid-s) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// NShards is the number of shards.
const NShards = 10

// Config is a configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func CloneConfig(config *Config) Config {
	newMap := make(map[int][]string)
	for k, v := range config.Groups {
		newMap[k] = slices.Clone(v)
	}
	newConfig := Config{config.Num, config.Shards, newMap}
	return newConfig
}

func (cfg *Config) Clone() *Config {
	newConfig := new(Config)
	newMap := make(map[int][]string)
	for k, v := range cfg.Groups {
		newMap[k] = slices.Clone(v)
	}
	newConfig.Num = cfg.Num
	newConfig.Groups = newMap
	newConfig.Shards = cfg.Shards
	return newConfig
}

// HasGroup checks whether the group is in the config or not.
func (cfg *Config) HasGroup(gid int) bool {
	_, hasGroup := cfg.Groups[gid]
	return hasGroup
}

// Len returns the number of groups in the config.
func (cfg *Config) Len() int {
	return len(cfg.Groups)
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	ClientID  int32
	Timestamp int64
	Servers   map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClientID  int32
	Timestamp int64
	GIDs      []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClientID  int32
	Timestamp int64
	Shard     int
	GID       int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	ClientID  int32
	Timestamp int64
	Num       int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
