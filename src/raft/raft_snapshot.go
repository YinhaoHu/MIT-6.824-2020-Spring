package raft

import (
	"6.824/labgob"
	"6.824/util"
	"bytes"
	"time"
)

type SnapshotData any

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              SnapshotData
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int // Logical Index
	LastIncludedTerm  int
	Data              SnapshotData
	// Fields `Done` and `Offset` in the paper are ignored.
}

type InstallSnapshotReply struct {
	Term int //currentTerm, for leader to update itself
}

// broadcasts the snapshot to all followers. Thread safety should be guaranteed by caller.
func (rf *Raft) broadCastSnapshot() {
	if rf.snapshotDisabled {
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.snapshotArg.Term,
		LeaderID:          rf.snapshotArg.LeaderID,
		LastIncludedIndex: rf.snapshotArg.LastIncludedIndex,
		LastIncludedTerm:  rf.snapshotArg.LastIncludedTerm,
		Data:              rf.snapshot.Data,
	}
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			go func(server int) {
				for !rf.killed() {
					var reply InstallSnapshotReply
					ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
					util.RaftDebugLog("Leader %v received %+v for message in broadcastSnapshot.", rf.me, reply)
					if ok {
						lockID := rf.lock()
						if args.Term > rf.CurrentTerm && rf.state == LeaderState {
							rf.convertToFollower()
							rf.persist()
							util.RaftDebugLog("leader %v convert to follower", rf.me)
						} else if rf.nextIndex[server] < args.LastIncludedIndex {
							rf.nextIndex[server] = args.LastIncludedIndex + 1
							rf.matchIndex[server] = args.LastIncludedIndex
						}
						if rf.snapshotPoint[server] < args.LastIncludedIndex {
							rf.snapshotPoint[server] = args.LastIncludedIndex
						}
						util.RaftDebugLog("Leader %v updated rf.snapshotPoint[%v]=%v", rf.me, server, args.LastIncludedIndex)
						rf.unlock(lockID)
						break
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.snapshotDisabled {
		return
	}
	lockID := rf.lock()
	util.RaftDebugLog("server %v received InstallSnapshot(args=%+v) \nCurrentLog[LLL=%v,LPL=%v]:%v", rf.me, *args,
		rf.logicalIndex(len(rf.Log)), len(rf.Log),
		rf.Log)
	reply.Term = rf.CurrentTerm
	if args.Term >= rf.CurrentTerm && rf.state == LeaderState {
		rf.convertToFollower()
		rf.unlock(lockID)
		return
	}
	if args.Term < rf.CurrentTerm {
		rf.unlock(lockID)
		return
	}
	rf.lastHeartBeatTime = time.Now()

	// If RPC request or response contains term T > CurrentTerm:
	// set CurrentTerm = T, convert to follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.persist()
		if rf.state != FollowerState {
			rf.convertToFollower()
		}
	}

	// Compact the log as needed.
	if rf.snapshot.LastIncludedIndex < args.LastIncludedIndex {
		if len(rf.Log) > rf.physicalIndex(args.LastIncludedIndex)+1 {
			rf.Log = append(rf.Log[:1], rf.Log[rf.physicalIndex(args.LastIncludedIndex)+1:]...)
		} else {
			rf.Log = rf.Log[:1]
		}
		rf.Log[0].Term = args.LastIncludedTerm
		rf.snapshot.Data = args.Data
		rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
		rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
		rf.snapshotArg = *args
		if rf.commitIndex < args.LastIncludedIndex {
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = rf.commitIndex
			rf.applyChannel <- ApplyMsg{
				SnapshotValid: true,
				SnapshotData:  args.Data,
			}
		}
		rf.persistStateAndSnapshot()
	}
	util.RaftDebugLog("server %v finished InstallSnapshot(snapshot=%+v,reply=%+v)\nCurrentLog[LLL=%v,LPL=%v]=%v,snapshotArg=%+v",
		rf.me, rf.snapshot,
		*reply, rf.logicalIndex(len(rf.Log)), len(rf.Log), rf.Log, rf.snapshotArg)
	rf.unlock(lockID)
}

// MakeSnapshot makes snapshot when server needs.
func (rf *Raft) makeSnapshot(snapshot *Snapshot) bool {
	util.RaftDebugLog("Raft.makeSnapshot: server %v try to make snapshot", rf.me)
	if rf.state != LeaderState {
		util.RaftDebugLog("Raft.MakeSnapshot: server %v did not make snapshot ",
			rf.me)
		return false
	}
	util.RaftDebugLog("Raft.makeSnapshot: before log[LLL=%v,LPL=%v,commitIndex=%v] = %v", rf.logicalIndex(len(rf.Log)),
		len(rf.Log), rf.commitIndex, rf.Log)
	snapshot.LastIncludedTerm = rf.Log[rf.physicalIndex(snapshot.LastIncludedIndex)].Term
	rf.Log = append(rf.Log[0:1], rf.Log[rf.physicalIndex(snapshot.LastIncludedIndex)+1:]...)
	rf.replicaCount = append(rf.replicaCount[0:1], rf.replicaCount[rf.physicalIndex(snapshot.LastIncludedIndex)+1:]...)
	rf.snapshot = *snapshot
	rf.Log[0].Term = rf.snapshot.LastIncludedTerm
	rf.persistStateAndSnapshot()
	util.RaftDebugLog("Raft.MakeSnapshot: server %v made snapshot[LLL=%v,LPL=%v](%+v)\n Current Log: %+v", rf.me,
		len(rf.Log)+rf.snapshot.LastIncludedIndex, len(rf.Log), rf.snapshot, rf.Log)
	rf.snapshotArg = InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.snapshot.LastIncludedIndex,
		LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
		Data:              snapshot.Data,
	}
	return true
}

// GetSnapshot sends Storage snapshot to the upper level when raft module starts.
func (rf *Raft) GetSnapshot() (SnapshotData, bool) {
	if rf.snapshot.LastIncludedIndex == 0 {
		var snapshotData SnapshotData
		return snapshotData, false
	}
	return rf.snapshot.Data, true
}

func (rf *Raft) readSnapshot() {
	buffer := bytes.NewBuffer(rf.persister.ReadSnapshot())
	decoder := labgob.NewDecoder(buffer)
	err := decoder.Decode(&rf.snapshot)
	if err != nil {
		if err.Error() != "EOF" {
			panic("Raft.GetSnapshot decode error")
		} else {
			return
		}
	}
	rf.Log[0].Term = rf.snapshot.LastIncludedTerm
	rf.commitIndex = rf.snapshot.LastIncludedIndex
	rf.lastApplied = rf.snapshot.LastIncludedIndex
	rf.snapshotArg = InstallSnapshotArgs{rf.CurrentTerm, rf.me, rf.snapshot.LastIncludedIndex,
		rf.snapshot.LastIncludedTerm, rf.snapshot.Data}
	util.KVRaftDebugLog("server %v reported snapshot. Snapshot:%v", rf.me, rf.snapshot)
}

func (rf *Raft) InitializeSnapshot(workCh chan Snapshot, doneCh chan int) {
	rf.snapshotDisabled = false
	rf.snapshotChannel = workCh
	rf.snapshotDoneCh = doneCh
	go func() {
		for !rf.killed() {
			snapshot := <-rf.snapshotChannel
			lockID := rf.lock()
			if rf.snapshot.LastIncludedIndex >= snapshot.LastIncludedIndex {
				rf.snapshotDoneCh <- 1
			} else {
				made := rf.makeSnapshot(&snapshot)
				rf.snapshotDoneCh <- 1
				if made {
					rf.broadCastSnapshot()
				}
			}
			rf.unlock(lockID)
		}
	}()
}
