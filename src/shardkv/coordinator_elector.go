package shardkv

import "sync"

type CoordinatorElectionArg struct {
	NewConfigNum int
	Participant  int // participant group id.
}

type CoordinatorElectionReply struct {
	Coordinator int
}

type CoordinatorElector struct {
	history map[int]int // C# -> GID
	mutex   sync.Mutex
}

var coordinatorElector CoordinatorElector

func init() {
	coordinatorElector.history = make(map[int]int)
}

func (ce *CoordinatorElector) Remove(gid int) {
	ce.mutex.Lock()
	for k, v := range ce.history {
		if v == gid {
			delete(ce.history, k)
		}
	}
	ce.mutex.Unlock()
}

func (ce *CoordinatorElector) Participate(arg CoordinatorElectionArg) CoordinatorElectionReply {
	ce.mutex.Lock()
	coordinatorID, exist := ce.history[arg.NewConfigNum]
	if !exist {
		coordinatorID = arg.Participant
		ce.history[arg.NewConfigNum] = arg.Participant
	}
	ce.mutex.Unlock()
	return CoordinatorElectionReply{Coordinator: coordinatorID}
}
