package mr

import (
	"os"
	"strconv"
)

type TaskStatus int

const (
	Pending TaskStatus = iota
	Executing
	Done
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type Task struct {
	Input   string
	Type    TaskType // "map" or "reduce"
	ID      int
	NReduce int
	Fetched bool // if false, the worker needs to wait for a while.
}

// Arg is used to be a placeholder in the rpc call only.
type Arg struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
