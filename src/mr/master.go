package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const TaskTimeSecondLimit = 10

type Coordinator struct {
	MapPartitions []string
	MapStatus     []TaskStatus // 0: ready, 1: executing, 2: done
	MapTotal      int
	MapCount      int
	ReduceStatus  []TaskStatus // 0: ready, 1: executing, 2: done
	ReduceTotal   int
	ReduceCount   int
	Mutex         sync.Mutex
}

var master Coordinator

// monitorTask monitors the executing time of one task. If time is greater than 10 sec,
// mark the status of this task to be pending.
func (c *Coordinator) monitorTask(taskType TaskType, taskID int) {
	time.Sleep(TaskTimeSecondLimit * time.Second)
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	switch taskType {
	case MapTask:
		if c.MapStatus[taskID] != Done {
			c.MapStatus[taskID] = Pending
		}
	case ReduceTask:
		if c.ReduceStatus[taskID] != Done {
			c.ReduceStatus[taskID] = Pending
		}
	}
}

// GetTask get an available task.
//
// Error would occur if and only if there is no task to do.
// For Map task, input is the filename to map, id is the number of the map task.
// For Reduce task, input the total number of map which is used to get a list of
// input file names, id is the number of the reduce task which is also needed to be
// used to get the file names.
func (c *Coordinator) GetTask(arg *Arg, task *Task) error {
	master.Mutex.Lock()
	defer master.Mutex.Unlock()
	if master.MapTotal != master.MapCount {
		for i := 0; i < master.MapTotal; i++ {
			if master.MapStatus[i] == Pending {
				task.Input = master.MapPartitions[i]
				task.ID = i
				task.Type = MapTask
				task.Fetched = true
				task.NReduce = master.ReduceTotal
				master.MapStatus[i] = Executing
				go c.monitorTask(task.Type, task.ID)
				return nil
			}
		}
		task.Fetched = false
		return nil
	} else if master.ReduceTotal != master.ReduceCount {
		for i := 0; i < master.ReduceTotal; i++ {
			if master.ReduceStatus[i] == Pending {
				task.Input = strconv.Itoa(master.MapTotal)
				task.ID = i
				task.Type = ReduceTask
				task.NReduce = master.ReduceTotal
				task.Fetched = true
				master.ReduceStatus[i] = Executing
				go c.monitorTask(task.Type, task.ID)
				return nil
			}
		}
		task.Fetched = false
		return nil
	} else {
		return fmt.Errorf("there is no task to do, you should quit")
	}
}

// FinishTask Notice the coordinator that the specific task is done.
// For simplicity, no error would occur.
func (c *Coordinator) FinishTask(task *Task, arg *Arg) error {
	master.Mutex.Lock()
	defer master.Mutex.Unlock()
	if task.Type == MapTask {
		if master.MapStatus[task.ID] != Done {
			master.MapCount++
			master.MapStatus[task.ID] = Done
		}
	} else {
		if master.ReduceStatus[task.ID] != Done {
			master.ReduceCount++
			master.ReduceStatus[task.ID] = Done
		}
	}
	return nil
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	done := (c.MapTotal == c.MapCount) && (c.ReduceTotal == c.ReduceCount)
	DebugLog("map count = %v/%v reduce count = %v/%v", c.MapCount, c.MapTotal, c.ReduceCount, c.ReduceTotal)
	return done
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	master = Coordinator{MapTotal: len(files), MapCount: 0, MapStatus: make([]TaskStatus, len(files)), MapPartitions: files,
		ReduceCount: 0, ReduceTotal: nReduce, ReduceStatus: make([]TaskStatus, nReduce)}
	master.server()
	return &master
}

// server starts a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	_ = os.Remove(coordinatorSock())
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listne error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Fatalf("Coordinator: %v", err)
		}
	}()
}
