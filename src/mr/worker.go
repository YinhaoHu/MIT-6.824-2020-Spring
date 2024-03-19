package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey used for sort
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// ihash is used in ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func handleMap(task *Task, fun func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal(err)
	}
	result := fun(task.Input, string(content))

	nReduce := task.NReduce
	fileNames := make([]string, nReduce)
	files := make([]*os.File, nReduce)
	jsons := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		fileNames[i] = fmt.Sprintf("mr-%d-%d", task.ID, i)
		files[i], err = ioutil.TempFile("", fmt.Sprintf("temp-%s", fileNames[i]))
		jsons[i] = json.NewEncoder(files[i])
	}
	for _, pair := range result {
		r := ihash(pair.Key) % nReduce
		DebugLog("Map output: %v to file %v", pair, fileNames[r])
		err = jsons[r].Encode(&pair)
		if err != nil {
			log.Fatal(err)
		}
	}
	for i := 0; i < nReduce; i++ {
		_ = os.Rename(files[i].Name(), fileNames[i])
		_ = os.Chmod(files[i].Name(), 0755)
		_ = files[i].Close()
	}
}

func handleReduce(task *Task, fun func(string, []string) string) {
	nMap, err := strconv.Atoi(task.Input)
	if err != nil {
		log.Fatal(err)
	}
	outputFileName := fmt.Sprintf("mr-out-%d", task.ID)
	outputFile, err := os.CreateTemp("", fmt.Sprintf("temp-%s", outputFileName))

	mapping := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inputFileName := fmt.Sprintf("mr-%d-%d", i, task.ID)
		inputFile, _ := os.OpenFile(inputFileName, os.O_RDONLY, 0755)
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			mapping[kv.Key] = append(mapping[kv.Key], kv.Value)
		}
		_ = inputFile.Close()
	}
	output := make([]KeyValue, 0)
	for k, v := range mapping {
		kv := KeyValue{k, fun(k, v)}
		output = append(output, kv)
	}
	sort.Sort(ByKey(output))
	debugOutput := string("")
	for _, kv := range output {
		_, _ = fmt.Fprintf(outputFile, "%v %v\n", kv.Key, kv.Value)
		debugOutput = fmt.Sprintf("%s %s %s\n", debugOutput, kv.Key, kv.Value)
	}
	_ = os.Rename(outputFile.Name(), outputFileName)
	DebugLog("debug output in file %s: %s", outputFileName, debugOutput)
	_ = os.Chmod(outputFile.Name(), 0755)
	_ = outputFile.Close()
}

// Worker is called by main/mrworker.go
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	DebugLog("worker %v starts", os.Getpid())
	for {
		var arg Arg
		var task Task
		err := call("Coordinator.GetTask", &arg, &task)
		if err != nil {
			os.Exit(0)
		}
		if task.Fetched {
			switch task.Type {
			case MapTask:
				handleMap(&task, mapf)
			case ReduceTask:
				handleReduce(&task, reducef)
			}
			_ = call("Coordinator.FinishTask", &task, &arg)
			DebugLog("worker %v finished task:%+v", os.Getpid(), task)
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

// call sends an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	err = c.Call(rpcname, args, reply)
	_ = c.Close()
	return err
}
