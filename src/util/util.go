package util

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	raftDebug    = false // Debug flag, set true to allow debug log.
	kvRaftDebug  = false
	smDebug      = false
	shardKVDebug = true
	logToScreen  = false
	// Terminal font control characters
	reset  = "\033[0m"
	red    = "\033[31m"
	yellow = "\033[33m"
)

var debugLogMutex sync.Mutex
var logDirName string = "log"

// DebugLog logs the information in the debug mode.
//
// The log entry has the needed extra information, including
// caller function name and line number. Plus, this is thread safe in order to
// avoid the mess output in the concurrency sceanrio.

func getAvailablePort() int {
	// Listen on a random port to find an available port
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	defer func(listener net.Listener) {
		err = listener.Close()
		if err != nil {
			panic(err)
		}
	}(listener)

	// Get the address of the listener
	addr := listener.Addr().(*net.TCPAddr)

	// Return the port number
	return addr.Port
}

func init() {
	profileListenAddress := fmt.Sprintf("localhost:%v", getAvailablePort())
	go func() {
		fmt.Println(http.ListenAndServe(profileListenAddress, nil))
	}()
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	now := time.Now()
	fmt.Printf("~~~~~ Customized necessary information ~~~~~\n")
	fmt.Printf("pprof is initialized. visit in http://%v/debug/pprof/\n", profileListenAddress)
	fmt.Printf("Current time is %v:%v:%v\n", now.Hour(), now.Minute(), now.Second())
	if (raftDebug || kvRaftDebug || smDebug || shardKVDebug) && !logToScreen {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		logfileName := logDirName + "/" + strconv.Itoa(os.Getpid()) + ".log"
		fmt.Printf("Log file path = %v\n", logfileName)
		if _, err := os.Stat(logDirName); os.IsNotExist(err) {
			err = os.Mkdir(logDirName, 0755)
			if err != nil {
				panic("Create log directory error.")
			}
		}
		file, err := os.OpenFile(logfileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			panic("Open log file error.")
		}
		log.SetOutput(file)
	}
	fmt.Printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
}

// Assert prints error message if condition is not satisfied.
func Assert(condition bool, format string, a ...interface{}) {
	if !condition {
		rawMessage := fmt.Sprintf(format, a...)
		_, path, line, _ := runtime.Caller(1)
		pathNames := strings.Split(path, "/")
		panicString := fmt.Sprintf(red+"Assert (%v %v): %s"+reset, pathNames[len(pathNames)-1], line, rawMessage)
		panic(panicString)
	}
}

func debugLog(format string, a ...interface{}) {
	debugLogMutex.Lock()
	rawLog := fmt.Sprintf(format, a...)
	_, path, line, ok := runtime.Caller(2)
	pathNames := strings.Split(path, "/")
	file := pathNames[len(pathNames)-1]
	if !ok {
		panic("debug log fails.")
	}
	log.Printf("%v %v : %v\n", file, line, rawLog)
	debugLogMutex.Unlock()
}

func KVRaftDebugLog(format string, a ...interface{}) {
	if kvRaftDebug {
		debugLog(format, a...)
	}
}

func RaftDebugLog(format string, a ...interface{}) {
	if raftDebug {
		debugLog(format, a...)
	}
}

func SMDebugLog(format string, a ...interface{}) {
	if smDebug {
		debugLog(format, a...)
	}
}

func ShardKVDebugLog(format string, a ...interface{}) {
	if shardKVDebug {
		debugLog(format, a...)
	}
}

// MinInt returns the min value between x and y.
//
// This function is DEPRECATED and used for the old code before Go 1.22.
func MinInt(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}
