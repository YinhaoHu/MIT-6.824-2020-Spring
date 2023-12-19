package raft

import (
	"fmt"
	"log"
	"runtime"
	"sync"
)

const (
	debug = false // Debug flag, set true to allow debug log.
	// Terminal font control characters
	reset  = "\033[0m"
	red    = "\033[31m"
	yellow = "\033[33m"
)

var debugLogMutex sync.Mutex

// DebugLog logs the information in the debug mode.
//
// The log entry has the needed extra information, including
// caller function name and line number. Plus, this is thread safe in order to
// avoid the mess output in the concurrency sceanrio.

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

// Assert prints error message if condition is not satisfied.
func Assert(condition bool, format string, a ...interface{}) {
	if !condition {
		rawMessage := fmt.Sprintf(format, a...)
		_, _, line, _ := runtime.Caller(1)
		log.Fatalf(red+"Assert (line %v): %s"+reset, line, rawMessage)
	}
}

func PrintCheckPoint(format string, a ...interface{}) {
	if debug {
		rawMessage := fmt.Sprintf(format, a...)
		log.Printf(yellow+"%s"+reset, rawMessage)
	}
}

func DebugLog(format string, a ...interface{}) {
	if debug {
		rawLog := fmt.Sprintf(format, a...)
		// If I need.
		_, _, line, ok := runtime.Caller(1)
		if !ok {
			panic("debug log fails.")
		}
		debugLogMutex.Lock()
		log.Printf("%v : %v\n", line, rawLog)
		debugLogMutex.Unlock()
	}
}

func MinInt(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}
