package mr

import (
	"fmt"
	"log"
	"runtime"
	"sync"
)

const (
	debug = false // Debug flag, set true to allow debug log.
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
