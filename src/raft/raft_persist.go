package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
)

func (rf *Raft) persistStateAndSnapshot() {
	stateBuffer := new(bytes.Buffer)
	stateEncoder := labgob.NewEncoder(stateBuffer)
	err := stateEncoder.Encode(rf.CurrentTerm)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	err = stateEncoder.Encode(rf.VotedFor)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	err = stateEncoder.Encode(rf.Log)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	stateData := stateBuffer.Bytes()

	snapshotBuffer := new(bytes.Buffer)
	snapshotEncoder := labgob.NewEncoder(snapshotBuffer)
	err = snapshotEncoder.Encode(rf.snapshot)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	snapshotData := snapshotBuffer.Bytes()
	rf.persister.SaveStateAndSnapshot(stateData, snapshotData)
}

// save Raft's persistent state to stable Storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Store : 1)log, 2)current term and 3)voted for.
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	err := encoder.Encode(rf.CurrentTerm)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	err = encoder.Encode(rf.VotedFor)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	err = encoder.Encode(rf.Log)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int
	var votedFor int
	var logentries []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logentries) != nil {
		log.Fatalf("error in read from persister")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = logentries
	}
}
