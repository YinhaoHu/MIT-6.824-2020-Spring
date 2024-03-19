package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrServerKilled = "ErrServerKilled"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientID  int32
	Timestamp int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientID  int32
	Timestamp int64
}

type GetReply struct {
	Err   Err
	Value string
}
