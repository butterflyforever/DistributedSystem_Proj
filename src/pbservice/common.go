package pbservice

import "hash/fnv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	Id      int64
	Viewnum uint

	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id      int64
	Viewnum uint
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type DataBackupArgs struct {
	Database map[string]string
	Record   map[int64]string
	Viewnum  uint
}

type DataBackupReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
