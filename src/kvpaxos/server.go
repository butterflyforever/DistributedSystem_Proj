package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0
const GET = "GET"
const PUT = "PUT"
const PUTHASH = "PUTHASH"
const RELEASE = "RELEASE"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpID          int64
	OpType        string
	Key           string
	Value         string
	PreviousValue string
	ReleaseID     int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	history   map[int64]*CachedInfo
	data      map[string]string
	curMaxSeq int
}

type CachedInfo struct {
	Value string
	Seq   int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// filter
	cachedValue, isCached := kv.history[args.Id]
	if isCached {
		// if ErrNoKey, also let Err=OK
		reply.Err = OK
		reply.Value = cachedValue.Value

		//debug
		DPrintf("CachedGet id=%d, key=%s, value=%s, me=%d\n",
			args.Id, args.Key, cachedValue.Value, kv.me)

		return nil
	}

	op := Op{}
	op.OpType = GET
	op.OpID = args.Id
	op.Key = args.Key

	for {
		kv.curMaxSeq++
		seq := kv.curMaxSeq

		// debug
		DPrintf("Get id=%d, key=%s, me=%d, seq=%d\n", op.OpID, op.Key, kv.me, seq)

		kv.px.Start(seq, op)
		v := kv.waitForDecided(seq)

		// debug
		DPrintf("GetMakeup vid=%d, vtype=%s, vkey=%s, vvalue=%s, vpre=%s, me=%d, seq=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, v.PreviousValue, kv.me, seq)

		if v.OpID == op.OpID {
			// reach the agreement
			curValue, curEx := kv.data[op.Key]
			if curEx == false {
				reply.Err = ErrNoKey
				reply.Value = ""
				v.Value = ""
			} else {
				reply.Err = OK
				reply.Value = curValue
				v.Value = curValue
			}

			cachedInfo := CachedInfo{}
			cachedInfo.Seq = seq
			cachedInfo.Value = curValue
			kv.history[op.OpID] = &cachedInfo
			// kv.releaseMem(seq)
			kv.px.Done(seq)
			break
		} else {
			cachedInfo := CachedInfo{}
			cachedInfo.Seq = seq
			// update the database
			if v.OpType == PUT {
				kv.data[v.Key] = v.Value

				cachedInfo.Value = v.Value
				kv.history[v.OpID] = &cachedInfo
			} else if v.OpType == PUTHASH {
				preValue, preEx := kv.data[v.Key]
				if preEx == false {
					preValue = ""
				}
				kv.data[v.Key] = strconv.FormatUint(uint64(hash(preValue+v.Value)), 10)
				cachedInfo.Value = preValue
				kv.history[v.OpID] = &cachedInfo
			} else if v.OpType == GET {
				cachedInfo.Value = kv.data[v.Key]
				kv.history[v.OpID] = &cachedInfo
			} else {
				// Release
				// No need to remove repeated
				kv.history[v.ReleaseID].Value = ""
				// delete(kv.history, v.ReleaseID)
			}
		}

	}

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// filter
	cachedValue, isCached := kv.history[args.Id]
	if isCached {
		reply.Err = OK
		reply.PreviousValue = cachedValue.Value

		//debug
		DPrintf("CachedPut id=%d, dohash=%t, key=%s, value=%s, preValue=%s, me=%d\n",
			args.Id, args.DoHash, args.Key, args.Value, cachedValue.Value, kv.me)

		return nil
	}

	op := Op{}
	op.OpID = args.Id
	op.Key = args.Key
	op.Value = args.Value

	if args.DoHash {
		op.OpType = PUTHASH
	} else {
		op.OpType = PUT
	}

	for {
		kv.curMaxSeq++
		seq := kv.curMaxSeq

		// debug
		DPrintf("Put id=%d, type=%s, key=%s, value=%s, me=%d, seq=%d\n",
			op.OpID, op.OpType, op.Key, op.Value, kv.me, seq)

		kv.px.Start(seq, op)
		v := kv.waitForDecided(seq)
		cachedInfo := CachedInfo{}
		cachedInfo.Seq = seq

		// debug
		DPrintf("PutMakeup id=%d, type=%s, key=%s, value=%s, preValue=%s, me=%d, seq=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, v.PreviousValue, kv.me, seq)

		if v.OpID == op.OpID {
			// reach the agreement

			if args.DoHash {
				preValue, preEx := kv.data[op.Key]
				if preEx == false {
					preValue = ""
				}
				kv.data[op.Key] = strconv.FormatUint(uint64(hash(preValue+op.Value)), 10)

				reply.Err = OK
				reply.PreviousValue = preValue

				// save to history
				// v.PreviousValue = preValue
				cachedInfo.Value = preValue
				kv.history[op.OpID] = &cachedInfo
			} else {
				kv.data[op.Key] = op.Value
				reply.Err = OK
				reply.PreviousValue = ""

				// save to history
				// v.PreviousValue = ""
				cachedInfo.Value = ""
				kv.history[op.OpID] = &cachedInfo
			}
			// kv.releaseMem(seq)
			kv.px.Done(seq)
			break
		} else {
			// update the database
			if v.OpType == PUT {
				kv.data[v.Key] = v.Value
				cachedInfo.Value = v.Value
				kv.history[v.OpID] = &cachedInfo
			} else if v.OpType == PUTHASH {
				preValue, preEx := kv.data[v.Key]
				if preEx == false {
					preValue = ""
				}
				kv.data[v.Key] = strconv.FormatUint(uint64(hash(preValue+v.Value)), 10)
				cachedInfo.Value = preValue
				kv.history[v.OpID] = &cachedInfo
			} else if v.OpType == GET {
				cachedInfo.Value = kv.data[v.Key]
				kv.history[v.OpID] = &cachedInfo
			} else {
				// Release
				// No need to remove repeated
				kv.history[v.ReleaseID].Value = ""
				// delete(kv.history, v.ReleaseID)
			}
		}
	}

	return nil
}

func (kv *KVPaxos) waitForDecided(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, v := kv.px.Status(seq)
		if decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) releaseMem(seq int) {
	kv.px.Done(seq)
	// forget := make([]int64, 0)
	for id, op := range kv.history {
		if op.Seq <= seq {
			kv.history[id].Value = ""
		}
	}
}

func (kv *KVPaxos) Release(args *RelArgs, reply *RelReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{}
	op.OpType = RELEASE
	op.OpID = args.Id
	op.ReleaseID = args.ReleaseId

	for {
		kv.curMaxSeq++
		seq := kv.curMaxSeq

		// debug
		DPrintf("Release id=%d, releaseId=%d, me=%d, seq=%d\n", op.OpID, op.ReleaseID, kv.me, seq)

		kv.px.Start(seq, op)
		v := kv.waitForDecided(seq)

		// debug
		DPrintf("ReleaseMakeup vid=%d, vtype=%s, vkey=%s, vvalue=%s, vpre=%s, me=%d, seq=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, v.PreviousValue, kv.me, seq)

		if v.OpID == op.OpID {
			// reach the agreement
			// delete(kv.history, op.ReleaseID)
			kv.history[v.ReleaseID].Value = ""
			reply.Err = OK
			kv.px.Done(seq)
			break
		} else {
			cachedInfo := CachedInfo{}
			cachedInfo.Seq = seq
			// update the database
			if v.OpType == PUT {
				kv.data[v.Key] = v.Value

				cachedInfo.Value = v.Value
				kv.history[v.OpID] = &cachedInfo
			} else if v.OpType == PUTHASH {
				preValue, preEx := kv.data[v.Key]
				if preEx == false {
					preValue = ""
				}
				kv.data[v.Key] = strconv.FormatUint(uint64(hash(preValue+v.Value)), 10)
				cachedInfo.Value = preValue
				kv.history[v.OpID] = &cachedInfo
			} else if v.OpType == GET {
				cachedInfo.Value = kv.data[v.Key]
				kv.history[v.OpID] = &cachedInfo
			} else {
				// Release
				// No need to remove repeated
				// delete(kv.history, v.ReleaseID)
				kv.history[v.ReleaseID].Value = ""
			}
		}

	}

	return nil

}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.history = make(map[int64]*CachedInfo)
	kv.data = make(map[string]string)
	kv.curMaxSeq = -1

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
