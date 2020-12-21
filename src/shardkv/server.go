package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 1
const GET = "GET"
const PUT = "PUT"
const PUTHASH = "PUTHASH"
const SEND = "SEND"
const RECEIVE = "RECEIVE"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	OpID          int64
	OpType        string
	Key           string
	Value         string
	PreviousValue string

	// for receive
	ConfigNum    int
	ShardNum     int
	Shard        map[string]string
	HistoryShard map[int64]string

	// for send
	ShardNumsToSend []int
	TargetServers   [][]string
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	history   [shardmaster.NShards]map[int64]string //id -> cache
	shards    [shardmaster.NShards]map[string]string
	isHold    [shardmaster.NShards]bool
	curMaxSeq int
	isInitial bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{}
	op.OpType = GET
	op.OpID = args.Id
	op.Key = args.Key

	for {
		// filter
		cachedValue, isCached := kv.history[key2shard(args.Key)][args.Id]
		if isCached {
			reply.Err = OK
			reply.Value = cachedValue

			//debug
			DPrintf("CachedGet id=%d, key=%s, cachedValue=%s, me=%d, gid=%v\n",
				args.Id, args.Key, cachedValue, kv.me, kv.gid)

			return nil
		}

		kv.curMaxSeq++
		seq := kv.curMaxSeq

		// debug
		DPrintf("Get id=%d, type=%v, key=%s, me=%d, gid=%d, seq=%d\n",
			op.OpID, op.OpType, op.Key, kv.me, kv.gid, seq)

		kv.px.Start(seq, op)
		v := kv.waitForDecided(seq)

		// debug
		DPrintf("GetMakeup vid=%d, vtype=%s, vkey=%s, vvalue=%s, vpre=%s, vconfigNum=%d, vShardNum=%v, vShardNumsToSend=%v, me=%d, gid=%d, seq=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, v.PreviousValue, v.ConfigNum, v.ShardNum, v.ShardNumsToSend, kv.me, kv.gid, seq)

		if v.OpID == op.OpID {
			// reach the agreement
			kv.GetHelper(&v, reply)
			// debug
			DPrintf("GetHelper result vid=%d, vtype=%s, vkey=%s, replyErr=%v, replyvalue=%v, me=%d, gid=%d, seq=%d\n",
				v.OpID, v.OpType, v.Key, reply.Err, reply.Value, kv.me, kv.gid, seq)
			kv.px.Done(seq)
			break
		} else {
			kv.catchUp(&v)
		}

	}

	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
		// filter
		cachedValue, isCached := kv.history[key2shard(args.Key)][args.Id]
		if isCached {
			reply.Err = OK
			reply.PreviousValue = cachedValue

			//debug
			DPrintf("CachedPut id=%d, dohash=%t, key=%s, value=%s, preValue=%s, me=%d, gid=%d\n",
				args.Id, args.DoHash, args.Key, args.Value, cachedValue, kv.me, kv.gid)

			return nil
		}

		kv.curMaxSeq++
		seq := kv.curMaxSeq

		// debug
		DPrintf("Put id=%d, type=%s, key=%s, value=%s, me=%d, gid=%d, seq=%d\n",
			op.OpID, op.OpType, op.Key, op.Value, kv.me, kv.gid, seq)

		kv.px.Start(seq, op)
		v := kv.waitForDecided(seq)

		// debug
		DPrintf("PutMakeup vid=%d, vtype=%s, vkey=%s, vvalue=%s, vpre=%s, vconfigNum=%d, vShardNum=%v, vShardNumsToSend=%v, me=%d, gid=%d, seq=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, v.PreviousValue, v.ConfigNum, v.ShardNum, v.ShardNumsToSend, kv.me, kv.gid, seq)

		if v.OpID == op.OpID {
			// reach the agreement
			if !args.DoHash {
				// do put
				kv.PutHelper(&v, reply)
			} else {
				// do puthash
				kv.PutHashHelper(&v, reply)
			}
			// debug
			DPrintf("PutHashHelper result vid=%d, vtype=%s, vkey=%s, vvalue=%s, replyErr=%v, replyvalue=%v, me=%d, gid=%d, seq=%d\n",
				v.OpID, v.OpType, v.Key, v.Value, reply.Err, reply.PreviousValue, kv.me, kv.gid, seq)
			kv.px.Done(seq)
			break

		} else {
			kv.catchUp(&v)
		}
	}

	return nil
}

func (kv *ShardKV) Receive(args *ReceiveArgs, reply *ReceiveReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{}
	op.OpType = RECEIVE
	op.OpID = args.Id
	op.ShardNum = args.ShardNum

	// copy it
	// op.Shard = make(map[string]string)
	// for k, v := range args.Shard {
	// 	op.Shard[k] = v
	// }

	op.Shard = args.Shard
	op.HistoryShard = args.History

	// op.HistoryShard = make(map[int64]string)
	// for k, v := range args.History {
	// 	op.HistoryShard[k] = v
	// }

	for {
		// filter
		// args.Id should be viewNum + shards num
		_, isCached := kv.history[args.ShardNum][args.Id]
		if isCached {
			reply.Err = OK

			//debug
			DPrintf("CachedReceive id=%d, shardNum=%v, me=%d, gid=%d\n",
				args.Id, args.ShardNum, kv.me, kv.gid)

			return nil
		}

		kv.curMaxSeq++
		seq := kv.curMaxSeq

		// debug
		DPrintf("Receive id=%d, type=%v, ConfigNum=%v, shardNum=%v, shardHistory=%v, shard=%v, me=%d, gid=%d, seq=%d\n",
			op.OpID, op.OpType, op.ConfigNum, op.ShardNum, op.HistoryShard, op.Shard, kv.me, kv.gid, seq)

		kv.px.Start(seq, op)
		v := kv.waitForDecided(seq)

		// debug
		DPrintf("ReceiveMakeup vid=%d, vtype=%s, vkey=%s, vvalue=%s, vpre=%s, vconfigNum=%d, vShardNum=%v, vShardNumsToSend=%v, me=%d, gid=%d, seq=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, v.PreviousValue, v.ConfigNum, v.ShardNum, v.ShardNumsToSend, kv.me, kv.gid, seq)

		if v.OpID == op.OpID {
			// reach the agreement
			// do receive
			kv.ReceiveHelper(&v, reply)
			kv.px.Done(seq)
			break
		} else {
			kv.catchUp(&v)
		}

	}

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// No need to filter

	// check if it need to send
	config := kv.sm.Query(-1)
	// init state
	if config.Num == 0 {
		return
	}

	shardNumsToSend := make([]int, 0)
	targetServers := make([][]string, 0)
	for i, gid := range config.Shards {
		if gid != kv.gid && kv.isHold[i] {
			shardNumsToSend = append(shardNumsToSend, i)
			targetServers = append(targetServers, config.Groups[gid])
		}
	}

	if !kv.isInitial && len(shardNumsToSend) == 0 {
		return
	}

	op := Op{}
	op.OpType = SEND
	// op.OpID = int64(config.Num)<<10 + int64(shardNum)
	// Do i need to make them unique?
	op.OpID = nrand()
	op.ConfigNum = config.Num
	op.ShardNumsToSend = shardNumsToSend
	op.TargetServers = targetServers

	for {
		kv.curMaxSeq++
		seq := kv.curMaxSeq

		// debug
		DPrintf("Send id=%d, shardNum=%v, configNum=%d, me=%d, gid=%d, seq=%d\n", op.OpID, op.ShardNum, op.ConfigNum, kv.me, kv.gid, seq)

		kv.px.Start(seq, op)
		v := kv.waitForDecided(seq)

		// debug
		DPrintf("SendMakeup vid=%d, vtype=%s, vkey=%s, vvalue=%s, vpre=%s, vconfigNum=%d, vShardNum=%v, vShardNumsToSend=%v, me=%d, gid=%d, seq=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, v.PreviousValue, v.ConfigNum, v.ShardNum, v.ShardNumsToSend, kv.me, kv.gid, seq)

		if v.OpID == op.OpID {
			// reach the agreement
			// do send
			kv.SendHelper(&v)
			kv.px.Done(seq)
			break
		} else {
			kv.catchUp(&v)
		}
	}
}

func (kv *ShardKV) GetHelper(v *Op, reply *GetReply) {
	shardNum := key2shard(v.Key)
	if !kv.isHold[shardNum] {
		reply.Err = ErrWrongGroup
	} else {
		if value, exist := kv.shards[shardNum][v.Key]; !exist {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			// debug
			DPrintf("GetHelper vid=%d, vtype=%s, vkey=%v, shardNum=%d, kv.isHold=%v, shard=%v, me=%d, gid=%d\n",
				v.OpID, v.OpType, v.Key, shardNum, kv.isHold, kv.shards[shardNum], kv.me, kv.gid)

			reply.Err = OK
			reply.Value = value
			// Save this request to record
			kv.history[shardNum][v.OpID] = value
		}
	}

}

func (kv *ShardKV) PutHelper(v *Op, reply *PutReply) {
	shardNum := key2shard(v.Key)
	if !kv.isHold[shardNum] {
		reply.Err = ErrWrongGroup
	} else {
		// debug
		DPrintf("PutHelper vid=%d, vtype=%s, vkey=%v, vvalue=%v, shardNum=%d, kv.isHold=%v, shard=%v, me=%d, gid=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, shardNum, kv.isHold, kv.shards[shardNum], kv.me, kv.gid)

		reply.Err = OK
		kv.shards[shardNum][v.Key] = v.Value
		kv.history[shardNum][v.OpID] = v.Value
	}
}

func (kv *ShardKV) PutHashHelper(v *Op, reply *PutReply) {
	shardNum := key2shard(v.Key)
	if !kv.isHold[shardNum] {
		reply.Err = ErrWrongGroup
	} else {
		if value, exist := kv.shards[shardNum][v.Key]; !exist {
			reply.PreviousValue = ""
		} else {
			reply.PreviousValue = value
		}
		// debug
		DPrintf("PutHashHelper vid=%d, vtype=%s, vkey=%v, vvalue=%v, shardNum=%d, kv.isHold=%v, shard=%v, me=%d, gid=%d\n",
			v.OpID, v.OpType, v.Key, v.Value, shardNum, kv.isHold, kv.shards[shardNum], kv.me, kv.gid)

		reply.Err = OK
		kv.shards[shardNum][v.Key] = strconv.Itoa(int(hash(reply.PreviousValue + v.Value)))
		kv.history[shardNum][v.OpID] = reply.PreviousValue
	}
}

func (kv *ShardKV) SendHelper(v *Op) {
	// using goroutine when call rpc receive func to avoid deadlock

	// init
	if kv.isInitial {
		// initialize
		config := kv.sm.Query(1)
		for i, gid := range config.Shards {
			if gid == kv.gid {
				kv.isHold[i] = true
			}
		}
		kv.isInitial = false
	} else {
		// debug
		DPrintf("SendHelper vid=%d, vtype=%s, kv.isHold=%v, ShardNumsToSend=%v, me=%d, gid=%d\n",
			v.OpID, v.OpType, kv.isHold, v.ShardNumsToSend, kv.me, kv.gid)

		// do send
		for i := range v.ShardNumsToSend {
			shardNum := v.ShardNumsToSend[i]

			if !kv.isHold[shardNum] {
				return
			}

			shard := kv.shards[shardNum]
			servers := v.TargetServers[i]
			history := kv.history[shardNum]

			args := &ReceiveArgs{}
			args.ShardNum = shardNum
			args.Shard = shard
			args.History = history
			args.Id = int64(v.ConfigNum)<<10 + int64(shardNum)

			// debug
			DPrintf("SendHelper Send Shard vid=%d, vtype=%s, kv.isHold=%v, ShardNumsToSend=%v, shardNum=%v, shard=%v, history=%v, sendId=%d, me=%d, gid=%d\n",
				v.OpID, v.OpType, kv.isHold, v.ShardNumsToSend, shardNum, shard, history, args.Id, kv.me, kv.gid)

			kv.isHold[shardNum] = false
			go func(servers []string, recvArgs *ReceiveArgs) {
				for {
					// try each known server.
					for _, srv := range servers {
						var reply ReceiveReply
						ok := call(srv, "ShardKV.Receive", recvArgs, &reply)
						if ok {
							return
						}
					}
					time.Sleep(100 * time.Millisecond)
				}

			}(servers, args)

		}
	}

}

func (kv *ShardKV) ReceiveHelper(v *Op, reply *ReceiveReply) {
	// debug
	DPrintf("ReceiveHelper vid=%d, vtype=%s, shardNum=%d, shard=%v, kv.isHold=%v, me=%d, gid=%d\n",
		v.OpID, v.OpType, v.ShardNum, v.Shard, kv.isHold, kv.me, kv.gid)

	kv.shards[v.ShardNum] = make(map[string]string)
	for k, value := range v.Shard {
		kv.shards[v.ShardNum][k] = value
	}

	kv.history[v.ShardNum] = make(map[int64]string)
	for k, value := range v.HistoryShard {
		kv.history[v.ShardNum][k] = value
	}

	kv.isHold[v.ShardNum] = true
	kv.history[v.ShardNum][v.OpID] = ""
	reply.Err = OK
}

func (kv *ShardKV) catchUp(v *Op) {
	// update the config array
	if v.OpType == PUT {
		kv.PutHelper(v, &PutReply{})
	} else if v.OpType == PUTHASH {
		kv.PutHashHelper(v, &PutReply{})
	} else if v.OpType == SEND {
		kv.SendHelper(v)
	} else if v.OpType == RECEIVE {
		kv.ReceiveHelper(v, &ReceiveReply{})
	}
}

func (kv *ShardKV) waitForDecided(seq int) Op {
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

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.curMaxSeq = -1
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shards[i] = make(map[string]string)
		kv.history[i] = make(map[int64]string)
		kv.isHold[i] = false
	}
	kv.isInitial = true

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
