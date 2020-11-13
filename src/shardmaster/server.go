package shardmaster

import (
	crand "crypto/rand"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sort"
	"sync"
	"syscall"
	"time"
)

const Debug = 0
const JOIN = "JOIN"
const LEAVE = "LEAVE"
const MOVE = "MOVE"
const QUERY = "QUERY"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs   []Config // indexed by config num
	curMaxSeq int
}

type Op struct {
	// Your data here.
	OpID     int64
	OpType   string
	GID      int64
	Servers  []string
	Shard    int
	QueryNum int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	id := nrand()
	op := Op{}
	op.OpType = JOIN
	op.OpID = id
	op.GID = args.GID
	op.Servers = args.Servers

	for {
		sm.curMaxSeq++
		seq := sm.curMaxSeq

		// debug
		DPrintf("Join id=%d, GID=%d, servers=%v, me=%d, seq=%d\n",
			op.OpID, op.GID, op.Servers, sm.me, seq)

		sm.px.Start(seq, op)
		v := sm.waitForDecided(seq)

		// debug
		DPrintf("JoinMakeup vid=%d, vtype=%s, vGID=%d, vservers=%v, vshard=%d, queryNum=%d, me=%d, seq=%d\n",
			v.OpID, v.OpType, v.GID, v.Servers, v.Shard, v.QueryNum, sm.me, seq)

		if v.OpID == op.OpID {
			// reach the agreement

			// Dojoin
			sm.joinHelper(op.GID, op.Servers)
			sm.px.Done(seq)
			break
		} else {
			sm.catchUp(&v)
		}

	}

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	id := nrand()
	op := Op{}
	op.OpType = LEAVE
	op.OpID = id
	op.GID = args.GID

	for {
		sm.curMaxSeq++
		seq := sm.curMaxSeq

		// debug
		DPrintf("Leave id=%d, GID=%d, me=%d, seq=%d\n", op.OpID, op.GID, sm.me, seq)

		sm.px.Start(seq, op)
		v := sm.waitForDecided(seq)

		// debug
		DPrintf("LeaveMakeup vid=%d, vtype=%s, vGID=%d, vservers=%v, vshard=%d, queryNum=%d, me=%d, seq=%d\n",
			v.OpID, v.OpType, v.GID, v.Servers, v.Shard, v.QueryNum, sm.me, seq)

		if v.OpID == op.OpID {
			// reach the agreement

			// Doleave
			sm.leaveHelper(op.GID)

			sm.px.Done(seq)
			break
		} else {
			sm.catchUp(&v)
		}

	}

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	id := nrand()
	op := Op{}
	op.OpType = MOVE
	op.OpID = id
	op.GID = args.GID
	op.Shard = args.Shard

	for {
		sm.curMaxSeq++
		seq := sm.curMaxSeq

		// debug
		DPrintf("Move id=%d, GID=%d, shard=%d, me=%d, seq=%d\n",
			op.OpID, op.GID, op.Shard, sm.me, seq)

		sm.px.Start(seq, op)
		v := sm.waitForDecided(seq)

		// debug
		DPrintf("MoveMakeup vid=%d, vtype=%s, vGID=%d, vservers=%v, vshard=%d, queryNum=%d, me=%d, seq=%d\n",
			v.OpID, v.OpType, v.GID, v.Servers, v.Shard, v.QueryNum, sm.me, seq)

		if v.OpID == op.OpID {
			// reach the agreement

			// Domove
			sm.moveHelper(op.GID, op.Shard)

			sm.px.Done(seq)
			break
		} else {
			sm.catchUp(&v)
		}

	}

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	id := nrand()
	op := Op{}
	op.OpType = QUERY
	op.OpID = id
	op.QueryNum = args.Num

	for {
		sm.curMaxSeq++
		seq := sm.curMaxSeq

		// debug
		DPrintf("Query id=%d, GID=%d, Num=%d, me=%d, seq=%d\n",
			op.OpID, op.GID, op.QueryNum, sm.me, seq)

		sm.px.Start(seq, op)
		v := sm.waitForDecided(seq)

		// debug
		DPrintf("QueryMakeup vid=%d, vtype=%s, vGID=%d, vservers=%v, vshard=%d, me=%d, seq=%d\n",
			v.OpID, v.OpType, v.GID, v.Servers, v.Shard, sm.me, seq)

		if v.OpID == op.OpID {
			// reach the agreement

			// Doquery
			if v.QueryNum == -1 || v.QueryNum >= len(sm.configs) {
				reply.Config = sm.configs[len(sm.configs)-1]
			} else {
				reply.Config = sm.configs[v.QueryNum]
			}
			sm.px.Done(seq)
			break
		} else {
			sm.catchUp(&v)
		}

	}

	return nil
}

func (sm *ShardMaster) joinHelper(GID int64, servers []string) {
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Shards = sm.configs[newConfig.Num-1].Shards
	newConfig.Groups = make(map[int64][]string)

	for k, v := range sm.configs[newConfig.Num-1].Groups {
		newConfig.Groups[k] = v
	}
	newConfig.Groups[GID] = servers

	// Re-balance
	sm.rebalance(&newConfig)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) leaveHelper(GID int64) {
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Shards = sm.configs[len(sm.configs)-1].Shards
	newConfig.Groups = make(map[int64][]string)

	for k, v := range sm.configs[len(sm.configs)-1].Groups {
		newConfig.Groups[k] = v
	}
	delete(newConfig.Groups, GID)
	for i := range newConfig.Shards {
		if newConfig.Shards[i] == GID {
			newConfig.Shards[i] = 0
		}
	}

	// Re-balance
	sm.rebalance(&newConfig)

	sm.configs = append(sm.configs, newConfig)

}

func (sm *ShardMaster) moveHelper(GID int64, shard int) {
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Shards = sm.configs[len(sm.configs)-1].Shards
	newConfig.Groups = make(map[int64][]string)

	for k, v := range sm.configs[len(sm.configs)-1].Groups {
		newConfig.Groups[k] = v
	}

	if _, ok := newConfig.Groups[GID]; ok {
		newConfig.Shards[shard] = GID
	} else {
		// Invalid move!
		fmt.Printf("Invalid move. GID=%d, shard=%d\n", GID, shard)
	}

	sm.configs = append(sm.configs, newConfig)

}

func (sm *ShardMaster) rebalance(config *Config) {
	group := []int64{}
	for gid := range config.Groups {
		group = append(group, gid)
	}

	// No group
	if len(group) == 0 {
		return
	}

	// keep the order
	sort.Slice(group, func(i, j int) bool { return group[i] < group[j] })
	groupCountMap := map[int64]int{}
	for _, g := range group {
		groupCountMap[g] = 0
	}
	for i, s := range config.Shards {
		if config.Shards[i] == 0 {
			config.Shards[i] = group[0]
			s = group[0]
		}
		groupCountMap[s]++
	}

	minNum := NShards + 1
	minGroup := int64(-1)
	maxNum := -1
	maxGroup := int64(-1)

	for maxNum-minNum != 0 && maxNum-minNum != 1 {
		// move one shard
		if minGroup != -1 && maxGroup != -1 {
			for i, s := range config.Shards {
				if s == maxGroup {
					config.Shards[i] = minGroup
					groupCountMap[minGroup]++
					groupCountMap[maxGroup]--
					break
				}
			}
		}

		// recheck if balance
		minNum = NShards + 1
		maxNum = -1
		for _, g := range group {
			if groupCountMap[g] > maxNum {
				maxNum = groupCountMap[g]
				maxGroup = g
			}
			if groupCountMap[g] < minNum {
				minNum = groupCountMap[g]
				minGroup = g
			}
		}

	}
}

func (sm *ShardMaster) catchUp(v *Op) {
	// update the config array
	if v.OpType == JOIN {
		sm.joinHelper(v.GID, v.Servers)
	} else if v.OpType == LEAVE {
		sm.leaveHelper(v.GID)
	} else if v.OpType == MOVE {
		sm.moveHelper(v.GID, v.Shard)
	}
}

func (sm *ShardMaster) waitForDecided(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, v := sm.px.Status(seq)
		if decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.curMaxSeq = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
