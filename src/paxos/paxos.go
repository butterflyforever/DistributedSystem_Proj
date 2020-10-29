package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	gap   = 10000
	OK    = "OK"
	Stale = "Stale"
	Err   = "Err"
)

type Instance struct {
	isDecided bool
	np        int
	na        int
	va        interface{}
	vDecided  interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*Instance
	curMax    int
	doneValue []int
	majority  int
}

type PrepareArgs struct {
	Seq    int
	N      int
	Sender int
	Min    int
}

type PrepareReply struct {
	Err string
	Seq int
	Na  int
	Va  interface{}
	N   int
}

type AcceptArgs struct {
	Seq    int
	N      int
	V      interface{}
	Sender int
	Min    int
}

type AcceptReply struct {
	Err string
	Seq int
	N   int
}

type DecidedArgs struct {
	Seq    int
	V      interface{}
	Sender int
	Min    int
}

type DecidedReply struct {
	Err    string
	Seq    int
	Sender int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Generate unique proposal number
func generateN(base int, peerNum int) int {
	return base*gap + peerNum
}

func (px *Paxos) peerDone(seq int, peerNum int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.doneValue[peerNum] < seq {
		px.doneValue[peerNum] = seq

		// Cannot use px.Min() because of lock in px.Min()
		curMinDone := seq
		for i := range px.doneValue {
			if curMinDone > px.doneValue[i] {
				curMinDone = px.doneValue[i]
			}
		}

		// Collect all instances less than curMinDone
		doneInstance := []int{}
		for k := range px.instances {
			if k <= curMinDone {
				doneInstance = append(doneInstance, k)
			}
		}

		// Delete them
		for i := range doneInstance {
			delete(px.instances, doneInstance[i])
		}

	}

}

func (px *Paxos) getInstance(seq int) *Instance {
	px.mu.Lock()
	defer px.mu.Unlock()
	targetIns := px.instances[seq]
	if targetIns == nil {
		targetIns = &Instance{}
		targetIns.isDecided = false
		targetIns.np = -1
	}
	return targetIns
}

// Proposer Actions
func (px *Paxos) Propose(seq int, v interface{}) {
	// Your code here.

	// Check if seq num valid
	if seq < px.Min() {
		return
	}

	//Debug
	fmt.Printf("propose\n")

	base := 0
	for targetIns := px.getInstance(seq); targetIns.isDecided == false; {
		if px.dead {
			break
		}

		base = int(math.Max(float64(targetIns.np/gap+1), float64(base+1)))
		N := generateN(base, px.me)

		//Debug
		fmt.Printf("propose number N: %d, seq %d\n", N, seq)

		maxNFromAcceptors := -1
		prepareMajorityOK := false
		prepareNum := 0
		prepareReplyChan := make(chan *PrepareReply)

		// Send prepare to all acceptors
		for i := range px.peers {
			go func(i int) {
				preArgs := &PrepareArgs{}
				preArgs.Sender = px.me
				preArgs.Seq = seq
				preArgs.N = N
				preArgs.Min = px.doneValue[px.me]

				var reply PrepareReply

				// Call the local acceptor using function
				// Call the external acceptor using RPCs
				ok := true

				if i != px.me {
					ok = call(px.peers[i], "Paxos.Prepare", preArgs, &reply)
				} else {
					px.Prepare(preArgs, &reply)
					ok = true
				}
				// Send all reply to channel
				if ok != true {
					reply.Err = Err
				}
				prepareReplyChan <- &reply
			}(i)
		}

		// Check if get enough prepareOKs
		na := -1
		var vPrime interface{}
		// _ = vPrime
		for range px.peers {

			reply := <-prepareReplyChan
			if reply.Err == OK {
				prepareNum++
				if na < reply.Na {
					na = reply.Na
					vPrime = reply.Va
				}
				if prepareNum >= px.majority {
					prepareMajorityOK = true
				}
			} else if reply.Err == Stale {
				if maxNFromAcceptors < reply.N {
					maxNFromAcceptors = reply.N
				}

			}
		}

		if vPrime == nil {
			vPrime = v
		}

		//Debug
		fmt.Printf("vPrime: %s\n", vPrime)

		fmt.Printf("prepare OK: %t\n", prepareMajorityOK)

		// TODO - Do we need Paxo's Timeout? or just RPC timeout

		// Send accept(n, v') to all acceptors
		if prepareMajorityOK == true {

			accMajorityOK := false
			accNum := 0
			accReplyChan := make(chan *AcceptReply)

			// Send prepare to all acceptors
			for i := range px.peers {
				go func(i int) {
					accArgs := &AcceptArgs{}
					accArgs.Sender = px.me
					accArgs.Seq = seq
					accArgs.N = N
					accArgs.V = vPrime
					accArgs.Min = px.doneValue[px.me]
					var reply AcceptReply

					// Call the local acceptor using function
					// Call the external acceptor using RPCs
					ok := true
					if i != px.me {
						ok = call(px.peers[i], "Paxos.Accept", accArgs, &reply)
					} else {
						px.Accept(accArgs, &reply)
						ok = true
					}
					if ok != true {
						reply.Err = Err
					}
					accReplyChan <- &reply
				}(i)
			}

			// Check if get enough acceptOKs
			for range px.peers {
				reply := <-accReplyChan
				if reply.Err == OK {
					accNum++
					if accNum >= px.majority {
						accMajorityOK = true
					}
				} else if reply.Err == Stale {
					if maxNFromAcceptors < reply.N {
						maxNFromAcceptors = reply.N
					}
				}
			}

			// Send decided to all acceptors
			if accMajorityOK == true {
				for i := range px.peers {
					go func(i int) {
						decArgs := &DecidedArgs{}
						decArgs.Sender = px.me
						decArgs.Seq = seq
						decArgs.V = vPrime
						decArgs.Min = px.doneValue[px.me]
						var reply DecidedReply

						// Call the local acceptor using function
						// Call the external acceptor using RPCs
						if i != px.me {
							_ = call(px.peers[i], "Paxos.Decided", decArgs, &reply)
						} else {
							px.Decided(decArgs, &reply)
						}

					}(i)
				}
				break
			}

		}

		base = int(math.Max(float64(maxNFromAcceptors/gap), float64(base)))

		// For rpc test. Sleep for a while to prevent endless loop ...
		randNum := 5 + rand.Intn(45)
		time.Sleep(time.Duration(randNum) * time.Millisecond)

	}

}

// Acceptor Handlers
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.peerDone(args.Min, args.Sender)

	px.mu.Lock()
	defer px.mu.Unlock()
	if args.Seq > px.curMax {
		px.curMax = args.Seq
	}
	reply.Seq = args.Seq

	targetIns := px.instances[args.Seq]
	if targetIns == nil {
		targetIns = &Instance{}
		targetIns.isDecided = false
		targetIns.np = -1
		targetIns.na = -1
		targetIns.va = nil
	}

	if args.N > targetIns.np {
		targetIns.np = args.N
		px.instances[args.Seq] = targetIns
		reply.Err = OK
		reply.Na = targetIns.na
		reply.Va = targetIns.va

		//debug
		fmt.Printf("Prepare isDecided=%t, va=%v, me=%d, sender=%d, seq %d\n", targetIns.isDecided, targetIns.va, px.me, args.Sender, args.Seq)

	} else {
		reply.Err = Stale
		// reply.n only works when Stale
		reply.N = targetIns.np
	}
	// Your code here.
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	// Your code here.
	px.peerDone(args.Min, args.Sender)

	px.mu.Lock()
	defer px.mu.Unlock()
	if args.Seq > px.curMax {
		px.curMax = args.Seq
	}
	reply.Seq = args.Seq

	targetIns := px.instances[args.Seq]
	if targetIns == nil {
		targetIns = &Instance{}
		targetIns.isDecided = false
		targetIns.np = -1
		targetIns.na = -1
		targetIns.va = nil
	}

	if args.N >= targetIns.np {
		targetIns.np = args.N
		targetIns.na = args.N
		targetIns.va = args.V
		px.instances[args.Seq] = targetIns
		reply.Err = OK

		//debug
		fmt.Printf("Accept isDecided=%t, va=%v, me=%d, sender=%d, seq %d\n", targetIns.isDecided, targetIns.va, px.me, args.Sender, args.Seq)

	} else {
		reply.Err = Stale
		// reply.n only works when Stale
		reply.N = targetIns.np
	}
	// Your code here.
	return nil

}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	// Your code here.
	px.peerDone(args.Min, args.Sender)

	px.mu.Lock()
	defer px.mu.Unlock()
	if args.Seq > px.curMax {
		px.curMax = args.Seq
	}
	reply.Seq = args.Seq

	targetIns := px.instances[args.Seq]
	if targetIns == nil {
		targetIns = &Instance{}
		targetIns.isDecided = false
		targetIns.np = -1
		targetIns.na = -1
		targetIns.va = nil
	}

	targetIns.isDecided = true
	targetIns.va = args.V
	targetIns.vDecided = args.V
	// if targetIns.np < args.
	px.instances[args.Seq] = targetIns
	reply.Err = OK

	//debug
	fmt.Printf("decided v=%v, sender=%d, me=%d, seq=%d\n", px.instances[args.Seq].va, args.Sender, px.me, args.Seq)

	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	// TBD: check seq < px.Min() in px.Propose? Start should return immidiately
	go px.Propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.peerDone(seq, px.me)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.curMax
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	// TBD: Do I need to lock? Or only use Min() in lock?
	px.mu.Lock()
	defer px.mu.Unlock()

	curMin := px.doneValue[0]
	for i := range px.doneValue {
		if px.doneValue[i] < curMin {
			curMin = px.doneValue[i]
		}
	}
	return curMin + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	curMin := px.Min()
	px.mu.Lock()
	defer px.mu.Unlock()

	targetIns := px.instances[seq]
	if seq < curMin || targetIns == nil {
		return false, nil
	} else {
		//debug
		fmt.Printf("Status: isDecided=%t, va=%v, me=%d, seq %d\n", targetIns.isDecided, targetIns.vDecided, px.me, seq)
		return targetIns.isDecided, targetIns.vDecided
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*Instance)
	px.curMax = -1
	px.doneValue = make([]int, len(px.peers))
	for i := range px.doneValue {
		px.doneValue[i] = -1
	}
	px.majority = len(px.peers)/2 + 1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
