package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	lastTimeSeversPing map[string]time.Time
	curView            View
	curViewAcked       bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	serverName := args.Me
	serverViewNum := args.Viewnum
	vs.lastTimeSeversPing[serverName] = time.Now()
	// fmt.Printf("Server cur primary %s, cur backup %s, cur num %d. Ping name %s, viewNum %d\n", vs.curView.Primary, vs.curView.Backup, vs.curView.Viewnum, serverName, serverViewNum)

	if serverViewNum == 0 && vs.curViewAcked {

		// restart or create a new one
		if serverName == vs.curView.Primary {
			// fmt.Printf("PING 011111111\n")
			if vs.curView.Backup != "" { // promote backup server
				vs.curView.Primary = vs.curView.Backup
				vs.curView.Backup = serverName
				vs.curViewAcked = false
			} else { // both crash, reset them
				vs.curView.Primary = ""
				vs.curView.Backup = ""
				vs.curViewAcked = true
			}
			vs.curView.Viewnum++
		} else if serverName == vs.curView.Backup {
			// fmt.Printf("PING 022222222\n")
			// Backup crash
			vs.curView.Backup = serverName
			vs.curView.Viewnum++
			vs.curViewAcked = false
		} else {
			// fmt.Printf("PING 033333333\n")
			if vs.curView.Primary == "" && vs.curView.Viewnum == 0 {
				// Initial state
				vs.curView.Primary = serverName
				vs.curView.Viewnum++
				vs.curViewAcked = false
			} else if vs.curView.Backup == "" {
				// As a backup
				vs.curView.Backup = serverName
				vs.curView.Viewnum++
				vs.curViewAcked = false
			}
		}

	} else if serverViewNum == vs.curView.Viewnum && serverName == vs.curView.Primary {
		vs.curViewAcked = true
	}

	reply.View = vs.curView

	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// Do I need to lock mutex?
	vs.mu.Lock()
	reply.View = vs.curView
	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	if vs.curViewAcked {
		curTime := time.Now()

		// Note: primary cannot be empty, backup can be
		var primaryTime time.Time
		var backupTime time.Time

		if value, ok := vs.lastTimeSeversPing[vs.curView.Primary]; ok {
			primaryTime = value
		} else {
			primaryTime = curTime
		}

		if value, ok := vs.lastTimeSeversPing[vs.curView.Backup]; ok {
			backupTime = value
		} else {
			backupTime = curTime
		}

		isPrimaryDead := curTime.Sub(primaryTime) >= DeadPings*PingInterval
		isBackupDead := curTime.Sub(backupTime) >= DeadPings*PingInterval

		if isPrimaryDead && isBackupDead {
			// both dead, reset
			vs.curView.Primary = ""
			vs.curView.Backup = ""
			vs.curViewAcked = true
		} else if isPrimaryDead {
			// fmt.Printf("PrimaryDead\n")
			vs.curView.Primary = vs.curView.Backup
			vs.curView.Backup = ""
			vs.curView.Viewnum++
			vs.curViewAcked = false
		} else if isBackupDead {
			// fmt.Printf("BackupDead\n")
			vs.curView.Backup = ""
			vs.curViewAcked = false
			vs.curView.Viewnum++
		}
	}
	vs.mu.Unlock()

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.lastTimeSeversPing = make(map[string]time.Time)
	vs.curView.Viewnum = 0
	vs.curView.Primary = ""
	vs.curView.Backup = ""
	vs.curViewAcked = true

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
