package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	data           map[string]string
	isPrimary      bool
	isBackup       bool
	records        map[int64]string
	mu             sync.Mutex
	curView        viewservice.View
	dataBackupDone bool
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if !pb.isPrimary {
		reply.Err = ErrWrongServer
		return nil
	}

	if args.Viewnum != pb.curView.Viewnum {
		// stale
		reply.Err = ErrWrongServer
		return nil
	}

	if !pb.dataBackupDone {
		// Haven't backup
		reply.Err = ErrWrongServer
		return nil
	}

	if val, exist := pb.records[args.Id]; exist {
		// Filter repeated requests
		reply.Err = OK
		reply.PreviousValue = val
	} else {
		// fmt.Printf("Backup %s\n", pb.curView.Backup)
		if pb.curView.Backup != "" {
			// fmt.Printf("Put\n")
			ok := true
			forwardReply := PutReply{}
			forwardReply.Err = ""
			forwardReply.PreviousValue = ""

			// Debug
			fmt.Printf("Put call PutForward, key %s, value %s, id %d\n", args.Key, args.Value, args.Id)
			//

			ok = call(pb.curView.Backup, "PBServer.PutForward", args, &forwardReply)
			// Backup failed
			if ok == false {
				forwardReply.Err = ErrWrongServer
			}
			reply.Err = forwardReply.Err

			// Debug
			fmt.Printf("Put call PutForward have result, key %s, result %s, id %d, reply.Err %s\n", args.Key, args.Value, args.Id, reply.Err)
			//
			// fmt.Printf("Put %t, Err %s\n", ok, reply.Err)

		} else {
			reply.Err = OK
		}

		// Success, do put in local database
		if reply.Err == OK {
			if args.DoHash {
				preVal, preEx := pb.data[args.Key]
				if preEx == false {
					preVal = ""
				}
				// strconv.FormatUint uses base = 10
				pb.data[args.Key] = strconv.FormatUint(uint64(hash(preVal+args.Value)), 10)
				reply.PreviousValue = preVal

				// Save this request to record
				pb.records[args.Id] = preVal

				// Debug
				fmt.Printf("Put Prevalue, key %s, value %s, id %d, preVal %s\n", args.Key, args.Value, args.Id, preVal)
				//

			} else {
				// Don't modify reply.PreviousValue if not dohash

				pb.data[args.Key] = args.Value
				// Save this request to record
				pb.records[args.Id] = ""
			}

			// Debug
			fmt.Printf("Put local result, key %s, result %s, id %d, reply.Err %s\n", args.Key, pb.data[args.Key], args.Id, reply.Err)
			//

		}

		// fmt.Printf("Put End Err %s\n", reply.Err)

	}

	return nil
}

func (pb *PBServer) PutForward(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isBackup == false {
		reply.Err = ErrWrongServer
		return nil
	}

	if args.Viewnum != pb.curView.Viewnum {
		// stale
		reply.Err = ErrWrongServer
		return nil
	}

	// Debug
	fmt.Printf("PutForward get request, key %s, value %s, id %d\n", args.Key, args.Value, args.Id)
	//

	// Filter repeated PUT requests
	if val, exist := pb.records[args.Id]; exist {
		// Filter repeated requests
		reply.PreviousValue = val
		reply.Err = OK
		return nil
	}

	if args.DoHash {
		preVal, preEx := pb.data[args.Key]
		if preEx == false {
			preVal = ""
		}

		if v, e := pb.records[args.Id]; e {
			preVal = v
		}

		// strconv.FormatUint uses base = 10
		pb.data[args.Key] = strconv.FormatUint(uint64(hash(preVal+args.Value)), 10)
		reply.PreviousValue = preVal

		// Save this request to record
		pb.records[args.Id] = preVal
	} else {
		// Don't modify reply.PreviousValue if not dohash

		pb.data[args.Key] = args.Value
		// Save this request to record
		pb.records[args.Id] = ""
	}

	// Debug
	fmt.Printf("PutForward local result, key %s, value %s, id %d, result %s, preValue %s\n", args.Key, args.Value, args.Id, pb.data[args.Key], reply.PreviousValue)
	//

	reply.Err = OK
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary {
		reply.Err = ErrWrongServer
		return nil
	}

	if args.Viewnum != pb.curView.Viewnum {
		// stale
		reply.Err = ErrWrongServer
		return nil
	}

	if !pb.dataBackupDone {
		// Haven't backup
		reply.Err = ErrWrongServer
		return nil
	}

	if val, exist := pb.records[args.Id]; exist {
		// Filter repeated requests
		reply.Err = OK
		reply.Value = val
	} else {

		forwardReply := GetReply{}
		forwardReply.Err = ""
		forwardReply.Value = ""

		if pb.curView.Backup != "" {
			ok := true

			// Debug
			fmt.Printf("Get call GetForward, key %s, id %d\n", args.Key, args.Id)
			//

			ok = call(pb.curView.Backup, "PBServer.GetForward", args, &forwardReply)

			// Backup failed
			if ok == false {
				forwardReply.Err = ErrWrongServer
			}
			reply.Err = forwardReply.Err
			reply.Value = forwardReply.Value

			// Debug
			fmt.Printf("Result : Get call GetForward, key %s, id %d, reply.Err %s. Result is %s\n", args.Key, args.Id, reply.Err, reply.Value)
			//

		} else {
			reply.Err = OK
		}

		// TOCHECK: Do I need to save ErrNoKey to record? Now Do not save

		// No Backup or Backup Success, do Get in local database
		// if reply.Err != OK, just return.
		if forwardReply.Err == "" {
			// No backup, only primary
			if v, e := pb.data[args.Key]; !e {
				reply.Err = ErrNoKey
				reply.Value = ""
			} else {
				reply.Value = v
				// Save this request to record
				pb.records[args.Id] = v
			}
		} else if reply.Err == OK {
			// replace primary with backup value
			pb.data[args.Key] = forwardReply.Value
			pb.records[args.Id] = forwardReply.Value
			reply.Value = forwardReply.Value
		}
	}
	return nil
}

func (pb *PBServer) GetForward(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isBackup == false {
		reply.Err = ErrWrongServer
		return nil
	}

	if args.Viewnum != pb.curView.Viewnum {
		// stale
		reply.Err = ErrWrongServer
		return nil
	}

	// Debug
	fmt.Printf("GetForward: key %s, id %d\n", args.Key, args.Id)
	//

	// if val, exist := pb.records[args.Id]; exist {
	// 	// Filter repeated requests
	// 	reply.Err = OK
	// 	reply.Value = val
	// } else {

	// }

	// Do GET in local database

	// Check if has key
	if v, e := pb.data[args.Key]; !e {
		// TOCHECK: Do I need to save ErrNoKey to record? Now Do not save
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = v
		// Save this request to record
		pb.records[args.Id] = v
	}

	return nil
}

func (pb *PBServer) DataBackup(args *DataBackupArgs, reply *DataBackupReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isBackup {
		reply.Err = ErrWrongServer
		return nil
	}

	if args.Viewnum != pb.curView.Viewnum {
		// stale
		reply.Err = ErrWrongServer
		return nil
	}

	// filter out repeated dataBackup
	if !pb.dataBackupDone {
		pb.data = args.Database
		pb.records = args.Record
		pb.dataBackupDone = true
	}

	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	view, _ := pb.vs.Ping(pb.curView.Viewnum)
	if view.Viewnum != pb.curView.Viewnum {
		// new view
		if view.Primary == pb.me {
			pb.isPrimary = true
			pb.isBackup = false
			pb.dataBackupDone = view.Backup == ""
		} else if view.Backup == pb.me {
			pb.isPrimary = false
			pb.isBackup = true
			pb.dataBackupDone = false
		} // otherwise, no action
		pb.curView = view
	}

	// Check whether need dataBackup
	if !pb.dataBackupDone && pb.isPrimary {
		args := &DataBackupArgs{}
		var reply DataBackupReply
		args.Database = pb.data
		args.Record = pb.records
		args.Viewnum = pb.curView.Viewnum

		// I have to use loop here. I have to make sure that dataBackup is successful
		// after tick, because the next tick only Ping when dataBackupDone == true
		for ok := false; !ok; {
			ok = call(pb.curView.Backup, "PBServer.DataBackup", args, &reply)
			if reply.Err != OK {
				ok = false
			}
		}
		pb.dataBackupDone = true
	}

}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.isPrimary = false
	pb.isBackup = false
	pb.records = make(map[int64]string)
	pb.curView.Viewnum = 0
	pb.curView.Primary = ""
	pb.curView.Backup = ""
	pb.dataBackupDone = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
