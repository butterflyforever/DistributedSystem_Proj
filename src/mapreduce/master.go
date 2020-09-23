package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	var wg sync.WaitGroup

	go mr.createThreadForWorkers(&wg)

	// Map
	mr.writeTaskToChannel(Map, &wg)

	wg.Wait()

	// Reduce
	mr.writeTaskToChannel(Reduce, &wg)

	wg.Wait()

	close(mr.taskChannel)
	close(mr.registerChannel)

	return mr.KillWorkers()
}

func (mr *MapReduce) createThreadForWorkers(wg *sync.WaitGroup) {
	for {
		workerAddr, done := <-mr.registerChannel
		// fmt.Printf("WorkerInfo %s\n", workerAddr)
		if done {
			fmt.Println("new worker ", workerAddr)
			mr.Workers[workerAddr] = new(WorkerInfo)
			mr.Workers[workerAddr].address = workerAddr
			go mr.assignTasks(workerAddr, wg)
		} else {
			fmt.Println("no more workers")
			break
		}
	}
}

func (mr *MapReduce) assignTasks(workerAddr string, wg *sync.WaitGroup) {
	for {
		jobArgs, done := <-mr.taskChannel
		if done {
			fmt.Printf("assignTasks receive task %s \n", jobArgs.Operation)
			reply := &DoJobReply{}
			ok := call(workerAddr, "Worker.DoJob", jobArgs, reply)
			if ok == false {
				fmt.Printf("DoJob: RPC %s DoJob %s error\n", workerAddr, jobArgs.Operation)
				mr.taskChannel <- jobArgs
			} else {
				fmt.Printf("DoJob: RPC %s DoJob %s success\n", workerAddr, jobArgs.Operation)
				wg.Done()
			}
		} else {
			fmt.Printf("no more tasks\n")
			break
		}

	}
}

func (mr *MapReduce) writeTaskToChannel(jobType JobType, wg *sync.WaitGroup) {
	workArgs := DoJobArgs{
		File:      mr.file,
		Operation: jobType}

	var jobNum int

	if jobType == Map {
		workArgs.NumOtherPhase = mr.nReduce
		jobNum = mr.nMap
	} else {
		workArgs.NumOtherPhase = mr.nMap
		jobNum = mr.nReduce
	}

	for i := 0; i < jobNum; i++ {
		wg.Add(1)
		workArgs.JobNumber = i
		mr.taskChannel <- workArgs
	}
}
