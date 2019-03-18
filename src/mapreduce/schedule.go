package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

type SyncTask struct {
	mu          sync.Mutex
	taskToDoIdx int
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	st := &SyncTask{}
	var wg sync.WaitGroup
	for {
		taskToDoIdx := 0
		st.mu.Lock()
		taskToDoIdx = st.taskToDoIdx
		st.mu.Unlock()

		if taskToDoIdx >= ntasks {
			debug("break loop\n")
			break
		}

		debug("%s for loop.... %d\n", phase, taskToDoIdx)
		select {
		case workerRPCAddr := <-registerChan:
			wg.Add(1)
			go func() {
				taskToDoIdx := 0
				var tasks []string
				st.mu.Lock()
				taskToDoIdx = st.taskToDoIdx
				tasks = mapFiles[st.taskToDoIdx : st.taskToDoIdx+5]
				st.taskToDoIdx += 5
				st.mu.Unlock()

				debug("all task %s\n", tasks)
				for i, t := range tasks {
					debug("%s #%d call worker: %s, file: %s\n", phase, taskToDoIdx+i, workerRPCAddr, t)
					taskArg := DoTaskArgs{
						JobName:       jobName,
						File:          t,
						Phase:         phase,
						TaskNumber:    taskToDoIdx + i,
						NumOtherPhase: n_other,
					}
					call(workerRPCAddr, "Worker.DoTask", &taskArg, nil)
					debug("done call rpc\n")
				}
				debug("done current loop job\n")
				go func() {
					registerChan <- workerRPCAddr
					debug("done put worker back queue\n")
				}()
				wg.Done()
			}()
		default:
			break
		}
	}

	debug("wait all worker\n")
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
	return
}
