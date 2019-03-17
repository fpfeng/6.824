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
	tasks       []string
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

	st := &SyncTask{tasks: mapFiles}
	for {
		taskToDoIdx := 0
		st.mu.Lock()
		taskToDoIdx = st.taskToDoIdx
		st.mu.Unlock()

		if taskToDoIdx >= ntasks {
			break
		}

		debug("%s for loop.... %d\n", phase, taskToDoIdx)
		select {
		case workerRPCAddr := <-registerChan:
			taskToDoIdx := 0
			var tasks []string
			st.mu.Lock()
			taskToDoIdx = st.taskToDoIdx
			tasks = st.tasks[st.taskToDoIdx : st.taskToDoIdx+3]
			st.taskToDoIdx += 3
			st.mu.Unlock()

			debug("all task %s\n", tasks)
			for i, t := range tasks {
				block := make(chan int)
				go func(taskIdx int, fileName string) {
					debug("%s call worker: %s, file: %s\n", phase, workerRPCAddr, fileName)
					taskArg := DoTaskArgs{
						JobName:       jobName,
						File:          fileName,
						Phase:         phase,
						TaskNumber:    taskIdx,
						NumOtherPhase: n_other,
					}
					call(workerRPCAddr, "Worker.DoTask", &taskArg, nil)
					block <- 1
				}(taskToDoIdx+i, t)
				<-block
			}
			debug("done current loop job\n")
			debug("release lock\n")
			go func() {
				registerChan <- workerRPCAddr
			}()
			debug("done put worker back queue\n")
		default:
			break
		}
	}

	fmt.Printf("Schedule: %v done\n", phase)
	return
}
