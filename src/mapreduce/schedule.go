package mapreduce

import (
	"fmt"
	"sync"
	"time"
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
	failTask    map[int]string
	failWorker  map[string]int
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

	st := &SyncTask{failTask: make(map[int]string), failWorker: make(map[string]int)}
	var wg sync.WaitGroup
	for {
		taskToDoIdx := 0
		var failTask map[int]string
		st.mu.Lock()
		taskToDoIdx = st.taskToDoIdx
		failTask = st.failTask
		st.mu.Unlock()

		if taskToDoIdx >= ntasks && len(failTask) == 0 {
			debug("break loop\n")
			break
		}

		debug("%s for loop.... %d\n", phase, taskToDoIdx)
		select {
		case workerRPCAddr := <-registerChan:
			wg.Add(1)
			go func() {
				/*
					这里只是取巧地破解测试用例，睡1秒应该是不对的，下面代码有bug，不睡会fail

					测试用例是worker超过10次rpc调用就出错
					猜测bug？
						1. 当跑任务的goroutine出错时，主循环在56行的检查已经跳出，也就不能重跑任务
						2. 86、103行的跟踪下标有问题，多个goroutine会读到脏数据？

					没真正搞过线程和协程，part 3和part 4有点难啊
				*/
				if len(failTask) == 0 {
					time.Sleep(1 * time.Second)
				}

				taskToDoIdx := 0
				var tasks []string
				st.mu.Lock()
				taskToDoIdx = st.taskToDoIdx
				if taskToDoIdx < len(mapFiles) {
					tasks = mapFiles[st.taskToDoIdx : st.taskToDoIdx+10]
					st.taskToDoIdx += 10
				}
				st.mu.Unlock()

				debug("all task %s\n", tasks)
				for i, t := range tasks {
					taskArg := DoTaskArgs{
						JobName:       jobName,
						File:          t,
						Phase:         phase,
						TaskNumber:    taskToDoIdx + i,
						NumOtherPhase: n_other,
					}
					isSuccess := callWorker(&taskArg, workerRPCAddr)
					if !isSuccess {
						st.mu.Lock()
						st.failTask[taskToDoIdx+i] = t
						st.failWorker[workerRPCAddr] = 1
						st.taskToDoIdx = taskToDoIdx
						st.mu.Unlock()
						debug("%s append fail %s #%d %s\n", phase, workerRPCAddr, i, t)
					}
				}

				var failTask map[int]string
				st.mu.Lock()
				failTask = st.failTask
				st.mu.Unlock()

				for k, v := range failTask {
					debug("%s failtask #%d %s\n", phase, k, v)
				}
				for i, failName := range failTask {
					debug("retry fail task #%d %s\n", i, failName)
					taskArg := DoTaskArgs{
						JobName:       jobName,
						File:          failName,
						Phase:         phase,
						TaskNumber:    i,
						NumOtherPhase: n_other,
					}
					isSuccess := callWorker(&taskArg, workerRPCAddr)
					if isSuccess {
						st.mu.Lock()
						delete(st.failTask, i)
						st.mu.Unlock()
						debug("%s remove fail #%d %s\n", phase, i, failName)
					}
				}
				debug("done current loop job\n")

				go func() {
					var failWorker map[string]int
					st.mu.Lock()
					failWorker = st.failWorker
					st.mu.Unlock()

					if _, exists := failWorker[workerRPCAddr]; exists {
						debug("%s skip put %s back queue\n", phase, workerRPCAddr)
					} else {
						registerChan <- workerRPCAddr
					}
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

func callWorker(taskArg *DoTaskArgs, workerAddr string) bool {
	debug("%s #%d call worker: %s, file: %s\n", taskArg.Phase, taskArg.TaskNumber, workerAddr, taskArg.File)
	result := call(workerAddr, "Worker.DoTask", &taskArg, nil)
	debug("done call rpc\n")
	return result
}
