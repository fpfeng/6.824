package mapreduce

import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
)

type KVArray []struct{ KeyValue }

func (kva KVArray) Len() int {
	return len(kva)
}

func (kva KVArray) Swap(i, j int) {
	kva[i], kva[j] = kva[j], kva[i]
}

func (kva KVArray) Less(i, j int) bool {
	return kva[i].Key < kva[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	intermediateFileName := reduceName(jobName, nMap-1, reduceTask)
	f, err := os.Open(intermediateFileName)
	defer f.Close()
	checkError(err)

	r := bufio.NewReader(f)
	dec := json.NewDecoder(r)

	var KVs KVArray
	err = dec.Decode(&KVs)
	checkError(err)

	sort.Sort(KVs)

	group := make(map[string][]string)

	for _, kv := range KVs {
		if group[kv.Key] == nil {
			group[kv.Key] = make([]string, 0)
		}

		currentKeyContent := group[kv.Key]
		currentKeyContent = append(currentKeyContent, kv.Value)

		group[kv.Key] = currentKeyContent
	}

	f, err = os.Create("outFile")
	defer f.Close()
	checkError(err)

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)

	for k, values := range group {
		enc.Encode(KeyValue{k, reduceF(k, values)})
	}
}
