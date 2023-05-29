package tp

import (
	"MorphDAG/core/state"
	"MorphDAG/core/types"
	"sort"
	"strings"
	"sync"
)

const MaxQueue = 200000

var hJobQueue chan []*RWNode
var cJobQueue chan []*RWNode
var hPending *types.ConcurPending
var cPending *types.ConcurPending
var hStopSignal chan struct{}
var cStopSignal chan struct{}

func reset(label string) {
	switch label {
	case "hot":
		hJobQueue = make(chan []*RWNode, MaxQueue)
		hPending = types.NewPending()
		hStopSignal = make(chan struct{})
	case "cold":
		cJobQueue = make(chan []*RWNode, MaxQueue)
		cPending = types.NewPending()
		cStopSignal = make(chan struct{})
	}
}

// Dispatcher masters all tx distributors
type Dispatcher struct {
	hDistributor *distributor
	cDistributor *distributor
}

func NewDispatcher(maxProcessors int) *Dispatcher {
	hDistributor := newDistributor(maxProcessors, "hot")
	cDistributor := newDistributor(maxProcessors, "cold")
	return &Dispatcher{hDistributor, cDistributor}
}

// distributor assign txs to threads
type distributor struct {
	processorPool chan chan *RWNode
	load          int
}

func newDistributor(maxProcessors int, label string) *distributor {
	reset(label)
	pool := make(chan chan *RWNode, maxProcessors)
	return &distributor{processorPool: pool}
}

func (d *Dispatcher) Run(q QueueGraph, statedb *state.StateDB, deps []string, hNum, cNum int) {
	var wg sync.WaitGroup
	tracker := NewTracker(q, deps)

	wg.Add(3)

	go func() {
		defer wg.Done()
		tracker.Run()
	}()
	go func() {
		defer wg.Done()
		if hNum > 0 {
			d.hDistributor.load = hNum
			d.hDistributor.dispatch(statedb, "hot")
		}
	}()
	go func() {
		defer wg.Done()
		if cNum > 0 {
			d.cDistributor.load = cNum
			d.cDistributor.dispatch(statedb, "cold")
		}
	}()

	wg.Wait()
}

func (ds *distributor) dispatch(statedb *state.StateDB, label string) {
	var totalNum int
	var jobQueue chan []*RWNode
	var pending *types.ConcurPending
	var stopSignal chan struct{}

	// pick the first parallel group and determine the number of processors
	if label == "hot" {
		jobQueue = hJobQueue
		pending = hPending
		stopSignal = hStopSignal
	} else {
		jobQueue = cJobQueue
		pending = cPending
		stopSignal = cStopSignal
	}

	parallel := <-jobQueue
	totalNum += len(parallel)
	for i := 0; i < len(parallel); i++ {
		processor := NewProcessor(ds.processorPool)
		processor.Start(statedb, label)
	}
	record := recordID(parallel)
	pending.BatchAppend(record)
	ds.transferNodes(parallel)

	for {
		for {
			// if and only if the previous dependent transactions are executed
			if pending.Len() == 0 {
				break
			}
		}

		//fmt.Println(totalNum)
		if totalNum == ds.load {
			// kill all the processors if all tasks are completed
			close(stopSignal)
			return
		}

		select {
		case jobs := <-jobQueue:
			totalNum += len(jobs)
			poolSize := len(ds.processorPool)
			if poolSize < len(jobs) {
				// add the number of processors
				addNum := len(jobs) - poolSize
				for i := 0; i < addNum; i++ {
					processor := NewProcessor(ds.processorPool)
					processor.Start(statedb, label)
				}
			}
			record2 := recordID(jobs)
			pending.BatchAppend(record2)
			ds.transferNodes(jobs)
		}
	}
}

func (ds *distributor) transferNodes(parallel []*RWNode) {
	for _, node := range parallel {
		go func(node *RWNode) {
			// fetch the available (free) thread and assign tx to it
			processor := <-ds.processorPool
			processor <- node
		}(node)
	}
}

// Tracker track dependency and put txs into the JobQueue
type Tracker struct {
	graph QueueGraph
	deps  []string
}

func NewTracker(cq QueueGraph, deps []string) *Tracker {
	return &Tracker{cq, deps}
}

func (t *Tracker) Run() {
	//s := time.Now()
	var hcaddrs []string
	var haddrs []string
	var caddrs []string

	for key := range t.graph.HcQueues {
		hcaddrs = append(hcaddrs, key)
	}
	sort.Strings(hcaddrs)

	t.arrangeRNodes("combined", hcaddrs)
	t.arrangeRWNodes("combined", hcaddrs)
	t.arrangeWNodes("combined", hcaddrs)

	// parallelize pure hot nodes and cold nodes
	var wg sync.WaitGroup
	wg.Add(2)
	hotQueues := t.graph.HotQueues
	coldQueues := t.graph.ColdQueues

	go func() {
		defer wg.Done()
		for key := range hotQueues {
			haddrs = append(haddrs, key)
		}
		sort.Strings(haddrs)
		t.arrangeRNodes("pureHot", haddrs)
		t.arrangeRWNodes("pureHot", haddrs)
		t.arrangeWNodes("pureHot", haddrs)
	}()

	go func() {
		defer wg.Done()
		for key := range coldQueues {
			caddrs = append(caddrs, key)
		}
		sort.Strings(caddrs)
		t.arrangeColdNodes(caddrs, coldQueues)
	}()

	wg.Wait()
	//ee := time.Since(s)
	//fmt.Printf("time of scheduling: %s\n", ee)
}

func (t *Tracker) arrangeRNodes(gType string, addrs []string) {
	rNodes := t.graph.CombineNodes(gType, "r", t.deps, addrs)
	t.loopSeekParallel(rNodes)
}

func (t *Tracker) arrangeRNodes2(addrs []string) [][]*RWNode {
	rNodes := t.graph.CombineNodes("pureHot", "r", t.deps, addrs)
	parallelnodes := t.loopSeekParallel2(rNodes)
	return parallelnodes
}

func (t *Tracker) arrangeRWNodes(gType string, addrs []string) {
	var queues map[string]*Queue
	if gType == "pureHot" {
		queues = t.graph.HotQueues
	} else {
		queues = t.graph.HcQueues
	}

	for {
		var col []*RWNode
		// fetch nodes
		for _, addr := range addrs {
			q := queues[addr]
			for i := 0; i < len(q.rwSlice); i++ {
				node := q.rwSlice[i]
				if !node.IsExecuted() && !ExistInNodes(col, node) {
					col = append(col, node)
					break
				}
			}
		}

		if len(col) == 0 {
			break
		}

		t.advancedSeekParallel(col)
	}
}

func (t *Tracker) arrangeRWNodes2(addrs []string) [][]*RWNode {
	var parallelnodes [][]*RWNode
	for {
		var col []*RWNode
		// fetch nodes
		for _, addr := range addrs {
			q := t.graph.HotQueues[addr]
			for i := 0; i < len(q.rwSlice); i++ {
				node := q.rwSlice[i]
				if !node.IsExecuted() && !ExistInNodes(col, node) {
					col = append(col, node)
					break
				}
			}
		}

		if len(col) == 0 {
			break
		}

		res := t.advancedSeekParallel2(col)
		parallelnodes = append(parallelnodes, res)
	}
	return parallelnodes
}

func (t *Tracker) arrangeWNodes(gType string, addrs []string) {
	wNodes := t.graph.CombineNodes(gType, "iw", t.deps, addrs)
	var col []*RWNode

	// delete executed txs
	for _, node := range wNodes {
		if !node.IsExecuted() {
			col = append(col, node)
		}
	}

	hJobQueue <- col
	// no need to check for conflicts
	//t.loopSeekParallel(col)
}

func (t *Tracker) arrangeWNodes2(addrs []string) []*RWNode {
	wNodes := t.graph.CombineNodes("pureHot", "iw", t.deps, addrs)
	var col []*RWNode

	// delete executed txs
	for _, node := range wNodes {
		if !node.IsExecuted() {
			col = append(col, node)
		}
	}

	return col
}

// parallelHotAndCold pipelines the execution of pure hotspot txs and pure cold txs
//func (t *Tracker) parallelHotAndCold(addrs []string, coldNodes [][]*RWNode) {
//	// put a parallel tx set into the JobQueue
//	r1 := t.arrangeRNodes2(addrs)
//	if len(r1) < len(coldNodes) {
//		r2 := t.arrangeRWNodes2(addrs)
//		for i := 0; i < len(r1); i++ {
//			var parallel []*RWNode
//			parallel = append(parallel, r1[i]...)
//			parallel = append(parallel, coldNodes[i]...)
//			jobQueue <- parallel
//		}
//		coldNodes = coldNodes[len(r1):]
//
//		if len(r2) < len(coldNodes) {
//			r3 := t.arrangeWNodes2(addrs)
//			for j := 0; j < len(coldNodes); j++ {
//				var parallel []*RWNode
//				if j < len(r2) {
//					parallel = append(parallel, r2[j]...)
//					parallel = append(parallel, coldNodes[j]...)
//				} else if j == len(r2)+1 {
//					parallel = append(parallel, r3...)
//					parallel = append(parallel, coldNodes[j]...)
//				} else {
//					parallel = coldNodes[j]
//				}
//				jobQueue <- parallel
//			}
//		} else {
//			for j := 0; j < len(r2); j++ {
//				var parallel []*RWNode
//				if j < len(coldNodes) {
//					parallel = append(parallel, r2[j]...)
//					parallel = append(parallel, coldNodes[j]...)
//				} else {
//					parallel = r2[j]
//				}
//				jobQueue <- parallel
//			}
//			t.arrangeWNodes("pureHot", addrs)
//		}
//	} else {
//		for i := 0; i < len(r1); i++ {
//			var parallel []*RWNode
//			if i < len(coldNodes) {
//				parallel = append(parallel, r1[i]...)
//				parallel = append(parallel, coldNodes[i]...)
//			} else {
//				parallel = r1[i]
//			}
//			jobQueue <- parallel
//		}
//		t.arrangeRWNodes("pureHot", addrs)
//		t.arrangeWNodes("pureHot", addrs)
//	}
//}

func (t *Tracker) arrangeColdNodes(addrs []string, coldQueues map[string][]*RWNode) {
	for {
		// ID -> [RWNode1, RWNode1,...]
		var parallel = make(map[string][]*RWNode)
		var group []*RWNode
		var final []*RWNode

		for _, addr := range addrs {
			cq := coldQueues[addr]
			for i := 0; i < len(cq); i++ {
				if !cq[i].IsExecuted() {
					group = append(group, cq[i])
					break
				}
			}
		}

		if len(group) == 0 {
			break
		}

		for _, node := range group {
			id := node.tx.String()
			parallel[id] = append(parallel[id], node)
		}

		for id := range parallel {
			node := parallel[id][0]
			// if all the operations can be obtained
			if node.ops == len(parallel[id]) {
				final = append(final, node)
				node.isExecuted = true
			}
		}

		cJobQueue <- final
	}
}

//func (t *Tracker) arrangeColdNodes(addrs []string) [][]*RWNode {
//	var finals [][]*RWNode
//	for {
//		// ID -> [RWNode1, RWNode1,...]
//		var parallel = make(map[string][]*RWNode)
//		var group []*RWNode
//		var final []*RWNode
//
//		for _, addr := range addrs {
//			cq := t.graph.ColdQueues[addr]
//			for i := 0; i < len(cq); i++ {
//				if !cq[i].IsExecuted() {
//					group = append(group, cq[i])
//					break
//				}
//			}
//		}
//
//		if len(group) == 0 {
//			break
//		}
//
//		for _, node := range group {
//			id := string(node.tx.ID)
//			parallel[id] = append(parallel[id], node)
//		}
//
//		for id := range parallel {
//			node := parallel[id][0]
//			// if all the operations can be obtained
//			if node.ops == len(parallel[id]) {
//				final = append(final, node)
//				node.isExecuted = true
//			}
//		}
//		//jobQueue <- final
//		finals = append(finals, final)
//	}
//	return finals
//}

func (t *Tracker) seekParallel(nodes []*RWNode) {
	var parallel []*RWNode

	for i := 0; i < len(nodes); i++ {
		if i == 0 {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
			continue
		}

		isParallel := true
		for j := 0; j < len(parallel); j++ {
			if isConflict := isInConflict(nodes[i], parallel[j]); isConflict {
				isParallel = false
				break
			}
		}
		if isParallel {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
		}
	}
	// put a parallel tx set into the JobQueue
	hJobQueue <- parallel
}

func (t *Tracker) loopSeekParallel(nodes []*RWNode) {
	for {
		var parallel []*RWNode
		var remaining []*RWNode
		tiny := initializeTinyQueue()

		for i := 0; i < len(nodes); i++ {
			if i == 0 {
				parallel = append(parallel, nodes[i])
				nodes[i].isExecuted = true
				tiny.insertNode(nodes[i])
				continue
			}
			// check if the current node is in conflict with the added nodes
			isConflict := tiny.isInConflict(nodes[i])
			if !isConflict {
				parallel = append(parallel, nodes[i])
				nodes[i].isExecuted = true
				tiny.insertNode(nodes[i])
			} else {
				remaining = append(remaining, nodes[i])
			}
		}
		// put a parallel tx set into the JobQueue
		hJobQueue <- parallel
		nodes = remaining
		if len(nodes) == 0 {
			break
		}
	}
}

func (t *Tracker) loopSeekParallel2(nodes []*RWNode) [][]*RWNode {
	var parallels [][]*RWNode
	for {
		var parallel []*RWNode
		var remaining []*RWNode
		tiny := initializeTinyQueue()

		for i := 0; i < len(nodes); i++ {
			if i == 0 {
				parallel = append(parallel, nodes[i])
				nodes[i].isExecuted = true
				tiny.insertNode(nodes[i])
				continue
			}
			// check if the current node is in conflict with the added nodes
			isConflict := tiny.isInConflict(nodes[i])
			if !isConflict {
				parallel = append(parallel, nodes[i])
				nodes[i].isExecuted = true
				tiny.insertNode(nodes[i])
			} else {
				remaining = append(remaining, nodes[i])
			}
		}
		parallels = append(parallels, parallel)
		nodes = remaining
		if len(nodes) == 0 {
			break
		}
	}
	return parallels
}

func (t *Tracker) advancedSeekParallel(nodes []*RWNode) {
	var parallel []*RWNode
	tiny := initializeTinyQueue()

	for i := 0; i < len(nodes); i++ {
		if i == 0 {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
			tiny.insertNode(nodes[i])
			continue
		}
		// check if the current node is in conflict with the added nodes
		isConflict := tiny.isInConflict(nodes[i])
		if !isConflict {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
			tiny.insertNode(nodes[i])
		}
	}
	// put a parallel tx set into the JobQueue
	hJobQueue <- parallel
}

func (t *Tracker) advancedSeekParallel2(nodes []*RWNode) []*RWNode {
	var parallel []*RWNode
	tiny := initializeTinyQueue()

	for i := 0; i < len(nodes); i++ {
		if i == 0 {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
			tiny.insertNode(nodes[i])
			continue
		}
		// check if the current node is in conflict with the added nodes
		isConflict := tiny.isInConflict(nodes[i])
		if !isConflict {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
			tiny.insertNode(nodes[i])
		}
	}
	return parallel
}

type tinyQueue struct {
	tiny map[string]*queue
}

type queue struct {
	rSlice  []*RWNode
	wSlice  []*RWNode
	iwSlice []*RWNode
}

func initializeTinyQueue() *tinyQueue {
	return &tinyQueue{tiny: make(map[string]*queue)}
}

func (tq *tinyQueue) insertNode(node *RWNode) {
	rwSet := GenerateRWSet(node)
	for addr := range rwSet {
		if _, ok := tq.tiny[addr]; !ok {
			newQueue := &queue{
				rSlice:  make([]*RWNode, 0, 5000),
				wSlice:  make([]*RWNode, 0, 5000),
				iwSlice: make([]*RWNode, 0, 5000),
			}
			tq.tiny[addr] = newQueue
		}
		hasRInserted := false
		hasWInserted := false
		hasIWInserted := false
		for _, rw := range rwSet[addr] {
			if strings.Compare(rw.Label, "r") == 0 && !hasRInserted {
				tq.tiny[addr].rSlice = append(tq.tiny[addr].rSlice, node)
				hasRInserted = true
			} else if strings.Compare(rw.Label, "w") == 0 && !hasWInserted {
				tq.tiny[addr].wSlice = append(tq.tiny[addr].wSlice, node)
				hasWInserted = true
			} else if strings.Compare(rw.Label, "iw") == 0 && !hasIWInserted {
				tq.tiny[addr].iwSlice = append(tq.tiny[addr].iwSlice, node)
				hasIWInserted = true
			}
		}
	}
}

func (tq *tinyQueue) isInConflict(node *RWNode) bool {
	rwSet := GenerateRWSet(node)

	for addr := range rwSet {
		if q, ok := tq.tiny[addr]; ok {
			for _, rw := range rwSet[addr] {
				if strings.Compare(rw.Label, "r") == 0 {
					if len(q.wSlice) > 0 || len(q.iwSlice) > 0 {
						return true
					}
				} else if strings.Compare(rw.Label, "w") == 0 {
					if len(q.rSlice) > 0 || len(q.wSlice) > 0 || len(q.iwSlice) > 0 {
						return true
					}
				} else {
					if len(q.rSlice) > 0 || len(q.wSlice) > 0 {
						return true
					}
				}
			}
		}
	}

	return false
}

func isInConflict(node1, node2 *RWNode) bool {
	rwSet1 := GenerateRWSet(node1)
	rwSet2 := GenerateRWSet(node2)

	for addr := range rwSet1 {
		if _, ok := rwSet2[addr]; ok {
			// conflict condition: (node1 r : node2 w), (node1 w : node2 r)
			read1, write1, read2, write2 := false, false, false, false

			for _, rw := range rwSet1[addr] {
				if strings.Compare(rw.Label, "r") == 0 {
					read1 = true
				}
				if strings.Compare(rw.Label, "w") == 0 {
					write1 = true
				}
			}

			for _, rw := range rwSet2[addr] {
				if strings.Compare(rw.Label, "r") == 0 {
					read2 = true
				}
				if strings.Compare(rw.Label, "w") == 0 {
					write2 = true
				}
			}

			if (read1 && write2) || (write1 && read2) {
				return true
			}
			return false
		}
	}
	return false
}

func recordID(nodes []*RWNode) []string {
	var record []string
	for _, node := range nodes {
		newID := node.tx.String()
		record = append(record, newID)
	}
	return record
}
