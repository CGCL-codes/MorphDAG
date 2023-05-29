package tp

import (
	"MorphDAG/core/types"
	"reflect"
	"strings"
)

// QueueGraph used to sort transactions for scheduling txs to threads
type QueueGraph struct {
	HotQueues  map[string]*Queue
	HcQueues   map[string]*Queue
	ColdQueues map[string][]*RWNode
}

type Queue struct {
	rSlice  []*RWNode
	rwSlice []*RWNode
	wSlice  []*RWNode
}

type RWNode struct {
	tx         *types.Transaction
	blkID      string
	location   int64
	ops        int // number of read/write operations
	hasHot     bool
	hasCold    bool
	isExecuted bool
}

func (node *RWNode) Tx() *types.Transaction { return node.tx }
func (node *RWNode) BlkId() string          { return node.blkID }
func (node *RWNode) Location() int64        { return node.location }
func (node *RWNode) Ops() int               { return node.ops }
func (node *RWNode) SetOps(ops int)         { node.ops = ops }
func (node *RWNode) HasHot() bool           { return node.hasHot }
func (node *RWNode) HasCold() bool          { return node.hasCold }
func (node *RWNode) IsExecuted() bool       { return node.isExecuted }

// CreateGraph creates a new queue graph
func CreateGraph(deps []string, txs map[string][]*types.Transaction, hotAccounts map[string]struct{}) (QueueGraph, int, int) {
	var hcQueues = make(map[string]*Queue)
	var hotQueues = make(map[string]*Queue)
	graph := mapToGraph(deps, txs, hotAccounts)
	hotGraph, hcGraph, coldGraph, hNum, cNum := divide(graph, hotAccounts)

	arrangeHotspots(hcGraph, hcQueues)
	arrangeHotspots(hotGraph, hotQueues)

	return QueueGraph{HotQueues: hotQueues, HcQueues: hcQueues, ColdQueues: coldGraph}, hNum, cNum
}

// CombineNodes combines all nodes in a given set
func (q QueueGraph) CombineNodes(gType string, rwType string, deps []string, addrs []string) []*RWNode {
	var sortedMap = make(map[string][]*RWNode)
	var queues map[string]*Queue
	var combined []*RWNode

	if gType == "pureHot" {
		queues = q.HotQueues
	} else {
		queues = q.HcQueues
	}

	for _, addr := range addrs {
		qq := queues[addr]
		var slice []*RWNode

		if strings.Compare(rwType, "r") == 0 {
			slice = qq.rSlice
		} else {
			slice = qq.wSlice
		}

		for _, node := range slice {
			rank := node.blkID
			if !ExistInNodes(sortedMap[rank], node) {
				sortedMap[rank] = append(sortedMap[rank], node)
			}
		}
	}

	for _, id := range deps {
		if _, ok := sortedMap[id]; ok {
			combined = append(combined, sortedMap[id]...)
		}
	}

	return combined
}

// arrangeHotspots arrange nodes accessing hotspots to the QueueGraph
func arrangeHotspots(graph map[string][]*RWNode, queues map[string]*Queue) {
	var addrs []string
	for addr := range graph {
		addrs = append(addrs, addr)
	}
	initialGraph(queues, addrs)

	for _, addr := range addrs {
		for _, node := range graph[addr] {
			rwSets := GenerateRWSet(node)
			isWrite := false
			isInWrite := false

			for _, rw := range rwSets[addr] {
				if strings.Compare(rw.Label, "w") == 0 {
					isWrite = true
				}
				if strings.Compare(rw.Label, "iw") == 0 {
					isInWrite = true
				}
			}

			if isInWrite {
				queues[addr].wSlice = append(queues[addr].wSlice, node)
			} else if isWrite {
				queues[addr].rwSlice = append(queues[addr].rwSlice, node)
			} else {
				queues[addr].rSlice = append(queues[addr].rSlice, node)
			}
		}
	}
}

// mapToGraph initializes rwNodes and maps them to the basic graph
func mapToGraph(deps []string, txs map[string][]*types.Transaction, hotAccounts map[string]struct{}) map[string][]*RWNode {
	var graph = make(map[string][]*RWNode)
	rwNodes := createRWNodes(deps, txs)

	for _, node := range rwNodes {
		rwSets := GenerateRWSet(node)
		node.SetOps(len(rwSets))
		for addr := range rwSets {
			if _, ok := hotAccounts[addr]; ok {
				if !node.hasHot {
					node.hasHot = true
				}
			} else {
				if !node.hasCold {
					node.hasCold = true
				}
			}
			graph[addr] = append(graph[addr], node)
		}
	}

	return graph
}

// divide partitions the graph into three sub-graphs
func divide(graph map[string][]*RWNode, hotAccounts map[string]struct{}) (map[string][]*RWNode, map[string][]*RWNode, map[string][]*RWNode, int, int) {
	var hotGraph = make(map[string][]*RWNode)
	var hcGraph = make(map[string][]*RWNode)
	var coldGraph = make(map[string][]*RWNode)
	var hotTx = make(map[string]struct{})
	var coldTx = make(map[string]struct{})

	for addr, nodes := range graph {
		for _, node := range nodes {
			if node.hasHot && node.hasCold {
				hotTx[node.tx.String()] = struct{}{}
				// access hotspot and cold accounts simultaneously
				if _, ok := hotAccounts[addr]; ok {
					hcGraph[addr] = append(hcGraph[addr], node)
				}
			} else if node.hasHot && !node.hasCold {
				hotTx[node.tx.String()] = struct{}{}
				hotGraph[addr] = append(hotGraph[addr], node)
			} else {
				coldTx[node.tx.String()] = struct{}{}
				coldGraph[addr] = append(coldGraph[addr], node)
			}
		}
	}
	//	if _, ok := hotAccounts[addr]; ok {
	//		// add to the hotGraph
	//		for _, node := range graph[addr] {
	//			if !node.isHot {
	//				node.isHot = true
	//			}
	//		}
	//		hotGraph[addr] = graph[addr]
	//	} else {
	//		// add to the coldGraph
	//		coldGraph[addr] = graph[addr]
	//	}
	//}

	//// remove some hot txs accessing cold accounts
	//for addr := range coldGraph {
	//	var newSet []*RWNode
	//	for _, node := range coldGraph[addr] {
	//		if !node.isHot {
	//			newSet = append(newSet, node)
	//		}
	//	}
	//	coldGraph[addr] = newSet

	return hotGraph, hcGraph, coldGraph, len(hotTx), len(coldTx)
}

// initialGraph initializes each rw node set in queues
func initialGraph(queues map[string]*Queue, addrs []string) {
	for _, addr := range addrs {
		queues[addr] = &Queue{
			rSlice:  make([]*RWNode, 0, 20000),
			rwSlice: make([]*RWNode, 0, 20000),
			wSlice:  make([]*RWNode, 0, 20000),
		}
	}
}

// createRWNodes create rw nodes for a given transaction set
func createRWNodes(deps []string, txs map[string][]*types.Transaction) []*RWNode {
	var rwNodes []*RWNode

	for _, key := range deps {
		txList := txs[key]
		for i, tx := range txList {
			rwNode := &RWNode{tx: tx, blkID: key, location: int64(i)}
			rwNodes = append(rwNodes, rwNode)
		}
	}

	return rwNodes
}

func GenerateRWSet(node *RWNode) map[string][]*types.RWSet {
	var rwSet map[string][]*types.RWSet
	if node.tx.Payload == nil {
		// judge if tx is a common transfer
		rwSet = node.tx.CreateRWSets()
	} else {
		rwSet = node.tx.Payload.RWSets
	}

	return rwSet
}

func ExistInNodes(slice []*RWNode, node *RWNode) bool {
	for _, n := range slice {
		if reflect.DeepEqual(node.tx.String(), n.tx.String()) {
			return true
		}
	}
	return false
}
