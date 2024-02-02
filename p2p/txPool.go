package p2p

import (
	"MorphDAG/config"
	"MorphDAG/core/types"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type TxPool struct {
	pending *pendingPool
	queue   *sync.Map
}

func NewTxPool() *TxPool {
	pool := &TxPool{
		pending: newPending(),
		queue:   new(sync.Map),
	}

	return pool
}

// GetScale gets the current workload scale
func (tp *TxPool) GetScale() int {
	//return tp.pending.len()
	length := int(atomic.LoadInt64(&tp.pending.length))
	fmt.Printf("tx pool size: %d\n", length)
	return length
}

// RetrievePending retrieves txs from the pending pool and adds them into the queue pool
func (tp *TxPool) RetrievePending() {
	pendingTxs := tp.pending.empty()
	tp.queue = pendingTxs
}

// Pick randomly picks #size txs into a new block
func (tp *TxPool) Pick(size int) []*types.Transaction {
	var selectedTxs = make(map[int]struct{})
	var ids []string
	var txs []*types.Transaction
	var pickSize int
	var poolSize int

	tp.queue.Range(func(key, value any) bool {
		id := key.(string)
		ids = append(ids, id)
		poolSize++
		return true
	})

	// if the current pool length is smaller than batchsize, then pick all the txs in the pool
	if poolSize < size {
		pickSize = poolSize
	} else {
		pickSize = size
	}

	for i := 0; i < pickSize; i++ {
		for {
			rand.Seed(time.Now().UnixNano())
			random := rand.Intn(len(ids))
			if _, ok := selectedTxs[random]; !ok {
				selectedTxs[random] = struct{}{}
				break
			}
		}
	}

	for key := range selectedTxs {
		id := ids[key]
		v, _ := tp.queue.Load(id)
		t, ok := v.(*types.Transaction)
		if ok {
			txs = append(txs, t)
		}
	}

	return txs
}

// DeleteTxs deletes txs in other concurrent blocks
func (tp *TxPool) DeleteTxs(txs map[string]struct{}) []*types.Transaction {
	var deleted []*types.Transaction

	for del := range txs {
		if v, ok := tp.queue.Load(del); ok {
			t := v.(*types.Transaction)
			deleted = append(deleted, t)
			tp.queue.Delete(del)
		}
	}

	return deleted
}

type pendingPool struct {
	mu     sync.Mutex
	temp   *sync.Map
	txs    []*types.Transaction
	length int64
}

func (pp *pendingPool) append(tx *types.Transaction) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if _, ok := pp.temp.Load(tx.String()); !ok {
		pp.txs = append(pp.txs, tx)
		pp.temp.Store(tx.String(), tx)
		atomic.AddInt64(&pp.length, 1)
	}
}

func (pp *pendingPool) batchAppend(txs []*types.Transaction) {
	for _, t := range txs {
		pp.append(t)
	}
}

func (pp *pendingPool) pop() *types.Transaction {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	last := pp.txs[len(pp.txs)-1]
	pp.txs = pp.txs[:len(pp.txs)-1]
	return last
}

func (pp *pendingPool) delete(tx *types.Transaction) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	for i := range pp.txs {
		if pp.txs[i] == tx {
			pp.txs = append(pp.txs[:i], pp.txs[i+1:]...)
			break
		}
	}
	//id := string(tx.ID)
	//delete(pp.txs, id)
}

func (pp *pendingPool) len() int64 {
	return atomic.LoadInt64(&pp.length)
}

func (pp *pendingPool) swap(i, j int) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.txs[i], pp.txs[j] = pp.txs[j], pp.txs[i]
}

func (pp *pendingPool) isFull() bool {
	return atomic.LoadInt64(&pp.length) == config.MaximumPoolSize
}

func (pp *pendingPool) isEmpty() bool {
	return atomic.LoadInt64(&pp.length) == 0
}

func (pp *pendingPool) empty() *sync.Map {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	temp := new(sync.Map)
	if !pp.isEmpty() {
		temp = pp.temp
		pp.txs = make([]*types.Transaction, 0, config.MaximumPoolSize)
		pp.temp = new(sync.Map)
		pp.length = int64(0)
	}

	return temp
}

func newPending() *pendingPool {
	return &pendingPool{
		temp:   new(sync.Map),
		txs:    make([]*types.Transaction, 0, config.MaximumPoolSize),
		length: int64(0),
	}
}
