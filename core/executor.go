package core

import (
	"MorphDAG/config"
	"MorphDAG/core/state"
	"MorphDAG/core/tp"
	"MorphDAG/core/types"
	"fmt"
	"time"
)

type Executor struct {
	dispatcher  *tp.Dispatcher
	state       *state.StateDB
	hotAccounts map[string]struct{}
}

// NewExecutor creates a new executor
func NewExecutor(state *state.StateDB, maxProcessors int) *Executor {
	return &Executor{
		dispatcher: tp.NewDispatcher(maxProcessors),
		state:      state,
	}
}

// Processing processes given transaction sets
func (e *Executor) Processing(txs map[string][]*types.Transaction, deps []string, ratio float32) ([]byte, time.Duration) {
	// update hotspot accounts
	start := time.Now()
	e.UpdateHotAccounts(txs, ratio, config.HotMode, config.ColdMode)
	// execute txs and commit state updates
	graph, hNum, cNum := tp.CreateGraph(deps, txs, e.hotAccounts)
	e.dispatcher.Run(graph, e.state, deps, hNum, cNum)
	stateRoot := e.state.Commit()
	duration := time.Since(start)
	fmt.Printf("Time of processing transactions is: %s\n", duration)
	// clear in-memory data (default interval: one epoch)
	e.state.Reset()
	return stateRoot, duration
}

// ProcessingTest processes given transaction sets (for test)
func (e *Executor) ProcessingTest(txs map[string][]*types.Transaction, deps []string, ratio float32) int64 {
	// update hotspot accounts
	start := time.Now()
	e.UpdateHotAccounts(txs, ratio, config.HotMode, config.ColdMode)
	// execute txs and commit state updates
	graph, hNum, cNum := tp.CreateGraph(deps, txs, e.hotAccounts)
	e.dispatcher.Run(graph, e.state, deps, hNum, cNum)
	e.state.Commit()
	duration := time.Since(start)
	fmt.Printf("Time of processing transactions is: %s\n", duration)
	// clear in-memory data (default interval: one epoch)
	e.state.Reset()
	return int64(duration)
}

// SerialProcessing serially processes given transaction sets
func (e *Executor) SerialProcessing(txs map[string][]*types.Transaction) {
	start := time.Now()
	for blk := range txs {
		for _, tx := range txs[blk] {
			msg := tx.AsMessage()
			err := tp.ApplyMessageForSerial(e.state, msg)
			if err != nil {
				panic(err)
			}
		}
	}
	e.state.Commit()
	duration := time.Since(start)
	fmt.Printf("Time of processing transactions is: %s\n", duration)
	e.state.Reset()
}

// SerialProcessingTest serially processes given transaction sets (for test)
func (e *Executor) SerialProcessingTest(txs map[string][]*types.Transaction) int64 {
	start := time.Now()
	for blk := range txs {
		for _, tx := range txs[blk] {
			msg := tx.AsMessage()
			err := tp.ApplyMessageForSerial(e.state, msg)
			if err != nil {
				panic(err)
			}
		}
	}
	e.state.Commit()
	duration := time.Since(start)
	fmt.Printf("Time of processing transactions is: %s\n", duration)
	e.state.Reset()
	return int64(duration)
}

func (e *Executor) UpdateHotAccounts(txs map[string][]*types.Transaction, ratio float32, hotMode, coldMode bool) {
	hotAccounts := AnalyzeHotAccounts(txs, ratio, hotMode, coldMode)
	e.hotAccounts = hotAccounts
}

func (e *Executor) GetHotAccounts() map[string]struct{} { return e.hotAccounts }
