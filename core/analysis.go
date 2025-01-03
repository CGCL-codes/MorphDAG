package core

import (
	"MorphDAG/config"
	"MorphDAG/core/types"
	"math"
	"sort"
)

// CalculateConcurrency calculates resilient block concurrency according to the pending pool size
func CalculateConcurrency(scale int) int {
	var resCon int
	// coefficient after fitting
	resCon = int(math.Ceil(0.005565*float64(scale) - 1.894))
	if resCon > config.MaximumConcurrency {
		return config.MaximumConcurrency
	}
	return resCon
}

// AnalyzeHotAccounts analyzes hot accounts in all concurrent blocks in the current epoch
func AnalyzeHotAccounts(txs map[string][]*types.Transaction, ratio float32, hotMode, coldMode bool) map[string]struct{} {
	var sum = make(map[string]int)
	var reverse = make(map[int][]string)
	var frequency []int
	var hotAccounts = make(map[string]struct{})

	if coldMode {
		return hotAccounts
	}

	for _, set := range txs {
		for _, tx := range set {
			for acc := range tx.Payload.RWSets {
				sum[acc]++
			}
		}
	}

	if hotMode {
		for addr := range sum {
			hotAccounts[addr] = struct{}{}
		}
		return hotAccounts
	}

	for addr, num := range sum {
		reverse[num] = append(reverse[num], addr)
		frequency = append(frequency, num)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(frequency)))

	// top-k mostly accessed accounts
	hotNum := float32(len(sum)) * ratio
	hotFreq := frequency[int(hotNum)-1]

	for _, freq := range frequency {
		if freq < hotFreq {
			break
		}
		hotAcc := reverse[freq]
		for _, acc := range hotAcc {
			hotAccounts[acc] = struct{}{}
		}
	}

	return hotAccounts
}
