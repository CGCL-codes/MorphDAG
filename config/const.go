package config

// Initialization parameter
const DBfile = "../dagdata/MorphDAG_%s"
const DBfile2 = "../dagdata/MorphDAG_State_%s"
const CommonLoads = "../experiments/loads.txt"
const LargeLoads = "../experiments/large_loads.txt"
const EthTxFile = "../data/newEthTxs1.txt"

// PoW parameter
const TargetBits = 12

// PoS parameter
const TotalStake = 1275

// P2P parameter
const CommandLength = 12
const MaximumPoolSize = 100000

// Blockchain parameter
const BlockSize = 1000
const SafeConcurrency = 90
const EpochTime = 6
const MaximumProcessors = 100000
const HotRatio = 0.01
const ColdMode = false
const HotMode = false

// Experimental results
const ExpResult1 = "../experiments/tps_appending_result.txt"
const ExpResult2 = "../experiments/tps_overall_result.txt"
