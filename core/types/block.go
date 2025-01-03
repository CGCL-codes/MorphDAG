package types

import (
	"MorphDAG/utils"
	"bytes"
	"encoding/gob"
	"github.com/wealdtech/go-merkletree"
	"log"
	"time"
)

// Block represents a block in the blockchain
type Block struct {
	Timestamp int64
	//Transactions   []*Transaction
	// TODO: separate block header and body transfer
	//Transactions   map[string]struct{}
	MerkleRootHash []byte
	MerkleProof    *merkletree.Proof
	PrevBlockHash  [][]byte
	BlockHash      []byte
	TxRoot         []byte
	StateRoot      []byte
	Nonce          int
	Epoch          int
	Info           *VRFInfo
}

// VRFInfo denotes the information for vrf verification
type VRFInfo struct {
	Pk    []byte
	Hash  []byte
	Pi    []byte
	Phi   int
	Stake int
}

// NewBlock creates and returns Block
func NewBlock(txs map[string]struct{}, rootHash, stateRoot []byte, height, con, stake int) *Block {
	block := &Block{
		time.Now().Unix(),
		rootHash,
		new(merkletree.Proof),
		[][]byte{},
		[]byte{},
		[]byte{},
		stateRoot,
		0,
		height,
		nil,
	}

	block.TxRoot = HashTransactions(txs)
	newvrf := NewVRF(block, con, stake)
	selected, hash, info := newvrf.Sortition()

	if !selected {
		return nil
	}

	block.BlockHash = hash[:]
	block.Info = info

	return block
}

// NewGenesisBlock creates and returns genesis Block
func NewGenesisBlock(num int) *Block {
	data := utils.IntToHex(int64(num*1000 + 1000))
	return &Block{
		0,
		[]byte{},
		new(merkletree.Proof),
		[][]byte{},
		data,
		[]byte{},
		[]byte{},
		0,
		0,
		nil,
	}
}

// Serialize serializes the block
func (b *Block) Serialize() []byte {
	var result bytes.Buffer
	encoder := gob.NewEncoder(&result)

	err := encoder.Encode(b)
	if err != nil {
		log.Panic(err)
	}

	return result.Bytes()
}

// DeserializeBlock deserializes a block
func DeserializeBlock(d []byte) *Block {
	var block Block

	decoder := gob.NewDecoder(bytes.NewReader(d))
	err := decoder.Decode(&block)
	if err != nil {
		log.Panic(err)
	}

	return &block
}

// HashTransactions returns a hash of the transactions in the block
func HashTransactions(txs map[string]struct{}) []byte {
	var transactions [][]byte
	for id := range txs {
		transactions = append(transactions, []byte(id))
	}

	mTree, err := merkletree.New(transactions)
	if err != nil {
		log.Panic(err)
	}

	root := mTree.Root()
	return root
}
