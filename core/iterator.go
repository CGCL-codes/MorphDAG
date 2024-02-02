package core

import (
	"MorphDAG/core/types"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
)

// Iterator is used to iterate over blockchain blocks
type Iterator struct {
	currentHash map[string]struct{}
	db          *leveldb.DB
}

// Previous returns previous block hashes starting from the current epoch
func (i *Iterator) Previous() []*types.Block {
	var hashes = make(map[string]struct{})
	var blks []*types.Block

	for hash := range i.currentHash {
		blockData, err := FetchBlock(i.db, []byte(hash))
		if err != nil {
			log.Panic(err)
		}
		block := types.DeserializeBlock(blockData)
		blks = append(blks, block)
		for _, prevHash := range block.PrevBlockHash {
			hashes[string(prevHash)] = struct{}{}
		}
	}

	i.currentHash = hashes
	return blks
}
