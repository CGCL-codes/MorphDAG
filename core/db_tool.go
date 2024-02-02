package core

import (
	"MorphDAG/core/types"
	"bytes"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"strconv"
)

// StoreBlock stores block data to db
func StoreBlock(db *leveldb.DB, blk types.Block) error {
	blockData := blk.Serialize()
	blockHash := blk.BlockHash
	key := bytes.Join([][]byte{[]byte("b"), blockHash[:]}, []byte{})
	// log.Println("Store block key: ", key)
	err := db.Put(key, blockData, nil)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

// StoreBlockHashes stores block hashes in the latest epoch
func StoreBlockHashes(db *leveldb.DB, chainSets ChainSets) error {
	data := chainSets.Serialize()
	err := db.Put([]byte("h"), data, nil)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

// StoreConcurrency stores block concurrency of a given height
func StoreConcurrency(db *leveldb.DB, height, con int) error {
	heightString := strconv.Itoa(height)
	conString := strconv.Itoa(con)
	err := db.Put([]byte(heightString), []byte(conString), nil)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

// RemoveBlock removes block from db
func RemoveBlock(db *leveldb.DB, blkHash []byte) error {
	key := bytes.Join([][]byte{[]byte("b"), blkHash[:]}, []byte{})
	err := db.Delete(key, nil)
	if err != nil {
		return errors.New("error happened in blk removing")
	}
	return nil
}

// FetchBlockHashes gets latest block hashes from db
func FetchBlockHashes(db *leveldb.DB) ([]byte, error) {
	key := []byte("h")
	hashes, err := db.Get(key, nil)
	if err != nil {
		return nil, errors.New("error happened in block hashes fetching")
	}
	return hashes, nil
}

func FetchConcurrency(db *leveldb.DB, height int) (string, error) {
	key := []byte(strconv.Itoa(height))
	con, err := db.Get(key, nil)
	if err != nil {
		return "", errors.New("error happened in concurrrency fetching")
	}
	return string(con), err
}

// FetchBlock gets block data via key from db
func FetchBlock(db *leveldb.DB, blkHash []byte) ([]byte, error) {
	key := bytes.Join([][]byte{[]byte("b"), blkHash[:]}, []byte{})
	// log.Println("Fetch block key:", key)
	value, err := db.Get(key, nil)
	if err != nil {
		return nil, errors.New("error happened in block fetching")
	}
	return value, nil
}

// StoreTx stores tx to db
func StoreTx(db *leveldb.DB, tx types.Transaction) error {
	txData := tx.Serialize()
	txHash := tx.Hash()
	// Todo: read-write set
	key := bytes.Join([][]byte{[]byte("t"), txHash[:]}, []byte{})
	log.Println("Store transaction key: ", key)
	err := db.Put(key, txData, nil)
	if err != nil {
		return errors.New("error happened in tx storing")
	}
	return nil
}

// RemoveTx removes tx from db
func RemoveTx(db *leveldb.DB, txHash []byte) error {
	key := bytes.Join([][]byte{[]byte("t"), txHash[:]}, []byte{})
	err := db.Delete(key, nil)
	if err != nil {
		return errors.New("error happened in tx removing")
	}
	return nil
}

// FetchTx gets value via key from db
func FetchTx(db *leveldb.DB, txHash []byte) ([]byte, error) {
	key := bytes.Join([][]byte{[]byte("t"), txHash[:]}, []byte{})
	log.Println("Fetch transaction key:", key)
	value, err := db.Get(key, nil)
	if err != nil {
		return nil, errors.New("error happened in tx fetching")
	}
	return value, nil
}

//func CompactDB(db *leveldb.DB) error {
//	dbRange := util.BytesPrefix([]byte("t"))
//	return db.CompactRange(*dbRange)
//}

// LoadDB opens db instance or create a db if not exist
func LoadDB(blkFile string) (*leveldb.DB, error) {
	// log.Println("LoadDB() function is called")
	db, err := leveldb.OpenFile(blkFile, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return db, nil
}
