package types

import (
	"MorphDAG/config"
	"MorphDAG/utils"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	m "github.com/ethereum/go-ethereum/common/math"
	"github.com/yoseplee/vrf"
	"log"
	"math"
	"math/big"
	"math/rand"
	"time"
)

type VRF struct {
	block *Block
	stake int
	phi   int // sortition target
}

// NewVRF creates a new vrf instance
func NewVRF(b *Block, con int, stake int) *VRF {
	newvrf := &VRF{block: b, stake: stake}
	newvrf.updateTarget(con)
	return newvrf
}

// Sortition conducts a cryptographic sortition
func (vrf *VRF) Sortition() (bool, []byte, *VRFInfo) {
	pk, pi, h := generateRandom()

	intValue := new(big.Int).SetBytes(h)
	hashlen := m.BigPow(2, 256)

	intValue2 := new(big.Float).SetInt(intValue)
	hashlen2 := new(big.Float).SetInt(hashlen)

	res, _ := intValue2.Quo(intValue2, hashlen2).Float64()

	p := float64(vrf.phi) / float64(config.TotalStake)
	pp := math.Pow(1-p, float64(vrf.stake))

	if res >= pp {
		// selected as a block proposer
		fmt.Println("selected!")
		blkHash := vrf.prepareData(pk, h, pi)
		vrfInfo := &VRFInfo{pk, h, pi, vrf.phi, vrf.stake}
		return true, blkHash, vrfInfo
	}

	// fmt.Println("fail to be selected")
	return false, nil, nil
}

// prepareData prepares necessary data for block creation
func (vrf *VRF) prepareData(pk, hash, pi []byte) []byte {
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(100000000)

	data := bytes.Join(
		[][]byte{
			vrf.block.HashTransactions(),
			utils.IntToHex(int64(vrf.phi)),
			utils.IntToHex(int64(vrf.stake)),
			utils.IntToHex(int64(r)),
			pk, hash, pi,
		},
		[]byte{},
	)

	blkHash := sha256.Sum256(data)

	return blkHash[:]
}

// updateInput updates a sortition target according to the degree of concurrency
func (vrf *VRF) updateTarget(con int) {
	// coefficient after fitting
	newTarget := int(math.Ceil(2.208e-13*math.Pow(float64(con), 9) - 8.07e-11*math.Pow(float64(con), 8) + 1.24e-08*math.Pow(float64(con), 7) - 1.037e-06*math.Pow(float64(con), 6) + 5.115e-05*math.Pow(float64(con), 5) - 0.001507*math.Pow(float64(con), 4) + 0.02579*math.Pow(float64(con), 3) - 0.2265*math.Pow(float64(con), 2) + 1.934*float64(con) - 1.065))
	vrf.phi = newTarget
}

// VerifyVRF verifies a sortition result
func VerifyVRF(pk, hash, pi []byte, phi, stake int) bool {
	res, err := vrf.Verify(pk, pi, []byte("leader"))
	if err != nil {
		log.Panic(err)
	}

	if !res {
		fmt.Println("wrong random number")
		return false
	}

	intValue := new(big.Int).SetBytes(hash)
	hashlen := m.BigPow(2, 256)

	intValue2 := new(big.Float).SetInt(intValue)
	hashlen2 := new(big.Float).SetInt(hashlen)

	r, _ := intValue2.Quo(intValue2, hashlen2).Float64()

	p := float64(phi) / float64(config.TotalStake)
	pp := math.Pow(1-p, float64(stake))

	if r >= pp {
		// fmt.Println("vrf verified!")
		return true
	} else {
		fmt.Println("wrong probability")
		return false
	}
}

// generateRandom outputs vrf random number and proof
func generateRandom() ([]byte, []byte, []byte) {
	pk, sk, err := ed25519.GenerateKey(nil)
	if err != nil {
		log.Panic(err)
	}

	msg := []byte("leader")
	pi, h, err := vrf.Prove(pk, sk, msg[:])
	if err != nil {
		log.Panic(err)
	}

	return pk, pi, h
}
