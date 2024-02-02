package types

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"github.com/tv42/base58"
	"log"
	"math/big"
	r "math/rand"
	"time"
)

// Transaction represents an Ethereum transaction
type Transaction struct {
	Header *TransactionHeader
	// Signature []byte
	Payload *Payload
	Amount  int64
	ID      []byte
}

type TransactionHeader struct {
	From          []byte
	To            []byte
	Timestamp     uint64
	EndTime1      uint64 // test the time spent for being appended into the DAG
	EndTime2      uint64 // test the time spent for completing execution
	PayloadLength uint32
	// PayloadHash   [32]byte
	Nonce uint32
}

type Payload struct {
	RWSets map[string][]*RWSet
}

type RWSet struct {
	Label string
	Addr  []byte
	Value int64
}

type PayloadInfo struct {
	TxID string
	Data *Payload
}

// Serialize returns a serialized Payload
func (py Payload) Serialize() []byte {
	var encoded bytes.Buffer

	enc := gob.NewEncoder(&encoded)
	err := enc.Encode(py)
	if err != nil {
		log.Panic(err)
	}

	return encoded.Bytes()
}

// Serialize returns a serialized Transaction
func (tx Transaction) Serialize() []byte {
	var encoded bytes.Buffer

	enc := gob.NewEncoder(&encoded)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}

	return encoded.Bytes()
}

// Hash returns the hash of the Transaction
func (tx *Transaction) Hash() []byte {
	var hash [32]byte

	txCopy := *tx
	txCopy.ID = []byte{}

	hash = sha256.Sum256(txCopy.Serialize())

	return hash[:]
}

// Sign transaction
//func (tx *Transaction) Sign(privateKey ecdsa.PrivateKey) {
//
//	dataToSign := fmt.Sprintf("%x\n", *tx)
//
//	r, s, err := ecdsa.Sign(rand.Reader, &privateKey, []byte(dataToSign))
//	if err != nil {
//		log.Panic(err)
//	}
//	signature := append(r.Bytes(), s.Bytes()...)
//
//	tx.Signature = signature
//}

// String returns a human-readable representation of a transaction
func (tx *Transaction) String() string {
	var lines string
	lines = fmt.Sprintf("%x", tx.ID[:4])
	return lines
}

// VerifyTransaction verify transaction
//func (tx *Transaction) VerifyTransaction() bool {
//
//	txHash := tx.Hash()
//	data := tx.Payload.Serialize()
//	payloadHash := sha256.Sum256(data)
//
//	return reflect.DeepEqual(payloadHash, tx.Header.PayloadHash) && SignatureVerify(tx.Header.From, tx.Signature, txHash)
//}

//func (tx *Transaction) GenerateNonce(prefix []byte) uint32 {
//
//	newT := tx
//	newTHash := newT.Hash()
//	for {
//
//		if reflect.DeepEqual(prefix, newTHash[:len(prefix)]) {
//			break
//		}
//
//		newT.Header.Nonce++
//	}
//
//	return newT.Header.Nonce
//}

// CreateRWSets create read/write sets for a common transfer transaction
func (tx *Transaction) CreateRWSets() map[string][]*RWSet {
	var rwSets = make(map[string][]*RWSet)

	addr1 := tx.Header.From
	addr2 := tx.Header.To
	addr1RW1 := &RWSet{Label: "r", Addr: addr1}
	addr1RW2 := &RWSet{Label: "iw", Addr: addr1, Value: int64(10)}
	addr2RW1 := &RWSet{Label: "r", Addr: addr2}
	addr2RW2 := &RWSet{Label: "iw", Addr: addr2, Value: int64(10)}

	rwSets[string(addr1)] = append(rwSets[string(addr1)], addr1RW1, addr1RW2)
	rwSets[string(addr2)] = append(rwSets[string(addr2)], addr2RW1, addr2RW2)

	tx.Payload.RWSets = rwSets

	return rwSets
}

// AsMessage converts a tx to a message structure
func (tx *Transaction) AsMessage() Message {
	return NewMessage(tx.Header.From, tx.Header.To, tx.Nonce(), tx.Value(), tx.Data(), tx.Len(), true)
}

func (tx *Transaction) Data() *Payload   { return tx.Payload }
func (tx *Transaction) Value() int64     { return tx.Amount }
func (tx *Transaction) Nonce() uint32    { return tx.Header.Nonce }
func (tx *Transaction) Len() uint32      { return tx.Header.PayloadLength }
func (tx *Transaction) CheckNonce() bool { return true }
func (tx *Transaction) GetStart() uint64 { return tx.Header.Timestamp }
func (tx *Transaction) GetEnd1() uint64  { return tx.Header.EndTime1 }
func (tx *Transaction) GetEnd2() uint64  { return tx.Header.EndTime2 }

func (tx *Transaction) SetStart() {
	tx.Header.Timestamp = uint64(time.Now().Unix())
}

func (tx *Transaction) SetEnd1() {
	tx.Header.EndTime1 = uint64(time.Now().Unix())
}

func (tx *Transaction) SetEnd2() {
	tx.Header.EndTime2 = uint64(time.Now().Unix())
}

// SignatureVerify verify signatures
func SignatureVerify(publicKey, sig, hash []byte) bool {

	b, _ := base58.DecodeToBig(publicKey)
	pubL := splitBig(b, 2)
	x, y := pubL[0], pubL[1]

	b, _ = base58.DecodeToBig(sig)
	sigL := splitBig(b, 2)
	r, s := sigL[0], sigL[1]

	pub := ecdsa.PublicKey{elliptic.P256(), x, y}

	return ecdsa.Verify(&pub, hash, r, s)
}

func splitBig(b *big.Int, parts int) []*big.Int {

	bs := b.Bytes()
	if len(bs)%2 != 0 {
		bs = append([]byte{0}, bs...)
	}

	l := len(bs) / parts
	as := make([]*big.Int, parts)

	for i, _ := range as {

		as[i] = new(big.Int).SetBytes(bs[i*l : (i+1)*l])
	}

	return as
}

// NewTransaction creates a new transaction
func NewTransaction(amount int64, from, to []byte, payload *Payload) *Transaction {
	tx := Transaction{Header: &TransactionHeader{From: from, To: to}, Payload: payload, Amount: amount}
	r.Seed(time.Now().UnixNano())
	tx.Header.Nonce = r.Uint32()
	tx.Header.Timestamp = uint64(time.Now().Unix())
	payloadData := payload.Serialize()
	// tx.Header.PayloadHash = sha256.Sum256(payloadData)
	tx.Header.PayloadLength = uint32(len(payloadData))
	tx.ID = tx.Hash()

	return &tx
}

// NewCoinbaseTX creates a new coinbase transaction
func NewCoinbaseTX(to string) *Transaction {
	//randData := make([]byte, 20)
	//_, err := rand.Read(randData)
	//if err != nil {
	//	log.Panic(err)
	//}
	data := "coinbase"
	tx := Transaction{Header: &TransactionHeader{To: []byte(to)}, ID: []byte(data)}

	return &tx
}

// DeserializePayload deserializes a payload
func DeserializePayload(data []byte) Payload {
	var payload Payload

	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(&payload)
	if err != nil {
		log.Panic(err)
	}

	return payload
}

// DeserializeTransaction deserializes a transaction
func DeserializeTransaction(data []byte) Transaction {
	var transaction Transaction

	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(&transaction)
	if err != nil {
		log.Panic(err)
	}

	return transaction
}

type Message struct {
	to         []byte
	from       []byte
	nonce      uint32
	amount     int64
	data       *Payload
	length     uint32
	checkNonce bool
}

func NewMessage(from, to []byte, nonce uint32, amount int64, data *Payload, length uint32, checkNonce bool) Message {
	return Message{
		from:       from,
		to:         to,
		nonce:      nonce,
		amount:     amount,
		data:       data,
		length:     length,
		checkNonce: checkNonce,
	}
}

func (m Message) From() []byte     { return m.from }
func (m Message) To() []byte       { return m.to }
func (m Message) Nonce() uint32    { return m.nonce }
func (m Message) Value() int64     { return m.amount }
func (m Message) Data() *Payload   { return m.data }
func (m Message) Len() uint32      { return m.length }
func (m Message) CheckNonce() bool { return m.checkNonce }
