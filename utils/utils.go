package utils

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/libp2p/go-libp2p/core/crypto"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"strconv"
)

// ConvertBinToDec converts a block hash to an integer type
func ConvertBinToDec(blockHash []byte, bitNumber int) int {
	var sum, tmp int

	hashString := fmt.Sprintf("%b", blockHash)
	data := hashString[len(hashString)-bitNumber-1 : len(hashString)-1]
	length := bitNumber

	// convert from binary to decimal
	for _, s := range data {
		n, _ := strconv.Atoi(string(s))
		//if err != nil {
		//	log.Println(err)
		//}
		if n == 1 {
			tmp = int(math.Pow(2, float64(length-1)))
		} else {
			tmp = 0
		}
		sum += tmp
		length--
	}

	return sum
}

// IntToHex converts an int64 to a byte array
func IntToHex(num int64) []byte {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.BigEndian, num)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// ReverseBytes reverses a byte array
func ReverseBytes(data []byte) {
	for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
		data[i], data[j] = data[j], data[i]
	}
}

// FileExists determine if a file exists
func FileExists(fileName string) bool {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return false
	}
	return true
}

// ReadStrings read strings from a specific file
func ReadStrings(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var rds []string

	for scanner.Scan() {
		rds = append(rds, scanner.Text())
	}

	return rds, nil
}

func GetPrivateKey(path string) (crypto.PrivKey, error) {
	if FileExists(path) {
		// read the private key from the file
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}

		sk, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, err
		}

		return sk, nil
	} else {
		r := rand.Reader
		sk, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
		if err != nil {
			return nil, err
		}

		// write the private key to the file
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		data, err := crypto.MarshalPrivateKey(sk)
		if err != nil {
			return nil, err
		}

		_, err = f.Write(data)
		if err != nil {
			return nil, err
		}

		return sk, nil
	}
}

func DoExist(slice []string, n string) bool {
	for _, e := range slice {
		if reflect.DeepEqual(n, e) {
			return true
		}
	}
	return false
}
