package main

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/dgraph-io/badger"
	"github.com/lukechampine/blake3"
	"github.com/restic/chunker"
)

func main() {
	version := []byte{0}

	db, err := badger.Open(badger.DefaultOptions(".snappy-chandler"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	err = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("version"))
		if err != nil {
			log.Println("version key not found, assuming fresh db and writing initial data")
			txn.Set([]byte("version"), []byte{0})
			poly, err := chunker.RandomPolynomial()
			if err != nil {
				return err
			}
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(poly))
			txn.Set([]byte("polynomial"), b)
			return nil
		}
		return item.Value(func(val []byte) error {
			if err != nil {
				return fmt.Errorf("version check failed: %w", err)
			}
			if val[0] != version[0] { // yep, I hope we never get past 256 versions
				return fmt.Errorf("version check failed: %v (db) != %v (tool)", val, version)
			}
			return nil
		})
	})
	if err != nil {
		log.Fatal(err)
	}
}
