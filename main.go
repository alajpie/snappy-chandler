package main

import (
	"fmt"
	"log"

	"github.com/dgraph-io/badger"
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
			log.Println("version key not found, assuming fresh db and writing our own version number")
			txn.Set([]byte("version"), []byte{0})
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
