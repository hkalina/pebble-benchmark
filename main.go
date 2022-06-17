package main

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"os"
	"time"
)

func main() {
	db, err := pebble.Open(os.Args[1], &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	printStats(db)
	for i := uint64(0); i < 1_000_000; i++ {
		prefix := intToBytes(i)
		generateData(db, prefix)
		printStats(db)
	}
}

func printStats(db *pebble.DB) {
	fmt.Printf("%s:\n%s\n", time.Now(), db.Metrics().String())
}

func generateData(db *pebble.DB, prefix []byte) {
	for i := uint64(0); i < 1_000_000; i++ {
		key := append(prefix, intToBytes(i)...)
		err := db.Set(key, []byte{0x12, 0x34, 0x56, 0x78, 0x90}, nil)
		if err != nil {
			panic(err)
		}
	}
}

func intToBytes(i uint64) []byte {
	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], i)
	return bytes[:]
}
