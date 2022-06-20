package main

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"os"
	"time"
)

func main() {
	useCompact := false
	db, err := pebble.Open(os.Args[1], &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for i := uint64(0); i < 1_000_000; i++ {
		prefix := intToBytes(i)

		// insert 1_000_000 records
		insertStart := time.Now()
		for i := uint64(0); i < 1_000_000; i++ {
			key := append(prefix, intToBytes(i)...)
			err := db.Set(key, []byte{0x12, 0x34, 0x56, 0x78, 0x90}, nil)
			if err != nil {
				panic(err)
			}
		}
		insertDur := time.Since(insertStart)
		metrcs := db.Metrics().String()

		// run compact - if omitted, the performance is bad
		compactStart := time.Now()
		if useCompact {
			err = db.Compact([]byte{}, []byte{0xFF}, true)
			if err != nil {
				panic(err)
			}
		}
		compactDur := time.Since(compactStart)

		fmt.Printf("iteration: %s:\n%d insert: %d compact: %d\n%s\n", time.Now(), i, insertDur.Milliseconds(), compactDur.Milliseconds(), metrcs)
	}
}

func intToBytes(i uint64) []byte {
	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], i)
	return bytes[:]
}
