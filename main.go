package main

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

var insertedAmountCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "pebble_benchmark_inserted_amount",
	Help: "Amount of inserted items",
})
var insertCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "pebble_benchmark_insert",
	Help: "Time consumed by inserting",
})
var compactCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "pebble_benchmark_compact",
	Help: "Time consumed by compact",
})

var waitGroup sync.WaitGroup

func main() {
	useCompact := os.Args[2] == "compact"

	db, err := pebble.Open(os.Args[1], &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	waitGroup.Add(1)
	go runPrometheusServer()

	for i := 0; i < 16; i++ {
		waitGroup.Add(1)
		go runInserts(db, useCompact && i == 0) // compact only in one worker thread
	}
	waitGroup.Wait()
}

func runInserts(db *pebble.DB, useCompact bool) {
	defer waitGroup.Done()

	for i := uint64(0); true; i++ {
		prefix := intToBytes(i)

		// insert 1_000_000 records
		for ii := uint64(0); ii < 100_000; ii++ {
			key := append(prefix, intToBytes(ii)...)
			value := randomBytes()
			insertStart := time.Now()
			err := db.Set(key, value, nil)
			if err != nil {
				panic(err)
			}
			insertCounter.Add(float64(time.Since(insertStart).Nanoseconds()))
			insertedAmountCounter.Inc()
		}
		fmt.Printf("%s:\n%s\n", time.Now().String(), db.Metrics().String()) // print pebble metrics

		// run compact - if omitted, the performance is bad
		if useCompact {
			compactStart := time.Now()
			err := db.Compact([]byte{}, []byte{0xFF}, true)
			if err != nil {
				panic(err)
			}
			compactCounter.Add(float64(time.Since(compactStart).Nanoseconds()))
		}
	}
}

func intToBytes(i uint64) []byte {
	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], i)
	return bytes[:]
}

func randomBytes() []byte {
	bytes := make([]byte, 40)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return bytes
}

func runPrometheusServer() {
	defer waitGroup.Done()
	srv := &http.Server{
		Addr:              ":4321",
		ReadTimeout:       time.Second * 2,
		WriteTimeout:      time.Second * 15,
		IdleTimeout:       time.Second * 2,
		ReadHeaderTimeout: time.Second * 2,
		Handler:           promhttp.Handler(),
	}
	if err := srv.ListenAndServe(); err != nil {
		fmt.Printf("prometheus failed: %s", err)
	}
}
