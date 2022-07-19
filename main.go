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

var waitGroup sync.WaitGroup

func main() {
	db, err := pebble.Open(os.Args[1], &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	go runPrometheusServer()

	runInserts(db)
}

func runInserts(db *pebble.DB) {
	defer waitGroup.Done()

	for {
		prefix := intToBytes(uint64(time.Now().UnixNano()))

		// insert one batch of records
		insertStart := time.Now()
		batch := db.NewBatch()
		for ii := uint64(0); ii < 100*1024; ii++ {
			key := append(prefix, intToBytes(ii)...)
			value := randomBytes()
			err := batch.Set(key, value, pebble.NoSync)
			if err != nil {
				panic(err)
			}
		}
		err := db.Apply(batch, pebble.NoSync)
		if err != nil {
			panic(err)
		}
		insertCounter.Add(float64(time.Since(insertStart).Nanoseconds()))
		insertedAmountCounter.Add(100 * 1024)
		fmt.Printf("%s:\n%s\n", time.Now().String(), db.Metrics().String()) // print pebble metrics
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
