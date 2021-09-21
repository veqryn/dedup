// Package github.com/veqryn/dedup/gentestdata can be run to generate test data
// consisting of a file containing random strings. To run:
// 	go run github.com/veqryn/dedup/gentestdata
// or
// 	go build -o ./gen_test_data github.com/veqryn/dedup/gentestdata
// 	./gen_test_data --file=testdata.log
package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"log"
	"math"
	"math/rand"
	"os"
	"time"
)

func main() {
	// Flags
	fLoc := flag.String("file", "testdata.log", "file location for the test data to be created")
	lineCount := flag.Int("lines", 100, "how many lines to generate")
	strlen := flag.Int("strlen", 50, "length of the strings to generate")
	flag.Parse()

	if fLoc == nil || *fLoc == "" {
		log.Fatal("file flag must non-empty or omitted for the default")
	}
	if lineCount == nil || *lineCount <= 0 {
		log.Fatal("lines flag must be a positive integer or omitted for the default")
	}
	if strlen == nil || *strlen <= 0 {
		log.Fatal("strlen flag must be a positive integer or omitted for the default")
	}

	// Create file
	f, err := os.OpenFile(*fLoc, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Buffer the writes, make sure to flush when done
	w := bufio.NewWriterSize(f, 256*1024)
	defer w.Flush()

	// Create a new random source
	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Array buffer length: two hex characters = one byte
	buff := make([]byte, int(math.Ceil(float64(*strlen)/2.0)))

	// Loop
	for i := 0; i < *lineCount; i++ {
		// Read from random bytes
		_, err = random.Read(buff)
		if err != nil {
			log.Fatal(err)
		}

		// Encode to hex, cut off at strlen
		line := hex.EncodeToString(buff)[:*strlen]
		_, err = w.WriteString(line + "\n")
		if err != nil {
			log.Fatal(err)
		}
	}
}
