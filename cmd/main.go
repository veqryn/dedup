// Package github.com/veqryn/dedup/cmd can be run to deduplicate string data. To run:
// 	go run github.com/veqryn/dedup/cmd
// or
// 	go build -o ./dedup github.com/veqryn/dedup/cmd
// 	./dedup --in=testdata/testdata.log --out=deduped.log
package main

import (
	"flag"
	"log"
	"os"

	"github.com/veqryn/dedup"
)

func main() {
	// Flags
	inFileLoc := flag.String("in", "", "input file location")
	outFileLoc := flag.String("out", "", "output file location")
	avgTmpFileBytes := flag.Int64("tmp-file-bytes", 250000000,
		"max temporary file byte size. app will use 2-5x more memory than this to run")
	flag.Parse()

	if inFileLoc == nil || *inFileLoc == "" {
		log.Fatal("in flag must be non-empty or omitted for the default")
	}
	if outFileLoc == nil || *outFileLoc == "" {
		log.Fatal("out flag must be non-empty or omitted for the default")
	}
	if avgTmpFileBytes == nil || *avgTmpFileBytes <= 0 {
		log.Fatal("avgTmpFileBytes flag must be a positive integer or omitted for the default")
	}

	// Open input file for reading
	inFile, err := os.Open(*inFileLoc)
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	// Create output file for writing
	outFile, err := os.OpenFile(*outFileLoc, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	// Dedup
	log.Println("Starting dedup...")
	err = dedup.Dedup(outFile, *avgTmpFileBytes, inFile)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Success!")
}
