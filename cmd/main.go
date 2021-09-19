// Package github.com/veqryn/string-dedup/cmd can be run to deduplicate string data. To run:
// 	go run github.com/veqryn/string-dedup/cmd
// or
// 	go build -o ./dedup github.com/veqryn/string-dedup/cmd
// 	./dedup --in=testdata.log --out=deduped.log
package main

import (
	"flag"
	"log"
	"os"

	dedup "github.com/veqryn/string-dedup"
)

func main() {
	inFileLoc := flag.String("in", "testdata.log", "input file location")
	outFileLoc := flag.String("out", "deduped.log", "output file location")
	flag.Parse()

	if inFileLoc == nil || *inFileLoc == "" {
		log.Fatal("in flag must non-empty or omitted for the default")
	}
	if outFileLoc == nil || *outFileLoc == "" {
		log.Fatal("out flag must non-empty or omitted for the default")
	}

	inFile, err := os.Open(*inFileLoc)
	if err != nil {
		log.Fatal(err)
	}

	outFile, err := os.OpenFile(*outFileLoc, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Starting dedup...")
	err = dedup.Dedup(inFile, outFile)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Success!")
}
