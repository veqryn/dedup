// Package github.com/veqryn/dedup/cmd can be run to deduplicate string data. To run:
// 	go run github.com/veqryn/dedup/cmd
// or
// 	go build -o ./dedup github.com/veqryn/dedup/cmd
// 	./dedup --in=testdata/testdata.log --out=deduped.log
package main

import (
	"flag"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/veqryn/dedup"
)

// arrayFlags lets you set a flag multiple times
type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, string(os.PathListSeparator))
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	// Flags
	var inFileLoc arrayFlags
	var skipPatterns arrayFlags
	flag.Var(&inFileLoc, "in", "input file location (flag can be used multiple times)")
	flag.Var(&skipPatterns, "skip-pattern", "re2 regex pattern that will skip the line if it matches (flag can be used multiple times)")
	outFileLoc := flag.String("out", "", "output file location")
	tmpFileBytes := flag.Uint64("tmp-file-bytes", 250000000,
		"max temporary file byte size. app will use 2-5x more memory than this to run")
	appendFlag := flag.Bool("append", false, "should append to file (default: only allow new files)")
	flag.Parse()

	if inFileLoc == nil || len(inFileLoc) == 0 {
		log.Fatal("in flag must be non-empty or omitted for the default")
	}
	if outFileLoc == nil || *outFileLoc == "" {
		log.Fatal("out flag must be non-empty or omitted for the default")
	}
	if tmpFileBytes == nil || *tmpFileBytes <= 0 {
		log.Fatal("tmp-file-bytes flag must be a positive integer or omitted for the default")
	}

	// Compile regexp's
	var skipPatternsCompiled []*regexp.Regexp
	for _, pattern := range skipPatterns {
		re2, err := regexp.Compile(pattern)
		if err != nil {
			log.Fatal(err)
		}
		skipPatternsCompiled = append(skipPatternsCompiled, re2)
	}

	// Open input file for reading
	var inFiles []io.Reader
	var progressFiles []io.Reader
	for _, fileLoc := range inFileLoc {
		inFile, err := os.Open(fileLoc)
		if err != nil {
			log.Fatal(err)
		}
		defer inFile.Close()
		inFiles = append(inFiles, inFile)

		// Open input file again to track progress
		progressFile, err := os.Open(fileLoc)
		if err != nil {
			log.Fatal(err)
		}
		defer progressFile.Close()
		progressFiles = append(progressFiles, progressFile)
	}
	inReader := io.MultiReader(inFiles...)
	progressReader := io.MultiReader(progressFiles...)

	// Create output file for writing
	var fileOpts int
	if appendFlag != nil && *appendFlag {
		fileOpts = os.O_CREATE | os.O_APPEND | os.O_WRONLY
	} else {
		fileOpts = os.O_CREATE | os.O_EXCL | os.O_WRONLY
	}
	outFile, err := os.OpenFile(*outFileLoc, fileOpts, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	// Dedup
	log.Println("Starting dedup...")
	err = dedup.Dedup(outFile, *tmpFileBytes, skipPatternsCompiled, inReader, progressReader)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Success!")
}
