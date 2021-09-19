// Package dedup (github.com/veqryn/string-dedup) is a program to remove duplicate strings/URL's.
// 	Assumptions:
// 	* Strings/URL's are all valid and UTF-8
// 	* Duplicate is defined as an exact match
// 	* Input and output will be new-line delimited files
// 	* Average line count of the file will be greater than 10 billion (>= 1 terrabyte) and too large for memory
// 	* Average string/URL length is around 100 characters
// 	* Unlimited disk space
package dedup

import (
	"bufio"
	"os"
)

// TODO: remove
// analyze file, get size or line count or something
// split into reasonable sized files or random-access or memory mapped files
// for each file, remove duplicates while sorting in memory, and output to a new file
// for each new file, read them all in simulanteously, removing duplicates and advancing the pointer in each file, and outputting to the final file

// Dedup ...
func Dedup(inFile, outFile *os.File, maxTmpFileLines uint) error {
	// Split file into smaller ones
	chunks, err := split(inFile, maxTmpFileLines)
	if err != nil {
		return err
	}

	// Deduplicate the smaller files
	dedupedChunks := make([]*os.File, 0, len(chunks))
	for _, chunk := range chunks {
		deduped, err := removeDuplicates(chunk)
		if err != nil {
			return err
		}

		dedupedChunks = append(dedupedChunks, deduped)
	}

	// Merge the deduplicated files into a larger final duplicated file
	err = mergeChunks(dedupedChunks, outFile)
	if err != nil {
		return err
	}
	return nil
}

// TODO: close temp files and delete them
// defer os.Remove(f.Name())

func split(f *os.File, maxTmpFileLines uint) ([]*os.File, error) {
	var smallChunks []*os.File
	var currentChunk *os.File
	var err error

	// Line scanner, with max line size defined by bufio.MaxScanTokenSize (64k)
	scanner := bufio.NewScanner(f)

	var i uint
	for scanner.Scan() {
		if currentChunk == nil || i >= maxTmpFileLines {
			currentChunk, err = os.CreateTemp("", "dups.small.*.log")
			if err != nil {
				return smallChunks, err
			}
			smallChunks = append(smallChunks, currentChunk)
			i = 0
		}

		_, err = currentChunk.WriteString(scanner.Text() + "\n")
		if err != nil {
			return smallChunks, err
		}
		i++
	}
	return smallChunks, scanner.Err()
}

func removeDuplicates(f *os.File) (*os.File, error) {
	return nil, nil
}

func mergeChunks(chunks []*os.File, out *os.File) error {
	return nil
}
