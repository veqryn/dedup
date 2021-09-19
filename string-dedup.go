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
	"os"
)

// analyze file, get size or line count or something
// split into reasonable sized files or random-access or memory mapped files
// for each file, remove duplicates while sorting in memory, and output to a new file
// for each new file, read them all in simulanteously, removing duplicates and advancing the pointer in each file, and outputting to the final file
func Dedup(inFile, outFile *os.File) error {
	chunks, err := split(inFile)
	if err != nil {
		return err
	}

	dedupedChunks := make([]*os.File, 0, len(chunks))
	for _, chunk := range chunks {
		deduped := removeDuplicates(chunk)
		dedupedChunks = append(dedupedChunks, deduped)
	}

	err = mergeChunks(dedupedChunks, outFile)
	if err != nil {
		return err
	}
	return nil
}

func split(f *os.File) ([]*os.File, error) {
	return nil, nil
}

func removeDuplicates(f *os.File) *os.File {
	return nil
}

func mergeChunks(chunks []*os.File, out *os.File) error {
	return nil
}
