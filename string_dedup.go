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
	var currentChunkWriter *bufio.Writer
	var err error

	// Line scanner, with max line size defined by bufio.MaxScanTokenSize (64k)
	scanner := bufio.NewScanner(f)

	var i uint // Lines written
	for scanner.Scan() {
		if currentChunkWriter == nil || i >= maxTmpFileLines {
			// Flush the current writer if there is one
			if currentChunkWriter != nil {
				err = currentChunkWriter.Flush()
				if err != nil {
					return smallChunks, err
				}
			}

			// Create a new temporary file
			currentChunkFile, err := os.CreateTemp("", "dups.small.*.log")
			if err != nil {
				return smallChunks, err
			}

			// Buffer the writes
			currentChunkWriter = bufio.NewWriter(currentChunkFile)
			smallChunks = append(smallChunks, currentChunkFile)
			i = 0
		}

		_, err = currentChunkWriter.Write(scanner.Bytes())
		if err != nil {
			return smallChunks, err
		}
		i++
	}

	// Flush the last writer
	if currentChunkWriter != nil {
		err = currentChunkWriter.Flush()
		if err != nil {
			return smallChunks, err
		}
	}
	return smallChunks, scanner.Err()
}

func removeDuplicates(f *os.File) (*os.File, error) {
	scanner := bufio.NewScanner(f)

	// Use a hash set to remove duplicates
	dedupSet := make(map[string]struct{})
	for scanner.Scan() {
		dedupSet[scanner.Text()] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Create a new temporary file
	dedupedChunkFile, err := os.CreateTemp("", "dedup.small.*.log")
	if err != nil {
		return nil, err
	}

	// Write deduplicated lines
	dedupedChunkWriter := bufio.NewWriter(dedupedChunkFile)
	for key := range dedupSet {
		_, err = dedupedChunkWriter.WriteString(key)
		if err != nil {
			return dedupedChunkFile, err
		}
	}

	// Flush the last writer
	err = dedupedChunkWriter.Flush()
	if err != nil {
		return dedupedChunkFile, err
	}

	return dedupedChunkFile, nil
}

func mergeChunks(chunks []*os.File, out *os.File) error {
	// Use a hash set to remove duplicates
	dedupSet := make(map[string]struct{})
	for _, dedupChunk := range chunks {
		scanner := bufio.NewScanner(dedupChunk)
		for scanner.Scan() {
			dedupSet[scanner.Text()] = struct{}{}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}

	// Write deduplicated lines
	dedupedChunkWriter := bufio.NewWriter(out)
	for key := range dedupSet {
		_, err := dedupedChunkWriter.WriteString(key)
		if err != nil {
			return err
		}
	}

	// Flush the last writer
	return dedupedChunkWriter.Flush()
}
