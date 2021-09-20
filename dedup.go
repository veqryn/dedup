// Package dedup (github.com/veqryn/dedup) is a program to remove duplicate strings/URL's.
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

// Dedup is given a file to read from, and a file to write to. It will then de-duplicatestrings/URL's
// by splitting the input file into smaller temporary files (as defined by maxTmpFileLines),
// deduplicating each file into additional temporary files, then merge the deduplicated files into a
// large one while deduplicating it.
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

		// Cleanup temporary files
		chunk.Close()
		os.Remove(chunk.Name())
	}

	// Merge the deduplicated files into a larger final duplicated file
	err = mergeChunks(dedupedChunks, outFile)
	if err != nil {
		return err
	}
	return nil
}

// split takes one large file, and splits its content into multiple smaller files
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

		_, err = currentChunkWriter.WriteString(scanner.Text() + "\n")
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

// removeDuplicates takes a single file, reads in the lines one by one, deduplicating as it goes,
// the writes the resulting deduplicated lines to a new temporary file
func removeDuplicates(f *os.File) (*os.File, error) {
	// Seed to the beginning of the file to start reading from the beginning
	_, err := f.Seek(0, 0)
	if err != nil {
		return nil, err
	}

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
		_, err = dedupedChunkWriter.WriteString(key + "\n")
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

// mergeChunks takes multiple small files, and merges them into a single large one,
// deduplicating lines as it goes
func mergeChunks(chunks []*os.File, out *os.File) error {
	// Use a hash set to remove duplicates
	dedupSet := make(map[string]struct{})
	for _, dedupChunk := range chunks {
		// Seed to the beginning of the file to start reading from the beginning
		_, err := dedupChunk.Seek(0, 0)
		if err != nil {
			return err
		}

		// Scan lines one by one, and put into hash set
		scanner := bufio.NewScanner(dedupChunk)
		for scanner.Scan() {
			dedupSet[scanner.Text()] = struct{}{}
		}
		if err := scanner.Err(); err != nil {
			return err
		}

		// Cleanup temporary files
		dedupChunk.Close()
		os.Remove(dedupChunk.Name())
	}

	// Write deduplicated lines
	dedupedChunkWriter := bufio.NewWriter(out)
	for key := range dedupSet {
		_, err := dedupedChunkWriter.WriteString(key + "\n")
		if err != nil {
			return err
		}
	}

	// Flush the last writer
	return dedupedChunkWriter.Flush()
}
