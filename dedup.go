// Package dedup (github.com/veqryn/dedup) is a program to remove duplicate strings/URL's.
// 	Assumptions:
// 	* Strings/URL's are all valid and UTF-8
// 	* Duplicate is defined as an exact match
// 	* Input and output will be new-line delimited files
// 	* Line count of the file can be greater than 10 billion (>= 1 terrabyte), too large for memory
// 	* Average string/URL length is around 100 characters
// 	* Unlimited disk space
package dedup

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"sync/atomic"
	"time"
)

const defaultBufferSize int = 256 * 1024 // 256 kb

var delimiter byte = "\n"[0]

//
// Implementation Design:
// When the deduplicated content is larger in bytes than our machine's memory, we will not be able
// to hold the final file in memory. This presents a problem: even if we split the input file and
// deduplicate each chunk, how do we recombine without allowing duplicates if we cannot hold the
// chunks all in memory at the same time.
// The solution chosen for this implementation deduplicates AND sorts the chucks before writing
// them. Then, when the chunks are being merged again, we need only read the first line from each
// chunk, and compare it against the first line from all other chunks. Whichever line would come
// first lexicographically will be written to the output (merged) file. We are guaranteed that
// by doing so, the merge algorithm will see any duplicates between the files in sequence, and we
// deduplicate by skipping all but the first.
// The resulting output (merged) file is then fully deduplicated, and it is also sorted as a
// side effect of choosing this implementation.
// A second side benefit of this implementation is that this program can be run against an input
// file of arbitrary size (>petabytes) and it can run using very little memory (<megabyte),
// though more memory allocated to it will speed up its run time. Setting the memory to be
// larger than the final output file's size, will cut the run time by at least half and remove the
// need to split the input file into chunks or create any temporary files.
//

// Dedup is given a file to write to, a file to read from, and the temporary file size
// for when it needs to spill to disk. It will de-duplicate strings/URL's by reading the input file
// into a set, and writing out the set to a temporary file each time the set approaches tmpFileBytes
// in size. It will then merge the temporary files while deduplicating the lines, into the final file.
func Dedup(outFile *os.File, tmpFileBytes uint64, skipPatterns []*regexp.Regexp, inFile, inFileAgain io.Reader) error {
	// Allow cancellation of progress tracker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get the number of lines in the file, to track progress
	var progress uint64
	if inFileAgain != nil {
		go func() {
			goal, countErr := countLines(inFileAgain)
			if countErr != nil {
				fmt.Println("Error counting lines")
				return
			}
			fmt.Println("Finished counting lines:", goal)
			goal *= 2 // Have to write or ignore every line we've read
			digits := int(math.Floor(math.Log10(float64(goal)) + 1))
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					prog := atomic.LoadUint64(&progress)
					fmt.Printf("Progress: %*d/%d=%d%%\n", digits, prog, goal, prog*100/goal)
				}
			}
		}()
	}

	// Write out chunks
	chunks, err := splitSortDeduplicate(outFile, tmpFileBytes, skipPatterns, &progress, inFile)

	// No matter how or when we exit, cleanup all temporary files
	defer func(chunks []*os.File) {
		for _, chunk := range chunks {
			// Only cleanup files this package created, do not cleanup the outfile (we didn't create it)
			if chunk != outFile {
				chunk.Close()
				os.Remove(chunk.Name())
			}
		}
	}(chunks)

	// Handle error from splitSortDeduplicate
	if err != nil {
		return err
	}

	// No need to merge anything if the input file was empty,
	// or we were able to fit it in memory and wrote everything directly to the output file already
	if len(chunks) <= 1 {
		return nil
	}

	fmt.Println("Merging temporary files into:", outFile.Name())
	return mergeChunks(outFile, &progress, chunks)
}

// countLines returns the number of lines in a file
func countLines(r io.Reader) (uint64, error) {
	buf := make([]byte, defaultBufferSize)

	var count uint64
	var totalCount uint64
	var progress uint64

	lineSep := []byte{'\n'}
	var last byte

	for {
		c, err := r.Read(buf)
		count = uint64(bytes.Count(buf[:c], lineSep))
		totalCount += count
		progress += count
		if progress >= 100000000 {
			progress = 0
			fmt.Println("Counted lines:", totalCount)
		}

		if c > 0 {
			last = buf[c-1]
		}
		if err == io.EOF {
			if last != '\n' {
				totalCount++ // final line
			}
			return totalCount, nil
		}
		if err != nil {
			return totalCount, err
		}
	}
}

// splitSortDeduplicate reads in the input file, and deduplicates the lines as it reads them in.
// If the total size of the deduplicated lines exceeds tmpFileBytes, it will begin writing out
// the sets as sorted chunks to temporary files. If the size doesn't exceed tmpFileBytes,
// it will write the full sorted deduplicated set to the output file.
// It returns all files it wrote to.
func splitSortDeduplicate(outFile *os.File, tmpFileBytes uint64, skipPatterns []*regexp.Regexp, progress *uint64, inFile io.Reader) ([]*os.File, error) {
	// Create a scanner to buffer the input file and read in line tokens
	scanner := bufio.NewScanner(inFile)

	// Set scanner's buffer size to be a bit larger
	scanner.Buffer(make([]byte, 0, defaultBufferSize), bufio.MaxScanTokenSize)

	// Create a hash set (map with empty values) with decent initial size
	set := make(map[string]struct{}, 1024)

	// Create counters and a slice of temporary files being created
	var (
		chunks      []*os.File
		bytesUsed   uint64
		previousLen int
		currentLen  int
		lineCount   uint64
		lineNum     uint64
	)

	// Advance the scanner to the next token
	hasNext := scanner.Scan()
	if !hasNext {
		return nil, scanner.Err()
	}

	// Loop until the file is finished
loop:
	for {
		// Read the token in and add to the set
		line := scanner.Text()
		hasNext = scanner.Scan() // Peak ahead
		lineCount++
		lineNum++

		// Skip lines
		for _, pattern := range skipPatterns {
			if pattern.MatchString(line) {
				lineCount++ // One more line that doesn't have to be written
				continue loop
			}
		}

		// This is what is written, to chunks or to the output file directly
		set[line] = struct{}{}

		// The length of a map is stored in the map (in golang), so the operation is nearly free
		currentLen = len(set)

		// Peek ahead to see if there are more tokens, or exit loop if the file is finished
		if !hasNext {
			if currentLen == previousLen {
				lineCount++ // One more line that doesn't have to be written
			}
			atomic.AddUint64(progress, lineCount)
			break loop
		}
		if lineCount >= 1000 {
			atomic.AddUint64(progress, lineCount)
			lineCount = 0
		}

		// If the length of the set increased, add the byte length of the string to the memory counter,
		// plus one for a new line
		if currentLen > previousLen {
			bytesUsed += uint64(len(line)) + 1

			// If the total bytes of all distinct strings in the set, plus the upcoming line,
			// are equal or greater than what we want, then spill to a new temp file
			if bytesUsed+uint64(len(scanner.Bytes()))+1 > tmpFileBytes {
				// Create a new temporary file
				chunkFile, err := os.CreateTemp("", "dedup.*.log")
				if err != nil {
					return chunks, err
				}
				chunks = append(chunks, chunkFile)
				fmt.Println("Creating temporary file:", chunkFile.Name())

				// Sort and write to file
				err = writeSlice(chunkFile, sortKeys(set), nil)
				if err != nil {
					return chunks, err
				}

				// Overwrite the set so the old one can be GC'ed, reset counters
				set = make(map[string]struct{}, 1024)
				bytesUsed = 0
				currentLen = 0
			}
		} else {
			lineCount++ // One more line that doesn't have to be written
		}
		previousLen = currentLen
	}
	err := scanner.Err()
	if err != nil {
		return chunks, err
	}

	// There is at least one string left in the set.
	// If no temporary files have been created, it means all the deduplicated strings fit into
	// memory, and we can write directly to the output file without having to make temporary chunks
	finalChunk := outFile
	finalProgress := progress
	if len(chunks) > 0 {
		// If we have already made other temporary files, then we have to make another
		finalChunk, err = os.CreateTemp("", "dedup.*.log")
		if err != nil {
			return chunks, err
		}
		finalProgress = nil
		fmt.Println("Creating temporary file:", finalChunk.Name())
	} else {
		fmt.Println("Writing to file:", finalChunk.Name())
	}
	chunks = append(chunks, finalChunk)

	// Write any remaining distinct strings
	return chunks, writeSlice(finalChunk, sortKeys(set), finalProgress)
}

// sortKeys takes a map and puts the keys into a sorted slice
func sortKeys(set map[string]struct{}) []string {
	slice := make([]string, len(set))
	i := 0
	for k := range set {
		slice[i] = k
		i++
	}

	// Sort in place
	sort.Strings(slice)
	return slice
}

// writeSlice writes all strings in the slice to the file, delimited by a new line
func writeSlice(f *os.File, slice []string, progress *uint64) error {
	// Buffer the writes
	writer := bufio.NewWriterSize(f, defaultBufferSize)
	var line string
	var err error

	// Write to temporary file
	var lineCount uint64
	for _, line = range slice {
		// Write line
		_, err = writer.WriteString(line)
		if err != nil {
			return err
		}

		// Write delimiter
		err = writer.WriteByte(delimiter)
		if err != nil {
			return err
		}

		lineCount++
		if lineCount >= 1000 && progress != nil {
			atomic.AddUint64(progress, lineCount)
			lineCount = 0
		}
	}
	if progress != nil {
		atomic.AddUint64(progress, lineCount)
	}

	// Flush all remaining bytes to the file
	return writer.Flush()
}

// mergeChunks merges and deduplicates the chunk files into the output file
func mergeChunks(outFile *os.File, progress *uint64, chunks []*os.File) error {
	// Create a slice of buffered scanners for each chunk
	scanners := make([]*sortableScanner, 0, len(chunks))

	// Add sorted scanners to the slice
	for _, chunk := range chunks {
		ss := &sortableScanner{
			scanner: bufio.NewScanner(chunk), // Use default buffer size since there are many chunks
			f:       chunk,
		}
		scanners = append(scanners, ss)

		// Seek to the beginning of the file to start reading again from the start
		_, err := ss.f.Seek(0, 0)
		if err != nil {
			return err
		}

		// Scan the next token
		ok, err := ss.next()
		if err != nil {
			return err
		}

		// Assert that there is content (every file guaranteed to have at least one line in it)
		if !ok {
			panic(ss.f.Name() + " has no content")
		}
	}

	return mergeSortableScanners(outFile, progress, scanners)
}

// mergeSortableScanners reads a single token from each of the chunks, then chooses which one comes first
// lexicographically, and writes that to a buffer. It then reads new token for that chunk, and
// chooses again, repeating this process until all lines have been read from all chunks.
// To deduplicate, it remembers the previous line written to the output file, and if the next line
// is equal then it is skipped. This works because all the chunk files are sorted already, so it is
// guaranteed that all duplicates will be seen together as it reads from the chunks.
func mergeSortableScanners(outFile *os.File, progress *uint64, scanners []*sortableScanner) error {
	// Create function to sort the scanners by their token, lexicographically by their bytes
	sortScanners := func(i, j int) bool {
		return scanners[i].token < scanners[j].token
	}

	// Create a buffered writer
	writer := bufio.NewWriterSize(outFile, defaultBufferSize)
	var (
		previousLine string
		hasPrevious  bool
		ok           bool
		err          error
		lineCount    uint64
	)

	// Loop until there aren't any scanners left
	for len(scanners) > 0 {
		// Sort the scanners
		if len(scanners) > 1 {
			sort.Slice(scanners, sortScanners)
		}

		// Pull the top token string, and compare to the previous line.
		// If it matches the previous line, it is a duplicate we can skip.
		if !hasPrevious || previousLine != scanners[0].token {
			// Write to the output buffer
			_, err = writer.WriteString(scanners[0].token)
			if err != nil {
				return err
			}

			// Write delimiter
			err = writer.WriteByte(delimiter)
			if err != nil {
				return err
			}
			previousLine = scanners[0].token
			hasPrevious = true
		}

		// Regardless of whether it was written or ignored, advance the progress
		lineCount++
		if lineCount >= 1000 {
			atomic.AddUint64(progress, lineCount)
			lineCount = 0
		}

		// Scan the next value
		ok, err = scanners[0].next()
		if err != nil {
			return err
		}
		if !ok {
			// This scanner doesn't have any more lines, so remove from the slice
			scanners = scanners[1:]
		}
	}
	atomic.AddUint64(progress, lineCount)

	// Flush all remaining bytes to the file
	return writer.Flush()
}

// sortableScanner is a struct containing the latest token string read in from the file,
// as well as the file and scanner objects. It has methods to obtain the next token,
// and the whole struct can easily be sorted in a slice based off the token.
type sortableScanner struct {
	token   string
	scanner *bufio.Scanner
	f       *os.File
}

// next scans the next token string in the file, and sets it to the sortableScanner's token field.
// It returns true if this was successful, false if the end of the file was reached or an error.
func (ss *sortableScanner) next() (bool, error) {
	if ss.scanner.Scan() {
		ss.token = ss.scanner.Text()
		return true, nil
	}

	// Return any error
	return false, ss.scanner.Err()
}
