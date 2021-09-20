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
	"sort"
)

const defaultBufferSize int = 256 * 1024 // 256 kb

var delimiter byte = "\n"[0]

// Dedup is given a file to read from, a file to write to, and the average temporary file size
// for when it needs to spill to disk. It will de-duplicate strings/URL's by reading the input file
// into a set, and writing out the set to a temporary file each time the set approaches avgTmpFileBytes
// in size. It will then merge the temporary files while deduplicating the lines, into the final file.
func Dedup(inFile, outFile *os.File, avgTmpFileBytes int64) error {

	chunks, err := splitSortDeduplicate(inFile, outFile, avgTmpFileBytes)
	if err != nil {
		return err
	}

	// If the input file was empty, exit
	if len(chunks) < 1 {
		return nil
	}

	return mergeChunks(chunks, outFile)
}

func splitSortDeduplicate(inFile, outFile *os.File, avgTmpFileBytes int64) ([]*os.File, error) {
	// Create a scanner to buffer the input file and read in line tokens
	scanner := bufio.NewScanner(inFile)

	// Set scanner's buffer size to be a bit larger
	scanner.Buffer(make([]byte, 0, defaultBufferSize), bufio.MaxScanTokenSize)

	// Create a hash set (map with empty values) with decent initial size
	set := make(map[string]struct{}, 1024)

	// Create counters and a slice of temporary files being created
	var (
		chunks      []*os.File
		bytesUsed   int64
		previousLen int
		currentLen  int
	)

	// Read lines from the scanner one by one, putting them into the set
	for scanner.Scan() {
		// Add to the set
		line := scanner.Text()
		set[line] = struct{}{}

		// The length of a map is stored in the map (in golang), so the operation is nearly free
		currentLen = len(set)

		// If the length of the set increased, add the byte length of the string to the memory counter
		if currentLen > previousLen {
			bytesUsed += int64(len(line))

			// If the total bytes of all distinct strings in the set are greater than we want, spill to a new temp file
			if bytesUsed >= avgTmpFileBytes {
				// Create a new temporary file
				chunkFile, err := os.CreateTemp("", "deduped.*.log")
				if err != nil {
					return chunks, err
				}
				chunks = append(chunks, chunkFile)

				// Sort and write to file
				err = writeSlice(chunkFile, sortKeys(set))
				if err != nil {
					return chunks, err
				}

				// Overwrite the set so the old one can be GC'ed, reset counters
				set = make(map[string]struct{}, 1024)
				bytesUsed = 0
				currentLen = 0
			}
		}
		previousLen = currentLen
	}
	err := scanner.Err()
	if err != nil {
		return chunks, err
	}

	// If there are no more strings, return
	if len(set) == 0 {
		return chunks, nil
	}

	// Create a new temporary file
	finalChunk, err := os.CreateTemp("", "deduped.*.log")
	if err != nil {
		return chunks, err
	}
	chunks = append(chunks, finalChunk)

	// Write any remaining distinct strings
	return chunks, writeSlice(finalChunk, sortKeys(set))
}

func sortKeys(set map[string]struct{}) []string {
	// Put into an array/slice so it can be sorted
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

func writeSlice(f *os.File, slice []string) error {
	// Buffer the writes
	writer := bufio.NewWriterSize(f, defaultBufferSize)
	var line string
	var err error

	// Write to temporary file
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
	}

	// Flush all remaining bytes to the file
	return writer.Flush()
}

func mergeChunks(chunks []*os.File, outFile *os.File) error {
	// Create a slice of buffered scanners for each chunk
	scanners := make([]*sortableScanner, 0, len(chunks))
	var ok bool
	var err error

	// No matter how we exit, cleanup all remaining temporary files
	defer func() {
		for _, ss := range scanners {
			ss.f.Close()
			os.Remove(ss.f.Name())
		}
	}()

	// Initialize and add sorted scanners
	for _, chunk := range chunks {
		// Seed to the beginning of the file to start reading from the beginning
		_, err = chunk.Seek(0, 0)
		if err != nil {
			return err
		}

		// Use default buffer size since we may have many chunks
		ss := &sortableScanner{
			scanner: bufio.NewScanner(chunk),
			f:       chunk,
		}

		// Scan the next value
		ok, err = ss.next()
		if err != nil {
			return err
		}

		// If the file has content, add to the slice
		if ok {
			scanners = append(scanners, ss)
		}
	}

	// Create function to sort the scanners
	sortScanners := func(i, j int) bool {
		return scanners[i].token < scanners[j].token
	}

	// Create a buffered writer
	writer := bufio.NewWriterSize(outFile, defaultBufferSize)
	var previousLine string
	var hasPrevious bool

	// Loop until there aren't any scanners left
	for len(scanners) > 0 {
		// Sort the scanners
		if len(scanners) > 1 {
			sort.Slice(scanners, sortScanners)
		}

		// Pull the top token string, and compare to the previous line
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

	// Flush all remaining bytes to the file
	return writer.Flush()
}

type sortableScanner struct {
	token   string
	scanner *bufio.Scanner
	f       *os.File
}

func (ss *sortableScanner) next() (bool, error) {
	if ss.scanner.Scan() {
		ss.token = ss.scanner.Text()
		return true, nil
	}
	// There is an error, or the end of the file has been reached, so delete the temporary file
	ss.f.Close()
	os.Remove(ss.f.Name())

	// Return the error
	return false, ss.scanner.Err()
}
