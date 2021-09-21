package dedup

import (
	"bufio"
	"os"
	"testing"
)

func TestDedup(t *testing.T) {
	inFile, err := os.Open("testdata/testdata.log")
	if err != nil {
		t.Fatal(err)
	}
	defer inFile.Close()

	outFile, err := os.CreateTemp("", "dedup.test.*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(outFile.Name())
	defer outFile.Close()

	// testdata.log has 100 distinct lines, 204 total lines. Try to dedup 20 lines at a time
	err = Dedup(outFile, 20*50, inFile)
	if err != nil {
		t.Fatal(err)
	}

	// Seek to the beginning of the file to start reading from the beginning
	_, err = outFile.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(outFile)

	// Read the data back in, confirm expectations
	var i int
	dedupSet := make(map[string]struct{})
	for scanner.Scan() {
		dedupSet[scanner.Text()] = struct{}{}
		i++
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}

	// The length of the hash set should match the length of the file
	if i != 100 || i != len(dedupSet) {
		t.Fatalf("Unique set length (%d) should be positive and match file line length (%d)", len(dedupSet), i)
	}
	t.Logf("Line count matches (%d)", i)
}
