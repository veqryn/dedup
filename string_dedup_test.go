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

	outFile, err := os.OpenFile("testdata/deduped.log", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal()
	}
	defer os.Remove("testdata/deduped.log")
	defer outFile.Close()

	// testdata.log has 204 lines. Try to dedup 20 lines at a time
	err = Dedup(inFile, outFile, 20)
	if err != nil {
		t.Fatal()
	}

	// Read the data back in, confirm expectations
	var i int
	dedupSet := make(map[string]struct{})
	scanner := bufio.NewScanner(outFile)
	for scanner.Scan() {
		dedupSet[scanner.Text()] = struct{}{}
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}

	// The length of the hash set should match the length of the file
	if i != len(dedupSet) {
		t.Errorf("Unique set length (%d) should match file line length (%d)", len(dedupSet), i)
	}
}
