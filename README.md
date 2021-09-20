# string-dedup
Deduplicate string data

### How to execute
The main executable is located in the `cmd/` dir, and it has the following flags:
* `--in` input file location (default "testdata.log")
* `--max-tmp-file-lines` maximum temporary file line count (default 1000000)
* `--out` output file location (default "deduped.log")

How to compile and run:
* `cd <repo-directory>`
* `go build -o ./dedup github.com/veqryn/string-dedup/cmd`
* `./dedup --in=testdata/testdata.log --out=deduped.log`

### Input and Output format
The input should be a single new-line delimited file containing a single string on each line.
The output will be a single new-line delimited file containing deduplicated strings.

### How it works
This package will split the input file into multiple smaller temporary files (as defined by `--max-tmp-file-lines`). For each of these files, it will read in all lines into a hash set to deduplicate, then write the resulting set to an additional temporary file. Finally, it will merge all deduplicated files into a hash set and write it out to the final file.

### Resource requirements
Memory and time requirements are proportional to the size of the final file without duplicates.
Needs approximately 30 seconds and 2-3 GB per 1 GB output file.

##### Benchmarks
Average of 3 runs:
* 2 GB file with no duplicates finished in 61 seconds, using 5.5 GB of RAM
* 4 GB file with no duplicates finished in 118 seconds, using 10 GB of RAM

### Testing
Testing is currently being done using the standard Golang testing format (file ending in `_test.go`). Reading in a pre-created data file that contains approximately 50% duplicates, it runs the dedup program against this file then checks that the resulting file has the correct line count and no duplicates.
