# dedup
Deduplicate string data

### How to execute
The main executable is located in the `cmd/` dir, and it has the following flags:
* `--out` output file location
* `--tmp-file-bytes` maximum temporary file bytes (default 250000000)
* `--in` input file location

How to compile and run:
* `cd <repo-directory>`
* `go build -o ./dedup github.com/veqryn/dedup/cmd`
* `./dedup --out=deduped.log --in=testdata/testdata.log`

### Input and Output format
The input should be a single new-line delimited file containing a single string on each line.
The output will be a single new-line delimited file containing sorted deduplicated strings.

### How it works
This package is given a file to write to, a file to read from, and the temporary file size for when it needs to spill to disk. It will de-duplicate strings/URL's by reading the input file line by line into a set (value-less hashmap), and writing out the set to a temporary file each time the set approaches the `--tmp-file-bytes` limit. It will then merge the temporary files while deduplicating the lines, into the final output file.

##### Design considerations
When the deduplicated content is larger in bytes than our machine's memory, we will not be able to hold the final file in memory. This presents a problem: even if we split the input file and deduplicate each chunk, how do we recombine without allowing duplicates if we cannot hold the chunks all in memory at the same time.

The solution chosen for this implementation deduplicates AND sorts the chucks before writing them. Then, when the chunks are being merged again, we need only read the first line from each chunk, and compare it against the first line from all other chunks. Whichever line would come first lexicographically will be written to the output (merged) file. We are guaranteed that by doing so, the merge algorithm will see any duplicates between the files in sequence, and we deduplicate by skipping all but the first.

The resulting output (merged) file is then fully deduplicated, and it is also sorted as a side effect of choosing this implementation.

A second side benefit of this implementation is that this program can be run against an input file of arbitrary size (>petabytes) and it can run using very little memory (<megabyte), though more memory allocated to it will speed up its run time. Setting the memory to be larger than the final output file's size, will cut the run time by at least half and remove the need to split the input file into chunks or create any temporary files.

### Resource requirements
With the default settings, `dedup` uses around 500 MB to 1.5 GB of RAM, and can dedup very large files in about 1 minute per 4 GB.
If the resulting file is less than the `--tmp-file-bytes` flag (default 250MB), than it will take about 20 seconds per 4 GB processed.
In general, depending significantly on the source data, the application uses RAM equal to 2x-6x whatever the `--tmp-file-bytes` flag is set to. This can be used to force the program to use very little RAM (such as just 10 MB), at the cost of taking additional time to complete.

##### Benchmarks
Average of 3 runs:
* 2 GB file with no duplicates finished in 29 seconds, using 1 GB of RAM
* 4 GB file with no duplicates finished in 57 seconds, using 1 GB of RAM
* 200 GB file with 75% duplicates finished in 48 minutes, using 1 GB of RAM

### Testing
Testing is currently being done using the standard Golang testing format (file ending in `_test.go`). Reading in a pre-created data file that contains approximately 50% duplicates, it runs the dedup program against this file then checks that the resulting file has the correct line count and no duplicates.
