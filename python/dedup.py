"""Remove duplicate lines from a file
"""

import argparse
import tempfile
from typing import BinaryIO, List


def main():
    # Flags
    parser = argparse.ArgumentParser(prog="dedup", description="Remove duplicate lines from a file")
    parser.add_argument("--in", dest="in_", default="../testdata/testdata.log", help="input file location")
    parser.add_argument("--out", dest="out_", default="../tmp/deduped.log", help="output file location")
    parser.add_argument("--tmp-file-bytes", dest="tmp_file_bytes_", default=250000000,
                        help="max temporary file byte size. app will use 2-5x more memory than this to run")
    args = parser.parse_args()

    # Parse flags
    if not args.in_:
        print("in flag must be non-empty or omitted for the default")
        return

    if not args.out_:
        print("out flag must be non-empty or omitted for the default")
        return

    if not args.tmp_file_bytes_ or args.tmp_file_bytes_ <= 0:
        print("tmp-file-bytes flag must be a positive integer or omitted for the default")
        return

    print("Starting dedup...")
    with open(args.out_, "wb") as out_file, open(args.in_, "rb") as in_file:
        dedup(out_file, args.tmp_file_bytes_, in_file)
    print("Success!")


def dedup(out_file: BinaryIO, tmp_file_bytes: int, in_file: BinaryIO):
    chunks = __split_sort_dedup(out_file, tmp_file_bytes, in_file)

    if len(chunks) <= 1:
        return

    __merge_chunks(out_file, chunks)


def __split_sort_dedup(out_file: BinaryIO, tmp_file_bytes: int, in_file: BinaryIO) -> List[BinaryIO]:
    # Create a set to eliminate duplicates
    distinct = set()

    # Read the first line
    current_line = in_file.readline()
    if not current_line:
        return []

    chunks: List[BinaryIO] = []
    bytes_used = 0
    previous_len = 0

    # Loop until the line is empty, signalling the file is finished being read
    while True:
        # Remove the trailing new line, both to save space and because
        # we still need to dedup the last line, which may not have a new line
        distinct.add(current_line.rstrip(b"\n"))

        # Read the next line, so we know how many bytes it has
        next_line = in_file.readline()

        # If no more strings, exit loop
        if not next_line:
            break

        # Current length of the distinct set
        current_len = len(distinct)

        # If hte length of the set has increased, add the latest line's byte length to the counter
        if current_len > previous_len:
            bytes_used += len(current_line)

            # If the total bytes are all distinct strings so far, plus the upcoming line,
            # greater than what we want, then spill to a new temporary file
            if bytes_used + len(next_line) > tmp_file_bytes:
                # Create temporary file and add to the file list
                chunk_file = tempfile.NamedTemporaryFile(mode="w+b", prefix="dedup.", suffix=".log")
                chunks.append(chunk_file)

                # Sort the distinct set and write to the file, with new lines
                for line in sorted(distinct):
                    chunk_file.write(line + b"\n")

                # Reset the set and counters
                distinct = set()
                bytes_used = 0
                current_len = 0

        # Set the current line and previous distinct set size
        previous_len = current_len
        current_line = next_line

    # There is at least one string left in the distinct set.
    # If no temporary files have been created yet, it means all the deduplicated strings fit into
    # memory, and we can write directly to the output file without having to make any temporary chunks.
    final_chunk = out_file
    if len(chunks) > 0:
        # If we have already made other temporary files, then we have to make another
        final_chunk = tempfile.NamedTemporaryFile(mode="w+b", prefix="dedup.", suffix=".log")
    chunks.append(final_chunk)

    # Write any remaining strings to the final chunk
    for line in sorted(distinct):
        final_chunk.write(line + b"\n")

    return chunks


def __merge_chunks(out_file: BinaryIO, chunks: List[BinaryIO]):
    # Make all chunks sortable and put into a list
    sorted_chunks: List[__SortableIO] = []
    for chunk in chunks:
        sortable = __SortableIO(chunk)
        sorted_chunks.append(sortable)

        # Seek to start of file
        chunk.seek(0, 0)

        # Load next token
        sortable.next()

        # Assert each chunk has at least one line in it
        assert len(sortable.token) > 0

    # Loop while we have chunks left
    previous_line = b""
    while len(sorted_chunks) > 0:
        # Sort the chunks
        sorted_chunks.sort()

        # If the new top token matches the previously written token, it is a duplicate we can skip
        if not previous_line or previous_line != sorted_chunks[0].token:
            out_file.write(sorted_chunks[0].token)
            previous_line = sorted_chunks[0].token

        # Read in the next token
        sorted_chunks[0].next()

        # If the new token is empty, remove this chunk from the list and close the file
        if not sorted_chunks[0].token:
            sorted_chunks[0].f.close()
            sorted_chunks.pop(0)

    # Flush all written lines
    out_file.flush()


class __SortableIO:
    def __init__(self, f: BinaryIO):
        self.f = f
        self.token = b""

    def __lt__(self, other):
        return self.token < other.token

    def next(self):
        self.token = self.f.readline()


if __name__ == "__main__":
    main()
