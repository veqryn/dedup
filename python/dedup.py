"""Remove duplicate lines from a file
"""

import argparse
import typing


def main():
    # Flags
    parser = argparse.ArgumentParser(prog="dedup", description="Remove duplicate lines from a file")
    parser.add_argument("--in", dest="in_", default="../testdata/testdata.log", help="input file location")
    parser.add_argument("--out", dest="out_", default="../tmp/deduped.log", help="output file location")
    args = parser.parse_args()

    # Parse flags
    if not args.in_:
        print("in flag must be non-empty or omitted for the default")
        return

    if not args.out_:
        print("out flag must be non-empty or omitted for the default")
        return

    print("Starting dedup...")
    with open(args.out_, "w") as out_file, open(args.in_, "r") as in_file:
        dedup(out_file, in_file)
    print("Success!")


def dedup(out_file: typing.TextIO, in_file: typing.TextIO):
    # Create a set, and write every line from the input file to it
    distinct = set()
    for line in in_file:
        # Remove the trailing new line, both to save space and because
        # we still need to dedup the last line, which may not have it
        distinct.add(line.rstrip("\n"))

    # Write the set to the output file, appending new lines
    for line in distinct:
        out_file.write(line + "\n")
    out_file.flush()


if __name__ == "__main__":
    main()
