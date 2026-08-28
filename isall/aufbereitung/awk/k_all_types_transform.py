#!/usr/bin/env python3
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="Post-process ALL_TYPES export file.")
    parser.add_argument('files', nargs='*', help="Input files")
    args = parser.parse_args()

    # Emulating AWK BEGIN block variables
    FS = ";"
    ORS = "\n"

    # Default to stdin if no files are provided
    files = args.files if args.files else ['-']

    for filename in files:
        if filename == '-':
            file_obj = sys.stdin
        else:
            try:
                # Using surrogateescape to gracefully handle arbitrary binary/non-UTF-8 characters if any
                file_obj = open(filename, 'r', encoding='utf-8', errors='surrogateescape')
            except Exception as e:
                sys.stderr.write(f"Error opening file {filename}: {e}\n")
                return 1

        try:
            for line in file_obj:
                # Replicate RS (Record Separator) default splitting (handles \r\n and \n)
                if line.endswith('\r\n'):
                    line_content = line[:-2]
                elif line.endswith('\n'):
                    line_content = line[:-1]
                else:
                    line_content = line

                # Replicate AWK splitting behavior for FS = ";"
                if len(line_content) == 0:
                    fields = []
                else:
                    fields = line_content.split(FS)

                NF = len(fields)

                # Validation & transformation logic
                if NF == 12:
                    sys.stdout.write(f"D;{line_content}{ORS}")
                else:
                    sys.stdout.write(f"Error: Incorrect nos of Fields {ORS}")
                    return 2
        finally:
            if file_obj is not sys.stdin:
                file_obj.close()

    return 0

if __name__ == '__main__':
    sys.exit(main())