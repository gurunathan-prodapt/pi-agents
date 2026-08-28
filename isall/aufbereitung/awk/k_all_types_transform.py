#!/usr/bin/env python3
import sys
import argparse

class AwkRecord:
    def __init__(self, line="", fs=";", ofs=";"):
        self._fs = fs
        self._ofs = ofs
        self.set_value(line)

    def set_value(self, line):
        self._raw = line
        if not line:
            self._fields = []
        else:
            self._fields = line.split(self._fs)

    @property
    def value(self):
        return self._raw

    @value.setter
    def value(self, val):
        self.set_value(val)

    @property
    def NF(self):
        return len(self._fields)

    def get_field(self, idx):
        if idx == 0:
            return self._raw
        if 1 <= idx <= len(self._fields):
            return self._fields[idx - 1]
        return ""

    def set_field(self, idx, val):
        if idx == 0:
            self.set_value(val)
            return
        while len(self._fields) < idx:
            self._fields.append("")
        self._fields[idx - 1] = str(val)
        self._raw = self._ofs.join(self._fields)


def main():
    parser = argparse.ArgumentParser(description="AWK k_all_types_transform conversion")
    parser.add_argument('files', nargs='*', help='Input files')
    args = parser.parse_args()

    # BEGIN block
    FS = ";"
    OFS = ";"
    ORS = "\n"

    NR = 0
    FNR = 0
    FILENAME = ""

    exit_code = 0
    exit_called = False

    def awk_exit(code=0):
        nonlocal exit_code, exit_called
        exit_code = code
        exit_called = True
        raise SystemExit()

    def process_stream(stream, name):
        nonlocal NR, FNR, FILENAME
        FILENAME = name
        FNR = 0
        for line in stream:
            if line.endswith('\r\n'):
                line = line[:-2]
            elif line.endswith('\n'):
                line = line[:-1]

            NR += 1
            FNR += 1

            record = AwkRecord(line, fs=FS, ofs=OFS)

            if record.NF == 12:
                sys.stdout.write(f"D;{record.value}{ORS}")
            else:
                # REVIEW: In AWK, print "Error: Incorrect nos of Fields " outputs to stdout. In typical UNIX applications, error messages are written to stderr. The conversion preserves writing to stdout to maintain exact stream compatibility, but this should be confirmed with the system architect.
                sys.stdout.write(f"Error: Incorrect nos of Fields {ORS}")
                awk_exit(2)

    files = args.files if args.files else ['-']

    try:
        for file in files:
            if exit_called:
                break
            if file == '-':
                process_stream(sys.stdin, "-")
            else:
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        process_stream(f, file)
                except FileNotFoundError:
                    sys.stderr.write(f"Error: File not found {file}\n")
                    sys.exit(1)
                except Exception as e:
                    sys.stderr.write(f"Error reading {file}: {e}\n")
                    sys.exit(1)
    except SystemExit:
        pass

    # END block
    # (Empty in AWK script)

    return exit_code

if __name__ == "__main__":
    sys.exit(main())