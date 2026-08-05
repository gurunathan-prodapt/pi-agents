#!/usr/bin/env python3
import sys
import argparse

class AwkExit(Exception):
    def __init__(self, code=0):
        super().__init__()
        self.code = code

class AwkRecord:
    def __init__(self, raw_line="", fs=";", ofs=";"):
        self.fs = fs
        self.ofs = ofs
        self.set_value(raw_line)

    def set_value(self, raw_line):
        self._raw = raw_line
        self._fields = raw_line.split(self.fs)
        self.NF = len(self._fields)

    def get_field(self, idx):
        if idx == 0:
            return self._raw
        if 1 <= idx <= len(self._fields):
            return self._fields[idx - 1]
        return ""

    def set_field(self, idx, val):
        if idx == 0:
            self.set_value(str(val))
        else:
            while len(self._fields) < idx:
                self._fields.append("")
            self._fields[idx - 1] = str(val)
            self._raw = self.ofs.join(self._fields)
            self.NF = len(self._fields)

def main():
    parser = argparse.ArgumentParser(description="Faithful Python 3 conversion of k_vvtn_iar_bgf_gutschrift.awk")
    parser.add_argument('files', nargs='*', help='Input files')
    args = parser.parse_args()

    # AWK BEGIN block variables
    FS = ";"
    OFS = ";"
    RS = "\n"
    ORS = "\n"

    NR = 0
    FNR = 0
    FILENAME = ""

    def run_end_block():
        # END { }
        pass

    def awk_print(*print_args, sep=None, end=None):
        if sep is None:
            sep = OFS
        if end is None:
            end = ORS
        sys.stdout.write(sep.join(map(str, print_args)) + end)

    try:
        inputs = args.files if args.files else ['-']
        
        for file_path in inputs:
            FNR = 0
            if file_path == '-':
                FILENAME = "-"
                infile = sys.stdin
            else:
                FILENAME = file_path
                try:
                    infile = open(file_path, 'r', errors='replace')
                except Exception as e:
                    sys.stderr.write(f"Error opening file {file_path}: {e}\n")
                    sys.exit(1)
            
            try:
                for line in infile:
                    # Strip trailing record separators
                    if line.endswith('\n'):
                        line = line[:-1]
                    if line.endswith('\r'):
                        line = line[:-1]
                    
                    NR += 1
                    FNR += 1
                    
                    record = AwkRecord(line, fs=FS, ofs=OFS)
                    
                    if record.NF == 25:
                        awk_print("D;" + record.get_field(0))
                    else:
                        awk_print("Error: Incorrect nos of Fields ")
                        raise AwkExit(2)
            finally:
                if file_path != '-':
                    infile.close()
                    
    except AwkExit as ae:
        run_end_block()
        sys.exit(ae.code)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        sys.stderr.write(f"Unexpected error: {e}\n")
        sys.exit(1)
        
    run_end_block()
    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())