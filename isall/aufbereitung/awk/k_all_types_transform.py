#!/usr/bin/env python3
import sys
import argparse

class AwkContext:
    def __init__(self):
        self.FS = ";"
        self.OFS = ";"
        self.RS = "\n"
        self.ORS = "\n"
        self.FILENAME = ""
        self.NR = 0
        self.FNR = 0
        self._fields = []
        self._raw_record = ""

    @property
    def NF(self):
        return len(self._fields)

    @NF.setter
    def NF(self, value):
        val = int(value)
        if val < 0:
            val = 0
        if val < len(self._fields):
            self._fields = self._fields[:val]
        elif val > len(self._fields):
            self._fields.extend([""] * (val - len(self._fields)))
        self._rebuild_record()

    @property
    def r_0(self):
        return self._raw_record

    @r_0.setter
    def r_0(self, value):
        self._raw_record = str(value)
        self._recompute_fields()

    def get_field(self, idx):
        if idx == 0:
            return self._raw_record
        if 1 <= idx <= len(self._fields):
            return self._fields[idx - 1]
        return ""

    def set_field(self, idx, value):
        if idx == 0:
            self.r_0 = value
            return
        idx = int(idx)
        if idx < 0:
            raise ValueError("Field index cannot be negative")
        
        if idx > len(self._fields):
            self._fields.extend([""] * (idx - len(self._fields)))
        self._fields[idx - 1] = str(value)
        self._rebuild_record()

    def _recompute_fields(self):
        if self.FS == " ":
            s = self._raw_record.strip()
            if not s:
                self._fields = []
            else:
                self._fields = s.split()
        elif len(self.FS) > 1:
            import re
            self._fields = re.split(self.FS, self._raw_record)
        else:
            self._fields = self._raw_record.split(self.FS)

    def _rebuild_record(self):
        self._raw_record = self.OFS.join(self._fields)

    def print_fields(self, *args):
        if not args:
            sys.stdout.write(self._raw_record + self.ORS)
        else:
            out = self.OFS.join(str(arg) for arg in args)
            sys.stdout.write(out + self.ORS)

def main():
    parser = argparse.ArgumentParser(description="Post-processing of the ALL_TYPES export file")
    parser.add_argument('files', nargs='*', help="Input files")
    args = parser.parse_args()

    ctx = AwkContext()
    
    # BEGIN block
    ctx.FS = ";"
    ctx.OFS = ";"
    
    # Helper to process record
    def process_record():
        if ctx.NF == 12:
            ctx.print_fields("D;" + ctx.r_0)
        else:
            # REVIEW: Standard Output for Error: The original AWK script prints its error message 
            # "Error: Incorrect nos of Fields " to stdout instead of stderr. 
            # The Python implementation mimics this exactly to preserve behavioral compatibility.
            ctx.print_fields("Error: Incorrect nos of Fields ")
            sys.exit(2)

    files = args.files if args.files else ["-"]
    
    for filename in files:
        ctx.FILENAME = filename
        ctx.FNR = 0
        if filename == "-":
            stream = sys.stdin
        else:
            try:
                # REVIEW: UTF-8 encoding not explicitly requested by source/design; 
                # using system default to preserve uncertainty.
                stream = open(filename, "r")
            except Exception as e:
                sys.stderr.write(f"Error opening {filename}: {e}\n")
                sys.exit(1)
        
        try:
            for line in stream:
                # Strip RS (trailing newline)
                # REVIEW: Trailing Semicolons: Note that standard AWK split behavior on a line ending 
                # in a semicolon will count the empty string after the trailing semicolon as a field. 
                # Python's .split(';') behaves identically.
                if line.endswith("\r\n"):
                    record = line[:-2]
                elif line.endswith("\n"):
                    record = line[:-1]
                else:
                    record = line
                
                ctx.NR += 1
                ctx.FNR += 1
                ctx.r_0 = record
                
                process_record()
        finally:
            if filename != "-":
                stream.close()

    # END block (empty)
    return 0

if __name__ == "__main__":
    sys.exit(main())