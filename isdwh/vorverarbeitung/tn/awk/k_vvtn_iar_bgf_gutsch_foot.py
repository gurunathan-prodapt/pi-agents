#!/usr/bin/env python3
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="Faithful Python 3 conversion of k_vvtn_iar_bgf_gutsch_foot.awk")
    parser.add_argument('-v', action='append', default=[], help="AWK-style variable assignment (e.g., -v FLNM=filename)")
    parser.add_argument('--flnm', type=str, default=None, help="Explicit filename parameter")
    parser.add_argument('files', nargs='*', help="Input files (reads from stdin if none provided)")
    
    args = parser.parse_args()
    
    # Resolve FLNM variable from either -v or --flnm
    flnm = ""
    for val in args.v:
        if '=' in val:
            k, v = val.split('=', 1)
            if k.strip() == 'FLNM':
                flnm = v
    if args.flnm is not None:
        flnm = args.flnm
        
    # BEGIN block
    FS = ";"
    OFS = ";"
    ORS = "\n"
    
    NR = 0
    
    def get_field(fields, idx):
        if idx == 0:
            return ";".join(fields)
        if 0 < idx <= len(fields):
            return fields[idx-1]
        return ""

    files = args.files if args.files else ['-']
    
    for filepath in files:
        FNR = 0
        if filepath == '-':
            stream = sys.stdin
        else:
            try:
                stream = open(filepath, 'r', encoding='utf-8', errors='replace')
            except Exception as e:
                sys.stderr.write(f"Error opening file {filepath}: {e}\n")
                return 1
        
        try:
            for line in stream:
                # Strip trailing newline/carriage return
                if line.endswith('\n'):
                    line = line[:-1]
                if line.endswith('\r'):
                    line = line[:-1]
                    
                NR += 1
                FNR += 1
                
                fields = line.split(FS)
                
                f1 = get_field(fields, 1)
                f2 = get_field(fields, 2)
                
                output_line = f"X;Datei {flnm};{f1};{f2};File for BGF IAR Gutschrift;{f1}"
                sys.stdout.write(output_line + ORS)
        except Exception as e:
            sys.stderr.write(f"Error reading file {filepath}: {e}\n")
            return 1
        finally:
            if filepath != '-':
                stream.close()
                
    # END block
    return 0

if __name__ == "__main__":
    sys.exit(main())