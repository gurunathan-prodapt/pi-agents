# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output is truncated at the end of `bin/r_abgl_kunde_woech.py` (`query = query.replace("@stichtag", f`), resulting in incomplete code and a syntax error. Additionally, the SQL file `sql/d_abgl_kunde_woech.sql` is missing from the build output.

## Required Changes

1. Complete the generation of `bin/r_abgl_kunde_woech.py` so that it is syntactically valid.
2. Ensure all target files specified in the design, including `sql/d_abgl_kunde_woech.sql`, are fully generated without truncation.