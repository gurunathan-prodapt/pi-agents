# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_ABTN_SMART_KUBI`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

Build output contains file(s) that do not appear anywhere in the design document's own File Disposition Table — these were invented by Build, not authorized by Design:
  - dwh_abtn_smart_kubi_dag.py

## Required Changes

Remove the listed undeclared file(s) from the build output. If a file is genuinely needed (e.g. an orchestration wrapper), it must first be added as an explicit row in the design document's File Disposition Table — do not infer it from prose elsewhere in the document.