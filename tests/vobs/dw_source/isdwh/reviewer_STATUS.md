# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_ABPZ_KKM_AIL_AGENT`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The generated PySpark script contains a Python syntax error. The `SparkSession.builder` chain is split across multiple lines without line continuation characters (`\`) or enclosing parentheses, which will cause a parse error.

## Required Changes

1. Add line continuation backslashes (`\`) or enclose the `SparkSession.builder` chain in parentheses in `agent_ads_lookup.py`.