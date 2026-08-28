# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_PFPL_CL_TARIF_SMART`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output is mostly correct, but the Python wrapper script `r_sqlscript.py` completely drops the large literal `usage()` text block present in the source script. Per Check 5, all literal print/echo strings must be preserved unless explicitly retired.

## Required Changes

(see explanation above)
## Per-File Review Results

- ✅ `DW.DWH_PFPL_CL_TARIF_SMART.xml`
- ✅ `local/home/gurunathan_t/single_job_demo_v2/d_pfpl_classic_tarif_smart.sql`
- ❌ `local/home/gurunathan_t/single_job_demo_v2/r_sqlscript`
  - 1. Restore the exact `usage()` text block from the source script (the block containing `Programm: ...`, `Version: ...`, `Aufruf: ...`, and the parameter descriptions). 2. Ensure this custom usage text is printed when the `-h` flag is passed or when invalid arguments are provided. 3. Note that `argparse.parse_args()` raises `SystemExit` on invalid arguments, so `except Exception:` will not catch it. Adjust the argument parsing to catch `SystemExit` or override the parser's `error()` method so that the custom usage text is printed and `melde_fehler` is called, matching the source's error handling.