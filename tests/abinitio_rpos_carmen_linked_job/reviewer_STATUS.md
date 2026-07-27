# Reviewer Rejected — Human Review Required

**Job:** `DW.RPOS_CARM_IMPORT`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output for the .mp file is incomplete and ends abruptly with a syntax error ('"BHB_Zielverzeichnis": f'). Additionally, the PySpark logic seems to have been placed in the .ksh replacement instead, which also drops the literal print statements from the original .ksh script (e.g., 'Error evaluating: parameter DB_TNS_NAME_DWH...'). The build needs to be retried to fix the incomplete file, properly separate the wrapper and graph logic, and preserve all required literal strings.

## Required Changes

1. Fix the incomplete file generation for the .mp file to resolve the syntax error.
2. Ensure all literal error messages from the .mp source (e.g., 'Invalid data format in monats_id') are preserved in the PySpark validation logic.
3. Preserve the literal print statements from the .ksh source script (e.g., 'Error evaluating: parameter DB_TNS_NAME_DWH...').
4. Clarify the separation of concerns between the .mp and .ksh replacements so they do not duplicate the PySpark logic.
## Per-File Review Results

- ✅ `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`
- ❌ `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`
  - Fix the incomplete file generation (syntax error at EOF) and ensure literal error messages from the source are preserved.
- ❌ `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh`
  - Preserve the literal print statements from the source script (e.g., 'Error evaluating: parameter...') and avoid duplicating the PySpark logic meant for the .mp file.