# Reviewer Rejected — Human Review Required

**Job:** `DW.RPOS_CARM_IMPORT`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output failed to implement the target files specified in the final sections of the design document. It completely missed the wrapper script, placed a stub for the PySpark script in the wrong directory (under cfg/bd_proc/ instead of mp/), and ignored the complete PySpark and Airflow DAG code provided in the design.

## Required Changes

1. Generate the PySpark script `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` using the full code provided in section 6.1 of the design document.
2. Generate the wrapper script `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import_wrapper.py` (or `.py` as named in the design) using the full code provided in the design document, ensuring all verbatim print statements are preserved.
3. Update the Airflow DAG to match the final version provided in section 6.2 of the design document, which includes the GCS sensor and correct task dependencies.
4. Remove the incorrectly placed stub file `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.py`.