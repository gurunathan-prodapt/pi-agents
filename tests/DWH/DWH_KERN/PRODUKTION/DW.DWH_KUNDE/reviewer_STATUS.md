# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The design document contains multiple conflicting drafts of the migration concatenated together, which caused the build step to generate three separate DAG files (dw_dwh_kunde_abgl_woechentlich.py, dw_dwh_kunde_abgl_woechentlich_js.py, d_abgl_kunde_woech_dag.py) and multiple overlapping Python execution scripts. This will cause DAG ID collisions and duplicate executions in Airflow. The design must be consolidated into a single, unified architecture with exactly one DAG and one execution script.

## Required Changes

1. Consolidate the design document to contain only one final version of the migration architecture.
2. Ensure only one Airflow DAG file is generated for the job.
3. Ensure only one Python execution script is generated for the KornShell wrapper.
4. Remove duplicate/overlapping files from the build output.