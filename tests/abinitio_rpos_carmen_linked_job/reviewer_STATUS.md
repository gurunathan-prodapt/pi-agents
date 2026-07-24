# Reviewer Rejected — Human Review Required

**Job:** `DW.RPOS_CARM_IMPORT`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The design document contains multiple concatenated and conflicting file plans (e.g., generating both `dw_rpos_carm_import.py` and `dw_rpos_carm_import_dag.py` with the same DAG ID `dw_rpos_carm_import`, as well as multiple PySpark scripts and config files). This causes the build to output duplicate files and creates an Airflow DAG ID collision. Additionally, several literal error messages from the source (e.g., 'Invalid Data in field monats_id', 'Invalid Data in field debitor_id') were dropped in the PySpark scripts.

## Required Changes

1. Consolidate the design into a single, coherent file plan with exactly one Airflow DAG, one PySpark script, and one config file.
2. Ensure the Airflow DAG ID is unique and only defined in one file to prevent Airflow parse errors.
3. Preserve all literal error messages from the source `.mp` file (e.g., 'Invalid Data in field monats_id', 'Invalid Data in field debitor_id', etc.) in the PySpark validation logic.