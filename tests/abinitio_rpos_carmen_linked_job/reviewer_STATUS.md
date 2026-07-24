# Reviewer Rejected — Human Review Required

**Job:** `DW.RPOS_CARM_IMPORT`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output has structural disconnects spanning all three generated files, and a critical logic bug in the PySpark script. The DAG orchestrates the PySpark script directly but passes arguments that the PySpark script does not parse (it relies on `os.environ` which will be `None` in Dataproc, causing a crash). Meanwhile, the generated Python wrapper script is orphaned (not called by the DAG) and attempts to pass different arguments while duplicating the audit table update logic. Additionally, the PySpark script's `save_with_paired_reload` function hardcodes 5 join keys for the delete-before-insert anti-join, which is incorrect for all 5 target tables (legacy logic used 4 keys for the first four tables, and 3 completely different keys for `dwh_ta_t_rpos_carm`), leading to incorrect deletions. A full job retry is required to align the orchestration and fix the deletion keys.

## Required Changes

1. Align argument passing across the DAG, wrapper script, and PySpark script. The PySpark script must use `argparse` to read parameters (including GCP_PROJECT, BQ_DATASET, GCS_BUCKET) instead of `os.environ`, and the DAG/wrapper must pass them correctly.
2. Clarify the orchestration: if the DAG calls the PySpark script directly, the wrapper script should not duplicate the Dataproc submission and audit table updates.
3. In the PySpark script, modify `save_with_paired_reload` to accept a `join_keys` parameter. Pass the correct keys for each table to match legacy delete statements (e.g., `['rechnung_datum', 'rechnung_id', 'standardvertrags_id', 'vertrags_id']` for the first four tables, and `['debitor_id', 'rechnung_datum', 'rechnung_id']` for `dwh_ta_t_rpos_carm`).
4. The DAG uses `DataprocSubmitJobOperator` (for clusters) but the design specifies Dataproc Serverless. Use `DataprocCreateBatchOperator` instead.
## Per-File Review Results

- ✅ `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`
- ✅ `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`
- ✅ `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh`