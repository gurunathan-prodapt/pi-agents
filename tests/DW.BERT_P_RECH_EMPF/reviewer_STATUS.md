# Reviewer Rejected — Human Review Required

**Job:** `DW.BERT_P_RECH_EMPF`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The generated Airflow DAG fails to pass the required environment variables (`BERT_DIR_ROOT` and `DW_DIR_UTL`) to the Python script in the `BashOperator`'s `env` dictionary. The migrated Python script explicitly checks for these variables and will immediately raise a `SystemExit` if they are missing, causing the task to fail.

## Required Changes

1. Update the Airflow DAG to include `BERT_DIR_ROOT`, `DW_DIR_UTL`, and `BQ_LOCATION` in the `env` dictionary of the `BashOperator`.
## Per-File Review Results

- ❌ `local/home/gurunathan_t/single_job_demo/DW.BERT_P_RECH_EMPF.xml`
  - In the `BashOperator` for `dw_bert_p_rech_empf_task`, add `"BERT_DIR_ROOT"`, `"DW_DIR_UTL"`, and `"BQ_LOCATION"` to the `env` dictionary. The migrated Python script explicitly checks for `BERT_DIR_ROOT` and `DW_DIR_UTL` and will raise a `SystemExit` if they are missing. You can map `BERT_DIR_ROOT` and `DW_DIR_UTL` to `DWH_HOME_PATH` (or a subpath of it), and map `BQ_LOCATION` to `GCP_REGION`.
- ✅ `local/home/gurunathan_t/single_job_demo/d_ausd_rechempf.sql`
- ✅ `local/home/gurunathan_t/single_job_demo/r_ausd_rechempf.ksh`