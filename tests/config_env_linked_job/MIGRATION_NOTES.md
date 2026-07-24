# Migration Notes: `DW.CFG_LOAD_PARAMS`

This document provides comprehensive migration notes for the transition of the legacy UC4 job `DW.CFG_LOAD_PARAMS` to Google Cloud Platform (GCP).

---

## 1. Executive Summary

The legacy UC4 job `DW.CFG_LOAD_PARAMS` has been migrated from an on-premises Oracle-based environment to a native Google Cloud Platform (GCP) architecture. 

* **Legacy Platform:** UC4/Automic Scheduler + KornShell (KSH) + Oracle SQL\*Loader (`sqlldr`) + Oracle SQL\*Plus (`sqlplus`).
* **Target Platform:** Google Cloud Composer (Apache Airflow 2) + Python 3 + Google Cloud Storage (GCS) + Dataform + BigQuery.
* **Core Migration Strategy:** 
  * The orchestration logic from UC4 has been consolidated into a Cloud Composer Airflow DAG.
  * The file-parsing and staging logic previously handled by `r_load_params.ksh` and Oracle SQL\*Loader has been rewritten into a native Python script (`r_load_params.py`) utilizing the Google Cloud Storage and BigQuery Client Libraries.
  * The database merge/upsert logic previously executed via SQL\*Plus (`d_param_load.sql`) has been converted into a Dataform SQLX incremental operations model (`d_param_load.sqlx`).

---

## 2. Generated Artifacts

The migration process generated the following files, each playing a specific role in the target architecture:

| Target File Path | Language / Type | Role / Description |
| :--- | :--- | :--- |
| `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py` | Python (Airflow DAG) | **Primary Production DAG.** Orchestrates the end-to-end pipeline: executes the Python staging loader, compiles the Dataform repository, and triggers the Dataform workflow invocation to merge parameters. |
| `config_env_linked_job/iscfg/bin/r_load_params.py` | Python | **Staging Loader Script.** Replaces `r_load_params.ksh` and `sqlldr`. Downloads the properties file from GCS, parses key-value pairs, truncates the staging table, and loads the records into BigQuery (`DWH_STG.PARAM_LOAD`). |
| `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` | SQLX (Dataform) | **ELT Merge Script.** Replaces `d_param_load.sql`. Executes an incremental `MERGE` operation to upsert staged parameters from `PARAM_LOAD` into the master parameter table `DWH_ADM.JOB_PARAMS`. |
| `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params_dag.py` | Python (Airflow DAG) | *Alternative/Simplified DAG.* Executes the parameter load via direct Python module import. Retained for local/direct execution testing. |
| `dw_cfg_load_params.py` | Python (Airflow DAG) | *Alternative/Simplified DAG.* Executes the parameter load via `BashOperator` invoking the Python script. Retained for legacy-style execution testing. |

---

## 3. Key Design Decisions

### 3.1 Transition from Oracle Utilities to GCP Native
* **Decision:** Replaced Oracle SQL\*Loader (`sqlldr`) and SQL\*Plus (`sqlplus`) with native Google Cloud SDKs.
* **Reasoning:** Eliminates the need to maintain heavy Oracle client binaries, TNS configurations, and local file system dependencies on Cloud Composer workers.
* **Trade-off:** Parsing the properties file is now performed in-memory within a Python task. Because configuration files are extremely small (typically <1MB), this is highly cost-effective and avoids the overhead of spinning up a Dataproc cluster (which was initially proposed in the early UC4-only design phase).

### 3.2 Dataform for ELT Merge
* **Decision:** Migrated the database merge logic to Dataform (`d_param_load.sqlx`) rather than running raw SQL queries via Python.
* **Reasoning:** Aligns with modern ELT practices, provides built-in compilation checks, maintains data lineage within BigQuery, and separates orchestration (Airflow) from data transformation (Dataform).

### 3.3 Strict Message Preservation
* **Decision:** Retained all original German terminal output and error messages verbatim inside `r_load_params.py`.
* **Reasoning:** Ensures that legacy log-scraping and monitoring systems parsing stdout/stderr continue to function without modification.
* **Preserved Literals:**
  * `FEHLER: Parameterdatei {param_file_path} nicht gefunden` (Exits with code `1` or `8`)
  * `Lade Parameter nach {dataset_id}.{table_id} ...`
  * `Parameterladen erfolgreich abgeschlossen` (Exits with code `0`)

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the migrated pipeline, the following manual setup steps must be completed:

### 4.1 Schema and Dataset Creation
Ensure the target BigQuery datasets and tables exist with the correct schemas:
1. **Staging Dataset:** `DWH_STG`
   * Table: `PARAM_LOAD`
   * Schema:
     * `param_key` (STRING)
     * `param_value` (STRING)
     * `loaded_at` (TIMESTAMP)
2. **Administrative Dataset:** `DWH_ADM`
   * Table: `JOB_PARAMS`
   * Schema:
     * `param_key` (STRING)
     * `param_value` (STRING)
     * `updated_at` (TIMESTAMP)

### 4.2 IAM and Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
* **BigQuery:** `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` (to write to staging and execute queries).
* **Cloud Storage:** `roles/storage.objectViewer` on the GCS bucket containing the parameter files.
* **Dataform:** `roles/dataform.editor` (to compile and trigger Dataform workflows).

### 4.3 Airflow Variables
Configure the following Airflow Variables in the Cloud Composer UI or via the `gcloud` CLI:
* `GCP_PROJECT`: The target Google Cloud Project ID.
* `GCP_REGION`: The GCP region where Dataform is deployed (e.g., `us-central1`).
* `GCS_BUCKET`: The GCS bucket name where parameter files are uploaded.
* `DATAFORM_REPOSITORY`: The name of the target Dataform repository.
* `BQ_DATASET`: The target administrative dataset (defaults to `DWH_ADM`).

### 4.4 File Placement
Upload the parameter properties file to the designated GCS bucket path:
* **Path:** `gs://{GCS_BUCKET}/config/param_load.properties`

### 4.5 Scheduling
The production DAG is configured to run daily at 02:00 AM (`'0 2 * * *'`). If this job needs to be triggered on-demand or via an external event (e.g., file arrival in GCS), update the `schedule_interval` in `dw_cfg_load_params.py` to `None` and configure a Cloud Storage Trigger Cloud Function to trigger the DAG.

---

## 5. Known Gaps & Unresolved References

* **Source Code Gaps:** The original `r_load_params.ksh` and `d_param_load.sql` source files were missing during the initial migration phase. The target Python and SQLX scripts were reconstructed based on execution logs and lineage. A manual code review against the actual legacy files (if recovered) is recommended to ensure no hidden edge cases (e.g., custom local environment variables or complex shell logic) were missed.
* **Dataform Repository Integration:** The production DAG assumes a pre-existing Dataform repository configured with Git. The compilation step targets the `main` branch. If using a different branch or development workspace, the `git_commit_val` parameter in `DataformCreateCompilationResultOperator` must be updated.
* **Retired Components:** The following legacy files were confirmed obsolete by human review on 2026-07-24 and have been retired (not migrated):
  * `.DW_INIT`
  * `DW.BERT_LESE_LOG`
  * `DW.HOLE_PFAD`

---

## 6. Validation

To validate the migration, execute the following testing steps:

### 6.1 DAG Parsing Test
Verify that the Airflow DAG compiles without syntax or import errors:
```bash
python3 dags/dw_cfg_load_params.py
```

### 6.2 Task-Level Validation
Test the parameter loading task individually to verify GCS connectivity and BigQuery staging:
```bash
airflow tasks test dw_cfg_load_params load_parameters_to_staging 2026-04-21
```
* **Expected Result:** The task should complete successfully, and the console logs must output:
  ```
  Lade Parameter nach DWH_STG.PARAM_LOAD ...
  Parameterladen erfolgreich abgeschlossen
  ```

### 6.3 End-to-End Pipeline Run
1. Trigger the `dw_cfg_load_params` DAG manually from the Airflow UI.
2. Verify that:
   * `load_parameters_to_staging` completes and populates `DWH_STG.PARAM_LOAD`.
   * `create_compilation_result` compiles the Dataform repository.
   * `run_dataform_merge` executes the merge and updates `DWH_ADM.JOB_PARAMS`.
3. Query `DWH_ADM.JOB_PARAMS` in BigQuery to verify that the parameters match the values in the GCS properties file.

---

## 7. Rollback Procedure

If issues are encountered post-deployment, follow these steps to roll back the migration:

1. **Pause the Airflow DAG:**
   Disable the migrated DAG to prevent further automated runs:
   ```bash
   airflow dags pause dw_cfg_load_params
   ```
2. **Restore Target Table State:**
   If a bad parameter load corrupted `DWH_ADM.JOB_PARAMS`, restore the table to its pre-migration state using BigQuery Time Travel:
   ```sql
   CREATE OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` AS
   SELECT * FROM `DWH_ADM.JOB_PARAMS`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Revert Properties File:**
   Revert the properties file in `gs://{GCS_BUCKET}/config/param_load.properties` to its previous known-good version.
4. **Revert Code Changes:**
   If code modifications caused the failure, revert the git commits for the DAG, Python script, or Dataform SQLX files in the deployment repository.