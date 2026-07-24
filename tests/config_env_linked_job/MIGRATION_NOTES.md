# MIGRATION_NOTES.md: DW.CFG_LOAD_PARAMS

This document provides the comprehensive migration notes for transitioning the legacy Oracle/KornShell staging job `DW.CFG_LOAD_PARAMS` to a modern Google Cloud Platform (GCP) architecture using Cloud Composer (Airflow 2.x) and BigQuery.

---

## 1. SUMMARY

The legacy `DW.CFG_LOAD_PARAMS` workflow has been migrated from an on-premises Oracle and UC4/Automic scheduler environment to **GCP (Cloud Composer + BigQuery)**. 

### Legacy vs. Target Architecture
*   **Legacy Process**: Sourced environment variables via `.dw_init`, parsed database connection details from a local properties file (`dwh_env.properties`), executed Oracle **SQL\*Loader** (`sqlldr`) to stage parameters into `DWH_STG.PARAM_LOAD`, and ran Oracle **SQL\*Plus** (`sqlplus`) to execute `d_param_load.sql` which merged parameters into `DWH_ADM.JOB_PARAMS`.
*   **Target Process**: A Cloud Composer DAG orchestrates a unified Python script (`r_load_params.py`). This script natively parses the properties file, loads the data into BigQuery using the Google Cloud BigQuery SDK (replacing `sqlldr`), and executes the parameterized merge SQL query (`d_param_load.sql`) directly within the same execution block (replacing `sqlplus`).

---

## 2. GENERATED ARTIFACTS

The migration process generated the following files, organized by their target paths:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/dw_cfg_load_params_dag.py` | **Airflow DAG**: Orchestrates the execution of the parameter load process. Replaces the legacy UC4 `JOBS_UNIX` scheduling and job definition. |
| `config_env_linked_job/iscfg/bin/r_load_params.py` | **Python Execution Script**: Replaces `r_load_params.ksh`. Parses the local properties file, loads data to BigQuery via `load_table_from_json()`, and executes the post-load SQL merge. |
| `config_env_linked_job/iscfg/cfg/d_param_load.sql` | **Parameterized BigQuery SQL**: Replaces the legacy Oracle SQL script. Contains the standard BigQuery `MERGE` statement with dynamic dataset and project placeholders. |

---

## 3. KEY DESIGN DECISIONS

### Unified Python-Based Execution Strategy
*   **Decision**: Consolidate the properties parsing, BigQuery staging, and post-load SQL execution into a single Python script (`r_load_params.py`) executed via Airflow's `BashOperator`.
*   **Reasoning**: This eliminates the risk of orphaning the SQL script or creating complex, multi-tool dependencies (such as mixing Airflow, Dataproc, and Dataform for a simple parameter load). It ensures that the entire ingestion and merge logic runs within a single, easily monitored transactional block.
*   **Trade-off**: Moving away from Dataform for this specific task means SQL compilation is handled dynamically at runtime via Python string formatting. However, this is highly acceptable given the lightweight nature of the parameter configuration step.

### Native BigQuery SDK Ingestion (SQL\*Loader Replacement)
*   **Decision**: Replace Oracle `sqlldr` with the BigQuery Python Client's `load_table_from_json()` method using a `WRITE_TRUNCATE` write disposition.
*   **Reasoning**: This removes the need for legacy control files (`.ctl`) and local binary dependencies on the Airflow worker, leveraging GCP-native, high-speed streaming ingestion instead.

### Verbatim Error Message Retention
*   **Decision**: Retain legacy German error logging (`FEHLER: d_param_load.sql beendet mit RC={rc}`) within the Python exception handling blocks.
*   **Reasoning**: Preserves compatibility with legacy log-scraping and monitoring systems that look for specific error patterns, ensuring zero disruption to operations.

---

## 4. MANUAL STEPS BEFORE GO-LIVE

The following setup steps must be completed in the target environment before executing the migrated workflow:

### A. Schema & Dataset Creation
Ensure the following BigQuery datasets and tables exist in your target GCP project:
1.  **Staging Dataset**: `DW_STG` (or the value configured in `BQ_DATASET_STG`).
    *   Table: `PARAM_LOAD`
    *   Schema: `param_key STRING (REQUIRED)`, `param_value STRING (NULLABLE)`, `loaded_at TIMESTAMP (NULLABLE)`
2.  **Admin Dataset**: `DWH_ADM` (or the value configured in `BQ_DATASET_ADM`).
    *   Table: `JOB_PARAMS`
    *   Schema: `param_key STRING (REQUIRED)`, `param_value STRING (NULLABLE)`, `updated_at TIMESTAMP (NULLABLE)`

### B. IAM & Permissions
The Cloud Composer service account (e.g., `service-XXXXXXXX@gcp-sa-composer.iam.gserviceaccount.com`) must be granted the following IAM roles:
*   `roles/bigquery.dataEditor` on the `DW_STG` and `DWH_ADM` datasets.
*   `roles/bigquery.jobUser` on the target GCP project to run load and query jobs.

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Composer environment (via Airflow UI -> Admin -> Variables):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | Target GCP Project ID. |
| `GCS_BUCKET` | `my-composer-bucket` | GCS Bucket for environment logs/exports. |
| `DWH_HOME` | `/home/airflow/gcs/dags` | Base directory where code and config files reside. |
| `DWH_LOG_DIR` | `/home/airflow/gcs/logs` | Directory for writing execution logs. |

### D. Configuration File Deployment
1.  Deploy the `dwh_env.properties` file to the path: `{DWH_HOME}/cfg/dwh_env.properties`.
2.  Deploy the parameterized `d_param_load.sql` file to the path: `{DWH_HOME}/config_env_linked_job/iscfg/cfg/d_param_load.sql`.

### E. Scheduling
The DAG is currently configured with `schedule_interval=None` (manual execution). If this job must run on a time-based schedule, update the `schedule_interval` parameter in `dags/dw_cfg_load_params_dag.py` to the desired cron expression (e.g., `0 6 * * *` for daily at 6 AM).

---

## 5. KNOWN GAPS & UNRESOLVED REFERENCES

### Retired Legacy Components
The following legacy components have been officially retired and flagged as **NO SOURCE NEEDED** based on human review:
*   `.DW_INIT`: Environment initialization is now handled natively by Cloud Composer's environment variables.
*   `DW.HOLE_PFAD`: Path resolution is replaced by standard Python `os.path` operations and Airflow variables.
*   `DW.BERT_LESE_LOG`: Log evaluation is replaced by native Google Cloud Logging and Airflow task lifecycle states.

### Properties File Format Constraints
*   The custom Python parser in `r_load_params.py` assumes standard `key=value` formatting. If the legacy `dwh_env.properties` file contains multi-line values, backslash escapes, or complex inline comments, the parser must be updated to use a robust parser like `configparser` or a custom regex.

---

## 6. VALIDATION

To validate the migration, execute the following test cases in a non-production environment:

### Test Case 1: Local Python Script Execution (Dry Run)
1.  Set the required environment variables in your terminal:
    ```bash
    export GCP_PROJECT="your-dev-project-id"
    export DWH_HOME="/path/to/your/local/migration/folder"
    export BQ_DATASET_STG="DW_STG"
    export BQ_DATASET_ADM="DWH_ADM"
    ```
2.  Run the Python script:
    ```bash
    python3 config_env_linked_job/iscfg/bin/r_load_params.py
    ```
3.  **Verify**:
    *   The console outputs: `Lade Parameter nach PARAM_LOAD auf <host>/<sid>`.
    *   The console outputs: `Parameterladen erfolgreich abgeschlossen`.
    *   The script exits with code `0`.

### Test Case 2: End-to-End Airflow DAG Execution
1.  Trigger the `dw_cfg_load_params` DAG manually from the Airflow UI.
2.  **Verify**:
    *   The DAG runs and all tasks complete with a `SUCCESS` status.
    *   Query the BigQuery table `DW_STG.PARAM_LOAD` and confirm it contains the keys and values from `dwh_env.properties`.
    *   Query the BigQuery table `DWH_ADM.JOB_PARAMS` and confirm the parameters have been successfully merged (inserted or updated).

### Test Case 3: Failure Path Verification
1.  Temporarily rename the `d_param_load.sql` file to force a failure.
2.  Trigger the DAG.
3.  **Verify**:
    *   The DAG task `r_load_params` fails.
    *   The task logs contain the exact legacy German error message:
        `FEHLER: d_param_load.sql beendet mit RC=1`

---

## 7. ROLLBACK PROCEDURE

In the event of a critical failure during deployment or go-live, execute the following rollback steps:

### Step 1: Revert Code & Orchestration
1.  Pause the `dw_cfg_load_params` DAG in the Cloud Composer Airflow UI.
2.  Revert the Git repository to the commit prior to the deployment of the new DAG and Python scripts.
3.  Redeploy the codebase to remove the DAG from the Composer environment.

### Step 2: Database State Rollback
Since the merge operation is an upsert, rolling back requires restoring the `DWH_ADM.JOB_PARAMS` table to its state prior to the run. Use BigQuery's **Time Travel** feature to restore the table:

```sql
-- 1. Backup the corrupted table (optional safety step)
CREATE OR REPLACE TABLE `DWH_ADM.JOB_PARAMS_BACKUP` AS
SELECT * FROM `DWH_ADM.JOB_PARAMS`;

-- 2. Restore the table to its state 1 hour ago (adjust interval as needed)
CREATE OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` AS
SELECT * FROM `DWH_ADM.JOB_PARAMS`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

### Step 3: Legacy Environment Reactivation
If necessary, reactivate the legacy UC4 job `DW.CFG_LOAD_PARAMS` in the Automic scheduler to resume on-premises operations.