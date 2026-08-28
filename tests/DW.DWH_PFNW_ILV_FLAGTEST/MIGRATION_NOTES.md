# Migration Notes: DW.DWH_PFNW_ILV_FLAGTEST

This document details the migration of the legacy UC4 UNIX job `DW.DWH_PFNW_ILV_FLAGTEST` and its associated scripts to Google Cloud Platform (GCP).

---

## 1. Summary
The legacy UC4 job `DW.DWH_PFNW_ILV_FLAGTEST` has been migrated from an on-premise UC4/Oracle environment to **Google Cloud Platform (GCP)**. 

* **Source Platform:** UC4 (Automic) Scheduler, Oracle Database, KornShell (KSH)
* **Target Platform:** Cloud Composer (Apache Airflow), BigQuery, Python 3
* **Functional Purpose:** This job acts as a gatekeeper/validation step. It executes a SQL query to check if a specific "Fill-Flag" for Maximo has been set in a control table. If the flag is set (the query returns rows), the script exits with a failure code (`1`) to halt downstream Inter-Company Activity (ILV) processing. If no rows are returned, it exits with success (`0`), allowing the downstream pipeline to proceed.

---

## 2. Generated Artifacts
The migration process generated three primary files, each serving a distinct role in the target architecture:

1. **`dw_dwh_pfnw_ilv_flagtest.py` (Airflow DAG)**
   * *Role:* Orchestrates the execution of the validation task. It defines the DAG structure, execution parameters, and metadata.
2. **`d_pfnw_ilv_flagtest.sql` (BigQuery Standard SQL)**
   * *Role:* The rewritten validation query. It targets the migrated BigQuery table schema to check the status of the Maximo Fill-Flag.
3. **`k_pfnw_ilv_flagtest.py` (Python 3 Script)**
   * *Role:* Replaces the legacy KornShell wrapper script (`k_pfnw_ilv_flagtest.ksh`). It reads the SQL query file, executes it against BigQuery using the native Google Cloud Client Library, evaluates the row count, and manages exit codes and logging.

---

## 3. Key Design Decisions

### Python 3 over Bash/KSH
The legacy shell script (`k_pfnw_ilv_flagtest.ksh`) relied on command-line argument parsing, complex text manipulation via `nawk` (to strip SQL\*Plus headers/footers), and custom error-trapping frameworks. To ensure maintainability, security, and native integration with GCP, this logic was rewritten in **Python 3**.

### Native BigQuery Client Integration
Instead of wrapping a command-line tool like `bq` or `sqlplus`, the Python script utilizes the native `google.cloud.bigquery` library. This approach:
* Eliminates the need for fragile text-parsing utilities (`nawk`, `grep`) because BigQuery returns structured row objects.
* Simplifies row-count evaluation (`results.total_rows`).
* Provides robust, native exception handling (`GoogleCloudError`).

### Table Name Sanitization
BigQuery table identifiers do not support the dollar sign (`$`) character. The legacy Oracle table `IS_MAINT_SCHEMA.DWH$TA_K_ILV_ABR_ILV` was renamed to `is_maint_schema.dwh_ta_k_ilv_abr_ilv` in the migrated SQL script to comply with BigQuery naming standards.

### Airflow Orchestration Structure
Because no parent workflow (JOBP) or calendar schedule was provided in the source extraction, the DAG is configured with `schedule=None`. It is designed to be triggered either manually, via an external trigger, or integrated directly as an upstream task/sensor within the main ILV processing DAG.

---

## 4. Manual Steps Before Go-Live

### 1. Schema and Table Verification
Ensure that the administrative dataset and control table have been successfully migrated to BigQuery:
* **Dataset:** `is_maint_schema`
* **Table:** `dwh_ta_k_ilv_abr_ilv`

### 2. IAM & Permissions
The service account used by the Cloud Composer workers (or the specific Airflow connection) must be granted the following IAM roles on the target BigQuery dataset/table:
* `roles/bigquery.jobUser` (to run the query job)
* `roles/bigquery.dataViewer` (to read the control table)

### 3. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in your Cloud Composer environment:
* `GCP_PROJECT`: The target GCP Project ID.
* `GCP_REGION`: The target GCP Region (e.g., `europe-west3`).

### 4. Deployment of Scripts
* Place `dw_dwh_pfnw_ilv_flagtest.py` in the Airflow `dags/` folder.
* Place `k_pfnw_ilv_flagtest.py` and `d_pfnw_ilv_flagtest.sql` in the designated scripts/resources directory accessible by the Airflow workers (e.g., `/home/airflow/gcs/dags/scripts/`).

---

## 5. Known Gaps & Unresolved References

### Placeholder Task in DAG (Redesign Item)
The generated DAG `dw_dwh_pfnw_ilv_flagtest.py` currently utilizes an `EmptyOperator` as a placeholder for the execution task:
```python
dw_dwh_pfnw_ilv_flagtest_task = EmptyOperator(
    task_id="dw_dwh_pfnw_ilv_flagtest_task",
    ...
)
```
* **Required Action:** Replace this placeholder with a `BashOperator` that executes the Python script, or refactor the task to use a `PythonOperator` that imports and runs the `k_pfnw_ilv_flagtest.py` logic directly.
* *Example BashOperator implementation:*
  ```python
  from airflow.operators.bash import BashOperator

  dw_dwh_pfnw_ilv_flagtest_task = BashOperator(
      task_id="dw_dwh_pfnw_ilv_flagtest_task",
      bash_command="python3 /path/to/k_pfnw_ilv_flagtest.py -q /path/to/d_pfnw_ilv_flagtest.sql -j PFNW_ILV_FLAGTEST",
  )
  ```

### Legacy Logging Framework (`DWMSG_*`)
The legacy script made extensive use of custom logging and error-reporting utilities (`DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`, etc.). These are not present in the target environment. The migrated Python script uses standard Python `logging` and writes logs to `/tmp/validation_*.log`. 
* **Required Action:** If centralized enterprise logging or alerting (e.g., Slack/PagerDuty notifications) is required, configure Airflow's `on_failure_callback` or integrate Cloud Logging.

### Environment Bootstrap (`.dw_init`)
The legacy `.dw_init` environment file was omitted from the migration bundle. Any environment variables it dynamically set (outside of `DWH_JOB_KENNUNG`) must be verified and, if necessary, added to the Cloud Composer environment configuration.

---

## 6. Validation

### Local/Manual Test Execution
You can validate the Python script and SQL query by running them directly in an environment with active Google Cloud Application Default Credentials (ADC):

```bash
# Set the tracking environment variable
export DW_EintragsNr="99999"

# Execute the validation script
python3 k_pfnw_ilv_flagtest.py \
  -q d_pfnw_ilv_flagtest.sql \
  -j PFNW_ILV_FLAGTEST \
  -v
```

### Test Scenarios & Expected Results

| Scenario | Database State | Expected Console Output | Expected Exit Code |
| :--- | :--- | :--- | :--- |
| **Validation Passes** | No rows exist in `dwh_ta_k_ilv_abr_ilv` matching the criteria. | `Query reported no errors - script ends without any errors` | `0` |
| **Validation Fails** | A row exists where `job_starten_flag = 0` for MAXIMO FILL. | `Query reported an error - script aborts` | `1` |
| **Database Error** | Table or dataset does not exist, or permission is denied. | `ERROR: Query reported an error` | `1` |
| **Missing File** | The SQL file path passed to `-q` is invalid. | `ERROR: Query-Datei <path> not readable` | `1` |

---

## 7. Rollback Procedure

If issues are encountered post-deployment, execute the following steps to roll back to the legacy environment:

1. **Pause the Airflow DAG:**
   Disable the `dw_dwh_pfnw_ilv_flagtest` DAG in the Cloud Composer UI to prevent automated or accidental manual executions.
2. **Re-enable UC4 Job:**
   Set the active flag of the legacy UC4 job `DW.DWH_PFNW_ILV_FLAGTEST` back to active (`active=1`) in the Automic UI.
3. **Verify Legacy Infrastructure:**
   Ensure that the on-premise Oracle database and the legacy host execution paths (`$HOME/aktuell/pruef/nw/bin/k_pfnw_ilv_flagtest.ksh`) remain intact and have not been decommissioned.