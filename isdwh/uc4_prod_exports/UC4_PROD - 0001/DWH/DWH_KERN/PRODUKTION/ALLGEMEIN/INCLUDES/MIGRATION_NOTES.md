# MIGRATION_NOTES.md

**Job:** Shared Files — `isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/ALLGEMEIN/INCLUDES`  
**Target Platform:** Google Cloud Composer (Apache Airflow) & Google Cloud Dataproc

---

## 1. Summary

This migration covers the transition of legacy UC4 Include Scripts (`JOBI` objects) to a modern, cloud-native orchestration model on Google Cloud Platform (GCP). 

### Migrated Components
*   **`DW.HOLE_PFAD.xml`**: Originally responsible for setting up environment variables, home directories, and performing complex date arithmetic (e.g., calculating previous, current, and next month strings).
*   **`DW.LESE_LOG.xml`**: Originally responsible for parsing execution logs, trapping errors, executing diagnostic scripts (`SHOWLOG.KSH`), and updating administrative monitoring tables.

### Target Platform
The legacy scripts have been refactored into a centralized, reusable Python helper module (`dwh_uc4_helpers.py`) deployed to **Google Cloud Composer (Apache Airflow)**. Downstream processing payloads run on **Google Cloud Dataproc** (PySpark) and **BigQuery**, utilizing these helpers for environment setup and execution monitoring.

---

## 2. Generated Artifacts

The migration process generated two primary Python modules located in the target repository:

### 1. `dwh_uc4_helpers.py`
*   **Path:** `dags/utils/dwh_uc4_helpers.py` (or packaged within the Airflow `plugins/` directory).
*   **Role:** The core shared library containing:
    *   `get_global_gcp_config()`: Dynamically retrieves GCP project, region, bucket, and cluster configurations from Airflow Variables.
    *   `resolve_dwh_variables()`: Replicates the date arithmetic and path resolution logic of `DW.HOLE_PFAD`.
    *   `run_hole_pfad_task()`: A PythonOperator-compatible wrapper that executes variable resolution and pushes results to Airflow XCom.
    *   `register_job_monitor_state()`: Replaces the legacy `DW.DWH_ADM_JOB_MONITOR_START` and `DW.DWH_ADM_JOB_MONITOR_END` database logging scripts.
    *   `dwh_on_success_callback()` / `dwh_on_failure_callback()`: Replaces the error-trapping and log-printing logic of `DW.LESE_LOG`.

### 2. `dwh_parent_workflow_execution.py`
*   **Path:** `dags/dwh_parent_workflow_execution.py`
*   **Role:** A reference DAG implementation demonstrating how to import and wire the shared helper module into operational pipelines. It establishes the execution context and binds the success/failure callbacks to a Dataproc PySpark task.

---

## 3. Key Design Decisions

### Reusable Helper Module vs. Standalone DAGs
In UC4, include scripts are copy-pasted or referenced dynamically at runtime. In Airflow, creating standalone DAGs for these utilities would introduce unnecessary scheduling overhead and complicate state sharing. 
*   **Decision:** Consolidate the include scripts into a single, importable Python module (`dwh_uc4_helpers.py`).
*   **Trade-off:** Downstream DAGs must explicitly import this module, but it ensures a single point of maintenance and clean, Pythonic code reuse.

### XCom for Variable Propagation
Legacy scripts set global UC4 variables (e.g., `&LASTMONTH_YYYYMM`) that downstream tasks read from memory.
*   **Decision:** Use Airflow's **XCom (Cross-Communication)** engine. The initialization task pushes calculated dates and paths to XCom, allowing downstream tasks to pull them dynamically using Jinja templates:
    `{{ task_instance.xcom_pull(task_ids='initialize_dwh_context', key='LASTMONTH_YYYYMM') }}`.

### Decoupling Logging from Legacy Database Tables
The legacy system relied on direct SQL updates to administrative tables (`DWH_ADM_JOB_MONITOR`) and custom shell scripts (`SHOWLOG.KSH`).
*   **Decision:** Standardize on **GCP Cloud Logging** and Airflow's native logging framework. The callbacks write structured JSON/text logs to standard output, which are automatically ingested by Cloud Logging. 
*   **Trade-off:** Direct database updates are replaced by log-based metrics or Cloud Logging sinks that export to BigQuery. This reduces database lock contention and decouples orchestration from the transactional database.

---

## 4. Manual Steps Before Go-Live

Before deploying and executing workflows that depend on these shared includes, the following configuration steps must be completed:

### 1. Airflow Variables Creation
Create the following Airflow Variables in the Cloud Composer environment (via the Airflow UI under **Admin -> Variables** or the `gcloud composer environments run` CLI):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-project` | Target GCP Project ID |
| `GCP_REGION` | `europe-west3` | Target GCP Region |
| `GCS_BUCKET` | `prod-dwh-assets-bucket` | GCS Bucket for scripts and temporary files |
| `DATAPROC_CLUSTER` | `dwh-prod-dataproc-cluster` | Name of the active Dataproc cluster |
| `dwh_home` | `/opt/dwh` | Legacy path mapping (if needed by scripts) |
| `home` | `/home/airflow` | Airflow home directory |
| `aktiv_carmen` | `1` | Feature toggle (0/1) |
| `aktiv_crs` | `1` | Feature toggle (0/1) |

### 2. IAM & Permissions
Ensure the Cloud Composer Service Account (typically `service-[project-number]@gcp-sa-composer.iam.gserviceaccount.com` or a custom user-managed service account) has the following IAM roles:
*   **Dataproc Editor** (`roles/dataproc.editor`): To submit PySpark jobs.
*   **Storage Object Viewer** (`roles/storage.objectViewer`): To read scripts from the GCS bucket.
*   **Logs Writer** (`roles/logging.logWriter`): To write structured logs to Cloud Logging.

### 3. Code Deployment
1.  Copy `dwh_uc4_helpers.py` to the `dags/utils/` directory of your Cloud Composer environment's GCS bucket:
    ```bash
    gsutil cp dwh_uc4_helpers.py gs://[your-composer-bucket]/dags/utils/dwh_uc4_helpers.py
    ```
2.  Copy the parent workflow DAG to the `dags/` root directory:
    ```bash
    gsutil cp dwh_parent_workflow_execution.py gs://[your-composer-bucket]/dags/dwh_parent_workflow_execution.py
    ```

---

## 5. Known Gaps & Unresolved References

The following items were flagged during migration as requiring manual follow-up or architectural redesign:

### 1. Legacy Database Monitoring Redesign (B4 Redesign Item)
*   **Gap:** The legacy sub-includes `DW.DWH_ADM_JOB_MONITOR_START` and `DW.DWH_ADM_JOB_MONITOR_END` are missing from the source export.
*   **Resolution:** The helper module provides a stub function `register_job_monitor_state()`. If physical database tracking is strictly required for compliance, this stub must be updated to use an Airflow `PostgresHook` or `BigQueryHook` to write directly to a monitoring table. Otherwise, rely on Cloud Logging sinks.

### 2. Retirement of `SHOWLOG.KSH`
*   **Gap:** The legacy error handler called a custom shell script (`$HOME/tools/showlog -uc4`) to format and display logs.
*   **Resolution:** This script has been retired. The `dwh_on_failure_callback` now prints the direct Airflow Task Instance log URL (`ti.log_url`) to standard output, allowing operators to navigate directly to the failing log line in the Cloud Composer UI.

### 3. Downstream Pipeline Wiring
*   **Gap:** The downstream consumers (`DW.DWH_EXIS_IKDB_STAMM_R`, `DW.DWH_IPGD_APN_TYP`, and `DW.DWH_IPGD_QUELLREC`) have not yet been migrated.
*   **Resolution:** When migrating these downstream pipelines, ensure they import `dwh_uc4_helpers.py` and use the XCom variables instead of attempting to read local environment variables.

---

## 6. Validation

To validate the migration of the shared includes, perform the following tests:

### Unit Testing
Run a local Python test to verify that the date arithmetic matches the legacy UC4 logic:
```bash
python3 -c "
from dags.utils.dwh_uc4_helpers import resolve_dwh_variables
vars = resolve_dwh_variables('2023-10-15T00:00:00')
assert vars['LASTMONTH_YYYYMM'] == '202309', 'Failed LASTMONTH calculation'
assert vars['PRELASTMONTH_YYYYMM'] == '202308', 'Failed PRELASTMONTH calculation'
assert vars['NEXTMONTH_YYYYMM'] == '202311', 'Failed NEXTMONTH calculation'
print('All date arithmetic unit tests passed!')
"
```

### Integration Testing (Airflow DAG Run)
1.  Trigger the `dwh_parent_workflow_execution` DAG manually from the Airflow UI.
2.  **Verify Success Path:**
    *   Ensure the `initialize_dwh_context` task completes successfully.
    *   Check the task's XCom tab to verify that all variables (e.g., `LASTMONTH_YYYYMM`) are populated correctly.
    *   Verify that the task logs output: `Rueckgabewert: '0'`.
3.  **Verify Failure Path:**
    *   Temporarily modify the PySpark task to point to a non-existent GCS URI to force a failure.
    *   Trigger the DAG.
    *   Verify that the DAG fails and that the task logs output the error block:
        ```text
        ********************************************************************************
        Rueckgabewert: '1' (Fehlerfall)***************************
        ********************************************************************************
        Access Real-Time Task Execution logs via Composer/Airflow GUI:
        Log URL: http://...
        ```

---

## 7. Rollback Procedure

In the event of an operational failure or regression caused by the migrated includes:

1.  **Disable Downstream Workflows:** Pause any newly migrated DAGs that import `dwh_uc4_helpers.py` via the Airflow UI.
2.  **Revert Helper Code:** If a bug is found in the helper module, revert the `dwh_uc4_helpers.py` file in the GCS bucket to the previous stable version:
    ```bash
    gsutil cp gs://[your-composer-bucket]/dags/utils/dwh_uc4_helpers.py.bak gs://[your-composer-bucket]/dags/utils/dwh_uc4_helpers.py
    ```
3.  **Legacy Fallback:** If a complete rollback to UC4 is required, re-enable the corresponding UC4 Job Plans (`JOBP`) and ensure the Airflow DAG schedules are paused to prevent duplicate processing.