# Migration Notes: DW.DWH_ALL_TYPES_MASTER

This document provides comprehensive migration notes and operational instructions for transitioning the legacy UC4/Automic job `DW.DWH_ALL_TYPES_MASTER` to its cloud-native target environment on Google Cloud Platform (GCP).

---

## 1. Summary

The legacy UC4 Unix job `DW.DWH_ALL_TYPES_MASTER` has been migrated from an on-premises Unix/Oracle environment to **GCP (Cloud Composer / Apache Airflow & BigQuery)**. 

### Scope of Migration
*   **Orchestration:** Migrated from UC4 (`JOBS_UNIX`) to an **Apache Airflow DAG** running on Cloud Composer.
*   **Primary Processing:** The legacy Ab Initio graph execution (`all_types_graph`) has been mapped to a **Dataproc Serverless PySpark task** (pre-migrated in PR #883).
*   **Post-Processing Wrapper:** The KornShell (KSH) master script `r_all_types_master.ksh` has been refactored into a **Python 3 script** (`r_all_types_master.py`).
*   **Data Transformation:** The AWK script `k_all_types_transform.awk` has been converted to a **Python 3 script** (`k_all_types_transform.py`) to preserve strict row-validation and exit-code signaling.
*   **Database Refresh:** The Oracle SQL*Plus script `d_all_types.sql` has been rewritten into **BigQuery Standard SQL** utilizing dynamic SQL and structured exception handling.
*   **Environment Sourcing:** The legacy `.dw_init` environment script has been **retired**. Environment variables and directory paths are now managed natively via Airflow Variables and GCP environment configurations.

---

## 2. Generated Artifacts

The migration process generated the following files, each serving a specific role in the target architecture:

| Generated File Path | Language / Type | Role |
| :--- | :--- | :--- |
| `DWH_ALL_TYPES_JOB/dw_dwh_all_types_master.py` | Python (Airflow DAG) | The master DAG that orchestrates the sequential execution of the Dataproc PySpark job and the post-processing Python wrapper. |
| `isall/aufbereitung/bin/r_all_types_master.py` | Python 3 | Replaces `r_all_types_master.ksh`. Coordinates the execution of the BigQuery SQL refresh and invokes the AWK-migrated Python script. |
| `isall/aufbereitung/awk/k_all_types_transform.py` | Python 3 | Replaces `k_all_types_transform.awk`. Validates that input records contain exactly 12 fields, prepends `"D;"` to valid records, and exits with code `2` on failure. |
| `isall/aufbereitung/sql/d_all_types.sql` | BigQuery SQL | Replaces `d_all_types.sql`. Truncates the target table and inserts qualified records from the raw table using dynamic SQL parameterized by project and dataset. |

---

## 3. Key Design Decisions

### 3.1. Python 3 for AWK Transformation
*   **Decision:** Convert `k_all_types_transform.awk` to a standalone Python 3 script rather than attempting to express the logic in SQL.
*   **Reasoning:** The AWK script acts as a strict validation gate. If a record does not contain exactly 12 fields, it must immediately halt processing and return a process-level exit code (`exit 2`) to fail the orchestrator. BigQuery SQL cannot raise custom OS-level exit codes mid-query. Python's streaming file-processing capabilities handle this control flow natively and efficiently.

### 3.2. Python 3 for KSH Wrapper
*   **Decision:** Refactor `r_all_types_master.ksh` into `r_all_types_master.py`.
*   **Reasoning:** Python provides cross-platform compatibility, robust subprocess execution tracking (`subprocess.run(..., check=True)`), and native integration with the Google Cloud BigQuery client library. This eliminates the need for legacy shell utilities like `sqlplus` and `tee`.

### 3.3. Dynamic BigQuery SQL with Exception Handling
*   **Decision:** Wrap the BigQuery SQL statements in a `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block and use `EXECUTE IMMEDIATE` for table references.
*   **Reasoning:** 
    *   **Environment Portability:** Using `EXECUTE IMMEDIATE` with `@gcp_project` and `@bq_dataset` query parameters prevents hardcoding environment-specific paths, allowing the same SQL file to run unmodified across Dev, Test, and Prod.
    *   **Error Propagation:** The exception block catches failures, logs diagnostic details, and re-raises the error to ensure the calling Airflow task registers a failure (mimicking Oracle's `WHENEVER SQLERROR EXIT FAILURE`).

### 3.4. Retirement of `.dw_init`
*   **Decision:** Retire the `.dw_init` environment script.
*   **Reasoning:** Sourcing flat-file environment scripts is a legacy pattern. In Cloud Composer, environment variables, directory paths, and system configurations are managed natively via Airflow Variables and GKE container environment variables.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target GCP environment before triggering the DAG:

### 4.1. Schema and Dataset Creation
1.  Ensure the target BigQuery dataset (configured in the Airflow Variable `BQ_DATASET`) exists in your project.
2.  Verify that the following tables exist with compatible schemas:
    *   `cds$ta_all_types_raw` (Source table)
    *   `sof$ta_all_types` (Target table; ensure `processed_at` is typed as `DATETIME` or `TIMESTAMP`).

### 4.2. IAM & Permissions
Ensure the service account running the Cloud Composer workers has the following IAM roles:
*   **BigQuery Admin** (`roles/bigquery.admin`) or **BigQuery Data Editor** (`roles/bigquery.dataEditor`) + **BigQuery Job User** (`roles/bigquery.jobUser`).
*   **Dataproc Editor** (`roles/dataproc.editor`) to submit PySpark jobs.
*   **Storage Object Admin** (`roles/storage.objectAdmin`) on the GCS bucket hosting scripts and data.

### 4.3. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | The target GCP Project ID. |
| `GCP_REGION` | `europe-west3` | The GCP region for Dataproc and Composer. |
| `DATAPROC_CLUSTER` | `dwh-dataproc-cluster` | The name of the Dataproc cluster. |
| `GCS_BUCKET` | `my-dwh-migration-bucket` | The GCS bucket containing PySpark scripts and data. |
| `BQ_DATASET` | `dwh_all_types` | The target BigQuery dataset name. |
| `ALL_DIR_ROOT` | `/isall` | The root directory path for scripts on the worker. |

### 4.4. File Deployment
Deploy the migrated scripts to their respective locations:
*   Upload `dw_dwh_all_types_master.py` to the Composer DAGs bucket (`gs://<composer-bucket>/dags/`).
*   Upload `all_types_graph.py` (from PR #883) to `gs://<GCS_BUCKET>/pyspark_scripts/`.
*   Deploy `r_all_types_master.py`, `k_all_types_transform.py`, and `d_all_types.sql` to the local file system of the execution environment (e.g., mounted directory `/isall/` on the worker or via GCS Fuse).

### 4.5. Scheduling
The DAG is configured with `schedule=None` (externally triggered), matching the legacy UC4 configuration. If this job needs to be scheduled, update the `schedule` parameter in `dw_dwh_all_types_master.py` with a standard cron expression.

---

## 5. Known Gaps & Unresolved References

### 5.1. Local File System Dependency (Redesign Item)
*   **Gap:** The migrated Python wrapper `r_all_types_master.py` and the AWK-migrated script `k_all_types_transform.py` read and write files on the local file system (e.g., `/isall/data/all_types_export.csv`).
*   **Risk:** Cloud Composer workers are ephemeral and distributed. Local file writes will not persist across different worker nodes or restarts.
*   **Recommendation (B4 Redesign):** Refactor `r_all_types_master.py` and `k_all_types_transform.py` to read and write directly to Google Cloud Storage using the `google-cloud-storage` client library, or ensure that a shared persistent volume (such as GCS Fuse) is consistently mounted at `/isall` across all Composer workers.

### 5.2. Sourced Environment Files
*   **Gap:** The legacy `.dw_init` script sourced `.dw_global` and `.dw_lokal`. These files were flagged as "not needed" during human review.
*   **Action:** If any hidden environment variables from those files are discovered to be missing during testing, they must be added to the Airflow Variables configuration.

---

## 6. Validation

To validate the migration, execute the following test cases.

### 6.1. Unit Test: AWK-to-Python Script
Run the Python script locally with valid and invalid data:
```bash
# Test Case 1: Valid 12-field record (Should pass and prepend "D;")
echo "1;2;3;4;5;6;7;8;9;10;11;12" | python3 isall/aufbereitung/awk/k_all_types_transform.py
# Expected Output: D;1;2;3;4;5;6;7;8;9;10;11;12

# Test Case 2: Invalid field count (Should print error and exit with code 2)
echo "1;2;3" | python3 isall/aufbereitung/awk/k_all_types_transform.py
# Expected Output: Error: Incorrect nos of Fields 
# Expected Exit Code: 2 (Verify with 'echo $?')
```

### 6.2. Unit Test: BigQuery SQL Script
Execute the SQL script in the BigQuery Console. Declare the parameters at the top of your query window for testing:
```sql
DECLARE gcp_project STRING DEFAULT 'your-project-id';
DECLARE bq_dataset STRING DEFAULT 'your_dataset';

-- Paste the body of isall/aufbereitung/sql/d_all_types.sql here (excluding the outer BEGIN/END if testing statements individually)
```
*   **Passing Criteria:** The query executes without errors, truncates `sof$ta_all_types`, and inserts rows from `cds$ta_all_types_raw` where `status = 'READY'`.

### 6.3. End-to-End DAG Integration Test
1.  Trigger the DAG `dw_dwh_all_types_master` manually from the Airflow UI.
2.  Monitor the execution of both tasks:
    *   `jobs_unix_dw_dwh_all_types_master` (Dataproc PySpark job)
    *   `post_processing_master` (Python wrapper execution)
3.  **Passing Criteria:**
    *   Both tasks complete with a `success` status.
    *   The log file `all_types_master_DDMMYYYY.log` is created in the log directory and contains the exact German log statements:
        *   `----Starte SQL-Refresh----`
        *   `----Starte AWK-Nachbearbeitung----`
        *   `Die Abarbeitung wurde ohne erkennbare Fehler beendet`
    *   The target table `sof$ta_all_types` is successfully populated.

---

## 7. Rollback Procedure

If critical issues are encountered post-go-live, follow these steps to revert to the legacy environment:

1.  **Pause the Airflow DAG:**
    Go to the Cloud Composer Airflow UI and toggle the switch to pause the DAG `dw_dwh_all_types_master`.
2.  **Revert Database State (Optional):**
    If the target table `sof$ta_all_types` was corrupted by the migration run, truncate the table or restore it to its pre-migration state using a BigQuery table snapshot:
    ```sql
    TRUNCATE TABLE `your-project-id.your_dataset.sof$ta_all_types`;
    ```
3.  **Re-enable Legacy Orchestration:**
    Re-activate the `DW.DWH_ALL_TYPES_MASTER` job in the legacy UC4/Automic system.
4.  **Verify Legacy Execution:**
    Trigger a manual run in UC4 and verify that the Ab Initio graph, Oracle SQL, and AWK steps execute and log successfully on the legacy Unix host.