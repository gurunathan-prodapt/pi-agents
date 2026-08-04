# Migration Notes: Daily Invoice Data Export Workflow

This document details the migration of the daily invoice/rechnung data export workflow from the legacy UC4 scheduler and Oracle database environment to Google Cloud Platform (GCP) using Apache Airflow (Cloud Composer), BigQuery, and Google Cloud Storage (GCS).

---

## 1. Summary

The legacy workflow (`DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP`) was responsible for extracting daily invoice data from the Oracle `DWH_KERN` layer, formatting it as a pipe-separated flat file, and saving it to a local Unix reporting directory. 

This workflow has been migrated to **Google Cloud Platform (GCP)**:
*   **Orchestration:** Managed via **Google Cloud Composer (Airflow 2)**.
*   **Data Warehouse:** Migrated from Oracle to **Google BigQuery**.
*   **Storage:** Migrated from local Unix file systems to **Google Cloud Storage (GCS)**.
*   **Execution Logic:** Migrated from KornShell (`.ksh`) and SQL\*Plus to **Python 3** utilizing native Google Cloud Client Libraries.

---

## 2. Generated Artifacts

The migration process generated the following key artifacts:

| File Path | Language / Type | Role |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/dw_dwh_rechnung_export_taeglich_jp.py` | Python (Airflow DAG) | Orchestrates the daily workflow. Replaces the UC4 Job Plan (`JOBP`) and Unix Job (`JOBS_UNIX`). |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.py` | Python 3 (Executable) | Replaces the legacy shell script (`r_exp_rechnung_taeglich.ksh`). Handles argument parsing, BigQuery execution, pipe-separated formatting, and GCS upload. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sqlx` | SQLX (Dataform) | Defines the clean BigQuery SQL view structure for the daily invoice extraction. |

---

## 3. Key Design Decisions

### Python over Bash
The legacy KornShell script (`r_exp_rechnung_taeglich.ksh`) was converted to a native Python 3 script (`r_exp_rechnung_taeglich.py`). This avoids relying on fragile shell-wrapped CLI commands (`bq` or `gsutil`) and instead utilizes the robust, officially supported `google-cloud-bigquery` and `google-cloud-storage` SDKs. This approach provides superior error handling, native logging, and platform independence.

### Decoupled Formatting and Spooling
Legacy SQL\*Plus formatting directives (such as `set colsep '|'`, `set pagesize 0`, and `set heading off`) are not supported in BigQuery. The formatting logic has been decoupled from the database layer:
*   The query is executed cleanly in BigQuery.
*   The Python script iterates over the result set and dynamically constructs the pipe-separated format (`|`) before streaming the data to GCS.

### Dynamic Date Handling
The legacy script's date fallback logic (defaulting to yesterday's date if no `-s` parameter is supplied) has been preserved using Python's `datetime` and `timedelta` modules. In the Airflow DAG, the execution date is dynamically injected using the `{{ ds_nodash }}` macro to ensure deterministic backfills.

### Preservation of German Operational Logs
To maintain operational continuity and compatibility with legacy log parsers, all original German console outputs and warning messages have been preserved exactly as defined in the legacy scripts (e.g., `"Rechnungsexport fuer Stichtag {stichtag} angestossen"` and `"Keine Rechnungsdaten fuer Stichtag {stichtag} exportiert"`).

---

## 4. Manual Steps Before Go-Live

Before activating the migrated workflow in production, the following manual setup steps must be completed:

### 1. Schema and Dataset Creation
Ensure that the target BigQuery dataset (default: `dwh_kern`) and the source table `T_RECHNUNG` exist and are populated with data.
```sql
-- Example table verification
SELECT * FROM `your_project.dwh_kern.T_RECHNUNG` LIMIT 10;
```

### 2. IAM & Permissions
The service account running the Cloud Composer workers must be granted the following IAM roles:
*   **BigQuery:** `roles/bigquery.jobUser` and `roles/bigquery.dataViewer` (on the dataset containing `T_RECHNUNG`).
*   **Cloud Storage:** `roles/storage.objectAdmin` (on the target GCS bucket).

### 3. Airflow Variables
The following Airflow variables must be configured in the Cloud Composer environment:
*   `GCP_PROJECT`: The ID of your Google Cloud Project.
*   `GCS_BUCKET`: The name of the target GCS bucket where the export files will be stored (e.g., `my-dwh-export-bucket`).
*   `AIRFLOW_CONN_DW_UNIX_ISTNS`: The connection ID representing the execution profile (if using SSH/Worker execution).
*   `BQ_DATASET` (Optional): Overrides the default dataset name (`dwh_kern`).

### 4. Script Deployment
Deploy the executable Python script `r_exp_rechnung_taeglich.py` to the designated Airflow DAGs folder or execution path defined in the DAG configuration:
*   Target Path: `/opt/airflow/dags/dw_source/isdwh/exporter/rechnung/bin/r_exp_rechnung_taeglich.py`

### 5. Scheduling
The DAG is currently configured with `schedule=None` to match the legacy externally-triggered behavior. If this job needs to run on a daily time-based schedule, update the `schedule` parameter in `dw_dwh_rechnung_export_taeglich_jp.py` (e.g., `schedule="0 2 * * *"` for a daily run at 02:00 AM).

---

## 5. Known Gaps & Unresolved References

### Dataform vs. Direct Query Execution
The SQL query is defined as a Dataform view in `d_exp_rechnung_taeglich.sqlx`. However, the Python script `r_exp_rechnung_taeglich.py` executes the query directly against the BigQuery table to stream the results. 
*   **Redesign (B4) Item:** In a future optimization phase, the Python-based streaming export can be replaced by a native Airflow `BigQueryToGCSOperator` pointing directly to the Dataform-managed view, provided that the downstream consumers can accept standard CSV/JSON formats instead of custom pipe-separated text.

---

## 6. Validation

To validate the migration, perform the following test steps:

### Manual Execution Test
Run the Python script manually from a terminal within the Composer environment to verify database connectivity and GCS permissions:
```bash
export GCP_PROJECT="your-gcp-project-id"
export GCS_BUCKET="your-gcs-bucket-name"
export BQ_DATASET="dwh_kern"

python3 r_exp_rechnung_taeglich.py -s 20231024
```

### Airflow DAG Test
1. Trigger the DAG `dw_dwh_rechnung_export_taeglich_jp` manually from the Airflow UI.
2. Verify that the task `dw_dwh_rechnung_export_taeglich_js` completes successfully.

### Definition of "Passing"
The validation is successful if:
1. The task execution log contains the following output:
    ```text
    Starte Export Rechnungsdaten fuer Stichtag 20231024
    Anzahl exportierter Rechnungssaetze: <count>
    Export Rechnungsdaten ohne erkennbare Fehler beendet
    Rechnungsexport fuer Stichtag 20231024 angestossen
    ```
2. A file named `rechnung_export_20231024.dat` is successfully created in the GCS bucket under the path `rechnung/ausgang/`.
3. The exported file contains pipe-separated values matching the schema and row count of the source BigQuery table for that business date.

---

## 7. Rollback Procedure

In the event of an issue during deployment or execution, follow these steps to roll back to the legacy system:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the DAG `dw_dwh_rechnung_export_taeglich_jp` to **Off** (paused).
2.  **Clean Up GCS (Optional):**
    Remove any partially exported files from the GCS bucket to prevent downstream processing of corrupt data:
    ```bash
    gsutil rm gs://{GCS_BUCKET}/rechnung/ausgang/rechnung_export_{stichtag}.dat
    ```
3.  **Re-enable Legacy Scheduler:**
    Re-activate the UC4 Job Plan `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` in the Automic/UC4 interface.
4.  **Verify Legacy Execution:**
    Trigger the legacy UC4 job manually and verify that the export file is successfully written to the legacy Unix directory.