# MIGRATION_NOTES.md — DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS

This document provides the comprehensive migration notes for transitioning the daily invoice export job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` from its legacy UC4, KornShell, and Oracle/Hive infrastructure to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow), BigQuery, and Google Cloud Storage (GCS).

---

## 1. Summary

The legacy job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` has been migrated from an on-premises scheduling and database environment to a modern, cloud-native architecture on **Google Cloud Platform**.

*   **Source Platform:** UC4/Automic Scheduler, KornShell (`.ksh`) wrapper scripts, and Oracle/Hive SQL databases.
*   **Target Platform:** Google Cloud Composer (Apache Airflow 2.x), Google BigQuery, and Google Cloud Storage (GCS).
*   **Migration Pattern:** High-Confidence Data Engineering Pattern (**UC4+KSH+SQL_MEDIUM**). The legacy orchestration, parameter parsing, and validation logic are consolidated into Python-based Airflow operators, while the core data extraction runs as a high-performance native BigQuery query and export operation.

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy components. The relative folder structures have been preserved to maintain namespace integrity:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/dwh_rechnung_export_taeglich.py` | **Primary Airflow DAG:** Replaces the legacy UC4 XML job definition. Manages the end-to-end orchestration, scheduling, and task dependencies. |
| `dags/bin/r_exp_rechnung_taeglich.py` | **Python Runner Module:** Replaces `r_exp_rechnung_taeglich.ksh`. Implements the initialization logging, GCS file validation, and row-counting logic. |
| `dags/bin/r_exp_rechnung_taeglich_operator.py` | **Reusable Operator Logic:** Contains helper functions for dynamic date resolution (`Stichtag`) and direct BigQuery validation queries. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | **BigQuery SQL Script:** Replaces the legacy SQL script. Contains the parameterized query to extract daily invoice data from `T_RECHNUNG`. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bq_client.py` | **BigQuery Utility Client:** Reusable wrapper class for executing parameterized queries via the native Google Cloud BigQuery Python SDK. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/gcs_client.py` | **GCS Utility Client:** Reusable wrapper class for uploading and managing blobs in Google Cloud Storage. |

---

## 3. Key Design Decisions

### Direct BigQuery-to-GCS Export vs. Local Staging
*   **Decision:** Instead of pulling query results into the Airflow worker's local disk and writing a file, the architecture utilizes BigQuery's native export capabilities (`BigQueryInsertJobOperator` with an `extract` configuration).
*   **Reasoning:** This avoids disk space bottlenecks on Cloud Composer workers, minimizes network egress costs, and leverages Google's high-speed internal network to write directly to GCS.

### Preservation of Verbatim German Logging
*   **Decision:** All original German print statements and warning formats (e.g., `[W] ... Keine Rechnungsdaten fuer Stichtag ... exportiert`) are preserved exactly as-is in the Python code.
*   **Reasoning:** This ensures that legacy log-monitoring tools, operational runbooks, and support teams can verify job success or diagnose failures without adjusting to new log formats.

### Staging Table for Export Formatting
*   **Decision:** The query results are first written to a temporary staging table (`TEMP_RECHNUNG_EXPORT_{{ ds_nodash }}`) before being extracted to GCS as a pipe-delimited (`|`) file.
*   **Reasoning:** BigQuery's direct export API requires a table source. Staging the data ensures that the export is atomic, consistent, and easily auditable.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following setup steps must be completed in the target GCP environment:

### 1. Schema & Dataset Creation
Ensure the target BigQuery dataset and table exist in your project:
*   **Dataset:** `DWH_KERN` (or the dataset configured in Airflow variables).
*   **Table:** `T_RECHNUNG` must be defined with a schema matching the legacy structure, specifically containing the `RECHNUNGSDATUM` (DATE) column.

### 2. IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
*   `roles/bigquery.admin` or `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the target BigQuery project.
*   `roles/storage.objectAdmin` on the target GCS export bucket.

### 3. Airflow Variables Configuration
Configure the following variables in the Airflow UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-production-project` | The GCP Project ID where BigQuery and GCS reside. |
| `GCS_BUCKET` | `my-dwh-export-bucket` | The destination GCS bucket for the exported files. |
| `BQ_DATASET` | `DWH_KERN` | (Optional) Overrides the default BigQuery dataset name. |

### 4. Connection Strings
Ensure that the default Google Cloud connection (`google_cloud_default`) is configured in Airflow (**Admin -> Connections**) and points to the correct target project.

### 5. Scheduling Alignment
The DAG is configured to run daily (`@daily`). Ensure that this schedule aligns with the upstream job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` which populates the `T_RECHNUNG` table.

---

## 5. Known Gaps & Unresolved References

### 1. Upstream Job Synchronization
*   **Gap:** The legacy job was triggered as part of a larger UC4 job plan (`DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml`).
*   **Resolution:** In the current setup, the DAG runs on a time-based cron schedule. If strict dependency management is required, a `ExternalTaskSensor` or an Airflow dataset trigger should be implemented to link this export DAG directly to the upstream table-population DAG.

### 2. Downstream SFTP Delivery
*   **Gap:** The legacy shell script wrote files to a local directory where downstream systems likely retrieved them via SFTP.
*   **Resolution:** The migrated job writes files to `gs://{GCS_BUCKET}/rechnung_export/daily/`. If downstream consumers cannot pull directly from GCS, a post-processing task using the `SFTPOperator` must be added to push the file to the target SFTP server.

---

## 6. Validation

To validate the migration, execute the following test steps:

### How to Run the Tests
1.  **Manual Trigger:** Trigger the DAG manually from the Airflow UI with a custom configuration payload to test a specific historical date:
    ```json
    {
      "s": "20231024"
    }
    ```
2.  **CLI Dry-Run:** You can test individual tasks using the Airflow CLI:
    ```bash
    airflow tasks test dwh_rechnung_export_taeglich_js execute_bq_sql_export 2023-10-24
    ```

### What "Passing" Looks Like
*   **Task Logs:** The task `log_start_verbatim` must output:
    ```text
    Rechnungsexport fuer Stichtag 20231024
    Starte Export Rechnungsdaten fuer 20231024...
    ```
*   **GCS Output:** A file named `rechnung_export_20231024.csv` must be successfully created in the GCS bucket path `rechnung_export/daily/`.
*   **File Format:** The file must be pipe-delimited (`|`) and contain the correct header and data rows.
*   **Validation Task:** The `validate_and_log_verbatim` task must output:
    ```text
    Anzahl exportierter Rechnungsdatensaetze: [X]
    Export Rechnungsdaten erfolgreich beendet.
    ```
    *(Where `[X]` is the exact number of records matching that date in `T_RECHNUNG`)*.

---

## 7. Rollback Procedure

If a critical issue is discovered post-go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG:** Turn off the toggle switch for `dwh_rechnung_export_taeglich_js` in the Airflow UI to prevent further scheduled executions.
2.  **Re-enable UC4 Job:** Reactivate the legacy job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` in the UC4/Automic scheduler.
3.  **Verify Legacy Execution:** Monitor the first legacy run to ensure it successfully connects to the database, generates the local flat file, and logs output as expected.
4.  **Clean Up Cloud Staging:** Delete any temporary tables created during failed cloud runs in BigQuery (`TEMP_RECHNUNG_EXPORT_*`) to avoid unnecessary storage costs.