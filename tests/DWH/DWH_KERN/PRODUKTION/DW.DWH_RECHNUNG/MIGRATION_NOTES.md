# Migration Notes: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS

This document details the migration of the daily invoice export job (`DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`) from its legacy Oracle, XML, and KSH shell script environment to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy job scheduled, validated, and exported daily active billing data from the Oracle table `DWH_KERN.T_RECHNUNG` into a pipe-separated flat file. 

This job has been migrated to a modern, cloud-native architecture:
*   **Source Platform:** Oracle DB, UC4 Scheduler, Korn Shell (`.ksh`) scripts, SQL*Plus.
*   **Target Platform:** Google Cloud Platform (GCP).
*   **Target Orchestration:** Cloud Composer (Apache Airflow 2.x).
*   **Target Data Processing:** Google Cloud Dataform (SQLX) executing on BigQuery.
*   **Target Storage & Egress:** Google Cloud Storage (GCS).

---

## 2. Generated Artifacts

The migration process has generated the following modular files, structured for deployment into Cloud Composer and Dataform repositories:

| Target File Path | Role | Description |
| :--- | :--- | :--- |
| `definitions/d_exp_rechnung_taeglich.sqlx` | **Dataform Model** | Replaces the legacy Oracle SQL script. Defines the incremental staging table `tmp_export_rechnung_taeglich` in BigQuery, filtered by the dynamic parameter `stichtag`. |
| `dags/dwh_rechnung_export_taeglich_dag.py` | **Airflow DAG** | The main orchestration entry point. Defines tasks, dependencies, scheduling (`06:00 UTC`), and coordinates the execution flow. |
| `dags/modules/date_utils.py` | **Python Helper** | Resolves the target execution date (`stichtag`). Supports manual overrides via DAG run configurations, defaulting to `T-1` (yesterday) for scheduled runs. |
| `dags/modules/logger.py` | **Python Helper** | Preserves and outputs the exact legacy German console log statements to prevent breaking downstream log parsers. |
| `dags/modules/dataform_operations.py` | **Python Helper** | Interfaces with the Google Cloud Dataform API to dynamically compile and execute the SQLX model. |
| `dags/modules/bq_operations.py` | **Python Helper** | Executes validation queries (row-count checks) and extracts BigQuery table data to GCS as a pipe-separated file. |

---

## 3. Key Design Decisions

### Modular Python Architecture
Instead of placing all logic inside a single monolithic DAG file, the orchestration code is split into reusable modules (`date_utils.py`, `logger.py`, `dataform_operations.py`, `bq_operations.py`). This improves testability, maintainability, and code readability.

### Dynamic Dataform Compilation
To support backfills and historical runs, the `stichtag` parameter is resolved at runtime by Airflow and passed to Dataform as a compilation variable. This ensures that the SQLX model compiles dynamically for the exact date requested.

### Native BigQuery Egress
The legacy process used SQL*Plus spooling to format and output data. The migrated architecture uses BigQuery's native table extraction capabilities (`bigquery.ExtractJobConfig`) to export data directly to GCS. This is highly performant and avoids pulling large datasets into the Airflow worker memory.

### Verbatim Log Preservation
Downstream operational auditing tools and log parsers rely on specific German console outputs. To prevent breaking these integrations, all legacy log strings (e.g., `[W] Keine Rechnungsdaten fuer Stichtag...`) have been preserved character-for-character in `logger.py`.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG schedule in production, the following setup steps must be completed:

### 1. BigQuery Dataset & Source Table Setup
Ensure that the source table referenced by Dataform exists and is populated:
*   **Source Table:** `dw_source.tb_rechnungen` (must be partitioned/indexed appropriately).
*   **Staging Dataset:** Create the dataset `dw_staging` in your target BigQuery region if it does not already exist.

### 2. Dataform Repository Configuration
*   Create a Dataform repository named to match your `DATAFORM_REPOSITORY_ID` variable.
*   Ensure the `definitions/d_exp_rechnung_taeglich.sqlx` file is committed to the `main` branch of that repository.

### 3. IAM Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
*   `roles/dataform.editor` (To compile and trigger Dataform workflows).
*   `roles/bigquery.jobUser` (To run validation queries and extraction jobs).
*   `roles/bigquery.dataEditor` (On the `dw_staging` and `dw_source` datasets).
*   `roles/storage.objectAdmin` (On the target GCS export bucket).

### 4. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer UI (`Admin -> Variables`):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-production-project` | The target Google Cloud Project ID. |
| `GCP_LOCATION` | `europe-west3` | The GCP region for BigQuery and Dataform. |
| `DATAFORM_REPOSITORY_ID` | `dwh-dataform-repo` | The ID of the Dataform repository. |
| `GCS_BUCKET` | `my-dwh-export-bucket` | The GCS bucket where the `.dat` files will be written. |

### 5. Downstream System Reconfiguration
Downstream consumers must be reconfigured to pull the generated flat files from the GCS bucket (`gs://<GCS_BUCKET>/rechnung_export_YYYYMMDD.dat`) instead of the legacy local Unix path (`$HOME/aktuell/export/rechnung/ausgang`).

---

## 5. Known Gaps & Unresolved References

*   **Upstream Dependency Alignment:** This DAG is scheduled to run daily at `06:00 UTC`. It assumes that the upstream billing load process (`DW.DWH_RECHNUNG_LOAD_JS` or equivalent) has completed successfully. If the loading process is migrated to Airflow, this DAG should be updated to use an `ExternalTaskSensor` or be chained directly to the loading DAG instead of relying on a time-based schedule.
*   **Dataform Git Branching:** The Dataform helper is currently hardcoded to compile from the `main` branch. If your deployment pipeline uses environment-specific branches (e.g., `production`, `staging`), update the `git_commitish` parameter in `dataform_pipeline` accordingly.

---

## 6. Validation

To validate the migration, perform the following tests in a non-production environment:

### Test 1: Scheduled Run Simulation (T-1)
1. Trigger the DAG manually in the Airflow UI **without** providing any JSON configuration.
2. Verify that `resolve_stichtag_task` calculates yesterday's date (`YYYY-MM-DD`).
3. Verify that the Dataform model compiles, executes, and filters the source table using yesterday's date.
4. Verify that the output file `rechnung_export_YYYYMMDD.dat` is created in the target GCS bucket.

### Test 2: Manual Backfill Run (Custom Date)
1. Trigger the DAG manually in the Airflow UI, passing a custom configuration:
   ```json
   {"stichtag": "2023-11-20"}
   ```
2. Verify that the logs output:
   `Rechnungsexport fuer Stichtag 2023-11-20 angestossen`
3. Verify that the generated file in GCS is named `rechnung_export_20231120.dat`.

### Test 3: Zero-Row Validation
1. Trigger the DAG for a date that contains no billing records (e.g., `2099-01-01`):
   ```json
   {"stichtag": "2099-01-01"}
   ```
2. Verify that the DAG completes successfully without failing.
3. Verify that the logs output the exact warning:
   `[W] Keine Rechnungsdaten fuer Stichtag 2099-01-01 exportiert`
4. Verify that no export file is written to GCS for this date.

---

## 7. Rollback Procedure

If a critical issue is discovered in production, execute the following rollback steps:

1.  **Pause the Airflow DAG:** Navigate to the Airflow UI and toggle the switch for `dw_dwh_rechnung_export_taeglich_js` to **Off**.
2.  **Re-enable Legacy Scheduling:** Re-enable the legacy job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` in the UC4 scheduler.
3.  **Verify Legacy Execution:** Monitor the legacy Unix environment to ensure that the shell script executes, queries the Oracle database, and spools the output file to the local directory as expected.