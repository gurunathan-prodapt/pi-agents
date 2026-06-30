# Migration Notes: `ausd_bp_ta_p_basisprod`

This document details the migration of the `ausd_bp_ta_p_basisprod` data pipeline from a legacy Oracle/UC4 environment to Google Cloud Platform (BigQuery and Cloud Composer/Airflow).

---

## 1. Summary

The `ausd_bp_ta_p_basisprod` job is a medium-to-high complexity data processing pipeline within the `BERT` (Forderungsscoring) module of the Data Warehouse. It consolidates contract, SIM card, telephone number, APN mapping, and tariff option data to prepare and instantiate base products for downstream scoring.

*   **Source Platform**: UC4/Automic, KornShell Orchestration (`r_ausd_bp_ta_p_basisprod.ksh`, `k_ausd_bp_ta_p_basisprod.ksh`), and Oracle SQL\*Plus (`d_ausd_bp_ta_p_basisprod.sql`).
*   **Target Platform**: Google Cloud Platform (GCP)
    *   **Orchestration**: Apache Airflow (Google Cloud Composer)
    *   **Compute & Storage**: Google Cloud BigQuery
    *   **Target Table**: `sof.ta_p_basisprod` (formerly `sof$ta_p_basisprod`)

---

## 2. Generated Artifacts

The migration process has generated the following target files to replace the legacy KornShell and SQL\*Plus scripts:

| Target File Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_p_basisprod.py` | Python (Airflow DAG) | Orchestrates the pipeline. Handles parameter validation (`stichtag`), executes table truncation, triggers the main BigQuery SQL load, and logs execution status to audit metadata. |
| `gcp_sql/d_ausd_bp_ta_p_basisprod.sql` | BigQuery SQL | The consolidated, BigQuery-compatible SQL script. Performs the massive multi-table join to populate the target table `ta_p_basisprod`. Uses Airflow variables for environment-agnostic dataset referencing. |

---

## 3. Key Design Decisions

### Consolidation of Shell Logic into Airflow
The legacy architecture used two layers of KornShell wrappers (`r_*.ksh` and `k_*.ksh`) to validate dates, parse parameters, and execute SQL\*Plus. This logic has been consolidated directly into the Airflow DAG:
*   **Date Validation**: Handled natively via a Python operator (`validate_date_format`) which checks the incoming `stichtag` parameter format (`DDMMYYYY`).
*   **Environment Isolation**: Hardcoded Oracle schema prefixes (`sof$`, `isbert_schema`) have been replaced with dynamic Airflow variables (`sof_dataset`, `isbert_schema_dataset`, `gcp_project_id`), allowing the same code to run across Dev, QA, and Prod environments without modification.

### SQL Optimization & Modernization
*   **Removal of Optimizer Hints**: Oracle-specific parallel execution hints (e.g., `/*+ ORDERED FULL... parallel(cn,4) */`) were stripped out. BigQuery automatically handles query planning, scaling, and parallelization.
*   **Truncate-and-Insert Pattern**: To maintain transactional consistency and prevent duplicate loads on retry, the DAG explicitly executes a `TRUNCATE TABLE` task followed by an `INSERT INTO` task.
*   **Dynamic Date Lookup**: The legacy script dynamically determined a processing date (`v_datum`) from the `dwtk_meldungen` table. This has been translated into a BigQuery SQL scripting variable declaration (`DECLARE v_datum STRING DEFAULT (...)`) at the beginning of the SQL file, preserving the exact business logic.

---

## 4. Manual Steps Before Go-Live

Before activating the Airflow DAG in production, the following setup steps must be completed:

### 1. Schema & Dataset Creation
Ensure the target BigQuery datasets exist in your GCP project:
*   `sof` (or the configured dataset name for target tables)
*   `isbert_schema` (or the configured dataset name for metadata/audit tables)

Ensure the target table `ta_p_basisprod` and the audit table `job_audit_metadata` are created with schemas matching the legacy Oracle definitions.

### 2. IAM & Permissions
The Cloud Composer Service Account must have the following IAM roles:
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target datasets (`sof`, `isbert_schema`).
*   **BigQuery Job User** (`roles/bigquery.jobUser`) on the GCP project.

### 3. Airflow Variables Configuration
The following Airflow variables must be configured in the Cloud Composer environment:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `gcp_project_id` | `my-gcp-project-prod` | The target GCP Project ID. |
| `gcp_location` | `EU` | The geographic location for BigQuery jobs. |
| `sof_dataset` | `sof` | The BigQuery dataset name for SOF tables. |
| `isbert_schema_dataset` | `isbert_schema` | The BigQuery dataset name for audit/metadata. |

### 4. Connection Strings
The Airflow DAG uses the default Google Cloud connection (`google_cloud_default`). Ensure this connection is properly configured in Airflow Connections to point to the correct GCP environment.

### 5. Scheduling
The DAG is currently configured with `schedule_interval=None` (manual/triggered execution). If this job needs to run on a schedule or downstream of other tables (e.g., after `ta_cntrct_dist` is loaded), update the `schedule_interval` or configure an Airflow Dataset/Trigger rule.

---

## 5. Known Gaps & Unresolved References

1.  **MultiSIM 10 Hardcoding**: The legacy SQL explicitly maps slave cards up to `MS10` (columns `MS1_` through `MS10_`). This hardcoded structure was carried over to BigQuery to maintain schema compatibility. If future business requirements introduce `MS11` or beyond, both the target BigQuery schema and the `d_ausd_bp_ta_p_basisprod.sql` file must be manually updated.
2.  **Upstream Table Dependencies**: The SQL query references several upstream tables (e.g., `ta_cntrct_dist`, `ta_bcp_iccid`, `ta_bcp_msisdn`, etc.). These tables must be fully migrated and populated in BigQuery before this job can run successfully.
3.  **Omitted Temp Table Cleanup**: The legacy Oracle script contained commented-out steps for dropping temporary tables (e.g., `sof$ta_msisdn_his`). These have been omitted from the BigQuery SQL to avoid maintenance overhead, assuming staging cleanup is handled by upstream processes.

---

## 6. Validation

To validate the migrated pipeline, perform the following steps:

### How to Run the Tests
1.  **Dry Run**: Trigger the DAG manually in Airflow with a test configuration payload containing a dummy `stichtag`:
    ```json
    {
      "stichtag": "31122023"
    }
    ```
2.  **Verify Execution Flow**: Ensure all tasks (`validate_date_format` -> `truncate_target_table` -> `load_basisprod` -> `mark_job_execution_status`) complete with a `SUCCESS` status.

### What "Passing" Means
*   **Row Count Match**: Run a comparison query between the legacy Oracle `sof$ta_p_basisprod` table and the new BigQuery `sof.ta_p_basisprod` table for a given historical processing date. The row counts must match exactly.
*   **Data Integrity**: Sample key columns (e.g., `CNTRCT_ID`, `TNV_ICCID`, `TNV_MSISDN`, `APN`) across both platforms to verify that string concatenations (such as the APN field merge) and `LEFT JOIN` logic yielded identical results.
*   **Audit Trail**: Verify that a new entry has been written to the `job_audit_metadata` table with `status = 'SUCCESS'` and the correct timestamp.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during go-live:

1.  **Pause the Airflow DAG**: Immediately pause the `dw_bert_ausd_bp_ta_p_basisprod` DAG in the Airflow UI to prevent further automated runs.
2.  **Re-enable Legacy Orchestration**: Re-enable the UC4/Automic job `DW.BERT_AUSD_BP_TA_P_BASISPROD` in the legacy environment.
3.  **Restore Target Table (Optional)**: If downstream systems rely on the BigQuery table and it contains corrupted data, restore it to its pre-migration state using BigQuery's time-travel feature:
    ```sql
    CREATE OR REPLACE TABLE `sof.ta_p_basisprod`
    AS SELECT * FROM `sof.ta_p_basisprod` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```