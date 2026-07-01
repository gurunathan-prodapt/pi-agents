# MIGRATION NOTES: k_ausd_bp_ta_rn_einzeln.ksh

This document provides comprehensive migration notes for transitioning the legacy KornShell orchestrator `k_ausd_bp_ta_rn_einzeln.ksh` to Google Cloud Platform (GCP) using Apache Airflow (Cloud Composer) and Google BigQuery.

---

## 1. Summary

The legacy KornShell script `k_ausd_bp_ta_rn_einzeln.ksh` has been migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**. 

*   **Legacy Platform**: Unix/Linux environment executing KornShell (`.ksh`) scripts, utilizing local utility scripts (`h_alis_*.ksh`), local file-based logging, and executing SQL scripts via SQL*Plus.
*   **Target Platform**: **Google Cloud Platform (GCP)**.
    *   **Orchestration**: **Apache Airflow (Cloud Composer)** manages parameter parsing, execution flow, and alerting.
    *   **Data Processing & Validation**: **Google BigQuery** handles data transformations, parameter validation, date parsing, and execution logging via standard SQL and Stored Procedures.
*   **Business Context**: This job orchestrates the data preparation and extraction pipeline for the `PoolBasisprodukt` table within the `isbert` reporting system.

---

## 2. Generated Artifacts

The migration process generated two primary artifacts to replace the legacy shell script and its dependencies:

### 1. `dags/k_ausd_bp_ta_rn_einzeln_dag.py` (Apache Airflow DAG)
*   **Role**: Acts as the entry point and orchestrator. It replaces the legacy command-line parameter parsing (`getopts`) with Airflow Params.
*   **Key Features**:
    *   Exposes runtime parameters: `job_kennung`, `eintrags_nr`, `stichtag`, and `wiederanlauf_wert`.
    *   Uses the `BigQueryInsertJobOperator` to trigger the safe outer wrapper stored procedure in BigQuery.
    *   Passes parameters securely as named query parameters to prevent SQL injection.

### 2. `stored_procedures/sp_d_ausd_bp_ta_rn_einzeln.sql` (BigQuery SQL Suite)
*   **Role**: Encapsulates all validation, date calculation, logging, and business logic.
*   **Components**:
    *   **`job_log` (Table DDL)**: Replaces the legacy local temp file (`bert_k_ausd_bp_ta_rn_einzeln.tmp`) with a persistent, queryable audit table.
    *   **`sp_validate_required_string`**: Replaces `pruefeParameterGesetzt` by asserting that mandatory parameters are present.
    *   **`sp_validate_ddmmyyyy`**: Replaces `DWDate_Datum_Check` by validating and parsing the `DDMMYYYY` date string.
    *   **`sp_init_restart_value`**: Replaces legacy shell logic to default the restart value to `'0'` if empty.
    *   **`sp_get_business_dates`**: Replaces `gestern.ksh` by dynamically calculating today's and yesterday's dates.
    *   **`sp_write_job_log`**: Standardizes success logging.
    *   **`sp_d_ausd_bp_ta_rn_einzeln`**: Placeholder for the core business logic originally located in `d_ausd_bp_ta_rn_einzeln.sql`.
    *   **`sp_k_ausd_bp_ta_rn_einzeln`**: The main wrapper procedure coordinating the validation, date calculation, business execution, and logging steps.
    *   **`sp_k_ausd_bp_ta_rn_einzeln_safe`**: A failure-safe outer wrapper that catches exceptions, logs failures to the `job_log` table with the exact error message, and re-raises the error to ensure Airflow registers the task failure.

---

## 3. Key Design Decisions

*   **Push-Down Architecture**: All business logic, parameter validation, and date calculations are pushed down to BigQuery Stored Procedures. This minimizes data movement, leverages BigQuery's massive parallel processing, and keeps the Airflow DAG lightweight and focused purely on orchestration.
*   **Modular Stored Procedures**: Instead of creating a single monolithic procedure, the validation, logging, and date calculation steps were split into reusable helper procedures. This mirrors the modular design of the legacy `h_alis_*.ksh` utilities and promotes code reuse across other migrated jobs.
*   **Robust Exception Handling**: The introduction of `sp_k_ausd_bp_ta_rn_einzeln_safe` ensures that any runtime failure (e.g., validation errors, query failures) is gracefully caught, logged to the persistent `job_log` table, and then re-raised using `RAISE USING MESSAGE` to guarantee that the Airflow task is marked as failed.
*   **Elimination of Local Filesystem Dependencies**: The legacy script relied on a local temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_einzeln.tmp`) to pass record counts. This has been replaced by standard BigQuery `OUT` parameters and persistent logging tables, removing any dependency on local VM disks.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job in production, the following manual steps must be completed:

### 1. Schema & Dataset Creation
Ensure that the target BigQuery dataset exists in your project:
```sql
CREATE SCHEMA IF NOT EXISTS `prod-isbert-data.isbert_aufbereitung`
OPTIONS(location="EU");
```
Ensure that the target table `PoolBasisprodukt` exists and is populated according to the business requirements.

### 2. Deploy Stored Procedures
Execute the entire SQL script contained in `stored_procedures/sp_d_ausd_bp_ta_rn_einzeln.sql` against your target BigQuery project to provision the `job_log` table and deploy all 8 stored procedures.

### 3. IAM & Permissions
Ensure that the Service Account running Cloud Composer / Airflow has the following IAM roles:
*   `roles/bigquery.jobUser` (to run BigQuery jobs)
*   `roles/bigquery.dataEditor` on the `isbert_aufbereitung` dataset (to execute procedures and write to the log table)

### 4. Connection Strings
Verify that the Airflow connection `google_cloud_default` is correctly configured in your Cloud Composer environment to point to the target GCP project (`prod-isbert-data`).

### 5. Scheduling & Triggering
The DAG is currently configured with `schedule_interval=None` (on-demand). If this job needs to be scheduled or chained to an upstream DAG, update the `schedule_interval` or configure a `TriggerDagRunOperator` in the parent DAG.

---

## 5. Known Gaps & Unresolved References

*   **Core Business Logic Integration (B4 Redesign Item)**: The procedure `sp_d_ausd_bp_ta_rn_einzeln` currently contains a placeholder that counts rows in `PoolBasisprodukt`. The actual SQL logic from the legacy file `d_ausd_bp_ta_rn_einzeln.sql` must be migrated to BigQuery standard SQL syntax and inlined into this procedure.
*   **Commented-out Legacy Logic**: The legacy script contains commented-out shell commands utilizing `sed`, `sort`, and `join` on files like `cibasis_data24.dat` and `cibasis_fax.dat`. The migration team must verify with business analysts if these file-based transformations are completely obsolete or if they need to be rebuilt as staging tables and queries in BigQuery.
*   **FOS Job Tracking**: The legacy script has a commented-out call to `FOSJobErzeugeEintrag`. If downstream legacy systems still require this tracking, a mechanism (such as writing to a shared Pub/Sub topic or a centralized tracking table) must be implemented.

---

## 6. Validation

To validate the migration, execute the following test cases:

### Unit Testing (BigQuery)
Run the validation procedures directly in the BigQuery console to verify parameter and date validation:

```sql
-- Test 1: Missing Parameter (Should fail with 'Jobkennung fehlt')
CALL `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`('', 'E123', '31122023', '0');

-- Test 2: Invalid Date Format (Should fail with 'Stichtag hat nicht das Format DDMMYYYY')
CALL `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`('J123', 'E123', '2023-12-31', '0');

-- Test 3: Successful Execution
CALL `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`('J123', 'E123', '31122023', '0');
```

### Integration Testing (Airflow)
1. Upload `dags/k_ausd_bp_ta_rn_einzeln_dag.py` to your Cloud Composer DAGs bucket.
2. Navigate to the Airflow UI and locate `k_ausd_bp_ta_rn_einzeln_dag`.
3. Trigger the DAG manually with the following configuration JSON:
   ```json
   {
     "job_kennung": "TEST_JOB_01",
     "eintrags_nr": "9999",
     "stichtag": "31122023",
     "wiederanlauf_wert": "0"
   }
   ```
4. **Definition of "Passing"**:
   * The Airflow DAG run completes with a `SUCCESS` status.
   * A new row is written to `prod-isbert-data.isbert_aufbereitung.job_log` containing the parameters above, `status = 'SUCCESS'`, and a non-null timestamp.

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, follow these steps to roll back the deployment:

1.  **Pause the Airflow DAG**: Navigate to the Airflow UI and toggle the switch to pause `k_ausd_bp_ta_rn_einzeln_dag`.
2.  **Revert BigQuery Objects** (Optional): If you need to clean up the deployed database objects, execute:
    ```sql
    DROP PROCEDURE IF EXISTS `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`;
    DROP PROCEDURE IF EXISTS `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln`;
    ```
3.  **Restore Table State** (If data corruption occurred): Use BigQuery's time travel feature to restore the `PoolBasisprodukt` table to its state prior to the run:
    ```sql
    CREATE OR REPLACE TABLE `prod-isbert-data.isbert_aufbereitung.PoolBasisprodukt`
    AS SELECT * FROM `prod-isbert-data.isbert_aufbereitung.PoolBasisprodukt`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
4.  **Re-enable Legacy Scheduler**: Reactivate the legacy UC4/Automic job pointing to the legacy KornShell script `k_ausd_bp_ta_rn_einzeln.ksh`.