# MIGRATION NOTES

**Migration Target:** Google Cloud Platform (BigQuery + Cloud Composer / Apache Airflow)  
**Source Component:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` (and associated Oracle SQL execution)

---

## 1. Summary

The legacy KornShell script `k_ausd_bp_ta_bpr_apn.ksh` has been successfully migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**. 

The source script functioned as an orchestration wrapper that validated runtime parameters, verified date formats, derived relative dates (today/yesterday), executed an external Oracle SQL*Plus script (`d_ausd_bp_ta_bpr_apn.sql`), and logged execution metadata. 

In the target architecture:
*   **Orchestration & Parameter Passing** are handled by **Apache Airflow (Cloud Composer)**.
*   **Validation, Date Derivation, and Core ETL Logic** are encapsulated within **BigQuery Stored Procedures** using Google Standard SQL.
*   **Legacy File-Based Operations** (commented-out `sed`/`sort`/`join` routines) have been refactored into clean, declarative SQL operations within BigQuery.

---

## 2. Generated Artifacts

The migration process generated the following artifacts, organized by their target environment:

### BigQuery Stored Procedures (`bigquery/stored_procedures/`)
*   **`sp_k_ausd_bp_ta_bpr_apn.sql`**  
    *Role:* The primary entry point. It replaces the core logic of the KSH wrapper. It performs parameter validation, parses the legacy `DDMMYYYY` date format into a native BigQuery `DATE` object, executes the core transformation (currently a placeholder for the migrated `d_ausd_bp_ta_bpr_apn.sql`), logs execution metadata to a tracking table, and returns an execution summary.
*   **`sp_merge_cibasis_legacy.sql`**  
    *Role:* Recreates the commented-out legacy file-based processing pipeline. It replaces the OS-level `sed`, `sort`, and `join` commands on `.dat` files with high-performance `FULL OUTER JOIN` and `REGEXP_REPLACE` operations on BigQuery staging tables.
*   **`sp_validate_ddmmyyyy.sql`**  
    *Role:* A reusable utility procedure that validates whether a string conforms to the legacy `DDMMYYYY` format, mimicking the behavior of the legacy `h_alis_date.ksh` helper.

### Orchestration (`orchestration/dags/`)
*   **`dag_k_ausd_bp_ta_bpr_apn.py`**  
    *Role:* An Apache Airflow DAG that schedules and triggers the main BigQuery stored procedure. It dynamically passes runtime parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) using Airflow's `dag_run.conf` or default execution context.

---

## 3. Key Design Decisions

### Stored Procedures over Cloud Run / Dataflow
*   *Decision:* Implement the orchestration and transformation logic directly inside BigQuery Stored Procedures.
*   *Reasoning:* The source process is highly database-centric. Keeping the logic in BigQuery minimizes data egress, reduces architectural complexity, and leverages BigQuery's serverless compute engine. It avoids the overhead of managing containerized applications (Cloud Run) or Apache Beam pipelines (Dataflow) for standard SQL transformations.

### Backward-Compatible Parameter Interface
*   *Decision:* Retain the `DDMMYYYY` string format for the `p_Stichtag` parameter in the stored procedure interface, but immediately parse it to a native BigQuery `DATE` (`YYYY-MM-DD`) internally.
*   *Reasoning:* This maintains compatibility with upstream legacy schedulers or external systems that trigger this job, while ensuring downstream BigQuery tables benefit from native date partitioning and performance optimizations.

### Separation of Legacy File-Merge Logic
*   *Decision:* Isolate the commented-out file-merge logic into its own stored procedure (`sp_merge_cibasis_legacy.sql`).
*   *Reasoning:* The source script had this logic commented out, indicating it may be deprecated. By isolating it, we avoid cluttering the active production pipeline while still providing a fully functional, SQL-compliant version should business requirements demand its reactivation.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated pipeline in production, the following manual setup steps must be completed:

### 1. Schema & Dataset Creation
Ensure the target BigQuery dataset exists and create the required tables.
```sql
-- Create Dataset (if not exists)
CREATE SCHEMA IF NOT EXISTS `project_id.isbert_dataset`;

-- Create Target Table
CREATE TABLE IF NOT EXISTS `project_id.isbert_dataset.PoolBasisprodukt` (
  -- Define columns matching the schema of PoolBasisprodukt
  stichtag DATE,
  -- [Add other columns here]
);

-- Create Job Tracking Table
CREATE TABLE IF NOT EXISTS `project_id.isbert_dataset.job_tracking` (
  tab_name STRING,
  job_kennung STRING,
  eintrags_nr STRING,
  stichtag DATE,
  wiederanlauf_wert STRING,
  records INT64,
  created_at TIMESTAMP
);
```

### 2. IAM & Permissions
The service account running the Airflow DAG (Cloud Composer worker service account) must have the following IAM roles:
*   `roles/bigquery.jobUser` (to run BigQuery jobs)
*   `roles/bigquery.dataEditor` on the dataset `project_id.isbert_dataset` (to read/write tables and execute procedures)

### 3. Airflow Connection Configuration
Ensure that the Airflow connection `google_cloud_default` is configured with the correct GCP Project ID where the BigQuery dataset resides.

### 4. Scheduling Configuration
The DAG is currently configured with `schedule_interval=None` (manual/ad-hoc trigger). Update the `schedule_interval` in `dag_k_ausd_bp_ta_bpr_apn.py` to match your business requirements (e.g., daily, weekly) if automated scheduling is required.

---

## 5. Known Gaps & Unresolved References

### 1. Core SQL Logic Integration (Critical)
*   *Gap:* The actual transformation logic from the Oracle SQL script `d_ausd_bp_ta_bpr_apn.sql` is not embedded in the main stored procedure.
*   *Action Required:* Once `d_ausd_bp_ta_bpr_apn.sql` is migrated to BigQuery SQL, its logic must replace the placeholder `CREATE TEMP TABLE tmp_poolbasisprodukt` block inside `sp_k_ausd_bp_ta_bpr_apn.sql`.

### 2. Legacy File Ingestion
*   *Gap:* The legacy script processed local files (`cibasis_data24.dat`, etc.). If the legacy file-merge logic (`sp_merge_cibasis_legacy.sql`) is reactivated, these files must first be uploaded to Google Cloud Storage (GCS) and loaded into BigQuery staging tables (`cibasis_data24_source`, etc.).
*   *Action Required:* Establish a GCS upload pipeline (e.g., via Storage Transfer Service or `gsutil`) and define BigQuery external tables or load jobs for these files.

---

## 6. Validation

To validate the migration, execute the following test suite:

### Test Case 1: Parameter Validation (Failure Path)
Trigger the stored procedure with missing or invalid parameters to ensure the validation framework catches errors.
```sql
-- This should fail with "FEHLER: 1 - Jobkennung fehlt"
CALL `project_id.isbert_dataset.sp_k_ausd_bp_ta_bpr_apn`('', '12345', '31122023', '0');

-- This should fail with "FEHLER: Ungueltiges Datum im Format DDMMYYYY"
CALL `project_id.isbert_dataset.sp_k_ausd_bp_ta_bpr_apn`('JOB_TEST', '12345', '31-12-2023', '0');
```

### Test Case 2: Successful Execution (Happy Path)
Trigger the stored procedure with valid parameters.
```sql
CALL `project_id.isbert_dataset.sp_k_ausd_bp_ta_bpr_apn`('JOB_TEST', '12345', '31122023', '0');
```
*   **Expected "Passing" Criteria:**
    1.  The procedure executes without runtime errors.
    2.  A summary row is returned showing `records_processed`.
    3.  A new entry is written to `project_id.isbert_dataset.job_tracking` containing the metadata for `JOB_TEST`.
    4.  Data is successfully inserted into `project_id.isbert_dataset.PoolBasisprodukt`.

### Test Case 3: Airflow DAG End-to-End Test
Trigger the DAG manually from the Airflow UI with the following configuration JSON:
```json
{
  "p_JobKennung": "AIRFLOW_TEST",
  "p_EintragsNr": "99999",
  "p_Stichtag": "31122023",
  "p_wiederanlaufWert": "0"
}
```
*   **Expected "Passing" Criteria:**
    *   The DAG run completes with a `success` status.
    *   The BigQuery job logs show the successful execution of the stored procedure.

---

## 7. Rollback Procedure

In the event of a critical failure in the production GCP environment, follow these steps to roll back to the legacy on-premises system:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch for `dag_k_ausd_bp_ta_bpr_apn` to **Off**.
2.  **Re-enable Legacy Scheduling:**
    Resume the legacy scheduler (e.g., UC4, Cron) job that triggers `k_ausd_bp_ta_bpr_apn.ksh`.
3.  **Data Cleanup (Optional):**
    If the migrated job partially loaded data into BigQuery that needs to be purged to prevent duplicates during the rollback run, execute:
    ```sql
    DELETE FROM `project_id.isbert_dataset.PoolBasisprodukt`
    WHERE stichtag = PARSE_DATE('%d%m%Y', '31122023'); -- Replace with the active Stichtag
    
    DELETE FROM `project_id.isbert_dataset.job_tracking`
    WHERE job_kennung = 'AIRFLOW_TEST'; -- Replace with the active Job ID
    ```