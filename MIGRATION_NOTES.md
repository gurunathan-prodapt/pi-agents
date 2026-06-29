# Migration Notes: `k_aurd_rechstan.ksh` to Google BigQuery & Apache Airflow

This document provides the technical migration notes for transitioning the legacy KornShell orchestration script `k_aurd_rechstan.ksh` to a cloud-native architecture on Google Cloud Platform (GCP) using Google BigQuery and Google Cloud Composer (Apache Airflow).

---

## 1. Summary

The legacy KornShell script `k_aurd_rechstan.ksh` (located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh`) served as an orchestration wrapper. It performed parameter validation, date format verification, and executed the core database transformation script `d_aurd_rechstan.sql` via Oracle SQL*Plus. It also handled execution logging and job state tracking.

This orchestration and validation logic has been migrated to:
*   **Target Platform:** Google Cloud Platform (GCP)
*   **Orchestration Engine:** Google Cloud Composer (Apache Airflow)
*   **Database Engine:** Google BigQuery (GoogleSQL Stored Procedures)

The migration preserves the original business rules, parameter validation constraints, and job-tracking behaviors while eliminating legacy shell dependencies, local temporary files, and Oracle-specific utilities.

---

## 2. Generated Artifacts

The migration process has generated two primary artifacts to replace the legacy shell script:

| Artifact Path | Language / Type | Role / Description |
| :--- | :--- | :--- |
| `gcp/bigquery/stored_procedures/r_aurd_rechstan_control.sql` | GoogleSQL (BigQuery Stored Procedure) | Encapsulates all parameter validation, date parsing (`DDMMYYYY`), job logging, and execution control logic. It acts as the direct database-level replacement for the shell wrapper. |
| `dags/k_aurd_rechstan_dag.py` | Python (Apache Airflow DAG) | Orchestrates the execution of the BigQuery stored procedure. It handles parameter passing from DAG run configurations, manages execution retries, and schedules the job. |

---

## 3. Key Design Decisions

### Consolidated Stored Procedure Architecture
*   **Decision:** Combine validation, logging, and execution control into a single main stored procedure (`r_aurd_rechstan_control`) supported by modular helper procedures.
*   **Reasoning:** This keeps database-centric logic (such as checking table states, updating job logs, and validating dates) close to the data. It minimizes network round-trips between Airflow and BigQuery and ensures that validation rules are consistently applied regardless of how the procedure is invoked.

### Elimination of Local Temporary Files
*   **Decision:** Replace the legacy temporary file (`bert_k_aurd_rechstan_$$.tmp`) used for capturing record counts with a BigQuery scripting variable (`DECLARE v_records INT64`).
*   **Reasoning:** Cloud-native database engines do not rely on local file systems for inter-step communication. Using native SQL variables simplifies the execution flow and prevents file-system write bottlenecks or permission issues.

### Native Date Parsing and Validation
*   **Decision:** Replace the legacy utility script `h_alis_date.ksh` with BigQuery's native `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` function.
*   **Reasoning:** This eliminates external shell dependencies and leverages BigQuery's robust, built-in date parsing capabilities to safely handle invalid date formats without crashing the execution thread.

### Decoupled Orchestration (Airflow) and Execution (BigQuery)
*   **Decision:** Use Airflow strictly for high-level orchestration (scheduling, parameter passing, and alerting) while delegating all computational and transactional logic to BigQuery.
*   **Reasoning:** This aligns with modern ELT (Extract, Load, Transform) best practices. Airflow remains lightweight and state-free, while BigQuery handles the heavy data processing.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated artifacts in a production environment, the following setup steps must be completed:

### A. Schema and Table Creation
Ensure that the target dataset exists and create the metadata tracking tables if they do not already exist in your BigQuery environment.

```sql
-- Create the target dataset (if not exists)
-- CREATE SCHEMA IF NOT EXISTS `gcp-isbert-prod.isbert_aufbereitung` OPTIONS(location="europe-west3");

-- Create Job Control Table
CREATE TABLE IF NOT EXISTS `gcp-isbert-prod.isbert_aufbereitung.job_table` (
  table_name STRING,
  status_code STRING,
  active_flag STRING,
  stichtag_from DATE,
  stichtag_to DATE,
  job_type STRING,
  restart_flag STRING,
  record_count INT64,
  description STRING,
  job_kennung STRING,
  eintrags_nr STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- Create Job Error Log Table
CREATE TABLE IF NOT EXISTS `gcp-isbert-prod.isbert_aufbereitung.job_error_log` (
  job_name STRING,
  entry_nr STRING,
  stichtag STRING,
  error_code INT64,
  error_message STRING,
  created_at TIMESTAMP
);

-- Create Target Table Placeholder (for validation purposes)
CREATE TABLE IF NOT EXISTS `gcp-isbert-prod.isbert_aufbereitung.RKopfStan` (
  stichtag_from DATE,
  data_payload STRING
);
```

### B. IAM & Permissions
The service account used by Cloud Composer / Airflow (typically the Composer worker service account) must be granted the following IAM roles on the target GCP project:
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the dataset `isbert_aufbereitung`.
*   **BigQuery Job User** (`roles/bigquery.jobUser`) on the project `gcp-isbert-prod`.

### C. Airflow Connections & Variables
1.  **Connection:** Verify that an Airflow connection named `google_cloud_default` is configured in your Composer environment and has access to the target GCP project.
2.  **Variables:** If you wish to parameterize the project or dataset dynamically, configure Airflow variables for `gcp_project_id` and `gcp_dataset_id`.

### D. Deploying Stored Procedures
Execute the SQL script `gcp/bigquery/stored_procedures/r_aurd_rechstan_control.sql` in the BigQuery console to compile and register the stored procedures in the `isbert_aufbereitung` dataset.

---

## 5. Known Gaps & Unresolved References

### Core SQL Transformation Gap (B4 Redesign Item)
*   **Description:** The legacy shell script executed an external SQL file: `${BERT_DIR_ROOT}/aufbereitung/sql/d_aurd_rechstan.sql`. The actual business transformation logic resides inside that SQL file.
*   **Current State:** The migrated stored procedure `r_aurd_rechstan_control` currently contains a placeholder block where this transformation logic must be inserted:
    ```sql
    -- =========================================================================
    -- Core business logic placeholder:
    -- Place the BigQuery-adapted contents of d_aurd_rechstan.sql here.
    -- =========================================================================
    ```
*   **Action Required:** The contents of `d_aurd_rechstan.sql` must be translated from Oracle SQL syntax to GoogleSQL and embedded directly into the stored procedure or executed as a separate step in the Airflow DAG.

---

## 6. Validation

To validate the migration, perform the following test cases:

### Test Case 1: Parameter Validation (Failure Scenario)
*   **Action:** Trigger the Airflow DAG with missing parameters (e.g., empty `job_kennung`).
*   **Expected Result:** The DAG task completes successfully, but a row is written to `gcp-isbert-prod.isbert_aufbereitung.job_error_log` with `error_code = 1` and `error_message = 'Jobkennung fehlt'`.

### Test Case 2: Date Validation (Failure Scenario)
*   **Action:** Trigger the Airflow DAG with an invalid date format (e.g., `stichtag = "31-12-2023"` or `stichtag = "99999999"`).
*   **Expected Result:** The stored procedure raises an error: `Ungueltiges Datum im Format DDMMYYYY`. The Airflow task fails, and an entry is written to `job_error_log`.

### Test Case 3: Successful Execution
*   **Action:** Trigger the Airflow DAG with valid parameters:
    ```json
    {
      "job_kennung": "JOB_TEST_01",
      "eintrags_nr": "123456",
      "stichtag": "15102023",
      "restart_value": "0"
    }
    ```
*   **Expected Result:**
    1.  The Airflow DAG runs and completes with a `SUCCESS` status.
    2.  No new entries are written to `job_error_log`.
    3.  A new row is inserted into `job_table` with:
        *   `table_name = 'RKopfStan'`
        *   `status_code = 'A'`
        *   `active_flag = 'I'`
        *   `stichtag_from = '2023-10-15'`
        *   `job_kennung = 'JOB_TEST_01'`
        *   `eintrags_nr = '123456'`

---

## 7. Rollback Procedure

In the event of an issue during deployment or go-live, follow these steps to roll back to the legacy environment:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch for `k_aurd_rechstan_dag` to **Off** (Paused) to prevent any automated or accidental executions.
2.  **Re-enable Legacy Scheduling:**
    If the job was scheduled via an enterprise scheduler (e.g., UC4/Automic), point the scheduler task back to the legacy KornShell script path:
    `/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh`
3.  **Verify Database State:**
    If the migrated stored procedure was executed and corrupted target tables, restore the target table `RKopfStan` to its pre-execution state using BigQuery's Time Travel feature:
    ```sql
    CREATE OR REPLACE TABLE `gcp-isbert-prod.isbert_aufbereitung.RKopfStan` AS
    SELECT * FROM `gcp-isbert-prod.isbert_aufbereitung.RKopfStan`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```