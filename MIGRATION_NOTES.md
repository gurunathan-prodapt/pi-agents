# MIGRATION NOTES: `k_ausd_v_ta_apn_ve.ksh`

This document provides the migration notes, architectural changes, and deployment instructions for transitioning the legacy KornShell wrapper job `k_ausd_v_ta_apn_ve.ksh` to a cloud-native architecture on Google Cloud Platform (GCP).

---

## 1. Summary

The legacy KornShell script `k_ausd_v_ta_apn_ve.ksh` has been migrated from an on-premises Unix environment (utilizing an Oracle SQL*Plus client) to a modern, serverless data warehousing architecture on **Google Cloud Platform (GCP)**. 

* **Source Platform:** Unix / KornShell (KSH), Oracle SQL*Plus, local filesystem-based state tracking.
* **Target Platform:** Google Cloud Composer (Apache Airflow) and Google BigQuery (GoogleSQL Stored Procedures).
* **Core Functionality Migrated:** 
  * Command-line parameter parsing and validation (`-j` for Job Identifier, `-f` for Entry Number).
  * Operational state tracking (registering active runs, deactivating older runs).
  * Execution of core business logic targeting the `ta_apn_ve` table.
  * Record count capture and execution logging.

---

## 2. Generated Artifacts

The migration process has decomposed the legacy shell script into two primary maintainable artifacts:

| Target Relative Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dag_k_ausd_v_ta_apn_ve.py` | Python / Airflow | **Orchestrator DAG:** Handles environment-specific configuration routing, validates input parameters, and triggers the BigQuery stored procedure. |
| `stored_procedures/sp_k_ausd_v_ta_apn_ve.sql` | GoogleSQL | **Stored Procedure:** Encapsulates the core database operations, parameter validation, transactional state logging (`job_table`, `job_run_summary`, `job_error_log`), and the placeholder for the business logic. |

---

## 3. Key Design Decisions

### 3.1. Shift from Shell Orchestration to Cloud Composer (Airflow)
* **Decision:** Replace `getopts` and shell-based validation with Airflow DAG parameters (`params`) and a `BigQueryInsertJobOperator`.
* **Reasoning:** Centralizes scheduling, monitoring, and alerting. Airflow provides native integration with GCP services and eliminates the need to maintain VM-based shell execution environments.

### 3.2. Procedural Encapsulation in BigQuery
* **Decision:** Implement parameter validation, job logging, and active job deactivation inside a single BigQuery Stored Procedure (`sp_k_ausd_v_ta_apn_ve`) using GoogleSQL procedural language.
* **Reasoning:** Minimizes round-trips between the orchestrator and the database. It ensures that operational metadata updates and core data transformations happen within the same database session.

### 3.3. Transactional Integrity for Job State
* **Decision:** Wrap the job registration, core execution, and older job deactivation inside a `BEGIN TRANSACTION ... COMMIT TRANSACTION` block.
* **Reasoning:** The legacy script relied on sequential execution to prevent race conditions. In a highly parallel cloud environment, transactional boundaries prevent concurrent runs from corrupting the operational state in `job_table`.

### 3.4. Elimination of Temporary Files
* **Decision:** Replace the legacy pattern of writing record counts to a temporary file (`bert_k_ausd_v_ta_apn_ve_$$.tmp`) with BigQuery's system variable `@@row_count`.
* **Reasoning:** Eliminates local disk dependencies, simplifies the code, and leverages native database engine capabilities to track DML performance.

---

## 4. Manual Steps Before Go-Live

Before activating this pipeline in a production environment, the following setup steps must be completed:

### 4.1. Schema and Operational Table Creation
Ensure that the operational logging tables exist in your target BigQuery dataset (`dw_isbert_dev` or `dw_isbert_prod`). Although the stored procedure script contains `CREATE TABLE IF NOT EXISTS` statements, database administrators should verify schemas and partition strategies:
```sql
-- Run this in the target BigQuery dataset to verify/create tables manually if required:
CREATE TABLE IF NOT EXISTS `dw_isbert.job_table` (
  job_kennung STRING NOT NULL,
  eintrags_nr STRING NOT NULL,
  tab_name STRING,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `dw_isbert.job_run_summary` (
  job_kennung STRING,
  eintrags_nr STRING,
  tab_name STRING,
  records_processed INT64,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `dw_isbert.job_error_log` (
  job_kennung STRING,
  eintrags_nr STRING,
  err_nr INT64,
  err_arg STRING,
  created_at TIMESTAMP
);
```

### 4.2. Deploy the Stored Procedure
Execute the contents of `stored_procedures/sp_k_ausd_v_ta_apn_ve.sql` within your target BigQuery dataset to register the procedure.

### 4.3. IAM & Permissions
The Cloud Composer Service Account (e.g., `composer-env-sa@gcp-proj-dw-*.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target datasets (`dw_isbert_dev` / `dw_isbert_prod`).

### 4.4. Airflow Connection Setup
Ensure an Airflow Connection named `bigquery_default` is configured in Cloud Composer. It should use the standard Google Cloud connection type with application default credentials.

### 4.5. Scheduling & Upstream Integration
This DAG is configured with `schedule_interval=None` because the legacy script was designed to be called via a framing script (`r_ausd_vertrag.ksh`). 
* **Action:** If `r_ausd_vertrag.ksh` is also being migrated to Airflow, update its DAG to trigger `dag_k_ausd_v_ta_apn_ve` using the `TriggerDagRunOperator`, passing the required `p_JobKennung` and `p_EintragsNr` parameters.

---

## 5. Known Gaps & Unresolved References

### 5.1. Core Business Logic SQL (Redesign / B4 Item)
* **Gap:** The legacy script executed an external SQL file: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_apn_ve.sql`. The contents of this file were not provided in the source wrapper script.
* **Resolution Required:** Locate `d_ausd_v_ta_apn_ve.sql`, translate its dialect (likely Oracle SQL) to GoogleSQL, and insert it into the designated placeholder block inside `sp_k_ausd_v_ta_apn_ve.sql`:
  ```sql
  -- =========================================================================
  -- CORE BUSINESS LOGIC PLACEHOLDER
  -- (Migrated from original d_ausd_v_ta_apn_ve.sql)
  -- =========================================================================
  ```

### 5.2. Upstream Orchestrator Integration
* **Gap:** The legacy script warns: *"Bitte ueber Rahmenscript aufrufen"* (Please call via frame script).
* **Resolution Required:** The migration of the parent frame script (`r_ausd_vertrag.ksh`) must be coordinated to ensure parameters are passed dynamically to this Airflow DAG.

---

## 6. Validation

To validate the migration, execute the pipeline in the development environment using the following test cases:

### 6.1. Test Case 1: Happy Path Execution
1. Trigger the DAG `dag_k_ausd_v_ta_apn_ve` manually via the Airflow UI.
2. Provide the following configuration JSON:
   ```json
   {
     "env": "dev",
     "p_JobKennung": "JOB_TEST_001",
     "p_EintragsNr": "REC_999"
   }
   ```
3. **Expected Result:**
   * The DAG completes with a `SUCCESS` status.
   * `dw_isbert_dev.job_table` contains a record with `status = 'ACTIVE'` for `job_kennung = 'JOB_TEST_001'`.
   * `dw_isbert_dev.job_run_summary` contains a record capturing the processed row count.

### 6.2. Test Case 2: Parameter Validation Failure
1. Trigger the DAG manually with missing parameters:
   ```json
   {
     "env": "dev",
     "p_JobKennung": "",
     "p_EintragsNr": ""
   }
   ```
2. **Expected Result:**
   * The Airflow task fails during the pre-execution validation check, or the stored procedure returns an error.
   * `dw_isbert_dev.job_error_log` contains an entry with `err_nr = 193` and `err_arg = 'Jobkennung'`.

### 6.3. Test Case 3: Active Job Deactivation
1. Trigger a run with `p_JobKennung = 'JOB_CONCURRENT'` and `p_EintragsNr = 'RUN_A'`. Verify it is marked `ACTIVE` in `job_table`.
2. Trigger a second run with `p_JobKennung = 'JOB_CONCURRENT'` and `p_EintragsNr = 'RUN_B'`.
3. **Expected Result:**
   * `RUN_B` completes successfully.
   * In `job_table`, the record for `RUN_A` is updated to `status = 'INACTIVE'`.
   * The record for `RUN_B` is set to `status = 'ACTIVE'`.

---

## 7. Rollback Procedure

If issues are encountered during deployment or go-live, follow these steps to roll back the changes:

1. **Pause the Airflow DAG:**
   * Navigate to the Cloud Composer UI and toggle the DAG `dag_k_ausd_v_ta_apn_ve` to **Off** (Paused).
2. **Revert Database Changes (Optional):**
   * If the stored procedure needs to be removed, execute:
     ```sql
     DROP PROCEDURE IF EXISTS `dw_isbert_prod.sp_k_ausd_v_ta_apn_ve`;
     ```
3. **Re-enable Legacy Execution:**
   * If the legacy on-premises environment is still active, point the upstream scheduler (e.g., UC4/Automic or cron) back to the legacy KornShell wrapper script `k_ausd_v_ta_apn_ve.ksh`.
4. **Audit Operational State:**
   * Check `job_table` to ensure no orphaned `ACTIVE` locks remain from failed cloud runs that might conflict with legacy execution. Clean up if necessary:
     ```sql
     UPDATE `dw_isbert_prod.job_table` 
     SET status = 'FAILED', updated_at = CURRENT_TIMESTAMP() 
     WHERE status = 'ACTIVE' AND job_kennung = 'YOUR_JOB_ID';
     ```