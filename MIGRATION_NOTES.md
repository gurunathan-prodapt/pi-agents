# MIGRATION_NOTES.md

This document provides comprehensive migration notes for transitioning the KornShell control script `k_ausd_bp_ta_bpr_apn.ksh` and its associated SQL execution logic to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy KornShell control script `k_ausd_bp_ta_bpr_apn.ksh` has been migrated from an on-premise Unix environment to a cloud-native architecture on **Google Cloud Platform (GCP)**. 

* **Source Component:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` (and its downstream SQL dependency `d_ausd_bp_ta_bpr_apn.sql`).
* **Target Platform:** **Google Cloud BigQuery** (for data processing, validation, and metadata logging) and **Cloud Composer / Apache Airflow** (for orchestration and scheduling).
* **Core Functionality Migrated:** Parameter parsing and validation, date validation, execution of data extraction/transformation logic, record count capture, and job execution logging.

---

## 2. Generated Artifacts

The migration process has generated three primary artifacts to replace the legacy shell script and its dependencies:

| File Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `gcp_target/stored_procedures/r_ausd_bp_ta_bpr_apn.sql` | BigQuery SQL (Stored Procedure) | **Orchestration Wrapper:** Handles parameter validation, date parsing, execution of the inner business logic, record counting, and writing execution metadata to the control table. |
| `gcp_target/stored_procedures/d_ausd_bp_ta_bpr_apn.sql` | BigQuery SQL (Stored Procedure) | **Business Logic Container:** Houses the core data transformation and loading logic. This isolates the actual SQL queries from the operational wrapper. |
| `gcp_target/dags/dag_r_ausd_bp_ta_bpr_apn.py` | Python (Apache Airflow DAG) | **Orchestration Workflow:** Schedules and triggers the wrapper stored procedure, passing runtime parameters dynamically via Airflow's execution context. |

---

## 3. Key Design Decisions

### 3.1. Two-Tier Stored Procedure Architecture
* **Decision:** Split the migration into a wrapper procedure (`r_...`) and an inner business logic procedure (`d_...`).
* **Reasoning:** This mirrors the legacy structure where `k_ausd_bp_ta_bpr_apn.ksh` acted as the control wrapper for `d_ausd_bp_ta_bpr_apn.sql`. It keeps operational logic (validation, logging, error handling) decoupled from data transformation logic, simplifying future maintenance.

### 3.2. Elimination of Local Filesystem State
* **Decision:** Replaced the temporary file `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp` (used to store record counts) with a direct `COUNT(*)` query on the target table, logged directly to a BigQuery metadata table (`job_control_table`).
* **Reasoning:** Cloud-native execution environments should be stateless. Eliminating local disk writes prevents file-locking issues, disk space exhaustion, and permission errors on ephemeral cloud runners.

### 3.3. Native SQL Date Arithmetic
* **Decision:** Replaced the external helper script `gestern.ksh` with BigQuery's native `CURRENT_DATE()` and `DATE_SUB()` functions.
* **Reasoning:** Reduces external dependencies and improves execution performance by keeping date calculations within the BigQuery engine.

### 3.4. Declarative Error Handling
* **Decision:** Replaced legacy shell-sourced validation helpers (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) with BigQuery's native `RAISE USING MESSAGE` statements.
* **Reasoning:** Standardizes error propagation, allowing Airflow to immediately detect failures and trigger alerts using native GCP monitoring.

---

## 4. Manual Steps Before Go-Live

Before deploying and executing the migrated workflow, the following setup steps must be completed in the target GCP environment:

### 4.1. Schema and Table Creation
Ensure the target tables and control tables exist in your BigQuery dataset. If they do not exist, execute the following DDL:

```sql
-- 1. Target Data Table (Example structure; align with actual business schema)
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${GCP_DATASET}.poolbasisprodukt` (
  job_kennung STRING,
  eintrags_nr STRING,
  stichtag DATE,
  restart_value STRING,
  created_at TIMESTAMP
);

-- 2. Job Control Table (Replaces legacy FOS job logging)
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${GCP_DATASET}.job_control_table` (
  tab_name STRING,
  job_kennung STRING,
  eintrags_nr STRING,
  stichtag DATE,
  restart_value STRING,
  record_count INT64,
  status_code STRING,
  process_type STRING,
  active_flag STRING,
  description STRING,
  created_at TIMESTAMP
);
```

### 4.2. IAM and Permissions
The service account executing the Airflow DAG and BigQuery jobs must have the following IAM roles:
* **BigQuery Data Editor** (on the target dataset)
* **BigQuery Job User** (on the GCP project)

### 4.3. Airflow Connection Configuration
* Ensure that the Airflow connection `bigquery_default` is configured and points to the correct GCP Project ID and service account key (if running outside of GKE/Composer workload identity).

### 4.4. Scheduling and Variables
* The DAG is configured to run daily at `06:00 UTC` (`0 6 * * *`). Adjust this schedule in `dag_r_ausd_bp_ta_bpr_apn.py` if upstream dependencies require a different execution window.

---

## 5. Known Gaps & Unresolved References

### 5.1. Inner SQL Business Logic Migration (Placeholder)
* **Gap:** The inner business logic procedure `d_ausd_bp_ta_bpr_apn.sql` currently contains a simplified staging-to-target insert template.
* **Action Required:** The original Oracle SQL within `d_ausd_bp_ta_bpr_apn.sql` must be translated to BigQuery standard SQL. Watch out for:
  * Oracle-specific outer joins (`(+)`) $\rightarrow$ convert to standard `LEFT/RIGHT OUTER JOIN`.
  * Oracle functions like `NVL` $\rightarrow$ convert to `COALESCE`.
  * Oracle date formatting (`TO_DATE`, `TO_CHAR`) $\rightarrow$ convert to `PARSE_DATE`, `FORMAT_DATE`.

### 5.2. Legacy Commented-Out File Processing (B4 Redesign)
* **Gap:** The legacy shell script contained commented-out post-processing logic involving `sed`, `sort`, and `join`.
* **Action Required:** If this logic is ever reactivated, **do not** implement it as file-based operations. Instead, implement these transformations directly in BigQuery using SQL constructs such as:
  * `REGEXP_REPLACE` (for `sed` operations).
  * `QUALIFY ROW_NUMBER() OVER (...)` (for deduplication/sorting).
  * Standard SQL `INNER/FULL OUTER JOIN` (for `join` operations).

---

## 6. Validation

To validate the migration, perform both unit and integration testing.

### 6.1. Unit Testing (BigQuery Console)
Run the stored procedure directly in the BigQuery console with test parameters:

```sql
DECLARE test_job_kennung STRING DEFAULT 'JOB_TEST_01';
DECLARE test_eintrags_nr STRING DEFAULT '1001';
DECLARE test_stichtag STRING DEFAULT '31122023'; -- DDMMYYYY format
DECLARE test_restart STRING DEFAULT '0';

CALL `${GCP_PROJECT_ID}.${GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
  test_job_kennung,
  test_eintrags_nr,
  test_stichtag,
  test_restart
);
```

#### Expected "Passing" Result:
1. The query execution completes successfully.
2. A row is inserted into `${GCP_PROJECT_ID}.${GCP_DATASET}.poolbasisprodukt` with `stichtag = '2023-12-31'`.
3. A row is inserted into `${GCP_PROJECT_ID}.${GCP_DATASET}.job_control_table` containing the correct record count, status `'A'`, and description `'Initialbefuellung'`.
4. The console output displays: `---------- ENDE Datenverarbeitung ----------`.

### 6.2. Integration Testing (Airflow)
1. Upload `dag_r_ausd_bp_ta_bpr_apn.py` to your Airflow DAGs folder.
2. Trigger the DAG manually with the following configuration JSON:
   ```json
   {
     "p_JobKennung": "JOB_AIRFLOW_TEST",
     "p_EintragsNr": "2002",
     "p_Stichtag": "15112023",
     "p_wiederanlaufWert": "0"
   }
   ```
3. Verify that the DAG task `run_r_ausd_bp_ta_bpr_apn` completes with a `success` status.

---

## 7. Rollback Procedure

If critical issues are discovered in production, follow these steps to roll back to the legacy on-premise execution path:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `dag_r_ausd_bp_ta_bpr_apn` to **Off**.
2. **Re-enable Legacy Scheduling:**
   Uncomment or re-enable the cron job / scheduler trigger for the legacy shell script `k_ausd_bp_ta_bpr_apn.ksh` on the on-premise server.
3. **Verify Legacy Execution:**
   Trigger a manual run of the legacy script to ensure it connects to the legacy database and processes data correctly:
   ```bash
   ./k_ausd_bp_ta_bpr_apn.ksh -j <JOB_ID> -f <ENTRY_NO> -s <DDMMYYYY> -l 0
   ```
4. **Data Reconciliation:**
   If the cloud process partially ran, check the target tables in BigQuery and the legacy database to identify and resolve any data gaps or double-processing issues.