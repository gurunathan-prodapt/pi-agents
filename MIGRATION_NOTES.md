# Migration Notes: `k_ausd_bp_ta_bpr_apn.ksh`

This document details the migration of the legacy KornShell (KSH) orchestration script and its associated Oracle SQL transformation logic to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` served as an orchestration wrapper. It validated input parameters, verified date formats, derived relative dates (today and yesterday), executed an Oracle SQL\*Plus transformation script (`d_ausd_bp_ta_bpr_apn.sql`) to populate the `PoolBasisprodukt` table, and logged operational metrics to temporary files and legacy tracking tables.

### Target Platform
*   **Database & Compute:** Google Cloud BigQuery (Serverless Stored Procedures)
*   **Orchestration:** Cloud Composer (Apache Airflow)

This migration completely retires the file-based Unix execution environment, Oracle SQL\*Plus client dependencies, and legacy shell helper scripts.

---

## 2. Generated Artifacts

The migration process generated the following files, organized by role:

| File Path | Role / Purpose |
| :--- | :--- |
| **`ddl/job_error_log.sql`** | Creates the error logging table in BigQuery. Replaces the legacy `f_alis_msgerr.ksh` / `DWMSG_MeldeFehler` mechanism. |
| **`ddl/job_audit_log.sql`** | Creates the operational audit logging table in BigQuery. Replaces the legacy `FOSJobErzeugeEintrag` and temporary file-based record logging. |
| **`ddl/PoolBasisprodukt.sql`** | Creates the target analytics table `PoolBasisprodukt` in BigQuery. |
| **`stored_procedures/sp_d_ausd_bp_ta_bpr_apn.sql`** | Child stored procedure containing the core transformation logic (migrated from the legacy Oracle SQL script `d_ausd_bp_ta_bpr_apn.sql`). |
| **`stored_procedures/sp_k_ausd_bp_ta_bpr_apn.sql`** | Parent stored procedure containing the orchestration, parameter validation, date derivation, and logging logic (migrated from `k_ausd_bp_ta_bpr_apn.ksh`). |
| **`dags/dag_k_ausd_bp_ta_bpr_apn.py`** | Apache Airflow DAG to schedule, parameterize, and trigger the parent stored procedure daily. |

---

## 3. Key Design Decisions

### Serverless Architecture (No VM Execution)
*   **Decision:** Instead of migrating the KSH script to a Compute Engine VM or GKE container, all orchestration, validation, and execution logic was refactored into a native **BigQuery Stored Procedure** (`sp_k_ausd_bp_ta_bpr_apn`).
*   **Reasoning:** Eliminates the maintenance overhead, security patching, and idle costs of virtual machines. BigQuery stored procedures natively support procedural logic (`DECLARE`, `IF`, `RAISE USING MESSAGE`), making them ideal for lightweight shell wrappers.

### In-Memory State & Native Logging
*   **Decision:** Replaced temporary file writes (e.g., `bert_k_ausd_bp_ta_bpr_apn.tmp`) and Unix utility parsing (`cat`, `sed`) with native BigQuery variables and structured logging tables (`job_error_log`, `job_audit_log`).
*   **Reasoning:** Eliminates disk I/O latency, simplifies debugging, and provides a centralized, queryable audit trail within BigQuery.

### Native SQL Date Functions
*   **Decision:** Replaced external helper scripts (`gestern.ksh`, `h_alis_date.ksh`) with standard BigQuery SQL functions:
    *   `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` for format validation.
    *   `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` for yesterday's date calculation.
*   **Reasoning:** Reduces external dependencies and ensures high-performance, standard-compliant date arithmetic.

### Trade-offs
*   **Procedural SQL Complexity:** Moving shell logic into BigQuery introduces procedural SQL constructs. While slightly more complex than pure declarative SQL, it keeps the orchestration layer (Airflow) clean and focused solely on scheduling.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated pipeline, the following manual setup steps must be completed:

### 1. Schema & Dataset Creation
Ensure the target BigQuery dataset exists in your GCP project.
```sql
CREATE SCHEMA IF NOT EXISTS `your_project_id.dataset_isbert`
OPTIONS(location="EU");
```

### 2. Deploy DDL and Stored Procedures
Execute the generated SQL files in the target BigQuery dataset in the following order:
1.  `ddl/job_error_log.sql`
2.  `ddl/job_audit_log.sql`
3.  `ddl/PoolBasisprodukt.sql`
4.  `stored_procedures/sp_d_ausd_bp_ta_bpr_apn.sql`
5.  `stored_procedures/sp_k_ausd_bp_ta_bpr_apn.sql`

### 3. IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles on the target BigQuery dataset:
*   **BigQuery Job User** (`roles/bigquery.jobUser`) at the project level to run query jobs.
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset to read/write tables and execute stored procedures.

### 4. Connection Strings & Environment Variables
In the Airflow DAG (`dags/dag_k_ausd_bp_ta_bpr_apn.py`), update the following global constants to match your target environment:
*   `GCP_PROJECT_ID` (e.g., `gcp-project-dw-prod`)
*   `BIGQUERY_DATASET` (e.g., `dataset_isbert`)
*   `GCP_LOCATION` (e.g., `EU` or `US`)

### 5. Secrets
No database secrets (usernames/passwords) are required. Authentication is handled entirely via GCP IAM using the Cloud Composer Service Account.

### 6. Scheduling
Upload the DAG file `dag_k_ausd_bp_ta_bpr_apn.py` to the Cloud Composer DAGs folder in Google Cloud Storage. By default, the DAG is paused upon creation. Unpause it in the Airflow UI when ready.

---

## 5. Known Gaps & Unresolved References

### 1. Transformation Logic Placeholder (B4 Redesign Item)
*   **Gap:** The child stored procedure `sp_d_ausd_bp_ta_bpr_apn.sql` currently contains a placeholder simulation that inserts a dummy record.
*   **Action Required:** The core Oracle SQL transformation logic from the legacy `d_ausd_bp_ta_bpr_apn.sql` must be migrated to BigQuery SQL syntax and pasted inside the body of `sp_d_ausd_bp_ta_bpr_apn.sql`.

### 2. Commented-out Post-processing Logic
*   **Gap:** The legacy KSH script contained commented-out code that manipulated files (`cibasis_data24.dat`, `sed`, `sort`, `join`) to construct a CSV (`cibasisprodukt.csv`). This was omitted from the active migration.
*   **Action Required:** Confirm with business stakeholders that this file generation is indeed obsolete. If it is still required, a redesign is needed to export the BigQuery table to Google Cloud Storage using the `EXPORT DATA` statement.

### 3. Hardcoded Project/Dataset References
*   **Gap:** The DDL and Stored Procedure scripts contain placeholder references (`project.dataset`).
*   **Action Required:** Replace all instances of `project.dataset` with your actual GCP Project ID and Dataset ID before deployment.

---

## 6. Validation

Validation must be performed in two phases: Unit Testing (SQL) and Integration Testing (Airflow).

### Phase 1: Unit Testing (BigQuery Console)

#### Test Case 1: Valid Execution
Run the procedure with valid parameters.
```sql
CALL `your_project_id.dataset_isbert.sp_k_ausd_bp_ta_bpr_apn`(
  'TEST_JOB_001',
  'ENTRY_100',
  '07052001',
  '0'
);
```
*   **Expected Result:** 
    *   The procedure completes successfully.
    *   A record is inserted into `PoolBasisprodukt` with `stichtag = '2001-05-07'`.
    *   A success record is written to `job_audit_log` with `records_loaded = 1`.

#### Test Case 2: Invalid Date Format
Run the procedure with an invalid date format.
```sql
CALL `your_project_id.dataset_isbert.sp_k_ausd_bp_ta_bpr_apn`(
  'TEST_JOB_001',
  'ENTRY_100',
  '2001-05-07', -- Wrong format (expected DDMMYYYY)
  '0'
);
```
*   **Expected Result:**
    *   The procedure fails with error: `FEHLER: 0 E 192 - Invalid date format for Stichtag: 2001-05-07`.
    *   An error entry is written to `job_error_log` with `error_code = 192`.

#### Test Case 3: Missing Parameters
Run the procedure with a missing parameter.
```sql
CALL `your_project_id.dataset_isbert.sp_k_ausd_bp_ta_bpr_apn`(
  '', -- Missing Jobkennung
  'ENTRY_100',
  '07052001',
  '0'
);
```
*   **Expected Result:**
    *   The procedure fails with error: `FEHLER: 0 E 193 - Jobkennung is missing`.
    *   An error entry is written to `job_error_log` with `error_code = 193`.

### Phase 2: Integration Testing (Airflow)
1.  Navigate to the Airflow UI.
2.  Locate the DAG `dw_k_ausd_bp_ta_bpr_apn`.
3.  Trigger the DAG manually with the following configuration JSON:
    ```json
    {
      "p_JobKennung": "AIRFLOW_TEST",
      "p_EintragsNr": "999",
      "p_Stichtag": "15122023",
      "p_wiederanlaufWert": "0"
    }
    ```
4.  **Expected Result:** The DAG run completes with a `success` status, and the BigQuery logs confirm execution of the stored procedure with the passed parameters.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, execute the following steps to revert to the legacy system:

1.  **Pause the Airflow DAG:**
    In the Airflow UI, toggle the switch for `dw_k_ausd_bp_ta_bpr_apn` to **Off** (Paused).
2.  **Clean Up BigQuery Artifacts (Optional):**
    If you need to remove the migrated database objects, execute:
    ```sql
    DROP PROCEDURE IF EXISTS `your_project_id.dataset_isbert.sp_k_ausd_bp_ta_bpr_apn`;
    DROP PROCEDURE IF EXISTS `your_project_id.dataset_isbert.sp_d_ausd_bp_ta_bpr_apn`;
    -- Note: Do not drop tables if they contain production data from other runs.
    ```
3.  **Re-enable Legacy Scheduler:**
    Resume the legacy scheduling tool (e.g., Control-M, Cron) that triggers the original KornShell script `k_ausd_bp_ta_bpr_apn.ksh` on the legacy execution server.
4.  **Verify Legacy Execution:**
    Monitor the legacy logs and Oracle database to ensure data processing resumes correctly.