# Migration Notes: CRM_CUSTOMER_LOAD

## 1. Summary
The `CRM_CUSTOMER_LOAD` pipeline has been migrated from a legacy on-premise environment to **Google Cloud Platform (GCP)**. 

* **Source Components:** KornShell (`.ksh`) orchestration scripts, Oracle SQL*Plus extracts, Oracle PL/SQL stored procedures, and local file-based event polling.
* **Target Components:** Python 3 orchestration scripts, Google BigQuery (for staging, data warehousing, and stored procedures), Google Cloud Storage (GCS) or local persistent volumes for event markers, and Google Cloud Logging.

This weekly batch job orchestrates the extraction, validation, historization, and scoring of CRM customer profiles. It coordinates upstream dependencies (Finance and Retail pipelines) before executing database transformations and downstream Python-based machine learning scoring.

---

## 2. Generated Artifacts
The migration process generated two primary Python 3 artifacts to replace the legacy shell scripts:

### 1. `customer/process_customer_data.py`
* **Role:** Main Orchestrator.
* **Replaces:** `customer/process_customer_data.ksh`
* **Description:** Handles command-line arguments (`RUN_DATE`, `CUSTOMER_SEGMENT`, `FORCE_RELOAD`), manages environment variables, coordinates upstream event-waiting, executes the transpiled BigQuery segment extract query, validates staging table counts, calls the BigQuery stored procedure for customer historization, and triggers the downstream customer scoring script.

### 2. `lib/retry_handler.py`
* **Role:** Shared ETL Utility Module.
* **Replaces:** `lib/retry_handler.ksh`
* **Description:** Provides reusable utility functions imported by the main orchestrator:
  * `retry_command()`: Executes shell commands with exponential backoff.
  * `wait_for_event()`: Polls for completion markers (files or UC4 API).
  * `check_prereq_job()`: Queries the BigQuery audit table to verify upstream job completion.
  * `log_job_audit()`: Performs an upsert (`MERGE`) into the BigQuery `etl_job_audit` table.

---

## 3. Key Design Decisions

### Python 3 over Bash/Shell Wrappers
The legacy pipeline contained complex control flow, database interactions, and external script invocations. Migrating to Python 3 provides robust error handling, native GCP client integration, and cleaner modularity compared to shell wrappers.

### BigQuery Client API Integration
Legacy Oracle SQL*Plus (`sqlplus`) invocations have been replaced with the native Google Cloud BigQuery Client API (`google-cloud-bigquery`). This eliminates SQL*Plus formatting hacks (e.g., `SET HEADING OFF FEEDBACK OFF`) and allows parameterized queries to prevent SQL injection.

### Sequence Replacement via UUIDs
BigQuery does not support native transactional sequences (like Oracle's `ETL_AUDIT_SEQ.NEXTVAL`). In `lib/retry_handler.py`, the unique audit identifier is generated programmatically using Python's `uuid.uuid4()`, which guarantees uniqueness without database-side sequence overhead.

### Event Polling & File Markers
The legacy script relies on file-based event markers (e.g., `/opt/etl/events/FINANCE_GL_CLOSE_COMPLETE_20240115.done`). 
* **Decision:** The Python code preserves this logic using `pathlib.Path`. In GCP, the `ETL_EVENTS_DIR` environment variable can point to a local directory, a shared persistent volume, or a GCS bucket mounted via Cloud Storage FUSE.
* **Trade-off:** While file-based polling is preserved for backward compatibility, the recommended target state is to transition to native Airflow (Cloud Composer) Sensors or Pub/Sub triggers.

---

## 4. Manual Steps Before Go-Live

### 1. Schema and Dataset Creation
Ensure the target BigQuery dataset and tables are created in your GCP project:
* **Dataset:** Create the dataset specified in your `BQ_DATASET` environment variable (e.g., `crm_prod`).
* **Staging Table:** Create `STG_CUSTOMER_PROFILE` with a schema matching the legacy Oracle table. Ensure `LOAD_DATE` is of type `DATE` and `ETL_STATUS` is of type `STRING`.
* **Audit Table:** Create the `etl_job_audit` table with the following schema:
  ```sql
  CREATE TABLE my_project.crm_prod.etl_job_audit (
      AUDIT_ID STRING,
      JOB_NAME STRING,
      RUN_DATE DATE,
      JOB_STATUS STRING,
      ROWS_PROCESSED INT64,
      AUDIT_TIMESTAMP TIMESTAMP,
      HOST_NAME STRING
  );
  ```

### 2. Deploy Stored Procedures
Transpile and deploy the legacy PL/SQL package `PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD` as a BigQuery stored procedure:
```sql
CREATE OR REPLACE PROCEDURE `my_project.crm_prod.MASTER_CRM_LOAD`(p_run_date DATE, p_segment STRING)
BEGIN
  -- Transpiled historization logic goes here
END;
```

### 3. IAM & Permissions
The Service Account executing the Python scripts must have the following IAM roles:
* **BigQuery:** `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the target project/dataset.
* **Storage (if using GCS for event markers):** `roles/storage.objectAdmin` on the bucket hosting the `.done` files.

### 4. Connection Strings & Secrets
The legacy `env_crm.properties` file containing Oracle credentials (`DB_USER`, `DB_PASS`, etc.) is no longer required for database access. Instead, configure the following environment variables in your execution environment (e.g., Cloud Composer, GKE, or VM):
* `GCP_PROJECT`: Your target GCP Project ID.
* `BQ_DATASET`: Your target BigQuery dataset name.
* `SQLPLUS_DIR`: Directory containing the transpiled `customer_segment_extract.sql` file.
* `PYTHON_DIR`: Directory containing the `customer_scoring.py` script.
* `LOG_DIR`: Directory where execution logs will be written.
* `ETL_EVENTS_DIR`: Directory where upstream `.done` marker files are written.

### 5. Scheduling
If using **Cloud Composer (Airflow)**, wrap the execution of `process_customer_data.py` in a `BashOperator` or convert it to a `PythonOperator`. 
* **Schedule:** Weekly.
* **Arguments:** `process_customer_data.py {{ ds }} ALL N`

---

## 5. Known Gaps & Unresolved References

### 1. Transpilation of `customer_segment_extract.sql` (B4 Redesign Item)
* **Gap:** The SQL script `customer_segment_extract.sql` was not provided in the migration scope.
* **Action Required:** A database engineer must manually transpile this file from Oracle SQL to BigQuery SQL dialect and place it in the directory defined by `SQLPLUS_DIR`. Ensure that any legacy Oracle-specific functions (e.g., `SYSDATE`, `NVL`, `DECODE`) are converted to BigQuery equivalents (`CURRENT_TIMESTAMP()`, `COALESCE`, `CASE WHEN`).

### 2. Downstream `customer_scoring.py`
* **Gap:** The Python scoring script was not provided in this migration scope.
* **Action Required:** Ensure that `customer_scoring.py` is migrated to Python 3, compatible with BigQuery, and deployed to the directory defined by `PYTHON_DIR`.

### 3. Legacy `uc4api` Dependency
* **Gap:** The utility library contains a fallback check for `uc4api` to query the legacy UC4 scheduler.
* **Action Required:** If the pipeline is fully migrated to Cloud Composer, this fallback is obsolete. It is recommended to remove the `uc4api` check in a future refactoring phase and rely entirely on Airflow DAG dependencies.

---

## 6. Validation

### How to Run the Tests

#### 1. Local/Dry-Run Validation
You can run the orchestrator locally or in a development container by mocking the BigQuery environment. If the `etl_lib` package is not installed, the script falls back to mock functions for event waiting and auditing.
```bash
export GCP_PROJECT="my-dev-project"
export BQ_DATASET="crm_dev"
export SQLPLUS_DIR="./customer"
export PYTHON_DIR="./customer"
export LOG_DIR="./logs"
export ETL_EVENTS_DIR="./events"

# Create dummy SQL file and scoring script for testing
echo "SELECT 1;" > ./customer/customer_segment_extract.sql
echo "print('Scoring complete')" > ./customer/customer_scoring.py

# Create mock upstream event markers
mkdir -p ./events
touch ./events/FINANCE_GL_CLOSE_COMPLETE_2024-01-15.done
touch ./events/RETAIL_DAILY_COMPLETE_2024-01-15.done

# Run the script
python3 customer/process_customer_data.py 2024-01-15 ALL Y
```

#### 2. Integration Validation
Run the pipeline against the development BigQuery environment:
```bash
python3 customer/process_customer_data.py <RUN_DATE> <SEGMENT> <FORCE_RELOAD>
```

### What "Passing" Means
The migration is validated as successful when:
1. The script exits with status code `0`.
2. The log file `crm_load_<SEGMENT>_<YYYYMMDD>_<HHMMSS>.log` is created and contains no `ERROR` entries.
3. The query count on `STG_CUSTOMER_PROFILE` executes successfully.
4. The BigQuery stored procedure `MASTER_CRM_LOAD` completes execution.
5. A record is successfully merged into the `etl_job_audit` table in BigQuery with `JOB_STATUS = 'SUCCESS'`.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, follow these steps to roll back to the legacy on-premise pipeline:

### Step 1: Revert Scheduler / Orchestration
* Point the scheduler (UC4 or Airflow) back to the legacy KornShell script:
  ```bash
  # Legacy execution command
  process_customer_data.ksh <RUN_DATE> <SEGMENT> [FORCE_RELOAD]
  ```

### Step 2: Database State Rollback
If the migrated pipeline partially processed data or corrupted the target tables:
1. **Identify the Run Date:** Determine the `<RUN_DATE>` of the failed execution.
2. **Clean Staging Data:** Purge any staging data loaded during the failed run:
   ```sql
   DELETE FROM `my_project.crm_prod.STG_CUSTOMER_PROFILE`
   WHERE LOAD_DATE = PARSE_DATE('%Y-%m-%d', 'FAILED_RUN_DATE');
   ```
3. **Revert Historization:** If the stored procedure modified production tables, execute your database-specific restore scripts or restore the BigQuery dataset to a snapshot taken prior to the job run using BigQuery Time Travel:
   ```sql
   -- Example: Restore table to 1 hour ago
   FOR SYSTEM TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
   ```

### Step 3: Audit Log Correction
Update the audit log to reflect the failure or remove the record to allow the legacy system to re-run:
```sql
UPDATE `my_project.crm_prod.etl_job_audit`
SET JOB_STATUS = 'FAILED', AUDIT_TIMESTAMP = CURRENT_TIMESTAMP()
WHERE JOB_NAME = 'CRM_CUSTOMER_LOAD' AND RUN_DATE = 'FAILED_RUN_DATE';
```