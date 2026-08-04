# Migration Notes: CUSTOMER.HISTORIZATION_LOAD

This document details the migration of the legacy UC4 UNIX job `CUSTOMER.HISTORIZATION_LOAD` to Google Cloud Platform (GCP) utilizing Cloud Composer (Apache Airflow) and Google Cloud BigQuery.

---

## 1. Summary
The `CUSTOMER.HISTORIZATION_LOAD` job has been migrated from a legacy UC4 UNIX environment to a cloud-native orchestration and data warehousing platform. 

* **Source Platform:** UC4/Automic Engine executing KornShell (`.ksh`) scripts on a remote UNIX host (`|ETLHOST2|HOST`) using Oracle SQL*Plus.
* **Target Platform:** Google Cloud Platform (GCP)
  * **Orchestration:** Cloud Composer (Apache Airflow 2.x)
  * **Database Engine:** Google Cloud BigQuery (Standard SQL)
* **Business Purpose:** Executes the weekly Slowly Changing Dimension Type 2 (SCD2) historization process, merging weekly customer segment and score data from staging into the customer segment dimension table. It includes a critical post-load quality check to flag anomalous segment shifts (e.g., due to bad join keys).

---

## 2. Generated Artifacts
The migration process generated five core files, each mapping to a specific component of the legacy architecture:

| Generated File Path | Language / Type | Legacy Component | Role / Description |
| :--- | :--- | :--- | :--- |
| `customer/CUSTOMER_HISTORIZATION_LOAD_dag.py` | Python (Airflow DAG) | `CUSTOMER.HISTORIZATION_LOAD` (UC4 XML) | Orchestrates the execution of the wrapper script. Configured as an on-demand (non-scheduled) DAG. |
| `customer/r_historization_load.py` | Python 3 | `r_historization_load.ksh` | Wrapper script that sets up the execution environment, logs execution milestones, and executes the core controller. |
| `customer/k_historization_load.py` | Python 3 | `k_historization_load.ksh` | Core controller script. Uses the `google-cloud-bigquery` client library to execute the load and quality check SQL scripts sequentially, parses the quality check output, and evaluates thresholds. |
| `customer/d_historization_load.sql` | BigQuery Standard SQL | `d_historization_load.sql` | Procedural SQL script executing the SCD Type 2 merge and insert statements inside an atomic transaction block. |
| `customer/d_segment_quality_check.sql` | BigQuery Standard SQL | `d_segment_quality_check.sql` | Parameterized query calculating the percentage of customer records re-versioned on the given run date. |

---

## 3. Key Design Decisions

### 3.1 Shell-to-Python Modernization
* **Decision:** Migrated the legacy KornShell scripts (`r_historization_load.ksh` and `k_historization_load.ksh`) to native Python 3 scripts (`r_historization_load.py` and `k_historization_load.py`).
* **Reasoning:** Eliminates the dependency on legacy UNIX execution hosts and SSH keys. Python scripts run natively within GKE-managed Cloud Composer workers, leveraging Google Cloud IAM service accounts for secure, passwordless authentication to BigQuery.

### 3.2 BigQuery Scripting and Transaction Control
* **Decision:** Wrapped the SCD Type 2 logic inside a BigQuery `BEGIN TRANSACTION ... COMMIT TRANSACTION` block with an `EXCEPTION WHEN ERROR` rollback handler.
* **Reasoning:** The SCD2 process is a two-step operation: expiring existing active records and inserting new active versions. Executing these steps atomically prevents data corruption or partial updates in the event of a mid-query failure.

### 3.3 Single-Evaluation Execution Timestamp
* **Decision:** Declared a script-level local variable `v_current_timestamp` initialized to `CURRENT_TIMESTAMP()` at the start of the BigQuery scripting block.
* **Reasoning:** Using inline `CURRENT_TIMESTAMP()` across separate statements can cause sub-second timestamp drift. Capturing the timestamp once guarantees that the expiration timestamp (`VALID_TO` of the old record) and the start timestamp (`VALID_FROM` of the new record) match exactly, preserving referential integrity.

### 3.4 Parameterization and SQL*Plus Variable Replacement
* **Decision:** Replaced SQL*Plus positional parameters (`&1`) with BigQuery query parameters (`@run_date_param` and `@run_date`).
* **Reasoning:** Protects against SQL injection, avoids brittle string substitution, and aligns with modern database development standards.

### 3.5 On-Demand Scheduling
* **Decision:** Configured the Airflow DAG with `schedule=None`.
* **Reasoning:** The legacy UC4 job did not have an independent cron schedule; it was executed as an included module within larger weekly workflows. Setting `schedule=None` keeps the DAG as a callable unit that can be triggered via `TriggerDagRunOperator` or manual invocation.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target environment before deploying and running the migrated pipeline:

### 4.1 Schema and Dataset Creation
Ensure the target BigQuery dataset and tables exist. If they do not, execute the following DDL statements in BigQuery:

```sql
-- Create Dataset (if not exists)
CREATE SCHEMA IF NOT EXISTS `your_gcp_project.ANALYTICS_SCHEMA`;

-- Create Target Dimension Table
CREATE TABLE IF NOT EXISTS `your_gcp_project.ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` (
    CUSTOMER_ID STRING NOT NULL,
    SEGMENT_CODE STRING,
    SCORE_BAND STRING,
    SCORE_VALUE NUMERIC,
    IS_CURRENT INT64,
    VALID_FROM TIMESTAMP,
    VALID_TO TIMESTAMP
);

-- Create Staging Table
CREATE TABLE IF NOT EXISTS `your_gcp_project.ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` (
    CUSTOMER_ID STRING NOT NULL,
    SEGMENT_CODE STRING,
    SCORE_BAND STRING,
    SCORE_VALUE NUMERIC,
    RUN_DATE DATE
);
```

### 4.2 IAM and Permissions
The Cloud Composer worker service account (e.g., `sa-composer@your-project.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **BigQuery Data Editor** on the target dataset (`ANALYTICS_SCHEMA`).
* **BigQuery Job User** on the GCP project.

### 4.3 Airflow Variables Configuration
Add the following Airflow Variables via the Airflow UI (**Admin -> Variables**) or CLI:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-gcp-project-123` | The target Google Cloud Project ID. |
| `GCP_REGION` | `us-central1` | The target GCP region. |
| `CRM_HOME` | `/home/airflow/gcs/dags` | The root directory where the Python and SQL scripts reside. |
| `BQ_DATASET` | `ANALYTICS_SCHEMA` | The target BigQuery dataset name. |

### 4.4 Code Deployment
Copy the generated files to your Cloud Composer GCS bucket:
* Upload `CUSTOMER_HISTORIZATION_LOAD_dag.py` to `gs://<composer-bucket>/dags/`.
* Upload `r_historization_load.py`, `k_historization_load.py`, `d_historization_load.sql`, and `d_segment_quality_check.sql` to `gs://<composer-bucket>/dags/customer/`.

---

## 5. Known Gaps & Unresolved References

### 5.1 Downstream Job Integration
* **Gap:** The downstream job `CUSTOMER.WEEKLY_SCHEDULE` has not yet been migrated to GCP.
* **Resolution:** The final task hand-off is currently omitted. Once `CUSTOMER.WEEKLY_SCHEDULE` is migrated, configure a `TriggerDagRunOperator` at the end of `CUSTOMER_HISTORIZATION_LOAD_dag.py` or establish an `ExternalTaskSensor` in the downstream DAG.

### 5.2 Redesign (B4) Opportunity: Native Airflow Operators
* **Gap:** The current Airflow DAG uses a `BashOperator` to execute the Python wrapper script `r_historization_load.py`, which in turn spawns `k_historization_load.py` via a subprocess. This was done to preserve the exact legacy logging and execution structure.
* **Redesign Recommendation:** Refactor the DAG to bypass the shell/subprocess layer entirely. The SQL scripts should be executed directly using the `BigQueryInsertJobOperator`. The quality check result can be fetched using a `BigQueryValueCheckOperator` or passed via XComs to a `BranchPythonOperator` to evaluate the threshold.

---

## 6. Validation

### 6.1 How to Run the Tests
1. **Seed Test Data:** Populate the staging table with mock records for a specific run date:
   ```sql
   INSERT INTO `your_gcp_project.ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
   VALUES ('C100', 'GOLD', 'A', 95.5, '2023-10-31');
   ```
2. **Trigger the DAG:** In the Airflow UI, navigate to `CUSTOMER_HISTORIZATION_LOAD_dag` and click **Trigger DAG w/ Config**. Pass the execution date in the JSON configuration:
   ```json
   {
     "RUN_DATE": "2023-10-31"
   }
   ```
3. **Monitor Logs:** Inspect the task logs for `run_historization_load_wrapper`.

### 6.2 What "Passing" Looks Like
The run is successful if:
* The Airflow task completes with a `SUCCESS` status.
* The task logs output the following exact sequence of messages:
  ```text
  [YYYY-MM-DD HH:MM:SS] Starting SCD2 historization for run date 2023-10-31
  [YYYY-MM-DD HH:MM:SS] Running SCD2 merge for customer segment dimension
  [YYYY-MM-DD HH:MM:SS] Historization merge complete, 0% of customers re-versioned
  [YYYY-MM-DD HH:MM:SS] Historization load completed for 2023-10-31
  ```
* **Database Verification:** Query the target table to verify the record was inserted:
  ```sql
  SELECT * FROM `your_gcp_project.ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` WHERE CUSTOMER_ID = 'C100';
  ```
  Verify that `IS_CURRENT = 1`, `VALID_FROM` is populated with the execution timestamp, and `VALID_TO` is `NULL`.

### 6.3 Quality Check Threshold Validation
To test the threshold warning logic:
1. Insert a record into `DIM_CUSTOMER_SEGMENT` with `IS_CURRENT = 1`.
2. Insert a record into `STG_CUSTOMER_SCORE_OUTPUT` for the same customer but with a different `SEGMENT_CODE` and a new `RUN_DATE`.
3. Trigger the DAG for the new `RUN_DATE`.
4. Since 100% of the customer base changed segments (exceeding the 25% threshold), verify the logs output the warning:
   ```text
   [YYYY-MM-DD HH:MM:SS] WARN: 100% of customers changed segment this week (expected <= 25%) - flagging for review, not failing the job
   ```
   *Note: The job must still exit with code `0` (Success) despite the warning, matching legacy behavior.*

---

## 7. Rollback Procedure

If the historization load corrupts data or runs with incorrect parameters, perform the following steps to roll back the target table to its pre-run state:

### 7.1 Identify the Execution Timestamp
Find the exact `VALID_FROM` timestamp of the corrupted run by querying the target table:
```sql
SELECT DISTINCT VALID_FROM 
FROM `your_gcp_project.ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` 
ORDER BY VALID_FROM DESC;
```

### 7.2 Execute Rollback SQL
Run the following transactional SQL block in BigQuery (replace `TIMESTAMP('2023-10-31 10:00:00 UTC')` with the timestamp identified in step 7.1):

```sql
BEGIN TRANSACTION;

-- 1. Delete the new active records inserted during the failed run
DELETE FROM `your_gcp_project.ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
WHERE VALID_FROM = TIMESTAMP('2023-10-31 10:00:00 UTC');

-- 2. Re-activate the previous records that were expired during the failed run
UPDATE `your_gcp_project.ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
SET IS_CURRENT = 1,
    VALID_TO = NULL
WHERE VALID_TO = TIMESTAMP('2023-10-31 10:00:00 UTC');

COMMIT TRANSACTION;
```

### 7.3 Airflow Rollback
If the DAG needs to be disabled, pause the DAG in the Airflow UI to prevent any automated upstream triggers from executing the pipeline.