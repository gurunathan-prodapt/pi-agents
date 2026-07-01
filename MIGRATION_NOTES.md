# Migration Notes: `k_ausd_bp_ta_bpr_evn.ksh`

This document outlines the migration details, architectural changes, deployment requirements, and validation steps for transitioning the legacy KornShell (KSH) job `k_ausd_bp_ta_bpr_evn.ksh` to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy KornShell script `k_ausd_bp_ta_bpr_evn.ksh` served as an orchestration and validation wrapper for an Oracle SQL\*Plus script (`d_ausd_bp_ta_bpr_evn.sql`). This job has been migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**.

### Migration Mapping
* **Source Orchestration**: KornShell (KSH) CLI wrapper with local file-based logging and date utilities (`gestern.ksh`).
* **Target Orchestration**: **Cloud Composer (Apache Airflow)** DAG.
* **Source Database Engine**: Oracle SQL\*Plus.
* **Target Database Engine**: **Google Cloud BigQuery** Stored Procedure.

---

## 2. Generated Artifacts

The migration process generated two primary artifacts to replace the legacy shell script:

| Artifact Path | Technology | Role / Description |
| :--- | :--- | :--- |
| `dags/k_ausd_bp_ta_bpr_evn_dag.py` | Python / Apache Airflow | Orchestrates the workflow. It parses and validates execution parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`), checks date formats, and triggers the BigQuery stored procedure. |
| `stored_procedures/r_ausd_bp_ta_bpr_evn.sql` | BigQuery SQL (BQSQL) | Encapsulates the core business logic. It performs database-level parameter validation, writes execution states to audit/error tables, executes the main transformation query, and counts processed records. |

---

## 3. Key Design Decisions

### 3.1. Dual-Layer Parameter Validation
* **Decision**: Implement parameter validation in both the Airflow DAG (Python) and the BigQuery Stored Procedure (SQL).
* **Reasoning**: 
  * **Airflow Layer**: Fails fast before allocating cloud compute resources, preventing unnecessary BigQuery query costs if mandatory parameters are missing or malformed.
  * **BigQuery Layer**: Ensures transactional integrity and guarantees that any direct manual execution of the stored procedure outside of Airflow is safely validated.

### 3.2. Transition from File-Based Logging to Relational Logging
* **Decision**: Replace local temporary files (`.tmp`) and standard output logs with dedicated BigQuery logging tables (`job_audit_log` and `job_error_log`).
* **Reasoning**: Local disk storage is ephemeral in Cloud Composer worker nodes. Centralizing execution metadata in BigQuery tables enables persistent auditing, easier debugging, and integration with dashboarding tools (e.g., Looker Studio).

### 3.3. Native Date Arithmetic
* **Decision**: Replace the legacy `gestern.ksh` utility with native BigQuery date functions (`CURRENT_DATE()` and `DATE_SUB()`).
* **Reasoning**: Eliminates external script dependencies and leverages BigQuery's highly optimized, standard-compliant date engine.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated workflow, the following infrastructure and configuration steps must be completed.

### 4.1. Schema and Logging Table Creation
Execute the following DDL statements in your target BigQuery dataset (`${BQ_DATASET}`) to establish the logging and target tables:

```sql
-- Create Job Audit Log Table
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log` (
  job_name STRING,
  job_identifier STRING,
  entry_nr STRING,
  stichtag DATE,
  status STRING,
  record_count INT64,
  created_at TIMESTAMP
);

-- Create Job Error Log Table
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.job_error_log` (
  job_name STRING,
  error_code INT64,
  error_arg STRING,
  created_at TIMESTAMP
);

-- Create Target Business Table (if not already existing)
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt` (
  business_date DATE,
  -- Add other business columns matching the legacy schema of PoolBasisprodukt
  inserted_at TIMESTAMP
);
```

### 4.2. IAM & Permissions
Ensure that the Service Account running the Cloud Composer environment (or the specific Airflow task connection) has the following IAM roles:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset `${BQ_DATASET}`.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project `${GCP_PROJECT_ID}`.

### 4.3. Airflow Configuration & Environment Variables
1. Define the following environment variables in your Cloud Composer environment:
   * `GCP_PROJECT_ID`: Your target Google Cloud Project ID.
   * `BQ_DATASET`: Your target BigQuery Dataset name.
2. Ensure the Airflow Google Cloud connection (`google_cloud_default`) is configured and active.

### 4.4. Scheduling
The DAG is currently configured with `schedule_interval=None` (manual/triggered execution). If this job must run on a schedule matching the legacy framework:
1. Update the `schedule_interval` in `dags/k_ausd_bp_ta_bpr_evn_dag.py` (e.g., to `'0 2 * * *'` for daily execution at 2:00 AM).
2. Define default parameters using Airflow's `params` or retrieve them dynamically from upstream tasks.

---

## 5. Known Gaps & Unresolved References

### 5.1. Missing Core Business Logic (Redesign Item B4)
* **Gap**: The actual SQL transformation logic originally located in `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_evn.sql` was not part of the shell wrapper script and is not yet implemented inside the stored procedure.
* **Action Required**: Locate `d_ausd_bp_ta_bpr_evn.sql` in the legacy repository, translate its Oracle SQL syntax to BigQuery Standard SQL, and insert it into the designated placeholder block inside `stored_procedures/r_ausd_bp_ta_bpr_evn.sql`.

### 5.2. Commented File-Processing Pipeline
* **Gap**: The legacy shell script contained a commented-out pipeline utilizing `sed`, `sort`, and `join` to generate a CSV file (`cibasisprodukt.csv`).
* **Action Required**: Confirm with business stakeholders if this CSV export is still required. If needed, implement a downstream Airflow task using the `BigQueryToGCSOperator` or use BigQuery's native `EXPORT DATA` SQL statement.

---

## 6. Validation

To validate the migration, execute the following test cases in your lower environments (Dev/Test).

### 6.1. Test Case 1: Parameter Validation Failure (Airflow Layer)
1. Trigger the DAG `k_ausd_bp_ta_bpr_evn_dag` manually with an empty configuration `{}`.
2. **Expected Result**: The task `validate_parameters` must fail immediately with a `ValueError` indicating missing parameters.

### 6.2. Test Case 2: Invalid Date Format
1. Trigger the DAG with the following configuration:
   ```json
   {
     "p_JobKennung": "TEST_JOB",
     "p_EintragsNr": "001",
     "p_Stichtag": "20231231"
   }
   ```
   *(Note: `20231231` is `YYYYMMDD`, but the legacy system expects `DDMMYYYY` format `31122023`)*.
2. **Expected Result**: The task `validate_parameters` must fail, stating that the date format does not match `DDMMYYYY`.

### 6.3. Test Case 3: Successful End-to-End Execution
1. Trigger the DAG with valid parameters:
   ```json
   {
     "p_JobKennung": "TEST_JOB",
     "p_EintragsNr": "001",
     "p_Stichtag": "31122023"
   }
   ```
2. **Expected Result**:
   * The DAG completes with a `SUCCESS` status.
   * A new row is added to `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log` with status `STARTED`.
   * A second row is added to `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log` with status `FINISHED` and the correct `record_count` of processed rows.
   * No new entries are created in `${GCP_PROJECT_ID}.${BQ_DATASET}.job_error_log`.

---

## 7. Rollback Procedure

If critical issues are discovered in production, execute the following steps to roll back to the legacy environment:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the switch for `k_ausd_bp_ta_bpr_evn_dag` to **Off** (paused).
2. **Re-enable Legacy Scheduling**:
   Uncomment or re-enable the cron job or scheduling framework entry that triggers the legacy shell script `k_ausd_bp_ta_bpr_evn.ksh`.
3. **Verify Downstream Consumers**:
   Ensure downstream systems are redirected to read from the legacy Oracle database table `PoolBasisprodukt` instead of the BigQuery table.
4. **Data Reconciliation (if applicable)**:
   If the migrated job ran successfully in production before failing, identify any data written to BigQuery and manually sync or backfill those records in the Oracle database to prevent data gaps.