# Migration Notes: k_ausd_bp_ta_bcp_iccid.ksh Migration to BigQuery

This document details the migration of the legacy KornShell (Ksh) orchestration script `k_ausd_bp_ta_bcp_iccid.ksh` and its associated environment to Google Cloud Platform (GCP), utilizing BigQuery and Cloud Composer (Apache Airflow).

---

## 1. Summary

The legacy KornShell script `k_ausd_bp_ta_bcp_iccid.ksh` served as an orchestration and validation wrapper. It validated input parameters, managed operational dates, executed a downstream Oracle SQL*Plus script (`d_ausd_bp_ta_bcp_iccid.sql`), and logged execution metrics.

This job has been migrated to **Google Cloud Platform (GCP)**. The orchestration, parameter validation, and logging logic have been converted into **BigQuery Stored Procedures** (using GoogleSQL scripting), while the scheduling and execution trigger have been migrated to **Cloud Composer (Apache Airflow)**.

---

## 2. Generated Artifacts

The migration process generated the following target files, each serving a specific role in the new architecture:

| Target File Path | Language | Role |
| :--- | :--- | :--- |
| `gcp_migration/bigquery/ddl/job_tracking_tables.sql` | GoogleSQL (DDL) | Creates the metadata tracking tables (`job_tracking` and `job_error_log`) in the `isbert_metadata` dataset. These tables replace the legacy FOS job tracking and local temporary log files. |
| `gcp_migration/bigquery/procedures/reusable_job_logging.sql` | GoogleSQL (DML/DDL) | Defines reusable helper procedures and functions for parameter validation, date parsing, and execution logging. |
| `gcp_migration/bigquery/procedures/k_ausd_bp_ta_bcp_iccid.sql` | GoogleSQL (DML/DDL) | The primary stored procedure replacing the legacy KornShell script. It orchestrates validation, logs job start/success/failure, and executes the core data transformation. |
| `gcp_migration/airflow/dags/dag_k_ausd_bp_ta_bcp_iccid.py` | Python (Airflow DAG) | Cloud Composer DAG that replaces legacy UC4/Automic scheduling. It extracts execution parameters and triggers the BigQuery stored procedure. |

---

## 3. Key Design Decisions

### 3.1. Push-Down Orchestration to BigQuery
* **Decision**: Move parameter validation, date parsing, and execution logging directly into BigQuery stored procedures rather than handling them entirely inside Python/Airflow.
* **Reasoning**: This keeps the operational logic close to the data, minimizes the complexity of the Airflow DAG, and allows database administrators to maintain and debug execution logic using standard SQL.

### 3.2. Centralized Metadata Logging
* **Decision**: Replace legacy on-premises FOS tracking commands (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) and local temporary files (`bert_k_ausd_bp_ta_bcp_iccid.tmp`) with structured BigQuery tables (`job_tracking` and `job_error_log`).
* **Reasoning**: Provides a unified, queryable, and durable audit trail of all job runs directly within the data warehouse, facilitating easier monitoring and alerting.

### 3.3. Reusable Validation and Logging Modules
* **Decision**: Extract validation and logging routines into a separate file (`reusable_job_logging.sql`) as standalone procedures.
* **Reasoning**: Promotes code reuse across other migrated jobs within the `isbert` system, ensuring consistent logging behavior and reducing code duplication.

### 3.4. Safe Date Parsing
* **Decision**: Implement a custom SQL function `fn_parse_ddmmyyyy_to_date` using `SAFE.PARSE_DATE`.
* **Reasoning**: Legacy systems frequently pass dates as strings in `DDMMYYYY` format. Using `SAFE.PARSE_DATE` prevents query failures on malformed inputs, allowing the procedure to log a clean validation error instead of crashing.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job in a production environment, the following manual setup steps must be completed:

### 4.1. Schema and Dataset Creation
Ensure the target BigQuery datasets exist in your project:
```sql
CREATE SCHEMA IF NOT EXISTS `prj-dw-isbert-prod.isbert_metadata` OPTIONS(location="EU");
CREATE SCHEMA IF NOT EXISTS `prj-dw-isbert-prod.isbert_schema` OPTIONS(location="EU");
```

### 4.2. Deploy DDL and Stored Procedures
Execute the generated SQL scripts in the target BigQuery project in the following order:
1. `gcp_migration/bigquery/ddl/job_tracking_tables.sql`
2. `gcp_migration/bigquery/procedures/reusable_job_logging.sql`
3. `gcp_migration/bigquery/procedures/k_ausd_bp_ta_bcp_iccid.sql`

### 4.3. IAM & Permissions
The Cloud Composer service account (e.g., `service-XXXXXX@gcp-sa-composer.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the `isbert_schema` and `isbert_metadata` datasets.

### 4.4. Airflow DAG Deployment
1. Copy `gcp_migration/airflow/dags/dag_k_ausd_bp_ta_bcp_iccid.py` to the `dags/` folder of your Cloud Composer environment's GCS bucket.
2. Verify that the `PROJECT_ID` and `DATASET` variables in the DAG file match your target environment configuration.

---

## 5. Known Gaps & Unresolved References

### 5.1. Core SQL Transformation Placeholder (Redesign Item)
* **Gap**: The business logic originally contained in `d_ausd_bp_ta_bcp_iccid.sql` is not fully implemented in the generated stored procedure. It is currently represented by an `EXECUTE IMMEDIATE` placeholder block:
  ```sql
  -- Placeholder for core logic migrated from d_ausd_bp_ta_bcp_iccid.sql
  ```
* **Action Required**: The legacy Oracle SQL script `d_ausd_bp_ta_bcp_iccid.sql` must be translated to BigQuery-compliant GoogleSQL. Once translated, replace the placeholder inside `gcp_migration/bigquery/procedures/k_ausd_bp_ta_bcp_iccid.sql` with the actual DML/DDL statements.

### 5.2. Commented File-Based Post-Processing
* **Gap**: The legacy shell script contained commented-out UNIX commands (`sed`, `sort`, `join`) to manipulate physical CSV files (`cibasisprodukt.csv`).
* **Action Required**: If downstream systems still require these physical file exports, you must implement a BigQuery `EXPORT DATA` statement at the end of the stored procedure to write the contents of the target table directly to a Google Cloud Storage (GCS) bucket.

---

## 6. Validation

To validate the migration, perform the following test steps:

### 6.1. How to Run the Tests
1. Open the Airflow UI in Cloud Composer.
2. Locate `dag_k_ausd_bp_ta_bcp_iccid` and trigger it manually with the following test configuration JSON:
   ```json
   {
     "p_job_kennung": "TEST_JOB_01",
     "p_eintrags_nr": "99999",
     "p_stichtag": "15102023",
     "p_wiederanlauf_wert": "0"
   }
   ```

### 6.2. What "Passing" Means
The test run is successful if:
1. The Airflow DAG task `run_k_ausd_bp_ta_bcp_iccid` completes with a `success` status.
2. A query to `isbert_metadata.job_tracking` returns a record for the run with `run_status = 'S'` (Success):
   ```sql
   SELECT * FROM `isbert_metadata.job_tracking` 
   WHERE job_id = 'TEST_JOB_01' AND entry_no = '99999';
   ```
3. No errors are logged in `isbert_metadata.job_error_log` for this execution.
4. (After core SQL integration) The target table `isbert_schema.PoolBasisprodukt` contains the expected transformed data for the business date `2023-10-15`.

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, execute the following rollback steps:

1. **Pause the Airflow DAG**: In the Airflow UI, toggle the switch for `dag_k_ausd_bp_ta_bcp_iccid` to **Off** to prevent further scheduled or manual executions.
2. **Revert Target Tables (Optional)**: If the failed run corrupted target tables, use BigQuery's Time Travel feature to restore the tables to their state prior to the run:
   ```sql
   CREATE OR REPLACE TABLE `isbert_schema.PoolBasisprodukt`
   AS SELECT * FROM `isbert_schema.PoolBasisprodukt`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Re-enable Legacy Scheduling**: Reactivate the legacy UC4/Automic scheduler job pointing to the on-premises KornShell script environment.