# Migration Notes: DW.DWH_ABTN_SMART_KUBI

This document outlines the migration details, design decisions, manual setup steps, and validation procedures for migrating the legacy UC4/Automic job `DW.DWH_ABTN_SMART_KUBI` to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary

The legacy job `DW.DWH_ABTN_SMART_KUBI` was a Unix-based UC4 job (`JOBS_UNIX`) that executed an Oracle PL/SQL script (`d_abtn_x_smart_kubi.sql`) via a KornShell wrapper (`r_sqlscript`). 

The job's primary business function is to aggregate and load contract and tariff data into a temporary/reporting table (`DWH$TA_T_SMART_KUBI`). A key operational feature is the dynamic calculation of a reporting month identifier (`MONATSID`):
* If executed before the 15th of the month, it targets the previous month (`YYYYMM`).
* If executed on or after the 15th, it targets the current month (`YYYYMM`).

### Migration Target
* **Orchestration**: Cloud Composer (Apache Airflow 2.x)
* **Compute/Database**: Google Cloud BigQuery (Serverless SQL Scripting)
* **Codebase**: Python 3.x (for environment initialization, logging wrappers, and Airflow DAGs)

---

## 2. Generated Artifacts

The migration process has generated the following files, preserving the structural integrity of the source repository:

| Generated File Path | Role / Description |
| :--- | :--- |
| `dags/dw_dwh_abtn_smart_kubi.py` | **Primary Airflow DAG**: Orchestrates the workflow, dynamically calculates `MONATSID` based on the DAG's logical date, and executes the BigQuery SQL script. |
| `sql/d_abtn_x_smart_kubi.sql` | **BigQuery SQL Script**: Migrated from Oracle PL/SQL. Performs the table truncation, ANSI left outer joins, conditional logic mapping, and data insertion. |
| `utils/dw_init.py` | **Environment Initializer**: Python translation of `.dw_init`. Sets up environment variables, GCS bucket paths, and legacy directory mappings. |
| `utils/f_alis_msgerr.py` | **Logging & Error Helper**: Python translation of `f_alis_msgerr.ksh`. Provides status reporting and error logging hooks. |
| `utils/h_alis_sqlplus.py` | **SQL Execution Helper**: Python translation of `h_alis_sqlplus.ksh`. Retained for backward-compatible/hybrid execution paths. |
| `utils/r_sqlscript.py` | **Execution Wrapper**: Python translation of the `r_sqlscript` shell wrapper. Supports parameterized BigQuery executions with legacy-aligned logging. |

---

## 3. Key Design Decisions

### 3.1 Direct BigQuery SQL Scripting (No Python/Bash Wrapper for SQL)
* **Decision**: The Oracle PL/SQL anonymous block was converted directly into a native BigQuery SQL Script (`DECLARE`, `BEGIN...EXCEPTION...END`).
* **Reasoning**: This allows BigQuery to handle the procedural logic (truncation, insertion, exception catching) serverlessly. It eliminates the overhead of spawning containerized Python or Bash processes to run SQL, reducing execution costs and complexity.

### 3.2 Idempotent Date Calculation
* **Decision**: The legacy dynamic date logic (`MONATSID` calculation) was moved into Airflow Jinja macros using the DAG's `logical_date` (formerly `execution_date`) instead of the system's real-time clock.
* **Reasoning**: If a historical run needs to be re-executed or backfilled, using `logical_date` guarantees that the job targets the correct historical reporting month, ensuring strict pipeline idempotency.

### 3.3 ANSI Join & Partition Pruning Rewrite
* **Decision**: 
  1. Proprietary Oracle outer join syntax `(+)` was rewritten to standard ANSI `LEFT OUTER JOIN`.
  2. Oracle-specific partition targeting syntax `partition(dwh$ta_f_d1_twvv_tn_&1)` was replaced with standard `WHERE` clause filtering on the partitioned column (`gueltigkeitszeitpunkt`).
* **Reasoning**: BigQuery does not support proprietary Oracle join syntax. Furthermore, BigQuery's query engine automatically performs partition pruning when filtering on partition keys, eliminating the need for explicit partition naming.

### 3.4 Dual-Execution Path Support
* **Decision**: We provided both a direct Airflow operator execution path (`BigQueryInsertJobOperator` using SQL templates) and a Python wrapper execution path (`r_sqlscript.py`).
* **Reasoning**: This provides maximum flexibility. Modern serverless pipelines can use the direct Airflow operator, while legacy-aligned hybrid pipelines can use the Python wrapper to maintain identical logging and sequence tracking in the database.

---

## 4. Manual Steps Before Go-Live

Before deploying the DAG and executing the pipeline, the following manual setup steps must be completed in the target GCP environment:

### 4.1 BigQuery Dataset & Table Creation
Ensure the target BigQuery dataset (e.g., `dwh_dataset`) exists and contains the following tables with compatible schemas:
1. `dwh_ta_t_smart_kubi` (Target table)
2. `dwh_ta_f_d1_twvv_tn` (Source fact table - partitioned on `gueltigkeitszeitpunkt`)
3. `dwh_vi_l_map_fa_tarif` (Source dimension view/table)
4. `bl_d_tarif` (Source dimension table)
5. `dwh_ta_c_vertrag` (Source dimension table)
6. `dwh_error_log` (Target logging table for exception handling)

### 4.2 Airflow Variables Setup
Configure the following Airflow Variables in the Cloud Composer UI (`Admin -> Variables`):
* `GCP_PROJECT`: The ID of your Google Cloud Project.
* `GCS_BUCKET`: The GCS bucket name where SQL templates and scripts are stored (e.g., `my-composer-bucket`).
* `BQ_DATASET`: The default BigQuery dataset name.
* `BQ_LOCATION`: The processing region for BigQuery (e.g., `EU` or `US`).
* `SQL_TEMPLATE_PATH`: The GCS or local path where Airflow searches for SQL files (e.g., `/home/airflow/gcs/dags/sql`).

### 4.3 IAM & Permissions
Ensure the service account running Cloud Composer has the following IAM roles:
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset.
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the GCS bucket containing the SQL scripts.

### 4.4 Connection Strings
Verify that the Airflow connection `google_cloud_default` is configured correctly with the appropriate GCP service account credentials.

---

## 5. Known Gaps & Unresolved References

### 5.1 Centralized Logging Framework (`BERT_MELDUNG`)
* **Gap**: The legacy Oracle system relied on a highly integrated PL/SQL logging package (`BERT_MELDUNG`). In the migrated BigQuery SQL script, this is mapped to a standard DML insert into a local `dwh_error_log` table.
* **Follow-up**: If your enterprise requires centralized logging (e.g., Cloud Logging/Stackdriver), the exception block in `d_abtn_x_smart_kubi.sql` should be updated, or Airflow's native task failure callbacks should be configured to forward alerts to your operations center.

### 5.2 Legacy Bug in `r_sqlscript` Path Validation
* **Gap**: The legacy KornShell script contained a logical check: `if [ -f "$l_DBskript" ] then ErrNr=198`. This flags an error (code 198) when the SQL script *exists*, rather than when it is *missing*. 
* **Follow-up**: This behavior was preserved in `r_sqlscript.py` to maintain exact parity, but it is highly recommended to correct this logic to `if not os.path.isfile(l_DBskript)` during the final integration testing phase.

---

## 6. Validation

To validate the migration and ensure functional parity, perform the following tests:

### 6.1 SQL Syntax Validation (Dry Run)
Run a dry-run query in the BigQuery console using the migrated SQL script to verify syntax and schema compatibility:
```sql
-- Set dry run in BigQuery query settings
-- Verify that all tables, columns, and functions resolve correctly.
```

### 6.2 Airflow DAG Local Test
Run a local test of the Airflow DAG for a specific historical date to verify that the dynamic `MONATSID` calculation behaves exactly like the legacy system:

```bash
# Test execution before the 15th (should target previous month)
airflow dags test dw_dwh_abtn_smart_kubi 2023-10-10

# Test execution on/after the 15th (should target current month)
airflow dags test dw_dwh_abtn_smart_kubi 2023-10-20
```

* **Passing Criteria**: 
  * Execution on `2023-10-10` must pass `@param_monats_id = 202309` to the SQL query.
  * Execution on `2023-10-20` must pass `@param_monats_id = 202310` to the SQL query.

### 6.3 Data Parity Validation
1. Run the legacy Oracle job for a specific reporting month (e.g., `202308`).
2. Run the migrated BigQuery DAG for the same reporting month.
3. Compare the row counts and column-level checksums between Oracle's `DWH$TA_T_SMART_KUBI` and BigQuery's `dwh_ta_t_smart_kubi`.
* **Passing Criteria**: Row counts and aggregated metrics (e.g., `SUM(anzahl)`) must match exactly (100% parity).

---

## 7. Rollback Procedure

If critical issues are identified in production after go-live, follow these steps to roll back to the legacy environment:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the `dw_dwh_abtn_smart_kubi` DAG to **Paused** (Off) to prevent any further automated executions.
2. **Re-enable the UC4 Job**:
   In the UC4/Automic console, locate the job `DW.DWH_ABTN_SMART_KUBI` and set its active flag back to `1` (Active).
3. **Clean Up Stale Target Data**:
   If the BigQuery job partially executed or loaded incorrect data, truncate the target table to avoid downstream processing errors:
   ```sql
   TRUNCATE TABLE `your_project.dwh_dataset.dwh_ta_t_smart_kubi`;
   ```
4. **Verify Legacy Execution**:
   Trigger a manual run of the UC4 job and verify in the Oracle database that the table `DWH$TA_T_SMART_KUBI` is populated correctly.