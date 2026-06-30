# Migration Notes: `ausd_bp_ta_cntrct_evn`

This document provides comprehensive migration notes for the job `ausd_bp_ta_cntrct_evn`, detailing the transition from the legacy Oracle Data Warehouse (DWH) environment to Google Cloud Platform (GCP).

---

## 1. Summary
The `ausd_bp_ta_cntrct_evn` job has been migrated from an on-premises Oracle DWH orchestrated via Automic/UC4 and KornShell (`.ksh`) wrapper scripts to **Google Cloud Platform (GCP)**. 

* **Source Platform:** Oracle Database, UC4/Automic Orchestrator, KornShell (KSH) wrappers.
* **Target Platform:** Google Cloud Platform (GCP) using **Cloud Composer (Apache Airflow)** for orchestration and **BigQuery** for serverless data warehousing and transformation.
* **Job Purpose:** This job truncates and repopulates the contract EVN (Einzelverbindungsnachweis / detailed call record) table `sof_ta_cntrct_evn` by aggregating and pivoting event indicators from the base product event table `sof_ta_bpr_evn`.

---

## 2. Generated Artifacts
The migration process consolidated four legacy layers (UC4 XML, wrapper shell, control shell, and SQL*Plus script) into two clean, production-grade target files:

### 1. `dags/dw_bert_ausd_bp_ta_cntrct_evn.py` (Airflow DAG)
* **Role:** Orchestrates the execution pipeline. It defines the DAG structure, schedules the daily run, manages task dependencies, and triggers the BigQuery SQL execution.
* **Key Features:** 
  * Uses `BigQueryExecuteQueryOperator` to run the SQL transformation.
  * Implements `EmptyOperator` for clean execution boundaries (`start` and `end`).
  * Dynamically resolves environment variables using Airflow Variables.

### 2. `sql/d_ausd_bp_ta_cntrct_evn.sql` (BigQuery SQL Script)
* **Role:** Contains the core data transformation logic.
* **Key Features:**
  * Truncates the target table `sof_ta_cntrct_evn`.
  * Performs a high-performance aggregation and pivot of base product IDs (`bpr_id`) to assign specific weight values into the target table.
  * Uses Jinja templating (`{{ var.value.get(...) }}`) to dynamically inject the GCP Project ID and Dataset Name at runtime.

---

## 3. Key Design Decisions

### Consolidation of Legacy Layers
The legacy architecture was highly fragmented, spreading simple logic across four layers (UC4 XML -> Wrapper Shell -> Control Shell -> SQL*Plus). In the target architecture, this has been consolidated into a single Airflow DAG and a single BigQuery SQL script. This reduces maintenance overhead, simplifies debugging, and eliminates shell-scripting vulnerabilities.

### Schema Normalization
Oracle table names containing special characters (specifically `$`) have been normalized to standard BigQuery-compatible underscores:
* `sof$ta_bpr_evn` $\rightarrow$ `sof_ta_bpr_evn`
* `sof$ta_cntrct_evn` $\rightarrow$ `sof_ta_cntrct_evn`

### Refactoring Oracle-Specific SQL to ANSI SQL
* **Decode to Case-When:** The Oracle-specific `DECODE` function was refactored into standard ANSI SQL `CASE WHEN` statements, improving readability and compatibility.
* **Hint Deprecation:** The Oracle parallel execution hint `/*+ parallel(bpr,4) */` was removed. BigQuery is a serverless, column-oriented database that automatically handles query parallelization and execution planning dynamically.

### Environment Parameterization
Hardcoded schema and project references have been replaced with Airflow Jinja templates referencing Airflow Variables (`gcp_project_id` and `gcp_dataset_name`). This allows the exact same code to run unmodified across Development, Test, and Production environments.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following setup steps must be completed:

### 1. Schema & Table Creation
Ensure that the target dataset and tables are provisioned in BigQuery. If they do not exist, create them using the following schemas:

```sql
-- Create Dataset (if not exists)
CREATE SCHEMA IF NOT EXISTS `your_gcp_project.isbert_schema`
OPTIONS(location="EU");

-- Create Target Table
CREATE TABLE IF NOT EXISTS `your_gcp_project.isbert_schema.sof_ta_cntrct_evn`
(
  cntrct_id INT64 OPTIONS(description="Contract Identifier"),
  evn INT64 OPTIONS(description="Aggregated EVN Weight Value")
);
```

### 2. IAM & Permissions
The Cloud Composer Service Account (typically `service-XXX@cloudservices.gserviceaccount.com` or a dedicated custom service account) must be granted the following IAM roles on the target BigQuery dataset:
* **`roles/bigquery.dataEditor`** (on the target dataset `isbert_schema` to truncate and insert data).
* **`roles/bigquery.jobUser`** (on the project level to run BigQuery jobs).

### 3. Airflow Variables Configuration
In the Airflow Web UI, navigate to **Admin -> Variables** and define the following variables:

| Key | Example Value | Description |
| :--- | :--- | :--- |
| `gcp_project_id` | `prod-dwh-gcp-project` | The target GCP Project ID. |
| `gcp_dataset_name` | `isbert_schema` | The target BigQuery dataset name. |

### 4. Scheduling & Deployment
1. Upload `dags/dw_bert_ausd_bp_ta_cntrct_evn.py` to the Cloud Composer DAGs folder (e.g., `gs://<composer-bucket>/dags/`).
2. Upload `sql/d_ausd_bp_ta_cntrct_evn.sql` to the SQL folder in the DAGs directory (e.g., `gs://<composer-bucket>/dags/sql/`).
3. By default, the DAG is scheduled to run daily at **04:00 AM UTC** (`0 4 * * *`). Ensure this does not conflict with upstream loads of `sof_ta_bpr_evn`.

---

## 5. Known Gaps & Unresolved References

### 1. Redesign (B4) Item: Full Truncate-and-Reload vs. Incremental Processing
* **The Gap:** The legacy shell scripts (`r_ausd_bp_ta_cntrct_evn.ksh` and `k_ausd_bp_ta_cntrct_evn.ksh`) contained logic for handling a reporting date (`stichtag`) and a restart checkpoint contract ID (`wiederanlaufwert`). This allowed the legacy system to perform incremental processing or filter data based on specific dates. The migrated SQL script performs a **full truncate and reload** of the entire `sof_ta_cntrct_evn` table from the entire `sof_ta_bpr_evn` table.
* **Impact:** If `sof_ta_bpr_evn` grows extremely large, a daily full truncate-and-reload will become highly inefficient, slow, and expensive in BigQuery.
* **Recommendation (B4 Redesign):** For the next phase, convert `sof_ta_cntrct_evn` into a partitioned table (e.g., partitioned by ingestion date or a business date). Modify the SQL to perform an incremental `MERGE` or partition-level overwrite instead of a global `TRUNCATE`.

### 2. Metadata Logging (`dwtk_meldungen`)
* **The Gap:** The legacy Oracle SQL script referenced a metadata/auditing table (`isbert_schema.dwtk_meldungen`) to log job execution details. This logging has been omitted in the standard SQL migration.
* **Mitigation:** Airflow natively handles task logging, execution history, and status tracking. If enterprise-level audit tables are strictly required, a post-execution task must be added to the DAG to write to a centralized BigQuery audit table.

---

## 6. Validation

To validate the migration, perform the following tests:

### 1. DAG Compilation Test
Run a local syntax and compilation check on the Airflow DAG:
```bash
python dags/dw_bert_ausd_bp_ta_cntrct_evn.py
```
* **Passing Criteria:** The command exits with code `0` and no syntax or import errors are displayed.

### 2. BigQuery SQL Dry-Run
Validate the SQL syntax and estimate bytes scanned without executing the query:
```bash
bq query --use_legacy_sql=false --dry_run < sql/d_ausd_bp_ta_cntrct_evn.sql
```
* **Passing Criteria:** BigQuery returns a successful validation message along with the estimated bytes to be read.

### 3. Data Reconciliation (UAT)
Run the legacy Oracle job and the new BigQuery DAG on identical source datasets, then execute the following reconciliation queries:

```sql
-- Run in BigQuery
SELECT COUNT(*), SUM(evn) FROM `isbert_schema.sof_ta_cntrct_evn`;

-- Run in Oracle
SELECT COUNT(*), SUM(evn) FROM isbert_schema.sof$ta_cntrct_evn;
```
* **Passing Criteria:** The row counts and the sum of the `evn` metric must match **100% exactly** between both systems.

---

## 7. Rollback Procedure

If the migrated pipeline fails in production or causes data corruption, execute the following rollback steps:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_cntrct_evn` to **Off** to prevent further scheduled executions.
2. **Restore Target Table (if needed):**
   If the target table in BigQuery was corrupted or truncated incorrectly, restore it to its pre-fail state using BigQuery's "Time Travel" feature:
   ```sql
   CREATE OR REPLACE TABLE `your_gcp_project.isbert_schema.sof_ta_cntrct_evn` AS
   SELECT * FROM `your_gcp_project.isbert_schema.sof_ta_cntrct_evn`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Re-enable Legacy Pipeline:**
   If necessary, reactivate the UC4/Automic schedule for `DW.BERT_AUSD_BP_TA_CNTRCT_EVN` to resume processing on the legacy Oracle platform.