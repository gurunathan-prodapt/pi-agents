# MIGRATION NOTES: DW.BERT_AUSD_BP_TA_BCP_MSISDN

This document provides comprehensive migration notes for transitioning the legacy UC4 UNIX job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` to Google Cloud Platform (GCP). It details the target architecture, design decisions, manual setup requirements, known gaps, and validation procedures.

---

## 1. Summary

The legacy UC4 UNIX job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` has been migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**. 

* **Legacy Platform:** UC4 (Automic) scheduler executing an on-premise UNIX shell script (`r_ausd_bp_ta_bcp_msisdn.ksh`) that prepared instantiated basic products ("instantiierten Basisprodukte") within the `BERT_P_BASISPRODUKT` model suite.
* **Target Platform:** **Cloud Composer (Apache Airflow)** for orchestration and **BigQuery** as the serverless data warehouse engine.
* **Key Objective:** Transition from a file-and-shell-based execution model to an idempotent, ELT-based SQL execution model leveraging BigQuery's native compute power.

---

## 2. Generated Artifacts

The migration process generated the following core files, located in the Airflow deployment directory:

| File Path | Language / Format | Role & Description |
| :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_bcp_msisdn.py` | Python (Airflow DAG) | The orchestrator file defining the DAG structure, configuration lookups, and task dependencies. It uses the `BigQueryInsertJobOperator` to execute the SQL logic. |
| `dags/sql/dw_bert_ausd_bp_ta_bcp_msisdn.sql` | SQL (BigQuery Dialect) | The core business logic file. It contains modular, temporary stored procedures, temporary staging tables, validation assertions, and an idempotent `MERGE` statement. |

---

## 3. Key Design Decisions

### SQL-First Architecture vs. Dataproc (PySpark)
The automated migration tool initially suggested a Dataproc/PySpark pipeline (`r_ausd_bp_ta_bcp_msisdn.py`). However, because the target data warehouse is BigQuery, we opted for a **native BigQuery SQL execution model** using the `BigQueryInsertJobOperator`. 
* **Why:** Running native SQL inside BigQuery eliminates the overhead of spinning up/down Dataproc clusters, reduces execution costs, simplifies the codebase, and leverages BigQuery's automatic scaling.

### Replacement of Legacy Includes
* **`DW.HOLE_PFAD` & `. $HOME/.dw_init`:** These legacy UNIX scripts dynamically established operational paths and environment profiles. In GCP, these are replaced by **Airflow Variables** (e.g., `GCP_PROJECT_ID`, `BQ_DATASET_BERT`) and **IAM Service Account configurations**, injecting environment properties directly into the execution context.
* **`DW.BERT_LESE_LOG`:** This legacy post-execution script read database and execution logs. In the migrated state, logging is handled natively by **GCP Cloud Logging** and Airflow's built-in task log aggregation.

### Idempotency and Restartability
The legacy documentation states *"Restart jederzeit möglich"* (Restart possible at any time). To preserve this behavior:
* The target load step in `dw_bert_ausd_bp_ta_bcp_msisdn.sql` uses a **`MERGE` statement** instead of a blind `INSERT`. 
* If the job is restarted or run multiple times for the same business date, it will update existing records and insert new ones without creating duplicate entries.

---

## 4. Manual Steps Before Go-Live

Before enabling and running the migrated DAG in production, the following manual setup steps must be completed:

### 1. Schema and Dataset Creation
Ensure the target BigQuery datasets and tables exist. If they do not, create them:
```sql
CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.dw_bert`
OPTIONS(location="EU");

CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.dw_bert_staging`
OPTIONS(location="EU");

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT_ID.dw_bert.bert_ausd_bp_ta_bcp_msisdn` (
  msisdn STRING,
  product_id STRING,
  customer_id STRING,
  valid_from DATE,
  valid_to DATE,
  process_date DATE,
  batch_id STRING,
  is_valid_record BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

### 2. IAM & Permissions
The Cloud Composer worker service account (e.g., `dwh-bert-sa@YOUR_GCP_PROJECT_ID.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **BigQuery Job User** (`roles/bigquery.jobUser`) at the project level.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the `dw_bert` and `dw_bert_staging` datasets.

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment (via UI or CLI):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT_ID` | `gcp-dwh-prod` | The target GCP Project ID. |
| `BQ_DATASET_BERT` | `dw_bert` | The production dataset name. |
| `BQ_DATASET_STAGING` | `dw_bert_staging` | The staging dataset name. |
| `CONN_ID_BIGQUERY` | `bigquery_default` | The Airflow connection ID for GCP. |
| `BIGQUERY_LOCATION` | `EU` | The geographic location of the BigQuery datasets. |

### 4. Scheduling
Because the UC4 export did not contain an `EVNT_TIME` object, the DAG is currently configured with `schedule=None` (manual/triggered execution). If this job must run on a schedule, update the `schedule` parameter in `dags/dw_bert_ausd_bp_ta_bcp_msisdn.py` to a valid cron expression (e.g., `schedule="0 2 * * *"` for daily at 2:00 AM).

---

## 5. Known Gaps & Unresolved References

### Missing Shell Script Internals (B4 Redesign Item)
* **Gap:** The internal business logic and SQL queries executed inside the legacy shell script `r_ausd_bp_ta_bcp_msisdn.ksh` were not provided in the UC4 XML export.
* **Resolution Required:** A database developer must manually inspect the legacy `.ksh` script, extract the core transformation logic, and replace the placeholder logic inside the following temporary procedures in `dags/sql/dw_bert_ausd_bp_ta_bcp_msisdn.sql`:
  * `sp_prepare_source()`: Replace the mock `tmp_source` table with the actual source extraction logic.
  * `sp_transform_basis_products()`: Replace the mock transformation rules with the actual business rules for preparing instantiated basic products.

---

## 6. Validation

To validate the migration, execute the following testing steps:

### Step 1: SQL Syntax Dry-Run
Validate the BigQuery SQL script syntax without executing or billing for slot usage:
```bash
bq query \
  --use_legacy_sql=false \
  --dry_run \
  < dags/sql/dw_bert_ausd_bp_ta_bcp_msisdn.sql
```
* **Passing Criteria:** The command returns a success message indicating the query is valid and estimates the bytes that will be processed (0 bytes for dry-runs).

### Step 2: Airflow DAG Parsing Test
Verify that the Airflow DAG compiles without syntax or import errors:
```bash
python3 dags/dw_bert_ausd_bp_ta_bcp_msisdn.py
```
* **Passing Criteria:** The command exits with code `0` and outputs no errors.

### Step 3: Integration Test Run
Trigger a manual test run of the task using the Airflow CLI:
```bash
airflow tasks test dw_bert_ausd_bp_ta_bcp_msisdn bert_ausd_bp_ta_bcp_msisdn 2026-04-21
```
* **Passing Criteria:** 
  1. The task execution completes with a `SUCCESS` status.
  2. The `sp_validate_output()` assertion passes.
  3. The target table `dw_bert.bert_ausd_bp_ta_bcp_msisdn` contains the expected records.

---

## 7. Rollback Procedure

If a critical issue is discovered post-deployment, execute the following steps to roll back the changes:

1. **Pause the Airflow DAG:**
   ```bash
   airflow dags pause dw_bert_ausd_bp_ta_bcp_msisdn
   ```
2. **Remove Artifacts:**
   Delete the DAG and SQL files from the Cloud Composer GCS bucket:
   ```bash
   gsutil rm gs://<composer-bucket>/dags/dw_bert_ausd_bp_ta_bcp_msisdn.py
   gsutil rm gs://<composer-bucket>/dags/sql/dw_bert_ausd_bp_ta_bcp_msisdn.sql
   ```
3. **Restore Target Table (If Corrupted):**
   If the target table was corrupted during execution, restore it to a known good state using BigQuery Time Travel (e.g., restoring to state 1 hour ago):
   ```sql
   CREATE OR REPLACE TABLE `YOUR_GCP_PROJECT_ID.dw_bert.bert_ausd_bp_ta_bcp_msisdn`
   AS SELECT * FROM `YOUR_GCP_PROJECT_ID.dw_bert.bert_ausd_bp_ta_bcp_msisdn`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```