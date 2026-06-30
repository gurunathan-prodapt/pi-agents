# MIGRATION_NOTES.md

**Job Name:** `ausd_bp_ta_bpr_evn`  
**Source System:** Oracle DB & UC4/Automic Scheduler  
**Target System:** Google Cloud Platform (GCP) — Cloud Composer (Apache Airflow) & BigQuery  

---

## 1. Summary

The legacy batch job `DW.BERT_AUSD_BP_TA_BPR_EVN` has been migrated from an on-premise Oracle and UC4/Automic environment to Google Cloud Platform (GCP). 

### Scope of Migration
* **Legacy Workload:** A multi-layered batch job that extracts, filters, and loads EVN (Einzelverbindungsnachweis / itemized bill) basis product instances from a master product instance table (`sof$ta_bpr_instance`) into a target table (`sof$ta_bpr_evn`).
* **Target Platform:** Google Cloud Composer (Apache Airflow) for orchestration and Google BigQuery for data warehousing and transformation.
* **Migration Strategy:** Consolidated refactoring. The legacy KornShell (KSH) wrappers, parameter checks, and SQL\*Plus scripts have been consolidated into a single, unified Apache Airflow DAG executing native BigQuery SQL.

---

## 2. Generated Artifacts

The migration replaces the legacy files with two primary target artifacts:

### 1. `dags/dw_bert_ausd_bp_ta_bpr_evn_dag.py`
* **Role:** Airflow DAG Orchestrator.
* **Details:** 
  * Implements parameter parsing and validation (e.g., `stichtag` and `wiederanlauf_wert`).
  * Dynamically builds the SQL query using a Python helper function to allow runtime overrides of datasets, tables, and product IDs (`bpr_ids`).
  * Executes the transformation via the `BigQueryExecuteQueryOperator`.
  * Replaces: `DW.BERT_AUSD_BP_TA_BPR_EVN.xml` (UC4), `r_ausd_bp_ta_bpr_evn.ksh`, and `k_ausd_bp_ta_bpr_evn.ksh`.

### 2. `gcs/queries/d_ausd_bp_ta_bpr_evn.sql`
* **Role:** Static BigQuery SQL Script.
* **Details:** 
  * Contains the core data transformation logic.
  * Performs a `TRUNCATE` on the target table `sof_ta_bpr_evn` followed by an `INSERT` of filtered records from `sof_ta_bpr_instance` based on the specified EVN product IDs.
  * Replaces: `d_ausd_bp_ta_bpr_evn.sql` (Oracle SQL\*Plus).

---

## 3. Key Design Decisions

### Consolidation of Orchestration Layers
* **Decision:** Replaced the multi-tiered legacy shell structure (`r_*.ksh` calling `k_*.ksh` calling `d_*.sql`) with a single Airflow DAG.
* **Reasoning:** Reduces operational complexity, eliminates shell-script maintenance overhead, and leverages native Airflow logging, retries, and alerting.

### Schema and Naming Conventions
* **Decision:** Converted Oracle table names containing `$` (e.g., `sof$ta_bpr_evn`) to standard BigQuery formatting using underscores (e.g., `sof_ta_bpr_evn`).
* **Reasoning:** BigQuery does not support special characters like `$` in table identifiers. Standardizing on underscores ensures ANSI SQL compliance and compatibility with GCP tools.

### Performance Optimization & Hint Removal
* **Decision:** Removed Oracle-specific optimizer hints (e.g., `/*+ full(bp) parallel(bp,4) */`) and database links (e.g., `@pcrs1`).
* **Reasoning:** BigQuery is a serverless, columnar database that automatically parallelizes and optimizes queries. Manual execution hints are obsolete and unsupported.

### Idempotency and Restartability
* **Decision:** Implemented a `TRUNCATE` and `INSERT` pattern for the target table.
* **Reasoning:** Ensures that if a job fails mid-run, it can be safely restarted without risk of duplicating or corrupting data.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the DAG in production, the following setup steps must be completed:

### 1. BigQuery Dataset & Table Creation
Ensure the target dataset and tables exist in the correct region (e.g., `EU`):
* **Dataset:** `bert_dataset` (or environment-specific equivalent like `bert_dataset_prod`).
* **Source Table:** `sof_ta_bpr_instance` must be populated and schema-aligned.
* **Target Table:** Create `sof_ta_bpr_evn` with the appropriate schema:
  ```sql
  CREATE TABLE IF NOT EXISTS `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn` (
      cntrct_id INT64,
      bpr_id INT64
  );
  ```

### 2. IAM & Permissions
The Cloud Composer Service Account (e.g., `sa-composer@gcp-bert-prd.iam.gserviceaccount.com`) must have the following roles:
* `roles/bigquery.dataEditor` on the target dataset.
* `roles/bigquery.jobUser` on the GCP project.

### 3. Airflow Connections
* Verify that the Airflow connection `google_cloud_default` is configured and has access to the target GCP project (`gcp-bert-prd` or `gcp-bert-dev`).

### 4. GCS Query Deployment
* Upload the static SQL file `d_ausd_bp_ta_bpr_evn.sql` to the Cloud Composer GCS bucket under the `gcs/queries/` directory.

### 5. Scheduling & Upstream Triggers
* The DAG is currently configured with `schedule_interval=None`. If this job must run on a schedule or be triggered by an upstream DAG (e.g., after the master product instance table is loaded), configure the schedule or add a `TriggerDagRunOperator` in the upstream DAG.

---

## 5. Known Gaps & Unresolved References

### 1. Dynamic Watermark Lookup (`dwtk_meldungen`)
* **Context:** The legacy Oracle script dynamically queried `isbert_schema.dwtk_meldungen` to retrieve a watermark date (`v_datum`) from the `BERT_DROP_TEMP_TABLE` job run.
* **Current Status:** This lookup has been bypassed in the target design because BigQuery uses static target tables with `TRUNCATE` instead of dropping and recreating dynamic tables daily.
* **Follow-up (B4 Redesign Item):** If downstream processes strictly require this watermark date or if the target table must be partitioned/suffixed by this date, a redesign task must be logged to add an upstream metadata lookup task in Airflow to fetch and pass this date dynamically.

### 2. Hardcoded Product IDs
* **Context:** The EVN product IDs (`32, 2506, 2839, 2840, 3055, 3056, 3821`) are hardcoded in the SQL script and the DAG default arguments.
* **Follow-up:** If these IDs change frequently, they should be moved to an Airflow Variable or a configuration table in BigQuery to avoid code changes during product updates.

---

## 6. Validation

To validate the migrated workload, perform the following steps:

### Execution Test
1. Navigate to the Airflow UI.
2. Trigger the DAG `dw_bert_ausd_bp_ta_bpr_evn` manually.
3. (Optional) Pass custom configuration JSON to test parameter overrides:
   ```json
   {
     "project_id": "gcp-bert-dev",
     "dataset": "bert_dataset_test",
     "bpr_ids": [32, 2506]
   }
   ```

### Definition of "Passing"
The migration is considered successful and ready for production when:
* **DAG Status:** The DAG run completes with a `SUCCESS` state.
* **Task Logs:** The `run_evn_provisioning` task log shows successful query execution with no syntax or permission errors.
* **Data Integrity:** 
  * The target table `sof_ta_bpr_evn` is successfully truncated and repopulated.
  * A comparison query shows that the row count and data in `sof_ta_bpr_evn` match the legacy Oracle table `sof$ta_bpr_evn` for the same source snapshot.
  ```sql
  -- Validation Query
  SELECT bpr_id, COUNT(*) 
  FROM `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn` 
  GROUP BY bpr_id;
  ```

---

## 7. Rollback Procedure

If issues are detected post-go-live, execute the following rollback steps:

### Step 1: Pause the Airflow DAG
* Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_bpr_evn` to **Off** (Paused) to prevent further automated runs.

### Step 2: Revert Data State (Optional)
If a bad run corrupted the target table and you need to restore the previous day's state:
* **Option A (Time Travel):** Restore the table to a state prior to the DAG run using BigQuery Time Travel:
  ```sql
  CREATE OR REPLACE TABLE `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`
  AS SELECT * FROM `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
  ```
* **Option B (Manual Truncate):** If the table is populated daily from scratch, simply truncate the table to clear invalid data:
  ```sql
  TRUNCATE TABLE `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`;
  ```

### Step 3: Reactivate Legacy Workload
* Re-enable the UC4 job `DW.BERT_AUSD_BP_TA_BPR_EVN` in the legacy scheduler to resume processing on the on-premise Oracle database.