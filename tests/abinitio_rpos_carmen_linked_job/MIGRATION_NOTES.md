# Migration Notes: DW.RPOS_CARM_IMPORT

This document provides comprehensive technical notes for the migration of the UC4 job `DW.RPOS_CARM_IMPORT` and its associated Ab Initio graph `map_rpos_carmen_import` to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy UC4 Unix job `DW.RPOS_CARM_IMPORT` has been migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**. 

* **Source Platform:** UC4/Automic Scheduler executing an Ab Initio graph (`map_rpos_carmen_import.mp`) via a KornShell wrapper (`map_rpos_carmen_import.ksh`) and the `r_ai_start` launcher utility.
* **Target Platform:** **Google Cloud Composer (Apache Airflow)** orchestrating a **Google Cloud Dataproc Serverless (PySpark)** pipeline, with data storage and transactional processing migrated to **Google BigQuery**.
* **Functional Scope:** Ingests retail point-of-sale (RPOS) billing and invoice transaction data from the external "Carmen" leasing system, validates the payload, correlates transactions with historical master contract data, routes records to specialized financial tables (Factoring Invoices, Factoring Credits, Reselling, and Temporary Storage), and updates operational audit logs.

---

## 2. Generated Artifacts

The migration process generated the following files, preserving the original repository structure to maintain folder integrity:

| Target File Path | Language / Type | Role / Description |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` | Python (Apache Airflow DAG) | Orchestrates the end-to-end workflow. Defines a standalone, externally triggered DAG that submits the PySpark execution job to Dataproc. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Python 3 (Orchestration Wrapper) | Replaces the legacy KornShell wrapper (`.ksh`). Handles environment validation, executes pre-load idempotency deletes on BigQuery, submits the PySpark batch job, and performs post-load audit updates. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import_pyspark.py` | Python 3 (PySpark Pipeline) | Replaces the core Ab Initio graph (`.mp`) logic. Performs file parsing, strict schema validation, historical contract joins, ranking, and multi-channel routing. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | Python 3 (PySpark Module) | A structural placeholder mirroring the legacy `.mp` path, containing environment-specific configurations and schema definitions. |

---

## 3. Key Design Decisions

### 3.1. Orchestration & Execution Split
* **Decision:** Decouple the orchestration wrapper from the core data transformation.
* **Reasoning:** The legacy KornShell wrapper performed both environment setup and database control tasks. In the target architecture, the Airflow DAG handles high-level scheduling, the Python wrapper (`run/map_rpos_carmen_import.py`) manages transactional BigQuery DML (pre-deletes and post-audits), and Dataproc Serverless executes the heavy-duty PySpark transformation. This ensures optimal resource utilization and separation of concerns.

### 3.2. Bulk Left Anti-Joins for Idempotency (Delete-before-Insert)
* **Decision:** Replace row-by-row cursor deletions with bulk `LEFT ANTI JOIN` operations in PySpark.
* **Reasoning:** The legacy Ab Initio graph performed row-by-row key-based deletions on target tables to prevent duplicate records. Executing row-by-row DML on BigQuery is highly inefficient, costly, and prone to transaction locks. The PySpark implementation reads target tables, performs a bulk `LEFT ANTI JOIN` against the incoming dataset to filter out existing matching records in-memory, and then overwrites or appends the partition in bulk.

### 3.3. Literal String & Error Message Retention
* **Decision:** Retain all original German error descriptions and validation messages character-for-character (e.g., `"Invalid data format in monats_id"`, `"Invalid Data in field debitor_id"`).
* **Reasoning:** To comply with strict compliance and automated log-parsing rules, all legacy validation literals are preserved exactly as defined in the original `.xfr` and `.dml` files.

### 3.4. Elimination of Legacy Shell Utilities
* **Decision:** Decommission legacy helper scripts such as `.project.ksh`, `AB_CATALOG_FUNCTIONS.KSH`, `.CCR_INIT`, and `.DW_INIT`.
* **Reasoning:** These utilities are tightly coupled with the on-premise Unix environment and the Ab Initio Co>Operating System. Sourced variables have been successfully mapped to native Airflow Variables, BigQuery parameter overrides, and runtime configuration dictionaries.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target environment before triggering the pipeline:

### 4.1. BigQuery Schema & Dataset Creation
Ensure that the target BigQuery dataset (configured via the `BQ_DATASET` variable) exists and contains the following tables with schemas matching the legacy Oracle definitions:
* `dwh_ta_f_rpos_carm` (Base Invoice Transactions)
* `dwh_ta_f_gpos_fact_carm` (Factoring Credit Notes)
* `dwh_ta_f_rpos_fact_carm` (Factoring Invoices)
* `dwh_ta_f_rpos_reselling_carm` (Reselling Transactions)
* `dwh_ta_t_rpos_carm` (Temporary Storage)
* `dwh_ta_k_meldungen` (Operational Logging)
* `dwh_ta_k_rech_absgrp` (Billing Group Run Markers)
* `dwh_ta_c_vertrag` (Master Contract History)

### 4.2. IAM & Permissions
The service account executing the Cloud Composer workers and Dataproc Serverless batches must have the following IAM roles:
* **BigQuery Admin** or **BigQuery Data Editor** + **BigQuery Job User** on the target dataset.
* **Storage Object Admin** on the GCS buckets used for script storage and temporary staging.
* **Dataproc Worker** and **Dataproc Editor** to submit and run serverless batches.

### 4.3. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-123` | Target Google Cloud Project ID |
| `GCP_REGION` | `europe-west3` | GCP Region for Dataproc and Composer |
| `DATAPROC_CLUSTER` | `dataproc-ephemeral-cluster` | Name of the Dataproc cluster/template |
| `GCS_BUCKET` | `prod-dwh-carmen-import` | Primary GCS bucket for input files and scripts |
| `BQ_DATASET` | `dwh_core` | Target BigQuery dataset name |

### 4.4. GCS Artifact Deployment
Upload the generated PySpark scripts to their designated GCS paths:
* Upload `map_rpos_carmen_import_pyspark.py` to `gs://{GCS_BUCKET}/pyspark/map_rpos_carmen_import_pyspark.py`.
* Ensure the input Carmen POS files are delivered to `gs://{GCS_BUCKET}/crs/work/` matching the file mask `CARMEN_B_*_pos.fix`.

---

## 5. Known Gaps & Unresolved References

### 5.1. Standalone DAG Status (Unresolved Parent Container)
* **Gap:** The legacy UC4 job was extracted as an isolated object without its parent JobPlan (`JOBP`) or calendar triggers.
* **Redesign Action:** The migrated DAG is currently configured with `schedule=None` (externally triggered). Once the master orchestration pipeline is migrated, this DAG should be integrated using a `TriggerDagRunOperator` or merged as a task block within the parent pipeline's DAG.

### 5.2. Strict `raise_error` Crash Behavior (B4 Redesign Item)
* **Gap:** The PySpark pipeline mimics the legacy `force_error()` behavior by raising a terminal `ValueError` when encountering invalid data formats. This immediately aborts the Spark executor and fails the entire batch run.
* **Redesign Recommendation:** In a future sprint (B4 optimization phase), replace this crash-first approach with a **Quarantine Pattern**. Invalid rows should be routed to a dead-letter GCS path or a `quarantine` BigQuery table, allowing valid records to process uninterrupted while alerting operations.

---

## 6. Validation

To validate the migrated pipeline, execute the following test suite:

### 6.1. DAG Parsing Test
Run a local syntax and import check on the Airflow DAG:
```bash
python3 abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py
```
*Passing criteria:* The command exits with code `0` without throwing any Airflow import or syntax errors.

### 6.2. Dry-Run Validation
Execute the Python wrapper script in dry-run mode by setting dummy environment variables:
```bash
export GCP_PROJECT="your-dev-project"
export BQ_DATASET="your_dev_dataset"
export GCS_BUCKET="your-dev-bucket"
# Run the wrapper script
python3 abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py -help
```
*Passing criteria:* The script prints the help menu and exits with code `1` (matching legacy behavior).

### 6.3. End-to-End Execution Test
1. Place a sample test file (e.g., `CARMEN_B_test_pos.fix`) containing valid header ("H"), payload ("P"), and trailer ("X") records into `gs://{GCS_BUCKET}/crs/work/`.
2. Trigger the `dw_rpos_carm_import` DAG manually from the Airflow UI.
3. Monitor the Dataproc Serverless batch execution logs.

### 6.4. Definition of "Passing"
The validation run is successful if and only if:
* The Airflow DAG run finishes with a `success` state.
* The Dataproc Serverless batch logs show `State: SUCCEEDED`.
* Target BigQuery tables (`dwh_ta_f_rpos_carm`, etc.) show an increase in row counts corresponding exactly to the input file payload.
* No duplicate records exist for the processed keys (verifying the bulk anti-join logic).
* The audit tables `dwh_ta_k_meldungen` and `dwh_ta_k_rech_absgrp` contain updated run markers matching the test file metadata.

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, execute the following rollback steps:

### 7.1. Orchestration Rollback
1. Pause the migrated Airflow DAG in the Cloud Composer UI:
   ```bash
   gcloud composer environments run {COMPOSER_ENV} \
       --location {GCP_REGION} \
       dags pause -- dw_rpos_carm_import
   ```
2. If integrated into a master pipeline, revert the master DAG to its previous Git commit.

### 7.2. Data Rollback (BigQuery)
If the failed run corrupted or appended bad data to the target tables, restore them to their pre-run state using BigQuery's system history (Time Travel):
```sql
-- Example: Restore dwh_ta_f_rpos_carm to its state 1 hour ago
CREATE OR REPLACE TABLE `your_project.your_dataset.dwh_ta_f_rpos_carm`
AS SELECT * FROM `your_project.your_dataset.dwh_ta_f_rpos_carm`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```
*Repeat this query for all affected target tables:*
* `dwh_ta_f_gpos_fact_carm`
* `dwh_ta_f_rpos_fact_carm`
* `dwh_ta_f_rpos_reselling_carm`
* `dwh_ta_t_rpos_carm`
* `dwh_ta_k_meldungen`
* `dwh_ta_k_rech_absgrp`

### 7.3. Legacy Fallback
If necessary, re-enable the legacy UC4 job `DW.RPOS_CARM_IMPORT` in the on-premise scheduler to resume processing via the legacy Ab Initio environment.