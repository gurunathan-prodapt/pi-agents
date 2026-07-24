# Migration Notes: `DW.RPOS_CARM_IMPORT`

## 1. Summary

The legacy job `DW.RPOS_CARM_IMPORT` has been migrated from an on-premises Ab Initio and UC4/Automic environment to a modern, cloud-native architecture on the **Google Cloud Platform (GCP)**. 

### Scope of Migration
* **Orchestration**: Legacy UC4 Job Definition (`DW.RPOS_CARM_IMPORT.xml`) and KornShell wrapper (`map_rpos_carmen_import.ksh`) have been migrated to **Cloud Composer (Apache Airflow)**.
* **Data Processing**: The legacy Ab Initio graph (`map_rpos_carmen_import.mp`) has been converted to a **Dataproc Serverless PySpark** application.
* **Data Warehouse**: Legacy Oracle database tables have been mapped to **Google BigQuery** tables.
* **Storage**: Legacy Unix local directories (e.g., `$DW_DIR_IMP_SAP`) have been migrated to **Google Cloud Storage (GCS)**.

### Business Purpose
This pipeline ingests, validates, and routes commercial billing and factoring transaction records (RPOS Carmen) from incoming CSV files. It performs historical contract synchronization, splits data into distinct business streams (Factoring Invoices, Factoring Credit Notes, Reselling, and Temporary data), and executes an idempotent reload cycle across five target tables while updating operational audit logs.

---

## 2. Generated Artifacts

The migration process generated three primary artifacts. In accordance with the **Folder Integrity Rule**, their paths mirror the legacy repository structure:

### 1. Airflow DAG
* **Path**: `dags/abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import_dag.py`
* **Role**: Orchestrates the workflow. It defines the DAG structure, retrieves global environment variables from the Airflow Variable store, sets job-specific parameters, and submits the PySpark job to Dataproc Serverless.

### 2. PySpark ETL Application
* **Path**: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py`
* **Role**: Executes the core data transformation logic. It reads raw CSV data from GCS, sanitizes decimal formats, validates schemas, performs a temporal join with the contract master ledger (`dwh_ta_c_vertrag`), splits the records into five destination streams, and writes the outputs to BigQuery.

### 3. Python Wrapper Orchestration Script
* **Path**: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py`
* **Role**: Replaces the legacy KornShell wrapper script. It performs pre-flight environment validations, scans GCS for incoming files matching the file mask, submits the Dataproc Serverless batch job, parses the file footer metadata, and performs post-job auditing updates in BigQuery.

---

## 3. Key Design Decisions

### Idempotent Paired Reload Pattern (Left Anti-Join)
* **Decision**: Instead of executing standard SQL `DELETE` and `INSERT` statements directly in BigQuery, the PySpark application utilizes a **Left Anti-Join** pattern.
* **Reasoning**: BigQuery is optimized for analytical queries rather than frequent DML mutations. Running large-scale `DELETE` operations can hit rate limits and degrade performance. The PySpark script reads the existing target table, performs a left anti-join against the incoming keys to filter out matching records, unions the remaining records with the new dataset, and overwrites the target table. This ensures atomic, transactional updates and bypasses BigQuery DML limitations.

### Validation Crash Strategy (Fail-Fast)
* **Decision**: Legacy Ab Initio `force_error()` calls have been mapped to PySpark's `F.raise_error()` function.
* **Reasoning**: If critical fields (such as `monats_id`, `rechnung_datum`, or `vertrags_id`) contain malformed data, the job must fail immediately to prevent downstream data corruption. Using `F.raise_error()` ensures that Spark aborts the execution, allowing Airflow to capture the failure and alert operators.

### Temporal Proof Join
* **Decision**: The contract master ledger (`dwh_ta_c_vertrag`) is deduplicated by partitioning over `vertrag_id_carmen` and ordering by `gueltig_von DESC` and `dwh_vertrag_id DESC` (taking the first rank). The transaction's month-end date is then evaluated against the contract's active window (`gueltig_von` and `gueltig_bis`).
* **Reasoning**: This preserves the exact business logic of the legacy Ab Initio "Proof Join" component, ensuring that transactions are only associated with active, valid contracts.

### Decoupled Execution
* **Decision**: The legacy KornShell script's responsibilities have been split. Airflow handles scheduling and high-level orchestration, while the Python wrapper script handles pre-flight checks, file discovery, and post-job metadata updates.
* **Reasoning**: This separation of concerns keeps the Airflow DAG lightweight and prevents long-running file parsing or API polling operations from blocking Airflow worker slots.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target environment before triggering the migrated workflow:

### 1. GCS Bucket Structure
Create the following directory structure within your designated GCS bucket (`gs://{GCS_BUCKET}/`):
* `gs://{GCS_BUCKET}/crs/work/` (Staging directory for incoming files)
* `gs://{GCS_BUCKET}/crs/store/` (Archive directory for processed files)
* `gs://{GCS_BUCKET}/pyspark/` (Directory for storing the PySpark ETL script)

### 2. BigQuery Dataset & Tables
Ensure that the target BigQuery dataset (configured via `BQ_DATASET`) exists and contains the following tables with schemas matching the legacy Oracle definitions:
* `ta_f_rpos_carm`
* `ta_f_rpos_fact_carm`
* `ta_f_gpos_fact_carm`
* `ta_f_rpos_reselling_carm`
* `ta_t_rpos_carm`
* `ta_c_vertrag` (Must be pre-populated with contract master data)
* `ta_k_rech_absgrp`
* `ta_k_meldungen`

### 3. IAM & Permissions
The Service Account executing the Cloud Composer DAG and Dataproc Serverless jobs must be granted the following IAM roles:
* **Dataproc Editor** (`roles/dataproc.editor`)
* **Storage Admin** (`roles/storage.admin`) on the GCS bucket
* **BigQuery Admin** (`roles/bigquery.admin`) on the target dataset

### 4. Airflow Variables
Configure the following keys in the Airflow Variable Store:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-project` | Target GCP Project ID |
| `GCP_REGION` | `europe-west3` | GCP Region for Composer/Dataproc |
| `DATAPROC_CLUSTER` | `dataproc-serverless-templates` | Dataproc cluster/template name |
| `GCS_BUCKET` | `prod-dwh-billing-bucket` | Primary GCS bucket name |
| `TEMP_GCS_BUCKET` | `prod-dwh-temp-bucket` | Temporary GCS bucket for BigQuery writes |
| `DATAPROC_REGION` | `europe-west3` | Dataproc Serverless execution region |

### 5. Artifact Deployment
1. Upload `map_rpos_carmen_import.py` (the PySpark script) to `gs://{GCS_BUCKET}/pyspark/`.
2. Deploy `dw_rpos_carm_import_dag.py` to the Airflow `dags/` folder.

---

## 5. Known Gaps & Unresolved References

### 1. Standalone DAG Orchestration (Missing Parent JOBP)
* **Gap**: The legacy UC4 extraction did not include the parent Job Plan (`JOBP`) or master schedule.
* **Status**: The migrated Airflow DAG is currently configured with `schedule_interval=None`. It must be triggered manually, via an external file sensor, or integrated into a master DAG once the parent workflow is migrated.

### 2. Human-Confirmed Exemptions
The following legacy scripts and utilities were confirmed by human review as **not needed** and have been omitted from the migration:
* `.CCR_INIT` / `.DW_INIT` (Superseded by Airflow environment variables)
* `AB_CATALOG_FUNCTIONS.KSH` (Replaced by native Python/Spark functions)
* `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` / `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` (Replaced by Airflow task lifecycle states)
* `DW.HOLE_PFAD` / `DW.LESE_LOG` (Replaced by native GCS and Cloud Logging APIs)
* `H_ALIS_*` utility scripts (Deprecated legacy reporting frameworks)

### 3. BigQuery Table Partitioning (B4 Redesign Item)
* **Gap**: The current PySpark implementation uses a full table read, left anti-join, and overwrite pattern. While functional, this pattern will experience performance degradation as the target tables grow.
* **Redesign Recommendation**: It is highly recommended to partition the target BigQuery tables (e.g., `ta_f_rpos_carm`) by `rechnung_datum`. The PySpark script should be updated to overwrite specific partitions dynamically rather than performing full table overwrites.

---

## 6. Validation

To validate the migrated pipeline, execute the following test suite:

### 1. Dry-Run PySpark Validation
Run the PySpark script locally or on a development Dataproc cluster using a small sample dataset to verify schema parsing and validation rules:
```bash
python3 map_rpos_carmen_import.py \
  --filename=CARMEN_B_TEST_pos.fix \
  --resolved_dir=crs/work/ \
  --gcs_bucket={YOUR_TEST_BUCKET}
```

### 2. End-to-End Integration Test
1. Place a mock billing file named `CARMEN_B_20260421_pos.fix` into `gs://{GCS_BUCKET}/crs/work/`.
2. Trigger the `dw_rpos_carm_import` DAG manually from the Airflow UI.
3. Monitor the Airflow task logs and Dataproc batch logs for successful execution.

### 3. Definition of "Passing"
The migration is considered successful and ready for production when:
* The Airflow DAG and Dataproc Serverless tasks complete with `SUCCESS` status.
* The input file is successfully moved from `gs://{GCS_BUCKET}/crs/work/` to `gs://{GCS_BUCKET}/crs/store/`.
* Data is correctly routed to the five target BigQuery tables based on the business rules (e.g., records with `rpos_geschaftsform_kenn = 'F'` exist only in `ta_f_rpos_fact_carm`).
* The audit tables `ta_k_meldungen` and `ta_k_rech_absgrp` are updated with the correct record counts, file names, and processing timestamps matching the file's footer.

---

## 7. Rollback Procedure

In the event of a critical failure during or immediately after go-live, execute the following steps to revert to the pre-migration state:

### Step 1: Pause the Airflow DAG
Immediately pause the DAG in the Airflow UI or via the CLI to prevent further executions:
```bash
airflow dags pause dw_rpos_carm_import
```

### Step 2: Restore BigQuery Tables
Restore the target tables to their state prior to the failed run using BigQuery's time-travel feature or table snapshots:
```sql
-- Example: Restore ta_f_rpos_carm to its state 1 hour ago
CREATE OR REPLACE TABLE `bq_dataset.ta_f_rpos_carm`
AS SELECT * FROM `bq_dataset.ta_f_rpos_carm`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```
*Repeat this query for all five target tables and the two audit tables.*

### Step 3: Revert GCS Files
If the input file was archived, move it back to the staging directory to allow for reprocessing:
```bash
gsutil mv gs://{GCS_BUCKET}/crs/store/CARMEN_B_*_pos.fix gs://{GCS_BUCKET}/crs/work/
```