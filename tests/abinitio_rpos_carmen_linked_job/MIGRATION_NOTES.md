# Migration Notes: DW.RPOS_CARM_IMPORT

This document details the migration of the legacy UC4 job `DW.RPOS_CARM_IMPORT`, its associated Ab Initio graph `map_rpos_carmen_import.mp`, and its KornShell wrapper `map_rpos_carmen_import.ksh` to a cloud-native architecture on Google Cloud Platform (GCP).

---

## 1. Summary

The `DW.RPOS_CARM_IMPORT` workflow has been migrated from an on-premises Ab Initio and UC4 environment to **Google Cloud Platform (GCP)**. 

* **Source Orchestration:** UC4 (`JOBS_UNIX` object executing a KornShell wrapper).
* **Source Data Engine:** Ab Initio GDE (Graph `map_rpos_carmen_import.mp` executing via `mp run`).
* **Target Orchestration:** Google Cloud Composer (Apache Airflow 2.x).
* **Target Data Engine:** Dataproc Serverless (PySpark 3.x).
* **Target Data Warehouse:** Google BigQuery.

### Business Purpose
This pipeline ingests, validates, enriches, and routes Carmen RPOS billing and invoice transactional records. It performs lookups against contract master tables to align records with active contract periods, categorizes transactions into distinct business streams (Factoring Invoices, Factoring Credit Notes, Reselling, and Temporary positions), and enforces target ledger integrity via an idempotent "Delete-Then-Insert" pattern.

---

## 2. Generated Artifacts

The migration process generated the following core artifacts:

| Target File Path | Language / Type | Role / Description |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` | Python (Apache Airflow DAG) | Orchestrates the pipeline. It resolves environment variables, defines the execution DAG, and submits the PySpark job to Dataproc Serverless. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Python (PySpark 3.x) | The primary production-ready data pipeline. It replaces the legacy KSH wrapper and Ab Initio graph logic. It handles file ingestion, validation, contract enrichment, pre-load deletions, routing, and audit logging. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | Python (PySpark 3.x) | Consolidated/re-engineered PySpark module containing the core data transformation logic mapped directly from the Ab Initio `.mp` graph. |

---

## 3. Key Design Decisions

### A. Dataproc Serverless vs. Persistent Clusters
* **Decision:** Dataproc Serverless was selected to execute the PySpark pipeline.
* **Reasoning:** The import job is batch-oriented and triggered externally or on-demand. Dataproc Serverless eliminates the operational overhead of managing VM clusters, scales dynamically based on input file sizes, and ensures a zero-cost idle state.

### B. Idempotency via Pre-Load Deletes (Delete-Then-Insert)
* **Decision:** Re-implemented the legacy Ab Initio pre-load deletion logic using BigQuery `MERGE` statements with a `WHEN MATCHED THEN DELETE` clause.
* **Reasoning:** To prevent duplicate records upon job retries, the pipeline must clear existing records matching the natural keys (`rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`) of the incoming batch. Executing this via a staging table merge in BigQuery ensures transactional safety and high performance.

### C. Analytical Windowing for Scan/Ranking
* **Decision:** Replaced the legacy Ab Initio `Scan` component with PySpark Window functions.
* **Reasoning:** The legacy graph ranked contract records over `gueltig_von desc` and `dwh_vertrag_id desc` to resolve overlapping contract periods. This is natively and efficiently handled in PySpark using:
  ```python
  Window.partitionBy("vertrags_id", "rechnung_id", ...).orderBy(col("gueltig_von").desc(), col("dwh_vertrag_id").desc())
  ```

### D. Decoupled Environment Configuration
* **Decision:** Implemented a strict environment variable classification policy.
* **Reasoning:** To ensure seamless portability across Dev, Test, and Prod environments, all infrastructure-specific parameters (GCP Project, GCS Bucket, BigQuery Dataset) are resolved dynamically at runtime via Airflow Variables. Job-specific constants (e.g., record identifiers like `H`, `P`, `X`) remain encapsulated within the job configuration.

### E. Verbatim German Logging Preservation
* **Decision:** All logging statements, audit messages, and console outputs from the legacy KornShell script and Ab Initio graph have been preserved verbatim in German.
* **Reasoning:** This maintains operational continuity, allowing existing downstream log parsers and operations teams to monitor the pipeline without modifying their patterns.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target GCP environment prior to executing the pipeline:

### A. BigQuery Schema and Dataset Creation
Ensure the target BigQuery dataset (defined by the `BQ_DATASET` variable) exists and contains the following tables with schemas matching the legacy Oracle definitions:
* `DWH$TA_C_VERTRAG` (Contract Master Lookup Table)
* `DWH$TA_F_RPOS_CARM` (General Invoice Positions)
* `DWH$TA_F_GPOS_FACT_CARM` (Factoring Gross Positions)
* `DWH$TA_F_RPOS_FACT_CARM` (Factoring Billing Positions)
* `DWH$TA_F_RPOS_RESELLING_CARM` (Reselling Positions)
* `DWH$TA_T_RPOS_CARM` (Temporary Invoicing Positions)
* `DWH$TA_K_RECH_ABSGRP` (Reconciliation Log Table)
* `DWH$TA_K_MELDUNGEN` (Job Metrics Log Table)

### B. IAM & Permissions
The Cloud Composer Service Account and the Dataproc Serverless Execution Service Account must be granted the following IAM roles:
* **Storage:** `roles/storage.objectAdmin` on the GCS bucket.
* **BigQuery:** `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the target dataset and project.
* **Dataproc:** `roles/dataproc.worker` and `roles/dataproc.editor`.

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment:
* `GCP_PROJECT`: The target GCP Project ID.
* `GCP_REGION`: The target GCP Region (e.g., `europe-west3`).
* `GCS_BUCKET`: The GCS bucket name used for staging and archiving (e.g., `my-company-dwh-bucket`).
* `BQ_DATASET`: The target BigQuery dataset name (e.g., `dwh_billing`).
* `DATAPROC_CLUSTER` *(Optional)*: Set only if executing on a persistent Dataproc cluster instead of Serverless.

### D. GCS Directory Structure & Artifact Deployment
1. Create the following directory structure in your GCS bucket:
   ```
   gs://{GCS_BUCKET}/pyspark_scripts/
   gs://{GCS_BUCKET}/crs/work/
   gs://{GCS_BUCKET}/crs/store/
   ```
2. Upload the PySpark script `map_rpos_carmen_import.py` to `gs://{GCS_BUCKET}/pyspark_scripts/`.
3. Deploy the Airflow DAG `dw_rpos_carm_import.py` to the Composer DAGs folder.

---

## 5. Known Gaps & Unresolved References

### A. Standalone DAG Orchestration
* **Status:** The legacy UC4 job was exported as a standalone object without its parent workflow (`JOBP`) or scheduler.
* **Resolution:** The migrated DAG is currently configured with `schedule=None` (triggered manually or externally). If this job must run as part of a larger daily/monthly sequence, it should be integrated into an orchestrating parent DAG or triggered via an `ExternalTaskSensor`.

### B. Retired Legacy Utilities
* **Status:** Legacy shell utilities and includes (`.dw_init`, `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`, `DW.HOLE_PFAD`, `DW.LESE_LOG`, `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`) have been retired.
* **Resolution:** These utilities handled legacy log parsing, path detection, and environment initialization. In the target architecture, these are handled natively by Cloud Composer logging, Airflow task lifecycles, and GCS file detection.

### C. Staging Table Concurrency
* **Status:** The PySpark script utilizes a staging table named `DWH_TA_STAGE_RPOS_CARM` to perform pre-load delete comparisons.
* **Resolution:** To prevent write collisions if multiple instances of this job run concurrently, `max_active_runs` is set to `1` in the Airflow DAG.

---

## 6. Validation

To validate the migrated pipeline, perform the following steps:

### A. How to Run the Test
1. Place a sample Carmen RPOS flat file (e.g., `CARMEN_B_test_pos.fix`) into the GCS input directory: `gs://{GCS_BUCKET}/crs/work/`.
2. Trigger the Airflow DAG `dw_rpos_carm_import` manually from the Airflow UI, or execute the PySpark script directly via the gcloud CLI:
   ```bash
   gcloud dataproc batches submit pyspark \
       gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py \
       --project {GCP_PROJECT} \
       --region {GCP_REGION} \
       --env-vars GCP_PROJECT={GCP_PROJECT},GCS_BUCKET={GCS_BUCKET},BQ_DATASET={BQ_DATASET} \
       -- \
       "gs://{GCS_BUCKET}/crs/work/CARMEN_B_test_pos.fix" "12345"
   ```

### B. What "Passing" Looks Like
A successful validation run must meet the following criteria:
1. **Airflow Task Status:** The task `dw_rpos_carm_import_task` completes with a `SUCCESS` status.
2. **File Archival:** The input file is successfully moved from `gs://{GCS_BUCKET}/crs/work/` to `gs://{GCS_BUCKET}/crs/store/`.
3. **Idempotency Check:** If the same file is processed twice, no duplicate records are created in the target BigQuery tables (verified by checking row counts and `ladedatum` timestamps).
4. **Data Routing:** Records are correctly routed to target tables based on business rules:
   * Factoring Invoices (`decoded_geschaftsform = 'F'`) are written to `DWH$TA_F_RPOS_FACT_CARM`.
   * Factoring Credit Notes (`decoded_geschaftsform = 'G'`) are written to `DWH$TA_F_GPOS_FACT_CARM`.
   * Reselling Positions (`decoded_geschaftsform = 'R'`) are written to `DWH$TA_F_RPOS_RESELLING_CARM`.
   * All records are appended to the general table `DWH$TA_F_RPOS_CARM`.
5. **Audit Logging:** 
   * `DWH$TA_K_RECH_ABSGRP` contains a new or updated row with the correct `monats_id`, `dateiname`, and `ladedatum`.
   * `DWH$TA_K_MELDUNGEN` has its `anzahl_ds_eof` updated to match the exact number of payload records processed.

---

## 7. Rollback Procedure

If a critical failure occurs during deployment or post-go-live execution, execute the following rollback steps:

### Step 1: Pause the Airflow DAG
Immediately pause the Airflow DAG to prevent subsequent scheduled or automated executions:
```bash
airflow dags pause dw_rpos_carm_import
```

### Step 2: Revert GCS Files
If a file was partially processed or failed mid-execution, move the source file from the archive directory back to the active work directory:
```bash
gsutil mv gs://{GCS_BUCKET}/crs/store/CARMEN_B_failed_file_pos.fix gs://{GCS_BUCKET}/crs/work/
```

### Step 3: Clean Up Corrupted Target Data
If the PySpark job failed after executing deletes but before completing the inserts (or vice versa), use BigQuery to remove the corrupted batch data using the load timestamp (`ladedatum`):
```sql
DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.DWH$TA_F_RPOS_CARM` 
WHERE TIMESTAMP_TRUNC(ladedatum, MINUTE) = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE);
```
*(Repeat this delete query for all affected target tables using the appropriate timestamp or batch identifier).*

### Step 4: Re-enable Legacy UC4 Job
If reverting completely to the on-premises environment:
1. Re-enable the legacy UC4 job `DW.RPOS_CARM_IMPORT`.
2. Ensure the on-premises database connections and Ab Initio filesystems are active and synchronized.