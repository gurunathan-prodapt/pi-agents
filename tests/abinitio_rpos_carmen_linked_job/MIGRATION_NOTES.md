# Migration Notes: DW.RPOS_CARM_IMPORT

This document provides comprehensive migration notes for transitioning the legacy UC4 UNIX job `DW.RPOS_CARM_IMPORT` and its associated Ab Initio graph `map_rpos_carmen_import` to a modern, cloud-native architecture on Google Cloud Platform (GCP).

---

## 1. Summary

The `DW.RPOS_CARM_IMPORT` workflow has been migrated from a legacy on-premises environment orchestrated by UC4/Automic and processed via Ab Initio to **Google Cloud Composer (Apache Airflow)** and **Google Cloud Dataproc Serverless (PySpark)**. 

### Scope of Migration
*   **Source Orchestrator:** UC4 UNIX Job (`DW.RPOS_CARM_IMPORT`)
*   **Source Processing Engine:** Ab Initio Graph (`map_rpos_carmen_import.mp`) and KornShell Wrapper (`map_rpos_carmen_import.ksh`)
*   **Target Orchestrator:** Apache Airflow (Cloud Composer)
*   **Target Processing Engine:** PySpark (Dataproc Serverless)
*   **Target Database:** Google Cloud BigQuery (replacing Oracle DWH)
*   **Target File System:** Google Cloud Storage (GCS) (replacing local SAN mounts)

The pipeline ingests raw retail point of sale (RPOS) Carmen billing flat files, performs strict schema validation, enriches the transactions with historical contract metadata, executes temporal boundary checks, and loads the processed records into BigQuery using an idempotent **Delete-then-Insert** pattern.

---

## 2. Generated Artifacts

The migration process generated three primary files, each serving a distinct role in the target architecture:

| Artifact Path | Language / Type | Role & Description |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` | Python (Airflow DAG) | **Orchestration Layer:** Defines the Airflow DAG structure, default arguments, and schedules the Dataproc PySpark job. It maps legacy UC4 parameters to Airflow variables. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | PySpark (Python 3) | **ETL Processing Layer:** Replaces the Ab Initio `.mp` graph. Handles GCS file ingestion, strict data validation, left-outer joins with BigQuery reference tables, ranking/deduplication, and transactional target writes. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Python 3 | **Wrapper / Standalone Orchestrator:** Replaces the legacy `.ksh` wrapper. Provides a command-line interface to execute validations, manage pre-insert BigQuery deletions, and trigger the PySpark pipeline. |

---

## 3. Key Design Decisions

### Standalone DAG Architecture
Because the source extraction bundle contained no parent workflow (`JOBP`) or active schedule (`JSCH`), the migrated DAG is configured as **externally triggered** (`schedule_interval=None`). It is designed to be kicked off by an upstream file-arrival sensor or an external event trigger when a new billing file lands in GCS.

### Dataproc Serverless (PySpark) for Scale
Ab Initio's high-performance data processing has been mapped to **PySpark on Dataproc Serverless**. This eliminates the need to manage persistent cluster infrastructure, reduces operational overhead, and provides elastic scaling to handle large billing batches.

### Idempotent Delete-then-Insert Pattern
To prevent duplicate records on historical reruns, the pipeline identifies unique keys in the incoming batch and deletes matching records from target tables before appending new data. 

### STRUCT UNNEST Optimization for BigQuery Deletes
Standard single-row DML deletes are highly inefficient in BigQuery and can hit rate limits. To optimize this, the PySpark script aggregates unique deletion keys into an array of structs and executes a single batch `DELETE` query using `STRUCT UNNEST`:
```sql
DELETE FROM `project.dataset.table` t
WHERE EXISTS (
    SELECT 1 FROM UNNEST([STRUCT(...), STRUCT(...)]) k
    WHERE t.key = k.key
)
```
This design decision significantly reduces BigQuery slot consumption and ensures transactional consistency.

### ANSI SQL Translation
Oracle-specific outer join syntax `(+)` used in the contract history lookup has been rewritten to standard ANSI `LEFT OUTER JOIN` in BigQuery.

---

## 4. Manual Steps Before Go-Live

Before deploying the DAG and running the pipeline in production, the following manual setup steps must be completed:

### A. BigQuery Dataset & Schema Creation
Ensure the target BigQuery dataset (configured via `BQ_DATASET`) exists. Create the following tables with schemas matching the legacy Oracle definitions:
*   `DWH_TA_F_RPOS_CARM` (Core Fact Table)
*   `DWH_TA_F_RPOS_FACT_CARM` (Factoring Invoices)
*   `DWH_TA_F_GPOS_FACT_CARM` (Factoring Credit Notes)
*   `DWH_TA_F_RPOS_RESELLING_CARM` (Reselling)
*   `DWH_TA_T_RPOS_CARM` (Temporary Billing Positions)
*   `DWH_TA_C_VERTRAG` (Contract Reference Table - must be populated with active contract data)
*   `DWH_TA_K_RECH_ABSGRP` (Billing Status Log)
*   `DWH_TA_K_MELDUNGEN` (System Audit Log)

### B. GCS Bucket Structure
Create the GCS bucket configured in `GCS_BUCKET` and establish the following directory structure:
*   `gs://{GCS_BUCKET}/crs/work/` (Landing zone for incoming `CARMEN_B_*_pos.fix` files)
*   `gs://{GCS_BUCKET}/crs/store/` (Archive zone for processed files)
*   `gs://{GCS_BUCKET}/pyspark_scripts/` (Deployment location for `map_rpos_carmen_import.py`)

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | GCP Project ID where resources reside. |
| `GCP_REGION` | `europe-west3` | GCP Region for Composer and Dataproc. |
| `DATAPROC_REGION` | `europe-west3` | Dataproc Serverless execution region. |
| `DATAPROC_CLUSTER` | `dataproc-ephemeral-cluster` | Target Dataproc cluster name (if not using serverless). |
| `GCS_BUCKET` | `my-billing-data-bucket` | GCS Bucket name for files and scripts. |
| `DW_DIR_IMP_SAP` | `gs://my-billing-data-bucket` | Base GCS path replacing legacy `$DW_DIR_IMP_SAP`. |
| `BQ_DATASET` | `billing_dwh` | Target BigQuery dataset name. |
| `BQ_LOCATION` | `EU` | BigQuery dataset geographical location. |

### D. IAM & Permissions
Ensure the Cloud Composer / Cloud Dataflow service account has the following IAM roles:
*   `BigQuery Admin` (or `BigQuery Data Editor` + `BigQuery Job User`)
*   `Storage Object Admin` (on the configured GCS bucket)
*   `Dataproc Worker`

---

## 5. Known Gaps & Unresolved References

### High-Frequency BigQuery Deletes (Redesign B4 Item)
While the `STRUCT UNNEST` batching pattern optimizes deletions, executing frequent DML deletes on BigQuery can still lead to table fragmentation and metadata overhead if run multiple times per hour. 
*   *Recommendation:* For extreme scale, transition this pipeline to a **BigQuery Partition Swap** or a **Merge Staging Table** pattern in a future phase.

### Audit Logging Integration
The legacy workflow logs audit tracking indicators to Oracle tables `DWH_TA_K_MELDUNGEN` and `DWH_TA_K_RECH_ABSGRP`. The migrated code emulates this via BigQuery updates.
*   *Follow-up:* Integrate these audit logs with **Google Cloud Logging** and **Cloud Monitoring** to align with modern cloud operations standards.

### Dynamic Trigger Configuration
The mechanism that passes the dynamic `BHB_Dateiname` and `BHB_Eintragsnr` from the upstream file-arrival event to the Airflow DAG run configuration (`dag_run.conf`) must be finalized during integration testing.

---

## 6. Validation

To validate the migrated pipeline, perform the following steps in a non-production environment:

### A. How to Run the Tests
1.  **Upload Test Data:** Place a sample billing file (e.g., `CARMEN_B_test_pos.fix`) into `gs://{GCS_BUCKET}/crs/work/`.
2.  **Trigger the DAG:** Manually trigger the Airflow DAG `dw_rpos_carm_import` with the following JSON configuration:
    ```json
    {
      "BHB_Dateiname": "CARMEN_B_test_pos.fix",
      "BHB_Eintragsnr": "99999"
    }
    ```
3.  **Monitor Execution:** Verify that the Airflow task completes successfully and check the Dataproc driver logs for any PySpark execution errors.

### B. What "Passing" Means
The migration is considered successful and ready for production when:
*   The Airflow DAG and Dataproc Serverless jobs complete with a `SUCCESS` status.
*   **Zero records are duplicated** in the target BigQuery tables.
*   The row counts in `DWH_TA_F_RPOS_CARM` match the record count specified in the input file's trailer record.
*   The audit tables `DWH_TA_K_RECH_ABSGRP` and `DWH_TA_K_MELDUNGEN` are updated with the correct run statistics and timestamp.
*   The input file is successfully archived or moved to the GCS store directory.

---

## 7. Rollback Procedure

If critical issues are encountered post-go-live, execute the following steps to roll back to the legacy environment:

1.  **Pause the Airflow DAG:**
    ```bash
    gcloud composer environments run [COMPOSER_ENV] \
        --location [REGION] dags pause -- dw_rpos_carm_import
    ```
2.  **Re-enable the UC4 Job:** Set the active flag to `1` on the UC4 `DW.RPOS_CARM_IMPORT` UNIX job.
3.  **Data Cleanup (If Partial Run Occurred):**
    If the migrated pipeline failed mid-transaction and left partial data in BigQuery:
    *   Identify the `rechnung_id` and `rechnung_datum` keys from the failed batch.
    *   Execute manual cleanup queries in BigQuery to remove the partially loaded records before restarting the legacy UC4 job:
        ```sql
        DELETE FROM `project.dataset.DWH_TA_F_RPOS_CARM` 
        WHERE rechnung_id = 'FAILED_BATCH_ID' AND rechnung_datum = 'FAILED_BATCH_DATE';
        ```