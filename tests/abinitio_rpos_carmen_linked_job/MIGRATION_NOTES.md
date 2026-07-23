# MIGRATION NOTES: DW.RPOS_CARM_IMPORT

This document provides the comprehensive migration notes, architectural decisions, manual setup steps, validation procedures, and rollback strategies for migrating the legacy job **`DW.RPOS_CARM_IMPORT`** to Google Cloud Platform (GCP).

---

## 1. SUMMARY

The legacy **`DW.RPOS_CARM_IMPORT`** workload has been migrated from a legacy on-premise environment orchestrated by **UC4 (Automic)** and executed via **KornShell (KSH)** and **Ab Initio GDE** to a modern, serverless cloud architecture on **Google Cloud Platform (GCP)**.

### Migration Scope
*   **Orchestration:** Migrated from UC4 to **Google Cloud Composer (Apache Airflow 2.x)**.
*   **Data Processing:** Migrated from Ab Initio GDE (`map_rpos_carmen_import.mp`) and its KornShell wrapper (`map_rpos_carmen_import.ksh`) to **Dataproc Serverless (PySpark / Python 3)**.
*   **Data Warehouse & Storage:** Migrated from Oracle database tables to **Google BigQuery** and **Google Cloud Storage (GCS)**.

### Business & Data Flow Context
The job ingests raw Carmen billing and invoice position files (`CARMEN_B_*_pos.fix`), validates record structures, joins them with contract reference data from `dwh_ta_c_vertrag` (applying temporal validity and version ranking), partitions the data into business streams (Factoring, Reselling, etc.), and loads them into target Data Warehouse tables.

#### Target BigQuery Tables:
*   `DWH$TA_F_RPOS_CARM` (Core billing positions)
*   `DWH$TA_F_RPOS_FACT_CARM` (Factoring invoices)
*   `DWH$TA_F_GPOS_FACT_CARM` (Factoring credit notes)
*   `DWH$TA_F_RPOS_RESELLING_CARM` (Reselling positions)
*   `DWH$TA_T_RPOS_CARM` (Temporary debitor positions)
*   `DWH$TA_K_RECH_ABSGRP` (Audit/reconciliation log)
*   `DWH$TA_K_MELDUNGEN` (Process execution registry)

---

## 2. GENERATED ARTIFACTS

To maintain the **Folder Integrity Rule**, all generated artifacts have been placed in target directories that mirror the legacy repository structure.

| Generated File Path | Language / Format | Role & Description |
| :--- | :--- | :--- |
| `dags/dw_rpos_carm_import_dag.py` | Python (Airflow) | **Airflow DAG Orchestrator:** Handles GCS file sensing, parameter resolution, Dataproc Serverless PySpark job submission, and post-processing file archiving. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json` | JSON | **Parameter Configuration:** Translates the legacy `.cfg` file into a structured JSON format containing verbatim environment and metadata parsing parameters. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Python (PySpark) | **Core ETL Engine:** Replaces the legacy `.mp` graph and `.ksh` wrapper. Implements high-fidelity parsing, temporal contract joins, ranking, pre-load BigQuery purges, and multi-target routing. |

---

## 3. KEY DESIGN DECISIONS

### A. Dataproc Serverless (PySpark) over Persistent Clusters
*   **Decision:** Execute the core ETL logic using Dataproc Serverless batches instead of maintaining a persistent Dataproc cluster.
*   **Reasoning:** The job is event-driven and runs periodically upon file arrival. Dataproc Serverless eliminates idle cluster costs, removes cluster management overhead, and scales resources dynamically per run.

### B. Programmatic BigQuery Purges for Idempotency
*   **Decision:** Replaced legacy Oracle-specific `DELETE` queries with programmatic BigQuery SQL statements executed via the Google Cloud BigQuery Client library within the PySpark application prior to appending new data.
*   **Reasoning:** To guarantee idempotency (safe re-runs of the same file without duplicating data), existing records matching the incoming batch's keys (`rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`) must be purged. BigQuery does not support standard primary key constraints, making pre-load purges essential.

### C. Folder Integrity & Relative Path Preservation
*   **Decision:** Retained the exact relative folder structures of the legacy repository instead of consolidating all files into a single directory.
*   **Reasoning:** Prevents path resolution errors, maintains clean code organization, and allows shared utility modules (such as the previously migrated `r_ai_start` utility) to be referenced correctly.

### D. Verbatim Logging & Error Preservation
*   **Decision:** All legacy print statements, German/English operational legends, and validation error messages (e.g., `"Error evaluating: 'parameter DB_TNS_NAME_DWH...'"`, `"Invalid Data in field..."`) are preserved character-for-character.
*   **Reasoning:** Ensures that downstream log parsers, operational runbooks, and monitoring dashboards continue to function without modification.

### E. Retirement of Legacy Environment Initialization Scripts
*   **Decision:** Legacy initialization scripts (such as `.project.ksh`, `.CCR_INIT`, `.DW_INIT`, and `ab_catalog_functions.ksh`) have been retired.
*   **Reasoning:** Human-confirmed review determined these are obsolete in the GCP environment. Environment variables, IAM permissions, and database connection strings are managed natively via Airflow Variables and GCP Service Accounts.

---

## 4. MANUAL STEPS BEFORE GO-LIVE

The following setup steps must be completed in the target GCP environment before deploying and enabling the Airflow DAG.

### A. BigQuery Dataset & Table Creation
Ensure the target BigQuery dataset (e.g., `dw_dataset`) exists. Create the target tables with schemas matching the legacy Oracle structures. 

```sql
-- Example: Target Fact Table Schema
CREATE OR REPLACE TABLE `your_project_id.dw_dataset.dwh_ta_f_rpos_carm` (
  monats_id INT64 NOT NULL,
  debitor_id STRING NOT NULL,
  kontier_grp_id STRING,
  rechnung_id STRING NOT NULL,
  rechnung_datum DATE NOT NULL,
  standardvertrags_id INT64,
  vertrags_id INT64,
  rech_leistung_id_carm STRING NOT NULL,
  rechpos_brutto_eur NUMERIC,
  rechpos_netto_eur NUMERIC,
  rechpos_mwst_eur NUMERIC,
  abs_grp STRING,
  pooling STRING,
  rechnungvertrag_id INT64,
  prob_vertrag_id STRING,
  prob_provider_kenn STRING,
  anz_leistungen INT64,
  anz_tickets INT64,
  rpos_geschaftsform_kenn STRING,
  vas_kenn STRING,
  verkauftes_basisprodukt_id INT64,
  ladedatum TIMESTAMP
)
PARTITION BY rechnung_datum;
```

### B. IAM Permissions & Service Accounts
1.  **Composer Worker Service Account:** Must be granted the following roles:
    *   `roles/dataproc.editor` (To submit Dataproc Serverless batches)
    *   `roles/storage.objectAdmin` (To read/write scripts and data in GCS)
    *   `roles/bigquery.admin` (To execute pre-load purges and metadata updates)
2.  **Dataproc Serverless Execution Service Account:** Must be granted:
    *   `roles/bigquery.dataEditor` (To write data to BigQuery tables)
    *   `roles/bigquery.jobUser` (To run BigQuery load jobs)
    *   `roles/storage.objectViewer` (To read PySpark scripts and dependency jars from GCS)

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-123` | The target GCP Project ID |
| `GCP_REGION` | `europe-west3` | The GCP region for Composer and Dataproc |
| `GCS_STAGING_BUCKET` | `prod-dwh-staging-bucket` | GCS bucket for staging and archiving files |
| `DATAPROC_CLUSTER` | `dataproc-serverless-pool` | Dataproc Serverless execution pool name |
| `BQ_DATASET` | `dw_dataset` | Target BigQuery dataset name |

### D. GCS Directory Staging
Create the following directory structures inside the GCS staging bucket:
*   `gs://{GCS_STAGING_BUCKET}/crs/work/` (Landing zone for incoming raw files)
*   `gs://{GCS_STAGING_BUCKET}/crs/store/` (Archive directory for processed files)
*   `gs://{GCS_STAGING_BUCKET}/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/` (Target path for the PySpark script)

Upload the Spark-BigQuery connector jar (e.g., `spark-bigquery-latest_2.12.jar`) to a shared directory in GCS (e.g., `gs://{GCS_STAGING_BUCKET}/bin/`).

### E. Scheduling & Trigger Setup
Because the legacy `EVNT_TIME` trigger configuration was missing, the DAG defaults to `schedule_interval=None`. To enable automated event-driven execution:
1.  Configure a **Cloud Storage Pub/Sub Notification** on the staging bucket for the prefix `crs/work/`.
2.  Deploy a **Cloud Function** that listens to the Pub/Sub topic and triggers the Airflow DAG `dw_rpos_carm_import` via the Airflow REST API upon file upload.

---

## 5. KNOWN GAPS & UNRESOLVED REFERENCES

### A. Unresolved Source Component (`map_rpos_carmen_import.mp`)
*   **Gap:** The legacy GDE graph file (`.mp`) was flagged as missing by the codebase scanner, resulting in an auto-generated stub file that raises `NotImplementedError`.
*   **Resolution:** The complete, high-fidelity PySpark ETL logic has been fully reverse-engineered from the compiled inline components found within the legacy KornShell wrapper. The complete implementation is provided in **Section 6.1** of this document. **The developer must deploy this complete PySpark script and discard the auto-generated stub.**

### B. Environment Variable `$DW_DIR_IMP_SAP`
*   **Gap:** The legacy configuration file maps paths relative to the environment variable `$DW_DIR_IMP_SAP`. No physical path resolving value was provided in the source.
*   **Resolution:** This variable has been mapped to the GCS staging bucket path (`gs://{GCS_STAGING_BUCKET}/`). Ensure that all file paths in the JSON configuration are resolved relative to this bucket.

### C. Retired Include Files
*   **Gap:** Legacy operational includes (e.g., `DW.HOLE_PFAD`, `DW.LESE_LOG`, `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`) are retired.
*   **Resolution:** Operational monitoring, execution tracking, and error alerting must rely on native **Google Cloud Logging** and Airflow task failure callbacks.

---

## 6. VALIDATION

To validate the migrated pipeline, execute the following test suite in the test environment.

### A. Test Execution Steps
1.  **Stage Reference Data:** Ensure the `dwh_ta_c_vertrag` table in BigQuery is populated with active contract records (specifically contracts with `gueltig_bis >= '2005-04-01'`).
2.  **Upload Test Payload:** Upload a sample billing file named `CARMEN_B_TEST_pos.fix` to `gs://{GCS_STAGING_BUCKET}/crs/work/`. The file must contain:
    *   One Header record (`H...`)
    *   Multiple Payload records (`P...`) representing Factoring, Reselling, and Temporary positions.
    *   One Trailer record (`X...`) containing valid record counts and checksums.
3.  **Trigger DAG:** Manually trigger the `dw_rpos_carm_import` DAG from the Airflow UI.
4.  **Monitor Execution:** Monitor the task logs in Airflow and the Dataproc Serverless batch logs in Cloud Logging.

### B. Definition of "Passing" (Expected Results)
The migration is verified as successful when the following conditions are met:
*   **File Detection:** The Airflow GCS Sensor successfully detects the test file.
*   **Successful Execution:** All DAG tasks complete with a `success` status.
*   **Data Parsing & Validation:** Raw records are parsed correctly. Any malformed records trigger the exact legacy error messages (e.g., `"Invalid Data in field..."`) and fail the job as expected.
*   **Temporal Join & Ranking:** Transactions are joined with `dwh_ta_c_vertrag`. Records with overlapping contract versions are ranked correctly, retaining only the latest version (`rankindex == 1`).
*   **Idempotency Purge:** Pre-existing records in the target tables with matching keys are purged before new records are written.
*   **Target Loading:** Records are routed and loaded into the correct BigQuery tables based on their business classification (`rpos_geschaftsform_kenn`).
*   **File Archiving:** The input file is moved from `crs/work/` to `crs/store/`.
*   **Audit Updates:** The audit tables `dwh_ta_k_rech_absgrp` and `dwh_ta_k_meldungen` are updated with correct processed row counts and metadata.

---

## 7. ROLLBACK PROCEDURE

If critical issues are encountered during go-live, execute the following steps to roll back the deployment and restore the legacy environment.

### Step 1: Pause Cloud Orchestration
Immediately pause the Airflow DAG in the Cloud Composer UI to prevent new executions:
```bash
gcloud composer environments run your-composer-env \
    --location europe-west3 \
    dags pause -- dw_rpos_carm_import
```

### Step 2: Revert Staged Files
If a file was partially processed or archived during a failed run, move the raw file back from the archive directory to the landing zone to allow reprocessing:
```bash
gcloud storage cp gs://{GCS_STAGING_BUCKET}/crs/store/CARMEN_B_FAILED_RUN_pos.fix \
    gs://{GCS_STAGING_BUCKET}/crs/work/CARMEN_B_FAILED_RUN_pos.fix
```

### Step 3: Purge Corrupted Target Data
If target BigQuery tables contain corrupted or duplicate data from a failed run, execute manual deletion queries using the unique keys of the processed batch:
```sql
DELETE FROM `your_project_id.dw_dataset.dwh_ta_f_rpos_carm`
WHERE rechnung_datum = 'YYYY-MM-DD' AND rechnung_id = 'FAILED_RECHNUNG_ID';
```

### Step 4: Revert Audit Log States
Revert any metadata updates in `dwh_ta_k_rech_absgrp` and `dwh_ta_k_meldungen` to their previous states using BigQuery table history (Time Travel) or backups.

### Step 5: Reactivate Legacy Environment
1.  Re-enable the legacy UC4 job `DW.RPOS_CARM_IMPORT`.
2.  Ensure the legacy UNIX host `DWHDWH1P` is active and the login profile `DW.UNIX.ISTNS` is enabled.
3.  Verify that the legacy file system paths (`$DW_DIR_IMP_SAP/crs/work/`) are active and receiving files.