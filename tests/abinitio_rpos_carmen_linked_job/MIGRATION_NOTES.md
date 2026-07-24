# Migration Notes: DW.RPOS_CARM_IMPORT

This document provides comprehensive migration notes and operational guidelines for the transition of the **DW.RPOS_CARM_IMPORT** workflow from the legacy UC4 / Ab Initio platform to Google Cloud Platform (GCP).

---

## 1. Summary

The **DW.RPOS_CARM_IMPORT** workflow has been migrated from a legacy Unix-hosted Ab Initio environment orchestrated by UC4 to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**. 

### Scope of Migration
* **Source Platform:** Ab Initio GDE (Graphical Development Environment) running on host `DWHDWH1P` under UC4/Automic scheduler.
* **Target Platform:** Google Cloud Composer (Apache Airflow), Cloud Dataproc Serverless (PySpark), Google Cloud Storage (GCS), and Google Cloud BigQuery.
* **Business Purpose:** Ingests, normalizes, and validates daily CARMEN billing and invoice position (RPOS) flat files. It performs temporal contract matching against historical contract dimensions (`dwh_ta_c_vertrag`) and routes the processed records into distinct target tables based on business classifications (Factoring Rechnungen, Factoring Gutschriften, Reselling, and Temporary Positions). Finally, it updates operational audit logs and control tables.

---

## 2. Generated Artifacts

The migration process has generated the following target files, preserving the original repository layout to maintain folder integrity:

| Target File Path | Language / Format | Role / Description |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import_dag.py` | Python (Airflow DAG) | Orchestrates the entire workflow. Handles file detection, input validation, Dataproc Serverless batch submission, and post-execution file archiving. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Python (PySpark) | Core ETL pipeline. Parses raw GCS files, performs data validation, executes temporal joins with BigQuery contract dimensions, applies deduplication, routes records to target tables, and stages audit metadata. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json` | JSON | Verbatim translation of the legacy `.cfg` file parameters used by the PySpark script at runtime. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.yaml` | YAML | Alternative YAML representation of the job configurations for environment-specific parameterization. |

---

## 3. Key Design Decisions

### Decoupled Orchestration (No Local Imports in DAG)
To eliminate Airflow DAG parsing errors, the Airflow DAG file (`dw_rpos_carm_import_dag.py`) does not import any PySpark modules or business logic. It utilizes the `DataprocCreateBatchOperator` to submit the PySpark job to Dataproc Serverless, passing all configurations as command-line arguments.

### Idempotent Delete Strategy
To prevent duplicate data loads upon workflow re-runs, the PySpark script implements an idempotent loading strategy:
1. It extracts distinct business keys (`rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`) from the incoming batch.
2. It programmatically executes a BigQuery `DELETE` query against the target tables for those specific keys before appending the new records.
3. This ensures that re-running a failed or partial batch does not result in duplicate records.

### Staging & Merging for Audit Logs
Updating the operational control tables (`dwh_ta_k_meldungen` and `dwh_ta_k_rech_absgrp`) requires transactional updates. 
* The PySpark script writes the parsed footer metadata to temporary staging tables (`dwh_ta_k_meldungen_stage` and `dwh_ta_k_rech_absgrp_stage`).
* The Airflow DAG then executes BigQuery `MERGE` statements to upsert this metadata into the production control tables, ensuring transactional consistency.

### Preservation of Validation Messages
Legacy German-language validation error messages (e.g., `"Invalid data format in monats_id"`, `"T bedeutet temporärer Satz"`) have been retained verbatim in the PySpark code. This prevents breaking downstream monitoring, alerting, or reporting systems that parse these specific log strings.

---

## 4. Manual Steps Before Go-Live

Before deploying the migrated workflow to production, the following setup steps must be completed:

### 4.1 Schema & Dataset Creation
Ensure that the target BigQuery dataset exists and that all target tables are created with schemas matching the legacy Oracle DDLs:
* `dwh_ta_f_rpos_carm`
* `dwh_ta_f_gpos_fact_carm`
* `dwh_ta_f_rpos_fact_carm`
* `dwh_ta_f_rpos_reselling_carm`
* `dwh_ta_t_rpos_carm`
* `dwh_ta_c_vertrag` (Source contract dimension, must be pre-populated)
* `dwh_ta_k_rech_absgrp`
* `dwh_ta_k_meldungen`

### 4.2 IAM & Permissions
The Cloud Composer/Airflow worker service account and the Dataproc Serverless execution service account must be granted the following IAM roles:
* **Dataproc Serverless Service Account:**
  * `roles/dataproc.worker`
  * `roles/bigquery.dataEditor`
  * `roles/bigquery.jobUser`
  * `roles/storage.objectAdmin` (on the GCS staging bucket)
* **Composer Environment Service Account:**
  * `roles/dataproc.editor`
  * `roles/bigquery.jobUser`

### 4.3 Airflow Variables Configuration
The following Airflow Variables must be configured in the Cloud Composer environment:

```json
{
  "gcp_project": "your-gcp-project-id",
  "gcp_region": "europe-west3",
  "gcs_bucket": "your-gcs-data-bucket",
  "bq_dataset": "your_bigquery_dataset_name",
  "spark_service_account": "dataproc-serverless-sa@your-gcp-project-id.iam.gserviceaccount.com"
}
```

### 4.4 GCS Folder Structure & Artifact Deployment
1. Create the following folder structure in your GCS bucket:
   ```
   gs://{GCS_BUCKET}/code/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/
   gs://{GCS_BUCKET}/code/abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/
   gs://{GCS_BUCKET}/crs/work/
   gs://{GCS_BUCKET}/crs/store/
   ```
2. Upload `map_rpos_carmen_import.py` to the `run/` directory.
3. Upload `map_rpos_carmen_import.json` and `map_rpos_carmen_import.yaml` to the `cfg/bd_proc/` directory.
4. Upload the shared utility script `r_ai_start.py` to `gs://{GCS_BUCKET}/abinitio_pyspark_linked_job/isccr/abinitio/bin/`.

### 4.5 Scheduling Integration
Because the original `EVNT_TIME` scheduling file was not provided, the DAG is currently configured with `schedule_interval=None`. You must manually wire this DAG to its upstream trigger (e.g., using an `ExternalTaskSensor` or triggering it via a Cloud Storage event when the CARMEN flat file lands in `gs://{GCS_BUCKET}/crs/work/`).

---

## 5. Known Gaps & Unresolved References

* **Audit Entry ID (`entry_nr`):** The legacy workflow relies on a dynamic run parameter (`entrynr = :eintragsnr`) to update the audit table `dwh_ta_k_meldungen`. In the migrated Airflow DAG, this is passed via DAG Run configuration (`{{ dag_run.conf.get("entry_nr", "0") }}`). The upstream orchestrator must pass a valid `entry_nr` when triggering this DAG; otherwise, it defaults to `0`.
* **Downstream Consumers:** No downstream consumer jobs were identified in the provided metadata. If downstream jobs rely on the completion of this import, they must be manually configured to trigger after `dw_rpos_carm_import` completes.
* **Contract Table Partitioning:** The temporal join against `dwh_ta_c_vertrag` scans historical contract records. To optimize query performance and minimize BigQuery slot usage, it is highly recommended to partition `dwh_ta_c_vertrag` on the `gueltig_bis` column.

---

## 6. Validation

To validate the migration, execute the following test plan in a non-production environment:

### 6.1 How to Run the Tests
1. Place a sample CARMEN billing file (e.g., `CARMEN_B_test_pos.fix`) containing mock payload (`P`) and footer (`X`) records into `gs://{GCS_BUCKET}/crs/work/`.
2. Trigger the Airflow DAG manually via the Airflow UI or CLI:
   ```bash
   gcloud composer environments run {COMPOSER_ENV} \
       --location {REGION} \
       dags trigger -- dw_rpos_carm_import --conf '{"entry_nr": "9999"}'
   ```
3. Monitor the execution in the Airflow Graph View and check the Dataproc batch logs for any runtime exceptions.

### 6.2 What "Passing" Means
The test run is considered successful if:
* **Task Execution:** All Airflow tasks (`list_incoming_files`, `validate_inputs`, `run_map_rpos_carmen_import`, `archive_files`) complete with a `SUCCESS` status.
* **Idempotency:** Re-running the same DAG run does not result in duplicate records in the target BigQuery tables.
* **Data Routing:** Records are correctly routed to their respective target tables based on the `rpos_geschaftsform_kenn` and `typ` fields:
  * `rpos_geschaftsform_kenn = 'F'` -> `dwh_ta_f_rpos_fact_carm`
  * `rpos_geschaftsform_kenn = 'G'` -> `dwh_ta_f_gpos_fact_carm`
  * `rpos_geschaftsform_kenn = 'R'` -> `dwh_ta_f_rpos_reselling_carm`
  * `typ = 'T'` -> `dwh_ta_t_rpos_carm`
* **Audit Reconciliation:** 
  * The `dwh_ta_k_meldungen` table contains a record for `entrynr = 9999` with `anzahl_ds_eof` matching the exact count of payload records processed.
  * The `dwh_ta_k_rech_absgrp` table is updated with the correct `rechnung_datum` and `ladedatum`.
* **File Archiving:** The input file is successfully moved from `gs://{GCS_BUCKET}/crs/work/` to `gs://{GCS_BUCKET}/crs/store/`.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, execute the following steps to roll back the system to its previous state:

1. **Pause the Airflow DAG:**
   Immediately pause the `dw_rpos_carm_import` DAG in the Airflow UI or via the CLI to prevent further executions:
   ```bash
   gcloud composer environments run {COMPOSER_ENV} \
       --location {REGION} \
       dags pause -- dw_rpos_carm_import
   ```
2. **Purge Partially Loaded Data:**
   If the PySpark job failed mid-execution or loaded corrupt data, run the following SQL script in BigQuery to delete the data loaded during the failed run (replace placeholders with the actual run parameters):
   ```sql
   DELETE FROM `your-gcp-project-id.your_bigquery_dataset_name.dwh_ta_f_rpos_fact_carm` WHERE ladedatum >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   DELETE FROM `your-gcp-project-id.your_bigquery_dataset_name.dwh_ta_f_gpos_fact_carm` WHERE ladedatum >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   DELETE FROM `your-gcp-project-id.your_bigquery_dataset_name.dwh_ta_f_rpos_reselling_carm` WHERE ladedatum >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   DELETE FROM `your-gcp-project-id.your_bigquery_dataset_name.dwh_ta_t_rpos_carm` WHERE ladedatum >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Restore Input Files:**
   If files were archived to `gs://{GCS_BUCKET}/crs/store/` during a partial failure, move them back to the staging directory:
   ```bash
   gsutil mv gs://{GCS_BUCKET}/crs/store/CARMEN_B_*_pos.fix gs://{GCS_BUCKET}/crs/work/
   ```
4. **Re-enable Legacy UC4 Job:**
   Re-activate the legacy UC4 job `DW.RPOS_CARM_IMPORT` on host `DWHDWH1P` to resume processing on the legacy platform.