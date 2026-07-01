# MIGRATION_NOTES.md: DW.BERT_DROP_TEMP_TABLE

This document provides comprehensive migration notes for transitioning the UC4 UNIX Job `DW.BERT_DROP_TEMP_TABLE` to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy UC4 UNIX job `DW.BERT_DROP_TEMP_TABLE` has been migrated to a cloud-native architecture on Google Cloud Platform (GCP). 

* **Source Component:** UC4 UNIX Job `DW.BERT_DROP_TEMP_TABLE` executing the shell script `r_drop_temp_table.ksh` under the UNIX login `DW.UNIX.ISBERT`.
* **Target Platform:** Google Cloud Composer (Apache Airflow) and Google BigQuery.
* **Functional Purpose:** This job performs database maintenance by dropping temporary staging tables used during the BERT master data processing workflow. 
* **Migration Strategy:** The legacy shell script was optimized. Instead of using a costly Dataproc PySpark cluster to execute simple table drops (as initially suggested by the automated conversion tool), the logic was refactored into native **BigQuery Standard SQL DDL** executed via the Airflow `BigQueryInsertJobOperator`.

---

## 2. Generated Artifacts

The migration process generated the following files:

### 1. `dags/dw_bert_drop_temp_table.py`
* **Role:** Airflow DAG definition file.
* **Details:** Orchestrates the execution of the cleanup task. It utilizes the `BigQueryInsertJobOperator` to run the SQL script and applies an Airflow Pool (`bert_write_lock_pool`) to replicate legacy synchronization locks.

### 2. `sql/bert/r_drop_temp_table.sql`
* **Role:** BigQuery Standard SQL Script.
* **Details:** Contains the DDL statements to clean up the temporary staging tables. It defines a temporary stored procedure (`drop_table_if_exists`) to safely drop tables dynamically based on environment-specific variables.

---

## 3. Key Design Decisions

### Shift from Dataproc to Native BigQuery SQL
The automated UC4 conversion tool suggested deploying a Dataproc PySpark job (`r_drop_temp_table.py`) to replace the legacy shell script. 
* **Decision:** Rejected Dataproc in favor of native BigQuery SQL.
* **Reasoning:** Spinning up a Dataproc cluster takes several minutes and incurs unnecessary compute costs. Executing native `DROP TABLE IF EXISTS` statements directly in BigQuery via the `BigQueryInsertJobOperator` takes seconds, costs virtually nothing, and eliminates cluster management overhead.

### Replicating Legacy Sync Locks via Airflow Pools
In the legacy UC4 environment, the job utilized multiple synchronization resources (`DW.BERT_RECH_SYNC`, `DW.BERT_VERT_SYNC`, etc.) to prevent dropping tables while active loading jobs were running.
* **Decision:** Implemented an Airflow Pool named `bert_write_lock_pool` with a slot capacity of `1`.
* **Reasoning:** This ensures mutual exclusion. Any task assigned to this pool will run sequentially, preventing race conditions where tables are dropped mid-load.

### Safe Drop Procedure
* **Decision:** Implemented a temporary stored procedure using dynamic SQL (`EXECUTE IMMEDIATE`).
* **Reasoning:** If an upstream loading job fails before creating a specific temporary table, a standard `DROP TABLE` statement might fail and halt the pipeline. The custom procedure ensures that the cleanup script runs idempotently and safely without throwing errors for non-existent tables.

---

## 4. Manual Steps Before Go-Live

To prepare the target environment for execution, complete the following manual setup steps:

### 1. Schema & Dataset Verification
Ensure that the target staging dataset (e.g., `dev_bert_staging` or `prod_bert_staging`) exists in Google BigQuery within your project.

### 2. IAM & Permissions
Ensure that the Google Service Account running the Cloud Composer workers (`sa-composer-bert@<project-id>.iam.gserviceaccount.com`) has the following IAM roles:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the staging dataset.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.

### 3. Airflow Variables Configuration
Add the following variables to your Cloud Composer environment (via Airflow UI -> Admin -> Variables or gcloud CLI):

| Variable Key | Example Dev Value | Example Prod Value | Description |
|---|---|---|---|
| `gcp_project_id` | `gcp-dev-dwh-1` | `gcp-prod-dwh-1` | Target GCP Project ID |
| `bert_staging_dataset` | `dev_bert_staging` | `prod_bert_staging` | BigQuery dataset containing staging tables |

### 4. Airflow Pool Creation
Create the mutual exclusion pool in the Airflow UI:
1. Navigate to **Admin -> Pools**.
2. Click **Create**.
3. Set **Pool** to `bert_write_lock_pool`.
4. Set **Slots** to `1`.
5. Description: `Prevents concurrent modifications to BERT staging tables.`

### 5. Connection Strings
Verify that the Airflow connection `google_cloud_default` is configured and has access to the target GCP project.

---

## 5. Known Gaps & Unresolved References

### 1. Table Inventory Validation
The list of tables dropped in `sql/bert/r_drop_temp_table.sql` was derived from the legacy script context:
* `temp_rech`
* `temp_vert`
* `temp_gp`
* `temp_basis`
* `temp_adress`
* `temp_stamm`

**Action Required:** The migration team must inspect the physical legacy script `r_drop_temp_table.ksh` on the source UNIX host to verify that no other temporary tables were omitted from this list.

### 2. Workflow Integration (Redesign / B4 Item)
This DAG is currently configured with `schedule_interval=None` (manual trigger). In the legacy system, this job was part of the `DW.BERT_STAMMDATEN_JP` workflow.
**Action Required:** Once the parent workflow is migrated, this DAG must either be integrated as a downstream task within the master DAG or triggered via a `TriggerDagRunOperator` at the end of the master sequence.

---

## 6. Validation

Follow these steps to validate the migration in the target environment:

### Test Execution Procedure
1. Upload `sql/bert/r_drop_temp_table.sql` to the GCS bucket mapped to the Airflow search path (typically `/home/airflow/gcs/sql/bert/r_drop_temp_table.sql`).
2. Upload `dags/dw_bert_drop_temp_table.py` to the Composer `dags/` folder.
3. In the BigQuery Console, create dummy tables to simulate the staging environment:
   ```sql
   CREATE TABLE `your_project.your_staging_dataset.temp_rech` (id INT64);
   CREATE TABLE `your_project.your_staging_dataset.temp_vert` (id INT64);
   ```
4. Trigger the `dw_bert_drop_temp_table` DAG manually from the Airflow UI.

### Definition of "Passing"
* The DAG run completes with a status of **Success**.
* The Airflow task logs show successful execution of the BigQuery job.
* In the BigQuery Console, verify that the dummy tables (`temp_rech`, `temp_vert`, etc.) have been successfully deleted.
* Running the DAG a second time (when tables do not exist) also completes successfully without errors (proving idempotency).

---

## 7. Rollback Procedure

If issues arise during go-live, execute the following steps to revert to the legacy system:

1. **Pause the Airflow DAG:**
   Disable the `dw_bert_drop_temp_table` DAG in the Cloud Composer UI to prevent automated runs.
2. **Restore Dropped Tables (If Necessary):**
   If tables were dropped prematurely and need to be recovered, use BigQuery's Time Travel feature to restore them:
   ```sql
   CREATE OR REPLACE TABLE `your_project.your_staging_dataset.temp_rech`
   AS SELECT * FROM `your_project.your_staging_dataset.temp_rech`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Reactivate UC4 Job:**
   Re-enable the active flag for the `DW.BERT_DROP_TEMP_TABLE` job in the UC4 system.
4. **Verify Legacy Agent:**
   Ensure the UNIX agent `|DWHDWH2P|HOST` and login credentials `DW.UNIX.ISBERT` are active and ready to process jobs.