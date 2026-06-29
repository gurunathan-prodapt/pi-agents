# Migration Notes: BERT_P_ADRESSEN

This document provides comprehensive migration notes and operational guidelines for the transition of the `BERT_P_ADRESSEN` master address data preparation pipeline from its legacy UC4/UNIX environment to Google Cloud Platform (GCP).

---

## 1. Summary

The `BERT_P_ADRESSEN` job is a critical data-warehousing orchestration pipeline responsible for the preparation, cleansing, standardization, and historization (SCD Type 2) of master address data within the `BERT` business domain. 

* **Legacy Platform:** UC4/Automic Scheduler, UNIX Korn Shell (`r_ausd_adressen.ksh`) running under the user profile `DW.UNIX.ISBERT` on host `|DWHDWH2P|HOST`, interacting with an Oracle Database.
* **Target Platform:** Google Cloud Platform (GCP) utilizing **Cloud Composer (Apache Airflow 2.x)** for orchestration and **BigQuery** as the serverless compute and storage engine.
* **Business Impact:** Migrating the core transformation logic from row-by-row/procedural shell execution to BigQuery's massively parallel processing (MPP) engine drastically reduces the legacy 6-hour execution window, optimizing system resource utilization and ensuring downstream SLAs are met.

---

## 2. Generated Artifacts

The migration process has produced the following modularized artifacts:

| Artifact Path | Target Technology | Role / Description |
| :--- | :--- | :--- |
| `src/sql/ddl/dw_bert_t_adressen.sql` | BigQuery SQL (DDL) | Defines the target schema for the master address table `dw_bert.t_adressen` (with SCD Type 2 tracking columns, partitioned by `valid_from`, and clustered by `address_id`) and the execution audit table `dw_bert.metadata_job_runs`. |
| `src/sql/procedures/sp_prep_adressen.sql` | BigQuery SQL Scripting | Encapsulates the core transformation, cleansing, and SCD Type 2 merge logic previously handled by `r_ausd_adressen.ksh`. |
| `src/python/utils/logging_helper.py` | Python (GCP Logging API) | A reusable utility module that replaces the legacy `DW.BERT_LESE_LOG` include, providing structured JSON logging to GCP Cloud Logging. |
| `src/dags/dw_bert_p_adressen.py` | Python (Airflow 2.x SDK) | The Cloud Composer DAG orchestrating the pipeline. It implements concurrency guards, upstream task sensors, and executes the BigQuery stored procedure. |

---

## 3. Key Design Decisions

### Serverless Compute Shift (Shell to Stored Procedure)
* **Decision:** The legacy Korn Shell script (`r_ausd_adressen.ksh`) was refactored into a native BigQuery Stored Procedure (`dw_bert.sp_prep_adressen()`).
* **Reasoning:** Executing SQL transformations directly inside BigQuery eliminates the overhead of managing physical or virtual UNIX VM nodes, simplifies the network topology, and leverages BigQuery's serverless scaling to process high-volume address datasets efficiently.

### Concurrency & Lock Emulation (Sync Objects to Airflow Controls)
* **Decision:** Legacy UC4 Sync Objects were mapped to native Airflow orchestration patterns:
  * **`DW.BERT_ADRESS_SYNC` (Else=Skip):** Emulated via a custom Python task (`guard_active_run`) that queries the Airflow Metadata Database. If another instance of the DAG is running, it raises an `AirflowSkipException` to terminate the current run gracefully without failing.
  * **`DW.BERT_GP_SYNC` & `DW.BERT_RECH_SYNC` (Else=Wait):** Emulated using `ExternalTaskSensor` operators (`sensor_gp` and `sensor_rech`) that poll for the successful completion of the upstream business partner (`dw_bert_p_geschaeftsp`) and invoice recipient (`dw_bert_p_rechempf`) DAGs.
* **Trade-off:** Using sensors consumes lightweight worker slots during the polling phase. To mitigate this, the sensors are configured in `reschedule` mode with a 120-second poke interval, freeing up worker slots between checks.

### Table Partitioning & Clustering
* **Decision:** The target table `dw_bert.t_adressen` is partitioned by `DATE(valid_from)` and clustered by `address_id`.
* **Reasoning:** Address history queries typically filter on active records or specific time slices. Partitioning minimizes slot-hour consumption, while clustering by `address_id` optimizes the performance of the SCD Type 2 `MERGE` operations.

---

## 4. Manual Steps Before Go-Live

To deploy and configure the migrated pipeline in a production environment, complete the following manual steps:

### 1. Schema & Dataset Provisioning
Ensure the target BigQuery datasets exist in your project, then execute the DDL script to provision the tables:
```bash
# Create datasets if they do not exist
bq mk --location=EU --dataset dw_bert
bq mk --location=EU --dataset dw_bert_staging

# Execute the DDL script
bq query --use_legacy_sql=false < src/sql/ddl/dw_bert_t_adressen.sql
```

### 2. Deploy the Stored Procedure
Deploy the compiled stored procedure to the `dw_bert` dataset:
```bash
bq query --use_legacy_sql=false < src/sql/procedures/sp_prep_adressen.sql
```

### 3. IAM & Service Account Configuration
Create or configure the runtime service account `sa-bert-dwh-prod@<gcp-project>.iam.gserviceaccount.com` with the following IAM roles:
* **BigQuery Admin** (`roles/bigquery.admin`) - required to run the stored procedure, modify target tables, and write audit logs.
* **Composer Worker** (`roles/composer.worker`) - required for execution within the Cloud Composer environment.

### 4. Airflow Environment Variables & Connections
In the Cloud Composer Airflow UI, navigate to **Admin -> Variables** and define:
* `gcp_project_id`: The target GCP project ID (e.g., `gcp-dwh-prod`).
* `gcp_conn_id`: The Airflow connection ID for GCP (defaults to `google_cloud_default`).

Ensure that the service account key or workload identity is correctly associated with the configured Airflow connection.

### 5. Code Deployment
* Copy `src/python/utils/logging_helper.py` to the Cloud Storage DAG bucket under the `dags/utils/` directory:
  ```bash
  gsutil cp src/python/utils/logging_helper.py gs://<composer-dag-bucket>/dags/utils/
  ```
* Copy the DAG file `src/dags/dw_bert_p_adressen.py` to the root of the DAG bucket:
  ```bash
  gsutil cp src/dags/dw_bert_p_adressen.py gs://<composer-dag-bucket>/dags/
  ```

---

## 5. Known Gaps & Unresolved References

### 1. Legacy Shell Script Gaps (B4 Redesign Item)
* **Risk:** The physical content of `r_ausd_adressen.ksh` was not fully available during the initial migration phase. The current stored procedure assumes standard SQL-based delta processing, cleansing, and SCD Type 2 loading.
* **Follow-up:** Verify if the legacy shell script executed external binaries (e.g., third-party postal validation software, address cleansing APIs, or file exports). If external utilities are required, they must be refactored into Cloud Run microservices and invoked via an `HttpOperator` within the DAG.

### 2. Upstream DAG Naming Dependencies
* **Risk:** The `ExternalTaskSensor` tasks assume that the upstream DAGs are named exactly `dw_bert_p_geschaeftsp` and `dw_bert_p_rechempf`.
* **Follow-up:** Confirm the final DAG IDs of the migrated business partner and invoice recipient pipelines and update the `external_dag_id` parameters in `dw_bert_p_adressen.py` if necessary.

### 3. Initial Seed / Historical Load
* **Risk:** The stored procedure uses a delta-load pattern based on the maximum `end_time` of previous successful runs in `dw_bert.metadata_job_runs`. On the first execution, this table will be empty, defaulting the delta watermark to `1970-01-01`.
* **Follow-up:** Ensure that the staging table `dw_bert_staging.stg_addresses` is fully seeded with historical data before the first run, or execute a manual historical migration load into `dw_bert.t_adressen` prior to enabling the automated schedule.

---

## 6. Validation

To validate the migrated pipeline, execute the following test suite in a non-production environment:

### Unit Test: Concurrency Guard
1. Trigger the `dw_bert_p_adressen` DAG manually.
2. While the first run is active, immediately trigger a second run manually.
3. **Pass Criteria:** The second run must execute the `guard_active_run` task, log a `SKIPPED` event, raise an `AirflowSkipException`, and terminate gracefully without failing the DAG run.

### Integration Test: Upstream Sensors
1. Clear the state of `dw_bert_p_adressen` while the upstream DAGs (`dw_bert_p_geschaeftsp` and `dw_bert_p_rechempf`) have not run for the current execution date.
2. Verify that the sensors enter a `reschedule` state.
3. Manually trigger and successfully complete the upstream DAGs.
4. **Pass Criteria:** The sensors must detect the successful status of the upstream DAGs, transition to `success`, and trigger the downstream stored procedure task.

### Functional Test: SCD Type 2 Logic
1. Insert a mock address record into `dw_bert_staging.stg_addresses`:
   ```sql
   INSERT INTO `dw_bert_staging.stg_addresses` (address_id, street_name, postal_code, city, country_code, last_modified_timestamp)
   VALUES ('TEST_ADR_01', 'Main Street 1', '12345', 'Berlin', 'DE', CURRENT_TIMESTAMP());
   ```
2. Execute the DAG. Verify that the record is inserted into `dw_bert.t_adressen` with `is_current = TRUE` and `valid_to = '9999-12-31 23:59:59'`.
3. Insert an updated version of the same address into staging:
   ```sql
   INSERT INTO `dw_bert_staging.stg_addresses` (address_id, street_name, postal_code, city, country_code, last_modified_timestamp)
   VALUES ('TEST_ADR_01', 'Main Street 2', '12345', 'Berlin', 'DE', CURRENT_TIMESTAMP());
   ```
4. Execute the DAG again.
5. **Pass Criteria:** 
   * The original record in `dw_bert.t_adressen` must be updated to `is_current = FALSE` with `valid_to` set to the execution timestamp.
   * A new record must be inserted with the updated street name, `is_current = TRUE`, and `valid_to = '9999-12-31 23:59:59'`.
   * A successful run entry must be written to `dw_bert.metadata_job_runs`.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during the go-live window, execute the following rollback steps:

### Step 1: Pause the Airflow DAG
Disable the active schedule in the Cloud Composer UI:
```bash
gcloud composer environments run <composer-env-name> \
    --location <location> \
    dags pause -- dw_bert_p_adressen
```

### Step 2: Restore the BigQuery Target Table
If data corruption occurred in `dw_bert.t_adressen`, restore the table to its pre-migration state using BigQuery's time-travel feature (substitute the timestamp with the point-in-time immediately preceding the migration run):
```sql
CREATE OR REPLACE TABLE `dw_bert.t_adressen` AS
SELECT * FROM `dw_bert.t_adressen`
FOR SYSTEM_TIME AS OF TIMESTAMP('2026-04-21 06:00:00+00');
```

### Step 3: Reactivate the Legacy UC4 Job
1. Log into the UC4/Automic administration console.
2. Locate the job plan `DW.BERT_STAMMDATEN_JP`.
3. Re-enable the active execution flag for the `BERT_P_ADRESSEN` job.
4. Verify that the legacy UNIX host `|DWHDWH2P|HOST` and the user profile `DW.UNIX.ISBERT` are active and ready to accept execution requests.