# MIGRATION NOTES: DW.DWH_EXIS_IKDB_STAMM_R

This document provides the complete migration notes, design decisions, manual setup steps, known gaps, validation procedures, and rollback strategies for the migrated job `DW.DWH_EXIS_IKDB_STAMM_R`.

---

## 1. SUMMARY

The legacy orchestration wrapper pattern `DW.DWH_EXIS_IKDB_STAMM_R` has been migrated from its on-premises environment to Google Cloud Platform (GCP). 

* **Legacy Environment:** Managed via UC4 Job Descriptor XML (`DW.DWH_EXIS_IKDB_STAMM_R.xml`) and a KornShell wrapper script (`r_exp_ikdb.ksh`) executing on a UNIX host. It queried an Oracle database for metadata tracking (`DWTK_MELDUNGEN`) and exported contract master data from the interactive database system (`IKDB`).
* **Target Platform:** 
  * **Orchestration:** Managed by **Apache Airflow (Cloud Composer)**.
  * **Computation (Extraction & Transformation):** Managed by **GCP Dataproc Spark (PySpark)**.
  * **Storage & Target Tables:** **Google Cloud Storage (GCS)** for raw CSV exports and **BigQuery** for source data and operational metadata tracking (`DWTK_MELDUNGEN`).

---

## 2. GENERATED ARTIFACTS

The migration process generated the following files, each serving a specific role in the target architecture:

1. **`isdwh/dw_dwh_exis_ikdb_stamm_r.py` (Airflow DAG)**
   * **Role:** The workflow orchestrator. It replaces the UC4 XML structure and the control flow logic of the KornShell wrapper. It manages concurrency locks (`max_active_runs=1`), executes the dynamic database execution check, branches based on prior run status, and triggers the Dataproc PySpark job.
2. **`isdwh/exis_ikdb_stamm_r.py` (PySpark Script)**
   * **Role:** The computational engine. It is submitted to a Dataproc cluster to execute the master data extraction query, write the resulting dataset to GCS as a CSV, and update the operational metadata table (`DWTK_MELDUNGEN`) with the execution status.
3. **`pyspark_scripts/sql/d_ikdb_exp_stamm.sql` (BigQuery SQL)**
   * **Role:** The SQL extraction query template. It contains the BigQuery-compliant SQL syntax used to pull contract master data from the source dataset.

---

## 3. KEY DESIGN DECISIONS

### 3.1. Dynamic Branching vs. Sequential Shell Scripting
* **Decision:** The legacy KornShell script checked the database state using inline SQL and conditionally exited or continued. In the target architecture, this is split into an Airflow `BranchPythonOperator` (`check_already_executed`) and a downstream `DataprocSubmitJobOperator` (`run_export_ikdb_task`).
* **Reasoning:** This separates orchestration logic (checking if a job needs to run) from heavy compute logic (running the actual export). It prevents spinning up or utilizing Dataproc resources if the export has already run successfully for the target date.

### 3.2. Concurrency Control via Airflow Configuration
* **Decision:** The legacy UC4 sync object (`DW.DWH_JOB_EXIS_IKDB_STAMM_SYNC`) with `Else="Wait"` is mapped directly to `max_active_runs=1` at the DAG level.
* **Reasoning:** This natively prevents parallel executions of the same DAG, protecting database concurrency locks and ensuring that backfill or catchup runs queue up safely.

### 3.3. Verbatim Log Preservation
* **Decision:** Original German log messages (e.g., `"Zuweisung erfolgt"`, `"Exportjob lief bereits am Datum"`, `"Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet"`) are preserved verbatim in the Python logging statements.
* **Reasoning:** This ensures administrative trace compatibility for operations teams transitioning from the legacy system to GCP.

---

## 4. MANUAL STEPS BEFORE GO-LIVE

The following manual setups must be completed in the target environment before activating the pipeline:

### 4.1. Schema & Dataset Creation
Ensure the following BigQuery datasets and tables exist:
* **Source Dataset:** `{YYYY}_IKDB_SOURCE.contract_master_table` (where `{YYYY}` represents the target execution years, e.g., `2026_IKDB_SOURCE`).
* **Metadata Dataset & Table:** `metadata_dataset.DWTK_MELDUNGEN` with the following schema:
  * `JOB_KENNUNG` (STRING)
  * `STATUS_NR` (STRING)
  * `STICHTAG` (STRING)
  * `TSTAMP` (STRING)

### 4.2. IAM & Permissions
The Cloud Composer service account must have the following IAM roles:
* **Dataproc Administrator** (`roles/dataproc.admin`) or **Dataproc Editor** (`roles/dataproc.editor`) to submit jobs.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the target GCS bucket.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) and **BigQuery Job User** (`roles/bigquery.jobUser`) to read source tables and write to the metadata table.

### 4.3. Airflow Connections & Variables
1. **Airflow Variables:** Configure the following variables in the Airflow UI (**Admin -> Variables**):
   * `GCP_PROJECT`: Your GCP Project ID.
   * `GCP_REGION`: Your Dataproc/Composer region (e.g., `europe-west3`).
   * `DATAPROC_CLUSTER`: The name of your active Dataproc cluster.
   * `GCS_BUCKET`: The target GCS bucket name (without the `gs://` prefix).
2. **Airflow Connections:** Configure the connection for the metadata database (**Admin -> Connections**):
   * **Connection ID:** `metadata_db`
   * **Connection Type:** Postgres (or Oracle/Spanner depending on where your operational metadata store is hosted).

### 4.4. Scheduling & Upstream Wiring
* The DAG is configured with `schedule=None`. It must be triggered either manually for testing or via an upstream parent workflow using the `TriggerDagRunOperator`.

---

## 5. KNOWN GAPS & UNRESOLVED REFERENCES

* **Unmerged Upstream Dependency:** The shared includes module containing legacy helper logic (such as `DW.HOLE_PFAD` and `DW.LESE_LOG`) is currently sitting in an unmerged Pull Request (`PR: https://github.com/gurunathan-prodapt/pi-agents/pull/626`). 
  * *Impact:* This workflow cannot be fully integrated into the shared corporate path until this PR is merged.
  * *Workaround:* The generated DAG uses standard Airflow variables and direct GCS paths to bypass this dependency for standalone testing.
* **Metadata Database Hook:** The `check_prior_run_status` function uses `PostgresHook(postgres_conn_id='metadata_db')`. If the target metadata store is hosted on Oracle or BigQuery directly, this hook must be updated to `OracleHook` or a BigQuery client hook.

---

## 6. VALIDATION

To validate the migration, execute the following test cases:

### 6.1. Test Case 1: First-Time Run (Successful Export)
1. Ensure no entry exists in `DWTK_MELDUNGEN` for the target date (e.g., `20260420` for execution date `2026-04-21`).
2. Trigger the DAG manually in Airflow with the logical date `2026-04-21`.
3. **Expected Result:**
   * The `check_already_executed` task branches to `run_export_ikdb_task`.
   * The Dataproc job executes successfully, exporting CSV data to `gs://{GCS_BUCKET}/exports/STAMM_OUT_TMD/20260420/`.
   * A row is appended to `DWTK_MELDUNGEN` with `STATUS_NR = '2'` and `STICHTAG = '20260420'`.
   * The DAG ends with a `success` state.

### 6.2. Test Case 2: Duplicate Run (Skip Export)
1. Trigger the DAG again for the same logical date `2026-04-21`.
2. **Expected Result:**
   * The `check_already_executed` task queries `DWTK_MELDUNGEN`, finds the existing `STATUS_NR = '2'` record, and branches to `skipped_already_run`.
   * The `run_export_ikdb_task` is skipped.
   * The DAG ends with a `success` state without modifying any files or writing new success logs.

### 6.3. Test Case 3: Failure Handling
1. Temporarily rename the source BigQuery table or revoke permissions to simulate a failure.
2. Trigger the DAG.
3. **Expected Result:**
   * The `run_export_ikdb_task` fails.
   * The `on_failure_alarm` callback triggers, printing the failure alert to the Airflow logs.
   * A row is appended to `DWTK_MELDUNGEN` with `STATUS_NR = '3'` (Failure status).
   * The DAG ends with a `failed` state.

---

## 7. ROLLBACK PROCEDURE

If a critical issue is discovered post-go-live, perform the following steps to roll back:

1. **Pause the Airflow DAG:**
   * Run the following CLI command or toggle the pause switch in the Airflow UI:
     ```bash
     gcloud composer environments run <ENVIRONMENT_NAME> \
         --location <LOCATION> \
         dags pause -- dw_dwh_exis_ikdb_stamm_r
     ```
2. **Clean Up Target GCS Exports (Optional):**
   * If a partial or corrupted export file was written to GCS for the active business date, delete it:
     ```bash
     gsutil rm -r gs://{GCS_BUCKET}/exports/STAMM_OUT_TMD/{TARGET_DATE}/
     ```
3. **Revert Metadata Status:**
   * If necessary, remove or update the status record in the metadata table to allow the legacy system to re-run:
     ```sql
     DELETE FROM metadata_dataset.DWTK_MELDUNGEN 
     WHERE JOB_KENNUNG = 'EXIS_IKDB_STAMM_R' AND STICHTAG = '{TARGET_DATE}';
     ```
4. **Reactivate Legacy UC4 Job:**
   * Set the active flag of the legacy UC4 job `DW.DWH_EXIS_IKDB_STAMM_R` back to `1` (Active) and resume scheduling in the legacy environment.