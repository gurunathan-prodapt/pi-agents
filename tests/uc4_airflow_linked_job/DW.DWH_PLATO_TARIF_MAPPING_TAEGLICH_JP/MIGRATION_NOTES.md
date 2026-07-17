# MIGRATION_NOTES.md

## 1. Summary
This document details the migration of the legacy UC4 workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and Dataproc (PySpark).

* **Source Platform:** Automic UC4 (JOBP workflow and JOBS_UNIX task)
* **Target Platform:** GCP Cloud Composer (Airflow 2.x) & Dataproc Serverless / Managed Cluster
* **Migration Scope:**
  * **Orchestration:** Migrated the parent JOBP `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` to an Airflow DAG.
  * **Processing:** Migrated the validation step `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to a PySpark script.
  * **Business Purpose:** Automates the daily setup of the Plato Mapping Table, which maps Plato base tariffs to Data Warehouse (DWH) base tariffs for downstream reporting.

---

## 2. Generated Artifacts
The migration process generated the following files, structured according to the repository's folder integrity rules:

### 1. Airflow DAG
* **File Path:** `dags/dw_dwh_plato_tarif_mapping_taeglich_jp.py`
* **Role:** Orchestrates the daily execution sequence. It defines the DAG parameters, handles task scheduling, configures the Dataproc job submission, and implements the failure notification callback.

### 2. PySpark Script
* **File Path:** `pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py`
* **Role:** Executes the validation logic on Dataproc. It initializes a Spark Session, preserves the legacy logging output, and gracefully terminates the session.

---

## 3. Key Design Decisions

### Airflow Concurrency & Sync Mapping
* **Decision:** Set `max_active_runs=1` on the DAG.
* **Reasoning:** The legacy UC4 workflow used a Sync Object (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC`) with an `Else="Wait"` rule to prevent concurrent executions of the daily mapping. Restricting active runs to 1 in Airflow natively mirrors this serialization behavior without requiring complex external locking mechanisms.

### Dataproc Job Submission
* **Decision:** Used the `DataprocSubmitJobOperator` with a PySpark payload.
* **Reasoning:** This aligns with enterprise cloud architecture standards, separating orchestration (Airflow) from heavy compute (Dataproc Spark).

### Verbatim Log Preservation
* **Decision:** Retained the exact print statement `print("Doing nothinig")` in the PySpark script.
* **Reasoning:** Preserves legacy diagnostic signatures to ensure automated log parsers or operators familiar with the legacy system see consistent outputs.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Setup
Ensure the following variables are configured in your Cloud Composer environment (via Airflow UI -> Admin -> Variables or CLI):
* `GCP_PROJECT`: Your GCP Project ID.
* `DATAPROC_REGION`: The region where your Dataproc cluster runs (e.g., `europe-west3`).
* `DATAPROC_CLUSTER`: The name of your active Dataproc cluster.
* `GCS_BUCKET`: The GCS bucket name where Spark scripts are stored.

### 2. GCS Artifact Deployment
Upload the PySpark script to your GCS bucket:
```bash
gsutil cp pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py gs://${GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py
```

### 3. IAM & Permissions
Ensure the Cloud Composer worker service account has the following permissions:
* `roles/dataproc.editor` (to submit jobs to the Dataproc cluster)
* `roles/storage.objectViewer` (to read the PySpark script from GCS)

### 4. Connection Strings & Network Peering
Verify that the Dataproc cluster has network access (VPC Peering/Firewalls) to any downstream databases or APIs required for the Plato mapping tables.

---

## 5. Known Gaps & Unresolved References

### 1. Alarm Notification Integration (`DW.CALL_STANDARD`)
* **Gap:** The legacy job triggered `DW.CALL_STANDARD` with parameter `##911011` on failure.
* **Current State:** Implemented as a Python function stub (`on_failure_alarm`) that logs the failure and payload to standard output.
* **Action Required:** Platform administrators must integrate this stub with the enterprise alerting channel (e.g., sending a message to a Google Cloud Pub/Sub topic, Slack webhook, or PagerDuty).

### 2. ENDED_SKIPPED Pass-Through
* **Gap:** In UC4, if a task is skipped, downstream tasks can still run without triggering failure alerts. Airflow's default `TriggerRule.ALL_SUCCESS` will skip downstream tasks if an upstream task is skipped.
* **Action Required:** If upstream skip logic is introduced in the future, the trigger rules on the `end` task must be reviewed to prevent unintended workflow stalls.

---

## 6. Validation

### Local/Dev Environment Test Execution
To validate the DAG structure and syntax, run the following commands within your Airflow development environment:

1. **Syntax and Compilation Check:**
   ```bash
   python dags/dw_dwh_plato_tarif_mapping_taeglich_jp.py
   ```
   *Passing criteria:* The command exits with code `0` without syntax or import errors.

2. **Airflow Task Parse Test:**
   ```bash
   airflow dags list-import-errors
   ```
   *Passing criteria:* The DAG `dw_dwh_plato_tarif_mapping_taeglich_jp` is listed with zero import errors.

3. **Unit Test Task Execution (Dry Run):**
   ```bash
   airflow tasks test dw_dwh_plato_tarif_mapping_taeglich_jp dw_dwh_dummy_absd_plato_tarife 2026-03-30
   ```
   *Passing criteria:* The task successfully submits the job to the Dataproc cluster, prints `"Doing nothinig"` in the driver logs, and completes with a success status.

---

## 7. Rollback Procedure

In the event of an issue during deployment or execution on the target platform, follow these rollback steps:

1. **Pause the Airflow DAG:**
   Disable the DAG in the Airflow UI or via the CLI to prevent further scheduled runs:
   ```bash
   airflow dags pause dw_dwh_plato_tarif_mapping_taeglich_jp
   ```

2. **Remove Orchestration Artifacts:**
   Delete the DAG file from the Cloud Composer DAGs folder:
   ```bash
   gcloud composer environments storage dags delete \
       --environment <your-composer-env> \
       --location <your-region> \
       dw_dwh_plato_tarif_mapping_taeglich_jp.py
   ```

3. **Re-enable Legacy UC4 Execution:**
   * Log into the Automic UC4 client.
   * Locate the workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.
   * Set the active status flag back to Active (`1`).
   * Verify that the associated Sync Object is cleared and ready for processing.