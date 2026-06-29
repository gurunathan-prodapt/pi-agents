# Migration Notes: `DW.BERT_P_VERTRAG_JP`

This document details the migration of the legacy UC4 Job Plan `DW.BERT_P_VERTRAG_JP` to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow 2.x), Cloud Dataproc Serverless, and Google BigQuery.

---

## 1. Summary

The legacy UC4 Job Plan **`DW.BERT_P_VERTRAG_JP`** has been migrated to a modern, cloud-native orchestration pipeline on Google Cloud Platform. 

*   **Source Platform**: UC4 / Automic Workload Automation (orchestrating Unix agent tasks and database scripts).
*   **Target Platform**: Google Cloud Composer (Apache Airflow 2.x) orchestrating serverless PySpark jobs on Cloud Dataproc and SQL transformations in Google BigQuery.
*   **Pipeline Purpose**: This pipeline orchestrates the core contract (`VERTRAG`) and reporting data warehouse pipeline for the **BERT (Business Entity Reporting Tool / Stammdaten)** subject area. It performs staging, validation, discount calculations, barrier checks, account mappings, and business partner joins, culminating in a consolidated staging layer and a final write to the main contract reporting table (`dw_bert_ausd_v_ta_p_vertrag`).

---

## 2. Generated Artifacts

The migration process has generated the following files, organized by their role in the target architecture:

| File Path | Role | Description |
| :--- | :--- | :--- |
| **`src/dags/dw_bert_p_vertrag_jp.py`** | Orchestration (DAG) | The primary Airflow DAG file defining the workflow structure, task dependencies, concurrency limits, and retry configurations. |
| **`src/dags/callbacks.py`** | Alerting & Monitoring | Shared Python module containing the `on_terminal_failure` callback to handle terminal task failures (mimicking the legacy `DW.CALL_STANDARD` action). |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_period.py`** | Compute Task | PySpark stub to set the active reporting period parameters. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_discount_rr.py`** | Compute Task | PySpark stub for discount computation with recovery processing loop. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_cntrct_valid.py`** | Compute Task | PySpark stub to validate contract constraints. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_barrier.py`** | Compute Task | PySpark stub to process contract barriers. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_vvl_dwh.py`** | Compute Task | PySpark stub to process contract extensions (VVL) for the DWH. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_inv_assign.py`** | Compute Task | PySpark stub to assign invoice profiles. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_inv_def.py`** | Compute Task | PySpark stub to define invoice structures. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_acc_ref.py`** | Compute Task | PySpark stub to map account references. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_action_assoc.py`** | Compute Task | PySpark stub to associate contract promotional actions. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_discount.py`** | Compute Task | PySpark stub for standard discount mappings. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_apn_ve.py`** | Compute Task | PySpark stub for APN and Sales Entity associations. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_bp_ref.py`** | Compute Task | PySpark stub for Business Partner references. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_notice.py`** | Compute Task | PySpark stub for notices and terminations. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_cntrct_crs.py`** | Compute Task | PySpark stub to interface with the Contract Reference System (CRS). |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_barrier_zusgf.py`** | Compute Task | PySpark stub to summarize barrier metrics. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_vvl_upgrade.py`** | Compute Task | PySpark stub to process tariff upgrades. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_inv_acc.py`** | Compute Task | PySpark stub to map invoices to accounts. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_disc_zusgf.py`** | Compute Task | PySpark stub to summarize discount metrics. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_p_discount.py`** | Compute Task | PySpark stub to prepare final discount table records. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_p_discount_rr.py`** | Compute Task | PySpark stub for discount calculations with recovery logic. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_cntrct_crs3.py`** | Compute Task | PySpark stub to process additional CRS attributes. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_vertrag_tmp.py`** | Compute Task | PySpark stub to merge variables into the temporary contract store. |
| **`src/pyspark_scripts/dw_bert_ausd_v_ta_p_vertrag.py`** | Compute Task | PySpark stub to write the final consolidated contract records. |

---

## 3. Key Design Decisions

### 3.1. Orchestration & Concurrency Control
*   **Decision**: Enforce `max_active_runs=1` at the DAG level.
*   **Reasoning**: The legacy UC4 Job Plan utilized synchronization objects (`DW.BERT_P_VERTRAG_JP_SYNC` and `DW.BERT_BFC_JP_SYNC`) with an "Else=Wait" action to prevent concurrent executions of the pipeline. Setting `max_active_runs=1` in Airflow natively guarantees that only one instance of this pipeline runs at any given time, preventing race conditions on staging tables.

### 3.2. Task Recovery & Retry Policies
*   **Decision**: Implement task-specific retry parameters for recovery tasks.
*   **Reasoning**: The legacy jobs `DW.BERT_AUSD_V_TA_DISCOUNT_RR` and `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` were configured with a custom recovery loop (up to 10 retries, waiting 15 minutes between attempts). 
    *   Standard tasks in the DAG default to `retries=0` to fail fast.
    *   `task_discount_rr` and `task_p_discount_rr` are explicitly configured with `retries=10` and `retry_delay=timedelta(minutes=15)`.

### 3.3. Centralized Failure Handling
*   **Decision**: Implement a shared `callbacks.py` module containing `on_terminal_failure`.
*   **Reasoning**: The legacy pipeline executed `DW.CALL_STANDARD` upon terminal task failures. By using Airflow's `on_failure_callback` (triggered only when a task exhausts all its retries), we decouple alerting logic from the DAG structure and provide a single integration point for enterprise alerting systems (e.g., Slack, PagerDuty, or Google Cloud Pub/Sub).

### 3.4. Serverless Compute Strategy
*   **Decision**: Use `DataprocSubmitJobOperator` to run PySpark scripts.
*   **Reasoning**: Migrating legacy Unix agent scripts to PySpark on Dataproc Serverless provides a highly scalable, serverless execution environment. This avoids the overhead of managing persistent VM-based execution nodes while ensuring compatibility with complex data transformation libraries.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the migrated pipeline in production, the following manual setup steps must be completed:

### 4.1. BigQuery Schema & Dataset Creation
Ensure the target BigQuery datasets and tables exist in your production GCP project:
1.  Create the dataset `bert_production` (or your environment-specific equivalent).
2.  Initialize the staging table `dw_bert_ausd_v_ta_vertrag_tmp`.
3.  Initialize the final production table `dw_bert_ausd_v_ta_p_vertrag`.

### 4.2. IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
*   **Dataproc Editor** (`roles/dataproc.editor`) to submit serverless Spark jobs.
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) and **BigQuery User** (`roles/bigquery.user`) to read/write data.
*   **Storage Object Admin** (`roles/storage.objectAdmin`) on the GCS bucket hosting the PySpark scripts and temporary Spark execution files.

### 4.3. Airflow Variables & Connections
Configure the following Airflow variables in the Composer environment:
*   `gcp_project_id`: The target GCP Project ID.
*   `dataproc_region`: The GCP region where Dataproc Serverless jobs should run (e.g., `europe-west3`).
*   `dataproc_cluster_name`: The name of the Dataproc cluster (if using shared clusters) or serverless batch configuration.
*   `gcs_bucket_name`: The GCS bucket where PySpark scripts are stored (e.g., `prod-bert-etl-resources`).

### 4.4. Code Deployment
1.  Upload `callbacks.py` and `dw_bert_p_vertrag_jp.py` to the Composer environment's `dags/` folder.
2.  Upload all 22 PySpark scripts from `src/pyspark_scripts/` to the GCS bucket path configured in the DAG (e.g., `gs://<gcs_bucket_name>/pyspark_scripts/`).

### 4.5. Scheduling Configuration
The legacy `EVNT_TIME` scheduler logic was not provided in the source XML. The DAG is currently configured with `schedule=None` (manual trigger). 
*   **Action**: Identify the legacy cron/calendar schedule and update the `schedule` parameter in `dw_bert_p_vertrag_jp.py` (e.g., `schedule="0 2 * * *"` for daily execution at 2:00 AM).

---

## 5. Known Gaps & Unresolved References

### 5.1. Business Logic Implementation (Redesign B4 Item)
*   **Gap**: The 22 PySpark scripts generated in `src/pyspark_scripts/` are currently **functional stubs**. They contain the correct structure and logging but do not contain the actual business logic (SQL queries, Ab Initio graph logic, or Unix shell commands) executed by the legacy jobs.
*   **Resolution**: The legacy ETL code/scripts corresponding to each job must be extracted, translated to PySpark or BigQuery SQL, and pasted into the designated placeholder blocks within each script.

### 5.2. Alerting Integration
*   **Gap**: The `on_terminal_failure` callback in `src/dags/callbacks.py` currently logs errors to Cloud Logging but does not dispatch external notifications.
*   **Resolution**: Integrate the callback with your enterprise alerting tool (e.g., configure a Google Cloud Pub/Sub publisher, Slack Webhook, or PagerDuty API call within the function).

---

## 6. Validation

To validate the migration, perform the following testing steps in a non-production environment:

### 6.1. DAG Parsing Test
Verify that the Airflow DAG compiles without syntax or import errors:
```bash
python3 src/dags/dw_bert_p_vertrag_jp.py
```
*Passing criteria*: The command exits with code `0` and outputs no errors.

### 6.2. Non-Production Dry Run
1.  Deploy the DAG and PySpark stubs to a development Composer environment.
2.  Trigger the DAG manually via the Airflow UI.
3.  Monitor the execution sequence.
*Passing criteria*: 
*   The DAG executes linearly from `guard_active_run` to `end`.
*   All 24 tasks complete with a `success` status.
*   Dataproc Serverless batches are successfully created and terminated.

### 6.3. Failure Callback Validation
1.  Temporarily modify `dw_bert_ausd_v_ta_discount_rr.py` to raise an exception (`raise Exception("Simulated Failure")`).
2.  Trigger the DAG.
3.  Verify that `task_discount_rr` retries 10 times with 15-minute intervals.
4.  Verify that upon the 11th failure, the task transitions to `failed` and the `on_terminal_failure` callback is executed (check Airflow task logs for the `CRITICAL ALERT` log entry).

---

## 7. Rollback Procedure

In the event of an unrecoverable failure or data mismatch in production post-go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG**:
    *   Log into the Airflow UI.
    *   Locate `dw_bert_p_vertrag_jp` and toggle the active switch to **Off**.
    *   This prevents any new instances of the pipeline from starting on GCP.
2.  **Clean Up Active Runs**:
    *   If a DAG run is currently active, manually terminate/fail the active run in the Airflow UI to release any locked resources.
3.  **Re-enable Legacy UC4 Job Plan**:
    *   Log into the UC4/Automic console.
    *   Locate the Job Plan `DW.BERT_P_VERTRAG_JP`.
    *   Set the status to **Active** and restore its scheduling/event triggers.
4.  **Data Reconciliation (Optional)**:
    *   If the migrated pipeline failed mid-run and wrote partial data to BigQuery, run a cleanup script to truncate or restore the staging table `dw_bert_ausd_v_ta_vertrag_tmp` to its pre-run state before resuming UC4 operations.