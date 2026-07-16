# Migration Notes: CRM Weekly Workflow (`customer/crm_weekly_workflow.xml`)

This document details the migration of the legacy UC4 workflow `CRM_WEEKLY_WORKFLOW` to Apache Airflow running on Google Cloud Composer.

---

## 1. Summary
The **CRM_WEEKLY_WORKFLOW** is a critical weekly pipeline scheduled to run every Sunday at 04:00 Europe/London. It executes customer segmentation, scoring, and data tracking within the CRM Analytics ETL system. 

The workflow has been migrated from **UC4 (Automic)** to **Apache Airflow (Google Cloud Composer)**. The legacy execution model (which relied on local Unix agents, Ab Initio Co-Operating Systems, and Spark on Yarn) has been modernized to run as a serverless, cloud-native orchestration DAG utilizing **Google Cloud Dataproc** (PySpark) and **Google Cloud Storage (GCS)**.

---

## 2. Generated Artifacts
The migration process generated the following files, which must be deployed to their respective locations in the target environment:

| Relative File Path | Target Location | Role / Description |
| :--- | :--- | :--- |
| `dags/crm_weekly_workflow.py` | Cloud Composer DAGs Bucket (`gs://<composer-bucket>/dags/`) | Main Airflow DAG orchestrating all tasks, sensors, retries, and dependencies. |
| `pyspark_scripts/process_customer_data.py` | GCS Code Bucket (`gs://<code-bucket>/pyspark_scripts/`) | PySpark script replacing the legacy `process_customer_data.ksh` shell script for VIP, Retail, and Wholesale segment extractions. |
| `pyspark_scripts/crm_customer_scoring.py` | GCS Code Bucket (`gs://<code-bucket>/pyspark_scripts/`) | PySpark script replacing the legacy Ab Initio graph (`crm_customer_scoring.mp`) for centralized customer scoring. |
| `pyspark_scripts/customer_segmentation.py` | GCS Code Bucket (`gs://<code-bucket>/pyspark_scripts/`) | PySpark script replacing the legacy Scala Spark assembly (`crm-assembly.jar`) for modeling. |
| `pyspark_scripts/crm_lineage_tracker.py` | GCS Code Bucket (`gs://<code-bucket>/pyspark_scripts/`) | Python script replacing the legacy local lineage tracker for metadata logging. |

---

## 3. Key Design Decisions

### 3.1. Event-Driven Cross-Pipeline Synchronization
*   **Legacy Approach:** UC4 used native Event (`EVNT`) objects to block execution until upstream finance and retail processes completed.
*   **Airflow Approach:** Migrated to `GCSObjectExistenceSensor` tasks. Upstream pipelines (once migrated) will write empty marker files to GCS (`events/FINANCE_GL_CLOSE_COMPLETE_<YYYY-MM-DD>` and `events/RETAIL_DAILY_COMPLETE_<YYYY-MM-DD>`). This decouples the DAGs while maintaining strict cross-pipeline dependency management.

### 3.2. Soft-Failure Pass-Through (Non-Blocking Upstreams)
*   **Legacy Approach:** UC4 used `ON_FAILURE then CONTINUE` for the retail event wait and the lineage tracking steps.
*   **Airflow Approach:** 
    *   The `wait_retail_event` sensor has a timeout of 120 minutes. Downstream extraction tasks (`customer_extract_*`) use `trigger_rule=TriggerRule.ALL_DONE`. If the retail sensor times out, the extraction tasks still execute, allowing the workflow to proceed with stale daily data as designed.
    *   The `completion_notify` task uses `TriggerRule.ALL_DONE` to ensure that execution success emails are dispatched even if the non-blocking `python_lineage` metadata task fails.

### 3.3. Timezone-Aware Scheduling
*   **Legacy Approach:** Scheduled to run at 04:00 Europe/London.
*   **Airflow Approach:** The DAG uses a timezone-aware `pendulum.timezone("Europe/London")` instance. This guarantees that Airflow correctly handles Daylight Saving Time (DST) transitions twice a year without manual scheduler intervention.

### 3.4. Compute Modernization
*   **Legacy Approach:** Heavy reliance on dedicated Unix agents, Ab Initio licenses, and on-premise Yarn clusters.
*   **Airflow Approach:** Standardized on **Google Cloud Dataproc** using PySpark. This eliminates licensing costs, simplifies the technology stack to pure Python/PySpark, and leverages ephemeral or shared serverless scaling.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following infrastructure, security, and configuration steps must be completed:

### 4.1. Schema & Dataset Creation
Ensure that the target BigQuery datasets or Hive Metastore tables corresponding to the legacy databases are created and accessible:
*   `DW_OWNER.STG_CUSTOMER_SALES`
*   `DW_OWNER.FACT_REGIONAL_SUMMARY`
*   `FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION`

### 4.2. IAM & Permissions
The Cloud Composer service account (e.g., `service-XXXXXXXX@gcp-sa-composer.iam.gserviceaccount.com`) must be granted the following IAM roles:
*   **Dataproc Editor** (`roles/dataproc.editor`) on the target project to submit PySpark jobs.
*   **Storage Object Viewer** (`roles/storage.objectViewer`) on the code bucket.
*   **Storage Object Admin** (`roles/storage.objectAdmin`) on the events prefix to read/write pipeline signals.

### 4.3. Airflow Variables Configuration
The following variables must be populated in the Airflow UI (**Admin -> Variables**) or via the `gcloud composer environments run` CLI:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `gcp_project_id` | `prod-gcp-project-123` | The target GCP Project ID. |
| `dataproc_cluster_name` | `crm-shared-dataproc-cluster` | Name of the active Dataproc cluster. |
| `dataproc_region` | `europe-west2` | GCP Region where Dataproc is deployed. |
| `gcs_bucket_name` | `company-crm-etl-prod` | GCS Bucket containing scripts and event markers. |
| `env` | `PROD` | Environment identifier (`DEV`, `TEST`, `PROD`). |
| `crm_notify_email` | `crm-etl@company.com` | Primary email address for workflow alerts. |

### 4.4. Connection Strings & Secrets
*   **SMTP Configuration:** Ensure that Cloud Composer's SMTP settings are configured in the Airflow configuration overrides (or integrated with SendGrid) to allow the `EmailOperator` to send the completion and failure notifications.

### 4.5. Scheduling & Catchup
*   The DAG is configured with `catchup=False` and `start_date=datetime(2023, 1, 1)`. 
*   Upon deployment, the DAG will remain paused. Unpause the DAG in the Airflow UI to activate the weekly Sunday 04:00 schedule.

---

## 5. Known Gaps & Unresolved References

The migration tool flagged several source files as **NOT FOUND** in the legacy codebase export. These have been generated as Python stubs containing `NotImplementedError` and require manual development/refactoring before the DAG can run successfully:

1.  **`process_customer_data.py` (High Priority):**
    *   *Gap:* Legacy shell script `process_customer_data.ksh` was missing.
    *   *Action:* Translate the legacy shell script logic (which extracts VIP, Retail, and Wholesale segments) into PySpark and upload it to `gs://{GCS_BUCKET}/pyspark_scripts/process_customer_data.py`.
2.  **`crm_customer_scoring.py` (High Priority):**
    *   *Gap:* Legacy Ab Initio graph `crm_customer_scoring.mp` was missing.
    *   *Action:* Re-implement the Ab Initio scoring logic in PySpark and upload it to `gs://{GCS_BUCKET}/pyspark_scripts/crm_customer_scoring.py`.
3.  **`customer_segmentation.py` (Medium Priority):**
    *   *Gap:* Legacy Scala Spark assembly `crm-assembly.jar` was missing.
    *   *Action:* Extract the Scala logic from class `com.company.crm.CustomerSegmentation`, rewrite it in PySpark, and upload it to `gs://{GCS_BUCKET}/pyspark_scripts/customer_segmentation.py`.
4.  **`crm_lineage_tracker.py` (Low Priority):**
    *   *Gap:* Legacy Python lineage script was missing.
    *   *Action:* Re-create or migrate the metadata logging script and upload it to `gs://{GCS_BUCKET}/pyspark_scripts/crm_lineage_tracker.py`.
5.  **Unmigrated Upstream Triggers:**
    *   The upstream finance and retail pipelines are not yet migrated. To prevent this DAG from timing out during testing, mock files must be manually placed in GCS (see Section 6).

---

## 6. Validation

To validate the migrated workflow, perform the following steps:

### 6.1. Unit & Syntax Validation
Run a local syntax and DAG import check using the Airflow CLI in your development environment:
```bash
python3 dags/crm_weekly_workflow.py
```
*If no output/errors are returned, the DAG file is syntactically correct and imports successfully.*

### 6.2. Mocking Upstream Events for Integration Testing
Because the upstream pipelines are not yet migrated, you must manually simulate their completion signals to satisfy the sensors:

1.  **Trigger the DAG manually** from the Airflow UI for a specific logical date (e.g., `2023-10-29`).
2.  **Create the Finance Event Marker:**
    ```bash
    touch FINANCE_GL_CLOSE_COMPLETE_2023-10-29
    gsutil cp FINANCE_GL_CLOSE_COMPLETE_2023-10-29 gs://<YOUR_BUCKET_NAME>/events/
    ```
3.  **Create the Retail Event Marker:**
    ```bash
    touch RETAIL_DAILY_COMPLETE_2023-10-29
    gsutil cp RETAIL_DAILY_COMPLETE_2023-10-29 gs://<YOUR_BUCKET_NAME>/events/
    ```

### 6.3. What "Passing" Means
A validation run is considered successful when:
*   Both `wait_finance_event` and `wait_retail_event` transition to `SUCCESS`.
*   The three parallel extraction tasks execute and complete successfully.
*   The `abinitio_transform` and downstream `spark_segmentation` / `python_lineage` tasks complete without throwing errors.
*   An email notification with the subject `[CRM-OK] Weekly CRM Load 2023-10-29` is successfully received.
*   The Airflow task logs show the exact legacy log outputs:
    *   `[OK] CRM_WEEKLY_WORKFLOW completed for 2023-10-29` (on success).
    *   `[CRITICAL] CRM_WEEKLY_WORKFLOW FAILED for 2023-10-29` (if any critical task fails).

---

## 7. Rollback Procedure

In the event of a critical failure during the production cutover, execute the following rollback steps:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch next to `crm_weekly_workflow` to **Off (Paused)**. This prevents any further scheduled runs from triggering.
2.  **Re-enable the UC4 Schedule:**
    Log into the UC4 Automic interface, locate the `CRM_WEEKLY_WORKFLOW` object, and set its status back to **Active** (ensure the schedule is turned back on).
3.  **Verify Legacy Execution:**
    Monitor the next scheduled run in UC4 to ensure that the legacy agents, Ab Initio graphs, and Scala jobs execute and complete as they did pre-migration.
4.  **Triage:**
    Inspect the Airflow task logs and Cloud Logging to diagnose the root cause of the failure before attempting another deployment.