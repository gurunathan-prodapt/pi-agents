# Migration Notes: CRM Weekly Workflow (`customer/crm_weekly_workflow.xml`)

This document provides comprehensive migration notes for transitioning the legacy UC4 workflow `customer/crm_weekly_workflow.xml` to a modern, cloud-native orchestration and processing architecture on Google Cloud Platform (GCP).

---

## 1. Summary

The legacy UC4 workflow orchestrating the weekly CRM data pipeline has been migrated to **Google Cloud Composer (Airflow 2)** and **Cloud Dataproc (Serverless/Managed Spark)**. 

### Migration Scope
* **Orchestration:** Converted the UC4 XML workflow definition (`crm_weekly_workflow.xml`) into a Python-based Airflow DAG (`crm_weekly_workflow.py`).
* **Processing Logic:** Ported legacy shell scripts (`.ksh`), Ab Initio graphs (`.mp`), and Scala Spark jobs (`.scala`) into unified, maintainable **PySpark** applications.
* **Event-Driven Triggers:** Replaced legacy UC4 event triggers with Airflow GCS Sensors monitoring upstream state files.

### Target Platform Architecture
* **Orchestrator:** Cloud Composer (Airflow 2.x)
* **Compute Engine:** Cloud Dataproc (running PySpark jobs)
* **Storage & Staging:** Google Cloud Storage (GCS)
* **Data Warehouse:** Google BigQuery (replacing legacy relational/file-based targets)

---

## 2. Generated Artifacts

The migration process generated the following files, each serving a specific role in the target environment:

| Generated File Path | Role / Description |
| :--- | :--- |
| `dags/crm_weekly_workflow.py` | **Airflow DAG:** Orchestrates the entire weekly pipeline. Manages task dependencies, GCS sensors, Dataproc job submissions, SLA monitoring, and failure alerts. |
| `pyspark_scripts/process_customer_data.py` | **PySpark Application:** Replaces `process_customer_data.ksh`. Extracts customer segment data (VIP, RETAIL, WHOLESALE) from BigQuery and stages it in GCS as Parquet. |
| `pyspark_scripts/crm_customer_scoring.py` | **PySpark Application:** Replaces the Ab Initio graph `crm_customer_scoring.mp`. Performs data quality validation and calculates customer scores. |
| `pyspark_scripts/customer_segmentation.py` | **PySpark Application:** Replaces `customer_segmentation.scala`. Joins scored customer data with financial reconciliation data and writes the final cohort matrix to BigQuery. |
| `pyspark_scripts/crm_lineage_tracker.py` | **PySpark Application:** Replaces `crm_lineage_tracker.py`. Logs operational metadata, execution timestamps, and pipeline status to a centralized BigQuery lineage table. |

---

## 3. Key Design Decisions

### 1. Unified PySpark Runtime
* **Decision:** Convert all processing scripts (Shell, Ab Initio, and Scala) to PySpark.
* **Justification:** Standardizing on PySpark simplifies the CI/CD pipeline, reduces the skill-set footprint required for maintenance, and allows seamless integration with Cloud Dataproc without managing complex Scala assembly JARs or legacy proprietary runtimes (Ab Initio).

### 2. Idempotency via Deterministic Job IDs
* **Decision:** Implemented a custom Airflow user-defined filter `generate_deterministic_uuid` to generate reproducible Dataproc job IDs based on the Airflow `run_id` and `task_id`.
* **Justification:** This prevents duplicate job submissions on Dataproc in the event of Airflow task retries or scheduler hiccups, ensuring strict execution safety.

### 3. Non-Blocking Upstream Dependencies
* **Decision:** The `crm_wait_retail_event` sensor is configured with a 2-hour timeout, and the final notification task (`crm_completion_notify`) uses `TriggerRule.ALL_DONE`.
* **Justification:** This preserves the legacy behavior where retail data failures or delays are non-blocking. The pipeline will still attempt to complete and notify the team even if the retail sensor times out.

### 4. Dynamic Configuration via Airflow Variables
* **Decision:** Externalized environment-specific configurations (GCP Project, Region, Dataproc Cluster, GCS Bucket, Environment, and Notification Emails) into Airflow Variables.
* **Justification:** Decouples the pipeline code from environment topologies, enabling the exact same DAG file to run unmodified across `DEV`, `UAT`, and `PROD` environments.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following infrastructure, security, and configuration steps must be completed:

### 1. Schema & Dataset Creation (BigQuery)
Ensure the target BigQuery datasets and tables exist with appropriate schemas:
* `DW_OWNER.STG_CUSTOMER_SALES` (Source)
* `DW_OWNER.FACT_REGIONAL_SUMMARY` (Source)
* `FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION` (Source)
* `DW_OWNER.CRM_PIPELINE_LINEAGE_LOGS` (Target - Lineage)
* Target tables for segmented cohorts will be dynamically created as `DW_OWNER.FACT_CUSTOMER_SEGMENTATION_YYYY_MM_DD`.

### 2. IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
* **Dataproc Editor** (`roles/dataproc.editor`)
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the target GCS bucket.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) and **BigQuery Job User** (`roles/bigquery.jobUser`).

### 3. Airflow Variables Configuration
Import the following variables into the Cloud Composer Airflow UI (**Admin -> Variables**):

```json
{
  "GCP_PROJECT": "your-gcp-project-id",
  "GCP_REGION": "europe-west1",
  "DATAPROC_CLUSTER": "your-dataproc-cluster-name",
  "GCS_BUCKET": "your-gcs-data-bucket",
  "ENV": "PROD",
  "NOTIFY_EMAIL": "crm-etl@company.com"
}
```

### 4. Code Deployment
1. Upload the DAG file `crm_weekly_workflow.py` to the Composer DAGs bucket: `gs://<composer-dag-bucket>/dags/`.
2. Upload the four PySpark scripts to the designated GCS bucket:
   * `gs://<YOUR_GCS_BUCKET>/pyspark_scripts/process_customer_data.py`
   * `gs://<YOUR_GCS_BUCKET>/pyspark_scripts/crm_customer_scoring.py`
   * `gs://<YOUR_GCS_BUCKET>/pyspark_scripts/customer_segmentation.py`
   * `gs://<YOUR_GCS_BUCKET>/pyspark_scripts/crm_lineage_tracker.py`

### 5. Upstream Pipeline Alignment
Ensure that the upstream pipelines responsible for Finance and Retail data are configured to write their completion state files to:
* `gs://<YOUR_GCS_BUCKET>/finance/finance_daily.json`
* `gs://<YOUR_GCS_BUCKET>/sales/retail_daily.json`

---

## 5. Known Gaps & Unresolved References

* **Upstream State File Schema:** The GCS sensors check only for the *existence* of `finance_daily.json` and `retail_daily.json`. If these files are written before the actual data transfer is fully finalized, it could cause race conditions. Ensure upstream pipelines write these files as the absolute final step of their execution.
* **Hardcoded BigQuery Datasets:** The PySpark scripts reference hardcoded BigQuery datasets (`DW_OWNER` and `FINANCE_SCHEMA`). If these dataset names differ across environments (e.g., `DEV_DW_OWNER`), these should be refactored to be passed as command-line arguments or resolved via Airflow variables.
* **SLA Miss Callback Context:** The `on_sla_miss` callback executes with a limited context dictionary in Airflow. The current implementation uses a dummy context for the `EmailOperator` execution. If detailed task-level SLA metadata is required in the email, this callback must be expanded to parse the `slas` parameter.

---

## 6. Validation

To validate the migration in a non-production environment:

### 1. Local/CI Syntax Validation
Run Pytest or basic Python compilation checks on the DAG and PySpark files:
```bash
python -m py_compile dags/crm_weekly_workflow.py
python -m py_compile pyspark_scripts/*.py
```

### 2. Airflow DAG Import Test
Verify that the DAG is parsed by Airflow without errors:
```bash
airflow dags list-import-errors
```

### 3. End-to-End Dry Run
1. Seed the source BigQuery tables with mock data.
2. Manually upload dummy trigger files to GCS:
   ```bash
   touch finance_daily.json && gsutil cp finance_daily.json gs://<YOUR_GCS_BUCKET>/finance/
   touch retail_daily.json && gsutil cp retail_daily.json gs://<YOUR_GCS_BUCKET>/sales/
   ```
3. Trigger the DAG manually from the Airflow UI.
4. **Definition of "Passing":**
   * All tasks transition to `SUCCESS` (or `SKIPPED` where expected).
   * Parquet files are successfully written to `gs://<YOUR_GCS_BUCKET>/staging/` and `gs://<YOUR_GCS_BUCKET>/analytical/`.
   * The final table `DW_OWNER.FACT_CUSTOMER_SEGMENTATION_YYYY_MM_DD` is populated in BigQuery.
   * A new row is appended to `DW_OWNER.CRM_PIPELINE_LINEAGE_LOGS`.
   * A completion email is received.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live:

1. **Pause the Airflow DAG:** Immediately turn off the toggle switch for `crm_weekly_workflow` in the Airflow UI to prevent subsequent weekly runs.
2. **Kill Running Dataproc Jobs:** If a run is currently active and hung, navigate to the Dataproc Console, locate the jobs prefixed with `crm-`, and terminate them manually.
3. **Re-enable Legacy Scheduler:** Re-activate the weekly schedule for `CRM_WEEKLY_WORKFLOW` in the legacy UC4 engine.
4. **Clean Up Partial Cloud Outputs:** Run the following commands to clean up partial data writes in GCS and BigQuery to prevent duplicate records upon reprocessing:
   ```bash
   # Clean GCS Staging
   gsutil rm -rf gs://<YOUR_GCS_BUCKET>/staging/PROD/customer_extracts/*/<RUN_DATE>/
   gsutil rm -rf gs://<YOUR_GCS_BUCKET>/analytical/PROD/customer_scores/<RUN_DATE>/
   
   # Clean BigQuery Tables
   bq rm -f -t DW_OWNER.FACT_CUSTOMER_SEGMENTATION_<RUN_DATE_NODASH>
   ```