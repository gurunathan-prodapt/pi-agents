# MIGRATION_NOTES.md

## 1. Summary
The legacy UC4/Automic workflow `RETAIL_DAILY_WORKFLOW` (`JOBP`) has been migrated to **Google Cloud Composer (Apache Airflow)**. This daily retail sales Extraction, Transformation, and Loading (ETL) pipeline coordinates regional transactional data extraction, Slowly Changing Dimension (SCD) Type 2 processing, analytical aggregations, and data quality checks.

* **Source Workflow**: `RETAIL_DAILY_WORKFLOW` (8 internal jobs, 1 external dependency)
* **Target Platform**: Google Cloud Composer (Apache Airflow)
* **Execution Engines**: Google Cloud Dataproc (PySpark) & BigQuery / Oracle (Source POS)
* **Migration Pattern**: `UC4_ONLY` (1:1 translation of topologies, schedules, variables, and cross-domain dependencies)

---

## 2. Generated Artifacts
The migration process generated the following modular, production-ready files:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `sales/retail_daily_workflow.py` | **Core DAG Orchestrator** | Defines the Airflow DAG, task dependencies, schedules, retries, and SLA configurations. |
| `sales/gcp_dataproc_helpers.py` | **Infrastructure Helper** | Reusable utility module to generate standardized PySpark job configurations for Cloud Dataproc. |
| `sales/pipeline_notifications.py` | **Notification & Event Handler** | Centralizes failure callbacks and implements verbatim legacy event publication logic. |

---

## 3. Key Design Decisions

### Modular Code Architecture
Instead of a single monolithic DAG file, helper functions and notification callbacks are separated into `gcp_dataproc_helpers.py` and `pipeline_notifications.py`. This promotes reusability, simplifies unit testing, and keeps the main DAG file clean and maintainable.

### Environment Variable Policy Compliance
To avoid environment hardcoding, global infrastructure parameters (GCP Project, Dataproc Region, Cluster Name, and GCS Bucket) are resolved dynamically at runtime. The code attempts to read from OS environment variables first (standard in containerized/Composer environments) and falls back to Airflow Variables.

### Non-Blocking Data Quality Gate
The legacy task `RETAIL_DATA_QUALITY_CHECK` was designed to alert operations on failure but allow the workflow to continue. In Airflow, this is achieved by:
1. Setting `on_failure_callback=None` on the `retail_data_quality_check` task to prevent critical pipeline abort alarms.
2. Setting `trigger_rule=TriggerRule.ALL_DONE` on the downstream `retail_completion_notify` and `send_completion_email` tasks, ensuring they execute regardless of whether the data quality check succeeded or failed.

### Verbatim Literal Preservation
To guarantee downstream compatibility, the exact string formats and event names from the legacy system are preserved character-for-character:
* **Email Body**: `"RETAIL_DAILY_WORKFLOW completed for LOAD_DATE={LOAD_DATE}"`
* **Event Publication**: `"RETAIL_DAILY_COMPLETE"`

---

## 4. Manual Steps Before Go-Live

### Schema & Dataset Creation
1. Ensure the target BigQuery datasets and staging tables exist.
2. Verify that the Oracle POS source database contains the `SOURCE_OPS.SALES_TXN` table and that the schema is accessible.

### IAM & Permissions
Ensure the Cloud Composer environment's service account has the following IAM roles:
* `roles/dataproc.editor` (to submit PySpark jobs to the Dataproc cluster)
* `roles/storage.objectViewer` (to read PySpark scripts from GCS)
* `roles/composer.worker` (standard execution permissions)

### Connection Strings
Create an Airflow Connection for the Oracle POS source database:
* **Connection ID**: `oracle_dw_connection`
* **Connection Type**: `Oracle` (or `JDBC` depending on your environment's driver configuration)

### Secrets & Airflow Variables
Configure the following Airflow Variables in the Composer environment:
* `gcp_project`: Your Google Cloud Project ID
* `dataproc_region`: The GCP region where Dataproc runs (e.g., `europe-west1`)
* `dataproc_cluster`: The name of your active Dataproc cluster
* `gcs_bucket`: The GCS bucket containing your PySpark scripts (e.g., `retail-data-warehouse-bucket`)

### Scheduling & Script Deployment
1. Upload the PySpark scripts (`load_daily_sales.py`, `load_product_master.py`, `sales_rollup.py`, `sales_aggregation.py`, and `retail_data_quality.py`) to `gs://<your-gcs-bucket>/pyspark_scripts/`.
2. Upload the generated python files (`retail_daily_workflow.py`, `gcp_dataproc_helpers.py`, and `pipeline_notifications.py`) to the Composer DAGs folder (`gs://<composer-dag-bucket>/dags/`).

---

## 5. Known Gaps & Unresolved References

### Cross-Domain Dependency (B4 Redesign Item)
* **Reference**: `finance_gl_close_sensor` -> `finance_daily_workflow` (`finance_daily_gl_close`)
* **Status**: **Unresolved / Pending Migration**
* **Risk**: The upstream `FINANCE_DAILY_WORKFLOW` has not yet been migrated to Cloud Composer. 
* **Mitigation**: Until the finance workflow is migrated, the `finance_gl_close_sensor` will fail or timeout. For initial testing in lower environments, this sensor task can be temporarily mocked or disabled.

### Downstream Event Consumer
* **Reference**: `CRM_WEEKLY_WORKFLOW`
* **Status**: **External Dependency**
* **Risk**: The CRM weekly workflow expects the legacy event `RETAIL_DAILY_COMPLETE` to trigger its execution.
* **Mitigation**: The `publish_completion_event` task logs this event verbatim. Ensure that the downstream system's integration layer is configured to capture this log or that a Pub/Sub message is wired to this task to trigger the downstream CRM workflow.

---

## 6. Validation

### How to Run the Tests
1. **DAG Syntax & Import Test**:
   Run the following command in your local development environment or Cloud Shell to ensure there are no Python import or syntax errors:
   ```bash
   python3 dags/retail_daily_workflow.py
   ```
2. **Airflow CLI Backfill / Dry Run**:
   Perform a dry run of the DAG for a specific historical date to verify task rendering:
   ```bash
   airflow dags test retail_daily_workflow 2024-01-10
   ```

### What "Passing" Means
* **No Import Errors**: The DAG is successfully parsed by Airflow and appears in the Composer UI.
* **Task Rendering**: All tasks render their templates correctly (e.g., `{{ ds }}` resolves to `2024-01-10`).
* **Dependency Graph**: The DAG topology matches the legacy sequence, specifically verifying that `retail_product_master_load` waits for both regional extractions and the finance sensor.
* **Verbatim Output**: The task execution logs for `retail_completion_notify` output the exact string:
  `[VERBATIM ECHO OUTPUT]: RETAIL_DAILY_WORKFLOW completed for LOAD_DATE=2024-01-10`

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, execute the following rollback steps:

1. **Pause the Airflow DAG**:
   Immediately pause the `retail_daily_workflow` DAG in the Cloud Composer UI or via the gcloud CLI:
   ```bash
   gcloud composer environments run <env-name> \
       --location <region> \
       dags pause -- retail_daily_workflow
   ```
2. **Re-enable Legacy Scheduling**:
   Re-activate the `RETAIL_DAILY_WORKFLOW` active flag in the UC4/Automic UI to resume legacy orchestration.
3. **Remove Composer Artifacts**:
   Remove the migrated DAG file from the Composer bucket to prevent accidental execution:
   ```bash
   gsutil rm gs://<composer-dag-bucket>/dags/retail_daily_workflow.py
   ```