# Migration Notes: `sales/retail_daily_workflow.xml` to Cloud Composer (Airflow)

This document details the migration of the `RETAIL_DAILY_WORKFLOW` pipeline from UC4 to Google Cloud Composer (Airflow), following the high-confidence `UC4_ONLY` migration pattern.

---

## 1. Summary
The `RETAIL_DAILY_WORKFLOW` is a daily retail sales Extraction, Transformation, and Loading (ETL) pipeline serving the Retail Data Warehouse. It automates the staging extraction of sales transactions from regional Oracle point-of-sale (POS) databases, loads Slowly Changing Dimension (SCD) Type 2 product tables, executes an Ab Initio aggregation transformation, runs a Spark analytical aggregation, performs analytical data quality checks, and publishes a downstream event to trigger the CRM Weekly Workflow.

* **Source Platform:** UC4 / Automic Workload Automation (`sales/retail_daily_workflow.xml`)
* **Target Platform:** Google Cloud Composer (Airflow 2.x) & Google Cloud Dataproc
* **Schedule:** Daily at 02:00 Europe/London (handles GMT/BST transitions natively)

---

## 2. Generated Artifacts
The migration process generated the following files:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `dags/retail_daily_workflow.py` | Airflow DAG Definition | The core orchestration file containing task definitions, dependencies, schedules, and parameter mappings. |
| `pyspark_scripts/retail_pre_check.py` | Dataproc PySpark Script | Verifies the accessibility and data availability of the source Oracle POS database. |
| `pyspark_scripts/load_daily_sales.py` | Dataproc PySpark Script | Parameterized extraction script used for both Northern and Southern regional POS data. |
| `pyspark_scripts/load_product_master.py` | Dataproc PySpark Script | Processes and loads product Master dimension reference data (SCD Type 2). |
| `pyspark_scripts/sales_rollup.py` | Dataproc PySpark Script | Refactored PySpark implementation of the legacy Ab Initio `sales_rollup.xfr` aggregation rules. |
| `pyspark_scripts/retail_data_quality.py` | Dataproc PySpark Script | Performs schema, null-value, and threshold validation on loaded tables. |
| `jars/retail-etl-assembly.jar` | Scala Spark Binary | Pre-compiled Scala Spark assembly containing the core analytical aggregation logic (`com.company.retail.SalesAggregation`). |

---

## 3. Key Design Decisions

### Dataproc for Compute Offloading
To align with modern cloud architecture, heavy compute tasks (previously running on legacy local engines or Ab Initio servers) are offloaded to a managed Google Cloud Dataproc cluster. Airflow acts strictly as an orchestrator using the `DataprocSubmitJobOperator`.

### Parallel Regional Extraction
The extraction tasks for the Northern and Southern regions (`retail_stg_extract_north` and `retail_stg_extract_south`) run in parallel to minimize the overall batch window.

### Emulating UC4 "SUCCESS_OR_WARNING" Behavior
In UC4, the data quality check task could emit warnings (`SUCCESS_OR_WARNING`) without halting the pipeline. To replicate this behavior in Airflow:
* The `retail_data_quality_check` task runs with default success rules but is designed to handle soft-failures internally.
* Downstream notification and event publishing tasks (`retail_completion_notify_email` and `retail_completion_publish_event`) use `trigger_rule='all_done'` to ensure they execute even if upstream validation steps flag non-critical warnings.

### Pub/Sub Event Publishing
The legacy downstream trigger (`CRM_WEEKLY_WORKFLOW`) was migrated from a direct UC4 workflow call to a decoupled event-driven pattern using `PubSubPublishMessageOperator`. This isolates the Sales domain from the CRM domain.

---

## 4. Manual Steps Before Go-Live

### 1. Schema & Dataset Creation
* Ensure the target BigQuery datasets or Cloud Storage buckets exist to receive the extracted POS data.
* Ensure the Oracle source schema `SOURCE_OPS` and table `SALES_TXN` are accessible from the Dataproc cluster.

### 2. IAM & Permissions
The Cloud Composer environment service account must have the following roles:
* `roles/dataproc.editor` (to submit jobs to the Dataproc cluster)
* `roles/pubsub.publisher` (to publish to the `retail-daily-complete-topic` topic)
* `roles/storage.objectViewer` (to read scripts and JARs from GCS)

### 3. Connection Strings
Create an Airflow Connection for the Oracle database:
* **Conn ID:** `oracle_dw_login`
* **Conn Type:** `Oracle`
* **Host / Port / Schema / Credentials:** Configure according to your network topology (VPN/Interconnect required for on-premise Oracle POS).

### 4. Airflow Variables & Secrets
Configure the following Airflow Variables in the Composer UI or via CLI:

```bash
airflow variables set GCP_PROJECT "your-gcp-project-id"
airflow variables set DATAPROC_REGION "your-dataproc-region"
airflow variables set DATAPROC_CLUSTER "your-dataproc-cluster-name"
airflow variables set GCS_BUCKET "your-gcs-bucket-name"
airflow variables set env "PROD"
airflow variables set batch_mode "DAILY"
```

### 5. Scheduling & Catchup
The DAG is configured with `catchup=False` and `is_paused_upon_creation=False`. Ensure that the DAG is kept paused in the Airflow UI until the exact cutover date to prevent accidental backfilling.

---

## 5. Known Gaps & Unresolved References

### Upstream Dependency (`finance_daily_workflow`)
The `wait_for_finance_gl_close` task is an `ExternalTaskSensor` that monitors `finance_daily_workflow`. 
* **Gap:** If the Finance daily workflow has not yet been migrated to this Cloud Composer environment, this sensor will continuously timeout.
* **Mitigation:** For early testing phases, temporarily mock or disable this sensor task, or point it to a stub DAG.

### Downstream Trigger (`crm_weekly_workflow`)
The downstream pipeline is triggered via a Pub/Sub message published to `retail-daily-complete-topic`.
* **Gap:** The CRM team must implement a Pub/Sub sensor or trigger in their migrated Airflow DAG to consume this event.

---

## 6. Validation

### How to Run Local/Staging Tests
1. Upload the DAG file to the `dags/` folder of your Cloud Composer bucket.
2. Upload all PySpark scripts to `gs://{GCS_BUCKET}/pyspark_scripts/`.
3. Upload the Scala JAR to `gs://{GCS_BUCKET}/jars/`.
4. Trigger a manual run of the DAG via the Airflow UI with a specific execution date:
   ```bash
   airflow dags trigger -e "2024-01-02T02:00:00" retail_daily_workflow
   ```

### What "Passing" Means
* **`retail_pre_check`:** Successfully queries the Oracle POS database and confirms data exists for the previous day.
* **`retail_stg_extract_*`:** Parallel Dataproc jobs complete successfully, writing raw data to GCS staging.
* **`retail_product_master_load`:** Successfully processes SCD Type 2 dimensions.
* **`retail_abinitio_transform` & `retail_spark_aggregation`:** Run to completion on Dataproc without memory or executor errors.
* **`retail_completion_notify_email`:** An email notification is received at `dw-alerts@company.com`.
* **`retail_completion_publish_event`:** A message is successfully published to the GCP Pub/Sub topic.

---

## 7. Rollback Procedure

In the event of a critical failure during go-live deployment:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `retail_daily_workflow` to **Off**.
2. **Re-enable UC4 Schedule:**
   Log into the UC4 Automic console, locate the `RETAIL_DAILY_WORKFLOW` object, and set its status back to **Active**.
3. **Verify Database State:**
   If the migration failed mid-run, check the target database tables. If partial data was written, execute a cleanup script to remove records where `LOAD_DATE` matches the failed run date to prevent duplication when UC4 re-runs the batch.