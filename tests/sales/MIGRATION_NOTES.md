# Migration Notes: SALES.PRODUCT_AND_SALES_EXTRACT

This document details the migration of the legacy UC4 UNIX job `SALES.PRODUCT_AND_SALES_EXTRACT` and its associated scripts/SQL files to Google Cloud Platform (GCP) utilizing **Cloud Composer (Apache Airflow 2.x)** and **Google BigQuery**.

---

## 1. Summary

The legacy UC4 UNIX job `SALES.PRODUCT_AND_SALES_EXTRACT` was hosted on the physical server `ETLHOST3`. It performed three core operational functions:
1. **Source Availability Check**: Polled the local filesystem for an upstream POS landing marker file (`pos_feed_${RUN_DATE}.done`) with a retry/sleep loop.
2. **Product Master SCD2 Load**: Executed an Oracle SQL*Plus script to perform a Slowly Changing Dimension (SCD) Type 2 merge on the product master table.
3. **Daily Sales Extract**: Executed an Oracle SQL*Plus script to extract daily transactions for a given run date into a staging table.

These components have been fully refactored and migrated to:
* **Orchestration**: Apache Airflow 2.x (GCP Cloud Composer).
* **Execution Logic**: Python 3.x scripts utilizing native Google Cloud SDKs (`google-cloud-storage` and `google-cloud-bigquery`).
* **Database Engine**: Google BigQuery (Standard SQL Scripting with explicit transaction controls).

---

## 2. Generated Artifacts

The migration process generated five distinct artifacts. The table below lists each file and its role in the target architecture:

| Generated File Path | Target Technology | Role / Description |
| :--- | :--- | :--- |
| `sales/SALES.PRODUCT_AND_SALES_EXTRACT.py` | Apache Airflow 2.x | The parent DAG file that defines the workflow, default arguments, environment variables, and task orchestration. |
| `sales/r_product_and_sales_extract.py` | Python 3.x | Wrapper script replacing the legacy `r_product_and_sales_extract.ksh`. Handles logging directory creation, timestamped log file generation, `RUN_DATE` validation, and execution of the core logic. |
| `sales/k_product_and_sales_extract.py` | Python 3.x | Core execution script replacing `k_product_and_sales_extract.ksh`. Implements GCS-native file polling and sequential BigQuery SQL execution via the BigQuery Python Client API. |
| `sales/d_daily_sales_extract.sql` | BigQuery SQL | Refactored BigQuery SQL Scripting block replacing the Oracle SQL*Plus script. Implements dynamic SQL formatting to inject environment variables and wraps the `DELETE` + `INSERT` operations in an atomic transaction. |
| `sales/d_product_master_load.sql` | BigQuery SQL | Refactored BigQuery SQL Scripting block replacing the Oracle SCD2 merge script. Implements a multi-statement transaction block using a session-scoped variable to guarantee temporal consistency. |

---

## 3. Key Design Decisions

### Python Conversion for Shell Orchestration
* **Decision**: Converted `k_product_and_sales_extract.ksh` and `r_product_and_sales_extract.ksh` to native Python 3.x scripts.
* **Reasoning**: The legacy scripts contained file-system polling (`[ -f ...]`) and sleep loops. These procedural operations cannot be executed natively inside BigQuery SQL. Python provides robust, cloud-native libraries (`google-cloud-storage`) to handle file polling without polluting the Airflow worker's local disk.

### GCS-Native File Polling
* **Decision**: Replaced local Unix file polling with Google Cloud Storage (GCS) blob detection.
* **Reasoning**: In a cloud-native serverless environment, local filesystems on Airflow workers are ephemeral. Upstream POS feeds will land in a GCS bucket. Polling is handled via the GCS Python client library checking for the existence of `gs://[GCS_BUCKET]/inbound/pos_feed_{run_date}.done`.

### BigQuery Scripting and Explicit Transactions
* **Decision**: Wrapped SQL operations in `BEGIN TRANSACTION ... COMMIT TRANSACTION` blocks.
* **Reasoning**: 
  * For `d_daily_sales_extract.sql`, the `DELETE` (staging purge) and `INSERT` (staging load) must execute atomically to prevent partial-failure states where staging is cleared but not reloaded.
  * For `d_product_master_load.sql`, the SCD Type 2 `MERGE` (expiring old records) and subsequent `INSERT` (adding new active versions) must execute as a single atomic unit to prevent dimension corruption.

### Temporal Consistency in SCD Type 2
* **Decision**: Declared a session-scoped variable `current_datetime_val` initialized to `CURRENT_DATETIME()` at the start of the SQL block.
* **Reasoning**: Replacing Oracle's `SYSDATE` with inline `CURRENT_DATETIME()` calls across separate statements inside a transaction can introduce microsecond discrepancies. This would cause the subsequent join to fail when matching newly expired rows. A single declared variable guarantees identical matching values.

### Dynamic SQL for Dataset Parameterization
* **Decision**: Utilized `EXECUTE IMMEDIATE FORMAT` in `d_daily_sales_extract.sql`.
* **Reasoning**: This avoids hardcoding GCP Project IDs and BigQuery Dataset names in the SQL files, allowing the same SQL artifact to be deployed seamlessly across Dev, Test, and Prod environments.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the migrated workflow, the following manual setup steps must be completed:

### 1. Schema and Dataset Creation
Ensure the target BigQuery dataset (matching the `BQ_DATASET` variable) exists, and create the following tables with schemas compatible with the migrated data types:
* `STG_DAILY_SALES`
* `SRC_POS_TRANSACTIONS`
* `DIM_STORE`
* `DIM_PRODUCT`
* `STG_PRODUCT_MASTER`

*Note: Ensure that numeric fields representing currency (e.g., `SALE_AMOUNT`, `UNIT_PRICE`) are defined as `NUMERIC` in BigQuery to prevent rounding errors.*

### 2. IAM and Permissions
The Service Account running the Cloud Composer environment (Airflow workers) must be granted the following IAM roles:
* **BigQuery**: `roles/bigquery.admin` (or a combination of `bigquery.dataEditor` and `bigquery.jobUser`) to execute scripts and transactions.
* **Google Cloud Storage**: `roles/storage.objectViewer` on the GCS bucket hosting the inbound landing files.

### 3. Airflow Variables Setup
Configure the following Airflow Variables in the Cloud Composer environment:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-prod` | The target GCP Project ID. |
| `GCS_BUCKET` | `my-retail-etl-bucket` | The GCS bucket where the inbound POS files land. |
| `BQ_DATASET` | `ANALYTICS_SCHEMA` | The target BigQuery dataset name. |
| `env_home` | `/home/airflow/gcs/dags` | The base directory where the Python and SQL scripts are stored. |

### 4. GCS Bucket Structure
Create the folder structure inside the designated `GCS_BUCKET`:
* `inbound/` (This is where the upstream process must write the `pos_feed_{RUN_DATE}.done` marker and data files).

---

## 5. Known Gaps & Unresolved References

### 1. Airflow DAG Task Operator (Placeholder Gap)
* **Gap**: The generated DAG `sales_product_and_sales_extract` currently uses an `EmptyOperator` placeholder task (`sales_product_and_sales_extract_task`) because the original UC4 launcher was unrecognized.
* **Resolution**: This task must be manually updated to execute the Python wrapper script. It is recommended to use the `BashOperator` or `KubernetesPodOperator` to invoke the script:
  ```python
  from airflow.operators.bash import BashOperator

  sales_product_and_sales_extract_task = BashOperator(
      task_id="sales_product_and_sales_extract_task",
      bash_command="python3 {{ var.value.env_home }}/sales/r_product_and_sales_extract.py",
      env={"RUN_DATE": "{{ ds }}"},
  )
  ```

### 2. Downstream Integration
* **Gap**: The downstream job `SALES.DAILY_SCHEDULE` has not yet been migrated.
* **Resolution**: Once `SALES.DAILY_SCHEDULE` is migrated to Airflow, add a `TriggerDagRunOperator` at the end of this DAG, or configure an `ExternalTaskSensor` in the downstream DAG to establish the dependency link.

### 3. SQL File Path Resolution
* **Gap**: The Python script `k_product_and_sales_extract.py` resolves SQL paths using `os.path.join(retail_home, "sales", ...)`.
* **Resolution**: Ensure that the SQL files (`d_product_master_load.sql` and `d_daily_sales_extract.sql`) are uploaded to the Cloud Composer DAGs bucket under the `/dags/sales/` directory, and that the Airflow variable `env_home` (which maps to `RETAIL_HOME`) is set to `/home/airflow/gcs/dags`.

---

## 6. Validation

To validate the migration, perform the following steps in a test environment:

### How to Run the Test
1. Upload all five generated files to their respective paths in the Cloud Composer DAGs bucket.
2. Ensure the Airflow variables (`GCP_PROJECT`, `GCS_BUCKET`, `BQ_DATASET`, `env_home`) are configured.
3. Upload a dummy marker file to GCS:
   ```bash
   gsutil cp /dev/null gs://[GCS_BUCKET]/inbound/pos_feed_[TEST_DATE].done
   ```
4. Trigger the DAG manually from the Airflow UI, overriding the configuration to pass the test date:
   ```json
   {
     "RUN_DATE": "[TEST_DATE]"
   }
   ```

### What "Passing" Means
The test is successful if:
* The polling loop in `k_product_and_sales_extract.py` detects the GCS marker file on the first check and logs:
  `Source feed marker found on check 1/10`.
* The Product Master SCD2 step executes without error, and the BigQuery table `DIM_PRODUCT` correctly reflects the updated/inserted records.
* The Daily Sales Extract step executes, purging any existing records in `STG_DAILY_SALES` for `[TEST_DATE]` and inserting the new transactions.
* The Airflow DAG run status transitions to **Success**.
* The generated log file in `gs://[GCS_BUCKET]/logs/product_and_sales_extract_[TIMESTAMP].log` contains the complete execution audit trail.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, execute the following rollback steps:

### 1. Database Rollback (Data State)
* **Staging Table (`STG_DAILY_SALES`)**:
  If the daily sales extract loaded corrupt data, purge the staging table for the affected run date:
  ```sql
  DELETE FROM `ANALYTICS_SCHEMA.STG_DAILY_SALES` WHERE SALE_DATE = 'YYYY-MM-DD';
  ```
* **Dimension Table (`DIM_PRODUCT`)**:
  Because SCD Type 2 updates are stateful, reverting them requires restoring the table to its pre-run state. Use BigQuery's Time Travel feature to restore the table to a point in time before the DAG execution:
  ```sql
  CREATE OR REPLACE TABLE `ANALYTICS_SCHEMA.DIM_PRODUCT`
  AS SELECT * FROM `ANALYTICS_SCHEMA.DIM_PRODUCT`
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
  ```

### 2. Orchestration Rollback
1. **Pause the Airflow DAG**: Go to the Airflow UI and toggle the switch to pause `sales_product_and_sales_extract`.
2. **Re-enable UC4 Job**: Re-activate the legacy `SALES.PRODUCT_AND_SALES_EXTRACT` job in the UC4 console.
3. **Verify Host Connectivity**: Ensure that the legacy execution environment on `ETLHOST3` is active and that the local filesystem paths are intact.