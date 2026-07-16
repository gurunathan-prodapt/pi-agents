# Migration Validation Test Suite: `sales/retail_daily_workflow.xml`

This document defines the comprehensive migration-validation test suite to verify that the migrated Apache Airflow DAG (`retail_daily_workflow.py`) and its associated PySpark scripts running on Google Cloud Composer/Dataproc are behaviorally equivalent to the legacy UC4 workflow.

---

## Section 1: DAG Structural & Configuration Validation

### Test Case 1.1: Airflow DAG Topology and Properties Verification
* **Purpose**: Verify that the migrated Airflow DAG matches the legacy UC4 workflow properties, task dependencies, retry policies, timeouts, and trigger rules.
* **Setup**: 
  * Deploy `retail_daily_workflow.py`, `gcp_dataproc_helpers.py`, and `pipeline_notifications.py` to the Airflow `dags/` folder.
  * Initialize the Airflow metadata database.
* **Action**: Run a programmatic PyTest suite against the Airflow DAG structure.
* **Code**:
```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

@pytest.fixture(scope="module")
def dagbag():
    # Mock Airflow variables required for DAG parsing
    Variable.set("gcp_project", "test-project")
    Variable.set("dataproc_region", "europe-west1")
    Variable.set("dataproc_cluster", "test-cluster")
    Variable.set("gcs_bucket", "test-bucket")
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_properties(dagbag):
    dag = dagbag.get_dag(dag_id="retail_daily_workflow")
    assert dag is not None
    assert dag.schedule_interval == "0 2 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(minutes=240)
    assert dag.default_args['owner'] == 'DW_TEAM'
    assert dag.default_args['email'] == ['dw-alerts@company.com']

def test_task_dependencies(dagbag):
    dag = dagbag.get_dag(dag_id="retail_daily_workflow")
    
    # Verify pre-check triggers regional extractions
    pre_check = dag.get_task("retail_pre_check")
    assert "retail_stg_extract_north" in pre_check.downstream_task_ids
    assert "retail_stg_extract_south" in pre_check.downstream_task_ids

    # Verify merge barrier before product master load
    prod_master = dag.get_task("retail_product_master_load")
    upstream_tasks = prod_master.upstream_task_ids
    assert "retail_stg_extract_north" in upstream_tasks
    assert "retail_stg_extract_south" in upstream_tasks
    assert "finance_gl_close_sensor" in upstream_tasks

    # Verify non-blocking DQ check and downstream trigger rules
    dq_check = dag.get_task("retail_data_quality_check")
    completion_notify = dag.get_task("retail_completion_notify")
    
    assert dq_check.on_failure_callback is None
    assert completion_notify.trigger_rule == TriggerRule.ALL_DONE
    assert "retail_completion_notify" in dq_check.downstream_task_ids

def test_task_retry_policies(dagbag):
    dag = dagbag.get_dag(dag_id="retail_daily_workflow")
    
    assert dag.get_task("retail_pre_check").retries == 2
    assert dag.get_task("retail_pre_check").retry_delay == timedelta(seconds=120)
    
    assert dag.get_task("retail_stg_extract_north").retries == 3
    assert dag.get_task("retail_stg_extract_north").retry_delay == timedelta(seconds=60)
    
    assert dag.get_task("retail_abinitio_transform").retries == 0
```
* **Pass/Fail Criterion**: The PyTest execution must pass with 100% success, confirming that all task configurations, retries, timeouts, and trigger rules are identical to the legacy UC4 specification.

---

## Section 2: Input/Output Parity & Transformation Correctness

### Test Case 2.1: Regional Extraction Parity (Oracle vs. PySpark)
* **Purpose**: Prove that the PySpark-translated extraction script (`load_daily_sales.py`) produces identical staging outputs to the legacy KornShell script (`load_daily_sales.ksh`) for both North and South regions.
* **Setup**:
  * Populate the source Oracle POS database (`SOURCE_OPS.SALES_TXN`) with a controlled set of 10,000 transactions for a specific date (`2024-01-15`), including edge cases (NULL values, maximum numeric limits, and special characters in text fields).
  * Run the legacy extraction script to generate legacy staging files.
  * Run the migrated PySpark job on Dataproc to output to a GCS staging path.
* **Action**: Execute a comparison script that reads both outputs, normalizes schemas, and checks for differences.
* **Code**:
```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("ParityValidation").getOrCreate()

def test_extraction_parity(spark):
    # Load legacy output (e.g., CSV/Text extracted from Oracle)
    legacy_df = spark.read.csv("gs://test-bucket/legacy_staging/2024-01-15_NORTH.csv", header=True, inferSchema=True)
    
    # Load migrated PySpark output (Parquet/Avro/CSV)
    migrated_df = spark.read.parquet("gs://test-bucket/staging/north/load_date=2024-01-15/")
    
    # Align column names and order
    legacy_df = legacy_df.select(sorted(legacy_df.columns))
    migrated_df = migrated_df.select(sorted(migrated_df.columns))
    
    # Row count verification
    assert legacy_df.count() == migrated_df.count(), "Row counts do not match!"
    
    # Content verification via subtract
    diff_legacy_to_migrated = legacy_df.subtract(migrated_df)
    diff_migrated_to_legacy = migrated_df.subtract(legacy_df)
    
    assert diff_legacy_to_migrated.count() == 0, "Rows present in legacy but missing/different in migrated"
    assert diff_migrated_to_legacy.count() == 0, "Rows present in migrated but missing/different in legacy"
```
* **Pass/Fail Criterion**: Row counts must match exactly, and the symmetric difference (using Spark `subtract`) between the legacy and migrated datasets must be exactly 0.

### Test Case 2.2: SCD Type 2 Product Master Logic Verification
* **Purpose**: Verify that `load_product_master.py` correctly handles Slowly Changing Dimension (SCD) Type 2 logic, including record insertions, updates, history preservation, and active flag management.
* **Setup**:
  * Create a baseline `DIM_PRODUCT` table in BigQuery containing active and historical records.
  * Prepare an incoming delta dataset containing:
    * New products (Insert).
    * Existing products with changed attributes (Update/Expire old, Insert new).
    * Existing products with no changes (No-op).
* **Action**: Run the PySpark SCD Type 2 job and execute validation queries against the target BigQuery table.
* **Code**:
```sql
-- Assert that updated records have their previous version expired and a new active version created
WITH validation AS (
  SELECT 
    product_id,
    COUNT(*) as version_count,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) as active_count,
    SUM(CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) as open_end_date_count
  FROM 
    `${GCP_PROJECT}.${BQ_DATASET}.DIM_PRODUCT`
  GROUP BY 
    product_id
)
SELECT
  -- Every product must have exactly one active version
  ASSERT(active_count = 1, 'Product ' || CAST(product_id AS STRING) || ' does not have exactly 1 active record') as active_check,
  -- Every active version must have an open end_date (NULL or '9999-12-31')
  ASSERT(open_end_date_count = 1, 'Product ' || CAST(product_id AS STRING) || ' active record is missing open end date') as end_date_check
FROM 
  validation;
```
* **Pass/Fail Criterion**: The SQL assertion query must execute successfully without throwing assertion errors, confirming that SCD Type 2 integrity is maintained.

---

## Section 3: External System Replacements & Integration

### Test Case 3.1: Oracle Source Connectivity and Pre-Check Validation
* **Purpose**: Verify that the `retail_pre_check` task successfully connects to the target Oracle database and correctly evaluates the presence of source records.
* **Setup**:
  * Configure the Airflow connection `oracle_dw_connection` with valid test-database credentials.
  * Scenario A: Source records exist for the execution date.
  * Scenario B: No source records exist for the execution date.
* **Action**: Execute the `retail_pre_check` task independently for both scenarios.
* **Pass/Fail Criterion**:
  * **Scenario A**: The task completes with state `SUCCESS`.
  * **Scenario B**: The task fails with an database exception or returns 0 rows, triggering the retry mechanism and ultimately failing if no data is loaded within the retry window.

### Test Case 3.2: Cross-Domain Dependency Sensor Validation
* **Purpose**: Verify that the `finance_gl_close_sensor` correctly pauses execution until the upstream `finance_daily_workflow` DAG's `finance_daily_gl_close` task completes successfully.
* **Setup**:
  * Trigger a run of `retail_daily_workflow` while the upstream `finance_daily_workflow` is still running or has not started.
* **Action**: Monitor the state of `finance_gl_close_sensor`.
* **Pass/Fail Criterion**:
  * The sensor must remain in a `sensing` (up_for_reschedule/poke) state while the upstream task is incomplete.
  * Once the upstream task `finance_daily_gl_close` transitions to `SUCCESS`, the sensor must immediately transition to `SUCCESS` on its next poke interval.

---

## Section 4: Verbatim Literal & Notification Verification

### Test Case 4.1: Verbatim Completion Notification and Event Publication
* **Purpose**: Prove that the migrated pipeline preserves the exact string literals and event publication mechanisms expected by downstream legacy systems (such as `CRM_WEEKLY_WORKFLOW`).
* **Setup**:
  * Execute the `retail_completion_notify` task within a test DAG run for `logical_date = '2024-01-15'`.
* **Action**: Capture and inspect stdout/stderr logs and the published event payload.
* **Code**:
```python
def test_verbatim_notifications(caplog):
    import logging
    from pipeline_notifications import publish_completion_event
    
    context = {'ds': '2024-01-15'}
    
    with caplog.at_level(logging.INFO):
        publish_completion_event(**context)
        
    # Verify Verbatim String Literal 1: Output String Preservation
    expected_msg = "RETAIL_DAILY_WORKFLOW completed for LOAD_DATE=2024-01-15"
    assert any(expected_msg in record.message for record in caplog.records), \
        f"Verbatim completion message not found. Expected: '{expected_msg}'"
        
    # Verify Verbatim String Literal 2: Published Event Preservation
    expected_event = "[VERBATIM EVENT PUBLISH]: uc4api publish_event RETAIL_DAILY_COMPLETE date=2024-01-15"
    assert any(expected_event in record.message for record in caplog.records), \
        f"Verbatim event publication string not found. Expected: '{expected_event}'"
```
* **Pass/Fail Criterion**: The log assertions must pass, proving that the exact legacy strings are preserved character-for-character.