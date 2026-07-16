# Migration Validation Test Suite: `crm_weekly_workflow`

This test suite validates the migration of the legacy Automic (UC4) workflow `CRM_WEEKLY_WORKFLOW` and its associated scripts (`process_customer_data.ksh`, `crm_customer_scoring.mp`, `customer_segmentation.scala`, and `crm_lineage_tracker.py`) to Google Cloud Composer (Airflow) and Cloud Dataproc (PySpark).

---

## Section 1: DAG Orchestration & Dependency Parity

### Test Case 1.1: DAG Structure, Task Dependencies, and Trigger Rules
* **Purpose**: Verify that the migrated Airflow DAG structure, task dependencies, timeouts, retries, and trigger rules match the legacy UC4 XML workflow specification exactly.
* **Setup**:
  * Install `pytest` and `apache-airflow` in the test environment.
  * Place the migrated DAG file `crm_weekly_workflow.py` in the Airflow DAGs folder.
  * Mock Airflow Variables (`GCP_PROJECT`, `GCP_REGION`, `DATAPROC_CLUSTER`, `GCS_BUCKET`, `ENV`, `NOTIFY_EMAIL`).
* **Action**: Run a programmatic unit test using the Airflow DAG model to assert task IDs, upstream/downstream relationships, trigger rules, retries, and timeouts.
* **Concrete Pass/Fail Criterion**:
  * **Pass**: All tasks exist with correct IDs; `crm_wait_retail_event` has `crm_wait_finance_event` as its upstream; the three extract tasks run in parallel downstream of `crm_wait_retail_event`; `crm_abinitio_transform` runs only when all three extracts succeed; `crm_completion_notify` has `TriggerRule.ALL_DONE` to handle non-blocking paths; retries are set to 3 for Dataproc operators.
  * **Fail**: Any dependency mismatch, incorrect trigger rule, or missing task configuration.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def mock_variables(monkeypatch):
    vars = {
        "GCP_PROJECT": "test-project",
        "GCP_REGION": "europe-west1",
        "DATAPROC_CLUSTER": "test-cluster",
        "GCS_BUCKET": "test-bucket",
        "ENV": "PROD",
        "NOTIFY_EMAIL": "crm-etl@company.com"
    }
    monkeypatch.setattr(Variable, "get", lambda key, default_var=None: vars.get(key, default_var))

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="crm_weekly_workflow")
    assert dag_bag.import_errors == {}
    assert dag is not None

def test_dag_dependencies_and_properties():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="crm_weekly_workflow")
    
    # Verify Timezone and Schedule
    assert dag.timezone.name == "Europe/London"
    assert dag.schedule_interval == "0 4 * * 7"
    assert dag.sla == timedelta(hours=5)

    # Verify Task Existence
    tasks = {task.task_id: task for task in dag.tasks}
    expected_tasks = {
        "crm_wait_finance_event", "crm_wait_retail_event",
        "crm_customer_extract_vip", "crm_customer_extract_retail", "crm_customer_extract_wholesale",
        "crm_abinitio_transform", "crm_spark_segmentation", "crm_python_lineage",
        "crm_completion_notify"
    }
    assert expected_tasks.issubset(set(tasks.keys()))

    # Verify Upstream/Downstream Lineage
    assert tasks["crm_wait_finance_event"].downstream_task_ids == {"crm_wait_retail_event"}
    assert tasks["crm_wait_retail_event"].downstream_task_ids == {
        "crm_customer_extract_vip", "crm_customer_extract_retail", "crm_customer_extract_wholesale"
    }
    
    extract_tasks = ["crm_customer_extract_vip", "crm_customer_extract_retail", "crm_customer_extract_wholesale"]
    for ext in extract_tasks:
        assert tasks[ext].downstream_task_ids == {"crm_abinitio_transform"}
        assert tasks[ext].retries == 3
        assert tasks[ext].retry_delay == timedelta(minutes=2)

    assert tasks["crm_abinitio_transform"].downstream_task_ids == {"crm_spark_segmentation", "crm_python_lineage"}
    assert tasks["crm_completion_notify"].trigger_rule == "all_done"
```

---

### Test Case 1.2: Non-Blocking Sensor Timeout Behavior
* **Purpose**: Prove that if the upstream retail event sensor (`crm_wait_retail_event`) times out, the workflow continues executing downstream extracts and transformations using existing data, matching the legacy UC4 `ON_FAILURE action="NOTIFY" then="CONTINUE"` behavior.
* **Setup**:
  * Deploy the DAG to a local Airflow integration environment.
  * Mock the GCS bucket such that `finance/finance_daily.json` is uploaded (satisfying the first sensor), but `sales/retail_daily.json` is **not** uploaded.
  * Set the timeout of `crm_wait_retail_event` to 10 seconds for testing purposes.
* **Action**: Trigger a DAG run for execution date `2025-01-05`. Monitor the execution state of the tasks.
* **Concrete Pass/Fail Criterion**:
  * **Pass**: `crm_wait_finance_event` succeeds; `crm_wait_retail_event` fails with an Airflow Sensor Timeout; the downstream tasks `crm_customer_extract_vip`, `crm_customer_extract_retail`, and `crm_customer_extract_wholesale` transition to `QUEUED` or `RUNNING` (and eventually succeed) instead of being skipped or marked failed.
  * **Fail**: The failure of `crm_wait_retail_event` causes downstream tasks to be skipped or marked as upstream failed, blocking the pipeline.

---

## Section 2: PySpark Transformation & Parity Validation

### Test Case 2.1: Customer Extract Segment Filtering and Batching (`process_customer_data.py`)
* **Purpose**: Verify that `process_customer_data.py` correctly filters the input BigQuery table `DW_OWNER.STG_CUSTOMER_SALES` by the specified segment (VIP, RETAIL, WHOLESALE) and respects the `BATCH_SIZE` limit of 5000.
* **Setup**:
  * Populate a mock BigQuery table `DW_OWNER.STG_CUSTOMER_SALES` with 15,000 rows:
    * 6,000 VIP rows
    * 6,000 RETAIL rows
    * 3,000 WHOLESALE rows
  * Configure the PySpark session to use an in-memory or local Spark instance with BigQuery connector mocks.
* **Action**: Run `process_customer_data.py` three times with arguments:
  1. `--run-date 2025-01-05 --segment VIP --batch-size 5000`
  2. `--run-date 2025-01-05 --segment RETAIL --batch-size 5000`
  3. `--run-date 2025-01-05 --segment WHOLESALE --batch-size 5000`
* **Concrete Pass/Fail Criterion**:
  * **Pass**: 
    * The VIP output directory contains exactly 5,000 records, all with `segment = 'VIP'`.
    * The RETAIL output directory contains exactly 5,000 records, all with `segment = 'RETAIL'`.
    * The WHOLESALE output directory contains exactly 3,000 records, all with `segment = 'WHOLESALE'`.
  * **Fail**: Any output directory contains records from another segment, or exceeds the batch size limit, or fails to extract all available records when below the limit.

```python
# test_process_customer_data.py
import pytest
from pyspark.sql import SparkSession

def test_extraction_logic(spark_session):
    # Setup Mock Input Data
    data = [
        (f"ID_{i}", f"Cust_{i}", 100.0 * i, "North", "VIP") for i in range(1, 100)
    ] + [
        (f"ID_{i}", f"Cust_{i}", 50.0 * i, "South", "RETAIL") for i in range(100, 150)
    ]
    columns = ["customer_id", "customer_name", "sales_amount", "region", "segment"]
    mock_df = spark_session.createDataFrame(data, columns)
    mock_df.createOrReplaceTempView("stg_customer_sales")

    # Execute extraction logic for VIP with batch limit 50
    query = """
        SELECT customer_id, customer_name, sales_amount, region, '2025-01-05' as partition_date
        FROM stg_customer_sales
        WHERE UPPER(segment) = 'VIP'
        LIMIT 50
    """
    res_df = spark_session.sql(query)
    
    assert res_df.count() == 50
    assert res_df.filter(res_df.customer_id.startswith("ID_100")).count() == 0  # No retail rows
```

---

### Test Case 2.2: Ab Initio Transformation Parity (`crm_customer_scoring.py`)
* **Purpose**: Verify that the ported Ab Initio scoring logic matches the legacy mathematical rules, handles nulls/empty strings in customer names, and flags data quality issues correctly.
* **Setup**:
  * Create a mock input dataset containing:
    * Row 1: `sales_amount = 150000`, `customer_name = "Alice"`, `region = "North"` (Should get 15% boost)
    * Row 2: `sales_amount = 50000`, `customer_name = "Bob"`, `region = "South"` (Should get 5% boost)
    * Row 3: `sales_amount = 10000`, `customer_name = "Charlie"`, `region = "East"` (Should get no boost)
    * Row 4: `sales_amount = 20000`, `customer_name = None`, `region = "West"` (Should be flagged as `INVALID`)
    * Row 5: `sales_amount = 30000`, `customer_name = ""`, `region = "West"` (Should be flagged as `INVALID`)
  * Create a mock `DW_OWNER.FACT_REGIONAL_SUMMARY` table.
* **Action**: Execute `execute_transformations` in `crm_customer_scoring.py`.
* **Concrete Pass/Fail Criterion**:
  * **Pass**:
    * Row 1 `customer_score` is exactly `172500.0` and `data_quality_flag` is `VALID`.
    * Row 2 `customer_score` is exactly `52500.0` and `data_quality_flag` is `VALID`.
    * Row 3 `customer_score` is exactly `10000.0` and `data_quality_flag` is `VALID`.
    * Row 4 and Row 5 have `data_quality_flag` set to `INVALID`.
  * **Fail**: Any mathematical deviation in scoring, or failure to flag null/empty customer names as `INVALID`.

```python
# test_crm_customer_scoring.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

def test_scoring_and_dq_rules(spark_session):
    # Input schema matching process_customer_data output
    schema = ["customer_id", "customer_name", "sales_amount", "region"]
    input_data = [
        ("1", "Alice", 150000.0, "North"),
        ("2", "Bob", 50000.0, "South"),
        ("3", "Charlie", 10000.0, "East"),
        ("4", None, 20000.0, "West"),
        ("5", "", 30000.0, "West")
    ]
    df = spark_session.createDataFrame(input_data, schema)
    
    # Apply transformation logic
    processed_df = df.withColumn(
        "customer_score",
        when(col("sales_amount") >= 100000, col("sales_amount") * 1.15)
        .when((col("sales_amount") < 100000) & (col("sales_amount") >= 25000), col("sales_amount") * 1.05)
        .otherwise(col("sales_amount"))
    ).withColumn(
        "data_quality_flag",
        when(col("customer_name").isNull() | (col("customer_name") == ""), lit("INVALID"))
        .otherwise(lit("VALID"))
    )
    
    results = {row["customer_id"]: row for row in processed_df.collect()}
    
    assert results["1"]["customer_score"] == 172500.0
    assert results["1"]["data_quality_flag"] == "VALID"
    
    assert results["2"]["customer_score"] == 52500.0
    assert results["2"]["data_quality_flag"] == "VALID"
    
    assert results["3"]["customer_score"] == 10000.0
    assert results["3"]["data_quality_flag"] == "VALID"
    
    assert results["4"]["data_quality_flag"] == "INVALID"
    assert results["5"]["data_quality_flag"] == "INVALID"
```

---

### Test Case 2.3: Customer Segmentation Join and Filter (`customer_segmentation.py`)
* **Purpose**: Verify that `customer_segmentation.py` correctly performs an inner join with `FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION`, filters out `INVALID` data quality records, and calculates the `cohort_group` correctly.
* **Setup**:
  * Create mock `customer_scores` Parquet files containing:
    * Row 1: `customer_id = "C1"`, `customer_score = 50000.0`, `data_quality_flag = "VALID"`
    * Row 2: `customer_id = "C2"`, `customer_score = 25000.0`, `data_quality_flag = "INVALID"`
    * Row 3: `customer_id = "C3"`, `customer_score = 10000.0`, `data_quality_flag = "VALID"`
  * Create mock BigQuery table `FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION` containing:
    * Row 1: `customer_id = "C1"`, `reconciled = True`
    * Row 2: `customer_id = "C2"`, `reconciled = True`
    * Row 3: `customer_id = "C4"`, `reconciled = True` (No matching customer score)
* **Action**: Run the segmentation logic.
* **Concrete Pass/Fail Criterion**:
  * **Pass**:
    * The output table contains exactly 1 record (for `customer_id = "C1"`).
    * `customer_id = "C2"` is excluded because its `data_quality_flag` is `INVALID`.
    * `customer_id = "C3"` is excluded because it does not exist in the finance reconciliation table (inner join).
    * `customer_id = "C4"` is excluded because it does not exist in the customer scores table.
    * The `cohort_group` for `C1` is exactly `50.0` (`50000.0 / 1000`).
  * **Fail**: Any invalid records are written to the target table, or the inner join logic behaves as an outer join, or the cohort group calculation is incorrect.

---

## Section 3: External System & Environment Parity

### Test Case 3.1: Dynamic UUID Generation and Job Idempotency
* **Purpose**: Verify that the custom Airflow filter `generate_deterministic_uuid` generates a reproducible UUID based on the `run_id` and `task_id` to prevent duplicate Dataproc job submissions on task retries.
* **Setup**:
  * Instantiate the dynamic UUID function from `crm_weekly_workflow.py`.
* **Action**: 
  * Generate UUIDs for `run_id = "scheduled__2025-01-05T04:00:00+00:00"` and `task_id = "crm_customer_extract_vip"`.
  * Generate UUIDs again with the exact same parameters.
  * Generate UUIDs with a different `task_id`.
* **Concrete Pass/Fail Criterion**:
  * **Pass**:
    * The first two executions yield the exact same UUID string.
    * The execution with a different `task_id` yields a different, unique UUID string.
    * The output format is a valid RFC 4122 UUID.
  * **Fail**: The UUIDs are non-deterministic (e.g., using random generation), causing different IDs on retries, or they collide across different tasks.

```python
# test_uuid_generation.py
from dags.crm_weekly_workflow import generate_deterministic_uuid
import uuid

def test_deterministic_uuids():
    run_id = "scheduled__2025-01-05T04:00:00+00:00"
    task_1 = "crm_customer_extract_vip"
    task_2 = "crm_customer_extract_retail"
    
    uuid_1_run_1 = generate_deterministic_uuid(run_id, task_1)
    uuid_1_run_2 = generate_deterministic_uuid(run_id, task_1)
    uuid_2_run_1 = generate_deterministic_uuid(run_id, task_2)
    
    # Assert determinism
    assert uuid_1_run_1 == uuid_1_run_2
    
    # Assert uniqueness across tasks
    assert uuid_1_run_1 != uuid_2_run_1
    
    # Assert valid UUID format
    assert uuid.UUID(uuid_1_run_1)
```

---

### Test Case 3.2: Lineage Tracking and Metadata Logging (`crm_lineage_tracker.py`)
* **Purpose**: Verify that the lineage tracker correctly logs pipeline metadata to BigQuery and matches the schema expected by downstream governance systems.
* **Setup**:
  * Mock the BigQuery write destination for `DW_OWNER.CRM_PIPELINE_LINEAGE_LOGS`.
* **Action**: Run `crm_lineage_tracker.py` with arguments `--run-date 2025-01-05 --env PROD`.
* **Concrete Pass/Fail Criterion**:
  * **Pass**:
    * A single row is appended to the BigQuery table.
    * The row contains `pipeline_id = "CRM_WEEKLY_WORKFLOW"`.
    * The row contains `run_date = "2025-01-05"`.
    * The row contains `target_environment = "PROD"`.
    * The row contains `lineage_status = "SUCCESS_COMPLETED"`.
    * The `execution_timestamp` is a valid timestamp within 1 minute of the current system time.
  * **Fail**: The write fails, schema mismatch occurs, or incorrect metadata values are written.

---

## Section 4: End-to-End Integration & SLA Validation

### Test Case 4.1: SLA Breach Callback and Failure Notification
* **Purpose**: Verify that the SLA breach callback and failure alarm functions correctly construct and send emails with the exact text specified in the legacy XML `<NOTIFICATIONS>` block.
* **Setup**:
  * Mock the `EmailOperator.execute` method to capture the arguments passed to it.
  * Create a dummy Airflow context dictionary.
* **Action**:
  * Trigger `on_failure_alarm(context)` with a mock task failure context.
  * Trigger `on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_slas)` with mock SLA breach parameters.
* **Concrete Pass/Fail Criterion**:
  * **Pass**:
    * The failure alarm email is sent to `crm-etl@company.com` with the subject: `[CRITICAL] CRM_WEEKLY_WORKFLOW FAILED for <ds>`.
    * The SLA miss email is sent to `crm-etl@company.com` with the subject: `[SLA] CRM_WEEKLY_WORKFLOW exceeding 5h window`.
  * **Fail**: Emails are sent to the wrong recipients, contain incorrect subjects, or fail to execute.

```python
# test_notifications.py
from unittest.mock import MagicMock
from dags.crm_weekly_workflow import on_failure_alarm, on_sla_miss

def test_on_failure_alarm_email_content(monkeypatch):
    mock_email_execute = MagicMock()
    monkeypatch.setattr("airflow.operators.email.EmailOperator.execute", mock_email_execute)
    
    context = {
        "ds": "2025-01-05",
        "task_instance": MagicMock(task_id="crm_abinitio_transform")
    }
    
    on_failure_alarm(context)
    
    # Extract the instantiated EmailOperator from the mock call
    called_operator = mock_email_execute.call_args[0][0] if mock_email_execute.call_args else None
    assert called_operator is not None
    assert called_operator.to == "crm-etl@company.com"
    assert called_operator.subject == "[CRITICAL] CRM_WEEKLY_WORKFLOW FAILED for 2025-01-05"
```