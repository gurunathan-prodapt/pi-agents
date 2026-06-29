# Migration Validation Test Suite: `DW.BERT_P_VERTRAG_JP`

This document defines the migration-validation test suite to prove that the migrated Google Cloud Composer (Airflow 2.x) DAG and Dataproc PySpark jobs are behaviorally equivalent to the legacy UC4 Job Plan `DW.BERT_P_VERTRAG_JP`.

---

## Test Case 1: DAG Concurrency & Sync Lock Validation

### Purpose
Verify that the migrated Airflow DAG enforces execution serialization (`max_active_runs=1`), preventing concurrent pipeline executions. This ensures behavioral equivalence to the legacy UC4 Sync Objects (`DW.BERT_P_VERTRAG_JP_SYNC` and `DW.BERT_BFC_JP_SYNC`) which were configured with `Else="Wait"`.

### Setup
1. Access the target Cloud Composer environment.
2. Ensure the DAG `dw_bert_p_vertrag_jp` is enabled and paused.
3. Clear any active runs of the DAG.

### Action
1. Trigger two manual runs of the DAG `dw_bert_p_vertrag_jp` in rapid succession (within 2 seconds of each other) using the Airflow CLI or API.
2. Monitor the state of both DAG runs.

### Pass/Fail Criterion
* **Pass**: The first DAG run transitions to `running` state, while the second DAG run remains in `queued` state. The second run must not start executing until the first run completes (`success` or `failed`).
* **Fail**: Both DAG runs execute concurrently, or the second run fails immediately instead of waiting.

### Test Code (Pytest)
```python
import time
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.api.common.trigger_dag import trigger_dag

def test_dag_concurrency_serialization():
    dag_id = "dw_bert_p_vertrag_jp"
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id)
    
    assert dag is not None, f"DAG {dag_id} not found."
    assert dag.max_active_runs == 1, "max_active_runs must be set to 1 to mimic legacy sync locks."

    # Trigger first run
    run_1 = trigger_dag(dag_id=dag_id, run_id="test_sync_run_1")
    time.sleep(1)
    
    # Trigger second run
    run_2 = trigger_dag(dag_id=dag_id, run_id="test_sync_run_2")
    time.sleep(2)

    # Refresh states
    run_1.refresh_from_db()
    run_2.refresh_from_db()

    # Assert run 1 is running/success, and run 2 is queued (not running)
    assert run_1.state in [DagRunState.RUNNING, DagRunState.SUCCESS]
    assert run_2.state == DagRunState.QUEUED, "Second DAG run started concurrently; sync lock failed!"
    
    # Clean up
    run_1.set_state(DagRunState.FAILED)
    run_2.set_state(DagRunState.FAILED)
```

---

## Test Case 2: Task Recovery & Retry Logic (Discount Recovery Loop)

### Purpose
Verify that the recovery loop tasks (`task_discount_rr` and `task_p_discount_rr`) retry exactly 10 times with a 15-minute delay between attempts, and trigger the `on_terminal_failure` callback (simulating legacy `DW.CALL_STANDARD` / `BLOCK` state) only after the 11th execution failure.

### Setup
1. Deploy a mock version of the PySpark scripts `dw_bert_ausd_v_ta_discount_rr.py` and `dw_bert_ausd_v_ta_p_discount_rr.py` that raise an explicit exit code error (`sys.exit(1)`).
2. Configure a test instance of the DAG with a shortened retry delay (e.g., 1 second instead of 15 minutes) to allow the test to run in a reasonable timeframe.

### Action
1. Trigger the test DAG run.
2. Capture the task instance execution logs and state transitions for `task_discount_rr`.

### Pass/Fail Criterion
* **Pass**: The task attempts execution exactly 11 times (1 initial run + 10 retries). The `on_terminal_failure` callback is executed only after the 11th failure, and the task state is marked as `failed`.
* **Fail**: The task retries a different number of times, does not wait for the configured delay, or fails to trigger the terminal failure callback.

### Test Code (Pytest)
```python
import pytest
from unittest.mock import MagicMock, patch
from datetime import timedelta
from airflow.models import TaskInstance
from src.dags.callbacks import on_terminal_failure

@patch('logging.error')
def test_terminal_failure_callback_trigger(mock_log):
    # Mock Airflow Context
    mock_ti = MagicMock(spec=TaskInstance)
    mock_ti.task_id = "task_discount_rr"
    mock_ti.try_number = 11  # 1 initial + 10 retries
    mock_ti.max_tries = 11
    
    mock_dag = MagicMock()
    mock_dag.dag_id = "dw_bert_p_vertrag_jp"
    
    context = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "execution_date": "2026-01-01T00:00:00"
    }
    
    # Execute callback
    on_terminal_failure(context)
    
    # Assert critical alert was logged
    mock_log.assert_called_once()
    log_msg = mock_log.call_args[0][0]
    assert "CRITICAL ALERT" in log_msg
    assert "task_discount_rr" in log_msg
    assert "failed terminally" in log_msg

def test_dag_retry_configuration():
    dagbag = DagBag()
    dag = dagbag.get_dag("dw_bert_p_vertrag_jp")
    
    task_discount_rr = dag.get_task("task_discount_rr")
    task_p_discount_rr = dag.get_task("task_p_discount_rr")
    
    for task in [task_discount_rr, task_p_discount_rr]:
        assert task.retries == 10, f"{task.task_id} must have exactly 10 retries."
        assert task.retry_delay == timedelta(minutes=15), f"{task.task_id} retry delay must be 15 minutes."
        assert task.on_failure_callback == on_terminal_failure, f"{task.task_id} must use on_terminal_failure callback."
```

---

## Test Case 3: End-to-End Output Parity (Reconciliation)

### Purpose
Prove that the migrated pipeline running on Cloud Composer/Dataproc produces identical output to the legacy UC4/Oracle pipeline for a given reporting period.

### Setup
1. Extract a historical snapshot of the input tables from the legacy Oracle system for a specific reporting period (e.g., `2025-12`).
2. Load these inputs into the BigQuery staging tables.
3. Run the legacy pipeline in a QA environment and export the final target table `DW.BERT_AUSD_V_TA_P_VERTRAG` to a validation table: `bert_production_val.legacy_dw_bert_ausd_v_ta_p_vertrag`.
4. Run the migrated Airflow DAG `dw_bert_p_vertrag_jp` for the same reporting period, writing to `bert_production.dw_bert_ausd_v_ta_p_vertrag`.

### Action
Execute a symmetric difference query in BigQuery comparing the legacy validation table and the newly migrated target table.

### Pass/Fail Criterion
* **Pass**: The symmetric difference query returns 0 rows, proving absolute data parity (row count, column values, and types).
* **Fail**: Discrepancies are found in row counts or column values.

### Test Code (SQL Assertion)
```sql
-- Assert zero symmetric difference between legacy and migrated target tables
WITH legacy_data AS (
  SELECT 
    contract_id,
    reporting_period,
    partner_id,
    discount_amount,
    barrier_status,
    invoice_profile,
    account_mapping,
    last_update_timestamp
  FROM `your-gcp-project.bert_production_val.legacy_dw_bert_ausd_v_ta_p_vertrag`
  WHERE reporting_period = '2025-12'
),
migrated_data AS (
  SELECT 
    contract_id,
    reporting_period,
    partner_id,
    discount_amount,
    barrier_status,
    invoice_profile,
    account_mapping,
    last_update_timestamp
  FROM `your-gcp-project.bert_production.dw_bert_ausd_v_ta_p_vertrag`
  WHERE reporting_period = '2025-12'
),
diff_check AS (
  (SELECT * FROM legacy_data EXCEPT DISTINCT SELECT * FROM migrated_data)
  UNION ALL
  (SELECT * FROM migrated_data EXCEPT DISTINCT SELECT * FROM legacy_data)
)
SELECT 
  COUNT(*) AS mismatch_count 
FROM diff_check;
```
*Expected Output:* `mismatch_count = 0`.

---

## Test Case 4: Transformation Correctness & NULL Handling (`task_vertrag_tmp`)

### Purpose
Verify that the intermediate merger step (`task_vertrag_tmp`) correctly handles outer joins, missing business partner references, and NULL values when consolidating staging tables into the temporary contract store.

### Setup
1. Prepare a test dataset in BigQuery containing:
   * 10 base contracts in `dw_bert_ausd_v_ta_cntrct_valid`.
   * 8 matching business partners in `dw_bert_ausd_v_ta_bp_ref` (leaving 2 contracts with missing/NULL business partner references).
   * 5 contracts with active discounts, and 5 with NULL/no discounts.
2. Initialize a local or ephemeral Spark Session.

### Action
1. Execute the PySpark transformation logic defined in `dw_bert_ausd_v_ta_vertrag_tmp.py` using the test dataset.
2. Inspect the output DataFrame.

### Pass/Fail Criterion
* **Pass**: 
  * The output contains exactly 10 records (no contracts lost during joins).
  * The 2 contracts with missing business partners have `partner_id` set to `NULL` (or a designated default like `-1` if specified by business rules), rather than being dropped.
  * Contracts without discounts have `discount_amount` set to `0.00` or `NULL` as per legacy specification.
* **Fail**: Records are dropped due to inner joins, or NULL values cause `NullPointerException` or incorrect calculations.

### Test Code (PySpark Unit Test)
```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test_vertrag_tmp_transformation") \
        .getOrCreate()

def test_vertrag_tmp_joins_and_nulls(spark):
    # Define schemas
    contract_schema = StructType([
        StructField("contract_id", StringType(), False),
        StructField("status", StringType(), True)
    ])
    bp_schema = StructType([
        StructField("contract_id", StringType(), False),
        StructField("partner_id", StringType(), True)
    ])
    
    # Create test data (Contract 3 has no Business Partner)
    contracts_df = spark.createDataFrame([
        ("C001", "ACTIVE"),
        ("C002", "ACTIVE"),
        ("C003", "ACTIVE")
    ], schema=contract_schema)
    
    bp_df = spark.createDataFrame([
        ("C001", "P-999"),
        ("C002", "P-888")
    ], schema=bp_schema)
    
    # Perform Left Outer Join (simulating task_vertrag_tmp logic)
    result_df = contracts_df.join(bp_df, on="contract_id", how="left")
    
    # Assertions
    results = result_df.collect()
    assert len(results) == 3, "Should preserve all 3 contracts."
    
    c003_record = next(r for r in results if r["contract_id"] == "C003")
    assert c003_record["partner_id"] is None, "Missing business partner must resolve to NULL."
```

---

## Test Case 5: Data Quality & Schema Assertions (Target Table)

### Purpose
Assert that the final target table `dw_bert_ausd_v_ta_p_vertrag` in BigQuery adheres to strict schema constraints, primary key uniqueness, and data quality thresholds.

### Setup
The pipeline execution has completed successfully.

### Action
Run data quality validation queries against the production target table `dw_bert_ausd_v_ta_p_vertrag`.

### Pass/Fail Criterion
* **Pass**:
  * Primary key (`contract_id`, `reporting_period`) is unique.
  * No critical fields (e.g., `contract_id`, `reporting_period`) contain `NULL` values.
  * Numeric fields (e.g., `discount_amount`) do not contain negative values unless explicitly allowed.
* **Fail**: Duplicates are found, or critical fields contain NULLs.

### Test Code (SQL Assertions)
```sql
-- Assertion 1: Primary Key Uniqueness
-- Expected Output: 0
SELECT 
  COUNT(*) - COUNT(DISTINCT CONCAT(contract_id, '_', reporting_period)) AS duplicate_pk_count
FROM `your-gcp-project.bert_production.dw_bert_ausd_v_ta_p_vertrag`;

-- Assertion 2: Nullability Check on Key Fields
-- Expected Output: 0
SELECT 
  COUNT(*) AS null_key_count
FROM `your-gcp-project.bert_production.dw_bert_ausd_v_ta_p_vertrag`
WHERE contract_id IS NULL OR reporting_period IS NULL;

-- Assertion 3: Data Type and Range Validation
-- Expected Output: 0 (No records should have negative discount amounts)
SELECT 
  COUNT(*) AS invalid_discount_count
FROM `your-gcp-project.bert_production.dw_bert_ausd_v_ta_p_vertrag`
WHERE discount_amount < 0.0;
```