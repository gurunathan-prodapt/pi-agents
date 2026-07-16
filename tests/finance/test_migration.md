# Migration Validation Test Suite: `finance_daily_workflow`

This document provides a comprehensive suite of migration-validation tests to prove that the migrated Airflow DAG and PySpark scripts are behaviorally equivalent to the legacy UC4 workflow.

---

## Section 1: Orchestration & DAG Structure Validation

### Test Case 1.1: DAG Structure, Schedule, and Parameter Parity
#### Purpose
To verify that the migrated Airflow DAG matches the legacy UC4 `JOBP` properties, including scheduling, timezone, catchup, concurrency limits, and default parameters.

#### Setup
*   Access to the Airflow environment (or a local unit-testing environment with `pytest` and `apache-airflow` installed).
*   The migrated DAG file `dags/finance_daily_workflow.py` placed in the Airflow DAGs folder.

#### Action
Run a programmatic unit test using `pytest` to inspect the DAG structure and properties.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag

def test_dag_properties():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="finance_daily_workflow")
    
    assert dag is not None, "DAG finance_daily_workflow failed to load."
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"
    
    # Schedule & Timezone Parity
    assert dag.schedule_interval == "0 1 * * 1-5", "Schedule interval must be Monday-Friday at 01:00"
    assert dag.timezone.name == "Europe/London", "Timezone must be Europe/London"
    
    # Concurrency & Catchup Parity
    assert dag.catchup is False, "Catchup must be disabled (False)"
    assert dag.max_active_runs == 1, "max_active_runs must be strictly 1 to prevent race conditions"
    
    # Default Arguments Parity
    default_args = dag.default_args
    assert default_args.get("owner") == "finance_etl", "Owner must be 'finance_etl'"
    assert default_args.get("retries") == 0, "Default retries must be 0 (overridden per task)"
    assert "finance-etl@company.com" in default_args.get("email", []), "Primary notification email missing"
```

#### Pass/Fail Criterion
*   **Pass:** The test runs successfully with no assertion errors, proving that the scheduling, concurrency, and ownership properties match the legacy specification.
*   **Fail:** Any assertion fails, or the DAG fails to load due to syntax/import errors.

---

### Test Case 1.2: Task Dependency and Trigger Rule Parity
#### Purpose
To verify that the task execution sequence and dependency rules match the legacy UC4 workflow. Specifically, `gl_extract` must run only after both `acct_load` and `rate_extract` complete, and downstream triggers must execute in a fire-and-forget manner.

#### Setup
*   The same Airflow environment as Test Case 1.1.

#### Action
Run a programmatic unit test to verify task dependencies and downstream trigger configurations.

```python
# test_dag_dependencies.py
from airflow.models import DagBag

def test_task_dependencies():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="finance_daily_workflow")
    
    # Verify Task Existence
    tasks = {task.task_id: task for task in dag.tasks}
    expected_tasks = [
        "concurrency_guard",
        "finance_daily_pre_check",
        "finance_daily_acct_load",
        "finance_daily_rate_extract",
        "finance_daily_gl_extract",
        "finance_daily_gl_close",
        "trigger_retail_daily_workflow",
        "trigger_crm_weekly_workflow"
    ]
    for task_id in expected_tasks:
        assert task_id in tasks, f"Task {task_id} is missing from the DAG"
        
    # Verify Downstream/Upstream Relationships
    assert "finance_daily_pre_check" in tasks["concurrency_guard"].downstream_task_ids
    assert "finance_daily_acct_load" in tasks["finance_daily_pre_check"].downstream_task_ids
    assert "finance_daily_rate_extract" in tasks["finance_daily_pre_check"].downstream_task_ids
    
    assert "finance_daily_gl_extract" in tasks["finance_daily_acct_load"].downstream_task_ids
    assert "finance_daily_gl_extract" in tasks["finance_daily_rate_extract"].downstream_task_ids
    
    assert "finance_daily_gl_close" in tasks["finance_daily_gl_extract"].downstream_task_ids
    assert "trigger_retail_daily_workflow" in tasks["finance_daily_gl_close"].downstream_task_ids
    assert "trigger_crm_weekly_workflow" in tasks["finance_daily_gl_close"].downstream_task_ids

    # Verify Fire-and-Forget behavior (wait_for_completion=False)
    assert tasks["trigger_retail_daily_workflow"].wait_for_completion is False
    assert tasks["trigger_crm_weekly_workflow"].wait_for_completion is False
```

#### Pass/Fail Criterion
*   **Pass:** All task dependency and operator configuration assertions pass.
*   **Fail:** Any dependency is misaligned, or a trigger operator is configured to wait for downstream completion.

---

### Test Case 1.3: Concurrency Guard Validation
#### Purpose
To verify that the `concurrency_guard` task successfully skips execution if another instance of the DAG is already running, matching the legacy UC4 "Sync Wait" behavior.

#### Setup
*   An active Airflow database (or mocked `DagRun` state).
*   A test runner that executes the `run_guard_logic` Python callable.

#### Action
Execute the guard logic under two scenarios:
1.  **Scenario A:** No other active runs exist.
2.  **Scenario B:** Another active run exists.

```python
# test_concurrency_guard.py
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowSkipException
from dags.finance_daily_workflow import run_guard_logic

@patch('airflow.models.DagRun.find')
def test_concurrency_guard_no_collision(mock_find):
    # Mock current run and no other active runs
    context = {'run_id': 'manual__2024-01-01T01:00:00+00:00'}
    mock_run_self = MagicMock()
    mock_run_self.run_id = 'manual__2024-01-01T01:00:00+00:00'
    mock_find.return_return_value = [mock_run_self]
    
    # Should run without raising an exception
    run_guard_logic(**context)

@patch('airflow.models.DagRun.find')
def test_concurrency_guard_collision(mock_find):
    # Mock current run and an existing running instance
    context = {'run_id': 'manual__2024-01-01T01:00:00+00:00'}
    mock_run_self = MagicMock()
    mock_run_self.run_id = 'manual__2024-01-01T01:00:00+00:00'
    
    mock_run_other = MagicMock()
    mock_run_other.run_id = 'scheduled__2024-01-01T01:00:00+00:00'
    
    mock_find.return_value = [mock_run_self, mock_run_other]
    
    # Should raise AirflowSkipException
    with pytest.raises(AirflowSkipException) as exc_info:
        run_guard_logic(**context)
    assert "Another active instance is running" in str(exc_info.value)
```

#### Pass/Fail Criterion
*   **Pass:** Scenario A completes silently; Scenario B raises `AirflowSkipException`.
*   **Fail:** Scenario B allows execution to proceed, or Scenario A raises an unexpected exception.

---

## Section 2: PySpark Task & Data Transformation Validation

### Test Case 2.1: Pre-Check Connectivity Validation
#### Purpose
To verify that `finance_daily_pre_check.py` correctly validates connectivity to the Oracle database and handles connection failures gracefully.

#### Setup
*   A local Spark session.
*   A mock JDBC server or a mocked Spark `DataFrameReader.load` method.

#### Action
Execute the pre-check script under two scenarios:
1.  **Scenario A:** Database is online (returns `DB_OK`).
2.  **Scenario B:** Database is offline (throws an exception).

```python
# test_pre_check.py
import pytest
from unittest.mock import patch, MagicMock
import sys

# Import the main function from the script
from pyspark_scripts.finance_daily_pre_check import main as pre_check_main

@patch('pyspark_scripts.finance_daily_pre_check.SparkSession')
@patch('pyspark_scripts.finance_daily_pre_check.sys.exit')
def test_pre_check_success(mock_sys_exit, mock_spark_session):
    # Mock Spark Session and JDBC read returning 1 row
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 1
    mock_spark.read.format().option().option().option().option().option().load.return_value = mock_df
    mock_spark_session.builder.appName().getOrCreate.return_value = mock_spark
    
    pre_check_main()
    mock_sys_exit.assert_called_once_with(0)

@patch('pyspark_scripts.finance_daily_pre_check.SparkSession')
@patch('pyspark_scripts.finance_daily_pre_check.sys.exit')
def test_pre_check_failure(mock_sys_exit, mock_spark_session):
    # Mock Spark Session throwing an exception during load
    mock_spark = MagicMock()
    mock_spark.read.format().option().option().option().option().option().load.side_effect = Exception("Connection Timeout")
    mock_spark_session.builder.appName().getOrCreate.return_value = mock_spark
    
    pre_check_main()
    mock_sys_exit.assert_called_once_with(1)
```

#### Pass/Fail Criterion
*   **Pass:** The script exits with status `0` when the database is online, and status `1` when the database is offline.
*   **Fail:** The script exits with `0` on database failure, or fails to raise/log the connection error.

---

### Test Case 2.2: Account Master Dimension Refresh (Stub Validation)
#### Purpose
To verify that the missing legacy component `run_account_load.ksh` is safely guarded with a `NotImplementedError` in the migrated PySpark stub, preventing silent failures of unmigrated code.

#### Setup
*   The migrated script `pyspark_scripts/finance_daily_acct_load.py`.

#### Action
Execute the script directly and assert that it raises `NotImplementedError`.

```python
# test_acct_load_stub.py
import pytest
from pyspark_scripts.finance_daily_acct_load import main as acct_load_main

def test_acct_load_raises_not_implemented():
    with pytest.raises(NotImplementedError) as exc_info:
        acct_load_main()
    assert "Missing legacy component run_account_load.ksh" in str(exc_info.value)
```

#### Pass/Fail Criterion
*   **Pass:** The script raises `NotImplementedError` with a clear message pointing to the missing legacy component.
*   **Fail:** The script exits with status `0` or fails with an unrelated error.

---

### Test Case 2.3: Daily Exchange Rate Extraction (Stub Validation)
#### Purpose
To verify that the missing legacy component `rate_extract.sql` is safely guarded with a `NotImplementedError` in the migrated PySpark stub.

#### Setup
*   The migrated script `pyspark_scripts/finance_daily_rate_extract.py`.

#### Action
Execute the script directly and assert that it raises `NotImplementedError`.

```python
# test_rate_extract_stub.py
import pytest
from pyspark_scripts.finance_daily_rate_extract import main as rate_extract_main

def test_rate_extract_raises_not_implemented():
    with pytest.raises(NotImplementedError) as exc_info:
        rate_extract_main()
    assert "Missing legacy component rate_extract.sql" in str(exc_info.value)
```

#### Pass/Fail Criterion
*   **Pass:** The script raises `NotImplementedError` with a clear message pointing to the missing legacy component.
*   **Fail:** The script exits with status `0` or fails with an unrelated error.

---

### Test Case 2.4: GL Extract Multi-Entity Processing (Stub Validation)
#### Purpose
To verify that the missing legacy component `run_gl_close.ksh` (which processes `UK_ENTITY`, `DE_ENTITY`, and `FR_ENTITY`) is safely guarded with a `NotImplementedError` in the migrated PySpark stub.

#### Setup
*   The migrated script `pyspark_scripts/finance_daily_gl_extract.py`.

#### Action
Execute the script directly and assert that it raises `NotImplementedError`.

```python
# test_gl_extract_stub.py
import pytest
from pyspark_scripts.finance_daily_gl_extract import main as gl_extract_main

def test_gl_extract_raises_not_implemented():
    with pytest.raises(NotImplementedError) as exc_info:
        gl_extract_main()
    assert "Missing legacy component run_gl_close.ksh" in str(exc_info.value)
```

#### Pass/Fail Criterion
*   **Pass:** The script raises `NotImplementedError` with a clear message pointing to the missing legacy component.
*   **Fail:** The script exits with status `0` or fails with an unrelated error.

---

### Test Case 2.5: GL Close Audit Logging and Event Publication
#### Purpose
To verify that `finance_daily_gl_close.py` correctly writes audit records to GCS in Parquet format, matches the legacy log output, and handles input arguments correctly.

#### Setup
*   A local Spark session.
*   A temporary directory to act as the mock GCS bucket.

#### Action
Run the `finance_daily_gl_close.py` script with mock arguments and verify the output Parquet schema and data.

```python
# test_gl_close.py
import os
import pytest
import sys
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark_scripts.finance_daily_gl_close import main as gl_close_main

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test-gl-close") \
        .getOrCreate()

def test_gl_close_execution(spark, tmp_path):
    test_date = "2024-01-01"
    allow_empty = "N"
    mock_bucket = str(tmp_path)
    
    # Set environment variable and mock sys.argv
    with patch.dict(os.environ, {"GCS_BUCKET": mock_bucket}), \
         patch.object(sys, 'argv', ["script_name", test_date, allow_empty]):
        
        gl_close_main()
        
        # Verify Parquet Audit Log Output
        expected_path = f"{mock_bucket}/audit/daily_audit_log/date={test_date}"
        assert os.path.exists(expected_path), "Audit log directory was not created"
        
        # Read written Parquet data
        df = spark.read.parquet(expected_path)
        
        # Schema Assertions
        expected_schema = ["period_date", "status", "event_published"]
        assert all(col in df.columns for col in expected_schema), "Schema mismatch in audit log"
        
        # Data Parity Assertions
        row = df.collect()[0]
        assert row["period_date"] == test_date
        assert row["status"] == "SUCCESS"
        assert row["event_published"] == "FINANCE_GL_CLOSE_COMPLETE"
```

#### Pass/Fail Criterion
*   **Pass:** The script writes a Parquet file to the designated path with the correct schema, partition structure, and row values.
*   **Fail:** The script fails to execute, writes incorrect data, or fails to output the audit log.

---

## Section 3: End-to-End Integration & Alerting Validation

### Test Case 3.1: Failure Callback and Alerting Parity
#### Purpose
To verify that the `on_failure_alarm` callback is triggered upon task failure and correctly formats the alert message matching the legacy notification requirements.

#### Setup
*   A mocked Airflow context dictionary containing task instance, DAG, and execution date details.

#### Action
Call `on_failure_alarm` with the mocked context and capture stdout to verify the alert message.

```python
# test_alerting.py
from unittest.mock import MagicMock
import io
import sys
from dags.finance_daily_workflow import on_failure_alarm

def test_on_failure_alarm_output():
    # Mock Airflow Context
    mock_task_instance = MagicMock()
    mock_task_instance.task_id = "finance_daily_gl_extract"
    
    mock_dag = MagicMock()
    mock_dag.dag_id = "finance_daily_workflow"
    
    context = {
        "task_instance": mock_task_instance,
        "dag": mock_dag,
        "ds": "2024-01-01"
    }
    
    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    on_failure_alarm(context)
    
    # Reset redirect
    sys.stdout = sys.__stdout__
    
    expected_message = "ALERT: Task finance_daily_gl_extract inside finance_daily_workflow failed on 2024-01-01. Notification dispatched."
    assert expected_message in captured_output.getvalue().strip()
```

#### Pass/Fail Criterion
*   **Pass:** The callback prints the exact formatted alert string to stdout (which maps to the system log/alerting mechanism).
*   **Fail:** The output message is malformed, missing key metadata (task_id, dag_id, or execution date), or raises an exception.