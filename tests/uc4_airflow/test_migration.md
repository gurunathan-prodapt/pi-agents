Here is a comprehensive suite of migration-validation tests for the `DW.DWH_ADM_JOB_MONITOR_START` utility. These tests are designed to prove behavioral equivalence between the legacy UC4 JOBI script and the migrated Apache Airflow Python implementation.

---

# Test Case 1: Monitored DAG Execution (Positive Test)

### Purpose
Verify that when a DAG is explicitly configured for monitoring in the metadata table with an active flag (`'J'`), the utility correctly identifies it, logs the exact legacy-compliant message, and registers the active run in the running jobs table.

### Setup
1. Mock the Airflow Variables:
   * `GCP_PROJECT` = `"test-gcp-project"`
   * `BQ_DATASET` = `"test_dataset"`
   * `BQ_CONNECTION_ID` = `"google_cloud_default"`
2. Mock the BigQuery Hook to return a record indicating the DAG is monitored:
   * Query: `SELECT dag_id, monitoring_enabled_flag FROM test-gcp-project.test_dataset.dwh_monitored_jps WHERE dag_id = 'my_monitored_dag' OR dag_id = 'ALL'`
   * Returned Rows: `[("my_monitored_dag", "J")]`
3. Prepare an Airflow context dictionary:
   * `dag_run`: Mock object with `dag_id="my_monitored_dag"` and `run_id="manual__2025-01-01T00:00:00"`
   * `ti`: Mock object with `task_id="my_task"`

### Action
Execute `execute_job_monitor_start(context)` and capture the logs and the SQL statements executed by the BigQuery Hook.

### Pass/Fail Criterion
* **Pass**: 
  * The log contains the exact literal: `"Added my_task with manual__2025-01-01T00:00:00"`.
  * An `INSERT` query is executed against `test-gcp-project.test_dataset.dwh_running_jobs` with values `'my_task'` and `'manual__2025-01-01T00:00:00'`.
* **Fail**: The insert query is skipped, the log message is missing or formatted incorrectly, or an unhandled exception is thrown.

### Test Code
```python
import pytest
import logging
from unittest.mock import MagicMock, patch
from uc4_airflow.dw_dwh_adm_job_monitor_start import execute_job_monitor_start

@patch("airflow.models.Variable.get")
@patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook")
def test_monitor_start_positive_monitored(mock_bq_hook, mock_var_get, caplog):
    # Setup
    mock_var_get.side_effect = lambda key, default_var=None: {
        "GCP_PROJECT": "test-gcp-project",
        "BQ_DATASET": "test_dataset",
        "BQ_CONNECTION_ID": "google_cloud_default"
    }.get(key, default_var)

    # Mock BigQuery Hook response for the check query
    mock_hook_instance = MagicMock()
    mock_hook_instance.get_records.return_value = [("my_monitored_dag", "J")]
    mock_bq_hook.return_value = mock_hook_instance

    # Mock Airflow Context
    mock_dag_run = MagicMock()
    mock_dag_run.dag_id = "my_monitored_dag"
    mock_dag_run.run_id = "manual__2025-01-01T00:00:00"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "my_task"

    context = {
        "dag_run": mock_dag_run,
        "ti": mock_ti
    }

    # Action
    with caplog.at_level(logging.INFO):
        run_id = execute_job_monitor_start(context=context)

    # Assertions
    assert run_id == "manual__2025-01-01T00:00:00"
    
    # 1. Output Parity: Verify exact legacy log statement
    assert "Added my_task with manual__2025-01-01T00:00:00" in caplog.text

    # 2. Transformation Correctness: Verify correct SQL check was run
    mock_hook_instance.get_records.assert_called_once()
    check_sql = mock_hook_instance.get_records.call_args[1]["sql"]
    assert "FROM `test-gcp-project.test_dataset.dwh_monitored_jps`" in check_sql
    assert "dag_id = 'my_monitored_dag'" in check_sql

    # 3. External System Replacement: Verify insert query execution
    mock_hook_instance.run_query.assert_called_once()
    insert_sql = mock_hook_instance.run_query.call_args[1]["sql"]
    assert "INSERT INTO `test-gcp-project.test_dataset.dwh_running_jobs`" in insert_sql
    assert "('my_task', 'manual__2025-01-01T00:00:00')" in insert_sql
```

---

# Test Case 2: Non-Monitored DAG Execution (Negative Test)

### Purpose
Verify that when a DAG is not configured for monitoring (or has its active flag set to something other than `'J'`), the utility skips registration and does not write to the running jobs table.

### Setup
1. Mock the Airflow Variables as in Test Case 1.
2. Mock the BigQuery Hook to return a record indicating monitoring is disabled:
   * Returned Rows: `[("my_unmonitored_dag", "N")]` or empty list `[]`.
3. Prepare an Airflow context dictionary:
   * `dag_run`: Mock object with `dag_id="my_unmonitored_dag"` and `run_id="manual__2025-01-01T00:00:00"`
   * `ti`: Mock object with `task_id="my_task"`

### Action
Execute `execute_job_monitor_start(context)` and capture the logs and the SQL statements executed by the BigQuery Hook.

### Pass/Fail Criterion
* **Pass**: 
  * The log contains: `"DAG my_unmonitored_dag is not configured for monitoring or check returned False."`
  * No `INSERT` query is executed against `dwh_running_jobs`.
* **Fail**: An insert query is executed, or the function crashes.

### Test Code
```python
import pytest
import logging
from unittest.mock import MagicMock, patch
from uc4_airflow.dw_dwh_adm_job_monitor_start import execute_job_monitor_start

@patch("airflow.models.Variable.get")
@patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook")
def test_monitor_start_negative_unmonitored(mock_bq_hook, mock_var_get, caplog):
    # Setup
    mock_var_get.side_effect = lambda key, default_var=None: {
        "GCP_PROJECT": "test-gcp-project",
        "BQ_DATASET": "test_dataset",
        "BQ_CONNECTION_ID": "google_cloud_default"
    }.get(key, default_var)

    # Mock BigQuery Hook to return disabled flag
    mock_hook_instance = MagicMock()
    mock_hook_instance.get_records.return_value = [("my_unmonitored_dag", "N")]
    mock_bq_hook.return_value = mock_hook_instance

    # Mock Airflow Context
    mock_dag_run = MagicMock()
    mock_dag_run.dag_id = "my_unmonitored_dag"
    mock_dag_run.run_id = "manual__2025-01-01T00:00:00"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "my_task"

    context = {
        "dag_run": mock_dag_run,
        "ti": mock_ti
    }

    # Action
    with caplog.at_level(logging.INFO):
        execute_job_monitor_start(context=context)

    # Assertions
    # 1. Verify no insert query was run
    mock_hook_instance.run_query.assert_not_called()
    
    # 2. Verify skip log message
    assert "DAG my_unmonitored_dag is not configured for monitoring" in caplog.text
```

---

# Test Case 3: Wildcard 'ALL' Monitoring Configuration

### Purpose
Verify that if the metadata table contains a wildcard entry (`dag_id = 'ALL'` with flag `'J'`), any executing DAG is automatically registered for monitoring, matching the legacy UC4 behavior (`IF &ADMGB = &ADMJP OR "ALL"`).

### Setup
1. Mock the Airflow Variables as in Test Case 1.
2. Mock the BigQuery Hook to return a wildcard record:
   * Returned Rows: `[("ALL", "J")]`
3. Prepare an Airflow context dictionary:
   * `dag_run`: Mock object with `dag_id="any_random_dag"` and `run_id="scheduled__2025-01-01T12:00:00"`
   * `ti`: Mock object with `task_id="any_task"`

### Action
Execute `execute_job_monitor_start(context)` and capture the logs and the SQL statements executed by the BigQuery Hook.

### Pass/Fail Criterion
* **Pass**: 
  * The utility identifies the wildcard match.
  * The log contains: `"Added any_task with scheduled__2025-01-01T12:00:00"`.
  * An `INSERT` query is executed against `test-gcp-project.test_dataset.dwh_running_jobs`.
* **Fail**: The wildcard is ignored, and the run is not registered.

### Test Code
```python
import pytest
import logging
from unittest.mock import MagicMock, patch
from uc4_airflow.dw_dwh_adm_job_monitor_start import execute_job_monitor_start

@patch("airflow.models.Variable.get")
@patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook")
def test_monitor_start_wildcard_all(mock_bq_hook, mock_var_get, caplog):
    # Setup
    mock_var_get.side_effect = lambda key, default_var=None: {
        "GCP_PROJECT": "test-gcp-project",
        "BQ_DATASET": "test_dataset",
        "BQ_CONNECTION_ID": "google_cloud_default"
    }.get(key, default_var)

    # Mock BigQuery Hook to return wildcard row
    mock_hook_instance = MagicMock()
    mock_hook_instance.get_records.return_value = [("ALL", "J")]
    mock_bq_hook.return_value = mock_hook_instance

    # Mock Airflow Context
    mock_dag_run = MagicMock()
    mock_dag_run.dag_id = "any_random_dag"
    mock_dag_run.run_id = "scheduled__2025-01-01T12:00:00"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "any_task"

    context = {
        "dag_run": mock_dag_run,
        "ti": mock_ti
    }

    # Action
    with caplog.at_level(logging.INFO):
        execute_job_monitor_start(context=context)

    # Assertions
    # Verify that wildcard triggered the insert
    mock_hook_instance.run_query.assert_called_once()
    insert_sql = mock_hook_instance.run_query.call_args[1]["sql"]
    assert "INSERT INTO `test-gcp-project.test_dataset.dwh_running_jobs`" in insert_sql
    assert "('any_task', 'scheduled__2025-01-01T12:00:00')" in insert_sql
    assert "Added any_task with scheduled__2025-01-01T12:00:00" in caplog.text
```

---

# Test Case 4: Graceful Error Handling (Missing BigQuery Table / Connection Failure)

### Purpose
Verify that if the BigQuery metadata table is missing, or if there is a network/permission failure when querying BigQuery, the utility handles the exception gracefully, logs a warning, and allows the parent DAG run to continue without failing.

### Setup
1. Mock the Airflow Variables as in Test Case 1.
2. Mock the BigQuery Hook to raise an exception (e.g., `GoogleCloudError` or generic `Exception`) when `get_records` is called.
3. Prepare an Airflow context dictionary.

### Action
Execute `execute_job_monitor_start(context)` and capture the logs.

### Pass/Fail Criterion
* **Pass**:
  * The function does not raise an exception to the caller.
  * A warning log is emitted containing: `"Could not query test-gcp-project.test_dataset.dwh_monitored_jps"`.
  * The function returns the `run_id` successfully.
* **Fail**: The exception propagates upward, causing the task/DAG to fail.

### Test Code
```python
import pytest
import logging
from unittest.mock import MagicMock, patch
from uc4_airflow.dw_dwh_adm_job_monitor_start import execute_job_monitor_start

@patch("airflow.models.Variable.get")
@patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook")
def test_monitor_start_graceful_failure(mock_bq_hook, mock_var_get, caplog):
    # Setup
    mock_var_get.side_effect = lambda key, default_var=None: {
        "GCP_PROJECT": "test-gcp-project",
        "BQ_DATASET": "test_dataset",
        "BQ_CONNECTION_ID": "google_cloud_default"
    }.get(key, default_var)

    # Mock BigQuery Hook to raise an exception
    mock_hook_instance = MagicMock()
    mock_hook_instance.get_records.side_effect = Exception("Table not found or Access Denied")
    mock_bq_hook.return_value = mock_hook_instance

    # Mock Airflow Context
    mock_dag_run = MagicMock()
    mock_dag_run.dag_id = "my_dag"
    mock_dag_run.run_id = "manual__2025-01-01T00:00:00"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "my_task"

    context = {
        "dag_run": mock_dag_run,
        "ti": mock_ti
    }

    # Action & Assertion (Should NOT raise an exception)
    try:
        with caplog.at_level(logging.WARNING):
            run_id = execute_job_monitor_start(context=context)
    except Exception as e:
        pytest.fail(f"execute_job_monitor_start raised an exception unexpectedly: {e}")

    # Assertions
    assert run_id == "manual__2025-01-01T00:00:00"
    assert "Could not query test-gcp-project.test_dataset.dwh_monitored_jps" in caplog.text
    assert "Defaulting to non-monitored" in caplog.text
    mock_hook_instance.run_query.assert_not_called()
```

---

# Test Case 5: Schema and Data Quality Assertions (BigQuery DDL & DML Validation)

### Purpose
Ensure that the target BigQuery tables (`dwh_monitored_jps` and `dwh_running_jobs`) are structured correctly to support the queries and inserts executed by the utility, and verify that the append-only pattern works without constraint violations.

### Setup
Execute these assertions directly against the target BigQuery environment (or a local emulator/test dataset) using standard SQL.

### Action
Run the following DDL and DML validation scripts.

### Pass/Fail Criterion
* **Pass**: All SQL assertions return `TRUE` or execute without error.
* **Fail**: Table schemas do not match, or DML inserts fail due to type mismatches.

### SQL Assertions
```sql
-- 1. Verify Schema of dwh_monitored_jps
SELECT 
  column_name, 
  data_type 
FROM 
  `your_project.monitoring_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = 'dwh_monitored_jps'
  AND (
    (column_name = 'dag_id' AND data_type = 'STRING') OR
    (column_name = 'monitoring_enabled_flag' AND data_type = 'STRING')
  );
-- Expectation: 2 rows returned matching the types.

-- 2. Verify Schema of dwh_running_jobs
SELECT 
  column_name, 
  data_type 
FROM 
  `your_project.monitoring_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = 'dwh_running_jobs'
  AND (
    (column_name = 'job_name' AND data_type = 'STRING') OR
    (column_name = 'run_id' AND data_type = 'STRING')
  );
-- Expectation: 2 rows returned matching the types.

-- 3. Verify Append-Only Insert Compatibility (No Primary Key Violations)
-- BigQuery does not enforce primary keys, but we must ensure we can insert duplicate job names with different run IDs.
INSERT INTO `your_project.monitoring_dataset.dwh_running_jobs` (job_name, run_id)
VALUES 
  ('test_job_concurrency', 'run_1'),
  ('test_job_concurrency', 'run_2');

-- Clean up test data
DELETE FROM `your_project.monitoring_dataset.dwh_running_jobs` 
WHERE job_name = 'test_job_concurrency';
```