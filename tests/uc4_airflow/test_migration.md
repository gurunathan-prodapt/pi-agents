Here is a comprehensive suite of migration-validation tests designed to verify that the migrated Airflow Python implementation of `DW.DWH_ADM_JOB_MONITOR_START` behaves identically to the legacy UC4 JOBI script.

---

# Migration Validation Test Suite: DW.DWH_ADM_JOB_MONITOR_START

## Test Architecture Overview
Since the legacy object is a `JOBI` (reusable include script) and the target is a Python helper function executed within an Airflow task context, these tests use `pytest` along with standard mocking libraries (`unittest.mock`) to simulate the Airflow context (`dag_id`, `task_id`, `run_id`) and Airflow Variables (`Variable.get`/`Variable.set`).

### Shared Test Setup / Fixtures
The following Python code defines the test harness used across all test cases. Save this as `test_dw_dwh_adm_job_monitor_start.py`.

```python
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

# Import the function under test
# Assuming the code is in a file named `dw_dwh_adm_job_monitor_start.py`
from dw_dwh_adm_job_monitor_start import execute_job_monitor_start, DW_DWH_MONITORED_JPS_VAR, DW_DWH_RUNNING_JOBS_VAR

@pytest.fixture
def base_context():
    """Generates a mock Airflow task execution context."""
    mock_dag = MagicMock()
    mock_dag.dag_id = "MY_TEST_DAG"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "MY_TEST_TASK"
    
    return {
        'dag': mock_dag,
        'task_instance': mock_ti,
        'run_id': 'manual__2023-10-27T12:00:00+00:00'
    }
```

---

## Test Case 1: Direct Match Monitoring (Output Parity & State Update)

### Purpose
Verify that when a DAG ID matches an active entry in the monitored JobPlans configuration (`DW_DWH_MONITORED_JPS`), the job is successfully registered in the running jobs registry (`DW_DWH_RUNNING_JOBS`) with its run ID, and the correct log output is generated.

### Setup
* **Context**: 
  * `dag_id` = `"MY_MONITORED_DAG"`
  * `task_id` = `"EXTRACT_CUSTOMER_TASK"`
  * `run_id` = `"scheduled__2023-10-27T00:00:00+00:00"`
* **Airflow Variables**:
  * `DW_DWH_MONITORED_JPS` = `{"MY_MONITORED_DAG": "J", "OTHER_DAG": "N"}`
  * `DW_DWH_RUNNING_JOBS` = `{"PRE_EXISTING_JOB": "run_999"}`

### Action
Execute `execute_job_monitor_start` with the configured context and capture standard output.

### Pass/Fail Criterion
* **Pass**: 
  * The function prints exactly: `Added EXTRACT_CUSTOMER_TASK with scheduled__2023-10-27T00:00:00+00:00`.
  * `Variable.set` is called to update `DW_DWH_RUNNING_JOBS` with the new state: `{"PRE_EXISTING_JOB": "run_999", "EXTRACT_CUSTOMER_TASK": "scheduled__2023-10-27T00:00:00+00:00"}`.
* **Fail**: Any mismatch in the printed log, failure to update the variable, or overwriting unrelated keys in the running jobs registry.

### Test Code
```python
@patch('dw_dwh_adm_job_monitor_start.Variable')
def test_direct_match_monitoring(mock_variable, base_context, capsys):
    # Arrange
    base_context['dag'].dag_id = "MY_MONITORED_DAG"
    base_context['task_instance'].task_id = "EXTRACT_CUSTOMER_TASK"
    base_context['run_id'] = "scheduled__2023-10-27T00:00:00+00:00"
    
    # Mock Variable.get calls
    def mock_get(key, deserialize_json=True, default_var=None):
        if key == DW_DWH_MONITORED_JPS_VAR:
            return {"MY_MONITORED_DAG": "J", "OTHER_DAG": "N"}
        if key == DW_DWH_RUNNING_JOBS_VAR:
            return {"PRE_EXISTING_JOB": "run_999"}
        return default_var
        
    mock_variable.get.side_effect = mock_get

    # Act
    execute_job_monitor_start(**base_context)

    # Assert
    # 1. Verify stdout output matches legacy print statement
    captured = capsys.readouterr()
    assert "Added EXTRACT_CUSTOMER_TASK with scheduled__2023-10-27T00:00:00+00:00" in captured.out

    # 2. Verify state update in DW_DWH_RUNNING_JOBS
    expected_updated_jobs = {
        "PRE_EXISTING_JOB": "run_999",
        "EXTRACT_CUSTOMER_TASK": "scheduled__2023-10-27T00:00:00+00:00"
    }
    mock_variable.set.assert_called_once_with(
        DW_DWH_RUNNING_JOBS_VAR, 
        expected_updated_jobs, 
        serialize_json=True
    )
```

---

## Test Case 2: Wildcard "ALL" Monitoring

### Purpose
Verify that if the wildcard `"ALL"` is configured with active status `"J"` in `DW_DWH_MONITORED_JPS`, any executing DAG is registered, regardless of whether its specific DAG ID is explicitly listed.

### Setup
* **Context**: 
  * `dag_id` = `"ANY_RANDOM_DAG"`
  * `task_id` = `"RANDOM_TASK"`
  * `run_id` = `"manual__2023-10-27T15:30:00"`
* **Airflow Variables**:
  * `DW_DWH_MONITORED_JPS` = `{"ALL": "J"}`
  * `DW_DWH_RUNNING_JOBS` = `{}`

### Action
Execute `execute_job_monitor_start` with the configured context.

### Pass/Fail Criterion
* **Pass**: 
  * The function prints: `Added RANDOM_TASK with manual__2023-10-27T15:30:00`.
  * `Variable.set` is called to update `DW_DWH_RUNNING_JOBS` to `{"RANDOM_TASK": "manual__2023-10-27T15:30:00"}`.
* **Fail**: The task is skipped or fails to register under the `"ALL"` wildcard rule.

### Test Code
```python
@patch('dw_dwh_adm_job_monitor_start.Variable')
def test_wildcard_all_monitoring(mock_variable, base_context, capsys):
    # Arrange
    base_context['dag'].dag_id = "ANY_RANDOM_DAG"
    base_context['task_instance'].task_id = "RANDOM_TASK"
    base_context['run_id'] = "manual__2023-10-27T15:30:00"
    
    def mock_get(key, deserialize_json=True, default_var=None):
        if key == DW_DWH_MONITORED_JPS_VAR:
            return {"ALL": "J"}
        if key == DW_DWH_RUNNING_JOBS_VAR:
            return {}
        return default_var
        
    mock_variable.get.side_effect = mock_get

    # Act
    execute_job_monitor_start(**base_context)

    # Assert
    captured = capsys.readouterr()
    assert "Added RANDOM_TASK with manual__2023-10-27T15:30:00" in captured.out
    
    mock_variable.set.assert_called_once_with(
        DW_DWH_RUNNING_JOBS_VAR, 
        {"RANDOM_TASK": "manual__2023-10-27T15:30:00"}, 
        serialize_json=True
    )
```

---

## Test Case 3: Unmonitored DAG (Negative Match)

### Purpose
Verify that if a DAG ID is not listed in `DW_DWH_MONITORED_JPS` (and `"ALL"` is not active), the job is ignored, no logs are printed, and the running jobs registry is not modified.

### Setup
* **Context**: 
  * `dag_id` = `"UNMONITORED_DAG"`
  * `task_id` = `"SOME_TASK"`
  * `run_id` = `"scheduled__2023-10-27T00:00:00"`
* **Airflow Variables**:
  * `DW_DWH_MONITORED_JPS` = `{"MONITORED_DAG_A": "J", "MONITORED_DAG_B": "J"}`
  * `DW_DWH_RUNNING_JOBS` = `{"EXISTING_JOB": "run_111"}`

### Action
Execute `execute_job_monitor_start` with the configured context.

### Pass/Fail Criterion
* **Pass**: 
  * No "Added..." text is printed to standard output.
  * `Variable.set` is **never** called (no state changes).
* **Fail**: The job is incorrectly registered, or the variable is updated/overwritten.

### Test Code
```python
@patch('dw_dwh_adm_job_monitor_start.Variable')
def test_unmonitored_dag_no_action(mock_variable, base_context, capsys):
    # Arrange
    base_context['dag'].dag_id = "UNMONITORED_DAG"
    base_context['task_instance'].task_id = "SOME_TASK"
    
    def mock_get(key, deserialize_json=True, default_var=None):
        if key == DW_DWH_MONITORED_JPS_VAR:
            return {"MONITORED_DAG_A": "J", "MONITORED_DAG_B": "J"}
        if key == DW_DWH_RUNNING_JOBS_VAR:
            return {"EXISTING_JOB": "run_111"}
        return default_var
        
    mock_variable.get.side_effect = mock_get

    # Act
    execute_job_monitor_start(**base_context)

    # Assert
    captured = capsys.readouterr()
    assert "Added" not in captured.out
    mock_variable.set.assert_not_called()
```

---

## Test Case 4: Inactive Monitoring Flag (Status "N")

### Purpose
Verify that if a DAG ID is listed in `DW_DWH_MONITORED_JPS` but its active flag is set to something other than `"J"` (e.g., `"N"`), the job is ignored and not registered.

### Setup
* **Context**: 
  * `dag_id` = `"DISABLED_DAG"`
  * `task_id` = `"DISABLED_TASK"`
  * `run_id` = `"scheduled__2023-10-27T00:00:00"`
* **Airflow Variables**:
  * `DW_DWH_MONITORED_JPS` = `{"DISABLED_DAG": "N"}`
  * `DW_DWH_RUNNING_JOBS` = `{}`

### Action
Execute `execute_job_monitor_start` with the configured context.

### Pass/Fail Criterion
* **Pass**: 
  * No "Added..." text is printed.
  * `Variable.set` is **never** called.
* **Fail**: The job is registered despite having an inactive status flag.

### Test Code
```python
@patch('dw_dwh_adm_job_monitor_start.Variable')
def test_inactive_monitoring_flag(mock_variable, base_context, capsys):
    # Arrange
    base_context['dag'].dag_id = "DISABLED_DAG"
    base_context['task_instance'].task_id = "DISABLED_TASK"
    
    def mock_get(key, deserialize_json=True, default_var=None):
        if key == DW_DWH_MONITORED_JPS_VAR:
            return {"DISABLED_DAG": "N"}
        if key == DW_DWH_RUNNING_JOBS_VAR:
            return {}
        return default_var
        
    mock_variable.get.side_effect = mock_get

    # Act
    execute_job_monitor_start(**base_context)

    # Assert
    captured = capsys.readouterr()
    assert "Added" not in captured.out
    mock_variable.set.assert_not_called()
```

---

## Test Case 5: Empty/Null Parent JobPlan Handling

### Purpose
Verify that if the parent JobPlan name (`adm_jp`) is empty, whitespace-only, or `None` (mimicking the legacy UC4 check `:IF &ADMJP NE " "`), the script exits gracefully without performing any lookups or updates.

### Setup
* **Context**: 
  * `dag_id` = `""` (or `"   "`, or `None`)
  * `task_id` = `"ORPHAN_TASK"`
* **Airflow Variables**:
  * `DW_DWH_MONITORED_JPS` = `{"ALL": "J"}`

### Action
Execute `execute_job_monitor_start` with the empty `dag_id` context.

### Pass/Fail Criterion
* **Pass**: 
  * The function exits gracefully.
  * `Variable.get` is **never** called (proving the execution short-circuited before querying variables).
  * `Variable.set` is **never** called.
* **Fail**: The function attempts to query variables or throws an exception (e.g., `AttributeError` on `.strip()`).

### Test Code
```python
@pytest.mark.parametrize("empty_dag_id", ["", "   ", None])
@patch('dw_dwh_adm_job_monitor_start.Variable')
def test_empty_parent_jobplan_short_circuit(mock_variable, empty_dag_id, base_context):
    # Arrange
    base_context['dag'].dag_id = empty_dag_id
    base_context['task_instance'].task_id = "ORPHAN_TASK"

    # Act
    execute_job_monitor_start(**base_context)

    # Assert
    mock_variable.get.assert_not_called()
    mock_variable.set.assert_not_called()
```

---

## Test Case 6: Error Handling and Exception Propagation

### Purpose
Verify that if an unexpected error occurs (e.g., the Airflow Metadata Database is unreachable, causing `Variable.get` to raise an exception), the error is caught, logged, and then re-raised to ensure the calling task fails.

### Setup
* **Context**: Standard valid context.
* **Airflow Variables**: Mock `Variable.get` to raise an operational database exception.

### Action
Execute `execute_job_monitor_start` and catch the raised exception.

### Pass/Fail Criterion
* **Pass**: 
  * The function prints an error message containing `"Error updating job monitoring status:"`.
  * The original exception (or a wrapped exception) is re-raised, ensuring the Airflow task fails.
* **Fail**: The exception is silently swallowed, which would cause a false-positive success in the Airflow task run.

### Test Code
```python
@patch('dw_dwh_adm_job_monitor_start.Variable')
def test_error_handling_and_propagation(mock_variable, base_context, capsys):
    # Arrange
    base_context['dag'].dag_id = "MY_MONITORED_DAG"
    
    # Simulate database connection failure
    mock_variable.get.side_effect = Exception("Database connection timeout")

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        execute_job_monitor_start(**base_context)
        
    assert "Database connection timeout" in str(exc_info.value)
    
    captured = capsys.readouterr()
    assert "Error updating job monitoring status: Database connection timeout" in captured.out
```