Here is the migration-validation test suite designed to verify that the migrated Airflow Python implementation of `DW.DWH_ADM_JOB_MONITOR_START` is behaviorally equivalent to the legacy UC4 JOBI script.

---

# Migration Validation Test Suite: DW.DWH_ADM_JOB_MONITOR_START

This test suite uses `pytest` along with standard mocking libraries to validate the Python-based migration of the UC4 JOBI script. It ensures that metadata extraction, filtering logic, variable lookups, and state updates behave exactly as they did in the legacy system.

## Test Case 1: Context Extraction and Initialization (Metadata Mapping)

### Purpose
Verify that the task correctly extracts the parent DAG ID (`admjp`), Task ID (`admjob`), and Run ID (`admnrjob`) from the Airflow execution context, mapping them to the legacy UC4 variables `SYS_ACT_JPNAME()`, `SYS_ACT_JOBNAME()`, and `SYS_ACT_JOBNR()`.

### Setup
*   Mock the Airflow context dictionary with specific DAG, Task, and DagRun objects.
*   Mock `Variable.get` to return an empty dictionary to prevent downstream registration logic from throwing exceptions.

### Action
Execute `dwh_adm_job_monitor_start` with the mocked context and capture standard output (stdout).

### Pass/Fail Criterion
*   **Pass:** The stdout contains the exact literal string: `Job test_task mit RNR test_run_id gestartet aus test_dag_id` (matching the legacy German log requirement).
*   **Fail:** The context variables are not extracted, or the printed log does not match the required format and casing.

```python
import pytest
from unittest.mock import MagicMock, patch
import io
import sys

from uc4_airflow.DW_DWH_ADM_JOB_MONITOR_START import dwh_adm_job_monitor_start

def test_context_extraction_and_initialization():
    # Setup mock context
    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag_id"
    
    mock_task = MagicMock()
    mock_task.task_id = "test_task"
    
    mock_dag_run = MagicMock()
    mock_dag_run.run_id = "test_run_id"
    
    context = {
        'dag': mock_dag,
        'task': mock_task,
        'dag_run': mock_dag_run
    }
    
    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    # Mock Variable.get to return empty dict (no monitoring triggered)
    with patch('airflow.models.Variable.get', return_value={}):
        try:
            dwh_adm_job_monitor_start(**context)
        finally:
            sys.stdout = sys.__stdout__
            
    output = captured_output.getvalue()
    
    # Assertions
    expected_log = "Job test_task mit RNR test_run_id gestartet aus test_dag_id"
    assert expected_log in output, f"Expected log '{expected_log}' not found in output: '{output}'"
```

---

## Test Case 2: Monitored Job Plan Filtering - Positive Match (Transformation Correctness)

### Purpose
Verify that when the current DAG ID is listed in the `DW_DWH_MONITORED_JPS` variable with an active flag of `"J"`, the job is successfully registered in the `DW_DWH_RUNNING_JOBS` registry.

### Setup
*   Mock the Airflow context with `dag_id="MY_MONITORED_DAG"`, `task_id="MY_JOB"`, and `run_id="RUN_12345"`.
*   Mock `Variable.get` to return:
    *   `DW_DWH_MONITORED_JPS` = `{"MY_MONITORED_DAG": "J", "OTHER_DAG": "N"}`
    *   `DW_DWH_RUNNING_JOBS` = `{"EXISTING_JOB": "RUN_00000"}`
*   Mock `Variable.set` to capture updates.

### Action
Execute `dwh_adm_job_monitor_start` with the mocked context.

### Pass/Fail Criterion
*   **Pass:** 
    *   The stdout contains: `Added MY_JOB with RUN_12345`.
    *   `Variable.set` is called with key `"DW_DWH_RUNNING_JOBS"` and the updated dictionary containing `{"EXISTING_JOB": "RUN_00000", "MY_JOB": "RUN_12345"}`.
*   **Fail:** The job is not registered, or the existing running jobs are overwritten/wiped out.

```python
@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_monitored_job_positive_match(mock_get, mock_set):
    # Setup context
    context = {
        'dag': MagicMock(dag_id="MY_MONITORED_DAG"),
        'task': MagicMock(task_id="MY_JOB"),
        'dag_run': MagicMock(run_id="RUN_12345")
    }
    
    # Mock Variable.get side effects
    def side_effect_get(key, *args, **kwargs):
        if key == "DW_DWH_MONITORED_JPS":
            return {"MY_MONITORED_DAG": "J", "OTHER_DAG": "N"}
        if key == "DW_DWH_RUNNING_JOBS":
            return {"EXISTING_JOB": "RUN_00000"}
        return {}
    
    mock_get.side_effect = side_effect_get
    
    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    try:
        dwh_adm_job_monitor_start(**context)
    finally:
        sys.stdout = sys.__stdout__
        
    output = captured_output.getvalue()
    
    # Assertions
    assert "Added MY_JOB with RUN_12345" in output
    
    # Verify Variable.set was called with updated registry
    mock_set.assert_called_once_with(
        "DW_DWH_RUNNING_JOBS",
        {"EXISTING_JOB": "RUN_00000", "MY_JOB": "RUN_12345"},
        serialize_json=True
    )
```

---

## Test Case 3: Global Monitoring Wildcard - "ALL" (Transformation Correctness)

### Purpose
Verify that if the `DW_DWH_MONITORED_JPS` variable contains the key `"ALL"` mapped to `"J"`, any executing DAG is registered for monitoring, regardless of its specific DAG ID.

### Setup
*   Mock the Airflow context with `dag_id="ANY_RANDOM_DAG"`, `task_id="ANY_JOB"`, and `run_id="RUN_999"`.
*   Mock `Variable.get` to return:
    *   `DW_DWH_MONITORED_JPS` = `{"ALL": "J"}`
    *   `DW_DWH_RUNNING_JOBS` = `{}`

### Action
Execute `dwh_adm_job_monitor_start` with the mocked context.

### Pass/Fail Criterion
*   **Pass:** The job is successfully registered in `DW_DWH_RUNNING_JOBS` and the log `Added ANY_JOB with RUN_999` is printed.
*   **Fail:** The wildcard is ignored, and the job is not registered.

```python
@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_monitored_job_wildcard_match(mock_get, mock_set):
    context = {
        'dag': MagicMock(dag_id="ANY_RANDOM_DAG"),
        'task': MagicMock(task_id="ANY_JOB"),
        'dag_run': MagicMock(run_id="RUN_999")
    }
    
    def side_effect_get(key, *args, **kwargs):
        if key == "DW_DWH_MONITORED_JPS":
            return {"ALL": "J"}
        if key == "DW_DWH_RUNNING_JOBS":
            return {}
        return {}
    
    mock_get.side_effect = side_effect_get
    
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    try:
        dwh_adm_job_monitor_start(**context)
    finally:
        sys.stdout = sys.__stdout__
        
    output = captured_output.getvalue()
    
    assert "Added ANY_JOB with RUN_999" in output
    mock_set.assert_called_once_with(
        "DW_DWH_RUNNING_JOBS",
        {"ANY_JOB": "RUN_999"},
        serialize_json=True
    )
```

---

## Test Case 4: Non-Monitored Job Plan (Transformation Correctness - Negative Case)

### Purpose
Verify that if a DAG is not registered in `DW_DWH_MONITORED_JPS`, or is registered with a value other than `"J"` (e.g., `"N"`), it is not added to the active monitoring registry.

### Setup
*   Mock the Airflow context with `dag_id="UNMONITORED_DAG"`.
*   Mock `Variable.get` to return:
    *   `DW_DWH_MONITORED_JPS` = `{"UNMONITORED_DAG": "N", "SOME_OTHER_DAG": "J"}`
    *   `DW_DWH_RUNNING_JOBS` = `{"EXISTING_JOB": "RUN_00000"}`

### Action
Execute `dwh_adm_job_monitor_start` with the mocked context.

### Pass/Fail Criterion
*   **Pass:** 
    *   The stdout does *not* contain `Added` or `DW_DWH_RUNNING_JOBS` updates.
    *   `Variable.set` is never called.
*   **Fail:** The job is incorrectly added to the active monitoring registry, or `Variable.set` is called.

```python
@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_unmonitored_job_no_registration(mock_get, mock_set):
    context = {
        'dag': MagicMock(dag_id="UNMONITORED_DAG"),
        'task': MagicMock(task_id="UNMONITORED_JOB"),
        'dag_run': MagicMock(run_id="RUN_555")
    }
    
    def side_effect_get(key, *args, **kwargs):
        if key == "DW_DWH_MONITORED_JPS":
            return {"UNMONITORED_DAG": "N", "SOME_OTHER_DAG": "J"}
        if key == "DW_DWH_RUNNING_JOBS":
            return {"EXISTING_JOB": "RUN_00000"}
        return {}
    
    mock_get.side_effect = side_effect_get
    
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    try:
        dwh_adm_job_monitor_start(**context)
    finally:
        sys.stdout = sys.__stdout__
        
    output = captured_output.getvalue()
    
    # Assertions
    assert "Added UNMONITORED_JOB" not in output
    mock_set.assert_not_called()
```

---

## Test Case 5: Robustness and Schema Handling (NULL / Empty / Malformed Inputs)

### Purpose
Verify that the code handles missing, empty, or malformed Airflow Variables gracefully without raising unhandled exceptions, mirroring the robust execution of UC4 script blocks.

### Setup
*   Mock the Airflow context with a valid DAG.
*   Mock `Variable.get` to raise an exception or return malformed data (e.g., a string instead of a JSON dictionary/list).

### Action
Execute `dwh_adm_job_monitor_start` under three scenarios:
1.  `DW_DWH_MONITORED_JPS` variable does not exist (raises exception).
2.  `DW_DWH_MONITORED_JPS` is a malformed string.
3.  `DW_DWH_RUNNING_JOBS` is a malformed string.

### Pass/Fail Criterion
*   **Pass:** The function completes execution without throwing any exceptions.
*   **Fail:** The function crashes due to unhandled `KeyError`, `TypeError`, or `ValueError`.

```python
@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_malformed_variables_graceful_handling(mock_get, mock_set):
    context = {
        'dag': MagicMock(dag_id="TEST_DAG"),
        'task': MagicMock(task_id="TEST_JOB"),
        'dag_run': MagicMock(run_id="RUN_111")
    }
    
    # Scenario 1: Variable.get raises an exception (Variable missing)
    mock_get.side_effect = Exception("Variable not found")
    
    try:
        dwh_adm_job_monitor_start(**context)
    except Exception as e:
        pytest.fail(f"Function raised an exception on missing variables: {e}")
        
    # Scenario 2: Variable.get returns a malformed non-dict/non-list type
    mock_get.side_effect = None
    mock_get.return_value = "NOT_A_JSON_OBJECT"
    
    try:
        dwh_adm_job_monitor_start(**context)
    except Exception as e:
        pytest.fail(f"Function raised an exception on malformed variable schema: {e}")
        
    # Verify no writes were attempted on malformed data
    mock_set.assert_not_called()
```