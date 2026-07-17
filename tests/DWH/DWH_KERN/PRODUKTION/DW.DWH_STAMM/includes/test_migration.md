Here is the comprehensive suite of migration-validation tests designed to verify the behavioral equivalence of the migrated Python utility modules against their legacy UC4 XML Include (`JOBI`) counterparts.

---

# Test Suite: Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes

## Test Case 1: Path Resolution Parity (`DW.HOLE_PFAD_KNZB`)
### Purpose
Verify that `get_path_variables` correctly retrieves the environment paths from the Airflow Variable Metadata DB, returns them as a structured dictionary, and pushes them to XCom with the exact expected keys.

### Setup
* Mock the Airflow `Variable.get` interface to return predefined path values.
* Mock the Airflow TaskInstance (`ti`) context object to capture XCom pushes.

### Action
Execute `get_path_variables` passing a mocked context dictionary.

### Pass/Fail Criterion
* **Pass:** The function returns a dictionary containing the exact keys `DWH_HOME`, `HOME`, and `ISTNS_HOME` with their corresponding mock values. Additionally, the mock `ti.xcom_push` must be called exactly 3 times with the correct key-value pairs.
* **Fail:** Any of the variables are missing, keys are incorrectly cased, XCom pushes fail, or an unexpected exception is raised.

```python
import pytest
from unittest.mock import MagicMock, call
from airflow.exceptions import AirflowException

# Import the migrated module
import dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_STAMM.includes.DW_HOLE_PFAD_KNZB as hole_pfad

def test_hole_pfad_knzb_success(monkeypatch):
    # Setup mock variables
    mock_vars = {
        "dw_variablen_dwh_home": "gs://prod-dwh-bucket/dwh_home",
        "dw_variablen_home": "gs://prod-dwh-bucket/home",
        "dw_variablen_istns_home": "gs://prod-dwh-bucket/istns_home"
    }
    
    def mock_variable_get(key, *args, **kwargs):
        if key in mock_vars:
            return mock_vars[key]
        raise KeyError(f"Variable {key} not found")
        
    monkeypatch.setattr(hole_pfad.Variable, "get", mock_variable_get)
    
    # Setup mock context and TaskInstance
    mock_ti = MagicMock()
    mock_context = {'ti': mock_ti}
    
    # Action
    result = hole_pfad.get_path_variables(**mock_context)
    
    # Assertions
    expected_result = {
        "DWH_HOME": "gs://prod-dwh-bucket/dwh_home",
        "HOME": "gs://prod-dwh-bucket/home",
        "ISTNS_HOME": "gs://prod-dwh-bucket/istns_home"
    }
    
    assert result == expected_result, f"Expected {expected_result}, got {result}"
    
    # Verify XCom pushes
    expected_calls = [
        call(key='DWH_HOME', value="gs://prod-dwh-bucket/dwh_home"),
        call(key='HOME', value="gs://prod-dwh-bucket/home"),
        call(key='ISTNS_HOME', value="gs://prod-dwh-bucket/istns_home")
    ]
    mock_ti.xcom_push.assert_has_calls(expected_calls, any_order=True)
```

---

## Test Case 2: Path Resolution Error Handling & Robustness
### Purpose
Verify that `get_path_variables` raises an `AirflowException` when any of the required Airflow variables are missing from the Metadata DB, matching the legacy UC4 behavior where a missing variable causes the parent job to abend.

### Setup
* Mock the Airflow `Variable.get` interface to raise a `KeyError` or return `None` for one or more variables.

### Action
Execute `get_path_variables` inside a `pytest.raises` block.

### Pass/Fail Criterion
* **Pass:** The function raises an `AirflowException` containing a descriptive error message indicating a failure to fetch variables from the metadata store.
* **Fail:** The function fails silently, returns partial/incomplete data, or raises a generic unhandled system exception.

```python
def test_hole_pfad_knzb_missing_variable_raises_exception(monkeypatch):
    # Setup mock to simulate a missing variable in Airflow DB
    def mock_variable_get(key, *args, **kwargs):
        if key == "dw_variablen_dwh_home":
            return "gs://prod-dwh-bucket/dwh_home"
        # Simulate missing variable for 'home'
        raise ValueError("Variable key does not exist")
        
    monkeypatch.setattr(hole_pfad.Variable, "get", mock_variable_get)
    
    mock_ti = MagicMock()
    mock_context = {'ti': mock_ti}
    
    # Action & Assertion
    with pytest.raises(AirflowException) as exc_info:
        hole_pfad.get_path_variables(**mock_context)
        
    assert "Error fetching path variables from Airflow metadata store" in str(exc_info.value)
```

---

## Test Case 3: Log Output Parity & German Literal Preservation (`DW.LESE_LOG_KNZB`)
### Purpose
Verify that `write_execution_log` correctly extracts the DAG ID and Task ID from the Airflow context and outputs the exact German log string format: `Protokolleintrag: <task_id> innerhalb <dag_id>`.

### Setup
* Mock the Python `logging` library or use pytest's `caplog` fixture to capture standard output logs.
* Mock the Airflow context dictionary containing a mock DAG and a mock Task Instance.

### Action
Execute `write_execution_log` with the mocked context.

### Pass/Fail Criterion
* **Pass:** The captured log contains the exact string: `Protokolleintrag: test_task_id innerhalb test_dag_id`.
* **Fail:** The log output is missing, the German phrasing is altered, or the task/DAG identifiers are incorrectly mapped.

```python
import logging
import dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_STAMM.includes.DW_LESE_LOG_KNZB as lese_log

def test_lese_log_knzb_output_parity(caplog):
    # Setup mock context
    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag_id"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "test_task_id"
    
    mock_context = {
        'dag': mock_dag,
        'task_instance': mock_ti
    }
    
    # Action
    with caplog.at_level(logging.INFO):
        lese_log.write_execution_log(**mock_context)
        
    # Assertions
    expected_log_message = "Protokolleintrag: test_task_id innerhalb test_dag_id"
    assert any(expected_log_message in record.message for record in caplog.records), \
        f"Expected log message '{expected_log_message}' was not found in captured logs."
```

---

## Test Case 4: Logging Fallback / Out-of-Context Execution
### Purpose
Verify that `write_execution_log` is robust enough to handle empty, partial, or missing execution contexts (e.g., when run during manual testing or outside of an active Airflow TaskInstance) without throwing exceptions, falling back gracefully to placeholders.

### Setup
* Prepare an empty context dictionary `{}` and a `None` context.
* Use pytest's `caplog` fixture to capture standard output logs.

### Action
Execute `write_execution_log` with empty and `None` contexts.

### Pass/Fail Criterion
* **Pass:** The function executes successfully without raising any exceptions and outputs the fallback log: `Protokolleintrag: UNKNOWN_TASK innerhalb UNKNOWN_DAG`.
* **Fail:** The function raises a `KeyError`, `AttributeError`, or fails to log the fallback statement.

```python
def test_lese_log_knzb_fallback_handling(caplog):
    # Action 1: Empty context dictionary
    with caplog.at_level(logging.INFO):
        try:
            lese_log.write_execution_log(**{})
        except Exception as e:
            pytest.fail(f"write_execution_log raised an exception on empty context: {e}")
            
    expected_fallback_msg = "Protokolleintrag: UNKNOWN_TASK innerhalb UNKNOWN_DAG"
    assert any(expected_fallback_msg in record.message for record in caplog.records), \
        "Fallback message not found for empty context."
        
    caplog.clear()
    
    # Action 2: None context (simulated by calling without unpacking)
    with caplog.at_level(logging.INFO):
        try:
            lese_log.write_execution_log()
        except Exception as e:
            pytest.fail(f"write_execution_log raised an exception on missing context: {e}")
            
    assert any(expected_fallback_msg in record.message for record in caplog.records), \
        "Fallback message not found for missing context."
```