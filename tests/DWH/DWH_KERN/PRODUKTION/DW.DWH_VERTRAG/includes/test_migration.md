Here is the comprehensive suite of migration-validation tests for the shared includes `DW.HOLE_PFAD_VTRG` and `DW.LESE_LOG_VTRG`. 

These tests are written using `pytest` and standard Airflow testing patterns. They validate behavioral equivalence, fallback logic, error handling, and strict adherence to the legacy output formats.

---

## Test Suite Overview

To run these tests, ensure your testing environment has `pytest` and `apache-airflow` installed. You can execute the suite using:
```bash
pytest test_shared_includes.py -v
```

---

## 1. Output Parity & Literal Matching (`DW.LESE_LOG_VTRG`)

### Purpose
To prove that the migrated Python utility `execute_protocol_log` produces the exact same German log output format as the legacy UC4 JOBI script (`"Protokolleintrag: &ADMJOB innerhalb &ADMJP"`), using the active Airflow context parameters.

### Setup
* Mock the Airflow context dictionary containing a mock DAG and a mock TaskInstance.
* Capture the standard logging output using `pytest`'s `caplog` fixture.

### Action
Execute `execute_protocol_log` with the mocked context.

### Pass/Fail Criterion
* **Pass:** The log output contains the exact string: `Protokolleintrag: test_task_id innerhalb test_dag_id`.
* **Fail:** The log output is missing, formatted differently, translated, or contains incorrect context values.

```python
# test_shared_includes.py
import logging
import pytest
from unittest.mock import MagicMock
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.models import Variable

# Import the migrated modules under test
import dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.dw_lese_log_vtrg as lese_log
import dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.dw_hole_pfad_vtrg as hole_pfad


def test_lese_log_output_parity(caplog):
    """
    Validates that the log output matches the legacy UC4 print statement:
    'Protokolleintrag: &ADMJOB innerhalb &ADMJP'
    """
    # Setup mock context
    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag_id"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "test_task_id"
    
    context = {
        'dag': mock_dag,
        'ti': mock_ti
    }
    
    # Action
    with caplog.at_level(logging.INFO):
        lese_log.execute_protocol_log(**context)
        
    # Pass/Fail Assertion
    expected_literal = "Protokolleintrag: test_task_id innerhalb test_dag_id"
    assert any(expected_literal in record.message for record in caplog.records), \
        f"Expected log literal '{expected_literal}' was not found in logs."
```

---

## 2. Context Missing / Robustness Handling (`DW.LESE_LOG_VTRG`)

### Purpose
To verify that the logging utility handles missing or incomplete Airflow contexts gracefully without crashing silently, falling back to `"UNKNOWN_TASK"` and `"UNKNOWN_DAG"`.

### Setup
* Provide an empty context dictionary `{}` to the function.
* Capture the standard logging output.

### Action
Execute `execute_protocol_log` with the empty context.

### Pass/Fail Criterion
* **Pass:** The function executes successfully and logs: `Protokolleintrag: UNKNOWN_TASK innerhalb UNKNOWN_DAG`.
* **Fail:** The function raises an unhandled exception or logs incorrect fallback values.

```python
def test_lese_log_missing_context_fallback(caplog):
    """
    Validates that missing context keys fall back to UNKNOWN values 
    without raising unhandled exceptions.
    """
    # Setup empty context
    context = {}
    
    # Action
    with caplog.at_level(logging.INFO):
        lese_log.execute_protocol_log(**context)
        
    # Pass/Fail Assertion
    expected_fallback = "Protokolleintrag: UNKNOWN_TASK innerhalb UNKNOWN_DAG"
    assert any(expected_fallback in record.message for record in caplog.records), \
        f"Expected fallback log '{expected_fallback}' was not found."
```

---

## 3. Path Resolution & XCom Push (`DW.HOLE_PFAD_VTRG`)

### Purpose
To prove that `resolve_dwh_paths` successfully retrieves variables from the Airflow Variable store and pushes them to XCom with the correct keys (`DWH_HOME`, `HOME`, `PMS_HOME`), mimicking the legacy `GET_VAR` assignments.

### Setup
* Mock `Variable.get` to return predefined mock GCS paths.
* Mock the Airflow TaskInstance (`ti`) to track `xcom_push` calls.

### Action
Execute `resolve_dwh_paths` with the mocked context.

### Pass/Fail Criterion
* **Pass:** 
  * The returned dictionary contains the correct mapped paths.
  * `xcom_push` is called exactly 3 times with the correct keys and values.
* **Fail:** Variables are not resolved, or XCom pushes are missing or miskeyed.

```python
def test_hole_pfad_resolution_and_xcom(monkeypatch):
    """
    Validates that paths are resolved from Airflow Variables and pushed to XCom.
    """
    # Setup mock variables
    mock_vars = {
        "dw_variablen_dwh_home": "gs://prod-bucket/dwh_home_dir",
        "dw_variablen_home": "gs://prod-bucket/home_dir",
        "dw_variablen_pms_home": "gs://prod-bucket/pms_home_dir"
    }
    
    def mock_variable_get(key, default_var=None):
        return mock_vars.get(key, default_var)
        
    monkeypatch.setattr(Variable, "get", mock_variable_get)
    
    # Setup mock TaskInstance for XCom tracking
    mock_ti = MagicMock()
    pushed_xcoms = {}
    
    def mock_xcom_push(key, value):
        pushed_xcoms[key] = value
        
    mock_ti.xcom_push.side_effect = mock_xcom_push
    context = {'ti': mock_ti}
    
    # Action
    result = hole_pfad.resolve_dwh_paths(**context)
    
    # Pass/Fail Assertions
    assert result["DWH_HOME"] == "gs://prod-bucket/dwh_home_dir"
    assert result["HOME"] == "gs://prod-bucket/home_dir"
    assert result["PMS_HOME"] == "gs://prod-bucket/pms_home_dir"
    
    assert pushed_xcoms["DWH_HOME"] == "gs://prod-bucket/dwh_home_dir"
    assert pushed_xcoms["HOME"] == "gs://prod-bucket/home_dir"
    assert pushed_xcoms["PMS_HOME"] == "gs://prod-bucket/pms_home_dir"
    assert mock_ti.xcom_push.call_count == 3
```

---

## 4. GCS Path Fallback Policies (`DW.HOLE_PFAD_VTRG`)

### Purpose
To verify that when Airflow Variables are not set in the environment, the utility falls back to the correct GCS bucket structure using the `GCS_BUCKET` environment variable, rather than hardcoded local POSIX paths.

### Setup
* Mock `Variable.get` to return its `default_var` argument.
* Set the `GCS_BUCKET` environment variable to `test-migration-bucket`.

### Action
Execute `resolve_dwh_paths` without context.

### Pass/Fail Criterion
* **Pass:** The returned paths are prefixed with `gs://test-migration-bucket/` (e.g., `gs://test-migration-bucket/dwh_home`).
* **Fail:** The paths fall back to local file paths (e.g., `/opt/dwh`) or default to the fallback bucket literal `gs://dwh-bucket-default` when `GCS_BUCKET` is present.

```python
def test_hole_pfad_gcs_fallback_policies(monkeypatch):
    """
    Validates that fallback paths use GCS prefixes derived from the GCS_BUCKET env var.
    """
    # Force Variable.get to return the default_var argument
    monkeypatch.setattr(Variable, "get", lambda key, default_var=None: default_var)
    
    # Set environment variable
    monkeypatch.setenv("GCS_BUCKET", "test-migration-bucket")
    
    # Action
    result = hole_pfad.resolve_dwh_paths(context=None)
    
    # Pass/Fail Assertions
    assert result["DWH_HOME"] == "gs://test-migration-bucket/dwh_home"
    assert result["HOME"] == "gs://test-migration-bucket/home"
    assert result["PMS_HOME"] == "gs://test-migration-bucket/pms_home"
```

---

## 5. Error Handling & Pipeline Halt Assertion

### Purpose
To prove that if path resolution fails (e.g., due to database connectivity issues with the Airflow Variable store), the task raises an `AirflowException` to immediately halt downstream execution.

### Setup
* Mock `Variable.get` to raise an operational exception.

### Action
Execute `resolve_dwh_paths`.

### Pass/Fail Criterion
* **Pass:** The function raises `AirflowException`, preventing downstream tasks from running with unresolved or empty paths.
* **Fail:** The function fails silently, returns partial paths, or raises a generic non-Airflow exception.

```python
def test_hole_pfad_raises_airflow_exception_on_failure(monkeypatch):
    """
    Validates that any failure during path resolution raises an AirflowException
    to guarantee downstream tasks halt immediately.
    """
    # Setup mock to raise an exception
    def mock_get_raise_error(key, default_var=None):
        raise RuntimeError("Database connection lost")
        
    monkeypatch.setattr(Variable, "get", mock_get_raise_error)
    
    # Action & Pass/Fail Assertion
    with pytest.raises(AirflowException) as exc_info:
        hole_pfad.resolve_dwh_paths(context=None)
        
    assert "Failed to resolve path variables" in str(exc_info.value)
```