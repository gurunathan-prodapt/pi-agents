# Migration Validation Test Suite
**Job / Include Group:** Shared Files — `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes`  
**Target Modules:** `dags/utils/dw_hole_pfad_knzb.py` and `dags/utils/dw_lese_log_knzb.py`

This test suite validates the behavioral equivalence of the migrated Python utility modules against their legacy UC4 Include (`JOBI`) counterparts.

---

## Test Case 1: Strict Path Resolution (Output Parity & Variable Mapping)

### Purpose
Verify that `dw_hole_pfad_knzb.get_knzb_paths(use_fallback=False)` correctly retrieves global path variables from the Airflow Variable store, matching the legacy behavior where `DW.VARIABLEN` keys must exist.

### Setup
* Mock the Airflow `Variable.get` method to return specific values for the target keys:
  * `dw_variablen_dwh_home` $\rightarrow$ `/opt/prod/dwh_home`
  * `dw_variablen_home` $\rightarrow$ `/home/prod_user`
  * `dw_variablen_istns_home` $\rightarrow$ `/opt/prod/istns_home`

### Action
Execute `get_knzb_paths(use_fallback=False)` and capture the returned dictionary.

### Pass/Fail Criterion
* **Pass:** The returned dictionary matches exactly:
  ```python
  {
      "DWH_HOME": "/opt/prod/dwh_home",
      "HOME": "/home/prod_user",
      "ISTNS_HOME": "/opt/prod/istns_home"
  }
  ```
* **Fail:** Any key is missing, contains an incorrect value, or raises an unexpected exception.

### Test Code
```python
import pytest
from unittest.mock import patch
from dags.utils.dw_hole_pfad_knzb import get_knzb_paths

@patch("dags.utils.dw_hole_pfad_knzb.Variable.get")
def test_strict_path_resolution_success(mock_variable_get):
    # Setup mock registry
    mock_vars = {
        "dw_variablen_dwh_home": "/opt/prod/dwh_home",
        "dw_variablen_home": "/home/prod_user",
        "dw_variablen_istns_home": "/opt/prod/istns_home"
    }
    mock_variable_get.side_effect = lambda key, *args, **kwargs: mock_vars[key]

    # Action
    result = get_knzb_paths(use_fallback=False)

    # Assertions
    assert result["DWH_HOME"] == "/opt/prod/dwh_home"
    assert result["HOME"] == "/home/prod_user"
    assert result["ISTNS_HOME"] == "/opt/prod/istns_home"
    assert len(result) == 3
```

---

## Test Case 2: Missing Variable Error Handling (Strict Mode)

### Purpose
Verify that if any required Airflow Variable is missing, the strict path resolution mode raises a descriptive `KeyError` to prevent downstream tasks from executing with incomplete configurations.

### Setup
* Mock the Airflow `Variable.get` method to raise a `KeyError` when a key is missing (simulating an unconfigured Cloud Composer environment).

### Action
Execute `get_knzb_paths(use_fallback=False)` inside an exception assertion block.

### Pass/Fail Criterion
* **Pass:** The function raises a `KeyError` containing the names of the missing variables and instructions for configuring them in Cloud Composer.
* **Fail:** The function returns a partial dictionary, returns fallback values, or raises a non-descriptive exception.

### Test Code
```python
import pytest
from unittest.mock import patch
from dags.utils.dw_hole_pfad_knzb import get_knzb_paths

@patch("dags.utils.dw_hole_pfad_knzb.Variable.get")
def test_strict_path_resolution_missing_variable(mock_variable_get):
    # Simulate missing key in Airflow Variable store
    mock_variable_get.side_effect = KeyError("dw_variablen_home")

    # Action & Assertion
    with pytest.raises(KeyError) as exc_info:
        get_knzb_paths(use_fallback=False)
    
    assert "Required Airflow Variable missing" in str(exc_info.value)
    assert "dw_variablen_home" in str(exc_info.value)
```

---

## Test Case 3: Fallback Path Resolution (Robustness & Default Handling)

### Purpose
Verify that when `use_fallback=True` is passed, the module gracefully falls back to predefined default paths if variables are missing from the Airflow Variable store.

### Setup
* Mock the Airflow `Variable.get` method to return the `default_var` parameter when called (simulating missing keys in the environment).

### Action
Execute `get_knzb_paths(use_fallback=True)` and capture the returned dictionary.

### Pass/Fail Criterion
* **Pass:** The function returns the default paths:
  * `DWH_HOME` $\rightarrow$ `/opt/dwh_home`
  * `HOME` $\rightarrow$ `/home/dwh_user`
  * `ISTNS_HOME` $\rightarrow$ `/opt/istns_home`
* **Fail:** The function raises a `KeyError` or returns empty values.

### Test Code
```python
import pytest
from unittest.mock import patch
from dags.utils.dw_hole_pfad_knzb import get_knzb_paths

@patch("dags.utils.dw_hole_pfad_knzb.Variable.get")
def test_fallback_path_resolution(mock_variable_get):
    # Simulate Airflow returning the default value when key is missing
    mock_variable_get.side_effect = lambda key, default_var=None: default_var

    # Action
    result = get_knzb_paths(use_fallback=True)

    # Assertions
    assert result["DWH_HOME"] == "/opt/dwh_home"
    assert result["HOME"] == "/home/dwh_user"
    assert result["ISTNS_HOME"] == "/opt/istns_home"
```

---

## Test Case 4: Context Logging Output Parity (Character-for-Character Match)

### Purpose
Verify that `log_uc4_context_helper` extracts the active DAG ID and Task ID from the Airflow execution context and outputs the exact German log string format defined in the legacy UC4 include:  
`"Protokolleintrag: <task_id> innerhalb <dag_id>"`

### Setup
* Create mock Airflow context objects representing a running task instance:
  * Mock DAG with `dag_id = "dw_dwh_stamm_knzb_abgl_start_js"`
  * Mock Task with `task_id = "run_stamm_load"`
* Mock the target logger `airflow.task` to capture log calls.

### Action
Execute `log_uc4_context_helper(**context)` with the mocked context.

### Pass/Fail Criterion
* **Pass:** The logger captures exactly one `INFO` level log containing:  
  `"Protokolleintrag: run_stamm_load innerhalb dw_dwh_stamm_knzb_abgl_start_js"`
* **Fail:** The log message is formatted incorrectly, uses incorrect casing, or fails to resolve the context variables.

### Test Code
```python
import pytest
from unittest.mock import MagicMock, patch
from dags.utils.dw_lese_log_knzb import log_uc4_context_helper

@patch("dags.utils.dw_lese_log_knzb.logger")
def test_context_logging_output_parity(mock_logger):
    # Setup mock Airflow context
    mock_dag = MagicMock()
    mock_dag.dag_id = "dw_dwh_stamm_knzb_abgl_start_js"
    
    mock_task = MagicMock()
    mock_task.task_id = "run_stamm_load"
    
    context = {
        "dag": mock_dag,
        "task": mock_task
    }

    # Action
    log_uc4_context_helper(**context)

    # Assertions
    expected_message = "Protokolleintrag: run_stamm_load innerhalb dw_dwh_stamm_knzb_abgl_start_js"
    mock_logger.info.assert_called_once_with(expected_message)
```

---

## Test Case 5: Context Logging Fallback (Robustness & Non-blocking Behavior)

### Purpose
Verify that if the Airflow context is missing, incomplete, or malformed, the logging helper falls back to safe default values (`Unknown_DAG` / `Unknown_Task`) and does not raise an exception (ensuring logging failures never block the main data pipeline).

### Setup
* Mock the target logger `airflow.task` to capture log calls.
* Prepare three test scenarios:
  1. Empty context dictionary (`{}`)
  2. Context with `None` values (`{"dag": None, "task": None}`)
  3. Context that triggers an unexpected attribute exception during resolution.

### Action
Execute `log_uc4_context_helper` under each scenario.

### Pass/Fail Criterion
* **Pass:** 
  * For scenarios 1 & 2, the logger outputs: `"Protokolleintrag: Unknown_Task innerhalb Unknown_DAG"`.
  * For scenario 3, the exception is caught gracefully, and a warning log is written without raising an exception to the caller.
* **Fail:** An exception propagates out of `log_uc4_context_helper`, halting execution.

### Test Code
```python
import pytest
from unittest.mock import MagicMock, patch
from dags.utils.dw_lese_log_knzb import log_uc4_context_helper

@patch("dags.utils.dw_lese_log_knzb.logger")
def test_context_logging_fallback_empty_context(mock_logger):
    # Scenario 1: Empty context
    log_uc4_context_helper()
    mock_logger.info.assert_called_with("Protokolleintrag: Unknown_Task innerhalb Unknown_DAG")

@patch("dags.utils.dw_lese_log_knzb.logger")
def test_context_logging_fallback_none_values(mock_logger):
    # Scenario 2: None values
    context = {"dag": None, "task": None}
    log_uc4_context_helper(**context)
    mock_logger.info.assert_called_with("Protokolleintrag: Unknown_Task innerhalb Unknown_DAG")

@patch("dags.utils.dw_lese_log_knzb.logger")
def test_context_logging_exception_safety(mock_logger):
    # Scenario 3: Context object raises an unexpected exception on attribute access
    mock_dag = MagicMock()
    type(mock_dag).dag_id = property(lambda self: raise_exception())

    def raise_exception():
        raise RuntimeError("Unexpected metadata corruption")

    context = {"dag": mock_dag}

    # Action
    try:
        log_uc4_context_helper(**context)
    except Exception as err:
        pytest.fail(f"Logging helper raised an exception: {err}. It must be non-blocking.")

    # Assertions
    mock_logger.warning.assert_called_once()
    assert "Failed to log runtime context dynamically" in mock_logger.warning.call_args[0][0]
```