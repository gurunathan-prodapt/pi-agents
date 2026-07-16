# Migration Validation Test Suite
**Job:** Shared Files — `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes`  
**Target Platform:** Cloud Composer (Apache Airflow)  
**Modules Under Test:** 
* `plugins/utils/dw_hole_pfad_knzb.py` (Migrated from `DW.HOLE_PFAD_KNZB.xml`)
* `plugins/utils/dw_lese_log_knzb.py` (Migrated from `DW.LESE_LOG_KNZB.xml`)

---

## Test Case 1: Path Resolution - Successful Variable Retrieval (Output Parity)

### Purpose
Verify that `get_path_variables()` correctly retrieves and resolves path variables when they are defined in the Airflow Variable store, matching the behavior of the legacy UC4 `GET_VAR` calls on the `DW.VARIABLEN` container.

### Setup
* Mock or populate the Airflow Variable store with the following keys and values:
  * `dw_variablen_dwh_home` = `gs://prod-dwh-bucket/dwh_home/`
  * `dw_variablen_home` = `gs://prod-dwh-bucket/home/`
  * `dw_variablen_istns_home` = `gs://prod-dwh-bucket/istns_home/`
* Clear any environment variable named `GCS_BUCKET` to isolate testing to the Airflow Variable store.

### Action
Execute `get_path_variables()` within a test harness.

### Pass/Fail Criterion
* **Pass:** The function returns a dictionary containing exactly:
  ```python
  {
      "DWH_HOME": "gs://prod-dwh-bucket/dwh_home/",
      "HOME": "gs://prod-dwh-bucket/home/",
      "ISTNS_HOME": "gs://prod-dwh-bucket/istns_home/"
  }
  ```
* **Fail:** Any key is missing, values do not match the setup, or an exception is raised.

---

## Test Case 2: Path Resolution - Fallback to Environment Variables (Transformation Correctness)

### Purpose
Verify that when Airflow Variables are absent, the module correctly falls back to constructing paths using the `GCS_BUCKET` environment variable.

### Setup
* Mock the Airflow Variable store to return `None` or raise a `KeyError` for:
  * `dw_variablen_dwh_home`
  * `dw_variablen_home`
  * `dw_variablen_istns_home`
* Set the environment variable `GCS_BUCKET` to `test-fallback-bucket`.

### Action
Execute `get_path_variables()`.

### Pass/Fail Criterion
* **Pass:** The function returns:
  ```python
  {
      "DWH_HOME": "gs://test-fallback-bucket/dwh/",
      "HOME": "gs://test-fallback-bucket/home/",
      "ISTNS_HOME": "gs://test-fallback-bucket/istns/"
  }
  ```
* **Fail:** The fallback paths are incorrectly formatted, or an `AirflowException` is raised despite `GCS_BUCKET` being set.

---

## Test Case 3: Path Resolution - Missing Configuration Validation (Edge Case / Exception Handling)

### Purpose
Verify that the module raises an `AirflowException` when neither Airflow Variables nor the fallback environment variables are configured, preventing downstream tasks from executing with empty paths.

### Setup
* Mock the Airflow Variable store to return `None` for all three variables.
* Ensure the environment variable `GCS_BUCKET` is unset or empty.

### Action
Execute `get_path_variables()` inside an exception-assertion block.

### Pass/Fail Criterion
* **Pass:** An `AirflowException` is raised with a message containing:  
  `Missing required path variable configurations!`
* **Fail:** The function returns a dictionary with empty/None values, or raises a generic non-Airflow exception.

---

## Test Case 4: XCom Integration - Task Instance Push (External-System Replacement)

### Purpose
Verify that the `verify_and_load_paths_callable` wrapper correctly pushes resolved paths to Airflow XComs for downstream task consumption.

### Setup
* Mock the Airflow Variable store to return valid paths.
* Create a mocked Airflow Task Instance (`ti`) and execution context dictionary.

### Action
Execute `verify_and_load_paths_callable(**context)`.

### Pass/Fail Criterion
* **Pass:** The mocked `ti.xcom_push` is called exactly three times with the following key-value pairs:
  * `key="DWH_HOME"`, `value` matches resolved `DWH_HOME`
  * `key="HOME"`, `value` matches resolved `HOME`
  * `key="ISTNS_HOME"`, `value` matches resolved `ISTNS_HOME`
* **Fail:** Any of the three keys are not pushed, or the values pushed do not match the resolved variables.

---

## Test Case 5: Structured Audit Logging - Verbatim Output Parity (Output Parity)

### Purpose
Verify that `log_uc4_metadata` outputs the exact German audit log format matching the legacy UC4 `DW.LESE_LOG_KNZB` script:  
`"Protokolleintrag: &ADMJOB innerhalb &ADMJP"`

### Setup
* Create a mock DAG object with `dag_id="DW_STAMM_KNZB_ABGL_START_JS"`.
* Create a mock TaskInstance object with `task_id="dw_stamm_knzb_task"`.
* Construct an Airflow context dictionary:
  ```python
  context = {
      "dag": mock_dag,
      "task_instance": mock_task_instance
  }
  ```
* Intercept/capture log messages written to the `airflow.task` logger.

### Action
Execute `log_uc4_metadata(context)`.

### Pass/Fail Criterion
* **Pass:** The captured log output contains the exact string:  
  `Protokolleintrag: dw_stamm_knzb_task innerhalb DW_STAMM_KNZB_ABGL_START_JS`  
  bounded by lines of 80 hyphens (`--------------------------------------------------------------------------------`).
* **Fail:** The log message is missing, formatted incorrectly, or does not preserve the literal German text structure.

---

## Test Case 6: Logging Resilience - Missing Context (Edge Case / Exception Handling)

### Purpose
Verify that `log_uc4_metadata` is completely non-blocking and fails gracefully when the execution context is missing, malformed, or empty, ensuring logging failures never crash the parent pipeline.

### Setup
* Prepare an empty context dictionary: `context = {}`.
* Intercept/capture log messages written to the `airflow.task` logger.

### Action
Execute `log_uc4_metadata(context)`.

### Pass/Fail Criterion
* **Pass:** 
  1. The function executes without raising any exception.
  2. The captured log output contains:  
     `Protokolleintrag: Unknown_Task innerhalb Unknown_DAG`
* **Fail:** An exception is raised to the caller, or the execution of the calling thread is halted.

---

## Runnable Test Suite (Pytest Implementation)

This executable test suite implements all the validation test cases defined above.

```python
import os
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

# Import the migrated utility modules
from dw_hole_pfad_knzb import get_path_variables, verify_and_load_paths_callable
from dw_lese_log_knzb import log_uc4_metadata


# ==============================================================================
# TESTS FOR: dw_hole_pfad_knzb.py
# ==============================================================================

@patch("dw_hole_pfad_knzb.Variable")
@patch.dict(os.environ, {}, clear=True)
def test_get_path_variables_success_from_variables(mock_variable):
    """Test Case 1: Successful path resolution from Airflow Variables."""
    mock_variable.get.side_effect = lambda key, default_var=None: {
        "dw_variablen_dwh_home": "gs://prod-dwh-bucket/dwh_home/",
        "dw_variablen_home": "gs://prod-dwh-bucket/home/",
        "dw_variablen_istns_home": "gs://prod-dwh-bucket/istns_home/"
    }.get(key, default_var)

    result = get_path_variables()

    assert result["DWH_HOME"] == "gs://prod-dwh-bucket/dwh_home/"
    assert result["HOME"] == "gs://prod-dwh-bucket/home/"
    assert result["ISTNS_HOME"] == "gs://prod-dwh-bucket/istns_home/"


@patch("dw_hole_pfad_knzb.Variable")
@patch.dict(os.environ, {"GCS_BUCKET": "test-fallback-bucket"}, clear=True)
def test_get_path_variables_fallback_to_env(mock_variable):
    """Test Case 2: Fallback to GCS_BUCKET environment variable when Airflow vars are missing."""
    # Variable.get returns default_var if the variable is not set in Airflow
    mock_variable.get.side_effect = lambda key, default_var=None: default_var

    result = get_path_variables()

    assert result["DWH_HOME"] == "gs://test-fallback-bucket/dwh/"
    assert result["HOME"] == "gs://test-fallback-bucket/home/"
    assert result["ISTNS_HOME"] == "gs://test-fallback-bucket/istns/"


@patch("dw_hole_pfad_knzb.Variable")
@patch.dict(os.environ, {}, clear=True)
def test_get_path_variables_missing_config_raises_exception(mock_variable):
    """Test Case 3: AirflowException raised when no configuration is available."""
    mock_variable.get.return_value = None

    with pytest.raises(AirflowException) as exc_info:
        get_path_variables()
    
    assert "Missing required path variable configurations!" in str(exc_info.value)


@patch("dw_hole_pfad_knzb.get_path_variables")
def test_verify_and_load_paths_callable_pushes_to_xcom(mock_get_paths):
    """Test Case 4: Callable wrapper successfully pushes resolved paths to XCom."""
    mock_get_paths.return_value = {
        "DWH_HOME": "gs://bucket/dwh/",
        "HOME": "gs://bucket/home/",
        "ISTNS_HOME": "gs://bucket/istns/"
    }
    
    mock_ti = MagicMock()
    context = {"ti": mock_ti}

    verify_and_load_paths_callable(**context)

    # Verify xcom_push was called for each path variable
    mock_ti.xcom_push.assert_any_call(key="DWH_HOME", value="gs://bucket/dwh/")
    mock_ti.xcom_push.assert_any_call(key="HOME", value="gs://bucket/home/")
    mock_ti.xcom_push.assert_any_call(key="ISTNS_HOME", value="gs://bucket/istns/")
    assert mock_ti.xcom_push.call_count == 3


# ==============================================================================
# TESTS FOR: dw_lese_log_knzb.py
# ==============================================================================

@patch("dw_lese_log_knzb.logger")
def test_log_uc4_metadata_verbatim_output(mock_logger):
    """Test Case 5: Verify exact German audit log format matches legacy output."""
    mock_dag = MagicMock()
    mock_dag.dag_id = "DW_STAMM_KNZB_ABGL_START_JS"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "dw_stamm_knzb_task"

    context = {
        "dag": mock_dag,
        "task_instance": mock_ti
    }

    log_uc4_metadata(context)

    # Verify the exact log format is printed
    mock_logger.info.assert_any_call("-" * 80)
    mock_logger.info.assert_any_call(
        "Protokolleintrag: dw_stamm_knzb_task innerhalb DW_STAMM_KNZB_ABGL_START_JS"
    )


@patch("dw_lese_log_knzb.logger")
def test_log_uc4_metadata_resilience_empty_context(mock_logger):
    """Test Case 6: Verify logging is non-blocking and handles empty context gracefully."""
    context = {}

    # This call must not raise any exceptions
    try:
        log_uc4_metadata(context)
    except Exception as e:
        pytest.fail(f"log_uc4_metadata raised an exception on empty context: {e}")

    # Verify fallback values are logged
    mock_logger.info.assert_any_call(
        "Protokolleintrag: Unknown_Task innerhalb Unknown_DAG"
    )
```