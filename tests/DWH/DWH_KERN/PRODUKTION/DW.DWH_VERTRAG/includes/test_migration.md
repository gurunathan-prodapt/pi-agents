Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Python modules (`path_resolver.py` and `logging_helper.py`) are behaviorally equivalent to their legacy UC4 Job Include (`JOBI`) counterparts.

---

# Migration Validation Test Suite: `DW.DWH_VERTRAG/includes`

## Section 1: Path Resolution Parity (`DW.HOLE_PFAD_VTRG`)

### Test Case 1.1: Successful Path Resolution (Happy Path)
* **Purpose**: Verify that when all target Airflow variables are correctly configured in the environment, `DynamicPathResolver.get_paths()` retrieves and returns them accurately, matching the legacy variable assignment behavior.
* **Setup**:
  * Mock or populate the Airflow Metadata Database (or use `unittest.mock`) with the following key-value pairs:
    * `dw_variablen_dwh_home` = `/opt/dwh_prod`
    * `dw_variablen_home` = `/home/airflow_prod`
    * `dw_variablen_pms_home` = `/opt/pms_prod`
* **Action**: Execute `DynamicPathResolver.get_paths()`.
* **Pass/Fail Criterion**: The returned dictionary must exactly match:
  ```python
  {
      "DWH_HOME": "/opt/dwh_prod",
      "HOME": "/home/airflow_prod",
      "PMS_HOME": "/opt/pms_prod",
  }
  ```
  No exceptions must be raised.

### Test Case 1.2: Missing Variable Error Handling (Strict Parity)
* **Purpose**: Verify that if any of the required variables are missing from the environment, the resolver raises an `AirflowException` (preventing downstream tasks from running with incomplete or empty path parameters, matching UC4's strict variable resolution failure behavior).
* **Setup**:
  * Populate only a subset of the variables (e.g., set `dw_variablen_dwh_home` and `dw_variablen_home`, but leave `dw_variablen_pms_home` undefined).
* **Action**: Execute `DynamicPathResolver.get_paths()`.
* **Pass/Fail Criterion**: The execution must fail, raising an `airflow.exceptions.AirflowException` containing the error message: `"Failed to resolve path variables:"`.

---

## Section 2: Logging and Metadata Parity (`DW.LESE_LOG_VTRG`)

### Test Case 2.1: Log Message Format and Literal Compliance
* **Purpose**: Verify that the generated log message strictly preserves the legacy German literal structure (`Protokolleintrag: <task_id> innerhalb <dag_id>`) and correctly extracts metadata from the Airflow execution context.
* **Setup**:
  * Create a mock Airflow context dictionary containing:
    * `context['dag'].dag_id` = `"dw_dwh_vertrag_tarif_sync_start_js"`
    * `context['task_instance'].task_id` = `"sync_initialization_task"`
  * Mock/intercept the standard Python `logging.info` stream.
* **Action**: Execute `log_execution_details_callable(**context)`.
* **Pass/Fail Criterion**: The intercepted log output must contain the exact string:
  `"Protokolleintrag: sync_initialization_task innerhalb dw_dwh_vertrag_tarif_sync_start_js"`

---

## Section 3: Runnable Pytest Suite

This executable script uses `pytest` and standard mocking techniques to validate both components without requiring a live, running Airflow database connection.

```python
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

# Import the migrated modules under test
from path_resolver import DynamicPathResolver
from logging_helper import log_execution_details_callable


# ==============================================================================
# 1. TESTS FOR: DW.HOLE_PFAD_VTRG (path_resolver.py)
# ==============================================================================

@patch("path_resolver.Variable")
def test_path_resolver_success(mock_variable):
    """
    GIVEN: Airflow variables are fully configured.
    WHEN: DynamicPathResolver.get_paths() is executed.
    THEN: It must return a dictionary with the correct mapped paths.
    """
    # Setup mock behavior for Variable.get
    def mock_get(key, *args, **kwargs):
        mapping = {
            "dw_variablen_dwh_home": "/opt/dwh_prod",
            "dw_variablen_home": "/home/airflow_prod",
            "dw_variablen_pms_home": "/opt/pms_prod"
        }
        if key in mapping:
            return mapping[key]
        raise KeyError(f"Variable {key} does not exist")
        
    mock_variable.get.side_effect = mock_get

    # Action
    resolved_paths = DynamicPathResolver.get_paths()

    # Assertions
    assert resolved_paths["DWH_HOME"] == "/opt/dwh_prod"
    assert resolved_paths["HOME"] == "/home/airflow_prod"
    assert resolved_paths["PMS_HOME"] == "/opt/pms_prod"
    assert len(resolved_paths) == 3


@patch("path_resolver.Variable")
def test_path_resolver_missing_variable_raises_exception(mock_variable):
    """
    GIVEN: One or more Airflow variables are missing.
    WHEN: DynamicPathResolver.get_paths() is executed.
    THEN: An AirflowException must be raised to halt downstream execution.
    """
    # Setup mock behavior to simulate a missing variable
    def mock_get(key, *args, **kwargs):
        mapping = {
            "dw_variablen_dwh_home": "/opt/dwh_prod",
            # "dw_variablen_home" is missing
            "dw_variablen_pms_home": "/opt/pms_prod"
        }
        if key in mapping:
            return mapping[key]
        raise KeyError(f"Variable {key} does not exist")

    mock_variable.get.side_effect = mock_get

    # Action & Assertion
    with pytest.raises(AirflowException) as exc_info:
        DynamicPathResolver.get_paths()
    
    assert "Failed to resolve path variables" in str(exc_info.value)


# ==============================================================================
# 2. TESTS FOR: DW.LESE_LOG_VTRG (logging_helper.py)
# ==============================================================================

def test_logging_helper_literal_and_metadata_parity(caplog):
    """
    GIVEN: An Airflow task context containing DAG and Task IDs.
    WHEN: log_execution_details_callable is invoked.
    THEN: The exact German literal log message must be written to the log stream.
    """
    # Setup mock context
    mock_dag = MagicMock()
    mock_dag.dag_id = "DW.DWH_VERTRAG_TARIF_SYNC_START_JS"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "DW.HOLE_PFAD_VTRG_TASK"
    
    context = {
        "dag": mock_dag,
        "task_instance": mock_ti
    }

    # Action
    with caplog.at_level("INFO"):
        log_execution_details_callable(**context)

    # Assertions
    # Verify exact character-for-character match of the legacy German print statement
    expected_literal = "Protokolleintrag: DW.HOLE_PFAD_VTRG_TASK innerhalb DW.DWH_VERTRAG_TARIF_SYNC_START_JS"
    
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[0].message == expected_literal
```