# Migration Validation Test Suite: `dw_dwh_stamm_includes.py`

This document contains the migration-validation tests for the migrated UC4 include components (`DW.HOLE_PFAD_KNZB` and `DW.LESE_LOG_KNZB`) consolidated into `dags/utils/dw_dwh_stamm_includes.py`.

---

## Test Case 1: Path Resolution Output Parity & Fallback Behavior

### Purpose
Verify that `hole_pfad_knzb()` correctly retrieves path variables from the Airflow Variable store when they exist, and falls back to structured GCS paths based on `GCS_BUCKET` when they do not. This ensures behavioral equivalence to the legacy `GET_VAR` lookups against `DW.VARIABLEN`.

### Setup
*   A Python environment with `pytest` and `apache-airflow` installed.
*   The Airflow Metadata Database initialized (or mocked using standard unit testing frameworks).
*   Clear any existing Airflow Variables with keys: `GCS_BUCKET`, `dw_variablen_dwh_home`, `dw_variablen_home`, and `dw_variablen_istns_home`.

### Action
Execute a test script that:
1.  Runs `hole_pfad_knzb()` with no variables set in the database (verifying default fallback behavior).
2.  Sets custom values for the variables in the Airflow Variable store and runs `hole_pfad_knzb()` again (verifying explicit lookup behavior).

### Pass/Fail Criterion
*   **Pass:** 
    *   When no variables are set, the function returns paths pointing to the default bucket `gs://YOUR_BUCKET_NAME`.
    *   When `GCS_BUCKET` is set, the default paths dynamically update to use the configured bucket.
    *   When explicit keys (e.g., `dw_variablen_dwh_home`) are set, the function returns those exact values, overriding any default bucket logic.
*   **Fail:** Any returned path does not match the expected value, or the function raises an unhandled exception.

```python
import pytest
from unittest import mock
from airflow.models import Variable
from dags.utils.dw_dwh_stamm_includes import hole_pfad_knzb

@pytest.fixture(autouse=True)
def clear_airflow_variables():
    """Ensure a clean state for Airflow Variables before each test run."""
    for key in ["GCS_BUCKET", "dw_variablen_dwh_home", "dw_variablen_home", "dw_variablen_istns_home"]:
        try:
            Variable.delete(key)
        except KeyError:
            pass

def test_hole_pfad_knzb_default_fallbacks():
    """Verify default fallback paths when no Airflow Variables are configured."""
    resolved_paths = hole_pfad_knzb()
    
    assert resolved_paths["DWH_HOME"] == "gs://YOUR_BUCKET_NAME/dwh/home"
    assert resolved_paths["HOME"] == "gs://YOUR_BUCKET_NAME/home"
    assert resolved_paths["ISTNS_HOME"] == "gs://YOUR_BUCKET_NAME/istns_home"

def test_hole_pfad_knzb_custom_gcs_bucket():
    """Verify paths dynamically update when GCS_BUCKET is set."""
    Variable.set("GCS_BUCKET", "gs://my-custom-environment-bucket")
    
    resolved_paths = hole_pfad_knzb()
    
    assert resolved_paths["DWH_HOME"] == "gs://my-custom-environment-bucket/dwh/home"
    assert resolved_paths["HOME"] == "gs://my-custom-environment-bucket/home"
    assert resolved_paths["ISTNS_HOME"] == "gs://my-custom-environment-bucket/istns_home"

def test_hole_pfad_knzb_explicit_variables():
    """Verify explicit variable overrides take precedence over defaults."""
    Variable.set("dw_variablen_dwh_home", "gs://prod-bucket/opt/dwh")
    Variable.set("dw_variablen_home", "gs://prod-bucket/users/home")
    Variable.set("dw_variablen_istns_home", "gs://prod-bucket/opt/istns")
    
    resolved_paths = hole_pfad_knzb()
    
    assert resolved_paths["DWH_HOME"] == "gs://prod-bucket/opt/dwh"
    assert resolved_paths["HOME"] == "gs://prod-bucket/users/home"
    assert resolved_paths["ISTNS_HOME"] == "gs://prod-bucket/opt/istns"
```

---

## Test Case 2: Logging Output Parity (Literal Preservation)

### Purpose
Verify that `log_parent_context()` preserves the exact output format of the legacy UC4 script `DW.LESE_LOG_KNZB` (`"Protokolleintrag: &ADMJOB innerhalb &ADMJP"`), mapping `&ADMJOB` to the Airflow `task_id` and `&ADMJP` to the Airflow `dag_id`.

### Setup
*   A Python environment with `pytest` and `apache-airflow`.
*   A mocked Airflow `TaskInstance` and execution context dictionary.
*   A standard Python `logging` handler configured to capture log output.

### Action
Execute `log_parent_context()` passing a mocked `TaskInstance` and context dictionary, then inspect the captured log output.

### Pass/Fail Criterion
*   **Pass:** The captured logs contain the exact literal string: `Protokolleintrag: <task_id> innerhalb <dag_id>` bounded by lines of 60 equals signs (`=`).
*   **Fail:** The log message is missing, formatted incorrectly, or does not match the legacy literal structure.

```python
import logging
import pytest
from unittest.mock import MagicMock
from dags.utils.dw_dwh_stamm_includes import log_parent_context

def test_log_parent_context_output_parity(caplog):
    """Verify that the log output matches the legacy UC4 print statement exactly."""
    # Mock the Airflow TaskInstance
    mock_ti = MagicMock()
    mock_ti.dag_id = "DW_DWH_STAMM_KNZB_ABGL_START_JS"
    mock_ti.task_id = "run_pyspark_stamm"
    
    # Execute logging helper within caplog context
    with caplog.at_level(logging.INFO):
        log_parent_context(ti=mock_ti)
        
    # Extract log records
    log_messages = [record.message for record in caplog.records]
    
    # Expected outputs
    expected_boundary = "=" * 60
    expected_literal = "Protokolleintrag: run_pyspark_stamm innerhalb DW_DWH_STAMM_KNZB_ABGL_START_JS"
    
    # Assertions
    assert expected_boundary in log_messages
    assert expected_literal in log_messages
    assert log_messages.count(expected_boundary) == 2
```

---

## Test Case 3: Logging Fail-Safe Robustness

### Purpose
Verify that `log_parent_context()` is completely fail-safe. If the context is corrupted, missing, or throws an unexpected exception, the function must catch the error, log a warning, and allow the pipeline to continue execution without crashing.

### Setup
*   A Python environment with `pytest` and `apache-airflow`.
*   A corrupted context (e.g., passing `None` for both `ti` and `context` parameters to trigger an exception).

### Action
Execute `log_parent_context(None)` and verify that no exception is raised to the caller, and that a non-critical warning is logged.

### Pass/Fail Criterion
*   **Pass:** The function executes without raising any exceptions, and logs a warning containing `"Non-critical failure logging context"`.
*   **Fail:** The function raises an exception (e.g., `AttributeError`), which would crash a downstream production pipeline.

```python
import logging
import pytest
from dags.utils.dw_dwh_stamm_includes import log_parent_context

def test_log_parent_context_fail_safe(caplog):
    """Verify that the logging helper does not raise exceptions on bad input."""
    # Trigger an intentional AttributeError inside the try-block by passing None
    try:
        with caplog.at_level(logging.WARNING):
            log_parent_context(ti=None, context=None)
    except Exception as e:
        pytest.fail(f"log_parent_context raised an exception: {e}. It must be fail-safe.")
        
    # Verify that a warning was captured instead of crashing
    warnings = [record.message for record in caplog.records if record.levelno == logging.WARNING]
    assert any("Non-critical failure logging context" in w for w in warnings)
```

---

## Test Case 4: Downstream Integration & Type Assertions

### Purpose
Verify that the outputs of `hole_pfad_knzb()` are of the correct type (`Dict[str, str]`) and can be successfully injected into downstream task environments (such as Spark/Dataproc configurations).

### Setup
*   A Python environment with `pytest` and `apache-airflow`.
*   Mocked Airflow Variables representing valid GCS paths.

### Action
1.  Call `hole_pfad_knzb()`.
2.  Assert that all returned keys (`DWH_HOME`, `HOME`, `ISTNS_HOME`) are present.
3.  Assert that all returned values are strings and start with the valid GCS protocol prefix (`gs://`).

### Pass/Fail Criterion
*   **Pass:** The returned object is a dictionary, contains all three keys, and all values are strings starting with `gs://`.
*   **Fail:** The returned object is missing keys, contains non-string types, or contains paths that do not conform to GCS URI standards.

```python
import pytest
from airflow.models import Variable
from dags.utils.dw_dwh_stamm_includes import hole_pfad_knzb

def test_downstream_compatibility_and_types():
    """Verify that the returned paths are structurally valid for GCS integration."""
    Variable.set("GCS_BUCKET", "gs://dwh-prod-bucket")
    
    paths = hole_pfad_knzb()
    
    # Type Assertions
    assert isinstance(paths, dict)
    assert len(paths) == 3
    
    for key in ["DWH_HOME", "HOME", "ISTNS_HOME"]:
        assert key in paths
        assert isinstance(paths[key], str)
        assert paths[key].startswith("gs://")
        assert not paths[key].endswith("/")  # Ensure no trailing slashes to prevent double-slash bugs downstream