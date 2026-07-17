Here is the comprehensive migration-validation test suite for the migrated `uc4_helpers.py` module. 

Since these includes are structural utility scripts rather than standalone data pipelines, the validation strategy focuses on **functional equivalence, context extraction accuracy, fallback robustness, and strict compliance with legacy logging patterns**.

---

# MIGRATION VALIDATION TEST SUITE
**Target Module:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/uc4_helpers.py`  
**Source Components:** `DW.HOLE_PFAD_VTRG` (JOBI), `DW.LESE_LOG_VTRG` (JOBI)

---

## SECTION 1 — UNIT & FUNCTIONAL EQUIVALENCE TESTS

### Test Case 1.1: Path Resolution via Unified JSON Variable (`dw_variablen`)
#### Purpose
Verify that `hole_pfad_vtrg()` correctly parses and extracts the environment paths (`DWH_HOME`, `HOME`, `PMS_HOME`) when they are stored in the unified JSON Airflow Variable `dw_variablen`.

#### Setup
*   Mock the Airflow `Variable.get` method to return a valid JSON string when queried with the key `"dw_variablen"`.
*   Ensure individual variable lookups are not triggered if the JSON block contains all keys.

#### Action
Execute `hole_pfad_vtrg()` and capture the returned dictionary.

#### Pass/Fail Criterion
*   **Pass:** The returned dictionary matches the mock JSON values exactly:
    *   `DWH_HOME` == `/opt/dwh/prod`
    *   `HOME` == `/home/airflow`
    *   `PMS_HOME` == `/opt/pms/prod`
*   **Fail:** Any path is returned as `None`, or an exception is raised during parsing.

```python
import pytest
from unittest.mock import MagicMock, patch

@patch("airflow.models.Variable.get")
def test_hole_pfad_vtrg_from_json(mock_variable_get):
    # Setup mock JSON container
    mock_json_data = {
        "DWH_HOME": "/opt/dwh/prod",
        "HOME": "/home/airflow",
        "PMS_HOME": "/opt/pms/prod"
    }
    
    def side_effect(key, *args, **kwargs):
        if key == "dw_variablen":
            if kwargs.get("deserialize_json"):
                return mock_json_data
            return '{"DWH_HOME": "/opt/dwh/prod", "HOME": "/home/airflow", "PMS_HOME": "/opt/pms/prod"}'
        raise ValueError(f"Unexpected key lookup: {key}")

    mock_variable_get.side_effect = side_effect

    # Action
    from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.uc4_helpers import hole_pfad_vtrg
    result = hole_pfad_vtrg()

    # Assertions
    assert result["DWH_HOME"] == "/opt/dwh/prod"
    assert result["HOME"] == "/home/airflow"
    assert result["PMS_HOME"] == "/opt/pms/prod"
```

---

### Test Case 1.2: Path Resolution via Individual Fallback Variables
#### Purpose
Verify that if the unified JSON variable `dw_variablen` does not exist or is corrupted, `hole_pfad_vtrg()` gracefully falls back to querying individual Airflow Variables (`dwh_home`, `home`, `pms_home`).

#### Setup
*   Mock `Variable.get` for `"dw_variablen"` to raise an exception (simulating a missing variable).
*   Mock individual lookups for `"dwh_home"`, `"home"`, and `"pms_home"` to return valid string paths.

#### Action
Execute `hole_pfad_vtrg()` and capture the returned dictionary.

#### Pass/Fail Criterion
*   **Pass:** The function catches the JSON retrieval exception, proceeds to individual lookups, and returns the correct fallback paths.
*   **Fail:** The function raises an unhandled exception or fails to resolve the fallback variables.

```python
@patch("airflow.models.Variable.get")
def test_hole_pfad_vtrg_fallback_individual(mock_variable_get):
    # Setup side effect: JSON lookup fails, individual lookups succeed
    def side_effect(key, *args, **kwargs):
        if key == "dw_variablen":
            raise KeyError("Variable not found")
        elif key == "dwh_home":
            return "/opt/fallback/dwh"
        elif key == "home":
            return "/opt/fallback/home"
        elif key == "pms_home":
            return "/opt/fallback/pms"
        return kwargs.get("default_var")

    mock_variable_get.side_effect = side_effect

    # Action
    from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.uc4_helpers import hole_pfad_vtrg
    result = hole_pfad_vtrg()

    # Assertions
    assert result["DWH_HOME"] == "/opt/fallback/dwh"
    assert result["HOME"] == "/opt/fallback/home"
    assert result["PMS_HOME"] == "/opt/fallback/pms"
```

---

## SECTION 2 — COMPLIANCE & OUTPUT PARITY TESTS

### Test Case 2.1: Exact Log Output Matching (German Literal Rule)
#### Purpose
Verify that `lese_log_vtrg()` outputs the exact character-for-character German log string matching the legacy UC4 print statement: `"Protokolleintrag: &ADMJOB innerhalb &ADMJP"`.

#### Setup
*   Construct a mock Airflow execution context dictionary containing a mock DAG object and a mock Task Instance object.
*   Redirect standard output (`sys.stdout`) to capture the printed string.

#### Action
Execute `lese_log_vtrg(context)` with the mock context.

#### Pass/Fail Criterion
*   **Pass:** The captured standard output contains the exact string: `Protokolleintrag: test_task_id innerhalb test_dag_id\n`.
*   **Fail:** The output string is modified, translated, or missing the exact German structure.

```python
import sys
from io import StringIO

def test_lese_log_vtrg_exact_output_parity():
    # Setup mock Airflow context
    class MockDAG:
        dag_id = "test_dag_id"

    class MockTaskInstance:
        task_id = "test_task_id"

    mock_context = {
        "dag": MockDAG(),
        "task_instance": MockTaskInstance()
    }

    # Redirect stdout to capture print statements
    captured_output = StringIO()
    sys.stdout = captured_output

    try:
        # Action
        from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.uc4_helpers import lese_log_vtrg
        lese_log_vtrg(mock_context)
    finally:
        # Reset redirect
        sys.stdout = sys.__stdout__

    # Assertions
    expected_output = "Protokolleintrag: test_task_id innerhalb test_dag_id\n"
    assert captured_output.getvalue() == expected_output
```

---

### Test Case 2.2: Context Extraction Robustness & Fallback
#### Purpose
Verify that `lese_log_vtrg()` does not crash if executed with an incomplete or malformed Airflow context dictionary, and instead falls back to safe default values (`UNKNOWN_DAG`, `UNKNOWN_TASK`).

#### Setup
*   Construct an empty context dictionary `{}`.
*   Redirect standard output to capture the printed string.

#### Action
Execute `lese_log_vtrg({})`.

#### Pass/Fail Criterion
*   **Pass:** The function executes without raising a `KeyError` and prints: `Protokolleintrag: UNKNOWN_TASK innerhalb UNKNOWN_DAG\n`.
*   **Fail:** The function crashes with an unhandled exception.

```python
def test_lese_log_vtrg_missing_context_fallback():
    # Setup empty context
    malformed_context = {}

    # Redirect stdout
    captured_output = StringIO()
    sys.stdout = captured_output

    try:
        # Action
        from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.uc4_helpers import lese_log_vtrg
        lese_log_vtrg(malformed_context)
    finally:
        sys.stdout = sys.__stdout__

    # Assertions
    expected_output = "Protokolleintrag: UNKNOWN_TASK innerhalb UNKNOWN_DAG\n"
    assert captured_output.getvalue() == expected_output
```

---

## SECTION 3 — INTEGRATION & AIRFLOW ENVIRONMENT TESTS

### Test Case 3.1: Airflow Variable Schema Validation
#### Purpose
Ensure that the target Cloud Composer environment has the required variables registered with valid schemas (non-empty strings or valid JSON structures) to prevent runtime failures during downstream DAG execution.

#### Setup
*   This test runs against a live/staging Airflow Metadata Database or a running Composer environment.
*   Import the Airflow `Variable` model.

#### Action
Query the Airflow metadata database for the required variables.

#### Pass/Fail Criterion
*   **Pass:** Either the unified JSON variable `dw_variablen` exists and contains the keys `DWH_HOME`, `HOME`, and `PMS_HOME`, **OR** the individual variables `dwh_home`, `home`, and `pms_home` exist and are non-empty.
*   **Fail:** Neither configuration method is populated, which will cause downstream tasks to resolve paths to `None`.

```python
import json
from airflow.models import Variable

def test_airflow_variables_exist_in_env():
    """
    Integration test to run inside the target Airflow environment
    to verify configuration readiness.
    """
    # Attempt Method A: Unified JSON
    dw_vars_raw = Variable.get("dw_variablen", default_var=None)
    if dw_vars_raw:
        try:
            dw_vars = json.loads(dw_vars_raw)
            assert "DWH_HOME" in dw_vars and dw_vars["DWH_HOME"] is not None
            assert "HOME" in dw_vars and dw_vars["HOME"] is not None
            assert "PMS_HOME" in dw_vars and dw_vars["PMS_HOME"] is not None
            return  # Pass
        except json.JSONDecodeError:
            pytest.fail("Variable 'dw_variablen' is present but is not valid JSON.")

    # Attempt Method B: Individual Fallbacks
    dwh_home = Variable.get("dwh_home", default_var=None)
    home = Variable.get("home", default_var=None)
    pms_home = Variable.get("pms_home", default_var=None)

    assert dwh_home is not None, "DWH_HOME path variable is unconfigured in Airflow."
    assert home is not None, "HOME path variable is unconfigured in Airflow."
    assert pms_home is not None, "PMS_HOME path variable is unconfigured in Airflow."
```