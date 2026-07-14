# Migration Validation Test Suite: UC4 PROD INCLUDES

This document defines the migration-validation tests to prove the behavioral equivalence of the migrated Python modules (`dwh_env_resolver.py` and `dwh_monitor_callback.py`) to their legacy UC4 XML counterparts (`DW.HOLE_PFAD.xml` and `DW.LESE_LOG.xml`).

---

## SECTION 1 — DATE ARITHMETIC & OUTPUT PARITY TESTS

### Test Case 1.1: Date Calculation Parity (Standard & Leap Year Boundaries)
#### Purpose
Verify that `calculate_relative_dates` produces the exact same relative dates (`LASTMONTH_YYYYMM`, `PRELASTMONTH_YYYYMM`, `NEXTMONTH_YYYYMM`) as the legacy UC4 date functions (`SUB_PERIOD`, `SUB_DAYS`, `ADD_PERIOD`) across standard, leap-year, and year-boundary dates.

#### Setup
*   Install `pytest` and `freezegun` (or use standard `datetime` objects).
*   Import `calculate_relative_dates` from `dwh_env_resolver`.

#### Action
Execute the calculation function using a matrix of edge-case dates and assert the outputs match the mathematically proven legacy values.

```python
import pytest
from datetime import datetime
from dwh_env_resolver import calculate_relative_dates

@pytest.mark.parametrize(
    "input_date, expected_last, expected_prelast, expected_next",
    [
        # Standard mid-year date
        (datetime(2023, 6, 15), "202305", "202304", "202307"),
        # Year boundary (January)
        (datetime(2023, 1, 15), "202212", "202211", "202302"),
        # Year boundary (December)
        (datetime(2023, 12, 31), "202311", "202310", "202401"),
        # Leap year February boundary (Leap Year)
        (datetime(2020, 3, 1), "202002", "202001", "202004"),
        # Non-leap year March boundary
        (datetime(2021, 3, 1), "202102", "202101", "202104"),
        # End of month day-shifting checks
        (datetime(2023, 10, 31), "202309", "202308", "202311"),
    ]
)
def test_date_calculation_parity(input_date, expected_last, expected_prelast, expected_next):
    result = calculate_relative_dates(input_date)
    assert result["LASTMONTH_YYYYMM"] == expected_last
    assert result["PRELASTMONTH_YYYYMM"] == expected_prelast
    assert result["NEXTMONTH_YYYYMM"] == expected_next
```

#### Pass/Fail Criterion
*   **Pass:** All calculated date strings match the expected legacy values exactly.
*   **Fail:** Any calculated date string deviates from the expected output (e.g., incorrect month subtraction or year-wrap failure).

---

## SECTION 2 — ENVIRONMENT RESOLUTION & VARIABLE FALLBACKS

### Test Case 2.1: Airflow Variable Extraction & Fallbacks
#### Purpose
Verify that `get_dwh_variables` correctly extracts variables from the Airflow Metadata Database and applies the exact fallback values defined in the legacy UC4 `DW.VARIABLEN` table.

#### Setup
*   Mock the Airflow `Variable.get` method using `unittest.mock`.

#### Action
Execute `get_dwh_variables` under two scenarios:
1.  When variables are fully defined in the environment.
2.  When variables are missing (testing fallback defaults).

```python
from unittest.mock import patch
from dwh_env_resolver import get_dwh_variables

def test_get_dwh_variables_with_values():
    mock_vars = {
        "DWH_HOME": "/custom/opt/dwh",
        "HOME": "/custom/home/dwh",
        "KWS_HOME": "/kws",
        "PMS_HOME": "/pms",
        "ISTNS_HOME": "/istns",
        "AKTIV_CARMEN": "1",
        "AKTIV_CRS": "1",
        "AKTIV_CTEL": "1",
        "AKTIV_DPPS": "1",
        "AKTIV_KDS": "1",
        "AKTIV_WUERFEL": "1",
        "AKTIV_XTRA": "1",
        "AKTUELL_CACHE": "cache_val"
    }
    
    def mock_get(key, default_value=None):
        return mock_vars.get(key, default_value)

    with patch("dwh_env_resolver.Variable.get", side_effect=mock_get):
        resolved = get_dwh_variables()
        for key, val in mock_vars.items():
            assert resolved[key] == val

def test_get_dwh_variables_fallbacks():
    # When Variable.get returns the default_value parameter
    def mock_get_default(key, default_value=None):
        return default_value

    with patch("dwh_env_resolver.Variable.get", side_effect=mock_get_default):
        resolved = get_dwh_variables()
        assert resolved["DWH_HOME"] == "/opt/dwh"
        assert resolved["HOME"] == "/home/dwh"
        assert resolved["KWS_HOME"] == ""
        assert resolved["AKTIV_CARMEN"] == "0"
```

#### Pass/Fail Criterion
*   **Pass:** The function successfully retrieves custom values when present and falls back to the exact legacy defaults when absent.
*   **Fail:** Any fallback value or key name differs from the legacy specification.

---

## SECTION 3 — CONSOLE LOGGING & VERBATIM OUTPUT PARITY

### Test Case 3.1: Verbatim Console Output & Exit Status Simulation
#### Purpose
Verify that `resolve_lese_log_behavior` outputs the exact, character-for-character German console messages and asterisks as the legacy `DW.LESE_LOG.xml` script for both success and failure states.

#### Setup
*   Mock the standard output stream (`sys.stdout`) to capture printed lines.
*   Mock the Airflow Task Instance (`ti`), DAG, and context dictionary.

#### Action
Call `resolve_lese_log_behavior` with:
1.  A context containing no exception (Success Flow).
2.  A context containing an exception (Failure Flow).

```python
import io
import sys
from unittest.mock import MagicMock
from dwh_monitor_callback import resolve_lese_log_behavior

def test_lese_log_success_output():
    # Setup mock context for success
    mock_ti = MagicMock()
    mock_ti.task_id = "test_task"
    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag"
    
    context = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "exception": None
    }
    
    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    try:
        resolve_lese_log_behavior(context)
    finally:
        sys.stdout = sys.__stdout__
        
    output = captured_output.getvalue()
    
    # Assert exact legacy format
    expected_success = (
        "****************************************************************\n"
        "Rueckgabewert: '0' ***************************************\n"
        "****************************************************************\n"
    )
    assert expected_success in output

def test_lese_log_failure_output():
    # Setup mock context for failure
    mock_ti = MagicMock()
    mock_ti.task_id = "test_task"
    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag"
    
    context = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "exception": Exception("Task Failed")
    }
    
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    try:
        resolve_lese_log_behavior(context)
    finally:
        sys.stdout = sys.__stdout__
        
    output = captured_output.getvalue()
    
    # Assert exact legacy format and showlog call
    assert "Executing: /home/dwh/tools/showlog -uc4 test_dag.test_task" in output
    expected_failure = (
        "****************************************************************\n"
        "Rueckgabewert: '1' (Fehlerfall)***************************\n"
        "****************************************************************\n"
    )
    assert expected_failure in output
```

#### Pass/Fail Criterion
*   **Pass:** The printed output matches the legacy console output character-for-character, including the exact number of asterisks and German text.
*   **Fail:** Any mismatch in the string format, spacing, or text content occurs.

---

## SECTION 4 — INTEGRATION & PIPELINE STATE ASSERTIONS

### Test Case 4.1: Airflow XCom Push and Callback Integration
#### Purpose
Verify that the `resolve_hole_pfad_context` task successfully pushes calculated values to Airflow's XCom system, making them available for downstream tasks.

#### Setup
*   Create a mock Airflow Task Instance (`ti`) with an active XCom push tracking dictionary.

#### Action
Execute `resolve_hole_pfad_context` with the mock context and verify the keys and values pushed to XCom.

```python
from unittest.mock import MagicMock, patch
from datetime import datetime
from dwh_env_resolver import resolve_hole_pfad_context

def test_xcom_push_integration():
    mock_ti = MagicMock()
    pushed_xcoms = {}
    
    def mock_xcom_push(key, value):
        pushed_xcoms[key] = value
        
    mock_ti.xcom_push.side_effect = mock_xcom_push
    mock_dag = MagicMock()
    mock_dag.dag_id = "test_integration_dag"
    
    context = {
        "ti": mock_ti,
        "logical_date": datetime(2023, 6, 15),
        "dag": mock_dag
    }
    
    # Mock Variable.get to avoid DB calls
    with patch("dwh_env_resolver.Variable.get", return_value="mock_val"):
        resolve_hole_pfad_context(**context)
        
    # Verify XCom payloads exist and have correct structures
    assert "dwh_paths" in pushed_xcoms
    assert "dwh_dates" in pushed_xcoms
    assert pushed_xcoms["dwh_dates"]["LASTMONTH_YYYYMM"] == "202305"
    assert pushed_xcoms["dwh_paths"]["DWH_HOME"] == "mock_val"
```

#### Pass/Fail Criterion
*   **Pass:** Both `dwh_paths` and `dwh_dates` are pushed to XCom with correct, verified structures.
*   **Fail:** XCom pushes are missing, or contain incorrect keys/values.