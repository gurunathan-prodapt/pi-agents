Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Python helper module (`dwh_uc4_helpers.py`) is behaviorally equivalent to the legacy UC4 include scripts (`DW.HOLE_PFAD` and `DW.LESE_LOG`).

---

# Test Suite: Shared Files — `dwh_uc4_helpers.py` Validation

## Section 1: Output Parity & Date Arithmetic Validation

### Test Case 1.1: Date Arithmetic Boundary & Leap Year Parity
#### Purpose
Verify that the Python-based date arithmetic logic in `resolve_dwh_variables` produces the exact same string values (`YYYYMM`) for `PRELASTMONTH_YYYYMM`, `LASTMONTH_YYYYMM`, and `NEXTMONTH_YYYYMM` as the legacy UC4 date functions across critical calendar boundaries (leap years, year transitions, and month ends).

#### Setup
*   Install `pytest` and `freezegun` (or mock the execution context).
*   Prepare a matrix of test dates representing edge cases:
    *   **Year Transition:** `2023-01-15` (Should transition back to previous year for last/prelast months).
    *   **Leap Year February:** `2024-02-29` (Should handle leap day correctly).
    *   **Leap Year March Transition:** `2024-03-15` (Verifies subtraction across leap month).
    *   **Standard Month End:** `2023-10-31`.

#### Action
Run the following unit test suite using `pytest`:

```python
import pytest
from datetime import datetime
# Import the migrated helper function
from dwh_uc4_helpers import resolve_dwh_variables

@pytest.mark.parametrize(
    "logical_date_str, expected_prelast, expected_last, expected_next",
    [
        # 1. Standard Year Transition
        ("2023-01-15T00:00:00+00:00", "202211", "202212", "202302"),
        # 2. Leap Year February
        ("2024-02-29T12:30:00+00:00", "202312", "202401", "202403"),
        # 3. Leap Year March (checks if subtracting 1 day from March 1st yields Feb 29)
        ("2024-03-15T08:00:00+00:00", "202401", "202402", "202404"),
        # 4. Standard Month End
        ("2023-10-31T23:59:59+00:00", "202308", "202309", "202311"),
        # 5. First Day of Year
        ("2023-01-01T00:00:00+00:00", "202211", "202212", "202302"),
    ]
)
def test_date_arithmetic_parity(logical_date_str, expected_prelast, expected_last, expected_next):
    """
    Asserts that Python date calculations match legacy UC4 logic:
      - PRELASTMONTH_YYYYMM = (First of current month) - 2 months
      - LASTMONTH_YYYYMM = (First of current month) - 1 day
      - NEXTMONTH_YYYYMM = (Logical date) + 1 month
    """
    resolved = resolve_dwh_variables(logical_date_str)
    
    assert resolved["PRELASTMONTH_YYYYMM"] == expected_prelast, \
        f"Failed PRELAST: Expected {expected_prelast}, got {resolved['PRELASTMONTH_YYYYMM']} for {logical_date_str}"
        
    assert resolved["LASTMONTH_YYYYMM"] == expected_last, \
        f"Failed LAST: Expected {expected_last}, got {resolved['LASTMONTH_YYYYMM']} for {logical_date_str}"
        
    assert resolved["NEXTMONTH_YYYYMM"] == expected_next, \
        f"Failed NEXT: Expected {expected_next}, got {resolved['NEXTMONTH_YYYYMM']} for {logical_date_str}"
```

#### Pass/Fail Criterion
*   **Pass:** All 5 date scenarios calculate the exact 6-character string values matching legacy UC4 expectations.
*   **Fail:** Any calculated string deviates by even one digit or fails to parse due to timezone/microsecond offsets.

---

## Section 2: Transformation Correctness & Fallback Handling

### Test Case 2.1: Airflow Variable Resolution & Fallback Robustness
#### Purpose
Ensure that `resolve_dwh_variables` correctly retrieves environment variables from the Airflow Metadata Store when they exist, and falls back to the exact legacy defaults specified in the design document when they are missing.

#### Setup
*   Mock the Airflow `Variable.get` interface using `unittest.mock`.

#### Action
Run the following test to verify both custom configurations and fallback behaviors:

```python
from unittest.mock import patch
from dwh_uc4_helpers import resolve_dwh_variables

@patch("dwh_uc4_helpers.Variable.get")
def test_variable_resolution_and_fallbacks(mock_variable_get):
    # Define side-effects for Variable.get:
    # Simulate some variables being customized in Airflow, and others missing (raising KeyError or returning default)
    def variable_side_effect(key, default_var=None):
        custom_vars = {
            "dwh_home": "/custom/opt/dwh",
            "aktiv_carmen": "0",
            "aktuell_cache": "9"
        }
        return custom_vars.get(key, default_var)
        
    mock_variable_get.side_effect = variable_side_effect
    
    # Execute resolution
    resolved = resolve_dwh_variables("2023-06-15T00:00:00")
    
    # Assert custom values are respected
    assert resolved["DWH_HOME"] == "/custom/opt/dwh"
    assert resolved["AKTIV_CARMEN"] == "0"
    assert resolved["AKTUELL_CACHE"] == "9"
    
    # Assert fallbacks are correctly applied for unconfigured variables
    assert resolved["HOME"] == "/home/airflow"
    assert resolved["KWS_HOME"] == "/opt/kws"
    assert resolved["AKTIV_XTRA"] == "1"
```

#### Pass/Fail Criterion
*   **Pass:** Custom values are successfully injected, and all missing variables default to their exact legacy UC4 values.
*   **Fail:** A default value is modified, or a custom variable fails to override the default.

---

## Section 3: External-System Replacements & Callback Behavior

### Test Case 3.1: Failure Callback & Log Redirection (`DW.LESE_LOG` Parity)
#### Purpose
Verify that when a task fails, `dwh_on_failure_callback` intercepts the failure, prints the legacy-compatible error markers to standard output, outputs the Airflow Log URL (replacing `SHOWLOG.KSH`), and registers the `FAILED` state in the monitoring system.

#### Setup
*   Create a mock Airflow Context dictionary containing a mock Task Instance (`ti`) and an exception.
*   Capture standard output (`sys.stdout`) to verify the printed log markers.

#### Action
Run the following test execution:

```python
import sys
from io import StringIO
from unittest.mock import MagicMock, patch
from dwh_uc4_helpers import dwh_on_failure_callback

@patch("dwh_uc4_helpers.register_job_monitor_state")
def test_failure_callback_behavior(mock_register_state):
    # 1. Setup mock context
    mock_ti = MagicMock()
    mock_ti.task_id = "test_pyspark_task"
    mock_ti.log_url = "https://airflow.gcp.internal/log?dag_id=test&task_id=test_pyspark_task"
    
    context = {
        "task_instance": mock_ti,
        "run_id": "scheduled__2023-06-15T00:00:00+00:00",
        "exception": Exception("Dataproc OutOfMemoryError")
    }
    
    # 2. Capture stdout
    captured_output = StringIO()
    sys.stdout = captured_output
    
    try:
        dwh_on_failure_callback(context)
    finally:
        sys.stdout = sys.__stdout__  # Reset redirect
        
    output_str = captured_output.getvalue()
    
    # 3. Assertions
    # Verify legacy-compatible stdout markers
    assert "Rueckgabewert: '1' (Fehlerfall)" in output_str
    assert "****************************************************************" in output_str
    assert mock_ti.log_url in output_str
    assert "Dataproc OutOfMemoryError" in output_str
    
    # Verify monitoring state registration
    mock_register_state.assert_called_once_with(
        job_kennung="test_pyspark_task_scheduled__2023-06-15T00:00:00+00:00",
        status="FAILED",
        detail="Task terminated unexpectedly. Reference Exception: Dataproc OutOfMemoryError"
    )
```

#### Pass/Fail Criterion
*   **Pass:** The console output contains the exact legacy error markers, the Airflow log URL is printed, and `register_job_monitor_state` is called with status `FAILED`.
*   **Fail:** The callback raises an internal exception, fails to print the log URL, or fails to register the `FAILED` state.

---

## Section 4: Data-Quality & Integration Assertions

### Test Case 4.1: End-to-End XCom Pipeline Integration
#### Purpose
Verify that when `run_hole_pfad_task` is executed within an Airflow task context, all resolved variables are successfully pushed to XCom, making them available for downstream consumption.

#### Setup
*   Instantiate a mock Airflow Task Instance with an active XCom push tracking dictionary.

#### Action
Run the following integration test:

```python
from unittest.mock import MagicMock, patch
from dwh_uc4_helpers import run_hole_pfad_task

@patch("dwh_uc4_helpers.register_job_monitor_state")
@patch("dwh_uc4_helpers.Variable.get")
def test_run_hole_pfad_task_xcom_push(mock_variable_get, mock_register_state):
    # Mock Variable lookups to return standard defaults
    mock_variable_get.side_effect = lambda key, default_var=None: default_var
    
    # Mock Task Instance and XCom tracking
    mock_ti = MagicMock()
    pushed_xcoms = {}
    def mock_xcom_push(key, value):
        pushed_xcoms[key] = value
    mock_ti.xcom_push.side_effect = mock_xcom_push
    mock_ti.task_id = "init_task"
    
    context = {
        "ti": mock_ti,
        "task_instance": mock_ti,
        "logical_date": "2023-06-15T00:00:00+00:00",
        "run_id": "manual__2023-06-15"
    }
    
    # Execute task wrapper
    run_hole_pfad_task(**context)
    
    # Assertions
    # 1. Verify monitoring start was registered
    mock_register_state.assert_any_call(
        job_kennung="init_task_manual__2023-06-15",
        status="RUNNING",
        detail="DW.HOLE_PFAD: Initializing environment paths and date ranges."
    )
    
    # 2. Verify all 16 variables are pushed to XCom
    assert len(pushed_xcoms) == 16
    assert pushed_xcoms["DWH_HOME"] == "/opt/dwh"
    assert pushed_xcoms["PRELASTMONTH_YYYYMM"] == "202304"
    assert pushed_xcoms["LASTMONTH_YYYYMM"] == "202305"
    assert pushed_xcoms["NEXTMONTH_YYYYMM"] == "202307"
```

#### Pass/Fail Criterion
*   **Pass:** The task registers the `RUNNING` state, calculates the correct dates, and pushes all 16 environment and date variables to XCom.
*   **Fail:** Any variable is missing from the XCom payload, or the date calculations are incorrect.