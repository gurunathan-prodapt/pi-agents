Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow DAG and its utility module (`dw_job_helper.py`) are behaviorally equivalent to the legacy UC4 include scripts (`DW.HOLE_PFAD` and `DW.LESE_LOG`).

---

# SECTION 1 — DATE MATH & OUTPUT PARITY VALIDATION

## Test Case 1.1: Dynamic Date Math Boundary Verification
### Purpose
To verify that the Python-based date arithmetic in `calculate_dwh_variables` produces the exact same string outputs (`YYYYMM`) as the legacy UC4 script across standard dates, leap years, year boundaries, and month-end transitions.

### Setup
* A test suite running in a Python environment with `pytest` and `freezegun` (or standard datetime mocking).
* The migrated utility module `dw_job_helper.py` imported into the test context.

### Action
Execute `calculate_dwh_variables` with a matrix of critical logical dates representing edge cases:
1. **Standard Date**: `2026-03-29`
2. **Year Boundary (January)**: `2026-01-15`
3. **Leap Year (February/March transition)**: `2024-03-01`
4. **Leap Year Day**: `2024-02-29`
5. **Month End**: `2026-10-31`

### Code Assertion
```python
import pytest
from datetime import datetime
from dw_job_helper import calculate_dwh_variables

@pytest.mark.parametrize(
    "logical_date, expected_prelast, expected_last, expected_next",
    [
        # 1. Standard Date
        # Legacy: 
        #   &LASTMONTH_YYYYMM = 20260301 -> SUB_DAYS(1) -> 20260228 -> 202602
        #   &PRELASTMONTH_YYYYMM = SUB_PERIOD(20260301, MM:2) -> 20260101 -> 202601
        #   &NEXTMONTH_YYYYMM = ADD_PERIOD(20260329, MM:1) -> 20260429 -> 202604
        ("2026-03-29", "202601", "202602", "202604"),
        
        # 2. Year Boundary (January)
        #   &LASTMONTH_YYYYMM -> 20260101 -> SUB_DAYS(1) -> 20251231 -> 202512
        #   &PRELASTMONTH_YYYYMM -> SUB_PERIOD(20260101, MM:2) -> 20251101 -> 202511
        #   &NEXTMONTH_YYYYMM -> ADD_PERIOD(20260115, MM:1) -> 20260215 -> 202602
        ("2026-01-15", "202511", "202512", "202602"),
        
        # 3. Leap Year (March 1st)
        #   &LASTMONTH_YYYYMM -> 20240301 -> SUB_DAYS(1) -> 20240229 -> 202402
        #   &PRELASTMONTH_YYYYMM -> SUB_PERIOD(20240301, MM:2) -> 20240101 -> 202401
        #   &NEXTMONTH_YYYYMM -> ADD_PERIOD(20240301, MM:1) -> 20240401 -> 202404
        ("2024-03-01", "202401", "202402", "202404"),
        
        # 4. Leap Year Day (February 29th)
        #   &LASTMONTH_YYYYMM -> 20240201 -> SUB_DAYS(1) -> 20240131 -> 202401
        #   &PRELASTMONTH_YYYYMM -> SUB_PERIOD(20240201, MM:2) -> 20231201 -> 202312
        #   &NEXTMONTH_YYYYMM -> ADD_PERIOD(20240229, MM:1) -> 20240329 -> 202403
        ("2024-02-29", "202312", "202401", "202403"),
        
        # 5. Month End (October 31st)
        #   &LASTMONTH_YYYYMM -> 20261001 -> SUB_DAYS(1) -> 20260930 -> 202609
        #   &PRELASTMONTH_YYYYMM -> SUB_PERIOD(20261001, MM:2) -> 20260801 -> 202608
        #   &NEXTMONTH_YYYYMM -> ADD_PERIOD(20261031, MM:1) -> 20261130 -> 202611
        ("2026-10-31", "202608", "202609", "202611"),
    ]
)
def test_date_math_parity(logical_date, expected_prelast, expected_last, expected_next):
    variables = calculate_dwh_variables(logical_date)
    
    assert variables["PRELASTMONTH_YYYYMM"] == expected_prelast, f"Failed PRELASTMONTH for {logical_date}"
    assert variables["LASTMONTH_YYYYMM"] == expected_last, f"Failed LASTMONTH for {logical_date}"
    assert variables["NEXTMONTH_YYYYMM"] == expected_next, f"Failed NEXTMONTH for {logical_date}"
```

### Pass/Fail Criterion
* **Pass**: All calculated date variables match the expected legacy values exactly for all test cases.
* **Fail**: Any calculated date string deviates from the expected `YYYYMM` format or value.

---

# SECTION 2 — TRANSFORMATION & EXCEPTION HANDLING VALIDATION

## Test Case 2.1: Airflow Variable Resolution and Fallback Defaults
### Purpose
To verify that `calculate_dwh_variables` correctly retrieves values from the Airflow Variable store and gracefully falls back to specified defaults when variables are missing.

### Setup
* A test environment with a mocked Airflow Metadata Database or mocked `Variable.get` method.

### Action
1. **Scenario A (Variables Present)**: Populate the Airflow Variable store with custom values for all keys.
2. **Scenario B (Variables Missing)**: Clear the Airflow Variable store to trigger default fallbacks.

### Code Assertion
```python
from unittest.mock import patch
from dw_job_helper import calculate_dwh_variables

def test_variable_resolution_with_values():
    mock_vars = {
        "dwh_home": "/custom/dwh",
        "home": "/custom/home",
        "kws_home": "/custom/kws",
        "aktiv_carmen": "1",
        "aktiv_crs": "0",
        "aktuell_cache": "cache_v2"
    }
    
    def mock_get(key, default_var=None):
        return mock_vars.get(key, default_var)
        
    with patch("dw_job_helper.Variable.get", side_effect=mock_get):
        variables = calculate_dwh_variables("2026-03-29")
        
        assert variables["DWH_HOME"] == "/custom/dwh"
        assert variables["HOME"] == "/custom/home"
        assert variables["KWS_HOME"] == "/custom/kws"
        assert variables["AKTIV_CARMEN"] == "1"
        assert variables["AKTIV_CRS"] == "0"
        assert variables["AKTUELL_CACHE"] == "cache_v2"

def test_variable_resolution_defaults():
    # Mock Variable.get to return the default_var argument when called
    def mock_get(key, default_var=None):
        return default_var
        
    with patch("dw_job_helper.Variable.get", side_effect=mock_get):
        variables = calculate_dwh_variables("2026-03-29")
        
        assert variables["DWH_HOME"] == "/home/dwh"
        assert variables["HOME"] == "/home"
        assert variables["KWS_HOME"] is None
        assert variables["AKTIV_CARMEN"] == "0"
        assert variables["AKTUELL_CACHE"] is None
```

### Pass/Fail Criterion
* **Pass**: Custom values are returned when present; default values are returned when variables are absent.
* **Fail**: A missing variable raises a `KeyError` or returns an unexpected default.

---

## Test Case 2.2: Invalid Input Date Handling
### Purpose
To verify that the utility module rejects malformed or invalid date strings with a clear error and does not proceed with incorrect calculations.

### Setup
* A test environment running `pytest`.

### Action
Call `calculate_dwh_variables` with invalid date formats:
1. `2026/03/29` (Incorrect separator)
2. `29-03-2026` (Incorrect ordering)
3. `not-a-date` (Alphanumeric string)

### Code Assertion
```python
import pytest
from dw_job_helper import calculate_dwh_variables

def test_invalid_date_formats():
    invalid_dates = ["2026/03/29", "29-03-2026", "not-a-date", "2026-02-30"]
    
    for invalid_date in invalid_dates:
        with pytest.raises(ValueError) as exc_info:
            calculate_dwh_variables(invalid_date)
        assert "Expected 'YYYY-MM-DD'" in str(exc_info.value) or "does not match format" in str(exc_info.value)
```

### Pass/Fail Criterion
* **Pass**: Every invalid date string raises a `ValueError`.
* **Fail**: The function attempts to parse the invalid date or returns incorrect date calculations.

---

# SECTION 3 — LOGGING & STATUS EVALUATION VALIDATION

## Test Case 3.1: Post-Execution Status Evaluation (`DW.LESE_LOG` Parity)
### Purpose
To verify that `evaluate_job_status` behaves identically to the legacy `DW.LESE_LOG` script by outputting the exact expected log structures and exiting with the correct return codes.

### Setup
* A test environment capturing standard output/error and logging streams.

### Action
1. **Scenario A (Success)**: Call `evaluate_job_status(0, "TEST_JOB_SUCCESS")`.
2. **Scenario B (Failure)**: Call `evaluate_job_status(127, "TEST_JOB_FAILURE")`.

### Code Assertion
```python
import pytest
import logging
from unittest.mock import patch
from dw_job_helper import evaluate_job_status

def test_evaluate_job_status_success(caplog):
    caplog.set_level(logging.INFO)
    
    with pytest.raises(SystemExit) as exc_info:
        evaluate_job_status(0, "TEST_JOB_SUCCESS")
        
    # Assert exit code is 0
    assert exc_info.value.code == 0
    
    # Assert exact legacy log formatting is preserved
    assert "****************************************************************" in caplog.text
    assert "Rueckgabewert: '0' ***************************************" in caplog.text

def test_evaluate_job_status_failure(caplog):
    caplog.set_level(logging.ERROR)
    
    with pytest.raises(SystemExit) as exc_info:
        evaluate_job_status(127, "TEST_JOB_FAILURE")
        
    # Assert exit code matches the input return code
    assert exc_info.value.code == 127
    
    # Assert exact legacy log formatting is preserved
    assert "Airflow Task Execution Failure detected for Job Scope: TEST_JOB_FAILURE" in caplog.text
    assert "****************************************************************" in caplog.text
    assert "Rueckgabewert: '127' (Fehlerfall)***************************" in caplog.text
```

### Pass/Fail Criterion
* **Pass**: 
  * Success (0) exits with code `0` and prints the success banner.
  * Failure (non-zero) exits with the original non-zero code and prints the error banner.
* **Fail**: The system exits with an incorrect code, or the log output does not match the legacy format.

---

# SECTION 4 — PIPELINE INTEGRATION & END-TO-END VALIDATION

## Test Case 4.1: DAG Execution and XCom Variable Propagation
### Purpose
To verify that the Airflow DAG `dw_sap_gutschriften_taeglich_jp` correctly orchestrates the tasks, runs the date calculations, pushes them to XCom, and pulls them in downstream tasks.

### Setup
* A local Airflow integration environment (e.g., `db init` completed on a local SQLite instance).

### Action
1. Trigger a local execution of the DAG `dw_sap_gutschriften_taeglich_jp` for the logical date `2026-03-29`.
2. Extract the XCom values pushed by the task `initialize_legacy_paths`.
3. Verify task execution states.

### Code Assertion
```python
from datetime import datetime
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
import pytest

def test_dag_xcom_propagation():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag("dw_sap_gutschriften_taeglich_jp")
    
    assert dag is not None
    assert len(dag.tasks) == 2
    
    # Create a test DagRun
    execution_date = datetime(2026, 3, 29)
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_id="test_run_1",
        run_type=DagRunType.MANUAL
    )
    
    # Run Task 1: initialize_legacy_paths
    ti_init = TaskInstance(task=dag.get_task("initialize_legacy_paths"), run_id=dag_run.run_id)
    ti_init.run(ignore_ti_check=True)
    assert ti_init.state == TaskInstanceState.SUCCESS
    
    # Verify XCom values pushed to the DB
    assert ti_init.xcom_pull(key="LASTMONTH_YYYYMM") == "202602"
    assert ti_init.xcom_pull(key="PRELASTMONTH_YYYYMM") == "202601"
    assert ti_init.xcom_pull(key="NEXTMONTH_YYYYMM") == "202604"
    assert ti_init.xcom_pull(key="DWH_HOME") == "/home/dwh" # Default fallback
    
    # Run Task 2: execute_workload
    ti_workload = TaskInstance(task=dag.get_task("execute_workload"), run_id=dag_run.run_id)
    ti_workload.run(ignore_ti_check=True)
    assert ti_workload.state == TaskInstanceState.SUCCESS
```

### Pass/Fail Criterion
* **Pass**: Both tasks execute successfully, variables are pushed to XCom with correct values, and the downstream task consumes them without error.
* **Fail**: Any task fails, or XCom values are missing or incorrect.