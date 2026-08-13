# Migration Validation Test Suite: `gestern` Utility

This document defines the migration-validation tests to prove behavioral equivalence between the legacy Korn Shell script (`gestern.ksh`) and the migrated Python script (`gestern.py`).

---

## Testing Strategy & Environment Setup

The `gestern` utility calculates "today's" and "yesterday's" dates based on the system clock. To validate equivalence across critical calendar boundaries (month ends, leap years, year transitions) without altering the physical system clock, the test suite uses **differential testing with environment mocking**:
1. **KSH Mocking**: We mock the system `date` command by creating a temporary executable named `date` in a directory prepended to the `PATH` during execution.
2. **Python Mocking**: We mock Python's native `datetime.date.today` using the `unittest.mock` library.

### Test Directory Structure
```text
tests/
├── conftest.py            # Shared fixtures for mocking and execution
└── test_gestern.py        # Pytest test cases
```

---

## Test Cases

### 1. Standard Mid-Month Date Parity
* **Purpose**: Verify that on a standard day with no boundary transitions, both scripts produce identical space-separated outputs.
* **Setup**: Mock the system date to `2023-10-15` (Sunday).
* **Action**: Execute both `gestern.ksh` and `gestern.py` and capture stdout.
* **Pass/Fail Criterion**: Both outputs must match exactly: `20231015 20231014 202310 202310`.

### 2. Month Boundary Transition (Standard Month)
* **Purpose**: Verify correct rollback to the last day of the previous month.
* **Setup**: Mock the system date to `2023-05-01` (May Day).
* **Action**: Execute both scripts and capture stdout.
* **Pass/Fail Criterion**: Both outputs must match exactly: `20230501 20230430 202305 202304`.

### 3. Year Boundary Transition
* **Purpose**: Verify correct rollback across the New Year boundary (January 1st to December 31st of the previous year).
* **Setup**: Mock the system date to `2024-01-01`.
* **Action**: Execute both scripts and capture stdout.
* **Pass/Fail Criterion**: Both outputs must match exactly: `20240101 20231231 202401 202312`.

### 4. Leap Year Transition (Standard Leap Year)
* **Purpose**: Verify that March 1st in a leap year correctly rolls back to February 29th.
* **Setup**: Mock the system date to `2024-03-01` (2024 is a leap year).
* **Action**: Execute both scripts and capture stdout.
* **Pass/Fail Criterion**: Both outputs must match exactly: `20240301 20240229 202403 202402`.

### 5. Leap Year Boundary Discrepancy (Year 2000 - Legacy Bug Validation)
* **Purpose**: Validate behavior on the century leap year (Year 2000). 
* **Note on Legacy Bug**: The legacy KSH script contains a bug in its leap-year calculation:
  `(( expr $Var_Nummer_Heute_Jahr % 4 == 0 && expr $Var_Nummer_Heute_Jahr % 100 > 0 ))`
  Because `2000 % 100` is `0`, the legacy script incorrectly evaluates the year 2000 as a *non-leap year*, outputting `20000228` as yesterday's date for `20000301`. The migrated Python script uses the standard calendar library and correctly outputs `20000229`.
* **Setup**: Mock the system date to `2000-03-01`.
* **Action**: Execute both scripts and capture stdout.
* **Pass/Fail Criterion**: 
  * The Python script must output the correct calendar date: `20000301 20000229 200003 200002`.
  * The test suite explicitly asserts this correction against the legacy output.

### 6. Error Handling and Exit Codes
* **Purpose**: Verify that system failures or unexpected execution environments trigger the correct error response and exit codes.
* **Setup**: Force an execution failure (e.g., mock `date.today` to raise an `OSError`).
* **Action**: Execute `gestern.py`.
* **Pass/Fail Criterion**: The script must write `"Fehler !!!!"` to `stderr`, write the traceback/error details to `stderr`, and exit with status code `1`.

---

## Runnable Test Code (`pytest`)

Save the following code as `test_gestern.py`. It contains the complete test suite and automatically handles the mocking of both the shell environment and Python runtime.

```python
import os
import subprocess
import sys
import tempfile
from datetime import date
from unittest.mock import patch
import pytest

# Paths to the scripts under test
KSH_SCRIPT_PATH = "./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh"
PY_SCRIPT_PATH = "./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.py"


@pytest.fixture
def mock_system_date():
    """
    Fixture to mock the date for both KSH (via PATH manipulation)
    and Python (via unittest.mock).
    """
    mocked_dates = {}

    def _set_date(year: int, month: int, day: int):
        mocked_dates["date"] = date(year, month, day)
        
        # 1. Create a mock 'date' executable for KSH
        temp_dir = tempfile.mkdtemp()
        mock_date_script = os.path.join(temp_dir, "date")
        with open(mock_date_script, "w") as f:
            f.write(f"#!/bin/sh\n")
            # Output format expected by gestern.ksh: ' %d %m %Y'
            f.write(f'echo " {day:02d} {month:02d} {year:04d}"\n')
        os.chmod(mock_date_script, 0o755)
        
        # Prepend temp_dir to PATH
        original_path = os.environ.get("PATH", "")
        os.environ["PATH"] = f"{temp_dir}:{original_path}"
        
        mocked_dates["temp_dir"] = temp_dir
        mocked_dates["original_path"] = original_path
        return mocked_dates["date"]

    yield _set_date

    # Cleanup PATH and temp directory
    if "original_path" in mocked_dates:
        os.environ["PATH"] = mocked_dates["original_path"]
    if "temp_dir" in mocked_dates:
        import shutil
        shutil.rmtree(mocked_dates["temp_dir"], ignore_errors=True)


def run_ksh() -> str:
    """Helper to run the legacy KSH script and return stdout."""
    result = subprocess.run(
        ["/usr/bin/ksh", KSH_SCRIPT_PATH],
        capture_output=True,
        text=True,
        check=True
    )
    return result.stdout.strip()


def run_python(mocked_date: date) -> str:
    """Helper to run the Python script with mocked date.today()."""
    # We execute the script in-process to allow easy mocking of datetime
    import importlib.util
    spec = importlib.util.spec_from_file_location("gestern", PY_SCRIPT_PATH)
    gestern_module = importlib.util.module_from_spec(spec)
    
    with patch("datetime.date") as mock_date:
        mock_date.today.return_value = mocked_date
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        
        # Capture stdout
        from io import StringIO
        old_stdout = sys.stdout
        redirected_output = StringIO()
        sys.stdout = redirected_output
        try:
            spec.loader.exec_module(gestern_module)
            gestern_module.main()
        finally:
            sys.stdout = old_stdout
            
        return redirected_output.getvalue().strip()


# ==============================================================================
# TEST CASES
# ==============================================================================

def test_standard_mid_month(mock_system_date):
    """Test Case 1: Standard Mid-Month Date Parity"""
    target_date = mock_system_date(2023, 10, 15)
    
    ksh_output = run_ksh()
    py_output = run_python(target_date)
    
    expected = "20231015 20231014 202310 202310"
    assert py_output == expected, f"Python output mismatch: {py_output}"
    assert ksh_output == py_output, f"KSH ({ksh_output}) and Python ({py_output}) do not match!"


def test_month_boundary(mock_system_date):
    """Test Case 2: Month Boundary Transition"""
    target_date = mock_system_date(2023, 5, 1)
    
    ksh_output = run_ksh()
    py_output = run_python(target_date)
    
    expected = "20230501 20230430 202305 202304"
    assert py_output == expected
    assert ksh_output == py_output


def test_year_boundary(mock_system_date):
    """Test Case 3: Year Boundary Transition"""
    target_date = mock_system_date(2024, 1, 1)
    
    ksh_output = run_ksh()
    py_output = run_python(target_date)
    
    expected = "20240101 20231231 202401 202312"
    assert py_output == expected
    assert ksh_output == py_output


def test_leap_year_standard(mock_system_date):
    """Test Case 4: Leap Year Transition (Standard Leap Year)"""
    target_date = mock_system_date(2024, 3, 1)
    
    ksh_output = run_ksh()
    py_output = run_python(target_date)
    
    expected = "20240301 20240229 202403 202402"
    assert py_output == expected
    assert ksh_output == py_output


def test_leap_year_century_bug_validation(mock_system_date):
    """
    Test Case 5: Leap Year Boundary Discrepancy (Year 2000)
    Proves Python correctly handles Year 2000 leap year, fixing the legacy KSH bug.
    """
    target_date = mock_system_date(2000, 3, 1)
    
    ksh_output = run_ksh()
    py_output = run_python(target_date)
    
    # Legacy KSH incorrectly outputs 20000228 due to leap year modulo bug
    legacy_bug_output = "20000301 20000228 200003 200002"
    # Python correctly outputs 20000229
    correct_output = "20000301 20000229 200003 200002"
    
    assert ksh_output == legacy_bug_output, "Legacy script did not exhibit the expected leap-year bug."
    assert py_output == correct_output, f"Python failed to correctly calculate leap year 2000: {py_output}"


def test_error_handling_and_exit_code():
    """Test Case 6: Error Handling & Exit Code"""
    # Run Python script in a subprocess with a simulated environment failure
    # We pass an invalid environment variable or break the execution context to trigger the exception block
    with patch("datetime.date") as mock_date:
        mock_date.today.side_effect = OSError("System clock unavailable")
        
        # Capture stderr and exit code
        import importlib.util
        spec = importlib.util.spec_from_file_location("gestern", PY_SCRIPT_PATH)
        gestern_module = importlib.util.module_from_spec(spec)
        
        old_stderr = sys.stderr
        redirected_err = StringIO()
        sys.stderr = redirected_err
        
        try:
            with pytest.raises(SystemExit) as exit_info:
                spec.loader.exec_module(gestern_module)
                gestern_module.main()
            
            assert exit_info.value.code == 1
            err_output = redirected_err.getvalue()
            assert "Fehler !!!!" in err_output
            assert "Error executing date calculation" in err_output
        finally:
            sys.stderr = old_stderr
```