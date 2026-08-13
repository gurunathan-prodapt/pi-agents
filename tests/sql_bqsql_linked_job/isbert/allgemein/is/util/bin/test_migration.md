# Migration Validation Test Suite: `h_alis_sqlplus.py`

This document defines the migration-validation test suite for the migrated Python utility `h_alis_sqlplus.py` (originally `h_alis_sqlplus.ksh`). 

Since this script is an orchestration utility rather than a static data transformation, the validation focus is on **behavioral equivalence**: parameter validation, file-system checks, environment variable handling, external process execution (`sqlplus`), and error propagation.

---

## Test Case 1: Parameter Validation (Error Code 196)

### Purpose
Verify that the Python function `starte_sql_skript` behaves identically to the legacy KornShell script when required parameters (`p_eintragsnr` or `p_skript`) are missing or empty. It must trigger the error logging utility with error code `196` and return `196`.

### Setup
* Mock the unresolved external component `dwmsg_melde_fehler` to prevent `NotImplementedError` and capture its invocation arguments.
* Ensure the environment has `DW_ORAUSER` set (though validation should fail before checking it).

### Action
1. Call `starte_sql_skript` with an empty `p_eintragsnr`.
2. Call `starte_sql_skript` with an empty `p_skript`.

### Pass/Fail Criterion
* **Pass**: Both calls return exit code `196`. The mocked `dwmsg_melde_fehler` is called with parameters: `(p_eintragsnr, "E", 196, "alis_sqlplus V1.1.3 starteSQLSkript")`.
* **Fail**: The function returns any other exit code, raises an unhandled exception, or fails to call the error logging utility.

---

## Test Case 2: File Readability Validation (Error Code 201)

### Purpose
Verify that the Python function checks for the existence and readability of the target SQL script before attempting execution. If the file is missing or unreadable, it must trigger the error logging utility with error code `201` and return `201`.

### Setup
* Mock `dwmsg_melde_fehler` to capture invocation arguments.
* Provide a non-existent file path (e.g., `/tmp/non_existent_file_12345.sql`) as the `p_skript` argument.

### Action
Call `starte_sql_skript("TEST_ENTRY_01", "/tmp/non_existent_file_12345.sql")`.

### Pass/Fail Criterion
* **Pass**: The function returns exit code `201`. The mocked `dwmsg_melde_fehler` is called with parameters: `("TEST_ENTRY_01", "E", 201, "/tmp/non_existent_file_12345.sql")`.
* **Fail**: The function attempts to execute `sqlplus` anyway, returns a different exit code, or fails to log the error.

---

## Test Case 3: Successful SQL*Plus Execution & Argument Passing

### Purpose
Verify that when valid parameters and an existing script are provided, the utility correctly constructs and executes the `sqlplus` command line, passing all trailing arguments, and returns `0` upon successful execution.

### Setup
* Create a temporary, readable SQL script file.
* Set the environment variable `DW_ORAUSER=test_user/test_pass@test_db`.
* Mock `subprocess.run` to return a successful execution state (`returncode = 0`) without actually invoking the local OS `sqlplus` binary.

### Action
Call `starte_sql_skript("TEST_ENTRY_02", "/tmp/temp_script.sql", "param1", "param2")`.

### Pass/Fail Criterion
* **Pass**: 
  * The function returns exit code `0`.
  * `subprocess.run` is called with the exact command array: `['sqlplus', 'test_user/test_pass@test_db', '@/tmp/temp_script.sql', 'param1', 'param2']`.
  * `stdin` is redirected to `subprocess.DEVNULL` (equivalent to legacy `</dev/null`).
* **Fail**: The command array is malformed, arguments are shifted incorrectly, or the return code is non-zero.

---

## Test Case 4: SQL*Plus Failure & Exit Code Propagation

### Purpose
Verify that if the downstream `sqlplus` process fails (returns a non-zero exit code), the Python wrapper captures this exit code and propagates it back to the caller, matching the legacy `errcode=$?` behavior.

### Setup
* Create a temporary, readable SQL script file.
* Set the environment variable `DW_ORAUSER=test_user/test_pass@test_db`.
* Mock `subprocess.run` to return a failed execution state (`returncode = 42`).

### Action
Call `starte_sql_skript("TEST_ENTRY_03", "/tmp/temp_script.sql")`.

### Pass/Fail Criterion
* **Pass**: The function returns exit code `42` (propagated directly from the mocked subprocess execution).
* **Fail**: The function returns `0`, raises an unhandled exception, or returns a hardcoded error code instead of the actual process exit code.

---

## Test Case 5: Missing Environment Variable (`DW_ORAUSER`)

### Purpose
Verify that if the required environment variable `DW_ORAUSER` is missing, the script raises a clear, actionable error rather than attempting to execute `sqlplus` with empty credentials.

### Setup
* Create a temporary, readable SQL script file.
* Ensure `DW_ORAUSER` is removed from `os.environ`.

### Action
Call `starte_sql_skript("TEST_ENTRY_04", "/tmp/temp_script.sql")`.

### Pass/Fail Criterion
* **Pass**: The function raises a `ValueError` stating `"DW_ORAUSER environment variable is not set."`
* **Fail**: The function attempts to run `sqlplus` with an empty string or `None`, or fails silently.

---

## Runnable Test Code (Pytest Suite)

Save the following code as `test_h_alis_sqlplus.py` and run it using `pytest`.

```python
import os
import sys
import pytest
import pathlib
from unittest.mock import MagicMock, patch

# Import the target module
# Adjust import path as necessary depending on your project structure
import h_alis_sqlplus as target_module


@pytest.fixture
def mock_error_logger():
    """Mocks the unresolved external error logging utility."""
    with patch("h_alis_sqlplus.dwmsg_melde_fehler") as mock_log:
        yield mock_log


@pytest.fixture
def temp_sql_file(tmp_path):
    """Creates a temporary readable SQL script file."""
    file_path = tmp_path / "test_script.sql"
    file_path.write_text("SELECT 1 FROM DUAL;")
    return str(file_path)


def test_parameter_validation_missing_entry_nr(mock_error_logger):
    """TC1: Verify error code 196 when entry number is missing."""
    exit_code = target_module.starte_sql_skript("", "some_script.sql")
    
    assert exit_code == 196
    mock_error_logger.assert_called_once_with(
        "", "E", 196, f"{target_module.MODUL_NAME} {target_module.MODUL_VERSION} starteSQLSkript"
    )


def test_parameter_validation_missing_script(mock_error_logger):
    """TC1: Verify error code 196 when script path is missing."""
    exit_code = target_module.starte_sql_skript("12345", "")
    
    assert exit_code == 196
    mock_error_logger.assert_called_once_with(
        "12345", "E", 196, f"{target_module.MODUL_NAME} {target_module.MODUL_VERSION} starteSQLSkript"
    )


def test_file_readability_validation(mock_error_logger):
    """TC2: Verify error code 201 when script file does not exist."""
    non_existent_path = "/tmp/non_existent_file_999.sql"
    exit_code = target_module.starte_sql_skript("12345", non_existent_path)
    
    assert exit_code == 201
    mock_error_logger.assert_called_once_with("12345", "E", 201, non_existent_path)


@patch("subprocess.run")
def test_successful_sqlplus_execution(mock_run, temp_sql_file):
    """TC3: Verify successful execution and correct argument passing."""
    # Setup mock subprocess response
    mock_response = MagicMock()
    mock_response.returncode = 0
    mock_run.return_value = mock_response

    # Setup environment
    os.environ["DW_ORAUSER"] = "scott/tiger@orcl"

    # Action
    exit_code = target_module.starte_sql_skript(
        "12345", temp_sql_file, "param_a", "param_b"
    )

    # Assertions
    assert exit_code == 0
    mock_run.assert_called_once_with(
        ["sqlplus", "scott/tiger@orcl", f"@{temp_sql_file}", "param_a", "param_b"],
        stdin=target_module.subprocess.DEVNULL,
        capture_output=False,
        text=True,
        check=False
    )


@patch("subprocess.run")
def test_sqlplus_failure_propagation(mock_run, temp_sql_file):
    """TC4: Verify that non-zero exit codes from sqlplus are propagated."""
    # Setup mock subprocess response to return failure code 42
    mock_response = MagicMock()
    mock_response.returncode = 42
    mock_run.return_value = mock_response

    # Setup environment
    os.environ["DW_ORAUSER"] = "scott/tiger@orcl"

    # Action
    exit_code = target_module.starte_sql_skript("12345", temp_sql_file)

    # Assertions
    assert exit_code == 42


def test_missing_dw_orauser_env_variable(temp_sql_file):
    """TC5: Verify ValueError is raised when DW_ORAUSER is missing."""
    if "DW_ORAUSER" in os.environ:
        del os.environ["DW_ORAUSER"]

    with pytest.raises(ValueError) as exc_info:
        target_module.starte_sql_skript("12345", temp_sql_file)
    
    assert "DW_ORAUSER environment variable is not set" in str(exc_info.value)
```