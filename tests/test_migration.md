The migration of `r_ausd_bp_ta_bpr_basis.ksh` to a Python-based Airflow DAG requires comprehensive testing to ensure behavioral equivalence. This document outlines a suite of tests covering output parity, transformation correctness, external system replacements, and data quality assertions relevant to an orchestration script.

Given that `k_ausd_bp_ta_bpr_basis.ksh` (the core processing script) is a separate migration, these tests will focus on the orchestrator's behavior, using mocks for the core script and utility functions.

---

## Test Case 1.1: Default Execution - Log Content and Exit Status Parity

*   **Purpose:** Verify that when executed without any command-line arguments, the migrated Python orchestrator produces log output and an exit status functionally equivalent to the legacy KornShell script. This includes correct parameter defaulting (`Stichtag` to system date, `Wiederanlaufwert` to 0), proper logging of job metadata, and a successful completion message.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Create a mock `k_ausd_bp_ta_bpr_basis.ksh` script that simply prints its arguments and exits successfully (e.g., `echo "Mock core script executed with args: $@" && exit 0`). Place it in a mock `BERT_DIR_ROOT/aufbereitung/bin/`.
        *   Mock `DWDate_Gib_Zeitraum` to return a consistent date (e.g., `20231026`).
        *   Mock `DWMSG_ErmittleNr` to return a consistent job entry number (e.g., `12345`).
        *   Mock other `DWMSG_*` functions to capture their output to a temporary log file.
        *   Set `BERT_DIR_ROOT` environment variable to the mock directory.
    2.  **Migrated Environment (Python/Pytest):**
        *   Use `unittest.mock.patch` to mock `common.utils.get_system_date_ddmmyyyy` to return `20231026`.
        *   Use `unittest.mock.patch` to mock `common.utils.get_next_job_entry_number` to return `12345`.
        *   Mock `subprocess.run` (or the Airflow operator responsible for invoking the core script) to capture the command and arguments it receives, and to simulate a successful execution (return code 0).
        *   Configure Python's `logging` to capture all `INFO` level messages to a `StringIO` object for comparison.
        *   Set `BERT_DIR_ROOT` environment variable for the Python process.
*   **Action:**
    1.  Execute the legacy `r_ausd_bp_ta_bpr_basis.ksh` script without any command-line arguments. Capture its stdout/stderr and the content of the generated log file. Record its exit status.
    2.  Call the `run_orchestration()` function in the migrated Python code. Capture the log output and its return value.
*   **Pass/Fail Criterion:**
    *   The captured log output from the migrated job (after normalizing dynamic elements like timestamps, `LogDatei` path, and process IDs) must be functionally equivalent to the legacy job's combined stdout/stderr and log file content.
    *   The exit status of the legacy job must be `0`, and the return value of the `run_orchestration` function must be `0`.
    *   The parameters passed to the mocked core script invocation must be identical:
        *   `-j ausd_bp_ta_bpr_basis`
        *   `-s 20231026` (mocked system date)
        *   `-f 12345` (mocked entry number)
        *   `-l 0` (default `Wiederanlaufwert`)

```python
# pytest_orchestrator.py
import os
import sys
import logging
from io import StringIO
from unittest.mock import patch, MagicMock
import pytest

# Assume r_ausd_bp_ta_bpr_basis_orchestrator.py and common/utils.py are in PYTHONPATH
from r_ausd_bp_ta_bpr_basis_orchestrator import run_orchestration, JOB_KENNUNG

# Setup a fixture to capture logs
@pytest.fixture
def caplog_fixture(caplog):
    caplog.set_level(logging.INFO)
    return caplog

@patch('r_ausd_bp_ta_bpr_basis_orchestrator.subprocess.run')
@patch('common.utils.get_system_date_ddmmyyyy', return_value='20231026')
@patch('common.utils.get_next_job_entry_number', return_value=12345)
@patch('common.utils.build_log_filename', return_value='/tmp/mock_log.log')
@patch('common.utils.create_log_entry')
@patch('common.utils.set_stichtag_info')
@patch('common.utils.set_status_ok')
@patch('common.utils.append_to_log')
@patch('common.utils.pruefe_parameter_gesetzt')
def test_default_execution_parity(
    mock_pruefe_param, mock_append_log, mock_set_status_ok, mock_set_stichtag_info,
    mock_create_log_entry, mock_build_log_filename, mock_get_next_job_entry_number,
    mock_get_sysdate, mock_subprocess_run, caplog_fixture
):
    # Mock subprocess.run to simulate success
    mock_subprocess_run.return_value = MagicMock(
        returncode=0, stdout="Core script mock output.", stderr=""
    )

    # Set BERT_DIR_ROOT for the test
    os.environ['BERT_DIR_ROOT'] = '/mock/bert/root'

    # Action: Run the orchestration function
    exit_code = run_orchestration()

    # Assertions for exit status
    assert exit_code == 0

    # Assertions for core script invocation
    expected_core_script_path = '/mock/bert/root/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.py'
    mock_subprocess_run.assert_called_once_with(
        [
            expected_core_script_path,
            '-j', JOB_KENNUNG,
            '-s', '20231026', # Defaulted stichtag
            '-f', '12345',    # Mocked entry number
            '-l', '0'         # Defaulted wiederanlaufwert
        ],
        capture_output=True, text=True, check=True
    )

    # Assertions for logging (simplified, full parity would involve regex matching)
    assert "Starting orchestration." in caplog_fixture.text
    assert f"Job-Nr    : '12345'" in caplog_fixture.text
    assert f"JobKennung: '{JOB_KENNUNG}'" in caplog_fixture.text
    assert f"Logdatei  : '/tmp/mock_log.log'" in caplog_fixture.text
    assert f"Stichtag  : '20231026'" in caplog_fixture.text
    assert f"Invoking core script: {expected_core_script_path} -j {JOB_KENNUNG} -s 20231026 -f 12345 -l 0" in caplog_fixture.text
    assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in caplog_fixture.text
    assert "Orchestration completed successfully." in caplog_fixture.text

    # Verify utility calls
    mock_get_sysdate.assert_called_once()
    mock_get_next_job_entry_number.assert_called_once()
    mock_build_log_filename.assert_called_once_with(JOB_KENNUNG, 12345)
    mock_create_log_entry.assert_called_once_with(12345, JOB_KENNUNG, 'r_ausd_bp_ta_bpr_basis_orchestrator.py', '/tmp/mock_log.log')
    mock_set_stichtag_info.assert_called_once_with(12345, '20231026', 'DDMMYYYY')
    mock_set_status_ok.assert_called_once_with(12345, '/tmp/mock_log.log')
    mock_append_log.assert_called_once_with('/tmp/mock_log.log', "Die Abarbeitung wurde ohne erkennbare Fehler beendet")
    mock_pruefe_param.assert_called_once_with("Stichtag", "20231026")

    # Clean up environment variable
    del os.environ['BERT_DIR_ROOT']
```

---

## Test Case 1.2: Parameterized Execution - Log Content and Exit Status Parity

*   **Purpose:** Verify that when executed with specific `Stichtag` and `Wiederanlaufwert` parameters, the migrated Python orchestrator produces equivalent log output and exit status, correctly passing these parameters to the core script.
*   **Setup:** Same as Test Case 1.1, but the `run_orchestration` function will be called with explicit `stichtag` and `wiederanlaufwert` arguments.
*   **Action:**
    1.  Execute the legacy script: `r_ausd_bp_ta_bpr_basis.ksh -s 20230115 -l 1000`. Capture output and exit status.
    2.  Call the `run_orchestration(stichtag='20230115', wiederanlaufwert=1000)` function in the migrated Python code. Capture log output and return value.
*   **Pass/Fail Criterion:**
    *   Log output (normalized) must be functionally equivalent.
    *   Exit status must be `0`.
    *   Parameters passed to the mocked core script must be identical:
        *   `-j ausd_bp_ta_bpr_basis`
        *   `-s 20230115`
        *   `-f 12345` (mocked entry number)
        *   `-l 1000`

```python
# pytest_orchestrator.py (continued)
@patch('r_ausd_bp_ta_bpr_basis_orchestrator.subprocess.run')
@patch('common.utils.get_system_date_ddmmyyyy', return_value='20231026') # Still mocked, but should not be used for stichtag
@patch('common.utils.get_next_job_entry_number', return_value=12345)
@patch('common.utils.build_log_filename', return_value='/tmp/mock_log.log')
@patch('common.utils.create_log_entry')
@patch('common.utils.set_stichtag_info')
@patch('common.utils.set_status_ok')
@patch('common.utils.append_to_log')
@patch('common.utils.pruefe_parameter_gesetzt')
def test_parameterized_execution_parity(
    mock_pruefe_param, mock_append_log, mock_set_status_ok, mock_set_stichtag_info,
    mock_create_log_entry, mock_build_log_filename, mock_get_next_job_entry_number,
    mock_get_sysdate, mock_subprocess_run, caplog_fixture
):
    mock_subprocess_run.return_value = MagicMock(
        returncode=0, stdout="Core script mock output.", stderr=""
    )
    os.environ['BERT_DIR_ROOT'] = '/mock/bert/root'

    # Action: Run with specific parameters
    exit_code = run_orchestration(stichtag='20230115', wiederanlaufwert=1000)

    assert exit_code == 0

    expected_core_script_path = '/mock/bert/root/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.py'
    mock_subprocess_run.assert_called_once_with(
        [
            expected_core_script_path,
            '-j', JOB_KENNUNG,
            '-s', '20230115', # Provided stichtag
            '-f', '12345',
            '-l', '1000'      # Provided wiederanlaufwert
        ],
        capture_output=True, text=True, check=True
    )

    assert f"Stichtag  : '20230115'" in caplog_fixture.text
    assert f"Invoking core script: {expected_core_script_path} -j {JOB_KENNUNG} -s 20230115 -f 12345 -l 1000" in caplog_fixture.text
    mock_pruefe_param.assert_called_once_with("Stichtag", "20230115")
    mock_get_sysdate.assert_called_once() # Still called, but its value is overridden by explicit stichtag

    del os.environ['BERT_DIR_ROOT']
```

---

## Test Case 2.1: Parameter Parsing and Defaulting - Stichtag

*   **Purpose:** Verify that the `stichtag` parameter is correctly parsed and defaulted according to the legacy logic.
*   **Setup:**
    *   Mock `common.utils.get_system_date_ddmmyyyy` to return a fixed date (e.g., `20231026`).
    *   Mock `common.utils.log_error_and_exit` to raise a `ValueError` instead of exiting, allowing the test to catch the error.
    *   Mock `subprocess.run` to ensure it's not called on error.
*   **Action:**
    1.  Call `run_orchestration()` (no `stichtag`).
    2.  Call `run_orchestration(stichtag='20230101')`.
    3.  Call `run_orchestration(stichtag=' ')` (empty string, simulating an invalid input that `pruefeParameterGesetzt` should catch).
    4.  Call `run_orchestration(stichtag='INVALID_DATE_FORMAT')`.
*   **Pass/Fail Criterion:**
    *   For `run_orchestration()`: The `stichtag` passed to the core script mock must be `20231026`.
    *   For `run_orchestration(stichtag='20230101')`: The `stichtag` passed to the core script mock must be `20230101`.
    *   For `run_orchestration(stichtag=' ')`: `common.utils.pruefe_parameter_gesetzt` must be called with `Stichtag` and `' '`, and `common.utils.log_error_and_exit` must be called, raising a `ValueError`. The core script should *not* be invoked.
    *   For `run_orchestration(stichtag='INVALID_DATE_FORMAT')`: This specific validation is not explicitly in the KSH `getopts` or `pruefeParameterGesetzt` (it would pass as a string). The Python `datetime` conversion would typically happen *within* the core script. For the orchestrator, it should still pass the string. If the core script is a BigQuery operator, it might fail there. For this test, we verify the orchestrator passes the string as-is.

```python
# pytest_orchestrator.py (continued)
@patch('r_ausd_bp_ta_bpr_basis_orchestrator.subprocess.run')
@patch('common.utils.get_system_date_ddmmyyyy', return_value='20231026')
@patch('common.utils.get_next_job_entry_number', return_value=12345)
@patch('common.utils.build_log_filename', return_value='/tmp/mock_log.log')
@patch('common.utils.create_log_entry')
@patch('common.utils.set_stichtag_info')
@patch('common.utils.set_status_ok')
@patch('common.utils.append_to_log')
@patch('common.utils.log_error_and_exit', side_effect=ValueError("Mocked error exit"))
@patch('common.utils.pruefe_parameter_gesetzt')
def test_stichtag_parsing_and_defaulting(
    mock_pruefe_param, mock_log_error_and_exit, mock_append_log, mock_set_status_ok,
    mock_set_stichtag_info, mock_create_log_entry, mock_build_log_filename,
    mock_get_next_job_entry_number, mock_get_sysdate, mock_subprocess_run, caplog_fixture
):
    os.environ['BERT_DIR_ROOT'] = '/mock/bert/root'
    mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    # Test 1: No stichtag provided, should default to system date
    run_orchestration()
    args, _ = mock_subprocess_run.call_args
    assert '-s' in args[0] and args[0][args[0].index('-s') + 1] == '20231026'
    mock_pruefe_param.assert_called_with("Stichtag", "20231026")
    mock_subprocess_run.reset_mock()
    mock_pruefe_param.reset_mock()

    # Test 2: Specific stichtag provided
    run_orchestration(stichtag='20230101')
    args, _ = mock_subprocess_run.call_args
    assert '-s' in args[0] and args[0][args[0].index('-s') + 1] == '20230101'
    mock_pruefe_param.assert_called_with("Stichtag", "20230101")
    mock_subprocess_run.reset_mock()
    mock_pruefe_param.reset_mock()

    # Test 3: Empty stichtag string (should trigger validation error)
    with pytest.raises(ValueError, match="Mocked error exit"):
        run_orchestration(stichtag=' ')
    mock_pruefe_param.assert_called_with("Stichtag", " ")
    mock_log_error_and_exit.assert_called_once_with("Parameter 'Stichtag' is not set.")
    mock_subprocess_run.assert_not_called() # Core script should not be invoked
    mock_log_error_and_exit.reset_mock()
    mock_pruefe_param.reset_mock()

    # Test 4: Invalid date format (orchestrator passes it, core script would fail)
    # The orchestrator itself doesn't validate date *format*, only presence.
    # This is consistent with the KSH script's `pruefeParameterGesetzt`.
    run_orchestration(stichtag='INVALID_DATE_FORMAT')
    args, _ = mock_subprocess_run.call_args
    assert '-s' in args[0] and args[0][args[0].index('-s') + 1] == 'INVALID_DATE_FORMAT'
    mock_pruefe_param.assert_called_with("Stichtag", "INVALID_DATE_FORMAT")
    mock_subprocess_run.assert_called_once() # Core script is invoked with invalid date
    mock_subprocess_run.reset_mock()
    mock_pruefe_param.reset_mock()

    del os.environ['BERT_DIR_ROOT']
```

---

## Test Case 2.2: Parameter Parsing and Defaulting - Wiederanlaufwert

*   **Purpose:** Verify that the `wiederanlaufwert` parameter is correctly parsed and defaulted.
*   **Setup:** Same as Test Case 2.1.
*   **Action:**
    1.  Call `run_orchestration()` (no `wiederanlaufwert`).
    2.  Call `run_orchestration(wiederanlaufwert=500)`.
    3.  Attempt to call `run_orchestration(wiederanlaufwert='not-a-number')`.
*   **Pass/Fail Criterion:**
    *   For `run_orchestration()`: The `wiederanlaufwert` passed to the core script mock must be `0`.
    *   For `run_orchestration(wiederanlaufwert=500)`: The `wiederanlaufwert` passed to the core script mock must be `500`.
    *   For `run_orchestration(wiederanlaufwert='not-a-number')`: The `argparse` mechanism (or direct Python type conversion) should raise a `TypeError` or similar exception before `run_orchestration` even starts, indicating an invalid type. The core script should *not* be invoked.

```python
# pytest_orchestrator.py (continued)
@patch('r_ausd_bp_ta_bpr_basis_orchestrator.subprocess.run')
@patch('common.utils.get_system_date_ddmmyyyy', return_value='20231026')
@patch('common.utils.get_next_job_entry_number', return_value=12345)
@patch('common.utils.build_log_filename', return_value='/tmp/mock_log.log')
@patch('common.utils.create_log_entry')
@patch('common.utils.set_stichtag_info')
@patch('common.utils.set_status_ok')
@patch('common.utils.append_to_log')
@patch('common.utils.pruefe_parameter_gesetzt')
def test_wiederanlaufwert_parsing_and_defaulting(
    mock_pruefe_param, mock_append_log, mock_set_status_ok, mock_set_stichtag_info,
    mock_create_log_entry, mock_build_log_filename, mock_get_next_job_entry_number,
    mock_get_sysdate, mock_subprocess_run, caplog_fixture
):
    os.environ['BERT_DIR_ROOT'] = '/mock/bert/root'
    mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    # Test 1: No wiederanlaufwert provided, should default to 0
    run_orchestration()
    args, _ = mock_subprocess_run.call_args
    assert '-l' in args[0] and args[0][args[0].index('-l') + 1] == '0'
    mock_subprocess_run.reset_mock()

    # Test 2: Specific wiederanlaufwert provided
    run_orchestration(wiederanlaufwert=500)
    args, _ = mock_subprocess_run.call_args
    assert '-l' in args[0] and args[0][args[0].index('-l') + 1] == '500'
    mock_subprocess_run.reset_mock()

    # Test 3: Invalid wiederanlaufwert type (argparse handles this before run_orchestration)
    # This requires testing the argparse part directly or simulating its failure.
    # For direct `run_orchestration` call, Python's type hinting would catch it.
    # If called via argparse, it would raise a SystemExit.
    # Here, we simulate the `argparse` behavior by directly checking the type.
    # The `run_orchestration` function itself expects `int` for `wiederanlaufwert`.
    # If a string is passed, it would be a TypeError.
    with pytest.raises(TypeError):
        run_orchestration(wiederanlaufwert='not-a-number')
    mock_subprocess_run.assert_not_called() # Core script should not be invoked

    del os.environ['BERT_DIR_ROOT']
```

---

## Test Case 3.1: Environment Variable Resolution (BERT_DIR_ROOT)

*   **Purpose:** Verify that `BERT_DIR_ROOT` is correctly resolved in the migrated environment, impacting the path to the core script.
*   **Setup:**
    *   **Legacy:** Set `BERT_DIR_ROOT=/legacy/bert/root` before executing the KSH script.
    *   **Migrated:** Set `BERT_DIR_ROOT=/gcp/bert/root` as an environment variable for the Python process.
    *   Mock `subprocess.run` to capture the full command.
*   **Action:**
    1.  Run the legacy script with `BERT_DIR_ROOT` set to `/legacy/bert/root`. Observe the `Name_Kernskript` value in the logs or by inspecting the script's execution.
    2.  Run the migrated Python orchestration with `BERT_DIR_ROOT` set to `/gcp/bert/root`.
*   **Pass/Fail Criterion:**
    *   The `Name_Kernskript` variable in the legacy script should resolve to `/legacy/bert/root/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh`.
    *   The `core_script_command` in the migrated Python script (captured by `mock_subprocess_run`) should use `/gcp/bert/root/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.py` (assuming Python migration of the core script).

```python
# pytest_orchestrator.py (continued)
@patch('r_ausd_bp_ta_bpr_basis_orchestrator.subprocess.run')
@patch('common.utils.get_system_date_ddmmyyyy', return_value='20231026')
@patch('common.utils.get_next_job_entry_number', return_value=12345)
@patch('common.utils.build_log_filename', return_value='/tmp/mock_log.log')
@patch('common.utils.create_log_entry')
@patch('common.utils.set_stichtag_info')
@patch('common.utils.set_status_ok')
@patch('common.utils.append_to_log')
@patch('common.utils.pruefe_parameter_gesetzt')
def test_bert_dir_root_resolution(
    mock_pruefe_param, mock_append_log, mock_set_status_ok, mock_set_stichtag_info,
    mock_create_log_entry, mock_build_log_filename, mock_get_next_job_entry_number,
    mock_get_sysdate, mock_subprocess_run, caplog_fixture
):
    mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    test_bert_dir_root = '/custom/gcp/bert/root'
    os.environ['BERT_DIR_ROOT'] = test_bert_dir_root

    run_orchestration()

    expected_core_script_path = f'{test_bert_dir_root}/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.py'
    mock_subprocess_run.assert_called_once()
    args, _ = mock_subprocess_run.call_args
    assert args[0][0] == expected_core_script_path
    assert f"Invoking core script: {expected_core_script_path}" in caplog_fixture.text

    del os.environ['BERT_DIR_ROOT']
```

---

## Test Case 3.2: Logging Framework Replacement (DWMSG_* to Cloud Logging)

*   **Purpose:** Verify that the migrated logging correctly captures messages and metadata, and directs them to Cloud Logging (simulated by `caplog_fixture`), replacing the legacy file-based `DWMSG_*` functions.
*   **Setup:**
    *   **Legacy:** Run the script and inspect the generated `$LogDatei` and stdout.
    *   **Migrated:** Use `caplog_fixture` to capture all `INFO` level logs from the Python orchestrator.
*   **Action:**
    1.  Run the legacy script with default parameters.
    2.  Run the migrated Python orchestration with default parameters.
*   **Pass/Fail Criterion:**
    *   The `caplog_fixture` should contain log entries corresponding to:
        *   Job start (`DWMSG_ErzeugeEintrag` equivalent)
        *   Stichtag info (`DWMSG_SetzeStichtagInfo` equivalent)
        *   Job metadata print statements
        *   Core script invocation message
        *   Success message (`Die Abarbeitung wurde ohne erkennbare Fehler beendet`)
        *   Status update (`DWMSG_SetzeStatusOK` equivalent)
    *   The content of these log entries (after normalizing dynamic values) should match the content found in the legacy `$LogDatei` and stdout.
    *   The structure of the log entries should be appropriate for Cloud Logging (e.g., clear message, level, timestamp).

```python
# This test is largely covered by Test Case 1.1's assertions on `caplog_fixture.text`
# and the verification of `common.utils` mock calls.
# The `caplog_fixture` already captures all INFO level logs.
# Additional assertions could be added to check specific log message formats.

# Example assertion for a specific log message format:
# assert "DWMSG_ErzeugeEintrag: 12345, ausd_bp_ta_bpr_basis, r_ausd_bp_ta_bpr_basis_orchestrator.py, /tmp/mock_log.log" in caplog_fixture.text
# This confirms the `create_log_entry` utility was called and logged correctly.
```

---

## Test Case 3.3: Error Handling Replacement (trap to Airflow/Python Exceptions)

*   **Purpose:** Verify that when the core script fails, the orchestrator correctly handles the error, logs it, and raises an exception (which would fail the Airflow task).
*   **Setup:**
    *   **Legacy:** Mock `k_ausd_bp_ta_bpr_basis.ksh` to `exit 1` (simulate failure).
    *   **Migrated:** Mock `subprocess.run` to raise a `subprocess.CalledProcessError` (simulating a non-zero exit code from the core script).
    *   Mock `common.utils.log_error_and_exit` to raise a specific exception (e.g., `RuntimeError`) to confirm error handling flow.
*   **Action:**
    1.  Run the legacy script. Observe its exit status and log output (should contain `DWMSG_Fehlerbehandlung` messages).
    2.  Call the `run_orchestration()` function in the migrated Python code.
*   **Pass/Fail Criterion:**
    *   **Legacy:** The legacy script should exit with a non-zero status. The log file should contain error messages from `DWMSG_Fehlerbehandlung`.
    *   **Migrated:** The `run_orchestration` function should raise a `RuntimeError` (or the exception configured in `log_error_and_exit`). The `caplog_fixture` should contain an error message indicating the core script failure. The `subprocess.run` mock should have been called with `check=True` to ensure it raises an exception on non-zero exit.

```python
# pytest_orchestrator.py (continued)
@patch('r_ausd_bp_ta_bpr_basis_orchestrator.subprocess.run')
@patch('common.utils.get_system_date_ddmmyyyy', return_value='20231026')
@patch('common.utils.get_next_job_entry_number', return_value=12345)
@patch('common.utils.build_log_filename', return_value='/tmp/mock_log.log')
@patch('common.utils.create_log_entry')
@patch('common.utils.set_stichtag_info')
@patch('common.utils.set_status_ok')
@patch('common.utils.append_to_log')
@patch('common.utils.pruefe_parameter_gesetzt')
@patch('common.utils.log_error_and_exit', side_effect=RuntimeError("Mocked core script failure"))
def test_core_script_failure_handling(
    mock_log_error_and_exit, mock_pruefe_param, mock_append_log, mock_set_status_ok,
    mock_set_stichtag_info, mock_create_log_entry, mock_build_log_filename,
    mock_get_next_job_entry_number, mock_get_sysdate, mock_subprocess_run, caplog_fixture
):
    os.environ['BERT_DIR_ROOT'] = '/mock/bert/root'

    # Configure mock_subprocess_run to simulate a failed core script
    mock_subprocess_run.side_effect = subprocess.CalledProcessError(
        returncode=1, cmd="mock_cmd", stderr="Core script failed due to X"
    )

    # Action: Run the orchestration function, expecting it to raise an error
    with pytest.raises(RuntimeError, match="Mocked core script failure"):
        run_orchestration()

    # Assertions
    mock_subprocess_run.assert_called_once()
    mock_log_error_and_exit.assert_called_once_with(
        "Core script failed with exit code 1: Core script failed due to X"
    )
    assert "ERROR: Core script failed with exit code 1: Core script failed due to X" in caplog_fixture.text
    assert "Orchestration completed successfully." not in caplog_fixture.text
    mock_set_status_ok.assert_not_called() # Should not be called on failure

    del os.environ['BERT_DIR_ROOT']
```

---

## Test Case 4.1: Parameter Type and Format Validation (Orchestrator Level)

*   **Purpose:** Verify that the orchestrator correctly validates the types and basic presence of input parameters before proceeding to invoke the core script. This is an orchestrator-level check, not a deep business logic validation.
*   **Setup:**
    *   Mock `common.utils.log_error_and_exit` to raise a `ValueError` to catch validation failures.
    *   Mock `subprocess.run` to ensure it's not called.
*   **Action:**
    1.  Attempt to call `run_orchestration(stichtag=' ')` (empty string, caught by `pruefe_parameter_gesetzt`).
    2.  Attempt to call `run_orchestration(wiederanlaufwert='not-a-number')` (caught by Python type system).
*   **Pass/Fail Criterion:**
    *   For `stichtag=' '`: A `ValueError` (from `log_error_and_exit`) must be raised, and `subprocess.run` must *not* be called.
    *   For `wiederanlaufwert='not-a-number'`: A `TypeError` must be raised (due to Python's type system for the `wiederanlaufwert: int` annotation), and `subprocess.run` must *not* be called.

```python
# This test is covered by Test Case 2.1 (Test 3) and Test Case 2.2 (Test 3).
# The `pytest.raises` context manager is used to assert that specific exceptions are raised,
# and `mock_subprocess_run.assert_not_called()` confirms the core script is not invoked.
```

---

## Test Case 4.2: `DW_EintragsNr` and `JobKennung` Consistency

*   **Purpose:** Verify that the `DW_EintragsNr` and `JobKennung` are consistently generated/assigned and passed to the core script, and reflected in the logs.
*   **Setup:**
    *   Mock `common.utils.get_next_job_entry_number` to return a sequence of numbers (e.g., `100`, then `101` for subsequent calls).
    *   Mock `subprocess.run` to capture the parameters.
*   **Action:**
    1.  Call `run_orchestration()` once.
    2.  Call `run_orchestration()` a second time.
*   **Pass/Fail Criterion:**
    *   The first invocation of the core script should receive `-f 100`.
    *   The second invocation of the core script should receive `-f 101`.
    *   Both invocations should receive `-j ausd_bp_ta_bpr_basis`.
    *   The `DW_EintragsNr` and `JobKennung` in the captured logs for each run should match these values.

```python
# pytest_orchestrator.py (continued)
@patch('r_ausd_bp_ta_bpr_basis_orchestrator.subprocess.run')
@patch('common.utils.get_system_date_ddmmyyyy', return_value='20231026')
@patch('common.utils.get_next_job_entry_number') # Will configure side_effect
@patch('common.utils.build_log_filename', return_value='/tmp/mock_log.log')
@patch('common.utils.create_log_entry')
@patch('common.utils.set_stichtag_info')
@patch('common.utils.set_status_ok')
@patch('common.utils.append_to_log')
@patch('common.utils.pruefe_parameter_gesetzt')
def test_job_kennung_and_eintragsnr_consistency(
    mock_pruefe_param, mock_append_log, mock_set_status_ok, mock_set_stichtag_info,
    mock_create_log_entry, mock_build_log_filename, mock_get_next_job_entry_number,
    mock_get_sysdate, mock_subprocess_run, caplog_fixture
):
    os.environ['BERT_DIR_ROOT'] = '/mock/bert/root'
    mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    # Configure get_next_job_entry_number to return a sequence
    mock_get_next_job_entry_number.side_effect = [100, 101]

    # Run 1
    caplog_fixture.clear() # Clear logs from previous tests
    run_orchestration()
    args_run1, _ = mock_subprocess_run.call_args
    assert '-f' in args_run1[0] and args_run1[0][args_run1[0].index('-f') + 1] == '100'
    assert '-j' in args_run1[0] and args_run1[0][args_run1[0].index('-j') + 1] == JOB_KENNUNG
    assert f"Job-Nr    : '100'" in caplog_fixture.text
    assert f"JobKennung: '{JOB_KENNUNG}'" in caplog_fixture.text
    mock_subprocess_run.reset_mock()

    # Run 2
    caplog_fixture.clear()
    run_orchestration()
    args_run2, _ = mock_subprocess_run.call_args
    assert '-f' in args_run2[0] and args_run2[0][args_run2[0].index('-f') + 1] == '101'
    assert '-j' in args_run2[0] and args_run2[0][args_run2[0].index('-j') + 1] == JOB_KENNUNG
    assert f"Job-Nr    : '101'" in caplog_fixture.text
    assert f"JobKennung: '{JOB_KENNUNG}'" in caplog_fixture.text

    del os.environ['BERT_DIR_ROOT']
```