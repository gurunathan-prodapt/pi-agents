Here is the migration-validation test suite designed to verify behavioral equivalence between the legacy UC4/KornShell environment and the migrated Apache Airflow/Python environment for the job `DW.EXTTEST_LEGACY_DWH`.

---

# Migration Validation Test Suite: DW.EXTTEST_LEGACY_DWH

## Test Case 1: CLI Help Argument Parity
### Purpose
Verify that both the legacy KornShell script (`r_legacy_ksh_dwh`) and the migrated Python script (`r_legacy_ksh_dwh.py`) handle the `-h` and `--help` arguments identically, producing equivalent usage instructions and exiting with status code `0`.

### Setup
*   The legacy script is available at `/vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh`.
*   The migrated Python script is available at `/vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh.py`.
*   Python 3.x is installed in the test execution environment.

### Action
Execute both scripts with the `-h` and `--help` flags and capture their standard output (`stdout`), standard error (`stderr`), and exit codes.

### Pass/Fail Criterion
*   **Pass**: Both scripts exit with code `0`. The stdout of both scripts contains the exact usage text:
    ```text
    Usage: <script_name> [-h]
      Runs the legacy_ksh_dwh export.
    ```
*   **Fail**: Any script exits with a non-zero code, or the output text differs (excluding the script name itself).

### Test Code
```python
import subprocess
import os
import pytest

LEGACY_PATH = "/vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh"
MIGRATED_PATH = "/vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh.py"

@pytest.mark.parametrize("script_path, interpreter", [
    (LEGACY_PATH, ["ksh"]),
    (MIGRATED_PATH, ["python3"])
])
@pytest.mark.parametrize("flag", ["-h", "--help"])
def test_help_argument_parity(script_path, interpreter, flag):
    if not os.path.exists(script_path):
        pytest.skip(f"Script {script_path} not found in this test environment.")

    cmd = interpreter + [script_path, flag]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Script failed with exit code {result.returncode}"
    assert "Usage:" in result.stdout
    assert "Runs the legacy_ksh_dwh export." in result.stdout
    assert result.stderr == ""
```

---

## Test Case 2: Successful Execution & Log Parity
### Purpose
Verify that a standard execution (without arguments) of both the legacy and migrated scripts results in a successful run, producing equivalent log messages and exiting with status code `0`.

### Setup
*   For the legacy script, ensure that a mock `sqlplus` executable is available in the test environment's `PATH` that returns `0` (since the actual database connection is not under test here).
*   For the migrated script, ensure `MOCK_FAIL_LEGACY_DWH` is **not** set to `1`.

### Action
Execute both scripts without arguments and capture the log outputs.

### Pass/Fail Criterion
*   **Pass**: Both scripts exit with code `0`. The log output contains the start message and the completion message with the correct timestamp format (`YYYY-MM-DD HH:MM:SS`).
*   **Fail**: Any script exits with a non-zero code, or the log messages do not match the expected patterns:
    *   `Starting legacy_ksh_dwh export`
    *   `legacy_ksh_dwh export completed`

### Test Code
```python
import subprocess
import os
import re
import pytest

@pytest.mark.parametrize("script_path, interpreter", [
    (LEGACY_PATH, ["ksh"]),
    (MIGRATED_PATH, ["python3"])
])
def test_successful_execution_parity(script_path, interpreter):
    if not os.path.exists(script_path):
        pytest.skip(f"Script {script_path} not found.")

    # Ensure clean environment for Python script
    env = os.environ.copy()
    env.pop("MOCK_FAIL_LEGACY_DWH", None)

    cmd = interpreter + [script_path]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    assert result.returncode == 0, f"Execution failed: {result.stderr}"
    
    # Validate log format: YYYY-MM-DD HH:MM:SS <Message>
    log_pattern_start = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} Starting legacy_ksh_dwh export"
    log_pattern_end = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} legacy_ksh_dwh export completed"

    assert re.search(log_pattern_start, result.stdout), "Start log message missing or malformed"
    assert re.search(log_pattern_end, result.stdout), "Completion log message missing or malformed"
```

---

## Test Case 3: Failure Handling and Exit Code Parity
### Purpose
Verify that when the underlying export process fails, both the legacy and migrated scripts log the exact error message and propagate a non-zero exit code (`1`) to the caller.

### Setup
*   **Legacy**: Configure the mock `sqlplus` executable in the `PATH` to return exit code `1`.
*   **Migrated**: Set the environment variable `MOCK_FAIL_LEGACY_DWH=1` to simulate a database failure.

### Action
Execute both scripts and capture the exit codes and standard output logs.

### Pass/Fail Criterion
*   **Pass**: Both scripts exit with code `1`. The log output contains the exact error message:
    `ERROR: legacy_ksh_dwh export failed`
*   **Fail**: Any script exits with code `0` or logs a different error message.

### Test Code
```python
import subprocess
import os
import re
import pytest

@pytest.mark.parametrize("script_path, interpreter, env_vars", [
    # For legacy, we assume the test environment PATH has a failing mock sqlplus
    (LEGACY_PATH, ["ksh"], {}),
    # For migrated, we trigger the failure via the design-specified env var
    (MIGRATED_PATH, ["python3"], {"MOCK_FAIL_LEGACY_DWH": "1"})
])
def test_failure_handling_parity(script_path, interpreter, env_vars):
    if not os.path.exists(script_path):
        pytest.skip(f"Script {script_path} not found.")

    env = os.environ.copy()
    env.update(env_vars)

    cmd = interpreter + [script_path]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    assert result.returncode == 1, f"Expected exit code 1, got {result.returncode}"
    
    log_pattern_err = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} ERROR: legacy_ksh_dwh export failed"
    assert re.search(log_pattern_err, result.stdout), "Error log message missing or malformed"
```

---

## Test Case 4: Airflow DAG Structure and Metadata Validation
### Purpose
Verify that the migrated Airflow DAG (`dw_exttest_legacy_dwh`) is structurally correct, contains the expected task, uses the correct operator, and passes the required environment variables.

### Setup
*   The Airflow DAG file `DW.EXTTEST_LEGACY_DWH.py` is placed in the Airflow `dags/` directory or loaded into a `DagBag` for testing.
*   Airflow variables `GCP_PROJECT` and `GCS_BUCKET` are mocked or set in the test environment.

### Action
Parse the DAG file using the Airflow `DagBag` and inspect the properties of the DAG and its tasks.

### Pass/Fail Criterion
*   **Pass**: 
    *   The DAG parses without import errors.
    *   The DAG ID is exactly `dw_exttest_legacy_dwh`.
    *   The DAG has exactly one task with ID `dw_exttest_legacy_dwh_task`.
    *   The task is an instance of `BashOperator`.
    *   The task's environment dictionary (`env`) contains `"DWH_JOB_KENNUNG": "EXTTEST_LEGACY_DWH"`.
    *   The DAG schedule is `None` (externally triggered).
*   **Fail**: Any of the structural assertions fail, or the DAG fails to parse.

### Test Code
```python
import os
import pytest
from airflow.models import DagBag, Variable

# Mock Airflow Variables required by the DAG during parsing
@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    variables = {
        "GCP_PROJECT": "test-gcp-project",
        "GCS_BUCKET": "test-gcs-bucket"
    }
    def mock_get(key, default_var=None):
        return variables.get(key, default_var)
    monkeypatch.setattr(Variable, "get", mock_get)

def test_airflow_dag_structure():
    dag_file_path = os.path.join(
        os.path.dirname(__file__), 
        "../vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.py"
    )
    
    if not os.path.exists(dag_file_path):
        pytest.skip(f"DAG file not found at {dag_file_path}")

    dagbag = DagBag(dag_folder=os.path.dirname(dag_file_path), include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

    dag_id = "dw_exttest_legacy_dwh"
    assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag"

    dag = dagbag.get_dag(dag_id)
    assert dag.schedule_interval is None, "DAG schedule must be None (externally triggered)"
    assert dag.max_active_runs == 1, "max_active_runs must be 1"

    task_id = "dw_exttest_legacy_dwh_task"
    assert task_id in dag.task_ids, f"Task {task_id} missing from DAG"

    task = dag.get_task(task_id)
    from airflow.operators.bash import BashOperator
    assert isinstance(task, BashOperator), f"Task must be a BashOperator, got {type(task)}"
    
    # Verify environment variable propagation
    assert "DWH_JOB_KENNUNG" in task.env, "DWH_JOB_KENNUNG missing from task env"
    assert task.env["DWH_JOB_KENNUNG"] == "EXTTEST_LEGACY_DWH", "DWH_JOB_KENNUNG has incorrect value"
```