# Migration Validation Test Suite: `DW.DWH_EXIS_CPDW_DIRECT`

This document defines the migration-validation tests to prove behavioral equivalence between the legacy UC4 job `DW.DWH_EXIS_CPDW_DIRECT` and its migrated Airflow DAG `dw_dwh_exis_cpdw_direct`.

---

## Test Case 1: DAG Structure and Parameter Validation

### Purpose
Verify that the migrated Airflow DAG is correctly structured, contains the required parameters, matches the concurrency limits (`max_active_runs=1`), and is configured as an unscheduled workflow (`schedule=None`) to act as a callable task.

### Setup
* Access to the Airflow environment where the migrated DAG `dw_dwh_exis_cpdw_direct` is deployed.
* Python environment with `pytest` and `apache-airflow` installed.

### Action
Run a unit test using the Airflow `DagBag` to inspect the DAG's properties and parameters.

```python
import pytest
from airflow.models import DagBag

def test_dag_structure_and_properties():
    dag_bag = DagBag(include_examples=False)
    dag_id = "dw_dwh_exis_cpdw_direct"
    
    # Assert DAG exists
    assert dag_id in dag_bag.dags, f"DAG {dag_id} not found in DagBag"
    dag = dag_bag.get_dag(dag_id)
    
    # Assert Scheduling and Concurrency
    assert dag.schedule_interval is None, "DAG must be unscheduled (schedule=None)"
    assert dag.max_active_runs == 1, "max_active_runs must be set to 1"
    assert dag.catchup is False, "catchup must be False"
    
    # Assert Parameters
    assert "DWH_JOB_KENNUNG" in dag.params, "DWH_JOB_KENNUNG parameter is missing"
    assert dag.params["DWH_JOB_KENNUNG"] == "EXIS_CPDW_DIRECT", "DWH_JOB_KENNUNG value mismatch"
    
    # Assert Task Inventory
    task_id = "dwh_exis_cpdw_direct"
    assert dag.has_task(task_id), f"Task {task_id} is missing from the DAG"
```

### Pass/Fail Criterion
* **Pass**: The test executes successfully with no assertion errors.
* **Fail**: Any assertion fails (e.g., the DAG is scheduled, has incorrect parameters, or is missing the core task).

---

## Test Case 2: Command Execution & Environment Setup (Behavioral Equivalence)

### Purpose
The legacy job executes a custom binary with specific arguments: `$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp`. It also sources `.dw_init` and sets `DWH_JOB_KENNUNG`. 

Since the generated code uses an `EmptyOperator` placeholder, this test validates that once the operator is resolved to an execution operator (e.g., `SSHOperator` or `BashOperator`), it correctly executes the equivalent environment setup and binary command.

### Setup
* The `EmptyOperator` in the migrated DAG is replaced with the target execution operator (e.g., `SSHOperator` targeting the legacy host `dwhdwh1p` or a containerized `BashOperator`).
* A test environment where the Airflow task can be rendered.

### Action
Render the task templates and assert that the command executed matches the legacy shell execution sequence.

```python
import pytest
from airflow.models import DagBag, TaskInstance
from datetime import datetime

def test_rendered_command_equivalence():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_exis_cpdw_direct")
    task = dag.get_task("dwh_exis_cpdw_direct")
    
    # If the operator is still an EmptyOperator, this test should fail to force implementation
    assert task.__class__.__name__ != "EmptyOperator", (
        "Task 'dwh_exis_cpdw_direct' is still an EmptyOperator. "
        "It must be migrated to SSHOperator, BashOperator, or a custom operator."
    )
    
    # Create a dummy TaskInstance to render templates
    ti = TaskInstance(task=task, execution_date=datetime(2023, 1, 1))
    
    # Extract the command attribute (adapts to SSHOperator or BashOperator)
    command = getattr(task, "command", None) or getattr(task, "bash_command", None)
    assert command is not None, "Could not extract execution command from task"
    
    # Assert environment setup and binary execution are present in the command
    assert ". $HOME/.dw_init" in command, "Command does not source .dw_init"
    assert "EXIS_CPDW_DIRECT" in command, "Command does not set DWH_JOB_KENNUNG"
    assert "r_exis -s cpdw -r direct -f sftp" in command, "Command does not execute r_exis with correct flags"
```

### Pass/Fail Criterion
* **Pass**: The task is no longer an `EmptyOperator`, and its rendered command contains the environment initialization, the correct job identifier, and the exact `r_exis` execution string.
* **Fail**: The task remains an `EmptyOperator`, or the rendered command lacks the required initialization or execution parameters.

---

## Test Case 3: Source Data Extraction Parity (Pre-SFTP)

### Purpose
Verify that the lookup data extracted by the migrated process is identical in schema, row count, and content to the data extracted by the legacy process prior to SFTP transmission.

### Setup
1. Run the legacy job `DW.DWH_EXIS_CPDW_DIRECT` on the legacy host `dwhdwh1p` against a static test database instance. Capture the generated export file (e.g., `/tmp/legacy_cpdw_export.csv`).
2. Run the migrated Airflow task against the same static database instance. Capture the generated export file (e.g., `/tmp/migrated_cpdw_export.csv`).

### Action
Execute a comparison script to validate file integrity, schema, row count, and data parity.

```python
import hashlib
import pandas as pd
import pytest

def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def test_data_extraction_parity():
    legacy_file = "/tmp/legacy_cpdw_export.csv"
    migrated_file = "/tmp/migrated_cpdw_export.csv"
    
    # 1. Row Count and Schema Validation
    df_legacy = pd.read_csv(legacy_file, sep=None, engine='python')
    df_migrated = pd.read_csv(migrated_file, sep=None, engine='python')
    
    assert df_legacy.shape == df_migrated.shape, (
        f"Shape mismatch! Legacy: {df_legacy.shape}, Migrated: {df_migrated.shape}"
    )
    
    # 2. Column Name and Type Validation
    assert list(df_legacy.columns) == list(df_migrated.columns), "Column headers do not match"
    for col in df_legacy.columns:
        assert df_legacy[col].dtype == df_migrated[col].dtype, f"Data type mismatch in column {col}"
        
    # 3. Sort and Checksum Validation (to ensure absolute data parity)
    df_legacy_sorted = df_legacy.sort_values(by=list(df_legacy.columns)).reset_index(drop=True)
    df_migrated_sorted = df_migrated.sort_values(by=list(df_migrated.columns)).reset_index(drop=True)
    
    legacy_sorted_path = "/tmp/legacy_sorted.csv"
    migrated_sorted_path = "/tmp/migrated_sorted.csv"
    
    df_legacy_sorted.to_csv(legacy_sorted_path, index=False)
    df_migrated_sorted.to_csv(migrated_sorted_path, index=False)
    
    assert calculate_md5(legacy_sorted_path) == calculate_md5(migrated_sorted_path), (
        "Data content mismatch between legacy and migrated exports!"
    )
```

### Pass/Fail Criterion
* **Pass**: The extracted files have identical schemas, identical row counts, and matching MD5 checksums when sorted.
* **Fail**: Any mismatch in row count, column types, or sorted MD5 checksums is detected.

---

## Test Case 4: SFTP Transfer Validation (External System Replacement)

### Purpose
Verify that the migrated job successfully transfers the exported lookup data to the target CPDW SFTP server, matching the legacy transfer behavior (target directory, file naming conventions, and file permissions).

### Setup
* A test SFTP server configured to mimic the CPDW target environment.
* SFTP credentials configured in the Airflow Connection manager (e.g., `cpdw_sftp_conn`).

### Action
1. Execute the migrated Airflow DAG.
2. Connect to the target SFTP server and verify the transferred file.

```python
import paramiko
import pytest
from airflow.hooks.base import BaseHook

def test_sftp_delivery():
    # Retrieve connection details from Airflow
    conn = BaseHook.get_connection("cpdw_sftp_conn")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    ssh.connect(
        hostname=conn.host,
        port=conn.port or 22,
        username=conn.login,
        password=conn.password
    )
    
    sftp = ssh.open_sftp()
    
    # Define target directory based on CPDW specifications
    target_dir = "/target/cpdw/direct/"
    sftp.chdir(target_dir)
    
    # List files and find the transferred export file
    files = sftp.listdir_attr()
    
    # Filter files matching the expected naming pattern (e.g., lookup_export_*.csv)
    export_files = [f for f in files if f.filename.startswith("lookup_export_")]
    
    assert len(export_files) > 0, "No export file found on the target SFTP server"
    
    # Get the latest file
    latest_file = sorted(export_files, key=lambda x: x.st_mtime)[-1]
    
    # Assert file is not empty
    assert latest_file.st_size > 0, f"Transferred file {latest_file.filename} is empty (0 bytes)"
    
    # Assert file permissions (e.g., -rw-r----- / 0o640)
    # 0o640 octal is 416 decimal
    assert (latest_file.st_mode & 0o777) == 0o640, (
        f"Incorrect file permissions: {oct(latest_file.st_mode & 0o777)}"
    )
    
    sftp.close()
    ssh.close()
```

### Pass/Fail Criterion
* **Pass**: The file is successfully located in the target CPDW SFTP directory, has a non-zero size, matches the expected naming pattern, and has the correct file permissions.
* **Fail**: The file is missing, empty, has incorrect permissions, or the SFTP connection fails.

---

## Test Case 5: Error Handling and Log Parsing (Equivalent to `DW.LESE_LOG`)

### Purpose
Verify that the migrated job handles execution failures (e.g., database offline, SFTP connection failure) correctly by failing the task, triggering retries, and logging descriptive errors as the legacy `:inc DW.LESE_LOG` would.

### Setup
* Temporarily alter the SFTP connection parameters in Airflow to use an invalid port or host to simulate a network failure.

### Action
1. Trigger the migrated Airflow DAG `dw_dwh_exis_cpdw_direct`.
2. Monitor the task execution state and inspect the logs.

```python
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType

def test_error_handling_on_failure(cli_runner):
    # Trigger the DAG run
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_exis_cpdw_direct")
    
    # Trigger a DAG run with invalid SFTP connection overrides
    dag_run = dag.create_dagrun(
        run_id="test_failure_run",
        state=State.RUNNING,
        execution_date=datetime.utcnow(),
        run_type=DagRunType.MANUAL,
        conf={"cpdw_sftp_conn_id": "invalid_sftp_connection"}
    )
    
    # Run the task and expect it to fail
    ti = dag_run.get_task_instance("dwh_exis_cpdw_direct")
    
    with pytest.raises(Exception):
        ti.run(ignore_ti_state=True, ignore_task_deps=True)
        
    # Refresh task instance state
    ti.refresh_from_db()
    
    # Assert task state is UP_FOR_RETRY (since retries=1 is configured in DEFAULT_ARGS)
    assert ti.state == State.UP_FOR_RETRY, (
        f"Expected task state to be UP_FOR_RETRY, but got {ti.state}"
    )
```

### Pass/Fail Criterion
* **Pass**: The task fails on the initial attempt, transitions to `UP_FOR_RETRY` (respecting the `retries: 1` configuration), and writes a clear error message to the Airflow task logs.
* **Fail**: The task completes successfully despite the invalid connection, or fails without scheduling a retry.