# Migration Validation Test Suite: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document defines the migration-validation tests to verify that the migrated Airflow DAG `dw_dwh_dummy_absd_plato_tarife` is behaviorally equivalent to the legacy UC4 job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.

---

## Test Case 1: DAG Structure and Metadata Validation (Static Analysis)

### Purpose
To verify that the migrated Airflow DAG is correctly parsed by the Airflow engine, contains the correct metadata, and maps the legacy parameters (owner, retries, schedule) accurately.

### Setup
* The Airflow environment must have the DAG file `DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` placed in the `dags/` directory.
* Install `pytest` and `apache-airflow` in the test environment.

### Action
Run a pytest suite that loads the `DagBag` and asserts the structural properties of the DAG.

```python
import pytest
from airflow.models import DagBag
from airflow.operators.bash import BashOperator

def test_dag_metadata_and_structure():
    # Load the DAG bag
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    
    # 1. Assert DAG exists and has no import errors
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    assert dag_id in dag_bag.dags, f"DAG {dag_id} failed to load."
    assert len(dag_bag.import_errors) == 0, f"Import errors found: {dag_bag.import_errors}"
    
    dag = dag_bag.get_dag(dag_id)
    
    # 2. Assert DAG Properties
    assert dag.schedule_interval is None, "DAG schedule should be None (manual/external trigger only)."
    assert dag.max_active_runs == 1, "max_active_runs must be 1 to prevent parallel execution conflicts."
    assert dag.catchup is False, "catchup must be False."
    
    # 3. Assert Default Arguments
    assert dag.default_args.get('owner') == 'dw.unix.istns', "Owner must map to legacy login 'dw.unix.istns'."
    assert dag.default_args.get('retries') == 1, "Retries must be configured to 1."
    assert dag.default_args.get('retry_delay') == pytest.approx(300, abs=1), "Retry delay must be 5 minutes (300s)."

    # 4. Assert Task Properties
    task_id = "dw_dwh_dummy_absd_plato_tarife"
    assert task_id in dag.task_ids, f"Task {task_id} is missing from the DAG."
    
    task = dag.get_task(task_id)
    assert isinstance(task, BashOperator), f"Task {task_id} must be a BashOperator."
    
    # Verify the exact command matches the legacy print statement (including the typo 'nothinig')
    assert task.bash_command == 'echo "Doing nothinig"', "Bash command must preserve the legacy print output."
```

### Pass/Fail Criterion
* **Pass**: The test suite executes successfully with zero failures, confirming all metadata, task types, and configurations match the legacy specification.
* **Fail**: Any assertion fails (e.g., incorrect owner, missing task, or modified bash command).

---

## Test Case 2: Execution and Output Parity Validation (Integration Test)

### Purpose
To verify that executing the migrated Airflow task produces the exact same output (`Doing nothinig`) in the execution logs as the legacy UC4 job's `:print Doing nothinig` directive.

### Setup
* A running Airflow local/worker instance or a mocked task instance execution environment.

### Action
Execute the task instance locally and capture the standard output.

```python
import sys
from io import StringIO
from datetime import datetime
from airflow.models import DagBag, TaskInstance

def test_task_execution_output(capsys):
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Create a dummy execution context
    execution_date = datetime(2023, 1, 1)
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    # Run the task
    # Note: In a unit test environment, we can run the task directly.
    # We mock the context to avoid database writes if running in a stateless CI/CD pipeline.
    context = {
        'ti': ti,
        'execution_date': execution_date,
        'ds': '2023-01-01',
    }
    
    # Capture stdout during execution
    old_stdout = sys.stdout
    sys.stdout = mystdout = StringIO()
    
    try:
        task.execute(context=context)
    finally:
        sys.stdout = old_stdout
        
    output = mystdout.getvalue()
    
    # Assert that the bash command executed successfully and printed the exact string
    assert "Doing nothinig" in output, f"Expected output 'Doing nothinig' was not found in task logs. Found: {output}"
```

### Pass/Fail Criterion
* **Pass**: The task executes without errors, and the string `"Doing nothinig"` is successfully printed to the standard output/log stream.
* **Fail**: The task execution fails, or the output does not contain the exact string `"Doing nothinig"`.

---

## Test Case 3: External-System Bypass Validation

### Purpose
To verify that the migrated task executes natively within the Cloud Composer/Airflow environment and does not attempt to connect to the legacy physical host `DWHDWH1P` or use the legacy login credentials `DW.UNIX.ISTNS` over SSH/Telnet.

### Setup
* Ensure no SSH connections or external host configurations are defined in the DAG file.

### Action
Inspect the task configuration to ensure no external connection parameters are injected.

```python
from airflow.models import DagBag

def test_no_external_host_dependencies():
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Ensure no SSH/SFTP operators or external connection parameters are used
    forbidden_attributes = ['ssh_conn_id', 'remote_host', 'conn_id']
    for attr in forbidden_attributes:
        assert not hasattr(task, attr), f"Task contains legacy external infrastructure reference: {attr}"
```

### Pass/Fail Criterion
* **Pass**: The task is confirmed to be a pure local `BashOperator` with no external connection dependencies.
* **Fail**: The task contains references to external hosts, SSH connections, or remote execution parameters.

---

## Test Case 4: Downstream Dependency Readiness (Manual Trigger Verification)

### Purpose
To verify that the DAG is configured for manual triggering (`schedule=None`) and is prepared to be integrated with the downstream workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` once it is migrated.

### Setup
* Load the DAG object.

### Action
Verify the trigger configuration and concurrency limits.

```python
from airflow.models import DagBag

def test_downstream_integration_readiness():
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    
    # Verify manual trigger configuration
    assert dag.schedule_interval is None, "DAG must be set to manual trigger (schedule=None)."
    
    # Verify that max_active_runs is constrained to prevent race conditions
    assert dag.max_active_runs == 1, "max_active_runs must be 1 to ensure sequential execution."
```

### Pass/Fail Criterion
* **Pass**: The DAG is confirmed to be manually triggerable with strict concurrency controls.
* **Fail**: The DAG has an active schedule or allows multiple concurrent runs.