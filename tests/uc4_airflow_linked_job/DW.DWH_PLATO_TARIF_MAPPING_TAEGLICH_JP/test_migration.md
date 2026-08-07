# Migration Validation Test Suite
**Target Job:** `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`  
**Migrated DAG:** `dw_dwh_dummy_absd_plato_tarife_dag`

This document defines the migration-validation tests to prove that the migrated Apache Airflow DAG is behaviorally equivalent to the legacy UC4 UNIX job. 

---

## Test Case 1: DAG Parsing and Metadata Validation

### Purpose
Verify that the migrated Python DAG file is syntactically correct, can be loaded by the Airflow `DagBag` without errors or warnings, and matches the metadata specified in the migration design document.

### Setup
1. Ensure the migrated DAG file `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` is placed in the Airflow `dags/` directory or a path accessible by the test runner.
2. Install `pytest` and `apache-airflow` in the test environment.

### Action
Run the following `pytest` test suite to programmatically assert DAG-level configurations:

```python
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    # Load the DAGs from the default or specified directory
    return DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)

def test_dag_loading_no_errors(dagbag):
    """Assert that the DAG loads without import errors."""
    dag_id = "dw_dwh_dummy_absd_plato_tarife_dag"
    assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag."
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"

def test_dag_metadata(dagbag):
    """Assert that the DAG metadata matches the migration design specifications."""
    dag = dagbag.get_dag("dw_dwh_dummy_absd_plato_tarife_dag")
    
    assert dag.schedule_interval is None, "Schedule should be None (externally triggered)."
    assert dag.catchup is False, "Catchup should be set to False."
    assert dag.max_active_runs == 1, "max_active_runs must be restricted to 1."
    assert "migrated_uc4" in dag.tags, "Missing 'migrated_uc4' tag."
    assert "dummy" in dag.tags, "Missing 'dummy' tag."
    assert dag.default_args.get("retries") == 1, "Default retries should be 1."
```

### Pass/Fail Criterion
* **Pass:** The test suite executes successfully with zero failures and zero import errors.
* **Fail:** Any import errors are raised, or any metadata assertions (such as schedule, catchup, or tags) fail.

---

## Test Case 2: Task Operator and Command Validation

### Purpose
The legacy UC4 job executed an internal script command `:print Doing nothinig` (including the typo). The design document suggested an `EmptyOperator`, but the generated migration code implemented a `BashOperator` executing `echo 'Doing nothinig'`. This test ensures that the task is correctly instantiated as a `BashOperator` and contains the exact command to replicate the legacy print statement.

### Setup
The same environment as Test Case 1.

### Action
Run the following test to validate task-level properties:

```python
def test_task_operator_and_command(dagbag):
    """Assert that the task is a BashOperator and executes the correct echo command."""
    dag = dagbag.get_dag("dw_dwh_dummy_absd_plato_tarife_dag")
    task_id = "dw_dwh_dummy_absd_plato_tarife"
    
    assert dag.has_task(task_id), f"Task {task_id} is missing from the DAG."
    task = dag.get_task(task_id)
    
    # Verify operator type
    assert task.task_type == "BashOperator", f"Expected BashOperator, got {task.task_type}"
    
    # Verify command parity (including the legacy typo 'nothinig')
    assert task.bash_command.strip() == "echo 'Doing nothinig'", \
        f"Bash command mismatch. Expected \"echo 'Doing nothinig'\", got \"{task.bash_command}\""
```

### Pass/Fail Criterion
* **Pass:** The task exists, is a `BashOperator`, and its command is exactly `echo 'Doing nothinig'`.
* **Fail:** The task is missing, uses a different operator, or the bash command does not match the legacy print statement.

---

## Test Case 3: Execution and Log Output Parity

### Purpose
Verify that running the task in an Airflow execution context succeeds and writes the expected string to the execution logs, proving behavioral equivalence to the legacy UC4 `:print` directive.

### Setup
1. A local Airflow development/test environment (e.g., LocalExecutor or CeleryExecutor).
2. Access to the Airflow CLI or the metadata database.

### Action
Execute the task using the Airflow CLI and capture the standard output:

```bash
# Run a local test execution of the specific task
airflow tasks test dw_dwh_dummy_absd_plato_tarife_dag dw_dwh_dummy_absd_plato_tarife 2023-01-01
```

Alternatively, run via a Python integration test:

```python
def test_task_execution_logging(capsys):
    """Execute the task locally and assert that 'Doing nothinig' is printed to stdout."""
    from airflow.models import TaskInstance
    from airflow.utils.state import DagRunState, TaskInstanceState
    from airflow.utils.types import DagRunType
    from datetime import datetime
    from uc4_airflow_linked_job.DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DWH_DUMMY_ABSD_PLATO_TARIFE import dag
    
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Create a dummy TaskInstance context
    ti = TaskInstance(task=task, execution_date=datetime(2023, 1, 1))
    
    # Run the task
    ti.run(ignore_ti_state=True, test_mode=True)
    
    # Assert task completed successfully
    assert ti.state == TaskInstanceState.SUCCESS
```

### Pass/Fail Criterion
* **Pass:** The task execution state is `SUCCESS`, and the execution logs contain the line:
  `Running command: echo 'Doing nothinig'` followed by the output `Doing nothinig`.
* **Fail:** The task fails, times out, or the string `Doing nothinig` is missing from the execution logs.

---

## Test Case 4: External-System Retirement Validation

### Purpose
The legacy UC4 job was configured to run on host `|DWHDWH1P|HOST` using login credentials `DW.UNIX.ISTNS`. Because this job is a dummy milestone, these external system dependencies must be retired. This test proves that the migrated task executes entirely within the Cloud Composer/Airflow worker environment without attempting to establish SSH, SFTP, or database connections to the legacy host.

### Setup
An Airflow environment with no connections configured for `|DWHDWH1P|HOST` or `DW.UNIX.ISTNS`.

### Action
1. Inspect the task definition in the DAG file.
2. Trigger a manual run of the DAG via the Airflow UI or CLI:
   ```bash
   airflow dags trigger dw_dwh_dummy_absd_plato_tarife_dag
   ```
3. Monitor the execution of the DAG run.

### Pass/Fail Criterion
* **Pass:** The DAG run transitions to `SUCCESS` in under 5 seconds (well within the legacy ERT of 11 seconds) without throwing connection errors, authentication failures, or missing connection warnings.
* **Fail:** The task attempts to resolve an external connection, hangs waiting for a remote host, or fails due to missing SSH/Login credentials.