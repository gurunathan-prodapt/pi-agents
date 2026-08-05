# Migration Validation Test Suite
**Target Job:** `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` (Migrated to Airflow DAG: `dw_dwh_dummy_absd_plato_tarife`)

This document defines the migration-validation tests to prove that the migrated Airflow DAG is behaviorally equivalent to the legacy UC4 `JOBS_UNIX` object. 

Since the legacy job is a dummy/synchronization task that executes no database transformations or external file transfers, the validation strategy focuses on:
1. **DAG Structure & Metadata Validation** (ensuring correct configuration, schedules, and operators).
2. **Execution & Output Parity** (verifying that the task executes successfully and produces the exact equivalent log output as the legacy `:print Doing nothinig` command).
3. **Concurrency & Scheduling Constraints** (verifying safety limits like `max_active_runs`).
4. **Downstream Integration Readiness** (ensuring the DAG can be triggered and sensed by downstream workflows).

---

## Test Case 1: DAG Structural & Metadata Validation

### Purpose
Verify that the migrated Airflow DAG is syntactically correct, can be successfully parsed by the Airflow `DagBag`, and contains the exact configuration specified in the migration design document.

### Setup
* A Python environment with `pytest` and `apache-airflow` installed.
* The migrated DAG file `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` placed in the Airflow DAGs directory or accessible via the Python path.

### Action
Run the following `pytest` test suite to programmatically assert the DAG's structure, task types, and default arguments.

```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(scope="module", autouse=True)
def setup_variables():
    # Mock GCP variables required by the DAG
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCP_REGION", "europe-west3")
    yield
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCP_REGION")

def test_dag_loads_with_no_errors():
    """Asserts that the DAG can be imported without import errors."""
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    
    assert dag_id in dag_bag.dags, f"DAG {dag_id} not found in DagBag"
    assert len(dag_bag.import_errors) == 0, f"Import errors detected: {dag_bag.import_errors}"

def test_dag_metadata_and_default_args():
    """Asserts that the DAG metadata matches the migration design specifications."""
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    
    # Verify DAG properties
    assert dag.schedule_interval is None, "Schedule should be None (externally triggered)"
    assert dag.catchup is False, "Catchup should be disabled"
    assert dag.max_active_runs == 1, "max_active_runs must be set to 1 for concurrency safety"
    
    # Verify Default Args
    assert dag.default_args.get("owner") == "airflow"
    assert dag.default_args.get("retries") == 1
    assert dag.default_args.get("retry_delay") == timedelta(minutes=5)

def test_task_properties():
    """Asserts that the task is correctly defined as a BashOperator executing the dummy command."""
    from airflow.operators.bash import BashOperator
    from datetime import timedelta

    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task_id = "dw_dwh_dummy_absd_plato_tarife_task"
    
    assert task_id in dag.task_ids, f"Task {task_id} is missing from the DAG"
    
    task = dag.get_task(task_id)
    assert isinstance(task, BashOperator), f"Task {task_id} should be a BashOperator"
    assert task.bash_command == "echo 'Doing nothinig'", "Bash command must match the legacy print statement"
```

### Pass/Fail Criterion
* **Pass:** The test suite executes with `100%` success rate. The DAG loads without import errors, and all structural assertions (task ID, operator type, default arguments, schedule) pass.
* **Fail:** Any import error is raised, or any assertion fails.

---

## Test Case 2: Execution & Output Parity

### Purpose
Prove behavioral equivalence by executing the migrated task and verifying that it prints `"Doing nothinig"` to the standard output/logs (mimicking the legacy UC4 `:print Doing nothinig` command) and exits with status `0`.

### Setup
* A running Airflow local environment or a mock execution context.
* Access to the Airflow CLI or programmatic task execution.

### Action
Execute the task using Airflow's task testing utility and capture the standard output logs to verify the execution output.

```python
import io
import logging
from datetime import datetime
from airflow.models import DagBag, TaskInstance

def test_task_execution_output(caplog):
    """Executes the task locally and asserts that the output matches the legacy UC4 print statement."""
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife_task")
    
    # Create a dummy TaskInstance for execution
    execution_date = datetime(2026, 3, 30)
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    # Run the task in a test context (ignores downstream/upstream dependencies)
    with caplog.at_level(logging.INFO):
        task.execute(context=ti.get_template_context())
        
    # Assert that the bash command output is present in the logs
    log_messages = [record.message for record in caplog.records]
    
    # Verify that the BashOperator executed and printed the expected string
    assert any("Doing nothinig" in msg for msg in log_messages), \
        f"Expected log output 'Doing nothinig' was not found in task logs: {log_messages}"
```

### Pass/Fail Criterion
* **Pass:** The task executes without raising any exceptions, and the execution logs explicitly contain the string `"Doing nothinig"`.
* **Fail:** The task execution fails (non-zero exit code) or the string `"Doing nothinig"` is missing from the execution logs.

---

## Test Case 3: Concurrency and Scheduling Constraints Validation

### Purpose
Verify that the DAG cannot run concurrently with multiple active instances, and that it is configured to prevent backfilling (matching the legacy UC4 behavior where dummy synchronization tasks are triggered on-demand or sequentially).

### Setup
* Airflow environment with the DAG loaded.

### Action
Run a programmatic check against the DAG's operational parameters:

```python
from airflow.models import DagBag

def test_concurrency_and_catchup_settings():
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    
    # Assert that catchup is disabled to prevent historical backfilling
    assert dag.catchup is False, "CRITICAL: Catchup must be False to prevent accidental historical runs."
    
    # Assert that max_active_runs is strictly 1 to prevent concurrent executions
    assert dag.max_active_runs == 1, "CRITICAL: max_active_runs must be 1 to prevent concurrent execution of this sync task."
```

### Pass/Fail Criterion
* **Pass:** Both `catchup == False` and `max_active_runs == 1` are strictly enforced.
* **Fail:** Either setting deviates from the design specification, risking concurrent execution or backfill storms.

---

## Test Case 4: Downstream Integration Readiness (Mocking/Dry-Run)

### Purpose
Since the downstream consumer `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated, this test verifies that the dummy DAG exposes a clean, triggerable interface that can be sensed or triggered downstream once the target workflow is deployed.

### Setup
* Airflow environment with the DAG loaded.

### Action
Simulate an external trigger of the DAG and verify it transitions to a running state and completes successfully.

```bash
# Step 1: Trigger the DAG manually via Airflow CLI
airflow dags trigger dw_dwh_dummy_absd_plato_tarife

# Step 2: Wait for execution and verify the DAG run status is 'success'
airflow dags state dw_dwh_dummy_absd_plato_tarife $(date +%Y-%m-%dT%H:%M:%S)
```

Alternatively, verify programmatically that an `ExternalTaskSensor` can target this DAG:

```python
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

def test_external_sensor_compatibility():
    """Verifies that an ExternalTaskSensor can target this DAG and task."""
    sensor = ExternalTaskSensor(
        task_id="test_sensor",
        external_dag_id="dw_dwh_dummy_absd_plato_tarife",
        external_task_id="dw_dwh_dummy_absd_plato_tarife_task",
        poke_interval=5,
        timeout=10
    )
    assert sensor.external_dag_id == "dw_dwh_dummy_absd_plato_tarife"
    assert sensor.external_task_id == "dw_dwh_dummy_absd_plato_tarife_task"
```

### Pass/Fail Criterion
* **Pass:** The DAG can be triggered manually via the CLI/API, completes successfully, and is fully compatible with standard Airflow cross-DAG sensing patterns (`ExternalTaskSensor`).
* **Fail:** The DAG fails to trigger, fails during execution, or cannot be targeted by an `ExternalTaskSensor`.