# Migration Validation Test Suite
**Job under test:** `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` (Migrated to `dw_dwh_dummy_absd_plato_tarife`)

This document defines the migration-validation tests to prove that the migrated Airflow DAG is behaviorally equivalent to the legacy UC4 Unix job. Since this is a utility/dummy milestone task, validation focuses on structural integrity, execution state parity, log output correctness, and idempotency.

---

## Test Case 1: DAG Structure and Metadata Validation

### Purpose
Verify that the migrated Airflow DAG is parsed correctly by the Airflow engine and matches the structural properties, schedules, and configurations defined in the migration design document.

### Setup
1. Ensure `pytest` and `apache-airflow` are installed in the test environment.
2. Place the migrated DAG file `DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` in the Airflow DAGs directory or add it to the python path.
3. Set dummy environment variables to prevent import-time failures:
   ```bash
   export GCP_PROJECT="test-gcp-project"
   export DATAPROC_REGION="europe-west3"
   export DATAPROC_CLUSTER="test-cluster"
   export GCS_BUCKET="test-bucket"
   ```

### Action
Run a pytest suite that loads the DAG and asserts its structural attributes against the legacy specifications.

```python
import pytest
from airflow.models import DagBag

def test_dag_metadata_and_structure():
    # Load the DAGs
    dagbag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    
    # 1. Assert no import errors occurred
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    # 2. Assert DAG existence
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} not found in DagBag"
    
    # 3. Assert DAG Properties
    assert dag.schedule_interval is None, "Schedule should be None (manual/triggered)"
    assert dag.catchup is False, "Catchup should be disabled (False)"
    assert dag.max_active_runs == 1, "Max active runs should be constrained to 1"
    assert dag.tags == ['migrated_uc4', 'dummy_job'], "DAG tags do not match specification"
    
    # 4. Assert Task Inventory
    task_id = "dwh_dummy_absd_plato_tarife"
    assert task_id in dag.task_ids, f"Task {task_id} is missing from the DAG"
    
    task = dag.get_task(task_id)
    from airflow.operators.python import PythonOperator
    assert isinstance(task, PythonOperator), f"Task {task_id} must be a PythonOperator"
    
    # 5. Assert Default Args
    assert task.retries == 0, "Retries should be set to 0 as per legacy configuration"
    assert task.owner == "airflow", "Owner should be 'airflow'"
```

### Pass/Fail Criterion
* **Pass:** The DAG imports without errors, contains exactly one task named `dwh_dummy_absd_plato_tarife` of type `PythonOperator`, has `schedule=None`, `retries=0`, and `max_active_runs=1`.
* **Fail:** Any of the structural assertions fail, or the DAG fails to import due to syntax or environment errors.

---

## Test Case 2: Behavioral Equivalence & Log Output Parity

### Purpose
Verify that executing the migrated task produces the exact same behavioral outcome as the legacy UC4 job—specifically, logging the verbatim string `"Doing nothinig"` (including the legacy typo) and completing successfully.

### Setup
1. Initialize a local Airflow metadata database or mock the execution context.
2. Configure a standard Python logging handler to capture standard output during task execution.

### Action
Execute the task directly within a test harness and capture the logs.

```python
import logging
import pytest
from airflow.models import TaskInstance
from datetime import datetime
from uc4_airflow_linked_job.DW_DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DWH_DUMMY_ABSD_PLATO_TARIFE import dag, log_dummy_action

def test_task_execution_and_log_output(caplog):
    # 1. Get the task object
    task = dag.get_task("dwh_dummy_absd_plato_tarife")
    
    # 2. Execute the python callable directly within the log-capturing context
    with caplog.at_level(logging.INFO):
        task.python_callable()
        
    # 3. Assert that the exact legacy print statement was logged
    expected_log = "Doing nothinig"
    log_messages = [record.message for record in caplog.records]
    
    assert any(expected_log in msg for msg in log_messages), \
        f"Expected log message '{expected_log}' was not found in captured logs: {log_messages}"

def test_task_instance_run_success():
    # Simulate a full TaskInstance execution run
    execution_date = datetime(2026, 3, 30)
    task = dag.get_task("dwh_dummy_absd_plato_tarife")
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    # Run the task instance without database commit side-effects
    ti.run(ignore_ti_state=True, mark_success=False, test_mode=True)
    
    # Assert task ended with success state
    assert ti.state == "success", f"Task failed with state: {ti.state}"
```

### Pass/Fail Criterion
* **Pass:** The task executes without raising exceptions, its execution state is marked as `success`, and the logs contain the exact string `"Doing nothinig"`.
* **Fail:** The task execution fails, throws an exception, or the log output does not contain the verbatim legacy string.

---

## Test Case 3: Idempotency & Recovery Validation

### Purpose
Prove the legacy operational assertion: *"Wiederanlauf ohne weitere Maßnahmen möglich"* (Restart possible without further measures). The task must be completely idempotent, meaning it can be executed repeatedly without causing side effects, database locks, or state pollution.

### Setup
1. Ensure the test environment has no persistent state from previous runs.

### Action
Trigger the DAG task multiple times consecutively and verify that all runs succeed independently and identically.

```python
import pytest
from airflow.models import TaskInstance
from datetime import datetime, timedelta
from uc4_airflow_linked_job.DW_DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DWH_DUMMY_ABSD_PLATO_TARIFE import dag

def test_task_idempotency():
    task = dag.get_task("dwh_dummy_absd_plato_tarife")
    base_date = datetime(2026, 3, 30)
    
    # Execute the task 3 times consecutively simulating manual retries/re-runs
    for run_id in range(3):
        execution_date = base_date + timedelta(hours=run_id)
        ti = TaskInstance(task=task, execution_date=execution_date)
        
        try:
            ti.run(ignore_ti_state=True, mark_success=False, test_mode=True)
        except Exception as e:
            pytest.fail(f"Idempotency run {run_id} failed with exception: {e}")
            
        assert ti.state == "success", f"Run {run_id} failed to reach success state."
```

### Pass/Fail Criterion
* **Pass:** All consecutive runs complete successfully with state `success` without requiring manual cleanup, state resets, or intervention.
* **Fail:** Any of the consecutive runs fail, hang, or throw an exception.

---

## Test Case 4: Environment Variable Resilience

### Purpose
Verify that the DAG is decoupled from external infrastructure. Since the legacy Unix host (`|DWHDWH1P|HOST`) and login (`DW.UNIX.ISTNS`) are retired, the DAG must load and execute successfully even if standard GCP environment variables are missing or empty (as the dummy task does not utilize them).

### Setup
1. Clear the environment variables `GCP_PROJECT`, `DATAPROC_REGION`, `DATAPROC_CLUSTER`, and `GCS_BUCKET` from the active shell session.

### Action
Import the DAG module and execute the task to ensure it does not raise `KeyError` or configuration exceptions.

```python
import os
import importlib
import pytest

def test_environment_variable_resilience(monkeypatch):
    # 1. Clear GCP environment variables
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.delenv("DATAPROC_REGION", raising=False)
    monkeypatch.delenv("DATAPROC_CLUSTER", raising=False)
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    
    # 2. Force reload the module to trigger environment variable evaluation
    try:
        import uc4_airflow_linked_job.DW_DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DWH_DUMMY_ABSD_PLATO_TARIFE as dag_module
        importlib.reload(dag_module)
    except Exception as e:
        pytest.fail(f"DAG failed to load when environment variables were missing: {e}")
        
    # 3. Verify variables are evaluated as None without crashing
    assert dag_module.GCP_PROJECT_ID is None
    assert dag_module.GCS_BUCKET is None
    
    # 4. Execute the task to ensure runtime safety
    try:
        dag_module.log_dummy_action()
    except Exception as e:
        pytest.fail(f"Task execution failed when environment variables were missing: {e}")
```

### Pass/Fail Criterion
* **Pass:** The DAG module imports successfully, sets the configuration variables to `None` without raising exceptions, and the task executes successfully.
* **Fail:** The import fails with a `KeyError` or the task execution crashes due to missing environment variables.