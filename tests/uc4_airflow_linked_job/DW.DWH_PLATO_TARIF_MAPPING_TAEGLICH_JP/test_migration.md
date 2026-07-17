# Migration Validation Test Suite: `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`

This document outlines the test cases required to validate the migration of the UC4 UNIX Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to its Cloud Composer (Airflow) equivalent. 

Since this is a structural dummy milestone task, the validation focus is on **metadata compliance, typographical preservation, execution idempotency, and DAG structural integrity**.

---

## Section 1: Output Parity & Typographical Preservation

### Purpose
To verify that the migrated Airflow task outputs the exact string from the legacy UC4 script (`:print Doing nothinig`), preserving the typographical error verbatim to prevent breaking downstream regex log monitors.

### Setup
* A local or development Airflow environment with the migrated DAG `dw_dwh_dummy_absd_plato_tarife` loaded.
* The `pytest` framework installed.

### Action
Execute the Airflow task locally or in a test environment and capture the standard output/logs.

```python
# test_output_parity.py
import logging
import pytest
from airflow.models import TaskInstance
from airflow.utils.state import State
from datetime import datetime

# Import the callable directly from the migrated DAG file
from DW_DWH_DUMMY_ABSD_PLATO_TARIFE import execute_dummy_script, dag

def test_verbatim_log_output(caplog):
    """
    Asserts that the execution of the dummy script logs the exact legacy string
    including the typographical error 'nothinig'.
    """
    caplog.set_level(logging.INFO)
    
    # Execute the python callable
    execute_dummy_script()
    
    # Extract log messages
    log_messages = [record.message for record in caplog.records]
    
    # Assertions
    assert "Executing script body from DW.DWH_DUMMY_ABSD_PLATO_TARIFE..." in log_messages
    assert "Doing nothinig" in log_messages
    assert "Execution finished successfully." in log_messages
```

### Pass/Fail Criterion
* **Pass**: The log output contains the exact string `"Doing nothinig"`.
* **Fail**: The log output is missing, or the spelling has been corrected to `"Doing nothing"`.

---

## Section 2: DAG Structural & Metadata Validation

### Purpose
To verify that the migrated DAG's metadata (owner, retries, active runs, and pause state) matches the legacy UC4 configuration parameters.

### Setup
* The migrated DAG file `DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` is placed in the Airflow `dags/` directory.

### Action
Run a Python unit test using the Airflow metadata model to assert DAG properties.

```python
# test_dag_metadata.py
from airflow.models import DagBag

def test_dag_metadata_assertions():
    dag_bag = DagBag(include_examples=False)
    dag_id = 'dw_dwh_dummy_absd_plato_tarife'
    
    # Assert DAG loaded without import errors
    assert dag_id in dag_bag.dags
    dag = dag_bag.get_dag(dag_id)
    
    # Assert Default Arguments & Attributes
    assert dag.default_args.get('owner') == 'air_istns'
    assert dag.default_args.get('retries') == 0
    assert dag.max_active_runs == 1
    assert dag.schedule_interval is None
    assert dag.catchup is False
    
    # Assert Task Properties
    task = dag.get_task('dwh_dummy_absd_plato_tarife')
    assert task.retries == 0
```

### Pass/Fail Criterion
* **Pass**: All metadata assertions pass, confirming that the owner is `'air_istns'`, retries are set to `0`, and concurrent runs are limited to `1`.
* **Fail**: Any metadata attribute deviates from the specified design parameters.

---

## Section 3: Idempotency & Restartability (Wiederanlauf)

### Purpose
To validate the German documentation assertion: *"Wiederanlauf ohne weitere Maßnahmen möglich"* (Restart possible without further measures). The task must be completely stateless and idempotent, meaning multiple consecutive runs produce the same outcome without side effects.

### Setup
* A running Airflow metadata database (SQLite/PostgreSQL) in a test environment.

### Action
Trigger the DAG three times consecutively and verify the execution states.

```python
# test_idempotency.py
import uuid
from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

def test_consecutive_runs_success():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag('dw_dwh_dummy_absd_plato_tarife')
    
    for i in range(3):
        run_id = f"test_run_{uuid.uuid4()}"
        dag_run = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=run_id,
            run_type=DagRunType.MANUAL,
            conf={}
        )
        
        ti = dag_run.get_task_instance('dwh_dummy_absd_plato_tarife')
        ti.task = dag.get_task('dwh_dummy_absd_plato_tarife')
        
        # Run the task instance
        ti.run(ignore_ti_state=True, ignore_all_deps=True)
        
        # Assert task completed successfully without side-effects
        assert ti.state == TaskInstanceState.SUCCESS
```

### Pass/Fail Criterion
* **Pass**: All consecutive runs complete with a `SUCCESS` state, and no database locks, duplicate key violations, or state mutations occur.
* **Fail**: Any run fails or leaves behind persistent state that blocks subsequent runs.

---

## Section 4: Environment Variable Resolution

### Purpose
To ensure that the DAG does not hardcode GCP infrastructure variables and instead resolves them dynamically from the Airflow Variable store, preventing deployment failures across environments (Dev/UAT/Prod).

### Setup
* Clear any existing Airflow variables for `GCP_PROJECT`, `GCP_REGION`, and `DATAPROC_CLUSTER`.

### Action
Assert that the DAG file imports successfully and falls back to safe defaults or `None` when variables are missing, and correctly resolves them when they are present.

```python
# test_variable_resolution.py
from airflow.models import Variable
from airflow.utils.db import create_default_connections

def test_variable_resolution_with_defaults(monkeypatch):
    # Clear variables from DB context
    from DW_DWH_DUMMY_ABSD_PLATO_TARIFE import GCP_PROJECT_ID, DATAPROC_REGION
    
    # Assert fallback defaults are applied
    assert GCP_PROJECT_ID is None
    assert DATAPROC_REGION == "europe-west3"

def test_variable_resolution_configured(monkeypatch):
    # Mock Airflow Variable getters
    variables = {
        "GCP_PROJECT": "prod-gcp-project",
        "GCP_REGION": "europe-west1",
        "DATAPROC_CLUSTER": "prod-dataproc-cluster"
    }
    
    def mock_get(key, default_var=None):
        return variables.get(key, default_var)
        
    monkeypatch.setattr(Variable, "get", mock_get)
    
    # Reload module to trigger variable evaluation
    import importlib
    import DW_DWH_DUMMY_ABSD_PLATO_TARIFE
    importlib.reload(DW_DWH_DUMMY_ABSD_PLATO_TARIFE)
    
    assert DW_DWH_DUMMY_ABSD_PLATO_TARIFE.GCP_PROJECT_ID == "prod-gcp-project"
    assert DW_DWH_DUMMY_ABSD_PLATO_TARIFE.DATAPROC_REGION == "europe-west1"
    assert DW_DWH_DUMMY_ABSD_PLATO_TARIFE.DATAPROC_CLUSTER_NAME == "prod-dataproc-cluster"
```

### Pass/Fail Criterion
* **Pass**: The DAG dynamically resolves variables when configured in the Airflow environment and does not crash when they are absent.
* **Fail**: Hardcoded GCP project or cluster names are found in the DAG code, or the DAG fails to parse when variables are missing.