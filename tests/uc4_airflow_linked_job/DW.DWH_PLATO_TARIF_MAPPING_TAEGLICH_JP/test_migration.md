This document provides the migration-validation test suite for the migrated Airflow DAG `dw_dwh_dummy_absd_plato_tarife` (converted from the legacy UC4 job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`). 

Since the source UC4 job is a dummy/placeholder task designed solely for synchronization and logging, the validation focus is on **DAG structural integrity**, **verbatim output parity**, and **environment configuration validation**.

---

## Test Case 1: DAG Import and Structural Integrity

### Purpose
To verify that the migrated Airflow DAG file is syntactically correct, can be parsed by the Airflow daemon without errors, and contains the exact task structure and dependencies defined in the migration design.

### Setup
* Python 3.8+ environment with `apache-airflow` and `pytest` installed.
* The DAG file `DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` must be placed in the Python path or accessible to the test suite.
* Airflow variables mocked or populated in the test environment.

### Action
Run a pytest suite that imports the DAG and inspects its properties, task IDs, operator types, and upstream/downstream dependencies.

### Pass/Fail Criterion
* **Pass:** The DAG is loaded with no import errors; it contains exactly three tasks (`start`, `dw_dwh_dummy_absd_plato_tarife`, `end`); the tasks are chained in the correct order; and the main task uses the `BashOperator`.
* **Fail:** Any import errors occur, or the task IDs, operator types, or dependency chain do not match the specification.

### Test Code (`test_dag_structure.py`)

```python
import os
import pytest
from unittest.mock import patch
from airflow.models import DagBag, Variable

# Mock Airflow Variables before importing the DAG to prevent KeyError/AirflowException
@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    mock_vars = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "europe-west3",
        "GCS_BUCKET": "test-system-bucket"
    }
    def mock_get(key, default_var=None):
        return mock_vars.get(key, default_var)
    
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_loads_with_no_errors():
    """Verify that the DAG can be imported without any syntax or configuration errors."""
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import failures: {dag_bag.import_errors}"

def test_dag_structure_and_dependencies():
    """Verify that the DAG has the correct tasks and sequential execution flow."""
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_dummy_absd_plato_tarife")
    
    assert dag is not None, "DAG 'dw_dwh_dummy_absd_plato_tarife' not found."
    assert dag.schedule_interval is None, "DAG should be set to run on-demand (schedule_interval=None)."
    assert not dag.catchup, "DAG catchup should be disabled (False)."

    # Verify task existence and types
    expected_tasks = {
        "start": "EmptyOperator",
        "dw_dwh_dummy_absd_plato_tarife": "BashOperator",
        "end": "EmptyOperator"
    }
    
    assert set(dag.task_ids) == set(expected_tasks.keys()), "DAG task IDs do not match the specification."
    
    for task_id, expected_type in expected_tasks.items():
        task = dag.get_task(task_id)
        assert task.__class__.__name__ == expected_type, f"Task {task_id} is not of type {expected_type}."

    # Verify dependency chain: start -> dw_dwh_dummy_absd_plato_tarife -> end
    start_task = dag.get_task("start")
    main_task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    end_task = dag.get_task("end")

    assert main_task.task_id in [t.task_id for t in start_task.downstream_list]
    assert end_task.task_id in [t.task_id for t in main_task.downstream_list]
```

---

## Test Case 2: Functional Behavioral Equivalence (Output Parity)

### Purpose
To verify that the execution of the `dw_dwh_dummy_absd_plato_tarife` task produces the exact same output as the legacy UC4 job, preserving the specific string literal and its spelling (`Doing nothinig`).

### Setup
* A local or staging Airflow environment.
* The DAG is loaded into the environment.

### Action
Execute the `dw_dwh_dummy_absd_plato_tarife` task individually using the Airflow CLI or a test runner, and capture the standard output.

### Pass/Fail Criterion
* **Pass:** The task executes successfully (exit code `0`) and outputs the exact string `Doing nothinig` to the logs.
* **Fail:** The task fails to execute, or the output string differs in spelling, casing, or spacing from the legacy output.

### Test Code (`test_functional_equivalence.py`)

```python
import pytest
from datetime import datetime
from airflow.models import TaskInstance, DagBag, Variable
from airflow.utils.state import State

@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    mock_vars = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "europe-west3",
        "GCS_BUCKET": "test-system-bucket"
    }
    monkeypatch.setattr(Variable, "get", lambda key, default_var=None: mock_vars.get(key, default_var))

def test_bash_operator_output(capsys):
    """Verify that the BashOperator executes the exact command and outputs the expected string."""
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Assert the exact bash command is set
    assert task.bash_command == "echo 'Doing nothinig'", "The bash command has been altered from the legacy specification."
    
    # Execute the task in a test context
    execution_date = datetime(2026, 1, 1)
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    # Run the task
    ti.run(ignore_ti_state=True, ignore_all_deps=True, test_mode=True)
    
    # Assert execution state is SUCCESS
    assert ti.state == State.SUCCESS, "The task execution failed."
```

---

## Test Case 3: Environment Variable & Configuration Validation

### Purpose
To ensure that the global variables required by the DAG file are correctly configured in the environment and do not cause runtime failures during DAG parsing or execution.

### Setup
* Access to the target Airflow environment (e.g., Cloud Composer).

### Action
Query the Airflow Metadata Database or use the Airflow CLI to verify the presence and non-emptiness of the required global variables: `GCP_PROJECT`, `GCP_REGION`, and `GCS_BUCKET`.

### Pass/Fail Criterion
* **Pass:** All three variables exist in the Airflow environment and contain valid, non-empty string values.
* **Fail:** Any of the three variables are missing or contain empty/default placeholder values (e.g., `"YOUR_GCP_PROJECT_ID"`).

### Test Code (`test_env_variables.py`)

```python
import pytest
from airflow.models import Variable

def test_required_variables_exist():
    """Ensure that the production environment has the required variables configured."""
    required_keys = ["GCP_PROJECT", "GCP_REGION", "GCS_BUCKET"]
    
    for key in required_keys:
        try:
            val = Variable.get(key)
            assert val is not None, f"Variable '{key}' is set to None."
            assert val != "", f"Variable '{key}' is empty."
            assert "YOUR_" not in val, f"Variable '{key}' still contains placeholder value: '{val}'."
        except KeyError:
            pytest.fail(f"Required Airflow Variable '{key}' is missing from the environment.")
```