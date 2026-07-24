Here is a comprehensive suite of migration-validation tests designed to verify that the migrated Airflow DAG behaves identically to the legacy UC4 UNIX dummy job.

---

# MIGRATION VALIDATION TEST SUITE
**Target Job:** `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`  
**Migrated File:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`

---

## SECTION 1: DAG Structure & Metadata Validation

### Purpose
To verify that the migrated Airflow DAG is syntactically correct, loads without import errors, and matches the structural metadata (DAG ID, schedule, start date, and task structure) defined in the migration design.

### Setup
* A Python environment with `apache-airflow` and `pytest` installed.
* The DAG file placed in the Airflow DAGs directory or accessible via the Python path.

### Action
Run a pytest script that loads the DAG using Airflow's `DagBag` and asserts its structural properties.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag
from datetime import datetime, timedelta

@pytest.fixture(scope="module")
def dagbag():
    # Load the DAG file directly
    return DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/", include_examples=False)

def test_dag_loads_with_no_errors(dagbag):
    """Verify the DAG file contains no import errors."""
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

def test_dag_metadata(dagbag):
    """Verify DAG configuration matches the migration design."""
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag"
    
    dag = dagbag.get_dag(dag_id)
    
    # Assert DAG properties
    assert dag.schedule_interval is None, "Schedule should be None (manual/parent triggered)"
    assert dag.catchup is False, "Catchup should be disabled"
    assert dag.max_active_runs == 1, "Max active runs must be restricted to 1"
    assert dag.default_args.get("owner") == "airflow"
    assert dag.default_args.get("retries") == 0
    assert dag.default_args.get("start_date") == datetime(2026, 3, 30)

def test_task_structure(dagbag):
    """Verify the DAG contains exactly one PythonOperator task."""
    dag = dagbag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task_id = "dw_dwh_dummy_absd_plato_tarife"
    
    assert task_id in dag.task_ids, f"Task {task_id} is missing from the DAG"
    
    task = dag.get_task(task_id)
    from airflow.operators.python import PythonOperator
    assert isinstance(task, PythonOperator), "Task must be a PythonOperator"
    assert len(dag.tasks) == 1, "DAG must contain exactly one task"
```

### Pass/Fail Criterion
* **Pass:** The DAG loads with zero import errors, and all metadata assertions (DAG ID, schedule, task type, and default arguments) evaluate to `True`.
* **Fail:** Any import error is raised, or any metadata assertion fails.

---

## SECTION 2: Output Parity & Literal Matching

### Purpose
To guarantee behavioral equivalence with the legacy UC4 job by verifying that the task outputs the exact string `"Doing nothinig"` (including the legacy typo) to the execution logs.

### Setup
* A Python environment with `pytest` and `apache-airflow` installed.
* Access to the DAG's python callable `log_dummy_action`.

### Action
Execute the Python callable directly within a test harness, capturing the standard logging output to assert character-for-character equivalence.

```python
# test_output_parity.py
import logging
import pytest
from uc4_airflow_linked_job.DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DUMMY_ABSD_PLATO_TARIFE import log_dummy_action

def test_log_output_parity(caplog):
    """Verify the logged output matches the legacy UC4 print statement exactly."""
    # Capture INFO level logs
    with caplog.at_level(logging.INFO):
        log_dummy_action()
        
    # Assert that the exact legacy string was logged
    expected_literal = "Doing nothinig"
    
    # Check if the exact literal is present in the captured logs
    log_messages = [record.message for record in caplog.records]
    assert expected_literal in log_messages, (
        f"Output parity failed. Expected exact log: '{expected_literal}'. "
        f"Found logs: {log_messages}"
    )
```

### Pass/Fail Criterion
* **Pass:** The log stream contains the exact string `"Doing nothinig"`.
* **Fail:** The log stream does not contain the string, or contains a corrected spelling (e.g., `"Doing nothing"`), violating the **OUTPUT/PRINT LITERAL RULE**.

---

## SECTION 3: Environment Variable & Parsing Resilience

### Purpose
To verify that the DAG parses successfully and safely in all environments, even if the global variables `GCP_PROJECT` and `GCP_REGION` are not yet defined in the target Airflow environment.

### Setup
* A clean Airflow metadata database environment (or mocked environment variables) where `GCP_PROJECT` and `GCP_REGION` are absent.

### Action
Mock the `Variable.get` method to return `None` (simulating missing environment configuration) and attempt to import the DAG.

```python
# test_resilience.py
import pytest
from unittest.mock import patch

@patch('airflow.models.Variable.get')
def test_dag_parsing_without_variables(mock_variable_get):
    """Verify that the DAG can be parsed even if GCP variables are missing."""
    # Configure mock to return None when variables are requested
    mock_variable_get.side_effect = lambda key, default_var=None: default_var
    
    try:
        # Import the DAG module dynamically
        import uc4_airflow_linked_job.DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DUMMY_ABSD_PLATO_TARIFE as dag_module
        assert dag_module.GCP_PROJECT is None
        assert dag_module.GCP_REGION is None
    except Exception as e:
        pytest.fail(f"DAG failed to parse when GCP variables were missing: {e}")
```

### Pass/Fail Criterion
* **Pass:** The module imports successfully without throwing a `KeyError` or `ValueError`, and the variables default gracefully to `None`.
* **Fail:** The import fails, indicating that the DAG has hard dependencies on environment variables during the parsing phase.

---

## SECTION 4: Execution State & Idempotency

### Purpose
To verify that the task executes successfully within an Airflow context, transitions to a `SUCCESS` state, and can be run repeatedly (idempotent) without causing side effects or errors.

### Setup
* An Airflow metadata database initialized for testing (e.g., using `airflow db init`).

### Action
Create a dummy DAG run and execute the task instance programmatically.

```python
# test_execution.py
import pytest
from datetime import datetime
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

def test_task_execution_success():
    """Verify that the task executes and completes with a SUCCESS status."""
    dagbag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Create a unique execution date
    execution_date = datetime(2026, 3, 30, 12, 0, 0)
    
    # Create a mock DagRun in the metadata database
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL,
    )
    
    # Get the TaskInstance
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.task = task
    
    # Run the task instance
    ti.run(ignore_ti_state=True, ignore_all_deps=True)
    
    # Assert execution state is SUCCESS
    assert ti.state == TaskInstanceState.SUCCESS, f"Task failed with state: {ti.state}"
```

### Pass/Fail Criterion
* **Pass:** The task instance runs to completion and its state is updated to `SUCCESS`.
* **Fail:** The task execution raises an exception, or the final state is `FAILED` or `UPSTREAM_FAILED`.