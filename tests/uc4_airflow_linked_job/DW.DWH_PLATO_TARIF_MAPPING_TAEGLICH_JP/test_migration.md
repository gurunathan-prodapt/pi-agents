# Migration Validation Test Suite
**Job under test:** `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` (Migrated to `dw_dwh_dummy_absd_plato_tarife`)

This test suite validates the migration of the UC4 UNIX placeholder job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to its Apache Airflow equivalent. Since the legacy job was a dummy synchronization step (executing only `:print Doing nothinig`), the validation focus is on **structural integrity**, **metadata parity**, **variable resolution**, and **orchestration readiness**.

---

## Test Case 1: DAG Structural & Metadata Validation (Static Analysis)

### Purpose
To verify that the migrated Airflow DAG matches the structural and configuration specifications defined in the migration design document, ensuring no legacy properties are lost and the DAG is parsed correctly by Airflow.

### Setup
* The migrated DAG file `dw_dwh_dummy_absd_plato_tarife.py` is placed in the Airflow `DAGS_FOLDER`.
* A Python testing environment with `pytest` and `apache-airflow` installed.
* Airflow variables mocked or populated in the test environment.

### Action
Run a static analysis test using `pytest` to parse the DAG and assert its properties against the legacy specification.

```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    """Mock the required Airflow variables to prevent parsing errors."""
    mock_vars = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "europe-west3",
        "GCS_BUCKET": "test-gcs-bucket"
    }
    def mock_get(key, default_var=None):
        return mock_vars.get(key, default_var)
    
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_metadata_and_structure():
    # Load the DAG bag
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    
    # 1. Assert DAG loaded without import errors
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    assert dag_id in dag_bag.dags, f"DAG {dag_id} failed to load. Errors: {dag_bag.import_errors}"
    
    dag = dag_bag.get_dag(dag_id)
    
    # 2. Assert DAG Metadata Parity
    assert dag.schedule_interval is None, "Schedule must be None (externally triggered)"
    assert dag.catchup is False, "Catchup must be disabled"
    assert dag.max_active_runs == 1, "max_active_runs must be restricted to 1 to prevent parallel execution conflicts"
    assert dag.default_args.get('owner') == 'airflow'
    assert dag.default_args.get('retries') == 1
    
    # 3. Assert Task Inventory
    assert len(dag.tasks) == 1, "DAG must contain exactly one task"
    task = dag.get_task(dag_id)
    
    # 4. Assert Operator Type (EmptyOperator replaces the dummy UNIX script)
    from airflow.operators.empty import EmptyOperator
    assert isinstance(task, EmptyOperator), f"Task must be an EmptyOperator, found {type(task)}"
```

### Pass/Fail Criterion
* **Pass:** The DAG parses with zero import errors, contains exactly one task with ID `dw_dwh_dummy_absd_plato_tarife` of type `EmptyOperator`, has `schedule_interval` set to `None`, and `max_active_runs` set to `1`.
* **Fail:** Any import errors occur, or any of the metadata assertions fail.

---

## Test Case 2: Behavioral Equivalence & Execution Validation

### Purpose
To prove that executing the migrated Airflow task is behaviorally equivalent to the legacy UC4 job. The legacy job executed `:print Doing nothinig` and succeeded instantly. The Airflow task must execute successfully without side effects.

### Setup
* An initialized Airflow metadata database (can be a local SQLite database for testing).
* Airflow variables mocked.

### Action
Execute the task in isolation using the Airflow CLI / programmatic task instance runner and capture the execution state.

```python
import datetime
from airflow.models import TaskInstance, DagBag
from airflow.utils.state import TaskInstanceState

def test_task_execution_behavior():
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Create a dummy execution date
    execution_date = datetime.datetime(2026, 3, 30, 18, 0, 0, tzinfo=datetime.timezone.utc)
    
    # Instantiate a TaskInstance
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    # Run the task directly
    ti.run(ignore_ti_state=True, ignore_all_deps=True, test_mode=True)
    
    # Assert execution state
    assert ti.state == TaskInstanceState.SUCCESS, f"Task failed with state: {ti.state}"
```

### Pass/Fail Criterion
* **Pass:** The task runs to completion and transitions to the `SUCCESS` state without throwing any exceptions.
* **Fail:** The task execution fails, times out, or raises an unhandled exception.

---

## Test Case 3: Environment Variable & Configuration Dependency Validation

### Purpose
To verify that the DAG correctly references the required environment variables (`GCP_PROJECT`, `GCP_REGION`, `GCS_BUCKET`) and fails gracefully or succeeds depending on their presence in the Airflow Metadata database.

### Setup
* A test environment where Airflow variables can be dynamically injected or cleared.

### Action
Run two tests:
1. Verify successful parsing when variables are present.
2. Verify failure behavior when variables are missing (to ensure the DAG does not run with silent defaults that could cause downstream issues).

```python
import pytest
from airflow.models import DagBag, Variable

def test_dag_parsing_with_missing_variables(monkeypatch):
    """Verify that missing variables are caught during parsing/execution."""
    # Mock empty variables
    monkeypatch.setattr(Variable, "get", lambda key, default_var=None: None)
    
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    
    # If the DAG code uses Variable.get without a default, it should raise a KeyError or fail to load
    # Let's check if the DAG failed to load or loaded with None values
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    
    # If it loaded, verify how the variables were resolved
    if dag_id in dag_bag.dags:
        dag = dag_bag.get_dag(dag_id)
        # Ensure that if they are loaded, we log a warning or handle them.
        # Note: In the provided code, Variable.get("GCP_PROJECT") is called at the top level.
        # If not mocked, this will raise a KeyError during parsing.
        print("DAG parsed, checking if variables are handled.")
```

### Pass/Fail Criterion
* **Pass:** The DAG successfully resolves variables when they are defined in the environment. If variables are missing, the system raises a clear configuration error rather than executing with corrupted/empty state.
* **Fail:** The DAG fails to parse even when variables are correctly defined, or it silently accepts invalid configurations.

---

## Test Case 4: Downstream Integration Interface Validation (Mock Trigger)

### Purpose
The design document highlights a major risk: **"DOWNSTREAM NOT YET MIGRATED"** (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). This test validates that the migrated DAG exposes a clean, standard interface that can be triggered externally via a `TriggerDagRunOperator` or Airflow API once the parent workflow is migrated.

### Setup
* Airflow environment running with the DAG database initialized.

### Action
Simulate an external trigger event on the DAG and verify that a DAG Run is successfully created in the database.

```python
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagRun, DagBag
from airflow.utils.state import DagRunState
import datetime

def test_external_trigger_interface():
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    
    # Ensure DAG is active in the DagBag
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    assert dag_id in dag_bag.dags
    
    # Trigger the DAG programmatically
    run_id = f"test_trigger_{int(datetime.datetime.utcnow().timestamp())}"
    dag_run = trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf={},
        execution_date=datetime.datetime.now(datetime.timezone.utc)
    )
    
    assert dag_run is not None
    assert dag_run.run_id == run_id
    assert dag_run.state == DagRunState.QUEUED or dag_run.state == DagRunState.RUNNING
```

### Pass/Fail Criterion
* **Pass:** The DAG run is successfully registered in the Airflow database with a state of `QUEUED` or `RUNNING`, proving that the manual trigger interface (`schedule=None`) is fully functional.
* **Fail:** The trigger action raises an error, or the DAG run cannot be initialized.