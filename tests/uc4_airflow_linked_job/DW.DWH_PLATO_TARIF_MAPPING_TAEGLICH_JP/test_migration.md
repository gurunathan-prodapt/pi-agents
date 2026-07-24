Here is the comprehensive migration-validation test suite for the migrated Airflow DAG `dw_dwh_dummy_absd_plato_tarife`. 

Since this is a structural/dummy synchronization job, the validation strategy focuses on **DAG structural integrity**, **verbatim log output parity**, **environment variable resolution**, and **idempotency**.

---

# MIGRATION VALIDATION TEST SUITE: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## Test Case 1: DAG Import and Structural Integrity

### Purpose
Verify that the migrated Airflow DAG file is syntactically correct, parses without errors in the Airflow environment, and preserves the exact task structure, default arguments, and task dependencies defined in the migration design.

### Setup
* A Python environment with `apache-airflow` and `pytest` installed.
* The DAG file `dw_dwh_dummy_absd_plato_tarife.py` placed in the Python path or mock-loaded.
* Airflow Variables mocked to prevent import-time lookup failures.

### Action
Run a unit test that loads the DAG from the file, inspects its properties, and asserts its structure.

```python
import pytest
from airflow.models import DagBag, Variable
from unittest.mock import patch

@pytest.fixture(autouse=True)
def mock_airflow_variables():
    """Mock Airflow Variables to isolate DAG parsing from the database."""
    with patch.object(Variable, 'get') as mock_get:
        mock_get.side_effect = lambda key, default_var=None: {
            "GCP_PROJECT": "mock-gcp-project",
            "GCP_REGION": "europe-west3"
        }.get(key, default_var)
        yield

def test_dag_loads_with_correct_structure():
    # Load the DAG file
    dagbag = DagBag(dag_folder="dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    
    # Assert no import errors occurred
    assert len(dagbag.import_errors) == 0, f"DAG import failures: {dagbag.import_errors}"
    
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag"
    
    dag = dagbag.get_dag(dag_id)
    
    # Assert DAG configurations
    assert dag.schedule_interval is None, "Schedule should be None (manual/triggered only)"
    assert dag.catchup is False, "Catchup should be disabled"
    assert dag.max_active_runs == 1, "Max active runs must be restricted to 1"
    assert dag.default_args.get("retries") == 0, "Retries must be 0 to match legacy failure-halt behavior"
    assert dag.default_args.get("owner") == "airflow", "Owner must be 'airflow'"
    
    # Assert Task Inventory
    expected_tasks = {"start", "dw_dwh_dummy_absd_plato_tarife", "end"}
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # Assert Task Dependencies: start -> dw_dwh_dummy_absd_plato_tarife -> end
    start_task = dag.get_task("start")
    dummy_task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    end_task = dag.get_task("end")
    
    assert dummy_task.task_id in start_task.downstream_task_ids, "Dependency missing: start -> dw_dwh_dummy_absd_plato_tarife"
    assert end_task.task_id in dummy_task.downstream_task_ids, "Dependency missing: dw_dwh_dummy_absd_plato_tarife -> end"
```

### Pass/Fail Criterion
* **Pass**: The DAG loads with zero import errors, contains exactly the three specified tasks, has a `None` schedule, `retries` set to `0`, and executes in the strict linear order: `start` $\rightarrow$ `dw_dwh_dummy_absd_plato_tarife` $\rightarrow$ `end`.
* **Fail**: Any import errors are raised, tasks are missing, or dependencies are incorrectly wired.

---

## Test Case 2: Verbatim Output Parity (Log Validation)

### Purpose
Prove that the Python task executes and outputs the exact literal string `"Doing nothinig"` (preserving the legacy typo character-for-character) to the execution logs, satisfying the **OUTPUT/PRINT LITERAL RULE**.

### Setup
* A Python testing environment with `pytest` and standard library `logging` capture capabilities.

### Action
Directly execute the Python callable `execute_legacy_print` mapped to the `PythonOperator` and capture standard logging output.

```python
import logging
import importlib.util
import sys

def test_verbatim_output_parity(caplog):
    # Dynamically import the DAG file to access the python_callable
    spec = importlib.util.spec_from_file_location(
        "dw_dwh_dummy_absd_plato_tarife", 
        "dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["dw_dwh_dummy_absd_plato_tarife"] = module
    spec.loader.exec_module(module)
    
    # Execute the legacy print function with log capturing enabled
    with caplog.at_level(logging.INFO):
        module.execute_legacy_print()
        
    # Assert that the exact string with the legacy typo is printed
    expected_message = "Doing nothinig"
    assert len(caplog.records) == 1, "Expected exactly one log record"
    assert caplog.records[0].message == expected_message, (
        f"Log output mismatch! Expected exact string '{expected_message}', "
        f"but got '{caplog.records[0].message}'"
    )
```

### Pass/Fail Criterion
* **Pass**: The log capture contains exactly one log record with the message `"Doing nothinig"`.
* **Fail**: The log message is missing, altered, or corrected (e.g., spelling corrected to "nothing").

---

## Test Case 3: Environment Variable Sourcing and Fallbacks

### Purpose
Verify that the DAG correctly attempts to source global environment variables (`GCP_PROJECT` and `GCP_REGION`) from Airflow's Variable store, and handles missing variables gracefully without raising runtime exceptions during DAG parsing.

### Setup
* A Python environment where Airflow Variables can be dynamically mocked or cleared.

### Action
Import the DAG module under two distinct scenarios:
1. **Scenario A**: Variables are defined in the Airflow Variable store.
2. **Scenario B**: Variables are completely absent from the Airflow Variable store.

```python
from unittest.mock import patch
from airflow.models import Variable
import importlib
import sys

def test_variable_sourcing_success():
    """Scenario A: Variables exist in Airflow."""
    with patch.object(Variable, 'get') as mock_get:
        mock_get.side_effect = lambda key, default_var=None: {
            "GCP_PROJECT": "prod-gcp-project-123",
            "GCP_REGION": "europe-west3"
        }.get(key, default_var)
        
        # Force reload of the module to trigger global variable evaluation
        if "dw_dwh_dummy_absd_plato_tarife" in sys.modules:
            del sys.modules["dw_dwh_dummy_absd_plato_tarife"]
            
        import dags.uc4_airflow_linked_job.DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.dw_dwh_dummy_absd_plato_tarife as dag_mod
        
        assert dag_mod.GCP_PROJECT == "prod-gcp-project-123"
        assert dag_mod.GCP_REGION == "europe-west3"

def test_variable_sourcing_fallback():
    """Scenario B: Variables are missing (should fallback to None without crashing)."""
    with patch.object(Variable, 'get') as mock_get:
        # Simulate Variable.get returning the default_var when key is missing
        mock_get.side_effect = lambda key, default_var=None: default_var
        
        if "dw_dwh_dummy_absd_plato_tarife" in sys.modules:
            del sys.modules["dw_dwh_dummy_absd_plato_tarife"]
            
        import dags.uc4_airflow_linked_job.DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.dw_dwh_dummy_absd_plato_tarife as dag_mod
        
        assert dag_mod.GCP_PROJECT is None
        assert dag_mod.GCP_REGION is None
```

### Pass/Fail Criterion
* **Pass**: In Scenario A, variables resolve to their mocked values. In Scenario B, variables resolve to `None` without throwing any `KeyError` or import-time exceptions.
* **Fail**: The module raises an exception during import when variables are missing, or fails to bind the correct values when they are present.

---

## Test Case 4: End-to-End Execution and Idempotency Simulation

### Purpose
Verify that the DAG executes successfully from end-to-end in a local runner environment, and that repeated executions are strictly idempotent (producing the same success state without side effects).

### Setup
* A local Airflow database initialized for testing (e.g., SQLite).
* Airflow Variables `GCP_PROJECT` and `GCP_REGION` set in the test DB.

### Action
Trigger the DAG twice sequentially using Airflow's native `dag.test()` execution utility.

```python
from datetime import datetime
from airflow.models import DagBag, Variable
from airflow.utils.state import DagRunState, TaskInstanceState

def test_dag_execution_and_idempotency(val_db_session=None):
    # Initialize variables in the test environment
    Variable.set("GCP_PROJECT", "test-project")
    Variable.set("GCP_REGION", "us-central1")
    
    dagbag = DagBag(dag_folder="dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    
    # --- FIRST RUN ---
    execution_date_1 = datetime(2026, 3, 30, 12, 0, 0)
    dag_run_1 = dag.test(execution_date=execution_date_1)
    
    # Assert overall DAG run success
    assert dag_run_1.state == DagRunState.SUCCESS, "First DAG run failed"
    
    # Assert individual task states
    for ti in dag_run_1.get_task_instances():
        assert ti.state == TaskInstanceState.SUCCESS, f"Task {ti.task_id} failed in Run 1"
        
    # --- SECOND RUN (Idempotency Check) ---
    execution_date_2 = datetime(2026, 3, 31, 12, 0, 0)
    dag_run_2 = dag.test(execution_date=execution_date_2)
    
    # Assert overall DAG run success on second run
    assert dag_run_2.state == DagRunState.SUCCESS, "Second DAG run failed"
    
    # Assert individual task states on second run
    for ti in dag_run_2.get_task_instances():
        assert ti.state == TaskInstanceState.SUCCESS, f"Task {ti.task_id} failed in Run 2"
```

### Pass/Fail Criterion
* **Pass**: Both sequential runs complete with a final state of `SUCCESS`, and all internal tasks (`start`, `dw_dwh_dummy_absd_plato_tarife`, `end`) transition to `SUCCESS` without raising errors.
* **Fail**: Any task fails, or the DAG run state results in `FAILED` or `UPSTREAM_FAILED`.