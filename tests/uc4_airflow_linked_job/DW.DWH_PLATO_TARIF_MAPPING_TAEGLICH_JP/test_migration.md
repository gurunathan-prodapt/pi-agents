# Migration Validation Test Suite: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document defines the migration-validation tests to prove that the migrated Airflow DAG `dw_dwh_plato_tarif_mapping_taeglich_dag` is behaviorally equivalent to the legacy UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.

---

## Test Case 1: DAG Import and Metadata Validation

### Purpose
Verify that the migrated Airflow DAG file is syntactically correct, loads into the Airflow environment without import errors, and preserves all required metadata properties derived from the legacy UC4 configuration.

### Setup
* A Python environment with Apache Airflow (`apache-airflow >= 2.0.0`) and `pytest` installed.
* The target DAG file `dw_dwh_dummy_absd_plato_tarife.py` placed in the Python path or mock DAGs folder.
* Mocked Airflow Variables `GCP_PROJECT` and `GCP_REGION` to prevent import-time lookup failures.

### Action
Execute a pytest script that loads the DAG using Airflow’s `DagBag` and asserts its top-level configuration properties.

### Code Implementation
```python
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def mock_airflow_variables():
    """Mock Airflow Variables required during DAG import."""
    with patch.object(Variable, 'get') as mock_get:
        def side_effect(key, default_var=None):
            variables = {
                "GCP_PROJECT": "test-gcp-project",
                "GCP_REGION": "europe-west3"
            }
            return variables.get(key, default_var)
        mock_get.side_effect = side_effect
        yield

def test_dag_loads_with_correct_metadata():
    # Load the DAG file
    dagbag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    
    # Assert no import errors occurred
    assert len(dagbag.import_errors) == 0, f"DAG import failures: {dagbag.import_errors}"
    
    # Retrieve the target DAG
    dag_id = "dw_dwh_plato_tarif_mapping_taeglich_dag"
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} not found in DagBag"
    
    # Assert Metadata Parity
    assert dag.schedule_interval is None, "Schedule must be None (handled by parent orchestrator)"
    assert dag.catchup is False, "Catchup must be disabled"
    assert dag.max_active_runs == 1, "Max active runs must be restricted to 1 to replicate UC4 sync behavior"
    assert dag.is_paused_upon_creation is False, "is_paused_upon_creation must be False to match UC4 <Active>1</Active>"
    assert dag.default_args.get("owner") == "airflow"
    assert dag.default_args.get("retries") == 0, "Retries must be 0 to match legacy configuration"
    assert dag.default_args.get("start_date") == datetime(2026, 3, 30), "Start date must match UC4 last modified date"
```

### Pass/Fail Criterion
* **Pass**: The DAG loads with zero import errors, and all asserted metadata values (schedule, catchup, max active runs, paused state, start date) match the design specification exactly.
* **Fail**: Any import errors are raised, or any metadata assertion fails.

---

## Test Case 2: Task Dependency and Structure Validation

### Purpose
Verify that the DAG structure matches the specified execution flow (`start >> dw_dwh_dummy_absd_plato_tarife >> end`) and that the task types are correctly mapped to lightweight operators rather than heavy Dataproc operators.

### Setup
* Same environment as Test Case 1.

### Action
Inspect the DAG's task list, task types, and upstream/downstream relationships programmatically.

### Code Implementation
```python
def test_dag_structure_and_operator_types():
    dagbag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_plato_tarif_mapping_taeglich_dag")
    
    # Assert expected tasks exist
    expected_tasks = {"start", "dw_dwh_dummy_absd_plato_tarife", "end"}
    assert set(dag.task_ids) == expected_tasks, f"Expected tasks {expected_tasks}, but got {dag.task_ids}"
    
    # Verify Operator Types
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    
    start_task = dag.get_task("start")
    dummy_task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    end_task = dag.get_task("end")
    
    assert isinstance(start_task, EmptyOperator), "Start task must be an EmptyOperator"
    assert isinstance(dummy_task, PythonOperator), "dw_dwh_dummy_absd_plato_tarife must be a PythonOperator"
    assert isinstance(end_task, EmptyOperator), "End task must be an EmptyOperator"
    
    # Verify Lineage / Dependencies
    assert start_task.downstream_task_ids == {"dw_dwh_dummy_absd_plato_tarife"}
    assert dummy_task.upstream_task_ids == {"start"}
    assert dummy_task.downstream_task_ids == {"end"}
    assert end_task.upstream_task_ids == {"dw_dwh_dummy_absd_plato_tarife"}
```

### Pass/Fail Criterion
* **Pass**: The DAG contains exactly the three specified tasks, uses the lightweight `PythonOperator` for the dummy task, and strictly enforces the linear execution order.
* **Fail**: Any unexpected tasks are found, incorrect operators are used (e.g., Dataproc operators), or the dependency chain is broken.

---

## Test Case 3: Output Parity and Typo Verification (Behavioral Equivalence)

### Purpose
Verify that executing the `dw_dwh_dummy_absd_plato_tarife` task produces the exact character-for-character output of the legacy UC4 script (`Doing nothinig`), preserving the original typographical spelling error.

### Setup
* Same environment as Test Case 1.
* Standard output (`sys.stdout`) capture mechanism.

### Action
Directly execute the Python callable associated with the `dw_dwh_dummy_absd_plato_tarife` task and capture the printed output.

### Code Implementation
```python
def test_output_parity_verbatim_string(capsys):
    dagbag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_plato_tarif_mapping_taeglich_dag")
    dummy_task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Retrieve and execute the python callable
    callable_fn = dummy_task.python_callable
    assert callable_fn is not None, "PythonOperator must have a defined python_callable"
    
    # Execute the function
    callable_fn()
    
    # Capture stdout
    captured = capsys.readouterr()
    
    # OUTPUT/PRINT LITERAL RULE: Must match legacy text exactly: "Doing nothinig"
    expected_output = "Doing nothinig\n"
    assert captured.out == expected_output, f"Expected output '{expected_output!r}', but got '{captured.out!r}'"
```

### Pass/Fail Criterion
* **Pass**: The captured standard output matches `"Doing nothinig\n"` exactly, preserving the spelling error.
* **Fail**: The output is missing, modified, or corrected (e.g., "Doing nothing").

---

## Test Case 4: Environment Variable Sourcing

### Purpose
Verify that the DAG correctly attempts to source global environment variables (`GCP_PROJECT`, `GCP_REGION`) from the Airflow Variable store at runtime, ensuring no hardcoded values or placeholders remain in the execution context.

### Setup
* Same environment as Test Case 1.

### Action
Load the DAG file and inspect the module-level variables `GCP_PROJECT` and `GCP_REGION` to ensure they resolve to the values configured in the Airflow Variable store.

### Code Implementation
```python
def test_gcp_variable_sourcing():
    # Set specific test values in the mock environment
    test_project = "prod-dwh-gcp-project-123"
    test_region = "europe-west3"
    
    with patch("airflow.models.Variable.get") as mock_variable_get:
        mock_variable_get.side_effect = lambda key, *args, **kwargs: {
            "GCP_PROJECT": test_project,
            "GCP_REGION": test_region
        }.get(key)
        
        # Import the module dynamically to trigger top-level variable evaluation
        import importlib
        import uc4_airflow_linked_job.DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.dw_dwh_dummy_absd_plato_tarife as dag_module
        importlib.reload(dag_module)
        
        # Assert that the module-level variables match the mocked Airflow Variables
        assert dag_module.GCP_PROJECT == test_project, "GCP_PROJECT was not correctly sourced from Airflow Variables"
        assert dag_module.GCP_REGION == test_region, "GCP_REGION was not correctly sourced from Airflow Variables"
        
        # Ensure no placeholder values remain
        assert "YOUR_GCP_PROJECT_ID" not in [dag_module.GCP_PROJECT, dag_module.GCP_REGION]
        assert "YOUR_DATAPROC_REGION" not in [dag_module.GCP_PROJECT, dag_module.GCP_REGION]
```

### Pass/Fail Criterion
* **Pass**: The DAG successfully retrieves and assigns the environment variables from the Airflow Variable store, and no default placeholder strings (e.g., `YOUR_GCP_PROJECT_ID`) are present.
* **Fail**: The variables fail to resolve, fallback to placeholders, or raise unhandled exceptions during import.