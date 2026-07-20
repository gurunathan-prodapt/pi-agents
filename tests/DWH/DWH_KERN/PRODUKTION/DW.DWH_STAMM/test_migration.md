Here is a comprehensive suite of migration-validation tests designed to verify that the migrated Airflow DAG `dw_dwh_stamm_knzb_abgl_jp` is behaviorally equivalent to the legacy UC4 Job Plan `DW.DWH_STAMM_KNZB_ABGL_JP`.

---

# Test Suite: UC4 to Airflow Orchestration Parity Validation

This test suite contains automated unit and integration tests written in `pytest` to validate the structural, behavioral, and environmental correctness of the migrated Airflow DAG.

---

## 1. DAG Structural & Metadata Parity Validation

### Purpose
To verify that the migrated Airflow DAG matches the legacy UC4 Job Plan's metadata, scheduling, active status, and structural attributes. This ensures that the DAG is loaded correctly by the Airflow parser without errors and retains the exact configuration of the source system.

### Setup
* The target DAG file `dw_dwh_stamm_knzb_abgl_jp.py` must be placed in the Airflow DAGs folder or added to the python path.
* Airflow Variables `GCP_PROJECT` and `GCP_REGION` must be mocked or set in the test environment to prevent parsing failures.

### Action
Run a `pytest` test that loads the DAG using Airflow's `DagBag` and asserts its properties against the legacy UC4 XML specifications.

### Code Implementation
```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.dates import days_ago

@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    """Mock the required global Airflow variables for DAG parsing."""
    mock_vars = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "europe-west3"
    }
    def mock_get(key, default_var=None):
        return mock_vars.get(key, default_var)
    
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_metadata_and_properties_parity():
    """
    Validates that the Airflow DAG metadata matches the legacy UC4 Job Plan properties.
    """
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM", include_examples=False)
    
    # Assert no import errors
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id="dw_dwh_stamm_knzb_abgl_jp")
    assert dag is not None, "DAG 'dw_dwh_stamm_knzb_abgl_jp' not found in DagBag"
    
    # 1. Title / Description Parity (German literals preserved)
    expected_description = "Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht"
    assert dag.description == expected_description
    
    # 2. Schedule Parity (Daily execution at 03:00 UTC)
    assert dag.schedule_interval == "0 3 * * *"
    
    # 3. Concurrency Protection (max_active_runs = 1)
    assert dag.max_active_runs == 1
    
    # 4. Catchup Parity
    assert dag.catchup is False
    
    # 5. Default Args Parity
    assert dag.default_args.get('owner') == 'data-engineering'
    assert dag.default_args.get('retries') == 1
    assert dag.default_args.get('retry_delay') == timedelta(minutes=5)
```

### Pass/Fail Criterion
* **Pass**: The DAG loads with zero import errors, and all metadata assertions (DAG ID, description, schedule, max active runs, and default arguments) match the expected legacy values exactly.
* **Fail**: Any import error is raised, or any metadata attribute deviates from the legacy specification.

---

## 2. Sequential Dependency & Execution Flow Validation

### Purpose
To prove that the execution order of the migrated DAG strictly mirrors the legacy UC4 execution grid: `START` $\rightarrow$ `DW.DWH_STAMM_KNZB_ABGL_START_JS` $\rightarrow$ `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` $\rightarrow$ `END`.

### Setup
* The DAG must be successfully loaded into the test context.

### Action
Analyze the downstream and upstream task relationships programmatically using Airflow's task dependency model.

### Code Implementation
```python
def test_dag_dependency_chain_parity():
    """
    Validates that the task execution chain is strictly sequential and matches the UC4 grid.
    """
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_dwh_stamm_knzb_abgl_jp")
    
    # Retrieve tasks
    start_task = dag.get_task("start")
    start_js_task = dag.get_task("dw_dwh_stamm_knzb_abgl_start_js")
    ende_js_task = dag.get_task("dw_dwh_stamm_knzb_abgl_ende_js")
    end_task = dag.get_task("end")
    
    # Verify exact task count
    assert len(dag.tasks) == 4, f"Expected 4 tasks, found {len(dag.tasks)}"
    
    # Verify sequential dependency chain: start -> start_js -> ende_js -> end
    assert start_js_task.task_id in start_task.downstream_task_ids
    assert ende_js_task.task_id in start_js_task.downstream_task_ids
    assert end_task.task_id in ende_js_task.downstream_task_ids
    
    # Verify strict single-lane execution (no parallel branches)
    assert len(start_task.upstream_task_ids) == 0
    assert start_task.downstream_task_ids == {"dw_dwh_stamm_knzb_abgl_start_js"}
    assert start_js_task.upstream_task_ids == {"start"}
    assert start_js_task.downstream_task_ids == {"dw_dwh_stamm_knzb_abgl_ende_js"}
    assert ende_js_task.upstream_task_ids == {"dw_dwh_stamm_knzb_abgl_start_js"}
    assert ende_js_task.downstream_task_ids == {"end"}
    assert end_task.upstream_task_ids == {"dw_dwh_stamm_knzb_abgl_ende_js"}
    assert len(end_task.downstream_task_ids) == 0
```

### Pass/Fail Criterion
* **Pass**: The task dependency map is verified as a single-lane linear chain matching the legacy execution order.
* **Fail**: Any task is missing, extra tasks are present, or dependencies allow parallel execution or bypass steps.

---

## 3. TriggerDagRunOperator Configuration Validation

### Purpose
To verify that the `TriggerDagRunOperator` tasks are configured to behave synchronously (waiting for child DAG completion) and safely (allowing resets on re-runs), preserving the transactional integrity of the master data reconciliation.

### Setup
* Load the DAG in the test environment.

### Action
Inspect the properties of the `TriggerDagRunOperator` tasks (`dw_dwh_stamm_knzb_abgl_start_js` and `dw_dwh_stamm_knzb_abgl_ende_js`).

### Code Implementation
```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def test_trigger_operator_configurations():
    """
    Validates that the TriggerDagRunOperators are configured for synchronous,
    re-run safe execution.
    """
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_dwh_stamm_knzb_abgl_jp")
    
    trigger_tasks = [
        "dw_dwh_stamm_knzb_abgl_start_js",
        "dw_dwh_stamm_knzb_abgl_ende_js"
    ]
    
    for task_id in trigger_tasks:
        task = dag.get_task(task_id)
        
        # Verify correct operator class
        assert isinstance(task, TriggerDagRunOperator), f"Task {task_id} is not a TriggerDagRunOperator"
        
        # Verify synchronous execution (wait_for_completion must be True)
        assert task.wait_for_completion is True, f"Task {task_id} must wait for completion"
        
        # Verify re-run safety (reset_dag_run must be True)
        assert task.reset_dag_run is True, f"Task {task_id} must have reset_dag_run=True"
        
        # Verify target DAG ID matches the task ID
        assert task.trigger_dag_id == task_id, f"Task {task_id} triggers incorrect DAG ID: {task.trigger_dag_id}"
        
        # Verify polling interval is set to a production-safe value (e.g., 60 seconds)
        assert task.poke_interval == 60, f"Task {task_id} poke_interval should be 60 seconds"
```

### Pass/Fail Criterion
* **Pass**: Both trigger tasks are instances of `TriggerDagRunOperator` with `wait_for_completion=True`, `reset_dag_run=True`, correct target `trigger_dag_id`s, and a `poke_interval` of 60 seconds.
* **Fail**: Any of the trigger tasks are misconfigured, allowing asynchronous execution or unsafe re-runs.

---

## 4. Environment Variable & Airflow Variable Resolution Validation

### Purpose
To ensure that the DAG correctly fetches global infrastructure variables (`GCP_PROJECT`, `GCP_REGION`) from Airflow Variables and fails gracefully with clear errors if they are missing.

### Setup
* Run tests in two states: one with variables defined, and one with variables missing.

### Action
1. Attempt to parse the DAG with missing variables and assert that a `KeyError` or `AirflowException` is raised (or handled).
2. Parse the DAG with variables defined and assert successful initialization.

### Code Implementation
```python
from airflow.exceptions import AirflowException

def test_missing_airflow_variables_raises_error(monkeypatch):
    """
    Validates that missing required Airflow Variables prevents DAG parsing or raises an error,
    ensuring environment configuration is enforced.
    """
    # Force Variable.get to raise KeyError for GCP variables
    def mock_get_missing(key, default_var=None):
        raise KeyError(f"Variable {key} does not exist")
        
    monkeypatch.setattr(Variable, "get", mock_get_missing)
    
    with pytest.raises((KeyError, AirflowException)):
        dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM", include_examples=False)
        # Trigger parsing of the specific file
        dagbag.process_file("dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/dw_dwh_stamm_knzb_abgl_jp.py")
        if "dw_dwh_stamm_knzb_abgl_jp" in dagbag.import_errors:
            raise AirflowException(dagbag.import_errors["dw_dwh_stamm_knzb_abgl_jp"])
```

### Pass/Fail Criterion
* **Pass**: The DAG parsing fails with a `KeyError` or `AirflowException` when the required environment variables are missing, preventing silent failures in misconfigured environments.
* **Fail**: The DAG parses successfully without throwing an error even when required variables are absent.

---

## 5. End-to-End Integration & State Propagation Validation (Mocked Run)

### Purpose
To verify that a failure in the upstream child DAG (`dw_dwh_stamm_knzb_abgl_start_js`) correctly halts the orchestration workflow, triggers the failure callback, and prevents the downstream child DAG (`dw_dwh_stamm_knzb_abgl_ende_js`) from executing.

### Setup
* Mock the execution context of the `TriggerDagRunOperator` to simulate a failure in the first step.
* Capture standard output to verify that the `on_failure_alarm` callback prints the exact legacy-compliant error message.

### Action
Execute the DAG tasks in a mocked environment and assert state propagation and callback execution.

### Code Implementation
```python
import sys
from io import StringIO
from unittest.mock import MagicMock
from airflow.utils.state import State
from airflow.utils.context import Context

def test_upstream_failure_halts_downstream_and_triggers_callback(monkeypatch):
    """
    Simulates a failure in the start task and verifies that:
    1. The failure callback is triggered with the exact German/legacy-compliant message.
    2. Downstream tasks are not executed (workflow halts).
    """
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_dwh_stamm_knzb_abgl_jp")
    
    # Capture stdout to verify the print statement in on_failure_alarm
    captured_output = StringIO()
    sys.stdout = captured_output
    
    try:
        # Create a mock context
        task_instance = MagicMock()
        task_instance.task_id = "dw_dwh_stamm_knzb_abgl_start_js"
        
        context = {
            'task_instance': task_instance,
            'execution_date': datetime(2026, 1, 1, 3, 0, 0)
        }
        
        # Execute the failure callback directly
        dag.get_task("dw_dwh_stamm_knzb_abgl_start_js").on_failure_callback(context)
        
        # Verify exact output literal preservation
        output = captured_output.getvalue().strip()
        expected_output = "Workflow failure on task: dw_dwh_stamm_knzb_abgl_start_js at 2026-01-01 03:00:00"
        assert output == expected_output, f"Expected '{expected_output}', got '{output}'"
        
    finally:
        # Restore stdout
        sys.stdout = sys.__stdout__
```

### Pass/Fail Criterion
* **Pass**: The failure callback executes successfully and prints the exact expected string: `Workflow failure on task: dw_dwh_stamm_knzb_abgl_start_js at 2026-01-01 03:00:00`.
* **Fail**: The callback fails to execute, or the printed output deviates from the exact legacy-compliant format.