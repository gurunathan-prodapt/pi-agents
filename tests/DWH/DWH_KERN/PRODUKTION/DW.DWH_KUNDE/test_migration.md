This document provides the migration-validation test suite for the migrated Airflow DAG `dw_dwh_kunde_abgl_woechentlich_jp.py`, which replaces the legacy UC4 Job Plan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml`.

Since this is a **pure orchestration migration (UC4_ONLY pattern)**, the validation tests focus on DAG structure, task dependencies, sensor configurations, trigger mechanisms, scheduling parity, and environment variable resolution.

---

## Test Case 1: DAG Compilation and Structural Integrity

### Purpose
Verify that the migrated Airflow DAG compiles without syntax or import errors, contains no dependency cycles, and matches the expected task structure.

### Setup
* Python environment with `apache-airflow` installed.
* The migrated DAG file `dw_dwh_kunde_abgl_woechentlich_jp.py` placed in the Python path or mock DAGs folder.
* Airflow variables mocked to prevent database lookup failures during import.

### Action
Run the following `pytest` test suite:

```python
import pytest
from airflow.models import DagBag, Variable
from unittest.mock import patch

@pytest.fixture(autouse=True)
def mock_airflow_variables():
    """Mock Airflow Variables to allow DAG compilation without a running DB."""
    with patch.object(Variable, 'get') as mock_get:
        def side_effect(key, default_var=None):
            variables = {
                "GCP_PROJECT": "test-gcp-project",
                "DATAPROC_REGION": "europe-west3",
                "DATAPROC_CLUSTER": "test-dataproc-cluster",
                "GCS_BUCKET": "test-gcs-bucket"
            }
            return variables.get(key, default_var)
        mock_get.side_effect = side_effect
        yield

def test_dag_compiles_and_has_no_cycles():
    """Asserts that the DAG compiles with no import errors or cycles."""
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE", include_examples=False)
    dag_id = "dw_dwh_kunde_abgl_woechentlich_jp"
    
    # Assert no import errors
    assert dag_id in dagbag.dags, f"DAG {dag_id} failed to load. Errors: {dagbag.import_errors}"
    dag = dagbag.get_dag(dag_id)
    
    # Assert no cycles
    assert len(dag.cycle_detector()) == 0, "DAG contains a cycle!"
```

### Pass/Fail Criterion
* **Pass:** The DAG loads successfully with zero import errors, and the cycle detector returns an empty list.
* **Fail:** Any import error is raised, or a cyclic dependency is detected.

---

## Test Case 2: Upstream Dependency Sensor Configurations

### Purpose
Verify that the four `ExternalTaskSensor` tasks are correctly configured to monitor the exact upstream DAGs and tasks specified in the migration design.

### Setup
* Load the DAG using `DagBag`.

### Action
Run the following `pytest` assertions to validate sensor configurations:

```python
def test_upstream_sensors_configuration():
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_kunde_abgl_woechentlich_jp")
    
    expected_sensors = {
        "wait_for_dw_dwh_abrechnung_reformat_js": {
            "external_dag_id": "dw_dwh_abrechnung_reformat_js",
            "external_task_id": "end",
            "poke_interval": 300,
            "timeout": 7200
        },
        "wait_for_dw_dwh_rechnung_export_taeglich_js": {
            "external_dag_id": "dw_dwh_rechnung_export_taeglich_js",
            "external_task_id": "end",
            "poke_interval": 300,
            "timeout": 7200
        },
        "wait_for_dw_dwh_tarifhist_scd_monatlich_js": {
            "external_dag_id": "dw_dwh_tarifhist_scd_monatlich_js",
            "external_task_id": "end",
            "poke_interval": 600,
            "timeout": 14400
        },
        "wait_for_dw_dwh_umsatz_konsolidierung_monatlich_js": {
            "external_dag_id": "dw_dwh_umsatz_konsolidierung_monatlich_js",
            "external_task_id": "end",
            "poke_interval": 600,
            "timeout": 14400
        }
    }
    
    for task_id, expected in expected_sensors.items():
        assert task_id in dag.task_ids, f"Sensor {task_id} is missing from the DAG."
        task = dag.get_task(task_id)
        
        assert task.external_dag_id == expected["external_dag_id"]
        assert task.external_task_id == expected["external_task_id"]
        assert task.allowed_states == ["success"]
        assert task.mode == "reschedule"
        assert task.poke_interval == expected["poke_interval"]
        assert task.timeout == expected["timeout"]
```

### Pass/Fail Criterion
* **Pass:** All four sensors exist, target the correct external DAGs and tasks, use `reschedule` mode, and have the correct timeouts and poke intervals.
* **Fail:** Any sensor is missing, targets an incorrect DAG/task, or uses incorrect intervals/modes.

---

## Test Case 3: Downstream Trigger Operator Configuration

### Purpose
Verify that the `TriggerDagRunOperator` task (`dw_dwh_kunde_abgl_woechentlich_js`) is configured to trigger the correct child execution DAG, waits for its completion, and resets previous runs.

### Setup
* Load the DAG using `DagBag`.

### Action
Run the following `pytest` assertions:

```python
def test_trigger_operator_configuration():
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_kunde_abgl_woechentlich_jp")
    
    task_id = "dw_dwh_kunde_abgl_woechentlich_js"
    assert task_id in dag.task_ids, f"Trigger task {task_id} is missing."
    
    task = dag.get_task(task_id)
    
    # Verify target DAG and execution parameters
    assert task.trigger_dag_id == "dw_dwh_kunde_abgl_woechentlich_js"
    assert task.wait_for_completion is True
    assert task.poke_interval == 60
    assert task.reset_dag_run is True
```

### Pass/Fail Criterion
* **Pass:** The trigger operator targets `dw_dwh_kunde_abgl_woechentlich_js`, has `wait_for_completion=True`, `poke_interval=60`, and `reset_dag_run=True`.
* **Fail:** Any of these parameters deviate from the target specification.

---

## Test Case 4: Execution Flow and Dependency Graph Parity

### Purpose
Verify that the execution flow matches the legacy UC4 Job Plan structure:
1. All four upstream sensors must run in parallel.
2. Once all sensors succeed, the `start` boundary marker is executed.
3. The `start` marker triggers the main execution job (`dw_dwh_kunde_abgl_woechentlich_js`).
4. Once the execution job completes, the `end` boundary marker is executed.

### Setup
* Load the DAG using `DagBag`.

### Action
Run the following `pytest` assertions to validate the dependency graph:

```python
def test_dag_dependency_graph():
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_kunde_abgl_woechentlich_jp")
    
    sensors = [
        "wait_for_dw_dwh_abrechnung_reformat_js",
        "wait_for_dw_dwh_rechnung_export_taeglich_js",
        "wait_for_dw_dwh_tarifhist_scd_monatlich_js",
        "wait_for_dw_dwh_umsatz_konsolidierung_monatlich_js"
    ]
    
    start_task = dag.get_task("start")
    trigger_task = dag.get_task("dw_dwh_kunde_abgl_woechentlich_js")
    end_task = dag.get_task("end")
    
    # Assert all sensors are upstream of 'start'
    for sensor_id in sensors:
        sensor_task = dag.get_task(sensor_id)
        assert start_task in sensor_task.downstream_list, f"{sensor_id} must be upstream of 'start'"
        
    # Assert 'start' is upstream of 'trigger_kunde_abgleich'
    assert trigger_task in start_task.downstream_list, "'start' must be upstream of the trigger task"
    
    # Assert 'trigger_kunde_abgleich' is upstream of 'end'
    assert end_task in trigger_task.downstream_list, "Trigger task must be upstream of 'end'"
```

### Pass/Fail Criterion
* **Pass:** The dependency graph matches the linear flow: `[Sensors] >> start >> trigger >> end`.
* **Fail:** Any dependency is missing, out of order, or incorrectly wired.

---

## Test Case 5: Schedule and Metadata Parity

### Purpose
Verify that the Airflow DAG schedule, description, and active state match the legacy UC4 Job Plan specifications.

### Setup
* Load the DAG using `DagBag`.

### Action
Run the following `pytest` assertions:

```python
def test_metadata_and_schedule_parity():
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_kunde_abgl_woechentlich_jp")
    
    # Schedule Parity: Weekly on Mondays at 03:00 AM UTC
    assert dag.schedule_interval == "0 3 * * 1"
    
    # Catchup and Concurrency Parity
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    
    # Active State Parity (is_paused_upon_creation should be False to match Active=1)
    assert dag.is_paused_upon_creation is False
    
    # Description Parity (German description preserved)
    expected_desc = "Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE) gegen das Referenzsystem"
    assert dag.description == expected_desc
```

### Pass/Fail Criterion
* **Pass:** The schedule is exactly `'0 3 * * 1'`, catchup is disabled, max active runs is 1, `is_paused_upon_creation` is `False`, and the description matches the legacy German title.
* **Fail:** Any metadata field deviates from the expected values.

---

## Test Case 6: Error Handling and Callback Registration

### Purpose
Verify that the failure callback `on_failure_alarm` is correctly registered on the main trigger task and executes without errors.

### Setup
* Load the DAG using `DagBag`.
* Mock a task instance context dictionary.

### Action
Run the following `pytest` assertions:

```python
from unittest.mock import MagicMock
import sys
from io import StringIO

def test_failure_callback_registration_and_execution():
    dagbag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_kunde_abgl_woechentlich_jp")
    
    trigger_task = dag.get_task("dw_dwh_kunde_abgl_woechentlich_js")
    
    # Assert callback is registered
    assert trigger_task.on_failure_callback is not None
    
    # Mock context
    mock_ti = MagicMock()
    mock_ti.task_id = "dw_dwh_kunde_abgl_woechentlich_js"
    mock_ti.log_url = "http://localhost:8080/log?dag_id=test&task_id=test"
    
    context = {
        "task_instance": mock_ti,
        "execution_date": "2026-01-05T03:00:00"
    }
    
    # Capture stdout to verify print statement in callback
    captured_output = StringIO()
    sys.stdout = captured_output
    
    # Execute callback
    trigger_task.on_failure_callback(context)
    
    # Reset redirect
    sys.stdout = sys.__stdout__
    
    output = captured_output.getvalue()
    assert "ALERT: Task dw_dwh_kunde_abgl_woechentlich_js failed" in output
    assert "Logs: http://localhost:8080/log" in output
```

### Pass/Fail Criterion
* **Pass:** The callback is registered on the trigger task, executes successfully when called with a mock context, and outputs the expected alert message.
* **Fail:** The callback is missing, or its execution raises an exception.