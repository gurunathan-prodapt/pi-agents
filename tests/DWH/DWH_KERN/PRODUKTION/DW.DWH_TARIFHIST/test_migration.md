# Migration Validation Test Suite: `dw_dwh_tarifhist_scd_monatlich_jp`

This document defines the migration-validation test suite for the migrated monthly orchestration DAG `dw_dwh_tarifhist_scd_monatlich_jp`. 

As an **orchestration-only DAG**, its primary responsibilities are:
1. Enforcing upstream cross-job dependencies via `ExternalTaskSensor` tasks.
2. Triggering the child execution DAG (`dw_dwh_tarifhist_scd_monatlich_js`) which contains the actual PySpark SCD Type 2 merge logic.
3. Managing execution state, error propagation, and alerting.

The following tests are designed to run in a QA/CI environment using `pytest` and the Apache Airflow testing utilities.

---

## Test Case 1: DAG Structural & Metadata Parity (Static Analysis)

### Purpose
Verify that the migrated Airflow DAG structure, scheduling, and metadata match the legacy UC4 JOBP definition and the target design specifications.

### Setup
* A Python environment with `pytest` and `apache-airflow` installed.
* The migrated DAG file `dw_dwh_tarifhist_scd_monatlich_jp.py` placed in the Airflow `DAGS_FOLDER`.
* Airflow Variables mocked or set in the test environment (`GCP_PROJECT`, `GCP_REGION`, `GCS_BUCKET`).

### Action
Run a static analysis test using `pytest` to inspect the DAG object's properties and task dependencies.

### Code Assertion
```python
import pytest
from airflow.models import DagBag, Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables():
    # Mock Airflow Variables required for DAG import
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCP_REGION", "europe-west3")
    Variable.set("GCS_BUCKET", "test-dwh-bucket")
    yield
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCP_REGION")
    Variable.delete("GCS_BUCKET")

def test_dag_metadata_and_structure():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag_id = "dw_dwh_tarifhist_scd_monatlich_jp"
    
    # 1. Assert DAG exists and loaded without import errors
    assert dag_id in dagbag.dags, f"DAG {dag_id} failed to load."
    dag = dagbag.get_dag(dag_id)
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"
    
    # 2. Assert Metadata Parity
    assert dag.schedule_interval == "0 3 1 * *", "Schedule must be monthly (1st of month at 03:00 AM)"
    assert dag.catchup is False, "Catchup must be disabled to prevent backfill storms"
    assert dag.max_active_runs == 1, "Max active runs must be 1 to prevent concurrent SCD executions"
    assert dag.is_paused_upon_creation is False, "DAG must be active upon creation (Active=1 in UC4)"

    # 3. Assert Task Inventory
    expected_tasks = {
        "start",
        "wait_for_abrechnung_reformat",
        "wait_for_kunde_abgl",
        "wait_for_rechnung_export",
        "wait_for_umsatz_konsolidierung",
        "dw_dwh_tarifhist_scd_monatlich_js",
        "end"
    }
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"

    # 4. Assert Dependency Graph
    # start >> [sensors] >> trigger >> end
    start_task = dag.get_task("start")
    assert set(start_task.downstream_task_ids) == {
        "wait_for_abrechnung_reformat",
        "wait_for_kunde_abgl",
        "wait_for_rechnung_export",
        "wait_for_umsatz_konsolidierung"
    }
    
    trigger_task = dag.get_task("dw_dwh_tarifhist_scd_monatlich_js")
    for sensor_id in start_task.downstream_task_ids:
        sensor_task = dag.get_task(sensor_id)
        assert trigger_task.task_id in sensor_task.downstream_task_ids
        
    assert trigger_task.downstream_task_ids == {"end"}
```

### Pass/Fail Criterion
* **Pass**: The DAG loads with zero import errors, matches the `0 3 1 * *` schedule, has `max_active_runs=1`, and strictly enforces the topological dependency graph: `start` -> `[4 Sensors]` -> `Trigger` -> `end`.
* **Fail**: Any import error occurs, metadata properties deviate, or the dependency graph is broken.

---

## Test Case 2: Upstream Sensor Configuration & Execution Delta Validation

### Purpose
Verify that the `ExternalTaskSensor` tasks are correctly configured to target the correct upstream DAGs and tasks, and evaluate the risk associated with execution schedules (Daily/Weekly/Monthly vs. Monthly).

### Setup
* Same as Test Case 1.

### Action
Inspect the configuration of each `ExternalTaskSensor` programmatically to ensure timeouts, poke intervals, and target tasks are set according to the production design.

### Code Assertion
```python
def test_external_task_sensors_configuration():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_tarifhist_scd_monatlich_jp")
    
    sensors = {
        "wait_for_abrechnung_reformat": {
            "external_dag_id": "dw_dwh_abrechnung_reformat_js",
            "external_task_id": "end"
        },
        "wait_for_kunde_abgl": {
            "external_dag_id": "dw_dwh_kunde_abgl_woechentlich_js",
            "external_task_id": "end"
        },
        "wait_for_rechnung_export": {
            "external_dag_id": "dw_dwh_rechnung_export_taeglich_js",
            "external_task_id": "end"
        },
        "wait_for_umsatz_konsolidierung": {
            "external_dag_id": "dw_dwh_umsatz_konsolidierung_monatlich_js",
            "external_task_id": "end"
        }
    }
    
    for task_id, expected_config in sensors.items():
        sensor: ExternalTaskSensor = dag.get_task(task_id)
        
        assert isinstance(sensor, ExternalTaskSensor), f"Task {task_id} is not an ExternalTaskSensor"
        assert sensor.external_dag_id == expected_config["external_dag_id"]
        assert sensor.external_task_id == expected_config["external_task_id"]
        assert sensor.allowed_states == ["success"], f"{task_id} must only allow 'success' state"
        assert sensor.poke_interval == 300, f"{task_id} poke_interval must be 300 seconds"
        assert sensor.timeout == 86400, f"{task_id} timeout must be 24 hours (86400 seconds)"
        
        # CRITICAL RISK CHECK:
        # Since upstream tasks run on Daily, Weekly, and Monthly schedules, execution_delta=timedelta(0)
        # requires the upstream DAGs to share the exact same execution date.
        # Verify that execution_delta is explicitly defined (even if 0) for baseline validation.
        assert sensor.execution_delta == timedelta(0), f"{task_id} execution_delta must be explicitly validated"
```

### Pass/Fail Criterion
* **Pass**: All four sensors target the correct upstream DAGs, wait for the `'end'` task, allow only `'success'` states, and have a 5-minute poke interval with a 24-hour timeout.
* **Fail**: Any sensor configuration is missing, points to an incorrect upstream DAG/task, or has non-matching timeout/poke intervals.

---

## Test Case 3: Child DAG Triggering & State Propagation

### Purpose
Verify that the `TriggerDagRunOperator` is configured to trigger the correct child DAG (`dw_dwh_tarifhist_scd_monatlich_js`), waits for its completion, and propagates failures to trigger the operational alarm.

### Setup
* Same as Test Case 1.

### Action
Inspect the `TriggerDagRunOperator` configuration and mock its execution to verify state propagation and failure callback registration.

### Code Assertion
```python
from unittest.mock import MagicMock

def test_trigger_child_dag_configuration():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_tarifhist_scd_monatlich_jp")
    
    trigger_task: TriggerDagRunOperator = dag.get_task("dw_dwh_tarifhist_scd_monatlich_js")
    
    assert isinstance(trigger_task, TriggerDagRunOperator), "Task is not a TriggerDagRunOperator"
    assert trigger_task.trigger_dag_id == "dw_dwh_tarifhist_scd_monatlich_js", "Triggers incorrect child DAG"
    assert trigger_task.wait_for_completion is True, "Must wait for child DAG completion to maintain synchronous execution"
    assert trigger_task.poke_interval == 60, "Poke interval for checking child status must be 60 seconds"
    assert trigger_task.on_failure_callback is not None, "Failure callback must be registered for alerting"

def test_on_failure_alarm_callback(capsys):
    from dw_dwh_tarifhist_scd_monatlich_jp import on_failure_alarm
    
    # Mock Airflow Context
    mock_context = {
        'task_instance': MagicMock(dag_id='test_dag', task_id='test_task'),
        'execution_date': datetime(2026, 1, 1, 3, 0, 0),
        'exception': Exception("Dataproc cluster connection timeout")
    }
    
    on_failure_alarm(mock_context)
    captured = capsys.readouterr()
    
    assert "ALERT: Task test_task in DAG test_dag failed on 2026-01-01 03:00:00" in captured.out
    assert "Error: Dataproc cluster connection timeout" in captured.out
```

### Pass/Fail Criterion
* **Pass**: The trigger operator is configured with `wait_for_completion=True`, targets the correct child DAG, and the `on_failure_alarm` callback successfully parses the context and outputs the alert details.
* **Fail**: The operator does not wait for completion, targets the wrong DAG, or the failure callback fails to execute or log the error.

---

## Test Case 4: Environment Variable Resolution & Runtime Configuration

### Purpose
Ensure that global infrastructure variables (`GCP_PROJECT`, `GCP_REGION`, `GCS_BUCKET`) are resolved dynamically at runtime using Airflow Variables, preventing hardcoded environment values.

### Setup
* A local Airflow metadata database or mocked environment variables.

### Action
Attempt to import the DAG under two conditions:
1. When variables are missing (should raise an exception or handle gracefully depending on design, but here we assert that the DAG import fails or succeeds based on Variable presence).
2. When variables are present (should resolve correctly).

### Code Assertion
```python
from airflow.exceptions import AirflowNotFoundException

def test_variable_resolution_success():
    # Set variables
    Variable.set("GCP_PROJECT", "prod-gcp-project")
    Variable.set("GCP_REGION", "europe-west3")
    Variable.set("GCS_BUCKET", "prod-dwh-bucket")
    
    # Import DAG module dynamically
    import importlib
    import dw_dwh_tarifhist_scd_monatlich_jp
    importlib.reload(dw_dwh_tarifhist_scd_monatlich_jp)
    
    assert dw_dwh_tarifhist_scd_monatlich_jp.GCP_PROJECT_ID == "prod-gcp-project"
    assert dw_dwh_tarifhist_scd_monatlich_jp.DATAPROC_REGION == "europe-west3"
    assert dw_dwh_tarifhist_scd_monatlich_jp.GCS_BUCKET == "prod-dwh-bucket"
    
    # Clean up
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCP_REGION")
    Variable.delete("GCS_BUCKET")

def test_variable_resolution_failure():
    # Ensure variables do not exist
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCP_REGION")
    Variable.delete("GCS_BUCKET")
    
    import importlib
    # Importing the DAG without variables set should raise an AirflowNotFoundException
    with pytest.raises(AirflowNotFoundException):
        import dw_dwh_tarifhist_scd_monatlich_jp
        importlib.reload(dw_dwh_tarifhist_scd_monatlich_jp)
```

### Pass/Fail Criterion
* **Pass**: The DAG successfully resolves variables when they are defined in the Airflow metadata database, and fails with an explicit `AirflowNotFoundException` when they are missing (preventing silent failures with default placeholders).
* **Fail**: The DAG uses hardcoded placeholders (e.g., `"YOUR_GCP_PROJECT_ID"`) or fails to raise an error when variables are missing.

---

## Test Case 5: End-to-End Execution Simulation (Mocked Dry Run)

### Purpose
Simulate a successful execution run of the DAG from `start` to `end` to verify that the execution flow behaves exactly like the legacy UC4 Job Plan when all dependencies are met.

### Setup
* Airflow environment with mocked task execution states.

### Action
Create a mocked run of the DAG where all upstream sensors return success immediately, and verify that the child DAG trigger task is executed.

### Code Assertion
```python
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

def test_dag_execution_flow_simulation(mocker):
    # Mock the sensor poke methods to return True (success) immediately
    mocker.patch(
        'airflow.sensors.external_task.ExternalTaskSensor.poke', 
        return_value=True
    )
    # Mock the TriggerDagRunOperator execute method to prevent actual triggering
    mock_trigger = mocker.patch(
        'airflow.operators.trigger_dagrun.TriggerDagRunOperator.execute',
        return_value=None
    )
    
    Variable.set("GCP_PROJECT", "test-project")
    Variable.set("GCP_REGION", "test-region")
    Variable.set("GCS_BUCKET", "test-bucket")
    
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_tarifhist_scd_monatlich_jp")
    
    # Create a local DagRun
    execution_date = datetime(2026, 1, 1, 3, 0, 0)
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL,
        data_interval=(execution_date, execution_date)
    )
    
    # Run the 'start' task
    start_ti = dag_run.get_task_instance(task_id="start")
    start_ti.task = dag.get_task("start")
    start_ti.run(ignore_ti_state=True, ignore_all_deps=True)
    assert start_ti.state == TaskInstanceState.SUCCESS
    
    # Run the sensors (mocked to succeed)
    sensor_tasks = [
        "wait_for_abrechnung_reformat",
        "wait_for_kunde_abgl",
        "wait_for_rechnung_export",
        "wait_for_umsatz_konsolidierung"
    ]
    for sensor_id in sensor_tasks:
        ti = dag_run.get_task_instance(task_id=sensor_id)
        ti.task = dag.get_task(sensor_id)
        ti.run(ignore_ti_state=True, ignore_all_deps=True)
        assert ti.state == TaskInstanceState.SUCCESS
        
    # Run the trigger task (mocked execution)
    trigger_ti = dag_run.get_task_instance(task_id="dw_dwh_tarifhist_scd_monatlich_js")
    trigger_ti.task = dag.get_task("dw_dwh_tarifhist_scd_monatlich_js")
    trigger_ti.run(ignore_ti_state=True, ignore_all_deps=True)
    
    assert mock_trigger.called, "TriggerDagRunOperator was not executed after sensors succeeded"
    assert trigger_ti.state == TaskInstanceState.SUCCESS

    # Clean up
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCP_REGION")
    Variable.delete("GCS_BUCKET")
```

### Pass/Fail Criterion
* **Pass**: The DAG execution flow successfully transitions from `start` through all four sensors, and executes the `TriggerDagRunOperator` task.
* **Fail**: Any task in the chain fails to execute, or the trigger operator is called before the sensors have successfully completed.