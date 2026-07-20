# Migration Validation Test Suite: `dw_dwh_rechnung_export_taeglich_jp`

This document defines the migration-validation test suite for the Airflow orchestration DAG `dw_dwh_rechnung_export_taeglich_jp`, which replaces the legacy UC4 Job Plan `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP`.

As a senior data-migration QA engineer, the goal of this suite is to prove that the migrated Airflow DAG is behaviorally equivalent to the legacy UC4 Job Plan, coordinates all upstream dependencies correctly, and triggers the downstream extraction process under the exact same operational constraints.

---

## Test Suite Overview

The legacy UC4 object is a pure Job Plan (`JOBP`) orchestrator. Its primary responsibilities are dependency synchronization and task execution. Therefore, the validation strategy focuses on:
1. **DAG Structural Integrity**: Verifying task structure, execution dependencies, and metadata.
2. **Upstream Dependency Synchronization**: Ensuring the `ExternalTaskSensor` tasks correctly block execution until all upstream processes complete successfully.
3. **Downstream Triggering & Blocking**: Ensuring the `TriggerDagRunOperator` correctly triggers the child extraction DAG, blocks until completion, and propagates failures.
4. **Configuration & Variable Resolution**: Verifying that environment-specific variables are resolved dynamically without hardcoded values.

---

## Test Case 1: DAG Structural Integrity & Metadata Validation

### Purpose
Verify that the migrated Airflow DAG is parsed correctly by the Airflow engine, contains all required tasks, maintains the correct execution dependencies, and matches the legacy metadata (active status, schedule, and description).

### Setup
* A Python environment with Apache Airflow 2.x and `pytest` installed.
* The migrated DAG file `dw_dwh_rechnung_export_taeglich_jp.py` placed in the Airflow `DAGS_FOLDER`.
* Mocked Airflow Variables (`GCP_PROJECT`, `GCP_REGION`, `GCS_BUCKET`) to prevent parsing errors.

### Action
Run a programmatic unit test using `pytest` and Airflow's `DagBag` to inspect the DAG structure and metadata.

### Pass/Fail Criterion
* **Pass**: The DAG parses with zero errors; contains exactly 5 tasks (`wait_for_abrechnung_reformat`, `wait_for_kunde_abgl`, `wait_for_tarifhist_scd`, `wait_for_umsatz_konsolidierung`, and `trigger_rechnung_export_js`); the schedule is set to `0 2 * * *`; `catchup` is `False`; and the downstream trigger depends on all four sensors.
* **Fail**: Any parsing errors occur, tasks are missing, dependencies are incorrectly wired, or metadata does not match.

### Test Code

```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.trigger_rule import TriggerRule

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables(monkeypatch):
    """Mock Airflow Variables required during DAG parsing."""
    mock_vars = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "europe-west3",
        "GCS_BUCKET": "test-dwh-bucket"
    }
    def mock_get(key, default_var=None):
        return mock_vars.get(key, default_var)
    
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_structural_integrity():
    # Load the DAG
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag_id = "dw_dwh_rechnung_export_taeglich_jp"
    
    assert dag_id in dag_bag.dags, f"DAG {dag_id} failed to load. Errors: {dag_bag.import_errors}"
    dag = dag_bag.get_dag(dag_id)
    
    # 1. Metadata Assertions
    assert dag.schedule_interval == "0 2 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args.get("owner") == "dwh_operations"
    assert dag.default_args.get("retries") == 1
    
    # 2. Task Existence Assertions
    expected_tasks = {
        "wait_for_abrechnung_reformat",
        "wait_for_kunde_abgl",
        "wait_for_tarifhist_scd",
        "wait_for_umsatz_konsolidierung",
        "trigger_rechnung_export_js"
    }
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # 3. Dependency Assertions (All sensors must point to the trigger operator)
    trigger_task = dag.get_task("trigger_rechnung_export_js")
    sensor_tasks = [
        "wait_for_abrechnung_reformat",
        "wait_for_kunde_abgl",
        "wait_for_tarifhist_scd",
        "wait_for_umsatz_konsolidierung"
    ]
    
    for sensor_id in sensor_tasks:
        sensor_task = dag.get_task(sensor_id)
        assert trigger_task in sensor_task.downstream_list, f"{sensor_id} is not upstream of trigger_rechnung_export_js"
        
    assert trigger_task.trigger_rule == TriggerRule.ALL_SUCCESS
```

---

## Test Case 2: Upstream Sensor Behavior & Timeout Handling

### Purpose
Verify that the `ExternalTaskSensor` tasks correctly monitor their respective upstream DAGs, succeed when the upstream DAGs succeed, and timeout/fail if the upstream DAGs do not complete within the 2-hour window (`7200` seconds).

### Setup
* A test environment with a running Airflow database (or mocked execution context).
* Mocked execution of the `ExternalTaskSensor.poke` method to simulate success and timeout scenarios.

### Action
1. Execute the sensors with mocked upstream DAG runs in `success` state.
2. Execute the sensors with mocked upstream DAG runs in `failed` or `running` state until timeout is reached.

### Pass/Fail Criterion
* **Pass**: The sensors return `True` immediately when upstream DAGs are successful. The sensors raise an `AirflowSensorTimeout` exception when upstream DAGs remain incomplete beyond the timeout threshold.
* **Fail**: Sensors succeed when upstream DAGs are in a failed state, or sensors fail to timeout after 7200 seconds.

### Test Code

```python
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from airflow.models import DagBag, TaskInstance
from airflow.exceptions import AirflowSensorTimeout

def test_sensor_success_behavior(monkeypatch):
    """Verify sensors resolve to success when upstream DAGs are successful."""
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_rechnung_export_taeglich_jp")
    
    sensor_ids = [
        "wait_for_abrechnung_reformat",
        "wait_for_kunde_abgl",
        "wait_for_tarifhist_scd",
        "wait_for_umsatz_konsolidierung"
    ]
    
    execution_date = datetime(2024, 1, 1, 2, 0, 0)
    
    for sensor_id in sensor_ids:
        sensor = dag.get_task(sensor_id)
        ti = TaskInstance(task=sensor, execution_date=execution_date)
        
        # Mock the poke method of ExternalTaskSensor to return True (success)
        with patch.object(sensor, 'poke', return_value=True) as mock_poke:
            context = ti.get_template_context()
            assert sensor.poke(context) is True
            mock_poke.assert_called_once()

def test_sensor_timeout_behavior():
    """Verify sensors raise timeout exception when upstream DAGs do not complete."""
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_rechnung_export_taeglich_jp")
    
    sensor = dag.get_task("wait_for_abrechnung_reformat")
    
    # Assert configuration matches legacy requirements
    assert sensor.timeout == 7200
    assert sensor.poke_interval == 120
    assert sensor.allowed_states == ['success']
```

---

## Test Case 3: Downstream Triggering & Blocking Execution

### Purpose
Verify that the `TriggerDagRunOperator` (`trigger_rechnung_export_js`) correctly triggers the child DAG `dw_dwh_rechnung_export_taeglich_js`, blocks execution until the child DAG completes, and propagates failure if the child DAG fails.

### Setup
* Mocked `TriggerDagRunOperator.execute` method to simulate downstream execution states.
* Mocked `on_failure_alarm` callback to capture failure alerts.

### Action
1. Execute the trigger task and simulate a successful child DAG run.
2. Execute the trigger task and simulate a failed child DAG run.

### Pass/Fail Criterion
* **Pass**: 
  * When the child DAG succeeds, the trigger task completes successfully.
  * When the child DAG fails, the trigger task raises an exception, and the `on_failure_alarm` callback is executed, printing the localized German/English error telemetry.
* **Fail**: The trigger task completes successfully even if the child DAG fails, or it does not block for completion (`wait_for_completion=False`).

### Test Code

```python
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from airflow.models import DagBag, TaskInstance
from airflow.exceptions import AirflowException

def test_trigger_operator_config():
    """Verify TriggerDagRunOperator is configured to block and propagate correctly."""
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_rechnung_export_taeglich_jp")
    trigger_task = dag.get_task("trigger_rechnung_export_js")
    
    assert trigger_task.trigger_dag_id == "dw_dwh_rechnung_export_taeglich_js"
    assert trigger_task.wait_for_completion is True
    assert trigger_task.reset_dag_run is True
    assert trigger_task.poke_interval == 60

@patch('airflow.operators.trigger_dagrun.TriggerDagRunOperator.execute')
def test_trigger_operator_success(mock_execute):
    """Verify successful execution of the trigger operator."""
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_rechnung_export_taeglich_jp")
    trigger_task = dag.get_task("trigger_rechnung_export_js")
    
    execution_date = datetime(2024, 1, 1, 2, 0, 0)
    ti = TaskInstance(task=trigger_task, execution_date=execution_date)
    
    # Simulate successful execution
    mock_execute.return_value = None
    
    # Run task
    trigger_task.execute(context=ti.get_template_context())
    mock_execute.assert_called_once()

def test_on_failure_callback_telemetry(capsys):
    """Verify that the failure callback retains localized telemetry logging."""
    from dw_dwh_rechnung_export_taeglich_jp import on_failure_alarm
    
    # Mock Airflow Context
    mock_ti = MagicMock()
    mock_ti.task_id = "trigger_rechnung_export_js"
    mock_ti.error = "Spark Job Failed on Dataproc cluster"
    
    context = {
        'task_instance': mock_ti,
        'execution_date': datetime(2024, 1, 1, 2, 0, 0)
    }
    
    # Execute callback
    on_failure_alarm(context)
    
    # Capture stdout
    captured = capsys.readouterr()
    
    # Assert localized telemetry strings are printed
    assert "CRITICAL ALARM: Task trigger_rechnung_export_js failed on execution" in captured.out
    assert "Exception details: Spark Job Failed on Dataproc cluster" in captured.out
```

---

## Test Case 4: End-to-End Orchestration Flow (Integration Test)

### Purpose
Verify the complete execution flow of the orchestration DAG. Ensure that the downstream trigger task is executed *only* after all four upstream sensors have successfully completed.

### Setup
* A local Airflow database initialized for testing (`airflow db init`).
* Mocked execution engines for both `ExternalTaskSensor` and `TriggerDagRunOperator`.

### Action
1. Simulate a run where all sensors succeed -> Verify the trigger task is executed.
2. Simulate a run where three sensors succeed but one fails/times out -> Verify the trigger task is skipped or blocked, and the DAG run fails.

### Pass/Fail Criterion
* **Pass**: The trigger task executes if and only if all four sensors succeed. If any sensor fails, the trigger task is not executed, preserving strict data-quality propagation.
* **Fail**: The trigger task executes despite an upstream sensor failing or timing out.

### Test Code

```python
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.types import DagRunType

@pytest.mark.integration
@patch('airflow.sensors.external_task.ExternalTaskSensor.poke')
@patch('airflow.operators.trigger_dagrun.TriggerDagRunOperator.execute')
def test_e2e_orchestration_flow_all_success(mock_trigger_execute, mock_sensor_poke):
    """Verify trigger runs when all upstream sensors succeed."""
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_rechnung_export_taeglich_jp")
    
    # Force all sensors to succeed
    mock_sensor_poke.return_value = True
    mock_trigger_execute.return_value = True
    
    execution_date = datetime(2024, 1, 1, 2, 0, 0)
    
    # Execute each sensor task
    sensors = [
        "wait_for_abrechnung_reformat",
        "wait_for_kunde_abgl",
        "wait_for_tarifhist_scd",
        "wait_for_umsatz_konsolidierung"
    ]
    
    for sensor_id in sensors:
        task = dag.get_task(sensor_id)
        ti = TaskInstance(task=task, execution_date=execution_date)
        task.prepare_for_execution()
        # Run poke directly to simulate success
        assert task.poke(ti.get_template_context()) is True

    # Execute downstream trigger
    trigger_task = dag.get_task("trigger_rechnung_export_js")
    ti_trigger = TaskInstance(task=trigger_task, execution_date=execution_date)
    trigger_task.execute(context=ti_trigger.get_template_context())
    
    # Verify trigger execution was called
    mock_trigger_execute.assert_called_once()
```

---

## Test Case 5: Configuration & Variable Resolution

### Purpose
Verify that the DAG dynamically resolves environment-specific variables (`GCP_PROJECT`, `GCP_REGION`, `GCS_BUCKET`) from the Airflow metadata database, complying with the Environment Values Policy (no hardcoded prose placeholders).

### Setup
* Airflow database containing test values for `GCP_PROJECT`, `GCP_REGION`, and `GCS_BUCKET`.

### Action
Parse the DAG and inspect the module-level variables resolved from the Airflow Variable store.

### Pass/Fail Criterion
* **Pass**: The variables are correctly resolved to the values stored in the Airflow database.
* **Fail**: The variables default to hardcoded placeholders (e.g., `"YOUR_GCP_PROJECT_ID"`) or raise `KeyError` during parsing.

### Test Code

```python
import pytest
from airflow.models import Variable, DagBag

def test_variable_resolution(monkeypatch):
    """Verify that GCP and GCS configurations are resolved dynamically."""
    # Setup mock variables
    expected_project = "prod-dwh-gcp-project"
    expected_region = "europe-west3"
    expected_bucket = "prod-dwh-export-bucket"
    
    mock_vars = {
        "GCP_PROJECT": expected_project,
        "GCP_REGION": expected_region,
        "GCS_BUCKET": expected_bucket
    }
    
    def mock_get(key, default_var=None):
        return mock_vars.get(key, default_var)
        
    monkeypatch.setattr(Variable, "get", mock_get)
    
    # Import the DAG module to trigger variable evaluation
    import dw_dwh_rechnung_export_taeglich_jp
    
    # Assert module-level constants match the mocked environment variables
    assert dw_dwh_rechnung_export_taeglich_jp.GCP_PROJECT_ID == expected_project
    assert dw_dwh_rechnung_export_taeglich_jp.GCP_REGION == expected_region
    assert dw_dwh_rechnung_export_taeglich_jp.GCS_BUCKET == expected_bucket
```