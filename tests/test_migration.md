Here is the comprehensive migration-validation test suite designed to verify that the migrated Airflow DAGs and utility functions behave identically to the legacy UC4 JOBI scripts.

---

# Test Suite: Ab Initio Gatekeeper & State Reporting Migration Validation

This test suite validates the behavioral equivalence of the migrated Airflow-based gatekeeper and state-reporting tasks against the legacy UC4 JOBI scripts (`DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` and `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`).

---

## Section 1: Unit & Integration Tests (Pytest)

These tests use `pytest` along with Airflow's testing utilities and `unittest.mock` to validate the logic of the utility functions without requiring a live, running Airflow environment.

### Test Case 1.1: Start Guard - Successful Polling Loop ('go' state)
* **Purpose**: Verify that the polling loop correctly initializes the state tracking variable, loops while the status is `'wait'`, and successfully exits when the status changes to `'go'`, updating the tracking variable to `'ACTIVE in Ab Initio [time] [date]'`.
* **Setup**: 
  * Mock `time.sleep` to prevent test delays.
  * Mock `airflow.models.Variable.get` to return `'wait'` on the first call, and `'go'` on the second call.
  * Mock `airflow.models.Variable.set` to capture state updates.
* **Action**: Execute `poll_ab_initio_status_fn` with a mocked Airflow context.
* **Pass/Fail Criterion**: 
  * **Pass**: The function exits without raising an exception. `Variable.set` is called twice: first to set the state to `WAIT for Ab Initio (...)` and second to set the state to `ACTIVE in Ab Initio ...`.
  * **Fail**: The function loops infinitely, raises an unexpected exception, or fails to update the variable with the correct keys and values.

```python
# tests/test_ab_initio_start_success.py
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
from dags.utils.ab_initio_utils import poll_ab_initio_status_fn

@patch("dags.utils.ab_initio_utils.time.sleep", return_value=None)
@patch("dags.utils.ab_initio_utils.Variable")
def test_poll_ab_initio_status_success(mock_variable, mock_sleep):
    # Setup mock context
    mock_context = {
        'dag': MagicMock(dag_id='test_dag_id'),
        'task': MagicMock(task_id='test_task_id')
    }
    betr_job = "test_dag_id -> test_task_id"

    # Mock Variable.get to simulate state transition: wait -> go
    mock_variable.get.side_effect = [
        {},  # Initial call in update_variable_state (initialization)
        {"status_dwh": "wait"},  # First loop iteration
        {"status_dwh": "go"}     # Second loop iteration
    ]

    # Action
    poll_ab_initio_status_fn(**mock_context)

    # Assertions
    assert mock_variable.set.call_count == 2
    
    # Verify initial state write
    first_call_args = mock_variable.set.call_args_list[0][0]
    assert first_call_args[0] == "dw_adm_ab_initio_var"
    assert betr_job in first_call_args[1]
    assert "WAIT for Ab Initio" in first_call_args[1][betr_job]

    # Verify final success state write
    second_call_args = mock_variable.set.call_args_list[1][0]
    assert second_call_args[0] == "dw_adm_ab_initio_var"
    assert "ACTIVE in Ab Initio" in second_call_args[1][betr_job]
```

---

### Test Case 1.2: Start Guard - Abort State ('exit1')
* **Purpose**: Verify that if the external status is set to `'exit1'`, the polling loop immediately terminates, updates the tracking variable to `'Pruefjob wurde abgebrochen [time]'`, and raises an `AirflowException`.
* **Setup**:
  * Mock `time.sleep`.
  * Mock `airflow.models.Variable.get` to return `{"status_dwh": "exit1"}`.
* **Action**: Execute `poll_ab_initio_status_fn` with a mocked Airflow context.
* **Pass/Fail Criterion**:
  * **Pass**: The function raises an `AirflowException` containing the message `"Ab Initio check returned failure state (exit1)"`, and updates the tracking variable with the abort message.
  * **Fail**: The function exits cleanly, loops infinitely, or fails to update the variable with the abort status.

```python
# tests/test_ab_initio_start_abort.py
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
from dags.utils.ab_initio_utils import poll_ab_initio_status_fn

@patch("dags.utils.ab_initio_utils.time.sleep", return_value=None)
@patch("dags.utils.ab_initio_utils.Variable")
def test_poll_ab_initio_status_abort(mock_variable, mock_sleep):
    mock_context = {
        'dag': MagicMock(dag_id='test_dag_id'),
        'task': MagicMock(task_id='test_task_id')
    }
    betr_job = "test_dag_id -> test_task_id"

    # Mock Variable.get to return exit1 immediately
    mock_variable.get.side_effect = [
        {},  # Initial call
        {"status_dwh": "exit1"}  # First loop iteration
    ]

    # Action & Assertion
    with pytest.raises(AirflowException) as exc_info:
        poll_ab_initio_status_fn(**mock_context)
        
    assert "Ab Initio check returned failure state (exit1)" in str(exc_info.value)
    
    # Verify abort state write
    second_call_args = mock_variable.set.call_args_list[1][0]
    assert "Pruefjob wurde abgebrochen" in second_call_args[1][betr_job]
```

---

### Test Case 1.3: End Guard - Completion State Update
* **Purpose**: Verify that the end script correctly reads the current status and updates the tracking variable to `'fertig (HH:MM:SS DD.MM.YYYY)'` without wiping out other keys in the JSON object.
* **Setup**:
  * Mock `airflow.models.Variable.get` to return an existing JSON dictionary containing other keys (to verify non-destructive updates).
* **Action**: Execute `update_ab_initio_status_fn` with a mocked Airflow context.
* **Pass/Fail Criterion**:
  * **Pass**: The tracking variable is updated with the key `"{dag_id} -> {task_id}"` set to `"fertig (...)"`, while preserving existing keys (e.g., `"status_dwh"` and other jobs' statuses).
  * **Fail**: The variable is overwritten entirely, or the completion timestamp is missing or malformed.

```python
# tests/test_ab_initio_end.py
import pytest
from unittest.mock import MagicMock, patch
from dags.utils.ab_initio_utils import update_ab_initio_status_fn

@patch("dags.utils.ab_initio_utils.Variable")
def test_update_ab_initio_status_success(mock_variable):
    mock_context = {
        'dag': MagicMock(dag_id='test_end_dag'),
        'task': MagicMock(task_id='test_end_task')
    }
    betr_job = "test_end_dag -> test_end_task"

    # Mock existing variable state to ensure non-destructive updates
    mock_variable.get.return_value = {
        "status_dwh": "go",
        "some_other_job -> task": "ACTIVE"
    }

    # Action
    update_ab_initio_status_fn(**mock_context)

    # Assertions
    mock_variable.set.assert_called_once()
    called_args = mock_variable.set.call_args[0]
    
    assert called_args[0] == "dw_adm_ab_initio_var"
    updated_dict = called_args[1]
    
    # Verify non-destructive updates
    assert updated_dict["status_dwh"] == "go"
    assert updated_dict["some_other_job -> task"] == "ACTIVE"
    
    # Verify completion status format
    assert betr_job in updated_dict
    assert updated_dict[betr_job].startswith("fertig (")
```

---

## Section 2: System Integration & Concurrency Tests

These tests verify the behavior of the tasks when interacting with a real Airflow Metadata Database and check for race conditions.

### Test Case 2.1: Concurrent State Modification (Race Condition Prevention)
* **Purpose**: Ensure that if multiple tasks or external systems update the `dw_adm_ab_initio_var` variable concurrently, updates are safe and do not overwrite each other (verifying the "read-modify-write" safety of `update_variable_state`).
* **Setup**:
  * Initialize the Airflow Variable `dw_adm_ab_initio_var` in the local test database.
  * Simulate two concurrent threads executing `update_variable_state` with different keys.
* **Action**: Run concurrent updates using a Python `ThreadPoolExecutor`.
* **Pass/Fail Criterion**:
  * **Pass**: Both keys exist in the final JSON dictionary stored in the Airflow Variable.
  * **Fail**: One of the updates is lost (classic race condition / dirty write).

```python
# tests/test_concurrency.py
import pytest
from concurrent.futures import ThreadPoolExecutor
from airflow.models import Variable
from dags.utils.ab_initio_utils import update_variable_state

@pytest.mark.integration
def test_concurrent_variable_updates(dag_bag):
    # Initialize Variable
    Variable.set("dw_adm_ab_initio_var", {"status_dwh": "wait"}, serialize_json=True)

    def update_job_1():
        update_variable_state("job_1", "status_1")

    def update_job_2():
        update_variable_state("job_2", "status_2")

    # Execute concurrently
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(update_job_1), executor.submit(update_job_2)]
        for future in futures:
            future.result()

    # Fetch final state
    final_state = Variable.get("dw_adm_ab_initio_var", deserialize_json=True)

    # Assert both updates survived
    assert final_state["status_dwh"] == "wait"
    assert final_state["job_1"] == "status_1"
    assert final_state["job_2"] == "status_2"
```

---

## Section 3: Airflow DAG Validation Tests

These tests verify that the DAGs are correctly structured, have the correct timeouts, and do not contain syntax errors.

### Test Case 3.1: DAG Structure and Timeout Assertions
* **Purpose**: Verify that the DAGs are loaded without import errors, have the correct task IDs, and that the start guard DAG has an explicit execution timeout to prevent infinite billing loops.
* **Setup**: Load the DAGs using Airflow's `DagBag`.
* **Action**: Inspect the DAG properties and task configurations.
* **Pass/Fail Criterion**:
  * **Pass**: 
    * No import errors are present.
    * `dw_dwh_adm_pruefe_ab_initio_start_inc_guard` has a task named `ab_initio_gatekeeper` with an `execution_timeout` of exactly 1 hour (`timedelta(hours=1)`).
    * `dw_dwh_adm_pruefe_ab_initio_ende_inc` has a task named `update_ab_initio_status`.
  * **Fail**: Import errors exist, tasks are missing, or the timeout is missing/incorrect.

```python
# tests/test_dag_structure.py
import pytest
from datetime import timedelta
from airflow.models import DagBag

def test_dag_bag_loading():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

    # Verify Start Guard DAG
    start_dag = dag_bag.get_dag("dw_dwh_adm_pruefe_ab_initio_start_inc_guard")
    assert start_dag is not None
    assert "ab_initio_gatekeeper" in start_dag.task_ids
    
    gatekeeper_task = start_dag.get_task("ab_initio_gatekeeper")
    assert gatekeeper_task.execution_timeout == timedelta(hours=1)

    # Verify End Guard DAG
    end_dag = dag_bag.get_dag("dw_dwh_adm_pruefe_ab_initio_ende_inc")
    assert end_dag is not None
    assert "update_ab_initio_status" in end_dag.task_ids
```