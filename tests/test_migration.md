# Migration Validation Test Suite
**Target Job:** `DW.DWH_ADM_PRUEFE_AB_INITIO` (Start & End Synchronization Gatekeepers)  
**Target Platform:** Cloud Composer (Airflow) & BigQuery  

---

## 1. Test Suite Overview & Testing Strategy

The legacy UC4 JOBI scripts act as a state-synchronization barrier between external Ab Initio processing and downstream incremental DWH loads. 
* **`START_INC`** acts as a blocking polling loop (sensor) checking for status `'go'` or terminal failure `'exit1'`.
* **`ENDE_INC`** acts as a post-execution hook updating the global state variable to `'fertig'`.

To guarantee behavioral equivalence, the validation strategy uses **`pytest`** with the Airflow testing framework to mock the Airflow Variable Store and task execution contexts.

### Test Environment Setup
Ensure your testing environment has `pytest` and `apache-airflow` installed:
```bash
pip install pytest apache-airflow
```

---

## 2. Test Cases

### Test Case 1: START_INC — Successful Status Transition ('go')
#### Purpose
Verify that the `START_INC` sensor successfully detects the `'go'` status in the Airflow Variable Store, returns `True`, and allows downstream tasks to proceed.

#### Setup
* Mock the Airflow Variable `dw_adm_ab_initio_var` to return `{"STATUS_DWH": "go"}`.

#### Action
Execute the `check_ab_initio_status` callable within a mocked Airflow task context.

#### Pass/Fail Criterion
* **Pass:** The function returns `True` without raising any exceptions.
* **Fail:** The function returns `False`, raises an exception, or fails to parse the variable.

#### Test Code
```python
import pytest
from unittest.mock import patch
from airflow.models import Variable

# Import the callable under test
from dags.tasks.dw_dwh_adm_pruefe_ab_initio_start_inc import check_ab_initio_status

@patch('airflow.models.Variable.get')
def test_start_inc_status_go(mock_variable_get):
    # Setup: Mock Variable to return 'go' status
    mock_variable_get.return_value = {"STATUS_DWH": "go"}
    
    # Action
    result = check_ab_initio_status()
    
    # Assert
    assert result is True, "Sensor should return True when STATUS_DWH is 'go'"
```

---

### Test Case 2: START_INC — Terminal Failure Status ('exit1')
#### Purpose
Verify that the `START_INC` sensor immediately aborts execution by raising an `AirflowFailException` when the state transitions to `'exit1'`. This prevents unnecessary polling and resource consumption when upstream processes fail.

#### Setup
* Mock the Airflow Variable `dw_adm_ab_initio_var` to return `{"STATUS_DWH": "exit1"}`.

#### Action
Execute the `check_ab_initio_status` callable and catch exceptions.

#### Pass/Fail Criterion
* **Pass:** The function raises `airflow.exceptions.AirflowFailException`.
* **Fail:** The function returns `False`, returns `True`, or raises any other exception type.

#### Test Code
```python
import pytest
from unittest.mock import patch
from airflow.exceptions import AirflowFailException

from dags.tasks.dw_dwh_adm_pruefe_ab_initio_start_inc import check_ab_initio_status

@patch('airflow.models.Variable.get')
def test_start_inc_status_exit1(mock_variable_get):
    # Setup: Mock Variable to return 'exit1' status
    mock_variable_get.return_value = {"STATUS_DWH": "exit1"}
    
    # Action & Assert
    with pytest.raises(AirflowFailException) as exc_info:
        check_ab_initio_status()
    
    assert "Terminal status 'exit1' encountered" in str(exc_info.value)
```

---

### Test Case 3: START_INC — Polling State ('wait' / Missing Key)
#### Purpose
Verify that the `START_INC` sensor returns `False` (triggering a retry/poke cycle) when the status is `'wait'` or when the variable key is missing entirely.

#### Setup
* Mock the Airflow Variable `dw_adm_ab_initio_var` to return `{"STATUS_DWH": "wait"}` for Run A, and an empty dictionary `{}` for Run B.

#### Action
Execute the `check_ab_initio_status` callable for both scenarios.

#### Pass/Fail Criterion
* **Pass:** The function returns `False` in both scenarios, indicating that the sensor should continue polling.
* **Fail:** The function returns `True` or raises an exception.

#### Test Code
```python
import pytest
from unittest.mock import patch
from dags.tasks.dw_dwh_adm_pruefe_ab_initio_start_inc import check_ab_initio_status

@patch('airflow.models.Variable.get')
def test_start_inc_status_wait_and_missing(mock_variable_get):
    # Scenario A: Status is explicitly 'wait'
    mock_variable_get.return_value = {"STATUS_DWH": "wait"}
    result_wait = check_ab_initio_status()
    assert result_wait is False, "Sensor should return False when status is 'wait'"
    
    # Scenario B: Key is missing (fallback to default 'wait')
    mock_variable_get.return_value = {}
    result_missing = check_ab_initio_status()
    assert result_missing is False, "Sensor should return False when status key is missing"
```

---

### Test Case 4: ENDE_INC — State Variable Update & Context Logging
#### Purpose
Verify that `ENDE_INC` correctly reads the current status, formats the completion timestamp, and updates the Airflow Variable Store with the job execution details under the key `"<DAG_ID> -> <TASK_ID>"`.

#### Setup
* Mock the Airflow context dictionary with a specific `logical_date`, `dag_id`, and `task_id`.
* Mock `Variable.get` to return `{"STATUS_DWH": "go"}`.
* Mock `Variable.set` to capture the updated payload.

#### Action
Execute `log_and_update_ab_initio_status_callable` with the mocked context.

#### Pass/Fail Criterion
* **Pass:** `Variable.set` is called with the key `"dw_adm_ab_initio_var"`, and the JSON payload contains the key `"test_dag -> test_task"` mapped to the value `"fertig (12:00:00 15.10.2023)"`.
* **Fail:** The variable is not updated, the timestamp formatting is incorrect, or the dictionary structure is corrupted.

#### Test Code
```python
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from dags.tasks.dw_dwh_adm_pruefe_ab_initio_ende_inc import log_and_update_ab_initio_status_callable

@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_ende_inc_variable_update(mock_variable_get, mock_variable_set):
    # Setup
    mock_variable_get.return_value = {"STATUS_DWH": "go"}
    
    # Mock Airflow Context
    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag"
    mock_task = MagicMock()
    mock_task.task_id = "test_task"
    
    context = {
        'logical_date': datetime(2023, 10, 15, 12, 0, 0),
        'dag': mock_dag,
        'task': mock_task
    }
    
    # Action
    log_and_update_ab_initio_status_callable(**context)
    
    # Assert
    mock_variable_set.assert_called_once()
    called_key, called_value = mock_variable_set.call_args[0]
    
    assert called_key == "dw_adm_ab_initio_var"
    assert called_value["test_dag -> test_task"] == "fertig (12:00:00 15.10.2023)"
    assert called_value["STATUS_DWH"] == "go"  # Verify existing keys are preserved
```

---

### Test Case 5: ENDE_INC — Fallback Handling for Missing Variable Store
#### Purpose
Verify that if the Airflow Variable `dw_adm_ab_initio_var` does not exist yet in the environment, `ENDE_INC` gracefully initializes a new dictionary and writes the completion status without throwing a `KeyError`.

#### Setup
* Mock `Variable.get` to raise a `KeyError` (simulating a missing variable).
* Mock `Variable.set` to capture the initialized payload.

#### Action
Execute `log_and_update_ab_initio_status_callable` with the mocked context.

#### Pass/Fail Criterion
* **Pass:** The function catches the `KeyError`, initializes a new dictionary, and calls `Variable.set` with the new entry.
* **Fail:** The task crashes with a `KeyError` or unhandled exception.

#### Test Code
```python
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from dags.tasks.dw_dwh_adm_pruefe_ab_initio_ende_inc import log_and_update_ab_initio_status_callable

@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_ende_inc_fallback_missing_variable(mock_variable_get, mock_variable_set):
    # Setup: Simulate missing variable in Airflow DB
    mock_variable_get.side_effect = KeyError("Variable not found")
    
    mock_dag = MagicMock()
    mock_dag.dag_id = "fallback_dag"
    mock_task = MagicMock()
    mock_task.task_id = "fallback_task"
    
    context = {
        'logical_date': datetime(2023, 10, 15, 12, 0, 0),
        'dag': mock_dag,
        'task': mock_task
    }
    
    # Action
    log_and_update_ab_initio_status_callable(**context)
    
    # Assert
    mock_variable_set.assert_called_once()
    called_key, called_value = mock_variable_set.call_args[0]
    
    assert called_key == "dw_adm_ab_initio_var"
    assert "fallback_dag -> fallback_task" in called_value
    assert called_value["fallback_dag -> fallback_task"] == "fertig (12:00:00 15.10.2023)"
```