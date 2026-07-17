Here is a comprehensive suite of migration-validation tests designed for a Senior QA Engineer to verify the behavioral equivalence of the migrated Airflow DAG against the legacy UC4 workflow.

---

# Test Suite: `dw_dwh_stamm_knzb_abgl_jp` Validation

This test suite uses `pytest` along with standard Airflow testing utilities to validate the behavior of the migrated DAG, its state transitions, error handling, and log outputs.

## 1. Output Parity & Log Verbatim Verification

### Purpose
Verify that the migrated Python tasks output the exact German log messages and formatting defined in the legacy UC4 scripts (`DW.DWH_STAMM_KNZB_ABGL_START_JS`, `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`, and `DW.LESE_LOG_KNZB`).

### Setup
*   Initialize the Airflow metadata database in a test environment.
*   Mock the Airflow Variable `dw_variablen` with dummy path values.
*   Mock the Airflow Variable `dw_variablen_knzb` with `{"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": "20241104"}`.

### Action
Execute the `knzb_abgl_start` and `knzb_abgl_ende` tasks within a test execution context and capture the standard logging output.

### Pass/Fail Criterion
*   **Pass**: The logs contain the exact strings:
    *   `Protokolleintrag: DW.DWH_STAMM_KNZB_ABGL_START_JS innerhalb dw_dwh_stamm_knzb_abgl_jp`
    *   `Protokolleintrag: DW.DWH_STAMM_KNZB_ABGL_ENDE_JS innerhalb dw_dwh_stamm_knzb_abgl_jp`
    *   `KNZB-Stammdatenabgleich fuer Lauf 20241104 erfolgreich beendet`
*   **Fail**: Any of the log strings differ by character, casing, or punctuation from the legacy specification.

```python
# test_log_parity.py
import pytest
import logging
from unittest.mock import patch
from airflow.models import TaskInstance, Variable
from airflow.utils.state import State
from airflow.utils.context import Context

from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp import (
    dag, process_abgl_start, process_abgl_ende
)

@patch('dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp.Variable')
def test_log_parity_and_verbatim_output(mock_variable, caplog):
    # Setup mock variables
    mock_variable.get.side_effect = lambda key, deserialize_json=False: {
        "dw_variablen": {"DWH_HOME": "/opt/dwh", "HOME": "/home/dwh", "ISTNS_HOME": "/opt/istns"},
        "dw_variablen_knzb": {"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": "20241104"}
    }[key]

    context = Context({
        'ds_nodash': '20241104',
        'task': dag.get_task('knzb_abgl_start')
    })

    # Action: Run Start Task
    with caplog.at_level(logging.INFO):
        process_abgl_start(**context)
        
    # Assert Start Logs
    assert any(
        "Protokolleintrag: DW.DWH_STAMM_KNZB_ABGL_START_JS innerhalb dw_dwh_stamm_knzb_abgl_jp" in record.message
        for record in caplog.records
    ), "Start task log output does not match legacy format."

    caplog.clear()

    # Action: Run Ende Task
    context_ende = Context({
        'ds_nodash': '20241104',
        'task': dag.get_task('knzb_abgl_ende')
    })
    with caplog.at_level(logging.INFO):
        process_abgl_ende(**context_ende)

    # Assert Ende Logs
    assert any(
        "KNZB-Stammdatenabgleich fuer Lauf 20241104 erfolgreich beendet" in record.message
        for record in caplog.records
    ), "Ende task completion log does not match legacy format."
    assert any(
        "Protokolleintrag: DW.DWH_STAMM_KNZB_ABGL_ENDE_JS innerhalb dw_dwh_stamm_knzb_abgl_jp" in record.message
        for record in caplog.records
    ), "Ende task include log does not match legacy format."
```

---

## 2. Transformation Correctness: Lock State Transitions

### Purpose
Verify that the workflow correctly transitions the state variable `ABGLEICH_STATUS` inside the Airflow Variable `dw_variablen_knzb` through its lifecycle: `FREI` $\rightarrow$ `LAEUFT` $\rightarrow$ `FREI`.

### Setup
*   Set the initial Airflow Variable `dw_variablen_knzb` to `{"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": "20241103"}`.

### Action
1.  Execute `process_abgl_start` with execution date `20241104`.
2.  Inspect the updated Airflow Variable `dw_variablen_knzb`.
3.  Execute `process_abgl_ende` with execution date `20241104`.
4.  Inspect the final Airflow Variable `dw_variablen_knzb`.

### Pass/Fail Criterion
*   **Pass**: 
    *   After step 1, `ABGLEICH_STATUS` is `"LAEUFT"` and `LETZTER_LAUF` is `"20241104"`.
    *   After step 3, `ABGLEICH_STATUS` is `"FREI"` and `LETZTER_LAUF` remains `"20241104"`.
*   **Fail**: Any state variable contains an unexpected value at any point in the lifecycle.

```python
# test_state_transitions.py
import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp import (
    process_abgl_start, process_abgl_ende
)

@patch('dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp.Variable')
def test_lock_state_transitions(mock_variable):
    # Shared state mock
    state_store = {
        "dw_variablen": {"DWH_HOME": "/opt/dwh", "HOME": "/home/dwh", "ISTNS_HOME": "/opt/istns"},
        "dw_variablen_knzb": {"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": "20241103"}
    }

    def mock_get(key, deserialize_json=False):
        return state_store[key]

    def mock_set(key, val, serialize_json=False):
        state_store[key] = val

    mock_variable.get.side_effect = mock_get
    mock_variable.set.side_effect = mock_set

    # 1. Start Task Execution
    context = Context({'ds_nodash': '20241104'})
    process_abgl_start(**context)

    # Assert intermediate state
    assert state_store["dw_variablen_knzb"]["ABGLEICH_STATUS"] == "LAEUFT"
    assert state_store["dw_variablen_knzb"]["LETZTER_LAUF"] == "20241104"

    # 2. Ende Task Execution
    process_abgl_ende(**context)

    # Assert final state
    assert state_store["dw_variablen_knzb"]["ABGLEICH_STATUS"] == "FREI"
    assert state_store["dw_variablen_knzb"]["LETZTER_LAUF"] == "20241104"
```

---

## 3. Edge Case: Locked Process Handling (`GESPERRT`)

### Purpose
Verify that if the process is explicitly locked (`ABGLEICH_STATUS == "GESPERRT"`), the DAG immediately aborts execution and raises an `AirflowFailException` to prevent unauthorized runs, matching the legacy `:IF &ABGLEICH_STATUS = "GESPERRT" -> STOP_JOB()` logic.

### Setup
*   Set the Airflow Variable `dw_variablen_knzb` to `{"ABGLEICH_STATUS": "GESPERRT", "LETZTER_LAUF": "20241103"}`.

### Action
*   Execute `process_abgl_start` with execution date `20241104`.

### Pass/Fail Criterion
*   **Pass**: The task raises `AirflowFailException` and the state variables remain unchanged.
*   **Fail**: The task completes successfully or modifies the state variables.

```python
# test_locked_process.py
import pytest
from unittest.mock import patch
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp import process_abgl_start

@patch('dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp.Variable')
def test_locked_process_aborts(mock_variable):
    state_store = {
        "dw_variablen": {"DWH_HOME": "/opt/dwh", "HOME": "/home/dwh", "ISTNS_HOME": "/opt/istns"},
        "dw_variablen_knzb": {"ABGLEICH_STATUS": "GESPERRT", "LETZTER_LAUF": "20241103"}
    }
    mock_variable.get.side_effect = lambda key, deserialize_json=False: state_store[key]
    mock_variable.set.side_effect = lambda key, val, serialize_json=False: state_store.update({key: val})

    context = Context({'ds_nodash': '20241104'})

    # Assert that AirflowFailException is raised
    with pytest.raises(AirflowFailException) as exc_info:
        process_abgl_start(**context)

    assert "Processing aborted because status is set to GESPERRT." in str(exc_info.value)
    # Verify state was not modified
    assert state_store["dw_variablen_knzb"]["ABGLEICH_STATUS"] == "GESPERRT"
```

---

## 4. Error Handling & Deadlock Prevention (`on_failure_callback`)

### Purpose
Verify that if a task fails while holding the execution lock (`ABGLEICH_STATUS == "LAEUFT"`), the `on_failure_callback` catches the failure and transitions the state to `"ERROR_STATE"` to alert operations and prevent a silent deadlock.

### Setup
*   Set the Airflow Variable `dw_variablen_knzb` to `{"ABGLEICH_STATUS": "LAEUFT", "LETZTER_LAUF": "20241104"}`.

### Action
*   Trigger the `on_workflow_failure` callback function with a mocked context object.

### Pass/Fail Criterion
*   **Pass**: The Airflow Variable `dw_variablen_knzb` is updated to `{"ABGLEICH_STATUS": "ERROR_STATE", "LETZTER_LAUF": "20241104"}`.
*   **Fail**: The variable is not updated, or updated to an incorrect value.

```python
# test_failure_callback.py
import pytest
from unittest.mock import patch
from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp import on_workflow_failure

@patch('dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp.Variable')
def test_on_failure_callback_sets_error_state(mock_variable):
    state_store = {
        "dw_variablen_knzb": {"ABGLEICH_STATUS": "LAEUFT", "LETZTER_LAUF": "20241104"}
    }
    
    def mock_get(key, deserialize_json=False):
        return state_store[key]

    def mock_set(key, val, serialize_json=False):
        state_store[key] = val

    mock_variable.get.side_effect = mock_get
    mock_variable.set.side_effect = mock_set

    # Execute callback
    mock_context = {}
    on_workflow_failure(mock_context)

    # Assert state transitioned to ERROR_STATE
    assert state_store["dw_variablen_knzb"]["ABGLEICH_STATUS"] == "ERROR_STATE"
    assert state_store["dw_variablen_knzb"]["LETZTER_LAUF"] == "20241104"
```

---

## 5. Concurrency & Guard Task Validation

### Purpose
Verify that the `start_guard` task prevents parallel execution of the DAG, enforcing the `max_active_runs=1` constraint dynamically even if triggered manually.

### Setup
*   Mock `DagRun.find` to return an active running instance of the same DAG with a different `run_id`.

### Action
*   Execute the `check_active_runs` function within a mocked context.

### Pass/Fail Criterion
*   **Pass**: The task raises an `AirflowSkipException`, skipping the current run to prevent concurrent execution.
*   **Fail**: The task completes successfully, allowing parallel execution.

```python
# test_concurrency_guard.py
import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State
from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.dw_dwh_stamm_knzb_abgl_jp import check_active_runs

@patch('airflow.models.DagRun.find')
def test_guard_task_skips_on_concurrent_run(mock_dagrun_find):
    # Mock an existing active run
    mock_run = MagicMock()
    mock_run.run_id = "manual__2024-11-04T00:00:00+00:00"
    mock_dagrun_find.return_value = [mock_run]

    # Context for the current run (different run_id)
    mock_dag = MagicMock()
    mock_dag.dag_id = 'dw_dwh_stamm_knzb_abgl_jp'
    context = {
        'dag': mock_dag,
        'run_id': 'scheduled__2024-11-04T00:00:00+00:00'
    }

    # Assert that AirflowSkipException is raised
    with pytest.raises(AirflowSkipException) as exc_info:
        check_active_runs(**context)

    assert "Another instance of this DAG is currently running. Skipping execution." in str(exc_info.value)
```