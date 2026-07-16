Here is a comprehensive suite of migration-validation tests designed to verify that the migrated Apache Airflow DAGs and Python helper modules behave identically to the legacy UC4/Automic objects.

---

# Test Suite: KNZB Master Data Reconciliation Orchestration Validation

This test suite validates the migration of the orchestration workflow `DW.DWH_STAMM_KNZB_ABGL_JP` and its components from UC4 to Google Cloud Composer (Apache Airflow). 

Since this is a **Complexity Tier: Medium** migration following the `UC4_ONLY` pattern, the tests focus on:
*   State transition correctness (locking/unlocking mechanism).
*   Airflow Variable manipulation parity.
*   Strict execution ordering and dependency validation.
*   Log format and metadata parity.

---

## Section 1: Unit Tests for Shared Utility Modules

### Test Case 1.1: Environment Path Resolution (`hole_pfad_knzb.py`)
#### Purpose
Verify that the migrated helper module `resolve_knzb_paths` correctly retrieves environment paths from the Airflow Variable `dw_variablen` and falls back to default values gracefully without throwing unhandled exceptions.

#### Setup
*   A test environment running `pytest` with `apache-airflow` installed.
*   Mocked Airflow Variable models.

#### Action
Execute the following unit test using `pytest`:

```python
import pytest
from unittest.mock import patch
from airflow.models import Variable
from helpers.hole_pfad_knzb import resolve_knzb_paths

@patch('airflow.models.Variable.get')
def test_resolve_knzb_paths_success(mock_get):
    # Mock successful retrieval of JSON variable container
    mock_get.return_value = {
        "DWH_HOME": "gs://prod-dwh-bucket/dwh",
        "HOME": "gs://prod-home-bucket/home",
        "ISTNS_HOME": "gs://prod-istns-bucket/istns"
    }
    
    paths = resolve_knzb_paths()
    
    assert paths["DWH_HOME"] == "gs://prod-dwh-bucket/dwh"
    assert paths["HOME"] == "gs://prod-home-bucket/home"
    assert paths["ISTNS_HOME"] == "gs://prod-istns-bucket/istns"
    mock_get.assert_called_once_with("dw_variablen", deserialize_json=True, default_var={})

@patch('airflow.models.Variable.get')
def test_resolve_knzb_paths_defaults(mock_get):
    # Mock empty variable container to trigger defaults
    mock_get.return_value = {}
    
    paths = resolve_knzb_paths()
    
    assert paths["DWH_HOME"] == "gs://your-dwh-home-bucket/dwh"
    assert paths["HOME"] == "gs://your-home-bucket/home"
    assert paths["ISTNS_HOME"] == "gs://your-istns-home-bucket/istns"
```

#### Pass/Fail Criterion
*   **Pass:** The module successfully parses the JSON dictionary, maps keys to the correct return values, and falls back to default values when keys are missing.
*   **Fail:** Any exception is raised during normal parsing, or the returned dictionary keys do not match `DWH_HOME`, `HOME`, and `ISTNS_HOME`.

---

### Test Case 1.2: Metadata Logging Parity (`lese_log_knzb.py`)
#### Purpose
Verify that the logging helper outputs the exact German log format defined in the legacy UC4 JOBI script: `"Protokolleintrag: [task_id] innerhalb [dag_id]"`.

#### Setup
*   A mocked Airflow execution context dictionary containing a mock DAG and Task Instance.

#### Action
Execute the following unit test:

```python
import logging
from unittest.mock import MagicMock
from helpers.lese_log_knzb import log_uc4_metadata

def test_log_uc4_metadata_output(caplog):
    # Mock Airflow Context
    mock_dag = MagicMock()
    mock_dag.dag_id = "dw_dwh_stamm_knzb_abgl_jp"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "dw_dwh_stamm_knzb_abgl_start_js"
    
    context = {
        'dag': mock_dag,
        'task_instance': mock_ti
    }
    
    with caplog.at_level(logging.INFO):
        log_uc4_metadata(context)
        
    expected_log = "Protokolleintrag: dw_dwh_stamm_knzb_abgl_start_js innerhalb dw_dwh_stamm_knzb_abgl_jp"
    assert expected_log in caplog.text
```

#### Pass/Fail Criterion
*   **Pass:** The log output matches the expected string character-for-character, preserving the exact German phrasing.
*   **Fail:** The log output is missing, formatted incorrectly, or contains different wording.

---

## Section 2: State-Control & Lock Validation (Start/End DAGs)

### Test Case 2.1: Start-Block Lock Enforcement (`dw_dwh_stamm_knzb_abgl_start_js`)
#### Purpose
Verify that the start-block DAG (`dw_dwh_stamm_knzb_abgl_start_js`) correctly blocks execution and raises an `AirflowSkipException` if the state is `"GESPERRT"`, mimicking the legacy `:STOP_JOB()` command.

#### Setup
*   Airflow Variable `dw_variablen_knzb` is set to `{"abgleich_status": "GESPERRT", "letzter_lauf": "20260101"}`.

#### Action
Run the `check_and_update_status` task within a test context:

```python
import pytest
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from dw_dwh_stamm_knzb_abgl_start_js import check_and_update_knzb_status

def test_start_js_blocks_when_gesperrt(monkeypatch):
    # Mock Airflow Variable storage
    state = {"abgleich_status": "GESPERRT", "letzter_lauf": "20260101"}
    monkeypatch.setattr(Variable, "get", lambda *args, **kwargs: state)
    
    context = {'ds_nodash': '20260102'}
    
    with pytest.raises(AirflowSkipException) as exc_info:
        check_and_update_knzb_status(**context)
        
    assert "ist gesperrt - Abbruch der Verarbeitung" in str(exc_info.value)
```

#### Pass/Fail Criterion
*   **Pass:** The task raises `AirflowSkipException` with the exact German warning message, and the variable state remains unchanged.
*   **Fail:** The task completes successfully, raises a different exception, or modifies the state variable.

---

### Test Case 2.2: Start-Block State Transition (`dw_dwh_stamm_knzb_abgl_start_js`)
#### Purpose
Verify that when the state is not locked (e.g., `"FREI"`), the start-block transitions the state to `"LAEUFT"` and updates the `letzter_lauf` timestamp to the current execution date.

#### Setup
*   Airflow Variable `dw_variablen_knzb` is set to `{"abgleich_status": "FREI", "letzter_lauf": "20260101"}`.

#### Action
Run the `check_and_update_status` task and assert the updated variable state:

```python
from unittest.mock import patch
from airflow.models import Variable
from dw_dwh_stamm_knzb_abgl_start_js import check_and_update_knzb_status

@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_start_js_transitions_to_laeuft(mock_get, mock_set):
    mock_get.return_value = {"abgleich_status": "FREI", "letzter_lauf": "20260101"}
    context = {'ds_nodash': '20260102'}
    
    check_and_update_knzb_status(**context)
    
    # Verify state updated to LAEUFT and date updated to context date
    expected_updated_state = {
        "abgleich_status": "LAEUFT",
        "letzter_lauf": "20260102"
    }
    mock_set.assert_called_once_with("dw_variablen_knzb", expected_updated_state, serialize_json=True)
```

#### Pass/Fail Criterion
*   **Pass:** The state transitions to `"LAEUFT"`, the execution date is updated to the current run date (`20260102`), and the changes are saved back to the Airflow Variable.
*   **Fail:** The state is not updated, or the date is written incorrectly.

---

### Test Case 2.3: End-Block Lock Release (`dw_dwh_stamm_knzb_abgl_ende_js`)
#### Purpose
Verify that the end-block DAG (`dw_dwh_stamm_knzb_abgl_ende_js`) successfully resets the lock status back to `"FREI"` and outputs the completion log.

#### Setup
*   Airflow Variable `dw_variablen_knzb` is set to `{"abgleich_status": "LAEUFT", "letzter_lauf": "20260102"}`.
*   Airflow Variable `dw_variablen` is populated with valid paths.

#### Action
Run the `release_knzb_lock_callable` task:

```python
from unittest.mock import patch, MagicMock
from airflow.models import Variable
from dw_dwh_stamm_knzb_abgl_ende_js import release_knzb_lock_callable

@patch('dw_dwh_stamm_knzb_abgl_ende_js.resolve_knzb_paths')
@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_end_js_releases_lock(mock_get, mock_set, mock_resolve_paths, capsys):
    mock_resolve_paths.return_value = {"DWH_HOME": "gs://mock-dwh"}
    mock_get.return_value = {"abgleich_status": "LAEUFT", "letzter_lauf": "20260102"}
    
    context = {}
    release_knzb_lock_callable(**context)
    
    # Verify status is reset to FREI
    expected_updated_state = {
        "abgleich_status": "FREI",
        "letzter_lauf": "20260102"
    }
    mock_set.assert_called_once_with("dw_variablen_knzb", expected_updated_state, serialize_json=True)
    
    # Verify exact German completion print statement
    captured = capsys.readouterr()
    assert "KNZB-Stammdatenabgleich fuer Lauf 20260102 erfolgreich beendet" in captured.out
```

#### Pass/Fail Criterion
*   **Pass:** The variable `abgleich_status` is updated to `"FREI"`, and the exact German completion message is printed to standard output.
*   **Fail:** The lock is not released, or the completion message is missing/malformed.

---

## Section 3: Master Pipeline Orchestration & Dependency Validation

### Test Case 3.1: DAG Structure and Dependency Assertions
#### Purpose
Verify that the master DAG (`dw_dwh_stamm_knzb_abgl_jp`) and its child DAGs are loaded into the Airflow Bag without syntax errors, and that the master DAG strictly enforces the linear execution sequence:
`start >> dw_dwh_stamm_knzb_abgl_start_js >> dw_dwh_stamm_knzb_abgl_ende_js >> end`

#### Setup
*   An Airflow environment with the DAG files placed in the `dags/` folder.

#### Action
Execute a structure validation script:

```python
from airflow.models import DagBag

def test_dag_loading_and_dependencies():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    
    # 1. Assert no import errors
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    # 2. Assert DAGs exist
    master_dag = dag_bag.get_dag("dw_dwh_stamm_knzb_abgl_jp")
    start_dag = dag_bag.get_dag("dw_dwh_stamm_knzb_abgl_start_js")
    end_dag = dag_bag.get_dag("dw_dwh_stamm_knzb_abgl_ende_js")
    
    assert master_dag is not None
    assert start_dag is not None
    assert end_dag is not None
    
    # 3. Assert Master DAG Task Dependencies
    start_task = master_dag.get_task("start")
    start_js_task = master_dag.get_task("dw_dwh_stamm_knzb_abgl_start_js")
    end_js_task = master_dag.get_task("dw_dwh_stamm_knzb_abgl_ende_js")
    end_task = master_dag.get_task("end")
    
    assert start_js_task in start_task.downstream_list
    assert end_js_task in start_js_task.downstream_list
    assert end_task in end_js_task.downstream_list
    
    # 4. Assert Concurrency Constraints
    assert master_dag.max_active_runs == 1
    assert start_dag.max_active_runs == 1
    assert end_dag.max_active_runs == 1
```

#### Pass/Fail Criterion
*   **Pass:** All DAGs load successfully with zero import errors, the task dependency chain matches the legacy UC4 sequence exactly, and `max_active_runs` is set to `1` across all components to prevent race conditions.
*   **Fail:** Any import errors are detected, tasks are out of order, or concurrency limits are misconfigured.