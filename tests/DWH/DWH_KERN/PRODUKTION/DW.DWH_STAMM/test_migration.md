# Migration Validation Test Suite: `DW_DWH_STAMM_KNZB_ABGL_JP`

This document contains production-grade migration-validation tests to verify that the migrated Airflow DAG and its helper modules behave identically to the legacy UC4/Automic Job Plan (`DW.DWH_STAMM_KNZB_ABGL_JP`).

---

## Test Case 1: DAG Structural and Dependency Validation
### Purpose
Verify that the migrated Airflow DAG preserves the exact task structure, scheduling, default arguments, and sequential execution order of the legacy UC4 Job Plan.

### Setup
* The target DAG file `DW_DWH_STAMM_KNZB_ABGL_JP.py` is placed in the Airflow `dags/` directory.
* An active Airflow environment (or a local unit-test environment with `pytest` and `apache-airflow` installed).

### Action
Run a programmatic unit test using `pytest` to parse the DAG and assert its structure, task dependencies, and metadata.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag

def test_dag_structure_and_properties():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag_id = "DW_DWH_STAMM_KNZB_ABGL_JP"
    
    # 1. Assert DAG exists and loaded without import errors
    assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag"
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id)
    
    # 2. Assert Metadata and Scheduling
    assert dag.schedule_interval == "0 4 * * *", "Schedule interval must be daily at 04:00 UTC"
    assert dag.catchup is False, "Catchup must be disabled"
    assert "dwh" in dag.tags
    assert "uc4_migration" in dag.tags
    
    # 3. Assert Task Presence
    expected_tasks = {"DW_DWH_STAMM_KNZB_ABGL_START_JS", "DW_DWH_STAMM_KNZB_ABGL_ENDE_JS"}
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # 4. Assert Sequential Execution Order (start_task >> ende_task)
    start_task = dag.get_task("DW_DWH_STAMM_KNZB_ABGL_START_JS")
    ende_task = dag.get_task("DW_DWH_STAMM_KNZB_ABGL_ENDE_JS")
    
    assert ende_task.task_id in [t.task_id for t in start_task.downstream_list], "End task must be downstream of Start task"
    assert start_task.task_id in [t.task_id for t in ende_task.upstream_list], "Start task must be upstream of End task"
```

### Pass/Fail Criterion
* **Pass**: The test executes successfully with zero import errors, confirming exact task names, scheduling, and sequential dependencies.
* **Fail**: Any import error occurs, or task dependencies do not match the legacy sequence.

---

## Test Case 2: Start Task Execution - State "FREI" (Happy Path)
### Purpose
Verify that when the global state `ABGLEICH_STATUS` is `"FREI"`, the start task successfully transitions the state to `"LAEUFT"`, updates `LETZTER_LAUF` to the current date, and logs the correct German output.

### Setup
* Mock the Airflow `Variable` model to simulate pre-existing variables.
* Initialize `DW_VARIABLEN_KNZB` with `{"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": "20241103"}`.
* Initialize `DW_VARIABLEN` with standard path variables.

### Action
Execute the `execute_start_js` Python callable within a mocked Airflow context and capture logs and variable updates.

```python
# test_start_task_happy_path.py
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from airflow.exceptions import AirflowFailException

# Import the target function
from dags.DW.DWH_KERN.PRODUKTION.DW_DWH_STAMM.DW_DWH_STAMM_KNZB_ABGL_JP import execute_start_js

@patch("DW_HOLE_PFAD_KNZB.Variable")
@patch("DW_DWH_STAMM_KNZB_ABGL_JP.Variable")
def test_execute_start_js_frei(mock_var_main, mock_var_include, caplog):
    import logging
    caplog.set_level(logging.INFO)
    
    # Mock DW_VARIABLEN (for include_hole_pfad_knzb)
    mock_var_include.get.return_value = {
        "DWH_HOME": "/opt/dwh",
        "HOME": "/home/airflow",
        "ISTNS_HOME": "/opt/istns"
    }
    
    # Mock DW_VARIABLEN_KNZB (for execute_start_js)
    mock_var_main.get.return_value = {
        "ABGLEICH_STATUS": "FREI",
        "LETZTER_LAUF": "20241103"
    }
    
    # Mock Airflow Context
    mock_context = {
        'task': MagicMock(task_id='DW_DWH_STAMM_KNZB_ABGL_START_JS'),
        'dag': MagicMock(dag_id='DW_DWH_STAMM_KNZB_ABGL_JP')
    }
    
    # Run the task
    execute_start_js(**mock_context)
    
    # Assert Variable Updates
    mock_var_main.set.assert_called_once()
    called_key, called_val = mock_var_main.set.call_args[0]
    assert called_key == "DW_VARIABLEN_KNZB"
    assert called_val["ABGLEICH_STATUS"] == "LAEUFT"
    assert called_val["LETZTER_LAUF"] == datetime.now().strftime("%Y%m%d")
    
    # Assert Verbatim German Log Output from JOBI DW.LESE_LOG_KNZB
    expected_log = "Protokolleintrag: DW_DWH_STAMM_KNZB_ABGL_START_JS innerhalb DW_DWH_STAMM_KNZB_ABGL_JP"
    assert any(expected_log in record.message for record in caplog.records)
```

### Pass/Fail Criterion
* **Pass**: The variable `ABGLEICH_STATUS` is updated to `"LAEUFT"`, `LETZTER_LAUF` is set to today's date, and the exact German log message is printed.
* **Fail**: The task raises an exception, variables are not updated, or the log output does not match.

---

## Test Case 3: Start Task Execution - State "GESPERRT" (Abrupt Stop Path)
### Purpose
Verify that when the global state `ABGLEICH_STATUS` is `"GESPERRT"`, the start task halts execution immediately, raises an `AirflowFailException` (equivalent to legacy `STOP_JOB()`), and logs the exact German warning message.

### Setup
* Initialize `DW_VARIABLEN_KNZB` with `{"ABGLEICH_STATUS": "GESPERRT", "LETZTER_LAUF": "20241103"}`.

### Action
Execute the `execute_start_js` Python callable and assert that it raises `AirflowFailException`.

```python
# test_start_task_locked.py
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from airflow.exceptions import AirflowFailException

from dags.DW.DWH_KERN.PRODUKTION.DW_DWH_STAMM.DW_DWH_STAMM_KNZB_ABGL_JP import execute_start_js

@patch("DW_HOLE_PFAD_KNZB.Variable")
@patch("DW_DWH_STAMM_KNZB_ABGL_JP.Variable")
def test_execute_start_js_gesperrt(mock_var_main, mock_var_include, caplog):
    import logging
    caplog.set_level(logging.ERROR)
    
    # Mock DW_VARIABLEN
    mock_var_include.get.return_value = {
        "DWH_HOME": "/opt/dwh",
        "HOME": "/home/airflow",
        "ISTNS_HOME": "/opt/istns"
    }
    
    # Mock DW_VARIABLEN_KNZB as GESPERRT
    mock_var_main.get.return_value = {
        "ABGLEICH_STATUS": "GESPERRT",
        "LETZTER_LAUF": "20241103"
    }
    
    mock_context = {
        'task': MagicMock(task_id='DW_DWH_STAMM_KNZB_ABGL_START_JS'),
        'dag': MagicMock(dag_id='DW_DWH_STAMM_KNZB_ABGL_JP')
    }
    
    # Assert that AirflowFailException is raised (equivalent to legacy STOP_JOB())
    with pytest.raises(AirflowFailException) as exc_info:
        execute_start_js(**mock_context)
        
    assert "Abrupt stop triggered by legacy logic" in str(exc_info.value)
    
    # Assert Verbatim German Log Output
    today_str = datetime.now().strftime("%Y%m%d")
    expected_error_log = f"KNZB-Abgleich fuer {today_str} ist gesperrt - Abbruch der Verarbeitung"
    assert any(expected_error_log in record.message for record in caplog.records)
    
    # Assert that variables were NOT modified
    mock_var_main.set.assert_not_called()
```

### Pass/Fail Criterion
* **Pass**: The task raises `AirflowFailException`, logs the exact German warning message, and does not modify the variable store.
* **Fail**: The task completes without raising an exception, or the log message is missing/incorrect.

---

## Test Case 4: End Task Execution (Lock Release & Completion Log)
### Purpose
Verify that the end task successfully resets the state `ABGLEICH_STATUS` back to `"FREI"`, retrieves the correct run date, and prints the verbatim German completion statement.

### Setup
* Initialize `DW_VARIABLEN_KNZB` with `{"ABGLEICH_STATUS": "LAEUFT", "LETZTER_LAUF": "20241104"}`.

### Action
Execute the `execute_ende_js` Python callable within a mocked Airflow context and capture logs and variable updates.

```python
# test_end_task.py
import pytest
from unittest.mock import MagicMock, patch

from dags.DW.DWH_KERN.PRODUKTION.DW_DWH_STAMM.DW_DWH_STAMM_KNZB_ABGL_JP import execute_ende_js

@patch("DW_HOLE_PFAD_KNZB.Variable")
@patch("DW_DWH_STAMM_KNZB_ABGL_JP.Variable")
def test_execute_ende_js(mock_var_main, mock_var_include, caplog):
    import logging
    caplog.set_level(logging.INFO)
    
    # Mock DW_VARIABLEN
    mock_var_include.get.return_value = {
        "DWH_HOME": "/opt/dwh",
        "HOME": "/home/airflow",
        "ISTNS_HOME": "/opt/istns"
    }
    
    # Mock DW_VARIABLEN_KNZB
    mock_var_main.get.return_value = {
        "ABGLEICH_STATUS": "LAEUFT",
        "LETZTER_LAUF": "20241104"
    }
    
    mock_context = {
        'task': MagicMock(task_id='DW_DWH_STAMM_KNZB_ABGL_ENDE_JS'),
        'dag': MagicMock(dag_id='DW_DWH_STAMM_KNZB_ABGL_JP')
    }
    
    # Run the task
    execute_ende_js(**mock_context)
    
    # Assert Variable Reset to FREI
    mock_var_main.set.assert_called_once()
    called_key, called_val = mock_var_main.set.call_args[0]
    assert called_key == "DW_VARIABLEN_KNZB"
    assert called_val["ABGLEICH_STATUS"] == "FREI"
    assert called_val["LETZTER_LAUF"] == "20241104"  # Retained
    
    # Assert Verbatim German Completion Log Output
    expected_completion_log = "KNZB-Stammdatenabgleich fuer Lauf 20241104 erfolgreich beendet"
    assert any(expected_completion_log in record.message for record in caplog.records)
    
    # Assert Verbatim German Log Output from JOBI DW.LESE_LOG_KNZB
    expected_include_log = "Protokolleintrag: DW_DWH_STAMM_KNZB_ABGL_ENDE_JS innerhalb DW_DWH_STAMM_KNZB_ABGL_JP"
    assert any(expected_include_log in record.message for record in caplog.records)
```

### Pass/Fail Criterion
* **Pass**: The variable `ABGLEICH_STATUS` is reset to `"FREI"`, and both the completion log and the include log are printed verbatim in German.
* **Fail**: The variable is not reset, or the logs do not match the legacy output specifications.