Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow DAG and its Python tasks are behaviorally equivalent to the legacy UC4/Automic workflow.

---

## Test Case 1: Path Variable Retrieval (`dw_hole_pfad_vtrg.py`)

### Purpose
To verify that the migrated helper module `dw_hole_pfad_vtrg.py` correctly retrieves path variables from Airflow Variables and falls back to the specified default values if they are not set. This ensures behavioral parity with the legacy `:inc DW.HOLE_PFAD_VTRG` include.

### Setup
* A clean Python testing environment with `pytest` and `pytest-mock` installed.
* Mocked Airflow `Variable.get` interface.

### Action
Execute unit tests that check both the fallback defaults and custom configured values.

```python
# test_dw_hole_pfad_vtrg.py
import pytest
from unittest.mock import patch
from dags.dwh_vertrag.includes.dw_hole_pfad_vtrg import get_path_variables

def test_get_path_variables_defaults():
    """Verify fallback values when Airflow variables are not set."""
    with patch("airflow.models.Variable.get") as mock_get:
        # Simulate missing variables returning default_var
        mock_get.side_effect = lambda key, default_var=None: default_var
        
        paths = get_path_variables()
        
        assert paths["DWH_HOME"] == "/opt/dwh"
        assert paths["HOME"] == "/home/dwh_user"
        assert paths["PMS_HOME"] == "/opt/pms"

def test_get_path_variables_configured():
    """Verify custom values are correctly retrieved from Airflow Variables."""
    configured_paths = {
        "GCP_DWH_HOME": "/custom/gcp/dwh",
        "GCP_HOME": "/custom/gcp/home",
        "GCP_PMS_HOME": "/custom/gcp/pms"
    }
    with patch("airflow.models.Variable.get") as mock_get:
        mock_get.side_effect = lambda key, default_var=None: configured_paths.get(key, default_var)
        
        paths = get_path_variables()
        
        assert paths["DWH_HOME"] == "/custom/gcp/dwh"
        assert paths["HOME"] == "/custom/gcp/home"
        assert paths["PMS_HOME"] == "/custom/gcp/pms"
```

### Pass/Fail Criterion
* **Pass:** The function returns the exact default paths when variables are absent, and returns the configured paths when variables are present.
* **Fail:** Any path key is missing, or the returned values do not match the expected mock values.

---

## Test Case 2: Verbatim Log Output (`dw_lese_log_vtrg.py`)

### Purpose
To verify that the logging helper `dw_lese_log_vtrg.py` preserves the exact German log format (`"Protokolleintrag: {admjob} innerhalb {admjp}"`) as defined in the legacy `:inc DW.LESE_LOG_VTRG` include.

### Setup
* Python environment with `pytest` and standard library `logging` capture.

### Action
Call `write_execution_log` with specific task and DAG identifiers and capture the log output.

```python
# test_dw_lese_log_vtrg.py
import logging
from dags.dwh_vertrag.includes.dw_lese_log_vtrg import write_execution_log

def test_write_execution_log_verbatim(caplog):
    """Verify that the log output matches the legacy German format exactly."""
    test_job = "dw_dwh_vertrag_tarif_sync_start_js"
    test_jp = "dw_dwh_vertrag_tarif_sync_jp"
    
    with caplog.at_level(logging.INFO, logger="airflow.task"):
        write_execution_log(admjob=test_job, admjp=test_jp)
        
    assert len(caplog.records) == 1
    expected_message = f"Protokolleintrag: {test_job} innerhalb {test_jp}"
    assert caplog.records[0].message == expected_message
```

### Pass/Fail Criterion
* **Pass:** The captured log message matches the expected string literal character-for-character.
* **Fail:** The log message is missing, or contains spelling, spacing, or casing deviations from the legacy format.

---

## Test Case 3: Start Task Lock Validation (`dw_dwh_vertrag_tarif_sync_start.py`)

### Purpose
To verify that the start task correctly aborts execution with an `AirflowFailException` and prints the exact legacy German error message when the sync status is locked (`GESPERRT`).

### Setup
* Mocked Airflow `Variable` model.
* Mocked execution context dictionary containing dummy DAG and task objects.

### Action
Set the mocked `DW_VARIABLEN_VTRG_SYNC_STATUS` variable to `"GESPERRT"`, execute `execute_start_task`, and capture stdout and exceptions.

```python
# test_dw_dwh_vertrag_tarif_sync_start.py
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowFailException
from dags.dwh_vertrag.tasks.dw_dwh_vertrag_tarif_sync_start import execute_start_task

def test_execute_start_task_locked(capsys):
    """Verify that the start task fails and prints the correct message when locked."""
    mock_context = {
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task": MagicMock(task_id="dw_dwh_vertrag_tarif_sync_start_js")
    }
    
    # Mock Variable.get to return "GESPERRT"
    with patch("airflow.models.Variable.get") as mock_get, \
         patch("airflow.models.Variable.set") as mock_set:
        
        mock_get.side_effect = lambda key, default_var=None: "GESPERRT" if key == "DW_VARIABLEN_VTRG_SYNC_STATUS" else default_var
        
        with pytest.raises(AirflowFailException) as exc_info:
            execute_start_task(**mock_context)
            
        # Verify exception message
        assert "Job aborted due to GESPERRT status lock." in str(exc_info.value)
        
        # Verify stdout print matches legacy German literal
        captured = capsys.readouterr()
        lauf_datum = datetime.now().strftime("%Y%m%d")
        expected_print = f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch\n"
        assert captured.out == expected_print
        
        # Verify no state mutations occurred
        mock_set.assert_not_called()
```

### Pass/Fail Criterion
* **Pass:** The task raises `AirflowFailException`, prints the exact German warning message with the current date, and does not update any Airflow Variables.
* **Fail:** The task completes successfully, prints a modified message, or attempts to update variables while locked.

---

## Test Case 4: Start Task State Mutation & Execution Log

### Purpose
To verify that when the sync status is unlocked (`FREI`), the start task successfully transitions the state to `LAEUFT`, updates the last run date, and writes the execution log.

### Setup
* Mocked Airflow `Variable` model.
* Mocked execution context dictionary.

### Action
Set the mocked `DW_VARIABLEN_VTRG_SYNC_STATUS` variable to `"FREI"`, execute `execute_start_task`, and assert that state mutations and log calls are executed correctly.

```python
def test_execute_start_task_success(caplog):
    """Verify state transitions and logging when the job is not locked."""
    mock_context = {
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task": MagicMock(task_id="dw_dwh_vertrag_tarif_sync_start_js")
    }
    
    lauf_datum = datetime.now().strftime("%Y%m%d")
    
    with patch("airflow.models.Variable.get") as mock_get, \
         patch("airflow.models.Variable.set") as mock_set, \
         patch("dags.dwh_vertrag.tasks.dw_dwh_vertrag_tarif_sync_start.write_execution_log") as mock_log:
        
        mock_get.side_effect = lambda key, default_var=None: "FREI" if key == "DW_VARIABLEN_VTRG_SYNC_STATUS" else default_var
        
        execute_start_task(**mock_context)
        
        # Verify state mutations
        mock_set.assert_any_call("DW_VARIABLEN_VTRG_SYNC_STATUS", "LAEUFT")
        mock_set.assert_any_call("DW_VARIABLEN_VTRG_LETZTER_LAUF", lauf_datum)
        
        # Verify execution log call
        mock_log.assert_called_once_with(
            admjob="dw_dwh_vertrag_tarif_sync_start_js", 
            admjp="dw_dwh_vertrag_tarif_sync_jp"
        )
```

### Pass/Fail Criterion
* **Pass:** The status is updated to `LAEUFT`, the last run date is set to the current date, and the execution log helper is called with the correct context parameters.
* **Fail:** Variables are not updated, or are updated with incorrect values, or the execution log is not written.

---

## Test Case 5: Ende Task State Reset & Success Log (`dw_dwh_vertrag_tarif_sync_ende.py`)

### Purpose
To verify that the end task successfully resets the sync status back to `FREI`, prints the exact legacy German success message, and writes the final execution log.

### Setup
* Mocked Airflow `Variable` model.
* Mocked execution context dictionary.

### Action
Execute `execute_ende_task` with a mocked last run date of `"20260716"`, and capture stdout and state mutations.

```python
# test_dw_dwh_vertrag_tarif_sync_ende.py
from unittest.mock import patch, MagicMock
from dags.dwh_vertrag.tasks.dw_dwh_vertrag_tarif_sync_ende import execute_ende_task

def test_execute_ende_task_success(capsys):
    """Verify that the end task resets the lock and logs success."""
    mock_context = {
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task": MagicMock(task_id="dw_dwh_vertrag_tarif_sync_ende_js")
    }
    
    with patch("airflow.models.Variable.get") as mock_get, \
         patch("airflow.models.Variable.set") as mock_set, \
         patch("dags.dwh_vertrag.tasks.dw_dwh_vertrag_tarif_sync_ende.write_execution_log") as mock_log:
        
        mock_get.side_effect = lambda key, default_var=None: "20260716" if key == "DW_VARIABLEN_VTRG_LETZTER_LAUF" else default_var
        
        execute_ende_task(**mock_context)
        
        # Verify lock release mutation
        mock_set.assert_called_once_with("DW_VARIABLEN_VTRG_SYNC_STATUS", "FREI")
        
        # Verify stdout print matches legacy German literal
        captured = capsys.readouterr()
        expected_print = "Vertrags-/Tarifabgleich fuer Lauf 20260716 erfolgreich beendet\n"
        assert captured.out == expected_print
        
        # Verify execution log call
        mock_log.assert_called_once_with(
            admjob="dw_dwh_vertrag_tarif_sync_ende_js", 
            admjp="dw_dwh_vertrag_tarif_sync_jp"
        )
```

### Pass/Fail Criterion
* **Pass:** The status is reset to `FREI`, the success message matches the legacy template exactly, and the execution log is written.
* **Fail:** The status is not reset, or the printed success message deviates from the legacy literal.

---

## Test Case 6: DAG Structure & Schedule Validation (`dw_dwh_vertrag_tarif_sync_jp.py`)

### Purpose
To verify that the Airflow DAG structure, task dependencies, and cron schedule match the legacy UC4 Job Plan specifications.

### Setup
* Python environment with the Airflow DAG file loaded.

### Action
Load the DAG object and assert its properties, task sequence, and scheduling parameters.

```python
# test_dw_dwh_vertrag_tarif_sync_jp.py
from airflow.models import DagBag

def test_dag_structure_and_properties():
    """Verify DAG configuration, schedule, and task dependencies."""
    dagbag = DagBag(dag_folder="dags/dwh_vertrag", include_examples=False)
    dag_id = "dw_dwh_vertrag_tarif_sync_jp"
    
    assert dag_id in dagbag.dags, f"DAG {dag_id} failed to load."
    dag = dagbag.get_dag(dag_id)
    
    # Verify Schedule (Weekly on Sunday at 03:00 AM)
    assert dag.schedule_interval == "0 3 * * 7"
    
    # Verify Task IDs
    expected_tasks = {
        "dw_dwh_vertrag_tarif_sync_start_js",
        "dw_dwh_vertrag_tarif_sync_ende_js"
    }
    assert set(dag.task_ids) == expected_tasks
    
    # Verify Task Dependencies (Start >> Ende)
    start_task = dag.get_task("dw_dwh_vertrag_tarif_sync_start_js")
    ende_task = dag.get_task("dw_dwh_vertrag_tarif_sync_ende_js")
    
    assert ende_task in start_task.downstream_list
    assert start_task in ende_task.upstream_list
```

### Pass/Fail Criterion
* **Pass:** The DAG loads without import errors, has the exact schedule `'0 3 * * 7'`, contains both tasks, and enforces the sequential dependency `start >> ende`.
* **Fail:** The DAG fails to load, has an incorrect schedule, or has incorrect task dependencies.