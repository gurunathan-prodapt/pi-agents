# Migration Validation Test Suite: `DW.DWH_VERTRAG_TARIF_SYNC_JP`

This document defines the migration-validation tests to prove that the migrated Apache Airflow DAG and its Python helper modules are behaviorally equivalent to the legacy UC4/Automic workflow.

---

## Test Case 1: Start Task - Normal Execution Flow (Status "FREI")

### Purpose
Verify that when the synchronization status is set to `"FREI"`, the `start_task` executes successfully, transitions the status to `"LAEUFT"`, records the execution date in `"LETZTER_LAUF"`, and outputs the correct log messages.

### Setup
* Mock or initialize the Airflow Variable store.
* Set `DW_VARIABLEN_VTRG__SYNC_STATUS` = `"FREI"`.
* Delete or reset `DW_VARIABLEN_VTRG__LETZTER_LAUF`.
* Set environment variables or Airflow Variables for paths: `DWH_HOME="/opt/dwh"`, `HOME="/home/airflow"`, `PMS_HOME="/opt/pms"`.

### Action
Execute the `run_start_js` Python callable within a mocked Airflow task context where the execution date (`ds_nodash`) is set to `"20260720"`.

### Pass/Fail Criterion
* **Pass:** 
  * The task completes without raising any exceptions.
  * `DW_VARIABLEN_VTRG__SYNC_STATUS` is updated to `"LAEUFT"`.
  * `DW_VARIABLEN_VTRG__LETZTER_LAUF` is updated to `"20260720"`.
  * Standard output contains the exact string: `Protokolleintrag: DW.DWH_VERTRAG_TARIF_SYNC_START_JS innerhalb DW_DWH_VERTRAG_TARIF_SYNC_JP`.
* **Fail:** Any exception is raised, variables are not updated correctly, or the log output does not match the legacy format.

### Test Code
```python
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowFailException

# Import the target code
from DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.dw_dwh_vertrag_tarif_sync_jp import run_start_js

@patch("DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.dw_dwh_vertrag_tarif_sync_jp.Variable")
@patch("DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.includes.dw_hole_pfad_vtrg.Variable")
def test_start_task_normal_flow(mock_path_var, mock_dag_var, capsys):
    # Setup variables
    variables_store = {
        "DW_VARIABLEN_VTRG__SYNC_STATUS": "FREI",
        "DWH_HOME": "/opt/dwh",
        "HOME": "/home/airflow",
        "PMS_HOME": "/opt/pms"
    }
    
    def get_var(key, default_var=None):
        return variables_store.get(key, default_var)
        
    def set_var(key, value):
        variables_store[key] = value

    mock_dag_var.get.side_effect = get_var
    mock_dag_var.set.side_effect = set_var
    mock_path_var.get.side_effect = get_var

    # Mock Context
    context = {"ds_nodash": "20260720"}

    # Action
    run_start_js(**context)

    # Assertions
    assert variables_store["DW_VARIABLEN_VTRG__SYNC_STATUS"] == "LAEUFT"
    assert variables_store["DW_VARIABLEN_VTRG__LETZTER_LAUF"] == "20260720"
    
    captured = capsys.readouterr()
    assert "Protokolleintrag: DW.DWH_VERTRAG_TARIF_SYNC_START_JS innerhalb DW_DWH_VERTRAG_TARIF_SYNC_JP" in captured.out
```

---

## Test Case 2: Start Task - Gatekeeper Block (Status "GESPERRT")

### Purpose
Verify that when the synchronization status is set to `"GESPERRT"`, the `start_task` immediately aborts execution, raises an `AirflowFailException`, and prints the exact German error message matching the legacy UC4 output.

### Setup
* Mock or initialize the Airflow Variable store.
* Set `DW_VARIABLEN_VTRG__SYNC_STATUS` = `"GESPERRT"`.

### Action
Execute the `run_start_js` Python callable within a mocked Airflow task context where the execution date (`ds_nodash`) is set to `"20260720"`.

### Pass/Fail Criterion
* **Pass:**
  * The task raises `AirflowFailException`.
  * The exception message and standard output contain the exact string: `Vertrags-/Tarifabgleich fuer 20260720 ist gesperrt - Abbruch`.
  * State variables `DW_VARIABLEN_VTRG__SYNC_STATUS` and `DW_VARIABLEN_VTRG__LETZTER_LAUF` remain unmodified.
* **Fail:** The task completes successfully, raises a different exception, or prints a mismatched message.

### Test Code
```python
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowFailException

from DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.dw_dwh_vertrag_tarif_sync_jp import run_start_js

@patch("DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.dw_dwh_vertrag_tarif_sync_jp.Variable")
@patch("DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.includes.dw_hole_pfad_vtrg.Variable")
def test_start_task_blocked_flow(mock_path_var, mock_dag_var, capsys):
    variables_store = {
        "DW_VARIABLEN_VTRG__SYNC_STATUS": "GESPERRT",
        "DWH_HOME": "/opt/dwh",
        "HOME": "/home/airflow",
        "PMS_HOME": "/opt/pms"
    }
    
    mock_dag_var.get.side_effect = lambda key, default_var=None: variables_store.get(key, default_var)
    mock_dag_var.set.side_effect = lambda key, val: variables_store.update({key: val})
    mock_path_var.get.side_effect = lambda key, default_var=None: variables_store.get(key, default_var)

    context = {"ds_nodash": "20260720"}

    # Action & Assertion
    with pytest.raises(AirflowFailException) as exc_info:
        run_start_js(**context)
        
    assert "Vertrags-/Tarifabgleich fuer 20260720 ist gesperrt - Abbruch" in str(exc_info.value)
    
    # Verify state was not modified
    assert variables_store["DW_VARIABLEN_VTRG__SYNC_STATUS"] == "GESPERRT"
    assert "DW_VARIABLEN_VTRG__LETZTER_LAUF" not in variables_store
```

---

## Test Case 3: Ende Task - Reset and Release Flow

### Purpose
Verify that the `ende_task` successfully resets the synchronization status back to `"FREI"`, reads the correct run date from `"LETZTER_LAUF"`, and outputs the exact legacy-compliant completion log.

### Setup
* Mock or initialize the Airflow Variable store.
* Set `DW_VARIABLEN_VTRG__SYNC_STATUS` = `"LAEUFT"`.
* Set `DW_VARIABLEN_VTRG__LETZTER_LAUF` = `"20260720"`.

### Action
Execute the `run_ende_js` Python callable within a mocked Airflow task context.

### Pass/Fail Criterion
* **Pass:**
  * The task completes without raising any exceptions.
  * `DW_VARIABLEN_VTRG__SYNC_STATUS` is updated to `"FREI"`.
  * Standard output contains the exact string: `Vertrags-/Tarifabgleich fuer Lauf 20260720 erfolgreich beendet`.
  * Standard output contains the exact logging string: `Protokolleintrag: DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS innerhalb DW_DWH_VERTRAG_TARIF_SYNC_JP`.
* **Fail:** The status is not reset to `"FREI"`, or the printed output deviates from the legacy German format.

### Test Code
```python
import pytest
from unittest.mock import MagicMock, patch

from DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.dw_dwh_vertrag_tarif_sync_jp import run_ende_js

@patch("DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.dw_dwh_vertrag_tarif_sync_jp.Variable")
@patch("DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.includes.dw_hole_pfad_vtrg.Variable")
def test_ende_task_flow(mock_path_var, mock_dag_var, capsys):
    variables_store = {
        "DW_VARIABLEN_VTRG__SYNC_STATUS": "LAEUFT",
        "DW_VARIABLEN_VTRG__LETZTER_LAUF": "20260720",
        "DWH_HOME": "/opt/dwh",
        "HOME": "/home/airflow",
        "PMS_HOME": "/opt/pms"
    }
    
    def get_var(key, default_var=None):
        return variables_store.get(key, default_var)
        
    def set_var(key, value):
        variables_store[key] = value

    mock_dag_var.get.side_effect = get_var
    mock_dag_var.set.side_effect = set_var
    mock_path_var.get.side_effect = get_var

    context = {"ds_nodash": "20260720"}

    # Action
    run_ende_js(**context)

    # Assertions
    assert variables_store["DW_VARIABLEN_VTRG__SYNC_STATUS"] == "FREI"
    
    captured = capsys.readouterr()
    assert "Vertrags-/Tarifabgleich fuer Lauf 20260720 erfolgreich beendet" in captured.out
    assert "Protokolleintrag: DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS innerhalb DW_DWH_VERTRAG_TARIF_SYNC_JP" in captured.out
```

---

## Test Case 4: DAG Structural and Metadata Validation

### Purpose
Verify that the Airflow DAG is correctly parsed, contains the exact task sequence (`start_task >> ende_task`), matches the legacy weekly schedule, and enforces the single-concurrency constraint (`max_active_runs=1`).

### Setup
* Load the DAG file `dw_dwh_vertrag_tarif_sync_jp.py` into an Airflow DagBag context.

### Action
Inspect the DAG properties, task IDs, dependencies, and scheduling parameters.

### Pass/Fail Criterion
* **Pass:**
  * The DAG parses with zero import errors.
  * The DAG ID is exactly `"DW_DWH_VERTRAG_TARIF_SYNC_JP"`.
  * The schedule interval is `"0 6 * * 0"` (weekly Sunday morning).
  * `max_active_runs` is set to `1`.
  * The task dependency structure is exactly `start_task` upstream of `ende_task`.
* **Fail:** Import errors occur, schedule/concurrency parameters are misconfigured, or task dependencies are incorrect.

### Test Code
```python
import pytest
from airflow.models import DagBag

def test_dag_structure_and_metadata():
    dagbag = DagBag(dag_folder="dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG", include_examples=False)
    
    # Assert no import errors
    assert len(dagbag.import_errors) == 0, f"DAG import failures: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id="DW_DWH_VERTRAG_TARIF_SYNC_JP")
    assert dag is not None
    
    # Assert scheduling and concurrency
    assert dag.schedule_interval == "0 6 * * 0"
    assert dag.max_active_runs == 1
    
    # Assert task existence and sequence
    assert "start_task" in dag.task_ids
    assert "ende_task" in dag.task_ids
    
    start_task = dag.get_task("start_task")
    ende_task = dag.get_task("ende_task")
    
    assert ende_task in start_task.downstream_list
    assert start_task in ende_task.upstream_list
```