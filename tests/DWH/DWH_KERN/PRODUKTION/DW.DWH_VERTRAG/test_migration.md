# Migration Validation Test Suite: `DW.DWH_VERTRAG_TARIF_SYNC_JP`

This validation suite contains functional, integration, and state-transition tests to verify that the migrated Apache Airflow DAG (`dw_dwh_vertrag_tarif_sync_jp`) behaves identically to the legacy UC4 orchestration workflow.

---

## Section 1: DAG Structure & Metadata Validation

### Purpose
To verify that the migrated Airflow DAG structure, scheduling, and task dependencies match the legacy UC4 Job Plan (`JOBP`) definition exactly.

### Setup
* The migrated DAG file `dw_dwh_vertrag_tarif_sync_jp.py` is loaded into the Airflow environment.
* A Python testing environment with `pytest` and `apache-airflow` installed.

### Action
Run a unit test using the Airflow DAG bag to inspect the DAG structure, schedule, and task dependencies.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag

def test_dag_metadata_and_dependencies():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag_id = "dw_dwh_vertrag_tarif_sync_jp"
    
    # Assert DAG exists
    assert dag_id in dag_bag.dags, f"DAG {dag_id} failed to load."
    dag = dag_bag.get_dag(dag_id)
    
    # Assert Metadata
    assert dag.schedule_interval == "0 0 * * 0", "Schedule must be weekly (Sunday at midnight)."
    assert dag.catchup is False, "Catchup must be disabled."
    assert dag.max_active_runs == 1, "Max active runs must be restricted to 1 to prevent parallel runs."
    
    # Assert Task Inventory
    expected_tasks = {
        "start",
        "dw_dwh_vertrag_tarif_sync_start_js",
        "dw_dwh_vertrag_tarif_sync_ende_js",
        "end"
    }
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # Assert Sequential Execution Chain
    # start >> start_js >> ende_js >> end
    start_task = dag.get_task("start")
    start_js_task = dag.get_task("dw_dwh_vertrag_tarif_sync_start_js")
    ende_js_task = dag.get_task("dw_dwh_vertrag_tarif_sync_ende_js")
    end_task = dag.get_task("end")
    
    assert start_js_task.task_id in [t.task_id for t in start_task.downstream_list]
    assert ende_js_task.task_id in [t.task_id for t in start_js_task.downstream_list]
    assert end_task.task_id in [t.task_id for t in ende_js_task.downstream_list]
```

### Pass/Fail Criterion
* **Pass**: The DAG loads without import errors, has a weekly cron schedule of `0 0 * * 0`, limits active runs to `1`, and enforces the exact linear execution chain: `start` $\rightarrow$ `start_js` $\rightarrow$ `ende_js` $\rightarrow$ `end`.
* **Fail**: Any import errors occur, or task dependencies/properties deviate from the specification.

---

## Section 2: Start Task Lock Check & State Transitions (Success Path)

### Purpose
To verify that when the synchronization status is `"FREI"`, the start task successfully transitions the system state to `"LAEUFT"`, records the execution date, and outputs the correct log formats.

### Setup
* Mock the Airflow Variable store.
* Initialize `vtrg_sync_status` to `"FREI"`.
* Clear `vtrg_letzter_lauf`.

### Action
Execute the `execute_start_task` function within a mocked Airflow context and assert state changes and log outputs.

```python
# test_start_task_success.py
import logging
from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest
from airflow.models import Variable

from tasks.dw_dwh_vertrag_tarif_sync_start import execute_start_task

@patch("tasks.dw_dwh_vertrag_tarif_sync_start.Variable")
@patch("tasks.dw_dwh_vertrag_tarif_sync_start.resolve_environment_paths")
def test_start_task_success_path(mock_resolve_paths, mock_variable, caplog):
    # Setup mock variables
    mock_variables_store = {
        "vtrg_sync_status": "FREI",
        "DWH_HOME": "/opt/dwh",
        "HOME": "/home/airflow",
        "PMS_HOME": "/opt/pms"
    }
    
    def mock_get(key, default_var=None):
        return mock_variables_store.get(key, default_var)
        
    mock_variable.get.side_effect = mock_get
    
    # Mock context
    mock_context = {
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task": MagicMock(task_id="dw_dwh_vertrag_tarif_sync_start_js")
    }
    
    # Run task
    with caplog.at_level(logging.INFO):
        execute_start_task(**mock_context)
        
    # Assert State Transitions
    expected_date = datetime.now().strftime("%Y%m%d")
    mock_variable.set.assert_any_call("vtrg_sync_status", "LAEUFT")
    mock_variable.set.assert_any_call("vtrg_letzter_lauf", expected_date)
    
    # Assert Standard Log Output (OUTPUT/PRINT LITERAL RULE)
    assert f"Protokolleintrag: dw_dwh_vertrag_tarif_sync_start_js innerhalb dw_dwh_vertrag_tarif_sync_jp" in caplog.text
```

### Pass/Fail Criterion
* **Pass**: The task runs without errors, sets `vtrg_sync_status` to `"LAEUFT"`, sets `vtrg_letzter_lauf` to the current date (`YYYYMMDD`), and prints the exact log literal: `"Protokolleintrag: dw_dwh_vertrag_tarif_sync_start_js innerhalb dw_dwh_vertrag_tarif_sync_jp"`.
* **Fail**: The task raises an exception, fails to update the variables, or outputs incorrect log formats.

---

## Section 3: Start Task Lock Check (Gesperrt / Aborted Path)

### Purpose
To verify that when the synchronization status is `"GESPERRT"`, the start task halts execution immediately, raises an `AirflowFailException`, and logs the exact failure message.

### Setup
* Mock the Airflow Variable store.
* Initialize `vtrg_sync_status` to `"GESPERRT"`.

### Action
Execute the `execute_start_task` function and assert that the expected exception is raised and variables are left unmodified.

```python
# test_start_task_locked.py
import logging
from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest
from airflow.exceptions import AirflowFailException

from tasks.dw_dwh_vertrag_tarif_sync_start import execute_start_task

@patch("tasks.dw_dwh_vertrag_tarif_sync_start.Variable")
def test_start_task_locked_path(mock_variable, caplog):
    # Setup mock variables
    mock_variable.get.side_effect = lambda key, default_var=None: "GESPERRT" if key == "vtrg_sync_status" else default_var
    
    # Mock context
    mock_context = {
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task": MagicMock(task_id="dw_dwh_vertrag_tarif_sync_start_js")
    }
    
    # Run task and assert failure
    lauf_datum = datetime.now().strftime("%Y%m%d")
    with pytest.raises(AirflowFailException) as exc_info:
        with caplog.at_level(logging.ERROR):
            execute_start_task(**mock_context)
            
    # Assert exact error message in logs (OUTPUT/PRINT LITERAL RULE)
    assert f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch" in caplog.text
    assert "Job aborted because sync is locked: GESPERRT" in str(exc_info.value)
    
    # Assert no state variables were updated
    mock_variable.set.assert_not_called()
```

### Pass/Fail Criterion
* **Pass**: The task raises `AirflowFailException`, logs the exact message `"Vertrags-/Tarifabgleich fuer <YYYYMMDD> ist gesperrt - Abbruch"`, and does not call `Variable.set()`.
* **Fail**: The task completes successfully, updates variables, or fails to log the exact error message.

---

## Section 4: End Task Lock Reset & Success Logging

### Purpose
To verify that the end task successfully resets the synchronization lock back to `"FREI"`, reads the last execution run date, and outputs the correct success log literals.

### Setup
* Mock the Airflow Variable store.
* Initialize `vtrg_letzter_lauf` to `"20260716"`.
* Initialize `vtrg_sync_status` to `"LAEUFT"`.

### Action
Execute the `execute_end_task` function within a mocked Airflow context and assert state changes and log outputs.

```python
# test_end_task.py
import logging
from unittest.mock import MagicMock, patch
import pytest

from tasks.dw_dwh_vertrag_tarif_sync_ende import execute_end_task

@patch("tasks.dw_dwh_vertrag_tarif_sync_ende.Variable")
@patch("tasks.dw_dwh_vertrag_tarif_sync_ende.resolve_environment_paths")
def test_end_task_execution(mock_resolve_paths, mock_variable, caplog):
    # Setup mock variables
    mock_variables_store = {
        "vtrg_sync_status": "LAEUFT",
        "vtrg_letzter_lauf": "20260716"
    }
    mock_variable.get.side_effect = lambda key, default_var=None: mock_variables_store.get(key, default_var)
    
    # Mock context
    mock_context = {
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task": MagicMock(task_id="dw_dwh_vertrag_tarif_sync_ende_js")
    }
    
    # Run task
    with caplog.at_level(logging.INFO):
        execute_end_task(**mock_context)
        
    # Assert State Reset
    mock_variable.set.assert_any_call("vtrg_sync_status", "FREI")
    
    # Assert Standard Log Outputs (OUTPUT/PRINT LITERAL RULE)
    assert "Vertrags-/Tarifabgleich fuer Lauf 20260716 erfolgreich beendet" in caplog.text
    assert "Protokolleintrag: dw_dwh_vertrag_tarif_sync_ende_js innerhalb dw_dwh_vertrag_tarif_sync_jp" in caplog.text
```

### Pass/Fail Criterion
* **Pass**: The task runs without errors, sets `vtrg_sync_status` to `"FREI"`, and outputs both exact log literals:
  1. `"Vertrags-/Tarifabgleich fuer Lauf 20260716 erfolgreich beendet"`
  2. `"Protokolleintrag: dw_dwh_vertrag_tarif_sync_ende_js innerhalb dw_dwh_vertrag_tarif_sync_jp"`
* **Fail**: The task fails to reset the status variable, or fails to output the exact log literals.

---

## Section 5: Shared Include Logic (Path Resolution)

### Purpose
To verify that the path resolution utility (`resolve_environment_paths`) correctly falls back to default values or retrieves variables from the Airflow Variable store.

### Setup
* Mock the Airflow Variable store to return custom values for `DWH_HOME`, `HOME`, and `PMS_HOME`.

### Action
Execute `resolve_environment_paths` and assert returned paths.

```python
# test_utils.py
from unittest.mock import patch
from tasks.utils import resolve_environment_paths

@patch("tasks.utils.Variable")
def test_resolve_environment_paths_custom(mock_variable):
    # Mock custom paths
    custom_paths = {
        "DWH_HOME": "/custom/dwh",
        "HOME": "/custom/home",
        "PMS_HOME": "/custom/pms"
    }
    mock_variable.get.side_effect = lambda key, default_var=None: custom_paths.get(key, default_var)
    
    resolved = resolve_environment_paths()
    
    assert resolved["DWH_HOME"] == "/custom/dwh"
    assert resolved["HOME"] == "/custom/home"
    assert resolved["PMS_HOME"] == "/custom/pms"

@patch("tasks.utils.Variable")
def test_resolve_environment_paths_defaults(mock_variable):
    # Mock missing variables to trigger defaults
    mock_variable.get.side_effect = lambda key, default_var=None: default_var
    
    resolved = resolve_environment_paths()
    
    assert resolved["DWH_HOME"] == "/opt/dwh"
    assert resolved["HOME"] == "/home/airflow"
    assert resolved["PMS_HOME"] == "/opt/pms"
```

### Pass/Fail Criterion
* **Pass**: The utility correctly resolves paths using Airflow variables when present, and falls back to the specified defaults (`/opt/dwh`, `/home/airflow`, `/opt/pms`) when variables are missing.
* **Fail**: The utility returns incorrect paths or fails to handle missing variables gracefully.