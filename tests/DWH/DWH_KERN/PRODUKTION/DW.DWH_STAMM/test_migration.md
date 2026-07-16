Here is a comprehensive suite of migration-validation tests designed to prove that the re-engineered Apache Airflow DAG and its Python tasks are behaviorally equivalent to the legacy UC4 XML workflow.

---

# Test Case 1: DAG Structure and Metadata Validation

### Purpose
To verify that the migrated Airflow DAG structure, task dependencies, and metadata match the legacy UC4 Job Plan (`DW.DWH_STAMM_KNZB_ABGL_JP.xml`) configuration.

### Setup
* The migrated DAG file `dw_dwh_stamm_knzb_abgl_jp.py` is placed in the Airflow `dags/` directory.
* An active Airflow environment (or a local unit test environment using `pytest` and `apache-airflow`) is running.

### Action
Run a Python unit test using `pytest` to parse the DAG and assert its properties, task IDs, and sequential execution flow.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag

def test_dag_metadata_and_dependencies():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag_id = "dw_dwh_stamm_knzb_abgl_jp"
    
    # 1. Assert DAG exists and loaded without errors
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} failed to load."
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"
    
    # 2. Assert DAG Metadata
    assert dag.owner == "DWH_KERN"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule_interval is None
    
    # 3. Assert Task Inventory
    expected_tasks = {
        "start",
        "dw_dwh_stamm_knzb_abgl_start_js",
        "dw_dwh_stamm_knzb_abgl_ende_js",
        "end"
    }
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # 4. Assert Linear Dependency Chain (start -> start_js -> ende_js -> end)
    start_task = dag.get_task("start")
    start_js_task = dag.get_task("dw_dwh_stamm_knzb_abgl_start_js")
    ende_js_task = dag.get_task("dw_dwh_stamm_knzb_abgl_ende_js")
    end_task = dag.get_task("end")
    
    assert start_js_task.task_id in [t.task_id for t in start_task.downstream_list]
    assert ende_js_task.task_id in [t.task_id for t in start_js_task.downstream_list]
    assert end_task.task_id in [t.task_id for t in ende_js_task.downstream_list]
```

### Pass/Fail Criterion
* **Pass:** The DAG parses successfully with zero import errors, has exactly the 4 specified tasks, and enforces the strict linear dependency chain matching the legacy UC4 columns.
* **Fail:** Any import errors occur, metadata properties differ, or task dependencies do not match the linear sequence.

---

# Test Case 2: Start Task - Lock Detection (`GESPERRT` State)

### Purpose
To verify that `dw_dwh_stamm_knzb_abgl_start_js` correctly detects a locked state (`GESPERRT`), aborts execution immediately, raises an `AirflowFailException`, and outputs the exact legacy German log message.

### Setup
* Mock the Airflow Variable `dw_variablen_knzb_abgleich_status` to return `"GESPERRT"`.
* Mock the helper function `include_hole_pfad_knzb` to return dummy paths.

### Action
Execute the `run_start_js` function within a test harness and capture log outputs and exceptions.

```python
# test_start_js_locked.py
import pytest
import logging
from unittest.mock import patch
from airflow.exceptions import AirflowFailException
from tasks.dw_dwh_stamm_knzb_abgl_start_js import run_start_js

@patch("tasks.dw_dwh_stamm_knzb_abgl_start_js.include_hole_pfad_knzb")
@patch("tasks.dw_dwh_stamm_knzb_abgl_start_js.Variable")
def test_run_start_js_locked(mock_variable, mock_paths, caplog):
    # Setup mocks
    mock_paths.return_value = ("/dummy/dwh", "/dummy/home", "/dummy/istns")
    
    # Mock Variable.get to return "GESPERRT" for status
    def mock_get(key, default_var=None):
        if key == "dw_variablen_knzb_abgleich_status":
            return "GESPERRT"
        return default_var
    mock_variable.get.side_effect = mock_get

    # Execute and assert exception
    with caplog.at_level(logging.ERROR):
        with pytest.raises(AirflowFailException) as exc_info:
            run_start_js()
            
    # Assert exact German log output is preserved
    assert any(
        "ist gesperrt - Abbruch der Verarbeitung" in record.message 
        for record in caplog.records
    ), "Legacy German error log message was not found or altered."
    
    # Assert Variable.set was NOT called (no state changes should occur on failure)
    mock_variable.set.assert_not_called()
```

### Pass/Fail Criterion
* **Pass:** The task raises `AirflowFailException`, logs the exact German string `"KNZB-Abgleich fuer <Datum> ist gesperrt - Abbruch der Verarbeitung"`, and does not alter any Airflow Variables.
* **Fail:** The task completes without raising an exception, or the log output deviates from the legacy German text.

---

# Test Case 3: Start Task - Successful Initialization (`FREI` State)

### Purpose
To verify that when the lock is free (`FREI`), `dw_dwh_stamm_knzb_abgl_start_js` transitions the status to `"LAEUFT"`, updates the last run date, and writes the correct execution logs.

### Setup
* Mock the Airflow Variable `dw_variablen_knzb_abgleich_status` to return `"FREI"`.
* Mock the current system date to a fixed value (e.g., `2026-07-16`).

### Action
Execute the `run_start_js` function and verify state transitions in the Airflow Variable store.

```python
# test_start_js_success.py
import pytest
import logging
from unittest.mock patch, call
from datetime import datetime
from tasks.dw_dwh_stamm_knzb_abgl_start_js import run_start_js

@patch("tasks.dw_dwh_stamm_knzb_abgl_start_js.include_hole_pfad_knzb")
@patch("tasks.dw_dwh_stamm_knzb_abgl_start_js.Variable")
@patch("tasks.dw_dwh_stamm_knzb_abgl_start_js.datetime")
def test_run_start_js_success(mock_datetime, mock_variable, mock_paths, caplog):
    # Setup fixed date
    mock_datetime.now.return_value = datetime(2026, 7, 16)
    mock_paths.return_value = ("/dummy/dwh", "/dummy/home", "/dummy/istns")
    
    # Mock Variable.get to return "FREI"
    mock_variable.get.return_value = "FREI"

    with caplog.at_level(logging.INFO):
        run_start_js()

    # Assert state transitions are written back to Airflow Variables
    mock_variable.set.assert_has_calls([
        call("dw_variablen_knzb_abgleich_status", "LAEUFT"),
        call("dw_variablen_knzb_letzter_lauf", "20260716")
    ], any_order=False)

    # Assert legacy include log output is written verbatim
    assert any(
        "Protokolleintrag: DW.DWH_STAMM_KNZB_ABGL_START_JS innerhalb DW.DWH_STAMM_KNZB_ABGL_JP" in record.message
        for record in caplog.records
    ), "Legacy include log output (DW.LESE_LOG_KNZB) was not written correctly."
```

### Pass/Fail Criterion
* **Pass:** The status variable is updated to `"LAEUFT"`, the run date is updated to `"20260716"`, and the exact German log entry from `DW.LESE_LOG_KNZB` is printed.
* **Fail:** Variables are not updated, are updated with incorrect values, or the log output is missing.

---

# Test Case 4: End Task - Successful Completion and Lock Release

### Purpose
To verify that `dw_dwh_stamm_knzb_abgl_ende_js` successfully resets the lock status to `"FREI"`, reads the correct execution date, and logs the completion messages verbatim.

### Setup
* Mock the Airflow Variable `dw_variablen_knzb_letzter_lauf` to return `"20260716"`.
* Mock the helper function `include_hole_pfad_knzb` to return dummy paths.

### Action
Execute the `run_ende_js` function and verify state transitions and log outputs.

```python
# test_ende_js.py
import pytest
import logging
from unittest.mock import patch, call
from tasks.dw_dwh_stamm_knzb_abgl_ende_js import run_ende_js

@patch("tasks.dw_dwh_stamm_knzb_abgl_ende_js.include_hole_pfad_knzb")
@patch("tasks.dw_dwh_stamm_knzb_abgl_ende_js.Variable")
def test_run_ende_js_success(mock_variable, mock_paths, caplog):
    mock_paths.return_value = ("/dummy/dwh", "/dummy/home", "/dummy/istns")
    
    # Mock Variable.get to return the last run date
    mock_variable.get.return_value = "20260716"

    with caplog.at_level(logging.INFO):
        run_ende_js()

    # Assert lock status is released back to "FREI"
    mock_variable.set.assert_called_once_with("dw_variablen_knzb_abgleich_status", "FREI")

    # Assert verbatim completion log output
    assert any(
        "KNZB-Stammdatenabgleich fuer Lauf 20260716 erfolgreich beendet" in record.message
        for record in caplog.records
    ), "Verbatim completion log output was missing or modified."

    # Assert legacy include log output is written verbatim
    assert any(
        "Protokolleintrag: DW.DWH_STAMM_KNZB_ABGL_ENDE_JS innerhalb DW.DWH_STAMM_KNZB_ABGL_JP" in record.message
        for record in caplog.records
    ), "Legacy include log output (DW.LESE_LOG_KNZB) was not written correctly."
```

### Pass/Fail Criterion
* **Pass:** The status variable is reset to `"FREI"`, and both the completion log and the `DW.LESE_LOG_KNZB` log are printed verbatim in German.
* **Fail:** The lock is not released, or the log messages do not match the legacy output character-for-character.

---

# Test Case 5: Integration and State-Machine Validation

### Purpose
To verify the end-to-end state machine transitions of the workflow across a full execution cycle (Start -> End) using a real/mocked Airflow Metadata database.

### Setup
* Initialize the Airflow Variable `dw_variablen_knzb_abgleich_status` to `"FREI"`.
* Initialize the Airflow Variable `dw_variablen_knzb_letzter_lauf` to `"19700101"`.

### Action
Execute the tasks sequentially as defined in the DAG and assert the state of the variables at each stage.

```python
# test_state_machine_integration.py
import pytest
from datetime import datetime
from airflow.models import Variable
from tasks.dw_dwh_stamm_knzb_abgl_start_js import run_start_js
from tasks.dw_dwh_stamm_knzb_abgl_ende_js import run_ende_js

@pytest.mark.integration
def test_full_workflow_state_machine(db_clean):  # db_clean fixture resets Airflow DB
    # 1. Initial State
    Variable.set("dw_variablen_knzb_abgleich_status", "FREI")
    Variable.set("dw_variablen_knzb_letzter_lauf", "19700101")
    
    # 2. Execute Start Task
    run_start_js()
    
    # Assert intermediate state
    current_date = datetime.now().strftime("%Y%m%d")
    assert Variable.get("dw_variablen_knzb_abgleich_status") == "LAEUFT"
    assert Variable.get("dw_variablen_knzb_letzter_lauf") == current_date
    
    # 3. Execute End Task
    run_ende_js()
    
    # Assert final state
    assert Variable.get("dw_variablen_knzb_abgleich_status") == "FREI"
    assert Variable.get("dw_variablen_knzb_letzter_lauf") == current_date
```

### Pass/Fail Criterion
* **Pass:** The state machine transitions seamlessly: `FREI` -> `LAEUFT` -> `FREI`, and the execution date is updated to the current date.
* **Fail:** Any intermediate state is incorrect, or the final state fails to release the lock.