Here is a comprehensive suite of migration-validation tests designed to prove behavioral equivalence between the legacy UC4 workflow and the migrated Airflow DAG.

---

# Test Suite: `dw_dwh_stamm_knzb_abgl_jp` Validation

## Section 1: Path Resolution & Environment Configuration (`dw_hole_pfad_knzb`)

### Test Case 1.1: Path Resolution with Custom Airflow Variables
* **Purpose**: Verify that `resolve_paths()` correctly retrieves custom values from Airflow Variables when they are defined in the environment.
* **Setup**:
  * Initialize an isolated Airflow metadata database context (or mock `Variable.get`).
  * Set the following Airflow Variables:
    * `DWH_HOME` = `/custom/path/dwh`
    * `HOME` = `/custom/path/home`
    * `ISTNS_HOME` = `/custom/path/istns`
* **Action**: Call `resolve_paths()` from `includes.dw_hole_pfad_knzb`.
* **Pass/Fail Criterion**: The returned dictionary must match the configured variables exactly, and `DWH_JOB_KENNUNG` must equal `"STAMM_KNZB_ABGL"`.

### Test Case 1.2: Path Resolution Fallback Defaults
* **Purpose**: Verify that `resolve_paths()` falls back to the exact legacy-equivalent default paths when Airflow Variables are missing.
* **Setup**:
  * Ensure Airflow Variables `DWH_HOME`, `HOME`, and `ISTNS_HOME` are **not** set in the environment.
* **Action**: Call `resolve_paths()`.
* **Pass/Fail Criterion**: The returned dictionary must match the following defaults:
  * `DWH_HOME` == `"/opt/dwh"`
  * `HOME` == `"/home/dwh_user"`
  * `ISTNS_HOME` == `"/opt/istns"`
  * `DWH_JOB_KENNUNG` == `"STAMM_KNZB_ABGL"`

```python
# pytest code for Section 1
import pytest
from unittest.mock import patch
from includes.dw_hole_pfad_knzb import resolve_paths

def test_resolve_paths_custom_variables():
    mock_vars = {
        "DWH_HOME": "/custom/path/dwh",
        "HOME": "/custom/path/home",
        "ISTNS_HOME": "/custom/path/istns"
    }
    with patch("airflow.models.Variable.get", side_effect=lambda key, default_var=None: mock_vars.get(key, default_var)):
        paths = resolve_paths()
        assert paths["DWH_HOME"] == "/custom/path/dwh"
        assert paths["HOME"] == "/custom/path/home"
        assert paths["ISTNS_HOME"] == "/custom/path/istns"
        assert paths["DWH_JOB_KENNUNG"] == "STAMM_KNZB_ABGL"

def test_resolve_paths_defaults():
    with patch("airflow.models.Variable.get", side_effect=lambda key, default_var=None: default_var):
        paths = resolve_paths()
        assert paths["DWH_HOME"] == "/opt/dwh"
        assert paths["HOME"] == "/home/dwh_user"
        assert paths["ISTNS_HOME"] == "/opt/istns"
        assert paths["DWH_JOB_KENNUNG"] == "STAMM_KNZB_ABGL"
```

---

## Section 2: Logging Output Parity (`dw_lese_log_knzb`)

### Test Case 2.1: Verbatim German Log Pattern Matching
* **Purpose**: Verify that the logging utility outputs the exact German log pattern defined in the legacy UC4 include `DW.LESE_LOG_KNZB.xml`.
* **Setup**: Define a dummy `dag_id` as `"test_dag"` and `task_id` as `"test_task"`.
* **Action**: Execute `log_activity("test_dag", "test_task")` and capture standard output (stdout).
* **Pass/Fail Criterion**: The captured stdout must contain the exact string: `Protokolleintrag: test_task innerhalb test_dag`.

```python
# pytest code for Section 2
import sys
from io import StringIO
from includes.dw_lese_log_knzb import log_activity

def test_log_activity_output_parity():
    captured_output = StringIO()
    sys.stdout = captured_output
    try:
        log_activity(dag_id="test_dag", task_id="test_task")
    finally:
        sys.stdout = sys.__stdout__
    
    expected_output = "Protokolleintrag: test_task innerhalb test_dag\n"
    assert captured_output.getvalue() == expected_output
```

---

## Section 3: Task 1 Initialization & Locking (`dw_dwh_stamm_knzb_abgl_start_js`)

### Test Case 3.1: Execution Blocked when Status is "GESPERRT"
* **Purpose**: Verify that the pipeline immediately aborts and raises an `AirflowFailException` if the global lock variable `dw_variablen_knzb_abgleich_status` is set to `"GESPERRT"`.
* **Setup**:
  * Set Airflow Variable `dw_variablen_knzb_abgleich_status` = `"GESPERRT"`.
  * Mock the Airflow context dictionary with `ds` = `"2024-11-04"`.
* **Action**: Execute the `run_start_js` python callable with the mocked context.
* **Pass/Fail Criterion**: 
  * The task must raise `airflow.exceptions.AirflowFailException`.
  * The exception message and stdout must match the legacy German abort message: `KNZB-Abgleich fuer <YYYYMMdd>d ist gesperrt - Abbruch der Verarbeitung` (where `<YYYYMMdd>d` is the current date formatted with a trailing 'd').

### Test Case 3.2: Successful Execution Initialization
* **Purpose**: Verify that when the lock is free, the start task sets the correct state variables and transitions the lock to `"LAEUFT"`.
* **Setup**:
  * Set Airflow Variable `dw_variablen_knzb_abgleich_status` = `"FREI"`.
  * Mock the Airflow context dictionary with `ds` = `"2024-11-04"`, `dag` as a mock DAG object, and `task` as a mock Task object.
* **Action**: Execute `run_start_js` with the mocked context.
* **Pass/Fail Criterion**:
  * The task must complete successfully without raising exceptions.
  * Airflow Variable `dw_variablen_knzb_abgleich_status` must be updated to `"LAEUFT"`.
  * Airflow Variable `dw_variablen_knzb_letzter_lauf` must be updated to `"2024-11-04"`.

```python
# pytest code for Section 3
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from airflow.exceptions import AirflowFailException
from dw_dwh_stamm_knzb_abgl_jp import run_start_js

@patch("dw_dwh_stamm_knzb_abgl_jp.Variable")
@patch("dw_dwh_stamm_knzb_abgl_jp.resolve_paths")
@patch("dw_dwh_stamm_knzb_abgl_jp.log_activity")
def test_start_js_blocked(mock_log, mock_resolve, mock_variable):
    mock_resolve.return_value = {}
    # Simulate GESPERRT status
    mock_variable.get.side_effect = lambda key, default_var=None: "GESPERRT" if key == "dw_variablen_knzb_abgleich_status" else default_var
    
    context = {"ds": "2024-11-04"}
    lauf_datum = datetime.now().strftime("%Y%m%dd")
    expected_error = f"Aborted: KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung"
    
    with pytest.raises(AirflowFailException) as exc_info:
        run_start_js(**context)
    
    assert expected_error in str(exc_info.value)
    # Ensure status was NOT updated to LAEUFT
    mock_variable.set.assert_not_called()

@patch("dw_dwh_stamm_knzb_abgl_jp.Variable")
@patch("dw_dwh_stamm_knzb_abgl_jp.resolve_paths")
@patch("dw_dwh_stamm_knzb_abgl_jp.log_activity")
def test_start_js_success(mock_log, mock_resolve, mock_variable):
    mock_resolve.return_value = {}
    # Simulate FREI status
    mock_variable.get.side_effect = lambda key, default_var=None: "FREI" if key == "dw_variablen_knzb_abgleich_status" else default_var
    
    mock_dag = MagicMock()
    mock_dag.dag_id = "dw_dwh_stamm_knzb_abgl_jp"
    mock_task = MagicMock()
    mock_task.task_id = "dw_dwh_stamm_knzb_abgl_start_js"
    
    context = {
        "ds": "2024-11-04",
        "dag": mock_dag,
        "task": mock_task
    }
    
    run_start_js(**context)
    
    # Assert state updates
    mock_variable.set.assert_any_call("dw_variablen_knzb_abgleich_status", "LAEUFT")
    mock_variable.set.assert_any_call("dw_variablen_knzb_letzter_lauf", "2024-11-04")
    # Assert logging was triggered
    mock_log.assert_called_once_with("dw_dwh_stamm_knzb_abgl_jp", "dw_dwh_stamm_knzb_abgl_start_js")
```

---

## Section 4: Task 2 Completion & Lock Release (`dw_dwh_stamm_knzb_abgl_ende_js`)

### Test Case 4.1: Lock Release and Completion Logging
* **Purpose**: Verify that the completion task releases the execution lock, logs the correct run date, and prints the exact legacy success message.
* **Setup**:
  * Set Airflow Variable `dw_variablen_knzb_letzter_lauf` = `"2024-11-04"`.
  * Mock the Airflow context with `dag` and `task` objects.
* **Action**: Execute `run_ende_js` with the mocked context.
* **Pass/Fail Criterion**:
  * Airflow Variable `dw_variablen_knzb_abgleich_status` must be set back to `"FREI"`.
  * Captured stdout must contain the exact German success message: `KNZB-Stammdatenabgleich fuer Lauf 2024-11-04 erfolgreich beendet`.
  * The activity logging utility must be called with the correct task and DAG IDs.

```python
# pytest code for Section 4
import sys
from io import StringIO
from unittest.mock import MagicMock, patch
from dw_dwh_stamm_knzb_abgl_jp import run_ende_js

@patch("dw_dwh_stamm_knzb_abgl_jp.Variable")
@patch("dw_dwh_stamm_knzb_abgl_jp.resolve_paths")
@patch("dw_dwh_stamm_knzb_abgl_jp.log_activity")
def test_ende_js_success(mock_log, mock_resolve, mock_variable):
    mock_resolve.return_value = {}
    # Simulate retrieving the last run date
    mock_variable.get.side_effect = lambda key, default_var=None: "2024-11-04" if key == "dw_variablen_knzb_letzter_lauf" else default_var
    
    mock_dag = MagicMock()
    mock_dag.dag_id = "dw_dwh_stamm_knzb_abgl_jp"
    mock_task = MagicMock()
    mock_task.task_id = "dw_dwh_stamm_knzb_abgl_ende_js"
    
    context = {
        "dag": mock_dag,
        "task": mock_task
    }
    
    captured_output = StringIO()
    sys.stdout = captured_output
    try:
        run_ende_js(**context)
    finally:
        sys.stdout = sys.__stdout__
        
    # Assert lock release
    mock_variable.set.assert_any_call("dw_variablen_knzb_abgleich_status", "FREI")
    
    # Assert verbatim output parity
    expected_print = "KNZB-Stammdatenabgleich fuer Lauf 2024-11-04 erfolgreich beendet\n"
    assert expected_print in captured_output.getvalue()
    
    # Assert logging was triggered
    mock_log.assert_called_once_with("dw_dwh_stamm_knzb_abgl_jp", "dw_dwh_stamm_knzb_abgl_ende_js")
```

---

## Section 5: DAG Integrity & Structural Assertions

### Test Case 5.1: DAG Structural Integrity
* **Purpose**: Verify that the DAG is loaded with the correct properties, task dependencies, and schedules matching the legacy design.
* **Setup**: Import the `dag` object from `dw_dwh_stamm_knzb_abgl_jp`.
* **Action**: Inspect DAG attributes and task relationships.
* **Pass/Fail Criterion**:
  * `dag.dag_id` must be `"dw_dwh_stamm_knzb_abgl_jp"`.
  * `dag.schedule_interval` must be `"0 6 * * *"`.
  * `dag.catchup` must be `False`.
  * `dag.max_active_runs` must be `1`.
  * The task dependency chain must be exactly: `dw_dwh_stamm_knzb_abgl_start_js >> dw_dwh_stamm_knzb_abgl_ende_js`.

```python
# pytest code for Section 5
from dw_dwh_stamm_knzb_abgl_jp import dag

def test_dag_structural_integrity():
    assert dag.dag_id == "dw_dwh_stamm_knzb_abgl_jp"
    assert dag.schedule_interval == "0 6 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    
    # Verify Task IDs
    task_ids = list(dag.task_dict.keys())
    assert "dw_dwh_stamm_knzb_abgl_start_js" in task_ids
    assert "dw_dwh_stamm_knzb_abgl_ende_js" in task_ids
    
    # Verify Dependencies
    start_task = dag.get_task("dw_dwh_stamm_knzb_abgl_start_js")
    ende_task = dag.get_task("dw_dwh_stamm_knzb_abgl_ende_js")
    
    assert ende_task in start_task.downstream_list
    assert start_task in ende_task.upstream_list
```