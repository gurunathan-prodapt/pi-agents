Here is a comprehensive suite of migration-validation tests designed to verify that the migrated Airflow DAG behaves identically to the legacy UC4 workflow.

---

# Test Suite: `dw_dwh_vertrag_tarif_sync_jp` Validation

## Section 1 — DAG Structure & Metadata Validation

### Test Case 1.1: DAG Structural Integrity & Dependency Mapping
#### Purpose
Verify that the migrated Airflow DAG matches the legacy UC4 Job Plan (`DW.DWH_VERTRAG_TARIF_SYNC_JP`) structure, task IDs, default arguments, and execution paths.

#### Setup
* The target DAG file `dw_dwh_vertrag_tarif_sync_jp.py` is placed in the Airflow `dags/` directory.
* A Python testing environment with `pytest` and `apache-airflow` installed.

#### Action
Run a unit test that parses the DAG and asserts its structure:
```python
import pytest
from airflow.models import DagBag

def test_dag_metadata_and_dependencies():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag_id = "dw_dwh_vertrag_tarif_sync_jp"
    
    # 1. Assert DAG exists and loaded without import errors
    assert dag_id in dagbag.dags, f"DAG {dag_id} failed to load."
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id)
    
    # 2. Assert Metadata
    assert dag.schedule_interval is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args.get('retries') == 0
    
    # 3. Assert Task Inventory
    expected_tasks = {"check_and_lock_sync", "skip_execution", "execute_sync_dummy", "release_sync_lock"}
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # 4. Assert Downstream Dependencies
    check_task = dag.get_task("check_and_lock_sync")
    assert "execute_sync_dummy" in check_task.downstream_task_ids
    assert "skip_execution" in check_task.downstream_task_ids
    
    dummy_task = dag.get_task("execute_sync_dummy")
    assert "release_sync_lock" in dummy_task.downstream_task_ids
```

#### Pass/Fail Criterion
* **Pass**: The DAG loads cleanly with no import errors, matches all metadata parameters, and contains the exact task dependency graph.
* **Fail**: Any import error occurs, or task dependencies do not match the target design.

---

## Section 2 — State Machine & Lock-Handling Validation

### Test Case 2.1: Guard Task - Locked State (`GESPERRT`)
#### Purpose
Verify that when the synchronization lock variable `dw_variablen_vtrg_sync_status` is set to `GESPERRT`, the workflow branches to `skip_execution` and does **not** alter state variables or run the sync.

#### Setup
* Initialize Airflow Variables:
  * `dw_variablen_vtrg_sync_status` = `GESPERRT`
  * `dw_variablen_vtrg_letzter_lauf` = `20241124`

#### Action
Execute the `check_and_lock_sync` task using a mocked execution context and capture standard output and variable changes.

```python
import pytest
from unittest.mock import MagicMock, patch
from airflow.models import Variable
from dags.dwh_dwh_vertrag.dw_dwh_vertrag_tarif_sync_jp import check_and_lock_sync_status

@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_check_and_lock_sync_status_locked(mock_get, mock_set, capsys):
    # Mock Variable.get calls
    def side_effect_get(key, default_var=None):
        if key == "dw_variablen_vtrg_sync_status":
            return "GESPERRT"
        return default_var
    mock_get.side_effect = side_effect_get

    # Mock Context
    context = {
        'ds_nodash': '20241201',
        'dag': MagicMock(dag_id='dw_dwh_vertrag_tarif_sync_jp'),
        'task': MagicMock(task_id='check_and_lock_sync')
    }

    # Execute
    next_task = check_and_lock_sync_status(**context)

    # Assertions
    assert next_task == "skip_execution"
    
    # Verify no state-changing Variable.set was called
    mock_set.assert_not_called()
    
    # Verify verbatim German abort message in stdout
    captured = capsys.readouterr()
    assert "Vertrags-/Tarifabgleich fuer 20241201 ist gesperrt - Abbruch" in captured.out
```

#### Pass/Fail Criterion
* **Pass**: The task returns `"skip_execution"`, does not call `Variable.set`, and prints the exact German abort message: `"Vertrags-/Tarifabgleich fuer 20241201 ist gesperrt - Abbruch"`.
* **Fail**: The task returns any other branch, updates variables, or prints an incorrect message.

---

### Test Case 2.2: Guard Task - Free State (`FREI`)
#### Purpose
Verify that when the synchronization lock variable is `FREI`, the workflow branches to `execute_sync_dummy`, updates the lock status to `LAEUFT`, and sets the last run date.

#### Setup
* Initialize Airflow Variables:
  * `dw_variablen_vtrg_sync_status` = `FREI`

#### Action
Execute the `check_and_lock_sync` task using a mocked execution context.

```python
@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_check_and_lock_sync_status_free(mock_get, mock_set):
    # Mock Variable.get calls
    def side_effect_get(key, default_var=None):
        if key == "dw_variablen_vtrg_sync_status":
            return "FREI"
        return default_var
    mock_get.side_effect = side_effect_get

    # Mock Context
    context = {
        'ds_nodash': '20241201',
        'dag': MagicMock(dag_id='dw_dwh_vertrag_tarif_sync_jp'),
        'task': MagicMock(task_id='check_and_lock_sync')
    }

    # Execute
    next_task = check_and_lock_sync_status(**context)

    # Assertions
    assert next_task == "execute_sync_dummy"
    
    # Verify atomic state updates
    mock_set.assert_any_call("dw_variablen_vtrg_sync_status", "LAEUFT")
    mock_set.assert_any_call("dw_variablen_vtrg_letzter_lauf", "20241201")
```

#### Pass/Fail Criterion
* **Pass**: The task returns `"execute_sync_dummy"` and updates `dw_variablen_vtrg_sync_status` to `"LAEUFT"` and `dw_variablen_vtrg_letzter_lauf` to `"20241201"`.
* **Fail**: The task branches incorrectly or fails to update the variables with the correct values.

---

### Test Case 2.3: Lock Release Task (`release_sync_lock`)
#### Purpose
Verify that the `release_sync_lock` task resets the synchronization lock back to `FREI` and prints the verbatim success message.

#### Setup
* Initialize Airflow Variables:
  * `dw_variablen_vtrg_letzter_lauf` = `20241201`

#### Action
Execute the `release_sync_lock_status` task using a mocked execution context.

```python
from dags.dwh_dwh_vertrag.dw_dwh_vertrag_tarif_sync_jp import release_sync_lock_status

@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get')
def test_release_sync_lock_status(mock_get, mock_set, capsys):
    # Mock Variable.get calls
    def side_effect_get(key, default_var=None):
        if key == "dw_variablen_vtrg_letzter_lauf":
            return "20241201"
        return default_var
    mock_get.side_effect = side_effect_get

    # Mock Context
    context = {
        'ds_nodash': '20241201',
        'dag': MagicMock(dag_id='dw_dwh_vertrag_tarif_sync_jp'),
        'task': MagicMock(task_id='release_sync_lock')
    }

    # Execute
    release_sync_lock_status(**context)

    # Assertions
    mock_set.assert_called_once_with("dw_variablen_vtrg_sync_status", "FREI")
    
    # Verify verbatim German success message in stdout
    captured = capsys.readouterr()
    assert "Vertrags-/Tarifabgleich fuer Lauf 20241201 erfolgreich beendet" in captured.out
```

#### Pass/Fail Criterion
* **Pass**: The task sets `dw_variablen_vtrg_sync_status` to `"FREI"` and prints `"Vertrags-/Tarifabgleich fuer Lauf 20241201 erfolgreich beendet"`.
* **Fail**: The variable is not reset to `"FREI"`, or the output message does not match the legacy format.

---

## Section 3 — Modular Includes & Logging Validation

### Test Case 3.1: Environment Path Resolution (`dw_hole_pfad_vtrg`)
#### Purpose
Verify that the modularized include `dw_hole_pfad_vtrg.py` correctly resolves environment paths from the Airflow Variable store, falling back to safe defaults if they are missing.

#### Setup
* Clear or set specific values in the Airflow Variable store.

#### Action
Execute `load_env_paths()` under two scenarios: with variables defined, and using defaults.

```python
from dags.dwh_dwh_vertrag.includes.dw_hole_pfad_vtrg import load_env_paths

@patch('airflow.models.Variable.get')
def test_load_env_paths_custom(mock_get):
    def side_effect_get(key, default_var=None):
        mapping = {
            "dw_variablen_dwh_home": "/custom/dwh",
            "dw_variablen_home": "/custom/home",
            "dw_variablen_pms_home": "/custom/pms"
        }
        return mapping.get(key, default_var)
    mock_get.side_effect = side_effect_get

    paths = load_env_paths()
    assert paths["DWH_HOME"] == "/custom/dwh"
    assert paths["HOME"] == "/custom/home"
    assert paths["PMS_HOME"] == "/custom/pms"

@patch('airflow.models.Variable.get')
def test_load_env_paths_defaults(mock_get):
    # Force fallback to default_var
    mock_get.side_effect = lambda key, default_var=None: default_var

    paths = load_env_paths()
    assert paths["DWH_HOME"] == "/opt/dwh"
    assert paths["HOME"] == "/home/dwh"
    assert paths["PMS_HOME"] == "/opt/pms"
```

#### Pass/Fail Criterion
* **Pass**: The helper correctly returns custom paths when defined, and falls back to the exact legacy-equivalent defaults (`/opt/dwh`, `/home/dwh`, `/opt/pms`) when undefined.
* **Fail**: Paths are resolved incorrectly or raise exceptions.

---

### Test Case 3.2: Verbatim Logging Utility (`dw_lese_log_vtrg`)
#### Purpose
Verify that the logging utility prints the exact German log format matching the legacy UC4 include `DW.LESE_LOG_VTRG`.

#### Setup
* None.

#### Action
Call `log_execution_status` with test parameters and capture standard output.

```python
from dags.dwh_dwh_vertrag.includes.dw_lese_log_vtrg import log_execution_status

def test_log_execution_status(capsys):
    log_execution_status(dag_id="TEST_DAG", task_id="TEST_TASK")
    captured = capsys.readouterr()
    assert captured.out.strip() == "Protokolleintrag: TEST_TASK innerhalb TEST_DAG"
```

#### Pass/Fail Criterion
* **Pass**: The output matches the string `"Protokolleintrag: TEST_TASK innerhalb TEST_DAG"` exactly.
* **Fail**: The output string differs in casing, spacing, or wording.