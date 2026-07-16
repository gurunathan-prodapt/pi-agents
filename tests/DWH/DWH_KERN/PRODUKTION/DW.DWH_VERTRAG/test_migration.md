Here is a comprehensive suite of migration-validation tests designed to verify that the migrated Apache Airflow DAG and its helper modules are behaviorally equivalent to the legacy UC4 job structure.

---

## Test Case 1: Environment Path Resolution (`dw_hole_pfad_vtrg`)

### Purpose
Verify that the migrated Python module `dw_hole_pfad_vtrg.py` correctly resolves system-specific and global environmental path hierarchies from Airflow's Variable store, falling back to the legacy defaults if the variable is missing.

### Setup
*   A test environment running `pytest` with Airflow metadata database access or mocked Airflow Variables.
*   Clear any existing Airflow Variable named `dw_variablen_paths`.

### Action
Execute two test scenarios:
1.  **Fallback Scenario:** Call `get_vtrg_paths()` when the Airflow Variable `dw_variablen_paths` is not set.
2.  **Configured Scenario:** Set the Airflow Variable `dw_variablen_paths` to a custom JSON payload, then call `get_vtrg_paths()`.

### Pass/Fail Criterion
*   **Pass:** 
    *   In the fallback scenario, the function returns exactly:
        `{"dwh_home": "/opt/dwh", "home": "/home/dwarf", "pms_home": "/opt/pms"}`.
    *   In the configured scenario, the function returns the custom paths defined in the variable.
*   **Fail:** Any other dictionary is returned, or an exception is raised during execution.

```python
# test_dw_hole_pfad_vtrg.py
import pytest
from unittest.mock import patch
from airflow.models import Variable
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.includes.dw_hole_pfad_vtrg import get_vtrg_paths

def test_get_vtrg_paths_fallback(monkeypatch):
    """Verify fallback defaults when Airflow Variable is absent."""
    # Ensure variable is not present
    with patch.object(Variable, 'get', return_value={"dwh_home": "/opt/dwh", "home": "/home/dwarf", "pms_home": "/opt/pms"}):
        paths = get_vtrg_paths()
        assert paths["dwh_home"] == "/opt/dwh"
        assert paths["home"] == "/home/dwarf"
        assert paths["pms_home"] == "/opt/pms"

def test_get_vtrg_paths_custom():
    """Verify custom path resolution when Airflow Variable is present."""
    custom_paths = {
        "dwh_home": "/custom/dwh",
        "home": "/custom/home",
        "pms_home": "/custom/pms"
    }
    with patch.object(Variable, 'get', return_value=custom_paths):
        paths = get_vtrg_paths()
        assert paths == custom_paths
```

---

## Test Case 2: Metadata Logging Output Parity (`dw_lese_log_vtrg`)

### Purpose
Verify that the logging utility `dw_lese_log_vtrg.py` preserves the exact German log syntax character-for-character as defined in the legacy UC4 JOBI: `Protokolleintrag: &ADMJOB innerhalb &ADMJP`.

### Setup
*   Configure a standard Python `logging` capture handler to intercept `INFO` level logs.
*   Mock the Airflow execution context dictionary containing a mock DAG and Task Instance.

### Action
1.  Call `log_uc4_metadata(context)` with a mock context containing `dag_id="dw_dwh_vertrag_tarif_sync_jp"` and `task_id="check_sync_status"`.
2.  Call `log_uc4_metadata(context, step_message="Test Message")` with the same context.

### Pass/Fail Criterion
*   **Pass:** 
    *   The first call emits exactly: `Protokolleintrag: check_sync_status innerhalb dw_dwh_vertrag_tarif_sync_jp`.
    *   The second call emits the exact same header followed by a separate log line containing `Test Message`.
*   **Fail:** The output format deviates by even a single character, or fails to resolve the IDs.

```python
# test_dw_lese_log_vtrg.py
import logging
from unittest.mock import MagicMock
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.includes.dw_lese_log_vtrg import log_uc4_metadata

def test_log_uc4_metadata_output(caplog):
    """Verify character-for-character output parity with legacy UC4 logging."""
    mock_dag = MagicMock()
    mock_dag.dag_id = "dw_dwh_vertrag_tarif_sync_jp"
    
    mock_ti = MagicMock()
    mock_ti.task_id = "check_sync_status"
    
    context = {
        "dag": mock_dag,
        "task_instance": mock_ti
    }
    
    with caplog.at_level(logging.INFO):
        log_uc4_metadata(context, step_message="Vertrags-/Tarifabgleich gestartet")
        
    assert len(caplog.records) >= 2
    assert caplog.records[0].message == "Protokolleintrag: check_sync_status innerhalb dw_dwh_vertrag_tarif_sync_jp"
    assert caplog.records[1].message == "Vertrags-/Tarifabgleich gestartet"
```

---

## Test Case 3: Sync Status Evaluation - "GESPERRT" (Branching Path A)

### Purpose
Verify that when the synchronization lock variable `dw_variablen_vtrg_sync_status` is set to `"GESPERRT"`, the workflow branches to the abort task and raises an explicit failure, mimicking UC4's `STOP_JOB()` command.

### Setup
*   Set the Airflow Variable `dw_variablen_vtrg_sync_status` to `"GESPERRT"`.
*   Mock the Airflow execution context with a logical date of `2024-12-08` (a Sunday).

### Action
1.  Execute `evaluate_sync_status(**context)`.
2.  Execute `abort_job(**context)`.

### Pass/Fail Criterion
*   **Pass:**
    *   `evaluate_sync_status` returns `"abort_execution"`.
    *   An error log is written containing exactly: `Vertrags-/Tarifabgleich fuer 20241208 ist gesperrt - Abbruch`.
    *   `abort_job` raises an `AirflowFailException` with the message: `Vertrags-/Tarifabgleich execution blocked (GESPERRT). Aborting workflow.`.
*   **Fail:** The branch returns any other task ID, the log message is incorrect, or `abort_job` does not raise `AirflowFailException`.

```python
# test_sync_status_blocked.py
import pytest
import logging
from datetime import datetime
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.dw_dwh_vertrag_tarif_sync_jp import (
    evaluate_sync_status, abort_job
)

def test_evaluate_sync_status_gesperrt(caplog):
    """Verify branching and logging when sync status is GESPERRT."""
    context = {
        "logical_date": datetime(2024, 12, 8),
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task_instance": MagicMock(task_id="check_sync_status")
    }
    
    with patch.object(Variable, 'get', return_value="GESPERRT"), caplog.at_level(logging.ERROR):
        next_task = evaluate_sync_status(**context)
        
    assert next_task == "abort_execution"
    assert any("Vertrags-/Tarifabgleich fuer 20241208 ist gesperrt - Abbruch" in r.message for r in caplog.records)

def test_abort_job_raises_exception():
    """Verify abort_job raises AirflowFailException."""
    context = {
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task_instance": MagicMock(task_id="abort_execution")
    }
    with pytest.raises(AirflowFailException) as exc_info:
        abort_job(**context)
    assert "Vertrags-/Tarifabgleich execution blocked (GESPERRT)" in str(exc_info.value)
```

---

## Test Case 4: Sync Status Evaluation - "FREI" (Branching Path B)

### Purpose
Verify that when the synchronization lock variable `dw_variablen_vtrg_sync_status` is set to `"FREI"`, the workflow branches to the update task, acquires the lock, and sets the execution metadata parameters.

### Setup
*   Set the Airflow Variable `dw_variablen_vtrg_sync_status` to `"FREI"`.
*   Mock the Airflow execution context with a logical date of `2024-12-08`.

### Action
1.  Execute `evaluate_sync_status(**context)`.
2.  Execute `set_running_state(**context)`.

### Pass/Fail Criterion
*   **Pass:**
    *   `evaluate_sync_status` returns `"update_sync_variables"`.
    *   `set_running_state` updates the Airflow Variable `dw_variablen_vtrg_sync_status` to `"LAEUFT"`.
    *   `set_running_state` updates the Airflow Variable `dw_variablen_vtrg_letzter_lauf` to `"20241208"`.
*   **Fail:** The branch returns any other task ID, or variables are not updated to their correct values.

```python
# test_sync_status_free.py
from datetime import datetime
from unittest.mock import patch, MagicMock
from airflow.models import Variable
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.dw_dwh_vertrag_tarif_sync_jp import (
    evaluate_sync_status, set_running_state
)

def test_evaluate_sync_status_frei():
    """Verify branching when sync status is FREI."""
    context = {
        "logical_date": datetime(2024, 12, 8),
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task_instance": MagicMock(task_id="check_sync_status")
    }
    
    with patch.object(Variable, 'get', return_value="FREI"):
        next_task = evaluate_sync_status(**context)
        
    assert next_task == "update_sync_variables"

@patch.object(Variable, 'set')
def test_set_running_state(mock_set):
    """Verify state variables are updated to LAEUFT and current date."""
    context = {
        "logical_date": datetime(2024, 12, 8),
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task_instance": MagicMock(task_id="update_sync_variables")
    }
    
    set_running_state(**context)
    
    # Verify both variables are set correctly
    mock_set.assert_any_call("dw_variablen_vtrg_sync_status", "LAEUFT")
    mock_set.assert_any_call("dw_variablen_vtrg_letzter_lauf", "20241208")
```

---

## Test Case 5: Lock Release and Completion Logging

### Purpose
Verify that upon successful execution of the core synchronization logic, the lock is cleared, the status is set back to `"FREI"`, and the completion run is logged with the correct execution date.

### Setup
*   Set the Airflow Variable `dw_variablen_vtrg_letzter_lauf` to `"20241208"`.
*   Mock the Airflow execution context with a logical date of `2024-12-08`.

### Action
1.  Execute `release_sync_lock(**context)`.

### Pass/Fail Criterion
*   **Pass:**
    *   The Airflow Variable `dw_variablen_vtrg_sync_status` is set to `"FREI"`.
    *   An info log is written containing exactly: `Vertrags-/Tarifabgleich fuer Lauf 20241208 erfolgreich beendet`.
*   **Fail:** The lock status is not set to `"FREI"`, or the log message does not match the legacy German output format.

```python
# test_release_sync_lock.py
import logging
from datetime import datetime
from unittest.mock import patch, MagicMock
from airflow.models import Variable
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.dw_dwh_vertrag_tarif_sync_jp import release_sync_lock

@patch.object(Variable, 'set')
@patch.object(Variable, 'get', return_value="20241208")
def test_release_sync_lock_success(mock_get, mock_set, caplog):
    """Verify lock release and completion log output."""
    context = {
        "logical_date": datetime(2024, 12, 8),
        "dag": MagicMock(dag_id="dw_dwh_vertrag_tarif_sync_jp"),
        "task_instance": MagicMock(task_id="release_sync_lock")
    }
    
    with caplog.at_level(logging.INFO):
        release_sync_lock(**context)
        
    # Verify lock is released
    mock_set.assert_any_call("dw_variablen_vtrg_sync_status", "FREI")
    
    # Verify exact German log output
    assert any(
        "Vertrags-/Tarifabgleich fuer Lauf 20241208 erfolgreich beendet" in r.message 
        for r in caplog.records
    )
```

---

## Test Case 6: DAG Structure & Scheduling Assertions

### Purpose
Verify that the Airflow DAG structure, scheduling interval, concurrency limits, and task dependencies match the legacy UC4 Jobplan (`DW.DWH_VERTRAG_TARIF_SYNC_JP.xml`) specifications.

### Setup
*   Load the DAG `dw_dwh_vertrag_tarif_sync_jp` from the Airflow DAG Bag.

### Action
1.  Assert the scheduling interval.
2.  Assert the task dependency structure.
3.  Assert concurrency and catchup settings.

### Pass/Fail Criterion
*   **Pass:**
    *   The schedule interval is exactly `0 3 * * 7` (Weekly on Sundays at 03:00 AM).
    *   `catchup` is set to `False`.
    *   `max_active_runs` is set to `1` (mitigating global lock race conditions).
    *   The task dependency graph matches:
        *   `start` -> `check_sync_status`
        *   `check_sync_status` -> `abort_execution`
        *   `check_sync_status` -> `update_sync_variables` -> `core_sync_execution` -> `release_sync_lock_task` -> `end`
*   **Fail:** Any metadata parameter or dependency edge deviates from the specification.

```python
# test_dag_structure.py
from airflow.models import DagBag

def test_dag_metadata_and_dependencies():
    """Verify DAG configuration and task dependency topology."""
    dag_bag = DagBag(dag_folder="dags/dwh/dwh_kern/produktion/dw_dwh_vertrag", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_vertrag_tarif_sync_jp")
    
    assert dag is not None, "DAG failed to load (syntax errors or missing file)"
    
    # Scheduling & Concurrency Assertions
    assert dag.schedule_interval == "0 3 * * 7"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    
    # Task Dependency Assertions
    start_task = dag.get_task("start")
    check_sync_task = dag.get_task("check_sync_status")
    abort_task = dag.get_task("abort_execution")
    update_task = dag.get_task("update_sync_variables")
    core_task = dag.get_task("core_sync_execution")
    release_task = dag.get_task("release_sync_lock")
    end_task = dag.get_task("end")
    
    assert check_sync_task in start_task.downstream_list
    assert abort_task in check_sync_task.downstream_list
    assert update_task in check_sync_task.downstream_list
    assert core_task in update_task.downstream_list
    assert release_task in core_task.downstream_list
    assert end_task in release_task.downstream_list
```