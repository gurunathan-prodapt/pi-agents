As a senior data-migration QA engineer, I've analyzed the provided migration design and the generated Airflow DAG for `DW.BERT_ABLAUFSTEUERUNG`. The tests below are designed to validate the behavioral equivalence of the migrated orchestration logic, covering the specified criteria.

The tests will primarily use `pytest` for structural and configuration validation of the Airflow DAG. For runtime behavior (like `TimeSensor` or `TriggerDagRunOperator` interactions), I will describe the necessary observational or simulated testing approaches.

To run the `pytest` examples, ensure you have `pytest` installed (`pip install pytest`) and that the `dw_bert_ablaufsteuerung.py` file is accessible in a directory that `pytest` can discover (e.g., a `dags` folder, and your test file in a `tests` folder at the same level).

```python
# tests/conftest.py (Optional, for common fixtures or setup)
import pytest
from airflow.models.dagbag import DagBag
import os

@pytest.fixture(scope="session")
def dagbag():
    """Fixture to load the DAGs from the dags folder."""
    # Assuming the DAG file is in a 'dags' directory relative to the test file
    # Adjust this path if your DAGs are located elsewhere
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dags_folder = os.path.join(current_dir, "../dags") # Adjust if dags folder is elsewhere
    
    # Ensure the dags folder exists
    if not os.path.exists(dags_folder):
        # Fallback: try to load from current directory if dags folder not found
        dags_folder = current_dir

    # DagBag loads all DAGs in the specified folder
    # include_examples=False to avoid loading Airflow's example DAGs
    # store_serialized_dags=False to avoid issues with local testing setup
    return DagBag(dag_folder=dags_folder, include_examples=False, store_serialized_dags=False)

@pytest.fixture(scope="session")
def bert_dag(dagbag):
    """Fixture to get the specific DW.BERT_ABLAUFSTEUERUNG DAG."""
    dag_id = 'dw_bert_ablaufsteuerung'
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} not found in dagbag."
    return dag

```

---

## 1. DAG Metadata and Structure Validation

**Purpose:** To verify that the core properties of the migrated Airflow DAG (e.g., `dag_id`, `schedule_interval`, `start_date`, `max_active_runs`, `catchup`, `default_args`) match the design document and that all expected tasks are present and of the correct type. This covers aspects of "Data-quality / schema assertions" for the DAG itself.

**Setup:**
1.  Ensure the `dw_bert_ablaufsteuerung.py` file is placed in a directory discoverable by Airflow (e.g., a `dags` folder).
2.  Have `pytest` and `apache-airflow` installed in your test environment.

**Action:**
Run the `pytest` suite. The test will load the DAG and assert its properties and the presence and type of each task.

**Pass/Fail Criterion:**
The test passes if all assertions hold true:
*   `dag_id` is `dw_bert_ablaufsteuerung`.
*   `schedule_interval` is `0 0 * * *`.
*   `catchup` is `False`.
*   `max_active_runs` is `1`.
*   `is_paused_upon_creation` is `False`.
*   `default_args.owner` is `data-platform`.
*   `default_args.retries` is `0`.
*   `default_args.retry_delay` is `timedelta(minutes=0)`.
*   All 15 tasks listed in the design document are present in the DAG.
*   Each task is an instance of the expected Airflow Operator type (`PythonOperator`, `TimeSensor`, `ShortCircuitOperator`, `TriggerDagRunOperator`).

**Runnable Test Code (`tests/test_dag_structure.py`):**

```python
import pytest
from datetime import timedelta, time
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.time import TimeSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Assuming conftest.py with dagbag and bert_dag fixtures is set up

def test_dag_properties(bert_dag):
    """Verify core DAG properties."""
    assert bert_dag.dag_id == 'dw_bert_ablaufsteuerung'
    assert bert_dag.schedule_interval == '0 0 * * *'
    assert not bert_dag.catchup
    assert bert_dag.max_active_runs == 1
    assert not bert_dag.is_paused_upon_creation
    assert bert_dag.default_args['owner'] == 'data-platform'
    assert bert_dag.default_args['retries'] == 0
    assert bert_dag.default_args['retry_delay'] == timedelta(minutes=0)
    assert bert_dag.default_args['on_failure_callback'] is not None

def test_all_tasks_present_and_correct_type(bert_dag):
    """Verify all expected tasks are present and are of the correct operator type."""
    expected_tasks = {
        'guard_active_run': PythonOperator,
        'wait_until_20_00_for_monthly_jp': TimeSensor,
        'calendar_check_dw_new_calendar_1': ShortCircuitOperator,
        'trigger_dw_bert_monatlich_jp': TriggerDagRunOperator,
        'wait_until_04_03_for_housekeeping_jp': TimeSensor,
        'calendar_check_dw_new_calendar_2': ShortCircuitOperator,
        'trigger_dw_bert_adm_housekeeping_jp': TriggerDagRunOperator,
        'wait_until_01_30_for_daily_export_jp': TimeSensor,
        'trigger_dw_dwh_apt_export_taeglich_jp': TriggerDagRunOperator,
        'wait_until_01_00_for_stammdaten_jp': TimeSensor,
        'trigger_dw_bert_stammdaten_jp': TriggerDagRunOperator,
        'wait_until_07_00_for_adm_check_evt': TimeSensor,
        'trigger_dw_bert_run_adm_check_jp_evt': TriggerDagRunOperator,
        'wait_until_01_00_for_monthly_export_evt': TimeSensor,
        'trigger_dw_dwh_run_apt_export_monatlich_jp_evt': TriggerDagRunOperator,
    }

    assert len(bert_dag.tasks) == len(expected_tasks), \
        f"Expected {len(expected_tasks)} tasks, but found {len(bert_dag.tasks)}"

    for task_id, expected_type in expected_tasks.items():
        task = bert_dag.get_task(task_id)
        assert task is not None, f"Task {task_id} not found in DAG."
        assert isinstance(task, expected_type), \
            f"Task {task_id} is of type {type(task).__name__}, expected {expected_type.__name__}."

```

---

## 2. Task Dependency Validation

**Purpose:** To ensure that the sequence of tasks and their dependencies in the Airflow DAG precisely match the "Execution Order" specified in Section 4 of the design document. This directly addresses "Output parity" in terms of job flow.

**Setup:**
1.  Same as Test 1.

**Action:**
Run the `pytest` suite. The test will inspect the `upstream_task_ids` of each task to verify the dependencies.

**Pass/Fail Criterion:**
The test passes if the `upstream_task_ids` for each task exactly match the defined dependencies in the design document.

**Runnable Test Code (`tests/test_dag_dependencies.py`):**

```python
import pytest

# Assuming conftest.py with dagbag and bert_dag fixtures is set up

def test_task_dependencies(bert_dag):
    """Verify the exact task dependencies match the design."""
    expected_dependencies = {
        'guard_active_run': set(), # No upstream tasks
        'wait_until_20_00_for_monthly_jp': {'guard_active_run'},
        'calendar_check_dw_new_calendar_1': {'wait_until_20_00_for_monthly_jp'},
        'trigger_dw_bert_monatlich_jp': {'calendar_check_dw_new_calendar_1'},
        'wait_until_04_03_for_housekeeping_jp': {'trigger_dw_bert_monatlich_jp'},
        'calendar_check_dw_new_calendar_2': {'wait_until_04_03_for_housekeeping_jp'},
        'trigger_dw_bert_adm_housekeeping_jp': {'calendar_check_dw_new_calendar_2'},
        'wait_until_01_30_for_daily_export_jp': {'trigger_dw_bert_adm_housekeeping_jp'},
        'trigger_dw_dwh_apt_export_taeglich_jp': {'wait_until_01_30_for_daily_export_jp'},
        'wait_until_01_00_for_stammdaten_jp': {'trigger_dw_dwh_apt_export_taeglich_jp'},
        'trigger_dw_bert_stammdaten_jp': {'wait_until_01_00_for_stammdaten_jp'},
        'wait_until_07_00_for_adm_check_evt': {'trigger_dw_bert_stammdaten_jp'},
        'trigger_dw_bert_run_adm_check_jp_evt': {'wait_until_07_00_for_adm_check_evt'},
        'wait_until_01_00_for_monthly_export_evt': {'trigger_dw_bert_run_adm_check_jp_evt'},
        'trigger_dw_dwh_run_apt_export_monatlich_jp_evt': {'wait_until_01_00_for_monthly_export_evt'},
    }

    for task_id, expected_upstream in expected_dependencies.items():
        task = bert_dag.get_task(task_id)
        assert task is not None, f"Task {task_id} not found."
        assert set(task.upstream_task_ids) == expected_upstream, \
            f"Dependencies for task {task_id} are incorrect. Expected {expected_upstream}, got {set(task.upstream_task_ids)}."

```

---

## 3. Time Sensor Logic Validation

**Purpose:** To verify that all `TimeSensor` tasks are configured with the exact `target_time` specified in the design document, ensuring the timing of execution matches the legacy UC4 job. This contributes to "Output parity".

**Setup:**
1.  Same as Test 1.

**Action:**
Run the `pytest` suite. The test will retrieve the `target_time` attribute from each `TimeSensor` instance and compare it against the expected value.

**Pass/Fail Criterion:**
The test passes if the `target_time` for each `TimeSensor` task matches the corresponding time specified in the design document.

**Runnable Test Code (`tests/test_time_sensors.py`):**

```python
import pytest
from datetime import time
from airflow.sensors.time import TimeSensor

# Assuming conftest.py with dagbag and bert_dag fixtures is set up

def test_time_sensor_targets(bert_dag):
    """Verify TimeSensor tasks have the correct target times."""
    expected_time_targets = {
        'wait_until_20_00_for_monthly_jp': time(20, 0, 0),
        'wait_until_04_03_for_housekeeping_jp': time(4, 3, 0),
        'wait_until_01_30_for_daily_export_jp': time(1, 30, 0),
        'wait_until_01_00_for_stammdaten_jp': time(1, 0, 0),
        'wait_until_07_00_for_adm_check_evt': time(7, 0, 0),
        'wait_until_01_00_for_monthly_export_evt': time(1, 0, 0),
    }

    for task_id, expected_time in expected_time_targets.items():
        task = bert_dag.get_task(task_id)
        assert isinstance(task, TimeSensor), f"Task {task_id} is not a TimeSensor."
        # TimeSensor stores target_time as a string in ISO format
        assert task.target_time == expected_time.isoformat(), \
            f"TimeSensor {task_id} has incorrect target_time. Expected {expected_time.isoformat()}, got {task.target_time}."

```

---

## 4. Calendar Short-Circuit Logic Validation

**Purpose:** To verify that the `ShortCircuitOperator` tasks are correctly configured to implement the calendar logic. Since the actual `DW.NEW_CALENDAR` logic is a placeholder, this test focuses on the *structure* and *behavior* of the short-circuiting mechanism, which is a key "Transformation correctness" aspect for this orchestration DAG.

**Setup:**
1.  Same as Test 1.
2.  The `calendar_check_dw_new_calendar_func` in `dw_bert_ablaufsteuerung.py` should be temporarily modified to return `False` for testing the short-circuit path.

**Action:**
1.  **Structural Check (pytest):** Run the `pytest` suite to ensure the tasks are `ShortCircuitOperator` and call the correct Python function.
2.  **Behavioral Check (Manual/Simulated):**
    *   **Scenario A (Calendar True):** Deploy the DAG with `calendar_check_dw_new_calendar_func` returning `True`. Observe that all downstream tasks (e.g., `trigger_dw_bert_monatlich_jp`) execute.
    *   **Scenario B (Calendar False):** Temporarily modify `calendar_check_dw_new_calendar_func` to return `False`. Deploy the DAG and trigger a run. Observe that all downstream tasks dependent on `calendar_check_dw_new_calendar_1` (and `_2`) are skipped.

**Pass/Fail Criterion:**
*   **Structural:** The `pytest` assertions pass, confirming `ShortCircuitOperator` usage and correct `python_callable`.
*   **Behavioral:**
    *   Scenario A: All downstream tasks execute as expected.
    *   Scenario B: All downstream tasks are correctly skipped when the calendar function returns `False`. This confirms the "Else=Skip" behavior for calendar conditions.

**Runnable Test Code (Structural Check - `tests/test_calendar_checks.py`):**

```python
import pytest
from airflow.operators.python import ShortCircuitOperator
from dw_bert_ablaufsteuerung import calendar_check_dw_new_calendar_func # Direct import for callable check

# Assuming conftest.py with dagbag and bert_dag fixtures is set up

def test_calendar_check_operators(bert_dag):
    """Verify calendar check tasks are ShortCircuitOperators and call the correct function."""
    calendar_tasks = [
        'calendar_check_dw_new_calendar_1',
        'calendar_check_dw_new_calendar_2',
    ]

    for task_id in calendar_tasks:
        task = bert_dag.get_task(task_id)
        assert isinstance(task, ShortCircuitOperator), \
            f"Task {task_id} is of type {type(task).__name__}, expected ShortCircuitOperator."
        assert task.python_callable == calendar_check_dw_new_calendar_func, \
            f"Task {task_id} calls an incorrect function. Expected calendar_check_dw_new_calendar_func."

```

---

## 5. Triggered DAGs (`TriggerDagRunOperator`) Validation

**Purpose:** To verify that the `TriggerDagRunOperator` tasks are correctly configured with the right `trigger_dag_id` and, crucially, the correct `wait_for_completion` parameter, matching the UC4 `ActFlg` (Active Flag) behavior. This addresses "Output parity" and "External-system replacements" (where other DAGs are the 'external systems').

**Setup:**
1.  Same as Test 1.

**Action:**
Run the `pytest` suite. The test will inspect the `trigger_dag_id` and `wait_for_completion` attributes of each `TriggerDagRunOperator`.

**Pass/Fail Criterion:**
The test passes if:
*   Each `TriggerDagRunOperator` has the correct `trigger_dag_id` as specified in the design.
*   `wait_for_completion` is `True` for `trigger_dw_bert_monatlich_jp`, `trigger_dw_bert_adm_housekeeping_jp`, `trigger_dw_bert_stammdaten_jp`, and `trigger_dw_dwh_run_apt_export_monatlich_jp_evt`.
*   `wait_for_completion` is `False` for `trigger_dw_dwh_apt_export_taeglich_jp` and `trigger_dw_bert_run_adm_check_jp_evt`.

**Runnable Test Code (`tests/test_triggered_dags.py`):**

```python
import pytest
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Assuming conftest.py with dagbag and bert_dag fixtures is set up

def test_trigger_dag_run_operators(bert_dag):
    """Verify TriggerDagRunOperator tasks have correct trigger_dag_id and wait_for_completion."""
    expected_triggers = {
        'trigger_dw_bert_monatlich_jp': {'dag_id': 'dw_bert_monatlich_jp', 'wait': True},
        'trigger_dw_bert_adm_housekeeping_jp': {'dag_id': 'dw_bert_adm_housekeeping_jp', 'wait': True},
        'trigger_dw_dwh_apt_export_taeglich_jp': {'dag_id': 'dw_dwh_apt_export_taeglich_jp', 'wait': False},
        'trigger_dw_bert_stammdaten_jp': {'dag_id': 'dw_bert_stammdaten_jp', 'wait': True},
        'trigger_dw_bert_run_adm_check_jp_evt': {'dag_id': 'dw_bert_run_adm_check_jp_evt', 'wait': False},
        'trigger_dw_dwh_run_apt_export_monatlich_jp_evt': {'dag_id': 'dw_dwh_run_apt_export_monatlich_jp_evt', 'wait': True},
    }

    for task_id, expected_config in expected_triggers.items():
        task = bert_dag.get_task(task_id)
        assert isinstance(task, TriggerDagRunOperator), \
            f"Task {task_id} is of type {type(task).__name__}, expected TriggerDagRunOperator."
        assert task.trigger_dag_id == expected_config['dag_id'], \
            f"TriggerDagRunOperator {task_id} has incorrect trigger_dag_id. Expected {expected_config['dag_id']}, got {task.trigger_dag_id}."
        assert task.wait_for_completion == expected_config['wait'], \
            f"TriggerDagRunOperator {task_id} has incorrect wait_for_completion. Expected {expected_config['wait']}, got {task.wait_for_completion}."

```

---

## 6. `guard_active_run` Logic Validation

**Purpose:** To verify the presence and basic configuration of the `guard_active_run` task, which is intended to mirror the UC4 "Else=Skip" logic for concurrent runs. This contributes to "Transformation correctness" and "Output parity" regarding concurrent execution.

**Setup:**
1.  Same as Test 1.

**Action:**
1.  **Structural Check (pytest):** Run the `pytest` suite to ensure the task exists, is a `PythonOperator`, and calls the designated `guard_active_run_func`.
2.  **Behavioral Check (Manual/Observational):**
    *   Deploy the DAG to an Airflow environment.
    *   Trigger a DAG run. While it's running, manually trigger a second instance of the same DAG.
    *   Observe the behavior. With `max_active_runs=1` at the DAG level, the second run should remain in a queued state until the first completes. The `guard_active_run` task itself will execute in the first run. If `max_active_runs` was higher, the `guard_active_run_func` would need to contain more complex logic to enforce the "Else=Skip" behavior. For this design, `max_active_runs=1` is the primary mechanism.

**Pass/Fail Criterion:**
*   **Structural:** The `pytest` assertions pass.
*   **Behavioral:** When attempting to run multiple instances concurrently, Airflow's scheduler correctly prevents more than one active run, or the `guard_active_run` task (if enhanced) correctly skips subsequent tasks in concurrent runs.

**Runnable Test Code (Structural Check - `tests/test_guard_active_run.py`):**

```python
import pytest
from airflow.operators.python import PythonOperator
from dw_bert_ablaufsteuerung import guard_active_run_func # Direct import for callable check

# Assuming conftest.py with dagbag and bert_dag fixtures is set up

def test_guard_active_run_task(bert_dag):
    """Verify guard_active_run task is a PythonOperator and calls the correct function."""
    task_id = 'guard_active_run'
    task = bert_dag.get_task(task_id)
    assert task is not None, f"Task {task_id} not found."
    assert isinstance(task, PythonOperator), \
        f"Task {task_id} is of type {type(task).__name__}, expected PythonOperator."
    assert task.python_callable == guard_active_run_func, \
        f"Task {task_id} calls an incorrect function. Expected guard_active_run_func."

```

---

## 7. Default Arguments and Error Handling

**Purpose:** To verify that the DAG's `default_args` are correctly applied, specifically focusing on `retries` and the `on_failure_callback`. This ensures consistent behavior across tasks and proper error notification, contributing to overall "Data-quality / schema assertions" for the DAG's operational aspects.

**Setup:**
1.  Same as Test 1.

**Action:**
1.  **Structural Check (pytest):** Run the `pytest` suite to verify `default_args` and the `on_failure_callback` are set.
2.  **Behavioral Check (Manual/Simulated):**
    *   Deploy the DAG to an Airflow environment.
    *   Temporarily modify a task (e.g., `guard_active_run_func`) to raise an exception, simulating a failure.
    *   Trigger a DAG run.
    *   Observe that the `on_failure_callback` function is invoked (e.g., check logs for its print statement, or verify an email if configured).
    *   Observe that tasks do not retry, as `retries` is set to `0`.

**Pass/Fail Criterion:**
*   **Structural:** The `pytest` assertions pass.
*   **Behavioral:** The `on_failure_callback` is triggered upon task failure, and tasks do not automatically retry.

**Runnable Test Code (Structural Check - `tests/test_error_handling.py`):**

```python
import pytest
from datetime import timedelta
from dw_bert_ablaufsteuerung import on_failure_callback # Direct import for callable check

# Assuming conftest.py with dagbag and bert_dag fixtures is set up

def test_default_args_and_failure_callback(bert_dag):
    """Verify default_args and on_failure_callback are correctly set."""
    assert bert_dag.default_args['owner'] == 'data-platform'
    assert bert_dag.default_args['depends_on_past'] is False
    assert bert_dag.default_args['email_on_failure'] is False
    assert bert_dag.default_args['email_on_retry'] is False
    assert bert_dag.default_args['retries'] == 0
    assert bert_dag.default_args['retry_delay'] == timedelta(minutes=0)
    assert bert_dag.default_args['on_failure_callback'] == on_failure_callback, \
        "on_failure_callback is not set to the expected function."

```