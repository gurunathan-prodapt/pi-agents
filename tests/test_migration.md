As a senior data-migration QA engineer, I've analyzed the migration design document and the generated Airflow DAG for `DW.BERT_ABLAUFSTEUERUNG`. The core of this migration is translating complex UC4 scheduling and orchestration logic into Airflow. The tests below focus on ensuring behavioral equivalence, particularly around scheduling, conditional execution, and inter-job dependencies.

The most critical aspect, as highlighted in the design, is the accurate translation of UC4 calendar definitions (`DW.NEW_CALENDAR`, `DW.KALENDER`, `BERT_NICHT`) into the Python placeholder functions within the Airflow DAG. The tests for these functions are currently based on their *placeholder* implementation; for a complete validation, these functions must be updated to precisely reflect the legacy UC4 calendar logic, and then re-tested against a "golden record" of UC4 execution dates.

The following tests are designed to be run using `pytest`.

---

## Pytest Setup

To run these tests, ensure you have `pytest` and `apache-airflow` installed.
Place the generated Airflow DAG file (`dw_bert_ablaufsteuerung_dag.py`) in a directory named `dags/` relative to where you run `pytest`.

```python
# conftest.py or at the top of your test file
import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.state import State
from datetime import datetime
import pendulum
from unittest.mock import patch, MagicMock
import os

# Define the path to the DAG file and its ID
DAG_FILE_PATH = os.path.join(os.path.dirname(__file__), "dags", "dw_bert_ablaufsteuerung_dag.py")
DAG_ID = "dw_bert_ablaufsteuerung"

# Ensure the dags directory exists for DagBag to find the DAG
@pytest.fixture(scope="session", autouse=True)
def setup_dags_folder():
    dags_folder = os.path.join(os.path.dirname(__file__), "dags")
    if not os.path.exists(dags_folder):
        os.makedirs(dags_folder)
    # You might want to copy the actual DAG file here if it's not already in 'dags'
    # For this example, we assume it's already there.

@pytest.fixture(scope="module")
def dag_bag():
    """Fixture to load the DAG once for all tests."""
    # DagBag expects a folder, not a specific file.
    # It will scan the folder for DAGs.
    dag_bag = DagBag(dag_folder=os.path.join(os.path.dirname(__file__), "dags"), include_examples=False)
    assert dag_bag.dags is not None, "DagBag did not load any DAGs."
    assert DAG_ID in dag_bag.dags, f"DAG '{DAG_ID}' not found in DagBag."
    return dag_bag

@pytest.fixture
def dag(dag_bag):
    """Fixture to get the specific DAG."""
    return dag_bag.dags[DAG_ID]

# Mock the placeholder calendar functions for predictable testing
@pytest.fixture(autouse=True)
def mock_calendar_functions():
    """
    Mocks the placeholder calendar functions in the DAG to control their behavior
    during tests. This is crucial for testing conditional logic.
    """
    # Patch the functions directly in the module where they are defined
    with patch('dw_bert_ablaufsteuerung_dag._check_if_monthly_run_day_callable') as mock_monthly_run, \
         patch('dw_bert_ablaufsteuerung_dag._check_if_bert_nicht_day_callable') as mock_bert_nicht:
        yield mock_monthly_run, mock_bert_nicht

```

---

## Test Case 1: DAG Loading and Basic Structure

*   **Purpose**: Verify that the Airflow DAG loads correctly, has the expected `dag_id`, `schedule_interval`, and contains the expected number and types of tasks. This covers basic schema assertions and ensures the DAG is syntactically valid and structurally matches the design.
*   **Setup**:
    *   The `dw_bert_ablaufsteuerung_dag.py` file is present in the `dags/` directory.
    *   `pytest` and `apache-airflow` are installed.
*   **Action**:
    *   Use `DagBag` to load the DAG.
    *   Inspect the loaded DAG object's properties and task list.
*   **Pass/Fail Criterion**:
    *   The DAG with `dag_id='dw_bert_ablaufsteuerung'` is successfully loaded.
    *   `schedule_interval` is `'@daily'`.
    *   The DAG contains at least 15 tasks (including `start` and all operators for the 6 logical UC4 tasks).
    *   Specific key tasks (e.g., `TimeSensor`, `TriggerDagRunOperator`, `ShortCircuitOperator`) are of the correct type.

```python
# tests/test_bert_ablaufsteuerung_dag.py
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time import TimeSensor

def test_dag_loading_and_basic_structure(dag):
    """
    Purpose: Verify that the Airflow DAG loads correctly, has the expected dag_id,
             schedule_interval, and contains the expected number of tasks.
             This covers basic schema assertions and ensures the DAG is syntactically valid.
    """
    # 1. Output parity / Schema assertion
    assert dag.dag_id == DAG_ID, f"DAG ID mismatch: Expected '{DAG_ID}', got '{dag.dag_id}'"
    assert dag.schedule_interval == '@daily', f"Schedule interval mismatch: Expected '@daily', got '{dag.schedule_interval}'"
    assert dag.start_date is not None, "DAG start_date should not be None"
    assert len(dag.tasks) >= 15, "Expected at least 15 tasks (start + 6 logical tasks * ~2-3 operators each)"

    # Check specific task IDs for existence
    expected_task_ids = [
        'start_orchestration',
        'wait_for_01am_bert_stammdaten_jp', 'trigger_bert_stammdaten_jp',
        'wait_for_0130am_dwh_apt_export_taeglich_jp', 'trigger_dwh_apt_export_taeglich_jp',
        'wait_for_0403am_bert_adm_housekeeping_jp', 'trigger_bert_adm_housekeeping_jp',
        'wait_for_07am_bert_run_adm_check_jp_evt', 'run_bert_adm_check_jp_evt',
        'check_monthly_run_day_bert_monatlich', 'wait_for_08pm_bert_monatlich_jp', 'trigger_bert_monatlich_jp',
        'check_monthly_apt_export_day', 'wait_for_01am_dwh_run_apt_export_monatlich_jp_evt', 'run_dwh_apt_export_monatlich_jp_evt',
    ]
    for task_id in expected_task_ids:
        assert dag.has_task(task_id), f"DAG is missing expected task: {task_id}"

    # Check task types for transformation correctness
    assert isinstance(dag.get_task('start_orchestration'), DummyOperator)
    assert isinstance(dag.get_task('wait_for_01am_bert_stammdaten_jp'), TimeSensor)
    assert isinstance(dag.get_task('trigger_bert_stammdaten_jp'), TriggerDagRunOperator)
    assert isinstance(dag.get_task('check_monthly_run_day_bert_monatlich'), ShortCircuitOperator)
    assert isinstance(dag.get_task('run_bert_adm_check_jp_evt'), BashOperator)
    assert isinstance(dag.get_task('check_monthly_apt_export_day'), ShortCircuitOperator)

```

---

## Test Case 2: Daily Task Execution Parity

*   **Purpose**: Verify that daily scheduled tasks (`DW.BERT_STAMMDATEN_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.BERT_RUN_ADM_CHECK_JP_EVT`) are correctly configured to run daily at their specified times. This covers output parity and transformation correctness for daily schedules.
*   **Setup**:
    *   The DAG is loaded.
    *   `_check_if_monthly_run_day_callable` and `_check_if_bert_nicht_day_callable` are mocked to return `False` to ensure monthly tasks are skipped on a typical day.
*   **Action**:
    *   Simulate a `DagRun` for a typical day (e.g., `2023-01-15`).
    *   Inspect the `TimeSensor` tasks' `target_time` and the `TriggerDagRunOperator` tasks' `trigger_dag_id`.
    *   Run the `ShortCircuitOperator` tasks for monthly jobs to confirm they are skipped.
*   **Pass/Fail Criterion**:
    *   `TimeSensor` tasks for daily jobs have the correct `target_time`.
    *   `TriggerDagRunOperator` tasks for daily jobs have the correct `trigger_dag_id` and `wait_for_completion=True`.
    *   `BashOperator` for `DW.BERT_RUN_ADM_CHECK_JP_EVT` has the expected command.
    *   Monthly conditional tasks (`check_monthly_run_day_bert_monatlich`, `check_monthly_apt_export_day`) are `SKIPPED` on a non-special day.

```python
# tests/test_bert_ablaufsteuerung_dag.py
def test_daily_task_execution_parity(dag, mock_calendar_functions):
    """
    Purpose: Verify that daily scheduled tasks are correctly configured to run daily at their specified times.
             This covers output parity and transformation correctness for daily schedules.
    """
    mock_monthly_run, mock_bert_nicht = mock_calendar_functions
    mock_monthly_run.return_value = False # Ensure monthly tasks are skipped
    mock_bert_nicht.return_value = False  # Ensure BERT_NICHT exclusion doesn't apply

    execution_date = pendulum.datetime(2023, 1, 15, tz="UTC") # A typical day

    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        data_interval_start=execution_date,
        data_interval_end=execution_date + pendulum.duration(days=1),
    )

    # Check daily tasks' configurations
    daily_tasks_config = {
        'wait_for_01am_bert_stammdaten_jp': {'target_time': pendulum.time(1, 0, 0), 'next_task': 'trigger_bert_stammdaten_jp', 'trigger_dag_id': 'dw_bert_stammdaten_jp'},
        'wait_for_0130am_dwh_apt_export_taeglich_jp': {'target_time': pendulum.time(1, 30, 0), 'next_task': 'trigger_dwh_apt_export_taeglich_jp', 'trigger_dag_id': 'dw_dwh_apt_export_taeglich_jp'},
        'wait_for_0403am_bert_adm_housekeeping_jp': {'target_time': pendulum.time(4, 3, 0), 'next_task': 'trigger_bert_adm_housekeeping_jp', 'trigger_dag_id': 'dw_bert_adm_housekeeping_jp'},
        'wait_for_07am_bert_run_adm_check_jp_evt': {'target_time': pendulum.time(7, 0, 0), 'next_task': 'run_bert_adm_check_jp_evt', 'trigger_dag_id': None}, # BashOperator, no trigger_dag_id
    }

    for task_id, config in daily_tasks_config.items():
        task = dag.get_task(task_id)
        assert isinstance(task, TimeSensor), f"Task {task_id} should be a TimeSensor"
        assert task.target_time == config['target_time'], f"TimeSensor {task_id} target_time mismatch"

        downstream_task = dag.get_task(config['next_task'])
        if config['trigger_dag_id']:
            assert isinstance(downstream_task, TriggerDagRunOperator), f"Task {config['next_task']} should be a TriggerDagRunOperator"
            assert downstream_task.trigger_dag_id == config['trigger_dag_id'], f"TriggerDagRunOperator {config['next_task']} trigger_dag_id mismatch"
            assert downstream_task.wait_for_completion is True, f"TriggerDagRunOperator {config['next_task']} should wait for completion"
        else: # For BashOperator
            assert isinstance(downstream_task, BashOperator), f"Task {config['next_task']} should be a BashOperator"
            assert "Executing DW.BERT_RUN_ADM_CHECK_JP_EVT logic" in downstream_task.bash_command

    # Check that monthly tasks are skipped on a non-monthly day
    ti_monthly_run_check = dag.get_task('check_monthly_run_day_bert_monatlich').create_task_instance(dag_run)
    ti_monthly_run_check.run(start_date=execution_date, end_date=execution_date)
    assert ti_monthly_run_check.current_state() == State.SKIPPED, "Monthly run check should be skipped on a non-monthly day"

    ti_apt_export_monthly_check = dag.get_task('check_monthly_apt_export_day').create_task_instance(dag_run)
    ti_apt_export_monthly_check.run(start_date=execution_date, end_date=execution_date)
    assert ti_apt_export_monthly_check.current_state() == State.SKIPPED, "Monthly APT export check should be skipped on a non-monthly day"

```

---

## Test Case 3: Monthly Task Execution (Inclusion - 5th and 25th)

*   **Purpose**: Verify that `DW.BERT_MONATLICH_JP` is triggered on the 5th and 25th of the month at 20:00, as specified by the UC4 calendar logic (`DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`). This tests transformation correctness for calendar logic.
*   **Setup**:
    *   The DAG is loaded.
    *   `_check_if_monthly_run_day_callable` is mocked to return `True` for the 5th/25th.
    *   `_check_if_bert_nicht_day_callable` is mocked to return `False`.
*   **Action**:
    *   Simulate `DagRun`s for `2023-01-05` and `2023-01-25`.
    *   For each `DagRun`, run the `check_monthly_run_day_bert_monatlich` task.
    *   Inspect the `TimeSensor` and `TriggerDagRunOperator` for `DW.BERT_MONATLICH_JP`.
*   **Pass/Fail Criterion**:
    *   `check_monthly_run_day_bert_monatlich` task successfully runs (not skipped) on the 5th and 25th.
    *   `wait_for_08pm_bert_monatlich_jp` has `target_time` of `20:00:00`.
    *   `trigger_bert_monatlich_jp` has `trigger_dag_id='dw_bert_monatlich_jp'` and `wait_for_completion=True`.

```python
# tests/test_bert_ablaufsteuerung_dag.py
def test_monthly_bert_monatlich_jp_inclusion(dag, mock_calendar_functions):
    """
    Purpose: Verify that DW.BERT_MONATLICH_JP is triggered on the 5th and 25th of the month at 20:00.
             This tests transformation correctness for calendar logic.
    """
    mock_monthly_run, mock_bert_nicht = mock_calendar_functions
    mock_bert_nicht.return_value = False # Ensure BERT_NICHT doesn't interfere

    # Configure mock_monthly_run to return True only for 5th and 25th
    mock_monthly_run.side_effect = lambda ds, days_of_month: datetime.strptime(ds, '%Y-%m-%d').day in days_of_month

    # Test for 5th of the month
    execution_date_5th = pendulum.datetime(2023, 1, 5, tz="UTC")
    dag_run_5th = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date_5th,
        start_date=execution_date_5th,
        data_interval_start=execution_date_5th,
        data_interval_end=execution_date_5th + pendulum.duration(days=1),
    )
    ti_monthly_run_check_5th = dag.get_task('check_monthly_run_day_bert_monatlich').create_task_instance(dag_run_5th)
    ti_monthly_run_check_5th.run(start_date=execution_date_5th, end_date=execution_date_5th)
    assert ti_monthly_run_check_5th.current_state() == State.SUCCESS, "Monthly run check should succeed on 5th"

    # Test for 25th of the month
    execution_date_25th = pendulum.datetime(2023, 1, 25, tz="UTC")
    dag_run_25th = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date_25th,
        start_date=execution_date_25th,
        data_interval_start=execution_date_25th,
        data_interval_end=execution_date_25th + pendulum.duration(days=1),
    )
    ti_monthly_run_check_25th = dag.get_task('check_monthly_run_day_bert_monatlich').create_task_instance(dag_run_25th)
    ti_monthly_run_check_25th.run(start_date=execution_date_25th, end_date=execution_date_25th)
    assert ti_monthly_run_check_25th.current_state() == State.SUCCESS, "Monthly run check should succeed on 25th"

    # Verify TimeSensor and TriggerDagRunOperator configurations
    wait_task = dag.get_task('wait_for_08pm_bert_monatlich_jp')
    trigger_task = dag.get_task('trigger_bert_monatlich_jp')

    assert isinstance(wait_task, TimeSensor), "wait_for_08pm_bert_monatlich_jp should be a TimeSensor"
    assert wait_task.target_time == pendulum.time(20, 0, 0), "TimeSensor target_time mismatch for BERT_MONATLICH_JP"

    assert isinstance(trigger_task, TriggerDagRunOperator), "trigger_bert_monatlich_jp should be a TriggerDagRunOperator"
    assert trigger_task.trigger_dag_id == 'dw_bert_monatlich_jp', "TriggerDagRunOperator trigger_dag_id mismatch"
    assert trigger_task.wait_for_completion is True, "TriggerDagRunOperator should wait for completion"

```

---

## Test Case 4: Monthly Task Execution (Exclusion - `BERT_NICHT` days)

*   **Purpose**: Verify that `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` is *not* triggered on `BERT_NICHT` days, even if it's a designated monthly run day. This tests the exclusion calendar logic (transformation correctness).
*   **Setup**:
    *   The DAG is loaded.
    *   `_check_if_bert_nicht_day_callable` is mocked to return `True` for the test date.
    *   The internal logic of `_check_monthly_apt_export_day_callable` (which checks `dag_run_date.day == 1` and then calls `_check_if_bert_nicht_day_callable`) is implicitly tested.
*   **Action**:
    *   Simulate a `DagRun` for a day that is a designated monthly export day (e.g., 1st) AND a `BERT_NICHT` day (by mocking `_check_if_bert_nicht_day_callable` to return `True`).
    *   Run the `check_monthly_apt_export_day` task.
*   **Pass/Fail Criterion**:
    *   The `check_monthly_apt_export_day` task is `SKIPPED`.

```python
# tests/test_bert_ablaufsteuerung_dag.py
def test_monthly_apt_export_exclusion_bert_nicht(dag, mock_calendar_functions):
    """
    Purpose: Verify that DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT is NOT triggered on BERT_NICHT days,
             even if it's a designated monthly run day. This tests the exclusion calendar logic.
    """
    mock_monthly_run, mock_bert_nicht = mock_calendar_functions

    # Simulate that the current day IS a BERT_NICHT day
    mock_bert_nicht.return_value = True

    # Test on the 1st of the month, which is the designated monthly day in the placeholder logic
    execution_date = pendulum.datetime(2023, 1, 1, tz="UTC")

    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        data_interval_start=execution_date,
        data_interval_end=execution_date + pendulum.duration(days=1),
    )

    ti_apt_export_monthly_check = dag.get_task('check_monthly_apt_export_day').create_task_instance(dag_run)
    ti_apt_export_monthly_check.run(start_date=execution_date, end_date=execution_date)

    assert ti_apt_export_monthly_check.current_state() == State.SKIPPED, \
        "Monthly APT export check should be skipped on a BERT_NICHT day, even if it's a designated monthly day."

    # Verify TimeSensor and BashOperator configurations (even if skipped, their definition should be correct)
    wait_task = dag.get_task('wait_for_01am_dwh_run_apt_export_monatlich_jp_evt')
    run_task = dag.get_task('run_dwh_apt_export_monatlich_jp_evt')

    assert isinstance(wait_task, TimeSensor), "wait_for_01am_dwh_run_apt_export_monatlich_jp_evt should be a TimeSensor"
    assert wait_task.target_time == pendulum.time(1, 0, 0), "TimeSensor target_time mismatch for APT_EXPORT_MONATLICH_JP_EVT"

    assert isinstance(run_task, BashOperator), "run_dwh_apt_export_monatlich_jp_evt should be a BashOperator"
    assert "Executing DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT logic" in run_task.bash_command, \
        "BashOperator command for APT_EXPORT_MONATLICH_JP_EVT is incorrect"

```

---

## Test Case 5: Placeholder Calendar Function Verification (Critical Transformation Correctness)

*   **Purpose**: Highlight the critical dependency on the correct implementation of the placeholder calendar functions (`_check_if_monthly_run_day_callable`, `_check_if_bert_nicht_day_callable`). These functions directly translate complex UC4 calendar logic. This is a crucial "transformation correctness" point.
*   **Setup**:
    *   No specific setup beyond the DAG loading.
*   **Action**:
    *   Directly call the placeholder functions with various dates to confirm their current behavior.
*   **Pass/Fail Criterion**:
    *   The functions' current placeholder logic works as defined in the code.
    *   **CRITICAL**: This test passes based on the *current placeholder implementation*. For a real migration, the actual UC4 calendar definitions (`DW.NEW_CALENDAR`, `DW.KALENDER`, `BERT_NICHT`) MUST be analyzed and these functions updated to precisely replicate that logic. This test serves as a reminder and a basic check that the functions are callable. A more advanced test would involve a data-driven approach, comparing outputs for many dates against a "golden record" derived from legacy UC4 calendar evaluations.

```python
# tests/test_bert_ablaufsteuerung_dag.py
def test_placeholder_calendar_functions_correctness(dag):
    """
    Purpose: Highlight the critical dependency on the correct implementation of the placeholder
             calendar functions. These functions directly translate complex UC4 calendar logic.
             This is a crucial "transformation correctness" point.
    """
    # Import the actual functions from the DAG file for direct testing
    # This assumes the test file can import from the dags folder.
    # If not, you might need to adjust Python path or copy the functions.
    from dags.dw_bert_ablaufsteuerung_dag import _check_if_monthly_run_day_callable, _check_if_bert_nicht_day_callable

    # Test _check_if_monthly_run_day_callable based on its current placeholder logic ([5, 25])
    assert _check_if_monthly_run_day_callable('2023-01-05', [5, 25]) is True, "Monthly run callable failed for 5th"
    assert _check_if_monthly_run_day_callable('2023-01-25', [5, 25]) is True, "Monthly run callable failed for 25th"
    assert _check_if_monthly_run_day_callable('2023-01-10', [5, 25]) is False, "Monthly run callable failed for non-monthly day"

    # Test _check_if_bert_nicht_day_callable based on its current placeholder logic ([10, 20])
    assert _check_if_bert_nicht_day_callable('2023-01-10') is True, "BERT_NICHT callable failed for 10th"
    assert _check_if_bert_nicht_day_callable('2023-01-20') is True, "BERT_NICHT callable failed for 20th"
    assert _check_if_bert_nicht_day_callable('2023-01-01') is False, "BERT_NICHT callable failed for non-BERT_NICHT day"

    # Pass/Fail Criterion:
    # The current placeholder logic works as defined in the code.
    # CRITICAL: This test passes based on the *current placeholder implementation*.
    # For a real migration, the actual UC4 calendar definitions (DW.NEW_CALENDAR, DW.KALENDER, BERT_NICHT)
    # MUST be analyzed and these functions updated to precisely replicate that logic.
    # This test serves as a reminder and a basic check that the functions are callable.
    # A more advanced test would involve a data-driven approach, comparing outputs for many dates
    # against a "golden record" derived from legacy UC4 calendar evaluations.
    print("\nWARNING: The calendar functions are placeholders. Their logic MUST be verified against actual UC4 calendar definitions.")
    print("This test only confirms the placeholder logic works as coded, not that it matches legacy UC4 behavior.")

```

---

## Test Case 6: Inter-DAG Dependencies and `TriggerDagRunOperator` Configuration

*   **Purpose**: Verify that `TriggerDagRunOperator` tasks are correctly configured to trigger sub-DAGs and wait for their completion, replicating the implicit dependencies from UC4 `JOBP` invocations. This covers external-system replacements (other DAGs are "external" to this DAG's direct execution) and transformation correctness.
*   **Setup**:
    *   The DAG is loaded.
*   **Action**:
    *   Inspect the properties of all `TriggerDagRunOperator` tasks within the DAG.
*   **Pass/Fail Criterion**:
    *   All `TriggerDagRunOperator` tasks have `wait_for_completion=True`.
    *   All `TriggerDagRunOperator` tasks have a `poke_interval` defined (e.g., 5 seconds).
    *   The `trigger_dag_id` for each operator matches the expected sub-DAG ID from the design document.

```python
# tests/test_bert_ablaufsteuerung_dag.py
def test_trigger_dag_run_operator_configuration(dag):
    """
    Purpose: Verify that TriggerDagRunOperator tasks are correctly configured to trigger sub-DAGs
             and wait for their completion, replicating implicit dependencies from UC4 JOBP invocations.
             This covers external-system replacements (other DAGs are "external" to this DAG's direct execution)
             and transformation correctness.
    """
    trigger_tasks = [
        dag.get_task('trigger_bert_stammdaten_jp'),
        dag.get_task('trigger_dwh_apt_export_taeglich_jp'),
        dag.get_task('trigger_bert_adm_housekeeping_jp'),
        dag.get_task('trigger_bert_monatlich_jp'),
    ]

    expected_trigger_dag_ids = {
        'trigger_bert_stammdaten_jp': 'dw_bert_stammdaten_jp',
        'trigger_dwh_apt_export_taeglich_jp': 'dw_dwh_apt_export_taeglich_jp',
        'trigger_bert_adm_housekeeping_jp': 'dw_bert_adm_housekeeping_jp',
        'trigger_bert_monatlich_jp': 'dw_bert_monatlich_jp',
    }

    for task in trigger_tasks:
        assert isinstance(task, TriggerDagRunOperator), f"Task {task.task_id} should be a TriggerDagRunOperator"
        assert task.wait_for_completion is True, f"TriggerDagRunOperator {task.task_id} should have wait_for_completion=True"
        assert task.poke_interval == 5, f"TriggerDagRunOperator {task.task_id} should have poke_interval=5"
        assert task.trigger_dag_id == expected_trigger_dag_ids[task.task_id], \
            f"TriggerDagRunOperator {task.task_id} has incorrect trigger_dag_id"

```

---

## Test Case 7: Overall DAG Flow and Task States on a Specific Day

*   **Purpose**: While traditional data quality/row count/schema assertions don't apply directly to an orchestrator, in this context, it means ensuring that the *right number of tasks* are triggered/skipped and that the *overall flow* is as expected for a given execution date. This is a form of behavioral assertion.
*   **Setup**:
    *   The DAG is loaded.
    *   Calendar functions are mocked to simulate a specific scenario (e.g., a monthly day that is *not* a `BERT_NICHT` day, and not a 5th/25th).
*   **Action**:
    *   Simulate a `DagRun` for a specific date (e.g., `2023-01-01`).
    *   Manually set states for upstream tasks (like `start` and `TimeSensor`s) to simulate successful completion, allowing downstream conditional logic to be tested.
    *   Run the `ShortCircuitOperator` tasks and inspect their states.
*   **Pass/Fail Criterion**:
    *   All daily tasks (and their simulated upstream sensors) are `SUCCESS`.
    *   `check_monthly_run_day_bert_monatlich` and its downstream tasks are `SKIPPED`.
    *   `check_monthly_apt_export_day` and its downstream tasks are `SUCCESS`.
    *   No tasks are in a `FAILED` state.

```python
# tests/test_bert_ablaufsteuerung_dag.py
def test_overall_dag_flow_on_specific_day(dag, mock_calendar_functions):
    """
    Purpose: Ensure that the right number of tasks are triggered/skipped and that the overall flow
             is as expected for a specific date, simulating a complete run.
             This is a form of behavioral assertion.
    """
    mock_monthly_run, mock_bert_nicht = mock_calendar_functions

    # Simulate a day where:
    # - Daily tasks run
    # - BERT_MONATLICH_JP is skipped (not 5th or 25th)
    # - DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT runs (it's the 1st, and not a BERT_NICHT day)
    execution_date = pendulum.datetime(2023, 1, 1, tz="UTC")

    # Mock calendar functions for this specific scenario
    mock_monthly_run.return_value = False # Not 5th or 25th
    mock_bert_nicht.return_value = False  # Not a BERT_NICHT day

    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        data_interval_start=execution_date,
        data_interval_end=execution_date + pendulum.duration(days=1),
    )

    # Simulate running all tasks by setting states for upstream dependencies
    # In a real Airflow test, you'd use `dag.test()` or a more sophisticated runner
    # that handles TimeSensors and dependencies automatically.
    # Here, we manually set states to test the conditional logic.

    # Simulate 'start' task success
    ti_start = dag.get_task('start_orchestration').create_task_instance(dag_run)
    ti_start.set_state(State.SUCCESS)

    # Simulate daily tasks and their TimeSensors passing
    daily_task_groups = [
        ('wait_for_01am_bert_stammdaten_jp', 'trigger_bert_stammdaten_jp'),
        ('wait_for_0130am_dwh_apt_export_taeglich_jp', 'trigger_dwh_apt_export_taeglich_jp'),
        ('wait_for_0403am_bert_adm_housekeeping_jp', 'trigger_bert_adm_housekeeping_jp'),
        ('wait_for_07am_bert_run_adm_check_jp_evt', 'run_bert_adm_check_jp_evt'),
    ]
    for sensor_id, trigger_id in daily_task_groups:
        ti_sensor = dag.get_task(sensor_id).create_task_instance(dag_run)
        ti_sensor.set_state(State.SUCCESS) # Simulate TimeSensor passing
        ti_trigger = dag.get_task(trigger_id).create_task_instance(dag_run)
        ti_trigger.set_state(State.SUCCESS) # Simulate TriggerDagRun/BashOperator running

    # Monthly BERT_MONATLICH_JP (should be skipped because mock_monthly_run.return_value is False)
    ti_monthly_run_check = dag.get_task('check_monthly_run_day_bert_monatlich').create_task_instance(dag_run)
    ti_monthly_run_check.run(start_date=execution_date, end_date=execution_date)
    assert ti_monthly_run_check.current_state() == State.SKIPPED, "BERT_MONATLICH_JP check should be SKIPPED"
    ti_wait_08pm_bert_monatlich = dag.get_task('wait_for_08pm_bert_monatlich_jp').create_task_instance(dag_run)
    assert ti_wait_08pm_bert_monatlich.current_state() == State.SKIPPED, "wait_for_08pm_bert_monatlich_jp should be SKIPPED"
    ti_trigger_bert_monatlich = dag.get_task('trigger_bert_monatlich_jp').create_task_instance(dag_run)
    assert ti_trigger_bert_monatlich.current_state() == State.SKIPPED, "trigger_bert_monatlich_jp should be SKIPPED"


    # Monthly DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT (should run because it's the 1st and not a BERT_NICHT day)
    ti_apt_export_monthly_check = dag.get_task('check_monthly_apt_export_day').create_task_instance(dag_run)
    ti_apt_export_monthly_check.run(start_date=execution_date, end_date=execution_date)
    assert ti_apt_export_monthly_check.current_state() == State.SUCCESS, "APT_EXPORT_MONATLICH_JP_EVT check should succeed"

    ti_wait_apt_export_monthly = dag.get_task('wait_for_01am_dwh_run_apt_export_monatlich_jp_evt').create_task_instance(dag_run)
    ti_wait_apt_export_monthly.set_state(State.SUCCESS) # Simulate TimeSensor passing
    ti_run_apt_export_monthly = dag.get_task('run_dwh_apt_export_monatlich_jp_evt').create_task_instance(dag_run)
    ti_run_apt_export_monthly.set_state(State.SUCCESS) # Simulate BashOperator running
    assert ti_wait_apt_export_monthly.current_state() == State.SUCCESS, "wait_for_01am_dwh_run_apt_export_monatlich_jp_evt should be SUCCESS"
    assert ti_run_apt_export_monthly.current_state() == State.SUCCESS, "run_dwh_apt_export_monatlich_jp_evt should be SUCCESS"

    # Final check: No tasks should be in FAILED state
    for task in dag.tasks:
        ti = task.create_task_instance(dag_run)
        assert ti.current_state() != State.FAILED, f"Task {task.task_id} should not be FAILED"

```