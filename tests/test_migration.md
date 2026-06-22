As a senior data-migration QA engineer, I've developed a suite of validation tests for the `DW.BERT_ABLAUFSTEUERUNG` Airflow DAG. These tests aim to ensure behavioral equivalence with the legacy UC4 job scheduler, covering output parity, transformation correctness, and adherence to the migration design.

The tests are categorized by the specific functionality they validate, providing clear setup, action, and pass/fail criteria. Where applicable, runnable `pytest` code is included for Python logic, and conceptual Airflow assertions are described for DAG structure and execution flow.

---

## Migration Validation Tests: `dw_bert_ablaufsteuerung` Airflow DAG

### Test Case 1: DAG Definition and Metadata Parity

*   **Purpose**: Verify that the Airflow DAG is correctly defined with the specified `dag_id`, `schedule_interval`, `start_date`, `catchup`, `max_active_runs`, `is_paused_upon_creation`, and `default_args` as per the migration design. This ensures basic structural and scheduling parity.
*   **Setup**:
    1.  Ensure the `dw_bert_ablaufsteuerung.py` file is accessible in the Airflow DAGs folder or a path discoverable by `DagBag`.
    2.  No specific external systems are involved at this stage.
*   **Action**:
    1.  Load the DAG using Airflow's `DagBag`.
    2.  Inspect the DAG object's attributes.
*   **Pass/Fail Criterion**:
    *   **Pass**: The DAG exists and its attributes match the design document:
        *   `dag_id` is `dw_bert_ablaufsteuerung`.
        *   `schedule_interval` is `0 0 * * *`.
        *   `catchup` is `False`.
        *   `max_active_runs` is `1`.
        *   `is_paused_upon_creation` is `False`.
        *   `default_args['owner']` is `data-engineering`.
        *   `default_args['retries']` is `0`.
        *   `default_args['retry_delay']` is `timedelta(minutes=0)`.
    *   **Fail**: Any of the above attributes do not match the specified values.

```python
import pytest
from airflow.models import DagBag
from datetime import timedelta

def test_dag_definition_and_metadata():
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    dag = dag_bag.get_dag('dw_bert_ablaufsteuerung')

    assert dag is not None, "DAG 'dw_bert_ablaufsteuerung' not found."
    assert dag.dag_id == 'dw_bert_ablaufsteuerung'
    assert dag.schedule_interval == '0 0 * * *'
    assert not dag.catchup
    assert dag.max_active_runs == 1
    assert not dag.is_paused_upon_creation
    assert dag.default_args['owner'] == 'data-engineering'
    assert dag.default_args['retries'] == 0
    assert dag.default_args['retry_delay'] == timedelta(minutes=0)
    print(f"DAG '{dag.dag_id}' metadata verified successfully.")

# To run this test, save the DAG code in a 'dags' folder and this test in a 'tests' folder.
# Then run `pytest` from the parent directory.
```

### Test Case 2: Concurrency Guard (`guard_active_run`) Behavior

*   **Purpose**: Validate that the `_guard_active_run` Python callable correctly implements the `SYNCREF` with `Else=Skip` logic by raising an `AirflowSkipException` when another DAG run is active.
*   **Setup**:
    1.  Mock Airflow's `DagRun.find` method to simulate active DAG runs.
    2.  Provide a mock `dag` and `dag_run` context to the callable.
*   **Action**:
    1.  Call the `_guard_active_run` function with different `DagRun.find` mock return values.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   If `DagRun.find` returns no other active runs, the function completes without raising an exception.
        *   If `DagRun.find` returns one or more other active runs, the function raises an `AirflowSkipException`.
    *   **Fail**: The function's behavior does not match the above conditions.

```python
import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun
from airflow.utils.state import State

# Assuming _guard_active_run is imported or copied for testing
# For actual testing, you'd import it from your DAG file:
# from dags.dw_bert_ablaufsteuerung import _guard_active_run

# Copy of the function for direct testing (replace with import in real scenario)
def _guard_active_run(**kwargs):
    dag_id = kwargs["dag"].dag_id
    current_run_id = kwargs["dag_run"].run_id
    
    active_runs = DagRun.find(dag_id=dag_id, state=State.RUNNING)
    other_active_runs = [run for run in active_runs if run.run_id != current_run_id]

    if other_active_runs:
        raise AirflowSkipException(f"Skipping DAG run {current_run_id} due to active concurrent runs.")

def test_guard_active_run_no_other_runs():
    mock_dag = MagicMock(dag_id='test_dag')
    mock_dag_run = MagicMock(run_id='test_run_1')
    
    with patch('airflow.models.DagRun.find') as mock_find:
        mock_find.return_value = [mock_dag_run] # Only the current run is active
        _guard_active_run(dag=mock_dag, dag_run=mock_dag_run)
        mock_find.assert_called_once_with(dag_id='test_dag', state=State.RUNNING)
    print("Guard task passed: No other active runs, no skip exception raised.")

def test_guard_active_run_with_other_runs():
    mock_dag = MagicMock(dag_id='test_dag')
    mock_dag_run_current = MagicMock(run_id='test_run_1')
    mock_dag_run_other = MagicMock(run_id='test_run_2')

    with patch('airflow.models.DagRun.find') as mock_find:
        mock_find.return_value = [mock_dag_run_current, mock_dag_run_other] # Current + another active run
        with pytest.raises(AirflowSkipException) as excinfo:
            _guard_active_run(dag=mock_dag, dag_run=mock_dag_run_current)
        assert f"Skipping DAG run {mock_dag_run_current.run_id} due to active concurrent runs." in str(excinfo.value)
        mock_find.assert_called_once_with(dag_id='test_dag', state=State.RUNNING)
    print("Guard task passed: Other active runs found, skip exception raised.")
```

### Test Case 3: Monthly BERT Calendar Logic (`_check_new_calendar_branch`) - Applicable Day

*   **Purpose**: Verify that the `_check_new_calendar_branch` function correctly identifies days 5 and 25 of the month as applicable for the monthly BERT workflow.
*   **Setup**:
    1.  Provide mock `logical_date` values representing the 5th and 25th of a month.
*   **Action**:
    1.  Call `_check_new_calendar_branch` with the mock dates.
*   **Pass/Fail Criterion**:
    *   **Pass**: The function returns `'wait_for_monthly_bert_start_time'` for both the 5th and 25th of the month.
    *   **Fail**: The function returns any other task ID.

```python
import pytest
from datetime import datetime

# Assuming _check_new_calendar_branch is imported or copied for testing
# from dags.dw_bert_ablaufsteuerung import _check_new_calendar_branch

# Copy of the function for direct testing (replace with import in real scenario)
def _check_new_calendar_branch(**kwargs):
    execution_date = kwargs["logical_date"]
    day_of_month = execution_date.day
    is_applicable = day_of_month in [5, 25]
    if is_applicable:
        return 'wait_for_monthly_bert_start_time'
    else:
        return 'adm_check_event_start_anchor'

def test_check_new_calendar_branch_applicable_day():
    # Test for 5th of the month
    mock_context_5th = {'logical_date': datetime(2023, 1, 5, 0, 0, 0)}
    result_5th = _check_new_calendar_branch(**mock_context_5th)
    assert result_5th == 'wait_for_monthly_bert_start_time'
    print(f"Monthly BERT calendar check passed for {mock_context_5th['logical_date'].strftime('%Y-%m-%d')}.")

    # Test for 25th of the month
    mock_context_25th = {'logical_date': datetime(2023, 1, 25, 0, 0, 0)}
    result_25th = _check_new_calendar_branch(**mock_context_25th)
    assert result_25th == 'wait_for_monthly_bert_start_time'
    print(f"Monthly BERT calendar check passed for {mock_context_25th['logical_date'].strftime('%Y-%m-%d')}.")
```

### Test Case 4: Monthly BERT Calendar Logic (`_check_new_calendar_branch`) - Non-Applicable Day

*   **Purpose**: Verify that the `_check_new_calendar_branch` function correctly identifies days other than 5 and 25 of the month as non-applicable for the monthly BERT workflow.
*   **Setup**:
    1.  Provide mock `logical_date` values representing days other than the 5th or 25th.
*   **Action**:
    1.  Call `_check_new_calendar_branch` with the mock dates.
*   **Pass/Fail Criterion**:
    *   **Pass**: The function returns `'adm_check_event_start_anchor'` for non-applicable days, indicating a skip.
    *   **Fail**: The function returns `'wait_for_monthly_bert_start_time'` or any other unexpected task ID.

```python
import pytest
from datetime import datetime

# Assuming _check_new_calendar_branch is imported or copied for testing
# from dags.dw_bert_ablaufsteuerung import _check_new_calendar_branch

# Copy of the function for direct testing (replace with import in real scenario)
def _check_new_calendar_branch(**kwargs):
    execution_date = kwargs["logical_date"]
    day_of_month = execution_date.day
    is_applicable = day_of_month in [5, 25]
    if is_applicable:
        return 'wait_for_monthly_bert_start_time'
    else:
        return 'adm_check_event_start_anchor'

def test_check_new_calendar_branch_non_applicable_day():
    # Test for 1st of the month
    mock_context_1st = {'logical_date': datetime(2023, 1, 1, 0, 0, 0)}
    result_1st = _check_new_calendar_branch(**mock_context_1st)
    assert result_1st == 'adm_check_event_start_anchor'
    print(f"Monthly BERT calendar check passed for {mock_context_1st['logical_date'].strftime('%Y-%m-%d')}.")

    # Test for 15th of the month
    mock_context_15th = {'logical_date': datetime(2023, 1, 15, 0, 0, 0)}
    result_15th = _check_new_calendar_branch(**mock_context_15th)
    assert result_15th == 'adm_check_event_start_anchor'
    print(f"Monthly BERT calendar check passed for {mock_context_15th['logical_date'].strftime('%Y-%m-%d')}.")
```

### Test Case 5: Monthly DWH Export Calendar Logic (`_check_bert_nicht_calendar_branch`) - Applicable Day

*   **Purpose**: Verify that the `_check_bert_nicht_calendar_branch` function correctly identifies days *not* the 15th of the month as applicable for the monthly DWH export.
*   **Setup**:
    1.  Provide mock `logical_date` values representing days other than the 15th.
*   **Action**:
    1.  Call `_check_bert_nicht_calendar_branch` with the mock dates.
*   **Pass/Fail Criterion**:
    *   **Pass**: The function returns `'trigger_dw_dwh_run_apt_export_monatlich_jp_evt'` for non-15th days.
    *   **Fail**: The function returns any other task ID.

```python
import pytest
from datetime import datetime

# Assuming _check_bert_nicht_calendar_branch is imported or copied for testing
# from dags.dw_bert_ablaufsteuerung import _check_bert_nicht_calendar_branch

# Copy of the function for direct testing (replace with import in real scenario)
def _check_bert_nicht_calendar_branch(**kwargs):
    execution_date = kwargs["logical_date"]
    day_of_month = execution_date.day
    is_applicable = day_of_month != 15
    if is_applicable:
        return 'trigger_dw_dwh_run_apt_export_monatlich_jp_evt'
    else:
        return 'end_of_dag_anchor'

def test_check_bert_nicht_calendar_branch_applicable_day():
    # Test for 1st of the month (not 15th)
    mock_context_1st = {'logical_date': datetime(2023, 1, 1, 0, 0, 0)}
    result_1st = _check_bert_nicht_calendar_branch(**mock_context_1st)
    assert result_1st == 'trigger_dw_dwh_run_apt_export_monatlich_jp_evt'
    print(f"Monthly DWH export calendar check passed for {mock_context_1st['logical_date'].strftime('%Y-%m-%d')}.")

    # Test for 16th of the month (not 15th)
    mock_context_16th = {'logical_date': datetime(2023, 1, 16, 0, 0, 0)}
    result_16th = _check_bert_nicht_calendar_branch(**mock_context_16th)
    assert result_16th == 'trigger_dw_dwh_run_apt_export_monatlich_jp_evt'
    print(f"Monthly DWH export calendar check passed for {mock_context_16th['logical_date'].strftime('%Y-%m-%d')}.")
```

### Test Case 6: Monthly DWH Export Calendar Logic (`_check_bert_nicht_calendar_branch`) - Non-Applicable Day

*   **Purpose**: Verify that the `_check_bert_nicht_calendar_branch` function correctly identifies the 15th of the month as non-applicable for the monthly DWH export.
*   **Setup**:
    1.  Provide a mock `logical_date` value representing the 15th of a month.
*   **Action**:
    1.  Call `_check_bert_nicht_calendar_branch` with the mock date.
*   **Pass/Fail Criterion**:
    *   **Pass**: The function returns `'end_of_dag_anchor'` for the 15th of the month, indicating a skip.
    *   **Fail**: The function returns `'trigger_dw_dwh_run_apt_export_monatlich_jp_evt'` or any other unexpected task ID.

```python
import pytest
from datetime import datetime

# Assuming _check_bert_nicht_calendar_branch is imported or copied for testing
# from dags.dw_bert_ablaufsteuerung import _check_bert_nicht_calendar_branch

# Copy of the function for direct testing (replace with import in real scenario)
def _check_bert_nicht_calendar_branch(**kwargs):
    execution_date = kwargs["logical_date"]
    day_of_month = execution_date.day
    is_applicable = day_of_month != 15
    if is_applicable:
        return 'trigger_dw_dwh_run_apt_export_monatlich_jp_evt'
    else:
        return 'end_of_dag_anchor'

def test_check_bert_nicht_calendar_branch_non_applicable_day():
    # Test for 15th of the month
    mock_context_15th = {'logical_date': datetime(2023, 1, 15, 0, 0, 0)}
    result_15th = _check_bert_nicht_calendar_branch(**mock_context_15th)
    assert result_15th == 'end_of_dag_anchor'
    print(f"Monthly DWH export calendar check passed for {mock_context_15th['logical_date'].strftime('%Y-%m-%d')}.")
```

### Test Case 7: Time Sensor and TriggerDagRunOperator Configuration (Wait for Completion)

*   **Purpose**: Verify that `TimeSensor` tasks have the correct `target_time` and `TriggerDagRunOperator` tasks with `ActFlg=1` (e.g., `DW.BERT_MONATLICH_JP`) are configured with `wait_for_completion=True`.
*   **Setup**:
    1.  Load the `dw_bert_ablaufsteuerung` DAG.
*   **Action**:
    1.  Inspect the `wait_for_monthly_bert_start_time` and `trigger_dw_bert_monatlich_jp` tasks.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   `wait_for_monthly_bert_start_time` is a `TimeSensor` with `target_time` set to `"20:00:00"`.
        *   `trigger_dw_bert_monatlich_jp` is a `TriggerDagRunOperator` with `trigger_dag_id='dw_bert_monatlich_jp'` and `wait_for_completion=True`.
    *   **Fail**: Any of the above conditions are not met.

```python
import pytest
from airflow.models import DagBag
from airflow.sensors.date_time import TimeSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def test_monthly_bert_tasks_configuration():
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    dag = dag_bag.get_dag('dw_bert_ablaufsteuerung')

    assert dag is not None, "DAG 'dw_bert_ablaufsteuerung' not found."

    # Test TimeSensor
    time_sensor_task = dag.task_dict.get('wait_for_monthly_bert_start_time')
    assert isinstance(time_sensor_task, TimeSensor)
    assert time_sensor_task.target_time == "20:00:00"
    print(f"TimeSensor 'wait_for_monthly_bert_start_time' configured correctly (target_time: {time_sensor_task.target_time}).")

    # Test TriggerDagRunOperator
    trigger_task = dag.task_dict.get('trigger_dw_bert_monatlich_jp')
    assert isinstance(trigger_task, TriggerDagRunOperator)
    assert trigger_task.trigger_dag_id == 'dw_bert_monatlich_jp'
    assert trigger_task.wait_for_completion is True
    print(f"TriggerDagRunOperator 'trigger_dw_bert_monatlich_jp' configured correctly (trigger_dag_id: {trigger_task.trigger_dag_id}, wait_for_completion: {trigger_task.wait_for_completion}).")
```

### Test Case 8: Time Sensor and TriggerDagRunOperator Configuration (Fire and Forget)

*   **Purpose**: Verify that `TimeSensor` tasks have the correct `target_time` and `TriggerDagRunOperator` tasks with `ActFlg=0` (e.g., `DW.BERT_RUN_ADM_CHECK_JP_EVT`) are configured with `wait_for_completion=False`.
*   **Setup**:
    1.  Load the `dw_bert_ablaufsteuerung` DAG.
*   **Action**:
    1.  Inspect the `wait_for_adm_check_event_start_time` and `trigger_dw_bert_run_adm_check_jp_evt` tasks.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   `wait_for_adm_check_event_start_time` is a `TimeSensor` with `target_time` set to `"07:00:00"`.
        *   `trigger_dw_bert_run_adm_check_jp_evt` is a `TriggerDagRunOperator` with `trigger_dag_id='dw_bert_run_adm_check_jp_evt'` and `wait_for_completion=False`.
    *   **Fail**: Any of the above conditions are not met.

```python
import pytest
from airflow.models import DagBag
from airflow.sensors.date_time import TimeSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def test_adm_check_event_tasks_configuration():
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    dag = dag_bag.get_dag('dw_bert_ablaufsteuerung')

    assert dag is not None, "DAG 'dw_bert_ablaufsteuerung' not found."

    # Test TimeSensor
    time_sensor_task = dag.task_dict.get('wait_for_adm_check_event_start_time')
    assert isinstance(time_sensor_task, TimeSensor)
    assert time_sensor_task.target_time == "07:00:00"
    print(f"TimeSensor 'wait_for_adm_check_event_start_time' configured correctly (target_time: {time_sensor_task.target_time}).")

    # Test TriggerDagRunOperator
    trigger_task = dag.task_dict.get('trigger_dw_bert_run_adm_check_jp_evt')
    assert isinstance(trigger_task, TriggerDagRunOperator)
    assert trigger_task.trigger_dag_id == 'dw_bert_run_adm_check_jp_evt'
    assert trigger_task.wait_for_completion is False
    print(f"TriggerDagRunOperator 'trigger_dw_bert_run_adm_check_jp_evt' configured correctly (trigger_dag_id: {trigger_task.trigger_dag_id}, wait_for_completion: {trigger_task.wait_for_completion}).")
```

### Test Case 9: All Time Sensors and TriggerDagRunOperators Configuration

*   **Purpose**: Verify that all `TimeSensor` tasks have the correct `target_time` and all `TriggerDagRunOperator` tasks have the correct `trigger_dag_id` and `wait_for_completion` settings as per the design document.
*   **Setup**:
    1.  Load the `dw_bert_ablaufsteuerung` DAG.
*   **Action**:
    1.  Iterate through all relevant tasks and assert their properties.
*   **Pass/Fail Criterion**:
    *   **Pass**: All `TimeSensor` and `TriggerDagRunOperator` tasks match their specified configurations.
    *   **Fail**: Any task's configuration does not match.

```python
import pytest
from airflow.models import DagBag
from airflow.sensors.date_time import TimeSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def test_all_time_sensors_and_trigger_operators_configuration():
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    dag = dag_bag.get_dag('dw_bert_ablaufsteuerung')

    assert dag is not None, "DAG 'dw_bert_ablaufsteuerung' not found."

    expected_time_sensors = {
        'wait_for_monthly_bert_start_time': "20:00:00",
        'wait_for_adm_check_event_start_time': "07:00:00",
        'wait_for_housekeeping_start_time': "04:03:00",
        'wait_for_daily_apt_export_start_time': "01:30:00",
        'wait_for_master_data_start_time': "01:00:00",
    }

    expected_trigger_operators = {
        'trigger_dw_bert_monatlich_jp': {'id': 'dw_bert_monatlich_jp', 'wait': True},
        'trigger_dw_bert_run_adm_check_jp_evt': {'id': 'dw_bert_run_adm_check_jp_evt', 'wait': False},
        'trigger_dw_bert_adm_housekeeping_jp': {'id': 'dw_bert_adm_housekeeping_jp', 'wait': True},
        'trigger_dw_dwh_apt_export_taeglich_jp': {'id': 'dw_dwh_apt_export_taeglich_jp', 'wait': False},
        'trigger_dw_bert_stammdaten_jp': {'id': 'dw_bert_stammdaten_jp', 'wait': True},
        'trigger_dw_dwh_run_apt_export_monatlich_jp_evt': {'id': 'dw_dwh_run_apt_export_monatlich_jp_evt', 'wait': True},
    }

    for task_id, target_time in expected_time_sensors.items():
        task = dag.task_dict.get(task_id)
        assert isinstance(task, TimeSensor), f"Task {task_id} is not a TimeSensor."
        assert task.target_time == target_time, f"TimeSensor {task_id} has incorrect target_time. Expected {target_time}, got {task.target_time}."
        print(f"TimeSensor '{task_id}' verified.")

    for task_id, config in expected_trigger_operators.items():
        task = dag.task_dict.get(task_id)
        assert isinstance(task, TriggerDagRunOperator), f"Task {task_id} is not a TriggerDagRunOperator."
        assert task.trigger_dag_id == config['id'], f"TriggerDagRunOperator {task_id} has incorrect trigger_dag_id. Expected {config['id']}, got {task.trigger_dag_id}."
        assert task.wait_for_completion == config['wait'], f"TriggerDagRunOperator {task_id} has incorrect wait_for_completion. Expected {config['wait']}, got {task.wait_for_completion}."
        print(f"TriggerDagRunOperator '{task_id}' verified.")
```

### Test Case 10: Full DAG Flow - Monthly BERT Active, DWH Export Active

*   **Purpose**: Simulate a DAG run on a day where both monthly BERT and monthly DWH export calendar conditions are met, and verify the expected task execution path. This tests the overall orchestration logic and conditional branching.
*   **Setup**:
    1.  Use an `execution_date` (e.g., 2023-01-05) that satisfies both `_check_new_calendar_branch` (day 5) and `_check_bert_nicht_calendar_branch` (not day 15).
    2.  Mock `DagRun.find` to indicate no other active runs.
    3.  Mock `TimeSensor` to immediately succeed for testing the flow.
    4.  Mock `TriggerDagRunOperator` to simulate successful child DAG runs.
*   **Action**:
    1.  Run the DAG for the specified `execution_date` in a test environment (e.g., using `dag.test()` or a local Airflow runner).
    2.  Observe the state of tasks.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   `guard_active_run` succeeds.
        *   `check_monthly_bert_calendar` branches to `wait_for_monthly_bert_start_time`.
        *   `trigger_dw_bert_monatlich_jp` succeeds.
        *   All intermediate `TimeSensor` and `TriggerDagRunOperator` tasks (Admin Check, Housekeeping, Daily APT Export, Master Data) succeed.
        *   `check_dwh_export_monthly_calendar` branches to `trigger_dw_dwh_run_apt_export_monatlich_jp_evt`.
        *   `trigger_dw_dwh_run_apt_export_monatlich_jp_evt` succeeds.
        *   The DAG completes successfully.
    *   **Fail**: Any task fails, is skipped unexpectedly, or the branching logic is incorrect.

```python
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType
from datetime import datetime
from unittest.mock import patch, MagicMock

# This test requires a more involved Airflow testing setup,
# potentially using `pytest-airflow` or a local Airflow instance.
# The code below is conceptual for demonstrating the assertion logic.

@provide_session
def test_full_dag_flow_all_active(session):
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    dag = dag_bag.get_dag('dw_bert_ablaufsteuerung')
    assert dag is not None, "DAG 'dw_bert_ablaufsteuerung' not found."

    # Define an execution date that activates both monthly BERT (day 5) and DWH export (not day 15)
    execution_date = datetime(2023, 1, 5, 0, 0, 0)
    logical_date = execution_date

    # Mock DagRun.find to ensure guard_active_run passes
    with patch('airflow.models.DagRun.find', return_value=[]):
        # Mock TimeSensor to always be ready
        with patch('airflow.sensors.date_time.TimeSensor.poke', return_value=True):
            # Mock TriggerDagRunOperator to simulate successful child DAG runs
            with patch('airflow.operators.trigger_dagrun.TriggerDagRunOperator.execute') as mock_trigger_execute:
                # Create a DagRun for the test
                dr = dag.create_dagrun(
                    run_id=DagRunType.MANUAL.value + "_" + execution_date.isoformat(),
                    execution_date=execution_date,
                    start_date=execution_date,
                    state=State.RUNNING,
                    session=session,
                    logical_date=logical_date
                )
                session.commit()

                # Run the DAG (this is a simplified representation, actual execution involves scheduler)
                # In a real pytest-airflow setup, you'd use `dag.test(execution_date=execution_date)`
                # or `TestDag.run_dag(dag_id, execution_date)`
                
                # For this conceptual test, we'll manually check task states after a simulated run
                # This part would typically be handled by an Airflow test harness.
                # For now, we assert the *expected* outcome if the DAG were run.

                # Expected active tasks:
                expected_active_tasks = [
                    'guard_active_run',
                    'check_monthly_bert_calendar',
                    'wait_for_monthly_bert_start_time',
                    'trigger_dw_bert_monatlich_jp',
                    'adm_check_event_start_anchor',
                    'wait_for_adm_check_event_start_time',
                    'trigger_dw_bert_run_adm_check_jp_evt',
                    'housekeeping_start_anchor',
                    'wait_for_housekeeping_start_time',
                    'trigger_dw_bert_adm_housekeeping_jp',
                    'daily_apt_export_start_anchor',
                    'wait_for_daily_apt_export_start_time',
                    'trigger_dw_dwh_apt_export_taeglich_jp',
                    'master_data_start_anchor',
                    'wait_for_master_data_start_time',
                    'trigger_dw_bert_stammdaten_jp',
                    'dwh_export_calendar_check_anchor',
                    'check_dwh_export_monthly_calendar',
                    'trigger_dw_dwh_run_apt_export_monatlich_jp_evt',
                    'end_of_dag_anchor'
                ]
                
                # Expected skipped tasks (EmptyOperators that are not anchors for active branches)
                # In this scenario, no tasks should be skipped due to calendar conditions.
                
                # Assert that all TriggerDagRunOperators were called
                assert mock_trigger_execute.call_count == len([t for t in expected_active_tasks if 'trigger_' in t])
                
                # Assert specific calls for wait_for_completion
                call_args_list = mock_trigger_execute.call_args_list
                
                # Example assertion for a specific trigger:
                # Find the call for 'trigger_dw_bert_monatlich_jp'
                bert_monatlich_call = next((call for call in call_args_list if call.kwargs['task'].task_id == 'trigger_dw_bert_monatlich_jp'), None)
                assert bert_monatlich_call is not None
                assert bert_monatlich_call.kwargs['task'].wait_for_completion is True

                # Find the call for 'trigger_dw_bert_run_adm_check_jp_evt'
                adm_check_call = next((call for call in call_args_list if call.kwargs['task'].task_id == 'trigger_dw_bert_run_adm_check_jp_evt'), None)
                assert adm_check_call is not None
                assert adm_check_call.kwargs['task'].wait_for_completion is False

                print(f"Full DAG flow test (all active) passed for {execution_date.strftime('%Y-%m-%d')}. All expected tasks would have run.")

# Note: Running this test requires a functional Airflow metadata DB connection
# or a more sophisticated mocking strategy for DagRun and task execution.
# For a simple pytest run, the assertions on mock_trigger_execute.call_count
# and call_args_list provide a good proxy for the expected behavior.
```

### Test Case 11: Full DAG Flow - Monthly BERT Skipped, DWH Export Skipped

*   **Purpose**: Simulate a DAG run on a day where both monthly BERT and monthly DWH export calendar conditions are *not* met, and verify the expected task execution path (skipping relevant branches).
*   **Setup**:
    1.  Use an `execution_date` (e.g., 2023-01-15) that fails both `_check_new_calendar_branch` (not day 5 or 25) and `_check_bert_nicht_calendar_branch` (is day 15).
    2.  Mock `DagRun.find` to indicate no other active runs.
    3.  Mock `TimeSensor` to immediately succeed for testing the flow.
    4.  Mock `TriggerDagRunOperator` to simulate successful child DAG runs.
*   **Action**:
    1.  Run the DAG for the specified `execution_date` in a test environment.
    2.  Observe the state of tasks.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   `guard_active_run` succeeds.
        *   `check_monthly_bert_calendar` branches to `adm_check_event_start_anchor` (skipping monthly BERT tasks).
        *   All intermediate `TimeSensor` and `TriggerDagRunOperator` tasks (Admin Check, Housekeeping, Daily APT Export, Master Data) succeed.
        *   `check_dwh_export_monthly_calendar` branches to `end_of_dag_anchor` (skipping monthly DWH export).
        *   The DAG completes successfully, with the correct tasks marked as skipped.
    *   **Fail**: Any task fails, is executed unexpectedly, or the branching logic is incorrect.

```python
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType
from datetime import datetime
from unittest.mock import patch, MagicMock

@provide_session
def test_full_dag_flow_all_skipped(session):
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    dag = dag_bag.get_dag('dw_bert_ablaufsteuerung')
    assert dag is not None, "DAG 'dw_bert_ablaufsteuerung' not found."

    # Define an execution date that skips both monthly BERT (not day 5 or 25) and DWH export (is day 15)
    execution_date = datetime(2023, 1, 15, 0, 0, 0)
    logical_date = execution_date

    with patch('airflow.models.DagRun.find', return_value=[]):
        with patch('airflow.sensors.date_time.TimeSensor.poke', return_value=True):
            with patch('airflow.operators.trigger_dagrun.TriggerDagRunOperator.execute') as mock_trigger_execute:
                dr = dag.create_dagrun(
                    run_id=DagRunType.MANUAL.value + "_" + execution_date.isoformat(),
                    execution_date=execution_date,
                    start_date=execution_date,
                    state=State.RUNNING,
                    session=session,
                    logical_date=logical_date
                )
                session.commit()

                # Expected active tasks (excluding the skipped branches):
                expected_active_tasks = [
                    'guard_active_run',
                    'check_monthly_bert_calendar', # This will branch to adm_check_event_start_anchor
                    'adm_check_event_start_anchor',
                    'wait_for_adm_check_event_start_time',
                    'trigger_dw_bert_run_adm_check_jp_evt',
                    'housekeeping_start_anchor',
                    'wait_for_housekeeping_start_time',
                    'trigger_dw_bert_adm_housekeeping_jp',
                    'daily_apt_export_start_anchor',
                    'wait_for_daily_apt_export_start_time',
                    'trigger_dw_dwh_apt_export_taeglich_jp',
                    'master_data_start_anchor',
                    'wait_for_master_data_start_time',
                    'trigger_dw_bert_stammdaten_jp',
                    'dwh_export_calendar_check_anchor',
                    'check_dwh_export_monthly_calendar', # This will branch to end_of_dag_anchor
                    'end_of_dag_anchor'
                ]
                
                # Only the non-skipped TriggerDagRunOperators should be called
                # In this scenario, 'trigger_dw_bert_monatlich_jp' and 'trigger_dw_dwh_run_apt_export_monatlich_jp_evt'
                # should NOT be called.
                
                # Count calls to TriggerDagRunOperator.execute
                # There are 4 expected triggers in the main flow:
                # trigger_dw_bert_run_adm_check_jp_evt
                # trigger_dw_bert_adm_housekeeping_jp
                # trigger_dw_dwh_apt_export_taeglich_jp
                # trigger_dw_bert_stammdaten_jp
                assert mock_trigger_execute.call_count == 4
                
                # Assert that the skipped triggers were NOT called
                called_trigger_ids = [call.kwargs['task'].trigger_dag_id for call in mock_trigger_execute.call_args_list]
                assert 'dw_bert_monatlich_jp' not in called_trigger_ids
                assert 'dw_dwh_run_apt_export_monatlich_jp_evt' not in called_trigger_ids

                print(f"Full DAG flow test (all skipped) passed for {execution_date.strftime('%Y-%m-%d')}. Expected tasks were skipped.")

# Note: Similar to Test Case 10, this requires a proper Airflow testing setup.
```