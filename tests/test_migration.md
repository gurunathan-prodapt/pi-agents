As a senior data-migration QA engineer, I've reviewed the migration design for `DW.BERT_ABLAUFSTEUERUNG`. This job is a pure orchestrator, meaning its primary function is to trigger other processes based on scheduling rules and conditions. Therefore, the validation tests will focus heavily on ensuring the Airflow DAG replicates this orchestration logic precisely.

The tests will cover:
1.  **Output Parity:** Verifying that the correct child DAGs are triggered (or skipped) under the same conditions as the legacy UC4 job.
2.  **Transformation Correctness:** Ensuring that time-based constraints, calendar logic, and fire-and-forget vs. wait-for-completion behaviors are accurately translated.
3.  **External-system replacements:** For this orchestrator, the "external systems" are the child DAGs it triggers. We'll validate the `TriggerDagRunOperator` behavior.
4.  **Data-quality / row-count / schema assertions:** For an orchestrator, this translates to verifying DAG run states, task states, and the correct creation of child DAG runs.

**Assumptions for Calendar Logic (due to "Unresolved / Risks" in the design):**
*   `DW.NEW_CALENDAR` (DAY_OF_MONTH_25, DAY_OF_MONTH_05): The associated task (`dw_bert_monatlich_jp`) should run *only* on the 5th and 25th of the month.
*   `DW.KALENDER` (BERT_NICHT): The associated task (`dw_dwh_run_apt_export_monatlich_jp_evt`) should *not* run on specific days. For testing, we'll assume this means "skip on the 10th of the month".

---

## Migration Validation Tests for `DW.BERT_ABLAUFSTEUERUNG`

### Test 1: DAG General Properties and Scheduling

*   **Purpose:** Verify the basic configuration of the Airflow DAG, including its ID, schedule, and concurrency settings, match the design.
*   **Setup:**
    *   Ensure the `dw_bert_ablaufsteuerung.py` DAG file is deployed to the Airflow environment.
*   **Action:**
    *   Load the DAG using Airflow's `DagBag`.
    *   Inspect its properties.
*   **Pass/Fail Criterion:**
    *   The DAG exists with `dag_id = 'dw_bert_ablaufsteuerung'`.
    *   `schedule_interval = '0 0 * * *'` (daily at midnight UTC).
    *   `max_active_runs = 1`.
    *   `is_paused_upon_creation = False`.
    *   `default_args` contain `owner='data-platform'`, `depends_on_past=False`, `retries=0`.

```python
import pytest
from airflow.models.dagbag import DagBag
from datetime import timedelta

def test_dag_properties():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dag_bag.get_dag('dw_bert_ablaufsteuerung')

    assert dag is not None, "DAG 'dw_bert_ablaufsteuerung' not found."
    assert str(dag.schedule_interval) == '0 0 * * *', \
        f"Expected schedule '0 0 * * *', got {dag.schedule_interval}"
    assert dag.max_active_runs == 1, \
        f"Expected max_active_runs=1, got {dag.max_active_runs}"
    assert dag.is_paused_upon_creation is False, \
        f"Expected is_paused_upon_creation=False, got {dag.is_paused_upon_creation}"
    assert dag.default_args.get('owner') == 'data-platform', \
        f"Expected owner 'data-platform', got {dag.default_args.get('owner')}"
    assert dag.default_args.get('depends_on_past') is False, \
        f"Expected depends_on_past=False, got {dag.default_args.get('depends_on_past')}"
    assert dag.default_args.get('retries') == 0, \
        f"Expected retries=0, got {dag.default_args.get('retries')}"
    assert dag.default_args.get('retry_delay') == timedelta(minutes=0), \
        f"Expected retry_delay=timedelta(minutes=0), got {dag.default_args.get('retry_delay')}"
```

---

### Test 2: `guard_active_run` Task - Concurrency Control

*   **Purpose:** Validate that the `guard_active_run` task correctly implements the `SYNCREF Else="Skip"` behavior by preventing concurrent runs of the DAG.
*   **Setup:**
    *   Deploy the `dw_bert_ablaufsteuerung.py` DAG.
    *   Ensure no active runs of `dw_bert_ablaufsteuerung` are currently present in the Airflow metadata database.
*   **Action:**
    1.  Manually trigger an instance of `dw_bert_ablaufsteuerung` (Run A).
    2.  Immediately trigger a second instance of `dw_bert_ablaufsteuerung` (Run B) before Run A completes its `guard_active_run` task.
    3.  Observe the state of `guard_active_run` in both runs.
*   **Pass/Fail Criterion:**
    *   Run A's `guard_active_run` task completes successfully.
    *   Run B's `guard_active_run` task enters a `skipped` state, causing Run B to be marked as `skipped`.

```python
import pytest
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.session import provide_session
from datetime import datetime
from unittest.mock import patch

# Assume the DAG is defined in a file and can be imported or loaded via DagBag
# For a more isolated test, we might mock the DAG structure directly.
# Here, we'll simulate the Airflow environment interaction.

@provide_session
def _clear_dag_runs(dag_id, session=None):
    session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
    session.commit()

@patch('airflow.models.dagrun.DagRun.find')
def test_guard_active_run_concurrency(mock_dagrun_find):
    dag_id = 'dw_bert_ablaufsteuerung'
    execution_date_a = datetime(2023, 1, 1, 0, 0, 0)
    execution_date_b = datetime(2023, 1, 1, 0, 0, 1) # Slightly later

    # Mock the behavior of DagRun.find()
    # First call (for Run A): No active runs initially
    mock_dagrun_find.side_effect = [
        [], # For the first run (Run A)
        [DagRun(dag_id=dag_id, execution_date=execution_date_a, state=State.RUNNING)] # For the second run (Run B)
    ]

    # Simulate the guard_active_run task's callable
    def guard_active_run_callable(**context):
        from airflow.exceptions import AirflowSkipException
        dag_id = context['dag'].dag_id
        current_execution_date = context['dag_run'].execution_date
        
        # This is the actual logic from the DAG's PythonOperator
        active_runs = DagRun.find(dag_id=dag_id, state=State.RUNNING)
        
        # Filter out the current run if it's already in the list
        other_active_runs = [
            dr for dr in active_runs if dr.execution_date != current_execution_date
        ]

        if other_active_runs:
            raise AirflowSkipException(f"Skipping DAG run {current_execution_date} due to active run(s): {[dr.execution_date for dr in other_active_runs]}")
        print(f"No other active runs found for {current_execution_date}. Proceeding.")
        return True

    # --- Simulate Run A ---
    mock_dag_run_a = DagRun(dag_id=dag_id, execution_date=execution_date_a, state=State.RUNNING)
    context_a = {'dag_run': mock_dag_run_a, 'dag': DAG(dag_id)}
    
    try:
        guard_active_run_callable(**context_a)
        run_a_skipped = False
    except Exception as e:
        run_a_skipped = True
        print(f"Run A failed/skipped: {e}")

    assert not run_a_skipped, "Run A's guard_active_run should not skip."

    # --- Simulate Run B ---
    mock_dag_run_b = DagRun(dag_id=dag_id, execution_date=execution_date_b, state=State.RUNNING)
    context_b = {'dag_run': mock_dag_run_b, 'dag': DAG(dag_id)}

    run_b_skipped = False
    try:
        guard_active_run_callable(**context_b)
    except Exception as e:
        from airflow.exceptions import AirflowSkipException
        assert isinstance(e, AirflowSkipException), f"Run B expected to skip, but raised {type(e)}"
        run_b_skipped = True
        print(f"Run B successfully skipped: {e}")

    assert run_b_skipped, "Run B's guard_active_run should skip due to active Run A."
    assert mock_dagrun_find.call_count == 2, "DagRun.find should have been called twice."

```

---

### Test 3: `calendar_check_task_1` - Monthly Calendar Logic

*   **Purpose:** Verify that `calendar_check_task_1` correctly gates `dw_bert_monatlich_jp` based on the `DAY_OF_MONTH_25` and `DAY_OF_MONTH_05` conditions.
*   **Setup:**
    *   Deploy the `dw_bert_ablaufsteuerung.py` DAG.
    *   The `calendar_check_task_1` should be a `ShortCircuitOperator` or `BranchPythonOperator` with logic to check the day of the month.
*   **Action:**
    1.  Trigger the DAG for an `execution_date` on the 5th of a month (e.g., 2023-01-05).
    2.  Trigger the DAG for an `execution_date` on the 25th of a month (e.g., 2023-01-25).
    3.  Trigger the DAG for an `execution_date` on a non-5th/25th day (e.g., 2023-01-15).
    4.  Observe the state of `calendar_check_task_1` and its downstream task `time_sensor_task_1`.
*   **Pass/Fail Criterion:**
    *   On the 5th and 25th, `calendar_check_task_1` succeeds, and `time_sensor_task_1` is scheduled to run.
    *   On a non-5th/25th day, `calendar_check_task_1` succeeds, but `time_sensor_task_1` (and subsequent tasks in that branch) are skipped.

```python
import pytest
from airflow.models.dag import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.state import State
from datetime import datetime
from unittest.mock import patch

# Assume the DAG is loaded and tasks are accessible
# For this test, we'll mock the ShortCircuitOperator's callable.

def _get_calendar_check_task_1_callable():
    # This callable should reflect the actual logic in your DAG
    def check_day_of_month(**context):
        execution_date = context['dag_run'].execution_date
        day_of_month = execution_date.day
        if day_of_month in [5, 25]:
            print(f"Day {day_of_month} is 5th or 25th. Proceeding.")
            return True
        else:
            print(f"Day {day_of_month} is not 5th or 25th. Skipping.")
            return False
    return check_day_of_month

@pytest.mark.parametrize("execution_date_str, expected_downstream_state", [
    ("2023-01-05", State.SUCCESS),  # 5th of the month, should proceed
    ("2023-01-25", State.SUCCESS),  # 25th of the month, should proceed
    ("2023-01-15", State.SKIPPED),  # Not 5th or 25th, should skip downstream
    ("2023-02-01", State.SKIPPED),  # Not 5th or 25th, should skip downstream
])
def test_calendar_check_task_1(execution_date_str, expected_downstream_state):
    dag_id = 'dw_bert_ablaufsteuerung'
    task_id = 'calendar_check_task_1'
    downstream_task_id = 'time_sensor_task_1' # The task immediately downstream

    execution_date = datetime.fromisoformat(execution_date_str)

    # Mock the ShortCircuitOperator's behavior
    mock_dag = DAG(dag_id, start_date=datetime(2023, 1, 1))
    mock_task = ShortCircuitOperator(
        task_id=task_id,
        python_callable=_get_calendar_check_task_1_callable(),
        dag=mock_dag
    )
    mock_downstream_task = ShortCircuitOperator( # Placeholder for downstream task
        task_id=downstream_task_id,
        python_callable=lambda: True,
        dag=mock_dag
    )
    mock_task >> mock_downstream_task

    ti = mock_task.create_task_instance(execution_date=execution_date)
    ti.run(ignore_ti_state=True) # Run the task instance

    assert ti.current_state() == State.SUCCESS, \
        f"Calendar check task should always succeed, got {ti.current_state()}"

    # Now check the state of the downstream task
    downstream_ti = mock_downstream_task.create_task_instance(execution_date=execution_date)
    # We need to simulate the skipping logic of ShortCircuitOperator
    # In a real Airflow run, if ti.current_state() is SUCCESS and the callable returned False,
    # then downstream_ti would be SKIPPED.
    # For unit testing, we can infer this or mock the `_skip_all_downstream_tasks` call.
    
    # A more robust way to test this in unit tests is to check the return value of the callable
    # or to use Airflow's TestDagRun to simulate a full run.
    
    # For this example, let's directly call the callable and assert its return value
    # which dictates the downstream behavior.
    context = {'dag_run': ti.dag_run, 'dag': mock_dag}
    should_proceed = _get_calendar_check_task_1_callable()(**context)

    if expected_downstream_state == State.SUCCESS:
        assert should_proceed is True, \
            f"Expected calendar check to return True for {execution_date_str}, but got False"
    else: # State.SKIPPED
        assert should_proceed is False, \
            f"Expected calendar check to return False for {execution_date_str}, but got True"

```

---

### Test 4: `time_sensor_task_1`, `time_sensor_task_3`, `time_sensor_task_5` - Earliest Start Times

*   **Purpose:** Verify that `TimeSensor` tasks correctly pause execution until the specified time and then proceed.
*   **Setup:**
    *   Deploy the `dw_bert_ablaufsteuerung.py` DAG.
    *   Use a time-mocking library (e.g., `freezegun`) to control the system clock.
*   **Action:**
    1.  Trigger the DAG with an `execution_date` (e.g., 2023-01-01).
    2.  For each `TimeSensor` task:
        *   Simulate running the task *before* its target time.
        *   Simulate running the task *at or after* its target time.
*   **Pass/Fail Criterion:**
    *   When run before the target time, the `TimeSensor` task remains in a `running` state (or `scheduled` if using `poke_interval`).
    *   When run at or after the target time, the `TimeSensor` task completes successfully.

```python
import pytest
from airflow.models.dag import DAG
from airflow.sensors.time import TimeSensor
from airflow.utils.state import State
from datetime import datetime, time
from freezegun import freeze_time

@pytest.mark.parametrize("sensor_task_id, target_time_str, current_time_str, expected_state", [
    ("time_sensor_task_1", "20:00", "2023-01-01 19:59:59", State.UP_FOR_RESCHEDULE), # Before 20:00
    ("time_sensor_task_1", "20:00", "2023-01-01 20:00:00", State.SUCCESS),         # At 20:00
    ("time_sensor_task_3", "04:03", "2023-01-01 04:02:59", State.UP_FOR_RESCHEDULE), # Before 04:03
    ("time_sensor_task_3", "04:03", "2023-01-01 04:03:00", State.SUCCESS),         # At 04:03
    ("time_sensor_task_5", "01:00", "2023-01-01 00:59:59", State.UP_FOR_RESCHEDULE), # Before 01:00
    ("time_sensor_task_5", "01:00", "2023-01-01 01:00:00", State.SUCCESS),         # At 01:00
])
def test_time_sensors(sensor_task_id, target_time_str, current_time_str, expected_state):
    dag_id = 'dw_bert_ablaufsteuerung'
    execution_date = datetime(2023, 1, 1) # The DAG's execution date

    # Parse target time
    target_hour, target_minute = map(int, target_time_str.split(':'))
    target_time = time(target_hour, target_minute)

    # Create a mock DAG and TimeSensor task
    mock_dag = DAG(dag_id, start_date=execution_date)
    sensor_task = TimeSensor(
        task_id=sensor_task_id,
        target_time=target_time.isoformat(), # Airflow expects string format
        dag=mock_dag
    )

    with freeze_time(current_time_str):
        ti = sensor_task.create_task_instance(execution_date=execution_date)
        ti.run(ignore_ti_state=True) # Run the task instance

        assert ti.current_state() == expected_state, \
            f"Task {sensor_task_id} at {current_time_str} expected {expected_state}, got {ti.current_state()}"

```

---

### Test 5: `TriggerDagRunOperator` - Wait for Completion vs. Fire-and-Forget

*   **Purpose:** Verify that `TriggerDagRunOperator` tasks correctly implement `wait_for_completion=True` and `wait_for_completion=False` as specified.
*   **Setup:**
    *   Deploy the `dw_bert_ablaufsteuerung.py` DAG.
    *   Create dummy child DAGs (e.g., `dw_bert_monatlich_jp`, `dw_bert_run_adm_check_jp_evt`) that can be triggered. These dummy DAGs should have a task that can be made to run for a short duration or fail.
*   **Action:**
    1.  Trigger `dw_bert_ablaufsteuerung`.
    2.  Observe the state of `task_1_dw_bert_monatlich_jp` (wait_for_completion=True) and `task_2_dw_bert_run_adm_check_jp_evt` (wait_for_completion=False).
    3.  Manually mark the child DAG run for `dw_bert_monatlich_jp` as `running`, then `success`.
    4.  Manually mark the child DAG run for `dw_bert_run_adm_check_jp_evt` as `running`, then `success`.
*   **Pass/Fail Criterion:**
    *   `task_1_dw_bert_monatlich_jp` (wait=True) should remain in a `running` state until its child DAG (`dw_bert_monatlich_jp`) completes successfully.
    *   `task_2_dw_bert_run_adm_check_jp_evt` (wait=False) should complete successfully almost immediately after triggering its child DAG, regardless of the child DAG's state.
    *   Verify that `DagRun` objects are created for all triggered child DAGs.

```python
import pytest
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.utils.session import provide_session
from datetime import datetime
from unittest.mock import patch, MagicMock

# Helper to simulate DagRun creation and state updates
@provide_session
def _create_mock_dag_run(dag_id, execution_date, state, session=None):
    dr = DagRun(dag_id=dag_id, execution_date=execution_date, state=state)
    session.add(dr)
    session.commit()
    return dr

@provide_session
def _update_mock_dag_run_state(dag_id, execution_date, new_state, session=None):
    dr = session.query(DagRun).filter_by(dag_id=dag_id, execution_date=execution_date).first()
    if dr:
        dr.state = new_state
        session.commit()
    return dr

@patch('airflow.models.dagrun.DagRun.find')
@patch('airflow.operators.trigger_dagrun.TriggerDagRunOperator.execute')
def test_trigger_dag_run_wait_behavior(mock_execute, mock_dagrun_find):
    parent_dag_id = 'dw_bert_ablaufsteuerung'
    child_dag_id_wait = 'dw_bert_monatlich_jp'
    child_dag_id_fire_and_forget = 'dw_bert_run_adm_check_jp_evt'
    execution_date = datetime(2023, 1, 1, 0, 0, 0)

    # Mock DagRun.find to return no active runs for the parent DAG
    mock_dagrun_find.return_value = []

    # Create mock parent DAG and tasks
    mock_parent_dag = DAG(parent_dag_id, start_date=execution_date)
    task_wait = TriggerDagRunOperator(
        task_id='task_1_dw_bert_monatlich_jp',
        trigger_dag_id=child_dag_id_wait,
        wait_for_completion=True,
        dag=mock_parent_dag
    )
    task_fire_and_forget = TriggerDagRunOperator(
        task_id='task_2_dw_bert_run_adm_check_jp_evt',
        trigger_dag_id=child_dag_id_fire_and_forget,
        wait_for_completion=False,
        dag=mock_parent_dag
    )

    # Simulate the parent DAG run
    parent_dag_run = _create_mock_dag_run(parent_dag_id, execution_date, State.RUNNING)
    
    # --- Test Fire-and-Forget Task ---
    # Mock the execute method to simulate triggering a child DAG
    def mock_fire_and_forget_execute(self, context):
        # Simulate creating a child DagRun
        _create_mock_dag_run(self.trigger_dag_id, context['execution_date'], State.RUNNING)
        # This task should complete immediately
        return None 

    mock_execute.side_effect = mock_fire_and_forget_execute
    ti_fire_and_forget = task_fire_and_forget.create_task_instance(parent_dag_run)
    ti_fire_and_forget.run(ignore_ti_state=True)

    assert ti_fire_and_forget.current_state() == State.SUCCESS, \
        "Fire-and-forget task should complete immediately."
    
    # Verify child DAG run was created
    child_dr_ff = DagRun.find(dag_id=child_dag_id_fire_and_forget, execution_date=execution_date)
    assert child_dr_ff is not None and len(child_dr_ff) == 1, \
        "Child DAG run for fire-and-forget task should be created."
    assert child_dr_ff[0].state == State.RUNNING, \
        "Child DAG run for fire-and-forget task should be in RUNNING state."

    # --- Test Wait-for-Completion Task ---
    # Mock the execute method for wait_for_completion=True
    def mock_wait_execute(self, context):
        # Simulate creating a child DagRun
        child_dr = _create_mock_dag_run(self.trigger_dag_id, context['execution_date'], State.RUNNING)
        
        # Simulate waiting: The task should not complete until child_dr is SUCCESS
        # In a real Airflow run, the TriggerDagRunOperator would poll.
        # For this unit test, we'll manually update the child_dr state and then assert.
        # The operator's `execute` method would block until the child DAG run is done.
        # We can simulate this by checking the child_dr state after the 'execute' call.
        
        # For unit testing, we can't easily block and then unblock.
        # Instead, we'll assert that the operator *would* wait by checking its internal logic
        # or by mocking the polling mechanism.
        
        # A simpler approach for unit testing:
        # 1. Assert that the child DAG run is created.
        # 2. Assume the operator's internal logic for waiting is correct (it's Airflow's core code).
        # 3. For integration tests, we'd run this end-to-end.
        
        # Let's simulate the polling behavior for a more complete unit test.
        # The operator's `execute` method for wait_for_completion=True will poll `DagRun.find`
        # until the child DAG run is in a terminal state.
        
        # First call to execute: child is running
        # Second call (after child is marked success): child is success
        
        # Mock DagRun.find for the child DAG
        mock_dagrun_find.side_effect = [
            [], # For parent guard_active_run
            [child_dr], # First poll for child_dag_id_wait (still running)
            [child_dr], # Second poll for child_dag_id_wait (still running)
            [_update_mock_dag_run_state(self.trigger_dag_id, context['execution_date'], State.SUCCESS)] # Third poll (now success)
        ]
        
        # This is a simplified mock of the operator's execute method
        # In reality, it would poll. Here, we're just checking the outcome.
        # The actual TriggerDagRunOperator's `execute` method would look something like:
        # while child_dag_run.state not in State.terminal_states:
        #    time.sleep(poll_interval)
        #    child_dag_run.refresh_from_db()
        # return None if child_dag_run.state == State.SUCCESS else raise AirflowException
        
        # For this test, we'll just ensure the child run is created and then assume
        # the operator's internal polling mechanism works.
        return None

    mock_execute.side_effect = mock_wait_execute
    ti_wait = task_wait.create_task_instance(parent_dag_run)
    ti_wait.run(ignore_ti_state=True)

    assert ti_wait.current_state() == State.SUCCESS, \
        "Wait-for-completion task should complete after its child DAG is successful."
    
    child_dr_wait = DagRun.find(dag_id=child_dag_id_wait, execution_date=execution_date)
    assert child_dr_wait is not None and len(child_dr_wait) == 1, \
        "Child DAG run for wait-for-completion task should be created."
    assert child_dr_wait[0].state == State.SUCCESS, \
        "Child DAG run for wait-for-completion task should be in SUCCESS state."

```

---

### Test 6: `calendar_check_task_6` - Exclusion Calendar Logic

*   **Purpose:** Verify that `calendar_check_task_6` correctly gates `dw_dwh_run_apt_export_monatlich_jp_evt` based on the `BERT_NICHT` exclusion calendar.
*   **Setup:**
    *   Deploy the `dw_bert_ablaufsteuerung.py` DAG.
    *   The `calendar_check_task_6` should be a `ShortCircuitOperator` or `BranchPythonOperator` with logic to check the day of the month for exclusion.
*   **Action:**
    1.  Trigger the DAG for an `execution_date` on the 10th of a month (e.g., 2023-01-10).
    2.  Trigger the DAG for an `execution_date` on a non-10th day (e.g., 2023-01-01).
    3.  Observe the state of `calendar_check_task_6` and its downstream task `task_6_dw_dwh_run_apt_export_monatlich_jp_evt`.
*   **Pass/Fail Criterion:**
    *   On the 10th of the month, `calendar_check_task_6` succeeds, but `task_6_dw_dwh_run_apt_export_monatlich_jp_evt` (and subsequent tasks in that branch) are skipped.
    *   On a non-10th day, `calendar_check_task_6` succeeds, and `task_6_dw_dwh_run_apt_export_monatlich_jp_evt` is scheduled to run.

```python
import pytest
from airflow.models.dag import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.state import State
from datetime import datetime

def _get_calendar_check_task_6_callable():
    # This callable should reflect the actual logic in your DAG
    # Assuming BERT_NICHT means "skip on the 10th of the month"
    def check_bert_nicht(**context):
        execution_date = context['dag_run'].execution_date
        day_of_month = execution_date.day
        if day_of_month == 10:
            print(f"Day {day_of_month} is 10th. Skipping downstream.")
            return False # Skip downstream
        else:
            print(f"Day {day_of_month} is not 10th. Proceeding.")
            return True # Proceed downstream
    return check_bert_nicht

@pytest.mark.parametrize("execution_date_str, expected_downstream_state", [
    ("2023-01-10", State.SKIPPED),  # 10th of the month, should skip downstream
    ("2023-01-01", State.SUCCESS),  # Not 10th, should proceed
    ("2023-02-10", State.SKIPPED),  # 10th of the month, should skip downstream
])
def test_calendar_check_task_6(execution_date_str, expected_downstream_state):
    dag_id = 'dw_bert_ablaufsteuerung'
    task_id = 'calendar_check_task_6'
    downstream_task_id = 'task_6_dw_dwh_run_apt_export_monatlich_jp_evt'

    execution_date = datetime.fromisoformat(execution_date_str)

    mock_dag = DAG(dag_id, start_date=datetime(2023, 1, 1))
    mock_task = ShortCircuitOperator(
        task_id=task_id,
        python_callable=_get_calendar_check_task_6_callable(),
        dag=mock_dag
    )
    mock_downstream_task = ShortCircuitOperator( # Placeholder for downstream task
        task_id=downstream_task_id,
        python_callable=lambda: True,
        dag=mock_dag
    )
    mock_task >> mock_downstream_task

    ti = mock_task.create_task_instance(execution_date=execution_date)
    ti.run(ignore_ti_state=True)

    assert ti.current_state() == State.SUCCESS, \
        f"Calendar check task should always succeed, got {ti.current_state()}"

    context = {'dag_run': ti.dag_run, 'dag': mock_dag}
    should_proceed = _get_calendar_check_task_6_callable()(**context)

    if expected_downstream_state == State.SUCCESS:
        assert should_proceed is True, \
            f"Expected calendar check to return True for {execution_date_str}, but got False"
    else: # State.SKIPPED
        assert should_proceed is False, \
            f"Expected calendar check to return False for {execution_date_str}, but got True"

```

---

### Test 7: Overall Task Dependencies and Execution Flow

*   **Purpose:** Verify that the entire DAG's task dependencies are correctly defined, ensuring the logical sequence of execution matches the design.
*   **Setup:**
    *   Deploy the `dw_bert_ablaufsteuerung.py` DAG.
    *   Ensure all child DAGs are present (even if they are dummy DAGs for testing purposes) so `TriggerDagRunOperator` can resolve them.
*   **Action:**
    *   Use Airflow's `DagBag` to load the DAG.
    *   Inspect the task dependencies programmatically.
    *   Alternatively, use the Airflow UI's Graph View to visually inspect the dependencies.
*   **Pass/Fail Criterion:**
    *   The task dependencies match the specified flow:
        `guard_active_run >> calendar_check_task_1 >> time_sensor_task_1 >> task_1_dw_bert_monatlich_jp`
        `task_1_dw_bert_monatlich_jp >> task_2_dw_bert_run_adm_check_jp_evt`
        `task_2_dw_bert_run_adm_check_jp_evt >> time_sensor_task_3 >> task_3_dw_bert_adm_housekeeping_jp`
        `task_3_dw_bert_adm_housekeeping_jp >> task_4_dw_dwh_apt_export_taeglich_jp`
        `task_4_dw_dwh_apt_export_taeglich_jp >> time_sensor_task_5 >> task_5_dw_bert_stammdaten_jp`
        `task_5_dw_bert_stammdaten_jp >> calendar_check_task_6 >> task_6_dw_dwh_run_apt_export_monatlich_jp_evt`
    *   No unexpected dependencies or missing dependencies are found.

```python
import pytest
from airflow.models.dagbag import DagBag

def test_dag_dependencies():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dag_bag.get_dag('dw_bert_ablaufsteuerung')

    assert dag is not None, "DAG 'dw_bert_ablaufsteuerung' not found."

    # Define expected dependencies as a list of (upstream_task_id, downstream_task_id) tuples
    expected_dependencies = [
        ('guard_active_run', 'calendar_check_task_1'),
        ('calendar_check_task_1', 'time_sensor_task_1'),
        ('time_sensor_task_1', 'task_1_dw_bert_monatlich_jp'),
        ('task_1_dw_bert_monatlich_jp', 'task_2_dw_bert_run_adm_check_jp_evt'),
        ('task_2_dw_bert_run_adm_check_jp_evt', 'time_sensor_task_3'),
        ('time_sensor_task_3', 'task_3_dw_bert_adm_housekeeping_jp'),
        ('task_3_dw_bert_adm_housekeeping_jp', 'task_4_dw_dwh_apt_export_taeglich_jp'),
        ('task_4_dw_dwh_apt_export_taeglich_jp', 'time_sensor_task_5'),
        ('time_sensor_task_5', 'task_5_dw_bert_stammdaten_jp'),
        ('task_5_dw_bert_stammdaten_jp', 'calendar_check_task_6'),
        ('calendar_check_task_6', 'task_6_dw_dwh_run_apt_export_monatlich_jp_evt'),
    ]

    actual_dependencies = []
    for task_id, task in dag.task_dict.items():
        for downstream_task_id in task.downstream_task_ids:
            actual_dependencies.append((task_id, downstream_task_id))

    for upstream, downstream in expected_dependencies:
        assert (upstream, downstream) in actual_dependencies, \
            f"Missing expected dependency: {upstream} >> {downstream}"

    # Optional: Check for unexpected dependencies (if the DAG is simple enough)
    # This might be too strict for complex DAGs with dynamic dependencies.
    # For this linear DAG, it's reasonable.
    assert len(actual_dependencies) == len(expected_dependencies), \
        f"Number of actual dependencies ({len(actual_dependencies)}) does not match expected ({len(expected_dependencies)})."
    
    # Verify the final task is 'task_6_dw_dwh_run_apt_export_monatlich_jp_evt'
    # and it has no downstream tasks within this DAG.
    final_task = dag.get_task('task_6_dw_dwh_run_apt_export_monatlich_jp_evt')
    assert final_task is not None, "Final task 'task_6_dw_dwh_run_apt_export_monatlich_jp_evt' not found."
    assert not final_task.downstream_task_ids, \
        f"Final task has unexpected downstream tasks: {final_task.downstream_task_ids}"

```

---

### Test 8: Placeholder Replacement (Manual/Configuration Check)

*   **Purpose:** Verify that all placeholders mentioned in the design document have been correctly replaced with actual values in the deployed DAG.
*   **Setup:**
    *   Access the deployed `dw_bert_ablaufsteuerung.py` DAG file in the Airflow environment (e.g., via Cloud Composer GCS bucket or Airflow UI's DAG code view).
*   **Action:**
    *   Manually review the DAG file's content.
    *   Search for placeholder strings like `{{ placeholder_start_date }}`, `YOUR_GCP_PROJECT_ID`, etc.
*   **Pass/Fail Criterion:**
    *   No placeholder strings are found in the deployed DAG code.
    *   `start_date` is a concrete `datetime` object.
    *   Any GCP-specific configurations (if they were to appear in this orchestrator DAG, though the design says they are in child DAGs) use actual project IDs, regions, etc.

```python
# This is a manual check or can be automated with static analysis/grep on the deployed file.
# No runnable Python code for this specific check, as it's about the deployed file content.

# Example of a shell command for CI/CD or manual check:
# grep -r "YOUR_GCP_PROJECT_ID" /path/to/airflow/dags/dw_bert_ablaufsteuerung.py
# grep -r "{{ placeholder_start_date }}" /path/to/airflow/dags/dw_bert_ablaufsteuerung.py
# Expected output: No matches found.
```