As a senior data-migration QA engineer, I've reviewed the migration design for `DW.BERT_ABLAUFSTEUERUNG` and the initial Airflow DAG implementation. The current DAG uses `DummyOperator`s and placeholder logic for scheduling and dependencies, as per the phased build plan (P2/P3 items are pending).

The tests below are designed to validate the *orchestration logic* of the migrated scheduler. They cover the current state of the DAG and outline how to test the future implementations of calendar logic, earliest start times, and synchronization objects.

---

## Migration Validation Tests for DW.BERT_ABLAUFSTEUERUNG

### 1. Output Parity Tests

#### Test Case 1.1: Overall DAG Scheduling (Daily)

*   **Purpose:** To verify that the migrated Airflow DAG is configured to run daily, replicating the `Period: 1` from the legacy UC4 scheduler.
*   **Setup:**
    *   The `bert_ablaufsteuerung_dag.py` file is deployed to an Airflow environment (e.g., Cloud Composer).
    *   Access to Airflow UI or CLI.
*   **Action:**
    1.  Trigger the `bert_ablaufsteuerung_dag` manually or wait for its scheduled run.
    2.  Observe the DAG's `schedule_interval` property in the Airflow UI or via the Airflow CLI.
    3.  Inspect the DAG's run history over several days.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `schedule_interval` for `bert_ablaufsteuerung_dag` is set to `'@daily'` (or equivalent cron expression `0 0 * * *`), and the DAG triggers a new run every day.
    *   **Fail:** The `schedule_interval` is incorrect, or the DAG does not trigger daily runs.

*   **Runnable Test Code (pytest - static check):**

```python
import pytest
from airflow.models.dagbag import DagBag
from datetime import datetime

@pytest.fixture(scope="module")
def dag_bag():
    # Load DAGs from the directory where bert_ablaufsteuerung_dag.py is located
    return DagBag(dag_folder='dags', include_examples=False)

def test_dag_schedule_interval(dag_bag):
    """
    Verify the bert_ablaufsteuerung_dag has the correct daily schedule_interval.
    """
    dag_id = 'bert_ablaufsteuerung_dag'
    dag = dag_bag.get_dag(dag_id)

    assert dag is not None, f"DAG {dag_id} not found in DagBag."
    assert dag.schedule_interval == '@daily', \
        f"Expected schedule_interval '@daily', but got {dag.schedule_interval}"

def test_dag_start_date(dag_bag):
    """
    Verify the bert_ablaufsteuerung_dag has a reasonable start_date.
    """
    dag_id = 'bert_ablaufsteuerung_dag'
    dag = dag_bag.get_dag(dag_id)

    assert dag is not None, f"DAG {dag_id} not found in DagBag."
    # Check if start_date is before or equal to a recent date, indicating it's ready to run
    assert dag.start_date <= datetime.now(), \
        f"DAG start_date {dag.start_date} is in the future, preventing immediate runs."
    # Specific check for the provided start_date
    assert dag.start_date == datetime(2023, 1, 1), \
        f"Expected start_date datetime(2023, 1, 1), but got {dag.start_date}"
```

#### Test Case 1.2: Child Task Triggering

*   **Purpose:** To verify that all specified child Job Plans (JOBP) and Events (EVNT) from the legacy UC4 scheduler have corresponding tasks in the Airflow DAG and are triggered during a DAG run.
*   **Setup:**
    *   The `bert_ablaufsteuerung_dag.py` is deployed and active in Airflow.
    *   A successful run of `bert_ablaufsteuerung_dag` has completed.
*   **Action:**
    1.  Access the Airflow UI for a completed run of `bert_ablaufsteuerung_dag`.
    2.  Navigate to the Graph View or Task List.
    3.  Verify that all expected child tasks (e.g., `bert_monthly_jp_task`, `bert_adm_check_evt_task`, etc.) have run and completed successfully (or are in a state indicating they were triggered).
*   **Pass/Fail Criterion:**
    *   **Pass:** All six child tasks listed in the design document (`DW.BERT_MONATLICH_JP`, `DW.BERT_RUN_ADM_CHECK_JP_EVT`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`, `DW.BERT_STAMMDATEN_JP`, `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`) have corresponding tasks in the Airflow DAG, and all these tasks are triggered and complete successfully in a standard DAG run.
    *   **Fail:** Any expected child task is missing, or fails to trigger, or consistently fails during execution.

*   **Runnable Test Code (pytest - static check):**

```python
import pytest
from airflow.models.dagbag import DagBag

@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder='dags', include_examples=False)

def test_all_child_tasks_exist(dag_bag):
    """
    Verify that all expected child tasks are defined in the DAG.
    """
    dag_id = 'bert_ablaufsteuerung_dag'
    dag = dag_bag.get_dag(dag_id)

    assert dag is not None, f"DAG {dag_id} not found in DagBag."

    expected_task_ids = {
        'bert_monthly_jp_task',
        'bert_adm_check_evt_task',
        'bert_adm_housekeeping_jp_task',
        'dwh_apt_export_daily_jp_task',
        'bert_master_data_jp_task',
        'dwh_run_apt_export_monthly_evt_task',
    }

    actual_task_ids = {task.task_id for task in dag.tasks}

    # Check if all expected tasks are present
    missing_tasks = expected_task_ids - actual_task_ids
    assert not missing_tasks, f"Missing expected tasks: {missing_tasks}"

    # Optionally, check for unexpected tasks if the DAG should be strictly defined
    # unexpected_tasks = actual_task_ids - expected_task_ids - {'start', 'end'}
    # assert not unexpected_tasks, f"Unexpected tasks found: {unexpected_tasks}"
```

#### Test Case 1.3: Earliest Start Time (ErlstStTime) Adherence (Future Implementation)

*   **Purpose:** To verify that tasks with `ErlstStTime` (Earliest Start Time) in UC4 do not start before their specified time in Airflow. This test is for future implementation (P2/P3).
*   **Setup:**
    *   The Airflow DAG has been updated to incorporate `ErlstStTime` logic (e.g., using `TimeSensor` or custom Python logic).
    *   An Airflow environment where the DAG can be observed.
*   **Action:**
    1.  Schedule a DAG run that includes tasks with `ErlstStTime` (e.g., `bert_monthly_jp_task` at 20:00, `bert_adm_check_evt_task` at 07:00).
    2.  Monitor the Airflow UI (Graph View, Gantt Chart) or task logs for these specific tasks.
    3.  Verify their actual start times.
*   **Pass/Fail Criterion:**
    *   **Pass:** No task starts before its specified `ErlstStTime`. Tasks correctly wait until the `ErlstStTime` before proceeding.
    *   **Fail:** A task starts before its `ErlstStTime`.

*   **Runnable Test Code (Conceptual - requires Airflow runtime observation):**
    *   This test requires observing actual Airflow task runs. A `pytest` test could only verify the *presence* of a `TimeSensor` or similar logic, not its runtime behavior.

```python
# This is a conceptual test description, not directly runnable pytest code.
# It describes how to observe Airflow runtime behavior.

# To verify this, you would:
# 1. Ensure the DAG code includes TimeSensor or similar logic for ErlstStTime.
#    Example (conceptual):
#    from airflow.sensors.time import TimeSensor
#    bert_monthly_jp_task = TimeSensor(
#        task_id='wait_for_20_00',
#        target_time='20:00:00',
#        dag=dag,
#    ) >> DummyOperator(task_id='bert_monthly_jp_task_actual_logic')
#
# 2. Trigger the DAG on a day where bert_monthly_jp_task is expected to run.
# 3. Observe the Airflow UI (Graph View, Task Logs) for the 'wait_for_20_00' task.
#    - It should remain in 'running' or 'scheduled' state until 20:00.
#    - The 'bert_monthly_jp_task_actual_logic' should only start after 20:00.
# 4. Check the actual start time of the downstream task in the Airflow UI.
```

#### Test Case 1.4: Calendar-Based Execution (Future Implementation)

*   **Purpose:** To verify that tasks dependent on UC4 calendars (`DW.NEW_CALENDAR`, `DW.KALENDER`) execute only on the correct days and are skipped/not triggered on excluded days. This test is for future implementation (P2/P3).
*   **Setup:**
    *   The Airflow DAG has been updated to incorporate UC4 calendar logic (e.g., using `BranchPythonOperator`, `PythonSensor`, or custom `schedule_interval` logic).
    *   An Airflow environment where the DAG can be observed.
*   **Action:**
    1.  **For `DW.BERT_MONATLICH_JP` (NEW_CALENDAR: 5th or 25th):**
        *   Trigger the DAG on the 5th of a month. Verify `bert_monthly_jp_task` runs.
        *   Trigger the DAG on the 25th of a month. Verify `bert_monthly_jp_task` runs.
        *   Trigger the DAG on a non-5th/25th day (e.g., 10th). Verify `bert_monthly_jp_task` is skipped or not triggered.
    2.  **For `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` (KALENDER excluding 'BERT_NICHT'):**
        *   Identify a day that is part of `DW.KALENDER` and *not* 'BERT_NICHT'. Trigger the DAG. Verify `dwh_run_apt_export_monthly_evt_task` runs.
        *   Identify a day that is part of 'BERT_NICHT'. Trigger the DAG. Verify `dwh_run_apt_export_monthly_evt_task` is skipped or not triggered.
    3.  Observe the Airflow UI (Graph View, Task Logs) for the execution status of these tasks.
*   **Pass/Fail Criterion:**
    *   **Pass:** Tasks execute precisely according to their defined UC4 calendar rules, running on included days and being skipped/not triggered on excluded days.
    *   **Fail:** Tasks run on incorrect days or are skipped on days they should run.

*   **Runnable Test Code (Conceptual - requires Airflow runtime observation):**
    *   This test requires observing actual Airflow task runs across different dates. A `pytest` test could only verify the *presence* of the calendar logic, not its runtime behavior.

```python
# This is a conceptual test description, not directly runnable pytest code.
# It describes how to observe Airflow runtime behavior.

# To verify this, you would:
# 1. Ensure the DAG code includes Python logic to check calendar conditions.
#    Example (conceptual, using a BranchPythonOperator):
#    from airflow.operators.python import BranchPythonOperator
#    from airflow.operators.dummy import DummyOperator
#    from datetime import datetime
#
#    def check_monthly_calendar(**kwargs):
#        execution_date = kwargs['ds_nodash'] # YYYYMMDD
#        day = datetime.strptime(execution_date, '%Y%m%d').day
#        if day == 5 or day == 25:
#            return 'bert_monthly_jp_task'
#        return 'skip_bert_monthly_jp_task'
#
#    branch_monthly_calendar = BranchPythonOperator(
#        task_id='branch_monthly_calendar',
#        python_callable=check_monthly_calendar,
#        dag=dag,
#    )
#
#    bert_monthly_jp_task = DummyOperator(task_id='bert_monthly_jp_task', dag=dag)
#    skip_bert_monthly_jp_task = DummyOperator(task_id='skip_bert_monthly_jp_task', dag=dag)
#
#    branch_monthly_calendar >> [bert_monthly_jp_task, skip_bert_monthly_jp_task]
#
# 2. Manually trigger the DAG for specific execution dates (e.g., 2024-01-05, 2024-01-10, 2024-01-25).
# 3. Observe the Airflow UI (Graph View) for each run:
#    - On 5th/25th: 'bert_monthly_jp_task' should run, 'skip_bert_monthly_jp_task' should be skipped.
#    - On other days: 'bert_monthly_jp_task' should be skipped, 'skip_bert_monthly_jp_task' should run.
```

### 2. Transformation Correctness Tests

#### Test Case 2.1: Task Mapping and Naming

*   **Purpose:** To verify that each UC4 Job Plan (JOBP) and Event (EVNT) referenced in the legacy scheduler is correctly mapped to an Airflow task with an appropriate `task_id`.
*   **Setup:**
    *   The `bert_ablaufsteuerung_dag.py` file is available.
*   **Action:**
    1.  Load the `bert_ablaufsteuerung_dag.py` into a `DagBag` (as in `pytest` setup).
    2.  Inspect the `task_id`s of all tasks within the DAG.
    3.  Compare them against the expected mappings from the design document.
*   **Pass/Fail Criterion:**
    *   **Pass:** All six UC4 child objects have a corresponding Airflow task with a `task_id` that clearly identifies the original UC4 object (e.g., `DW.BERT_MONATLICH_JP` maps to `bert_monthly_jp_task`).
    *   **Fail:** Any expected task is missing, or a task's `task_id` does not align with the design.

*   **Runnable Test Code (pytest):**
    *   (Already covered by `test_all_child_tasks_exist` in Test Case 1.2, which checks for the presence of specific task IDs.)

#### Test Case 2.2: Parallel Execution of Child Tasks

*   **Purpose:** To verify that the child tasks, which have no explicit sequential dependencies among themselves in the legacy UC4 definition, are configured to run in parallel in Airflow after the initial `start_task`.
*   **Setup:**
    *   The `bert_ablaufsteuerung_dag.py` file is available.
*   **Action:**
    1.  Load the `bert_ablaufsteuerung_dag.py` into a `DagBag`.
    2.  Inspect the task dependencies using `dag.task_dict[task_id].upstream_task_ids` and `downstream_task_ids`.
    3.  Verify that all child tasks are downstream of `start_task` and upstream of `end_task`, without direct dependencies among themselves.
*   **Pass/Fail Criterion:**
    *   **Pass:** All child tasks are direct downstream dependencies of `start_task` and direct upstream dependencies of `end_task`, and there are no explicit dependencies defined between the child tasks themselves.
    *   **Fail:** Child tasks have unintended sequential dependencies, or the overall parallel structure is not maintained.

*   **Runnable Test Code (pytest):**

```python
import pytest
from airflow.models.dagbag import DagBag

@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder='dags', include_examples=False)

def test_child_tasks_run_in_parallel(dag_bag):
    """
    Verify that child tasks are configured to run in parallel after 'start_task'.
    """
    dag_id = 'bert_ablaufsteuerung_dag'
    dag = dag_bag.get_dag(dag_id)

    assert dag is not None, f"DAG {dag_id} not found in DagBag."

    start_task = dag.get_task('start')
    end_task = dag.get_task('end')

    child_task_ids = {
        'bert_monthly_jp_task',
        'bert_adm_check_evt_task',
        'bert_adm_housekeeping_jp_task',
        'dwh_apt_export_daily_jp_task',
        'bert_master_data_jp_task',
        'dwh_run_apt_export_monthly_evt_task',
    }

    # Check that all child tasks are downstream of 'start_task'
    for child_task_id in child_task_ids:
        child_task = dag.get_task(child_task_id)
        assert start_task.task_id in child_task.upstream_task_ids, \
            f"Task {child_task_id} is not downstream of 'start_task'."

    # Check that 'end_task' is downstream of all child tasks
    for child_task_id in child_task_ids:
        child_task = dag.get_task(child_task_id)
        assert end_task.task_id in child_task.downstream_task_ids, \
            f"Task {child_task_id} is not upstream of 'end_task'."

    # Check that no child task has another child task as an upstream dependency
    for task_id_1 in child_task_ids:
        task_1 = dag.get_task(task_id_1)
        for task_id_2 in child_task_ids:
            if task_id_1 != task_id_2:
                assert task_id_2 not in task_1.upstream_task_ids, \
                    f"Unexpected dependency: {task_id_1} depends on {task_id_2}."
```

#### Test Case 2.3: Synchronization Object (`DW.BERT_ABLAUFSTEUERUNG_SYNC`) Implementation (Future Implementation)

*   **Purpose:** To verify that the `DW.BERT_ABLAUFSTEUERUNG_SYNC` synchronization object, which implies a critical section or resource management, is correctly translated into an Airflow mechanism (e.g., Pools, `ExternalTaskSensor`, or custom locking). This test is for future implementation (P2/P3).
*   **Setup:**
    *   The Airflow DAG has been updated to incorporate the synchronization logic.
    *   An Airflow environment where the DAG can be observed, potentially with other DAGs or tasks that would contend for the synchronized resource.
*   **Action:**
    1.  **If using Airflow Pools:**
        *   Configure a Pool in Airflow corresponding to `DW.BERT_ABLAUFSTEUERUNG_SYNC`.
        *   Assign relevant tasks in `bert_ablaufsteuerung_dag` (and potentially other DAGs) to this Pool.
        *   Run the DAG and observe the concurrency limits enforced by the Pool.
    2.  **If using `ExternalTaskSensor` (for cross-DAG sync):**
        *   Create a dummy "sync" DAG that sets a flag or completes.
        *   Configure `bert_ablaufsteuerung_dag` tasks to wait on this `ExternalTaskSensor`.
        *   Run both DAGs and observe the waiting behavior.
    3.  **If using custom Python locking:**
        *   Implement the custom locking mechanism.
        *   Run the DAG and verify that only one instance of the critical section runs at a time.
*   **Pass/Fail Criterion:**
    *   **Pass:** The chosen Airflow mechanism correctly replicates the locking/unlocking behavior of `DW.BERT_ABLAUFSTEUERUNG_SYNC`, ensuring resource integrity or sequential execution where required.
    *   **Fail:** The synchronization mechanism fails to prevent concurrent access or incorrect sequencing, leading to resource contention or data corruption.

*   **Runnable Test Code (Conceptual - depends heavily on implementation choice):**
    *   This test is highly dependent on the chosen implementation strategy for the synchronization object.

```python
# This is a conceptual test description.
# Example for Airflow Pools:
# You would define a pool in Airflow UI/config:
# Pool Name: bert_ablaufsteuerung_sync_pool
# Slots: 1
#
# And in your DAG:
# with DAG(...) as dag:
#     task_using_sync = DummyOperator(
#         task_id='task_using_sync',
#         pool='bert_ablaufsteuerung_sync_pool',
#         # ... other task definitions
#     )
#
# To test:
# 1. Trigger multiple instances of 'bert_ablaufsteuerung_dag' concurrently.
# 2. Observe in Airflow UI that 'task_using_sync' (or tasks assigned to the pool)
#    only run one at a time, with others waiting for a slot in 'bert_ablaufsteuerung_sync_pool'.
```

### 3. External-System Replacements Tests

#### Test Case 3.1: UC4 Calendar Logic Translation (Future Implementation)

*   **Purpose:** To specifically validate the correctness of the Python code or Airflow configuration used to translate UC4 calendar rules (`DW.NEW_CALENDAR`, `DW.KALENDER`) into Airflow-native logic. This test focuses on the *mechanism* of translation. This test is for future implementation (P2/P3).
*   **Setup:**
    *   The Python functions or Airflow sensors implementing the calendar logic are available.
*   **Action:**
    1.  Create unit tests for the Python functions responsible for calendar checks.
    2.  Provide various input dates (e.g., 5th, 25th, 10th of a month; dates in `DW.KALENDER` and `BERT_NICHT`).
    3.  Assert the expected boolean outcome (should run/should not run) for each date.
*   **Pass/Fail Criterion:**
    *   **Pass:** The Python calendar logic correctly identifies all valid and invalid execution dates according to the UC4 calendar definitions.
    *   **Fail:** The Python calendar logic misinterprets any calendar rule, leading to incorrect date evaluations.

*   **Runnable Test Code (pytest - unit test for calendar logic):**

```python
import pytest
from datetime import datetime

# Assume this function is implemented in a utility module, e.g., `dags.utils.calendar_helpers`
# For the purpose of this test, we'll define a mock version.
def is_new_calendar_day(execution_date: datetime) -> bool:
    """
    Mock function to check if a given date is the 5th or 25th of the month,
    replicating DW.NEW_CALENDAR logic.
    """
    return execution_date.day in [5, 25]

# Assume this function is implemented in a utility module, e.g., `dags.utils.calendar_helpers`
# For the purpose of this test, we'll define a mock version.
# In a real scenario, 'bert_nicht_days' would be loaded from a configuration or database.
BERT_NICHT_DAYS = {
    datetime(2024, 1, 15).date(), # Example 'BERT_NICHT' day
    datetime(2024, 2, 10).date(),
}
def is_kalender_day_excluding_bert_nicht(execution_date: datetime) -> bool:
    """
    Mock function to check if a given date is a 'DW.KALENDER' day
    excluding 'BERT_NICHT' days.
    For simplicity, assume all days are 'DW.KALENDER' days unless in BERT_NICHT_DAYS.
    """
    return execution_date.date() not in BERT_NICHT_DAYS

def test_new_calendar_logic():
    """
    Test the logic for DW.NEW_CALENDAR (5th or 25th of the month).
    """
    # Test cases for 5th and 25th
    assert is_new_calendar_day(datetime(2024, 1, 5)) is True
    assert is_new_calendar_day(datetime(2024, 2, 25)) is True

    # Test cases for other days
    assert is_new_calendar_day(datetime(2024, 1, 1)) is False
    assert is_new_calendar_day(datetime(2024, 3, 15)) is False
    assert is_new_calendar_day(datetime(2024, 4, 30)) is False

def test_kalender_excluding_bert_nicht_logic():
    """
    Test the logic for DW.KALENDER excluding 'BERT_NICHT' days.
    """
    # Test cases for 'BERT_NICHT' days
    assert is_kalender_day_excluding_bert_nicht(datetime(2024, 1, 15)) is False
    assert is_kalender_day_excluding_bert_nicht(datetime(2024, 2, 10)) is False

    # Test cases for non-'BERT_NICHT' days
    assert is_kalender_day_excluding_bert_nicht(datetime(2024, 1, 1)) is True
    assert is_kalender_day_excluding_bert_nicht(datetime(2024, 1, 16)) is True
    assert is_kalender_day_excluding_bert_nicht(datetime(2024, 2, 9)) is True
```

### 4. Data-Quality / Row-Count / Schema Assertions

#### Test Case 4.1: DAG and Task Run Status

*   **Purpose:** To verify that the Airflow DAG and its constituent tasks complete with the expected status (e.g., 'success', 'skipped', 'failed').
*   **Setup:**
    *   The `bert_ablaufsteuerung_dag.py` is deployed and active in Airflow.
    *   Multiple runs of the DAG have occurred, including runs on calendar-relevant and irrelevant days (once calendar logic is implemented).
*   **Action:**
    1.  Access the Airflow UI.
    2.  Review the DAG Runs view for `bert_ablaufsteuerung_dag`.
    3.  For each DAG run, inspect the status of the overall DAG and its individual tasks.
    4.  Verify that tasks that should run complete successfully, and tasks that should be skipped (due to calendar logic or `ErlstStTime`) are marked as 'skipped'.
*   **Pass/Fail Criterion:**
    *   **Pass:** All DAG runs complete with a 'success' status (unless an intentional failure scenario is being tested). Individual tasks within the DAG runs show expected statuses ('success' for executed tasks, 'skipped' for conditionally skipped tasks).
    *   **Fail:** DAG runs or critical tasks fail unexpectedly, or tasks are not skipped when they should be.

*   **Runnable Test Code (Conceptual - requires Airflow runtime observation):**
    *   This test requires observing actual Airflow task runs. Airflow's REST API or CLI can be used to programmatically check statuses.

```python
# This is a conceptual test description.
# Example using Airflow CLI (can be wrapped in a Python script for pytest):
#
# def check_dag_run_status(dag_id, execution_date, expected_status):
#     import subprocess
#     cmd = [
#         "airflow", "dags", "state", dag_id,
#         "--execution-date", execution_date.isoformat()
#     ]
#     result = subprocess.run(cmd, capture_output=True, text=True)
#     assert result.returncode == 0, f"CLI command failed: {result.stderr}"
#     assert result.stdout.strip() == expected_status, \
#         f"Expected DAG run status '{expected_status}' for {execution_date}, got '{result.stdout.strip()}'"
#
# def check_task_run_status(dag_id, task_id, execution_date, expected_status):
#     import subprocess
#     cmd = [
#         "airflow", "tasks", "state", dag_id, task_id,
#         "--execution-date", execution_date.isoformat()
#     ]
#     result = subprocess.run(cmd, capture_output=True, text=True)
#     assert result.returncode == 0, f"CLI command failed: {result.stderr}"
#     assert result.stdout.strip() == expected_status, \
#         f"Expected task '{task_id}' status '{expected_status}' for {execution_date}, got '{result.stdout.strip()}'"
#
# # Example usage in a pytest function (after calendar logic is implemented):
# def test_dag_and_task_statuses_on_specific_date():
#     dag_id = 'bert_ablaufsteuerung_dag'
#     # Test a day where bert_monthly_jp_task should run
#     execution_date_run = datetime(2024, 1, 5)
#     # Trigger DAG (e.g., via Airflow CLI or API) and wait for completion
#     # ...
#     check_dag_run_status(dag_id, execution_date_run, 'success')
#     check_task_run_status(dag_id, 'bert_monthly_jp_task', execution_date_run, 'success')
#     check_task_run_status(dag_id, 'dwh_apt_export_daily_jp_task', execution_date_run, 'success')
#
#     # Test a day where bert_monthly_jp_task should be skipped
#     execution_date_skip = datetime(2024, 1, 10)
#     # Trigger DAG and wait for completion
#     # ...
#     check_dag_run_status(dag_id, execution_date_skip, 'success') # DAG might still succeed if other tasks run
#     check_task_run_status(dag_id, 'bert_monthly_jp_task', execution_date_skip, 'skipped')
```

#### Test Case 4.2: Number of Tasks Executed

*   **Purpose:** To verify that the correct number of tasks are executed (or skipped) in a DAG run, depending on the specific calendar conditions and `ErlstStTime` logic.
*   **Setup:**
    *   The `bert_ablaufsteuerung_dag.py` is deployed and active in Airflow, with calendar and `ErlstStTime` logic implemented.
    *   A successful DAG run has completed for a specific execution date.
*   **Action:**
    1.  Access the Airflow UI for a completed DAG run.
    2.  Count the number of tasks that transitioned to a 'success' state and the number that transitioned to a 'skipped' state.
    3.  Compare these counts against the expected number of active/skipped tasks for that specific execution date (e.g., on the 5th of the month, `bert_monthly_jp_task` should run; on the 10th, it should be skipped).
*   **Pass/Fail Criterion:**
    *   **Pass:** The total count of 'success' and 'skipped' tasks matches the expected number of tasks for the given execution date, based on the implemented calendar and `ErlstStTime` logic.
    *   **Fail:** The number of executed or skipped tasks does not match expectations, indicating a flaw in the scheduling or conditional execution logic.

*   **Runnable Test Code (Conceptual - requires Airflow runtime observation):**

```python
# This is a conceptual test description.
# Similar to Test Case 4.1, this would involve querying Airflow's metadata database
# or using the Airflow API/CLI to count task instances by state for a given DAG run.
#
# Example (conceptual):
# def get_task_counts_for_dag_run(dag_id, execution_date):
#     # This would involve querying Airflow's DB or API
#     # For example, using Airflow's ORM:
#     # from airflow.models import DagRun, TaskInstance
#     # dag_run = DagRun.find(dag_id=dag_id, execution_date=execution_date)[0]
#     # task_instances = dag_run.get_task_instances()
#     # success_count = sum(1 for ti in task_instances if ti.current_state() == State.SUCCESS)
#     # skipped_count = sum(1 for ti in task_instances if ti.current_state() == State.SKIPPED)
#     # return success_count, skipped_count
#     pass # Placeholder for actual implementation
#
# def test_task_counts_on_calendar_day():
#     dag_id = 'bert_ablaufsteuerung_dag'
#     execution_date = datetime(2024, 1, 5) # A day where bert_monthly_jp_task runs
#     # Trigger DAG and wait for completion
#     # ...
#     success_count, skipped_count = get_task_counts_for_dag_run(dag_id, execution_date)
#
#     # Assuming 6 child tasks + start/end = 8 total tasks.
#     # On 2024-01-05, all 6 child tasks run, so 8 successful tasks.
#     # If calendar logic causes some to skip, adjust expected counts.
#     expected_success_count = 8 # start, end, and 6 child tasks
#     expected_skipped_count = 0
#
#     assert success_count == expected_success_count
#     assert skipped_count == expected_skipped_count
#
# def test_task_counts_on_non_calendar_day():
#     dag_id = 'bert_ablaufsteuerung_dag'
#     execution_date = datetime(2024, 1, 10) # A day where bert_monthly_jp_task skips
#     # Trigger DAG and wait for completion
#     # ...
#     success_count, skipped_count = get_task_counts_for_dag_run(dag_id, execution_date)
#
#     # Assuming 1 task (bert_monthly_jp_task) is skipped.
#     # So, start, end, 5 child tasks = 7 successful tasks.
#     expected_success_count = 7
#     expected_skipped_count = 1
#
#     assert success_count == expected_success_count
#     assert skipped_count == expected_skipped_count
```