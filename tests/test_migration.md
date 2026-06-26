As a senior data-migration QA engineer, I've reviewed the migration design and the generated Airflow DAG for `DW.BERT_ABLAUFSTEUERUNG`. The following tests are designed to validate the behavioral equivalence of the migrated orchestration job.

The tests focus on:
1.  **Conditional Execution:** Ensuring calendar-based inclusions and exclusions are correctly applied.
2.  **Task Sequencing:** Verifying the order of operations and dependencies.
3.  **Time-Based Constraints:** Assessing the translation of `ErlstStTime` from UC4.
4.  **External System Triggers:** Confirming that tasks initiating external interactions are correctly orchestrated.

Given the nature of an orchestration job, "output parity" primarily refers to the correct sequence and conditional execution of child jobs/events. "Transformation correctness" applies to the logic determining these conditions (e.g., calendar checks). "Data quality/row count/schema assertions" are not directly applicable to the orchestrator itself but to the data processed by its child jobs, which are outside the scope of this specific DAG migration.

---

### Test Case 1: `DW.BERT_MONATLICH_JP` - Execution on Valid Days (5th/25th)

*   **Purpose:** Verify that the `_check_monthly_run_day` Python callable correctly allows the `DW.BERT_MONATLICH_JP` task (and its subsequent chain) to proceed when the `execution_date` falls on the 5th or 25th of the month, as specified by `DW.NEW_CALENDAR` with `DAY_OF_MONTH_25` and `DAY_OF_MONTH_05`.
*   **Setup:**
    *   An Airflow environment where the `dw_bert_ablaufsteuerung` DAG is deployed.
    *   `pytest` with `unittest.mock.patch` to simulate specific `execution_date` values for the `_check_monthly_run_day` function.
*   **Action:**
    1.  Invoke the `_check_monthly_run_day` function with `ds='2023-01-05'`.
    2.  Invoke the `_check_monthly_run_day` function with `ds='2023-01-25'`.
*   **Pass/Fail Criterion:**
    *   **Pass:** Neither invocation raises an `AirflowSkipException`. This indicates the task would proceed.
    *   **Fail:** Either invocation raises an `AirflowSkipException`, meaning the task would be incorrectly skipped.

```python
import pytest
from airflow.models.dagbag import DagBag
from airflow.exceptions import AirflowSkipException
import pendulum
from unittest.mock import patch

@pytest.fixture(scope="module")
def dag():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    return dag_bag.get_dag('dw_bert_ablaufsteuerung')

def test_check_monthly_run_day_execution(dag):
    """
    Verifies _check_monthly_run_day allows execution on the 5th and 25th.
    """
    # Test on 5th of the month
    with patch('pendulum.parse', return_value=pendulum.datetime(2023, 1, 5)):
        try:
            dag.get_task('check_bert_monatlich_jp_run_day').python_callable(ds='2023-01-05')
            assert True, "Task should not be skipped on the 5th."
        except AirflowSkipException:
            pytest.fail("Task was skipped on the 5th, but should have run.")

    # Test on 25th of the month
    with patch('pendulum.parse', return_value=pendulum.datetime(2023, 1, 25)):
        try:
            dag.get_task('check_bert_monatlich_jp_run_day').python_callable(ds='2023-01-25')
            assert True, "Task should not be skipped on the 25th."
        except AirflowSkipException:
            pytest.fail("Task was skipped on the 25th, but should have run.")

```

---

### Test Case 2: `DW.BERT_MONATLICH_JP` - Skipping on Invalid Days

*   **Purpose:** Verify that the `_check_monthly_run_day` Python callable correctly skips the `DW.BERT_MONATLICH_JP` task when the `execution_date` is not the 5th or 25th of the month.
*   **Setup:**
    *   An Airflow environment where the `dw_bert_ablaufsteuerung` DAG is deployed.
    *   `pytest` with `unittest.mock.patch` to simulate specific `execution_date` values for the `_check_monthly_run_day` function.
*   **Action:**
    1.  Invoke the `_check_monthly_run_day` function with `ds='2023-01-01'`.
    2.  Invoke the `_check_monthly_run_day` function with `ds='2023-01-10'`.
*   **Pass/Fail Criterion:**
    *   **Pass:** Both invocations raise an `AirflowSkipException` with a message indicating the skip reason.
    *   **Fail:** Either invocation does *not* raise an `AirflowSkipException`, meaning the task would be incorrectly executed.

```python
# (Continuing from previous code block)

def test_check_monthly_run_day_skipping(dag):
    """
    Verifies _check_monthly_run_day skips execution on days other than 5th or 25th.
    """
    # Test on 1st of the month
    with patch('pendulum.parse', return_value=pendulum.datetime(2023, 1, 1)):
        with pytest.raises(AirflowSkipException) as excinfo:
            dag.get_task('check_bert_monatlich_jp_run_day').python_callable(ds='2023-01-01')
        assert "Skipping DW.BERT_MONATLICH_JP as it's not the 5th or 25th" in str(excinfo.value)

    # Test on 10th of the month
    with patch('pendulum.parse', return_value=pendulum.datetime(2023, 1, 10)):
        with pytest.raises(AirflowSkipException) as excinfo:
            dag.get_task('check_bert_monatlich_jp_run_day').python_callable(ds='2023-01-10')
        assert "Skipping DW.BERT_MONATLICH_JP as it's not the 5th or 25th" in str(excinfo.value)

```

---

### Test Case 3: `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` - Skipping on `BERT_NICHT` Day (10th)

*   **Purpose:** Verify that the `_check_bert_nicht_exclusion` Python callable correctly skips the `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` task when the `execution_date` falls on the placeholder `BERT_NICHT` exclusion day (10th of the month).
*   **Setup:**
    *   An Airflow environment where the `dw_bert_ablaufsteuerung` DAG is deployed.
    *   `pytest` with `unittest.mock.patch` to simulate `ds='2023-01-10'` for the `_check_bert_nicht_exclusion` function.
*   **Action:**
    1.  Invoke the `_check_bert_nicht_exclusion` function with `ds='2023-01-10'`.
*   **Pass/Fail Criterion:**
    *   **Pass:** The invocation raises an `AirflowSkipException` with a message indicating the `BERT_NICHT` exclusion.
    *   **Fail:** The invocation does *not* raise an `AirflowSkipException`, meaning the task would be incorrectly executed.

```python
# (Continuing from previous code block)

def test_check_bert_nicht_exclusion_skipping(dag):
    """
    Verifies _check_bert_nicht_exclusion skips execution on the 10th (placeholder).
    """
    # Test on 10th of the month (placeholder exclusion)
    with patch('pendulum.parse', return_value=pendulum.datetime(2023, 1, 10)):
        with pytest.raises(AirflowSkipException) as excinfo:
            dag.get_task('check_bert_nicht_exclusion').python_callable(ds='2023-01-10')
        assert "Skipping DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT due to BERT_NICHT exclusion" in str(excinfo.value)

```

---

### Test Case 4: `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` - Execution on Non-`BERT_NICHT` Day

*   **Purpose:** Verify that the `_check_bert_nicht_exclusion` Python callable correctly allows the `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` task to proceed when the `execution_date` is not the placeholder `BERT_NICHT` exclusion day (10th of the month).
*   **Setup:**
    *   An Airflow environment where the `dw_bert_ablaufsteuerung` DAG is deployed.
    *   `pytest` with `unittest.mock.patch` to simulate `ds='2023-01-01'` and `ds='2023-01-05'` for the `_check_bert_nicht_exclusion` function.
*   **Action:**
    1.  Invoke the `_check_bert_nicht_exclusion` function with `ds='2023-01-01'`.
    2.  Invoke the `_check_bert_nicht_exclusion` function with `ds='2023-01-05'`.
*   **Pass/Fail Criterion:**
    *   **Pass:** Neither invocation raises an `AirflowSkipException`. This indicates the task would proceed.
    *   **Fail:** Either invocation raises an `AirflowSkipException`, meaning the task would be incorrectly skipped.

```python
# (Continuing from previous code block)

def test_check_bert_nicht_exclusion_execution(dag):
    """
    Verifies _check_bert_nicht_exclusion allows execution on days other than the 10th.
    """
    # Test on 1st of the month
    with patch('pendulum.parse', return_value=pendulum.datetime(2023, 1, 1)):
        try:
            dag.get_task('check_bert_nicht_exclusion').python_callable(ds='2023-01-01')
            assert True, "Task should not be skipped on the 1st."
        except AirflowSkipException:
            pytest.fail("Task was skipped on the 1st, but should have run.")

    # Test on 5th of the month
    with patch('pendulum.parse', return_value=pendulum.datetime(2023, 1, 5)):
        try:
            dag.get_task('check_bert_nicht_exclusion').python_callable(ds='2023-01-05')
            assert True, "Task should not be skipped on the 5th."
        except AirflowSkipException:
            pytest.fail("Task was skipped on the 5th, but should have run.")

```

---

### Test Case 5: Overall DAG Orchestration Flow and Task Dependencies (Output Parity)

*   **Purpose:** Verify the overall sequence of tasks and how conditional skips affect downstream tasks, ensuring the Airflow DAG's behavior aligns with the intended UC4 orchestration logic. This test highlights a potential behavioral difference due to Airflow's default `trigger_rule`.
*   **Setup:**
    *   An Airflow environment where the `dw_bert_ablaufsteuerung` DAG is deployed.
    *   Access to Airflow UI or logs to observe task states (success, skipped, upstream_failed).
*   **Action:**
    1.  **Scenario A (Generic Day, e.g., Jan 1st):** Trigger the DAG for `execution_date = 2023-01-01`.
    2.  **Scenario B (Monthly Run Day, e.g., Jan 5th):** Trigger the DAG for `execution_date = 2023-01-05`.
    3.  **Scenario C (BERT_NICHT Exclusion Day, e.g., Jan 10th):** Trigger the DAG for `execution_date = 2023-01-10`.
*   **Pass/Fail Criterion:**
    *   **Pass (Conditional):**
        *   **Scenario A (Jan 1st):**
            *   `check_bert_monatlich_jp_run_day` is SKIPPED.
            *   `dw_bert_monatlich_jp` and all subsequent tasks (`dw_bert_run_adm_check_jp_evt`, `dw_bert_adm_housekeeping_jp`, `dw_dwh_apt_export_taeglich_jp`, `dw_bert_stammdaten_jp`, `check_bert_nicht_exclusion`, `dw_dwh_run_apt_export_monatlich_jp_evt`) are marked as UPSTREAM_SKIPPED or SKIPPED.
            *   *Note:* This behavior (all subsequent tasks skipping) is due to Airflow's default `trigger_rule='all_success'`. If the UC4 job was designed such that other branches continue even if an upstream job is skipped, this is a **behavioral discrepancy**. The design document implies a strict sequence, so this might be acceptable, but it's crucial to confirm. If other tasks should run, their `trigger_rule` must be changed (e.g., `all_done`, `none_failed_min_one_success`).
        *   **Scenario B (Jan 5th):**
            *   `check_bert_monatlich_jp_run_day` and `dw_bert_monatlich_jp` run successfully.
            *   All subsequent tasks (`dw_bert_run_adm_check_jp_evt` through `dw_dwh_run_apt_export_monatlich_jp_evt`) run successfully (assuming Jan 5th is not a `BERT_NICHT` day).
        *   **Scenario C (Jan 10th):**
            *   `check_bert_monatlich_jp_run_day` is SKIPPED (as 10th is not 5th/25th).
            *   `dw_bert_monatlich_jp` and its direct downstream tasks are SKIPPED (due to `trigger_rule='all_success'`).
            *   `check_bert_nicht_exclusion` is SKIPPED (due to its own logic).
            *   `dw_dwh_run_apt_export_monatlich_jp_evt` is SKIPPED (due to `check_bert_nicht_exclusion` skipping).
            *   *Note:* Similar to Scenario A, the cascading skip of the entire chain due to `dw_bert_monatlich_jp` skipping needs to be confirmed as desired behavior.
    *   **Fail:** The observed task states (execution, skipping, order) do not match the expected behavior for any of the scenarios.

---

### Test Case 6: `ErlstStTime` (Earliest Start Time) Behavioral Equivalence

*   **Purpose:** Assess whether the `ErlstStTime` constraints specified in the UC4 design (e.g., `DW.BERT_MONATLICH_JP` at 20:00, `DW.BERT_STAMMDATEN_JP` at 01:00) are correctly translated and enforced in the Airflow DAG.
*   **Setup:**
    *   Review the UC4 design document's `ErlstStTime` requirements.
    *   Examine the Airflow DAG code (`dags/dw_bert_ablaufsteuerung_dag.py`).
*   **Action:**
    1.  Compare the `ErlstStTime` values for each UC4 job/event with the corresponding Airflow task implementation.
    2.  Determine if `TimeSensor` tasks or other time-based scheduling mechanisms are used to enforce these earliest start times.
*   **Pass/Fail Criterion:**
    *   **Fail (Behavioral Discrepancy):** The current Airflow DAG does **not** implement specific `TimeSensor` tasks or other mechanisms to enforce the `ErlstStTime` for individual tasks. The DAG runs `@daily`, and tasks execute as soon as their upstream dependencies are met, regardless of the time of day. For example, if the DAG starts at 00:00 and `dw_bert_stammdaten_jp` (UC4 `ErlstStTime=01:00`) has its upstream tasks complete by 00:15, it will execute at 00:15, violating the 01:00 earliest start time. This is a significant behavioral difference from the legacy UC4 system.
    *   **Recommendation:** To achieve behavioral equivalence for `ErlstStTime`, `TimeSensor` tasks should be introduced before tasks with critical earliest start times, or the DAG's `schedule_interval` and `start_date` should be carefully configured to align with the earliest start times of the *first* task in the chain, and subsequent tasks should use `TimeSensor`s.

---

### Test Case 7: External System Interaction (APT Exports Triggering)

*   **Purpose:** Verify that the tasks representing APT exports (`DW.DWH_APT_EXPORT_TAEGLICH_JP`, `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`) are correctly triggered according to their dependencies and calendar conditions. This validates the orchestration aspect of external system interaction, not the actual data transfer.
*   **Setup:**
    *   An Airflow environment where the `dw_bert_ablaufsteuerung` DAG is deployed.
    *   Access to Airflow UI or logs to observe task states.
*   **Action:**
    1.  Trigger the DAG for an `execution_date` where both daily and monthly APT exports are expected to run (e.g., Jan 5th, not Jan 10th).
    2.  Trigger the DAG for an `execution_date` where only the daily APT export is expected (e.g., Jan 1st, not Jan 5th/25th, not Jan 10th).
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   In Action 1: Both `dw_dwh_apt_export_taeglich_jp` and `dw_dwh_run_apt_export_monatlich_jp_evt` tasks execute successfully (i.e., their `BashOperator` commands run).
        *   In Action 2: `dw_dwh_apt_export_taeglich_jp` executes successfully, and `dw_dwh_run_apt_export_monatlich_jp_evt` is skipped (due to `check_bert_nicht_exclusion` not being triggered, or `check_bert_nicht_exclusion` running and not skipping, but its upstream `dw_bert_stammdaten_jp` being skipped due to `dw_bert_monatlich_jp` skipping).
        *   The `echo` statements in the `BashOperator`s confirm that the *triggering* mechanism is working as expected for these placeholders.
    *   **Fail:** The APT export tasks do not execute when expected, or execute when they should be skipped, indicating a failure in the orchestration logic for external system interactions.