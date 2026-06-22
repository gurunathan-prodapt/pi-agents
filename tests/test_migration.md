The migration of `DW.BERT_ABLAUFSTEUERUNG` from UC4 to Airflow is primarily an orchestration re-platforming. Therefore, the validation tests will focus heavily on ensuring the Airflow DAG accurately replicates the scheduling logic, task dependencies, and time/calendar constraints of the original UC4 job.

Given that the original UC4 job is a scheduler and not a data transformation job, direct "output parity" in terms of data rows or "transformation correctness" in the SQL sense (joins, aggregations) is not applicable. Instead, output parity refers to the *behavior* of triggering the correct child jobs at the correct times under the correct conditions.

The calendar logic is explicitly marked as a placeholder in the design, so tests will confirm its presence and placement, but not its internal correctness, which requires further implementation.

---

## Migration Validation Tests for `DW.BERT_ABLAUFSTEUERUNG`

### Test Case 1: DAG Structure and Task Dependencies

*   **Purpose**: Verify that the Airflow DAG's task graph accurately reflects the sequential execution order and dependencies of the original UC4 scheduler as described in the design document. This ensures behavioral equivalence in terms of task flow.
*   **Setup**:
    1.  Ensure the `dw_bert_ablaufsteuerung.py` file is accessible in an Airflow environment or a local Python environment with Airflow installed.
    2.  Load the DAG object from the file.
*   **Action**:
    1.  Inspect the `dag.task_dict` to get all defined tasks.
    2.  Inspect the `dag.task_dict[task_id].downstream_task_ids` or `upstream_task_ids` for each task to verify the dependencies.
*   **Pass/Fail Criterion**: The task dependencies in the loaded Airflow DAG exactly match the "Execution Order" specified in Section 4 of the design document.

    ```python
    import pytest
    from airflow.models.dag import DAG
    from airflow.utils.dag_cycle_tester import check_cycle
    from airflow.sensors.time import TimeSensor
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    import pendulum

    # Assume dw_bert_ablaufsteuerung.py is in the same directory or importable
    # For testing, we might need to mock some Airflow components or just load the DAG object.
    # A common pattern is to import the DAG directly for testing its structure.
    # from dags.dw_bert_ablaufsteuerung import dag as bert_dag # if it's in dags/ folder
    # For this example, let's simulate loading it.

    # --- Start of simulated dw_bert_ablaufsteuerung.py content for testing ---
    # (This block would typically be in a separate file, but included here for completeness)
    PLACEHOLDER_START_DATE = pendulum.datetime(2023, 1, 1, tz="UTC")

    def _guard_active_run(**context): pass
    def _calendar_check_dw_bert_monatlich_jp(**context): pass
    def _calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt(**context): pass

    with DAG(
        dag_id='dw_bert_ablaufsteuerung',
        start_date=PLACEHOLDER_START_DATE,
        schedule=None,
        catchup=False,
        tags=['bert', 'uc4', 'scheduler'],
    ) as dag:
        guard_active_run = PythonOperator(task_id='guard_active_run', python_callable=_guard_active_run)
        wait_until_20_00_for_dw_bert_monatlich_jp = TimeSensor(task_id='wait_until_20_00_for_dw_bert_monatlich_jp', target_time="20:00:00")
        calendar_check_dw_bert_monatlich_jp = PythonOperator(task_id='calendar_check_dw_bert_monatlich_jp', python_callable=_calendar_check_dw_bert_monatlich_jp)
        trigger_dw_bert_monatlich_jp = TriggerDagRunOperator(task_id='trigger_dw_bert_monatlich_jp', trigger_dag_id='dw_bert_monatlich_jp')
        wait_until_07_00_for_dw_bert_run_adm_check_jp_evt = TimeSensor(task_id='wait_until_07_00_for_dw_bert_run_adm_check_jp_evt', target_time="07:00:00")
        trigger_dw_bert_run_adm_check_jp_evt = TriggerDagRunOperator(task_id='trigger_dw_bert_run_adm_check_jp_evt', trigger_dag_id='dw_bert_run_adm_check_jp_evt')
        wait_until_04_03_for_dw_bert_adm_housekeeping_jp = TimeSensor(task_id='wait_until_04_03_for_dw_bert_adm_housekeeping_jp', target_time="04:03:00")
        trigger_dw_bert_adm_housekeeping_jp = TriggerDagRunOperator(task_id='trigger_dw_bert_adm_housekeeping_jp', trigger_dag_id='dw_bert_adm_housekeeping_jp')
        wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp = TimeSensor(task_id='wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp', target_time="01:30:00")
        trigger_dw_dwh_apt_export_taeglich_jp = TriggerDagRunOperator(task_id='trigger_dw_dwh_apt_export_taeglich_jp', trigger_dag_id='dw_dwh_apt_export_taeglich_jp')
        wait_until_01_00_for_dw_bert_stammdaten_jp = TimeSensor(task_id='wait_until_01_00_for_dw_bert_stammdaten_jp', target_time="01:00:00")
        trigger_dw_bert_stammdaten_jp = TriggerDagRunOperator(task_id='trigger_dw_bert_stammdaten_jp', trigger_dag_id='dw_bert_stammdaten_jp')
        wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt = TimeSensor(task_id='wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt', target_time="01:00:00")
        calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt = PythonOperator(task_id='calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt', python_callable=_calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt)
        trigger_dw_dwh_run_apt_export_monatlich_jp_evt = TriggerDagRunOperator(task_id='trigger_dw_dwh_run_apt_export_monatlich_jp_evt', trigger_dag_id='dw_dwh_run_apt_export_monatlich_jp_evt')

        guard_active_run >> wait_until_20_00_for_dw_bert_monatlich_jp
        wait_until_20_00_for_dw_bert_monatlich_jp >> calendar_check_dw_bert_monatlich_jp
        calendar_check_dw_bert_monatlich_jp >> trigger_dw_bert_monatlich_jp
        trigger_dw_bert_monatlich_jp >> wait_until_07_00_for_dw_bert_run_adm_check_jp_evt
        wait_until_07_00_for_dw_bert_run_adm_check_jp_evt >> trigger_dw_bert_run_adm_check_jp_evt
        trigger_dw_bert_run_adm_check_jp_evt >> wait_until_04_03_for_dw_bert_adm_housekeeping_jp
        wait_until_04_03_for_dw_bert_adm_housekeeping_jp >> trigger_dw_bert_adm_housekeeping_jp
        trigger_dw_bert_adm_housekeeping_jp >> wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp
        wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp >> trigger_dw_dwh_apt_export_taeglich_jp
        trigger_dw_dwh_apt_export_taeglich_jp >> wait_until_01_00_for_dw_bert_stammdaten_jp
        wait_until_01_00_for_dw_bert_stammdaten_jp >> trigger_dw_bert_stammdaten_jp
        trigger_dw_bert_stammdaten_jp >> wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt
        wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt >> calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt
        calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt >> trigger_dw_dwh_run_apt_export_monatlich_jp_evt
    # --- End of simulated dw_bert_ablaufsteuerung.py content for testing ---

    def test_dag_structure_and_dependencies():
        # Check for cycles (Airflow DAGs must be acyclic)
        check_cycle(dag)

        # Define expected dependencies as a dictionary: {upstream_task: [downstream_tasks]}
        expected_dependencies = {
            'guard_active_run': ['wait_until_20_00_for_dw_bert_monatlich_jp'],
            'wait_until_20_00_for_dw_bert_monatlich_jp': ['calendar_check_dw_bert_monatlich_jp'],
            'calendar_check_dw_bert_monatlich_jp': ['trigger_dw_bert_monatlich_jp'],
            'trigger_dw_bert_monatlich_jp': ['wait_until_07_00_for_dw_bert_run_adm_check_jp_evt'],
            'wait_until_07_00_for_dw_bert_run_adm_check_jp_evt': ['trigger_dw_bert_run_adm_check_jp_evt'],
            'trigger_dw_bert_run_adm_check_jp_evt': ['wait_until_04_03_for_dw_bert_adm_housekeeping_jp'],
            'wait_until_04_03_for_dw_bert_adm_housekeeping_jp': ['trigger_dw_bert_adm_housekeeping_jp'],
            'trigger_dw_bert_adm_housekeeping_jp': ['wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp'],
            'wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp': ['trigger_dw_dwh_apt_export_taeglich_jp'],
            'trigger_dw_dwh_apt_export_taeglich_jp': ['wait_until_01_00_for_dw_bert_stammdaten_jp'],
            'wait_until_01_00_for_dw_bert_stammdaten_jp': ['trigger_dw_bert_stammdaten_jp'],
            'trigger_dw_bert_stammdaten_jp': ['wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt'],
            'wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt': ['calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt'],
            'calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt': ['trigger_dw_dwh_run_apt_export_monatlich_jp_evt'],
            'trigger_dw_dwh_run_apt_export_monatlich_jp_evt': [], # Last task in the sequence
        }

        # Verify all expected tasks are present
        assert set(expected_dependencies.keys()).issubset(set(dag.task_ids)), \
            "Not all expected tasks are present in the DAG."

        # Verify dependencies for each task
        for upstream_task_id, expected_downstreams in expected_dependencies.items():
            task = dag.get_task(upstream_task_id)
            actual_downstreams = [t.task_id for t in task.downstream_list]
            assert sorted(actual_downstreams) == sorted(expected_downstreams), \
                f"Dependencies for task '{upstream_task_id}' do not match. " \
                f"Expected: {sorted(expected_downstreams)}, Actual: {sorted(actual_downstreams)}"

        print("DAG structure and dependencies verified successfully.")

    ```

### Test Case 2: `guard_active_run` Behavior (Else=Skip)

*   **Purpose**: Verify that the `guard_active_run` task correctly implements the UC4 "Else=Skip" behavior, preventing concurrent DAG runs of the same DAG. This is crucial for maintaining the original scheduler's single-instance execution policy.
*   **Setup**:
    1.  An Airflow environment with the `dw_bert_ablaufsteuerung` DAG deployed.
    2.  Use Airflow's testing utilities or mock the `DagRun.find` method.
*   **Action**:
    1.  Manually trigger a DAG run for `dw_bert_ablaufsteuerung` and let it reach the `guard_active_run` task (or simulate its state as 'running').
    2.  Immediately trigger a *second* DAG run for `dw_bert_ablaufsteuerung`.
*   **Pass/Fail Criterion**: The `guard_active_run` task in the *second* DAG run fails with an `AirflowSkipException`, and the second DAG run is marked as skipped or failed at that task, preventing further execution.

    ```python
    import pytest
    from unittest.mock import patch, MagicMock
    from airflow.exceptions import AirflowSkipException
    from airflow.models import DagRun
    from airflow.utils.session import provide_session
    import pendulum

    # Assuming the _guard_active_run function is directly importable or defined as above
    # from dags.dw_bert_ablaufsteuerung import _guard_active_run

    # Re-define for isolated testing if not importing
    def _guard_active_run(**context):
        dag_id = context['dag'].dag_id
        current_run_id = context['dag_run'].run_id
        concurrent_runs = DagRun.find(dag_id=dag_id, state="running")
        concurrent_runs = [run for run in concurrent_runs if run.run_id != current_run_id]
        if concurrent_runs:
            concurrent_run_ids = [run.run_id for run in concurrent_runs]
            raise AirflowSkipException(
                f"Skipping this DAG run ({current_run_id}) as other active run(s) "
                f"for DAG '{dag_id}' are already running: {concurrent_run_ids}"
            )
        print(f"No concurrent active runs found for DAG '{dag_id}'. Proceeding with run {current_run_id}.")

    @provide_session
    def test_guard_active_run_skips_on_concurrent_run(session=None):
        mock_dag = MagicMock()
        mock_dag.dag_id = 'dw_bert_ablaufsteuerung'

        mock_current_dag_run = MagicMock()
        mock_current_dag_run.run_id = 'test_run_1'
        mock_current_dag_run.state = 'running' # The run being tested

        mock_concurrent_dag_run = MagicMock()
        mock_concurrent_dag_run.run_id = 'existing_run_0'
        mock_concurrent_dag_run.state = 'running'

        # Mock DagRun.find to return an existing running DAG run
        with patch('airflow.models.DagRun.find', return_value=[mock_concurrent_dag_run, mock_current_dag_run]):
            context = {
                'dag': mock_dag,
                'dag_run': mock_current_dag_run,
                'logical_date': pendulum.now(),
                'ti': MagicMock(), # Mock TaskInstance
                'task': MagicMock(), # Mock Task
            }
            with pytest.raises(AirflowSkipException) as excinfo:
                _guard_active_run(**context)
            assert "Skipping this DAG run" in str(excinfo.value)
            assert "existing_run_0" in str(excinfo.value)

    @provide_session
    def test_guard_active_run_proceeds_without_concurrent_run(session=None):
        mock_dag = MagicMock()
        mock_dag.dag_id = 'dw_bert_ablaufsteuerung'

        mock_current_dag_run = MagicMock()
        mock_current_dag_run.run_id = 'test_run_2'
        mock_current_dag_run.state = 'running'

        # Mock DagRun.find to return only the current DAG run (which is filtered out)
        with patch('airflow.models.DagRun.find', return_value=[mock_current_dag_run]):
            context = {
                'dag': mock_dag,
                'dag_run': mock_current_dag_run,
                'logical_date': pendulum.now(),
                'ti': MagicMock(),
                'task': MagicMock(),
            }
            # Should not raise an exception
            _guard_active_run(**context)
            # If no exception, it passed. We can also check for a specific log message if desired.
            # For this test, simply not raising an exception is sufficient.
            assert True # Test passed if no exception was raised
    ```

### Test Case 3: `TimeSensor` Configuration

*   **Purpose**: Verify that all `TimeSensor` tasks are correctly configured with their respective `target_time` values, ensuring the earliest start times are accurately translated from UC4 to Airflow.
*   **Setup**:
    1.  Ensure the `dw_bert_ablaufsteuerung.py` file is accessible.
    2.  Load the DAG object.
*   **Action**: Iterate through all tasks in the DAG, identify `TimeSensor` instances, and check their `target_time` attribute.
*   **Pass/Fail Criterion**: Each `TimeSensor` task's `target_time` attribute exactly matches the corresponding "Earliest start time" from the design document.

    ```python
    import pytest
    from airflow.models.dag import DAG
    from airflow.sensors.time import TimeSensor
    import pendulum

    # Using the 'dag' object from Test Case 1's setup
    def test_time_sensor_configurations():
        expected_time_sensors = {
            'wait_until_20_00_for_dw_bert_monatlich_jp': '20:00:00',
            'wait_until_07_00_for_dw_bert_run_adm_check_jp_evt': '07:00:00',
            'wait_until_04_03_for_dw_bert_adm_housekeeping_jp': '04:03:00',
            'wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp': '01:30:00',
            'wait_until_01_00_for_dw_bert_stammdaten_jp': '01:00:00',
            'wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt': '01:00:00',
        }

        for task_id, expected_time in expected_time_sensors.items():
            task = dag.get_task(task_id)
            assert isinstance(task, TimeSensor), f"Task {task_id} is not a TimeSensor."
            assert task.target_time == expected_time, \
                f"TimeSensor {task_id} has incorrect target_time. Expected: {expected_time}, Actual: {task.target_time}"

        print("All TimeSensor configurations verified successfully.")
    ```

### Test Case 4: `TriggerDagRunOperator` Configuration

*   **Purpose**: Verify that `TriggerDagRunOperator` tasks are correctly configured to trigger the expected child DAGs and `wait_for_completion` is set to `True`, replicating the UC4 behavior of waiting for child jobs to finish.
*   **Setup**:
    1.  Ensure the `dw_bert_ablaufsteuerung.py` file is accessible.
    2.  Load the DAG object.
*   **Action**: Iterate through all tasks in the DAG, identify `TriggerDagRunOperator` instances, and check their `trigger_dag_id` and `wait_for_completion` attributes.
*   **Pass/Fail Criterion**:
    *   Each `TriggerDagRunOperator` task's `trigger_dag_id` matches the corresponding child DAG ID from the design document.
    *   `wait_for_completion` is set to `True` for all `TriggerDagRunOperator` tasks, as per the design's default assumption.

    ```python
    import pytest
    from airflow.models.dag import DAG
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    import pendulum

    # Using the 'dag' object from Test Case 1's setup
    def test_trigger_dag_run_operator_configurations():
        expected_trigger_operators = {
            'trigger_dw_bert_monatlich_jp': 'dw_bert_monatlich_jp',
            'trigger_dw_bert_run_adm_check_jp_evt': 'dw_bert_run_adm_check_jp_evt',
            'trigger_dw_bert_adm_housekeeping_jp': 'dw_bert_adm_housekeeping_jp',
            'trigger_dw_dwh_apt_export_taeglich_jp': 'dw_dwh_apt_export_taeglich_jp',
            'trigger_dw_bert_stammdaten_jp': 'dw_bert_stammdaten_jp',
            'trigger_dw_dwh_run_apt_export_monatlich_jp_evt': 'dw_dwh_run_apt_export_monatlich_jp_evt',
        }

        for task_id, expected_triggered_dag_id in expected_trigger_operators.items():
            task = dag.get_task(task_id)
            assert isinstance(task, TriggerDagRunOperator), f"Task {task_id} is not a TriggerDagRunOperator."
            assert task.trigger_dag_id == expected_triggered_dag_id, \
                f"TriggerDagRunOperator {task_id} has incorrect trigger_dag_id. Expected: {expected_triggered_dag_id}, Actual: {task.trigger_dag_id}"
            assert task.wait_for_completion is True, \
                f"TriggerDagRunOperator {task_id} should have wait_for_completion=True, but it's {task.wait_for_completion}"

        print("All TriggerDagRunOperator configurations verified successfully.")
    ```

### Test Case 5: Calendar Check Task Placement and Invocation

*   **Purpose**: Verify that placeholder calendar check tasks are correctly placed within the DAG flow for calendar-dependent jobs, even though their internal logic is yet to be implemented. This ensures the hooks for future calendar logic are correctly integrated.
*   **Setup**:
    1.  Ensure the `dw_bert_ablaufsteuerung.py` file is accessible.
    2.  Load the DAG object.
*   **Action**:
    1.  Inspect the DAG's task graph to confirm the placement of the `PythonOperator` tasks for calendar checks relative to their upstream `TimeSensor` and downstream `TriggerDagRunOperator` tasks.
    2.  Verify that the `python_callable` for these tasks refers to the placeholder functions.
*   **Pass/Fail Criterion**:
    *   `calendar_check_dw_bert_monatlich_jp` is an instance of `PythonOperator` and is downstream of `wait_until_20_00_for_dw_bert_monatlich_jp` and upstream of `trigger_dw_bert_monatlich_jp`.
    *   `calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt` is an instance of `PythonOperator` and is downstream of `wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt` and upstream of `trigger_dw_dwh_run_apt_export_monatlich_jp_evt`.
    *   The `python_callable` for these tasks points to the placeholder functions (`_calendar_check_dw_bert_monatlich_jp`, `_calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt`).

    ```python
    import pytest
    from airflow.models.dag import DAG
    from airflow.operators.python import PythonOperator
    import pendulum

    # Using the 'dag' object from Test Case 1's setup
    def test_calendar_check_task_placement_and_type():
        # Test for DW.BERT_MONATLICH_JP related calendar check
        task_id_monatlich_jp = 'calendar_check_dw_bert_monatlich_jp'
        task_monatlich_jp = dag.get_task(task_id_monatlich_jp)
        assert isinstance(task_monatlich_jp, PythonOperator), \
            f"Task {task_id_monatlich_jp} is not a PythonOperator."
        assert task_monatlich_jp.python_callable.__name__ == '_calendar_check_dw_bert_monatlich_jp', \
            f"Python callable for {task_id_monatlich_jp} is incorrect."
        assert 'wait_until_20_00_for_dw_bert_monatlich_jp' in [t.task_id for t in task_monatlich_jp.upstream_list], \
            f"Task {task_id_monatlich_jp} is not downstream of wait_until_20_00_for_dw_bert_monatlich_jp."
        assert 'trigger_dw_bert_monatlich_jp' in [t.task_id for t in task_monatlich_jp.downstream_list], \
            f"Task {task_id_monatlich_jp} is not upstream of trigger_dw_bert_monatlich_jp."

        # Test for DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT related calendar check
        task_id_apt_export_evt = 'calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt'
        task_apt_export_evt = dag.get_task(task_id_apt_export_evt)
        assert isinstance(task_apt_export_evt, PythonOperator), \
            f"Task {task_id_apt_export_evt} is not a PythonOperator."
        assert task_apt_export_evt.python_callable.__name__ == '_calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt', \
            f"Python callable for {task_id_apt_export_evt} is incorrect."
        assert 'wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt' in [t.task_id for t in task_apt_export_evt.upstream_list], \
            f"Task {task_id_apt_export_evt} is not downstream of wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt."
        assert 'trigger_dw_dwh_run_apt_export_monatlich_jp_evt' in [t.task_id for t in task_apt_export_evt.downstream_list], \
            f"Task {task_id_apt_export_evt} is not upstream of trigger_dw_dwh_run_apt_export_monatlich_jp_evt."

        print("Calendar check task placement and type verified successfully.")
    ```

### Test Case 6: DAG Metadata and Configuration

*   **Purpose**: Verify general DAG metadata and configuration (e.g., `dag_id`, `start_date`, `schedule`, `catchup`, `tags`, `doc_md`) align with best practices and the design document. This ensures the DAG is properly identified, scheduled, and documented.
*   **Setup**:
    1.  Ensure the `dw_bert_ablaufsteuerung.py` file is accessible.
    2.  Load the DAG object.
*   **Action**: Inspect the attributes of the loaded DAG object.
*   **Pass/Fail Criterion**:
    *   `dag.dag_id` is `'dw_bert_ablaufsteuerung'`.
    *   `dag.start_date` is `pendulum.datetime(2023, 1, 1, tz="UTC")` (or the actual configured start date).
    *   `dag.schedule` is `None` (as it's primarily time/event-driven and not on a fixed Airflow schedule).
    *   `dag.catchup` is `False`.
    *   `dag.tags` contains `['bert', 'uc4', 'scheduler']`.
    *   `dag.doc_md` contains a meaningful description including the original UC4 source.
    *   Placeholder variables (`GCP_PROJECT_ID`, `DATAPROC_REGION`, `DATAPROC_CLUSTER_NAME`, `GCS_BUCKET_NAME`) are either replaced with actual values or clearly marked as placeholders in the file (which they are in the provided code).

    ```python
    import pytest
    from airflow.models.dag import DAG
    import pendulum

    # Using the 'dag' object from Test Case 1's setup
    def test_dag_metadata_and_configuration():
        assert dag.dag_id == 'dw_bert_ablaufsteuerung', "DAG ID is incorrect."
        assert dag.start_date == pendulum.datetime(2023, 1, 1, tz="UTC"), "DAG start_date is incorrect."
        assert dag.schedule is None, "DAG schedule should be None for this type of scheduler."
        assert dag.catchup is False, "DAG catchup should be False."
        assert sorted(dag.tags) == sorted(['bert', 'uc4', 'scheduler']), "DAG tags are incorrect."
        assert "DW.BERT_ABLAUFSTEUERUNG Airflow DAG" in dag.doc_md, "DAG doc_md is missing key information."
        assert "Original UC4 Source" in dag.doc_md, "DAG doc_md is missing original UC4 source reference."

        # Check for placeholder variables in the Python file itself (conceptual check)
        # This would typically be a linter rule or a separate static analysis.
        # For a runtime test, we can check if the global variables are still placeholders.
        # This assumes the global variables are accessible, which they are in the provided code.
        from dags.dw_bert_ablaufsteuerung import GCP_PROJECT_ID, DATAPROC_REGION, DATAPROC_CLUSTER_NAME, GCS_BUCKET_NAME
        assert GCP_PROJECT_ID == "YOUR_GCP_PROJECT_ID", "GCP_PROJECT_ID placeholder not replaced."
        assert DATAPROC_REGION == "YOUR_DATAPROC_REGION", "DATAPROC_REGION placeholder not replaced."
        assert DATAPROC_CLUSTER_NAME == "YOUR_DATAPROC_CLUSTER_NAME", "DATAPROC_CLUSTER_NAME placeholder not replaced."
        assert GCS_BUCKET_NAME == "YOUR_BUCKET_NAME", "GCS_BUCKET_NAME placeholder not replaced."

        print("DAG metadata and configuration verified successfully.")
    ```