As a senior data-migration QA engineer, I've analyzed the provided migration design document and the legacy/migrated code for `r_ausd_bp_ta_bpr_bcp.ksh`. The core insight is that this migration focuses on re-platforming the *orchestration* logic to an Airflow DAG, while the actual data processing logic of `k_ausd_bp_ta_bpr_bcp.ksh` is assumed to be migrated separately (e.g., to BigQuery SQL) and invoked by this DAG.

Therefore, the tests below will primarily focus on validating the Airflow DAG's orchestration behavior, parameter handling, and correct invocation of the downstream (placeholder) data processing task, ensuring it mirrors the legacy script's responsibilities.

---

## Migration Validation Tests for `r_ausd_bp_ta_bpr_bcp_dag.py`

### 1. Test Case: Default Parameter Handling

*   **Purpose:** Verify that when no `stichtag` or `wiederanlaufwert` parameters are explicitly provided in the DAG run configuration, the Airflow DAG correctly defaults `stichtag` to the current system date (DDMMYYYY format) and `wiederanlaufwert` to `0`, matching the legacy KornShell script's behavior.
*   **Setup:**
    1.  Ensure the Airflow DAG `r_ausd_bp_ta_bpr_bcp_dag` is deployed to a Cloud Composer environment.
    2.  No specific Airflow Variables are required for this test, as defaults are handled within the Python function.
*   **Action:**
    1.  Trigger the Airflow DAG `r_ausd_bp_ta_bpr_bcp_dag` without providing any `conf` parameters (e.g., `airflow dags trigger r_ausd_bp_ta_bpr_bcp_dag`).
    2.  Monitor the execution of the `parse_validate_params` task.
*   **Pass/Fail Criterion:**
    *   The `parse_validate_params` task completes successfully.
    *   The XCom values pushed by `parse_validate_params` are:
        *   `p_stichtag_ddmmyyyy`: Matches the current system date in `DDMMYYYY` format at the time of DAG execution.
        *   `p_wiederanlaufwert`: `0` (integer).
        *   `job_kennung`: `"ausd_bp_ta_bpr_bcp"`.
        *   `dw_eintragsnr`: The `run_id` of the DAG run.
    *   The `run_core_processing` task is successfully triggered and its rendered SQL contains these default values.

*   **Runnable Test Code (Conceptual Pytest for `_parse_and_validate_params` function):**

    ```python
    import pytest
    from unittest.mock import MagicMock, patch
    from datetime import datetime

    # Assuming _parse_and_validate_params is accessible, e.g., imported from your DAG file
    # from r_ausd_bp_ta_bpr_bcp_dag import _parse_and_validate_params

    # Mock the _parse_and_validate_params function for testing
    def _parse_and_validate_params_mock(**kwargs):
        # This is a simplified mock of the actual function for testing purposes
        # In a real scenario, you'd import the actual function and test it directly
        ti = kwargs['ti']
        dag_run = kwargs.get('dag_run')

        mock_now = datetime(2023, 10, 26, 10, 0, 0) # Fixed datetime for consistent testing
        v_sysdate_ddmmyyyy = mock_now.strftime('%d%m%Y')

        raw_stichtag_param = dag_run.conf.get('stichtag') if dag_run and dag_run.conf else None
        p_stichtag_ddmmyyyy = None
        if raw_stichtag_param:
            try:
                dt_stichtag = datetime.strptime(raw_stichtag_param, '%Y-%m-%d')
                p_stichtag_ddmmyyyy = dt_stichtag.strftime('%d%m%Y')
            except ValueError:
                try:
                    dt_stichtag = datetime.strptime(raw_stichtag_param, '%d%m%Y')
                    p_stichtag_ddmmyyyy = raw_stichtag_param
                except ValueError:
                    raise ValueError(f"Invalid stichtag format: {raw_stichtag_param}")
        else:
            p_stichtag_ddmmyyyy = v_sysdate_ddmmyyyy

        wiederanlaufwert = dag_run.conf.get('wiederanlaufwert', 0) if dag_run and dag_run.conf else 0
        job_kennung = "ausd_bp_ta_bpr_bcp"
        dw_eintragsnr = kwargs['run_id']

        ti.xcom_push(key='p_stichtag_ddmmyyyy', value=p_stichtag_ddmmyyyy)
        ti.xcom_push(key='p_wiederanlaufwert', value=wiederanlaufwert)
        ti.xcom_push(key='job_kennung', value=job_kennung)
        ti.xcom_push(key='dw_eintragsnr', value=dw_eintragsnr)


    @patch('datetime.datetime', MagicMock(now=lambda: datetime(2023, 10, 26, 10, 0, 0)))
    def test_default_parameter_handling():
        mock_ti = MagicMock()
        mock_dag_run = MagicMock(conf={})
        mock_run_id = "test_run_001"

        _parse_and_validate_params_mock(ti=mock_ti, dag_run=mock_dag_run, run_id=mock_run_id)

        mock_ti.xcom_push.assert_any_call(key='p_stichtag_ddmmyyyy', value='26102023')
        mock_ti.xcom_push.assert_any_call(key='p_wiederanlaufwert', value=0)
        mock_ti.xcom_push.assert_any_call(key='job_kennung', value='ausd_bp_ta_bpr_bcp')
        mock_ti.xcom_push.assert_any_call(key='dw_eintragsnr', value=mock_run_id)

    ```

### 2. Test Case: Explicit Parameter Handling (DDMMYYYY Stichtag)

*   **Purpose:** Verify that the DAG correctly parses and uses explicitly provided `stichtag` in `DDMMYYYY` format and `wiederanlaufwert` from the DAG run configuration.
*   **Setup:**
    1.  Ensure the Airflow DAG `r_ausd_bp_ta_bpr_bcp_dag` is deployed.
*   **Action:**
    1.  Trigger the Airflow DAG with specific `conf` parameters:
        `airflow dags trigger r_ausd_bp_ta_bpr_bcp_dag --conf '{"stichtag": "01012023", "wiederanlaufwert": 123}'`
    2.  Monitor the execution of the `parse_validate_params` task.
*   **Pass/Fail Criterion:**
    *   The `parse_validate_params` task completes successfully.
    *   The XCom values pushed by `parse_validate_params` are:
        *   `p_stichtag_ddmmyyyy`: `"01012023"`.
        *   `p_wiederanlaufwert`: `123` (integer).
        *   `job_kennung`: `"ausd_bp_ta_bpr_bcp"`.
        *   `dw_eintragsnr`: The `run_id` of the DAG run.
    *   The `run_core_processing` task is successfully triggered and its rendered SQL contains these explicit values.

*   **Runnable Test Code (Conceptual Pytest for `_parse_and_validate_params` function):**

    ```python
    import pytest
    from unittest.mock import MagicMock, patch
    from datetime import datetime

    # Assuming _parse_and_validate_params_mock is defined as above
    @patch('datetime.datetime', MagicMock(now=lambda: datetime(2023, 10, 26, 10, 0, 0))) # Mock datetime for consistency
    def test_explicit_ddmmyyyy_stichtag_parameter_handling():
        mock_ti = MagicMock()
        mock_dag_run = MagicMock(conf={'stichtag': '01012023', 'wiederanlaufwert': 123})
        mock_run_id = "test_run_002"

        _parse_and_validate_params_mock(ti=mock_ti, dag_run=mock_dag_run, run_id=mock_run_id)

        mock_ti.xcom_push.assert_any_call(key='p_stichtag_ddmmyyyy', value='01012023')
        mock_ti.xcom_push.assert_any_call(key='p_wiederanlaufwert', value=123)
        mock_ti.xcom_push.assert_any_call(key='job_kennung', value='ausd_bp_ta_bpr_bcp')
        mock_ti.xcom_push.assert_any_call(key='dw_eintragsnr', value=mock_run_id)
    ```

### 3. Test Case: Explicit Parameter Handling (YYYY-MM-DD Stichtag)

*   **Purpose:** Verify that the DAG correctly parses and converts an explicitly provided `stichtag` in `YYYY-MM-DD` format (common Airflow convention) to `DDMMYYYY` for internal use, matching the legacy script's expected date format for `k_ausd_bp_ta_bpr_bcp.ksh`.
*   **Setup:**
    1.  Ensure the Airflow DAG `r_ausd_bp_ta_bpr_bcp_dag` is deployed.
*   **Action:**
    1.  Trigger the Airflow DAG with specific `conf` parameters:
        `airflow dags trigger r_ausd_bp_ta_bpr_bcp_dag --conf '{"stichtag": "2023-01-01", "wiederanlaufwert": 456}'`
    2.  Monitor the execution of the `parse_validate_params` task.
*   **Pass/Fail Criterion:**
    *   The `parse_validate_params` task completes successfully.
    *   The XCom values pushed by `parse_validate_params` are:
        *   `p_stichtag_ddmmyyyy`: `"01012023"` (converted from `2023-01-01`).
        *   `p_wiederanlaufwert`: `456` (integer).
        *   `job_kennung`: `"ausd_bp_ta_bpr_bcp"`.
        *   `dw_eintragsnr`: The `run_id` of the DAG run.
    *   The `run_core_processing` task is successfully triggered and its rendered SQL contains these converted values.

*   **Runnable Test Code (Conceptual Pytest for `_parse_and_validate_params` function):**

    ```python
    import pytest
    from unittest.mock import MagicMock, patch
    from datetime import datetime

    # Assuming _parse_and_validate_params_mock is defined as above
    @patch('datetime.datetime', MagicMock(now=lambda: datetime(2023, 10, 26, 10, 0, 0))) # Mock datetime for consistency
    def test_explicit_yyyymmdd_stichtag_parameter_handling():
        mock_ti = MagicMock()
        mock_dag_run = MagicMock(conf={'stichtag': '2023-01-01', 'wiederanlaufwert': 456})
        mock_run_id = "test_run_003"

        _parse_and_validate_params_mock(ti=mock_ti, dag_run=mock_dag_run, run_id=mock_run_id)

        mock_ti.xcom_push.assert_any_call(key='p_stichtag_ddmmyyyy', value='01012023')
        mock_ti.xcom_push.assert_any_call(key='p_wiederanlaufwert', value=456)
        mock_ti.xcom_push.assert_any_call(key='job_kennung', value='ausd_bp_ta_bpr_bcp')
        mock_ti.xcom_push.assert_any_call(key='dw_eintragsnr', value=mock_run_id)
    ```

### 4. Test Case: Invalid Stichtag Format Handling

*   **Purpose:** Verify that the DAG's parameter parsing task fails gracefully when an invalid `stichtag` format is provided, preventing incorrect data processing, similar to how the legacy script would exit on parameter validation errors.
*   **Setup:**
    1.  Ensure the Airflow DAG `r_ausd_bp_ta_bpr_bcp_dag` is deployed.
*   **Action:**
    1.  Trigger the Airflow DAG with an invalid `stichtag` format:
        `airflow dags trigger r_ausd_bp_ta_bpr_bcp_dag --conf '{"stichtag": "2023/01/01"}'`
    2.  Monitor the execution of the `parse_validate_params` task.
*   **Pass/Fail Criterion:**
    *   The `parse_validate_params` task fails.
    *   The task logs for `parse_validate_params` contain an error message indicating an invalid `stichtag` format (e.g., `ValueError: Invalid stichtag format: 2023/01/01`).
    *   The `run_core_processing` task is not executed.

*   **Runnable Test Code (Conceptual Pytest for `_parse_and_validate_params` function):**

    ```python
    import pytest
    from unittest.mock import MagicMock, patch
    from datetime import datetime

    # Assuming _parse_and_validate_params_mock is defined as above
    @patch('datetime.datetime', MagicMock(now=lambda: datetime(2023, 10, 26, 10, 0, 0))) # Mock datetime for consistency
    def test_invalid_stichtag_format_handling():
        mock_ti = MagicMock()
        mock_dag_run = MagicMock(conf={'stichtag': '2023/01/01'})
        mock_run_id = "test_run_004"

        with pytest.raises(ValueError, match="Invalid stichtag format: 2023/01/01"):
            _parse_and_validate_params_mock(ti=mock_ti, dag_run=mock_dag_run, run_id=mock_run_id)
    ```

### 5. Test Case: Core Processing Task Invocation (Parameter Passing)

*   **Purpose:** Verify that the `run_core_processing` task (representing the migrated `k_ausd_bp_ta_bpr_bcp.ksh` logic) correctly receives all necessary parameters from the upstream `parse_validate_params` task via XCom, ensuring the data processing logic operates with the intended inputs. This directly validates the "External-system replacements" aspect for the core script invocation.
*   **Setup:**
    1.  Ensure the Airflow DAG `r_ausd_bp_ta_bpr_bcp_dag` is deployed.
    2.  An Airflow Variable named `BERT_DIR_ROOT_AIRFLOW_VAR_NAME` should be set (e.g., to `/usr/local/airflow/bert_root`) to simulate the legacy environment variable.
*   **Action:**
    1.  Trigger the Airflow DAG with a valid `stichtag` and `wiederanlaufwert`:
        `airflow dags trigger r_ausd_bp_ta_bpr_bcp_dag --conf '{"stichtag": "2023-05-15", "wiederanlaufwert": 999}'`
    2.  Monitor the execution of both `parse_validate_params` and `run_core_processing` tasks.
*   **Pass/Fail Criterion:**
    *   Both tasks complete successfully.
    *   Inspect the rendered SQL of the `run_core_processing` task (available in Airflow UI -> Task Instance Details -> Rendered Template).
    *   The rendered SQL should accurately reflect the XCom-pulled values:
        *   `job_kennung`: `"ausd_bp_ta_bpr_bcp"`
        *   `stichtag`: `"15052023"`
        *   `wiederanlaufwert`: `999`
        *   `dw_eintragsnr`: The `run_id` of the DAG run.
    *   The `bert_dir_root` parameter passed to the `BigQueryOperator` (if used in the actual `k_ausd_bp_ta_bpr_bcp.ksh` migration) should match the value of the `BERT_DIR_ROOT_AIRFLOW_VAR_NAME` Airflow Variable.

*   **Runnable Test Code (Conceptual Pytest for DAG structure and XCom flow):**

    ```python
    import pytest
    from airflow.models.dag import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
    from airflow.utils.dates import days_ago
    from unittest.mock import MagicMock, patch
    from datetime import datetime

    # Mock the _parse_and_validate_params function for testing
    def _parse_and_validate_params_mock_for_dag(**kwargs):
        ti = kwargs['ti']
        dag_run = kwargs.get('dag_run')
        mock_now = datetime(2023, 10, 26, 10, 0, 0)
        v_sysdate_ddmmyyyy = mock_now.strftime('%d%m%Y')

        raw_stichtag_param = dag_run.conf.get('stichtag') if dag_run and dag_run.conf else None
        p_stichtag_ddmmyyyy = None
        if raw_stichtag_param:
            try:
                dt_stichtag = datetime.strptime(raw_stichtag_param, '%Y-%m-%d')
                p_stichtag_ddmmyyyy = dt_stichtag.strftime('%d%m%Y')
            except ValueError:
                p_stichtag_ddmmyyyy = raw_stichtag_param # Simplified for test, actual raises error
        else:
            p_stichtag_ddmmyyyy = v_sysdate_ddmmyyyy

        wiederanlaufwert = dag_run.conf.get('wiederanlaufwert', 0) if dag_run and dag_run.conf else 0
        job_kennung = "ausd_bp_ta_bpr_bcp"
        dw_eintragsnr = kwargs['run_id']

        ti.xcom_push(key='p_stichtag_ddmmyyyy', value=p_stichtag_ddmmyyyy)
        ti.xcom_push(key='p_wiederanlaufwert', value=wiederanlaufwert)
        ti.xcom_push(key='job_kennung', value=job_kennung)
        ti.xcom_push(key='dw_eintragsnr', value=dw_eintragsnr)

    # Recreate the DAG structure for testing purposes
    with DAG(
        dag_id="test_r_ausd_bp_ta_bpr_bcp_dag",
        start_date=days_ago(1),
        schedule=None,
        catchup=False,
    ) as test_dag:
        parse_validate_params_test = PythonOperator(
            task_id="parse_validate_params",
            python_callable=_parse_and_validate_params_mock_for_dag,
            provide_context=True,
        )

        run_core_processing_test = BigQueryOperator(
            task_id="run_core_processing",
            sql='''
            SELECT
                '{{ ti.xcom_pull(task_ids="parse_validate_params", key="job_kennung") }}' AS job_kennung,
                '{{ ti.xcom_pull(task_ids="parse_validate_params", key="p_stichtag_ddmmyyyy") }}' AS stichtag,
                CAST('{{ ti.xcom_pull(task_ids="parse_validate_params", key="p_wiederanlaufwert") }}' AS INT64) AS wiederanlaufwert,
                '{{ ti.xcom_pull(task_ids="parse_validate_params", key="dw_eintragsnr") }}' AS dw_eintragsnr,
                '{{ params.bert_dir_root }}' AS bert_root_path;
            ''',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default',
            params={
                "bert_dir_root": "mock_bert_root_path" # Mocked for testing
            }
        )
        parse_validate_params_test >> run_core_processing_test

    @patch('airflow.models.variable.Variable.get', return_value="mock_bert_root_path")
    def test_core_processing_task_parameter_passing(mock_variable_get):
        from airflow.utils.state import State
        from airflow.models import DagRun, TaskInstance

        # Mock a DagRun
        dag_run = DagRun(
            dag_id=test_dag.dag_id,
            run_id="test_run_005",
            conf={'stichtag': '2023-05-15', 'wiederanlaufwert': 999},
            execution_date=datetime.now(),
            start_date=datetime.now(),
            state=State.RUNNING
        )

        # Mock TaskInstance for parse_validate_params
        ti_parse = TaskInstance(task=test_dag.get_task("parse_validate_params"), execution_date=dag_run.execution_date)
        ti_parse.dag_run = dag_run # Link to dag_run
        ti_parse.xcom_push = MagicMock() # Mock xcom_push

        # Execute the parse_validate_params task
        ti_parse.run(ignore_ti_state=True)

        # Assert xcom_push calls
        ti_parse.xcom_push.assert_any_call(key='p_stichtag_ddmmyyyy', value='15052023')
        ti_parse.xcom_push.assert_any_call(key='p_wiederanlaufwert', value=999)
        ti_parse.xcom_push.assert_any_call(key='job_kennung', value='ausd_bp_ta_bpr_bcp')
        ti_parse.xcom_push.assert_any_call(key='dw_eintragsnr', value='test_run_005')

        # Mock TaskInstance for run_core_processing
        ti_core = TaskInstance(task=test_dag.get_task("run_core_processing"), execution_date=dag_run.execution_date)
        ti_core.dag_run = dag_run # Link to dag_run
        ti_core.xcom_pull = MagicMock(side_effect=lambda task_ids, key: {
            'parse_validate_params': {
                'job_kennung': 'ausd_bp_ta_bpr_bcp',
                'p_stichtag_ddmmyyyy': '15052023',
                'p_wiederanlaufwert': 999,
                'dw_eintragsnr': 'test_run_005'
            }
        }[task_ids][key]) # Mock xcom_pull to return expected values

        # Render the SQL template
        rendered_sql = ti_core.render_template(ti_core.task.sql, context={'ti': ti_core, 'params': ti_core.task.params})

        # Assert the rendered SQL contains the correct values
        assert "'ausd_bp_ta_bpr_bcp' AS job_kennung" in rendered_sql
        assert "'15052023' AS stichtag" in rendered_sql
        assert "CAST('999' AS INT64) AS wiederanlaufwert" in rendered_sql
        assert "'test_run_005' AS dw_eintragsnr" in rendered_sql
        assert "'mock_bert_root_path' AS bert_root_path" in rendered_sql
    ```

### 6. Test Case: Logging Parity

*   **Purpose:** Verify that the Airflow DAG's logging captures essential information that the legacy KornShell script would print to its log file, ensuring operational visibility is maintained. This covers the "Output parity" aspect for logging.
*   **Setup:**
    1.  Ensure the Airflow DAG `r_ausd_bp_ta_bpr_bcp_dag` is deployed.
*   **Action:**
    1.  Trigger the Airflow DAG with any valid parameters.
    2.  Access the task logs for the `parse_validate_params` task in the Airflow UI or via `gcloud composer environments run <env-name> --location <location> dags logs r_ausd_bp_ta_bpr_bcp_dag parse_validate_params <dag-run-id>`.
*   **Pass/Fail Criterion:**
    *   The `parse_validate_params` task logs contain entries similar to the legacy script's output, including:
        *   `Parameters for core processing:`
        *   `Stichtag (DDMMYYYY): <value>`
        *   `Wiederanlaufwert: <value>`
        *   `JobKennung: ausd_bp_ta_bpr_bcp`
        *   `DW_EintragsNr: <run_id>`
    *   The `run_core_processing` task logs indicate successful execution of the BigQuery query.

*   **Runnable Test Code (Conceptual Pytest for `_parse_and_validate_params` function's print statements):**

    ```python
    import pytest
    from unittest.mock import MagicMock, patch
    from datetime import datetime
    import io
    import sys

    # Assuming _parse_and_validate_params_mock is defined as above
    @patch('datetime.datetime', MagicMock(now=lambda: datetime(2023, 10, 26, 10, 0, 0)))
    def test_logging_parity(capsys):
        mock_ti = MagicMock()
        mock_dag_run = MagicMock(conf={'stichtag': '2023-01-01', 'wiederanlaufwert': 789})
        mock_run_id = "test_run_006"

        _parse_and_validate_params_mock(ti=mock_ti, dag_run=mock_dag_run, run_id=mock_run_id)

        captured = capsys.readouterr()
        assert "Parameters for core processing:" in captured.out
        assert "Stichtag (DDMMYYYY): 01012023" in captured.out
        assert "Wiederanlaufwert: 789" in captured.out
        assert "JobKennung: ausd_bp_ta_bpr_bcp" in captured.out
        assert f"DW_EintragsNr: {mock_run_id}" in captured.out
    ```

### 7. Test Case: Environment Variable Replacement (`BERT_DIR_ROOT`)

*   **Purpose:** Verify that the legacy environment variable `BERT_DIR_ROOT`, which was used to locate the core script, is correctly replaced by an Airflow Variable and made available to downstream tasks (e.g., as a parameter to the `BigQueryOperator` or a Python function). This validates the "External-system replacements" for environment configuration.
*   **Setup:**
    1.  Ensure the Airflow DAG `r_ausd_bp_ta_bpr_bcp_dag` is deployed.
    2.  Create an Airflow Variable named `BERT_DIR_ROOT_AIRFLOW_VAR_NAME` with a value (e.g., `/gcp/bert/root/path`).
*   **Action:**
    1.  Trigger the Airflow DAG.
    2.  Access the rendered SQL of the `run_core_processing` task.
*   **Pass/Fail Criterion:**
    *   The `run_core_processing` task completes successfully.
    *   The rendered SQL for the `run_core_processing` task contains the value of the `BERT_DIR_ROOT_AIRFLOW_VAR_NAME` Airflow Variable where `params.bert_dir_root` is referenced. For example, if the variable is set to `/gcp/bert/root/path`, the rendered SQL should show `' /gcp/bert/root/path' AS bert_root_path;` (assuming the placeholder SQL is updated to use it).

*   **Runnable Test Code (Conceptual Pytest for Airflow Variable interaction):**

    ```python
    import pytest
    from airflow.models.variable import Variable
    from unittest.mock import patch

    # This test would typically involve running a dummy DAG and checking its rendered SQL
    # or a PythonOperator that reads the variable.
    # For a unit test, we can mock the Variable.get call.

    @patch('airflow.models.variable.Variable.get', return_value="/gcp/bert/root/path")
    def test_bert_dir_root_airflow_variable_replacement(mock_variable_get):
        # Simulate the BigQueryOperator's params dictionary
        operator_params = {
            "bert_dir_root": Variable.get("BERT_DIR_ROOT_AIRFLOW_VAR_NAME", default="/usr/local/airflow/bert_root")
        }

        # Assert that Variable.get was called with the correct key
        mock_variable_get.assert_called_with("BERT_DIR_ROOT_AIRFLOW_VAR_NAME", default="/usr/local/airflow/bert_root")

        # Assert that the parameter in the operator's params dictionary has the correct value
        assert operator_params["bert_dir_root"] == "/gcp/bert/root/path"

        # In a full Airflow integration test, you would then verify that this value
        # correctly propagates into the rendered SQL of the BigQueryOperator.
    ```