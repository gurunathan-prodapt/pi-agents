As a senior data-migration QA engineer, I've designed a suite of validation tests to ensure the migrated `r_ausd_v_ta_cntrct_crs2.ksh` job to Google Cloud Platform (GCP) is behaviourally equivalent to its legacy KornShell counterpart. The focus is on the wrapper script's operational logic, logging, parameter handling, and error management, as the core business logic (`k_ausd_v_ta_cntrct_crs2.ksh`) is migrated separately.

**Assumptions for Testing:**
*   GCP project ID: `my-gcp-project`
*   BigQuery dataset ID: `my_dataset`
*   All audit tables (`job_control`, `job_log`, `job_error_log`) and stored procedures (`Vertragsdatenabgleich`, `k_ausd_v_ta_cntrct_crs2`) are deployed to `my-gcp-project.my_dataset`.
*   For error testing, the `k_ausd_v_ta_cntrct_crs2` stored procedure has been temporarily modified to accept a `p_force_error BOOL` parameter, allowing it to simulate a failure. Similarly, the `Vertragsdatenabgleich` stored procedure accepts `p_force_core_error BOOL` to pass this down.
*   Each test case assumes a clean state for the audit tables (e.g., truncated) before execution.

---

## Migration Validation Tests for `r_ausd_v_ta_cntrct_crs2.ksh`

### Test Case 1: Help Message Display

*   **Purpose:** Verify that calling the job with the help flag (`-h` in legacy, `p_help => TRUE` in migrated) displays the usage information and exits without performing any job processing or logging.
*   **Setup:**
    1.  Ensure audit tables (`job_control`, `job_log`, `job_error_log`) are empty.
    2.  Deploy the `Vertragsdatenabgleich` BigQuery Stored Procedure.
*   **Action:**
    *   **Legacy:** Execute the KornShell script: `r_ausd_v_ta_cntrct_crs2.ksh -h`
    *   **Migrated:** Execute the BigQuery Stored Procedure:
        ```sql
        CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => TRUE, p_s => NULL, p_l => NULL, p_force_core_error => FALSE);
        ```
*   **Pass/Fail Criterion:**
    *   **Legacy:** The script's standard output contains the `usage` message, and the script exits with status `0`. No log file should be created or modified.
    *   **Migrated:**
        1.  The BigQuery query result should contain the usage information (program name, version, description, parameters).
        2.  A query against `my-gcp-project.my_dataset.job_control` should return 0 rows.
        3.  A query against `my-gcp-project.my_dataset.job_log` should return 0 rows.
        4.  A query against `my-gcp-project.my_dataset.job_error_log` should return 0 rows.
    *   **Pytest Assertion (Conceptual):**
        ```python
        def test_help_message_display(bigquery_client):
            # Action: Call the BQ SP with p_help=TRUE
            query_job = bigquery_client.query("""
                CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => TRUE, p_s => NULL, p_l => NULL, p_force_core_error => FALSE);
            """)
            results = list(query_job.result())

            # Assert 1: Check usage message in results
            assert len(results) == 1
            assert results[0].program_name == 'Programm: Vertragsdatenabgleich'
            assert 'Beschreibung: Rahmenskript' in results[0].description

            # Assert 2-4: Check audit tables are empty
            assert bigquery_client.query("SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_control`").result().total_rows == 0
            assert bigquery_client.query("SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_log`").result().total_rows == 0
            assert bigquery_client.query("SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_error_log`").result().total_rows == 0
        ```

### Test Case 2: Successful Job Execution

*   **Purpose:** Verify that the job runs successfully, correctly logs its lifecycle, updates the job status, and invokes the core processing logic.
*   **Setup:**
    1.  Ensure audit tables are empty.
    2.  Deploy `Vertragsdatenabgleich` and `k_ausd_v_ta_cntrct_crs2` (with `p_force_error => FALSE` as default or explicitly passed).
*   **Action:**
    *   **Legacy:** Execute the KornShell script: `r_ausd_v_ta_cntrct_crs2.ksh -s "test_s" -l "test_l"`
    *   **Migrated:** Execute the BigQuery Stored Procedure (via Airflow DAG or direct call):
        ```sql
        CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => FALSE, p_s => 'test_s', p_l => 'test_l', p_force_core_error => FALSE);
        ```
*   **Pass/Fail Criterion:**
    *   **Legacy:**
        1.  The script exits with status `0`.
        2.  A log file (e.g., `BERT_V_TA_CNTRCT_CRS2_YYYYMMDD_XXXX.log`) is created.
        3.  The log file contains entries for job start, `StichtagInfo`, core script invocation, and success message.
        4.  The `DWMSG_SetzeStatusOK` call is executed.
    *   **Migrated:**
        1.  The BigQuery SP call completes successfully without error.
        2.  **`job_control` table:**
            *   One row exists for `job_key = 'BERT_V_TA_CNTRCT_CRS2'`.
            *   `status` is 'OK'.
            *   `start_timestamp` and `end_timestamp` are populated.
            *   `parameters` JSON contains `p_s_param: 'test_s'` and `p_l_param: 'test_l'`.
            *   `sysdate_info` matches `CURRENT_DATE()` of execution.
        3.  **`job_log` table:**
            *   At least 3 'INFO' entries exist for the `entry_number` from `job_control`:
                *   One for job start (from `Vertragsdatenabgleich`).
                *   One for core process start (from `k_ausd_v_ta_cntrct_crs2`).
                *   One for core process completion (from `k_ausd_v_ta_cntrct_crs2`).
                *   One for overall success (from `Vertragsdatenabgleich`).
            *   Messages reflect successful execution.
        4.  **`job_error_log` table:** No rows exist for this job execution.
    *   **Pytest Assertion (Conceptual):**
        ```python
        import datetime
        def test_successful_job_execution(bigquery_client):
            # Action: Call the BQ SP
            bigquery_client.query("""
                CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => FALSE, p_s => 'test_s', p_l => 'test_l', p_force_core_error => FALSE);
            """).result()

            # Assert 1: Check job_control
            job_control_rows = list(bigquery_client.query("SELECT * FROM `my-gcp-project.my_dataset.job_control` WHERE job_key = 'BERT_V_TA_CNTRCT_CRS2'").result())
            assert len(job_control_rows) == 1
            control_entry = job_control_rows[0]
            assert control_entry.status == 'OK'
            assert control_entry.start_timestamp is not None
            assert control_entry.end_timestamp is not None
            assert control_entry.parameters == {'p_s_param': 'test_s', 'p_l_param': 'test_l'}
            assert control_entry.sysdate_info == datetime.date.today()

            # Assert 2: Check job_log
            job_log_rows = list(bigquery_client.query(f"SELECT message, log_level FROM `my-gcp-project.my_dataset.job_log` WHERE entry_number = {control_entry.entry_number} ORDER BY log_timestamp").result())
            assert len(job_log_rows) >= 4 # Wrapper start, core start, core end, wrapper end
            assert all(row.log_level == 'INFO' for row in job_log_rows)
            assert "Job start for 'BERT_V_TA_CNTRCT_CRS2'" in job_log_rows[0].message
            assert "k_ausd_v_ta_cntrct_crs2: Starting core reconciliation process." in [r.message for r in job_log_rows]
            assert "k_ausd_v_ta_cntrct_crs2: Core reconciliation process completed." in [r.message for r in job_log_rows]
            assert "The processing finished without detectable errors" in job_log_rows[-1].message

            # Assert 3: Check job_error_log
            assert bigquery_client.query(f"SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_error_log` WHERE entry_number = {control_entry.entry_number}").result().total_rows == 0
        ```

### Test Case 3: Core Logic Failure Handling

*   **Purpose:** Verify that if the core processing logic (`k_ausd_v_ta_cntrct_crs2`) fails, the wrapper correctly traps the error, logs it to the error table, updates the job status to `ERROR`, and re-raises the error.
*   **Setup:**
    1.  Ensure audit tables are empty.
    2.  Deploy `Vertragsdatenabgleich` and `k_ausd_v_ta_cntrct_crs2` (modified to accept `p_force_error`).
*   **Action:**
    *   **Legacy:** Simulate a failure in `k_ausd_v_ta_cntrct_crs2.ksh` (e.g., by adding `exit 1` inside it) and execute: `r_ausd_v_ta_cntrct_crs2.ksh`
    *   **Migrated:** Execute the BigQuery Stored Procedure, forcing an error in the core logic:
        ```sql
        -- This call is expected to fail and raise an error
        CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => FALSE, p_s => 'error_s', p_l => 'error_l', p_force_core_error => TRUE);
        ```
*   **Pass/Fail Criterion:**
    *   **Legacy:**
        1.  The script exits with a non-zero status (e.g., `1`).
        2.  The log file contains an error message from `DWMSG_Fehlerbehandlung` and "AppError: Abbruch".
    *   **Migrated:**
        1.  The BigQuery SP call should fail and raise an error.
        2.  **`job_control` table:**
            *   One row exists for `job_key = 'BERT_V_TA_CNTRCT_CRS2'`.
            *   `status` is 'ERROR'.
            *   `start_timestamp` and `end_timestamp` are populated.
        3.  **`job_log` table:**
            *   At least 2 entries: initial 'INFO' from wrapper, and an 'ERROR' entry from wrapper.
            *   An 'INFO' entry from `k_ausd_v_ta_cntrct_crs2` for starting.
            *   No 'INFO' entry for `k_ausd_v_ta_cntrct_crs2` completion.
        4.  **`job_error_log` table:**
            *   One row exists for the `entry_number` from `job_control`.
            *   `error_message` contains the simulated error message (e.g., "Simulated error in k_ausd_v_ta_cntrct_crs2 for testing purposes.").
            *   `error_code` and `stack_trace` are populated.
    *   **Pytest Assertion (Conceptual):**
        ```python
        import pytest
        def test_core_logic_failure_handling(bigquery_client):
            # Action: Call the BQ SP with p_force_core_error=TRUE, expecting an error
            with pytest.raises(Exception) as excinfo:
                bigquery_client.query("""
                    CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => FALSE, p_s => 'error_s', p_l => 'error_l', p_force_core_error => TRUE);
                """).result()
            assert "Simulated error in k_ausd_v_ta_cntrct_crs2" in str(excinfo.value)

            # Assert 1: Check job_control
            job_control_rows = list(bigquery_client.query("SELECT * FROM `my-gcp-project.my_dataset.job_control` WHERE job_key = 'BERT_V_TA_CNTRCT_CRS2'").result())
            assert len(job_control_rows) == 1
            control_entry = job_control_rows[0]
            assert control_entry.status == 'ERROR'
            assert control_entry.start_timestamp is not None
            assert control_entry.end_timestamp is not None

            # Assert 2: Check job_log
            job_log_rows = list(bigquery_client.query(f"SELECT message, log_level FROM `my-gcp-project.my_dataset.job_log` WHERE entry_number = {control_entry.entry_number} ORDER BY log_timestamp").result())
            assert len(job_log_rows) >= 3 # Wrapper start, core start, wrapper error
            assert any(row.log_level == 'ERROR' for row in job_log_rows)
            assert "An error occurred for job 'BERT_V_TA_CNTRCT_CRS2'" in [r.message for r in job_log_rows]
            assert "k_ausd_v_ta_cntrct_crs2: Starting core reconciliation process." in [r.message for r in job_log_rows]
            assert not any("k_ausd_v_ta_cntrct_crs2: Core reconciliation process completed." in r.message for r in job_log_rows)

            # Assert 3: Check job_error_log
            job_error_rows = list(bigquery_client.query(f"SELECT error_message FROM `my-gcp-project.my_dataset.job_error_log` WHERE entry_number = {control_entry.entry_number}").result())
            assert len(job_error_rows) == 1
            assert "Simulated error in k_ausd_v_ta_cntrct_crs2" in job_error_rows[0].error_message
        ```

### Test Case 4: Parameter Passing to Core Logic

*   **Purpose:** Verify that the `JobKennung` and `DW_EintragsNr` (migrated as `v_job_key` and `v_entry_number`) are correctly generated and passed from the wrapper to the core processing stored procedure.
*   **Setup:**
    1.  Ensure audit tables are empty.
    2.  Deploy `Vertragsdatenabgleich` and `k_ausd_v_ta_cntrct_crs2`.
*   **Action:**
    *   **Legacy:** Execute `r_ausd_v_ta_cntrct_crs2.ksh`. Inspect the log file for the command line used to call `k_ausd_v_ta_cntrct_crs2.ksh`.
    *   **Migrated:** Execute the BigQuery Stored Procedure:
        ```sql
        CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => FALSE, p_s => 'param_check', p_l => 'param_check', p_force_core_error => FALSE);
        ```
*   **Pass/Fail Criterion:**
    *   **Legacy:** The log file should contain a line similar to `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh -j BERT_V_TA_CNTRCT_CRS2 -f <entry_number>`.
    *   **Migrated:**
        1.  Retrieve the `entry_number` from the `job_control` table for the latest run.
        2.  Query the `job_log` table for messages from `k_ausd_v_ta_cntrct_crs2` for that `entry_number`.
        3.  The log messages from `k_ausd_v_ta_cntrct_crs2` (e.g., "Starting core reconciliation process.") should correctly reference `job_key = 'BERT_V_TA_CNTRCT_CRS2'` and the `entry_number` generated by the wrapper. This confirms the parameters were passed correctly.
    *   **Pytest Assertion (Conceptual):**
        ```python
        def test_parameter_passing_to_core_logic(bigquery_client):
            # Action: Call the BQ SP
            bigquery_job = bigquery_client.query("""
                CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => FALSE, p_s => 'param_check', p_l => 'param_check', p_force_core_error => FALSE);
            """)
            bigquery_job.result()

            # Get the entry_number from job_control
            job_control_rows = list(bigquery_client.query("SELECT entry_number FROM `my-gcp-project.my_dataset.job_control` WHERE job_key = 'BERT_V_TA_CNTRCT_CRS2' ORDER BY start_timestamp DESC LIMIT 1").result())
            assert len(job_control_rows) == 1
            entry_number = job_control_rows[0].entry_number

            # Assert that k_ausd_v_ta_cntrct_crs2 logged with the correct parameters
            job_log_rows = list(bigquery_client.query(f"""
                SELECT message FROM `my-gcp-project.my_dataset.job_log`
                WHERE job_key = 'BERT_V_TA_CNTRCT_CRS2' AND entry_number = {entry_number}
                AND message LIKE 'k_ausd_v_ta_cntrct_crs2: Starting core reconciliation process.'
            """).result())
            assert len(job_log_rows) == 1
            # The message itself implicitly confirms the parameters were received by k_ausd_v_ta_cntrct_crs2
            # For a more explicit test, k_ausd_v_ta_cntrct_crs2 could log the received parameters directly.
        ```

### Test Case 5: Date Handling (`sysdate_info`)

*   **Purpose:** Verify that the job correctly captures and stores the execution date.
*   **Setup:**
    1.  Ensure audit tables are empty.
    2.  Deploy `Vertragsdatenabgleich`.
*   **Action:**
    *   **Legacy:** Execute `r_ausd_v_ta_cntrct_crs2.ksh`. Inspect the log file for the `DWMSG_SetzeStichtagInfo` entry.
    *   **Migrated:** Execute the BigQuery Stored Procedure:
        ```sql
        CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => FALSE, p_s => 'date_test', p_l => 'date_test', p_force_core_error => FALSE);
        ```
*   **Pass/Fail Criterion:**
    *   **Legacy:** The log file should contain a line indicating `StichtagInfo` was set to the current date in `DDMMYYYY` format.
    *   **Migrated:**
        1.  Query the `job_control` table for the latest run.
        2.  The `sysdate_info` column should contain the current date (as a `DATE` type) when the SP was executed.
    *   **Pytest Assertion (Conceptual):**
        ```python
        import datetime
        def test_date_handling(bigquery_client):
            # Action: Call the BQ SP
            bigquery_client.query("""
                CALL `my-gcp-project.my_dataset.Vertragsdatenabgleich`(p_help => FALSE, p_s => 'date_test', p_l => 'date_test', p_force_core_error => FALSE);
            """).result()

            # Get the sysdate_info from job_control
            job_control_rows = list(bigquery_client.query("SELECT sysdate_info FROM `my-gcp-project.my_dataset.job_control` WHERE job_key = 'BERT_V_TA_CNTRCT_CRS2' ORDER BY start_timestamp DESC LIMIT 1").result())
            assert len(job_control_rows) == 1
            assert job_control_rows[0].sysdate_info == datetime.date.today()
        ```

### Test Case 6: Airflow DAG Orchestration (End-to-End Success)

*   **Purpose:** Verify that the Cloud Composer DAG can successfully trigger the BigQuery Stored Procedure, leading to a complete and successful job execution. This tests the external system replacement for orchestration.
*   **Setup:**
    1.  Ensure audit tables are empty.
    2.  Deploy the `vertraegsdatenabgleich_dag.py` to Cloud Composer.
    3.  Ensure `Vertragsdatenabgleich` and `k_ausd_v_ta_cntrct_crs2` are deployed and configured for success.
*   **Action:**
    *   **Legacy:** Not applicable (this tests the new orchestration).
    *   **Migrated:** Trigger the `vertraegsdatenabgleich_daily_job` DAG in Cloud Composer (e.g., via Airflow UI or `airflow dags trigger`).
*   **Pass/Fail Criterion:**
    *   **Migrated:**
        1.  The Airflow DAG run completes successfully (green status).
        2.  The `call_vertraegsdatenabgleich_sp` task within the DAG completes successfully.
        3.  **`job_control` table:** One row exists for `job_key = 'BERT_V_TA_CNTRCT_CRS2'` with `status = 'OK'`.
        4.  **`job_log` table:** Contains all expected 'INFO' messages for a successful run.
        5.  **`job_error_log` table:** No rows exist for this job execution.
    *   **Pytest Assertion (Conceptual - requires Airflow testing framework):**
        ```python
        # This would typically involve Airflow's testing utilities or integration tests
        # that interact with a running Airflow instance.
        from airflow.models.dagrun import DagRun
        from airflow.utils.state import State
        from airflow.utils import timezone

        def test_airflow_dag_success(airflow_client, bigquery_client):
            # Action: Trigger the DAG
            execution_date = timezone.utcnow()
            dag_id = "vertraegsdatenabgleich_daily_job"
            dag_run = airflow_client.trigger_dag(dag_id=dag_id, execution_date=execution_date)

            # Wait for DAG to complete (simplified, in real test use polling)
            # For actual testing, you'd poll dag_run.get_state() until it's finished
            # For this example, assume it completes quickly.
            # time.sleep(60) # Example wait

            # Assert 1-2: Check Airflow DAG status
            dag_run.refresh_from_db()
            assert dag_run.state == State.SUCCESS
            task_instance = dag_run.get_task_instance(task_id="call_vertraegsdatenabgleich_sp")
            assert task_instance.state == State.SUCCESS

            # Assert 3-5: Check BigQuery audit tables (similar to Test Case 2)
            job_control_rows = list(bigquery_client.query("SELECT * FROM `my-gcp-project.my_dataset.job_control` WHERE job_key = 'BERT_V_TA_CNTRCT_CRS2' ORDER BY start_timestamp DESC LIMIT 1").result())
            assert len(job_control_rows) == 1
            assert job_control_rows[0].status == 'OK'
            assert bigquery_client.query(f"SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_log` WHERE entry_number = {job_control_rows[0].entry_number} AND log_level = 'INFO'").result().total_rows >= 4
            assert bigquery_client.query(f"SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_error_log` WHERE entry_number = {job_control_rows[0].entry_number}").result().total_rows == 0
        ```

### Test Case 7: Airflow DAG Orchestration (End-to-End Failure)

*   **Purpose:** Verify that the Cloud Composer DAG correctly handles a failure originating from the BigQuery Stored Procedure, marking the DAG run as failed.
*   **Setup:**
    1.  Ensure audit tables are empty.
    2.  Deploy the `vertraegsdatenabgleich_dag.py` to Cloud Composer.
    3.  Modify the Airflow DAG to pass `p_force_core_error => TRUE` to the `Vertragsdatenabgleich` SP.
*   **Action:**
    *   **Legacy:** Not applicable.
    *   **Migrated:** Trigger the `vertraegsdatenabgleich_daily_job` DAG in Cloud Composer.
*   **Pass/Fail Criterion:**
    *   **Migrated:**
        1.  The Airflow DAG run completes with a failed status (red status).
        2.  The `call_vertraegsdatenabgleich_sp` task within the DAG fails.
        3.  **`job_control` table:** One row exists for `job_key = 'BERT_V_TA_CNTRCT_CRS2'` with `status = 'ERROR'`.
        4.  **`job_log` table:** Contains 'ERROR' messages related to the failure.
        5.  **`job_error_log` table:** Contains an entry detailing the error.
    *   **Pytest Assertion (Conceptual - requires Airflow testing framework):**
        ```python
        from airflow.models.dagrun import DagRun
        from airflow.utils.state import State
        from airflow.utils import timezone

        def test_airflow_dag_failure(airflow_client, bigquery_client):
            # Action: Trigger the DAG (assuming DAG is modified to pass p_force_core_error=TRUE)
            execution_date = timezone.utcnow()
            dag_id = "vertraegsdatenabgleich_daily_job"
            # In a real test, you'd dynamically modify the DAG or use a test-specific DAG version
            # For this example, we assume the DAG's BigQueryInsertJobOperator is configured to pass p_force_core_error => TRUE
            dag_run = airflow_client.trigger_dag(dag_id=dag_id, execution_date=execution_date)

            # Wait for DAG to complete
            # time.sleep(60) # Example wait

            # Assert 1-2: Check Airflow DAG status
            dag_run.refresh_from_db()
            assert dag_run.state == State.FAILED
            task_instance = dag_run.get_task_instance(task_id="call_vertraegsdatenabgleich_sp")
            assert task_instance.state == State.FAILED

            # Assert 3-5: Check BigQuery audit tables (similar to Test Case 3)
            job_control_rows = list(bigquery_client.query("SELECT * FROM `my-gcp-project.my_dataset.job_control` WHERE job_key = 'BERT_V_TA_CNTRCT_CRS2' ORDER BY start_timestamp DESC LIMIT 1").result())
            assert len(job_control_rows) == 1
            assert job_control_rows[0].status == 'ERROR'
            assert bigquery_client.query(f"SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_log` WHERE entry_number = {job_control_rows[0].entry_number} AND log_level = 'ERROR'").result().total_rows >= 1
            assert bigquery_client.query(f"SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_error_log` WHERE entry_number = {job_control_rows[0].entry_number}").result().total_rows == 1
        ```