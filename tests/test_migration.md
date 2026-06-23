As a senior data-migration QA engineer, I have analyzed the migration design for `r_ausd_v_ta_cntrct_crs2.ksh` to a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). The original script is an orchestrator, handling environment setup, parameter parsing, logging, and invoking a core processing script. The migrated BigQuery Stored Procedure (`sp_vertragsdatenabgleich`) replicates this orchestration logic, using BigQuery tables for logging and configuration, and calling a migrated core BigQuery Stored Procedure (`sp_k_ausd_v_ta_cntrct_crs2`).

The following test cases are designed to validate the behavioral equivalence of the migrated code, covering output parity, transformation correctness (control flow, parameter handling), external system replacements (logging, config), and data quality assertions for the logging tables.

---

**Pre-requisites for all tests:**

1.  **BigQuery Environment**: A GCP project and BigQuery dataset must be available.
2.  **DDL Deployment**: The DDL for `job_execution_log`, `job_error_log`, and `config_job_control` tables must be deployed to the target BigQuery dataset.
3.  **Stored Procedure Deployment**: The `sp_k_ausd_v_ta_cntrct_crs2` (placeholder) and `sp_vertragsdatenabgleich` stored procedures must be deployed.
4.  **Configuration Data**: The `config_job_control` table must contain an entry for the job being tested.

    ```sql
    -- Replace YOUR_PROJECT_ID and YOUR_DATASET_ID with actual values
    MERGE INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.config_job_control` T
    USING (SELECT
        'TA_CNTRCT_CRS2' AS job_kennung,
        'r_ausd_v_ta_cntrct_crs2' AS program_name,
        'sp_k_ausd_v_ta_cntrct_crs2' AS kernel_script_name,
        'Contract reconciliation wrapper job' AS description
    ) S
    ON T.job_kennung = S.job_kennung
    WHEN MATCHED THEN
        UPDATE SET
            program_name = S.program_name,
            kernel_script_name = S.kernel_script_name,
            description = S.description,
            updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, program_name, kernel_script_name, description)
        VALUES (S.job_kennung, S.program_name, S.kernel_script_name, S.description);
    ```

5.  **Pytest Setup**: The following Python setup code (using `pytest` and `google-cloud-bigquery` library) can be used to run the tests. Remember to replace placeholders like `YOUR_PROJECT_ID` and `YOUR_DATASET_ID`.

    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import date, datetime, timezone
    import time
    import json

    # --- Configuration ---
    # !!! REPLACE THESE PLACEHOLDERS WITH YOUR ACTUAL GCP PROJECT AND DATASET IDs !!!
    PROJECT_ID = "your-gcp-project-id"
    DATASET_ID = "your_dataset_id"

    JOB_EXEC_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_execution_log"
    JOB_ERROR_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_error_log"
    CONFIG_JOB_CONTROL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.config_job_control"
    SP_VERTRAGSDATENABGLEICH = f"{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich"
    SP_K_AUSD_V_TA_CNTRCT_CRS2 = f"{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_cntrct_crs2"

    CLIENT = bigquery.Client(project=PROJECT_ID)

    # --- Helper Functions ---
    def execute_bq_query(query):
        query_job = CLIENT.query(query)
        return query_job.result()

    def call_sp(sp_name, **kwargs):
        params = []
        for k, v in kwargs.items():
            if isinstance(v, str):
                params.append(f"{k} => '{v}'")
            elif isinstance(v, bool):
                params.append(f"{k} => {str(v).upper()}")
            elif v is None:
                params.append(f"{k} => NULL")
            else:
                params.append(f"{k} => {v}")
        param_str = ", ".join(params)
        query = f"CALL {sp_name}({param_str});"
        print(f"Executing: {query}")
        try:
            results = execute_bq_query(query)
            # Capture SELECT outputs from the procedure
            return [dict(row) for row in results]
        except Exception as e:
            print(f"Error calling SP: {e}")
            raise

    def get_log_entries(table_name, job_id=None, entry_number=None):
        query = f"SELECT * FROM `{table_name}` WHERE 1=1"
        if job_id:
            query += f" AND job_id = '{job_id}'"
        if entry_number:
            query += f" AND entry_number = {entry_number}"
        query += " ORDER BY created_at ASC"
        return list(execute_bq_query(query))

    def clear_log_tables(job_id=None):
        if job_id:
            execute_bq_query(f"DELETE FROM `{JOB_EXEC_LOG_TABLE}` WHERE job_id = '{job_id}'").result()
            execute_bq_query(f"DELETE FROM `{JOB_ERROR_LOG_TABLE}` WHERE job_id = '{job_id}'").result()
        else: # Clear all for setup
            execute_bq_query(f"TRUNCATE TABLE `{JOB_EXEC_LOG_TABLE}`").result()
            execute_bq_query(f"TRUNCATE TABLE `{JOB_ERROR_LOG_TABLE}`").result()

    def setup_config_job_control_entry(job_kennung, program_name, kernel_script_name):
        query = f"""
            MERGE INTO `{CONFIG_JOB_CONTROL_TABLE}` T
            USING (SELECT
                '{job_kennung}' AS job_kennung,
                '{program_name}' AS program_name,
                '{kernel_script_name}' AS kernel_script_name,
                'Test job entry' AS description
            ) S
            ON T.job_kennung = S.job_kennung
            WHEN MATCHED THEN
                UPDATE SET
                    program_name = S.program_name,
                    kernel_script_name = S.kernel_script_name,
                    description = S.description,
                    updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (job_kennung, program_name, kernel_script_name, description)
                VALUES (S.job_kennung, S.program_name, S.kernel_script_name, S.description);
        """
        execute_bq_query(query)

    def set_core_sp_behavior(should_fail=False, error_message="Simulated core logic error"):
        if should_fail:
            sp_body = f"""
                BEGIN
                    RAISE SCRIPT_EXCEPTION('{error_message}');
                END;
            """
        else:
            sp_body = f"""
                BEGIN
                    SELECT FORMAT('INFO: Core procedure sp_k_ausd_v_ta_cntrct_crs2 called for job %s, execution %d. Parameters s=%s, l=%s',
                                  p_job_kennung, p_job_execution_nr, p_s, p_l) AS core_procedure_message;
                END;
            """
        execute_bq_query(f"""
            CREATE OR REPLACE PROCEDURE {SP_K_AUSD_V_TA_CNTRCT_CRS2}(
                p_job_kennung STRING, p_job_execution_nr INT64, p_s STRING, p_l STRING
            )
            {sp_body}
        """)

    # --- Pytest Fixtures ---
    @pytest.fixture(scope="module", autouse=True)
    def setup_module():
        # Ensure tables and SPs exist (DDL and SP creation should be part of deployment)
        # For testing, we'll assume they are already deployed.
        # Populate config_job_control for the test job
        setup_config_job_control_entry('TA_CNTRCT_CRS2', 'r_ausd_v_ta_cntrct_crs2', 'sp_k_ausd_v_ta_cntrct_crs2')
        yield

    @pytest.fixture(autouse=True)
    def setup_each_test():
        # Clear logs before each test to ensure isolation
        clear_log_tables('TA_CNTRCT_CRS2')
        clear_log_tables('UNKNOWN_JOB') # For specific error cases
        clear_log_tables('NON_EXISTENT_JOB') # For specific error cases
        # Reset sp_k_ausd_v_ta_cntrct_crs2 to default (no error)
        set_core_sp_behavior(should_fail=False)
        yield
    ```

---

## Test Case 1: Successful Execution - Happy Path

*   **Purpose**: Verify the migrated stored procedure executes successfully with valid parameters, logs correctly, and calls the core procedure. This covers output parity (logging), external system replacement (logging tables, core SP call), and basic transformation correctness (control flow).
*   **Setup**:
    *   Ensure `config_job_control` has an entry for `TA_CNTRCT_CRS2` pointing to `sp_k_ausd_v_ta_cntrct_crs2`.
    *   Ensure `sp_k_ausd_v_ta_cntrct_crs2` is configured to succeed (default behavior).
    *   Clear `job_execution_log` and `job_error_log` for `job_id = 'TA_CNTRCT_CRS2'`.
*   **Action**: Call `sp_vertragsdatenabgleich` with valid `p_job_kennung`, `p_s`, and `p_l` parameters.

    ```python
    def test_successful_execution():
        job_kennung = 'TA_CNTRCT_CRS2'
        p_s_val = 'test_s_param'
        p_l_val = 'test_l_param'

        # Action
        console_output = call_sp(SP_VERTRAGSDATENABGLEICH, p_job_kennung=job_kennung, p_s=p_s_val, p_l=p_l_val)

        # Pass/Fail Criteria
        # 1. No exception raised (handled by pytest not failing the test)
        # 2. job_execution_log entries
        exec_logs = get_log_entries(JOB_EXEC_LOG_TABLE, job_id=job_kennung)
        assert len(exec_logs) == 2, "Expected two job_execution_log entries (STARTED and OK)"

        started_log = next((log for log in exec_logs if log['status'] == 'STARTED'), None)
        ok_log = next((log for log in exec_logs if log['status'] == 'OK'), None)

        assert started_log is not None, "Expected a 'STARTED' log entry"
        assert ok_log is not None, "Expected an 'OK' log entry"
        assert started_log['entry_number'] == ok_log['entry_number'], "Entry numbers should match for STARTED and OK"
        assert started_log['job_id'] == job_kennung
        assert ok_log['job_id'] == job_kennung
        assert ok_log['end_timestamp'] is not None
        assert ok_log['message'] == 'Job execution completed successfully.'
        assert ok_log['stichtag'] == date.today()

        # Verify parameters_json
        params_json = json.loads(started_log['parameters_json'])
        assert params_json['p_s'] == p_s_val
        assert params_json['p_l'] == p_l_val
        assert params_json['p_h'] is False

        # 3. job_error_log is empty
        error_logs = get_log_entries(JOB_ERROR_LOG_TABLE, job_id=job_kennung)
        assert len(error_logs) == 0, "Expected no job_error_log entries"

        # 4. Console output parity (simulated by SELECT statements)
        output_messages = [list(row.values())[0] for row in console_output] # Extract message from single-column SELECTs
        assert any(f"Job {job_kennung} started at" in msg for msg in output_messages)
        assert any(f"Program Name: r_ausd_v_ta_cntrct_crs2" in msg for msg in output_messages)
        assert any(f"Stichtag: {date.today().strftime('%Y-%m-%d')}" in msg for msg in output_messages)
        assert any(f"Log Entry Number: {started_log['entry_number']}" in msg for msg in output_messages)
        assert any(f"INFO: Core procedure sp_k_ausd_v_ta_cntrct_crs2 called for job {job_kennung}" in msg for msg in output_messages)
        assert any("Job execution completed successfully." in msg for msg in output_messages)
    ```

## Test Case 2: Help Message Display (`p_h` parameter)

*   **Purpose**: Verify the procedure correctly displays the usage message when `p_h` is true and exits without further processing or logging. This covers output parity and transformation correctness (parameter handling, control flow).
*   **Setup**: Clear `job_execution_log` and `job_error_log` for any `job_id`.
*   **Action**: Call `sp_vertragsdatenabgleich` with `p_h => TRUE`.

    ```python
    def test_help_message_display():
        # Action
        console_output = call_sp(SP_VERTRAGSDATENABGLEICH, p_h=True)

        # Pass/Fail Criteria
        # 1. No exception raised
        # 2. Console output contains usage information
        output_messages = [list(row.values())[0] for row in console_output]
        assert any("Usage: CALL" in msg for msg in output_messages)
        assert any("p_job_kennung: Identifier for the job" in msg for msg in output_messages)
        assert any("p_h: Display this help message" in msg for msg in output_messages)

        # 3. No log entries are created
        exec_logs = get_log_entries(JOB_EXEC_LOG_TABLE)
        error_logs = get_log_entries(JOB_ERROR_LOG_TABLE)
        assert len(exec_logs) == 0, "Expected no job_execution_log entries"
        assert len(error_logs) == 0, "Expected no job_error_log entries"
    ```

## Test Case 3: Missing Mandatory Parameter - `p_job_kennung`

*   **Purpose**: Verify the procedure handles a missing `p_job_kennung` by logging an error and raising an exception, mimicking the original script's exit behavior. This covers transformation correctness (parameter validation, error handling) and external system replacement (error logging).
*   **Setup**: Clear `job_execution_log` and `job_error_log` for any `job_id`.
*   **Action**: Call `sp_vertragsdatenabgleich` with `p_job_kennung => NULL` (or empty string), and valid `p_s`, `p_l`.

    ```python
    def test_missing_p_job_kennung():
        job_kennung = 'UNKNOWN_JOB' # This is the job_id used in error log for this specific error

        # Action & Pass/Fail Criteria (Exception)
        with pytest.raises(Exception) as excinfo:
            call_sp(SP_VERTRAGSDATENABGLEICH, p_job_kennung=None, p_s='s_val', p_l='l_val')
        assert "Parameter p_job_kennung is mandatory and cannot be empty." in str(excinfo.value)

        # 1. job_error_log entry
        error_logs = get_log_entries(JOB_ERROR_LOG_TABLE, job_id=job_kennung)
        assert len(error_logs) == 1, "Expected one job_error_log entry"
        assert error_logs[0]['job_id'] == job_kennung
        assert error_logs[0]['error_code'] == 1
        assert "Parameter p_job_kennung is mandatory" in error_logs[0]['error_message']
        assert error_logs[0]['entry_number'] is None # No entry_number generated yet

        # 2. job_execution_log is empty
        exec_logs = get_log_entries(JOB_EXEC_LOG_TABLE)
        assert len(exec_logs) == 0, "Expected no job_execution_log entries"
    ```

## Test Case 4: Invalid Job Kennung (Not in `config_job_control`)

*   **Purpose**: Verify the procedure handles an unknown `p_job_kennung` by logging an error and raising an exception. This covers transformation correctness (configuration lookup, error handling) and external system replacement (error logging).
*   **Setup**: Clear `job_execution_log` and `job_error_log` for `job_id = 'NON_EXISTENT_JOB'`.
*   **Action**: Call `sp_vertragsdatenabgleich` with `p_job_kennung => 'NON_EXISTENT_JOB'`, and valid `p_s`, `p_l`.

    ```python
    def test_invalid_job_kennung():
        job_kennung = 'NON_EXISTENT_JOB'

        # Action & Pass/Fail Criteria (Exception)
        with pytest.raises(Exception) as excinfo:
            call_sp(SP_VERTRAGSDATENABGLEICH, p_job_kennung=job_kennung, p_s='s_val', p_l='l_val')
        assert f"Job Kennung not found in `config_job_control`: {job_kennung}" in str(excinfo.value)

        # 1. job_error_log entry
        error_logs = get_log_entries(JOB_ERROR_LOG_TABLE, job_id=job_kennung)
        assert len(error_logs) == 1, "Expected one job_error_log entry"
        assert error_logs[0]['job_id'] == job_kennung
        assert error_logs[0]['error_code'] == 2
        assert f"Job Kennung not found in `config_job_control`: {job_kennung}" in error_logs[0]['error_message']
        assert error_logs[0]['entry_number'] is None # No entry_number generated yet

        # 2. job_execution_log is empty
        exec_logs = get_log_entries(JOB_EXEC_LOG_TABLE)
        assert len(exec_logs) == 0, "Expected no job_execution_log entries"
    ```

## Test Case 5: Missing Mandatory Parameter - `p_s`

*   **Purpose**: Verify the procedure handles a missing `p_s` parameter by logging an error and raising an exception. This covers transformation correctness (parameter validation, error handling) and external system replacement (error logging).
*   **Setup**: Clear `job_execution_log` and `job_error_log` for `job_id = 'TA_CNTRCT_CRS2'`.
*   **Action**: Call `sp_vertragsdatenabgleich` with valid `p_job_kennung`, `p_s => NULL`, and valid `p_l`.

    ```python
    def test_missing_p_s_parameter():
        job_kennung = 'TA_CNTRCT_CRS2'

        # Action & Pass/Fail Criteria (Exception)
        with pytest.raises(Exception) as excinfo:
            call_sp(SP_VERTRAGSDATENABGLEICH, p_job_kennung=job_kennung, p_s=None, p_l='l_val')
        assert "Parameter -s is mandatory." in str(excinfo.value)

        # 1. job_error_log entry
        error_logs = get_log_entries(JOB_ERROR_LOG_TABLE, job_id=job_kennung)
        assert len(error_logs) == 1, "Expected one job_error_log entry"
        assert error_logs[0]['job_id'] == job_kennung
        assert error_logs[0]['error_code'] == 3
        assert "Parameter -s is mandatory." in error_logs[0]['error_message']
        assert error_logs[0]['entry_number'] is not None # Entry number should be generated before this check

        # 2. job_execution_log entries (STARTED and FAILED)
        exec_logs = get_log_entries(JOB_EXEC_LOG_TABLE, job_id=job_kennung)
        assert len(exec_logs) == 2, "Expected two job_execution_log entries (STARTED and FAILED)"
        started_log = next((log for log in exec_logs if log['status'] == 'STARTED'), None)
        failed_log = next((log for log in exec_logs if log['status'] == 'FAILED'), None)
        assert started_log is not None
        assert failed_log is not None
        assert started_log['entry_number'] == failed_log['entry_number'] == error_logs[0]['entry_number']
        assert failed_log['message'] == 'ERROR: Job execution failed: Parameter -s is mandatory.'
    ```

## Test Case 6: Missing Mandatory Parameter - `p_l`

*   **Purpose**: Verify the procedure handles a missing `p_l` parameter by logging an error and raising an exception. This covers transformation correctness (parameter validation, error handling) and external system replacement (error logging).
*   **Setup**: Clear `job_execution_log` and `job_error_log` for `job_id = 'TA_CNTRCT_CRS2'`.
*   **Action**: Call `sp_vertragsdatenabgleich` with valid `p_job_kennung`, valid `p_s`, and `p_l => NULL`.

    ```python
    def test_missing_p_l_parameter():
        job_kennung = 'TA_CNTRCT_CRS2'

        # Action & Pass/Fail Criteria (Exception)
        with pytest.raises(Exception) as excinfo:
            call_sp(SP_VERTRAGSDATENABGLEICH, p_job_kennung=job_kennung, p_s='s_val', p_l=None)
        assert "Parameter -l is mandatory." in str(excinfo.value)

        # 1. job_error_log entry
        error_logs = get_log_entries(JOB_ERROR_LOG_TABLE, job_id=job_kennung)
        assert len(error_logs) == 1, "Expected one job_error_log entry"
        assert error_logs[0]['job_id'] == job_kennung
        assert error_logs[0]['error_code'] == 4
        assert "Parameter -l is mandatory." in error_logs[0]['error_message']
        assert error_logs[0]['entry_number'] is not None

        # 2. job_execution_log entries (STARTED and FAILED)
        exec_logs = get_log_entries(JOB_EXEC_LOG_TABLE, job_id=job_kennung)
        assert len(exec_logs) == 2, "Expected two job_execution_log entries (STARTED and FAILED)"
        started_log = next((log for log in exec_logs if log['status'] == 'STARTED'), None)
        failed_log = next((log for log in exec_logs if log['status'] == 'FAILED'), None)
        assert started_log is not None
        assert failed_log is not None
        assert started_log['entry_number'] == failed_log['entry_number'] == error_logs[0]['entry_number']
        assert failed_log['message'] == 'ERROR: Job execution failed: Parameter -l is mandatory.'
    ```

## Test Case 7: Core Script Failure Simulation

*   **Purpose**: Verify the wrapper procedure correctly handles an error raised by the invoked core procedure (`sp_k_ausd_v_ta_cntrct_crs2`), logging the failure and propagating the exception. This covers transformation correctness (error handling, control flow) and external system replacement (error logging, core SP invocation).
*   **Setup**:
    *   Modify `sp_k_ausd_v_ta_cntrct_crs2` to `RAISE SCRIPT_EXCEPTION('Simulated core logic error');`.
    *   Ensure `config_job_control` has the `TA_CNTRCT_CRS2` entry.
    *   Clear `job_execution_log` and `job_error_log` for `job_id = 'TA_CNTRCT_CRS2'`.
*   **Action**: Call `sp_vertragsdatenabgleich` with valid `p_job_kennung`, `p_s`, `p_l`.

    ```python
    def test_core_script_failure():
        job_kennung = 'TA_CNTRCT_CRS2'
        error_msg = 'Simulated core logic error'
        set_core_sp_behavior(should_fail=True, error_message=error_msg)

        # Action & Pass/Fail Criteria (Exception)
        with pytest.raises(Exception) as excinfo:
            call_sp(SP_VERTRAGSDATENABGLEICH, p_job_kennung=job_kennung, p_s='s_val', p_l='l_val')
        assert error_msg in str(excinfo.value)

        # 1. job_execution_log entries (STARTED and FAILED)
        exec_logs = get_log_entries(JOB_EXEC_LOG_TABLE, job_id=job_kennung)
        assert len(exec_logs) == 2, "Expected two job_execution_log entries (STARTED and FAILED)"
        started_log = next((log for log in exec_logs if log['status'] == 'STARTED'), None)
        failed_log = next((log for log in exec_logs if log['status'] == 'FAILED'), None)
        assert started_log is not None
        assert failed_log is not None
        assert started_log['entry_number'] == failed_log['entry_number']
        assert failed_log['status'] == 'FAILED'
        assert error_msg in failed_log['message']

        # 2. job_error_log entry
        error_logs = get_log_entries(JOB_ERROR_LOG_TABLE, job_id=job_kennung)
        assert len(error_logs) == 1, "Expected one job_error_log entry"
        assert error_logs[0]['job_id'] == job_kennung
        assert error_logs[0]['entry_number'] == started_log['entry_number']
        assert error_logs[0]['error_code'] is not None # BigQuery provides an error code for RAISE
        assert error_msg in error_logs[0]['error_message']
    ```

## Test Case 8: Data Quality - `entry_number` Uniqueness

*   **Purpose**: Verify that `entry_number` is unique for each job execution, ensuring proper tracking of individual runs. This covers data quality assertions.
*   **Setup**: Clear `job_execution_log` for `job_id = 'TA_CNTRCT_CRS2'`.
*   **Action**: Call `sp_vertragsdatenabgleich` successfully multiple times (e.g., 3 times) with the same `p_job_kennung` but potentially different `p_s`/`p_l` values.

    ```python
    def test_entry_number_uniqueness():
        job_kennung = 'TA_CNTRCT_CRS2'
        num_runs = 3
        entry_numbers = set()

        # Action
        for i in range(num_runs):
            call_sp(SP_VERTRAGSDATENABGLEICH, p_job_kennung=job_kennung, p_s=f's_val_{i}', p_l=f'l_val_{i}')
            # Give a small delay to ensure timestamps for entry_number generation are distinct enough if relying on them
            time.sleep(0.1)

        # Pass/Fail Criteria
        exec_logs = get_log_entries(JOB_EXEC_LOG_TABLE, job_id=job_kennung)
        assert len(exec_logs) == num_runs * 2, f"Expected {num_runs * 2} job_execution_log entries"

        for log in exec_logs:
            entry_numbers.add(log['entry_number'])

        assert len(entry_numbers) == num_runs, "Expected unique entry_number for each job run"
    ```