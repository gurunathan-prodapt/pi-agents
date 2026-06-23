As a senior data-migration QA engineer, I've analyzed the provided KornShell script `r_ausd_v_ta_vvl_dwh.ksh` and its BigQuery migration design. The original script is an orchestration wrapper, meaning its primary function is to manage execution flow, parameters, logging, and error handling, rather than performing direct data transformations. Therefore, the migration validation tests will focus on replicating these orchestration behaviors in the BigQuery environment.

The tests below are designed to prove behavioral equivalence, covering output parity (logging, status), transformation correctness (parameter handling, date formatting), external system replacements (BigQuery Stored Procedures for `DWMSG_*` functions), and data quality/schema assertions for the new `job_log` table.

For runnable code, I will use `test_project.test_dataset` as placeholders for your actual BigQuery project and dataset names. The `FORMAT_BQM_TEXT` function in the generated code is assumed to behave like BigQuery's standard `FORMAT` function for string formatting.

---

## Migration Validation Tests for `r_ausd_v_ta_vvl_dwh.ksh`

### Test Case 1: Happy Path - Default Execution

*   **Purpose:** Verify the BigQuery orchestration script executes successfully without any command-line parameters, correctly initializes logging, calls the core processing stored procedure, and marks the job as successful. This test validates the basic end-to-end flow.
*   **Setup:**
    *   Ensure the `test_project.test_dataset.job_log` table exists and is empty before execution.
    *   Ensure all `DWMSG_*_SP` and `k_ausd_v_ta_vvl_dwh_sp` are deployed and accessible within `test_project.test_dataset`.
    *   The `k_ausd_v_ta_vvl_dwh_sp` should be implemented to complete successfully without errors for this test.
*   **Action:** Execute the `r_ausd_v_ta_vvl_dwh_bq.sql` script in BigQuery without passing any explicit parameters.
    ```sql
    -- Execute the main orchestration script
    EXECUTE SCRIPT `test_project.test_dataset.r_ausd_v_ta_vvl_dwh_bq.sql`();
    ```
*   **Pass/Fail Criterion:**
    *   The BigQuery script execution completes without reporting any errors.
    *   Query the `test_project.test_dataset.job_log` table. It must contain at least 6 entries for the executed job (initial entry, reference date set, script start, core script start, core script end, job success).
    *   The `status` column for the `job_nr` associated with this execution must ultimately be 'SUCCESS'.
    *   All expected log entries (`job_kennung`, `script_name`, `log_level`, `log_message`, `log_ts`, `reference_date`) must be populated correctly.
    *   The `log_message` for the reference date entry should reflect the current date.

    ```python
    # Pytest assertion example (requires google-cloud-bigquery client)
    import pytest
    from google.cloud import bigquery
    from datetime import datetime
    import re

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client()

    def test_happy_path_default_execution(bq_client):
        project_id = "test_project"
        dataset_id = "test_dataset"
        job_log_table = f"`{project_id}.{dataset_id}.job_log`"
        script_path = f"`{project_id}.{dataset_id}.r_ausd_v_ta_vvl_dwh_bq.sql`"

        # Clear job_log before test
        bq_client.query(f"TRUNCATE TABLE {job_log_table}").result()

        # Execute the script
        bq_client.query(f"EXECUTE SCRIPT {script_path}()").result()

        # Assertions
        results = bq_client.query(f"""
            SELECT job_nr, job_kennung, script_name, log_level, log_message, status, reference_date, log_ts
            FROM {job_log_table}
            ORDER BY log_ts
        """).result()
        logs = list(results)

        assert len(logs) >= 6, f"Expected at least 6 log entries, but found {len(logs)}."

        # Check for initial entry
        initial_entry = next((l for l in logs if "Job started" in l.log_message), None)
        assert initial_entry is not None, "Initial 'Job started' log entry not found."
        assert initial_entry.status == 'RUNNING'

        # Check for reference date
        ref_date_entry = next((l for l in logs if "Reference date set" in l.log_message), None)
        assert ref_date_entry is not None, "Reference date log entry not found."
        assert ref_date_entry.reference_date is not None
        # Assuming current date is used for v_sysdate
        assert str(ref_date_entry.reference_date) == datetime.now().strftime('%Y-%m-%d')

        # Check for core script start/end
        core_start = next((l for l in logs if "Starting core processing" in l.log_message), None)
        assert core_start is not None, "Core script 'Starting' log entry not found."
        core_end = next((l for l in logs if "Finished core processing" in l.log_message), None)
        assert core_end is not None, "Core script 'Finished' log entry not found."

        # Check for final success status
        final_success_entry = next((l for l in logs if l.log_message == 'Job completed successfully.'), None)
        assert final_success_entry is not None, "Final 'Job completed successfully' log entry not found."
        assert final_success_entry.log_level == 'INFO'

        # Verify the overall job status is SUCCESS
        job_nr = logs[0].job_nr
        final_job_status_query = bq_client.query(f"""
            SELECT status FROM {job_log_table}
            WHERE job_nr = {job_nr} AND status = 'SUCCESS'
            ORDER BY log_ts DESC LIMIT 1
        """).result()
        final_job_status_df = final_job_status_query.to_dataframe()
        assert not final_job_status_df.empty, "Final job status 'SUCCESS' not found."
        assert final_job_status_df['status'].iloc[0] == 'SUCCESS'
    ```

### Test Case 2: Parameter Handling - `-s` and `-l`

*   **Purpose:** Verify the BigQuery orchestration script correctly accepts and logs the generic parameters `-s` and `-l`, mirroring the `getopts` behavior of the legacy script.
*   **Setup:**
    *   Ensure the `test_project.test_dataset.job_log` table exists and is empty.
    *   All `DWMSG_*_SP` and `k_ausd_v_ta_vvl_dwh_sp` are deployed and accessible, with `k_ausd_v_ta_vvl_dwh_sp` completing successfully.
*   **Action:** Execute the `r_ausd_v_ta_vvl_dwh_bq.sql` script with parameters `p_s` and `p_l`.
    ```sql
    -- Execute the main orchestration script with parameters
    EXECUTE SCRIPT `test_project.test_dataset.r_ausd_v_ta_vvl_dwh_bq.sql`(
        p_s => 'test_s_value',
        p_l => 'test_l_value'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The script completes without error.
    *   Query `test_project.test_dataset.job_log` and verify that it contains entries with `log_message` indicating the values of `p_s` and `p_l` were logged (e.g., "Parameter -s: test_s_value").
    *   The `status` column for the `job_nr` in `job_log` must ultimately be 'SUCCESS'.

    ```python
    # Pytest assertion example
    def test_parameter_handling(bq_client):
        project_id = "test_project"
        dataset_id = "test_dataset"
        job_log_table = f"`{project_id}.{dataset_id}.job_log`"
        script_path = f"`{project_id}.{dataset_id}.r_ausd_v_ta_vvl_dwh_bq.sql`"

        bq_client.query(f"TRUNCATE TABLE {job_log_table}").result()

        # Execute with parameters
        bq_client.query(f"""
            EXECUTE SCRIPT {script_path}(
                p_s => 'test_s_value',
                p_l => 'test_l_value'
            )
        """).result()

        results = bq_client.query(f"""
            SELECT log_message, status
            FROM {job_log_table}
            WHERE log_message LIKE 'Parameter -s:%' OR log_message LIKE 'Parameter -l:%'
            ORDER BY log_ts
        """).result()
        logs = list(results)

        assert any("Parameter -s: test_s_value" in l.log_message for l in logs), "Parameter -s value not logged."
        assert any("Parameter -l: test_l_value" in l.log_message for l in logs), "Parameter -l value not logged."

        # Verify final status
        job_nr_query = bq_client.query(f"SELECT job_nr FROM {job_log_table} LIMIT 1").result()
        job_nr = job_nr_query.to_dataframe()['job_nr'].iloc[0]
        final_job_status_query = bq_client.query(f"""
            SELECT status FROM {job_log_table}
            WHERE job_nr = {job_nr} AND status = 'SUCCESS'
            ORDER BY log_ts DESC LIMIT 1
        """).result()
        final_job_status_df = final_job_status_query.to_dataframe()
        assert not final_job_status_df.empty, "Final job status 'SUCCESS' not found after parameter execution."
        assert final_job_status_df['status'].iloc[0] == 'SUCCESS'
    ```

### Test Case 3: Help Option (`-h`)

*   **Purpose:** Verify that when the help parameter (`p_h`) is set to TRUE, the script prints the usage message and exits immediately without performing any other job logic (e.g., no log entries, no core script invocation). This replicates the legacy script's `usage` function and `exit` behavior.
*   **Setup:**
    *   Ensure the `test_project.test_dataset.job_log` table exists and is empty.
    *   All `DWMSG_*_SP` and `k_ausd_v_ta_vvl_dwh_sp` are deployed.
*   **Action:** Execute the `r_ausd_v_ta_vvl_dwh_bq.sql` script with `p_h` set to TRUE.
    ```sql
    -- Execute the main orchestration script with help parameter
    EXECUTE SCRIPT `test_project.test_dataset.r_ausd_v_ta_vvl_dwh_bq.sql`(
        p_h => TRUE
    );
    ```
*   **Pass/Fail Criterion:**
    *   The BigQuery script execution completes without error.
    *   The `test_project.test_dataset.job_log` table must remain empty, indicating no job logic (including logging) was executed.
    *   (Optional, if execution environment allows capturing script output): The BigQuery job output should contain the usage message defined in the script.

    ```python
    # Pytest assertion example
    def test_help_option(bq_client):
        project_id = "test_project"
        dataset_id = "test_dataset"
        job_log_table = f"`{project_id}.{dataset_id}.job_log`"
        script_path = f"`{project_id}.{dataset_id}.r_ausd_v_ta_vvl_dwh_bq.sql`"

        bq_client.query(f"TRUNCATE TABLE {job_log_table}").result()

        # Execute with help parameter. BigQuery script output is not directly captured by client.query().
        # We primarily rely on the absence of side effects (like logging).
        bq_client.query(f"EXECUTE SCRIPT {script_path}(p_h => TRUE)").result()

        # Assertions
        results = bq_client.query(f"SELECT COUNT(*) FROM {job_log_table}").result()
        log_count = results.to_dataframe()['f0_'].iloc[0]
        assert log_count == 0, "No log entries should be created when help option is used."

        # To verify the usage message, one would typically need to capture stdout/stderr of the execution environment.
        # For BigQuery scripts, this is harder. If running via `bq query --format=prettyjson`,
        # the 'query' field in the result might contain the SELECT output.
    ```

### Test Case 4: Error Handling - Core Script Failure

*   **Purpose:** Verify that if the core processing stored procedure (`k_ausd_v_ta_vvl_dwh_sp`) fails, the orchestration script correctly catches the error using its `EXCEPTION WHEN ERROR` block, logs the error details, and marks the job as FAILED. This replicates the legacy script's `trap ERR` behavior.
*   **Setup:**
    *   Ensure the `test_project.test_dataset.job_log` table exists and is empty.
    *   Temporarily modify `k_ausd_v_ta_vvl_dwh_sp` to intentionally raise an error (e.g., `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core script error';`).
    *   All other `DWMSG_*_SP` are deployed.
*   **Action:** Execute the `r_ausd_v_ta_vvl_dwh_bq.sql` script.
    ```sql
    -- Execute the main orchestration script
    EXECUTE SCRIPT `test_project.test_dataset.r_ausd_v_ta_vvl_dwh_bq.sql`();
    ```
*   **Pass/Fail Criterion:**
    *   The BigQuery script execution should report an error (e.g., the BigQuery job status will be FAILED).
    *   Query `test_project.test_dataset.job_log`. It must contain an entry with `log_level = 'ERROR'` and a `log_message` indicating the failure, including the simulated error message.
    *   The `status` column for the `job_nr` associated with this execution must ultimately be 'FAILED'.
    *   `DWMSG_Fehlerbehandlung_SP` must have been called, as evidenced by the log entries.

    ```python
    # Pytest assertion example
    def test_core_script_failure(bq_client):
        project_id = "test_project"
        dataset_id = "test_dataset"
        job_log_table = f"`{project_id}.{dataset_id}.job_log`"
        script_path = f"`{project_id}.{dataset_id}.r_ausd_v_ta_vvl_dwh_bq.sql`"
        core_sp_path = f"`{project_id}.{dataset_id}.k_ausd_v_ta_vvl_dwh_sp`"

        bq_client.query(f"TRUNCATE TABLE {job_log_table}").result()

        # Temporarily modify k_ausd_v_ta_vvl_dwh_sp to fail
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE {core_sp_path}(
                IN p_JobKennung STRING,
                IN p_DW_EintragsNr INT64
            )
            BEGIN
                INSERT INTO {job_log_table} (job_nr, log_level, log_message, log_ts)
                VALUES (p_DW_EintragsNr, 'INFO', 'Simulating core script start before error.', CURRENT_TIMESTAMP());
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core script error';
            END;
        """).result()

        # Execute the script, expecting it to fail
        try:
            bq_client.query(f"EXECUTE SCRIPT {script_path}()").result()
            pytest.fail("Script was expected to fail but completed successfully.")
        except Exception as e:
            # Verify the error message propagated from SIGNAL SQLSTATE
            assert "Simulated core script error" in str(e)

        # Assertions in job_log
        results = bq_client.query(f"""
            SELECT job_nr, log_level, log_message, status
            FROM {job_log_table}
            ORDER BY log_ts
        """).result()
        logs = list(results)

        assert any(l.log_level == 'ERROR' and "Job failed. Error: Simulated core script error" in l.log_message for l in logs), \
            "Error log entry with expected message not found."

        job_nr = logs[0].job_nr
        final_job_status_query = bq_client.query(f"""
            SELECT status FROM {job_log_table}
            WHERE job_nr = {job_nr} AND status = 'FAILED'
            ORDER BY log_ts DESC LIMIT 1
        """).result()
        final_job_status_df = final_job_status_query.to_dataframe()
        assert not final_job_status_df.empty, "Final job status 'FAILED' not found."
        assert final_job_status_df['status'].iloc[0] == 'FAILED'

        # Revert k_ausd_v_ta_vvl_dwh_sp to its original successful state for other tests
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE {core_sp_path}(
                IN p_JobKennung STRING,
                IN p_DW_EintragsNr INT64
            )
            BEGIN
                INSERT INTO {job_log_table} (job_nr, job_kennung, log_level, log_message, log_ts)
                VALUES (p_DW_EintragsNr, p_JobKennung, 'INFO', FORMAT("Starting core processing for JobKennung: %s, DW_EintragsNr: %d", p_JobKennung, p_DW_EintragsNr), CURRENT_TIMESTAMP());
                INSERT INTO {job_log_table} (job_nr, job_kennung, log_level, log_message, log_ts)
                VALUES (p_DW_EintragsNr, p_JobKennung, 'INFO', FORMAT("Finished core processing for JobKennung: %s, DW_EintragsNr: %d", p_JobKennung, p_DW_EintragsNr), CURRENT_TIMESTAMP());
            END;
        """).result()
    ```

### Test Case 5: Data Quality and Schema of `job_log` Table

*   **Purpose:** Verify that the `job_log` table schema matches the design, including column names, data types, and nullability constraints. This ensures data integrity for logging.
*   **Setup:**
    *   Ensure the `test_project.test_dataset.job_log` table exists.
*   **Action:** Query the `INFORMATION_SCHEMA.COLUMNS` view for the `job_log` table.
*   **Pass/Fail Criterion:**
    *   The query results must match the expected schema:
        *   `job_nr`: `INT64`, `NO` (NOT NULL)
        *   `job_kennung`: `STRING`, `YES` (NULLABLE)
        *   `script_name`: `STRING`, `YES` (NULLABLE)
        *   `log_identifier`: `STRING`, `YES` (NULLABLE)
        *   `log_level`: `STRING`, `YES` (NULLABLE)
        *   `log_message`: `STRING`, `YES` (NULLABLE)
        *   `log_ts`: `TIMESTAMP`, `YES` (NULLABLE)
        *   `reference_date`: `DATE`, `YES` (NULLABLE)
        *   `status`: `STRING`, `YES` (NULLABLE)

    ```sql
    -- SQL Assertion
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `test_project.test_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'job_log'
    ORDER BY
        ordinal_position;
    ```
    **Expected Output (Pass if matches):**
    ```
    column_name     data_type   is_nullable
    job_nr          INT64       NO
    job_kennung     STRING      YES
    script_name     STRING      YES
    log_identifier  STRING      YES
    log_level       STRING      YES
    log_message     STRING      YES
    log_ts          TIMESTAMP   YES
    reference_date  DATE        YES
    status          STRING      YES
    ```

### Test Case 6: `DWMSG_ErmittleNr_SP` Behavior (Job Number Uniqueness & Consistency)

*   **Purpose:** Verify that `DWMSG_ErmittleNr_SP` correctly generates a unique `job_nr` for each execution and that this number is consistently used across all log entries for a single job run. This ensures proper job tracking.
*   **Setup:**
    *   Ensure the `test_project.test_dataset.job_log` table exists and is empty.
    *   All `DWMSG_*_SP` and `k_ausd_v_ta_vvl_dwh_sp` are deployed and accessible, with `k_ausd_v_ta_vvl_dwh_sp` completing successfully.
*   **Action:** Execute the `r_ausd_v_ta_vvl_dwh_bq.sql` script twice, clearing the `job_log` table between runs.
*   **Pass/Fail Criterion:**
    *   Both script executions complete successfully.
    *   The `job_nr` generated for the first run must be different from the `job_nr` generated for the second run.
    *   For each individual run, all log entries in `job_log` must share the exact same `job_nr`.

    ```python
    # Pytest assertion example
    def test_job_nr_uniqueness_and_consistency(bq_client):
        project_id = "test_project"
        dataset_id = "test_dataset"
        job_log_table = f"`{project_id}.{dataset_id}.job_log`"
        script_path = f"`{project_id}.{dataset_id}.r_ausd_v_ta_vvl_dwh_bq.sql`"

        bq_client.query(f"TRUNCATE TABLE {job_log_table}").result()

        # Execute first time
        bq_client.query(f"EXECUTE SCRIPT {script_path}()").result()
        first_job_nr_query = bq_client.query(f"SELECT job_nr FROM {job_log_table} LIMIT 1").result()
        first_job_nr = first_job_nr_query.to_dataframe()['job_nr'].iloc[0]
        first_run_logs_count_query = bq_client.query(f"SELECT COUNT(*) FROM {job_log_table} WHERE job_nr = {first_job_nr}").result()
        first_run_logs_count = first_run_logs_count_query.to_dataframe()['f0_'].iloc[0]
        assert first_run_logs_count >= 6, "First run should have logged multiple entries."

        # Clear logs and execute second time
        bq_client.query(f"TRUNCATE TABLE {job_log_table}").result()
        bq_client.query(f"EXECUTE SCRIPT {script_path}()").result()
        second_job_nr_query = bq_client.query(f"SELECT job_nr FROM {job_log_table} LIMIT 1").result()
        second_job_nr = second_job_nr_query.to_dataframe()['job_nr'].iloc[0]
        second_run_logs_count_query = bq_client.query(f"SELECT COUNT(*) FROM {job_log_table} WHERE job_nr = {second_job_nr}").result()
        second_run_logs_count = second_run_logs_count_query.to_dataframe()['f0_'].iloc[0]
        assert second_run_logs_count >= 6, "Second run should have logged multiple entries."

        # Assert uniqueness of job_nr across runs
        assert first_job_nr != second_job_nr, "Job numbers should be unique across different executions."

        # Assert consistency within a run (all logs for a run have the same job_nr)
        all_job_nrs_first_run_query = bq_client.query(f"SELECT DISTINCT job_nr FROM {job_log_table} WHERE job_nr = {first_job_nr}").result()
        all_job_nrs_first_run = all_job_nrs_first_run_query.to_dataframe()['job_nr'].tolist()
        assert len(all_job_nrs_first_run) == 1 and all_job_nrs_first_run[0] == first_job_nr, \
            "All log entries for a single run must have the same job_nr."
    ```

### Test Case 7: `JobKennung` Generation and Usage

*   **Purpose:** Verify that `JobKennung` is generated as specified in the BigQuery script (using `ProgName` and a timestamp) and is consistently used across all log entries for a given job. This ensures proper identification of job runs.
*   **Setup:**
    *   Ensure the `test_project.test_dataset.job_log` table exists and is empty.
    *   All `DWMSG_*_SP` and `k_ausd_v_ta_vvl_dwh_sp` are deployed and accessible, with `k_ausd_v_ta_vvl_dwh_sp` completing successfully.
*   **Action:** Execute the `r_ausd_v_ta_vvl_dwh_bq.sql` script.
*   **Pass/Fail Criterion:**
    *   The script completes successfully.
    *   Query `test_project.test_dataset.job_log` and verify that all entries for a single execution share the same `job_kennung`.
    *   Verify the format of `job_kennung` matches `JOB_<UPPERCASE_SCRIPT_NAME>_YYYYMMDD_HHMMSS`.

    ```python
    # Pytest assertion example
    def test_job_kennung_consistency_and_format(bq_client):
        project_id = "test_project"
        dataset_id = "test_dataset"
        job_log_table = f"`{project_id}.{dataset_id}.job_log`"
        script_path = f"`{project_id}.{dataset_id}.r_ausd_v_ta_vvl_dwh_bq.sql`"

        bq_client.query(f"TRUNCATE TABLE {job_log_table}").result()

        bq_client.query(f"EXECUTE SCRIPT {script_path}()").result()

        results = bq_client.query(f"""
            SELECT DISTINCT job_kennung
            FROM {job_log_table}
            WHERE job_kennung IS NOT NULL
        """).result()
        job_kennungs = results.to_dataframe()['job_kennung'].tolist()

        assert len(job_kennungs) == 1, "All log entries for a single run must have the same JobKennung."
        
        actual_job_kennung = job_kennungs[0]
        # Regex to match JOB_R_AUSD_V_TA_VVL_DWH_BQ.SQL_YYYYMMDD_HHMMSS
        # Note: ProgName is 'r_ausd_v_ta_vvl_dwh_bq.sql' in the BQ script
        expected_pattern = r"JOB_R_AUSD_V_TA_VVL_DWH_BQ.SQL_\d{8}_\d{6}"
        assert re.match(expected_pattern, actual_job_kennung), \
            f"JobKennung format mismatch. Expected pattern: {expected_pattern}, Actual: {actual_job_kennung}"
    ```