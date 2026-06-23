The migration of `r_ausd_v_ta_discount_rr.ksh` to BigQuery stored procedures primarily involves transforming orchestration, logging, and error handling logic. The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery solution against the legacy KornShell script.

**Assumptions for Test Execution:**
*   All DDLs for `job_logging_table` and `configuration_table` have been executed.
*   All `DWMSG_*` helper stored procedures have been created.
*   The placeholder `k_ausd_v_ta_discount_rr` stored procedure has been created.
*   The main `Vertragsdatenabgleich` stored procedure has been created.
*   `your_project.your_dataset` is a placeholder for the actual BigQuery project and dataset names.
*   Pytest examples assume a `bigquery_client` fixture is available, configured to interact with the target BigQuery environment.

---

### Test Case 1: Successful Execution with Valid Parameters

*   **Purpose**: Verify that the migrated job runs successfully when invoked with valid parameters, and all expected log entries are created, culminating in a 'SUCCESS' status. This covers basic output parity and transformation correctness for the happy path.
*   **Setup**:
    *   Ensure `your_project.your_dataset.job_logging_table` is empty.
    *   Populate `your_project.your_dataset.configuration_table` with `('BERT_DIR_ROOT', '/path/to/bert')`.
*   **Action**:
    ```sql
    CALL `your_project.your_dataset.Vertragsdatenabgleich`(
        p_param_s => 'test_s_value',
        p_param_l => 'test_l_value',
        p_param_h => FALSE
    );
    ```
*   **Pass/Fail Criterion**:
    *   The call completes without raising an unhandled BigQuery error.
    *   Assert that `your_project.your_dataset.job_logging_table` contains at least 7 entries (Job started, Stichtag, Parameters, Conceptual Log File, Core logic start, Core logic complete, Job finished successfully).
    *   The last entry for the `job_key` of this run has `status = 'SUCCESS'`.
    *   The `message` fields contain expected strings like "Job `r_ausd_v_ta_discount_rr.ksh` started...", "Reference Date (Stichtag): ...", "Parameters received: -s=\"test_s_value\", -l=\"test_l_value\"", "Job finished successfully.".
    *   The `job_key` is consistent across all entries for this run.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_successful_execution(bigquery_client: bigquery.Client):
        # Setup: Clear logs, insert config
        bigquery_client.query("TRUNCATE TABLE `your_project.your_dataset.job_logging_table`").result()
        bigquery_client.query("DELETE FROM `your_project.your_dataset.configuration_table` WHERE config_key = 'BERT_DIR_ROOT'").result()
        bigquery_client.query("INSERT INTO `your_project.your_dataset.configuration_table` (config_key, config_value) VALUES ('BERT_DIR_ROOT', '/path/to/bert')").result()

        # Action: Call the main procedure
        bigquery_client.query("""
            CALL `your_project.your_dataset.Vertragsdatenabgleich`(
                p_param_s => 'test_s_value',
                p_param_l => 'test_l_value',
                p_param_h => FALSE
            );
        """).result()

        # Pass/Fail Criterion: Assert log entries
        logs = list(bigquery_client.query("SELECT job_key, message, log_level, status FROM `your_project.your_dataset.job_logging_table` ORDER BY created_at ASC").result())
        
        assert len(logs) >= 7 # At least start, stichtag, params, conceptual log, core start, core end, success
        
        job_key = logs[0].job_key
        assert all(log.job_key == job_key for log in logs)

        # Check specific messages and order
        assert "Job `r_ausd_v_ta_discount_rr.ksh` started in BigQuery." in logs[0].message
        assert "Reference Date (Stichtag):" in logs[1].message
        assert "Parameters received: -s=\"test_s_value\", -l=\"test_l_value\"" in logs[2].message
        assert "Conceptual Log File Name:" in logs[3].message
        assert "Executing core data reconciliation logic (placeholder)." in logs[4].message
        assert "Core data reconciliation logic completed (placeholder)." in logs[5].message
        assert "Job finished successfully." in logs[-1].message
        assert logs[-1].status == 'SUCCESS'
    ```

### Test Case 2: Help Parameter (`-h`) Execution

*   **Purpose**: Verify that when the help parameter is provided, the job logs the help message and exits gracefully without executing the core logic. This tests parameter handling and early exit logic, mimicking the original script's `usage` function and `exit`.
*   **Setup**:
    *   Ensure `your_project.your_dataset.job_logging_table` is empty.
    *   Populate `your_project.your_dataset.configuration_table` with `('BERT_DIR_ROOT', '/path/to/bert')`.
*   **Action**:
    ```sql
    CALL `your_project.your_dataset.Vertragsdatenabgleich`(
        p_param_s => NULL, -- Parameters -s and -l are ignored when -h is true
        p_param_l => NULL,
        p_param_h => TRUE
    );
    ```
*   **Pass/Fail Criterion**:
    *   The call completes without raising an unhandled BigQuery error.
    *   Assert that `your_project.your_dataset.job_logging_table` contains exactly 2 entries for this run (Job started, Help requested).
    *   The last entry for the `job_key` of this run has `status = 'SUCCESS'`.
    *   The `message` fields contain "Job `r_ausd_v_ta_discount_rr.ksh` started..." and "Help requested. Parameters: -s (some_value), -l (another_value).".
    *   There should be no log entries related to `DWMSG_SetzeStichtagInfo` or `k_ausd_v_ta_discount_rr` execution.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_help_parameter_execution(bigquery_client: bigquery.Client):
        # Setup: Clear logs, insert config
        bigquery_client.query("TRUNCATE TABLE `your_project.your_dataset.job_logging_table`").result()
        bigquery_client.query("DELETE FROM `your_project.your_dataset.configuration_table` WHERE config_key = 'BERT_DIR_ROOT'").result()
        bigquery_client.query("INSERT INTO `your_project.your_dataset.configuration_table` (config_key, config_value) VALUES ('BERT_DIR_ROOT', '/path/to/bert')").result()

        # Action: Call the main procedure with p_param_h = TRUE
        bigquery_client.query("""
            CALL `your_project.your_dataset.Vertragsdatenabgleich`(
                p_param_s => NULL,
                p_param_l => NULL,
                p_param_h => TRUE
            );
        """).result()

        # Pass/Fail Criterion: Assert log entries
        logs = list(bigquery_client.query("SELECT job_key, message, log_level, status FROM `your_project.your_dataset.job_logging_table` ORDER BY created_at ASC").result())
        
        assert len(logs) == 2 # Job started, Help requested
        
        job_key = logs[0].job_key
        assert all(log.job_key == job_key for log in logs)

        assert "Job `r_ausd_v_ta_discount_rr.ksh` started in BigQuery." in logs[0].message
        assert "Help requested. Parameters: -s (some_value), -l (another_value)." in logs[1].message
        assert logs[1].status == 'SUCCESS'
        
        # Ensure core logic was NOT called
        assert not any("Executing core data reconciliation logic" in log.message for log in logs)
    ```

### Test Case 3: Missing `BERT_DIR_ROOT` Configuration

*   **Purpose**: Verify that the job correctly handles a missing critical configuration (`BERT_DIR_ROOT`), logs an error, and aborts execution. This tests external-system replacement (configuration table) and error handling, mimicking a missing environment variable.
*   **Setup**:
    *   Ensure `your_project.your_dataset.job_logging_table` is empty.
    *   Ensure `your_project.your_dataset.configuration_table` *does not* contain an entry for `BERT_DIR_ROOT`.
*   **Action**:
    ```sql
    -- This call is expected to raise an error
    CALL `your_project.your_dataset.Vertragsdatenabgleich`(
        p_param_s => 'value_s',
        p_param_l => 'value_l',
        p_param_h => FALSE
    );
    ```
*   **Pass/Fail Criterion**:
    *   The call *raises* a BigQuery error (e.g., `BQ.ABORTED ERROR`).
    *   Assert that `your_project.your_dataset.job_logging_table` contains at least 2 entries: "Job started" and "ERROR: BERT_DIR_ROOT configuration not found.".
    *   The last entry for the `job_key` of this run has `status = 'FAILED'` and `log_level = 'ERROR'`.
    *   No entries related to `DWMSG_SetzeStichtagInfo` or `k_ausd_v_ta_discount_rr` should be present after the error.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_missing_bert_dir_root_config(bigquery_client: bigquery.Client):
        # Setup: Clear logs, ensure BERT_DIR_ROOT is missing
        bigquery_client.query("TRUNCATE TABLE `your_project.your_dataset.job_logging_table`").result()
        bigquery_client.query("DELETE FROM `your_project.your_dataset.configuration_table` WHERE config_key = 'BERT_DIR_ROOT'").result()

        # Action: Call the main procedure, expecting an error
        with pytest.raises(Exception) as excinfo:
            bigquery_client.query("""
                CALL `your_project.your_dataset.Vertragsdatenabgleich`(
                    p_param_s => 'value_s',
                    p_param_l => 'value_l',
                    p_param_h => FALSE
                );
            """).result()
        
        # Pass/Fail Criterion: Assert error message and log entries
        assert "Configuration error: BERT_DIR_ROOT is not set." in str(excinfo.value)

        logs = list(bigquery_client.query("SELECT job_key, message, log_level, status FROM `your_project.your_dataset.job_logging_table` ORDER BY created_at ASC").result())
        
        assert len(logs) >= 2 # Job started, Error message
        
        job_key = logs[0].job_key
        assert all(log.job_key == job_key for log in logs)

        assert "Job `r_ausd_v_ta_discount_rr.ksh` started in BigQuery." in logs[0].message
        assert "ERROR: BERT_DIR_ROOT configuration not found." in logs[1].message
        assert logs[1].log_level == 'ERROR'
        assert logs[1].status == 'FAILED'
        
        # Ensure core logic was NOT called
        assert not any("Executing core data reconciliation logic" in log.message for log in logs)
    ```

### Test Case 4: Core Script (`k_ausd_v_ta_discount_rr`) Failure

*   **Purpose**: Verify that if the invoked core script (`k_ausd_v_ta_discount_rr`) fails, the main orchestration procedure catches the error, logs it correctly, and sets the overall job status to 'FAILED'. This tests the `BEGIN...EXCEPTION...END` block and `DWMSG_Fehlerbehandlung`, replicating the `trap ERR` behavior.
*   **Setup**:
    *   Ensure `your_project.your_dataset.job_logging_table` is empty.
    *   Populate `your_project.your_dataset.configuration_table` with `('BERT_DIR_ROOT', '/path/to/bert')`.
    *   **Temporarily modify `your_project.your_dataset.k_ausd_v_ta_discount_rr` to raise an error**:
        ```sql
        CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_v_ta_discount_rr`(
            IN p_job_key STRING,
            IN p_param_s STRING,
            IN p_param_l STRING
        )
        BEGIN
            CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
                p_job_key,
                'Executing core data reconciliation logic (simulated failure).',
                'INFO',
                'RUNNING'
            );
            RAISE BQ.ABORTED ERROR 'Simulated core script failure for testing purposes.';
        END;
        ```
*   **Action**:
    ```sql
    -- This call is expected to raise an error
    CALL `your_project.your_dataset.Vertragsdatenabgleich`(
        p_param_s => 'fail_s',
        p_param_l => 'fail_l',
        p_param_h => FALSE
    );
    ```
*   **Pass/Fail Criterion**:
    *   The call *raises* a BigQuery error.
    *   Assert that `your_project.your_dataset.job_logging_table` contains entries for:
        *   Job started
        *   Reference Date
        *   Parameters received
        *   Conceptual Log File Name
        *   Core script execution start
        *   Error message from `DWMSG_Fehlerbehandlung` (containing "Simulated core script failure...")
    *   The last entry for the `job_key` of this run has `status = 'FAILED'` and `log_level = 'ERROR'`.
    *   The `UPDATE` statement in `DWMSG_Fehlerbehandlung` should have updated the initial 'RUNNING' status of the job's first log entry to 'FAILED'.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_core_script_failure(bigquery_client: bigquery.Client):
        # Setup: Clear logs, insert config
        bigquery_client.query("TRUNCATE TABLE `your_project.your_dataset.job_logging_table`").result()
        bigquery_client.query("DELETE FROM `your_project.your_dataset.configuration_table` WHERE config_key = 'BERT_DIR_ROOT'").result()
        bigquery_client.query("INSERT INTO `your_project.your_dataset.configuration_table` (config_key, config_value) VALUES ('BERT_DIR_ROOT', '/path/to/bert')").result()

        # Setup: Modify k_ausd_v_ta_discount_rr to fail
        bigquery_client.query("""
            CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_v_ta_discount_rr`(
                IN p_job_key STRING,
                IN p_param_s STRING,
                IN p_param_l STRING
            )
            BEGIN
                CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
                    p_job_key,
                    'Executing core data reconciliation logic (simulated failure).',
                    'INFO',
                    'RUNNING'
                );
                RAISE BQ.ABORTED ERROR 'Simulated core script failure for testing purposes.';
            END;
        """).result()

        # Action: Call the main procedure, expecting an error
        with pytest.raises(Exception) as excinfo:
            bigquery_client.query("""
                CALL `your_project.your_dataset.Vertragsdatenabgleich`(
                    p_param_s => 'fail_s',
                    p_param_l => 'fail_l',
                    p_param_h => FALSE
                );
            """).result()
        
        # Pass/Fail Criterion: Assert error message and log entries
        assert "Simulated core script failure for testing purposes." in str(excinfo.value)

        logs = list(bigquery_client.query("SELECT job_key, message, log_level, status FROM `your_project.your_dataset.job_logging_table` ORDER BY created_at ASC").result())
        
        # Expected logs: Job started, Stichtag, Params, Conceptual Log, Core start, Error handling
        assert len(logs) >= 6 
        
        job_key = logs[0].job_key
        assert all(log.job_key == job_key for log in logs)

        assert "Job `r_ausd_v_ta_discount_rr.ksh` started in BigQuery." in logs[0].message
        assert "Reference Date (Stichtag):" in logs[1].message
        assert "Parameters received: -s=\"fail_s\", -l=\"fail_l\"" in logs[2].message
        assert "Conceptual Log File Name:" in logs[3].message
        assert "Executing core data reconciliation logic (simulated failure)." in logs[4].message
        assert "Job failed with exit code" in logs[-1].message
        assert "Simulated core script failure for testing purposes." in logs[-1].message
        assert logs[-1].log_level == 'ERROR'
        assert logs[-1].status == 'FAILED'

        # Verify that the initial 'RUNNING' status was updated to 'FAILED'
        initial_entry_status = bigquery_client.query(f"SELECT status FROM `your_project.your_dataset.job_logging_table` WHERE job_key = '{job_key}' AND message LIKE 'Job `r_ausd_v_ta_discount_rr.ksh` started%'").result().to_dataframe().iloc[0]['status']
        assert initial_entry_status == 'FAILED'

        # Restore k_ausd_v_ta_discount_rr to its original placeholder state for other tests
        bigquery_client.query("""
            CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_v_ta_discount_rr`(
                IN p_job_key STRING,
                IN p_param_s STRING,
                IN p_param_l STRING
            )
            BEGIN
                CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
                    p_job_key,
                    'Executing core data reconciliation logic (placeholder).',
                    'INFO',
                    'RUNNING'
                );
                CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
                    p_job_key,
                    'Core data reconciliation logic completed (placeholder).',
                    'INFO',
                    'RUNNING'
                );
            END;
        """).result()
    ```

### Test Case 5: `DWMSG_ErmittleNr` Functionality (Data Quality)

*   **Purpose**: Verify that `DWMSG_ErmittleNr` correctly generates sequential job entry numbers, even across multiple job runs and direct calls. This ensures data quality for the `job_entry_nr` column.
*   **Setup**:
    *   Ensure `your_project.your_dataset.job_logging_table` is empty.
*   **Action**:
    *   Call `DWMSG_ErmittleNr` multiple times directly.
    *   Run `Vertragsdatenabgleich` once.
    *   Call `DWMSG_ErmittleNr` again.
*   **Pass/Fail Criterion**:
    *   The `p_next_job_entry_nr` output parameter from `DWMSG_ErmittleNr` should be `1` initially, then `2` after one log entry, etc.
    *   The `job_entry_nr` in `your_project.your_dataset.job_logging_table` should be unique and sequentially increasing.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_dwmsg_ermittlenr_sequence(bigquery_client: bigquery.Client):
        # Setup: Clear logs
        bigquery_client.query("TRUNCATE TABLE `your_project.your_dataset.job_logging_table`").result()

        # Action 1: Call DWMSG_ErmittleNr directly (should be 1)
        result1 = bigquery_client.query("CALL `your_project.your_dataset.DWMSG_ErmittleNr`(@next_nr); SELECT @next_nr;").result()
        next_nr_1 = list(result1)[0][0]
        assert next_nr_1 == 1

        # Action 2: Insert a log entry (simulating DWMSG_ErzeugeEintrag)
        bigquery_client.query("INSERT INTO `your_project.your_dataset.job_logging_table` (job_entry_nr, job_key, message, log_level, status) VALUES (1, 'TEST_KEY_1', 'Test message 1', 'INFO', 'RUNNING')").result()

        # Action 3: Call DWMSG_ErmittleNr again (should be 2)
        result2 = bigquery_client.query("CALL `your_project.your_dataset.DWMSG_ErmittleNr`(@next_nr); SELECT @next_nr;").result()
        next_nr_2 = list(result2)[0][0]
        assert next_nr_2 == 2

        # Action 4: Run the main job (will create multiple entries, starting from job_entry_nr = 2)
        bigquery_client.query("DELETE FROM `your_project.your_dataset.configuration_table` WHERE config_key = 'BERT_DIR_ROOT'").result()
        bigquery_client.query("INSERT INTO `your_project.your_dataset.configuration_table` (config_key, config_value) VALUES ('BERT_DIR_ROOT', '/path/to/bert')").result()
        bigquery_client.query("""
            CALL `your_project.your_dataset.Vertragsdatenabgleich`(
                p_param_s => 'test_s_value',
                p_param_l => 'test_l_value',
                p_param_h => FALSE
            );
        """).result()

        # Action 5: Call DWMSG_ErmittleNr again. It should be MAX(job_entry_nr) + 1
        max_nr_in_table = list(bigquery_client.query("SELECT MAX(job_entry_nr) FROM `your_project.your_dataset.job_logging_table`").result())[0][0]
        result3 = bigquery_client.query("CALL `your_project.your_dataset.DWMSG_ErmittleNr`(@next_nr); SELECT @next_nr;").result()
        next_nr_3 = list(result3)[0][0]
        assert next_nr_3 == max_nr_in_table + 1
    ```

### Test Case 6: `DWMSG_SetzeStatusOK` and `DWMSG_Fehlerbehandlung` Status Update

*   **Purpose**: Verify that `DWMSG_SetzeStatusOK` correctly updates the status of the *initial* job entry to 'SUCCESS' and `DWMSG_Fehlerbehandlung` updates it to 'FAILED'. This ensures the overall job status is accurately reflected in the logging table.
*   **Setup**:
    *   Ensure `your_project.your_dataset.job_logging_table` is empty.
    *   Populate `your_project.your_dataset.configuration_table` with `('BERT_DIR_ROOT', '/path/to/bert')`.
*   **Action**:
    1.  Run `Vertragsdatenabgleich` successfully (as in Test Case 1).
    2.  Run `Vertragsdatenabgleich` with a simulated core script failure (as in Test Case 4).
*   **Pass/Fail Criterion**:
    *   For the successful run, the `status` of the very first log entry (job start) for that specific `job_key` should be updated from 'RUNNING' to 'SUCCESS'.
    *   For the failed run, the `status` of the very first log entry (job start) for that specific `job_key` should be updated from 'RUNNING' to 'FAILED'.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_status_update_on_success_and_failure(bigquery_client: bigquery.Client):
        # Setup: Clear logs, insert config
        bigquery_client.query("TRUNCATE TABLE `your_project.your_dataset.job_logging_table`").result()
        bigquery_client.query("DELETE FROM `your_project.your_dataset.configuration_table` WHERE config_key = 'BERT_DIR_ROOT'").result()
        bigquery_client.query("INSERT INTO `your_project.your_dataset.configuration_table` (config_key, config_value) VALUES ('BERT_DIR_ROOT', '/path/to/bert')").result()

        # --- Test Success ---
        bigquery_client.query("""
            CALL `your_project.your_dataset.Vertragsdatenabgleich`(
                p_param_s => 'success_s',
                p_param_l => 'success_l',
                p_param_h => FALSE
            );
        """).result()

        success_logs = list(bigquery_client.query("SELECT job_key, message, status FROM `your_project.your_dataset.job_logging_table` WHERE message LIKE 'Job `r_ausd_v_ta_discount_rr.ksh` started%' ORDER BY created_at ASC").result())
        assert len(success_logs) == 1
        assert success_logs[0].status == 'SUCCESS'
        success_job_key = success_logs[0].job_key

        # --- Test Failure ---
        # Modify k_ausd_v_ta_discount_rr to fail
        bigquery_client.query("""
            CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_v_ta_discount_rr`(
                IN p_job_key STRING,
                IN p_param_s STRING,
                IN p_param_l STRING
            )
            BEGIN
                RAISE BQ.ABORTED ERROR 'Simulated core script failure for status update test.';
            END;
        """).result()

        with pytest.raises(Exception):
            bigquery_client.query("""
                CALL `your_project.your_dataset.Vertragsdatenabgleich`(
                    p_param_s => 'fail_s',
                    p_param_l => 'fail_l',
                    p_param_h => FALSE
                );
            """).result()
        
        failure_logs = list(bigquery_client.query(f"SELECT job_key, message, status FROM `your_project.your_dataset.job_logging_table` WHERE message LIKE 'Job `r_ausd_v_ta_discount_rr.ksh` started%' AND job_key != '{success_job_key}' ORDER BY created_at ASC").result())
        assert len(failure_logs) == 1
        assert failure_logs[0].status == 'FAILED'

        # Restore k_ausd_v_ta_discount_rr
        bigquery_client.query("""
            CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_v_ta_discount_rr`(
                IN p_job_key STRING,
                IN p_param_s STRING,
                IN p_param_l STRING
            )
            BEGIN
                CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
                    p_job_key,
                    'Executing core data reconciliation logic (placeholder).',
                    'INFO',
                    'RUNNING'
                );
                CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
                    p_job_key,
                    'Core data reconciliation logic completed (placeholder).',
                    'INFO',
                    'RUNNING'
                );
            END;
        """).result()
    ```

### Test Case 7: `DWMSG_Logdateiname` Output Format (Output Parity)

*   **Purpose**: Verify that `DWMSG_Logdateiname` generates a log file name string in the expected format, consistent with the original script's naming convention, even though it's conceptual in BigQuery.
*   **Setup**: None.
*   **Action**: Call `DWMSG_Logdateiname` directly.
    ```sql
    CALL `your_project.your_dataset.DWMSG_Logdateiname`(
        p_job_key => 'TEST_JOB_KEY',
        p_log_file_name => @log_name
    );
    SELECT @log_name;
    ```
*   **Pass/Fail Criterion**:
    *   The returned `p_log_file_name` should match the pattern `TEST_JOB_KEY_YYYYMMDD_HHMMSS.log`.

    ```python
    import pytest
    import re
    from google.cloud import bigquery

    def test_dwmsg_logdateiname_format(bigquery_client: bigquery.Client):
        # Action: Call the procedure
        result = bigquery_client.query("""
            CALL `your_project.your_dataset.DWMSG_Logdateiname`(
                p_job_key => 'TEST_JOB_KEY',
                p_log_file_name => @log_name
            );
            SELECT @log_name;
        """).result()
        
        log_name = list(result)[0][0]
        
        # Pass/Fail Criterion: Assert format
        assert re.match(r"TEST_JOB_KEY_\d{8}_\d{6}\.log", log_name) is not None
    ```

### Test Case 8: Parameter Handling - NULL values for `-s` and `-l`

*   **Purpose**: Verify that the main procedure correctly handles `NULL` values for `p_param_s` and `p_param_l` without error, and logs them as empty strings (BigQuery's `CONCAT` behavior for `NULL`). This covers NULL handling.
*   **Setup**:
    *   Ensure `your_project.your_dataset.job_logging_table` is empty.
    *   Populate `your_project.your_dataset.configuration_table` with `('BERT_DIR_ROOT', '/path/to/bert')`.
*   **Action**:
    ```sql
    CALL `your_project.your_dataset.Vertragsdatenabgleich`(
        p_param_s => NULL,
        p_param_l => NULL,
        p_param_h => FALSE
    );
    ```
*   **Pass/Fail Criterion**:
    *   The call completes successfully.
    *   The log entry for "Parameters received" should show empty strings for both parameters (e.g., `Parameters received: -s="", -l=""`).
    *   The job status should be 'SUCCESS'.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_null_parameter_handling(bigquery_client: bigquery.Client):
        # Setup: Clear logs, insert config
        bigquery_client.query("TRUNCATE TABLE `your_project.your_dataset.job_logging_table`").result()
        bigquery_client.query("DELETE FROM `your_project.your_dataset.configuration_table` WHERE config_key = 'BERT_DIR_ROOT'").result()
        bigquery_client.query("INSERT INTO `your_project.your_dataset.configuration_table` (config_key, config_value) VALUES ('BERT_DIR_ROOT', '/path/to/bert')").result()

        # Action: Call with NULL parameters
        bigquery_client.query("""
            CALL `your_project.your_dataset.Vertragsdatenabgleich`(
                p_param_s => NULL,
                p_param_l => NULL,
                p_param_h => FALSE
            );
        """).result()

        # Pass/Fail Criterion: Assert log entries
        logs = list(bigquery_client.query("SELECT message, status FROM `your_project.your_dataset.job_logging_table` WHERE message LIKE 'Parameters received%'").result())
        
        assert len(logs) == 1
        assert 'Parameters received: -s="", -l=""' in logs[0].message
        
        # Check overall success
        final_status = list(bigquery_client.query("SELECT status FROM `your_project.your_dataset.job_logging_table` ORDER BY created_at DESC LIMIT 1").result())[0][0]
        assert final_status == 'SUCCESS'
    ```

### Test Case 9: Data Quality - `job_logging_table` Schema and Constraints

*   **Purpose**: Verify that the `job_logging_table` adheres to its defined schema and constraints, ensuring data integrity for logging. This covers schema assertions.
*   **Setup**: None, this is a schema check.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA` for table and column details.
*   **Pass/Fail Criterion**:
    *   Table `your_project.your_dataset.job_logging_table` exists.
    *   Columns `job_entry_nr`, `job_key`, `log_level`, `message`, `created_at`, `status` exist with correct data types (`INT64`, `STRING`, `STRING`, `STRING`, `TIMESTAMP`, `STRING`).
    *   `job_entry_nr` and `job_key` are `NOT NULL` (mode `REQUIRED`).
    *   `created_at` has a `DEFAULT CURRENT_TIMESTAMP()`.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_job_logging_table_schema(bigquery_client: bigquery.Client):
        table_id = "your_project.your_dataset.job_logging_table"
        table = bigquery_client.get_table(table_id)

        # Check table existence
        assert table is not None

        # Check column schema
        expected_schema = {
            "job_entry_nr": {"field_type": "INTEGER", "mode": "REQUIRED"},
            "job_key": {"field_type": "STRING", "mode": "REQUIRED"},
            "log_level": {"field_type": "STRING", "mode": "NULLABLE"},
            "message": {"field_type": "STRING", "mode": "NULLABLE"},
            "created_at": {"field_type": "TIMESTAMP", "mode": "NULLABLE"}, # DEFAULT CURRENT_TIMESTAMP() makes it nullable
            "status": {"field_type": "STRING", "mode": "NULLABLE"},
        }

        actual_schema = {field.name: {"field_type": field.field_type, "mode": field.mode} for field in table.schema}

        for col_name, expected_props in expected_schema.items():
            assert col_name in actual_schema, f"Column {col_name} not found in table schema."
            assert actual_schema[col_name]["field_type"] == expected_props["field_type"], \
                f"Column {col_name} type mismatch: Expected {expected_props['field_type']}, got {actual_schema[col_name]['field_type']}"
            assert actual_schema[col_name]["mode"] == expected_props["mode"], \
                f"Column {col_name} mode mismatch: Expected {expected_props['mode']}, got {actual_schema[col_name]['mode']}"
        
        # Check default value for created_at (requires querying INFORMATION_SCHEMA.COLUMNS)
        query = f"""
            SELECT column_default_expression
            FROM `{table.project}.{table.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table.table_id}' AND column_name = 'created_at'
        """
        result = bigquery_client.query(query).result()
        default_expression = list(result)[0][0]
        assert default_expression == 'CURRENT_TIMESTAMP()'
    ```