This document outlines migration validation tests for the `r_ausd_v_ta_cntrct_crs.ksh` KornShell script, which has been migrated to a BigQuery Stored Procedure named `project.dataset.vertragsdatenabgleich_wrapper`. The tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

**General Setup for all Tests:**

Before running any tests, ensure the following:
1.  The DDL for `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log` tables has been applied to your BigQuery environment.
2.  The `project.dataset.k_ausd_v_ta_cntrct_crs` stub procedure has been deployed.
3.  The `project.dataset.vertragsdatenabgleich_wrapper` procedure has been deployed.
4.  For each test case, clear the contents of `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log` tables to ensure test isolation.

```sql
-- Example cleanup script (run before each test case)
TRUNCATE TABLE `project.dataset.job_control`;
TRUNCATE TABLE `project.dataset.job_log`;
TRUNCATE TABLE `project.dataset.job_error_log`;
```

---

## Test Case 1: Successful Execution (Happy Path)

*   **Purpose**: Verify that the migrated wrapper procedure executes successfully, logs all expected events, updates job status to 'OK', and correctly invokes the core processing procedure with the right parameters, mirroring the legacy script's successful run. This covers output parity and external system replacement (core script invocation).
*   **Setup**:
    1.  Clear `project.dataset.job_control`, `project.dataset.job_log`, `project.dataset.job_error_log` tables.
    2.  Ensure the `project.dataset.k_ausd_v_ta_cntrct_crs` stub is deployed and configured to succeed (as per its default implementation).
*   **Action**:
    Execute the wrapper procedure with a valid `job_kennung`.
    ```sql
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_JOB_SUCCESS');
    ```
*   **Pass/Fail Criterion**:
    1.  **`project.dataset.job_control` table**:
        *   Exactly one row is inserted.
        *   `job_kennung` column is 'TEST_JOB_SUCCESS'.
        *   `status` column is 'OK'.
        *   `program_name` is 'r_ausd_v_ta_cntrct_crs' and `program_version` is '1.0'.
        *   `start_time` and `end_time` are populated, and `end_time` is after `start_time`.
        *   `reference_date` is `CURRENT_DATE()`.
    2.  **`project.dataset.job_log` table**:
        *   At least 7 `INFO` level messages are present, reflecting the job lifecycle:
            *   Wrapper start message (e.g., "r_ausd_v_ta_cntrct_crs (Version 1.0) started for JobKennung: TEST_JOB_SUCCESS...")
            *   Reference date message (e.g., "Reference Date (Stichtag): YYYY-MM-DD")
            *   Core script invocation message (e.g., "Invoking core script: k_ausd_v_ta_cntrct_crs")
            *   Core script start message (e.g., "Core script k_ausd_v_ta_cntrct_crs started...")
            *   Core script processing message (e.g., "Simulating data processing steps...")
            *   Core script completion message (e.g., "Core script k_ausd_v_ta_cntrct_crs completed successfully.")
            *   Wrapper completion message (e.g., "r_ausd_v_ta_cntrct_crs completed successfully...")
        *   All `job_id`s in `job_log` match the `job_id` from the `job_control` entry.
    3.  **`project.dataset.job_error_log` table**:
        *   Zero rows are inserted.

    ```python
    # Example pytest-style assertion
    def test_successful_execution(bq_client):
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`;").result()

        bq_client.query("CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_JOB_SUCCESS');").result()

        # Assert job_control
        job_control_rows = list(bq_client.query("""
            SELECT job_id, job_kennung, status, program_name, program_version, start_time, end_time, reference_date
            FROM `project.dataset.job_control`
            WHERE job_kennung = 'TEST_JOB_SUCCESS'
        """).result())
        assert len(job_control_rows) == 1
        job_entry = job_control_rows[0]
        assert job_entry.job_kennung == 'TEST_JOB_SUCCESS'
        assert job_entry.status == 'OK'
        assert job_entry.program_name == 'r_ausd_v_ta_cntrct_crs'
        assert job_entry.program_version == '1.0'
        assert job_entry.start_time is not None
        assert job_entry.end_time is not None
        assert job_entry.end_time > job_entry.start_time
        assert job_entry.reference_date == bq_client.query("SELECT CURRENT_DATE();").result().to_dataframe().iloc[0,0]

        job_id = job_entry.job_id

        # Assert job_log
        log_messages = list(bq_client.query(f"""
            SELECT log_level, message FROM `project.dataset.job_log` WHERE job_id = {job_id} ORDER BY log_timestamp
        """).result())
        info_messages = [m.message for m in log_messages if m.log_level == 'INFO']
        assert any("r_ausd_v_ta_cntrct_crs (Version 1.0) started for JobKennung: TEST_JOB_SUCCESS" in msg for msg in info_messages)
        assert any("Reference Date (Stichtag):" in msg for msg in info_messages)
        assert any("Invoking core script: k_ausd_v_ta_cntrct_crs" in msg for msg in info_messages)
        assert any("Core script k_ausd_v_ta_cntrct_crs started" in msg for msg in info_messages)
        assert any("Simulating data processing steps" in msg for msg in info_messages)
        assert any("Core script k_ausd_v_ta_cntrct_crs completed successfully" in msg for msg in info_messages)
        assert any("r_ausd_v_ta_cntrct_crs completed successfully" in msg for msg in info_messages)
        assert not any(m.log_level == 'ERROR' for m in log_messages)

        # Assert job_error_log
        error_log_rows = list(bq_client.query(f"SELECT * FROM `project.dataset.job_error_log` WHERE job_id = {job_id};").result())
        assert len(error_log_rows) == 0
    ```

---

## Test Case 2: Help Message Display

*   **Purpose**: Verify that calling the procedure with `p_show_help = TRUE` displays the usage information and exits without performing any job processing or logging, mimicking the legacy script's `-h` option. This covers output parity and external system replacement (parameter handling).
*   **Setup**:
    1.  Clear `project.dataset.job_control`, `project.dataset.job_log`, `project.dataset.job_error_log` tables.
*   **Action**:
    Execute the wrapper procedure with `p_show_help` set to `TRUE`.
    ```sql
    -- This call is expected to return results directly, not modify tables.
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'DUMMY_HELP_CALL', p_show_help => TRUE);
    ```
*   **Pass/Fail Criterion**:
    1.  The query result should contain rows describing the usage and parameters (e.g., "Usage: CALL...", "p_job_kennung STRING...", "p_show_help BOOL...").
    2.  **`project.dataset.job_control` table**: Zero rows.
    3.  **`project.dataset.job_log` table**: Zero rows.
    4.  **`project.dataset.job_error_log` table**: Zero rows.

    ```python
    # Example pytest-style assertion
    def test_help_message_display(bq_client):
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`;").result()

        # Execute the call and capture results
        results = list(bq_client.query("CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'DUMMY_HELP_CALL', p_show_help => TRUE);").result())

        # Assert help messages are present
        assert any("Usage: CALL `project.dataset.vertragsdatenabgleich_wrapper`" in str(row) for row in results)
        assert any("p_job_kennung STRING: Job identifier" in str(row) for row in results)
        assert any("p_show_help BOOL: Display this help message" in str(row) for row in results)

        # Assert no table modifications
        assert bq_client.query("SELECT COUNT(1) FROM `project.dataset.job_control`;").result().to_dataframe().iloc[0,0] == 0
        assert bq_client.query("SELECT COUNT(1) FROM `project.dataset.job_log`;").result().to_dataframe().iloc[0,0] == 0
        assert bq_client.query("SELECT COUNT(1) FROM `project.dataset.job_error_log`;").result().to_dataframe().iloc[0,0] == 0
    ```

---

## Test Case 3: Missing Mandatory `p_job_kennung` Parameter

*   **Purpose**: Verify that the procedure correctly identifies a missing or empty `p_job_kennung`, logs an error to `job_error_log`, and raises a fatal error, preventing further processing, similar to how the legacy script would exit on parameter errors. This covers transformation correctness (NULL handling, parameter validation) and error handling.
*   **Setup**:
    1.  Clear `project.dataset.job_control`, `project.dataset.job_log`, `project.dataset.job_error_log` tables.
*   **Action**:
    Execute the wrapper procedure with `p_job_kennung` set to `NULL` or an empty string. This call is expected to fail.
    ```sql
    -- This call is expected to fail and raise an error.
    BEGIN
        CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => NULL);
    EXCEPTION WHEN ERROR THEN
        -- Expected error, do nothing or log for test framework
        SELECT 'Caught expected error for missing p_job_kennung' AS status;
    END;
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement should raise an error with a message containing "FATAL ERROR: JobKennung is mandatory."
    2.  **`project.dataset.job_control` table**: Zero rows (as the error occurs before the initial insert).
    3.  **`project.dataset.job_log` table**: Zero rows.
    4.  **`project.dataset.job_error_log` table**:
        *   Exactly one row is inserted.
        *   `job_id` is `0` (since `DW_EintragsNr` is not yet determined).
        *   `error_code` is 'PARAM_ERROR'.
        *   `error_message` is 'JobKennung is mandatory.'.
        *   `error_details` is 'Parameter `p_job_kennung` was not provided or was empty.'.

    ```python
    # Example pytest-style assertion
    import pytest

    def test_missing_mandatory_job_kennung(bq_client):
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`;").result()

        with pytest.raises(Exception) as excinfo:
            bq_client.query("CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => NULL);").result()
        assert "FATAL ERROR: JobKennung is mandatory." in str(excinfo.value)

        # Assert no table modifications for job_control and job_log
        assert bq_client.query("SELECT COUNT(1) FROM `project.dataset.job_control`;").result().to_dataframe().iloc[0,0] == 0
        assert bq_client.query("SELECT COUNT(1) FROM `project.dataset.job_log`;").result().to_dataframe().iloc[0,0] == 0

        # Assert job_error_log entry
        error_log_rows = list(bq_client.query("SELECT job_id, error_code, error_message, error_details FROM `project.dataset.job_error_log`;").result())
        assert len(error_log_rows) == 1
        error_entry = error_log_rows[0]
        assert error_entry.job_id == 0
        assert error_entry.error_code == 'PARAM_ERROR'
        assert error_entry.error_message == 'JobKennung is mandatory.'
        assert error_entry.error_details == 'Parameter `p_job_kennung` was not provided or was empty.'
    ```

---

## Test Case 4: Core Script Failure Handling

*   **Purpose**: Verify that if the invoked `project.dataset.k_ausd_v_ta_cntrct_crs` procedure fails, the wrapper correctly catches the error, logs it to `job_log` and `job_error_log`, updates the `job_control` status to 'ERROR', and re-raises the error to signal failure, mirroring the legacy script's `trap ERR` behavior. This covers error handling and external system replacement (core script invocation).
*   **Setup**:
    1.  Clear `project.dataset.job_control`, `project.dataset.job_log`, `project.dataset.job_error_log` tables.
    2.  Temporarily modify `project.dataset.k_ausd_v_ta_cntrct_crs` to simulate a failure when a specific `p_job_kennung` is passed.
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_cntrct_crs`(
            p_job_kennung STRING,
            p_dw_eintrags_nr INT64
        )
        BEGIN
            INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
            VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Core script k_ausd_v_ta_cntrct_crs started for JobKennung: %s, DW_EintragsNr: %d', p_job_kennung, p_dw_eintrags_nr));

            IF p_job_kennung = 'TEST_JOB_FAIL_CORE' THEN
                RAISE_ERROR('Simulated error during core processing for JobKennung: ' || p_job_kennung);
            END IF;

            INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
            VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Core script k_ausd_v_ta_cntrct_crs completed successfully.');
        END;
        ```
*   **Action**:
    Execute the wrapper procedure with the `job_kennung` that triggers the core script failure. This call is expected to fail.
    ```sql
    -- This call is expected to fail and raise an error.
    BEGIN
        CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_JOB_FAIL_CORE');
    EXCEPTION WHEN ERROR THEN
        SELECT 'Caught expected error for core script failure' AS status;
    END;
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement should raise an error with a message like "JOB FAILED: r_ausd_v_ta_cntrct_crs for JobKennung: TEST_JOB_FAIL_CORE. Details: Simulated error during core processing...".
    2.  **`project.dataset.job_control` table**:
        *   Exactly one row is inserted.
        *   `job_kennung` is 'TEST_JOB_FAIL_CORE'.
        *   `status` is 'ERROR'.
        *   `end_time` is populated.
    3.  **`project.dataset.job_log` table**:
        *   Contains `INFO` messages for job start, reference date, and core script invocation.
        *   Contains an `ERROR` level message indicating the failure of `r_ausd_v_ta_cntrct_crs` and the error details.
        *   All `job_id`s match the `job_id` from `job_control`.
    4.  **`project.dataset.job_error_log` table**:
        *   Exactly one row is inserted.
        *   `job_id` matches the `job_id` from `job_control`.
        *   `error_code` is populated (e.g., 'BQ_EXCEPTION' or similar BigQuery error code).
        *   `error_message` contains "Simulated error during core processing...".
*   **Cleanup**: Revert `project.dataset.k_ausd_v_ta_cntrct_crs` to its original stub definition (without the `RAISE_ERROR` condition).

    ```python
    # Example pytest-style assertion
    import pytest

    def test_core_script_failure_handling(bq_client):
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`;").result()

        # Deploy failing stub for k_ausd_v_ta_cntrct_crs
        bq_client.query("""
            CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_cntrct_crs`(
                p_job_kennung STRING, p_dw_eintrags_nr INT64
            ) BEGIN
                INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
                VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Core script started.');
                IF p_job_kennung = 'TEST_JOB_FAIL_CORE' THEN
                    RAISE_ERROR('Simulated error during core processing for JobKennung: ' || p_job_kennung);
                END IF;
            END;
        """).result()

        with pytest.raises(Exception) as excinfo:
            bq_client.query("CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_JOB_FAIL_CORE');").result()
        assert "JOB FAILED: r_ausd_v_ta_cntrct_crs for JobKennung: TEST_JOB_FAIL_CORE. Details: Simulated error during core processing" in str(excinfo.value)

        # Assert job_control
        job_control_rows = list(bq_client.query("""
            SELECT job_id, job_kennung, status, end_time
            FROM `project.dataset.job_control`
            WHERE job_kennung = 'TEST_JOB_FAIL_CORE'
        """).result())
        assert len(job_control_rows) == 1
        job_entry = job_control_rows[0]
        assert job_entry.status == 'ERROR'
        assert job_entry.end_time is not None
        job_id = job_entry.job_id

        # Assert job_log
        log_messages = list(bq_client.query(f"""
            SELECT log_level, message FROM `project.dataset.job_log` WHERE job_id = {job_id} ORDER BY log_timestamp
        """).result())
        assert any("r_ausd_v_ta_cntrct_crs (Version 1.0) started for JobKennung: TEST_JOB_FAIL_CORE" in msg.message for msg in log_messages)
        assert any(msg.log_level == 'ERROR' and "r_ausd_v_ta_cntrct_crs failed for JobKennung: TEST_JOB_FAIL_CORE" in msg.message for msg in log_messages)

        # Assert job_error_log
        error_log_rows = list(bq_client.query(f"SELECT error_code, error_message FROM `project.dataset.job_error_log` WHERE job_id = {job_id};").result())
        assert len(error_log_rows) == 1
        error_entry = error_log_rows[0]
        assert "Simulated error during core processing" in error_entry.error_message

        # Revert k_ausd_v_ta_cntrct_crs to original stub
        bq_client.query("""
            CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_cntrct_crs`(
                p_job_kennung STRING, p_dw_eintrags_nr INT64
            ) BEGIN
                INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
                VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Core script k_ausd_v_ta_cntrct_crs started for JobKennung: %s, DW_EintragsNr: %d', p_job_kennung, p_dw_eintrags_nr));
                INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
                VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Simulating data processing steps within k_ausd_v_ta_cntrct_crs...');
                INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
                VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Core script k_ausd_v_ta_cntrct_crs completed successfully.');
            END;
        """).result()
    ```

---

## Test Case 5: `DW_EintragsNr` (Job ID) Generation

*   **Purpose**: Verify that `DW_EintragsNr` (mapped to `job_id`) is correctly generated as a monotonically increasing sequence based on existing entries in `job_control`, including the case where the table is initially empty. This covers transformation correctness and data quality.
*   **Setup**:
    1.  Clear `project.dataset.job_control`, `project.dataset.job_log`, `project.dataset.job_error_log` tables.
    2.  Insert some dummy `job_control` entries to establish a sequence, including a gap.
        ```sql
        INSERT INTO `project.dataset.job_control` (job_id, job_kennung, program_name, program_version, start_time, status)
        VALUES
            (1, 'DUMMY_JOB_1', 'dummy_prog', '1.0', CURRENT_TIMESTAMP(), 'OK'),
            (5, 'DUMMY_JOB_2', 'dummy_prog', '1.0', CURRENT_TIMESTAMP(), 'OK'),
            (10, 'DUMMY_JOB_3', 'dummy_prog', '1.0', CURRENT_TIMESTAMP(), 'OK');
        ```
*   **Action**:
    Execute the wrapper procedure twice with different `job_kennung` values.
    ```sql
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_JOB_SEQUENCE_1');
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_JOB_SEQUENCE_2');
    ```
*   **Pass/Fail Criterion**:
    1.  The `job_id` for 'TEST_JOB_SEQUENCE_1' in `job_control` should be `11` (MAX(10) + 1).
    2.  The `job_id` for 'TEST_JOB_SEQUENCE_2' in `job_control` should be `12` (MAX(11) + 1).
    3.  Both jobs should complete successfully (`status = 'OK'`).

    ```python
    # Example pytest-style assertion
    def test_job_id_generation(bq_client):
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`;").result()

        # Insert dummy data to establish sequence
        bq_client.query("""
            INSERT INTO `project.dataset.job_control` (job_id, job_kennung, program_name, program_version, start_time, status)
            VALUES
                (1, 'DUMMY_JOB_1', 'dummy_prog', '1.0', CURRENT_TIMESTAMP(), 'OK'),
                (5, 'DUMMY_JOB_2', 'dummy_prog', '1.0', CURRENT_TIMESTAMP(), 'OK'),
                (10, 'DUMMY_JOB_3', 'dummy_prog', '1.0', CURRENT_TIMESTAMP(), 'OK');
        """).result()

        bq_client.query("CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_JOB_SEQUENCE_1');").result()
        bq_client.query("CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_JOB_SEQUENCE_2');").result()

        job_id_1 = bq_client.query("SELECT job_id FROM `project.dataset.job_control` WHERE job_kennung = 'TEST_JOB_SEQUENCE_1';").result().to_dataframe().iloc[0,0]
        job_id_2 = bq_client.query("SELECT job_id FROM `project.dataset.job_control` WHERE job_kennung = 'TEST_JOB_SEQUENCE_2';").result().to_dataframe().iloc[0,0]
        status_1 = bq_client.query("SELECT status FROM `project.dataset.job_control` WHERE job_kennung = 'TEST_JOB_SEQUENCE_1';").result().to_dataframe().iloc[0,0]
        status_2 = bq_client.query("SELECT status FROM `project.dataset.job_control` WHERE job_kennung = 'TEST_JOB_SEQUENCE_2';").result().to_dataframe().iloc[0,0]

        assert job_id_1 == 11
        assert job_id_2 == 12
        assert status_1 == 'OK'
        assert status_2 == 'OK'
    ```

---

## Test Case 6: Unused Parameters (`-s`, `-l`) Handling

*   **Purpose**: Verify that the procedure accepts the placeholder parameters `p_some_param_s` and `p_some_param_l` without error, and they do not interfere with the main logic or logging, mirroring the legacy script's behavior where these `getopts` parameters were declared but not used. This covers transformation correctness (parameter handling) and output parity.
*   **Setup**:
    1.  Clear `project.dataset.job_control`, `project.dataset.job_log`, `project.dataset.job_error_log` tables.
*   **Action**:
    Execute the wrapper procedure, providing values for the unused parameters.
    ```sql
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(
        p_job_kennung => 'TEST_JOB_WITH_UNUSED_PARAMS',
        p_some_param_s => 'value_s',
        p_some_param_l => 'value_l'
    );
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure executes successfully (`status = 'OK'` in `job_control`).
    2.  **`project.dataset.job_control` table**: One row inserted with `status = 'OK'`.
    3.  **`project.dataset.job_log` table**: Contains expected success messages, and the values 'value_s' or 'value_l' do not appear in any critical log messages (confirming they are not actively processed).
    4.  **`project.dataset.job_error_log` table**: Zero rows.

    ```python
    # Example pytest-style assertion
    def test_unused_parameters_handling(bq_client):
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`;").result()

        bq_client.query("""
            CALL `project.dataset.vertragsdatenabgleich_wrapper`(
                p_job_kennung => 'TEST_JOB_WITH_UNUSED_PARAMS',
                p_some_param_s => 'value_s',
                p_some_param_l => 'value_l'
            );
        """).result()

        # Assert job_control status
        job_control_status = bq_client.query("SELECT status FROM `project.dataset.job_control` WHERE job_kennung = 'TEST_JOB_WITH_UNUSED_PARAMS';").result().to_dataframe().iloc[0,0]
        assert job_control_status == 'OK'

        # Assert no error logs
        job_id = bq_client.query("SELECT job_id FROM `project.dataset.job_control` WHERE job_kennung = 'TEST_JOB_WITH_UNUSED_PARAMS';").result().to_dataframe().iloc[0,0]
        error_log_count = bq_client.query(f"SELECT COUNT(1) FROM `project.dataset.job_error_log` WHERE job_id = {job_id};").result().to_dataframe().iloc[0,0]
        assert error_log_count == 0

        # Assert unused parameter values do not appear in logs (critical check)
        log_messages = list(bq_client.query(f"SELECT message FROM `project.dataset.job_log` WHERE job_id = {job_id};").result())
        for msg in log_messages:
            assert 'value_s' not in msg.message
            assert 'value_l' not in msg.message
    ```

---

## Test Case 7: Data Quality and Schema Assertions for Logging Tables

*   **Purpose**: Verify that the DDL for `job_control`, `job_log`, and `job_error_log` tables is correct, including data types and nullability, and that data inserted conforms to these schemas. This covers data quality and schema assertions.
*   **Setup**:
    1.  Ensure DDLs for `job_control`, `job_log`, `job_error_log` are applied.
    2.  Execute a successful job and a failing job to populate all tables with diverse data.
        ```sql
        -- Ensure k_ausd_v_ta_cntrct_crs is set to fail for 'DQ_TEST_FAIL_CORE'
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_cntrct_crs`(p_job_kennung STRING, p_dw_eintrags_nr INT64)
        BEGIN
            IF p_job_kennung = 'DQ_TEST_FAIL_CORE' THEN RAISE_ERROR('Simulated DQ failure'); END IF;
            INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message) VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Core script completed.');
        END;

        CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'DQ_TEST_SUCCESS');
        BEGIN
            CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'DQ_TEST_FAIL_CORE');
        EXCEPTION WHEN ERROR THEN END;

        -- Revert k_ausd_v_ta_cntrct_crs
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_cntrct_crs`(p_job_kennung STRING, p_dw_eintrags_nr INT64)
        BEGIN
            INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message) VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Core script k_ausd_v_ta_cntrct_crs started for JobKennung: %s, DW_EintragsNr: %d', p_job_kennung, p_dw_eintrags_nr));
            INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message) VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Simulating data processing steps within k_ausd_v_ta_cntrct_crs...');
            INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message) VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Core script k_ausd_v_ta_cntrct_crs completed successfully.');
        END;
        ```
*   **Action**:
    Query BigQuery's `INFORMATION_SCHEMA` and perform data validation queries on the populated tables.
*   **Pass/Fail Criterion**:
    1.  **`project.dataset.job_control` schema**:
        *   `job_id`: `INT64`, `NOT NULL`.
        *   `job_kennung`: `STRING`, `NOT NULL`.
        *   `program_name`: `STRING`, `NOT NULL`.
        *   `program_version`: `STRING`, `NOT NULL`.
        *   `start_time`: `TIMESTAMP`, `NOT NULL`.
        *   `end_time`: `TIMESTAMP`, `NULLABLE`.
        *   `status`: `STRING`, `NOT NULL`.
        *   `log_file_name`: `STRING`, `NULLABLE`.
        *   `reference_date`: `DATE`, `NULLABLE`.
    2.  **`project.dataset.job_log` schema**:
        *   `job_id`: `INT64`, `NOT NULL`.
        *   `log_timestamp`: `TIMESTAMP`, `NOT NULL`.
        *   `log_level`: `STRING`, `NOT NULL`.
        *   `message`: `STRING`, `NOT NULL`.
    3.  **`project.dataset.job_error_log` schema**:
        *   `job_id`: `INT64`, `NOT NULL`.
        *   `error_timestamp`: `TIMESTAMP`, `NOT NULL`.
        *   `error_code`: `STRING`, `NULLABLE`.
        *   `error_message`: `STRING`, `NOT NULL`.
        *   `error_details`: `STRING`, `NULLABLE`.
    4.  **Data Quality**:
        *   No `NULL` values in any `NOT NULL` columns across all three tables.
        *   `status` column in `job_control` contains only 'RUNNING', 'OK', 'ERROR'.
        *   `log_level` column in `job_log` contains only 'INFO', 'WARNING', 'ERROR'.

    ```python
    # Example pytest-style assertion
    def test_data_quality_and_schema(bq_client):
        # Setup (as described above)
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`;").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`;").result()

        bq_client.query("""
            CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_cntrct_crs`(p_job_kennung STRING, p_dw_eintrags_nr INT64)
            BEGIN
                IF p_job_kennung = 'DQ_TEST_FAIL_CORE' THEN RAISE_ERROR('Simulated DQ failure'); END IF;
                INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message) VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Core script completed.');
            END;
        """).result()

        bq_client.query("CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'DQ_TEST_SUCCESS');").result()
        try:
            bq_client.query("CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'DQ_TEST_FAIL_CORE');").result()
        except Exception:
            pass # Expected error

        bq_client.query("""
            CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_cntrct_crs`(p_job_kennung STRING, p_dw_eintrags_nr INT64)
            BEGIN
                INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message) VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Core script k_ausd_v_ta_cntrct_crs started for JobKennung: %s, DW_EintragsNr: %d', p_job_kennung, p_dw_eintrags_nr));
                INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message) VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Simulating data processing steps within k_ausd_v_ta_cntrct_crs...');
                INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message) VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', 'Core script k_ausd_v_ta_cntrct_crs completed successfully.');
            END;
        """).result()

        # Assert job_control schema
        job_control_schema = list(bq_client.query("""
            SELECT column_name, data_type, is_nullable FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'job_control' ORDER BY ordinal_position;
        """).result())
        expected_job_control_schema = [
            ('job_id', 'INT64', 'NO'), ('job_kennung', 'STRING', 'NO'), ('program_name', 'STRING', 'NO'),
            ('program_version', 'STRING', 'NO'), ('start_time', 'TIMESTAMP', 'NO'), ('end_time', 'TIMESTAMP', 'YES'),
            ('status', 'STRING', 'NO'), ('log_file_name', 'STRING', 'YES'), ('reference_date', 'DATE', 'YES')
        ]
        for i, (col_name, data_type, is_nullable) in enumerate(expected_job_control_schema):
            assert job_control_schema[i].column_name == col_name
            assert job_control_schema[i].data_type == data_type
            assert job_control_schema[i].is_nullable == is_nullable

        # Assert job_log schema
        job_log_schema = list(bq_client.query("""
            SELECT column_name, data_type, is_nullable FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'job_log' ORDER BY ordinal_position;
        """).result())
        expected_job_log_schema = [
            ('job_id', 'INT64', 'NO'), ('log_timestamp', 'TIMESTAMP', 'NO'),
            ('log_level', 'STRING', 'NO'), ('message', 'STRING', 'NO')
        ]
        for i, (col_name, data_type, is_nullable) in enumerate(expected_job_log_schema):
            assert job_log_schema[i].column_name == col_name
            assert job_log_schema[i].data_type == data_type
            assert job_log_schema[i].is_nullable == is_nullable

        # Assert job_error_log schema
        job_error_log_schema = list(bq_client.query("""
            SELECT column_name, data_type, is_nullable FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'job_error_log' ORDER BY ordinal_position;
        """).result())
        expected_job_error_log_schema = [
            ('job_id', 'INT64', 'NO'), ('error_timestamp', 'TIMESTAMP', 'NO'),
            ('error_code', 'STRING', 'YES'), ('error_message', 'STRING', 'NO'),
            ('error_details', 'STRING', 'YES')
        ]
        for i, (col_name, data_type, is_nullable) in enumerate(expected_job_error_log_schema):
            assert job_error_log_schema[i].column_name == col_name
            assert job_error_log_schema[i].data_type == data_type
            assert job_error_log_schema[i].is_nullable == is_nullable

        # Assert data quality (no nulls in NOT NULL columns, valid enum values)
        dq_checks = bq_client.query("""
            SELECT
                (SELECT COUNTIF(job_id IS NULL OR job_kennung IS NULL OR program_name IS NULL OR program_version IS NULL OR start_time IS NULL OR status IS NULL) FROM `project.dataset.job_control`) AS job_control_null_violations,
                (SELECT COUNTIF(status NOT IN ('RUNNING', 'OK', 'ERROR')) FROM `project.dataset.job_control`) AS job_control_invalid_status,
                (SELECT COUNTIF(job_id IS NULL OR log_timestamp IS NULL OR log_level IS NULL OR message IS NULL) FROM `project.dataset.job_log`) AS job_log_null_violations,
                (SELECT COUNTIF(log_level NOT IN ('INFO', 'WARNING', 'ERROR')) FROM `project.dataset.job_log`) AS job_log_invalid_level,
                (SELECT COUNTIF(job_id IS NULL OR error_timestamp IS NULL OR error_message IS NULL) FROM `project.dataset.job_error_log`) AS job_error_log_null_violations
        """).result().to_dataframe().iloc[0]

        assert dq_checks.job_control_null_violations == 0
        assert dq_checks.job_control_invalid_status == 0
        assert dq_checks.job_log_null_violations == 0
        assert dq_checks.job_log_invalid_level == 0
        assert dq_checks.job_error_log_null_violations == 0
    ```