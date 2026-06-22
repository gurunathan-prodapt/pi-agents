As a senior data-migration QA engineer, I have designed a suite of validation tests for the migrated BigQuery Stored Procedure `project.dataset.BERT_V_TA_CNTRCT_CRS2`. These tests aim to ensure behavioral equivalence with the legacy KornShell script `r_ausd_v_ta_cntrct_crs2.ksh`, covering output parity, transformation correctness of the orchestration logic, external system interactions (logging/control tables, core script invocation), and data quality assertions.

The core processing script `k_ausd_v_ta_cntrct_crs2.ksh` is a placeholder in the migration design. For these tests, we will treat its migrated BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_cntrct_crs2` as a black box that can either succeed or fail, allowing us to focus on the wrapper's orchestration logic.

---

## Pre-requisites for all Tests

Before running any tests, ensure the following BigQuery objects exist and are accessible:

1.  **Logging and Control Tables DDL:**
    ```sql
    CREATE TABLE IF NOT EXISTS project.dataset.job_log (
        job_kennung STRING,
        eintrags_nr INT64,
        log_level STRING,
        message STRING,
        created_ts TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
        job_kennung STRING,
        eintrags_nr INT64,
        error_nr INT64,
        error_arg STRING,
        error_message STRING,
        created_ts TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS project.dataset.job_control (
        eintrags_nr INT64,
        job_kennung STRING,
        script_name STRING,
        log_name STRING,
        stichtag_info DATE,
        status STRING,
        created_ts TIMESTAMP,
        finished_ts TIMESTAMP
    );
    ```

2.  **Core Processing Stored Procedure (Placeholder):**
    The `project.dataset.k_ausd_v_ta_cntrct_crs2` stored procedure must exist. For testing purposes, we will use the provided placeholder, and for error scenarios, we will temporarily modify it.

    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_v_ta_cntrct_crs2(
        IN p_job_kennung STRING,
        IN p_eintrags_nr INT64
    )
    BEGIN
        INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
        VALUES (p_job_kennung, p_eintrags_nr, 'INFO', 'k_ausd_v_ta_cntrct_crs2: Core processing started.', CURRENT_TIMESTAMP());

        -- Placeholder for actual data transformation and loading logic.
        -- For example:
        -- INSERT INTO project.dataset.ta_cntrct_crs2 (...)
        -- SELECT ...
        -- FROM ...;

        INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
        VALUES (p_job_kennung, p_eintrags_nr, 'INFO', 'k_ausd_v_ta_cntrct_crs2: Core processing completed successfully.', CURRENT_TIMESTAMP());
    END;
    ```

3.  **Wrapper Stored Procedure:**
    The `project.dataset.BERT_V_TA_CNTRCT_CRS2` stored procedure must exist as provided in the migration design.

---

## Test Case 1: Help Message Display (`-h` parameter)

*   **Purpose**: Verify that calling the stored procedure with `p_h => TRUE` correctly displays the usage message and exits without performing any job processing or logging. This directly maps to the legacy script's `-h` behavior.
*   **Setup**:
    *   Ensure all logging and control tables are empty.
    ```sql
    TRUNCATE TABLE project.dataset.job_log;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_control;
    ```
*   **Action**:
    *   Execute the BigQuery Stored Procedure with the help flag.
    ```sql
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => TRUE, p_s => NULL, p_l => NULL);
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The query output (results pane in BigQuery UI or client output) contains the expected help messages. No rows are inserted into `project.dataset.job_log`, `project.dataset.job_error_log`, or `project.dataset.job_control`.
    *   **Fail**: No help message is displayed, or any rows are found in the logging/control tables.

    ```sql
    -- Assertion for no log entries
    SELECT COUNT(*) FROM project.dataset.job_log WHERE TRUE; -- Expected: 0
    SELECT COUNT(*) FROM project.dataset.job_error_log WHERE TRUE; -- Expected: 0
    SELECT COUNT(*) FROM project.dataset.job_control WHERE TRUE; -- Expected: 0
    ```

---

## Test Case 2: Missing Required Parameter (`-s` parameter)

*   **Purpose**: Verify that the stored procedure correctly identifies a missing required parameter (`p_s`), logs an error, and signals an error, mimicking the legacy script's `getopts` error handling.
*   **Setup**:
    *   Ensure all logging and control tables are empty.
    ```sql
    TRUNCATE TABLE project.dataset.job_log;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_control;
    ```
*   **Action**:
    *   Execute the BigQuery Stored Procedure without providing the `p_s` parameter (or setting it to `NULL`).
    ```sql
    -- This call is expected to fail and signal an error
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => NULL, p_l => NULL);
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The `CALL` statement fails with an error message containing "Missing required parameter: -s" (due to `SIGNAL SQLSTATE`).
        *   One row is inserted into `project.dataset.job_error_log` with `error_nr = 1` and `error_arg = '-s (Stichtag) parameter is missing.'`.
        *   No rows are inserted into `project.dataset.job_log` or `project.dataset.job_control` (as the error occurs before job initialization).
    *   **Fail**: The procedure completes successfully, or the error message/log entries are incorrect.

    ```sql
    -- Assertion for error log entry
    SELECT
        job_kennung,
        eintrags_nr,
        error_nr,
        error_arg,
        error_message
    FROM project.dataset.job_error_log
    WHERE error_nr = 1 AND error_arg LIKE '%-s (Stichtag) parameter is missing.%';
    -- Expected: 1 row matching the criteria

    -- Assertion for no other log entries
    SELECT COUNT(*) FROM project.dataset.job_log WHERE TRUE; -- Expected: 0
    SELECT COUNT(*) FROM project.dataset.job_control WHERE TRUE; -- Expected: 0
    ```

---

## Test Case 3: Successful Execution (Happy Path)

*   **Purpose**: Verify the end-to-end successful execution of the wrapper, including correct logging, `job_control` updates, and successful invocation of the core script. This covers output parity for status and logging.
*   **Setup**:
    *   Ensure all logging and control tables are empty.
    *   Ensure `project.dataset.k_ausd_v_ta_cntrct_crs2` is in its default successful state.
    ```sql
    TRUNCATE TABLE project.dataset.job_log;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_control;

    -- Reset k_ausd_v_ta_cntrct_crs2 to its successful placeholder state
    CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_v_ta_cntrct_crs2(
        IN p_job_kennung STRING,
        IN p_eintrags_nr INT64
    )
    BEGIN
        INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
        VALUES (p_job_kennung, p_eintrags_nr, 'INFO', 'k_ausd_v_ta_cntrct_crs2: Core processing started.', CURRENT_TIMESTAMP());
        INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
        VALUES (p_job_kennung, p_eintrags_nr, 'INFO', 'k_ausd_v_ta_cntrct_crs2: Core processing completed successfully.', CURRENT_TIMESTAMP());
    END;
    ```
*   **Action**:
    *   Execute the BigQuery Stored Procedure with valid parameters.
    ```sql
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2023-01-01', p_l => 'my_custom_log.log');
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The `CALL` statement completes successfully.
        *   `project.dataset.job_control` contains exactly one row with:
            *   `eintrags_nr = 1` (first run)
            *   `job_kennung = 'BERT_V_TA_CNTRCT_CRS2'`
            *   `script_name = 'BERT_V_TA_CNTRCT_CRS2'`
            *   `log_name = 'my_custom_log.log'`
            *   `stichtag_info = '2023-01-01'`
            *   `status = 'OK'`
            *   `created_ts` and `finished_ts` are populated.
        *   `project.dataset.job_log` contains at least 5 'INFO' entries for `eintrags_nr = 1`: job start, calling core script, core script start, core script end, job completed successfully.
        *   `project.dataset.job_error_log` contains zero rows.
    *   **Fail**: Any of the above conditions are not met.

    ```sql
    -- Assertion for job_control
    SELECT
        eintrags_nr,
        job_kennung,
        script_name,
        log_name,
        stichtag_info,
        status
    FROM project.dataset.job_control
    WHERE eintrags_nr = 1;
    -- Expected: 1 row with status 'OK', stichtag_info '2023-01-01', log_name 'my_custom_log.log'

    -- Assertion for job_log entries
    SELECT message FROM project.dataset.job_log WHERE eintrags_nr = 1 ORDER BY created_ts;
    -- Expected messages (order might vary slightly for core script messages vs wrapper messages):
    -- 'Job BERT_V_TA_CNTRCT_CRS2 version 1.0 started.'
    -- 'Calling core script: k_ausd_v_ta_cntrct_crs2'
    -- 'k_ausd_v_ta_cntrct_crs2: Core processing started.'
    -- 'k_ausd_v_ta_cntrct_crs2: Core processing completed successfully.'
    -- 'Job BERT_V_TA_CNTRCT_CRS2 completed successfully.'

    -- Assertion for no error log entries
    SELECT COUNT(*) FROM project.dataset.job_error_log WHERE TRUE; -- Expected: 0
    ```

---

## Test Case 4: Core Script Failure Handling

*   **Purpose**: Verify that the wrapper correctly handles errors originating from the invoked core script, logging the error and updating the job status to 'ERROR'. This tests the `BEGIN...EXCEPTION` block and error logging.
*   **Setup**:
    *   Ensure all logging and control tables are empty.
    *   **Temporarily modify `project.dataset.k_ausd_v_ta_cntrct_crs2` to simulate a failure.**
    ```sql
    TRUNCATE TABLE project.dataset.job_log;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_control;

    -- Modify k_ausd_v_ta_cntrct_crs2 to fail
    CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_v_ta_cntrct_crs2(
        IN p_job_kennung STRING,
        IN p_eintrags_nr INT64
    )
    BEGIN
        INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
        VALUES (p_job_kennung, p_eintrags_nr, 'INFO', 'k_ausd_v_ta_cntrct_crs2: Core processing started (will fail).', CURRENT_TIMESTAMP());
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core script failure!';
    END;
    ```
*   **Action**:
    *   Execute the BigQuery Stored Procedure with valid parameters.
    ```sql
    -- This call is expected to fail and signal an error
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2023-01-02', p_l => NULL);
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The `CALL` statement fails with an error message containing "Job BERT_V_TA_CNTRCT_CRS2 failed. Refer to job_error_log." (due to `SIGNAL SQLSTATE` from the wrapper).
        *   `project.dataset.job_control` contains exactly one row with:
            *   `eintrags_nr = 1`
            *   `job_kennung = 'BERT_V_TA_CNTRCT_CRS2'`
            *   `stichtag_info = '2023-01-02'`
            *   `status = 'ERROR'`
            *   `created_ts` and `finished_ts` are populated.
        *   `project.dataset.job_log` contains entries for job start, calling core script, core script start, and an 'ERROR' message indicating job failure.
        *   `project.dataset.job_error_log` contains exactly one row with:
            *   `job_kennung = 'BERT_V_TA_CNTRCT_CRS2'`
            *   `eintrags_nr = 1`
            *   `error_nr` (BigQuery error code)
            *   `error_arg` containing "Simulated core script failure!"
            *   `error_message = 'Job BERT_V_TA_CNTRCT_CRS2 failed.'`
    *   **Fail**: Any of the above conditions are not met.

    ```sql
    -- Assertion for job_control
    SELECT
        eintrags_nr,
        job_kennung,
        stichtag_info,
        status
    FROM project.dataset.job_control
    WHERE eintrags_nr = 1;
    -- Expected: 1 row with status 'ERROR', stichtag_info '2023-01-02'

    -- Assertion for job_log entries
    SELECT log_level, message FROM project.dataset.job_log WHERE eintrags_nr = 1 ORDER BY created_ts;
    -- Expected messages (order might vary slightly):
    -- 'INFO', 'Job BERT_V_TA_CNTRCT_CRS2 version 1.0 started.'
    -- 'INFO', 'Calling core script: k_ausd_v_ta_cntrct_crs2'
    -- 'INFO', 'k_ausd_v_ta_cntrct_crs2: Core processing started (will fail).'
    -- 'ERROR', 'Job BERT_V_TA_CNTRCT_CRS2 failed with error: Simulated core script failure!'

    -- Assertion for job_error_log entry
    SELECT
        job_kennung,
        eintrags_nr,
        error_arg,
        error_message
    FROM project.dataset.job_error_log
    WHERE eintrags_nr = 1 AND error_arg LIKE '%Simulated core script failure%';
    -- Expected: 1 row matching the criteria
    ```
    *Cleanup*: After this test, remember to revert `project.dataset.k_ausd_v_ta_cntrct_crs2` to its successful placeholder state for subsequent tests.

---

## Test Case 5: `DW_EintragsNr` Generation and `job_control` Updates

*   **Purpose**: Verify that `DW_EintragsNr` is correctly generated (incremented) and that `job_control` accurately reflects multiple job runs with different parameters. This tests the sequence generation logic.
*   **Setup**:
    *   Ensure all logging and control tables are empty.
    *   Ensure `project.dataset.k_ausd_v_ta_cntrct_crs2` is in its default successful state.
    ```sql
    TRUNCATE TABLE project.dataset.job_log;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_control;
    -- Ensure k_ausd_v_ta_cntrct_crs2 is successful (see Test Case 3 setup)
    ```
*   **Action**:
    *   Execute the BigQuery Stored Procedure multiple times with varying parameters.
    ```sql
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2023-01-03', p_l => 'run1.log');
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2023-01-04', p_l => 'run2.log');
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2023-01-05', p_l => NULL); -- Test default log name
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   All three `CALL` statements complete successfully.
        *   `project.dataset.job_control` contains exactly three rows.
        *   The `eintrags_nr` values are `1`, `2`, and `3` respectively.
        *   Each row correctly reflects its `stichtag_info` (`2023-01-03`, `2023-01-04`, `2023-01-05`) and `log_name` (`run1.log`, `run2.log`, `bert_v_ta_cntrct_crs2_20230105.log`).
        *   All three rows have `status = 'OK'`.
        *   `project.dataset.job_log` contains 5 'INFO' entries for each `eintrags_nr` (total 15 entries).
        *   `project.dataset.job_error_log` contains zero rows.
    *   **Fail**: Any of the above conditions are not met.

    ```sql
    -- Assertion for job_control entries
    SELECT
        eintrags_nr,
        job_kennung,
        stichtag_info,
        log_name,
        status
    FROM project.dataset.job_control
    ORDER BY eintrags_nr;
    -- Expected: 3 rows, with eintrags_nr 1, 2, 3 and corresponding stichtag_info and log_name. All status 'OK'.

    -- Assertion for total log entries
    SELECT COUNT(*) FROM project.dataset.job_log WHERE TRUE; -- Expected: 15
    SELECT COUNT(*) FROM project.dataset.job_error_log WHERE TRUE; -- Expected: 0
    ```

---

## Test Case 6: Date Handling and `stichtag_info` Format

*   **Purpose**: Verify that the `p_s` parameter (stichtag) is correctly handled as a `DATE` type and stored in `job_control.stichtag_info` in the correct `DATE` format, and that the derived `v_stichtag_formatted` (used for default log name) matches the `YYYYMMDD` format. This tests type handling and date transformation.
*   **Setup**:
    *   Ensure all logging and control tables are empty.
    *   Ensure `project.dataset.k_ausd_v_ta_cntrct_crs2` is in its default successful state.
    ```sql
    TRUNCATE TABLE project.dataset.job_log;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_control;
    -- Ensure k_ausd_v_ta_cntrct_crs2 is successful (see Test Case 3 setup)
    ```
*   **Action**:
    *   Execute the BigQuery Stored Procedure with a specific `p_s` and `p_l => NULL` to trigger the default log name generation.
    ```sql
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2024-02-29', p_l => NULL); -- Leap year date
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The `CALL` statement completes successfully.
        *   `project.dataset.job_control` contains one row with:
            *   `stichtag_info = DATE '2024-02-29'` (ensuring it's stored as a `DATE` type).
            *   `log_name = 'bert_v_ta_cntrct_crs2_20240229.log'` (verifying `YYYYMMDD` format for the derived part).
        *   `project.dataset.job_log` and `project.dataset.job_error_log` are consistent with a successful run.
    *   **Fail**: The `stichtag_info` or `log_name` values are incorrect, or the procedure fails.

    ```sql
    -- Assertion for job_control date handling
    SELECT
        stichtag_info,
        log_name,
        FORMAT_DATE('%Y-%m-%d', stichtag_info) AS stichtag_info_format_check
    FROM project.dataset.job_control
    WHERE eintrags_nr = 1;
    -- Expected:
    -- stichtag_info = '2024-02-29' (as DATE type)
    -- log_name = 'bert_v_ta_cntrct_crs2_20240229.log'
    -- stichtag_info_format_check = '2024-02-29'
    ```

---

## Test Case 7: Robustness to Invalid Date Format for `p_s`

*   **Purpose**: Verify that the BigQuery Stored Procedure correctly handles invalid date formats passed to `p_s`, leveraging BigQuery's native type checking.
*   **Setup**:
    *   Ensure all logging and control tables are empty.
    ```sql
    TRUNCATE TABLE project.dataset.job_log;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_control;
    ```
*   **Action**:
    *   Attempt to call the procedure with a string that cannot be cast to a `DATE` for `p_s`.
    ```sql
    -- This call is expected to fail at the BigQuery engine level due to type mismatch
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '29/02/2024', p_l => NULL);
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The `CALL` statement fails immediately with a BigQuery error indicating a type conversion issue (e.g., "Bad date: '29/02/2024'"). No entries are made in `job_log`, `job_error_log`, or `job_control` by the stored procedure itself, as the error occurs before the procedure's logic can execute.
    *   **Fail**: The procedure attempts to process the invalid date, or logs an incorrect error message, or completes successfully.

    ```sql
    -- Assertion for no log entries (as error is pre-execution)
    SELECT COUNT(*) FROM project.dataset.job_log WHERE TRUE; -- Expected: 0
    SELECT COUNT(*) FROM project.dataset.job_error_log WHERE TRUE; -- Expected: 0
    SELECT COUNT(*) FROM project.dataset.job_control WHERE TRUE; -- Expected: 0
    ```

---

## Test Case 8: `v_ProgVersion` and `v_ProgName` Logging

*   **Purpose**: Verify that the `v_ProgName` and `v_ProgVersion` variables are correctly initialized and used in the initial log message, ensuring output parity for job identification.
*   **Setup**:
    *   Ensure all logging and control tables are empty.
    *   Ensure `project.dataset.k_ausd_v_ta_cntrct_crs2` is in its default successful state.
    ```sql
    TRUNCATE TABLE project.dataset.job_log;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_control;
    -- Ensure k_ausd_v_ta_cntrct_crs2 is successful (see Test Case 3 setup)
    ```
*   **Action**:
    *   Execute the BigQuery Stored Procedure.
    ```sql
    CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2023-03-15', p_l => NULL);
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The `CALL` statement completes successfully.
        *   `project.dataset.job_log` contains an 'INFO' entry for the job start message that includes "Job BERT_V_TA_CNTRCT_CRS2 version 1.0 started.".
    *   **Fail**: The initial log message is missing or contains incorrect program name or version.

    ```sql
    -- Assertion for initial log message
    SELECT message
    FROM project.dataset.job_log
    WHERE eintrags_nr = 1 AND log_level = 'INFO'
    ORDER BY created_ts
    LIMIT 1;
    -- Expected: 'Job BERT_V_TA_CNTRCT_CRS2 version 1.0 started.'
    ```