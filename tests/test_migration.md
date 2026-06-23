As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `k_ausd_v_ta_discount.ksh` to a Google BigQuery Stored Procedure `r_ausd_v_ta_discount`. These tests focus on ensuring behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

The tests assume the BigQuery DDLs for `job_table`, `job_error_log`, and `job_run_control`, as well as the BigQuery Stored Procedures `d_ausd_v_ta_discount` (placeholder) and `r_ausd_v_ta_discount`, have been deployed to `your_project.your_dataset`.

---

## Migration Validation Tests: `k_ausd_v_ta_discount.ksh` to BigQuery

### Test 1: Successful First Execution

*   **Purpose**: Verify that the `r_ausd_v_ta_discount` procedure executes successfully for a new job, registers a new active job entry, calls the core SQL logic (`d_ausd_v_ta_discount`), and then correctly deactivates the job, logging the processed record count. This covers output parity and basic transformation correctness.
*   **Setup**:
    *   Ensure `your_project.your_dataset.job_table`, `your_project.your_dataset.job_error_log`, and `your_project.your_dataset.job_run_control` are empty.
    *   The `d_ausd_v_ta_discount` procedure should be in its default, successful placeholder state (e.g., returning a random positive integer for `records_processed`).
    ```sql
    -- Cleanup previous test data
    TRUNCATE TABLE `your_project.your_dataset.job_table`;
    TRUNCATE TABLE `your_project.your_dataset.job_error_log`;
    TRUNCATE TABLE `your_project.your_dataset.job_run_control`;
    ```
*   **Action**:
    Call the migrated BigQuery Stored Procedure with valid, unique parameters.
    ```sql
    CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_1', '001');
    ```
*   **Pass/Fail Criterion**:
    *   **`job_table` assertions**:
        *   One row exists for `job_kennung = 'TEST_JOB_1'` and `eintrags_nr = '001'`.
        *   `active_flag` is `FALSE`.
        *   `start_ts` and `end_ts` are populated, and `end_ts > start_ts`.
        *   `script_name` is `'k_ausd_v_ta_discount.ksh_wrapper'`.
    *   **`job_error_log` assertions**:
        *   The table is empty.
    *   **`job_run_control` assertions**:
        *   One row exists for `job_kennung = 'TEST_JOB_1'` and `eintrags_nr = '001'` with `script_name = 'k_ausd_v_ta_discount.ksh_wrapper'`.
        *   `records_processed` is a positive integer (from the `d_ausd_v_ta_discount` placeholder).
        *   `update_ts` is populated.
        *   (Optionally, verify a second entry in `job_run_control` from `d_ausd_v_ta_discount` itself, if its placeholder logs directly).

    ```sql
    -- Pytest-style assertion (conceptual)
    def test_successful_first_execution():
        # Setup (TRUNCATE statements as above)
        # Action (CALL statement as above)

        # Assertions
        job_table_results = bq_client.query("""
            SELECT job_kennung, eintrags_nr, active_flag, start_ts, end_ts, script_name
            FROM `your_project.your_dataset.job_table`
            WHERE job_kennung = 'TEST_JOB_1' AND eintrags_nr = '001'
        """).to_dataframe()

        assert len(job_table_results) == 1
        assert not job_table_results.iloc[0]['active_flag']
        assert job_table_results.iloc[0]['start_ts'] is not None
        assert job_table_results.iloc[0]['end_ts'] is not None
        assert job_table_results.iloc[0]['end_ts'] > job_table_results.iloc[0]['start_ts']
        assert job_table_results.iloc[0]['script_name'] == 'k_ausd_v_ta_discount.ksh_wrapper'

        error_log_results = bq_client.query("SELECT * FROM `your_project.your_dataset.job_error_log`").to_dataframe()
        assert len(error_log_results) == 0

        run_control_results = bq_client.query("""
            SELECT job_kennung, eintrags_nr, script_name, records_processed, update_ts
            FROM `your_project.your_dataset.job_run_control`
            WHERE job_kennung = 'TEST_JOB_1' AND eintrags_nr = '001' AND script_name = 'k_ausd_v_ta_discount.ksh_wrapper'
        """).to_dataframe()

        assert len(run_control_results) == 1
        assert run_control_results.iloc[0]['records_processed'] > 0
        assert run_control_results.iloc[0]['update_ts'] is not None
    ```

### Test 2: Successful Subsequent Execution (Deactivates Previous)

*   **Purpose**: Verify that when `r_ausd_v_ta_discount` is called for a `p_JobKennung` that has a previously active entry, the old entry is correctly deactivated before the new one is registered and completed. This tests job management logic.
*   **Setup**:
    *   Clear control tables.
    *   Insert a mock "active" entry into `job_table` for `TEST_JOB_2`.
    ```sql
    TRUNCATE TABLE `your_project.your_dataset.job_table`;
    TRUNCATE TABLE `your_project.your_dataset.job_error_log`;
    TRUNCATE TABLE `your_project.your_dataset.job_run_control`;

    INSERT INTO `your_project.your_dataset.job_table` (job_kennung, eintrags_nr, active_flag, start_ts, script_name)
    VALUES ('TEST_JOB_2', '001', TRUE, TIMESTAMP('2023-01-01 10:00:00 UTC'), 'k_ausd_v_ta_discount.ksh_wrapper');
    ```
*   **Action**:
    Call the procedure with the same `job_kennung` but a new `eintrags_nr`.
    ```sql
    CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_2', '002');
    ```
*   **Pass/Fail Criterion**:
    *   **`job_table` assertions**:
        *   Two rows exist for `job_kennung = 'TEST_JOB_2'`.
        *   The row with `eintrags_nr = '001'` has `active_flag = FALSE` and `end_ts` populated (and `end_ts > start_ts`).
        *   The row with `eintrags_nr = '002'` has `active_flag = FALSE`, `start_ts` and `end_ts` populated (and `end_ts > start_ts`).
    *   **`job_error_log` assertions**:
        *   The table is empty.
    *   **`job_run_control` assertions**:
        *   One row exists for `job_kennung = 'TEST_JOB_2'` and `eintrags_nr = '002'` with `script_name = 'k_ausd_v_ta_discount.ksh_wrapper'`, `records_processed` > 0, and `update_ts` populated.

### Test 3: Parameter Validation - Missing `p_JobKennung`

*   **Purpose**: Verify that the procedure correctly handles a `NULL` `p_JobKennung` by logging an error and raising an exception, without affecting job state tables. This tests parameter handling and error reporting.
*   **Setup**:
    *   Clear control tables.
    ```sql
    TRUNCATE TABLE `your_project.your_dataset.job_table`;
    TRUNCATE TABLE `your_project.your_dataset.job_error_log`;
    TRUNCATE TABLE `your_project.your_dataset.job_run_control`;
    ```
*   **Action**:
    Call the procedure with `p_JobKennung` as `NULL`. This call is expected to fail.
    ```sql
    -- This will raise an error, so it needs to be caught by the test runner
    CALL `your_project.your_dataset.r_ausd_v_ta_discount`(NULL, '001');
    ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement raises an error with a message containing `'ERROR: p_JobKennung cannot be NULL or empty.'`.
    *   **`job_table` assertions**:
        *   The table is empty.
    *   **`job_error_log` assertions**:
        *   One row exists:
            *   `job_kennung = 'UNKNOWN'`
            *   `eintrags_nr = '001'`
            *   `err_nr = 1001`
            *   `err_arg` contains `'ERROR: p_JobKennung cannot be NULL or empty.'`
            *   `error_ts` is populated.
            *   `script_name = 'k_ausd_v_ta_discount.ksh_wrapper'`
    *   **`job_run_control` assertions**:
        *   The table is empty.

### Test 4: Parameter Validation - Empty `p_JobKennung`

*   **Purpose**: Verify that the procedure correctly handles an empty string `p_JobKennung` by logging an error and raising an exception.
*   **Setup**:
    *   Clear control tables.
    ```sql
    TRUNCATE TABLE `your_project.your_dataset.job_table`;
    TRUNCATE TABLE `your_project.your_dataset.job_error_log`;
    TRUNCATE TABLE `your_project.your_dataset.job_run_control`;
    ```
*   **Action**:
    Call the procedure with `p_JobKennung` as an empty string.
    ```sql
    CALL `your_project.your_dataset.r_ausd_v_ta_discount`('', '001');
    ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement raises an error with a message containing `'ERROR: p_JobKennung cannot be NULL or empty.'`.
    *   **`job_table` assertions**:
        *   The table is empty.
    *   **`job_error_log` assertions**:
        *   One row exists:
            *   `job_kennung = ''`
            *   `eintrags_nr = '001'`
            *   `err_nr = 1001`
            *   `err_arg` contains `'ERROR: p_JobKennung cannot be NULL or empty.'`
            *   `error_ts` is populated.
            *   `script_name = 'k_ausd_v_ta_discount.ksh_wrapper'`
    *   **`job_run_control` assertions**:
        *   The table is empty.

### Test 5: Error During Core SQL Logic Execution (`d_ausd_v_ta_discount` fails)

*   **Purpose**: Verify that if the called `d_ausd_v_ta_discount` procedure fails, the wrapper procedure catches the error, rolls back its transaction, logs the error, and re-raises it. This tests robust error handling and transaction management.
*   **Setup**:
    *   Clear control tables.
    *   **Crucially**: Temporarily modify `your_project.your_dataset.d_ausd_v_ta_discount` to unconditionally raise an error.
    ```sql
    TRUNCATE TABLE `your_project.your_dataset.job_table`;
    TRUNCATE TABLE `your_project.your_dataset.job_error_log`;
    TRUNCATE TABLE `your_project.your_dataset.job_run_control`;

    -- Temporary modification for testing error handling
    CREATE OR REPLACE PROCEDURE `your_project.your_dataset.d_ausd_v_ta_discount`(
        IN p_JobKennung STRING,
        IN p_EintragsNr STRING,
        OUT records_processed INT64
    )
    BEGIN
        RAISE USING MESSAGE 'Simulated error in d_ausd_v_ta_discount';
    END;
    ```
*   **Action**:
    Call the `r_ausd_v_ta_discount` procedure with valid parameters.
    ```sql
    CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_FAIL', '001');
    ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement raises an error with a message containing `'Simulated error in d_ausd_v_ta_discount'`.
    *   **`job_table` assertions**:
        *   The table is empty (due to transaction rollback).
    *   **`job_error_log` assertions**:
        *   One row exists:
            *   `job_kennung = 'TEST_JOB_FAIL'`
            *   `eintrags_nr = '001'`
            *   `err_nr` is populated (BigQuery's internal error code).
            *   `err_arg` contains the error message from `d_ausd_v_ta_discount` and wrapper context.
            *   `error_ts` is populated.
            *   `script_name = 'k_ausd_v_ta_discount.ksh_wrapper'`
    *   **`job_run_control` assertions**:
        *   The table is empty (due to transaction rollback, and `d_ausd_v_ta_discount` also failed before logging).
*   **Post-test Cleanup**: Revert `d_ausd_v_ta_discount` to its original placeholder state.

### Test 6: Record Count Capture (External System Replacement)

*   **Purpose**: Verify that the `records_processed` value returned by `d_ausd_v_ta_discount` (replacing the legacy `tmpFile` mechanism) is correctly captured and logged in `job_run_control` by the wrapper. This specifically tests the replacement of an external system interaction.
*   **Setup**:
    *   Clear control tables.
    *   **Crucially**: Temporarily modify `your_project.your_dataset.d_ausd_v_ta_discount` to return a *specific, known* `records_processed` value.
    ```sql
    TRUNCATE TABLE `your_project.your_dataset.job_table`;
    TRUNCATE TABLE `your_project.your_dataset.job_error_log`;
    TRUNCATE TABLE `your_project.your_dataset.job_run_control`;

    -- Temporary modification for testing record count
    CREATE OR REPLACE PROCEDURE `your_project.your_dataset.d_ausd_v_ta_discount`(
        IN p_JobKennung STRING,
        IN p_EintragsNr STRING,
        OUT records_processed INT64
    )
    BEGIN
        SET records_processed = 12345; -- Specific value for testing
        -- d_ausd_v_ta_discount also logs to job_run_control in its placeholder
        INSERT INTO `your_project.your_dataset.job_run_control` (job_kennung, eintrags_nr, script_name, records_processed, update_ts)
        VALUES (p_JobKennung, p_EintragsNr, 'd_ausd_v_ta_discount.sql', records_processed, CURRENT_TIMESTAMP());
    END;
    ```
*   **Action**:
    Call the `r_ausd_v_ta_discount` procedure.
    ```sql
    CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_COUNT', '001');
    ```
*   **Pass/Fail Criterion**:
    *   **`job_run_control` assertions**:
        *   One row exists for `job_kennung = 'TEST_JOB_COUNT'` and `script_name = 'k_ausd_v_ta_discount.ksh_wrapper'`.
        *   The `records_processed` column in this row must be `12345`.
        *   (Verify a second entry from `d_ausd_v_ta_discount.sql` also exists with `records_processed = 12345`).
*   **Post-test Cleanup**: Revert `d_ausd_v_ta_discount` to its original placeholder state.

### Test 7: Data Quality - Timestamps and Active Flag

*   **Purpose**: Verify that `start_ts`, `end_ts`, and `active_flag` are correctly set and updated in `job_table` during a successful run, ensuring data quality for job state tracking.
*   **Setup**:
    *   Clear control tables.
    ```sql
    TRUNCATE TABLE `your_project.your_dataset.job_table`;
    TRUNCATE TABLE `your_project.your_dataset.job_error_log`;
    TRUNCATE TABLE `your_project.your_dataset.job_run_control`;
    ```
*   **Action**:
    Call the procedure.
    ```sql
    CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_TS', '001');
    ```
*   **Pass/Fail Criterion**:
    *   **`job_table` assertions**:
        *   Query `job_table` for `job_kennung = 'TEST_JOB_TS'`.
        *   `active_flag` must be `FALSE`.
        *   `start_ts` and `end_ts` must be non-NULL.
        *   `end_ts` must be strictly greater than `start_ts`.
        *   The difference between `CURRENT_TIMESTAMP()` and both `start_ts` and `end_ts` should be within a small, acceptable time window (e.g., less than 10 seconds), confirming they were set during the current execution.

### Test 8: Schema Assertions

*   **Purpose**: Verify that the DDLs for the control tables (`job_table`, `job_error_log`, `job_run_control`) are correctly applied and the tables exist with the expected schema, column types, and nullability constraints.
*   **Setup**:
    *   Ensure the DDL scripts provided in the migration design have been executed in BigQuery.
*   **Action**:
    Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the tables in `your_project.your_dataset`.
*   **Pass/Fail Criterion**:
    *   **`your_project.your_dataset.job_table`**:
        *   `job_kennung`: `STRING`, `NOT NULL`
        *   `eintrags_nr`: `STRING`, `NOT NULL`
        *   `table_name`: `STRING`, `NULLABLE`
        *   `active_flag`: `BOOL`, `NOT NULL`
        *   `start_ts`: `TIMESTAMP`, `NOT NULL`
        *   `end_ts`: `TIMESTAMP`, `NULLABLE`
        *   `script_name`: `STRING`, `NULLABLE`
    *   **`your_project.your_dataset.job_error_log`**:
        *   `job_kennung`: `STRING`, `NOT NULL`
        *   `eintrags_nr`: `STRING`, `NOT NULL`
        *   `err_nr`: `INT64`, `NULLABLE`
        *   `err_arg`: `STRING`, `NULLABLE`
        *   `error_ts`: `TIMESTAMP`, `NOT NULL`
        *   `script_name`: `STRING`, `NULLABLE`
    *   **`your_project.your_dataset.job_run_control`**:
        *   `job_kennung`: `STRING`, `NOT NULL`
        *   `eintrags_nr`: `STRING`, `NOT NULL`
        *   `script_name`: `STRING`, `NOT NULL`
        *   `records_processed`: `INT64`, `NULLABLE`
        *   `update_ts`: `TIMESTAMP`, `NOT NULL`

    ```sql
    -- Example SQL assertion for job_table schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'job_table'
    ORDER BY
        ordinal_position;

    -- Expected output for job_table:
    -- column_name   | data_type | is_nullable
    -- --------------|-----------|------------
    -- job_kennung   | STRING    | NO
    -- eintrags_nr   | STRING    | NO
    -- table_name    | STRING    | YES
    -- active_flag   | BOOL      | NO
    -- start_ts      | TIMESTAMP | NO
    -- end_ts        | TIMESTAMP | YES
    -- script_name   | STRING    | YES
    ```