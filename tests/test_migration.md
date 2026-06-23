As a senior data-migration QA engineer, I have analyzed the migration design for `k_ausd_v_ta_p_discount.ksh` to BigQuery. The migration involves re-implementing KornShell orchestration logic and an underlying SQL script into BigQuery Stored Procedures and tables.

The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery solution against the legacy KornShell job, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests for `k_ausd_v_ta_p_discount.ksh`

**Target Environment:** Google Cloud BigQuery

**Assumptions:**
*   All BigQuery DDLs (`dataset.job_table`, `dataset.job_run_log`, `dataset.ta_p_discount`) are deployed.
*   Both BigQuery Stored Procedures (`dataset.d_ausd_v_ta_p_discount`, `dataset.r_ausd_vertrag_control`) are deployed.
*   Source tables `dataset.ta_disc_zusgf` and `dataset.ta_cntrct_crs` exist and can be populated with test data.
*   `dataset` refers to the BigQuery dataset where these objects reside (e.g., `dev_dw_migration`).

---

### Test Case 1: Successful End-to-End Execution - Data Transformation and Logging

*   **Purpose**: Verify that the migrated control stored procedure (`r_ausd_vertrag_control`) correctly orchestrates the data transformation, populates the target table (`ta_p_discount`) with expected data, and accurately logs the successful execution and record count. This covers output parity for the main data output and logging.
*   **Setup**:
    1.  Clear `dataset.ta_p_discount`, `dataset.job_table`, and `dataset.job_run_log`.
    2.  Populate `dataset.ta_disc_zusgf` with sample data.
    3.  Populate `dataset.ta_cntrct_crs` with sample data, ensuring some matching and some non-matching `cntrct_id` and `cntrct_obj_version` to test the `JOIN` logic.
    4.  Define `p_JobKennung = 'TEST_JOB_1'` and `p_EintragsNr = 'ENTRY_001'`.
*   **Action**:
    *   Execute the main control stored procedure: `CALL dataset.r_ausd_vertrag_control('TEST_JOB_1', 'ENTRY_001');`
*   **Pass/Fail Criterion**:
    1.  **`ta_p_discount` Content**: The data in `dataset.ta_p_discount` exactly matches the expected output derived from the source tables and the `d_ausd_v_ta_p_discount` logic (specifically the `JOIN` condition and column mapping).
    2.  **`ta_p_discount` Row Count**: The number of rows in `dataset.ta_p_discount` equals the expected count after transformation.
    3.  **`job_run_log` Entry**: Exactly one entry exists in `dataset.job_run_log` for `job_kennung = 'TEST_JOB_1'` and `eintragsnr = 'ENTRY_001'`, with `error_message IS NULL` and `records_count` matching the actual row count in `ta_p_discount`.
    4.  **`job_table` Status**: The entry for `job_kennung = 'TEST_JOB_1'` and `eintragsnr = 'ENTRY_001'` in `dataset.job_table` has `active_flag = FALSE` (indicating successful completion and deactivation).
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: Clear tables
    TRUNCATE TABLE dataset.ta_p_discount;
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.job_run_log;

    -- Setup: Populate source tables
    INSERT INTO dataset.ta_disc_zusgf (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('C1', 'TYPE_A', 1, 10.5),
    ('C2', 'TYPE_B', 1, 20.0),
    ('C3', 'TYPE_C', 2, 5.0); -- This record should not join due to obj_version mismatch

    INSERT INTO dataset.ta_cntrct_crs (cntrct_id, obj_version, contract_number) VALUES
    ('C1', 1, 'CNTRCT_001'),
    ('C2', 1, 'CNTRCT_002'),
    ('C4', 1, 'CNTRCT_004'); -- This record should not join due to cntrct_id mismatch

    -- Action: Execute the control SP
    CALL dataset.r_ausd_vertrag_control('TEST_JOB_1', 'ENTRY_001');

    -- Pass/Fail Criterion 1 & 2: ta_p_discount content and row count
    SELECT
        (SELECT COUNT(*) FROM dataset.ta_p_discount) = 2 AS row_count_match,
        (SELECT COUNT(*) FROM dataset.ta_p_discount WHERE cntrct_id = 'C1' AND contract_number = 'CNTRCT_001' AND rabatt_alle = 10.5) = 1 AS record_c1_match,
        (SELECT COUNT(*) FROM dataset.ta_p_discount WHERE cntrct_id = 'C2' AND contract_number = 'CNTRCT_002' AND rabatt_alle = 20.0) = 1 AS record_c2_match,
        (SELECT COUNT(*) FROM dataset.ta_p_discount WHERE cntrct_id = 'C3') = 0 AS record_c3_not_present -- C3 has obj_version 2, no match in ta_cntrct_crs
    ;
    -- Expected results: row_count_match=TRUE, record_c1_match=TRUE, record_c2_match=TRUE, record_c3_not_present=TRUE

    -- Pass/Fail Criterion 3: job_run_log entry
    SELECT
        (SELECT COUNT(*) FROM dataset.job_run_log WHERE job_kennung = 'TEST_JOB_1' AND eintragsnr = 'ENTRY_001' AND error_message IS NULL AND records_count = 2) = 1 AS log_entry_match
    ;
    -- Expected result: log_entry_match=TRUE

    -- Pass/Fail Criterion 4: job_table status
    SELECT
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'TEST_JOB_1' AND eintragsnr = 'ENTRY_001' AND active_flag = FALSE) = 1 AS job_table_status_match
    ;
    -- Expected result: job_table_status_match=TRUE
    ```

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

*   **Purpose**: Verify that the control SP correctly handles a missing or empty `p_JobKennung` parameter by raising an error and logging it, mimicking the legacy `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.
*   **Setup**:
    1.  Clear `dataset.job_run_log` and `dataset.job_table`.
    2.  Define `p_EintragsNr = 'ENTRY_002'`.
*   **Action**:
    *   Attempt to execute `CALL dataset.r_ausd_vertrag_control(NULL, 'ENTRY_002');` (or `''` for an empty string).
*   **Pass/Fail Criterion**:
    1.  **Error Raised**: The call fails with a `SIGNAL SQLSTATE '45000'` error, and the error message contains "Parameter p_JobKennung is missing or empty."
    2.  **`job_run_log` Entry**: Exactly one entry exists in `dataset.job_run_log` for `job_kennung = 'UNKNOWN'` (as per SP logic for NULL `p_JobKennung`) and `eintragsnr = 'ENTRY_002'`, with `error_message` containing "Parameter p_JobKennung is missing or empty." and `records_count = 0`.
    3.  **No Data Change**: `dataset.ta_p_discount` remains unchanged (e.g., empty if it was empty before the call).
    4.  **No Job Table Entry**: No new entry is created in `dataset.job_table` for this failed run.
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: Clear tables
    TRUNCATE TABLE dataset.job_run_log;
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.ta_p_discount; -- Ensure it's empty or has a known state

    -- Action: Attempt to execute with missing p_JobKennung (expected to fail)
    BEGIN
        CALL dataset.r_ausd_vertrag_control(NULL, 'ENTRY_002');
    EXCEPTION WHEN ERROR THEN
        -- Pass/Fail Criterion 1: Error Raised
        SELECT @@error.message LIKE '%Parameter p_JobKennung is missing or empty%' AS error_message_match;
        -- Expected result: error_message_match=TRUE
    END;

    -- Pass/Fail Criterion 2: job_run_log entry
    SELECT
        (SELECT COUNT(*) FROM dataset.job_run_log WHERE job_kennung = 'UNKNOWN' AND eintragsnr = 'ENTRY_002' AND error_message LIKE '%Parameter p_JobKennung is missing or empty%' AND records_count = 0) = 1 AS log_entry_match
    ;
    -- Expected result: log_entry_match=TRUE

    -- Pass/Fail Criterion 3: No Data Change
    SELECT (SELECT COUNT(*) FROM dataset.ta_p_discount) = 0 AS ta_p_discount_empty;
    -- Expected result: ta_p_discount_empty=TRUE

    -- Pass/Fail Criterion 4: No Job Table Entry
    SELECT (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'UNKNOWN' AND eintragsnr = 'ENTRY_002') = 0 AS no_job_table_entry;
    -- Expected result: no_job_table_entry=TRUE
    ```

### Test Case 3: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose**: Verify that the control SP correctly handles a missing or empty `p_EintragsNr` parameter by raising an error and logging it.
*   **Setup**:
    1.  Clear `dataset.job_run_log` and `dataset.job_table`.
    2.  Define `p_JobKennung = 'TEST_JOB_2'`.
*   **Action**:
    *   Attempt to execute `CALL dataset.r_ausd_vertrag_control('TEST_JOB_2', '');` (or `NULL`).
*   **Pass/Fail Criterion**:
    1.  **Error Raised**: The call fails with a `SIGNAL SQLSTATE '45000'` error, and the error message contains "Parameter p_EintragsNr is missing or empty."
    2.  **`job_run_log` Entry**: Exactly one entry exists in `dataset.job_run_log` for `job_kennung = 'TEST_JOB_2'` and `eintragsnr = 'UNKNOWN'`, with `error_message` containing "Parameter p_EintragsNr is missing or empty." and `records_count = 0`.
    3.  **No Data Change**: `dataset.ta_p_discount` remains unchanged.
    4.  **No Job Table Entry**: No new entry is created in `dataset.job_table` for this failed run.
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: Clear tables
    TRUNCATE TABLE dataset.job_run_log;
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.ta_p_discount;

    -- Action: Attempt to execute with missing p_EintragsNr (expected to fail)
    BEGIN
        CALL dataset.r_ausd_vertrag_control('TEST_JOB_2', '');
    EXCEPTION WHEN ERROR THEN
        SELECT @@error.message LIKE '%Parameter p_EintragsNr is missing or empty%' AS error_message_match;
        -- Expected result: error_message_match=TRUE
    END;

    -- Pass/Fail Criterion 2: job_run_log entry
    SELECT
        (SELECT COUNT(*) FROM dataset.job_run_log WHERE job_kennung = 'TEST_JOB_2' AND eintragsnr = 'UNKNOWN' AND error_message LIKE '%Parameter p_EintragsNr is missing or empty%' AND records_count = 0) = 1 AS log_entry_match
    ;
    -- Expected result: log_entry_match=TRUE

    -- Pass/Fail Criterion 3 & 4: No Data Change & No Job Table Entry
    SELECT
        (SELECT COUNT(*) FROM dataset.ta_p_discount) = 0 AS ta_p_discount_empty,
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'TEST_JOB_2' AND eintragsnr = 'UNKNOWN') = 0 AS no_job_table_entry
    ;
    -- Expected results: ta_p_discount_empty=TRUE, no_job_table_entry=TRUE
    ```

### Test Case 4: Job Control - Deactivating Older Active Jobs

*   **Purpose**: Verify that the control SP correctly deactivates older active jobs for the same `p_JobKennung` but different `p_EintragsNr`, replicating the legacy job control logic. This validates the replacement of implicit job management.
*   **Setup**:
    1.  Clear `dataset.job_table`.
    2.  Insert multiple active job entries for `p_JobKennung = 'JOB_CONTROL_TEST'` with different `eintragsnr` values.
    3.  Insert one active job entry for a *different* `job_kennung` to ensure it's not affected.
    4.  Populate source tables (`ta_disc_zusgf`, `ta_cntrct_crs`) to ensure the `d_ausd_v_ta_p_discount` call succeeds.
    5.  Define `p_JobKennung = 'JOB_CONTROL_TEST'` and `p_EintragsNr = 'NEW_ENTRY_001'`.
*   **Action**:
    *   Execute the control stored procedure: `CALL dataset.r_ausd_vertrag_control('JOB_CONTROL_TEST', 'NEW_ENTRY_001');`
*   **Pass/Fail Criterion**:
    1.  **Old Jobs Deactivated**: All `job_table` entries for `job_kennung = 'JOB_CONTROL_TEST'` *except* `NEW_ENTRY_001` should have `active_flag = FALSE`.
    2.  **New Job Deactivated**: The entry for `job_kennung = 'JOB_CONTROL_TEST'` and `eintragsnr = 'NEW_ENTRY_001'` should exist and have `active_flag = FALSE` (as it completes successfully).
    3.  **Other Jobs Unchanged**: The `job_table` entry for the *different* `job_kennung` should remain `active_flag = TRUE`.
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: Clear and populate job_table
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.job_run_log;
    TRUNCATE TABLE dataset.ta_p_discount;

    INSERT INTO dataset.job_table (job_kennung, eintragsnr, active_flag, created_at, updated_at) VALUES
    ('JOB_CONTROL_TEST', 'OLD_ENTRY_001', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('JOB_CONTROL_TEST', 'OLD_ENTRY_002', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('JOB_CONTROL_TEST', 'OLD_ENTRY_003', FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()), -- Already inactive, should remain inactive
    ('OTHER_JOB', 'OTHER_ENTRY_001', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    -- Setup: Populate source tables for successful run
    INSERT INTO dataset.ta_disc_zusgf (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES ('X1', 'T1', 1, 1.0);
    INSERT INTO dataset.ta_cntrct_crs (cntrct_id, obj_version, contract_number) VALUES ('X1', 1, 'CX1');

    -- Action: Execute the control SP
    CALL dataset.r_ausd_vertrag_control('JOB_CONTROL_TEST', 'NEW_ENTRY_001');

    -- Pass/Fail Criterion 1, 2 & 3: Check job_table states
    SELECT
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'JOB_CONTROL_TEST' AND eintragsnr IN ('OLD_ENTRY_001', 'OLD_ENTRY_002') AND active_flag = FALSE) = 2 AS old_jobs_deactivated,
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'JOB_CONTROL_TEST' AND eintragsnr = 'OLD_ENTRY_003' AND active_flag = FALSE) = 1 AS already_inactive_job_unchanged,
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'JOB_CONTROL_TEST' AND eintragsnr = 'NEW_ENTRY_001' AND active_flag = FALSE) = 1 AS new_job_deactivated_after_success,
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'OTHER_JOB' AND eintragsnr = 'OTHER_ENTRY_001' AND active_flag = TRUE) = 1 AS other_job_unchanged
    ;
    -- Expected results: all TRUE
    ```

### Test Case 5: Error Handling during Data Transformation

*   **Purpose**: Verify that if the `d_ausd_v_ta_p_discount` stored procedure fails during data transformation, the control SP catches the error, logs it to `job_run_log`, and deactivates the job in `job_table`. This validates the BigQuery error handling replacing `DWMSG_MeldeFehler` and `exit`.
*   **Setup**:
    1.  Clear `dataset.job_table`, `dataset.job_run_log`, `dataset.ta_p_discount`.
    2.  Insert an active job entry for `p_JobKennung = 'ERROR_JOB'`, `p_EintragsNr = 'ERROR_ENTRY_001'` into `job_table`.
    3.  **Simulate an error in `d_ausd_v_ta_p_discount`**: This can be done by temporarily modifying `d_ausd_v_ta_p_discount` to `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated transformation error for testing.';` or by creating source data that causes a data type mismatch or constraint violation during the `INSERT`. For this example, we assume a simulated error.
*   **Action**:
    *   Execute the control stored procedure: `CALL dataset.r_ausd_vertrag_control('ERROR_JOB', 'ERROR_ENTRY_001');`
*   **Pass/Fail Criterion**:
    1.  **Error Raised**: The call fails with a `SIGNAL SQLSTATE '45000'` error originating from the control SP's `EXCEPTION` block, and the error message contains "Simulated transformation error".
    2.  **`job_run_log` Entry**: Exactly one entry exists in `dataset.job_run_log` for `job_kennung = 'ERROR_JOB'` and `eintragsnr = 'ERROR_ENTRY_001'`, with a non-NULL `error_message` containing details about the transformation error and `records_count = 0`.
    3.  **`job_table` Status**: The entry for `job_kennung = 'ERROR_JOB'` and `eintragsnr = 'ERROR_ENTRY_001'` in `dataset.job_table` has `active_flag = FALSE`.
    4.  **No Partial Data**: `dataset.ta_p_discount` should be empty (since `TRUNCATE` would have occurred, but `INSERT` failed).
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: Clear tables
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.job_run_log;
    TRUNCATE TABLE dataset.ta_p_discount;

    -- Setup: Insert an active job entry (it will be deactivated by the SP's error handling)
    INSERT INTO dataset.job_table (job_kennung, eintragsnr, active_flag, created_at, updated_at) VALUES
    ('ERROR_JOB', 'ERROR_ENTRY_001', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    -- IMPORTANT: For this test, you would temporarily modify 'dataset.d_ausd_v_ta_p_discount'
    -- to explicitly raise an error, e.g.:
    -- CREATE OR REPLACE PROCEDURE dataset.d_ausd_v_ta_p_discount(p_EintragsNr STRING, p_JobKennung STRING)
    -- BEGIN
    --     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated transformation error for testing.';
    -- END;
    -- Remember to revert this change after the test.

    -- Action: Execute the control SP, expecting it to fail
    BEGIN
        CALL dataset.r_ausd_vertrag_control('ERROR_JOB', 'ERROR_ENTRY_001');
    EXCEPTION WHEN ERROR THEN
        -- Pass/Fail Criterion 1: Error Raised
        SELECT @@error.message LIKE '%Simulated transformation error for testing%' AS error_message_match;
        -- Expected result: error_message_match=TRUE
    END;

    -- Pass/Fail Criterion 2: job_run_log entry
    SELECT
        (SELECT COUNT(*) FROM dataset.job_run_log WHERE job_kennung = 'ERROR_JOB' AND eintragsnr = 'ERROR_ENTRY_001' AND error_message LIKE '%Simulated transformation error for testing%' AND records_count = 0) = 1 AS log_entry_match
    ;
    -- Expected result: log_entry_match=TRUE

    -- Pass/Fail Criterion 3: job_table status
    SELECT
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'ERROR_JOB' AND eintragsnr = 'ERROR_ENTRY_001' AND active_flag = FALSE) = 1 AS job_table_status_match
    ;
    -- Expected result: job_table_status_match=TRUE

    -- Pass/Fail Criterion 4: No Partial Data
    SELECT (SELECT COUNT(*) FROM dataset.ta_p_discount) = 0 AS ta_p_discount_empty;
    -- Expected result: ta_p_discount_empty=TRUE
    ```

### Test Case 6: Schema and Data Type Integrity of `ta_p_discount`

*   **Purpose**: Verify that the schema of `dataset.ta_p_discount` matches the expected structure and data types as defined in the DDL, ensuring no data loss or truncation issues during transformation. This is a fundamental data quality check.
*   **Setup**:
    1.  Ensure `dataset.ta_p_discount` is created as per its DDL.
    2.  Populate `dataset.ta_disc_zusgf` and `dataset.ta_cntrct_crs` with data covering various data types and edge cases (e.g., max length strings, numbers with decimal places, NULLs).
*   **Action**:
    *   Execute `CALL dataset.r_ausd_vertrag_control('SCHEMA_TEST', 'ENTRY_001');` (assuming it runs successfully).
*   **Pass/Fail Criterion**:
    1.  **Schema Match**: Query `INFORMATION_SCHEMA.COLUMNS` for `dataset.ta_p_discount` and compare column names, data types, and nullability against the expected DDL.
    2.  **Data Integrity**: After a successful run, `SELECT * FROM dataset.ta_p_discount` should show that data types are correctly preserved (e.g., `NUMERIC` values are not truncated, `STRING` values are not cut off).
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: (Assuming DDLs are already deployed and source data is prepared)
    TRUNCATE TABLE dataset.ta_p_discount;
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.job_run_log;

    INSERT INTO dataset.ta_disc_zusgf (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('C_LONG_ID_12345678901234567890', 'TYPE_X', 999999999, 1234567890.123456789);
    INSERT INTO dataset.ta_cntrct_crs (cntrct_id, obj_version, contract_number) VALUES
    ('C_LONG_ID_12345678901234567890', 999999999, 'LONG_CONTRACT_NUMBER_XYZ_12345');

    -- Action: Execute the control SP
    CALL dataset.r_ausd_vertrag_control('SCHEMA_TEST', 'ENTRY_001');

    -- Pass/Fail Criterion 1: Schema Match
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        dataset.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'ta_p_discount'
    ORDER BY
        ordinal_position;
    -- Expected Output (compare against DDL):
    -- column_name        data_type   is_nullable
    -- cntrct_id          STRING      YES
    -- disc_vector_ty     STRING      YES
    -- cntrct_obj_version INT64       YES
    -- rabatt_alle        NUMERIC     YES
    -- contract_number    STRING      YES

    -- Pass/Fail Criterion 2: Data Integrity (example with specific data)
    SELECT
        cntrct_id = 'C_LONG_ID_12345678901234567890' AS cntrct_id_match,
        disc_vector_ty = 'TYPE_X' AS disc_vector_ty_match,
        cntrct_obj_version = 999999999 AS cntrct_obj_version_match,
        rabatt_alle = 1234567890.123456789 AS rabatt_alle_match,
        contract_number = 'LONG_CONTRACT_NUMBER_XYZ_12345' AS contract_number_match
    FROM
        dataset.ta_p_discount
    WHERE cntrct_id = 'C_LONG_ID_12345678901234567890';
    -- Expected results: all TRUE (ensures no truncation or type conversion issues)
    ```

### Test Case 7: Row Count Consistency (Zero Rows)

*   **Purpose**: Verify that the job correctly handles scenarios where the source data results in zero output rows, logging 0 records and maintaining correct job status.
*   **Setup**:
    1.  Clear `dataset.ta_p_discount`, `dataset.job_table`, `dataset.job_run_log`.
    2.  Populate `dataset.ta_disc_zusgf` and `dataset.ta_cntrct_crs` such that their `JOIN` condition produces zero rows (e.g., no matching `cntrct_id` or `cntrct_obj_version`).
*   **Action**:
    *   Execute the control stored procedure: `CALL dataset.r_ausd_vertrag_control('ZERO_ROWS_TEST', 'ENTRY_001');`
*   **Pass/Fail Criterion**:
    1.  **`ta_p_discount` Empty**: `SELECT COUNT(*) FROM dataset.ta_p_discount` returns 0.
    2.  **`job_run_log` Entry**: Exactly one entry exists in `dataset.job_run_log` for `job_kennung = 'ZERO_ROWS_TEST'` and `eintragsnr = 'ENTRY_001'`, with `error_message IS NULL` and `records_count = 0`.
    3.  **`job_table` Status**: The entry for `job_kennung = 'ZERO_ROWS_TEST'` and `eintragsnr = 'ENTRY_001'` in `dataset.job_table` has `active_flag = FALSE`.
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: Clear tables
    TRUNCATE TABLE dataset.ta_p_discount;
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.job_run_log;

    -- Setup: Populate source tables with no matching join conditions
    INSERT INTO dataset.ta_disc_zusgf (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('C1', 'TYPE_A', 1, 10.5);
    INSERT INTO dataset.ta_cntrct_crs (cntrct_id, obj_version, contract_number) VALUES
    ('C2', 1, 'CNTRCT_002'); -- No matching C1

    -- Action: Execute the control SP
    CALL dataset.r_ausd_vertrag_control('ZERO_ROWS_TEST', 'ENTRY_001');

    -- Pass/Fail Criterion 1: ta_p_discount empty
    SELECT (SELECT COUNT(*) FROM dataset.ta_p_discount) = 0 AS ta_p_discount_empty;
    -- Expected result: ta_p_discount_empty=TRUE

    -- Pass/Fail Criterion 2: job_run_log entry
    SELECT
        (SELECT COUNT(*) FROM dataset.job_run_log WHERE job_kennung = 'ZERO_ROWS_TEST' AND eintragsnr = 'ENTRY_001' AND error_message IS NULL AND records_count = 0) = 1 AS log_entry_match
    ;
    -- Expected result: log_entry_match=TRUE

    -- Pass/Fail Criterion 3: job_table status
    SELECT
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'ZERO_ROWS_TEST' AND eintragsnr = 'ENTRY_001' AND active_flag = FALSE) = 1 AS job_table_status_match
    ;
    -- Expected result: job_table_status_match=TRUE
    ```

### Test Case 8: NULL Handling in Transformation

*   **Purpose**: Verify how NULL values in source columns are handled during the transformation, especially for columns that are directly mapped to target columns without `NOT NULL` constraints.
*   **Setup**:
    1.  Clear `dataset.ta_p_discount`, `dataset.job_table`, `dataset.job_run_log`.
    2.  Populate `dataset.ta_disc_zusgf` and `dataset.ta_cntrct_crs` with data including NULLs in various columns that are part of the `SELECT` list in `d_ausd_v_ta_p_discount`.
*   **Action**:
    *   Execute the control stored procedure: `CALL dataset.r_ausd_vertrag_control('NULL_HANDLING_TEST', 'ENTRY_001');`
*   **Pass/Fail Criterion**:
    1.  **NULLs Preserved**: `SELECT * FROM dataset.ta_p_discount` shows that NULL values from source columns are correctly propagated to the target table.
    2.  **Row Count Correct**: The number of rows in `ta_p_discount` is as expected.
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: Clear tables
    TRUNCATE TABLE dataset.ta_p_discount;
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.job_run_log;

    -- Setup: Populate source tables with NULLs
    INSERT INTO dataset.ta_disc_zusgf (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('C_NULL_1', NULL, 1, 10.5),
    ('C_NULL_2', 'TYPE_B', 1, NULL);

    INSERT INTO dataset.ta_cntrct_crs (cntrct_id, obj_version, contract_number) VALUES
    ('C_NULL_1', 1, 'CNTRCT_NULL_1'),
    ('C_NULL_2', 1, NULL);

    -- Action: Execute the control SP
    CALL dataset.r_ausd_vertrag_control('NULL_HANDLING_TEST', 'ENTRY_001');

    -- Pass/Fail Criterion 1: NULLs Preserved
    SELECT
        (SELECT COUNT(*) FROM dataset.ta_p_discount WHERE cntrct_id = 'C_NULL_1' AND disc_vector_ty IS NULL AND rabatt_alle = 10.5 AND contract_number = 'CNTRCT_NULL_1') = 1 AS null_disc_vector_ty_ok,
        (SELECT COUNT(*) FROM dataset.ta_p_discount WHERE cntrct_id = 'C_NULL_2' AND disc_vector_ty = 'TYPE_B' AND rabatt_alle IS NULL AND contract_number IS NULL) = 1 AS null_rabatt_alle_contract_number_ok
    ;
    -- Expected results: all TRUE

    -- Pass/Fail Criterion 2: Row Count Correct
    SELECT (SELECT COUNT(*) FROM dataset.ta_p_discount) = 2 AS row_count_match;
    -- Expected result: row_count_match=TRUE
    ```

### Test Case 9: Idempotency of Job Control (Multiple Runs)

*   **Purpose**: Verify that running the job multiple times with the same or different parameters (that trigger deactivation logic) behaves consistently and correctly, ensuring the job control mechanism is robust and idempotent.
*   **Setup**:
    1.  Clear `dataset.ta_p_discount`, `dataset.job_table`, `dataset.job_run_log`.
    2.  Populate source tables (`ta_disc_zusgf`, `ta_cntrct_crs`) to ensure successful data transformation.
*   **Action**:
    1.  Execute `CALL dataset.r_ausd_vertrag_control('IDEMPOTENCY_TEST', 'ENTRY_A');`
    2.  Execute `CALL dataset.r_ausd_vertrag_control('IDEMPOTENCY_TEST', 'ENTRY_B');` (This should deactivate 'ENTRY_A')
    3.  Execute `CALL dataset.r_ausd_vertrag_control('IDEMPOTENCY_TEST', 'ENTRY_A');` (This should deactivate 'ENTRY_B')
*   **Pass/Fail Criterion**:
    1.  **`job_table` Status After Step 1**: The entry for `ENTRY_A` is `active_flag = FALSE`.
    2.  **`job_table` Status After Step 2**: The entry for `ENTRY_A` remains `active_flag = FALSE`, and `ENTRY_B` is `active_flag = FALSE`.
    3.  **`job_table` Status After Step 3**: The entry for `ENTRY_A` remains `active_flag = FALSE`, and `ENTRY_B` remains `active_flag = FALSE`.
    4.  **`job_run_log` Entries**: Three successful entries exist, each with the correct `records_count`.
    5.  **`ta_p_discount` Content**: The final `ta_p_discount` content should be identical after each successful run (due to the `TRUNCATE` + `INSERT` logic in `d_ausd_v_ta_p_discount`).
*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Setup: Clear tables and prepare source data
    TRUNCATE TABLE dataset.ta_p_discount;
    TRUNCATE TABLE dataset.job_table;
    TRUNCATE TABLE dataset.job_run_log;
    INSERT INTO dataset.ta_disc_zusgf (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES ('C1', 'T1', 1, 1.0);
    INSERT INTO dataset.ta_cntrct_crs (cntrct_id, obj_version, contract_number) VALUES ('C1', 1, 'CX1');

    -- Action 1
    CALL dataset.r_ausd_vertrag_control('IDEMPOTENCY_TEST', 'ENTRY_A');
    -- Pass/Fail 1
    SELECT (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'IDEMPOTENCY_TEST' AND eintragsnr = 'ENTRY_A' AND active_flag = FALSE) = 1 AS entry_a_deactivated_1;
    -- Expected result: entry_a_deactivated_1=TRUE

    -- Action 2
    CALL dataset.r_ausd_vertrag_control('IDEMPOTENCY_TEST', 'ENTRY_B');
    -- Pass/Fail 2
    SELECT
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'IDEMPOTENCY_TEST' AND eintragsnr = 'ENTRY_A' AND active_flag = FALSE) = 1 AS entry_a_deactivated_2,
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'IDEMPOTENCY_TEST' AND eintragsnr = 'ENTRY_B' AND active_flag = FALSE) = 1 AS entry_b_deactivated_2
    ;
    -- Expected results: entry_a_deactivated_2=TRUE, entry_b_deactivated_2=TRUE

    -- Action 3
    CALL dataset.r_ausd_vertrag_control('IDEMPOTENCY_TEST', 'ENTRY_A');
    -- Pass/Fail 3
    SELECT
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'IDEMPOTENCY_TEST' AND eintragsnr = 'ENTRY_A' AND active_flag = FALSE) = 1 AS entry_a_deactivated_3,
        (SELECT COUNT(*) FROM dataset.job_table WHERE job_kennung = 'IDEMPOTENCY_TEST' AND eintragsnr = 'ENTRY_B' AND active_flag = FALSE) = 1 AS entry_b_deactivated_3
    ;
    -- Expected results: entry_a_deactivated_3=TRUE, entry_b_deactivated_3=TRUE

    -- Pass/Fail 4: job_run_log entries
    SELECT (SELECT COUNT(*) FROM dataset.job_run_log WHERE job_kennung = 'IDEMPOTENCY_TEST' AND error_message IS NULL AND records_count = 1) = 3 AS total_successful_logs;
    -- Expected result: total_successful_logs=TRUE

    -- Pass/Fail 5: ta_p_discount content (should be consistent after each run)
    SELECT (SELECT COUNT(*) FROM dataset.ta_p_discount) = 1 AS final_ta_p_discount_count;
    SELECT cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle, contract_number FROM dataset.ta_p_discount;
    -- Expected content: C1, T1, 1, 1.0, CX1
    ```