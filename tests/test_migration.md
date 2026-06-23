As a senior data-migration QA engineer, I have analyzed the provided migration design and the legacy KornShell script. The migration aims to translate the orchestration logic and an external SQL script to Google BigQuery stored procedures, replacing shell utilities with native BigQuery features and logging mechanisms.

A key aspect of this migration is the placeholder nature of `d_ausd_bp_ta_apn_vertrag_proc`. For comprehensive testing, I will assume a minimal implementation for this procedure that allows verification of parameter passing and record counting. I will also update the provided generated code for `d_ausd_bp_ta_apn_vertrag_proc` and `r_ausd_bp_ta_apn_vertrag_proc` to reflect the necessary changes for testing (e.g., accepting `p_datum_heute`, `p_datum_gestern`, and `p_wiederanlaufWert` and logging them in the target table).

Here are the migration validation tests, organized by category, with purpose, setup, action, and concrete pass/fail criteria.

---

## Updated Generated Migration Code (for testing purposes)

To enable thorough testing, the `d_ausd_bp_ta_apn_vertrag_proc` and `r_ausd_bp_ta_apn_vertrag_proc` have been slightly modified from the original prompt's "GENERATED MIGRATION CODE" section. These modifications primarily involve updating the parameter list for `d_ausd_bp_ta_apn_vertrag_proc` to match the legacy script's parameter passing and including these parameters in the `poolbasisprodukt` table for verification.

--- FILE: bigquery/procedures/d_ausd_bp_ta_apn_vertrag_proc.sql ---
```sql
-- BigQuery Stored Procedure: project.dataset.d_ausd_bp_ta_apn_vertrag_proc
-- Replaces the logic from d_ausd_bp_ta_apn_vertrag.sql, orchestrated by k_ausd_bp_ta_apn_vertrag.ksh
-- NOTE: The original SQL content of d_ausd_bp_ta_apn_vertrag.sql was not provided.
-- This is a placeholder procedure. You must replace the content with the actual
-- translated SQL logic from d_ausd_bp_ta_apn_vertrag.sql.
--
-- MODIFIED FOR TESTING: Added p_datum_heute, p_datum_gestern to signature and target table.

CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_bp_ta_apn_vertrag_proc(
    p_EintragsNr STRING,
    p_JobKennung STRING,
    p_Stichtag DATE,
    p_wiederanlaufWert STRING,
    p_datum_heute DATE,         -- Added for testing parameter passing
    p_datum_gestern DATE,       -- Added for testing parameter passing
    OUT processed_rows INT64
)
BEGIN
    -- Placeholder for the actual SQL logic from d_ausd_bp_ta_apn_vertrag.sql
    -- This section should contain CREATE, MERGE, INSERT, UPDATE, DELETE statements
    -- that perform the core data extraction and transformation.

    -- Example: Insert into a hypothetical target table
    -- REPLACE WITH YOUR ACTUAL LOGIC
    CREATE SCHEMA IF NOT EXISTS project.dataset;
    CREATE TABLE IF NOT EXISTS project.dataset.poolbasisprodukt (
        col1 STRING,
        col2 DATE,
        job_kennung STRING,
        eintragsnr STRING,
        stichtag DATE,
        wiederanlauf_wert STRING, -- Added for testing
        datum_heute DATE,         -- Added for testing
        datum_gestern DATE        -- Added for testing
    );

    INSERT INTO project.dataset.poolbasisprodukt (col1, col2, job_kennung, eintragsnr, stichtag, wiederanlauf_wert, datum_heute, datum_gestern)
    SELECT
        'example_data_' || GENERATE_UUID(),
        CURRENT_DATE(),
        p_JobKennung,
        p_EintragsNr,
        p_Stichtag,
        p_wiederanlaufWert,
        p_datum_heute,
        p_datum_gestern
    FROM UNNEST(GENERATE_ARRAY(1, 10)) -- Simulate some rows being processed
    LIMIT 10;

    SET processed_rows = @@row_count; -- Get the count of rows affected by the last DML statement

    -- If the logic involves multiple DMLs, you might need to sum row counts or
    -- query the final target table to get the true processed_rows count.
    -- Example: SET processed_rows = (SELECT COUNT(*) FROM project.dataset.poolbasisprodukt WHERE stichtag = p_Stichtag);

EXCEPTION WHEN ERROR THEN
    INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
    VALUES (
        CURRENT_TIMESTAMP(),
        'd_ausd_bp_ta_apn_vertrag_proc',
        @@error.message,
        'ERROR',
        TO_JSON(STRUCT(p_EintragsNr, p_JobKennung, p_Stichtag, p_wiederanlaufWert AS wiederanlaufWert_param, p_datum_heute, p_datum_gestern))
    );
    RAISE; -- Re-raise the error to propagate it to the calling procedure
END;
```

--- FILE: bigquery/procedures/r_ausd_bp_ta_apn_vertrag_proc.sql ---
```sql
-- BigQuery Stored Procedure: project.dataset.r_ausd_bp_ta_apn_vertrag_proc
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- This procedure orchestrates parameter handling, validation, date calculations,
-- and execution of the core data transformation.
--
-- MODIFIED FOR TESTING: Updated CALL statement for d_ausd_bp_ta_apn_vertrag_proc.

CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag_Str STRING, -- Input as STRING for validation
    p_wiederanlaufWert STRING
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_apn_vertrag.ksh';
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_reference_date DATE;
    DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
    DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
    DECLARE v_processed_record_count INT64;
    DECLARE v_job_status STRING DEFAULT 'SUCCESS';
    DECLARE v_error_message STRING;

    -- 1. Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_error_message = 'Parameter "Job ID (p_JobKennung)" cannot be empty.';
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('MissingParameter' AS reason, 'p_JobKennung' AS parameter_name)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_error_message = 'Parameter "Entry Number (p_EintragsNr)" cannot be empty.';
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('MissingParameter' AS reason, 'p_EintragsNr' AS parameter_name)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END IF;

    IF p_Stichtag_Str IS NULL OR p_Stichtag_Str = '' THEN
        SET v_error_message = 'Parameter "Reference Date (p_Stichtag_Str)" cannot be empty.';
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('MissingParameter' AS reason, 'p_Stichtag_Str' AS parameter_name)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END IF;

    -- 2. Date Validation and Conversion
    BEGIN
        SET v_reference_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag_Str);
        IF v_reference_date IS NULL THEN
            SET v_error_message = FORMAT('Invalid date format for p_Stichtag_Str: "%s". Expected DDMMYYYY.', p_Stichtag_Str);
            INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
            VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('InvalidDateFormat' AS reason, 'p_Stichtag_Str' AS parameter_name, p_Stichtag_Str AS provided_value)));
            SET v_job_status = 'FAILED';
            RAISE EXCEPTION '%', v_error_message;
        END IF;
    EXCEPTION WHEN ERROR THEN
        -- This block catches parsing errors not caught by SAFE.PARSE_DATE returning NULL (e.g., if p_Stichtag_Str is a valid date but not DDMMYYYY)
        -- SAFE.PARSE_DATE handles most malformed dates by returning NULL. This is a safeguard.
        SET v_error_message = FORMAT('Unexpected error during date parsing for p_Stichtag_Str: "%s". Error: %s', p_Stichtag_Str, @@error.message);
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('DateParsingError' AS reason, 'p_Stichtag_Str' AS parameter_name, p_Stichtag_Str AS provided_value, 'SQL_Error' AS sql_error_type, @@error.message AS sql_error_message)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END;

    -- 3. Initialize p_wiederanlaufWert if empty (legacy behavior)
    IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
        SET p_wiederanlaufWert = '0';
    END IF;

    -- 4. Execute Core Data Transformation Procedure
    BEGIN
        CALL project.dataset.d_ausd_bp_ta_apn_vertrag_proc(
            p_EintragsNr,
            p_JobKennung,
            v_reference_date,
            p_wiederanlaufWert,
            v_datum_heute,
            v_datum_gestern,
            v_processed_record_count
        );
    EXCEPTION WHEN ERROR THEN
        SET v_error_message = FORMAT('Error executing d_ausd_bp_ta_apn_vertrag_proc: %s', @@error.message);
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('ProcedureExecutionError' AS reason, 'procedure_name' AS d_ausd_bp_ta_apn_vertrag_proc, 'SQL_Error' AS sql_error_type, @@error.message AS sql_error_message)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END;

    -- 5. Job Audit
    INSERT INTO project.dataset.job_audit (
        audit_timestamp,
        job_name,
        job_id,
        entry_number,
        reference_date,
        target_table,
        processed_record_count,
        status,
        start_timestamp,
        end_timestamp
    )
    VALUES (
        CURRENT_TIMESTAMP(),
        v_job_name,
        p_JobKennung,
        p_EintragsNr,
        v_reference_date,
        'PoolBasisprodukt', -- As identified in the design document
        IFNULL(v_processed_record_count, 0), -- Use 0 if the procedure didn't return a count
        v_job_status,
        v_start_timestamp,
        CURRENT_TIMESTAMP()
    );

EXCEPTION WHEN ERROR THEN
    -- Catch any unhandled errors and log them before exiting
    SET v_error_message = IFNULL(v_error_message, FORMAT('An unhandled error occurred in %s: %s', v_job_name, @@error.message));
    IF v_job_status = 'SUCCESS' THEN -- Only log if it's not already logged as FAILED
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('UnhandledError' AS reason, 'SQL_Error' AS sql_error_type, @@error.message AS sql_error_message)));
    END IF;
    -- Ensure audit entry is written even for failure
    INSERT INTO project.dataset.job_audit (
        audit_timestamp,
        job_name,
        job_id,
        entry_number,
        reference_date,
        target_table,
        processed_record_count,
        status,
        start_timestamp,
        end_timestamp
    )
    VALUES (
        CURRENT_TIMESTAMP(),
        v_job_name,
        p_JobKennung,
        p_EintragsNr,
        v_reference_date,
        'PoolBasisprodukt',
        IFNULL(v_processed_record_count, 0),
        'FAILED',
        v_start_timestamp,
        CURRENT_TIMESTAMP()
    );
    RAISE; -- Re-raise the error for external orchestration systems to catch
END;
```

---

## Migration Validation Tests

### 1. Test Case: Successful Execution - Happy Path

*   **Purpose:** Verify the end-to-end execution of the migrated job with valid inputs, ensuring all orchestration steps, parameter passing, and core logic invocation (placeholder) work as expected. This covers output parity for audit logs and basic data insertion.
*   **Setup:**
    1.  Ensure `project.dataset.error_log`, `project.dataset.job_audit`, and `project.dataset.poolbasisprodukt` tables exist (as per DDL).
    2.  Clear all data from these tables before execution to ensure a clean state.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        TRUNCATE TABLE project.dataset.poolbasisprodukt;
        ```
*   **Action:** Execute the main orchestration procedure with valid parameters.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB001',
        'ENTRY001',
        '01012023', -- DDMMYYYY format
        'RESTART0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **No errors raised:** The `CALL` statement completes successfully without an exception.
    2.  **Audit Log Entry:** Exactly one row exists in `project.dataset.job_audit` with:
        *   `job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'`
        *   `job_id = 'JOB001'`
        *   `entry_number = 'ENTRY001'`
        *   `reference_date = DATE '2023-01-01'`
        *   `target_table = 'PoolBasisprodukt'`
        *   `processed_record_count = 10` (based on placeholder `d_ausd_bp_ta_apn_vertrag_proc`)
        *   `status = 'SUCCESS'`
        ```sql
        SELECT COUNT(*) FROM project.dataset.job_audit
        WHERE job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'
          AND job_id = 'JOB001'
          AND entry_number = 'ENTRY001'
          AND reference_date = DATE '2023-01-01'
          AND target_table = 'PoolBasisprodukt'
          AND processed_record_count = 10
          AND status = 'SUCCESS';
        -- Expected: 1
        ```
    3.  **Error Log Empty:** No new rows are inserted into `project.dataset.error_log`.
        ```sql
        SELECT COUNT(*) FROM project.dataset.error_log;
        -- Expected: 0
        ```
    4.  **Target Data Correctness:** Exactly 10 rows exist in `project.dataset.poolbasisprodukt` with the correct parameters passed from the orchestrator.
        ```sql
        SELECT COUNT(*) FROM project.dataset.poolbasisprodukt
        WHERE job_kennung = 'JOB001'
          AND eintragsnr = 'ENTRY001'
          AND stichtag = DATE '2023-01-01'
          AND wiederanlauf_wert = 'RESTART0'
          AND datum_heute = CURRENT_DATE()
          AND datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        -- Expected: 10
        ```

### 2. Test Case: Missing Required Parameter - `p_JobKennung`

*   **Purpose:** Verify that the job correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and failing gracefully. This tests parameter validation and error handling.
*   **Setup:**
    1.  Clear `project.dataset.error_log` and `project.dataset.job_audit`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        ```
*   **Action:** Attempt to execute the procedure with `p_JobKennung` as `NULL` or an empty string.
    ```sql
    -- Test with NULL
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        NULL,
        'ENTRY001',
        '01012023',
        'RESTART0'
    );
    -- Test with empty string (run separately or adapt)
    -- CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
    --     '',
    --     'ENTRY001',
    --     '01012023',
    --     'RESTART0'
    -- );
    ```
*   **Pass/Fail Criterion:**
    1.  **Error Raised:** The `CALL` statement raises an exception with a message indicating the missing parameter.
    2.  **Error Log Entry:** Exactly one row exists in `project.dataset.error_log` with:
        *   `job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'`
        *   `severity = 'ERROR'`
        *   `error_message` containing "Job ID (p_JobKennung) cannot be empty."
        *   `additional_info` indicating `p_JobKennung` as the missing parameter.
        ```sql
        SELECT COUNT(*) FROM project.dataset.error_log
        WHERE job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'
          AND severity = 'ERROR'
          AND error_message LIKE '%Job ID (p_JobKennung) cannot be empty%';
        -- Expected: 1
        ```
    3.  **Audit Log Entry (Failed):** Exactly one row exists in `project.dataset.job_audit` with:
        *   `job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'`
        *   `job_id` is `NULL` or the provided empty string.
        *   `status = 'FAILED'`
        *   `processed_record_count = 0`
        ```sql
        SELECT COUNT(*) FROM project.dataset.job_audit
        WHERE job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'
          AND (job_id IS NULL OR job_id = '')
          AND status = 'FAILED'
          AND processed_record_count = 0;
        -- Expected: 1
        ```

### 3. Test Case: Missing Required Parameter - `p_EintragsNr`

*   **Purpose:** Verify that the job correctly identifies and handles a missing `p_EintragsNr` parameter, logging an error and failing gracefully.
*   **Setup:**
    1.  Clear `project.dataset.error_log` and `project.dataset.job_audit`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        ```
*   **Action:** Attempt to execute the procedure with `p_EintragsNr` as `NULL`.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB001',
        NULL,
        '01012023',
        'RESTART0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Error Raised:** The `CALL` statement raises an exception.
    2.  **Error Log Entry:** One row in `project.dataset.error_log` with `severity = 'ERROR'` and `error_message` indicating "Entry Number (p_EintragsNr) cannot be empty."
    3.  **Audit Log Entry (Failed):** One row in `project.dataset.job_audit` with `status = 'FAILED'` and `processed_record_count = 0`.

### 4. Test Case: Missing Required Parameter - `p_Stichtag_Str`

*   **Purpose:** Verify that the job correctly identifies and handles a missing `p_Stichtag_Str` parameter, logging an error and failing gracefully.
*   **Setup:**
    1.  Clear `project.dataset.error_log` and `project.dataset.job_audit`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        ```
*   **Action:** Attempt to execute the procedure with `p_Stichtag_Str` as `NULL`.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB001',
        'ENTRY001',
        NULL,
        'RESTART0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Error Raised:** The `CALL` statement raises an exception.
    2.  **Error Log Entry:** One row in `project.dataset.error_log` with `severity = 'ERROR'` and `error_message` indicating "Reference Date (p_Stichtag_Str) cannot be empty."
    3.  **Audit Log Entry (Failed):** One row in `project.dataset.job_audit` with `status = 'FAILED'` and `processed_record_count = 0`.

### 5. Test Case: Invalid Date Format for `p_Stichtag_Str`

*   **Purpose:** Verify that the job correctly validates the `DDMMYYYY` format for `p_Stichtag_Str`, logging an error and failing if the format is incorrect. This tests transformation correctness for type handling and validation.
*   **Setup:**
    1.  Clear `project.dataset.error_log` and `project.dataset.job_audit`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        ```
*   **Action:** Attempt to execute the procedure with an invalid date format for `p_Stichtag_Str`.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB001',
        'ENTRY001',
        '2023-01-01', -- Invalid format (YYYY-MM-DD)
        'RESTART0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Error Raised:** The `CALL` statement raises an exception.
    2.  **Error Log Entry:** Exactly one row exists in `project.dataset.error_log` with:
        *   `job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'`
        *   `severity = 'ERROR'`
        *   `error_message` containing "Invalid date format for p_Stichtag_Str: '2023-01-01'. Expected DDMMYYYY."
        *   `additional_info` indicating `InvalidDateFormat`.
        ```sql
        SELECT COUNT(*) FROM project.dataset.error_log
        WHERE job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'
          AND severity = 'ERROR'
          AND error_message LIKE '%Invalid date format for p_Stichtag_Str: "2023-01-01". Expected DDMMYYYY%';
        -- Expected: 1
        ```
    3.  **Audit Log Entry (Failed):** Exactly one row exists in `project.dataset.job_audit` with:
        *   `job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'`
        *   `status = 'FAILED'`
        *   `processed_record_count = 0`
        *   `reference_date` is `NULL` (as parsing failed).
        ```sql
        SELECT COUNT(*) FROM project.dataset.job_audit
        WHERE job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'
          AND status = 'FAILED'
          AND processed_record_count = 0
          AND reference_date IS NULL;
        -- Expected: 1
        ```

### 6. Test Case: `p_wiederanlaufWert` Default Handling

*   **Purpose:** Verify that `p_wiederanlaufWert` correctly defaults to '0' if not provided, mimicking the legacy script's `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0 fi` logic. This tests transformation correctness for NULL handling and default values.
*   **Setup:**
    1.  Clear `project.dataset.error_log`, `project.dataset.job_audit`, and `project.dataset.poolbasisprodukt`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        TRUNCATE TABLE project.dataset.poolbasisprodukt;
        ```
*   **Action:** Execute the procedure with `p_wiederanlaufWert` as `NULL`.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB002',
        'ENTRY002',
        '02022023',
        NULL -- p_wiederanlaufWert is NULL
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **No errors raised:** The `CALL` statement completes successfully.
    2.  **Audit Log Entry:** One row in `project.dataset.job_audit` with `status = 'SUCCESS'` and `processed_record_count = 10`.
    3.  **Target Data Correctness:** Exactly 10 rows exist in `project.dataset.poolbasisprodukt` where `wiederanlauf_wert = '0'`.
        ```sql
        SELECT COUNT(*) FROM project.dataset.poolbasisprodukt
        WHERE job_kennung = 'JOB002'
          AND wiederanlauf_wert = '0';
        -- Expected: 10
        ```

### 7. Test Case: Date Calculation Correctness (`v_datum_heute`, `v_datum_gestern`)

*   **Purpose:** Verify that `v_datum_heute` and `v_datum_gestern` are correctly calculated using BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions, mirroring the `gestern.ksh` script's functionality. This tests transformation correctness for date calculations.
*   **Setup:**
    1.  Clear `project.dataset.error_log`, `project.dataset.job_audit`, and `project.dataset.poolbasisprodukt`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        TRUNCATE TABLE project.dataset.poolbasisprodukt;
        ```
    2.  Note the current date and yesterday's date for comparison.
        *   `today = CURRENT_DATE()`
        *   `yesterday = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`
*   **Action:** Execute the procedure with valid parameters.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB003',
        'ENTRY003',
        '03032023',
        'RESTART1'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **No errors raised:** The `CALL` statement completes successfully.
    2.  **Target Data Correctness:** Exactly 10 rows exist in `project.dataset.poolbasisprodukt` where `datum_heute` matches `CURRENT_DATE()` and `datum_gestern` matches `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` at the time of execution.
        ```sql
        SELECT COUNT(*) FROM project.dataset.poolbasisprodukt
        WHERE job_kennung = 'JOB003'
          AND datum_heute = CURRENT_DATE()
          AND datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        -- Expected: 10
        ```

### 8. Test Case: Error During Core Data Transformation (`d_ausd_bp_ta_apn_vertrag_proc`)

*   **Purpose:** Verify that the orchestration procedure (`r_ausd_bp_ta_apn_vertrag_proc`) correctly handles errors originating from the core data transformation procedure (`d_ausd_bp_ta_apn_vertrag_proc`), logging the error and marking the job as failed. This tests error handling and external system replacement (SQL script execution).
*   **Setup:**
    1.  Clear `project.dataset.error_log` and `project.dataset.job_audit`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        ```
    2.  **Temporarily modify `d_ausd_bp_ta_apn_vertrag_proc` to intentionally raise an error.**
        ```sql
        CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_bp_ta_apn_vertrag_proc(
            p_EintragsNr STRING, p_JobKennung STRING, p_Stichtag DATE, p_wiederanlaufWert STRING,
            p_datum_heute DATE, p_datum_gestern DATE, OUT processed_rows INT64
        )
        BEGIN
            RAISE; -- Force an error
        END;
        ```
*   **Action:** Execute the main orchestration procedure with valid parameters.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB004',
        'ENTRY004',
        '04042023',
        'RESTART2'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Error Raised:** The `CALL` statement for `r_ausd_bp_ta_apn_vertrag_proc` raises an exception.
    2.  **Error Log Entries:** Exactly two rows exist in `project.dataset.error_log`:
        *   One from `d_ausd_bp_ta_apn_vertrag_proc` (due to the `RAISE;` statement).
        *   One from `r_ausd_bp_ta_apn_vertrag_proc` indicating an error during the execution of `d_ausd_bp_ta_apn_vertrag_proc`.
        ```sql
        SELECT COUNT(*) FROM project.dataset.error_log
        WHERE job_name = 'd_ausd_bp_ta_apn_vertrag_proc' AND severity = 'ERROR';
        -- Expected: 1
        SELECT COUNT(*) FROM project.dataset.error_log
        WHERE job_name = 'k_ausd_bp_ta_apn_vertrag.ksh' AND severity = 'ERROR'
          AND error_message LIKE '%Error executing d_ausd_bp_ta_apn_vertrag_proc%';
        -- Expected: 1
        ```
    3.  **Audit Log Entry (Failed):** Exactly one row exists in `project.dataset.job_audit` with:
        *   `job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'`
        *   `job_id = 'JOB004'`
        *   `entry_number = 'ENTRY004'`
        *   `reference_date = DATE '2023-04-04'`
        *   `status = 'FAILED'`
        *   `processed_record_count = 0` (as the core procedure failed before processing records).
        ```sql
        SELECT COUNT(*) FROM project.dataset.job_audit
        WHERE job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'
          AND job_id = 'JOB004'
          AND entry_number = 'ENTRY004'
          AND reference_date = DATE '2023-04-04'
          AND status = 'FAILED'
          AND processed_record_count = 0;
        -- Expected: 1
        ```
*   **Cleanup:** Revert `d_ausd_bp_ta_apn_vertrag_proc` to its original (placeholder) implementation.

### 9. Test Case: Empty `p_wiederanlaufWert` (Explicit Empty String)

*   **Purpose:** Verify that `p_wiederanlaufWert` correctly defaults to '0' when an explicit empty string is passed, similar to the `NULL` case. This covers an edge case for string parameters.
*   **Setup:**
    1.  Clear `project.dataset.error_log`, `project.dataset.job_audit`, and `project.dataset.poolbasisprodukt`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        TRUNCATE TABLE project.dataset.poolbasisprodukt;
        ```
*   **Action:** Execute the procedure with `p_wiederanlaufWert` as an empty string.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB005',
        'ENTRY005',
        '05052023',
        '' -- p_wiederanlaufWert is an empty string
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **No errors raised:** The `CALL` statement completes successfully.
    2.  **Audit Log Entry:** One row in `project.dataset.job_audit` with `status = 'SUCCESS'` and `processed_record_count = 10`.
    3.  **Target Data Correctness:** Exactly 10 rows exist in `project.dataset.poolbasisprodukt` where `wiederanlauf_wert = '0'`.
        ```sql
        SELECT COUNT(*) FROM project.dataset.poolbasisprodukt
        WHERE job_kennung = 'JOB005'
          AND wiederanlauf_wert = '0';
        -- Expected: 10
        ```

### 10. Test Case: `d_ausd_bp_ta_apn_vertrag_proc` Processes Zero Records

*   **Purpose:** Verify that the orchestration procedure correctly logs `processed_record_count = 0` in the `job_audit` table if the core data transformation procedure processes no records. This tests data quality/row count assertions.
*   **Setup:**
    1.  Clear `project.dataset.error_log`, `project.dataset.job_audit`, and `project.dataset.poolbasisprodukt`.
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_audit;
        TRUNCATE TABLE project.dataset.poolbasisprodukt;
        ```
    2.  **Temporarily modify `d_ausd_bp_ta_apn_vertrag_proc` to process zero records.**
        ```sql
        CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_bp_ta_apn_vertrag_proc(
            p_EintragsNr STRING, p_JobKennung STRING, p_Stichtag DATE, p_wiederanlaufWert STRING,
            p_datum_heute DATE, p_datum_gestern DATE, OUT processed_rows INT64
        )
        BEGIN
            -- Simulate no rows being processed
            SET processed_rows = 0;
        END;
        ```
*   **Action:** Execute the main orchestration procedure with valid parameters.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
        'JOB006',
        'ENTRY006',
        '06062023',
        'RESTART3'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **No errors raised:** The `CALL` statement completes successfully.
    2.  **Audit Log Entry:** Exactly one row exists in `project.dataset.job_audit` with:
        *   `job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'`
        *   `job_id = 'JOB006'`
        *   `status = 'SUCCESS'`
        *   `processed_record_count = 0`.
        ```sql
        SELECT COUNT(*) FROM project.dataset.job_audit
        WHERE job_name = 'k_ausd_bp_ta_apn_vertrag.ksh'
          AND job_id = 'JOB006'
          AND status = 'SUCCESS'
          AND processed_record_count = 0;
        -- Expected: 1
        ```
    3.  **Target Data Empty:** No rows are inserted into `project.dataset.poolbasisprodukt`.
        ```sql
        SELECT COUNT(*) FROM project.dataset.poolbasisprodukt;
        -- Expected: 0
        ```
*   **Cleanup:** Revert `d_ausd_bp_ta_apn_vertrag_proc` to its original (placeholder) implementation.