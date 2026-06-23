As a senior data-migration QA engineer, I've analyzed the provided migration design and generated BigQuery code for `k_ausd_v_ta_p_discount.ksh`. The following tests aim to validate the behavioral equivalence of the migrated solution, focusing on the control flow, parameter handling, logging, and the interaction with the core data transformation logic.

A critical observation is that the core data transformation logic from `d_ausd_v_ta_p_discount.sql` is a placeholder in the migrated code. Therefore, tests related to the *specific content* of the data transformation will be generalized or marked as dependent on the full implementation of `d_ausd_v_ta_p_discount`.

---

## Migration Validation Tests: `k_ausd_v_ta_p_discount.ksh` to BigQuery

### Test Environment Setup

Before running the tests, ensure the following BigQuery resources are deployed:
*   `project.dataset.ta_p_discount` table (DDL provided)
*   `project.dataset.job_log` table (DDL provided)
*   `project.dataset.error_log` table (DDL provided)
*   `project.dataset.d_ausd_v_ta_p_discount` stored procedure (placeholder implementation is sufficient for control flow tests)
*   `project.dataset.r_ausd_vertrag_control` stored procedure (main control procedure)

For testing the `d_ausd_v_ta_p_discount` procedure's behavior, we will assume a simple implementation for now: it inserts a predefined number of rows into `ta_p_discount` with the given `p_EintragsNr` and `p_JobKennung`.

```sql
-- Setup for d_ausd_v_ta_p_discount for testing purposes
-- This is a mock implementation to allow testing of the control procedure.
CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_p_discount`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING
)
BEGIN
    -- Simulate data transformation: insert 5 rows into ta_p_discount
    INSERT INTO `project.dataset.ta_p_discount` (id, eintrags_nr, job_kennung, discount_value, effective_date, end_date, created_at)
    SELECT
        GENERATE_UUID(),
        p_EintragsNr,
        p_JobKennung,
        CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) % 100 AS NUMERIC) / 100, -- Random discount
        CURRENT_DATE(),
        DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY),
        CURRENT_TIMESTAMP()
    FROM UNNEST(GENERATE_ARRAY(1, 5)) AS i; -- Insert 5 rows
END;
```

---

### Test Case 1: Successful Execution with Valid Parameters

*   **Purpose:** Verify that the migrated control procedure executes successfully when provided with all required and valid parameters, correctly orchestrates the data transformation, and logs the job status and processed record count. This covers output parity and basic transformation correctness (orchestration).
*   **Setup:**
    1.  Ensure `project.dataset.ta_p_discount`, `project.dataset.job_log`, and `project.dataset.error_log` tables are empty.
    2.  The mock `d_ausd_v_ta_p_discount` procedure is deployed and configured to insert a known number of rows (e.g., 5).
*   **Action:**
    Execute the `r_ausd_vertrag_control` procedure with valid `JobKennung` and `EintragsNr`.

    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_001', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure completes without raising an error.
    2.  `project.dataset.job_log` contains exactly one entry for `('TEST_JOB_001', 'ENTRY_001')` with `status = 'DONE'` and `records_processed = 5`.
    3.  `project.dataset.error_log` contains no new entries.
    4.  `project.dataset.ta_p_discount` contains 5 new rows with `eintrags_nr = 'ENTRY_001'` and `job_kennung = 'TEST_JOB_001'`.

    ```sql
    -- Assertion 1: No error raised (checked by execution environment)

    -- Assertion 2: Check job_log entry
    SELECT
        COUNT(*) AS entry_count,
        MAX(status) AS final_status,
        MAX(records_processed) AS final_records
    FROM `project.dataset.job_log`
    WHERE job_kennung = 'TEST_JOB_001' AND eintrags_nr = 'ENTRY_001';
    -- Expected: entry_count = 1, final_status = 'DONE', final_records = 5

    -- Assertion 3: Check error_log
    SELECT COUNT(*) FROM `project.dataset.error_log` WHERE job_kennung = 'TEST_JOB_001' AND eintrags_nr = 'ENTRY_001';
    -- Expected: 0

    -- Assertion 4: Check ta_p_discount content
    SELECT COUNT(*) FROM `project.dataset.ta_p_discount` WHERE eintrags_nr = 'ENTRY_001' AND job_kennung = 'TEST_JOB_001';
    -- Expected: 5
    ```

### Test Case 2: Missing `JobKennung` Parameter

*   **Purpose:** Verify that the migrated procedure correctly handles a missing `JobKennung` parameter, logs the specific error, and terminates gracefully, mirroring the legacy script's `ErrNr=193` behavior. This covers transformation correctness (error handling) and output parity (error logging).
*   **Setup:**
    1.  Ensure `project.dataset.job_log` and `project.dataset.error_log` tables are empty.
*   **Action:**
    Execute the `r_ausd_vertrag_control` procedure with a `NULL` or empty `JobKennung`.

    ```sql
    -- Option A: NULL JobKennung
    CALL `project.dataset.r_ausd_vertrag_control`(NULL, 'ENTRY_002');

    -- Option B: Empty string JobKennung
    -- CALL `project.dataset.r_ausd_vertrag_control`('', 'ENTRY_002');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure raises a `SQLSTATE '45000'` error with a message indicating `ErrNr=193` and `ErrArg='Jobkennung'`.
    2.  `project.dataset.error_log` contains exactly one entry for this execution with `error_number = 193`, `error_argument = 'Jobkennung'`, and `job_kennung` as `NULL` or empty.
    3.  `project.dataset.job_log` contains no entries for this execution (as validation happens before job logging).
    4.  `project.dataset.ta_p_discount` contains no new rows.

    ```sql
    -- Assertion 1: Error message check (checked by execution environment, e.g., BigQuery UI or client library)
    -- Expected error message: "FEHLER: 0 E 193 Jobkennung. Bitte ueber Rahmenscript aufrufen"

    -- Assertion 2: Check error_log entry
    SELECT
        COUNT(*) AS entry_count,
        MAX(error_number) AS err_num,
        MAX(error_argument) AS err_arg,
        MAX(job_kennung) AS job_k
    FROM `project.dataset.error_log`
    WHERE eintrags_nr = 'ENTRY_002';
    -- Expected: entry_count = 1, err_num = 193, err_arg = 'Jobkennung', job_k IS NULL (or '')

    -- Assertion 3: Check job_log
    SELECT COUNT(*) FROM `project.dataset.job_log` WHERE eintrags_nr = 'ENTRY_002';
    -- Expected: 0

    -- Assertion 4: Check ta_p_discount
    SELECT COUNT(*) FROM `project.dataset.ta_p_discount` WHERE eintrags_nr = 'ENTRY_002';
    -- Expected: 0
    ```

### Test Case 3: Missing `EintragsNr` Parameter

*   **Purpose:** Verify that the migrated procedure correctly handles a missing `EintragsNr` parameter, logs the specific error, and terminates gracefully, mirroring the legacy script's `ErrNr=193` behavior. This covers transformation correctness (error handling) and output parity (error logging).
*   **Setup:**
    1.  Ensure `project.dataset.job_log` and `project.dataset.error_log` tables are empty.
*   **Action:**
    Execute the `r_ausd_vertrag_control` procedure with a `NULL` or empty `EintragsNr`.

    ```sql
    -- Option A: NULL EintragsNr
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_003', NULL);

    -- Option B: Empty string EintragsNr
    -- CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_003', '');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure raises a `SQLSTATE '45000'` error with a message indicating `ErrNr=193` and `ErrArg='EintragsNr'`.
    2.  `project.dataset.error_log` contains exactly one entry for this execution with `error_number = 193`, `error_argument = 'EintragsNr'`, and `eintrags_nr` as `NULL` or empty.
    3.  `project.dataset.job_log` contains no entries for this execution.
    4.  `project.dataset.ta_p_discount` contains no new rows.

    ```sql
    -- Assertion 1: Error message check (checked by execution environment)
    -- Expected error message: "FEHLER: 0 E 193 EintragsNr. Bitte ueber Rahmenscript aufrufen"

    -- Assertion 2: Check error_log entry
    SELECT
        COUNT(*) AS entry_count,
        MAX(error_number) AS err_num,
        MAX(error_argument) AS err_arg,
        MAX(eintrags_nr) AS entry_nr
    FROM `project.dataset.error_log`
    WHERE job_kennung = 'TEST_JOB_003';
    -- Expected: entry_count = 1, err_num = 193, err_arg = 'EintragsNr', entry_nr IS NULL (or '')

    -- Assertion 3: Check job_log
    SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_kennung = 'TEST_JOB_003';
    -- Expected: 0

    -- Assertion 4: Check ta_p_discount
    SELECT COUNT(*) FROM `project.dataset.ta_p_discount` WHERE job_kennung = 'TEST_JOB_003';
    -- Expected: 0
    ```

### Test Case 4: Error During Data Transformation (`d_ausd_v_ta_p_discount` failure)

*   **Purpose:** Verify that if the underlying data transformation procedure (`d_ausd_v_ta_p_discount`) fails, the control procedure catches the error, logs it correctly in `error_log`, updates `job_log` to `FAILED`, and signals an error. This covers transformation correctness (error handling) and output parity.
*   **Setup:**
    1.  Ensure `project.dataset.job_log` and `project.dataset.error_log` tables are empty.
    2.  Modify the mock `d_ausd_v_ta_p_discount` procedure to intentionally raise an error.

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_p_discount`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING
    )
    BEGIN
        -- Simulate an error during data transformation
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error in d_ausd_v_ta_p_discount';
    END;
    ```
*   **Action:**
    Execute the `r_ausd_vertrag_control` procedure with valid parameters.

    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_004', 'ENTRY_004');
    ```
*   **Pass/Fail Criterion:**
    1.  The `r_ausd_vertrag_control` procedure raises a `SQLSTATE '45000'` error with a message indicating 'SQL execution failed within r_ausd_vertrag_control'.
    2.  `project.dataset.error_log` contains exactly one entry for this execution with `error_number = 999`, `error_argument = 'SQL_EXECUTION_FAILURE'`, and `error_message` containing 'Simulated error in d_ausd_v_ta_p_discount'.
    3.  `project.dataset.job_log` contains one entry for `('TEST_JOB_004', 'ENTRY_004')` with `status = 'FAILED'`. The `records_processed` might be `NULL` or 0, depending on the exact state at failure.
    4.  `project.dataset.ta_p_discount` contains no new rows.

    ```sql
    -- Assertion 1: Error message check (checked by execution environment)
    -- Expected error message: "SQL execution failed within r_ausd_vertrag_control: Simulated error in d_ausd_v_ta_p_discount"

    -- Assertion 2: Check error_log entry
    SELECT
        COUNT(*) AS entry_count,
        MAX(error_number) AS err_num,
        MAX(error_argument) AS err_arg,
        MAX(error_message) AS err_msg
    FROM `project.dataset.error_log`
    WHERE job_kennung = 'TEST_JOB_004' AND eintrags_nr = 'ENTRY_004';
    -- Expected: entry_count = 1, err_num = 999, err_arg = 'SQL_EXECUTION_FAILURE', err_msg LIKE '%Simulated error%'

    -- Assertion 3: Check job_log entry
    SELECT
        COUNT(*) AS entry_count,
        MAX(status) AS final_status
    FROM `project.dataset.job_log`
    WHERE job_kennung = 'TEST_JOB_004' AND eintrags_nr = 'ENTRY_004';
    -- Expected: entry_count = 1, final_status = 'FAILED'

    -- Assertion 4: Check ta_p_discount
    SELECT COUNT(*) FROM `project.dataset.ta_p_discount` WHERE eintrags_nr = 'ENTRY_004';
    -- Expected: 0
    ```
*   **Cleanup:** Revert `d_ausd_v_ta_p_discount` to its successful mock implementation for subsequent tests.

### Test Case 5: Record Count Accuracy

*   **Purpose:** Verify that the `records_processed` count in `job_log` accurately reflects the number of records inserted/updated by the `d_ausd_v_ta_p_discount` procedure for the specific `EintragsNr`. This covers output parity and data quality.
*   **Setup:**
    1.  Ensure `project.dataset.job_log` and `project.dataset.error_log` tables are empty.
    2.  Modify the mock `d_ausd_v_ta_p_discount` procedure to insert a *variable* number of rows based on `p_EintragsNr` (e.g., if `p_EintragsNr` ends in '1', insert 1 row; if '2', insert 2 rows, etc.).

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_p_discount`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING
    )
    BEGIN
        DECLARE num_rows INT64;
        SET num_rows = CAST(SUBSTR(p_EintragsNr, -1) AS INT64); -- Get last digit of EintragsNr

        INSERT INTO `project.dataset.ta_p_discount` (id, eintrags_nr, job_kennung, discount_value, effective_date, end_date, created_at)
        SELECT
            GENERATE_UUID(),
            p_EintragsNr,
            p_JobKennung,
            CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) % 100 AS NUMERIC) / 100,
            CURRENT_DATE(),
            DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY),
            CURRENT_TIMESTAMP()
        FROM UNNEST(GENERATE_ARRAY(1, num_rows)) AS i;
    END;
    ```
*   **Action:**
    Execute the `r_ausd_vertrag_control` procedure with different `EintragsNr` values that result in different record counts.

    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_005', 'ENTRY_001'); -- Should insert 1 row
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_005', 'ENTRY_002'); -- Should insert 2 rows
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_005', 'ENTRY_003'); -- Should insert 3 rows
    ```
*   **Pass/Fail Criterion:**
    1.  All procedures complete successfully.
    2.  `project.dataset.job_log` contains three entries, each with `status = 'DONE'`.
    3.  The `records_processed` for `ENTRY_001` is 1, for `ENTRY_002` is 2, and for `ENTRY_003` is 3.
    4.  The `ta_p_discount` table contains the corresponding number of rows for each `EintragsNr`.

    ```sql
    -- Assertion 1: No errors raised (checked by execution environment)

    -- Assertion 2 & 3: Check job_log entries for record counts
    SELECT
        eintrags_nr,
        records_processed
    FROM `project.dataset.job_log`
    WHERE job_kennung = 'TEST_JOB_005'
    ORDER BY eintrags_nr;
    -- Expected:
    -- eintrags_nr | records_processed
    -- ------------|------------------
    -- ENTRY_001   | 1
    -- ENTRY_002   | 2
    -- ENTRY_003   | 3

    -- Assertion 4: Check ta_p_discount counts
    SELECT eintrags_nr, COUNT(*) FROM `project.dataset.ta_p_discount` WHERE job_kennung = 'TEST_JOB_005' GROUP BY 1 ORDER BY 1;
    -- Expected:
    -- eintrags_nr | count
    -- ------------|------
    -- ENTRY_001   | 1
    -- ENTRY_002   | 2
    -- ENTRY_003   | 3
    ```
*   **Cleanup:** Revert `d_ausd_v_ta_p_discount` to its original mock implementation (e.g., always inserting 5 rows) if desired.

### Test Case 6: Schema and Data Type Integrity

*   **Purpose:** Verify that the DDLs for `ta_p_discount`, `job_log`, and `error_log` are correctly applied and that data types are handled as expected during insertions. This covers schema assertions and data quality.
*   **Setup:**
    1.  Ensure all DDLs are deployed.
    2.  Run a successful execution of `r_ausd_vertrag_control` (e.g., from Test Case 1).
*   **Action:**
    Inspect the schema of the tables and the data types of the inserted values.

    ```sql
    -- No direct action, inspection of metadata and data.
    ```
*   **Pass/Fail Criterion:**
    1.  The schema of `project.dataset.ta_p_discount` matches the DDL (e.g., `id` is STRING, `discount_value` is NUMERIC, `created_at` is TIMESTAMP).
    2.  The schema of `project.dataset.job_log` matches the DDL (e.g., `records_processed` is INT64, `status` is STRING).
    3.  The schema of `project.dataset.error_log` matches the DDL (e.g., `error_number` is INT64, `error_message` is STRING).
    4.  Inserted data in `ta_p_discount`, `job_log`, and `error_log` (from previous tests) conforms to the expected data types without truncation or conversion errors. For example, `created_at` should be a valid timestamp.

    ```sql
    -- Assertion 1, 2, 3: Check table schemas (using BigQuery INFORMATION_SCHEMA)
    SELECT column_name, data_type FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'ta_p_discount';
    SELECT column_name, data_type FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_log';
    SELECT column_name, data_type FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'error_log';
    -- Expected: Data types match the DDLs.

    -- Assertion 4: Sample data to check type integrity (e.g., no string in INT64 column)
    SELECT
        job_kennung,
        eintrags_nr,
        records_processed,
        status,
        created_at
    FROM `project.dataset.job_log`
    WHERE job_kennung = 'TEST_JOB_001' AND eintrags_nr = 'ENTRY_001';
    -- Expected: records_processed is an integer, created_at is a valid timestamp.
    ```

### Test Case 7: External System Replacements - Job Control Logic (Unresolved Risk)

*   **Purpose:** Validate that the BigQuery solution correctly replaces the job control logic implied by `h_alis_sqlplus.ksh` and `starteSQLSkript`, specifically "ignoring active jobs" and "deactivating older ones".
*   **Setup:**
    1.  This test requires a clear definition of the legacy `starteSQLSkript`'s job control behavior.
    2.  The current BigQuery `r_ausd_vertrag_control` procedure *does not* implement "ignoring active jobs" or "deactivating older ones". It only logs `STARTED` and then `DONE`/`FAILED`.
*   **Action:**
    1.  **Legacy:** Attempt to run `k_ausd_v_ta_p_discount.ksh` while a job with the same `JobKennung`/`EintragsNr` is already marked as 'active' in the legacy job control system.
    2.  **Migrated:** Attempt to run `r_ausd_vertrag_control` twice with the same `JobKennung`/`EintragsNr` without any explicit "active job" check.
*   **Pass/Fail Criterion:**
    *   **Legacy:** The legacy script should exit without executing `d_ausd_v_ta_p_discount.sql` if an active job is found, or perform deactivation as per its logic.
    *   **Migrated:**
        1.  The current BigQuery `r_ausd_vertrag_control` will *not* ignore an active job. It will insert a new `STARTED` entry into `job_log` and proceed to execute `d_ausd_v_ta_p_discount` again. This is a **FAIL** against the implied legacy behavior.
        2.  **To Pass:** The `r_ausd_vertrag_control` procedure (or a wrapper) needs to be enhanced to query `job_log` (or a dedicated job status table) for active jobs with the same parameters and implement the "ignore" or "deactivate" logic.

    ```sql
    -- Example of a failing scenario for the current BigQuery code:
    -- Setup: Run a job successfully
    CALL `project.dataset.r_ausd_vertrag_control`('DUPLICATE_JOB', 'DUPLICATE_ENTRY');

    -- Action: Run the same job again immediately
    CALL `project.dataset.r_ausd_vertrag_control`('DUPLICATE_JOB', 'DUPLICATE_ENTRY');

    -- Pass/Fail Criterion (for the current BigQuery code):
    -- This will likely succeed and create a second set of entries.
    SELECT job_kennung, eintrags_nr, status, records_processed FROM `project.dataset.job_log` WHERE job_kennung = 'DUPLICATE_JOB';
    -- Expected (current code): Two 'DONE' entries for ('DUPLICATE_JOB', 'DUPLICATE_ENTRY'). This is a FAIL against legacy "ignoring active jobs".
    -- Expected (if migrated correctly): Only one 'DONE' entry, or a specific error/log indicating the job was ignored/deactivated.
    ```
*   **Recommendation:** This test highlights an "Unresolved Risk" from the design document. The `r_ausd_vertrag_control` procedure needs to incorporate logic to check for existing 'STARTED' jobs for the same `JobKennung`/`EintragsNr` and act accordingly (e.g., `SIGNAL SQLSTATE` if already running, or update an existing `STARTED` entry if the legacy logic was to update rather than create new).

### Test Case 8: NULL Handling in `ta_p_discount` (Transformation Correctness)

*   **Purpose:** Verify that `d_ausd_v_ta_p_discount` correctly handles NULL values for columns that might be optional or derived from nullable sources. This is a placeholder test and depends heavily on the actual `d_ausd_v_ta_p_discount.sql` logic.
*   **Setup:**
    1.  Assume `d_ausd_v_ta_p_discount` processes source data where some columns might be NULL.
    2.  Modify the mock `d_ausd_v_ta_p_discount` to simulate inserting NULLs for specific columns.

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_p_discount`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING
    )
    BEGIN
        INSERT INTO `project.dataset.ta_p_discount` (id, eintrags_nr, job_kennung, discount_value, effective_date, end_date, created_at)
        VALUES
            (GENERATE_UUID(), p_EintragsNr, p_JobKennung, NULL, CURRENT_DATE(), NULL, CURRENT_TIMESTAMP()), -- NULL discount_value, end_date
            (GENERATE_UUID(), p_EintragsNr, p_JobKennung, 0.15, NULL, DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY), CURRENT_TIMESTAMP()); -- NULL effective_date
    END;
    ```
*   **Action:**
    Execute `r_ausd_vertrag_control` with parameters that trigger the NULL-handling scenario.

    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_006', 'ENTRY_006');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure completes successfully.
    2.  `project.dataset.ta_p_discount` contains the expected rows, and the NULL values are correctly stored in the respective columns without causing errors or unexpected default values (unless defaults are explicitly defined and desired).

    ```sql
    -- Assertion 1: No error raised (checked by execution environment)

    -- Assertion 2: Check ta_p_discount for NULLs
    SELECT
        COUNT(*) AS total_rows,
        COUNTIF(discount_value IS NULL) AS null_discount_count,
        COUNTIF(effective_date IS NULL) AS null_effective_date_count,
        COUNTIF(end_date IS NULL) AS null_end_date_count
    FROM `project.dataset.ta_p_discount`
    WHERE eintrags_nr = 'ENTRY_006';
    -- Expected (based on mock): total_rows = 2, null_discount_count = 1, null_effective_date_count = 1, null_end_date_count = 1
    ```
*   **Cleanup:** Revert `d_ausd_v_ta_p_discount` to its original mock implementation.

---

These tests provide a comprehensive framework for validating the migrated `k_ausd_v_ta_p_discount.ksh` script. The "Unresolved Risks" identified in the design document, particularly regarding the full job control logic of `starteSQLSkript`, are critical and require further investigation and implementation in the BigQuery solution to achieve true behavioral equivalence. The tests for `d_ausd_v_ta_p_discount` are placeholders and must be refined once the actual SQL transformation logic is available.