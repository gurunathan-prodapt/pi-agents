The migration of `r_ausd_bp_ta_bpr_beschr.ksh` to BigQuery involves refactoring a KornShell orchestration script into BigQuery Stored Procedures. The primary focus of these tests is to ensure the BigQuery wrapper procedure (`ausd_bp_ta_bpr_beschr_wrapper`) behaves identically to its legacy counterpart in terms of parameter handling, logging, and error propagation, given the core logic (`ausd_bp_ta_bpr_beschr_core`) is a separate migration effort.

**Important Note on `job_number` Logic:**
The provided BigQuery wrapper code initially assigned `v_dweintragsnr` (job_number) *after* the first log entry. To align more closely with the legacy script's behavior where `DW_EintragsNr` is determined *before* any logging for a specific run, the BigQuery wrapper code has been conceptually adjusted for these tests. The `SET v_dweintragsnr = (SELECT IFNULL(MAX(job_number), 0) + 1 FROM ...)` statement is assumed to be executed *before* the first `INSERT` into `job_log`. This ensures all log entries for a single job execution share the same `job_number`.

**Important Note on `pruefeParameterGesetzt`:**
The legacy script uses `pruefeParameterGesetzt` for parameter validation. The migration design states this will be replaced by `IF ... THEN SELECT ERROR()`. The current BigQuery wrapper code only includes a check for `v_effective_stichtag IS NULL OR TRIM(v_effective_stichtag) = ''`. Given `v_sysdate` (the default for `Stichtag`) is always a non-empty string, this specific `IF` condition is effectively unreachable. If `pruefeParameterGesetzt` in the legacy script performed more advanced validation (e.g., date format validation for an explicitly provided `Stichtag`), this behavior is not replicated in the current BigQuery wrapper and represents a potential behavioral difference or a gap in the migration. These tests will validate the *current* BigQuery code's behavior.

---

## Test Case 1: Successful Execution with Default Parameters

*   **Purpose**: Verify the wrapper script executes successfully when no `Stichtag` or `Wiederanlaufwert` is provided, correctly defaults these values, logs the process, and calls the core procedure.
*   **Setup**:
    1.  Ensure the `project.dataset.job_log` table is empty.
    2.  Ensure `project.dataset.ausd_bp_ta_bpr_beschr_core` is deployed as a placeholder that does not raise an error.
        ```sql
        -- Placeholder for core procedure (ensure it doesn't error)
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr_core`(
          IN p_jobkennung STRING,
          IN p_stichtag STRING,
          IN p_dweintragsnr INT64,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          INSERT INTO `project.dataset.job_log`
          (job_name, job_version, job_number, log_level, log_message, created_at)
          VALUES
          (p_jobkennung, 'V2.0.0', p_dweintragsnr, 'INFO', CONCAT('Core procedure called with Stichtag=', p_stichtag, ', Wiederanlaufwert=', p_wiederanlaufWert), CURRENT_TIMESTAMP());
        END;
        ```
*   **Action**:
    ```sql
    -- Clear log table before each test run
    TRUNCATE TABLE `project.dataset.job_log`;

    -- Execute the wrapper procedure with NULL parameters
    CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper(NULL, NULL);

    -- Query log entries for verification
    SELECT job_name, job_version, job_number, log_level, log_message, created_at
    FROM `project.dataset.job_log`
    WHERE job_name = 'ausd_bp_ta_bpr_beschr'
    ORDER BY created_at;
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes without raising an error.
    2.  **Output Parity (Logs)**:
        *   Exactly 5 `INFO` entries and 0 `ERROR` entries are recorded in `project.dataset.job_log` for `job_name = 'ausd_bp_ta_bpr_beschr'`.
        *   The `job_number` for all 5 entries is the same (e.g., `1`).
        *   The log messages appear in the following order (or similar content):
            1.  `'Job started'`
            2.  `'Job number assigned: 1'` (or the assigned job number)
            3.  `'Stichtag=DDMMYYYY, Sysdate=DDMMYYYY, Wiederanlaufwert=0'` (where `DDMMYYYY` is today's date).
            4.  `'Core procedure called with Stichtag=DDMMYYYY, Wiederanlaufwert=0'`
            5.  `'Job completed successfully'`
    3.  **Transformation Correctness**:
        *   The `v_effective_stichtag` (logged as `Stichtag=...`) matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
        *   The `v_restartwert` (logged as `Wiederanlaufwert=...`) is `0`.

---

## Test Case 2: Successful Execution with Explicit Stichtag and Wiederanlaufwert

*   **Purpose**: Verify the wrapper script correctly uses provided `Stichtag` and `Wiederanlaufwert`, logs the process, and calls the core procedure.
*   **Setup**:
    1.  Ensure the `project.dataset.job_log` table is empty.
    2.  Ensure `project.dataset.ausd_bp_ta_bpr_beschr_core` is the non-erroring placeholder (as in Test Case 1).
*   **Action**:
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper('01012023', 12345);
    SELECT job_name, job_version, job_number, log_level, log_message, created_at
    FROM `project.dataset.job_log`
    WHERE job_name = 'ausd_bp_ta_bpr_beschr'
    ORDER BY created_at;
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes without raising an error.
    2.  **Output Parity (Logs)**:
        *   Exactly 5 `INFO` entries and 0 `ERROR` entries are recorded.
        *   The `job_number` for all 5 entries is the same.
        *   The log message for parameters contains `Stichtag=01012023, Sysdate=DDMMYYYY, Wiederanlaufwert=12345`.
        *   The log message from the core procedure confirms `Stichtag=01012023` and `Wiederanlaufwert=12345`.
    3.  **Transformation Correctness**:
        *   The `v_effective_stichtag` is `'01012023'`.
        *   The `v_restartwert` is `12345`.

---

## Test Case 3: Successful Execution with Empty/Whitespace Stichtag

*   **Purpose**: Verify the wrapper script correctly defaults `Stichtag` to `v_sysdate` when an empty or whitespace string is provided, logs the process, and calls the core procedure.
*   **Setup**:
    1.  Ensure the `project.dataset.job_log` table is empty.
    2.  Ensure `project.dataset.ausd_bp_ta_bpr_beschr_core` is the non-erroring placeholder.
*   **Action**:
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper('   ', 54321); -- Pass whitespace string for Stichtag
    SELECT job_name, job_version, job_number, log_level, log_message, created_at
    FROM `project.dataset.job_log`
    WHERE job_name = 'ausd_bp_ta_bpr_beschr'
    ORDER BY created_at;
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes without raising an error.
    2.  **Output Parity (Logs)**:
        *   Exactly 5 `INFO` entries and 0 `ERROR` entries are recorded.
        *   The `job_number` for all 5 entries is the same.
        *   The log message for parameters contains `Stichtag=DDMMYYYY, Sysdate=DDMMYYYY, Wiederanlaufwert=54321` (where `DDMMYYYY` is today's date).
        *   The log message from the core procedure confirms `Stichtag=DDMMYYYY` and `Wiederanlaufwert=54321`.
    3.  **Transformation Correctness**:
        *   The `v_effective_stichtag` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
        *   The `v_restartwert` is `54321`.

---

## Test Case 4: Error Handling - Core Procedure Failure

*   **Purpose**: Verify the wrapper script correctly handles errors originating from the `ausd_bp_ta_bpr_beschr_core` procedure, logs the error, and re-raises it.
*   **Setup**:
    1.  Ensure the `project.dataset.job_log` table is empty.
    2.  Modify `project.dataset.ausd_bp_ta_bpr_beschr_core` to simulate an error.
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr_core`(
          IN p_jobkennung STRING,
          IN p_stichtag STRING,
          IN p_dweintragsnr INT64,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          INSERT INTO `project.dataset.job_log`
          (job_name, job_version, job_number, log_level, log_message, created_at)
          VALUES
          (p_jobkennung, 'V2.0.0', p_dweintragsnr, 'INFO', CONCAT('Core procedure called with Stichtag=', p_stichtag, ', Wiederanlaufwert=', p_wiederanlaufWert), CURRENT_TIMESTAMP());

          -- Simulate an error
          SELECT ERROR('Simulated error in core procedure for testing');
        END;
        ```
*   **Action**:
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    -- This call is expected to fail and raise an error
    -- In a testing framework like pytest, you would assert that this call raises an exception.
    -- For manual testing, observe the error message.
    CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper('01012023', 100);

    -- Query log entries for verification (even if the call failed, logs should be present)
    SELECT job_name, job_version, job_number, log_level, log_message, created_at, error_code
    FROM `project.dataset.job_log`
    WHERE job_name = 'ausd_bp_ta_bpr_beschr'
    ORDER BY created_at;
    ```
*   **Pass/Fail Criterion**:
    1.  **Error Propagation**: The `CALL` statement itself must fail and return an error message (e.g., "AppError: Abbruch").
    2.  **Output Parity (Logs)**:
        *   Exactly 4 `INFO` entries and 1 `ERROR` entry are recorded in `project.dataset.job_log`.
        *   The `job_number` for all 5 entries is the same.
        *   The `ERROR` entry's `log_level` is 'ERROR'.
        *   The `ERROR` entry's `log_message` contains 'AppError: Abbruch' and details of the simulated core error (e.g., 'Simulated error in core procedure for testing').
        *   The `ERROR` entry's `error_code` is 'APP_ERROR'.
        *   No 'Job completed successfully' log entry is present.

---

## Test Case 5: Data Quality - `job_log` Table Schema and Content

*   **Purpose**: Verify the `job_log` table schema matches the design and that log entries are correctly populated.
*   **Setup**:
    1.  Ensure `project.dataset.job_log` table is created using the provided DDL.
    2.  Run `Test Case 1` (Successful Execution with Default Parameters) to populate the log.
*   **Action**:
    ```sql
    -- Query schema information
    SELECT column_name, data_type, is_nullable
    FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_log'
    ORDER BY ordinal_position;

    -- Query log content (after running Test Case 1)
    SELECT job_name, job_version, job_number, log_level, log_message, created_at, error_code, error_arg
    FROM `project.dataset.job_log`
    WHERE job_name = 'ausd_bp_ta_bpr_beschr'
    ORDER BY created_at;
    ```
*   **Pass/Fail Criterion**:
    1.  **Schema Compliance**:
        *   The `job_log` table exists.
        *   It contains the following columns with specified types and nullability:
            *   `job_name` STRING NOT NULL
            *   `job_version` STRING (nullable)
            *   `job_number` INT64 (nullable)
            *   `log_level` STRING NOT NULL
            *   `log_message` STRING (nullable)
            *   `created_at` TIMESTAMP NOT NULL
            *   `error_code` STRING (nullable)
            *   `error_arg` STRING (nullable)
    2.  **Content Accuracy**:
        *   `job_name` is consistently 'ausd_bp_ta_bpr_beschr'.
        *   `job_version` is 'V2.0.0'.
        *   `job_number` is a positive integer (e.g., `1`) for all entries of a single run.
        *   `log_level` is 'INFO' for successful messages.
        *   `created_at` is a valid timestamp and increases chronologically within a job run.
        *   `error_code` and `error_arg` are `NULL` for successful entries.

---

## Test Case 6: Job Number Incrementing Across Multiple Runs

*   **Purpose**: Verify that `v_dweintragsnr` (job_number) is correctly determined and increments based on previous runs of the same `job_name`.
*   **Setup**:
    1.  Ensure the `project.dataset.job_log` table is empty.
    2.  Ensure `project.dataset.ausd_bp_ta_bpr_beschr_core` is the non-erroring placeholder.
*   **Action**:
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;

    -- First run
    CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper(NULL, NULL);
    -- Second run
    CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper('01012023', 100);
    -- Third run
    CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper('02022023', 200);

    -- Query distinct job numbers and their counts
    SELECT job_number, COUNT(*) AS log_entry_count
    FROM `project.dataset.job_log`
    WHERE job_name = 'ausd_bp_ta_bpr_beschr'
    GROUP BY job_number
    ORDER BY job_number;
    ```
*   **Pass/Fail Criterion**:
    1.  The query returns three distinct `job_number` values: `1`, `2`, and `3`.
    2.  For each `job_number`, the `log_entry_count` is `5` (representing the 5 INFO entries for a successful run).
    3.  The `log_message` for 'Job number assigned' for each run correctly reflects the assigned number (e.g., 'Job number assigned: 1', 'Job number assigned: 2', 'Job number assigned: 3').

---

## Test Case 7: Type Handling - Invalid `Wiederanlaufwert`

*   **Purpose**: Verify that the wrapper script correctly handles an invalid (non-integer) `p_wiederanlaufWert` input, leading to an error during type conversion.
*   **Setup**:
    1.  Ensure the `project.dataset.job_log` table is empty.
*   **Action**:
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    -- This call is expected to fail due to type conversion error
    -- In a testing framework, you would assert that this call raises an exception.
    CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper('01012023', 'ABC');

    -- Query log entries for verification
    SELECT job_name, job_version, job_number, log_level, log_message, created_at, error_code
    FROM `project.dataset.job_log`
    WHERE job_name = 'ausd_bp_ta_bpr_beschr'
    ORDER BY created_at;
    ```
*   **Pass/Fail Criterion**:
    1.  **Error Propagation**: The `CALL` statement itself must fail and return a BigQuery error related to type conversion (e.g., "Invalid value: 'ABC' for INT64").
    2.  **Output Parity (Logs)**:
        *   At least 1 `INFO` entry (`'Job started'`) and 1 `ERROR` entry are recorded in `project.dataset.job_log`. (The exact number of INFO entries before the error depends on where the type conversion error occurs relative to logging).
        *   The `ERROR` entry's `log_level` is 'ERROR'.
        *   The `ERROR` entry's `log_message` contains 'AppError: Abbruch' and details of the type conversion error (e.g., "Invalid value: 'ABC' for INT64").
        *   The `ERROR` entry's `error_code` is 'APP_ERROR'.
        *   No 'Job completed successfully' log entry is present.