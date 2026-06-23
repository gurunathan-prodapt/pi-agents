As a senior data-migration QA engineer, I have reviewed the provided KornShell script, the migration design document, and the generated BigQuery SQL code. My focus is on ensuring behavioral equivalence, data integrity, and robust error handling in the migrated solution.

The migration design correctly identifies the core components and their BigQuery counterparts. However, a critical discrepancy has been identified in the generated BigQuery stored procedure's error handling for initial parameter validation, which deviates from the design's intent to track all job attempts in `job_control` and the legacy script's behavior. This will be highlighted in the relevant test cases.

Below are the migration validation tests, structured with purpose, setup, action, and pass/fail criteria, including runnable SQL assertions where applicable.

---

## Migration Validation Tests for `r_ausd_v_ta_period.ksh` to BigQuery

**Assumptions for Test Execution:**
*   A BigQuery project (`your-gcp-project-id`) and dataset (`your_dataset_id`) exist.
*   The DDLs for `job_control`, `job_log`, and `job_error_log` have been executed in the target dataset.
*   The `project.dataset.vertragsdatenabgleich_wrapper` and `project.dataset.k_ausd_v_ta_period` stored procedures have been created.
*   For tests simulating core logic failure, the `k_ausd_v_ta_period` procedure will be temporarily modified as described in the setup.
*   All `created_ts` and `finished_ts` comparisons should account for potential minor time differences (e.g., within a few seconds).

**Common Setup (Pre-requisite for all tests):**
Before running each test, the following BigQuery tables should be cleared to ensure test isolation:
```sql
TRUNCATE TABLE `your-gcp-project-id.your_dataset_id.job_control`;
TRUNCATE TABLE `your-gcp-project-id.your_dataset_id.job_log`;
TRUNCATE TABLE `your-gcp-project-id.your_dataset_id.job_error_log`;
```

---

### Test Case 1: Happy Path Execution (Valid Parameters)

*   **Purpose:** Verify that the migrated wrapper executes successfully with valid input parameters, correctly logs job status, and invokes the core business logic. This covers output parity, transformation correctness for job control, and core logic invocation.
*   **Setup:**
    1.  Ensure `job_control`, `job_log`, `job_error_log` tables are empty.
    2.  Ensure `project.dataset.k_ausd_v_ta_period` is in its default, non-error-simulating state.
*   **Action:**
    *   **Legacy:** Execute the KornShell script with a valid stichtag:
        ```bash
        ./r_ausd_v_ta_period.ksh -s 01012023
        ```
    *   **Migrated:** Call the BigQuery stored procedure with valid parameters:
        ```sql
        CALL `your-gcp-project-id.your_dataset_id.vertragsdatenabgleich_wrapper`(
            p_stichtag => '01012023',
            p_log_level => 'INFO',
            p_show_help => FALSE
        );
        ```
*   **Expected Behavior (Legacy):**
    *   A log file (e.g., `BERT_V_TA_PERIOD_1.log`) is created/appended.
    *   Console output shows job start/end messages.
    *   The internal job control system (implied database) records a new job entry with status 'STARTED', then 'OK'.
    *   `k_ausd_v_ta_period.ksh` is invoked.
*   **Expected Behavior (Migrated):**
    *   A new entry is created in `job_control` with `status = 'STARTED'`, then updated to `status = 'OK'`.
    *   `job_log` contains messages indicating job start, core procedure invocation, core procedure completion, and job success.
    *   `job_error_log` remains empty.
    *   The `k_ausd_v_ta_period` stored procedure is successfully called.
*   **Pass/Fail Criterion:**
    1.  **`job_control` table:**
        *   One row exists.
        *   `job_entry_nr` is 1.
        *   `job_name` is 'BERT_V_TA_PERIOD'.
        *   `script_name` is 'r_ausd_v_ta_period.ksh'.
        *   `stichtag` is '2023-01-01'.
        *   `status` is 'OK'.
        *   `created_ts` and `finished_ts` are populated.
    2.  **`job_log` table:**
        *   At least 5 rows exist (job start, core start, core end, job success, and potentially others).
        *   Messages include: "Job started...", "Calling core procedure...", "Core procedure k_ausd_v_ta_period started (mock).", "Core procedure k_ausd_v_ta_period completed successfully (mock).", "Job BERT_V_TA_PERIOD finished successfully...".
        *   All `job_entry_nr` values match the one from `job_control`.
    3.  **`job_error_log` table:**
        *   Zero rows exist.

*   **Test Code (SQL Assertions):**
    ```sql
    -- Assert job_control status
    SELECT
        COUNT(1) AS row_count,
        MAX(job_entry_nr) AS max_job_entry_nr,
        MAX(job_name) AS job_name,
        MAX(script_name) AS script_name,
        MAX(stichtag) AS stichtag,
        MAX(status) AS status
    FROM `your-gcp-project-id.your_dataset_id.job_control`
    HAVING row_count = 1
       AND max_job_entry_nr = 1
       AND job_name = 'BERT_V_TA_PERIOD'
       AND script_name = 'r_ausd_v_ta_period.ksh'
       AND stichtag = '2023-01-01'
       AND status = 'OK';

    -- Assert job_log content
    SELECT
        COUNTIF(log_message LIKE 'Job started%') AS job_start_msg,
        COUNTIF(log_message LIKE 'Calling core procedure%') AS wrapper_call_core_msg,
        COUNTIF(log_message LIKE 'Core procedure k_ausd_v_ta_period started (mock)%') AS core_start_msg,
        COUNTIF(log_message LIKE 'Core procedure k_ausd_v_ta_period completed successfully (mock)%') AS core_end_msg,
        COUNTIF(log_message LIKE 'Job BERT_V_TA_PERIOD finished successfully%') AS job_success_msg,
        COUNT(DISTINCT job_entry_nr) AS distinct_job_entry_nrs
    FROM `your-gcp-project-id.your_dataset_id.job_log`
    HAVING job_start_msg = 1
       AND wrapper_call_core_msg = 1
       AND core_start_msg = 1
       AND core_end_msg = 1
       AND job_success_msg = 1
       AND distinct_job_entry_nrs = 1; -- All logs associated with the same job_entry_nr

    -- Assert job_error_log is empty
    SELECT COUNT(1) FROM `your-gcp-project-id.your_dataset_id.job_error_log` HAVING COUNT(1) = 0;
    ```

---

### Test Case 2: Help Message Display (`-h` / `p_show_help = TRUE`)

*   **Purpose:** Verify that the wrapper correctly handles the help flag, displays the usage information, and exits without performing any data processing or job control updates (except for a specific log entry in the migrated version). This covers parameter parsing and early exit logic.
*   **Setup:**
    1.  Ensure `job_control`, `job_log`, `job_error_log` tables are empty.
*   **Action:**
    *   **Legacy:** Execute the KornShell script with the help flag:
        ```bash
        ./r_ausd_v_ta_period.ksh -h
        ```
    *   **Migrated:** Call the BigQuery stored procedure with `p_show_help` set to `TRUE`:
        ```sql
        CALL `your-gcp-project-id.your_dataset_id.vertragsdatenabgleich_wrapper`(
            p_stichtag => NULL, -- Stichtag is not required for help
            p_log_level => 'INFO',
            p_show_help => TRUE
        );
        ```
*   **Expected Behavior (Legacy):**
    *   The `usage` message is printed to standard output.
    *   The script exits immediately with status 0.
    *   No log file is created/updated.
    *   No entries are made in the internal job control system.
*   **Expected Behavior (Migrated):**
    *   **Behavioral Difference Noted:** The migrated SP will insert one log message into `job_log` containing the usage information. This log entry will have `job_entry_nr` as `NULL` because `v_dw_eintrags_nr` is not determined before the help message is logged.
    *   No entries are made in `job_control`.
    *   `job_error_log` remains empty.
    *   The procedure returns successfully (does not raise an error).
*   **Pass/Fail Criterion:**
    1.  **`job_control` table:**
        *   Zero rows exist.
    2.  **`job_log` table:**
        *   Exactly one row exists.
        *   `log_message` contains "Usage: CALL `project.dataset.vertragsdatenabgleich_wrapper`...".
        *   `job_entry_nr` is `NULL`.
    3.  **`job_error_log` table:**
        *   Zero rows exist.

*   **Test Code (SQL Assertions):**
    ```sql
    -- Assert job_control is empty
    SELECT COUNT(1) FROM `your-gcp-project-id.your_dataset_id.job_control` HAVING COUNT(1) = 0;

    -- Assert job_log contains help message with NULL job_entry_nr
    SELECT
        COUNT(1) AS row_count,
        MAX(log_message) AS log_message_content,
        MAX(job_entry_nr) AS job_entry_nr_value
    FROM `your-gcp-project-id.your_dataset_id.job_log`
    HAVING row_count = 1
       AND log_message_content LIKE 'Usage: CALL `project.dataset.vertragsdatenabgleich_wrapper`%'
       AND job_entry_nr_value IS NULL;

    -- Assert job_error_log is empty
    SELECT COUNT(1) FROM `your-gcp-project-id.your_dataset_id.job_error_log` HAVING COUNT(1) = 0;
    ```

---

### Test Case 3: Missing Stichtag Parameter (`-s` missing / `p_stichtag IS NULL`)

*   **Purpose:** Verify that the wrapper correctly identifies and handles a missing required parameter (`p_stichtag`), logs the error, and terminates. This covers parameter validation and error handling.
*   **Setup:**
    1.  Ensure `job_control`, `job_log`, `job_error_log` tables are empty.
*   **Action:**
    *   **Legacy:** Execute the KornShell script without the `-s` parameter:
        ```bash
        ./r_ausd_v_ta_period.ksh
        ```
    *   **Migrated:** Call the BigQuery stored procedure with `p_stichtag` set to `NULL`:
        ```sql
        -- This call is expected to raise an error
        BEGIN
            CALL `your-gcp-project-id.your_dataset_id.vertragsdatenabgleich_wrapper`(
                p_stichtag => NULL,
                p_log_level => 'INFO',
                p_show_help => FALSE
            );
        EXCEPTION WHEN ERROR THEN
            -- Expected error, do nothing or log for external observation
            SELECT 'Caught expected error for missing stichtag.' AS status;
        END;
        ```
*   **Expected Behavior (Legacy):**
    *   `DWMSG_MeldeFehler` is called with `ErrNr=193` and `ErrArg="-s"`.
    *   `usage` message is printed.
    *   The script exits with status `193`.
    *   No log file is created/updated.
    *   No entries are made in the internal job control system.
*   **Expected Behavior (Migrated):**
    *   **Critical Behavioral Discrepancy:** The provided BigQuery SP's error handling for parameter validation is flawed.
        *   It will `RAISE BQ.ERROR` during parameter validation.
        *   This error is caught by the outer `EXCEPTION WHEN ERROR` block.
        *   However, `v_dw_eintrags_nr` is `NULL` at this point, meaning `job_control` will *not* be updated with an `ERROR` status.
        *   `job_log` and `job_error_log` will receive entries, but their `job_entry_nr` will be `NULL`, and the error messages (`v_log_message`, `v_err_nr`, `v_err_arg`) will be misleading (e.g., "Core procedure k_ausd_v_ta_period failed." with `error_nr=1`).
    *   The procedure will terminate by re-raising the error.
*   **Pass/Fail Criterion:**
    1.  **`job_control` table:**
        *   Zero rows exist. (This is a *fail* against the design, but a *pass* against the provided code's current behavior).
    2.  **`job_log` table:**
        *   At least 2 rows exist:
            *   One for "ERROR: Parameter -s (Stichtag) is missing." with `job_entry_nr` as `NULL`.
            *   One for "ERROR: Core procedure k_ausd_v_ta_period failed. Error: ..." with `job_entry_nr` as `NULL`.
    3.  **`job_error_log` table:**
        *   One row exists.
        *   `job_entry_nr` is `NULL`.
        *   `error_nr` is 1 (generic error from the catch block).
        *   `error_arg` is 'k_ausd_v_ta_period' (incorrect).
        *   `error_message` contains "Core procedure k_ausd_v_ta_period failed." (incorrect).

*   **Test Code (SQL Assertions):**
    ```sql
    -- Assert job_control is empty
    SELECT COUNT(1) FROM `your-gcp-project-id.your_dataset_id.job_control` HAVING COUNT(1) = 0;

    -- Assert job_log content for missing parameter error
    SELECT
        COUNT(1) AS row_count,
        COUNTIF(log_message LIKE 'ERROR: Parameter -s (Stichtag) is missing.%') AS missing_param_msg,
        COUNTIF(log_message LIKE 'ERROR: Core procedure k_ausd_v_ta_period failed.%') AS generic_error_msg,
        COUNTIF(job_entry_nr IS NULL) AS null_job_entry_nrs
    FROM `your-gcp-project-id.your_dataset_id.job_log`
    HAVING row_count >= 2 -- At least two error messages
       AND missing_param_msg = 1
       AND generic_error_msg = 1
       AND null_job_entry_nrs = row_count; -- All log entries have NULL job_entry_nr

    -- Assert job_error_log content for missing parameter error
    SELECT
        COUNT(1) AS row_count,
        MAX(error_nr) AS error_nr_value,
        MAX(error_arg) AS error_arg_value,
        MAX(error_message) AS error_message_content,
        MAX(job_entry_nr) AS job_entry_nr_value
    FROM `your-gcp-project-id.your_dataset_id.job_error_log`
    HAVING row_count = 1
       AND error_nr_value = 1
       AND error_arg_value = 'k_ausd_v_ta_period'
       AND error_message_content LIKE 'ERROR: Core procedure k_ausd_v_ta_period failed.%'
       AND job_entry_nr_value IS NULL;
    ```
    **QA Comment:** This test highlights a significant bug in the migrated BigQuery SP's error handling for initial parameter validation. The `job_control` table is not updated, and `job_log`/`job_error_log` entries are misleading and lack proper `job_entry_nr` association. This needs to be fixed to align with the design and legacy behavior.

---

### Test Case 4: Invalid Stichtag Format (`-s` with bad format / `PARSE_DATE` error)

*   **Purpose:** Verify that the wrapper correctly handles an invalid format for the `p_stichtag` parameter, logs the error, and terminates. This covers type handling, parameter validation, and error handling.
*   **Setup:**
    1.  Ensure `job_control`, `job_log`, `job_error_log` tables are empty.
*   **Action:**
    *   **Legacy:** Execute the KornShell script with an invalid stichtag format:
        ```bash
        ./r_ausd_v_ta_period.ksh -s 2023-01-01
        ```
    *   **Migrated:** Call the BigQuery stored procedure with an invalid `p_stichtag` format:
        ```sql
        -- This call is expected to raise an error
        BEGIN
            CALL `your-gcp-project-id.your_dataset_id.vertragsdatenabgleich_wrapper`(
                p_stichtag => '2023-01-01', -- Expected DDMMYYYY
                p_log_level => 'INFO',
                p_show_help => FALSE
            );
        EXCEPTION WHEN ERROR THEN
            -- Expected error, do nothing or log for external observation
            SELECT 'Caught expected error for invalid stichtag format.' AS status;
        END;
        ```
*   **Expected Behavior (Legacy):**
    *   `DWMSG_MeldeFehler` is called with `ErrNr=192` and `ErrArg="-s 2023-01-01"`.
    *   `usage` message is printed.
    *   The script exits with status `192`.
    *   No log file is created/updated.
    *   No entries are made in the internal job control system.
*   **Expected Behavior (Migrated):**
    *   **Critical Behavioral Discrepancy:** Similar to the missing stichtag case, the provided BigQuery SP's error handling for parameter validation is flawed.
        *   It will `RAISE BQ.ERROR` during parameter validation.
        *   This error is caught by the outer `EXCEPTION WHEN ERROR` block.
        *   `v_dw_eintrags_nr` is `NULL`, so `job_control` will *not* be updated.
        *   `job_log` and `job_error_log` will receive entries with `job_entry_nr` as `NULL` and misleading error messages.
    *   The procedure will terminate by re-raising the error.
*   **Pass/Fail Criterion:**
    1.  **`job_control` table:**
        *   Zero rows exist. (This is a *fail* against the design, but a *pass* against the provided code's current behavior).
    2.  **`job_log` table:**
        *   At least 2 rows exist:
            *   One for "ERROR: Invalid Stichtag format..." with `job_entry_nr` as `NULL`.
            *   One for "ERROR: Core procedure k_ausd_v_ta_period failed. Error: ..." with `job_entry_nr` as `NULL`.
    3.  **`job_error_log` table:**
        *   One row exists.
        *   `job_entry_nr` is `NULL`.
        *   `error_nr` is 1 (generic error from the catch block).
        *   `error_arg` is 'k_ausd_v_ta_period' (incorrect).
        *   `error_message` contains "Core procedure k_ausd_v_ta_period failed." (incorrect).

*   **Test Code (SQL Assertions):**
    ```sql
    -- Assert job_control is empty
    SELECT COUNT(1) FROM `your-gcp-project-id.your_dataset_id.job_control` HAVING COUNT(1) = 0;

    -- Assert job_log content for invalid parameter format error
    SELECT
        COUNT(1) AS row_count,
        COUNTIF(log_message LIKE 'ERROR: Invalid Stichtag format.%') AS invalid_format_msg,
        COUNTIF(log_message LIKE 'ERROR: Core procedure k_ausd_v_ta_period failed.%') AS generic_error_msg,
        COUNTIF(job_entry_nr IS NULL) AS null_job_entry_nrs
    FROM `your-gcp-project-id.your_dataset_id.job_log`
    HAVING row_count >= 2 -- At least two error messages
       AND invalid_format_msg = 1
       AND generic_error_msg = 1
       AND null_job_entry_nrs = row_count; -- All log entries have NULL job_entry_nr

    -- Assert job_error_log content for invalid parameter format error
    SELECT
        COUNT(1) AS row_count,
        MAX(error_nr) AS error_nr_value,
        MAX(error_arg) AS error_arg_value,
        MAX(error_message) AS error_message_content,
        MAX(job_entry_nr) AS job_entry_nr_value
    FROM `your-gcp-project-id.your_dataset_id.job_error_log`
    HAVING row_count = 1
       AND error_nr_value = 1
       AND error_arg_value = 'k_ausd_v_ta_period'
       AND error_message_content LIKE 'ERROR: Core procedure k_ausd_v_ta_period failed.%'
       AND job_entry_nr_value IS NULL;
    ```
    **QA Comment:** This test also highlights the same critical bug in the migrated BigQuery SP's error handling for initial parameter validation. The `job_control` table is not updated, and `job_log`/`job_error_log` entries are misleading and lack proper `job_entry_nr` association. This needs to be fixed to align with the design and legacy behavior.

---

### Test Case 5: Core Logic Failure (Simulated `k_ausd_v_ta_period` error)

*   **Purpose:** Verify that the wrapper correctly handles errors originating from the invoked core business logic procedure, updates job status to `ERROR`, and logs the failure details. This covers error trapping, `CALL` mechanism, and job status/error logging.
*   **Setup:**
    1.  Ensure `job_control`, `job_log`, `job_error_log` tables are empty.
    2.  **Modify `project.dataset.k_ausd_v_ta_period` to simulate an error:**
        ```sql
        CREATE OR REPLACE PROCEDURE `your-gcp-project-id.your_dataset_id.k_ausd_v_ta_period`(
            IN p_job_kennung STRING,
            IN p_dw_eintrags_nr INT64
        )
        BEGIN
            INSERT INTO `your-gcp-project-id.your_dataset_id.job_log` (job_name, job_entry_nr, log_message, created_ts)
            VALUES (p_job_kennung, p_dw_eintrags_nr, 'Core procedure k_ausd_v_ta_period started (mock, will fail).', CURRENT_TIMESTAMP());
            RAISE BQ.ERROR('Simulated error in core logic for testing purposes.'); -- Simulate failure
            INSERT INTO `your-gcp-project-id.your_dataset_id.job_log` (job_name, job_entry_nr, log_message, created_ts)
            VALUES (p_job_kennung, p_dw_eintrags_nr, 'Core procedure k_ausd_v_ta_period completed successfully (mock).', CURRENT_TIMESTAMP());
        END;
        ```
*   **Action:**
    *   **Legacy:** Execute the KornShell script with valid parameters, assuming `k_ausd_v_ta_period.ksh` would fail:
        ```bash
        ./r_ausd_v_ta_period.ksh -s 01012023
        # Assume k_ausd_v_ta_period.ksh exits with non-zero status
        ```
    *   **Migrated:** Call the BigQuery stored procedure with valid parameters:
        ```sql
        -- This call is expected to raise an error
        BEGIN
            CALL `your-gcp-project-id.your_dataset_id.vertragsdatenabgleich_wrapper`(
                p_stichtag => '01012023',
                p_log_level => 'INFO',
                p_show_help => FALSE
            );
        EXCEPTION WHEN ERROR THEN
            -- Expected error, do nothing or log for external observation
            SELECT 'Caught expected error from core logic failure.' AS status;
        END;
        ```
*   **Expected Behavior (Legacy):**
    *   `DWMSG_Fehlerbehandlung` is called by the `trap ERR` handler.
    *   The internal job control system updates the job entry to `status = 'ERROR'`.
    *   Error messages are logged to the log file.
    *   The script exits with a non-zero status.
*   **Expected Behavior (Migrated):**
    *   A new entry is created in `job_control` with `status = 'STARTED'`, then updated to `status = 'ERROR'`.
    *   `job_log` contains messages for job start, core procedure invocation, core procedure *start*, and then error messages from the core procedure and the wrapper's error handling.
    *   `job_error_log` contains one entry with details of the core procedure failure.
    *   The wrapper procedure will re-raise the error.
*   **Pass/Fail Criterion:**
    1.  **`job_control` table:**
        *   One row exists.
        *   `job_entry_nr` is 1.
        *   `job_name` is 'BERT_V_TA_PERIOD'.
        *   `stichtag` is '2023-01-01'.
        *   `status` is 'ERROR'.
        *   `created_ts` and `finished_ts` are populated.
    2.  **`job_log` table:**
        *   At least 5 rows exist.
        *   Messages include: "Job started...", "Calling core procedure...", "Core procedure k_ausd_v_ta_period started (mock, will fail).", "ERROR: Core procedure k_ausd_v_ta_period failed. Error: Simulated error...", "Job BERT_V_TA_PERIOD finished with ERROR...".
        *   All `job_entry_nr` values match the one from `job_control`.
    3.  **`job_error_log` table:**
        *   One row exists.
        *   `job_entry_nr` is 1.
        *   `error_nr` is 1.
        *   `error_arg` is 'k_ausd_v_ta_period'.
        *   `error_message` contains "ERROR: Core procedure k_ausd_v_ta_period failed. Error: Simulated error in core logic for testing purposes.".

*   **Test Code (SQL Assertions):**
    ```sql
    -- Assert job_control status
    SELECT
        COUNT(1) AS row_count,
        MAX(job_entry_nr) AS max_job_entry_nr,
        MAX(job_name) AS job_name,
        MAX(status) AS status
    FROM `your-gcp-project-id.your_dataset_id.job_control`
    HAVING row_count = 1
       AND max_job_entry_nr = 1
       AND job_name = 'BERT_V_TA_PERIOD'
       AND status = 'ERROR';

    -- Assert job_log content for core logic failure
    SELECT
        COUNTIF(log_message LIKE 'Job started%') AS job_start_msg,
        COUNTIF(log_message LIKE 'Calling core procedure%') AS wrapper_call_core_msg,
        COUNTIF(log_message LIKE 'Core procedure k_ausd_v_ta_period started (mock, will fail).%') AS core_start_msg,
        COUNTIF(log_message LIKE 'ERROR: Core procedure k_ausd_v_ta_period failed. Error: Simulated error%') AS core_fail_msg,
        COUNTIF(log_message LIKE 'Job BERT_V_TA_PERIOD finished with ERROR%') AS job_error_msg,
        COUNT(DISTINCT job_entry_nr) AS distinct_job_entry_nrs
    FROM `your-gcp-project-id.your_dataset_id.job_log`
    HAVING job_start_msg = 1
       AND wrapper_call_core_msg = 1
       AND core_start_msg = 1
       AND core_fail_msg = 1
       AND job_error_msg = 1
       AND distinct_job_entry_nrs = 1;

    -- Assert job_error_log content
    SELECT
        COUNT(1) AS row_count,
        MAX(job_entry_nr) AS job_entry_nr_value,
        MAX(error_nr) AS error_nr_value,
        MAX(error_arg) AS error_arg_value,
        MAX(error_message) AS error_message_content
    FROM `your-gcp-project-id.your_dataset_id.job_error_log`
    HAVING row_count = 1
       AND job_entry_nr_value = 1
       AND error_nr_value = 1
       AND error_arg_value = 'k_ausd_v_ta_period'
       AND error_message_content LIKE 'ERROR: Core procedure k_ausd_v_ta_period failed. Error: Simulated error in core logic for testing purposes.';
    ```
    **Cleanup:** Revert `project.dataset.k_ausd_v_ta_period` to its default, non-error-simulating state after this test.

---

### Test Case 6: Schema and Data Quality Assertions

*   **Purpose:** Verify that the DDLs for the control tables are correctly implemented, including column names, data types, and `NOT NULL` constraints. This covers data quality and schema assertions.
*   **Setup:**
    1.  Ensure the DDLs for `job_control`, `job_log`, `job_error_log` have been executed.
*   **Action:** Query the BigQuery `INFORMATION_SCHEMA` to inspect table schemas.
*   **Expected Behavior:** The schemas of the `job_control`, `job_log`, and `job_error_log` tables match the provided DDLs in terms of column names, data types, and nullability.
*   **Pass/Fail Criterion:**
    1.  **`job_control` schema:**
        *   Columns: `job_entry_nr` (INT64, NOT NULL), `job_name` (STRING, NOT NULL), `script_name` (STRING, NULLABLE), `log_file_name` (STRING, NULLABLE), `stichtag` (DATE, NULLABLE), `stichtag_format` (STRING, NULLABLE), `status` (STRING, NOT NULL), `created_ts` (TIMESTAMP, NOT NULL), `finished_ts` (TIMESTAMP, NULLABLE).
    2.  **`job_log` schema:**
        *   Columns: `job_name` (STRING, NOT NULL), `job_entry_nr` (INT64, NOT NULL), `log_message` (STRING, NOT NULL), `created_ts` (TIMESTAMP, NOT NULL).
    3.  **`job_error_log` schema:**
        *   Columns: `job_name` (STRING, NOT NULL), `job_entry_nr` (INT64, NOT NULL), `error_nr` (INT64, NULLABLE), `error_arg` (STRING, NULLABLE), `error_message` (STRING, NOT NULL), `created_ts` (TIMESTAMP, NOT NULL).

*   **Test Code (SQL Assertions):**
    ```sql
    -- Assert job_control schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `your-gcp-project-id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_control'
    ORDER BY ordinal_position;
    /* Expected output:
    column_name     data_type   is_nullable
    job_entry_nr    INT64       NO
    job_name        STRING      NO
    script_name     STRING      YES
    log_file_name   STRING      YES
    stichtag        DATE        YES
    stichtag_format STRING      YES
    status          STRING      NO
    created_ts      TIMESTAMP   NO
    finished_ts     TIMESTAMP   YES
    */

    -- Assert job_log schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `your-gcp-project-id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_log'
    ORDER BY ordinal_position;
    /* Expected output:
    column_name     data_type   is_nullable
    job_name        STRING      NO
    job_entry_nr    INT64       NO
    log_message     STRING      NO
    created_ts      TIMESTAMP   NO
    */

    -- Assert job_error_log schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `your-gcp-project-id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_error_log'
    ORDER BY ordinal_position;
    /* Expected output:
    column_name     data_type   is_nullable
    job_name        STRING      NO
    job_entry_nr    INT64       NO
    error_nr        INT64       YES
    error_arg       STRING      YES
    error_message   STRING      NO
    created_ts      TIMESTAMP   NO
    */
    ```

---