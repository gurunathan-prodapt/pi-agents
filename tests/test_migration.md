As a senior data-migration QA engineer, I've designed a comprehensive suite of migration validation tests for the `r_ausd_v_ta_vvl_upgrade.ksh` KornShell script, focusing on its re-implementation as `sp_vertragsdatenabgleich` in Google Cloud BigQuery. These tests aim to ensure behavioral equivalence, data integrity, and correct transformation logic.

**Assumptions:**
*   `PROJECT_ID` and `DATASET_ID` are placeholders for your actual Google Cloud project and BigQuery dataset. Replace them before running the tests.
*   The `sp_k_ausd_v_ta_vvl_upgrade` stored procedure, while a placeholder in the provided code, is assumed to be deployed and callable. For specific error-handling tests, temporary modifications to this procedure will be suggested.
*   All DDL and stored procedures provided in the "GENERATED MIGRATION CODE" section have been deployed to BigQuery.

---

## Migration Validation Tests for `sp_vertragsdatenabgleich`

### Test Case 1: Schema Validation of Audit and Control Tables

*   **Purpose**: To verify that the `job_audit_log` and `job_control` tables are created in BigQuery with the correct schema, data types, and nullability constraints as defined in the migration design. This ensures the foundation for logging and job control is correctly laid out.
*   **Setup**: Ensure the `job_audit_log` and `job_control` tables have been created using the provided DDL.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA` to inspect the table and column definitions.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Verify job_audit_log table schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `PROJECT_ID.DATASET_ID.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'job_audit_log'
    ORDER BY
        ordinal_position;

    -- Verify job_control table schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `PROJECT_ID.DATASET_ID.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'job_control'
    ORDER BY
        ordinal_position;
    ```

*   **Pass/Fail Criterion**:
    *   **Pass**: The query results for both tables match the expected schema, including column names, data types (e.g., `STRING`, `INT64`, `TIMESTAMP`), and `is_nullable` status (`NO` for `NOT NULL` columns). For `job_audit_log`, `job_name`, `job_entry_no`, `event_ts` should be `NOT NULL`. For `job_control`, `job_name`, `job_entry_no`, `job_status`, `updated_ts`, `status_ts` should be `NOT NULL`.
    *   **Fail**: Any discrepancy in table existence, column names, data types, or nullability.

---

### Test Case 2: Help Message Display and No-Op Execution

*   **Purpose**: To verify that calling the main stored procedure with `p_show_help = TRUE` correctly displays the usage information and exits without performing any job logic or modifying the audit/control tables, mirroring the `-h` flag behavior in the KSH script.
*   **Setup**:
    1.  Ensure `job_audit_log` and `job_control` tables are empty or in a known, clean state.
    2.  (Optional) Record the current row counts of `job_audit_log` and `job_control`.
*   **Action**: Execute the `sp_vertragsdatenabgleich` procedure with `p_show_help` set to `TRUE`.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Clean up tables before test
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_audit_log`;
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_control`;

    -- Execute the stored procedure with help flag
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(
        p_stichtag_in => NULL,
        p_log_level_in => NULL,
        p_show_help => TRUE
    );

    -- Verify no new entries were added to audit/control tables
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_audit_log`;
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_control`;
    ```

*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The BigQuery client output (or query result if using `SELECT` for help) contains the expected help messages (e.g., "Usage: CALL...", "p_stichtag_in:...", etc.).
        2.  The `COUNT(*)` queries for `job_audit_log` and `job_control` both return `0`, indicating no new entries were inserted.
    *   **Fail**:
        1.  Help messages are incorrect or missing.
        2.  Any entries are found in `job_audit_log` or `job_control`.

---

### Test Case 3: Successful Execution - Default Parameters

*   **Purpose**: To validate the "happy path" execution when no specific `stichtag` or `log_level` is provided, mimicking a simple `r_ausd_v_ta_vvl_upgrade.ksh` call. This tests default `stichtag` (current date), correct `job_entry_no` generation, and accurate logging/status updates.
*   **Setup**: Ensure `job_audit_log` and `job_control` tables are empty.
*   **Action**: Execute the `sp_vertragsdatenabgleich` procedure without specifying `p_stichtag_in` or `p_log_level_in`.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Clean up tables before test
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_audit_log`;
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_control`;

    -- Execute the stored procedure with default parameters
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(
        p_stichtag_in => NULL,
        p_log_level_in => NULL,
        p_show_help => FALSE
    );

    -- Verify job_audit_log entries
    SELECT
        job_name,
        job_entry_no,
        event_type,
        event_message,
        stichtag,
        stichtag_format
    FROM
        `PROJECT_ID.DATASET_ID.job_audit_log`
    WHERE
        job_name = 'sp_vertragsdatenabgleich'
    ORDER BY
        event_ts;

    -- Verify job_control entry
    SELECT
        job_name,
        job_entry_no,
        job_status,
        stichtag,
        stichtag_format
    FROM
        `PROJECT_ID.DATASET_ID.job_control`
    WHERE
        job_name = 'sp_vertragsdatenabgleich';
    ```

*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  `job_audit_log` contains exactly 3 entries for `job_name = 'sp_vertragsdatenabgleich'`:
            *   One `event_type = 'START'` entry.
            *   One `event_type = 'INFO'` entry from `sp_k_ausd_v_ta_vvl_upgrade`.
            *   One `event_type = 'FINISH'` entry.
        2.  The `stichtag` in all `job_audit_log` entries and the `job_control` entry matches `FORMAT_DATE('%Y%m%d', CURRENT_DATE())`.
        3.  The `job_entry_no` is consistent across all related log entries and `job_control` (expected to be `1`).
        4.  `job_control` contains one entry for `job_name = 'sp_vertragsdatenabgleich'` with `job_status = 'OK'`.
    *   **Fail**: Any deviation from the expected log entries, `stichtag`, `job_entry_no`, or `job_control` status.

---

### Test Case 4: Successful Execution - Specific Stichtag and Log Level

*   **Purpose**: To verify that the stored procedure correctly processes and logs a user-provided `stichtag` and `log_level`, mirroring the `-s` and `-l` parameters of the KSH script.
*   **Setup**: Ensure `job_audit_log` and `job_control` tables are empty.
*   **Action**: Execute the `sp_vertragsdatenabgleich` procedure with specific values for `p_stichtag_in` and `p_log_level_in`.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Clean up tables before test
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_audit_log`;
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_control`;

    -- Define test parameters
    DECLARE test_stichtag STRING DEFAULT '20231026';
    DECLARE test_log_level STRING DEFAULT 'DEBUG';

    -- Execute the stored procedure with specific parameters
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(
        p_stichtag_in => test_stichtag,
        p_log_level_in => test_log_level,
        p_show_help => FALSE
    );

    -- Verify job_audit_log entries
    SELECT
        job_name,
        job_entry_no,
        event_type,
        event_message,
        stichtag,
        stichtag_format
    FROM
        `PROJECT_ID.DATASET_ID.job_audit_log`
    WHERE
        job_name = 'sp_vertragsdatenabgleich'
    ORDER BY
        event_ts;

    -- Verify job_control entry
    SELECT
        job_name,
        job_entry_no,
        job_status,
        stichtag,
        stichtag_format
    FROM
        `PROJECT_ID.DATASET_ID.job_control`
    WHERE
        job_name = 'sp_vertragsdatenabgleich';
    ```

*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  `job_audit_log` contains exactly 3 entries as in Test Case 3.
        2.  The `stichtag` in all `job_audit_log` entries and the `job_control` entry is `'20231026'`.
        3.  The `event_message` for the `event_type = 'START'` entry in `job_audit_log` correctly reflects `log_level: DEBUG`.
        4.  `job_control` contains one entry for `job_name = 'sp_vertragsdatenabgleich'` with `job_status = 'OK'`.
        5.  The `job_entry_no` is consistent (expected `1`).
    *   **Fail**: Any deviation from the expected log entries, `stichtag`, `log_level` in messages, `job_entry_no`, or `job_control` status.

---

### Test Case 5: Error Handling - Core Kernel Script Failure

*   **Purpose**: To validate that errors originating from the core kernel logic (`sp_k_ausd_v_ta_vvl_upgrade`) are correctly caught by `sp_vertragsdatenabgleich`, logged as an error, and result in a 'FAILED' status in `job_control`, mirroring the `trap ERR` behavior in the KSH script.
*   **Setup**:
    1.  Ensure `job_audit_log` and `job_control` tables are empty.
    2.  **Crucially, temporarily modify `sp_k_ausd_v_ta_vvl_upgrade` to intentionally raise an error.** For example, change its body to:
        ```sql
        CREATE OR REPLACE PROCEDURE `PROJECT_ID.DATASET_ID.sp_k_ausd_v_ta_vvl_upgrade`(
            IN p_job_name STRING,
            IN p_job_entry_no INT64,
            IN p_stichtag STRING,
            IN p_stichtag_format STRING
        )
        BEGIN
            RAISE 'Simulated kernel error: Data reconciliation failed for stichtag %s', p_stichtag;
        END;
        ```
*   **Action**: Execute the `sp_vertragsdatenabgleich` procedure.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Clean up tables before test
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_audit_log`;
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_control`;

    -- Execute the stored procedure (this call is expected to fail)
    -- Wrap in a BEGIN...EXCEPTION block if your client doesn't handle direct SP failure well,
    -- but for testing the failure propagation, a direct call is often sufficient.
    BEGIN
        CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(
            p_stichtag_in => '20231027',
            p_log_level_in => 'INFO',
            p_show_help => FALSE
        );
    EXCEPTION WHEN ERROR THEN
        -- Expected error, do nothing or log for debugging
        SELECT 'Caught expected error from sp_vertragsdatenabgleich' AS status;
    END;

    -- Verify job_audit_log entries
    SELECT
        job_name,
        job_entry_no,
        event_type,
        error_no,
        event_message,
        stichtag
    FROM
        `PROJECT_ID.DATASET_ID.job_audit_log`
    WHERE
        job_name = 'sp_vertragsdatenabgleich'
    ORDER BY
        event_ts;

    -- Verify job_control entry
    SELECT
        job_name,
        job_entry_no,
        job_status,
        stichtag
    FROM
        `PROJECT_ID.DATASET_ID.job_control`
    WHERE
        job_name = 'sp_vertragsdatenabgleich';
    ```

*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The call to `sp_vertragsdatenabgleich` fails and propagates an error message (e.g., "Simulated kernel error...").
        2.  `job_audit_log` contains exactly 2 entries:
            *   One `event_type = 'START'` entry.
            *   One `event_type = 'ERROR'` entry, with `error_no = 1` (or a specific error code if implemented), and `event_message` containing the simulated error details.
        3.  `job_control` contains one entry for `job_name = 'sp_vertragsdatenabgleich'` with `job_status = 'FAILED'`.
        4.  The `job_entry_no` is consistent (expected `1`).
    *   **Fail**:
        1.  The `sp_vertragsdatenabgleich` procedure completes successfully without an error.
        2.  Incorrect number or type of entries in `job_audit_log`.
        3.  `job_control` status is not 'FAILED'.
    *   **Cleanup**: Revert `sp_k_ausd_v_ta_vvl_upgrade` to its original placeholder state after this test.

---

### Test Case 6: Job Entry Number Increment

*   **Purpose**: To verify that `job_entry_no` is correctly incremented for each new job run, ensuring proper sequencing and uniqueness in the audit log, similar to `DWMSG_ErmittleNr`.
*   **Setup**: Ensure `job_audit_log` and `job_control` tables are empty.
*   **Action**: Execute `sp_vertragsdatenabgleich` multiple times consecutively.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Clean up tables before test
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_audit_log`;
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_control`;

    -- Run 1 (Expected job_entry_no = 1)
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(p_stichtag_in => '20231028', p_show_help => FALSE);

    -- Run 2 (Expected job_entry_no = 2)
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(p_stichtag_in => '20231029', p_show_help => FALSE);

    -- Run 3 (Expected job_entry_no = 3)
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(p_stichtag_in => '20231030', p_show_help => FALSE);

    -- Verify job_audit_log entries for all runs
    SELECT
        job_name,
        job_entry_no,
        event_type,
        stichtag
    FROM
        `PROJECT_ID.DATASET_ID.job_audit_log`
    WHERE
        job_name = 'sp_vertragsdatenabgleich'
    ORDER BY
        job_entry_no, event_ts;

    -- Verify job_control entry (should reflect the last run)
    SELECT
        job_name,
        job_entry_no,
        job_status,
        stichtag
    FROM
        `PROJECT_ID.DATASET_ID.job_control`
    WHERE
        job_name = 'sp_vertragsdatenabgleich';
    ```

*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  `job_audit_log` contains 9 entries in total (3 runs * 3 entries per run).
        2.  The `job_entry_no` for the first run's entries is `1`, for the second run's entries is `2`, and for the third run's entries is `3`.
        3.  The `stichtag` values in `job_audit_log` correctly correspond to '20231028', '20231029', and '20231030' for `job_entry_no` 1, 2, and 3 respectively.
        4.  `job_control` contains one entry with `job_status = 'OK'`, `job_entry_no = 3`, and `stichtag = '20231030'`.
    *   **Fail**: Any inconsistency in `job_entry_no` sequencing or `job_control`'s final state.

---

### Test Case 7: `job_control` Table Update Logic

*   **Purpose**: To verify that the `job_control` table correctly reflects the latest status and `job_entry_no` for a given job, handling both initial inserts and subsequent updates (successive successful runs, or a mix of success and failure). This replaces the `DWMSG_SetzeStichtagInfo` and `DWMSG_SetzeStatusOK` logic.
*   **Setup**:
    1.  Ensure `job_audit_log` and `job_control` tables are empty.
    2.  Ensure `sp_k_ausd_v_ta_vvl_upgrade` is in its default successful state.
*   **Action**: Execute a sequence of successful and failed runs, checking `job_control` after each.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Clean up tables before test
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_audit_log`;
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_control`;

    -- --- Run 1: Successful execution ---
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(p_stichtag_in => '20231101', p_show_help => FALSE);

    -- Verify job_control after Run 1
    SELECT 'After Run 1 (Success)' AS Test_Phase, job_name, job_entry_no, job_status, stichtag FROM `PROJECT_ID.DATASET_ID.job_control` WHERE job_name = 'sp_vertragsdatenabgleich';

    -- --- Run 2: Failed execution (temporarily modify sp_k_ausd_v_ta_vvl_upgrade) ---
    -- Temporarily modify sp_k_ausd_v_ta_vvl_upgrade to raise an error:
    -- CREATE OR REPLACE PROCEDURE `PROJECT_ID.DATASET_ID.sp_k_ausd_v_ta_vvl_upgrade`(...) BEGIN RAISE 'Simulated kernel error'; END;
    BEGIN
        CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(p_stichtag_in => '20231102', p_show_help => FALSE);
    EXCEPTION WHEN ERROR THEN
        SELECT 'Caught expected error from sp_vertragsdatenabgleich for Run 2' AS status;
    END;

    -- Verify job_control after Run 2
    SELECT 'After Run 2 (Failure)' AS Test_Phase, job_name, job_entry_no, job_status, stichtag FROM `PROJECT_ID.DATASET_ID.job_control` WHERE job_name = 'sp_vertragsdatenabgleich';

    -- --- Run 3: Successful execution (revert sp_k_ausd_v_ta_vvl_upgrade) ---
    -- Revert sp_k_ausd_v_ta_vvl_upgrade to its original successful state.
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(p_stichtag_in => '20231103', p_show_help => FALSE);

    -- Verify job_control after Run 3
    SELECT 'After Run 3 (Success)' AS Test_Phase, job_name, job_entry_no, job_status, stichtag FROM `PROJECT_ID.DATASET_ID.job_control` WHERE job_name = 'sp_vertragsdatenabgleich';
    ```

*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  After Run 1: `job_control` shows `job_status = 'OK'`, `job_entry_no = 1`, `stichtag = '20231101'`.
        2.  After Run 2: `job_control` shows `job_status = 'FAILED'`, `job_entry_no = 2`, `stichtag = '20231102'`.
        3.  After Run 3: `job_control` shows `job_status = 'OK'`, `job_entry_no = 3`, `stichtag = '20231103'`.
        4.  The `updated_ts` and `status_ts` fields in `job_control` are updated with the timestamp of each respective run.
    *   **Fail**: Any incorrect status, `job_entry_no`, or `stichtag` in `job_control` after any of the runs.
    *   **Cleanup**: Ensure `sp_k_ausd_v_ta_vvl_upgrade` is reverted to its original placeholder state after this test.

---

### Test Case 8: `sp_k_ausd_v_ta_vvl_upgrade` Parameter Passing

*   **Purpose**: To verify that the `sp_vertragsdatenabgleich` orchestrator correctly passes all required parameters (`p_job_name`, `p_job_entry_no`, `p_stichtag`, `p_stichtag_format`) to the `sp_k_ausd_v_ta_vvl_upgrade` procedure. This ensures the core logic receives the correct context.
*   **Setup**:
    1.  Ensure `job_audit_log` and `job_control` tables are empty.
    2.  **Crucially, temporarily modify `sp_k_ausd_v_ta_vvl_upgrade` to log the received parameters into `job_audit_log`** (or a dedicated temporary table for more complex parameter types). For example, change its body to:
        ```sql
        CREATE OR REPLACE PROCEDURE `PROJECT_ID.DATASET_ID.sp_k_ausd_v_ta_vvl_upgrade`(
            IN p_job_name STRING,
            IN p_job_entry_no INT64,
            IN p_stichtag STRING,
            IN p_stichtag_format STRING
        )
        BEGIN
            INSERT INTO `PROJECT_ID.DATASET_ID.job_audit_log` (
                job_name,
                job_entry_no,
                event_type,
                event_message,
                stichtag,
                stichtag_format,
                event_ts
            )
            VALUES (
                p_job_name,
                p_job_entry_no,
                'INFO',
                CONCAT('Core SP received params: job_name=', p_job_name, ', job_entry_no=', CAST(p_job_entry_no AS STRING), ', stichtag=', p_stichtag, ', stichtag_format=', p_stichtag_format),
                p_stichtag,
                p_stichtag_format,
                CURRENT_TIMESTAMP()
            );
            -- Original placeholder logic (if any) or simply return
        END;
        ```
*   **Action**: Execute `sp_vertragsdatenabgleich` with specific parameters.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Clean up tables before test
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_audit_log`;
    TRUNCATE TABLE `PROJECT_ID.DATASET_ID.job_control`;

    -- Define test parameters
    DECLARE test_stichtag STRING DEFAULT '20231201';
    DECLARE test_log_level STRING DEFAULT 'DEBUG';
    DECLARE expected_job_name STRING DEFAULT 'sp_vertragsdatenabgleich';
    DECLARE expected_job_entry_no INT64 DEFAULT 1;
    DECLARE expected_stichtag_format STRING DEFAULT 'YYYYMMDD';

    -- Execute the orchestrator
    CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(
        p_stichtag_in => test_stichtag,
        p_log_level_in => test_log_level,
        p_show_help => FALSE
    );

    -- Verify the log entry from sp_k_ausd_v_ta_vvl_upgrade
    SELECT
        event_message
    FROM
        `PROJECT_ID.DATASET_ID.job_audit_log`
    WHERE
        job_name = expected_job_name
        AND event_type = 'INFO'
        AND event_message LIKE 'Core SP received params:%'
    ORDER BY
        event_ts DESC
    LIMIT 1;
    ```

*   **Pass/Fail Criterion**:
    *   **Pass**: The `event_message` retrieved from `job_audit_log` (generated by the modified `sp_k_ausd_v_ta_vvl_upgrade`) accurately reflects the parameters passed from `sp_vertragsdatenabgleich`:
        *   `job_name` should be `'sp_vertragsdatenabgleich'`.
        *   `job_entry_no` should be `1`.
        *   `stichtag` should be `'20231201'`.
        *   `stichtag_format` should be `'YYYYMMDD'`.
    *   **Fail**: Any discrepancy in the logged parameters.
    *   **Cleanup**: Revert `sp_k_ausd_v_ta_vvl_upgrade` to its original placeholder state after this test.

---