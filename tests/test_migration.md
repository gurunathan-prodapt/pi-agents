The migration of `r_ausd_v_ta_barrier_zusgf.ksh` to a BigQuery Stored Procedure (`sp_r_ausd_v_ta_barrier_zusgf`) primarily involves translating orchestration, logging, and error handling logic. The core data transformation is delegated to a "kernel script" (`k_ausd_v_ta_barrier_zusgf.ksh`), which will also be migrated to a BigQuery Stored Procedure (`sp_k_ausd_v_ta_barrier_zusgf`).

The tests below focus on validating the `sp_r_ausd_v_ta_barrier_zusgf` wrapper's behavior, including its interaction with the logging table and the kernel script.

**Assumptions for Testing:**

*   The `project.dataset.job_log` table exists as defined in the migration design.
*   We can temporarily replace the `project.dataset.sp_k_ausd_v_ta_barrier_zusgf` procedure with mock implementations during testing. This is a common practice in unit/integration testing for dependencies.
*   The `p_entry_number` parameter is expected to be provided by the caller, as the original `DWMSG_ErmittleNr` functionality is now externalized.

---

## Test Setup: Mock Kernel Procedures

Before running the tests, create these mock procedures in your BigQuery `project.dataset`. These will temporarily replace the actual `sp_k_ausd_v_ta_barrier_zusgf` during testing.

```sql
-- Mock for successful kernel script execution
-- This procedure will be temporarily named `sp_k_ausd_v_ta_barrier_zusgf` for success tests.
CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_barrier_zusgf_mock_success`(
    IN p_job_kennung STRING,
    IN p_entry_number INT64
)
BEGIN
    -- Simulate some work or internal logging if needed for more detailed kernel script tests.
    -- For this wrapper test, simply returning successfully is enough.
    SELECT FORMAT('Mock kernel script %s (Entry: %d) executed successfully.', p_job_kennung, p_entry_number) AS debug_message;
END;

-- Mock for failed kernel script execution
-- This procedure will be temporarily named `sp_k_ausd_v_ta_barrier_zusgf` for failure tests.
CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_barrier_zusgf_mock_fail`(
    IN p_job_kennung STRING,
    IN p_entry_number INT64
)
BEGIN
    -- Simulate an error that the wrapper should catch.
    RAISE USING MESSAGE = FORMAT('Simulated error from kernel script mock for JobKennung: %s, Entry: %d', p_job_kennung, p_entry_number);
END;
```

---

## Test Case 1: Successful Job Execution and Logging

*   **Purpose:** Verify that the wrapper procedure successfully orchestrates the kernel script, logs its start and successful completion, and correctly derives job metadata.
*   **Setup:**
    1.  Ensure the `project.dataset.job_log` table is empty or truncate it.
    2.  Replace the actual `sp_k_ausd_v_ta_barrier_zusgf` with `sp_k_ausd_v_ta_barrier_zusgf_mock_success`.
        ```sql
        -- Temporarily replace the kernel script with the success mock
        CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_barrier_zusgf`(
            IN p_job_kennung STRING,
            IN p_entry_number INT64
        ) AS (CALL `project.dataset.sp_k_ausd_v_ta_barrier_zusgf_mock_success`(p_job_kennung, p_entry_number));
        ```
*   **Action:** Execute the wrapper procedure with sample parameters.
    ```sql
    DECLARE v_test_job_kennung STRING DEFAULT 'MY_TEST_JOB';
    DECLARE v_test_entry_number INT64 DEFAULT 1001;
    CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
        p_job_kennung => v_test_job_kennung,
        p_entry_number => v_test_entry_number,
        p_debug_mode => TRUE,
        p_test_mode => FALSE
    );
    ```
*   **Pass/Fail Criterion:**
    *   Query the `project.dataset.job_log` table.
    *   **PASS** if:
        1.  Two entries exist for `job_number = 1001` and `job_name = 'MY_TEST_JOB'`.
        2.  One entry has `status = 'RUNNING'` and `message = 'Job started.'`.
        3.  The other entry has `status = 'SUCCESS'` and `message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'`.
        4.  Both entries have `script_name = 'sp_r_ausd_v_ta_barrier_zusgf'`.
        5.  The `stichtag` column matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `stichtag_format = 'DDMMYYYY'`.
        6.  `created_at` is populated for the 'RUNNING' entry, and `finished_at` is populated for the 'SUCCESS' entry.
    ```sql
    -- Pytest-like assertion (SQL equivalent)
    SELECT
        COUNT(1) AS total_entries,
        SUM(CASE WHEN status = 'RUNNING' AND message = 'Job started.' THEN 1 ELSE 0 END) AS running_entry_count,
        SUM(CASE WHEN status = 'SUCCESS' AND message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' THEN 1 ELSE 0 END) AS success_entry_count,
        SUM(CASE WHEN script_name = 'sp_r_ausd_v_ta_barrier_zusgf' THEN 1 ELSE 0 END) AS correct_script_name_count,
        SUM(CASE WHEN stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE()) AND stichtag_format = 'DDMMYYYY' THEN 1 ELSE 0 END) AS stichtag_match_count,
        SUM(CASE WHEN created_at IS NOT NULL THEN 1 ELSE 0 END) AS created_at_populated,
        SUM(CASE WHEN finished_at IS NOT NULL THEN 1 ELSE 0 END) AS finished_at_populated
    FROM `project.dataset.job_log`
    WHERE job_number = 1001 AND job_name = 'MY_TEST_JOB';
    -- Expected result: total_entries=2, running_entry_count=1, success_entry_count=1, correct_script_name_count=2, stichtag_match_count=2, created_at_populated=2, finished_at_populated=1
    ```

---

## Test Case 2: Error Handling and Logging

*   **Purpose:** Verify that the wrapper procedure correctly catches errors from the kernel script, logs the failure, and re-raises the error.
*   **Setup:**
    1.  Ensure the `project.dataset.job_log` table is empty or truncate it.
    2.  Replace the actual `sp_k_ausd_v_ta_barrier_zusgf` with `sp_k_ausd_v_ta_barrier_zusgf_mock_fail`.
        ```sql
        -- Temporarily replace the kernel script with the failure mock
        CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_barrier_zusgf`(
            IN p_job_kennung STRING,
            IN p_entry_number INT64
        ) AS (CALL `project.dataset.sp_k_ausd_v_ta_barrier_zusgf_mock_fail`(p_job_kennung, p_entry_number));
        ```
*   **Action:** Execute the wrapper procedure with sample parameters, expecting it to fail.
    ```sql
    DECLARE v_test_job_kennung STRING DEFAULT 'MY_FAIL_JOB';
    DECLARE v_test_entry_number INT64 DEFAULT 1002;
    -- This call is expected to raise an error, so wrap it in a try-catch if your test runner supports it.
    -- In BigQuery SQL, you'd typically just execute it and observe the error.
    BEGIN
        CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
            p_job_kennung => v_test_job_kennung,
            p_entry_number => v_test_entry_number,
            p_debug_mode => TRUE,
            p_test_mode => FALSE
        );
    EXCEPTION WHEN ERROR THEN
        SELECT 'Procedure failed as expected.' AS test_status, ERROR_MESSAGE() AS error_details;
    END;
    ```
*   **Pass/Fail Criterion:**
    *   The `CALL` statement should terminate with an error.
    *   Query the `project.dataset.job_log` table.
    *   **PASS** if:
        1.  Two entries exist for `job_number = 1002` and `job_name = 'MY_FAIL_JOB'`.
        2.  One entry has `status = 'RUNNING'` and `message = 'Job started.'`.
        3.  The other entry has `status = 'FAILED'`, `log_level = 'ERROR'`, `error_code` and `error_arg` populated, and `message` containing 'AppError: Abbruch'.
        4.  The `finished_at` timestamp is populated for the 'FAILED' entry.
    ```sql
    -- Pytest-like assertion (SQL equivalent)
    SELECT
        COUNT(1) AS total_entries,
        SUM(CASE WHEN status = 'RUNNING' AND message = 'Job started.' THEN 1 ELSE 0 END) AS running_entry_count,
        SUM(CASE WHEN status = 'FAILED' AND log_level = 'ERROR' AND message LIKE 'AppError: Abbruch%' AND error_code IS NOT NULL AND error_arg IS NOT NULL THEN 1 ELSE 0 END) AS failed_entry_count,
        SUM(CASE WHEN finished_at IS NOT NULL THEN 1 ELSE 0 END) AS finished_at_populated
    FROM `project.dataset.job_log`
    WHERE job_number = 1002 AND job_name = 'MY_FAIL_JOB';
    -- Expected result: total_entries=2, running_entry_count=1, failed_entry_count=1, finished_at_populated=1
    ```

---

## Test Case 3: Parameter Handling - `p_job_kennung` Override

*   **Purpose:** Verify that the `p_job_kennung` input parameter correctly overrides the default `JobKennung` value, is uppercased, and is used consistently in logging and passed to the kernel script.
*   **Setup:**
    1.  Ensure the `project.dataset.job_log` table is empty or truncate it.
    2.  Replace the actual `sp_k_ausd_v_ta_barrier_zusgf` with `sp_k_ausd_v_ta_barrier_zusgf_mock_success`.
*   **Action:** Execute the wrapper procedure with a custom, mixed-case `p_job_kennung`.
    ```sql
    DECLARE v_test_job_kennung STRING DEFAULT 'customJobName'; -- Mixed case
    DECLARE v_test_entry_number INT64 DEFAULT 1003;
    CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
        p_job_kennung => v_test_job_kennung,
        p_entry_number => v_test_entry_number,
        p_debug_mode => FALSE,
        p_test_mode => FALSE
    );
    ```
*   **Pass/Fail Criterion:**
    *   Query the `project.dataset.job_log` table.
    *   **PASS** if:
        1.  All entries for `job_number = 1003` have `job_name = 'CUSTOMJOBNAME'` (uppercased).
        2.  The `sp_k_ausd_v_ta_barrier_zusgf_mock_success` was called with `p_job_kennung = 'CUSTOMJOBNAME'`. (This would require inspecting BQ audit logs or modifying the mock to log its inputs, which is beyond simple SQL assertion). For this test, we rely on the `job_log` entry.
    ```sql
    -- Pytest-like assertion (SQL equivalent)
    SELECT
        COUNT(1) AS total_entries,
        SUM(CASE WHEN job_name = 'CUSTOMJOBNAME' THEN 1 ELSE 0 END) AS correct_job_name_count
    FROM `project.dataset.job_log`
    WHERE job_number = 1003;
    -- Expected result: total_entries=2, correct_job_name_count=2
    ```

---

## Test Case 4: Parameter Handling - Default `JobKennung`

*   **Purpose:** Verify that the default `JobKennung` (`BERT_V_TA_BARRIER_ZUSGF`) is used when `p_job_kennung` is `NULL` or not provided.
*   **Setup:**
    1.  Ensure the `project.dataset.job_log` table is empty or truncate it.
    2.  Replace the actual `sp_k_ausd_v_ta_barrier_zusgf` with `sp_k_ausd_v_ta_barrier_zusgf_mock_success`.
*   **Action:** Execute the wrapper procedure with `p_job_kennung` set to `NULL`.
    ```sql
    DECLARE v_test_entry_number INT64 DEFAULT 1004;
    CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
        p_job_kennung => NULL, -- Test NULL case
        p_entry_number => v_test_entry_number,
        p_debug_mode => FALSE,
        p_test_mode => FALSE
    );
    ```
*   **Pass/Fail Criterion:**
    *   Query the `project.dataset.job_log` table.
    *   **PASS** if:
        1.  All entries for `job_number = 1004` have `job_name = 'BERT_V_TA_BARRIER_ZUSGF'`.
    ```sql
    -- Pytest-like assertion (SQL equivalent)
    SELECT
        COUNT(1) AS total_entries,
        SUM(CASE WHEN job_name = 'BERT_V_TA_BARRIER_ZUSGF' THEN 1 ELSE 0 END) AS correct_job_name_count
    FROM `project.dataset.job_log`
    WHERE job_number = 1004;
    -- Expected result: total_entries=2, correct_job_name_count=2
    ```

---

## Test Case 5: Date Formatting and Logging (`v_sysdate`)

*   **Purpose:** Verify that the `v_sysdate_ddmmyyyy` variable is correctly formatted as `DDMMYYYY` and stored in the `stichtag` column of the `job_log` table.
*   **Setup:**
    1.  Ensure the `project.dataset.job_log` table is empty or truncate it.
    2.  Replace the actual `sp_k_ausd_v_ta_barrier_zusgf` with `sp_k_ausd_v_ta_barrier_zusgf_mock_success`.
*   **Action:** Execute the wrapper procedure.
    ```sql
    DECLARE v_test_job_kennung STRING DEFAULT 'DATE_TEST_JOB';
    DECLARE v_test_entry_number INT64 DEFAULT 1005;
    CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
        p_job_kennung => v_test_job_kennung,
        p_entry_number => v_test_entry_number,
        p_debug_mode => FALSE,
        p_test_mode => FALSE
    );
    ```
*   **Pass/Fail Criterion:**
    *   Query the `project.dataset.job_log` table.
    *   **PASS** if:
        1.  All entries for `job_number = 1005` have `stichtag` equal to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
        2.  All entries have `stichtag_format = 'DDMMYYYY'`.
    ```sql
    -- Pytest-like assertion (SQL equivalent)
    SELECT
        COUNT(1) AS total_entries,
        SUM(CASE WHEN stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE()) THEN 1 ELSE 0 END) AS correct_stichtag_count,
        SUM(CASE WHEN stichtag_format = 'DDMMYYYY' THEN 1 ELSE 0 END) AS correct_stichtag_format_count
    FROM `project.dataset.job_log`
    WHERE job_number = 1005 AND job_name = 'DATE_TEST_JOB';
    -- Expected result: total_entries=2, correct_stichtag_count=2, correct_stichtag_format_count=2
    ```

---

## Test Case 6: Debug Mode Output

*   **Purpose:** Verify that the `p_debug_mode` flag controls the `SELECT FORMAT` statements, mimicking the `print` statements of the original ksh script.
*   **Setup:**
    1.  Replace the actual `sp_k_ausd_v_ta_barrier_zusgf` with `sp_k_ausd_v_ta_barrier_zusgf_mock_success`.
*   **Action:**
    1.  Execute the wrapper procedure with `p_debug_mode => TRUE`.
    2.  Execute the wrapper procedure with `p_debug_mode => FALSE`.
    ```sql
    -- Action 1: Debug mode TRUE
    DECLARE v_test_job_kennung_debug STRING DEFAULT 'DEBUG_ON_JOB';
    DECLARE v_test_entry_number_debug INT64 DEFAULT 1006;
    CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
        p_job_kennung => v_test_job_kennung_debug,
        p_entry_number => v_test_entry_number_debug,
        p_debug_mode => TRUE,
        p_test_mode => FALSE
    );

    -- Action 2: Debug mode FALSE
    DECLARE v_test_job_kennung_nodebug STRING DEFAULT 'DEBUG_OFF_JOB';
    DECLARE v_test_entry_number_nodebug INT64 DEFAULT 1007;
    CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
        p_job_kennung => v_test_job_kennung_nodebug,
        p_entry_number => v_test_entry_number_nodebug,
        p_debug_mode => FALSE,
        p_test_mode => FALSE
    );
    ```
*   **Pass/Fail Criterion:**
    *   This test requires inspecting BigQuery job execution details or audit logs, as `SELECT FORMAT` output is not directly queryable from a table.
    *   **PASS** if:
        1.  When `p_debug_mode` was `TRUE` (for `job_number = 1006`), the BigQuery job execution details show output from the `SELECT FORMAT` statements (e.g., in the "Results" tab of the BQ UI for the procedure call, or in `INFORMATION_SCHEMA.JOBS` for query output).
        2.  When `p_debug_mode` was `FALSE` (for `job_number = 1007`), no such debug output is present in the job execution details.
    *   *(Note: Automated testing of `SELECT FORMAT` output in BigQuery is complex. This might be a manual verification step or require a custom BQ audit log parser.)*

---

## Test Case 7: `p_entry_number` Usage

*   **Purpose:** Verify that the `p_entry_number` input parameter is correctly used in logging and passed to the kernel script.
*   **Setup:**
    1.  Ensure the `project.dataset.job_log` table is empty or truncate it.
    2.  Replace the actual `sp_k_ausd_v_ta_barrier_zusgf` with `sp_k_ausd_v_ta_barrier_zusgf_mock_success`.
*   **Action:** Execute the wrapper procedure with a specific `p_entry_number`.
    ```sql
    DECLARE v_test_job_kennung STRING DEFAULT 'ENTRY_NUMBER_JOB';
    DECLARE v_test_entry_number INT64 DEFAULT 9999; -- A distinct entry number
    CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
        p_job_kennung => v_test_job_kennung,
        p_entry_number => v_test_entry_number,
        p_debug_mode => FALSE,
        p_test_mode => FALSE
    );
    ```
*   **Pass/Fail Criterion:**
    *   Query the `project.dataset.job_log` table.
    *   **PASS** if:
        1.  All entries for `job_name = 'ENTRY_NUMBER_JOB'` have `job_number = 9999`.
        2.  The `sp_k_ausd_v_ta_barrier_zusgf_mock_success` was called with `p_entry_number = 9999`. (Similar to Test Case 3, this relies on the `job_log` entry for verification within SQL).
    ```sql
    -- Pytest-like assertion (SQL equivalent)
    SELECT
        COUNT(1) AS total_entries,
        SUM(CASE WHEN job_number = 9999 THEN 1 ELSE 0 END) AS correct_entry_number_count
    FROM `project.dataset.job_log`
    WHERE job_name = 'ENTRY_NUMBER_JOB';
    -- Expected result: total_entries=2, correct_entry_number_count=2
    ```

---

## Cleanup: Restore Original Kernel Procedure

After all tests are complete, it's crucial to restore the original `sp_k_ausd_v_ta_barrier_zusgf` if it existed, or remove the mock if it was only for testing.

```sql
-- Example: If the original sp_k_ausd_v_ta_barrier_zusgf was a placeholder or empty,
-- you might just drop the mock and recreate the placeholder.
-- If it was a real procedure, you'd restore its original definition.
DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_v_ta_barrier_zusgf`;
DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_v_ta_barrier_zusgf_mock_success`;
DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_v_ta_barrier_zusgf_mock_fail`;

-- Recreate a placeholder or the actual sp_k_ausd_v_ta_barrier_zusgf if needed
-- CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_barrier_zusgf`(...) BEGIN ... END;
```