The migration of `r_ausd_bp_ta_bpr_bcp.ksh` to a BigQuery Stored Procedure `p_ausd_bp_ta_bpr_bcp` primarily involves re-implementing orchestration, parameter handling, and logging. The core data processing logic is delegated to a separate kernel procedure (`k_ausd_bp_ta_bpr_bcp` in legacy, `p_k_ausd_bp_ta_bpr_bcp` in BigQuery).

The tests below focus on validating the wrapper's behavior, including parameter parsing, defaulting, validation, error handling, and correct invocation of the downstream kernel procedure, as well as the integrity of the new BigQuery job log.

**Prerequisites for all tests:**

1.  The `project.dataset.job_log` table must be created as per the migration design:
    ```sql
    CREATE TABLE `project.dataset.job_log` (
        job_run_id INT64,
        job_name STRING,
        job_kennung STRING,
        log_timestamp TIMESTAMP,
        status STRING,
        error_nr INT64,
        error_arg STRING,
        stichtag STRING,
        wiederanlaufwert INT64,
        message STRING
    );
    ```
2.  A mock `project.dataset.p_k_ausd_bp_ta_bpr_bcp` procedure is required to simulate the kernel script's behavior and capture its inputs. This mock will log its invocation and parameters to the `job_log` table.

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.p_k_ausd_bp_ta_bpr_bcp`(
        in_stichtag STRING,
        in_wiederanlaufwert INT64
    )
    BEGIN
        -- This is a mock procedure for testing purposes.
        -- In a real scenario, this would contain the actual business logic.
        DECLARE v_mock_message STRING;
        SET v_mock_message = 'Mock kernel procedure p_k_ausd_bp_ta_bpr_bcp called with Stichtag: ' || in_stichtag || ' and Wiederanlaufwert: ' || CAST(in_wiederanlaufwert AS STRING);

        -- Log the invocation of the mock kernel procedure
        INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message, stichtag, wiederanlaufwert)
        VALUES (UNIX_MICROS(CURRENT_TIMESTAMP()), 'mock_k_ausd_bp_ta_bpr_bcp', 'MOCK_KERNEL', CURRENT_TIMESTAMP(), 'INFO', v_mock_message, in_stichtag, in_wiederanlaufwert);

        -- By default, simulate success.
        -- To simulate failure for specific tests, uncomment the following line:
        -- RAISE USING MESSAGE 'Simulated error from mock kernel procedure.';
    END;
    ```

---

## Test Case 1: Successful Execution with All Parameters Provided

**Purpose:**
To verify that the migrated BigQuery Stored Procedure `p_ausd_bp_ta_bpr_bcp` correctly parses and uses all provided input parameters (`in_stichtag`, `in_wiederanlaufWert`), logs the execution flow accurately, and successfully invokes the kernel procedure with the correct parameters. This covers output parity, transformation correctness (parameter handling), and external system replacement (kernel invocation).

**Setup:**
1.  Ensure the `project.dataset.job_log` table exists and is empty.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    ```
2.  Ensure the mock `p_k_ausd_bp_ta_bpr_bcp` procedure is set to succeed (no `RAISE` statement).

**Action:**
Call the main stored procedure with valid `Stichtag` and `Wiederanlaufwert`.

```sql
CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('01012023', '12345', 'TEST_SOURCE', 'FULL_RUN');
```

**Pass/Fail Criterion:**
1.  The `job_log` table contains exactly 5 entries for the job run.
2.  The final log entry for `p_ausd_bp_ta_bpr_bcp` has `status = 'SUCCESS'`.
3.  The `job_log` entry for the mock kernel procedure (`mock_k_ausd_bp_ta_bpr_bcp`) shows `stichtag = '01012023'` and `wiederanlaufwert = 12345`.
4.  All log entries correctly reflect the provided `stichtag` and `wiederanlaufwert`.

```sql
-- Assertions
SELECT
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp'),
        4,
        'Expected 4 log entries for the main job.'
    ) AS main_job_log_count,
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp'),
        1,
        'Expected 1 log entry for the mock kernel job.'
    ) AS kernel_job_log_count,
    ASSERT_EQ(
        (SELECT status FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp' ORDER BY log_timestamp DESC LIMIT 1),
        'SUCCESS',
        'Expected final status of main job to be SUCCESS.'
    ) AS final_status,
    ASSERT_EQ(
        (SELECT stichtag FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp' LIMIT 1),
        '01012023',
        'Expected Stichtag passed to kernel to be 01012023.'
    ) AS kernel_stichtag,
    ASSERT_EQ(
        (SELECT wiederanlaufwert FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp' LIMIT 1),
        12345,
        'Expected Wiederanlaufwert passed to kernel to be 12345.'
    ) AS kernel_wiederanlaufwert;
```

---

## Test Case 2: `Stichtag` Defaulting to Current Date

**Purpose:**
To verify that if `in_stichtag` is not provided (NULL or empty string), the procedure correctly defaults it to the current system date in `DDMMYYYY` format, logs this action, and passes the defaulted value to the kernel procedure. This covers transformation correctness (defaulting) and external system replacement.

**Setup:**
1.  Ensure the `project.dataset.job_log` table exists and is empty.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    ```
2.  Ensure the mock `p_k_ausd_bp_ta_bpr_bcp` procedure is set to succeed.

**Action:**
Call the main stored procedure without providing `in_stichtag`.

```sql
CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`(NULL, '54321', 'TEST_SOURCE', 'FULL_RUN');
-- Or: CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('', '54321', 'TEST_SOURCE', 'FULL_RUN');
```

**Pass/Fail Criterion:**
1.  The `job_log` table contains exactly 5 entries for the job run.
2.  The final log entry for `p_ausd_bp_ta_bpr_bcp` has `status = 'SUCCESS'`.
3.  A log entry exists with `message` indicating `Stichtag not provided, defaulting to current date: YYYYMMDD`.
4.  The `job_log` entry for the mock kernel procedure shows `stichtag` equal to today's date in `DDMMYYYY` format and `wiederanlaufwert = 54321`.

```sql
-- Assertions
DECLARE current_date_ddmmyyyy STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());

SELECT
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp'),
        4,
        'Expected 4 log entries for the main job.'
    ) AS main_job_log_count,
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp'),
        1,
        'Expected 1 log entry for the mock kernel job.'
    ) AS kernel_job_log_count,
    ASSERT_EQ(
        (SELECT status FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp' ORDER BY log_timestamp DESC LIMIT 1),
        'SUCCESS',
        'Expected final status of main job to be SUCCESS.'
    ) AS final_status,
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE message LIKE 'Stichtag not provided, defaulting to current date:%') > 0,
        'Expected log message for Stichtag defaulting.'
    ) AS stichtag_default_message,
    ASSERT_EQ(
        (SELECT stichtag FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp' LIMIT 1),
        current_date_ddmmyyyy,
        'Expected Stichtag passed to kernel to be today''s date.'
    ) AS kernel_stichtag,
    ASSERT_EQ(
        (SELECT wiederanlaufwert FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp' LIMIT 1),
        54321,
        'Expected Wiederanlaufwert passed to kernel to be 54321.'
    ) AS kernel_wiederanlaufwert;
```

---

## Test Case 3: `Wiederanlaufwert` Defaulting to `0`

**Purpose:**
To verify that if `in_wiederanlaufWert` is not provided (NULL or empty string), the procedure correctly defaults it to `0`, logs this action, and passes the defaulted value to the kernel procedure. This covers transformation correctness (defaulting) and external system replacement.

**Setup:**
1.  Ensure the `project.dataset.job_log` table exists and is empty.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    ```
2.  Ensure the mock `p_k_ausd_bp_ta_bpr_bcp` procedure is set to succeed.

**Action:**
Call the main stored procedure without providing `in_wiederanlaufWert`.

```sql
CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('15032023', NULL, 'TEST_SOURCE', 'FULL_RUN');
-- Or: CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('15032023', '', 'TEST_SOURCE', 'FULL_RUN');
```

**Pass/Fail Criterion:**
1.  The `job_log` table contains exactly 5 entries for the job run.
2.  The final log entry for `p_ausd_bp_ta_bpr_bcp` has `status = 'SUCCESS'`.
3.  A log entry exists with `message` indicating `Wiederanlaufwert not provided, defaulting to 0.`.
4.  The `job_log` entry for the mock kernel procedure shows `stichtag = '15032023'` and `wiederanlaufwert = 0`.

```sql
-- Assertions
SELECT
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp'),
        4,
        'Expected 4 log entries for the main job.'
    ) AS main_job_log_count,
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp'),
        1,
        'Expected 1 log entry for the mock kernel job.'
    ) AS kernel_job_log_count,
    ASSERT_EQ(
        (SELECT status FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp' ORDER BY log_timestamp DESC LIMIT 1),
        'SUCCESS',
        'Expected final status of main job to be SUCCESS.'
    ) AS final_status,
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE message LIKE 'Wiederanlaufwert not provided, defaulting to 0.') > 0,
        'Expected log message for Wiederanlaufwert defaulting.'
    ) AS wiederanlaufwert_default_message,
    ASSERT_EQ(
        (SELECT stichtag FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp' LIMIT 1),
        '15032023',
        'Expected Stichtag passed to kernel to be 15032023.'
    ) AS kernel_stichtag,
    ASSERT_EQ(
        (SELECT wiederanlaufwert FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp' LIMIT 1),
        0,
        'Expected Wiederanlaufwert passed to kernel to be 0.'
    ) AS kernel_wiederanlaufwert;
```

---

## Test Case 4: Invalid `Stichtag` Format

**Purpose:**
To verify that the procedure correctly handles and logs an error when `in_stichtag` is provided in an invalid format (not `DDMMYYYY`), and that the job fails gracefully. This covers transformation correctness (type handling/validation) and error handling.

**Setup:**
1.  Ensure the `project.dataset.job_log` table exists and is empty.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    ```
2.  Ensure the mock `p_k_ausd_bp_ta_bpr_bcp` procedure is set to succeed.

**Action:**
Call the main stored procedure with an invalid `Stichtag` format.

```sql
-- This call is expected to fail and raise an error.
-- In a test runner like pytest-bigquery, this would be asserted as an exception.
CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('2023-01-01', '100', 'TEST_SOURCE', 'FULL_RUN');
```

**Pass/Fail Criterion:**
1.  The procedure call fails and raises an error message containing "Invalid Stichtag format".
2.  The `job_log` table contains entries for the job run, with the final entry for `p_ausd_bp_ta_bpr_bcp` having `status = 'FAILED'`.
3.  The `job_log` entry for the failure contains `error_arg` or `message` reflecting the "Invalid Stichtag format" error.
4.  The mock kernel procedure (`mock_k_ausd_bp_ta_bpr_bcp`) is **not** called, meaning no log entry for it.

```sql
-- Assertions (run AFTER the failing CALL, potentially in a separate test step or error handler)
SELECT
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp' AND status = 'FAILED') > 0,
        'Expected a FAILED status entry for the main job.'
    ) AS failed_status_entry,
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE message LIKE '%Invalid Stichtag format%') > 0,
        'Expected error message about invalid Stichtag format.'
    ) AS error_message_content,
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp'),
        0,
        'Expected mock kernel procedure NOT to be called.'
    ) AS kernel_not_called;
```

---

## Test Case 5: Invalid `Wiederanlaufwert` Format

**Purpose:**
To verify that the procedure correctly handles and logs an error when `in_wiederanlaufWert` is provided in a non-numeric format, and that the job fails gracefully. This covers transformation correctness (type handling/validation) and error handling.

**Setup:**
1.  Ensure the `project.dataset.job_log` table exists and is empty.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    ```
2.  Ensure the mock `p_k_ausd_bp_ta_bpr_bcp` procedure is set to succeed.

**Action:**
Call the main stored procedure with a non-numeric `Wiederanlaufwert`.

```sql
-- This call is expected to fail and raise an error.
CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('01012023', 'ABC', 'TEST_SOURCE', 'FULL_RUN');
```

**Pass/Fail Criterion:**
1.  The procedure call fails and raises an error message related to casting 'ABC' to INT64.
2.  The `job_log` table contains entries for the job run, with the final entry for `p_ausd_bp_ta_bpr_bcp` having `status = 'FAILED'`.
3.  The `job_log` entry for the failure contains `error_arg` or `message` reflecting the casting error.
4.  The mock kernel procedure (`mock_k_ausd_bp_ta_bpr_bcp`) is **not** called.

```sql
-- Assertions (run AFTER the failing CALL)
SELECT
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp' AND status = 'FAILED') > 0,
        'Expected a FAILED status entry for the main job.'
    ) AS failed_status_entry,
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE message LIKE '%Cannot parse "ABC" as INT64%') > 0 OR
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE message LIKE '%Invalid cast from STRING to INT64%') > 0,
        'Expected error message about invalid Wiederanlaufwert format.'
    ) AS error_message_content,
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp'),
        0,
        'Expected mock kernel procedure NOT to be called.'
    ) AS kernel_not_called;
```

---

## Test Case 6: Kernel Script Failure Propagation

**Purpose:**
To verify that if the invoked kernel procedure (`p_k_ausd_bp_ta_bpr_bcp`) fails, the wrapper procedure `p_ausd_bp_ta_bpr_bcp` correctly catches the error, logs the failure, and propagates the error. This covers external system replacement (error handling from invoked SP).

**Setup:**
1.  Ensure the `project.dataset.job_log` table exists and is empty.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    ```
2.  Modify the mock `p_k_ausd_bp_ta_bpr_bcp` procedure to simulate a failure by uncommenting the `RAISE` statement:

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.p_k_ausd_bp_ta_bpr_bcp`(
        in_stichtag STRING,
        in_wiederanlaufwert INT64
    )
    BEGIN
        DECLARE v_mock_message STRING;
        SET v_mock_message = 'Mock kernel procedure p_k_ausd_bp_ta_bpr_bcp called with Stichtag: ' || in_stichtag || ' and Wiederanlaufwert: ' || CAST(in_wiederanlaufwert AS STRING);
        INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message, stichtag, wiederanlaufwert)
        VALUES (UNIX_MICROS(CURRENT_TIMESTAMP()), 'mock_k_ausd_bp_ta_bpr_bcp', 'MOCK_KERNEL', CURRENT_TIMESTAMP(), 'INFO', v_mock_message, in_stichtag, in_wiederanlaufwert);

        -- Simulate failure for this test case
        RAISE USING MESSAGE 'Simulated error from mock kernel procedure.';
    END;
    ```

**Action:**
Call the main stored procedure with valid parameters, expecting the kernel to fail.

```sql
-- This call is expected to fail and raise an error.
CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('01012023', '12345', 'TEST_SOURCE', 'FULL_RUN');
```

**Pass/Fail Criterion:**
1.  The procedure call fails and raises an error message containing "Simulated error from mock kernel procedure.".
2.  The `job_log` table contains entries for both the main job and the mock kernel job.
3.  The final log entry for `p_ausd_bp_ta_bpr_bcp` has `status = 'FAILED'`.
4.  The `job_log` entry for the failure contains `error_arg` or `message` reflecting the kernel's error.

```sql
-- Assertions (run AFTER the failing CALL)
SELECT
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp' AND status = 'FAILED') > 0,
        'Expected a FAILED status entry for the main job.'
    ) AS main_job_failed_status,
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'mock_k_ausd_bp_ta_bpr_bcp') > 0,
        'Expected mock kernel procedure to be called and log its invocation.'
    ) AS kernel_called,
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE message LIKE '%Simulated error from mock kernel procedure.%') > 0,
        'Expected error message to contain the kernel''s simulated error.'
    ) AS error_message_content;
```

---

## Test Case 7: Logging Content and Data Quality

**Purpose:**
To verify the comprehensive logging behavior, ensuring all expected log entries are present, correctly formatted, and contain accurate data for a successful run. This covers output parity (logging replacement) and data quality/schema assertions.

**Setup:**
1.  Ensure the `project.dataset.job_log` table exists and is empty.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    ```
2.  Ensure the mock `p_k_ausd_bp_ta_bpr_bcp` procedure is set to succeed.

**Action:**
Call the main stored procedure with all parameters provided.

```sql
CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('28022024', '999', 'TEST_SOURCE_DQ', 'FULL_RUN_DQ');
```

**Pass/Fail Criterion:**
1.  Exactly 5 log entries are created for the entire job run (4 for wrapper, 1 for kernel).
2.  Each log entry has a non-NULL `job_run_id`, `job_name`, `job_kennung`, `log_timestamp`, `status`, and `message`.
3.  The `stichtag` and `wiederanlaufwert` columns are correctly populated in relevant log entries.
4.  The `job_kennung` is correctly set to `DW.BERT.BP_TA_BPR_BCP` for the main job.
5.  The sequence of `status` and `message` entries reflects the expected job flow (RUNNING -> INFO -> INFO -> INFO -> SUCCESS).

```sql
-- Assertions
SELECT
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log`),
        5,
        'Expected exactly 5 log entries for the entire job run.'
    ) AS total_log_entries,
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_run_id IS NULL OR job_name IS NULL OR job_kennung IS NULL OR log_timestamp IS NULL OR status IS NULL OR message IS NULL) = 0,
        'Expected no NULL values in core log columns.'
    ) AS no_null_core_columns,
    ASSERT_EQ(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp' AND job_kennung = 'DW.BERT.BP_TA_BPR_BCP'),
        4, -- 4 entries for the main job
        'Expected correct job_kennung for main job entries.'
    ) AS main_job_kennung,
    ASSERT_TRUE(
        (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp' AND stichtag = '28022024' AND wiederanlaufwert = 999) = 2,
        'Expected stichtag and wiederanlaufwert to be populated in the final two main job log entries.'
    ) AS parameter_columns_populated,
    ASSERT_TRUE(
        (SELECT ARRAY_AGG(status ORDER BY log_timestamp) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp')[OFFSET(0)] = 'RUNNING' AND
        (SELECT ARRAY_AGG(status ORDER BY log_timestamp) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp')[OFFSET(1)] = 'INFO' AND
        (SELECT ARRAY_AGG(status ORDER BY log_timestamp) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp')[OFFSET(2)] = 'INFO' AND
        (SELECT ARRAY_AGG(status ORDER BY log_timestamp) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_bp_ta_bpr_bcp')[OFFSET(3)] = 'SUCCESS',
        'Expected correct sequence of statuses for the main job.'
    ) AS status_sequence;
```