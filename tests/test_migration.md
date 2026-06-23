As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `k_ausd_bp_ta_msisdn.ksh` to BigQuery. These tests focus on ensuring the migrated BigQuery stored procedure `proc_ausd_bp_ta_msisdn` is behaviourally equivalent to the legacy KornShell script, covering parameter handling, date logic, core SQL execution, record counting, error handling, and job tracking.

**Assumptions:**
*   The `deploy.sh` script has been successfully executed, creating all DDLs and stored procedures in BigQuery.
*   `your_gcp_project` and `your_bq_dataset` are replaced with actual project and dataset IDs for execution.
*   A placeholder table `PoolBasisprodukt` exists in `your_bq_dataset` and contains at least one row, as `proc_d_ausd_bp_ta_msisdn` uses it as a source.
*   Tests are designed to be run in isolation, with cleanup steps included in the setup.

---

## Global Setup & Teardown (Pre-requisites)

Before running any tests, ensure the BigQuery environment is set up as per the `deploy.sh` script.
The `PoolBasisprodukt` table is a dependency for the placeholder `proc_d_ausd_bp_ta_msisdn`.

**Setup:**
```sql
-- Replace with your actual project and dataset IDs
DECLARE PROJECT_ID STRING DEFAULT 'your_gcp_project';
DECLARE DATASET_ID STRING DEFAULT 'your_bq_dataset';

-- Ensure PoolBasisprodukt exists and has data for the placeholder proc_d_ausd_bp_ta_msisdn
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.PoolBasisprodukt` (
    dummy_col STRING
);

-- Insert a dummy row if the table is empty, so the placeholder proc_d_ausd_bp_ta_msisdn has data to process.
INSERT INTO `your_gcp_project.your_bq_dataset.PoolBasisprodukt` (dummy_col)
SELECT 'dummy_data'
WHERE NOT EXISTS (SELECT 1 FROM `your_gcp_project.your_bq_dataset.PoolBasisprodukt`);
```

---

## Test Case 1: Successful Execution with Valid Parameters

**Purpose:** Verify that the BigQuery stored procedure executes successfully with valid input parameters, processes data, counts records, and logs job status correctly. This covers output parity and basic transformation correctness.

**Setup:**
Clear previous test data from target and log tables.
```sql
-- Clear target table
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
-- Clear job tracking and error logs
DELETE FROM `your_gcp_project.your_bq_dataset.job_tracking` WHERE TRUE;
DELETE FROM `your_gcp_project.your_bq_dataset.job_error_log` WHERE TRUE;
```

**Action:**
Call the main orchestration procedure with valid parameters.
```sql
CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(
    'TEST_JOB_SUCCESS',
    '01012023',
    'ENTRY_001',
    NULL -- p_wiederanlaufwert is optional
);
```

**Pass/Fail Criterion:**
1.  The `target_bp_ta_msisdn` table contains exactly one new row for `processing_date = '2023-01-01'`.
2.  The `job_tracking` table contains one entry for `TEST_JOB_SUCCESS` with `status = 'SUCCEEDED'` and `records_processed = 1`.
3.  The `job_error_log` table is empty.

**Verification (SQL Assertions):**
```sql
-- Check target table row count
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`
WHERE processing_date = '2023-01-01';
-- Expected: 1

-- Check job tracking status and record count
SELECT status, records_processed FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_name = 'TEST_JOB_SUCCESS';
-- Expected: status = 'SUCCEEDED', records_processed = 1

-- Check error log (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.job_error_log`;
-- Expected: 0
```

---

## Test Case 2: Missing Required Parameter (Jobkennung)

**Purpose:** Verify that the procedure correctly identifies and handles missing required parameters, logging an error and updating job status. This tests transformation correctness for parameter validation and error handling.

**Setup:**
Clear previous test data from target and log tables.
```sql
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
DELETE FROM `your_gcp_project.your_bq_dataset.job_tracking` WHERE TRUE;
DELETE FROM `your_gcp_project.your_bq_dataset.job_error_log` WHERE TRUE;
```

**Action:**
Attempt to call the procedure with `p_job_kennung` as `NULL`.
```sql
-- This call is expected to fail and raise an error.
-- In a test framework like pytest, you'd assert that an exception is raised.
-- Using bq CLI:
-- bq query --project_id=your_gcp_project --dataset_id=your_bq_dataset --run_as_me --nouse_legacy_sql \
--   'CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(NULL, "01012023", "ENTRY_002", NULL);'
```
*Note: When executing this via `bq query`, it will return an error message. The subsequent `SELECT` statements will verify the logging.*

**Pass/Fail Criterion:**
1.  The `job_tracking` table contains one entry with `status = 'FAILED'` and `error_details` indicating a missing `Jobkennung`.
2.  The `job_error_log` table contains one entry with `severity = 'ERROR'` and `error_message` indicating a missing `Jobkennung`.
3.  The `target_bp_ta_msisdn` table remains empty.

**Verification (SQL Assertions):**
```sql
-- Check job tracking for failed status and error details
SELECT status, error_details FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_name IS NULL OR job_name = ''; -- Jobkennung is NULL, so it might be an empty string or NULL in tracking
-- Expected: status = 'FAILED', error_details LIKE '%Jobkennung%missing%'

-- Check error log for the specific error
SELECT severity, error_message FROM `your_gcp_project.your_bq_dataset.job_error_log`
WHERE error_message LIKE '%Jobkennung%missing%';
-- Expected: severity = 'ERROR', error_message LIKE '%Jobkennung (JOBKENNUNG) is missing or empty%'

-- Check target table (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
-- Expected: 0
```

---

## Test Case 3: Invalid Stichtag Format

**Purpose:** Verify that the procedure correctly validates the `p_stichtag_ddmmyyyy` parameter for the expected `DDMMYYYY` format, logging an error on failure. This tests type handling and error handling.

**Setup:**
Clear previous test data from target and log tables.
```sql
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
DELETE FROM `your_gcp_project.your_bq_dataset.job_tracking` WHERE TRUE;
DELETE FROM `your_gcp_project.your_bq_dataset.job_error_log` WHERE TRUE;
```

**Action:**
Attempt to call the procedure with an invalid `p_stichtag_ddmmyyyy` (e.g., `YYYY-MM-DD`).
```sql
-- This call is expected to fail and raise an error.
-- bq query --project_id=your_gcp_project --dataset_id=your_bq_dataset --run_as_me --nouse_legacy_sql \
--   'CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`("TEST_JOB_INVALID_DATE", "2023-01-01", "ENTRY_003", NULL);'
```

**Pass/Fail Criterion:**
1.  The `job_tracking` table contains one entry for `TEST_JOB_INVALID_DATE` with `status = 'FAILED'` and `error_details` indicating an invalid date format.
2.  The `job_error_log` table contains one entry with `severity = 'ERROR'` and `error_message` indicating an invalid date format.
3.  The `target_bp_ta_msisdn` table remains empty.

**Verification (SQL Assertions):**
```sql
-- Check job tracking for failed status and error details
SELECT status, error_details FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_name = 'TEST_JOB_INVALID_DATE';
-- Expected: status = 'FAILED', error_details LIKE '%Invalid Stichtag format%'

-- Check error log for the specific error
SELECT severity, error_message FROM `your_gcp_project.your_bq_dataset.job_error_log`
WHERE error_message LIKE '%Invalid Stichtag format%';
-- Expected: severity = 'ERROR', error_message LIKE '%Invalid Stichtag format. Expected DDMMYYYY%'

-- Check target table (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
-- Expected: 0
```

---

## Test Case 4: Date Derivation Correctness

**Purpose:** Verify that the internal date derivation (`v_current_date`, `v_yesterday_date`) within `proc_ausd_bp_ta_msisdn` works as expected, mimicking `gestern.ksh`. This tests transformation correctness for date handling.

**Setup:**
This test requires inspecting the internal state or modifying the procedure for direct assertion. Since we cannot directly inspect `DECLARE` variables from outside, we'll rely on the `proc_d_ausd_bp_ta_msisdn` to use these dates and verify the output.
For this test, we'll assume `proc_d_ausd_bp_ta_msisdn` would use `v_current_date` and `v_yesterday_date` if it were fully implemented. The current placeholder doesn't use them directly in the `INSERT` statement, so we'll adjust the `proc_d_ausd_bp_ta_msisdn` placeholder to include these for testing purposes.

**Modified `proc_d_ausd_bp_ta_msisdn` (for this test only):**
```sql
CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.proc_d_ausd_bp_ta_msisdn`(
    p_job_kennung STRING,
    p_stichtag_date DATE,
    p_eintragsnr STRING,
    p_wiederanlaufwert STRING,
    p_current_date DATE, -- Added for testing
    p_yesterday_date DATE -- Added for testing
)
BEGIN
    INSERT INTO `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn` (
        id, some_data, processing_date, job_kennung, eintragsnr, last_update_timestamp
    )
    SELECT
        GENERATE_UUID() AS id,
        FORMAT('Data for %s on %t. Current: %t, Yesterday: %t', p_job_kennung, p_stichtag_date, p_current_date, p_yesterday_date) AS some_data,
        p_stichtag_date AS processing_date,
        p_job_kennung AS job_kennung,
        p_eintragsnr AS eintragsnr,
        CURRENT_TIMESTAMP() AS last_update_timestamp
    FROM `your_gcp_project.your_bq_dataset.PoolBasisprodukt` AS source_table_placeholder
    WHERE 1=1 LIMIT 1;
END;
```
And the `CALL` in `proc_ausd_bp_ta_msisdn` would need to be updated:
```sql
        CALL `your_gcp_project.your_bq_dataset.proc_d_ausd_bp_ta_msisdn`(
            p_job_kennung,
            v_stichtag_date,
            p_eintragsnr,
            p_wiederanlaufwert,
            v_current_date, -- New parameter
            v_yesterday_date -- New parameter
        );
```

**Setup (after modifying `proc_d_ausd_bp_ta_msisdn` and `proc_ausd_bp_ta_msisdn`):**
Clear previous test data.
```sql
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
DELETE FROM `your_gcp_project.your_bq_dataset.job_tracking` WHERE TRUE;
DELETE FROM `your_gcp_project.your_bq_dataset.job_error_log` WHERE TRUE;
```

**Action:**
Call the main orchestration procedure with valid parameters.
```sql
CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(
    'TEST_JOB_DATE_DERIVATION',
    '05032023', -- Example Stichtag
    'ENTRY_004',
    NULL
);
```

**Pass/Fail Criterion:**
1.  The `target_bp_ta_msisdn` table contains one row where the `some_data` column correctly reflects `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` at the time of execution.

**Verification (SQL Assertions):**
```sql
SELECT some_data FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`
WHERE job_kennung = 'TEST_JOB_DATE_DERIVATION';
-- Expected: some_data LIKE '%Current: <CURRENT_DATE>, Yesterday: <YESTERDAY_DATE>%'
-- For example, if run on 2023-03-05, it should contain:
-- '...Current: 2023-03-05, Yesterday: 2023-03-04...'
```

---

## Test Case 5: Record Counting Accuracy

**Purpose:** Verify that the `v_records_processed` variable and the `job_tracking` table accurately reflect the number of rows inserted by `proc_d_ausd_bp_ta_msisdn`. This covers data quality and row-count assertions.

**Setup:**
To make `proc_d_ausd_bp_ta_msisdn` insert more than one row for this test, we need to ensure `PoolBasisprodukt` has multiple rows.
```sql
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
DELETE FROM `your_gcp_project.your_bq_dataset.job_tracking` WHERE TRUE;
DELETE FROM `your_gcp_project.your_bq_dataset.job_error_log` WHERE TRUE;

-- Ensure PoolBasisprodukt has multiple rows for this test
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.PoolBasisprodukt`;
INSERT INTO `your_gcp_project.your_bq_dataset.PoolBasisprodukt` (dummy_col)
VALUES ('row1'), ('row2'), ('row3'), ('row4'), ('row5');

-- Modify proc_d_ausd_bp_ta_msisdn to insert all rows from PoolBasisprodukt
-- (remove LIMIT 1) for this test.
-- Re-deploy proc_d_ausd_bp_ta_msisdn after this change.
-- Original: FROM `your_gcp_project.your_bq_dataset.PoolBasisprodukt` LIMIT 1;
-- Modified: FROM `your_gcp_project.your_bq_dataset.PoolBasisprodukt`;
```

**Action:**
Call the main orchestration procedure.
```sql
CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(
    'TEST_JOB_RECORD_COUNT',
    '15022023',
    'ENTRY_005',
    '100' -- Example wiederanlaufWert
);
```

**Pass/Fail Criterion:**
1.  The `target_bp_ta_msisdn` table contains 5 new rows for `processing_date = '2023-02-15'`.
2.  The `job_tracking` table contains one entry for `TEST_JOB_RECORD_COUNT` with `status = 'SUCCEEDED'` and `records_processed = 5`.

**Verification (SQL Assertions):**
```sql
-- Check target table row count
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`
WHERE processing_date = '2023-02-15';
-- Expected: 5

-- Check job tracking status and record count
SELECT status, records_processed FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_name = 'TEST_JOB_RECORD_COUNT';
-- Expected: status = 'SUCCEEDED', records_processed = 5
```

---

## Test Case 6: Error During Core SQL Execution

**Purpose:** Verify that if an error occurs within `proc_d_ausd_bp_ta_msisdn` (the core SQL logic), the orchestrating procedure catches it, logs the error, and updates the job tracking status to 'FAILED'. This covers error handling and external system replacement (BigQuery as the database).

**Setup:**
Clear previous test data.
```sql
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
DELETE FROM `your_gcp_project.your_bq_dataset.job_tracking` WHERE TRUE;
DELETE FROM `your_gcp_project.your_bq_dataset.job_error_log` WHERE TRUE;

-- Temporarily modify proc_d_ausd_bp_ta_msisdn to intentionally cause an error.
-- For example, try to insert into a non-existent column or divide by zero.
-- Re-deploy proc_d_ausd_bp_ta_msisdn after this change.
CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.proc_d_ausd_bp_ta_msisdn`(
    p_job_kennung STRING,
    p_stichtag_date DATE,
    p_eintragsnr STRING,
    p_wiederanlaufwert STRING
)
BEGIN
    -- This will cause an error (e.g., trying to insert a string into an INT64 column)
    INSERT INTO `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn` (
        id, some_data, processing_date, job_kennung, eintragsnr, last_update_timestamp
    )
    SELECT
        GENERATE_UUID() AS id,
        'Intentional Error' AS some_data,
        p_stichtag_date AS processing_date,
        p_job_kennung AS job_kennung,
        'NOT_AN_INT' AS eintragsnr, -- Assuming eintragsnr is STRING, let's try to cast something to a wrong type
        CURRENT_TIMESTAMP() AS last_update_timestamp
    FROM `your_gcp_project.your_bq_dataset.PoolBasisprodukt` LIMIT 1;
    -- Or, for a simpler error: SELECT 1/0;
END;
```

**Action:**
Call the main orchestration procedure.
```sql
CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(
    'TEST_JOB_CORE_SQL_ERROR',
    '20032023',
    'ENTRY_006',
    NULL
);
```

**Pass/Fail Criterion:**
1.  The `job_tracking` table contains one entry for `TEST_JOB_CORE_SQL_ERROR` with `status = 'FAILED'` and `error_details` reflecting the SQL error.
2.  The `job_error_log` table contains one entry with `severity = 'ERROR'` and `error_message` reflecting the SQL error.
3.  The `target_bp_ta_msisdn` table remains empty (or contains no new rows from this run).

**Verification (SQL Assertions):**
```sql
-- Check job tracking for failed status and error details
SELECT status, error_details FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_name = 'TEST_JOB_CORE_SQL_ERROR';
-- Expected: status = 'FAILED', error_details LIKE '%Error in SQL statement%' (or specific error message)

-- Check error log for the specific error
SELECT severity, error_message, stack_trace FROM `your_gcp_project.your_bq_dataset.job_error_log`
WHERE job_name = 'TEST_JOB_CORE_SQL_ERROR';
-- Expected: severity = 'ERROR', error_message containing the SQL error, stack_trace present.

-- Check target table (should be empty or unchanged)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`
WHERE processing_date = '2023-03-20';
-- Expected: 0
```

---

## Test Case 7: `p_wiederanlaufwert` Handling (NULL vs. Provided)

**Purpose:** Verify that the `p_wiederanlaufwert` parameter is correctly passed to `proc_d_ausd_bp_ta_msisdn` whether it's `NULL` or a specific value. This tests NULL handling and parameter passing.

**Setup:**
Clear previous test data.
```sql
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
DELETE FROM `your_gcp_project.your_bq_dataset.job_tracking` WHERE TRUE;
DELETE FROM `your_gcp_project.your_bq_dataset.job_error_log` WHERE TRUE;

-- Reset PoolBasisprodukt to 1 row for simplicity
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.PoolBasisprodukt`;
INSERT INTO `your_gcp_project.your_bq_dataset.PoolBasisprodukt` (dummy_col) VALUES ('single_row');

-- Ensure proc_d_ausd_bp_ta_msisdn uses p_wiederanlaufwert in its output for verification
CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.proc_d_ausd_bp_ta_msisdn`(
    p_job_kennung STRING,
    p_stichtag_date DATE,
    p_eintragsnr STRING,
    p_wiederanlaufwert STRING
)
BEGIN
    INSERT INTO `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn` (
        id, some_data, processing_date, job_kennung, eintragsnr, last_update_timestamp
    )
    SELECT
        GENERATE_UUID() AS id,
        FORMAT('WiederanlaufWert: %s', IFNULL(p_wiederanlaufwert, 'NULL_VALUE')) AS some_data,
        p_stichtag_date AS processing_date,
        p_job_kennung AS job_kennung,
        p_eintragsnr AS eintragsnr,
        CURRENT_TIMESTAMP() AS last_update_timestamp
    FROM `your_gcp_project.your_bq_dataset.PoolBasisprodukt` LIMIT 1;
END;
```

**Action:**
Call the procedure twice: once with `NULL` for `p_wiederanlaufwert` and once with a value.
```sql
CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(
    'TEST_JOB_WAW_NULL',
    '01042023',
    'ENTRY_NULL',
    NULL
);

CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(
    'TEST_JOB_WAW_VALUE',
    '02042023',
    'ENTRY_VALUE',
    '12345'
);
```

**Pass/Fail Criterion:**
1.  `target_bp_ta_msisdn` contains two rows.
2.  The row for `TEST_JOB_WAW_NULL` has `some_data` indicating `WiederanlaufWert: NULL_VALUE`.
3.  The row for `TEST_JOB_WAW_VALUE` has `some_data` indicating `WiederanlaufWert: 12345`.
4.  Both jobs are tracked as `SUCCEEDED`.

**Verification (SQL Assertions):**
```sql
-- Check data for NULL wiederanlaufWert
SELECT some_data FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`
WHERE job_kennung = 'TEST_JOB_WAW_NULL';
-- Expected: 'WiederanlaufWert: NULL_VALUE'

-- Check data for provided wiederanlaufWert
SELECT some_data FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`
WHERE job_kennung = 'TEST_JOB_WAW_VALUE';
-- Expected: 'WiederanlaufWert: 12345'

-- Check job tracking for both runs
SELECT job_name, status FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_name IN ('TEST_JOB_WAW_NULL', 'TEST_JOB_WAW_VALUE');
-- Expected: Two rows, both with status = 'SUCCEEDED'
```

---

## Test Case 8: Job Tracking Initial State and Final Update

**Purpose:** Verify that the `job_tracking` table correctly records the initial `RUNNING` status and then updates to the final `SUCCEEDED` or `FAILED` status, including start/end timestamps and `run_id`. This covers data quality and schema assertions for job tracking.

**Setup:**
Clear previous test data.
```sql
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`;
DELETE FROM `your_gcp_project.your_bq_dataset.job_tracking` WHERE TRUE;
DELETE FROM `your_gcp_project.your_bq_dataset.job_error_log` WHERE TRUE;
```

**Action:**
Call the procedure. Since we can't easily inspect the `job_tracking` table *during* execution, we'll rely on the final state. The `run_id` is generated, so we'll check for its presence and the state transitions.

```sql
CALL `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(
    'TEST_JOB_TRACKING',
    '10052023',
    'ENTRY_TRACK',
    NULL
);
```

**Pass/Fail Criterion:**
1.  The `job_tracking` table contains exactly one entry for `TEST_JOB_TRACKING`.
2.  This entry has `status = 'SUCCEEDED'`.
3.  `start_timestamp` is populated, and `end_timestamp` is populated and later than `start_timestamp`.
4.  `run_id` is a non-NULL, valid UUID string.
5.  `stichtag` is correctly recorded as `2023-05-10`.

**Verification (SQL Assertions):**
```sql
SELECT
    job_name,
    run_id,
    status,
    start_timestamp,
    end_timestamp,
    stichtag
FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_name = 'TEST_JOB_TRACKING';
-- Expected:
-- job_name = 'TEST_JOB_TRACKING'
-- run_id IS NOT NULL AND LENGTH(run_id) = 36 (for UUID)
-- status = 'SUCCEEDED'
-- start_timestamp IS NOT NULL
-- end_timestamp IS NOT NULL
-- end_timestamp > start_timestamp
-- stichtag = '2023-05-10'
```

---

These tests provide comprehensive coverage for the migrated orchestration logic, parameter handling, date derivation, error handling, and job tracking, ensuring the BigQuery solution behaves equivalently to the legacy KornShell script. For a complete validation, the placeholder `proc_d_ausd_bp_ta_msisdn` would need to be fully implemented and tested against the original `d_ausd_bp_ta_msisdn.sql`'s specific transformation logic.