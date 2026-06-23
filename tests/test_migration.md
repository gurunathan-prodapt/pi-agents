As a senior data-migration QA engineer, I've analyzed the migration design for `k_ausd_v_ta_action_assoc.ksh` to `sp_ausd_v_ta_action_assoc.sql`. The migration focuses on translating orchestration, parameter handling, and job management logic to BigQuery. The core business logic from `d_ausd_v_ta_action_assoc.sql` is assumed to be migrated separately into `sp_d_ausd_v_ta_action_assoc`.

The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements (logging/job control tables), and data quality/schema assertions.

**Assumptions for Testing Environment:**
*   The DDLs for `my_project.my_dataset.job_table`, `my_project.my_dataset.error_log`, and `my_project.my_dataset.job_log` have been deployed.
*   A dummy `my_project.my_dataset.some_target_table_after_sql_execution` and `my_project.my_dataset.sp_d_ausd_v_ta_action_assoc` are available to simulate the core business logic and record counting. These will be defined in the setup for relevant tests.
*   All tests are executed within the `my_project.my_dataset` context.

---

## Pre-requisite Setup: Dummy Core Logic and Target Table

To effectively test `sp_ausd_v_ta_action_assoc`, we need to simulate the behavior of the core SQL logic (`d_ausd_v_ta_action_assoc.sql`) which is migrated to `sp_d_ausd_v_ta_action_assoc`.

```sql
-- Dummy target table for record counting
CREATE OR REPLACE TABLE `my_project.my_dataset.some_target_table_after_sql_execution` (
    id STRING, -- Using STRING for UUID-like behavior
    job_kennung STRING,
    eintrags_nr STRING,
    data STRING,
    processing_ts TIMESTAMP
);

-- Dummy core SQL procedure to simulate data processing and record generation
-- This procedure will insert a random number of rows (1-10) for successful runs
-- and can be configured to fail for specific inputs.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_d_ausd_v_ta_action_assoc`(
  p_JobKennung STRING,
  p_EintragsNr STRING
)
BEGIN
  -- Simulate some data processing and insertion
  INSERT INTO `my_project.my_dataset.some_target_table_after_sql_execution` (id, job_kennung, eintrags_nr, data, processing_ts)
  SELECT
    GENERATE_UUID() AS id,
    p_JobKennung,
    p_EintragsNr,
    'processed_data_' || CAST(UNIX_MICROS() AS STRING),
    CURRENT_TIMESTAMP()
  FROM UNNEST(GENERATE_ARRAY(1, CAST(ABS(FARM_FINGERPRINT(CONCAT(p_JobKennung, p_EintragsNr))) % 10 + 1 AS INT64))) AS _ -- Inserts 1 to 10 rows
  ;

  -- Simulate an error for specific inputs to test error handling
  IF p_JobKennung = 'JOB_FAIL_CORE_SQL' THEN
    RAISE USING MESSAGE = 'Simulated error in sp_d_ausd_v_ta_action_assoc for JobKennung: JOB_FAIL_CORE_SQL';
  END IF;
END;
```

---

## Test Case 1: Successful Execution with Valid Parameters

**Purpose:** Verify that the stored procedure executes successfully with valid parameters, logs job status correctly, updates the job control table, and reports the correct number of processed records. This covers output parity and basic transformation correctness.

**Setup:**
1.  Ensure `job_table`, `error_log`, `job_log`, `some_target_table_after_sql_execution`, and `sp_d_ausd_v_ta_action_assoc` are created and empty.
2.  Define test parameters: `p_JobKennung = 'TEST_JOB_1'`, `p_EintragsNr = 'ENTRY_001'`.

**Action:**
Execute the BigQuery Stored Procedure:
```sql
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_1', 'ENTRY_001');
```

**Pass/Fail Criterion:**
1.  The procedure completes without raising an unhandled exception.
2.  **Output Parity:** The final `SELECT` statements return:
    *   `message`: `' ---------- ENDE Datenverarbeitung ---------- '`
    *   `processed_records`: A positive integer (between 1 and 10, based on dummy `sp_d_ausd_v_ta_action_assoc`).
3.  **Job Logging (`job_log`):**
    *   Exactly two entries for `('TEST_JOB_1', 'ENTRY_001')`: one with `status = 'STARTED'` and one with `status = 'FINISHED'`.
    *   The `FINISHED` entry should have `record_count` matching the `processed_records` output and a non-NULL `end_ts`.
    *   The `start_ts` and `end_ts` should be chronologically correct.
4.  **Job Control (`job_table`):**
    *   Exactly one entry for `('TEST_JOB_1', 'ENTRY_001')` with `active_flag = FALSE` and a non-NULL `end_ts`.
5.  **Error Logging (`error_log`):** No entries for this execution.
6.  **Data Quality/Row Count (`some_target_table_after_sql_execution`):** The number of rows inserted into `some_target_table_after_sql_execution` for `job_kennung = 'TEST_JOB_1'` and `eintrags_nr = 'ENTRY_001'` should match the `record_count` in `job_log` and the `processed_records` output.

**Runnable Test Code (SQL Assertions):**
```sql
-- Setup: Clear tables
TRUNCATE TABLE `my_project.my_dataset.job_table`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.some_target_table_after_sql_execution`;

-- Action: Execute SP
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_1', 'ENTRY_001');

-- Assertions
SELECT
  (SELECT COUNT(*) FROM `my_project.my_dataset.error_log` WHERE procedure_name = 'sp_ausd_v_ta_action_assoc') = 0 AS no_error_log_entries,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_1' AND eintrags_nr = 'ENTRY_001' AND status = 'STARTED') = 1 AS job_log_started_entry,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_1' AND eintrags_nr = 'ENTRY_001' AND status = 'FINISHED') = 1 AS job_log_finished_entry,
  (SELECT active_flag FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_1' AND eintrags_nr = 'ENTRY_001') = FALSE AS job_table_inactive,
  (SELECT end_ts IS NOT NULL FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_1' AND eintrags_nr = 'ENTRY_001') AS job_table_end_ts_not_null,
  (SELECT T1.record_count = T2.actual_rows
   FROM `my_project.my_dataset.job_log` T1
   JOIN (SELECT COUNT(*) AS actual_rows FROM `my_project.my_dataset.some_target_table_after_sql_execution` WHERE job_kennung = 'TEST_JOB_1' AND eintrags_nr = 'ENTRY_001') T2
   ON TRUE
   WHERE T1.job_kennung = 'TEST_JOB_1' AND T1.eintrags_nr = 'ENTRY_001' AND T1.status = 'FINISHED'
  ) AS record_count_matches_target_table;
```

---

## Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the procedure correctly handles a missing `p_JobKennung` parameter, logs an error, and terminates gracefully without processing. This tests transformation correctness (parameter validation, error handling).

**Setup:**
1.  Ensure `job_table`, `error_log`, `job_log` are empty.

**Action:**
Execute the BigQuery Stored Procedure with `p_JobKennung` as `NULL` or empty string.
```sql
-- Test with NULL
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`(NULL, 'ENTRY_002');

-- Test with empty string
-- CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('', 'ENTRY_002');
```

**Pass/Fail Criterion:**
1.  The procedure raises an exception with a message similar to `'FEHLER: 0 E 1 Jobkennung'`.
2.  **Error Logging (`error_log`):** Exactly one entry with:
    *   `error_code = 1`
    *   `error_arg = 'Jobkennung'`
    *   `procedure_name = 'sp_ausd_v_ta_action_assoc'`
    *   `message = 'Bitte ueber Rahmenscript aufrufen'`
3.  **Job Logging (`job_log`):** No entries for this execution.
4.  **Job Control (`job_table`):** No entries for this execution.
5.  **Data Quality/Row Count (`some_target_table_after_sql_execution`):** No new rows inserted.

**Runnable Test Code (SQL Assertions):**
```sql
-- Setup: Clear tables
TRUNCATE TABLE `my_project.my_dataset.job_table`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.some_target_table_after_sql_execution`;

-- Action: Execute SP (this will raise an error, so it needs to be caught by the testing framework or run as a separate statement)
-- Example of how to check for the error log entry after a failed call:
BEGIN
  CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`(NULL, 'ENTRY_002');
EXCEPTION WHEN ERROR THEN
  SELECT 'Procedure failed as expected' AS status;
END;

-- Assertions
SELECT
  (SELECT COUNT(*) FROM `my_project.my_dataset.error_log` WHERE error_code = 1 AND error_arg = 'Jobkennung' AND message = 'Bitte ueber Rahmenscript aufrufen') = 1 AS error_log_entry_correct,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung IS NULL OR job_kennung = '') = 0 AS no_job_log_entries,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table` WHERE job_kennung IS NULL OR job_kennung = '') = 0 AS no_job_table_entries,
  (SELECT COUNT(*) FROM `my_project.my_dataset.some_target_table_after_sql_execution` WHERE job_kennung IS NULL OR job_kennung = '') = 0 AS no_target_table_rows;
```

---

## Test Case 3: Parameter Validation - Missing `p_EintragsNr`

**Purpose:** Verify that the procedure correctly handles a missing `p_EintragsNr` parameter, logs an error, and terminates gracefully. This tests transformation correctness (parameter validation, error handling).

**Setup:**
1.  Ensure `job_table`, `error_log`, `job_log` are empty.

**Action:**
Execute the BigQuery Stored Procedure with `p_EintragsNr` as `NULL` or empty string.
```sql
-- Test with NULL
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_3', NULL);

-- Test with empty string
-- CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_3', '');
```

**Pass/Fail Criterion:**
1.  The procedure raises an exception with a message similar to `'FEHLER: 0 E 1 EintragsNr'`.
2.  **Error Logging (`error_log`):** Exactly one entry with:
    *   `error_code = 1`
    *   `error_arg = 'EintragsNr'`
    *   `procedure_name = 'sp_ausd_v_ta_action_assoc'`
    *   `message = 'Bitte ueber Rahmenscript aufrufen'`
3.  **Job Logging (`job_log`):** No entries for this execution.
4.  **Job Control (`job_table`):** No entries for this execution.
5.  **Data Quality/Row Count (`some_target_table_after_sql_execution`):** No new rows inserted.

**Runnable Test Code (SQL Assertions):**
```sql
-- Setup: Clear tables
TRUNCATE TABLE `my_project.my_dataset.job_table`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.some_target_table_after_sql_execution`;

-- Action: Execute SP
BEGIN
  CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_3', NULL);
EXCEPTION WHEN ERROR THEN
  SELECT 'Procedure failed as expected' AS status;
END;

-- Assertions
SELECT
  (SELECT COUNT(*) FROM `my_project.my_dataset.error_log` WHERE error_code = 1 AND error_arg = 'EintragsNr' AND message = 'Bitte ueber Rahmenscript aufrufen') = 1 AS error_log_entry_correct,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_3') = 0 AS no_job_log_entries,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_3') = 0 AS no_job_table_entries,
  (SELECT COUNT(*) FROM `my_project.my_dataset.some_target_table_after_sql_execution` WHERE job_kennung = 'TEST_JOB_3') = 0 AS no_target_table_rows;
```

---

## Test Case 4: Job Management - Deactivating Old Active Jobs

**Purpose:** Verify that the procedure correctly deactivates previously active jobs for the same `JobKennung` but different `EintragsNr` before starting a new one. This tests external-system replacements (job_table logic).

**Setup:**
1.  Insert a 'mock' active job into `job_table` for `TEST_JOB_4` with a different `EintragsNr`.
2.  Ensure `error_log` and `job_log` are empty.
3.  Define test parameters: `p_JobKennung = 'TEST_JOB_4'`, `p_EintragsNr = 'ENTRY_002'`.

**Action:**
```sql
-- Setup: Insert a mock active job
INSERT INTO `my_project.my_dataset.job_table` (job_kennung, eintrags_nr, active_flag, start_ts, end_ts)
VALUES ('TEST_JOB_4', 'ENTRY_001_OLD', TRUE, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), NULL);

-- Action: Execute the SP
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_4', 'ENTRY_002');
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully.
2.  **Job Control (`job_table`):**
    *   The entry for `('TEST_JOB_4', 'ENTRY_001_OLD')` should have `active_flag = FALSE` and a non-NULL `end_ts` (updated by the current run).
    *   The entry for `('TEST_JOB_4', 'ENTRY_002')` should exist, have `active_flag = FALSE` (after completion), and a non-NULL `end_ts`.
3.  **Job Logging (`job_log`):** Two entries for `('TEST_JOB_4', 'ENTRY_002')` (`STARTED` and `FINISHED`).
4.  **Error Logging (`error_log`):** No entries.

**Runnable Test Code (SQL Assertions):**
```sql
-- Setup: Clear tables and insert mock data
TRUNCATE TABLE `my_project.my_dataset.job_table`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.some_target_table_after_sql_execution`;

INSERT INTO `my_project.my_dataset.job_table` (job_kennung, eintrags_nr, active_flag, start_ts, end_ts)
VALUES ('TEST_JOB_4', 'ENTRY_001_OLD', TRUE, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), NULL);

-- Action: Execute SP
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_4', 'ENTRY_002');

-- Assertions
SELECT
  (SELECT active_flag FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_4' AND eintrags_nr = 'ENTRY_001_OLD') = FALSE AS old_job_deactivated,
  (SELECT end_ts IS NOT NULL FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_4' AND eintrags_nr = 'ENTRY_001_OLD') AS old_job_end_ts_updated,
  (SELECT active_flag FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_4' AND eintrags_nr = 'ENTRY_002') = FALSE AS new_job_completed_inactive,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_4' AND eintrags_nr = 'ENTRY_002' AND status = 'STARTED') = 1 AS new_job_log_started,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_4' AND eintrags_nr = 'ENTRY_002' AND status = 'FINISHED') = 1 AS new_job_log_finished,
  (SELECT COUNT(*) FROM `my_project.my_dataset.error_log`) = 0 AS no_error_log_entries;
```

---

## Test Case 5: Job Management - Re-running an Existing Job

**Purpose:** Verify that if the same `JobKennung` and `EintragsNr` are provided, the `job_table` entry is updated (not duplicated) and marked active, then inactive upon completion. This tests transformation correctness (MERGE logic).

**Setup:**
1.  Insert a 'mock' inactive job into `job_table` for `TEST_JOB_5`, `ENTRY_001`.
2.  Ensure `error_log` and `job_log` are empty.
3.  Define test parameters: `p_JobKennung = 'TEST_JOB_5'`, `p_EintragsNr = 'ENTRY_001'`.

**Action:**
```sql
-- Setup: Insert a mock inactive job
INSERT INTO `my_project.my_dataset.job_table` (job_kennung, eintrags_nr, active_flag, start_ts, end_ts)
VALUES ('TEST_JOB_5', 'ENTRY_001', FALSE, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));

-- Action: Execute the SP
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_5', 'ENTRY_001');
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully.
2.  **Job Control (`job_table`):**
    *   Exactly one entry for `('TEST_JOB_5', 'ENTRY_001')`.
    *   This entry should have `active_flag = FALSE` and an `end_ts` that is *newer* than the initial setup `end_ts`.
3.  **Job Logging (`job_log`):** Two entries for `('TEST_JOB_5', 'ENTRY_001')` (`STARTED` and `FINISHED`).
4.  **Error Logging (`error_log`):** No entries.

**Runnable Test Code (SQL Assertions):**
```sql
-- Setup: Clear tables and insert mock data
TRUNCATE TABLE `my_project.my_dataset.job_table`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.some_target_table_after_sql_execution`;

DECLARE initial_end_ts TIMESTAMP DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
INSERT INTO `my_project.my_dataset.job_table` (job_kennung, eintrags_nr, active_flag, start_ts, end_ts)
VALUES ('TEST_JOB_5', 'ENTRY_001', FALSE, TIMESTAMP_SUB(initial_end_ts, INTERVAL 1 HOUR), initial_end_ts);

-- Action: Execute SP
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_5', 'ENTRY_001');

-- Assertions
SELECT
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_5' AND eintrags_nr = 'ENTRY_001') = 1 AS single_job_table_entry,
  (SELECT active_flag FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_5' AND eintrags_nr = 'ENTRY_001') = FALSE AS job_table_inactive_after_rerun,
  (SELECT end_ts > initial_end_ts FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_5' AND eintrags_nr = 'ENTRY_001') AS job_table_end_ts_updated,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_5' AND eintrags_nr = 'ENTRY_001' AND status = 'STARTED') = 1 AS job_log_started_entry,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_5' AND eintrags_nr = 'ENTRY_001' AND status = 'FINISHED') = 1 AS job_log_finished_entry,
  (SELECT COUNT(*) FROM `my_project.my_dataset.error_log`) = 0 AS no_error_log_entries;
```

---

## Test Case 6: Error Handling - Core SQL Logic Failure

**Purpose:** Verify that if the underlying `sp_d_ausd_v_ta_action_assoc` (migrated core SQL) fails, the main orchestration procedure catches the error, logs it correctly, marks the job as `FAILED`, and deactivates it in `job_table`. This tests transformation correctness (exception handling) and external-system replacements (logging).

**Setup:**
1.  Configure `sp_d_ausd_v_ta_action_assoc` to raise an error for a specific `JobKennung` (e.g., `'JOB_FAIL_CORE_SQL'`).
2.  Ensure `job_table`, `error_log`, `job_log` are empty.
3.  Define test parameters: `p_JobKennung = 'JOB_FAIL_CORE_SQL'`, `p_EintragsNr = 'ENTRY_003'`.

**Action:**
```sql
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('JOB_FAIL_CORE_SQL', 'ENTRY_003');
```

**Pass/Fail Criterion:**
1.  The procedure raises an exception, indicating a failure.
2.  **Error Logging (`error_log`):** Exactly one entry with:
    *   `error_code = -1` (or specific code if defined for core SQL errors).
    *   `procedure_name = 'sp_ausd_v_ta_action_assoc'`
    *   `message` containing the error from `sp_d_ausd_v_ta_action_assoc`.
3.  **Job Logging (`job_log`):**
    *   Exactly two entries for `('JOB_FAIL_CORE_SQL', 'ENTRY_003')`: one with `status = 'STARTED'` and one with `status = 'FAILED'`.
    *   The `FAILED` entry should have `record_count = 0` (or NULL) and a non-NULL `end_ts`.
4.  **Job Control (`job_table`):**
    *   Exactly one entry for `('JOB_FAIL_CORE_SQL', 'ENTRY_003')` with `active_flag = FALSE` and a non-NULL `end_ts`.
5.  **Data Quality/Row Count (`some_target_table_after_sql_execution`):** No new rows inserted for this `JobKennung` (assuming the core SQL fails before insertion or rolls back).

**Runnable Test Code (SQL Assertions):**
```sql
-- Setup: Clear tables
TRUNCATE TABLE `my_project.my_dataset.job_table`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.some_target_table_after_sql_execution`;

-- Action: Execute SP (this will raise an error)
BEGIN
  CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('JOB_FAIL_CORE_SQL', 'ENTRY_003');
EXCEPTION WHEN ERROR THEN
  SELECT 'Procedure failed as expected due to core SQL error' AS status;
END;

-- Assertions
SELECT
  (SELECT COUNT(*) FROM `my_project.my_dataset.error_log` WHERE procedure_name = 'sp_ausd_v_ta_action_assoc' AND message LIKE '%Simulated error in sp_d_ausd_v_ta_action_assoc%') = 1 AS error_log_entry_for_core_sql_failure,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'JOB_FAIL_CORE_SQL' AND eintrags_nr = 'ENTRY_003' AND status = 'STARTED') = 1 AS job_log_started_entry,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'JOB_FAIL_CORE_SQL' AND eintrags_nr = 'ENTRY_003' AND status = 'FAILED') = 1 AS job_log_failed_entry,
  (SELECT record_count FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'JOB_FAIL_CORE_SQL' AND eintrags_nr = 'ENTRY_003' AND status = 'FAILED') = 0 AS failed_job_record_count_zero,
  (SELECT active_flag FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'JOB_FAIL_CORE_SQL' AND eintrags_nr = 'ENTRY_003') = FALSE AS job_table_inactive_after_failure,
  (SELECT COUNT(*) FROM `my_project.my_dataset.some_target_table_after_sql_execution` WHERE job_kennung = 'JOB_FAIL_CORE_SQL') = 0 AS no_target_table_rows_on_failure;
```

---

## Test Case 7: Input Parameter Trimming and Case Sensitivity

**Purpose:** Verify that input parameters are handled correctly, including trimming whitespace, and that the comparison logic for `JobKennung` and `EintragsNr` is case-sensitive as implied by typical shell script behavior. This tests transformation correctness (type handling, NULL handling, edge cases).

**Setup:**
1.  Ensure `job_table`, `error_log`, `job_log` are empty.
2.  Define test parameters with leading/trailing spaces and mixed case: `p_JobKennung = '  TEST_JOB_TRIM  '`, `p_EintragsNr = '  entry_004  '`.

**Action:**
```sql
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('  TEST_JOB_TRIM  ', '  entry_004  ');
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully.
2.  **Job Logging (`job_log`):**
    *   Entries for `job_kennung = 'TEST_JOB_TRIM'` (trimmed) and `eintrags_nr = 'entry_004'` (trimmed).
    *   The values stored in `job_log` and `job_table` should be the *trimmed* versions of the input parameters.
3.  **Job Control (`job_table`):**
    *   Entry for `job_kennung = 'TEST_JOB_TRIM'` and `eintrags_nr = 'entry_004'`.
4.  **Data Quality/Row Count (`some_target_table_after_sql_execution`):** Rows should be inserted with the *trimmed* `job_kennung` and `eintrags_nr`.
5.  **Case Sensitivity:** If a subsequent call uses `job_kennung = 'test_job_trim'` (different case), it should be treated as a *new* job, not an update to the existing one, unless explicit `LOWER()` or `UPPER()` was added in the migration (which is not indicated in the design). Based on the provided pseudocode, it should be case-sensitive.

**Runnable Test Code (SQL Assertions):**
```sql
-- Setup: Clear tables
TRUNCATE TABLE `my_project.my_dataset.job_table`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.some_target_table_after_sql_execution`;

-- Action: Execute SP with padded parameters
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('  TEST_JOB_TRIM  ', '  entry_004  ');

-- Assertions
SELECT
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_TRIM' AND eintrags_nr = 'entry_004' AND status = 'FINISHED') = 1 AS job_log_trimmed_params,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_TRIM' AND eintrags_nr = 'entry_004') = 1 AS job_table_trimmed_params,
  (SELECT COUNT(*) FROM `my_project.my_dataset.some_target_table_after_sql_execution` WHERE job_kennung = 'TEST_JOB_TRIM' AND eintrags_nr = 'entry_004') > 0 AS target_table_trimmed_params,
  (SELECT COUNT(*) FROM `my_project.my_dataset.error_log`) = 0 AS no_error_log_entries;

-- Test case sensitivity (assuming it should be case-sensitive as per BigQuery default string comparison)
-- This call should create new entries, not update the previous one.
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('  test_job_trim  ', '  entry_004  ');

SELECT
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'TEST_JOB_TRIM' AND eintrags_nr = 'entry_004') = 1 AS original_trimmed_job_exists,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'test_job_trim' AND eintrags_nr = 'entry_004') = 1 AS new_case_sensitive_job_exists,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table`) = 2 AS total_job_table_entries;
```

---

## Test Case 8: Concurrent Execution (Simulated)

**Purpose:** Verify that the job management logic correctly handles multiple jobs for the same `JobKennung` but different `EintragsNr`s, ensuring only the current job is active and old ones are deactivated. This simulates a scenario where multiple instances of the script might run for the same logical job but different partitions/entries.

**Setup:**
1.  Ensure `job_table`, `error_log`, `job_log` are empty.

**Action:**
Execute the procedure sequentially for the same `JobKennung` but different `EintragsNr`s.
```sql
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('CONCURRENT_JOB', 'ENTRY_A');
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('CONCURRENT_JOB', 'ENTRY_B');
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('CONCURRENT_JOB', 'ENTRY_C');
```

**Pass/Fail Criterion:**
1.  All procedures complete successfully.
2.  **Job Control (`job_table`):**
    *   Three distinct entries for `('CONCURRENT_JOB', 'ENTRY_A')`, `('CONCURRENT_JOB', 'ENTRY_B')`, `('CONCURRENT_JOB', 'ENTRY_C')`.
    *   All three entries should have `active_flag = FALSE` and non-NULL `end_ts`.
    *   Crucially, when `ENTRY_B` started, `ENTRY_A` should have been deactivated. When `ENTRY_C` started, `ENTRY_A` and `ENTRY_B` should have been deactivated. The final state should reflect all jobs as inactive.
3.  **Job Logging (`job_log`):** Six entries in total (3 `STARTED`, 3 `FINISHED`), correctly paired for each `JobKennung`/`EintragsNr` combination.
4.  **Error Logging (`error_log`):** No entries.

**Runnable Test Code (SQL Assertions):**
```sql
-- Setup: Clear tables
TRUNCATE TABLE `my_project.my_dataset.job_table`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.some_target_table_after_sql_execution`;

-- Action: Execute SPs sequentially
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('CONCURRENT_JOB', 'ENTRY_A');
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('CONCURRENT_JOB', 'ENTRY_B');
CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('CONCURRENT_JOB', 'ENTRY_C');

-- Assertions
SELECT
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'CONCURRENT_JOB') = 3 AS three_distinct_jobs_in_table,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_table` WHERE job_kennung = 'CONCURRENT_JOB' AND active_flag = FALSE) = 3 AS all_jobs_inactive_at_end,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'CONCURRENT_JOB' AND status = 'STARTED') = 3 AS three_started_logs,
  (SELECT COUNT(*) FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'CONCURRENT_JOB' AND status = 'FINISHED') = 3 AS three_finished_logs,
  (SELECT COUNT(*) FROM `my_project.my_dataset.error_log`) = 0 AS no_error_log_entries;

-- Verify specific deactivation order (more complex, might require examining timestamps)
-- This check ensures that when ENTRY_B started, ENTRY_A was deactivated.
-- And when ENTRY_C started, ENTRY_A and ENTRY_B were deactivated.
SELECT
  (SELECT T1.end_ts < T2.start_ts
   FROM `my_project.my_dataset.job_table` T1
   JOIN `my_project.my_dataset.job_table` T2
   ON T1.job_kennung = T2.job_kennung
   WHERE T1.eintrags_nr = 'ENTRY_A' AND T2.eintrags_nr = 'ENTRY_B'
  ) AS entry_a_deactivated_before_entry_b_starts,
  (SELECT T1.end_ts < T2.start_ts
   FROM `my_project.my_dataset.job_table` T1
   JOIN `my_project.my_dataset.job_table` T2
   ON T1.job_kennung = T2.job_kennung
   WHERE T1.eintrags_nr = 'ENTRY_B' AND T2.eintrags_nr = 'ENTRY_C'
  ) AS entry_b_deactivated_before_entry_c_starts;
```