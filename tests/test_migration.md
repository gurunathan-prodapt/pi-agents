As a senior data-migration QA engineer, I've analyzed the provided KornShell script (`k_ausd_v_ta_cntrct_crs.ksh`) and its BigQuery Stored Procedure migration (`sp_ausd_v_ta_cntrct_crs`). The original script is primarily an orchestration and job control mechanism, with the actual data processing delegated to an external SQL script (`d_ausd_v_ta_cntrct_crs.sql`).

The migration strategy involves translating the orchestration logic into BigQuery SQL within a stored procedure, replacing shell utilities with BigQuery constructs, and using BigQuery tables for job tracking and error logging. The core data processing logic from `d_ausd_v_ta_cntrct_crs.sql` is currently a placeholder in the generated BigQuery stored procedure.

The following test cases focus on validating the orchestration, parameter handling, job control, and logging aspects of the migrated BigQuery stored procedure, as these are fully defined in the provided code. Testing the actual data transformation (`d_ausd_v_ta_cntrct_crs.sql` content) would require its full translation and integration into the stored procedure.

**Assumptions for Testing:**
*   The BigQuery DDLs for `job_table`, `job_error_log`, and `job_audit_log` have been deployed to `project.dataset`.
*   Each test case assumes a clean state of these tables before execution. A helper procedure `project.dataset.clear_test_tables()` is provided for this purpose.
*   The `sp_ausd_v_ta_cntrct_crs` procedure is deployed to `project.dataset`.
*   The `v_records` variable in the generated BigQuery procedure is currently hardcoded to `0` due to the placeholder for the core SQL logic. Tests will reflect this behavior, with a note on future validation once the core logic is implemented.

---

### Helper Procedure: `clear_test_tables`

This procedure is used to reset the state of the job tracking and logging tables before each test case, ensuring test isolation.

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.clear_test_tables`()
BEGIN
  TRUNCATE TABLE `project.dataset.job_table`;
  TRUNCATE TABLE `project.dataset.job_error_log`;
  TRUNCATE TABLE `project.dataset.job_audit_log`;
  -- Drop any temporary tables used for specific tests if they exist
  EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS `project.dataset.test_target_table`';
END;
```

---

### Test Case 1: Parameter Validation - Missing `p_JobKennung`

**Purpose:**
To verify that the stored procedure correctly identifies and handles a missing `p_JobKennung` parameter, logs the error, and raises an `ERROR` as per the original script's behavior.

**Setup:**
Clear all job-related tables.
```sql
CALL `project.dataset.clear_test_tables`();
```

**Action:**
Execute the stored procedure with `p_JobKennung` as `NULL` and a valid `p_EintragsNr`.
```sql
-- This CALL is expected to fail and raise an ERROR
BEGIN
  CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`(NULL, 'E123');
EXCEPTION WHEN ERROR THEN
  -- Catch the error to allow subsequent assertions
  SELECT 'Procedure failed as expected' AS status;
END;
```

**Pass/Fail Criterion:**
1.  The procedure execution raises an `ERROR` containing "FEHLER: 0 E 193 Jobkennung".
2.  The `job_error_log` table contains exactly one entry with `err_nr = 193`, `err_arg = 'Jobkennung'`, and `message = 'Bitte ueber Rahmenscript aufrufen'`.

```sql
-- Assertion 1: Check error log entry
SELECT
  COUNT(1) AS error_count,
  MAX(err_nr) AS error_number,
  MAX(err_arg) AS error_argument,
  MAX(message) AS error_message
FROM `project.dataset.job_error_log`
WHERE procedure_name = 'sp_ausd_v_ta_cntrct_crs';

-- Expected Result:
-- error_count | error_number | error_argument | error_message
-- ------------|--------------|----------------|----------------------------------
-- 1           | 193          | Jobkennung     | Bitte ueber Rahmenscript aufrufen

-- Assertion 2: Verify no other tables were affected (optional, but good for robustness)
SELECT COUNT(1) FROM `project.dataset.job_table`; -- Expected: 0
SELECT COUNT(1) FROM `project.dataset.job_audit_log`; -- Expected: 0
```

---

### Test Case 2: Parameter Validation - Missing `p_EintragsNr`

**Purpose:**
To verify that the stored procedure correctly identifies and handles a missing `p_EintragsNr` parameter, logs the error, and raises an `ERROR`.

**Setup:**
Clear all job-related tables.
```sql
CALL `project.dataset.clear_test_tables`();
```

**Action:**
Execute the stored procedure with a valid `p_JobKennung` and `p_EintragsNr` as `NULL`.
```sql
-- This CALL is expected to fail and raise an ERROR
BEGIN
  CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('JABC', NULL);
EXCEPTION WHEN ERROR THEN
  SELECT 'Procedure failed as expected' AS status;
END;
```

**Pass/Fail Criterion:**
1.  The procedure execution raises an `ERROR` containing "FEHLER: 0 E 193 EintragsNr".
2.  The `job_error_log` table contains exactly one entry with `err_nr = 193`, `err_arg = 'EintragsNr'`, and `message = 'Bitte ueber Rahmenscript aufrufen'`.

```sql
-- Assertion 1: Check error log entry
SELECT
  COUNT(1) AS error_count,
  MAX(err_nr) AS error_number,
  MAX(err_arg) AS error_argument,
  MAX(message) AS error_message
FROM `project.dataset.job_error_log`
WHERE procedure_name = 'sp_ausd_v_ta_cntrct_crs';

-- Expected Result:
-- error_count | error_number | error_argument | error_message
-- ------------|--------------|----------------|----------------------------------
-- 1           | 193          | EintragsNr     | Bitte ueber Rahmenscript aufrufen
```

---

### Test Case 3: Parameter Validation - Empty String Parameters

**Purpose:**
To verify that the stored procedure correctly handles empty string parameters for `p_JobKennung` and `p_EintragsNr`, treating them as invalid. The `TRIM(param) = ''` logic should catch this.

**Setup:**
Clear all job-related tables.
```sql
CALL `project.dataset.clear_test_tables`();
```

**Action:**
Execute the stored procedure with empty strings for both parameters. The BigQuery code prioritizes `p_JobKennung` if both are empty.
```sql
-- This CALL is expected to fail and raise an ERROR
BEGIN
  CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('', '');
EXCEPTION WHEN ERROR THEN
  SELECT 'Procedure failed as expected' AS status;
END;
```

**Pass/Fail Criterion:**
1.  The procedure execution raises an `ERROR` containing "FEHLER: 0 E 193 Jobkennung".
2.  The `job_error_log` table contains exactly one entry with `err_nr = 193`, `err_arg = 'Jobkennung'`, and `message = 'Bitte ueber Rahmenscript aufrufen'`.

```sql
-- Assertion 1: Check error log entry
SELECT
  COUNT(1) AS error_count,
  MAX(err_nr) AS error_number,
  MAX(err_arg) AS error_argument,
  MAX(message) AS error_message
FROM `project.dataset.job_error_log`
WHERE procedure_name = 'sp_ausd_v_ta_cntrct_crs';

-- Expected Result:
-- error_count | error_number | error_argument | error_message
-- ------------|--------------|----------------|----------------------------------
-- 1           | 193          | Jobkennung     | Bitte ueber Rahmenscript aufrufen
```

---

### Test Case 4: Successful Execution - No Prior Active Jobs

**Purpose:**
To verify the end-to-end successful execution of the stored procedure when valid parameters are provided and no conflicting active jobs exist. This includes checking the initial 'ACTIVE' status, final 'DONE' status, and audit logging.

**Setup:**
Clear all job-related tables.
```sql
CALL `project.dataset.clear_test_tables`();
```

**Action:**
Execute the stored procedure with valid parameters.
```sql
CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('J001', 'E001');
```

**Pass/Fail Criterion:**
1.  The `job_table` contains exactly one entry for `('J001', 'E001')` with `tab_name = 'ta_cntrct_crs'`, `status = 'DONE'`, and `record_count = 0`.
2.  The `job_audit_log` contains exactly one entry for `('J001', 'E001')` with `tab_name = 'ta_cntrct_crs'`, `record_count = 0`, and `message = 'ENDE Datenverarbeitung'`.
3.  The `job_error_log` table is empty.
4.  `created_ts` and `updated_ts` in `job_table` are populated, and `updated_ts` is greater than or equal to `created_ts`. `event_ts` in `job_audit_log` is populated.

```sql
-- Assertion 1: Check job_table entry
SELECT
  job_kennung,
  eintrags_nr,
  tab_name,
  status,
  record_count,
  created_ts IS NOT NULL AS created_ts_populated,
  updated_ts IS NOT NULL AS updated_ts_populated,
  updated_ts >= created_ts AS updated_ts_is_later_or_equal
FROM `project.dataset.job_table`;

-- Expected Result:
-- job_kennung | eintrags_nr | tab_name       | status | record_count | created_ts_populated | updated_ts_populated | updated_ts_is_later_or_equal
-- ------------|-------------|----------------|--------|--------------|----------------------|----------------------|-----------------------------
-- J001        | E001        | ta_cntrct_crs  | DONE   | 0            | TRUE                 | TRUE                 | TRUE

-- Assertion 2: Check job_audit_log entry
SELECT
  job_kennung,
  eintrags_nr,
  tab_name,
  record_count,
  message,
  event_ts IS NOT NULL AS event_ts_populated
FROM `project.dataset.job_audit_log`;

-- Expected Result:
-- job_kennung | eintrags_nr | tab_name       | record_count | message                  | event_ts_populated
-- ------------|-------------|----------------|--------------|--------------------------|-------------------
-- J001        | E001        | ta_cntrct_crs  | 0            | ENDE Datenverarbeitung   | TRUE

-- Assertion 3: Check job_error_log is empty
SELECT COUNT(1) FROM `project.dataset.job_error_log`; -- Expected: 0
```

---

### Test Case 5: Job Control - Deactivate Older Active Jobs

**Purpose:**
To verify that the stored procedure correctly deactivates older active jobs for the same `tab_name` while leaving active jobs for other `tab_name` values untouched. It also ensures the current job is processed correctly.

**Setup:**
1.  Clear all job-related tables.
2.  Insert several job entries into `job_table` to simulate various scenarios:
    *   An 'ACTIVE' job for `ta_cntrct_crs` (should be deactivated).
    *   Another 'ACTIVE' job for `ta_cntrct_crs` (should be deactivated).
    *   An 'ACTIVE' job for a *different* `tab_name` (should remain active).
    *   A 'DONE' job for `ta_cntrct_crs` (should remain done).
```sql
CALL `project.dataset.clear_test_tables`();

INSERT INTO `project.dataset.job_table`
  (job_kennung, eintrags_nr, tab_name, status, created_ts, updated_ts)
VALUES
  ('OLD_J1', 'OLD_E1', 'ta_cntrct_crs', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)),
  ('OLD_J2', 'OLD_E2', 'ta_cntrct_crs', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)),
  ('OTHER_J', 'OTHER_E', 'other_contract_crs', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)),
  ('DONE_J', 'DONE_E', 'ta_cntrct_crs', 'DONE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR));
```

**Action:**
Execute the stored procedure with new valid parameters.
```sql
CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('NEW_J', 'NEW_E');
```

**Pass/Fail Criterion:**
1.  The two 'OLD_J1' and 'OLD_J2' entries in `job_table` now have `status = 'INACTIVE'`.
2.  The 'OTHER_J' entry in `job_table` remains `status = 'ACTIVE'`.
3.  The 'DONE_J' entry in `job_table` remains `status = 'DONE'`.
4.  A new entry for `('NEW_J', 'NEW_E')` exists with `status = 'DONE'`.
5.  `updated_ts` for the deactivated jobs (`OLD_J1`, `OLD_J2`) is updated to a recent timestamp.

```sql
-- Assertion 1: Check status of all jobs
SELECT
  job_kennung,
  eintrags_nr,
  tab_name,
  status,
  record_count,
  updated_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE) AS updated_recently
FROM `project.dataset.job_table`
ORDER BY job_kennung;

-- Expected Result:
-- job_kennung | eintrags_nr | tab_name           | status   | record_count | updated_recently
-- ------------|-------------|--------------------|----------|--------------|-----------------
-- DONE_J      | DONE_E      | ta_cntrct_crs      | DONE     | NULL         | FALSE
-- NEW_J       | NEW_E       | ta_cntrct_crs      | DONE     | 0            | TRUE
-- OLD_J1      | OLD_E1      | ta_cntrct_crs      | INACTIVE | NULL         | TRUE
-- OLD_J2      | OLD_E2      | ta_cntrct_crs      | INACTIVE | NULL         | TRUE
-- OTHER_J     | OTHER_E     | other_contract_crs | ACTIVE   | NULL         | FALSE

-- Note: record_count for pre-existing jobs might be NULL if not set during insertion.
-- The key is the status and updated_ts for OLD_J1 and OLD_J2.
```

---

### Test Case 6: Record Count Persistence (Current Placeholder Behavior)

**Purpose:**
To verify that the `v_records` variable, which currently defaults to `0` in the placeholder core logic, is correctly persisted in the `job_table` and `job_audit_log` upon successful completion. This test highlights the current limitation due to the placeholder.

**Setup:**
Clear all job-related tables.
```sql
CALL `project.dataset.clear_test_tables`();
```

**Action:**
Execute the stored procedure with valid parameters.
```sql
CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('J003', 'E003');
```

**Pass/Fail Criterion:**
1.  The `job_table` entry for `('J003', 'E003')` has `record_count = 0`.
2.  The `job_audit_log` entry for `('J003', 'E003')` has `record_count = 0`.

```sql
-- Assertion 1: Check record_count in job_table
SELECT record_count FROM `project.dataset.job_table` WHERE job_kennung = 'J003' AND eintrags_nr = 'E003';
-- Expected Result: 0

-- Assertion 2: Check record_count in job_audit_log
SELECT record_count FROM `project.dataset.job_audit_log` WHERE job_kennung = 'J003' AND eintrags_nr = 'E003';
-- Expected Result: 0
```

**Note on Transformation Correctness for `record_count`:**
This test validates the *current* behavior of the generated code. For full transformation correctness, once the `d_ausd_v_ta_cntrct_crs.sql` logic is translated into BigQuery DML, the `SET v_records = 0;` line must be replaced with `SET v_records = @@row_count;` (or similar logic to capture affected rows). At that point, this test case would need to be updated to:
1.  Include a setup for the source/target tables involved in the DML.
2.  Perform the DML within the procedure (or call a sub-procedure that does).
3.  Assert that `record_count` reflects the actual number of rows affected by the DML.

---

### Test Case 7: Concurrent/Sequential Runs of the Same Job

**Purpose:**
To verify how the BigQuery procedure handles multiple executions with the same `job_kennung` and `eintrags_nr`. The original script stated "aktive Jobs werden ignoriert", implying a single active instance. The BigQuery migration creates a new entry and deactivates *older* active jobs, which is a different behavior. This test validates the BigQuery implementation's behavior.

**Setup:**
Clear all job-related tables.
```sql
CALL `project.dataset.clear_test_tables`();
```

**Action:**
Execute the stored procedure twice with the exact same parameters.
```sql
CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('J004', 'E004');
CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('J004', 'E004');
```

**Pass/Fail Criterion:**
1.  The `job_table` contains exactly two entries for `('J004', 'E004')`.
2.  One entry has `status = 'INACTIVE'` (the first run, deactivated by the second run).
3.  One entry has `status = 'DONE'` (the second run).
4.  The `job_audit_log` contains two completion entries for `('J004', 'E004')`.

```sql
-- Assertion 1: Check job_table entries for the repeated job
SELECT
  job_kennung,
  eintrags_nr,
  tab_name,
  status,
  COUNT(1) AS num_entries
FROM `project.dataset.job_table`
WHERE job_kennung = 'J004' AND eintrags_nr = 'E004'
GROUP BY 1, 2, 3, 4
ORDER BY status;

-- Expected Result:
-- job_kennung | eintrags_nr | tab_name       | status   | num_entries
-- ------------|-------------|----------------|----------|------------
-- J004        | E004        | ta_cntrct_crs  | DONE     | 1
-- J004        | E004        | ta_cntrct_crs  | INACTIVE | 1

-- Assertion 2: Check job_audit_log entries for the repeated job
SELECT
  job_kennung,
  eintrags_nr,
  COUNT(1) AS num_audit_entries
FROM `project.dataset.job_audit_log`
WHERE job_kennung = 'J004' AND eintrags_nr = 'E004'
GROUP BY 1, 2;

-- Expected Result:
-- job_kennung | eintrags_nr | num_audit_entries
-- ------------|-------------|------------------
-- J004        | E004        | 2
```

**Note on Behavioral Difference:**
This test highlights a key behavioral difference from the original "aktive Jobs werden ignoriert" statement. The BigQuery implementation allows multiple runs of the same logical job, tracking each instance and deactivating previous ones. This should be confirmed with business stakeholders to ensure it aligns with the desired concurrency model in BigQuery. If strict single-instance execution is required, the BigQuery procedure would need an explicit check and `ERROR` or `RETURN` if an active job with the same `job_kennung` and `eintrags_nr` already exists.

---

### Test Case 8: Data Quality - `tab_name` Consistency

**Purpose:**
To verify that the `v_TabName` variable, hardcoded to `'ta_cntrct_crs'`, is consistently used and stored correctly across all relevant job tracking and audit log entries.

**Setup:**
Clear all job-related tables.
```sql
CALL `project.dataset.clear_test_tables`();
```

**Action:**
Execute the stored procedure with valid parameters.
```sql
CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('J006', 'E006');
```

**Pass/Fail Criterion:**
1.  The `job_table` entry for `('J006', 'E006')` has `tab_name = 'ta_cntrct_crs'`.
2.  The `job_audit_log` entry for `('J006', 'E006')` has `tab_name = 'ta_cntrct_crs'`.

```sql
-- Assertion 1: Check tab_name in job_table
SELECT tab_name FROM `project.dataset.job_table` WHERE job_kennung = 'J006' AND eintrags_nr = 'E006';
-- Expected Result: 'ta_cntrct_crs'

-- Assertion 2: Check tab_name in job_audit_log
SELECT tab_name FROM `project.dataset.job_audit_log` WHERE job_kennung = 'J006' AND eintrags_nr = 'E006';
-- Expected Result: 'ta_cntrct_crs'
```