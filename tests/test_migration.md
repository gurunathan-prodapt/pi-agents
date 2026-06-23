As a senior data-migration QA engineer, I have developed a comprehensive suite of validation tests for the migration of `r_ausd_bp_ta_rn_vertrag.ksh` to BigQuery stored procedures. These tests cover output parity, transformation correctness, external system replacements, and data quality assertions, ensuring the migrated solution is behaviourally equivalent to the legacy system.

The tests are structured with clear purpose, setup, action, and pass/fail criteria. SQL assertions are provided for direct execution against BigQuery.

---

## Test Data Setup (Pre-requisite for all tests)

Before running any tests, ensure the following BigQuery tables and procedures are created and populated.

**1. Create `job_audit` table:**

```sql
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_audit` (
  job_entry_nr INT64 OPTIONS(description="Unique entry number for the job execution, mimicking DW_EintragsNr"),
  job_kennung STRING OPTIONS(description="Identifier for the job, e.g., 'RN_VERTRAG'"),
  status STRING OPTIONS(description="Current status of the job (e.g., 'STARTED', 'OK', 'ERROR')"),
  error_nr INT64 OPTIONS(description="Error number if the job failed (e.g., 192, 193)"),
  error_arg STRING OPTIONS(description="Additional argument for the error, if any"),
  log_ts TIMESTAMP OPTIONS(description="Timestamp of the log entry"),
  message STRING OPTIONS(description="Detailed log message"),
  stichtag STRING OPTIONS(description="Processing date (DDMMYYYY) for the job"),
  sysdate_value STRING OPTIONS(description="System date (DDMMYYYY) when the job was run"),
  restart_value INT64 OPTIONS(description="Restart value (Wiederanlaufwert) used for the job"),
  log_file_name STRING OPTIONS(description="Simulated log file name for audit trail")
)
OPTIONS(
  description="Audit table for tracking BigQuery job executions, replacing legacy file-based logging."
);
```

**2. Create `ta_vertrag_cache` and `fos_vertrag` tables:**

These tables are assumed to have identical schemas, as the `k_ausd_bp_ta_rn_vertrag` procedure uses `SELECT src.*`. For testing purposes, we define a minimal schema.

```sql
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.ta_vertrag_cache` (
  dwh_vertrag_id INT64 NOT NULL,
  gueltig_von DATE NOT NULL,
  gueltig_bis DATE NOT NULL,
  ladedatum DATE NOT NULL,
  col1 STRING,
  col2 INT64
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.fos_vertrag` (
  dwh_vertrag_id INT64 NOT NULL,
  gueltig_von DATE NOT NULL,
  gueltig_bis DATE NOT NULL,
  ladedatum DATE NOT NULL,
  col1 STRING,
  col2 INT64
);
```

**3. Populate `ta_vertrag_cache` with test data:**

This data covers various scenarios for date filtering and `dwh_vertrag_id` for restart logic.
The `Stichtag` for most tests will be `2023-06-15` (DDMMYYYY: `15062023`).

```sql
TRUNCATE TABLE `my_project.my_dataset.ta_vertrag_cache`;
INSERT INTO `my_project.my_dataset.ta_vertrag_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col1, col2) VALUES
(100, '2023-01-01', '2023-12-31', '2023-01-01', 'A', 1), -- Valid for 15062023
(101, '2023-01-01', '2023-06-15', '2023-01-01', 'B', 2), -- NOT valid (Stichtag not < gueltig_bis)
(102, '2023-06-15', '2024-01-01', '2023-06-14', 'C', 3), -- Valid for 15062023
(103, '2023-06-16', '2024-01-01', '2023-06-15', 'D', 4), -- NOT valid (gueltig_von > Stichtag)
(104, '2023-01-01', '2023-12-31', '2023-06-15', 'E', 5), -- NOT valid (ladedatum >= Stichtag)
(105, '2023-01-01', '2023-06-16', '2023-06-14', 'F', 6), -- Valid for 15062023
(106, '2023-01-01', '2023-06-16', '2023-06-14', 'G', 7), -- Valid for 15062023
(107, '2023-01-01', '2023-06-16', '2023-06-14', 'H', 8), -- Valid for 15062023
(108, '2023-01-01', '2023-06-16', '2023-06-14', 'I', 9), -- Valid for 15062023
(109, '2023-01-01', '2023-06-16', '2023-06-14', 'J', 10); -- Valid for 15062023

-- Expected valid records for Stichtag '2023-06-15' (DDMMYYYY: '15062023'):
-- dwh_vertrag_id: 100, 102, 105, 106, 107, 108, 109 (Total 7 records)
```

**4. Deploy the BigQuery Stored Procedures:**

Ensure `k_ausd_bp_ta_rn_vertrag.sql` and `ausd_bp_ta_rn_vertrag_wrapper.sql` are deployed to `my_project.my_dataset`.

---

## Migration Validation Tests

### Test Case 1: Full Run with Default Parameters (No Restart, Default Stichtag)

**Purpose:**
To verify that the wrapper script correctly defaults `p_stichtag` to the current system date and `p_wiederanlaufWert` to `0` when no parameters are provided. It also validates the core logic for a full, initial run.

**Setup:**
1.  Ensure `my_project.my_dataset.ta_vertrag_cache` is populated as per the pre-requisite.
2.  Clear `my_project.my_dataset.fos_vertrag`.
3.  Clear `my_project.my_dataset.job_audit`.

```sql
TRUNCATE TABLE `my_project.my_dataset.fos_vertrag`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;
```

**Action:**
Call the wrapper procedure without any parameters. This will use `CURRENT_DATE()` for `Stichtag` and `0` for `Wiederanlaufwert`. For predictable results, we'll simulate a specific `CURRENT_DATE()` for this test.

```sql
-- Simulate CURRENT_DATE() for testing. In a real environment, this would be dynamic.
-- For this test, let's assume CURRENT_DATE() is '2023-06-15'.
-- The actual call would be:
CALL `my_project.my_dataset.ausd_bp_ta_rn_vertrag_wrapper`(NULL, NULL);
```

**Pass/Fail Criterion:**
1.  **Output Parity (Row Count):** The `fos_vertrag` table should contain 7 records.
2.  **Output Parity (Data Content):** The `fos_vertrag` table should contain records with `dwh_vertrag_id` 100, 102, 105, 106, 107, 108, 109.
3.  **Transformation Correctness (Date Filtering):** All inserted records must satisfy `gueltig_von <= '2023-06-15'` AND `'2023-06-15' < gueltig_bis` AND `ladedatum < '2023-06-15'`.
4.  **Transformation Correctness (Restart Logic):** All inserted records must satisfy `dwh_vertrag_id > 0`.
5.  **Logging:** The `job_audit` table should contain two entries for the latest `job_entry_nr`: one with `status = 'STARTED'` and one with `status = 'OK'`, both with `stichtag` matching the simulated `CURRENT_DATE()` (e.g., '15062023') and `restart_value = 0`.

```sql
-- Assertions for Pass/Fail Criterion
-- 1 & 2. Row Count and Data Content
SELECT
  CASE
    WHEN COUNT(1) = 7 AND
         ARRAY_AGG(dwh_vertrag_id ORDER BY dwh_vertrag_id) = [100, 102, 105, 106, 107, 108, 109]
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;

-- 3. Transformation Correctness (Date Filtering) - implicitly covered by the expected IDs, but can be explicit
SELECT
  CASE
    WHEN COUNT(1) = 7 AND
         COUNTIF(gueltig_von <= PARSE_DATE('%Y-%m-%d', '2023-06-15') AND
                 PARSE_DATE('%Y-%m-%d', '2023-06-15') < gueltig_bis AND
                 ladedatum < PARSE_DATE('%Y-%m-%d', '2023-06-15')) = 7
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;

-- 4. Transformation Correctness (Restart Logic)
SELECT
  CASE
    WHEN COUNTIF(dwh_vertrag_id > 0) = 7
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;

-- 5. Logging
SELECT
  CASE
    WHEN (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED' AND stichtag = '15062023' AND restart_value = 0) = 1 AND
         (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'OK' AND stichtag = '15062023' AND restart_value = 0) = 1 AND
         (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED') = (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'OK')
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

### Test Case 2: Run with Explicit Stichtag and No Restart Value

**Purpose:**
To verify that the wrapper script correctly processes an explicitly provided `p_stichtag` and defaults `p_wiederanlaufWert` to `0`.

**Setup:**
1.  Ensure `my_project.my_dataset.ta_vertrag_cache` is populated as per the pre-requisite.
2.  Clear `my_project.my_dataset.fos_vertrag`.
3.  Clear `my_project.my_dataset.job_audit`.

```sql
TRUNCATE TABLE `my_project.my_dataset.fos_vertrag`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;
```

**Action:**
Call the wrapper procedure with a specific `Stichtag` (`15062023`) and `NULL` for `Wiederanlaufwert`.

```sql
CALL `my_project.my_dataset.ausd_bp_ta_rn_vertrag_wrapper`('15062023', NULL);
```

**Pass/Fail Criterion:**
1.  **Output Parity (Row Count):** The `fos_vertrag` table should contain 7 records.
2.  **Output Parity (Data Content):** The `fos_vertrag` table should contain records with `dwh_vertrag_id` 100, 102, 105, 106, 107, 108, 109.
3.  **Transformation Correctness (Date Filtering):** All inserted records must satisfy `gueltig_von <= '2023-06-15'` AND `'2023-06-15' < gueltig_bis` AND `ladedatum < '2023-06-15'`.
4.  **Logging:** The `job_audit` table should contain two entries for the latest `job_entry_nr`: one with `status = 'STARTED'` and one with `status = 'OK'`, both with `stichtag = '15062023'` and `restart_value = 0`.

```sql
-- Assertions for Pass/Fail Criterion
-- 1 & 2. Row Count and Data Content
SELECT
  CASE
    WHEN COUNT(1) = 7 AND
         ARRAY_AGG(dwh_vertrag_id ORDER BY dwh_vertrag_id) = [100, 102, 105, 106, 107, 108, 109]
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;

-- 3. Transformation Correctness (Date Filtering)
SELECT
  CASE
    WHEN COUNT(1) = 7 AND
         COUNTIF(gueltig_von <= PARSE_DATE('%Y-%m-%d', '2023-06-15') AND
                 PARSE_DATE('%Y-%m-%d', '2023-06-15') < gueltig_bis AND
                 ladedatum < PARSE_DATE('%Y-%m-%d', '2023-06-15')) = 7
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;

-- 4. Logging
SELECT
  CASE
    WHEN (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED' AND stichtag = '15062023' AND restart_value = 0) = 1 AND
         (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'OK' AND stichtag = '15062023' AND restart_value = 0) = 1 AND
         (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED') = (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'OK')
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

### Test Case 3: Run with Explicit Stichtag and Restart Value

**Purpose:**
To verify the restart logic: records in `fos_vertrag` with `dwh_vertrag_id >= p_wiederanlaufWert` are deleted, and then new records with `dwh_vertrag_id > p_wiederanlaufWert` are inserted from `ta_vertrag_cache` based on date conditions.

**Setup:**
1.  Ensure `my_project.my_dataset.ta_vertrag_cache` is populated as per the pre-requisite.
2.  Populate `my_project.my_dataset.fos_vertrag` with some initial data, including records that should be deleted by the restart logic.
    *   Initial `fos_vertrag` will have IDs 100, 105, 106, 107, 108, 109 (6 records).
    *   `p_wiederanlaufWert = 105`.
    *   Expected delete: IDs 105, 106, 107, 108, 109.
    *   Expected insert: IDs 106, 107, 108, 109 (from `ta_vertrag_cache`, where `dwh_vertrag_id > 105` and date conditions met).
    *   Final `fos_vertrag` should have IDs 100, 106, 107, 108, 109 (5 records).
3.  Clear `my_project.my_dataset.job_audit`.

```sql
TRUNCATE TABLE `my_project.my_dataset.fos_vertrag`;
INSERT INTO `my_project.my_dataset.fos_vertrag` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col1, col2) VALUES
(100, '2023-01-01', '2023-12-31', '2023-01-01', 'A_old', 10), -- Should remain
(105, '2023-01-01', '2023-06-16', '2023-06-14', 'F_old', 60), -- Should be deleted
(106, '2023-01-01', '2023-06-16', '2023-06-14', 'G_old', 70), -- Should be deleted
(107, '2023-01-01', '2023-06-16', '2023-06-14', 'H_old', 80), -- Should be deleted
(108, '2023-01-01', '2023-06-16', '2023-06-14', 'I_old', 90), -- Should be deleted
(109, '2023-01-01', '2023-06-16', '2023-06-14', 'J_old', 100); -- Should be deleted

TRUNCATE TABLE `my_project.my_dataset.job_audit`;
```

**Action:**
Call the wrapper procedure with `Stichtag = '15062023'` and `Wiederanlaufwert = 105`.

```sql
CALL `my_project.my_dataset.ausd_bp_ta_rn_vertrag_wrapper`('15062023', 105);
```

**Pass/Fail Criterion:**
1.  **Output Parity (Row Count):** The `fos_vertrag` table should contain 5 records.
2.  **Output Parity (Data Content):** The `fos_vertrag` table should contain records with `dwh_vertrag_id` 100, 106, 107, 108, 109. The `col1` and `col2` for IDs 106, 107, 108, 109 should be from `ta_vertrag_cache` (G, H, I, J and 7, 8, 9, 10 respectively), not the old `fos_vertrag` values.
3.  **Transformation Correctness (Delete Logic):** Records with `dwh_vertrag_id` 105 and above that were initially in `fos_vertrag` should be deleted.
4.  **Transformation Correctness (Insert Logic):** Only records from `ta_vertrag_cache` where `dwh_vertrag_id > 105` and date conditions are met should be inserted.
5.  **Logging:** The `job_audit` table should contain two entries for the latest `job_entry_nr`: one with `status = 'STARTED'` and one with `status = 'OK'`, both with `stichtag = '15062023'` and `restart_value = 105`.

```sql
-- Assertions for Pass/Fail Criterion
-- 1 & 2. Row Count and Data Content
SELECT
  CASE
    WHEN COUNT(1) = 5 AND
         ARRAY_AGG(dwh_vertrag_id ORDER BY dwh_vertrag_id) = [100, 106, 107, 108, 109] AND
         (SELECT col1 FROM `my_project.my_dataset.fos_vertrag` WHERE dwh_vertrag_id = 100) = 'A_old' AND
         (SELECT col1 FROM `my_project.my_dataset.fos_vertrag` WHERE dwh_vertrag_id = 106) = 'G' AND
         (SELECT col2 FROM `my_project.my_dataset.fos_vertrag` WHERE dwh_vertrag_id = 106) = 7
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;

-- 3 & 4. Transformation Correctness (Delete and Insert Logic)
-- This is implicitly covered by the final data content assertion above.
-- Explicit check for deleted records:
SELECT
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM `my_project.my_dataset.fos_vertrag` WHERE dwh_vertrag_id = 105)
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;

-- 5. Logging
SELECT
  CASE
    WHEN (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED' AND stichtag = '15062023' AND restart_value = 105) = 1 AND
         (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'OK' AND stichtag = '15062023' AND restart_value = 105) = 1 AND
         (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED') = (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'OK')
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

### Test Case 4: Error Handling - Invalid `p_wiederanlaufWert`

**Purpose:**
To verify that the wrapper script correctly handles invalid input for `p_wiederanlaufWert` (e.g., negative value) by raising an error and logging it. This mimics the `ErrNr=193` behavior.

**Setup:**
1.  Ensure `my_project.my_dataset.ta_vertrag_cache` is populated as per the pre-requisite.
2.  Clear `my_project.my_dataset.fos_vertrag`.
3.  Clear `my_project.my_dataset.job_audit`.

```sql
TRUNCATE TABLE `my_project.my_dataset.fos_vertrag`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;
```

**Action:**
Call the wrapper procedure with a negative `Wiederanlaufwert`. This call is expected to fail.

```sql
-- This call is expected to raise an error.
-- The exact way to "call" and "catch" an error in a test framework depends on the framework.
-- In a direct BigQuery console, it will simply fail.
CALL `my_project.my_dataset.ausd_bp_ta_rn_vertrag_wrapper`('15062023', -1);
```

**Pass/Fail Criterion:**
1.  **Error Handling:** The call to the stored procedure should terminate with an error (e.g., `SQLSTATE '45000'`).
2.  **Logging:** The `job_audit` table should contain two entries for the latest `job_entry_nr`: one with `status = 'STARTED'` and one with `status = 'ERROR'`. The `ERROR` entry should have `error_nr = 193` and `error_arg` containing the error message.
3.  **Data Quality:** The `fos_vertrag` table should remain empty (no data inserted or deleted).

```sql
-- Assertions for Pass/Fail Criterion
-- 1. Error Handling - This needs to be checked by the calling mechanism (e.g., pytest catching an exception).
--    If running directly in BQ, the query will fail.

-- 2. Logging
SELECT
  CASE
    WHEN (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED' AND stichtag = '15062023' AND restart_value = -1) = 1 AND
         (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'ERROR' AND stichtag = '15062023' AND restart_value = -1 AND error_nr = 193 AND error_arg LIKE '%Invalid restart value%') = 1 AND
         (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED') = (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'ERROR')
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;

-- 3. Data Quality
SELECT
  CASE
    WHEN COUNT(1) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;
```

---

### Test Case 5: Error Handling - Invalid `p_stichtag` Format

**Purpose:**
To verify that the kernel script correctly handles an invalid `p_stichtag` format (e.g., not DDMMYYYY) by raising an error and that the wrapper logs this failure. This mimics a general script failure (`ErrNr=192`).

**Setup:**
1.  Ensure `my_project.my_dataset.ta_vertrag_cache` is populated as per the pre-requisite.
2.  Clear `my_project.my_dataset.fos_vertrag`.
3.  Clear `my_project.my_dataset.job_audit`.

```sql
TRUNCATE TABLE `my_project.my_dataset.fos_vertrag`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;
```

**Action:**
Call the wrapper procedure with an invalid `Stichtag` format (e.g., `2023-06-15`). This call is expected to fail within the kernel procedure when `PARSE_DATE` is invoked.

```sql
-- This call is expected to raise an error.
CALL `my_project.my_dataset.ausd_bp_ta_rn_vertrag_wrapper`('2023-06-15', 0);
```

**Pass/Fail Criterion:**
1.  **Error Handling:** The call to the stored procedure should terminate with an error (e.g., `Bad date: '2023-06-15'. Expected format 'DDMMYYYY'`).
2.  **Logging:** The `job_audit` table should contain two entries for the latest `job_entry_nr`: one with `status = 'STARTED'` and one with `status = 'ERROR'`. The `ERROR` entry should have `error_nr = 192` and `error_arg` containing the BigQuery error message related to `PARSE_DATE` failure.
3.  **Data Quality:** The `fos_vertrag` table should remain empty (no data inserted or deleted).

```sql
-- Assertions for Pass/Fail Criterion
-- 1. Error Handling - This needs to be checked by the calling mechanism.

-- 2. Logging
SELECT
  CASE
    WHEN (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED' AND stichtag = '2023-06-15' AND restart_value = 0) = 1 AND
         (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'ERROR' AND stichtag = '2023-06-15' AND restart_value = 0 AND error_nr = 192 AND message LIKE '%Bad date: ''2023-06-15''. Expected format ''%d%m%Y''%') = 1 AND
         (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED') = (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'ERROR')
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;

-- 3. Data Quality
SELECT
  CASE
    WHEN COUNT(1) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;
```

---

### Test Case 6: Empty Source Table (`ta_vertrag_cache`)

**Purpose:**
To verify that the job handles an empty source table gracefully, resulting in an empty target table and successful completion.

**Setup:**
1.  Clear `my_project.my_dataset.ta_vertrag_cache`.
2.  Clear `my_project.my_dataset.fos_vertrag`.
3.  Clear `my_project.my_dataset.job_audit`.

```sql
TRUNCATE TABLE `my_project.my_dataset.ta_vertrag_cache`;
TRUNCATE TABLE `my_project.my_dataset.fos_vertrag`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;
```

**Action:**
Call the wrapper procedure with a valid `Stichtag` and `Wiederanlaufwert = 0`.

```sql
CALL `my_project.my_dataset.ausd_bp_ta_rn_vertrag_wrapper`('15062023', 0);
```

**Pass/Fail Criterion:**
1.  **Output Parity (Row Count):** The `fos_vertrag` table should contain 0 records.
2.  **Logging:** The `job_audit` table should contain two entries for the latest `job_entry_nr`: one with `status = 'STARTED'` and one with `status = 'OK'`, both with `stichtag = '15062023'` and `restart_value = 0`.

```sql
-- Assertions for Pass/Fail Criterion
-- 1. Row Count
SELECT
  CASE
    WHEN COUNT(1) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.fos_vertrag`;

-- 2. Logging
SELECT
  CASE
    WHEN (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED' AND stichtag = '15062023' AND restart_value = 0) = 1 AND
         (SELECT COUNT(1) FROM `my_project.my_dataset.job_audit` WHERE status = 'OK' AND stichtag = '15062023' AND restart_value = 0) = 1 AND
         (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'STARTED') = (SELECT MAX(job_entry_nr) FROM `my_project.my_dataset.job_audit` WHERE status = 'OK')
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

### Test Case 7: Schema Assertion for Target Table

**Purpose:**
To ensure that the target table `fos_vertrag` maintains the expected schema, which is critical for `SELECT src.*` operations and downstream consumers.

**Setup:**
No specific setup beyond the initial table creation.

**Action:**
Query the information schema for `fos_vertrag`.

```sql
-- No direct action, this is a schema check.
```

**Pass/Fail Criterion:**
The `fos_vertrag` table schema should match the expected schema (e.g., `dwh_vertrag_id INT64`, `gueltig_von DATE`, `gueltig_bis DATE`, `ladedatum DATE`, `col1 STRING`, `col2 INT64`).

```sql
-- Assertion for Pass/Fail Criterion
SELECT
  CASE
    WHEN COUNT(1) = 6 -- Number of expected columns
         AND SUM(CASE WHEN column_name = 'dwh_vertrag_id' AND data_type = 'INT64' THEN 1 ELSE 0 END) = 1
         AND SUM(CASE WHEN column_name = 'gueltig_von' AND data_type = 'DATE' THEN 1 ELSE 0 END) = 1
         AND SUM(CASE WHEN column_name = 'gueltig_bis' AND data_type = 'DATE' THEN 1 ELSE 0 END) = 1
         AND SUM(CASE WHEN column_name = 'ladedatum' AND data_type = 'DATE' THEN 1 ELSE 0 END) = 1
         AND SUM(CASE WHEN column_name = 'col1' AND data_type = 'STRING' THEN 1 ELSE 0 END) = 1
         AND SUM(CASE WHEN column_name = 'col2' AND data_type = 'INT64' THEN 1 ELSE 0 END) = 1
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'fos_vertrag';
```