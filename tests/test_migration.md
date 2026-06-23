As a senior data-migration QA engineer, I've analyzed the migration design document and the provided legacy and target code for `k_ausd_bp_ta_bpr_instance.ksh`. The migration involves translating a KornShell orchestration script and an underlying SQL script (`d_ausd_bp_ta_bpr_instance.sql`) to BigQuery stored procedures.

The tests below are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality.

**Pre-requisites for Running Tests:**

1.  **BigQuery Environment:** A BigQuery project and dataset must be configured. Replace `your_project_id` and `your_dataset_id` with actual values.
2.  **DDL Execution:** The DDL for `job_error_log` and `job_run_log` must be executed to create these tables.
3.  **Target Table DDL:** The DDL for the target table `sof_ta_bpr_instance` (or `PoolBasisprodukt` if that's the final name) must be created and executed. Its schema should match the output of the `d_ausd_bp_ta_bpr_instance` procedure.
4.  **Source Table DDL & Data:** The DDL for source tables `cds_ta_cntrct` and `pds_ta_bpri_com` must be created. Crucially, these tables need to be populated with **representative test data** that covers all join conditions, filter conditions, date ranges, NULL scenarios, and edge cases described in the `d_ausd_bp_ta_bpr_instance` procedure. This is paramount for transformation correctness and output parity.
5.  **Stored Procedures Deployment:** Both `r_ausd_bp_ta_bpr_instance` and `d_ausd_bp_ta_bpr_instance` BigQuery stored procedures must be deployed.

---

## Test Case 1: Successful Execution - Happy Path

**Purpose:**
To verify that the migrated `r_ausd_bp_ta_bpr_instance` procedure executes successfully with valid parameters, orchestrates the data processing, and correctly logs a successful run and record count to `job_run_log`. This covers output parity and external system replacement for `tmpFile` and `FOSJobErzeugeEintrag`.

**Setup:**
1.  Ensure `job_run_log` and `sof_ta_bpr_instance` tables are empty or truncated.
2.  Populate `cds_ta_cntrct` and `pds_ta_bpri_com` with a diverse set of valid test data that should result in a non-zero number of records being inserted into `sof_ta_bpr_instance`. Include data that satisfies and data that does not satisfy the `WHERE` clause conditions in `d_ausd_bp_ta_bpr_instance`.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` stored procedure with valid input parameters.

```sql
-- Test Case 1: Successful Execution
DECLARE test_job_kennung STRING DEFAULT 'TEST_JOB_001_HP';
DECLARE test_eintrags_nr STRING DEFAULT '001_HP';
DECLARE test_stichtag STRING DEFAULT '25122023'; -- DDMMYYYY
DECLARE test_wiederanlauf_wert STRING DEFAULT NULL;

-- Clear previous logs and target data for a clean test run
DELETE FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = test_job_kennung;
DELETE FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` WHERE TRUE; -- Truncate target table

CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
  p_JobKennung => test_job_kennung,
  p_EintragsNr => test_eintrags_nr,
  p_Stichtag => test_stichtag,
  p_wiederanlaufWert => test_wiederanlauf_wert
);
```

**Pass/Fail Criterion:**
1.  **Pass:** The `job_run_log` table contains exactly one entry for `test_job_kennung` with `status = 'SUCCESS'`.
2.  **Pass:** The `record_count` in the `job_run_log` entry matches the actual `COUNT(*)` of rows in `sof_ta_bpr_instance` for the given `business_date` (derived from `test_stichtag`).
3.  **Pass:** The `sof_ta_bpr_instance` table contains the expected number of rows, and the data content (all columns) is identical to what the legacy script would produce for the same input data. (This requires a comparison with legacy output or a detailed assertion on expected data).

```sql
-- Verification for Test Case 1
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = 'TEST_JOB_001_HP' AND status = 'SUCCESS') = 1
         AND (SELECT record_count FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = 'TEST_JOB_001_HP' AND status = 'SUCCESS' ORDER BY created_at DESC LIMIT 1) = (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance`)
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_1_HappyPath_LogAndCount;

-- Detailed data comparison (example, requires expected data)
-- This would typically be done in a Python test framework comparing actual vs. expected tables.
-- For a SQL assertion, you'd need a pre-calculated 'expected_sof_ta_bpr_instance' table.
/*
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` EXCEPT DISTINCT SELECT * FROM `your_project_id.your_dataset_id.expected_sof_ta_bpr_instance`) = 0
         AND (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.expected_sof_ta_bpr_instance` EXCEPT DISTINCT SELECT * FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance`) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_1_HappyPath_DataContent;
*/
```

---

## Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:**
To verify that the procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and exiting gracefully without processing data. This covers external system replacement for `h_alis_parameter.ksh` and `f_alis_msgerr.ksh`.

**Setup:**
1.  Ensure `job_error_log` and `sof_ta_bpr_instance` tables are empty or truncated.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` stored procedure with `p_JobKennung` set to `NULL`.

```sql
-- Test Case 2: Missing p_JobKennung
DECLARE test_job_kennung STRING DEFAULT 'TEST_JOB_002_MISSING_JK';
DECLARE test_eintrags_nr STRING DEFAULT '002_MISSING_JK';
DECLARE test_stichtag STRING DEFAULT '25122023';
DECLARE test_wiederanlauf_wert STRING DEFAULT NULL;

-- Clear previous logs and target data for a clean test run
DELETE FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance' AND error_arg = 'Jobkennung';
DELETE FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` WHERE TRUE;

BEGIN
  CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
    p_JobKennung => NULL, -- Missing parameter
    p_EintragsNr => test_eintrags_nr,
    p_Stichtag => test_stichtag,
    p_wiederanlaufWert => test_wiederanlauf_wert
  );
EXCEPTION WHEN ERROR THEN
  -- Expected to catch an error, but the procedure logs and returns, not raises.
  -- The `SELECT FORMAT` statement in the procedure acts as a return message.
  SELECT FORMAT('Caught expected error for missing JobKennung: %s', ERROR_MESSAGE()) AS ErrorDetails;
END;
```

**Pass/Fail Criterion:**
1.  **Pass:** The `job_error_log` table contains exactly one entry for `job_name = 'r_ausd_bp_ta_bpr_instance'`, `error_code = 1`, and `error_arg = 'Jobkennung'`.
2.  **Pass:** The `sof_ta_bpr_instance` table remains empty (no data processing occurred).
3.  **Pass:** No entry is created in `job_run_log`.

```sql
-- Verification for Test Case 2
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance' AND error_code = 1 AND error_arg = 'Jobkennung') = 1
         AND (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance`) = 0
         AND (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance') = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_2_MissingJobKennung;
```

---

## Test Case 3: Parameter Validation - Invalid `p_Stichtag` Format

**Purpose:**
To verify that the procedure correctly validates the `p_Stichtag` format (`DDMMYYYY`), logs an error (error code 193), and exits gracefully. This covers external system replacement for `h_alis_date.ksh`.

**Setup:**
1.  Ensure `job_error_log` and `sof_ta_bpr_instance` tables are empty or truncated.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` stored procedure with an invalid `p_Stichtag` format (e.g., `YYYY-MM-DD`).

```sql
-- Test Case 3: Invalid p_Stichtag Format
DECLARE test_job_kennung STRING DEFAULT 'TEST_JOB_003_INVALID_ST';
DECLARE test_eintrags_nr STRING DEFAULT '003_INVALID_ST';
DECLARE test_stichtag STRING DEFAULT '2023-12-25'; -- Invalid format
DECLARE test_wiederanlauf_wert STRING DEFAULT NULL;

-- Clear previous logs and target data for a clean test run
DELETE FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance' AND error_code = 193;
DELETE FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` WHERE TRUE;

BEGIN
  CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
    p_JobKennung => test_job_kennung,
    p_EintragsNr => test_eintrags_nr,
    p_Stichtag => test_stichtag,
    p_wiederanlaufWert => test_wiederanlauf_wert
  );
EXCEPTION WHEN ERROR THEN
  SELECT FORMAT('Caught expected error for invalid Stichtag: %s', ERROR_MESSAGE()) AS ErrorDetails;
END;
```

**Pass/Fail Criterion:**
1.  **Pass:** The `job_error_log` table contains exactly one entry for `job_name = 'r_ausd_bp_ta_bpr_instance'`, `error_code = 193`, and `error_arg` containing the invalid date string.
2.  **Pass:** The `sof_ta_bpr_instance` table remains empty.
3.  **Pass:** No entry is created in `job_run_log`.

```sql
-- Verification for Test Case 3
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance' AND error_code = 193 AND error_arg LIKE '%2023-12-25%') = 1
         AND (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance`) = 0
         AND (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance') = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_3_InvalidStichtag;
```

---

## Test Case 4: Transformation Correctness - Filtering Logic

**Purpose:**
To verify that the `WHERE` clause conditions in `d_ausd_bp_ta_bpr_instance` are correctly translated and applied, ensuring only relevant records are inserted. This covers transformation correctness for filters and date handling.

**Setup:**
1.  Ensure `job_run_log` and `sof_ta_bpr_instance` tables are empty or truncated.
2.  Populate `cds_ta_cntrct` and `pds_ta_bpri_com` with specific test data:
    *   Records that *should* be included (e.g., `cntrct_st` in `(5, 6)`, `is_production = 1`, `insert_at <= p_Stichtag`, `modified_at IS NULL` or `> p_Stichtag`, etc.).
    *   Records that *should be excluded* (e.g., `cntrct_st = 1`, `is_production = 0`, `insert_at > p_Stichtag`, `valid_to <= p_Stichtag`, `cntrct_ty = 1` and `cntrct_parent IS NULL`).
    *   Include `NULL` values for `modified_at` and `valid_to` to test `IS NULL` conditions.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with a `p_Stichtag` that interacts with the test data's date fields.

```sql
-- Test Case 4: Transformation Correctness - Filtering Logic
DECLARE test_job_kennung STRING DEFAULT 'TEST_JOB_004_FILTER';
DECLARE test_eintrags_nr STRING DEFAULT '004_FILTER';
DECLARE test_stichtag STRING DEFAULT '01012023'; -- DDMMYYYY
DECLARE test_wiederanlauf_wert STRING DEFAULT '0';

-- Clear previous logs and target data for a clean test run
DELETE FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = test_job_kennung;
DELETE FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` WHERE TRUE;

-- Populate source tables with specific test data (conceptual, not runnable SQL)
-- INSERT INTO `your_project_id.your_dataset_id.cds_ta_cntrct` (...) VALUES (...);
-- INSERT INTO `your_project_id.your_dataset_id.pds_ta_bpri_com` (...) VALUES (...);

CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
  p_JobKennung => test_job_kennung,
  p_EintragsNr => test_eintrags_nr,
  p_Stichtag => test_stichtag,
  p_wiederanlaufWert => test_wiederanlauf_wert
);
```

**Pass/Fail Criterion:**
1.  **Pass:** The `sof_ta_bpr_instance` table contains exactly the set of records that are expected based on the `WHERE` clause logic and the specific test data. This requires a pre-calculated expected result set.
2.  **Pass:** The `record_count` in `job_run_log` matches the actual count of records in `sof_ta_bpr_instance`.

```sql
-- Verification for Test Case 4
-- This requires a comparison with a pre-defined 'expected_filtered_data' table.
-- Example:
/*
CREATE TEMPORARY TABLE expected_filtered_data AS
SELECT ... FROM `your_project_id.your_dataset_id.cds_ta_cntrct` c
JOIN `your_project_id.your_dataset_id.pds_ta_bpri_com` bp ON c.cntrct_id = bp.cntrct_id
WHERE   c.cntrct_st IN (5, 6)
  AND   c.redundant_owner_id = 1
  AND   c.insert_at <= PARSE_DATE('%d%m%Y', '01012023')
  AND   (   c.modified_at IS NULL OR c.modified_at > PARSE_DATE('%d%m%Y', '01012023'))
  AND   c.valid_from <= PARSE_DATE('%d%m%Y', '01012023')
  AND   (   c.valid_to IS NULL OR c.valid_to > PARSE_DATE('%d%m%Y', '01012023'))
  AND   c.is_production = 1
  AND   (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  AND   bp.insert_at <= PARSE_DATE('%d%m%Y', '01012023')
  AND   (   bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%d%m%Y', '01012023'))
  AND   bp.valid_from <= PARSE_DATE('%d%m%Y', '01012023')
  AND   (   bp.valid_to IS NULL OR bp.valid_to > PARSE_DATE('%d%m%Y', '01012023'))
  AND   bp.is_production = 1;

SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` EXCEPT DISTINCT SELECT * FROM expected_filtered_data) = 0
         AND (SELECT COUNT(*) FROM expected_filtered_data EXCEPT DISTINCT SELECT * FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance`) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_4_FilteringLogic;
*/
```

---

## Test Case 5: Transformation Correctness - `CONCAT` and Joins

**Purpose:**
To verify that the `CONCAT` function for `ICCID` and the `JOIN` conditions between `cds_ta_cntrct` and `pds_ta_bpri_com` are correctly implemented. This covers transformation correctness for joins and column transformations.

**Setup:**
1.  Ensure `job_run_log` and `sof_ta_bpr_instance` tables are empty or truncated.
2.  Populate `cds_ta_cntrct` and `pds_ta_bpri_com` with test data including:
    *   Records that successfully join.
    *   Records in `cds_ta_cntrct` without a match in `pds_ta_bpri_com` (should be excluded due to `INNER JOIN`).
    *   Records in `pds_ta_bpri_com` without a match in `cds_ta_cntrct` (should be excluded).
    *   Various values for `iccid_mi`, `iccid_ii`, `iccid_iai`, `iccid_nr`, `iccid_cd` to test `CONCAT`.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with valid parameters.

```sql
-- Test Case 5: Transformation Correctness - CONCAT and Joins
DECLARE test_job_kennung STRING DEFAULT 'TEST_JOB_005_CONCAT_JOIN';
DECLARE test_eintrags_nr STRING DEFAULT '005_CONCAT_JOIN';
DECLARE test_stichtag STRING DEFAULT '15062023';
DECLARE test_wiederanlauf_wert STRING DEFAULT '0';

-- Clear previous logs and target data for a clean test run
DELETE FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = test_job_kennung;
DELETE FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` WHERE TRUE;

-- Populate source tables with specific test data (conceptual)
-- INSERT INTO `your_project_id.your_dataset_id.cds_ta_cntrct` (...) VALUES (...);
-- INSERT INTO `your_project_id.your_dataset_id.pds_ta_bpri_com` (...) VALUES (...);

CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
  p_JobKennung => test_job_kennung,
  p_EintragsNr => test_eintrags_nr,
  p_Stichtag => test_stichtag,
  p_wiederanlaufWert => test_wiederanlauf_wert
);
```

**Pass/Fail Criterion:**
1.  **Pass:** For all records in `sof_ta_bpr_instance`, the `ICCID` column is correctly formed by concatenating `iccid_mi`, `iccid_ii`, `iccid_iai`, `iccid_nr`, `iccid_cd` with hyphens.
2.  **Pass:** The number of rows and the specific `CNTRCT_ID` values in `sof_ta_bpr_instance` accurately reflect the `INNER JOIN` logic on `c.cntrct_id = bp.cntrct_id`.

```sql
-- Verification for Test Case 5
-- Check CONCAT logic
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` s
          JOIN `your_project_id.your_dataset_id.pds_ta_bpri_com` bp
            ON s.BPR_ID = bp.BPR_ID AND s.BPR_INSTANCE_ID = bp.BPRI_COM_ID
          WHERE s.ICCID <> CONCAT(bp.iccid_mi,'-',bp.iccid_ii,'-',bp.iccid_iai,'-',bp.iccid_nr,'-',bp.iccid_cd)) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_5_ConcatLogic;

-- Check Join logic (conceptual, requires comparing with expected join result)
-- This is often best done by comparing the full output table with an expected table.
```

---

## Test Case 6: External System Replacement - Date Derivation (`gestern.ksh`)

**Purpose:**
To verify that the BigQuery `CURRENT_DATE()` and `DATE_SUB()` functions correctly replace the `gestern.ksh` script for deriving yesterday's and today's dates.

**Setup:**
No specific setup beyond the standard environment.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure. The derived dates are passed to `d_ausd_bp_ta_bpr_instance` but not explicitly used in the provided `d_ausd_bp_ta_bpr_instance` code for filtering. However, the `job_run_log` entry contains `created_at` which can be used to infer the execution date. We can also inspect the procedure's internal state if debugging. For this test, we'll focus on the `r_ausd_bp_ta_bpr_instance` procedure's internal date variables.

```sql
-- Test Case 6: Date Derivation (Conceptual - requires inspecting procedure's internal state or logging)
-- This test is harder to assert directly from outside the procedure without modifying it to log these values.
-- If we were to modify the procedure for testing:
/*
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance_TEST`(
  p_JobKennung STRING, p_EintragsNr STRING, p_Stichtag STRING, p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  INSERT INTO `your_project_id.your_dataset_id.job_run_log` (job_name, job_id, entry_nr, business_date, record_count, status, created_at, debug_heute, debug_gestern)
  VALUES ('TEST_DATE_DERIVATION', p_JobKennung, p_EintragsNr, PARSE_DATE('%d%m%Y', p_Stichtag), 0, 'SUCCESS', CURRENT_TIMESTAMP(), v_datum_heute, v_datum_gestern);
END;
*/

-- Assuming the main procedure is run and we check the `created_at` in `job_run_log`
-- and that `v_datum_heute` and `v_datum_gestern` are correctly derived internally.
-- For a more robust test, one would typically mock `CURRENT_DATE()` or assert against a known date.
```

**Pass/Fail Criterion:**
1.  **Pass:** If `v_datum_heute` and `v_datum_gestern` were logged, `v_datum_heute` should be the current date of execution, and `v_datum_gestern` should be the day before `v_datum_heute`.
2.  **Pass:** The `created_at` timestamp in `job_run_log` (from any successful run) reflects the actual execution time, confirming BigQuery's internal date functions are working as expected.

```sql
-- Verification for Test Case 6 (indirect)
-- This checks if the `created_at` in the log is consistent with the current date.
-- Direct verification of `v_datum_heute` and `v_datum_gestern` would require modifying the procedure to log them.
SELECT
  CASE
    WHEN (SELECT DATE(created_at) FROM `your_project_id.your_dataset_id.job_run_log` ORDER BY created_at DESC LIMIT 1) = CURRENT_DATE()
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_6_DateDerivation;
```

---

## Test Case 7: Data Quality - Schema and Nullability

**Purpose:**
To verify that the target table `sof_ta_bpr_instance` has the correct schema (column names, data types) and that nullability constraints are respected, especially for columns that were `NOT NULL` in the legacy system or are derived from non-nullable sources.

**Setup:**
1.  Ensure `sof_ta_bpr_instance` is created with the expected schema.
2.  Populate source tables with data that might produce `NULL`s in derived columns if not handled correctly (e.g., if `iccid_mi` was nullable and `CONCAT` didn't handle it, though in BQ `CONCAT` ignores `NULL`s by default).

**Action:**
Execute a successful run of `r_ausd_bp_ta_bpr_instance`.

```sql
-- Test Case 7: Data Quality - Schema and Nullability
-- (No specific CALL needed, relies on a successful run from other tests)
```

**Pass/Fail Criterion:**
1.  **Pass:** The schema of `your_project_id.your_dataset_id.sof_ta_bpr_instance` matches the expected schema (column names, data types).
2.  **Pass:** No `NULL` values are present in columns that are defined as `NOT NULL` in the target table.
3.  **Pass:** For the `ICCID` column, if any of its source components (`iccid_mi`, etc.) are `NULL`, the `CONCAT` function handles it gracefully (BigQuery `CONCAT` skips `NULL` arguments, which might be a behavioral difference if Oracle `||` produced `NULL` for any `NULL` operand). This needs to be explicitly checked against legacy behavior.

```sql
-- Verification for Test Case 7
-- Schema verification (conceptual, often done via information_schema or programmatic checks)
-- Example for checking a specific column's nullability:
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` WHERE CNTRCT_ID IS NULL) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_7_CNTRCT_ID_NotNull;

-- Check ICCID concatenation behavior with NULLs (if source data has NULLs for ICCID components)
-- This would require specific test data where one of the iccid_X columns is NULL.
-- If BigQuery's CONCAT(a,b,c) where b is NULL results in CONCAT(a,c), and Oracle's a||b||c where b is NULL results in NULL, this is a behavioral difference.
-- The current BigQuery CONCAT behavior is to skip NULLs, which means `CONCAT('A', NULL, 'B')` results in 'AB'.
-- If Oracle's `A || NULL || B` results in `NULL`, this is a critical difference.
-- Assuming BigQuery's CONCAT behavior is desired.
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` s
          JOIN `your_project_id.your_dataset_id.pds_ta_bpri_com` bp
            ON s.BPR_ID = bp.BPR_ID AND s.BPR_INSTANCE_ID = bp.BPRI_COM_ID
          WHERE (bp.iccid_mi IS NULL OR bp.iccid_ii IS NULL OR bp.iccid_iai IS NULL OR bp.iccid_nr IS NULL OR bp.iccid_cd IS NULL)
            AND s.ICCID IS NULL) = 0 -- If BigQuery CONCAT produces non-NULL for some NULL inputs
    THEN 'PASS (BQ CONCAT behavior)'
    ELSE 'FAIL (Unexpected NULLs in ICCID)'
  END AS TestResult_7_ICCID_NullHandling;
```

---

## Test Case 8: Idempotency and Restartability

**Purpose:**
To verify that running the job multiple times with the same parameters produces the same final state in the target table, and that logging correctly reflects each run. The `DELETE FROM ... WHERE TRUE` statement in `d_ausd_bp_ta_bpr_instance` ensures idempotency for the target data.

**Setup:**
1.  Ensure `job_run_log` and `sof_ta_bpr_instance` tables are empty or truncated.
2.  Populate source tables with valid test data.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure twice with identical parameters.

```sql
-- Test Case 8: Idempotency and Restartability
DECLARE test_job_kennung STRING DEFAULT 'TEST_JOB_008_IDEMPOTENT';
DECLARE test_eintrags_nr STRING DEFAULT '008_IDEMPOTENT';
DECLARE test_stichtag STRING DEFAULT '10102023';
DECLARE test_wiederanlauf_wert STRING DEFAULT '0';

-- Clear previous logs and target data for a clean test run
DELETE FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = test_job_kennung;
DELETE FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` WHERE TRUE;

-- First run
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
  p_JobKennung => test_job_kennung,
  p_EintragsNr => test_eintrags_nr,
  p_Stichtag => test_stichtag,
  p_wiederanlaufWert => test_wiederanlauf_wert
);

-- Capture state after first run
CREATE TEMPORARY TABLE first_run_target_state AS
SELECT * FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance`;

-- Second run (should be idempotent for target data)
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
  p_JobKennung => test_job_kennung,
  p_EintragsNr => test_eintrags_nr,
  p_Stichtag => test_stichtag,
  p_wiederanlaufWert => test_wiederanlauf_wert
);
```

**Pass/Fail Criterion:**
1.  **Pass:** The `sof_ta_bpr_instance` table contains the exact same data (row count and content) after the second run as it did after the first run.
2.  **Pass:** The `job_run_log` table contains two distinct entries for `test_job_kennung`, each with `status = 'SUCCESS'` and the correct `record_count`.

```sql
-- Verification for Test Case 8
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` EXCEPT DISTINCT SELECT * FROM first_run_target_state) = 0
         AND (SELECT COUNT(*) FROM first_run_target_state EXCEPT DISTINCT SELECT * FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance`) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_8_Idempotency_Data;

SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = 'TEST_JOB_008_IDEMPOTENT' AND status = 'SUCCESS') = 2
         AND (SELECT COUNT(DISTINCT created_at) FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = 'TEST_JOB_008_IDEMPOTENT' AND status = 'SUCCESS') = 2
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_8_Idempotency_Log;
```

---

## Test Case 9: Orchestration - Airflow DAG Parameter Passing

**Purpose:**
To verify that the Cloud Composer (Airflow) DAG correctly invokes the BigQuery stored procedure and passes parameters, especially date parameters, in the expected format. This covers external system replacement for the overall job orchestration.

**Setup:**
1.  Ensure the Airflow DAG `k_ausd_bp_ta_bpr_instance_migration_dag.py` is deployed to a Cloud Composer environment.
2.  Ensure BigQuery connection is configured in Airflow.
3.  Ensure source tables are populated with test data.
4.  Ensure `job_run_log` and `sof_ta_bpr_instance` are empty.

**Action:**
Trigger the Airflow DAG for a specific execution date (e.g., `2023-12-25`).

```python
# Test Case 9: Orchestration - Airflow DAG Parameter Passing
# This is a conceptual test, executed by triggering the Airflow DAG.

# In Airflow UI, manually trigger the DAG 'k_ausd_bp_ta_bpr_instance_migration_dag'
# for a specific logical date, e.g., 2023-12-25.
# The DAG should convert '20231225' (ds_nodash) to '25122023' for p_Stichtag.
```

**Pass/Fail Criterion:**
1.  **Pass:** The Airflow task `call_r_ausd_bp_ta_bpr_instance` completes successfully.
2.  **Pass:** The `job_run_log` table contains an entry where `job_name = 'r_ausd_bp_ta_bpr_instance'`, `status = 'SUCCESS'`, `job_id = 'DAILY_RUN'`, `entry_nr` matches the `ds_nodash` of the Airflow run (e.g., '20231225'), and `business_date` is `2023-12-25`.
3.  **Pass:** The `sof_ta_bpr_instance` table is populated with data consistent with the `p_Stichtag` derived from the Airflow execution date.

```sql
-- Verification for Test Case 9 (after DAG run)
DECLARE expected_dag_run_date DATE DEFAULT '2023-12-25'; -- Corresponds to Airflow's logical date
DECLARE expected_entry_nr STRING DEFAULT '20231225'; -- Corresponds to Airflow's ds_nodash

SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log`
          WHERE job_name = 'r_ausd_bp_ta_bpr_instance'
            AND job_id = 'DAILY_RUN'
            AND entry_nr = expected_entry_nr
            AND business_date = expected_dag_run_date
            AND status = 'SUCCESS') = 1
    THEN 'PASS'
    ELSE 'FAIL'
  END AS TestResult_9_AirflowOrchestration;

-- Further verification would involve comparing the data in sof_ta_bpr_instance
-- with an expected dataset for the given business_date.
```