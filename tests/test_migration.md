The following migration validation tests are designed to ensure the BigQuery stored procedures `proc_k_ausd_v_ta_cntrct_templ` and `proc_d_ausd_v_ta_cntrct_templ` are behaviourally equivalent to the legacy KornShell and Oracle SQL scripts.

**Important Considerations & Assumptions:**

1.  **Missing MERGE Statement:** The migration design document explicitly notes that the full `MERGE` statement from the legacy `d_ausd_v_ta_cntrct_templ.sql` was not available and needed further investigation. The provided generated BigQuery SQL (`proc_d_ausd_v_ta_cntrct_templ`) **does not contain any `MERGE` statement**, only an `INSERT`. The tests below will validate the `INSERT` logic as provided in the generated code. If the original SQL indeed had a `MERGE` operation that is critical to the business logic, this represents a significant gap in the migration and the generated code, and these tests will not cover that missing functionality. This should be flagged for further investigation.
2.  **Job Control Logic:** The legacy KornShell script contains logic for "ignoring active jobs" and "deactivating older active jobs." The generated `proc_k_ausd_v_ta_cntrct_templ` includes commented-out placeholders for this logic, indicating it's expected to be handled by an external orchestrator or a dedicated job control table, not fully within the BigQuery stored procedure itself. Therefore, tests for this specific job control flow are omitted from the BigQuery stored procedure tests, focusing instead on the parameter validation and core data processing orchestration.
3.  **Schema Mapping:** The tests assume the following BigQuery table mappings from the Oracle source:
    *   `isbert_schema.dwtk_meldungen` -> `project.dataset.isbert_dwtk_meldungen`
    *   `cds$ta_cntrct_template` -> `project.dataset.cds_ta_cntrct_template`
    *   `cds$ta_care_description` -> `project.dataset.cds_ta_care_description`
    *   `sof$ta_cntrct_templ` -> `project.dataset.sof_ta_cntrct_templ`
4.  **Test Data:** For each test, appropriate mock data must be loaded into the BigQuery source tables (`isbert_dwtk_meldungen`, `cds_ta_cntrct_template`, `cds_ta_care_description`) to simulate various scenarios.

---

## Test Case 1: Orchestration - Missing Required Parameters

**Purpose:** To verify that the migrated orchestration stored procedure (`proc_k_ausd_v_ta_cntrct_templ`) correctly validates input parameters (`p_jobkennung`, `p_eintragsnr`) and raises an error if they are missing, mimicking the `pruefeParameterGesetzt` behavior of the legacy KSH script.

**Setup:**
1.  Ensure the BigQuery stored procedures `proc_k_ausd_v_ta_cntrct_templ` and `proc_d_ausd_v_ta_cntrct_templ` are deployed.
2.  No specific data setup is required in source tables, as the error should occur before data processing.

**Action:**
Attempt to call `proc_k_ausd_v_ta_cntrct_templ` with missing or NULL values for `p_jobkennung` and `p_eintragsnr`.

**Concrete Pass/Fail Criterion:**
*   **Pass:** The call to `proc_k_ausd_v_ta_cntrct_templ` fails with an error message indicating the missing parameter, similar to:
    *   `FEHLER: Parameter "Jobkennung" (p_jobkennung) must be provided.`
    *   `FEHLER: Parameter "EintragsNr" (p_eintragsnr) must be provided.`
*   **Fail:** The stored procedure executes without raising an error for missing parameters, or raises a different, unexpected error.

```sql
-- Test 1.1: Missing p_jobkennung
CALL `project.dataset.proc_k_ausd_v_ta_cntrct_templ`(NULL, 12345);

-- Test 1.2: Empty p_jobkennung
CALL `project.dataset.proc_k_ausd_v_ta_cntrct_templ`('', 12345);

-- Test 1.3: Missing p_eintragsnr
CALL `project.dataset.proc_k_ausd_v_ta_cntrct_templ`('TEST_JOB', NULL);
```

## Test Case 2: Orchestration - Successful Execution and Record Count

**Purpose:** To verify that the migrated orchestration stored procedure successfully calls the data processing stored procedure, and accurately captures the number of records processed, mimicking the KSH script's record count capture from the temporary file.

**Setup:**
1.  Ensure `proc_k_ausd_v_ta_cntrct_templ` and `proc_d_ausd_v_ta_cntrct_templ` are deployed.
2.  Populate source tables (`isbert_dwtk_meldungen`, `cds_ta_cntrct_template`, `cds_ta_care_description`) with data that will result in a known number of records being inserted into `sof_ta_cntrct_templ`.
    *   Example: `isbert_dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` for snapshot date.
    *   Example: `cds_ta_cntrct_template` and `cds_ta_care_description` with matching `cds_description_id` and data satisfying all filter conditions.
3.  Ensure `sof_ta_cntrct_templ` is empty before the test.

**Action:**
Call `proc_k_ausd_v_ta_cntrct_templ` with valid parameters.

**Concrete Pass/Fail Criterion:**
*   **Pass:**
    1.  The stored procedure executes successfully without errors.
    2.  The `sof_ta_cntrct_templ` table contains the expected number of records.
    3.  The log message (or `v_records_processed` variable if inspected) from `proc_k_ausd_v_ta_cntrct_templ` reports the correct count of records processed.
*   **Fail:** The stored procedure fails, inserts an incorrect number of records, or reports an incorrect record count.

```sql
-- Example Setup (replace with actual data)
TRUNCATE TABLE `project.dataset.isbert_dwtk_meldungen`;
INSERT INTO `project.dataset.isbert_dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15');

TRUNCATE TABLE `project.dataset.cds_ta_cntrct_template`;
INSERT INTO `project.dataset.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production)
VALUES
  (1, 101, '2023-01-01', NULL, '2023-01-01', NULL, 1), -- Should be included
  (2, 102, '2023-01-10', '2023-01-20', '2023-01-01', NULL, 1), -- Should be excluded (modified_at > v_datum)
  (3, 103, '2023-01-05', NULL, '2023-01-01', '2023-01-10', 1), -- Should be excluded (valid_to <= v_datum)
  (4, 104, '2023-01-01', NULL, '2023-01-01', NULL, 0), -- Should be excluded (is_production = 0)
  (5, 105, '2023-01-01', NULL, '2023-01-01', NULL, 1); -- Should be included

TRUNCATE TABLE `project.dataset.cds_ta_care_description`;
INSERT INTO `project.dataset.cds_ta_care_description` (cds_description_id, language, cds_description)
VALUES
  (101, 1, 'Description A'), -- Should be included
  (102, 1, 'Description B'), -- Will be excluded by ct filters
  (103, 2, 'Description C'), -- Should be excluded (language != 1)
  (105, 1, 'Description E'); -- Should be included

TRUNCATE TABLE `project.dataset.sof_ta_cntrct_templ`;

-- Action
CALL `project.dataset.proc_k_ausd_v_ta_cntrct_templ`('TEST_JOB', 12345);

-- Verification (after call)
SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_templ`;
-- Expected count: 2 (from ct.id 1 and 5, matching cd.id 101 and 105 with language=1)

SELECT * FROM `project.dataset.sof_ta_cntrct_templ` ORDER BY CNTRCT_TEMPLATE_ID;
-- Expected content:
-- CNTRCT_TEMPLATE_ID | CDS_DESCRIPTION_ID | CDS_DESCRIPTION
-- -------------------|--------------------|----------------
-- 1                  | 101                | Description A
-- 5                  | 105                | Description E
```

## Test Case 3: Orchestration - Error Handling

**Purpose:** To verify that `proc_k_ausd_v_ta_cntrct_templ` correctly handles errors raised by `proc_d_ausd_v_ta_cntrct_templ` (or any other internal error) and raises an appropriate error message, mimicking the KSH script's error concept.

**Setup:**
1.  Deploy `proc_k_ausd_v_ta_cntrct_templ`.
2.  Modify `proc_d_ausd_v_ta_cntrct_templ` temporarily to force an error (e.g., by attempting to insert into a non-existent column, or explicitly raising an error).
    *   **Caution:** Remember to revert `proc_d_ausd_v_ta_cntrct_templ` after this test.
3.  Ensure `sof_ta_cntrct_templ` is empty.

**Action:**
Call `proc_k_ausd_v_ta_cntrct_templ` with valid parameters.

**Concrete Pass/Fail Criterion:**
*   **Pass:** The call to `proc_k_ausd_v_ta_cntrct_templ` fails with an error message that includes details about the underlying error from `proc_d_ausd_v_ta_cntrct_templ`, similar to:
    *   `ERROR: Job TEST_JOB (EintragsNr: 12345) failed. Message: <original error message from proc_d_ausd_v_ta_cntrct_templ>.`
*   **Fail:** The stored procedure executes successfully despite the error in `proc_d_ausd_v_ta_cntrct_templ`, or raises a generic, uninformative error.

```sql
-- Temporary modification to proc_d_ausd_v_ta_cntrct_templ to force an error
CREATE OR REPLACE PROCEDURE `project.dataset.proc_d_ausd_v_ta_cntrct_templ`()
BEGIN
  RAISE USING MESSAGE = 'Simulated error in data processing procedure.';
END;

-- Action
CALL `project.dataset.proc_k_ausd_v_ta_cntrct_templ`('TEST_JOB', 12345);

-- Expected outcome: Call fails with an error message containing "Simulated error in data processing procedure."

-- IMPORTANT: Revert proc_d_ausd_v_ta_cntrct_templ to its original, correct definition after this test.
```

## Test Case 4: Transformation - Snapshot Date Determination

**Purpose:** To verify that `proc_d_ausd_v_ta_cntrct_templ` correctly determines the snapshot date (`v_datum`) from `isbert_dwtk_meldungen`, including the `NVL` (COALESCE) behavior for an empty table.

**Setup:**
1.  Ensure `proc_d_ausd_v_ta_cntrct_templ` is deployed.
2.  Prepare `isbert_dwtk_meldungen` with various scenarios:
    *   Scenario A: Multiple entries for `BERT_DROP_TEMP_TABLE`.
    *   Scenario B: No entries for `BERT_DROP_TEMP_TABLE`.
    *   Scenario C: Entries for `BERT_DROP_TEMP_TABLE` but `timecreated` is NULL.

**Action:**
Call `proc_d_ausd_v_ta_cntrct_templ` and then inspect the data in `sof_ta_cntrct_templ` (which depends on `v_datum`). Alternatively, if `v_datum` can be logged or returned, inspect it directly.

**Concrete Pass/Fail Criterion:**
*   **Pass:**
    *   Scenario A: `v_datum` is the `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   Scenario B: `v_datum` defaults to `1900-01-01`.
    *   Scenario C: `v_datum` defaults to `1900-01-01` if `MAX(timecreated)` is NULL.
*   **Fail:** `v_datum` is incorrectly determined.

```sql
-- Test 4.1: Multiple entries, latest date should be picked
TRUNCATE TABLE `project.dataset.isbert_dwtk_meldungen`;
INSERT INTO `project.dataset.isbert_dwtk_meldungen` (job_kennung, timecreated) VALUES
  ('OTHER_JOB', '2023-01-01'),
  ('BERT_DROP_TEMP_TABLE', '2023-01-10'),
  ('BERT_DROP_TEMP_TABLE', '2023-01-05'),
  ('BERT_DROP_TEMP_TABLE', '2023-01-15');
-- Expected v_datum: '2023-01-15'

-- Test 4.2: No entries for BERT_DROP_TEMP_TABLE
TRUNCATE TABLE `project.dataset.isbert_dwtk_meldungen`;
INSERT INTO `project.dataset.isbert_dwtk_meldungen` (job_kennung, timecreated) VALUES
  ('OTHER_JOB', '2023-01-01');
-- Expected v_datum: '1900-01-01'

-- Test 4.3: Entries for BERT_DROP_TEMP_TABLE, but timecreated is NULL
TRUNCATE TABLE `project.dataset.isbert_dwtk_meldungen`;
INSERT INTO `project.dataset.isbert_dwtk_meldungen` (job_kennung, timecreated) VALUES
  ('BERT_DROP_TEMP_TABLE', NULL);
-- Expected v_datum: '1900-01-01'

-- To verify, you'd need to call proc_d_ausd_v_ta_cntrct_templ and then check the resulting data in sof_ta_cntrct_templ
-- with carefully crafted cds_ta_cntrct_template data that would only be included if v_datum is correct.
-- Alternatively, if the procedure could be modified to return v_datum for testing:
/*
CREATE OR REPLACE PROCEDURE `project.dataset.proc_d_ausd_v_ta_cntrct_templ`(OUT out_v_datum DATE)
BEGIN
  DECLARE v_datum DATE;
  SET v_datum = COALESCE(
    (SELECT MAX(m.timecreated) FROM `project.dataset.isbert_dwtk_meldungen` AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'),
    PARSE_DATE('%Y%m%d', '19000101')
  );
  SET out_v_datum = v_datum;
  -- ... rest of the procedure ...
END;
CALL `project.dataset.proc_d_ausd_v_ta_cntrct_templ`(v_result_date);
SELECT v_result_date;
*/
```

## Test Case 5: Transformation - Truncate Target Table

**Purpose:** To verify that `proc_d_ausd_v_ta_cntrct_templ` correctly truncates the target table `sof_ta_cntrct_templ` before inserting new data, mimicking the `DWPA_UTIL_SKRIPT.runstatement('TRUNCATE TABLE ...')` behavior.

**Setup:**
1.  Ensure `proc_d_ausd_v_ta_cntrct_templ` is deployed.
2.  Populate `sof_ta_cntrct_templ` with some dummy data.
3.  Populate source tables (`isbert_dwtk_meldungen`, `cds_ta_cntrct_template`, `cds_ta_care_description`) such that *some* new data will be inserted.

**Action:**
Call `proc_d_ausd_v_ta_cntrct_templ`.

**Concrete Pass/Fail Criterion:**
*   **Pass:** After the call, `sof_ta_cntrct_templ` contains only the newly inserted records, and none of the original dummy data.
*   **Fail:** `sof_ta_cntrct_templ` still contains the original dummy data in addition to the new records, indicating the truncate did not occur or was ineffective.

```sql
-- Example Setup
TRUNCATE TABLE `project.dataset.isbert_dwtk_meldungen`;
INSERT INTO `project.dataset.isbert_dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15');

TRUNCATE TABLE `project.dataset.cds_ta_cntrct_template`;
INSERT INTO `project.dataset.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production)
VALUES (100, 1000, '2023-01-01', NULL, '2023-01-01', NULL, 1); -- Data to be inserted

TRUNCATE TABLE `project.dataset.cds_ta_care_description`;
INSERT INTO `project.dataset.cds_ta_care_description` (cds_description_id, language, cds_description)
VALUES (1000, 1, 'New Description');

-- Populate target table with dummy data
INSERT INTO `project.dataset.sof_ta_cntrct_templ` (CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION)
VALUES (999, 9999, 'Old Dummy Data');

-- Action
CALL `project.dataset.proc_d_ausd_v_ta_cntrct_templ`();

-- Verification
SELECT * FROM `project.dataset.sof_ta_cntrct_templ`;
-- Expected: Only (100, 1000, 'New Description'), no (999, 9999, 'Old Dummy Data')
```

## Test Case 6: Transformation - Core Data Insertion Logic (Filters and Joins)

**Purpose:** To verify the correctness of the `INSERT` statement's join conditions, all filtering conditions, and NULL handling for date fields, ensuring output parity with the legacy SQL.

**Setup:**
1.  Ensure `proc_d_ausd_v_ta_cntrct_templ` is deployed.
2.  Populate `isbert_dwtk_meldungen` to set a specific `v_datum` (e.g., '2023-01-15').
3.  Populate `cds_ta_cntrct_template` and `cds_ta_care_description` with a comprehensive set of test data covering:
    *   Matching `cds_description_id` for join.
    *   Non-matching `cds_description_id`.
    *   `ct.insert_at` before, on, and after `v_datum`.
    *   `ct.modified_at` is NULL, before, on, and after `v_datum`.
    *   `ct.valid_from` before, on, and after `v_datum`.
    *   `ct.valid_to` is NULL, before, on, and after `v_datum`.
    *   `ct.is_production` = 0 and 1.
    *   `cd.language` = 1 and other values.
    *   Combinations of these conditions to test edge cases (e.g., `modified_at` is NULL and `valid_to` is NULL).

**Action:**
Call `proc_d_ausd_v_ta_cntrct_templ`.

**Concrete Pass/Fail Criterion:**
*   **Pass:** The final data in `sof_ta_cntrct_templ` exactly matches the expected output based on the legacy SQL's logic and the provided test data. This can be verified by comparing row counts and content.
*   **Fail:** The data in `sof_ta_cntrct_templ` differs from the expected output (e.g., incorrect rows included/excluded, incorrect values).

```sql
-- Example Setup (Comprehensive data for filters and joins)
TRUNCATE TABLE `project.dataset.isbert_dwtk_meldungen`;
INSERT INTO `project.dataset.isbert_dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15'); -- Sets v_datum = '2023-01-15'

TRUNCATE TABLE `project.dataset.cds_ta_cntrct_template`;
INSERT INTO `project.dataset.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production)
VALUES
  (1, 101, '2023-01-01', NULL, '2023-01-01', NULL, 1), -- INCLUDE: All conditions met (modified_at IS NULL, valid_to IS NULL)
  (2, 102, '2023-01-15', '2023-01-10', '2023-01-01', '2023-01-20', 1), -- INCLUDE: insert_at=v_datum, modified_at < v_datum, valid_from < v_datum, valid_to > v_datum
  (3, 103, '2023-01-10', '2023-01-15', '2023-01-01', NULL, 1), -- EXCLUDE: modified_at = v_datum (should be > v_datum)
  (4, 104, '2023-01-10', '2023-01-20', '2023-01-01', NULL, 1), -- EXCLUDE: modified_at > v_datum (correct)
  (5, 105, '2023-01-01', NULL, '2023-01-01', '2023-01-15', 1), -- EXCLUDE: valid_to = v_datum (should be > v_datum)
  (6, 106, '2023-01-01', NULL, '2023-01-01', '2023-01-10', 1), -- EXCLUDE: valid_to < v_datum
  (7, 107, '2023-01-01', NULL, '2023-01-20', NULL, 1), -- EXCLUDE: valid_from > v_datum
  (8, 108, '2023-01-01', NULL, '2023-01-01', NULL, 0), -- EXCLUDE: is_production = 0
  (9, 109, '2023-01-20', NULL, '2023-01-01', NULL, 1), -- EXCLUDE: insert_at > v_datum
  (10, 110, '2023-01-01', NULL, '2023-01-01', NULL, 1); -- INCLUDE: All conditions met

TRUNCATE TABLE `project.dataset.cds_ta_care_description`;
INSERT INTO `project.dataset.cds_ta_care_description` (cds_description_id, language, cds_description)
VALUES
  (101, 1, 'Desc 1'), -- Matches ct.1
  (102, 1, 'Desc 2'), -- Matches ct.2
  (103, 1, 'Desc 3'), -- Matches ct.3 (but ct.3 excluded by date)
  (104, 2, 'Desc 4'), -- Matches ct.4 (but language=2, so EXCLUDE)
  (105, 1, 'Desc 5'), -- Matches ct.5 (but ct.5 excluded by date)
  (106, 1, 'Desc 6'), -- Matches ct.6 (but ct.6 excluded by date)
  (107, 1, 'Desc 7'), -- Matches ct.7 (but ct.7 excluded by date)
  (108, 1, 'Desc 8'), -- Matches ct.8 (but ct.8 excluded by is_production)
  (109, 1, 'Desc 9'), -- Matches ct.9 (but ct.9 excluded by date)
  (110, 1, 'Desc 10'), -- Matches ct.10
  (111, 1, 'Desc 11'); -- No matching ct.cds_description_id (EXCLUDE)

TRUNCATE TABLE `project.dataset.sof_ta_cntrct_templ`;

-- Action
CALL `project.dataset.proc_d_ausd_v_ta_cntrct_templ`();

-- Verification
SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION
FROM `project.dataset.sof_ta_cntrct_templ`
ORDER BY CNTRCT_TEMPLATE_ID;

-- Expected Output:
-- CNTRCT_TEMPLATE_ID | CDS_DESCRIPTION_ID | CDS_DESCRIPTION
-- -------------------|--------------------|----------------
-- 1                  | 101                | Desc 1
-- 2                  | 102                | Desc 2
-- 10                 | 110                | Desc 10
```

## Test Case 7: Data Quality - Schema and Data Type Integrity

**Purpose:** To verify that the migrated BigQuery tables and the insertion process maintain the expected schema and data types, and that no data truncation or type conversion errors occur.

**Setup:**
1.  Ensure `proc_d_ausd_v_ta_cntrct_templ` is deployed.
2.  Populate source tables with data that includes:
    *   Maximum length strings for `cds_description`.
    *   Valid dates for all date fields.
    *   Integer values for IDs and flags.
3.  Ensure the BigQuery target table `sof_ta_cntrct_templ` has the correct schema (column names, data types, nullability) matching the Oracle target.

**Action:**
Call `proc_d_ausd_v_ta_cntrct_templ`.

**Concrete Pass/Fail Criterion:**
*   **Pass:**
    1.  The stored procedure executes without data type or schema-related errors.
    2.  The schema of `sof_ta_cntrct_templ` matches the expected target schema.
    3.  All inserted data retains its integrity (e.g., no string truncation, dates are correctly stored as DATE).
*   **Fail:** The procedure fails due to schema/type mismatch, or data in `sof_ta_cntrct_templ` shows signs of corruption (e.g., truncated strings, invalid dates).

```sql
-- Example Setup
TRUNCATE TABLE `project.dataset.isbert_dwtk_meldungen`;
INSERT INTO `project.dataset.isbert_dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15');

TRUNCATE TABLE `project.dataset.cds_ta_cntrct_template`;
INSERT INTO `project.dataset.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production)
VALUES (1, 101, '2023-01-01', NULL, '2023-01-01', NULL, 1);

TRUNCATE TABLE `project.dataset.cds_ta_care_description`;
INSERT INTO `project.dataset.cds_ta_care_description` (cds_description_id, language, cds_description)
VALUES (101, 1, 'This is a very long description string that should be fully inserted without any truncation issues if the target column has sufficient length.');

TRUNCATE TABLE `project.dataset.sof_ta_cntrct_templ`;

-- Action
CALL `project.dataset.proc_d_ausd_v_ta_cntrct_templ`();

-- Verification
-- 1. Check schema of target table
SELECT column_name, data_type, is_nullable
FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'sof_ta_cntrct_templ'
ORDER BY ordinal_position;
-- Expected: CNTRCT_TEMPLATE_ID (INT64), CDS_DESCRIPTION_ID (INT64), CDS_DESCRIPTION (STRING)

-- 2. Check data integrity
SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION
FROM `project.dataset.sof_ta_cntrct_templ`;
-- Expected: (1, 101, 'This is a very long description string that should be fully inserted without any truncation issues if the target column has sufficient length.')
-- Ensure the full string is present.
```