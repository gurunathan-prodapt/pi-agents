As a senior data-migration QA engineer, I have analyzed the provided migration design document and the generated BigQuery code. Below are the migration validation tests designed to ensure behavioral equivalence, data integrity, and correctness of the migrated job.

---

## General Pre-requisites and Setup for All Tests

Before running any of the tests, ensure the following:

*   **BigQuery Environment**:
    *   The BigQuery project (`my_project`) and dataset (`my_dataset`) exist.
    *   All DDLs (`dwtk_meldungen.sql`, `cds_ta_bp_ref.sql`, `sof_ta_bp_ref.sql`, `VIA.sql`, `job_control_table.sql`, `job_error_log.sql`) have been executed to create the necessary BigQuery tables.
    *   Both BigQuery Stored Procedures (`sp_d_ausd_v_ta_bp_ref.sql`, `sp_ausd_v_ta_bp_ref.sql`) have been deployed.
*   **Legacy Environment**:
    *   Access to the legacy Oracle database and the ability to execute the `k_ausd_v_ta_bp_ref.ksh` script.
    *   Tools or scripts to extract data from Oracle tables for comparison (e.g., SQL Developer, SQL*Plus, `sqoop`).
*   **Test Data Management**:
    *   For each test case, source tables (`BQ_DWTK_MELDUNGEN`, `BQ_CDS_TA_BP_REF`) should be populated with specific test data as described in the "Setup" section.
    *   Target tables (`BQ_SOF_TA_BP_REF`, `BQ_VIA`, `BQ_JOB_CONTROL`, `BQ_JOB_ERROR_LOG`) should be cleared before each test run to ensure idempotency and isolate test results.
    *   When comparing with legacy, ensure the Oracle source tables (`ORA_DWTK_MELDUNGEN`, `ORA_CDS_TA_BP_REF`, `ORA_VIA`) are in an identical state to their BigQuery counterparts before running the legacy job.

**Table Naming Convention**:
*   BigQuery tables: `my_project.my_dataset.<table_name>` (e.g., `my_project.my_dataset.sof_ta_bp_ref`)
*   Oracle tables: `isbert_schema.<table_name>` (e.g., `isbert_schema.sof$ta_bp_ref`)

---

## 1. Output Parity - Full End-to-End (Happy Path)

**Purpose**: To verify that the migrated BigQuery job, when executed with identical initial data, produces the same final data in `sof_ta_bp_ref` and `VIA` tables as the legacy Oracle job. This is the primary test for behavioral equivalence.

**Setup**:
1.  **Legacy Oracle**:
    *   Populate `isbert_schema.dwtk_meldungen` with diverse `timecreated` values, including one for `job_kennung = 'BERT_DROP_TEMP_TABLE'` that will define the `v_datum`.
    *   Populate `isbert_schema.cds$ta_bp_ref` with a comprehensive set of records covering all filter conditions:
        *   Records that *should* be included (meeting all criteria).
        *   Records that *should not* be included (failing one or more criteria: `insert_at > v_datum`, `modified_at <= v_datum`, `valid_from > v_datum`, `valid_to <= v_datum`, `is_production = 0`, `bp_ref_ty != 4`).
        *   Records with `modified_at IS NULL` and `valid_to IS NULL`.
    *   Populate `isbert_schema.VIA` with some existing records that will be updated by the `MERGE` and some that will not match.
    *   Ensure `isbert_schema.sof$ta_bp_ref` is empty or contains known data that will be truncated.
2.  **BigQuery**:
    *   Load an exact replica of the Oracle data from step 1 into `my_project.my_dataset.dwtk_meldungen`, `my_project.my_dataset.cds_ta_bp_ref`, and `my_project.my_dataset.VIA`.
    *   Ensure `my_project.my_dataset.sof_ta_bp_ref`, `my_project.my_dataset.job_control_table`, `my_project.my_dataset.job_error_log` are empty.

**Action**:
1.  Execute the legacy KornShell script:
    ```bash
    ./k_ausd_v_ta_bp_ref.ksh -j "TEST_JOB_PARITY" -f 1001
    ```
2.  Execute the migrated BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_JOB_PARITY', p_EintragsNr => 1001);
    ```

**Pass/Fail Criterion**:
*   **Pass**:
    *   The row count of `isbert_schema.sof$ta_bp_ref` is equal to the row count of `my_project.my_dataset.sof_ta_bp_ref`.
    *   The content of `isbert_schema.sof$ta_bp_ref` is identical to `my_project.my_dataset.sof_ta_bp_ref` (after accounting for potential column order differences and BigQuery's `INT64` vs Oracle's `NUMBER`/`INT`).
    *   The row count of `isbert_schema.VIA` is equal to the row count of `my_project.my_dataset.VIA`.
    *   The content of `isbert_schema.VIA` is identical to `my_project.my_dataset.VIA` (specifically for columns affected by the `MERGE`).
    *   The `my_project.my_dataset.job_control_table` shows a `SUCCESS` status for `TEST_JOB_PARITY` with `entry_nr = 1001`, and `records_processed` matches the count of records inserted into `my_project.my_dataset.sof_ta_bp_ref`.
    *   No errors are logged in `my_project.my_dataset.job_error_log`.
*   **Fail**: Any deviation from the above.

**Runnable Test Code (Conceptual SQL for comparison)**:
```sql
-- For sof_ta_bp_ref comparison (run these queries after both jobs complete)
-- Replace ORA_SOF_TA_BP_REF with your Oracle extraction method (e.g., a temporary BQ table loaded from Oracle)
SELECT
  (SELECT COUNT(*) FROM ORA_SOF_TA_BP_REF) = (SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_bp_ref`) AS row_count_match,
  (SELECT COUNT(*) FROM (
    SELECT cntrct_cp2_id, bp_id FROM ORA_SOF_TA_BP_REF
    EXCEPT DISTINCT
    SELECT cntrct_cp2_id, bp_id FROM `my_project.my_dataset.sof_ta_bp_ref`
  )) = 0
  AND
  (SELECT COUNT(*) FROM (
    SELECT cntrct_cp2_id, bp_id FROM `my_project.my_dataset.sof_ta_bp_ref`
    EXCEPT DISTINCT
    SELECT cntrct_cp2_id, bp_id FROM ORA_SOF_TA_BP_REF
  )) = 0 AS content_match;

-- For VIA comparison (assuming 'via_id', 'some_column', 'updated_at' are the relevant columns)
-- Replace ORA_VIA with your Oracle extraction method
SELECT
  (SELECT COUNT(*) FROM ORA_VIA) = (SELECT COUNT(*) FROM `my_project.my_dataset.VIA`) AS row_count_match,
  (SELECT COUNT(*) FROM (
    SELECT via_id, some_column, updated_at FROM ORA_VIA
    EXCEPT DISTINCT
    SELECT via_id, some_column, updated_at FROM `my_project.my_dataset.VIA`
  )) = 0
  AND
  (SELECT COUNT(*) FROM (
    SELECT via_id, some_column, updated_at FROM `my_project.my_dataset.VIA`
    EXCEPT DISTINCT
    SELECT via_id, some_column, updated_at FROM ORA_VIA
  )) = 0 AS content_match;

-- For job_control_table status
SELECT
  status = 'SUCCESS' AS job_status_ok,
  records_processed = (SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_bp_ref`) AS records_processed_match
FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_JOB_PARITY' AND entry_nr = 1001;
```

---

## 2. Transformation Correctness - Filter Logic (Date Conditions)

**Purpose**: To specifically test the correctness of date-based filtering logic in `sp_d_ausd_v_ta_bp_ref` (`insert_at`, `modified_at`, `valid_from`, `valid_to`) against the determined `v_datum`.

**Setup**:
1.  **BigQuery**:
    *   Clear all target tables.
    *   Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` to a specific date, e.g., `2023-01-15`.
        ```sql
        INSERT INTO `my_project.my_dataset.dwtk_meldungen` (timecreated, job_kennung) VALUES
        ('2023-01-15 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE'),
        ('2023-01-10 10:00:00 UTC', 'OTHER_JOB');
        ```
    *   Populate `my_project.my_dataset.cds_ta_bp_ref` with records designed to test each date filter. Ensure `is_production = 1` and `bp_ref_ty = 4` for all test records to isolate date filter testing.
        ```sql
        INSERT INTO `my_project.my_dataset.cds_ta_bp_ref` (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        (1, 101, '2023-01-15 00:00:00 UTC', NULL, '2023-01-15 00:00:00 UTC', NULL, 1, 4), -- INCLUDE (all on v_datum, modified_at/valid_to NULL)
        (2, 102, '2023-01-14 00:00:00 UTC', '2023-01-16 00:00:00 UTC', '2023-01-14 00:00:00 UTC', '2023-01-16 00:00:00 UTC', 1, 4), -- INCLUDE (all before/after v_datum)
        (3, 103, '2023-01-16 00:00:00 UTC', NULL, '2023-01-14 00:00:00 UTC', NULL, 1, 4), -- EXCLUDE (insert_at > v_datum)
        (4, 104, '2023-01-14 00:00:00 UTC', '2023-01-15 00:00:00 UTC', '2023-01-14 00:00:00 UTC', NULL, 1, 4), -- EXCLUDE (modified_at <= v_datum)
        (5, 105, '2023-01-14 00:00:00 UTC', NULL, '2023-01-16 00:00:00 UTC', NULL, 1, 4), -- EXCLUDE (valid_from > v_datum)
        (6, 106, '2023-01-14 00:00:00 UTC', NULL, '2023-01-14 00:00:00 UTC', '2023-01-15 00:00:00 UTC', 1, 4); -- EXCLUDE (valid_to <= v_datum)
        ```

**Action**:
1.  Execute the main BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_DATE_FILTERS', p_EintragsNr => 1002);
    ```

**Pass/Fail Criterion**:
*   **Pass**:
    *   The `my_project.my_dataset.sof_ta_bp_ref` table contains *only* records with `cntrct_cp2_id` 1 and 2.
    *   The `my_project.my_dataset.job_control_table` shows `SUCCESS` for `TEST_DATE_FILTERS` with `entry_nr = 1002`, and `records_processed = 2`.
    *   No errors logged in `my_project.my_dataset.job_error_log`.
*   **Fail**: Any record missing or unexpectedly present in `my_project.my_dataset.sof_ta_bp_ref`.

**Runnable Test Code (BigQuery SQL for verification)**:
```sql
-- Verify row count
SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_bp_ref`; -- Expected: 2

-- Verify content
SELECT cntrct_cp2_id, bp_id FROM `my_project.my_dataset.sof_ta_bp_ref` ORDER BY cntrct_cp2_id;
-- Expected:
-- cntrct_cp2_id | bp_id
-- --------------|-------
-- 1             | 101
-- 2             | 102

-- Verify job control status
SELECT status, records_processed FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_DATE_FILTERS' AND entry_nr = 1002;
-- Expected: status = 'SUCCESS', records_processed = 2
```

---

## 3. Transformation Correctness - `is_production` and `bp_ref_ty` Filters

**Purpose**: To verify the correctness of the `br.is_production = 1` and `br.bp_ref_ty = 4` filter conditions.

**Setup**:
1.  **BigQuery**:
    *   Clear all target tables.
    *   Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` to a date far in the past (e.g., `1900-01-01`), ensuring all date conditions would otherwise pass.
        ```sql
        INSERT INTO `my_project.my_dataset.dwtk_meldungen` (timecreated, job_kennung) VALUES
        ('1900-01-01 00:00:00 UTC', 'BERT_DROP_TEMP_TABLE');
        ```
    *   Populate `my_project.my_dataset.cds_ta_bp_ref` with records testing these specific filters:
        ```sql
        INSERT INTO `my_project.my_dataset.cds_ta_bp_ref` (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        (10, 201, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4), -- INCLUDE
        (11, 202, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 0, 4), -- EXCLUDE (is_production = 0)
        (12, 203, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 3), -- EXCLUDE (bp_ref_ty = 3)
        (13, 204, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 0, 3); -- EXCLUDE (both)
        ```

**Action**:
1.  Execute the main BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_PROD_BP_FILTERS', p_EintragsNr => 1003);
    ```

**Pass/Fail Criterion**:
*   **Pass**:
    *   `my_project.my_dataset.sof_ta_bp_ref` contains *only* the record with `cntrct_cp2_id = 10`.
    *   The `my_project.my_dataset.job_control_table` shows `SUCCESS` for `TEST_PROD_BP_FILTERS` with `entry_nr = 1003`, and `records_processed = 1`.
    *   No errors logged in `my_project.my_dataset.job_error_log`.
*   **Fail**: Any record missing or unexpectedly present in `my_project.my_dataset.sof_ta_bp_ref`.

**Runnable Test Code (BigQuery SQL for verification)**:
```sql
-- Verify row count
SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_bp_ref`; -- Expected: 1

-- Verify content
SELECT cntrct_cp2_id, bp_id FROM `my_project.my_dataset.sof_ta_bp_ref`;
-- Expected:
-- cntrct_cp2_id | bp_id
-- --------------|-------
-- 10            | 201

-- Verify job control status
SELECT status, records_processed FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_PROD_BP_FILTERS' AND entry_nr = 1003;
-- Expected: status = 'SUCCESS', records_processed = 1
```

---

## 4. Transformation Correctness - `MERGE` Operation on `VIA`

**Purpose**: To verify the `MERGE` logic for the `VIA` table, ensuring new records are inserted and existing records are updated correctly based on the `sof_ta_bp_ref` content.

**Setup**:
1.  **BigQuery**:
    *   Clear all target tables.
    *   Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` appropriately (e.g., `2023-01-01`).
    *   Populate `my_project.my_dataset.cds_ta_bp_ref` with records that will result in:
        *   `cntrct_cp2_id = 301` (for `INSERT` into `VIA`).
        *   `cntrct_cp2_id = 302` (for `UPDATE` in `VIA`).
        *   `cntrct_cp2_id = 303` (for `NO MATCH` in `VIA` - this record will be in `cds_ta_bp_ref` but not `sof_ta_bp_ref` due to filters, thus not affecting `VIA`).
        ```sql
        INSERT INTO `my_project.my_dataset.cds_ta_bp_ref` (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        (301, 3001, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4), -- Will be inserted into sof_ta_bp_ref, then VIA
        (302, 3002, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4), -- Will be inserted into sof_ta_bp_ref, then update VIA
        (303, 3003, '2023-01-02 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4); -- Will NOT be inserted into sof_ta_bp_ref (insert_at > v_datum)
        ```
    *   Populate `my_project.my_dataset.VIA` with some initial records:
        ```sql
        INSERT INTO `my_project.my_dataset.VIA` (via_id, some_column, updated_at) VALUES
        ('302', 'original_val_302', '2022-12-01 00:00:00 UTC'), -- To be updated
        ('304', 'original_val_304', '2022-12-01 00:00:00 UTC'); -- Should remain untouched
        ```

**Action**:
1.  Execute the main BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_VIA_MERGE', p_EintragsNr => 1004);
    ```

**Pass/Fail Criterion**:
*   **Pass**:
    *   `my_project.my_dataset.VIA` contains a new record for `via_id = '301'` with `some_column = 'default_val'` and `updated_at` set to the execution time.
    *   `my_project.my_dataset.VIA` has the record for `via_id = '302'` updated, with `some_column = 'updated_val'` and `updated_at` set to the execution time (different from original).
    *   `my_project.my_dataset.VIA` still contains the record for `via_id = '304'` with its original `some_column` and `updated_at` values.
    *   The `my_project.my_dataset.job_control_table` shows `SUCCESS` for `TEST_VIA_MERGE` with `entry_nr = 1004`.
    *   No errors logged in `my_project.my_dataset.job_error_log`.
*   **Fail**: Incorrect `INSERT` or `UPDATE` behavior in `my_project.my_dataset.VIA`.

**Runnable Test Code (BigQuery SQL for verification)**:
```sql
-- Check for inserted record (301)
SELECT via_id, some_column FROM `my_project.my_dataset.VIA` WHERE via_id = '301';
-- Expected: via_id = '301', some_column = 'default_val'

-- Check for updated record (302)
SELECT via_id, some_column, updated_at FROM `my_project.my_dataset.VIA` WHERE via_id = '302';
-- Expected: via_id = '302', some_column = 'updated_val', updated_at is recent (within test execution time)

-- Check for untouched record (304)
SELECT via_id, some_column, updated_at FROM `my_project.my_dataset.VIA` WHERE via_id = '304';
-- Expected: via_id = '304', some_column = 'original_val_304', updated_at = '2022-12-01 00:00:00 UTC'

-- Verify total row count in VIA
SELECT COUNT(*) FROM `my_project.my_dataset.VIA`; -- Expected: 3 (301, 302, 304)
```
**Note**: The `MERGE` logic provided in `sp_d_ausd_v_ta_bp_ref.sql` is a placeholder. This test assumes that placeholder logic is the actual intended behavior. If the actual `MERGE` logic is more complex, this test case must be updated to reflect that.

---

## 5. External System Replacements - `job_control_table` (Job Lifecycle)

**Purpose**: To verify that the `job_control_table` accurately reflects the job's lifecycle, including initial registration, deactivation of older jobs, and final status update.

**Setup**:
1.  **BigQuery**:
    *   Clear all target tables.
    *   Populate `my_project.my_dataset.dwtk_meldungen` to allow `v_datum` to be determined successfully (e.g., `2023-01-01`).
    *   Populate `my_project.my_dataset.cds_ta_bp_ref` to ensure `sp_d_ausd_v_ta_bp_ref` runs successfully and processes some records (e.g., one record that passes all filters).
    *   Pre-populate `my_project.my_dataset.job_control_table` with an "active" job for the same `p_JobKennung`:
        ```sql
        INSERT INTO `my_project.my_dataset.job_control_table` (job_kennung, entry_nr, status, start_time) VALUES
        ('TEST_JOB_LIFECYCLE', 1000, 'RUNNING', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));
        ```

**Action**:
1.  Execute the main BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_JOB_LIFECYCLE', p_EintragsNr => 1005);
    ```

**Pass/Fail Criterion**:
*   **Pass**:
    *   The record for `job_kennung = 'TEST_JOB_LIFECYCLE'` and `entry_nr = 1000` in `my_project.my_dataset.job_control_table` has `status = 'DEACTIVATED'` and `error_message = 'Deactivated by newer job instance'`.
    *   The record for `job_kennung = 'TEST_JOB_LIFECYCLE'` and `entry_nr = 1005` in `my_project.my_dataset.job_control_table` has `status = 'SUCCESS'`, `start_time` and `end_time` populated, and `records_processed` matching the actual count of records inserted into `sof_ta_bp_ref`.
    *   No errors logged in `my_project.my_dataset.job_error_log`.
*   **Fail**: Incorrect status or missing entries in `my_project.my_dataset.job_control_table`.

**Runnable Test Code (BigQuery SQL for verification)**:
```sql
-- Check status of the older job
SELECT status, error_message FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_JOB_LIFECYCLE' AND entry_nr = 1000;
-- Expected: status = 'DEACTIVATED', error_message = 'Deactivated by newer job instance'

-- Check status of the current job
SELECT status, records_processed FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_JOB_LIFECYCLE' AND entry_nr = 1005;
-- Expected: status = 'SUCCESS', records_processed > 0 (e.g., 1 from setup)
```

---

## 6. External System Replacements - `job_error_log` and Error Handling

**Purpose**: To verify that errors during job execution are correctly caught, logged to `job_error_log`, and the `job_control_table` is updated with a `FAILED` status.

**Setup**:
1.  **BigQuery**:
    *   Clear all target tables.
    *   Populate `my_project.my_dataset.dwtk_meldungen` to allow `v_datum` to be determined successfully (e.g., `2023-01-01`).
    *   **Introduce a deliberate error in `sp_d_ausd_v_ta_bp_ref` for this test.** For example, temporarily modify `sp_d_ausd_v_ta_bp_ref` to `RAISE` an error unconditionally at the beginning of its `BEGIN` block:
        ```sql
        -- Inside sp_d_ausd_v_ta_bp_ref
        BEGIN
          RAISE USING MESSAGE = 'Simulated error for TEST_ERROR_HANDLING'; -- Add this line temporarily
          DECLARE record_count INT64 DEFAULT 0;
          -- ... rest of the procedure
        ```
    *   Ensure `my_project.my_dataset.sof_ta_bp_ref`, `my_project.my_dataset.VIA` are empty.

**Action**:
1.  Execute the main BigQuery Stored Procedure. This call is expected to fail.
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_ERROR_HANDLING', p_EintragsNr => 1006);
    ```
2.  **Important**: After the test, revert the temporary change in `sp_d_ausd_v_ta_bp_ref`.

**Pass/Fail Criterion**:
*   **Pass**:
    *   The `CALL` statement for `sp_ausd_v_ta_bp_ref` raises an error (as expected by the test setup).
    *   `my_project.my_dataset.job_control_table` contains a record for `job_kennung = 'TEST_ERROR_HANDLING'` and `entry_nr = 1006` with `status = 'FAILED'` and `error_message` populated (containing "Simulated error...").
    *   `my_project.my_dataset.job_error_log` contains at least one entry for `job_kennung = 'TEST_ERROR_HANDLING'` with `error_level = 'ERROR'` and a descriptive `error_message` (containing "Job execution failed..." and "Simulated error...").
*   **Fail**: Job completes successfully, no error is logged, or `job_control_table` status is incorrect.

**Runnable Test Code (BigQuery SQL for verification)**:
```sql
-- Check job control table for FAILED status
SELECT status, error_message FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_ERROR_HANDLING' AND entry_nr = 1006;
-- Expected: status = 'FAILED', error_message LIKE '%Simulated error for TEST_ERROR_HANDLING%'

-- Check error log for detailed error
SELECT error_level, error_message FROM `my_project.my_dataset.job_error_log`
WHERE job_kennung = 'TEST_ERROR_HANDLING' AND entry_nr = 1006
ORDER BY log_time DESC LIMIT 1;
-- Expected: error_level = 'ERROR', error_message LIKE '%Job execution failed in sp_ausd_v_ta_bp_ref: Simulated error for TEST_ERROR_HANDLING%'
```

---

## 7. Data Quality - `v_datum` Determination (Edge Cases)

**Purpose**: To verify the `v_datum` determination logic, especially when `dwtk_meldungen` is empty or lacks the specific `job_kennung = 'BERT_DROP_TEMP_TABLE'`. The `COALESCE` to `DATE '1900-01-01'` should be triggered.

**Setup**:
1.  **BigQuery**:
    *   Clear all target tables.
    *   Populate `my_project.my_dataset.cds_ta_bp_ref` with some records that would normally be processed if `v_datum` was `1900-01-01` (e.g., `insert_at` in 2023).
        ```sql
        INSERT INTO `my_project.my_dataset.cds_ta_bp_ref` (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        (401, 4001, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        (402, 4002, '2023-01-02 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
        ```
    *   **Scenario A (Empty `dwtk_meldungen`)**: Ensure `my_project.my_dataset.dwtk_meldungen` is completely empty.
    *   **Scenario B (No matching `job_kennung`)**: Populate `my_project.my_dataset.dwtk_meldungen` but *without* any record where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        ```sql
        -- For Scenario B, clear dwtk_meldungen first, then insert:
        TRUNCATE TABLE `my_project.my_dataset.dwtk_meldungen`;
        INSERT INTO `my_project.my_dataset.dwtk_meldungen` (timecreated, job_kennung) VALUES
        ('2023-01-10 10:00:00 UTC', 'ANOTHER_JOB'),
        ('2023-01-12 10:00:00 UTC', 'YET_ANOTHER_JOB');
        ```

**Action**:
1.  Execute the main BigQuery Stored Procedure for Scenario A:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_VDATUM_EMPTY', p_EintragsNr => 1007);
    ```
2.  Clear `my_project.my_dataset.sof_ta_bp_ref`, `my_project.my_dataset.VIA`, `my_project.my_dataset.job_control_table`, `my_project.my_dataset.job_error_log` and re-setup `my_project.my_dataset.dwtk_meldungen` for Scenario B.
3.  Execute the main BigQuery Stored Procedure for Scenario B:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_VDATUM_NOMATCH', p_EintragsNr => 1008);
    ```

**Pass/Fail Criterion**:
*   **Pass**:
    *   For both scenarios, the `my_project.my_dataset.job_control_table` shows `SUCCESS` for the respective `job_kennung` and `entry_nr`.
    *   `my_project.my_dataset.sof_ta_bp_ref` contains all records from `my_project.my_dataset.cds_ta_bp_ref` that satisfy other filters (i.e., `cntrct_cp2_id` 401 and 402), as `v_datum` should have defaulted to `1900-01-01`.
    *   No errors logged in `my_project.my_dataset.job_error_log`.
*   **Fail**: Job fails, or `v_datum` is not correctly defaulted to `1900-01-01`, leading to incorrect `my_project.my_dataset.sof_ta_bp_ref` content.

**Runnable Test Code (BigQuery SQL for verification)**:
```sql
-- Verify row count for Scenario A
SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_bp_ref`; -- Expected: 2

-- Verify job control status for Scenario A
SELECT status FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_VDATUM_EMPTY' AND entry_nr = 1007;
-- Expected: status = 'SUCCESS'

-- (After re-running setup for Scenario B)
-- Verify row count for Scenario B
SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_bp_ref`; -- Expected: 2

-- Verify job control status for Scenario B
SELECT status FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_VDATUM_NOMATCH' AND entry_nr = 1008;
-- Expected: status = 'SUCCESS'
```

---

## 8. Data Quality - Row Count Assertion

**Purpose**: To ensure the `records_processed` reported by the BigQuery job in `job_control_table` accurately reflects the number of records inserted into `sof_ta_bp_ref`.

**Setup**:
1.  **BigQuery**:
    *   Clear all target tables.
    *   Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` (e.g., `2023-01-01`).
    *   Populate `my_project.my_dataset.cds_ta_bp_ref` with a known number of records (e.g., 5 records) that *will* pass all filter conditions.
        ```sql
        INSERT INTO `my_project.my_dataset.cds_ta_bp_ref` (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        (501, 5001, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        (502, 5002, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        (503, 5003, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        (504, 5004, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        (505, 5005, '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
        ```

**Action**:
1.  Execute the main BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_ROW_COUNT', p_EintragsNr => 1009);
    ```

**Pass/Fail Criterion**:
*   **Pass**:
    *   The `my_project.my_dataset.job_control_table` record for `TEST_ROW_COUNT` has `status = 'SUCCESS'` and `records_processed = 5`.
    *   The actual row count of `my_project.my_dataset.sof_ta_bp_ref` is 5.
*   **Fail**: `records_processed` does not match the actual count in `my_project.my_dataset.sof_ta_bp_ref`.

**Runnable Test Code (BigQuery SQL for verification)**:
```sql
-- Check records_processed in job_control_table
SELECT records_processed FROM `my_project.my_dataset.job_control_table`
WHERE job_kennung = 'TEST_ROW_COUNT' AND entry_nr = 1009;
-- Expected: 5

-- Check actual row count in target table
SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_bp_ref`;
-- Expected: 5
```

---

## 9. Schema Assertions

**Purpose**: To verify that the DDLs for the BigQuery tables match the expected schema, including data types and nullability, and are compatible with the data being loaded.

**Setup**:
1.  **BigQuery**:
    *   Ensure all DDLs (`dwtk_meldungen`, `cds_ta_bp_ref`, `sof_ta_bp_ref`, `VIA`, `job_control_table`, `job_error_log`) are applied.
    *   Populate `my_project.my_dataset.cds_ta_bp_ref` with data that includes `NULL` values for columns that are nullable in the DDL (e.g., `modified_at`, `valid_to`).
    *   Populate `my_project.my_dataset.dwtk_meldungen` with data.
    *   Run the job once (e.g., `CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'SCHEMA_TEST_RUN', p_EintragsNr => 999);`) to populate `my_project.my_dataset.sof_ta_bp_ref` and `my_project.my_dataset.VIA`.

**Action**:
1.  Inspect the schema of the BigQuery tables using `INFORMATION_SCHEMA`.
2.  Query the tables for data type consistency and nullability, especially for columns that can be `NULL`.

**Pass/Fail Criterion**:
*   **Pass**:
    *   `my_project.my_dataset.dwtk_meldungen`: `timecreated` is `TIMESTAMP`, `job_kennung` is `STRING`.
    *   `my_project.my_dataset.cds_ta_bp_ref`: `insert_at`, `modified_at`, `valid_from`, `valid_to` are `TIMESTAMP`. `is_production`, `bp_ref_ty` are `INT64`. `modified_at` and `valid_to` are nullable.
    *   `my_project.my_dataset.sof_ta_bp_ref`: `cntrct_cp2_id`, `bp_id` are `INT64`.
    *   `my_project.my_dataset.VIA`: `via_id` is `STRING`, `some_column` is `STRING`, `updated_at` is `TIMESTAMP`.
    *   `my_project.my_dataset.job_control_table`: `job_kennung` `STRING NOT NULL`, `entry_nr` `INT64 NOT NULL`, `status` `STRING`, `start_time` `TIMESTAMP`, `end_time` `TIMESTAMP`, `records_processed` `INT64`, `error_message` `STRING`.
    *   `my_project.my_dataset.job_error_log`: `log_time` `TIMESTAMP`, `job_kennung` `STRING`, `entry_nr` `INT64`, `error_level` `STRING`, `error_message` `STRING`.
    *   No data truncation or type conversion errors observed during job execution.
    *   `NULL` values are correctly handled and stored where expected (e.g., `modified_at` and `valid_to` can be `NULL` in `cds_ta_bp_ref`).
*   **Fail**: Any schema mismatch or data integrity issue.

**Runnable Test Code (BigQuery SQL for verification)**:
```sql
-- Example for checking schema of a table
SELECT
  column_name,
  data_type,
  is_nullable
FROM `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'cds_ta_bp_ref'
ORDER BY column_name;
/* Expected output for cds_ta_bp_ref:
column_name   | data_type | is_nullable
--------------|-----------|------------
bp_id         | INT64     | NO
bp_ref_ty     | INT64     | NO
cntrct_cp2_id | INT64     | NO
insert_at     | TIMESTAMP | NO
is_production | INT64     | NO
modified_at   | TIMESTAMP | YES
valid_from    | TIMESTAMP | NO
valid_to      | TIMESTAMP | YES
*/

-- Example for checking data integrity (e.g., unexpected NULLs in non-nullable columns)
SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_bp_ref` WHERE cntrct_cp2_id IS NULL;
-- Expected: 0 (assuming cntrct_cp2_id is not nullable in sof_ta_bp_ref)
```

---

## 10. Parameter Validation

**Purpose**: To ensure `sp_ausd_v_ta_bp_ref` correctly validates its input parameters (`p_JobKennung`, `p_EintragsNr`) and raises an error for invalid inputs, preventing further execution.

**Setup**:
1.  **BigQuery**:
    *   Clear `my_project.my_dataset.job_control_table` and `my_project.my_dataset.job_error_log`.

**Action**:
1.  Attempt to call `sp_ausd_v_ta_bp_ref` with `p_JobKennung` as `NULL`:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => NULL, p_EintragsNr => 1010);
    ```
2.  Attempt to call `sp_ausd_v_ta_bp_ref` with `p_JobKennung` as an empty string:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => '', p_EintragsNr => 1011);
    ```
3.  Attempt to call `sp_ausd_v_ta_bp_ref` with `p_EintragsNr` as `NULL`:
    ```sql
    CALL my_project.my_dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_PARAM_NULL_ENTRY', p_EintragsNr => NULL);
    ```

**Pass/Fail Criterion**:
*   **Pass**:
    *   Each `CALL` with invalid parameters results in a `RAISE` error with the specified error message (e.g., 'Parameter p_JobKennung darf nicht leer sein.' or 'Parameter p_EintragsNr darf nicht NULL sein.').
    *   `my_project.my_dataset.job_control_table` does *not* contain any `RUNNING` or `SUCCESS` entries for these calls. It should contain `FAILED` entries with appropriate error messages, indicating the parameter validation failure.
    *   `my_project.my_dataset.job_error_log` contains entries for these failed calls, indicating parameter validation errors.
*   **Fail**: Job proceeds with invalid parameters, or incorrect error messages are raised.

**Runnable Test Code (Conceptual Python/Pytest for error assertion)**:
```python
import pytest
from google.cloud import bigquery

client = bigquery.Client()
project_id = "my_project"
dataset_id = "my_dataset"

def test_invalid_job_kennung_null():
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL {project_id}.{dataset_id}.sp_ausd_v_ta_bp_ref(p_JobKennung => NULL, p_EintragsNr => 1010);").result()
    assert "Parameter p_JobKennung darf nicht leer sein." in str(excinfo.value)

def test_invalid_job_kennung_empty():
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL {project_id}.{dataset_id}.sp_ausd_v_ta_bp_ref(p_JobKennung => '', p_EintragsNr => 1011);").result()
    assert "Parameter p_JobKennung darf nicht leer sein." in str(excinfo.value)

def test_invalid_eintragsnr_null():
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL {project_id}.{dataset_id}.sp_ausd_v_ta_bp_ref(p_JobKennung => 'TEST_PARAM_NULL_ENTRY', p_EintragsNr => NULL);").result()
    assert "Parameter p_EintragsNr darf nicht NULL sein." in str(excinfo.value)

# Verify no successful job entries for these invalid calls
def test_no_successful_jobs_for_invalid_params():
    query = f"""
    SELECT COUNT(*) FROM `{project_id}.{dataset_id}.job_control_table`
    WHERE (job_kennung IN ('TEST_PARAM_NULL_ENTRY') OR entry_nr IN (1010, 1011))
    AND status = 'SUCCESS'
    """
    result = client.query(query).result()
    for row in result:
        assert row[0] == 0

# Verify error log entries for these invalid calls
def test_error_log_for_invalid_params():
    query = f"""
    SELECT COUNT(*) FROM `{project_id}.{dataset_id}.job_error_log`
    WHERE (job_kennung IN ('TEST_PARAM_NULL_ENTRY') OR entry_nr IN (1010, 1011))
    AND error_level = 'ERROR'
    """
    result = client.query(query).result()
    for row in result:
        assert row[0] >= 3 # At least one for each failed call
```