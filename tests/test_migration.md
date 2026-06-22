As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `r_ausd_v_ta_p_discount.ksh` to Google BigQuery. These tests aim to ensure behavioral equivalence, data integrity, and correctness across all aspects of the re-platformed ETL workflow.

The tests are categorized by the requested validation areas: Output Parity, Transformation Correctness, External-System Replacements, and Data Quality/Schema Assertions. Each test case includes its purpose, setup, action, and a concrete pass/fail criterion, with runnable SQL examples where applicable.

---

## Migration Validation Tests: `r_ausd_v_ta_p_discount`

### 1. Output Parity

#### Test Case 1.1: Full Data Parity (Happy Path)

*   **Purpose:** To verify that the final `sof_ta_p_discount` table in BigQuery is identical to the Oracle `sof$ta_p_discount` table after running the job with a representative, valid dataset. This is the ultimate behavioral equivalence check.
*   **Setup:**
    1.  **Legacy System:** Populate the Oracle source tables (`sof$ta_disc_zusgf`, `sof$ta_cntrct_crs`, `dwtk_meldungen`) with a diverse, representative dataset that covers various valid scenarios (e.g., multiple matching records, single matches, different `disc_vector_ty` values).
    2.  **Legacy System:** Execute the original `r_ausd_v_ta_p_discount.ksh` job in the Oracle environment.
    3.  **Legacy System:** Extract the final content of the Oracle `sof$ta_p_discount` table into a canonical format (e.g., CSV, JSON) or a temporary comparison table in BigQuery.
    4.  **Migrated System:** Populate the BigQuery source tables (`project.dataset.sof_ta_disc_zusgf`, `project.dataset.sof_ta_cntrct_crs`, `project.dataset.dwtk_meldungen`) with *exactly the same data* as used in the Oracle setup.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('BERT_R_AUS_D_TA_P_DISCOUNT', '12345');
        ```
    2.  Query the resulting `project.dataset.sof_ta_p_discount` table.
*   **Pass/Fail Criterion:**
    *   The BigQuery `project.dataset.sof_ta_p_discount` table must contain the exact same number of rows as the Oracle `sof$ta_p_discount` table.
    *   Every row and column value in the BigQuery `sof_ta_p_discount` table must precisely match the corresponding row and column value in the Oracle `sof$ta_p_discount` table.
    *   **Runnable Test Code (SQL Assertion):**
        ```sql
        -- Assuming 'legacy_sof_ta_p_discount_snapshot' is a BigQuery table
        -- containing the exact data extracted from Oracle after its run.
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_p_discount`) = (SELECT COUNT(*) FROM `project.dataset.legacy_sof_ta_p_discount_snapshot`)
                     AND NOT EXISTS (
                        (SELECT * FROM `project.dataset.sof_ta_p_discount` EXCEPT DISTINCT SELECT * FROM `project.dataset.legacy_sof_ta_p_discount_snapshot`)
                        UNION ALL
                        (SELECT * FROM `project.dataset.legacy_sof_ta_p_discount_snapshot` EXCEPT DISTINCT SELECT * FROM `project.dataset.sof_ta_p_discount`)
                     )
                THEN 'PASS: Output tables are identical'
                ELSE 'FAIL: Output tables differ'
            END AS test_result;
        ```
        *Note: For a more detailed failure analysis, run the `EXCEPT DISTINCT` queries separately to identify differing rows.*

### 2. Transformation Correctness

#### Test Case 2.1: `v_datum` Derivation Correctness

*   **Purpose:** To verify that the `v_datum` variable, representing the processing date, is correctly derived from the `dwtk_meldungen` table using the `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` and handles `NULL` results with `COALESCE('19000101')`.
*   **Setup:**
    1.  **Scenario A: Valid `timecreated` exists.**
        ```sql
        TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
        INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('OTHER_JOB', '2023-01-01 10:00:00 UTC'),
        ('BERT_DROP_TEMP_TABLE', '2023-03-15 12:30:00 UTC'),
        ('BERT_DROP_TEMP_TABLE', '2023-03-10 09:00:00 UTC');
        ```
    2.  **Scenario B: No `BERT_DROP_TEMP_TABLE` entries.**
        ```sql
        TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
        INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('OTHER_JOB', '2023-01-01 10:00:00 UTC');
        ```
    3.  **Scenario C: `dwtk_meldungen` is empty.**
        ```sql
        TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
        ```
*   **Action:**
    1.  For each scenario, execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB', '1');
        ```
    2.  Query the `project.dataset.job_log` table for the "Derived processing date" message.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The `job_log` should contain an entry with `message` like `'Derived processing date: 20230315'`.
    *   **Scenario B & C:** The `job_log` should contain an entry with `message` like `'Derived processing date: 19000101'`.
    *   **Runnable Test Code (SQL Assertion for Scenario A):**
        ```sql
        -- After running the SP with Scenario A setup
        SELECT
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM `project.dataset.job_log`
                    WHERE message LIKE 'Derived processing date: 20230315'
                    AND job_name = 'r_ausd_v_ta_p_discount'
                )
                THEN 'PASS: v_datum derived correctly for existing data'
                ELSE 'FAIL: v_datum derivation incorrect for existing data'
            END AS test_result;
        ```

#### Test Case 2.2: Join Logic Correctness (Inner Join)

*   **Purpose:** To verify that the `JOIN` condition (`da.cntrct_id = c.cntrct_id AND da.cntrct_obj_version = c.obj_version`) correctly filters and combines records, behaving as an inner join.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.sof_ta_disc_zusgf`;
    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_crs`;
    TRUNCATE TABLE `project.dataset.sof_ta_p_discount`; -- Ensure target is clean

    INSERT INTO `project.dataset.sof_ta_disc_zusgf` (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('C1', 'TYPE_A', 'V1', 'R1'), -- Match with C1/V1
    ('C2', 'TYPE_B', 'V2', 'R2'), -- Match with C2/V2
    ('C3', 'TYPE_C', 'V3', 'R3'), -- No match in cntrct_crs
    ('C4', 'TYPE_D', 'V4', 'R4'), -- cntrct_id match, obj_version mismatch
    ('C5', 'TYPE_E', 'V5', 'R5'); -- cntrct_id match, obj_version mismatch

    INSERT INTO `project.dataset.sof_ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES
    ('C1', 'V1', 'CN1'), -- Match with C1/V1
    ('C2', 'V2', 'CN2'), -- Match with C2/V2
    ('C6', 'V6', 'CN6'), -- No match in disc_zusgf
    ('C4', 'V99', 'CN4_V99'), -- cntrct_id match, obj_version mismatch
    ('C5', 'V55', 'CN5_V55'); -- cntrct_id match, obj_version mismatch
    ```
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_JOIN', '2');
        ```
    2.  Query the `project.dataset.sof_ta_p_discount` table.
*   **Pass/Fail Criterion:**
    *   The `sof_ta_p_discount` table must contain exactly 2 rows.
    *   The rows must be:
        *   `('C1', 'TYPE_A', 'V1', 'R1', 'CN1')`
        *   `('C2', 'TYPE_B', 'V2', 'R2', 'CN2')`
    *   **Runnable Test Code (SQL Assertion):**
        ```sql
        -- After running the SP with the setup data
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_p_discount`) = 2
                     AND EXISTS (SELECT 1 FROM `project.dataset.sof_ta_p_discount` WHERE cntrct_id = 'C1' AND disc_vector_ty = 'TYPE_A' AND cntrct_obj_version = 'V1' AND rabatt_alle = 'R1' AND contract_number = 'CN1')
                     AND EXISTS (SELECT 1 FROM `project.dataset.sof_ta_p_discount` WHERE cntrct_id = 'C2' AND disc_vector_ty = 'TYPE_B' AND cntrct_obj_version = 'V2' AND rabatt_alle = 'R2' AND contract_number = 'CN2')
                THEN 'PASS: Join logic correctly identified matching records'
                ELSE 'FAIL: Join logic incorrect'
            END AS test_result;
        ```

#### Test Case 2.3: NULL Handling in Join Keys

*   **Purpose:** To verify that records with `NULL` values in the join keys (`cntrct_id`, `cntrct_obj_version`/`obj_version`) are correctly excluded by the `JOIN` operation, as is standard SQL behavior for inner joins.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.sof_ta_disc_zusgf`;
    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_crs`;
    TRUNCATE TABLE `project.dataset.sof_ta_p_discount`;

    INSERT INTO `project.dataset.sof_ta_disc_zusgf` (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('C1', 'TYPE_A', 'V1', 'R1'),     -- Valid match
    (NULL, 'TYPE_B', 'V2', 'R2'),     -- NULL cntrct_id
    ('C3', 'TYPE_C', NULL, 'R3'),     -- NULL cntrct_obj_version
    (NULL, 'TYPE_D', NULL, 'R4');     -- Both NULL

    INSERT INTO `project.dataset.sof_ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES
    ('C1', 'V1', 'CN1'),              -- Valid match
    (NULL, 'V2', 'CN2_NULL_ID'),      -- NULL cntrct_id
    ('C3', NULL, 'CN3_NULL_VER'),     -- NULL obj_version
    (NULL, NULL, 'CN4_BOTH_NULL');    -- Both NULL
    ```
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_NULLS', '3');
        ```
    2.  Query the `project.dataset.sof_ta_p_discount` table.
*   **Pass/Fail Criterion:**
    *   The `sof_ta_p_discount` table must contain exactly 1 row.
    *   The row must be: `('C1', 'TYPE_A', 'V1', 'R1', 'CN1')`.
    *   No rows with `NULL` in `cntrct_id` or `cntrct_obj_version` (or `obj_version`) should be present.
    *   **Runnable Test Code (SQL Assertion):**
        ```sql
        -- After running the SP with the setup data
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_p_discount`) = 1
                     AND EXISTS (SELECT 1 FROM `project.dataset.sof_ta_p_discount` WHERE cntrct_id = 'C1' AND disc_vector_ty = 'TYPE_A' AND cntrct_obj_version = 'V1' AND rabatt_alle = 'R1' AND contract_number = 'CN1')
                THEN 'PASS: NULLs in join keys correctly excluded'
                ELSE 'FAIL: NULL handling in join keys incorrect'
            END AS test_result;
        ```

#### Test Case 2.4: Data Type Conversion/Compatibility

*   **Purpose:** To ensure that all column values are correctly mapped and inserted into `sof_ta_p_discount` without data loss, truncation, or unexpected type coercion, especially given that all DDLs use `STRING` type.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.sof_ta_disc_zusgf`;
    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_crs`;
    TRUNCATE TABLE `project.dataset.sof_ta_p_discount`;

    INSERT INTO `project.dataset.sof_ta_disc_zusgf` (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('ID-12345', 'DISC_TYPE_LONG_STRING_WITH_SPECIAL_CHARS_!@#$', 'V-999.01', 'RABATT_VALUE_123.45'),
    ('ID-67890', 'ANOTHER_TYPE', 'V-100.00', 'RABATT_VALUE_67.89');

    INSERT INTO `project.dataset.sof_ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES
    ('ID-12345', 'V-999.01', 'CONTRACT_NUM_ABC-XYZ-123'),
    ('ID-67890', 'V-100.00', 'CONTRACT_NUM_DEF-UVW-456');
    ```
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_TYPES', '4');
        ```
    2.  Query the `project.dataset.sof_ta_p_discount` table.
*   **Pass/Fail Criterion:**
    *   The `sof_ta_p_discount` table must contain 2 rows.
    *   All column values in the target table must exactly match the source values, demonstrating no truncation or alteration due to type handling.
    *   **Runnable Test Code (SQL Assertion):**
        ```sql
        -- After running the SP with the setup data
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_p_discount`) = 2
                     AND EXISTS (
                        SELECT 1 FROM `project.dataset.sof_ta_p_discount`
                        WHERE cntrct_id = 'ID-12345'
                          AND disc_vector_ty = 'DISC_TYPE_LONG_STRING_WITH_SPECIAL_CHARS_!@#$'
                          AND cntrct_obj_version = 'V-999.01'
                          AND rabatt_alle = 'RABATT_VALUE_123.45'
                          AND contract_number = 'CONTRACT_NUM_ABC-XYZ-123'
                     )
                     AND EXISTS (
                        SELECT 1 FROM `project.dataset.sof_ta_p_discount`
                        WHERE cntrct_id = 'ID-67890'
                          AND disc_vector_ty = 'ANOTHER_TYPE'
                          AND cntrct_obj_version = 'V-100.00'
                          AND rabatt_alle = 'RABATT_VALUE_67.89'
                          AND contract_number = 'CONTRACT_NUM_DEF-UVW-456'
                     )
                THEN 'PASS: Data types and values preserved correctly'
                ELSE 'FAIL: Data type conversion or value mismatch'
            END AS test_result;
        ```

### 3. External-System Replacements

#### Test Case 3.1: `TRUNCATE TABLE` Replacement

*   **Purpose:** To verify that the BigQuery `TRUNCATE TABLE` statement correctly clears the target table before insertion, effectively replacing the Oracle `DWPA_UTIL_SKRIPT.runstatement` call.
*   **Setup:**
    ```sql
    -- Populate target table with some initial data
    TRUNCATE TABLE `project.dataset.sof_ta_p_discount`;
    INSERT INTO `project.dataset.sof_ta_p_discount` (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle, contract_number) VALUES
    ('OLD_C1', 'OLD_T1', 'OLD_V1', 'OLD_R1', 'OLD_CN1'),
    ('OLD_C2', 'OLD_T2', 'OLD_V2', 'OLD_R2', 'OLD_CN2');

    -- Populate source tables with new data
    TRUNCATE TABLE `project.dataset.sof_ta_disc_zusgf`;
    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_crs`;
    INSERT INTO `project.dataset.sof_ta_disc_zusgf` (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('NEW_C1', 'NEW_T1', 'NEW_V1', 'NEW_R1');
    INSERT INTO `project.dataset.sof_ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES
    ('NEW_C1', 'NEW_V1', 'NEW_CN1');
    ```
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_TRUNCATE', '5');
        ```
    2.  Query the `project.dataset.sof_ta_p_discount` table.
    3.  Query the `project.dataset.job_log` table for the "Truncated target table" message.
*   **Pass/Fail Criterion:**
    *   The `sof_ta_p_discount` table must contain exactly 1 row.
    *   The row must be `('NEW_C1', 'NEW_T1', 'NEW_V1', 'NEW_R1', 'NEW_CN1')`. The old rows must be gone.
    *   The `job_log` table must contain an entry with `message` like `'Truncated target table sof_ta_p_discount'`.
    *   **Runnable Test Code (SQL Assertion):**
        ```sql
        -- After running the SP with the setup data
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_p_discount`) = 1
                     AND EXISTS (SELECT 1 FROM `project.dataset.sof_ta_p_discount` WHERE cntrct_id = 'NEW_C1')
                     AND NOT EXISTS (SELECT 1 FROM `project.dataset.sof_ta_p_discount` WHERE cntrct_id = 'OLD_C1')
                     AND EXISTS (SELECT 1 FROM `project.dataset.job_log` WHERE message LIKE 'Truncated target table sof_ta_p_discount%')
                THEN 'PASS: TRUNCATE TABLE and logging successful'
                ELSE 'FAIL: TRUNCATE TABLE or logging failed'
            END AS test_result;
        ```

#### Test Case 3.2: Logging and Error Handling Replacement

*   **Purpose:** To verify that the BigQuery `job_log` and `job_error_log` tables correctly capture job execution details and errors, replacing the KornShell logging and `f_alis_msgerr.ksh`.
*   **Setup:**
    1.  **Scenario A: Successful Execution.** Ensure source tables are populated for a successful run.
    2.  **Scenario B: Intentional Error.** Create a condition that will cause the stored procedure to fail (e.g., temporarily drop `sof_ta_disc_zusgf` table).
*   **Action:**
    1.  For Scenario A, execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_LOG_SUCCESS', '6');
        ```
    2.  For Scenario B, execute the BigQuery Stored Procedure:
        ```sql
        -- First, cause an error condition, e.g.:
        -- DROP TABLE `project.dataset.sof_ta_disc_zusgf`;
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_LOG_FAIL', '7');
        -- Recreate table after test if needed:
        -- CREATE TABLE `project.dataset.sof_ta_disc_zusgf` (...)
        ```
    3.  Query `project.dataset.job_log` and `project.dataset.job_error_log` tables.
*   **Pass/Fail Criterion:**
    *   **Scenario A (Success):**
        *   `job_log` must contain entries for 'STARTED', 'Derived processing date', 'Truncated target table', 'Successfully inserted X records', and a final 'COMPLETED' status entry for `job_name = 'r_ausd_v_ta_p_discount'`.
        *   `job_error_log` must contain no entries for this specific job run.
    *   **Scenario B (Failure):**
        *   `job_log` must contain 'STARTED' and a final 'FAILED' status entry for `job_name = 'r_ausd_v_ta_p_discount'`. The 'Successfully inserted' message should not be present.
        *   `job_error_log` must contain an entry for this job run with `severity = 'ERROR'`, a descriptive `error_message`, and a populated `stack_trace`.
    *   **Runnable Test Code (SQL Assertion for Scenario B failure):**
        ```sql
        -- After running the SP with Scenario B setup (e.g., dropped table)
        SELECT
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM `project.dataset.job_log`
                    WHERE job_name = 'r_ausd_v_ta_p_discount'
                      AND status = 'FAILED'
                      AND message LIKE 'Job failed%'
                      AND start_time = (SELECT MAX(start_time) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_v_ta_p_discount' AND message LIKE 'Job started%')
                )
                AND EXISTS (
                    SELECT 1 FROM `project.dataset.job_error_log`
                    WHERE job_name = 'r_ausd_v_ta_p_discount'
                      AND severity = 'ERROR'
                      AND error_message IS NOT NULL
                      AND stack_trace IS NOT NULL
                      AND error_time >= (SELECT MAX(start_time) FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_v_ta_p_discount' AND message LIKE 'Job started%')
                )
                THEN 'PASS: Error logging and status update successful'
                ELSE 'FAIL: Error logging or status update incorrect'
            END AS test_result;
        ```

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Schema Validation

*   **Purpose:** To verify that the schema of the target table `sof_ta_p_discount` in BigQuery matches the expected schema (column names, data types, order, nullability if specified).
*   **Setup:** Ensure the `project.dataset.sof_ta_p_discount` table has been created using the provided DDL.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for the table details.
*   **Pass/Fail Criterion:**
    *   The table must exist.
    *   It must have exactly 5 columns.
    *   The column names and their respective data types must match the DDL:
        *   `cntrct_id` (STRING)
        *   `disc_vector_ty` (STRING)
        *   `cntrct_obj_version` (STRING)
        *   `rabatt_alle` (STRING)
        *   `contract_number` (STRING)
    *   **Runnable Test Code (SQL Assertion):**
        ```sql
        SELECT
            CASE
                WHEN (
                    SELECT COUNT(*)
                    FROM `project.dataset`.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = 'sof_ta_p_discount'
                ) = 5
                AND EXISTS (SELECT 1 FROM `project.dataset`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'sof_ta_p_discount' AND column_name = 'cntrct_id' AND data_type = 'STRING')
                AND EXISTS (SELECT 1 FROM `project.dataset`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'sof_ta_p_discount' AND column_name = 'disc_vector_ty' AND data_type = 'STRING')
                AND EXISTS (SELECT 1 FROM `project.dataset`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'sof_ta_p_discount' AND column_name = 'cntrct_obj_version' AND data_type = 'STRING')
                AND EXISTS (SELECT 1 FROM `project.dataset`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'sof_ta_p_discount' AND column_name = 'rabatt_alle' AND data_type = 'STRING')
                AND EXISTS (SELECT 1 FROM `project.dataset`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'sof_ta_p_discount' AND column_name = 'contract_number' AND data_type = 'STRING')
                THEN 'PASS: Target table schema matches expected DDL'
                ELSE 'FAIL: Target table schema mismatch'
            END AS test_result;
        ```

#### Test Case 4.2: Row Count Parity

*   **Purpose:** To verify that the number of rows inserted into `sof_ta_p_discount` in BigQuery matches the number of rows inserted by the legacy Oracle job for the same input. This also validates the `@@row_count` capture.
*   **Setup:**
    1.  **Legacy System:** Run the Oracle job with a known set of source data and record the exact number of rows inserted into `sof$ta_p_discount`. Let's assume this is `N` rows.
    2.  **Migrated System:** Populate BigQuery source tables (`sof_ta_disc_zusgf`, `sof_ta_cntrct_crs`, `dwtk_meldungen`) with *exactly the same data* as used in the Oracle setup.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_ROWCOUNT', '8');
        ```
    2.  Query the `project.dataset.sof_ta_p_discount` table for its row count.
    3.  Query the `project.dataset.job_log` table for the `records_processed` value for the latest run.
*   **Pass/Fail Criterion:**
    *   The row count of `project.dataset.sof_ta_p_discount` must be equal to `N`.
    *   The `records_processed` value in the `job_log` for the latest successful run must be equal to `N`.
    *   **Runnable Test Code (SQL Assertion, assuming N=2 for example):**
        ```sql
        -- After running the SP with setup data that should result in 2 rows
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_p_discount`) = 2
                     AND (SELECT records_processed FROM `project.dataset.job_log` WHERE job_name = 'r_ausd_v_ta_p_discount' AND status = 'COMPLETED' ORDER BY start_time DESC LIMIT 1) = 2
                THEN 'PASS: Row count and logged records_processed match expected'
                ELSE 'FAIL: Row count or logged records_processed mismatch'
            END AS test_result;
        ```

#### Test Case 4.3: Data Integrity (No Duplicates)

*   **Purpose:** To ensure that the `INSERT` operation does not introduce duplicate records into `sof_ta_p_discount`, assuming the combination of `cntrct_id`, `disc_vector_ty`, and `cntrct_obj_version` forms a natural key.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.sof_ta_disc_zusgf`;
    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_crs`;
    TRUNCATE TABLE `project.dataset.sof_ta_p_discount`;

    -- Create source data that, if joined incorrectly, might produce duplicates
    -- (e.g., if obj_version was ignored in join, or if there were duplicates in source)
    INSERT INTO `project.dataset.sof_ta_disc_zusgf` (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle) VALUES
    ('C1', 'TYPE_A', 'V1', 'R1'),
    ('C1', 'TYPE_A', 'V2', 'R2'); -- Same cntrct_id, disc_vector_ty, different obj_version

    INSERT INTO `project.dataset.sof_ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES
    ('C1', 'V1', 'CN1'),
    ('C1', 'V2', 'CN2');
    ```
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_DUPLICATES', '9');
        ```
    2.  Query the `project.dataset.sof_ta_p_discount` table.
*   **Pass/Fail Criterion:**
    *   The `sof_ta_p_discount` table must contain exactly 2 rows.
    *   There should be no duplicate rows based on the combination of `cntrct_id`, `disc_vector_ty`, and `cntrct_obj_version`.
    *   **Runnable Test Code (SQL Assertion):**
        ```sql
        -- After running the SP with the setup data
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_p_discount`) = 2
                     AND (SELECT COUNT(DISTINCT CONCAT(cntrct_id, '|', disc_vector_ty, '|', cntrct_obj_version)) FROM `project.dataset.sof_ta_p_discount`) = 2
                THEN 'PASS: No duplicate records introduced'
                ELSE 'FAIL: Duplicate records found in target table'
            END AS test_result;
        ```

---

**General Notes for Execution:**

*   **Test Environment:** These tests should be executed in a dedicated BigQuery test environment that mirrors the production setup (project, dataset names).
*   **Data Isolation:** Each test case's setup should ensure data isolation, typically by `TRUNCATE`ing and re-inserting data into source and target tables.
*   **Pytest Integration:** These SQL assertions can be wrapped in Python `pytest` functions using the `google-cloud-bigquery` client library to execute the SQL and assert the results programmatically.
*   **Legacy Data Snapshots:** For output parity tests, reliable snapshots of legacy Oracle data are crucial. This might involve temporary BigQuery tables loaded from Oracle exports.
*   **Parameterization:** The `p_job_kennung` and `p_eintrags_nr` parameters for the BigQuery Stored Procedure should be varied in tests where their values might influence logging or behavior, although for this specific job, they primarily affect logging.