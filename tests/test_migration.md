As a senior data-migration QA engineer, I've reviewed the migration design document and the provided BigQuery stored procedure (`sp_k_ausd_bp_ta_tarifoption.sql`). The migration involves translating complex Oracle SQL and KornShell logic to BigQuery, including dynamic table naming, custom UDFs, and specific analytic function usage.

The absence of the original legacy source code (especially for the Oracle SQL and custom `concatX` functions) presents a significant challenge. For the purpose of these tests, I will assume that the *intended* behavior of the legacy system, as described in the design document, is correctly captured by the BigQuery implementation. Where specific legacy logic is unknown (e.g., exact `concatX` function behavior), I will highlight this and propose tests based on common string manipulation patterns and the context provided.

The tests are categorized to cover output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_TARIFOPTION

### Test 1: Output Parity - Final `sof_ta_tarifoption` Table

*   **Purpose**: To verify that the final output table `project.dataset.sof_ta_tarifoption` produced by the migrated job is identical to the legacy `sof$ta_tarifoption` table for a given set of inputs. This is the ultimate measure of behavioral equivalence.
*   **Setup**:
    1.  **Legacy Data Snapshot**: Obtain a full data snapshot of all relevant Oracle source tables (`isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, `sof$ta_bpr_opt_text_YYYYMMDD` for a specific `YYYYMMDD`) and the final output table (`sof$ta_tarifoption`) for a specific historical run date (`p_stichtag`). Let's call this `LEGACY_STAGING_DB`.
    2.  **BigQuery Source Data**: Load the exact same snapshot data into the corresponding BigQuery source tables: `project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_l_bpr_optionen_filter`, `project.dataset.sof_ta_bpr_opt_text_YYYYMMDD`. Ensure data types are accurately mapped.
    3.  **UDFs**: Ensure all `project.dataset.concatX` UDFs are deployed and their logic is confirmed to match the legacy Oracle functions.
    4.  **Parameters**: Identify the `p_jobkennung`, `p_eintragsnr`, `p_stichtag` (in DDMMYYYY format), and `p_wiederanlaufwert` that were used for the legacy run.
*   **Action**:
    1.  Execute the BigQuery stored procedure:
        ```sql
        CALL `project.dataset.sp_k_ausd_bp_ta_tarifoption`(
          'LEGACY_JOB_ID', -- p_jobkennung
          '12345',         -- p_eintragsnr
          'DDMMYYYY',      -- p_stichtag (e.g., '01012023')
          0                -- p_wiederanlaufwert
        );
        ```
        (Replace placeholders with actual values from the legacy run).
    2.  After execution, query the `project.dataset.sof_ta_tarifoption` table.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `project.dataset.sof_ta_tarifoption` table contains exactly the same number of rows and identical data in all columns as the legacy `sof$ta_tarifoption` table.
    *   **Fail**: Any discrepancy in row count or data content.

*   **Runnable Test Code (SQL Assertion)**:

    ```sql
    -- Assuming legacy_sof_ta_tarifoption is a BigQuery table containing the exact legacy output
    -- for the specific run being tested.

    -- 1. Check row counts
    SELECT
      CASE
        WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_tarifoption`) = (SELECT COUNT(*) FROM `LEGACY_STAGING_DB.sof_ta_tarifoption_snapshot`)
        THEN 'PASS: Row counts match'
        ELSE 'FAIL: Row counts differ'
      END AS row_count_check;

    -- 2. Check for missing rows in migrated output
    SELECT
      CASE
        WHEN COUNT(*) = 0
        THEN 'PASS: No missing rows in migrated output'
        ELSE 'FAIL: Migrated output is missing rows'
      END AS missing_rows_check
    FROM `LEGACY_STAGING_DB.sof_ta_tarifoption_snapshot` AS legacy
    LEFT JOIN `project.dataset.sof_ta_tarifoption` AS migrated
      ON legacy.cntrct_id = migrated.cntrct_id
      AND legacy.business_option = migrated.business_option
      AND legacy.sonstige_option = migrated.sonstige_option
      AND legacy.gprs_option = migrated.gprs_option
    WHERE migrated.cntrct_id IS NULL;

    -- 3. Check for extra rows in migrated output
    SELECT
      CASE
        WHEN COUNT(*) = 0
        THEN 'PASS: No extra rows in migrated output'
        ELSE 'FAIL: Migrated output has extra rows'
      END AS extra_rows_check
    FROM `project.dataset.sof_ta_tarifoption` AS migrated
    LEFT JOIN `LEGACY_STAGING_DB.sof_ta_tarifoption_snapshot` AS legacy
      ON migrated.cntrct_id = legacy.cntrct_id
      AND migrated.business_option = legacy.business_option
      AND migrated.sonstige_option = legacy.sonstige_option
      AND migrated.gprs_option = legacy.gprs_option
    WHERE legacy.cntrct_id IS NULL;

    -- 4. Check for data discrepancies (if row counts match and no missing/extra rows)
    -- This query will return any rows where data differs between legacy and migrated.
    -- An empty result set indicates data parity.
    SELECT
      'FAIL: Data discrepancy found' AS status,
      legacy.cntrct_id,
      legacy.business_option AS legacy_business_option,
      migrated.business_option AS migrated_business_option,
      legacy.sonstige_option AS legacy_sonstige_option,
      migrated.sonstige_option AS migrated_sonstige_option,
      legacy.gprs_option AS legacy_gprs_option,
      migrated.gprs_option AS migrated_gprs_option
    FROM `LEGACY_STAGING_DB.sof_ta_tarifoption_snapshot` AS legacy
    JOIN `project.dataset.sof_ta_tarifoption` AS migrated
      ON legacy.cntrct_id = migrated.cntrct_id
    WHERE
      NOT (legacy.business_option IS NOT DISTINCT FROM migrated.business_option) OR
      NOT (legacy.sonstige_option IS NOT DISTINCT FROM migrated.sonstige_option) OR
      NOT (legacy.gprs_option IS NOT DISTINCT FROM migrated.gprs_option);
    ```

### Test 2: Transformation Correctness - Dynamic Table Resolution

*   **Purpose**: To ensure the dynamic table `sof$ta_bpr_opt_text_&v_datum` is correctly resolved in BigQuery based on the `MAX(timecreated)` logic from `dwtk_meldungen`.
*   **Setup**:
    1.  **`dwtk_meldungen` Data**: Populate `project.dataset.dwtk_meldungen` with test data, including multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with varying `timecreated` values.
        *   Example:
            *   `job_kennung='OTHER_JOB', timecreated='2023-01-01 10:00:00'`
            *   `job_kennung='BERT_DROP_TEMP_TABLE', timecreated='2023-01-05 12:00:00'`
            *   `job_kennung='BERT_DROP_TEMP_TABLE', timecreated='2023-01-10 08:00:00'` (This should be selected)
            *   `job_kennung='BERT_DROP_TEMP_TABLE', timecreated='2023-01-08 15:00:00'`
    2.  **Dynamic Tables**: Create multiple `project.dataset.sof_ta_bpr_opt_text_YYYYMMDD` tables (e.g., `sof_ta_bpr_opt_text_20230105`, `sof_ta_bpr_opt_text_20230110`) with distinct data to easily identify which one was used.
    3.  **Other Source Data**: Populate `project.dataset.sof_ta_l_bpr_optionen_filter` with data that would join with both dynamic tables.
*   **Action**:
    1.  Execute the stored procedure with valid parameters:
        ```sql
        CALL `project.dataset.sp_k_ausd_bp_ta_tarifoption`(
          'TEST_JOB',
          '1',
          '01012023', -- Stichtag doesn't directly influence v_datum_sql, but is required
          0
        );
        ```
    2.  Query the intermediate table `project.dataset.sof_ta_bpr_opt_filter`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `sof_ta_bpr_opt_filter` table contains data that *only* originated from the `project.dataset.sof_ta_bpr_opt_text_20230110` table (corresponding to the `MAX(timecreated)` from `dwtk_meldungen`).
    *   **Fail**: Data from an incorrect `sof_ta_bpr_opt_text_YYYYMMDD` table is present, or the table could not be found.

*   **Runnable Test Code (SQL Assertion)**:

    ```sql
    -- Expected v_datum_sql based on setup
    DECLARE expected_v_datum_sql STRING DEFAULT '20230110'; -- Based on MAX(timecreated) in dwtk_meldungen

    -- Check if the data in sof_ta_bpr_opt_filter matches the expected dynamic table
    SELECT
      CASE
        WHEN (
          SELECT COUNT(*)
          FROM `project.dataset.sof_ta_bpr_opt_filter` AS f
          JOIN `project.dataset.sof_ta_bpr_opt_text_20230110` AS expected_t
            ON f.bpr_id = expected_t.bpr_id AND f.cntrct_id = expected_t.cntrct_id -- Assuming these are join keys
          WHERE f.pds_description = expected_t.pds_description -- Add more columns for robust check
        ) = (SELECT COUNT(*) FROM `project.dataset.sof_ta_bpr_opt_filter`)
        AND (
          SELECT COUNT(*)
          FROM `project.dataset.sof_ta_bpr_opt_filter` AS f
          LEFT JOIN `project.dataset.sof_ta_bpr_opt_text_20230105` AS unexpected_t -- An incorrect dynamic table
            ON f.bpr_id = unexpected_t.bpr_id
          WHERE unexpected_t.bpr_id IS NOT NULL
        ) = 0 -- Ensure no data from the incorrect table
        THEN 'PASS: Dynamic table resolved correctly'
        ELSE 'FAIL: Dynamic table resolution incorrect'
      END AS dynamic_table_resolution_check;
    ```

### Test 3: Transformation Correctness - `LEAD` Analytic Function

*   **Purpose**: To verify that the `LEAD` analytic function, with its new deterministic `ORDER BY cntrct_id, pds_description`, produces the correct `lagi` values, especially at `cntrct_id` boundaries.
*   **Setup**:
    1.  **Source Data**: Populate `project.dataset.sof_ta_l_bpr_optionen_filter` and `project.dataset.sof_ta_bpr_opt_text_YYYYMMDD` with data that includes:
        *   Multiple rows for the same `cntrct_id` with varying `pds_description` values (to test `ORDER BY pds_description`).
        *   Rows where `cntrct_id` changes (to test `LEAD` across partitions).
        *   The last row in the dataset (to test `LEAD` default value `-1`).
        *   Data that would result in `opt_kategorie` values like 'BUDGET', 'SONST', 'GPRS', and others.
*   **Action**:
    1.  Execute the stored procedure.
    2.  Query the internal logic that calculates `lagi` before the final `WHERE` clause. This might require temporarily modifying the stored procedure to output this intermediate result or inspecting the `sof_ta_tarifoption` table and inferring `lagi` from `cntrct_id` and the filter.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `lagi` column (or its logical equivalent in the final output) correctly reflects the `cntrct_id` of the *next* row, ordered by `cntrct_id` then `pds_description`, and defaults to `-1` for the last row. The final `sof_ta_tarifoption` table contains only rows where `lagi > cntrct_id` or `lagi = -1`.
    *   **Fail**: Incorrect `lagi` values, or the final filter (`WHERE lagi > cntrct_id OR lagi = -1`) excludes/includes incorrect rows.

*   **Runnable Test Code (SQL Assertion)**:

    ```sql
    -- This test requires inspecting the intermediate logic.
    -- For a black-box test, we can infer the LEAD function's effect.
    -- Assuming we can access the intermediate result before the final filter, or
    -- we can recreate the logic for verification.

    -- Example: Recreate the LEAD logic on the intermediate table
    WITH IntermediateData AS (
      SELECT
        bpr_opt.cntrct_id,
        bpr_opt.pds_description,
        LEAD(bpr_opt.cntrct_id, 1, -1) OVER (ORDER BY bpr_opt.cntrct_id, bpr_opt.pds_description) AS calculated_lagi
      FROM (
        SELECT
          bpr_id,
          cntrct_id,
          pds_description,
          opt_kategorie
        FROM `project.dataset.sof_ta_bpr_opt_filter`
        ORDER BY cntrct_id, pds_description
      ) AS bpr_opt
    )
    -- Now compare the rows that *should* be in sof_ta_tarifoption based on this logic
    -- with the actual rows in sof_ta_tarifoption.
    SELECT
      CASE
        WHEN (
          SELECT COUNT(*) FROM `project.dataset.sof_ta_tarifoption`
        ) = (
          SELECT COUNT(DISTINCT cntrct_id) -- Assuming one row per cntrct_id in final output
          FROM IntermediateData
          WHERE calculated_lagi > cntrct_id OR calculated_lagi = -1
        )
        THEN 'PASS: LEAD function and filter logic correct (row count match)'
        ELSE 'FAIL: LEAD function or filter logic incorrect'
      END AS lead_function_check;

    -- Further detailed checks would involve comparing individual cntrct_id values
    -- and their corresponding concatenated options against expected values.
    ```

### Test 4: Transformation Correctness - Custom Concatenation UDFs (`concatX`)

*   **Purpose**: To thoroughly test the BigQuery UDFs (`concat1`, `concat1r`, `concat2`, `concat2r`, `concat3`, `concat3r`) to ensure they replicate the exact behavior of the legacy Oracle `sof$ab_con.concatX` functions, including edge cases.
*   **Setup**:
    1.  **UDF Definition**: Ensure the BigQuery UDFs are deployed.
    2.  **Test Data**: Create a dedicated test table with various combinations of `pds_description` and `cntrct_id` values, specifically designed to test:
        *   Single `pds_description` for a `cntrct_id`.
        *   Multiple `pds_description` values for a `cntrct_id`.
        *   `pds_description` values containing commas, leading/trailing spaces.
        *   `NULL` or empty `pds_description` values.
        *   Very long `pds_description` values (to test potential length limits, though `SUBSTR(..., 1, 500)` handles this in the final step).
        *   `cntrct_id` values that are `NULL` or zero (if applicable).
    3.  **Expected Output**: For each test case, manually determine the expected output of each `concatX` function based on the *known* legacy logic (this is the critical missing piece, requiring reverse engineering or consultation with legacy experts).
*   **Action**:
    1.  Execute direct SQL queries calling each UDF with the test data.
        ```sql
        SELECT
          test_id,
          pds_description,
          cntrct_id,
          `project.dataset.concat1`(pds_description, cntrct_id) AS result_concat1,
          `project.dataset.concat1r`(pds_description, cntrct_id) AS result_concat1r,
          -- ... and so on for all concatX UDFs
        FROM `project.dataset.test_concat_data`;
        ```
    2.  Alternatively, run the full stored procedure with source data that triggers all `CASE WHEN` branches for `opt_kategorie` and then query `sof_ta_tarifoption`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The output of each `concatX` UDF for all test cases exactly matches the pre-calculated expected legacy output.
    *   **Fail**: Any discrepancy in the concatenated strings.

*   **Runnable Test Code (SQL Assertion - Example for `concat1`)**:

    ```sql
    -- Assuming project.dataset.test_concat_data has columns:
    -- test_id, pds_description, cntrct_id, expected_concat1_output
    SELECT
      CASE
        WHEN COUNTIF(
          `project.dataset.concat1`(t.pds_description, t.cntrct_id) IS NOT DISTINCT FROM t.expected_concat1_output
        ) = COUNT(*)
        THEN 'PASS: concat1 UDF behaves as expected'
        ELSE 'FAIL: concat1 UDF discrepancy found'
      END AS concat1_udf_check,
      ARRAY_AGG(
        STRUCT(
          t.test_id,
          t.pds_description,
          t.cntrct_id,
          `project.dataset.concat1`(t.pds_description, t.cntrct_id) AS actual_output,
          t.expected_concat1_output
        )
        ORDER BY t.test_id
      ) AS discrepancies
    FROM `project.dataset.test_concat_data` AS t
    WHERE NOT (
      `project.dataset.concat1`(t.pds_description, t.cntrct_id) IS NOT DISTINCT FROM t.expected_concat1_output
    );

    -- Repeat similar queries for concat1r, concat2, concat2r, concat3, concat3r.
    ```

### Test 5: Transformation Correctness - String Manipulation and `CASE WHEN` Logic

*   **Purpose**: To verify the `RTRIM(SUBSTR(LTRIM(..., ', '), 1, 500))` logic and the `CASE WHEN` statements that select the appropriate `concatX` UDF based on `opt_kategorie`.
*   **Setup**:
    1.  **Source Data**: Populate `project.dataset.sof_ta_l_bpr_optionen_filter` and `project.dataset.sof_ta_bpr_opt_text_YYYYMMDD` with data that:
        *   Includes `pds_description` values with leading/trailing spaces, leading commas, and lengths exceeding 500 characters.
        *   Includes rows with `opt_kategorie` values 'BUDGET', 'SONST', 'GPRS', and other categories.
        *   Includes `NULL` values for `pds_description` and `opt_kategorie`.
    2.  **Expected Output**: For each test case, manually calculate the expected `business_option`, `sonstige_option`, `gprs_option` values based on the expected UDF output and the string manipulation.
*   **Action**:
    1.  Execute the stored procedure.
    2.  Query the final `project.dataset.sof_ta_tarifoption` table.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `business_option`, `sonstige_option`, `gprs_option` columns in `sof_ta_tarifoption` correctly reflect the application of the `CASE WHEN` logic, the chosen `concatX` UDF, and the `RTRIM(SUBSTR(LTRIM(..., ', '), 1, 500))` string manipulation.
    *   **Fail**: Any discrepancy in the final output strings.

*   **Runnable Test Code (SQL Assertion)**:

    ```sql
    -- This test is best covered by Test 1 (Full Data Parity) if the test data is comprehensive.
    -- However, for isolated testing of this specific logic:

    -- Assuming a test table 'project.dataset.test_string_manipulation' with:
    -- cntrct_id, pds_description_raw, opt_kategorie, expected_business_option, expected_sonstige_option, expected_gprs_option

    WITH MigratedOutput AS (
      -- Replicate the core logic for a specific cntrct_id and its options
      -- This would be a simplified version of the stored procedure's inner query
      SELECT
        t.cntrct_id,
        RTRIM(SUBSTR(LTRIM(
          CASE
            WHEN l.opt_kategorie = 'BUDGET' THEN `project.dataset.concat1`(t.pds_description, t.cntrct_id)
            ELSE `project.dataset.concat1r`(t.pds_description, t.cntrct_id)
          END, ', '), 1, 500)) AS actual_business_option,
        RTRIM(SUBSTR(LTRIM(
          CASE
            WHEN l.opt_kategorie = 'SONST' THEN `project.dataset.concat2`(t.pds_description, t.cntrct_id)
            ELSE `project.dataset.concat2r`(t.pds_description, t.cntrct_id)
          END, ', '), 1, 500)) AS actual_sonstige_option,
        RTRIM(SUBSTR(LTRIM(
          CASE
            WHEN l.opt_kategorie = 'GPRS' THEN `project.dataset.concat3`(t.pds_description, t.cntrct_id)
            ELSE `project.dataset.concat3r`(t.pds_description, t.cntrct_id)
          END, ', '), 1, 500)) AS actual_gprs_option
      FROM `project.dataset.sof_ta_l_bpr_optionen_filter` AS l
      JOIN `project.dataset.sof_ta_bpr_opt_text_YYYYMMDD` AS t -- Use the dynamically resolved table
        ON t.bpr_id = l.bpr_id
      -- Add a WHERE clause to filter for specific test cases if needed
    )
    SELECT
      CASE
        WHEN COUNTIF(
          m.actual_business_option IS NOT DISTINCT FROM e.expected_business_option AND
          m.actual_sonstige_option IS NOT DISTINCT FROM e.expected_sonstige_option AND
          m.actual_gprs_option IS NOT DISTINCT FROM e.expected_gprs_option
        ) = COUNT(*)
        THEN 'PASS: String manipulation and CASE WHEN logic correct'
        ELSE 'FAIL: String manipulation or CASE WHEN logic discrepancy found'
      END AS string_logic_check,
      ARRAY_AGG(
        STRUCT(
          m.cntrct_id,
          m.actual_business_option, e.expected_business_option,
          m.actual_sonstige_option, e.expected_sonstige_option,
          m.actual_gprs_option, e.expected_gprs_option
        )
        ORDER BY m.cntrct_id
      ) AS discrepancies
    FROM MigratedOutput AS m
    JOIN `project.dataset.test_string_manipulation` AS e
      ON m.cntrct_id = e.cntrct_id
    WHERE NOT (
      m.actual_business_option IS NOT DISTINCT FROM e.expected_business_option AND
      m.actual_sonstige_option IS NOT DISTINCT FROM e.expected_sonstige_option AND
      m.actual_gprs_option IS NOT DISTINCT FROM e.expected_gprs_option
    );
    ```

### Test 6: External System Replacement - `job_audit_log` Logging

*   **Purpose**: To verify that the BigQuery `job_audit_log` table correctly captures the job's execution status, parameters, and messages, replacing the legacy custom logging.
*   **Setup**:
    1.  **`job_audit_log` Table**: Ensure `project.dataset.job_audit_log` table exists with the expected schema (`log_time`, `job_kennung`, `entry_nr`, `message_type`, `message`, `stichtag`, `status`).
    2.  **Test Scenarios**: Prepare for both successful and failed job executions.
*   **Action**:
    1.  **Successful Run**: Execute the stored procedure with valid parameters and data that leads to a successful completion.
        ```sql
        CALL `project.dataset.sp_k_ausd_bp_ta_tarifoption`('LOG_TEST_SUCCESS', '100', '01012023', 0);
        ```
    2.  **Failed Run (Parameter Error)**: Execute with an invalid `p_stichtag`.
        ```sql
        CALL `project.dataset.sp_k_ausd_bp_ta_tarifoption`('LOG_TEST_FAIL_PARAM', '101', 'INVALID_DATE', 0);
        ```
    3.  **Failed Run (SQL Error)**: Execute with data that would cause a SQL error (e.g., missing required source table, or data type mismatch if possible). This might require temporarily altering source data or the procedure for testing.
        ```sql
        -- Example: Simulate a missing dynamic table by ensuring no matching entry in dwtk_meldungen
        -- Or, temporarily drop a required source table before calling.
        CALL `project.dataset.sp_k_ausd_bp_ta_tarifoption`('LOG_TEST_FAIL_SQL', '102', '01012023', 0);
        ```
    4.  Query the `project.dataset.job_audit_log` table.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   For the successful run: `job_audit_log` contains entries for 'Starting', 'Parameters validated successfully', 'SQL script executed successfully', and a final 'SUCCESS' status, with correct `job_kennung`, `entry_nr`, `stichtag`, and `v_records` count.
        *   For the failed runs: `job_audit_log` contains entries for 'Starting', then an 'ERROR' message detailing the failure (e.g., 'Invalid Stichtag format', 'Error during SQL script execution'), and a final 'FAILED' status.
    *   **Fail**: Missing log entries, incorrect status, or inaccurate error messages.

*   **Runnable Test Code (SQL Assertion)**:

    ```sql
    -- After a successful run ('LOG_TEST_SUCCESS', '100', '01012023', 0)
    SELECT
      CASE
        WHEN (SELECT COUNT(*) FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_SUCCESS' AND status = 'SUCCESS') = 1
        AND (SELECT COUNT(*) FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_SUCCESS' AND message LIKE '%Starting%' AND status = 'RUNNING') = 1
        AND (SELECT COUNT(*) FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_SUCCESS' AND message LIKE '%Parameters validated successfully%' AND status = 'RUNNING') = 1
        AND (SELECT COUNT(*) FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_SUCCESS' AND message LIKE '%SQL script executed successfully%' AND status = 'SUCCESS') = 1
        AND (SELECT stichtag FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_SUCCESS' AND status = 'SUCCESS' LIMIT 1) = PARSE_DATE('%d%m%Y', '01012023')
        THEN 'PASS: Successful run logged correctly'
        ELSE 'FAIL: Successful run logging incorrect'
      END AS success_log_check;

    -- After a failed run ('LOG_TEST_FAIL_PARAM', '101', 'INVALID_DATE', 0)
    SELECT
      CASE
        WHEN (SELECT COUNT(*) FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_FAIL_PARAM' AND status = 'FAILED') = 1
        AND (SELECT message FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_FAIL_PARAM' AND status = 'FAILED' LIMIT 1) LIKE '%Invalid Stichtag format%'
        THEN 'PASS: Parameter validation failure logged correctly'
        ELSE 'FAIL: Parameter validation failure logging incorrect'
      END AS param_fail_log_check;

    -- After a failed run ('LOG_TEST_FAIL_SQL', '102', '01012023', 0)
    SELECT
      CASE
        WHEN (SELECT COUNT(*) FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_FAIL_SQL' AND status = 'FAILED') = 1
        AND (SELECT message FROM `project.dataset.job_audit_log` WHERE job_kennung = 'LOG_TEST_FAIL_SQL' AND status = 'FAILED' LIMIT 1) LIKE '%Error during SQL script execution%'
        THEN 'PASS: SQL execution failure logged correctly'
        ELSE 'FAIL: SQL execution failure logging incorrect'
      END AS sql_fail_log_check;
    ```

### Test 7: Data Quality - Row Count Parity and Schema Assertions

*   **Purpose**: To ensure that the migrated job consistently produces the same number of rows in its output tables and that the schema (column names, data types) of the final output table matches the legacy system.
*   **Setup**:
    1.  **Legacy Data Snapshot**: Obtain row counts and schema details for `sof$ta_bpr_opt_filter` and `sof$ta_tarifoption` from a legacy run.
    2.  **BigQuery Source Data**: Load representative data into BigQuery source tables.
*   **Action**:
    1.  Execute the stored procedure.
    2.  Query the row counts of `project.dataset.sof_ta_bpr_opt_filter` and `project.dataset.sof_ta_tarifoption`.
    3.  Inspect the schema of `project.dataset.sof_ta_tarifoption` using BigQuery's `INFORMATION_SCHEMA`.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The row count of `project.dataset.sof_ta_bpr_opt_filter` matches the legacy `sof$ta_bpr_opt_filter`.
        *   The row count of `project.dataset.sof_ta_tarifoption` matches the legacy `sof$ta_tarifoption`.
        *   The schema (column names, data types, nullability) of `project.dataset.sof_ta_tarifoption` is identical to the legacy `sof$ta_tarifoption`.
    *   **Fail**: Any discrepancy in row counts or schema.

*   **Runnable Test Code (SQL Assertion)**:

    ```sql
    -- Assuming legacy_sof_ta_bpr_opt_filter_snapshot and legacy_sof_ta_tarifoption_snapshot
    -- are BigQuery tables containing row counts from legacy.

    -- Row Count Parity for intermediate table
    SELECT
      CASE
        WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_bpr_opt_filter`) = (SELECT COUNT(*) FROM `LEGACY_STAGING_DB.sof_ta_bpr_opt_filter_snapshot`)
        THEN 'PASS: Intermediate table row count matches legacy'
        ELSE 'FAIL: Intermediate table row count differs'
      END AS intermediate_row_count_check;

    -- Row Count Parity for final table
    SELECT
      CASE
        WHEN (SELECT COUNT(*) FROM `project.dataset.sof_ta_tarifoption`) = (SELECT COUNT(*) FROM `LEGACY_STAGING_DB.sof_ta_tarifoption_snapshot`)
        THEN 'PASS: Final table row count matches legacy'
        ELSE 'FAIL: Final table row count differs'
      END AS final_row_count_check;

    -- Schema Assertion for final table
    -- This requires comparing against a predefined expected schema (from legacy)
    -- Example: Check column names and types
    SELECT
      CASE
        WHEN (
          SELECT COUNT(*)
          FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
          WHERE table_name = 'sof_ta_tarifoption'
            AND (
              (column_name = 'cntrct_id' AND data_type = 'INT64') OR
              (column_name = 'business_option' AND data_type = 'STRING') OR
              (column_name = 'sonstige_option' AND data_type = 'STRING') OR
              (column_name = 'gprs_option' AND data_type = 'STRING')
            )
        ) = 4 -- Number of expected columns
        THEN 'PASS: Final table schema matches expected'
        ELSE 'FAIL: Final table schema mismatch'
      END AS schema_check;

    -- More detailed schema check (e.g., nullability, exact order if important)
    -- This would involve querying INFORMATION_SCHEMA and comparing against a JSON/YAML representation
    -- of the expected schema.
    ```

### Test 8: Edge Case - `p_stichtag` Defaulting and Date Validation

*   **Purpose**: To verify that `p_stichtag` is correctly validated and that the `v_stichtag_date` variable is set correctly, and that the `v_datum_heute` and `v_datum_gestern` variables are derived as expected.
*   **Setup**:
    1.  No specific data setup beyond ensuring `dwtk_meldungen` is populated enough to allow the job to run.
*   **Action**:
    1.  **Valid `p_stichtag`**: Call with a valid date, e.g., `'15032024'`.
    2.  **Invalid `p_stichtag` (Format)**: Call with `'2024-03-15'` or `'15/03/2024'`.
    3.  **Invalid `p_stichtag` (Value)**: Call with `'32012024'` (invalid day).
    4.  **`p_stichtag` NULL/Empty**: Call with `NULL` or `''`.
    5.  Observe the `job_audit_log` for parameter validation messages and the overall job status.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   Valid `p_stichtag` leads to successful execution and `v_stichtag_date` correctly parsed.
        *   Invalid `p_stichtag` (format or value) leads to a 'FAILED' status in `job_audit_log` with an appropriate error message (`Invalid Stichtag format or value`).
        *   `p_stichtag` NULL/Empty leads to a 'FAILED' status with 'Stichtag parameter is required.'
        *   `v_datum_heute` and `v_datum_gestern` are correctly derived as `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` respectively. (This can be verified by inspecting the `job_audit_log` if the procedure logs these values, or by running a separate query for the current date).
    *   **Fail**: Incorrect validation, unexpected job status, or incorrect date derivations.

*   **Runnable Test Code (SQL Assertion)**:

    ```sql
    -- See Test 6 for logging assertions for parameter validation.
    -- To verify v_datum_heute and v_datum_gestern, if they are not logged,
    -- a separate test would be needed or a temporary modification to the SP.
    -- Assuming they are logged or can be inferred:

    -- Example: Verify v_datum_heute and v_datum_gestern (if logged)
    SELECT
      CASE
        WHEN (SELECT message FROM `project.dataset.job_audit_log` WHERE job_kennung = 'SOME_JOB_ID' AND message LIKE '%v_datum_heute:%' LIMIT 1) LIKE FORMAT('%%v_datum_heute: %s%%', FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()))
        AND (SELECT message FROM `project.dataset.job_audit_log` WHERE job_kennung = 'SOME_JOB_ID' AND message LIKE '%v_datum_gestern:%' LIMIT 1) LIKE FORMAT('%%v_datum_gestern: %s%%', FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
        THEN 'PASS: Date derivations correct'
        ELSE 'FAIL: Date derivations incorrect'
      END AS date_derivation_check;
    ```