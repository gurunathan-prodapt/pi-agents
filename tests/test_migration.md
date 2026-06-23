As a senior data-migration QA engineer, I've designed a suite of migration validation tests for the `r_ausd_v_ta_notice.ksh` job. These tests aim to ensure behavioral equivalence between the legacy KornShell/Oracle implementation and the new BigQuery stored procedure solution.

The tests are categorized to cover output parity, transformation correctness, external system replacements, and data quality assertions, as outlined in the migration design document.

---

## Test Setup Prerequisites

Before executing any test cases, the following setup steps are required:

1.  **BigQuery Environment:**
    *   Ensure the target BigQuery project and dataset (`my_project.my_dataset` in these examples) exist.
    *   Deploy the DDL for all target tables:
        *   `my_project.my_dataset.sof_ta_notice`
        *   `my_project.my_dataset.cds_ta_notice`
        *   `my_project.my_dataset.dwtk_meldungen`
        *   `my_project.my_dataset.job_log`
    *   Deploy the BigQuery stored procedures:
        *   `my_project.my_dataset.r_ausd_v_ta_notice`
        *   `my_project.my_dataset.k_ausd_v_ta_notice`
2.  **Legacy Environment:**
    *   Access to the legacy Oracle database and the ability to execute the original KornShell scripts.
    *   A mechanism to capture the final state of Oracle tables (`sof$ta_notice`, `isbert_schema.dwtk_meldungen`) and log files after legacy job execution for comparison.
3.  **Test Data Management:**
    *   A controlled process to populate `my_project.my_dataset.cds_ta_notice` and `my_project.my_dataset.dwtk_meldungen` with specific test data, mirroring the Oracle source.
    *   A method to clear `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` before each test run, unless otherwise specified.
    *   For output parity tests, a method to load the legacy Oracle `sof$ta_notice` data into a temporary BigQuery table (e.g., `my_project.my_dataset.legacy_sof_ta_notice_snapshot`) for direct comparison.

---

## Test Cases

### Test Case 1: End-to-End Output Parity - Standard Run

*   **Purpose:** Verify that the migrated BigQuery job produces the exact same final data in `sof_ta_notice` as the legacy KornShell/Oracle job under typical operating conditions. This is the primary behavioral equivalence test.
*   **Setup:**
    1.  **Legacy:** Populate Oracle `cds$ta_notice` with a diverse set of test data (including records that should and should not be selected, and various NULL combinations for `modified_at` and `valid_to`). Populate Oracle `isbert_schema.dwtk_meldungen` with an entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` and a `timecreated` value (e.g., '2023-01-15 10:00:00 UTC') to define a specific cutoff date. Ensure `sof$ta_notice` is empty.
    2.  **Migrated:** Populate `my_project.my_dataset.cds_ta_notice` and `my_project.my_dataset.dwtk_meldungen` with data identical to their Oracle counterparts. Ensure `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` are empty.
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_v_ta_notice.ksh`.
    2.  Execute the migrated BigQuery stored procedure: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_JOB_STANDARD', 10001);`
*   **Pass/Fail Criterion:**
    1.  The row count in Oracle `sof$ta_notice` must be identical to the row count in BigQuery `my_project.my_dataset.sof_ta_notice`.
    2.  A full data comparison between Oracle `sof$ta_notice` (after loading into `my_project.my_dataset.legacy_sof_ta_notice_snapshot`) and BigQuery `my_project.my_dataset.sof_ta_notice` must yield no differences.
    3.  The `my_project.my_dataset.job_log` table should contain entries indicating successful execution, including the correct record count.

    ```sql
    -- Assuming Oracle data is loaded into a temporary BQ table named `my_project.my_dataset.legacy_sof_ta_notice_snapshot`
    -- 1. Row Count Comparison
    SELECT
        (SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_notice`) AS bq_count,
        (SELECT COUNT(*) FROM `my_project.my_dataset.legacy_sof_ta_notice_snapshot`) AS legacy_count;
    -- Expected: bq_count = legacy_count

    -- 2. Data Content Comparison
    SELECT 'Only in BQ' AS source, * FROM `my_project.my_dataset.sof_ta_notice`
    EXCEPT DISTINCT
    SELECT 'Only in Legacy' AS source, * FROM `my_project.my_dataset.legacy_sof_ta_notice_snapshot`
    UNION ALL
    SELECT 'Only in Legacy' AS source, * FROM `my_project.my_dataset.legacy_sof_ta_notice_snapshot`
    EXCEPT DISTINCT
    SELECT 'Only in BQ' AS source, * FROM `my_project.my_dataset.sof_ta_notice`;
    -- Expected result: 0 rows for the UNION ALL query.

    -- 3. Log Verification
    SELECT log_level, message FROM `my_project.my_dataset.job_log`
    WHERE job_kennung = 'TEST_JOB_STANDARD' AND eintrags_nr = 10001
    ORDER BY created_at DESC;
    -- Expected: At least one 'INFO' entry with 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    -- and one 'INFO' entry from 'k_ausd_v_ta_notice' with 'ENDE Datenverarbeitung. Records=X'.
    ```

### Test Case 2: Cutoff Date (`v_datum`) Determination - `dwtk_meldungen` Scenarios

*   **Purpose:** Verify that the `v_datum` variable is correctly determined by the `r_ausd_v_ta_notice` stored procedure under various `dwtk_meldungen` conditions, matching the legacy logic.
*   **Setup:**
    *   For each scenario, ensure `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` are empty.
    *   Populate `my_project.my_dataset.cds_ta_notice` with a few records that would be affected by the `v_datum` value (e.g., `insert_at` dates just before/after expected `v_datum`).
    1.  **Scenario A: Multiple `BERT_DROP_TEMP_TABLE` entries.**
        *   Populate `my_project.my_dataset.dwtk_meldungen` with:
            *   `('BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00 UTC')`
            *   `('OTHER_JOB', '2023-01-05 12:00:00 UTC')`
            *   `('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC')`
            *   `('BERT_DROP_TEMP_TABLE', '2023-01-10 08:00:00 UTC')`
        *   Expected `v_datum`: '20230115'.
    2.  **Scenario B: No `BERT_DROP_TEMP_TABLE` entries.**
        *   Populate `my_project.my_dataset.dwtk_meldungen` with:
            *   `('OTHER_JOB', '2023-01-05 12:00:00 UTC')`
        *   Expected `v_datum`: '19000101'.
*   **Action:**
    1.  For Scenario A: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_DATUM_MULTI', 10002);`
    2.  For Scenario B: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_DATUM_NONE', 10003);`
*   **Pass/Fail Criterion:**
    1.  For each scenario, the data in `my_project.my_dataset.sof_ta_notice` must precisely reflect the filtering logic using the *expected* `v_datum` for that scenario. This can be verified by comparing `sof_ta_notice` against a pre-calculated `expected_sof_ta_notice` table for each `v_datum`.
    2.  The `job_log` entry from `k_ausd_v_ta_notice` should show the correct record count corresponding to the expected `v_datum`.

    ```sql
    -- Example for Scenario A (after running the SP)
    -- Create an expected table based on the expected v_datum ('20230115')
    CREATE OR REPLACE TABLE `my_project.my_dataset.expected_sof_ta_notice_s_a` AS
    SELECT
      n.cntrct_id, n.valid_from, n.valid_to, n.entry_date_of_notice
    FROM `my_project.my_dataset.cds_ta_notice` n
    WHERE n.insert_at <= PARSE_DATE('%Y%m%d', '20230115')
      AND (n.modified_at IS NULL OR n.modified_at > PARSE_DATE('%Y%m%d', '20230115'))
      AND (n.valid_to IS NULL OR n.valid_to > PARSE_DATE('%Y%m%d', '20230115'))
      AND n.is_production = 1;

    -- Compare actual output with expected output for Scenario A
    SELECT 'Only in Actual' AS source, * FROM `my_project.my_dataset.sof_ta_notice`
    EXCEPT DISTINCT
    SELECT 'Only in Expected' AS source, * FROM `my_project.my_dataset.expected_sof_ta_notice_s_a`
    UNION ALL
    SELECT 'Only in Expected' AS source, * FROM `my_project.my_dataset.expected_sof_ta_notice_s_a`
    EXCEPT DISTINCT
    SELECT 'Only in Actual' AS source, * FROM `my_project.my_dataset.sof_ta_notice`;
    -- Expected result: 0 rows. (Repeat for Scenario B with '19000101')
    ```

### Test Case 3: Transformation Logic - `WHERE` Clause Correctness (Date and NULL Handling)

*   **Purpose:** Verify that the `INSERT INTO ... SELECT` statement in `k_ausd_v_ta_notice` correctly filters records based on `insert_at`, `modified_at`, `valid_to`, and `is_production`, including boundary conditions and NULL values.
*   **Setup:**
    1.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` to a specific date, e.g., '20230101'.
    2.  Populate `my_project.my_dataset.cds_ta_notice` with a comprehensive set of test data covering:
        *   `insert_at` equal to, before, and after `v_datum`.
        *   `modified_at` IS NULL, equal to, before, and after `v_datum`.
        *   `valid_to` IS NULL, equal to, before, and after `v_datum`.
        *   `is_production = 1` and `is_production = 0`.
        *   Various combinations of these conditions.
    3.  Create a "truth" table (`my_project.my_dataset.expected_sof_ta_notice_transform`) with the records that *should* be inserted based on the expected logic.
    4.  Ensure `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` are empty.
*   **Action:**
    1.  Execute the migrated BigQuery stored procedure: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_TRANSFORM', 10004);`
*   **Pass/Fail Criterion:**
    1.  The data in `my_project.my_dataset.sof_ta_notice` must be identical to the data in `my_project.my_dataset.expected_sof_ta_notice_transform`.
    2.  The logged record count in `job_log` must match the count in `my_project.my_dataset.expected_sof_ta_notice_transform`.

    ```sql
    -- SQL for Pass/Fail Criterion
    SELECT
        (SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_notice`) AS actual_count,
        (SELECT COUNT(*) FROM `my_project.my_dataset.expected_sof_ta_notice_transform`) AS expected_count;
    -- Expected: actual_count = expected_count

    SELECT 'Only in Actual' AS source, * FROM `my_project.my_dataset.sof_ta_notice`
    EXCEPT DISTINCT
    SELECT 'Only in Expected' AS source, * FROM `my_project.my_dataset.expected_sof_ta_notice_transform`
    UNION ALL
    SELECT 'Only in Expected' AS source, * FROM `my_project.my_dataset.expected_sof_ta_notice_transform`
    EXCEPT DISTINCT
    SELECT 'Only in Actual' AS source, * FROM `my_project.my_dataset.sof_ta_notice`;
    -- Expected result: 0 rows for the UNION ALL query.
    ```

### Test Case 4: Schema and Data Type Handling

*   **Purpose:** Verify that the schema of `sof_ta_notice` is correct and that data types are handled without loss of precision or unexpected conversion errors during the `INSERT`.
*   **Setup:**
    1.  Ensure `my_project.my_dataset.cds_ta_notice` contains data that tests various date values (including NULLs) and representative `cntrct_id` values.
    2.  The DDL for `my_project.my_dataset.sof_ta_notice` should be deployed as specified in the design.
    3.  Ensure `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` are empty.
*   **Action:**
    1.  Execute the migrated BigQuery stored procedure: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_SCHEMA', 10005);`
*   **Pass/Fail Criterion:**
    1.  The schema of `my_project.my_dataset.sof_ta_notice` must match the expected DDL: `cntrct_id STRING`, `valid_from DATE`, `valid_to DATE`, `entry_date_of_notice DATE`.
    2.  No data type conversion errors should occur during the `INSERT` (the procedure should complete successfully).
    3.  Querying `my_project.my_dataset.sof_ta_notice` should show data correctly stored in the defined types, without truncation or malformation.

    ```python
    # Pytest example for schema assertion
    import pytest
    from google.cloud import bigquery

    def test_sof_ta_notice_schema():
        client = bigquery.Client()
        table_id = "my_project.my_dataset.sof_ta_notice"
        table = client.get_table(table_id)

        expected_schema = {
            "cntrct_id": "STRING",
            "valid_from": "DATE",
            "valid_to": "DATE",
            "entry_date_of_notice": "DATE",
        }

        actual_schema = {field.name: field.field_type for field in table.schema}

        assert actual_schema == expected_schema, f"Schema mismatch for {table_id}"

    # SQL to check for any unexpected data issues (e.g., if a date was inserted as a string)
    SELECT
        COUNT(*)
    FROM `my_project.my_dataset.sof_ta_notice`
    WHERE
        NOT (valid_from IS NULL OR SAFE.PARSE_DATE('%Y-%m-%d', CAST(valid_from AS STRING)) IS NOT NULL)
        OR NOT (valid_to IS NULL OR SAFE.PARSE_DATE('%Y-%m-%d', CAST(valid_to AS STRING)) IS NOT NULL)
        OR NOT (entry_date_of_notice IS NULL OR SAFE.PARSE_DATE('%Y-%m-%d', CAST(entry_date_of_notice AS STRING)) IS NOT NULL);
    -- Expected result: 0 rows.
    ```

### Test Case 5: Error Handling - Missing Parameters

*   **Purpose:** Verify that the `k_ausd_v_ta_notice` stored procedure correctly handles missing required parameters (`p_job_kennung`, `p_eintrags_nr`) and logs the error.
*   **Setup:**
    1.  Ensure `my_project.my_dataset.job_log` is empty.
*   **Action:**
    1.  Attempt to call `r_ausd_v_ta_notice` with a NULL `p_job_kennung`: `CALL my_project.my_dataset.r_ausd_v_ta_notice(NULL, 10006);`
    2.  Attempt to call `r_ausd_v_ta_notice` with a NULL `p_eintrags_nr`: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_ERROR_JOB', NULL);`
*   **Pass/Fail Criterion:**
    1.  For each action, the BigQuery stored procedure call must fail with an error message indicating the missing parameter (e.g., "FEHLER: Jobkennung fehlt" or "FEHLER: EintragsNr fehlt").
    2.  The `my_project.my_dataset.job_log` table must contain an `ERROR` level entry for `r_ausd_v_ta_notice` with a message "Abbruch wegen Fehler".

    ```python
    # Pytest example for error handling
    import pytest
    from google.cloud import bigquery

    def test_missing_job_kennung_error():
        client = bigquery.Client()
        # Clear logs for this specific test run
        client.query("DELETE FROM `my_project.my_dataset.job_log` WHERE eintrags_nr = 10006;").result()

        with pytest.raises(Exception) as excinfo:
            client.query("CALL `my_project.my_dataset.r_ausd_v_ta_notice`(NULL, 10006);").result()
        assert "FEHLER: Jobkennung fehlt" in str(excinfo.value)

        # Verify log entry
        query_job = client.query("""
            SELECT message, log_level FROM `my_project.my_dataset.job_log`
            WHERE eintrags_nr = 10006 AND log_level = 'ERROR'
            ORDER BY created_at DESC LIMIT 1
        """)
        results = query_job.result()
        assert len(list(results)) == 1
        log_entry = list(results)[0]
        assert "Abbruch wegen Fehler" in log_entry.message
        assert log_entry.log_level == 'ERROR'

    def test_missing_eintrags_nr_error():
        client = bigquery.Client()
        # Clear logs for this specific test run
        client.query("DELETE FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_ERROR_JOB' AND eintrags_nr IS NULL;").result()

        with pytest.raises(Exception) as excinfo:
            client.query("CALL `my_project.my_dataset.r_ausd_v_ta_notice`('TEST_ERROR_JOB', NULL);").result()
        assert "FEHLER: EintragsNr fehlt" in str(excinfo.value)

        # Verify log entry (note: eintrags_nr will be NULL in the log for this specific call)
        query_job = client.query("""
            SELECT message, log_level FROM `my_project.my_dataset.job_log`
            WHERE job_kennung = 'TEST_ERROR_JOB' AND eintrags_nr IS NULL AND log_level = 'ERROR'
            ORDER BY created_at DESC LIMIT 1
        """)
        results = query_job.result()
        assert len(list(results)) == 1
        log_entry = list(results)[0]
        assert "Abbruch wegen Fehler" in log_entry.message
        assert log_entry.log_level == 'ERROR'
    ```

### Test Case 6: Row Count Logging

*   **Purpose:** Verify that the `k_ausd_v_ta_notice` stored procedure accurately calculates and logs the number of records inserted into `sof_ta_notice`.
*   **Setup:**
    1.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` (e.g., '20230101').
    2.  Populate `my_project.my_dataset.cds_ta_notice` with a known number of records (e.g., 50 records) that *should* be inserted based on the `WHERE` clause.
    3.  Ensure `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` are empty.
*   **Action:**
    1.  Execute the migrated BigQuery stored procedure: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_ROW_COUNT', 10007);`
*   **Pass/Fail Criterion:**
    1.  The actual row count in `my_project.my_dataset.sof_ta_notice` must be 50.
    2.  The `my_project.my_dataset.job_log` table must contain an `INFO` level entry from `k_ausd_v_ta_notice` with a message "ENDE Datenverarbeitung. Records=50".

    ```sql
    -- SQL for Pass/Fail Criterion
    SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_notice`;
    -- Expected result: 50

    SELECT message FROM `my_project.my_dataset.job_log`
    WHERE job_kennung = 'TEST_ROW_COUNT' AND eintrags_nr = 10007 AND script_name = 'k_ausd_v_ta_notice'
    ORDER BY created_at DESC LIMIT 1;
    -- Expected message: 'ENDE Datenverarbeitung. Records=50'
    ```

### Test Case 7: Idempotency (Truncate and Insert)

*   **Purpose:** Verify that running the job multiple times produces the same result, as the `TRUNCATE` statement ensures a clean slate each time.
*   **Setup:**
    1.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` (e.g., '20230101').
    2.  Populate `my_project.my_dataset.cds_ta_notice` with a known set of data.
    3.  Ensure `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` are empty.
*   **Action:**
    1.  Execute the migrated BigQuery stored procedure for the first time: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_IDEMPOTENCY', 10008);`
    2.  Capture the state of `my_project.my_dataset.sof_ta_notice` into a temporary table:
        ```sql
        CREATE OR REPLACE TABLE `my_project.my_dataset.sof_ta_notice_run1` AS
        SELECT * FROM `my_project.my_dataset.sof_ta_notice`;
        ```
    3.  Execute the migrated BigQuery stored procedure for the second time: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_IDEMPOTENCY', 10009);`
*   **Pass/Fail Criterion:**
    1.  The data in `my_project.my_dataset.sof_ta_notice` after the second run must be identical to the data captured in `my_project.my_dataset.sof_ta_notice_run1`.
    2.  The row count in `my_project.my_dataset.sof_ta_notice` must be the same after both runs.

    ```sql
    -- SQL for Pass/Fail Criterion (after both runs and snapshot)
    SELECT 'Only in Run1' AS source, * FROM `my_project.my_dataset.sof_ta_notice_run1`
    EXCEPT DISTINCT
    SELECT 'Only in Run2' AS source, * FROM `my_project.my_dataset.sof_ta_notice`
    UNION ALL
    SELECT 'Only in Run2' AS source, * FROM `my_project.my_dataset.sof_ta_notice`
    EXCEPT DISTINCT
    SELECT 'Only in Run1' AS source, * FROM `my_project.my_dataset.sof_ta_notice_run1`;
    -- Expected result: 0 rows.
    ```

### Test Case 8: Empty Source Table (`cds_ta_notice`)

*   **Purpose:** Verify that the job handles an empty source table gracefully, resulting in an empty target table and correct log entries.
*   **Setup:**
    1.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` (e.g., '20230101').
    2.  Ensure `my_project.my_dataset.cds_ta_notice` is empty.
    3.  Ensure `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` are empty.
*   **Action:**
    1.  Execute the migrated BigQuery stored procedure: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_EMPTY_SOURCE', 10010);`
*   **Pass/Fail Criterion:**
    1.  The `my_project.my_dataset.sof_ta_notice` table must be empty.
    2.  The `my_project.my_dataset.job_log` table must contain an `INFO` level entry from `k_ausd_v_ta_notice` with a message "ENDE Datenverarbeitung. Records=0".

    ```sql
    -- SQL for Pass/Fail Criterion
    SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_notice`;
    -- Expected result: 0

    SELECT message FROM `my_project.my_dataset.job_log`
    WHERE job_kennung = 'TEST_EMPTY_SOURCE' AND eintrags_nr = 10010 AND script_name = 'k_ausd_v_ta_notice'
    ORDER BY created_at DESC LIMIT 1;
    -- Expected message: 'ENDE Datenverarbeitung. Records=0'
    ```

### Test Case 9: Oracle `TO_DATE` vs BigQuery `PARSE_DATE` Equivalence

*   **Purpose:** Explicitly confirm that the date parsing and comparison logic (`TO_DATE('&v_datum','YYYYMMDD')` vs `PARSE_DATE('%Y%m%d', v_datum)`) behaves identically, especially around boundary conditions.
*   **Setup:**
    1.  Set `v_datum` to a specific date string, e.g., '20230101'. Populate `my_project.my_dataset.dwtk_meldungen` accordingly.
    2.  Populate `my_project.my_dataset.cds_ta_notice` with records where `insert_at`, `modified_at`, `valid_to` are:
        *   Exactly `2023-01-01` (boundary).
        *   One day before `2023-01-01`.
        *   One day after `2023-01-01`.
        *   NULL.
        *   Ensure `is_production = 1` for all relevant test records.
    3.  Create a "truth" table (`my_project.my_dataset.expected_sof_ta_notice_date_parse`) based on the *legacy Oracle* interpretation of `TO_DATE` for these boundary conditions.
    4.  Ensure `my_project.my_dataset.sof_ta_notice` and `my_project.my_dataset.job_log` are empty.
*   **Action:**
    1.  Execute the migrated BigQuery stored procedure: `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_DATE_PARSE', 10011);`
*   **Pass/Fail Criterion:**
    1.  The data in `my_project.my_dataset.sof_ta_notice` must be identical to the data in `my_project.my_dataset.expected_sof_ta_notice_date_parse`. This confirms that `PARSE_DATE` correctly replicates `TO_DATE` behavior for the given format and comparisons.

    ```sql
    -- SQL for Pass/Fail Criterion (similar to Test Case 3)
    SELECT 'Only in Actual' AS source, * FROM `my_project.my_dataset.sof_ta_notice`
    EXCEPT DISTINCT
    SELECT 'Only in Expected' AS source, * FROM `my_project.my_dataset.expected_sof_ta_notice_date_parse`
    UNION ALL
    SELECT 'Only in Expected' AS source, * FROM `my_project.my_dataset.expected_sof_ta_notice_date_parse`
    EXCEPT DISTINCT
    SELECT 'Only in Actual' AS source, * FROM `my_project.my_dataset.sof_ta_notice`;
    -- Expected result: 0 rows.
    ```

### Test Case 10: Logging of Job Start/Success/Failure

*   **Purpose:** Verify that the `job_log` table correctly captures the start, successful completion, and error conditions of the `r_ausd_v_ta_notice` procedure.
*   **Setup:**
    1.  Ensure `my_project.my_dataset.job_log` is empty before each scenario.
    2.  **Scenario A (Success):** Populate `dwtk_meldungen` and `cds_ta_notice` for a successful run (e.g., 5 records inserted).
    3.  **Scenario B (Failure):** To simulate a failure within `k_ausd_v_ta_notice`, temporarily modify the `k_ausd_v_ta_notice` stored procedure to include a `RAISE USING MESSAGE = 'Simulated SQL Error';` statement after the `TRUNCATE` but before the `INSERT`. This ensures `r_ausd_v_ta_notice` catches the error.
*   **Action:**
    1.  **Scenario A:** `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_LOG_SUCCESS', 10012);`
    2.  **Scenario B:** (After modifying `k_ausd_v_ta_notice` for failure) `CALL my_project.my_dataset.r_ausd_v_ta_notice('TEST_LOG_FAILURE', 10013);`
*   **Pass/Fail Criterion:**
    1.  **Scenario A:** `job_log` must contain at least three `INFO` entries for `TEST_LOG_SUCCESS` and `eintrags_nr = 10012`:
        *   One from `r_ausd_v_ta_notice` with "Job started".
        *   One from `k_ausd_v_ta_notice` with "ENDE Datenverarbeitung. Records=X" (where X is the number of inserted records).
        *   One from `r_ausd_v_ta_notice` with "Die Abarbeitung wurde ohne erkennbare Fehler beendet".
    2.  **Scenario B:** `job_log` must contain:
        *   An `INFO` entry from `r_ausd_v_ta_notice` with "Job started".
        *   An `ERROR` entry from `r_ausd_v_ta_notice` with "Abbruch wegen Fehler".
        *   The procedure call itself should raise an exception to the caller (e.g., Cloud Composer).

    ```sql
    -- SQL for Scenario A (Success)
    SELECT log_level, message, script_name FROM `my_project.my_dataset.job_log`
    WHERE job_kennung = 'TEST_LOG_SUCCESS' AND eintrags_nr = 10012
    ORDER BY created_at ASC;
    -- Expected results (order matters):
    -- 1. INFO, 'Job started', 'r_ausd_v_ta_notice'
    -- 2. INFO, 'ENDE Datenverarbeitung. Records=X', 'k_ausd_v_ta_notice' (where X is the actual count)
    -- 3. INFO, 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', 'r_ausd_v_ta_notice'

    -- SQL for Scenario B (Failure)
    SELECT log_level, message, script_name FROM `my_project.my_dataset.job_log`
    WHERE job_kennung = 'TEST_LOG_FAILURE' AND eintrags_nr = 10013
    ORDER BY created_at ASC;
    -- Expected results (order matters):
    -- 1. INFO, 'Job started', 'r_ausd_v_ta_notice'
    -- 2. ERROR, 'Abbruch wegen Fehler', 'r_ausd_v_ta_notice'
    ```