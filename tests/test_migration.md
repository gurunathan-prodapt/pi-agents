As a senior data-migration QA engineer, I have analyzed the provided migration design and generated BigQuery code for the `r_ausd_bp_ta_msisdn.ksh` job. The following test cases are designed to ensure behavioral equivalence between the legacy Oracle/KornShell system and the new BigQuery/Cloud Composer implementation.

Each test case includes its purpose, setup instructions, the action to be performed, and a concrete pass/fail criterion. Where applicable, runnable BigQuery SQL assertions are provided, which can be integrated into a Python-based testing framework (e.g., Pytest with `google-cloud-bigquery` client).

---

## Migration Validation Tests for `r_ausd_bp_ta_msisdn.ksh`

### Test Case 1: End-to-End Output Parity (Happy Path)

*   **Purpose:** Verify that the final target table `dwh_bert_dataset.sof_ta_msisdn` in BigQuery contains identical data to the legacy `sof$ta_msisdn` table in Oracle when executed with standard, valid inputs. This covers the entire job flow from wrapper to core transformation.
*   **Setup:**
    1.  **Legacy System:**
        *   Populate Oracle tables `isbert_schema.dwtk_meldungen` and `sof$ta_msisdn_his` with a diverse set of representative data, including multiple `bpri_com_id` entries with varying `valid_to` dates (some NULL), and `dwtk_meldungen` entries for `BERT_DROP_TEMP_TABLE`.
        *   Execute the legacy job: `r_ausd_bp_ta_msisdn.ksh -s 01012023 -l 100`.
        *   Export the resulting data from Oracle's `sof$ta_msisdn` table into a "golden file" (e.g., CSV, JSON) or a temporary comparison table.
    2.  **BigQuery System:**
        *   Ensure `dwh_bert_dataset.dwtk_meldungen` and `dwh_bert_dataset.sof_ta_msisdn_his` are populated with *exactly the same data* as their Oracle counterparts.
        *   Ensure `dwh_bert_dataset.sof_ta_msisdn` is empty before the test run.
*   **Action:**
    Execute the BigQuery wrapper stored procedure:
    ```sql
    CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper('01012023', '100');
    ```
*   **Pass/Fail Criterion:**
    The content of `dwh_bert_dataset.sof_ta_msisdn` must be identical to the data captured from the Oracle `sof$ta_msisdn` table. This includes:
    *   **Row Count:** The number of rows must be the same.
    *   **Column Values:** All `BPR_INSTANCE_ID`, `MSISDN`, `CALLNUMBER_ROLE_ID`, and `VALID_TO` values must match exactly for corresponding records. Order of rows does not matter for comparison.

    ```python
    # Example Python/Pytest assertion
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client()

    def test_output_parity_happy_path():
        # Assume oracle_golden_df is a pandas DataFrame loaded from the Oracle golden file
        # or directly from Oracle for comparison.
        # For this example, let's simulate it.
        oracle_golden_data = [
            {'BPR_INSTANCE_ID': 'ID1', 'MSISDN': '123', 'CALLNUMBER_ROLE_ID': 'A', 'VALID_TO': pd.Timestamp('2023-12-31')},
            {'BPR_INSTANCE_ID': 'ID2', 'MSISDN': '456', 'CALLNUMBER_ROLE_ID': 'B', 'VALID_TO': pd.Timestamp('4712-12-31')},
            # ... more data
        ]
        oracle_golden_df = pd.DataFrame(oracle_golden_data)
        oracle_golden_df['VALID_TO'] = oracle_golden_df['VALID_TO'].dt.date # Convert to date for comparison

        # Execute the BigQuery procedure (assuming this is done before assertion)
        # client.query("CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper('01012023', '100')").result()

        # Fetch data from BigQuery target table
        query = "SELECT BPR_INSTANCE_ID, MSISDN, CALLNUMBER_ROLE_ID, VALID_TO FROM dwh_bert_dataset.sof_ta_msisdn ORDER BY BPR_INSTANCE_ID, MSISDN"
        bq_df = client.query(query).to_dataframe()
        bq_df['VALID_TO'] = bq_df['VALID_TO'].dt.date # Convert to date for comparison

        # Sort both DataFrames for consistent comparison
        oracle_golden_df_sorted = oracle_golden_df.sort_values(by=['BPR_INSTANCE_ID', 'MSISDN']).reset_index(drop=True)
        bq_df_sorted = bq_df.sort_values(by=['BPR_INSTANCE_ID', 'MSISDN']).reset_index(drop=True)

        pd.testing.assert_frame_equal(oracle_golden_df_sorted, bq_df_sorted, check_dtype=False)
    ```

### Test Case 2: Transformation Correctness: `v_datum` Derivation

*   **Purpose:** Validate that the `v_datum` variable in `d_ausd_bp_ta_msisdn_transform` is correctly derived from `dwtk_meldungen` as `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, formatted `YYYYMMDD`, and defaults to `19000101` if no matching records exist.
*   **Setup:**
    1.  **Scenario A (Matching records):**
        ```sql
        TRUNCATE TABLE dwh_bert_dataset.dwtk_meldungen;
        INSERT INTO dwh_bert_dataset.dwtk_meldungen (timecreated, job_kennung) VALUES
        ('2023-01-01 10:00:00 UTC', 'OTHER_JOB'),
        ('2023-01-05 12:30:00 UTC', 'BERT_DROP_TEMP_TABLE'),
        ('2023-01-03 08:00:00 UTC', 'BERT_DROP_TEMP_TABLE'),
        ('2023-01-10 15:00:00 UTC', 'BERT_DROP_TEMP_TABLE');
        ```
    2.  **Scenario B (No matching records):**
        ```sql
        TRUNCATE TABLE dwh_bert_dataset.dwtk_meldungen;
        INSERT INTO dwh_bert_dataset.dwtk_meldungen (timecreated, job_kennung) VALUES
        ('2023-01-01 10:00:00 UTC', 'OTHER_JOB');
        ```
    3.  **Scenario C (Empty table):**
        ```sql
        TRUNCATE TABLE dwh_bert_dataset.dwtk_meldungen;
        ```
*   **Action:**
    Since `v_datum` is an internal variable, we cannot directly query it. We will infer its correctness by observing the behavior of the `d_ausd_bp_ta_msisdn_transform` procedure, or by temporarily modifying the procedure to log `v_datum` for testing purposes. For a robust test, we can create a temporary procedure that only performs the `v_datum` derivation and returns it.

    ```sql
    -- Temporary procedure for testing v_datum derivation
    CREATE OR REPLACE PROCEDURE dwh_bert_dataset.test_v_datum_derivation(OUT result_v_datum STRING)
    BEGIN
      DECLARE v_datum_internal STRING;
      SET v_datum_internal = (
        SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM dwh_bert_dataset.dwtk_meldungen AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
      );
      SET result_v_datum = v_datum_internal;
    END;
    ```
    Execute: `CALL dwh_bert_dataset.test_v_datum_derivation(@derived_v_datum);`
*   **Pass/Fail Criterion:**
    *   **Scenario A:** `@derived_v_datum` must be `'20230110'`.
    *   **Scenario B & C:** `@derived_v_datum` must be `'19000101'`.

    ```python
    # Example Python/Pytest assertion
    def test_v_datum_derivation():
        client = bigquery.Client()

        # Scenario A
        client.query("TRUNCATE TABLE dwh_bert_dataset.dwtk_meldungen;").result()
        client.query("""
            INSERT INTO dwh_bert_dataset.dwtk_meldungen (timecreated, job_kennung) VALUES
            ('2023-01-01 10:00:00 UTC', 'OTHER_JOB'),
            ('2023-01-05 12:30:00 UTC', 'BERT_DROP_TEMP_TABLE'),
            ('2023-01-03 08:00:00 UTC', 'BERT_DROP_TEMP_TABLE'),
            ('2023-01-10 15:00:00 UTC', 'BERT_DROP_TEMP_TABLE');
        """).result()
        client.query("CALL dwh_bert_dataset.test_v_datum_derivation(@derived_v_datum);").result()
        result = client.query("SELECT @derived_v_datum AS v_datum;").to_dataframe().iloc[0]['v_datum']
        assert result == '20230110'

        # Scenario B
        client.query("TRUNCATE TABLE dwh_bert_dataset.dwtk_meldungen;").result()
        client.query("""
            INSERT INTO dwh_bert_dataset.dwtk_meldungen (timecreated, job_kennung) VALUES
            ('2023-01-01 10:00:00 UTC', 'OTHER_JOB');
        """).result()
        client.query("CALL dwh_bert_dataset.test_v_datum_derivation(@derived_v_datum);").result()
        result = client.query("SELECT @derived_v_datum AS v_datum;").to_dataframe().iloc[0]['v_datum']
        assert result == '19000101'

        # Scenario C
        client.query("TRUNCATE TABLE dwh_bert_dataset.dwtk_meldungen;").result()
        client.query("CALL dwh_bert_dataset.test_v_datum_derivation(@derived_v_datum);").result()
        result = client.query("SELECT @derived_v_datum AS v_datum;").to_dataframe().iloc[0]['v_datum']
        assert result == '19000101'
    ```

### Test Case 3: Transformation Correctness: Latest MSISDN Selection & NULL Handling

*   **Purpose:** Verify the core transformation logic in `d_ausd_bp_ta_msisdn_transform`, specifically the analytic function `MAX() OVER (PARTITION BY bpri_com_id)` and the `COALESCE` handling for `VALID_TO` dates (treating `NULL` as `4712-12-31`).
*   **Setup:**
    Populate `dwh_bert_dataset.sof_ta_msisdn_his` with the following data:
    ```sql
    TRUNCATE TABLE dwh_bert_dataset.sof_ta_msisdn_his;
    INSERT INTO dwh_bert_dataset.sof_ta_msisdn_his (bpri_com_id, msisdn, callnumber_role_id, valid_to) VALUES
    ('BP1', 'MSISDN1', 'ROLE_A', '2023-01-01'),
    ('BP1', 'MSISDN1', 'ROLE_A', '2023-06-30'), -- Older valid_to
    ('BP1', 'MSISDN2', 'ROLE_B', '2023-12-31'), -- Latest valid_to for BP1
    ('BP1', 'MSISDN3', 'ROLE_C', NULL),         -- NULL valid_to for BP1 (should be 4712-12-31)
    ('BP2', 'MSISDN4', 'ROLE_D', '2022-05-15'),
    ('BP2', 'MSISDN5', 'ROLE_E', '2022-05-15'), -- Same valid_to, both should be selected
    ('BP3', 'MSISDN6', 'ROLE_F', NULL),         -- NULL valid_to for BP3
    ('BP4', 'MSISDN7', 'ROLE_G', '4712-12-31'); -- Explicit far future date
    ```
*   **Action:**
    Execute the core transformation procedure:
    ```sql
    CALL dwh_bert_dataset.d_ausd_bp_ta_msisdn_transform();
    ```
*   **Pass/Fail Criterion:**
    Query `dwh_bert_dataset.sof_ta_msisdn`. The table should contain the following records:
    *   `BP1`, `MSISDN3`, `ROLE_C`, `4712-12-31` (because NULL is treated as 4712-12-31, which is the max)
    *   `BP2`, `MSISDN4`, `ROLE_D`, `2022-05-15`
    *   `BP2`, `MSISDN5`, `ROLE_E`, `2022-05-15` (both selected as they share the max `valid_to`)
    *   `BP3`, `MSISDN6`, `ROLE_F`, `4712-12-31`
    *   `BP4`, `MSISDN7`, `ROLE_G`, `4712-12-31`

    ```sql
    -- Expected output for dwh_bert_dataset.sof_ta_msisdn
    SELECT BPR_INSTANCE_ID, MSISDN, CALLNUMBER_ROLE_ID, VALID_TO FROM dwh_bert_dataset.sof_ta_msisdn ORDER BY BPR_INSTANCE_ID, MSISDN;
    -- Expected:
    -- BP1, MSISDN3, ROLE_C, 4712-12-31
    -- BP2, MSISDN4, ROLE_D, 2022-05-15
    -- BP2, MSISDN5, ROLE_E, 2022-05-15
    -- BP3, MSISDN6, ROLE_F, 4712-12-31
    -- BP4, MSISDN7, ROLE_G, 4712-12-31
    ```

### Test Case 4: Wrapper Logic: Default `p_stichtag`

*   **Purpose:** Verify that `p_stichtag` correctly defaults to the current system date (DDMMYYYY format) when not provided in the wrapper call.
*   **Setup:**
    1.  Ensure `dwh_bert_dataset.dwtk_meldungen` and `dwh_bert_dataset.sof_ta_msisdn_his` are populated with some data to allow the job to run successfully.
    2.  Clear `dwh_bert_dataset.job_log` before execution.
*   **Action:**
    Execute the BigQuery wrapper stored procedure without providing `p_stichtag_in`:
    ```sql
    CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper(NULL, '0'); -- or '' for empty string
    ```
*   **Pass/Fail Criterion:**
    1.  Query `dwh_bert_dataset.job_log` for the `STARTED` entry of the latest job run. The `stichtag` column in this log entry must be equal to `CURRENT_DATE()` of the execution day.
    2.  The job must complete successfully (`COMPLETED` entry in `job_log`).

    ```sql
    -- Assertion for job_log
    SELECT stichtag FROM dwh_bert_dataset.job_log
    WHERE job_name = 'ausd_bp_ta_msisdn' AND status = 'STARTED'
    ORDER BY created_at DESC LIMIT 1;
    -- Expected: CURRENT_DATE()
    ```

### Test Case 5: Wrapper Logic: Default `p_wiederanlaufWert` (and its non-impact on data)

*   **Purpose:** Verify that `p_wiederanlaufWert` defaults to `'0'` when not provided, and confirm that this parameter, as per the provided `d_ausd_bp_ta_msisdn.sql` and its BigQuery migration, does *not* influence the data inserted into `sof_ta_msisdn`.
*   **Setup:**
    1.  Populate `dwh_bert_dataset.dwtk_meldungen` and `dwh_bert_dataset.sof_ta_msisdn_his` with data (e.g., the same data as in Test Case 3).
    2.  Clear `dwh_bert_dataset.job_log` and `dwh_bert_dataset.sof_ta_msisdn`.
*   **Action:**
    Execute the BigQuery wrapper stored procedure without providing `p_wiederanlaufWert_in`:
    ```sql
    CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper('01012023', NULL); -- or '' for empty string
    ```
*   **Pass/Fail Criterion:**
    1.  Query `dwh_bert_dataset.job_log` for the `STARTED` entry of the latest job run. The `wiederanlaufwert` column in this log entry must be `'0'`.
    2.  The data in `dwh_bert_dataset.sof_ta_msisdn` must be identical to the expected output from Test Case 3 (i.e., the `p_wiederanlaufWert` had no filtering effect). This confirms the migrated code accurately reflects the legacy SQL's behavior of ignoring this parameter for data transformation.

    ```sql
    -- Assertion for job_log
    SELECT wiederanlaufwert FROM dwh_bert_dataset.job_log
    WHERE job_name = 'ausd_bp_ta_msisdn' AND status = 'STARTED'
    ORDER BY created_at DESC LIMIT 1;
    -- Expected: '0'

    -- Assertion for sof_ta_msisdn (should match Test Case 3's expected output)
    SELECT BPR_INSTANCE_ID, MSISDN, CALLNUMBER_ROLE_ID, VALID_TO FROM dwh_bert_dataset.sof_ta_msisdn ORDER BY BPR_INSTANCE_ID, MSISDN;
    -- Expected: (Same as Test Case 3)
    -- BP1, MSISDN3, ROLE_C, 4712-12-31
    -- BP2, MSISDN4, ROLE_D, 2022-05-15
    -- BP2, MSISDN5, ROLE_E, 2022-05-15
    -- BP3, MSISDN6, ROLE_F, 4712-12-31
    -- BP4, MSISDN7, ROLE_G, 4712-12-31
    ```

### Test Case 6: Wrapper Logic: Invalid `p_stichtag` Format (Error Handling)

*   **Purpose:** Verify that the wrapper and controller procedures correctly handle and log an invalid `p_stichtag` format.
*   **Setup:**
    1.  Clear `dwh_bert_dataset.job_log`.
    2.  Ensure source tables are populated to allow the job to attempt execution.
*   **Action:**
    Execute the BigQuery wrapper stored procedure with an invalid `p_stichtag_in` format (e.g., `YYYY-MM-DD` instead of `DDMMYYYY`):
    ```sql
    CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper('2023-01-01', '0');
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement must fail and raise an error (e.g., `SQLSTATE '45000'`).
    2.  Query `dwh_bert_dataset.job_log` for the latest job run. There must be a `STARTED` entry followed by a `FAILED` entry for the same `job_entry_nr`.
    3.  The `FAILED` entry must have:
        *   `log_level = 'ERROR'`
        *   `status = 'FAILED'`
        *   `error_code` indicating a validation error (e.g., `VALIDATION_ERROR` or `BQ_EXCEPTION_CODE`).
        *   `log_message` containing text similar to `'Invalid Stichtag format provided or derived: 2023-01-01. Expected DDMMYYYY.'` or `'Invalid p_Stichtag format (2023-01-01). Expected DDMMYYYY.'`.

    ```sql
    -- Assertion for job_log
    SELECT log_level, status, error_code, log_message
    FROM dwh_bert_dataset.job_log
    WHERE job_name = 'ausd_bp_ta_msisdn'
    ORDER BY created_at DESC LIMIT 1;
    -- Expected:
    -- log_level: 'ERROR'
    -- status: 'FAILED'
    -- error_code: 'VALIDATION_ERROR' (or similar BigQuery error code)
    -- log_message: (contains expected error message)
    ```

### Test Case 7: External System Replacement: Logging and Error Handling

*   **Purpose:** Verify that job execution status (start, success, failure) and relevant parameters are correctly logged to `dwh_bert_dataset.job_log`, replacing the legacy file-based logging.
*   **Setup:**
    1.  Clear `dwh_bert_dataset.job_log`.
    2.  Populate source tables for a successful run.
*   **Action:**
    1.  **Successful Run:** Execute `CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper('01012023', '0');`
    2.  **Failed Run:** Execute `CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper('INVALID_DATE', '0');` (this will raise an error as per Test Case 6).
*   **Pass/Fail Criterion:**
    1.  **Successful Run:**
        *   Query `dwh_bert_dataset.job_log`. There must be two entries for the same `job_entry_nr`: one with `status = 'STARTED'` and one with `status = 'COMPLETED'`.
        *   The `STARTED` entry must have `log_level = 'INFO'`, `log_message` indicating job start, `stichtag` as `2023-01-01`, and `wiederanlaufwert` as `'0'`.
        *   The `COMPLETED` entry must have `log_level = 'INFO'`, `log_message` indicating success, and `finished_at` populated.
    2.  **Failed Run:**
        *   Query `dwh_bert_dataset.job_log`. There must be two entries for the same `job_entry_nr`: one with `status = 'STARTED'` and one with `status = 'FAILED'`.
        *   The `STARTED` entry must have `log_level = 'INFO'`, `log_message` indicating job start, `stichtag` as `NULL` (if validation fails before parsing) or the invalid string, and `wiederanlaufwert` as `'0'`.
        *   The `FAILED` entry must have `log_level = 'ERROR'`, `log_message` containing the error details, `error_code`, `error_argument`, and `finished_at` populated.

    ```sql
    -- Assertion for successful run
    SELECT job_entry_nr, log_level, status, stichtag, wiederanlaufwert, log_message, finished_at
    FROM dwh_bert_dataset.job_log
    WHERE job_name = 'ausd_bp_ta_msisdn'
    ORDER BY created_at DESC LIMIT 2;
    -- Expected: Two rows, one STARTED, one COMPLETED, with correct details.

    -- Assertion for failed run (after executing the failing call)
    SELECT job_entry_nr, log_level, status, stichtag, wiederanlaufwert, log_message, error_code, error_argument, finished_at
    FROM dwh_bert_dataset.job_log
    WHERE job_name = 'ausd_bp_ta_msisdn'
    ORDER BY created_at DESC LIMIT 2;
    -- Expected: Two rows, one STARTED, one FAILED, with correct error details.
    ```

### Test Case 8: Data Quality: Target Table Schema and Nullability

*   **Purpose:** Verify that the `dwh_bert_dataset.sof_ta_msisdn` table has the correct schema (column names, data types) and nullability as inferred from the migration design and transformation logic.
*   **Setup:** None. This is a metadata check.
*   **Action:**
    Inspect the schema of `dwh_bert_dataset.sof_ta_msisdn` using BigQuery's `INFORMATION_SCHEMA`.
*   **Pass/Fail Criterion:**
    The schema must match the following:
    *   `BPR_INSTANCE_ID`: `STRING`, `NULLABLE`
    *   `MSISDN`: `STRING`, `NULLABLE`
    *   `CALLNUMBER_ROLE_ID`: `STRING`, `NULLABLE`
    *   `VALID_TO`: `DATE`, `NOT NULL` (because `COALESCE(cn1.valid_to, DATE '4712-12-31')` ensures it's never NULL in the target).

    ```sql
    SELECT column_name, data_type, is_nullable
    FROM dwh_bert_dataset.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'sof_ta_msisdn'
    ORDER BY ordinal_position;
    -- Expected Output:
    -- column_name       data_type   is_nullable
    -- BPR_INSTANCE_ID   STRING      YES
    -- MSISDN            STRING      YES
    -- CALLNUMBER_ROLE_ID STRING      YES
    -- VALID_TO          DATE        NO
    ```

### Test Case 9: Data Quality: Row Count Parity

*   **Purpose:** Ensure that the number of records processed and inserted into the target table is consistent with the legacy system for a given set of source data.
*   **Setup:**
    1.  **Legacy System:**
        *   Populate Oracle tables `isbert_schema.dwtk_meldungen` and `sof$ta_msisdn_his` with a known number of records.
        *   Execute the legacy job.
        *   Record `SELECT COUNT(*) FROM sof$ta_msisdn;`
    2.  **BigQuery System:**
        *   Populate `dwh_bert_dataset.dwtk_meldungen` and `dwh_bert_dataset.sof_ta_msisdn_his` with *exactly the same data* as their Oracle counterparts.
        *   Ensure `dwh_bert_dataset.sof_ta_msisdn` is empty.
*   **Action:**
    Execute the BigQuery wrapper stored procedure:
    ```sql
    CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper('01012023', '0');
    ```
*   **Pass/Fail Criterion:**
    The row count of `dwh_bert_dataset.sof_ta_msisdn` must exactly match the row count obtained from the Oracle `sof$ta_msisdn` table.

    ```sql
    -- Assertion for BigQuery row count
    SELECT COUNT(*) FROM dwh_bert_dataset.sof_ta_msisdn;
    -- Expected: Must match the count from Oracle's sof$ta_msisdn.
    ```

### Test Case 10: Edge Case: Empty Source Tables

*   **Purpose:** Verify the job's behavior when source tables (`dwtk_meldungen`, `sof_ta_msisdn_his`) are empty. The job should run without error and result in an empty target table.
*   **Setup:**
    1.  Ensure `dwh_bert_dataset.dwtk_meldungen`, `dwh_bert_dataset.sof_ta_msisdn_his`, and `dwh_bert_dataset.sof_ta_msisdn` are all empty.
    2.  Clear `dwh_bert_dataset.job_log`.
*   **Action:**
    Execute the BigQuery wrapper stored procedure:
    ```sql
    CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper('01012023', '0');
    ```
*   **Pass/Fail Criterion:**
    1.  The job must complete successfully (`COMPLETED` entry in `dwh_bert_dataset.job_log`).
    2.  `dwh_bert_dataset.sof_ta_msisdn` must remain empty (row count = 0).
    3.  The `v_datum` derivation should correctly default to `19000101` (this can be inferred from the `job_log` if `v_datum` was logged, or by temporarily modifying `d_ausd_bp_ta_msisdn_transform` to return it).

    ```sql
    -- Assertion for job_log
    SELECT status FROM dwh_bert_dataset.job_log
    WHERE job_name = 'ausd_bp_ta_msisdn' AND status = 'COMPLETED'
    ORDER BY created_at DESC LIMIT 1;
    -- Expected: 'COMPLETED'

    -- Assertion for target table row count
    SELECT COUNT(*) FROM dwh_bert_dataset.sof_ta_msisdn;
    -- Expected: 0
    ```