As a senior data-migration QA engineer, I've analyzed the migration design for `k_ausd_v_ta_acc_ref.ksh` to a BigQuery Stored Procedure. The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality assertions.

Each test case includes its purpose, setup, action, and concrete pass/fail criteria. Where applicable, BigQuery SQL assertions are provided.

---

## Migration Validation Tests for `usp_k_ausd_v_ta_acc_ref`

**Assumptions & Prerequisites:**

*   The BigQuery dataset `isbert_rpt_staging` exists.
*   All DDLs for `sof_ta_acc_ref`, `dwtk_meldungen`, `cds_ta_acc_ref`, `job_control`, `job_error_log`, and `job_run_log` have been executed in `isbert_rpt_staging`.
*   The `usp_k_ausd_v_ta_acc_ref` stored procedure has been successfully created.
*   A dedicated test environment for the legacy Oracle system is available to run the original KornShell script and capture its results (output table `sof$ta_acc_ref`, console logs, temporary files).
*   The data ingestion pipeline from Oracle to BigQuery for `dwtk_meldungen` and `cds_ta_acc_ref` is assumed to be separately validated and capable of populating the BigQuery staging tables with accurate replicas of the Oracle source data for testing purposes.

---

### Test Case 1: Happy Path - Full Data Load and Output Parity

*   **Purpose:** Verify the core transformation logic, including date determination, truncation, filtering, and insertion, produces identical results to the legacy system under normal operating conditions. This covers output parity and transformation correctness.
*   **Setup:**
    1.  **Legacy Oracle:**
        *   `isbert_schema.dwtk_meldungen`: Insert a record with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = TO_TIMESTAMP('2023-01-15 10:00:00', 'YYYY-MM-DD HH24:MI:SS')`.
        *   `cds$ta_acc_ref@pcrs1`: Insert a diverse set of records, including:
            *   Records with `is_production = 1` and `insert_at`, `modified_at`, `valid_from`, or `valid_to` on or after '2023-01-15'.
            *   Records with `is_production = 1` and all relevant dates before '2023-01-15'.
            *   Records with `is_production = 0`.
            *   Records with NULLs in some date columns.
        *   `sof$ta_acc_ref`: Ensure this table is empty or contains old data that will be truncated.
    2.  **BigQuery:**
        *   `isbert_rpt_staging.dwtk_meldungen`: Mirror the Oracle `dwtk_meldungen` data.
        *   `isbert_rpt_staging.cds_ta_acc_ref`: Mirror the Oracle `cds$ta_acc_ref@pcrs1` data.
        *   `isbert_rpt_staging.sof_ta_acc_ref`: Ensure this table is empty or contains old data.
        *   `isbert_rpt_staging.job_control`, `job_run_log`, `job_error_log`: Ensure these tables are empty or clean for the specific `p_job_kennung`/`p_eintragsnr`.
*   **Action:**
    1.  Execute the legacy KornShell script: `k_ausd_v_ta_acc_ref.ksh -j "TEST_JOB_1" -f "001"`
    2.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('TEST_JOB_1', '001');
        ```
*   **Pass/Fail Criteria:**
    1.  **Row Count Parity:** The number of records in Oracle `sof$ta_acc_ref` must exactly match the number of records in BigQuery `isbert_rpt_staging.sof_ta_acc_ref`.
    2.  **Data Content Parity:** All records in BigQuery `isbert_rpt_staging.sof_ta_acc_ref` must exactly match the corresponding records in Oracle `sof$ta_acc_ref` (column by column comparison, accounting for data type conversions like `DATE` precision if applicable).
    3.  **Job Log Parity:** The record count reported in the legacy script's temporary file (or console output) must match `processed_records` in `isbert_rpt_staging.job_run_log` for `TEST_JOB_1`/`001`. The `job_run_log` status should be 'SUCCESS'.

    ```sql
    -- BigQuery SQL for Row Count Parity
    SELECT
        (SELECT COUNT(*) FROM `isbert_rpt_staging.sof_ta_acc_ref`) = (SELECT <legacy_oracle_row_count_from_test_run>) AS row_count_match;

    -- BigQuery SQL for Data Content Parity (example for a few columns, extend for all)
    -- This assumes you can export Oracle data to a temporary BigQuery table for comparison,
    -- or perform a direct comparison if a federated query is possible.
    -- For practical purposes, a Python script would typically extract both datasets and compare.
    SELECT
        COUNT(*)
    FROM
        `isbert_rpt_staging.sof_ta_acc_ref` bq
    FULL OUTER JOIN
        `your_oracle_export_dataset.sof_ta_acc_ref_export` ora
    ON
        bq.ta_acc_ref_key = ora.ta_acc_ref_key -- Assuming ta_acc_ref_key is a unique identifier
        AND bq.insert_at = ora.insert_at
        AND bq.modified_at = ora.modified_at
        AND bq.is_production = ora.is_production
        -- ... add all other columns for comparison
    WHERE
        bq.ta_acc_ref_key IS NULL OR ora.ta_acc_ref_key IS NULL;
    -- Pass if this query returns 0 (no mismatches or missing rows)

    -- BigQuery SQL for Job Log Verification
    SELECT
        processed_records = (SELECT <legacy_record_count_from_tmp_file>) AND status = 'SUCCESS' AS job_log_match
    FROM
        `isbert_rpt_staging.job_run_log`
    WHERE
        job_kennung = 'TEST_JOB_1' AND eintragsnr = '001'
    ORDER BY start_timestamp DESC
    LIMIT 1;
    ```

---

### Test Case 2: `v_datum` Calculation - Default Value ('19000101')

*   **Purpose:** Verify that when no relevant `dwtk_meldungen` entry exists, the processing date (`v_datum`) correctly defaults to '19000101', leading to the inclusion of all `is_production = 1` records regardless of their dates. This covers transformation correctness (aggregation, NULL handling, default values).
*   **Setup:**
    1.  **Legacy Oracle:**
        *   `isbert_schema.dwtk_meldungen`: Ensure no records exist with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   `cds$ta_acc_ref@pcrs1`: Insert records with `is_production = 1` and various dates (some very old, some recent). Also include records with `is_production = 0`.
        *   `sof$ta_acc_ref`: Empty.
    2.  **BigQuery:**
        *   `isbert_rpt_staging.dwtk_meldungen`: Empty or no records with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   `isbert_rpt_staging.cds_ta_acc_ref`: Mirror the Oracle data.
        *   `isbert_rpt_staging.sof_ta_acc_ref`: Empty.
*   **Action:**
    1.  Execute the legacy KornShell script: `k_ausd_v_ta_acc_ref.ksh -j "TEST_JOB_2" -f "001"`
    2.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('TEST_JOB_2', '001');
        ```
*   **Pass/Fail Criteria:**
    1.  **Row Count Parity:** The number of records in Oracle `sof$ta_acc_ref` must exactly match the number of records in BigQuery `isbert_rpt_staging.sof_ta_acc_ref`. This count should be equal to the total number of `is_production = 1` records in the source `cds$ta_acc_ref`.
    2.  **Data Content Parity:** All records in BigQuery `isbert_rpt_staging.sof_ta_acc_ref` must exactly match the corresponding records in Oracle `sof$ta_acc_ref`.
    3.  **Transformation Logic:** Verify that all `is_production = 1` records from `cds_ta_acc_ref` (regardless of their date fields) are present in `sof_ta_acc_ref`, and no `is_production = 0` records are present.

---

### Test Case 3: Filtering - Date Columns and `is_production`

*   **Purpose:** Verify the combined filtering logic based on `v_datum` and `is_production = 1` works correctly, including edge cases for date comparisons and NULL handling in date fields. This covers transformation correctness (filters, NULL handling).
*   **Setup:**
    1.  **Legacy Oracle:**
        *   `isbert_schema.dwtk_meldungen`: Insert a record with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')`. (This sets `v_datum` to '20230301').
        *   `cds$ta_acc_ref@pcrs1`: Insert records with `is_production = 1` and `is_production = 0`, and varying date values:
            *   `is_production = 1`, all dates < '2023-03-01' (should be excluded).
            *   `is_production = 1`, `insert_at` = '2023-03-01', others < '2023-03-01' (should be included).
            *   `is_production = 1`, `modified_at` = '2023-03-02', others < '2023-03-01' (should be included).
            *   `is_production = 1`, `valid_from` = '2023-03-01', others < '2023-03-01' (should be included).
            *   `is_production = 1`, `valid_to` = '2023-03-01', others < '2023-03-01' (should be included).
            *   `is_production = 1`, `insert_at` IS NULL, `modified_at` IS NULL, `valid_from` IS NULL, `valid_to` IS NULL (should be excluded, as `NULL >= date` is false).
            *   `is_production = 0`, with any dates (should be excluded).
        *   `sof$ta_acc_ref`: Empty.
    2.  **BigQuery:**
        *   `isbert_rpt_staging.dwtk_meldungen`: Mirror the Oracle data.
        *   `isbert_rpt_staging.cds_ta_acc_ref`: Mirror the Oracle data.
        *   `isbert_rpt_staging.sof_ta_acc_ref`: Empty.
*   **Action:**
    1.  Execute the legacy KornShell script: `k_ausd_v_ta_acc_ref.ksh -j "TEST_JOB_3" -f "001"`
    2.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('TEST_JOB_3', '001');
        ```
*   **Pass/Fail Criteria:**
    1.  **Row Count Parity:** The number of records in Oracle `sof$ta_acc_ref` must exactly match the number of records in BigQuery `isbert_rpt_staging.sof_ta_acc_ref`.
    2.  **Data Content Parity:** All records in BigQuery `isbert_rpt_staging.sof_ta_acc_ref` must exactly match the corresponding records in Oracle `sof$ta_acc_ref`.
    3.  **Filter Logic:** Only records where `is_production = 1` AND at least one of `insert_at`, `modified_at`, `valid_from`, `valid_to` is on or after '2023-03-01' should be present in the target table. Records with all relevant date fields as NULL should be excluded.

---

### Test Case 4: Truncation Behavior

*   **Purpose:** Verify that the target table `sof_ta_acc_ref` is correctly truncated before new data is inserted, ensuring a clean load each run. This covers transformation correctness (DDL operations).
*   **Setup:**
    1.  **Legacy Oracle:**
        *   `isbert_schema.dwtk_meldungen`: Insert a record with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = TO_TIMESTAMP('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')`.
        *   `cds$ta_acc_ref@pcrs1`: Insert 5 records with `is_production = 1` and dates >= '2023-01-01'.
        *   `sof$ta_acc_ref`: Insert 10 "dummy" records (e.g., `is_production = 0` or very old dates) that should be removed by truncation.
    2.  **BigQuery:**
        *   `isbert_rpt_staging.dwtk_meldungen`: Mirror the Oracle data.
        *   `isbert_rpt_staging.cds_ta_acc_ref`: Mirror the Oracle data.
        *   `isbert_rpt_staging.sof_ta_acc_ref`: Insert 10 "dummy" records.
*   **Action:**
    1.  Execute the legacy KornShell script: `k_ausd_v_ta_acc_ref.ksh -j "TEST_JOB_4" -f "001"`
    2.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('TEST_JOB_4', '001');
        ```
*   **Pass/Fail Criteria:**
    1.  **Row Count Parity:** Both Oracle `sof$ta_acc_ref` and BigQuery `isbert_rpt_staging.sof_ta_acc_ref` must contain exactly 5 records after execution. The initial 10 "dummy" records must be gone.
    2.  **Data Content Parity:** The 5 records in both target tables must be identical and correspond to the 5 valid records from `cds$ta_acc_ref`.

---

### Test Case 5: Job Control - Active Job Ignored

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly identifies and skips execution if an active job with the same `job_kennung` and `eintragsnr` is already running, mimicking the legacy `starteSQLSkript` behavior. This covers external system replacements (job control framework).
*   **Setup:**
    1.  **BigQuery:**
        *   `isbert_rpt_staging.job_control`: Insert a record for `job_kennung = 'ACTIVE_JOB'` and `eintragsnr = '001'` with `status = 'ACTIVE'`.
        *   `isbert_rpt_staging.sof_ta_acc_ref`: Populate with some initial data (e.g., 10 records).
        *   `isbert_rpt_staging.dwtk_meldungen`, `cds_ta_acc_ref`: Populate with data that would normally result in 5 records being loaded.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('ACTIVE_JOB', '001');
        ```
*   **Pass/Fail Criteria:**
    1.  **Procedure Termination:** The stored procedure call must terminate with an error (due to `RAISE USING MESSAGE = 'Job already active. Skipping execution.'`).
    2.  **No Data Modification:** The `isbert_rpt_staging.sof_ta_acc_ref` table must remain unchanged (still contain the initial 10 records).
    3.  **Job Log Status:** A new entry in `isbert_rpt_staging.job_run_log` should exist for `ACTIVE_JOB`/`001` with `status = 'SKIPPED'` and `log_message` indicating the skip reason.
    4.  **Error Log:** An entry should be present in `isbert_rpt_staging.job_error_log` reflecting the skipped job message.

    ```sql
    -- Python/Pytest pseudo-code for verification
    def test_active_job_skipped():
        # Setup: Insert active job into job_control
        bq_client.query("INSERT INTO `isbert_rpt_staging.job_control` (job_kennung, eintragsnr, status) VALUES ('ACTIVE_JOB', '001', 'ACTIVE')").result()
        bq_client.query("INSERT INTO `isbert_rpt_staging.sof_ta_acc_ref` (ta_acc_ref_key) VALUES ('dummy_data')").result() # Initial data

        try:
            bq_client.query("CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('ACTIVE_JOB', '001')").result()
            assert False, "Procedure should have raised an error for active job."
        except Exception as e:
            assert "Job already active. Skipping execution." in str(e)

        # Verify no data modification
        result = bq_client.query("SELECT COUNT(*) FROM `isbert_rpt_staging.sof_ta_acc_ref`").result()
        assert list(result)[0].f0_ == 1 # Still 1 dummy record

        # Verify job log status
        result = bq_client.query("""
            SELECT status, log_message FROM `isbert_rpt_staging.job_run_log`
            WHERE job_kennung = 'ACTIVE_JOB' AND eintragsnr = '001'
            ORDER BY start_timestamp DESC LIMIT 1
        """).result()
        log_entry = list(result)[0]
        assert log_entry.status == 'SKIPPED'
        assert "Job already active. Skipping execution." in log_entry.log_message

        # Verify error log
        result = bq_client.query("""
            SELECT error_message FROM `isbert_rpt_staging.job_error_log`
            WHERE job_kennung = 'ACTIVE_JOB' AND eintragsnr = '001'
            ORDER BY error_timestamp DESC LIMIT 1
        """).result()
        error_entry = list(result)[0]
        assert "Job already active. Skipping execution." in error_entry.error_message
    ```

---

### Test Case 6: Error Handling and Logging

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly handles runtime errors, logs them, and updates job status accordingly. This covers external system replacements (error logging).
*   **Setup:**
    1.  **BigQuery:**
        *   `isbert_rpt_staging.job_control`: Clean for `job_kennung = 'ERROR_JOB'` and `eintragsnr = '001'`.
        *   `isbert_rpt_staging.sof_ta_acc_ref`: Empty.
        *   `isbert_rpt_staging.dwtk_meldungen`: Valid data.
        *   `isbert_rpt_staging.cds_ta_acc_ref`: Insert data that will cause a BigQuery error during the `INSERT ... SELECT` statement. For example, if `ta_acc_gueltigkeit_von` in `cds_ta_acc_ref` is defined as `DATE` but contains a string that cannot be parsed as a date, or if a column is `NOT NULL` in `sof_ta_acc_ref` but `cds_ta_acc_ref` provides a `NULL` value.
            *   *Example Error Scenario:* Change `isbert_rpt_staging.sof_ta_acc_ref` DDL to make `ta_acc_ref_key` `NOT NULL`, then insert a record into `cds_ta_acc_ref` with `ta_acc_ref_key = NULL`. (This would require temporarily altering the DDL for this test, or finding another way to inject an error). A simpler way might be to introduce a data type mismatch if possible, e.g., if `ta_acc_ref_key` is `STRING` in `sof_ta_acc_ref` but `INT64` in `cds_ta_acc_ref` for a specific test record.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('ERROR_JOB', '001');
        ```
*   **Pass/Fail Criteria:**
    1.  **Procedure Termination:** The stored procedure call must terminate with an error (due to the `RAISE` in the `EXCEPTION` block).
    2.  **No Data Insertion (or partial):** `isbert_rpt_staging.sof_ta_acc_ref` should either be empty or contain only a partial set of records if the error occurred mid-transaction (BigQuery DML is atomic, so it should be empty if the error is in the main `INSERT`).
    3.  **Job Log Status:** A new entry in `isbert_rpt_staging.job_run_log` should exist for `ERROR_JOB`/`001` with `status = 'FAILED'`.
    4.  **Error Log:** An entry must be present in `isbert_rpt_staging.job_error_log` for `ERROR_JOB`/`001` containing the specific error message, SQL state, and stack trace from the BigQuery execution.
    5.  **Job Control Status:** The `isbert_rpt_staging.job_control` table for `ERROR_JOB`/`001` should have `status = 'FAILED'`.

    ```sql
    -- BigQuery SQL for verification (after procedure call)
    SELECT
        status, log_message
    FROM
        `isbert_rpt_staging.job_run_log`
    WHERE
        job_kennung = 'ERROR_JOB' AND eintragsnr = '001'
    ORDER BY start_timestamp DESC
    LIMIT 1;
    -- Expected: status = 'FAILED', log_message contains error details

    SELECT
        error_message, sql_state, stack_trace
    FROM
        `isbert_rpt_staging.job_error_log`
    WHERE
        job_kennung = 'ERROR_JOB' AND eintragsnr = '001'
    ORDER BY error_timestamp DESC
    LIMIT 1;
    -- Expected: error_message, sql_state, stack_trace populated with error details

    SELECT
        status
    FROM
        `isbert_rpt_staging.job_control`
    WHERE
        job_kennung = 'ERROR_JOB' AND eintragsnr = '001';
    -- Expected: status = 'FAILED'
    ```

---

### Test Case 7: Empty Source `cds_ta_acc_ref`

*   **Purpose:** Verify the job handles an empty source table gracefully, resulting in an empty target table and correct record count. This covers data quality (row count) and transformation correctness.
*   **Setup:**
    1.  **Legacy Oracle:**
        *   `isbert_schema.dwtk_meldungen`: Insert a record with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = TO_TIMESTAMP('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')`.
        *   `cds$ta_acc_ref@pcrs1`: Ensure this table is empty.
        *   `sof$ta_acc_ref`: Empty.
    2.  **BigQuery:**
        *   `isbert_rpt_staging.dwtk_meldungen`: Mirror the Oracle data.
        *   `isbert_rpt_staging.cds_ta_acc_ref`: Ensure this table is empty.
        *   `isbert_rpt_staging.sof_ta_acc_ref`: Empty.
*   **Action:**
    1.  Execute the legacy KornShell script: `k_ausd_v_ta_acc_ref.ksh -j "TEST_JOB_7" -f "001"`
    2.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('TEST_JOB_7', '001');
        ```
*   **Pass/Fail Criteria:**
    1.  **Empty Target:** Both Oracle `sof$ta_acc_ref` and BigQuery `isbert_rpt_staging.sof_ta_acc_ref` must be empty after execution.
    2.  **Record Count:** The record count reported by the legacy script and in `isbert_rpt_staging.job_run_log` must be 0.
    3.  **Job Status:** `isbert_rpt_staging.job_run_log` should show `status = 'SUCCESS'`.

---

### Test Case 8: Schema and Data Type Integrity

*   **Purpose:** Verify that the target `sof_ta_acc_ref` table in BigQuery has the correct schema, column names, and data types as defined in the DDL, and that data is stored without unexpected truncation or conversion errors. This covers schema assertions and data quality.
*   **Setup:**
    1.  **BigQuery:** Ensure `isbert_rpt_staging.sof_ta_acc_ref` exists.
    2.  **Legacy Oracle:** Obtain the DDL for `sof$ta_acc_ref` and its column definitions.
*   **Action:**
    1.  Inspect the schema of `isbert_rpt_staging.sof_ta_acc_ref` in BigQuery using `DESCRIBE TABLE`.
    2.  Run Test Case 1 (Happy Path) to populate the table, then query a sample of data to check actual values.
*   **Pass/Fail Criteria:**
    1.  **Column Names:** All column names in BigQuery `isbert_rpt_staging.sof_ta_acc_ref` must match the expected names from the Oracle source.
    2.  **Data Types:** BigQuery data types must be appropriate mappings of the Oracle types (e.g., Oracle `DATE` to BigQuery `DATE`, Oracle `VARCHAR2` to BigQuery `STRING`, Oracle `NUMBER` to BigQuery `INT64` or `BIGNUMERIC`).
    3.  **Data Integrity:** After a successful run (e.g., Test Case 1), query a sample of data from `isbert_rpt_staging.sof_ta_acc_ref` and verify that values are correctly stored without truncation, unexpected NULLs, or format issues.

    ```sql
    -- BigQuery SQL for Schema Inspection
    SELECT
        column_name,
        data_type
    FROM
        `isbert_rpt_staging`.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'sof_ta_acc_ref'
    ORDER BY
        ordinal_position;

    -- Expected output (example):
    -- column_name          data_type
    -- insert_at            DATE
    -- modified_at          DATE
    -- valid_from           DATE
    -- valid_to             DATE
    -- is_production        INT64
    -- ta_acc_ref_key       STRING
    -- ...
    ```

---