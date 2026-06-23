As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `k_ausd_v_ta_p_vertrag.ksh` to Google BigQuery. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

The tests are structured with a clear purpose, setup instructions (for both legacy and BigQuery environments), actions to perform, and concrete pass/fail criteria, including runnable SQL assertions where applicable.

---

## Migration Validation Tests for `k_ausd_v_ta_p_vertrag.ksh`

### Test Case 1: Successful Execution - Output Parity & Transformation Correctness

*   **Purpose:** Verify that the migrated BigQuery job successfully processes data and produces identical output to the legacy system when given valid inputs. This covers the core `INSERT ... SELECT` logic, `LEFT JOIN` conversion, and accurate record counting.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Ensure the Oracle `dwtk_meldungen` table contains at least one entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with a `timecreated` value (e.g., `2023-01-01 10:00:00`).
        *   Populate the Oracle `sof$ta_vertrag_tmp` table with a diverse set of contract data. This data should include cases that will result in both matching and non-matching `twin_vertrag_id` values for the `LEFT JOIN` with `vertrag_id_carmen`.
        *   Execute the legacy `k_ausd_v_ta_p_vertrag.ksh` script with valid parameters (e.g., `./k_ausd_v_ta_p_vertrag.ksh -j LEGACY_JOB_SUCCESS -f 12345`).
        *   Capture the final state of the Oracle `sof$ta_p_vertrag` table (all rows, all columns).
        *   Capture the record count reported in the temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_p_vertrag_$$.tmp`).
        *   Capture relevant entries from the legacy job management system (if applicable).
    2.  **BigQuery Environment:**
        *   Create and populate `my_dataset.dwtk_meldungen` with data mirroring the Oracle `dwtk_meldungen` table.
        *   Create and populate `my_dataset.sof_ta_vertrag_tmp` with data identical to the Oracle `sof$ta_vertrag_tmp` table.
        *   Ensure `my_dataset.sof_ta_p_vertrag` and all other temporary `sof_ta_` tables mentioned in `p_ausd_v_ta_p_vertrag_data_process` are empty.
        *   Ensure `my_dataset.job_table`, `my_dataset.job_run_log`, `my_dataset.job_error_log` are empty or in a known baseline state.
*   **Action:** Execute the BigQuery stored procedure:
    ```sql
    CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_SUCCESS', '12345');
    ```
*   **Pass/Fail Criteria:**
    1.  **Output Parity (Data):** The data in `my_dataset.sof_ta_p_vertrag` must be identical to the captured data from Oracle `sof$ta_p_vertrag`. This includes row count, column values, and data types.
        ```sql
        -- Example SQL assertion for row count
        SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag`;
        -- Expected: <Legacy_Oracle_Row_Count>

        -- Example SQL assertion for data comparison (requires a way to load legacy data into a temp BQ table)
        -- Assume `my_dataset.legacy_sof_ta_p_vertrag_snapshot` holds the legacy output
        SELECT
            (SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag`) = (SELECT COUNT(*) FROM `my_dataset.legacy_sof_ta_p_vertrag_snapshot`) AND
            (SELECT COUNT(*) FROM (
                SELECT * FROM `my_dataset.sof_ta_p_vertrag`
                EXCEPT DISTINCT
                SELECT * FROM `my_dataset.legacy_sof_ta_p_vertrag_snapshot`
            )) = 0 AND
            (SELECT COUNT(*) FROM (
                SELECT * FROM `my_dataset.legacy_sof_ta_p_vertrag_snapshot`
                EXCEPT DISTINCT
                SELECT * FROM `my_dataset.sof_ta_p_vertrag`
            )) = 0;
        -- Expected: TRUE
        ```
    2.  **Record Count:** The `records_processed` field in `my_dataset.job_run_log` for `job_id = 'BQ_JOB_SUCCESS'` must match the record count captured from the legacy temporary file.
        ```sql
        SELECT records_processed FROM `my_dataset.job_run_log` WHERE job_id = 'BQ_JOB_SUCCESS' AND status = 'SUCCESS';
        -- Expected: <Legacy_Temporary_File_Record_Count>
        ```
    3.  **Job Status:** `my_dataset.job_run_log` should show `status = 'SUCCESS'` for the run. `my_dataset.job_table` should show `status = 'COMPLETED'` for `job_id = 'BQ_JOB_SUCCESS'`.
        ```sql
        SELECT status FROM `my_dataset.job_run_log` WHERE job_id = 'BQ_JOB_SUCCESS' ORDER BY start_time DESC LIMIT 1;
        -- Expected: 'SUCCESS'
        SELECT status FROM `my_dataset.job_table` WHERE job_id = 'BQ_JOB_SUCCESS';
        -- Expected: 'COMPLETED'
        ```
    4.  **Temporary Table Truncation:** All temporary tables listed in `p_ausd_v_ta_p_vertrag_data_process` (e.g., `my_dataset.sof_ta_disc_zusgf`, `my_dataset.sof_ta_vertrag_tmp`) should be empty after the run.
        ```sql
        SELECT COUNT(*) FROM `my_dataset.sof_ta_vertrag_tmp`;
        -- Expected: 0
        -- Repeat for all other truncated tables.
        ```

### Test Case 2: Parameter Validation - Missing JobKennung

*   **Purpose:** Verify that the migrated job correctly handles a missing or empty `p_JobKennung` parameter, logs an error, and exits gracefully without processing any data.
*   **Setup:**
    1.  **Legacy Environment:** Execute `k_ausd_v_ta_p_vertrag.ksh` without the `-j` parameter (e.g., `./k_ausd_v_ta_p_vertrag.ksh -f 12345`). Capture the error message printed to stderr and the exit code.
    2.  **BigQuery Environment:**
        *   Populate `my_dataset.sof_ta_vertrag_tmp` with some data (to confirm no processing occurs).
        *   Ensure `my_dataset.job_table`, `my_dataset.job_run_log`, `my_dataset.job_error_log` are empty.
*   **Action:** Attempt to execute the BigQuery stored procedure with `NULL` or an empty string for `p_JobKennung`:
    ```sql
    -- Attempt 1: NULL p_JobKennung
    CALL `my_dataset.r_ausd_vertrag_control`(NULL, '12345');
    -- Attempt 2: Empty string p_JobKennung
    CALL `my_dataset.r_ausd_vertrag_control`('', '12345');
    ```
*   **Pass/Fail Criteria:**
    1.  **Error Handling:** Both calls should `RAISE` an error, preventing successful completion.
    2.  **Error Logging:** `my_dataset.job_error_log` should contain an entry with `error_message` similar to "Parameter p_JobKennung is missing or empty."
        ```sql
        SELECT COUNT(*) FROM `my_dataset.job_error_log` WHERE error_message LIKE '%p_JobKennung is missing or empty%';
        -- Expected: 2 (one for NULL, one for empty string)
        ```
    3.  **Job Status:** `my_dataset.job_run_log` should show `status = 'FAILURE'` for the attempted runs. `my_dataset.job_table` should show `status = 'FAILED'` for any `job_id` that might have been implicitly created before the validation failed (e.g., if an empty string was passed).
        ```sql
        SELECT COUNT(*) FROM `my_dataset.job_run_log` WHERE status = 'FAILURE' AND message LIKE '%p_JobKennung is missing or empty%';
        -- Expected: 2
        ```
    4.  **No Data Processing:** `my_dataset.sof_ta_p_vertrag` must remain empty (or unchanged from its initial state).
        ```sql
        SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag`;
        -- Expected: 0
        ```

### Test Case 3: Parameter Validation - Missing EintragsNr

*   **Purpose:** Verify that the migrated job correctly handles a missing or empty `p_EintragsNr` parameter, logs an error, and exits gracefully without processing any data.
*   **Setup:**
    1.  **Legacy Environment:** Execute `k_ausd_v_ta_p_vertrag.ksh` without the `-f` parameter (e.g., `./k_ausd_v_ta_p_vertrag.ksh -j LEGACY_JOB_FAIL_F`). Capture the error message printed to stderr and the exit code.
    2.  **BigQuery Environment:**
        *   Populate `my_dataset.sof_ta_vertrag_tmp` with some data.
        *   Ensure `my_dataset.job_table`, `my_dataset.job_run_log`, `my_dataset.job_error_log` are empty.
*   **Action:** Attempt to execute the BigQuery stored procedure with `NULL` or an empty string for `p_EintragsNr`:
    ```sql
    -- Attempt 1: NULL p_EintragsNr
    CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_FAIL_F', NULL);
    -- Attempt 2: Empty string p_EintragsNr
    CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_FAIL_F', '');
    ```
*   **Pass/Fail Criteria:**
    1.  **Error Handling:** Both calls should `RAISE` an error.
    2.  **Error Logging:** `my_dataset.job_error_log` should contain entries for `job_id = 'BQ_JOB_FAIL_F'` with `error_message` similar to "Parameter p_EintragsNr is missing or empty."
        ```sql
        SELECT COUNT(*) FROM `my_dataset.job_error_log` WHERE job_id = 'BQ_JOB_FAIL_F' AND error_message LIKE '%p_EintragsNr is missing or empty%';
        -- Expected: 2
        ```
    3.  **Job Status:** `my_dataset.job_run_log` should show `status = 'FAILURE'` for the runs. `my_dataset.job_table` should show `status = 'FAILED'` for `job_id = 'BQ_JOB_FAIL_F'`.
        ```sql
        SELECT COUNT(*) FROM `my_dataset.job_run_log` WHERE job_id = 'BQ_JOB_FAIL_F' AND status = 'FAILURE';
        -- Expected: 2
        SELECT status FROM `my_dataset.job_table` WHERE job_id = 'BQ_JOB_FAIL_F';
        -- Expected: 'FAILED'
        ```
    4.  **No Data Processing:** `my_dataset.sof_ta_p_vertrag` must remain empty (or unchanged).
        ```sql
        SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag`;
        -- Expected: 0
        ```

### Test Case 4: Job Management - Deactivation of Old Active Jobs

*   **Purpose:** Verify that the migrated job correctly deactivates any previously 'ACTIVE' jobs with the same `p_JobKennung` before registering itself. This ensures proper job state management.
*   **Setup:**
    1.  **Legacy Environment:** Simulate a scenario where a job with the same `JobKennung` is already active in the legacy job management system. Run the legacy script and observe its behavior (it should deactivate the old one).
    2.  **BigQuery Environment:**
        *   Insert an entry into `my_dataset.job_table` with `job_id = 'BQ_JOB_DEACTIVATE'`, `status = 'ACTIVE'`, and an old `start_time` (e.g., `CURRENT_TIMESTAMP() - INTERVAL 1 DAY`).
        *   Populate source tables (`my_dataset.dwtk_meldungen`, `my_dataset.sof_ta_vertrag_tmp`) for successful data processing.
        *   Ensure `my_dataset.job_run_log` and `my_dataset.job_error_log` are empty.
*   **Action:** Execute the BigQuery stored procedure with `p_JobKennung = 'BQ_JOB_DEACTIVATE'` and valid `p_EintragsNr`:
    ```sql
    CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_DEACTIVATE', '67890');
    ```
*   **Pass/Fail Criteria:**
    1.  **Job Table Update:** After the call, `my_dataset.job_table` should contain two entries for `job_id = 'BQ_JOB_DEACTIVATE'`:
        *   One with `status = 'INACTIVE'` and an `last_update_time` reflecting the deactivation.
        *   One with `status = 'COMPLETED'` (assuming successful run) and a `start_time` and `end_time` for the current run.
        ```sql
        SELECT status, COUNT(*) FROM `my_dataset.job_table` WHERE job_id = 'BQ_JOB_DEACTIVATE' GROUP BY status;
        -- Expected:
        -- status      | COUNT(*)
        -- ------------|---------
        -- INACTIVE    | 1
        -- COMPLETED   | 1
        ```
    2.  **Data Processing:** `my_dataset.sof_ta_p_vertrag` should contain the expected processed data (as verified in Test Case 1).

### Test Case 5: Transformation Correctness - NULL Handling and Edge Cases in JOIN

*   **Purpose:** Verify that the `LEFT JOIN` logic in `p_ausd_v_ta_p_vertrag_data_process` correctly handles cases where `v.twin_vertrag_id` does not find a match in `pv.vertrag_id_carmen`, resulting in `NULL` values for `pv` columns. Also, test with an empty source table.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Populate Oracle `sof$ta_vertrag_tmp` with:
            *   Rows where `twin_vertrag_id` has a match in `vertrag_id_carmen`.
            *   Rows where `twin_vertrag_id` has NO match in `vertrag_id_carmen`.
            *   Rows where `twin_vertrag_id` is `NULL`.
            *   Rows where `vertrag_id_carmen` is `NULL` (if relevant for the join key).
        *   Run the legacy script and capture the final `sof$ta_p_vertrag` output.
    2.  **BigQuery Environment:**
        *   Populate `my_dataset.sof_ta_vertrag_tmp` with data identical to the Oracle `sof$ta_vertrag_tmp` for the above scenarios.
        *   Ensure `my_dataset.sof_ta_p_vertrag` is empty.
*   **Action:** Execute the BigQuery stored procedure:
    ```sql
    CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_NULL_JOIN', '11111');
    ```
*   **Pass/Fail Criteria:**
    1.  **Output Parity:** The data in `my_dataset.sof_ta_p_vertrag` must be identical to the captured data from Oracle `sof$ta_p_vertrag`, specifically verifying that `NULL` values resulting from the `LEFT JOIN` are correctly propagated to the target table.
        ```sql
        -- Use the same data comparison assertion as in Test Case 1.
        ```
    2.  **Empty Source Table Scenario:** If `my_dataset.sof_ta_vertrag_tmp` is empty, `my_dataset.sof_ta_p_vertrag` should also be empty, and `records_processed` in `job_run_log` should be 0.
        ```sql
        -- To test this, repeat the test with an empty `my_dataset.sof_ta_vertrag_tmp`.
        SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag`;
        -- Expected: 0
        SELECT records_processed FROM `my_dataset.job_run_log` WHERE job_id = 'BQ_JOB_NULL_JOIN_EMPTY_SRC';
        -- Expected: 0
        ```

### Test Case 6: External System Replacement - `dwtk_meldungen` Date Derivation

*   **Purpose:** Verify that the `v_datum` variable, derived from `my_dataset.dwtk_meldungen` using `MAX(m.timecreated)`, is correctly calculated within `p_ausd_v_ta_p_vertrag_data_process`. Although `v_datum` is not directly used in the provided `INSERT` statement, its correct derivation is a transformation correctness point.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Populate Oracle `dwtk_meldungen` with multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, ensuring different `timecreated` values (e.g., `2023-01-01`, `2023-01-15`, `2022-12-01`).
        *   Determine the `v_datum` value that the legacy script would derive (it should be `FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated))`).
    2.  **BigQuery Environment:**
        *   Populate `my_dataset.dwtk_meldungen` with identical data.
        *   Populate `my_dataset.sof_ta_vertrag_tmp` with data for a successful run.
        *   *Temporary Modification (for testing `v_datum` directly):* Add a temporary `INSERT` into `job_run_log` or a dedicated debug table within `p_ausd_v_ta_p_vertrag_data_process` to log the calculated `v_datum`.
            ```sql
            -- Inside p_ausd_v_ta_p_vertrag_data_process, after SET v_datum = ...
            INSERT INTO `my_dataset.job_run_log` (run_id, job_id, message) VALUES (GENERATE_UUID(), p_job_kennung, 'Derived v_datum: ' || v_datum);
            ```
*   **Action:** Execute the BigQuery stored procedure:
    ```sql
    CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_DATE_DERIV', '22222');
    ```
*   **Pass/Fail Criteria:**
    1.  **`v_datum` Correctness:** Query the `job_run_log` (or debug table) for the logged `v_datum`. It must match the `FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated))` from the `my_dataset.dwtk_meldungen` table for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. If `dwtk_meldungen` is empty for that `job_kennung`, `v_datum` should be '19000101'.
        ```sql
        -- Assuming temporary logging of v_datum in job_run_log.message
        SELECT message FROM `my_dataset.job_run_log` WHERE job_id = 'BQ_JOB_DATE_DERIV' AND message LIKE 'Derived v_datum:%';
        -- Expected: 'Derived v_datum: <Expected_Date_YYYYMMDD>'
        ```
    2.  **Output Parity (Implicit):** If `v_datum` were to influence the output data, successful output parity (as in Test Case 1) would implicitly confirm its correct derivation.

### Test Case 7: Data Quality - Schema and Data Type Assertions

*   **Purpose:** Verify that the schema and data types of the target table `my_dataset.sof_ta_p_vertrag` match the expected schema and data types, and that no data truncation or unexpected type conversions occur.
*   **Setup:**
    1.  **Legacy Environment:** Obtain the precise schema (column names, data types, nullability, lengths/precision) of Oracle `sof$ta_p_vertrag`.
    2.  **BigQuery Environment:**
        *   Ensure `my_dataset.sof_ta_vertrag_tmp` is populated with data that covers various data types and potential edge cases (e.g., max length strings, boundary numbers, dates, `NULL` values).
        *   Ensure `my_dataset.sof_ta_p_vertrag` is empty.
*   **Action:** Execute the BigQuery stored procedure (as in Test Case 1):
    ```sql
    CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_SCHEMA_DQ', '33333');
    ```
*   **Pass/Fail Criteria:**
    1.  **Schema Match:** The schema of `my_dataset.sof_ta_p_vertrag` (column names, data types, nullability) must match the legacy Oracle `sof$ta_p_vertrag` schema.
        ```sql
        -- Example SQL to retrieve BigQuery schema
        SELECT column_name, data_type, is_nullable
        FROM `my_dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'sof_ta_p_vertrag'
        ORDER BY ordinal_position;
        -- Compare this output to the documented Oracle schema.
        ```
    2.  **Data Type Integrity:** Query `my_dataset.sof_ta_p_vertrag` and verify that data types are correct and no data loss or unexpected conversions occurred (e.g., string fields are not truncated, numeric fields retain precision, date/timestamp fields are correctly formatted).
        ```sql
        -- Example: Check string length for a specific column
        SELECT MAX(LENGTH(vertrag_id_carmen)) FROM `my_dataset.sof_ta_p_vertrag`;
        -- Expected: Should not exceed the defined column length in BQ, and should match legacy max length.

        -- Example: Check for unexpected NULLs in NOT NULL columns
        SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag` WHERE vertrag_id_carmen IS NULL; -- Assuming this is NOT NULL
        -- Expected: 0
        ```
    3.  **NULLability:** Verify that columns expected to be `NOT NULL` are indeed `NOT NULL`, and columns that can be `NULL` (e.g., from `LEFT JOIN` where no match is found) correctly store `NULL`s.

### Test Case 8: Idempotency - Rerunning the Job

*   **Purpose:** Verify that running the job multiple times with the same parameters (`p_JobKennung`, `p_EintragsNr`) behaves as expected, specifically regarding job status updates and data processing. The design explicitly mentions "deactivates old active jobs".
*   **Setup:**
    1.  **BigQuery Environment:**
        *   Populate source tables (`my_dataset.dwtk_meldungen`, `my_dataset.sof_ta_vertrag_tmp`) for successful data processing.
        *   Ensure `my_dataset.job_table`, `my_dataset.job_run_log`, `my_dataset.job_error_log` are empty.
*   **Action:**
    1.  Execute the BigQuery stored procedure for the first time:
        ```sql
        CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_IDEMPOTENT', '44444');
        ```
    2.  Execute the BigQuery stored procedure for the second time with the *same* parameters:
        ```sql
        CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_IDEMPOTENT', '44444');
        ```
*   **Pass/Fail Criteria:**
    1.  **Job Table:** `my_dataset.job_table` should contain two entries for `job_id = 'BQ_JOB_IDEMPOTENT'`:
        *   One with `status = 'INACTIVE'` (the first run, deactivated by the second).
        *   One with `status = 'COMPLETED'` (the second run).
        ```sql
        SELECT status, COUNT(*) FROM `my_dataset.job_table` WHERE job_id = 'BQ_JOB_IDEMPOTENT' GROUP BY status;
        -- Expected:
        -- status      | COUNT(*)
        -- ------------|---------
        -- INACTIVE    | 1
        -- COMPLETED   | 1
        ```
    2.  **Job Run Log:** `my_dataset.job_run_log` should contain two entries for `job_id = 'BQ_JOB_IDEMPOTENT'`, both with `status = 'SUCCESS'` and the same `records_processed` count.
        ```sql
        SELECT status, records_processed, COUNT(*) FROM `my_dataset.job_run_log` WHERE job_id = 'BQ_JOB_IDEMPOTENT' GROUP BY status, records_processed;
        -- Expected:
        -- status      | records_processed | COUNT(*)
        -- ------------|-------------------|---------
        -- SUCCESS     | <Expected_Count>  | 2
        ```
    3.  **Output Data:** The final state of `my_dataset.sof_ta_p_vertrag` should be identical after both runs. Since the procedure `TRUNCATE`s the target table, the data should be the same as if it was run only once.
        ```sql
        SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag`;
        -- Expected: <Expected_Count_from_single_run>
        ```

### Test Case 9: Error during Data Processing

*   **Purpose:** Verify that if an error occurs during the `p_ausd_v_ta_p_vertrag_data_process` procedure (e.g., due to data type mismatch, constraint violation, or an explicit `RAISE`), the control procedure correctly catches it, logs it, and marks the job as `FAILED`.
*   **Setup:**
    1.  **BigQuery Environment:**
        *   Create a scenario that will cause `p_ausd_v_ta_p_vertrag_data_process` to fail. For example:
            *   Modify the schema of `my_dataset.sof_ta_p_vertrag` to have a `NOT NULL` constraint on a column that `p_ausd_v_ta_p_vertrag_data_process` attempts to insert `NULL` into.
            *   Modify the schema of a string column in `my_dataset.sof_ta_p_vertrag` to be too short for the input data from `my_dataset.sof_ta_vertrag_tmp`.
            *   *Alternatively (for controlled testing):* Temporarily modify `p_ausd_v_ta_p_vertrag_data_process` to include a `RAISE` statement after the `TRUNCATE` but before the `INSERT`.
        *   Populate source tables (`my_dataset.dwtk_meldungen`, `my_dataset.sof_ta_vertrag_tmp`) with data that triggers the error.
        *   Ensure `my_dataset.job_table`, `my_dataset.job_run_log`, `my_dataset.job_error_log` are empty.
*   **Action:** Execute the BigQuery stored procedure with parameters that trigger the error:
    ```sql
    CALL `my_dataset.r_ausd_vertrag_control`('BQ_JOB_DATA_ERROR', '55555');
    ```
*   **Pass/Fail Criteria:**
    1.  **Error Handling:** The call should `RAISE` an error to the caller, indicating job failure.
    2.  **Error Logging:** `my_dataset.job_error_log` should contain an entry for `job_id = 'BQ_JOB_DATA_ERROR'` with a detailed `error_message` and `error_detail` reflecting the underlying data processing error.
        ```sql
        SELECT COUNT(*) FROM `my_dataset.job_error_log` WHERE job_id = 'BQ_JOB_DATA_ERROR' AND error_message IS NOT NULL;
        -- Expected: 1
        SELECT error_message, error_detail FROM `my_dataset.job_error_log` WHERE job_id = 'BQ_JOB_DATA_ERROR';
        -- Expected: error_message and error_detail should contain relevant BigQuery error information.
        ```
    3.  **Job Status:** `my_dataset.job_run_log` should show `status = 'FAILURE'`. `my_dataset.job_table` should show `status = 'FAILED'` for `job_id = 'BQ_JOB_DATA_ERROR'`.
        ```sql
        SELECT status FROM `my_dataset.job_run_log` WHERE job_id = 'BQ_JOB_DATA_ERROR';
        -- Expected: 'FAILURE'
        SELECT status FROM `my_dataset.job_table` WHERE job_id = 'BQ_JOB_DATA_ERROR';
        -- Expected: 'FAILED'
        ```
    4.  **Data State:** `my_dataset.sof_ta_p_vertrag` should be in a consistent state. Given the `TRUNCATE` at the beginning of `p_ausd_v_ta_p_vertrag_data_process`, if the error occurs during the `INSERT`, the table should ideally remain empty due to BigQuery's transactional behavior.
        ```sql
        SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag`;
        -- Expected: 0
        ```