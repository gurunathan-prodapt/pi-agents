As a senior data-migration QA engineer, I have developed a comprehensive suite of validation tests for the migration of `k_ausd_v_ta_discount_rr.ksh` to Google Cloud BigQuery. These tests are designed to ensure the migrated solution is functionally equivalent, robust, and adheres to the specified design.

---

## Migration Validation Tests: `k_ausd_v_ta_discount_rr.ksh` to BigQuery

### 1. Orchestration & Control Tests

These tests verify the correct functioning of the BigQuery stored procedure (`control_ausd_v_ta_discount_rr`) which replaces the KornShell script's orchestration logic.

#### Test Case 1.1: Successful Job Execution (Happy Path)

*   **Purpose:** Verify that the BigQuery stored procedure executes successfully with valid parameters, processes data, and updates the job status and record count correctly in the `job_table`.
*   **Setup:**
    1.  Ensure all source BigQuery tables (`raw_isbert.dwtk_meldungen`, `raw_isbert.cds_ta_discount_bc_assoc`, etc.) are populated with a representative dataset that should result in some records being inserted into the target table.
    2.  `raw_isbert.dwtk_meldungen` should contain at least one entry with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and a `timecreated` value (e.g., `2023-01-15 10:00:00 UTC`).
    3.  The target table `curated_rpt.sof_ta_discount_rr` should be empty.
    4.  The `job_table` and `job_error_log` should be empty.
*   **Action:** Execute the BigQuery stored procedure with valid `p_JobKennung` and `p_EintragsNr`.

    ```sql
    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => 'TEST_JOB_DISCOUNT_RR',
        p_EintragsNr => '12345'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes without error.
    2.  The `your_project.your_dataset.job_table` contains one entry for `TEST_JOB_DISCOUNT_RR` with `status = 'COMPLETED'`, `start_time` and `end_time` populated, and `record_count` reflecting the number of rows inserted into `curated_rpt.sof_ta_discount_rr`.
    3.  The `your_project.your_dataset.job_error_log` table remains empty.
    4.  The `curated_rpt.sof_ta_discount_rr` table contains the expected number of rows.

    ```sql
    -- Pytest assertion (conceptual)
    def test_successful_job_execution():
        # ... setup_source_data ...
        initial_target_count = get_table_row_count(CURATED_DATASET, 'sof_ta_discount_rr')
        assert initial_target_count == 0

        call_stored_procedure('TEST_JOB_DISCOUNT_RR', '12345')

        job_entry = get_job_table_entry('TEST_JOB_DISCOUNT_RR', '12345')
        assert job_entry['status'] == 'COMPLETED'
        assert job_entry['start_time'] is not None
        assert job_entry['end_time'] is not None
        assert job_entry['record_count'] > 0 # Assuming data was inserted

        final_target_count = get_table_row_count(CURATED_DATASET, 'sof_ta_discount_rr')
        assert final_target_count == job_entry['record_count']

        error_log_count = get_table_row_count(CONTROL_DATASET, 'job_error_log')
        assert error_log_count == 0
    ```

#### Test Case 1.2: Job Execution with Missing `p_JobKennung` Parameter

*   **Purpose:** Verify that the stored procedure correctly handles missing mandatory parameters, logs an error, and marks the job as `FAILED`.
*   **Setup:**
    1.  Ensure `job_table` and `job_error_log` are empty.
*   **Action:** Attempt to execute the stored procedure without providing `p_JobKennung`.

    ```sql
    -- This will cause a BigQuery SQL error for missing parameter
    -- CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(p_EintragsNr => '12345');
    -- Instead, simulate the condition where p_JobKennung is passed as NULL or empty string
    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => NULL,
        p_EintragsNr => '12345'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution fails and returns an error message indicating `p_JobKennung` cannot be NULL or empty.
    2.  The `your_project.your_dataset.job_table` contains one entry with `status = 'FAILED'`, `start_time` populated, `end_time` populated, and `record_count = 0`.
    3.  The `your_project.your_dataset.job_error_log` contains one entry with `severity = 'ERROR'`, `step = 'Parameter Validation'`, and an appropriate error message.

    ```sql
    -- Pytest assertion (conceptual)
    def test_job_execution_missing_jobkennung():
        # ... clear job_table and job_error_log ...
        with pytest.raises(Exception) as excinfo:
            call_stored_procedure(None, '12345') # Simulate passing NULL

        assert "p_JobKennung cannot be NULL or empty" in str(excinfo.value)

        job_entry = get_job_table_entry(None, '12345') # Need to query by run_id or similar if job_kennung is NULL
        assert job_entry['status'] == 'FAILED'
        assert job_entry['record_count'] == 0

        error_log_entry = get_error_log_entry(job_entry['run_id'])
        assert error_log_entry['severity'] == 'ERROR'
        assert error_log_entry['step'] == 'Parameter Validation'
        assert "p_JobKennung cannot be NULL or empty" in error_log_entry['error_message']
    ```

#### Test Case 1.3: Job Status and Record Count Logging

*   **Purpose:** Verify that the `job_table` accurately reflects the job's lifecycle (start, completion/failure) and the final record count.
*   **Setup:**
    1.  Populate source tables as in Test 1.1.
    2.  Ensure `job_table` and `job_error_log` are empty.
*   **Action:** Execute the stored procedure with valid parameters.
*   **Pass/Fail Criterion:**
    1.  Before the procedure completes, if possible to inspect (e.g., in a separate session if the procedure is long-running), the `job_table` should show `status = 'RUNNING'`.
    2.  Upon completion, the `job_table` entry for the specific `run_id` shows `status = 'COMPLETED'`, `start_time` and `end_time` are recorded, and `record_count` matches the actual number of rows in `curated_rpt.sof_ta_discount_rr`.
    3.  If an error occurs during transformation (e.g., due to bad data), the `job_table` should show `status = 'FAILED'` and `record_count = 0` (or the count at the point of failure if partial inserts are committed).
*   **Runnable Code (Conceptual):** See Test 1.1 for successful path. For failure, induce an error in the transformation logic (e.g., by making a required join column NULL in source data, which would cause an error if not handled).

#### Test Case 1.4: Error Logging on Transformation Failure

*   **Purpose:** Verify that any errors during the data transformation phase are caught, logged to `job_error_log`, and the main `job_table` reflects a `FAILED` status.
*   **Setup:**
    1.  Populate source tables such that the transformation logic will encounter an error (e.g., a data type mismatch if a column expected to be `NUMERIC` contains non-numeric strings, or a `NOT NULL` constraint violation if one were added to the target table). For this test, let's assume a scenario where `dv.CALC_RULE_VALUE` in `cds_ta_disc_vector` contains a non-numeric string, which would cause a BigQuery error when trying to insert into a `NUMERIC` column.
    2.  Ensure `job_table` and `job_error_log` are empty.
*   **Action:** Execute the stored procedure with parameters that trigger the error condition.

    ```sql
    -- Example: Insert bad data into cds_ta_disc_vector to trigger an error
    INSERT INTO `your_project.raw_isbert.cds_ta_disc_vector` (CALC_RULE_VALUE, discount_id, disc_vector_ty, discount_obj_version, insert_at, modified_at)
    VALUES ('NOT_A_NUMBER', 1, 'TYPE_A', 1, CURRENT_TIMESTAMP(), NULL);

    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => 'TEST_JOB_FAILURE',
        p_EintragsNr => '67890'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution fails.
    2.  The `your_project.your_dataset.job_table` contains one entry for `TEST_JOB_FAILURE` with `status = 'FAILED'`, `start_time` and `end_time` populated, and `record_count = 0`.
    3.  The `your_project.your_dataset.job_error_log` contains one entry with `severity = 'ERROR'`, `step = 'Execute Transformation Logic'`, and an error message detailing the transformation failure (e.g., "Invalid NUMERIC value: 'NOT_A_NUMBER'").

### 2. Data Transformation Tests (Output Parity & Correctness)

These tests focus on the core SQL logic, ensuring that the BigQuery translation produces identical results to the Oracle source given the same input data.

#### Test Case 2.1: Full Data Parity (End-to-End)

*   **Purpose:** The ultimate test of transformation correctness: ensure that for a given set of source data, the migrated BigQuery job produces an identical target table to the legacy Oracle job.
*   **Setup:**
    1.  **Legacy System:** Run the original `k_ausd_v_ta_discount_rr.ksh` job against a known, representative snapshot of Oracle source data. Capture the exact state of the target table `sof$ta_discount_rr` after this run. (This requires access to the legacy system).
    2.  **BigQuery System:** Ingest the *exact same* snapshot of source data into the BigQuery `raw_isbert` tables.
    3.  Ensure `curated_rpt.sof_ta_discount_rr` is empty.
*   **Action:** Execute the BigQuery stored procedure with appropriate parameters.

    ```sql
    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => 'PARITY_TEST',
        p_EintragsNr => 'SNAPSHOT_1'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The BigQuery `curated_rpt.sof_ta_discount_rr` table contains the exact same number of rows as the Oracle `sof$ta_discount_rr` table.
    2.  Every column in every row of the BigQuery target table matches its corresponding column and row in the Oracle target table. This includes data types, values, and NULL handling.

    ```sql
    -- Pytest assertion (conceptual)
    def test_full_data_parity():
        # Assume oracle_target_data is loaded from the legacy system
        # Assume bq_source_data is loaded into BigQuery raw tables

        call_stored_procedure('PARITY_TEST', 'SNAPSHOT_1')

        bq_target_data = get_table_data(CURATED_DATASET, 'sof_ta_discount_rr', order_by_cols=['cntrct_id', 'discount_id'])
        oracle_target_data = load_oracle_target_data(order_by_cols=['cntrct_id', 'discount_id']) # Placeholder for legacy data retrieval

        assert len(bq_target_data) == len(oracle_target_data)

        # Detailed row-by-row, column-by-column comparison
        for i in range(len(bq_target_data)):
            bq_row = bq_target_data[i]
            oracle_row = oracle_target_data[i]
            for col_name in bq_row.keys(): # Assuming column names match
                assert bq_row[col_name] == oracle_row[col_name], \
                    f"Mismatch in row {i}, column {col_name}: BQ={bq_row[col_name]}, Oracle={oracle_row[col_name]}"

    -- SQL assertion for row count and checksum (if direct comparison is hard)
    -- Run this after the BQ job and after getting Oracle's checksum/count
    SELECT COUNT(*) FROM `your_project.curated_rpt.sof_ta_discount_rr`;
    -- Compare with Oracle's COUNT(*)

    -- For checksum, you'd need a consistent way to hash rows, e.g.,
    -- SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) FROM `your_project.curated_rpt.sof_ta_discount_rr` ORDER BY 1;
    -- Compare with Oracle's equivalent checksum (e.g., DBMS_CRYPTO.HASH or custom hash)
    ```

#### Test Case 2.2: `v_datum` Determination - Standard Case

*   **Purpose:** Verify that the `v_datum_str` variable is correctly determined from `raw_isbert.dwtk_meldungen` when a matching entry exists.
*   **Setup:**
    1.  Populate `raw_isbert.dwtk_meldungen` with multiple entries, including one with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and a specific `timecreated` (e.g., `2023-03-10 12:00:00 UTC`).
    2.  Ensure other `dwtk_meldungen` entries have different `job_kennung` or earlier `timecreated` values.
    3.  The target table `curated_rpt.sof_ta_discount_rr` should be empty.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  The `v_datum_str` used internally by the stored procedure (which can be verified by inspecting logs or by adding a temporary `SELECT v_datum_str` statement within the procedure for testing) should be `'20230310'`.
    2.  The records inserted into `curated_rpt.sof_ta_discount_rr` should reflect filtering based on this `v_datum_str`.
*   **Runnable Code (Conceptual):**
    ```sql
    -- Setup:
    TRUNCATE TABLE `your_project.raw_isbert.dwtk_meldungen`;
    INSERT INTO `your_project.raw_isbert.dwtk_meldungen` (timecreated, job_kennung) VALUES
    ('2023-03-01 08:00:00 UTC', 'OTHER_JOB'),
    ('2023-03-10 12:00:00 UTC', 'BERT_DROP_TEMP_TABLE'),
    ('2023-03-05 15:00:00 UTC', 'BERT_DROP_TEMP_TABLE'); -- Max should be 2023-03-10

    -- Action:
    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => 'V_DATUM_TEST',
        p_EintragsNr => '1'
    );

    -- Pass/Fail Criterion (internal check, or by observing filter effects):
    -- The v_datum_str should be '20230310'.
    -- If you can't inspect internal variables, you'd verify by checking the data in sof_ta_discount_rr
    -- and ensuring only records <= 2023-03-10 are included, and records > 2023-03-10 are excluded.
    ```

#### Test Case 2.3: `v_datum` Determination - No Matching Entry

*   **Purpose:** Verify that `v_datum_str` defaults to `'19000101'` if no `dwtk_meldungen` entry matches `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
*   **Setup:**
    1.  Ensure `raw_isbert.dwtk_meldungen` is either empty or contains no entries with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  The target table `curated_rpt.sof_ta_discount_rr` should be empty.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  The `v_datum_str` used internally should be `'19000101'`.
    2.  The `job_error_log` should contain a `WARNING` entry indicating "No processing date found, defaulting to 19000101."
    3.  The records inserted into `curated_rpt.sof_ta_discount_rr` should reflect filtering based on `'19000101'`.
*   **Runnable Code (Conceptual):**
    ```sql
    -- Setup:
    TRUNCATE TABLE `your_project.raw_isbert.dwtk_meldungen`;
    INSERT INTO `your_project.raw_isbert.dwtk_meldungen` (timecreated, job_kennung) VALUES
    ('2023-01-01 08:00:00 UTC', 'ANOTHER_JOB');

    -- Action:
    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => 'V_DATUM_DEFAULT_TEST',
        p_EintragsNr => '2'
    );

    -- Pass/Fail Criterion (check job_error_log and filter effects):
    SELECT error_message, severity FROM `your_project.your_dataset.job_error_log` WHERE job_kennung = 'V_DATUM_DEFAULT_TEST';
    -- Expected: 'No processing date found, defaulting to 19000101.', 'WARNING'
    -- Verify that only records with dates <= 1900-01-01 are processed.
    ```

#### Test Case 2.4: `TRUNCATE TABLE` Pre-processing

*   **Purpose:** Verify that the target table `curated_rpt.sof_ta_discount_rr` is truncated before new data is inserted.
*   **Setup:**
    1.  Populate `curated_rpt.sof_ta_discount_rr` with some dummy data (e.g., 5 rows).
    2.  Populate source tables such that the job will insert a different number of rows (e.g., 3 rows).
    3.  `raw_isbert.dwtk_meldungen` has a valid `v_datum` entry.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  After the procedure completes, the `curated_rpt.sof_ta_discount_rr` table should contain only the newly inserted rows (3 rows in this example), not the sum of old and new rows.
    2.  The `record_count` in `job_table` should match the number of newly inserted rows.
*   **Runnable Code (Conceptual):**
    ```sql
    -- Setup:
    INSERT INTO `your_project.curated_rpt.sof_ta_discount_rr` (cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, cntrct_template_id, disc_invoice_item_id, rabatt, rabatthoehe, rabattierte_rech_pos) VALUES
    (100, 1, 'TYPE_X', 1, 1, 1, 'Old Discount A', 10.0, 'Old Item A'),
    (101, 2, 'TYPE_Y', 1, 2, 2, 'Old Discount B', 20.0, 'Old Item B');
    -- (Add more dummy data to make it 5 rows)

    -- Ensure source data will produce, say, 3 new rows.
    -- Action:
    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => 'TRUNCATE_TEST',
        p_EintragsNr => '3'
    );

    -- Pass/Fail Criterion:
    SELECT COUNT(*) FROM `your_project.curated_rpt.sof_ta_discount_rr`;
    -- Expected: 3 (the number of rows inserted by the job, not 5+3=8)
    ```

#### Test Case 2.5: Join Logic Correctness

*   **Purpose:** Verify that all `INNER JOIN` conditions are correctly translated and function as expected, ensuring only matching records are processed.
*   **Setup:**
    1.  Populate source tables with data that includes:
        *   Records that match all join conditions.
        *   Records that fail one or more join conditions (e.g., `cds_ta_discount_bc_assoc` entry with `discount_id` not present in `cds_ta_discount`).
        *   Records with NULL values in join keys (these should be excluded by `INNER JOIN`).
    2.  Set `v_datum` to a value that doesn't interfere with the join test (e.g., a very old date or a future date if `insert_at` is future).
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  Only records that satisfy *all* join conditions are present in `curated_rpt.sof_ta_discount_rr`.
    2.  Records that fail any join condition are correctly excluded.
    3.  Records with NULL join keys are excluded.
*   **Runnable Code (Conceptual):**
    ```sql
    -- Setup:
    -- Insert data into all raw tables.
    -- Example:
    -- cds_ta_discount_bc_assoc: (cntrct_id=1, discount_id=10, ...)
    -- cds_ta_discount: (discount_id=10, cds_description_id=100, ...)
    -- cds_ta_care_description: (cds_description_id=100, language=1, ...)
    -- cds_ta_disc_invoice_item: (disc_invoice_item_id=1000, cds_description_id=200, ...)
    -- cds_ta_care_description: (cds_description_id=200, language=1, ...)
    -- cds_ta_disc_vector: (discount_id=10, disc_vector_ty='A', discount_obj_version=1, ...)
    -- This set should produce 1 row.

    -- Add a record in cds_ta_discount_bc_assoc with discount_id=99 (not in cds_ta_discount)
    INSERT INTO `your_project.raw_isbert.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES
    (2, 99, 1, '2023-01-01 00:00:00 UTC', NULL);

    -- Action:
    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => 'JOIN_TEST',
        p_EintragsNr => '4'
    );

    -- Pass/Fail Criterion:
    SELECT COUNT(*) FROM `your_project.curated_rpt.sof_ta_discount_rr`;
    -- Expected: 1 (the record with discount_id=99 should be excluded)
    ```

#### Test Case 2.6: Filter Logic - `language`

*   **Purpose:** Verify that the `cd.language = 1` and `cdii.language = 1` filters are correctly applied.
*   **Setup:**
    1.  Populate source tables with data where:
        *   `cds_ta_care_description` entries have `language = 1` (should be included).
        *   `cds_ta_care_description` entries have `language != 1` (should be excluded).
    2.  Ensure other filters (`v_datum`, `is_production`) are set to allow these records through.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  Only records associated with `cds_description_id`s where `language = 1` (for both `cd` and `cdii` aliases) are present in `curated_rpt.sof_ta_discount_rr`.
    2.  Records with `language != 1` are correctly excluded.

#### Test Case 2.7: Filter Logic - `insert_at` and `modified_at` (Boundary Conditions)

*   **Purpose:** Verify the correct application of date filters `col.insert_at <= v_datum_str` and `(col.modified_at IS NULL OR col.modified_at > v_datum_str)`. Test boundary conditions.
*   **Setup:**
    1.  Set `v_datum_str` to a specific date (e.g., `'20230115'`).
    2.  Populate source tables with `insert_at` and `modified_at` values around this `v_datum_str`:
        *   `insert_at` on `v_datum_str` (e.g., `2023-01-15 00:00:00 UTC`) - should be included.
        *   `insert_at` just before `v_datum_str` (e.g., `2023-01-14 23:59:59 UTC`) - should be included.
        *   `insert_at` just after `v_datum_str` (e.g., `2023-01-15 00:00:01 UTC`) - should be excluded.
        *   `modified_at` on `v_datum_str` (e.g., `2023-01-15 00:00:00 UTC`) - should be excluded (`> v_datum_str` is false).
        *   `modified_at` just after `v_datum_str` (e.g., `2023-01-15 00:00:01 UTC`) - should be included.
        *   `modified_at` just before `v_datum_str` (e.g., `2023-01-14 23:59:59 UTC`) - should be excluded.
        *   `modified_at IS NULL` - should be included.
    3.  Ensure other filters allow these records through.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  Only records satisfying all `insert_at` and `modified_at` conditions relative to `v_datum_str` are present in `curated_rpt.sof_ta_discount_rr`.
    2.  Boundary conditions are handled correctly (inclusive for `insert_at <=`, exclusive for `modified_at >`).

#### Test Case 2.8: Filter Logic - `valid_from` and `valid_to` (NULL and Boundary)

*   **Purpose:** Verify the correct application of `d.valid_from <= v_datum_str` and `(d.valid_to IS NULL OR d.valid_to > v_datum_str)` filters.
*   **Setup:**
    1.  Set `v_datum_str` to a specific date (e.g., `'20230601'`).
    2.  Populate `raw_isbert.cds_ta_discount` with `valid_from` and `valid_to` values around this `v_datum_str`:
        *   `valid_from` on `v_datum_str` - included.
        *   `valid_from` before `v_datum_str` - included.
        *   `valid_from` after `v_datum_str` - excluded.
        *   `valid_to IS NULL` - included.
        *   `valid_to` after `v_datum_str` - included.
        *   `valid_to` on `v_datum_str` - excluded.
        *   `valid_to` before `v_datum_str` - excluded.
    3.  Ensure other filters allow these records through.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  Only records satisfying all `valid_from` and `valid_to` conditions relative to `v_datum_str` are present in `curated_rpt.sof_ta_discount_rr`.
    2.  NULL values for `valid_to` are correctly handled as "always valid".
    3.  Boundary conditions are handled correctly.

#### Test Case 2.9: Filter Logic - `is_production`

*   **Purpose:** Verify that `d.is_production = 1` filter is correctly applied.
*   **Setup:**
    1.  Populate `raw_isbert.cds_ta_discount` with records where `is_production = 1` and `is_production = 0` (or other values).
    2.  Ensure other filters allow these records through.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  Only records from `cds_ta_discount` where `is_production = 1` contribute to the final output.
    2.  Records where `is_production != 1` are correctly excluded.

#### Test Case 2.10: NULL Handling in `modified_at` and `valid_to` filters

*   **Purpose:** Explicitly verify that `IS NULL` conditions in the `modified_at` and `valid_to` filters are correctly translated and behave identically to Oracle's `NVL` or implicit NULL handling.
*   **Setup:**
    1.  Set `v_datum_str` to a specific date.
    2.  Populate source tables with records where:
        *   `da.modified_at IS NULL`
        *   `d.modified_at IS NULL`
        *   `d.valid_to IS NULL`
        *   `dv.modified_at IS NULL`
        *   `dii.modified_at IS NULL`
    3.  Ensure all other filter conditions are met for these records.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  Records where `modified_at` or `valid_to` are `NULL` are included in the output, provided all other filter conditions (especially `insert_at` and `valid_from`) are met. This confirms `(col IS NULL OR col > v_datum_str)` logic.

#### Test Case 2.11: Data Type and Value Transformation (`NVL`, `NUMERIC`)

*   **Purpose:** Verify that Oracle-specific functions like `NVL` are correctly translated to BigQuery `IFNULL`, and that data types like `NUMBER` (Oracle) map correctly to `NUMERIC` (BigQuery) without loss of precision or unexpected rounding.
*   **Setup:**
    1.  Populate source tables with data that includes:
        *   `CALC_RULE_VALUE` with various decimal places and magnitudes (e.g., `123.45`, `0.001`, `999999.99`).
        *   Source columns that might be NULL in Oracle but are handled by `NVL` (though the provided SQL doesn't show `NVL` for selected columns, it's a general migration concern).
    2.  Ensure `v_datum` and other filters allow these records through.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    1.  The `rabatthoehe` column in `curated_rpt.sof_ta_discount_rr` correctly stores the `CALC_RULE_VALUE` as a `NUMERIC` type, preserving precision.
    2.  Any implicit `NVL` behavior in Oracle (e.g., if a `STRING` column was `NULL` and concatenated, resulting in an empty string) is replicated correctly by BigQuery's `IFNULL` or default behavior. (Based on the provided SQL, `NVL` is not used in the `SELECT` list, but this is a general check).
*   **Runnable Code (Conceptual):**
    ```sql
    -- Setup:
    TRUNCATE TABLE `your_project.raw_isbert.cds_ta_disc_vector`;
    INSERT INTO `your_project.raw_isbert.cds_ta_disc_vector` (CALC_RULE_VALUE, discount_id, disc_vector_ty, discount_obj_version, insert_at, modified_at) VALUES
    (123.4567, 1, 'TYPE_A', 1, '2023-01-01 00:00:00 UTC', NULL),
    (0.000001, 2, 'TYPE_B', 1, '2023-01-01 00:00:00 UTC', NULL),
    (999999999.99, 3, 'TYPE_C', 1, '2023-01-01 00:00:00 UTC', NULL);
    -- Ensure corresponding join data exists in other tables.

    -- Action:
    CALL `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
        p_JobKennung => 'DATATYPE_TEST',
        p_EintragsNr => '5'
    );

    -- Pass/Fail Criterion:
    SELECT rabatthoehe, BQ.rabatthoehe = ORACLE.rabatthoehe AS value_match
    FROM `your_project.curated_rpt.sof_ta_discount_rr` BQ
    JOIN (
        -- Simulate Oracle output for comparison
        SELECT 123.4567 AS rabatthoehe UNION ALL
        SELECT 0.000001 UNION ALL
        SELECT 999999999.99
    ) ORACLE ON BQ.rabatthoehe = ORACLE.rabatthoehe;
    -- All value_match should be TRUE.
    ```

### 3. External-System Replacements

These tests verify that interactions with external systems (or their BigQuery replacements) behave as specified.

#### Test Case 3.1: Oracle `dwtk_meldungen` Read Replacement

*   **Purpose:** Verify that the BigQuery query against `raw_isbert.dwtk_meldungen` correctly replaces the Oracle read for `v_datum`.
*   **Setup:** Covered by Test Cases 2.2 and 2.3.
*   **Action:** Covered by Test Cases 2.2 and 2.3.
*   **Pass/Fail Criterion:** Covered by Test Cases 2.2 and 2.3.

#### Test Case 3.2: Oracle `TRUNCATE TABLE` Replacement

*   **Purpose:** Verify that the BigQuery `TRUNCATE TABLE` command correctly replaces the Oracle `DWPA_UTIL_SKRIPT.runstatement` call for truncation.
*   **Setup:** Covered by Test Case 2.4.
*   **Action:** Covered by Test Case 2.4.
*   **Pass/Fail Criterion:** Covered by Test Case 2.4.

#### Test Case 3.3: Temporary File for Record Count Replacement

*   **Purpose:** Verify that the temporary file mechanism for record count is correctly replaced by direct `COUNT(*)` and variable assignment within the BigQuery stored procedure.
*   **Setup:** Covered by Test Case 1.1.
*   **Action:** Covered by Test Case 1.1.
*   **Pass/Fail Criterion:** The `record_count` in `job_table` accurately reflects the number of rows inserted, confirming the internal counting mechanism works without external files.

### 4. Data Quality / Row-Count / Schema Assertions

These tests ensure the integrity and structure of the target data.

#### Test Case 4.1: Target Table Schema Validation

*   **Purpose:** Verify that the `curated_rpt.sof_ta_discount_rr` table in BigQuery has the correct columns, data types, and (where applicable) nullability as defined in the DDL and derived from the Oracle source.
*   **Setup:**
    1.  Ensure the DDL for `curated_rpt.sof_ta_discount_rr` has been applied.
*   **Action:** Query the BigQuery information schema for the target table.
*   **Pass/Fail Criterion:** The schema of `your_project.curated_rpt.sof_ta_discount_rr` matches the expected DDL:
    *   `cntrct_id` INT64
    *   `discount_id` INT64
    *   `disc_vector_ty` STRING
    *   `cntrct_obj_version` INT64
    *   `cntrct_template_id` INT64
    *   `disc_invoice_item_id` INT64
    *   `rabatt` STRING
    *   `rabatthoehe` NUMERIC
    *   `rabattierte_rech_pos` STRING

    ```sql
    -- SQL Assertion
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your_project.curated_rpt.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_discount_rr'
    ORDER BY
        ordinal_position;

    -- Expected output (compare against this)
    -- column_name           data_type   is_nullable
    -- cntrct_id             INT64       YES
    -- discount_id           INT64       YES
    -- disc_vector_ty        STRING      YES
    -- cntrct_obj_version    INT64       YES
    -- cntrct_template_id    INT64       YES
    -- disc_invoice_item_id  INT64       YES
    -- rabatt                STRING      YES
    -- rabatthoehe           NUMERIC     YES
    -- rabattierte_rech_pos  STRING      YES
    ```

#### Test Case 4.2: Target Table Row Count Assertion

*   **Purpose:** Verify that the number of rows processed and inserted into the target table is consistent and matches expectations based on the source data and transformation logic.
*   **Setup:**
    1.  Populate source tables with a known number of records that should pass all filters and joins (e.g., 100 records).
    2.  Execute the BigQuery stored procedure.
*   **Action:** Query the target table for its row count and check the `job_table` entry.
*   **Pass/Fail Criterion:**
    1.  The `COUNT(*)` on `your_project.curated_rpt.sof_ta_discount_rr` matches the expected number of rows (e.g., 100).
    2.  The `record_count` in the corresponding `job_table` entry also matches this count.

    ```sql
    -- SQL Assertion
    SELECT COUNT(*) FROM `your_project.curated_rpt.sof_ta_discount_rr`;
    -- Compare with expected count and job_table.record_count
    ```

#### Test Case 4.3: Target Table Data Integrity (NULLs, Duplicates)

*   **Purpose:** Verify that the target table does not contain unexpected NULL values or duplicate records, which could indicate issues in transformation or join logic.
*   **Setup:**
    1.  Populate source tables with data, including scenarios that might lead to NULLs or duplicates if the logic is flawed (e.g., missing join keys, non-unique source data if uniqueness is expected in target).
    2.  Execute the BigQuery stored procedure.
*   **Action:** Query the target table for NULLs in critical columns and for duplicate records based on a natural key (if one exists, otherwise all columns).
*   **Pass/Fail Criterion:**
    1.  No unexpected NULL values are found in columns that are expected to always have a value (e.g., `cntrct_id`, `discount_id`).
    2.  No duplicate records are found based on the combination of `cntrct_id`, `discount_id`, `disc_vector_ty`, `cntrct_obj_version`, `cntrct_template_id`, `disc_invoice_item_id` (assuming this combination should be unique, or define a natural key based on business understanding).

    ```sql
    -- SQL Assertion for unexpected NULLs (example for cntrct_id)
    SELECT COUNT(*) FROM `your_project.curated_rpt.sof_ta_discount_rr` WHERE cntrct_id IS NULL;
    -- Expected: 0

    -- SQL Assertion for duplicates (assuming the combination of these columns should be unique)
    SELECT
        cntrct_id,
        discount_id,
        disc_vector_ty,
        cntrct_obj_version,
        cntrct_template_id,
        disc_invoice_item_id,
        COUNT(*) as num_duplicates
    FROM
        `your_project.curated_rpt.sof_ta_discount_rr`
    GROUP BY
        cntrct_id,
        discount_id,
        disc_vector_ty,
        cntrct_obj_version,
        cntrct_template_id,
        disc_invoice_item_id
    HAVING
        num_duplicates > 1;
    -- Expected: 0 rows returned
    ```