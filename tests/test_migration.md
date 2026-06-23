As a senior data-migration QA engineer, I've analyzed the provided migration design and generated BigQuery code for `k_ausd_v_ta_vertrag_tmp.ksh`. The migration involves re-implementing KornShell orchestration logic into a BigQuery Stored Procedure and translating the core SQL logic from `d_ausd_v_ta_vertrag_tmp.sql` to BigQuery Standard SQL.

The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests for `k_ausd_v_ta_vertrag_tmp.ksh`

**Assumptions for Testing:**
*   A "golden dataset" from the legacy system (Oracle database) is available for comparison. This includes the final state of `SOF$TA_VERTRAG_TMP` and `VIA` after a successful legacy run, as well as any relevant log outputs.
*   Access to both the legacy Oracle environment (for baseline data extraction) and the target BigQuery environment (for executing tests and verifying results).
*   The `project.dataset` placeholder in the generated code will be replaced with actual BigQuery project and dataset names for execution.
*   The `VIA` table's schema is a placeholder in the DDL; its actual usage and schema would need to be confirmed for more specific tests. For now, we'll focus on its existence and basic row count.
*   The `log_error` helper procedure is correctly deployed and functional.

---

### 1. Schema and Data Type Integrity Test

*   **Purpose:** Verify that all BigQuery tables (source, target, and logging) are created with the correct column names and data types as per the migration design and inferred from the SQL logic. This ensures no data truncation or type-related errors occur during data loading or processing.
*   **Setup:**
    1.  Ensure all BigQuery DDL scripts (`bigquery_ddl/*.sql`) have been executed to create the necessary tables in the target `project.dataset`.
    2.  Have the legacy Oracle table schemas available for comparison.
*   **Action:**
    1.  Query the BigQuery information schema to retrieve the schema details for each created table.
    2.  Compare these schemas against the expected BigQuery types derived from the legacy Oracle schemas and the transformation logic.
*   **Pass/Fail Criteria:**
    *   All tables (`sof_ta_vertrag_tmp`, `via`, `dwtk_meldungen`, `sof_ta_cntrct_crs3`, `job_table`, `error_log`, `job_result_log`, and all other joined tables like `sof_ta_bp_ref`, `sof_ta_inv_acc`, etc.) exist in `project.dataset`.
    *   Each column in the BigQuery tables matches the expected data type and nullability, preventing data loss or conversion errors.
    *   For `sof_ta_vertrag_tmp`, specifically verify that `NUMERIC` types (e.g., `vertragsbindung`) have appropriate precision and scale.
*   **Test Code (SQL Assertion):**

    ```sql
    -- Example for sof_ta_vertrag_tmp
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_vertrag_tmp'
    ORDER BY
        ordinal_position;

    -- Expected output (example snippet):
    -- column_name             data_type   is_nullable
    -- ------------------------------------------------
    -- vertrag_id_carmen       STRING      YES
    -- partner_id_carmen       STRING      YES
    -- ...
    -- vertragsbindung         NUMERIC     YES
    -- ...
    -- cntrct_validity_id      INT64       YES

    -- Repeat for all other tables.
    ```

---

### 2. Parameter Validation and Error Handling Test

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly handles missing or empty input parameters, mimicking the `getopts` and `pruefeParameterGesetzt` logic of the legacy script. It should log errors and raise an exception.
*   **Setup:**
    1.  Ensure `job_table` and `error_log` are empty or cleared before each test run.
    2.  The `log_error` procedure must be deployed.
*   **Action:**
    1.  Call the stored procedure `r_ausd_vertrag_control` with a missing `p_jobkennung`.
    2.  Call the stored procedure `r_ausd_vertrag_control` with an empty `p_jobkennung`.
    3.  Call the stored procedure `r_ausd_vertrag_control` with a missing `p_eintrags_nr`.
    4.  Call the stored procedure `r_ausd_vertrag_control` with an empty `p_eintrags_nr`.
*   **Pass/Fail Criteria:**
    *   For each invalid call, the stored procedure should terminate with an error message (e.g., `RAISE USING MESSAGE`).
    *   An entry should be recorded in `project.dataset.error_log` with `severity = 'ERROR'`, `error_code = '193'`, and an appropriate `error_message` (e.g., "Parameter p_JobKennung is missing or empty.").
    *   No data transformation should occur in `sof_ta_vertrag_tmp`.
    *   The `job_table` should contain an entry for the attempted job with `status = 'FAILED'`.
*   **Test Code (BigQuery Scripting):**

    ```sql
    -- Test 2.1: Missing p_jobkennung
    BEGIN
        CALL `project.dataset.r_ausd_vertrag_control`(NULL, '12345');
    EXCEPTION WHEN ERROR THEN
        SELECT 'Test 2.1 Passed: Procedure raised an error for missing p_jobkennung.' AS status;
    END;
    SELECT * FROM `project.dataset.error_log` WHERE error_code = '193' AND error_message LIKE '%p_JobKennung%';
    SELECT * FROM `project.dataset.job_table` WHERE status = 'FAILED' AND job_kennung IS NULL;

    -- Test 2.2: Empty p_jobkennung
    BEGIN
        CALL `project.dataset.r_ausd_vertrag_control`('', '12345');
    EXCEPTION WHEN ERROR THEN
        SELECT 'Test 2.2 Passed: Procedure raised an error for empty p_jobkennung.' AS status;
    END;
    SELECT * FROM `project.dataset.error_log` WHERE error_code = '193' AND error_message LIKE '%p_JobKennung%';
    SELECT * FROM `project.dataset.job_table` WHERE status = 'FAILED' AND job_kennung = '';

    -- Repeat for p_eintrags_nr
    ```

---

### 3. Job Status Management Test (Active Job Deactivation)

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly manages job status, specifically deactivating previously 'RUNNING' jobs for the same `job_kennung` but a different `eintrags_nr`, replicating the `starteSQLSkript` behavior.
*   **Setup:**
    1.  Populate `project.dataset.job_table` with a 'RUNNING' job for a specific `job_kennung` and `eintrags_nr`.
    2.  Ensure `sof_ta_vertrag_tmp` is empty.
    3.  Populate all source tables with minimal valid data to allow the procedure to run successfully.
*   **Action:**
    1.  Insert a record into `job_table`: `('JOB_A', 'ENTRY_1', CURRENT_TIMESTAMP(), 'RUNNING', NULL, NULL, NULL, CURRENT_TIMESTAMP())`.
    2.  Call `r_ausd_vertrag_control('JOB_A', 'ENTRY_2')`.
*   **Pass/Fail Criteria:**
    *   After the call, `job_table` should contain three records:
        *   The initial `('JOB_A', 'ENTRY_1')` record with `status = 'DEACTIVATED'`.
        *   A new `('JOB_A', 'ENTRY_2')` record with `status = 'COMPLETED'` (assuming successful data processing).
    *   The `message` column for the deactivated job should be 'Deactivated by new run'.
*   **Test Code (BigQuery Scripting):**

    ```sql
    -- Clear job_table for a clean test
    DELETE FROM `project.dataset.job_table` WHERE job_kennung = 'JOB_A';

    -- Setup: Insert a "running" job
    INSERT INTO `project.dataset.job_table` (job_kennung, eintrags_nr, start_time, status, last_updated)
    VALUES ('JOB_A', 'ENTRY_1', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 'RUNNING', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));

    -- Action: Call the procedure with a new entry number for the same job_kennung
    CALL `project.dataset.r_ausd_vertrag_control`('JOB_A', 'ENTRY_2');

    -- Verification
    SELECT
        job_kennung,
        eintrags_nr,
        status,
        message
    FROM
        `project.dataset.job_table`
    WHERE
        job_kennung = 'JOB_A'
    ORDER BY
        eintrags_nr;

    -- Expected Output:
    -- job_kennung | eintrags_nr | status       | message
    -- ------------|-------------|--------------|--------------------------
    -- JOB_A       | ENTRY_1     | DEACTIVATED  | Deactivated by new run
    -- JOB_A       | ENTRY_2     | COMPLETED    | NULL (or success message)
    ```

---

### 4. Output Parity and Transformation Correctness Test (Golden Dataset Comparison)

*   **Purpose:** This is the most critical test. Verify that the data generated in `project.dataset.sof_ta_vertrag_tmp` by the BigQuery Stored Procedure is identical to the data generated by the legacy `k_ausd_v_ta_vertrag_tmp.ksh` script in the Oracle `SOF$TA_VERTRAG_TMP` table, given the same input data. This covers all aspects of transformation logic (joins, filters, `CASE` statements, date functions, `NULL` handling).
*   **Setup:**
    1.  **Legacy Baseline:** Execute the legacy `k_ausd_v_ta_vertrag_tmp.ksh` script with a specific set of input data in Oracle. Extract the resulting data from `SOF$TA_VERTRAG_TMP` into a "golden dataset" (e.g., CSV, JSON, or a temporary BigQuery table).
    2.  **BigQuery Setup:** Load the *exact same input data* into all relevant BigQuery source tables (`dwtk_meldungen`, `sof_ta_cntrct_crs3`, `sof_ta_bp_ref`, etc.).
    3.  Ensure `project.dataset.sof_ta_vertrag_tmp` is empty before the test run.
*   **Action:**
    1.  Call `project.dataset.r_ausd_vertrag_control` with the same `p_jobkennung` and `p_eintrags_nr` used for the legacy run.
    2.  Extract the data from the resulting `project.dataset.sof_ta_vertrag_tmp` table.
    3.  Compare this extracted data row-by-row and column-by-column with the "golden dataset".
*   **Pass/Fail Criteria:**
    *   The row count in `project.dataset.sof_ta_vertrag_tmp` must exactly match the row count in the "golden dataset".
    *   Every column value for every row in `project.dataset.sof_ta_vertrag_tmp` must exactly match the corresponding value in the "golden dataset". This includes `NULL` values, string comparisons (case sensitivity, trimming), and numeric/date precision.
    *   The `job_table` and `job_result_log` should reflect a successful run and the correct record count.
*   **Test Code (Python with BigQuery client and pandas for comparison):**

    ```python
    import pandas as pd
    from google.cloud import bigquery

    client = bigquery.Client()
    project_id = "your-gcp-project-id"
    dataset_id = "your_bigquery_dataset"

    def run_legacy_and_extract_golden_data():
        # This function would involve executing the legacy ksh script
        # and extracting data from Oracle. This is highly dependent on your legacy setup.
        # For demonstration, assume 'golden_data.csv' is already created.
        # Example:
        # os.system("./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh -j LEGACY_JOB -f LEGACY_ENTRY")
        # Then, extract from Oracle:
        # pd.read_sql("SELECT * FROM SOF$TA_VERTRAG_TMP", oracle_conn)
        return pd.read_csv("golden_data_sof_ta_vertrag_tmp.csv")

    def run_bq_procedure_and_extract_data(job_kennung, eintrags_nr):
        query = f"""
        CALL `{project_id}.{dataset_id}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');
        """
        client.query(query).result() # Execute the stored procedure

        # Extract data from the target table
        df_bq = client.query(f"SELECT * FROM `{project_id}.{dataset_id}.sof_ta_vertrag_tmp` ORDER BY vertrag_id_carmen").to_dataframe()
        return df_bq

    def test_output_parity():
        # Setup: Load identical input data into BigQuery source tables
        # (This step is manual or via separate data loading scripts)

        # 1. Get golden dataset from legacy run
        df_golden = run_legacy_and_extract_golden_data()
        print(f"Legacy golden dataset rows: {len(df_golden)}")

        # 2. Run BigQuery procedure
        df_bq = run_bq_procedure_and_extract_data("BQ_JOB_TEST", "BQ_ENTRY_TEST")
        print(f"BigQuery output rows: {len(df_bq)}")

        # 3. Compare dataframes
        # Ensure column order and types are consistent for comparison
        df_golden = df_golden.astype(df_bq.dtypes.to_dict()) # Adjust types if necessary
        df_golden = df_golden[df_bq.columns] # Ensure same columns and order

        pd.testing.assert_frame_equal(df_golden, df_bq, check_dtype=True, check_exact=False, rtol=1e-9)
        print("Output Parity Test Passed: Dataframes are identical.")

    # Execute the test
    # test_output_parity()
    ```

---

### 5. `v_datum` Calculation and `DATE_DIFF` Logic Test

*   **Purpose:** Verify the correct calculation of `v_datum` from `dwtk_meldungen` and its subsequent use in the `DATE_DIFF` logic within the `upgradeberechtigt` `CASE` statement. This is a specific transformation logic test.
*   **Setup:**
    1.  Populate `project.dataset.dwtk_meldungen` with various `timecreated` values for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, including `NULL` and multiple entries.
    2.  Populate `project.dataset.sof_ta_cntrct_crs3` with diverse `commitment_reference_date` and `cntrct_start_date` values, along with `sof_ta_period` data for `number_time_measurement`.
    3.  Ensure `sof_ta_vertrag_tmp` is empty.
*   **Action:**
    1.  Run the `r_ausd_vertrag_control` procedure.
    2.  Query `project.dataset.sof_ta_vertrag_tmp` and inspect the `upgradeberechtigt` column for various contract scenarios.
*   **Pass/Fail Criteria:**
    *   The `v_datum` value (derived from `MAX(m.timecreated)` or '19000101') used in the procedure should be correctly identified.
    *   The `upgradeberechtigt` column values (`'J'` or `'N'`) should be correctly calculated based on the `DATE_DIFF` logic and the `number_time_measurement` from `sof_ta_period`.
    *   Specifically test:
        *   `number_time_measurement` is `NULL` or `0`.
        *   `number_time_measurement = 12` and `DATE_DIFF > 9` months.
        *   `number_time_measurement` is `NULL` or `0` or `24` and `DATE_DIFF > 23` months.
        *   Cases where `b.sperrart_alle IS NULL` or `b.sperrgrund_zusgf = 2`.
        *   Cases where `commitment_reference_date` is `NULL` (should default to `cntrct_start_date`).
*   **Test Code (SQL Assertion - example for one scenario):**

    ```sql
    -- Setup: Insert specific data for testing v_datum and upgradeberechtigt logic
    TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
    INSERT INTO `project.dataset.dwtk_meldungen` (timecreated, job_kennung) VALUES
    ('2023-01-15 10:00:00', 'BERT_DROP_TEMP_TABLE'),
    ('2023-01-20 11:00:00', 'BERT_DROP_TEMP_TABLE'); -- Max timecreated will be 2023-01-20

    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_crs3`;
    TRUNCATE TABLE `project.dataset.sof_ta_period`;
    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_valid`;
    TRUNCATE TABLE `project.dataset.sof_ta_barrier_zusgf`;

    INSERT INTO `project.dataset.sof_ta_cntrct_crs3` (cntrct_id, cntrct_start_date, commitment_reference_date, cntrct_ty, cntrct_validity_id) VALUES
    ('C1', '2022-01-01', '2022-01-01', 10, 101), -- Case 1: 12 months, diff > 9
    ('C2', '2022-01-01', NULL, 10, 102);        -- Case 2: 24 months, diff > 23, commitment_reference_date is NULL

    INSERT INTO `project.dataset.sof_ta_cntrct_valid` (cntrct_validity_id, first_period_id) VALUES
    (101, 201), (102, 202);

    INSERT INTO `project.dataset.sof_ta_period` (period_id, number_time_measurement, einheit) VALUES
    (201, 12, 'MONTH'),
    (202, 24, 'MONTH');

    INSERT INTO `project.dataset.sof_ta_barrier_zusgf` (cntrct_id, sperrart_alle, sperrgrund_zusgf) VALUES
    ('C1', NULL, NULL),
    ('C2', 'Sperre', 2);

    -- Execute the procedure
    CALL `project.dataset.r_ausd_vertrag_control`('DATE_TEST_JOB', 'DATE_TEST_ENTRY');

    -- Verify results
    SELECT
        vertrag_id_carmen,
        vertragsbeginn,
        commitment_reference_date,
        upgradeberechtigt
    FROM
        `project.dataset.sof_ta_vertrag_tmp`
    WHERE
        vertrag_id_carmen IN ('C1', 'C2')
    ORDER BY vertrag_id_carmen;

    -- Expected Output (based on v_datum = '20230120'):
    -- vertrag_id_carmen | vertragsbeginn | commitment_reference_date | upgradeberechtigt
    -- ------------------|----------------|---------------------------|------------------
    -- C1                | 2022-01-01     | 2022-01-01                | J (DATE_DIFF('20230120', '20220101', MONTH) = 12 > 9)
    -- C2                | 2022-01-01     | NULL                      | J (DATE_DIFF('20230120', '20220101', MONTH) = 12, but this case is for 24 months, so it should be N. Re-evaluate expected output based on exact logic.)
    -- Let's re-evaluate C2: DATE_DIFF('20230120', '20220101', MONTH) is 12.
    -- The condition for 24 months is `DATE_DIFF(...) > 23`. So C2 should be 'N'.
    -- Corrected Expected Output:
    -- vertrag_id_carmen | vertragsbeginn | commitment_reference_date | upgradeberechtigt
    -- ------------------|----------------|---------------------------|------------------
    -- C1                | 2022-01-01     | 2022-01-01                | J
    -- C2                | 2022-01-01     | NULL                      | N
    ```

---

### 6. External System Replacement Verification (Logging Tables)

*   **Purpose:** Verify that the new BigQuery logging tables (`job_table`, `error_log`, `job_result_log`) are correctly populated and updated throughout the stored procedure's execution, replacing the legacy temporary files and error logging mechanisms.
*   **Setup:**
    1.  Ensure all logging tables are empty.
    2.  Populate source tables with valid data to allow a successful run.
*   **Action:**
    1.  Call `project.dataset.r_ausd_vertrag_control('LOG_TEST_JOB', 'LOG_TEST_ENTRY')`.
    2.  Query `job_table`, `error_log`, and `job_result_log`.
*   **Pass/Fail Criteria:**
    *   `job_table` should contain an entry for `('LOG_TEST_JOB', 'LOG_TEST_ENTRY')` with `status = 'COMPLETED'`, `start_time`, `end_time`, and `processed_records` populated.
    *   `job_result_log` should contain an entry for `('LOG_TEST_JOB', 'LOG_TEST_ENTRY')` with `target_table = 'sof_ta_vertrag_tmp'` and `record_count` matching the actual number of rows inserted into `sof_ta_vertrag_tmp`.
    *   `error_log` should be empty (for a successful run).
    *   If an error is intentionally introduced (e.g., by corrupting input data or forcing a division by zero in the SQL), `error_log` should contain the error details, and `job_table` should show `status = 'FAILED'`.
*   **Test Code (SQL Assertion):**

    ```sql
    -- Clear logging tables
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_result_log`;

    -- Assume source tables are populated for a successful run
    CALL `project.dataset.r_ausd_vertrag_control`('LOG_TEST_JOB', 'LOG_TEST_ENTRY');

    -- Verify job_table
    SELECT
        job_kennung,
        eintrags_nr,
        status,
        processed_records
    FROM
        `project.dataset.job_table`
    WHERE
        job_kennung = 'LOG_TEST_JOB' AND eintrags_nr = 'LOG_TEST_ENTRY';
    -- Expected: status = 'COMPLETED', processed_records > 0

    -- Verify job_result_log
    SELECT
        job_kennung,
        eintrags_nr,
        target_table,
        record_count
    FROM
        `project.dataset.job_result_log`
    WHERE
        job_kennung = 'LOG_TEST_JOB' AND eintrags_nr = 'LOG_TEST_ENTRY';
    -- Expected: target_table = 'sof_ta_vertrag_tmp', record_count matches actual rows

    -- Verify error_log (should be empty for a successful run)
    SELECT COUNT(*) FROM `project.dataset.error_log`;
    -- Expected: 0
    ```

---

### 7. Row Count Assertion Test

*   **Purpose:** Verify that the total number of records processed and inserted into `sof_ta_vertrag_tmp` matches the expected count from the legacy system. This is a high-level check for data completeness.
*   **Setup:**
    1.  Perform a legacy run and record the final row count in `SOF$TA_VERTRAG_TMP`.
    2.  Load the exact same input data into BigQuery source tables.
    3.  Ensure `sof_ta_vertrag_tmp` is empty.
*   **Action:**
    1.  Call `project.dataset.r_ausd_vertrag_control('COUNT_TEST_JOB', 'COUNT_TEST_ENTRY')`.
    2.  Query the row count of `project.dataset.sof_ta_vertrag_tmp`.
*   **Pass/Fail Criteria:**
    *   The `COUNT(*)` from `project.dataset.sof_ta_vertrag_tmp` must exactly match the recorded legacy row count.
    *   The `processed_records` in `job_table` and `record_count` in `job_result_log` should also match this count.
*   **Test Code (SQL Assertion):**

    ```sql
    -- Assume legacy_row_count is obtained from a legacy run
    DECLARE legacy_row_count INT64 DEFAULT 12345; -- Replace with actual legacy count

    -- Clear target table
    TRUNCATE TABLE `project.dataset.sof_ta_vertrag_tmp`;

    -- Execute the procedure
    CALL `project.dataset.r_ausd_vertrag_control`('COUNT_TEST_JOB', 'COUNT_TEST_ENTRY');

    -- Verify row count
    SELECT
        (SELECT COUNT(*) FROM `project.dataset.sof_ta_vertrag_tmp`) AS bq_row_count,
        legacy_row_count AS expected_row_count,
        (SELECT processed_records FROM `project.dataset.job_table` WHERE job_kennung = 'COUNT_TEST_JOB' AND eintrags_nr = 'COUNT_TEST_ENTRY') AS logged_row_count;

    -- Pass if bq_row_count = expected_row_count AND bq_row_count = logged_row_count
    ```

---

### 8. `UNION ALL` Logic and Filtering Test

*   **Purpose:** Verify that the two branches of the `UNION ALL` query are correctly applied based on the `c.cntrct_ty` filter (`<> 20` and `= 20`) and that the `LEFT JOIN`s in each branch behave as expected.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_cntrct_crs3` with contracts where `cntrct_ty = 20` and `cntrct_ty <> 20`.
    2.  Populate `project.dataset.sof_ta_bp_ref` with entries for both `cntrct_cp2_id` (for `cntrct_ty <> 20` branch) and `cntrct_parent` (for `cntrct_ty = 20` branch). Include cases where joins might not find a match.
    3.  Ensure `sof_ta_vertrag_tmp` is empty.
*   **Action:**
    1.  Run the `r_ausd_vertrag_control` procedure.
    2.  Query `project.dataset.sof_ta_vertrag_tmp` and inspect `vertrag_id_carmen` and `partner_id_carmen` for contracts with `cntrct_ty = 20` and `cntrct_ty <> 20`.
*   **Pass/Fail Criteria:**
    *   Contracts with `cntrct_ty <> 20` should have their `partner_id_carmen` derived from `bp.bp_id` where `bp.cntrct_cp2_id = c.cntrct_id`.
    *   Contracts with `cntrct_ty = 20` should have their `partner_id_carmen` derived from `bp.bp_id` where `bp.cntrct_cp2_id = c.cntrct_parent`.
    *   Records should appear only once in the final output, as expected from `UNION ALL`.
    *   `NULL` values for `partner_id_carmen` should correctly appear if the `LEFT JOIN` fails to find a match in `sof_ta_bp_ref`.
*   **Test Code (SQL Assertion - conceptual):**

    ```sql
    -- Setup: Insert data to test UNION ALL and join conditions
    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_crs3`;
    TRUNCATE TABLE `project.dataset.sof_ta_bp_ref`;

    INSERT INTO `project.dataset.sof_ta_cntrct_crs3` (cntrct_id, cntrct_ty, cntrct_parent) VALUES
    ('C_NON_20_1', 10, NULL),
    ('C_NON_20_2', 15, NULL),
    ('C_20_1', 20, 'PARENT_C_20_1'),
    ('C_20_2', 20, 'PARENT_C_20_2');

    INSERT INTO `project.dataset.sof_ta_bp_ref` (cntrct_cp2_id, bp_id) VALUES
    ('C_NON_20_1', 'BP_NON_20_1'),
    ('PARENT_C_20_1', 'BP_20_1');

    -- Execute the procedure
    CALL `project.dataset.r_ausd_vertrag_control`('UNION_TEST_JOB', 'UNION_TEST_ENTRY');

    -- Verify results
    SELECT
        vertrag_id_carmen,
        partner_id_carmen
    FROM
        `project.dataset.sof_ta_vertrag_tmp`
    WHERE
        vertrag_id_carmen IN ('C_NON_20_1', 'C_NON_20_2', 'C_20_1', 'C_20_2')
    ORDER BY vertrag_id_carmen;

    -- Expected Output:
    -- vertrag_id_carmen | partner_id_carmen
    -- ------------------|------------------
    -- C_20_1            | BP_20_1
    -- C_20_2            | NULL
    -- C_NON_20_1        | BP_NON_20_1
    -- C_NON_20_2        | NULL
    ```

---

### 9. `VIA` Table Placeholder Test

*   **Purpose:** Acknowledge the `VIA` table as a target and ensure it exists and is not inadvertently affected or left unpopulated if it was intended to be populated by `d_ausd_v_ta_vertrag_tmp.sql`. (The provided SQL does not show any writes to `VIA`, so this test is primarily for schema existence and to flag if `VIA` was intended to be populated).
*   **Setup:**
    1.  Ensure `project.dataset.via` is created.
*   **Action:**
    1.  Run the `r_ausd_vertrag_control` procedure.
    2.  Query `project.dataset.via` for its existence and row count.
*   **Pass/Fail Criteria:**
    *   The table `project.dataset.via` must exist.
    *   If the legacy `d_ausd_v_ta_vertrag_tmp.sql` *did* write to `VIA`, then the BigQuery procedure *should* also write to it, and the row count should match the legacy output.
    *   **Note:** Based on the provided `d_ausd_v_ta_vertrag_tmp.sql` content, there are no `INSERT`s into `VIA`. Therefore, the expected behavior is that `VIA` remains empty after the procedure runs. If this is incorrect, the migration design needs to be updated.
*   **Test Code (SQL Assertion):**

    ```sql
    -- Verify VIA table exists
    SELECT
        COUNT(*)
    FROM
        `project.dataset.INFORMATION_SCHEMA.TABLES`
    WHERE
        table_name = 'via';
    -- Expected: 1

    -- Verify row count (assuming no writes based on provided SQL)
    SELECT COUNT(*) FROM `project.dataset.via`;
    -- Expected: 0 (if no writes are intended)
    -- If writes are intended, this should match the legacy count.
    ```

---

These tests provide a comprehensive validation strategy for the migrated `k_ausd_v_ta_vertrag_tmp.ksh` job to its BigQuery Stored Procedure equivalent. The emphasis on golden dataset comparison and detailed logic checks ensures high confidence in the behavioral equivalence of the new system.