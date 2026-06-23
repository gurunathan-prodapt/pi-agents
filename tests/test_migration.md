As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `k_ausd_v_ta_vvl_dwh.ksh`. The core challenge is the absence of the `d_ausd_v_ta_vvl_dwh.sql` content, which contains the actual data transformation logic. This significantly impacts the ability to write comprehensive "Transformation correctness" and "Output parity" tests for the data itself.

Therefore, the tests below focus heavily on the orchestration logic, parameter handling, job tracking, and error logging, which are explicitly defined in the migration design. For the data transformation part, I've included placeholder tests and made assumptions about the behavior of the core SQL logic (e.g., inserting a fixed number of rows) to enable testing of the surrounding orchestration.

**Assumptions for Testing:**

1.  **Core SQL Logic (`d_ausd_v_ta_vvl_dwh.sql`):** For tests requiring data transformation, the `EXECUTE IMMEDIATE` block within `project.dataset.r_ausd_vertrag_control_sp` is assumed to be temporarily modified to perform a simple `INSERT` statement that adds 10 rows to `project.dataset.target_table` and correctly sets `v_records_processed` using `@@row_count`. This allows for testing the record count capture and job status updates.
2.  **`target_table` Schema:** The placeholder DDL for `target_table` has been slightly adjusted in the test setups to use `STRING` for `column_id` to accommodate `GENERATE_UUID()` in the simulated insert.
3.  **Missing Logic:** Logic for "deactivating old active jobs" is not present in the original script or detailed in the design, so it cannot be tested.

---

## Migration Validation Tests for `k_ausd_v_ta_vvl_dwh.ksh`

### Test Case 1: Successful Job Execution - Output Parity & Job Tracking

*   **Purpose**: Verify that a successful execution of the migrated stored procedure correctly processes parameters, executes the core logic (simulated), and updates the `job_table` with 'COMPLETED' status and the correct record count. This mimics the original script's successful execution and temporary file output.
*   **Setup**:
    1.  Ensure `project.dataset.job_table`, `project.dataset.job_error_log`, and `project.dataset.target_table` are empty.
    2.  The `project.dataset.r_ausd_vertrag_control_sp` is deployed. For this test, temporarily modify the `EXECUTE IMMEDIATE` block within the stored procedure to simulate a successful data transformation that inserts 10 rows into `project.dataset.target_table` and captures the `@@row_count`.
        ```sql
        -- Temporary modification within project.dataset.r_ausd_vertrag_control_sp for testing:
        EXECUTE IMMEDIATE """
            INSERT INTO project.dataset.target_table (column_id, column_name, created_at)
            SELECT
                GENERATE_UUID(),
                'TestName_' || CAST(i AS STRING),
                CURRENT_TIMESTAMP()
            FROM
                UNNEST(GENERATE_ARRAY(1, 10)) AS i;
        """;
        SET v_records_processed = @@row_count;
        ```
    3.  Ensure `project.dataset.target_table` has a compatible schema (e.g., `column_id STRING, column_name STRING, created_at TIMESTAMP`).
*   **Action**: Call the stored procedure with valid parameters:
    ```sql
    CALL project.dataset.r_ausd_vertrag_control_sp('TEST_JOB_SUCCESS', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion**:
    1.  Exactly one row exists in `project.dataset.job_table` for `job_kennung = 'TEST_JOB_SUCCESS'` and `eintrags_nr = 'ENTRY_001'`.
    2.  The `status` column for this job entry is 'COMPLETED'.
    3.  The `record_count` column for this job entry is `10`.
    4.  `created_ts` and `finished_ts` are populated, and `finished_ts` is after `created_ts`.
    5.  `project.dataset.job_error_log` contains zero rows.
    6.  `project.dataset.target_table` contains exactly 10 new rows.
*   **Test Code (SQL Assertions)**:
    ```sql
    -- Setup: Clear tables before test
    TRUNCATE TABLE project.dataset.job_table;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.target_table;

    -- Action: Execute the stored procedure
    CALL project.dataset.r_ausd_vertrag_control_sp('TEST_JOB_SUCCESS', 'ENTRY_001');

    -- Assertions
    SELECT
        CASE
            WHEN (SELECT COUNT(*) FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_SUCCESS' AND eintrags_nr = 'ENTRY_001') = 1
            AND (SELECT status FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_SUCCESS' AND eintrags_nr = 'ENTRY_001') = 'COMPLETED'
            AND (SELECT record_count FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_SUCCESS' AND eintrags_nr = 'ENTRY_001') = 10
            AND (SELECT created_ts FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_SUCCESS' AND eintrags_nr = 'ENTRY_001') IS NOT NULL
            AND (SELECT finished_ts FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_SUCCESS' AND eintrags_nr = 'ENTRY_001') IS NOT NULL
            AND (SELECT finished_ts FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_SUCCESS' AND eintrags_nr = 'ENTRY_001') > (SELECT created_ts FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_SUCCESS' AND eintrags_nr = 'ENTRY_001')
            AND (SELECT COUNT(*) FROM project.dataset.job_error_log) = 0
            AND (SELECT COUNT(*) FROM project.dataset.target_table) = 10
            THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case 2: Parameter Validation - Missing JobKennung

*   **Purpose**: Verify that the stored procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and failing gracefully, similar to `pruefeParameterGesetzt Jobkennung p_JobKennung` and `DWMSG_MeldeFehler` in the original script.
*   **Setup**:
    1.  Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.
    2.  The `project.dataset.r_ausd_vertrag_control_sp` is deployed.
*   **Action**: Call the stored procedure with a `NULL` `p_JobKennung`:
    ```sql
    BEGIN
        CALL project.dataset.r_ausd_vertrag_control_sp(NULL, 'ENTRY_002');
    EXCEPTION WHEN ERROR THEN
        -- Error caught, continue for assertions
    END;
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure execution raises an error (e.g., `JobKennung parameter cannot be empty.`).
    2.  Exactly one row exists in `project.dataset.job_error_log` for `eintrags_nr = 'ENTRY_002'`.
    3.  The `err_nr` in `job_error_log` is '1001' and `err_arg` is 'PARAMETER_VALIDATION'.
    4.  The `message` in `job_error_log` contains 'JobKennung parameter cannot be empty.'.
    5.  Exactly one row exists in `project.dataset.job_table` for `eintrags_nr = 'ENTRY_002'`.
    6.  The `status` in `job_table` for this entry is 'FAILED'.
    7.  `record_count` in `job_table` is `NULL` or `0`.
    8.  `project.dataset.target_table` contains zero rows (no transformation occurred).
*   **Test Code (SQL Assertions)**:
    ```sql
    -- Setup: Clear tables before test
    TRUNCATE TABLE project.dataset.job_table;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.target_table;

    -- Action: Execute the stored procedure (expecting an error)
    BEGIN
        CALL project.dataset.r_ausd_vertrag_control_sp(NULL, 'ENTRY_002');
    EXCEPTION WHEN ERROR THEN
        -- Error caught, now perform assertions
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM project.dataset.job_error_log WHERE eintrags_nr = 'ENTRY_002' AND err_nr = '1001' AND err_arg = 'PARAMETER_VALIDATION' AND message LIKE '%JobKennung parameter cannot be empty.%') = 1
                AND (SELECT COUNT(*) FROM project.dataset.job_table WHERE eintrags_nr = 'ENTRY_002' AND status = 'FAILED' AND (record_count = 0 OR record_count IS NULL)) = 1
                AND (SELECT COUNT(*) FROM project.dataset.target_table) = 0
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
    END;
    ```

### Test Case 3: Parameter Validation - Missing EintragsNr

*   **Purpose**: Verify that the stored procedure correctly identifies and handles a missing `p_EintragsNr` parameter, logging an error and failing gracefully.
*   **Setup**:
    1.  Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.
    2.  The `project.dataset.r_ausd_vertrag_control_sp` is deployed.
*   **Action**: Call the stored procedure with a `NULL` `p_EintragsNr`:
    ```sql
    BEGIN
        CALL project.dataset.r_ausd_vertrag_control_sp('TEST_JOB_MISSING_ENTRY', NULL);
    EXCEPTION WHEN ERROR THEN
        -- Error caught, continue for assertions
    END;
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure execution raises an error (e.g., `EintragsNr parameter cannot be empty.`).
    2.  Exactly one row exists in `project.dataset.job_error_log` for `job_kennung = 'TEST_JOB_MISSING_ENTRY'`.
    3.  The `err_nr` in `job_error_log` is '1002' and `err_arg` is 'PARAMETER_VALIDATION'.
    4.  The `message` in `job_error_log` contains 'EintragsNr parameter cannot be empty.'.
    5.  Exactly one row exists in `project.dataset.job_table` for `job_kennung = 'TEST_JOB_MISSING_ENTRY'`.
    6.  The `status` in `job_table` for this entry is 'FAILED'.
    7.  `record_count` in `job_table` is `NULL` or `0`.
    8.  `project.dataset.target_table` contains zero rows.
*   **Test Code (SQL Assertions)**:
    ```sql
    -- Setup: Clear tables before test
    TRUNCATE TABLE project.dataset.job_table;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.target_table;

    -- Action: Execute the stored procedure (expecting an error)
    BEGIN
        CALL project.dataset.r_ausd_vertrag_control_sp('TEST_JOB_MISSING_ENTRY', NULL);
    EXCEPTION WHEN ERROR THEN
        -- Error caught, now perform assertions
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM project.dataset.job_error_log WHERE job_kennung = 'TEST_JOB_MISSING_ENTRY' AND err_nr = '1002' AND err_arg = 'PARAMETER_VALIDATION' AND message LIKE '%EintragsNr parameter cannot be empty.%') = 1
                AND (SELECT COUNT(*) FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_MISSING_ENTRY' AND status = 'FAILED' AND (record_count = 0 OR record_count IS NULL)) = 1
                AND (SELECT COUNT(*) FROM project.dataset.target_table) = 0
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
    END;
    ```

### Test Case 4: Transformation Correctness - Simulated SQL Error

*   **Purpose**: Verify that if the underlying SQL transformation (`d_ausd_v_ta_vvl_dwh_migrated.sql`) fails, the orchestration procedure catches the error, logs it, and updates the `job_table` with a 'FAILED' status. This simulates a failure in `starteSQLSkript` and subsequent `DWMSG_MeldeFehler`.
*   **Setup**:
    1.  Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.
    2.  Temporarily modify the `EXECUTE IMMEDIATE` block within `project.dataset.r_ausd_vertrag_control_sp` to explicitly `RAISE` an error, simulating a SQL transformation failure.
        ```sql
        -- Temporary modification within project.dataset.r_ausd_vertrag_control_sp for testing:
        EXECUTE IMMEDIATE """
            RAISE USING MESSAGE 'Simulated SQL transformation error!';
        """;
        -- SET v_records_processed = @@row_count; -- This line would not be reached
        ```
*   **Action**: Call the stored procedure with valid parameters:
    ```sql
    BEGIN
        CALL project.dataset.r_ausd_vertrag_control_sp('TEST_JOB_SQL_ERROR', 'ENTRY_003');
    EXCEPTION WHEN ERROR THEN
        -- Error caught, continue for assertions
    END;
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure execution raises an error (e.g., `Transformation failed: Simulated SQL transformation error!`).
    2.  Exactly one row exists in `project.dataset.job_error_log` for `job_kennung = 'TEST_JOB_SQL_ERROR'` and `eintrags_nr = 'ENTRY_003'`.
    3.  The `err_nr` in `job_error_log` is '9999' and `err_arg` is 'TRANSFORMATION_ERROR'.
    4.  The `message` in `job_error_log` contains 'Simulated SQL transformation error!'.
    5.  Exactly one row exists in `project.dataset.job_table` for `job_kennung = 'TEST_JOB_SQL_ERROR'` and `eintrags_nr = 'ENTRY_003'`.
    6.  The `status` in `job_table` for this entry is 'FAILED'.
    7.  `record_count` in `job_table` is `NULL` or `0`.
    8.  `project.dataset.target_table` contains zero rows (no data inserted).
*   **Test Code (SQL Assertions)**:
    ```sql
    -- Setup: Clear tables before test
    TRUNCATE TABLE project.dataset.job_table;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.target_table;

    -- IMPORTANT: Manually modify project.dataset.r_ausd_vertrag_control_sp to include:
    -- EXECUTE IMMEDIATE """ RAISE USING MESSAGE 'Simulated SQL transformation error!'; """;
    -- for the core logic part, then re-deploy the SP.

    -- Action: Execute the stored procedure (expecting an error)
    BEGIN
        CALL project.dataset.r_ausd_vertrag_control_sp('TEST_JOB_SQL_ERROR', 'ENTRY_003');
    EXCEPTION WHEN ERROR THEN
        -- Error caught, now perform assertions
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM project.dataset.job_error_log WHERE job_kennung = 'TEST_JOB_SQL_ERROR' AND eintrags_nr = 'ENTRY_003' AND err_nr = '9999' AND err_arg = 'TRANSFORMATION_ERROR' AND message LIKE '%Simulated SQL transformation error!%') = 1
                AND (SELECT COUNT(*) FROM project.dataset.job_table WHERE job_kennung = 'TEST_JOB_SQL_ERROR' AND eintrags_nr = 'ENTRY_003' AND status = 'FAILED' AND (record_count = 0 OR record_count IS NULL)) = 1
                AND (SELECT COUNT(*) FROM project.dataset.target_table) = 0
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
    END;

    -- IMPORTANT: Revert project.dataset.r_ausd_vertrag_control_sp to its original (simulated successful insert) state after this test.
    ```

### Test Case 5: Data Quality - Schema Assertions for Control Tables

*   **Purpose**: Verify that the DDLs for `job_table` and `job_error_log` create tables with the expected schema, data types, and nullability constraints. This ensures the logging and tracking mechanisms are robust.
*   **Setup**:
    1.  Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are created using the provided DDLs.
*   **Action**: Query the BigQuery `INFORMATION_SCHEMA` for the table definitions.
*   **Pass/Fail Criterion**:
    1.  `project.dataset.job_table` has exactly 7 columns with the specified names, data types, and nullability: `job_kennung` (STRING, NOT NULL), `eintrags_nr` (STRING, NOT NULL), `tab_name` (STRING, NULLABLE), `status` (STRING, NULLABLE), `record_count` (INT64, NULLABLE), `created_ts` (TIMESTAMP, NULLABLE), `finished_ts` (TIMESTAMP, NULLABLE).
    2.  `project.dataset.job_error_log` has exactly 6 columns with the specified names, data types, and nullability: `job_kennung` (STRING, NOT NULL), `eintrags_nr` (STRING, NOT NULL), `err_nr` (STRING, NULLABLE), `err_arg` (STRING, NULLABLE), `created_ts` (TIMESTAMP, NULLABLE), `message` (STRING, NULLABLE).
*   **Test Code (SQL Assertions)**:
    ```sql
    -- Assertions for job_table schema
    SELECT
        CASE
            WHEN (
                SELECT COUNT(*)
                FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = 'job_table'
                AND (
                    (column_name = 'job_kennung' AND data_type = 'STRING' AND is_nullable = 'NO') OR
                    (column_name = 'eintrags_nr' AND data_type = 'STRING' AND is_nullable = 'NO') OR
                    (column_name = 'tab_name' AND data_type = 'STRING' AND is_nullable = 'YES') OR
                    (column_name = 'status' AND data_type = 'STRING' AND is_nullable = 'YES') OR
                    (column_name = 'record_count' AND data_type = 'INT64' AND is_nullable = 'YES') OR
                    (column_name = 'created_ts' AND data_type = 'TIMESTAMP' AND is_nullable = 'YES') OR
                    (column_name = 'finished_ts' AND data_type = 'TIMESTAMP' AND is_nullable = 'YES')
                )
            ) = 7 -- Check for 7 matching columns
            AND (SELECT COUNT(*) FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_table') = 7 -- Check for no extra columns
            THEN 'PASS'
            ELSE 'FAIL'
        END AS job_table_schema_test_result;

    -- Assertions for job_error_log schema
    SELECT
        CASE
            WHEN (
                SELECT COUNT(*)
                FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = 'job_error_log'
                AND (
                    (column_name = 'job_kennung' AND data_type = 'STRING' AND is_nullable = 'NO') OR
                    (column_name = 'eintrags_nr' AND data_type = 'STRING' AND is_nullable = 'NO') OR
                    (column_name = 'err_nr' AND data_type = 'STRING' AND is_nullable = 'YES') OR
                    (column_name = 'err_arg' AND data_type = 'STRING' AND is_nullable = 'YES') OR
                    (column_name = 'created_ts' AND data_type = 'TIMESTAMP' AND is_nullable = 'YES') OR
                    (column_name = 'message' AND data_type = 'STRING' AND is_nullable = 'YES')
                )
            ) = 6 -- Check for 6 matching columns
            AND (SELECT COUNT(*) FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_error_log') = 6 -- Check for no extra columns
            THEN 'PASS'
            ELSE 'FAIL'
        END AS job_error_log_schema_test_result;
    ```

### Test Case 6: External System Replacement - Temporary File Elimination

*   **Purpose**: Verify that the mechanism for capturing record counts no longer relies on a temporary file on the local filesystem, but is handled internally by BigQuery variables/mechanisms. This addresses the replacement of `$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_dwh_$$.tmp`.
*   **Setup**:
    1.  The `project.dataset.r_ausd_vertrag_control_sp` is deployed (with the simulated `INSERT 10 rows` logic from Test Case 1).
    2.  Ensure `project.dataset.job_table` is empty.
*   **Action**: Execute the stored procedure.
    ```sql
    CALL project.dataset.r_ausd_vertrag_control_sp('TEMP_FILE_REPLACEMENT_TEST', 'ENTRY_004');
    ```
*   **Pass/Fail Criterion**:
    1.  The `record_count` in `project.dataset.job_table` is correctly populated (e.g., `10`), demonstrating internal capture.
    2.  No temporary files are created on any accessible local filesystem during the BigQuery stored procedure execution (this is an inherent characteristic of BigQuery's managed service model, which does not expose local filesystem access for user code).
*   **Test Code (Conceptual / Observational)**:
    This test is primarily conceptual, verifying an architectural change rather than a direct code output. BigQuery Stored Procedures operate within the BigQuery environment and do not have access to a local filesystem in the way a shell script does. The `v_records_processed` variable and `@@row_count` mechanism directly replace the temporary file.
    ```python
    # This is a conceptual test, typically verified by understanding the BigQuery architecture.
    # In a Python-based testing framework (e.g., pytest with BigQuery client), you might do:

    # from google.cloud import bigquery
    # client = bigquery.Client()
    # project_id = "your-gcp-project"
    # dataset_id = "your_dataset"

    # job_kennung = 'TEMP_FILE_REPLACEMENT_TEST'
    # eintrags_nr = 'ENTRY_004'

    # # Call the BigQuery Stored Procedure
    # query = f"CALL {project_id}.{dataset_id}.r_ausd_vertrag_control_sp('{job_kennung}', '{eintrags_nr}');"
    # job = client.query(query)
    # job.result() # Wait for job to complete

    # # Assert that record_count is correctly captured internally
    # query_check_job_table = f"""
    #     SELECT record_count FROM {project_id}.{dataset_id}.job_table
    #     WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';
    # """
    # rows = client.query(query_check_job_table).result()
    # record_count = [row.record_count for row in rows][0]

    # assert record_count == 10, "Record count should be 10, indicating internal capture."

    # print("BigQuery Stored Procedures inherently do not create local temporary files.")
    # print("The record count was successfully captured internally and stored in job_table.")

    -- SQL equivalent for checking record count (part of the pass criterion)
    TRUNCATE TABLE project.dataset.job_table;
    TRUNCATE TABLE project.dataset.target_table; -- Clear target table too for clean state

    CALL project.dataset.r_ausd_vertrag_control_sp('TEMP_FILE_REPLACEMENT_TEST', 'ENTRY_004');

    SELECT
        CASE
            WHEN (SELECT record_count FROM project.dataset.job_table WHERE job_kennung = 'TEMP_FILE_REPLACEMENT_TEST' AND eintrags_nr = 'ENTRY_004') = 10
            THEN 'PASS'
            ELSE 'FAIL'
        END AS record_count_internal_capture_test_result;
    ```

### Test Case 7: Transformation Correctness - Core SQL Logic (Placeholder)

*   **Purpose**: To ensure that when the actual `d_ausd_v_ta_vvl_dwh_migrated.sql` is integrated, it correctly performs the data transformations (joins, aggregations, filters, type handling, NULL handling, edge cases) as per the original `d_ausd_v_ta_vvl_dwh.sql`. This test case serves as a placeholder and a reminder for when the actual SQL is available.
*   **Setup**:
    1.  **Crucially, the actual `d_ausd_v_ta_vvl_dwh.sql` must be migrated to BigQuery SQL and integrated into `project.dataset.r_ausd_vertrag_control_sp`**, replacing the placeholder `INSERT 10 rows` logic.
    2.  Populate `project.dataset.source_table` (the assumed source for `d_ausd_v_ta_vvl_dwh.sql`) with a comprehensive dataset, including various data types, NULL values, and edge cases relevant to the original SQL's logic.
    3.  Create `project.dataset.expected_target_table` with the exact schema and data that the migrated SQL is expected to produce from the `source_table`.
    4.  Ensure `project.dataset.target_table` has the correct schema as per the migrated SQL.
*   **Action**: Call the stored procedure with appropriate parameters.
    ```sql
    TRUNCATE TABLE project.dataset.target_table; -- Ensure a clean slate
    CALL project.dataset.r_ausd_vertrag_control_sp('ACTUAL_TRANSFORM_TEST', 'ENTRY_005');
    ```
*   **Pass/Fail Criterion**:
    1.  The data in `project.dataset.target_table` exactly matches the data in `project.dataset.expected_target_table` (row count, column values, data types).
    2.  The `record_count` in `project.dataset.job_table` for `job_kennung = 'ACTUAL_TRANSFORM_TEST'` accurately reflects the number of rows processed/inserted/updated by the core SQL logic, matching the count in `expected_target_table`.
    3.  The job status in `job_table` is 'COMPLETED'.
*   **Test Code (Conceptual / Placeholder)**:
    ```sql
    -- This test is critical and requires the actual d_ausd_v_ta_vvl_dwh.sql content to be migrated
    -- and integrated into project.dataset.r_ausd_vertrag_control_sp.

    -- Setup (Example):
    -- 1. Create and populate project.dataset.source_table with diverse test data.
    --    CREATE TABLE project.dataset.source_table (
    --        id INT64, value STRING, amount NUMERIC, status STRING, created_date DATE, nullable_field STRING
    --    );
    --    INSERT INTO project.dataset.source_table VALUES
    --    (1, 'A', 10.5, 'ACTIVE', '2023-01-01', 'data'),
    --    (2, 'B', 20.0, 'INACTIVE', '2023-01-02', NULL),
    --    (3, 'C', 15.2, 'ACTIVE', '2023-01-03', 'more data'),
    --    (4, 'D', NULL, 'ACTIVE', '2023-01-04', 'even more data'),
    --    (5, 'E', 5.0, 'ACTIVE', '2023-01-05', 'final data');
    -- 2. Create project.dataset.expected_target_table with the expected output.
    --    CREATE TABLE project.dataset.expected_target_table ( ... );
    --    INSERT INTO project.dataset.expected_target_table VALUES ( ... );
    -- 3. Ensure project.dataset.target_table has the correct schema.
    -- 4. Modify project.dataset.r_ausd_vertrag_control_sp to execute the actual migrated SQL.

    -- Action:
    TRUNCATE TABLE project.dataset.target_table;
    TRUNCATE TABLE project.dataset.job_table;
    TRUNCATE TABLE project.dataset.job_error_log;
    CALL project.dataset.r_ausd_vertrag_control_sp('ACTUAL_TRANSFORM_TEST', 'ENTRY_005');

    -- Pass/Fail Criterion (Example using a checksum for data comparison):
    SELECT
        CASE
            WHEN (SELECT COUNT(*) FROM project.dataset.target_table) = (SELECT COUNT(*) FROM project.dataset.expected_target_table)
            AND (SELECT SUM(FARM_FINGERPRINT(TO_JSON_STRING(t))) FROM project.dataset.target_table AS t) =
                (SELECT SUM(FARM_FINGERPRINT(TO_JSON_STRING(e))) FROM project.dataset.expected_target_table AS e)
            AND (SELECT record_count FROM project.dataset.job_table WHERE job_kennung = 'ACTUAL_TRANSFORM_TEST' AND eintrags_nr = 'ENTRY_005') = (SELECT COUNT(*) FROM project.dataset.expected_target_table)
            AND (SELECT status FROM project.dataset.job_table WHERE job_kennung = 'ACTUAL_TRANSFORM_TEST' AND eintrags_nr = 'ENTRY_005') = 'COMPLETED'
            THEN 'PASS'
            ELSE 'FAIL'
        END AS transformation_correctness_test_result;
    ```

### Test Case 8: Idempotency / Multiple Runs

*   **Purpose**: Verify that running the job multiple times with the same parameters behaves predictably. Given the simulated `INSERT` logic, this test will confirm that new rows are appended and job tracking is consistent. If the actual `d_ausd_v_ta_vvl_dwh.sql` uses `MERGE` or `TRUNCATE+INSERT`, the expected outcome would change (e.g., target table count remains the same).
*   **Setup**:
    1.  Ensure `project.dataset.job_table`, `project.dataset.job_error_log`, and `project.dataset.target_table` are empty.
    2.  The `project.dataset.r_ausd_vertrag_control_sp` is deployed with the simulated `INSERT 10 rows` logic (from Test Case 1).
*   **Action**: Call the stored procedure twice with the same parameters:
    ```sql
    CALL project.dataset.r_ausd_vertrag_control_sp('IDEMPOTENCY_TEST', 'ENTRY_006');
    CALL project.dataset.r_ausd_vertrag_control_sp('IDEMPOTENCY_TEST', 'ENTRY_006');
    ```
*   **Pass/Fail Criterion**:
    1.  Exactly two rows exist in `project.dataset.job_table` for `job_kennung = 'IDEMPOTENCY_TEST'` and `eintrags_nr = 'ENTRY_006'`.
    2.  Both job entries have a `status` of 'COMPLETED' and `record_count` of `10`.
    3.  `project.dataset.target_table` contains exactly 20 rows (10 from each run).
    4.  `project.dataset.job_error_log` contains zero rows.
*   **Test Code (SQL Assertions)**:
    ```sql
    -- Setup: Clear tables before test
    TRUNCATE TABLE project.dataset.job_table;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.target_table;

    -- Action: Execute the stored procedure twice
    CALL project.dataset.r_ausd_vertrag_control_sp('IDEMPOTENCY_TEST', 'ENTRY_006');
    CALL project.dataset.r_ausd_vertrag_control_sp('IDEMPOTENCY_TEST', 'ENTRY_006');

    -- Assertions
    SELECT
        CASE
            WHEN (SELECT COUNT(*) FROM project.dataset.job_table WHERE job_kennung = 'IDEMPOTENCY_TEST' AND eintrags_nr = 'ENTRY_006' AND status = 'COMPLETED' AND record_count = 10) = 2
            AND (SELECT COUNT(*) FROM project.dataset.job_error_log) = 0
            AND (SELECT COUNT(*) FROM project.dataset.target_table) = 20
            THEN 'PASS'
            ELSE 'FAIL'
        END AS idempotency_test_result;
    ```