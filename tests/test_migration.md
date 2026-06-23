The migration of `k_ausd_v_ta_barrier_zusgf.ksh` to a BigQuery Stored Procedure (`proc_ausd_v_ta_barrier_zusgf`) primarily involves transforming control flow, parameter handling, job management, and external system interactions (like temporary files and Oracle SQL execution) into BigQuery SQL. The core data transformation logic, originally in `d_ausd_v_ta_barrier_zusgf.sql`, is assumed to be migrated into a separate BigQuery Stored Procedure (`proc_d_ausd_v_ta_barrier_zusgf`).

The following tests focus on validating the behavior of the orchestrating `proc_ausd_v_ta_barrier_zusgf` and its interaction with the data processing component and job management tables.

---

## Pre-requisites for all Tests

1.  **BigQuery Environment Setup:** A BigQuery project and dataset (e.g., `project.dataset`) are available.
2.  **Table DDLs Executed:** All DDLs provided in the migration design document have been executed, creating the necessary tables:
    *   `project.dataset.job_error_log`
    *   `project.dataset.job_table`
    *   `project.dataset.ta_barrier_zusgf` (target result table)
    *   `project.dataset.job_run_log`
    *   `project.dataset.ta_barrier` (source table, inferred)
    *   `project.dataset.dwtk_meldungen` (source table, inferred)
3.  **Stored Procedures Deployed:**
    *   `project.dataset.proc_ausd_v_ta_barrier_zusgf` (the main orchestrator, under test)
    *   `project.dataset.proc_d_ausd_v_ta_barrier_zusgf` (the data processing logic, mocked for these tests)

### Mock `proc_d_ausd_v_ta_barrier_zusgf` for Testing

To effectively test the orchestrator, we'll use a mock version of `proc_d_ausd_v_ta_barrier_zusgf` that allows us to simulate success/failure and control the number of rows inserted.

```sql
-- Mock for the data processing stored procedure
CREATE OR REPLACE PROCEDURE `project.dataset.proc_d_ausd_v_ta_barrier_zusgf`(
    IN p_SimulateFailure BOOL,
    IN p_RowsToInsert INT64
)
BEGIN
    IF p_SimulateFailure THEN
        RAISE USING MESSAGE 'Simulated failure in proc_d_ausd_v_ta_barrier_zusgf.';
    END IF;

    -- Clear previous data in the target table for a clean run in tests
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;

    -- Simulate data insertion
    FOR i IN 1 TO p_RowsToInsert DO
        INSERT INTO `project.dataset.ta_barrier_zusgf` (cntrct_id, sperrart_alle, sperrgrund_alle, stilllegungszeitraum_alle, sperrgrund_zusgf)
        VALUES (
            CAST(i + 1000 AS INT64),
            'Sperrart_' || CAST(i AS STRING),
            'Sperrgrund_' || CAST(i AS STRING),
            '2023-01-01 - 2023-12-31',
            MOD(i, 3) + 1
        );
    END FOR;
END;
```

### Conceptual `proc_ausd_v_ta_barrier_zusgf` (for context)

This is the general structure of the stored procedure being tested, reflecting the migration design.

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.proc_ausd_v_ta_barrier_zusgf`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_SimulateDataProcFailure BOOL DEFAULT FALSE, -- For testing
    IN p_DataProcRowsToInsert INT64 DEFAULT 5 -- For testing
)
BEGIN
    DECLARE v_records INT64;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_job_status STRING;
    DECLARE v_error_message STRING;

    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_job_status = 'FAILED'; -- Default to FAILED, update on success

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_error_message = 'FEHLER: Jobkennung ist nicht gesetzt.';
        INSERT INTO `project.dataset.job_error_log` (job_kennung, entry_nr, error_code, error_message)
        VALUES (COALESCE(p_JobKennung, 'UNKNOWN'), COALESCE(p_EintragsNr, 'UNKNOWN'), '193', v_error_message);
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_error_message = 'FEHLER: EintragsNr ist nicht gesetzt.';
        INSERT INTO `project.dataset.job_error_log` (job_kennung, entry_nr, error_code, error_message)
        VALUES (p_JobKennung, COALESCE(p_EintragsNr, 'UNKNOWN'), '193', v_error_message);
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- Job Management: Check if already active
    IF EXISTS (SELECT 1 FROM `project.dataset.job_table` WHERE job_kennung = p_JobKennung AND entry_nr = p_EintragsNr AND status = 'ACTIVE') THEN
        -- Job is already active, ignore as per legacy design
        SELECT FORMAT('Job %s/%s is already active. Ignoring execution.', p_JobKennung, p_EintragsNr);
        RETURN; -- Exit gracefully
    END IF;

    -- Job Management: Deactivate older active jobs for the same JobKennung
    UPDATE `project.dataset.job_table`
    SET status = 'INACTIVE', end_time = CURRENT_TIMESTAMP(), last_updated = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung AND status = 'ACTIVE';

    -- Register current job as active
    INSERT INTO `project.dataset.job_table` (job_kennung, entry_nr, status, start_time)
    VALUES (p_JobKennung, p_EintragsNr, 'ACTIVE', v_start_time);

    BEGIN
        -- Invoke core data processing
        CALL `project.dataset.proc_d_ausd_v_ta_barrier_zusgf`(p_SimulateDataProcFailure, p_DataProcRowsToInsert);

        -- Get record count from the target table
        SELECT COUNT(*) INTO v_records FROM `project.dataset.ta_barrier_zusgf`;

        SET v_job_status = 'COMPLETED';

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        INSERT INTO `project.dataset.job_error_log` (job_kennung, entry_nr, error_code, error_message)
        VALUES (p_JobKennung, p_EintragsNr, 'SQL_ERROR', v_error_message);
        SET v_records = 0; -- No records processed on failure
        SET v_job_status = 'FAILED';
    END;

    SET v_end_time = CURRENT_TIMESTAMP();

    -- Update job_table status
    UPDATE `project.dataset.job_table`
    SET status = v_job_status, end_time = v_end_time, last_updated = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung AND entry_nr = p_EintragsNr;

    -- Log job run details
    INSERT INTO `project.dataset.job_run_log` (job_kennung, entry_nr, start_time, end_time, record_count, status)
    VALUES (p_JobKennung, p_EintragsNr, v_start_time, v_end_time, v_records, v_job_status);

    -- Return success message (or record count)
    IF v_job_status = 'COMPLETED' THEN
        SELECT FORMAT('Job %s/%s completed successfully. Processed %d records.', p_JobKennung, p_EintragsNr, v_records);
    ELSE
        SELECT FORMAT('Job %s/%s failed. Error: %s', p_JobKennung, p_EintragsNr, v_error_message);
    END IF;
END;
```

---

## Migration Validation Tests

### Test Case 1: Successful Execution with Valid Parameters

*   **Purpose:** Verify the end-to-end flow for a successful run, including parameter handling, job registration, data processing invocation, record count retrieval, and logging. This covers output parity for job status and record count.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.job_run_log`;
    TRUNCATE TABLE `project.dataset.job_error_log`;
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
    ```
*   **Action:** Call the main stored procedure with valid parameters, simulating a successful data processing run that inserts 5 rows.
    ```sql
    CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`('TEST_JOB_01', '001', FALSE, 5);
    ```
*   **Pass/Fail Criteria:**
    *   `project.dataset.job_table` should contain one entry for `('TEST_JOB_01', '001')` with `status = 'COMPLETED'`.
    *   `project.dataset.job_run_log` should contain one entry for `('TEST_JOB_01', '001')` with `status = 'SUCCESS'` and `record_count = 5`.
    *   `project.dataset.ta_barrier_zusgf` should contain exactly 5 rows.
    *   `project.dataset.job_error_log` should be empty.

### Test Case 2: Parameter Validation - Missing JobKennung

*   **Purpose:** Verify that the procedure correctly handles a missing `p_JobKennung` parameter, logs an error, and prevents further execution.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.job_run_log`;
    TRUNCATE TABLE `project.dataset.job_error_log`;
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
    ```
*   **Action:** Call the main stored procedure with `p_JobKennung` as `NULL` (or an empty string) and a valid `p_EintragsNr`.
    ```sql
    -- Attempt to call with NULL JobKennung
    -- This call is expected to fail and raise an error.
    BEGIN
        CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`(NULL, '002', FALSE, 0);
    EXCEPTION WHEN ERROR THEN
        SELECT 'Expected error caught for missing JobKennung.';
    END;
    ```
*   **Pass/Fail Criteria:**
    *   The `CALL` statement should terminate with an error message indicating `Jobkennung ist nicht gesetzt.`.
    *   `project.dataset.job_error_log` should contain one entry with `error_code = '193'` and `error_message` indicating missing `Jobkennung`.
    *   `project.dataset.job_table` should remain empty (no job registered).
    *   `project.dataset.job_run_log` should remain empty.
    *   `project.dataset.ta_barrier_zusgf` should remain empty.

### Test Case 3: Parameter Validation - Missing EintragsNr

*   **Purpose:** Verify that the procedure correctly handles a missing `p_EintragsNr` parameter, logs an error, and prevents further execution.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.job_run_log`;
    TRUNCATE TABLE `project.dataset.job_error_log`;
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
    ```
*   **Action:** Call the main stored procedure with a valid `p_JobKennung` and `p_EintragsNr` as `NULL`.
    ```sql
    -- Attempt to call with NULL EintragsNr
    -- This call is expected to fail and raise an error.
    BEGIN
        CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`('TEST_JOB_03', NULL, FALSE, 0);
    EXCEPTION WHEN ERROR THEN
        SELECT 'Expected error caught for missing EintragsNr.';
    END;
    ```
*   **Pass/Fail Criteria:**
    *   The `CALL` statement should terminate with an error message indicating `EintragsNr ist nicht gesetzt.`.
    *   `project.dataset.job_error_log` should contain one entry with `error_code = '193'` and `error_message` indicating missing `EintragsNr`.
    *   `project.dataset.job_table` should remain empty.
    *   `project.dataset.job_run_log` should remain empty.
    *   `project.dataset.ta_barrier_zusgf` should remain empty.

### Test Case 4: Job Management - Ignoring Already Active Job

*   **Purpose:** Verify that if a job with the same `JobKennung` and `EintragsNr` is already marked as `ACTIVE` in `job_table`, the new execution is ignored without re-processing data or changing job status. This directly tests the "aktive Jobs werden ignoriert" logic.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.job_run_log`;
    TRUNCATE TABLE `project.dataset.job_error_log`;
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;

    -- Insert an active job entry
    INSERT INTO `project.dataset.job_table` (job_kennung, entry_nr, status, start_time)
    VALUES ('TEST_JOB_04', '001', 'ACTIVE', TIMESTAMP('2023-01-01 10:00:00'));

    -- Insert some initial data into the target table
    INSERT INTO `project.dataset.ta_barrier_zusgf` (cntrct_id, sperrart_alle, sperrgrund_alle, stilllegungszeitraum_alle, sperrgrund_zusgf)
    VALUES (1000, 'Initial', 'Data', '2022-01-01 - 2022-12-31', 1);
    ```
*   **Action:** Call the main stored procedure with the same `JobKennung` and `EintragsNr` as the active job.
    ```sql
    CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`('TEST_JOB_04', '001', FALSE, 5);
    ```
*   **Pass/Fail Criteria:**
    *   `project.dataset.job_table` entry for `('TEST_JOB_04', '001')` should still have `status = 'ACTIVE'` and `start_time = '2023-01-01 10:00:00'`. No `end_time` or `last_updated` should be set by this call.
    *   `project.dataset.job_run_log` should *not* have a new entry for this run.
    *   `project.dataset.ta_barrier_zusgf` should still contain only the initial 1 row, meaning `proc_d_ausd_v_ta_barrier_zusgf` was not called.
    *   `project.dataset.job_error_log` should be empty.

### Test Case 5: Job Management - Deactivating Older Active Jobs

*   **Purpose:** Verify that the procedure deactivates older active jobs for the same `JobKennung` before starting a new one, as per the "alte aktive Jobs werden einfach dekativiert" design point.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.job_run_log`;
    TRUNCATE TABLE `project.dataset.job_error_log`;
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;

    -- Insert multiple active job entries for the same JobKennung
    INSERT INTO `project.dataset.job_table` (job_kennung, entry_nr, status, start_time) VALUES
    ('TEST_JOB_05', '001', 'ACTIVE', TIMESTAMP('2023-01-01 10:00:00')),
    ('TEST_JOB_05', '002', 'ACTIVE', TIMESTAMP('2023-01-01 11:00:00')),
    ('OTHER_JOB', '001', 'ACTIVE', TIMESTAMP('2023-01-01 10:30:00'));
    ```
*   **Action:** Call the main stored procedure with a new `EintragsNr` for `TEST_JOB_05`.
    ```sql
    CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`('TEST_JOB_05', '003', FALSE, 7);
    ```
*   **Pass/Fail Criteria:**
    *   `project.dataset.job_table` entries for `('TEST_JOB_05', '001')` and `('TEST_JOB_05', '002')` should be updated to `status = 'INACTIVE'`, with `end_time` and `last_updated` set.
    *   `project.dataset.job_table` should have a new entry for `('TEST_JOB_05', '003')` with `status = 'COMPLETED'`.
    *   `project.dataset.job_table` entry for `('OTHER_JOB', '001')` should remain `ACTIVE`.
    *   `project.dataset.job_run_log` should have one entry for `('TEST_JOB_05', '003')` with `status = 'SUCCESS'` and `record_count = 7`.
    *   `project.dataset.ta_barrier_zusgf` should contain exactly 7 rows.
    *   `project.dataset.job_error_log` should be empty.

### Test Case 6: Data Processing Failure

*   **Purpose:** Verify that if `proc_d_ausd_v_ta_barrier_zusgf` fails, the main procedure handles the error, logs it to `job_error_log`, and updates the job status in `job_table` and `job_run_log` to `FAILED`.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.job_run_log`;
    TRUNCATE TABLE `project.dataset.job_error_log`;
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
    ```
*   **Action:** Call the main stored procedure with valid parameters, but instruct the mock `proc_d_ausd_v_ta_barrier_zusgf` to simulate a failure.
    ```sql
    -- This call is expected to fail and raise an error.
    BEGIN
        CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`('TEST_JOB_06', '001', TRUE, 0);
    EXCEPTION WHEN ERROR THEN
        SELECT 'Expected error caught for data processing failure.';
    END;
    ```
*   **Pass/Fail Criteria:**
    *   The `CALL` statement should terminate with an error.
    *   `project.dataset.job_table` should have one entry for `('TEST_JOB_06', '001')` with `status = 'FAILED'`.
    *   `project.dataset.job_run_log` should have one entry for `('TEST_JOB_06', '001')` with `status = 'FAILED'` and `record_count = 0`.
    *   `project.dataset.job_error_log` should contain an entry detailing the error from `proc_d_ausd_v_ta_barrier_zusgf` (e.g., `Simulated failure...`).
    *   `project.dataset.ta_barrier_zusgf` should remain empty.

### Test Case 7: Record Count Parity (Output Parity)

*   **Purpose:** Verify that the record count reported by the BigQuery procedure matches the record count from the legacy script for the same input data. This is a critical output parity check.
*   **Setup:**
    1.  **Legacy Run:**
        *   Prepare a specific, known set of input data in the legacy Oracle tables (e.g., `ta_barrier`, `dwtk_meldungen`).
        *   Execute the legacy `k_ausd_v_ta_barrier_zusgf.ksh` script with specific `JobKennung` and `EintragsNr`.
        *   Capture the `v_records` value from the temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_barrier_zusgf_$$.tmp`). Let's assume this value is `LEGACY_RECORD_COUNT`.
        *   Capture the final state of the legacy `ta_barrier_zusgf` table.
    2.  **BigQuery Setup:**
        ```sql
        TRUNCATE TABLE `project.dataset.job_table`;
        TRUNCATE TABLE `project.dataset.job_run_log`;
        TRUNCATE TABLE `project.dataset.job_error_log`;
        TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
        -- Load the *exact same* input data into BigQuery tables:
        -- INSERT INTO `project.dataset.ta_barrier` (...) VALUES (...);
        -- INSERT INTO `project.dataset.dwtk_meldungen` (...) VALUES (...);
        ```
        *Note: For this test, `proc_d_ausd_v_ta_barrier_zusgf` should be the *actual* migrated logic, not the mock, to ensure true data transformation parity.*
*   **Action:** Call `proc_ausd_v_ta_barrier_zusgf` with the *same* `JobKennung` and `EintragsNr` used for the legacy run.
    ```sql
    CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`('TEST_JOB_07', '001', FALSE, LEGACY_RECORD_COUNT); -- LEGACY_RECORD_COUNT is passed to mock, but actual proc would derive it
    ```
*   **Pass/Fail Criteria:**
    *   Query `project.dataset.job_run_log` for `('TEST_JOB_07', '001')`. The `record_count` in this entry must exactly match `LEGACY_RECORD_COUNT`.
    *   The data in `project.dataset.ta_barrier_zusgf` must be *identical* (row count, column values, data types, NULL handling) to the data produced by the legacy `d_ausd_v_ta_barrier_zusgf.sql` script. This requires a detailed comparison query.
        ```sql
        -- Example comparison (assuming you can load legacy output to a temp BQ table)
        SELECT
            (SELECT COUNT(*) FROM `project.dataset.ta_barrier_zusgf`) = (SELECT COUNT(*) FROM `project.dataset.legacy_ta_barrier_zusgf_output`) AS row_count_match,
            (SELECT COUNT(*) FROM (
                SELECT * FROM `project.dataset.ta_barrier_zusgf`
                EXCEPT DISTINCT
                SELECT * FROM `project.dataset.legacy_ta_barrier_zusgf_output`
            )) = 0 AS data_match;
        ```

### Test Case 8: Transformation Correctness (Core SQL Logic)

*   **Purpose:** Verify that the core data transformation logic within `proc_d_ausd_v_ta_barrier_zusgf` produces identical results to `d_ausd_v_ta_barrier_zusgf.sql` for various scenarios, including edge cases, joins, aggregations, filters, type handling, and NULL handling.
*   **Setup:**
    1.  **Legacy Runs:**
        *   Create diverse test data sets for `ta_barrier` and `dwtk_meldungen` in the legacy Oracle environment. These should cover:
            *   Normal data.
            *   Empty input tables.
            *   NULL values in critical columns (`sperrart`, `sperrgrund`, `sperr_beginn`, `sperr_ende`).
            *   Edge cases for date ranges (e.g., `sperr_beginn` > `sperr_ende`).
            *   Specific values for `sperrart`, `sperrgrund` that might trigger special logic (e.g., `ist_stillegung`).
            *   Data that results in zero output rows.
            *   Data that results in many output rows.
        *   For each test data set, execute the legacy `d_ausd_v_ta_barrier_zusgf.sql` and capture its output (e.g., by selecting from the target table `ta_barrier_zusgf` in Oracle). Store these as expected results.
    2.  **BigQuery Setup:**
        ```sql
        TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
        -- For each test data set:
        -- TRUNCATE TABLE `project.dataset.ta_barrier`;
        -- TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
        -- INSERT INTO `project.dataset.ta_barrier` (...) VALUES (...); -- Load specific test data
        -- INSERT INTO `project.dataset.dwtk_meldungen` (...) VALUES (...); -- Load specific test data
        ```
        *Note: This test directly targets `proc_d_ausd_v_ta_barrier_zusgf`. The mock should be replaced with the actual migrated procedure.*
*   **Action:** For each test data set, load it into BigQuery, then call `proc_d_ausd_v_ta_barrier_zusgf`.
    ```sql
    -- For each test data set:
    CALL `project.dataset.proc_d_ausd_v_ta_barrier_zusgf`(FALSE, 0); -- Parameters for actual proc, not mock
    ```
*   **Pass/Fail Criteria:**
    *   For each test data set, the data in `project.dataset.ta_barrier_zusgf` after running `proc_d_ausd_v_ta_barrier_zusgf` must be *exactly identical* (row count, column values, data types, NULL handling) to the expected output captured from the legacy `d_ausd_v_ta_barrier_zusgf.sql` for the corresponding input.
    *   This comparison should be automated using `EXCEPT DISTINCT` or similar row-by-row, column-by-column comparison queries.

### Test Case 9: External System Replacement - Temporary File for Record Count

*   **Purpose:** Verify that the BigQuery procedure correctly replaces the legacy temporary file mechanism for record count retrieval with a native BigQuery approach (e.g., `SELECT COUNT(*)`).
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.job_run_log`;
    TRUNCATE TABLE `project.dataset.job_error_log`;
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
    ```
*   **Action:** Call the main stored procedure, instructing the mock `proc_d_ausd_v_ta_barrier_zusgf` to insert a specific number of rows (e.g., 12 rows).
    ```sql
    CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`('TEST_JOB_09', '001', FALSE, 12);
    ```
*   **Pass/Fail Criteria:**
    *   `project.dataset.job_run_log` should contain one entry for `('TEST_JOB_09', '001')` with `status = 'SUCCESS'` and `record_count = 12`.
    *   This confirms that the count from `ta_barrier_zusgf` was correctly retrieved and logged, replacing the temporary file mechanism.

### Test Case 10: Data Quality / Schema Assertions

*   **Purpose:** Verify that the output table `ta_barrier_zusgf` adheres to the expected schema, data types, and basic data quality rules after the migration.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `project.dataset.job_table`;
    TRUNCATE TABLE `project.dataset.job_run_log`;
    TRUNCATE TABLE `project.dataset.job_error_log`;
    TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
    -- Run the procedure with a comprehensive set of input data that covers various scenarios
    -- and potential data issues, ensuring a good population of ta_barrier_zusgf.
    CALL `project.dataset.proc_ausd_v_ta_barrier_zusgf`('TEST_JOB_10', '001', FALSE, 100);
    ```
*   **Action:** Query the `project.dataset.ta_barrier_zusgf` table and its schema.
*   **Pass/Fail Criteria:**
    *   **Schema Match:** The schema of `project.dataset.ta_barrier_zusgf` must match the DDL provided in the migration design and the expected output schema from the legacy system.
        ```sql
        -- Assert column names, data types, and nullability
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM
            `project.dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE
            table_name = 'ta_barrier_zusgf'
        ORDER BY
            ordinal_position;
        -- Expected output should match:
        -- column_name        data_type   is_nullable
        -- cntrct_id          INT64       NO
        -- sperrart_alle      STRING      YES (or NO if legacy enforced it)
        -- sperrgrund_alle    STRING      YES (or NO if legacy enforced it)
        -- stilllegungszeitraum_alle STRING YES (or NO if legacy enforced it)
        -- sperrgrund_zusgf   INT64       YES (or NO if legacy enforced it)
        ```
    *   **Nullability Constraints:** No unexpected `NULL` values in columns defined as `NOT NULL`.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.ta_barrier_zusgf` WHERE cntrct_id IS NULL; -- Should be 0
        -- Add similar checks for other NOT NULL columns if applicable based on legacy behavior.
        ```
    *   **Data Range/Format:** If `sperrgrund_zusgf` has an expected range (e.g., 1, 2, 3), assert it.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.ta_barrier_zusgf` WHERE sperrgrund_zusgf NOT IN (1, 2, 3); -- Should be 0 if these are the only valid values
        ```
    *   **Uniqueness (if applicable):** If `cntrct_id` is expected to be unique, assert it.
        ```sql
        SELECT cntrct_id FROM `project.dataset.ta_barrier_zusgf` GROUP BY cntrct_id HAVING COUNT(*) > 1; -- Should return 0 rows
        ```