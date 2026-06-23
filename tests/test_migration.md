As a senior data-migration QA engineer, I've analyzed the provided KornShell script `k_ausd_v_ta_inv_acc.ksh` and the migration design. The core of the migration involves re-platforming the shell orchestration and its dependent SQL logic (`d_ausd_v_ta_inv_acc.sql`) to Google Cloud BigQuery, using BigQuery Stored Procedures for orchestration and BigQuery SQL for data transformations.

The tests below aim to prove behavioral equivalence, covering output parity, transformation correctness, external system replacements (specifically job control), and data quality/schema assertions. Since the `d_ausd_v_ta_inv_acc.sql` content is not provided, the transformation correctness tests are designed to be generic, focusing on common SQL constructs and data scenarios. Specific test data for these cases would be derived directly from the SQL script's logic.

---

## Test Case 1: End-to-End Data Parity (Happy Path)

*   **Purpose**: Verify that the migrated BigQuery stored procedure produces the exact same final dataset in the `ta_inv_acc` table as the legacy KornShell script when executed with valid, typical inputs. This covers output parity and transformation correctness for a standard scenario.
*   **Setup**:
    1.  **Legacy Environment**:
        *   Populate all source tables required by `d_ausd_v_ta_inv_acc.sql` in the Oracle database with a representative dataset (e.g., 10,000 records, including various data types, NULLs, and edge cases relevant to the SQL logic).
        *   Ensure the Oracle `ta_inv_acc` table is empty or in a known pre-execution state.
        *   Set up the Oracle job control table (if applicable) to indicate the job is not active for `TEST_JOB_INV_ACC`.
    2.  **BigQuery Environment**:
        *   Create and populate BigQuery tables corresponding to the Oracle source tables with *identical* data.
        *   Ensure the BigQuery `ta_inv_acc` table is empty or in the same known pre-execution state.
        *   Set up the BigQuery job control table to indicate the job is not active for `TEST_JOB_INV_ACC`.
        *   Deploy the migrated BigQuery Stored Procedure (e.g., `my_project.my_dataset.sp_ta_inv_acc_processing`).
*   **Action**:
    1.  **Legacy**: Execute the legacy KornShell script:
        ```bash
        ./k_ausd_v_ta_inv_acc.ksh -j "TEST_JOB_INV_ACC" -f "12345"
        ```
    2.  **BigQuery**: Execute the migrated BigQuery Stored Procedure:
        ```sql
        CALL my_project.my_dataset.sp_ta_inv_acc_processing('TEST_JOB_INV_ACC', 12345);
        ```
*   **Pass/Fail Criterion**:
    1.  **Row Count**: The number of records in the Oracle `ta_inv_acc` table must be identical to the number of records in the BigQuery `ta_inv_acc` table.
    2.  **Data Content**: Perform a row-by-row comparison of the `ta_inv_acc` tables. All columns in all rows must match exactly. This can be achieved by exporting both tables to a common format (e.g., CSV, Avro) and using a diff tool, or by loading the legacy data into a temporary BigQuery table and performing a SQL comparison.

    ```sql
    -- Example BigQuery comparison (assuming legacy data is loaded into `ta_inv_acc_legacy_export`)
    -- This query identifies any rows that exist in one table but not the other.
    SELECT 'Only in Legacy' AS source, * FROM (
      SELECT * FROM `my_project.my_dataset.ta_inv_acc_legacy_export`
      EXCEPT DISTINCT
      SELECT * FROM `my_project.my_dataset.ta_inv_acc`
    )
    UNION ALL
    SELECT 'Only in BigQuery' AS source, * FROM (
      SELECT * FROM `my_project.my_dataset.ta_inv_acc`
      EXCEPT DISTINCT
      SELECT * FROM `my_project.my_dataset.ta_inv_acc_legacy_export`
    );
    -- Pass if the above query returns 0 rows.
    ```

---

## Test Case 2: Parameter Handling and Validation

*   **Purpose**: Verify that the BigQuery stored procedure correctly parses and validates input parameters (`p_JobKennung`, `p_EintragsNr`), mirroring the `getopts` and `pruefeParameterGesetzt` logic of the KornShell script.
*   **Setup**:
    1.  **BigQuery Environment**: Deploy the migrated BigQuery Stored Procedure.
*   **Action**:
    1.  **Missing `p_JobKennung`**: Attempt to call the SP without providing the `p_JobKennung` parameter (e.g., pass `NULL` or an empty string if the SP expects a non-NULL/non-empty value).
    2.  **Missing `p_EintragsNr`**: Attempt to call the SP without providing the `p_EintragsNr` parameter.
    3.  **Valid Parameters**: Call with valid parameters.
*   **Pass/Fail Criterion**:
    1.  **Missing Parameters**: The BigQuery stored procedure should either:
        *   Fail gracefully with a clear error message indicating the missing parameter (similar to the legacy `DWMSG_MeldeFehler` and `exit $ErrNr`).
        *   Return a specific error code or status indicating parameter validation failure.
        *   (If BigQuery's parameter handling is stricter than shell, it might fail at call time, which is also acceptable if it prevents incorrect execution).
    2.  **Valid Parameters**: The SP should execute successfully without parameter-related errors.

    ```sql
    -- Example BigQuery calls for testing parameter validation
    -- Scenario 1: Missing JobKennung (assuming it's a required string parameter)
    -- CALL my_project.my_dataset.sp_ta_inv_acc_processing(NULL, 12345);
    -- Expected: Error message like "JobKennung cannot be NULL" or similar.

    -- Scenario 2: Missing EintragsNr (assuming it's a required integer parameter)
    -- CALL my_project.my_dataset.sp_ta_inv_acc_processing('TEST_JOB_INV_ACC', NULL);
    -- Expected: Error message like "EintragsNr cannot be NULL" or similar.

    -- Scenario 3: Valid Parameters
    CALL my_project.my_dataset.sp_ta_inv_acc_processing('TEST_JOB_INV_ACC', 12345);
    -- Expected: Successful execution.
    ```

---

## Test Case 3: Job Control - Active Job Handling

*   **Purpose**: Verify that the BigQuery stored procedure correctly identifies and handles cases where the job is already active, mirroring the "aktive Jobs werden ignoriert" logic from the legacy script.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   Deploy the migrated BigQuery Stored Procedure.
        *   Create and populate a `job_control_table` in BigQuery (e.g., `my_project.my_dataset.job_control_table`).
        *   Insert a record into `job_control_table` for `job_kennung = 'TEST_JOB_INV_ACC'` with `status = 'ACTIVE'` and a recent `start_time`.
        *   Ensure the `ta_inv_acc` table is in a known state (e.g., empty or containing specific pre-existing data).
*   **Action**:
    1.  Execute the BigQuery Stored Procedure with the `p_JobKennung` that is already marked as active:
        ```sql
        CALL my_project.my_dataset.sp_ta_inv_acc_processing('TEST_JOB_INV_ACC', 12345);
        ```
*   **Pass/Fail Criterion**:
    1.  The BigQuery stored procedure should *not* perform any data transformations on `ta_inv_acc`. The `ta_inv_acc` table should remain unchanged from its initial state.
    2.  The SP should exit gracefully, potentially logging a message indicating that the job was ignored because it was already active. It should not return a critical error code that would trigger alerts for a failed job, but rather an informational status or a specific return code indicating "job already active".
    3.  The `job_control_table` should *not* have a new entry for this execution, and the existing 'ACTIVE' entry should remain unchanged (unless the legacy system would update a timestamp on the active job, which should then be mirrored).

    ```sql
    -- Example BigQuery assertion (after calling the SP)
    -- Check if the target table was modified
    SELECT COUNT(*) FROM `my_project.my_dataset.ta_inv_acc`;
    -- Expected: Count should be 0 (or whatever the initial known state was).

    -- Check job control table status (assuming 'job_kennung', 'status', 'start_time' columns)
    SELECT job_kennung, status, start_time
    FROM `my_project.my_dataset.job_control_table`
    WHERE job_kennung = 'TEST_JOB_INV_ACC'
    ORDER BY start_time DESC
    LIMIT 1;
    -- Expected: status = 'ACTIVE' (reflecting no change by the ignored run), and start_time matches the pre-existing active job.
    ```

---

## Test Case 4: Job Control - Successful Execution and Status Update

*   **Purpose**: Verify that the BigQuery stored procedure correctly updates the job control table upon successful completion, including recording the number of processed records, mirroring the `starteSQLSkript` and `eval "v_records=\`cat $tmpFile\`"` logic.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   Deploy the migrated BigQuery Stored Procedure.
        *   Ensure the `job_control_table` has no active entry for `job_kennung = 'TEST_JOB_INV_ACC'`.
        *   Populate source tables with data that will result in a non-zero number of records being processed into `ta_inv_acc`.
        *   Ensure `ta_inv_acc` is empty.
*   **Action**:
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL my_project.my_dataset.sp_ta_inv_acc_processing('TEST_JOB_INV_ACC', 12345);
        ```
*   **Pass/Fail Criterion**:
    1.  The `ta_inv_acc` table should be populated with data.
    2.  A new entry should be present in the `job_control_table` for `job_kennung = 'TEST_JOB_INV_ACC'` with `status = 'SUCCESS'` (or equivalent).
    3.  The `job_control_table` entry should contain the correct number of processed records, matching the count of rows inserted/updated in `ta_inv_acc`.

    ```sql
    -- Example BigQuery assertion (after calling the SP)
    DECLARE processed_records INT64;
    SET processed_records = (SELECT COUNT(*) FROM `my_project.my_dataset.ta_inv_acc`);

    SELECT
      job_kennung,
      status,
      processed_record_count -- Assuming this column exists in job_control_table
    FROM `my_project.my_dataset.job_control_table`
    WHERE job_kennung = 'TEST_JOB_INV_ACC'
    ORDER BY start_time DESC
    LIMIT 1;
    -- Expected: status = 'SUCCESS' and processed_record_count = processed_records.
    ```

---

## Test Case 5: Job Control - Failed Execution and Status Update

*   **Purpose**: Verify that the BigQuery stored procedure correctly updates the job control table when an error occurs during data processing, covering error handling (`f_alis_msgerr.ksh`) and job status updates.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   Deploy the migrated BigQuery Stored Procedure.
        *   Ensure the `job_control_table` has no active entry for `job_kennung = 'TEST_JOB_INV_ACC'`.
        *   Manipulate source data or the SP logic (e.g., introduce a deliberate SQL error, violate a constraint, or pass invalid data that causes a transformation error) to force a failure during the data processing step.
        *   Ensure `ta_inv_acc` is empty.
*   **Action**:
    1.  Execute the BigQuery Stored Procedure with the setup designed to cause a failure:
        ```sql
        CALL my_project.my_dataset.sp_ta_inv_acc_processing('TEST_JOB_INV_ACC', 12345);
        ```
*   **Pass/Fail Criterion**:
    1.  The BigQuery stored procedure should terminate with an error.
    2.  A new entry should be present in the `job_control_table` for `job_kennung = 'TEST_JOB_INV_ACC'` with `status = 'FAILED'` (or equivalent).
    3.  The `job_control_table` entry should ideally contain an error message or code detailing the cause of the failure.
    4.  The `ta_inv_acc` table should either be empty (if the transaction was rolled back) or in a partially processed state, depending on the error handling and transaction management within the SP. The expected state should be defined based on the legacy system's behavior in case of failure.

    ```sql
    -- Example BigQuery assertion (after attempting to call the SP)
    SELECT
      job_kennung,
      status,
      error_message -- Assuming this column exists in job_control_table
    FROM `my_project.my_dataset.job_control_table`
    WHERE job_kennung = 'TEST_JOB_INV_ACC'
    ORDER BY start_time DESC
    LIMIT 1;
    -- Expected: status = 'FAILED' and error_message contains relevant details.
    ```

---

## Test Case 6: Data Transformation - NULL Handling and Edge Cases

*   **Purpose**: Verify that the migrated BigQuery SQL logic correctly handles NULL values, zero values, empty sets, and other specific edge cases as defined by the original `d_ausd_v_ta_inv_acc.sql` script. This is a deeper dive into transformation correctness.
*   **Setup**:
    1.  **Legacy Environment**:
        *   Populate Oracle source tables with specific test data designed to trigger edge cases:
            *   Rows with all NULLs in relevant columns.
            *   Rows with partial NULLs.
            *   Rows with zero values (if numeric columns are involved).
            *   Empty source tables (for specific inputs to joins/aggregations).
            *   Boundary date values (e.g., min/max dates, start/end of month/year).
            *   Data that might cause division by zero, type conversion errors, or specific conditional logic paths.
        *   Ensure `ta_inv_acc` is empty.
        *   Set job control to not active.
    2.  **BigQuery Environment**:
        *   Populate BigQuery source tables with *identical* specific test data.
        *   Ensure `ta_inv_acc` is empty.
        *   Set job control to not active.
        *   Deploy the migrated BigQuery Stored Procedure.
*   **Action**:
    1.  **Legacy**: Execute the legacy KornShell script.
    2.  **BigQuery**: Execute the migrated BigQuery Stored Procedure.
*   **Pass/Fail Criterion**:
    1.  Perform a row-by-row comparison of the Oracle `ta_inv_acc` table and the BigQuery `ta_inv_acc` table.
    2.  All columns, including those with NULLs, zero values, or specific edge-case data, must match exactly.
    3.  The number of records should be identical.

    ```sql
    -- Similar to Test Case 1, but specifically targeting the edge case data.
    -- The key is the meticulous setup of the source data to cover all identified edge cases from d_ausd_v_ta_inv_acc.sql.
    SELECT 'Only in Legacy (Edge Case)' AS source, * FROM (
      SELECT * FROM `my_project.my_dataset.ta_inv_acc_legacy_export_edge_case`
      EXCEPT DISTINCT
      SELECT * FROM `my_project.my_dataset.ta_inv_acc_edge_case`
    )
    UNION ALL
    SELECT 'Only in BigQuery (Edge Case)' AS source, * FROM (
      SELECT * FROM `my_project.my_dataset.ta_inv_acc_edge_case`
      EXCEPT DISTINCT
      SELECT * FROM `my_project.my_dataset.ta_inv_acc_legacy_export_edge_case`
    );
    -- Pass if the above query returns 0 rows.
    ```

---

## Test Case 7: Schema and Data Type Fidelity

*   **Purpose**: Verify that the schema (column names, data types, nullability) of the target `ta_inv_acc` table in BigQuery is functionally equivalent to the Oracle `ta_inv_acc` table, ensuring no loss of information or unexpected behavior due to type conversions.
*   **Setup**:
    1.  **Legacy Environment**: Identify the schema of the Oracle `ta_inv_acc` table using Oracle's data dictionary views (e.g., `ALL_TAB_COLUMNS`).
    2.  **BigQuery Environment**: Identify the schema of the BigQuery `ta_inv_acc` table using BigQuery's `INFORMATION_SCHEMA`.
*   **Action**:
    1.  Retrieve schema information for both tables.
*   **Pass/Fail Criterion**:
    1.  **Column Names**: All column names must be identical (considering case-sensitivity differences between Oracle and BigQuery if applicable).
    2.  **Data Types**: BigQuery data types must be appropriate mappings of the Oracle data types, ensuring no loss of precision or range. For example:
        *   Oracle `NUMBER(p,s)` -> BigQuery `NUMERIC` or `BIGNUMERIC` (for exact precision) or `FLOAT64` (if precision loss is acceptable).
        *   Oracle `DATE` / `TIMESTAMP` -> BigQuery `DATE` / `TIMESTAMP`.
        *   Oracle `VARCHAR2` / `CHAR` -> BigQuery `STRING`.
    3.  **Nullability**: Nullability constraints should be preserved where applicable. If a column was `NOT NULL` in Oracle, it should ideally be `NOT NULL` in BigQuery, or the transformation logic should guarantee non-NULL values.

    ```sql
    -- Example BigQuery query to get schema
    SELECT
      column_name,
      data_type,
      is_nullable
    FROM `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'ta_inv_acc'
    ORDER BY ordinal_position;

    -- Example Oracle query (to be run in Oracle)
    -- SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE
    -- FROM ALL_TAB_COLUMNS
    -- WHERE TABLE_NAME = 'TA_INV_ACC' AND OWNER = 'YOUR_SCHEMA'
    -- ORDER BY COLUMN_ID;

    -- Pass if the BigQuery schema is a functionally equivalent representation of the Oracle schema.
    ```

---

## Test Case 8: Performance Comparison (Non-Functional)

*   **Purpose**: While not strictly a behavioral equivalence test, performance is often a key driver for migration. This test aims to ensure the BigQuery solution meets or exceeds performance expectations under production-like load.
*   **Setup**:
    1.  **Legacy Environment**:
        *   Populate Oracle source tables with a large, production-like volume of data (e.g., 10x typical daily load, or a volume representative of peak processing).
        *   Ensure `ta_inv_acc` is empty.
        *   Set job control to not active.
    2.  **BigQuery Environment**:
        *   Populate BigQuery source tables with an *identical* large volume of data.
        *   Ensure `ta_inv_acc` is empty.
        *   Set job control to not active.
        *   Deploy the migrated BigQuery Stored Procedure.
*   **Action**:
    1.  **Legacy**: Execute the legacy KornShell script and accurately record its total execution time.
    2.  **BigQuery**: Execute the migrated BigQuery Stored Procedure and accurately record its total execution time.
*   **Pass/Fail Criterion**:
    1.  The BigQuery stored procedure's execution time should be within an acceptable threshold (e.g., 80% of the legacy execution time, or a specific SLA, e.g., "must complete within 1 hour").
    2.  The cost of the BigQuery job should be monitored and fall within expected budgets.

    ```python
    # Example Python/Pytest structure for performance testing
    import time
    import subprocess
    from google.cloud import bigquery

    def test_performance_comparison():
        print("Starting legacy job performance test...")
        legacy_start_time = time.time()
        # Replace with actual command to run legacy script
        # For example, using subprocess.run to execute the ksh script
        # result = subprocess.run(["./k_ausd_v_ta_inv_acc.ksh", "-j", "PERF_TEST", "-f", "99999"], capture_output=True, text=True, check=True)
        # print(f"Legacy output: {result.stdout}")
        # print(f"Legacy errors: {result.stderr}")
        # For demonstration, simulate a duration:
        time.sleep(600) # Simulate 10 minutes
        legacy_end_time = time.time()
        legacy_duration = legacy_end_time - legacy_start_time
        print(f"Legacy job duration: {legacy_duration:.2f} seconds")

        print("Starting BigQuery job performance test...")
        bq_client = bigquery.Client()
        bq_start_time = time.time()
        # Execute BigQuery Stored Procedure
        query_job = bq_client.query("CALL my_project.my_dataset.sp_ta_inv_acc_processing('PERF_TEST', 99999);")
        query_job.result() # Waits for job to complete
        bq_end_time = time.time()
        bq_duration = bq_end_time - bq_start_time
        print(f"BigQuery job duration: {bq_duration:.2f} seconds")

        # Assert BigQuery is faster or within tolerance
        # Example: BigQuery must be at least 20% faster (0.8 factor)
        assert bq_duration < legacy_duration * 0.8, f"BigQuery job ({bq_duration:.2f}s) is not significantly faster than legacy ({legacy_duration:.2f}s)."
        # Or, assert against an absolute SLA:
        # assert bq_duration < 3600, f"BigQuery job exceeded 1 hour SLA ({bq_duration:.2f}s)."
    ```