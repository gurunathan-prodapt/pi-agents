As a senior data-migration QA engineer, I've reviewed the migration design and the generated code for `k_ausd_bp_ta_bpr_opt_text.ksh` to a BigQuery Stored Procedure. The following test cases are designed to ensure the migrated solution is functionally equivalent and robust.

**Key Observation & Discrepancy:**
The migration design document states: "This logic reads from `source_dataset.PoolBasisprodukt` and other necessary BigQuery tables". However, the provided generated BigQuery Stored Procedure `r_ausd_bp_ta_bpr_opt_text` **does not reference `source_dataset.PoolBasisprodukt` at all**. Instead, its core logic performs an `INNER JOIN` between `project.dataset.sof_ta_bpr_optionen` and `project.dataset.sof_ta_bpr_beschr`.

For the purpose of these tests, I will assume the provided generated code is the definitive implementation of the core logic, and thus the tests will focus on `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr` as the primary source tables for the business logic. This discrepancy should be clarified with the development team.

---

## Migration Validation Tests for `k_ausd_bp_ta_bpr_opt_text.ksh`

**Global Setup:**
Before running any tests, ensure the following BigQuery tables and stored procedure are created:

*   `project.dataset.job_error_audit`
*   `project.dataset.job_run_audit`
*   `project.dataset.sof_ta_bpr_optionen`
*   `project.dataset.sof_ta_bpr_beschr`
*   `project.dataset.target_bp_ta_bpr_opt_text`
*   `project.dataset.r_ausd_bp_ta_bpr_opt_text` (the stored procedure)

For each test, the audit tables and target table should be cleared to ensure isolation.

```sql
-- Helper SQL to clear tables before each test run
DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;
-- Clear source tables if they are being dynamically populated for tests
DELETE FROM `project.dataset.sof_ta_bpr_optionen` WHERE TRUE;
DELETE FROM `project.dataset.sof_ta_bpr_beschr` WHERE TRUE;
```

---

### Test Case 1: Successful Execution with Valid Data

*   **Purpose:** Verify the core business logic executes correctly with valid input parameters and produces the expected output in the target table, along with proper audit logging.
*   **Setup:**
    1.  Clear all audit and target tables.
    2.  Populate source tables `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr` with matching data.

    ```sql
    -- Clear tables
    DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
    DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
    DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_optionen` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_beschr` WHERE TRUE;

    -- Populate source_dataset.sof_ta_bpr_optionen
    INSERT INTO `project.dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID) VALUES
    (101, 1),
    (102, 2),
    (103, 3),
    (104, 4);

    -- Populate source_dataset.sof_ta_bpr_beschr
    INSERT INTO `project.dataset.sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION) VALUES
    (1, 'Description for BPR 1'),
    (2, 'Description for BPR 2'),
    (3, 'Description for BPR 3');
    ```
*   **Action:** Execute the stored procedure with valid parameters.

    ```sql
    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        'TEST_JOB_1',
        'ENTRY_1',
        '01012023',
        '0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  `target_bp_ta_bpr_opt_text` contains 3 rows with the correct joined data.
    2.  `job_error_audit` contains 0 rows.
    3.  `job_run_audit` contains 2 rows: one 'RUNNING' and one 'SUCCESS' entry, with `processed_records` = 3.

    ```sql
    -- Check target table content
    SELECT CNTRCT_ID, BPR_ID, PDS_DESCRIPTION
    FROM `project.dataset.target_bp_ta_bpr_opt_text`
    ORDER BY CNTRCT_ID;
    -- Expected:
    -- CNTRCT_ID | BPR_ID | PDS_DESCRIPTION
    -- ----------|--------|--------------------
    -- 101       | 1      | Description for BPR 1
    -- 102       | 2      | Description for BPR 2
    -- 103       | 3      | Description for BPR 3

    -- Check job_error_audit
    SELECT COUNT(*) FROM `project.dataset.job_error_audit`;
    -- Expected: 0

    -- Check job_run_audit
    SELECT status, processed_records
    FROM `project.dataset.job_run_audit`
    WHERE job_id = 'TEST_JOB_1' AND entry_number = 'ENTRY_1'
    ORDER BY run_timestamp;
    -- Expected:
    -- status  | processed_records
    -- --------|------------------
    -- RUNNING | NULL
    -- SUCCESS | 3
    ```

---

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** Verify the stored procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and signaling SQLSTATE.
*   **Setup:**
    1.  Clear all audit and target tables.
    2.  Source tables can be empty or populated, as the error occurs before core logic execution.
*   **Action:** Execute the stored procedure with `p_JobKennung` as `NULL` or an empty string.

    ```sql
    -- Clear tables
    DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
    DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
    DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;

    -- Attempt to call with missing p_JobKennung (will raise an error)
    -- This call is expected to fail and should be wrapped in a TRY-CATCH block in a test runner
    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        NULL, -- Missing p_JobKennung
        'ENTRY_2',
        '02012023',
        '0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution fails with an `SQLSTATE '45000'` error.
    2.  `job_error_audit` contains 1 row with `error_message` indicating `p_JobKennung` is missing.
    3.  `job_run_audit` contains 0 rows (as the error occurs before the initial 'RUNNING' log).
    4.  `target_bp_ta_bpr_opt_text` contains 0 rows.

    ```sql
    -- Check job_error_audit
    SELECT error_message FROM `project.dataset.job_error_audit` WHERE job_id = 'UNKNOWN';
    -- Expected: 'ERROR: Parameter p_JobKennung is missing or empty.'

    -- Check job_run_audit
    SELECT COUNT(*) FROM `project.dataset.job_run_audit`;
    -- Expected: 0

    -- Check target table
    SELECT COUNT(*) FROM `project.dataset.target_bp_ta_bpr_opt_text`;
    -- Expected: 0
    ```

---

### Test Case 3: Parameter Validation - Invalid `p_Stichtag` Format

*   **Purpose:** Verify the stored procedure correctly identifies and handles an invalid `p_Stichtag` format, logging an error and signaling SQLSTATE.
*   **Setup:**
    1.  Clear all audit and target tables.
    2.  Source tables can be empty or populated.
*   **Action:** Execute the stored procedure with an incorrectly formatted `p_Stichtag`.

    ```sql
    -- Clear tables
    DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
    DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
    DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;

    -- Attempt to call with invalid p_Stichtag (will raise an error)
    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        'TEST_JOB_3',
        'ENTRY_3',
        '2023-01-03', -- Invalid format (YYYY-MM-DD instead of DDMMYYYY)
        '0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution fails with an `SQLSTATE '45000'` error.
    2.  `job_error_audit` contains 1 row with `error_message` indicating an invalid date format.
    3.  `job_run_audit` contains 0 rows.
    4.  `target_bp_ta_bpr_opt_text` contains 0 rows.

    ```sql
    -- Check job_error_audit
    SELECT error_message FROM `project.dataset.job_error_audit` WHERE job_id = 'TEST_JOB_3';
    -- Expected: 'ERROR: Invalid date format for p_Stichtag: 2023-01-03. Expected DDMMYYYY.'

    -- Check job_run_audit
    SELECT COUNT(*) FROM `project.dataset.job_run_audit`;
    -- Expected: 0

    -- Check target table
    SELECT COUNT(*) FROM `project.dataset.target_bp_ta_bpr_opt_text`;
    -- Expected: 0
    ```

---

### Test Case 4: Core Logic - No Matching Records

*   **Purpose:** Verify the core business logic correctly handles cases where the `INNER JOIN` yields no results, resulting in an empty target table and `processed_records` count of 0.
*   **Setup:**
    1.  Clear all audit and target tables.
    2.  Populate source tables such that `BPR_ID` values in `sof_ta_bpr_optionen` do not match any `BPR_ID` in `sof_ta_bpr_beschr`.

    ```sql
    -- Clear tables
    DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
    DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
    DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_optionen` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_beschr` WHERE TRUE;

    -- Populate sof_ta_bpr_optionen with BPR_IDs that won't match
    INSERT INTO `project.dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID) VALUES
    (201, 10),
    (202, 11);

    -- Populate sof_ta_bpr_beschr with different BPR_IDs
    INSERT INTO `project.dataset.sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION) VALUES
    (1, 'Description A'),
    (2, 'Description B');
    ```
*   **Action:** Execute the stored procedure with valid parameters.

    ```sql
    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        'TEST_JOB_4',
        'ENTRY_4',
        '04012023',
        '0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  `target_bp_ta_bpr_opt_text` contains 0 rows.
    2.  `job_error_audit` contains 0 rows.
    3.  `job_run_audit` contains 2 rows: one 'RUNNING' and one 'SUCCESS' entry, with `processed_records` = 0.

    ```sql
    -- Check target table content
    SELECT COUNT(*) FROM `project.dataset.target_bp_ta_bpr_opt_text`;
    -- Expected: 0

    -- Check job_error_audit
    SELECT COUNT(*) FROM `project.dataset.job_error_audit`;
    -- Expected: 0

    -- Check job_run_audit
    SELECT status, processed_records
    FROM `project.dataset.job_run_audit`
    WHERE job_id = 'TEST_JOB_4' AND entry_number = 'ENTRY_4'
    ORDER BY run_timestamp;
    -- Expected:
    -- status  | processed_records
    -- --------|------------------
    -- RUNNING | NULL
    -- SUCCESS | 0
    ```

---

### Test Case 5: Core Logic - `PDS_DESCRIPTION` is NULL

*   **Purpose:** Verify that `NULL` values in `PDS_DESCRIPTION` are correctly carried through to the target table.
*   **Setup:**
    1.  Clear all audit and target tables.
    2.  Populate source tables with a matching `BPR_ID` where `PDS_DESCRIPTION` is `NULL`.

    ```sql
    -- Clear tables
    DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
    DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
    DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_optionen` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_beschr` WHERE TRUE;

    -- Populate sof_ta_bpr_optionen
    INSERT INTO `project.dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID) VALUES
    (301, 1);

    -- Populate sof_ta_bpr_beschr with a NULL description
    INSERT INTO `project.dataset.sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION) VALUES
    (1, NULL);
    ```
*   **Action:** Execute the stored procedure with valid parameters.

    ```sql
    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        'TEST_JOB_5',
        'ENTRY_5',
        '05012023',
        '0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  `target_bp_ta_bpr_opt_text` contains 1 row, and its `PDS_DESCRIPTION` column is `NULL`.
    2.  `job_error_audit` contains 0 rows.
    3.  `job_run_audit` contains 2 rows: one 'RUNNING' and one 'SUCCESS' entry, with `processed_records` = 1.

    ```sql
    -- Check target table content
    SELECT CNTRCT_ID, BPR_ID, PDS_DESCRIPTION
    FROM `project.dataset.target_bp_ta_bpr_opt_text`;
    -- Expected:
    -- CNTRCT_ID | BPR_ID | PDS_DESCRIPTION
    -- ----------|--------|--------------------
    -- 301       | 1      | NULL

    -- Check job_run_audit for record count
    SELECT processed_records
    FROM `project.dataset.job_run_audit`
    WHERE job_id = 'TEST_JOB_5' AND status = 'SUCCESS';
    -- Expected: 1
    ```

---

### Test Case 6: Idempotency - Multiple Runs

*   **Purpose:** Verify that running the stored procedure multiple times with the same inputs results in the same final state in the target table, due to the `DELETE FROM ... WHERE TRUE` operation.
*   **Setup:**
    1.  Clear all audit and target tables.
    2.  Populate source tables with valid data.

    ```sql
    -- Clear tables
    DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
    DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
    DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_optionen` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_beschr` WHERE TRUE;

    -- Populate source_dataset.sof_ta_bpr_optionen
    INSERT INTO `project.dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID) VALUES
    (401, 1),
    (402, 2);

    -- Populate source_dataset.sof_ta_bpr_beschr
    INSERT INTO `project.dataset.sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION) VALUES
    (1, 'Desc A'),
    (2, 'Desc B');
    ```
*   **Action:** Execute the stored procedure twice consecutively with the same parameters.

    ```sql
    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        'TEST_JOB_6',
        'ENTRY_6',
        '06012023',
        '0'
    );

    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        'TEST_JOB_6',
        'ENTRY_6',
        '06012023',
        '0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  `target_bp_ta_bpr_opt_text` contains the expected 2 rows, not 4.
    2.  `job_error_audit` contains 0 rows.
    3.  `job_run_audit` contains 4 rows: two 'RUNNING' and two 'SUCCESS' entries, each 'SUCCESS' entry with `processed_records` = 2.

    ```sql
    -- Check target table content
    SELECT COUNT(*) FROM `project.dataset.target_bp_ta_bpr_opt_text`;
    -- Expected: 2 (not 4)

    -- Check job_run_audit for multiple runs
    SELECT status, processed_records
    FROM `project.dataset.job_run_audit`
    WHERE job_id = 'TEST_JOB_6' AND entry_number = 'ENTRY_6'
    ORDER BY run_timestamp;
    -- Expected:
    -- status  | processed_records
    -- --------|------------------
    -- RUNNING | NULL
    -- SUCCESS | 2
    -- RUNNING | NULL
    -- SUCCESS | 2
    ```

---

### Test Case 7: Error Handling - Runtime Exception in Core Logic

*   **Purpose:** Verify that if an unexpected error occurs during the core business logic execution (e.g., a data type mismatch if not properly handled, or a division by zero if such logic existed), it is caught, logged to `job_error_audit`, and the `job_run_audit` records a 'FAILED' status.
*   **Setup:**
    1.  Clear all audit and target tables.
    2.  (Hypothetical) Modify the stored procedure temporarily to introduce an error, e.g., `SELECT 1/0;` within the `BEGIN...END` block of the core logic. *Since we cannot modify the generated code for testing, this test case is conceptual and would require a mock procedure or a specific data scenario that triggers a BigQuery runtime error.* For this example, we'll simulate a scenario where `sof_ta_bpr_optionen` has a non-numeric `BPR_ID` if `BPR_ID` was expected to be numeric in a calculation (though here it's just a join key). Let's assume a future version of the SQL might have a cast that fails.

    ```sql
    -- Clear tables
    DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
    DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
    DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_optionen` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_beschr` WHERE TRUE;

    -- Populate source_dataset.sof_ta_bpr_optionen with data that might cause an error
    -- (e.g., if BPR_ID was used in a numeric operation and was 'ABC')
    -- For the current simple JOIN, this won't cause an error, but demonstrates the intent.
    INSERT INTO `project.dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID) VALUES
    (501, 1);
    -- If BPR_ID was STRING and a CAST to INT64 was done:
    -- INSERT INTO `project.dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID) VALUES (501, 'ABC');

    -- Populate sof_ta_bpr_beschr
    INSERT INTO `project.dataset.sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION) VALUES
    (1, 'Valid Description');
    ```
*   **Action:** Execute the stored procedure. (Assume a runtime error occurs within the core logic block).

    ```sql
    -- This call is expected to fail due to an internal error
    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        'TEST_JOB_7',
        'ENTRY_7',
        '07012023',
        '0'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution fails with an `SQLSTATE '45000'` error.
    2.  `job_error_audit` contains 1 row with `error_message` reflecting the runtime error and a `stack_trace`.
    3.  `job_run_audit` contains 2 rows: one 'RUNNING' and one 'FAILED' entry, with `processed_records` = 0 for the 'FAILED' entry.
    4.  `target_bp_ta_bpr_opt_text` contains 0 rows.

    ```sql
    -- Check job_error_audit
    SELECT error_message, stack_trace
    FROM `project.dataset.job_error_audit`
    WHERE job_id = 'TEST_JOB_7';
    -- Expected: error_message reflecting the specific runtime error, stack_trace populated.

    -- Check job_run_audit
    SELECT status, processed_records
    FROM `project.dataset.job_run_audit`
    WHERE job_id = 'TEST_JOB_7' AND entry_number = 'ENTRY_7'
    ORDER BY run_timestamp;
    -- Expected:
    -- status  | processed_records
    -- --------|------------------
    -- RUNNING | NULL
    -- FAILED  | 0
    ```

---

### Test Case 8: Schema and Data Type Assertions

*   **Purpose:** Verify that the target table `target_bp_ta_bpr_opt_text` and audit tables (`job_error_audit`, `job_run_audit`) have the correct schema and data types as defined in the DDLs.
*   **Setup:** Ensure all DDLs have been executed. No specific data population is needed for this test.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` views.
*   **Pass/Fail Criterion:** The schema details match the DDLs.

    ```sql
    -- Check target_bp_ta_bpr_opt_text schema
    SELECT column_name, data_type, is_nullable
    FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'target_bp_ta_bpr_opt_text'
    ORDER BY ordinal_position;
    -- Expected:
    -- column_name | data_type | is_nullable
    -- -------------|-----------|------------
    -- CNTRCT_ID   | INT64     | NO
    -- BPR_ID      | INT64     | NO
    -- PDS_DESCRIPTION | STRING    | YES

    -- Check job_error_audit schema
    SELECT column_name, data_type, is_nullable
    FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_error_audit'
    ORDER BY ordinal_position;
    -- Expected:
    -- column_name     | data_type | is_nullable
    -- -----------------|-----------|------------
    -- job_id          | STRING    | NO
    -- entry_number    | STRING    | YES
    -- key_date        | DATE      | YES
    -- error_timestamp | TIMESTAMP | NO
    -- error_message   | STRING    | NO
    -- stack_trace     | STRING    | YES

    -- Check job_run_audit schema
    SELECT column_name, data_type, is_nullable
    FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_run_audit'
    ORDER BY ordinal_position;
    -- Expected:
    -- column_name     | data_type | is_nullable
    -- -----------------|-----------|------------
    -- job_id          | STRING    | NO
    -- entry_number    | STRING    | YES
    -- key_date        | DATE      | YES
    -- run_timestamp   | TIMESTAMP | NO
    -- status          | STRING    | NO
    -- processed_records | INT64     | YES
    -- start_time      | TIMESTAMP | YES
    -- end_time        | TIMESTAMP | YES
    ```

---

### Test Case 9: External System Replacement - Airflow DAG Invocation

*   **Purpose:** Verify that the Airflow DAG correctly invokes the BigQuery Stored Procedure with the specified parameters, particularly the `p_Stichtag` format. This is an integration test for the orchestration layer.
*   **Setup:**
    1.  Deploy the `k_ausd_bp_ta_bpr_opt_text_dag.py` to a Cloud Composer environment.
    2.  Clear all audit and target tables.
    3.  Populate source tables with valid data (e.g., same as Test Case 1).
*   **Action:** Manually trigger the `k_ausd_bp_ta_bpr_opt_text_dag` in Airflow for a specific `execution_date`, e.g., `2023-01-08`.
*   **Pass/Fail Criterion:**
    1.  The Airflow DAG run completes successfully.
    2.  `target_bp_ta_bpr_opt_text` contains the expected data (e.g., 3 rows from Test Case 1 setup).
    3.  `job_run_audit` contains 'RUNNING' and 'SUCCESS' entries for `job_id = 'BP_TA_BPR_OPT_TEXT_JOB'`, `entry_number = '1'`, and `key_date = '2023-01-08'` (derived from `08012023`).
    4.  `job_error_audit` contains 0 rows.

    ```sql
    -- Check target table content (assuming Test Case 1 data)
    SELECT COUNT(*) FROM `project.dataset.target_bp_ta_bpr_opt_text`;
    -- Expected: 3

    -- Check job_run_audit for DAG invocation
    SELECT status, processed_records, key_date
    FROM `project.dataset.job_run_audit`
    WHERE job_id = 'BP_TA_BPR_OPT_TEXT_JOB' AND entry_number = '1'
    ORDER BY run_timestamp;
    -- Expected:
    -- status  | processed_records | key_date
    -- --------|-------------------|-----------
    -- RUNNING | NULL              | 2023-01-08
    -- SUCCESS | 3                 | 2023-01-08
    ```

---

### Test Case 10: `p_wiederanlaufWert` Handling

*   **Purpose:** Verify that the `p_wiederanlaufWert` parameter is passed to the stored procedure without causing issues, even if its specific business logic usage is not evident in the provided code. This ensures the migration correctly handles all input parameters from the legacy script.
*   **Setup:**
    1.  Clear all audit and target tables.
    2.  Populate source tables with valid data (e.g., same as Test Case 1).
*   **Action:** Execute the stored procedure with a non-default `p_wiederanlaufWert`.
    ```sql
    -- Clear tables
    DELETE FROM `project.dataset.job_error_audit` WHERE TRUE;
    DELETE FROM `project.dataset.job_run_audit` WHERE TRUE;
    DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_optionen` WHERE TRUE;
    DELETE FROM `project.dataset.sof_ta_bpr_beschr` WHERE TRUE;

    -- Populate source_dataset.sof_ta_bpr_optionen
    INSERT INTO `project.dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID) VALUES
    (101, 1), (102, 2), (103, 3);
    -- Populate source_dataset.sof_ta_bpr_beschr
    INSERT INTO `project.dataset.sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION) VALUES
    (1, 'Desc 1'), (2, 'Desc 2'), (3, 'Desc 3');

    CALL `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
        'TEST_JOB_10',
        'ENTRY_10',
        '10012023',
        '50' -- Non-default value
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure executes successfully.
    2.  `target_bp_ta_bpr_opt_text` contains the expected data (e.g., 3 rows).
    3.  `job_run_audit` contains 'RUNNING' and 'SUCCESS' entries.
    4.  `job_error_audit` contains 0 rows.
    *Note: Since the provided BigQuery SP code does not use `p_wiederanlaufWert` in its logic, this test primarily confirms that passing a value for it does not cause a failure.*

    ```sql
    -- Check target table content
    SELECT COUNT(*) FROM `project.dataset.target_bp_ta_bpr_opt_text`;
    -- Expected: 3

    -- Check job_run_audit
    SELECT status
    FROM `project.dataset.job_run_audit`
    WHERE job_id = 'TEST_JOB_10' AND entry_number = 'ENTRY_10'
    ORDER BY run_timestamp;
    -- Expected:
    -- status
    -- --------
    -- RUNNING
    -- SUCCESS
    ```