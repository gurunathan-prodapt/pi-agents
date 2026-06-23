The migration of `k_ausd_v_ta_vvl_upgrade.ksh` to a BigQuery stored procedure (`r_ausd_vertrag_control`) primarily involves re-implementing orchestration, parameter handling, error management, and result logging. The core data transformation logic resides in the separately migrated `d_ausd_v_ta_vvl_upgrade_proc`.

The following tests focus on validating the behavioral equivalence of the `r_ausd_vertrag_control` orchestrator, ensuring it correctly handles inputs, calls the core logic, and manages logging as specified in the migration design.

---

## Migration Validation Tests for `k_ausd_v_ta_vvl_upgrade.ksh`

### Test Case 1: Successful Execution with Valid Parameters

*   **Purpose:** Verify the end-to-end successful execution of the migrated orchestrator with valid inputs. This includes correct job registration, successful invocation of the core SQL logic, accurate record count capture, and proper logging of completion status and results.
*   **Setup:**
    1.  Ensure all DDLs (`ta_vvl_upgrade`, `job_table`, `job_error_log`, `job_result_log`) are deployed and tables are empty.
    2.  Deploy `d_ausd_v_ta_vvl_upgrade_proc` as provided, which currently returns a dummy `processed_records` value (e.g., `12345`). For a real test, this procedure would contain the fully migrated DML and return the actual affected row count.
*   **Action:**
    Execute the `r_ausd_vertrag_control` stored procedure with valid `JobKennung` and `EintragsNr`.

    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_SUCCESS', 'ENTRY_001');
    ```
*   **Pass/Fail Criteria:**
    1.  The call to `r_ausd_vertrag_control` completes without raising an error.
    2.  **`job_table` Assertion:** One record exists for `TEST_JOB_SUCCESS`/`ENTRY_001` with `status = 'COMPLETED'`.
    3.  **`job_result_log` Assertion:** One record exists for `TEST_JOB_SUCCESS`/`ENTRY_001` with `records_processed = 12345` (or the expected value from `d_ausd_v_ta_vvl_upgrade_proc`).
    4.  **`job_error_log` Assertion:** No records exist for `TEST_JOB_SUCCESS`/`ENTRY_001`.

    ```sql
    -- Assert job_table status
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_table`
    WHERE
        job_kennung = 'TEST_JOB_SUCCESS'
        AND eintrags_nr = 'ENTRY_001'
        AND status = 'COMPLETED';
    -- Expected: 1

    -- Assert job_result_log entry
    SELECT
        COUNT(1),
        MAX(records_processed)
    FROM
        `project.dataset.job_result_log`
    WHERE
        job_kennung = 'TEST_JOB_SUCCESS'
        AND eintrags_nr = 'ENTRY_001'
        AND tab_name = 'ta_vvl_upgrade'
        AND records_processed = 12345; -- Adjust if d_ausd_v_ta_vvl_upgrade_proc returns a different value
    -- Expected: 1, 12345

    -- Assert job_error_log is empty for this job
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_error_log`
    WHERE
        job_kennung = 'TEST_JOB_SUCCESS'
        AND eintrags_nr = 'ENTRY_001';
    -- Expected: 0
    ```

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** Verify that the orchestrator correctly identifies and handles a missing or empty `p_JobKennung` parameter, logging the error and marking the job as failed, mirroring the original script's `pruefeParameterGesetzt` behavior.
*   **Setup:**
    1.  Ensure logging tables are empty or cleared.
    2.  `d_ausd_v_ta_vvl_upgrade_proc` is deployed and functional (it should not be called in this scenario).
*   **Action:**
    Execute the `r_ausd_vertrag_control` stored procedure with `p_JobKennung` as `NULL` or an empty string.

    ```sql
    -- Test with NULL
    CALL `project.dataset.r_ausd_vertrag_control`(NULL, 'ENTRY_002_NULL');
    -- Test with empty string
    CALL `project.dataset.r_ausd_vertrag_control`('', 'ENTRY_002_EMPTY');
    ```
*   **Pass/Fail Criteria:**
    1.  Both calls to `r_ausd_vertrag_control` raise an error (e.g., `BQS001`).
    2.  **`job_table` Assertion:** Two records exist, one for `ENTRY_002_NULL` and one for `ENTRY_002_EMPTY`, both with `status = 'FAILED'`.
    3.  **`job_error_log` Assertion:** Two records exist, one for `ENTRY_002_NULL` and one for `ENTRY_002_EMPTY`, both with `error_code = 'BQS001'` and `error_message` indicating `p_JobKennung` is mandatory.
    4.  **`job_result_log` Assertion:** No records exist for these job runs.

    ```sql
    -- Assert job_table status for NULL JobKennung
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_table`
    WHERE
        eintrags_nr = 'ENTRY_002_NULL'
        AND status = 'FAILED';
    -- Expected: 1

    -- Assert job_error_log entry for NULL JobKennung
    SELECT
        COUNT(1),
        MAX(error_code),
        MAX(error_message)
    FROM
        `project.dataset.job_error_log`
    WHERE
        eintrags_nr = 'ENTRY_002_NULL'
        AND error_code = 'BQS001'
        AND error_message LIKE '%p_JobKennung is mandatory%';
    -- Expected: 1, 'BQS001', 'Parameter p_JobKennung is mandatory and cannot be empty.'

    -- Repeat similar assertions for 'ENTRY_002_EMPTY'

    -- Assert job_result_log is empty for these jobs
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_result_log`
    WHERE
        eintrags_nr IN ('ENTRY_002_NULL', 'ENTRY_002_EMPTY');
    -- Expected: 0
    ```

### Test Case 3: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose:** Verify that the orchestrator correctly identifies and handles a missing or empty `p_EintragsNr` parameter, logging the error and marking the job as failed.
*   **Setup:**
    1.  Ensure logging tables are empty or cleared.
    2.  `d_ausd_v_ta_vvl_upgrade_proc` is deployed and functional.
*   **Action:**
    Execute the `r_ausd_vertrag_control` stored procedure with `p_EintragsNr` as `NULL` or an empty string.

    ```sql
    -- Test with NULL
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_003_NULL', NULL);
    -- Test with empty string
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_003_EMPTY', '');
    ```
*   **Pass/Fail Criteria:**
    1.  Both calls to `r_ausd_vertrag_control` raise an error (e.g., `BQS002`).
    2.  **`job_table` Assertion:** Two records exist, one for `TEST_JOB_003_NULL` and one for `TEST_JOB_003_EMPTY`, both with `status = 'FAILED'`.
    3.  **`job_error_log` Assertion:** Two records exist, one for `TEST_JOB_003_NULL` and one for `TEST_JOB_003_EMPTY`, both with `error_code = 'BQS002'` and `error_message` indicating `p_EintragsNr` is mandatory.
    4.  **`job_result_log` Assertion:** No records exist for these job runs.

    ```sql
    -- Assert job_table status for NULL EintragsNr
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_table`
    WHERE
        job_kennung = 'TEST_JOB_003_NULL'
        AND status = 'FAILED';
    -- Expected: 1

    -- Assert job_error_log entry for NULL EintragsNr
    SELECT
        COUNT(1),
        MAX(error_code),
        MAX(error_message)
    FROM
        `project.dataset.job_error_log`
    WHERE
        job_kennung = 'TEST_JOB_003_NULL'
        AND error_code = 'BQS002'
        AND error_message LIKE '%p_EintragsNr is mandatory%';
    -- Expected: 1, 'BQS002', 'Parameter p_EintragsNr is mandatory and cannot be empty.'

    -- Repeat similar assertions for 'TEST_JOB_003_EMPTY'

    -- Assert job_result_log is empty for these jobs
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_result_log`
    WHERE
        job_kennung IN ('TEST_JOB_003_NULL', 'TEST_JOB_003_EMPTY');
    -- Expected: 0
    ```

### Test Case 4: Error Handling - Core SQL Logic (`d_ausd_v_ta_vvl_upgrade_proc`) Failure

*   **Purpose:** Verify that if the underlying core SQL logic (`d_ausd_v_ta_vvl_upgrade_proc`) fails, the orchestrator correctly catches the error, logs it, and marks the overall job as FAILED. This replaces the shell script's `WHENEVER SQLERROR EXIT FAILURE` and `DWMSG_MeldeFehler` behavior.
*   **Setup:**
    1.  Ensure logging tables are empty or cleared.
    2.  Modify `d_ausd_v_ta_vvl_upgrade_proc` to explicitly `RAISE` an error. For example, change its body to:
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_vvl_upgrade_proc`(
            OUT processed_records INT64
        )
        BEGIN
            RAISE 'Simulated SQL error during data processing in d_ausd_v_ta_vvl_upgrade_proc';
        END;
        ```
*   **Action:**
    Execute the `r_ausd_vertrag_control` stored procedure with valid parameters.

    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_CORE_FAIL', 'ENTRY_004');
    ```
*   **Pass/Fail Criteria:**
    1.  The call to `r_ausd_vertrag_control` raises an error.
    2.  **`job_table` Assertion:** One record exists for `TEST_JOB_CORE_FAIL`/`ENTRY_004` with `status = 'FAILED'`.
    3.  **`job_error_log` Assertion:** One record exists for `TEST_JOB_CORE_FAIL`/`ENTRY_004` with `procedure_name = 'r_ausd_vertrag_control'` and `error_message` containing the simulated error message from `d_ausd_v_ta_vvl_upgrade_proc`. The `error_code` should be `BQS999` (the generic fallback).
    4.  **`job_result_log` Assertion:** No records exist for this job run.

    ```sql
    -- Assert job_table status
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_table`
    WHERE
        job_kennung = 'TEST_JOB_CORE_FAIL'
        AND eintrags_nr = 'ENTRY_004'
        AND status = 'FAILED';
    -- Expected: 1

    -- Assert job_error_log entry
    SELECT
        COUNT(1),
        MAX(error_code),
        MAX(error_message)
    FROM
        `project.dataset.job_error_log`
    WHERE
        job_kennung = 'TEST_JOB_CORE_FAIL'
        AND eintrags_nr = 'ENTRY_004'
        AND procedure_name = 'r_ausd_vertrag_control'
        AND error_message LIKE '%Simulated SQL error%';
    -- Expected: 1, 'BQS999', 'Simulated SQL error during data processing in d_ausd_v_ta_vvl_upgrade_proc'

    -- Assert job_result_log is empty
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_result_log`
    WHERE
        job_kennung = 'TEST_JOB_CORE_FAIL'
        AND eintrags_nr = 'ENTRY_004';
    -- Expected: 0
    ```

### Test Case 5: Record Count Capture and Logging

*   **Purpose:** Verify that the `processed_records` output from `d_ausd_v_ta_vvl_upgrade_proc` is correctly captured by `r_ausd_vertrag_control` and logged into `job_result_log`. This replaces the original script's temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_upgrade_$$.tmp`) mechanism.
*   **Setup:**
    1.  Ensure logging tables are empty or cleared.
    2.  Modify `d_ausd_v_ta_vvl_upgrade_proc` to return a specific, non-default `processed_records` value (e.g., `54321`).
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_vvl_upgrade_proc`(
            OUT processed_records INT64
        )
        BEGIN
            SET processed_records = 54321; -- Specific value for this test
        END;
        ```
*   **Action:**
    Execute the `r_ausd_vertrag_control` stored procedure with valid parameters.

    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_COUNT', 'ENTRY_005');
    ```
*   **Pass/Fail Criteria:**
    1.  The call to `r_ausd_vertrag_control` completes without raising an error.
    2.  **`job_table` Assertion:** One record exists for `TEST_JOB_COUNT`/`ENTRY_005` with `status = 'COMPLETED'`.
    3.  **`job_result_log` Assertion:** One record exists for `TEST_JOB_COUNT`/`ENTRY_005` with `records_processed = 54321`.
    4.  **`job_error_log` Assertion:** No records exist for this job.

    ```sql
    -- Assert job_table status
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_table`
    WHERE
        job_kennung = 'TEST_JOB_COUNT'
        AND eintrags_nr = 'ENTRY_005'
        AND status = 'COMPLETED';
    -- Expected: 1

    -- Assert job_result_log entry with correct count
    SELECT
        COUNT(1),
        MAX(records_processed)
    FROM
        `project.dataset.job_result_log`
    WHERE
        job_kennung = 'TEST_JOB_COUNT'
        AND eintrags_nr = 'ENTRY_005'
        AND records_processed = 54321;
    -- Expected: 1, 54321

    -- Assert job_error_log is empty
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_error_log`
    WHERE
        job_kennung = 'TEST_JOB_COUNT'
        AND eintrags_nr = 'ENTRY_005';
    -- Expected: 0
    ```

### Test Case 6: Schema and Data Type Verification for Logging Tables

*   **Purpose:** Verify that the schemas and data types of the newly created logging tables (`job_table`, `job_error_log`, `job_result_log`) match the design document and are appropriate for the data they store. This ensures data quality and integrity for the new logging mechanism.
*   **Setup:**
    1.  Ensure all DDLs for `job_table`, `job_error_log`, and `job_result_log` have been successfully applied in BigQuery.
*   **Action:**
    Query the BigQuery `INFORMATION_SCHEMA` to retrieve the schema details for each logging table.
*   **Pass/Fail Criteria:**
    The retrieved schema details (column names, data types, nullability, descriptions) for each table must precisely match the definitions provided in the DDL files.

    ```sql
    -- Assert schema for job_table
    SELECT
        column_name,
        data_type,
        is_nullable,
        description
    FROM
        `project.dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'job_table'
    ORDER BY
        ordinal_position;
    /* Expected Output:
    column_name     data_type   is_nullable description
    job_id          STRING      NO          Unique identifier for each job run
    job_kennung     STRING      YES         Job identifier from input parameters
    eintrags_nr     STRING      YES         Entry number from input parameters
    tab_name        STRING      YES         Target table name (e.g., ta_vvl_upgrade)
    status          STRING      YES         Current status of the job (e.g., 'RUNNING', 'COMPLETED', 'FAILED')
    created_ts      TIMESTAMP   YES         Timestamp when the job record was created
    updated_ts      TIMESTAMP   YES         Timestamp when the job record was last updated
    */

    -- Assert schema for job_error_log
    SELECT
        column_name,
        data_type,
        is_nullable,
        description
    FROM
        `project.dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'job_error_log'
    ORDER BY
        ordinal_position;
    /* Expected Output:
    column_name     data_type   is_nullable description
    error_ts        TIMESTAMP   YES         Timestamp of the error
    procedure_name  STRING      YES         Name of the stored procedure where the error occurred
    error_code      STRING      YES         SQLSTATE or custom error code
    error_message   STRING      YES         Detailed error message
    job_kennung     STRING      YES         Job identifier from input parameters
    eintrags_nr     STRING      YES         Entry number from input parameters
    */

    -- Assert schema for job_result_log
    SELECT
        column_name,
        data_type,
        is_nullable,
        description
    FROM
        `project.dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'job_result_log'
    ORDER BY
        ordinal_position;
    /* Expected Output:
    column_name         data_type   is_nullable description
    job_kennung         STRING      YES         Job identifier from input parameters
    eintrags_nr         STRING      YES         Entry number from input parameters
    tab_name            STRING      YES         Target table name
    records_processed   INT64       YES         Number of records processed by the SQL script
    finished_ts         TIMESTAMP   YES         Timestamp when the job result was logged
    */
    ```

### Test Case 7: Multiple Concurrent/Sequential Runs

*   **Purpose:** Verify that the logging mechanism correctly handles multiple job executions, ensuring each run is logged distinctly and does not interfere with previous or concurrent runs. This checks the robustness of the job tracking and logging.
*   **Setup:**
    1.  Ensure logging tables are empty or cleared.
    2.  `d_ausd_v_ta_vvl_upgrade_proc` is configured to return a consistent `processed_records` value (e.g., `100`).
*   **Action:**
    Execute the `r_ausd_vertrag_control` stored procedure multiple times with different or same parameters.

    ```sql
    -- Run 1
    CALL `project.dataset.r_ausd_vertrag_control`('JOB_MULTI_01', 'ENTRY_A');
    -- Run 2 (same parameters as Run 1, simulating re-run or concurrent execution)
    CALL `project.dataset.r_ausd_vertrag_control`('JOB_MULTI_01', 'ENTRY_A');
    -- Run 3 (different parameters)
    CALL `project.dataset.r_ausd_vertrag_control`('JOB_MULTI_02', 'ENTRY_B');
    ```
*   **Pass/Fail Criteria:**
    1.  All calls complete without error.
    2.  **`job_table` Assertion:** Three distinct records exist, each with `status = 'COMPLETED'`. The `job_id` should be unique for each run.
    3.  **`job_result_log` Assertion:** Three distinct records exist, each corresponding to a successful run, with `records_processed = 100`.
    4.  **`job_error_log` Assertion:** No records exist.

    ```sql
    -- Assert distinct job_table entries
    SELECT
        COUNT(DISTINCT job_id)
    FROM
        `project.dataset.job_table`
    WHERE
        job_kennung IN ('JOB_MULTI_01', 'JOB_MULTI_02')
        AND eintrags_nr IN ('ENTRY_A', 'ENTRY_B')
        AND status = 'COMPLETED';
    -- Expected: 3

    -- Assert distinct job_result_log entries
    SELECT
        COUNT(1),
        SUM(records_processed)
    FROM
        `project.dataset.job_result_log`
    WHERE
        job_kennung IN ('JOB_MULTI_01', 'JOB_MULTI_02')
        AND eintrags_nr IN ('ENTRY_A', 'ENTRY_B')
        AND records_processed = 100;
    -- Expected: 3, 300 (3 runs * 100 records each)

    -- Assert job_error_log is empty
    SELECT
        COUNT(1)
    FROM
        `project.dataset.job_error_log`
    WHERE
        job_kennung IN ('JOB_MULTI_01', 'JOB_MULTI_02')
        AND eintrags_nr IN ('ENTRY_A', 'ENTRY_B');
    -- Expected: 0
    ```