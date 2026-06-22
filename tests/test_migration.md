The migration of `r_ausd_bp_ta_bcp_iccid.ksh` to a BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_ibcp_ccid`) primarily involves translating orchestration logic, parameter handling, logging, and error management. The core data processing logic, residing in `k_ausd_bp_ta_bcp_iccid.ksh`, is explicitly noted as a separate, undefined migration. Therefore, these tests focus on the orchestrator's behavior, its interaction with the (stub) kernel procedure, and its logging/auditing mechanisms.

---

## Test 1: Successful Execution with Default Parameters

*   **Purpose**: Verify that the BigQuery orchestrator stored procedure (`ausd_bp_ta_ibcp_ccid`) correctly handles missing `p_stichtag_str` and `p_wiederanlaufWert_in` parameters, defaulting them as per the legacy script's behavior, and successfully invokes the kernel stored procedure, logging the execution.
*   **Setup**:
    1.  Ensure the `my_project.my_dataset.job_audit`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log` tables are empty before execution.
    2.  The `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` stub procedure is deployed and configured not to raise any errors.
*   **Action**:
    Execute the BigQuery orchestrator stored procedure without providing any parameters for `p_stichtag_str` and `p_wiederanlaufWert_in`.
    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully without raising an error.
    2.  Query `my_project.my_dataset.job_audit`:
        *   Exactly one row exists with `status = 'OK'`.
        *   The `stichtag` column for this row is `CURRENT_DATE()` (today's date).
        *   The `wiederanlauf_wert` column is `0`.
        *   The `job_id` column is 'ausd_bp_ta_ibcp_ccid'.
        *   `start_timestamp` and `end_timestamp` columns are populated.
    3.  Query `my_project.my_dataset.job_log`:
        *   At least two 'INFO' level entries exist for the `run_id` corresponding to the `job_audit` entry.
        *   One 'INFO' entry's `message` should indicate job start, including today's date (in 'DDMMYYYY' format) and `WiederanlaufWert: 0`.
        *   Another 'INFO' entry's `message` should be 'Job completed successfully'.
    4.  Query `my_project.my_dataset.job_error_log`:
        *   No rows exist in this table.

---

## Test 2: Successful Execution with Explicit Parameters

*   **Purpose**: Verify that the BigQuery orchestrator stored procedure correctly processes explicitly provided `p_stichtag_str` and `p_wiederanlaufWert_in` parameters, and successfully invokes the kernel stored procedure, logging the execution.
*   **Setup**:
    1.  Ensure the `my_project.my_dataset.job_audit`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log` tables are empty before execution.
    2.  The `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` stub procedure is deployed and configured not to raise any errors.
    3.  Define a specific `stichtag` (e.g., '01012023') and `wiederanlaufWert` (e.g., 12345).
*   **Action**:
    Execute the BigQuery orchestrator stored procedure with explicit parameters.
    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`('01012023', 12345);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully without raising an error.
    2.  Query `my_project.my_dataset.job_audit`:
        *   Exactly one row exists with `status = 'OK'`.
        *   The `stichtag` column is `DATE('2023-01-01')`.
        *   The `wiederanlauf_wert` column is `12345`.
        *   The `job_id` column is 'ausd_bp_ta_ibcp_ccid'.
    3.  Query `my_project.my_dataset.job_log`:
        *   At least two 'INFO' level entries exist for the `run_id` corresponding to the `job_audit` entry.
        *   One 'INFO' entry's `message` should indicate job start, including `Stichtag: 01012023` and `WiederanlaufWert: 12345`.
        *   Another 'INFO' entry's `message` should be 'Job completed successfully'.
    4.  Query `my_project.my_dataset.job_error_log`:
        *   No rows exist in this table.

---

## Test 3: Parameter Validation - Invalid Stichtag Format

*   **Purpose**: Verify that the BigQuery orchestrator stored procedure correctly identifies and handles an invalid `p_stichtag_str` format, raising an error and logging the failure details. This mirrors the legacy script's parameter validation and error reporting (`ErrNr=193`).
*   **Setup**:
    1.  Ensure the `my_project.my_dataset.job_audit`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log` tables are empty before execution.
    2.  The `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` stub procedure is deployed and configured not to raise any errors on its own.
*   **Action**:
    Execute the BigQuery orchestrator stored procedure with an invalid `p_stichtag_str` format.
    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`('2023-01-01', 0); -- Invalid format (expected DDMMYYYY)
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement fails and raises an error. The error message should be similar to "Invalid p_stichtag_str format. Expected DDMMYYYY."
    2.  Query `my_project.my_dataset.job_audit`:
        *   Exactly one row exists with `status = 'ERROR'`.
        *   The `message` column contains text indicating job failure due to an invalid date format.
        *   The `stichtag` column is `NULL` (as the error occurs during parsing before it's successfully set).
        *   The `wiederanlauf_wert` column is `0`.
    3.  Query `my_project.my_dataset.job_log`:
        *   At least one 'ERROR' level entry exists for the `run_id` corresponding to the `job_audit` entry.
        *   The `message` in this entry should clearly state the invalid date format error.
    4.  Query `my_project.my_dataset.job_error_log`:
        *   Exactly one row exists.
        *   The `error_message` column contains "Invalid p_stichtag_str format. Expected DDMMYYYY.".
        *   The `stichtag` column is `NULL`.
        *   The `wiederanlauf_wert` column is `0`.

---

## Test 4: Kernel Stored Procedure Failure Handling

*   **Purpose**: Verify that the orchestrator stored procedure correctly handles errors originating from the invoked kernel stored procedure, logging the failure details and updating the audit status. This demonstrates robust error handling, similar to the legacy script's `trap ERR` and `DWMSG_Fehlerbehandlung`.
*   **Setup**:
    1.  Ensure the `my_project.my_dataset.job_audit`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log` tables are empty before execution.
    2.  Modify the `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` stub procedure to intentionally raise an error.
        ```sql
        CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid`(
            IN p_stichtag DATE,
            IN p_wiederanlaufWert INT64
        )
        BEGIN
            RAISE USING MESSAGE 'Simulated kernel script error for testing purposes.';
        END;
        ```
*   **Action**:
    Execute the BigQuery orchestrator stored procedure with valid parameters.
    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`('01012023', 0);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement fails and raises an error. The error message should be "Simulated kernel script error for testing purposes."
    2.  Query `my_project.my_dataset.job_audit`:
        *   Exactly one row exists with `status = 'ERROR'`.
        *   The `message` column contains text indicating job failure due to the simulated kernel error.
        *   The `stichtag` column is `DATE('2023-01-01')`.
        *   The `wiederanlauf_wert` column is `0`.
    3.  Query `my_project.my_dataset.job_log`:
        *   At least one 'ERROR' level entry exists for the `run_id` corresponding to the `job_audit` entry.
        *   The `message` in this entry should clearly state the simulated kernel error.
    4.  Query `my_project.my_dataset.job_error_log`:
        *   Exactly one row exists.
        *   The `error_message` column contains "Simulated kernel script error for testing purposes.".
        *   The `stichtag` column is `DATE('2023-01-01')`.
        *   The `wiederanlauf_wert` column is `0`.

---

## Test 5: Logging and Audit Table Schema and Data Quality

*   **Purpose**: Verify that the `job_audit`, `job_log`, and `job_error_log` tables conform to their defined schemas and that data types and NULLability are correctly handled across various execution scenarios (success, parameter error, kernel error).
*   **Setup**:
    1.  Ensure the DDL for `job_audit`, `job_log`, `job_error_log` has been executed.
    2.  Run Test 1 (success with defaults), Test 2 (success with explicit), Test 3 (invalid stichtag), and Test 4 (kernel failure) sequentially to populate the tables with diverse data.
    3.  After Test 4, reset `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` to its non-failing stub version.
*   **Action**:
    Query the schema information for the tables and inspect the data.
    ```sql
    -- Check schema conformance
    SELECT column_name, data_type, is_nullable
    FROM `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name IN ('job_audit', 'job_log', 'job_error_log')
    ORDER BY table_name, ordinal_position;

    -- Inspect data for type and NULL handling
    SELECT * FROM `my_project.my_dataset.job_audit`;
    SELECT * FROM `my_project.my_dataset.job_log`;
    SELECT * FROM `my_project.my_dataset.job_error_log`;
    ```
*   **Pass/Fail Criterion**:
    1.  **Schema Conformance**: The `INFORMATION_SCHEMA` query confirms that column names, data types, and `is_nullable` properties for `job_audit`, `job_log`, and `job_error_log` precisely match the DDL provided in the migration design document.
    2.  **Data Type Integrity**: All columns in the queried data contain values of the expected type (e.g., `stichtag` is a `DATE`, `wiederanlauf_wert` is an `INT64`, timestamps are `TIMESTAMP`, strings are `STRING`). No type coercion errors or unexpected values are observed.
    3.  **NULL Handling**:
        *   The `stichtag` column in `job_audit` and `job_error_log` is `NULL` for the run corresponding to Test 3 (invalid date format).
        *   All columns defined as `NOT NULL` (implicitly by not having `OPTIONS(description="...")` in BigQuery DDL, or explicitly if `NOT NULL` was added) are populated for all rows.
    4.  **Row Counts**:
        *   `my_project.my_dataset.job_audit` contains 4 rows (one for each test run).
        *   `my_project.my_dataset.job_log` contains at least 8 rows (2 'INFO' for each success, 1 'INFO' + 1 'ERROR' for each failure).
        *   `my_project.my_dataset.job_error_log` contains 2 rows (one for Test 3, one for Test 4).

---

## Test 6: Output Parity - Log Message Content

*   **Purpose**: Verify that the content of log messages in `job_log` and `job_audit` tables closely mirrors the informational output printed by the legacy script to its log file. This ensures that operational monitoring and debugging information remains consistent and understandable.
*   **Setup**:
    1.  Perform a successful run of the legacy `r_ausd_bp_ta_bcp_iccid.ksh` script with specific parameters (e.g., `-s 01012023 -l 12345`). Capture its `LogDatei` output.
    2.  Ensure `my_project.my_dataset.job_audit` and `my_project.my_dataset.job_log` tables are empty.
    3.  Execute the BigQuery orchestrator stored procedure with the *same* parameters.
        ```sql
        CALL `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`('01012023', 12345);
        ```
*   **Action**:
    Compare the captured legacy log file content with the `message` fields in `my_project.my_dataset.job_log` and `my_project.my_dataset.job_audit` for the corresponding BigQuery run. Focus on the informational messages printed by the shell script:
    ```
    print " Job-Nr    : '$DW_EintragsNr'"
    print " JobKennung: '$JobKennung'"
    print " Logdatei  : '$LogDatei'"
    print " Stichtag  : '$p_stichtag'"
    print "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
    ```
*   **Pass/Fail Criterion**:
    1.  The `job_log` table contains entries that convey equivalent information to the legacy script's log output for corresponding events (job start, parameters, job success).
    2.  The `job_audit` table's `message` field accurately summarizes the job's outcome (e.g., 'Job completed successfully').
    3.  Key parameters (`Stichtag`, `WiederanlaufWert`) are correctly reflected in the BigQuery log messages, even if the exact formatting differs. The *meaning* and *completeness* of the information should be equivalent.

---

## Test 7: Edge Case - Empty String for Stichtag

*   **Purpose**: Verify that providing an empty string for `p_stichtag_str` correctly triggers the default behavior (using `CURRENT_DATE()`), similar to how a completely missing parameter would behave in the legacy script. This tests the `TRIM(p_stichtag_str) = ''` condition.
*   **Setup**:
    1.  Ensure the `my_project.my_dataset.job_audit`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log` tables are empty before execution.
    2.  The `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` stub procedure is deployed and configured not to raise any errors.
*   **Action**:
    Execute the BigQuery orchestrator stored procedure with an empty string for `p_stichtag_str`.
    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`('', 0);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully without raising an error.
    2.  Query `my_project.my_dataset.job_audit`:
        *   Exactly one row exists with `status = 'OK'`.
        *   The `stichtag` column is `CURRENT_DATE()` (today's date).
        *   The `wiederanlauf_wert` column is `0`.
    3.  Query `my_project.my_dataset.job_log`:
        *   At least two 'INFO' level entries exist for the `run_id` from `job_audit`.
        *   One 'INFO' entry's `message` should indicate job start, including today's date (in 'DDMMYYYY' format) and `WiederanlaufWert: 0`.
    4.  Query `my_project.my_dataset.job_error_log`:
        *   No rows exist in this table.

---

## Test 8: External System Replacement - Kernel SP Invocation

*   **Purpose**: Verify that the BigQuery orchestrator stored procedure correctly invokes the `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` kernel stored procedure with the correct parameters, mirroring the shell script's execution of the kernel script.
*   **Setup**:
    1.  Ensure the `my_project.my_dataset.job_audit`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log` tables are empty before execution.
    2.  Modify the `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` stub procedure to log its received parameters to the `job_log` table. This allows verification of the parameters passed from the orchestrator.
        ```sql
        CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid`(
            IN p_stichtag DATE,
            IN p_wiederanlaufWert INT64
        )
        BEGIN
            -- Log the received parameters for verification
            INSERT INTO `my_project.my_dataset.job_log` (run_id, log_timestamp, log_level, procedure_name, message)
            VALUES (GENERATE_UUID(), CURRENT_TIMESTAMP(), 'INFO', 'k_ausd_bp_ta_bcp_iccid',
                    FORMAT("Kernel SP invoked with Stichtag: %t, WiederanlaufWert: %d", p_stichtag, p_wiederanlaufWert));

            -- Original placeholder logic (ensure it doesn't fail)
            SELECT
                FORMAT("Kernel procedure k_ausd_bp_ta_bcp_iccid executed with Stichtag: %t, WiederanlaufWert: %d. Implement actual logic here.", p_stichtag, p_wiederanlaufWert) AS status_message;
        END;
        ```
*   **Action**:
    Execute the BigQuery orchestrator stored procedure with specific parameters.
    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`('15032024', 54321);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully.
    2.  Query `my_project.my_dataset.job_log`:
        *   An 'INFO' level entry from `procedure_name = 'k_ausd_bp_ta_bcp_iccid'` exists.
        *   The `message` in this entry confirms that `Stichtag: 2024-03-15` and `WiederanlaufWert: 54321` were correctly received by the kernel stored procedure. This verifies the parameter passing mechanism between the orchestrator and kernel.