As a senior data-migration QA engineer, I've designed a suite of validation tests for the migrated BigQuery stored procedures, focusing on the wrapper functionality of `r_ausd_v_ta_action_assoc.ksh`. Since the core business logic (`k_ausd_v_ta_action_assoc.ksh`) is a placeholder, these tests primarily validate the orchestration, logging, parameter handling, and error management aspects of the wrapper.

The tests cover output parity, transformation correctness, external system replacements (conceptually, by verifying internal BigQuery mechanisms), and data quality/schema assertions for the audit tables.

**Assumptions:**
*   All DDLs for `job_log`, `job_control`, `job_log_detail`, `job_error_log` have been deployed to `your_project.your_dataset`.
*   All utility procedures (`util_create_job_entry`, `util_log_detail`, `util_set_stichtag_info`, `util_set_status_ok`, `util_handle_error`) have been deployed.
*   The main orchestration procedures (`sp_vertragsdatenabgleich`, `sp_vertragsdatenabgleich_entry`) have been deployed.
*   The placeholder core logic procedure (`sp_k_ausd_v_ta_action_assoc`) has been deployed.
*   `your_project.your_dataset` should be replaced with the actual BigQuery project and dataset names.
*   Tests should be run in an isolated environment or with appropriate cleanup to avoid interference between test runs.

---

## Migration Validation Tests for `r_ausd_v_ta_action_assoc.ksh`

### Test Case 1: Successful Job Execution - Basic Flow

*   **Purpose**: Verify the end-to-end successful execution of the migrated wrapper, including job initialization, `Stichtag` setting, core logic invocation (placeholder), and final status update. This covers output parity for successful runs.
*   **Setup**:
    1.  Ensure all DDLs and procedures are deployed.
    2.  Ensure the `sp_k_ausd_v_ta_action_assoc` procedure is in its default, non-erroring state (i.e., the `SELECT 1 / 0;` line is commented out).
    3.  Clear all audit tables before execution:
        ```sql
        TRUNCATE TABLE `your_project.your_dataset.job_log`;
        TRUNCATE TABLE `your_project.your_dataset.job_control`;
        TRUNCATE TABLE `your_project.your_dataset.job_log_detail`;
        TRUNCATE TABLE `your_project.your_dataset.job_error_log`;
        ```
*   **Action**:
    *   Call the entry-point stored procedure with a valid `Stichtag`.
    ```sql
    CALL `your_project.your_dataset.sp_vertragsdatenabgleich_entry`(CURRENT_DATE(), '1.0', FALSE);
    ```
*   **Pass/Fail Criterion**:
    1.  **Job Log Status**: Exactly one record exists in `your_project.your_dataset.job_log` with `status = 'OK'`, `job_name = 'ContractDataReconciliation'`, `start_time IS NOT NULL`, and `end_time IS NOT NULL`.
        ```sql
        SELECT
            COUNT(1) AS record_count,
            MAX(status) AS job_status,
            MAX(job_name) AS job_name_val
        FROM `your_project.your_dataset.job_log`
        WHERE
            status = 'OK'
            AND job_name = 'ContractDataReconciliation'
            AND start_time IS NOT NULL
            AND end_time IS NOT NULL;
        -- Expected: record_count = 1, job_status = 'OK', job_name_val = 'ContractDataReconciliation'
        ```
    2.  **Job Control Stichtag**: Exactly one record exists in `your_project.your_dataset.job_control` for the `job_id` from `job_log`, with `parameter_name = 'Stichtag'` and `parameter_value` matching the input `Stichtag` (formatted as YYYY-MM-DD).
        ```sql
        SELECT
            COUNT(1) AS record_count,
            MAX(parameter_name) AS param_name,
            MAX(parameter_value) AS param_value
        FROM `your_project.your_dataset.job_control`
        WHERE
            job_id = (SELECT job_id FROM `your_project.your_dataset.job_log` WHERE status = 'OK' LIMIT 1)
            AND parameter_name = 'Stichtag'
            AND parameter_value = FORMAT_DATE('%Y-%m-%d', CURRENT_DATE());
        -- Expected: record_count = 1, param_name = 'Stichtag', param_value = (current date in YYYY-MM-DD)
        ```
    3.  **Detailed Logs**: At least 6 records exist in `your_project.your_dataset.job_log_detail` for the `job_id`, containing expected messages:
        *   "Job ContractDataReconciliation started with Stichtag: YYYY-MM-DD" (INFO)
        *   "Stichtag set to: YYYY-MM-DD" (INFO)
        *   "Placeholder for core logic (sp_k_ausd_v_ta_action_assoc) called..." (INFO)
        *   "Core business logic executed successfully." (INFO)
        *   "Job completed successfully." (INFO)
        ```sql
        SELECT
            COUNT(1) AS log_message_count
        FROM `your_project.your_dataset.job_log_detail`
        WHERE
            job_id = (SELECT job_id FROM `your_project.your_dataset.job_log` WHERE status = 'OK' LIMIT 1)
            AND (
                message LIKE 'Job ContractDataReconciliation started%'
                OR message LIKE 'Stichtag set to%'
                OR message LIKE 'Placeholder for core logic%'
                OR message LIKE 'Core business logic executed successfully%'
                OR message LIKE 'Job completed successfully%'
            );
        -- Expected: log_message_count >= 5 (depending on exact logging, 6 is expected for this setup)
        ```
    4.  **Error Log Empty**: Zero records exist in `your_project.your_dataset.job_error_log`.
        ```sql
        SELECT COUNT(1) FROM `your_project.your_dataset.job_error_log`;
        -- Expected: 0
        ```

### Test Case 2: Error Handling - Core Logic Failure

*   **Purpose**: Verify that errors occurring within the core business logic (simulated by the placeholder `sp_k_ausd_v_ta_action_assoc`) are correctly caught, logged, and result in the job being marked as 'ERROR'. This covers transformation correctness for error handling.
*   **Setup**:
    1.  Ensure all DDLs and procedures are deployed.
    2.  **Modify `sp_k_ausd_v_ta_action_assoc` to simulate an error.** Uncomment the `SELECT 1 / 0;` line within the `BEGIN...END` block of `sp_k_ausd_v_ta_action_assoc`.
        ```sql
        -- In sql/procedures/sp_k_ausd_v_ta_action_assoc.sql:
        CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_action_assoc(
            IN p_job_id STRING,
            IN p_stichtag_date DATE
        )
        BEGIN
            -- ... existing logging ...
            -- Simulate an error for testing
            SELECT 1 / 0; -- UNCOMMENT THIS LINE
            -- ...
        END;
        ```
    3.  Clear all audit tables before execution (as in Test Case 1).
*   **Action**:
    *   Call the entry-point stored procedure. This call is expected to fail.
    ```sql
    CALL `your_project.your_dataset.sp_vertragsdatenabgleich_entry`(CURRENT_DATE(), '1.0', FALSE);
    ```
*   **Pass/Fail Criterion**:
    1.  **Call Fails**: The `CALL` statement itself should fail and raise an error (e.g., "Job failed for job_id ...: Division by zero").
    2.  **Job Log Status**: Exactly one record exists in `your_project.your_dataset.job_log` with `status = 'ERROR'`, `job_name = 'ContractDataReconciliation'`, `start_time IS NOT NULL`, and `end_time IS NOT NULL`.
        ```sql
        SELECT
            COUNT(1) AS record_count,
            MAX(status) AS job_status
        FROM `your_project.your_dataset.job_log`
        WHERE
            status = 'ERROR'
            AND job_name = 'ContractDataReconciliation'
            AND start_time IS NOT NULL
            AND end_time IS NOT NULL;
        -- Expected: record_count = 1, job_status = 'ERROR'
        ```
    3.  **Error Log Entry**: Exactly one record exists in `your_project.your_dataset.job_error_log` for the `job_id`, with `error_message` indicating a division by zero error, `severity = 'HIGH'`, and `source_procedure = 'sp_vertragsdatenabgleich'`.
        ```sql
        SELECT
            COUNT(1) AS error_count,
            MAX(error_message) AS err_msg,
            MAX(severity) AS err_severity,
            MAX(source_procedure) AS err_source
        FROM `your_project.your_dataset.job_error_log`
        WHERE
            job_id = (SELECT job_id FROM `your_project.your_dataset.job_log` WHERE status = 'ERROR' LIMIT 1)
            AND error_message LIKE '%Division by zero%'
            AND severity = 'HIGH'
            AND source_procedure = 'sp_vertragsdatenabgleich';
        -- Expected: error_count = 1, err_msg LIKE '%Division by zero%', err_severity = 'HIGH', err_source = 'sp_vertragsdatenabgleich'
        ```
    4.  **Detailed Logs**: Records exist in `your_project.your_dataset.job_log_detail` for the `job_id`, including "Job ... started", "Stichtag set to:", "Placeholder for core logic ... called", and "Job failed: Division by zero" (ERROR level).
        ```sql
        SELECT
            COUNTIF(message LIKE 'Job ContractDataReconciliation started%') AS start_msg,
            COUNTIF(message LIKE 'Stichtag set to%') AS stichtag_msg,
            COUNTIF(message LIKE 'Placeholder for core logic%') AS placeholder_msg,
            COUNTIF(message LIKE 'Job failed: Division by zero%' AND log_level = 'ERROR') AS error_msg
        FROM `your_project.your_dataset.job_log_detail`
        WHERE job_id = (SELECT job_id FROM `your_project.your_dataset.job_log` WHERE status = 'ERROR' LIMIT 1);
        -- Expected: start_msg = 1, stichtag_msg = 1, placeholder_msg = 1, error_msg = 1
        ```

### Test Case 3: Parameter Validation - Missing Stichtag

*   **Purpose**: Verify that the entry-point procedure correctly validates required parameters, specifically `p_stichtag_date`, and raises an error if it's missing, mimicking the legacy script's `getopts` error handling for missing arguments. This covers transformation correctness for parameter handling.
*   **Setup**:
    1.  Ensure all DDLs and procedures are deployed.
    2.  Ensure the `sp_k_ausd_v_ta_action_assoc` procedure is in its default, non-erroring state.
    3.  Clear all audit tables before execution.
*   **Action**:
    *   Attempt to call `sp_vertragsdatenabgleich_entry` without providing `p_stichtag_date` (pass `NULL`). This call is expected to fail.
    ```sql
    CALL `your_project.your_dataset.sp_vertragsdatenabgleich_entry`(NULL, '1.0', FALSE);
    ```
*   **Pass/Fail Criterion**:
    1.  **Call Fails**: The `CALL` statement should fail and raise an error with the exact message "Parameter p_stichtag_date must be provided.".
    2.  **Audit Tables Empty**: Zero records exist in `your_project.your_dataset.job_log`, `job_control`, `job_log_detail`, and `job_error_log`. This confirms the error occurs *before* any job initialization or logging.
        ```sql
        SELECT COUNT(1) FROM `your_project.your_dataset.job_log`; -- Expected: 0
        SELECT COUNT(1) FROM `your_project.your_dataset.job_control`; -- Expected: 0
        SELECT COUNT(1) FROM `your_project.your_dataset.job_log_detail`; -- Expected: 0
        SELECT COUNT(1) FROM `your_project.your_dataset.job_error_log`; -- Expected: 0
        ```

### Test Case 4: Debug Mode Logging

*   **Purpose**: Verify that the `p_debug` parameter correctly influences logging behavior, specifically adding debug messages when enabled. This covers output parity for conditional logging.
*   **Setup**:
    1.  Ensure all DDLs and procedures are deployed.
    2.  Ensure the `sp_k_ausd_v_ta_action_assoc` procedure is in its default, non-erroring state.
    3.  Clear all audit tables before execution.
*   **Action**:
    *   Call the entry-point stored procedure with `p_debug = TRUE`.
    ```sql
    CALL `your_project.your_dataset.sp_vertragsdatenabgleich_entry`(CURRENT_DATE(), '1.0', TRUE);
    ```
*   **Pass/Fail Criterion**:
    1.  **Job Log Status**: The job completes successfully (as per Test Case 1, criterion 1).
    2.  **Debug Log Entry**: At least one record exists in `your_project.your_dataset.job_log_detail` for the `job_id` with `log_level = 'DEBUG'` and `message = 'Debug mode is ON.'`.
        ```sql
        SELECT
            COUNT(1) AS debug_log_count
        FROM `your_project.your_dataset.job_log_detail`
        WHERE
            job_id = (SELECT job_id FROM `your_project.your_dataset.job_log` WHERE status = 'OK' LIMIT 1)
            AND log_level = 'DEBUG'
            AND message = 'Debug mode is ON.';
        -- Expected: debug_log_count = 1
        ```
    3.  All other success criteria from Test Case 1 are met.

### Test Case 5: Parameter Handling - Job Version

*   **Purpose**: Verify that the `p_job_version` parameter is correctly captured and stored in the `job_log` table. This covers transformation correctness for parameter handling.
*   **Setup**:
    1.  Ensure all DDLs and procedures are deployed.
    2.  Ensure the `sp_k_ausd_v_ta_action_assoc` procedure is in its default, non-erroring state.
    3.  Clear all audit tables before execution.
*   **Action**:
    *   Call the entry-point stored procedure with a specific `p_job_version`.
    ```sql
    CALL `your_project.your_dataset.sp_vertragsdatenabgleich_entry`(CURRENT_DATE(), '2.1-BETA', FALSE);
    ```
*   **Pass/Fail Criterion**:
    1.  **Job Log Version**: Exactly one record exists in `your_project.your_dataset.job_log` with `status = 'OK'` and `version = '2.1-BETA'`.
        ```sql
        SELECT
            COUNT(1) AS record_count,
            MAX(version) AS job_version
        FROM `your_project.your_dataset.job_log`
        WHERE
            status = 'OK'
            AND version = '2.1-BETA';
        -- Expected: record_count = 1, job_version = '2.1-BETA'
        ```
    2.  **Parameters JSON**: The `parameters_json` column in `job_log` contains the `p_job_version` value.
        ```sql
        SELECT
            JSON_VALUE(parameters_json, '$.p_job_version') AS extracted_version
        FROM `your_project.your_dataset.job_log`
        WHERE status = 'OK' LIMIT 1;
        -- Expected: extracted_version = '2.1-BETA'
        ```
    3.  All other success criteria from Test Case 1 are met.

### Test Case 6: Data Quality - `job_control` Stichtag Format

*   **Purpose**: Verify that the `Stichtag` is stored in `job_control` in the expected format (`YYYY-MM-DD`) and that the `valid_from` column is correctly populated as a `DATE` type. This covers data quality and type handling.
*   **Setup**:
    1.  Ensure all DDLs and procedures are deployed.
    2.  Ensure the `sp_k_ausd_v_ta_action_assoc` procedure is in its default, non-erroring state.
    3.  Clear all audit tables before execution.
*   **Action**:
    *   Call the entry-point stored procedure with a specific `Stichtag`.
    ```sql
    CALL `your_project.your_dataset.sp_vertragsdatenabgleich_entry`(DATE '2023-10-26', '1.0', FALSE);
    ```
*   **Pass/Fail Criterion**:
    1.  **Stichtag Value and Type**: Exactly one record exists in `your_project.your_dataset.job_control` for the `job_id` with `parameter_name = 'Stichtag'`, `parameter_value` exactly `'2023-10-26'`, and `valid_from` as `DATE '2023-10-26'`.
        ```sql
        SELECT
            COUNT(1) AS record_count,
            MAX(parameter_value) AS param_val,
            MAX(valid_from) AS valid_from_date,
            LOGICAL_AND(STARTS_WITH(parameter_value, '20') AND LENGTH(parameter_value) = 10) AS is_yyyy_mm_dd_format,
            LOGICAL_AND(TYPEOF(valid_from) = 'DATE') AS is_date_type
        FROM `your_project.your_dataset.job_control`
        WHERE
            job_id = (SELECT job_id FROM `your_project.your_dataset.job_log` WHERE status = 'OK' LIMIT 1)
            AND parameter_name = 'Stichtag';
        -- Expected: record_count = 1, param_val = '2023-10-26', valid_from_date = DATE '2023-10-26',
        --           is_yyyy_mm_dd_format = TRUE, is_date_type = TRUE
        ```
    2.  All other success criteria from Test Case 1 are met.

### Test Case 7: External System Replacement - Environment Variables / Config

*   **Purpose**: Verify that the migration correctly replaces the sourcing of environment variables (like `$HOME/.dw_init`, `BERT_DIR_ROOT`) and `DWMSG_` functions with BigQuery-native mechanisms (parameters, config tables, or direct procedure calls). This test conceptually confirms the absence of legacy external dependencies.
*   **Setup**:
    1.  Ensure all DDLs and procedures are deployed.
    2.  Ensure the `sp_k_ausd_v_ta_action_assoc` procedure is in its default, non-erroring state.
    3.  Clear all audit tables before execution.
*   **Action**:
    *   Execute a successful run of `sp_vertragsdatenabgleich_entry`.
    ```sql
    CALL `your_project.your_dataset.sp_vertragsdatenabgleich_entry`(CURRENT_DATE(), '1.0', FALSE);
    ```
*   **Pass/Fail Criterion**:
    1.  The job completes successfully (as per Test Case 1).
    2.  **Code Review / Manual Verification**: Confirm that the BigQuery procedures (`sp_vertragsdatenabgleich_entry`, `sp_vertragsdatenabgleich`, and the utility procedures) do not contain any references to external filesystems, shell commands, or environment variables that were part of the legacy `.dw_init` or `BERT_DIR_ROOT` context. Instead, all necessary configurations (like `job_name`, `ProgVersion`) are either hardcoded, passed as parameters, or derived from BigQuery tables/functions (e.g., `GENERATE_UUID()`, `CURRENT_TIMESTAMP()`).
    3.  The `job_log.parameters_json` column accurately reflects the input parameters, demonstrating that parameters are handled internally within BigQuery.

### Test Case 8: Row Count Assertions - Audit Tables

*   **Purpose**: Verify that the correct number of entries are made in the audit tables for a standard successful run, ensuring data quality and consistency in logging.
*   **Setup**:
    1.  Ensure all DDLs and procedures are deployed.
    2.  Ensure the `sp_k_ausd_v_ta_action_assoc` procedure is in its default, non-erroring state.
    3.  Clear all audit tables before execution.
*   **Action**:
    *   Call the entry-point stored procedure with a valid `Stichtag`.
    ```sql
    CALL `your_project.your_dataset.sp_vertragsdatenabgleich_entry`(CURRENT_DATE(), '1.0', FALSE);
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_log` Count**: `SELECT COUNT(1) FROM your_project.your_dataset.job_log` returns `1`.
    2.  **`job_control` Count**: `SELECT COUNT(1) FROM your_project.your_dataset.job_control` returns `1`.
    3.  **`job_error_log` Count**: `SELECT COUNT(1) FROM your_project.your_dataset.job_error_log` returns `0`.
    4.  **`job_log_detail` Count**: `SELECT COUNT(1) FROM your_project.your_dataset.job_log_detail` returns `6`. (This count is based on the current logging in the provided code: 1 from `util_create_job_entry`, 1 from `sp_vertragsdatenabgleich` start, 1 from `util_set_stichtag_info`, 1 from `sp_k_ausd_v_ta_action_assoc` placeholder, 1 from `sp_vertragsdatenabgleich` success, 1 from `util_set_status_ok`).
        ```sql
        SELECT COUNT(1) FROM `your_project.your_dataset.job_log`; -- Expected: 1
        SELECT COUNT(1) FROM `your_project.your_dataset.job_control`; -- Expected: 1
        SELECT COUNT(1) FROM `your_project.your_dataset.job_error_log`; -- Expected: 0
        SELECT COUNT(1) FROM `your_project.your_dataset.job_log_detail`; -- Expected: 6
        ```