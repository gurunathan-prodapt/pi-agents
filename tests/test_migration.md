As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_bp_ta_iccid_vertrag.ksh` to BigQuery stored procedures. The primary focus of this migration is the orchestration logic, parameter handling, and robust logging/error management. The core data processing logic within `k_ausd_bp_ta_iccid_vertrag` is treated as a black box for this wrapper migration, but its invocation and error propagation are critical.

The following test cases are designed to ensure the migrated BigQuery wrapper procedure (`project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`) is behaviourally equivalent to the legacy KornShell script, covering output parity, transformation correctness, external system replacements (logging), and data quality/schema assertions.

**Assumptions:**
*   The BigQuery tables (`job_control`, `job_run_log`, `job_error_log`, `job_usage_log`) and stored procedures (`k_ausd_bp_ta_iccid_vertrag`, `ausd_bp_ta_iccid_vertrag_wrapper`) have been deployed to `project.dataset`.
*   `project.dataset.k_ausd_bp_ta_iccid_vertrag` is implemented as provided in the `GENERATED MIGRATION CODE` section (i.e., it logs its execution and then returns successfully).
*   Tests will be executed sequentially, and logging tables will be cleared before each relevant test to ensure isolation.
*   The `CURRENT_DATE()` function in BigQuery will be consistent during a test run.

---

### Test Setup Helper (Run before each test case that requires a clean state)

```sql
-- Replace with your actual project and dataset IDs
DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

-- Clear all logging and control tables
EXECUTE IMMEDIATE FORMAT("DELETE FROM %s.%s.job_control WHERE TRUE", PROJECT_ID, DATASET_ID);
EXECUTE IMMEDIATE FORMAT("DELETE FROM %s.%s.job_run_log WHERE TRUE", PROJECT_ID, DATASET_ID);
EXECUTE IMMEDIATE FORMAT("DELETE FROM %s.%s.job_error_log WHERE TRUE", PROJECT_ID, DATASET_ID);
EXECUTE IMMEDIATE FORMAT("DELETE FROM %s.%s.job_usage_log WHERE TRUE", PROJECT_ID, DATASET_ID);
```

---

### Test Case 1: Successful Execution with All Parameters Provided

*   **Purpose**: Verify that the wrapper procedure correctly accepts both `p_stichtag` and `p_wiederanlaufWert`, logs the execution, calls the core procedure, and updates the job status to 'OK'.
*   **Setup**:
    *   Clear all logging and control tables using the helper script above.
*   **Action**:
    ```sql
    -- Replace with your actual project and dataset IDs
    DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
    DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(
        p_stichtag => '01012023',
        p_wiederanlaufWert => 100
    );
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_control`**: One entry exists with `job_name = 'r_ausd_bp_ta_iccid_vertrag_wrapper_sp'`, `stichtag = '01012023'`, `wiederanlaufwert = 100`, `status = 'OK'`, `start_timestamp` and `end_timestamp` are populated.
    2.  **`job_run_log`**: At least 4 'INFO' entries exist, including messages for job start, parameters, core procedure call, and successful completion.
    3.  **`job_error_log`**: No entries exist.
    4.  **`job_usage_log`**: No entries exist.

    ```sql
    -- Assertions
    -- Check job_control
    SELECT
        COUNT(1) AS record_count,
        MAX(CASE WHEN job_name = 'r_ausd_bp_ta_iccid_vertrag_wrapper_sp' AND stichtag = '01012023' AND wiederanlaufwert = 100 AND status = 'OK' AND start_timestamp IS NOT NULL AND end_timestamp IS NOT NULL THEN 1 ELSE 0 END) AS is_correct_entry
    FROM project.dataset.job_control;
    -- Expected: record_count = 1, is_correct_entry = 1

    -- Check job_run_log
    SELECT
        COUNT(1) AS info_log_count,
        MAX(CASE WHEN message LIKE '%Job started%' THEN 1 ELSE 0 END) AS has_start_msg,
        MAX(CASE WHEN message LIKE '%Parameters - Stichtag: 01012023, Wiederanlaufwert: 100%' THEN 1 ELSE 0 END) AS has_param_msg,
        MAX(CASE WHEN message LIKE '%Calling core procedure k_ausd_bp_ta_iccid_vertrag%' THEN 1 ELSE 0 END) AS has_core_call_msg,
        MAX(CASE WHEN message LIKE '%Core procedure k_ausd_bp_ta_iccid_vertrag completed successfully%' THEN 1 ELSE 0 END) AS has_core_success_msg
    FROM project.dataset.job_run_log
    WHERE log_level = 'INFO';
    -- Expected: info_log_count >= 4, has_start_msg=1, has_param_msg=1, has_core_call_msg=1, has_core_success_msg=1

    -- Check job_error_log
    SELECT COUNT(1) FROM project.dataset.job_error_log;
    -- Expected: 0

    -- Check job_usage_log
    SELECT COUNT(1) FROM project.dataset.job_usage_log;
    -- Expected: 0
    ```

---

### Test Case 2: Successful Execution with Default Stichtag

*   **Purpose**: Verify that `p_stichtag` defaults to `CURRENT_DATE()` (DDMMYYYY format) when not provided, and the job completes successfully.
*   **Setup**:
    *   Clear all logging and control tables using the helper script.
*   **Action**:
    ```sql
    -- Replace with your actual project and dataset IDs
    DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
    DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(
        p_stichtag => NULL,
        p_wiederanlaufWert => 200
    );
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_control`**: One entry exists with `stichtag` matching `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `wiederanlaufwert = 200`, and `status = 'OK'`.
    2.  **`job_run_log`**: Contains messages reflecting the defaulted `stichtag`.
    3.  **`job_error_log`**: No entries exist.
    4.  **`job_usage_log`**: No entries exist.

    ```sql
    -- Assertions
    DECLARE expected_stichtag STRING;
    SET expected_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Check job_control
    SELECT
        COUNT(1) AS record_count,
        MAX(CASE WHEN stichtag = expected_stichtag AND wiederanlaufwert = 200 AND status = 'OK' THEN 1 ELSE 0 END) AS is_correct_entry
    FROM project.dataset.job_control;
    -- Expected: record_count = 1, is_correct_entry = 1

    -- Check job_run_log for defaulted stichtag
    SELECT
        COUNT(1) AS log_count,
        MAX(CASE WHEN message LIKE FORMAT('%%Parameters - Stichtag: %s, Wiederanlaufwert: 200%%', expected_stichtag) THEN 1 ELSE 0 END) AS has_correct_param_msg
    FROM project.dataset.job_run_log
    WHERE log_level = 'INFO';
    -- Expected: log_count >= 4, has_correct_param_msg = 1
    ```

---

### Test Case 3: Successful Execution with Default Wiederanlaufwert

*   **Purpose**: Verify that `p_wiederanlaufWert` defaults to `0` when not provided, and the job completes successfully.
*   **Setup**:
    *   Clear all logging and control tables using the helper script.
*   **Action**:
    ```sql
    -- Replace with your actual project and dataset IDs
    DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
    DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(
        p_stichtag => '15032024',
        p_wiederanlaufWert => NULL
    );
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_control`**: One entry exists with `stichtag = '15032024'`, `wiederanlaufwert = 0`, and `status = 'OK'`.
    2.  **`job_run_log`**: Contains messages reflecting the defaulted `wiederanlaufwert`.
    3.  **`job_error_log`**: No entries exist.
    4.  **`job_usage_log`**: No entries exist.

    ```sql
    -- Assertions
    -- Check job_control
    SELECT
        COUNT(1) AS record_count,
        MAX(CASE WHEN stichtag = '15032024' AND wiederanlaufwert = 0 AND status = 'OK' THEN 1 ELSE 0 END) AS is_correct_entry
    FROM project.dataset.job_control;
    -- Expected: record_count = 1, is_correct_entry = 1

    -- Check job_run_log for defaulted wiederanlaufwert
    SELECT
        COUNT(1) AS log_count,
        MAX(CASE WHEN message LIKE '%%Parameters - Stichtag: 15032024, Wiederanlaufwert: 0%%' THEN 1 ELSE 0 END) AS has_correct_param_msg
    FROM project.dataset.job_run_log
    WHERE log_level = 'INFO';
    -- Expected: log_count >= 4, has_correct_param_msg = 1
    ```

---

### Test Case 4: Successful Execution with Both Parameters Defaulted

*   **Purpose**: Verify that both `p_stichtag` and `p_wiederanlaufWert` default correctly when neither is provided, and the job completes successfully.
*   **Setup**:
    *   Clear all logging and control tables using the helper script.
*   **Action**:
    ```sql
    -- Replace with your actual project and dataset IDs
    DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
    DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(
        p_stichtag => NULL,
        p_wiederanlaufWert => NULL
    );
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_control`**: One entry exists with `stichtag` matching `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `wiederanlaufwert = 0`, and `status = 'OK'`.
    2.  **`job_run_log`**: Contains messages reflecting both defaulted parameters.
    3.  **`job_error_log`**: No entries exist.
    4.  **`job_usage_log`**: No entries exist.

    ```sql
    -- Assertions
    DECLARE expected_stichtag STRING;
    SET expected_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Check job_control
    SELECT
        COUNT(1) AS record_count,
        MAX(CASE WHEN stichtag = expected_stichtag AND wiederanlaufwert = 0 AND status = 'OK' THEN 1 ELSE 0 END) AS is_correct_entry
    FROM project.dataset.job_control;
    -- Expected: record_count = 1, is_correct_entry = 1

    -- Check job_run_log for defaulted parameters
    SELECT
        COUNT(1) AS log_count,
        MAX(CASE WHEN message LIKE FORMAT('%%Parameters - Stichtag: %s, Wiederanlaufwert: 0%%', expected_stichtag) THEN 1 ELSE 0 END) AS has_correct_param_msg
    FROM project.dataset.job_run_log
    WHERE log_level = 'INFO';
    -- Expected: log_count >= 4, has_correct_param_msg = 1
    ```

---

### Test Case 5: Invalid Stichtag Format (e.g., YYYY-MM-DD)

*   **Purpose**: Verify that the procedure correctly validates the `p_stichtag` format (DDMMYYYY) and handles invalid input by logging errors and terminating.
*   **Setup**:
    *   Clear all logging and control tables using the helper script.
*   **Action**:
    ```sql
    -- Replace with your actual project and dataset IDs
    DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
    DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

    -- This call is expected to fail and raise an error
    BEGIN
        CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(
            p_stichtag => '2023-01-01', -- Invalid format
            p_wiederanlaufWert => 100
        );
    EXCEPTION WHEN ERROR THEN
        -- Expected error, do nothing or log for test runner
        SELECT 'Caught expected error for invalid Stichtag' AS status;
    END;
    ```
*   **Pass/Fail Criterion**:
    1.  **Procedure Call**: The `CALL` statement should terminate with an error (due to `RAISE`).
    2.  **`job_control`**: One entry exists with `status = 'ERROR'`, `end_timestamp` populated.
    3.  **`job_error_log`**: One entry exists with `error_message` indicating "Invalid Stichtag format" and `stichtag = '2023-01-01'`.
    4.  **`job_usage_log`**: One entry exists with a usage message and `provided_stichtag = '2023-01-01'`.
    5.  **`job_run_log`**: Contains an 'ERROR' entry related to the validation failure.

    ```sql
    -- Assertions
    -- Check job_control
    SELECT
        COUNT(1) AS record_count,
        MAX(CASE WHEN status = 'ERROR' AND stichtag = '2023-01-01' AND wiederanlaufwert = 100 AND end_timestamp IS NOT NULL THEN 1 ELSE 0 END) AS is_correct_error_entry
    FROM project.dataset.job_control;
    -- Expected: record_count = 1, is_correct_error_entry = 1

    -- Check job_error_log
    SELECT
        COUNT(1) AS error_count,
        MAX(CASE WHEN error_message LIKE '%Invalid Stichtag format%' AND stichtag = '2023-01-01' THEN 1 ELSE 0 END) AS has_correct_error_msg
    FROM project.dataset.job_error_log;
    -- Expected: error_count = 1, has_correct_error_msg = 1

    -- Check job_usage_log
    SELECT
        COUNT(1) AS usage_count,
        MAX(CASE WHEN message LIKE '%Invalid Stichtag provided: 2023-01-01%' AND provided_stichtag = '2023-01-01' THEN 1 ELSE 0 END) AS has_correct_usage_msg
    FROM project.dataset.job_usage_log;
    -- Expected: usage_count = 1, has_correct_usage_msg = 1

    -- Check job_run_log for error message
    SELECT
        COUNT(1) AS error_log_count,
        MAX(CASE WHEN log_level = 'ERROR' AND message LIKE '%Validation Failed: Invalid Stichtag format "2023-01-01"%' THEN 1 ELSE 0 END) AS has_validation_error_log
    FROM project.dataset.job_run_log;
    -- Expected: error_log_count >= 1, has_validation_error_log = 1
    ```

---

### Test Case 6: Core Procedure (`k_ausd_bp_ta_iccid_vertrag`) Failure

*   **Purpose**: Verify that if the invoked core procedure fails, the wrapper catches the error, logs it, and updates the job status to 'ERROR'.
*   **Setup**:
    *   Clear all logging and control tables using the helper script.
    *   **Temporarily modify `k_ausd_bp_ta_iccid_vertrag` to raise an error.**
        ```sql
        -- Replace with your actual project and dataset IDs
        DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
        DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

        CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_bp_ta_iccid_vertrag(
            job_entry_nr INT64,
            p_stichtag STRING,
            p_wiederanlaufWert INT64
        )
        BEGIN
            INSERT INTO project.dataset.job_run_log (log_timestamp, job_entry_nr, log_level, message)
            VALUES (CURRENT_TIMESTAMP(), job_entry_nr, 'INFO', FORMAT('Executing core logic for Stichtag: %s, Wiederanlaufwert: %d (SIMULATING FAILURE)', p_stichtag, p_wiederanlaufWert));
            RAISE 'Simulated error in k_ausd_bp_ta_iccid_vertrag'; -- Simulate an error
        END;
        ```
*   **Action**:
    ```sql
    -- Replace with your actual project and dataset IDs
    DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
    DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

    -- This call is expected to fail and raise an error
    BEGIN
        CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(
            p_stichtag => '01012023',
            p_wiederanlaufWert => 100
        );
    EXCEPTION WHEN ERROR THEN
        -- Expected error, do nothing or log for test runner
        SELECT 'Caught expected error from core procedure failure' AS status;
    END;
    ```
*   **Pass/Fail Criterion**:
    1.  **Procedure Call**: The `CALL` statement should terminate with an error (due to `RAISE`).
    2.  **`job_control`**: One entry exists with `status = 'ERROR'`, `end_timestamp` populated.
    3.  **`job_error_log`**: One entry exists with `error_message` containing "Simulated error in k_ausd_bp_ta_iccid_vertrag" and a populated `stack_trace`.
    4.  **`job_usage_log`**: No entries exist.
    5.  **`job_run_log`**: Contains an 'ERROR' entry related to the core procedure failure.

    ```sql
    -- Assertions
    -- Check job_control
    SELECT
        COUNT(1) AS record_count,
        MAX(CASE WHEN status = 'ERROR' AND stichtag = '01012023' AND wiederanlaufwert = 100 AND end_timestamp IS NOT NULL THEN 1 ELSE 0 END) AS is_correct_error_entry
    FROM project.dataset.job_control;
    -- Expected: record_count = 1, is_correct_error_entry = 1

    -- Check job_error_log
    SELECT
        COUNT(1) AS error_count,
        MAX(CASE WHEN error_message LIKE '%Simulated error in k_ausd_bp_ta_iccid_vertrag%' AND stack_trace IS NOT NULL THEN 1 ELSE 0 END) AS has_correct_error_msg
    FROM project.dataset.job_error_log;
    -- Expected: error_count = 1, has_correct_error_msg = 1

    -- Check job_usage_log
    SELECT COUNT(1) FROM project.dataset.job_usage_log;
    -- Expected: 0

    -- Check job_run_log for error message
    SELECT
        COUNT(1) AS error_log_count,
        MAX(CASE WHEN log_level = 'ERROR' AND message LIKE '%Job failed: r_ausd_bp_ta_iccid_vertrag_wrapper_sp. Error: Simulated error in k_ausd_bp_ta_iccid_vertrag%' THEN 1 ELSE 0 END) AS has_core_error_log
    FROM project.dataset.job_run_log;
    -- Expected: error_log_count >= 1, has_core_error_log = 1
    ```
*   **Cleanup**:
    *   **Restore `k_ausd_bp_ta_iccid_vertrag` to its original (placeholder) state.**
        ```sql
        -- Replace with your actual project and dataset IDs
        DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
        DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

        CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_bp_ta_iccid_vertrag(
            job_entry_nr INT64,
            p_stichtag STRING,
            p_wiederanlaufWert INT64
        )
        BEGIN
            INSERT INTO project.dataset.job_run_log (log_timestamp, job_entry_nr, log_level, message)
            VALUES (CURRENT_TIMESTAMP(), job_entry_nr, 'INFO', FORMAT('Executing core logic for Stichtag: %s, Wiederanlaufwert: %d', p_stichtag, p_wiederanlaufWert));
            SELECT 'Core procedure executed successfully' AS status;
        END;
        ```

---

### Test Case 7: `job_entry_nr` Generation and Increment

*   **Purpose**: Verify that `job_entry_nr` is correctly generated by incrementing the maximum existing entry in `job_control` for sequential runs.
*   **Setup**:
    *   Clear all logging and control tables using the helper script.
*   **Action**:
    ```sql
    -- Replace with your actual project and dataset IDs
    DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project-id';
    DECLARE DATASET_ID STRING DEFAULT 'your-bigquery-dataset';

    -- First call
    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(p_stichtag => '01012023', p_wiederanlaufWert => 1);
    -- Second call
    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(p_stichtag => '02012023', p_wiederanlaufWert => 2);
    -- Third call
    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(p_stichtag => '03012023', p_wiederanlaufWert => 3);
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_control`**: Three entries exist, with `job_entry_nr` values of 1, 2, and 3 (or 1, 2, 3 if starting from 0). The `stichtag` and `wiederanlaufwert` should match the input parameters for each respective call. All statuses should be 'OK'.

    ```sql
    -- Assertions
    SELECT
        job_entry_nr,
        stichtag,
        wiederanlaufwert,
        status
    FROM project.dataset.job_control
    ORDER BY job_entry_nr;
    -- Expected output:
    -- job_entry_nr | stichtag   | wiederanlaufwert | status
    -- -------------|------------|------------------|--------
    -- 1            | '01012023' | 1                | 'OK'
    -- 2            | '02012023' | 2                | 'OK'
    -- 3            | '03012023' | 3                | 'OK'

    SELECT COUNT(DISTINCT job_entry_nr) FROM project.dataset.job_control;
    -- Expected: 3
    ```

---

### Test Case 8: Schema and Data Type Integrity

*   **Purpose**: Verify that the data types and nullability constraints of the logging tables are respected and that data is inserted without type conversion errors. This is implicitly covered by previous tests, but an explicit check ensures the DDLs are correct and the SPs adhere to them.
*   **Setup**:
    *   Clear all logging and control tables using the helper script.
*   **Action**:
    *   Execute Test Case 1 (Successful Execution with All Parameters Provided).
*   **Pass/Fail Criterion**:
    1.  **`job_control`**: All columns (`job_entry_nr`, `job_name`, `stichtag`, `wiederanlaufwert`, `start_timestamp`, `end_timestamp`, `status`) contain data of the expected types and no NULLs where `NOT NULL` is specified.
    2.  **`job_run_log`**: All columns (`log_timestamp`, `job_entry_nr`, `log_level`, `message`) contain data of the expected types and no NULLs where `NOT NULL` is specified.
    3.  **`job_error_log`**: All columns (`error_timestamp`, `job_entry_nr`, `error_message`, `stack_trace`, `stichtag`, `wiederanlaufwert`) contain data of the expected types. `stack_trace`, `stichtag`, `wiederanlaufwert` can be NULL.
    4.  **`job_usage_log`**: All columns (`usage_timestamp`, `job_entry_nr`, `message`, `provided_stichtag`, `provided_wiederanlaufwert`) contain data of the expected types. `job_entry_nr`, `provided_stichtag`, `provided_wiederanlaufwert` can be NULL.

    ```sql
    -- Assertions (example for job_control, similar checks for other tables)
    SELECT
        COUNT(1) AS total_rows,
        COUNTIF(job_entry_nr IS NULL) AS null_job_entry_nr,
        COUNTIF(job_name IS NULL) AS null_job_name,
        COUNTIF(status IS NULL) AS null_status,
        COUNTIF(start_timestamp IS NULL) AS null_start_timestamp,
        -- stichtag, wiederanlaufwert, end_timestamp can be NULL based on context
        COUNTIF(NOT SAFE.PARSE_DATE('%d%m%Y', stichtag) IS NULL) AS valid_stichtag_format_count, -- Check format if not null
        COUNTIF(wiederanlaufwert IS NOT NULL AND NOT SAFE_CAST(wiederanlaufwert AS INT64) IS NULL) AS valid_wiederanlaufwert_type_count
    FROM project.dataset.job_control;
    -- Expected: total_rows = 1, null_job_entry_nr = 0, null_job_name = 0, null_status = 0, null_start_timestamp = 0
    --           valid_stichtag_format_count = 1 (if stichtag is not null), valid_wiederanlaufwert_type_count = 1 (if wiederanlaufwert is not null)

    -- Example for job_run_log
    SELECT
        COUNT(1) AS total_rows,
        COUNTIF(log_timestamp IS NULL) AS null_log_timestamp,
        COUNTIF(job_entry_nr IS NULL) AS null_job_entry_nr,
        COUNTIF(log_level IS NULL) AS null_log_level,
        COUNTIF(message IS NULL) AS null_message
    FROM project.dataset.job_run_log;
    -- Expected: total_rows >= 4, all null_counts = 0
    ```

---