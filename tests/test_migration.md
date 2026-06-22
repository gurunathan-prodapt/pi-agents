The migration of `r_ausd_v_ta_p_discount_rr.ksh` to BigQuery Stored Procedures orchestrated by Airflow requires thorough validation. These tests focus on ensuring behavioral equivalence, particularly for the wrapper script's responsibilities: parameter handling, logging, and error management, given that the core reconciliation logic (`k_ausd_v_ta_p_discount_rr.ksh` / `core_discount_rr_process`) is treated as a black box for these wrapper-level tests.

---

## Migration Validation Tests for `r_ausd_v_ta_p_discount_rr.ksh`

### Prerequisites for all Tests:

*   **Legacy Environment:** Access to a system where the original `r_ausd_v_ta_p_discount_rr.ksh` script can be executed, and its output (stdout, stderr, log files, exit codes) can be captured.
*   **GCP Environment:**
    *   BigQuery `my_gcp_project.my_bq_dataset.job_audit_log` table created.
    *   BigQuery Stored Procedures `my_gcp_project.my_bq_dataset.vertragsdatenabgleich` and `my_gcp_project.my_bq_dataset.core_discount_rr_process` deployed.
    *   Airflow DAG `r_ausd_v_ta_p_discount_rr_dag` deployed and configured with the correct GCP connection.
*   **Test Data:** No specific input data is required for the wrapper script tests, as they focus on orchestration and logging. However, the `core_discount_rr_process` placeholder should be configured to either succeed or fail as needed for specific test cases.

---

### 1. Output Parity Tests

These tests verify that the migrated job produces equivalent outputs (logs, status) for the same inputs as the legacy system.

#### Test Case 1.1: Successful Execution (Happy Path)

*   **Purpose:** Verify the migrated job completes successfully and logs correctly when the core logic succeeds, mirroring the legacy script's successful run.
*   **Setup:**
    *   Ensure the `core_discount_rr_process` BigQuery Stored Procedure is configured to complete successfully (i.e., no `RAISE SCRIPT EXCEPTION` is active).
    *   Record the current timestamp before execution to filter `job_audit_log` entries.
*   **Action:**
    1.  **Legacy:** Execute the original KornShell script with valid parameters:
        ```bash
        # Example: Simulate a valid stichtag and log level
        # Note: The -s and -l parameters are parsed but not used in the wrapper itself,
        # but are passed to the core script. For this test, any valid format is fine.
        # Capture stdout, stderr, and the generated log file.
        LEGACY_START_TIME=$(date +%s)
        ./r_ausd_v_ta_p_discount_rr.ksh -s 20231026 -l INFO > legacy_stdout.log 2>&1
        LEGACY_EXIT_CODE=$?
        # Find the generated log file (e.g., r_ausd_v_ta_p_discount_rr_XXXXXXXX.log)
        # and capture its content.
        ```
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr_dag` with equivalent parameters:
        *   `p_stichtag_val = "20231026"`
        *   `p_log_level_val = "INFO"`
        (Ensure the DAG's `p_stichtag_val` is set to "20231026" or use an Airflow macro that resolves to it).
*   **Pass/Fail Criterion:**
    *   **Legacy:**
        *   `LEGACY_EXIT_CODE` must be `0`.
        *   The generated log file (e.g., `r_ausd_v_ta_p_discount_rr_*.log`) must contain messages indicating successful start, core script invocation, and successful completion.
        *   Example log content: "Job started.", "Calling core reconciliation logic.", "Die Abarbeitung wurde ohne erkennbare Fehler beendet".
    *   **Migrated:**
        *   The Airflow task `execute_vertragsdatenabgleich_sp` must succeed.
        *   Query the `job_audit_log` table for entries created after `LEGACY_START_TIME` (or a more precise timestamp from the Airflow run):
            ```sql
            SELECT
                job_id,
                entry_number,
                status,
                message,
                error_code,
                error_message,
                stichtag_info,
                parameters
            FROM
                `my_gcp_project.my_bq_dataset.job_audit_log`
            WHERE
                logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE) -- Adjust interval as needed
                AND status = 'SUCCESS'
            ORDER BY
                logged_at ASC;
            ```
        *   The query result must contain exactly 3 entries for a single `job_id` with `status = 'SUCCESS'`.
        *   The `message` fields of these entries must correspond to:
            1.  `'Job started.'`
            2.  `'Calling core reconciliation logic.'`
            3.  `'Job completed successfully.'`
        *   The `stichtag_info` field in the first entry must be `'Processing data for Stichtag: 20231026'`.
        *   The `parameters` JSON field must correctly reflect `{"p_stichtag":"20231026","p_log_level":"INFO"}`.
        *   `error_code` and `error_message` fields must be `NULL`.

#### Test Case 1.2: Invalid Parameter Handling (`-s` missing/invalid)

*   **Purpose:** Verify the migrated job handles invalid or missing `p_stichtag` parameters identically to the legacy script's `ErrNr=192` (parameter unknown/missing argument).
*   **Setup:** Record the current timestamp before execution.
*   **Action:**
    1.  **Legacy:** Execute the original KornShell script with an invalid `-s` parameter (e.g., missing value):
        ```bash
        LEGACY_START_TIME=$(date +%s)
        ./r_ausd_v_ta_p_discount_rr.ksh -s > legacy_stdout_invalid_s.log 2>&1
        LEGACY_EXIT_CODE=$?
        ```
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr_dag` with `p_stichtag_val = NULL` or a malformed string (e.g., "2023-10-26" if YYYYMMDD is strictly enforced).
        *   For `p_stichtag_val = NULL`:
            ```python
            # In DAG definition:
            p_stichtag_val = None # Or an empty string ""
            ```
*   **Pass/Fail Criterion:**
    *   **Legacy:**
        *   `LEGACY_EXIT_CODE` must be `193` (for missing argument) or `192` (for unknown parameter if `-s` was not recognized). The script's `getopts` logic for `:)` maps to `ErrNr=193`.
        *   `legacy_stdout_invalid_s.log` must contain "Notwendiges Argument fehlt" (Necessary argument missing) or similar, and the `usage` message.
    *   **Migrated:**
        *   The Airflow task `execute_vertragsdatenabgleich_sp` must fail.
        *   Query the `job_audit_log` table for entries related to this run:
            ```sql
            SELECT
                job_id,
                entry_number,
                status,
                message,
                error_code,
                error_message,
                parameters
            FROM
                `my_gcp_project.my_bq_dataset.job_audit_log`
            WHERE
                logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE) -- Adjust interval
                AND status = 'FAILED'
            ORDER BY
                logged_at ASC;
            ```
        *   The query result must contain exactly 2 entries for a single `job_id` with `status = 'FAILED'`.
        *   The `error_code` field in the final entry must be `'192'`.
        *   The `error_message` field must contain `'Invalid or missing p_stichtag parameter. Expected YYYYMMDD.'`.
        *   The `message` field of the final entry must be `'Job failed.'`.

#### Test Case 1.3: Core Script Failure Simulation

*   **Purpose:** Verify the migrated job correctly captures and logs errors originating from the core reconciliation logic, mirroring the legacy script's `trap ERR` behavior.
*   **Setup:**
    *   Modify the `core_discount_rr_process` BigQuery Stored Procedure to `RAISE SCRIPT EXCEPTION` unconditionally or based on a specific `p_stichtag` value (e.g., uncomment the `RAISE SCRIPT EXCEPTION` line).
    *   Record the current timestamp before execution.
*   **Action:**
    1.  **Legacy:** Modify `k_ausd_v_ta_p_discount_rr.ksh` to simulate an error (e.g., `exit 1` or `false` command) and execute `r_ausd_v_ta_p_discount_rr.ksh` with valid parameters.
        ```bash
        # Example: Modify k_ausd_v_ta_p_discount_rr.ksh to contain 'false' at the beginning
        # Then run:
        LEGACY_START_TIME=$(date +%s)
        ./r_ausd_v_ta_p_discount_rr.ksh -s 20231026 -l INFO > legacy_stdout_core_fail.log 2>&1
        LEGACY_EXIT_CODE=$?
        ```
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr_dag` with valid parameters (e.g., `p_stichtag_val = "20231026"`).
*   **Pass/Fail Criterion:**
    *   **Legacy:**
        *   `LEGACY_EXIT_CODE` must be non-zero (e.g., `1` or the error code from `DWMSG_MeldeFehler`).
        *   The generated log file must contain "AppError: Abbruch" or similar error messages from `DWMSG_Fehlerbehandlung`.
    *   **Migrated:**
        *   The Airflow task `execute_vertragsdatenabgleich_sp` must fail.
        *   Query the `job_audit_log` table for entries related to this run:
            ```sql
            SELECT
                job_id,
                entry_number,
                status,
                message,
                error_code,
                error_message
            FROM
                `my_gcp_project.my_bq_dataset.job_audit_log`
            WHERE
                logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE) -- Adjust interval
                AND status = 'FAILED'
            ORDER BY
                logged_at ASC;
            ```
        *   The query result must contain exactly 3 entries for a single `job_id` with `status = 'FAILED'`.
        *   The `error_code` field in the final entry must be `'193'` (default for unexpected errors from core logic).
        *   The `error_message` field must contain the message from the `RAISE SCRIPT EXCEPTION` in `core_discount_rr_process` (e.g., "Simulated error in core process...").
        *   The `message` field of the final entry must be `'Job failed.'`.

---

### 2. Transformation Correctness Tests (Wrapper Logic)

These tests focus on the internal logic of the wrapper script's migration, such as logging content, error code mapping, and metadata generation.

#### Test Case 2.1: Logging Content and Sequence

*   **Purpose:** Verify all expected log messages are recorded in `job_audit_log` in the correct sequence, with accurate metadata, for a successful run.
*   **Setup:** Execute a successful run of the migrated job (as in Test Case 1.1).
*   **Action:** Query the `job_audit_log` table for the `job_id` of the successful run.
    ```sql
    SELECT
        entry_number,
        message,
        status,
        stichtag_info,
        parameters,
        start_timestamp,
        end_timestamp,
        logged_at
    FROM
        `my_gcp_project.my_bq_dataset.job_audit_log`
    WHERE
        job_id = '{{ JOB_ID_FROM_SUCCESSFUL_RUN }}'
    ORDER BY
        entry_number ASC;
    ```
*   **Pass/Fail Criterion:**
    *   The query must return 3 rows.
    *   **Row 1 (Job Start):**
        *   `entry_number` = 1
        *   `message` = `'Job started.'`
        *   `status` = `'RUNNING'`
        *   `stichtag_info` = `'Processing data for Stichtag: 20231026'` (assuming `p_stichtag` was '20231026')
        *   `parameters` = `{"p_stichtag":"20231026","p_log_level":"INFO"}`
        *   `start_timestamp` is populated. `end_timestamp` is `NULL`.
    *   **Row 2 (Calling Core):**
        *   `entry_number` = 2
        *   `message` = `'Calling core reconciliation logic.'`
        *   `status` = `'RUNNING'`
        *   `stichtag_info` = `NULL`
        *   `parameters` = `NULL`
        *   `start_timestamp` and `end_timestamp` are `NULL`.
    *   **Row 3 (Job Success):**
        *   `entry_number` = 3
        *   `message` = `'Job completed successfully.'`
        *   `status` = `'SUCCESS'`
        *   `stichtag_info` = `'Processing data for Stichtag: 20231026'`
        *   `parameters` = `NULL`
        *   `start_timestamp` and `end_timestamp` are populated, with `end_timestamp` being after `start_timestamp`.
    *   All `logged_at` timestamps must be sequential and increasing.

#### Test Case 2.2: Error Code Mapping

*   **Purpose:** Verify that specific legacy error codes (`192` for parameter validation, `193` for general script errors) are correctly mapped and recorded in the `job_audit_log`.
*   **Setup:**
    *   Execute a run with invalid parameters (as in Test Case 1.2) to get `job_id_param_fail`.
    *   Execute a run with simulated core script failure (as in Test Case 1.3) to get `job_id_core_fail`.
*   **Action:** Query `job_audit_log` for the final entry of each failed run.
    ```sql
    -- For parameter failure
    SELECT error_code, error_message FROM `my_gcp_project.my_bq_dataset.job_audit_log`
    WHERE job_id = '{{ JOB_ID_PARAM_FAIL }}' AND status = 'FAILED';

    -- For core logic failure
    SELECT error_code, error_message FROM `my_gcp_project.my_bq_dataset.job_audit_log`
    WHERE job_id = '{{ JOB_ID_CORE_FAIL }}' AND status = 'FAILED';
    ```
*   **Pass/Fail Criterion:**
    *   For `job_id_param_fail`: `error_code` must be `'192'` and `error_message` must contain `'Invalid or missing p_stichtag parameter.'`.
    *   For `job_id_core_fail`: `error_code` must be `'193'` and `error_message` must contain the simulated error message from `core_discount_rr_process`.

#### Test Case 2.3: `stichtag_info` Formatting

*   **Purpose:** Verify the `stichtag_info` field in `job_audit_log` is correctly formatted based on the input `p_stichtag`.
*   **Setup:** Execute a successful run of the migrated job with a specific `p_stichtag` (e.g., "20231026").
*   **Action:** Query the `job_audit_log` table for the `job_id` of the successful run, specifically looking at the first and last entries.
    ```sql
    SELECT
        entry_number,
        stichtag_info
    FROM
        `my_gcp_project.my_bq_dataset.job_audit_log`
    WHERE
        job_id = '{{ JOB_ID_FROM_SUCCESSFUL_RUN }}'
        AND entry_number IN (1, 3) -- First and last entries
    ORDER BY
        entry_number ASC;
    ```
*   **Pass/Fail Criterion:**
    *   For `entry_number = 1`, `stichtag_info` must be `'Processing data for Stichtag: 20231026'`.
    *   For `entry_number = 3`, `stichtag_info` must also be `'Processing data for Stichtag: 20231026'`.

#### Test Case 2.4: Concurrent Execution Handling

*   **Purpose:** Verify that concurrent executions of the job correctly log their independent entries without interference, replicating the intended behavior of separate log files in the legacy system.
*   **Setup:** Ensure `core_discount_rr_process` is configured to succeed.
*   **Action:**
    1.  Trigger two instances of the Airflow DAG `r_ausd_v_ta_p_discount_rr_dag` concurrently (e.g., by manually triggering it twice in quick succession).
    2.  Wait for both DAG runs to complete.
*   **Pass/Fail Criterion:**
    *   Both Airflow task instances must succeed.
    *   Query the `job_audit_log` table for entries created during the test window:
        ```sql
        SELECT
            job_id,
            COUNT(*) AS log_entry_count,
            ARRAY_AGG(message ORDER BY entry_number) AS messages_in_order
        FROM
            `my_gcp_project.my_bq_dataset.job_audit_log`
        WHERE
            logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE) -- Adjust interval
            AND status = 'SUCCESS'
        GROUP BY
            job_id
        HAVING
            COUNT(DISTINCT job_id) = 2; -- Expecting two distinct job_ids
        ```
    *   The query must return two distinct `job_id`s.
    *   For each `job_id`, `log_entry_count` must be `3`.
    *   For each `job_id`, `messages_in_order` must be `['Job started.', 'Calling core reconciliation logic.', 'Job completed successfully.']`.
    *   This confirms that each concurrent run is isolated and logs correctly to its own `job_id` stream within the `job_audit_log` table.

---

### 3. External-System Replacements Tests

The design document explicitly states: "No external systems (like Oracle, SFTP, S3) are directly referenced in this wrapper script". This section focuses on validating this assertion in the migrated context.

#### Test Case 3.1: Absence of External System Calls

*   **Purpose:** Confirm that the migrated BigQuery Stored Procedure does not introduce any unintended calls to external systems (Oracle, SFTP, S3, etc.) that were not present in the original wrapper script.
*   **Setup:** N/A (this is primarily a static analysis and runtime monitoring task).
*   **Action:**
    1.  **Code Review:** Manually review the BigQuery Stored Procedure `vertragsdatenabgleich` and `core_discount_rr_process` (once its full logic is known) for any BigQuery functions or statements that interact with external systems (e.g., `EXTERNAL_QUERY`, `EXPORT DATA` to GCS/S3, `LOAD DATA` from external sources, `CREATE EXTERNAL TABLE`).
    2.  **Runtime Monitoring:** During execution of the Airflow DAG, monitor GCP network logs (e.g., VPC Flow Logs, Cloud Audit Logs for BigQuery) for any outbound connections or unexpected service calls initiated by the BigQuery job.
*   **Pass/Fail Criterion:**
    *   No `EXTERNAL_QUERY` or similar functions that connect to external databases (Oracle, etc.) are found in the BigQuery SP code.
    *   No `EXPORT DATA` statements to external locations (SFTP, S3, etc.) are found in the BigQuery SP code.
    *   No unexpected outbound network traffic or external service calls are observed in GCP monitoring logs originating from the BigQuery job execution.

---

### 4. Data-Quality / Row-Count / Schema Assertions

These tests validate the structure and integrity of the `job_audit_log` table, which replaces the legacy file-based logging.

#### Test Case 4.1: `job_audit_log` Schema Validation

*   **Purpose:** Verify the `job_audit_log` table schema matches the DDL provided in the migration design and expected data types.
*   **Setup:** Ensure the `job_audit_log` table has been created using the provided DDL.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `job_audit_log` table.
    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable,
        description
    FROM
        `my_gcp_project.my_bq_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'job_audit_log'
    ORDER BY
        ordinal_position;
    ```
*   **Pass/Fail Criterion:**
    *   All expected columns must exist with the correct `data_type` and `is_nullable` properties as defined in the DDL:
        *   `job_id` (STRING, NOT NULL)
        *   `entry_number` (INT64, NOT NULL)
        *   `log_file_name` (STRING, NULLABLE)
        *   `start_timestamp` (TIMESTAMP, NULLABLE)
        *   `end_timestamp` (TIMESTAMP, NULLABLE)
        *   `status` (STRING, NULLABLE)
        *   `error_code` (STRING, NULLABLE)
        *   `error_message` (STRING, NULLABLE)
        *   `stichtag_info` (STRING, NULLABLE)
        *   `parameters` (JSON, NULLABLE)
        *   `message` (STRING, NULLABLE)
        *   `logged_at` (TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP())
    *   The table must be partitioned by `DATE(logged_at)` and clustered by `job_id, status`.

#### Test Case 4.2: `job_audit_log` Row Counts per Execution Type

*   **Purpose:** Verify the correct number of log entries are created in `job_audit_log` for different execution scenarios (successful, parameter validation failure, core logic failure).
*   **Setup:**
    *   Execute one successful run (Test Case 1.1).
    *   Execute one parameter validation failure run (Test Case 1.2).
    *   Execute one core logic failure run (Test Case 1.3).
    *   Record the `job_id` for each run.
*   **Action:** For each `job_id`, count the number of entries in `job_audit_log`.
    ```sql
    SELECT COUNT(*) FROM `my_gcp_project.my_bq_dataset.job_audit_log` WHERE job_id = '{{ JOB_ID_SUCCESS }}';
    SELECT COUNT(*) FROM `my_gcp_project.my_bq_dataset.job_audit_log` WHERE job_id = '{{ JOB_ID_PARAM_FAIL }}';
    SELECT COUNT(*) FROM `my_gcp_project.my_bq_dataset.job_audit_log` WHERE job_id = '{{ JOB_ID_CORE_FAIL }}';
    ```
*   **Pass/Fail Criterion:**
    *   For the successful run (`JOB_ID_SUCCESS`): Count must be `3`.
    *   For the parameter validation failure run (`JOB_ID_PARAM_FAIL`): Count must be `2`.
    *   For the core logic failure run (`JOB_ID_CORE_FAIL`): Count must be `3`.