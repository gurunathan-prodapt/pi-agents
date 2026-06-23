As a senior data-migration QA engineer, I've designed a suite of validation tests for the `r_ausd_v_ta_bp_ref.ksh` KornShell script's migration to a BigQuery Stored Procedure. These tests focus on ensuring behavioral equivalence, covering output parity, transformation correctness, external system interactions, and data quality.

**Pre-requisites for all tests:**

Before running any tests, ensure the following BigQuery objects are created and configured:

1.  **BigQuery Project and Dataset:** Replace `your_project_id.your_dataset_id` with your actual project and dataset.
2.  **`job_audit_log` Table:** Execute the provided DDL for the `job_audit_log` table.
    ```sql
    -- FILE: sql/ddl/job_audit_log.sql
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_audit_log` (
        job_instance_id STRING NOT NULL OPTIONS(description="Unique identifier for each job execution instance (corresponds to DW_EintragsNr)"),
        job_name STRING OPTIONS(description="Name of the job (e.g., 'Vertragsdatenabgleich', from ProgName)"),
        job_kennung STRING OPTIONS(description="Kennung for the job (e.g., 'BERT_V_TA_BP_REF', from JobKennung)"),
        start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job instance started"),
        end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job instance ended"),
        status STRING OPTIONS(description="Current status of the job instance (e.g., 'STARTED', 'RUNNING', 'SUCCESS', 'FAILED')"),
        message_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp for the individual log message"),
        message_type STRING NOT NULL OPTIONS(description="Type of log message (e.g., 'INFO', 'ERROR', 'USAGE', 'WARNING')"),
        message_text STRING OPTIONS(description="Content of the log message"),
        error_code INT64 OPTIONS(description="Numeric error code if an error occurred (corresponds to ErrNr)"),
        error_argument STRING OPTIONS(description="Argument associated with the error (corresponds to ErrArg)"),
        parameters_s STRING OPTIONS(description="Value of the -s parameter passed to the job"),
        parameters_l STRING OPTIONS(description="Value of the -l parameter passed to the job"),
        stichtag_info STRING OPTIONS(description="Date information in DDMMYYYY format (corresponds to v_sysdate)"),
        log_file_name STRING OPTIONS(description="Simulated log file name for reference")
    )
    OPTIONS(
        description="Table to store audit and logging information for BigQuery job executions."
    );
    ```
3.  **Dummy Core Stored Procedures:** Create these to simulate the behavior of `k_ausd_v_ta_bp_ref`.
    *   `k_ausd_v_ta_bp_ref_success`: Simulates a successful core script execution.
        ```sql
        CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.k_ausd_v_ta_bp_ref_success`(
            IN p_job_kennung STRING,
            IN p_job_instance_id STRING
        )
        BEGIN
            INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
                job_instance_id, message_timestamp, message_type, message_text
            )
            VALUES (
                p_job_instance_id, CURRENT_TIMESTAMP(), 'INFO',
                FORMAT('Core script k_ausd_v_ta_bp_ref_success called with JobKennung: %s, JobInstanceId: %s', p_job_kennung, p_job_instance_id)
            );
            -- Simulate some work
            SELECT 'Core script executed successfully' AS message;
        END;
        ```
    *   `k_ausd_v_ta_bp_ref_failure`: Simulates a core script execution that raises an error.
        ```sql
        CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.k_ausd_v_ta_bp_ref_failure`(
            IN p_job_kennung STRING,
            IN p_job_instance_id STRING
        )
        BEGIN
            INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
                job_instance_id, message_timestamp, message_type, message_text
            )
            VALUES (
                p_job_instance_id, CURRENT_TIMESTAMP(), 'ERROR',
                FORMAT('Core script k_ausd_v_ta_bp_ref_failure called and intentionally failed with JobKennung: %s, JobInstanceId: %s', p_job_kennung, p_job_instance_id)
            );
            RAISE USING MESSAGE 'Simulated error in k_ausd_v_ta_bp_ref_failure';
        END;
        ```
4.  **Modified `vertragsdatenabgleich_wrapper` Stored Procedure (for testing):** The provided migration code is slightly modified to accept `p_core_proc_name` as a parameter, allowing us to dynamically call either the success or failure dummy core procedure for testing.
    ```sql
    -- FILE: sql/stored_procedures/vertragsdatenabgleich_wrapper.sql (Modified for testing)
    CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(
        IN p_s STRING,
        IN p_l STRING,
        IN p_help BOOL,
        IN p_core_proc_name STRING -- Added for testing core script invocation
    )
    OPTIONS(description="Wrapper procedure for contract data reconciliation (ta_bp_ref). Orchestrates the call to the core k_ausd_v_ta_bp_ref procedure.")
    BEGIN
        DECLARE v_ProgName STRING DEFAULT 'Vertragsdatenabgleich';
        DECLARE v_ProgVersion STRING DEFAULT 'V1.0.0';
        DECLARE v_JobKennung STRING DEFAULT 'BERT_V_TA_BP_REF';
        DECLARE v_sysdate STRING;
        DECLARE v_job_instance_id STRING;
        DECLARE v_start_timestamp TIMESTAMP;

        SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
        SET v_start_timestamp = CURRENT_TIMESTAMP();
        SET v_job_instance_id = GENERATE_UUID();

        IF p_help THEN
            SELECT FORMAT(
                """
                Programm: %s
                Version:  %s
                Aufruf:   CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(p_s => '[value]', p_l => '[value]', p_help => [TRUE/FALSE], p_core_proc_name => '[core_proc_name]')
                Parameter:
                    p_s    : This parameter corresponds to the original -s option. Its specific use is determined by the core script.
                    p_l    : This parameter corresponds to the original -l option. Its specific use is determined by the core script.
                    p_help : Set to TRUE to display this usage message.
                    p_core_proc_name : (For testing) Name of the core procedure to call (e.g., 'k_ausd_v_ta_bp_ref_success').

                Beschreibung:
                    Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_bp_ref.
                """,
                v_ProgName,
                v_ProgVersion
            );
            INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
                job_instance_id, job_name, job_kennung, start_timestamp, status,
                message_timestamp, message_type, message_text,
                parameters_s, parameters_l
            )
            VALUES (
                v_job_instance_id, v_ProgName, v_JobKennung, v_start_timestamp, 'COMPLETED',
                CURRENT_TIMESTAMP(), 'USAGE', 'Usage information displayed.',
                p_s, p_l
            );
            RETURN;
        END IF;

        INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
            job_instance_id, job_name, job_kennung, start_timestamp, status,
            message_timestamp, message_type, message_text,
            parameters_s, parameters_l, stichtag_info, log_file_name
        )
        VALUES (
            v_job_instance_id, v_ProgName, v_JobKennung, v_start_timestamp, 'STARTED',
            CURRENT_TIMESTAMP(), 'INFO', 'Job execution started.',
            p_s, p_l, v_sysdate, v_job_instance_id || '.log'
        );

        BEGIN
            INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
                job_instance_id, message_timestamp, message_type, message_text
            )
            VALUES (
                v_job_instance_id, CURRENT_TIMESTAMP(), 'INFO',
                FORMAT('Job-Nr: %s, JobKennung: %s, Logdatei: %s.log', v_job_instance_id, v_JobKennung, v_job_instance_id)
            );

            EXECUTE IMMEDIATE FORMAT(
                'CALL `your_project_id.your_dataset_id.%s`(@job_kennung, @job_instance_id)',
                p_core_proc_name
            ) USING v_JobKennung AS job_kennung, v_job_instance_id AS job_instance_id;

            INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
                job_instance_id, message_timestamp, message_type, message_text
            )
            VALUES (
                v_job_instance_id, CURRENT_TIMESTAMP(), 'INFO', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
            );

            UPDATE `your_project_id.your_dataset_id.job_audit_log`
            SET status = 'SUCCESS', end_timestamp = CURRENT_TIMESTAMP()
            WHERE job_instance_id = v_job_instance_id AND status = 'STARTED';

        EXCEPTION WHEN ERROR THEN
            DECLARE v_error_code INT64 DEFAULT IFNULL(@@error.code, -1);
            DECLARE v_error_message STRING DEFAULT IFNULL(@@error.message, 'Unknown error');

            INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
                job_instance_id, message_timestamp, message_type, message_text,
                error_code, error_argument
            )
            VALUES (
                v_job_instance_id, CURRENT_TIMESTAMP(), 'ERROR',
                FORMAT('Job execution failed: %s', v_error_message),
                v_error_code, v_error_message
            );

            UPDATE `your_project_id.your_dataset_id.job_audit_log`
            SET status = 'FAILED', end_timestamp = CURRENT_TIMESTAMP()
            WHERE job_instance_id = v_job_instance_id AND status = 'STARTED';

            RAISE;
        END;
    END;
    ```

---

## Test Case 1: Successful Execution - No Parameters

**Purpose:** Verify that the migrated wrapper job executes successfully without optional parameters, correctly orchestrates the core script, and logs the entire process as successful. This covers output parity and transformation correctness for the happy path.

**Setup:**
1.  Ensure the `job_audit_log` table is empty or truncated.
2.  Ensure `k_ausd_v_ta_bp_ref_success` and the modified `vertragsdatenabgleich_wrapper` procedures are deployed.

**Action:**
Execute the BigQuery Stored Procedure with default/NULL parameters for `p_s`, `p_l`, and `p_help`, and specify the success dummy core procedure.

```sql
CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(
    p_s => NULL,
    p_l => NULL,
    p_help => FALSE,
    p_core_proc_name => 'k_ausd_v_ta_bp_ref_success'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without raising an error.
2.  Query the `job_audit_log` table for the most recent `job_instance_id`.
    ```sql
    SELECT *
    FROM `your_project_id.your_dataset_id.job_audit_log`
    ORDER BY message_timestamp DESC
    LIMIT 10;
    ```
3.  **Assertions:**
    *   There should be at least 4 entries for the `job_instance_id`:
        *   One `message_type = 'INFO'` with `message_text = 'Job execution started.'` and `status = 'STARTED'`.
        *   One `message_type = 'INFO'` containing "Core script k_ausd_v_ta_bp_ref_success called...".
        *   One `message_type = 'INFO'` with `message_text = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'`.
        *   The initial `status = 'STARTED'` entry should be updated to `status = 'SUCCESS'` and `end_timestamp` should be populated.
    *   `job_name` should be 'Vertragsdatenabgleich'.
    *   `job_kennung` should be 'BERT_V_TA_BP_REF'.
    *   `stichtag_info` should be today's date in `DDMMYYYY` format.
    *   `parameters_s` and `parameters_l` should be `NULL`.
    *   `error_code` and `error_argument` should be `NULL` for all entries.

---

## Test Case 2: Successful Execution - With Parameters

**Purpose:** Verify that the migrated wrapper job correctly passes optional parameters (`-s`, `-l`) to the core script and logs them appropriately. This covers output parity and transformation correctness for parameter handling.

**Setup:**
1.  Ensure the `job_audit_log` table is empty or truncated.
2.  Ensure `k_ausd_v_ta_bp_ref_success` and the modified `vertragsdatenabgleich_wrapper` procedures are deployed.

**Action:**
Execute the BigQuery Stored Procedure with specific values for `p_s` and `p_l`.

```sql
CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(
    p_s => 'test_s_value',
    p_l => 'test_l_value',
    p_help => FALSE,
    p_core_proc_name => 'k_ausd_v_ta_bp_ref_success'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without raising an error.
2.  Query the `job_audit_log` table for the most recent `job_instance_id`.
    ```sql
    SELECT *
    FROM `your_project_id.your_dataset_id.job_audit_log`
    ORDER BY message_timestamp DESC
    LIMIT 10;
    ```
3.  **Assertions:**
    *   All assertions from Test Case 1 regarding success status and message types apply.
    *   `parameters_s` should be 'test_s_value'.
    *   `parameters_l` should be 'test_l_value'.
    *   The log entry for the core script call should reflect these parameters being passed (implicitly, as the dummy core script logs its invocation parameters).

---

## Test Case 3: Error Handling - Core Script Failure

**Purpose:** Verify that the wrapper correctly handles errors originating from the invoked core script, logs the error details, updates the job status to 'FAILED', and re-raises the error. This covers transformation correctness for error handling and external system replacement (core script interaction).

**Setup:**
1.  Ensure the `job_audit_log` table is empty or truncated.
2.  Ensure `k_ausd_v_ta_bp_ref_failure` and the modified `vertragsdatenabgleich_wrapper` procedures are deployed.

**Action:**
Execute the BigQuery Stored Procedure, specifying the failure dummy core procedure. This call is expected to fail.

```sql
-- This call is expected to fail and raise an error.
-- In a test framework (e.g., pytest), you would assert that an exception is raised.
CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(
    p_s => NULL,
    p_l => NULL,
    p_help => FALSE,
    p_core_proc_name => 'k_ausd_v_ta_bp_ref_failure'
);
```

**Pass/Fail Criterion:**
1.  The procedure call **must** raise an error (e.g., `Simulated error in k_ausd_v_ta_bp_ref_failure`).
2.  Query the `job_audit_log` table for the most recent `job_instance_id`.
    ```sql
    SELECT *
    FROM `your_project_id.your_dataset_id.job_audit_log`
    ORDER BY message_timestamp DESC
    LIMIT 10;
    ```
3.  **Assertions:**
    *   There should be at least 3 entries for the `job_instance_id`:
        *   One `message_type = 'INFO'` with `message_text = 'Job execution started.'` and `status = 'STARTED'`.
        *   One `message_type = 'ERROR'` containing "Core script k_ausd_v_ta_bp_ref_failure called and intentionally failed...".
        *   One `message_type = 'ERROR'` with `message_text` indicating "Job execution failed: Simulated error in k_ausd_v_ta_bp_ref_failure". This entry should have `error_code` and `error_argument` populated (e.g., `error_argument` containing the error message).
        *   The initial `status = 'STARTED'` entry should be updated to `status = 'FAILED'` and `end_timestamp` should be populated.
    *   `job_name` should be 'Vertragsdatenabgleich'.
    *   `job_kennung` should be 'BERT_V_TA_BP_REF'.
    *   `stichtag_info` should be today's date in `DDMMYYYY` format.
    *   `parameters_s` and `parameters_l` should be `NULL`.

---

## Test Case 4: Usage Information (`p_help = TRUE`)

**Purpose:** Verify that passing `p_help = TRUE` (equivalent to `-h` in the original script) displays the usage information and logs this action, then exits without invoking the core script. This covers output parity and transformation correctness for parameter handling.

**Setup:**
1.  Ensure the `job_audit_log` table is empty or truncated.
2.  Ensure the modified `vertragsdatenabgleich_wrapper` procedure is deployed.

**Action:**
Execute the BigQuery Stored Procedure with `p_help = TRUE`.

```sql
-- This call is expected to return a result set with usage information.
CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(
    p_s => 'any_s',
    p_l => 'any_l',
    p_help => TRUE,
    p_core_proc_name => 'k_ausd_v_ta_bp_ref_success' -- Core proc name should not matter here
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without raising an error.
2.  The procedure should return a result set containing the formatted usage message.
    *   The message should include "Programm: Vertragsdatenabgleich", "Version: V1.0.0", and the parameter descriptions.
3.  Query the `job_audit_log` table for the most recent `job_instance_id`.
    ```sql
    SELECT *
    FROM `your_project_id.your_dataset_id.job_audit_log`
    ORDER BY message_timestamp DESC
    LIMIT 10;
    ```
4.  **Assertions:**
    *   There should be exactly one entry for the `job_instance_id`.
    *   `message_type` should be 'USAGE'.
    *   `message_text` should be 'Usage information displayed.'.
    *   `status` should be 'COMPLETED'.
    *   `job_name` should be 'Vertragsdatenabgleich'.
    *   `job_kennung` should be 'BERT_V_TA_BP_REF'.
    *   `start_timestamp` and `end_timestamp` should be populated.
    *   `parameters_s` should be 'any_s' and `parameters_l` should be 'any_l' (reflecting the passed parameters, even though they are not used).
    *   There should be **no** log entries indicating the core script was called.

---

## Test Case 5: Data Quality and Schema Assertions for `job_audit_log`

**Purpose:** Verify the schema, data types, and integrity of the `job_audit_log` table, ensuring it accurately captures job metadata and log entries as specified in the design. This covers data quality and schema assertions.

**Setup:**
1.  Run Test Case 2 (Successful Execution - With Parameters) to populate the `job_audit_log` table with a successful job run.

**Action:**
Execute SQL queries to inspect the schema and data within `job_audit_log`.

```sql
-- 1. Check table schema and column types
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'job_audit_log'
ORDER BY
    ordinal_position;

-- 2. Check data integrity for a recent job instance
SELECT
    job_instance_id,
    COUNT(*) AS num_entries,
    MIN(start_timestamp) AS first_start_ts,
    MAX(end_timestamp) AS last_end_ts,
    COUNTIF(status = 'STARTED') AS started_count,
    COUNTIF(status = 'SUCCESS') AS success_count,
    COUNTIF(status = 'FAILED') AS failed_count,
    COUNTIF(status = 'COMPLETED') AS completed_count,
    COUNTIF(message_type = 'INFO') AS info_count,
    COUNTIF(message_type = 'ERROR') AS error_count,
    COUNTIF(message_type = 'USAGE') AS usage_count,
    COUNTIF(job_name IS NULL) AS null_job_name,
    COUNTIF(job_kennung IS NULL) AS null_job_kennung,
    COUNTIF(message_text IS NULL) AS null_message_text,
    COUNTIF(stichtag_info IS NULL) AS null_stichtag_info,
    COUNTIF(log_file_name IS NULL) AS null_log_file_name,
    COUNTIF(LENGTH(stichtag_info) != 8) AS invalid_stichtag_format
FROM
    `your_project_id.your_dataset_id.job_audit_log`
WHERE
    job_instance_id = (SELECT job_instance_id FROM `your_project_id.your_dataset_id.job_audit_log` ORDER BY start_timestamp DESC LIMIT 1)
GROUP BY
    job_instance_id;
```

**Pass/Fail Criterion:**
1.  **Schema Check:**
    *   `job_instance_id`: `STRING`, `NOT NULL`
    *   `job_name`: `STRING`, `NULLABLE`
    *   `job_kennung`: `STRING`, `NULLABLE`
    *   `start_timestamp`: `TIMESTAMP`, `NULLABLE`
    *   `end_timestamp`: `TIMESTAMP`, `NULLABLE`
    *   `status`: `STRING`, `NULLABLE`
    *   `message_timestamp`: `TIMESTAMP`, `NOT NULL`
    *   `message_type`: `STRING`, `NOT NULL`
    *   `message_text`: `STRING`, `NULLABLE`
    *   `error_code`: `INT64`, `NULLABLE`
    *   `error_argument`: `STRING`, `NULLABLE`
    *   `parameters_s`: `STRING`, `NULLABLE`
    *   `parameters_l`: `STRING`, `NULLABLE`
    *   `stichtag_info`: `STRING`, `NULLABLE`
    *   `log_file_name`: `STRING`, `NULLABLE`
2.  **Data Integrity Check (for a successful run from Test Case 2):**
    *   `num_entries` should be >= 4.
    *   `first_start_ts` and `last_end_ts` should be populated and `last_end_ts` should be after `first_start_ts`.
    *   `started_count` should be 1.
    *   `success_count` should be 1.
    *   `failed_count` should be 0.
    *   `completed_count` should be 0 (for a successful run, the final status is 'SUCCESS', not 'COMPLETED' which is used for 'USAGE').
    *   `info_count` should be >= 3.
    *   `error_count` should be 0.
    *   `usage_count` should be 0.
    *   `null_job_name`, `null_job_kennung`, `null_message_text`, `null_stichtag_info`, `null_log_file_name` should all be 0 for relevant entries.
    *   `invalid_stichtag_format` should be 0.
    *   `job_instance_id` values should be unique across different job runs.

---

## Test Case 6: `stichtag_info` Date Formatting

**Purpose:** Verify that the `stichtag_info` field in the `job_audit_log` correctly captures the current date in `DDMMYYYY` format, as per the original script's `date +%d%m%Y`. This covers transformation correctness for date handling.

**Setup:**
1.  Ensure the `job_audit_log` table is empty or truncated.
2.  Run Test Case 1 (Successful Execution - No Parameters) to populate the `job_audit_log`.

**Action:**
Query the `job_audit_log` for the `stichtag_info` of the most recent job run.

```sql
SELECT
    stichtag_info,
    CURRENT_DATE() AS current_system_date,
    FORMAT_DATE('%d%m%Y', CURRENT_DATE()) AS expected_format
FROM
    `your_project_id.your_dataset_id.job_audit_log`
WHERE
    job_instance_id = (SELECT job_instance_id FROM `your_project_id.your_dataset_id.job_audit_log` ORDER BY start_timestamp DESC LIMIT 1)
    AND message_type = 'INFO'
    AND message_text = 'Job execution started.'
LIMIT 1;
```

**Pass/Fail Criterion:**
1.  The `stichtag_info` value must match the `expected_format` (today's date in `DDMMYYYY`).
2.  The length of `stichtag_info` must be 8 characters.

---