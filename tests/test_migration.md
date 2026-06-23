The migration of `r_ausd_bp_ta_bpr_opt_text.ksh` to Google BigQuery involves translating shell script orchestration and parameter handling into BigQuery Stored Procedures and leveraging BigQuery tables for logging and auditing. The core data transformation logic, originally in `k_ausd_bp_ta_bpr_opt_text.ksh`, will reside in a separate BigQuery Stored Procedure.

The following tests are designed to validate the behavioral equivalence of the migrated BigQuery solution against the legacy KornShell script, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests for `r_ausd_bp_ta_bpr_opt_text.ksh`

### Test Case 1: Successful Execution with All Parameters Provided

*   **Purpose:** Verify that the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure correctly processes explicitly provided `p_input_stichtag` and `p_input_wiederanlaufwert` parameters, invokes the downstream kernel procedure, and logs a successful completion.
*   **Setup:**
    1.  Ensure the `job_control`, `job_error_log`, and `job_message_log` tables are empty.
    2.  The `k_ausd_bp_ta_bpr_opt_text` stored procedure is deployed and does not raise any errors.
*   **Action:**
    Execute the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure with a specific cutoff date and restart value.

    ```sql
    CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`(
        '01012023', -- p_input_stichtag (DDMMYYYY)
        12345       -- p_input_wiederanlaufwert
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **`job_control` table:**
        *   Exactly one row exists.
        *   `job_status` is 'OK'.
        *   `parameter_stichtag` is `DATE '2023-01-01'`.
        *   `parameter_wiederanlaufwert` is `12345`.
        *   `sys_date` matches `CURRENT_DATE()` at the time of execution.
        *   `start_time` and `end_time` are populated, with `end_time` being after `start_time`.
        *   `error_code` is `NULL` and `error_message` is `NULL`.
        *   `message` is 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'.
    2.  **`job_error_log` table:** No rows exist.
    3.  **`job_message_log` table:**
        *   Contains at least four entries for the executed `job_id`:
            *   'Job started. Stichtag: 2023-01-01, Wiederanlaufwert: 12345'
            *   'k_ausd_bp_ta_bpr_opt_text started.'
            *   'k_ausd_bp_ta_bpr_opt_text completed successfully (placeholder).'
            *   'Job completed successfully.'
        *   All messages are associated with the same `job_id` from `job_control`.

    ```sql
    -- Pytest assertion example
    def test_successful_execution_with_all_params(bigquery_client):
        # Clear logging tables
        bigquery_client.query("TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.job_control`").result()
        bigquery_client.query("TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.job_error_log`").result()
        bigquery_client.query("TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.job_message_log`").result()

        # Action: Call the SP
        bigquery_client.query("CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`('01012023', 12345)").result()

        # Assertions
        job_control_rows = list(bigquery_client.query("SELECT * FROM `your_gcp_project.your_bigquery_dataset.job_control`").result())
        assert len(job_control_rows) == 1
        assert job_control_rows[0]['job_status'] == 'OK'
        assert job_control_rows[0]['parameter_stichtag'] == date(2023, 1, 1)
        assert job_control_rows[0]['parameter_wiederanlaufwert'] == 12345
        assert job_control_rows[0]['sys_date'] == date.today() # Assuming CURRENT_DATE() is today
        assert job_control_rows[0]['error_code'] is None
        assert job_control_rows[0]['error_message'] is None
        assert job_control_rows[0]['message'] == 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'

        job_error_log_rows = list(bigquery_client.query("SELECT * FROM `your_gcp_project.your_bigquery_dataset.job_error_log`").result())
        assert len(job_error_log_rows) == 0

        job_message_log_rows = list(bigquery_client.query("SELECT message FROM `your_gcp_project.your_bigquery_dataset.job_message_log` ORDER BY log_timestamp").result())
        messages = [row['message'] for row in job_message_log_rows]
        assert 'Job started. Stichtag: 2023-01-01, Wiederanlaufwert: 12345' in messages
        assert 'k_ausd_bp_ta_bpr_opt_text started.' in messages
        assert 'k_ausd_bp_ta_bpr_opt_text completed successfully (placeholder).' in messages
        assert 'Job completed successfully.' in messages
    ```

### Test Case 2: Successful Execution with Default Parameters

*   **Purpose:** Verify that the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure correctly applies default values for `p_input_stichtag` (to `CURRENT_DATE()`) and `p_input_wiederanlaufwert` (to `0`) when they are not provided, and logs a successful completion.
*   **Setup:**
    1.  Ensure the `job_control`, `job_error_log`, and `job_message_log` tables are empty.
    2.  The `k_ausd_bp_ta_bpr_opt_text` stored procedure is deployed and does not raise any errors.
*   **Action:**
    Execute the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure without providing any parameters (or passing `NULL`).

    ```sql
    CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`(NULL, NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  **`job_control` table:**
        *   Exactly one row exists.
        *   `job_status` is 'OK'.
        *   `parameter_stichtag` is `CURRENT_DATE()`.
        *   `parameter_wiederanlaufwert` is `0`.
        *   `sys_date` matches `CURRENT_DATE()` at the time of execution.
        *   `error_code` is `NULL` and `error_message` is `NULL`.
    2.  **`job_error_log` table:** No rows exist.
    3.  **`job_message_log` table:**
        *   Contains messages indicating job start, kernel start/completion, and job completion, with `Stichtag` matching `CURRENT_DATE()` and `Wiederanlaufwert` as `0`.

    ```sql
    -- SQL Assertion
    SELECT
        COUNT(1) AS row_count,
        MAX(CASE WHEN job_status = 'OK' THEN 1 ELSE 0 END) AS is_ok_status,
        MAX(CASE WHEN parameter_stichtag = CURRENT_DATE() THEN 1 ELSE 0 END) AS stichtag_default_correct,
        MAX(CASE WHEN parameter_wiederanlaufwert = 0 THEN 1 ELSE 0 END) AS wiederanlaufwert_default_correct,
        MAX(CASE WHEN sys_date = CURRENT_DATE() THEN 1 ELSE 0 END) AS sys_date_correct,
        MAX(CASE WHEN error_code IS NULL AND error_message IS NULL THEN 1 ELSE 0 END) AS no_errors
    FROM `your_gcp_project.your_bigquery_dataset.job_control`
    WHERE job_id = (SELECT job_id FROM `your_gcp_project.your_bigquery_dataset.job_control` ORDER BY start_time DESC LIMIT 1);

    -- Expected result for the above query:
    -- row_count | is_ok_status | stichtag_default_correct | wiederanlaufwert_default_correct | sys_date_correct | no_errors
    -- ----------|--------------|--------------------------|----------------------------------|------------------|----------
    -- 1         | 1            | 1                        | 1                                | 1                | 1

    SELECT
        COUNTIF(message LIKE 'Job started. Stichtag: %' AND message LIKE '%Wiederanlaufwert: 0') AS start_msg_correct,
        COUNTIF(message = 'k_ausd_bp_ta_bpr_opt_text started.') AS kernel_start_msg,
        COUNTIF(message = 'k_ausd_bp_ta_bpr_opt_text completed successfully (placeholder).') AS kernel_complete_msg,
        COUNTIF(message = 'Job completed successfully.') AS complete_msg
    FROM `your_gcp_project.your_bigquery_dataset.job_message_log`
    WHERE job_id = (SELECT job_id FROM `your_gcp_project.your_bigquery_dataset.job_control` ORDER BY start_time DESC LIMIT 1);

    -- Expected result for the above query:
    -- start_msg_correct | kernel_start_msg | kernel_complete_msg | complete_msg
    -- -------------------|------------------|---------------------|-------------
    -- 1                 | 1                | 1                   | 1
    ```

### Test Case 3: Invalid Stichtag Format

*   **Purpose:** Verify that the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure correctly identifies and handles an invalid `p_input_stichtag` format, logs the error, and signals an error to the caller. This corresponds to the legacy script's parameter validation and error handling.
*   **Setup:**
    1.  Ensure the `job_control`, `job_error_log`, and `job_message_log` tables are empty.
*   **Action:**
    Execute the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure with a `p_input_stichtag` in an incorrect format (e.g., 'YYYY-MM-DD' instead of 'DDMMYYYY').

    ```sql
    -- This call is expected to fail and raise an error
    CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`(
        '2023-01-01', -- Invalid format
        100
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement must raise an error (e.g., `SQLSTATE '45000'`) with a message indicating invalid date format.
    2.  **`job_control` table:**
        *   Exactly one row exists.
        *   `job_status` is 'ERROR'.
        *   `error_code` is `193` (as per design, corresponding to 'Notwendiges Argument fehlt' or invalid format).
        *   `error_message` contains 'Invalid Stichtag format. Expected DDMMYYYY.'.
        *   `end_time` is populated.
    3.  **`job_error_log` table:**
        *   Exactly one row exists for the `job_id`.
        *   `error_code` is `193`.
        *   `error_message` contains 'Invalid Stichtag format. Expected DDMMYYYY.'.
        *   `script_name` is 'ausd_bp_ta_bpr_opt_text_wrapper'.
    4.  **`job_message_log` table:**
        *   Contains 'Job started' and 'Job failed: Invalid Stichtag format. Expected DDMMYYYY.' messages for the `job_id`.

    ```sql
    -- Pytest assertion example
    import pytest
    from datetime import date

    def test_invalid_stichtag_format(bigquery_client):
        # Clear logging tables
        bigquery_client.query("TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.job_control`").result()
        bigquery_client.query("TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.job_error_log`").result()
        bigquery_client.query("TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.job_message_log`").result()

        # Action: Call the SP, expecting an error
        with pytest.raises(Exception) as excinfo:
            bigquery_client.query("CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`('2023-01-01', 100)").result()
        assert "Invalid Stichtag format. Expected DDMMYYYY." in str(excinfo.value)

        # Assertions on logging tables
        job_control_rows = list(bigquery_client.query("SELECT * FROM `your_gcp_project.your_bigquery_dataset.job_control`").result())
        assert len(job_control_rows) == 1
        assert job_control_rows[0]['job_status'] == 'ERROR'
        assert job_control_rows[0]['error_code'] == 193
        assert 'Invalid Stichtag format. Expected DDMMYYYY.' in job_control_rows[0]['error_message']

        job_error_log_rows = list(bigquery_client.query("SELECT * FROM `your_gcp_project.your_bigquery_dataset.job_error_log`").result())
        assert len(job_error_log_rows) == 1
        assert job_error_log_rows[0]['error_code'] == 193
        assert 'Invalid Stichtag format. Expected DDMMYYYY.' in job_error_log_rows[0]['error_message']
        assert job_error_log_rows[0]['script_name'] == 'ausd_bp_ta_bpr_opt_text_wrapper'

        job_message_log_rows = list(bigquery_client.query("SELECT message FROM `your_gcp_project.your_bigquery_dataset.job_message_log` ORDER BY log_timestamp").result())
        messages = [row['message'] for row in job_message_log_rows]
        assert 'Job started.' in messages[0] # The exact stichtag might not be parsed yet
        assert 'Job failed: Invalid Stichtag format. Expected DDMMYYYY.' in messages
    ```

### Test Case 4: Downstream Kernel Script Failure

*   **Purpose:** Verify that the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure correctly catches and logs errors originating from the downstream `k_ausd_bp_ta_bpr_opt_text` procedure, updates the job status to 'ERROR', and re-raises the error. This mimics the `trap ERR` behavior of the legacy script.
*   **Setup:**
    1.  Temporarily modify the `k_ausd_bp_ta_bpr_opt_text` stored procedure to intentionally raise an error (e.g., `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel error';`).
    2.  Ensure the `job_control`, `job_error_log`, and `job_message_log` tables are empty.
*   **Action:**
    Execute the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure with valid parameters.

    ```sql
    -- Modify k_ausd_bp_ta_bpr_opt_text first:
    CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bigquery_dataset.k_ausd_bp_ta_bpr_opt_text`(
        p_job_id STRING, p_job_kennung STRING, p_stichtag DATE, p_wiederanlaufwert INT64
    )
    BEGIN
        INSERT INTO `your_gcp_project.your_bigquery_dataset.job_message_log` (log_timestamp, job_id, message_type, message, script_name)
        VALUES (CURRENT_TIMESTAMP(), p_job_id, 'INFO', 'k_ausd_bp_ta_bpr_opt_text started.', 'k_ausd_bp_ta_bpr_opt_text');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel error'; -- INTENTIONAL ERROR
    END;

    -- Then call the wrapper (expected to fail)
    CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`('01012023', 12345);
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement must raise an error (e.g., `SQLSTATE '45000'`) with the message 'Simulated kernel error'.
    2.  **`job_control` table:**
        *   Exactly one row exists.
        *   `job_status` is 'ERROR'.
        *   `error_code` is `1` (generic error code for BigQuery, or a specific one if mapped).
        *   `error_message` contains 'Simulated kernel error'.
        *   `end_time` is populated.
    3.  **`job_error_log` table:**
        *   Exactly one row exists for the `job_id`.
        *   `error_message` contains 'Simulated kernel error'.
        *   `script_name` is 'ausd_bp_ta_bpr_opt_text_wrapper' (as the error is caught and logged by the wrapper).
    4.  **`job_message_log` table:**
        *   Contains 'Job started', 'k_ausd_bp_ta_bpr_opt_text started.', and 'Job failed: Simulated kernel error' messages for the `job_id`.

    *(Remember to revert `k_ausd_bp_ta_bpr_opt_text` to its original placeholder state after this test.)*

### Test Case 5: Data Quality - Logging Table Schemas

*   **Purpose:** Verify that the `job_control`, `job_error_log`, and `job_message_log` tables have the correct schema, column names, and data types as defined in the DDL, ensuring data integrity for auditing and logging.
*   **Setup:** The logging tables (`job_control`, `job_error_log`, `job_message_log`) have been created using the provided DDL.
*   **Action:**
    Query the BigQuery `INFORMATION_SCHEMA` to retrieve the schema details for each table.

    ```sql
    SELECT column_name, data_type, is_nullable
    FROM `your_gcp_project.your_bigquery_dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'job_control'
    ORDER BY ordinal_position;

    SELECT column_name, data_type, is_nullable
    FROM `your_gcp_project.your_bigquery_dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'job_error_log'
    ORDER BY ordinal_position;

    SELECT column_name, data_type, is_nullable
    FROM `your_gcp_project.your_bigquery_dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'job_message_log'
    ORDER BY ordinal_position;
    ```
*   **Pass/Fail Criterion:**
    The query results for each table must exactly match the expected schema and data types from the DDL:

    **`job_control` Expected Schema:**
    | column_name          | data_type | is_nullable |
    | :------------------- | :-------- | :---------- |
    | job_id               | STRING    | NO          |
    | job_name             | STRING    | YES         |
    | job_status           | STRING    | YES         |
    | start_time           | TIMESTAMP | YES         |
    | end_time             | TIMESTAMP | YES         |
    | parameter_stichtag   | DATE      | YES         |
    | parameter_wiederanlaufwert | INT64     | YES         |
    | log_file_path        | STRING    | YES         |
    | sys_date             | DATE      | YES         |
    | error_code           | INT64     | YES         |
    | error_message        | STRING    | YES         |
    | message              | STRING    | YES         |

    **`job_error_log` Expected Schema:**
    | column_name     | data_type | is_nullable |
    | :-------------- | :-------- | :---------- |
    | log_timestamp   | TIMESTAMP | NO          |
    | job_id          | STRING    | NO          |
    | error_code      | INT64     | YES         |
    | error_argument  | STRING    | YES         |
    | error_message   | STRING    | YES         |
    | script_name     | STRING    | YES         |

    **`job_message_log` Expected Schema:**
    | column_name     | data_type | is_nullable |
    | :-------------- | :-------- | :---------- |
    | log_timestamp   | TIMESTAMP | NO          |
    | job_id          | STRING    | NO          |
    | message_type    | STRING    | YES         |
    | message         | STRING    | YES         |
    | script_name     | STRING    | YES         |

### Test Case 6: External System Replacement - Log File Path Generation

*   **Purpose:** Verify that the `log_file_path` generated and stored in the `job_control` table adheres to the expected BigQuery environment's logging strategy (e.g., a GCS path) and incorporates relevant job metadata. This replaces the dynamic log file creation of the legacy shell script.
*   **Setup:**
    1.  Ensure the `job_control` table is empty.
    2.  The `k_ausd_bp_ta_bpr_opt_text` stored procedure is deployed and does not raise any errors.
*   **Action:**
    Execute the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure with any valid parameters.

    ```sql
    CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`('01012023', 12345);
    ```
*   **Pass/Fail Criterion:**
    1.  **`job_control` table:**
        *   Exactly one row exists with `job_status = 'OK'`.
        *   The `log_file_path` column contains a string that starts with `gs://your-log-bucket/ausd_bp_ta_bpr_opt_text_`.
        *   The `log_file_path` string ends with a `.log` extension.
        *   The `job_id` (a UUID) from the `job_control` table is present within the `log_file_path` string.
        *   The timestamp part of the `log_file_path` (between `ausd_bp_ta_bpr_opt_text_` and `_` + `job_id`) is a valid representation of the `start_time` (e.g., `YYYY-MM-DD_HH:MM:SS.microseconds`).

    ```sql
    -- SQL Assertion
    SELECT
        job_id,
        log_file_path,
        start_time,
        STARTS_WITH(log_file_path, 'gs://your-log-bucket/ausd_bp_ta_bpr_opt_text_') AS starts_with_prefix,
        ENDS_WITH(log_file_path, '.log') AS ends_with_suffix,
        CONTAINS_SUBSTR(log_file_path, job_id) AS contains_job_id,
        -- Attempt to parse the timestamp part and compare with start_time
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d_%H:%M:%E*S', SUBSTR(log_file_path, LENGTH('gs://your-log-bucket/ausd_bp_ta_bpr_opt_text_') + 1, LENGTH(log_file_path) - LENGTH('gs://your-log-bucket/ausd_bp_ta_bpr_opt_text_') - LENGTH(job_id) - 5)) AS parsed_timestamp_from_path,
        start_time
    FROM `your_gcp_project.your_bigquery_dataset.job_control`
    WHERE job_id = (SELECT job_id FROM `your_gcp_project.your_bigquery_dataset.job_control` ORDER BY start_time DESC LIMIT 1);

    -- Expected result: starts_with_prefix, ends_with_suffix, contains_job_id should all be TRUE.
    -- parsed_timestamp_from_path should be very close to start_time (allowing for minor formatting differences).
    ```

### Test Case 7: Data Transformation - Core Logic in `k_ausd_bp_ta_bpr_opt_text` (Conceptual)

*   **Purpose:** Verify that the core data filtering and selection logic within `k_ausd_bp_ta_bpr_opt_text` (once fully implemented) produces the expected output based on the `p_stichtag` and `p_wiederanlaufwert` parameters, matching the described legacy behavior. This test case outlines the requirements for the kernel procedure's implementation.
*   **Setup:**
    1.  A mock source table (e.g., `your_gcp_project.your_bigquery_dataset.your_source_table`) is created and populated with diverse test data, including various combinations of `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID`.
    2.  A mock target table (e.g., `your_gcp_project.your_bigquery_dataset.your_target_table`) is created to receive the processed data.
    3.  The `k_ausd_bp_ta_bpr_opt_text` stored procedure is fully implemented to perform the described filtering and insertion into `your_target_table`.
*   **Action:**
    Execute the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure with specific `p_input_stichtag` and `p_input_wiederanlaufwert` values.

    ```sql
    -- Example: Populate mock source data
    CREATE OR REPLACE TABLE `your_gcp_project.your_bigquery_dataset.your_source_table` AS
    SELECT * FROM UNNEST([
        STRUCT(1001 AS DWH_VERTRAG_ID, DATE '2022-01-01' AS Gueltig_von, DATE '2023-01-15' AS Gueltig_bis, DATE '2022-12-01' AS LADEDATUM, 'Data1' AS data), -- Should be selected (stichtag=2023-01-01, wiederanlauf=0)
        STRUCT(1002 AS DWH_VERTRAG_ID, DATE '2022-06-01' AS Gueltig_von, DATE '2023-06-01' AS Gueltig_bis, DATE '2022-12-15' AS LADEDATUM, 'Data2' AS data), -- Should be selected
        STRUCT(1003 AS DWH_VERTRAG_ID, DATE '2023-01-01' AS Gueltig_von, DATE '2023-02-01' AS Gueltig_bis, DATE '2022-12-20' AS LADEDATUM, 'Data3' AS data), -- Should be selected
        STRUCT(1004 AS DWH_VERTRAG_ID, DATE '2022-01-01' AS Gueltig_von, DATE '2022-12-31' AS Gueltig_bis, DATE '2022-11-01' AS LADEDATUM, 'Data4' AS data), -- Not selected (Gueltig_bis <= Stichtag)
        STRUCT(1005 AS DWH_VERTRAG_ID, DATE '2023-01-02' AS Gueltig_von, DATE '2023-02-01' AS Gueltig_bis, DATE '2022-12-25' AS LADEDATUM, 'Data5' AS data), -- Not selected (Gueltig_von > Stichtag)
        STRUCT(1006 AS DWH_VERTRAG_ID, DATE '2022-01-01' AS Gueltig_von, DATE '2023-01-15' AS Gueltig_bis, DATE '2023-01-01' AS LADEDATUM, 'Data6' AS data), -- Not selected (LADEDATUM >= Stichtag)
        STRUCT(1007 AS DWH_VERTRAG_ID, DATE '2022-01-01' AS Gueltig_von, DATE '2023-01-15' AS Gueltig_bis, DATE '2022-12-01' AS LADEDATUM, 'Data7' AS data)  -- Not selected (DWH_VERTRAG_ID <= wiederanlaufwert=1000)
    ]) AS t;

    -- Clear target table
    TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.your_target_table`;

    -- Call the wrapper, which in turn calls the kernel
    CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`(
        '01012023', -- p_input_stichtag = 2023-01-01
        1000        -- p_input_wiederanlaufwert = 1000
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The `your_target_table` contains only the rows that satisfy all filtering conditions:
        *   `Gueltig_von <= p_stichtag`
        *   `Gueltig_bis > p_stichtag`
        *   `LADEDATUM < p_stichtag`
        *   `DWH_VERTRAG_ID > p_wiederanlaufwert`
    2.  The row count in `your_target_table` matches the expected count based on the mock source data and the applied filtering rules. For the example data above, with `p_stichtag = '2023-01-01'` and `p_wiederanlaufwert = 1000`, the expected rows are:
        *   `DWH_VERTRAG_ID = 1001`
        *   `DWH_VERTRAG_ID = 1002`
        *   `DWH_VERTRAG_ID = 1003`
        (Total 3 rows)

    ```sql
    -- SQL Assertion
    SELECT
        DWH_VERTRAG_ID,
        Gueltig_von,
        Gueltig_bis,
        LADEDATUM
    FROM `your_gcp_project.your_bigquery_dataset.your_target_table`
    ORDER BY DWH_VERTRAG_ID;

    -- Expected result for the example:
    -- DWH_VERTRAG_ID | Gueltig_von | Gueltig_bis | LADEDATUM
    -- ---------------|-------------|-------------|------------
    -- 1001           | 2022-01-01  | 2023-01-15  | 2022-12-01
    -- 1002           | 2022-06-01  | 2023-06-01  | 2022-12-15
    -- 1003           | 2023-01-01  | 2023-02-01  | 2022-12-20

    SELECT COUNT(1) FROM `your_gcp_project.your_bigquery_dataset.your_target_table`;
    -- Expected: 3
    ```