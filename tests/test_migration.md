As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `r_ausd_bp_ta_apn_carmen.ksh` to `sp_ausd_bp_ta_apn_carmen` in BigQuery. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

**Assumptions for Testing:**

*   `your_project_id.your_dataset_id` will be replaced with the actual BigQuery project and dataset IDs.
*   The DDLs for `job_audit_log`, `job_error_log`, and `job_status` tables have been executed.
*   A mock `sp_k_ausd_bp_ta_apn_carmen` procedure is available to simulate the core script's behavior without requiring its full migration. This mock procedure will log its invocation and can be configured to succeed or fail.

---

### Mock `sp_k_ausd_bp_ta_apn_carmen` Procedure

This mock procedure is essential for isolating the testing of the wrapper script (`sp_ausd_bp_ta_apn_carmen`) from the yet-to-be-migrated core script.

```sql
-- DDL for Mock sp_k_ausd_bp_ta_apn_carmen
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_apn_carmen`(
  IN p_jobkennung STRING,
  IN p_stichtag STRING,
  IN p_job_nr INT64,
  IN p_wiederanlaufWert INT64,
  IN p_simulate_error BOOL DEFAULT FALSE,
  IN p_error_message STRING DEFAULT 'Simulated core script error',
  IN p_error_code INT64 DEFAULT 999
)
OPTIONS(
  description="MOCK BigQuery Stored Procedure for core processing logic.
               Used to test the wrapper's invocation and error handling."
)
BEGIN
  -- Log the invocation parameters for verification in job_audit_log
  INSERT INTO `your_project_id.your_dataset_id.job_audit_log`
  (job_nr, job_kennung, source_name, log_ref, stichtag, sysdate_ddmmyyyy, status, created_ts, message)
  VALUES
  (p_job_nr, p_jobkennung, 'sp_k_ausd_bp_ta_apn_carmen_MOCK', 'mock_log', p_stichtag, FORMAT_DATE('%d%m%Y', CURRENT_DATE()), 'INVOKED', CURRENT_TIMESTAMP(),
   CONCAT('Mock core script invoked with Stichtag: ', p_stichtag, ', Wiederanlaufwert: ', CAST(p_wiederanlaufWert AS STRING),
          ', Simulate Error: ', CAST(p_simulate_error AS STRING), ', Error Message: ', p_error_message));

  IF p_simulate_error THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Error ', CAST(p_error_code AS STRING), ': ', p_error_message);
  END IF;

  -- Simulate some successful work if no error
  -- SELECT 'Core script executed successfully' AS status;
END;
```

---

### Test Cases

#### Test Case 1: Default Parameter Handling (No Stichtag, No Wiederanlaufwert)

*   **Purpose:** Verify that the BigQuery stored procedure correctly defaults `Stichtag` to the current system date and `Wiederanlaufwert` to `0` when no parameters are provided, mirroring the KornShell behavior.
*   **Setup:**
    1.  Ensure `job_audit_log`, `job_error_log`, and `job_status` tables are empty or truncated.
    2.  The mock `sp_k_ausd_bp_ta_apn_carmen` is configured to succeed (`p_simulate_error = FALSE`).
*   **Action:** Execute the BigQuery stored procedure without any input parameters for `p_stichtag` and `p_wiederanlaufWert`.
    ```sql
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`(NULL, NULL);
    ```
*   **Pass/Fail Criteria:**
    1.  The `job_audit_log` table contains three entries for `job_kennung = 'ausd_bp_ta_apn_carmen'`:
        *   One with `status = 'STARTED'`.
        *   One with `source_name = 'sp_k_ausd_bp_ta_apn_carmen_MOCK'` and `status = 'INVOKED'`.
        *   One with `status = 'SUCCESS'`.
    2.  The `stichtag` column in all audit log entries matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    3.  The `message` field of the `INVOKED` entry from the mock core script confirms `Wiederanlaufwert: 0`.
    4.  The `job_status` table contains one entry for the job with `status = 'SUCCESS'`.
    5.  The `job_error_log` table is empty.

#### Test Case 2: Explicit Stichtag and Wiederanlaufwert

*   **Purpose:** Verify that the BigQuery stored procedure correctly processes explicit `Stichtag` and `Wiederanlaufwert` parameters, passing them to the core script.
*   **Setup:**
    1.  Ensure `job_audit_log`, `job_error_log`, and `job_status` tables are empty or truncated.
    2.  The mock `sp_k_ausd_bp_ta_apn_carmen` is configured to succeed (`p_simulate_error = FALSE`).
    3.  Define a specific `Stichtag` (e.g., '01012023') and `Wiederanlaufwert` (e.g., 12345).
*   **Action:** Execute the BigQuery stored procedure with explicit parameters.
    ```sql
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('01012023', 12345);
    ```
*   **Pass/Fail Criteria:**
    1.  The `job_audit_log` table contains three entries for `job_kennung = 'ausd_bp_ta_apn_carmen'`: `STARTED`, `INVOKED` (from mock), `SUCCESS`.
    2.  The `stichtag` column in all audit log entries is '01012023'.
    3.  The `message` field of the `INVOKED` entry from the mock core script confirms `Wiederanlaufwert: 12345`.
    4.  The `job_status` table contains one entry for the job with `status = 'SUCCESS'`.
    5.  The `job_error_log` table is empty.

#### Test Case 3: Stichtag Validation Error (Empty String)

*   **Purpose:** Verify that the BigQuery stored procedure correctly handles an empty string `Stichtag` parameter, treating it as an invalid input and logging an error, similar to `pruefeParameterGesetzt` in KornShell.
*   **Setup:**
    1.  Ensure `job_audit_log`, `job_error_log`, and `job_status` tables are empty or truncated.
*   **Action:** Execute the BigQuery stored procedure with an empty string for `p_stichtag`.
    ```sql
    -- This call is expected to fail and raise an error
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('', 0);
    ```
*   **Pass/Fail Criteria:**
    1.  The `CALL` statement fails and raises an error with `MESSAGE_TEXT` containing "Error 193: Stichtag - Required parameter missing or invalid".
    2.  The `job_error_log` table contains one entry:
        *   `job_kennung = 'ausd_bp_ta_apn_carmen'`
        *   `error_nr = 193`
        *   `error_arg = 'Stichtag'`
        *   `message` contains "Required parameter missing or invalid".
    3.  The `job_audit_log` table contains **no** `STARTED` or `SUCCESS` entries for the main wrapper, as the error occurs before the main audit log entry. (The error log entry is created before the `STARTED` audit log entry in the BQ SP).
    4.  The mock `sp_k_ausd_bp_ta_apn_carmen` is **not** invoked.
    5.  The `job_status` table is empty.

#### Test Case 4: Core Script Failure Handling

*   **Purpose:** Verify that the BigQuery stored procedure correctly handles errors originating from the invoked core script (`sp_k_ausd_bp_ta_apn_carmen`), logging the error and updating job status. This mimics the `trap ERR` behavior.
*   **Setup:**
    1.  Ensure `job_audit_log`, `job_error_log`, and `job_status` tables are empty or truncated.
    2.  Configure the mock `sp_k_ausd_bp_ta_apn_carmen` to simulate an error:
        ```sql
        -- Recreate mock to simulate error
        CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_apn_carmen`(
          IN p_jobkennung STRING, IN p_stichtag STRING, IN p_job_nr INT64, IN p_wiederanlaufWert INT64,
          IN p_simulate_error BOOL DEFAULT FALSE, IN p_error_message STRING DEFAULT 'Simulated core script error', IN p_error_code INT64 DEFAULT 999
        )
        BEGIN
          INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (job_nr, job_kennung, source_name, log_ref, stichtag, sysdate_ddmmyyyy, status, created_ts, message)
          VALUES (p_job_nr, p_jobkennung, 'sp_k_ausd_bp_ta_apn_carmen_MOCK', 'mock_log', p_stichtag, FORMAT_DATE('%d%m%Y', CURRENT_DATE()), 'INVOKED', CURRENT_TIMESTAMP(),
                  CONCAT('Mock core script invoked with Stichtag: ', p_stichtag, ', Wiederanlaufwert: ', CAST(p_wiederanlaufWert AS STRING), ', Simulating Error.'));
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error 500: Core logic failed';
        END;
        ```
*   **Action:** Execute the BigQuery stored procedure with valid parameters.
    ```sql
    -- This call is expected to fail due to the mock core script
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('01012023', 0);
    ```
*   **Pass/Fail Criteria:**
    1.  The `CALL` statement fails and raises an error with `MESSAGE_TEXT` containing "AppError: Abbruch - Error 500: Core logic failed".
    2.  The `job_audit_log` table contains three entries for `job_kennung = 'ausd_bp_ta_apn_carmen'`:
        *   One with `status = 'STARTED'`.
        *   One with `source_name = 'sp_k_ausd_bp_ta_apn_carmen_MOCK'` and `status = 'INVOKED'`.
        *   One with `status = 'ERROR'`, and `message` containing "AppError: Abbruch - Error 500: Core logic failed".
    3.  The `job_error_log` table contains one entry:
        *   `job_kennung = 'ausd_bp_ta_apn_carmen'`
        *   `error_nr` is `500` (or `NULL` if `ERROR_CODE()` returns `NULL` for custom `SIGNAL`s, but `COALESCE` should handle this).
        *   `error_arg` contains "Core logic failed".
        *   `message` contains "AppError: Abbruch".
    4.  The `job_status` table contains one entry for the job with `status = 'ERROR'`.

#### Test Case 5: `job_nr` Increment and `log_ref` Generation

*   **Purpose:** Verify that the `job_nr` is correctly incremented for each run of the job (per `job_kennung`) and that `log_ref` is generated consistently. This tests the replacement of `DWMSG_ErmittleNr` and `DWMSG_Logdateiname`.
*   **Setup:**
    1.  Ensure `job_audit_log`, `job_error_log`, and `job_status` tables are empty or truncated.
    2.  The mock `sp_k_ausd_bp_ta_apn_carmen` is configured to succeed (`p_simulate_error = FALSE`).
*   **Action:** Execute the BigQuery stored procedure multiple times with different parameters.
    ```sql
    -- First run
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('01012023', 100);
    -- Second run
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('02012023', 200);
    -- Third run (simulating an error to check error logging with correct job_nr)
    -- Temporarily reconfigure mock to fail
    CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_apn_carmen`(
      IN p_jobkennung STRING, IN p_stichtag STRING, IN p_job_nr INT64, IN p_wiederanlaufWert INT64,
      IN p_simulate_error BOOL DEFAULT FALSE, IN p_error_message STRING DEFAULT 'Simulated core script error', IN p_error_code INT64 DEFAULT 999
    )
    BEGIN
      INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (job_nr, job_kennung, source_name, log_ref, stichtag, sysdate_ddmmyyyy, status, created_ts, message)
      VALUES (p_job_nr, p_jobkennung, 'sp_k_ausd_bp_ta_apn_carmen_MOCK', 'mock_log', p_stichtag, FORMAT_DATE('%d%m%Y', CURRENT_DATE()), 'INVOKED', CURRENT_TIMESTAMP(),
              CONCAT('Mock core script invoked with Stichtag: ', p_stichtag, ', Wiederanlaufwert: ', CAST(p_wiederanlaufWert AS STRING), ', Simulating Error.'));
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error 501: Another core logic failure';
    END;
    -- Execute third run
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('03012023', 300);
    -- Restore mock to succeed for future tests
    CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_apn_carmen`(
      IN p_jobkennung STRING, IN p_stichtag STRING, IN p_job_nr INT64, IN p_wiederanlaufWert INT64,
      IN p_simulate_error BOOL DEFAULT FALSE, IN p_error_message STRING DEFAULT 'Simulated core script error', IN p_error_code INT64 DEFAULT 999
    )
    BEGIN
      INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (job_nr, job_kennung, source_name, log_ref, stichtag, sysdate_ddmmyyyy, status, created_ts, message)
      VALUES (p_job_nr, p_jobkennung, 'sp_k_ausd_bp_ta_apn_carmen_MOCK', 'mock_log', p_stichtag, FORMAT_DATE('%d%m%Y', CURRENT_DATE()), 'INVOKED', CURRENT_TIMESTAMP(),
              CONCAT('Mock core script invoked with Stichtag: ', p_stichtag, ', Wiederanlaufwert: ', CAST(p_wiederanlaufWert AS STRING), ', Simulate Error: ', CAST(p_simulate_error AS STRING), ', Error Message: ', p_error_message));
      IF p_simulate_error THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Error ', CAST(p_error_code AS STRING), ': ', p_error_message);
      END IF;
    END;
    ```
*   **Pass/Fail Criteria:**
    1.  Query `job_audit_log` for `job_kennung = 'ausd_bp_ta_apn_carmen'`. There should be 3 sets of `STARTED`/`INVOKED`/`SUCCESS` (or `ERROR`) entries.
    2.  The `job_nr` values for these runs should be `1`, `2`, and `3` respectively (or `MAX(job_nr)+1` if previous runs existed).
    3.  For each `job_nr`, the `log_ref` should be `job_ausd_bp_ta_apn_carmen_<job_nr>.log`.
    4.  The `job_error_log` should contain one entry for the third run, with the correct `job_nr` (e.g., `3`).

#### Test Case 6: NULL Handling for Optional Parameters

*   **Purpose:** Verify that explicitly passing `NULL` for optional parameters (`p_stichtag`, `p_wiederanlaufWert`) results in the same default behavior as not providing them at all.
*   **Setup:**
    1.  Ensure `job_audit_log`, `job_error_log`, and `job_status` tables are empty or truncated.
    2.  The mock `sp_k_ausd_bp_ta_apn_carmen` is configured to succeed (`p_simulate_error = FALSE`).
*   **Action:** Execute the BigQuery stored procedure with `NULL` for both parameters.
    ```sql
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`(NULL, NULL);
    ```
*   **Pass/Fail Criteria:**
    1.  This test should yield identical results to **Test Case 1 (Default Parameter Handling)**.
    2.  The `job_audit_log` table contains three entries: `STARTED`, `INVOKED`, `SUCCESS`.
    3.  The `stichtag` column in all audit log entries matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    4.  The `message` field of the `INVOKED` entry from the mock core script confirms `Wiederanlaufwert: 0`.
    5.  The `job_status` table contains one entry for the job with `status = 'SUCCESS'`.
    6.  The `job_error_log` table is empty.

#### Test Case 7: Data Type Handling for Wiederanlaufwert

*   **Purpose:** Verify that the `p_wiederanlaufWert` parameter, defined as `INT64` in BigQuery, correctly handles integer values and raises an error for non-integer inputs, consistent with type enforcement. (KornShell would likely treat non-numeric as 0 if used in arithmetic, or error if used in comparison).
*   **Setup:**
    1.  Ensure `job_audit_log`, `job_error_log`, and `job_status` tables are empty or truncated.
*   **Action:** Attempt to call the procedure with a non-integer value for `p_wiederanlaufWert`.
    ```sql
    -- This call is expected to fail at the BigQuery parameter parsing level
    CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('01012023', 'ABC');
    ```
*   **Pass/Fail Criteria:**
    1.  The `CALL` statement fails with a BigQuery error indicating a data type mismatch (e.g., "Invalid value: 'ABC' for type INT64").
    2.  No entries are created in `job_audit_log`, `job_error_log`, or `job_status` tables, as the error occurs before the procedure's execution begins. This is a BigQuery-level validation, which is stricter than KornShell's implicit type handling. This is an acceptable behavioral difference as it improves data quality and type safety.

---

### General Data Quality / Schema Assertions (Post-Execution)

These assertions should be run after any test case that modifies the audit/error logs to ensure data integrity.

*   **Purpose:** Verify the schema, data types, and basic data quality of the logging tables.
*   **Setup:** Run any of the above test cases.
*   **Action:** Execute SQL queries against the logging tables.
*   **Pass/Fail Criteria:**
    1.  **Schema Conformance:**
        ```sql
        -- Check job_audit_log schema
        SELECT column_name, data_type
        FROM `your_project_id.your_dataset_id`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'job_audit_log'
        ORDER BY ordinal_position;
        -- Expected: job_nr INT64, job_kennung STRING, source_name STRING, log_ref STRING, stichtag STRING, sysdate_ddmmyyyy STRING, status STRING, created_ts TIMESTAMP, message STRING
        ```
        (Repeat for `job_error_log` and `job_status`)
    2.  **Non-Null Constraints (Implicit):**
        ```sql
        -- Check for unexpected NULLs in critical fields in job_audit_log
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_audit_log`
        WHERE job_nr IS NULL OR job_kennung IS NULL OR status IS NULL OR created_ts IS NULL;
        -- Expected: 0
        ```
        (Repeat for `job_error_log` and `job_status` for their critical fields)
    3.  **Date Format Consistency:**
        ```sql
        -- Check if stichtag and sysdate_ddmmyyyy are in DDMMYYYY format
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_audit_log`
        WHERE NOT REGEXP_CONTAINS(stichtag, r'^\d{8}$')
           OR NOT REGEXP_CONTAINS(sysdate_ddmmyyyy, r'^\d{8}$');
        -- Expected: 0
        ```
    4.  **Status Values:**
        ```sql
        -- Check for valid status values in job_audit_log
        SELECT DISTINCT status FROM `your_project_id.your_dataset_id.job_audit_log`;
        -- Expected: Only 'STARTED', 'INVOKED', 'SUCCESS', 'ERROR'
        ```
        (Repeat for `job_status` with 'STARTED', 'OK', 'ERROR')
    5.  **Referential Integrity (Conceptual):**
        ```sql
        -- Verify job_error_log entries have a corresponding job_audit_log entry (by job_nr and job_kennung)
        SELECT COUNT(DISTINCT e.job_nr)
        FROM `your_project_id.your_dataset_id.job_error_log` e
        LEFT JOIN `your_project_id.your_dataset_id.job_audit_log` a
          ON e.job_nr = a.job_nr AND e.job_kennung = a.job_kennung
        WHERE a.job_nr IS NULL;
        -- Expected: 0 (unless an error occurs before the first audit log entry, as in Test Case 3)
        ```

---

These tests provide a robust framework for validating the migration of the KornShell wrapper script to BigQuery, ensuring functional equivalence and adherence to the migration design. The use of a mock core script is crucial for isolating the testing scope and managing dependencies.