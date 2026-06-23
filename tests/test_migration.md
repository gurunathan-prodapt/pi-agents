As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migrated `r_ausd_bp_ta_bpr_apn.ksh` job. These tests focus on ensuring behavioral equivalence between the legacy KornShell script and its BigQuery Stored Procedure counterpart, covering output parity, transformation correctness, external system interactions, and data quality.

For the purpose of these tests, we will assume the BigQuery project and dataset are `test_project.test_dataset`.

---

## Setup for All Tests

Before running any tests, ensure the following BigQuery objects are created:

1.  **`job_audit_log` table:**
    ```sql
    CREATE TABLE IF NOT EXISTS `test_project.test_dataset.job_audit_log` (
        job_nr INT64 NOT NULL OPTIONS(description="Unique job run number for a given job_kennung"),
        job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job (e.g., ausd_bp_ta_bpr_apn)"),
        prog_name STRING OPTIONS(description="Program name as defined in the source script"),
        prog_version STRING OPTIONS(description="Program version"),
        log_datei STRING OPTIONS(description="Simulated log file name"),
        stichtag STRING OPTIONS(description="Reference date for the job run (DDMMYYYY)"),
        status STRING NOT NULL OPTIONS(description="Execution status (e.g., STARTED, OK, ERROR)"),
        message STRING OPTIONS(description="Detailed message about the status or error"),
        created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry")
    );
    ```

2.  **`k_ausd_bp_ta_bpr_apn_mock_log` table:** This table is used to capture parameters passed to the kernel stored procedure for validation, as the kernel itself is a placeholder.
    ```sql
    CREATE TABLE IF NOT EXISTS `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log` (
        job_kennung STRING,
        stichtag STRING,
        job_nr INT64,
        wiederanlaufWert INT64,
        call_timestamp TIMESTAMP
    );
    ```

3.  **Modified `k_ausd_bp_ta_bpr_apn` (Kernel) Stored Procedure:** This version includes logging to the mock table and a conditional error for testing.
    ```sql
    CREATE OR REPLACE PROCEDURE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn`(
      IN p_job_kennung STRING,
      IN p_stichtag STRING,
      IN p_job_nr INT64,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      -- Log parameters received by the kernel for testing purposes
      INSERT INTO `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log` (
        job_kennung, stichtag, job_nr, wiederanlaufWert, call_timestamp
      )
      VALUES (
        p_job_kennung, p_stichtag, p_job_nr, p_wiederanlaufWert, CURRENT_TIMESTAMP()
      );

      -- Simulate an error in the kernel if a specific stichtag is passed
      IF p_stichtag = 'SIMULATE_KERNEL_ERROR' THEN
        RAISE USING MESSAGE = 'Simulated error within k_ausd_bp_ta_bpr_apn for testing.';
      END IF;

      -- Original TODO: Implement the core business logic from k_ausd_bp_ta_bpr_apn.ksh here.
    END;
    ```

4.  **`ausd_bp_ta_bpr_apn` (Wrapper) Stored Procedure:** The migrated code under test.
    ```sql
    CREATE OR REPLACE PROCEDURE `test_project.test_dataset.ausd_bp_ta_bpr_apn`(
      IN p_stichtag STRING,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      -- Declare variables
      DECLARE v_prog_name STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
      DECLARE v_prog_version STRING DEFAULT 'V2.0.0';
      DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_bpr_apn';
      DECLARE v_job_nr INT64;
      DECLARE v_log_datei STRING;
      DECLARE v_sysdate STRING;
      DECLARE v_final_stichtag STRING;
      DECLARE v_final_wiederanlaufWert INT64; -- Corrected from INT66
      DECLARE v_err_msg STRING DEFAULT NULL;

      -- 1. Initialize defaults and parse parameters
      SET v_final_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

      -- Derive system date in DDMMYYYY format
      SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

      -- Default stichtag if not provided or empty
      SET v_final_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

      -- 2. Validate required parameters
      ASSERT v_final_stichtag IS NOT NULL AND v_final_stichtag != '' AS 'Stichtag must be set and not empty.';

      -- Optional: validate date format DDMMYYYY
      ASSERT SAFE.PARSE_DATE('%d%m%Y', v_final_stichtag) IS NOT NULL AS 'Invalid Stichtag format. Expected DDMMYYYY.';

      -- 3. Create job number and log reference (placeholder logic for audit table)
      -- This assumes job_nr is sequential per job_kennung
      SET v_job_nr = (
        SELECT IFNULL(MAX(job_nr), 0) + 1
        FROM `test_project.test_dataset.job_audit_log`
        WHERE job_kennung = v_job_kennung
      );

      SET v_log_datei = CONCAT('job_', v_job_kennung, '_', CAST(v_job_nr AS STRING), '.log');

      -- 4. Insert audit start record
      INSERT INTO `test_project.test_dataset.job_audit_log` (
        job_nr,
        job_kennung,
        prog_name,
        prog_version,
        log_datei,
        stichtag,
        status,
        message,
        created_at
      )
      VALUES (
        v_job_nr,
        v_job_kennung,
        v_prog_name,
        v_prog_version,
        v_log_datei,
        v_final_stichtag,
        'STARTED',
        'Job started',
        CURRENT_TIMESTAMP()
      );

      -- 5. Begin error trapping block
      BEGIN
        -- 6. Invoke the core kernel script (migrated k_ausd_bp_ta_bpr_apn.ksh)
        CALL `test_project.test_dataset.k_ausd_bp_ta_bpr_apn`(
          v_job_kennung,
          v_final_stichtag,
          v_job_nr,
          v_final_wiederanlaufWert
        );

        -- 7. Mark success if kernel procedure completes without error
        INSERT INTO `test_project.test_dataset.job_audit_log` (
          job_nr,
          job_kennung,
          prog_name,
          prog_version,
          log_datei,
          stichtag,
          status,
          message,
          created_at
        )
        VALUES (
          v_job_nr,
          v_job_kennung,
          v_prog_name,
          v_prog_version,
          v_log_datei,
          v_final_stichtag,
          'OK',
          'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
          CURRENT_TIMESTAMP()
        );

      EXCEPTION WHEN ERROR THEN
        SET v_err_msg = @@error.message;

        -- 8. Log error message
        INSERT INTO `test_project.test_dataset.job_audit_log` (
          job_nr,
          job_kennung,
          prog_name,
          prog_version,
          log_datei,
          stichtag,
          status,
          message,
          created_at
        )
        VALUES (
          v_job_nr,
          v_job_kennung,
          v_prog_name,
          v_prog_version,
          v_log_datei,
          v_final_stichtag,
          'ERROR',
          CONCAT('AppError: Abbruch - ', v_err_msg),
          CURRENT_TIMESTAMP()
        );

        -- Re-raise the error to signal job failure to the orchestrator
        RAISE USING MESSAGE = CONCAT('AppError: Abbruch - ', v_err_msg);
      END;
    END;
    ```

---

## 1. Output Parity & Transformation Correctness Tests

These tests verify that the BigQuery SP produces the same logical outputs (audit log entries, parameters passed to the kernel) as the legacy script for various input scenarios.

### Test Case 1.1: No Parameters Provided (Default Behavior)

*   **Purpose:** Verify that `p_stichtag` defaults to `CURRENT_DATE()` (DDMMYYYY) and `p_wiederanlaufWert` defaults to `0` when no parameters are explicitly passed. Also, check successful logging.
*   **Setup:**
    ```sql
    -- Clear audit and mock logs for a clean test run
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`(NULL, NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  Two entries in `job_audit_log` for `job_kennung = 'ausd_bp_ta_bpr_apn'`, with `status` 'STARTED' and 'OK' respectively.
    2.  The `stichtag` in both `job_audit_log` entries and in `k_ausd_bp_ta_bpr_apn_mock_log` should be `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    3.  The `wiederanlaufWert` in `k_ausd_bp_ta_bpr_apn_mock_log` should be `0`.
    4.  `job_nr` should be `1` for this first run.

    ```sql
    -- Assertion Query
    SELECT
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status IN ('STARTED', 'OK')) = 2 AS audit_log_count_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'OK' AND message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet') = 1 AS success_message_ok,
        (SELECT stichtag FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'OK') = FORMAT_DATE('%d%m%Y', CURRENT_DATE()) AS audit_stichtag_default_ok,
        (SELECT job_nr FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'OK') = 1 AS audit_job_nr_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 1 AS kernel_call_count_ok,
        (SELECT stichtag FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = FORMAT_DATE('%d%m%Y', CURRENT_DATE()) AS kernel_stichtag_default_ok,
        (SELECT wiederanlaufWert FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 0 AS kernel_wiederanlauf_default_ok
    ```

### Test Case 1.2: Both Parameters Provided (Valid Inputs)

*   **Purpose:** Verify that explicit `p_stichtag` and `p_wiederanlaufWert` are correctly used and passed to the kernel.
*   **Setup:**
    ```sql
    -- Clear audit and mock logs for a clean test run
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('01012023', 12345);
    ```
*   **Pass/Fail Criterion:**
    1.  Two entries in `job_audit_log` for `job_kennung = 'ausd_bp_ta_bpr_apn'`, with `status` 'STARTED' and 'OK'.
    2.  The `stichtag` in `job_audit_log` and `k_ausd_bp_ta_bpr_apn_mock_log` should be `'01012023'`.
    3.  The `wiederanlaufWert` in `k_ausd_bp_ta_bpr_apn_mock_log` should be `12345`.
    4.  `job_nr` should be `1`.

    ```sql
    -- Assertion Query
    SELECT
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status IN ('STARTED', 'OK')) = 2 AS audit_log_count_ok,
        (SELECT stichtag FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'OK') = '01012023' AS audit_stichtag_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 1 AS kernel_call_count_ok,
        (SELECT stichtag FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = '01012023' AS kernel_stichtag_ok,
        (SELECT wiederanlaufWert FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 12345 AS kernel_wiederanlauf_ok
    ```

### Test Case 1.3: Only `p_stichtag` Provided

*   **Purpose:** Verify `p_stichtag` is used and `p_wiederanlaufWert` defaults to `0`.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('15032024', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  Two entries in `job_audit_log` with `status` 'STARTED' and 'OK'.
    2.  `stichtag` in logs and kernel call is `'15032024'`.
    3.  `wiederanlaufWert` in kernel call is `0`.

    ```sql
    -- Assertion Query
    SELECT
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status IN ('STARTED', 'OK')) = 2 AS audit_log_count_ok,
        (SELECT stichtag FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'OK') = '15032024' AS audit_stichtag_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 1 AS kernel_call_count_ok,
        (SELECT stichtag FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = '15032024' AS kernel_stichtag_ok,
        (SELECT wiederanlaufWert FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 0 AS kernel_wiederanlauf_default_ok
    ```

### Test Case 1.4: Empty String for `p_stichtag`

*   **Purpose:** Verify that an empty string for `p_stichtag` correctly defaults to `CURRENT_DATE()`. This covers `NULLIF(TRIM(p_stichtag), '')`.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  Two entries in `job_audit_log` with `status` 'STARTED' and 'OK'.
    2.  `stichtag` in logs and kernel call is `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    3.  `wiederanlaufWert` in kernel call is `0`.

    ```sql
    -- Assertion Query
    SELECT
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status IN ('STARTED', 'OK')) = 2 AS audit_log_count_ok,
        (SELECT stichtag FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'OK') = FORMAT_DATE('%d%m%Y', CURRENT_DATE()) AS audit_stichtag_default_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 1 AS kernel_call_count_ok,
        (SELECT stichtag FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = FORMAT_DATE('%d%m%Y', CURRENT_DATE()) AS kernel_stichtag_default_ok,
        (SELECT wiederanlaufWert FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 0 AS kernel_wiederanlauf_default_ok
    ```

### Test Case 1.5: Invalid `p_stichtag` Format

*   **Purpose:** Verify that the added date format validation (`ASSERT SAFE.PARSE_DATE`) correctly catches invalid formats and raises an error.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    -- This call is expected to fail. Wrap in a try-catch in Python/Airflow if needed.
    -- For direct SQL, it will just raise an error.
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('2023-01-01', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement should raise an error with a message containing `'Invalid Stichtag format. Expected DDMMYYYY.'`.
    2.  Two entries in `job_audit_log`: one 'STARTED' and one 'ERROR'.
    3.  The 'ERROR' entry's `message` should contain `'Invalid Stichtag format. Expected DDMMYYYY.'`.
    4.  No entry should be present in `k_ausd_bp_ta_bpr_apn_mock_log` (kernel should not be called).

    ```sql
    -- Assertion Query (run after attempting the CALL)
    SELECT
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'STARTED') = 1 AS audit_started_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'ERROR') = 1 AS audit_error_ok,
        (SELECT message FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'ERROR') LIKE '%Invalid Stichtag format. Expected DDMMYYYY.%' AS error_message_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 0 AS kernel_not_called_ok
    ```

---

## 2. External-System Replacements Tests

These tests focus on the correct interaction with the `job_audit_log` table (replacement for shell logging) and the invocation of the kernel stored procedure (replacement for shell script invocation).

### Test Case 2.1: Audit Log Entries for Successful Run

*   **Purpose:** Verify that `job_audit_log` correctly records 'STARTED' and 'OK' statuses with appropriate details for a successful job execution.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('01012024', 500);
    ```
*   **Pass/Fail Criterion:**
    1.  Exactly two rows in `job_audit_log` for `job_kennung = 'ausd_bp_ta_bpr_apn'`.
    2.  One row with `status = 'STARTED'`, `message = 'Job started'`.
    3.  One row with `status = 'OK'`, `message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'`.
    4.  Both rows have the same `job_nr`, `prog_name`, `prog_version`, `log_datei`, and `stichtag`.
    5.  `created_at` timestamps are sequential.

    ```sql
    -- Assertion Query
    SELECT
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn') = 2 AS total_audit_entries_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'STARTED' AND message = 'Job started') = 1 AS started_entry_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'OK' AND message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet') = 1 AS ok_entry_ok,
        (SELECT COUNT(DISTINCT job_nr) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn') = 1 AS single_job_nr_ok,
        (SELECT COUNT(DISTINCT stichtag) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn') = 1 AS single_stichtag_ok,
        (SELECT MAX(created_at) > MIN(created_at) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn') AS timestamps_sequential_ok
    ```

### Test Case 2.2: Audit Log Entries for Failed Run (Kernel Error)

*   **Purpose:** Verify that `job_audit_log` correctly records 'STARTED' and 'ERROR' statuses when the kernel stored procedure raises an error.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    -- The kernel is designed to raise an error if stichtag is 'SIMULATE_KERNEL_ERROR'
    -- This call is expected to fail and re-raise the error.
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('SIMULATE_KERNEL_ERROR', 0);
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement should raise an error with a message containing `'Simulated error within k_ausd_bp_ta_bpr_apn for testing.'`.
    2.  Two entries in `job_audit_log`: one 'STARTED' and one 'ERROR'.
    3.  The 'ERROR' entry's `message` should contain `'AppError: Abbruch - Simulated error within k_ausd_bp_ta_bpr_apn for testing.'`.
    4.  One entry in `k_ausd_bp_ta_bpr_apn_mock_log` (kernel was called before it failed).

    ```sql
    -- Assertion Query (run after attempting the CALL)
    SELECT
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'STARTED') = 1 AS audit_started_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'ERROR') = 1 AS audit_error_ok,
        (SELECT message FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND status = 'ERROR') LIKE '%AppError: Abbruch - Simulated error within k_ausd_bp_ta_bpr_apn for testing.%' AS error_message_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 1 AS kernel_called_ok
    ```

### Test Case 2.3: Kernel Stored Procedure Invocation

*   **Purpose:** Verify that the `k_ausd_bp_ta_bpr_apn` stored procedure is called exactly once with the correct parameters during a successful run.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('10102023', 999);
    ```
*   **Pass/Fail Criterion:**
    1.  Exactly one row in `k_ausd_bp_ta_bpr_apn_mock_log`.
    2.  The `job_kennung` in `k_ausd_bp_ta_bpr_apn_mock_log` is `'ausd_bp_ta_bpr_apn'`.
    3.  The `stichtag` is `'10102023'`.
    4.  The `wiederanlaufWert` is `999`.
    5.  The `job_nr` matches the `job_nr` from the `job_audit_log` entries.

    ```sql
    -- Assertion Query
    SELECT
        (SELECT COUNT(*) FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 1 AS kernel_call_count_ok,
        (SELECT job_kennung FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 'ausd_bp_ta_bpr_apn' AS kernel_job_kennung_ok,
        (SELECT stichtag FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = '10102023' AS kernel_stichtag_ok,
        (SELECT wiederanlaufWert FROM `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`) = 999 AS kernel_wiederanlauf_ok,
        (SELECT T1.job_nr = T2.job_nr FROM `test_project.test_dataset.job_audit_log` AS T1 JOIN `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log` AS T2 ON T1.job_kennung = T2.job_kennung LIMIT 1) AS job_nr_match_ok
    ```

---

## 3. Data Quality / Row-Count / Schema Assertions

These tests focus on the integrity and correctness of the `job_audit_log` table, which is the primary data output of this wrapper script.

### Test Case 3.1: `job_nr` Incrementing Correctly

*   **Purpose:** Verify that the `job_nr` is correctly incremented for each new run of the job, even across multiple successful and failed executions.
*   **Setup:**
    ```sql
    TRUNCATE TABLE `test_project.test_dataset.job_audit_log`;
    TRUNCATE TABLE `test_project.test_dataset.k_ausd_bp_ta_bpr_apn_mock_log`;
    ```
*   **Action:**
    ```sql
    -- Run 1 (Success)
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('01012023', 100);
    -- Run 2 (Success)
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('02012023', 200);
    -- Run 3 (Failure due to invalid stichtag)
    BEGIN
      CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('INVALID_DATE', 300);
    EXCEPTION WHEN ERROR THEN
      -- Expected error, do nothing
    END;
    -- Run 4 (Success)
    CALL `test_project.test_dataset.ausd_bp_ta_bpr_apn`('03012023', 400);
    ```
*   **Pass/Fail Criterion:**
    1.  The `job_audit_log` should contain entries for `job_nr` 1, 2, 3, and 4.
    2.  For each `job_nr`, there should be a 'STARTED' and a final 'OK' or 'ERROR' status.

    ```sql
    -- Assertion Query
    SELECT
        (SELECT COUNT(DISTINCT job_nr) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn') = 4 AS distinct_job_nr_count_ok,
        (SELECT MAX(job_nr) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn') = 4 AS max_job_nr_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND job_nr = 1 AND status = 'OK') = 1 AS job_nr_1_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND job_nr = 2 AND status = 'OK') = 1 AS job_nr_2_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND job_nr = 3 AND status = 'ERROR') = 1 AS job_nr_3_error_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_kennung = 'ausd_bp_ta_bpr_apn' AND job_nr = 4 AND status = 'OK') = 1 AS job_nr_4_ok
    ```

### Test Case 3.2: Schema and Data Type Integrity of `job_audit_log`

*   **Purpose:** Verify that the `job_audit_log` table adheres to its defined schema and that data types are correctly handled.
*   **Setup:** (Implicit, as the table is created in the global setup)
*   **Action:** No specific action needed beyond the previous test runs which populated the table.
*   **Pass/Fail Criterion:**
    1.  All columns in `job_audit_log` have the expected data types (e.g., `job_nr` is `INT64`, `stichtag` is `STRING`, `created_at` is `TIMESTAMP`).
    2.  No `NULL` values in `job_nr`, `job_kennung`, `status`, `created_at` columns.
    3.  `stichtag` values are always 8 characters long (DDMMYYYY format).

    ```sql
    -- Assertion Query
    SELECT
        (SELECT data_type FROM `test_project.test_dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_audit_log' AND column_name = 'job_nr') = 'INT64' AS job_nr_type_ok,
        (SELECT data_type FROM `test_project.test_dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_audit_log' AND column_name = 'job_kennung') = 'STRING' AS job_kennung_type_ok,
        (SELECT data_type FROM `test_project.test_dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_audit_log' AND column_name = 'stichtag') = 'STRING' AS stichtag_type_ok,
        (SELECT data_type FROM `test_project.test_dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_audit_log' AND column_name = 'status') = 'STRING' AS status_type_ok,
        (SELECT data_type FROM `test_project.test_dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_audit_log' AND column_name = 'created_at') = 'TIMESTAMP' AS created_at_type_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE job_nr IS NULL OR job_kennung IS NULL OR status IS NULL OR created_at IS NULL) = 0 AS non_nullable_columns_not_null_ok,
        (SELECT COUNT(*) FROM `test_project.test_dataset.job_audit_log` WHERE LENGTH(stichtag) != 8) = 0 AS stichtag_length_ok
    ```