The migration of `r_ausd_bp_ta_msisdn_his.ksh` to a BigQuery Stored Procedure (`ausd_bp_ta_msisdn_his_wrapper`) primarily involves re-implementing orchestration, parameter handling, and logging. The core data transformation logic resides in a separate kernel script/procedure. Therefore, the tests will focus on ensuring the wrapper's behavior, logging, and error handling are equivalent to the legacy shell script.

The tests will cover:
1.  **Output Parity**: Verifying that the BigQuery audit tables (`job_registry`, `job_message_log`, `job_error_log`) accurately reflect the job's execution, status, and messages, similar to how the legacy script would write to log files and update internal status.
2.  **Transformation Correctness**: Ensuring parameter parsing, defaulting logic, date determination, and the invocation of the kernel procedure are correct.
3.  **External-system replacements**: Validating that the BigQuery audit tables correctly replace the shell script's logging mechanisms and that the `CALL` to the kernel SP functions as expected.
4.  **Data-quality / row-count / schema assertions**: Implicitly covered by verifying the content and existence of records in the audit tables.

---

## Test Setup (Pre-requisites for all tests)

Before running any tests, ensure the following BigQuery objects are created:

1.  **BigQuery Dataset**: `my_project.my_dataset`
2.  **Audit Tables**:
    *   `my_project.my_dataset.job_registry`
    *   `my_project.my_dataset.job_message_log`
    *   `my_project.my_dataset.job_error_log`
3.  **Kernel Stored Procedure Stub**: `my_project.my_dataset.k_ausd_bp_ta_msisdn_his`
4.  **Wrapper Stored Procedure**: `my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper`

**Cleanup Script (to run before each test case for isolation):**

```sql
-- Clear audit tables before each test run
DELETE FROM my_project.my_dataset.job_registry WHERE TRUE;
DELETE FROM my_project.my_dataset.job_message_log WHERE TRUE;
DELETE FROM my_project.my_dataset.job_error_log WHERE TRUE;
```

---

## Test Case 1: Happy Path - All Parameters Provided

**Purpose:** Verify that the wrapper executes successfully when all required parameters (`p_stichtag`, `p_wiederanlaufWert`) are explicitly provided, logging all steps correctly and updating the job status to 'OK'. This covers output parity and transformation correctness for parameter handling.

**Setup:**
1.  Run the cleanup script to ensure audit tables are empty.
2.  Define test parameters:
    *   `p_stichtag`: '2023-01-15' (a specific date)
    *   `p_wiederanlaufWert`: 12345

**Action:**
Execute the BigQuery Stored Procedure with the defined parameters.

```sql
CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(DATE '2023-01-15', 12345);
```

**Pass/Fail Criterion:**
1.  **`job_registry`**: Exactly one row exists with `job_name = 'AUSD_BP_TA_MSISDN_HIS'`, `script_name = 'r_ausd_bp_ta_msisdn_his_wrapper'`, `stichtag = '2023-01-15'`, and `status = 'OK'`. `created_at` and `finished_at` should be populated.
2.  **`job_message_log`**:
    *   Exactly three rows exist for the `job_entry_nr` from `job_registry`.
    *   One message indicating job start, containing `Stichtag: 2023-01-15` and `Wiederanlaufwert: 12345`.
    *   One message from the kernel stub, containing `Kernel script k_ausd_bp_ta_msisdn_his called with Stichtag: 2023-01-15, Wiederanlaufwert: 12345`.
    *   One message indicating job completion.
3.  **`job_error_log`**: Zero rows exist.

**Runnable Test Code (SQL Assertions):**

```sql
-- Assert job_registry entry
SELECT
    COUNT(1) AS row_count,
    MAX(CASE WHEN job_name = 'AUSD_BP_TA_MSISDN_HIS' AND script_name = 'r_ausd_bp_ta_msisdn_his_wrapper' AND stichtag = DATE '2023-01-15' AND status = 'OK' AND created_at IS NOT NULL AND finished_at IS NOT NULL THEN 1 ELSE 0 END) AS is_correct_entry
FROM my_project.my_dataset.job_registry;
-- Expected: row_count = 1, is_correct_entry = 1

-- Get the job_entry_nr for subsequent checks
DECLARE v_job_entry_nr INT64;
SET v_job_entry_nr = (SELECT job_entry_nr FROM my_project.my_dataset.job_registry WHERE job_name = 'AUSD_BP_TA_MSISDN_HIS' AND stichtag = DATE '2023-01-15');

-- Assert job_message_log entries
SELECT
    COUNT(1) AS row_count,
    MAX(CASE WHEN message_text LIKE '%Job AUSD_BP_TA_MSISDN_HIS (Version 1.0) started for Stichtag 2023-01-15 with Wiederanlaufwert 12345%' THEN 1 ELSE 0 END) AS has_start_message,
    MAX(CASE WHEN message_text LIKE '%Kernel script k_ausd_bp_ta_msisdn_his called with Stichtag: 2023-01-15, Wiederanlaufwert: 12345%' THEN 1 ELSE 0 END) AS has_kernel_message,
    MAX(CASE WHEN message_text LIKE '%Job AUSD_BP_TA_MSISDN_HIS completed successfully%' THEN 1 ELSE 0 END) AS has_completion_message
FROM my_project.my_dataset.job_message_log
WHERE job_entry_nr = v_job_entry_nr;
-- Expected: row_count = 3, has_start_message = 1, has_kernel_message = 1, has_completion_message = 1

-- Assert job_error_log is empty
SELECT COUNT(1) AS row_count FROM my_project.my_dataset.job_error_log WHERE job_entry_nr = v_job_entry_nr;
-- Expected: row_count = 0
```

---

## Test Case 2: Happy Path - Default Stichtag and Wiederanlaufwert

**Purpose:** Verify that the wrapper correctly defaults `p_stichtag` to `CURRENT_DATE()` and `p_wiederanlaufWert` to `0` when they are not provided (or are NULL), and executes successfully. This covers transformation correctness for defaulting logic.

**Setup:**
1.  Run the cleanup script.
2.  Define test parameters:
    *   `p_stichtag`: NULL
    *   `p_wiederanlaufWert`: NULL

**Action:**
Execute the BigQuery Stored Procedure with NULL parameters.

```sql
CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(NULL, NULL);
```

**Pass/Fail Criterion:**
1.  **`job_registry`**: Exactly one row exists with `job_name = 'AUSD_BP_TA_MSISDN_HIS'`, `script_name = 'r_ausd_bp_ta_msisdn_his_wrapper'`, `stichtag = CURRENT_DATE()`, and `status = 'OK'`.
2.  **`job_message_log`**:
    *   Exactly three rows exist for the `job_entry_nr` from `job_registry`.
    *   One message indicating job start, containing `Stichtag: <CURRENT_DATE>` and `Wiederanlaufwert: 0`.
    *   One message from the kernel stub, containing `Kernel script k_ausd_bp_ta_msisdn_his called with Stichtag: <CURRENT_DATE>, Wiederanlaufwert: 0`.
    *   One message indicating job completion.
3.  **`job_error_log`**: Zero rows exist.

**Runnable Test Code (SQL Assertions):**

```sql
-- Assert job_registry entry
SELECT
    COUNT(1) AS row_count,
    MAX(CASE WHEN job_name = 'AUSD_BP_TA_MSISDN_HIS' AND script_name = 'r_ausd_bp_ta_msisdn_his_wrapper' AND stichtag = CURRENT_DATE() AND status = 'OK' THEN 1 ELSE 0 END) AS is_correct_entry
FROM my_project.my_dataset.job_registry;
-- Expected: row_count = 1, is_correct_entry = 1

-- Get the job_entry_nr for subsequent checks
DECLARE v_job_entry_nr INT64;
SET v_job_entry_nr = (SELECT job_entry_nr FROM my_project.my_dataset.job_registry WHERE job_name = 'AUSD_BP_TA_MSISDN_HIS' AND stichtag = CURRENT_DATE());

-- Assert job_message_log entries
SELECT
    COUNT(1) AS row_count,
    MAX(CASE WHEN message_text LIKE FORMAT('%%Job AUSD_BP_TA_MSISDN_HIS (Version 1.0) started for Stichtag %s with Wiederanlaufwert 0%%', FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())) THEN 1 ELSE 0 END) AS has_start_message,
    MAX(CASE WHEN message_text LIKE FORMAT('%%Kernel script k_ausd_bp_ta_msisdn_his called with Stichtag: %s, Wiederanlaufwert: 0%%', FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())) THEN 1 ELSE 0 END) AS has_kernel_message,
    MAX(CASE WHEN message_text LIKE '%Job AUSD_BP_TA_MSISDN_HIS completed successfully%' THEN 1 ELSE 0 END) AS has_completion_message
FROM my_project.my_dataset.job_message_log
WHERE job_entry_nr = v_job_entry_nr;
-- Expected: row_count = 3, has_start_message = 1, has_kernel_message = 1, has_completion_message = 1

-- Assert job_error_log is empty
SELECT COUNT(1) AS row_count FROM my_project.my_dataset.job_error_log WHERE job_entry_nr = v_job_entry_nr;
-- Expected: row_count = 0
```

---

## Test Case 3: Error Path - Missing Stichtag (Validation Failure)

**Purpose:** Verify that the wrapper correctly handles the critical error of a missing `Stichtag` (even after defaulting attempts), logs the error, updates the job status to 'ERROR', and raises an exception. This covers transformation correctness for validation and external-system replacements for error logging.

**Setup:**
1.  Run the cleanup script.
2.  Modify the `ausd_bp_ta_msisdn_his_wrapper` SP temporarily to force `v_stichtag` to NULL after defaulting, simulating a scenario where `CURRENT_DATE()` might fail or be invalid (e.g., if `p_stichtag` was an invalid string in the legacy script that resulted in an empty variable). For this test, we'll simulate this by passing a NULL `p_stichtag` and then assuming `v_sysdate` also somehow becomes NULL. *Note: In a real BigQuery environment, `CURRENT_DATE()` will always return a date, so this specific scenario might require a more complex stub or a direct modification of the SP for testing purposes.* For simplicity, we'll assume the `IF v_stichtag IS NULL` check is the target.

**Action:**
Execute the BigQuery Stored Procedure with a NULL `p_stichtag`. The SP's internal logic should then trigger the `IF v_stichtag IS NULL` check if `v_sysdate` was also somehow NULL (which it won't be in BQ). To properly test this, we need to *force* `v_stichtag` to be NULL after the `IFNULL` assignment. This requires a slight modification to the SP for testing, or we assume `p_stichtag` is passed as a `STRING` and then converted, and the conversion fails. Given the current SP definition, `p_stichtag DATE` means it's already a date or NULL. The only way `v_stichtag` becomes NULL is if `p_stichtag` is NULL *and* `v_sysdate` is NULL, which is impossible for `CURRENT_DATE()`.

Let's refine this test to match the legacy script's `pruefeParameterGesetzt Stichtag p_stichtag` which checks if `p_stichtag` is empty. In the BigQuery SP, `v_stichtag` is set to `IFNULL(p_stichtag, v_sysdate)`. So, `v_stichtag` will *never* be NULL if `CURRENT_DATE()` works. This means the `IF v_stichtag IS NULL` check in the BigQuery SP is effectively unreachable under normal circumstances.

**Revised Test Case 3: Error Path - Kernel SP Raises an Error**

**Purpose:** Verify that the wrapper correctly handles an error originating from the invoked kernel stored procedure, logs the error, updates the job status to 'ERROR', and re-raises the exception. This covers external-system replacements for error handling.

**Setup:**
1.  Run the cleanup script.
2.  Modify the `k_ausd_bp_ta_msisdn_his` stub to raise an exception.

```sql
-- Temporarily modify the kernel stub to simulate an error
CREATE OR REPLACE PROCEDURE my_project.my_dataset.k_ausd_bp_ta_msisdn_his(
    job_kennung STRING,
    stichtag DATE,
    job_entry_nr INT64,
    wiederanlaufWert INT64
)
BEGIN
    INSERT INTO my_project.my_dataset.job_message_log (job_name, job_entry_nr, message_text, created_at)
    VALUES (
        job_kennung,
        job_entry_nr,
        FORMAT('Kernel script k_ausd_bp_ta_msisdn_his called with Stichtag: %s, Wiederanlaufwert: %d (and will fail)', FORMAT_DATE('%Y-%m-%d', stichtag), wiederanlaufWert),
        CURRENT_TIMESTAMP()
    );
    RAISE EXCEPTION 'Simulated error in kernel script: Data processing failed.';
END;
```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.

```sql
CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(DATE '2023-01-15', 12345);
```

**Pass/Fail Criterion:**
1.  **Execution**: The `CALL` statement should fail and return an error message (e.g., "Simulated error in kernel script: Data processing failed.").
2.  **`job_registry`**: Exactly one row exists with `job_name = 'AUSD_BP_TA_MSISDN_HIS'`, `script_name = 'r_ausd_bp_ta_msisdn_his_wrapper'`, `stichtag = '2023-01-15'`, and `status = 'ERROR'`. `created_at` and `finished_at` should be populated.
3.  **`job_message_log`**:
    *   Exactly two rows exist for the `job_entry_nr` from `job_registry`.
    *   One message indicating job start.
    *   One message from the kernel stub (before it raises the error).
    *   No job completion message.
4.  **`job_error_log`**: Exactly one row exists for the `job_entry_nr` from `job_registry`, with `error_message` containing "Simulated error in kernel script: Data processing failed." or similar.

**Runnable Test Code (SQL Assertions):**

```sql
-- Assert job_registry entry
SELECT
    COUNT(1) AS row_count,
    MAX(CASE WHEN job_name = 'AUSD_BP_TA_MSISDN_HIS' AND script_name = 'r_ausd_bp_ta_msisdn_his_wrapper' AND stichtag = DATE '2023-01-15' AND status = 'ERROR' AND created_at IS NOT NULL AND finished_at IS NOT NULL THEN 1 ELSE 0 END) AS is_correct_entry
FROM my_project.my_dataset.job_registry;
-- Expected: row_count = 1, is_correct_entry = 1

-- Get the job_entry_nr for subsequent checks
DECLARE v_job_entry_nr INT64;
SET v_job_entry_nr = (SELECT job_entry_nr FROM my_project.my_dataset.job_registry WHERE job_name = 'AUSD_BP_TA_MSISDN_HIS' AND stichtag = DATE '2023-01-15');

-- Assert job_message_log entries
SELECT
    COUNT(1) AS row_count,
    MAX(CASE WHEN message_text LIKE '%Job AUSD_BP_TA_MSISDN_HIS (Version 1.0) started for Stichtag 2023-01-15 with Wiederanlaufwert 12345%' THEN 1 ELSE 0 END) AS has_start_message,
    MAX(CASE WHEN message_text LIKE '%Kernel script k_ausd_bp_ta_msisdn_his called with Stichtag: 2023-01-15, Wiederanlaufwert: 12345 (and will fail)%' THEN 1 ELSE 0 END) AS has_kernel_message,
    MAX(CASE WHEN message_text LIKE '%Job AUSD_BP_TA_MSISDN_HIS completed successfully%' THEN 1 ELSE 0 END) AS has_completion_message
FROM my_project.my_dataset.job_message_log
WHERE job_entry_nr = v_job_entry_nr;
-- Expected: row_count = 2, has_start_message = 1, has_kernel_message = 1, has_completion_message = 0

-- Assert job_error_log entry
SELECT
    COUNT(1) AS row_count,
    MAX(CASE WHEN error_message LIKE '%Simulated error in kernel script: Data processing failed.%' THEN 1 ELSE 0 END) AS has_correct_error_message
FROM my_project.my_dataset.job_error_log
WHERE job_entry_nr = v_job_entry_nr;
-- Expected: row_count = 1, has_correct_error_message = 1

-- Revert the kernel stub after testing
CREATE OR REPLACE PROCEDURE my_project.my_dataset.k_ausd_bp_ta_msisdn_his(
    job_kennung STRING,
    stichtag DATE,
    job_entry_nr INT64,
    wiederanlaufWert INT64
)
BEGIN
    INSERT INTO my_project.my_dataset.job_message_log (job_name, job_entry_nr, message_text, created_at)
    VALUES (
        job_kennung,
        job_entry_nr,
        FORMAT('Kernel script k_ausd_bp_ta_msisdn_his called with Stichtag: %s, Wiederanlaufwert: %d', FORMAT_DATE('%Y-%m-%d', stichtag), wiederanlaufWert),
        CURRENT_TIMESTAMP()
    );
END;
```

---

## Test Case 4: Multiple Concurrent Runs (Job Entry Number Uniqueness)

**Purpose:** Verify that `DW_EintragsNr` is unique for each job run, even if executed in quick succession, ensuring proper isolation and tracking of individual job instances. This covers data quality and schema assertions for the `job_registry` and `job_message_log`.

**Setup:**
1.  Run the cleanup script.

**Action:**
Execute the BigQuery Stored Procedure multiple times in quick succession (e.g., within a few milliseconds if possible, or simply back-to-back).

```sql
-- Execute multiple times
CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(DATE '2023-01-01', 100);
CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(DATE '2023-01-02', 200);
CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(DATE '2023-01-03', 300);
```

**Pass/Fail Criterion:**
1.  **`job_registry`**: Exactly three rows exist, each with a unique `job_entry_nr`. All three should have `status = 'OK'`.
2.  **`job_message_log`**: Exactly nine rows exist (3 runs * 3 messages per run), and each set of three messages should correspond to a unique `job_entry_nr` from the `job_registry`.
3.  **`job_error_log`**: Zero rows exist.

**Runnable Test Code (SQL Assertions):**

```sql
-- Assert job_registry entries
SELECT
    COUNT(1) AS total_runs,
    COUNT(DISTINCT job_entry_nr) AS unique_job_entries,
    COUNTIF(status = 'OK') AS successful_runs
FROM my_project.my_dataset.job_registry;
-- Expected: total_runs = 3, unique_job_entries = 3, successful_runs = 3

-- Assert job_message_log entries
SELECT
    COUNT(1) AS total_messages,
    COUNT(DISTINCT job_entry_nr) AS unique_job_entries_in_messages
FROM my_project.my_dataset.job_message_log;
-- Expected: total_messages = 9, unique_job_entries_in_messages = 3

-- Assert job_error_log is empty
SELECT COUNT(1) AS row_count FROM my_project.my_dataset.job_error_log;
-- Expected: row_count = 0
```