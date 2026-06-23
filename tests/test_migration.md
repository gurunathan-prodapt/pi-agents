The migration of `r_ausd_v_ta_discount.ksh` to a BigQuery Stored Procedure `project.dataset.sp_vertragsdatenabgleich_ta_discount` involves a shift from a shell-based orchestration to a SQL-native one. The primary focus of these tests is to ensure the BigQuery stored procedure correctly handles job orchestration, parameter processing, logging, and error management as specified in the migration design, reflecting the *intended* behavior and the new BigQuery architecture.

A key observation from the design document and the provided KSH script is a change in the interface:
*   The KSH script internally defines `JobKennung` and `v_sysdate` and only accepts `-h` as a command-line parameter.
*   The BigQuery stored procedure `sp_vertragsdatenabgleich_ta_discount` expects `p_job_identifier`, `p_stichtag`, and `p_logfile_suffix` as input parameters. This represents an enhancement or re-design of the job's external interface, which these tests will validate against the *new design*.

**Pre-requisites for Running Tests:**

1.  **BigQuery Environment:** Access to a BigQuery project and dataset (replace `project.dataset` with your actual project and dataset names).
2.  **Audit Tables:** The `job_control` and `job_error_log` tables must be created as per the DDL in the migration design.
3.  **Stored Procedures:**
    *   `project.dataset.sp_vertragsdatenabgleich_ta_discount`
    *   `project.dataset.sp_log_error`
    *   `project.dataset.sp_k_ausd_v_ta_discount` (the placeholder for core logic)
    must be created. For testing error scenarios, `sp_k_ausd_v_ta_discount` will be temporarily replaced with a version that raises an exception.

**Helper Procedures for Testing:**

To ensure test isolation and facilitate error simulation, the following helper procedures are recommended:

```sql
-- Helper to clear audit tables before each test
CREATE OR REPLACE PROCEDURE project.dataset.clear_audit_tables()
BEGIN
  DELETE FROM project.dataset.job_control WHERE TRUE;
  DELETE FROM project.dataset.job_error_log WHERE TRUE;
END;

-- Helper to simulate core logic failure for testing
CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount_fail(
  IN job_kennung STRING,
  IN entry_nr INT64
)
BEGIN
  RAISE BQ_EXCEPTION 'Simulated error from core logic for testing purposes.';
END;

-- Helper to simulate core logic success (original placeholder)
CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount_success(
  IN job_kennung STRING,
  IN entry_nr INT64
)
BEGIN
  -- Simulate successful operation
  SELECT 'Core logic executed successfully' AS status_message;
END;
```

---

## Test Case 1.1: Successful Execution - Default Parameters

**Purpose:** Verify the happy path where the job runs successfully with default `stichtag_info` and a basic job identifier. This tests the core orchestration flow, including job control entry creation and status update.

**Setup:**
1.  Clear audit tables:
    ```sql
    CALL project.dataset.clear_audit_tables();
    ```
2.  Ensure the core logic placeholder simulates success:
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount(
      IN job_kennung STRING,
      IN entry_nr INT64
    ) AS (project.dataset.sp_k_ausd_v_ta_discount_success(job_kennung, entry_nr));
    ```

**Action:**
Execute the main stored procedure with a job identifier, letting `p_stichtag` and `p_logfile_suffix` default.

```sql
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
  p_job_identifier => 'TEST_JOB_DEFAULT_PARAMS'
);
```

**Pass/Fail Criterion:**
*   One row exists in `project.dataset.job_control`.
*   `job_control.job_kennung` is `'TEST_JOB_DEFAULT_PARAMS'`.
*   `job_control.status` is `'OK'`.
*   `job_control.entry_nr` is `1`.
*   `job_control.stichtag_info` is `FORMAT_DATE('%Y%m%d', CURRENT_DATE())` (current date in YYYYMMDD format).
*   `job_control.log_file` is `'r_ausd_v_ta_discount_.log'` (empty suffix).
*   `job_control.created_ts` and `job_control.finished_ts` are populated, with `finished_ts` being after `created_ts`.
*   Zero rows exist in `project.dataset.job_error_log`.

```sql
-- Pytest-style assertion (conceptual)
# assert_query_result("""
#   SELECT job_kennung, status, entry_nr, stichtag_info, log_file
#   FROM project.dataset.job_control
# """, [('TEST_JOB_DEFAULT_PARAMS', 'OK', 1, FORMAT_DATE('%Y%m%d', CURRENT_DATE()), 'r_ausd_v_ta_discount_.log')])
# assert_query_result("SELECT COUNT(*) FROM project.dataset.job_error_log", [(0,)])

-- SQL Assertion
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM project.dataset.job_control) = 1
     AND (SELECT job_kennung FROM project.dataset.job_control) = 'TEST_JOB_DEFAULT_PARAMS'
     AND (SELECT status FROM project.dataset.job_control) = 'OK'
     AND (SELECT entry_nr FROM project.dataset.job_control) = 1
     AND (SELECT stichtag_info FROM project.dataset.job_control) = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
     AND (SELECT log_file FROM project.dataset.job_control) = 'r_ausd_v_ta_discount_.log'
     AND (SELECT created_ts IS NOT NULL AND finished_ts IS NOT NULL AND finished_ts >= created_ts FROM project.dataset.job_control)
     AND (SELECT COUNT(*) FROM project.dataset.job_error_log) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

## Test Case 1.2: Successful Execution - Custom Parameters

**Purpose:** Verify successful execution when all optional parameters (`p_stichtag`, `p_logfile_suffix`) are provided, and `p_job_identifier` is handled correctly.

**Setup:**
1.  Clear audit tables:
    ```sql
    CALL project.dataset.clear_audit_tables();
    ```
2.  Ensure the core logic placeholder simulates success:
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount(
      IN job_kennung STRING,
      IN entry_nr INT64
    ) AS (project.dataset.sp_k_ausd_v_ta_discount_success(job_kennung, entry_nr));
    ```

**Action:**
Execute the main stored procedure with custom values for all parameters.

```sql
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
  p_job_identifier => 'MyCustomJob',
  p_stichtag => '20231231',
  p_logfile_suffix => 'DAILY_RUN'
);
```

**Pass/Fail Criterion:**
*   One row exists in `project.dataset.job_control`.
*   `job_control.job_kennung` is `'MYCUSTOMJOB'` (uppercase conversion).
*   `job_control.status` is `'OK'`.
*   `job_control.entry_nr` is `1`.
*   `job_control.stichtag_info` is `'20231231'`.
*   `job_control.log_file` is `'r_ausd_v_ta_discount_DAILY_RUN.log'`.
*   `job_control.created_ts` and `job_control.finished_ts` are populated, with `finished_ts` being after `created_ts`.
*   Zero rows exist in `project.dataset.job_error_log`.

```sql
-- SQL Assertion
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM project.dataset.job_control) = 1
     AND (SELECT job_kennung FROM project.dataset.job_control) = 'MYCUSTOMJOB'
     AND (SELECT status FROM project.dataset.job_control) = 'OK'
     AND (SELECT entry_nr FROM project.dataset.job_control) = 1
     AND (SELECT stichtag_info FROM project.dataset.job_control) = '20231231'
     AND (SELECT log_file FROM project.dataset.job_control) = 'r_ausd_v_ta_discount_DAILY_RUN.log'
     AND (SELECT created_ts IS NOT NULL AND finished_ts IS NOT NULL AND finished_ts >= created_ts FROM project.dataset.job_control)
     AND (SELECT COUNT(*) FROM project.dataset.job_error_log) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

## Test Case 1.3: Help Message Display (`-h` equivalent)

**Purpose:** Verify that calling the stored procedure with `p_help => TRUE` displays the usage information and does not execute any job logic or write to audit tables. This mirrors the KSH `-h` behavior.

**Setup:**
1.  Clear audit tables:
    ```sql
    CALL project.dataset.clear_audit_tables();
    ```
2.  Ensure the core logic placeholder simulates success (though it shouldn't be called):
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount(
      IN job_kennung STRING,
      IN entry_nr INT64
    ) AS (project.dataset.sp_k_ausd_v_ta_discount_success(job_kennung, entry_nr));
    ```

**Action:**
Execute the main stored procedure with `p_help` set to `TRUE`.

```sql
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
  p_job_identifier => 'HELP_TEST', -- Mandatory parameter still needed for syntax, but logic should exit before validation
  p_help => TRUE
);
```

**Pass/Fail Criterion:**
*   The BigQuery console output (or client output) contains the usage instructions defined in the `IF p_help THEN` block.
*   Zero rows exist in `project.dataset.job_control`.
*   Zero rows exist in `project.dataset.job_error_log`.

```sql
-- SQL Assertion (for audit tables, console output needs manual verification)
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM project.dataset.job_control) = 0
     AND (SELECT COUNT(*) FROM project.dataset.job_error_log) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;

-- Expected console output (example):
-- Usage: CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(p_stichtag => <stichtag>, p_logfile_suffix => <suffix>, p_job_identifier => <jobkennung>, p_help => TRUE/FALSE)
--   p_stichtag: Reference date (e.g., YYYYMMDD).
--   p_logfile_suffix: Suffix for log file (optional).
--   p_job_identifier: Unique job identifier.
--   p_help: Display this help message.
```

---

## Test Case 1.4: Missing Mandatory Parameter (`p_job_identifier`)

**Purpose:** Verify that the stored procedure correctly identifies and handles a missing mandatory parameter (`p_job_identifier`), logs the error, and raises an exception. This replaces the KSH `if [ ! $ErrNr -eq 0 ]` block for parameter validation.

**Setup:**
1.  Clear audit tables:
    ```sql
    CALL project.dataset.clear_audit_tables();
    ```
2.  Ensure the core logic placeholder simulates success (it should not be called):
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount(
      IN job_kennung STRING,
      IN entry_nr INT64
    ) AS (project.dataset.sp_k_ausd_v_ta_discount_success(job_kennung, entry_nr));
    ```

**Action:**
Attempt to execute the main stored procedure with `p_job_identifier` set to `NULL`. This call is expected to fail.

```sql
-- This call is expected to raise an exception.
-- The exact way to execute and catch this in a test framework depends on the client.
-- In BigQuery console, it will show as an error.
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
  p_job_identifier => NULL
);
```

**Pass/Fail Criterion:**
*   The stored procedure execution fails and raises an exception with a message indicating a missing job identifier.
*   One row exists in `project.dataset.job_error_log`.
*   `job_error_log.error_message` contains `'Job identifier (-j) is a mandatory parameter.'`.
*   `job_error_log.error_nr` is `1`.
*   `job_error_log.error_arg` is `'Missing Parameter'`.
*   Zero rows exist in `project.dataset.job_control` (because the error occurs before the transaction to insert into `job_control` starts).

```sql
-- SQL Assertion (for audit tables, procedure failure needs external verification)
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM project.dataset.job_control) = 0
     AND (SELECT COUNT(*) FROM project.dataset.job_error_log) = 1
     AND (SELECT error_message FROM project.dataset.job_error_log) = 'Job identifier (-j) is a mandatory parameter.'
     AND (SELECT error_nr FROM project.dataset.job_error_log) = 1
     AND (SELECT error_arg FROM project.dataset.job_error_log) = 'Missing Parameter'
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

## Test Case 1.5: Core Script Failure Simulation

**Purpose:** Verify the error handling mechanism when the invoked core processing script (`sp_k_ausd_v_ta_discount`) fails. This tests the `EXCEPTION WHEN ERROR` block and its interaction with `job_control` and `job_error_log`, replacing the KSH `trap ERR` behavior.

**Setup:**
1.  Clear audit tables:
    ```sql
    CALL project.dataset.clear_audit_tables();
    ```
2.  Modify the core logic placeholder to simulate a failure:
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount(
      IN job_kennung STRING,
      IN entry_nr INT64
    ) AS (project.dataset.sp_k_ausd_v_ta_discount_fail(job_kennung, entry_nr));
    ```

**Action:**
Execute the main stored procedure with valid parameters. This call is expected to fail due to the simulated error in the core script.

```sql
-- This call is expected to raise an exception.
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
  p_job_identifier => 'CORE_FAILURE_TEST'
);
```

**Pass/Fail Criterion:**
*   The stored procedure execution fails and raises an exception originating from the core script.
*   One row exists in `project.dataset.job_control`.
*   `job_control.job_kennung` is `'CORE_FAILURE_TEST'`.
*   `job_control.status` is `'ERROR'`.
*   `job_control.entry_nr` is `1`.
*   `job_control.created_ts` and `job_control.finished_ts` are populated.
*   One row exists in `project.dataset.job_error_log`.
*   `job_error_log.job_kennung` is `'CORE_FAILURE_TEST'`.
*   `job_error_log.entry_nr` is `1`.
*   `job_error_log.error_message` contains `'Simulated error from core logic for testing purposes.'`.
*   `job_error_log.error_nr` is `2` (generic SQL error).
*   `job_error_log.error_arg` is `'BigQuery Error'`.

```sql
-- SQL Assertion (for audit tables, procedure failure needs external verification)
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM project.dataset.job_control) = 1
     AND (SELECT job_kennung FROM project.dataset.job_control) = 'CORE_FAILURE_TEST'
     AND (SELECT status FROM project.dataset.job_control) = 'ERROR'
     AND (SELECT entry_nr FROM project.dataset.job_control) = 1
     AND (SELECT created_ts IS NOT NULL AND finished_ts IS NOT NULL FROM project.dataset.job_control)
     AND (SELECT COUNT(*) FROM project.dataset.job_error_log) = 1
     AND (SELECT job_kennung FROM project.dataset.job_error_log) = 'CORE_FAILURE_TEST'
     AND (SELECT entry_nr FROM project.dataset.job_error_log) = 1
     AND (SELECT error_message LIKE '%Simulated error from core logic%' FROM project.dataset.job_error_log)
     AND (SELECT error_nr FROM project.dataset.job_error_log) = 2
     AND (SELECT error_arg FROM project.dataset.job_error_log) = 'BigQuery Error'
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

## Test Case 1.6: Multiple Runs - Entry Number Increment

**Purpose:** Verify that the `entry_nr` in `job_control` correctly increments for subsequent runs of the same job identifier, mimicking the `DWMSG_ErmittleNr` behavior.

**Setup:**
1.  Clear audit tables:
    ```sql
    CALL project.dataset.clear_audit_tables();
    ```
2.  Ensure the core logic placeholder simulates success:
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount(
      IN job_kennung STRING,
      IN entry_nr INT64
    ) AS (project.dataset.sp_k_ausd_v_ta_discount_success(job_kennung, entry_nr));
    ```

**Action:**
Execute the main stored procedure three times with the same `p_job_identifier`.

```sql
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(p_job_identifier => 'INCREMENT_TEST_JOB');
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(p_job_identifier => 'INCREMENT_TEST_JOB');
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(p_job_identifier => 'INCREMENT_TEST_JOB');
```

**Pass/Fail Criterion:**
*   Three rows exist in `project.dataset.job_control`.
*   All three rows have `job_kennung` as `'INCREMENT_TEST_JOB'` and `status` as `'OK'`.
*   The `entry_nr` values for these rows are `1`, `2`, and `3` respectively.
*   Zero rows exist in `project.dataset.job_error_log`.

```sql
-- SQL Assertion
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM project.dataset.job_control WHERE job_kennung = 'INCREMENT_TEST_JOB' AND status = 'OK') = 3
     AND (SELECT ARRAY_AGG(entry_nr ORDER BY entry_nr) FROM project.dataset.job_control WHERE job_kennung = 'INCREMENT_TEST_JOB') = [1, 2, 3]
     AND (SELECT COUNT(*) FROM project.dataset.job_error_log) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

## Test Case 2.1: `typeset -u JobKennung` (Uppercase Conversion)

**Purpose:** Verify that the `p_job_identifier` input is consistently converted to uppercase before being stored in `job_control.job_kennung` and passed to the core script. This replaces the KSH `typeset -u` behavior.

**Setup:**
1.  Clear audit tables:
    ```sql
    CALL project.dataset.clear_audit_tables();
    ```
2.  Ensure the core logic placeholder simulates success:
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount(
      IN job_kennung STRING,
      IN entry_nr INT64
    ) AS (project.dataset.sp_k_ausd_v_ta_discount_success(job_kennung, entry_nr));
    ```

**Action:**
Execute the main stored procedure with a mixed-case `p_job_identifier`.

```sql
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
  p_job_identifier => 'MiXeD_CaSe_JoB'
);
```

**Pass/Fail Criterion:**
*   One row exists in `project.dataset.job_control`.
*   `job_control.job_kennung` is `'MIXED_CASE_JOB'`.
*   `job_control.status` is `'OK'`.

```sql
-- SQL Assertion
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM project.dataset.job_control) = 1
     AND (SELECT job_kennung FROM project.dataset.job_control) = 'MIXED_CASE_JOB'
     AND (SELECT status FROM project.dataset.job_control) = 'OK'
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

## Test Case 2.2: `date +%d%m%Y` (Date Formatting for `stichtag_info`)

**Purpose:** Verify that `stichtag_info` is correctly populated, either from `p_stichtag` or defaulting to the current date in `YYYYMMDD` format when `p_stichtag` is `NULL`. This replaces the KSH `date +%d%m%Y` and `DWMSG_SetzeStichtagInfo` logic.

**Setup:**
1.  Clear audit tables:
    ```sql
    CALL project.dataset.clear_audit_tables();
    ```
2.  Ensure the core logic placeholder simulates success:
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_discount(
      IN job_kennung STRING,
      IN entry_nr INT64
    ) AS (project.dataset.sp_k_ausd_v_ta_discount_success(job_kennung, entry_nr));
    ```

**Action:**
Execute the main stored procedure twice: once with a specific `p_stichtag` and once with `p_stichtag => NULL`.

```sql
CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
  p_job_identifier => 'DATE_TEST_SPECIFIC',
  p_stichtag => '20240115'
);

CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
  p_job_identifier => 'DATE_TEST_DEFAULT',
  p_stichtag => NULL
);
```

**Pass/Fail Criterion:**
*   Two rows exist in `project.dataset.job_control`, both with `status` `'OK'`.
*   The row for `DATE_TEST_SPECIFIC` has `stichtag_info` as `'20240115'`.
*   The row for `DATE_TEST_DEFAULT` has `stichtag_info` as `FORMAT_DATE('%Y%m%d', CURRENT_DATE())`.

```sql
-- SQL Assertion
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM project.dataset.job_control WHERE job_kennung = 'DATE_TEST_SPECIFIC' AND stichtag_info = '20240115' AND status = 'OK') = 1
     AND (SELECT COUNT(*) FROM project.dataset.job_control WHERE job_kennung = 'DATE_TEST_DEFAULT' AND stichtag_info = FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AND status = 'OK') = 1
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;
```

---

## Test Case 3.1: `job_control` Schema and Data Types

**Purpose:** Verify that the `job_control` table schema matches the design document's specification, ensuring correct data types and column names.

**Setup:**
No specific setup required beyond the table being created.

**Action:**
Query the BigQuery `INFORMATION_SCHEMA` for the `job_control` table.

```sql
SELECT
  column_name,
  data_type
FROM
  project.dataset.INFORMATION_SCHEMA.COLUMNS
WHERE
  table_name = 'job_control'
ORDER BY
  ordinal_position;
```

**Pass/Fail Criterion:**
The query result matches the expected schema:

| column_name  | data_type |
| :----------- | :-------- |
| entry_nr     | INT64     |
| job_kennung  | STRING    |
| script_name  | STRING    |
| log_file     | STRING    |
| status       | STRING    |
| stichtag_info| STRING    |
| created_ts   | TIMESTAMP |
| finished_ts  | TIMESTAMP |

```sql
-- SQL Assertion (conceptual, typically done via schema introspection tools)
-- This would involve comparing the query result to a predefined expected structure.
-- For example, in Python with BigQuery client:
# schema = client.get_table('project.dataset.job_control').schema
# expected_schema = [
#     SchemaField('entry_nr', 'INT64'),
#     SchemaField('job_kennung', 'STRING'),
#     # ... etc.
# ]
# assert schema == expected_schema
```

---

## Test Case 3.2: `job_error_log` Schema and Data Types

**Purpose:** Verify that the `job_error_log` table schema matches the design document's specification, ensuring correct data types and column names.

**Setup:**
No specific setup required beyond the table being created.

**Action:**
Query the BigQuery `INFORMATION_SCHEMA` for the `job_error_log` table.

```sql
SELECT
  column_name,
  data_type
FROM
  project.dataset.INFORMATION_SCHEMA.COLUMNS
WHERE
  table_name = 'job_error_log'
ORDER BY
  ordinal_position;
```

**Pass/Fail Criterion:**
The query result matches the expected schema:

| column_name   | data_type |
| :------------ | :-------- |
| job_kennung   | STRING    |
| entry_nr      | INT64     |
| error_nr      | INT64     |
| error_arg     | STRING    |
| error_message | STRING    |
| created_ts    | TIMESTAMP |

```sql
-- SQL Assertion (conceptual, typically done via schema introspection tools)
-- Similar to Test Case 3.1, compare the query result to a predefined expected structure.
```