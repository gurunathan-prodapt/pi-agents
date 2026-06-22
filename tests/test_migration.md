The migration of `r_ausd_bp_ta_bpr_apn.ksh` to BigQuery involves transforming a KornShell orchestration script and its core data processing logic into BigQuery Stored Procedures. The following tests are designed to ensure the migrated solution is behaviourally equivalent to the legacy system.

---

## Test Case 1: Schema Validation

**Purpose:** To verify that the DDL for the target BigQuery tables (`job_audit`, `contract_cache`, `fos_table`) matches the expected schema and data types, including primary keys and descriptions. This ensures data integrity and compatibility with the migrated logic.

**Setup:**
1.  Ensure the BigQuery dataset `my_project.my_dataset` exists.
2.  Execute the DDL scripts for `job_audit.sql`, `contract_cache.sql`, and `fos_table.sql`.

**Action:**
Query the BigQuery `INFORMATION_SCHEMA` to retrieve the schema details for each created table.

**Pass/Fail Criterion:**
*   **Pass:** The schema of `my_project.my_dataset.job_audit`, `my_project.my_dataset.contract_cache`, and `my_project.my_dataset.fos_table` precisely matches the DDL provided in the migration design, including column names, data types, nullability, and primary key definitions.
*   **Fail:** Any discrepancy in column names, data types, nullability, or primary key constraints.

**Runnable Test Code (SQL Assertions):**

```sql
-- Assert job_audit schema
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'job_audit'
ORDER BY
    ordinal_position;

-- Expected output (example for job_audit):
-- column_name        data_type   is_nullable
-- job_entry_number   INT64       NO
-- job_name           STRING      NO
-- script_name        STRING      YES
-- start_timestamp    TIMESTAMP   YES
-- end_timestamp      TIMESTAMP   YES
-- status             STRING      YES
-- message            STRING      YES
-- stichtag           DATE        YES
-- wiederanlaufwert   INT64       YES
-- sysdate_at_run     DATE        YES
-- log_file_name      STRING      YES

-- Assert contract_cache schema
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'contract_cache'
ORDER BY
    ordinal_position;

-- Assert fos_table schema
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'fos_table'
ORDER BY
    ordinal_position;

-- Verify Primary Keys (example for fos_table)
SELECT
    constraint_name,
    column_name
FROM
    `my_project.my_dataset.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
WHERE
    table_name = 'fos_table' AND constraint_name LIKE '%PRIMARY_KEY%'
ORDER BY
    ordinal_position;
-- Expected output for fos_table primary key:
-- constraint_name    column_name
-- pk_fos_table       dwh_vertrag_id
-- pk_fos_table       stichtag_lauf
```

---

## Test Case 2: Successful Run - All Parameters Provided

**Purpose:** To verify that the migrated job executes successfully when both `Stichtag` and `Wiederanlaufwert` are explicitly provided, producing the correct output in `fos_table` and accurate audit logs. This covers output parity and transformation correctness for a common scenario.

**Setup:**
1.  Clear `my_project.my_dataset.job_audit` and `my_project.my_dataset.fos_table`.
2.  Insert sample data into `my_project.my_dataset.contract_cache`.
    *   `dwh_vertrag_id`: 101, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2023-12-31', `ladedatum`: '2023-03-01', `col_a`: 'A1', `col_b`: 'B1'
    *   `dwh_vertrag_id`: 102, `gueltig_von`: '2023-06-01', `gueltig_bis`: '2024-06-30', `ladedatum`: '2023-05-01', `col_a`: 'A2', `col_b`: 'B2'
    *   `dwh_vertrag_id`: 103, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2023-04-30', `ladedatum`: '2023-02-01', `col_a`: 'A3', `col_b`: 'B3' (will be filtered out by `Stichtag < gueltig_bis`)
    *   `dwh_vertrag_id`: 104, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-07-01', `col_a`: 'A4', `col_b`: 'B4' (will be filtered out by `ladedatum < Stichtag`)
    *   `dwh_vertrag_id`: 105, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-03-01', `col_a`: 'A5', `col_b`: 'B5'

**Action:**
Execute the main stored procedure with specific parameters:
`CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('01072023', 102);`
(Here, `Stichtag` is '2023-07-01', `Wiederanlaufwert` is 102)

**Pass/Fail Criterion:**
*   **Pass:**
    1.  `my_project.my_dataset.job_audit` contains one entry with `status = 'SUCCESS'`, `stichtag = '2023-07-01'`, `wiederanlaufwert = 102`. The message should indicate successful completion and include row counts from `k_ausd_bp_ta_bpr_apn`.
    2.  `my_project.my_dataset.fos_table` contains exactly 1 row:
        *   `dwh_vertrag_id`: 105, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-03-01', `col_a`: 'A5', `col_b`: 'B5', `stichtag_lauf`: '2023-07-01', `created_ts`: (any timestamp).
        *   (Rows 101, 102 filtered by `dwh_vertrag_id > 102`; 103 by `gueltig_bis`; 104 by `ladedatum`).
*   **Fail:** Any deviation in audit log status, parameter values, or `fos_table` content/row count.

**Runnable Test Code (SQL Assertions):**

```sql
-- Setup: Insert sample data into contract_cache
TRUNCATE TABLE `my_project.my_dataset.contract_cache`;
INSERT INTO `my_project.my_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col_a, col_b) VALUES
(101, '2023-01-01', '2023-12-31', '2023-03-01', 'A1', 'B1'),
(102, '2023-06-01', '2024-06-30', '2023-05-01', 'A2', 'B2'),
(103, '2023-01-01', '2023-04-30', '2023-02-01', 'A3', 'B3'), -- Stichtag 2023-07-01 > gueltig_bis
(104, '2023-01-01', '2024-12-31', '2023-07-01', 'A4', 'B4'), -- ladedatum 2023-07-01 not < Stichtag 2023-07-01
(105, '2023-01-01', '2024-12-31', '2023-03-01', 'A5', 'B5');

TRUNCATE TABLE `my_project.my_dataset.fos_table`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;

-- Action: Execute the stored procedure
CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn`('01072023', 102);

-- Assertion 1: Check job_audit table
SELECT
    status,
    stichtag,
    wiederanlaufwert,
    message LIKE '%SUCCESS%' AS message_success,
    message LIKE '%Inserted rows: 1%' AS message_inserted_count,
    message LIKE '%Deleted rows: 0%' AS message_deleted_count -- No prior data to delete in fos_table
FROM
    `my_project.my_dataset.job_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_apn'
ORDER BY start_timestamp DESC
LIMIT 1;
-- Expected: status='SUCCESS', stichtag='2023-07-01', wiederanlaufwert=102, message_success=TRUE, message_inserted_count=TRUE, message_deleted_count=TRUE

-- Assertion 2: Check fos_table content
SELECT
    dwh_vertrag_id,
    gueltig_von,
    gueltig_bis,
    ladedatum,
    col_a,
    col_b,
    stichtag_lauf
FROM
    `my_project.my_dataset.fos_table`;
-- Expected:
-- dwh_vertrag_id | gueltig_von | gueltig_bis | ladedatum | col_a | col_b | stichtag_lauf
-- -------------- | ----------- | ----------- | --------- | ----- | ----- | -------------
-- 105            | 2023-01-01  | 2024-12-31  | 2023-03-01| A5    | B5    | 2023-07-01
```

---

## Test Case 3: Successful Run - Stichtag Defaulting

**Purpose:** To verify that `Stichtag` defaults to the current system date when not provided, and `Wiederanlaufwert` defaults to 0, with correct data processing and audit logging. This covers parameter handling and defaulting.

**Setup:**
1.  Clear `my_project.my_dataset.job_audit` and `my_project.my_dataset.fos_table`.
2.  Insert sample data into `my_project.my_dataset.contract_cache`.
    *   `dwh_vertrag_id`: 201, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-03-01', `col_a`: 'X1', `col_b`: 'Y1'
    *   `dwh_vertrag_id`: 202, `gueltig_von`: '2023-06-01', `gueltig_bis`: '2024-06-30', `ladedatum`: '2023-05-01', `col_a`: 'X2', `col_b`: 'Y2'
    *   `dwh_vertrag_id`: 203, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2023-04-30', `ladedatum`: '2023-02-01', `col_a`: 'X3', `col_b`: 'Y3' (will be filtered out if `Stichtag` is current date > '2023-04-30')

**Action:**
Execute the main stored procedure without `Stichtag` and `Wiederanlaufwert`:
`CALL my_project.my_dataset.ausd_bp_ta_bpr_apn(NULL, NULL);`
Assume `CURRENT_DATE()` for this test is '2023-07-15'.

**Pass/Fail Criterion:**
*   **Pass:**
    1.  `my_project.my_dataset.job_audit` contains one entry with `status = 'SUCCESS'`, `stichtag = '2023-07-15'` (or `CURRENT_DATE()`), `wiederanlaufwert = 0`. The message should indicate successful completion and include row counts.
    2.  `my_project.my_dataset.fos_table` contains exactly 2 rows:
        *   `dwh_vertrag_id`: 201, `stichtag_lauf`: '2023-07-15'
        *   `dwh_vertrag_id`: 202, `stichtag_lauf`: '2023-07-15'
        *   (Row 203 filtered out by `gueltig_bis`).
*   **Fail:** Incorrect defaulting, audit log status, or `fos_table` content/row count.

**Runnable Test Code (SQL Assertions):**

```sql
-- Setup: Insert sample data into contract_cache
TRUNCATE TABLE `my_project.my_dataset.contract_cache`;
INSERT INTO `my_project.my_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col_a, col_b) VALUES
(201, '2023-01-01', '2024-12-31', '2023-03-01', 'X1', 'Y1'),
(202, '2023-06-01', '2024-06-30', '2023-05-01', 'X2', 'Y2'),
(203, '2023-01-01', '2023-04-30', '2023-02-01', 'X3', 'Y3'); -- Will be filtered out by Stichtag > gueltig_bis

TRUNCATE TABLE `my_project.my_dataset.fos_table`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;

-- Action: Execute the stored procedure with NULL parameters
-- For deterministic testing, we might need to mock CURRENT_DATE() or use a fixed date.
-- Assuming CURRENT_DATE() is '2023-07-15' for this test.
CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn`(NULL, NULL);

-- Assertion 1: Check job_audit table
SELECT
    status,
    stichtag,
    wiederanlaufwert,
    message LIKE '%SUCCESS%' AS message_success,
    message LIKE '%Stichtag not provided, defaulting to system date.%' AS message_stichtag_default,
    message LIKE '%Inserted rows: 2%' AS message_inserted_count
FROM
    `my_project.my_dataset.job_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_apn'
ORDER BY start_timestamp DESC
LIMIT 1;
-- Expected: status='SUCCESS', stichtag='2023-07-15' (or CURRENT_DATE()), wiederanlaufwert=0, message_success=TRUE, message_stichtag_default=TRUE, message_inserted_count=TRUE

-- Assertion 2: Check fos_table content
SELECT
    dwh_vertrag_id,
    stichtag_lauf
FROM
    `my_project.my_dataset.fos_table`
ORDER BY dwh_vertrag_id;
-- Expected:
-- dwh_vertrag_id | stichtag_lauf
-- -------------- | -------------
-- 201            | 2023-07-15
-- 202            | 2023-07-15
```

---

## Test Case 4: Invalid Stichtag Format

**Purpose:** To verify that the job correctly handles an invalid `Stichtag` format, logs the error, and terminates gracefully without processing data. This covers transformation correctness (error handling) and external system replacements (audit logging).

**Setup:**
1.  Clear `my_project.my_dataset.job_audit` and `my_project.my_dataset.fos_table`.
2.  Insert some dummy data into `my_project.my_dataset.contract_cache` (it shouldn't be processed).

**Action:**
Execute the main stored procedure with an invalid `Stichtag` string:
`CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('2023-07-01', 0);` (Expected DDMMYYYY, got YYYY-MM-DD)

**Pass/Fail Criterion:**
*   **Pass:**
    1.  The stored procedure call `RAISE`s an error.
    2.  `my_project.my_dataset.job_audit` contains one entry with `status = 'FAILED'`, `stichtag = NULL`, `wiederanlaufwert = 0`. The `message` field should clearly indicate an invalid `Stichtag` format.
    3.  `my_project.my_dataset.fos_table` remains empty.
*   **Fail:** The procedure completes successfully, `fos_table` contains data, or the audit log does not reflect the failure correctly.

**Runnable Test Code (SQL Assertions):**

```sql
-- Setup: Insert dummy data (should not be processed)
TRUNCATE TABLE `my_project.my_dataset.contract_cache`;
INSERT INTO `my_project.my_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col_a, col_b) VALUES
(301, '2023-01-01', '2024-12-31', '2023-03-01', 'Z1', 'W1');

TRUNCATE TABLE `my_project.my_dataset.fos_table`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;

-- Action: Execute the stored procedure with invalid Stichtag
-- This will likely be run within a TRY-CATCH block in a testing framework
-- or observed by the orchestrator for the RAISE.
-- For direct SQL, we expect it to fail.
-- Example of how to check for error in a script:
-- SELECT
--   CASE
--     WHEN SAFE.PARSE_DATE('%d%m%Y', '2023-07-01') IS NULL THEN 'Invalid date format'
--     ELSE 'Valid date format'
--   END AS date_validation_result;
-- The SP itself will RAISE.

-- Assertion 1: Check job_audit table for failure
SELECT
    status,
    stichtag,
    wiederanlaufwert,
    message LIKE '%Invalid p_stichtag format: 2023-07-01. Expected DDMMYYYY.%' AS message_error_match
FROM
    `my_project.my_dataset.job_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_apn'
ORDER BY start_timestamp DESC
LIMIT 1;
-- Expected: status='FAILED', stichtag=NULL, wiederanlaufwert=0, message_error_match=TRUE

-- Assertion 2: Check fos_table content (should be empty)
SELECT COUNT(*) FROM `my_project.my_dataset.fos_table`;
-- Expected: 0
```

---

## Test Case 5: Wiederanlaufwert (Restart Logic) Functionality

**Purpose:** To verify the `Wiederanlaufwert` logic in `k_ausd_bp_ta_bpr_apn`, specifically that it correctly deletes existing records with `dwh_vertrag_id >= Wiederanlaufwert` and then inserts new records with `dwh_vertrag_id > Wiederanlaufwert`. This is crucial for transformation correctness and output parity in restart scenarios.

**Setup:**
1.  Clear `my_project.my_dataset.job_audit`.
2.  Populate `my_project.my_dataset.fos_table` with initial data:
    *   `dwh_vertrag_id`: 401, `stichtag_lauf`: '2023-06-01'
    *   `dwh_vertrag_id`: 402, `stichtag_lauf`: '2023-06-01'
    *   `dwh_vertrag_id`: 403, `stichtag_lauf`: '2023-06-01'
3.  Populate `my_project.my_dataset.contract_cache` with source data:
    *   `dwh_vertrag_id`: 401, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-03-01', `col_a`: 'C1', `col_b`: 'D1'
    *   `dwh_vertrag_id`: 402, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-03-01', `col_a`: 'C2', `col_b`: 'D2'
    *   `dwh_vertrag_id`: 403, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-03-01', `col_a`: 'C3', `col_b`: 'D3'
    *   `dwh_vertrag_id`: 404, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-03-01', `col_a`: 'C4', `col_b`: 'D4'
    *   `dwh_vertrag_id`: 405, `gueltig_von`: '2023-01-01', `gueltig_bis`: '2024-12-31', `ladedatum`: '2023-03-01', `col_a`: 'C5', `col_b`: 'D5'

**Action:**
Execute the main stored procedure with `Stichtag = '01072023'` and `Wiederanlaufwert = 403`:
`CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('01072023', 403);`

**Pass/Fail Criterion:**
*   **Pass:**
    1.  `my_project.my_dataset.job_audit` contains one entry with `status = 'SUCCESS'`, `stichtag = '2023-07-01'`, `wiederanlaufwert = 403`. The message should indicate `Deleted rows: 1` (for 403) and `Inserted rows: 2` (for 404, 405).
    2.  `my_project.my_dataset.fos_table` contains 4 rows:
        *   `dwh_vertrag_id`: 401, `stichtag_lauf`: '2023-06-01' (original)
        *   `dwh_vertrag_id`: 402, `stichtag_lauf`: '2023-06-01' (original)
        *   `dwh_vertrag_id`: 404, `stichtag_lauf`: '2023-07-01' (new)
        *   `dwh_vertrag_id`: 405, `stichtag_lauf`: '2023-07-01' (new)
        *   (Original 403 should be deleted, new 401, 402 should not be inserted due to `dwh_vertrag_id > 403` filter).
*   **Fail:** Incorrect deletion or insertion logic, leading to wrong row counts or data in `fos_table`.

**Runnable Test Code (SQL Assertions):**

```sql
-- Setup: Populate fos_table with initial data
TRUNCATE TABLE `my_project.my_dataset.fos_table`;
INSERT INTO `my_project.my_dataset.fos_table` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col_a, col_b, stichtag_lauf, created_ts) VALUES
(401, '2023-01-01', '2024-12-31', '2023-03-01', 'C1', 'D1', '2023-06-01', CURRENT_TIMESTAMP()),
(402, '2023-01-01', '2024-12-31', '2023-03-01', 'C2', 'D2', '2023-06-01', CURRENT_TIMESTAMP()),
(403, '2023-01-01', '2024-12-31', '2023-03-01', 'C3', 'D3', '2023-06-01', CURRENT_TIMESTAMP());

-- Setup: Populate contract_cache with source data
TRUNCATE TABLE `my_project.my_dataset.contract_cache`;
INSERT INTO `my_project.my_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col_a, col_b) VALUES
(401, '2023-01-01', '2024-12-31', '2023-03-01', 'C1', 'D1'),
(402, '2023-01-01', '2024-12-31', '2023-03-01', 'C2', 'D2'),
(403, '2023-01-01', '2024-12-31', '2023-03-01', 'C3', 'D3'),
(404, '2023-01-01', '2024-12-31', '2023-03-01', 'C4', 'D4'),
(405, '2023-01-01', '2024-12-31', '2023-03-01', 'C5', 'D5');

TRUNCATE TABLE `my_project.my_dataset.job_audit`;

-- Action: Execute the stored procedure
CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn`('01072023', 403);

-- Assertion 1: Check job_audit table
SELECT
    status,
    stichtag,
    wiederanlaufwert,
    message LIKE '%SUCCESS%' AS message_success,
    message LIKE '%Deleted rows: 1%' AS message_deleted_count,
    message LIKE '%Inserted rows: 2%' AS message_inserted_count
FROM
    `my_project.my_dataset.job_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_apn'
ORDER BY start_timestamp DESC
LIMIT 1;
-- Expected: status='SUCCESS', stichtag='2023-07-01', wiederanlaufwert=403, message_success=TRUE, message_deleted_count=TRUE, message_inserted_count=TRUE

-- Assertion 2: Check fos_table content
SELECT
    dwh_vertrag_id,
    stichtag_lauf
FROM
    `my_project.my_dataset.fos_table`
ORDER BY dwh_vertrag_id, stichtag_lauf;
-- Expected:
-- dwh_vertrag_id | stichtag_lauf
-- -------------- | -------------
-- 401            | 2023-06-01
-- 402            | 2023-06-01
-- 404            | 2023-07-01
-- 405            | 2023-07-01
```

---

## Test Case 6: Filtering Logic - Edge Cases for Dates

**Purpose:** To thoroughly test the date filtering conditions (`gueltig_von <= Stichtag < gueltig_bis` AND `ladedatum < Stichtag`) in `k_ausd_bp_ta_bpr_apn` with various edge cases. This ensures transformation correctness.

**Setup:**
1.  Clear `my_project.my_dataset.job_audit` and `my_project.my_dataset.fos_table`.
2.  Populate `my_project.my_dataset.contract_cache` with data specifically designed to test date boundaries.
    *   `Stichtag` for this test will be '2023-07-01'.
    *   `dwh_vertrag_id`: 501, `gueltig_von`: '2023-07-01', `gueltig_bis`: '2023-07-02', `ladedatum`: '2023-06-30' (Should be included: `gueltig_von <= Stichtag`, `Stichtag < gueltig_bis`, `ladedatum < Stichtag`)
    *   `dwh_vertrag_id`: 502, `gueltig_von`: '2023-06-30', `gueltig_bis`: '2023-07-01', `ladedatum`: '2023-06-29' (Should be excluded: `Stichtag < gueltig_bis` is false)
    *   `dwh_vertrag_id`: 503, `gueltig_von`: '2023-07-02', `gueltig_bis`: '2023-07-03', `ladedatum`: '2023-06-30' (Should be excluded: `gueltig_von <= Stichtag` is false)
    *   `dwh_vertrag_id`: 504, `gueltig_von`: '2023-07-01', `gueltig_bis`: '2023-07-02', `ladedatum`: '2023-07-01' (Should be excluded: `ladedatum < Stichtag` is false)
    *   `dwh_vertrag_id`: 505, `gueltig_von`: '2023-01-01', `gueltig_bis`: '9999-12-31', `ladedatum`: '2023-06-01' (Should be included: typical valid record)

**Action:**
Execute the main stored procedure:
`CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('01072023', 0);`

**Pass/Fail Criterion:**
*   **Pass:**
    1.  `my_project.my_dataset.job_audit` contains one entry with `status = 'SUCCESS'`, `stichtag = '2023-07-01'`, `wiederanlaufwert = 0`. The message should indicate `Inserted rows: 2`.
    2.  `my_project.my_dataset.fos_table` contains exactly 2 rows:
        *   `dwh_vertrag_id`: 501, `stichtag_lauf`: '2023-07-01'
        *   `dwh_vertrag_id`: 505, `stichtag_lauf`: '2023-07-01'
*   **Fail:** Any other row count or incorrect records in `fos_table`.

**Runnable Test Code (SQL Assertions):**

```sql
-- Setup: Populate contract_cache with date edge cases
TRUNCATE TABLE `my_project.my_dataset.contract_cache`;
INSERT INTO `my_project.my_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col_a, col_b) VALUES
(501, '2023-07-01', '2023-07-02', '2023-06-30', 'E1', 'F1'), -- INCLUDED
(502, '2023-06-30', '2023-07-01', '2023-06-29', 'E2', 'F2'), -- EXCLUDED: Stichtag (07-01) NOT < gueltig_bis (07-01)
(503, '2023-07-02', '2023-07-03', '2023-06-30', 'E3', 'F3'), -- EXCLUDED: gueltig_von (07-02) NOT <= Stichtag (07-01)
(504, '2023-07-01', '2023-07-02', '2023-07-01', 'E4', 'F4'), -- EXCLUDED: ladedatum (07-01) NOT < Stichtag (07-01)
(505, '2023-01-01', '9999-12-31', '2023-06-01', 'E5', 'F5'); -- INCLUDED

TRUNCATE TABLE `my_project.my_dataset.fos_table`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;

-- Action: Execute the stored procedure
CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn`('01072023', 0);

-- Assertion 1: Check job_audit table
SELECT
    status,
    stichtag,
    wiederanlaufwert,
    message LIKE '%SUCCESS%' AS message_success,
    message LIKE '%Inserted rows: 2%' AS message_inserted_count
FROM
    `my_project.my_dataset.job_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_apn'
ORDER BY start_timestamp DESC
LIMIT 1;
-- Expected: status='SUCCESS', stichtag='2023-07-01', wiederanlaufwert=0, message_success=TRUE, message_inserted_count=TRUE

-- Assertion 2: Check fos_table content
SELECT
    dwh_vertrag_id,
    stichtag_lauf
FROM
    `my_project.my_dataset.fos_table`
ORDER BY dwh_vertrag_id;
-- Expected:
-- dwh_vertrag_id | stichtag_lauf
-- -------------- | -------------
-- 501            | 2023-07-01
-- 505            | 2023-07-01
```

---

## Test Case 7: Empty Source Table

**Purpose:** To verify that the job handles an empty `contract_cache` gracefully, resulting in no inserts into `fos_table` and a successful audit log entry. This covers data quality and row-count assertions for an empty input scenario.

**Setup:**
1.  Clear `my_project.my_dataset.job_audit` and `my_project.my_dataset.fos_table`.
2.  Ensure `my_project.my_dataset.contract_cache` is empty.

**Action:**
Execute the main stored procedure:
`CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('01072023', 0);`

**Pass/Fail Criterion:**
*   **Pass:**
    1.  `my_project.my_dataset.job_audit` contains one entry with `status = 'SUCCESS'`, `stichtag = '2023-07-01'`, `wiederanlaufwert = 0`. The message should indicate `Inserted rows: 0`.
    2.  `my_project.my_dataset.fos_table` remains empty.
*   **Fail:** The job fails, or `fos_table` contains unexpected data.

**Runnable Test Code (SQL Assertions):**

```sql
-- Setup: Ensure contract_cache is empty
TRUNCATE TABLE `my_project.my_dataset.contract_cache`;
TRUNCATE TABLE `my_project.my_dataset.fos_table`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;

-- Action: Execute the stored procedure
CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn`('01072023', 0);

-- Assertion 1: Check job_audit table
SELECT
    status,
    stichtag,
    wiederanlaufwert,
    message LIKE '%SUCCESS%' AS message_success,
    message LIKE '%Inserted rows: 0%' AS message_inserted_count
FROM
    `my_project.my_dataset.job_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_apn'
ORDER BY start_timestamp DESC
LIMIT 1;
-- Expected: status='SUCCESS', stichtag='2023-07-01', wiederanlaufwert=0, message_success=TRUE, message_inserted_count=TRUE

-- Assertion 2: Check fos_table content
SELECT COUNT(*) FROM `my_project.my_dataset.fos_table`;
-- Expected: 0
```

---

## Test Case 8: Error Handling in Core Processing (`k_ausd_bp_ta_bpr_apn`)

**Purpose:** To verify that errors occurring within the core processing stored procedure (`k_ausd_bp_ta_bpr_apn`) are caught by the wrapper (`ausd_bp_ta_bpr_apn`), logged to `job_audit`, and re-raised. This covers external system replacements (audit logging) and robust error handling.

**Setup:**
1.  Clear `my_project.my_dataset.job_audit` and `my_project.my_dataset.fos_table`.
2.  Insert some valid data into `my_project.my_dataset.contract_cache`.
3.  **Simulate an error:** Temporarily modify `k_ausd_bp_ta_bpr_apn` to `RAISE` an error after the `DELETE` but before the `INSERT`, or during the `INSERT`. For example, introduce a `RAISE` statement or a division by zero.
    *   *Note: In a real test environment, this might involve deploying a specific version of the SP for the test or using a mocking framework.* For this example, we'll assume a `RAISE` is injected.

**Action:**
Execute the main stored procedure:
`CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('01072023', 0);`

**Pass/Fail Criterion:**
*   **Pass:**
    1.  The stored procedure call `RAISE`s an error.
    2.  `my_project.my_dataset.job_audit` contains one entry with `status = 'FAILED'`. The `message` field should contain the error message from `k_ausd_bp_ta_bpr_apn` (e.g., "Core processing failed: Simulated error").
    3.  `my_project.my_dataset.fos_table` should reflect the state *before* the error occurred (e.g., if error was during insert, delete might have happened, but no new inserts).
*   **Fail:** The job completes successfully, the error is not logged, or `fos_table` is in an inconsistent state not matching the point of failure.

**Runnable Test Code (Conceptual, as SP modification is manual):**

```sql
-- Setup: Insert sample data
TRUNCATE TABLE `my_project.my_dataset.contract_cache`;
INSERT INTO `my_project.my_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col_a, col_b) VALUES
(601, '2023-01-01', '2024-12-31', '2023-03-01', 'G1', 'H1');

TRUNCATE TABLE `my_project.my_dataset.fos_table`;
TRUNCATE TABLE `my_project.my_dataset.job_audit`;

-- Manual Step: Temporarily modify k_ausd_bp_ta_bpr_apn to introduce an error.
-- Example modification (add this line inside k_ausd_bp_ta_bpr_apn, e.g., after the DELETE block):
-- RAISE USING MESSAGE = 'Simulated error during core processing!';

-- Action: Execute the stored procedure
-- This call is expected to fail and raise an error.
-- CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn`('01072023', 0);

-- Assertion 1: Check job_audit table for failure
SELECT
    status,
    stichtag,
    wiederanlaufwert,
    message LIKE '%FAILED%' AS message_failed,
    message LIKE '%Simulated error during core processing!%' AS message_error_detail
FROM
    `my_project.my_dataset.job_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_apn'
ORDER BY start_timestamp DESC
LIMIT 1;
-- Expected: status='FAILED', stichtag='2023-07-01', wiederanlaufwert=0, message_failed=TRUE, message_error_detail=TRUE

-- Assertion 2: Check fos_table content (should be empty if error before insert)
SELECT COUNT(*) FROM `my_project.my_dataset.fos_table`;
-- Expected: 0 (assuming error occurred before any inserts)
```