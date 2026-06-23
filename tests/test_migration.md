As a senior data-migration QA engineer, I've reviewed the migration design document and the generated BigQuery code for `r_ausd_bp_ta_bpr_opt_text.ksh`.

**Critical Discrepancy Identified:**

The migration design document explicitly states the core transformation logic should include date filtering (`GUELTIG_VON <= v_stichtag`, `v_stichtag < GUELTIG_BIS`, `LADEDATUM < v_stichtag`) and conditional `DWH_VERTRAG_ID` filtering (`DWH_VERTRAG_ID > v_wiederanlaufWert`) in the `SELECT` statement.

However, the provided BigQuery stored procedure's `INSERT INTO ... SELECT ...` statement **does not implement any of these critical filtering conditions**. It performs a simple `INNER JOIN` between `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr` without any `WHERE` clauses based on `v_stichtag` or `v_wiederanlaufWert` for the `SELECT` part. The `v_wiederanlaufWert` is only used for a `DELETE` operation *before* the `INSERT`.

This is a **major functional gap** and indicates that the core data processing logic described in the design document (and presumably present in the legacy `k_ausd_bp_ta_bpr_opt_text.ksh` script) has **not been migrated correctly or completely** into the BigQuery stored procedure.

The tests below are designed to validate the behavior as described in the **design document**. Therefore, the current BigQuery stored procedure code, as provided, would **fail** the tests related to core transformation logic (Test Cases 4.1, 4.2, 4.3). This needs to be addressed by updating the BigQuery stored procedure to include the missing `WHERE` clauses.

---

## Migration Validation Tests for `project.dataset.ausd_bp_ta_bpr_opt_text`

### Pre-requisite Setup for All Tests

Before running any tests, ensure the following DDLs are executed to create the necessary tables. These mock source tables include columns inferred from the design document's description of the core logic, which are currently missing from the provided BigQuery stored procedure's `SELECT` statement.

```sql
-- DDL for mock source table sof_ta_bpr_optionen
-- Includes inferred date columns for testing the design document's specified logic.
CREATE OR REPLACE TABLE `project.dataset.sof_ta_bpr_optionen` (
    cntrct_id INT64 OPTIONS(description="Contract ID"),
    bpr_id INT64 OPTIONS(description="Basisprodukt ID"),
    GUELTIG_VON DATE OPTIONS(description="Validity start date, inferred from design"),
    GUELTIG_BIS DATE OPTIONS(description="Validity end date, inferred from design"),
    LADEDATUM DATE OPTIONS(description="Load date, inferred from design")
);

-- DDL for mock source table sof_ta_bpr_beschr
CREATE OR REPLACE TABLE `project.dataset.sof_ta_bpr_beschr` (
    bpr_id INT64 OPTIONS(description="Basisprodukt ID"),
    pds_description STRING OPTIONS(description="Basisprodukt Description")
);

-- DDL for target table sof_ta_bpr_opt_text
CREATE OR REPLACE TABLE `project.dataset.sof_ta_bpr_opt_text` (
    cntrct_id INT64 OPTIONS(description="Contract ID"),
    bpr_id INT64 OPTIONS(description="Basisprodukt ID"),
    pds_description STRING OPTIONS(description="Basisprodukt Description")
);

-- DDL for job_audit table
CREATE OR REPLACE TABLE `project.dataset.job_audit` (
    job_name STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL,
    message STRING,
    stichtag DATE,
    wiederanlaufwert INT64
);

-- DDL for job_log table
CREATE OR REPLACE TABLE `project.dataset.job_log` (
    log_time TIMESTAMP NOT NULL,
    log_level STRING NOT NULL,
    job_name STRING NOT NULL,
    message STRING NOT NULL
);
```

---

### 1. Parameter Handling & Defaults

#### 1.1 Test Case: Default `p_stichtag` and `p_wiederanlaufWert`

*   **Purpose:** Verify that `p_stichtag` defaults to `CURRENT_DATE()` and `p_wiederanlaufWert` defaults to `0` when no parameters are provided. Also, verify that the table is truncated when `p_wiederanlaufWert` is 0.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_optionen` and `project.dataset.sof_ta_bpr_beschr` with sample data.
    2.  Populate `project.dataset.sof_ta_bpr_opt_text` with some existing data (e.g., 5 rows).
    3.  Clear `project.dataset.job_audit` and `project.dataset.job_log`.
*   **Action:**
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`(NULL, NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof_ta_bpr_opt_text` table should contain only the newly inserted rows (i.e., the previous 5 rows should be truncated).
    2.  The `job_audit` table should have one entry for `ausd_bp_ta_bpr_opt_text` with `status = 'SUCCESS'`, `stichtag` equal to the `CURRENT_DATE()` of execution, and `wiederanlaufwert = 0`.
    3.  The `job_log` table should contain an `INFO` message indicating `p_stichtag_str not provided, defaulting to CURRENT_DATE()`.
    4.  The `job_log` table should contain an `INFO` message indicating 'No restart value provided, performing full load by truncating table.'

#### 1.2 Test Case: Explicit `p_stichtag` and Default `p_wiederanlaufWert`

*   **Purpose:** Verify that `p_stichtag` is correctly parsed and used, and `p_wiederanlaufWert` defaults to `0`.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_optionen` and `project.dataset.sof_ta_bpr_beschr` with sample data.
    2.  Populate `project.dataset.sof_ta_bpr_opt_text` with some existing data.
    3.  Clear `project.dataset.job_audit` and `project.dataset.job_log`.
*   **Action:**
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('01012023', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof_ta_bpr_opt_text` table should contain only the newly inserted rows (truncated previous data).
    2.  The `job_audit` table should have one entry with `status = 'SUCCESS'`, `stichtag = '2023-01-01'`, and `wiederanlaufwert = 0`.
    3.  The `job_log` table should contain an `INFO` message indicating `p_stichtag parsed as: 2023-01-01`.
    4.  The `job_log` table should contain an `INFO` message indicating 'No restart value provided, performing full load by truncating table.'

#### 1.3 Test Case: Invalid `p_stichtag_str` Format

*   **Purpose:** Verify that the procedure handles invalid `p_stichtag_str` format gracefully and logs an error.
*   **Setup:**
    1.  Clear `project.dataset.job_audit` and `project.dataset.job_log`.
*   **Action:**
    ```sql
    -- This call is expected to fail
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('2023-01-01', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement should raise an error and terminate.
    2.  The `job_audit` table should have one entry with `status = 'FAILED'`, `stichtag = NULL`, and a `message` indicating a parsing error for `p_stichtag_str`.
    3.  The `job_log` table should contain an `ERROR` message related to `Error parsing p_stichtag_str`.

### 2. Restart Logic (`p_wiederanlaufWert`)

#### 2.1 Test Case: `p_wiederanlaufWert > 0` (Partial Delete and Insert)

*   **Purpose:** Verify that when `p_wiederanlaufWert` is provided and greater than 0, existing records in the target table with `cntrct_id >= p_wiederanlaufWert` are deleted before new data is inserted.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_optionen` and `project.dataset.sof_ta_bpr_beschr` with data that would result in new `cntrct_id`s being inserted (e.g., `cntrct_id` 101, 102, 103).
    2.  Populate `project.dataset.sof_ta_bpr_opt_text` with existing data, some of which should be deleted by the restart logic (e.g., `cntrct_id` 99, 100, 101, 102, 103, 104).
        ```sql
        TRUNCATE TABLE `project.dataset.sof_ta_bpr_opt_text`;
        INSERT INTO `project.dataset.sof_ta_bpr_opt_text` (cntrct_id, bpr_id, pds_description) VALUES
            (99, 1, 'Old Desc 1'),
            (100, 2, 'Old Desc 2'),
            (101, 3, 'Old Desc 3'), -- Should be deleted
            (102, 4, 'Old Desc 4'), -- Should be deleted
            (103, 5, 'Old Desc 5'), -- Should be deleted
            (104, 6, 'Old Desc 6'); -- Should be deleted
        ```
    3.  Clear `project.dataset.job_audit` and `project.dataset.job_log`.
*   **Action:**
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('01012023', 101);
    ```
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof_ta_bpr_opt_text` table should contain:
        *   Records with `cntrct_id < 101` (e.g., 99, 100 from setup).
        *   All new records inserted by the `SELECT` statement (e.g., 101, 102, 103 from source).
        *   Records with `cntrct_id >= 101` from the initial setup should be replaced by the new inserts.
    2.  The `job_audit` table should have one entry with `status = 'SUCCESS'`, `stichtag = '2023-01-01'`, and `wiederanlaufwert = 101`.
    3.  The `job_log` table should contain an `INFO` message indicating 'Restart logic active. Deleting records from ... with cntrct_id >= 101.' and another message confirming the number of records deleted.

#### 2.2 Test Case: `p_wiederanlaufWert > 0` (No Records to Delete)

*   **Purpose:** Verify that the restart logic correctly handles cases where no records match the deletion criterion.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_optionen` and `project.dataset.sof_ta_bpr_beschr` with data.
    2.  Populate `project.dataset.sof_ta_bpr_opt_text` with existing data, all `cntrct_id`s being less than the `p_wiederanlaufWert`.
        ```sql
        TRUNCATE TABLE `project.dataset.sof_ta_bpr_opt_text`;
        INSERT INTO `project.dataset.sof_ta_bpr_opt_text` (cntrct_id, bpr_id, pds_description) VALUES
            (1, 1, 'Desc A'),
            (2, 2, 'Desc B');
        ```
    3.  Clear `project.dataset.job_audit` and `project.dataset.job_log`.
*   **Action:**
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('01012023', 100);
    ```
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof_ta_bpr_opt_text` table should contain the original records (1, 2) plus all new records inserted by the `SELECT` statement.
    2.  The `job_audit` table should have one entry with `status = 'SUCCESS'`, `stichtag = '2023-01-01'`, and `wiederanlaufwert = 100`.
    3.  The `job_log` table should contain an `INFO` message indicating 'Restart logic active. Deleting records from ... with cntrct_id >= 100.' and another message confirming '0 records deleted'.

### 3. Logging and Auditing

#### 3.1 Test Case: Successful Execution Logging

*   **Purpose:** Verify that `job_audit` and `job_log` tables are correctly populated for a successful run.
*   **Setup:**
    1.  Populate source tables with minimal data.
    2.  Clear `project.dataset.job_audit` and `project.dataset.job_log`.
*   **Action:**
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('01012023', 0);
    ```
*   **Pass/Fail Criterion:**
    1.  `project.dataset.job_audit` should contain one row:
        *   `job_name = 'ausd_bp_ta_bpr_opt_text'`
        *   `start_time` and `end_time` are populated and `end_time > start_time`
        *   `status = 'SUCCESS'`
        *   `stichtag = '2023-01-01'`
        *   `wiederanlaufwert = 0`
        *   `message` indicates successful insertion and row count.
    2.  `project.dataset.job_log` should contain multiple `INFO` entries covering:
        *   Parameter parsing/defaulting.
        *   Restart logic (truncation in this case).
        *   Number of records inserted.
    3.  No `ERROR` or `WARNING` entries should be present in `job_log`.

#### 3.2 Test Case: Failed Execution Logging

*   **Purpose:** Verify that `job_audit` and `job_log` tables are correctly populated for a failed run.
*   **Setup:**
    1.  Clear `project.dataset.job_audit` and `project.dataset.job_log`.
    2.  (Simulate a failure, e.g., by temporarily dropping a source table or introducing a syntax error in the procedure's `INSERT` statement for testing purposes, then revert). For this test, we'll use the invalid `p_stichtag_str` from 1.3.
*   **Action:**
    ```sql
    -- This call is expected to fail
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('INVALID_DATE', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  `project.dataset.job_audit` should contain one row:
        *   `job_name = 'ausd_bp_ta_bpr_opt_text'`
        *   `start_time` and `end_time` are populated.
        *   `status = 'FAILED'`
        *   `stichtag = NULL` (as parsing failed before it could be set)
        *   `wiederanlaufwert = 0`
        *   `message` contains the error details (e.g., 'Error parsing p_stichtag_str...').
    2.  `project.dataset.job_log` should contain at least one `ERROR` entry detailing the failure.

### 4. Core Transformation Logic (Based on Design Document)

**NOTE:** As highlighted in the introduction, the provided BigQuery stored procedure **does not implement** the date filtering (`GUELTIG_VON`, `GUELTIG_BIS`, `LADEDATUM`) or the `DWH_VERTRAG_ID` filtering in its `SELECT` statement, as described in the design document. These tests are written assuming the **design document's logic is correct**, and thus the current BigQuery code would **fail** these tests. This indicates a bug or incomplete migration.

#### 4.1 Test Case: Date Filtering (`GUELTIG_VON`, `GUELTIG_BIS`, `LADEDATUM`)

*   **Purpose:** Verify that records are filtered based on `GUELTIG_VON <= v_stichtag`, `v_stichtag < GUELTIG_BIS`, and `LADEDATUM < v_stichtag`.
*   **Setup:**
    1.  Clear all tables.
    2.  Populate `project.dataset.sof_ta_bpr_beschr`:
        ```sql
        INSERT INTO `project.dataset.sof_ta_bpr_beschr` (bpr_id, pds_description) VALUES
            (1, 'Desc A'), (2, 'Desc B'), (3, 'Desc C'), (4, 'Desc D');
        ```
    3.  Populate `project.dataset.sof_ta_bpr_optionen` with various date scenarios:
        ```sql
        INSERT INTO `project.dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id, GUELTIG_VON, GUELTIG_BIS, LADEDATUM) VALUES
            -- Expected to be selected (stichtag = 2023-01-15)
            (101, 1, '2023-01-01', '2023-01-31', '2023-01-10'), -- Valid
            (102, 2, '2023-01-15', '2023-01-31', '2023-01-14'), -- Valid (GUELTIG_VON = stichtag, LADEDATUM < stichtag)
            -- Expected to be filtered out
            (103, 3, '2023-01-01', '2023-01-15', '2023-01-10'), -- GUELTIG_BIS not > stichtag
            (104, 4, '2023-01-16', '2023-01-31', '2023-01-10'), -- GUELTIG_VON not <= stichtag
            (105, 1, '2023-01-01', '2023-01-31', '2023-01-15'), -- LADEDATUM not < stichtag
            (106, 2, '2023-01-01', '2023-01-31', '2023-01-16'); -- LADEDATUM not < stichtag
        ```
*   **Action:**
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('15012023', 0); -- Stichtag = 2023-01-15
    ```
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof_ta_bpr_opt_text` table should contain exactly 2 rows (for `cntrct_id` 101, 102).
    2.  **Current BQ Code Behavior:** The current BQ code would insert all 6 rows from `sof_ta_bpr_optionen` (assuming `bpr_id`s match `sof_ta_bpr_beschr`), which would **FAIL** this test.

#### 4.2 Test Case: `DWH_VERTRAG_ID` Filtering with `p_wiederanlaufWert` (in `SELECT` statement)

*   **Purpose:** Verify that when `p_wiederanlaufWert > 0`, the `SELECT` statement *also* filters for `cntrct_id > p_wiederanlaufWert` (in addition to the `DELETE` operation).
*   **Setup:**
    1.  Clear all tables.
    2.  Populate `project.dataset.sof_ta_bpr_beschr` with `bpr_id` 1, 2, 3.
    3.  Populate `project.dataset.sof_ta_bpr_optionen` with data, all valid for a `stichtag` (e.g., '2023-01-15'):
        ```sql
        INSERT INTO `project.dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id, GUELTIG_VON, GUELTIG_BIS, LADEDATUM) VALUES
            (100, 1, '2023-01-01', '2023-01-31', '2023-01-10'),
            (101, 2, '2023-01-01', '2023-01-31', '2023-01-10'),
            (102, 3, '2023-01-01', '2023-01-31', '2023-01-10');
        ```
    4.  Populate `project.dataset.sof_ta_bpr_opt_text` with some initial data:
        ```sql
        INSERT INTO `project.dataset.sof_ta_bpr_opt_text` (cntrct_id, bpr_id, pds_description) VALUES
            (99, 1, 'Old 99'), (100, 1, 'Old 100'), (101, 2, 'Old 101');
        ```
*   **Action:**
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('15012023', 100); -- p_wiederanlaufWert = 100
    ```
*   **Pass/Fail Criterion:**
    1.  **Expected behavior (Design Doc):**
        *   `DELETE` would remove `cntrct_id` 100, 101 from `sof_ta_bpr_opt_text`.
        *   `SELECT` would then *only* pick `cntrct_id` 101, 102 (because `cntrct_id > 100`).
        *   Final `sof_ta_bpr_opt_text` should contain: `(99, 1, 'Old 99')`, `(101, 2, 'Desc B')`, `(102, 3, 'Desc C')`. Total 3 rows.
    2.  **Current BQ Code Behavior:**
        *   `DELETE` removes `cntrct_id` 100, 101 from `sof_ta_bpr_opt_text`.
        *   `SELECT` inserts all 3 rows (100, 101, 102) from `sof_ta_bpr_optionen`.
        *   Final `sof_ta_bpr_opt_text` would contain: `(99, 1, 'Old 99')`, `(100, 1, 'Desc A')`, `(101, 2, 'Desc B')`, `(102, 3, 'Desc C')`. Total 4 rows. This would **FAIL** the test against the design document's logic.

#### 4.3 Test Case: Join Correctness and Data Integrity

*   **Purpose:** Verify that the `INNER JOIN` correctly links `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr` on `bpr_id`, and that `cntrct_id`, `bpr_id`, `pds_description` are correctly populated.
*   **Setup:**
    1.  Clear all tables.
    2.  Populate `project.dataset.sof_ta_bpr_beschr`:
        ```sql
        INSERT INTO `project.dataset.sof_ta_bpr_beschr` (bpr_id, pds_description) VALUES
            (10, 'Product A Description'),
            (20, 'Product B Description'),
            (30, 'Product C Description');
        ```
    3.  Populate `project.dataset.sof_ta_bpr_optionen` (ensure all dates are valid for a future `stichtag` to avoid date filtering issues if the BQ code were fixed):
        ```sql
        INSERT INTO `project.dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id, GUELTIG_VON, GUELTIG_BIS, LADEDATUM) VALUES
            (1, 10, '2023-01-01', '2024-01-01', '2023-01-01'),
            (2, 20, '2023-01-01', '2024-01-01', '2023-01-01'),
            (3, 10, '2023-01-01', '2024-01-01', '2023-01-01'), -- Duplicate bpr_id for cntrct_id
            (4, 40, '2023-01-01', '2024-01-01', '2023-01-01'); -- No matching bpr_id in _beschr
        ```
*   **Action:**
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('01062023', 0); -- Stichtag = 2023-06-01
    ```
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof_ta_bpr_opt_text` table should contain 3 rows:
        ```
        cntrct_id | bpr_id | pds_description
        ----------|--------|----------------------
        1         | 10     | Product A Description
        2         | 20     | Product B Description
        3         | 10     | Product A Description
        ```
    2.  Row with `cntrct_id = 4` should *not* be present due to the `INNER JOIN`.
    3.  All `pds_description` values should correctly correspond to their `bpr_id` from `sof_ta_bpr_beschr`.
    4.  **Current BQ Code Behavior:** This test would pass for the join logic, but if the `stichtag` filtering was active (as per design), the `GUELTIG_VON`, `GUELTIG_BIS`, `LADEDATUM` columns would also be considered. Since the current code ignores them, it effectively passes this test for the *join part only*, but not for the full filtering as per design.

### 5. Data Quality / Row Count / Schema Assertions

#### 5.1 Test Case: Schema Validation

*   **Purpose:** Verify that the target table `project.dataset.sof_ta_bpr_opt_text` has the correct schema (column names, types, nullability).
*   **Setup:** Ensure the table `project.dataset.sof_ta_bpr_opt_text` exists.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS`.
*   **Pass/Fail Criterion:**
    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `project.dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'sof_ta_bpr_opt_text'
    ORDER BY
        ordinal_position;
    ```
    Expected output:
    ```
    column_name   | data_type | is_nullable
    --------------|-----------|------------
    cntrct_id     | INT64     | YES
    bpr_id        | INT64     | YES
    pds_description| STRING    | YES
    ```
    (Note: `OPTIONS(description=...)` does not affect `is_nullable` unless `NOT NULL` is specified in DDL. The provided DDL does not specify `NOT NULL` for these columns, so `YES` is expected for `is_nullable`.)

#### 5.2 Test Case: Row Count Parity (after fixing core logic)

*   **Purpose:** After the core transformation logic is correctly implemented in the BigQuery stored procedure (i.e., including date and `cntrct_id` filtering), this test verifies that the final row count in `sof_ta_bpr_opt_text` matches the legacy system's output for identical inputs.
*   **Setup:**
    1.  **Crucial:** Obtain a snapshot of the legacy `k_ausd_bp_ta_bpr_opt_text.ksh`'s output (the `fos_table`) for a specific set of input parameters (`-s`, `-l`).
    2.  Replicate the exact source data (`sof_ta_bpr_optionen`, `sof_ta_bpr_beschr`) in BigQuery that was used by the legacy job.
    3.  Clear `project.dataset.sof_ta_bpr_opt_text`.
*   **Action:**
    ```sql
    -- Assuming legacy job was run with stichtag 'DDMMYYYY' and wiederanlaufwert X
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('DDMMYYYY', X);
    ```
*   **Pass/Fail Criterion:**
    ```sql
    SELECT COUNT(*) FROM `project.dataset.sof_ta_bpr_opt_text`;
    ```
    The count should exactly match the row count of the legacy `fos_table` output for the same input parameters.

#### 5.3 Test Case: Full Data Parity (after fixing core logic)

*   **Purpose:** After the core transformation logic is correctly implemented, this test verifies that the entire dataset in `sof_ta_bpr_opt_text` is identical to the legacy system's output for identical inputs.
*   **Setup:**
    1.  **Crucial:** Obtain the full output data from the legacy `k_ausd_bp_ta_bpr_opt_text.ksh` (the `fos_table`) for a specific set of input parameters (`-s`, `-l`). Load this into a temporary BigQuery table, e.g., `project.dataset.legacy_fos_output`.
    2.  Replicate the exact source data (`sof_ta_bpr_optionen`, `sof_ta_bpr_beschr`) in BigQuery that was used by the legacy job.
    3.  Clear `project.dataset.sof_ta_bpr_opt_text`.
*   **Action:**
    ```sql
    -- Assuming legacy job was run with stichtag 'DDMMYYYY' and wiederanlaufwert X
    CALL `project.dataset.ausd_bp_ta_bpr_opt_text`('DDMMYYYY', X);
    ```
*   **Pass/Fail Criterion:**
    ```sql
    -- Compare row counts first
    SELECT
        (SELECT COUNT(*) FROM `project.dataset.sof_ta_bpr_opt_text`) AS new_count,
        (SELECT COUNT(*) FROM `project.dataset.legacy_fos_output`) AS legacy_count;

    -- Then compare data content
    SELECT
        'Mismatch in new_output - legacy_output' AS mismatch_type,
        t1.*
    FROM
        `project.dataset.sof_ta_bpr_opt_text` AS t1
    FULL OUTER JOIN
        `project.dataset.legacy_fos_output` AS t2
        ON t1.cntrct_id = t2.cntrct_id
        AND t1.bpr_id = t2.bpr_id
        AND t1.pds_description = t2.pds_description
    WHERE
        t2.cntrct_id IS NULL OR t1.cntrct_id IS NULL;
    ```
    *   The `new_count` and `legacy_count` should be identical.
    *   The second query should return 0 rows, indicating no differences between the new output and the legacy output.

---

**Conclusion on Discrepancy:**

The most critical finding is the **missing core transformation logic** in the provided BigQuery stored procedure. The current procedure only performs a simple join and insert, completely ignoring the date-based filtering and conditional `DWH_VERTRAG_ID` filtering described in the design document. This must be rectified before proceeding with further validation, as the current BigQuery code is functionally different from the legacy system's described behavior.