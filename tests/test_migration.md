As a senior data-migration QA engineer, I have analyzed the provided migration design document and the generated BigQuery code for `r_ausd_bp_ta_cntrct_evn.ksh`. The migration involves translating KornShell logic into BigQuery Stored Procedures, with a focus on parameter handling, date determination, restart functionality, and structured logging.

The following test cases are designed to validate the migrated BigQuery solution against the legacy KornShell script, ensuring behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

---

## Pre-requisite Setup for All Tests

Before running any tests, ensure the following BigQuery tables and procedures exist and are accessible. The `project.dataset` placeholder should be replaced with your actual BigQuery project and dataset.

```sql
-- Create job_log table
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  job_nr INT64,
  job_name STRING,
  job_status STRING,
  log_ts TIMESTAMP,
  stichtag STRING,
  restart_value INT64,
  message STRING
);

-- Create contract_cache table (source)
CREATE OR REPLACE TABLE `project.dataset.contract_cache` (
    DWH_VERTRAG_ID INT66,
    Gueltig_von DATE,
    Gueltig_bis DATE,
    LADEDATUM DATE,
    CONTRACT_DETAILS STRING
);

-- Create fos_table table (target)
CREATE OR REPLACE TABLE `project.dataset.fos_table` (
    DWH_VERTRAG_ID INT66,
    Gueltig_von DATE,
    Gueltig_bis DATE,
    LADEDATUM DATE,
    CONTRACT_DETAILS STRING
);

-- Deploy sp_log_job_event, sp_validate_stichtag, k_ausd_bp_ta_cntrct_evn, and ausd_bp_ta_cntrct_evn procedures
-- (As provided in the "GENERATED MIGRATION CODE" section)
```

**Common Test Data for `contract_cache` (used in multiple tests, assuming `Stichtag = '15032023'` (March 15, 2023) for expected outcomes):**

```sql
TRUNCATE TABLE `project.dataset.contract_cache`;
INSERT INTO `project.dataset.contract_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, CONTRACT_DETAILS) VALUES
(100, '2023-03-01', '2023-03-31', '2023-03-10', 'Contract A - Pass'),
(101, '2023-03-15', '2023-03-31', '2023-03-10', 'Contract B - Pass (Gueltig_von = Stichtag)'),
(102, '2023-03-16', '2023-03-31', '2023-03-10', 'Contract C - Fail (Gueltig_von > Stichtag)'),
(103, '2023-03-01', '2023-03-15', '2023-03-10', 'Contract D - Fail (Gueltig_bis = Stichtag)'),
(104, '2023-03-01', '2023-03-31', '2023-03-15', 'Contract E - Fail (LADEDATUM = Stichtag)'),
(105, '2023-03-01', '2023-03-31', '2023-03-16', 'Contract F - Fail (LADEDATUM > Stichtag)'),
(106, '2023-03-01', '2023-03-31', '2023-03-10', 'Contract G - Pass'),
(107, '2023-03-01', '2023-03-31', '2023-03-10', 'Contract H - Pass'),
(108, NULL,         '2023-03-31', '2023-03-10', 'Contract I - Fail (Gueltig_von NULL)'),
(109, '2023-03-01', NULL,         '2023-03-10', 'Contract J - Fail (Gueltig_bis NULL)'),
(110, '2023-03-01', '2023-03-31', NULL,         'Contract K - Fail (LADEDATUM NULL)'),
(111, '2023-03-01', '2023-03-31', '2023-03-10', 'Contract L - Pass');
```
Expected `DWH_VERTRAG_ID`s to be inserted for `Stichtag = '15032023'` and `restart_value = 0`: `100, 101, 106, 107, 111` (5 records).

---

## Test Case 1: Basic Data Provisioning (Output Parity & Transformation Correctness)

*   **Purpose:** Verify that the migrated job correctly extracts and inserts data into the target table (`fos_table`) when no restart value is provided and a specific `Stichtag` is given. This covers the core filtering logic.
*   **Setup:**
    1.  Clear `project.dataset.fos_table` and `project.dataset.job_log`.
    2.  Populate `project.dataset.contract_cache` with the common test data defined above.
    3.  Define `Stichtag` as `'15032023'`.
*   **Action:** Execute the migrated wrapper procedure.
    ```python
    # pytest-style pseudocode
    def test_basic_data_provisioning():
        # Clear target and log tables
        bq_client.query("TRUNCATE TABLE `project.dataset.fos_table`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`").result()

        # Populate source table (using the common test data)
        # ... (SQL INSERT statements for contract_cache as defined above) ...

        # Execute the migrated job
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`('15032023', NULL)").result()

        # Assertions
        # ... (See Pass/Fail Criterion below) ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Row Count:** The number of rows in `project.dataset.fos_table` must be 5.
        ```sql
        SELECT COUNT(1) FROM `project.dataset.fos_table`;
        -- Expected: 5
        ```
    2.  **Data Content:** The `DWH_VERTRAG_ID`s in `project.dataset.fos_table` must be `100, 101, 106, 107, 111`. All other columns for these IDs must exactly match the `contract_cache` source.
        ```sql
        SELECT DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, CONTRACT_DETAILS
        FROM `project.dataset.fos_table`
        ORDER BY DWH_VERTRAG_ID;
        -- Expected:
        -- 100, 2023-03-01, 2023-03-31, 2023-03-10, 'Contract A - Pass'
        -- 101, 2023-03-15, 2023-03-31, 2023-03-10, 'Contract B - Pass (Gueltig_von = Stichtag)'
        -- 106, 2023-03-01, 2023-03-31, 2023-03-10, 'Contract G - Pass'
        -- 107, 2023-03-01, 2023-03-31, 2023-03-10, 'Contract H - Pass'
        -- 111, 2023-03-01, 2023-03-31, 2023-03-10, 'Contract L - Pass'
        ```
    3.  **Logging:** `project.dataset.job_log` must contain `START` and `SUCCESS` entries for both wrapper and core procedures, with `stichtag = '15032023'` and `restart_value = 0`.
        ```sql
        SELECT job_status, stichtag, restart_value, message
        FROM `project.dataset.job_log`
        WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        ORDER BY log_ts;
        -- Expected:
        -- 'START', '15032023', 0, 'Wrapper procedure started'
        -- 'START', '15032023', 0, 'Core procedure started'
        -- 'SUCCESS', '15032023', 0, 'Core procedure completed successfully'
        -- 'SUCCESS', '15032023', 0, 'Wrapper procedure completed successfully'
        ```

---

## Test Case 2: Restart Functionality (Transformation Correctness)

*   **Purpose:** Verify that the migrated job correctly handles the `p_wiederanlaufWert` parameter, performing the deletion and then inserting only records with `DWH_VERTRAG_ID` greater than the restart value.
*   **Setup:**
    1.  Clear `project.dataset.fos_table` and `project.dataset.job_log`.
    2.  Populate `project.dataset.contract_cache` with the common test data.
    3.  Pre-populate `project.dataset.fos_table` with some data, including records that would be deleted by the restart logic and some that should remain.
        ```sql
        INSERT INTO `project.dataset.fos_table` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, CONTRACT_DETAILS) VALUES
        (100, '2023-03-01', '2023-03-31', '2023-03-10', 'Contract A - Existing'),
        (101, '2023-03-15', '2023-03-31', '2023-03-10', 'Contract B - Existing'),
        (106, '2023-03-01', '2023-03-31', '2023-03-10', 'Contract G - Existing'),
        (107, '2023-03-01', '2023-03-31', '2023-03-10', 'Contract H - Existing'), -- This will be deleted by restart_value=107
        (108, '2023-01-01', '2023-12-31', '2023-01-01', 'Contract I - Existing (higher ID)'), -- This will be deleted by restart_value=107
        (120, '2023-01-01', '2023-12-31', '2023-01-01', 'Contract X - Existing (higher ID)'); -- This will be deleted by restart_value=107
        ```
    4.  Define `Stichtag` as `'15032023'` and `p_wiederanlaufWert` as `107`.
*   **Action:** Execute the migrated wrapper procedure.
    ```python
    # pytest-style pseudocode
    def test_restart_functionality():
        # Clear and populate tables as per setup
        # ...
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`('15032023', 107)").result()
        # Assertions
        # ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Row Count:** The number of rows in `project.dataset.fos_table` must be 4.
        ```sql
        SELECT COUNT(1) FROM `project.dataset.fos_table`;
        -- Expected: 4
        ```
    2.  **Data Content:** The `DWH_VERTRAG_ID`s in `project.dataset.fos_table` must be `100, 101, 106, 111`. Records `107, 108, 120` should be deleted. Record `111` should be newly inserted.
        ```sql
        SELECT DWH_VERTRAG_ID, CONTRACT_DETAILS
        FROM `project.dataset.fos_table`
        ORDER BY DWH_VERTRAG_ID;
        -- Expected:
        -- 100, 'Contract A - Existing'
        -- 101, 'Contract B - Existing'
        -- 106, 'Contract G - Existing'
        -- 111, 'Contract L - Pass' (from contract_cache)
        ```
    3.  **Logging:** `project.dataset.job_log` must contain `START`, `INFO` (for deletion), and `SUCCESS` entries, with `stichtag = '15032023'` and `restart_value = 107`. The `INFO` message should reflect the deletion.
        ```sql
        SELECT job_status, stichtag, restart_value, message
        FROM `project.dataset.job_log`
        WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        ORDER BY log_ts;
        -- Expected:
        -- 'START', '15032023', 107, 'Wrapper procedure started'
        -- 'START', '15032023', 107, 'Core procedure started'
        -- 'INFO', '15032023', 107, 'Deleted records from target where DWH_VERTRAG_ID >= 107'
        -- 'SUCCESS', '15032023', 107, 'Core procedure completed successfully'
        -- 'SUCCESS', '15032023', 107, 'Wrapper procedure completed successfully'
        ```

---

## Test Case 3: Default Stichtag (Transformation Correctness)

*   **Purpose:** Verify that if `p_stichtag` is not provided, the job defaults to the current system date and processes data accordingly.
*   **Setup:**
    1.  Clear `project.dataset.fos_table` and `project.dataset.job_log`.
    2.  Populate `project.dataset.contract_cache` with data relevant to `CURRENT_DATE()`. For this test, assume `CURRENT_DATE()` is `2023-03-15` (to match the common test data).
    3.  Set `p_wiederanlaufWert` to `NULL`.
*   **Action:** Execute the migrated wrapper procedure without `p_stichtag`.
    ```python
    # pytest-style pseudocode
    def test_default_stichtag():
        # Clear and populate tables as per setup
        # ...
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`(NULL, NULL)").result()
        # Assertions
        # ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Row Count & Data Content:** The `fos_table` must contain the 5 records (`100, 101, 106, 107, 111`) as if `'15032023'` was explicitly passed, and `v_restart_value = 0`.
        ```sql
        SELECT COUNT(1) FROM `project.dataset.fos_table`;
        -- Expected: 5
        SELECT DWH_VERTRAG_ID FROM `project.dataset.fos_table` ORDER BY DWH_VERTRAG_ID;
        -- Expected: 100, 101, 106, 107, 111
        ```
    2.  **Logging:** `project.dataset.job_log` must show `CURRENT_DATE()` (in `DDMMYYYY` format, e.g., `'15032023'`) as the `stichtag` parameter for all log entries.
        ```sql
        SELECT job_status, stichtag, restart_value, message
        FROM `project.dataset.job_log`
        WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        ORDER BY log_ts;
        -- Expected: All 'stichtag' entries should be '15032023' (or actual CURRENT_DATE in DDMMYYYY format)
        ```

---

## Test Case 4: Invalid Stichtag Format (Error Handling)

*   **Purpose:** Verify that the job correctly handles an invalid `p_stichtag` format and logs an error without processing data.
*   **Setup:**
    1.  Clear `project.dataset.fos_table` and `project.dataset.job_log`.
    2.  Populate `project.dataset.contract_cache` with some data (e.g., the common test data).
*   **Action:** Execute the migrated wrapper procedure with an invalid `p_stichtag`. This call is expected to fail.
    ```python
    # pytest-style pseudocode
    import pytest

    def test_invalid_stichtag_format():
        # Clear tables
        bq_client.query("TRUNCATE TABLE `project.dataset.fos_table`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`").result()
        # Populate source table (optional, but good for consistency)
        # ... (SQL INSERT statements for contract_cache as defined above) ...

        # Execute the migrated job, expecting an error
        with pytest.raises(Exception) as excinfo:
            bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`('2023-03-15', NULL)").result() # Invalid format 'YYYY-MM-DD'
        assert "Invalid stichtag format" in str(excinfo.value)

        # Assertions
        # ... (See Pass/Fail Criterion below) ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Error Propagation:** The call to `ausd_bp_ta_cntrct_evn` must raise an error (e.g., `SQLSTATE '45000'`) with a message indicating an invalid date format.
    2.  **No Data Changes:** `project.dataset.fos_table` must remain empty.
        ```sql
        SELECT COUNT(1) FROM `project.dataset.fos_table`;
        -- Expected: 0
        ```
    3.  **Logging:** `project.dataset.job_log` must contain a `START` entry for the wrapper and an `ERROR` entry for the wrapper, with a message indicating the invalid date format.
        ```sql
        SELECT job_status, message
        FROM `project.dataset.job_log`
        WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        ORDER BY log_ts;
        -- Expected:
        -- 'START', 'Wrapper procedure started'
        -- 'ERROR', 'Wrapper procedure failed: Invalid stichtag format. Expected DDMMYYYY, got: 2023-03-15'
        ```

---

## Test Case 5: NULL Handling in Source Date Columns (Transformation Correctness / Edge Case)

*   **Purpose:** Verify how the job behaves when `Gueltig_von`, `Gueltig_bis`, or `LADEDATUM` in `contract_cache` are NULL. These records should be excluded.
*   **Setup:**
    1.  Clear `project.dataset.fos_table` and `project.dataset.job_log`.
    2.  Populate `project.dataset.contract_cache` with the common test data, which includes records with NULLs in date columns (IDs 108, 109, 110).
    3.  Define a valid `Stichtag` as `'15032023'`.
*   **Action:** Execute the migrated wrapper procedure.
    ```python
    # pytest-style pseudocode
    def test_null_date_handling():
        # Clear and populate tables as per setup
        # ...
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`('15032023', NULL)").result()
        # Assertions
        # ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Row Count & Data Content:** The `fos_table` must contain only the 5 valid records (`100, 101, 106, 107, 111`). Records `108, 109, 110` (with NULL dates) must *not* be inserted.
        ```sql
        SELECT COUNT(1) FROM `project.dataset.fos_table`;
        -- Expected: 5
        SELECT DWH_VERTRAG_ID FROM `project.dataset.fos_table` WHERE DWH_VERTRAG_ID IN (108, 109, 110);
        -- Expected: 0 rows
        ```
    2.  **Logging:** `project.dataset.job_log` should show `SUCCESS` entries, as this is expected data filtering, not an error in job execution.
        ```sql
        SELECT job_status FROM `project.dataset.job_log` WHERE job_name = 'ausd_bp_ta_cntrct_evn' ORDER BY log_ts DESC LIMIT 1;
        -- Expected: 'SUCCESS'
        ```

---

## Test Case 6: Empty Source Table (Data Quality / Row Count)

*   **Purpose:** Verify job behavior when the source `contract_cache` table is empty. The job should run successfully but insert no records.
*   **Setup:**
    1.  Clear `project.dataset.fos_table` and `project.dataset.job_log`.
    2.  Ensure `project.dataset.contract_cache` is empty.
    3.  Define a valid `Stichtag` as `'15032023'`.
*   **Action:** Execute the migrated wrapper procedure.
    ```python
    # pytest-style pseudocode
    def test_empty_source_table():
        # Clear tables
        bq_client.query("TRUNCATE TABLE `project.dataset.fos_table`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.contract_cache`").result() # Ensure empty

        # Execute the migrated job
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`('15032023', NULL)").result()
        # Assertions
        # ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Row Count:** `project.dataset.fos_table` must remain empty.
        ```sql
        SELECT COUNT(1) FROM `project.dataset.fos_table`;
        -- Expected: 0
        ```
    2.  **Logging:** `project.dataset.job_log` must show `START` and `SUCCESS` entries, indicating the job ran successfully but found no data to process.
        ```sql
        SELECT job_status, message
        FROM `project.dataset.job_log`
        WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        ORDER BY log_ts;
        -- Expected:
        -- 'START', 'Wrapper procedure started'
        -- 'START', 'Core procedure started'
        -- 'SUCCESS', 'Core procedure completed successfully'
        -- 'SUCCESS', 'Wrapper procedure completed successfully'
        ```

---

## Test Case 7: Logging Parity (External System Replacement)

*   **Purpose:** Verify that the logging mechanism in BigQuery (`job_log` table) accurately captures job events, parameters, and status, mirroring the information that would be in the legacy log file. This specifically checks the sequence and content of log messages.
*   **Setup:**
    1.  Clear `project.dataset.fos_table` and `project.dataset.job_log`.
    2.  Populate `project.dataset.contract_cache` with the common test data.
    3.  Define `Stichtag` as `'15032023'` and `p_wiederanlaufWert` as `105`.
*   **Action:** Execute the migrated wrapper procedure.
    ```python
    # pytest-style pseudocode
    def test_logging_parity():
        # Clear and populate tables as per setup
        # ...
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`('15032023', 105)").result()
        # Assertions
        # ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Log Entries & Order:** `project.dataset.job_log` must contain exactly 5 entries in the following order (by `log_ts`):
        *   Wrapper START
        *   Core START
        *   Core INFO (for deletion)
        *   Core SUCCESS
        *   Wrapper SUCCESS
        ```sql
        SELECT job_status, message
        FROM `project.dataset.job_log`
        WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        ORDER BY log_ts;
        -- Expected sequence and messages:
        -- 'START', 'Wrapper procedure started'
        -- 'START', 'Core procedure started'
        -- 'INFO', 'Deleted records from target where DWH_VERTRAG_ID >= 105'
        -- 'SUCCESS', 'Core procedure completed successfully'
        -- 'SUCCESS', 'Wrapper procedure completed successfully'
        ```
    2.  **Log Content:** Each entry must have correct `job_name` (`'ausd_bp_ta_cntrct_evn'`), `stichtag` (`'15032023'`), and `restart_value` (`105`). The `job_nr` should be consistent across all entries for a single execution.
        ```sql
        SELECT DISTINCT job_name, stichtag, restart_value FROM `project.dataset.job_log`;
        -- Expected: One row with ('ausd_bp_ta_cntrct_evn', '15032023', 105)
        SELECT COUNT(DISTINCT job_nr) FROM `project.dataset.job_log`;
        -- Expected: 1 (all log entries for one run should share the same job_nr)
        ```

---

## Test Case 8: Idempotency with Restart (Transformation Correctness)

*   **Purpose:** Verify that running the job multiple times with the same restart value and `Stichtag` produces the same final state in `fos_table`. This ensures the deletion logic correctly prepares the table for re-insertion.
*   **Setup:**
    1.  Clear `project.dataset.fos_table` and `project.dataset.job_log`.
    2.  Populate `project.dataset.contract_cache` with the common test data.
    3.  Define `Stichtag` as `'15032023'` and `p_wiederanlaufWert` as `107`.
*   **Action:**
    1.  Execute the job once.
    2.  Capture the state of `fos_table`.
    3.  Clear `job_log` (to avoid clutter for the second run's logs).
    4.  Execute the job a second time with the *same parameters*.
    ```python
    # pytest-style pseudocode
    def test_idempotency_with_restart():
        # Clear and populate tables as per setup
        # ...

        # First run
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`('15032023', 107)").result()
        first_run_data = bq_client.query("SELECT * FROM `project.dataset.fos_table` ORDER BY DWH_VERTRAG_ID").to_dataframe()

        # Clear logs for the second run
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`").result()

        # Second run
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_evn`('15032023', 107)").result()
        second_run_data = bq_client.query("SELECT * FROM `project.dataset.fos_table` ORDER BY DWH_VERTRAG_ID").to_dataframe()

        # Assertions
        # ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Data Parity:** The content of `project.dataset.fos_table` after the second run must be identical to the content captured after the first run.
        ```sql
        -- After first run, capture data:
        -- SELECT DWH_VERTRAG_ID, CONTRACT_DETAILS FROM `project.dataset.fos_table` ORDER BY DWH_VERTRAG_ID;
        -- Expected: 100, 101, 106, 111

        -- After second run, verify data:
        SELECT DWH_VERTRAG_ID, CONTRACT_DETAILS
        FROM `project.dataset.fos_table`
        ORDER BY DWH_VERTRAG_ID;
        -- Expected: Same as above (100, 101, 106, 111)
        ```
    2.  **Row Count:** The row count in `fos_table` should be the same (4 records) after both runs.
        ```sql
        SELECT COUNT(1) FROM `project.dataset.fos_table`;
        -- Expected: 4
        ```

---

## Test Case 9: Schema Assertion (Data Quality)

*   **Purpose:** Verify that the schema of the target `fos_table` after migration is compatible with the expected output, specifically that `src.*` correctly maps columns and types from `contract_cache`.
*   **Setup:**
    1.  Ensure `project.dataset.contract_cache` and `project.dataset.fos_table` are defined with compatible schemas (as per the pre-requisite setup).
    2.  Run a successful execution of the job (e.g., Test Case 1) to ensure data is present in `fos_table`.
*   **Action:** Query the schema of both `project.dataset.contract_cache` and `project.dataset.fos_table`.
    ```python
    # pytest-style pseudocode
    def test_schema_assertion():
        # Ensure a successful run has occurred (e.g., by calling Test Case 1 setup and action)
        # ...

        # Get schema for source
        source_schema = bq_client.get_table("project.dataset.contract_cache").schema
        # Get schema for target
        target_schema = bq_client.get_table("project.dataset.fos_table").schema

        # Assertions
        # ...
    ```
*   **Pass/Fail Criterion:**
    1.  **Schema Match:** The schema of `project.dataset.fos_table` must exactly match the schema of `project.dataset.contract_cache` in terms of column names, data types, and nullability (assuming `src.*` implies a direct copy).
        ```sql
        -- Example SQL to compare schemas (conceptual, actual implementation might vary by tool)
        SELECT
            t1.column_name, t1.data_type, t1.is_nullable,
            t2.column_name, t2.data_type, t2.is_nullable
        FROM
            `project.dataset.INFORMATION_SCHEMA.COLUMNS` AS t1
        FULL OUTER JOIN
            `project.dataset.INFORMATION_SCHEMA.COLUMNS` AS t2
        ON
            t1.column_name = t2.column_name AND t1.table_name = 'contract_cache' AND t2.table_name = 'fos_table'
        WHERE
            t1.table_name = 'contract_cache' OR t2.table_name = 'fos_table'
        HAVING
            t1.data_type != t2.data_type OR t1.is_nullable != t2.is_nullable OR t1.column_name IS NULL OR t2.column_name IS NULL;
        -- Expected: 0 rows (indicating identical schemas)
        ```