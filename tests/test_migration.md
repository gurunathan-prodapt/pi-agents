As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `k_ausd_bp_ta_rn_da_vda_tk.ksh` to a BigQuery Stored Procedure. These tests focus on ensuring behavioral equivalence, covering parameter handling, date validation, orchestration, record count capture, and robust error logging.

A key aspect of testing this orchestration script is its interaction with the core SQL logic (`d_ausd_bp_ta_rn_da_vda_tk.sql`). Since the core SQL is a separate migration, for these tests, we will use a **mock BigQuery Stored Procedure** for `d_ausd_bp_ta_rn_da_vda_tk`. This mock will simulate its behavior, including inserting data into a target table and optionally raising errors, allowing us to isolate and test the orchestration logic.

---

## Pre-requisite Setup for All Tests

Before running any tests, the following BigQuery objects must be created. These represent the target tables and the mock for the core SQL logic.

```sql
-- DDL for job_run_log table
CREATE TABLE IF NOT EXISTS my_project.my_dataset.job_run_log (
    tab_name STRING NOT NULL OPTIONS(description="Name of the job or table processed"),
    job_kennung STRING OPTIONS(description="Job identifier from input parameters"),
    eintrags_nr STRING OPTIONS(description="Entry number from input parameters"),
    stichtag STRING OPTIONS(description="Processing date (DDMMYYYY) from input parameters"),
    wiederanlauf_wert STRING OPTIONS(description="Restart value from input parameters"),
    records_processed INT64 OPTIONS(description="Number of records processed or inserted"),
    created_at TIMESTAMP OPTIONS(description="Timestamp of the log entry")
);

-- DDL for job_error_log table
CREATE TABLE IF NOT EXISTS my_project.my_dataset.job_error_log (
    job_name STRING NOT NULL OPTIONS(description="Name of the job that failed"),
    entry_nr STRING OPTIONS(description="Entry number associated with the job run"),
    stichtag STRING OPTIONS(description="Processing date (DDMMYYYY) during the failure"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    created_at TIMESTAMP OPTIONS(description="Timestamp of the error entry")
);

-- DDL for bp_target_table (simulates the output of the core SQL)
CREATE TABLE IF NOT EXISTS my_project.my_dataset.bp_target_table (
    id STRING,
    value STRING,
    processing_date DATE
);

-- Mock BigQuery Stored Procedure for d_ausd_bp_ta_rn_da_vda_tk
-- This mock simulates the behavior of the core SQL logic for testing purposes.
CREATE OR REPLACE PROCEDURE my_project.my_dataset.d_ausd_bp_ta_rn_da_vda_tk(
    IN p_stichtag_date DATE,
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_wiederanlaufWert STRING
)
BEGIN
    -- Clear target table for predictable testing
    TRUNCATE TABLE my_project.my_dataset.bp_target_table;

    -- Simulate data processing based on parameters
    -- If p_EintragsNr is 'FAIL_CORE_SQL', simulate an error in the core SQL.
    IF p_EintragsNr = 'FAIL_CORE_SQL' THEN
        RAISE USING MESSAGE 'Simulated error in core SQL procedure: d_ausd_bp_ta_rn_da_vda_tk.';
    END IF;

    -- Insert dummy data into the target table.
    -- The number of rows is controlled by p_EintragsNr for record count tests.
    -- If p_EintragsNr can be parsed as an integer, it will determine the row count.
    -- Otherwise, default to 5 rows.
    DECLARE num_rows INT64;
    SET num_rows = SAFE_CAST(p_EintragsNr AS INT64);
    IF num_rows IS NULL OR num_rows <= 0 THEN
        SET num_rows = 5; -- Default rows if EintragsNr is not a positive integer
    END IF;

    INSERT INTO my_project.my_dataset.bp_target_table (id, value, processing_date)
    SELECT
        GENERATE_UUID(),
        FORMAT('Data for Job: %s, Entry: %s, Stichtag: %s, Restart: %s', p_JobKennung, p_EintragsNr, FORMAT_DATE('%Y%m%d', p_stichtag_date), p_wiederanlaufWert),
        p_stichtag_date
    FROM UNNEST(GENERATE_ARRAY(1, num_rows));
END;
```

---

## Test Case 1: Successful Execution - Output Parity & Logging

**Purpose:** Verify the migrated stored procedure executes successfully with valid inputs, orchestrates the mock core SQL, correctly captures the record count, and logs the successful run. This covers output parity for job status and logging.

**Setup:**
1.  Ensure the `job_run_log`, `job_error_log`, `bp_target_table`, and mock `d_ausd_bp_ta_rn_da_vda_tk` exist.
2.  Clear previous log entries and target table data to ensure a clean state for the test.

```sql
TRUNCATE TABLE my_project.my_dataset.job_run_log;
TRUNCATE TABLE my_project.my_dataset.job_error_log;
TRUNCATE TABLE my_project.my_dataset.bp_target_table;
```

**Action:**
Execute the migrated BigQuery Stored Procedure with a set of valid parameters.

```sql
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    'TEST_JOB_001',
    '10', -- This will make the mock core SQL insert 10 rows
    '01012023',
    '1'
);
```

**Pass/Fail Criterion:**
*   The `CALL` statement completes without raising an error.
*   One row exists in `my_project.my_dataset.job_run_log` with:
    *   `tab_name` = 'k_ausd_bp_ta_rn_da_vda_tk'
    *   `job_kennung` = 'TEST_JOB_001'
    *   `eintrags_nr` = '10'
    *   `stichtag` = '01012023'
    *   `wiederanlauf_wert` = '1'
    *   `records_processed` = 10 (matching the rows inserted by the mock core SQL)
*   Zero rows exist in `my_project.my_dataset.job_error_log`.

```python
# Pytest assertion example
def test_successful_execution(bigquery_client):
    # Action
    bigquery_client.query("""
        CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
            'TEST_JOB_001', '10', '01012023', '1'
        );
    """).result()

    # Assertions
    run_log_rows = list(bigquery_client.query("SELECT * FROM my_project.my_dataset.job_run_log").result())
    error_log_rows = list(bigquery_client.query("SELECT * FROM my_project.my_dataset.job_error_log").result())

    assert len(run_log_rows) == 1
    assert run_log_rows[0]['tab_name'] == 'k_ausd_bp_ta_rn_da_vda_tk'
    assert run_log_rows[0]['job_kennung'] == 'TEST_JOB_001'
    assert run_log_rows[0]['eintrags_nr'] == '10'
    assert run_log_rows[0]['stichtag'] == '01012023'
    assert run_log_rows[0]['wiederanlauf_wert'] == '1'
    assert run_log_rows[0]['records_processed'] == 10
    assert len(error_log_rows) == 0
```

---

## Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify the procedure correctly identifies and raises an error when `p_JobKennung` is missing (NULL or empty string), mirroring the legacy script's `pruefeParameterGesetzt` behavior.

**Setup:**
1.  Ensure the `job_run_log` and `job_error_log` tables exist.
2.  Clear previous log entries.

```sql
TRUNCATE TABLE my_project.my_dataset.job_run_log;
TRUNCATE TABLE my_project.my_dataset.job_error_log;
```

**Action:**
Attempt to execute the procedure with `p_JobKennung` as NULL and then as an empty string.

```sql
-- Attempt 1: p_JobKennung is NULL
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    NULL,
    '10',
    '01012023',
    '0'
);

-- Attempt 2: p_JobKennung is an empty string
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    '',
    '10',
    '01012023',
    '0'
);
```

**Pass/Fail Criterion:**
*   Both `CALL` statements must raise an error.
*   The error message for both attempts should contain "ERROR: JobKennung must be provided."
*   Two rows exist in `my_project.my_dataset.job_error_log`, each corresponding to an attempt, with `error_message` containing the expected text.
*   Zero rows exist in `my_project.my_dataset.job_run_log`.

```python
# Pytest assertion example
import pytest

def test_missing_jobkennung(bigquery_client):
    # Action 1: NULL
    with pytest.raises(Exception) as excinfo:
        bigquery_client.query("CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(NULL, '10', '01012023', '0');").result()
    assert "ERROR: JobKennung must be provided." in str(excinfo.value)

    # Action 2: Empty string
    with pytest.raises(Exception) as excinfo:
        bigquery_client.query("CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('', '10', '01012023', '0');").result()
    assert "ERROR: JobKennung must be provided." in str(excinfo.value)

    # Assertions
    error_log_rows = list(bigquery_client.query("SELECT error_message FROM my_project.my_dataset.job_error_log").result())
    run_log_rows = list(bigquery_client.query("SELECT * FROM my_project.my_dataset.job_run_log").result())

    assert len(error_log_rows) == 2
    assert "ERROR: JobKennung must be provided." in error_log_rows[0]['error_message']
    assert "ERROR: JobKennung must be provided." in error_log_rows[1]['error_message']
    assert len(run_log_rows) == 0
```

---

## Test Case 3: Date Validation - Invalid `p_Stichtag` Format

**Purpose:** Verify the procedure correctly identifies and raises an error when `p_Stichtag` is not in the expected `DDMMYYYY` format, replicating `DWDate_Datum_Check` functionality.

**Setup:**
1.  Ensure the `job_run_log` and `job_error_log` tables exist.
2.  Clear previous log entries.

```sql
TRUNCATE TABLE my_project.my_dataset.job_run_log;
TRUNCATE TABLE my_project.my_dataset.job_error_log;
```

**Action:**
Attempt to execute the procedure with `p_Stichtag` in an incorrect format (e.g., '2023-01-01', '01/01/2023', 'INVALID_DATE').

```sql
-- Attempt 1: YYYY-MM-DD format
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    'TEST_JOB_002',
    '1',
    '2023-01-01',
    '0'
);

-- Attempt 2: MM/DD/YYYY format
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    'TEST_JOB_002',
    '1',
    '01/01/2023',
    '0'
);

-- Attempt 3: Completely invalid string
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    'TEST_JOB_002',
    '1',
    'INVALID_DATE',
    '0'
);
```

**Pass/Fail Criterion:**
*   All `CALL` statements must raise an error.
*   The error message for all attempts should contain "ERROR: Stichtag must be in DDMMYYYY format."
*   Three rows exist in `my_project.my_dataset.job_error_log`, each corresponding to an attempt, with `error_message` containing the expected text.
*   Zero rows exist in `my_project.my_dataset.job_run_log`.

```python
# Pytest assertion example
import pytest

def test_invalid_stichtag_format(bigquery_client):
    invalid_dates = ['2023-01-01', '01/01/2023', 'INVALID_DATE']
    for date_str in invalid_dates:
        with pytest.raises(Exception) as excinfo:
            bigquery_client.query(f"CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('TEST_JOB_002', '1', '{date_str}', '0');").result()
        assert "ERROR: Stichtag must be in DDMMYYYY format." in str(excinfo.value)

    # Assertions
    error_log_rows = list(bigquery_client.query("SELECT error_message FROM my_project.my_dataset.job_error_log").result())
    run_log_rows = list(bigquery_client.query("SELECT * FROM my_project.my_dataset.job_run_log").result())

    assert len(error_log_rows) == len(invalid_dates)
    for row in error_log_rows:
        assert "ERROR: Stichtag must be in DDMMYYYY format." in row['error_message']
    assert len(run_log_rows) == 0
```

---

## Test Case 4: `p_wiederanlaufWert` Handling (NULL/Empty)

**Purpose:** Verify that `p_wiederanlaufWert` correctly defaults to '0' when provided as NULL or an empty string, as specified in the migration design. This covers transformation correctness for NULL handling.

**Setup:**
1.  Ensure the `job_run_log`, `job_error_log`, `bp_target_table`, and mock `d_ausd_bp_ta_rn_da_vda_tk` exist.
2.  Clear previous log entries and target table data.

```sql
TRUNCATE TABLE my_project.my_dataset.job_run_log;
TRUNCATE TABLE my_project.my_dataset.job_error_log;
TRUNCATE TABLE my_project.my_dataset.bp_target_table;
```

**Action:**
Execute the procedure twice: once with `p_wiederanlaufWert` as NULL, and once as an empty string.

```sql
-- Attempt 1: p_wiederanlaufWert is NULL
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    'TEST_JOB_003_NULL',
    '5',
    '02012023',
    NULL
);

-- Attempt 2: p_wiederanlaufWert is an empty string
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    'TEST_JOB_003_EMPTY',
    '5',
    '03012023',
    ''
);
```

**Pass/Fail Criterion:**
*   Both `CALL` statements complete without raising an error.
*   Two rows exist in `my_project.my_dataset.job_run_log`.
*   For both entries, `wiederanlauf_wert` must be '0'.
*   Zero rows exist in `my_project.my_dataset.job_error_log`.

```python
# Pytest assertion example
def test_wiederanlaufwert_defaulting(bigquery_client):
    # Action 1: NULL
    bigquery_client.query("CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('TEST_JOB_003_NULL', '5', '02012023', NULL);").result()

    # Action 2: Empty string
    bigquery_client.query("CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('TEST_JOB_003_EMPTY', '5', '03012023', '');").result()

    # Assertions
    run_log_rows = list(bigquery_client.query("SELECT job_kennung, wiederanlauf_wert FROM my_project.my_dataset.job_run_log ORDER BY job_kennung").result())
    error_log_rows = list(bigquery_client.query("SELECT * FROM my_project.my_dataset.job_error_log").result())

    assert len(run_log_rows) == 2
    assert run_log_rows[0]['job_kennung'] == 'TEST_JOB_003_EMPTY'
    assert run_log_rows[0]['wiederanlauf_wert'] == '0'
    assert run_log_rows[1]['job_kennung'] == 'TEST_JOB_003_NULL'
    assert run_log_rows[1]['wiederanlauf_wert'] == '0'
    assert len(error_log_rows) == 0
```

---

## Test Case 5: Error Handling - Core SQL Procedure Failure

**Purpose:** Verify that if the invoked core SQL procedure (`d_ausd_bp_ta_rn_da_vda_tk`) raises an error, the orchestration procedure catches it, logs it to `job_error_log`, and re-raises it to signal failure to the caller. This covers external system replacement error handling.

**Setup:**
1.  Ensure the `job_run_log`, `job_error_log`, `bp_target_table`, and mock `d_ausd_bp_ta_rn_da_vda_tk` exist.
2.  Clear previous log entries and target table data.
3.  The mock `d_ausd_bp_ta_rn_da_vda_tk` is designed to fail if `p_EintragsNr` is 'FAIL_CORE_SQL'.

```sql
TRUNCATE TABLE my_project.my_dataset.job_run_log;
TRUNCATE TABLE my_project.my_dataset.job_error_log;
TRUNCATE TABLE my_project.my_dataset.bp_target_table;
```

**Action:**
Execute the orchestration procedure with parameters that cause the mock core SQL to fail.

```sql
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    'TEST_JOB_004',
    'FAIL_CORE_SQL', -- This will trigger a failure in the mock d_ausd_bp_ta_rn_da_vda_tk
    '04012023',
    '0'
);
```

**Pass/Fail Criterion:**
*   The `CALL` statement must raise an error.
*   The error message should contain "Simulated error in core SQL procedure: d_ausd_bp_ta_rn_da_vda_tk."
*   One row exists in `my_project.my_dataset.job_error_log` with:
    *   `job_name` = 'k_ausd_bp_ta_rn_da_vda_tk'
    *   `entry_nr` = 'FAIL_CORE_SQL'
    *   `stichtag` = '04012023'
    *   `error_message` containing "Simulated error in core SQL procedure: d_ausd_bp_ta_rn_da_vda_tk."
*   Zero rows exist in `my_project.my_dataset.job_run_log`.

```python
# Pytest assertion example
import pytest

def test_core_sql_failure_handling(bigquery_client):
    # Action
    with pytest.raises(Exception) as excinfo:
        bigquery_client.query("CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('TEST_JOB_004', 'FAIL_CORE_SQL', '04012023', '0');").result()
    assert "Simulated error in core SQL procedure: d_ausd_bp_ta_rn_da_vda_tk." in str(excinfo.value)

    # Assertions
    error_log_rows = list(bigquery_client.query("SELECT * FROM my_project.my_dataset.job_error_log").result())
    run_log_rows = list(bigquery_client.query("SELECT * FROM my_project.my_dataset.job_run_log").result())

    assert len(error_log_rows) == 1
    assert error_log_rows[0]['job_name'] == 'k_ausd_bp_ta_rn_da_vda_tk'
    assert error_log_rows[0]['entry_nr'] == 'FAIL_CORE_SQL'
    assert error_log_rows[0]['stichtag'] == '04012023'
    assert "Simulated error in core SQL procedure: d_ausd_bp_ta_rn_da_vda_tk." in error_log_rows[0]['error_message']
    assert len(run_log_rows) == 0
```

---

## Test Case 6: Date Derivation Correctness (Internal Check)

**Purpose:** Verify that the internal date derivation (`v_datum_heute`, `v_datum_gestern`) using BigQuery functions (`CURRENT_DATE()`, `DATE_SUB()`) works as expected. While these are not directly passed to the core SQL in the generated code, their correct derivation is part of the transformation logic.

**Setup:**
1.  This test requires a slight modification or an additional logging step within the `r_ausd_bp_ta_rn_da_vda_tk` procedure to expose these internal variables, or a separate helper procedure to test date functions. For the purpose of this exercise, we'll assume we can inspect the values if they were logged or passed.
2.  Alternatively, we can assert the `p_stichtag_date` passed to the mock core SQL is correctly parsed.

**Action:**
Execute the migrated BigQuery Stored Procedure with a valid `p_Stichtag`.

```sql
CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    'TEST_JOB_005',
    '1',
    '15032024', -- Example Stichtag
    '0'
);
```

**Pass/Fail Criterion:**
*   The `CALL` statement completes successfully.
*   The `job_run_log` entry confirms successful execution.
*   (Implicit/Requires internal logging): If `v_datum_heute` and `v_datum_gestern` were logged or passed to the mock core SQL, verify they match `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` respectively, for the date the test was run.
*   The `p_stichtag_date` parameter passed to the mock `d_ausd_bp_ta_rn_da_vda_tk` should be `DATE('2024-03-15')`.

```python
# Pytest assertion example (assuming mock core SQL logs received parameters)
import datetime

def test_date_derivation_correctness(bigquery_client):
    # Action
    bigquery_client.query("CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('TEST_JOB_005', '1', '15032024', '0');").result()

    # Assertions for job_run_log (standard check)
    run_log_rows = list(bigquery_client.query("SELECT * FROM my_project.my_dataset.job_run_log WHERE job_kennung = 'TEST_JOB_005'").result())
    assert len(run_log_rows) == 1
    assert run_log_rows[0]['stichtag'] == '15032024'

    # To directly test v_datum_heute and v_datum_gestern, we'd need to modify
    # the r_ausd_bp_ta_rn_da_vda_tk procedure to log these values, or
    # modify the mock d_ausd_bp_ta_rn_da_vda_tk to capture them and make them queryable.
    # For now, we can verify the stichtag_date passed to the mock.
    # This requires a way to inspect the parameters received by the mock.
    # A more robust mock would store these parameters in a temporary table.

    # Example of how to verify p_stichtag_date passed to mock
    # (This would require the mock to log its inputs)
    # For this test, we'll rely on the successful execution and the fact that
    # SAFE.PARSE_DATE was used, which is a standard BQ function.
    # The `v_stichtag_date` is passed to the mock, so we can infer its correctness.
    target_table_rows = list(bigquery_client.query("SELECT processing_date FROM my_project.my_dataset.bp_target_table LIMIT 1").result())
    assert len(target_table_rows) > 0
    assert target_table_rows[0]['processing_date'] == datetime.date(2024, 3, 15)

    # For v_datum_heute and v_datum_gestern, a separate BQ function test or
    # temporary logging within the SP would be ideal.
    # For example, a helper function:
    # CREATE OR REPLACE FUNCTION my_project.my_dataset.get_today() RETURNS DATE AS (CURRENT_DATE());
    # CREATE OR REPLACE FUNCTION my_project.my_dataset.get_yesterday() RETURNS DATE AS (DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));
    # Then assert these functions return expected values.
```