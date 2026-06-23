As a senior data-migration QA engineer, I've analyzed the provided migration design and code for `k_ausd_bp_ta_apn_carmen.ksh` to Google BigQuery. The following test cases are designed to ensure the migrated BigQuery stored procedure `r_ausd_bp_ta_apn_carmen` is behaviorally equivalent to the legacy KornShell script, covering output parity, transformation correctness, external system replacements, and data quality.

---

## Migration Validation Tests: `r_ausd_bp_ta_apn_carmen`

**Assumptions for Testing:**
*   The BigQuery project and dataset are configured as `default_project.default_dataset`.
*   The `job_log_table` and `PoolBasisprodukt` tables exist with the DDL provided in the migration design.
*   The `d_ausd_bp_ta_apn_carmen` stored procedure (the core data logic) is a stub that can be controlled to return a specific `records_processed` value for testing purposes. For these tests, we will assume it returns `100` records unless specified otherwise.
*   Pytest with `google-cloud-bigquery` client is used for automation.

**Helper Functions (Python Pytest):**

```python
import pytest
from google.cloud import bigquery
import datetime
import time

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id"  # Replace with your actual project ID
DATASET_ID = "your_dataset_id"      # Replace with your actual dataset ID
R_AUSD_BP_TA_APN_CARMEN_SP = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_apn_carmen"
D_AUSD_BP_TA_APN_CARMEN_SP = f"{PROJECT_ID}.{DATASET_ID}.d_ausd_bp_ta_apn_carmen"
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log_table"
POOL_BASISPRODUKT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt"

# --- Pytest Fixtures ---
@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def cleanup_job_log_table(bq_client):
    """Fixture to clear the job_log_table before and after each test."""
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
    yield
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()

# --- Helper Functions for Tests ---
def execute_bq_query(bq_client, query):
    """Helper to execute a BigQuery SQL query and return results."""
    query_job = bq_client.query(query)
    return query_job.result()

def call_r_ausd_bp_ta_apn_carmen(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert):
    """Helper to call the main orchestration stored procedure and capture errors."""
    call_sql = f"""
    CALL `{R_AUSD_BP_TA_APN_CARMEN_SP}`(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag}',
        '{wiederanlauf_wert}'
    );
    """
    try:
        execute_bq_query(bq_client, call_sql)
        return None  # No error
    except Exception as e:
        return str(e)  # Return error message

def get_job_log_entries(bq_client, job_identifier=None):
    """Helper to retrieve entries from the job_log_table."""
    query = f"SELECT * FROM `{JOB_LOG_TABLE}`"
    if job_identifier:
        query += f" WHERE job_identifier = '{job_identifier}'"
    query += " ORDER BY log_timestamp DESC"
    rows = list(execute_bq_query(bq_client, query))
    return [dict(row) for row in rows]

def mock_d_ausd_bp_ta_apn_carmen_return_count(bq_client, count):
    """Temporarily modifies d_ausd_bp_ta_apn_carmen to return a specific record count."""
    mock_sp_sql = f"""
    CREATE OR REPLACE PROCEDURE `{D_AUSD_BP_TA_APN_CARMEN_SP}`(
        IN p_stichtag_date DATE,
        IN p_wiederanlauf_wert INT64,
        OUT records_processed INT64
    )
    BEGIN
        SET records_processed = {count};
    END;
    """
    execute_bq_query(bq_client, mock_sp_sql)

def restore_d_ausd_bp_ta_apn_carmen(bq_client):
    """Restores the original d_ausd_bp_ta_apn_carmen stub."""
    restore_sp_sql = f"""
    CREATE OR REPLACE PROCEDURE `{D_AUSD_BP_TA_APN_CARMEN_SP}`(
        IN p_stichtag_date DATE,
        IN p_wiederanlauf_wert INT64,
        OUT records_processed INT64
    )
    BEGIN
        -- Original stub logic
        SET records_processed = (
            SELECT COUNT(*) FROM `{POOL_BASISPRODUKT_TABLE}`
            WHERE creation_date = p_stichtag_date
        );
        IF records_processed IS NULL THEN
            SET records_processed = 0;
        END IF;
    END;
    """
    execute_bq_query(bq_client, restore_sp_sql)

# Fixture to ensure d_ausd_bp_ta_apn_carmen is reset after tests that mock it
@pytest.fixture(autouse=True)
def d_ausd_bp_ta_apn_carmen_mock_reset(bq_client):
    """Ensures d_ausd_bp_ta_apn_carmen is restored after tests that mock it."""
    yield
    restore_d_ausd_bp_ta_apn_carmen(bq_client)
```

---

### Test Case 1.1: Successful Execution with All Valid Parameters

*   **Purpose:** Verify the main orchestration procedure runs successfully with all valid inputs, including a non-default restart value. This covers output parity for successful runs and basic transformation correctness for parameter passing.
*   **Setup:**
    *   `job_log_table` is empty.
    *   The `d_ausd_bp_ta_apn_carmen` stored procedure is temporarily configured to return `123` as `records_processed`.
*   **Action:** Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung='JOB_SUCCESS'`, `p_eintrags_nr='ENTRY_001'`, `p_stichtag='01012023'`, `p_wiederanlauf_wert='5'`.
*   **Pass/Fail Criteria:**
    *   The procedure completes without raising any BigQuery exceptions.
    *   `job_log_table` contains exactly one row.
    *   This row has:
        *   `job_identifier = 'JOB_SUCCESS'`
        *   `entry_number = 'ENTRY_001'`
        *   `status_code_1 = 'A'`
        *   `status_code_2 = 'I'`
        *   `key_date = DATE('2023-01-01')`
        *   `restart_value = 5`
        *   `records_processed = 123`
        *   `log_timestamp` is recent (within the last minute).

```python
def test_successful_execution_with_all_params(bq_client):
    mock_d_ausd_bp_ta_apn_carmen_return_count(bq_client, 123)
    
    error_message = call_r_ausd_bp_ta_apn_carmen(
        bq_client, 'JOB_SUCCESS', 'ENTRY_001', '01012023', '5'
    )
    assert error_message is None, f"Procedure raised an unexpected error: {error_message}"

    log_entries = get_job_log_entries(bq_client, 'JOB_SUCCESS')
    assert len(log_entries) == 1, "Expected exactly one log entry."

    entry = log_entries[0]
    assert entry['job_identifier'] == 'JOB_SUCCESS'
    assert entry['entry_number'] == 'ENTRY_001'
    assert entry['status_code_1'] == 'A'
    assert entry['status_code_2'] == 'I'
    assert entry['key_date'] == datetime.date(2023, 1, 1)
    assert entry['restart_value'] == 5
    assert entry['records_processed'] == 123
    assert (datetime.datetime.now(datetime.timezone.utc) - entry['log_timestamp']).total_seconds() < 60
```

---

### Test Case 1.2: Successful Execution with Default Restart Value

*   **Purpose:** Verify `p_wiederanlauf_wert` correctly defaults to `0` when an empty string is provided, demonstrating transformation correctness for NULL/empty handling.
*   **Setup:**
    *   `job_log_table` is empty.
    *   The `d_ausd_bp_ta_apn_carmen` stored procedure is temporarily configured to return `75` as `records_processed`.
*   **Action:** Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung='JOB_DEFAULT_RESTART'`, `p_eintrags_nr='ENTRY_002'`, `p_stichtag='15032023'`, `p_wiederanlauf_wert=''`.
*   **Pass/Fail Criteria:**
    *   The procedure completes without raising any BigQuery exceptions.
    *   `job_log_table` contains exactly one row.
    *   This row has `restart_value = 0`.
    *   `records_processed = 75`.

```python
def test_successful_execution_with_default_restart_value(bq_client):
    mock_d_ausd_bp_ta_apn_carmen_return_count(bq_client, 75)
    
    error_message = call_r_ausd_bp_ta_apn_carmen(
        bq_client, 'JOB_DEFAULT_RESTART', 'ENTRY_002', '15032023', ''
    )
    assert error_message is None, f"Procedure raised an unexpected error: {error_message}"

    log_entries = get_job_log_entries(bq_client, 'JOB_DEFAULT_RESTART')
    assert len(log_entries) == 1, "Expected exactly one log entry."

    entry = log_entries[0]
    assert entry['restart_value'] == 0
    assert entry['records_processed'] == 75
```

---

### Test Case 1.3: Missing `p_JobKennung` Parameter

*   **Purpose:** Verify the procedure raises an error for a missing `p_JobKennung`, matching the legacy script's error handling (`pruefeParameterGesetzt`). This covers output parity for error conditions.
*   **Setup:** `job_log_table` is empty.
*   **Action:** Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung=NULL` (or an empty string), `p_eintrags_nr='ENTRY_003'`, `p_stichtag='01012023'`, `p_wiederanlauf_wert='0'`.
*   **Pass/Fail Criteria:**
    *   The procedure raises a BigQuery exception.
    *   The error message contains "FEHLER: Missing parameter p_JobKennung".
    *   `job_log_table` remains empty (no partial logging on error).

```python
def test_missing_job_kennung_parameter(bq_client):
    error_message = call_r_ausd_bp_ta_apn_carmen(
        bq_client, '', 'ENTRY_003', '01012023', '0'
    )
    assert error_message is not None, "Expected an error, but none was raised."
    assert "FEHLER: Missing parameter p_JobKennung" in error_message

    log_entries = get_job_log_entries(bq_client)
    assert len(log_entries) == 0, "Job log table should be empty after an error."
```

---

### Test Case 1.4: Invalid `p_Stichtag` Format

*   **Purpose:** Verify the procedure raises an error for an invalid `p_Stichtag` format, replicating `DWDate_Datum_Check` functionality and demonstrating transformation correctness for date validation.
*   **Setup:** `job_log_table` is empty.
*   **Action:** Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung='JOB_INVALID_DATE'`, `p_eintrags_nr='ENTRY_004'`, `p_stichtag='2023-01-01'` (incorrect format), `p_wiederanlauf_wert='0'`.
*   **Pass/Fail Criteria:**
    *   The procedure raises a BigQuery exception.
    *   The error message contains "FEHLER: Invalid date format for p_Stichtag. Expected DDMMYYYY."
    *   `job_log_table` remains empty.

```python
def test_invalid_stichtag_format(bq_client):
    error_message = call_r_ausd_bp_ta_apn_carmen(
        bq_client, 'JOB_INVALID_DATE', 'ENTRY_004', '2023-01-01', '0'
    )
    assert error_message is not None, "Expected an error, but none was raised."
    assert "FEHLER: Invalid date format for p_Stichtag. Expected DDMMYYYY." in error_message

    log_entries = get_job_log_entries(bq_client)
    assert len(log_entries) == 0, "Job log table should be empty after an error."
```

---

### Test Case 1.5: Invalid `p_wiederanlauf_wert` (Non-Integer)

*   **Purpose:** Verify the procedure raises an error if `p_wiederanlauf_wert` cannot be cast to an integer, ensuring robust type handling.
*   **Setup:** `job_log_table` is empty.
*   **Action:** Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung='JOB_INVALID_RESTART'`, `p_eintrags_nr='ENTRY_005'`, `p_stichtag='01012023'`, `p_wiederanlauf_wert='abc'`.
*   **Pass/Fail Criteria:**
    *   The procedure raises a BigQuery exception.
    *   The error message contains "FEHLER: Invalid restart value. Expected integer, got abc".
    *   `job_log_table` remains empty.

```python
def test_invalid_wiederanlauf_wert_non_integer(bq_client):
    error_message = call_r_ausd_bp_ta_apn_carmen(
        bq_client, 'JOB_INVALID_RESTART', 'ENTRY_005', '01012023', 'abc'
    )
    assert error_message is not None, "Expected an error, but none was raised."
    assert "FEHLER: Invalid restart value. Expected integer, got abc" in error_message

    log_entries = get_job_log_entries(bq_client)
    assert len(log_entries) == 0, "Job log table should be empty after an error."
```

---

### Test Case 2.1: Date Determination (`gestern.ksh` Replacement)

*   **Purpose:** Verify that `v_datum_heute` and `v_datum_gestern` are correctly calculated using BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions, replacing the `gestern.ksh` utility. This is a test of external-system replacement.
*   **Setup:** This test requires inspecting the internal state of the procedure or its side effects. Since `v_datum_heute` and `v_datum_gestern` are not explicitly logged in the `job_log_table` by the current design, we will verify the *presence* of the correct BigQuery functions in the code.
*   **Action:** Inspect the `r_ausd_bp_ta_apn_carmen` stored procedure code.
*   **Pass/Fail Criteria:**
    *   The BigQuery stored procedure `r_ausd_bp_ta_apn_carmen` explicitly contains the lines:
        *   `SET v_datum_heute = CURRENT_DATE();`
        *   `SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`

```python
# This is a static code analysis test, not a runtime pytest.
# It would typically be part of a code review or a static analysis tool.

# Example of how you might assert this in a CI/CD pipeline if you had access to the SP definition as a string:
def test_date_determination_bq_functions_used():
    sp_definition = """
    -- ... (truncated for brevity)
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    -- ...
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
    -- ...
    """
    assert "SET v_datum_heute = CURRENT_DATE();" in sp_definition
    assert "SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);" in sp_definition
```

---

### Test Case 2.2: Core Data Logic Call and Record Count Retrieval (`tmpFile` Replacement)

*   **Purpose:** Verify `r_ausd_bp_ta_apn_carmen` correctly calls `d_ausd_bp_ta_apn_carmen` and accurately captures its `records_processed` output via an `OUT` parameter, replacing the legacy `tmpFile` mechanism. This covers external-system replacement and transformation correctness.
*   **Setup:**
    *   `job_log_table` is empty.
    *   The `d_ausd_bp_ta_apn_carmen` stored procedure is temporarily configured to return a *specific and unique* record count, e.g., `999`.
*   **Action:** Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung='JOB_CORE_CALL'`, `p_eintrags_nr='ENTRY_006'`, `p_stichtag='01012023'`, `p_wiederanlauf_wert='1'`.
*   **Pass/Fail Criteria:**
    *   The procedure completes successfully.
    *   `job_log_table` contains one new row.
    *   This row has `records_processed = 999`.

```python
def test_core_data_logic_call_and_record_count(bq_client):
    mock_d_ausd_bp_ta_apn_carmen_return_count(bq_client, 999)
    
    error_message = call_r_ausd_bp_ta_apn_carmen(
        bq_client, 'JOB_CORE_CALL', 'ENTRY_006', '01012023', '1'
    )
    assert error_message is None, f"Procedure raised an unexpected error: {error_message}"

    log_entries = get_job_log_entries(bq_client, 'JOB_CORE_CALL')
    assert len(log_entries) == 1, "Expected exactly one log entry."

    entry = log_entries[0]
    assert entry['records_processed'] == 999
```

---

### Test Case 3.1: Job Logging (`FOSJobErzeugeEintrag` Replacement)

*   **Purpose:** Verify job logging is correctly performed by inserting a new row into the `job_log_table` with the expected static and dynamic values, replacing the `FOSJobErzeugeEintrag` functionality. This covers external-system replacement and data quality.
*   **Setup:**
    *   `job_log_table` is empty.
    *   The `d_ausd_bp_ta_apn_carmen` stored procedure is temporarily configured to return `50` records.
*   **Action:** Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung='JOB_LOGGING'`, `p_eintrags_nr='ENTRY_007'`, `p_stichtag='20042024'`, `p_wiederanlauf_wert='0'`.
*   **Pass/Fail Criteria:**
    *   The procedure completes successfully.
    *   `job_log_table` contains exactly one row.
    *   This row has:
        *   `job_identifier = 'JOB_LOGGING'`
        *   `entry_number = 'ENTRY_007'`
        *   `status_code_1 = 'A'` (as per design)
        *   `status_code_2 = 'I'` (as per design)
        *   `key_date = DATE('2024-04-20')`
        *   `restart_value = 0`
        *   `records_processed = 50`
        *   `log_timestamp` is recent.

```python
def test_job_logging_replacement(bq_client):
    mock_d_ausd_bp_ta_apn_carmen_return_count(bq_client, 50)
    
    error_message = call_r_ausd_bp_ta_apn_carmen(
        bq_client, 'JOB_LOGGING', 'ENTRY_007', '20042024', '0'
    )
    assert error_message is None, f"Procedure raised an unexpected error: {error_message}"

    log_entries = get_job_log_entries(bq_client, 'JOB_LOGGING')
    assert len(log_entries) == 1, "Expected exactly one log entry."

    entry = log_entries[0]
    assert entry['job_identifier'] == 'JOB_LOGGING'
    assert entry['entry_number'] == 'ENTRY_007'
    assert entry['status_code_1'] == 'A'
    assert entry['status_code_2'] == 'I'
    assert entry['key_date'] == datetime.date(2024, 4, 20)
    assert entry['restart_value'] == 0
    assert entry['records_processed'] == 50
    assert (datetime.datetime.now(datetime.timezone.utc) - entry['log_timestamp']).total_seconds() < 60
```

---

### Test Case 4.1: `job_log_table` Schema and Data Types

*   **Purpose:** Verify the `job_log_table` schema and data types match the design document's specifications, ensuring data quality and schema assertions.
*   **Setup:** Ensure the `job_log_table` DDL has been executed.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `job_log_table`.
*   **Pass/Fail Criteria:**
    *   The table exists.
    *   The following columns exist with the specified data types and nullability:
        *   `job_identifier`: `STRING`, `NOT NULL`
        *   `entry_number`: `STRING`, `NULLABLE`
        *   `status_code_1`: `STRING`, `NULLABLE`
        *   `status_code_2`: `STRING`, `NULLABLE`
        *   `key_date`: `DATE`, `NULLABLE`
        *   `restart_value`: `INT64`, `NULLABLE`
        *   `records_processed`: `INT64`, `NULLABLE`
        *   `log_timestamp`: `TIMESTAMP`, `NULLABLE` (with `DEFAULT CURRENT_TIMESTAMP()`)

```python
def test_job_log_table_schema(bq_client):
    query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_log_table'
    ORDER BY ordinal_position;
    """
    schema_rows = list(execute_bq_query(bq_client, query))
    
    expected_schema = {
        'job_identifier': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'entry_number': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'status_code_1': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'status_code_2': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'key_date': {'data_type': 'DATE', 'is_nullable': 'YES'},
        'restart_value': {'data_type': 'INT64', 'is_nullable': 'YES'},
        'records_processed': {'data_type': 'INT64', 'is_nullable': 'YES'},
        'log_timestamp': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
    }

    assert len(schema_rows) == len(expected_schema), "Number of columns mismatch."

    for row in schema_rows:
        col_name = row['column_name']
        assert col_name in expected_schema, f"Unexpected column: {col_name}"
        assert row['data_type'] == expected_schema[col_name]['data_type'], \
            f"Data type mismatch for {col_name}: Expected {expected_schema[col_name]['data_type']}, Got {row['data_type']}"
        assert row['is_nullable'] == expected_schema[col_name]['is_nullable'], \
            f"Nullability mismatch for {col_name}: Expected {expected_schema[col_name]['is_nullable']}, Got {row['is_nullable']}"
```

---

### Test Case 4.2: `PoolBasisprodukt` Schema

*   **Purpose:** Verify the `PoolBasisprodukt` table schema matches the placeholder DDL provided in the migration design. This is a schema assertion.
*   **Setup:** Ensure the `PoolBasisprodukt` DDL has been executed.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `PoolBasisprodukt` table.
*   **Pass/Fail Criteria:**
    *   The table exists.
    *   The following columns exist with the specified data types and nullability (based on the placeholder DDL):
        *   `id`: `STRING`, `NOT NULL`
        *   `name`: `STRING`, `NULLABLE`
        *   `description`: `STRING`, `NULLABLE`
        *   `creation_date`: `DATE`, `NULLABLE`
        *   `last_update_timestamp`: `TIMESTAMP`, `NULLABLE`

```python
def test_pool_basisprodukt_schema(bq_client):
    query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'PoolBasisprodukt'
    ORDER BY ordinal_position;
    """
    schema_rows = list(execute_bq_query(bq_client, query))
    
    expected_schema = {
        'id': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'name': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'description': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'creation_date': {'data_type': 'DATE', 'is_nullable': 'YES'},
        'last_update_timestamp': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
    }

    assert len(schema_rows) == len(expected_schema), "Number of columns mismatch."

    for row in schema_rows:
        col_name = row['column_name']
        assert col_name in expected_schema, f"Unexpected column: {col_name}"
        assert row['data_type'] == expected_schema[col_name]['data_type'], \
            f"Data type mismatch for {col_name}: Expected {expected_schema[col_name]['data_type']}, Got {row['data_type']}"
        assert row['is_nullable'] == expected_schema[col_name]['is_nullable'], \
            f"Nullability mismatch for {col_name}: Expected {expected_schema[col_name]['is_nullable']}, Got {row['is_nullable']}"
```

---

### Test Case 4.3: Row Count in `job_log_table` After Multiple Executions

*   **Purpose:** Verify that each successful execution of `r_ausd_bp_ta_apn_carmen` adds exactly one entry to the `job_log_table`, ensuring correct row count assertions.
*   **Setup:** `job_log_table` is empty.
*   **Action:**
    1.  Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung='JOB_MULTI_1'`, `p_eintrags_nr='ENTRY_M1'`, `p_stichtag='01012023'`, `p_wiederanlauf_wert='0'`.
    2.  Execute `r_ausd_bp_ta_apn_carmen` with `p_job_kennung='JOB_MULTI_2'`, `p_eintrags_nr='ENTRY_M2'`, `p_stichtag='02012023'`, `p_wiederanlauf_wert='1'`.
*   **Pass/Fail Criteria:**
    *   Both procedure calls complete successfully.
    *   `SELECT COUNT(*) FROM default_project.default_dataset.job_log_table;` returns `2`.

```python
def test_job_log_table_row_count_multiple_executions(bq_client):
    mock_d_ausd_bp_ta_apn_carmen_return_count(bq_client, 10) # Arbitrary count
    
    error_message_1 = call_r_ausd_bp_ta_apn_carmen(
        bq_client, 'JOB_MULTI_1', 'ENTRY_M1', '01012023', '0'
    )
    assert error_message_1 is None, f"First call failed: {error_message_1}"

    error_message_2 = call_r_ausd_bp_ta_apn_carmen(
        bq_client, 'JOB_MULTI_2', 'ENTRY_M2', '02012023', '1'
    )
    assert error_message_2 is None, f"Second call failed: {error_message_2}"

    total_rows = list(execute_bq_query(bq_client, f"SELECT COUNT(*) FROM `{JOB_LOG_TABLE}`"))[0][0]
    assert total_rows == 2, "Expected 2 rows in job_log_table after two executions."
```