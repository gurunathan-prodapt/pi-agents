The following migration validation tests are designed to ensure the BigQuery Stored Procedure `r_ausd_bp_ta_rn_da_vda_tk` is behaviourally equivalent to the legacy KornShell script `k_ausd_bp_ta_rn_da_vda_tk.ksh`.

**General Setup for all Tests:**

Before running any tests, ensure the following BigQuery resources are in place:

1.  **BigQuery Project and Dataset:** Replace `project.dataset` with your actual project ID and dataset name.
2.  **Log Tables:** Create the `error_log`, `process_log`, and `job_table` as defined in the DDLs provided in the migration design.
3.  **Stored Procedure:** Deploy the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure to your BigQuery dataset.
4.  **Test Harness:** A Python environment with `google-cloud-bigquery` installed is assumed for running the test code examples.

```python
# Python setup for BigQuery client
from google.cloud import bigquery
import datetime
import time

client = bigquery.Client()
PROJECT_ID = "your-gcp-project-id" # Replace with your project ID
DATASET_ID = "your_dataset_id"     # Replace with your dataset ID
SP_NAME = "r_ausd_bp_ta_rn_da_vda_tk"

def clear_log_tables():
    """Clears all log tables before a test run."""
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.process_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
    print("Log tables cleared.")

def execute_sp(job_kennung, entry_nr, stichtag, wiederanlauf_wert):
    """Executes the BigQuery stored procedure."""
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`(
        p_JobKennung => '{job_kennung}',
        p_EintragsNr => '{entry_nr}',
        p_Stichtag => '{stichtag}',
        p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    print(f"Executing SP with: JobKennung={job_kennung}, EintragsNr={entry_nr}, Stichtag={stichtag}, WiederanlaufWert={wiederanlauf_wert}")
    try:
        job = client.query(query)
        job.result() # Waits for the job to complete
        print("Stored procedure executed successfully.")
        return True, None
    except Exception as e:
        print(f"Stored procedure execution failed: {e}")
        return False, str(e)

def fetch_log_entries(table_name):
    """Fetches all entries from a specified log table."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` ORDER BY log_timestamp ASC"
    rows = client.query(query).result()
    return [dict(row) for row in rows]

# Example usage:
# clear_log_tables()
# success, error_msg = execute_sp("JOB123", "ENTRY001", "01012023", 0)
# process_logs = fetch_log_entries("process_log")
# error_logs = fetch_log_entries("error_log")
# job_entries = fetch_log_entries("job_table")
```

---

### Test Case 1: Successful Execution with Valid Parameters

**Purpose:** Verify that the stored procedure executes successfully with valid input parameters, logs progress correctly, and records a job entry, demonstrating output parity and basic transformation correctness.

**Setup:**
*   Ensure all log tables (`error_log`, `process_log`, `job_table`) are empty.
*   The stored procedure `r_ausd_bp_ta_rn_da_vda_tk` is deployed.

**Action:**
Execute the stored procedure with a set of valid parameters.

```python
clear_log_tables()
today_str = datetime.date.today().strftime("%d%m%Y")
success, error_msg = execute_sp("TEST_JOB", "ENTRY_001", today_str, 1)
```

**Pass/Fail Criterion:**
*   The stored procedure execution completes without raising an error.
*   `process_log` contains exactly two entries: one for "Pruefe Datum OK" and one for "---------- ENDE Datenverarbeitung ----------".
*   `job_table` contains exactly one entry, with `records_processed` equal to `12345` (the placeholder value from the SP).
*   `error_log` contains zero entries.
*   The `table_name` field in log entries is `PoolBasisprodukt`.
*   The `business_date_param` in log entries matches the input `p_Stichtag`.

**Test Code (Assertions):**

```python
assert success is True, f"SP execution failed unexpectedly: {error_msg}"

process_logs = fetch_log_entries("process_log")
error_logs = fetch_log_entries("error_log")
job_entries = fetch_log_entries("job_table")

assert len(process_logs) == 2, f"Expected 2 process log entries, got {len(process_logs)}"
assert process_logs[0]['message'] == 'Pruefe Datum OK'
assert process_logs[1]['message'] == '---------- ENDE Datenverarbeitung ----------'
assert process_logs[0]['table_name'] == 'PoolBasisprodukt'
assert process_logs[0]['job_kennung'] == 'TEST_JOB'
assert process_logs[0]['entry_number'] == 'ENTRY_001'
assert process_logs[0]['business_date_param'] == today_str

assert len(job_entries) == 1, f"Expected 1 job table entry, got {len(job_entries)}"
assert job_entries[0]['records_processed'] == 12345 # Placeholder value
assert job_entries[0]['table_name'] == 'PoolBasisprodukt'
assert job_entries[0]['job_status_code_1'] == 'A'
assert job_entries[0]['job_status_code_2'] == 'I'
assert job_entries[0]['process_flag_1'] == 'J'
assert job_entries[0]['process_flag_2'] == 'N'
assert job_entries[0]['business_date_start'] == datetime.datetime.strptime(today_str, "%d%m%Y").date()
assert job_entries[0]['business_date_end'] == datetime.datetime.strptime(today_str, "%d%m%Y").date()
assert job_entries[0]['description'] == 'Initialbefuellung'

assert len(error_logs) == 0, f"Expected 0 error log entries, got {len(error_logs)}"
```

---

### Test Case 2: Missing `p_JobKennung` Parameter

**Purpose:** Verify that the stored procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and terminating execution, mirroring the legacy script's `pruefeParameterGesetzt` behavior.

**Setup:**
*   Ensure all log tables are empty.

**Action:**
Execute the stored procedure with `p_JobKennung` as an empty string or `NULL`.

```python
clear_log_tables()
today_str = datetime.date.today().strftime("%d%m%Y")
success, error_msg = execute_sp("", "ENTRY_002", today_str, 0) # Empty string for p_JobKennung
```

**Pass/Fail Criterion:**
*   The stored procedure execution fails and raises an error.
*   The error message contains "FEHLER: 1 Jobkennung fehlt".
*   `error_log` contains exactly one entry with `error_code = 1` and `error_message = 'Jobkennung fehlt'`.
*   `process_log` contains zero entries.
*   `job_table` contains zero entries.

**Test Code (Assertions):**

```python
assert success is False, "SP execution should have failed due to missing JobKennung"
assert "FEHLER: 1 Jobkennung fehlt" in error_msg, f"Expected specific error message, got: {error_msg}"

error_logs = fetch_log_entries("error_log")
process_logs = fetch_log_entries("process_log")
job_entries = fetch_log_entries("job_table")

assert len(error_logs) == 1, f"Expected 1 error log entry, got {len(error_logs)}"
assert error_logs[0]['error_code'] == 1
assert error_logs[0]['error_message'] == 'Jobkennung fehlt'
assert error_logs[0]['table_name'] == 'PoolBasisprodukt'
assert error_logs[0]['job_kennung'] == '' # The value passed in
assert error_logs[0]['entry_number'] == 'ENTRY_002'
assert error_logs[0]['business_date_param'] == today_str

assert len(process_logs) == 0, f"Expected 0 process log entries, got {len(process_logs)}"
assert len(job_entries) == 0, f"Expected 0 job table entries, got {len(job_entries)}"
```

---

### Test Case 3: Invalid `p_Stichtag` Date Format

**Purpose:** Verify that the stored procedure correctly handles an invalid date format for `p_Stichtag`, logging an error and terminating execution, mirroring the legacy script's `DWDate_Datum_Check` behavior.

**Setup:**
*   Ensure all log tables are empty.

**Action:**
Execute the stored procedure with `p_Stichtag` in an incorrect format (e.g., `YYYY-MM-DD`).

```python
clear_log_tables()
success, error_msg = execute_sp("TEST_JOB_DATE", "ENTRY_003", "2023-01-01", 0)
```

**Pass/Fail Criterion:**
*   The stored procedure execution fails and raises an error.
*   The error message contains "FEHLER: Ungueltiges Datumsformat fuer Stichtag".
*   `error_log` contains exactly one entry with `error_code = 194` and `error_message = 'Ungueltiges Datumsformat fuer Stichtag'`.
*   `process_log` contains zero entries (as date check happens before first process log).
*   `job_table` contains zero entries.

**Test Code (Assertions):**

```python
assert success is False, "SP execution should have failed due to invalid Stichtag format"
assert "FEHLER: Ungueltiges Datumsformat fuer Stichtag" in error_msg, f"Expected specific error message, got: {error_msg}"

error_logs = fetch_log_entries("error_log")
process_logs = fetch_log_entries("process_log")
job_entries = fetch_log_entries("job_table")

assert len(error_logs) == 1, f"Expected 1 error log entry, got {len(error_logs)}"
assert error_logs[0]['error_code'] == 194
assert error_logs[0]['error_message'] == 'Ungueltiges Datumsformat fuer Stichtag'
assert error_logs[0]['table_name'] == 'PoolBasisprodukt'
assert error_logs[0]['job_kennung'] == 'TEST_JOB_DATE'
assert error_logs[0]['entry_number'] == 'ENTRY_003'
assert error_logs[0]['business_date_param'] == '2023-01-01'

assert len(process_logs) == 0, f"Expected 0 process log entries, got {len(process_logs)}"
assert len(job_entries) == 0, f"Expected 0 job table entries, got {len(job_entries)}"
```

---

### Test Case 4: `p_wiederanlaufWert` Defaulting

**Purpose:** Verify that `p_wiederanlaufWert` is correctly initialized to `0` if `NULL` is passed, demonstrating transformation correctness.

**Setup:**
*   Ensure all log tables are empty.

**Action:**
Execute the stored procedure with `p_wiederanlaufWert` explicitly set to `NULL`.

```python
clear_log_tables()
today_str = datetime.date.today().strftime("%d%m%Y")
# In BigQuery SQL, passing NULL for an INT64 parameter is direct.
# For Python client, we can pass None, which translates to NULL.
success, error_msg = execute_sp("TEST_JOB_RESTART", "ENTRY_004", today_str, None)
```

**Pass/Fail Criterion:**
*   The stored procedure executes successfully without raising an error.
*   `process_log` and `job_table` entries are created as in a successful run.
*   `error_log` contains zero entries.
*   (Implicit) The internal `p_wiederanlaufWert` variable within the SP should be `0` when the core logic is executed. This is hard to assert directly from logs, but the successful execution implies the `IF p_wiederanlaufWert IS NULL THEN SET p_wiederanlaufWert = 0; END IF;` block was correctly handled.

**Test Code (Assertions):**

```python
assert success is True, f"SP execution failed unexpectedly: {error_msg}"

process_logs = fetch_log_entries("process_log")
job_entries = fetch_log_entries("job_table")
error_logs = fetch_log_entries("error_log")

assert len(process_logs) == 2, "Expected 2 process log entries for successful run"
assert len(job_entries) == 1, "Expected 1 job table entry for successful run"
assert len(error_logs) == 0, "Expected 0 error log entries for successful run"

# Further assertions can be made on the content of process_logs and job_entries
# to ensure they are consistent with a successful run, as in Test Case 1.
# The key here is that the SP did NOT fail due to a NULL p_wiederanlaufWert.
```

---

### Test Case 5: External System Replacements - Date Derivations

**Purpose:** Verify that BigQuery's native date functions (`CURRENT_DATE()`, `DATE_SUB()`) correctly replace the `gestern.ksh` script for deriving "today" and "yesterday" dates. While these aren't explicitly logged in the current SP, their correct internal calculation is crucial for the core SQL logic.

**Setup:**
*   This test primarily verifies the internal logic of the SP. If the core SQL (`d_ausd_bp_ta_rn_da_vda_tk.sql`) were fully integrated and used `v_datum_heute` and `v_datum_gestern`, we would assert against the output of that core logic.
*   For this test, we'll assume the `job_table`'s `business_date_start` and `business_date_end` are derived from `p_Stichtag`, and `v_datum_heute`/`v_datum_gestern` are used elsewhere. We can add a temporary log entry to the SP to verify `v_datum_heute` and `v_datum_gestern` if needed.

**Action:**
Execute the stored procedure with a valid `p_Stichtag`.

```python
clear_log_tables()
test_date_str = "15032023" # A specific date for predictable 'yesterday'
success, error_msg = execute_sp("TEST_DATE_DERIV", "ENTRY_005", test_date_str, 0)
```

**Pass/Fail Criterion:**
*   The stored procedure executes successfully.
*   (Implicit) The internal `v_datum_heute` variable should be `CURRENT_DATE()` at the time of execution.
*   (Implicit) The internal `v_datum_gestern` variable should be `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` at the time of execution.
*   The `job_table` entry's `business_date_start` and `business_date_end` correctly reflect the parsed `p_Stichtag`.

**Test Code (Assertions):**

```python
assert success is True, f"SP execution failed unexpectedly: {error_msg}"

job_entries = fetch_log_entries("job_table")
assert len(job_entries) == 1, "Expected 1 job table entry"

# Verify that the business_date_start/end are correctly parsed from p_Stichtag
expected_stichtag_date = datetime.datetime.strptime(test_date_str, "%d%m%Y").date()
assert job_entries[0]['business_date_start'] == expected_stichtag_date
assert job_entries[0]['business_date_end'] == expected_stichtag_date

# To explicitly test v_datum_heute and v_datum_gestern, you would need to temporarily
# modify the stored procedure to log these values, e.g.:
# INSERT INTO `project.dataset.process_log` VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, CONCAT('Today: ', CAST(v_datum_heute AS STRING), ', Yesterday: ', CAST(v_datum_gestern AS STRING)));
# Then, fetch process_logs and assert against the expected dates.
# For example:
# today_actual = datetime.date.today()
# yesterday_actual = today_actual - datetime.timedelta(days=1)
# assert f"Today: {today_actual.isoformat()}" in process_logs[1]['message'] # Assuming it's the second log entry
# assert f"Yesterday: {yesterday_actual.isoformat()}" in process_logs[1]['message']
```

---

### Test Case 6: Unexpected Error During Core Logic Execution

**Purpose:** Verify that the `EXCEPTION WHEN ERROR` block at the end of the stored procedure correctly catches unhandled errors that occur during the (simulated) core business logic, logging them to `error_log` and raising a generic error message. This covers the general error handling and robustness.

**Setup:**
*   Ensure all log tables are empty.
*   **Modify the Stored Procedure temporarily:** Introduce a deliberate error within the "PLACEHOLDER FOR CORE BUSINESS LOGIC" section. For example, try to divide by zero or reference a non-existent table.

```sql
-- TEMPORARY MODIFICATION FOR THIS TEST ONLY
-- Inside `project.dataset.r_ausd_bp_ta_rn_da_vda_tk` procedure:
-- Replace the placeholder comment with:
-- SELECT 1 / 0; -- Deliberate error for testing
-- SET v_records = 12345; -- Placeholder
```

**Action:**
Execute the stored procedure with valid parameters.

```python
clear_log_tables()
today_str = datetime.date.today().strftime("%d%m%Y")
success, error_msg = execute_sp("TEST_UNEXPECTED_ERROR", "ENTRY_006", today_str, 0)
```

**Pass/Fail Criterion:**
*   The stored procedure execution fails and raises an error.
*   The error message contains "UNEXPECTED ERROR:" and details of the BigQuery error (e.g., "Division by zero").
*   `error_log` contains exactly one entry with `error_code` and `error_message` reflecting the BigQuery runtime error.
*   `process_log` contains at least one entry ("Pruefe Datum OK") before the error occurred.
*   `job_table` contains zero entries (as the error occurred before the job entry was logged).

**Test Code (Assertions):**

```python
assert success is False, "SP execution should have failed due to unexpected error"
assert "UNEXPECTED ERROR:" in error_msg, f"Expected generic unexpected error message, got: {error_msg}"
assert "Division by zero" in error_msg or "invalid table" in error_msg, "Expected specific BigQuery error detail"

error_logs = fetch_log_entries("error_log")
process_logs = fetch_log_entries("process_log")
job_entries = fetch_log_entries("job_table")

assert len(error_logs) == 1, f"Expected 1 error log entry, got {len(error_logs)}"
assert error_logs[0]['error_message'] is not None and error_logs[0]['error_message'] != ''
assert error_logs[0]['table_name'] == 'PoolBasisprodukt'
assert error_logs[0]['job_kennung'] == 'TEST_UNEXPECTED_ERROR'
assert error_logs[0]['entry_number'] == 'ENTRY_006'
assert error_logs[0]['business_date_param'] == today_str

assert len(process_logs) >= 1, f"Expected at least 1 process log entry, got {len(process_logs)}"
assert process_logs[0]['message'] == 'Pruefe Datum OK' # Should be logged before the error
assert len(job_entries) == 0, f"Expected 0 job table entries, got {len(job_entries)}"

# IMPORTANT: Revert the temporary SP modification after this test.
```

---

### Test Case 7: Data Quality - Log Table Schema and Data Types

**Purpose:** Verify that the schema of the log tables (`error_log`, `process_log`, `job_table`) matches the design and that data types are correctly handled when inserting data. This ensures data quality and consistency for downstream reporting/monitoring.

**Setup:**
*   Ensure the log tables are created as per the DDLs.
*   Execute a successful run of the stored procedure (as in Test Case 1) to populate the logs.

**Action:**
Query the information schema for the log tables and inspect the data types of the columns.

```python
clear_log_tables()
today_str = datetime.date.today().strftime("%d%m%Y")
execute_sp("SCHEMA_TEST", "ENTRY_007", today_str, 0)

# Fetch schema information
def get_table_schema(table_name):
    query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    rows = client.query(query).result()
    return {row['column_name']: row['data_type'] for row in rows}

error_schema = get_table_schema("error_log")
process_schema = get_table_schema("process_log")
job_schema = get_table_schema("job_table")
```

**Pass/Fail Criterion:**
*   The retrieved schemas for each log table match the expected column names and data types from the DDLs.
*   For `error_log`: `log_timestamp` (TIMESTAMP), `table_name` (STRING), `job_kennung` (STRING), `entry_number` (STRING), `business_date_param` (STRING), `error_code` (INT64), `error_message` (STRING).
*   For `process_log`: `log_timestamp` (TIMESTAMP), `table_name` (STRING), `job_kennung` (STRING), `entry_number` (STRING), `business_date_param` (STRING), `message` (STRING).
*   For `job_table`: `log_timestamp` (TIMESTAMP), `table_name` (STRING), `job_status_code_1` (STRING), `job_status_code_2` (STRING), `business_date_start` (DATE), `business_date_end` (DATE), `process_flag_1` (STRING), `process_flag_2` (STRING), `records_processed` (INT64), `description` (STRING).

**Test Code (Assertions):**

```python
expected_error_schema = {
    'log_timestamp': 'TIMESTAMP',
    'table_name': 'STRING',
    'job_kennung': 'STRING',
    'entry_number': 'STRING',
    'business_date_param': 'STRING',
    'error_code': 'INT64',
    'error_message': 'STRING'
}
assert error_schema == expected_error_schema, f"Error log schema mismatch: {error_schema}"

expected_process_schema = {
    'log_timestamp': 'TIMESTAMP',
    'table_name': 'STRING',
    'job_kennung': 'STRING',
    'entry_number': 'STRING',
    'business_date_param': 'STRING',
    'message': 'STRING'
}
assert process_schema == expected_process_schema, f"Process log schema mismatch: {process_schema}"

expected_job_schema = {
    'log_timestamp': 'TIMESTAMP',
    'table_name': 'STRING',
    'job_status_code_1': 'STRING',
    'job_status_code_2': 'STRING',
    'business_date_start': 'DATE',
    'business_date_end': 'DATE',
    'process_flag_1': 'STRING',
    'process_flag_2': 'STRING',
    'records_processed': 'INT64',
    'description': 'STRING'
}
assert job_schema == expected_job_schema, f"Job table schema mismatch: {job_schema}"
```