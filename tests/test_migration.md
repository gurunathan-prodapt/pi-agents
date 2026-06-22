The migration of `k_ausd_bp_ta_tarifoption.ksh` to a BigQuery stored procedure `r_ausd_bp_ta_tarifoption` involves significant changes in execution environment, parameter handling, error logging, and data processing. The following test cases are designed to ensure behavioral equivalence and correctness across these aspects.

**Assumptions for Testing:**
*   The `project.dataset.job_log` and `project.dataset.PoolBasisprodukt` tables exist with the schemas defined in the migration design.
*   The BigQuery stored procedure `project.dataset.r_ausd_bp_ta_tarifoption` is deployed and accessible.
*   The `d_ausd_bp_ta_tarifoption.sql` core logic, as represented in the BigQuery stored procedure, performs a `SELECT * FROM PoolBasisprodukt WHERE business_date = v_stichtag_date;`.
*   The BigQuery stored procedure implements parameter validation for `p_JobKennung`, `p_EintragsNr`, and `p_Stichtag` similar to the `pruefeParameterGesetzt` function in the ksh script, using internal error codes:
    *   `v_err_nr = 1`: `Jobkennung fehlt`
    *   `v_err_nr = 3`: `Stichtag fehlt`
    *   `v_err_nr = 4`: `EintragsNr fehlt`
    *   `v_err_nr = 2`: `Datum hat ungueltiges Format` (as already in the provided code)

---

### Test Setup (Pytest Fixtures)

The following `pytest` fixtures provide a BigQuery client and ensure a clean state for the `job_log` and `PoolBasisprodukt` tables before and after each test.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date, timedelta

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP project ID
DATASET_ID = "your_bigquery_dataset"  # Replace with your BigQuery dataset ID
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
POOLBASISPRODUKT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt"
STORED_PROCEDURE_ID = f"{DATASET_ID}.r_ausd_bp_ta_tarifoption"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for tests."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def setup_and_teardown_tables(bq_client):
    """Ensures tables exist and are clean before each test."""
    # Ensure job_log table exists
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{JOB_LOG_TABLE}` (
          job_name STRING,
          tab_name STRING,
          error_nr INT64,
          error_msg STRING,
          record_count INT64,
          status_msg STRING,
          created_at TIMESTAMP
        );
    """).result()

    # Ensure PoolBasisprodukt table exists
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{POOLBASISPRODUKT_TABLE}` (
          column1 STRING,
          column2 INT64,
          business_date DATE
        );
    """).result()

    # Clean tables before each test
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{POOLBASISPRODUKT_TABLE}`").result()

    yield # Run the test

    # Clean tables after each test (optional, but good for isolation)
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{POOLBASISPRODUKT_TABLE}`").result()

def call_stored_procedure(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert):
    """Helper function to call the BigQuery stored procedure."""
    query = f"""
        CALL `{STORED_PROCEDURE_ID}`(
            p_JobKennung => @job_kennung,
            p_EintragsNr => @eintrags_nr,
            p_Stichtag => @stichtag,
            p_wiederanlaufWert => @wiederanlauf_wert
        );
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr),
            bigquery.ScalarQueryParameter("stichtag", "STRING", stichtag),
            bigquery.ScalarQueryParameter("wiederanlauf_wert", "STRING", wiederanlauf_wert),
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        # Stored procedures might raise errors that are caught by BigQuery,
        # but the SP itself might log and LEAVE. We're interested in the log.
        print(f"Stored procedure call resulted in an error: {e}")
        pass # Allow the test to check the job_log for specific errors

def get_job_log_entries(bq_client):
    """Helper function to retrieve all entries from job_log, ordered by creation time."""
    query = f"SELECT * FROM `{JOB_LOG_TABLE}` ORDER BY created_at ASC"
    rows = bq_client.query(query).result()
    return [dict(row) for row in rows]

def insert_poolbasisprodukt_data(bq_client, data):
    """Helper function to insert data into PoolBasisprodukt."""
    rows_to_insert = []
    for col1, col2, biz_date_str in data:
        rows_to_insert.append(
            bigquery.Row((col1, col2, date.fromisoformat(biz_date_str)),
                          ("column1", "column2", "business_date"))
        )
    errors = bq_client.insert_rows(POOLBASISPRODUKT_TABLE, rows_to_insert)
    assert not errors, f"Errors inserting data: {errors}"

```

---

### Test Case 1: Parameter Validation - Missing `p_JobKennung`

*   **Purpose**: Verify that the BigQuery stored procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and exiting early, mirroring the legacy script's `pruefeParameterGesetzt Jobkennung p_JobKennung` behavior.
*   **Setup**:
    *   The `project.dataset.job_log` table is empty.
*   **Action**:
    *   Call `project.dataset.r_ausd_bp_ta_tarifoption` with `p_JobKennung = NULL` (or an empty string), and valid values for other parameters (e.g., `p_EintragsNr = 'ENTRY_123'`, `p_Stichtag = '01012023'`, `p_wiederanlaufWert = '0'`).
*   **Pass/Fail Criterion**:
    *   The `job_log` table contains exactly one entry.
    *   This entry has:
        *   `job_name = 'r_ausd_bp_ta_tarifoption'`
        *   `tab_name = 'PoolBasisprodukt'`
        *   `error_nr = 1`
        *   `error_msg = 'Jobkennung fehlt'`
    *   No other log entries (e.g., "Pruefe Datum", "Datum OK") are present, indicating an early exit.

```python
def test_missing_jobkennung(bq_client):
    call_stored_procedure(bq_client, None, 'ENTRY_123', '01012023', '0')
    log_entries = get_job_log_entries(bq_client)

    assert len(log_entries) == 1
    assert log_entries[0]['job_name'] == 'r_ausd_bp_ta_tarifoption'
    assert log_entries[0]['tab_name'] == 'PoolBasisprodukt'
    assert log_entries[0]['error_nr'] == 1
    assert log_entries[0]['error_msg'] == 'Jobkennung fehlt'
    assert log_entries[0]['status_msg'] is None
    assert log_entries[0]['record_count'] is None
```

---

### Test Case 2: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose**: Verify that the BigQuery stored procedure correctly identifies and handles a missing `p_EintragsNr` parameter, logging an error and exiting early, mirroring the legacy script's `pruefeParameterGesetzt EintragsNr p_EintragsNr` behavior.
*   **Setup**:
    *   The `project.dataset.job_log` table is empty.
*   **Action**:
    *   Call `project.dataset.r_ausd_bp_ta_tarifoption` with a valid `p_JobKennung = 'JOB_ABC'`, `p_EintragsNr = NULL` (or an empty string), and valid `p_Stichtag = '01012023'`, `p_wiederanlaufWert = '0'`.
*   **Pass/Fail Criterion**:
    *   The `job_log` table contains exactly one entry.
    *   This entry has:
        *   `job_name = 'r_ausd_bp_ta_tarifoption'`
        *   `tab_name = 'PoolBasisprodukt'`
        *   `error_nr = 4` (assuming this is the assigned error code for missing EintragsNr)
        *   `error_msg = 'EintragsNr fehlt'`
    *   No other log entries are present.

```python
def test_missing_eintragsnr(bq_client):
    call_stored_procedure(bq_client, 'JOB_ABC', None, '01012023', '0')
    log_entries = get_job_log_entries(bq_client)

    assert len(log_entries) == 1
    assert log_entries[0]['job_name'] == 'r_ausd_bp_ta_tarifoption'
    assert log_entries[0]['tab_name'] == 'PoolBasisprodukt'
    assert log_entries[0]['error_nr'] == 4
    assert log_entries[0]['error_msg'] == 'EintragsNr fehlt'
    assert log_entries[0]['status_msg'] is None
    assert log_entries[0]['record_count'] is None
```

---

### Test Case 3: Parameter Validation - Missing `p_Stichtag`

*   **Purpose**: Verify that the BigQuery stored procedure correctly identifies and handles a missing `p_Stichtag` parameter, logging an error and exiting early, mirroring the legacy script's `pruefeParameterGesetzt Stichtag p_Stichtag` behavior.
*   **Setup**:
    *   The `project.dataset.job_log` table is empty.
*   **Action**:
    *   Call `project.dataset.r_ausd_bp_ta_tarifoption` with valid `p_JobKennung = 'JOB_ABC'`, `p_EintragsNr = 'ENTRY_123'`, `p_Stichtag = NULL` (or an empty string), and valid `p_wiederanlaufWert = '0'`.
*   **Pass/Fail Criterion**:
    *   The `job_log` table contains exactly one entry.
    *   This entry has:
        *   `job_name = 'r_ausd_bp_ta_tarifoption'`
        *   `tab_name = 'PoolBasisprodukt'`
        *   `error_nr = 3` (assuming this is the assigned error code for missing Stichtag)
        *   `error_msg = 'Stichtag fehlt'`
    *   No other log entries are present.

```python
def test_missing_stichtag(bq_client):
    call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', None, '0')
    log_entries = get_job_log_entries(bq_client)

    assert len(log_entries) == 1
    assert log_entries[0]['job_name'] == 'r_ausd_bp_ta_tarifoption'
    assert log_entries[0]['tab_name'] == 'PoolBasisprodukt'
    assert log_entries[0]['error_nr'] == 3
    assert log_entries[0]['error_msg'] == 'Stichtag fehlt'
    assert log_entries[0]['status_msg'] is None
    assert log_entries[0]['record_count'] is None
```

---

### Test Case 4: Date Validation - Invalid `p_Stichtag` Format

*   **Purpose**: Verify that the BigQuery stored procedure correctly identifies and handles an invalid `p_Stichtag` format, logging an error and exiting, mirroring the legacy script's `DWDate_Datum_Check` behavior.
*   **Setup**:
    *   The `project.dataset.job_log` table is empty.
*   **Action**:
    *   Call `project.dataset.r_ausd_bp_ta_tarifoption` with valid `p_JobKennung = 'JOB_ABC'`, `p_EintragsNr = 'ENTRY_123'`, `p_Stichtag = '2023-01-01'` (an invalid format for DDMMYYYY), and valid `p_wiederanlaufWert = '0'`.
*   **Pass/Fail Criterion**:
    *   The `job_log` table contains exactly one entry.
    *   This entry has:
        *   `job_name = 'r_ausd_bp_ta_tarifoption'`
        *   `tab_name = 'PoolBasisprodukt'`
        *   `error_nr = 2`
        *   `error_msg = 'Datum hat ungueltiges Format'`
    *   No "Datum OK" or transformation-related logs are present.

```python
def test_invalid_stichtag_format(bq_client):
    call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', '2023-01-01', '0')
    log_entries = get_job_log_entries(bq_client)

    assert len(log_entries) == 1
    assert log_entries[0]['job_name'] == 'r_ausd_bp_ta_tarifoption'
    assert log_entries[0]['tab_name'] == 'PoolBasisprodukt'
    assert log_entries[0]['error_nr'] == 2
    assert log_entries[0]['error_msg'] == 'Datum hat ungueltiges Format'
    assert log_entries[0]['status_msg'] is None
    assert log_entries[0]['record_count'] is None
```

---

### Test Case 5: `p_wiederanlaufWert` NULL/Empty Handling

*   **Purpose**: Verify that `p_wiederanlaufWert` is correctly initialized to '0' if `NULL` or empty, as specified in the migration design and mirroring the ksh script's `if [[ -z "$p_wiederanlaufWert" ]]` logic.
*   **Setup**:
    *   The `project.dataset.job_log` table is empty.
    *   Populate `project.dataset.PoolBasisprodukt` with some test data, including a `business_date` matching the `p_Stichtag` to ensure successful execution.
        *   `('A', 100, '2023-01-01')`
*   **Action**:
    *   Call `project.dataset.r_ausd_bp_ta_tarifoption` with valid `p_JobKennung = 'JOB_ABC'`, `p_EintragsNr = 'ENTRY_123'`, `p_Stichtag = '01012023'`, and `p_wiederanlaufWert = NULL` (or an empty string).
*   **Pass/Fail Criterion**:
    *   The procedure executes successfully (no error logs).
    *   The `job_log` table contains the expected sequence of status messages ("Pruefe Datum", "Datum OK") and a final record count entry.
    *   The `record_count` in the final log entry is 1, indicating the core logic executed correctly, implying `p_wiederanlaufWert` was handled as '0' (if it were used in the core logic).

```python
def test_wiederanlaufwert_null_empty_handling(bq_client):
    insert_poolbasisprodukt_data(bq_client, [('A', 100, '2023-01-01')])
    call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', '01012023', None) # Test with NULL
    # Or call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', '01012023', '') # Test with empty string

    log_entries = get_job_log_entries(bq_client)

    assert len(log_entries) == 3 # Pruefe Datum, Datum OK, Initialbefuellung
    assert log_entries[0]['status_msg'] == 'Pruefe Datum'
    assert log_entries[1]['status_msg'] == 'Datum OK'
    assert log_entries[2]['status_msg'] == 'Initialbefuellung'
    assert log_entries[2]['record_count'] == 1
    assert all(entry['error_nr'] is None for entry in log_entries)
    assert all(entry['error_msg'] is None for entry in log_entries)
```

---

### Test Case 6: Successful Execution - Output Parity & Transformation Correctness

*   **Purpose**: Verify that with valid inputs, the BigQuery stored procedure executes successfully, logs correctly, and the core transformation (as defined by the placeholder) produces the expected record count, demonstrating output parity with the legacy script's successful run.
*   **Setup**:
    *   The `project.dataset.job_log` table is empty.
    *   Populate `project.dataset.PoolBasisprodukt` with known test data:
        *   `('A', 100, '2023-01-01')`
        *   `('B', 200, '2023-01-01')`
        *   `('C', 300, '2023-01-02')`
    *   The core SQL logic is assumed to be `SELECT * FROM PoolBasisprodukt WHERE business_date = v_stichtag_date;`.
*   **Action**:
    *   Call `project.dataset.r_ausd_bp_ta_tarifoption` with `p_JobKennung = 'JOB_ABC'`, `p_EintragsNr = 'ENTRY_123'`, `p_Stichtag = '01012023'`, `p_wiederanlaufWert = '0'`.
*   **Pass/Fail Criterion**:
    *   The `job_log` table contains exactly three entries in the correct order:
        1.  `job_name='r_ausd_bp_ta_tarifoption'`, `tab_name='PoolBasisprodukt'`, `status_msg='Pruefe Datum'`.
        2.  `job_name='r_ausd_bp_ta_tarifoption'`, `tab_name='PoolBasisprodukt'`, `status_msg='Datum OK'`.
        3.  `job_name='r_ausd_bp_ta_tarifoption'`, `tab_name='PoolBasisprodukt'`, `record_count=2`, `status_msg='Initialbefuellung'`.
    *   No error entries in `job_log`.

```python
def test_successful_execution_and_record_count(bq_client):
    insert_poolbasisprodukt_data(bq_client, [
        ('A', 100, '2023-01-01'),
        ('B', 200, '2023-01-01'),
        ('C', 300, '2023-01-02')
    ])
    call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', '01012023', '0')
    log_entries = get_job_log_entries(bq_client)

    assert len(log_entries) == 3
    assert log_entries[0]['status_msg'] == 'Pruefe Datum'
    assert log_entries[1]['status_msg'] == 'Datum OK'
    assert log_entries[2]['status_msg'] == 'Initialbefuellung'
    assert log_entries[2]['record_count'] == 2
    assert all(entry['error_nr'] is None for entry in log_entries)
```

---

### Test Case 7: Transformation Correctness - No Matching Data

*   **Purpose**: Verify correct behavior when the core transformation finds no matching data for the given `p_Stichtag`, resulting in a zero record count.
*   **Setup**:
    *   The `project.dataset.job_log` table is empty.
    *   Populate `project.dataset.PoolBasisprodukt` with test data, but *no* data for the target `p_Stichtag`:
        *   `('A', 100, '2023-01-02')`
        *   `('B', 200, '2023-01-03')`
*   **Action**:
    *   Call `project.dataset.r_ausd_bp_ta_tarifoption` with `p_JobKennung = 'JOB_ABC'`, `p_EintragsNr = 'ENTRY_123'`, `p_Stichtag = '01012023'`, `p_wiederanlaufWert = '0'`.
*   **Pass/Fail Criterion**:
    *   The `job_log` table contains exactly three entries in the correct order.
    *   The final entry has `record_count=0`.
    *   No error entries in `job_log`.

```python
def test_no_matching_data(bq_client):
    insert_poolbasisprodukt_data(bq_client, [
        ('A', 100, '2023-01-02'),
        ('B', 200, '2023-01-03')
    ])
    call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', '01012023', '0')
    log_entries = get_job_log_entries(bq_client)

    assert len(log_entries) == 3
    assert log_entries[0]['status_msg'] == 'Pruefe Datum'
    assert log_entries[1]['status_msg'] == 'Datum OK'
    assert log_entries[2]['status_msg'] == 'Initialbefuellung'
    assert log_entries[2]['record_count'] == 0
    assert all(entry['error_nr'] is None for entry in log_entries)
```

---

### Test Case 8: Transformation Correctness - Empty Source Table

*   **Purpose**: Verify correct behavior when the `PoolBasisprodukt` source table is entirely empty.
*   **Setup**:
    *   The `project.dataset.job_log` table is empty.
    *   The `project.dataset.PoolBasisprodukt` table is empty.
*   **Action**:
    *   Call `project.dataset.r_ausd_bp_ta_tarifoption` with `p_JobKennung = 'JOB_ABC'`, `p_EintragsNr = 'ENTRY_123'`, `p_Stichtag = '01012023'`, `p_wiederanlaufWert = '0'`.
*   **Pass/Fail Criterion**:
    *   The `job_log` table contains exactly three entries in the correct order.
    *   The final entry has `record_count=0`.
    *   No error entries in `job_log`.

```python
def test_empty_source_table(bq_client):
    # PoolBasisprodukt is empty due to fixture setup
    call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', '01012023', '0')
    log_entries = get_job_log_entries(bq_client)

    assert len(log_entries) == 3
    assert log_entries[0]['status_msg'] == 'Pruefe Datum'
    assert log_entries[1]['status_msg'] == 'Datum OK'
    assert log_entries[2]['status_msg'] == 'Initialbefuellung'
    assert log_entries[2]['record_count'] == 0
    assert all(entry['error_nr'] is None for entry in log_entries)
```

---

### Test Case 9: External System Replacement - Date Derivation

*   **Purpose**: Verify that BigQuery's native date functions (`CURRENT_DATE()`, `DATE_SUB`) correctly replace the `gestern.ksh` script for deriving `v_datum_heute` and `v_datum_gestern`. While these variables are not used in the provided BigQuery SP's core logic placeholder, their correct assignment is a direct replacement of a legacy external dependency.
*   **Setup**:
    *   This test primarily verifies the internal logic of the stored procedure. Direct external assertion of these internal variables is not possible without modifying the SP.
*   **Action**:
    *   Execute the stored procedure with valid parameters.
*   **Pass/Fail Criterion**:
    *   The procedure completes successfully (as verified by Test Case 6).
    *   **Implicit Check**: The BigQuery stored procedure's code explicitly uses `DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();` and `DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`. This directly translates the functionality of `gestern.ksh`. If the actual `d_ausd_bp_ta_tarifoption.sql` (migrated) were to use these variables, their correctness would be reflected in the final data output. For this test, we assert the *presence* and *correctness of the BigQuery functions* used for their assignment.

```python
# This test is more about code review and ensuring the BigQuery functions
# are used as intended, as direct external assertion of internal DECLAREd variables
# is not straightforward without modifying the SP.
# However, if the core logic (from d_ausd_bp_ta_tarifoption.sql) were to use
# v_datum_heute or v_datum_gestern, then a test case would involve:
# 1. Setting up PoolBasisprodukt data that depends on these dates.
# 2. Calling the SP.
# 3. Asserting the record_count or target table output based on the expected date calculations.

# Example (conceptual, if v_datum_heute/gestern were used in the core logic):
# def test_date_derivation_correctness(bq_client):
#     today_str = date.today().isoformat()
#     yesterday_str = (date.today() - timedelta(days=1)).isoformat()
#     insert_poolbasisprodukt_data(bq_client, [
#         ('X', 1, today_str),
#         ('Y', 2, yesterday_str),
#         ('Z', 3, '2023-01-01') # Irrelevant date
#     ])
#     # Assuming core logic filters by v_datum_heute or v_datum_gestern
#     # This would require modifying the SP's example core logic to use these.
#     # For now, we rely on the explicit use of CURRENT_DATE() and DATE_SUB() in the SP.
#     call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', date.today().strftime('%d%m%Y'), '0')
#     log_entries = get_job_log_entries(bq_client)
#     # Assertions would depend on how v_datum_heute/gestern are used in the core logic.
#     # For instance, if it counted records for today, record_count should be 1.
#     # assert log_entries[-1]['record_count'] == 1
```

---

### Test Case 10: Data Quality - `job_log` Schema and Data Types

*   **Purpose**: Verify that the `job_log` table has the correct schema and that data inserted into it conforms to the expected types, ensuring data quality for auditing and monitoring.
*   **Setup**:
    *   The `project.dataset.job_log` table exists (ensured by fixture).
    *   Execute a test case that populates the `job_log` (e.g., Test Case 6).
*   **Action**:
    *   Query the schema of `project.dataset.job_log` using BigQuery's `INFORMATION_SCHEMA`.
    *   Retrieve an entry from `job_log` and inspect its data types.
*   **Pass/Fail Criterion**:
    *   The schema of `job_log` matches the DDL provided in the design document: `job_name STRING`, `tab_name STRING`, `error_nr INT64`, `error_msg STRING`, `record_count INT64`, `status_msg STRING`, `created_at TIMESTAMP`.
    *   Data types of inserted values (e.g., `error_nr` is an integer, `created_at` is a timestamp) are correctly stored.

```python
def test_job_log_schema_and_data_types(bq_client):
    # First, run a successful test to populate the log
    insert_poolbasisprodukt_data(bq_client, [('A', 100, '2023-01-01')])
    call_stored_procedure(bq_client, 'JOB_ABC', 'ENTRY_123', '01012023', '0')

    # Query schema from INFORMATION_SCHEMA
    schema_query = f"""
        SELECT column_name, data_type
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_log'
        ORDER BY ordinal_position
    """
    schema_rows = bq_client.query(schema_query).result()
    actual_schema = {row.column_name: row.data_type for row in schema_rows}

    expected_schema = {
        'job_name': 'STRING',
        'tab_name': 'STRING',
        'error_nr': 'INT64',
        'error_msg': 'STRING',
        'record_count': 'INT64',
        'status_msg': 'STRING',
        'created_at': 'TIMESTAMP'
    }
    assert actual_schema == expected_schema

    # Retrieve a log entry and check data types
    log_entries = get_job_log_entries(bq_client)
    assert len(log_entries) > 0
    first_entry = log_entries[0] # Should be 'Pruefe Datum'

    assert isinstance(first_entry['job_name'], str)
    assert isinstance(first_entry['tab_name'], str)
    assert first_entry['error_nr'] is None # No error for this entry
    assert first_entry['error_msg'] is None
    assert first_entry['record_count'] is None
    assert isinstance(first_entry['status_msg'], str)
    assert isinstance(first_entry['created_at'], datetime)

    # Check an error entry if possible (e.g., from test_missing_jobkennung)
    # For this, we'd need to run an error test and then query.
    # Or, we can assert the types of the last entry (record_count)
    last_entry = log_entries[-1] # Should be 'Initialbefuellung'
    assert isinstance(last_entry['record_count'], int)
```