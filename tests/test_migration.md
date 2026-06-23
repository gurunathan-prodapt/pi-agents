The migration of `k_ausd_v_ta_disc_zusgf.ksh` to a BigQuery Stored Procedure (`r_ausd_vertrag_control`) involves significant changes in technology and execution environment. The following test cases are designed to validate the behavioral equivalence, transformation correctness, and data integrity of the migrated solution.

**Assumptions for Testing:**
*   A BigQuery project and dataset are configured for testing.
*   The `job_error_log`, `job_run_log`, and `ta_disc_zusgf` tables (with at least `eintragsnr`, `job_kennung`, and `some_other_col` for `ta_disc_zusgf`) have been created in the target BigQuery dataset.
*   A mock `d_ausd_v_ta_disc_zusgf` BigQuery Stored Procedure is available to simulate the core SQL logic's impact on `ta_disc_zusgf`. This mock will be configured per test case to control the number of affected rows.
*   For legacy script execution, it's assumed a controlled environment (e.g., a Docker container) is available where the original KornShell script can be run with its dependencies and environment variables configured. The `pytest` examples will conceptually represent the legacy script's expected output.

---

## Test Case 1: Successful Execution - Output Parity & Record Counting

*   **Purpose:** Verify that the migrated job executes successfully with valid parameters, produces the expected output messages, and correctly logs the number of processed records, matching the legacy behavior. This validates output parity and basic transformation correctness.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_error_log`, `your_project_id.your_dataset_id.job_run_log`, and `your_project_id.your_dataset_id.ta_disc_zusgf` tables are empty.
    2.  Deploy a mock `your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf` stored procedure that inserts a known number of rows (e.g., 5 rows) into `ta_disc_zusgf` for the given `p_EintragsNr`.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_disc_zusgf.ksh` script with valid parameters, e.g., `./k_ausd_v_ta_disc_zusgf.ksh -j "TEST_JOB_01" -f "ENTRY_001"`. Capture its standard output and the content of `$DW_DIR_UTL/bert_k_ausd_v_ta_disc_zusgf_$$.tmp`.
    2.  Execute the BigQuery Stored Procedure `your_project_id.your_dataset_id.r_ausd_vertrag_control` with the same parameters: `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`('TEST_JOB_01', 'ENTRY_001');`.
*   **Pass/Fail Criterion:**
    *   **Output Parity:** The BigQuery procedure's final `SELECT` messages (`' ---------- ENDE Datenverarbeitung ----------'` and the `v_records` value) should match the legacy script's `print` statements and the value read from its `tmpFile`.
    *   **Record Count:** The `records_processed` column in `your_project_id.your_dataset_id.job_run_log` for this run should match the number of rows inserted by the mock `d_ausd_v_ta_disc_zusgf` (e.g., 5).
    *   **Log Entries:** Exactly one entry in `job_run_log` and zero entries in `job_error_log`.
    *   **Data Impact:** `ta_disc_zusgf` should contain the expected number of new rows (e.g., 5) with `eintragsnr = 'ENTRY_001'` and `job_kennung = 'TEST_JOB_01'`.

```python
import subprocess
from google.cloud import bigquery
import pytest
import datetime

# Configuration for BigQuery
PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _run_bq_query(query):
    """Helper to run BigQuery SQL queries."""
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    """Clears all relevant tables before each test."""
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Fixture to set up and tear down test environment."""
    _clear_tables()
    # Deploy mock d_ausd_v_ta_disc_zusgf for this test case
    mock_d_ausd_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_disc_zusgf`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN num_rows_to_affect INT64 DEFAULT 5 -- Default for this test
    )
    BEGIN
      FOR i IN 1 TO num_rows_to_affect DO
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf` (eintragsnr, job_kennung, some_other_col)
        VALUES (p_EintragsNr, p_JobKennung, FORMAT('data_%d', i));
      END FOR;
    END;
    """
    _run_bq_query(mock_d_ausd_sql)
    yield # Run the test
    _clear_tables()

def test_successful_execution_parity_and_record_count():
    job_kennung = "TEST_JOB_01"
    eintrags_nr = "ENTRY_001"
    expected_records = 5 # As defined in the mock d_ausd_v_ta_disc_zusgf

    # --- Legacy Script Execution (Conceptual / Simulated) ---
    # In a real test, you would execute the actual ksh script in a controlled environment
    # and capture its stdout and the content of the temporary file.
    # For this example, we'll assume the expected output based on the script logic.
    # Example of how you might run it:
    # legacy_result = subprocess.run(
    #     ['./k_ausd_v_ta_disc_zusgf.ksh', '-j', job_kennung, '-f', eintrags_nr],
    #     capture_output=True, text=True, env={'DW_DIR_UTL': '/tmp'} # Set a temp dir for tmpFile
    # )
    # assert "---------- ENDE Datenverarbeitung ----------" in legacy_result.stdout
    # # Read tmpFile content (assuming it's written to /tmp/bert_k_ausd_v_ta_disc_zusgf_PID.tmp)
    # legacy_records_from_tmpfile = int(open(f"/tmp/bert_k_ausd_v_ta_disc_zusgf_{legacy_result.pid}.tmp").read().strip())
    # assert legacy_records_from_tmpfile == expected_records

    # For this test, we'll directly assert against the expected values.
    expected_legacy_output_message = "---------- ENDE Datenverarbeitung ----------"
    expected_legacy_records_output = str(expected_records)

    # --- BigQuery Stored Procedure Execution ---
    call_query = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');"
    bq_results = _run_bq_query(call_query)

    # Capture messages and records_processed from BigQuery procedure's final SELECT statements
    bq_messages = []
    bq_processed_records = None
    for row in bq_results:
        if 'message' in row.keys():
            bq_messages.append(row['message'])
        if 'records_processed' in row.keys():
            bq_processed_records = row['records_processed']

    # --- Pass/Fail Criterion Assertions ---
    # 1. Output Parity
    assert expected_legacy_output_message in bq_messages
    assert bq_processed_records == expected_records

    # 2. Log Entries
    error_log_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").rows[0][0]
    run_log_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`").rows[0][0]
    assert error_log_count == 0
    assert run_log_count == 1

    # 3. Run Log Content
    run_log_entry = list(_run_bq_query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`"))[0]
    assert run_log_entry['job_kennung'] == job_kennung
    assert run_log_entry['eintrags_nr'] == eintrags_nr
    assert run_log_entry['tab_name'] == 'ta_disc_zusgf'
    assert run_log_entry['records_processed'] == expected_records
    assert run_log_entry['status'] == 'DONE'
    assert isinstance(run_log_entry['log_ts'], datetime.datetime)
    assert (datetime.datetime.now(datetime.timezone.utc) - run_log_entry['log_ts']).total_seconds() < 60 # Logged recently

    # 4. Data Impact on ta_disc_zusgf
    ta_disc_zusgf_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf` WHERE eintragsnr = '{eintrags_nr}' AND job_kennung = '{job_kennung}'").rows[0][0]
    assert ta_disc_zusgf_count == expected_records
```

---

## Test Case 2: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** Verify that the migrated job correctly handles a missing `p_JobKennung` parameter, logs the error, and exits with the expected error message/SQLSTATE, mirroring the legacy script's behavior. This validates transformation correctness for parameter handling and error logging.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_error_log`, `your_project_id.your_dataset_id.job_run_log`, and `your_project_id.your_dataset_id.ta_disc_zusgf` tables are empty.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_disc_zusgf.ksh` script with missing `-j`, e.g., `./k_ausd_v_ta_disc_zusgf.ksh -f "ENTRY_002"`. Capture its standard error and exit code.
    2.  Attempt to execute the BigQuery Stored Procedure `your_project_id.your_dataset_id.r_ausd_vertrag_control` with `p_JobKennung` as `NULL` or an empty string: `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`(NULL, 'ENTRY_002');` and `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`('', 'ENTRY_002');`. Expect an error (BadRequest exception in Python).
*   **Pass/Fail Criterion:**
    *   **Error Message Parity:** The BigQuery procedure should raise an error with `MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen'` and a `SELECT` message like `'FEHLER: 0 E 193 Jobkennung'`. This should match the legacy script's output.
    *   **Exit Code/SQLSTATE:** The BigQuery procedure should terminate with an error (SQLSTATE '45000'), similar to the legacy script exiting with `ErrNr=193`.
    *   **Log Entries:** Two entries in `job_error_log` (one for NULL, one for empty string) with `err_nr = 193` and `err_arg = 'Jobkennung'`, and zero entries in `job_run_log`.
    *   **No Data Impact:** `ta_disc_zusgf` should remain empty.

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import datetime

PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _run_bq_query(query):
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    _clear_tables()
    # Mock d_ausd_v_ta_disc_zusgf is not called in this error scenario, but good to have it defined.
    mock_d_ausd_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_disc_zusgf`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN num_rows_to_affect INT64 DEFAULT 1
    )
    BEGIN
      -- This should not be reached in this test case
      SELECT 'Mock d_ausd_v_ta_disc_zusgf called unexpectedly' AS message;
    END;
    """
    _run_bq_query(mock_d_ausd_sql)
    yield
    _clear_tables()

def test_missing_jobkennung_parameter_validation():
    eintrags_nr = "ENTRY_002"
    expected_error_message_text = "Bitte ueber Rahmenscript aufrufen"
    expected_error_select_message = "FEHLER: 0 E 193 Jobkennung"
    expected_err_nr = 193
    expected_err_arg = "Jobkennung"

    # --- Legacy Script Execution (Conceptual) ---
    # Example:
    # legacy_result = subprocess.run(
    #     ['./k_ausd_v_ta_disc_zusgf.ksh', '-f', eintrags_nr],
    #     capture_output=True, text=True, env={'DW_DIR_UTL': '/tmp'}
    # )
    # assert legacy_result.returncode == expected_err_nr
    # assert expected_error_select_message in legacy_result.stderr or legacy_result.stdout
    # assert expected_error_message_text in legacy_result.stderr or legacy_result.stdout

    # --- BigQuery Stored Procedure Execution ---
    call_query_null = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`(NULL, '{eintrags_nr}');"
    call_query_empty = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('', '{eintrags_nr}');"

    # Test with NULL p_JobKennung
    with pytest.raises(BadRequest) as excinfo_null:
        _run_bq_query(call_query_null)
    assert expected_error_message_text in str(excinfo_null.value)

    # Test with empty string p_JobKennung
    with pytest.raises(BadRequest) as excinfo_empty:
        _run_bq_query(call_query_empty)
    assert expected_error_message_text in str(excinfo_empty.value)

    # --- Pass/Fail Criterion Assertions ---
    # 1. Log Entries (check for both calls)
    error_log_entries = list(_run_bq_query(f"SELECT err_nr, err_arg, job_kennung, eintrags_nr FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` ORDER BY error_ts ASC"))
    assert len(error_log_entries) == 2

    # Check first error log entry (for NULL)
    assert error_log_entries[0]['err_nr'] == expected_err_nr
    assert error_log_entries[0]['err_arg'] == expected_err_arg
    assert error_log_entries[0]['job_kennung'] is None
    assert error_log_entries[0]['eintrags_nr'] == eintrags_nr

    # Check second error log entry (for empty string)
    assert error_log_entries[1]['err_nr'] == expected_err_nr
    assert error_log_entries[1]['err_arg'] == expected_err_arg
    assert error_log_entries[1]['job_kennung'] == ''
    assert error_log_entries[1]['eintrags_nr'] == eintrags_nr

    run_log_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`").rows[0][0]
    assert run_log_count == 0

    # 2. No Data Impact
    ta_disc_zusgf_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`").rows[0][0]
    assert ta_disc_zusgf_count == 0
```

---

## Test Case 3: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose:** Verify that the migrated job correctly handles a missing `p_EintragsNr` parameter, logs the error, and exits with the expected error message/SQLSTATE. This validates transformation correctness for parameter handling and error logging.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_error_log`, `your_project_id.your_dataset_id.job_run_log`, and `your_project_id.your_dataset_id.ta_disc_zusgf` tables are empty.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_disc_zusgf.ksh` script with missing `-f`, e.g., `./k_ausd_v_ta_disc_zusgf.ksh -j "TEST_JOB_03"`. Capture its standard error and exit code.
    2.  Attempt to execute the BigQuery Stored Procedure `your_project_id.your_dataset_id.r_ausd_vertrag_control` with `p_EintragsNr` as `NULL` or an empty string: `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`('TEST_JOB_03', NULL);` and `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`('TEST_JOB_03', '');`. Expect an error (BadRequest exception in Python).
*   **Pass/Fail Criterion:**
    *   **Error Message Parity:** The BigQuery procedure should raise an error with `MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen'` and a `SELECT` message like `'FEHLER: 0 E 193 EintragsNr'`. This should match the legacy script's output.
    *   **Exit Code/SQLSTATE:** The BigQuery procedure should terminate with an error (SQLSTATE '45000'), similar to the legacy script exiting with `ErrNr=193`.
    *   **Log Entries:** Two entries in `job_error_log` (one for NULL, one for empty string) with `err_nr = 193` and `err_arg = 'EintragsNr'`, and zero entries in `job_run_log`.
    *   **No Data Impact:** `ta_disc_zusgf` should remain empty.

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import datetime

PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _run_bq_query(query):
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    _clear_tables()
    mock_d_ausd_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_disc_zusgf`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN num_rows_to_affect INT64 DEFAULT 1
    )
    BEGIN
      SELECT 'Mock d_ausd_v_ta_disc_zusgf called unexpectedly' AS message;
    END;
    """
    _run_bq_query(mock_d_ausd_sql)
    yield
    _clear_tables()

def test_missing_eintragsnr_parameter_validation():
    job_kennung = "TEST_JOB_03"
    expected_error_message_text = "Bitte ueber Rahmenscript aufrufen"
    expected_error_select_message = "FEHLER: 0 E 193 EintragsNr"
    expected_err_nr = 193
    expected_err_arg = "EintragsNr"

    # --- BigQuery Stored Procedure Execution ---
    call_query_null = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', NULL);"
    call_query_empty = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '');"

    # Test with NULL p_EintragsNr
    with pytest.raises(BadRequest) as excinfo_null:
        _run_bq_query(call_query_null)
    assert expected_error_message_text in str(excinfo_null.value)

    # Test with empty string p_EintragsNr
    with pytest.raises(BadRequest) as excinfo_empty:
        _run_bq_query(call_query_empty)
    assert expected_error_message_text in str(excinfo_empty.value)

    # --- Pass/Fail Criterion Assertions ---
    # 1. Log Entries
    error_log_entries = list(_run_bq_query(f"SELECT err_nr, err_arg, job_kennung, eintrags_nr FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` ORDER BY error_ts ASC"))
    assert len(error_log_entries) == 2

    # Check first error log entry (for NULL)
    assert error_log_entries[0]['err_nr'] == expected_err_nr
    assert error_log_entries[0]['err_arg'] == expected_err_arg
    assert error_log_entries[0]['job_kennung'] == job_kennung
    assert error_log_entries[0]['eintrags_nr'] is None

    # Check second error log entry (for empty string)
    assert error_log_entries[1]['err_nr'] == expected_err_nr
    assert error_log_entries[1]['err_arg'] == expected_err_arg
    assert error_log_entries[1]['job_kennung'] == job_kennung
    assert error_log_entries[1]['eintrags_nr'] == ''

    run_log_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`").rows[0][0]
    assert run_log_count == 0

    # 2. No Data Impact
    ta_disc_zusgf_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`").rows[0][0]
    assert ta_disc_zusgf_count == 0
```

---

## Test Case 4: Data Quality - `job_run_log` Schema and Content

*   **Purpose:** Verify that the `job_run_log` table has the correct schema and that the data inserted into it is accurate and complete for a successful run. This validates data quality and schema assertions.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_error_log`, `your_project_id.your_dataset_id.job_run_log`, and `your_project_id.your_dataset_id.ta_disc_zusgf` tables are empty.
    2.  Deploy a mock `your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf` to insert a known number of rows (e.g., 7).
*   **Action:**
    1.  Execute the BigQuery Stored Procedure `your_project_id.your_dataset_id.r_ausd_vertrag_control` with valid parameters, e.g., `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`('DQ_JOB', 'DQ_ENTRY');`.
*   **Pass/Fail Criterion:**
    *   **Schema:** Query `INFORMATION_SCHEMA.COLUMNS` to confirm `job_run_log` has the expected columns and data types: `log_ts` (TIMESTAMP), `procedure_name` (STRING), `job_kennung` (STRING), `eintrags_nr` (STRING), `tab_name` (STRING), `records_processed` (INT64), `status` (STRING).
    *   **Content:** One row in `job_run_log`.
        *   `log_ts` should be a recent timestamp.
        *   `procedure_name` should be `'r_ausd_vertrag_control'`.
        *   `job_kennung` should be `'DQ_JOB'`.
        *   `eintrags_nr` should be `'DQ_ENTRY'`.
        *   `tab_name` should be `'ta_disc_zusgf'`.
        *   `records_processed` should be 7.
        *   `status` should be `'DONE'`.

```python
import pytest
from google.cloud import bigquery
import datetime

PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _run_bq_query(query):
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    _clear_tables()
    mock_d_ausd_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_disc_zusgf`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN num_rows_to_affect INT64 DEFAULT 7 -- For this test
    )
    BEGIN
      FOR i IN 1 TO num_rows_to_affect DO
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf` (eintragsnr, job_kennung, some_other_col)
        VALUES (p_EintragsNr, p_JobKennung, FORMAT('data_%d', i));
      END FOR;
    END;
    """
    _run_bq_query(mock_d_ausd_sql)
    yield
    _clear_tables()

def test_job_run_log_schema_and_content():
    job_kennung = "DQ_JOB"
    eintrags_nr = "DQ_ENTRY"
    expected_records = 7

    # --- Action ---
    call_query = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');"
    _run_bq_query(call_query)

    # --- Pass/Fail Criterion Assertions ---
    # 1. Schema Assertion
    schema_query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_run_log'
    ORDER BY ordinal_position
    """
    schema_results = _run_bq_query(schema_query)
    actual_schema = {row['column_name']: row['data_type'] for row in schema_results}

    expected_schema = {
        'log_ts': 'TIMESTAMP',
        'procedure_name': 'STRING',
        'job_kennung': 'STRING',
        'eintrags_nr': 'STRING',
        'tab_name': 'STRING',
        'records_processed': 'INT64',
        'status': 'STRING'
    }
    assert actual_schema == expected_schema

    # 2. Content Assertion
    run_log_entries = list(_run_bq_query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`"))
    assert len(run_log_entries) == 1
    log_entry = run_log_entries[0]

    assert log_entry['procedure_name'] == 'r_ausd_vertrag_control'
    assert log_entry['job_kennung'] == job_kennung
    assert log_entry['eintrags_nr'] == eintrags_nr
    assert log_entry['tab_name'] == 'ta_disc_zusgf'
    assert log_entry['records_processed'] == expected_records
    assert log_entry['status'] == 'DONE'
    assert isinstance(log_entry['log_ts'], datetime.datetime)
    assert (datetime.datetime.now(datetime.timezone.utc) - log_entry['log_ts']).total_seconds() < 60
```

---

## Test Case 5: Data Quality - `job_error_log` Schema and Content

*   **Purpose:** Verify that the `job_error_log` table has the correct schema and that the data inserted into it is accurate and complete for an error scenario. This validates data quality and schema assertions for error handling.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_error_log`, `your_project_id.your_dataset_id.job_run_log`, and `your_project_id.your_dataset_id.ta_disc_zusgf` tables are empty.
*   **Action:**
    1.  Attempt to execute the BigQuery Stored Procedure `your_project_id.your_dataset_id.r_ausd_vertrag_control` with a missing `p_JobKennung`, e.g., `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`(NULL, 'ERROR_ENTRY');`.
*   **Pass/Fail Criterion:**
    *   **Schema:** Query `INFORMATION_SCHEMA.COLUMNS` to confirm `job_error_log` has the expected columns and data types: `error_ts` (TIMESTAMP), `procedure_name` (STRING), `err_nr` (INT64), `err_arg` (STRING), `job_kennung` (STRING), `eintrags_nr` (STRING).
    *   **Content:** One row in `job_error_log`.
        *   `error_ts` should be a recent timestamp.
        *   `procedure_name` should be `'r_ausd_vertrag_control'`.
        *   `err_nr` should be `193`.
        *   `err_arg` should be `'Jobkennung'`.
        *   `job_kennung` should be `NULL`.
        *   `eintrags_nr` should be `'ERROR_ENTRY'`.

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import datetime

PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _run_bq_query(query):
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    _clear_tables()
    # Mock d_ausd_v_ta_disc_zusgf is not called in this error scenario.
    yield
    _clear_tables()

def test_job_error_log_schema_and_content():
    eintrags_nr = "ERROR_ENTRY"
    expected_err_nr = 193
    expected_err_arg = "Jobkennung"

    # --- Action ---
    call_query = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`(NULL, '{eintrags_nr}');"
    with pytest.raises(BadRequest): # Expecting the procedure to raise an error
        _run_bq_query(call_query)

    # --- Pass/Fail Criterion Assertions ---
    # 1. Schema Assertion
    schema_query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_error_log'
    ORDER BY ordinal_position
    """
    schema_results = _run_bq_query(schema_query)
    actual_schema = {row['column_name']: row['data_type'] for row in schema_results}

    expected_schema = {
        'error_ts': 'TIMESTAMP',
        'procedure_name': 'STRING',
        'err_nr': 'INT64',
        'err_arg': 'STRING',
        'job_kennung': 'STRING',
        'eintrags_nr': 'STRING'
    }
    assert actual_schema == expected_schema

    # 2. Content Assertion
    error_log_entries = list(_run_bq_query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`"))
    assert len(error_log_entries) == 1
    log_entry = error_log_entries[0]

    assert log_entry['procedure_name'] == 'r_ausd_vertrag_control'
    assert log_entry['err_nr'] == expected_err_nr
    assert log_entry['err_arg'] == expected_err_arg
    assert log_entry['job_kennung'] is None # Because it was passed as NULL
    assert log_entry['eintrags_nr'] == eintrags_nr
    assert isinstance(log_entry['error_ts'], datetime.datetime)
    assert (datetime.datetime.now(datetime.timezone.utc) - log_entry['error_ts']).total_seconds() < 60
```

---

## Test Case 6: Edge Case - `d_ausd_v_ta_disc_zusgf` Affects Zero Rows

*   **Purpose:** Verify that the `r_ausd_vertrag_control` correctly logs 0 records processed if the underlying `d_ausd_v_ta_disc_zusgf` script performs no updates or inserts. This validates transformation correctness for record counting logic.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_error_log`, `your_project_id.your_dataset_id.job_run_log`, and `your_project_id.your_dataset_id.ta_disc_zusgf` tables are empty.
    2.  Deploy a mock `your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf` that inserts 0 rows.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure `your_project_id.your_dataset_id.r_ausd_vertrag_control` with valid parameters, e.g., `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`('ZERO_REC_JOB', 'ZERO_REC_ENTRY');`.
*   **Pass/Fail Criterion:**
    *   The `records_processed` value returned by the procedure and logged in `job_run_log` should be 0.
    *   `ta_disc_zusgf` should remain empty.
    *   Exactly one entry in `job_run_log`, zero in `job_error_log`.

```python
import pytest
from google.cloud import bigquery
import datetime

PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _run_bq_query(query):
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    _clear_tables()
    mock_d_ausd_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_disc_zusgf`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN num_rows_to_affect INT64 DEFAULT 0 -- For this test: 0 rows
    )
    BEGIN
      -- Simulate DML operation that affects 0 rows
      -- No INSERT statement here.
      SELECT 'Mock d_ausd_v_ta_disc_zusgf called, affecting 0 rows.' AS message;
    END;
    """
    _run_bq_query(mock_d_ausd_sql)
    yield
    _clear_tables()

def test_zero_records_affected():
    job_kennung = "ZERO_REC_JOB"
    eintrags_nr = "ZERO_REC_ENTRY"
    expected_records = 0

    # --- Action ---
    call_query = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');"
    bq_results = _run_bq_query(call_query)

    bq_processed_records = None
    for row in bq_results:
        if 'records_processed' in row.keys():
            bq_processed_records = row['records_processed']

    # --- Pass/Fail Criterion Assertions ---
    assert bq_processed_records == expected_records

    run_log_entries = list(_run_bq_query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`"))
    assert len(run_log_entries) == 1
    assert run_log_entries[0]['records_processed'] == expected_records
    assert run_log_entries[0]['status'] == 'DONE'

    error_log_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").rows[0][0]
    assert error_log_count == 0

    ta_disc_zusgf_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`").rows[0][0]
    assert ta_disc_zusgf_count == 0
```

---

## Test Case 7: External System Replacement - `h_alis_sqlplus.ksh` (Implicit)

*   **Purpose:** Verify that the replacement of `h_alis_sqlplus.ksh` (which wrapped SQL*Plus execution) with direct BigQuery stored procedure calls functions correctly. This is implicitly tested by the successful execution of `d_ausd_v_ta_disc_zusgf` via `r_ausd_vertrag_control`. This validates external system replacement.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_error_log`, `your_project_id.your_dataset_id.job_run_log`, and `your_project_id.your_dataset_id.ta_disc_zusgf` tables are empty.
    2.  Deploy a mock `your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf` that performs a known DML operation (e.g., inserts 3 rows).
*   **Action:**
    1.  Execute `your_project_id.your_dataset_id.r_ausd_vertrag_control` with valid parameters, e.g., `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`('SQLPLUS_REPL_JOB', 'SQLPLUS_REPL_ENTRY');`.
*   **Pass/Fail Criterion:**
    *   The `d_ausd_v_ta_disc_zusgf` procedure is successfully called, and its intended data modifications on `ta_disc_zusgf` are observed (e.g., 3 new rows).
    *   `job_run_log` is updated with the correct `records_processed` count.
    *   No errors related to SQL execution or connectivity are logged.

```python
import pytest
from google.cloud import bigquery
import datetime

PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _run_bq_query(query):
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    _clear_tables()
    # Mock d_ausd_v_ta_disc_zusgf that actually inserts rows
    mock_d_ausd_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_disc_zusgf`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN num_rows_to_affect INT64 DEFAULT 3 -- For this test
    )
    BEGIN
      FOR i IN 1 TO num_rows_to_affect DO
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf` (eintragsnr, job_kennung, some_other_col)
        VALUES (p_EintragsNr, p_JobKennung, FORMAT('data_sqlplus_replacement_%d', i));
      END FOR;
    END;
    """
    _run_bq_query(mock_d_ausd_sql)
    yield
    _clear_tables()

def test_sqlplus_replacement_functionality():
    job_kennung = "SQLPLUS_REPL_JOB"
    eintrags_nr = "SQLPLUS_REPL_ENTRY"
    expected_records = 3

    # --- Action ---
    call_query = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');"
    _run_bq_query(call_query)

    # --- Pass/Fail Criterion Assertions ---
    # 1. Verify d_ausd_v_ta_disc_zusgf's effect on ta_disc_zusgf
    ta_disc_zusgf_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf` WHERE eintragsnr = '{eintrags_nr}' AND job_kennung = '{job_kennung}'").rows[0][0]
    assert ta_disc_zusgf_count == expected_records

    # 2. Verify job_run_log entry
    run_log_entries = list(_run_bq_query(f"SELECT records_processed, status FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'"))
    assert len(run_log_entries) == 1
    assert run_log_entries[0]['records_processed'] == expected_records
    assert run_log_entries[0]['status'] == 'DONE'

    # 3. Verify no error logs
    error_log_count = _run_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").rows[0][0]
    assert error_log_count == 0
```

---

## Test Case 8: Transformation Correctness - `v_TabName` Value

*   **Purpose:** Verify that the `v_TabName` variable, which represents the target table name, is correctly set and logged in `job_run_log` as per the legacy script's hardcoded value. This validates transformation correctness for variable handling.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_error_log`, `your_project_id.your_dataset_id.job_run_log`, and `your_project_id.your_dataset_id.ta_disc_zusgf` tables are empty.
    2.  Deploy a mock `your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf` that performs a simple DML operation.
*   **Action:**
    1.  Execute `your_project_id.your_dataset_id.r_ausd_vertrag_control` with valid parameters, e.g., `CALL `your_project_id.your_dataset_id.r_ausd_vertrag_control`('TABNAME_JOB', 'TABNAME_ENTRY');`.
*   **Pass/Fail Criterion:**
    *   The `tab_name` column in `job_run_log` for the executed run should be `'ta_disc_zusgf'`, matching the hardcoded value in the legacy script.

```python
import pytest
from google.cloud import bigquery
import datetime

PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _run_bq_query(query):
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`")
    _run_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    _clear_tables()
    mock_d_ausd_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_disc_zusgf`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN num_rows_to_affect INT64 DEFAULT 1
    )
    BEGIN
      INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf` (eintragsnr, job_kennung, some_other_col)
      VALUES (p_EintragsNr, p_JobKennung, 'test_data');
    END;
    """
    _run_bq_query(mock_d_ausd_sql)
    yield
    _clear_tables()

def test_v_tabname_correctness():
    job_kennung = "TABNAME_JOB"
    eintrags_nr = "TABNAME_ENTRY"
    expected_tab_name = "ta_disc_zusgf"

    # --- Action ---
    call_query = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');"
    _run_bq_query(call_query)

    # --- Pass/Fail Criterion Assertions ---
    run_log_entry = list(_run_bq_query(f"SELECT tab_name FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'"))[0]
    assert run_log_entry['tab_name'] == expected_tab_name
```