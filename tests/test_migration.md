The following migration validation tests are designed for the BigQuery stored procedure `ausd_bp_ta_bpr_optionen_wrapper`, which replaces the KornShell script `r_ausd_bp_ta_bpr_optionen.ksh`. These tests cover output parity, transformation correctness, external system replacements (by verifying interactions with BigQuery logging/status tables and the core logic procedure), and data quality assertions.

The tests are written using `pytest` and assume a BigQuery client is configured to interact with the specified `PROJECT_ID` and `DATASET_ID`. A `setup_bigquery_objects` fixture is included to ensure the necessary tables and procedures are created before tests run.

---

## Test Setup (Pytest Fixtures and Helpers)

```python
import pytest
from google.cloud import bigquery
from datetime import date, datetime, timezone, timedelta
import uuid
import time

# --- Configuration ---
# Replace with your actual GCP project ID and BigQuery dataset name
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_bq_dataset"

# Fully qualified table names
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
JOB_STATUS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_status"
WRAPPER_PROC = f"{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_optionen_wrapper"
CORE_PROC = f"{PROJECT_ID}.{DATASET_ID}.k_ausd_bp_ta_bpr_optionen"
LOG_MSG_PROC = f"{PROJECT_ID}.{DATASET_ID}.f_alis_log_message"
UPDATE_STATUS_PROC = f"{PROJECT_ID}.{DATASET_ID}.f_alis_update_job_status"

# --- Pytest Fixtures ---

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the tests."""
    client = bigquery.Client(project=PROJECT_ID)
    yield client
    # Optional: Add cleanup logic here if you want to delete test data after all tests.
    # For example, truncating tables:
    # client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`;").result()
    # client.query(f"TRUNCATE TABLE `{JOB_STATUS_TABLE}`;").result()

@pytest.fixture(scope="module", autouse=True)
def setup_bigquery_objects(bq_client):
    """
    Ensures all necessary BigQuery tables and procedures are created before tests run.
    This fixture will run once for the module.
    """
    print(f"\n--- Setting up BigQuery objects in {PROJECT_ID}.{DATASET_ID} ---")

    # 1. Create Dataset if it doesn't exist
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US" # Or your desired location
        bq_client.create_dataset(dataset)
        print(f"Dataset {DATASET_ID} created.")

    # 2. DDL for job_log table
    job_log_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{JOB_LOG_TABLE}` (
        job_run_id STRING NOT NULL OPTIONS(description="Unique ID for each job run, corresponding to JobKennung"),
        job_name STRING NOT NULL OPTIONS(description="Name of the job (e.g., r_ausd_bp_ta_bpr_optionen.ksh)"),
        log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
        log_level STRING NOT NULL OPTIONS(description="Log level (INFO, WARNING, ERROR, DEBUG)"),
        message STRING NOT NULL OPTIONS(description="Log message content"),
        stichtag DATE OPTIONS(description="Reference date for the job run (p_stichtag)"),
        wiederanlaufwert INT64 OPTIONS(description="Restart value for the job (p_wiederanlaufWert)"),
        process_id STRING OPTIONS(description="Identifier for the process within the job run (like shell $$)"),
        line_number INT64 OPTIONS(description="Line number in the source code where the error occurred, if applicable"),
        error_code STRING OPTIONS(description="Error code from the original script's error handling (ErrNr)"),
        error_arg STRING OPTIONS(description="Error argument from the original script's error handling (ErrArg)"),
        log_entry_id STRING NOT NULL OPTIONS(description="Unique ID for this log entry, similar to DW_EintragsNr")
    )
    PARTITION BY DATE(log_timestamp)
    CLUSTER BY job_run_id, log_level;
    """
    bq_client.query(job_log_ddl).result()
    print(f"Table {JOB_LOG_TABLE} ensured.")

    # 3. DDL for job_status table
    job_status_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{JOB_STATUS_TABLE}` (
        job_run_id STRING NOT NULL OPTIONS(description="Unique ID for each job run, corresponding to JobKennung"),
        job_name STRING NOT NULL OPTIONS(description="Name of the job (e.g., r_ausd_bp_ta_bpr_optionen.ksh)"),
        start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job run started"),
        end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job run ended"),
        status STRING NOT NULL OPTIONS(description="Current status of the job run (RUNNING, SUCCESS, FAILED)"),
        stichtag DATE OPTIONS(description="Reference date for the job run (p_stichtag)"),
        wiederanlaufwert INT64 OPTIONS(description="Restart value for the job (p_wiederanlaufWert)")
    );
    """
    bq_client.query(job_status_ddl).result()
    print(f"Table {JOB_STATUS_TABLE} ensured.")

    # 4. Procedures (using the provided generated code)
    procedures_sql = {
        "f_alis_log_message": f"""
            CREATE OR REPLACE PROCEDURE `{LOG_MSG_PROC}`(
                IN p_job_run_id STRING, IN p_job_name STRING, IN p_log_level STRING, IN p_message STRING,
                IN p_stichtag DATE, IN p_wiederanlaufwert INT64, IN p_process_id STRING,
                IN p_line_number INT64, IN p_error_code STRING, IN p_error_arg STRING
            )
            BEGIN
                INSERT INTO `{JOB_LOG_TABLE}` (
                    job_run_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert, process_id, line_number, error_code, error_arg, log_entry_id
                )
                VALUES (
                    p_job_run_id, p_job_name, CURRENT_TIMESTAMP(), p_log_level, p_message, p_stichtag, p_wiederanlaufwert,
                    p_process_id, p_line_number, p_error_code, p_error_arg, GENERATE_UUID()
                );
            END;
        """,
        "f_alis_update_job_status": f"""
            CREATE OR REPLACE PROCEDURE `{UPDATE_STATUS_PROC}`(
                IN p_job_run_id STRING, IN p_job_name STRING, IN p_status STRING,
                IN p_stichtag DATE, IN p_wiederanlaufwert INT64, IN p_start_timestamp TIMESTAMP DEFAULT NULL
            )
            BEGIN
                IF p_status = 'RUNNING' THEN
                    INSERT INTO `{JOB_STATUS_TABLE}` (
                        job_run_id, job_name, start_timestamp, status, stichtag, wiederanlaufwert
                    )
                    VALUES (
                        p_job_run_id, p_job_name, COALESCE(p_start_timestamp, CURRENT_TIMESTAMP()), p_status, p_stichtag, p_wiederanlaufwert
                    );
                ELSE
                    UPDATE `{JOB_STATUS_TABLE}`
                    SET
                        end_timestamp = CURRENT_TIMESTAMP(),
                        status = p_status
                    WHERE job_run_id = p_job_run_id AND job_name = p_job_name;
                END IF;
            END;
        """,
        "k_ausd_bp_ta_bpr_optionen": f"""
            CREATE OR REPLACE PROCEDURE `{CORE_PROC}`(
                IN p_job_run_id STRING, IN p_stichtag DATE, IN p_wiederanlaufwert INT64
            )
            BEGIN
                CALL `{LOG_MSG_PROC}`(
                    p_job_run_id, 'k_ausd_bp_ta_bpr_optionen', 'INFO',
                    'Core business logic procedure (placeholder) called successfully.',
                    p_stichtag, p_wiederanlaufwert, CAST(CURRENT_PROCESS_ID() AS STRING),
                    NULL, NULL, NULL
                );
            END;
        """,
        "ausd_bp_ta_bpr_optionen_wrapper": f"""
            CREATE OR REPLACE PROCEDURE `{WRAPPER_PROC}`(
                IN p_stichtag_raw STRING OPTIONS(description="Optional Stichtag (reference date) in 'YYYY-MM-DD' format. Defaults to current date."),
                IN p_wiederanlaufwert_raw STRING OPTIONS(description="Optional Wiederanlaufwert (restart value). Defaults to 0.")
            )
            BEGIN
                DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_bpr_optionen.ksh';
                DECLARE v_job_run_id STRING DEFAULT GENERATE_UUID();
                DECLARE v_process_id STRING DEFAULT CAST(CURRENT_PROCESS_ID() AS STRING);
                DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

                DECLARE v_stichtag DATE;
                DECLARE v_wiederanlaufwert INT64;

                IF p_stichtag_raw IS NULL OR p_stichtag_raw = '' THEN
                    SET v_stichtag = CURRENT_DATE();
                    CALL `{LOG_MSG_PROC}`(
                        v_job_run_id, v_job_name, 'INFO',
                        FORMAT('Stichtag not provided, defaulting to current date: %t', v_stichtag),
                        v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
                    );
                ELSE
                    BEGIN
                        SET v_stichtag = PARSE_DATE('%Y-%m-%d', p_stichtag_raw);
                        CALL `{LOG_MSG_PROC}`(
                            v_job_run_id, v_job_name, 'INFO',
                            FORMAT('Stichtag provided: %t', v_stichtag),
                            v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
                        );
                    EXCEPTION WHEN ERROR THEN
                        CALL `{LOG_MSG_PROC}`(
                            v_job_run_id, v_job_name, 'ERROR',
                            FORMAT('Invalid Stichtag format provided: %s. Expected YYYY-MM-DD. Exiting.', p_stichtag_raw),
                            v_stichtag, v_wiederanlaufwert, v_process_id, NULL, 'DATE_PARSE_ERROR', p_stichtag_raw
                        );
                        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Invalid Stichtag format: %s', p_stichtag_raw);
                    END;
                END IF;

                IF p_wiederanlaufwert_raw IS NULL OR p_wiederanlaufwert_raw = '' THEN
                    SET v_wiederanlaufwert = 0;
                    CALL `{LOG_MSG_PROC}`(
                        v_job_run_id, v_job_name, 'INFO',
                        FORMAT('Wiederanlaufwert not provided, defaulting to 0: %d', v_wiederanlaufwert),
                        v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
                    );
                ELSE
                    BEGIN
                        SET v_wiederanlaufwert = CAST(p_wiederanlaufwert_raw AS INT64);
                        CALL `{LOG_MSG_PROC}`(
                            v_job_run_id, v_job_name, 'INFO',
                            FORMAT('Wiederanlaufwert provided: %d', v_wiederanlaufwert),
                            v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
                        );
                    EXCEPTION WHEN ERROR THEN
                        CALL `{LOG_MSG_PROC}`(
                            v_job_run_id, v_job_name, 'ERROR',
                            FORMAT('Invalid Wiederanlaufwert format provided: %s. Expected integer. Exiting.', p_wiederanlaufwert_raw),
                            v_stichtag, v_wiederanlaufwert, v_process_id, NULL, 'INT_PARSE_ERROR', p_wiederanlaufwert_raw
                        );
                        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Invalid Wiederanlaufwert format: %s', p_wiederanlaufwert_raw);
                    END;
                END IF;

                CALL `{UPDATE_STATUS_PROC}`(
                    v_job_run_id, v_job_name, 'RUNNING', v_stichtag, v_wiederanlaufwert, v_start_timestamp
                );

                BEGIN
                    CALL `{LOG_MSG_PROC}`(
                        v_job_run_id, v_job_name, 'INFO',
                        FORMAT('Job %s started with Stichtag=%t, Wiederanlaufwert=%d', v_job_name, v_stichtag, v_wiederanlaufwert),
                        v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
                    );

                    CALL `{CORE_PROC}`(
                        v_job_run_id, v_stichtag, v_wiederanlaufwert
                    );

                    CALL `{LOG_MSG_PROC}`(
                        v_job_run_id, v_job_name, 'INFO',
                        FORMAT('Job %s completed successfully.', v_job_name),
                        v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
                    );

                    CALL `{UPDATE_STATUS_PROC}`(
                        v_job_run_id, v_job_name, 'SUCCESS', v_stichtag, v_wiederanlaufwert
                    );

                EXCEPTION WHEN ERROR THEN
                    DECLARE v_error_message STRING;
                    DECLARE v_stack_trace STRING;
                    DECLARE v_error_code STRING;
                    DECLARE v_error_line INT64;

                    SET v_error_message = @@error.message;
                    SET v_stack_trace = @@error.stack_trace;
                    SET v_error_code = @@error.code;
                    SET v_error_line = @@error.statement_text_start;

                    CALL `{LOG_MSG_PROC}`(
                        v_job_run_id, v_job_name, 'ERROR',
                        FORMAT('Job %s failed. Error: %s. Stack Trace: %s', v_job_name, v_error_message, v_stack_trace),
                        v_stichtag, v_wiederanlaufwert, v_process_id, v_error_line, v_error_code, NULL
                    );

                    CALL `{UPDATE_STATUS_PROC}`(
                        v_job_run_id, v_job_name, 'FAILED', v_stichtag, v_wiederanlaufwert
                    );

                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Job %s failed: %s', v_job_name, v_error_message);
                END;
            END;
        """
    }

    for proc_name, proc_sql in procedures_sql.items():
        bq_client.query(proc_sql).result()
        print(f"Procedure {proc_name} ensured.")

    print("--- BigQuery objects setup complete ---")
    yield # Yield control to tests

    # Teardown: Clean up tables and procedures if desired.
    # For robust testing, consider truncating tables before each test or using
    # a dedicated test dataset that is dropped after the test suite.
    # For this example, we'll leave them for inspection.

# --- Helper Functions for Tests ---

def call_wrapper_procedure(bq_client, stichtag_raw=None, wiederanlaufwert_raw=None):
    """Helper function to call the wrapper procedure and capture outcome."""
    stichtag_param = f"'{stichtag_raw}'" if stichtag_raw is not None else "NULL"
    wiederanlaufwert_param = f"'{wiederanlaufwert_raw}'" if wiederanlaufwert_raw is not None else "NULL"
    query = f"CALL {WRAPPER_PROC}({stichtag_param}, {wiederanlaufwert_param});"
    try:
        job = bq_client.query(query)
        job.result() # Wait for job to complete
        return True, None # Success
    except Exception as e:
        return False, str(e) # Failure

def get_latest_job_run_id(bq_client):
    """Retrieves the job_run_id of the most recent job status entry."""
    query = f"""
    SELECT job_run_id
    FROM `{JOB_STATUS_TABLE}`
    ORDER BY start_timestamp DESC
    LIMIT 1
    """
    rows = bq_client.query(query).result()
    for row in rows:
        return row.job_run_id
    return None

def get_job_log_entries(bq_client, job_run_id):
    """Retrieves log entries for a given job_run_id."""
    query = f"""
    SELECT log_level, message, stichtag, wiederanlaufwert, error_code, error_arg
    FROM `{JOB_LOG_TABLE}`
    WHERE job_run_id = '{job_run_id}'
    ORDER BY log_timestamp
    """
    return list(bq_client.query(query).result())

def get_job_status_entry(bq_client, job_run_id):
    """Retrieves the job status entry for a given job_run_id."""
    query = f"""
    SELECT status, stichtag, wiederanlaufwert, start_timestamp, end_timestamp
    FROM `{JOB_STATUS_TABLE}`
    WHERE job_run_id = '{job_run_id}'
    """
    rows = list(bq_client.query(query).result())
    return rows[0] if rows else None

def get_table_schema(bq_client, table_id):
    """Retrieves the schema of a given BigQuery table."""
    table = bq_client.get_table(table_id)
    return {field.name: field.field_type for field in table.schema}

```

---

## Test Cases

### Test Case 1: Successful Execution with Default Parameters

**Purpose:** Verify that the wrapper procedure executes successfully when no parameters are provided, correctly applying default values for `Stichtag` and `Wiederanlaufwert`, and logging the execution flow. This covers output parity and transformation correctness for defaults.

**Setup:**
Ensure the `job_log` and `job_status` tables are empty or can be easily queried for the latest run. The `setup_bigquery_objects` fixture handles procedure creation.

**Action:**
Call the `ausd_bp_ta_bpr_optionen_wrapper` procedure without any parameters.

```python
def test_successful_execution_with_defaults(bq_client):
    # Action
    success, error_message = call_wrapper_procedure(bq_client)
    assert success, f"Procedure call failed: {error_message}"

    job_run_id = get_latest_job_run_id(bq_client)
    assert job_run_id is not None, "No job status entry found."

    # Retrieve logs and status
    log_entries = get_job_log_entries(bq_client, job_run_id)
    status_entry = get_job_status_entry(bq_client, job_run_id)

    # Pass/Fail Criterion
    # 1. Job Status: Should be SUCCESS
    assert status_entry.status == 'SUCCESS'
    assert status_entry.start_timestamp is not None
    assert status_entry.end_timestamp is not None
    assert status_entry.end_timestamp > status_entry.start_timestamp

    # 2. Parameter Defaults: Stichtag should be today, Wiederanlaufwert should be 0
    expected_stichtag = date.today()
    assert status_entry.stichtag == expected_stichtag
    assert status_entry.wiederanlaufwert == 0

    # 3. Log Entries: Verify key log messages and their order
    log_messages = [entry.message for entry in log_entries]
    assert any(f"Stichtag not provided, defaulting to current date: {expected_stichtag}" in msg for msg in log_messages)
    assert any("Wiederanlaufwert not provided, defaulting to 0: 0" in msg for msg in log_messages)
    assert any("Job r_ausd_bp_ta_bpr_optionen.ksh started with Stichtag=" in msg for msg in log_messages)
    assert any("Core business logic procedure (placeholder) called successfully." in msg for msg in log_messages)
    assert any("Job r_ausd_bp_ta_bpr_optionen.ksh completed successfully." in msg for msg in log_messages)
    assert all(entry.log_level == 'INFO' for entry in log_entries if entry.error_code is None)

    # 4. External System Replacement (Core Logic Call): Verify the core procedure was called
    assert any(entry.job_name == 'k_ausd_bp_ta_bpr_optionen' for entry in log_entries)

    print(f"\nTest 'test_successful_execution_with_defaults' passed for job_run_id: {job_run_id}")
```

### Test Case 2: Successful Execution with Explicit Parameters

**Purpose:** Verify that the wrapper procedure correctly processes and uses explicitly provided `Stichtag` and `Wiederanlaufwert` parameters, and logs them accurately. This covers output parity and transformation correctness for explicit inputs.

**Setup:**
As above.

**Action:**
Call the `ausd_bp_ta_bpr_optionen_wrapper` procedure with a specific `Stichtag` (e.g., '2023-01-15') and `Wiederanlaufwert` (e.g., '12345').

```python
def test_successful_execution_with_explicit_parameters(bq_client):
    # Setup specific parameters
    test_stichtag_str = '2023-01-15'
    test_stichtag_date = date(2023, 1, 15)
    test_wiederanlaufwert = 54321

    # Action
    success, error_message = call_wrapper_procedure(bq_client, test_stichtag_str, str(test_wiederanlaufwert))
    assert success, f"Procedure call failed: {error_message}"

    job_run_id = get_latest_job_run_id(bq_client)
    assert job_run_id is not None, "No job status entry found."

    # Retrieve logs and status
    log_entries = get_job_log_entries(bq_client, job_run_id)
    status_entry = get_job_status_entry(bq_client, job_run_id)

    # Pass/Fail Criterion
    # 1. Job Status: Should be SUCCESS
    assert status_entry.status == 'SUCCESS'

    # 2. Parameter Values: Should match provided inputs
    assert status_entry.stichtag == test_stichtag_date
    assert status_entry.wiederanlaufwert == test_wiederanlaufwert

    # 3. Log Entries: Verify key log messages reflect provided parameters
    log_messages = [entry.message for entry in log_entries]
    assert any(f"Stichtag provided: {test_stichtag_date}" in msg for msg in log_messages)
    assert any(f"Wiederanlaufwert provided: {test_wiederanlaufwert}" in msg for msg in log_messages)
    assert any(f"Job r_ausd_bp_ta_bpr_optionen.ksh started with Stichtag={test_stichtag_date}, Wiederanlaufwert={test_wiederanlaufwert}" in msg for msg in log_messages)
    assert any("Core business logic procedure (placeholder) called successfully." in msg for msg in log_messages)
    assert any("Job r_ausd_bp_ta_bpr_optionen.ksh completed successfully." in msg for msg in log_messages)
    assert all(entry.log_level == 'INFO' for entry in log_entries if entry.error_code is None)

    print(f"\nTest 'test_successful_execution_with_explicit_parameters' passed for job_run_id: {job_run_id}")
```

### Test Case 3: Invalid Stichtag Parameter Handling

**Purpose:** Verify that the wrapper procedure correctly handles an invalid `Stichtag` format, logs an error, updates the job status to FAILED, and terminates with an error. This covers transformation correctness (type handling, error handling) and output parity for error scenarios.

**Setup:**
As above.

**Action:**
Call the `ausd_bp_ta_bpr_optionen_wrapper` procedure with an invalid `Stichtag` string (e.g., '2023/01/15').

```python
def test_invalid_stichtag_parameter(bq_client):
    # Setup specific invalid parameter
    invalid_stichtag_str = '2023/01/15' # Invalid format

    # Action
    success, error_message = call_wrapper_procedure(bq_client, invalid_stichtag_str, '100')
    assert not success, "Procedure call should have failed but succeeded."

    job_run_id = get_latest_job_run_id(bq_client)
    assert job_run_id is not None, "No job status entry found."

    # Retrieve logs and status
    log_entries = get_job_log_entries(bq_client, job_run_id)
    status_entry = get_job_status_entry(bq_client, job_run_id)

    # Pass/Fail Criterion
    # 1. Job Status: Should be FAILED
    assert status_entry.status == 'FAILED'

    # 2. Error Message: Should contain specific error details
    assert "Invalid Stichtag format" in error_message
    assert "DATE_PARSE_ERROR" in error_message # Check for the SIGNAL SQLSTATE message

    # 3. Log Entries: Verify an ERROR log entry is present
    error_logs = [entry for entry in log_entries if entry.log_level == 'ERROR']
    assert len(error_logs) >= 1, "Expected at least one ERROR log entry."
    assert any(f"Invalid Stichtag format provided: {invalid_stichtag_str}. Expected YYYY-MM-DD. Exiting." in entry.message for entry in error_logs)
    assert any(entry.error_code == 'DATE_PARSE_ERROR' for entry in error_logs)
    assert any(entry.error_arg == invalid_stichtag_str for entry in error_logs)

    print(f"\nTest 'test_invalid_stichtag_parameter' passed for job_run_id: {job_run_id}")
```

### Test Case 4: Invalid Wiederanlaufwert Parameter Handling

**Purpose:** Verify that the wrapper procedure correctly handles an invalid `Wiederanlaufwert` format, logs an error, updates the job status to FAILED, and terminates with an error. This covers transformation correctness (type handling, error handling) and output parity for error scenarios.

**Setup:**
As above.

**Action:**
Call the `ausd_bp_ta_bpr_optionen_wrapper` procedure with an invalid `Wiederanlaufwert` string (e.g., 'abc').

```python
def test_invalid_wiederanlaufwert_parameter(bq_client):
    # Setup specific invalid parameter
    invalid_wiederanlaufwert_str = 'abc' # Invalid format

    # Action
    success, error_message = call_wrapper_procedure(bq_client, '2023-01-01', invalid_wiederanlaufwert_str)
    assert not success, "Procedure call should have failed but succeeded."

    job_run_id = get_latest_job_run_id(bq_client)
    assert job_run_id is not None, "No job status entry found."

    # Retrieve logs and status
    log_entries = get_job_log_entries(bq_client, job_run_id)
    status_entry = get_job_status_entry(bq_client, job_run_id)

    # Pass/Fail Criterion
    # 1. Job Status: Should be FAILED
    assert status_entry.status == 'FAILED'

    # 2. Error Message: Should contain specific error details
    assert "Invalid Wiederanlaufwert format" in error_message
    assert "INT_PARSE_ERROR" in error_message # Check for the SIGNAL SQLSTATE message

    # 3. Log Entries: Verify an ERROR log entry is present
    error_logs = [entry for entry in log_entries if entry.log_level == 'ERROR']
    assert len(error_logs) >= 1, "Expected at least one ERROR log entry."
    assert any(f"Invalid Wiederanlaufwert format provided: {invalid_wiederanlaufwert_str}. Expected integer. Exiting." in entry.message for entry in error_logs)
    assert any(entry.error_code == 'INT_PARSE_ERROR' for entry in error_logs)
    assert any(entry.error_arg == invalid_wiederanlaufwert_str for entry in error_logs)

    print(f"\nTest 'test_invalid_wiederanlaufwert_parameter' passed for job_run_id: {job_run_id}")
```

### Test Case 5: Core Logic Procedure Failure Handling

**Purpose:** Verify that if the `k_ausd_bp_ta_bpr_optionen` (core logic) procedure fails, the wrapper correctly catches the error, logs it, updates the job status to FAILED, and re-raises the error. This covers transformation correctness (error handling, control flow) and external system replacement (interaction with the core logic procedure).

**Setup:**
Temporarily replace the `k_ausd_bp_ta_bpr_optionen` procedure with a version that explicitly `SIGNAL`s an error. Restore the original placeholder after the test.

**Action:**
1.  Deploy a failing version of `k_ausd_bp_ta_bpr_optionen`.
2.  Call `ausd_bp_ta_bpr_optionen_wrapper`.
3.  Deploy the original placeholder version of `k_ausd_bp_ta_bpr_optionen`.

```python
def test_core_logic_procedure_failure(bq_client):
    # 1. Setup: Deploy a failing version of the core procedure
    failing_core_proc_sql = f"""
    CREATE OR REPLACE PROCEDURE `{CORE_PROC}`(
        IN p_job_run_id STRING, IN p_stichtag DATE, IN p_wiederanlaufwert INT64
    )
    BEGIN
        CALL `{LOG_MSG_PROC}`(
            p_job_run_id, 'k_ausd_bp_ta_bpr_optionen', 'INFO',
            'Simulating core logic failure.',
            p_stichtag, p_wiederanlaufwert, CAST(CURRENT_PROCESS_ID() AS STRING),
            NULL, NULL, NULL
        );
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core logic error!';
    END;
    """
    bq_client.query(failing_core_proc_sql).result()
    print(f"\nDeployed failing version of {CORE_PROC}.")

    # 2. Action: Call the wrapper
    success, error_message = call_wrapper_procedure(bq_client, '2023-03-01', '200')
    assert not success, "Procedure call should have failed but succeeded."

    job_run_id = get_latest_job_run_id(bq_client)
    assert job_run_id is not None, "No job status entry found."

    # Retrieve logs and status
    log_entries = get_job_log_entries(bq_client, job_run_id)
    status_entry = get_job_status_entry(bq_client, job_run_id)

    # Pass/Fail Criterion
    # 1. Job Status: Should be FAILED
    assert status_entry.status == 'FAILED'

    # 2. Error Message: Should contain the simulated error
    assert "Simulated core logic error!" in error_message
    assert "Job r_ausd_bp_ta_bpr_optionen.ksh failed: Simulated core logic error!" in error_message

    # 3. Log Entries: Verify ERROR log entries from both core and wrapper
    log_messages = [entry.message for entry in log_entries]
    assert any("Simulating core logic failure." in msg for msg in log_messages)
    assert any("Job r_ausd_bp_ta_bpr_optionen.ksh failed. Error: Simulated core logic error!" in msg for msg in log_messages)
    
    error_logs = [entry for entry in log_entries if entry.log_level == 'ERROR']
    assert len(error_logs) >= 1, "Expected at least one ERROR log entry."
    assert any("Simulated core logic error!" in entry.message for entry in error_logs)

    # 4. Teardown: Restore the original placeholder core procedure
    original_core_proc_sql = f"""
    CREATE OR REPLACE PROCEDURE `{CORE_PROC}`(
        IN p_job_run_id STRING, IN p_stichtag DATE, IN p_wiederanlaufwert INT64
    )
    BEGIN
        CALL `{LOG_MSG_PROC}`(
            p_job_run_id, 'k_ausd_bp_ta_bpr_optionen', 'INFO',
            'Core business logic procedure (placeholder) called successfully.',
            p_stichtag, p_wiederanlaufwert, CAST(CURRENT_PROCESS_ID() AS STRING),
            NULL, NULL, NULL
        );
    END;
    """
    bq_client.query(original_core_proc_sql).result()
    print(f"Restored original version of {CORE_PROC}.")
    print(f"\nTest 'test_core_logic_procedure_failure' passed for job_run_id: {job_run_id}")
```

### Test Case 6: Data Quality and Schema Assertions for Logging Tables

**Purpose:** Verify that the `job_log` and `job_status` tables exist, have the correct schema, and that critical non-nullable fields are always populated. This covers data quality and schema assertions.

**Setup:**
The `setup_bigquery_objects` fixture ensures tables are created. A successful run from previous tests will populate some data.

**Action:**
Query the schema of `job_log` and `job_status` tables. Perform a query to check for NULLs in NOT NULL columns.

```python
def test_logging_tables_schema_and_data_quality(bq_client):
    # Action: Get schemas
    job_log_schema = get_table_schema(bq_client, JOB_LOG_TABLE)
    job_status_schema = get_table_schema(bq_client, JOB_STATUS_TABLE)

    # Pass/Fail Criterion

    # 1. Schema Assertions for job_log
    expected_job_log_schema = {
        'job_run_id': 'STRING',
        'job_name': 'STRING',
        'log_timestamp': 'TIMESTAMP',
        'log_level': 'STRING',
        'message': 'STRING',
        'stichtag': 'DATE',
        'wiederanlaufwert': 'INT64',
        'process_id': 'STRING',
        'line_number': 'INT64',
        'error_code': 'STRING',
        'error_arg': 'STRING',
        'log_entry_id': 'STRING'
    }
    assert job_log_schema == expected_job_log_schema, "job_log table schema mismatch."

    # 2. Schema Assertions for job_status
    expected_job_status_schema = {
        'job_run_id': 'STRING',
        'job_name': 'STRING',
        'start_timestamp': 'TIMESTAMP',
        'end_timestamp': 'TIMESTAMP',
        'status': 'STRING',
        'stichtag': 'DATE',
        'wiederanlaufwert': 'INT64'
    }
    assert job_status_schema == expected_job_status_schema, "job_status table schema mismatch."

    # 3. Data Quality: Check for NULLs in NOT NULL columns for job_log
    null_check_log_query = f"""
    SELECT COUNT(*) FROM `{JOB_LOG_TABLE}`
    WHERE job_run_id IS NULL OR job_name IS NULL OR log_timestamp IS NULL OR log_level IS NULL OR message IS NULL OR log_entry_id IS NULL
    """
    null_log_count = bq_client.query(null_check_log_query).result().total_rows
    assert null_log_count == 0, f"Found {null_log_count} rows with NULLs in NOT NULL columns in {JOB_LOG_TABLE}."

    # 4. Data Quality: Check for NULLs in NOT NULL columns for job_status
    null_check_status_query = f"""
    SELECT COUNT(*) FROM `{JOB_STATUS_TABLE}`
    WHERE job_run_id IS NULL OR job_name IS NULL OR start_timestamp IS NULL OR status IS NULL
    """
    null_status_count = bq_client.query(null_check_status_query).result().total_rows
    assert null_status_count == 0, f"Found {null_status_count} rows with NULLs in NOT NULL columns in {JOB_STATUS_TABLE}."

    print(f"\nTest 'test_logging_tables_schema_and_data_quality' passed.")
```