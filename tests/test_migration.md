This document outlines a comprehensive set of migration validation tests for the `r_ausd_bp_ta_iccid_einzeln.ksh` KornShell script, migrated to a BigQuery Stored Procedure named `project.dataset.bereitstellung_basisprodukte_bert`. The tests cover output parity, transformation correctness, external system replacements, and data quality/schema assertions, as specified in the prompt.

The tests are designed to be run using `pytest` in a Python environment, leveraging the `google-cloud-bigquery` client library.

---

## Prerequisites

Before running the tests, ensure the following:

1.  **GCP Project and Dataset**: A Google Cloud Project and a BigQuery dataset exist. Replace `my_gcp_project` and `my_dwh_dataset` with your actual project and dataset IDs.
2.  **BigQuery API Enabled**: The BigQuery API is enabled for your GCP project.
3.  **Authentication**: Your environment is authenticated to GCP (e.g., `gcloud auth application-default login` or service account key).
4.  **Python Environment**: Python 3.x is installed, along with `pytest` and `google-cloud-bigquery`.
    ```bash
    pip install pytest google-cloud-bigquery
    ```
5.  **Mock Kernel Stored Procedure**: A mock BigQuery Stored Procedure `project.dataset.k_ausd_bp_ta_iccid_einzeln` and its control table `project.dataset.mock_kernel_control` are created. These are included in the `setup_bigquery_environment` fixture below.

---

## Test Setup (Python Pytest Fixture)

The following Python code sets up the BigQuery environment, including creating the necessary logging tables, the mock kernel stored procedure, and the main stored procedure under test.

```python
import pytest
from google.cloud import bigquery
import datetime
import time
import re

# --- Configuration ---
PROJECT_ID = "my_gcp_project"  # Replace with your GCP Project ID
DATASET_ID = "my_dwh_dataset"  # Replace with your BigQuery Dataset ID
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions for Test Setup/Teardown ---
def clear_log_tables():
    """Clears all log and mock tables."""
    tables = [
        f"{PROJECT_ID}.{DATASET_ID}.job_run_log",
        f"{PROJECT_ID}.{DATASET_ID}.job_error_log",
        f"{PROJECT_ID}.{DATASET_ID}.job_metadata_log",
        f"{PROJECT_ID}.{DATASET_ID}.job_status_log",
        f"{PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls",
    ]
    for table in tables:
        BQ_CLIENT.query(f"TRUNCATE TABLE {table}").result()

def set_mock_kernel_error_state(simulate_error: bool):
    """Sets the error simulation state for the mock kernel SP."""
    BQ_CLIENT.query(f"""
        MERGE INTO {PROJECT_ID}.{DATASET_ID}.mock_kernel_control T
        USING (SELECT 1 AS dummy) S
        ON TRUE
        WHEN MATCHED THEN UPDATE SET simulate_error = {simulate_error}
        WHEN NOT MATCHED THEN INSERT (simulate_error) VALUES ({simulate_error});
    """).result()

# --- Pytest Fixture for BigQuery Environment Setup ---
@pytest.fixture(scope="module", autouse=True)
def setup_bigquery_environment():
    """
    Ensures all necessary BigQuery tables and stored procedures are created
    or replaced before tests run.
    """
    ddl_statements = [
        # DDL for job_run_log
        f"""
        CREATE TABLE IF NOT EXISTS {PROJECT_ID}.{DATASET_ID}.job_run_log (
            run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job execution instance."),
            job_name STRING NOT NULL OPTIONS(description="Name of the job being executed."),
            start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution started."),
            end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job execution ended."),
            status STRING NOT NULL OPTIONS(description="Overall status of the job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED')."),
            message STRING OPTIONS(description="General message or brief summary of the job status.")
        );
        """,
        # DDL for job_error_log
        f"""
        CREATE TABLE IF NOT EXISTS {PROJECT_ID}.{DATASET_ID}.job_error_log (
            run_id STRING NOT NULL OPTIONS(description="Foreign key to job_run_log, linking to the specific job execution."),
            error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred."),
            error_code STRING OPTIONS(description="Error code (e.g., SQLSTATE)."),
            error_message STRING NOT NULL OPTIONS(description="Detailed error message."),
            stack_trace STRING OPTIONS(description="Optional stack trace or additional error context.")
        );
        """,
        # DDL for job_metadata_log
        f"""
        CREATE TABLE IF NOT EXISTS {PROJECT_ID}.{DATASET_ID}.job_metadata_log (
            run_id STRING NOT NULL OPTIONS(description="Foreign key to job_run_log, linking to the specific job execution."),
            meta_key STRING NOT NULL OPTIONS(description="Key for the metadata entry (e.g., 'log_file_name', 'stichtag_raw', 'wiederanlauf_wert_raw', 'stichtag_processed')."),
            meta_value STRING OPTIONS(description="Value of the metadata entry.")
        );
        """,
        # DDL for job_status_log
        f"""
        CREATE TABLE IF NOT EXISTS {PROJECT_ID}.{DATASET_ID}.job_status_log (
            run_id STRING NOT NULL OPTIONS(description="Foreign key to job_run_log, linking to the specific job execution."),
            status_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when this status update occurred."),
            status_message STRING NOT NULL OPTIONS(description="Brief message describing the job's status at this point (e.g., 'Job started', 'Parameters parsed', 'Kernel script called')."),
            detail STRING OPTIONS(description="Additional detail for the status message.")
        );
        """,
        # DDL for mock_kernel_sp_calls (to capture kernel SP invocations)
        f"""
        CREATE TABLE IF NOT EXISTS {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls (
            call_id STRING,
            run_id STRING,
            p_stichtag_passed STRING,
            p_wiederanlaufWert_passed INT64,
            call_timestamp TIMESTAMP
        );
        """,
        # DDL for mock_kernel_control (to simulate kernel SP errors)
        f"""
        CREATE TABLE IF NOT EXISTS {PROJECT_ID}.{DATASET_ID}.mock_kernel_control (
            simulate_error BOOL DEFAULT FALSE
        );
        """,
        # Mock kernel stored procedure (k_ausd_bp_ta_iccid_einzeln)
        f"""
        CREATE OR REPLACE PROCEDURE {PROJECT_ID}.{DATASET_ID}.k_ausd_bp_ta_iccid_einzeln(
            p_stichtag_in STRING,
            p_wiederanlaufWert_in INT64
        )
        BEGIN
            DECLARE v_call_id STRING DEFAULT GENERATE_UUID();
            DECLARE v_run_id STRING;

            -- Fetch the latest run_id from job_run_log for context
            SELECT run_id INTO v_run_id FROM {PROJECT_ID}.{DATASET_ID}.job_run_log ORDER BY start_timestamp DESC LIMIT 1;

            INSERT INTO {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls (call_id, run_id, p_stichtag_passed, p_wiederanlaufWert_passed, call_timestamp)
            VALUES (v_call_id, v_run_id, p_stichtag_in, p_wiederanlaufWert_in, CURRENT_TIMESTAMP());

            -- Simulate error if configured in mock_kernel_control
            IF (SELECT simulate_error FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_control LIMIT 1) THEN
                SIGNAL SQLSTATE '45001' SET MESSAGE_TEXT = 'Simulated error in k_ausd_bp_ta_iccid_einzeln';
            END IF;
        END;
        """,
        # Main stored procedure under test (bereitstellung_basisprodukte_bert)
        f"""
        CREATE OR REPLACE PROCEDURE {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert(
            p_stichtag STRING,           -- Input: Cutoff date in 'DDMMYYYY' format
            p_wiederanlaufWert INT64     -- Input: Restart value
        )
        BEGIN
            DECLARE v_run_id STRING;
            DECLARE v_job_name STRING DEFAULT 'bereitstellung_basisprodukte_bert';
            DECLARE v_stichtag STRING;
            DECLARE v_wiederanlaufWert INT64;
            DECLARE v_log_file_name STRING;
            DECLARE v_start_timestamp TIMESTAMP;
            DECLARE v_error_message STRING;
            DECLARE v_error_stack_trace STRING;

            SET v_start_timestamp = CURRENT_TIMESTAMP();
            SET v_run_id = GENERATE_UUID();

            -- Initialize job run log
            INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_run_log (run_id, job_name, start_timestamp, status, message)
            VALUES (v_run_id, v_job_name, v_start_timestamp, 'RUNNING', 'Job started');

            INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_status_log (run_id, status_timestamp, status_message)
            VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Job started and run_id generated');

            BEGIN
                -- Parameter defaulting and validation
                SET v_wiederanlaufWert = COALESCE(p_wiederanlaufWert, 0);
                SET v_stichtag = COALESCE(p_stichtag, FORMAT_DATE('%d%m%Y', CURRENT_DATE()));

                -- Log raw parameters
                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_metadata_log (run_id, meta_key, meta_value)
                VALUES
                    (v_run_id, 'p_stichtag_raw', p_stichtag),
                    (v_run_id, 'p_wiederanlaufWert_raw', CAST(p_wiederanlaufWert AS STRING));

                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_status_log (run_id, status_timestamp, status_message, detail)
                VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Parameters processed', CONCAT('Stichtag: ', v_stichtag, ', WiederanlaufWert: ', CAST(v_wiederanlaufWert AS STRING)));

                -- Generate simulated log file name
                SET v_log_file_name = CONCAT(
                    'log_',
                    REPLACE(v_job_name, '.', '_'),
                    '_',
                    v_stichtag,
                    '_',
                    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()),
                    '.log'
                );

                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_metadata_log (run_id, meta_key, meta_value)
                VALUES
                    (v_run_id, 'stichtag_processed', v_stichtag),
                    (v_run_id, 'wiederanlaufWert_processed', CAST(v_wiederanlaufWert AS STRING)),
                    (v_run_id, 'log_file_name', v_log_file_name);

                -- Validate Stichtag
                IF v_stichtag IS NULL OR v_stichtag = '' THEN
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Stichtag parameter is required and cannot be empty.';
                END IF;

                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_status_log (run_id, status_timestamp, status_message)
                VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Stichtag validated');

                -- Call the kernel stored procedure
                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_status_log (run_id, status_timestamp, status_message, detail)
                VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Calling kernel stored procedure', 'project.dataset.k_ausd_bp_ta_iccid_einzeln');

                CALL {PROJECT_ID}.{DATASET_ID}.k_ausd_bp_ta_iccid_einzeln(v_stichtag, v_wiederanlaufWert);

                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_status_log (run_id, status_timestamp, status_message)
                VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Kernel stored procedure completed successfully');

                -- Update job run log for success
                UPDATE {PROJECT_ID}.{DATASET_ID}.job_run_log
                SET
                    end_timestamp = CURRENT_TIMESTAMP(),
                    status = 'SUCCESS',
                    message = 'Job completed successfully'
                WHERE run_id = v_run_id;

                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_status_log (run_id, status_timestamp, status_message)
                VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Job completed successfully');

            EXCEPTION WHEN ERROR THEN
                SET v_error_message = @@error.message;
                SET v_error_stack_trace = @@error.stack_trace;

                -- Log the error
                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_error_log (run_id, error_timestamp, error_code, error_message, stack_trace)
                VALUES (v_run_id, CURRENT_TIMESTAMP(), @@error.code, v_error_message, v_error_stack_trace);

                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_status_log (run_id, status_timestamp, status_message, detail)
                VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Job failed', v_error_message);

                -- Update job run log for failure
                UPDATE {PROJECT_ID}.{DATASET_ID}.job_run_log
                SET
                    end_timestamp = CURRENT_TIMESTAMP(),
                    status = 'FAILED',
                    message = CONCAT('Job failed: ', v_error_message)
                WHERE run_id = v_run_id;

                -- Re-raise the error to indicate failure to the caller
                RAISE;
            END;
        END;
        """
    ]
    for ddl in ddl_statements:
        BQ_CLIENT.query(ddl).result()
    
    # Initial setup for mock control and clear logs before tests
    set_mock_kernel_error_state(False)
    clear_log_tables()
    yield
    # Optional: Teardown after all tests in the module
    # clear_log_tables()
```

---

## Migration Validation Tests

### Test Case 1: Happy Path - All Parameters Provided

*   **Purpose**: Verify the wrapper executes successfully when both `p_stichtag` and `p_wiederanlaufWert` are provided, and logs all events correctly.
*   **Setup**:
    *   Clear all logging tables.
    *   Ensure `mock_kernel_control.simulate_error` is `FALSE`.
*   **Action**: Call `project.dataset.bereitstellung_basisprodukte_bert('01012023', 100)`.
*   **Pass/Fail Criterion**:
    *   `job_run_log` contains exactly one entry with `status = 'SUCCESS'`.
    *   `job_metadata_log` contains entries for `p_stichtag_raw='01012023'`, `p_wiederanlaufWert_raw='100'`, `stichtag_processed='01012023'`, `wiederanlaufWert_processed='100'`, and `log_file_name` (matching the expected pattern).
    *   `job_status_log` contains a sequence of 6 distinct status messages, ending with 'Job completed successfully'.
    *   `mock_kernel_sp_calls` contains exactly one entry with `p_stichtag_passed = '01012023'` and `p_wiederanlaufWert_passed = 100`.
    *   `job_error_log` is empty.

```python
def test_happy_path_all_parameters_provided():
    clear_log_tables()
    set_mock_kernel_error_state(False)

    BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert('01012023', 100)").result()

    # Assert job_run_log
    run_log = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_run_log").result())
    assert len(run_log) == 1
    assert run_log[0].status == 'SUCCESS'
    assert run_log[0].job_name == 'bereitstellung_basisprodukte_bert'
    assert run_log[0].end_timestamp is not None

    run_id = run_log[0].run_id

    # Assert job_metadata_log
    meta_log = list(BQ_CLIENT.query(f"SELECT meta_key, meta_value FROM {PROJECT_ID}.{DATASET_ID}.job_metadata_log WHERE run_id = '{run_id}' ORDER BY meta_key").result())
    meta_dict = {row.meta_key: row.meta_value for row in meta_log}
    assert meta_dict.get('p_stichtag_raw') == '01012023'
    assert meta_dict.get('p_wiederanlaufWert_raw') == '100'
    assert meta_dict.get('stichtag_processed') == '01012023'
    assert meta_dict.get('wiederanlaufWert_processed') == '100'
    assert re.match(r"log_bereitstellung_basisprodukte_bert_01012023_\d{14}\.log", meta_dict.get('log_file_name'))

    # Assert job_status_log
    status_log = list(BQ_CLIENT.query(f"SELECT status_message FROM {PROJECT_ID}.{DATASET_ID}.job_status_log WHERE run_id = '{run_id}' ORDER BY status_timestamp").result())
    expected_statuses = [
        'Job started and run_id generated',
        'Parameters processed',
        'Stichtag validated',
        'Calling kernel stored procedure',
        'Kernel stored procedure completed successfully',
        'Job completed successfully'
    ]
    assert [row.status_message for row in status_log] == expected_statuses

    # Assert mock_kernel_sp_calls
    kernel_calls = list(BQ_CLIENT.query(f"SELECT p_stichtag_passed, p_wiederanlaufWert_passed FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls WHERE run_id = '{run_id}'").result())
    assert len(kernel_calls) == 1
    assert kernel_calls[0].p_stichtag_passed == '01012023'
    assert kernel_calls[0].p_wiederanlaufWert_passed == 100

    # Assert job_error_log is empty
    error_log = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_error_log WHERE run_id = '{run_id}'").result())
    assert len(error_log) == 0
```

### Test Case 2: Defaulting `p_wiederanlaufWert`

*   **Purpose**: Verify `p_wiederanlaufWert` defaults to `0` when `NULL` is provided.
*   **Setup**:
    *   Clear all logging tables.
    *   Ensure `mock_kernel_control.simulate_error` is `FALSE`.
*   **Action**: Call `project.dataset.bereitstellung_basisprodukte_bert('01012023', NULL)`.
*   **Pass/Fail Criterion**:
    *   `job_run_log` has one entry with `status = 'SUCCESS'`.
    *   `job_metadata_log` shows `p_wiederanlaufWert_raw` as `NULL` and `wiederanlaufWert_processed` as `'0'`.
    *   `mock_kernel_sp_calls` has one entry with `p_stichtag_passed = '01012023'` and `p_wiederanlaufWert_passed = 0`.

```python
def test_defaulting_wiederanlaufwert():
    clear_log_tables()
    set_mock_kernel_error_state(False)

    BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert('01012023', NULL)").result()

    run_log = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_run_log").result())
    assert len(run_log) == 1
    assert run_log[0].status == 'SUCCESS'
    run_id = run_log[0].run_id

    meta_log = list(BQ_CLIENT.query(f"SELECT meta_key, meta_value FROM {PROJECT_ID}.{DATASET_ID}.job_metadata_log WHERE run_id = '{run_id}' ORDER BY meta_key").result())
    meta_dict = {row.meta_key: row.meta_value for row in meta_log}
    assert meta_dict.get('p_wiederanlaufWert_raw') is None # BQ stores NULL as None in Python client
    assert meta_dict.get('wiederanlaufWert_processed') == '0'

    kernel_calls = list(BQ_CLIENT.query(f"SELECT p_stichtag_passed, p_wiederanlaufWert_passed FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls WHERE run_id = '{run_id}'").result())
    assert len(kernel_calls) == 1
    assert kernel_calls[0].p_stichtag_passed == '01012023'
    assert kernel_calls[0].p_wiederanlaufWert_passed == 0
```

### Test Case 3: Defaulting `p_stichtag`

*   **Purpose**: Verify `p_stichtag` defaults to `CURRENT_DATE()` in `DDMMYYYY` format when `NULL` is provided.
*   **Setup**:
    *   Clear all logging tables.
    *   Ensure `mock_kernel_control.simulate_error` is `FALSE`.
*   **Action**: Call `project.dataset.bereitstellung_basisprodukte_bert(NULL, 100)`.
*   **Pass/Fail Criterion**:
    *   `job_run_log` has one entry with `status = 'SUCCESS'`.
    *   `job_metadata_log` shows `p_stichtag_raw` as `NULL` and `stichtag_processed` as `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    *   `mock_kernel_sp_calls` has one entry with `p_stichtag_passed = FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `p_wiederanlaufWert_passed = 100`.

```python
def test_defaulting_stichtag():
    clear_log_tables()
    set_mock_kernel_error_state(False)
    
    expected_stichtag = datetime.datetime.now().strftime('%d%m%Y')
    BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert(NULL, 100)").result()

    run_log = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_run_log").result())
    assert len(run_log) == 1
    assert run_log[0].status == 'SUCCESS'
    run_id = run_log[0].run_id

    meta_log = list(BQ_CLIENT.query(f"SELECT meta_key, meta_value FROM {PROJECT_ID}.{DATASET_ID}.job_metadata_log WHERE run_id = '{run_id}' ORDER BY meta_key").result())
    meta_dict = {row.meta_key: row.meta_value for row in meta_log}
    assert meta_dict.get('p_stichtag_raw') is None
    assert meta_dict.get('stichtag_processed') == expected_stichtag

    kernel_calls = list(BQ_CLIENT.query(f"SELECT p_stichtag_passed, p_wiederanlaufWert_passed FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls WHERE run_id = '{run_id}'").result())
    assert len(kernel_calls) == 1
    assert kernel_calls[0].p_stichtag_passed == expected_stichtag
    assert kernel_calls[0].p_wiederanlaufWert_passed == 100
```

### Test Case 4: Defaulting Both Parameters

*   **Purpose**: Verify both parameters default correctly when neither is provided.
*   **Setup**:
    *   Clear all logging tables.
    *   Ensure `mock_kernel_control.simulate_error` is `FALSE`.
*   **Action**: Call `project.dataset.bereitstellung_basisprodukte_bert(NULL, NULL)`.
*   **Pass/Fail Criterion**:
    *   `job_run_log` has one entry with `status = 'SUCCESS'`.
    *   `job_metadata_log` shows `p_stichtag_raw` as `NULL`, `stichtag_processed` as `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `p_wiederanlaufWert_raw` as `NULL`, and `wiederanlaufWert_processed` as `'0'`.
    *   `mock_kernel_sp_calls` has one entry with `p_stichtag_passed = FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `p_wiederanlaufWert_passed = 0`.

```python
def test_defaulting_both_parameters():
    clear_log_tables()
    set_mock_kernel_error_state(False)
    
    expected_stichtag = datetime.datetime.now().strftime('%d%m%Y')
    BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert(NULL, NULL)").result()

    run_log = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_run_log").result())
    assert len(run_log) == 1
    assert run_log[0].status == 'SUCCESS'
    run_id = run_log[0].run_id

    meta_log = list(BQ_CLIENT.query(f"SELECT meta_key, meta_value FROM {PROJECT_ID}.{DATASET_ID}.job_metadata_log WHERE run_id = '{run_id}' ORDER BY meta_key").result())
    meta_dict = {row.meta_key: row.meta_value for row in meta_log}
    assert meta_dict.get('p_stichtag_raw') is None
    assert meta_dict.get('stichtag_processed') == expected_stichtag
    assert meta_dict.get('p_wiederanlaufWert_raw') is None
    assert meta_dict.get('wiederanlaufWert_processed') == '0'

    kernel_calls = list(BQ_CLIENT.query(f"SELECT p_stichtag_passed, p_wiederanlaufWert_passed FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls WHERE run_id = '{run_id}'").result())
    assert len(kernel_calls) == 1
    assert kernel_calls[0].p_stichtag_passed == expected_stichtag
    assert kernel_calls[0].p_wiederanlaufWert_passed == 0
```

### Test Case 5: `Stichtag` Validation Failure (Empty String)

*   **Purpose**: Verify the `Stichtag` validation correctly signals an error if it's an empty string after defaulting.
*   **Setup**:
    *   Clear all logging tables.
    *   Ensure `mock_kernel_control.simulate_error` is `FALSE`.
*   **Action**: Call `project.dataset.bereitstellung_basisprodukte_bert('', 100)`.
*   **Pass/Fail Criterion**:
    *   The call raises a `google.api_core.exceptions.BadRequest` error with `MESSAGE_TEXT` containing 'Stichtag parameter is required and cannot be empty.'.
    *   `job_run_log` has one entry with `status = 'FAILED'`.
    *   `job_error_log` has one entry with `error_message` containing 'Stichtag parameter is required and cannot be empty.'.
    *   `mock_kernel_sp_calls` is empty (kernel SP should not be called).

```python
def test_stichtag_validation_failure_empty_string():
    clear_log_tables()
    set_mock_kernel_error_state(False)

    with pytest.raises(Exception) as excinfo: # Expecting BadRequest from BQ
        BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert('', 100)").result()
    
    assert "Stichtag parameter is required and cannot be empty." in str(excinfo.value)

    run_log = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_run_log").result())
    assert len(run_log) == 1
    assert run_log[0].status == 'FAILED'
    run_id = run_log[0].run_id

    error_log = list(BQ_CLIENT.query(f"SELECT error_message FROM {PROJECT_ID}.{DATASET_ID}.job_error_log WHERE run_id = '{run_id}'").result())
    assert len(error_log) == 1
    assert "Stichtag parameter is required and cannot be empty." in error_log[0].error_message

    kernel_calls = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls WHERE run_id = '{run_id}'").result())
    assert len(kernel_calls) == 0 # Kernel SP should not be called
```

### Test Case 6: Kernel Stored Procedure Failure

*   **Purpose**: Verify the wrapper correctly handles and logs errors from the invoked kernel SP.
*   **Setup**:
    *   Clear all logging tables.
    *   Set `mock_kernel_control.simulate_error` to `TRUE`.
*   **Action**: Call `project.dataset.bereitstellung_basisprodukte_bert('01012023', 100)`.
*   **Pass/Fail Criterion**:
    *   The call raises a `google.api_core.exceptions.BadRequest` error with `MESSAGE_TEXT` containing 'Simulated error in k_ausd_bp_ta_iccid_einzeln'.
    *   `job_run_log` has one entry with `status = 'FAILED'`.
    *   `job_error_log` has one entry with `error_message` containing 'Simulated error in k_ausd_bp_ta_iccid_einzeln'.
    *   `mock_kernel_sp_calls` has one entry, indicating the kernel SP was called before it failed.
    *   `job_status_log` shows 'Calling kernel stored procedure' followed by 'Job failed'.

```python
def test_kernel_stored_procedure_failure():
    clear_log_tables()
    set_mock_kernel_error_state(True)

    with pytest.raises(Exception) as excinfo: # Expecting BadRequest from BQ
        BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert('01012023', 100)").result()
    
    assert "Simulated error in k_ausd_bp_ta_iccid_einzeln" in str(excinfo.value)

    run_log = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_run_log").result())
    assert len(run_log) == 1
    assert run_log[0].status == 'FAILED'
    run_id = run_log[0].run_id

    error_log = list(BQ_CLIENT.query(f"SELECT error_message FROM {PROJECT_ID}.{DATASET_ID}.job_error_log WHERE run_id = '{run_id}'").result())
    assert len(error_log) == 1
    assert "Simulated error in k_ausd_bp_ta_iccid_einzeln" in error_log[0].error_message

    kernel_calls = list(BQ_CLIENT.query(f"SELECT p_stichtag_passed, p_wiederanlaufWert_passed FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls WHERE run_id = '{run_id}'").result())
    assert len(kernel_calls) == 1 # Kernel SP was called before it failed
    assert kernel_calls[0].p_stichtag_passed == '01012023'
    assert kernel_calls[0].p_wiederanlaufWert_passed == 100

    status_log = list(BQ_CLIENT.query(f"SELECT status_message FROM {PROJECT_ID}.{DATASET_ID}.job_status_log WHERE run_id = '{run_id}' ORDER BY status_timestamp").result())
    assert 'Calling kernel stored procedure' in [row.status_message for row in status_log]
    assert 'Job failed' in [row.status_message for row in status_log]
```

### Test Case 7: Logging Table Schema and Data Types

*   **Purpose**: Verify the DDLs for the logging tables are correct and match expectations regarding column names and data types.
*   **Setup**: N/A (assumes DDLs are applied by the fixture).
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA.COLUMNS`.
*   **Pass/Fail Criterion**:
    *   Each logging table (`job_run_log`, `job_error_log`, `job_metadata_log`, `job_status_log`) has the expected columns with the correct data types.

```python
def test_logging_table_schema_and_data_types():
    expected_schemas = {
        "job_run_log": {
            "run_id": "STRING", "job_name": "STRING", "start_timestamp": "TIMESTAMP",
            "end_timestamp": "TIMESTAMP", "status": "STRING", "message": "STRING"
        },
        "job_error_log": {
            "run_id": "STRING", "error_timestamp": "TIMESTAMP", "error_code": "STRING",
            "error_message": "STRING", "stack_trace": "STRING"
        },
        "job_metadata_log": {
            "run_id": "STRING", "meta_key": "STRING", "meta_value": "STRING"
        },
        "job_status_log": {
            "run_id": "STRING", "status_timestamp": "TIMESTAMP", "status_message": "STRING", "detail": "STRING"
        }
    }

    for table_name, expected_cols in expected_schemas.items():
        query = f"""
            SELECT column_name, data_type
            FROM {PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{table_name}'
        """
        schema_info = list(BQ_CLIENT.query(query).result())
        actual_cols = {row.column_name: row.data_type for row in schema_info}

        assert len(actual_cols) == len(expected_cols), f"Mismatch in column count for {table_name}"
        for col_name, data_type in expected_cols.items():
            assert col_name in actual_cols, f"Missing column {col_name} in {table_name}"
            assert actual_cols[col_name] == data_type, f"Data type mismatch for {col_name} in {table_name}: Expected {data_type}, Got {actual_cols[col_name]}"
```

### Test Case 8: Log File Name Generation (Metadata)

*   **Purpose**: Verify the `log_file_name` metadata is generated correctly, following the pattern `log_bereitstellung_basisprodukte_bert_DDMMYYYY_YYYYMMDDHHMMSS.log`.
*   **Setup**:
    *   Clear all logging tables.
    *   Ensure `mock_kernel_control.simulate_error` is `FALSE`.
*   **Action**: Call `project.dataset.bereitstellung_basisprodukte_bert('01012023', 100)`.
*   **Pass/Fail Criterion**:
    *   `job_metadata_log` contains an entry where `meta_key = 'log_file_name'`.
    *   The `meta_value` for `log_file_name` matches the regex pattern `log_bereitstellung_basisprodukte_bert_01012023_\d{14}\.log`.

```python
def test_log_file_name_generation():
    clear_log_tables()
    set_mock_kernel_error_state(False)

    BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert('01012023', 100)").result()

    run_log = list(BQ_CLIENT.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_run_log").result())
    run_id = run_log[0].run_id

    meta_log = list(BQ_CLIENT.query(f"SELECT meta_key, meta_value FROM {PROJECT_ID}.{DATASET_ID}.job_metadata_log WHERE run_id = '{run_id}' AND meta_key = 'log_file_name'").result())
    assert len(meta_log) == 1
    log_file_name = meta_log[0].meta_value
    
    # Regex to match the expected pattern: log_jobname_stichtag_timestamp.log
    # Example: log_bereitstellung_basisprodukte_bert_01012023_20231027153000.log
    expected_pattern = r"log_bereitstellung_basisprodukte_bert_01012023_\d{14}\.log"
    assert re.match(expected_pattern, log_file_name) is not None
```

### Test Case 9: Multiple Runs - Isolation of Logs

*   **Purpose**: Ensure that each job run creates its own distinct set of log entries, identifiable by a unique `run_id`, demonstrating proper isolation and tracking.
*   **Setup**:
    *   Clear all logging tables.
    *   Ensure `mock_kernel_control.simulate_error` is `FALSE`.
*   **Action**:
    1.  Call `project.dataset.bereitstellung_basisprodukte_bert('01012023', 100)`.
    2.  Call `project.dataset.bereitstellung_basisprodukte_bert('02022023', 200)`.
*   **Pass/Fail Criterion**:
    *   `job_run_log` has two distinct `run_id` entries, both with `status = 'SUCCESS'`.
    *   Total entries in `job_run_log` = 2.
    *   Total entries in `mock_kernel_sp_calls` = 2.
    *   Total entries in `job_metadata_log` = 10 (5 metadata entries per run).
    *   Total entries in `job_status_log` = 12 (6 status entries per run).
    *   For each `run_id`, all associated `job_metadata_log`, `job_status_log`, and `mock_kernel_sp_calls` entries correctly correspond to that `run_id` and its parameters.

```python
def test_multiple_runs_isolation_of_logs():
    clear_log_tables()
    set_mock_kernel_error_state(False)

    # First run
    BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert('01012023', 100)").result()
    time.sleep(1) # Ensure timestamps are distinct for run_id retrieval
    # Second run
    BQ_CLIENT.query(f"CALL {PROJECT_ID}.{DATASET_ID}.bereitstellung_basisprodukte_bert('02022023', 200)").result()

    # Verify total counts
    assert list(BQ_CLIENT.query(f"SELECT COUNT(1) FROM {PROJECT_ID}.{DATASET_ID}.job_run_log").result())[0][0] == 2
    assert list(BQ_CLIENT.query(f"SELECT COUNT(1) FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls").result())[0][0] == 2
    assert list(BQ_CLIENT.query(f"SELECT COUNT(1) FROM {PROJECT_ID}.{DATASET_ID}.job_metadata_log").result())[0][0] == 10 # 5 entries per run
    assert list(BQ_CLIENT.query(f"SELECT COUNT(1) FROM {PROJECT_ID}.{DATASET_ID}.job_status_log").result())[0][0] == 12 # 6 entries per run

    # Verify distinct run_ids and their associated data
    run_logs = list(BQ_CLIENT.query(f"SELECT run_id, status FROM {PROJECT_ID}.{DATASET_ID}.job_run_log ORDER BY start_timestamp").result())
    assert len(run_logs) == 2
    assert run_logs[0].status == 'SUCCESS'
    assert run_logs[1].status == 'SUCCESS'
    assert run_logs[0].run_id != run_logs[1].run_id

    run_id_1 = run_logs[0].run_id
    run_id_2 = run_logs[1].run_id

    # Check data for run_id_1
    meta_log_1 = list(BQ_CLIENT.query(f"SELECT meta_key, meta_value FROM {PROJECT_ID}.{DATASET_ID}.job_metadata_log WHERE run_id = '{run_id_1}' ORDER BY meta_key").result())
    meta_dict_1 = {row.meta_key: row.meta_value for row in meta_log_1}
    assert meta_dict_1.get('stichtag_processed') == '01012023'
    assert meta_dict_1.get('wiederanlaufWert_processed') == '100'
    assert len(meta_log_1) == 5

    kernel_calls_1 = list(BQ_CLIENT.query(f"SELECT p_stichtag_passed, p_wiederanlaufWert_passed FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls WHERE run_id = '{run_id_1}'").result())
    assert len(kernel_calls_1) == 1
    assert kernel_calls_1[0].p_stichtag_passed == '01012023'
    assert kernel_calls_1[0].p_wiederanlaufWert_passed == 100

    # Check data for run_id_2
    meta_log_2 = list(BQ_CLIENT.query(f"SELECT meta_key, meta_value FROM {PROJECT_ID}.{DATASET_ID}.job_metadata_log WHERE run_id = '{run_id_2}' ORDER BY meta_key").result())
    meta_dict_2 = {row.meta_key: row.meta_value for row in meta_log_2}
    assert meta_dict_2.get('stichtag_processed') == '02022023'
    assert meta_dict_2.get('wiederanlaufWert_processed') == '200'
    assert len(meta_log_2) == 5

    kernel_calls_2 = list(BQ_CLIENT.query(f"SELECT p_stichtag_passed, p_wiederanlaufWert_passed FROM {PROJECT_ID}.{DATASET_ID}.mock_kernel_sp_calls WHERE run_id = '{run_id_2}'").result())
    assert len(kernel_calls_2) == 1
    assert kernel_calls_2[0].p_stichtag_passed == '02022023'
    assert kernel_calls_2[0].p_wiederanlaufWert_passed == 200
```