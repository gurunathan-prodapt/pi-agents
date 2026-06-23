The migration of `r_ausd_v_ta_inv_def.ksh` to `project.dataset.sp_vertragsdatenabgleich` involves re-platforming an orchestration script from KornShell to Google BigQuery Stored Procedures. The tests below focus on ensuring the BigQuery stored procedure replicates the behavioral aspects of the original script, particularly around parameter handling, logging, error management, and the invocation of the core logic.

**Assumptions:**
*   The BigQuery project and dataset (`project.dataset`) are correctly configured.
*   The `job_audit` and `job_error_log` tables have been created as per the design document.
*   A mock stored procedure `project.dataset.sp_k_ausd_v_ta_inv_def` exists to simulate the core business logic, allowing for testing of success and failure scenarios.

**General Setup for Pytest (Python)**

The following Python setup uses `pytest` and the `google-cloud-bigquery` library to execute tests. Replace `your-gcp-project-id` with your actual GCP project ID.

```python
import pytest
from google.cloud import bigquery
import uuid
import time
import json

# Configuration
PROJECT_ID = "your-gcp-project-id" # <<< REPLACE WITH YOUR GCP PROJECT ID
DATASET_ID = "dataset"
SP_NAME = "sp_vertragsdatenabgleich"
SP_CORE_NAME = "sp_k_ausd_v_ta_inv_def"
JOB_AUDIT_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.job_audit`"
JOB_ERROR_LOG_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.job_error_log`"

client = bigquery.Client(project=PROJECT_ID)

def execute_query(query):
    """Executes a BigQuery SQL query and returns the results."""
    print(f"\nExecuting SQL:\n{query}\n")
    query_job = client.query(query)
    return query_job.result()

def call_sp(sp_name, *args, **kwargs):
    """Constructs and calls a BigQuery stored procedure."""
    arg_strings = []
    for arg in args:
        if isinstance(arg, str):
            arg_strings.append(f"'{arg}'")
        elif isinstance(arg, bool):
            arg_strings.append(str(arg).upper())
        elif arg is None:
            arg_strings.append("NULL")
        else:
            arg_strings.append(str(arg))

    for k, v in kwargs.items():
        if isinstance(v, str):
            arg_strings.append(f"{k} => '{v}'")
        elif isinstance(v, bool):
            arg_strings.append(f"{k} => {str(v).upper()}")
        elif v is None:
            arg_strings.append(f"{k} => NULL")
        else:
            arg_strings.append(f"{k} => {str(v)}")

    call_statement = f"CALL `{PROJECT_ID}.{DATASET_ID}.{sp_name}`({', '.join(arg_strings)})"
    try:
        execute_query(call_statement)
        return True, None
    except Exception as e:
        return False, str(e)

@pytest.fixture(scope="module", autouse=True)
def setup_mock_core_sp():
    """Ensures the mock core SP exists for testing."""
    mock_sp_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.{SP_CORE_NAME}`(
        p_job_kennung STRING,
        p_entry_number STRING,
        p_source_param STRING,
        p_log_param STRING,
        p_simulate_error BOOL DEFAULT FALSE
    )
    BEGIN
        IF p_simulate_error THEN
            RAISE USING MESSAGE 'Simulated error in core logic: Data reconciliation failed.';
        END IF;
        -- Simulate some work and return a message
        SELECT 'Core logic executed successfully.' AS core_logic_status;
    END;
    """
    execute_query(mock_sp_sql)
    print(f"Mock SP `{SP_CORE_NAME}` created/replaced.")
    yield
    # Optional: Clean up mock SP after all tests
    # execute_query(f"DROP PROCEDURE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.{SP_CORE_NAME}`")

@pytest.fixture(autouse=True)
def cleanup_audit_tables():
    """Cleans up audit tables before each test."""
    execute_query(f"TRUNCATE TABLE {JOB_AUDIT_TABLE}")
    execute_query(f"TRUNCATE TABLE {JOB_ERROR_LOG_TABLE}")
    print(f"Cleaned up {JOB_AUDIT_TABLE} and {JOB_ERROR_LOG_TABLE}.")
    yield

```

---

## Test Case 1: Help Message Display

**Purpose:**
To verify that calling the stored procedure with `p_help = TRUE` (equivalent to `-h` in the legacy script) displays the help message and exits without performing any job execution or logging.

**Setup:**
*   Ensure `project.dataset.sp_vertragsdatenabgleich` is deployed.
*   Audit tables (`job_audit`, `job_error_log`) are empty.

**Action:**
Call the BigQuery stored procedure `sp_vertragsdatenabgleich` with `p_help => TRUE`.

```python
def test_help_message_display():
    """
    Tests that calling the SP with p_help=TRUE displays the help message
    and does not log any job activity.
    """
    success, error_message = call_sp(SP_NAME, p_help=True)

    # The SP should execute successfully (i.e., not raise a BigQuery error)
    # as it's designed to return the help message and exit gracefully.
    assert success is True, f"SP call failed unexpectedly: {error_message}"

    # Verify no entries in job_audit table
    audit_rows = list(execute_query(f"SELECT * FROM {JOB_AUDIT_TABLE}"))
    assert len(audit_rows) == 0, "Job audit table should be empty when help is requested."

    # Verify no entries in job_error_log table
    error_rows = list(execute_query(f"SELECT * FROM {JOB_ERROR_LOG_TABLE}"))
    assert len(error_rows) == 0, "Job error log table should be empty when help is requested."

    # Output parity check: The actual output of a BigQuery SP call is usually
    # captured by the client. For this test, we primarily check the side effects (logging).
    # A more robust check would involve capturing the SELECT statement output from the SP.
    # For now, we assume the SP's internal SELECT for help message is correct.
```

**Pass/Fail Criterion:**
*   The `CALL` statement for `sp_vertragsdatenabgleich(p_help => TRUE)` completes without raising a BigQuery error.
*   The `job_audit` table contains zero rows.
*   The `job_error_log` table contains zero rows.

---

## Test Case 2: Successful Job Execution

**Purpose:**
To verify the happy path where the job executes successfully, including correct logging of start/end times, status, and invocation of the core logic. This covers output parity for job details and transformation correctness for logging.

**Setup:**
*   Ensure `project.dataset.sp_vertragsdatenabgleich` is deployed.
*   Ensure the mock `project.dataset.sp_k_ausd_v_ta_inv_def` is deployed and configured to succeed.
*   Audit tables (`job_audit`, `job_error_log`) are empty.

**Action:**
Call the BigQuery stored procedure `sp_vertragsdatenabgleich` without any parameters (or with default `NULL` parameters).

```python
def test_successful_job_execution():
    """
    Tests the successful execution path of the SP, verifying audit logs.
    """
    success, error_message = call_sp(SP_NAME)

    assert success is True, f"SP call failed unexpectedly: {error_message}"

    # Verify one entry in job_audit table
    audit_rows = list(execute_query(f"SELECT * FROM {JOB_AUDIT_TABLE}"))
    assert len(audit_rows) == 1, "Expected exactly one entry in job_audit table for a successful run."

    audit_entry = audit_rows[0]
    assert audit_entry.job_key == "BERT_V_TA_INV_DEF", "Job key mismatch in audit log."
    assert audit_entry.status == "SUCCESS", "Job status should be 'SUCCESS'."
    assert audit_entry.message == "Job completed successfully.", "Success message mismatch."
    assert audit_entry.start_time is not None, "Start time should be logged."
    assert audit_entry.end_time is not None, "End time should be logged."
    assert audit_entry.end_time >= audit_entry.start_time, "End time should be after start time."
    assert audit_entry.entry_number is not None, "Entry number should be generated."
    assert audit_entry.stichtag_info == time.strftime('%Y-%m-%d'), "Stichtag info should be current date."
    assert json.loads(audit_entry.parameters) == {"p_source_param": None, "p_log_param": None}, "Default parameters should be logged as NULL."

    # Verify no entries in job_error_log table
    error_rows = list(execute_query(f"SELECT * FROM {JOB_ERROR_LOG_TABLE}"))
    assert len(error_rows) == 0, "Job error log table should be empty for a successful run."

    # Output parity: The original script prints job details and a success message.
    # The BigQuery SP uses SELECT statements for this.
    # The `call_sp` function doesn't capture SELECT output directly, but the presence
    # of the correct audit log entry confirms the internal logic.
```

**Pass/Fail Criterion:**
*   The `CALL` statement for `sp_vertragsdatenabgleich()` completes successfully.
*   The `job_audit` table contains exactly one row with `status = 'SUCCESS'`, valid `start_time` and `end_time`, and correct `job_key`, `entry_number`, `stichtag_info`, and `parameters` (as JSON `{"p_source_param": null, "p_log_param": null}`).
*   The `job_error_log` table contains zero rows.

---

## Test Case 3: Job Execution with Parameters

**Purpose:**
To verify that command-line parameters (`-s`, `-l` in legacy) are correctly passed to the BigQuery stored procedure (`p_source_param`, `p_log_param`) and are accurately logged in the `job_audit` table. This covers transformation correctness for parameter handling.

**Setup:**
*   Ensure `project.dataset.sp_vertragsdatenabgleich` is deployed.
*   Ensure the mock `project.dataset.sp_k_ausd_v_ta_inv_def` is deployed and configured to succeed.
*   Audit tables (`job_audit`, `job_error_log`) are empty.

**Action:**
Call the BigQuery stored procedure `sp_vertragsdatenabgleich` with specific values for `p_source_param` and `p_log_param`.

```python
def test_job_execution_with_parameters():
    """
    Tests that parameters passed to the SP are correctly logged in the audit table.
    """
    test_source_param = "test_source_value"
    test_log_param = "test_log_value"

    success, error_message = call_sp(SP_NAME, p_source_param=test_source_param, p_log_param=test_log_param)

    assert success is True, f"SP call failed unexpectedly: {error_message}"

    # Verify one entry in job_audit table
    audit_rows = list(execute_query(f"SELECT * FROM {JOB_AUDIT_TABLE}"))
    assert len(audit_rows) == 1, "Expected exactly one entry in job_audit table."

    audit_entry = audit_rows[0]
    assert audit_entry.status == "SUCCESS", "Job status should be 'SUCCESS'."
    
    # Verify parameters are correctly stored in JSON
    logged_params = json.loads(audit_entry.parameters)
    assert logged_params.get("p_source_param") == test_source_param, "Source parameter mismatch in audit log."
    assert logged_params.get("p_log_param") == test_log_param, "Log parameter mismatch in audit log."

    # Verify no entries in job_error_log table
    error_rows = list(execute_query(f"SELECT * FROM {JOB_ERROR_LOG_TABLE}"))
    assert len(error_rows) == 0, "Job error log table should be empty."
```

**Pass/Fail Criterion:**
*   The `CALL` statement for `sp_vertragsdatenabgleich(p_source_param => '...', p_log_param => '...')` completes successfully.
*   The `job_audit` table contains exactly one row with `status = 'SUCCESS'`.
*   The `parameters` JSON column in the `job_audit` entry correctly reflects the passed `p_source_param` and `p_log_param` values.
*   The `job_error_log` table contains zero rows.

---

## Test Case 4: Error Handling - Core Logic Failure

**Purpose:**
To verify that if the core business logic (simulated by `sp_k_ausd_v_ta_inv_def`) fails, the `sp_vertragsdatenabgleich` procedure correctly catches the error, logs it to `job_error_log`, updates the `job_audit` table with a 'FAILED' status, and re-raises the error. This covers transformation correctness for error handling and external system replacements for logging.

**Setup:**
*   Ensure `project.dataset.sp_vertragsdatenabgleich` is deployed.
*   Ensure the mock `project.dataset.sp_k_ausd_v_ta_inv_def` is deployed and configured to *fail* when `p_simulate_error` is `TRUE`.
*   Audit tables (`job_audit`, `job_error_log`) are empty.

**Action:**
Call the BigQuery stored procedure `sp_vertragsdatenabgleich`, which in turn calls the mock `sp_k_ausd_v_ta_inv_def` with `p_simulate_error => TRUE`.

```python
def test_error_handling_core_logic_failure():
    """
    Tests the error handling mechanism when the core logic (sp_k_ausd_v_ta_inv_def) fails.
    """
    # Modify the mock SP to simulate an error
    mock_sp_error_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.{SP_CORE_NAME}`(
        p_job_kennung STRING,
        p_entry_number STRING,
        p_source_param STRING,
        p_log_param STRING,
        p_simulate_error BOOL DEFAULT FALSE
    )
    BEGIN
        IF p_simulate_error THEN
            RAISE USING MESSAGE 'Simulated error in core logic: Data reconciliation failed.';
        END IF;
        SELECT 'Core logic executed successfully.' AS core_logic_status;
    END;
    """
    execute_query(mock_sp_error_sql)

    # Call the main SP, which will trigger the error in the mock core SP
    success, error_message = call_sp(SP_NAME, p_source_param="error_test", p_log_param="error_log", p_simulate_error=True)

    assert success is False, "SP call should have failed due to core logic error."
    assert "Simulated error in core logic" in error_message, "Error message should indicate core logic failure."

    # Verify one entry in job_audit table with FAILED status
    audit_rows = list(execute_query(f"SELECT * FROM {JOB_AUDIT_TABLE}"))
    assert len(audit_rows) == 1, "Expected exactly one entry in job_audit table for a failed run."

    audit_entry = audit_rows[0]
    assert audit_entry.job_key == "BERT_V_TA_INV_DEF", "Job key mismatch in audit log."
    assert audit_entry.status == "FAILED", "Job status should be 'FAILED'."
    assert audit_entry.message == "Job failed with an error.", "Failure message mismatch."
    assert audit_entry.start_time is not None, "Start time should be logged."
    assert audit_entry.end_time is not None, "End time should be logged."
    assert audit_entry.end_time >= audit_entry.start_time, "End time should be after start time."
    assert audit_entry.entry_number is not None, "Entry number should be generated."

    # Verify one entry in job_error_log table
    error_rows = list(execute_query(f"SELECT * FROM {JOB_ERROR_LOG_TABLE}"))
    assert len(error_rows) == 1, "Expected exactly one entry in job_error_log table for a failed run."

    error_entry = error_rows[0]
    assert error_entry.job_key == "BERT_V_TA_INV_DEF", "Job key mismatch in error log."
    assert error_entry.entry_number == audit_entry.entry_number, "Entry number in error log should match audit log."
    assert error_entry.error_time is not None, "Error time should be logged."
    assert "Simulated error in core logic" in error_entry.error_message, "Error message mismatch in error log."
    assert error_entry.procedure_name == SP_NAME, "Procedure name in error log should be the main SP."
    assert error_entry.severity == "ERROR", "Error severity should be 'ERROR'."
    assert error_entry.stack_trace is not None, "Stack trace should be present in error log."

    # Reset mock SP to default behavior for subsequent tests
    setup_mock_core_sp()
```

**Pass/Fail Criterion:**
*   The `CALL` statement for `sp_vertragsdatenabgleich()` raises a BigQuery error, and the `success` variable is `False`.
*   The `job_audit` table contains exactly one row with `status = 'FAILED'`, valid `start_time` and `end_time`, and a failure message.
*   The `job_error_log` table contains exactly one row with detailed error information, including `error_message` containing "Simulated error in core logic", `stack_trace`, and `severity = 'ERROR'`.
*   The `entry_number` in `job_audit` and `job_error_log` for the same job run matches.

---

## Test Case 5: Data Quality - Audit Table Schema and Constraints

**Purpose:**
To verify that the `job_audit` and `job_error_log` tables adhere to their defined schemas, data types, and non-null constraints. This covers data quality and schema assertions.

**Setup:**
*   Ensure `job_audit` and `job_error_log` tables are created as per the design.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA` to inspect table and column definitions. Attempt to insert data violating `NOT NULL` constraints.

```python
def test_audit_table_schema_and_constraints():
    """
    Verifies the schema, data types, and NOT NULL constraints of the audit tables.
    """
    # Test job_audit table schema
    audit_schema_query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_audit'
    ORDER BY ordinal_position;
    """
    audit_schema = list(execute_query(audit_schema_query))
    audit_schema_map = {row.column_name: {'data_type': row.data_type, 'is_nullable': row.is_nullable} for row in audit_schema}

    expected_audit_schema = {
        'job_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'job_key': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'job_name': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'job_version': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'entry_number': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'start_time': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
        'end_time': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
        'status': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'message': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'stichtag_info': {'data_type': 'DATE', 'is_nullable': 'YES'},
        'parameters': {'data_type': 'JSON', 'is_nullable': 'YES'}
    }
    assert audit_schema_map == expected_audit_schema, "job_audit table schema mismatch."

    # Test job_error_log table schema
    error_schema_query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_error_log'
    ORDER BY ordinal_position;
    """
    error_schema = list(execute_query(error_schema_query))
    error_schema_map = {row.column_name: {'data_type': row.data_type, 'is_nullable': row.is_nullable} for row in error_schema}

    expected_error_schema = {
        'job_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'job_key': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'entry_number': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'error_time': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
        'error_code': {'data_type': 'INT64', 'is_nullable': 'YES'},
        'error_argument': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'error_message': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'stack_trace': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'procedure_name': {'data_type': 'STRING', 'is_nullable': 'YES'},
        'severity': {'data_type': 'STRING', 'is_nullable': 'YES'}
    }
    assert error_schema_map == expected_error_schema, "job_error_log table schema mismatch."

    # Test NOT NULL constraints for job_audit
    with pytest.raises(Exception, match="Cannot insert NULL value into column job_id"):
        execute_query(f"INSERT INTO {JOB_AUDIT_TABLE} (job_id, job_key, entry_number) VALUES (NULL, 'KEY', '123')")
    with pytest.raises(Exception, match="Cannot insert NULL value into column job_key"):
        execute_query(f"INSERT INTO {JOB_AUDIT_TABLE} (job_id, job_key, entry_number) VALUES ('ID', NULL, '123')")
    with pytest.raises(Exception, match="Cannot insert NULL value into column entry_number"):
        execute_query(f"INSERT INTO {JOB_AUDIT_TABLE} (job_id, job_key, entry_number) VALUES ('ID', 'KEY', NULL)")

    # Test NOT NULL constraints for job_error_log
    with pytest.raises(Exception, match="Cannot insert NULL value into column job_id"):
        execute_query(f"INSERT INTO {JOB_ERROR_LOG_TABLE} (job_id, job_key, entry_number) VALUES (NULL, 'KEY', '123')")
    with pytest.raises(Exception, match="Cannot insert NULL value into column job_key"):
        execute_query(f"INSERT INTO {JOB_ERROR_LOG_TABLE} (job_id, job_key, entry_number) VALUES ('ID', NULL, '123')")
    with pytest.raises(Exception, match="Cannot insert NULL value into column entry_number"):
        execute_query(f"INSERT INTO {JOB_ERROR_LOG_TABLE} (job_id, job_key, entry_number) VALUES ('ID', 'KEY', NULL)")

    # Clean up any partial inserts from constraint tests
    cleanup_audit_tables()
```

**Pass/Fail Criterion:**
*   Queries against `INFORMATION_SCHEMA` return the exact expected column names, data types, and `is_nullable` properties for both `job_audit` and `job_error_log`.
*   Attempts to `INSERT` `NULL` values into columns defined as `NOT NULL` (e.g., `job_id`, `job_key`, `entry_number`) result in a BigQuery error indicating a constraint violation.

---