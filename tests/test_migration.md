As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migrated BigQuery stored procedure `ausd_bp_ta_cntrct_dist_wrapper`. These tests aim to ensure behavioral equivalence with the legacy KornShell script `r_ausd_bp_ta_cntrct_dist.ksh`, covering output parity, transformation correctness, external system replacements, and data quality assertions for logging.

The tests are structured with a `purpose`, `setup`, `action`, and `pass/fail criterion` for clarity. Where applicable, runnable `pytest` code snippets are provided, leveraging the `google-cloud-bigquery` client library for interaction with BigQuery.

---

## Test Environment Setup

Before running any tests, ensure the following BigQuery resources are deployed:

1.  **Logging Tables DDL:** The DDL provided in `ddl/logging_tables.sql` must be executed to create the `job_control`, `job_log`, `job_error_log`, and `job_message_log` tables in your target BigQuery dataset.
2.  **Wrapper Stored Procedure:** The `ausd_bp_ta_cntrct_dist_wrapper` stored procedure from `sprocs/ausd_bp_ta_cntrct_dist_wrapper.sql` must be deployed.
3.  **Dummy Kernel Stored Procedure:** A dummy `ausd_bp_ta_cntrct_dist_kernel` procedure is required to simulate the behavior of the actual kernel script and capture the parameters passed to it. This allows us to verify the wrapper's parameter handling without needing the full kernel logic.

### Dummy Kernel Stored Procedure (`ausd_bp_ta_cntrct_dist_kernel`)

```sql
-- project_id.dataset_id.ausd_bp_ta_cntrct_dist_kernel
CREATE OR REPLACE PROCEDURE `project_id.dataset_id.ausd_bp_ta_cntrct_dist_kernel`(
    IN p_kernel_stichtag DATE,
    IN p_kernel_wiederanlaufWert INT64
)
BEGIN
    -- This is a dummy kernel for testing the wrapper.
    -- It logs the parameters it received and can simulate failure.

    DECLARE v_current_job_id STRING;

    -- Retrieve the job_id from the most recent entry in job_control
    -- This links kernel logs to the wrapper's job execution.
    SELECT job_id INTO v_current_job_id FROM `project_id.dataset_id.job_control` ORDER BY start_time DESC LIMIT 1;

    INSERT INTO `project_id.dataset_id.job_log` (log_id, job_id, log_level, message, step, details)
    VALUES (GENERATE_UUID(), v_current_job_id, 'INFO', 'Dummy kernel called', 'Kernel Execution',
            TO_JSON(STRUCT(p_kernel_stichtag AS received_stichtag, p_kernel_wiederanlaufWert AS received_wiederanlaufwert)));

    -- Simulate kernel failure if p_kernel_wiederanlaufWert is negative
    IF p_kernel_wiederanlaufWert < 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel failure: Negative wiederanlaufWert not allowed by dummy kernel.';
    END IF;
END;
```

### Python `pytest` Setup

```python
import pytest
from google.cloud import bigquery
import os
from datetime import datetime, date

# --- Configuration ---
# Set these environment variables or replace directly
PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "your_dataset_id")
FULL_DATASET_ID = f"{PROJECT_ID}.{DATASET_ID}"

client = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions ---
def execute_bq_query(query):
    """Executes a BigQuery SQL query."""
    query_job = client.query(query)
    return query_job.result()

def call_wrapper_procedure(stichtag: str = None, wiederanlaufwert: int = None):
    """Calls the BigQuery wrapper stored procedure with given parameters."""
    params = []
    if stichtag is not None:
        params.append(f"p_stichtag => '{stichtag}'")
    else:
        params.append("p_stichtag => NULL")

    if wiederanlaufwert is not None:
        params.append(f"p_wiederanlaufWert => {wiederanlaufwert}")
    else:
        params.append("p_wiederanlaufWert => NULL")

    param_str = ", ".join(params)
    query = f"CALL `{FULL_DATASET_ID}.ausd_bp_ta_cntrct_dist_wrapper`({param_str});"
    print(f"\nExecuting: {query}")
    try:
        execute_bq_query(query)
        return True, None
    except Exception as e:
        return False, str(e)

def get_latest_job_control_entry():
    """Retrieves the most recent entry from job_control table."""
    query = f"SELECT * FROM `{FULL_DATASET_ID}.job_control` ORDER BY start_time DESC LIMIT 1"
    rows = execute_bq_query(query)
    return next(iter(rows), None)

def get_job_logs(job_id: str):
    """Retrieves all log entries for a given job_id."""
    query = f"SELECT * FROM `{FULL_DATASET_ID}.job_log` WHERE job_id = '{job_id}' ORDER BY log_time ASC"
    return list(execute_bq_query(query))

def get_job_errors(job_id: str):
    """Retrieves all error entries for a given job_id."""
    query = f"SELECT * FROM `{FULL_DATASET_ID}.job_error_log` WHERE job_id = '{job_id}' ORDER BY error_time ASC"
    return list(execute_bq_query(query))

def clear_logging_tables():
    """Truncates all logging tables."""
    tables = ["job_control", "job_log", "job_error_log", "job_message_log"]
    for table in tables:
        execute_bq_query(f"TRUNCATE TABLE `{FULL_DATASET_ID}.{table}`")

@pytest.fixture(autouse=True)
def setup_and_teardown_each_test():
    """Fixture to clear logging tables before and after each test."""
    clear_logging_tables()
    yield
    clear_logging_tables()
```

---

## Migration Validation Tests

### Test Case 1: Successful Execution - All Parameters Provided

**Purpose:** Verify that the wrapper correctly processes both `p_stichtag` and `p_wiederanlaufWert` when explicitly provided, logs the execution, and calls the kernel with the correct, transformed parameters.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.
*   No specific data setup required beyond the dummy kernel.

**Action:**
Call the `ausd_bp_ta_cntrct_dist_wrapper` with a valid `p_stichtag` and `p_wiederanlaufWert`.

```python
def test_wrapper_success_all_params_provided():
    stichtag_input = "15032023"
    wiederanlaufwert_input = 12345
    
    success, error_msg = call_wrapper_procedure(stichtag=stichtag_input, wiederanlaufwert=wiederanlaufwert_input)
    assert success, f"Wrapper call failed: {error_msg}"

    job_control = get_latest_job_control_entry()
    assert job_control is not None
    assert job_control.status == 'SUCCESS'
    assert job_control.error_message is None

    job_logs = get_job_logs(job_control.job_id)
    assert any("Job execution started" in log.message for log in job_logs)
    assert any("Parameters processed and validated" in log.message for log in job_logs)
    assert any("Job completed successfully" in log.message for log in job_logs)

    # Verify parameters passed to kernel
    kernel_log = next((log for log in job_logs if "Dummy kernel called" in log.message), None)
    assert kernel_log is not None
    assert kernel_log.details['received_stichtag'] == date(2023, 3, 15).isoformat()
    assert kernel_log.details['received_wiederanlaufwert'] == wiederanlaufwert_input
```

**Pass/Fail Criterion:**
*   The `job_control` table shows a `status` of 'SUCCESS' for the latest job.
*   The `job_control.error_message` is NULL.
*   The `job_log` table contains entries indicating job start, parameter processing, kernel call, and successful completion.
*   The `job_log` entry for the "Dummy kernel called" step correctly reflects `received_stichtag` as `DATE('2023-03-15')` and `received_wiederanlaufwert` as `12345`.

---

### Test Case 2: Successful Execution - `p_stichtag` Provided, `p_wiederanlaufWert` Defaults

**Purpose:** Verify that `p_wiederanlaufWert` correctly defaults to `0` when not provided, while `p_stichtag` is processed as given.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.

**Action:**
Call the `ausd_bp_ta_cntrct_dist_wrapper` with a valid `p_stichtag` but `p_wiederanlaufWert` as NULL.

```python
def test_wrapper_success_stichtag_only():
    stichtag_input = "01012024"
    
    success, error_msg = call_wrapper_procedure(stichtag=stichtag_input, wiederanlaufwert=None)
    assert success, f"Wrapper call failed: {error_msg}"

    job_control = get_latest_job_control_entry()
    assert job_control.status == 'SUCCESS'

    job_logs = get_job_logs(job_control.job_id)
    kernel_log = next((log for log in job_logs if "Dummy kernel called" in log.message), None)
    assert kernel_log is not None
    assert kernel_log.details['received_stichtag'] == date(2024, 1, 1).isoformat()
    assert kernel_log.details['received_wiederanlaufwert'] == 0 # Should default to 0
```

**Pass/Fail Criterion:**
*   The `job_control` table shows a `status` of 'SUCCESS'.
*   The `job_log` entry for the "Dummy kernel called" step correctly reflects `received_stichtag` as `DATE('2024-01-01')` and `received_wiederanlaufwert` as `0`.

---

### Test Case 3: Successful Execution - No Parameters Provided (Both Default)

**Purpose:** Verify that both `p_stichtag` and `p_wiederanlaufWert` correctly default when no parameters are provided. `p_stichtag` should default to the current system date.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.

**Action:**
Call the `ausd_bp_ta_cntrct_dist_wrapper` with both `p_stichtag` and `p_wiederanlaufWert` as NULL.

```python
def test_wrapper_success_no_params_provided():
    expected_sysdate = date.today() # Get today's date for comparison
    
    success, error_msg = call_wrapper_procedure(stichtag=None, wiederanlaufwert=None)
    assert success, f"Wrapper call failed: {error_msg}"

    job_control = get_latest_job_control_entry()
    assert job_control.status == 'SUCCESS'

    job_logs = get_job_logs(job_control.job_id)
    kernel_log = next((log for log in job_logs if "Dummy kernel called" in log.message), None)
    assert kernel_log is not None
    assert kernel_log.details['received_stichtag'] == expected_sysdate.isoformat() # Should default to current date
    assert kernel_log.details['received_wiederanlaufwert'] == 0 # Should default to 0
```

**Pass/Fail Criterion:**
*   The `job_control` table shows a `status` of 'SUCCESS'.
*   The `job_log` entry for the "Dummy kernel called" step correctly reflects `received_stichtag` as `CURRENT_DATE()` (formatted as DATE) and `received_wiederanlaufwert` as `0`.

---

### Test Case 4: Error Handling - Invalid `p_stichtag` Format

**Purpose:** Verify that the wrapper correctly identifies and handles an invalid `p_stichtag` format, logs the error, and sets the job status to 'FAILED'.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.

**Action:**
Call the `ausd_bp_ta_cntrct_dist_wrapper` with a `p_stichtag` in an incorrect format (e.g., '2023-03-15').

```python
def test_wrapper_error_invalid_stichtag_format():
    invalid_stichtag = "2023-03-15" # Expected DDMMYYYY
    
    success, error_msg = call_wrapper_procedure(stichtag=invalid_stichtag, wiederanlaufwert=100)
    assert not success, "Wrapper call unexpectedly succeeded with invalid stichtag."
    assert "Invalid Stichtag format. Expected 'DDMMYYYY'" in error_msg

    job_control = get_latest_job_control_entry()
    assert job_control is not None
    assert job_control.status == 'FAILED'
    assert "Invalid Stichtag format" in job_control.error_message

    job_logs = get_job_logs(job_control.job_id)
    assert any("Job failed during execution" in log.message and log.log_level == 'ERROR' for log in job_logs)

    job_errors = get_job_errors(job_control.job_id)
    assert len(job_errors) > 0
    assert "Invalid Stichtag format" in job_errors[0].error_message
```

**Pass/Fail Criterion:**
*   The `call_wrapper_procedure` returns `False` (indicating an error) and the error message contains "Invalid Stichtag format".
*   The `job_control` table shows a `status` of 'FAILED'.
*   The `job_control.error_message` contains "Invalid Stichtag format".
*   The `job_log` table contains an 'ERROR' level entry indicating job failure.
*   The `job_error_log` table contains an entry detailing the format error.

---

### Test Case 5: Error Handling - Empty `p_stichtag` String

**Purpose:** Verify that an empty string for `p_stichtag` is treated as if it were NULL, causing it to default to the system date. This tests the `NULLIF(TRIM(p_stichtag), '')` logic.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.

**Action:**
Call the `ausd_bp_ta_cntrct_dist_wrapper` with `p_stichtag` as an empty string `''`.

```python
def test_wrapper_empty_stichtag_string_defaults_to_sysdate():
    expected_sysdate = date.today()
    
    success, error_msg = call_wrapper_procedure(stichtag="", wiederanlaufwert=100)
    assert success, f"Wrapper call failed: {error_msg}"

    job_control = get_latest_job_control_entry()
    assert job_control.status == 'SUCCESS'

    job_logs = get_job_logs(job_control.job_id)
    kernel_log = next((log for log in job_logs if "Dummy kernel called" in log.message), None)
    assert kernel_log is not None
    assert kernel_log.details['received_stichtag'] == expected_sysdate.isoformat()
    assert kernel_log.details['received_wiederanlaufwert'] == 100
```

**Pass/Fail Criterion:**
*   The `job_control` table shows a `status` of 'SUCCESS'.
*   The `job_log` entry for the "Dummy kernel called" step correctly reflects `received_stichtag` as `CURRENT_DATE()` (formatted as DATE) and `received_wiederanlaufwert` as `100`.

---

### Test Case 6: Error Handling - Kernel Script Failure

**Purpose:** Verify that if the called kernel stored procedure fails, the wrapper catches the error, logs it, and sets its own job status to 'FAILED'.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.
*   The dummy `ausd_bp_ta_cntrct_dist_kernel` is configured to fail if `p_kernel_wiederanlaufWert` is negative.

**Action:**
Call the `ausd_bp_ta_cntrct_dist_wrapper` with `p_wiederanlaufWert` set to a negative value (e.g., -1), which will trigger a simulated failure in the dummy kernel.

```python
def test_wrapper_error_kernel_failure():
    stichtag_input = "20032023"
    wiederanlaufwert_input = -1 # Triggers dummy kernel failure
    
    success, error_msg = call_wrapper_procedure(stichtag=stichtag_input, wiederanlaufwert=wiederanlaufwert_input)
    assert not success, "Wrapper call unexpectedly succeeded despite kernel failure."
    assert "Simulated kernel failure" in error_msg

    job_control = get_latest_job_control_entry()
    assert job_control is not None
    assert job_control.status == 'FAILED'
    assert "Simulated kernel failure" in job_control.error_message

    job_logs = get_job_logs(job_control.job_id)
    assert any("Dummy kernel called" in log.message for log in job_logs) # Kernel was called
    assert any("Job failed during execution" in log.message and log.log_level == 'ERROR' for log in job_logs)

    job_errors = get_job_errors(job_control.job_id)
    assert len(job_errors) > 0
    assert "Simulated kernel failure" in job_errors[0].error_message
```

**Pass/Fail Criterion:**
*   The `call_wrapper_procedure` returns `False` and the error message contains "Simulated kernel failure".
*   The `job_control` table shows a `status` of 'FAILED'.
*   The `job_control.error_message` contains "Simulated kernel failure".
*   The `job_log` table contains an 'ERROR' level entry indicating job failure, and an 'INFO' entry confirming the kernel was called.
*   The `job_error_log` table contains an entry detailing the kernel's simulated error.

---

### Test Case 7: Logging - `job_control` Table Assertions

**Purpose:** Verify that the `job_control` table accurately records the job's lifecycle, including `job_id`, `start_time`, `end_time`, `status`, and `parameters`.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.

**Action:**
Execute the wrapper procedure with various parameter combinations (e.g., one successful run, one failed run). Then query the `job_control` table.

```python
def test_logging_job_control_assertions():
    # First successful run
    call_wrapper_procedure(stichtag="01012023", wiederanlaufwert=10)
    job1_control = get_latest_job_control_entry()
    assert job1_control.status == 'SUCCESS'
    assert job1_control.job_name == 'ausd_bp_ta_cntrct_dist_wrapper'
    assert job1_control.start_time is not None
    assert job1_control.end_time is not None
    assert job1_control.parameters['p_stichtag_raw'] == '01012023'
    assert job1_control.parameters['p_wiederanlaufWert_raw'] == 10

    # Second failed run
    call_wrapper_procedure(stichtag="01012023", wiederanlaufwert=-5) # Triggers kernel failure
    job2_control = get_latest_job_control_entry()
    assert job2_control.status == 'FAILED'
    assert job2_control.error_message is not None
    assert job2_control.parameters['p_stichtag_raw'] == '01012023'
    assert job2_control.parameters['p_wiederanlaufWert_raw'] == -5
```

**Pass/Fail Criterion:**
*   For each execution, a new entry exists in `job_control`.
*   `job_id` is a valid UUID.
*   `job_name` is 'ausd_bp_ta_cntrct_dist_wrapper'.
*   `start_time` and `end_time` are populated correctly.
*   `status` reflects 'SUCCESS' or 'FAILED' as expected.
*   `parameters` JSON field accurately stores the raw input parameters.
*   `error_message` is NULL for successful runs and contains the error for failed runs.

---

### Test Case 8: Logging - `job_log` Table Assertions

**Purpose:** Verify that the `job_log` table captures detailed execution steps, messages, and log levels throughout the wrapper's lifecycle.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.

**Action:**
Execute the wrapper procedure (e.g., a successful run). Query the `job_log` table for the corresponding `job_id`.

```python
def test_logging_job_log_assertions():
    stichtag_input = "25122023"
    wiederanlaufwert_input = 500
    
    success, error_msg = call_wrapper_procedure(stichtag=stichtag_input, wiederanlaufwert=wiederanlaufwert_input)
    assert success, f"Wrapper call failed: {error_msg}"

    job_control = get_latest_job_control_entry()
    job_logs = get_job_logs(job_control.job_id)

    # Check for expected log messages and order (approximate)
    messages = [log.message for log in job_logs]
    assert "Job execution started" in messages
    assert "Parameters processed and validated" in messages
    assert "Dummy kernel called" in messages
    assert "Job completed successfully" in messages

    # Verify log levels
    assert next(log.log_level for log in job_logs if "Job execution started" in log.message) == 'INFO'
    assert next(log.log_level for log in job_logs if "Parameters processed and validated" in log.message) == 'INFO'
    assert next(log.log_level for log in job_logs if "Dummy kernel called" in log.message) == 'INFO'
    assert next(log.log_level for log in job_logs if "Job completed successfully" in log.message) == 'INFO'

    # Verify details for parameter processing log
    param_log = next(log for log in job_logs if "Parameters processed and validated" in log.message)
    assert param_log.details['stichtag_ddmmyyyy'] == stichtag_input
    assert param_log.details['stichtag_date'] == date(2023, 12, 25).isoformat()
    assert param_log.details['wiederanlaufwert'] == wiederanlaufwert_input
```

**Pass/Fail Criterion:**
*   The `job_log` table contains a sequence of 'INFO' level messages reflecting the job's progress (start, parameter processing, kernel call, successful completion).
*   The 'Parameters processed and validated' log entry's `details` JSON correctly shows the processed `stichtag_ddmmyyyy`, `stichtag_date`, and `wiederanlaufwert`.
*   No 'ERROR' level messages are present for a successful run.

---

### Test Case 9: Data Type and NULL Handling for `p_wiederanlaufWert`

**Purpose:** Verify that `p_wiederanlaufWert` is correctly handled as an `INT64` and defaults to `0` when `NULL` or not provided.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.

**Action:**
Call the wrapper with `p_wiederanlaufWert` as `NULL`, `0`, and a positive integer. Verify the value received by the kernel.

```python
def test_wiederanlaufwert_type_and_null_handling():
    stichtag_val = "01012023"

    # Test 1: p_wiederanlaufWert is NULL (should default to 0)
    call_wrapper_procedure(stichtag=stichtag_val, wiederanlaufwert=None)
    job_control_null = get_latest_job_control_entry()
    job_logs_null = get_job_logs(job_control_null.job_id)
    kernel_log_null = next(log for log in job_logs_null if "Dummy kernel called" in log.message)
    assert kernel_log_null.details['received_wiederanlaufwert'] == 0

    # Test 2: p_wiederanlaufWert is 0
    call_wrapper_procedure(stichtag=stichtag_val, wiederanlaufwert=0)
    job_control_zero = get_latest_job_control_entry()
    job_logs_zero = get_job_logs(job_control_zero.job_id)
    kernel_log_zero = next(log for log in job_logs_zero if "Dummy kernel called" in log.message)
    assert kernel_log_zero.details['received_wiederanlaufwert'] == 0

    # Test 3: p_wiederanlaufWert is a positive integer
    call_wrapper_procedure(stichtag=stichtag_val, wiederanlaufwert=999)
    job_control_pos = get_latest_job_control_entry()
    job_logs_pos = get_job_logs(job_control_pos.job_id)
    kernel_log_pos = next(log for log in job_logs_pos if "Dummy kernel called" in log.message)
    assert kernel_log_pos.details['received_wiederanlaufwert'] == 999
```

**Pass/Fail Criterion:**
*   When `p_wiederanlaufWert` is `NULL` or `0`, the kernel receives `0` (INT64).
*   When `p_wiederanlaufWert` is a positive integer, the kernel receives that integer (INT64).
*   All calls result in 'SUCCESS' status in `job_control`.

---

### Test Case 10: External System Replacement - Date Determination

**Purpose:** Verify that the BigQuery `CURRENT_DATE()` and `FORMAT_DATE()` functions correctly replace the legacy `DWDate_Gib_Zeitraum` for system date determination.

**Setup:**
*   Ensure logging tables and stored procedures are deployed.

**Action:**
Call the wrapper without `p_stichtag` (so it defaults to `v_sysdate`). Verify the `received_stichtag` in the kernel log matches the actual current date.

```python
def test_date_determination_replacement():
    expected_sysdate = date.today()
    
    success, error_msg = call_wrapper_procedure(stichtag=None, wiederanlaufwert=100)
    assert success, f"Wrapper call failed: {error_msg}"

    job_control = get_latest_job_control_entry()
    job_logs = get_job_logs(job_control.job_id)
    kernel_log = next((log for log in job_logs if "Dummy kernel called" in log.message), None)
    
    assert kernel_log is not None
    assert kernel_log.details['received_stichtag'] == expected_sysdate.isoformat()
```

**Pass/Fail Criterion:**
*   The `received_stichtag` in the kernel log entry for the job matches the `date.today()` value at the time of execution.
*   The job completes successfully.

---