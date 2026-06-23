The migration of `r_ausd_v_ta_acc_ref.ksh` to a BigQuery Stored Procedure (`isbert_aufbereitung.vertragsdatenabgleich`) primarily involves replicating its orchestration, parameter handling, and logging mechanisms. The following test cases are designed to validate the behavioral equivalence of the migrated code, focusing on control flow, logging, and error handling.

**Assumptions for Testing:**

1.  **Corrected `v_job_kennung`:** The `v_job_kennung` variable in `isbert_aufbereitung.vertragsdatenabgleich` has been corrected to `DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_ACC_REF';` to match the static `JobKennung` from the legacy script, ensuring output parity.
2.  **`run_id` Propagation:** The `p_run_id` parameter has been added to `isbert_aufbereitung.k_ausd_v_ta_acc_ref` and `isbert_aufbereitung.f_alis_msgerr_bq_placeholder` and is correctly passed from `vertragsdatenabgleich` to ensure all log entries for a single execution can be correlated.
3.  **BigQuery Environment:** A GCP project and BigQuery dataset (`isbert_aufbereitung` and `isbert_logs`) are set up, and all DDLs and placeholder procedures provided in the migration design are deployed.

---

### Pytest Setup and Helper Functions

The following Python code provides the `pytest` framework and helper functions necessary to execute and assert the BigQuery tests.

```python
import pytest
from google.cloud import bigquery
import time
import uuid
import os

# Configuration for BigQuery project and dataset
# Replace with your actual GCP project ID
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id") 
DATASET_ID = "isbert_aufbereitung"
LOG_DATASET_ID = "isbert_logs"

# Ensure the project ID is set
if PROJECT_ID == "your-gcp-project-id":
    raise ValueError("GCP_PROJECT_ID environment variable not set or is default. Please configure it.")

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test session."""
    client = bigquery.Client(project=PROJECT_ID)
    yield client

@pytest.fixture(autouse=True)
def cleanup_logs(bq_client):
    """Clears log tables before each test to ensure isolation."""
    print(f"\n--- Cleaning up logs for project {PROJECT_ID} ---")
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{LOG_DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{LOG_DATASET_ID}.job_error_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{LOG_DATASET_ID}.job_log_detail`").result()
    yield

def call_bq_procedure(bq_client, procedure_name, **kwargs):
    """
    Executes a BigQuery stored procedure and captures its success/failure.
    Returns (True, None) on success, (False, error_message) on failure.
    """
    args_str = ", ".join([f"{k} => {repr(v)}" for k, v in kwargs.items()])
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.{procedure_name}`({args_str})"
    print(f"Executing: {query}")
    try:
        job = bq_client.query(query)
        job.result()  # Wait for job to complete
        return True, None  # Success
    except Exception as e:
        return False, str(e)  # Failure

def get_log_entries(bq_client, table_name, run_id=None):
    """
    Retrieves entries from a specified log table, optionally filtered by run_id.
    Returns a list of dictionaries, each representing a log row.
    """
    query = f"SELECT * FROM `{PROJECT_ID}.{LOG_DATASET_ID}.{table_name}`"
    if run_id:
        query += f" WHERE run_id = '{run_id}'"
    query += " ORDER BY log_timestamp ASC, entry_number ASC"  # Order for consistent checks
    rows = bq_client.query(query).result()
    return [dict(row) for row in rows]

def create_failing_k_ausd_v_ta_acc_ref(bq_client, dataset_id):
    """Temporarily replaces k_ausd_v_ta_acc_ref with a version that signals an error."""
    failing_proc_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{dataset_id}.k_ausd_v_ta_acc_ref`(
        IN p_job_kennung STRING,
        IN p_dw_eintrags_nr INT64,
        IN p_run_id STRING
    )
    BEGIN
        INSERT INTO `{PROJECT_ID}.{LOG_DATASET_ID}.job_log_detail` (
            job_kennung, entry_number, log_timestamp, log_level, message, run_id
        )
        VALUES (
            p_job_kennung,
            p_dw_eintrags_nr,
            CURRENT_TIMESTAMP(),
            'ERROR',
            'Simulated failure in k_ausd_v_ta_acc_ref',
            p_run_id
        );
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated k_ausd_v_ta_acc_ref failure';
    END;
    """
    bq_client.query(failing_proc_sql).result()
    print(f"--- Deployed failing k_ausd_v_ta_acc_ref ---")

def restore_original_k_ausd_v_ta_acc_ref(bq_client, dataset_id):
    """Restores the original placeholder k_ausd_v_ta_acc_ref procedure."""
    original_proc_sql = """
    CREATE OR REPLACE PROCEDURE isbert_aufbereitung.k_ausd_v_ta_acc_ref(
        IN p_job_kennung STRING,
        IN p_dw_eintrags_nr INT64,
        IN p_run_id STRING
    )
    BEGIN
        INSERT INTO isbert_logs.job_log_detail (
            job_kennung, entry_number, log_timestamp, log_level, message, run_id
        )
        VALUES (
            p_job_kennung,
            p_dw_eintrags_nr,
            CURRENT_TIMESTAMP(),
            'INFO',
            'Placeholder for k_ausd_v_ta_acc_ref executed. No actual business logic performed yet.',
            p_run_id
        );
    END;
    """
    bq_client.query(original_proc_sql).result()
    print(f"--- Restored original k_ausd_v_ta_acc_ref ---")

def create_failing_h_alis_date_bq_placeholder(bq_client, dataset_id):
    """Temporarily replaces h_alis_date_bq_placeholder with a version that signals an error for a specific date."""
    failing_proc_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{dataset_id}.h_alis_date_bq_placeholder`(
        IN p_input_date DATE,
        OUT p_output_format_yyyymmdd STRING
    )
    BEGIN
        IF p_input_date = DATE('2025-01-01') THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated h_alis_date_bq_placeholder failure for specific date';
        ELSE
            SET p_output_format_yyyymmdd = FORMAT_DATE('%Y%m%d', p_input_date);
        END IF;
    END;
    """
    bq_client.query(failing_proc_sql).result()
    print(f"--- Deployed failing h_alis_date_bq_placeholder ---")

def restore_original_h_alis_date_bq_placeholder(bq_client, dataset_id):
    """Restores the original placeholder h_alis_date_bq_placeholder procedure."""
    original_proc_sql = """
    CREATE OR REPLACE PROCEDURE isbert_aufbereitung.h_alis_date_bq_placeholder(
        IN p_input_date DATE,
        OUT p_output_format_yyyymmdd STRING
    )
    BEGIN
        SET p_output_format_yyyymmdd = FORMAT_DATE('%Y%m%d', p_input_date);
    END;
    """
    bq_client.query(original_proc_sql).result()
    print(f"--- Restored original h_alis_date_bq_placeholder ---")

```

---

### Test Case 1: Help Message Display

*   **Purpose:** Verify that calling the BigQuery stored procedure with `p_show_help = TRUE` (equivalent to `-h` in the legacy script) displays the usage information and exits without performing any job processing or logging. This validates output parity and control flow.
*   **Setup:**
    1.  Ensure the `isbert_aufbereitung.vertragsdatenabgleich` stored procedure is deployed.
    2.  Ensure logging tables (`job_log`, `job_error_log`, `job_log_detail`) are empty before the test. (Handled by `cleanup_logs` fixture).
*   **Action:** Call the `isbert_aufbereitung.vertragsdatenabgleich` procedure with `p_show_help => TRUE`.
*   **Pass/Fail Criterion:**
    *   The procedure execution completes successfully (it's designed to `RETURN` early).
    *   No new entries are found in `isbert_logs.job_log`, `isbert_logs.job_error_log`, or `isbert_logs.job_log_detail` tables.
    *   The BigQuery job's output (if captured) contains the expected help message. (Direct assertion of console output is difficult in BQ, but absence of log entries confirms early exit).

```python
def test_help_message_display(bq_client, cleanup_logs):
    # Action
    success, error_message = call_bq_procedure(bq_client, "vertragsdatenabgleich", p_show_help=True)

    # Assertions
    assert success, f"Procedure call failed unexpectedly: {error_message}"

    job_logs = get_log_entries(bq_client, "job_log")
    error_logs = get_log_entries(bq_client, "job_error_log")
    detail_logs = get_log_entries(bq_client, "job_log_detail")

    assert len(job_logs) == 0, f"Expected no job_log entries, but found {len(job_logs)}"
    assert len(error_logs) == 0, f"Expected no job_error_log entries, but found {len(error_logs)}"
    assert len(detail_logs) == 0, f"Expected no job_log_detail entries, but found {len(detail_logs)}"

    print("Test Case 1 Passed: Help message displayed, no logs generated.")
```

---

### Test Case 2: Successful Execution and Logging

*   **Purpose:** Verify the end-to-end successful execution of the wrapper procedure, ensuring it orchestrates correctly and logs job start, core script invocation, and job completion. This covers output parity for logging and transformation correctness for control flow.
*   **Setup:**
    1.  Ensure the `isbert_aufbereitung.vertragsdatenabgleich` and `isbert_aufbereitung.k_ausd_v_ta_acc_ref` (default placeholder) procedures are deployed.
    2.  Ensure logging tables are empty. (Handled by `cleanup_logs` fixture).
*   **Action:** Call the `isbert_aufbereitung.vertragsdatenabgleich` procedure with default parameters (or a specific `p_stichtag`).
*   **Pass/Fail Criterion:**
    *   The procedure execution completes successfully.
    *   `isbert_logs.job_log` contains exactly two entries for the same `run_id`: one with `status = 'RUNNING'` and one with `status = 'SUCCESS'`.
    *   The `SUCCESS` entry in `job_log` has `end_timestamp` populated.
    *   `isbert_logs.job_log_detail` contains at least three entries for the same `run_id`:
        *   An 'INFO' entry for 'Stichtag for processing'.
        *   An 'INFO' entry from the `k_ausd_v_ta_acc_ref` placeholder.
        *   An 'INFO' entry for job completion.
    *   The `job_kennung` in all log entries matches `BERT_V_TA_ACC_REF`.
    *   The `entry_number` and `run_id` are consistent across related log entries.

```python
def test_successful_execution_and_logging(bq_client, cleanup_logs):
    test_stichtag = "2023-01-15"
    
    # Action
    success, error_message = call_bq_procedure(bq_client, "vertragsdatenabgleich", p_stichtag=test_stichtag)

    # Assertions
    assert success, f"Procedure call failed unexpectedly: {error_message}"

    job_logs = get_log_entries(bq_client, "job_log")
    
    # Extract the run_id from the first log entry to filter subsequent queries
    assert len(job_logs) > 0, "No job_log entries found."
    run_id = job_logs[0]['run_id']

    job_logs_filtered = get_log_entries(bq_client, "job_log", run_id=run_id)
    detail_logs_filtered = get_log_entries(bq_client, "job_log_detail", run_id=run_id)

    # Verify job_log entries
    assert len(job_logs_filtered) == 2, f"Expected 2 job_log entries (RUNNING, SUCCESS), but found {len(job_logs_filtered)}"
    
    running_log = next((log for log in job_logs_filtered if log['status'] == 'RUNNING'), None)
    success_log = next((log for log in job_logs_filtered if log['status'] == 'SUCCESS'), None)

    assert running_log is not None, "RUNNING status not found in job_log"
    assert success_log is not None, "SUCCESS status not found in job_log"
    assert running_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
    assert success_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
    assert success_log['end_timestamp'] is not None
    assert success_log['entry_number'] == running_log['entry_number']
    assert success_log['run_id'] == running_log['run_id']

    # Verify job_log_detail entries
    assert len(detail_logs_filtered) >= 3, f"Expected at least 3 detail_log entries, but found {len(detail_logs_filtered)}"
    
    stichtag_info_log = next((log for log in detail_logs_filtered if 'Stichtag for processing' in log['message']), None)
    k_ausd_log = next((log for log in detail_logs_filtered if 'k_ausd_v_ta_acc_ref executed' in log['message']), None)
    completion_log = next((log for log in detail_logs_filtered if 'finished with status: SUCCESS' in log['message']), None)

    assert stichtag_info_log is not None, "Stichtag info not found in detail_log"
    assert k_ausd_log is not None, "k_ausd_v_ta_acc_ref execution log not found in detail_log"
    assert completion_log is not None, "Job completion log not found in detail_log"

    assert stichtag_info_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
    assert k_ausd_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
    assert completion_log['job_kennung'] == 'BERT_V_TA_ACC_REF'

    # Check stichtag info
    assert f"Stichtag for processing: {test_stichtag}" in stichtag_info_log['message']
    assert f"YYYYMMDD: {test_stichtag.replace('-', '')}" in stichtag_info_log['message']
    assert success_log['stichtag_info'] == test_stichtag.replace('-', '')

    print("Test Case 2 Passed: Successful execution and logging verified.")
```

---

### Test Case 3: Core Script Failure Handling

*   **Purpose:** Verify that the wrapper correctly handles errors originating from the invoked core script (`k_ausd_v_ta_acc_ref`), logs the failure, and propagates the error. This covers transformation correctness for error handling.
*   **Setup:**
    1.  Temporarily replace `isbert_aufbereitung.k_ausd_v_ta_acc_ref` with a version that `SIGNAL SQLSTATE` to simulate failure.
    2.  Ensure logging tables are empty. (Handled by `cleanup_logs` fixture).
*   **Action:** Call the `isbert_aufbereitung.vertragsdatenabgleich` procedure.
*   **Pass/Fail Criterion:**
    *   The procedure execution fails (i.e., `call_bq_procedure` returns `False`).
    *   `isbert_logs.job_log` contains exactly two entries for the same `run_id`: one 'RUNNING' and one 'FAILED'.
    *   The `FAILED` entry in `job_log` has `end_timestamp` populated and a message indicating failure.
    *   `isbert_logs.job_error_log` contains one entry for the same `run_id`, detailing the simulated error from `k_ausd_v_ta_acc_ref`.
    *   `isbert_logs.job_log_detail` contains entries for start, stichtag, the core script's error log, and the job failure message, all for the same `run_id`.
    *   The `job_kennung` in all log entries matches `BERT_V_TA_ACC_REF`.
    *   The `entry_number` and `run_id` are consistent across related log entries.

```python
def test_core_script_failure_handling(bq_client, cleanup_logs):
    test_stichtag = "2023-02-20"
    
    # Setup: Create a failing k_ausd_v_ta_acc_ref
    create_failing_k_ausd_v_ta_acc_ref(bq_client, DATASET_ID)
    
    try:
        # Action
        success, error_message = call_bq_procedure(bq_client, "vertragsdatenabgleich", p_stichtag=test_stichtag)

        # Assertions
        assert not success, "Procedure call was expected to fail but succeeded"
        assert "Simulated k_ausd_v_ta_acc_ref failure" in error_message

        job_logs = get_log_entries(bq_client, "job_log")
        assert len(job_logs) > 0, "No job_log entries found."
        run_id = job_logs[0]['run_id'] # Get run_id from the first log entry

        job_logs_filtered = get_log_entries(bq_client, "job_log", run_id=run_id)
        error_logs_filtered = get_log_entries(bq_client, "job_error_log", run_id=run_id)
        detail_logs_filtered = get_log_entries(bq_client, "job_log_detail", run_id=run_id)

        # Verify job_log entries
        assert len(job_logs_filtered) == 2, f"Expected 2 job_log entries (RUNNING, FAILED), but found {len(job_logs_filtered)}"
        running_log = next((log for log in job_logs_filtered if log['status'] == 'RUNNING'), None)
        failed_log = next((log for log in job_logs_filtered if log['status'] == 'FAILED'), None)

        assert running_log is not None, "RUNNING status not found in job_log"
        assert failed_log is not None, "FAILED status not found in job_log"
        assert failed_log['end_timestamp'] is not None
        assert 'failed with error' in failed_log['message']
        assert failed_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
        assert failed_log['entry_number'] == running_log['entry_number']
        assert failed_log['run_id'] == running_log['run_id']

        # Verify job_error_log entries
        assert len(error_logs_filtered) == 1, f"Expected 1 job_error_log entry, but found {len(error_logs_filtered)}"
        error_entry = error_logs_filtered[0]
        assert error_entry['error_message'] == 'Simulated k_ausd_v_ta_acc_ref failure'
        assert error_entry['program_name'] == 'vertragsdatenabgleich' # The wrapper is the one catching and logging
        assert error_entry['job_kennung'] == 'BERT_V_TA_ACC_REF'
        assert error_entry['entry_number'] == running_log['entry_number'] # Should be the same entry_number as the job run
        assert error_entry['run_id'] == running_log['run_id']

        # Verify job_log_detail entries
        assert len(detail_logs_filtered) >= 3, f"Expected at least 3 detail_log entries, but found {len(detail_logs_filtered)}"
        k_ausd_error_log = next((log for log in detail_logs_filtered if 'Simulated failure in k_ausd_v_ta_acc_ref' in log['message']), None)
        job_failure_log = next((log for log in detail_logs_filtered if 'finished with status: FAILED' in log['message']), None)
        
        assert k_ausd_error_log is not None, "k_ausd_v_ta_acc_ref error log not found in detail_log"
        assert job_failure_log is not None, "Job failure log not found in detail_log"
        assert k_ausd_error_log['log_level'] == 'ERROR'
        assert job_failure_log['log_level'] == 'ERROR'
        assert job_failure_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
        assert job_failure_log['entry_number'] == running_log['entry_number']
        assert job_failure_log['run_id'] == running_log['run_id']

        print("Test Case 3 Passed: Core script failure handling verified.")

    finally:
        # Teardown: Restore original k_ausd_v_ta_acc_ref
        restore_original_k_ausd_v_ta_acc_ref(bq_client, DATASET_ID)

```

---

### Test Case 4: Stichtag Parameter Handling

*   **Purpose:** Verify that the `p_stichtag` parameter is correctly processed, formatted, and reflected in the logging entries, ensuring type handling and data flow correctness.
*   **Setup:**
    1.  Ensure the `isbert_aufbereitung.vertragsdatenabgleich` procedure is deployed.
    2.  Ensure logging tables are empty. (Handled by `cleanup_logs` fixture).
*   **Action:** Call the `isbert_aufbereitung.vertragsdatenabgleich` procedure with a specific `p_stichtag` value.
*   **Pass/Fail Criterion:**
    *   The procedure execution completes successfully.
    *   The `stichtag_info` field in the `job_log` table for the `SUCCESS` entry matches the `YYYYMMDD` format of the provided `p_stichtag`.
    *   A `job_log_detail` entry exists with a message containing the provided `p_stichtag` in both `YYYY-MM-DD` and `YYYYMMDD` formats.
    *   The `job_kennung` remains `BERT_V_TA_ACC_REF` in all relevant log entries.
    *   The `entry_number` and `run_id` are consistent across related log entries.

```python
def test_stichtag_parameter_handling(bq_client, cleanup_logs):
    test_stichtag = "2024-03-01"
    expected_stichtag_yyyymmdd = "20240301"

    # Action
    success, error_message = call_bq_procedure(bq_client, "vertragsdatenabgleich", p_stichtag=test_stichtag)

    # Assertions
    assert success, f"Procedure call failed unexpectedly: {error_message}"

    job_logs = get_log_entries(bq_client, "job_log")
    assert len(job_logs) > 0, "No job_log entries found."
    run_id = job_logs[0]['run_id']

    job_logs_filtered = get_log_entries(bq_client, "job_log", run_id=run_id)
    detail_logs_filtered = get_log_entries(bq_client, "job_log_detail", run_id=run_id)

    success_log = next((log for log in job_logs_filtered if log['status'] == 'SUCCESS'), None)
    assert success_log is not None, "SUCCESS status not found in job_log"
    assert success_log['stichtag_info'] == expected_stichtag_yyyymmdd, \
        f"Expected stichtag_info '{expected_stichtag_yyyymmdd}', but got '{success_log['stichtag_info']}'"
    assert success_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
    assert success_log['run_id'] == run_id

    stichtag_detail_log = next((log for log in detail_logs_filtered if 'Stichtag for processing' in log['message']), None)
    assert stichtag_detail_log is not None, "Stichtag detail log not found"
    assert f"Stichtag for processing: {test_stichtag}" in stichtag_detail_log['message']
    assert f"YYYYMMDD: {expected_stichtag_yyyymmdd}" in stichtag_detail_log['message']
    assert stichtag_detail_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
    assert stichtag_detail_log['run_id'] == run_id

    print("Test Case 4 Passed: Stichtag parameter handling verified.")
```

---

### Test Case 5: `p_log_to_stdout_only` Flag Behavior (Legacy `-l` flag)

*   **Purpose:** Verify the behavior of the `p_log_to_stdout_only` flag. As noted in the design, its original purpose (redirecting to a log file) is superseded by BigQuery logging tables. The current implementation does not use this flag to suppress BigQuery logging. This test asserts that it does not prevent logging to BigQuery tables.
*   **Setup:**
    1.  Ensure the `isbert_aufbereitung.vertragsdatenabgleich` procedure is deployed.
    2.  Ensure logging tables are empty. (Handled by `cleanup_logs` fixture).
*   **Action:** Call the `isbert_aufbereitung.vertragsdatenabgleich` procedure with `p_log_to_stdout_only => TRUE`.
*   **Pass/Fail Criterion:**
    *   The procedure execution completes successfully.
    *   Logging entries are still created in `isbert_logs.job_log` and `isbert_logs.job_log_detail` as if the flag was `FALSE`.
    *   *Note:* If the intention is for this flag to suppress BigQuery logging, the procedure code needs to be updated, and this test would then assert the *absence* of log entries. For now, it asserts its non-effect on BQ logging.

```python
def test_log_to_stdout_only_flag_behavior(bq_client, cleanup_logs):
    test_stichtag = "2024-04-05"
    
    # Action
    success, error_message = call_bq_procedure(bq_client, "vertragsdatenabgleich", 
                                                p_stichtag=test_stichtag, 
                                                p_log_to_stdout_only=True)

    # Assertions
    assert success, f"Procedure call failed unexpectedly: {error_message}"

    job_logs = get_log_entries(bq_client, "job_log")
    assert len(job_logs) > 0, "No job_log entries found."
    run_id = job_logs[0]['run_id']

    job_logs_filtered = get_log_entries(bq_client, "job_log", run_id=run_id)
    detail_logs_filtered = get_log_entries(bq_client, "job_log_detail", run_id=run_id)

    # Assert that logging still occurred, as the flag is currently not implemented to suppress BQ logs.
    assert len(job_logs_filtered) == 2, f"Expected 2 job_log entries, but found {len(job_logs_filtered)}"
    assert next((log for log in job_logs_filtered if log['status'] == 'SUCCESS'), None) is not None, "SUCCESS status not found"
    assert len(detail_logs_filtered) >= 3, f"Expected at least 3 detail_log entries, but found {len(detail_logs_filtered)}"
    
    # Further check content to ensure it's a normal successful run log
    success_log = next((log for log in job_logs_filtered if log['status'] == 'SUCCESS'), None)
    assert success_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
    assert success_log['stichtag_info'] == test_stichtag.replace('-', '')
    assert success_log['run_id'] == run_id
    
    print("Test Case 5 Passed: p_log_to_stdout_only flag does not suppress BQ logging (current behavior).")
```

---

### Test Case 6: Error During Initialization/Logging Setup

*   **Purpose:** Verify robust error handling if an error occurs during the initial logging setup phase (specifically, during `h_alis_date_bq_placeholder` call, which happens before the main transaction and initial `RUNNING` log is committed). This ensures the `EXCEPTION` block is correctly triggered and error logs are captured. This covers transformation correctness for error handling.
*   **Setup:**
    1.  Temporarily modify `h_alis_date_bq_placeholder` to `SIGNAL SQLSTATE` when a specific `p_stichtag` is passed, simulating an error.
    2.  Ensure logging tables are empty. (Handled by `cleanup_logs` fixture).
*   **Action:** Call the `isbert_aufbereitung.vertragsdatenabgleich` procedure with the specific `p_stichtag` that triggers the error.
*   **Pass/Fail Criterion:**
    *   The procedure execution fails.
    *   `isbert_logs.job_log` contains exactly one 'FAILED' entry. (The 'RUNNING' entry is not committed due to the error occurring before the transaction).
    *   The `FAILED` entry in `job_log` has `end_timestamp` populated and a message indicating failure.
    *   `isbert_logs.job_error_log` contains one entry, detailing the simulated error.
    *   `isbert_logs.job_log_detail` contains one 'ERROR' entry for the job failure.
    *   The `job_kennung` in all log entries matches `BERT_V_TA_ACC_REF`.
    *   The `entry_number` and `run_id` are consistent across related log entries.

```python
def test_error_during_initialization_logging(bq_client, cleanup_logs):
    test_stichtag = "2025-01-01" # This date will trigger the simulated failure
    
    # Setup: Create a failing h_alis_date_bq_placeholder
    create_failing_h_alis_date_bq_placeholder(bq_client, DATASET_ID)
    
    try:
        # Action
        success, error_message = call_bq_procedure(bq_client, "vertragsdatenabgleich", p_stichtag=test_stichtag)

        # Assertions
        assert not success, "Procedure call was expected to fail but succeeded"
        assert "Simulated h_alis_date_bq_placeholder failure" in error_message

        job_logs = get_log_entries(bq_client, "job_log")
        assert len(job_logs) > 0, "No job_log entries found."
        run_id = job_logs[0]['run_id'] # Get run_id from the first log entry

        job_logs_filtered = get_log_entries(bq_client, "job_log", run_id=run_id)
        error_logs_filtered = get_log_entries(bq_client, "job_error_log", run_id=run_id)
        detail_logs_filtered = get_log_entries(bq_client, "job_log_detail", run_id=run_id)

        # Verify job_log entries:
        # The error occurs *before* the BEGIN TRANSACTION block, specifically during the CALL to h_alis_date_bq_placeholder.
        # This means the initial 'RUNNING' log entry is never inserted.
        # The EXCEPTION block is then hit, which inserts a FAILED entry.
        assert len(job_logs_filtered) == 1, f"Expected 1 job_log entry (FAILED), but found {len(job_logs_filtered)}"
        failed_log = job_logs_filtered[0]
        assert failed_log['status'] == 'FAILED'
        assert failed_log['end_timestamp'] is not None
        assert 'failed with error' in failed_log['message']
        assert failed_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
        assert failed_log['run_id'] == run_id

        # Verify job_error_log entries
        assert len(error_logs_filtered) == 1, f"Expected 1 job_error_log entry, but found {len(error_logs_filtered)}"
        error_entry = error_logs_filtered[0]
        assert 'Simulated h_alis_date_bq_placeholder failure' in error_entry['error_message']
        assert error_entry['program_name'] == 'vertragsdatenabgleich'
        assert error_entry['job_kennung'] == 'BERT_V_TA_ACC_REF'
        assert error_entry['run_id'] == run_id
        
        # Verify job_log_detail entries
        # Only the final error message from the EXCEPTION block will be logged here.
        assert len(detail_logs_filtered) == 1, f"Expected 1 detail_log entry for failure, but found {len(detail_logs_filtered)}"
        failure_detail_log = detail_logs_filtered[0]
        assert 'finished with status: FAILED' in failure_detail_log['message']
        assert failure_detail_log['log_level'] == 'ERROR'
        assert failure_detail_log['job_kennung'] == 'BERT_V_TA_ACC_REF'
        assert failure_detail_log['run_id'] == run_id

        print("Test Case 6 Passed: Error during initialization/logging setup verified.")

    finally:
        # Teardown: Restore original h_alis_date_bq_placeholder
        restore_original_h_alis_date_bq_placeholder(bq_client, DATASET_ID)

```