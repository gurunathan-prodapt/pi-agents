The migration of `r_ausd_v_ta_vvl_dwh.ksh` focuses on transforming a KornShell orchestration wrapper into a BigQuery Stored Procedure, leveraging BigQuery tables for logging and control. The core business logic (`k_ausd_v_ta_vvl_dwh.ksh`) is a separate migration effort, and for these tests, its behavior will be simulated.

The tests below are designed to validate the `project.dataset.vertragsdatenabgleich` BigQuery Stored Procedure, ensuring it replicates the orchestration, parameter handling, logging, and error management of the original KornShell script.

## General Setup for All Tests

Before running any tests, ensure the following:

1.  **BigQuery Environment:** A Google Cloud project and BigQuery dataset (`project.dataset`) are configured and accessible.
2.  **Table DDLs:** The DDLs for `job_control`, `job_messages`, `job_error_log`, and `job_audit` (as provided in the migration design) have been executed in your BigQuery dataset.
3.  **Stored Procedures:** The `project.dataset.vertragsdatenabgleich` and `project.dataset.k_ausd_v_ta_vvl_dwh` stored procedures are deployed.
4.  **Test Data Isolation:** For each test run, ensure the logging/control tables are cleared to prevent interference from previous runs.

    ```sql
    -- SQL to clear logging tables before each test
    TRUNCATE TABLE project.dataset.job_control;
    TRUNCATE TABLE project.dataset.job_messages;
    TRUNCATE TABLE project.dataset.job_error_log;
    TRUNCATE TABLE project.dataset.job_audit;
    ```

5.  **Legacy Baseline:** For output parity checks, execute the original `r_ausd_v_ta_vvl_dwh.ksh` script under various conditions (success, parameter error, simulated core logic error) and save its console output/log files. This provides a baseline for conceptual comparison with the BigQuery logging tables.

    *   **Legacy Success:** `ksh r_ausd_v_ta_vvl_dwh.ksh -s 20231026 -l INFO > legacy_success.log 2>&1`
    *   **Legacy Invalid Param:** `ksh r_ausd_v_ta_vvl_dwh.ksh -x > legacy_invalid_param.log 2>&1` (or `-s` without argument if `getopts` allows)
    *   **Legacy Core Logic Fail:** (Requires temporary modification of `k_ausd_v_ta_vvl_dwh.ksh` to `exit 1`) `ksh r_ausd_v_ta_vvl_dwh.ksh -s 20231026 -l INFO > legacy_core_fail.log 2>&1`

---

## Test Case 1: Successful Job Execution

*   **Purpose:** Validate that the migrated wrapper correctly orchestrates a successful job execution, including parameter handling, job initialization, invocation of the core logic (simulated success), and logging of successful completion. This test covers output parity for successful runs and verifies the correct population of control and logging tables.
*   **Covers:** Output parity, transformation correctness (init, call, success path, date handling), external-system replacements (logging to BQ tables), data-quality/row-count/schema assertions.
*   **Setup:**
    1.  Clear BigQuery logging tables (as per General Setup).
    2.  Deploy the `k_ausd_v_ta_vvl_dwh` stored procedure in its *successful* version:
        ```sql
        CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_v_ta_vvl_dwh(
            IN p_parent_job_kennung STRING,
            IN p_parent_entry_nr INT64
        )
        BEGIN
            -- Simulate successful core logic execution
            INSERT INTO project.dataset.job_messages (entry_nr, job_kennung, message_text, message_type, created_at)
            VALUES (p_parent_entry_nr, p_parent_job_kennung, 'k_ausd_v_ta_vvl_dwh: Core logic executed successfully.', 'INFO', CURRENT_TIMESTAMP());
        END;
        ```
*   **Action:**
    1.  Execute the `vertragsdatenabgleich` stored procedure with valid parameters:
        ```sql
        CALL project.dataset.vertragsdatenabgleich(p_stichtag => '20231026', p_loglevel => 'INFO');
        ```
    2.  Retrieve all records from `project.dataset.job_control`, `project.dataset.job_messages`, `project.dataset.job_error_log`, and `project.dataset.job_audit` for the most recent job.
*   **Pass/Fail Criterion:**
    1.  **No Error:** The BigQuery stored procedure call completes without raising an error.
    2.  **`job_control` Table:**
        *   Exactly one row exists for the executed job.
        *   The `status` column is 'OK'.
        *   `script_name` is 'r_ausd_v_ta_vvl_dwh'.
        *   `stichtag` is '20231026'.
        *   `created_at` and `finished_at` are populated, and `finished_at` is after `created_at`.
    3.  **`job_messages` Table:**
        *   At least 3 rows exist for the job (initial start, core logic success, job completion).
        *   Messages include "Job started...", "Core logic executed successfully.", and "Job completed successfully...".
        *   All `message_type` values are 'INFO'.
    4.  **`job_error_log` Table:** No rows exist for this job.
    5.  **`job_audit` Table:**
        *   Exactly one row exists for the job.
        *   The `status` is 'OK'.
    6.  **Output Parity (Conceptual):** The sequence and nature of log entries in BigQuery tables (`job_messages`, `job_control` status updates) conceptually match the successful execution flow observed in the `legacy_success.log` file. (Note: `JobKennung` and `entry_nr` values will differ from legacy due to new generation logic, but their role as unique identifiers is preserved).

```python
# Example Pytest assertion for Test Case 1
import pytest
from google.cloud import bigquery

def test_successful_job_execution(bigquery_client: bigquery.Client, project_id: str, dataset_id: str):
    # Setup: Clear tables and deploy successful k_ausd_v_ta_vvl_dwh SP
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_control;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_messages;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_error_log;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_audit;").result()

    successful_k_ausd_sp = f"""
    CREATE OR REPLACE PROCEDURE {project_id}.{dataset_id}.k_ausd_v_ta_vvl_dwh(
        IN p_parent_job_kennung STRING,
        IN p_parent_entry_nr INT64
    )
    BEGIN
        INSERT INTO {project_id}.{dataset_id}.job_messages (entry_nr, job_kennung, message_text, message_type, created_at)
        VALUES (p_parent_entry_nr, p_parent_job_kennung, 'k_ausd_v_ta_vvl_dwh: Core logic executed successfully.', 'INFO', CURRENT_TIMESTAMP());
    END;
    """
    bigquery_client.query(successful_k_ausd_sp).result()

    # Action: Call the main SP
    sp_call_query = f"CALL {project_id}.{dataset_id}.vertragsdatenabgleich(p_stichtag => '20231026', p_loglevel => 'INFO');"
    job = bigquery_client.query(sp_call_query)
    job.result() # Wait for completion, raises exception on error

    # Assertions
    # Get the job_kennung from the most recent job_control entry
    job_control_rows = list(bigquery_client.query(f"SELECT * FROM {project_id}.{dataset_id}.job_control ORDER BY created_at DESC LIMIT 1").result())
    assert len(job_control_rows) == 1, "Expected one job_control entry for successful run."
    job_control_entry = job_control_rows[0]
    job_kennung = job_control_entry['job_kennung']

    assert job_control_entry['status'] == 'OK', "Job control status should be 'OK'."
    assert job_control_entry['script_name'] == 'r_ausd_v_ta_vvl_dwh', "Script name mismatch."
    assert job_control_entry['stichtag'] == '20231026', "Stichtag mismatch."
    assert job_control_entry['created_at'] is not None, "Created_at should be populated."
    assert job_control_entry['finished_at'] is not None, "Finished_at should be populated."
    assert job_control_entry['finished_at'] > job_control_entry['created_at'], "Finished_at should be after created_at."

    job_messages_rows = list(bigquery_client.query(f"SELECT message_text, message_type FROM {project_id}.{dataset_id}.job_messages WHERE job_kennung = '{job_kennung}' ORDER BY created_at").result())
    assert len(job_messages_rows) >= 3, "Expected at least 3 job_messages entries."
    assert any("Job started" in r['message_text'] for r in job_messages_rows), "Missing 'Job started' message."
    assert any("Core logic executed successfully" in r['message_text'] for r in job_messages_rows), "Missing core logic success message."
    assert any("Job completed successfully" in r['message_text'] for r in job_messages_rows), "Missing 'Job completed successfully' message."
    assert all(r['message_type'] == 'INFO' for r in job_messages_rows), "All messages should be 'INFO'."

    job_error_rows = list(bigquery_client.query(f"SELECT * FROM {project_id}.{dataset_id}.job_error_log WHERE job_kennung = '{job_kennung}'").result())
    assert len(job_error_rows) == 0, "No error log entries expected for a successful run."

    job_audit_rows = list(bigquery_client.query(f"SELECT status FROM {project_id}.{dataset_id}.job_audit WHERE job_kennung = '{job_kennung}'").result())
    assert len(job_audit_rows) == 1, "Expected one job_audit entry."
    assert job_audit_rows[0]['status'] == 'OK', "Job audit status should be 'OK'."
```

---

## Test Case 2: Invalid `p_stichtag` Parameter

*   **Purpose:** Validate that the migrated wrapper correctly identifies and handles invalid input parameters (specifically `p_stichtag`), logging the error and terminating gracefully. This covers transformation correctness for parameter validation and error handling, and output parity for error scenarios.
*   **Covers:** Output parity (error logging), transformation correctness (parameter parsing, type handling, NULL handling, error handling), external-system replacements (error logging to BQ tables), data-quality/row-count/schema assertions.
*   **Setup:**
    1.  Clear BigQuery logging tables (as per General Setup).
*   **Action:**
    1.  Attempt to execute the `vertragsdatenabgleich` stored procedure with an invalid `p_stichtag` (e.g., `NULL` or a non-numeric string).
        ```sql
        -- Test 2a: NULL stichtag
        CALL project.dataset.vertragsdatenabgleich(p_stichtag => NULL, p_loglevel => 'INFO');

        -- Test 2b: Non-numeric stichtag
        CALL project.dataset.vertragsdatenabgleich(p_stichtag => 'INVALID_DATE', p_loglevel => 'INFO');
        ```
    2.  Retrieve all records from `project.dataset.job_control`, `project.dataset.job_messages`, `project.dataset.job_error_log`, and `project.dataset.job_audit` for the most recent job after each call.
*   **Pass/Fail Criterion:**
    1.  **Error Raised:** The BigQuery stored procedure call *must* raise an error (e.g., `SIGNAL SQLSTATE '45000'`) with a message indicating the parameter error.
    2.  **`job_control` Table:**
        *   Exactly one row exists for the executed job.
        *   The `status` column is 'ERROR'.
        *   `script_name` is 'r_ausd_v_ta_vvl_dwh'.
        *   `stichtag` is `NULL` (for `NULL` input) or the invalid string (for invalid string input).
        *   `created_at` and `finished_at` are populated.
    3.  **`job_messages` Table:**
        *   At least 2 rows exist (initial start, error message).
        *   One message explicitly states the parameter error (e.g., "Parameter -s (stichtag) is missing or invalid").
        *   At least one message has `message_type` 'ERROR'.
    4.  **`job_error_log` Table:**
        *   Exactly one row exists for the job.
        *   `error_nr` is populated (e.g., `1` as per the SP code for invalid stichtag).
        *   `error_arg` contains details about the invalid parameter.
    5.  **`job_audit` Table:**
        *   Exactly one row exists for the job.
        *   The `status` is 'ERROR'.
    6.  **Output Parity (Conceptual):** The BigQuery logging tables reflect an immediate termination due to an input parameter error, similar to how the legacy script would exit with `ErrNr=193` or `192` and log a usage message.

```python
# Example Pytest assertion for Test Case 2
import pytest
from google.cloud import bigquery

def test_invalid_stichtag_parameter(bigquery_client: bigquery.Client, project_id: str, dataset_id: str):
    # Setup: Clear tables
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_control;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_messages;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_error_log;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_audit;").result()

    # Test 2a: Call with NULL stichtag
    with pytest.raises(Exception) as excinfo:
        bigquery_client.query(f"CALL {project_id}.{dataset_id}.vertragsdatenabgleich(p_stichtag => NULL, p_loglevel => 'INFO');").result()
    assert "Parameter -s (stichtag) is missing or invalid" in str(excinfo.value), "Expected parameter validation error for NULL stichtag."

    job_control_rows = list(bigquery_client.query(f"SELECT * FROM {project_id}.{dataset_id}.job_control ORDER BY created_at DESC LIMIT 1").result())
    assert len(job_control_rows) == 1, "Expected one job_control entry for NULL stichtag failure."
    job_control_entry = job_control_rows[0]
    job_kennung_null = job_control_entry['job_kennung']

    assert job_control_entry['status'] == 'ERROR', "Job control status should be 'ERROR' for NULL stichtag."
    assert job_control_entry['stichtag'] is None, "Stichtag should be NULL for NULL input."

    job_messages_rows = list(bigquery_client.query(f"SELECT message_text, message_type FROM {project_id}.{dataset_id}.job_messages WHERE job_kennung = '{job_kennung_null}' ORDER BY created_at").result())
    assert any("Parameter -s (stichtag) is missing or invalid" in r['message_text'] and r['message_type'] == 'ERROR' for r in job_messages_rows), "Missing parameter error message for NULL stichtag."

    job_error_rows = list(bigquery_client.query(f"SELECT error_nr, error_arg FROM {project_id}.{dataset_id}.job_error_log WHERE job_kennung = '{job_kennung_null}'").result())
    assert len(job_error_rows) == 1, "Expected one error log entry for NULL stichtag."
    assert job_error_rows[0]['error_nr'] == 1, "Error number mismatch for NULL stichtag."
    assert "Invalid Stichtag" in job_error_rows[0]['error_arg'], "Error argument mismatch for NULL stichtag."

    job_audit_rows = list(bigquery_client.query(f"SELECT status FROM {project_id}.{dataset_id}.job_audit WHERE job_kennung = '{job_kennung_null}'").result())
    assert len(job_audit_rows) == 1, "Expected one job_audit entry for NULL stichtag."
    assert job_audit_rows[0]['status'] == 'ERROR', "Job audit status should be 'ERROR' for NULL stichtag."

    # Test 2b: Call with non-numeric stichtag
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_control;").result() # Clear for next test
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_messages;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_error_log;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_audit;").result()

    with pytest.raises(Exception) as excinfo:
        bigquery_client.query(f"CALL {project_id}.{dataset_id}.vertragsdatenabgleich(p_stichtag => 'ABC', p_loglevel => 'INFO');").result()
    assert "Parameter -s (stichtag) is missing or invalid" in str(excinfo.value), "Expected parameter validation error for non-numeric stichtag."

    job_control_rows = list(bigquery_client.query(f"SELECT * FROM {project_id}.{dataset_id}.job_control ORDER BY created_at DESC LIMIT 1").result())
    assert len(job_control_rows) == 1, "Expected one job_control entry for non-numeric stichtag failure."
    job_control_entry = job_control_rows[0]
    job_kennung_invalid = job_control_entry['job_kennung']

    assert job_control_entry['status'] == 'ERROR', "Job control status should be 'ERROR' for non-numeric stichtag."
    assert job_control_entry['stichtag'] == 'ABC', "Stichtag should be 'ABC' for 'ABC' input."

    job_messages_rows = list(bigquery_client.query(f"SELECT message_text, message_type FROM {project_id}.{dataset_id}.job_messages WHERE job_kennung = '{job_kennung_invalid}' ORDER BY created_at").result())
    assert any("Parameter -s (stichtag) is missing or invalid" in r['message_text'] and r['message_type'] == 'ERROR' for r in job_messages_rows), "Missing parameter error message for non-numeric stichtag."

    job_error_rows = list(bigquery_client.query(f"SELECT error_nr, error_arg FROM {project_id}.{dataset_id}.job_error_log WHERE job_kennung = '{job_kennung_invalid}'").result())
    assert len(job_error_rows) == 1, "Expected one error log entry for non-numeric stichtag."
    assert job_error_rows[0]['error_nr'] == 1, "Error number mismatch for non-numeric stichtag."
    assert "Invalid Stichtag" in job_error_rows[0]['error_arg'], "Error argument mismatch for non-numeric stichtag."

    job_audit_rows = list(bigquery_client.query(f"SELECT status FROM {project_id}.{dataset_id}.job_audit WHERE job_kennung = '{job_kennung_invalid}'").result())
    assert len(job_audit_rows) == 1, "Expected one job_audit entry for non-numeric stichtag."
    assert job_audit_rows[0]['status'] == 'ERROR', "Job audit status should be 'ERROR' for non-numeric stichtag."
```

---

## Test Case 3: Core Logic Failure (Simulated)

*   **Purpose:** Validate that the migrated wrapper correctly catches and logs errors originating from the invoked core processing logic (`k_ausd_v_ta_vvl_dwh`), updating job status and logging error details. This covers transformation correctness for error handling (`EXCEPTION` block) and output parity for core logic failures.
*   **Covers:** Output parity (error logging), transformation correctness (error handling, `EXCEPTION` block), external-system replacements (error logging to BQ tables), data-quality/row-count/schema assertions.
*   **Setup:**
    1.  Clear BigQuery logging tables (as per General Setup).
    2.  Deploy the `k_ausd_v_ta_vvl_dwh` stored procedure in its *failing* version:
        ```sql
        CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_v_ta_vvl_dwh(
            IN p_parent_job_kennung STRING,
            IN p_parent_entry_nr INT64
        )
        BEGIN
            -- Simulate core logic error
            INSERT INTO project.dataset.job_messages (entry_nr, job_kennung, message_text, message_type, created_at)
            VALUES (p_parent_entry_nr, p_parent_job_kennung, 'k_ausd_v_ta_vvl_dwh: Simulating core logic error.', 'INFO', CURRENT_TIMESTAMP());
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error in core logic for contract reconciliation.';
        END;
        ```
*   **Action:**
    1.  Execute the `vertragsdatenabgleich` stored procedure with valid parameters.
        ```sql
        CALL project.dataset.vertragsdatenabgleich(p_stichtag => '20231026', p_loglevel => 'INFO');
        ```
    2.  Retrieve all records from `project.dataset.job_control`, `project.dataset.job_messages`, `project.dataset.job_error_log`, and `project.dataset.job_audit` for the most recent job.
*   **Pass/Fail Criterion:**
    1.  **Error Raised:** The BigQuery stored procedure call *must* raise an error, propagating the error from `k_ausd_v_ta_vvl_dwh`. The error message should contain "Simulated error in core logic...".
    2.  **`job_control` Table:**
        *   Exactly one row exists for the executed job.
        *   The `status` column is 'ERROR'.
        *   `script_name` is 'r_ausd_v_ta_vvl_dwh'.
        *   `stichtag` is '20231026'.
        *   `created_at` and `finished_at` are populated.
    3.  **`job_messages` Table:**
        *   At least 3 rows exist (initial start, core logic info, error message).
        *   One message explicitly states the core logic error (e.g., "Simulating core logic error.").
        *   One message indicates job failure (e.g., "Job failed for JobKennung...").
        *   At least one message has `message_type` 'ERROR'.
    4.  **`job_error_log` Table:**
        *   Exactly one row exists for the job.
        *   `error_nr` and `error_arg` are populated with details from the simulated core logic error.
    5.  **`job_audit` Table:**
        *   Exactly one row exists for the job.
        *   The `status` is 'ERROR'.
    6.  **Output Parity (Conceptual):** The BigQuery logging tables reflect a job that started, attempted core logic, failed, and then terminated with an error status, similar to the legacy script's `trap ERR` behavior.

```python
# Example Pytest assertion for Test Case 3
import pytest
from google.cloud import bigquery

def test_core_logic_failure(bigquery_client: bigquery.Client, project_id: str, dataset_id: str):
    # Setup: Clear tables and deploy failing k_ausd_v_ta_vvl_dwh SP
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_control;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_messages;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_error_log;").result()
    bigquery_client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_audit;").result()

    failing_k_ausd_sp = f"""
    CREATE OR REPLACE PROCEDURE {project_id}.{dataset_id}.k_ausd_v_ta_vvl_dwh(
        IN p_parent_job_kennung STRING,
        IN p_parent_entry_nr INT64
    )
    BEGIN
        INSERT INTO {project_id}.{dataset_id}.job_messages (entry_nr, job_kennung, message_text, message_type, created_at)
        VALUES (p_parent_entry_nr, p_parent_job_kennung, 'k_ausd_v_ta_vvl_dwh: Simulating core logic error.', 'INFO', CURRENT_TIMESTAMP());
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error in core logic for contract reconciliation.';
    END;
    """
    bigquery_client.query(failing_k_ausd_sp).result()

    # Action: Call the main SP
    sp_call_query = f"CALL {project_id}.{dataset_id}.vertragsdatenabgleich(p_stichtag => '20231026', p_loglevel => 'INFO');"
    with pytest.raises(Exception) as excinfo:
        bigquery_client.query(sp_call_query).result()
    assert "Simulated error in core logic for contract reconciliation." in str(excinfo.value), "Expected core logic error to be propagated."

    # Assertions
    job_control_rows = list(bigquery_client.query(f"SELECT * FROM {project_id}.{dataset_id}.job_control ORDER BY created_at DESC LIMIT 1").result())
    assert len(job_control_rows) == 1, "Expected one job_control entry for core logic failure."
    job_control_entry = job_control_rows[0]
    job_kennung = job_control_entry['job_kennung']

    assert job_control_entry['status'] == 'ERROR', "Job control status should be 'ERROR' for core logic failure."
    assert job_control_entry['script_name'] == 'r_ausd_v_ta_vvl_dwh', "Script name mismatch."
    assert job_control_entry['stichtag'] == '20231026', "Stichtag mismatch."
    assert job_control_entry['created_at'] is not None, "Created_at should be populated."
    assert job_control_entry['finished_at'] is not None, "Finished_at should be populated."

    job_messages_rows = list(bigquery_client.query(f"SELECT message_text, message_type FROM {project_id}.{dataset_id}.job_messages WHERE job_kennung = '{job_kennung}' ORDER BY created_at").result())
    assert len(job_messages_rows) >= 3, "Expected at least 3 job_messages entries."
    assert any("Job started" in r['message_text'] for r in job_messages_rows), "Missing 'Job started' message."
    assert any("Simulating core logic error" in r['message_text'] for r in job_messages_rows), "Missing core logic simulation message."
    assert any("Job failed" in r['message_text'] and r['message_type'] == 'ERROR' for r in job_messages_rows), "Missing 'Job failed' error message."

    job_error_rows = list(bigquery_client.query(f"SELECT error_nr, error_arg FROM {project_id}.{dataset_id}.job_error_log WHERE job_kennung = '{job_kennung}'").result())
    assert len(job_error_rows) == 1, "Expected one error log entry for core logic failure."
    assert job_error_rows[0]['error_arg'] is not None, "Error argument should be populated."
    assert "Simulated error in core logic" in job_error_rows[0]['error_arg'], "Error argument should contain core logic error details."

    job_audit_rows = list(bigquery_client.query(f"SELECT status FROM {project_id}.{dataset_id}.job_audit WHERE job_kennung = '{job_kennung}'").result())
    assert len(job_audit_rows) == 1, "Expected one job_audit entry."
    assert job_audit_rows[0]['status'] == 'ERROR', "Job audit status should be 'ERROR'."
```