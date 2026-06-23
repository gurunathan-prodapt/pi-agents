The migration of `r_ausd_v_ta_apn_ve.ksh` to BigQuery involves transforming an orchestration KornShell script into a BigQuery Stored Procedure (`vertragsdatenabgleich_wrapper`) that manages job execution, logging, and error handling, and invokes a core logic procedure (`k_ausd_v_ta_apn_ve`). The primary outputs of this wrapper are its operational logs and job status, which are now captured in BigQuery tables (`job_control`, `job_log`, `job_error_log`).

The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery solution to the legacy KornShell script, covering output parity, transformation correctness, external system replacements, and data quality.

---

## Test Setup Prerequisites

Before running the tests, ensure the following:

1.  **BigQuery Project and Dataset:** The BigQuery project (`my-gcp-project`) and dataset (`my_dataset`) exist.
2.  **Table DDLs Deployed:** The `job_control`, `job_log`, and `job_error_log` tables are created in `my_dataset` using the provided DDLs.
3.  **Stored Procedures Deployed:** The `k_ausd_v_ta_apn_ve` and `vertragsdatenabgleich_wrapper` stored procedures are created in `my_dataset` using the provided SQL.
4.  **Python Environment:** A Python environment with `pytest` and `google-cloud-bigquery` installed.
5.  **GCP Authentication:** Your environment is authenticated to GCP with sufficient permissions to query and modify BigQuery tables and execute stored procedures in `my-gcp-project.my_dataset`.

### `conftest.py` for Pytest

```python
# conftest.py
import pytest
from google.cloud import bigquery
import time

PROJECT_ID = "my-gcp-project"
DATASET_ID = "my_dataset"

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    client = bigquery.Client(project=PROJECT_ID)
    yield client

@pytest.fixture(autouse=True)
def clear_bq_tables(bq_client):
    """Clears relevant BigQuery tables before each test."""
    tables = ["job_control", "job_log", "job_error_log"]
    for table_name in tables:
        table_full_id = f"`{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        try:
            # Use DELETE statement to clear table contents
            bq_client.query(f"DELETE FROM {table_full_id} WHERE TRUE").result()
            print(f"Cleared table: {table_full_id}")
        except Exception as e:
            print(f"Warning: Could not clear table {table_full_id}: {e}")
    yield

@pytest.fixture
def deploy_temp_k_ausd_v_ta_apn_ve(bq_client):
    """
    Fixture to temporarily deploy a specific version of k_ausd_v_ta_apn_ve
    and ensure the original is restored.
    """
    original_k_ausd_v_ta_apn_ve_sql = """
        CREATE OR REPLACE PROCEDURE `my-gcp-project.my_dataset.k_ausd_v_ta_apn_ve`(
            IN p_job_kennung STRING,
            IN p_job_entry_number INT64
        )
        BEGIN
            INSERT INTO `my-gcp-project.my_dataset.job_log` (job_entry_number, log_timestamp, log_level, message)
            VALUES (p_job_entry_number, CURRENT_TIMESTAMP(), 'INFO', FORMAT("Starting core logic for JobKennung: %s", p_job_kennung));

            -- Simulate some work or a dummy SELECT
            SELECT 'Core logic executed successfully' AS status_message;

            INSERT INTO `my-gcp-project.my_dataset.job_log` (job_entry_number, log_timestamp, log_level, message)
            VALUES (p_job_entry_number, CURRENT_TIMESTAMP(), 'INFO', FORMAT("Finished core logic for JobKennung: %s", p_job_kennung));

        EXCEPTION WHEN ERROR THEN
            INSERT INTO `my-gcp-project.my_dataset.job_error_log` (
                job_entry_number, error_timestamp, script_name, error_code, error_message, sql_state, stack_trace
            ) VALUES (
                p_job_entry_number, CURRENT_TIMESTAMP(), 'k_ausd_v_ta_apn_ve', ERROR_CODE(), ERROR_MESSAGE(), SQLSTATE(), STACK_TRACE()
            );
            RAISE;
        END;
    """
    # Ensure the default (successful) version is deployed initially
    bq_client.query(original_k_ausd_v_ta_apn_ve_sql).result()
    yield
    # Restore the original (successful) version after the test
    bq_client.query(original_k_ausd_v_ta_apn_ve_sql).result()
    print("\nRestored original k_ausd_v_ta_apn_ve procedure.")

```

---

## Test Case 1: Successful Execution (Happy Path)

*   **Purpose:** Verify that the `vertragsdatenabgleich_wrapper` procedure executes successfully, calls the core procedure (`k_ausd_v_ta_apn_ve`), and correctly updates the job control and log tables. This proves output parity for successful runs.
*   **Setup:**
    *   `job_control`, `job_log`, `job_error_log` tables are empty (handled by `clear_bq_tables` fixture).
    *   `k_ausd_v_ta_apn_ve` procedure is deployed and functions without error (handled by `deploy_temp_k_ausd_v_ta_apn_ve` fixture).
*   **Action:** Call `vertragsdatenabgleich_wrapper` with valid parameters.
*   **Pass/Fail Criterion:**
    *   Exactly one row in `job_control` table with `status = 'OK'`, `job_kennung = 'TEST_JOB_SUCCESS'`, `stichtag = DATE '2023-10-26'`.
    *   `start_time` and `end_time` columns in `job_control` are populated.
    *   At least 4 `INFO` level entries in `job_log` for the corresponding `job_entry_number`, including messages from both wrapper and core procedures.
    *   Zero rows in `job_error_log`.

```python
# test_wrapper_success.py
import pytest
from google.cloud import bigquery

PROJECT_ID = "my-gcp-project"
DATASET_ID = "my_dataset"

def test_successful_execution(bq_client, deploy_temp_k_ausd_v_ta_apn_ve):
    """
    Tests the happy path: wrapper executes successfully, calls core SP,
    and updates job control/log tables correctly.
    """
    job_kennung = 'TEST_JOB_SUCCESS'
    stichtag = '2023-10-26'

    # Action: Call the wrapper procedure
    call_sql = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
            p_job_kennung_param => '{job_kennung}',
            p_stichtag_param => '{stichtag}',
            p_show_help => FALSE
        );
    """
    bq_client.query(call_sql).result()

    # Pass/Fail Criterion: Assertions
    # Check job_control table
    job_control_query = f"""
        SELECT job_entry_number, job_kennung, stichtag, status, start_time, end_time, error_code, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
        WHERE job_kennung = '{job_kennung}'
    """
    job_control_rows = list(bq_client.query(job_control_query).result())
    assert len(job_control_rows) == 1, "Expected exactly one job_control entry for successful run."
    job_entry = job_control_rows[0]

    assert job_entry.status == 'OK', f"Expected status 'OK', got '{job_entry.status}'"
    assert job_entry.job_kennung == job_kennung
    assert str(job_entry.stichtag) == stichtag
    assert job_entry.start_time is not None
    assert job_entry.end_time is not None
    assert job_entry.error_code is None
    assert job_entry.error_message is None

    # Check job_log table
    job_log_query = f"""
        SELECT log_level, message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
        WHERE job_entry_number = {job_entry.job_entry_number}
        ORDER BY log_timestamp
    """
    job_log_rows = list(bq_client.query(job_log_query).result())
    assert len(job_log_rows) >= 4, "Expected at least 4 log entries (wrapper start, core start, core end, wrapper end)."
    assert all(row.log_level == 'INFO' for row in job_log_rows)
    assert any("Job started" in row.message for row in job_log_rows)
    assert any("Starting core logic" in row.message for row in job_log_rows)
    assert any("Finished core logic" in row.message for row in job_log_rows)
    assert any("Job status set to OK" in row.message for row in job_log_rows)

    # Check job_error_log table
    job_error_log_query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_entry_number = {job_entry.job_entry_number}
    """
    job_error_log_rows = list(bq_client.query(job_error_log_query).result())
    assert len(job_error_log_rows) == 0, "Expected no error log entries for successful run."

```

---

## Test Case 2: Parameter Validation - Help Option

*   **Purpose:** Verify that calling the wrapper with `p_show_help = TRUE` prints usage information and exits gracefully without performing any job processing or logging. This ensures output parity for the `-h` option.
*   **Setup:**
    *   `job_control`, `job_log`, `job_error_log` tables are empty (handled by `clear_bq_tables` fixture).
*   **Action:** Call `vertragsdatenabgleich_wrapper` with `p_show_help = TRUE`.
*   **Pass/Fail Criterion:**
    *   The BigQuery client output (or the result of the `SELECT` statement within the procedure) contains the usage message.
    *   Zero rows in `job_control`, `job_log`, `job_error_log` tables.

```python
# test_wrapper_help_option.py
import pytest
from google.cloud import bigquery

PROJECT_ID = "my-gcp-project"
DATASET_ID = "my_dataset"

def test_help_option(bq_client):
    """
    Tests that the wrapper's help option displays usage and performs no other actions.
    """
    # Action: Call the wrapper procedure with p_show_help = TRUE
    call_sql = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
            p_job_kennung_param => NULL, -- Parameters should not matter when help is true
            p_stichtag_param => NULL,
            p_show_help => TRUE
        );
    """
    # The procedure returns a SELECT statement for usage info, which is captured by .result()
    rows = list(bq_client.query(call_sql).result())

    # Pass/Fail Criterion: Assertions
    assert len(rows) == 1, "Expected one row for usage info."
    assert "Usage: CALL" in rows[0].usage_info
    assert "p_job_kennung_param" in rows[0].usage_info
    assert "p_stichtag_param" in rows[0].usage_info

    # Check that no tables were modified
    job_control_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_control`"
    job_log_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_log`"
    job_error_log_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`"

    assert bq_client.query(job_control_count_query).result().single_value == 0
    assert bq_client.query(job_log_count_query).result().single_value == 0
    assert bq_client.query(job_error_log_count_query).result().single_value == 0

```

---

## Test Case 3: Parameter Validation - Missing Mandatory Parameters

*   **Purpose:** Verify that the wrapper correctly identifies and handles missing mandatory parameters (`p_job_kennung_param` or `p_stichtag_param`), logs an error, and updates job status to 'ERROR'. This directly maps to the legacy `ErrNr=192` behavior.
*   **Setup:**
    *   `job_control`, `job_log`, `job_error_log` tables are empty (handled by `clear_bq_tables` fixture).
*   **Action:** Call `vertragsdatenabgleich_wrapper` with `p_job_kennung_param` as `NULL`.
*   **Pass/Fail Criterion:**
    *   The `CALL` statement should raise a BigQuery error (SQLSTATE '45000') with a message indicating missing parameters.
    *   Exactly one row in `job_control` table with `status = 'ERROR'`, `error_code = 192`, and `error_message` containing "ERROR 192: Job Kennung (-l) and Stichtag (-s) parameters are mandatory.".
    *   Exactly one row in `job_error_log` with `error_code = 192` and the corresponding message.
    *   At least one `ERROR` level entry in `job_log` for the corresponding `job_entry_number`.

```python
# test_wrapper_missing_params.py
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

PROJECT_ID = "my-gcp-project"
DATASET_ID = "my_dataset"

def test_missing_mandatory_parameters(bq_client):
    """
    Tests that the wrapper correctly handles missing mandatory parameters,
    logging an error and setting job status to ERROR.
    """
    stichtag = '2023-10-26'

    # Action: Call the wrapper procedure with p_job_kennung_param as NULL
    call_sql = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
            p_job_kennung_param => NULL,
            p_stichtag_param => '{stichtag}',
            p_show_help => FALSE
        );
    """
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(call_sql).result()

    # Pass/Fail Criterion: Assertions for the raised error
    assert "ERROR 192: Job Kennung (-l) and Stichtag (-s) parameters are mandatory." in str(excinfo.value)

    # Check job_control table
    job_control_query = f"""
        SELECT job_entry_number, job_kennung, stichtag, status, error_code, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
    """
    job_control_rows = list(bq_client.query(job_control_query).result())
    assert len(job_control_rows) == 1, "Expected exactly one job_control entry for parameter error."
    job_entry = job_control_rows[0]

    assert job_entry.status == 'ERROR', f"Expected status 'ERROR', got '{job_entry.status}'"
    assert job_entry.job_kennung is None # JobKennung might not be set before error
    assert job_entry.stichtag is None # Stichtag might not be set before error
    assert job_entry.error_code == 192
    assert "ERROR 192: Job Kennung (-l) and Stichtag (-s) parameters are mandatory." in job_entry.error_message

    # Check job_error_log table
    job_error_log_query = f"""
        SELECT error_code, error_message, script_name
        FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_entry_number = {job_entry.job_entry_number}
    """
    job_error_log_rows = list(bq_client.query(job_error_log_query).result())
    assert len(job_error_log_rows) == 1, "Expected exactly one error log entry for parameter error."
    error_entry = job_error_log_rows[0]
    assert error_entry.error_code == 192
    assert "ERROR 192: Job Kennung (-l) and Stichtag (-s) parameters are mandatory." in error_entry.error_message
    assert error_entry.script_name == 'vertragsdatenabgleich_wrapper'

    # Check job_log table for error message
    job_log_query = f"""
        SELECT log_level, message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
        WHERE job_entry_number = {job_entry.job_entry_number} AND log_level = 'ERROR'
    """
    job_log_rows = list(bq_client.query(job_log_query).result())
    assert len(job_log_rows) >= 1, "Expected at least one ERROR log entry."
    assert any("Job failed with error: ERROR 192" in row.message for row in job_log_rows)

```

---

## Test Case 4: Core Logic Procedure Failure

*   **Purpose:** Verify that if the `k_ausd_v_ta_apn_ve` procedure fails, the wrapper catches the error, logs it, updates the job control status to 'ERROR', and re-raises the error. This proves correct error handling and output parity for failure scenarios (legacy `trap ERR`).
*   **Setup:**
    *   `job_control`, `job_log`, `job_error_log` tables are empty (handled by `clear_bq_tables` fixture).
    *   **Temporarily modify `k_ausd_v_ta_apn_ve` to intentionally fail.** This is done by the `deploy_failing_k_ausd_v_ta_apn_ve` fixture.
*   **Action:** Call `vertragsdatenabgleich_wrapper` with valid parameters.
*   **Pass/Fail Criterion:**
    *   The `CALL` statement should raise a BigQuery error.
    *   Exactly one row in `job_control` table with `status = 'ERROR'`, `job_kennung = 'TEST_JOB_CORE_FAIL'`.
    *   `error_code` and `error_message` in `job_control` should reflect the core logic failure (e.g., `error_message` containing 'Simulated core logic error').
    *   At least one row in `job_error_log` for the wrapper, with `script_name = 'vertragsdatenabgleich_wrapper'` and error details.
    *   Multiple `INFO` and at least one `ERROR` level entries in `job_log`.

```python
# test_wrapper_core_failure.py
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

PROJECT_ID = "my-gcp-project"
DATASET_ID = "my_dataset"

@pytest.fixture
def deploy_failing_k_ausd_v_ta_apn_ve(bq_client):
    """
    Fixture to temporarily deploy a version of k_ausd_v_ta_apn_ve that always fails.
    """
    failing_k_ausd_v_ta_apn_ve_sql = f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_apn_ve`(
            IN p_job_kennung STRING,
            IN p_job_entry_number INT64
        )
        BEGIN
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log` (job_entry_number, log_timestamp, log_level, message)
            VALUES (p_job_entry_number, CURRENT_TIMESTAMP(), 'INFO', 'Simulating core logic failure...');
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core logic error'; -- Force an error
        EXCEPTION WHEN ERROR THEN
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_error_log` (
                job_entry_number, error_timestamp, script_name, error_code, error_message, sql_state, stack_trace
            ) VALUES (
                p_job_entry_number, CURRENT_TIMESTAMP(), 'k_ausd_v_ta_apn_ve', ERROR_CODE(), ERROR_MESSAGE(), SQLSTATE(), STACK_TRACE()
            );
            RAISE;
        END;
    """
    original_k_ausd_v_ta_apn_ve_sql = f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_apn_ve`(
            IN p_job_kennung STRING,
            IN p_job_entry_number INT64
        )
        BEGIN
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log` (job_entry_number, log_timestamp, log_level, message)
            VALUES (p_job_entry_number, CURRENT_TIMESTAMP(), 'INFO', FORMAT("Starting core logic for JobKennung: %s", p_job_kennung));
            SELECT 'Core logic executed successfully' AS status_message;
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log` (job_entry_number, log_timestamp, log_level, message)
            VALUES (p_job_entry_number, CURRENT_TIMESTAMP(), 'INFO', FORMAT("Finished core logic for JobKennung: %s", p_job_kennung));
        EXCEPTION WHEN ERROR THEN
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_error_log` (
                job_entry_number, error_timestamp, script_name, error_code, error_message, sql_state, stack_trace
            ) VALUES (
                p_job_entry_number, CURRENT_TIMESTAMP(), 'k_ausd_v_ta_apn_ve', ERROR_CODE(), ERROR_MESSAGE(), SQLSTATE(), STACK_TRACE()
            );
            RAISE;
        END;
    """
    bq_client.query(failing_k_ausd_v_ta_apn_ve_sql).result()
    yield
    bq_client.query(original_k_ausd_v_ta_apn_ve_sql).result()
    print("\nRestored original k_ausd_v_ta_apn_ve procedure after failure test.")


def test_core_logic_procedure_failure(bq_client, deploy_failing_k_ausd_v_ta_apn_ve):
    """
    Tests that the wrapper correctly handles a failure in the core logic procedure,
    logging the error and setting job status to ERROR.
    """
    job_kennung = 'TEST_JOB_CORE_FAIL'
    stichtag = '2023-10-26'

    # Action: Call the wrapper procedure
    call_sql = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
            p_job_kennung_param => '{job_kennung}',
            p_stichtag_param => '{stichtag}',
            p_show_help => FALSE
        );
    """
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(call_sql).result()

    # Pass/Fail Criterion: Assertions for the raised error
    assert "Simulated core logic error" in str(excinfo.value)

    # Check job_control table
    job_control_query = f"""
        SELECT job_entry_number, job_kennung, stichtag, status, error_code, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
        WHERE job_kennung = '{job_kennung}'
    """
    job_control_rows = list(bq_client.query(job_control_query).result())
    assert len(job_control_rows) == 1, "Expected exactly one job_control entry for core logic failure."
    job_entry = job_control_rows[0]

    assert job_entry.status == 'ERROR', f"Expected status 'ERROR', got '{job_entry.status}'"
    assert job_entry.job_kennung == job_kennung
    assert str(job_entry.stichtag) == stichtag
    assert job_entry.error_code is not None # BigQuery's internal error code
    assert "Simulated core logic error" in job_entry.error_message

    # Check job_error_log table for wrapper's error entry
    job_error_log_query = f"""
        SELECT error_code, error_message, script_name
        FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_entry_number = {job_entry.job_entry_number} AND script_name = 'vertragsdatenabgleich_wrapper'
    """
    job_error_log_rows = list(bq_client.query(job_error_log_query).result())
    assert len(job_error_log_rows) == 1, "Expected exactly one error log entry from wrapper for core logic failure."
    error_entry = job_error_log_rows[0]
    assert error_entry.error_code is not None
    assert "Simulated core logic error" in error_entry.error_message
    assert error_entry.script_name == 'vertragsdatenabgleich_wrapper'

    # Check job_error_log table for core procedure's error entry (if it logs before re-raising)
    core_error_log_query = f"""
        SELECT error_code, error_message, script_name
        FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_entry_number = {job_entry.job_entry_number} AND script_name = 'k_ausd_v_ta_apn_ve'
    """
    core_error_log_rows = list(bq_client.query(core_error_log_query).result())
    assert len(core_error_log_rows) == 1, "Expected exactly one error log entry from core procedure."
    assert "Simulated core logic error" in core_error_log_rows[0].error_message

    # Check job_log table for error message
    job_log_query = f"""
        SELECT log_level, message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
        WHERE job_entry_number = {job_entry.job_entry_number} AND log_level = 'ERROR'
    """
    job_log_rows = list(bq_client.query(job_log_query).result())
    assert len(job_log_rows) >= 1, "Expected at least one ERROR log entry."
    assert any("Job failed with error: Simulated core logic error" in row.message for row in job_log_rows)

```

---

## Test Case 5: Job Entry Number Generation and Uniqueness

*   **Purpose:** Verify that `job_entry_number` is correctly incremented and unique across multiple job executions, mimicking the `DWMSG_ErmittleNr` functionality. This validates transformation correctness for job identifiers.
*   **Setup:**
    *   `job_control`, `job_log`, `job_error_log` tables are empty (handled by `clear_bq_tables` fixture).
    *   `k_ausd_v_ta_apn_ve` is in a successful state (handled by `deploy_temp_k_ausd_v_ta_apn_ve` fixture).
*   **Action:** Call `vertragsdatenabgleich_wrapper` multiple times with different parameters.
*   **Pass/Fail Criterion:**
    *   Three rows in `job_control` table, each with a unique and sequentially incremented `job_entry_number` (e.g., 1, 2, 3).
    *   All three jobs have `status = 'OK'`.
    *   No rows in `job_error_log`.
    *   `job_log` contains entries for all three `job_entry_number`s.

```python
# test_wrapper_job_entry_number.py
import pytest
from google.cloud import bigquery

PROJECT_ID = "my-gcp-project"
DATASET_ID = "my_dataset"

def test_job_entry_number_generation(bq_client, deploy_temp_k_ausd_v_ta_apn_ve):
    """
    Tests that job_entry_number is correctly generated and incremented for multiple runs.
    """
    jobs_to_run = [
        {'kennung': 'JOB_A', 'stichtag': '2023-10-26'},
        {'kennung': 'JOB_B', 'stichtag': '2023-10-27'},
        {'kennung': 'JOB_C', 'stichtag': '2023-10-28'},
    ]

    # Action: Call the wrapper procedure multiple times
    for job_data in jobs_to_run:
        call_sql = f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
                p_job_kennung_param => '{job_data['kennung']}',
                p_stichtag_param => '{job_data['stichtag']}',
                p_show_help => FALSE
            );
        """
        bq_client.query(call_sql).result()

    # Pass/Fail Criterion: Assertions
    # Check job_control table
    job_control_query = f"""
        SELECT job_entry_number, job_kennung, status
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
        ORDER BY job_entry_number
    """
    job_control_rows = list(bq_client.query(job_control_query).result())
    assert len(job_control_rows) == len(jobs_to_run), "Expected correct number of job_control entries."

    expected_job_entry_numbers = [1, 2, 3] # Assuming tables were empty initially
    for i, job_entry in enumerate(job_control_rows):
        assert job_entry.job_entry_number == expected_job_entry_numbers[i]
        assert job_entry.job_kennung == jobs_to_run[i]['kennung']
        assert job_entry.status == 'OK'

    # Check job_log table for entries for all jobs
    job_log_count_query = f"""
        SELECT job_entry_number, COUNT(*) as log_count
        FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
        GROUP BY job_entry_number
        ORDER BY job_entry_number
    """
    job_log_counts = list(bq_client.query(job_log_count_query).result())
    assert len(job_log_counts) == len(jobs_to_run), "Expected log entries for all jobs."
    for log_count_entry in job_log_counts:
        assert log_count_entry.log_count >= 4 # At least 4 INFO messages per job

    # Check job_error_log table
    job_error_log_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`"
    assert bq_client.query(job_error_log_count_query).result().single_value == 0, "Expected no error log entries."

```

---

## Test Case 6: Data Type Handling and Stichtag Format

*   **Purpose:** Verify that the `p_stichtag_param` (corresponding to legacy `v_sysdate`) is correctly passed as a `DATE` type and stored accurately in the `job_control` table. This validates type handling and transformation correctness.
*   **Setup:**
    *   `job_control`, `job_log`, `job_error_log` tables are empty (handled by `clear_bq_tables` fixture).
    *   `k_ausd_v_ta_apn_ve` is in a successful state (handled by `deploy_temp_k_ausd_v_ta_apn_ve` fixture).
*   **Action:** Call `vertragsdatenabgleich_wrapper` with a specific `stichtag` in `YYYY-MM-DD` format.
*   **Pass/Fail Criterion:**
    *   One row in `job_control` table with `stichtag = DATE '2023-01-15'`.
    *   The `stichtag` column in `job_control` is of `DATE` type.
    *   The log message in `job_log` correctly reflects the `stichtag` value.

```python
# test_wrapper_stichtag_type.py
import pytest
from google.cloud import bigquery
from datetime import date

PROJECT_ID = "my-gcp-project"
DATASET_ID = "my_dataset"

def test_stichtag_data_type_and_format(bq_client, deploy_temp_k_ausd_v_ta_apn_ve):
    """
    Tests that the stichtag parameter is correctly handled as a DATE type.
    """
    job_kennung = 'DATE_TEST'
    stichtag_str = '2023-01-15'
    expected_stichtag_date = date(2023, 1, 15)

    # Action: Call the wrapper procedure
    call_sql = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
            p_job_kennung_param => '{job_kennung}',
            p_stichtag_param => '{stichtag_str}',
            p_show_help => FALSE
        );
    """
    bq_client.query(call_sql).result()

    # Pass/Fail Criterion: Assertions
    # Check job_control table
    job_control_query = f"""
        SELECT job_entry_number, stichtag, FORMAT_DATE('%Y-%m-%d', stichtag) as stichtag_formatted
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
        WHERE job_kennung = '{job_kennung}'
    """
    job_control_rows = list(bq_client.query(job_control_query).result())
    assert len(job_control_rows) == 1, "Expected one job_control entry."
    job_entry = job_control_rows[0]

    assert job_entry.stichtag == expected_stichtag_date, f"Expected stichtag {expected_stichtag_date}, got {job_entry.stichtag}"
    assert job_entry.stichtag_formatted == stichtag_str, f"Expected formatted stichtag {stichtag_str}, got {job_entry.stichtag_formatted}"

    # Verify the data type in BigQuery schema (optional, but good for completeness)
    table_ref = bq_client.dataset(DATASET_ID).table("job_control")
    table = bq_client.get_table(table_ref)
    stichtag_field = next((field for field in table.schema if field.name == "stichtag"), None)
    assert stichtag_field is not None, "Stichtag field not found in schema."
    assert stichtag_field.field_type == "DATE", f"Expected stichtag to be DATE type, got {stichtag_field.field_type}"

    # Check job_log table for stichtag in message
    job_log_query = f"""
        SELECT message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
        WHERE job_entry_number = {job_entry.job_entry_number}
        AND message LIKE '%Stichtag: %'
    """
    job_log_rows = list(bq_client.query(job_log_query).result())
    assert any(f"Stichtag: {stichtag_str}" in row.message for row in job_log_rows), "Expected stichtag to be present in log message."

```

---

## Test Case 7: External Orchestration (Airflow DAG) - Success Scenario

*   **Purpose:** Verify that the Airflow DAG can successfully trigger the BigQuery Stored Procedure, passing parameters correctly, and that the job completes successfully in BigQuery. This validates the external system replacement (Airflow for shell script orchestration).
*   **Setup:**
    *   An Airflow environment is running and accessible.
    *   The `vertragsdatenabgleich_ta_apn_ve` DAG (from `composer/dags/vertragsdatenabgleich_dag.py`) is deployed to Airflow.
    *   A BigQuery connection named `google_cloud_default` is configured in Airflow, pointing to `my-gcp-project`.
    *   `job_control`, `job_log`, `job_error_log` tables are empty.
    *   `k_ausd_v_ta_apn_ve` is in a successful state.
*   **Action:** Manually trigger the `vertragsdatenabgleich_ta_apn_ve` DAG in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   **Airflow UI:** The `call_vertragsdatenabgleich_wrapper` task within the DAG run completes successfully (green status).
    *   **BigQuery:**
        *   Exactly one row in `job_control` table with `status = 'OK'`, `job_kennung = 'TA_APN_VE_DAILY'`, and `stichtag` matching the DAG's execution date (e.g., if DAG run for `2023-10-26`, then `stichtag = DATE '2023-10-26'`).
        *   Multiple `INFO` level entries in `job_log` for the corresponding `job_entry_number`.
        *   Zero rows in `job_error_log`.

```sql
-- SQL to verify BigQuery state after Airflow DAG run (manual check)

-- 1. Check job_control table
SELECT
    job_entry_number,
    job_kennung,
    script_name,
    stichtag,
    status,
    start_time,
    end_time,
    error_code,
    error_message
FROM `my-gcp-project.my_dataset.job_control`
WHERE job_kennung = 'TA_APN_VE_DAILY'
ORDER BY start_time DESC
LIMIT 1;

-- Expected result:
-- job_entry_number: (latest incremented ID)
-- job_kennung: 'TA_APN_VE_DAILY'
-- script_name: 'vertragsdatenabgleich_wrapper'
-- stichtag: (DAG execution date, e.g., '2023-10-26')
-- status: 'OK'
-- start_time: (timestamp)
-- end_time: (timestamp)
-- error_code: NULL
-- error_message: NULL

-- 2. Check job_log table for the latest job_entry_number
-- (Replace <latest_job_entry_number> with the value from the previous query)
SELECT
    log_timestamp,
    log_level,
    message
FROM `my-gcp-project.my_dataset.job_log`
WHERE job_entry_number = <latest_job_entry_number>
ORDER BY log_timestamp;

-- Expected result: Multiple 'INFO' messages, including "Job started...", "Starting core logic...", "Finished core logic...", "Job status set to OK."

-- 3. Check job_error_log table
SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_error_log`
WHERE job_entry_number = <latest_job_entry_number>;

-- Expected result: 0
```

---

## Test Case 8: External Orchestration (Airflow DAG) - Failure Scenario

*   **Purpose:** Verify that if the BigQuery Stored Procedure called by the Airflow DAG fails, the Airflow task also fails, and the error is correctly logged in BigQuery. This validates robust error propagation through the external orchestration layer.
*   **Setup:**
    *   An Airflow environment is running and accessible.
    *   The `vertragsdatenabgleich_ta_apn_ve` DAG is deployed to Airflow.
    *   A BigQuery connection named `google_cloud_default` is configured in Airflow.
    *   `job_control`, `job_log`, `job_error_log` tables are empty.
    *   **Crucially, temporarily modify `k_ausd_v_ta_apn_ve` to intentionally fail** (e.g., by deploying the failing version from Test Case 4).
*   **Action:** Manually trigger the `vertragsdatenabgleich_ta_apn_ve` DAG in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   **Airflow UI:** The `call_vertragsdatenabgleich_wrapper` task within the DAG run fails (red status).
    *   **BigQuery:**
        *   Exactly one row in `job_control` table with `status = 'ERROR'`, `job_kennung = 'TA_APN_VE_DAILY'`, and `stichtag` matching the DAG's execution date.
        *   `error_code` and `error_message` in `job_control` should reflect the core logic failure (e.g., containing 'Simulated core logic error').
        *   At least one row in `job_error_log` for the wrapper, with `script_name = 'vertragsdatenabgleich_wrapper'` and error details.
        *   Multiple `INFO` and at least one `ERROR` level entries in `job_log`.

```sql
-- SQL to verify BigQuery state after Airflow DAG run failure (manual check)

-- 1. Check job_control table
SELECT
    job_entry_number,
    job_kennung,
    script_name,
    stichtag,
    status,
    start_time,
    end_time,
    error_code,
    error_message
FROM `my-gcp-project.my_dataset.job_control`
WHERE job_kennung = 'TA_APN_VE_DAILY'
ORDER BY start_time DESC
LIMIT 1;

-- Expected result:
-- job_entry_number: (latest incremented ID)
-- job_kennung: 'TA_APN_VE_DAILY'
-- script_name: 'vertragsdatenabgleich_wrapper'
-- stichtag: (DAG execution date, e.g., '2023-10-26')
-- status: 'ERROR'
-- start_time: (timestamp)
-- end_time: (timestamp)
-- error_code: (BigQuery internal error code)
-- error_message: (e.g., "Simulated core logic error")

-- 2. Check job_log table for the latest job_entry_number
-- (Replace <latest_job_entry_number> with the value from the previous query)
SELECT
    log_timestamp,
    log_level,
    message
FROM `my-gcp-project.my_dataset.job_log`
WHERE job_entry_number = <latest_job_entry_number>
ORDER BY log_timestamp;

-- Expected result: Multiple 'INFO' messages, then an 'ERROR' message like "Job failed with error: Simulated core logic error..."

-- 3. Check job_error_log table
SELECT
    error_timestamp,
    script_name,
    error_code,
    error_message,
    sql_state,
    stack_trace
FROM `my-gcp-project.my_dataset.job_error_log`
WHERE job_entry_number = <latest_job_entry_number>
ORDER BY error_timestamp;

-- Expected result: At least two entries, one from 'k_ausd_v_ta_apn_ve' and one from 'vertragsdatenabgleich_wrapper', both detailing the 'Simulated core logic error'.
```