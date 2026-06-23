The migration of `r_ausd_v_ta_barrier.ksh` to BigQuery involves a significant shift from shell scripting to SQL stored procedures and an orchestration layer. The following tests aim to validate the behavioral equivalence across these different components.

**Assumptions for Test Execution:**
*   A Google Cloud project (`my_project_id`) and dataset (`my_dataset_id`) are configured.
*   All DDLs (`job_log_table_ddl.sql`, `job_status_table_ddl.sql`) have been executed.
*   All BigQuery stored procedures (`DWMSG_*_SP`, `k_ausd_v_ta_barrier_sp`, `r_ausd_v_ta_barrier_sp`) have been deployed to `my_project_id.my_dataset_id`.
*   A Python environment with `pytest` and `google-cloud-bigquery` client library is available.
*   BigQuery client is authenticated and has necessary permissions.
*   For Airflow tests, an Airflow environment is running, and the DAG `r_ausd_v_ta_barrier_orchestration` is deployed and unpaused. The `google_cloud_default` connection is configured.
*   The `k_ausd_v_ta_barrier_sp` is a placeholder. For tests requiring its failure, we will assume a mechanism to simulate failure (e.g., by modifying it to `RAISE` an error under specific conditions or by having a test version).

---

## Test Setup (Common for all tests)

**Purpose:** To ensure a clean and consistent state before each test run, and to provide necessary helper functions for BigQuery interaction.

**Setup:**
1.  **BigQuery Client Initialization:** A `pytest` fixture to provide a BigQuery client.
2.  **Table Clearing:** A helper function to clear `job_log_table` and `job_status_table` before each test.
3.  **Mocking `k_ausd_v_ta_barrier_sp` (if needed):** For scenarios where the kernel needs to fail, a temporary replacement or conditional logic within `k_ausd_v_ta_barrier_sp` can be used. For simplicity in these examples, we'll assume `k_ausd_v_ta_barrier_sp` can be temporarily modified or a test version deployed.

**Runnable Test Code (Python/Pytest):**

```python
# conftest.py or a common test_utils.py
import pytest
from google.cloud import bigquery
import os
import time

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "my_project_id")
DATASET_ID = os.environ.get("GCP_DATASET_ID", "my_dataset_id")
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log_table"
JOB_STATUS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_status_table"
R_AUSD_V_TA_BARRIER_SP = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_v_ta_barrier_sp"
K_AUSD_V_TA_BARRIER_SP = f"{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_barrier_sp"
DWMSG_MELDEFEHLER_SP = f"{PROJECT_ID}.{DATASET_ID}.DWMSG_MeldeFehler_SP"
DWMSG_FEHLERBEHANDLUNG_SP = f"{PROJECT_ID}.{DATASET_ID}.DWMSG_Fehlerbehandlung_SP"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for tests."""
    client = bigquery.Client(project=PROJECT_ID)
    return client

@pytest.fixture(autouse=True)
def clear_tables_before_each_test(bq_client):
    """Clears log and status tables before each test."""
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{JOB_STATUS_TABLE}`").result()
    # Ensure tables are empty
    assert bq_client.query(f"SELECT COUNT(1) FROM `{JOB_LOG_TABLE}`").result().total_rows == 0
    assert bq_client.query(f"SELECT COUNT(1) FROM `{JOB_STATUS_TABLE}`").result().total_rows == 0
    yield # Allow test to run
    # Optional: Clear tables again after test if needed for specific scenarios
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{JOB_STATUS_TABLE}`").result()

def call_bq_procedure(bq_client, procedure_name, params):
    """Helper to call a BigQuery stored procedure."""
    param_str = ", ".join([f"{k} => {repr(v)}" for k, v in params.items()])
    query = f"CALL `{procedure_name}`({param_str})"
    print(f"Executing: {query}")
    return bq_client.query(query)

def get_table_data(bq_client, table_name, order_by=None):
    """Helper to fetch all data from a table."""
    query = f"SELECT * FROM `{table_name}`"
    if order_by:
        query += f" ORDER BY {order_by}"
    return list(bq_client.query(query).result())

def get_log_messages(bq_client, job_nr=None, job_kennung=None):
    """Helper to fetch log messages for a specific job."""
    query = f"SELECT log_message, severity FROM `{JOB_LOG_TABLE}`"
    conditions = []
    if job_nr is not None:
        conditions.append(f"job_nr = {job_nr}")
    if job_kennung is not None:
        conditions.append(f"job_kennung = '{job_kennung}'")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY log_ts ASC"
    return [(row.log_message, row.severity) for row in bq_client.query(query).result()]

def get_job_status(bq_client, job_nr=None, job_kennung=None):
    """Helper to fetch job status."""
    query = f"SELECT status FROM `{JOB_STATUS_TABLE}`"
    conditions = []
    if job_nr is not None:
        conditions.append(f"job_nr = {job_nr}")
    if job_kennung is not None:
        conditions.append(f"job_kennung = '{job_kennung}'")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY last_update_ts DESC LIMIT 1"
    result = bq_client.query(query).result()
    return result.rows[0].status if result.total_rows > 0 else None

# Helper to temporarily modify k_ausd_v_ta_barrier_sp for error simulation
def deploy_test_k_ausd_v_ta_barrier_sp(bq_client, should_fail=False):
    """Deploys a test version of k_ausd_v_ta_barrier_sp."""
    fail_logic = ""
    if should_fail:
        fail_logic = "RAISE USING MESSAGE = 'Simulated kernel failure for testing';"
    
    sp_code = f"""
    CREATE OR REPLACE PROCEDURE `{K_AUSD_V_TA_BARRIER_SP}`(
        IN p_job_kennung STRING,
        IN p_dw_eintrags_nr INT64
    )
    BEGIN
        INSERT INTO `{JOB_LOG_TABLE}`(job_nr, job_kennung, log_message, log_ts, severity)
        VALUES (
            p_dw_eintrags_nr,
            p_job_kennung,
            'k_ausd_v_ta_barrier_sp: Core kernel logic executed. (Test version)',
            CURRENT_TIMESTAMP(),
            'INFO'
        );
        {fail_logic}
    END;
    """
    bq_client.query(sp_code).result()
    print(f"Deployed test version of {K_AUSD_V_TA_BARRIER_SP} (should_fail={should_fail})")

# Restore original k_ausd_v_ta_barrier_sp after tests
@pytest.fixture(scope="module", autouse=True)
def restore_k_ausd_v_ta_barrier_sp(bq_client):
    """Ensures the original k_ausd_v_ta_barrier_sp is restored after module tests."""
    original_sp_code = """
    CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.k_ausd_v_ta_barrier_sp`(
        IN p_job_kennung STRING,
        IN p_dw_eintrags_nr INT64
    )
    BEGIN
        INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
        VALUES (
            p_dw_eintrags_nr,
            p_job_kennung,
            'k_ausd_v_ta_barrier_sp: Core kernel logic executed. (Placeholder)',
            CURRENT_TIMESTAMP(),
            'INFO'
        );
    END;
    """
    yield # Run tests
    bq_client.query(original_sp_code).result()
    print(f"Restored original {K_AUSD_V_TA_BARRIER_SP}")

```

---

## T1: Successful Execution - Output Parity & Transformation Correctness (Happy Path)

**Purpose:** To verify that the migrated `r_ausd_v_ta_barrier_sp` executes successfully, logs all expected messages, and updates the job status correctly when provided with valid parameters and the kernel SP succeeds. This covers the primary "happy path" flow.

**Setup:**
1.  Ensure `job_log_table` and `job_status_table` are empty.
2.  Ensure `k_ausd_v_ta_barrier_sp` is deployed in its default (non-failing) state.

**Action:**
Call `r_ausd_v_ta_barrier_sp` with valid `p_job_kennung`, `p_s`, and `p_l` parameters.

**Runnable Test Code (Python/Pytest):**

```python
# test_r_ausd_v_ta_barrier.py
import pytest
from google.cloud import bigquery
from .conftest import (
    bq_client, clear_tables_before_each_test, call_bq_procedure, 
    get_log_messages, get_job_status, get_table_data,
    PROJECT_ID, DATASET_ID, R_AUSD_V_TA_BARRIER_SP, JOB_LOG_TABLE, JOB_STATUS_TABLE,
    deploy_test_k_ausd_v_ta_barrier_sp, restore_k_ausd_v_ta_barrier_sp
)

def test_successful_execution_happy_path(bq_client, clear_tables_before_each_test, restore_k_ausd_v_ta_barrier_sp):
    """
    Tests the successful execution of r_ausd_v_ta_barrier_sp.
    Covers output parity (log messages) and transformation correctness (status updates).
    """
    deploy_test_k_ausd_v_ta_barrier_sp(bq_client, should_fail=False) # Ensure kernel succeeds

    test_job_kennung = "TEST_SUCCESS_JOB"
    test_s_param = "value_s"
    test_l_param = "value_l"

    # Action: Call the main stored procedure
    call_bq_procedure(bq_client, R_AUSD_V_TA_BARRIER_SP, {
        "p_job_kennung": test_job_kennung,
        "p_s": test_s_param,
        "p_l": test_l_param
    }).result()

    # Assertions
    # 1. Check job_log_table for expected messages
    log_entries = get_log_messages(bq_client, job_kennung=test_job_kennung)
    log_messages = [msg for msg, _ in log_entries]
    log_severities = [sev for _, sev in log_entries]

    assert len(log_entries) >= 5 # Minimum expected log entries
    assert "Job started. Type: BQ_SCRIPT" in log_messages
    assert "Reference Date (Stichtag) set:" in log_messages
    assert f"Job-Nr: {log_entries[2][0].split(': ')[1].split(',')[0]}" in log_messages # Extract job_nr from banner
    assert "k_ausd_v_ta_barrier_sp: Core kernel logic executed." in log_messages
    assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in log_messages
    assert "Job completed successfully." in log_messages
    
    # All expected messages should be INFO
    assert all(sev == 'INFO' for sev in log_severities)

    # 2. Check job_status_table for final status
    status_entry = get_job_status(bq_client, job_kennung=test_job_kennung)
    assert status_entry == "SUCCESS"

    # 3. Verify job_nr uniqueness (indirectly by checking first log entry's job_nr)
    first_log_entry = get_table_data(bq_client, JOB_LOG_TABLE, order_by="log_ts ASC")[0]
    assert first_log_entry.job_nr is not None
    assert first_log_entry.job_nr > 0 # Should be a positive integer

# Example of how to run this test:
# pytest your_test_file.py
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   The `r_ausd_v_ta_barrier_sp` completes without raising an unhandled error.
    *   `job_log_table` contains at least 5 `INFO` level entries, including:
        *   "Job started. Type: BQ_SCRIPT..."
        *   "Reference Date (Stichtag) set:..."
        *   "Job-Nr: ..., JobKennung: ..., Logdatei: ..." (the banner message)
        *   "k_ausd_v_ta_barrier_sp: Core kernel logic executed."
        *   "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
        *   "Job completed successfully."
    *   `job_status_table` contains an entry for the `test_job_kennung` with `status = 'SUCCESS'`.
    *   The `job_nr` assigned is a positive integer.
*   **Fail:** Any of the above conditions are not met.

---

## T2: Parameter Validation - Missing Required Parameters (`p_s` or `p_l` NULL)

**Purpose:** To verify that the migrated wrapper SP correctly identifies missing mandatory parameters (`p_s` or `p_l`), logs an error using `DWMSG_MeldeFehler_SP`, and raises an exception, preventing the kernel script from executing. This directly tests the `getopts` replacement logic.

**Setup:**
1.  Ensure `job_log_table` and `job_status_table` are empty.
2.  Ensure `k_ausd_v_ta_barrier_sp` is deployed in its default (non-failing) state.

**Action:**
Call `r_ausd_v_ta_barrier_sp` with `p_s` or `p_l` set to `NULL`.

**Runnable Test Code (Python/Pytest):**

```python
# test_r_ausd_v_ta_barrier.py
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest # For BigQuery errors
from .conftest import (
    bq_client, clear_tables_before_each_test, call_bq_procedure, 
    get_log_messages, get_job_status,
    R_AUSD_V_TA_BARRIER_SP, JOB_LOG_TABLE, JOB_STATUS_TABLE,
    deploy_test_k_ausd_v_ta_barrier_sp, restore_k_ausd_v_ta_barrier_sp
)

def test_parameter_validation_missing_s_param(bq_client, clear_tables_before_each_test, restore_k_ausd_v_ta_barrier_sp):
    """
    Tests that r_ausd_v_ta_barrier_sp handles missing 's' parameter correctly.
    Covers transformation correctness (parameter validation, error logging, exception handling).
    """
    deploy_test_k_ausd_v_ta_barrier_sp(bq_client, should_fail=False)

    test_job_kennung = "TEST_MISSING_S_JOB"
    test_l_param = "value_l"

    # Action: Call the main stored procedure with p_s = NULL
    with pytest.raises(BadRequest) as excinfo:
        call_bq_procedure(bq_client, R_AUSD_V_TA_BARRIER_SP, {
            "p_job_kennung": test_job_kennung,
            "p_s": None, # Simulate missing parameter
            "p_l": test_l_param
        }).result()
    
    # Assertions for the raised error
    assert "Parameter Error: 193 - s/l" in str(excinfo.value)

    # 1. Check job_log_table for error message
    log_entries = get_log_messages(bq_client, job_kennung=test_job_kennung)
    log_messages = [msg for msg, _ in log_entries]
    log_severities = [sev for _, sev in log_entries]

    # Expect at least one error message from DWMSG_MeldeFehler_SP
    assert any("Error (E): 193. Argument: s/l" in msg for msg in log_messages)
    assert any(sev == 'ERROR' for sev in log_severities)

    # 2. Check job_status_table for final status (should be FAILED if DWMSG_MeldeFehler_SP updates it)
    status_entry = get_job_status(bq_client, job_kennung=test_job_kennung)
    assert status_entry == "FAILED"

    # 3. Verify kernel script was NOT called (by checking its log message)
    assert "k_ausd_v_ta_barrier_sp: Core kernel logic executed." not in log_messages

def test_parameter_validation_missing_l_param(bq_client, clear_tables_before_each_test, restore_k_ausd_v_ta_barrier_sp):
    """
    Tests that r_ausd_v_ta_barrier_sp handles missing 'l' parameter correctly.
    """
    deploy_test_k_ausd_v_ta_barrier_sp(bq_client, should_fail=False)

    test_job_kennung = "TEST_MISSING_L_JOB"
    test_s_param = "value_s"

    # Action: Call the main stored procedure with p_l = NULL
    with pytest.raises(BadRequest) as excinfo:
        call_bq_procedure(bq_client, R_AUSD_V_TA_BARRIER_SP, {
            "p_job_kennung": test_job_kennung,
            "p_s": test_s_param,
            "p_l": None # Simulate missing parameter
        }).result()
    
    # Assertions for the raised error
    assert "Parameter Error: 193 - s/l" in str(excinfo.value)

    # 1. Check job_log_table for error message
    log_entries = get_log_messages(bq_client, job_kennung=test_job_kennung)
    log_messages = [msg for msg, _ in log_entries]
    log_severities = [sev for _, sev in log_entries]

    assert any("Error (E): 193. Argument: s/l" in msg for msg in log_messages)
    assert any(sev == 'ERROR' for sev in log_severities)

    # 2. Check job_status_table for final status
    status_entry = get_job_status(bq_client, job_kennung=test_job_kennung)
    assert status_entry == "FAILED"

    # 3. Verify kernel script was NOT called
    assert "k_ausd_v_ta_barrier_sp: Core kernel logic executed." not in log_messages
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   The call to `r_ausd_v_ta_barrier_sp` raises a `BadRequest` (or equivalent BigQuery error) containing the message "Parameter Error: 193 - s/l".
    *   `job_log_table` contains an `ERROR` level entry from `DWMSG_MeldeFehler_SP` with message "Error (E): 193. Argument: s/l".
    *   `job_status_table` contains an entry for the `test_job_kennung` with `status = 'FAILED'`.
    *   `job_log_table` does **not** contain the message "k_ausd_v_ta_barrier_sp: Core kernel logic executed.", indicating the kernel was not invoked.
*   **Fail:** Any of the above conditions are not met.

---

## T3: Kernel Script Failure - Error Handling

**Purpose:** To verify that the wrapper SP's `EXCEPTION WHEN ERROR THEN` block correctly catches errors originating from the invoked kernel SP (`k_ausd_v_ta_barrier_sp`), logs the failure using `DWMSG_Fehlerbehandlung_SP`, and updates the job status to 'FAILED'. This tests the BigQuery equivalent of shell `trap ERR`.

**Setup:**
1.  Ensure `job_log_table` and `job_status_table` are empty.
2.  Deploy a test version of `k_ausd_v_ta_barrier_sp` that explicitly raises an error.

**Action:**
Call `r_ausd_v_ta_barrier_sp` with valid parameters, which will then invoke the failing `k_ausd_v_ta_barrier_sp`.

**Runnable Test Code (Python/Pytest):**

```python
# test_r_ausd_v_ta_barrier.py
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from .conftest import (
    bq_client, clear_tables_before_each_test, call_bq_procedure, 
    get_log_messages, get_job_status,
    R_AUSD_V_TA_BARRIER_SP, JOB_LOG_TABLE, JOB_STATUS_TABLE,
    deploy_test_k_ausd_v_ta_barrier_sp, restore_k_ausd_v_ta_barrier_sp
)

def test_kernel_script_failure_error_handling(bq_client, clear_tables_before_each_test, restore_k_ausd_v_ta_barrier_sp):
    """
    Tests that r_ausd_v_ta_barrier_sp correctly handles errors from the kernel SP.
    Covers transformation correctness (error handling, trap replacement) and external system replacements (logging/status).
    """
    # Setup: Deploy a version of k_ausd_v_ta_barrier_sp that will fail
    deploy_test_k_ausd_v_ta_barrier_sp(bq_client, should_fail=True)

    test_job_kennung = "TEST_KERNEL_FAIL_JOB"
    test_s_param = "value_s"
    test_l_param = "value_l"

    # Action: Call the main stored procedure, expecting it to fail due to kernel
    with pytest.raises(BadRequest) as excinfo:
        call_bq_procedure(bq_client, R_AUSD_V_TA_BARRIER_SP, {
            "p_job_kennung": test_job_kennung,
            "p_s": test_s_param,
            "p_l": test_l_param
        }).result()
    
    # Assertions for the raised error
    assert "Simulated kernel failure for testing" in str(excinfo.value)
    assert "AppError: Abbruch" in str(excinfo.value) # From the wrapper's error log

    # 1. Check job_log_table for expected error messages
    log_entries = get_log_messages(bq_client, job_kennung=test_job_kennung)
    log_messages = [msg for msg, _ in log_entries]
    log_severities = [sev for _, sev in log_entries]

    assert "k_ausd_v_ta_barrier_sp: Core kernel logic executed." in log_messages # Kernel was called
    assert any("Job failed due to an unhandled exception." in msg for msg in log_messages) # From DWMSG_Fehlerbehandlung_SP
    assert any("AppError: Abbruch - Simulated kernel failure for testing" in msg for msg in log_messages) # From wrapper's specific error log
    
    # Verify severities
    assert 'ERROR' in log_severities
    assert 'INFO' in log_severities # Initial messages are INFO

    # 2. Check job_status_table for final status
    status_entry = get_job_status(bq_client, job_kennung=test_job_kennung)
    assert status_entry == "FAILED"

    # 3. Verify success messages are NOT present
    assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" not in log_messages
    assert "Job completed successfully." not in log_messages
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   The call to `r_ausd_v_ta_barrier_sp` raises a `BadRequest` (or equivalent BigQuery error) containing the simulated kernel error message and "AppError: Abbruch".
    *   `job_log_table` contains an `INFO` entry indicating `k_ausd_v_ta_barrier_sp` was called.
    *   `job_log_table` contains an `ERROR` level entry from `DWMSG_Fehlerbehandlung_SP` (e.g., "Job failed due to an unhandled exception.")
    *   `job_log_table` contains an `ERROR` level entry from the wrapper's `EXCEPTION` block (e.g., "AppError: Abbruch - ...").
    *   `job_status_table` contains an entry for the `test_job_kennung` with `status = 'FAILED'`.
    *   `job_log_table` does **not** contain the success messages ("Die Abarbeitung wurde ohne erkennbare Fehler beendet", "Job completed successfully.").
*   **Fail:** Any of the above conditions are not met.

---

## T4: `DWMSG_ErmittleNr_SP` - Job Number Generation

**Purpose:** To verify that `DWMSG_ErmittleNr_SP` correctly generates a unique, incrementing job number based on the existing entries in `job_log_table`. This tests a core utility function's logic.

**Setup:**
1.  Ensure `job_log_table` is empty.

**Action:**
1.  Call `DWMSG_ErmittleNr_SP` multiple times.
2.  Optionally, insert some dummy entries into `job_log_table` and then call `DWMSG_ErmittleNr_SP` again.

**Runnable Test Code (Python/Pytest):**

```python
# test_dwmsg_utilities.py
import pytest
from google.cloud import bigquery
from .conftest import (
    bq_client, clear_tables_before_each_test, call_bq_procedure, get_table_data,
    PROJECT_ID, DATASET_ID, DWMSG_ERMITTLENR_SP, JOB_LOG_TABLE
)

DWMSG_ERMITTLENR_SP = f"{PROJECT_ID}.{DATASET_ID}.DWMSG_ErmittleNr_SP" # Define if not in conftest

def test_dwmsg_ermittlenr_sp_generates_unique_incrementing_numbers(bq_client, clear_tables_before_each_test):
    """
    Tests that DWMSG_ErmittleNr_SP generates unique and incrementing job numbers.
    Covers transformation correctness (utility function logic) and data quality (uniqueness).
    """
    # 1. First call on empty table
    result = bq_client.query(f"CALL `{DWMSG_ERMITTLENR_SP}`(p_dw_eintrags_nr => @job_nr); SELECT @job_nr AS job_nr;").result()
    first_job_nr = result.rows[0].job_nr
    assert first_job_nr == 1

    # Simulate an entry being created (as DWMSG_ErzeugeEintrag_SP would do)
    bq_client.query(f"""
        INSERT INTO `{JOB_LOG_TABLE}`(job_nr, job_kennung, log_message, log_ts, severity)
        VALUES ({first_job_nr}, 'TEST_JOB_1', 'Initial entry', CURRENT_TIMESTAMP(), 'INFO');
    """).result()

    # 2. Second call
    result = bq_client.query(f"CALL `{DWMSG_ERMITTLENR_SP}`(p_dw_eintrags_nr => @job_nr); SELECT @job_nr AS job_nr;").result()
    second_job_nr = result.rows[0].job_nr
    assert second_job_nr == first_job_nr + 1

    # Simulate another entry
    bq_client.query(f"""
        INSERT INTO `{JOB_LOG_TABLE}`(job_nr, job_kennung, log_message, log_ts, severity)
        VALUES ({second_job_nr}, 'TEST_JOB_2', 'Second entry', CURRENT_TIMESTAMP(), 'INFO');
    """).result()

    # 3. Third call
    result = bq_client.query(f"CALL `{DWMSG_ERMITTLENR_SP}`(p_dw_eintrags_nr => @job_nr); SELECT @job_nr AS job_nr;").result()
    third_job_nr = result.rows[0].job_nr
    assert third_job_nr == second_job_nr + 1

    # 4. Test with a gap in job_nr (e.g., if a job failed before logging its entry)
    bq_client.query(f"""
        INSERT INTO `{JOB_LOG_TABLE}`(job_nr, job_kennung, log_message, log_ts, severity)
        VALUES (10, 'TEST_JOB_GAP', 'Entry with high job_nr', CURRENT_TIMESTAMP(), 'INFO');
    """).result()
    result = bq_client.query(f"CALL `{DWMSG_ERMITTLENR_SP}`(p_dw_eintrags_nr => @job_nr); SELECT @job_nr AS job_nr;").result()
    next_job_nr_after_gap = result.rows[0].job_nr
    assert next_job_nr_after_gap == 11 # Should pick up from MAX(job_nr) + 1
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   The first call returns `1`.
    *   Subsequent calls return `MAX(job_nr) + 1` from `job_log_table` at the time of the call.
    *   The generated numbers are always positive and unique.
*   **Fail:** The job numbers are not unique or do not follow the `MAX + 1` logic.

---

## T5: `DWMSG_Logdateiname_SP` - Log File Name Generation

**Purpose:** To verify that `DWMSG_Logdateiname_SP` constructs the logical log file name string correctly, incorporating the `job_kennung`, `DW_EintragsNr`, and current date. This tests the string manipulation and date formatting.

**Setup:**
1.  No specific setup beyond standard client.

**Action:**
Call `DWMSG_Logdateiname_SP` with various `job_kennung` and `DW_EintragsNr` values.

**Runnable Test Code (Python/Pytest):**

```python
# test_dwmsg_utilities.py
import pytest
from google.cloud import bigquery
import datetime
from .conftest import (
    bq_client, clear_tables_before_each_test, call_bq_procedure,
    PROJECT_ID, DATASET_ID
)

DWMSG_LOGDATEINAME_SP = f"{PROJECT_ID}.{DATASET_ID}.DWMSG_Logdateiname_SP"

def test_dwmsg_logdateiname_sp_generates_correct_format(bq_client, clear_tables_before_each_test):
    """
    Tests that DWMSG_Logdateiname_SP generates the log file name string in the expected format.
    Covers transformation correctness (utility function logic).
    """
    test_job_kennung = "MY_TEST_JOB"
    test_dw_eintrags_nr = 123
    expected_date_str = datetime.datetime.now().strftime('%Y%m%d')

    # Action: Call the stored procedure
    result = bq_client.query(f"""
        CALL `{DWMSG_LOGDATEINAME_SP}`(p_log_datei => @log_file_name, p_job_kennung => '{test_job_kennung}', p_dw_eintrags_nr => {test_dw_eintrags_nr});
        SELECT @log_file_name AS log_file_name;
    """).result()
    
    generated_log_file_name = result.rows[0].log_file_name

    # Assertions
    expected_pattern = f"log_{test_job_kennung}_{test_dw_eintrags_nr}_{expected_date_str}.jsonl"
    assert generated_log_file_name == expected_pattern

    # Test with different values
    test_job_kennung_2 = "ANOTHER_JOB"
    test_dw_eintrags_nr_2 = 456
    result_2 = bq_client.query(f"""
        CALL `{DWMSG_LOGDATEINAME_SP}`(p_log_datei => @log_file_name, p_job_kennung => '{test_job_kennung_2}', p_dw_eintrags_nr => {test_dw_eintrags_nr_2});
        SELECT @log_file_name AS log_file_name;
    """).result()
    generated_log_file_name_2 = result_2.rows[0].log_file_name
    expected_pattern_2 = f"log_{test_job_kennung_2}_{test_dw_eintrags_nr_2}_{expected_date_str}.jsonl"
    assert generated_log_file_name_2 == expected_pattern_2
```

**Pass/Fail Criterion:**
*   **Pass:** The generated `p_log_datei` string matches the expected format `log_<JOB_KENNUNG>_<DW_EINTRAGSNR>_<YYYYMMDD>.jsonl`.
*   **Fail:** The format or content of the generated string is incorrect.

---

## T6: `DWMSG_SetzeStichtagInfo_SP` - Stichtag Logging

**Purpose:** To verify that `DWMSG_SetzeStichtagInfo_SP` correctly logs the reference date (Stichtag) information into the `job_log_table` with the correct message and severity.

**Setup:**
1.  Ensure `job_log_table` and `job_status_table` are empty.
2.  Insert a dummy entry into `job_status_table` to provide a `job_kennung` for the `DWMSG_SetzeStichtagInfo_SP` to reference.

**Action:**
Call `DWMSG_SetzeStichtagInfo_SP` with a `job_nr`, `stichtag` string, and format.

**Runnable Test Code (Python/Pytest):**

```python
# test_dwmsg_utilities.py
import pytest
from google.cloud import bigquery
import datetime
from .conftest import (
    bq_client, clear_tables_before_each_test, call_bq_procedure, get_log_messages,
    PROJECT_ID, DATASET_ID, DWMSG_SETZESTICHTAGINFO_SP, JOB_STATUS_TABLE
)

DWMSG_SETZESTICHTAGINFO_SP = f"{PROJECT_ID}.{DATASET_ID}.DWMSG_SetzeStichtagInfo_SP"

def test_dwmsg_setzestichtaginfo_sp_logs_correctly(bq_client, clear_tables_before_each_test):
    """
    Tests that DWMSG_SetzeStichtagInfo_SP correctly logs the reference date.
    Covers transformation correctness (utility function logic) and output parity (log content).
    """
    test_job_nr = 99
    test_job_kennung = "STICHTAG_TEST"
    test_stichtag = "31122023"
    test_format = "DDMMYYYY"

    # Setup: Create a dummy job status entry for the SP to reference
    bq_client.query(f"""
        INSERT INTO `{JOB_STATUS_TABLE}`(job_nr, job_kennung, status, last_update_ts)
        VALUES ({test_job_nr}, '{test_job_kennung}', 'RUNNING', CURRENT_TIMESTAMP());
    """).result()

    # Action: Call the stored procedure
    call_bq_procedure(bq_client, DWMSG_SETZESTICHTAGINFO_SP, {
        "p_dw_eintrags_nr": test_job_nr,
        "p_stichtag": test_stichtag,
        "p_format": test_format
    }).result()

    # Assertions
    log_entries = get_log_messages(bq_client, job_nr=test_job_nr, job_kennung=test_job_kennung)
    log_messages = [msg for msg, _ in log_entries]
    log_severities = [sev for _, sev in log_entries]

    expected_message = f"Reference Date (Stichtag) set: {test_stichtag} (Format: {test_format})"
    assert expected_message in log_messages
    assert log_severities[log_messages.index(expected_message)] == 'INFO'
```

**Pass/Fail Criterion:**
*   **Pass:** `job_log_table` contains an `INFO` level entry for the given `job_nr` and `job_kennung` with the message "Reference Date (Stichtag) set: <stichtag> (Format: <format>)".
*   **Fail:** The log entry is missing, has incorrect content, or wrong severity.

---

## T7: Airflow Orchestration - Successful Run

**Purpose:** To verify that the Airflow DAG successfully triggers the BigQuery stored procedure (`r_ausd_v_ta_barrier_sp`) and the overall job completes successfully from an orchestration perspective. This validates the external system replacement for orchestration.

**Setup:**
1.  Ensure the Airflow DAG `r_ausd_v_ta_barrier_orchestration` is deployed and unpaused.
2.  Ensure `job_log_table` and `job_status_table` are empty.
3.  Ensure `k_ausd_v_ta_barrier_sp` is deployed in its default (non-failing) state.
4.  The DAG's parameters (`JOB_KENNUNG_PARAM`, `S_PARAM`, `L_PARAM`) are set to valid values.

**Action:**
Trigger the Airflow DAG `r_ausd_v_ta_barrier_orchestration` manually or wait for its scheduled run.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow task `execute_r_ausd_v_ta_barrier_sp` completes successfully (marked as green in Airflow UI).
    *   `job_log_table` contains all expected `INFO` messages as verified in T1.
    *   `job_status_table` contains an entry for the `JOB_KENNUNG_PARAM` with `status = 'SUCCESS'`.
*   **Fail:**
    *   The Airflow task fails or times out.
    *   The BigQuery tables do not reflect a successful run.

**Note:** This test is typically performed by observing the Airflow UI and querying BigQuery after the DAG run. Automated testing of Airflow DAGs usually involves Airflow's own testing utilities or integration tests that simulate DAG runs. For this document, the criterion focuses on observable outcomes.

---

## T8: Airflow Orchestration - Parameter Error Run

**Purpose:** To verify that the Airflow DAG correctly handles a BigQuery stored procedure failure due to invalid parameters, marking the Airflow task as failed. This validates error propagation from BigQuery back to the orchestrator.

**Setup:**
1.  Ensure the Airflow DAG `r_ausd_v_ta_barrier_orchestration` is deployed and unpaused.
2.  Ensure `job_log_table` and `job_status_table` are empty.
3.  **Modify the Airflow DAG:** Temporarily change the `S_PARAM` or `L_PARAM` in the DAG definition to `None` or an empty string to simulate a missing mandatory parameter for `r_ausd_v_ta_barrier_sp`. Re-deploy the DAG.

**Action:**
Trigger the modified Airflow DAG `r_ausd_v_ta_barrier_orchestration`.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow task `execute_r_ausd_v_ta_barrier_sp` fails (marked as red in Airflow UI).
    *   Airflow logs for the task show the `BadRequest` error message originating from BigQuery's parameter validation (e.g., "Parameter Error: 193 - s/l").
    *   `job_log_table` contains the `ERROR` level entry from `DWMSG_MeldeFehler_SP` and the wrapper's `RAISE` message, as verified in T2.
    *   `job_status_table` contains an entry for the `JOB_KENNUNG_PARAM` with `status = 'FAILED'`.
*   **Fail:**
    *   The Airflow task completes successfully despite the invalid parameters.
    *   The error is not correctly logged or propagated to Airflow.

---

## T9: Schema and Data Quality Assertions for Log/Status Tables

**Purpose:** To verify that the schema of `job_log_table` and `job_status_table` match the DDL specifications and contain valid data types, and that basic data quality (e.g., non-null constraints) is enforced.

**Setup:**
1.  Ensure the DDLs for `job_log_table` and `job_status_table` have been executed.
2.  Run a successful execution of `r_ausd_v_ta_barrier_sp` (e.g., using T1) to populate the tables with data.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA` and perform direct data checks on the tables.

**Runnable Test Code (Python/Pytest):**

```python
# test_schema_data_quality.py
import pytest
from google.cloud import bigquery
from .conftest import (
    bq_client, clear_tables_before_each_test, call_bq_procedure, get_table_data,
    PROJECT_ID, DATASET_ID, JOB_LOG_TABLE, JOB_STATUS_TABLE, R_AUSD_V_TA_BARRIER_SP,
    deploy_test_k_ausd_v_ta_barrier_sp, restore_k_ausd_v_ta_barrier_sp
)

def test_job_log_table_schema_and_data_quality(bq_client, clear_tables_before_each_test, restore_k_ausd_v_ta_barrier_sp):
    """
    Verifies the schema and basic data quality of the job_log_table.
    """
    deploy_test_k_ausd_v_ta_barrier_sp(bq_client, should_fail=False)
    # Populate table with data from a successful run
    call_bq_procedure(bq_client, R_AUSD_V_TA_BARRIER_SP, {
        "p_job_kennung": "SCHEMA_TEST_LOG", "p_s": "s_val", "p_l": "l_val"
    }).result()

    # 1. Schema Assertion
    query_schema = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_log_table'
        ORDER BY ordinal_position;
    """
    schema_result = list(bq_client.query(query_schema).result())
    
    expected_schema = [
        ('job_nr', 'INT64', 'NO'),
        ('job_kennung', 'STRING', 'NO'),
        ('log_message', 'STRING', 'YES'),
        ('log_ts', 'TIMESTAMP', 'NO'),
        ('severity', 'STRING', 'NO'),
    ]
    
    actual_schema = [(row.column_name, row.data_type, row.is_nullable) for row in schema_result]
    assert actual_schema == expected_schema

    # 2. Data Quality Assertion (e.g., non-nulls, valid values)
    # Check for non-null constraints
    query_null_check = f"""
        SELECT COUNT(1) FROM `{JOB_LOG_TABLE}`
        WHERE job_nr IS NULL OR job_kennung IS NULL OR log_ts IS NULL OR severity IS NULL;
    """
    null_count = bq_client.query(query_null_check).result().rows[0][0]
    assert null_count == 0, "Non-nullable columns in job_log_table contain NULLs."

    # Check for valid severity values (assuming 'INFO', 'WARNING', 'ERROR')
    query_severity_check = f"""
        SELECT COUNT(1) FROM `{JOB_LOG_TABLE}`
        WHERE severity NOT IN ('INFO', 'WARNING', 'ERROR');
    """
    invalid_severity_count = bq_client.query(query_severity_check).result().rows[0][0]
    assert invalid_severity_count == 0, "job_log_table contains invalid severity values."

    # Check that job_nr is always positive
    query_job_nr_positive = f"""
        SELECT COUNT(1) FROM `{JOB_LOG_TABLE}`
        WHERE job_nr <= 0;
    """
    non_positive_job_nr_count = bq_client.query(query_job_nr_positive).result().rows[0][0]
    assert non_positive_job_nr_count == 0, "job_log_table contains non-positive job_nr."


def test_job_status_table_schema_and_data_quality(bq_client, clear_tables_before_each_test, restore_k_ausd_v_ta_barrier_sp):
    """
    Verifies the schema and basic data quality of the job_status_table.
    """
    deploy_test_k_ausd_v_ta_barrier_sp(bq_client, should_fail=False)
    # Populate table with data from a successful run
    call_bq_procedure(bq_client, R_AUSD_V_TA_BARRIER_SP, {
        "p_job_kennung": "SCHEMA_TEST_STATUS", "p_s": "s_val", "p_l": "l_val"
    }).result()

    # 1. Schema Assertion
    query_schema = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_status_table'
        ORDER BY ordinal_position;
    """
    schema_result = list(bq_client.query(query_schema).result())
    
    expected_schema = [
        ('job_nr', 'INT64', 'NO'),
        ('job_kennung', 'STRING', 'NO'),
        ('status', 'STRING', 'NO'),
        ('last_update_ts', 'TIMESTAMP', 'NO'),
    ]
    
    actual_schema = [(row.column_name, row.data_type, row.is_nullable) for row in schema_result]
    assert actual_schema == expected_schema

    # 2. Data Quality Assertion
    # Check for non-null constraints
    query_null_check = f"""
        SELECT COUNT(1) FROM `{JOB_STATUS_TABLE}`
        WHERE job_nr IS NULL OR job_kennung IS NULL OR status IS NULL OR last_update_ts IS NULL;
    """
    null_count = bq_client.query(query_null_check).result().rows[0][0]
    assert null_count == 0, "Non-nullable columns in job_status_table contain NULLs."

    # Check for valid status values (assuming 'RUNNING', 'SUCCESS', 'FAILED')
    query_status_check = f"""
        SELECT COUNT(1) FROM `{JOB_STATUS_TABLE}`
        WHERE status NOT IN ('RUNNING', 'SUCCESS', 'FAILED');
    """
    invalid_status_count = bq_client.query(query_status_check).result().rows[0][0]
    assert invalid_status_count == 0, "job_status_table contains invalid status values."
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   The `INFORMATION_SCHEMA` queries return the expected column names, data types, and nullability for both `job_log_table` and `job_status_table`.
    *   No `NULL` values are found in columns defined as `NOT NULL`.
    *   `severity` column in `job_log_table` only contains expected values ('INFO', 'WARNING', 'ERROR').
    *   `status` column in `job_status_table` only contains expected values ('RUNNING', 'SUCCESS', 'FAILED').
    *   `job_nr` in both tables are positive integers.
*   **Fail:** Any of the above schema or data quality checks fail.