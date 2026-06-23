As a senior data-migration QA engineer, I've analyzed the migration design and the provided BigQuery code for `r_ausd_v_ta_p_discount_rr.ksh`. The original script is an orchestration wrapper, and the migration focuses on replicating its environment setup, parameter handling, logging, error trapping, and core script invocation within BigQuery.

The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery stored procedure (`project.dataset.vertragsdatenabgleich_wrapper`) and its associated logging/utility procedures and tables, against the legacy KornShell script.

---

## Test Setup Prerequisites

Before running the tests, ensure the following:

1.  **BigQuery Environment:** A BigQuery project and dataset (`project.dataset` in the DDLs) are configured and accessible.
2.  **DDL Deployment:** All DDLs for the audit/log tables (`job_error_log`, `job_log`, `job_status`, `job_stichtag`, `job_entry_sequence`) have been executed in the target BigQuery dataset.
3.  **Utility Procedure Deployment:** All `dwmsg_` procedures (`dwmsg_ermittlenr`, `dwmsg_erzeugeeintrag`, `dwmsg_setzestichtaginfo`, `dwmsg_meldefehler`, `dwmsg_setzestatusok`) have been created in the target BigQuery dataset.
4.  **Core Logic Placeholder Deployment:** The `project.dataset.k_ausd_v_ta_p_discount_rr` placeholder procedure has been created.
5.  **Wrapper Procedure Deployment:** The `project.dataset.vertragsdatenabgleich_wrapper` procedure has been created.
6.  **Python Environment:** `pytest` and `google-cloud-bigquery` library are installed.
7.  **Environment Variables:** `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET_ID` are set for the Python tests.

---

## Pytest Helper Functions

The following Python helper functions will be used across the test cases.

```python
import pytest
from google.cloud import bigquery
import time
import os
from datetime import date

# Configuration
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "your_dataset")
WRAPPER_PROC = f"{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper"
CORE_PROC = f"{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_p_discount_rr"
DWMSG_ERZEUGEEINTRAG_PROC = f"{PROJECT_ID}.{DATASET_ID}.dwmsg_erzeugeeintrag"
DWMSG_MELDEFEHLER_PROC = f"{PROJECT_ID}.{DATASET_ID}.dwmsg_meldefehler"


@pytest.fixture(scope="module")
def bigquery_client():
    """Provides a BigQuery client for tests."""
    return bigquery.Client(project=PROJECT_ID)

def clear_log_tables(client, job_kennung):
    """Clears all relevant log tables for a given job_kennung."""
    tables = [
        "job_error_log",
        "job_log",
        "job_status",
        "job_stichtag",
        "job_entry_sequence",
    ]
    for table in tables:
        query = f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.{table} WHERE job_kennung = '{job_kennung}'"
        client.query(query).result()
    # Ensure job_entry_sequence is reset for the job_kennung
    query = f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.job_entry_sequence WHERE job_kennung = '{job_kennung}'"
    client.query(query).result()


def get_log_entries(client, table_name, job_kennung, entry_nr=None):
    """Fetches log entries from a specific table."""
    query = f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{table_name} WHERE job_kennung = '{job_kennung}'"
    if entry_nr is not None:
        query += f" AND entry_nr = {entry_nr}"
    query += " ORDER BY log_timestamp ASC"
    rows = client.query(query).result()
    return list(rows)

def call_wrapper_procedure(client, job_kennung=None, stichtag=None, help_flag=False):
    """Calls the BigQuery wrapper procedure with specified parameters."""
    params = []
    if job_kennung is not None:
        params.append(f"p_job_kennung => '{job_kennung}'")
    if stichtag is not None:
        params.append(f"p_stichtag => '{stichtag}'")
    if help_flag:
        params.append("p_help => TRUE")

    param_str = ", ".join(params)
    query = f"CALL {WRAPPER_PROC}({param_str})"
    
    # For help_flag, the procedure returns SELECT statements and then RETURN.
    # The CALL itself will succeed without error and return no rows.
    # We'll check for no log entries in the pass/fail criterion.
    
    return client.query(query).result()

def modify_core_proc_behavior(client, should_fail=False, error_message="Simulated core script error"):
    """Modifies the k_ausd_v_ta_p_discount_rr procedure to either succeed or fail."""
    if should_fail:
        core_proc_code = f"""
        CREATE OR REPLACE PROCEDURE {CORE_PROC}(
            IN p_job_kennung STRING,
            IN p_entry_nr INT64,
            IN p_stichtag DATE
        )
        BEGIN
            CALL {DWMSG_ERZEUGEEINTRAG_PROC}(
                p_job_kennung,
                p_entry_nr,
                'Core script k_ausd_v_ta_p_discount_rr started (will fail).',
                'INFO'
            );
            RAISE_ERROR('{error_message}');
        EXCEPTION WHEN ERROR THEN
            CALL {DWMSG_MELDEFEHLER_PROC}(
                p_job_kennung,
                p_entry_nr,
                'Error in k_ausd_v_ta_p_discount_rr: ' || ERROR_MESSAGE(),
                'CORE',
                'BQ_CORE_SCRIPT_ERROR'
            );
            RAISE;
        END;
        """
    else:
        core_proc_code = f"""
        CREATE OR REPLACE PROCEDURE {CORE_PROC}(
            IN p_job_kennung STRING,
            IN p_entry_nr INT64,
            IN p_stichtag DATE
        )
        BEGIN
            CALL {DWMSG_ERZEUGEEINTRAG_PROC}(
                p_job_kennung,
                p_entry_nr,
                'Core script k_ausd_v_ta_p_discount_rr executed (placeholder).',
                'INFO'
            );
        EXCEPTION WHEN ERROR THEN
            CALL {DWMSG_MELDEFEHLER_PROC}(
                p_job_kennung,
                p_entry_nr,
                'Error in k_ausd_v_ta_p_discount_rr: ' || ERROR_MESSAGE(),
                'CORE',
                'BQ_CORE_SCRIPT_ERROR'
            );
            RAISE;
        END;
        """
    client.query(core_proc_code).result()

@pytest.fixture(autouse=True)
def reset_core_proc_behavior(bigquery_client):
    """Ensures the core procedure is reset to a non-failing state before each test."""
    modify_core_proc_behavior(bigquery_client, should_fail=False)
    yield
    # No specific teardown needed for the core proc, as it's reset before each test.

```

---

## Test Cases

### Test Case 1: Happy Path Execution - Default Parameters

*   **Purpose:** Verify that the `vertragsdatenabgleich_wrapper` procedure executes successfully using its default `job_kennung` and `stichtag`, correctly populating all logging tables and invoking the core procedure. This covers output parity for logging and transformation correctness for default parameter handling.
*   **Setup:**
    *   Clear all log tables for `BERT_V_TA_P_DISCOUNT_RR`.
    *   Ensure `k_ausd_v_ta_p_discount_rr` is configured to succeed (default behavior of `reset_core_proc_behavior` fixture).
*   **Action:**
    *   Call `project.dataset.vertragsdatenabgleich_wrapper()` without any parameters.
*   **Pass/Fail Criterion:**
    *   The procedure call completes without raising an error.
    *   `job_entry_sequence` table contains one entry for `BERT_V_TA_P_DISCOUNT_RR` with `entry_nr = 1`.
    *   `job_log` table contains at least 3 entries for `BERT_V_TA_P_DISCOUNT_RR` and `entry_nr = 1`, including messages like "Job start", "Core script ... executed", and "Job completed successfully".
    *   `job_status` table contains two entries for `BERT_V_TA_P_DISCOUNT_RR` and `entry_nr = 1`: one with `status = 'STARTED'` and one with `status = 'COMPLETED'`.
    *   `job_stichtag` table contains one entry for `BERT_V_TA_P_DISCOUNT_RR` and `entry_nr = 1`, with `stichtag` equal to today's date.
    *   `job_error_log` table contains no entries for `BERT_V_TA_P_DISCOUNT_RR` and `entry_nr = 1`.

```python
def test_happy_path_default_parameters(bigquery_client):
    job_kennung = "BERT_V_TA_P_DISCOUNT_RR"
    clear_log_tables(bigquery_client, job_kennung)
    
    # Action
    call_wrapper_procedure(bigquery_client)
    
    # Assertions
    entry_seq_rows = get_log_entries(bigquery_client, "job_entry_sequence", job_kennung)
    assert len(entry_seq_rows) == 1
    assert entry_seq_rows[0].entry_nr == 1

    job_log_rows = get_log_entries(bigquery_client, "job_log", job_kennung, entry_nr=1)
    assert len(job_log_rows) >= 3 # Start, Core, Completed
    assert any("Job start" in r.log_message for r in job_log_rows)
    assert any("Core script k_ausd_v_ta_p_discount_rr executed" in r.log_message for r in job_log_rows)
    assert any("Job completed successfully." in r.log_message for r in job_log_rows)

    job_status_rows = get_log_entries(bigquery_client, "job_status", job_kennung, entry_nr=1)
    assert len(job_status_rows) == 2
    assert any(r.status == 'STARTED' for r in job_status_rows)
    assert any(r.status == 'COMPLETED' for r in job_status_rows)

    job_stichtag_rows = get_log_entries(bigquery_client, "job_stichtag", job_kennung, entry_nr=1)
    assert len(job_stichtag_rows) == 1
    assert job_stichtag_rows[0].stichtag == date.today()

    job_error_log_rows = get_log_entries(bigquery_client, "job_error_log", job_kennung, entry_nr=1)
    assert len(job_error_log_rows) == 0
```

### Test Case 2: Happy Path Execution - Custom Parameters

*   **Purpose:** Verify that the wrapper correctly accepts and uses custom `p_job_kennung` and `p_stichtag` values, reflecting them accurately in all logging tables and when invoking the core procedure. This validates parameter handling and output parity.
*   **Setup:**
    *   Clear all log tables for `CUSTOM_JOB_ID`.
    *   Ensure `k_ausd_v_ta_p_discount_rr` is configured to succeed.
*   **Action:**
    *   Call `project.dataset.vertragsdatenabgleich_wrapper(p_job_kennung => 'CUSTOM_JOB_ID', p_stichtag => '2023-01-15')`.
*   **Pass/Fail Criterion:**
    *   The procedure call completes without raising an error.
    *   `job_entry_sequence` table contains one entry for `CUSTOM_JOB_ID` with `entry_nr = 1`.
    *   `job_log` table contains at least 3 entries for `CUSTOM_JOB_ID` and `entry_nr = 1`, with messages reflecting the custom job ID and successful execution.
    *   `job_status` table contains 'STARTED' and 'COMPLETED' entries for `CUSTOM_JOB_ID` and `entry_nr = 1`.
    *   `job_stichtag` table contains one entry for `CUSTOM_JOB_ID` and `entry_nr = 1`, with `stichtag` equal to `2023-01-15`.
    *   `job_error_log` table contains no entries for `CUSTOM_JOB_ID` and `entry_nr = 1`.

```python
def test_happy_path_custom_parameters(bigquery_client):
    job_kennung = "CUSTOM_JOB_ID"
    custom_stichtag = date(2023, 1, 15)
    clear_log_tables(bigquery_client, job_kennung)
    
    # Action
    call_wrapper_procedure(bigquery_client, job_kennung=job_kennung, stichtag=str(custom_stichtag))
    
    # Assertions
    entry_seq_rows = get_log_entries(bigquery_client, "job_entry_sequence", job_kennung)
    assert len(entry_seq_rows) == 1
    assert entry_seq_rows[0].entry_nr == 1

    job_log_rows = get_log_entries(bigquery_client, "job_log", job_kennung, entry_nr=1)
    assert len(job_log_rows) >= 3
    assert any(f"Job start: {job_kennung}" in r.log_message for r in job_log_rows)
    assert any("Core script k_ausd_v_ta_p_discount_rr executed" in r.log_message for r in job_log_rows)
    assert any("Job completed successfully." in r.log_message for r in job_log_rows)

    job_status_rows = get_log_entries(bigquery_client, "job_status", job_kennung, entry_nr=1)
    assert len(job_status_rows) == 2
    assert any(r.status == 'STARTED' and f"Stichtag: {custom_stichtag}" in r.message for r in job_status_rows)
    assert any(r.status == 'COMPLETED' for r in job_status_rows)

    job_stichtag_rows = get_log_entries(bigquery_client, "job_stichtag", job_kennung, entry_nr=1)
    assert len(job_stichtag_rows) == 1
    assert job_stichtag_rows[0].stichtag == custom_stichtag

    job_error_log_rows = get_log_entries(bigquery_client, "job_error_log", job_kennung, entry_nr=1)
    assert len(job_error_log_rows) == 0
```

### Test Case 3: Help Message Display

*   **Purpose:** Verify that calling the wrapper with `p_help=TRUE` displays the usage message and exits cleanly without performing any job execution or logging, mimicking the `-h` flag behavior of the legacy script. This covers output parity and external-system replacement (no logging to BQ tables).
*   **Setup:**
    *   Note the current state of log tables (or ensure they are empty for a specific `job_kennung`).
    *   Choose a `job_kennung` that is unlikely to have existing entries (e.g., `HELP_JOB`).
*   **Action:**
    *   Call `project.dataset.vertragsdatenabgleich_wrapper(p_help => TRUE)`.
*   **Pass/Fail Criterion:**
    *   The procedure call completes without raising an error.
    *   No new entries are added to `job_log`, `job_status`, `job_error_log`, `job_stichtag`, or `job_entry_sequence` for any `job_kennung` (especially `HELP_JOB` or the default `BERT_V_TA_P_DISCOUNT_RR`).
    *   (Optional, if BigQuery procedures could return structured output for help): The procedure's output contains the expected usage message. (Note: BigQuery `CALL` statements typically don't return results directly for `SELECT` statements within the procedure that are not part of a `CREATE TABLE AS SELECT` or similar. The current BQ code just prints to the console via `SELECT` and returns, so we primarily check for *absence* of side effects).

```python
def test_help_message_display(bigquery_client):
    job_kennung = "HELP_JOB" # Use a distinct job_kennung to ensure no accidental logging
    clear_log_tables(bigquery_client, job_kennung)
    clear_log_tables(bigquery_client, "BERT_V_TA_P_DISCOUNT_RR") # Also clear default

    # Action
    call_wrapper_procedure(bigquery_client, help_flag=True)
    
    # Assertions
    # Verify no log entries were created for the default job_kennung
    assert len(get_log_entries(bigquery_client, "job_log", "BERT_V_TA_P_DISCOUNT_RR")) == 0
    assert len(get_log_entries(bigquery_client, "job_status", "BERT_V_TA_P_DISCOUNT_RR")) == 0
    assert len(get_log_entries(bigquery_client, "job_error_log", "BERT_V_TA_P_DISCOUNT_RR")) == 0
    assert len(get_log_entries(bigquery_client, "job_stichtag", "BERT_V_TA_P_DISCOUNT_RR")) == 0
    assert len(get_log_entries(bigquery_client, "job_entry_sequence", "BERT_V_TA_P_DISCOUNT_RR")) == 0

    # Verify no log entries were created for the specific HELP_JOB job_kennung
    assert len(get_log_entries(bigquery_client, "job_log", job_kennung)) == 0
    assert len(get_log_entries(bigquery_client, "job_status", job_kennung)) == 0
    assert len(get_log_entries(bigquery_client, "job_error_log", job_kennung)) == 0
    assert len(get_log_entries(bigquery_client, "job_stichtag", job_kennung)) == 0
    assert len(get_log_entries(bigquery_client, "job_entry_sequence", job_kennung)) == 0
```

### Test Case 4: Error Handling - Core Script Failure

*   **Purpose:** Verify that if the core script (`k_ausd_v_ta_p_discount_rr`) fails, the wrapper correctly catches the error, logs it to `job_error_log`, updates the job status to 'FAILED', and re-raises the error to the caller, mimicking the `trap ERR` behavior. This covers transformation correctness for error handling and external-system replacement for error logging.
*   **Setup:**
    *   Clear all log tables for `BERT_V_TA_P_DISCOUNT_RR`.
    *   Modify `k_ausd_v_ta_p_discount_rr` to `RAISE_ERROR` immediately.
*   **Action:**
    *   Attempt to call `project.dataset.vertragsdatenabgleich_wrapper()`.
*   **Pass/Fail Criterion:**
    *   The call to `vertragsdatenabgleich_wrapper` *raises an error* (e.g., `google.api_core.exceptions.InternalServerError` or similar BigQuery error).
    *   `job_error_log` table contains one entry for `BERT_V_TA_P_DISCOUNT_RR` with `entry_nr = 1`, `component = 'WRAPPER'` (as the wrapper catches and re-logs the core error), and an error message indicating the failure.
    *   `job_status` table contains two entries for `BERT_V_TA_P_DISCOUNT_RR` and `entry_nr = 1`: one with `status = 'STARTED'` and one with `status = 'FAILED'`.
    *   `job_log` table contains "Job start" and "Core script ... started (will fail)" but *not* "Job completed successfully".

```python
def test_error_handling_core_script_failure(bigquery_client):
    job_kennung = "BERT_V_TA_P_DISCOUNT_RR"
    clear_log_tables(bigquery_client, job_kennung)
    
    # Setup: Modify core procedure to fail
    error_msg = "Simulated core script error for testing"
    modify_core_proc_behavior(bigquery_client, should_fail=True, error_message=error_msg)

    # Action: Expect an error to be raised
    with pytest.raises(Exception) as excinfo: # Catch generic exception as BQ client wraps it
        call_wrapper_procedure(bigquery_client)
    
    # Assert that the error message contains the simulated error
    assert error_msg in str(excinfo.value)

    # Assertions for logging
    entry_seq_rows = get_log_entries(bigquery_client, "job_entry_sequence", job_kennung)
    assert len(entry_seq_rows) == 1
    assert entry_seq_rows[0].entry_nr == 1

    job_log_rows = get_log_entries(bigquery_client, "job_log", job_kennung, entry_nr=1)
    assert any("Job start" in r.log_message for r in job_log_rows)
    assert any("Core script k_ausd_v_ta_p_discount_rr started (will fail)." in r.log_message for r in job_log_rows)
    assert not any("Job completed successfully." in r.log_message for r in job_log_rows)

    job_status_rows = get_log_entries(bigquery_client, "job_status", job_kennung, entry_nr=1)
    assert len(job_status_rows) == 2
    assert any(r.status == 'STARTED' for r in job_status_rows)
    assert any(r.status == 'FAILED' and "Job failed" in r.message for r in job_status_rows)

    job_error_log_rows = get_log_entries(bigquery_client, "job_error_log", job_kennung, entry_nr=1)
    assert len(job_error_log_rows) == 1
    assert job_error_log_rows[0].component == 'WRAPPER' # Wrapper logs its own error after catching core's
    assert "Job failed: " in job_error_log_rows[0].error_message
    assert error_msg in job_error_log_rows[0].error_message
```

### Test Case 5: Sequential Job Number Generation & Atomicity

*   **Purpose:** Verify that `v_entry_nr` (migrated from `DW_EintragsNr`) is correctly incremented and unique for each run of a given `job_kennung`, and that the `MERGE INTO` statement in `dwmsg_ermittlenr` provides atomic sequence generation. This covers data quality and transformation correctness for sequence generation.
*   **Setup:**
    *   Clear all log tables for `BERT_V_TA_P_DISCOUNT_RR`.
    *   Ensure `k_ausd_v_ta_p_discount_rr` is configured to succeed.
*   **Action:**
    *   Call `project.dataset.vertragsdatenabgleich_wrapper()` three times sequentially.
*   **Pass/Fail Criterion:**
    *   All three procedure calls complete without error.
    *   `job_entry_sequence` table for `BERT_V_TA_P_DISCOUNT_RR` shows `entry_nr = 3`.
    *   Each of the three runs' log entries across `job_log`, `job_status`, and `job_stichtag` are associated with unique and sequential `entry_nr` values (1, 2, and 3 respectively). There should be no gaps or duplicates in the `entry_nr` values for `BERT_V_TA_P_DISCOUNT_RR`.

```python
def test_sequential_job_number_generation(bigquery_client):
    job_kennung = "BERT_V_TA_P_DISCOUNT_RR"
    clear_log_tables(bigquery_client, job_kennung)
    
    # Action: Call the wrapper multiple times
    call_wrapper_procedure(bigquery_client) # Run 1
    call_wrapper_procedure(bigquery_client) # Run 2
    call_wrapper_procedure(bigquery_client) # Run 3
    
    # Assertions
    entry_seq_rows = get_log_entries(bigquery_client, "job_entry_sequence", job_kennung)
    assert len(entry_seq_rows) == 1
    assert entry_seq_rows[0].entry_nr == 3 # Last entry_nr should be 3

    # Verify log entries for each run
    for i in range(1, 4): # entry_nr 1, 2, 3
        job_log_rows = get_log_entries(bigquery_client, "job_log", job_kennung, entry_nr=i)
        assert len(job_log_rows) >= 3
        assert any(f"Job start: {job_kennung} with entry_nr: {i}" in r.log_message for r in job_log_rows)
        assert any("Job completed successfully." in r.log_message for r in job_log_rows)

        job_status_rows = get_log_entries(bigquery_client, "job_status", job_kennung, entry_nr=i)
        assert len(job_status_rows) == 2
        assert any(r.status == 'STARTED' for r in job_status_rows)
        assert any(r.status == 'COMPLETED' for r in job_status_rows)

        job_stichtag_rows = get_log_entries(bigquery_client, "job_stichtag", job_kennung, entry_nr=i)
        assert len(job_stichtag_rows) == 1
        assert job_stichtag_rows[0].stichtag == date.today()

        job_error_log_rows = get_log_entries(bigquery_client, "job_error_log", job_kennung, entry_nr=i)
        assert len(job_error_log_rows) == 0
```

### Test Case 6: `JobKennung` Uppercasing (Behavioral Difference)

*   **Purpose:** Verify that the BigQuery wrapper handles the `p_job_kennung` parameter as-is, unlike the original KornShell script which explicitly forced `JobKennung` to uppercase using `typeset -u`. This highlights a potential behavioral difference that might need to be documented or aligned if strict parity is required.
*   **Setup:**
    *   Clear all log tables for `lower_case_job_id`.
    *   Ensure `k_ausd_v_ta_p_discount_rr` is configured to succeed.
*   **Action:**
    *   Call `project.dataset.vertragsdatenabgleich_wrapper(p_job_kennung => 'lower_case_job_id')`.
*   **Pass/Fail Criterion:**
    *   The procedure call completes without raising an error.
    *   All log entries across `job_log`, `job_status`, `job_error_log`, `job_stichtag`, and `job_entry_sequence` for this run use `job_kennung = 'lower_case_job_id'` (lowercase), not `'LOWER_CASE_JOB_ID'`.
    *   **Note on Behavioral Difference:** The BigQuery procedure preserves the case of the input `p_job_kennung`, whereas the legacy KornShell script would have forced it to uppercase. This is a deliberate design choice in BigQuery (string types are case-sensitive by default) and should be noted.

```python
def test_job_kennung_casing(bigquery_client):
    job_kennung = "lower_case_job_id"
    clear_log_tables(bigquery_client, job_kennung)
    
    # Action
    call_wrapper_procedure(bigquery_client, job_kennung=job_kennung)
    
    # Assertions
    entry_seq_rows = get_log_entries(bigquery_client, "job_entry_sequence", job_kennung)
    assert len(entry_seq_rows) == 1
    assert entry_seq_rows[0].job_kennung == job_kennung # Verify case is preserved

    job_log_rows = get_log_entries(bigquery_client, "job_log", job_kennung, entry_nr=1)
    assert len(job_log_rows) >= 3
    assert all(r.job_kennung == job_kennung for r in job_log_rows)

    job_status_rows = get_log_entries(bigquery_client, "job_status", job_kennung, entry_nr=1)
    assert len(job_status_rows) == 2
    assert all(r.job_kennung == job_kennung for r in job_status_rows)

    job_stichtag_rows = get_log_entries(bigquery_client, "job_stichtag", job_kennung, entry_nr=1)
    assert len(job_stichtag_rows) == 1
    assert job_stichtag_rows[0].job_kennung == job_kennung

    job_error_log_rows = get_log_entries(bigquery_client, "job_error_log", job_kennung, entry_nr=1)
    assert len(job_error_log_rows) == 0
```