As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `r_ausd_v_ta_p_discount.ksh` to BigQuery stored procedures. Given that the core logic (`k_ausd_v_ta_p_discount.ksh`) is currently a placeholder, these tests focus primarily on the orchestration wrapper's behavior, logging, parameter handling, and error management.

The tests are structured to cover output parity (via logging tables), transformation correctness (of the orchestration logic), external system replacements (internal shell scripts replaced by BQ logic), and data quality/schema assertions for the new logging infrastructure.

---

## Migration Validation Tests: `r_ausd_v_ta_p_discount.ksh` to BigQuery

**Target BigQuery Project & Dataset:** `project.dataset` (replace with actual values)

**Pre-requisites:**
1.  The DDLs for `project.dataset.dw_job_log`, `project.dataset.dw_error_log`, and `project.dataset.dw_job_context` have been executed.
2.  The stored procedures `project.dataset.sp_k_ausd_v_ta_p_discount` and `project.dataset.sp_bert_v_ta_p_discount` (including the modifications for testing error scenarios as described in the thought process) have been deployed.
3.  A Python environment with `pytest` and `google-cloud-bigquery` client library is set up and authenticated to access the BigQuery project.

---

### Test Setup (Pytest Fixtures and Helpers)

```python
import pytest
from google.cloud import bigquery
import time
import datetime

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP project ID
DATASET_ID = "your_dataset_id"      # Replace with your BigQuery dataset ID
JOB_KENNUNG = "r_ausd_v_ta_p_discount"
SP_BERT_NAME = f"{PROJECT_ID}.{DATASET_ID}.sp_bert_v_ta_p_discount"
DW_JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.dw_job_log"
DW_ERROR_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.dw_error_log"
DW_JOB_CONTEXT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.dw_job_context"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    client = bigquery.Client(project=PROJECT_ID)
    return client

@pytest.fixture(autouse=True)
def clear_log_tables(bq_client):
    """Clears log tables before each test to ensure isolation."""
    bq_client.query(f"TRUNCATE TABLE {DW_JOB_LOG_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {DW_ERROR_LOG_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {DW_JOB_CONTEXT_TABLE}").result()
    time.sleep(1) # Give BigQuery a moment to process
    yield

def call_sp_bert(bq_client, p_h=False, p_s=None, p_l=None, p_simulate_core_error=False):
    """Helper function to call the main stored procedure."""
    params = []
    params.append(f"p_h => {str(p_h).upper()}")
    if p_s is not None:
        params.append(f"p_s => '{p_s}'")
    if p_l is not None:
        params.append(f"p_l => '{p_l}'")
    params.append(f"p_simulate_core_error => {str(p_simulate_core_error).upper()}")

    query = f"CALL {SP_BERT_NAME}({', '.join(params)})"
    print(f"Executing: {query}")
    try:
        job = bq_client.query(query)
        job.result() # Wait for the job to complete
        return True, None
    except Exception as e:
        return False, str(e)

def get_job_log_entries(bq_client, job_kennung=JOB_KENNUNG, dw_eintrags_nr=None):
    """Fetches job log entries."""
    query = f"SELECT * FROM {DW_JOB_LOG_TABLE} WHERE job_kennung = '{job_kennung}'"
    if dw_eintrags_nr is not None:
        query += f" AND dw_eintrags_nr = {dw_eintrags_nr}"
    query += " ORDER BY start_timestamp ASC"
    rows = list(bq_client.query(query).result())
    return rows

def get_error_log_entries(bq_client, job_kennung=JOB_KENNUNG, dw_eintrags_nr=None):
    """Fetches error log entries."""
    query = f"SELECT * FROM {DW_ERROR_LOG_TABLE} WHERE job_kennung = '{job_kennung}'"
    if dw_eintrags_nr is not None:
        query += f" AND dw_eintrags_nr = {dw_eintrags_nr}"
    query += " ORDER BY error_timestamp ASC"
    rows = list(bq_client.query(query).result())
    return rows

def get_job_context_entries(bq_client, job_kennung=JOB_KENNUNG, dw_eintrags_nr=None):
    """Fetches job context entries."""
    query = f"SELECT * FROM {DW_JOB_CONTEXT_TABLE} WHERE job_kennung = '{job_kennung}'"
    if dw_eintrags_nr is not None:
        query += f" AND dw_eintrags_nr = {dw_eintrags_nr}"
    query += " ORDER BY creation_timestamp ASC"
    rows = list(bq_client.query(query).result())
    return rows

def get_last_dw_eintrags_nr(bq_client, job_kennung=JOB_KENNUNG):
    """Gets the last DW_EintragsNr for a given job_kennung."""
    query = f"SELECT MAX(dw_eintrags_nr) FROM {DW_JOB_LOG_TABLE} WHERE job_kennung = '{job_kennung}'"
    row = list(bq_client.query(query).result())
    return row[0][0] if row and row[0][0] is not None else 0

```

---

### Test Case 1: Successful Execution (Happy Path)

*   **Purpose**: Verify that the `sp_bert_v_ta_p_discount` stored procedure executes successfully without errors, correctly logs its start and end status, and invokes the core logic procedure. This covers output parity for the logging tables and basic transformation correctness of the orchestration.
*   **Setup**: Ensure all logging tables are empty.
*   **Action**: Call `project.dataset.sp_bert_v_ta_p_discount()` with default parameters.
*   **Pass/Fail Criterion**:
    *   The stored procedure call completes without raising an exception.
    *   `dw_job_log` contains two entries for `JOB_KENNUNG`:
        *   One with `status = 'RUNNING'` and `message = 'Job orchestration started successfully.'`.
        *   One with `status = 'OK'` and `message = 'Job orchestration and core synchronization completed successfully.'`.
        *   Both entries should have the same `dw_eintrags_nr`.
    *   `dw_job_context` contains one entry for `JOB_KENNUNG` with the correct `stichtag` (current date) and `dw_eintrags_nr`.
    *   `dw_error_log` contains zero entries for `JOB_KENNUNG`.
    *   The `sp_k_ausd_v_ta_p_discount` (core logic) should have logged its execution in `dw_job_log` with `prog_name = 'sp_k_ausd_v_ta_p_discount'` and `status = 'RUNNING'`.

```python
def test_successful_execution(bq_client):
    success, error_msg = call_sp_bert(bq_client)
    assert success, f"Stored procedure call failed: {error_msg}"

    job_logs = get_job_log_entries(bq_client)
    assert len(job_logs) == 3, f"Expected 3 job log entries (wrapper start, core start, wrapper end), got {len(job_logs)}"

    # Verify wrapper start log
    wrapper_start_log = job_logs[0]
    assert wrapper_start_log.job_kennung == JOB_KENNUNG
    assert wrapper_start_log.prog_name == 'sp_bert_v_ta_p_discount'
    assert wrapper_start_log.status == 'RUNNING'
    assert 'started successfully' in wrapper_start_log.message
    dw_eintrags_nr = wrapper_start_log.dw_eintrags_nr

    # Verify core script log
    core_log = job_logs[1]
    assert core_log.job_kennung == JOB_KENNUNG
    assert core_log.prog_name == 'sp_k_ausd_v_ta_p_discount'
    assert core_log.status == 'RUNNING'
    assert 'Core kernel logic placeholder' in core_log.message
    assert core_log.dw_eintrags_nr == dw_eintrags_nr

    # Verify wrapper end log
    wrapper_end_log = job_logs[2]
    assert wrapper_end_log.job_kennung == JOB_KENNUNG
    assert wrapper_end_log.prog_name == 'sp_bert_v_ta_p_discount'
    assert wrapper_end_log.status == 'OK'
    assert 'completed successfully' in wrapper_end_log.message
    assert wrapper_end_log.dw_eintrags_nr == dw_eintrags_nr
    assert wrapper_end_log.start_timestamp is not None
    assert wrapper_end_log.end_timestamp is not None
    assert wrapper_end_log.end_timestamp > wrapper_end_log.start_timestamp

    # Verify job context
    job_context = get_job_context_entries(bq_client, dw_eintrags_nr=dw_eintrags_nr)
    assert len(job_context) == 1, f"Expected 1 job context entry, got {len(job_context)}"
    assert job_context[0].job_kennung == JOB_KENNUNG
    assert job_context[0].dw_eintrags_nr == dw_eintrags_nr
    assert job_context[0].stichtag == datetime.date.today()

    # Verify no errors logged
    error_logs = get_error_log_entries(bq_client)
    assert len(error_logs) == 0, f"Expected 0 error log entries, got {len(error_logs)}"

```

---

### Test Case 2: Parameter Handling - Help Message (`-h`)

*   **Purpose**: Verify that calling the stored procedure with `p_h=TRUE` correctly displays the usage information and exits without performing any job logic or logging. This tests the `usage()` function replacement.
*   **Setup**: Ensure all logging tables are empty.
*   **Action**: Call `project.dataset.sp_bert_v_ta_p_discount(p_h => TRUE)`.
*   **Pass/Fail Criterion**:
    *   The stored procedure call completes successfully (as it's designed to return early).
    *   The BigQuery query result contains rows with usage information (e.g., 'Usage: CALL...', 'p_h: BOOLEAN...').
    *   `dw_job_log`, `dw_error_log`, and `dw_job_context` tables remain empty.

```python
def test_help_parameter(bq_client):
    query = f"CALL {SP_BERT_NAME}(p_h => TRUE)"
    job = bq_client.query(query)
    rows = list(job.result()) # Collect results from the SELECT statements

    assert len(rows) >= 4, f"Expected at least 4 usage info rows, got {len(rows)}"
    assert any("Usage: CALL" in row.Usage_Info for row in rows), "Usage info not found in results"
    assert any("p_h: BOOLEAN" in row.Usage_Detail for row in rows), "p_h detail not found in results"

    # Verify no logging occurred
    assert len(get_job_log_entries(bq_client)) == 0, "Job log should be empty when -h is used"
    assert len(get_error_log_entries(bq_client)) == 0, "Error log should be empty when -h is used"
    assert len(get_job_context_entries(bq_client)) == 0, "Job context should be empty when -h is used"

```

---

### Test Case 3: Parameter Handling - Passing `p_s` and `p_l`

*   **Purpose**: Verify that the optional parameters `p_s` and `p_l` are correctly received by the wrapper and passed down to the core `sp_k_ausd_v_ta_p_discount` procedure. This tests transformation correctness for parameter handling.
*   **Setup**: Ensure all logging tables are empty.
*   **Action**: Call `project.dataset.sp_bert_v_ta_p_discount(p_s => 'TEST_SOURCE', p_l => 'EN')`.
*   **Pass/Fail Criterion**:
    *   The stored procedure call completes successfully.
    *   `dw_job_log` contains the expected entries (similar to Test Case 1).
    *   The log entry for `sp_k_ausd_v_ta_p_discount` in `dw_job_log` should indicate that it received the correct `p_s` and `p_l` values (e.g., by inspecting its `message` field, assuming the stub logs these).

```python
def test_pass_through_parameters(bq_client):
    test_s_param = "TEST_SOURCE_VAL"
    test_l_param = "DE"
    success, error_msg = call_sp_bert(bq_client, p_s=test_s_param, p_l=test_l_param)
    assert success, f"Stored procedure call failed: {error_msg}"

    job_logs = get_job_log_entries(bq_client)
    assert len(job_logs) == 3, f"Expected 3 job log entries, got {len(job_logs)}"

    # Find the log entry from sp_k_ausd_v_ta_p_discount
    core_log = next((log for log in job_logs if log.prog_name == 'sp_k_ausd_v_ta_p_discount'), None)
    assert core_log is not None, "Core script log entry not found"

    # The stub's message should reflect the passed parameters
    # This assumes the stub was modified to include these parameters in its log message for testing.
    # If not, a more direct way would be to query BQ's audit logs for the CALL statement,
    # or modify the stub to insert these parameters into a dedicated test table.
    assert f"p_s={test_s_param}" in core_log.message, f"p_s parameter not found in core log message: {core_log.message}"
    assert f"p_l={test_l_param}" in core_log.message, f"p_l parameter not found in core log message: {core_log.message}"

```

---

### Test Case 4: Error Handling - Core Logic Failure

*   **Purpose**: Verify that if the core `sp_k_ausd_v_ta_p_discount` procedure fails, the wrapper `sp_bert_v_ta_p_discount` correctly catches the error, logs it to `dw_error_log`, updates `dw_job_log` to `FAILED`, and re-raises the exception. This covers error handling and output parity for error logs.
*   **Setup**: Ensure all logging tables are empty. The `sp_k_ausd_v_ta_p_discount` procedure must be modified to accept a `p_simulate_error` parameter and raise an exception when it's `TRUE`. The `sp_bert_v_ta_p_discount` must also be modified to accept `p_simulate_core_error` and pass it to `sp_k_ausd_v_ta_p_discount`.
*   **Action**: Call `project.dataset.sp_bert_v_ta_p_discount(p_simulate_core_error => TRUE)`.
*   **Pass/Fail Criterion**:
    *   The stored procedure call raises an exception and fails.
    *   `dw_job_log` contains entries:
        *   One `RUNNING` entry for `sp_bert_v_ta_p_discount`.
        *   One `RUNNING` entry for `sp_k_ausd_v_ta_p_discount`.
        *   One `FAILED` entry for `sp_bert_v_ta_p_discount` with an appropriate error message.
    *   `dw_error_log` contains one entry for `JOB_KENNUNG` with details of the simulated error.
    *   `dw_job_context` contains one entry.

```python
def test_core_logic_failure(bq_client):
    success, error_msg = call_sp_bert(bq_client, p_simulate_core_error=True)
    assert not success, "Stored procedure call was expected to fail but succeeded"
    assert "Simulated error in sp_k_ausd_v_ta_p_discount" in error_msg, \
        f"Expected simulated error message, got: {error_msg}"

    job_logs = get_job_log_entries(bq_client)
    assert len(job_logs) == 3, f"Expected 3 job log entries (wrapper start, core start, wrapper failed), got {len(job_logs)}"

    # Verify wrapper start log
    wrapper_start_log = job_logs[0]
    assert wrapper_start_log.job_kennung == JOB_KENNUNG
    assert wrapper_start_log.status == 'RUNNING'
    dw_eintrags_nr = wrapper_start_log.dw_eintrags_nr

    # Verify core script log (it starts before failing)
    core_log = job_logs[1]
    assert core_log.job_kennung == JOB_KENNUNG
    assert core_log.prog_name == 'sp_k_ausd_v_ta_p_discount'
    assert core_log.status == 'RUNNING'
    assert core_log.dw_eintrags_nr == dw_eintrags_nr

    # Verify wrapper end log (should be FAILED)
    wrapper_end_log = job_logs[2]
    assert wrapper_end_log.job_kennung == JOB_KENNUNG
    assert wrapper_end_log.prog_name == 'sp_bert_v_ta_p_discount'
    assert wrapper_end_log.status == 'FAILED'
    assert 'Job orchestration failed' in wrapper_end_log.message
    assert 'Simulated error' in wrapper_end_log.message
    assert wrapper_end_log.dw_eintrags_nr == dw_eintrags_nr

    # Verify error log
    error_logs = get_error_log_entries(bq_client, dw_eintrags_nr=dw_eintrags_nr)
    assert len(error_logs) == 1, f"Expected 1 error log entry, got {len(error_logs)}"
    assert error_logs[0].job_kennung == JOB_KENNUNG
    assert error_logs[0].dw_eintrags_nr == dw_eintrags_nr
    assert 'Simulated error in sp_k_ausd_v_ta_p_discount' in error_logs[0].error_message
    assert error_logs[0].error_code is not None

    # Verify job context
    job_context = get_job_context_entries(bq_client, dw_eintrags_nr=dw_eintrags_nr)
    assert len(job_context) == 1, f"Expected 1 job context entry, got {len(job_context)}"
    assert job_context[0].dw_eintrags_nr == dw_eintrags_nr

```

---

### Test Case 5: Sequential Runs and `DW_EintragsNr` Increment

*   **Purpose**: Verify that `DW_EintragsNr` (job entry number) is correctly incremented for each subsequent run of the job, mimicking the `DWMSG_ErmittleNr` functionality. This covers transformation correctness for job metadata management.
*   **Setup**: Ensure all logging tables are empty.
*   **Action**: Call `project.dataset.sp_bert_v_ta_p_discount()` multiple times (e.g., 3 times).
*   **Pass/Fail Criterion**:
    *   All calls complete successfully.
    *   `dw_job_log` contains entries for each run, and the `dw_eintrags_nr` for each run is sequentially incremented (1, 2, 3...).
    *   `dw_job_context` also reflects these sequential `dw_eintrags_nr` values.

```python
def test_sequential_runs_eintrags_nr_increment(bq_client):
    num_runs = 3
    expected_eintrags_nrs = []

    for i in range(num_runs):
        success, error_msg = call_sp_bert(bq_client)
        assert success, f"Run {i+1} failed: {error_msg}"
        last_eintrags_nr = get_last_dw_eintrags_nr(bq_client)
        expected_eintrags_nrs.append(last_eintrags_nr)

    assert expected_eintrags_nrs == [1, 2, 3], \
        f"Expected DW_EintragsNr to be [1, 2, 3], got {expected_eintrags_nrs}"

    # Verify job logs for all runs
    all_job_logs = get_job_log_entries(bq_client)
    assert len(all_job_logs) == num_runs * 3, \
        f"Expected {num_runs * 3} job log entries, got {len(all_job_logs)}"

    # Verify job contexts for all runs
    all_job_contexts = get_job_context_entries(bq_client)
    assert len(all_job_contexts) == num_runs, \
        f"Expected {num_runs} job context entries, got {len(all_job_contexts)}"

    # Check that each context entry has a unique and sequential dw_eintrags_nr
    context_eintrags_nrs = sorted([ctx.dw_eintrags_nr for ctx in all_job_contexts])
    assert context_eintrags_nrs == [1, 2, 3]

```

---

### Test Case 6: Schema and Data Type Validation for Logging Tables

*   **Purpose**: Verify that the DDLs for the logging tables (`dw_job_log`, `dw_error_log`, `dw_job_context`) are correctly applied and that data types match expectations, ensuring data quality.
*   **Setup**: No specific setup beyond deployed DDLs.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA` to inspect table schemas.
*   **Pass/Fail Criterion**:
    *   Each table exists.
    *   Each column has the expected name, data type, and nullability.

```python
def test_logging_table_schemas(bq_client):
    expected_schemas = {
        DW_JOB_LOG_TABLE: {
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "dw_eintrags_nr": {"data_type": "INT64", "is_nullable": "NO"},
            "prog_name": {"data_type": "STRING", "is_nullable": "YES"},
            "prog_version": {"data_type": "STRING", "is_nullable": "YES"},
            "log_file_path": {"data_type": "STRING", "is_nullable": "YES"},
            "status": {"data_type": "STRING", "is_nullable": "NO"},
            "start_timestamp": {"data_type": "TIMESTAMP", "is_nullable": "YES"},
            "end_timestamp": {"data_type": "TIMESTAMP", "is_nullable": "YES"},
            "message": {"data_type": "STRING", "is_nullable": "YES"},
        },
        DW_ERROR_LOG_TABLE: {
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "dw_eintrags_nr": {"data_type": "INT64", "is_nullable": "NO"},
            "error_timestamp": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
            "error_message": {"data_type": "STRING", "is_nullable": "NO"},
            "error_code": {"data_type": "STRING", "is_nullable": "YES"},
            "stack_trace": {"data_type": "STRING", "is_nullable": "YES"},
        },
        DW_JOB_CONTEXT_TABLE: {
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "dw_eintrags_nr": {"data_type": "INT64", "is_nullable": "NO"},
            "stichtag": {"data_type": "DATE", "is_nullable": "NO"},
            "creation_timestamp": {"data_type": "TIMESTAMP", "is_nullable": "YES"}, # DEFAULT CURRENT_TIMESTAMP() implies nullable
        },
    }

    for table_full_name, expected_cols in expected_schemas.items():
        table_id = table_full_name.split('.')[-1]
        query = f"""
            SELECT column_name, data_type, is_nullable
            FROM {PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{table_id}'
        """
        rows = list(bq_client.query(query).result())
        actual_cols = {row.column_name: {"data_type": row.data_type, "is_nullable": row.is_nullable} for row in rows}

        assert len(actual_cols) == len(expected_cols), \
            f"Table {table_id}: Expected {len(expected_cols)} columns, got {len(actual_cols)}"

        for col_name, expected_props in expected_cols.items():
            assert col_name in actual_cols, f"Table {table_id}: Column '{col_name}' not found."
            actual_props = actual_cols[col_name]
            assert actual_props["data_type"] == expected_props["data_type"], \
                f"Table {table_id}, Column '{col_name}': Expected data_type '{expected_props['data_type']}', got '{actual_props['data_type']}'"
            assert actual_props["is_nullable"] == expected_props["is_nullable"], \
                f"Table {table_id}, Column '{col_name}': Expected is_nullable '{expected_props['is_nullable']}', got '{actual_props['is_nullable']}'"

```

---

### Test Case 7: Edge Case - Empty `dw_job_log` for First Run

*   **Purpose**: Verify that the `dw_eintrags_nr` calculation correctly handles the very first run when `dw_job_log` is empty, ensuring it starts with `1`. This covers NULL handling for `MAX(dw_eintrags_nr)`.
*   **Setup**: Ensure all logging tables are empty.
*   **Action**: Call `project.dataset.sp_bert_v_ta_p_discount()` once.
*   **Pass/Fail Criterion**:
    *   The call completes successfully.
    *   The `dw_eintrags_nr` for the first run is `1`.

```python
def test_first_run_eintrags_nr(bq_client):
    # Ensure tables are truly empty before this specific test
    bq_client.query(f"TRUNCATE TABLE {DW_JOB_LOG_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {DW_ERROR_LOG_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {DW_JOB_CONTEXT_TABLE}").result()
    time.sleep(1)

    success, error_msg = call_sp_bert(bq_client)
    assert success, f"Stored procedure call failed: {error_msg}"

    job_logs = get_job_log_entries(bq_client)
    assert len(job_logs) > 0, "No job logs found after first run"
    first_eintrags_nr = job_logs[0].dw_eintrags_nr
    assert first_eintrags_nr == 1, f"Expected first DW_EintragsNr to be 1, got {first_eintrags_nr}"

```