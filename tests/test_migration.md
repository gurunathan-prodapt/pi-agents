As a senior data-migration QA engineer, I have prepared a suite of migration validation tests for the `r_ausd_v_ta_period.ksh` script, focusing on its transformation into BigQuery Stored Procedures. These tests aim to ensure behavioral equivalence, covering output parity, transformation correctness (for control flow and logging), external system replacements (logging framework), and data quality assertions for the new logging infrastructure.

---

## Migration Validation Tests for `sp_bert_v_ta_period`

**Target Environment:** Google Cloud BigQuery
**Legacy Script:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh`
**Migrated Procedures:**
*   `your_gcp_project.your_bq_dataset.sp_bert_v_ta_period` (wrapper)
*   `your_gcp_project.your_bq_dataset.sp_k_ausd_v_ta_period` (core logic placeholder)
**Migrated Tables:**
*   `your_gcp_project.your_bq_dataset.job_log`
*   `your_gcp_project.your_bq_dataset.job_status`
*   `your_gcp_project.your_bq_dataset.job_control`

---

### **General Setup for All Tests**

**Pre-requisites:**
1.  A BigQuery project and dataset are provisioned. For these tests, we will use `my_project.my_dataset`.
2.  The DDLs for `job_log`, `job_status`, and `job_control` tables are executed in `my_project.my_dataset`.
3.  The BigQuery Stored Procedures `sp_k_ausd_v_ta_period` and `sp_bert_v_ta_period` are deployed to `my_project.my_dataset`.
    *   **Note on `sp_k_ausd_v_ta_period` and `sp_bert_v_ta_period`:** For robust testing of error handling, the provided generated code for both procedures has been slightly modified to include an additional `p_core_should_fail BOOLEAN DEFAULT FALSE` parameter. This parameter allows simulating a failure in the core script. This parameter is for testing purposes only and would typically be removed from production code.
4.  A Python environment with `pytest` and `google-cloud-bigquery` library installed.
5.  The `pytest` fixture below will handle cleaning up the logging tables before each test run to ensure a clean state.

**Python Test Harness (shared utilities):**

```python
import pytest
from google.cloud import bigquery
import datetime
import time
import re

# Configuration for BigQuery
PROJECT_ID = "my_project"
DATASET_ID = "my_dataset"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

# Table and Procedure names
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
JOB_STATUS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_status"
JOB_CONTROL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_control"
SP_BERT_V_TA_PERIOD = f"{PROJECT_ID}.{DATASET_ID}.sp_bert_v_ta_period"
SP_K_AUSD_V_TA_PERIOD = f"{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_period"

JOB_KENNUNG = "BERT_V_TA_PERIOD"

@pytest.fixture(scope="function", autouse=True)
def setup_bigquery_environment_for_test():
    """Cleans up logging tables before each test function."""
    print(f"\n--- Setting up for test: {pytest.current_test_name} ---")
    BQ_CLIENT.query(f"TRUNCATE TABLE {JOB_LOG_TABLE}").result()
    BQ_CLIENT.query(f"TRUNCATE TABLE {JOB_STATUS_TABLE}").result()
    BQ_CLIENT.query(f"TRUNCATE TABLE {JOB_CONTROL_TABLE}").result()
    print("Cleaned up logging tables.")
    yield
    print(f"--- Tearing down for test: {pytest.current_test_name} ---")

def execute_bq_procedure(procedure_call_sql: str):
    """Executes a BigQuery stored procedure and returns success status and error message."""
    print(f"Executing BQ procedure: {procedure_call_sql}")
    query_job = BQ_CLIENT.query(procedure_call_sql)
    try:
        query_job.result() # Waits for the job to complete
        print("Procedure executed successfully.")
        return True, None
    except Exception as e:
        print(f"Procedure execution failed: {e}")
        return False, str(e)

def fetch_table_data(table_name: str, filter_clause: str = "", order_by_clause: str = "ORDER BY created_at ASC"):
    """Fetches data from a BigQuery table."""
    query = f"SELECT * FROM {table_name} {filter_clause} {order_by_clause}"
    rows = BQ_CLIENT.query(query).result()
    return [dict(row) for row in rows]

def get_current_bq_date():
    """Returns the current date as BigQuery would see it (UTC)."""
    return datetime.datetime.utcnow().date()

# Helper to get the next expected job_entry_nr based on the procedure's logic
def get_next_expected_job_entry_nr():
    query = f"""
        SELECT IFNULL(MAX(job_entry_nr), 0) + 1
        FROM {JOB_LOG_TABLE}
        WHERE job_name = '{JOB_KENNUNG}'
    """
    rows = BQ_CLIENT.query(query).result()
    return [row[0] for row in rows][0]

# Helper to capture stdout from a BQ procedure call (for usage message)
def get_bq_procedure_stdout(procedure_call_sql: str):
    """Executes a BQ procedure and captures its stdout-like output (e.g., SELECT statements)."""
    query_job = BQ_CLIENT.query(procedure_call_sql)
    rows = query_job.result()
    output_lines = []
    for row in rows:
        # Assuming the procedure's SELECT statements return a single column for stdout-like output
        output_lines.append(str(row[0]))
    return "\n".join(output_lines)

# Store current test name for fixture
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    pytest.current_test_name = item.name
    outcome = yield
    report = outcome.get_result()
    # You can process the report here if needed
```

---

### 1. Test: Help Message Display (`-h` / `p_help=TRUE`)

*   **Purpose:** Verify that invoking the wrapper with the help parameter (`-h` in legacy, `p_help => TRUE` in BQ) displays the usage information and exits without executing any core logic or logging job entries. This tests output parity and parameter handling.

*   **Setup:**
    *   Ensure the `job_log`, `job_status`, and `job_control` tables are empty.
    *   The `sp_bert_v_ta_period` procedure is deployed.

*   **Action:**
    *   **Legacy:** Execute `r_ausd_v_ta_period.ksh -h`
    *   **Migrated:** Call the BigQuery stored procedure: `CALL my_project.my_dataset.sp_bert_v_ta_period(p_help => TRUE)`

*   **Pass/Fail Criterion:**
    *   The BigQuery procedure call completes successfully (does not raise an error).
    *   The output from the BigQuery procedure (via `SELECT` statement) closely matches the expected `usage()` output from the legacy script.
    *   No new entries are found in `my_project.my_dataset.job_log`, `my_project.my_dataset.job_status`, or `my_project.my_dataset.job_control` tables.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_help_message_display():
        expected_output_pattern = r"""
            Programm: Vertragsdatenabgleich
            Version:  V1.0.0
            Aufruf:   CALL `my_project.my_dataset.sp_bert_v_ta_period`\(p_help => \[TRUE\|FALSE\], p_param_s => '\.\.\.', p_param_l => '\.\.\.', p_core_should_fail => \[TRUE\|FALSE\]\);
            Parameter:
                p_help              : Displays this help message\.
                p_param_s           : \(Optional string parameter 's', passed to core script\)
                p_param_l           : \(Optional string parameter 'l', passed to core script\)
                p_core_should_fail  : \(Internal testing parameter\) If TRUE, forces the core script to raise an error\.

            Beschreibung:
                Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period\.
                This procedure orchestrates the execution, parameter handling, and logging
                for the ta_period contract data reconciliation\.
        """
        
        # Action: Call the BQ procedure with p_help=TRUE
        output = get_bq_procedure_stdout(f"CALL {SP_BERT_V_TA_PERIOD}(p_help => TRUE)")
        
        # Assertions
        assert re.search(expected_output_pattern, output, re.DOTALL), "Help message output does not match expected pattern."
        
        # Verify no log entries were created
        log_entries = fetch_table_data(JOB_LOG_TABLE)
        status_entries = fetch_table_data(JOB_STATUS_TABLE)
        control_entries = fetch_table_data(JOB_CONTROL_TABLE)
        
        assert len(log_entries) == 0, "Job log table should be empty after help message."
        assert len(status_entries) == 0, "Job status table should be empty after help message."
        assert len(control_entries) == 0, "Job control table should be empty after help message."
    ```

---

### 2. Test: Successful Job Execution (No Optional Parameters)

*   **Purpose:** Verify the wrapper script correctly initializes the job, calls the core script, logs all steps, and marks the job as successful when no optional parameters (`-s`, `-l`) are provided. This tests transformation correctness (control flow, logging) and output parity (log content).

*   **Setup:**
    *   Ensure logging tables are empty.
    *   `sp_bert_v_ta_period` and `sp_k_ausd_v_ta_period` are deployed.

*   **Action:**
    *   **Legacy:** Execute `r_ausd_v_ta_period.ksh`
    *   **Migrated:** Call the BigQuery stored procedure: `CALL my_project.my_dataset.sp_bert_v_ta_period()`

*   **Pass/Fail Criterion:**
    *   The BigQuery procedure call completes successfully.
    *   `my_project.my_dataset.job_log` contains at least 4 entries:
        1.  Wrapper start (`log_level='I'`, message: "Job wrapper for BERT_V_TA_PERIOD started.")
        2.  Core script start (`log_level='I'`, message: "Core script k_ausd_v_ta_period started...", `p_param_s` and `p_param_l` are 'NULL')
        3.  Core script end (`log_level='I'`, message: "Core script k_ausd_v_ta_period finished successfully...")
        4.  Wrapper success (`log_level='S'`, message: "Die Abarbeitung wurde ohne erkennbare Fehler beendet")
    *   `my_project.my_dataset.job_status` contains two entries for the same `job_entry_nr`: one with `status='RUNNING'` and a later one with `status='SUCCESS'`.
    *   `my_project.my_dataset.job_control` contains one entry with `stichtag` matching `CURRENT_DATE()`.
    *   The `job_entry_nr` is correctly incremented from previous runs (if any) or starts at 1.
    *   The `LogDatei` (simulated) and `business_date` values are consistent across log entries for the same job run.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_successful_job_execution_no_params():
        expected_entry_nr = get_next_expected_job_entry_nr()
        current_date = get_current_bq_date()

        # Action: Call the BQ procedure
        success, error_msg = execute_bq_procedure(f"CALL {SP_BERT_V_TA_PERIOD}()")
        assert success, f"Procedure call failed: {error_msg}"

        # Assertions for job_log
        log_entries = fetch_table_data(JOB_LOG_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        assert len(log_entries) >= 4, f"Expected at least 4 log entries, got {len(log_entries)}"

        # Check specific log messages and order
        assert log_entries[0]['message'] == f"Job wrapper for {JOB_KENNUNG} started."
        assert log_entries[0]['log_level'] == 'I'
        assert log_entries[0]['business_date'] == current_date

        assert "Core script k_ausd_v_ta_period started." in log_entries[1]['message']
        assert "Params: s=NULL, l=NULL" in log_entries[1]['message']
        assert log_entries[1]['log_level'] == 'I'

        assert "Core script k_ausd_v_ta_period finished successfully" in log_entries[2]['message']
        assert log_entries[2]['log_level'] == 'I'

        assert log_entries[3]['message'] == "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
        assert log_entries[3]['log_level'] == 'S'

        # Assertions for job_status
        status_entries = fetch_table_data(JOB_STATUS_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        assert len(status_entries) >= 2, f"Expected at least 2 status entries, got {len(status_entries)}"
        assert status_entries[0]['status'] == 'RUNNING'
        assert status_entries[-1]['status'] == 'SUCCESS'

        # Assertions for job_control
        control_entries = fetch_table_data(JOB_CONTROL_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        assert len(control_entries) == 1, "Expected 1 control entry"
        assert control_entries[0]['stichtag'] == current_date
        assert control_entries[0]['stichtag_format'] == 'YYYYMMDD'
    ```

---

### 3. Test: Successful Job Execution (With Optional Parameters)

*   **Purpose:** Verify that optional parameters (`-s`, `-l`) are correctly passed to the `sp_bert_v_ta_period` wrapper and subsequently forwarded to the `sp_k_ausd_v_ta_period` core script. This tests parameter handling and transformation correctness.

*   **Setup:**
    *   Ensure logging tables are empty.
    *   `sp_bert_v_ta_period` and `sp_k_ausd_v_ta_period` are deployed.

*   **Action:**
    *   **Legacy:** Execute `r_ausd_v_ta_period.ksh -s "test_s_val" -l "test_l_val"`
    *   **Migrated:** Call the BigQuery stored procedure: `CALL my_project.my_dataset.sp_bert_v_ta_period(p_param_s => 'test_s_val', p_param_l => 'test_l_val')`

*   **Pass/Fail Criterion:**
    *   The BigQuery procedure call completes successfully.
    *   `my_project.my_dataset.job_log` contains entries similar to the "No Optional Parameters" test, but the core script's start message (`log_level='I'`) explicitly shows `Params: s=test_s_val, l=test_l_val`.
    *   `my_project.my_dataset.job_status` and `my_project.my_dataset.job_control` entries are consistent with a successful run.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_successful_job_execution_with_params():
        expected_entry_nr = get_next_expected_job_entry_nr()
        current_date = get_current_bq_date()
        param_s = "test_s_val"
        param_l = "test_l_val"

        # Action: Call the BQ procedure with parameters
        success, error_msg = execute_bq_procedure(f"CALL {SP_BERT_V_TA_PERIOD}(p_param_s => '{param_s}', p_param_l => '{param_l}')")
        assert success, f"Procedure call failed: {error_msg}"

        # Assertions for job_log
        log_entries = fetch_table_data(JOB_LOG_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        assert len(log_entries) >= 4, f"Expected at least 4 log entries, got {len(log_entries)}"

        # Check core script start message for parameters
        core_start_log = next((entry for entry in log_entries if "Core script k_ausd_v_ta_period started." in entry['message']), None)
        assert core_start_log is not None, "Core script start log entry not found."
        assert f"Params: s={param_s}, l={param_l}" in core_start_log['message'], \
            f"Core script did not receive parameters correctly. Message: {core_start_log['message']}"

        # Verify overall success
        success_log = next((entry for entry in log_entries if entry['log_level'] == 'S'), None)
        assert success_log is not None, "Success log entry not found."

        status_entries = fetch_table_data(JOB_STATUS_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        assert status_entries[-1]['status'] == 'SUCCESS'
    ```

---

### 4. Test: Error Handling - Core Script Failure

*   **Purpose:** Verify that if the core script (`sp_k_ausd_v_ta_period`) encounters an error, the wrapper (`sp_bert_v_ta_period`) correctly catches it (simulating `trap ERR`), logs the error, and updates the job status to 'FAILED'. This tests transformation correctness (error handling) and external system replacement (DWMSG_Fehlerbehandlung).

*   **Setup:**
    *   Ensure logging tables are empty.
    *   `sp_bert_v_ta_period` and `sp_k_ausd_v_ta_period` are deployed, with `p_core_should_fail` parameter enabled for testing.

*   **Action:**
    *   **Legacy:** Simulate `k_ausd_v_ta_period.ksh` exiting with a non-zero status or encountering a runtime error that triggers the `trap ERR` handler.
    *   **Migrated:** Call the BigQuery stored procedure: `CALL my_project.my_dataset.sp_bert_v_ta_period(p_core_should_fail => TRUE)`

*   **Pass/Fail Criterion:**
    *   The BigQuery procedure call `RAISE`s an error, indicating failure to the caller.
    *   `my_project.my_dataset.job_log` contains an error entry (`log_level='E'`) from `sp_bert_v_ta_period` with a message indicating the failure. It should also contain the initial wrapper start and core script start messages.
    *   `my_project.my_dataset.job_status` contains two entries for the same `job_entry_nr`: one with `status='RUNNING'` and a later one with `status='FAILED'`.
    *   No 'S' (Success) log entry is present.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_error_handling_core_script_failure():
        expected_entry_nr = get_next_expected_job_entry_nr()
        current_date = get_current_bq_date()

        # Action: Call the BQ procedure, forcing core script to fail
        success, error_msg = execute_bq_procedure(f"CALL {SP_BERT_V_TA_PERIOD}(p_core_should_fail => TRUE)")
        assert not success, "Procedure call was expected to fail but succeeded."
        assert "Simulated error in sp_k_ausd_v_ta_period" in error_msg, "Error message does not match expected core script failure."

        # Assertions for job_log
        log_entries = fetch_table_data(JOB_LOG_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        assert len(log_entries) >= 3, f"Expected at least 3 log entries (wrapper start, core start, wrapper error), got {len(log_entries)}"

        # Check for wrapper start, core start, and wrapper error
        assert log_entries[0]['message'] == f"Job wrapper for {JOB_KENNUNG} started."
        assert log_entries[0]['log_level'] == 'I'

        assert "Core script k_ausd_v_ta_period started." in log_entries[1]['message']
        assert log_entries[1]['log_level'] == 'I'

        error_log = next((entry for entry in log_entries if entry['log_level'] == 'E'), None)
        assert error_log is not None, "Error log entry not found."
        assert "Execution aborted due to error." in error_log['message']
        assert error_log['error_nr'] == 1 # As defined in the BQ procedure

        # Assertions for job_status
        status_entries = fetch_table_data(JOB_STATUS_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        assert len(status_entries) >= 2, f"Expected at least 2 status entries, got {len(status_entries)}"
        assert status_entries[0]['status'] == 'RUNNING'
        assert status_entries[-1]['status'] == 'FAILED'

        # Verify no success log entry
        success_log = next((entry for entry in log_entries if entry['log_level'] == 'S'), None)
        assert success_log is None, "Success log entry should not be present in case of failure."
    ```

---

### 5. Test: Data Quality - Logging Tables Schema and Constraints

*   **Purpose:** Verify that the DDLs for `job_log`, `job_status`, and `job_control` tables are correctly implemented in BigQuery, matching the specified schema, data types, and nullability. This ensures data quality and schema assertions.

*   **Setup:**
    *   The DDLs for `job_log`, `job_status`, and `job_control` are assumed to be executed.

*   **Action:**
    *   Query BigQuery's `INFORMATION_SCHEMA` to retrieve table and column details.

*   **Pass/Fail Criterion:**
    *   Each table (`job_log`, `job_status`, `job_control`) exists.
    *   All expected columns are present with the correct data types and nullability constraints as defined in the DDLs.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_logging_tables_schema_and_constraints():
        # Define expected schemas
        expected_schemas = {
            JOB_LOG_TABLE: {
                'job_name': {'data_type': 'STRING', 'is_nullable': 'NO'},
                'job_entry_nr': {'data_type': 'INT64', 'is_nullable': 'NO'},
                'log_level': {'data_type': 'STRING', 'is_nullable': 'NO'},
                'error_nr': {'data_type': 'INT64', 'is_nullable': 'YES'},
                'error_arg': {'data_type': 'STRING', 'is_nullable': 'YES'},
                'message': {'data_type': 'STRING', 'is_nullable': 'NO'},
                'log_file_name': {'data_type': 'STRING', 'is_nullable': 'YES'},
                'business_date': {'data_type': 'DATE', 'is_nullable': 'YES'},
                'created_at': {'data_type': 'TIMESTAMP', 'is_nullable': 'NO'},
                'updated_at': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'}
            },
            JOB_STATUS_TABLE: {
                'job_name': {'data_type': 'STRING', 'is_nullable': 'NO'},
                'job_entry_nr': {'data_type': 'INT64', 'is_nullable': 'NO'},
                'status': {'data_type': 'STRING', 'is_nullable': 'NO'},
                'updated_at': {'data_type': 'TIMESTAMP', 'is_nullable': 'NO'}
            },
            JOB_CONTROL_TABLE: {
                'job_name': {'data_type': 'STRING', 'is_nullable': 'NO'},
                'job_entry_nr': {'data_type': 'INT64', 'is_nullable': 'NO'},
                'stichtag': {'data_type': 'DATE', 'is_nullable': 'NO'},
                'stichtag_format': {'data_type': 'STRING', 'is_nullable': 'YES'},
                'created_at': {'data_type': 'TIMESTAMP', 'is_nullable': 'NO'}
            }
        }

        for table_fqn, expected_schema in expected_schemas.items():
            dataset_id = table_fqn.split('.')[1]
            table_id = table_fqn.split('.')[2]
            
            query = f"""
                SELECT column_name, data_type, is_nullable
                FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_id}'
            """
            rows = BQ_CLIENT.query(query).result()
            actual_schema = {row['column_name']: {'data_type': row['data_type'], 'is_nullable': row['is_nullable']} for row in rows}

            assert len(actual_schema) == len(expected_schema), \
                f"Table {table_fqn}: Column count mismatch. Expected {len(expected_schema)}, got {len(actual_schema)}"
            
            for col_name, expected_props in expected_schema.items():
                assert col_name in actual_schema, f"Table {table_fqn}: Missing column {col_name}"
                assert actual_schema[col_name]['data_type'] == expected_props['data_type'], \
                    f"Table {table_fqn}, Column {col_name}: Data type mismatch. Expected {expected_props['data_type']}, got {actual_schema[col_name]['data_type']}"
                assert actual_schema[col_name]['is_nullable'] == expected_props['is_nullable'], \
                    f"Table {table_fqn}, Column {col_name}: Nullability mismatch. Expected {expected_props['is_nullable']}, got {actual_schema[col_name]['is_nullable']}"
            print(f"Schema for {table_fqn} verified successfully.")
    ```

---

### 6. Test: `DWMSG_ErmittleNr` Equivalence (Job Entry Number Generation)

*   **Purpose:** Verify that the `job_entry_nr` generation logic in BigQuery (`SELECT IFNULL(MAX(job_entry_nr), 0) + 1`) correctly mimics the `DWMSG_ErmittleNr` function from the legacy script, ensuring unique and sequential job identifiers. This tests transformation correctness.

*   **Setup:**
    *   Ensure logging tables are empty.
    *   `sp_bert_v_ta_period` is deployed.

*   **Action:**
    *   Execute `sp_bert_v_ta_period` multiple times in sequence.

*   **Pass/Fail Criterion:**
    *   Each successful execution of `sp_bert_v_ta_period` results in a `job_entry_nr` that is exactly one greater than the previous run's `job_entry_nr` for the same `job_name`. The first run should have `job_entry_nr = 1`.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_job_entry_number_generation():
        # First run
        expected_entry_nr_1 = get_next_expected_job_entry_nr()
        success_1, _ = execute_bq_procedure(f"CALL {SP_BERT_V_TA_PERIOD}()")
        assert success_1, "First procedure call failed."
        log_entries_1 = fetch_table_data(JOB_LOG_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr_1}")
        assert len(log_entries_1) > 0 and log_entries_1[0]['job_entry_nr'] == expected_entry_nr_1, \
            f"First run did not get expected job_entry_nr {expected_entry_nr_1}"

        # Second run
        expected_entry_nr_2 = get_next_expected_job_entry_nr()
        success_2, _ = execute_bq_procedure(f"CALL {SP_BERT_V_TA_PERIOD}()")
        assert success_2, "Second procedure call failed."
        log_entries_2 = fetch_table_data(JOB_LOG_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr_2}")
        assert len(log_entries_2) > 0 and log_entries_2[0]['job_entry_nr'] == expected_entry_nr_2, \
            f"Second run did not get expected job_entry_nr {expected_entry_nr_2}"

        assert expected_entry_nr_2 == expected_entry_nr_1 + 1, \
            f"Job entry number not sequential. Expected {expected_entry_nr_1 + 1}, got {expected_entry_nr_2}"
        print("Job entry numbers generated sequentially.")
    ```

---

### 7. Test: `v_sysdate` and `stichtag` Handling

*   **Purpose:** Verify that the system date (`v_sysdate` in legacy, `CURRENT_DATE()` in BQ) is consistently captured and used for the `business_date` in `job_log` entries and the `stichtag` in `job_control`. This tests transformation correctness.

*   **Setup:**
    *   Ensure logging tables are empty.
    *   `sp_bert_v_ta_period` is deployed.

*   **Action:**
    *   Execute `sp_bert_v_ta_period` once.

*   **Pass/Fail Criterion:**
    *   All `job_log` entries for the run have `business_date` equal to the date of execution.
    *   The `job_control` entry for the run has `stichtag` equal to the date of execution.
    *   The `stichtag_format` in `job_control` is 'YYYYMMDD'.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_sysdate_and_stichtag_handling():
        expected_entry_nr = get_next_expected_job_entry_nr()
        current_date = get_current_bq_date()

        # Action: Call the BQ procedure
        success, _ = execute_bq_procedure(f"CALL {SP_BERT_V_TA_PERIOD}()")
        assert success, "Procedure call failed."

        # Assertions for job_log business_date
        log_entries = fetch_table_data(JOB_LOG_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        for entry in log_entries:
            assert entry['business_date'] == current_date, \
                f"Log entry business_date mismatch. Expected {current_date}, got {entry['business_date']}"

        # Assertions for job_control stichtag
        control_entries = fetch_table_data(JOB_CONTROL_TABLE, f"WHERE job_name = '{JOB_KENNUNG}' AND job_entry_nr = {expected_entry_nr}")
        assert len(control_entries) == 1, "Expected 1 control entry."
        assert control_entries[0]['stichtag'] == current_date, \
            f"Control entry stichtag mismatch. Expected {current_date}, got {control_entries[0]['stichtag']}"
        assert control_entries[0]['stichtag_format'] == 'YYYYMMDD', \
            f"Control entry stichtag_format mismatch. Expected 'YYYYMMDD', got {control_entries[0]['stichtag_format']}"
        print("System date and stichtag handling verified.")
    ```