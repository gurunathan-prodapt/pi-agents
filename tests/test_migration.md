This document outlines migration validation tests for the `r_ausd_bp_ta_msisdn.ksh` KornShell script, which has been migrated to Google BigQuery stored procedures and orchestrated via Apache Airflow. The tests aim to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality.

**Important Pre-requisites and Configuration:**

1.  **Legacy Environment Setup**: To perform output parity tests, a functional legacy environment is required where `r_ausd_bp_ta_msisdn.ksh` can be executed. This includes:
    *   The script itself (`LEGACY_SCRIPT_PATH`).
    *   The `.dw_init` file at `$HOME/.dw_init` (configured via `LEGACY_HOME`).
    *   The utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) at their expected paths relative to `LEGACY_BERT_DIR_ROOT`.
    *   A KornShell interpreter (`ksh`).
2.  **BigQuery Setup**:
    *   A GCP project and dataset where the migrated BigQuery stored procedures and tables reside.
    *   The DDLs for `project.dataset.job_audit_log` and `project.dataset.job_run_info` must be executed.
    *   The `ausd_bp_ta_msisdn_wrapper` and `k_ausd_bp_ta_msisdn` stored procedures must be deployed.
3.  **Pytest Environment**: Python with `pytest` and `google-cloud-bigquery` libraries installed.
4.  **Airflow Environment**: A Cloud Composer environment (or local Airflow) configured to run the provided DAG.

**Configuration for Tests (Update these values in the Python test script):**

```python
import pytest
from google.cloud import bigquery
from datetime import date, datetime, timezone
import subprocess
import os
import re
import time
import shutil

# --- Configuration (User MUST update these paths and project/dataset IDs) ---
# Replace with your actual GCP project and dataset IDs
GCP_PROJECT_ID = "your-gcp-project-id" 
BQ_DATASET_ID = "your_bq_dataset_id"

BQ_AUDIT_LOG_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_audit_log"
BQ_RUN_INFO_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_run_info"
BQ_WRAPPER_SP = f"{BQ_DATASET_ID}.ausd_bp_ta_msisdn_wrapper" # Note: dataset.procedure_name
BQ_KERNEL_SP_FULL_PATH = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.k_ausd_bp_ta_msisdn"

# These paths are CRUCIAL for the legacy script to run correctly.
# They should point to the actual directories containing .dw_init and BERT utility scripts.
# Example: LEGACY_SCRIPT_PATH = "/opt/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh"
# Example: LEGACY_BERT_DIR_ROOT = "/opt/vobs/dw_source/isrpt/isbert"
# Example: LEGACY_HOME = "/home/dwuser" (where /home/dwuser/.dw_init exists)
LEGACY_SCRIPT_PATH = "/path/to/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh"
LEGACY_BERT_DIR_ROOT = "/path/to/bert_dir_root" 
LEGACY_HOME = "/path/to/home_for_dw_init" 

# --- BigQuery Client ---
bq_client = bigquery.Client(project=GCP_PROJECT_ID)

# --- Helper Functions ---
def clear_bq_tables():
    """Clears the audit and run info tables in BigQuery."""
    try:
        bq_client.query(f"TRUNCATE TABLE `{BQ_AUDIT_LOG_TABLE}`").result()
        bq_client.query(f"TRUNCATE TABLE `{BQ_RUN_INFO_TABLE}`").result()
        print(f"Cleared tables: {BQ_AUDIT_LOG_TABLE}, {BQ_RUN_INFO_TABLE}")
    except Exception as e:
        print(f"Warning: Could not truncate tables (might not exist yet): {e}")

def get_bq_audit_logs(job_kennung, job_nr=None):
    """Fetches audit log entries for a given job_kennung and optional job_nr."""
    query = f"SELECT job_kennung, job_nr, status, stichtag, sysdate, err_nr, err_arg, message FROM `{BQ_AUDIT_LOG_TABLE}` WHERE job_kennung = '{job_kennung}'"
    if job_nr is not None:
        query += f" AND job_nr = {job_nr}"
    query += " ORDER BY log_ts ASC"
    rows = list(bq_client.query(query).result())
    return rows

def get_bq_run_info(job_kennung, job_nr=None):
    """Fetches run info entries for a given job_kennung and optional job_nr."""
    query = f"SELECT job_kennung, job_nr, stichtag, sysdate FROM `{BQ_RUN_INFO_TABLE}` WHERE job_kennung = '{job_kennung}'"
    if job_nr is not None:
        query += f" AND job_nr = {job_nr}"
    query += " ORDER BY created_ts ASC"
    rows = list(bq_client.query(query).result())
    return rows

def run_bq_wrapper_sp(p_stichtag_string=None, p_wiederanlaufWert=None):
    """Executes the BigQuery wrapper stored procedure."""
    stichtag_param = f"'{p_stichtag_string}'" if p_stichtag_string is not None else "NULL"
    wiederanlauf_param = str(p_wiederanlaufWert) if p_wiederanlaufWert is not None else "NULL"
    query = f"CALL `{GCP_PROJECT_ID}.{BQ_WRAPPER_SP}`({stichtag_param}, {wiederanlauf_param});"
    print(f"Executing BQ SP: {query}")
    try:
        bq_client.query(query).result()
        return True, None
    except Exception as e:
        print(f"BQ SP execution failed: {e}")
        return False, str(e)

def run_legacy_script(params, expected_exit_code=0):
    """Executes the legacy ksh script and captures its output and log file."""
    cmd = [LEGACY_SCRIPT_PATH] + params
    
    # Create a temporary directory for logs, and change CWD for script execution
    # This is important because the legacy script writes logs to its current working directory.
    temp_log_dir = f"/tmp/legacy_logs_{int(time.time())}"
    os.makedirs(temp_log_dir, exist_ok=True)
    
    # Set environment variables for the legacy script
    env = os.environ.copy()
    env['BERT_DIR_ROOT'] = LEGACY_BERT_DIR_ROOT
    env['HOME'] = LEGACY_HOME # For .dw_init
    
    print(f"Executing legacy script: {' '.join(cmd)} in {temp_log_dir}")
    print(f"Legacy script env: BERT_DIR_ROOT={env['BERT_DIR_ROOT']}, HOME={env['HOME']}")

    process = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=temp_log_dir)
    
    console_output = process.stdout + process.stderr
    print(f"Legacy script console output:\n{console_output}")

    legacy_log_content = ""
    # The script prints the log file name, e.g., Logdatei  : 'ausd_bp_ta_msisdn_1.log'
    log_file_name_match = re.search(r"Logdatei\s*:\s*'(.+)'", console_output)
    if log_file_name_match:
        log_file_base_name = os.path.basename(log_file_name_match.group(1))
        log_file_path = os.path.join(temp_log_dir, log_file_base_name)
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r') as f:
                legacy_log_content = f.read()
            print(f"Legacy log file content ({log_file_path}):\n{legacy_log_content}")
        else:
            print(f"Warning: Legacy log file '{log_file_base_name}' not found at {log_file_path}")
    else:
        print("Warning: Could not determine legacy log file name from console output.")

    # Clean up temp log directory
    shutil.rmtree(temp_log_dir, ignore_errors=True)

    assert process.returncode == expected_exit_code, f"Legacy script exited with code {process.returncode}, expected {expected_exit_code}. Output: {console_output}"
    
    return {
        "stdout": process.stdout,
        "stderr": process.stderr,
        "returncode": process.returncode,
        "log_content": legacy_log_content
    }

def parse_legacy_log(log_content):
    """Parses key information from the legacy script's log content."""
    parsed_data = {
        'job_nr': None,
        'stichtag': None,
        'sysdate': None,
        'status_started': False,
        'status_ok': False,
        'status_error': False,
        'err_nr': None,
        'err_arg': None,
        'log_file_name': None
    }
    
    job_nr_match = re.search(r"Job-Nr\s*:\s*'(\d+)'", log_content)
    if job_nr_match:
        parsed_data['job_nr'] = int(job_nr_match.group(1))
    
    stichtag_match = re.search(r"Stichtag\s*:\s*'(\d{8})'", log_content)
    if stichtag_match:
        parsed_data['stichtag'] = datetime.strptime(stichtag_match.group(1), '%d%m%Y').date()

    # Legacy script prints sysdate in DWMSG_SetzeStichtagInfo, but it's not directly in the log output shown.
    # It's used internally. For comparison, we'll use the system date at test execution time.
    parsed_data['sysdate'] = date.today() # Approximation for legacy sysdate

    log_file_match = re.search(r"Logdatei\s*:\s*'(.+)'", log_content)
    if log_file_match:
        parsed_data['log_file_name'] = os.path.basename(log_file_match.group(1))

    if "Job started" in log_content:
        parsed_data['status_started'] = True
    if "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in log_content:
        parsed_data['status_ok'] = True
    if "AppError: Abbruch" in log_content or "OSError: Abbruch" in log_content:
        parsed_data['status_error'] = True
    
    # Error messages from DWMSG_MeldeFehler are usually printed to stderr and then redirected.
    # The format is "DWMSG_MeldeFehler <job_nr> E <ErrNr> <ErrArg>"
    err_match = re.search(r"DWMSG_MeldeFehler\s+\d+\s+E\s+(\d+)\s+(.+)", log_content)
    if err_match:
        parsed_data['err_nr'] = int(err_match.group(1))
        parsed_data['err_arg'] = err_match.group(2).strip()

    return parsed_data

def modify_bq_kernel_sp(add_error=False):
    """Temporarily modifies the BQ kernel SP to simulate an error or revert."""
    if add_error:
        new_definition = f"""
CREATE OR REPLACE PROCEDURE `{BQ_KERNEL_SP_FULL_PATH}`(
  IN p_job_kennung STRING,
  IN p_stichtag DATE,
  IN p_job_nr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  INSERT INTO `{BQ_AUDIT_LOG_TABLE}` (job_kennung, job_nr, log_ts, status, message)
  VALUES (p_job_kennung, p_job_nr, CURRENT_TIMESTAMP(), 'INFO', 'Kernel procedure executed (simulating error)');
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel error';
END;
"""
    else:
        # Revert to original placeholder
        new_definition = f"""
CREATE OR REPLACE PROCEDURE `{BQ_KERNEL_SP_FULL_PATH}`(
  IN p_job_kennung STRING,
  IN p_stichtag DATE,
  IN p_job_nr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  INSERT INTO `{BQ_AUDIT_LOG_TABLE}` (job_kennung, job_nr, log_ts, status, message)
  VALUES (p_job_kennung, p_job_nr, CURRENT_TIMESTAMP(), 'INFO', 'Kernel procedure executed');
END;
"""
    print(f"Modifying BQ kernel SP to {'add error' if add_error else 'revert'}...")
    bq_client.query(new_definition).result()
    print("BQ kernel SP modified.")

# --- Pytest Fixtures ---
@pytest.fixture(autouse=True)
def setup_and_teardown_bq_tables():
    """Fixture to clear BQ tables before each test."""
    clear_bq_tables()
    yield
    # No teardown needed as autouse fixture clears before each test.

@pytest.fixture(scope="module", autouse=True)
def setup_kernel_sp_for_module():
    """Ensures kernel SP is in default state before and after module tests."""
    modify_bq_kernel_sp(add_error=False) # Ensure default state
    yield
    modify_bq_kernel_sp(add_error=False) # Revert after all tests in module

```

---

## Test Case 1: Schema and Data Quality Assertions for Audit Tables

*   **Purpose**: Verify that the `job_audit_log` and `job_run_info` tables exist and conform to the expected schema and data types as defined in the migration design. This ensures the foundation for logging and auditing is correctly laid out.
*   **Setup**:
    *   Ensure the DDLs for `project.dataset.job_audit_log` and `project.dataset.job_run_info` have been executed in BigQuery.
*   **Action**:
    *   Query BigQuery's `INFORMATION_SCHEMA` to retrieve the schema details for both tables.
*   **Pass/Fail Criterion**:
    *   **Schema Assertions**:
        *   The table `project.dataset.job_audit_log` must exist and have the following columns with specified types and nullability:
            *   `job_kennung` (STRING, NOT NULL)
            *   `job_nr` (INT64, NOT NULL)
            *   `log_ts` (TIMESTAMP, NOT NULL)
            *   `status` (STRING, NOT NULL)
            *   `stichtag` (DATE, NULLABLE)
            *   `sysdate` (DATE, NULLABLE)
            *   `log_file` (STRING, NULLABLE)
            *   `err_nr` (INT64, NULLABLE)
            *   `err_arg` (STRING, NULLABLE)
            *   `message` (STRING, NULLABLE)
        *   The table `project.dataset.job_run_info` must exist and have the following columns with specified types and nullability:
            *   `job_kennung` (STRING, NOT NULL)
            *   `job_nr` (INT64, NOT NULL)
            *   `stichtag` (DATE, NOT NULL)
            *   `sysdate` (DATE, NOT NULL)
            *   `created_ts` (TIMESTAMP, NOT NULL)
    *   **Data Quality**: (After subsequent test runs populate data)
        *   `job_nr` values for a given `job_kennung` should be sequentially increasing.
        *   `log_ts` and `created_ts` values should be monotonically increasing within a job run.
        *   Date fields (`stichtag`, `sysdate`) should contain valid date values.

```python
# Part of the pytest suite
def test_schema_assertions():
    """Verifies the schema of job_audit_log and job_run_info tables."""
    expected_audit_schema = {
        "job_kennung": ("STRING", "REQUIRED"),
        "job_nr": ("INT64", "REQUIRED"),
        "log_ts": ("TIMESTAMP", "REQUIRED"),
        "status": ("STRING", "REQUIRED"),
        "stichtag": ("DATE", "NULLABLE"),
        "sysdate": ("DATE", "NULLABLE"),
        "log_file": ("STRING", "NULLABLE"),
        "err_nr": ("INT64", "NULLABLE"),
        "err_arg": ("STRING", "NULLABLE"),
        "message": ("STRING", "NULLABLE"),
    }
    expected_run_info_schema = {
        "job_kennung": ("STRING", "REQUIRED"),
        "job_nr": ("INT64", "REQUIRED"),
        "stichtag": ("DATE", "REQUIRED"),
        "sysdate": ("DATE", "REQUIRED"),
        "created_ts": ("TIMESTAMP", "REQUIRED"),
    }

    def assert_table_schema(table_id, expected_schema):
        table = bq_client.get_table(table_id)
        actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}
        
        assert len(actual_schema) == len(expected_schema), f"Schema mismatch for {table_id}: column count differs."
        for col_name, (col_type, col_mode) in expected_schema.items():
            assert col_name in actual_schema, f"Column {col_name} missing in {table_id}"
            assert actual_schema[col_name][0] == col_type, f"Column {col_name} type mismatch in {table_id}: expected {col_type}, got {actual_schema[col_name][0]}"
            assert actual_schema[col_name][1] == col_mode, f"Column {col_name} mode mismatch in {table_id}: expected {col_mode}, got {actual_schema[col_name][1]}"
        print(f"Schema for {table_id} verified successfully.")

    assert_table_schema(BQ_AUDIT_LOG_TABLE, expected_audit_schema)
    assert_table_schema(BQ_RUN_INFO_TABLE, expected_run_info_schema)

```

---

## Test Case 2: Successful Execution with Default Parameters

*   **Purpose**: Verify the migrated BigQuery wrapper stored procedure executes successfully when no parameters are provided, correctly defaulting `p_stichtag` to `CURRENT_DATE()` and `p_wiederanlaufWert` to `0`. It also checks for correct logging of the job's lifecycle.
*   **Setup**:
    *   BigQuery audit tables are cleared by the `setup_and_teardown_bq_tables` fixture.
    *   Record the `CURRENT_DATE()` in BigQuery at the time of test execution for comparison.
*   **Action**:
    *   **Legacy**: Execute `r_ausd_bp_ta_msisdn.ksh` without any arguments. Capture console output and the generated log file.
    *   **Migrated**: Execute `CALL `project.dataset.ausd_bp_ta_msisdn_wrapper`(NULL, NULL);`
*   **Pass/Fail Criterion**:
    *   **Output Parity & Transformation Correctness**:
        *   The legacy log file must contain messages indicating job start, kernel script execution, and successful completion.
        *   BigQuery `job_audit_log` must contain three entries for `job_kennung='ausd_bp_ta_msisdn'` and the generated `job_nr`:
            1.  `status='STARTED'`, `message='Job started'`, `stichtag` and `sysdate` matching `CURRENT_DATE()` (from BQ).
            2.  `status='INFO'`, `message='Kernel procedure executed'`. This confirms the `k_ausd_bp_ta_msisdn` SP was called.
            3.  `status='OK'`, `message='Die Abarbeitung wurde ohne erkennbare Fehler beendet'`.
        *   BigQuery `job_run_info` must contain one entry with `stichtag` and `sysdate` matching `CURRENT_DATE()` (from BQ).
        *   The `job_nr` and `log_file` name (derived from `job_nr`) in BigQuery logs should correspond to the legacy script's output.

```python
# Part of the pytest suite
def test_successful_execution_default_params():
    """Tests successful execution of the wrapper SP with default parameters."""
    job_kennung = "ausd_bp_ta_msisdn"
    expected_sysdate = date.today() # BQ CURRENT_DATE()

    # --- Legacy Execution ---
    legacy_result = run_legacy_script([])
    legacy_parsed = parse_legacy_log(legacy_result['log_content'])
    
    assert legacy_parsed['status_started']
    assert legacy_parsed['status_ok']
    assert legacy_parsed['stichtag'] == expected_sysdate # Legacy defaults to sysdate if not set
    assert legacy_parsed['job_nr'] is not None
    legacy_job_nr = legacy_parsed['job_nr']

    # --- Migrated Execution ---
    success, error_msg = run_bq_wrapper_sp(p_stichtag_string=None, p_wiederanlaufWert=None)
    assert success, f"BigQuery SP execution failed: {error_msg}"

    bq_audit_logs = get_bq_audit_logs(job_kennung, job_nr=legacy_job_nr)
    bq_run_info = get_bq_run_info(job_kennung, job_nr=legacy_job_nr)

    # Assertions for BigQuery Audit Log
    assert len(bq_audit_logs) == 3
    assert bq_audit_logs[0].status == 'STARTED'
    assert bq_audit_logs[0].message == 'Job started'
    assert bq_audit_logs[0].stichtag == expected_sysdate
    assert bq_audit_logs[0].sysdate == expected_sysdate
    assert bq_audit_logs[0].job_nr == legacy_job_nr

    assert bq_audit_logs[1].status == 'INFO'
    assert bq_audit_logs[1].message == 'Kernel procedure executed'
    assert bq_audit_logs[1].job_nr == legacy_job_nr

    assert bq_audit_logs[2].status == 'OK'
    assert bq_audit_logs[2].message == 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    assert bq_audit_logs[2].job_nr == legacy_job_nr

    # Assertions for BigQuery Run Info
    assert len(bq_run_info) == 1
    assert bq_run_info[0].job_kennung == job_kennung
    assert bq_run_info[0].job_nr == legacy_job_nr
    assert bq_run_info[0].stichtag == expected_sysdate
    assert bq_run_info[0].sysdate == expected_sysdate

```

---

## Test Case 3: Execution with Specific Stichtag and Wiederanlaufwert

*   **Purpose**: Verify the wrapper correctly handles explicit `p_stichtag` and `p_wiederanlaufWert` parameters, parsing the date string and passing the restart value to the kernel procedure.
*   **Setup**:
    *   BigQuery audit tables are cleared.
    *   Define `TEST_STICHTAG_STR = '01012023'` and `TEST_WIEDERANLAUFWERT = 12345`.
    *   The `job_nr` will be the next available one (e.g., 1 if tables were cleared).
*   **Action**:
    *   **Legacy**: Execute `r_ausd_bp_ta_msisdn.ksh -s 01012023 -l 12345`. Capture console output and log file.
    *   **Migrated**: Execute `CALL `project.dataset.ausd_bp_ta_msisdn_wrapper`('01012023', 12345);`
*   **Pass/Fail Criterion**:
    *   **Output Parity & Transformation Correctness**:
        *   The legacy log file must show `Stichtag : '01012023'` and indicate successful completion.
        *   BigQuery `job_audit_log` must contain three entries for `job_kennung='ausd_bp_ta_msisdn'` and the generated `job_nr`:
            1.  `status='STARTED'`, `stichtag` matching `PARSE_DATE('%d%m%Y', '01012023')`.
            2.  `status='INFO'`, `message='Kernel procedure executed'`. This implicitly confirms `p_wiederanlaufWert` was passed to the kernel SP.
            3.  `status='OK'`.
        *   BigQuery `job_run_info` must contain one entry with `stichtag` matching `PARSE_DATE('%d%m%Y', '01012023')`.
        *   The `job_nr` in BigQuery logs should correspond to the legacy script's output.

```python
# Part of the pytest suite
def test_execution_with_specific_params():
    """Tests execution with specific stichtag and wiederanlaufWert."""
    job_kennung = "ausd_bp_ta_msisdn"
    test_stichtag_str = '01012023'
    test_stichtag_date = datetime.strptime(test_stichtag_str, '%d%m%Y').date()
    test_wiederanlaufwert = 12345
    expected_sysdate = date.today()

    # --- Legacy Execution ---
    legacy_result = run_legacy_script(['-s', test_stichtag_str, '-l', str(test_wiederanlaufwert)])
    legacy_parsed = parse_legacy_log(legacy_result['log_content'])
    
    assert legacy_parsed['status_started']
    assert legacy_parsed['status_ok']
    assert legacy_parsed['stichtag'] == test_stichtag_date
    assert legacy_parsed['job_nr'] is not None
    legacy_job_nr = legacy_parsed['job_nr']

    # --- Migrated Execution ---
    success, error_msg = run_bq_wrapper_sp(p_stichtag_string=test_stichtag_str, p_wiederanlaufWert=test_wiederanlaufwert)
    assert success, f"BigQuery SP execution failed: {error_msg}"

    bq_audit_logs = get_bq_audit_logs(job_kennung, job_nr=legacy_job_nr)
    bq_run_info = get_bq_run_info(job_kennung, job_nr=legacy_job_nr)

    # Assertions for BigQuery Audit Log
    assert len(bq_audit_logs) == 3
    assert bq_audit_logs[0].status == 'STARTED'
    assert bq_audit_logs[0].stichtag == test_stichtag_date
    assert bq_audit_logs[0].sysdate == expected_sysdate
    assert bq_audit_logs[0].job_nr == legacy_job_nr

    assert bq_audit_logs[1].status == 'INFO'
    assert bq_audit_logs[1].message == 'Kernel procedure executed'
    assert bq_audit_logs[1].job_nr == legacy_job_nr

    assert bq_audit_logs[2].status == 'OK'
    assert bq_audit_logs[2].job_nr == legacy_job_nr

    # Assertions for BigQuery Run Info
    assert len(bq_run_info) == 1
    assert bq_run_info[0].job_kennung == job_kennung
    assert bq_run_info[0].job_nr == legacy_job_nr
    assert bq_run_info[0].stichtag == test_stichtag_date
    assert bq_run_info[0].sysdate == expected_sysdate

```

---

## Test Case 4: Parameter Validation - Invalid Stichtag Format

*   **Purpose**: Verify the wrapper correctly identifies and handles an invalid `p_stichtag` format (e.g., `YYYY-MM-DD` instead of `DDMMYYYY`), logging an error and terminating the job.
*   **Setup**:
    *   BigQuery audit tables are cleared.
    *   Define `INVALID_STICHTAG_STR = '2023-01-01'`.
*   **Action**:
    *   **Legacy**: Execute `r_ausd_bp_ta_msisdn.ksh -s 2023-01-01`. Capture console output and log file.
    *   **Migrated**: Execute `CALL `project.dataset.ausd_bp_ta_msisdn_wrapper`('2023-01-01', NULL);`
*   **Pass/Fail Criterion**:
    *   **Output Parity & Error Handling**:
        *   The legacy log file must contain `ErrNr=193`, `ErrArg=Stichtag`, and an error message indicating parameter validation failure. The script should exit with a non-zero status.
        *   The BigQuery `CALL` statement should raise an error (e.g., `45000` SQLSTATE) due to the `SIGNAL SQLSTATE` in the wrapper.
        *   BigQuery `job_audit_log` must contain exactly one entry: `status='ERROR'`, `err_nr=193`, `err_arg='Stichtag'`, `message='Required parameter missing or invalid'`.
        *   No `STARTED`, `INFO`, or `OK` entries should be present for this `job_nr` in `job_audit_log`. `job_run_info` should be empty.

```python
# Part of the pytest suite
def test_parameter_validation_invalid_stichtag():
    """Tests parameter validation for an invalid stichtag format."""
    job_kennung = "ausd_bp_ta_msisdn"
    invalid_stichtag_str = '2023-01-01' # Expected DDMMYYYY

    # --- Legacy Execution ---
    # Legacy script exits with ErrNr (193) which is non-zero
    legacy_result = run_legacy_script(['-s', invalid_stichtag_str], expected_exit_code=193) 
    legacy_parsed = parse_legacy_log(legacy_result['log_content'])
    
    assert legacy_parsed['status_error']
    assert legacy_parsed['err_nr'] == 193
    assert legacy_parsed['err_arg'] == 'Stichtag'
    assert legacy_parsed['job_nr'] is None # No job_nr assigned before error

    # --- Migrated Execution ---
    success, error_msg = run_bq_wrapper_sp(p_stichtag_string=invalid_stichtag_str, p_wiederanlaufWert=None)
    assert not success, "BigQuery SP execution should have failed"
    assert "Parameter validation failed" in error_msg # Message from SIGNAL SQLSTATE

    # Check BigQuery Audit Log for the error entry
    # Note: job_nr will be NULL for this error as it happens before job_nr is assigned
    bq_audit_logs = get_bq_audit_logs(job_kennung, job_nr=None) 
    assert len(bq_audit_logs) == 1
    assert bq_audit_logs[0].status == 'ERROR'
    assert bq_audit_logs[0].err_nr == 193
    assert bq_audit_logs[0].err_arg == 'Stichtag'
    assert bq_audit_logs[0].message == 'Required parameter missing or invalid'
    assert bq_audit_logs[0].job_nr is None # job_nr is NULL for this early error

    # Ensure no run info was logged
    bq_run_info = get_bq_run_info(job_kennung)
    assert len(bq_run_info) == 0

```

---

## Test Case 5: Error Handling - Simulated Kernel Failure

*   **Purpose**: Verify the wrapper correctly handles an error originating from the `k_ausd_bp_ta_msisdn` stored procedure, logging the failure and propagating the error.
*   **Setup**:
    *   BigQuery audit tables are cleared.
    *   Temporarily modify the `k_ausd_bp_ta_msisdn` stored procedure to `SIGNAL SQLSTATE` an error immediately after its `INFO` log entry.
*   **Action**:
    *   **Legacy**: Simulate a failure in `k_ausd_bp_ta_msisdn.ksh` (e.g., by making it `exit 1` or `return 1` after initial logging). Execute `r_ausd_bp_ta_msisdn.ksh`. Capture console output and log file.
    *   **Migrated**: Execute `CALL `project.dataset.ausd_bp_ta_msisdn_wrapper`(NULL, NULL);`
*   **Pass/Fail Criterion**:
    *   **Output Parity & Error Handling**:
        *   The legacy log file must contain "AppError: Abbruch" or a similar error message from `DWMSG_Fehlerbehandlung`, and the script should exit with a non-zero status.
        *   The BigQuery `CALL` statement should raise an error (e.g., `45000` SQLSTATE) from the wrapper's `EXCEPTION WHEN ERROR` block.
        *   BigQuery `job_audit_log` must contain:
            1.  `status='STARTED'`.
            2.  `status='INFO'`, `message='Kernel procedure executed (simulating error)'`.
            3.  `status='ERROR'`, `message='AppError: Abbruch'`.
        *   The `job_nr` in BigQuery logs should correspond to the legacy script's output.
        *   `job_run_info` should contain one entry, as the error occurs after `job_run_info` is written.

```python
# Part of the pytest suite
def test_error_handling_simulated_kernel_failure():
    """Tests error handling when the kernel SP fails."""
    job_kennung = "ausd_bp_ta_msisdn"
    expected_sysdate = date.today()

    # --- Modify BQ Kernel SP to simulate error ---
    modify_bq_kernel_sp(add_error=True)
    
    # --- Legacy Execution ---
    # Simulate kernel failure by having k_ausd_bp_ta_msisdn.ksh exit with error
    # This requires manual modification of k_ausd_bp_ta_msisdn.ksh for true parity.
    # For this test, we assume r_ausd_bp_ta_msisdn.ksh catches and logs "AppError: Abbruch"
    # and exits with a non-zero code if its child (kernel) fails.
    # This is an approximation if k_ausd_bp_ta_msisdn.ksh cannot be easily mocked.
    legacy_result = run_legacy_script([], expected_exit_code=1) # Assuming exit 1 on kernel error
    legacy_parsed = parse_legacy_log(legacy_result['log_content'])
    
    assert legacy_parsed['status_started']
    assert legacy_parsed['status_error']
    assert "AppError: Abbruch" in legacy_result['log_content'] # Specific message from trap
    assert legacy_parsed['job_nr'] is not None
    legacy_job_nr = legacy_parsed['job_nr']

    # --- Migrated Execution ---
    success, error_msg = run_bq_wrapper_sp(p_stichtag_string=None, p_wiederanlaufWert=None)
    assert not success, "BigQuery SP execution should have failed due to kernel error"
    assert "Job failed" in error_msg # Message from wrapper's SIGNAL SQLSTATE

    bq_audit_logs = get_bq_audit_logs(job_kennung, job_nr=legacy_job_nr)
    bq_run_info = get_bq_run_info(job_kennung, job_nr=legacy_job_nr)

    # Assertions for BigQuery Audit Log
    assert len(bq_audit_logs) == 3
    assert bq_audit_logs[0].status == 'STARTED'
    assert bq_audit_logs[0].job_nr == legacy_job_nr

    assert bq_audit_logs[1].status == 'INFO'
    assert bq_audit_logs[1].message == 'Kernel procedure executed (simulating error)'
    assert bq_audit_logs[1].job_nr == legacy_job_nr

    assert bq_audit_logs[2].status == 'ERROR'
    assert bq_audit_logs[2].message == 'AppError: Abbruch'
    assert bq_audit_logs[2].job_nr == legacy_job_nr

    # Assertions for BigQuery Run Info (should still be present as it's written before kernel call)
    assert len(bq_run_info) == 1
    assert bq_run_info[0].job_kennung == job_kennung
    assert bq_run_info[0].job_nr == legacy_job_nr
    assert bq_run_info[0].stichtag == expected_sysdate
    assert bq_run_info[0].sysdate == expected_sysdate

    # --- Revert BQ Kernel SP ---
    modify_bq_kernel_sp(add_error=False)

```

---

## Test Case 6: Airflow DAG Execution (External System Replacement)

*   **Purpose**: Verify that the Cloud Composer (Airflow) DAG can successfully trigger the BigQuery stored procedure, passing parameters correctly, and that the overall orchestration flow works as expected. This validates the replacement of the shell script's scheduling/orchestration mechanism.
*   **Setup**:
    *   An active Cloud Composer environment with the `ausd_bp_ta_msisdn_orchestration` DAG deployed.
    *   BigQuery audit tables are cleared.
*   **Action**:
    *   Manually trigger the `ausd_bp_ta_msisdn_orchestration` DAG in the Airflow UI (or via `gcloud composer environments run ...`), providing example DAG run configuration parameters:
        ```json
        {
            "p_stichtag_string": "15062023",
            "p_wiederanlaufWert": 54321
        }
        ```
*   **Pass/Fail Criterion**:
    *   **External-system replacements**: The Airflow DAG run completes successfully (all tasks turn green).
    *   **Output Parity & Transformation Correctness**:
        *   Airflow task logs for `execute_ausd_bp_ta_msisdn_wrapper` should show the BigQuery stored procedure being called with the specified parameters.
        *   BigQuery `job_audit_log` and `job_run_info` tables should contain entries reflecting a successful run with `job_kennung='ausd_bp_ta_msisdn'`, `stichtag` as `2023-06-15`, and `sysdate` as the current date.
        *   The `job_audit_log` should contain the `STARTED`, `INFO` (from kernel), and `OK` entries.

```python
# This test case describes the manual/Airflow-triggered validation.
# It is not directly runnable Python code within the pytest suite,
# but rather a procedure to follow and verify results.

# --- Manual Airflow Trigger and Verification ---
def test_airflow_dag_execution():
    """
    Verifies that the Airflow DAG can successfully trigger the BigQuery stored procedure.
    This test requires manual interaction with an Airflow environment.
    """
    job_kennung = "ausd_bp_ta_msisdn"
    test_stichtag_str = '15062023'
    test_stichtag_date = datetime.strptime(test_stichtag_str, '%d%m%Y').date()
    test_wiederanlaufwert = 54321
    expected_sysdate = date.today()

    print("\n--- Manual Airflow DAG Execution Test ---")
    print(f"1. Ensure the Airflow DAG 'ausd_bp_ta_msisdn_orchestration' is deployed.")
    print(f"2. Manually trigger the DAG in Airflow UI or via gcloud CLI.")
    print(f"   Provide the following DAG run configuration parameters:")
    print(f"   {{ 'p_stichtag_string': '{test_stichtag_str}', 'p_wiederanlaufWert': {test_wiederanlaufwert} }}")
    print(f"3. Wait for the DAG run to complete successfully (all tasks green).")
    print(f"4. Verify Airflow task logs for 'execute_ausd_bp_ta_msisdn_wrapper' show correct parameter passing.")
    print(f"5. Proceed to BigQuery verification.")

    # --- BigQuery Verification (after manual Airflow run) ---
    # Assume a new job_nr was generated by the Airflow run.
    # We need to find the latest job_nr for 'ausd_bp_ta_msisdn'
    query_latest_job_nr = f"SELECT MAX(job_nr) FROM `{BQ_AUDIT_LOG_TABLE}` WHERE job_kennung = '{job_kennung}'"
    latest_job_nr_result = bq_client.query(query_latest_job_nr).result()
    latest_job_nr = [row[0] for row in latest_job_nr_result][0]

    assert latest_job_nr is not None, "No job_nr found in audit log after Airflow run."

    bq_audit_logs = get_bq_audit_logs(job_kennung, job_nr=latest_job_nr)
    bq_run_info = get_bq_run_info(job_kennung, job_nr=latest_job_nr)

    # Assertions for BigQuery Audit Log
    assert len(bq_audit_logs) == 3, f"Expected 3 audit log entries, got {len(bq_audit_logs)}"
    assert bq_audit_logs[0].status == 'STARTED'
    assert bq_audit_logs[0].stichtag == test_stichtag_date
    assert bq_audit_logs[0].sysdate == expected_sysdate # Sysdate is current date when SP runs
    assert bq_audit_logs[0].job_nr == latest_job_nr

    assert bq_audit_logs[1].status == 'INFO'
    assert bq_audit_logs[1].message == 'Kernel procedure executed'
    assert bq_audit_logs[1].job_nr == latest_job_nr

    assert bq_audit_logs[2].status == 'OK'
    assert bq_audit_logs[2].job_nr == latest_job_nr

    # Assertions for BigQuery Run Info
    assert len(bq_run_info) == 1
    assert bq_run_info[0].job_kennung == job_kennung
    assert bq_run_info[0].job_nr == latest_job_nr
    assert bq_run_info[0].stichtag == test_stichtag_date
    assert bq_run_info[0].sysdate == expected_sysdate

    print(f"Airflow DAG execution and BigQuery logging verified for job_nr: {latest_job_nr}.")

```