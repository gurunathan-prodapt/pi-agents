The migration of `r_ausd_v_ta_acc_ref.ksh` focuses on transforming a KornShell wrapper script into a BigQuery stored procedure (`vertragsdatenabgleich_wrapper`) that orchestrates a core logic procedure (`k_ausd_v_ta_acc_ref`) and utilizes BigQuery tables for logging. The tests below validate this migration, ensuring behavioral equivalence and correctness across various scenarios.

**Testing Environment Setup (Conceptual Pytest Fixtures)**

To execute these tests, you would typically use a testing framework like `pytest` in conjunction with the Google Cloud BigQuery client library. The following conceptual setup demonstrates how the BigQuery environment would be prepared and cleaned for each test.

```python
import pytest
from google.cloud import bigquery
import subprocess
import os
import json
import time
from datetime import datetime, timezone

# --- Configuration ---
# Replace with your actual GCP Project ID and BigQuery Dataset ID
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_dataset_id"

# BigQuery table and procedure references
AUDIT_LOG_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.job_audit_log`"
ERROR_LOG_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.job_error_log`"
WRAPPER_SP = f"`{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`"
CORE_SP = f"`{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_acc_ref`"

# BigQuery Client instance
bq_client = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions for BigQuery Interaction ---
def run_bq_sp(sp_name: str, params: dict) -> dict:
    """
    Helper to execute a BigQuery stored procedure and capture its outcome.
    Returns a dictionary indicating success/failure and any error message.
    """
    param_str = ", ".join([f"{k} => {v}" for k, v in params.items()])
    query = f"CALL {sp_name}({param_str});"
    try:
        job = bq_client.query(query)
        job.result()  # Wait for job to complete
        return {"success": True, "message": "SP executed successfully."}
    except Exception as e:
        return {"success": False, "message": str(e)}

def get_bq_table_data(table_name: str) -> list[dict]:
    """Helper to fetch all data from a BigQuery table."""
    query = f"SELECT * FROM {table_name} ORDER BY insert_timestamp ASC;"
    rows = bq_client.query(query).result()
    return [dict(row) for row in rows]

def get_bq_table_row_count(table_name: str) -> int:
    """Helper to get row count from a BigQuery table."""
    query = f"SELECT COUNT(*) FROM {table_name};"
    rows = bq_client.query(query).result()
    return next(iter(rows))[0]

# --- Pytest Fixtures ---
@pytest.fixture(scope="module", autouse=True)
def setup_bigquery_environment():
    """
    Module-scoped fixture to ensure BigQuery dataset, tables, and stored procedures
    are created or replaced before any tests run.
    """
    # Create dataset if it doesn't exist
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))

    # Create/Replace job_audit_log table
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_LOG_TABLE} (
            job_name STRING NOT NULL,
            job_entry_number STRING NOT NULL,
            start_timestamp TIMESTAMP NOT NULL,
            end_timestamp TIMESTAMP,
            status STRING NOT NULL,
            message STRING,
            parameters JSON,
            insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
    """).result()

    # Create/Replace job_error_log table
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS {ERROR_LOG_TABLE} (
            job_name STRING NOT NULL,
            job_entry_number STRING NOT NULL,
            error_timestamp TIMESTAMP NOT NULL,
            error_message STRING NOT NULL,
            error_code STRING,
            stack_trace STRING,
            insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
    """).result()

    # Create/Replace k_ausd_v_ta_acc_ref (controllable for tests)
    # This procedure will be re-created by individual tests to simulate success/failure.
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE {CORE_SP}(
            IN p_job_kennung STRING,
            IN p_dw_eintrags_nr STRING,
            OUT p_return_code INT64,
            OUT p_return_message STRING
        )
        BEGIN
            -- Default to success; tests will override this by re-creating the SP
            SET p_return_code = 0;
            SET p_return_message = 'k_ausd_v_ta_acc_ref completed successfully (default).';
        END;
    """).result()

    # Create/Replace vertragsdatenabgleich_wrapper
    # This is the migrated wrapper code provided in the design document.
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE {WRAPPER_SP}(
            IN p_s_parameter STRING,
            IN p_l_parameter STRING,
            IN p_h_flag BOOL
        )
        BEGIN
            DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_ACC_REF';
            DECLARE v_job_entry_number STRING;
            DECLARE v_start_timestamp TIMESTAMP;
            DECLARE v_end_timestamp TIMESTAMP;
            DECLARE v_status STRING;
            DECLARE v_message STRING;
            DECLARE v_return_code INT64;
            DECLARE v_return_message STRING;

            SET v_job_entry_number = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()) || '_' || GENERATE_UUID();
            SET v_start_timestamp = CURRENT_TIMESTAMP();

            IF p_h_flag THEN
                SELECT '''Usage: CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(p_s_parameter => <string>, p_l_parameter => <string>, p_h_flag => FALSE);
                -h: Display this help message.
                -s: System parameter.
                -l: Log file name (for logging purposes).''';
                RETURN;
            END IF;

            IF p_s_parameter IS NULL OR p_l_parameter IS NULL THEN
                SET v_status = 'FAILED';
                SET v_message = 'Missing required parameters. Both -s and -l are mandatory.';
                INSERT INTO {AUDIT_LOG_TABLE} (job_name, job_entry_number, start_timestamp, end_timestamp, status, message, parameters)
                VALUES (v_job_kennung, v_job_entry_number, v_start_timestamp, CURRENT_TIMESTAMP(), v_status, v_message, TO_JSON(STRUCT(p_s_parameter AS s_param, p_l_parameter AS l_param)));

                INSERT INTO {ERROR_LOG_TABLE} (job_name, job_entry_number, error_timestamp, error_message, error_code)
                VALUES (v_job_kennung, v_job_entry_number, CURRENT_TIMESTAMP(), v_message, 'PARAM_MISSING');
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
            END IF;

            INSERT INTO {AUDIT_LOG_TABLE} (job_name, job_entry_number, start_timestamp, status, message, parameters)
            VALUES (v_job_kennung, v_job_entry_number, v_start_timestamp, 'STARTED', 'Job execution started.', TO_JSON(STRUCT(p_s_parameter AS s_param, p_l_parameter AS l_param)));

            BEGIN
                CALL {CORE_SP}(
                    p_job_kennung => v_job_kennung,
                    p_dw_eintrags_nr => v_job_entry_number,
                    p_return_code => v_return_code,
                    p_return_message => v_return_message
                );

                IF v_return_code = 0 THEN
                    SET v_status = 'COMPLETED';
                    SET v_message = 'Core logic executed successfully: ' || v_return_message;
                ELSE
                    SET v_status = 'FAILED';
                    SET v_message = 'Core logic failed: ' || v_return_message;
                    INSERT INTO {ERROR_LOG_TABLE} (job_name, job_entry_number, error_timestamp, error_message, error_code)
                    VALUES (v_job_kennung, v_job_entry_number, CURRENT_TIMESTAMP(), v_message, 'CORE_LOGIC_FAILED');
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
                END IF;

            EXCEPTION WHEN OTHERS THEN
                SET v_status = 'FAILED';
                SET v_message = 'An unexpected error occurred in the wrapper: ' || @@error.message;
                INSERT INTO {ERROR_LOG_TABLE} (job_name, job_entry_number, error_timestamp, error_message, error_code, stack_trace)
                VALUES (v_job_kennung, v_job_entry_number, CURRENT_TIMESTAMP(), v_message, 'UNEXPECTED_ERROR', @@error.stack_trace);
            END;

            SET v_end_timestamp = CURRENT_TIMESTAMP();

            UPDATE {AUDIT_LOG_TABLE}
            SET
                end_timestamp = v_end_timestamp,
                status = v_status,
                message = v_message
            WHERE job_name = v_job_kennung AND job_entry_number = v_job_entry_number AND start_timestamp = v_start_timestamp;

        END;
    """).result()

    yield # Tests run here

    # Teardown: Clean up tables (optional, but good for isolated testing)
    # bq_client.query(f"TRUNCATE TABLE {AUDIT_LOG_TABLE}").result()
    # bq_client.query(f"TRUNCATE TABLE {ERROR_LOG_TABLE}").result()

@pytest.fixture(autouse=True)
def cleanup_logs_before_each_test():
    """
    Function-scoped fixture to clear log tables and reset CORE_SP before each test.
    Ensures test isolation.
    """
    bq_client.query(f"TRUNCATE TABLE {AUDIT_LOG_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {ERROR_LOG_TABLE}").result()
    # Reset CORE_SP to default success state for the next test
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE {CORE_SP}(
            IN p_job_kennung STRING,
            IN p_dw_eintrags_nr STRING,
            OUT p_return_code INT64,
            OUT p_return_message STRING
        )
        BEGIN
            SET p_return_code = 0;
            SET p_return_message = 'k_ausd_v_ta_acc_ref completed successfully (default).';
        END;
    """).result()
```

---

### Test Case 1: Successful Execution (Happy Path)

**Purpose:**
To verify that the migrated `vertragsdatenabgleich_wrapper` BigQuery stored procedure executes successfully when all parameters are provided and the core logic (`k_ausd_v_ta_acc_ref`) also succeeds. This test ensures correct logging, parameter handling, and orchestration flow.

**Setup:**
1.  The `job_audit_log` and `job_error_log` tables are empty (handled by `cleanup_logs_before_each_test` fixture).
2.  The `k_ausd_v_ta_acc_ref` stored procedure is configured to return a success code (`p_return_code = 0`). This is the default behavior of the `CORE_SP` after `cleanup_logs_before_each_test`.

**Action:**
1.  Call the `project.dataset.vertragsdatenabgleich_wrapper` BigQuery stored procedure with valid `p_s_parameter` and `p_l_parameter`, and `p_h_flag = FALSE`.

```python
def test_successful_execution():
    """
    Test case for successful execution of the wrapper and core logic.
    """
    wrapper_params = {
        "p_s_parameter": "'test_system_success'",
        "p_l_parameter": "'test_log_file_success'",
        "p_h_flag": "FALSE"
    }
    
    # Action: Call the BigQuery stored procedure
    result = run_bq_sp(WRAPPER_SP, wrapper_params)
    assert result["success"] is True, f"Wrapper SP failed unexpectedly: {result['message']}"

    # Assertions
    audit_logs = get_bq_table_data(AUDIT_LOG_TABLE)
    error_logs = get_bq_table_data(ERROR_LOG_TABLE)

    # Output Parity & Data Quality
    assert len(audit_logs) == 2, "Expected 2 audit log entries (STARTED, COMPLETED)"
    assert len(error_logs) == 0, "Expected no error log entries"

    started_log = next((log for log in audit_logs if log['status'] == 'STARTED'), None)
    completed_log = next((log for log in audit_logs if log['status'] == 'COMPLETED'), None)

    assert started_log is not None, "Missing 'STARTED' log entry"
    assert completed_log is not None, "Missing 'COMPLETED' log entry"

    # Transformation Correctness
    assert started_log['job_name'] == 'BERT_V_TA_ACC_REF'
    assert completed_log['job_name'] == 'BERT_V_TA_ACC_REF'
    assert started_log['job_entry_number'] == completed_log['job_entry_number'], \
        "Job entry numbers should match for STARTED and COMPLETED entries"
    
    expected_params_json = json.dumps({"s_param": "test_system_success", "l_param": "test_log_file_success"})
    assert json.loads(started_log['parameters']) == json.loads(expected_params_json), \
        "Parameters in STARTED log entry do not match"
    assert "Core logic executed successfully" in completed_log['message']
    assert completed_log['end_timestamp'] is not None
    assert completed_log['end_timestamp'] >= started_log['start_timestamp']

    # External-system replacements: Implied by successful completion path.
    # The wrapper successfully called CORE_SP and processed its success return.
```

---

### Test Case 2: Core Logic Failure

**Purpose:**
To verify that the migrated `vertragsdatenabgleich_wrapper` correctly handles a failure originating from the invoked core logic (`k_ausd_v_ta_acc_ref`), logs the error, and updates the job status accordingly.

**Setup:**
1.  The `job_audit_log` and `job_error_log` tables are empty (handled by `cleanup_logs_before_each_test` fixture).
2.  Temporarily modify the `k_ausd_v_ta_acc_ref` stored procedure to return a failure code (`p_return_code = 1`) and an error message.

**Action:**
1.  Call the `project.dataset.vertragsdatenabgleich_wrapper` BigQuery stored procedure with valid `p_s_parameter` and `p_l_parameter`, and `p_h_flag = FALSE`.

```python
def test_core_logic_failure():
    """
    Test case for wrapper handling of core logic failure.
    """
    # Setup: Configure CORE_SP to fail
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE {CORE_SP}(
            IN p_job_kennung STRING,
            IN p_dw_eintrags_nr STRING,
            OUT p_return_code INT64,
            OUT p_return_message STRING
        )
        BEGIN
            SET p_return_code = 1;
            SET p_return_message = 'Simulated failure in k_ausd_v_ta_acc_ref.';
        END;
    """).result()

    wrapper_params = {
        "p_s_parameter": "'test_system_fail'",
        "p_l_parameter": "'test_log_file_fail'",
        "p_h_flag": "FALSE"
    }
    
    # Action: Call the BigQuery stored procedure
    result = run_bq_sp(WRAPPER_SP, wrapper_params)
    assert result["success"] is False, "Wrapper SP should have failed due to core logic error"
    assert "Core logic failed" in result["message"]

    # Assertions
    audit_logs = get_bq_table_data(AUDIT_LOG_TABLE)
    error_logs = get_bq_table_data(ERROR_LOG_TABLE)

    # Output Parity & Data Quality
    assert len(audit_logs) == 2, "Expected 2 audit log entries (STARTED, FAILED)"
    assert len(error_logs) == 1, "Expected 1 error log entry"

    started_log = next((log for log in audit_logs if log['status'] == 'STARTED'), None)
    failed_log = next((log for log in audit_logs if log['status'] == 'FAILED'), None)
    error_log_entry = error_logs[0]

    assert started_log is not None, "Missing 'STARTED' log entry"
    assert failed_log is not None, "Missing 'FAILED' log entry"

    # Transformation Correctness
    assert started_log['job_name'] == 'BERT_V_TA_ACC_REF'
    assert failed_log['job_name'] == 'BERT_V_TA_ACC_REF'
    assert started_log['job_entry_number'] == failed_log['job_entry_number'], \
        "Job entry numbers should match for STARTED and FAILED entries"
    
    expected_params_json = json.dumps({"s_param": "test_system_fail", "l_param": "test_log_file_fail"})
    assert json.loads(started_log['parameters']) == json.loads(expected_params_json), \
        "Parameters in STARTED log entry do not match"
    assert "Core logic failed: Simulated failure in k_ausd_v_ta_acc_ref." in failed_log['message']
    assert failed_log['end_timestamp'] is not None

    assert error_log_entry['job_name'] == 'BERT_V_TA_ACC_REF'
    assert error_log_entry['job_entry_number'] == started_log['job_entry_number']
    assert error_log_entry['error_message'] == 'Core logic failed: Simulated failure in k_ausd_v_ta_acc_ref.'
    assert error_log_entry['error_code'] == 'CORE_LOGIC_FAILED'
```

---

### Test Case 3: Missing Required Parameters

**Purpose:**
To verify that the migrated `vertragsdatenabgleich_wrapper` correctly identifies and handles missing required parameters (`p_s_parameter` or `p_l_parameter`), logs the error, and signals an error.

**Setup:**
1.  The `job_audit_log` and `job_error_log` tables are empty (handled by `cleanup_logs_before_each_test` fixture).
2.  The `k_ausd_v_ta_acc_ref` stored procedure is in its default success state, as it should not be invoked.

**Action:**
1.  Call the `project.dataset.vertragsdatenabgleich_wrapper` BigQuery stored procedure with `p_s_parameter = NULL` (or omitted) and a valid `p_l_parameter`, and `p_h_flag = FALSE`.
2.  Repeat the action for `p_l_parameter = NULL`.

```python
def test_missing_s_parameter():
    """
    Test case for wrapper handling of missing -s parameter.
    """
    wrapper_params = {
        "p_s_parameter": "NULL",
        "p_l_parameter": "'test_log_file_missing_s'",
        "p_h_flag": "FALSE"
    }
    
    # Action: Call the BigQuery stored procedure
    result = run_bq_sp(WRAPPER_SP, wrapper_params)
    assert result["success"] is False, "Wrapper SP should have failed due to missing parameter"
    assert "Missing required parameters" in result["message"]

    # Assertions
    audit_logs = get_bq_table_data(AUDIT_LOG_TABLE)
    error_logs = get_bq_table_data(ERROR_LOG_TABLE)

    # Output Parity & Data Quality
    assert len(audit_logs) == 1, "Expected 1 audit log entry (FAILED) for parameter validation error"
    assert len(error_logs) == 1, "Expected 1 error log entry for parameter validation error"

    failed_log = audit_logs[0]
    error_log_entry = error_logs[0]

    assert failed_log['status'] == 'FAILED'
    assert "Missing required parameters" in failed_log['message']
    assert failed_log['job_name'] == 'BERT_V_TA_ACC_REF'
    assert failed_log['end_timestamp'] is not None

    assert error_log_entry['job_name'] == 'BERT_V_TA_ACC_REF'
    assert error_log_entry['job_entry_number'] == failed_log['job_entry_number']
    assert error_log_entry['error_message'] == 'Missing required parameters. Both -s and -l are mandatory.'
    assert error_log_entry['error_code'] == 'PARAM_MISSING'

    # External-system replacements: k_ausd_v_ta_acc_ref was NOT called.
    # This is implicitly verified because the wrapper errors out before the CALL statement.

def test_missing_l_parameter():
    """
    Test case for wrapper handling of missing -l parameter.
    """
    wrapper_params = {
        "p_s_parameter": "'test_system_missing_l'",
        "p_l_parameter": "NULL",
        "p_h_flag": "FALSE"
    }
    
    # Action: Call the BigQuery stored procedure
    result = run_bq_sp(WRAPPER_SP, wrapper_params)
    assert result["success"] is False, "Wrapper SP should have failed due to missing parameter"
    assert "Missing required parameters" in result["message"]

    # Assertions (similar to missing_s_parameter)
    audit_logs = get_bq_table_data(AUDIT_LOG_TABLE)
    error_logs = get_bq_table_data(ERROR_LOG_TABLE)

    assert len(audit_logs) == 1
    assert len(error_logs) == 1
    failed_log = audit_logs[0]
    error_log_entry = error_logs[0]
    assert failed_log['status'] == 'FAILED'
    assert "Missing required parameters" in failed_log['message']
    assert error_log_entry['error_code'] == 'PARAM_MISSING'
```

---

### Test Case 4: Help Flag Invocation

**Purpose:**
To verify that when the help flag (`-h`) is passed, the migrated `vertragsdatenabgleich_wrapper` displays the usage information and exits immediately without performing any core logic or logging.

**Setup:**
1.  The `job_audit_log` and `job_error_log` tables are empty (handled by `cleanup_logs_before_each_test` fixture).
2.  The `k_ausd_v_ta_acc_ref` stored procedure is in its default success state, as it should not be invoked.

**Action:**
1.  Call the `project.dataset.vertragsdatenabgleich_wrapper` BigQuery stored procedure with `p_h_flag = TRUE`. Other parameters can be `NULL` or dummy values, as they should be ignored.

```python
def test_help_flag_invocation():
    """
    Test case for wrapper handling of the -h (help) flag.
    """
    wrapper_params = {
        "p_s_parameter": "NULL",
        "p_l_parameter": "NULL",
        "p_h_flag": "TRUE"
    }
    
    # Action: Call the BigQuery stored procedure
    # Note: BQ SPs don't return stdout directly like shell scripts.
    # The 'SELECT' statement in the SP will be part of the query results.
    query = f"CALL {WRAPPER_SP}(p_s_parameter => NULL, p_l_parameter => NULL, p_h_flag => TRUE);"
    job = bq_client.query(query)
    rows = list(job.result()) # Get results from the SELECT statement inside the SP

    # Assertions
    audit_logs = get_bq_table_data(AUDIT_LOG_TABLE)
    error_logs = get_bq_table_data(ERROR_LOG_TABLE)

    # Output Parity & Data Quality
    assert len(audit_logs) == 0, "Expected no audit log entries when help flag is used"
    assert len(error_logs) == 0, "Expected no error log entries when help flag is used"

    # Transformation Correctness: Check the usage message
    assert len(rows) == 1, "Expected one row containing the usage message"
    assert rows[0][0].startswith(f"Usage: CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`")
    assert "-h: Display this help message." in rows[0][0]

    # External-system replacements: k_ausd_v_ta_acc_ref should not be called.
    # This is implicitly verified by the absence of log entries and early return.
```

---

### Test Case 5: Unexpected Internal Wrapper Error

**Purpose:**
To verify that the migrated `vertragsdatenabgleich_wrapper` correctly handles an unexpected error within its own logic (e.g., a SQL error not related to core logic return code), logs it, and updates the job status. This tests the `EXCEPTION WHEN OTHERS` block.

**Setup:**
1.  The `job_audit_log` and `job_error_log` tables are empty (handled by `cleanup_logs_before_each_test` fixture).
2.  Temporarily modify the `k_ausd_v_ta_acc_ref` stored procedure to cause an *unexpected* error by signaling an SQLSTATE error directly.

**Action:**
1.  Modify `k_ausd_v_ta_acc_ref` to `SIGNAL SQLSTATE '45000'` with a custom message.
2.  Call the `project.dataset.vertragsdatenabgleich_wrapper` BigQuery stored procedure with valid parameters.

```python
def test_unexpected_internal_wrapper_error():
    """
    Test case for wrapper handling of an unexpected internal error (e.g., in core SP).
    """
    # Setup: Configure CORE_SP to raise an unexpected error (SIGNAL SQLSTATE)
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE {CORE_SP}(
            IN p_job_kennung STRING,
            IN p_dw_eintrags_nr STRING,
            OUT p_return_code INT64,
            OUT p_return_message STRING
        )
        BEGIN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated unexpected internal error in k_ausd_v_ta_acc_ref.';
        END;
    """).result()

    wrapper_params = {
        "p_s_parameter": "'test_system_unexpected'",
        "p_l_parameter": "'test_log_file_unexpected'",
        "p_h_flag": "FALSE"
    }
    
    # Action: Call the BigQuery stored procedure
    result = run_bq_sp(WRAPPER_SP, wrapper_params)
    assert result["success"] is False, "Wrapper SP should have failed due to unexpected error"
    assert "An unexpected error occurred in the wrapper" in result["message"]

    # Assertions
    audit_logs = get_bq_table_data(AUDIT_LOG_TABLE)
    error_logs = get_bq_table_data(ERROR_LOG_TABLE)

    # Output Parity & Data Quality
    assert len(audit_logs) == 2, "Expected 2 audit log entries (STARTED, FAILED)"
    assert len(error_logs) == 1, "Expected 1 error log entry"

    started_log = next((log for log in audit_logs if log['status'] == 'STARTED'), None)
    failed_log = next((log for log in audit_logs if log['status'] == 'FAILED'), None)
    error_log_entry = error_logs[0]

    assert started_log is not None, "Missing 'STARTED' log entry"
    assert failed_log is not None, "Missing 'FAILED' log entry"

    # Transformation Correctness
    assert started_log['job_name'] == 'BERT_V_TA_ACC_REF'
    assert failed_log['job_name'] == 'BERT_V_TA_ACC_REF'
    assert started_log['job_entry_number'] == failed_log['job_entry_number']
    
    assert "An unexpected error occurred in the wrapper" in failed_log['message']
    assert failed_log['end_timestamp'] is not None

    assert error_log_entry['job_name'] == 'BERT_V_TA_ACC_REF'
    assert error_log_entry['job_entry_number'] == started_log['job_entry_number']
    assert "Simulated unexpected internal error in k_ausd_v_ta_acc_ref." in error_log_entry['error_message']
    assert error_log_entry['error_code'] == 'UNEXPECTED_ERROR'
    assert error_log_entry['stack_trace'] is not None and len(error_log_entry['stack_trace']) > 0
```