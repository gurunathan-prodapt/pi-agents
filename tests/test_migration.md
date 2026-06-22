As a senior data-migration QA engineer, I've designed a suite of validation tests for the migrated `r_ausd_bp_ta_bcp_msisdn.ksh` job. These tests focus on ensuring the BigQuery Stored Procedure (`sp_r_ausd_bp_ta_bcp_msisdn`) behaves identically to the legacy KornShell script in terms of its orchestration, parameter handling, logging, and error management.

Given that the core business logic (`k_ausd_bp_ta_bcp_msisdn.ksh`) is migrated to a separate BigQuery Stored Procedure (`sp_ausd_bp_ta_bcp_msisdn_kernel`) and its details are not part of this design, the tests for the wrapper will use a mock kernel SP to simulate success and failure scenarios.

The tests are categorized to address output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

### **Prerequisites for Running Tests**

Before running these tests, ensure the following:

1.  **BigQuery Project and Dataset**: A BigQuery project and dataset are set up (e.g., `your-gcp-project-id.your_dataset`).
2.  **BigQuery Client**: Python environment with `google-cloud-bigquery` installed.
3.  **Authentication**: Your environment is authenticated to GCP with sufficient permissions to create/truncate tables, create/call stored procedures, and query BigQuery.
4.  **DDL Execution**: The DDL for `job_control`, `job_log`, and `job_error_log` tables, as well as the `sp_r_ausd_bp_ta_bcp_msisdn` and the mock `sp_ausd_bp_ta_bcp_msisdn_kernel` stored procedures, must be deployed to your BigQuery environment. The provided `pytest` setup will ensure this.

---

### **Common Test Setup (Pytest Fixtures)**

The following `pytest` fixtures (typically in `conftest.py`) provide a BigQuery client and handle the setup/teardown of logging tables and the mock kernel SP for each test.

```python
# conftest.py
import pytest
from google.cloud import bigquery
import os
from datetime import datetime
import pytz

# --- Configuration ---
# Replace with your actual GCP project ID and BigQuery dataset ID
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "your_dataset")

@pytest.fixture(scope="module")
def bigquery_client():
    """Provides a BigQuery client for the test module."""
    client = bigquery.Client(project=PROJECT_ID)
    # Store dataset_id on client for easier access in helper functions
    client.dataset_id = DATASET_ID
    yield client
    client.close()

@pytest.fixture(scope="function", autouse=True)
def setup_teardown_tables(bigquery_client):
    """
    Clears logging tables before each test and ensures mock kernel SP and
    logging tables exist.
    """
    
    # Clear logging tables
    tables_to_clear = ["job_control", "job_log", "job_error_log"]
    for table_name in tables_to_clear:
        try:
            bigquery_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.{table_name}`").result()
            print(f"Truncated table: {table_name}")
        except Exception as e:
            print(f"Warning: Could not truncate table {table_name}. It might not exist or is empty. Error: {e}")

    # Ensure logging tables exist (run DDL if not already done)
    ddl_statements = [
        f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.job_control` (
            job_run_id STRING NOT NULL, job_name STRING NOT NULL, start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP, status STRING NOT NULL, stichtag_param STRING,
            wiederanlauf_wert_param INT64, error_message STRING
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.job_log` (
            log_id STRING NOT NULL, job_run_id STRING NOT NULL, log_time TIMESTAMP NOT NULL,
            log_level STRING NOT NULL, message STRING NOT NULL, step STRING
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.job_error_log` (
            error_id STRING NOT NULL, job_run_id STRING NOT NULL, error_time TIMESTAMP NOT NULL,
            error_type STRING NOT NULL, error_message STRING NOT NULL, stack_trace STRING, source_file STRING
        );
        """
    ]
    for ddl in ddl_statements:
        bigquery_client.query(ddl).result()
        print(f"Ensured DDL for logging table.")

    # Ensure mock kernel SP exists
    mock_kernel_sp_ddl = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.sp_ausd_bp_ta_bcp_msisdn_kernel`(
        IN p_stichtag STRING,
        IN p_wiederanlaufWert INT64
    )
    BEGIN
        -- Simulate success by default.
        -- To simulate failure, pass 'KERNEL_FAIL' as p_stichtag.
        IF p_stichtag = 'KERNEL_FAIL' THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel error for testing purposes.';
        END IF;
    END;
    """
    bigquery_client.query(mock_kernel_sp_ddl).result()
    print(f"Ensured mock kernel SP exists.")

    yield # Run the test

    # Teardown (optional, can clear tables again if desired)
    # For now, `autouse=True` and `setup_teardown_tables` clears before each test.
```

### **Helper Functions (for `pytest` tests)**

```python
# test_r_ausd_bp_ta_bcp_msisdn.py (or a separate helpers.py)
import pytest
from google.cloud import bigquery
from datetime import datetime
import pytz

# Helper function to call the main SP
def call_main_sp(client, p_stichtag, p_wiederanlaufWert):
    project_id = client.project
    dataset_id = client.dataset_id # Access dataset_id from the client fixture

    stichtag_arg = f"'{p_stichtag}'" if p_stichtag is not None else "NULL"
    wiederanlauf_arg = str(p_wiederanlaufWert) if p_wiederanlaufWert is not None else "NULL"

    query = f"""
    CALL `{project_id}.{dataset_id}.sp_r_ausd_bp_ta_bcp_msisdn`({stichtag_arg}, {wiederanlauf_arg});
    """
    print(f"Executing: {query}")
    try:
        client.query(query).result()
        return True, None
    except Exception as e:
        return False, str(e)

# Helper function to fetch rows from a table
def fetch_rows(client, table_name, order_by=None):
    project_id = client.project
    dataset_id = client.dataset_id # Access dataset_id from the client fixture
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
    if order_by:
        query += f" ORDER BY {order_by}"
    rows = list(client.query(query).result())
    return rows

# Helper to get current date in DDMMYYYY format (UTC to match BigQuery's CURRENT_DATE())
def get_current_date_ddmmyyyy():
    return datetime.now(pytz.utc).strftime('%d%m%Y')
```

---

### **Migration Validation Test Cases**

#### 1. Test Case: Successful Execution with All Parameters Provided

*   **Purpose**: Verify the wrapper script executes successfully when both `p_stichtag` and `p_wiederanlaufWert` are explicitly provided, and the invoked kernel SP also succeeds. This covers **output parity** (parameters passed to kernel) and **external system replacement** (logging).
*   **Setup**:
    *   Logging tables (`job_control`, `job_log`, `job_error_log`) are cleared.
    *   The mock `sp_ausd_bp_ta_bcp_msisdn_kernel` is configured to succeed (default behavior).
*   **Action**: Call `sp_r_ausd_bp_ta_bcp_msisdn` with a valid `p_stichtag` (e.g., '01012023') and `p_wiederanlaufWert` (e.g., 100).
*   **Pass/Fail Criterion**:
    *   The `CALL` to the main SP completes without raising an error.
    *   `job_control` table contains exactly one entry with:
        *   `status = 'SUCCESS'`
        *   `stichtag_param = '01012023'`
        *   `wiederanlauf_wert_param = 100`
        *   `end_time` is populated.
        *   `error_message` is `NULL`.
    *   `job_log` table contains INFO entries for: "Job started.", "Calling kernel stored procedure...", "Kernel stored procedure... completed successfully.", and "Job completed successfully.". No ERROR entries are present.
    *   `job_error_log` table is empty.
*   **Runnable Test Code (Pytest)**:
    ```python
    # test_r_ausd_bp_ta_bcp_msisdn.py
    def test_successful_execution_all_params(bigquery_client, setup_teardown_tables):
        stichtag = '01012023'
        wiederanlauf_wert = 100

        success, error_msg = call_main_sp(bigquery_client, stichtag, wiederanlauf_wert)
        assert success, f"SP call failed unexpectedly: {error_msg}"

        # Assert job_control
        job_control_rows = fetch_rows(bigquery_client, "job_control")
        assert len(job_control_rows) == 1
        jc = job_control_rows[0]
        assert jc.status == 'SUCCESS'
        assert jc.stichtag_param == stichtag
        assert jc.wiederanlauf_wert_param == wiederanlauf_wert
        assert jc.end_time is not None
        assert jc.error_message is None

        # Assert job_log
        job_log_rows = fetch_rows(bigquery_client, "job_log", order_by="log_time")
        assert len(job_log_rows) >= 4 # At least start, kernel call, kernel success, job success
        assert any("Job started." in r.message for r in job_log_rows)
        assert any(f"Calling kernel stored procedure sp_ausd_bp_ta_bcp_msisdn_kernel with stichtag: {stichtag}, wiederanlaufWert: {wiederanlauf_wert}." in r.message for r in job_log_rows)
        assert any("Kernel stored procedure sp_ausd_bp_ta_bcp_msisdn_kernel completed successfully." in r.message for r in job_log_rows)
        assert any("Job completed successfully." in r.message for r in job_log_rows)
        assert not any(r.log_level == 'ERROR' for r in job_log_rows)

        # Assert job_error_log
        job_error_log_rows = fetch_rows(bigquery_client, "job_error_log")
        assert len(job_error_log_rows) == 0
    ```

#### 2. Test Case: Successful Execution with Default Stichtag

*   **Purpose**: Verify the wrapper correctly defaults `p_stichtag` to the current system date (DDMMYYYY) when not provided, and the job completes successfully. This covers **transformation correctness** (defaulting logic, date derivation).
*   **Setup**:
    *   Logging tables are cleared.
    *   The mock `sp_ausd_bp_ta_bcp_msisdn_kernel` is configured to succeed.
*   **Action**: Call `sp_r_ausd_bp_ta_bcp_msisdn` with `p_stichtag = NULL` and a valid `p_wiederanlaufWert` (e.g., 50).
*   **Pass/Fail Criterion**:
    *   The `CALL` to the main SP completes without raising an error.
    *   `job_control` table contains exactly one entry with:
        *   `status = 'SUCCESS'`
        *   `stichtag_param` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` (UTC).
        *   `wiederanlauf_wert_param = 50`.
    *   `job_log` table contains an INFO message: "p_stichtag not provided, defaulting to current system date: [current_date]".
    *   `job_error_log` table is empty.
*   **Runnable Test Code (Pytest)**:
    ```python
    # test_r_ausd_bp_ta_bcp_msisdn.py
    def test_successful_execution_default_stichtag(bigquery_client, setup_teardown_tables):
        wiederanlauf_wert = 50
        expected_stichtag = get_current_date_ddmmyyyy()

        success, error_msg = call_main_sp(bigquery_client, None, wiederanlauf_wert)
        assert success, f"SP call failed unexpectedly: {error_msg}"

        # Assert job_control
        job_control_rows = fetch_rows(bigquery_client, "job_control")
        assert len(job_control_rows) == 1
        jc = job_control_rows[0]
        assert jc.status == 'SUCCESS'
        assert jc.stichtag_param == expected_stichtag
        assert jc.wiederanlauf_wert_param == wiederanlauf_wert

        # Assert job_log
        job_log_rows = fetch_rows(bigquery_client, "job_log")
        assert any(f"p_stichtag not provided, defaulting to current system date: {expected_stichtag}" in r.message for r in job_log_rows)
        assert not any(r.log_level == 'ERROR' for r in job_log_rows)
    ```

#### 3. Test Case: Successful Execution with Default Wiederanlaufwert

*   **Purpose**: Verify the wrapper correctly defaults `p_wiederanlaufWert` to `0` when not provided, and the job completes successfully. This covers **transformation correctness** (defaulting logic).
*   **Setup**:
    *   Logging tables are cleared.
    *   The mock `sp_ausd_bp_ta_bcp_msisdn_kernel` is configured to succeed.
*   **Action**: Call `sp_r_ausd_bp_ta_bcp_msisdn` with a valid `p_stichtag` (e.g., '01012023') and `p_wiederanlaufWert = NULL`.
*   **Pass/Fail Criterion**:
    *   The `CALL` to the main SP completes without raising an error.
    *   `job_control` table contains exactly one entry with:
        *   `status = 'SUCCESS'`
        *   `stichtag_param = '01012023'`
        *   `wiederanlauf_wert_param = 0`.
    *   `job_log` table contains an INFO message: "p_wiederanlaufWert not provided, defaulting to 0.".
    *   `job_error_log` table is empty.
*   **Runnable Test Code (Pytest)**:
    ```python
    # test_r_ausd_bp_ta_bcp_msisdn.py
    def test_successful_execution_default_wiederanlaufwert(bigquery_client, setup_teardown_tables):
        stichtag = '01012023'
        expected_wiederanlauf_wert = 0

        success, error_msg = call_main_sp(bigquery_client, stichtag, None)
        assert success, f"SP call failed unexpectedly: {error_msg}"

        # Assert job_control
        job_control_rows = fetch_rows(bigquery_client, "job_control")
        assert len(job_control_rows) == 1
        jc = job_control_rows[0]
        assert jc.status == 'SUCCESS'
        assert jc.stichtag_param == stichtag
        assert jc.wiederanlauf_wert_param == expected_wiederanlauf_wert

        # Assert job_log
        job_log_rows = fetch_rows(bigquery_client, "job_log")
        assert any("p_wiederanlaufWert not provided, defaulting to 0." in r.message for r in job_log_rows)
        assert not any(r.log_level == 'ERROR' for r in job_log_rows)
    ```

#### 4. Test Case: Successful Execution with Both Parameters Defaulted

*   **Purpose**: Verify the wrapper correctly defaults both `p_stichtag` and `p_wiederanlaufWert` when neither is provided, and the job completes successfully. This covers **transformation correctness** (multiple defaulting logic).
*   **Setup**:
    *   Logging tables are cleared.
    *   The mock `sp_ausd_bp_ta_bcp_msisdn_kernel` is configured to succeed.
*   **Action**: Call `sp_r_ausd_bp_ta_bcp_msisdn` with `p_stichtag = NULL` and `p_wiederanlaufWert = NULL`.
*   **Pass/Fail Criterion**:
    *   The `CALL` to the main SP completes without raising an error.
    *   `job_control` table contains exactly one entry with:
        *   `status = 'SUCCESS'`
        *   `stichtag_param` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
        *   `wiederanlauf_wert_param = 0`.
    *   `job_log` table contains INFO messages for both "p_stichtag not provided..." and "p_wiederanlaufWert not provided...".
    *   `job_error_log` table is empty.
*   **Runnable Test Code (Pytest)**:
    ```python
    # test_r_ausd_bp_ta_bcp_msisdn.py
    def test_successful_execution_both_params_defaulted(bigquery_client, setup_teardown_tables):
        expected_stichtag = get_current_date_ddmmyyyy()
        expected_wiederanlauf_wert = 0

        success, error_msg = call_main_sp(bigquery_client, None, None)
        assert success, f"SP call failed unexpectedly: {error_msg}"

        # Assert job_control
        job_control_rows = fetch_rows(bigquery_client, "job_control")
        assert len(job_control_rows) == 1
        jc = job_control_rows[0]
        assert jc.status == 'SUCCESS'
        assert jc.stichtag_param == expected_stichtag
        assert jc.wiederanlauf_wert_param == expected_wiederanlauf_wert

        # Assert job_log
        job_log_rows = fetch_rows(bigquery_client, "job_log")
        assert any(f"p_stichtag not provided, defaulting to current system date: {expected_stichtag}" in r.message for r in job_log_rows)
        assert any("p_wiederanlaufWert not provided, defaulting to 0." in r.message for r in job_log_rows)
        assert not any(r.log_level == 'ERROR' for r in job_log_rows)
    ```

#### 5. Test Case: Parameter Validation - Invalid Stichtag Format

*   **Purpose**: Verify the wrapper correctly handles an invalid `p_stichtag` format (e.g., not DDMMYYYY) and fails gracefully, logging the error. This covers **transformation correctness** (parameter validation) and **external system replacement** (error logging).
*   **Setup**:
    *   Logging tables are cleared.
*   **Action**: Attempt to call `sp_r_ausd_bp_ta_bcp_msisdn` with `p_stichtag = '2023-01-01'` (invalid format) and `p_wiederanlaufWert = 10`. The call is expected to fail.
*   **Pass/Fail Criterion**:
    *   The `CALL` statement raises an error (e.g., `SIGNAL SQLSTATE '45000'`) with a message indicating invalid format.
    *   `job_control` table contains exactly one entry with:
        *   `status = 'FAILED'`
        *   `error_message` contains "Invalid p_stichtag format.".
        *   `end_time` is populated.
    *   `job_log` table contains an ERROR message: "Parameter Validation Error: Invalid p_stichtag format.".
    *   `job_error_log` table contains exactly one entry with:
        *   `error_type = 'PARAMETER_VALIDATION'`
        *   `error_message` matching the expected validation error.
*   **Runnable Test Code (Pytest)**:
    ```python
    # test_r_ausd_bp_ta_bcp_msisdn.py
    def test_invalid_stichtag_format(bigquery_client, setup_teardown_tables):
        stichtag = '2023-01-01' # Invalid format
        wiederanlauf_wert = 10

        success, error_msg = call_main_sp(bigquery_client, stichtag, wiederanlauf_wert)
        assert not success, "SP call was expected to fail but succeeded."
        assert "Invalid p_stichtag format" in error_msg

        # Assert job_control
        job_control_rows = fetch_rows(bigquery_client, "job_control")
        assert len(job_control_rows) == 1
        jc = job_control_rows[0]
        assert jc.status == 'FAILED'
        assert "Invalid p_stichtag format" in jc.error_message
        assert jc.end_time is not None

        # Assert job_log
        job_log_rows = fetch_rows(bigquery_client, "job_log")
        assert any("Parameter Validation Error: Invalid p_stichtag format." in r.message and r.log_level == 'ERROR' for r in job_log_rows)

        # Assert job_error_log
        job_error_log_rows = fetch_rows(bigquery_client, "job_error_log")
        assert len(job_error_log_rows) == 1
        jel = job_error_log_rows[0]
        assert jel.error_type == 'PARAMETER_VALIDATION'
        assert "Invalid p_stichtag format" in jel.error_message
    ```

#### 6. Test Case: Kernel Script Failure Handling

*   **Purpose**: Verify the wrapper correctly handles a failure originating from the invoked kernel stored procedure, logging the error and marking the job as failed. This covers **external system replacement** (error handling, logging).
*   **Setup**:
    *   Logging tables are cleared.
    *   The mock `sp_ausd_bp_ta_bcp_msisdn_kernel` is configured to *fail* (by passing `p_stichtag = 'KERNEL_FAIL'`).
*   **Action**: Call `sp_r_ausd_bp_ta_bcp_msisdn` with `p_stichtag = 'KERNEL_FAIL'` and `p_wiederanlaufWert = 10`. The call is expected to fail.
*   **Pass/Fail Criterion**:
    *   The `CALL` statement raises an error (e.g., `SIGNAL SQLSTATE '45000'`) with a message indicating the kernel failure.
    *   `job_control` table contains exactly one entry with:
        *   `status = 'FAILED'`
        *   `error_message` contains "Simulated kernel error for testing purposes.".
        *   `end_time` is populated.
    *   `job_log` table contains an ERROR message: "Job failed due to runtime error: Simulated kernel error...".
    *   `job_error_log` table contains exactly one entry with:
        *   `error_type = 'RUNTIME_ERROR'`
        *   `error_message` matching the kernel's simulated error.
*   **Runnable Test Code (Pytest)**:
    ```python
    # test_r_ausd_bp_ta_bcp_msisdn.py
    def test_kernel_script_failure(bigquery_client, setup_teardown_tables):
        stichtag = 'KERNEL_FAIL' # Special value to trigger mock kernel failure
        wiederanlauf_wert = 10

        success, error_msg = call_main_sp(bigquery_client, stichtag, wiederanlauf_wert)
        assert not success, "SP call was expected to fail due to kernel error but succeeded."
        assert "Simulated kernel error for testing purposes." in error_msg

        # Assert job_control
        job_control_rows = fetch_rows(bigquery_client, "job_control")
        assert len(job_control_rows) == 1
        jc = job_control_rows[0]
        assert jc.status == 'FAILED'
        assert "Simulated kernel error for testing purposes." in jc.error_message
        assert jc.end_time is not None

        # Assert job_log
        job_log_rows = fetch_rows(bigquery_client, "job_log")
        assert any("Job failed due to runtime error: Simulated kernel error for testing purposes." in r.message and r.log_level == 'ERROR' for r in job_log_rows)

        # Assert job_error_log
        job_error_log_rows = fetch_rows(bigquery_client, "job_error_log")
        assert len(job_error_log_rows) == 1
        jel = job_error_log_rows[0]
        assert jel.error_type == 'RUNTIME_ERROR'
        assert "Simulated kernel error for testing purposes." in jel.error_message
    ```

#### 7. Test Case: Logging Table Schema and Data Types

*   **Purpose**: Verify that the created logging tables (`job_control`, `job_log`, `job_error_log`) adhere to the specified schema and data types. This covers **data-quality / schema assertions**.
*   **Setup**: The DDL for the logging tables is executed as part of the `setup_teardown_tables` fixture.
*   **Action**: Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` for the details of each logging table.
*   **Pass/Fail Criterion**:
    *   The schema (column names, data types, nullability) of `job_control`, `job_log`, and `job_error_log` tables matches the DDL provided in the migration design.
*   **Runnable Test Code (Pytest)**:
    ```python
    # test_r_ausd_bp_ta_bcp_msisdn.py
    def test_logging_table_schema(bigquery_client):
        # Test job_control schema
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{bigquery_client.project}.{bigquery_client.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_control'
        ORDER BY ordinal_position;
        """
        schema_rows = list(bigquery_client.query(query).result())
        
        expected_schema_job_control = {
            "job_run_id": ("STRING", "NO"),
            "job_name": ("STRING", "NO"),
            "start_time": ("TIMESTAMP", "NO"),
            "end_time": ("TIMESTAMP", "YES"),
            "status": ("STRING", "NO"),
            "stichtag_param": ("STRING", "YES"),
            "wiederanlauf_wert_param": ("INT64", "YES"),
            "error_message": ("STRING", "YES"),
        }
        assert len(schema_rows) == len(expected_schema_job_control)
        for row in schema_rows:
            assert row.column_name in expected_schema_job_control
            assert expected_schema_job_control[row.column_name][0] == row.data_type.upper()
            assert expected_schema_job_control[row.column_name][1] == row.is_nullable.upper()

        # Test job_log schema
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{bigquery_client.project}.{bigquery_client.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_log'
        ORDER BY ordinal_position;
        """
        schema_rows = list(bigquery_client.query(query).result())
        expected_schema_job_log = {
            "log_id": ("STRING", "NO"),
            "job_run_id": ("STRING", "NO"),
            "log_time": ("TIMESTAMP", "NO"),
            "log_level": ("STRING", "NO"),
            "message": ("STRING", "NO"),
            "step": ("STRING", "YES"),
        }
        assert len(schema_rows) == len(expected_schema_job_log)
        for row in schema_rows:
            assert row.column_name in expected_schema_job_log
            assert expected_schema_job_log[row.column_name][0] == row.data_type.upper()
            assert expected_schema_job_log[row.column_name][1] == row.is_nullable.upper()

        # Test job_error_log schema
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{bigquery_client.project}.{bigquery_client.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_error_log'
        ORDER BY ordinal_position;
        """
        schema_rows = list(bigquery_client.query(query).result())
        expected_schema_job_error_log = {
            "error_id": ("STRING", "NO"),
            "job_run_id": ("STRING", "NO"),
            "error_time": ("TIMESTAMP", "NO"),
            "error_type": ("STRING", "NO"),
            "error_message": ("STRING", "NO"),
            "stack_trace": ("STRING", "YES"),
            "source_file": ("STRING", "YES"),
        }
        assert len(schema_rows) == len(expected_schema_job_error_log)
        for row in schema_rows:
            assert row.column_name in expected_schema_job_error_log
            assert expected_schema_job_error_log[row.column_name][0] == row.data_type.upper()
            assert expected_schema_job_error_log[row.column_name][1] == row.is_nullable.upper()
    ```

#### 8. Test Case: Empty Stichtag String Handling

*   **Purpose**: Verify that an empty string for `p_stichtag` is treated identically to `NULL` and correctly defaults to the current system date. This covers **transformation correctness** (NULL handling, edge cases).
*   **Setup**:
    *   Logging tables are cleared.
    *   The mock `sp_ausd_bp_ta_bcp_msisdn_kernel` is configured to succeed.
*   **Action**: Call `sp_r_ausd_bp_ta_bcp_msisdn` with `p_stichtag = ''` (empty string) and `p_wiederanlaufWert = 10`.
*   **Pass/Fail Criterion**:
    *   The `CALL` to the main SP completes without raising an error.
    *   `job_control` table contains exactly one entry with:
        *   `status = 'SUCCESS'`
        *   `stichtag_param` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
        *   `wiederanlauf_wert_param = 10`.
    *   `job_log` table contains an INFO message: "p_stichtag not provided, defaulting to current system date: [current_date]".
    *   `job_error_log` table is empty.
*   **Runnable Test Code (Pytest)**:
    ```python
    # test_r_ausd_bp_ta_bcp_msisdn.py
    def test_empty_stichtag_string_handling(bigquery_client, setup_teardown_tables):
        stichtag = '' # Empty string
        wiederanlauf_wert = 10
        expected_stichtag = get_current_date_ddmmyyyy()

        success, error_msg = call_main_sp(bigquery_client, stichtag, wiederanlauf_wert)
        assert success, f"SP call failed unexpectedly: {error_msg}"

        # Assert job_control
        job_control_rows = fetch_rows(bigquery_client, "job_control")
        assert len(job_control_rows) == 1
        jc = job_control_rows[0]
        assert jc.status == 'SUCCESS'
        assert jc.stichtag_param == expected_stichtag # Should default to current date
        assert jc.wiederanlauf_wert_param == wiederanlauf_wert

        # Assert job_log
        job_log_rows = fetch_rows(bigquery_client, "job_log")
        assert any(f"p_stichtag not provided, defaulting to current system date: {expected_stichtag}" in r.message for r in job_log_rows)
        assert not any(r.log_level == 'ERROR' for r in job_log_rows)
    ```