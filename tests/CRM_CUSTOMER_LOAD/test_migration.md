# Migration Validation Test Suite: CRM_CUSTOMER_LOAD

This document defines the migration-validation test suite for the `CRM_CUSTOMER_LOAD` job, verifying that the migrated Python orchestration (`process_customer_data.py` and `retry_handler.py`) is behaviorally equivalent to the legacy KornShell implementation (`process_customer_data.ksh` and `retry_handler.ksh`).

---

## Test Case 1: Upstream Event Synchronization & Timeout Behavior

### Purpose
Verify that the orchestration correctly synchronizes with upstream pipelines by polling for event markers, and that it handles success, failure, and timeout conditions in strict behavioral parity with the legacy shell script.

### Setup
1. **Environment Variables**:
   * `GCP_PROJECT="test-gcp-project"`
   * `BQ_DATASET="test_crm_dataset"`
   * `ETL_EVENTS_DIR="/tmp/etl/events"`
2. **State Preparation**:
   * Clear the `/tmp/etl/events` directory.
   * Ensure `uc4api` is mocked or unavailable in the test environment path to isolate file-marker polling.

### Action
Execute the test suite using `pytest` to run the following test cases:
1. **Case A (Critical Failure)**: `FINANCE_GL_CLOSE_COMPLETE` event is missing (no marker file).
2. **Case B (Non-Fatal Timeout)**: `FINANCE_GL_CLOSE_COMPLETE` is present, but `RETAIL_DAILY_COMPLETE` is missing.

```python
import os
import shutil
import pytest
import subprocess
from unittest.mock import patch, MagicMock

@pytest.fixture(autouse=True)
def setup_env(tmp_path):
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    
    os.environ["GCP_PROJECT"] = "test-gcp-project"
    os.environ["BQ_DATASET"] = "test_crm_dataset"
    os.environ["ETL_EVENTS_DIR"] = str(events_dir)
    os.environ["LOG_DIR"] = str(tmp_path / "logs")
    os.environ["SQLPLUS_DIR"] = str(tmp_path / "sql")
    os.environ["PYTHON_DIR"] = str(tmp_path / "bin")
    
    # Create dummy SQL and Python scripts to prevent file-not-found errors
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "customer_segment_extract.sql").write_text("SELECT 1;")
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "customer_scoring.py").write_text("print('Scoring complete')")
    
    yield tmp_path
    shutil.rmtree(tmp_path)

@patch("google.cloud.bigquery.Client")
def test_finance_gl_close_missing_aborts_job(mock_bq_client, setup_env):
    """Case A: If FINANCE_GL_CLOSE_COMPLETE fails/times out, the job must abort immediately with exit code 1."""
    from customer.process_customer_data import main
    
    # We pass short poll parameters to avoid long test execution times
    with patch("etl_lib.retry_handler.wait_for_event", return_value=1) as mock_wait:
        with patch("sys.argv", ["process_customer_data.py", "2024-01-15", "ALL", "N"]):
            exit_code = main()
            
            assert exit_code == 1
            mock_wait.assert_any_call("FINANCE_GL_CLOSE_COMPLETE", "2024-01-15")

@patch("google.cloud.bigquery.Client")
def test_retail_daily_missing_continues_job(mock_bq_client, setup_env):
    """Case B: If RETAIL_DAILY_COMPLETE times out, the job must log a warning and continue (exit code 0)."""
    from customer.process_customer_data import main
    
    # Mock BigQuery calls to avoid hitting actual GCP
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance
    
    # Mock staging count query to return 100 rows so it doesn't exit early
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [MagicMock(cnt=100)]
    mock_client_instance.query.return_value = mock_query_job

    def side_effect_wait(event_name, run_date, *args, **kwargs):
        if event_name == "FINANCE_GL_CLOSE_COMPLETE":
            return 0  # Success
        if event_name == "RETAIL_DAILY_COMPLETE":
            return 1  # Timeout/Failure
        return 0

    with patch("etl_lib.retry_handler.wait_for_event", side_effect=side_effect_wait):
        with patch("sys.argv", ["process_customer_data.py", "2024-01-15", "ALL", "N"]):
            exit_code = main()
            
            # Must complete successfully despite the retail timeout
            assert exit_code == 0
```

### Pass/Fail Criterion
* **Pass**: 
  * Case A returns exit code `1` and writes `ERROR: FINANCE_GL_CLOSE_COMPLETE did not complete. Aborting.` to the log file.
  * Case B returns exit code `0` and writes `WARN: RETAIL_DAILY_COMPLETE timed out. Proceeding with available data.` to the log file.
* **Fail**: Any non-zero exit code on Case B, or a zero exit code on Case A.

---

## Test Case 2: Customer Segment Extract Parameter & Query Parity

### Purpose
Verify that the transpiled BigQuery SQL query is executed with the exact parameter mappings, types, and values that were historically passed via SQL\*Plus `DEFINE` variables.

### Setup
1. **BigQuery Environment**:
   * Create a mock SQL file at `/tmp/etl/sql/customer_segment_extract.sql` containing a parameterized query.
2. **Mocking**:
   * Mock `google.cloud.bigquery.Client.query` to capture and inspect the `QueryJobConfig` parameters.

### Action
Run the Python script and assert that the parameters passed to the BigQuery API match the expected types and values.

```python
import os
import pytest
from unittest.mock import patch, MagicMock
from google.cloud import bigquery

@patch("google.cloud.bigquery.Client")
@patch("etl_lib.retry_handler.wait_for_event", return_value=0)
def test_query_parameter_mapping(mock_wait, mock_bq_client, setup_env):
    from customer.process_customer_data import main
    
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance
    
    # Mock query results for both the extract and the staging count
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [MagicMock(cnt=500)]
    mock_client_instance.query.return_value = mock_query_job
    
    # Set custom environment variables to test default overrides
    os.environ["BATCH_SIZE"] = "10000"
    os.environ["REGION_CODE"] = "US"
    
    with patch("sys.argv", ["process_customer_data.py", "2024-01-15", "VIP", "N"]):
        main()
        
        # Capture the first query call (which is the segment extract)
        first_call_args = mock_client_instance.query.call_args_list[0]
        _, kwargs = first_call_args
        job_config = kwargs.get("job_config")
        
        assert job_config is not None
        params = {p.name: (p.type_, p.value) for p in job_config.query_parameters}
        
        # Assert parameter types and values match legacy mappings
        assert params["run_date"] == ("STRING", "2024-01-15")
        assert params["customer_segment"] == ("STRING", "VIP")
        assert params["batch_size"] == ("INT64", 10000)
        assert params["region_code"] == ("STRING", "US")
        assert params["run_date_fmt"] == ("STRING", "20240115")
```

### Pass/Fail Criterion
* **Pass**: The BigQuery client is called with a `QueryJobConfig` containing all 5 parameters mapped to their correct BigQuery types (`STRING` for dates/segments/regions, `INT64` for batch size).
* **Fail**: Missing parameters, incorrect type casting (e.g., passing `batch_size` as a string), or incorrect string date formatting (e.g., not stripping hyphens for `run_date_fmt`).

---

## Test Case 3: Staging Count Validation & Force Reload Logic

### Purpose
Verify that the job conditionally exits or proceeds based on the row count of `STG_CUSTOMER_PROFILE` and the `FORCE_RELOAD` parameter, preserving the exact conditional branching of the legacy shell script.

### Setup
1. **BigQuery Mocking**:
   * Configure the BigQuery mock client to return `0` rows for the staging count query.
2. **Test Matrix**:
   * Run 1: `FORCE_RELOAD = "N"` (Expected: Exit 0, early termination).
   * Run 2: `FORCE_RELOAD = "Y"` (Expected: Continue execution to Step 2).

### Action
Execute the orchestration script under both parameter configurations.

```python
import os
import pytest
from unittest.mock import patch, MagicMock

@patch("google.cloud.bigquery.Client")
@patch("etl_lib.retry_handler.wait_for_event", return_value=0)
def test_staging_count_zero_no_force_exits_zero(mock_wait, mock_bq_client, setup_env):
    """If count is 0 and FORCE_RELOAD is N, exit 0 immediately."""
    from customer.process_customer_data import main
    
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance
    
    # Mock staging count query to return 0 rows
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [MagicMock(cnt=0)]
    mock_client_instance.query.return_value = mock_query_job
    
    with patch("sys.argv", ["process_customer_data.py", "2024-01-15", "ALL", "N"]):
        exit_code = main()
        assert exit_code == 0
        
        # Verify that MASTER_CRM_LOAD was NEVER called
        for call in mock_client_instance.query.call_args_list:
            query_str = call[0][0]
            assert "MASTER_CRM_LOAD" not in query_str

@patch("google.cloud.bigquery.Client")
@patch("etl_lib.retry_handler.wait_for_event", return_value=0)
def test_staging_count_zero_with_force_continues(mock_wait, mock_bq_client, setup_env):
    """If count is 0 and FORCE_RELOAD is Y, proceed to execute MASTER_CRM_LOAD."""
    from customer.process_customer_data import main
    
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance
    
    # Mock staging count query to return 0 rows
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [MagicMock(cnt=0)]
    mock_client_instance.query.return_value = mock_query_job
    
    with patch("sys.argv", ["process_customer_data.py", "2024-01-15", "ALL", "Y"]):
        exit_code = main()
        assert exit_code == 0  # Should complete successfully
        
        # Verify that MASTER_CRM_LOAD WAS called
        called_queries = [call[0][0] for call in mock_client_instance.query.call_args_list]
        assert any("MASTER_CRM_LOAD" in q for q in called_queries)
```

### Pass/Fail Criterion
* **Pass**: 
  * When `FORCE_RELOAD = N` and count is `0`, the script exits with code `0` and does not call the stored procedure.
  * When `FORCE_RELOAD = Y` and count is `0`, the script bypasses early termination and calls `MASTER_CRM_LOAD`.
* **Fail**: The script continues when it should exit, or exits when `FORCE_RELOAD = Y` is supplied.

---

## Test Case 4: Stored Procedure Execution Parity (`MASTER_CRM_LOAD`)

### Purpose
Verify that the BigQuery stored procedure `MASTER_CRM_LOAD` is called with the correct signature, date parsing, and dataset qualification, and that any database-level exceptions are propagated as exit code `3`.

### Setup
1. **BigQuery Environment**:
   * Deploy a mock stored procedure in the test BigQuery dataset to simulate success and failure states.
2. **Database State**:
   * Seed `STG_CUSTOMER_PROFILE` with test records.

### Action
Execute the stored procedure call via the Python script and verify the database state using SQL assertions.

```sql
-- SQL Assertion: Verify Staging Status Transition after MASTER_CRM_LOAD execution
-- This query must return 0 rows if the stored procedure executed successfully and processed all pending rows.

SELECT CUSTOMER_ID, LOAD_DATE, ETL_STATUS
FROM `test-gcp-project.test_crm_dataset.STG_CUSTOMER_PROFILE`
WHERE LOAD_DATE = DATE('2024-01-15')
  AND ETL_STATUS = 'PENDING';
```

```python
import pytest
from unittest.mock import patch, MagicMock
from google.cloud import bigquery

@patch("google.cloud.bigquery.Client")
@patch("etl_lib.retry_handler.wait_for_event", return_value=0)
def test_stored_procedure_failure_returns_exit_3(mock_wait, mock_bq_client, setup_env):
    """If the BigQuery stored procedure execution fails, the script must exit with code 3."""
    from customer.process_customer_data import main
    
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance
    
    # Mock staging count query to return 100 rows
    mock_query_job_success = MagicMock()
    mock_query_job_success.result.return_value = [MagicMock(cnt=100)]
    
    # Mock stored procedure query to raise an exception
    mock_query_job_fail = MagicMock()
    mock_query_job_fail.result.side_effect = Exception("BigQuery Access Denied or Syntax Error")
    
    def side_effect_query(query, *args, **kwargs):
        if "STG_CUSTOMER_PROFILE" in query:
            return mock_query_job_success
        if "MASTER_CRM_LOAD" in query:
            raise Exception("Stored Procedure Execution Failed")
        return mock_query_job_success

    mock_client_instance.query.side_effect = side_effect_query
    
    with patch("sys.argv", ["process_customer_data.py", "2024-01-15", "ALL", "N"]):
        exit_code = main()
        assert exit_code == 3
```

### Pass/Fail Criterion
* **Pass**: 
  * The stored procedure is invoked as `CALL \`test-gcp-project.test_crm_dataset.MASTER_CRM_LOAD\`(PARSE_DATE('%Y-%m-%d', @run_date), @segment)`.
  * A database exception during the stored procedure call results in an immediate exit with code `3`.
* **Fail**: The script returns a non-3 exit code on procedure failure, or fails to qualify the stored procedure with the correct project and dataset.

---

## Test Case 5: Downstream Python Scoring Invocation & Error Tolerance

### Purpose
Verify that the downstream `customer_scoring.py` script is executed with the correct command-line arguments, and that a non-zero exit code from the scoring script is treated as a non-fatal warning (allowing the main job to complete with exit code `0`).

### Setup
1. **Mocking**:
   * Mock `subprocess.run` to simulate a failure (exit code `42`) from the downstream Python script.

### Action
Run the orchestration script and verify that it logs the warning but does not abort.

```python
import pytest
import subprocess
from unittest.mock import patch, MagicMock

@patch("google.cloud.bigquery.Client")
@patch("etl_lib.retry_handler.wait_for_event", return_value=0)
@patch("subprocess.run")
def test_scoring_failure_is_non_fatal(mock_sub_run, mock_wait, mock_bq_client, setup_env):
    from customer.process_customer_data import main
    
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance
    
    # Mock staging count query to return 100 rows
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [MagicMock(cnt=100)]
    mock_client_instance.query.return_value = mock_query_job
    
    # Simulate customer_scoring.py returning exit code 42
    mock_sub_run.side_effect = subprocess.CalledProcessError(returncode=42, cmd="customer_scoring.py")
    
    with patch("sys.argv", ["process_customer_data.py", "2024-01-15", "RETAIL", "N"]):
        exit_code = main()
        
        # Job must succeed (exit code 0) despite scoring failure
        assert exit_code == 0
        
        # Verify subprocess was called with correct arguments
        mock_sub_run.assert_called_once()
        called_args = mock_sub_run.call_args[0][0]
        assert "customer_scoring.py" in called_args[1]
        assert called_args[2] == "--run-date"
        assert called_args[3] == "2024-01-15"
        assert called_args[4] == "--segment"
        assert called_args[5] == "RETAIL"
        assert called_args[6] == "--env"
        assert called_args[7] == "PROD"
```

### Pass/Fail Criterion
* **Pass**: The orchestration script executes `customer_scoring.py` with the correct arguments, captures the non-zero exit code, logs `WARN: Python scoring returned non-zero (rc=42) - non-fatal`, and exits with code `0`.
* **Fail**: The orchestration script aborts with a non-zero exit code when the scoring script fails, or fails to pass the correct arguments.

---

## Test Case 6: Audit Logging Parity (`log_job_audit`)

### Purpose
Verify that the migrated `log_job_audit` function correctly performs an upsert (MERGE) operation on the BigQuery `etl_job_audit` table, generating a unique UUID for new records and updating existing records.

### Setup
1. **BigQuery Environment**:
   * Ensure the `etl_job_audit` table is created in the test dataset.
2. **State Preparation**:
   * Delete any existing audit records for `JOB_NAME = 'CRM_CUSTOMER_LOAD'` and `RUN_DATE = '2024-01-15'`.

### Action
1. Call `log_job_audit` to insert a new record.
2. Assert the record is created with a valid UUID.
3. Call `log_job_audit` again with updated status and row counts.
4. Assert the record is updated in-place without creating duplicates.

```sql
-- SQL Assertion 1: Verify Initial Insert
SELECT COUNT(1) as row_count, MAX(JOB_STATUS) as status, MAX(ROWS_PROCESSED) as processed
FROM `test-gcp-project.test_crm_dataset.etl_job_audit`
WHERE JOB_NAME = 'CRM_CUSTOMER_LOAD'
  AND RUN_DATE = DATE('2024-01-15');

-- Expected Output:
-- row_count = 1, status = 'SUCCESS', processed = 5000
```

```python
import os
import pytest
import uuid
from unittest.mock import patch
from google.cloud import bigquery
from lib.retry_handler import log_job_audit

@pytest.mark.integration
def test_audit_logging_merge_logic():
    """Integration test against actual/emulator BigQuery to verify MERGE logic."""
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")
    
    if not gcp_project or not bq_dataset:
        pytest.skip("GCP_PROJECT or BQ_DATASET not set. Skipping integration test.")
        
    client = bigquery.Client(project=gcp_project)
    table_ref = f"{gcp_project}.{bq_dataset}.etl_job_audit"
    
    # Clean up prior runs
    client.query(f"DELETE FROM `{table_ref}` WHERE JOB_NAME = 'CRM_CUSTOMER_LOAD_TEST'").result()
    
    # 1. First Call: Insert
    rc1 = log_job_audit("CRM_CUSTOMER_LOAD_TEST", "2024-01-15", "RUNNING", 0)
    assert rc1 == 0
    
    # Verify Insert
    results = list(client.query(f"SELECT AUDIT_ID, JOB_STATUS, ROWS_PROCESSED FROM `{table_ref}` WHERE JOB_NAME = 'CRM_CUSTOMER_LOAD_TEST'").result())
    assert len(results) == 1
    first_audit_id = results[0].AUDIT_ID
    assert results[0].JOB_STATUS == "RUNNING"
    assert results[0].ROWS_PROCESSED == 0
    # Verify UUID format
    assert uuid.UUID(first_audit_id)
    
    # 2. Second Call: Update (MERGE)
    rc2 = log_job_audit("CRM_CUSTOMER_LOAD_TEST", "2024-01-15", "SUCCESS", 15500)
    assert rc2 == 0
    
    # Verify Update
    results_updated = list(client.query(f"SELECT AUDIT_ID, JOB_STATUS, ROWS_PROCESSED FROM `{table_ref}` WHERE JOB_NAME = 'CRM_CUSTOMER_LOAD_TEST'").result())
    assert len(results_updated) == 1
    assert results_updated[0].AUDIT_ID == first_audit_id  # Must preserve the original AUDIT_ID
    assert results_updated[0].JOB_STATUS == "SUCCESS"
    assert results_updated[0].ROWS_PROCESSED == 15500
```

### Pass/Fail Criterion
* **Pass**: 
  * The first call inserts a new row with a generated UUID.
  * The second call updates the existing row in-place (matching on `JOB_NAME` and `RUN_DATE`) without generating a new row or changing the original `AUDIT_ID`.
* **Fail**: Duplicate rows are created for the same job and run date, or the `AUDIT_ID` changes on update.