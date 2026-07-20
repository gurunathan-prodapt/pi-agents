# Migration Validation Test Suite: `d_call_sp_template.ksh` to `d_call_sp_template.py`

This document defines the migration-validation tests to prove that the migrated Python script `d_call_sp_template.py` is behaviorally equivalent to the legacy KornShell script `d_call_sp_template.ksh`. 

Since the legacy SQL script `d_call_sp_template.sql` (the Oracle Stored Procedure) was not provided, this test suite includes:
1. **Interface & Parameter Validation Tests**: Ensuring CLI arguments and environment variables are handled correctly.
2. **Mocked Behavioral Equivalence Tests**: Verifying that the Python script translates inputs into the correct BigQuery API calls.
3. **End-to-End Integration Tests**: Deploying a test harness in BigQuery to verify actual execution and side-effect logging.
4. **Error Handling & Resilience Tests**: Verifying correct exit code propagation on database failures.

---

## Test Case 1: CLI Parameter Parsing and Validation

### Purpose
Verify that the migrated Python script accepts exactly two positional arguments (`fachl_name1` and `fachl_name2`) and rejects incorrect invocations with standard exit codes, matching the parameter contract of the legacy shell script.

### Setup
* A Python environment with `pytest` installed.
* The target script `ksh_unsupported/d_call_sp_template.py` accessible in the path.

### Action
Run the script using `subprocess` with:
1. Zero arguments.
2. One argument.
3. Two arguments (valid).
4. Three arguments.

### Pass/Fail Criterion
* **Pass**: 
  * Invocations with 0, 1, or 3 arguments fail with exit code `2` (standard `argparse` error code) and print usage instructions to `stderr`.
  * Invocation with 2 arguments proceeds past argument parsing (fails only on missing environment variables, exit code `1`, not `2`).
* **Fail**: Any deviation from standard `argparse` exit codes or failure to parse exactly two positional parameters.

### Test Code (`test_cli_parsing.py`)
```python
import subprocess
import sys
import os

def test_cli_no_arguments():
    result = subprocess.run(
        [sys.executable, "ksh_unsupported/d_call_sp_template.py"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 2
    assert "error: the following arguments are required" in result.stderr

def test_cli_one_argument():
    result = subprocess.run(
        [sys.executable, "ksh_unsupported/d_call_sp_template.py", "param1"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 2
    assert "error: the following arguments are required" in result.stderr

def test_cli_three_arguments():
    result = subprocess.run(
        [sys.executable, "ksh_unsupported/d_call_sp_template.py", "param1", "param2", "param3"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 2
    assert "unrecognized arguments" in result.stderr
```

---

## Test Case 2: Environment Variable Validation

### Purpose
Verify that the script strictly validates the presence of the required global environment variables (`GCP_PROJECT` and `BQ_DATASET`) and terminates gracefully with exit code `1` if they are missing.

### Setup
* Clear `GCP_PROJECT` and `BQ_DATASET` from the execution environment.

### Action
Execute the Python script with two valid positional arguments under different environment configurations:
1. Both `GCP_PROJECT` and `BQ_DATASET` missing.
2. Only `GCP_PROJECT` missing.
3. Only `BQ_DATASET` missing.

### Pass/Fail Criterion
* **Pass**: The script exits with code `1` in all three scenarios and prints a descriptive error message to `stderr` indicating that required environment variables are missing.
* **Fail**: The script attempts to initialize the BigQuery client with missing variables, raises an unhandled exception, or exits with code `0`.

### Test Code (`test_env_validation.py`)
```python
import subprocess
import sys
import os
import pytest

@pytest.fixture
def clean_env():
    env = os.environ.copy()
    env.pop("GCP_PROJECT", None)
    env.pop("BQ_DATASET", None)
    return env

def test_missing_all_env_vars(clean_env):
    result = subprocess.run(
        [sys.executable, "ksh_unsupported/d_call_sp_template.py", "val1", "val2"],
        env=clean_env,
        capture_output=True,
        text=True
    )
    assert result.returncode == 1
    assert "Error: Required environment variables GCP_PROJECT or BQ_DATASET are missing." in result.stderr

def test_missing_bq_dataset(clean_env):
    clean_env["GCP_PROJECT"] = "test-project"
    result = subprocess.run(
        [sys.executable, "ksh_unsupported/d_call_sp_template.py", "val1", "val2"],
        env=clean_env,
        capture_output=True,
        text=True
    )
    assert result.returncode == 1
    assert "Error: Required environment variables GCP_PROJECT or BQ_DATASET are missing." in result.stderr
```

---

## Test Case 3: Mocked Behavioral Equivalence & Parameter Mapping

### Purpose
Verify that the Python script correctly maps the positional arguments to BigQuery query parameters and constructs the correct SQL statement to call the stored procedure. This ensures SQL injection prevention and correct parameter binding.

### Setup
* Mock the `google.cloud.bigquery.Client` and its `query` method using `unittest.mock`.
* Set dummy environment variables: `GCP_PROJECT=dummy-project`, `BQ_DATASET=dummy_dataset`, `BQ_LOCATION=EU`.

### Action
Call the `call_bigquery_stored_procedure` function with test inputs `"arg_value_1"` and `"arg_value_2"`.

### Pass/Fail Criterion
* **Pass**: 
  * `client.query` is called with the exact SQL string: `CALL \`dummy-project.dummy_dataset.d_call_sp_template\`(@param1, @param2);`.
  * `job_config` contains two scalar query parameters: `param1` with value `"arg_value_1"` and `param2` with value `"arg_value_2"`.
* **Fail**: The SQL query is malformed, parameters are missing, or parameters are not mapped as safe `ScalarQueryParameter` objects.

### Test Code (`test_behavioral_equivalence.py`)
```python
import os
import pytest
from unittest.mock import MagicMock, patch
from google.cloud import bigquery

# Import the function under test
from ksh_unsupported.d_call_sp_template import call_bigquery_stored_procedure

@patch("google.cloud.bigquery.Client")
def test_stored_procedure_call_mapping(mock_bq_client_class):
    # Setup environment
    os.environ["GCP_PROJECT"] = "dummy-project"
    os.environ["BQ_DATASET"] = "dummy_dataset"
    os.environ["BQ_LOCATION"] = "US"

    # Mock client instance and query job
    mock_client_instance = MagicMock()
    mock_bq_client_class.return_value = mock_client_instance
    mock_query_job = MagicMock()
    mock_client_instance.query.return_value = mock_query_job

    # Execute
    call_bigquery_stored_procedure("val_foo", "val_bar")

    # Assert Client Initialization
    mock_bq_client_class.assert_called_once_with(project="dummy-project", location="US")

    # Assert Query Call
    mock_client_instance.query.assert_called_once()
    called_args, called_kwargs = mock_client_instance.query.call_args
    
    # Verify SQL Statement
    expected_sql = "CALL `dummy-project.dummy_dataset.d_call_sp_template`(@param1, @param2);"
    assert called_args[0] == expected_sql

    # Verify Parameters
    job_config = called_kwargs.get("job_config")
    assert job_config is not None
    assert len(job_config.query_parameters) == 2
    
    param1 = job_config.query_parameters[0]
    assert param1.name == "param1"
    assert param1.type_ == "STRING"
    assert param1.value == "val_foo"

    param2 = job_config.query_parameters[1]
    assert param2.name == "param2"
    assert param2.type_ == "STRING"
    assert param2.value == "val_bar"
```

---

## Test Case 4: End-to-End Integration & Side-Effect Validation

### Purpose
To mitigate the risk of the missing `d_call_sp_template.sql` source code, this test deploys a test-double Stored Procedure in BigQuery, executes the migrated Python script against a live BigQuery sandbox, and asserts that the side-effects (data written to a log table) are correct.

### Setup
1. Create a test log table in BigQuery: `t_call_sp_log`.
2. Deploy a test-double version of the `d_call_sp_template` stored procedure that writes its inputs to `t_call_sp_log`.
3. Set active GCP credentials in the test environment.

### Action
1. Run the BigQuery DDL setup script.
2. Execute `d_call_sp_template.py "E2E_TEST_1" "E2E_TEST_2"`.
3. Query the log table `t_call_sp_log` and assert the row exists.
4. Run the BigQuery DDL teardown script.

### Pass/Fail Criterion
* **Pass**: The script completes with exit code `0`, and a row is successfully inserted into `t_call_sp_log` containing `param1 = 'E2E_TEST_1'` and `param2 = 'E2E_TEST_2'`.
* **Fail**: The script fails to execute, or the log table does not contain the expected values.

### Test Code (`test_e2e_integration.py` & SQL)

#### 1. BigQuery Setup DDL (Run before test)
```sql
-- Create Log Table
CREATE OR REPLACE TABLE `${GCP_PROJECT}.${BQ_DATASET}.t_call_sp_log` (
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    param1 STRING,
    param2 STRING
);

-- Create Test-Double Stored Procedure
CREATE OR REPLACE PROCEDURE `${GCP_PROJECT}.${BQ_DATASET}.d_call_sp_template`(param1 STRING, param2 STRING)
BEGIN
    INSERT INTO `${GCP_PROJECT}.${BQ_DATASET}.t_call_sp_log` (param1, param2)
    VALUES (param1, param2);
END;
```

#### 2. Python Integration Test
```python
import os
import subprocess
import sys
from google.cloud import bigquery

def test_e2e_stored_procedure_execution():
    # Ensure environment is configured for a real GCP Sandbox
    project = os.environ.get("GCP_PROJECT")
    dataset = os.environ.get("BQ_DATASET")
    assert project is not None, "GCP_PROJECT must be set for E2E tests"
    assert dataset is not None, "BQ_DATASET must be set for E2E tests"

    client = bigquery.Client(project=project)

    # Clear previous logs
    client.query(f"TRUNCATE TABLE `{project}.{dataset}.t_call_sp_log`").result()

    # Execute the migrated script
    result = subprocess.run(
        [sys.executable, "ksh_unsupported/d_call_sp_template.py", "INTEG_VAL_1", "INTEG_VAL_2"],
        capture_output=True,
        text=True
    )

    # Assert script success
    assert result.returncode == 0, f"Script failed: {result.stderr}"

    # Verify side-effects in BigQuery
    query = f"SELECT param1, param2 FROM `{project}.{dataset}.t_call_sp_log` LIMIT 1"
    query_job = client.query(query)
    rows = list(query_job.result())

    assert len(rows) == 1, "No log row was written by the stored procedure."
    assert rows[0]["param1"] == "INTEG_VAL_1"
    assert rows[0]["param2"] == "INTEG_VAL_2"
```

---

## Test Case 5: Error Handling and Exception Propagation

### Purpose
Verify that if the BigQuery Stored Procedure execution fails (e.g., due to database-level constraints, syntax errors, or permission issues), the Python script catches the `GoogleCloudError`, logs the error message to `stderr`, and exits with a non-zero exit code (`1`).

### Setup
* Mock `google.cloud.bigquery.Client` to raise a `google.cloud.exceptions.GoogleCloudError` when `query` is called.

### Action
Execute the Python script with valid arguments.

### Pass/Fail Criterion
* **Pass**: The script catches the exception, outputs the error message to `stderr`, and exits with code `1`.
* **Fail**: The script exits with code `0` (silent failure), or raises an unhandled traceback to the console without clean termination.

### Test Code (`test_error_handling.py`)
```python
import os
import pytest
from unittest.mock import MagicMock, patch
from google.cloud.exceptions import GoogleCloudError

# Import the function under test
from ksh_unsupported.d_call_sp_template import call_bigquery_stored_procedure

@patch("google.cloud.bigquery.Client")
def test_bigquery_error_propagation(mock_bq_client_class):
    # Setup environment
    os.environ["GCP_PROJECT"] = "dummy-project"
    os.environ["BQ_DATASET"] = "dummy_dataset"

    # Mock client to raise GoogleCloudError
    mock_client_instance = MagicMock()
    mock_bq_client_class.return_value = mock_client_instance
    
    # Simulate a BigQuery execution failure
    mock_client_instance.query.side_effect = GoogleCloudError("Access Denied or Procedure Not Found")

    # Assert that the script exits with code 1 when a GoogleCloudError occurs
    with pytest.raises(SystemExit) as exc_info:
        call_bigquery_stored_procedure("val1", "val2")
    
    assert exc_info.value.code == 1
```