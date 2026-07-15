Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Python-based environment configuration engine (`gcp_environment_loader.py`) is behaviorally equivalent to the legacy Ab Initio and Oracle DWH environment scripts (`.dw_global`, `.dw_ai`, `.dw_db`, and `.dw_init`).

---

# Test Suite: Environment Configuration Migration Validation

## Section 1: Output Parity & Path Resolution Tests

### Test Case 1.1: Dynamic Path Resolution Parity (`.dw_global` Emulation)
#### Purpose
Verify that the migrated Python utility resolves legacy environment variables (`AI_SERIAL`, `AI_TEMP`, `AI_MFS`) to the correct, environment-scoped Google Cloud Storage (GCS) URIs, matching the behavior of the legacy `.dw_global` script.

#### Setup
* Install `pytest` and `mock` in the test environment.
* Mock the Airflow `Variable.get` method to prevent external database calls during unit testing.

#### Action
Execute a parameterized test passing legacy variable names across different environment phases (`dev`, `qa`, `prod`) and assert that the returned GCS paths match the expected cloud-native patterns.

```python
import pytest
from unittest.mock import patch
from gcp_environment_loader import GCPEnvironmentLoader

@pytest.mark.parametrize("env_phase,legacy_var,expected_uri", [
    ("dev", "AI_SERIAL", "gs://dwh-landing-zone-dev/serial_staging/"),
    ("qa", "AI_TEMP", "gs://dwh-transient-zone-qa/temp_processing/"),
    ("prod", "AI_MFS", "gs://dwh-mfs-partitioned-prod/mfs_data/"),
])
def test_resolve_global_paths_hardcoded(env_phase, legacy_var, expected_uri):
    loader = GCPEnvironmentLoader(project_id="gcp-test-dwh", environment_phase=env_phase)
    assert loader.resolve_global_paths(legacy_var) == expected_uri
```

#### Pass/Fail Criterion
* **Pass**: The resolved GCS URI matches the expected string exactly for all environment phases.
* **Fail**: The resolved path is incorrect, or a `KeyError` is raised for standard hardcoded variables.


### Test Case 1.2: Airflow Variable Fallback Resolution
#### Purpose
Verify that variables not hardcoded in the mapping dictionary fall back to Airflow's metadata database using the correct environment-suffixed key structure (e.g., `legacy_var_name_env`).

#### Setup
* Mock `airflow.models.Variable.get` to return a mock path when queried with a specific key.

#### Action
Query a non-hardcoded variable (e.g., `AI_SERIAL_ARCHIVE`) and verify that the loader requests the correct key from Airflow.

```python
@patch("gcp_environment_loader.Variable.get")
def test_resolve_global_paths_fallback(mock_variable_get):
    mock_variable_get.return_value = "gs://custom-archive-bucket-prod/archive/"
    
    loader = GCPEnvironmentLoader(project_id="gcp-test-dwh", environment_phase="prod")
    result = loader.resolve_global_paths("AI_SERIAL_ARCHIVE")
    
    # Assert fallback key format: lowercased_variable_name_env
    mock_variable_get.assert_called_once_with("ai_serial_archive_prod")
    assert result == "gs://custom-archive-bucket-prod/archive/"
```

#### Pass/Fail Criterion
* **Pass**: The loader queries Airflow with the lowercase, environment-suffixed key and returns the correct value.
* **Fail**: The loader queries an incorrect key format or fails to fall back.

---

## Section 2: Transformation & Logic Correctness Tests

### Test Case 2.1: Parallelism Profile Sizing (`.dw_ai` Emulation)
#### Purpose
Verify that the loader correctly maps legacy parallel execution layouts (`MFS_DEPTH`) and environment phases to the correct PySpark scaling profiles and worker limits.

#### Setup
* Mock `airflow.models.Variable.get` to return a custom parallel depth.

#### Action
Initialize the loader for `dev`, `qa`, and `prod` environments and verify that the returned dictionary contains the correct degree of parallelism (`do_p`) and scaling configurations.

```python
@pytest.mark.parametrize("env_phase,mock_depth,expected_min,expected_max,expected_machine", [
    ("dev", "4", 2, 5, "n1-standard-4"),
    ("qa", "16", 2, 10, "n1-standard-8"),
    ("prod", "32", 4, 30, "n1-standard-16"),
])
def test_fetch_execution_parallelism(env_phase, mock_depth, expected_min, expected_max, expected_machine):
    with patch("gcp_environment_loader.Variable.get") as mock_get:
        mock_get.return_value = mock_depth
        
        loader = GCPEnvironmentLoader(project_id="gcp-test-dwh", environment_phase=env_phase)
        config = loader.fetch_execution_parallelism()
        
        mock_get.assert_called_once_with(f"mfs_depth_{env_phase}", default_var="8")
        assert config["do_p"] == int(mock_depth)
        assert config["scaling"]["min_workers"] == expected_min
        assert config["scaling"]["max_workers"] == expected_max
        assert config["scaling"]["machine_type"] == expected_machine
```

#### Pass/Fail Criterion
* **Pass**: The returned dictionary matches the exact scaling profile and degree of parallelism for the specified environment.
* **Fail**: The scaling profile values or machine types do not match the environment tier.

---

## Section 3: External System Replacements & Security Tests

### Test Case 3.1: Secure Database Profile Retrieval (`.dw_db` Emulation)
#### Purpose
Verify that Oracle database connection profiles are securely retrieved from Google Cloud Secret Manager using the correct secret path naming convention, and that JSON payloads are parsed correctly.

#### Setup
* Mock the `google.cloud.secretmanager.SecretManagerServiceClient` to prevent actual API calls.
* Prepare a valid mock JSON payload representing database credentials.

#### Action
Call `fetch_database_profile` and assert that the correct secret path is requested and that the returned dictionary matches the decrypted payload.

```python
from unittest.mock import MagicMock

@patch("gcp_environment_loader.secretmanager.SecretManagerServiceClient")
def test_fetch_database_profile_success(mock_secret_client_class):
    # Setup mock client and response
    mock_client = MagicMock()
    mock_secret_client_class.return_value = mock_client
    
    mock_response = MagicMock()
    mock_payload = {
        "host": "10.120.4.5",
        "port": 1521,
        "service_name": "ORAPROD",
        "user": "dw_core_app",
        "pass": "SecretPassword123"
    }
    mock_response.payload.data = json.dumps(mock_payload).encode("UTF-8")
    mock_client.access_secret_version.return_value = mock_response

    # Execute
    loader = GCPEnvironmentLoader(project_id="gcp-prod-dwh", environment_phase="prod")
    credentials = loader.fetch_database_profile("dw_core")

    # Assertions
    expected_secret_path = "projects/gcp-prod-dwh/secrets/db-conn-dw_core-prod/versions/latest"
    mock_client.access_secret_version.assert_called_once_with(request={"name": expected_secret_path})
    assert credentials == mock_payload
```

#### Pass/Fail Criterion
* **Pass**: The secret path is constructed correctly, the API is called, and the decrypted JSON payload is returned as a dictionary.
* **Fail**: An incorrect secret path is requested, or the payload fails to parse.

### Test Case 3.2: Secret Manager Error Handling & Resilience
#### Purpose
Verify that the loader raises a `ConnectionError` when Secret Manager is unreachable, and a `ValueError` when the secret payload contains invalid JSON.

#### Setup
* Mock `SecretManagerServiceClient` to raise a `GoogleAPIError` for the connection failure test.
* Mock `SecretManagerServiceClient` to return non-JSON data for the parsing failure test.

#### Action
Execute `fetch_database_profile` under both failure scenarios and assert that the correct exceptions are raised.

```python
from google.api_core.exceptions import Forbidden

@patch("gcp_environment_loader.secretmanager.SecretManagerServiceClient")
def test_fetch_database_profile_api_error(mock_secret_client_class):
    mock_client = MagicMock()
    mock_secret_client_class.return_value = mock_client
    mock_client.access_secret_version.side_effect = Forbidden("Access Denied")

    loader = GCPEnvironmentLoader(project_id="gcp-prod-dwh", environment_phase="prod")
    with pytest.raises(ConnectionError) as exc_info:
        loader.fetch_database_profile("dw_core")
    assert "Failed to access database credentials" in str(exc_info.value)

@patch("gcp_environment_loader.secretmanager.SecretManagerServiceClient")
def test_fetch_database_profile_invalid_json(mock_secret_client_class):
    mock_client = MagicMock()
    mock_secret_client_class.return_value = mock_client
    mock_response = MagicMock()
    mock_response.payload.data = b"NOT_VALID_JSON"
    mock_client.access_secret_version.return_value = mock_response

    loader = GCPEnvironmentLoader(project_id="gcp-prod-dwh", environment_phase="prod")
    with pytest.raises(ValueError) as exc_info:
        loader.fetch_database_profile("dw_core")
    assert "is not valid JSON" in str(exc_info.value)
```

#### Pass/Fail Criterion
* **Pass**: The loader wraps API exceptions in a `ConnectionError` and payload issues in a `ValueError` with descriptive error messages.
* **Fail**: Raw exceptions leak out, or incorrect exception types are raised.

---

## Section 4: Concurrency & Lifecycle Control Tests

### Test Case 4.1: Job Initialization Handshake & Lock Acquisition (`.dw_init` Emulation)
#### Purpose
Verify that the loader prevents concurrent execution of the same job in the same environment by checking and setting an exclusive lock in Airflow.

#### Setup
* Mock `airflow.models.Variable.get` and `airflow.models.Variable.set`.

#### Action
1. Simulate a clean run where no lock exists (`Variable.get` returns `"FALSE"`). Assert that the lock is set to `"TRUE"`.
2. Simulate a blocked run where a lock already exists (`Variable.get` returns `"TRUE"`). Assert that a `PermissionError` is raised.

```python
@patch("gcp_environment_loader.Variable.set")
@patch("gcp_environment_loader.Variable.get")
def test_job_initialization_handshake_success(mock_get, mock_set):
    mock_get.return_value = "FALSE"
    
    loader = GCPEnvironmentLoader(project_id="gcp-prod-dwh", environment_phase="prod")
    status = loader.job_initialization_handshake("customer_dimension_update")
    
    assert status == "SUCCESS"
    mock_get.assert_called_once_with("lock_customer_dimension_update_prod", default_var="FALSE")
    mock_set.assert_called_once_with("lock_customer_dimension_update_prod", "TRUE")

@patch("gcp_environment_loader.Variable.get")
def test_job_initialization_handshake_locked_violation(mock_get):
    mock_get.return_value = "TRUE"
    
    loader = GCPEnvironmentLoader(project_id="gcp-prod-dwh", environment_phase="prod")
    with pytest.raises(PermissionError) as exc_info:
        loader.job_initialization_handshake("customer_dimension_update")
    
    assert "Concurrency Lock Violation" in str(exc_info.value)
```

#### Pass/Fail Criterion
* **Pass**: The lock is successfully acquired when free, and blocks execution with a `PermissionError` when already active.
* **Fail**: Multiple jobs can acquire the lock simultaneously, or the lock key is named incorrectly.

### Test Case 4.2: Job Finalization & Lock Release
#### Purpose
Verify that the loader successfully releases the execution lock during finalization, allowing subsequent runs to proceed.

#### Setup
* Mock `airflow.models.Variable.set`.

#### Action
Call `job_finalization_release` and verify that the lock variable is set to `"FALSE"`.

```python
@patch("gcp_environment_loader.Variable.set")
def test_job_finalization_release(mock_set):
    loader = GCPEnvironmentLoader(project_id="gcp-prod-dwh", environment_phase="prod")
    loader.job_finalization_release("customer_dimension_update", exit_status="SUCCESS")
    
    mock_set.assert_called_once_with("lock_customer_dimension_update_prod", "FALSE")
```

#### Pass/Fail Criterion
* **Pass**: The lock variable is set to `"FALSE"` upon finalization.
* **Fail**: The lock remains `"TRUE"`, causing subsequent runs to be permanently blocked.

---

## Section 5: Metadata Schema & Audit Assertions

### Test Case 5.1: BigQuery Metadata Schema Verification
#### Purpose
Verify that the BigQuery metadata and audit tables are created with the correct schemas, primary keys, and column descriptions.

#### Setup
* Access to a GCP environment with BigQuery enabled (or a local dry-run SQL parser).

#### Action
Execute dry-run queries against the DDL statements in `create_metadata_tables.sql` and assert schema compliance.

```sql
-- Assertion Query: Verify table existence and column types in INFORMATION_SCHEMA
SELECT 
  table_name, column_name, data_type, is_nullable
FROM 
  `control_metadata.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name IN ('env_variable_mapping', 'pipeline_execution_audit', 'database_schema_mapping')
ORDER BY 
  table_name, ordinal_position;
```

#### Pass/Fail Criterion
* **Pass**: 
  * `env_variable_mapping` contains `legacy_var_name` (STRING), `gcp_target_type` (STRING), `gcp_target_key` (STRING), and `environment_phase` (STRING).
  * `pipeline_execution_audit` contains `job_id` (STRING), `pipeline_name` (STRING), `start_timestamp` (TIMESTAMP), `end_timestamp` (TIMESTAMP), and `execution_status` (STRING).
  * `database_schema_mapping` contains `legacy_oracle_schema` (STRING), `bq_project_id` (STRING), and `bq_dataset_name` (STRING).
* **Fail**: Any table is missing, column data types are mismatched, or primary key constraints are violated.