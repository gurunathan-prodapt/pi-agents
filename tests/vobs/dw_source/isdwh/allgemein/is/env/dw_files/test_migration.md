Here is a comprehensive suite of migration-validation tests designed for the migrated environment configuration files of the job **Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files**.

These tests are structured to run within a modern Python/GCP testing framework (such as `pytest` with `mock` and `google-cloud` test helpers) to prove that the migrated Python modules (`dw_ai.py`, `dw_db.py`, `dw_global_config.py`, and `.dw_init.py`) are behaviourally equivalent to their legacy shell script counterparts.

---

# Migration Validation Test Suite

## Section 1: Output Parity & Variable Resolution Tests

### Test Case 1.1: Verbatim Environment Variable Mapping Parity (`dw_ai.py`)
* **Purpose**: Prove that executing the migrated `dw_ai.py` populates the exact environment variables with the same values as the legacy `.dw_ai` shell script, including dynamic resolution of `$HOME`.
* **Setup**: 
  * Set the system environment variable `HOME` to `/home/testuser`.
  * Clear any existing target environment variables (`AB_HOME`, `AB_AIR_ROOT`, `AB_AIR_HOME`, `ETL_Host`, `ETL_Projekt`, `AI_PRIV_SAND_ROOT`, `AI_ENV_SAND_ROOT`) from `os.environ`.
* **Action**: Run `dw_ai.main()` programmatically.
* **Pass/Fail Criterion**: 
  * **Pass**: All variables are correctly injected into `os.environ` with exact value parity (e.g., `AI_PRIV_SAND_ROOT` must resolve to `/home/testuser/abinitio`).
  * **Fail**: Any variable is missing, has an incorrect value, or fails to resolve `$HOME`.

```python
import os
import pytest
from unittest import mock
import vobs.dw_source.isdwh.allgemein.is.env.dw_files.dw_ai as dw_ai

def test_dw_ai_variable_parity():
    # Setup
    test_home = "/home/testuser"
    custom_env = {"HOME": test_home, "PATH": "/usr/bin:/bin"}
    
    with mock.patch.dict(os.environ, custom_env, clear=True):
        # Action
        dw_ai.main()
        
        # Assertions
        assert os.environ.get("AB_HOME") == "/appl/local/abinitio/abinitio"
        assert os.environ.get("AB_AIR_ROOT") == "/appl/local/abinitio/TMD_EME/eme_dev/repo"
        assert os.environ.get("AB_AIR_HOME") == "/appl/local/abinitio/abinitio-V2-14"
        assert os.environ.get("ETL_Host") == "dxcsa4.bn.detemobil.de"
        assert os.environ.get("ETL_Projekt") == "BHB"
        assert os.environ.get("AI_PRIV_SAND_ROOT") == f"{test_home}/abinitio"
        assert os.environ.get("AI_ENV_SAND_ROOT") == "/appl/local/abinitio/sandboxes/DEV"
        assert f"{os.environ.get('AB_HOME')}/bin" in os.environ.get("PATH")
```

---

### Test Case 1.2: Verbatim German Error Output Parity (`dw_global_config.py`)
* **Purpose**: Prove that the validation logic in `dw_global_config.py` outputs the exact German warning messages to standard output when mandatory variables are missing, matching the legacy `.dw_global` script.
* **Setup**: 
  * Mock `airflow.models.Variable.get` to return `None` or empty strings for all directory variables.
* **Action**: Execute `validate_environment_variables` with an empty environment dictionary and capture `stdout`.
* **Pass/Fail Criterion**: 
  * **Pass**: The captured output contains the exact string `"Fehler in .dw_global:"` followed by `"   Umgebungsvariable <VAR> ist nicht gesetzt !"` for every missing variable.
  * **Fail**: The output format deviates by even a single character or space from the legacy console output.

```python
import sys
from io import StringIO
import pytest
from unittest import mock
import vobs.dw_source.isdwh.allgemein.is.env.dw_files.dw_global_config as dw_global

def test_verbatim_german_error_output(capsys):
    # Setup: Create an empty environment configuration dictionary
    empty_config = {key: "" for key in dw_global.get_required_env_keys()}
    
    # Action
    dw_global.validate_environment_variables(empty_config)
    captured = capsys.readouterr().out
    
    # Assertions
    assert "Fehler in .dw_global:\n" in captured
    for key in dw_global.get_required_env_keys():
        expected_line = f"   Umgebungsvariable {key} ist nicht gesetzt !"
        assert expected_line in captured
```

---

## Section 2: Transformation & Type Correctness Tests

### Test Case 2.1: Oracle NLS to GCP Date Format Mapping (`dw_db.py`)
* **Purpose**: Verify that the `NLSNormalizer` class correctly translates legacy Oracle date formats and character sets into Python/GCP-compatible standard formats.
* **Setup**: Define a set of legacy Oracle NLS parameters.
* **Action**: Pass these parameters through `NLSNormalizer.map_character_set` and `NLSNormalizer.map_date_format`.
* **Pass/Fail Criterion**: 
  * **Pass**: 
    * `GERMAN_GERMANY.WE8ISO8859P1` or `AMERICAN_AMERICA.AL32UTF8` maps to `"UTF-8"`.
    * `"YYYY-MM-DD HH24:MI:SS"` maps to `"%Y-%m-%d %H:%M:%S"`.
    * `"YYYY-MM-DD"` maps to `"%Y-%m-%d"`.
  * **Fail**: Any mapping returns an incompatible format string or raises an unhandled exception.

```python
import pytest
from vobs.dw_source.isdwh.allgemein.is.env.dw_files.dw_db import NLSNormalizer

@pytest.mark.parametrize("legacy_lang,expected_charset", [
    ("GERMAN_GERMANY.WE8ISO8859P1", "UTF-8"),
    ("AMERICAN_AMERICA.AL32UTF8", "UTF-8"),
    ("UTF-8", "UTF-8")
])
def test_nls_character_set_mapping(legacy_lang, expected_charset):
    assert NLSNormalizer.map_character_set(legacy_lang) == expected_charset


@pytest.mark.parametrize("legacy_format,expected_python_format", [
    ("YYYY-MM-DD HH24:MI:SS", "%Y-%m-%d %H:%M:%S"),
    ("YYYY-MM-DD", "%Y-%m-%d"),
    ('"YYYY-MM-DD"', "%Y-%m-%d")
])
def test_nls_date_format_mapping(legacy_format, expected_python_format):
    assert NLSNormalizer.map_date_format(legacy_format) == expected_python_format
```

---

## Section 3: External System Replacements & Security Tests

### Test Case 3.1: Decoupled Secret Resolution via Google Secret Manager (`dw_db.py`)
* **Purpose**: Prove that the system securely retrieves database credentials from Google Secret Manager instead of reading local plain-text files, and gracefully handles authorization failures.
* **Setup**: 
  * Mock the `secretmanager.SecretManagerServiceClient` API.
  * Configure the mock to return a valid JSON payload containing database credentials for a successful run, and raise a `GoogleAPICallError` for an unauthorized run.
* **Action**: Instantiate `SecretResolver` and call `get_secret`.
* **Pass/Fail Criterion**: 
  * **Pass**: 
    * On success, the secret payload is retrieved, decrypted in memory, and parsed correctly.
    * On failure, a `RuntimeError` is raised containing `"SecretVersionResolutionError"`, and no credentials are leaked or cached.
  * **Fail**: Plain-text credentials are written to logs, or the system fails to raise the correct exception on API failure.

```python
import pytest
from unittest import mock
from google.api_core.exceptions import PermissionDenied
from vobs.dw_source.isdwh.allgemein.is.env.dw_files.dw_db import SecretResolver

@mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
def test_secret_resolver_success(mock_client_class):
    # Setup Mock
    mock_client = mock_client_class.return_value
    mock_response = mock.Mock()
    mock_response.payload.data = b'{"ORACLE_SID": "DWHPROD", "DB_USER_DWH": "meyreis"}'
    mock_client.access_secret_version.return_value = mock_response
    
    # Action
    resolver = SecretResolver(project_id="test-gcp-project")
    secret = resolver.get_secret(secret_name="dw-database-access-credentials")
    
    # Assertions
    assert secret["ORACLE_SID"] == "DWHPROD"
    assert secret["DB_USER_DWH"] == "meyreis"


@mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
def test_secret_resolver_permission_denied(mock_client_class):
    # Setup Mock to simulate IAM failure
    mock_client = mock_client_class.return_value
    mock_client.access_secret_version.side_effect = PermissionDenied("IAM policy violation")
    
    # Action & Assertion
    resolver = SecretResolver(project_id="test-gcp-project")
    with pytest.raises(RuntimeError, match="SecretVersionResolutionError"):
        resolver.get_secret(secret_name="dw-database-access-credentials")
```

---

## Section 4: Data Quality, Schema, & Audit Logging Assertions

### Test Case 4.1: BigQuery Audit Logging and Schema Verification (`dw_db.py`)
* **Purpose**: Verify that the `BigQueryIntegrationManager` correctly formats and writes audit logs and configuration states to BigQuery with the correct schema fields.
* **Setup**: 
  * Mock the `bigquery.Client` object.
  * Capture the JSON payload sent to `insert_rows_json`.
* **Action**: Call `write_audit_log` with standard execution parameters.
* **Pass/Fail Criterion**: 
  * **Pass**: The payload sent to BigQuery contains all required schema fields (`log_id`, `job_name`, `execution_status`, `records_processed`, `error_message`, `started_at`, `finished_at`) with correct data types.
  * **Fail**: Any schema field is missing, or the timestamp format is invalid.

```python
import pytest
from unittest import mock
from vobs.dw_source.isdwh.allgemein.is.env.dw_files.dw_db import BigQueryIntegrationManager

@mock.patch("google.cloud.bigquery.Client")
def test_bigquery_audit_logging_schema(mock_bq_client_class):
    # Setup
    mock_bq_client = mock_bq_client_class.return_value
    mock_bq_client.insert_rows_json.return_value = []  # No errors
    
    manager = BigQueryIntegrationManager(project_id="test-project", dataset_id="metadata")
    
    # Action
    success = manager.write_audit_log(
        table_name="gcp_migration_audit_log",
        log_id="test-uuid-12345",
        job_name="dw_db_migration_initialization",
        status="SUCCESS",
        records_processed=100,
        error_message=None
    )
    
    # Assertions
    assert success is True
    mock_bq_client.insert_rows_json.assert_called_once()
    args, kwargs = mock_bq_client.insert_rows_json.call_args
    
    target_table = args[0]
    payload_list = args[1]
    
    assert target_table == "test-project.metadata.gcp_migration_audit_log"
    assert len(payload_list) == 1
    
    record = payload_list[0]
    assert record["log_id"] == "test-uuid-12345"
    assert record["job_name"] == "dw_db_migration_initialization"
    assert record["execution_status"] == "SUCCESS"
    assert record["records_processed"] == 100
    assert "started_at" in record
    assert "finished_at" in record
```

---

### Test Case 4.2: Directory Path Resolution Integrity (`.dw_init.py`)
* **Purpose**: Prove that the directory paths generated by `EnvInitializer` are structurally correct, resolve placeholders recursively, and map to the correct GCS bucket structure.
* **Setup**: 
  * Set `HOME` to `/home/user` and `GCS_BUCKET` to `prod-dwh-bucket`.
* **Action**: Run `set_base_directories()` on `EnvInitializer`.
* **Pass/Fail Criterion**: 
  * **Pass**: 
    * All 40+ directory variables are generated.
    * Every path starting with `$HOME` is correctly resolved to `/home/user`.
    * The `GCS_BUCKET` variable is correctly set to `prod-dwh-bucket`.
  * **Fail**: Any path contains unresolved literal `"$HOME"` or `"${HOME}"` strings, or the GCS bucket is unassigned.

```python
import os
import pytest
from unittest import mock
from isdwh.allgemein.is.env.dw_files.dw_init import EnvInitializer

def test_directory_path_resolution():
    # Setup
    custom_env = {
        "HOME": "/home/user",
        "GCS_BUCKET": "prod-dwh-bucket"
    }
    
    with mock.patch.dict(os.environ, custom_env, clear=True):
        # Action
        initializer = EnvInitializer(use_airflow_vars=False)
        resolved_paths = initializer.set_base_directories()
        
        # Assertions
        assert resolved_paths["DW_DIR_ROOT"] == "/home/user/aktuell"
        assert resolved_paths["DW_DIR_PROT"] == "/home/user/daten/logfiles"
        assert resolved_paths["DW_DIR_IMP_D1"] == "/home/user/daten/d1"
        assert resolved_paths["DW_DIR_IMP_PLATO"] == "/home/user/daten/dwh/plato"
        assert resolved_paths["GCS_BUCKET"] == "prod-dwh-bucket"
        
        # Verify no literal placeholders remain
        for key, path in resolved_paths.items():
            assert "$HOME" not in path
            assert "${HOME}" not in path
```