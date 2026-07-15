Here is the comprehensive migration-validation test suite for the **Shared Files** job (`vobs/dw_source/isdwh/allgemein/is/env/dw_files`). 

Since this job is an environment-initialization and configuration component rather than a data-pipeline job, the validation strategy focuses on **environment state parity, dynamic path resolution, secure credential retrieval, and validation-engine correctness**.

---

# Test Case 1: Environment Variable Parity & Value Mapping

### Purpose
To verify that executing the migrated Python scripts (`dw_init.py` and `dw_global.py`) populates the OS environment with the correct, behaviorally equivalent values as the legacy KornShell scripts (`.dw_init`, `.dw_global`, `.dw_ai`, and `.dw_db`).

### Setup
* A clean Python environment mimicking the Cloud Composer (Airflow) worker environment.
* Environment variables `GCS_BUCKET` set to `test-dwh-bucket` and `GCP_PROJECT` set to `test-gcp-project`.
* Mocked Google Secret Manager API to prevent external network calls during unit testing.

### Action
Execute the migrated initialization sequence and assert that all environment variables match their expected legacy-to-cloud mapped values.

```python
import os
import pytest
from unittest.mock import patch

# Import the migrated modules
import dw_config_helper
import dw_global
import dw_init

@pytest.fixture(autouse=True)
def setup_env():
    # Clear target environment variables to ensure clean state
    vars_to_clear = [
        "DW_DIR_ROOT", "DW_DIR_PROT", "DW_DIR_CUBES", "DW_DIR_IMP_D1", 
        "DW_DIR_IMP_XTRA", "DW_HOST_CUSTOMER", "ETL_Host", "ETL_Projekt",
        "NLS_LANG", "NLS_DATE_FORMAT", "NLS_DATE_LANGUAGE", "LANG",
        "DB_TNS_NAME_DWH", "DB_USER_DWH", "DB_PASSWD_DWH"
    ]
    for var in vars_to_clear:
        os.environ.pop(var, None)
        
    # Set GCP environment variables
    os.environ["GCS_BUCKET"] = "test-dwh-bucket"
    os.environ["GCP_PROJECT"] = "test-gcp-project"
    os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"

@patch("dw_config_helper.GCPConfigHelper.get_secret")
def test_environment_variable_parity(mock_get_secret):
    # Mock Secret Manager response
    mock_get_secret.return_value = "secret_decrypted_password"
    
    # Run the initialization entrypoint
    dw_init.main()
    
    # Assertions: Core Paths mapped to GCS
    assert os.environ.get("DW_DIR_ROOT") == "gs://test-dwh-bucket/dwh/core"
    assert os.environ.get("DW_DIR_PROT") == "gs://test-dwh-bucket/dwh/logs"
    assert os.environ.get("DW_DIR_CUBES") == "gs://test-dwh-bucket/dwh/cubes"
    assert os.environ.get("DW_DIR_IMP_D1") == "gs://test-dwh-bucket/dwh/imports/d1"
    assert os.environ.get("DW_DIR_IMP_XTRA") == "gs://test-dwh-bucket/dwh/imports/xtra"
    
    # Assertions: Legacy Host & Project Metadata
    assert os.environ.get("DW_HOST_CUSTOMER") == "dxcst3.bn.detemobil.de"
    assert os.environ.get("ETL_Host") == "dxcsa4.bn.detemobil.de"
    assert os.environ.get("ETL_Projekt") == "BHB"
    
    # Assertions: Database Session / NLS Settings
    assert os.environ.get("NLS_LANG") == "GERMAN_GERMANY.WE8ISO8859P1"
    assert os.environ.get("NLS_DATE_FORMAT") == "DD.MM.YY"
    assert os.environ.get("NLS_DATE_LANGUAGE") == "GERMAN_GERMANY.WE8ISO8859P1"
    assert os.environ.get("LANG") == "de"
    
    # Assertions: Database Connection Properties
    assert os.environ.get("DB_TNS_NAME_DWH") == "@eDWH3.devlab.de.tmo"
    assert os.environ.get("DB_USER_DWH") == "meyreis"
    assert os.environ.get("DB_PASSWD_DWH") == "secret_decrypted_password"
```

### Pass/Fail Criterion
* **Pass**: All asserted environment variables are populated with the exact mapped GCS URIs and legacy metadata strings.
* **Fail**: Any variable is missing, contains an incorrect GCS prefix structure, or fails to resolve.

---

# Test Case 2: Validation Engine Correctness (`dw_global` Assertions)

### Purpose
To verify that the validation engine in `dw_global.py` correctly identifies missing required environment variables, outputs the exact legacy error messages to `stderr`, and behaves identically to the legacy `.dw_global` shell script.

### Setup
* A test runner that intercepts `sys.stderr` and `sys.stdout`.
* A clean environment where some required variables (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`) are intentionally omitted.

### Action
Execute `setup_global_environment()` under incomplete environment conditions and capture the output.

```python
import sys
from io import StringIO
import pytest
from unittest.mock import patch
import dw_global

def test_validation_engine_missing_variables():
    # Clear environment to trigger validation failures
    vars_to_clear = ["DW_DIR_ROOT", "DW_DIR_PROT", "ORACLE_HOME"]
    for var in vars_to_clear:
        os.environ.pop(var, None)
        
    # Set only a subset of required variables
    os.environ["DW_DIR_CUBES"] = "gs://test-bucket/cubes"
    os.environ["DW_DIR_IMP_D1"] = "gs://test-bucket/d1"
    os.environ["DW_DIR_IMP_XTRA"] = "gs://test-bucket/xtra"
    os.environ["DW_DIR_IMP_CTEL"] = "gs://test-bucket/ctel"
    os.environ["DW_DIR_IMP_VO"] = "gs://test-bucket/vo"
    os.environ["DW_DIR_IMP_RV"] = "gs://test-bucket/rv"
    os.environ["DW_DIR_IMP_IF"] = "gs://test-bucket/ees"
    os.environ["DW_DIR_IMP_NNV"] = "gs://test-bucket/nnv"

    # Capture stderr
    captured_stderr = StringIO()
    with patch("sys.stderr", new=captured_stderr):
        dw_global.setup_global_environment()
        
    output = captured_stderr.getvalue()
    
    # Assert legacy error header is printed
    assert "Fehler in .dw_global:" in output
    # Assert specific missing variables are flagged in the legacy format
    assert "Umgebungsvariable DW_DIR_ROOT ist nicht gesetzt !" in output
    assert "Umgebungsvariable DW_DIR_PROT ist nicht gesetzt !" in output
    assert "Umgebungsvariable ORACLE_HOME ist nicht gesetzt !" in output
```

### Pass/Fail Criterion
* **Pass**: The script writes the exact legacy error messages to `sys.stderr` for all missing variables, while not flagging variables that are present.
* **Fail**: The script exits silently, logs to the wrong stream, or uses a different error message format than the legacy shell script.

---

# Test Case 3: External System Replacement (Secret Manager Integration)

### Purpose
To verify that database passwords are no longer stored in plain text or decrypted using the legacy `m_password` utility, but are instead securely retrieved from Google Cloud Secret Manager.

### Setup
* Google Cloud Secret Manager API mocked to simulate both successful retrieval and fallback scenarios.
* `GCP_PROJECT` environment variable set.

### Action
Run the database credential loader under two scenarios:
1. **Scenario A**: Secret Manager is fully accessible (Happy Path).
2. **Scenario B**: Secret Manager is inaccessible or the project is local (Fallback Path).

```python
import os
from unittest.mock import patch, MagicMock
import pytest
import dw_init

@patch("google.cloud.secretmanager.SecretManagerServiceClient")
def test_secret_manager_happy_path(mock_client_class):
    # Setup mock client behavior
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    
    # Mock payload response
    mock_response = MagicMock()
    mock_response.payload.data = b"super_secure_cloud_password"
    mock_client.access_secret_version.return_value = mock_response
    
    os.environ["GCP_PROJECT"] = "prod-dwh-project"
    
    # Execute credential load
    dw_init.load_db_credentials()
    
    # Verify Secret Manager was called with correct pathing
    mock_client.access_secret_version.assert_called_once_with(
        request={"name": "projects/prod-dwh-project/secrets/DB_PASSWD_DWH/versions/latest"}
    )
    # Verify environment variable was set to the secret value
    assert os.environ.get("DB_PASSWD_DWH") == "super_secure_cloud_password"


@patch("google.cloud.secretmanager.SecretManagerServiceClient")
def test_secret_manager_fallback(mock_client_class):
    # Force Secret Manager client to throw an exception (e.g., permission denied)
    mock_client_class.side_effect = Exception("Permission Denied")
    
    os.environ["GCP_PROJECT"] = "prod-dwh-project"
    os.environ.pop("DB_PASSWD_DWH", None)
    
    # Execute credential load
    dw_init.load_db_credentials()
    
    # Verify fallback to legacy placeholder string occurs gracefully
    assert os.environ.get("DB_PASSWD_DWH") == "<password encrypted with m_password>"
```

### Pass/Fail Criterion
* **Pass**: The application successfully queries Secret Manager using the correct GCP Project ID and falls back to the legacy placeholder string without crashing if the service is unavailable.
* **Fail**: The application crashes during secret resolution, leaks credentials in logs, or fails to set `DB_PASSWD_DWH`.

---

# Test Case 4: Dynamic Oracle Client Path Resolution

### Purpose
To verify that the dynamic resolution of `ORACLE_HOME` behaves identically to the legacy `.dw_init` script when running on hybrid runtimes (e.g., self-hosted Airflow runners with local Oracle clients).

### Setup
* Mocked filesystem paths using `unittest.mock.patch` to simulate the presence or absence of Oracle installation directories (`/appl/local/oracle/12.2.0.1.0` and `/appl/local/oracle/11.2.0`).

### Action
Execute path initialization under three filesystem states:
1. **State A**: Oracle 12c directory exists.
2. **State B**: Oracle 12c is missing, but Oracle 11g exists.
3. **State C**: No Oracle directories exist.

```python
import os
from unittest.mock import patch
import pytest
import dw_init

@patch("os.path.isdir")
def test_oracle_home_resolution_12c(mock_isdir):
    # Simulate that Oracle 12c directory exists
    mock_isdir.side_effect = lambda path: path == "/appl/local/oracle/12.2.0.1.0"
    
    os.environ.pop("ORACLE_HOME", None)
    dw_init.initialize_paths()
    
    assert os.environ.get("ORACLE_HOME") == "/appl/local/oracle/12.2.0.1.0"


@patch("os.path.isdir")
def test_oracle_home_resolution_11g(mock_isdir):
    # Simulate that only Oracle 11g directory exists
    mock_isdir.side_effect = lambda path: path == "/appl/local/oracle/11.2.0"
    
    os.environ.pop("ORACLE_HOME", None)
    dw_init.initialize_paths()
    
    assert os.environ.get("ORACLE_HOME") == "/appl/local/oracle/11.2.0"


@patch("os.path.isdir")
@patch("builtins.print")
def test_oracle_home_resolution_missing(mock_print, mock_isdir):
    # Simulate no Oracle directories exist
    mock_isdir.return_value = False
    
    os.environ.pop("ORACLE_HOME", None)
    dw_init.initialize_paths()
    
    # Assert warning is printed to stdout/stderr as in legacy
    mock_print.assert_any_call("Fehler in .dw_init:")
    mock_print.assert_any_call("   Konnte ORACLE_HOME nicht setzen !")
    assert os.environ.get("ORACLE_HOME") == ""
```

### Pass/Fail Criterion
* **Pass**: The resolution logic prioritizes Oracle 12c over 11g, falls back to 11g if 12c is missing, and prints the exact legacy warning message if neither is found.
* **Fail**: The resolution logic selects the wrong path, fails to warn when directories are missing, or crashes.