Here is the comprehensive migration-validation test suite for the **Shared Files** configuration module. 

Since this component is an environment initialization profile (`UC4_ONLY / Cloud Composer` pattern) rather than a traditional ETL pipeline, the validation strategy focuses on **state correctness, path resolution parity, dynamic fallback logic, and secure credential management**.

---

# Test Suite: Shared Files Environment Configuration Validation

## 1. Output Parity: Environment Variable State Parity
### Purpose
Verify that executing the migrated Python module `dwh_env_config.py` results in an environment state (`os.environ`) that is functionally equivalent to sourcing the legacy shell scripts (`.dw_ai`, `.dw_db`, `.dw_init`, `.dw_global`).

### Setup
- A test runner environment with Python 3.8+ and `pytest`.
- Mock the `airflow.models.Variable` class to prevent execution failure outside of an active Airflow worker.

### Action
Execute the migrated Python initialization sequence and compare the resulting environment variables against the expected legacy values.

```python
import os
import sys
import pytest
from unittest.mock import MagicMock

# Mock Airflow before importing the config manager
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.models'] = MagicMock()
from airflow.models import Variable
Variable.get = MagicMock(return_value="gs://test_migration_bucket")

# Import the migrated module
from dwh_env_config import DWHConfigManager

def test_environment_variable_parity():
    # Clear any existing target environment variables to prevent pollution
    target_keys = [
        "AB_HOME", "AB_AIR_ROOT", "AB_AIR_HOME", "ETL_Host", "ETL_Projekt",
        "AI_PRIV_SAND_ROOT", "AI_ENV_SAND_ROOT", "NLS_LANG", "DB_TNS_NAME_DWH",
        "DB_USER_DWH", "NLS_DATE_FORMAT", "NLS_DATE_LANGUAGE", "LANG", "DW_HOST_CUSTOMER"
    ]
    for key in target_keys:
        os.environ.pop(key, None)

    # Initialize Manager
    manager = DWHConfigManager(gcs_bucket_override="gs://test_migration_bucket")
    
    # Load profiles in sequence
    manager.load_init_profile()
    manager.load_ai_profile()
    manager.load_db_profile(use_secret_manager=False)
    manager.validate_global_profile()

    # Assertions: Ab Initio Parity (.dw_ai)
    assert os.environ.get("AB_HOME") == "/appl/local/abinitio/abinitio"
    assert os.environ.get("AB_AIR_ROOT") == "/appl/local/abinitio/TMD_EME/eme_dev/repo"
    assert os.environ.get("AB_AIR_HOME") == "/appl/local/abinitio/abinitio-V2-14"
    assert os.environ.get("ETL_Host") == "dxcsa4.bn.detemobil.de"
    assert os.environ.get("ETL_Projekt") == "BHB"
    assert os.environ.get("AI_ENV_SAND_ROOT") == "/appl/local/abinitio/sandboxes/DEV"
    
    # Assertions: DB Parity (.dw_db)
    assert os.environ.get("NLS_LANG") == "GERMAN_GERMANY.WE8ISO8859P1"
    assert os.environ.get("DB_TNS_NAME_DWH") == "@eDWH3.devlab.de.tmo"
    assert os.environ.get("DB_USER_DWH") == "meyreis"

    # Assertions: Global Parity (.dw_global)
    assert os.environ.get("NLS_DATE_FORMAT") == "DD.MM.YY"
    assert os.environ.get("NLS_DATE_LANGUAGE") == "GERMAN_GERMANY.WE8ISO8859P1"
    assert os.environ.get("LANG") == "de"

    # Assertions: Remote Host Parity (.dw_init)
    assert os.environ.get("DW_HOST_CUSTOMER") == "dxcst3.bn.detemobil.de"
```

### Pass/Fail Criterion
- **Pass:** All asserted environment variables are populated in `os.environ` with values matching the legacy shell defaults exactly.
- **Fail:** Any variable is missing, or its value deviates from the legacy specification.

---

## 2. Transformation Correctness: GCS Path Mapping Logic
### Purpose
Verify that the legacy local filesystem paths (`$HOME/daten/*`) are correctly transformed into Cloud Storage URI paths (`gs://{GCS_BUCKET}/dwh/daten/*`) based on the GCP Mapping Policy.

### Setup
- Set up a mock GCS bucket name: `gs://prod_dwh_storage_bucket`.

### Action
Initialize the configuration manager with the mock bucket and verify that the generated paths conform to the cloud storage structure.

```python
def test_gcs_path_mapping():
    bucket_name = "gs://prod_dwh_storage_bucket"
    manager = DWHConfigManager(gcs_bucket_override=bucket_name)
    init_vars = manager.load_init_profile()

    # Verify base root mapping
    assert init_vars["DW_DIR_ROOT"] == f"{bucket_name}/dwh/aktuell"
    assert init_vars["DW_DIR_PROT"] == f"{bucket_name}/dwh/daten/logfiles"

    # Verify nested import directories
    assert init_vars["DW_DIR_IMP_D1"] == f"{bucket_name}/dwh/daten/d1"
    assert init_vars["DW_DIR_IMP_BWA"] == f"{bucket_name}/dwh/daten/dpps/bwa"
    assert init_vars["DW_DIR_IMP_SAP_L_GUTGR"] == f"{bucket_name}/dwh/daten/sap/sap_l_gutgr"
    assert init_vars["DW_DIR_IMP_L_MAHNSTYP_IST"] == f"{bucket_name}/dwh/daten/sap/mahn"
    
    # Verify SMS modules
    assert init_vars["DW_DIR_SMS_PRG"] == f"{bucket_name}/dwh/aktuell/allgemein/is/util"
    assert init_vars["DW_DIR_SMS_ADR"] == f"{bucket_name}/dwh/daten/sms/adressen"
```

### Pass/Fail Criterion
- **Pass:** Every directory path variable starts with the prefix `gs://prod_dwh_storage_bucket/dwh/` and matches the legacy subpath structure.
- **Fail:** Paths contain literal `$HOME` strings, local file paths, or incorrect subdirectories.

---

## 3. Transformation Correctness: Dynamic Oracle Home & SID Fallback
### Purpose
Verify that the dynamic fallback logic for locating `ORACLE_HOME` and constructing `DW_DIR_UTL_FILE` behaves identically to the legacy shell script under different system states.

### Setup
- Use `pytest`'s `monkeypatch` to simulate different directory structures on the host system and different values of `$ORACLE_SID`.

### Action
Test three scenarios:
1. Oracle 12c directory exists.
2. Oracle 12c is missing, but Oracle 11g exists.
3. Neither exists (should trigger the exact legacy German warning to `stderr`).

```python
import sys

def test_oracle_home_resolution_12c(monkeypatch, tmp_path):
    # Scenario 1: Oracle 12.2 exists
    fake_oracle_12 = tmp_path / "appl/local/oracle/12.2.0.1.0"
    fake_oracle_12.mkdir(parents=True)
    
    # Patch Path checks to point to our temp directory
    monkeypatch.setattr("dwh_env_config.Path", lambda p: tmp_path / p.relative_to("/"))
    monkeypatch.setenv("ORACLE_SID", "PROD_DWH")
    os.environ.pop("ORACLE_HOME", None)

    manager = DWHConfigManager()
    init_vars = manager.load_init_profile()

    assert init_vars["ORACLE_HOME"] == str(fake_oracle_12)
    assert init_vars["DW_DIR_UTL_FILE"] == "/appl/local/oracle/admin/PROD_DWH/utl_file"


def test_oracle_home_resolution_11g(monkeypatch, tmp_path):
    # Scenario 2: Only Oracle 11.2 exists
    fake_oracle_11 = tmp_path / "appl/local/oracle/11.2.0"
    fake_oracle_11.mkdir(parents=True)
    
    monkeypatch.setattr("dwh_env_config.Path", lambda p: tmp_path / p.relative_to("/"))
    os.environ.pop("ORACLE_HOME", None)

    manager = DWHConfigManager()
    init_vars = manager.load_init_profile()

    assert init_vars["ORACLE_HOME"] == str(fake_oracle_11)


def test_oracle_home_missing_warning(monkeypatch, capsys):
    # Scenario 3: No Oracle home directories exist
    monkeypatch.setattr("dwh_env_config.Path", lambda p: MagicMock(is_dir=lambda: False))
    os.environ.pop("ORACLE_HOME", None)

    manager = DWHConfigManager()
    manager.load_init_profile()

    # Capture stderr output
    captured = capsys.readouterr()
    assert "Fehler in .dw_init:" in captured.err
    assert "   Konnte ORACLE_HOME nicht setzen !" in captured.err
```

### Pass/Fail Criterion
- **Pass:** 
  - Oracle 12c is preferred over 11g.
  - `DW_DIR_UTL_FILE` dynamically incorporates the active `$ORACLE_SID`.
  - Missing directories output the exact German error message: `"   Konnte ORACLE_HOME nicht setzen !"` to `sys.stderr`.
- **Fail:** The wrong directory is selected, or the error message does not match the legacy output.

---

## 4. External-System Replacements: Secret Manager Integration
### Purpose
Ensure that database passwords are not stored in plaintext or legacy-encrypted formats inside the code repository, and that they are securely retrieved from Google Cloud Secret Manager.

### Setup
- Mock the Google Cloud Secret Manager API client.
- Configure a mock secret payload.

### Action
Call `load_db_profile` with `use_secret_manager=True` and assert that the password variable is populated with the secret retrieved from GCP.

```python
from unittest.mock import patch

@patch("google.cloud.secretmanager.SecretManagerServiceClient")
def test_secret_manager_password_retrieval(mock_client_class):
    # Mock the Secret Manager client response
    mock_client = mock_client_class.return_value
    mock_response = MagicMock()
    mock_response.payload.data.decode.return_value = "SuperSecureDecryptedPassword123"
    mock_client.access_secret_version.return_value = mock_response

    manager = DWHConfigManager()
    db_vars = manager.load_db_profile(
        use_secret_manager=True, 
        secret_id="projects/12345/secrets/dwh-oracle-pass/versions/latest"
    )

    # Assert password was fetched and decrypted correctly
    assert db_vars["DB_PASSWD_DWH"] == "SuperSecureDecryptedPassword123"
    assert os.environ["DB_PASSWD_DWH"] == "SuperSecureDecryptedPassword123"
    
    # Verify client was called with correct resource path
    mock_client.access_secret_version.assert_called_once_with(
        request={"name": "projects/12345/secrets/dwh-oracle-pass/versions/latest"}
    )
```

### Pass/Fail Criterion
- **Pass:** The Secret Manager client is called with the correct secret ID, and the returned value is successfully bound to `os.environ['DB_PASSWD_DWH']`.
- **Fail:** The password defaults to the legacy placeholder `<password encrypted with m_password>` or the API call fails without a safe fallback.

---

## 5. Data-Quality & Validation Assertions: Global Profile Validation
### Purpose
Verify that the validation logic in `.dw_global` correctly identifies missing environment variables and outputs the exact legacy error log format.

### Setup
- Clear all environment variables.
- Capture `sys.stderr` outputs.

### Action
Run `validate_global_profile` when required variables are missing, and again when all variables are present.

```python
def test_global_profile_validation(monkeypatch, capsys):
    # Clear environment to trigger validation errors
    for var in ["DW_DIR_ROOT", "DW_DIR_PROT", "ORACLE_HOME"]:
        monkeypatch.delenv(var, raising=False)

    manager = DWHConfigManager()
    is_valid, missing = manager.validate_global_profile()

    assert is_valid is False
    assert "DW_DIR_ROOT" in missing
    assert "DW_DIR_PROT" in missing
    assert "ORACLE_HOME" in missing

    # Capture stderr and verify legacy output format
    captured = capsys.readouterr()
    assert "Fehler in .dw_global:" in captured.err
    assert "   Umgebungsvariable DW_DIR_ROOT ist nicht gesetzt !" in captured.err
    assert "   Umgebungsvariable ORACLE_HOME ist nicht gesetzt !" in captured.err


def test_global_profile_validation_success(monkeypatch):
    # Populate all required variables
    required_vars = [
        "DW_DIR_ROOT", "DW_DIR_PROT", "DW_DIR_CUBES", "DW_DIR_IMP_D1",
        "DW_DIR_IMP_XTRA", "DW_DIR_IMP_CTEL", "DW_DIR_IMP_VO", "DW_DIR_IMP_RV",
        "DW_DIR_IMP_IF", "DW_DIR_IMP_NNV", "ORACLE_HOME"
    ]
    for var in required_vars:
        monkeypatch.setenv(var, "/tmp/valid_path")

    manager = DWHConfigManager()
    is_valid, missing = manager.validate_global_profile()

    assert is_valid is True
    assert len(missing) == 0
```

### Pass/Fail Criterion
- **Pass:** 
  - If variables are missing, `is_valid` returns `False` and the exact German error block is printed to `stderr`.
  - If all variables are present, `is_valid` returns `True` and no errors are printed.
- **Fail:** The validation returns incorrect status codes, or the error messages deviate from the legacy shell script output.