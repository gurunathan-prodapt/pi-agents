# Migration Validation Test Suite: Shared Files (`dw_files`)

This document defines the migration-validation tests for the environment configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) migrated from legacy KornShell scripts to Python modules.

---

## Test Case 1: Output Parity — Environment Variable Mapping & Value Equivalence

### Purpose
To verify that the migrated Python modules populate the runtime environment (`os.environ`) with the exact variable names and values defined in the legacy shell scripts, ensuring downstream jobs receive identical configurations.

### Setup
*   **Target Environment**: Python 3.8+ environment with the migrated files (`helpers.py`, `dw_ai.py`, `dw_db.py`, `dw_global.py`, `dw_init.py`) placed in the Python path.
*   **Mocking**: Mock `GCS_BUCKET` to match the legacy `$HOME` directory structure to verify path equivalence.

### Action
Execute a validation script that runs the migrated Python modules and asserts that the resulting environment variables match the legacy specifications.

```python
# test_output_parity.py
import os
import pytest
from unittest import mock

# Import the migrated modules
import dw_ai
import dw_db
import dw_init


@mock.patch.dict(os.environ, {
    "HOME": "/home/testuser",
    "GCS_BUCKET": "gs://test_bucket",
    "ORACLE_HOME": "/appl/local/oracle/12.2.0.1.0",
    "ORACLE_SID": "TEST_SID"
})
def test_environment_variable_parity():
    # 1. Execute Ab Initio configuration
    dw_ai.configure_ab_initio_env()
    
    # Assert Ab Initio variables match legacy values
    assert os.environ.get('AB_HOME') == '/appl/local/abinitio/abinitio'
    assert os.environ.get('AB_AIR_ROOT') == '/appl/local/abinitio/TMD_EME/eme_dev/repo'
    assert os.environ.get('AB_AIR_HOME') == '/appl/local/abinitio/abinitio-V2-14'
    assert os.environ.get('ETL_Host') == 'dxcsa4.bn.detemobil.de'
    assert os.environ.get('ETL_Projekt') == 'BHB'
    assert os.environ.get('AI_PRIV_SAND_ROOT') == '/home/testuser/abinitio'
    assert os.environ.get('AI_ENV_SAND_ROOT') == '/appl/local/abinitio/sandboxes/DEV'
    assert '/appl/local/abinitio/abinitio/bin' in os.environ.get('PATH', '')

    # 2. Execute Database configuration
    db_configs = dw_db.get_database_configs()
    for k, v in db_configs.items():
        os.environ[k] = v
        
    # Assert Database variables match legacy values
    assert os.environ.get('DB_TNS_NAME_DWH') == '@eDWH3.devlab.de.tmo'
    assert os.environ.get('DB_USER_DWH') == 'meyreis'
    assert os.environ.get('NLS_LANG') == 'GERMAN_GERMANY.WE8ISO8859P1'

    # 3. Execute Master Initialization
    dw_init.bootstrap_environment()
    
    # Assert GCS path mappings match legacy structures (with GCS bucket prefix replacement)
    assert os.environ.get('DW_DIR_ROOT') == 'gs://test_bucket/aktuell'
    assert os.environ.get('DW_DIR_PROT') == 'gs://test_bucket/daten/logfiles'
    assert os.environ.get('DW_DIR_CUBES') == 'gs://test_bucket/daten/cubes'
    assert os.environ.get('DW_DIR_IMP_D1') == 'gs://test_bucket/daten/d1'
    assert os.environ.get('DW_DIR_IMP_BWA') == 'gs://test_bucket/daten/dpps/bwa'
    assert os.environ.get('DW_DIR_IMP_XTRA') == 'gs://test_bucket/daten/xtra'
    assert os.environ.get('DW_DIR_IMP_CTEL') == 'gs://test_bucket/daten/ctel'
    assert os.environ.get('DW_DIR_IMP_VO') == 'gs://test_bucket/daten/vo'
    assert os.environ.get('DW_DIR_IMP_RV') == 'gs://test_bucket/daten/rv'
    assert os.environ.get('DW_DIR_IMP_IF') == 'gs://test_bucket/daten/ees'
    assert os.environ.get('DW_DIR_IMP_NNV') == 'gs://test_bucket/daten/nnv'
    assert os.environ.get('DW_DIR_UTL_FILE') == '/appl/local/oracle/admin/TEST_SID/utl_file'
```

### Pass/Fail Criterion
*   **Pass**: All assertions in `test_environment_variable_parity` pass. Every legacy variable is present in `os.environ` with the correct mapped value.
*   **Fail**: Any assertion fails, indicating a missing variable, incorrect value mapping, or path resolution failure.

---

## Test Case 2: Transformation Correctness — Legacy Shell Parser (`helpers.py`)

### Purpose
To verify that the legacy shell parser (`load_legacy_shell_exports`) correctly parses POSIX-compliant shell exports, handles comments, ignores empty lines, and strips surrounding quotes.

### Setup
*   Create a temporary legacy-style configuration file containing edge cases (comments, spaces, single/double quotes, and exports).

### Action
Run the parser against the mock file and assert the parsed dictionary output.

```python
# test_helpers.py
import os
import tempfile
from helpers import load_legacy_shell_exports


def test_legacy_shell_parser():
    # Create a temporary legacy configuration file
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_cfg:
        temp_cfg.write("# This is a comment line\n")
        temp_cfg.write("\n")  # Empty line
        temp_cfg.write("SIMPLE_VAR=value1\n")
        temp_cfg.write("export EXPORTED_VAR=value2\n")
        temp_cfg.write("QUOTED_VAR=\"value3\"\n")
        temp_cfg.write("SINGLE_QUOTED_VAR='value4'\n")
        temp_cfg.write("  SPACED_VAR  =  value5  \n")
        temp_cfg.write("COMPLEX_VAL=\"key=val_with_equals\"\n")
        temp_cfg_path = temp_cfg.name

    try:
        env_dict = {}
        load_legacy_shell_exports(temp_cfg_path, env_dict)
        
        # Assertions
        assert env_dict["SIMPLE_VAR"] == "value1"
        assert env_dict["EXPORTED_VAR"] == "value2"
        assert env_dict["QUOTED_VAR"] == "value3"
        assert env_dict["SINGLE_QUOTED_VAR"] == "value4"
        assert env_dict["SPACED_VAR"] == "value5"
        assert env_dict["COMPLEX_VAL"] == "key=val_with_equals"
        assert len(env_dict) == 6
        
    finally:
        os.unlink(temp_cfg_path)
```

### Pass/Fail Criterion
*   **Pass**: The parser successfully extracts all variables, correctly handles spaces/quotes, ignores comments/empty lines, and returns the expected dictionary.
*   **Fail**: The parser throws an exception, fails to parse valid lines, or includes comments/quotes in the final values.

---

## Test Case 3: External-System Replacements — GCP Secret Manager Integration

### Purpose
To verify that database credentials are dynamically and securely retrieved from GCP Secret Manager when available, and that the system falls back gracefully to the legacy placeholder when the API is unavailable.

### Setup
*   **Scenario A**: GCP Secret Manager is available and contains the secret.
*   **Scenario B**: GCP Secret Manager is unavailable or throws an exception.

### Action
Execute unit tests using `unittest.mock` to simulate both scenarios.

```python
# test_secret_manager.py
import os
from unittest import mock
import pytest
import dw_db


@mock.patch('helpers.get_secret_from_gsm')
def test_database_config_with_secret_manager(mock_get_secret):
    # Scenario A: Secret Manager returns a valid password
    mock_get_secret.return_value = "SuperSecurePassword123"
    
    db_configs = dw_db.get_database_configs()
    
    assert db_configs['DB_PASSWD_DWH'] == "SuperSecurePassword123"
    assert db_configs['DB_USER_DWH'] == "meyreis"


@mock.patch('helpers.get_secret_from_gsm')
def test_database_config_fallback(mock_get_secret):
    # Scenario B: Secret Manager returns None (fallback to legacy string)
    mock_get_secret.return_value = None
    
    db_configs = dw_db.get_database_configs()
    
    assert db_configs['DB_PASSWD_DWH'] == "<password encrypted with m_password>"
    assert db_configs['DB_USER_DWH'] == "meyreis"
```

### Pass/Fail Criterion
*   **Pass**: 
    *   When Secret Manager is active, the password is set to the retrieved secret.
    *   When Secret Manager fails/is absent, the password falls back to the legacy placeholder without crashing.
*   **Fail**: The module crashes when Secret Manager is unavailable, or fails to apply the retrieved secret when it is available.

---

## Test Case 4: Data-Quality & Schema Assertions — Path Validation & Directory Resolution

### Purpose
To verify that the global validation logic (`dw_global.py`) correctly identifies missing critical paths and that `dw_init.py` correctly resolves `ORACLE_HOME` based on physical directory candidates.

### Setup
*   Mock the filesystem structure to simulate the presence or absence of Oracle installation directories.

### Action
Run assertions against path validation and dynamic directory resolution.

```python
# test_path_validation.py
import os
from unittest import mock
import pytest
import dw_global
import dw_init


def test_verify_required_paths_missing():
    # Clear environment to trigger validation failure
    with mock.patch.dict(os.environ, {}, clear=True):
        required_variables = ["DW_DIR_ROOT", "ORACLE_HOME"]
        # Should return False because variables are not set
        assert dw_global.verify_required_paths(required_variables) is False


def test_verify_required_paths_present():
    # Populate environment to trigger validation success
    test_env = {"DW_DIR_ROOT": "/some/path", "ORACLE_HOME": "/oracle/path"}
    with mock.patch.dict(os.environ, test_env, clear=True):
        required_variables = ["DW_DIR_ROOT", "ORACLE_HOME"]
        # Should return True because all variables are set
        assert dw_global.verify_required_paths(required_variables) is True


@mock.patch('os.path.isdir')
def test_resolve_oracle_home(mock_isdir):
    # Simulate that Oracle 12c is installed
    def isdir_side_effect(path):
        return path == '/appl/local/oracle/12.2.0.1.0'
    
    mock_isdir.side_effect = isdir_side_effect
    
    resolved = dw_init.resolve_oracle_home()
    assert resolved == '/appl/local/oracle/12.2.0.1.0'
```

### Pass/Fail Criterion
*   **Pass**: 
    *   `verify_required_paths` returns `False` when variables are missing and `True` when all are present.
    *   `resolve_oracle_home` correctly identifies the active Oracle directory candidate.
*   **Fail**: Validation returns incorrect boolean flags, or Oracle home resolution fails to identify the correct candidate path.