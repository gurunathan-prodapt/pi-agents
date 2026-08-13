Here is a comprehensive migration-validation test suite designed to verify that the migrated Python modules (`dw_global.py` and `dw_init.py`) are behaviorally equivalent to the legacy Korn Shell scripts (`.dw_global` and `.dw_init`).

---

# Migration Validation Test Suite: Shared Files (`dw_global` & `dw_init`)

## Test Case 1: Environment Validation Failure (`dw_global.py`)
### Purpose
Verify that if any of the seven critical environment variables are missing or empty, `dw_global.py` aborts execution, returns a non-zero exit code, and prints the exact legacy German error messages to `stderr`.

### Setup
*   An execution environment where one or more of the required variables (`DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `ORACLE_HOME`) are unset.

### Action
Run the `dw_global.py` script or call its `main()` function with an incomplete environment.

### Pass/Fail Criterion
*   **Pass**: The script raises an `EnvironmentError` (or exits with status code `1` when run as a script). `stderr` contains the exact German error strings:
    *   `"Fehler in .dw_global:"`
    *   `"   Umgebungsvariable <VARIABLE_NAME> ist nicht gesetzt !"`
    *   `"Breche ab .."`
*   **Fail**: The script exits with code `0`, does not print the error messages, or fails to identify all missing variables.

---

## Test Case 2: Environment Validation Success & Path/Locale Modification (`dw_global.py`)
### Purpose
Verify that when all required environment variables are present, `dw_global.py` successfully appends/prepends paths to `PATH` and `LD_LIBRARY_PATH`, and exports the correct Oracle NLS localization parameters.

### Setup
*   Set all required environment variables:
    *   `DW_DIR_ROOT=/tmp/dw_root`
    *   `DW_DIR_PROT=/tmp/dw_prot`
    *   `DW_DIR_CUBES=/tmp/dw_cubes`
    *   `DW_DIR_IMP_D1=/tmp/dw_d1`
    *   `DW_DIR_IMP_XTRA=/tmp/dw_xtra`
    *   `DW_DIR_IMP_CTEL=/tmp/dw_ctel`
    *   `ORACLE_HOME=/opt/oracle/product/8.1.6`
*   Set initial values for `PATH` and `LD_LIBRARY_PATH`.

### Action
Execute `dw_global.main()`.

### Pass/Fail Criterion
*   **Pass**: 
    *   `LD_LIBRARY_PATH` starts with `/opt/oracle/product/8.1.6/lib:`.
    *   `PATH` ends with `:/opt/oracle/product/8.1.6/bin:`.
    *   `NLS_LANG` is set to `GERMAN_GERMANY.WE8ISO8859P1`.
    *   `NLS_DATE_FORMAT` is set to `DD-MON-YY`.
    *   `NLS_DATE_LANGUAGE` is set to `AMERICAN`.
*   **Fail**: Any of the environment variables are missing, incorrectly formatted, or the script raises an unexpected error.

---

## Test Case 3: Dynamic `ORACLE_HOME` Resolution (`dw_init.py`)
### Purpose
Verify that if `ORACLE_HOME` is not pre-set, `dw_init.py` dynamically checks the filesystem and assigns the correct legacy Oracle path, or fails gracefully with the legacy German error message if no valid directories exist.

### Setup
*   Unset `ORACLE_HOME` from the environment.
*   Mock the filesystem directory checks (`os.path.isdir`) to simulate different Oracle installation states.

### Action
1.  **Scenario A**: Mock `/appl/local/oracle/oracle.8.1.6` as existing. Run `dw_init.init_env()`.
2.  **Scenario B**: Mock all candidate directories as non-existent. Run `dw_init.init_env()`.

### Pass/Fail Criterion
*   **Pass**:
    *   In **Scenario A**: `os.environ["ORACLE_HOME"]` is resolved to `/appl/local/oracle/8.1.6`.
    *   In **Scenario B**: The script prints `"Fehler in .dw_init:"`, `"   Konnte ORACLE_HOME nicht setzen !"`, and `"Breche ab .."` to `stderr` and exits with code `1`.
*   **Fail**: The script resolves to an incorrect path, fails to exit on missing directories, or prints incorrect log messages.

---

## Test Case 4: Directory Path Initialization & Legacy Bug Fix (`dw_init.py`)
### Purpose
Verify that all `DW_DIR_*` variables are correctly initialized relative to the `$HOME` directory, and that the legacy copy-paste bug for `DW_DIR_IMP_MP_ZM` is resolved (ensuring it is correctly exported instead of duplicating `DW_DIR_IMP_MP_TS`).

### Setup
*   Set `HOME=/home/dbuser`.
*   Ensure `ORACLE_HOME` is set to bypass resolution checks.

### Action
Execute `dw_init.init_env()`.

### Pass/Fail Criterion
*   **Pass**:
    *   `DW_DIR_ROOT` is set to `/home/dbuser/aktuell`.
    *   `DW_DIR_PROT` is set to `/home/dbuser/daten/logfiles`.
    *   `DW_DIR_IMP_MP_ZM` is set to `/home/dbuser/daten/mp/zm`.
    *   `DW_DIR_IMP_MP_TS` is set to `/home/dbuser/daten/mp/ts`.
    *   All other 15+ import directories match their respective `$HOME/daten/...` paths.
*   **Fail**: Any path is incorrectly constructed, or `DW_DIR_IMP_MP_ZM` is missing/incorrectly mapped.

---

## Test Case 5: Process Umask Application (`dw_init.py`)
### Purpose
Verify that executing `dw_init.py` correctly sets the process-level file creation mask (`umask`) to `022` (octal `0o022`), matching the legacy KSH behavior.

### Setup
*   Set the current process umask to a different value (e.g., `0o077`).

### Action
Execute `dw_init.init_env()`.

### Pass/Fail Criterion
*   **Pass**: The process umask is changed to `0o022`. (Verified by calling `os.umask(0o022)` and asserting that the returned previous umask was indeed `0o022`).
*   **Fail**: The umask remains unchanged or is set to an incorrect value.

---

# Runnable Pytest Validation Suite

Save the following code as `test_migration_env.py`. It uses `pytest` and standard mocking libraries to validate all five test cases against the migrated Python files.

```python
import os
import sys
import pathlib
import pytest
from unittest import mock

# Ensure the migration directory is in the python path
sys.path.append(str(pathlib.Path(__file__).parent))

import dw_global
import dw_init


@pytest.fixture
def clean_env():
    """Provides a clean environment for testing, restoring it after each test."""
    old_env = os.environ.copy()
    # Clear variables under test
    vars_to_clear = [
        "DW_DIR_ROOT", "DW_DIR_PROT", "DW_DIR_CUBES", "DW_DIR_IMP_D1",
        "DW_DIR_IMP_XTRA", "DW_DIR_IMP_CTEL", "DW_DIR_IMP_VO", "DW_DIR_IMP_RV",
        "DW_DIR_IMP_TRF", "DW_DIR_IMP_TS", "DW_DIR_IMP_ZM", "DW_DIR_IMP_AUF",
        "DW_DIR_IMP_GUT", "DW_DIR_IMP_KDG", "DW_DIR_IMP_MP_TS", "DW_DIR_IMP_MP_KDG",
        "DW_DIR_IMP_MP_ZM", "DW_DIR_IMP_IF", "DW_DIR_IMP_NNV", "DW_DIR_IMP_CARMEN",
        "GEN_HOME", "DW_DIR_CUSTOMER", "DW_HOST_CUSTOMER", "ORACLE_HOME",
        "LD_LIBRARY_PATH", "NLS_LANG", "NLS_DATE_FORMAT", "NLS_DATE_LANGUAGE"
    ]
    for var in vars_to_clear:
        os.environ.pop(var, None)
    
    os.environ["HOME"] = "/home/testuser"
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(old_env)


def test_dw_global_validation_failure(clean_env, capsys):
    """Test Case 1: Verify validation failure when required variables are missing."""
    # Only set one variable, leaving others missing
    os.environ["DW_DIR_ROOT"] = "/home/testuser/aktuell"

    with pytest.raises(EnvironmentError) as exc_info:
        dw_global.main()

    assert "Required environment variables are not set" in str(exc_info.value)
    
    # Capture stderr and verify German error messages
    captured = capsys.readouterr()
    assert "Fehler in .dw_global:" in captured.err
    assert "   Umgebungsvariable ORACLE_HOME ist nicht gesetzt !" in captured.err
    assert "Breche ab .." in captured.err


def test_dw_global_validation_success(clean_env):
    """Test Case 2: Verify path and locale exports on successful validation."""
    # Populate all required variables
    os.environ["DW_DIR_ROOT"] = "/home/testuser/aktuell"
    os.environ["DW_DIR_PROT"] = "/home/testuser/daten/logfiles"
    os.environ["DW_DIR_CUBES"] = "/home/testuser/daten/cubes"
    os.environ["DW_DIR_IMP_D1"] = "/home/testuser/daten/d1"
    os.environ["DW_DIR_IMP_XTRA"] = "/home/testuser/daten/xtra"
    os.environ["DW_DIR_IMP_CTEL"] = "/home/testuser/daten/ctel"
    os.environ["ORACLE_HOME"] = "/opt/oracle/product/8.1.6"
    os.environ["LD_LIBRARY_PATH"] = "/usr/lib"
    os.environ["PATH"] = "/usr/bin"

    dw_global.main()

    # Assert path modifications
    assert os.environ["LD_LIBRARY_PATH"] == "/opt/oracle/product/8.1.6/lib:/usr/lib"
    assert os.environ["PATH"] == "/usr/bin:/opt/oracle/product/8.1.6/bin:"

    # Assert NLS Locale settings
    assert os.environ["NLS_LANG"] == "GERMAN_GERMANY.WE8ISO8859P1"
    assert os.environ["NLS_DATE_FORMAT"] == "DD-MON-YY"
    assert os.environ["NLS_DATE_LANGUAGE"] == "AMERICAN"


@mock.patch("os.path.isdir")
def test_dw_init_oracle_home_resolution_success(mock_isdir, clean_env):
    """Test Case 3 (Success): Verify dynamic resolution of ORACLE_HOME."""
    # Mock directory checks so that 8.1.6 is found
    def side_effect(path):
        return path == "/appl/local/oracle/oracle.8.1.6"
    mock_isdir.side_effect = side_effect

    # Set up other variables to allow dw_global to pass
    os.environ["DW_DIR_CUSTOMER"] = "test_customer"
    
    # We call init_env directly to test the logic
    dw_init.init_env()

    assert os.environ["ORACLE_HOME"] == "/appl/local/oracle/8.1.6"


@mock.patch("os.path.isdir")
def test_dw_init_oracle_home_resolution_failure(mock_isdir, clean_env, capsys):
    """Test Case 3 (Failure): Verify abort when no ORACLE_HOME can be resolved."""
    # Mock all directories as non-existent
    mock_isdir.return_value = False

    with pytest.raises(SystemExit) as exc_info:
        dw_init.init_env()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Fehler in .dw_init:" in captured.err
    assert "   Konnte ORACLE_HOME nicht setzen !" in captured.err
    assert "Breche ab .." in captured.err


def test_dw_init_paths_and_bug_fix(clean_env):
    """Test Case 4: Verify directory paths and legacy copy-paste bug fix."""
    os.environ["ORACLE_HOME"] = "/opt/oracle/product/8.1.6"
    os.environ["DW_DIR_CUSTOMER"] = "test_customer"

    dw_init.init_env()

    # Verify standard paths
    assert os.environ["DW_DIR_ROOT"] == "/home/testuser/aktuell"
    assert os.environ["DW_DIR_PROT"] == "/home/testuser/daten/logfiles"
    assert os.environ["GEN_HOME"] == "/home/testuser/aktuell/generator"
    assert os.environ["DW_HOST_CUSTOMER"] == "dxcst3.bn.detemobil.de"

    # Verify bug fix: DW_DIR_IMP_MP_ZM must be set and distinct from DW_DIR_IMP_MP_TS
    assert os.environ["DW_DIR_IMP_MP_ZM"] == "/home/testuser/daten/mp/zm"
    assert os.environ["DW_DIR_IMP_MP_TS"] == "/home/testuser/daten/mp/ts"


def test_dw_init_umask(clean_env):
    """Test Case 5: Verify that umask is set to 022 (octal)."""
    os.environ["ORACLE_HOME"] = "/opt/oracle/product/8.1.6"
    os.environ["DW_DIR_CUSTOMER"] = "test_customer"
    
    # Set process umask to something else first
    os.umask(0o077)

    dw_init.init_env()

    # os.umask(mask) sets the mask and returns the PREVIOUS mask.
    # We use this to verify that init_env successfully set it to 0o022.
    previous_mask = os.umask(0o022)
    assert previous_mask == 0o022
```