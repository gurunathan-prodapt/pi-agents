Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Python scripts (`dw_global.py` and `dw_init.py`) are behaviorally equivalent to the legacy KornShell scripts (`.dw_global` and `.dw_init`).

---

# Test Suite: Shared Files Environment Initialization Validation

This test suite validates the environment setup, directory path construction, Oracle Home discovery, localization variable exports, and error handling behaviors.

## Test Case 1: `dw_global.py` Validation Failure Parity
### Purpose
Verify that if any mandatory environment variables are missing, `dw_global.py` prints the exact legacy German error messages to standard output and raises an `EnvironmentError` to halt downstream execution.

### Setup
- Clear the environment of the following variables: `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `ORACLE_HOME`.

### Action
Execute the `main()` function of `dw_global.py` and capture standard output and exceptions.

### Pass/Fail Criterion
- **Pass**: The script raises an `EnvironmentError`. Standard output contains the exact German error strings:
  - `Fehler in .dw_global:`
  - `   Umgebungsvariable DW_DIR_ROOT ist nicht gesetzt !` (and similarly for all other missing variables)
  - `Breche ab ..`
- **Fail**: The script completes without raising an error, or the printed error messages do not match the legacy German output character-for-character.

---

## Test Case 2: `dw_global.py` Success Path and Environment Modification
### Purpose
Verify that when all required environment variables are present, `dw_global.py` successfully updates `LD_LIBRARY_PATH`, `PATH`, and sets the correct Oracle NLS localization variables.

### Setup
- Set the following environment variables:
  - `DW_DIR_ROOT = "/mock/root"`
  - `DW_DIR_PROT = "/mock/prot"`
  - `DW_DIR_CUBES = "/mock/cubes"`
  - `DW_DIR_IMP_D1 = "/mock/d1"`
  - `DW_DIR_IMP_XTRA = "/mock/xtra"`
  - `DW_DIR_IMP_CTEL = "/mock/ctel"`
  - `ORACLE_HOME = "/appl/local/oracle/8.1.6"`
  - `LD_LIBRARY_PATH = "/usr/lib"`
  - `PATH = "/usr/bin"`

### Action
Execute the `main()` function of `dw_global.py`.

### Pass/Fail Criterion
- **Pass**: 
  - The script exits with code `0` (or returns `0` / None without raising an exception).
  - `os.environ["LD_LIBRARY_PATH"]` is updated to `/appl/local/oracle/8.1.6/lib:/usr/lib`.
  - `os.environ["PATH"]` is updated to `/usr/bin:/appl/local/oracle/8.1.6/bin:`.
  - `os.environ["NLS_LANG"]` is set to `GERMAN_GERMANY.WE8ISO8859P1`.
  - `os.environ["NLS_DATE_FORMAT"]` is set to `DD-MON-YY`.
  - `os.environ["NLS_DATE_LANGUAGE"]` is set to `AMERICAN`.
- **Fail**: Any of the environment variables are missing, incorrectly formatted, or the script raises an unexpected error.

---

## Test Case 3: `dw_init.py` Path Generation & Typo Correction
### Purpose
Verify that `dw_init.py` correctly constructs all `DW_DIR_*` paths relative to the `HOME` directory, and verify that the legacy typo for `DW_DIR_IMP_MP_ZM` is corrected (properly assigned and exported).

### Setup
- Set `HOME = "/home/testuser"`.
- Set `ORACLE_HOME = "/appl/local/oracle/8.1.6"` (to bypass Oracle discovery logic).

### Action
Execute the `main()` function of `dw_init.py`.

### Pass/Fail Criterion
- **Pass**:
  - `os.environ["DW_DIR_ROOT"]` is set to `/home/testuser/aktuell`.
  - `os.environ["DW_DIR_PROT"]` is set to `/home/testuser/daten/logfiles`.
  - `os.environ["DW_DIR_CUBES"]` is set to `/home/testuser/daten/cubes`.
  - `os.environ["DW_DIR_IMP_MP_ZM"]` is set to `/home/testuser/daten/mp/zm` (verifying the legacy typo correction).
  - `os.environ["GEN_HOME"]` is set to `/home/testuser/aktuell/generator`.
  - `os.environ["DW_DIR_CUSTOMER"]` is set to `<login>`.
  - `os.environ["DW_HOST_CUSTOMER"]` is set to `dxcst3.bn.detemobil.de`.
- **Fail**: Any path is incorrectly constructed or missing from `os.environ`.

---

## Test Case 4: `dw_init.py` Oracle Home Discovery
### Purpose
Verify that if `ORACLE_HOME` is unset, `dw_init.py` sequentially checks the filesystem and resolves to the correct path (including the legacy mapping of `oracle.8.1.6` to `8.1.6`).

### Setup
- Clear `ORACLE_HOME` from the environment.
- Mock `os.path.isdir` to return `True` only when checking `/appl/local/oracle/oracle.8.1.6`.

### Action
Execute the `main()` function of `dw_init.py`.

### Pass/Fail Criterion
- **Pass**: `os.environ["ORACLE_HOME"]` is resolved and set to `/appl/local/oracle/8.1.6`.
- **Fail**: `os.environ["ORACLE_HOME"]` is unset, resolved to the unmapped directory, or the script exits with an error.

---

## Test Case 5: `dw_init.py` Oracle Home Discovery Failure
### Purpose
Verify that if `ORACLE_HOME` is unset and no valid Oracle directories exist on the filesystem, `dw_init.py` prints the exact legacy German error messages to standard error and exits with code `1`.

### Setup
- Clear `ORACLE_HOME` from the environment.
- Mock `os.path.isdir` to return `False` for all paths.

### Action
Execute the `main()` function of `dw_init.py` and capture standard error and exit codes.

### Pass/Fail Criterion
- **Pass**:
  - The script exits with code `1` (raises `SystemExit` with code `1`).
  - Standard error contains the exact strings:
    - `Fehler in .dw_init:`
    - `   Konnte ORACLE_HOME nicht setzen !`
    - `Breche ab ..`
- **Fail**: The script does not exit, exits with a code other than `1`, or prints mismatched error messages.

---

## Test Case 6: `umask` Application
### Purpose
Verify that `dw_init.py` correctly applies the process-level file creation mask (`umask 022`).

### Setup
- Mock `os.umask` to track calls.

### Action
Execute the `main()` function of `dw_init.py` (with `ORACLE_HOME` pre-set to avoid discovery failure).

### Pass/Fail Criterion
- **Pass**: `os.umask` is called with the octal value `0o022` (decimal `18`).
- **Fail**: `os.umask` is not called, or is called with an incorrect mask value.

---

# Runnable Validation Code (Pytest)

The following `pytest` suite implements all the validation test cases described above.

```python
import os
import sys
import pytest
from unittest.mock import patch

# Import the migrated modules
# Adjust import paths as necessary depending on your project structure
import dw_global
import dw_init


# ==============================================================================
# TESTS FOR dw_global.py
# ==============================================================================

def test_dw_global_validation_failure(monkeypatch, capsys):
    """Test Case 1: Verify dw_global.py fails with exact German messages when env vars are missing."""
    # Clear all required variables
    required_vars = [
        "DW_DIR_ROOT", "DW_DIR_PROT", "DW_DIR_CUBES", 
        "DW_DIR_IMP_D1", "DW_DIR_IMP_XTRA", "DW_DIR_IMP_CTEL", "ORACLE_HOME"
    ]
    for var in required_vars:
        monkeypatch.delenv(var, raising=False)

    with pytest.raises(EnvironmentError) as exc_info:
        dw_global.main()

    assert "Missing required environment variables" in str(exc_info.value)

    captured = capsys.readouterr()
    stdout_lines = captured.out.splitlines()

    # Verify exact German error messages
    assert "Fehler in .dw_global:" in stdout_lines
    for var in required_vars:
        assert f"   Umgebungsvariable {var} ist nicht gesetzt !" in stdout_lines
    assert "Breche ab .." in stdout_lines


def test_dw_global_success_path(monkeypatch):
    """Test Case 2: Verify dw_global.py success path and environment modifications."""
    monkeypatch.setenv("DW_DIR_ROOT", "/mock/root")
    monkeypatch.setenv("DW_DIR_PROT", "/mock/prot")
    monkeypatch.setenv("DW_DIR_CUBES", "/mock/cubes")
    monkeypatch.setenv("DW_DIR_IMP_D1", "/mock/d1")
    monkeypatch.setenv("DW_DIR_IMP_XTRA", "/mock/xtra")
    monkeypatch.setenv("DW_DIR_IMP_CTEL", "/mock/ctel")
    monkeypatch.setenv("ORACLE_HOME", "/appl/local/oracle/8.1.6")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/usr/lib")
    monkeypatch.setenv("PATH", "/usr/bin")

    exit_code = dw_global.main()
    assert exit_code == 0 or exit_code is None

    # Verify path modifications
    assert os.environ["LD_LIBRARY_PATH"] == "/appl/local/oracle/8.1.6/lib:/usr/lib"
    assert os.environ["PATH"] == "/usr/bin:/appl/local/oracle/8.1.6/bin:"

    # Verify NLS settings
    assert os.environ["NLS_LANG"] == "GERMAN_GERMANY.WE8ISO8859P1"
    assert os.environ["NLS_DATE_FORMAT"] == "DD-MON-YY"
    assert os.environ["NLS_DATE_LANGUAGE"] == "AMERICAN"


# ==============================================================================
# TESTS FOR dw_init.py
# ==============================================================================

def test_dw_init_path_generation(monkeypatch):
    """Test Case 3: Verify dw_init.py constructs all paths relative to HOME."""
    monkeypatch.setenv("HOME", "/home/testuser")
    monkeypatch.setenv("ORACLE_HOME", "/appl/local/oracle/8.1.6")  # Bypass discovery

    # Mock umask to avoid modifying test runner process state
    with patch("os.umask") as mock_umask:
        dw_init.main()
        mock_umask.assert_called_once_with(0o022)

    assert os.environ["DW_DIR_ROOT"] == "/home/testuser/aktuell"
    assert os.environ["DW_DIR_PROT"] == "/home/testuser/daten/logfiles"
    assert os.environ["DW_DIR_CUBES"] == "/home/testuser/daten/cubes"
    assert os.environ["DW_DIR_IMP_D1"] == "/home/testuser/daten/d1"
    
    # Verify legacy typo correction (DW_DIR_IMP_MP_ZM)
    assert os.environ["DW_DIR_IMP_MP_ZM"] == "/home/testuser/daten/mp/zm"
    
    assert os.environ["GEN_HOME"] == "/home/testuser/aktuell/generator"
    assert os.environ["DW_DIR_CUSTOMER"] == "<login>"
    assert os.environ["DW_HOST_CUSTOMER"] == "dxcst3.bn.detemobil.de"


@patch("os.path.isdir")
def test_dw_init_oracle_discovery_success(mock_isdir, monkeypatch):
    """Test Case 4: Verify sequential discovery and mapping of ORACLE_HOME."""
    monkeypatch.setenv("HOME", "/home/testuser")
    monkeypatch.delenv("ORACLE_HOME", raising=False)

    # Simulate that only the oracle.8.1.6 directory exists
    def side_effect(path):
        return path == "/appl/local/oracle/oracle.8.1.6"
    mock_isdir.side_effect = side_effect

    with patch("os.umask"):
        dw_init.main()

    # Verify mapped path
    assert os.environ["ORACLE_HOME"] == "/appl/local/oracle/8.1.6"


@patch("os.path.isdir")
def test_dw_init_oracle_discovery_failure(mock_isdir, monkeypatch, capsys):
    """Test Case 5: Verify dw_init.py exits and prints German errors when ORACLE_HOME cannot be resolved."""
    monkeypatch.setenv("HOME", "/home/testuser")
    monkeypatch.delenv("ORACLE_HOME", raising=False)

    # Simulate no oracle directories exist
    mock_isdir.return_value = False

    with pytest.raises(SystemExit) as exc_info:
        dw_init.main()

    assert exc_info.value.code == 1

    captured = capsys.readouterr()
    stderr_lines = captured.err.splitlines()

    # Verify exact German error messages on stderr
    assert "Fehler in .dw_init:" in stderr_lines
    assert "   Konnte ORACLE_HOME nicht setzen !" in stderr_lines
    assert "Breche ab .." in stderr_lines
```