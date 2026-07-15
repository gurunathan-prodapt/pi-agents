Here is the comprehensive migration-validation test suite for the **Shared Files** configuration job. 

Since this job falls under the `UC4_ONLY` pattern (Environment Orchestration & Variable Injection), the validation strategy focuses on **state-mutation parity** (ensuring that executing the migrated Python modules results in the exact same environment variable side-effects as sourcing the legacy KornShell scripts).

---

# Test Suite Overview: Environment Variable Injection Parity

To run these tests, we use a `pytest` harness that compares the environment state before and after executing both the legacy shell scripts and the migrated Python scripts.

### Test Directory Structure
```text
tests/
├── conftest.py
└── test_dw_files_migration.py
```

### Shared Test Harness (`tests/conftest.py`)
This helper module provides utilities to capture environment mutations from both shell scripts and Python modules.

```python
# tests/conftest.py
import os
import sys
import pytest
import subprocess
import import_module_from_path  # Helper or standard importlib

def run_shell_and_capture_env(script_path, initial_env=None):
    """
    Executes a shell script in a subprocess and captures the resulting environment.
    """
    env = initial_env.copy() if initial_env else os.environ.copy()
    # Source the script and print the environment variables
    cmd = f"ksh -c '. {script_path} && env'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Shell script failed: {result.stderr}")
    
    captured_env = {}
    for line in result.stdout.splitlines():
        if '=' in line:
            key, _, value = line.partition('=')
            captured_env[key] = value
    return captured_env

def run_python_and_capture_env(module_path, initial_env=None):
    """
    Executes the migrated Python main() function in a clean process context 
    and returns the mutated os.environ.
    """
    import importlib.util
    
    # Backup environment
    old_env = os.environ.copy()
    if initial_env:
        os.environ.clear()
        os.environ.update(initial_env)
        
    try:
        spec = importlib.util.spec_from_file_location("migrated_module", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        # Execute the entry point
        module.main()
        mutated_env = os.environ.copy()
    finally:
        # Restore environment
        os.environ.clear()
        os.environ.update(old_env)
        
    return mutated_env
```

---

## Test Case 1: Output Parity — `.dw_ai` vs `dw_ai.py`

### Purpose
Verify that executing `dw_ai.py` injects the exact same Ab Initio framework variables into the environment as sourcing the legacy `.dw_ai` script.

### Setup
*   **Legacy File:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai`
*   **Migrated File:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_ai.py`
*   **Initial Environment:** Set `HOME="/home/testuser"` and `PATH="/usr/bin:/bin"`.

### Action
Execute both scripts in isolated environments and extract the target variables.

### Pass/Fail Criterion
*   **Pass:** The values of `AB_HOME`, `AB_AIR_ROOT`, `AB_AIR_HOME`, `ETL_Host`, `ETL_Projekt`, `AI_PRIV_SAND_ROOT`, `AI_ENV_SAND_ROOT`, and the appended `PATH` match exactly between the legacy and migrated runs.
*   **Fail:** Any variable is missing, or its value differs.

### Test Code
```python
# tests/test_dw_files_migration.py
import os
import pytest
from conftest import run_shell_and_capture_env, run_python_and_capture_env

def test_dw_ai_parity():
    initial_env = {
        "HOME": "/home/testuser",
        "PATH": "/usr/bin:/bin"
    }
    
    legacy_script = "vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai"
    migrated_script = "vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_ai.py"
    
    legacy_env = run_shell_and_capture_env(legacy_script, initial_env)
    migrated_env = run_python_and_capture_env(migrated_script, initial_env)
    
    assert migrated_env["AB_HOME"] == legacy_env["AB_HOME"] == "/appl/local/abinitio/abinitio"
    assert migrated_env["AB_AIR_ROOT"] == legacy_env["AB_AIR_ROOT"] == "/appl/local/abinitio/TMD_EME/eme_dev/repo"
    assert migrated_env["AB_AIR_HOME"] == "/appl/local/abinitio/abinitio-V2-14"
    assert migrated_env["ETL_Host"] == legacy_env["ETL_Host"] == "dxcsa4.bn.detemobil.de"
    assert migrated_env["ETL_Projekt"] == legacy_env["ETL_Projekt"] == "BHB"
    assert migrated_env["AI_PRIV_SAND_ROOT"] == legacy_env["AI_PRIV_SAND_ROOT"] == "/home/testuser/abinitio"
    assert migrated_env["AI_ENV_SAND_ROOT"] == legacy_env["AI_ENV_SAND_ROOT"] == "/appl/local/abinitio/sandboxes/DEV"
    
    # Verify PATH append logic
    assert migrated_env["PATH"] == legacy_env["PATH"] == "/usr/bin:/bin.:/appl/local/abinitio/abinitio/bin"
```

---

## Test Case 2: Transformation Correctness — `.dw_db` vs `dw_db.py`

### Purpose
Verify that database connection parameters and character set encodings (`NLS_LANG`) are correctly initialized and exported.

### Setup
*   **Legacy File:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db`
*   **Migrated File:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_db.py`

### Action
Execute both scripts and assert the presence and value of Oracle connection variables.

### Pass/Fail Criterion
*   **Pass:** `NLS_LANG`, `DB_TNS_NAME_DWH`, `DB_USER_DWH`, and `DB_PASSWD_DWH` are set identically.
*   **Fail:** Any database variable is missing or mismatched.

### Test Code
```python
def test_dw_db_parity():
    legacy_script = "vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db"
    migrated_script = "vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_db.py"
    
    legacy_env = run_shell_and_capture_env(legacy_script)
    migrated_env = run_python_and_capture_env(migrated_script)
    
    assert migrated_env["NLS_LANG"] == legacy_env["NLS_LANG"] == "GERMAN_GERMANY.WE8ISO8859P1"
    assert migrated_env["DB_TNS_NAME_DWH"] == legacy_env["DB_TNS_NAME_DWH"] == "@eDWH3.devlab.de.tmo"
    assert migrated_env["DB_USER_DWH"] == legacy_env["DB_USER_DWH"] == "meyreis"
    assert migrated_env["DB_PASSWD_DWH"] == legacy_env["DB_PASSWD_DWH"] == "<password encrypted with m_password>"
```

---

## Test Case 3: Validation & Error Handling — `.dw_global` vs `dw_global.py`

### Purpose
Verify that `dw_global.py` correctly replicates the validation logic of `.dw_global` when required variables are missing, and that it does not crash (matching legacy shell behavior).

### Setup
*   **Legacy File:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global`
*   **Migrated File:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_global.py`
*   **Initial Environment:** An empty environment dictionary (triggering validation failures).

### Action
Execute both scripts with missing environment variables and capture `stdout`.

### Pass/Fail Criterion
*   **Pass:** Both scripts print validation warnings listing the missing variables (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`) and exit with status code `0` (non-blocking validation).
*   **Fail:** The Python script exits with a non-zero code, or fails to print the warning block.

### Test Code
```python
def test_dw_global_validation_warnings(capsys):
    migrated_script = "vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_global.py"
    
    # Run with empty environment to trigger validation warnings
    empty_env = {}
    
    # Execute Python script directly to capture stdout
    import importlib.util
    spec = importlib.util.spec_from_file_location("dw_global", migrated_script)
    dw_global = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dw_global)
    
    # Run validation
    dw_global.main()
    captured = capsys.readouterr()
    
    # Assert warning header and missing variables are printed
    assert "Fehler in .dw_global:" in captured.out
    assert "Umgebungsvariable DW_DIR_ROOT ist nicht gesetzt !" in captured.out
    assert "Umgebungsvariable ORACLE_HOME ist nicht gesetzt !" in captured.out
```

---

## Test Case 4: External-System Replacements — Sourcing `setpya.sh`

### Purpose
Verify that the migrated Python script handles the external dependency `/appl/local/cognos/pya60207/setpya.sh` correctly. If the file exists, it must be sourced via subprocess; if it does not exist, it must fail gracefully without crashing.

### Setup
*   **Scenario A:** File `/appl/local/cognos/pya60207/setpya.sh` does not exist.
*   **Scenario B:** File `/appl/local/cognos/pya60207/setpya.sh` exists and exports `COGNOS_VAR="active"`.

### Action
Execute `dw_global.py` under both scenarios.

### Pass/Fail Criterion
*   **Pass:** 
    *   In Scenario A, the script runs successfully and logs a warning or skips sourcing.
    *   In Scenario B, the environment variable `COGNOS_VAR` is successfully imported into Python's `os.environ`.
*   **Fail:** The script throws an unhandled exception or fails to capture the exported variable.

### Test Code
```python
def test_dw_global_external_sourcing(tmp_path, monkeypatch):
    migrated_script = "vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_global.py"
    
    # Mock the path to point to a temporary test script
    fake_setpya = tmp_path / "setpya.sh"
    fake_setpya.write_text("export COGNOS_VAR='active'\n")
    
    # Patch the path inside the script
    import importlib.util
    spec = importlib.util.spec_from_file_location("dw_global", migrated_script)
    dw_global = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dw_global)
    
    # Override the hardcoded path with our fake path for testing
    monkeypatch.setattr(dw_global, "cognos_setpya_path", str(fake_setpya))
    
    # Run
    dw_global.main()
    
    # Assert variable was successfully captured and injected
    assert os.environ.get("COGNOS_VAR") == "active"
```

---

## Test Case 5: Complex Initialization & Path Resolution — `.dw_init` vs `dw_init.py`

### Purpose
Verify that `dw_init.py` correctly resolves the `$HOME` directory, maps all 40+ downstream directory variables, dynamically resolves `ORACLE_HOME` based on directory existence, and sets `DW_DIR_UTL_FILE` using the active `ORACLE_SID`.

### Setup
*   **Legacy File:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init`
*   **Migrated File:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_init.py`
*   **Mocks:** Create mock directories for `/appl/local/oracle/12.2.0.1.0` to test dynamic path resolution.

### Action
Execute both scripts with `ORACLE_SID="TEST_SID"` and `HOME="/home/testuser"`.

### Pass/Fail Criterion
*   **Pass:**
    *   `DW_DIR_UTL_FILE` is set to `/appl/local/oracle/admin/TEST_SID/utl_file`.
    *   `ORACLE_HOME` is resolved to `/appl/local/oracle/12.2.0.1.0`.
    *   All directory paths (e.g., `DW_DIR_IMP_SAP_L`, `DW_DIR_IMP_SUBSE`) are correctly prefixed with `/home/testuser`.
*   **Fail:** Any path is incorrectly resolved or mismatched.

### Test Code
```python
def test_dw_init_complex_resolution(tmp_path, monkeypatch):
    legacy_script = "vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init"
    migrated_script = "vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_init.py"
    
    # Mock Oracle directory structure
    fake_oracle_dir = tmp_path / "appl/local/oracle/12.2.0.1.0"
    fake_oracle_dir.mkdir(parents=True, exist_ok=True)
    
    initial_env = {
        "HOME": "/home/testuser",
        "ORACLE_SID": "PROD_SID",
        "ORACLE_HOME": ""  # Force dynamic resolution
    }
    
    # Patch os.path.isdir to return True for our fake oracle path
    orig_isdir = os.path.isdir
    def mock_isdir(path):
        if path == '/appl/local/oracle/12.2.0.1.0':
            return True
        return orig_isdir(path)
    
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    
    # Execute migrated script
    migrated_env = run_python_and_capture_env(migrated_script, initial_env)
    
    # Assertions
    assert migrated_env["DW_DIR_ROOT"] == "/home/testuser/aktuell"
    assert migrated_env["DW_DIR_PROT"] == "/home/testuser/daten/logfiles"
    assert migrated_env["DW_DIR_IMP_SAP_L"] == "/home/testuser/daten/sap/sap_l_gutgr"
    assert migrated_env["DW_DIR_IMP_SUBSE"] == "/home/testuser/daten/subse"
    assert migrated_env["DW_DIR_SMS_PRG"] == "/home/testuser/aktuell/allgemein/is/util"
    assert migrated_env["ORACLE_HOME"] == "/appl/local/oracle/12.2.0.1.0"
    assert migrated_env["DW_DIR_UTL_FILE"] == "/appl/local/oracle/admin/PROD_SID/utl_file"
```