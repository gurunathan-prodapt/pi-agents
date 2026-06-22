As a senior data-migration QA engineer, I have reviewed the migration design and the generated Python code for `vobs/dw_source/istools/seu/template/.dw_init`. The script's primary function is environment setup, not data transformation, so the tests will focus on verifying the correct setting of environment variables and handling of conditional logic.

The following test cases are designed to prove behavioral equivalence, covering output parity, transformation correctness, external system interactions (filesystem checks for Oracle), and assertions about the resulting environment.

---

## Migration Validation Tests for `dw_init_environment.py`

### Test Setup Prerequisites

To run these tests, you will need:
*   A Linux/Unix-like environment with `bash` or `ksh` available.
*   Python 3.x installed.
*   `pytest` installed (`pip install pytest`).
*   The legacy KornShell script (`.dw_init`) and the migrated Python script (`dw_init_environment.py`) in accessible paths.
*   A temporary directory for creating mock files/directories.

**Helper Functions (Python):**

```python
import os
import subprocess
import tempfile
import shutil
import pytest

# Path to the legacy KSH script
LEGACY_SCRIPT_PATH = "vobs/dw_source/istools/seu/template/.dw_init"
# Path to the migrated Python script
MIGRATED_SCRIPT_PATH = "dw_init_environment.py"

def run_legacy_script(home_dir, initial_env=None, oracle_paths_to_create=None):
    """Runs the legacy KSH script and captures its environment."""
    env = os.environ.copy()
    env['HOME'] = home_dir
    if initial_env:
        env.update(initial_env)

    # Create mock Oracle paths if specified
    if oracle_paths_to_create:
        for path in oracle_paths_to_create:
            os.makedirs(path, exist_ok=True)

    # The legacy script sources other scripts which might not exist.
    # To prevent errors, we can mock them as empty files.
    # This is a simplification; a full test would analyze and mock their content.
    with open(os.path.join(home_dir, '.dw_global'), 'w') as f:
        f.write('')
    with open(os.path.join(home_dir, '.dw_lokal'), 'w') as f:
        f.write('')

    # Run the script in a subshell to capture its exported environment
    # Using 'env -i' to start with a clean environment, then setting HOME and other initial_env
    # and then sourcing the script.
    command = [
        'bash', '-c',
        f'env -i HOME="{home_dir}" {" ".join(f"{k}=\"{v}\"" for k, v in initial_env.items()) if initial_env else ""} '
        f'bash -c "source {LEGACY_SCRIPT_PATH} && env"'
    ]
    
    result = subprocess.run(command, capture_output=True, text=True, env=env)
    
    if result.returncode != 0:
        print(f"Legacy script stderr:\n{result.stderr}")
        print(f"Legacy script stdout:\n{result.stdout}")
        # For ORACLE_HOME exit cases, we expect a non-zero return code.
        # For other cases, it's an error.
        if "Konnte ORACLE_HOME nicht setzen" not in result.stderr:
            raise RuntimeError(f"Legacy script failed with exit code {result.returncode}")

    env_output = result.stdout.strip().split('\n')
    captured_env = {}
    for line in env_output:
        if '=' in line:
            key, value = line.split('=', 1)
            captured_env[key] = value
    return captured_env, result.returncode, result.stderr

def run_migrated_script(home_dir, initial_env=None, oracle_paths_to_create=None):
    """Runs the migrated Python script and captures its environment."""
    env = os.environ.copy()
    env['HOME'] = home_dir
    if initial_env:
        env.update(initial_env)

    # Create mock Oracle paths if specified
    if oracle_paths_to_create:
        for path in oracle_paths_to_create:
            os.makedirs(path, exist_ok=True)

    # Run the Python script as a subprocess
    # The script modifies os.environ of its own process.
    # To capture this, we need to run it and then print the environment.
    command = [
        'python', '-c',
        f'import os; import sys; sys.path.insert(0, "{os.path.dirname(MIGRATED_SCRIPT_PATH)}"); '
        f'from {os.path.basename(MIGRATED_SCRIPT_PATH).replace(".py", "")} import initialize_environment; '
        f'initialize_environment(); '
        f'for k, v in os.environ.items(): print(f"{{k}}={{v}}")'
    ]
    
    result = subprocess.run(command, capture_output=True, text=True, env=env)

    if result.returncode != 0:
        print(f"Migrated script stderr:\n{result.stderr}")
        print(f"Migrated script stdout:\n{result.stdout}")
        # For ORACLE_HOME exit cases, we expect a non-zero return code.
        # For other cases, it's an error.
        if "ERROR: Could not set ORACLE_HOME" not in result.stderr:
            raise RuntimeError(f"Migrated script failed with exit code {result.returncode}")

    env_output = result.stdout.strip().split('\n')
    captured_env = {}
    for line in env_output:
        if '=' in line:
            key, value = line.split('=', 1)
            captured_env[key] = value
    return captured_env, result.returncode, result.stderr

# Fixture for temporary directory
@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

# Fixture for creating mock Oracle paths
@pytest.fixture
def mock_oracle_paths(temp_dir):
    # Map original paths to paths within the temp_dir
    original_oracle_paths = [
        "/appl/local/oracle/oracle.8.1.6",
        "/appl/local/oracle/7.3.4",
        "/appl/local/oracle/oracle.7.3.3",
        "/appl/local/oracle/7.3.2",
        "/appl/local/oracle/7.2.3",
    ]
    mocked_paths = {
        original: os.path.join(temp_dir, original.lstrip('/'))
        for original in original_oracle_paths
    }
    yield mocked_paths
    # Cleanup is handled by temp_dir fixture
```

---

### Test Case 1: Direct Environment Variable Assignments (Output Parity)

*   **Purpose:** To verify that all `DW_DIR_*` variables, `GEN_HOME`, and `DW_HOST_CUSTOMER` are correctly derived from the `HOME` environment variable and set in the Python process's environment, matching the legacy script's output. This covers output parity and transformation correctness for direct assignments.
*   **Setup:**
    1.  Create a temporary directory to serve as `HOME`.
    2.  Define a set of expected environment variables based on this `HOME`.
    3.  Ensure `ORACLE_HOME` and `DW_DIR_CUSTOMER` are not pre-set to test default behavior.
*   **Action:**
    1.  Execute the legacy KornShell script with the mock `HOME` and capture its environment variables.
    2.  Execute the migrated Python script with the same mock `HOME` and capture its environment variables.
*   **Pass/Fail Criterion:**
    *   The values of all `DW_DIR_*` variables, `GEN_HOME`, and `DW_HOST_CUSTOMER` captured from the Python script's environment must exactly match those captured from the legacy script's environment.
    *   The Python script should set `DW_DIR_IMP_MP_ZM` to `$HOME/daten/mp/zm` and export it, reflecting the typo correction. The legacy script will have `DW_DIR_IMP_MP_ZM` set but not exported, and `DW_DIR_IMP_MP_TS` exported twice (once correctly, once incorrectly by the typo line). This specific difference is expected and should be noted.

```python
def test_direct_env_variable_assignments(temp_dir):
    mock_home = os.path.join(temp_dir, "mock_user_home")
    os.makedirs(mock_home)

    initial_env = {
        'DW_DIR_CUSTOMER': '<login>' # Legacy script hardcodes this, Python uses a default if not set
    }

    legacy_env, legacy_rc, _ = run_legacy_script(mock_home, initial_env=initial_env)
    migrated_env, migrated_rc, _ = run_migrated_script(mock_home, initial_env=initial_env)

    assert legacy_rc == 0
    assert migrated_rc == 0

    expected_vars = [
        'DW_DIR_ROOT', 'DW_DIR_PROT', 'DW_DIR_CUBES', 'DW_DIR_IMP_D1',
        'DW_DIR_IMP_XTRA', 'DW_DIR_IMP_CTEL', 'DW_DIR_IMP_VO', 'DW_DIR_IMP_RV',
        'DW_DIR_IMP_TRF', 'DW_DIR_IMP_TS', 'DW_DIR_IMP_ZM', 'DW_DIR_IMP_AUF',
        'DW_DIR_IMP_GUT', 'DW_DIR_IMP_KDG', 'DW_DIR_IMP_MP_TS', 'DW_DIR_IMP_MP_KDG',
        'DW_DIR_IMP_IF', 'DW_DIR_IMP_NNV', 'DW_DIR_IMP_CARMEN', 'GEN_HOME',
        'DW_HOST_CUSTOMER'
    ]

    for var in expected_vars:
        if var == 'DW_DIR_IMP_MP_TS':
            # Legacy script has a typo: DW_DIR_IMP_MP_ZM=$HOME/daten/mp/zm; export DW_DIR_IMP_MP_TS
            # This means DW_DIR_IMP_MP_TS is exported twice, once correctly, once incorrectly.
            # The Python script corrects this.
            # We compare the *intended* value for DW_DIR_IMP_MP_TS.
            assert migrated_env.get(var) == f"{mock_home}/daten/mp/ts"
            # For legacy, we check if it's exported at all, and its value from the first correct assignment
            assert legacy_env.get(var) == f"{mock_home}/daten/mp/ts"
        elif var == 'DW_DIR_IMP_MP_ZM':
            # Legacy script sets DW_DIR_IMP_MP_ZM but exports DW_DIR_IMP_MP_TS instead.
            # So, DW_DIR_IMP_MP_ZM is NOT exported by the legacy script.
            # The Python script correctly sets and exports DW_DIR_IMP_MP_ZM.
            assert var not in legacy_env # Legacy does not export this due to typo
            assert migrated_env.get(var) == f"{mock_home}/daten/mp/zm"
        elif var == 'DW_DIR_CUSTOMER':
            # Legacy script hardcodes <login>, Python uses a placeholder if not set
            assert legacy_env.get(var) == '<login>'
            assert migrated_env.get(var) == '<login>' # Because we pre-set it in initial_env
        else:
            assert migrated_env.get(var) == legacy_env.get(var), f"Mismatch for {var}"

    # Verify DW_DIR_IMP_MP_ZM is exported by Python but not by legacy
    assert 'DW_DIR_IMP_MP_ZM' in migrated_env
    assert 'DW_DIR_IMP_MP_ZM' not in legacy_env # Due to legacy typo

    print("All direct environment variables match (accounting for known typo).")

```

### Test Case 2: `DW_DIR_CUSTOMER` Handling (Transformation Correctness)

*   **Purpose:** To verify that `DW_DIR_CUSTOMER` is handled as specified: using a pre-set environment variable if available, otherwise defaulting to the placeholder.
*   **Setup:**
    *   **Scenario A:** `DW_DIR_CUSTOMER` is pre-set to a specific value (e.g., "test_user").
    *   **Scenario B:** `DW_DIR_CUSTOMER` is not pre-set.
    *   Use a temporary `HOME` directory.
*   **Action:**
    1.  Execute both scripts for Scenario A and capture `DW_DIR_CUSTOMER`.
    2.  Execute both scripts for Scenario B and capture `DW_DIR_CUSTOMER`.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** Both legacy and Python scripts must set `DW_DIR_CUSTOMER` to "test_user".
    *   **Scenario B:** The legacy script must set `DW_DIR_CUSTOMER` to `<login>`. The Python script must set `DW_DIR_CUSTOMER` to `<REPLACE_ME_CUSTOMER_LOGIN>`. This is an expected difference due to the design document's instruction to replace the placeholder.

```python
def test_dw_dir_customer_handling(temp_dir):
    mock_home = os.path.join(temp_dir, "mock_user_home")
    os.makedirs(mock_home)

    # Scenario A: DW_DIR_CUSTOMER is pre-set
    pre_set_value = "test_user_login"
    legacy_env_a, _, _ = run_legacy_script(mock_home, initial_env={'DW_DIR_CUSTOMER': pre_set_value})
    migrated_env_a, _, _ = run_migrated_script(mock_home, initial_env={'DW_DIR_CUSTOMER': pre_set_value})
    assert legacy_env_a.get('DW_DIR_CUSTOMER') == pre_set_value
    assert migrated_env_a.get('DW_DIR_CUSTOMER') == pre_set_value
    print(f"DW_DIR_CUSTOMER (pre-set): Legacy='{legacy_env_a.get('DW_DIR_CUSTOMER')}', Migrated='{migrated_env_a.get('DW_DIR_CUSTOMER')}' - MATCH")

    # Scenario B: DW_DIR_CUSTOMER is not pre-set
    # Legacy script hardcodes it to <login>
    legacy_env_b, _, _ = run_legacy_script(mock_home, initial_env={})
    # Python script defaults to '<REPLACE_ME_CUSTOMER_LOGIN>' if not set
    migrated_env_b, _, _ = run_migrated_script(mock_home, initial_env={})
    assert legacy_env_b.get('DW_DIR_CUSTOMER') == '<login>'
    assert migrated_env_b.get('DW_DIR_CUSTOMER') == '<REPLACE_ME_CUSTOMER_LOGIN>'
    print(f"DW_DIR_CUSTOMER (not pre-set): Legacy='{legacy_env_b.get('DW_DIR_CUSTOMER')}', Migrated='{migrated_env_b.get('DW_DIR_CUSTOMER')}' - EXPECTED DIFFERENCE (placeholder replacement)")

```

### Test Case 3: `ORACLE_HOME` - Already Set (Transformation Correctness)

*   **Purpose:** To verify that if `ORACLE_HOME` is already set in the environment, neither the legacy nor the Python script attempts to change it, preserving existing configuration.
*   **Setup:**
    1.  Create a temporary `HOME` directory.
    2.  Pre-set `ORACLE_HOME` to a specific, non-candidate value (e.g., `/usr/local/oracle/my_custom_home`).
*   **Action:**
    1.  Execute the legacy KornShell script.
    2.  Execute the migrated Python script.
*   **Pass/Fail Criterion:** `ORACLE_HOME` must remain unchanged (i.e., equal to `/usr/local/oracle/my_custom_home`) after both script executions.

```python
def test_oracle_home_already_set(temp_dir):
    mock_home = os.path.join(temp_dir, "mock_user_home")
    os.makedirs(mock_home)
    pre_set_oracle_home = "/usr/local/oracle/my_custom_home"

    initial_env = {'ORACLE_HOME': pre_set_oracle_home}
    legacy_env, legacy_rc, _ = run_legacy_script(mock_home, initial_env=initial_env)
    migrated_env, migrated_rc, _ = run_migrated_script(mock_home, initial_env=initial_env)

    assert legacy_rc == 0
    assert migrated_rc == 0
    assert legacy_env.get('ORACLE_HOME') == pre_set_oracle_home
    assert migrated_env.get('ORACLE_HOME') == pre_set_oracle_home
    print(f"ORACLE_HOME (pre-set): Legacy='{legacy_env.get('ORACLE_HOME')}', Migrated='{migrated_env.get('ORACLE_HOME')}' - MATCH")

```

### Test Case 4: `ORACLE_HOME` - Candidate Path Exists (External System Replacement & Transformation Correctness)

*   **Purpose:** To verify `ORACLE_HOME` is correctly detected and set when one of the legacy candidate paths exists on the filesystem, including specific version mappings. This tests the `os.path.isdir` logic and its equivalence to `[ -d ... ]`.
*   **Setup:**
    1.  Create a temporary `HOME` directory.
    2.  Create a mock directory for *one* of the `ORACLE_HOME` candidate paths (e.g., `/appl/local/oracle/7.3.4`).
    3.  Ensure `ORACLE_HOME` is not pre-set.
*   **Action:**
    1.  Execute the legacy KornShell script.
    2.  Execute the migrated Python script.
*   **Pass/Fail Criterion:** `ORACLE_HOME` must be set to the expected value (e.g., `/appl/local/oracle/7.3.4` or `/appl/local/oracle/8.1.6` for `oracle.8.1.6`) by both scripts.

```python
@pytest.mark.parametrize("existing_path, expected_oracle_home", [
    ("/appl/local/oracle/oracle.8.1.6", "/appl/local/oracle/8.1.6"),
    ("/appl/local/oracle/7.3.4", "/appl/local/oracle/7.3.4"),
    ("/appl/local/oracle/oracle.7.3.3", "/appl/local/oracle/oracle.7.3.3"),
    ("/appl/local/oracle/7.3.2", "/appl/local/oracle/7.3.2"),
    ("/appl/local/oracle/7.2.3", "/appl/local/oracle/7.2.3"),
])
def test_oracle_home_candidate_path_exists(temp_dir, mock_oracle_paths, existing_path, expected_oracle_home):
    mock_home = os.path.join(temp_dir, "mock_user_home")
    os.makedirs(mock_home)

    # Create only the specified existing_path
    path_to_create_in_temp = mock_oracle_paths[existing_path]
    os.makedirs(path_to_create_in_temp, exist_ok=True)

    # Adjust expected_oracle_home if it's one of the mapped paths
    if expected_oracle_home.startswith("/appl/local/oracle/"):
        # Replace the prefix with the temp_dir equivalent
        # This is a bit tricky because the Python script's logic for ORACLE_HOME
        # still refers to the absolute paths, not the mocked ones.
        # So, we need to ensure the test environment *actually* has the paths.
        # The `run_legacy_script` and `run_migrated_script` helpers already handle this
        # by creating the paths directly in the filesystem.
        pass # No adjustment needed for expected_oracle_home itself, as it's the target value.

    legacy_env, legacy_rc, _ = run_legacy_script(mock_home, initial_env={}, oracle_paths_to_create=[existing_path])
    migrated_env, migrated_rc, _ = run_migrated_script(mock_home, initial_env={}, oracle_paths_to_create=[existing_path])

    assert legacy_rc == 0
    assert migrated_rc == 0
    assert legacy_env.get('ORACLE_HOME') == expected_oracle_home
    assert migrated_env.get('ORACLE_HOME') == expected_oracle_home
    print(f"ORACLE_HOME (path '{existing_path}' exists): Legacy='{legacy_env.get('ORACLE_HOME')}', Migrated='{migrated_env.get('ORACLE_HOME')}' - MATCH")

```

### Test Case 5: `ORACLE_HOME` - No Candidate Paths Exist (Transformation Correctness & Error Handling)

*   **Purpose:** To verify that both scripts handle the case where no `ORACLE_HOME` candidate paths are found, resulting in an error and exiting with a non-zero status code. This tests error handling and behavioral equivalence for failure conditions.
*   **Setup:**
    1.  Create a temporary `HOME` directory.
    2.  Ensure `ORACLE_HOME` is not pre-set.
    3.  Ensure none of the `ORACLE_HOME` candidate paths exist.
*   **Action:**
    1.  Execute the legacy KornShell script and capture its exit code and stderr.
    2.  Execute the migrated Python script and capture its exit code and stderr.
*   **Pass/Fail Criterion:**
    *   Both scripts must exit with a non-zero status code (e.g., `1`).
    *   Both scripts must print an error message to stderr indicating `ORACLE_HOME` could not be set. The exact message may differ but the intent should be clear.

```python
def test_oracle_home_no_candidate_paths(temp_dir):
    mock_home = os.path.join(temp_dir, "mock_user_home")
    os.makedirs(mock_home)

    legacy_env, legacy_rc, legacy_stderr = run_legacy_script(mock_home, initial_env={}, oracle_paths_to_create=[])
    migrated_env, migrated_rc, migrated_stderr = run_migrated_script(mock_home, initial_env={}, oracle_paths_to_create=[])

    assert legacy_rc != 0
    assert migrated_rc != 0
    assert "Konnte ORACLE_HOME nicht setzen" in legacy_stderr
    assert "Could not set ORACLE_HOME" in migrated_stderr
    print(f"ORACLE_HOME (no paths): Legacy exit code={legacy_rc}, Migrated exit code={migrated_rc} - MATCH (both failed as expected)")
    print(f"Legacy stderr: {legacy_stderr.strip()}")
    print(f"Migrated stderr: {migrated_stderr.strip()}")

```

### Test Case 6: Sourced Scripts and `umask` Warnings (Data Quality / Schema Assertions - N/A, but behavioral assertion)

*   **Purpose:** To verify that the Python script correctly identifies and warns about the unmigrated sourced scripts (`.dw_global`, `.dw_lokal`) and the untranslatable `umask` setting, as these are critical unresolved items from the design document.
*   **Setup:**
    1.  Create a temporary `HOME` directory.
    2.  Ensure `ORACLE_HOME` is set (to avoid early exit) or mock a path.
*   **Action:**
    1.  Execute the migrated Python script and capture its stderr output.
*   **Pass/Fail Criterion:** The stderr output of the Python script must contain specific warning messages about `.dw_global`, `.dw_lokal`, and `umask`. The legacy script does not produce such warnings, so this is a test of the migrated script's self-documentation/warning behavior.

```python
def test_warnings_for_unmigrated_components(temp_dir, mock_oracle_paths):
    mock_home = os.path.join(temp_dir, "mock_user_home")
    os.makedirs(mock_home)

    # Ensure ORACLE_HOME can be set to avoid early exit
    path_to_create_in_temp = mock_oracle_paths["/appl/local/oracle/7.3.4"]
    os.makedirs(path_to_create_in_temp, exist_ok=True)

    _, migrated_rc, migrated_stderr = run_migrated_script(mock_home, initial_env={}, oracle_paths_to_create=["/appl/local/oracle/7.3.4"])

    assert migrated_rc == 0 # Should not exit if ORACLE_HOME is set
    assert "WARNING: The legacy scripts '$HOME/.dw_global' and '$HOME/.dw_lokal' were not migrated." in migrated_stderr
    assert "INFO: 'umask' setting from the legacy script is not directly translatable to this environment." in migrated_stderr
    print("Migrated script correctly issued warnings for unmigrated components.")

```

---

### Summary of Test Coverage:

*   **Output Parity:** Covered by Test Case 1, ensuring environment variables are set to identical values where expected.
*   **Transformation Correctness:**
    *   Direct variable assignments: Test Case 1.
    *   `DW_DIR_CUSTOMER` placeholder: Test Case 2.
    *   `ORACLE_HOME` conditional logic: Test Cases 3, 4, 5.
    *   Typo handling (`DW_DIR_IMP_MP_ZM`): Test Case 1 explicitly checks the corrected behavior.
    *   NULL handling: Implicitly covered by `ORACLE_HOME` checks (when not set).
*   **External-system replacements:** `ORACLE_HOME` filesystem checks (`os.path.isdir` vs. `[ -d ... ]`) are covered in Test Cases 4 and 5.
*   **Data-quality / row-count / schema assertions:** Not directly applicable to this environment setup script. However, the tests assert the *quality* of the environment setup by verifying the correctness of variable values and error handling.
*   **Edge Cases:**
    *   `ORACLE_HOME` already set (Test Case 3).
    *   No `ORACLE_HOME` paths found (Test Case 5).
    *   `DW_DIR_CUSTOMER` not set (Test Case 2).
    *   Unmigrated components (`.dw_global`, `.dw_lokal`, `umask`) are explicitly checked for warning messages (Test Case 6).

These tests provide a robust validation of the migrated `dw_init_environment.py` script against the specified design and the legacy KornShell behavior.