As a senior data-migration QA engineer, I've reviewed the migration design and the generated BigQuery code for the `.dw_init` and `.dw_global` KornShell scripts. The migration focuses on translating environment variable setup and conditional logic into BigQuery stored procedures and configuration tables.

Below are the migration validation tests, covering output parity, transformation correctness, external system replacements, and data quality. Each test includes its purpose, setup, action, and concrete pass/fail criteria, with runnable SQL assertions where applicable.

---

## Migration Validation Tests for `vobs/dw_source/istools/seu/template/.dw_init`

**Global Test Setup (Pre-requisites for all tests):**

Before running any tests, ensure the following BigQuery resources are created and accessible:

*   **Project ID:** `your_project_id` (replace with actual project ID)
*   **Dataset ID:** `your_dataset_id` (replace with actual dataset ID)
*   **Table:** `your_project_id.your_dataset_id.dw_runtime_config`
    *   Schema: `config_name STRING NOT NULL, config_value STRING, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()`
*   **Table:** `your_project_id.your_dataset_id.oracle_home_config`
    *   Schema: `candidate STRING NOT NULL, is_active BOOL NOT NULL DEFAULT FALSE, priority INT NOT NULL DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()`
*   **Stored Procedure:** `your_project_id.your_dataset_id.dw_init_validate_config` (as provided in the generated code)

**Helper Function (Python/Pytest style for executing BigQuery SQL):**

```python
import google.cloud.bigquery as bq
import pytest

# Replace with your actual project and dataset IDs
PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"

client = bq.Client(project=PROJECT_ID)

def execute_bq_query(query: str):
    """Executes a BigQuery SQL query and returns the results."""
    query_job = client.query(query)
    return query_job.result()

def call_dw_init_validate_config(
    p_home_directory: str,
    p_dw_dir_customer: str,
    p_dw_host_customer: str
):
    """Calls the dw_init_validate_config stored procedure."""
    procedure_call = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.dw_init_validate_config`(
        '{p_home_directory}',
        '{p_dw_dir_customer}',
        '{p_dw_host_customer}'
    );
    """
    execute_bq_query(procedure_call)

def get_config_value(config_name: str) -> str:
    """Retrieves a specific config_value from dw_runtime_config."""
    query = f"""
    SELECT config_value FROM `{PROJECT_ID}.{DATASET_ID}.dw_runtime_config`
    WHERE config_name = '{config_name}';
    """
    rows = list(execute_bq_query(query))
    if rows:
        return rows[0].config_value
    return None

def get_all_config_values() -> dict:
    """Retrieves all config_name/config_value pairs from dw_runtime_config."""
    query = f"""
    SELECT config_name, config_value FROM `{PROJECT_ID}.{DATASET_ID}.dw_runtime_config`;
    """
    rows = list(execute_bq_query(query))
    return {row.config_name: row.config_value for row in rows}

def clear_dw_runtime_config():
    """Clears all entries from dw_runtime_config."""
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dw_runtime_config` WHERE TRUE;"
    execute_bq_query(query)

def setup_oracle_home_config(data: list):
    """Populates or updates oracle_home_config with test data."""
    clear_oracle_home_config() # Ensure a clean slate
    if not data:
        return

    values_str = ",\n".join([f"('{c}', {a}, {p})" for c, a, p in data])
    query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.oracle_home_config` (candidate, is_active, priority)
    VALUES
        {values_str}
    ON CONFLICT (candidate) DO UPDATE SET
        is_active = EXCLUDED.is_active,
        priority = EXCLUDED.priority;
    """
    execute_bq_query(query)

def clear_oracle_home_config():
    """Clears all entries from oracle_home_config."""
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.oracle_home_config` WHERE TRUE;"
    execute_bq_query(query)

# Initial population of oracle_home_config as per design
initial_oracle_data = [
    ('/appl/local/oracle/8.1.6', True, 5),
    ('/appl/local/oracle/7.3.4', False, 4),
    ('/appl/local/oracle/oracle.7.3.3', False, 3),
    ('/appl/local/oracle/7.3.2', False, 2),
    ('/appl/local/oracle/7.2.3', False, 1)
]
```

---

### Test Case 1: Basic Variable Assignment and Output Parity (Happy Path)

*   **Purpose:** Verify that all `DW_DIR_*` and `GEN_HOME` variables are correctly calculated and stored in `dw_runtime_config` when valid inputs are provided. This covers the core variable assignment transformation.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with the initial data, ensuring one active `ORACLE_HOME` (e.g., `/appl/local/oracle/8.1.6`).
*   **Action:**
    Call the stored procedure with a sample home directory and customer details:
    ```python
    p_home = "/home/testuser"
    p_customer_dir = "testlogin"
    p_customer_host = "test.host.com"
    call_dw_init_validate_config(p_home, p_customer_dir, p_customer_host)
    ```
*   **Pass/Fail Criterion:**
    Query `dw_runtime_config` and assert that all expected variables are present with their correct concatenated values.
    ```python
    expected_values = {
        'DW_DIR_ROOT': '/home/testuser/aktuell',
        'DW_DIR_PROT': '/home/testuser/daten/logfiles',
        'DW_DIR_CUBES': '/home/testuser/daten/cubes',
        'DW_DIR_IMP_D1': '/home/testuser/daten/d1',
        'DW_DIR_IMP_XTRA': '/home/testuser/daten/xtra',
        'DW_DIR_IMP_CTEL': '/home/testuser/daten/ctel',
        'DW_DIR_IMP_VO': '/home/testuser/daten/vo',
        'DW_DIR_IMP_RV': '/home/testuser/daten/rv',
        'DW_DIR_IMP_TRF': '/home/testuser/daten/trf',
        'DW_DIR_IMP_TS': '/home/testuser/daten/sd/ts',
        'DW_DIR_IMP_ZM': '/home/testuser/daten/sd/zm',
        'DW_DIR_IMP_AUF': '/home/testuser/daten/sd/auf',
        'DW_DIR_IMP_GUT': '/home/testuser/daten/sd/gut',
        'DW_DIR_IMP_KDG': '/home/testuser/daten/sd/kdg',
        'DW_DIR_IMP_MP_TS': '/home/testuser/daten/mp/ts',
        'DW_DIR_IMP_MP_KDG': '/home/testuser/daten/mp/kdg',
        'DW_DIR_IMP_MP_ZM': '/home/testuser/daten/mp/zm',
        'DW_DIR_IMP_IF': '/home/testuser/daten/if',
        'DW_DIR_IMP_NNV': '/home/testuser/daten/nnv',
        'DW_DIR_IMP_CARMEN': '/home/testuser/daten/carmen',
        'GEN_HOME': '/home/testuser/aktuell/generator',
        'DW_DIR_CUSTOMER': 'testlogin',
        'DW_HOST_CUSTOMER': 'test.host.com',
        'ORACLE_HOME': '/appl/local/oracle/8.1.6', # Based on initial_oracle_data
        'NLS_LANG': 'GERMAN_GERMANY.WE8ISO8859P1',
        'NLS_DATE_FORMAT': 'DD-MON-YY',
        'NLS_DATE_LANGUAGE': 'AMERICAN'
    }
    actual_values = get_all_config_values()
    assert actual_values == expected_values
    ```

---

### Test Case 2: ORACLE_HOME Resolution - Active Entry (Transformation Correctness)

*   **Purpose:** Verify `ORACLE_HOME` is correctly selected from `oracle_home_config` based on `is_active` and `priority`, replicating the legacy filesystem probing logic.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with multiple entries, ensuring one active and highest priority:
        ```python
        setup_oracle_home_config([
            ('/appl/local/oracle/7.2.3', False, 1),
            ('/appl/local/oracle/7.3.2', False, 2),
            ('/appl/local/oracle/oracle.7.3.3', True, 3), # Active, but lower priority
            ('/appl/local/oracle/7.3.4', False, 4),
            ('/appl/local/oracle/8.1.6', True, 5) # Active and highest priority
        ])
        ```
*   **Action:**
    Call the stored procedure with valid inputs:
    ```python
    call_dw_init_validate_config("/home/testuser", "testlogin", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    Assert that `ORACLE_HOME` in `dw_runtime_config` matches the expected highest priority active entry.
    ```python
    assert get_config_value('ORACLE_HOME') == '/appl/local/oracle/8.1.6'
    ```

---

### Test Case 3: ORACLE_HOME Resolution - No Active Entry (Edge Case / Transformation Correctness)

*   **Purpose:** Verify the `ASSERT` statement for `ORACLE_HOME` triggers if no active `ORACLE_HOME` is found in `oracle_home_config`, mimicking the legacy script's exit behavior.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Set all `is_active` flags in `oracle_home_config` to `FALSE`.
        ```python
        setup_oracle_home_config([
            ('/appl/local/oracle/8.1.6', False, 5),
            ('/appl/local/oracle/7.3.4', False, 4)
        ])
        ```
*   **Action:**
    Attempt to call the stored procedure:
    ```python
    with pytest.raises(Exception) as excinfo:
        call_dw_init_validate_config("/home/testuser", "testlogin", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    The stored procedure call should fail, and the error message should contain the `ASSERT` message:
    ```python
    assert "ORACLE_HOME could not be determined" in str(excinfo.value)
    ```

---

### Test Case 4: NLS Settings Parity (Output Parity)

*   **Purpose:** Verify NLS settings (`NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`) are correctly assigned and stored, matching the hardcoded values from `.dw_global`.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with an active `ORACLE_HOME`.
*   **Action:**
    Call the stored procedure with valid inputs:
    ```python
    call_dw_init_validate_config("/home/testuser", "testlogin", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    Assert that the NLS variables in `dw_runtime_config` match the expected values.
    ```python
    assert get_config_value('NLS_LANG') == 'GERMAN_GERMANY.WE8ISO8859P1'
    assert get_config_value('NLS_DATE_FORMAT') == 'DD-MON-YY'
    assert get_config_value('NLS_DATE_LANGUAGE') == 'AMERICAN'
    ```

---

### Test Case 5: Validation Checks - Missing `p_home_directory` (Edge Case / Transformation Correctness)

*   **Purpose:** Verify that if `p_home_directory` is `NULL` or empty, the subsequent `ASSERT` for `DW_DIR_ROOT` (which would become `'/aktuell'`) correctly triggers, mimicking the failure of the legacy script if `$HOME` was unset or empty.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with an active `ORACLE_HOME`.
*   **Action:**
    Attempt to call the stored procedure with `p_home_directory` as an empty string:
    ```python
    with pytest.raises(Exception) as excinfo:
        call_dw_init_validate_config("", "testlogin", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    The stored procedure call should fail, and the error message should contain the `ASSERT` message for `DW_DIR_ROOT`.
    ```python
    assert "Environment variable DW_DIR_ROOT is not set!" in str(excinfo.value)
    ```

---

### Test Case 6: Validation Checks - Missing `p_dw_dir_customer` (Edge Case / Transformation Correctness)

*   **Purpose:** Verify the `ASSERT` for `p_dw_dir_customer` triggers if the parameter is `NULL` or empty, reflecting the importance of this value.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with an active `ORACLE_HOME`.
*   **Action:**
    Attempt to call the stored procedure with `p_dw_dir_customer` as an empty string:
    ```python
    with pytest.raises(Exception) as excinfo:
        call_dw_init_validate_config("/home/testuser", "", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    The stored procedure call should fail, and the error message should contain the `ASSERT` message for `p_dw_dir_customer`.
    ```python
    assert "Parameter p_dw_dir_customer is not set!" in str(excinfo.value)
    ```

---

### Test Case 7: Validation Checks - Missing `p_dw_host_customer` (Edge Case / Transformation Correctness)

*   **Purpose:** Verify the `ASSERT` for `p_dw_host_customer` triggers if the parameter is `NULL` or empty, reflecting the importance of this value.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with an active `ORACLE_HOME`.
*   **Action:**
    Attempt to call the stored procedure with `p_dw_host_customer` as an empty string:
    ```python
    with pytest.raises(Exception) as excinfo:
        call_dw_init_validate_config("/home/testuser", "testlogin", "")
    ```
*   **Pass/Fail Criterion:**
    The stored procedure call should fail, and the error message should contain the `ASSERT` message for `p_dw_host_customer`.
    ```python
    assert "Parameter p_dw_host_customer is not set!" in str(excinfo.value)
    ```

---

### Test Case 8: Data Quality - Row Count and Schema of `dw_runtime_config`

*   **Purpose:** Verify the `dw_runtime_config` table has the correct number of entries and schema after a successful run, ensuring data integrity.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with an active `ORACLE_HOME`.
*   **Action:**
    Call the stored procedure with valid inputs:
    ```python
    call_dw_init_validate_config("/home/testuser", "testlogin", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    1.  Query the schema of `dw_runtime_config` and assert it matches the expected definition.
    2.  Query the row count of `dw_runtime_config` and assert it is 26 (the number of variables inserted).
    ```python
    # Schema assertion (conceptual, typically done via BigQuery API or information_schema)
    # Example:
    # table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.dw_runtime_config")
    # assert len(table.schema) == 3
    # assert table.schema[0].name == 'config_name' and table.schema[0].field_type == 'STRING'
    # ... and so on for all columns

    # Row count assertion
    query_count = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.dw_runtime_config`;"
    rows = list(execute_bq_query(query_count))
    assert rows[0][0] == 26
    ```

---

### Test Case 9: Idempotency - Clearing Previous Configuration (Transformation Correctness)

*   **Purpose:** Verify that running the procedure multiple times correctly clears and re-populates `dw_runtime_config`, ensuring idempotency.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with an active `ORACLE_HOME`.
*   **Action:**
    1.  Call `dw_init_validate_config` with `p_home_directory = "/home/user1"`.
    2.  Call `dw_init_validate_config` again with `p_home_directory = "/home/user2"`.
    ```python
    call_dw_init_validate_config("/home/user1", "login1", "host1.com")
    call_dw_init_validate_config("/home/user2", "login2", "host2.com")
    ```
*   **Pass/Fail Criterion:**
    After the second call, `dw_runtime_config` should reflect only the values from the second call.
    ```python
    assert get_config_value('DW_DIR_ROOT') == '/home/user2/aktuell'
    assert get_config_value('DW_DIR_CUSTOMER') == 'login2'
    assert get_config_value('DW_HOST_CUSTOMER') == 'host2.com'
    # Ensure no values from the first run persist
    assert '/home/user1/aktuell' not in get_all_config_values().values()
    ```

---

### Test Case 10: `DW_DIR_IMP_MP_ZM` Typo Correction (Transformation Correctness)

*   **Purpose:** Verify the correction of the typo `DW_DIR_IMP_MP_TS` to `DW_DIR_IMP_MP_ZM` in the generated code, ensuring the correct variable is set.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with an active `ORACLE_HOME`.
*   **Action:**
    Call the stored procedure with valid inputs:
    ```python
    call_dw_init_validate_config("/home/testuser", "testlogin", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    Assert that `DW_DIR_IMP_MP_ZM` is present in `dw_runtime_config` with the correct path, and `DW_DIR_IMP_MP_TS` is also present with its correct path.
    ```python
    assert get_config_value('DW_DIR_IMP_MP_ZM') == '/home/testuser/daten/mp/zm'
    assert get_config_value('DW_DIR_IMP_MP_TS') == '/home/testuser/daten/mp/ts'
    ```

---

### Test Case 11: `oracle_home_config` Priority Handling (Transformation Correctness)

*   **Purpose:** Verify that if multiple `is_active=TRUE` entries exist, the one with the highest `priority` is chosen for `ORACLE_HOME`.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with two active entries, one with higher priority:
        ```python
        setup_oracle_home_config([
            ('/appl/local/oracle/8.1.6', True, 5),
            ('/appl/local/oracle/7.3.4', True, 10) # Higher priority
        ])
        ```
*   **Action:**
    Call the stored procedure with valid inputs:
    ```python
    call_dw_init_validate_config("/home/testuser", "testlogin", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    `ORACLE_HOME` in `dw_runtime_config` should be `'/appl/local/oracle/7.3.4'`.
    ```python
    assert get_config_value('ORACLE_HOME') == '/appl/local/oracle/7.3.4'
    ```

---

### Test Case 12: `oracle_home_config` Tie-breaking (Candidate Path) (Transformation Correctness)

*   **Purpose:** Verify that if multiple `is_active=TRUE` entries have the same highest `priority`, the one with the lexicographically largest `candidate` path is chosen (due to `candidate DESC` in the `ORDER BY` clause).
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with two active entries with the same highest priority:
        ```python
        setup_oracle_home_config([
            ('/appl/local/oracle/8.1.6', True, 5),
            ('/appl/local/oracle/8.1.7', True, 5) # Same priority, but lexicographically larger path
        ])
        ```
*   **Action:**
    Call the stored procedure with valid inputs:
    ```python
    call_dw_init_validate_config("/home/testuser", "testlogin", "test.host.com")
    ```
*   **Pass/Fail Criterion:**
    `ORACLE_HOME` in `dw_runtime_config` should be `'/appl/local/oracle/8.1.7'`.
    ```python
    assert get_config_value('ORACLE_HOME') == '/appl/local/oracle/8.1.7'
    ```

---

### Test Case 13: External System Replacements - `LD_LIBRARY_PATH` and `PATH` (Conceptual)

*   **Purpose:** Verify that the orchestrator can correctly derive and use `ORACLE_HOME` to construct OS-level environment variables like `LD_LIBRARY_PATH` and `PATH` for external tools, as specified in the design.
*   **Setup:**
    1.  Clear `dw_runtime_config`.
    2.  Populate `oracle_home_config` with an active `ORACLE_HOME` (e.g., `/appl/local/oracle/8.1.6`).
*   **Action:**
    1.  Call `dw_init_validate_config` with valid inputs.
    2.  In the orchestrator (e.g., Airflow DAG), retrieve `ORACLE_HOME` from `dw_runtime_config`.
    3.  Use this `ORACLE_HOME` to set `LD_LIBRARY_PATH` and `PATH` for a dummy external process (e.g., a Python script that prints its environment variables).
    ```python
    # Orchestrator Python code snippet (conceptual)
    call_dw_init_validate_config("/home/testuser", "testlogin", "test.host.com")
    oracle_home = get_config_value('ORACLE_HOME')

    # Simulate setting environment variables for an external process
    # This would typically be done by an Airflow BashOperator or PythonOperator
    # that then calls an external script.
    env_vars = {
        "ORACLE_HOME": oracle_home,
        "LD_LIBRARY_PATH": f"{oracle_home}/lib:{oracle_home}/rdbms/lib", # Example
        "PATH": f"{oracle_home}/bin:$PATH" # Example
    }
    # Execute a dummy script with these environment variables
    # For example: subprocess.run(["/bin/bash", "-c", "env"], env=env_vars, check=True)
    ```
*   **Pass/Fail Criterion:**
    The external process should report `ORACLE_HOME`, `LD_LIBRARY_PATH`, and `PATH` being set correctly based on the value retrieved from `dw_runtime_config`. This is a conceptual test as it involves the orchestrator, which is outside the direct BigQuery migration.

---

### Test Case 14: External System Replacements - Cognos Setup and `umask` (Conceptual)

*   **Purpose:** Verify that the orchestrator can handle the legacy Cognos setup and `umask` settings, as these have no direct BigQuery equivalent.
*   **Setup:** N/A (conceptual test).
*   **Action:**
    1.  The orchestrator (e.g., Airflow DAG) triggers `dw_init_validate_config`.
    2.  If Cognos is still in use, the orchestrator executes a task that sets up the Cognos environment (e.g., running `setpya.sh` in a compatible container or VM).
    3.  Any file creation tasks in the orchestrator's compute environment are configured to use the desired `umask` (e.g., `022`).
*   **Pass/Fail Criterion:**
    *   Cognos-dependent tasks run successfully in the orchestrated environment.
    *   Files created by orchestrated tasks have the correct permissions (e.g., `rw-r--r--` for files, `rwxr-xr-x` for directories, corresponding to `umask 022`). This is a conceptual test requiring actual orchestrator implementation.

---

### Test Case 15: Missing `.dw_lokal` Handling (Risk Mitigation)

*   **Purpose:** Verify that the migration correctly acknowledges and handles the missing `.dw_lokal` script, as per the design document's "Unresolved / Risks" section.
*   **Setup:** N/A.
*   **Action:**
    Review the generated BigQuery stored procedure `dw_init_validate_config`.
*   **Pass/Fail Criterion:**
    The stored procedure's comments should explicitly mention the missing `.dw_lokal` and its implications, as seen in the provided generated code. This confirms the risk has been documented and acknowledged in the target code.
    ```sql
    -- Note on .dw_lokal:
    -- The original .dw_init sourced a missing file named .dw_lokal.
    -- This stored procedure cannot replicate its functionality as its content is unknown.
    -- Any critical configurations or logic previously defined in .dw_lokal need to be
    -- identified and explicitly added to this procedure or managed externally.
    ```
    This comment is present, so this test passes. Further action would be to ensure no critical functionality was lost.

---