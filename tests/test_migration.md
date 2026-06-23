As a senior data-migration QA engineer, I've analyzed the migration design for `h_alis_sqlplus.ksh` to BigQuery. The core challenge is translating shell script control flow and external `sqlplus` execution into BigQuery stored procedures and tables. A significant point of divergence is the handling of parameters for the invoked SQL scripts, which the current BigQuery implementation does not fully replicate.

The following test cases are designed to validate the migrated solution against the specified criteria.

---

## Migration Validation Tests for `h_alis_sqlplus.ksh`

### General Setup

Before running any tests, ensure the following BigQuery resources are provisioned and the Python environment is configured:

*   **BigQuery Project and Dataset:** A GCP project and a BigQuery dataset (e.g., `your-gcp-project-id.dataset`) must exist.
*   **BigQuery Tables:** `script_registry` and `error_log` tables must be created within the dataset.
*   **BigQuery Stored Procedures:** `DWMSG_MeldeFehler` and `starteSQLSkript` stored procedures must be created within the dataset.
*   **Python Environment:** `google-cloud-bigquery` library installed.
*   **Authentication:** Your environment must be authenticated to GCP (e.g., via `gcloud auth application-default login`).

The provided `pytest` setup code handles the creation of these objects if they don't exist and clears test-specific tables before each test.

```python
import pytest
from google.cloud import bigquery
import time
import os
import pandas as pd

# --- Configuration ---
# Set your GCP Project ID as an environment variable or replace directly
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id") 
DATASET_ID = "dataset" # As per design: project.dataset
SCRIPT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.script_registry"
ERROR_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.error_log"
DWMSG_MELDEFEHLER_PROC = f"{PROJECT_ID}.{DATASET_ID}.DWMSG_MeldeFehler"
STARTE_SQL_SKRIPT_PROC = f"{PROJECT_ID}.{DATASET_ID}.starteSQLSkript"

client = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions for BigQuery Interaction ---
def execute_bq_query(query):
    """Executes a BigQuery SQL query and returns the result."""
    query_job = client.query(query)
    return query_job.result()

def call_bq_procedure(proc_name, *args):
    """
    Constructs and executes a BigQuery CALL statement for a stored procedure.
    Handles STRING, INT, ARRAY<STRING>, and NULL arguments.
    """
    arg_strings = []
    for arg in args:
        if isinstance(arg, str):
            arg_strings.append(f"'{arg}'")
        elif isinstance(arg, int):
            arg_strings.append(str(arg))
        elif isinstance(arg, list): # For ARRAY<STRING>
            if not arg:
                arg_strings.append("[]")
            else:
                str_elements = [f"'{s}'" for s in arg]
                arg_strings.append(f"[{', '.join(str_elements)}]")
        elif arg is None:
            arg_strings.append("NULL")
        else:
            raise ValueError(f"Unsupported argument type: {type(arg)}")
    
    query = f"CALL {proc_name}({', '.join(arg_strings)});"
    print(f"Executing: {query}")
    query_job = client.query(query)
    # Procedures don't return rows, but the job object can be inspected for status
    return query_job.result()

def clear_table(table_id):
    """Truncates a BigQuery table."""
    execute_bq_query(f"TRUNCATE TABLE `{table_id}`")

def get_table_row_count(table_id):
    """Returns the row count of a BigQuery table."""
    result = execute_bq_query(f"SELECT COUNT(*) FROM `{table_id}`").to_dataframe()
    return result.iloc[0, 0]

def get_error_log_entries():
    """Retrieves all entries from the error_log table, ordered by timestamp."""
    return execute_bq_query(f"SELECT * FROM `{ERROR_LOG_TABLE}` ORDER BY timestamp DESC").to_dataframe()

def setup_bq_environment():
    """Ensures dataset, tables, and procedures exist in BigQuery."""
    # Ensure dataset exists
    try:
        client.get_dataset(DATASET_ID)
    except Exception:
        print(f"Creating dataset {DATASET_ID}...")
        client.create_dataset(DATASET_ID)

    # Create tables if they don't exist
    print(f"Creating/Verifying table {SCRIPT_REGISTRY_TABLE}...")
    execute_bq_query(f"""
        CREATE TABLE IF NOT EXISTS `{SCRIPT_REGISTRY_TABLE}` (
          script_name STRING,
          script_sql STRING,
          is_readable BOOLEAN,
          original_source_path STRING
        );
    """)
    print(f"Creating/Verifying table {ERROR_LOG_TABLE}...")
    execute_bq_query(f"""
        CREATE TABLE IF NOT EXISTS `{ERROR_LOG_TABLE}` (
          entry_number STRING,
          severity STRING,
          error_code INT64,
          message STRING,
          timestamp TIMESTAMP
        );
    """)

    # Create DWMSG_MeldeFehler procedure
    print(f"Creating/Verifying procedure {DWMSG_MELDEFEHLER_PROC}...")
    execute_bq_query(f"""
        CREATE OR REPLACE PROCEDURE `{DWMSG_MELDEFEHLER_PROC}`(
          p_Eintragsnr STRING,
          p_Severity STRING,
          p_ErrorCode INT64,
          p_Message STRING
        )
        BEGIN
          INSERT INTO `{ERROR_LOG_TABLE}` (entry_number, severity, error_code, message, timestamp)
          VALUES (p_Eintragsnr, p_Severity, p_ErrorCode, p_Message, CURRENT_TIMESTAMP());
        END;
    """)

    # Create starteSQLSkript procedure
    print(f"Creating/Verifying procedure {STARTE_SQL_SKRIPT_PROC}...")
    execute_bq_query(f"""
        CREATE OR REPLACE PROCEDURE `{STARTE_SQL_SKRIPT_PROC}`(
          p_Eintragsnr STRING,
          p_Skript STRING,
          p_Params ARRAY<STRING>
        )
        BEGIN
          DECLARE errcode INT64 DEFAULT 0;
          DECLARE ModulName STRING DEFAULT 'h_alis_sqlplus.ksh'; -- Original script name
          DECLARE ModulVersion STRING DEFAULT '1.0'; -- Placeholder, update as needed
          DECLARE v_script_sql STRING;
          DECLARE v_is_readable BOOLEAN;

          -- Parameter Validation
          IF p_Eintragsnr IS NULL OR p_Skript IS NULL THEN
            CALL `{DWMSG_MELDEFEHLER_PROC}`(
              p_Eintragsnr,
              'ERROR',
              196, -- Custom error code for missing parameters
              CONCAT('FEHLER: Eintragsnr oder Skriptname ist NULL. Modul: ', ModulName, ' Version: ', ModulVersion)
            );
            RETURN;
          END IF;

          -- Script Readability Check
          SELECT script_sql, is_readable
          INTO v_script_sql, v_is_readable
          FROM `{SCRIPT_REGISTRY_TABLE}`
          WHERE script_name = p_Skript
          LIMIT 1;

          IF v_script_sql IS NULL OR NOT v_is_readable THEN
            CALL `{DWMSG_MELDEFEHLER_PROC}`(
              p_Eintragsnr,
              'ERROR',
              201, -- Custom error code for script not found/readable
              CONCAT('FEHLER: Skript ', p_Skript, ' nicht gefunden oder nicht lesbar in script_registry. Modul: ', ModulName, ' Version: ', ModulVersion)
            );
            RETURN;
          END IF;

          -- Informational Output (using a SELECT statement for logging, or insert into invocation_log)
          -- This SELECT statement will produce a result set that the calling client could capture.
          SELECT CONCAT('INFO: Rufe SQL-Skript auf mit folgenden Einstellungen: Skript: ', p_Skript, ', Parameter: ', IF(ARRAY_LENGTH(p_Params) > 0, ARRAY_TO_STRING(p_Params, ' '), 'Keine'), '. Modul: ', ModulName, ' Version: ', ModulVersion);

          -- Dynamic SQL Execution
          BEGIN
            EXECUTE IMMEDIATE v_script_sql;
            SET errcode = 0;
          EXCEPTION WHEN ERROR THEN
            SET errcode = 1; -- Capture BigQuery execution error
            CALL `{DWMSG_MELDEFEHLER_PROC}`(
              p_Eintragsnr,
              'ERROR',
              errcode, -- BigQuery error code (or a mapped custom one if more specific)
              CONCAT('FEHLER: Fehler beim Ausführen des Skripts ', p_Skript, '. BigQuery Fehlermeldung: ', @@error.message, '. Modul: ', ModulName, ' Version: ', ModulVersion)
            );
          END;
        END;
    """)

# --- Pytest Fixture for Test Setup ---
@pytest.fixture(scope="module", autouse=True)
def bq_setup_teardown():
    """
    Fixture to set up BigQuery environment once per test session.
    """
    print("\nSetting up BigQuery environment...")
    setup_bq_environment()
    yield
    print("\nBigQuery environment setup complete. (No teardown for persistent objects)")

@pytest.fixture(autouse=True)
def clear_tables_before_each_test():
    """
    Fixture to clear test-specific tables before each test function.
    """
    clear_table(SCRIPT_REGISTRY_TABLE)
    clear_table(ERROR_LOG_TABLE)
    # Add a small delay to ensure tables are cleared before next test starts, if needed
    time.sleep(1)

```

### Test Case 1: Successful Script Execution

*   **Purpose:** Verify that `starteSQLSkript` successfully executes a valid BigQuery script and does not log any errors.
*   **Covers:** Output parity (absence of error output), Transformation correctness (dynamic execution), External-system replacements (script registry lookup).
*   **Setup:**
    *   The `script_registry` table contains an entry for `test_success_script` with valid BigQuery SQL (`SELECT 1 AS result;`) and `is_readable = TRUE`.
    *   The `error_log` table is empty.
*   **Action:** Call `project.dataset.starteSQLSkript` with `p_Eintragsnr = 'E001'`, `p_Skript = 'test_success_script'`, and an empty `p_Params` array.
*   **Pass/Fail Criterion:**
    *   The `error_log` table must contain exactly 0 rows.

```python
def test_successful_script_execution():
    """
    TC1: Verify successful execution of a valid BigQuery script.
    """
    # Setup: Populate script_registry with a simple, valid BigQuery script.
    execute_bq_query(f"""
        INSERT INTO `{SCRIPT_REGISTRY_TABLE}` (script_name, script_sql, is_readable, original_source_path)
        VALUES ('test_success_script', 'SELECT 1 AS result;', TRUE, '/path/to/original/script.sql');
    """)

    # Action: Call starteSQLSkript with valid parameters.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E001', 'test_success_script', [])

    # Pass/Fail: No errors in error_log.
    error_count = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count == 0, f"Expected 0 errors, but found {error_count} in error_log."
```

### Test Case 2: Missing `p_Eintragsnr` Parameter

*   **Purpose:** Verify `starteSQLSkript` correctly handles cases where the `p_Eintragsnr` parameter is `NULL`, mimicking the original script's `-z` check.
*   **Covers:** Transformation correctness (parameter validation), Output parity (error message content), External-system replacements (`DWMSG_MeldeFehler` replacement).
*   **Setup:** The `error_log` table is empty.
*   **Action:** Call `project.dataset.starteSQLSkript` with `p_Eintragsnr = NULL`, `p_Skript = 'some_script'`, and an empty `p_Params` array.
*   **Pass/Fail Criterion:**
    *   The `error_log` table must contain exactly 1 row.
    *   The entry's `error_code` must be `196`.
    *   The `severity` must be `'ERROR'`.
    *   The `message` must contain `'Eintragsnr oder Skriptname ist NULL'`.
    *   The `entry_number` in the log should be `NULL`.

```python
def test_missing_eintragsnr():
    """
    TC2: Verify error handling for missing p_Eintragsnr.
    """
    # Action: Call starteSQLSkript with p_Eintragsnr = NULL.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, None, 'some_script', [])

    # Pass/Fail: error_log contains one entry with error_code = 196.
    error_count = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count == 1, f"Expected 1 error, but found {error_count} in error_log."

    errors = get_error_log_entries()
    assert errors.iloc[0]['error_code'] == 196
    assert errors.iloc[0]['severity'] == 'ERROR'
    assert 'Eintragsnr oder Skriptname ist NULL' in errors.iloc[0]['message']
    assert pd.isna(errors.iloc[0]['entry_number']) # Check for pandas NaN for NULL
```

### Test Case 3: Missing `p_Skript` Parameter

*   **Purpose:** Verify `starteSQLSkript` correctly handles cases where the `p_Skript` parameter is `NULL`, mimicking the original script's `-z` check.
*   **Covers:** Transformation correctness (parameter validation), Output parity (error message content), External-system replacements (`DWMSG_MeldeFehler` replacement).
*   **Setup:** The `error_log` table is empty.
*   **Action:** Call `project.dataset.starteSQLSkript` with `p_Eintragsnr = 'E002'`, `p_Skript = NULL`, and an empty `p_Params` array.
*   **Pass/Fail Criterion:**
    *   The `error_log` table must contain exactly 1 row.
    *   The entry's `error_code` must be `196`.
    *   The `severity` must be `'ERROR'`.
    *   The `message` must contain `'Eintragsnr oder Skriptname ist NULL'`.
    *   The `entry_number` in the log should be `'E002'`.

```python
def test_missing_skript():
    """
    TC3: Verify error handling for missing p_Skript.
    """
    # Action: Call starteSQLSkript with p_Skript = NULL.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E002', None, [])

    # Pass/Fail: error_log contains one entry with error_code = 196.
    error_count = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count == 1, f"Expected 1 error, but found {error_count} in error_log."

    errors = get_error_log_entries()
    assert errors.iloc[0]['error_code'] == 196
    assert errors.iloc[0]['severity'] == 'ERROR'
    assert 'Eintragsnr oder Skriptname ist NULL' in errors.iloc[0]['message']
    assert errors.iloc[0]['entry_number'] == 'E002'
```

### Test Case 4: Script Not Found in `script_registry`

*   **Purpose:** Verify `starteSQLSkript` correctly handles a script name that does not exist in the `script_registry` table, replacing the original file existence check.
*   **Covers:** Transformation correctness (script lookup logic), External-system replacements (file system to `script_registry`).
*   **Setup:** The `script_registry` table does *not* contain an entry for `non_existent_script`. The `error_log` table is empty.
*   **Action:** Call `project.dataset.starteSQLSkript` with `p_Eintragsnr = 'E003'`, `p_Skript = 'non_existent_script'`, and an empty `p_Params` array.
*   **Pass/Fail Criterion:**
    *   The `error_log` table must contain exactly 1 row.
    *   The entry's `error_code` must be `201`.
    *   The `severity` must be `'ERROR'`.
    *   The `message` must contain `'Skript non_existent_script nicht gefunden oder nicht lesbar'`.

```python
def test_script_not_found():
    """
    TC4: Verify error handling when script is not found in script_registry.
    """
    # Action: Call starteSQLSkript with a non-existent p_Skript name.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E003', 'non_existent_script', [])

    # Pass/Fail: error_log contains one entry with error_code = 201.
    error_count = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count == 1, f"Expected 1 error, but found {error_count} in error_log."

    errors = get_error_log_entries()
    assert errors.iloc[0]['error_code'] == 201
    assert errors.iloc[0]['severity'] == 'ERROR'
    assert 'Skript non_existent_script nicht gefunden oder nicht lesbar' in errors.iloc[0]['message']
    assert errors.iloc[0]['entry_number'] == 'E003'
```

### Test Case 5: Script Not Readable in `script_registry`

*   **Purpose:** Verify `starteSQLSkript` correctly handles a script marked as `is_readable = FALSE` in the `script_registry` table, replacing the original file readability check (`-r`).
*   **Covers:** Transformation correctness (script lookup logic), External-system replacements (file system to `script_registry`).
*   **Setup:**
    *   The `script_registry` table contains an entry for `unreadable_script` with `is_readable = FALSE`.
    *   The `error_log` table is empty.
*   **Action:** Call `project.dataset.starteSQLSkript` with `p_Eintragsnr = 'E004'`, `p_Skript = 'unreadable_script'`, and an empty `p_Params` array.
*   **Pass/Fail Criterion:**
    *   The `error_log` table must contain exactly 1 row.
    *   The entry's `error_code` must be `201`.
    *   The `severity` must be `'ERROR'`.
    *   The `message` must contain `'Skript unreadable_script nicht gefunden oder nicht lesbar'`.

```python
def test_script_not_readable():
    """
    TC5: Verify error handling when script is marked as not readable.
    """
    # Setup: script_registry contains the script name, but is_readable = FALSE.
    execute_bq_query(f"""
        INSERT INTO `{SCRIPT_REGISTRY_TABLE}` (script_name, script_sql, is_readable, original_source_path)
        VALUES ('unreadable_script', 'SELECT 2 AS result;', FALSE, '/path/to/original/unreadable.sql');
    """)

    # Action: Call starteSQLSkript with the script name.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E004', 'unreadable_script', [])

    # Pass/Fail: error_log contains one entry with error_code = 201.
    error_count = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count == 1, f"Expected 1 error, but found {error_count} in error_log."

    errors = get_error_log_entries()
    assert errors.iloc[0]['error_code'] == 201
    assert errors.iloc[0]['severity'] == 'ERROR'
    assert 'Skript unreadable_script nicht gefunden oder nicht lesbar' in errors.iloc[0]['message']
    assert errors.iloc[0]['entry_number'] == 'E004'
```

### Test Case 6: Invoked Script Fails During Execution

*   **Purpose:** Verify `starteSQLSkript` correctly catches and logs errors that occur during the dynamic execution of the BigQuery script, replacing the original `sqlplus` exit code capture.
*   **Covers:** Transformation correctness (error handling, `EXECUTE IMMEDIATE`), Output parity (error message content).
*   **Setup:**
    *   The `script_registry` table contains an entry for `failing_script` with BigQuery SQL that will intentionally cause an error (e.g., `SELECT 1 / 0;`).
    *   The `error_log` table is empty.
*   **Action:** Call `project.dataset.starteSQLSkript` with `p_Eintragsnr = 'E005'`, `p_Skript = 'failing_script'`, and an empty `p_Params` array.
*   **Pass/Fail Criterion:**
    *   The `error_log` table must contain exactly 1 row.
    *   The entry's `error_code` must be `1` (the custom code for BigQuery execution errors).
    *   The `severity` must be `'ERROR'`.
    *   The `message` must contain `'Fehler beim Ausführen des Skripts failing_script'` and a BigQuery-specific error message like `'Division by zero'`.

```python
def test_invoked_script_fails():
    """
    TC6: Verify error handling when the dynamically invoked script fails.
    """
    # Setup: Populate script_registry with a BigQuery script that will intentionally fail.
    execute_bq_query(f"""
        INSERT INTO `{SCRIPT_REGISTRY_TABLE}` (script_name, script_sql, is_readable, original_source_path)
        VALUES ('failing_script', 'SELECT 1 / 0;', TRUE, '/path/to/original/failing.sql');
    """)

    # Action: Call starteSQLSkript with the failing script name.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E005', 'failing_script', [])

    # Pass/Fail: error_log contains one entry with error_code = 1 (BigQuery execution error).
    error_count = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count == 1, f"Expected 1 error, but found {error_count} in error_log."

    errors = get_error_log_entries()
    assert errors.iloc[0]['error_code'] == 1 # Custom error code for BQ execution failure
    assert errors.iloc[0]['severity'] == 'ERROR'
    assert 'Fehler beim Ausführen des Skripts failing_script' in errors.iloc[0]['message']
    assert 'Division by zero' in errors.iloc[0]['message'] # Specific BQ error message
    assert errors.iloc[0]['entry_number'] == 'E005'
```

### Test Case 7: Parameter Passing - Failure to Replicate Original Behavior (Gap)

*   **Purpose:** Demonstrate that the current `starteSQLSkript` implementation does *not* replicate the original behavior of passing parameters (`$*`) to the invoked SQL script. This highlights an "Unresolved Risk" from the design document.
*   **Covers:** Transformation correctness (identifying a critical gap in parameter handling).
*   **Setup:**
    *   The `script_registry` table contains an entry for `test_params_script` with BigQuery SQL that attempts to use a parameter (e.g., `SELECT @my_param;`). This script will fail if `@my_param` is not explicitly defined or passed.
    *   The `error_log` table is empty.
*   **Action:** Call `project.dataset.starteSQLSkript` with `p_Eintragsnr = 'E006'`, `p_Skript = 'test_params_script'`, and `p_Params = ['value1', 'value2']`.
*   **Pass/Fail Criterion:**
    *   The `error_log` table must contain exactly 1 row.
    *   The entry's `error_code` must be `1` (BigQuery execution error).
    *   The `severity` must be `'ERROR'`.
    *   The `message` must contain `'Fehler beim Ausführen des Skripts test_params_script'` and a BigQuery-specific error message indicating an undeclared variable (e.g., `'Undeclared variable: my_param'`). This confirms the parameters were *not* passed to the executed script.

```python
def test_parameter_passing_gap():
    """
    TC7: Demonstrate that the current migration does not pass parameters to the invoked script.
    This highlights an unresolved risk from the design document.
    """
    # Setup: Create a BigQuery script in script_registry that *expects* parameters
    # (e.g., a script that tries to use a variable that would be set by parameters).
    # This script will fail if the parameter is not passed/defined.
    execute_bq_query(f"""
        INSERT INTO `{SCRIPT_REGISTRY_TABLE}` (script_name, script_sql, is_readable, original_source_path)
        VALUES ('test_params_script', 'SELECT @my_param;', TRUE, '/path/to/original/param_script.sql');
    """)

    # Action: Call starteSQLSkript with p_Skript and p_Params.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E006', 'test_params_script', ['value1', 'value2'])

    # Pass/Fail: The error_log should contain an entry indicating a BigQuery execution error
    # (e.g., "Undeclared variable: my_param"), demonstrating the failure to pass parameters.
    error_count = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count == 1, f"Expected 1 error, but found {error_count} in error_log."

    errors = get_error_log_entries()
    assert errors.iloc[0]['error_code'] == 1 # Custom error code for BQ execution failure
    assert errors.iloc[0]['severity'] == 'ERROR'
    assert 'Fehler beim Ausführen des Skripts test_params_script' in errors.iloc[0]['message']
    assert 'Undeclared variable: my_param' in errors.iloc[0]['message'] # Specific BQ error message
    assert errors.iloc[0]['entry_number'] == 'E006'
```

### Test Case 8: Data Quality - `script_registry` Schema and Content

*   **Purpose:** Verify the schema of the `script_registry` table and ensure that inserted data conforms to expectations.
*   **Covers:** Data-quality / row-count / schema assertions.
*   **Setup:**
    *   The `script_registry` table is populated with various test entries, including valid and `is_readable=FALSE` scripts.
*   **Action:**
    *   Query `INFORMATION_SCHEMA.COLUMNS` to inspect the table schema.
    *   Query the `script_registry` table to inspect its content and row count.
*   **Pass/Fail Criterion:**
    *   The schema of `script_registry` must match `script_name STRING, script_sql STRING, is_readable BOOLEAN, original_source_path STRING`.
    *   The row count must match the number of inserted entries.
    *   The content of the rows (e.g., `is_readable` flag) must be as expected.

```python
def test_script_registry_schema_and_content():
    """
    TC8: Verify script_registry table schema and basic content.
    """
    # Setup: Populate script_registry with various entries.
    execute_bq_query(f"""
        INSERT INTO `{SCRIPT_REGISTRY_TABLE}` (script_name, script_sql, is_readable, original_source_path)
        VALUES
            ('script_a', 'SELECT CURRENT_DATE();', TRUE, '/a.sql'),
            ('script_b', 'CREATE TEMPORARY TABLE temp_data AS SELECT 1 AS id;', TRUE, '/b.sql'),
            ('script_c', 'INVALID SQL SYNTAX;', FALSE, '/c.sql');
    """)

    # Action: Query INFORMATION_SCHEMA.COLUMNS for script_registry and SELECT * FROM script_registry.
    schema_query = f"""
        SELECT column_name, data_type
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'script_registry'
        ORDER BY ordinal_position;
    """
    schema_df = execute_bq_query(schema_query).to_dataframe()
    content_df = execute_bq_query(f"SELECT * FROM `{SCRIPT_REGISTRY_TABLE}` ORDER BY script_name").to_dataframe()

    # Pass/Fail: Schema matches, row count is correct, and content is as expected.
    expected_schema = {
        'script_name': 'STRING',
        'script_sql': 'STRING',
        'is_readable': 'BOOL', # BigQuery uses BOOL for boolean
        'original_source_path': 'STRING'
    }
    assert len(schema_df) == len(expected_schema)
    for index, row in schema_df.iterrows():
        assert row['data_type'].upper() == expected_schema[row['column_name']].upper()

    assert len(content_df) == 3
    assert content_df.iloc[0]['script_name'] == 'script_a'
    assert content_df.iloc[0]['is_readable'] == True
    assert content_df.iloc[1]['script_name'] == 'script_b'
    assert content_df.iloc[1]['is_readable'] == True
    assert content_df.iloc[2]['script_name'] == 'script_c'
    assert content_df.iloc[2]['is_readable'] == False
```

### Test Case 9: Data Quality - `error_log` Schema and Content

*   **Purpose:** Verify the schema of the `error_log` table and ensure that error entries are correctly recorded with expected values.
*   **Covers:** Data-quality / row-count / schema assertions, Output parity (error logging).
*   **Setup:**
    *   Trigger multiple error conditions by calling `starteSQLSkript` with various invalid inputs.
*   **Action:**
    *   Query `INFORMATION_SCHEMA.COLUMNS` to inspect the table schema.
    *   Query the `error_log` table to inspect its content and row count.
*   **Pass/Fail Criterion:**
    *   The schema of `error_log` must match `entry_number STRING, severity STRING, error_code INT64, message STRING, timestamp TIMESTAMP`.
    *   The row count must match the number of triggered errors.
    *   The content of the rows (e.g., `error_code`, `severity`, `message`) must be as expected for each error type.

```python
def test_error_log_schema_and_content():
    """
    TC9: Verify error_log table schema and content after triggering errors.
    """
    # Setup: Trigger several error conditions.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, None, 'dummy_script_1', []) # Error 196
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E007', 'non_existent_script_2', []) # Error 201
    execute_bq_query(f"""
        INSERT INTO `{SCRIPT_REGISTRY_TABLE}` (script_name, script_sql, is_readable, original_source_path)
        VALUES ('failing_script_3', 'SELECT 1 / 0;', TRUE, '/path/to/original/failing3.sql');
    """)
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E008', 'failing_script_3', []) # Error 1 (BQ execution)

    # Action: Query INFORMATION_SCHEMA.COLUMNS for error_log and SELECT * FROM error_log.
    schema_query = f"""
        SELECT column_name, data_type
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'error_log'
        ORDER BY ordinal_position;
    """
    schema_df = execute_bq_query(schema_query).to_dataframe()
    content_df = get_error_log_entries()

    # Pass/Fail: Schema matches, row count is correct, and content is as expected.
    expected_schema = {
        'entry_number': 'STRING',
        'severity': 'STRING',
        'error_code': 'INT64',
        'message': 'STRING',
        'timestamp': 'TIMESTAMP'
    }
    assert len(schema_df) == len(expected_schema)
    for index, row in schema_df.iterrows():
        assert row['data_type'].upper() == expected_schema[row['column_name']].upper()

    assert len(content_df) == 3, f"Expected 3 error entries, got {len(content_df)}"
    # Check specific error codes and messages
    assert 196 in content_df['error_code'].values
    assert 201 in content_df['error_code'].values
    assert 1 in content_df['error_code'].values
    assert all(content_df['severity'] == 'ERROR')
    assert any('Eintragsnr oder Skriptname ist NULL' in msg for msg in content_df['message'])
    assert any('non_existent_script_2 nicht gefunden oder nicht lesbar' in msg for msg in content_df['message'])
    assert any('Fehler beim Ausführen des Skripts failing_script_3' in msg for msg in content_df['message'])
```

### Test Case 10: `NULL` and Empty Array Handling for `p_Params`

*   **Purpose:** Verify that `starteSQLSkript` gracefully handles `p_Params` being `NULL` or an empty array, as these are valid inputs for scripts that might not require parameters.
*   **Covers:** Transformation correctness (NULL handling, array handling).
*   **Setup:**
    *   The `script_registry` table contains an entry for `simple_script` with valid BigQuery SQL (`SELECT "executed" AS status;`).
    *   The `error_log` table is empty before each action.
*   **Action:**
    1.  Call `project.dataset.starteSQLSkript` with `p_Params = NULL`.
    2.  Call `project.dataset.starteSQLSkript` with `p_Params = []` (an empty array).
*   **Pass/Fail Criterion:**
    *   Both calls must complete successfully without logging any errors in the `error_log` table.

```python
def test_null_and_empty_params_array():
    """
    TC10: Verify handling of NULL and empty array for p_Params.
    """
    # Setup: Populate script_registry with a simple, valid BigQuery script.
    execute_bq_query(f"""
        INSERT INTO `{SCRIPT_REGISTRY_TABLE}` (script_name, script_sql, is_readable, original_source_path)
        VALUES ('simple_script', 'SELECT "executed" AS status;', TRUE, '/path/to/original/simple.sql');
    """)

    # Action 1: Call starteSQLSkript with p_Params = NULL.
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E009', 'simple_script', None)
    error_count_null = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count_null == 0, f"Expected 0 errors for NULL params, but found {error_count_null}."
    clear_table(ERROR_LOG_TABLE) # Clear for next action

    # Action 2: Call starteSQLSkript with p_Params = [] (empty array).
    call_bq_procedure(STARTE_SQL_SKRIPT_PROC, 'E010', 'simple_script', [])
    error_count_empty = get_table_row_count(ERROR_LOG_TABLE)
    assert error_count_empty == 0, f"Expected 0 errors for empty array params, but found {error_count_empty}."
```