The `h_alis_sqlplus.ksh` script is a utility wrapper, meaning its core function is to orchestrate the execution of other scripts and handle common concerns like parameter validation, file existence, and logging. The migration focuses on replacing these wrapper functionalities with BigQuery stored procedures and tables.

The tests below aim to ensure that the migrated BigQuery components (`starteSQLSkript` procedure, `sql_script_registry`, `execution_log`, `error_log` tables) exhibit behavior equivalent to the original KornShell script.

**Assumptions for Tests:**
*   `your_gcp_project` and `your_bq_dataset` are placeholders for your actual GCP project ID and BigQuery dataset name.
*   The DDL for `error_log`, `execution_log`, and `sql_script_registry` tables, along with the `starteSQLSkript` procedure and the dummy `test_target_procedure`, have been deployed to BigQuery.
*   The `sql_script_registry` table has been populated with the example DML provided in the migration design, including entries for `test/path/to/successful_script.sql`, `test/path/to/unreadable_script.sql`, and `test/path/to/failing_script.sql` pointing to `your_gcp_project.your_bq_dataset.test_target_procedure`.
*   Tests are written using `pytest` for structure and BigQuery SQL for assertions. You would typically use a BigQuery client library (e.g., `google-cloud-bigquery` in Python) to execute these SQL queries within your `pytest` setup.

---

## Test Case 1: Missing Required Parameters (`p_Eintragsnr` or `p_Skript`)

*   **Purpose**: Verify that the migrated `starteSQLSkript` procedure correctly handles calls with missing `p_Eintragsnr` or `p_Skript` parameters, mirroring the original shell script's `if [ -z ... ]` check and error code `196`.
*   **Output Parity**: The original script would call `DWMSG_MeldeFehler` with error code `196` and return `196`. The migrated procedure should raise an exception and log an error with code `196`.
*   **Transformation Correctness**: Validates the `IF p_Eintragsnr IS NULL OR p_Eintragsnr = '' OR p_Skript IS NULL OR p_Skript = '' THEN RAISE USING MESSAGE = '196'; END IF;` logic.

### Setup

1.  Ensure the `error_log` table is empty or clear previous entries for a clean test.
    ```sql
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
    ```

### Action

1.  Attempt to call `starteSQLSkript` with a `NULL` `p_Eintragsnr`.
    ```sql
    -- This call is expected to fail and raise an exception
    CALL `your_gcp_project.your_bq_dataset.starteSQLSkript`(NULL, 'some_script.sql', ['param1']);
    ```
2.  Attempt to call `starteSQLSkript` with an empty string `p_Skript`.
    ```sql
    -- This call is expected to fail and raise an exception
    CALL `your_gcp_project.your_bq_dataset.starteSQLSkript`('123', '', ['param1']);
    ```

### Pass/Fail Criterion

*   **Pass**:
    1.  Both `CALL` statements result in a BigQuery exception being raised.
    2.  The `error_log` table contains two new entries, each with `error_code` = `'196'`, `severity` = `'E'`, and a message indicating missing parameters.
*   **Fail**:
    1.  Either `CALL` statement completes without raising an exception.
    2.  The `error_log` table does not contain the expected entries or the `error_code` is incorrect.

### Runnable Test Code (Pytest / SQL Assertions)

```python
import pytest
from google.cloud import bigquery

# Assume bigquery_client is initialized and points to the correct project/dataset
# For example: bigquery_client = bigquery.Client(project='your_gcp_project')
# And dataset_id = 'your_bq_dataset'

@pytest.fixture(scope="module")
def bigquery_client():
    # Replace with your actual project ID and dataset ID
    project_id = "your_gcp_project"
    dataset_id = "your_bq_dataset"
    client = bigquery.Client(project=project_id)
    return client, dataset_id

def test_missing_required_parameters(bigquery_client):
    client, dataset_id = bigquery_client
    error_log_table = f"`{client.project}.{dataset_id}.error_log`"
    starte_sql_skript_proc = f"`{client.project}.{dataset_id}.starteSQLSkript`"

    # Clear error log before test
    client.query(f"TRUNCATE TABLE {error_log_table};").result()

    # Test 1: Missing p_Eintragsnr
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL {starte_sql_skript_proc}(NULL, 'some_script.sql', ['param1']);").result()
    assert "196" in str(excinfo.value)

    # Test 2: Missing p_Skript
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL {starte_sql_skript_proc}('123', '', ['param1']);").result()
    assert "196" in str(excinfo.value)

    # Verify error_log entries
    query = f"""
        SELECT error_code, severity, message
        FROM {error_log_table}
        WHERE error_code = '196'
        ORDER BY created_at
    """
    rows = list(client.query(query).result())

    assert len(rows) == 2
    assert rows[0].error_code == '196'
    assert rows[0].severity == 'E'
    assert "Missing required parameters" in rows[0].message
    assert rows[1].error_code == '196'
    assert rows[1].severity == 'E'
    assert "Missing required parameters" in rows[1].message

```

---

## Test Case 2: Script Not Found in Registry

*   **Purpose**: Verify that the migrated `starteSQLSkript` procedure correctly handles a script name that is not present in the `sql_script_registry` table, replacing the original filesystem check for non-existent files.
*   **External-system replacements**: Validates the lookup in `sql_script_registry`.
*   **Output Parity**: The original script would call `DWMSG_MeldeFehler` with error code `201` (for non-existent/unreadable). The migrated procedure should raise an exception and log an error with a custom code `200` (for "not found").

### Setup

1.  Ensure the `error_log` table is empty or clear previous entries.
    ```sql
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
    ```

### Action

1.  Attempt to call `starteSQLSkript` with a `p_Skript` value that does not exist in `sql_script_registry`.
    ```sql
    -- This call is expected to fail and raise an exception
    CALL `your_gcp_project.your_bq_dataset.starteSQLSkript`('456', 'non_existent_script.sql', ['arg1', 'arg2']);
    ```

### Pass/Fail Criterion

*   **Pass**:
    1.  The `CALL` statement results in a BigQuery exception being raised.
    2.  The `error_log` table contains one new entry with `error_code` = `'200'`, `severity` = `'E'`, and a message indicating the script was not found in the registry.
*   **Fail**:
    1.  The `CALL` statement completes without raising an exception.
    2.  The `error_log` table does not contain the expected entry or the `error_code` is incorrect.

### Runnable Test Code (Pytest / SQL Assertions)

```python
import pytest
from google.cloud import bigquery

def test_script_not_found_in_registry(bigquery_client):
    client, dataset_id = bigquery_client
    error_log_table = f"`{client.project}.{dataset_id}.error_log`"
    starte_sql_skript_proc = f"`{client.project}.{dataset_id}.starteSQLSkript`"

    # Clear error log before test
    client.query(f"TRUNCATE TABLE {error_log_table};").result()

    # Attempt to call with a non-existent script
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL {starte_sql_skript_proc}('456', 'non_existent_script.sql', ['arg1', 'arg2']);").result()
    assert "200" in str(excinfo.value) # Custom error code for not found

    # Verify error_log entry
    query = f"""
        SELECT error_code, severity, message
        FROM {error_log_table}
        WHERE entry_nr = '456'
    """
    rows = list(client.query(query).result())

    assert len(rows) == 1
    assert rows[0].error_code == '200'
    assert rows[0].severity == 'E'
    assert "not found in registry" in rows[0].message

```

---

## Test Case 3: Script Found but Not Readable/Executable in Registry

*   **Purpose**: Verify that the migrated `starteSQLSkript` procedure correctly handles a script found in the `sql_script_registry` but marked with `is_readable = FALSE`, replacing the original filesystem check for non-readable files.
*   **External-system replacements**: Validates the `is_readable` check in `sql_script_registry`.
*   **Output Parity**: The original script would call `DWMSG_MeldeFehler` with error code `201`. The migrated procedure should raise an exception and log an error with code `201`.

### Setup

1.  Ensure the `error_log` table is empty or clear previous entries.
    ```sql
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
    ```
2.  Ensure `sql_script_registry` contains an entry for `'test/path/to/unreadable_script.sql'` with `is_readable = FALSE`.
    ```sql
    -- Example DML (should be part of your initial setup)
    INSERT INTO `your_gcp_project.your_bq_dataset.sql_script_registry` (script_name, is_readable, target_procedure_name, description)
    VALUES ('test/path/to/unreadable_script.sql', FALSE, 'your_gcp_project.your_bq_dataset.test_target_procedure', 'A dummy script marked as unreadable for tests');
    ```

### Action

1.  Attempt to call `starteSQLSkript` with `p_Skript` = `'test/path/to/unreadable_script.sql'`.
    ```sql
    -- This call is expected to fail and raise an exception
    CALL `your_gcp_project.your_bq_dataset.starteSQLSkript`('789', 'test/path/to/unreadable_script.sql', ['param_a']);
    ```

### Pass/Fail Criterion

*   **Pass**:
    1.  The `CALL` statement results in a BigQuery exception being raised.
    2.  The `error_log` table contains one new entry with `error_code` = `'201'`, `severity` = `'E'`, and a message indicating the script is not readable/executable.
*   **Fail**:
    1.  The `CALL` statement completes without raising an exception.
    2.  The `error_log` table does not contain the expected entry or the `error_code` is incorrect.

### Runnable Test Code (Pytest / SQL Assertions)

```python
import pytest
from google.cloud import bigquery

def test_script_not_readable_in_registry(bigquery_client):
    client, dataset_id = bigquery_client
    error_log_table = f"`{client.project}.{dataset_id}.error_log`"
    starte_sql_skript_proc = f"`{client.project}.{dataset_id}.starteSQLSkript`"

    # Clear error log before test
    client.query(f"TRUNCATE TABLE {error_log_table};").result()

    # Ensure the unreadable script is in the registry
    client.query(f"""
        INSERT INTO `{client.project}.{dataset_id}.sql_script_registry` (script_name, is_readable, target_procedure_name, description)
        VALUES ('test/path/to/unreadable_script.sql', FALSE, '{client.project}.{dataset_id}.test_target_procedure', 'A dummy script marked as unreadable for tests')
        OPTIONS(merge_type='MERGE_INTO') -- Use MERGE_INTO to handle potential existing entries
    """).result()

    # Attempt to call with an unreadable script
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL {starte_sql_skript_proc}('789', 'test/path/to/unreadable_script.sql', ['param_a']);").result()
    assert "201" in str(excinfo.value)

    # Verify error_log entry
    query = f"""
        SELECT error_code, severity, message
        FROM {error_log_table}
        WHERE entry_nr = '789'
    """
    rows = list(client.query(query).result())

    assert len(rows) == 1
    assert rows[0].error_code == '201'
    assert rows[0].severity == 'E'
    assert "not marked as readable/executable" in rows[0].message

```

---

## Test Case 4: Successful Execution of a Migrated Script

*   **Purpose**: Validate the end-to-end successful execution flow, including correct parameter passing, logging, and the successful invocation of the target BigQuery procedure.
*   **Output Parity**: The original script would print `echo` messages and return `0`. The migrated procedure should log execution details and complete without error.
*   **Transformation Correctness**: Validates the dynamic `CALL` statement and correct handling of the `p_Params` array.

### Setup

1.  Ensure `execution_log` and `error_log` tables are empty or clear previous entries.
    ```sql
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.execution_log`;
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
    ```
2.  Ensure `sql_script_registry` contains an entry for `'test/path/to/successful_script.sql'` with `is_readable = TRUE` pointing to `your_gcp_project.your_bq_dataset.test_target_procedure`.
    ```sql
    -- Example DML (should be part of your initial setup)
    INSERT INTO `your_gcp_project.your_bq_dataset.sql_script_registry` (script_name, is_readable, target_procedure_name, description)
    VALUES ('test/path/to/successful_script.sql', TRUE, 'your_gcp_project.your_bq_dataset.test_target_procedure', 'A dummy script for successful execution tests');
    ```

### Action

1.  Call `starteSQLSkript` with valid parameters for a successful script.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.starteSQLSkript`('101', 'test/path/to/successful_script.sql', ['param_x', 'param_y', '123']);
    ```

### Pass/Fail Criterion

*   **Pass**:
    1.  The `CALL` statement completes successfully without raising any exceptions.
    2.  The `error_log` table remains empty.
    3.  The `execution_log` table contains three new entries:
        *   One from `starteSQLSkript` for starting execution.
        *   One from `test_target_procedure` for being invoked.
        *   One from `test_target_procedure` for successful completion.
        *   One from `starteSQLSkript` for successful completion.
    4.  All log entries correctly reflect the `entry_nr`, `script_name`, and `script_params` passed.
*   **Fail**:
    1.  The `CALL` statement raises an exception.
    2.  The `error_log` table contains entries.
    3.  The `execution_log` table does not contain the expected entries or the parameter values are incorrect.

### Runnable Test Code (Pytest / SQL Assertions)

```python
import pytest
from google.cloud import bigquery

def test_successful_script_execution(bigquery_client):
    client, dataset_id = bigquery_client
    execution_log_table = f"`{client.project}.{dataset_id}.execution_log`"
    error_log_table = f"`{client.project}.{dataset_id}.error_log`"
    starte_sql_skript_proc = f"`{client.project}.{dataset_id}.starteSQLSkript`"

    # Clear logs before test
    client.query(f"TRUNCATE TABLE {execution_log_table};").result()
    client.query(f"TRUNCATE TABLE {error_log_table};").result()

    # Ensure the successful script is in the registry
    client.query(f"""
        INSERT INTO `{client.project}.{dataset_id}.sql_script_registry` (script_name, is_readable, target_procedure_name, description)
        VALUES ('test/path/to/successful_script.sql', TRUE, '{client.project}.{dataset_id}.test_target_procedure', 'A dummy script for successful execution tests')
        OPTIONS(merge_type='MERGE_INTO')
    """).result()

    entry_nr = '101'
    script_name = 'test/path/to/successful_script.sql'
    params = ['param_x', 'param_y', '123']
    params_str = "ARRAY['param_x', 'param_y', '123']"

    # Execute the procedure
    client.query(f"CALL {starte_sql_skript_proc}('{entry_nr}', '{script_name}', {params_str});").result()

    # Verify no errors
    error_rows = list(client.query(f"SELECT * FROM {error_log_table} WHERE entry_nr = '{entry_nr}';").result())
    assert len(error_rows) == 0

    # Verify execution_log entries
    query = f"""
        SELECT module_name, script_name, script_params, log_message
        FROM {execution_log_table}
        WHERE entry_nr = '{entry_nr}'
        ORDER BY created_at
    """
    rows = list(client.query(query).result())

    assert len(rows) == 4 # starteSQLSkript start, test_target_procedure invoked, test_target_procedure success, starteSQLSkript success

    # Check starteSQLSkript start log
    assert rows[0].module_name == 'starteSQLSkript'
    assert rows[0].script_name == script_name
    assert rows[0].script_params == params
    assert "Starting execution" in rows[0].log_message

    # Check test_target_procedure invoked log
    assert rows[1].module_name == 'test_target_procedure'
    assert rows[1].script_name == 'test_target_procedure_script' # Name from inside the dummy proc
    assert rows[1].script_params == params
    assert "Test target procedure invoked" in rows[1].log_message

    # Check test_target_procedure success log
    assert rows[2].module_name == 'test_target_procedure'
    assert rows[2].script_name == 'test_target_procedure_script'
    assert rows[2].script_params == params
    assert "Test target procedure completed successfully" in rows[2].log_message

    # Check starteSQLSkript success log
    assert rows[3].module_name == 'starteSQLSkript'
    assert rows[3].script_name == script_name
    assert rows[3].script_params == params
    assert "Successfully executed target procedure" in rows[3].log_message

```

---

## Test Case 5: Failure During Execution of a Migrated Script

*   **Purpose**: Validate that errors originating from the called BigQuery procedure are correctly caught, logged, and re-raised by `starteSQLSkript`, mimicking the original script's capture of `sqlplus` exit codes.
*   **Output Parity**: The original script would return the non-zero exit code of `sqlplus`. The migrated procedure should log the error and re-raise the exception.
*   **Transformation Correctness**: Validates the `BEGIN...EXCEPTION...END` block for error handling.

### Setup

1.  Ensure `execution_log` and `error_log` tables are empty or clear previous entries.
    ```sql
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.execution_log`;
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
    ```
2.  Ensure `sql_script_registry` contains an entry for `'test/path/to/failing_script.sql'` with `is_readable = TRUE` pointing to `your_gcp_project.your_bq_dataset.test_target_procedure`.
    ```sql
    -- Example DML (should be part of your initial setup)
    INSERT INTO `your_gcp_project.your_bq_dataset.sql_script_registry` (script_name, is_readable, target_procedure_name, description)
    VALUES ('test/path/to/failing_script.sql', TRUE, 'your_gcp_project.your_bq_dataset.test_target_procedure', 'A dummy script for failing execution tests');
    ```

### Action

1.  Call `starteSQLSkript` with parameters that will cause the `test_target_procedure` to fail (e.g., by passing `'FAIL_ME'` as a parameter).
    ```sql
    -- This call is expected to fail and raise an exception
    CALL `your_gcp_project.your_bq_dataset.starteSQLSkript`('202', 'test/path/to/failing_script.sql', ['FAIL_ME', 'other_param']);
    ```

### Pass/Fail Criterion

*   **Pass**:
    1.  The `CALL` statement results in a BigQuery exception being raised by `starteSQLSkript`.
    2.  The `error_log` table contains two new entries:
        *   One from `test_target_procedure` with `error_code` = `'TEST_FAIL_CODE'` (or similar from the dummy proc).
        *   One from `starteSQLSkript` with an error code indicating the failure of the target procedure.
    3.  The `execution_log` table contains entries for the start of `starteSQLSkript` and the invocation of `test_target_procedure`, but no successful completion log for either.
*   **Fail**:
    1.  The `CALL` statement completes successfully.
    2.  The `error_log` table does not contain the expected entries or the error details are incorrect.
    3.  The `execution_log` shows successful completion.

### Runnable Test Code (Pytest / SQL Assertions)

```python
import pytest
from google.cloud import bigquery

def test_failing_script_execution(bigquery_client):
    client, dataset_id = bigquery_client
    execution_log_table = f"`{client.project}.{dataset_id}.execution_log`"
    error_log_table = f"`{client.project}.{dataset_id}.error_log`"
    starte_sql_skript_proc = f"`{client.project}.{dataset_id}.starteSQLSkript`"

    # Clear logs before test
    client.query(f"TRUNCATE TABLE {execution_log_table};").result()
    client.query(f"TRUNCATE TABLE {error_log_table};").result()

    # Ensure the failing script is in the registry
    client.query(f"""
        INSERT INTO `{client.project}.{dataset_id}.sql_script_registry` (script_name, is_readable, target_procedure_name, description)
        VALUES ('test/path/to/failing_script.sql', TRUE, '{client.project}.{dataset_id}.test_target_procedure', 'A dummy script for failing execution tests')
        OPTIONS(merge_type='MERGE_INTO')
    """).result()

    entry_nr = '202'
    script_name = 'test/path/to/failing_script.sql'
    params = ['FAIL_ME', 'other_param']
    params_str = "ARRAY['FAIL_ME', 'other_param']"

    # Execute the procedure, expecting an exception
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL {starte_sql_skript_proc}('{entry_nr}', '{script_name}', {params_str});").result()
    assert "Execution of target procedure failed" in str(excinfo.value)

    # Verify error_log entries
    error_query = f"""
        SELECT module_name, error_code, severity, message
        FROM {error_log_table}
        WHERE entry_nr = '{entry_nr}'
        ORDER BY created_at
    """
    error_rows = list(client.query(error_query).result())

    assert len(error_rows) == 2 # One from test_target_procedure, one from starteSQLSkript
    assert error_rows[0].module_name == 'test_target_procedure'
    assert error_rows[0].error_code == 'TEST_FAIL_CODE'
    assert "Simulated failure" in error_rows[0].message

    assert error_rows[1].module_name == 'starteSQLSkript'
    assert "Execution of target procedure failed" in error_rows[1].message

    # Verify execution_log entries (should only have start and target proc invocation)
    exec_query = f"""
        SELECT module_name, script_name, script_params, log_message
        FROM {execution_log_table}
        WHERE entry_nr = '{entry_nr}'
        ORDER BY created_at
    """
    exec_rows = list(client.query(exec_query).result())

    assert len(exec_rows) == 2 # starteSQLSkript start, test_target_procedure invoked
    assert exec_rows[0].module_name == 'starteSQLSkript'
    assert "Starting execution" in exec_rows[0].log_message
    assert exec_rows[1].module_name == 'test_target_procedure'
    assert "Test target procedure invoked" in exec_rows[1].log_message

```

---

## Test Case 6: Logging Content Verification (Output Parity & Data Quality)

*   **Purpose**: Verify that `execution_log` and `error_log` tables capture accurate and complete information, similar to the `echo` statements and `DWMSG_MeldeFehler` function in the original script. This covers various data types and NULL handling for log fields.
*   **Data-quality / row-count / schema assertions**: Checks the content of log tables.

### Setup

1.  Ensure `execution_log` and `error_log` tables are empty or clear previous entries.
    ```sql
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.execution_log`;
    TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
    ```
2.  Perform a successful and a failing execution (as in Test Cases 4 and 5) to populate the logs.

### Action

1.  Query the `execution_log` table for entries related to the successful execution.
2.  Query the `error_log` table for entries related to the failing execution.
3.  Examine the content of `script_params` for correct array handling, including empty arrays and parameters with special characters (if applicable).

### Pass/Fail Criterion

*   **Pass**:
    1.  All fields in `execution_log` (e.g., `module_name`, `module_version`, `entry_nr`, `script_name`, `script_params`, `log_message`, `created_at`) are correctly populated for successful runs.
    2.  All fields in `error_log` (e.g., `entry_nr`, `severity`, `error_code`, `message`, `module_name`, `module_version`, `created_at`) are correctly populated for failed runs.
    3.  `script_params` (an `ARRAY<STRING>`) correctly stores the passed parameters, including empty arrays or parameters with special characters (e.g., spaces, quotes) if tested.
*   **Fail**:
    1.  Any log field is `NULL` when it should be populated, or contains incorrect data.
    2.  `script_params` array is malformed or missing elements.

### Runnable Test Code (Pytest / SQL Assertions)

```python
import pytest
from google.cloud import bigquery

def test_logging_content_verification(bigquery_client):
    client, dataset_id = bigquery_client
    execution_log_table = f"`{client.project}.{dataset_id}.execution_log`"
    error_log_table = f"`{client.project}.{dataset_id}.error_log`"
    starte_sql_skript_proc = f"`{client.project}.{dataset_id}.starteSQLSkript`"

    # Clear logs before test
    client.query(f"TRUNCATE TABLE {execution_log_table};").result()
    client.query(f"TRUNCATE TABLE {error_log_table};").result()

    # Ensure registry entries exist
    client.query(f"""
        INSERT INTO `{client.project}.{dataset_id}.sql_script_registry` (script_name, is_readable, target_procedure_name, description)
        VALUES
            ('test/path/to/successful_script.sql', TRUE, '{client.project}.{dataset_id}.test_target_procedure', 'A dummy script for successful execution tests'),
            ('test/path/to/failing_script.sql', TRUE, '{client.project}.{dataset_id}.test_target_procedure', 'A dummy script for failing execution tests'),
            ('test/path/to/empty_params_script.sql', TRUE, '{client.project}.{dataset_id}.test_target_procedure', 'A dummy script for empty params test')
        OPTIONS(merge_type='MERGE_INTO')
    """).result()

    # Perform a successful execution
    success_entry_nr = '301'
    success_script_name = 'test/path/to/successful_script.sql'
    success_params = ['param with spaces', 'param_with_quotes\'', '123']
    success_params_str = "ARRAY['param with spaces', 'param_with_quotes\\'', '123']"
    client.query(f"CALL {starte_sql_skript_proc}('{success_entry_nr}', '{success_script_name}', {success_params_str});").result()

    # Perform a failing execution
    fail_entry_nr = '302'
    fail_script_name = 'test/path/to/failing_script.sql'
    fail_params = ['FAIL_ME', 'another_param']
    fail_params_str = "ARRAY['FAIL_ME', 'another_param']"
    with pytest.raises(Exception):
        client.query(f"CALL {starte_sql_skript_proc}('{fail_entry_nr}', '{fail_script_name}', {fail_params_str});").result()

    # Perform an execution with empty parameters
    empty_params_entry_nr = '303'
    empty_params_script_name = 'test/path/to/empty_params_script.sql'
    empty_params = []
    empty_params_str = "ARRAY<STRING>[]"
    client.query(f"CALL {starte_sql_skript_proc}('{empty_params_entry_nr}', '{empty_params_script_name}', {empty_params_str});").result()


    # Verify successful execution logs
    success_exec_query = f"""
        SELECT module_name, module_version, entry_nr, script_name, script_params, log_message, created_at
        FROM {execution_log_table}
        WHERE entry_nr = '{success_entry_nr}'
        ORDER BY created_at
    """
    success_exec_rows = list(client.query(success_exec_query).result())
    assert len(success_exec_rows) == 4 # 4 logs for a successful run

    # Check first log entry (starteSQLSkript start)
    assert success_exec_rows[0].module_name == 'starteSQLSkript'
    assert success_exec_rows[0].module_version == '1.0'
    assert success_exec_rows[0].entry_nr == success_entry_nr
    assert success_exec_rows[0].script_name == success_script_name
    assert success_exec_rows[0].script_params == success_params
    assert "Starting execution" in success_exec_rows[0].log_message
    assert success_exec_rows[0].created_at is not None

    # Check empty params execution logs
    empty_params_exec_query = f"""
        SELECT module_name, script_params, log_message
        FROM {execution_log_table}
        WHERE entry_nr = '{empty_params_entry_nr}'
        ORDER BY created_at
    """
    empty_params_exec_rows = list(client.query(empty_params_exec_query).result())
    assert len(empty_params_exec_rows) == 4
    assert empty_params_exec_rows[0].script_params == []
    assert "p_Params = []" in empty_params_exec_rows[0].log_message
    assert empty_params_exec_rows[1].script_params == []
    assert "invoked with parameters." in empty_params_exec_rows[1].log_message


    # Verify failing execution error logs
    fail_error_query = f"""
        SELECT entry_nr, severity, error_code, message, module_name, module_version, created_at
        FROM {error_log_table}
        WHERE entry_nr = '{fail_entry_nr}'
        ORDER BY created_at
    """
    fail_error_rows = list(client.query(fail_error_query).result())
    assert len(fail_error_rows) == 2 # 2 error logs for a failed run

    # Check first error log entry (from test_target_procedure)
    assert fail_error_rows[0].entry_nr == fail_entry_nr
    assert fail_error_rows[0].severity == 'E'
    assert fail_error_rows[0].error_code == 'TEST_FAIL_CODE'
    assert "Simulated failure" in fail_error_rows[0].message
    assert fail_error_rows[0].module_name == 'test_target_procedure'
    assert fail_error_rows[0].module_version == '1.0'
    assert fail_error_rows[0].created_at is not None

    # Check second error log entry (from starteSQLSkript)
    assert fail_error_rows[1].entry_nr == fail_entry_nr
    assert fail_error_rows[1].severity == 'E'
    assert "Execution of target procedure failed" in fail_error_rows[1].message
    assert fail_error_rows[1].module_name == 'starteSQLSkript'
    assert fail_error_rows[1].module_version == '1.0'
    assert fail_error_rows[1].created_at is not None

```

---

## Test Case 7: `sql_script_registry` Schema and Initial Data

*   **Purpose**: Verify the structural integrity and initial population of the `sql_script_registry` table, which replaces the filesystem for script existence checks.
*   **Data-quality / row-count / schema assertions**: Checks DDL and DML.

### Setup

1.  Ensure the `sql_script_registry` table has been created and populated with the initial DML as specified in the migration design.

### Action

1.  Query the table schema to verify column names and types.
2.  Query the table content to verify initial data.

### Pass/Fail Criterion

*   **Pass**:
    1.  The `sql_script_registry` table exists.
    2.  It has the expected columns: `script_name` (STRING, NOT NULL), `is_readable` (BOOLEAN, NOT NULL), `target_procedure_name` (STRING, NOT NULL), `description` (STRING), `created_at` (TIMESTAMP), `updated_at` (TIMESTAMP).
    3.  The `script_name` column is correctly identified as the primary key (or unique).
    4.  The table contains at least the initial example entries provided in the DML, with correct values.
*   **Fail**:
    1.  The table does not exist or has an incorrect schema.
    2.  Initial data is missing or incorrect.

### Runnable Test Code (Pytest / SQL Assertions)

```python
import pytest
from google.cloud import bigquery

def test_sql_script_registry_schema_and_data(bigquery_client):
    client, dataset_id = bigquery_client
    registry_table = f"`{client.project}.{dataset_id}.sql_script_registry`"

    # 1. Verify table existence and schema
    table = client.get_table(f"{client.project}.{dataset_id}.sql_script_registry")
    assert table is not None

    # Check column names and types
    expected_schema = {
        "script_name": "STRING",
        "is_readable": "BOOLEAN",
        "target_procedure_name": "STRING",
        "description": "STRING",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    }
    actual_schema = {field.name: field.field_type for field in table.schema}
    assert actual_schema == expected_schema

    # Check NOT NULL constraints (BigQuery schema doesn't directly expose this for all types,
    # but we can infer from DDL or test data integrity)
    # For simplicity, we'll rely on the DDL for NOT NULL, and test data for content.

    # 2. Verify initial data population
    query = f"""
        SELECT script_name, is_readable, target_procedure_name, description
        FROM {registry_table}
        WHERE script_name IN (
            'vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_bestandsdaten.sql',
            'test/path/to/successful_script.sql',
            'test/path/to/unreadable_script.sql',
            'test/path/to/failing_script.sql',
            'test/path/to/empty_params_script.sql'
        )
        ORDER BY script_name
    """
    rows = list(client.query(query).result())

    assert len(rows) >= 5 # At least the example entries and test entries

    # Verify specific entries
    script_names = [row.script_name for row in rows]
    assert 'vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_bestandsdaten.sql' in script_names
    assert 'test/path/to/successful_script.sql' in script_names
    assert 'test/path/to/unreadable_script.sql' in script_names
    assert 'test/path/to/failing_script.sql' in script_names
    assert 'test/path/to/empty_params_script.sql' in script_names

    # Example: Check 'test/path/to/unreadable_script.sql' entry
    unreadable_script_row = next(row for row in rows if row.script_name == 'test/path/to/unreadable_script.sql')
    assert unreadable_script_row.is_readable is False
    assert unreadable_script_row.target_procedure_name == f'{client.project}.{dataset_id}.test_target_procedure'
    assert "unreadable" in unreadable_script_row.description

    # Example: Check 'test/path/to/successful_script.sql' entry
    successful_script_row = next(row for row in rows if row.script_name == 'test/path/to/successful_script.sql')
    assert successful_script_row.is_readable is True
    assert successful_script_row.target_procedure_name == f'{client.project}.{dataset_id}.test_target_procedure'
    assert "successful execution" in successful_script_row.description

```