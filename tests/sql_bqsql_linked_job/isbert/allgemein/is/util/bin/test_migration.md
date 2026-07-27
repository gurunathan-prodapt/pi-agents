Here is a comprehensive suite of migration-validation test cases designed to verify that the migrated Python module (`h_alis_sqlplus.py`) is behaviorally equivalent to the legacy KornShell script (`h_alis_sqlplus.ksh`).

---

# Test Case 1: Parameter Validation (Missing Arguments)

### Purpose
Verify that the function rejects empty or missing mandatory parameters (`p_eintragsnr` or `p_skript`) with exit code `196` and triggers the external error logger `DWMSG_MeldeFehler` with the exact legacy error message format.

### Setup
- Mock the external error reporting function `_dwmsg_melde_fehler` to track its invocations.
- Ensure no environment variables interfere with basic validation.

### Action
1. Call `starte_sql_skript("", "some_script.sql")` (missing entry number).
2. Call `starte_sql_skript("12345", "")` (missing script path).

### Pass/Fail Criterion
- **Pass**: Both calls return `196`. The mock for `_dwmsg_melde_fehler` is called with parameters: `("E", 196, "alis_sqlplus V1.1.3 starteSQLSkript")`.
- **Fail**: Any call returns a code other than `196`, or the error message does not match the legacy string exactly.

---

# Test Case 2: File Readability Validation

### Purpose
Verify that the function checks for the existence and readability of the target SQL script before execution, returning `201` and calling the error logger if the file is missing or unreadable.

### Setup
- Mock `_dwmsg_melde_fehler`.
- Define a path to a non-existent file (e.g., `/tmp/non_existent_file_999.sql`).

### Action
Call `starte_sql_skript("12345", "/tmp/non_existent_file_999.sql")`.

### Pass/Fail Criterion
- **Pass**: The function returns `201`. The mock for `_dwmsg_melde_fehler` is called with parameters: `("12345", "E", 201, "/tmp/non_existent_file_999.sql")`.
- **Fail**: The function returns any other code, or attempts to execute a non-existent file.

---

# Test Case 3: Output Parity (Metadata Logging)

### Purpose
Verify that the exact German log messages and parameter listings are printed to standard output during execution, matching the legacy shell script's output format.

### Setup
- Create a temporary valid SQL file.
- Mock the BigQuery client to prevent actual network calls.
- Capture standard output (`sys.stdout`).

### Action
Call `starte_sql_skript("12345", "/tmp/test_script.sql", "param_a", "param_b")`.

### Pass/Fail Criterion
- **Pass**: Captured stdout contains exactly:
  ```normal
  Rufe SQL*PLUS auf mit folgenden Einstellungen
  Sql*Plus-Skript : /tmp/test_script.sql
  Skript-Parameter: param_a param_b
  ```
- **Fail**: The output is missing, formatted differently, or does not preserve the original German literals.

---

# Test Case 4: BigQuery SQL Transformation (Directives & Parameters)

### Purpose
Verify that when executing via BigQuery (`USE_BIGQUERY=True`):
1. Legacy Oracle SQL*Plus positional parameters (`&1`, `&&1`, `&2`, etc.) are correctly substituted with the passed arguments.
2. Proprietary SQL*Plus directives (e.g., `SET`, `WHENEVER`, `EXIT`, `COLUMN`) are stripped out to prevent BigQuery syntax errors.

### Setup
- Set environment variable `USE_BIGQUERY="True"`.
- Mock `google.cloud.bigquery.Client`.
- Create a temporary SQL file containing:
  ```sql
  SET PAGESIZE 100
  WHENEVER SQLERROR EXIT 1
  SELECT * FROM `my_project.my_dataset.my_table` 
  WHERE status = '&1' AND category = '&&2';
  EXIT;
  ```

### Action
Call `starte_sql_skript("12345", "/tmp/test_transform.sql", "ACTIVE", "FINANCE")`.

### Pass/Fail Criterion
- **Pass**: The query string passed to the mocked BigQuery `client.query()` is cleaned and substituted as:
  ```sql
  SELECT * FROM `my_project.my_dataset.my_table` 
  WHERE status = 'ACTIVE' AND category = 'FINANCE';
  ```
- **Fail**: SQL*Plus directives remain in the query string, or parameter placeholders are not correctly substituted.

---

# Test Case 5: BigQuery Execution Success and Failure Handling

### Purpose
Verify that a successful BigQuery job returns `0`, and any BigQuery execution failure (e.g., syntax error, permission issue) is caught and returns `1`.

### Setup
- Set environment variable `USE_BIGQUERY="True"`.
- Create a temporary valid SQL file.
- **Scenario A**: Mock `bigquery.Client` to return a successful job.
- **Scenario B**: Mock `bigquery.Client` to raise a `GoogleCloudError` exception.

### Action
1. Run Scenario A.
2. Run Scenario B.

### Pass/Fail Criterion
- **Pass**: Scenario A returns `0`. Scenario B returns `1` and prints the error traceback to `sys.stderr`.
- **Fail**: Scenario A returns non-zero, or Scenario B raises an uncaught exception or returns an incorrect code.

---

# Test Case 6: Legacy Fallback Path (SQL*Plus Subprocess)

### Purpose
Verify that if `USE_BIGQUERY` is set to `False` (or `DW_ORAUSER` is provided), the script falls back to executing the legacy `sqlplus` binary via a subprocess, passing arguments and redirecting standard input to `/dev/null`.

### Setup
- Set environment variable `USE_BIGQUERY="False"`.
- Set environment variable `DW_ORAUSER="scott/tiger@orcl"`.
- Mock `subprocess.run` to return a mock process with `returncode=0`.
- Create a temporary valid SQL file.

### Action
Call `starte_sql_skript("12345", "/tmp/test_legacy.sql", "arg1", "arg2")`.

### Pass/Fail Criterion
- **Pass**: `subprocess.run` is called with:
  - Command: `['sqlplus', 'scott/tiger@orcl', '@/tmp/test_legacy.sql', 'arg1', 'arg2']`
  - `stdin`: `subprocess.DEVNULL`
  - `check`: `False`
  - Returns `0`.
- **Fail**: Subprocess is not called, called with incorrect arguments, or standard input is not redirected to prevent interactive hangs.

---

# Runnable Test Suite (pytest)

Save the following code as `test_h_alis_sqlplus.py` to run automated validations.

```python
import os
import sys
import pytest
from unittest.mock import MagicMock, patch

# Import the target module
import h_alis_sqlplus


@pytest.fixture
def temp_sql_file(tmp_path):
    """Creates a temporary SQL file for testing."""
    sql_file = tmp_path / "test_script.sql"
    sql_file.write_text("SELECT * FROM test_table WHERE id = '&1';", encoding="utf-8")
    return str(sql_file)


@patch("h_alis_sqlplus._dwmsg_melde_fehler")
def test_parameter_validation_missing_args(mock_melde_fehler):
    """Test Case 1: Missing arguments trigger error 196."""
    # Missing entry number
    rc1 = h_alis_sqlplus.starte_sql_skript("", "some_script.sql")
    assert rc1 == 196
    mock_melde_fehler.assert_any_call("", "E", 196, "alis_sqlplus V1.1.3 starteSQLSkript")

    # Missing script path
    rc2 = h_alis_sqlplus.starte_sql_skript("12345", "")
    assert rc2 == 196
    mock_melde_fehler.assert_any_call("12345", "E", 196, "alis_sqlplus V1.1.3 starteSQLSkript")


@patch("h_alis_sqlplus._dwmsg_melde_fehler")
def test_file_readability_validation(mock_melde_fehler):
    """Test Case 2: Non-existent file triggers error 201."""
    non_existent = "/tmp/non_existent_file_999.sql"
    rc = h_alis_sqlplus.starte_sql_skript("12345", non_existent)
    assert rc == 201
    mock_melde_fehler.assert_called_once_with("12345", "E", 201, non_existent)


@patch("google.cloud.bigquery.Client")
def test_output_parity(mock_bq_client, temp_sql_file, capsys):
    """Test Case 3: Verify exact German log output matches legacy."""
    with patch.dict(os.environ, {"USE_BIGQUERY": "True", "DW_ORAUSER": ""}):
        rc = h_alis_sqlplus.starte_sql_skript("12345", temp_sql_file, "val1", "val2")
        assert rc == 0
        
        captured = capsys.readouterr()
        expected_output = (
            "Rufe SQL*PLUS auf mit folgenden Einstellungen\n"
            f"Sql*Plus-Skript : {temp_sql_file}\n"
            "Skript-Parameter: val1 val2\n"
        )
        assert expected_output in captured.out


@patch("google.cloud.bigquery.Client")
def test_bigquery_sql_transformation(mock_bq_client, tmp_path):
    """Test Case 4: Verify SQL*Plus parameter substitution and directive stripping."""
    sql_file = tmp_path / "transform.sql"
    sql_file.write_text(
        "SET PAGESIZE 100\n"
        "WHENEVER SQLERROR EXIT 1\n"
        "SELECT * FROM my_table WHERE col1 = '&1' AND col2 = '&&2';\n"
        "EXIT;",
        encoding="utf-8"
    )

    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance

    with patch.dict(os.environ, {"USE_BIGQUERY": "True", "DW_ORAUSER": ""}):
        rc = h_alis_sqlplus.starte_sql_skript("12345", str(sql_file), "foo", "bar")
        assert rc == 0

        # Capture the query passed to BigQuery
        called_query = mock_client_instance.query.call_args[0][0]
        
        # Directives must be stripped
        assert "SET PAGESIZE" not in called_query
        assert "WHENEVER SQLERROR" not in called_query
        assert "EXIT" not in called_query
        
        # Parameters must be substituted
        assert "col1 = 'foo'" in called_query
        assert "col2 = 'bar'" in called_query


@patch("google.cloud.bigquery.Client")
def test_bigquery_execution_failure(mock_bq_client, temp_sql_file):
    """Test Case 5: BigQuery execution failure returns 1."""
    mock_client_instance = MagicMock()
    mock_client_instance.query.side_effect = Exception("BigQuery Syntax Error")
    mock_bq_client.return_value = mock_client_instance

    with patch.dict(os.environ, {"USE_BIGQUERY": "True", "DW_ORAUSER": ""}):
        rc = h_alis_sqlplus.starte_sql_skript("12345", temp_sql_file, "val1")
        assert rc == 1


@patch("subprocess.run")
def test_legacy_fallback_path(mock_sub_run, temp_sql_file):
    """Test Case 6: Fallback to sqlplus subprocess when USE_BIGQUERY is False."""
    mock_sub_run.return_value = MagicMock(returncode=42)

    env_vars = {
        "USE_BIGQUERY": "False",
        "DW_ORAUSER": "scott/tiger@orcl"
    }
    with patch.dict(os.environ, env_vars):
        rc = h_alis_sqlplus.starte_sql_skript("12345", temp_sql_file, "arg1", "arg2")
        assert rc == 42
        mock_sub_run.assert_called_once_with(
            ["sqlplus", "scott/tiger@orcl", f"@{temp_sql_file}", "arg1", "arg2"],
            stdin=subprocess.DEVNULL,
            check=False
        )
```