# Migration Validation Test Suite: Shared Files — Utility Binaries

This document defines the migration-validation test suite for the migrated Python utility modules:
*   `f_alis_msgerr.py` (Error management and database logging)
*   `h_alis_date.py` (Date arithmetic and validation)
*   `h_alis_parameter.py` (Parameter parsing and domain validation)
*   `h_alis_sqlplus.py` (SQL*Plus execution wrapper)

These tests ensure behavioral equivalence between the legacy KornShell (`.ksh`) scripts and the migrated Python (`.py`) implementations, covering output parity, edge cases, database interactions, and error handling.

---

## Section 1: Unit & Behavioral Parity Tests for `h_alis_date.py`

### Test Case 1.1: Previous Month Calculation (`dw_date_vormonat`)
*   **Purpose**: Verify that `dw_date_vormonat` correctly calculates the previous month relative to a given execution date and formats it using Oracle-to-Python mapped format strings.
*   **Setup**: 
    *   Mock `datetime.now` to return a fixed date: `2023-11-15`.
    *   Clear environment variables.
*   **Action**: 
    1.  Call `dw_date_vormonat("TEST_VAR", "YYYYMM")`.
    2.  Call `dw_date_vormonat("TEST_VAR", "MM/YYYY")`.
*   **Pass/Fail Criterion**: 
    *   **Pass**: The first call returns `"202310"` and prints `TEST_VAR=202310` to `stdout`. The second call returns `"10/2023"` and prints `TEST_VAR=10/2023` to `stdout`.
    *   **Fail**: Any other output is returned, or the printed output does not match the shell assignment format.

```python
import pytest
from unittest.mock import patch
from datetime import datetime
import h_alis_date

@patch('h_alis_date.datetime')
def test_dw_date_vormonat(mock_datetime, capsys):
    # Mock current date to Nov 15, 2023
    mock_datetime.now.return_value = datetime(2023, 11, 15)
    mock_datetime.strptime = datetime.strptime

    # Test YYYYMM format
    res1 = h_alis_date.dw_date_vormonat("TEST_VAR", "YYYYMM")
    assert res1 == "202310"
    captured = capsys.readouterr()
    assert captured.out.strip() == "TEST_VAR=202310"

    # Test MM/YYYY format
    res2 = h_alis_date.dw_date_vormonat("TEST_VAR", "MM/YYYY")
    assert res2 == "10/2023"
    captured = capsys.readouterr()
    assert captured.out.strip() == "TEST_VAR=10/2023"
```

### Test Case 1.2: Date Format Validation (`dw_date_datum_check`)
*   **Purpose**: Verify that `dw_date_datum_check` correctly validates date strings against specified formats, matching the legacy database-driven validation.
*   **Setup**: None.
*   **Action**: Execute validation with valid and invalid date/format combinations.
*   **Pass/Fail Criterion**: 
    *   **Pass**: Returns `True` for valid dates and `False` for invalid dates (e.g., out-of-range months/days).
    *   **Fail**: Incorrectly validates an invalid date or rejects a valid one.

```python
def test_dw_date_datum_check():
    # Valid cases
    assert h_alis_date.dw_date_datum_check("20231015", "YYYYMMDD") is True
    assert h_alis_date.dw_date_datum_check("15.10.2023", "DD.MM.YYYY") is True
    
    # Invalid cases
    assert h_alis_date.dw_date_datum_check("20231315", "YYYYMMDD") is False  # Month 13
    assert h_alis_date.dw_date_datum_check("20230229", "YYYYMMDD") is False  # Non-leap year Feb 29
    assert h_alis_date.dw_date_datum_check("not_a_date", "YYYYMMDD") is False
```

### Test Case 1.3: Date Chronology Assertion (`dw_date_datum_le`)
*   **Purpose**: Verify that `dw_date_datum_le` asserts that `datum1 <= datum2` and raises a `ValueError` with the exact legacy German error message when the assertion fails.
*   **Setup**: None.
*   **Action**: 
    1.  Call with `datum1 <= datum2`.
    2.  Call with `datum1 > datum2`.
*   **Pass/Fail Criterion**: 
    *   **Pass**: Returns `True` for valid chronology. Raises `ValueError` with the exact message `"Datum 20231115 ist groesser als 20231114"` when `datum1 > datum2`.
    *   **Fail**: Does not raise an exception, or the exception message does not match the legacy literal.

```python
def test_dw_date_datum_le():
    # Equal dates
    assert h_alis_date.dw_date_datum_le("20231115", "20231115") is True
    # Chronological order
    assert h_alis_date.dw_date_datum_le("20231114", "20231115") is True
    
    # Out of order - must raise ValueError with exact legacy German message
    with pytest.raises(ValueError) as excinfo:
        h_alis_date.dw_date_datum_le("20231115", "20231114")
    assert "Datum 20231115 ist groesser als 20231114" in str(excinfo.value)
```

### Test Case 1.4: Date Range Calculation (`dw_date_gib_zeitraum`)
*   **Purpose**: Verify that `dw_date_gib_zeitraum` calculates correct start and end dates based on step units ('D', 'M', 'Y') and offsets, matching legacy business logic.
*   **Setup**: Mock `datetime.now` to return `2023-11-15`.
*   **Action**: 
    1.  Call with offset `-1`, unit `'M'` (Month).
    2.  Call with offset `5`, unit `'D'` (Day).
    3.  Call with offset `1`, unit `'Y'` (Year).
*   **Pass/Fail Criterion**: 
    *   **Pass**: 
        *   Month ('M') offset `-1` returns `("20231101", "20231031")` (Start is 1st of current month, End is ultimo of target month).
        *   Year ('Y') offset `1` returns `("20230101", "20241231")` (Start is New Year of current year, End is New Year's Eve of target year).
    *   **Fail**: Calculated dates do not align with legacy month-end/year-end boundary rules.

```python
@patch('h_alis_date.datetime')
def test_dw_date_gib_zeitraum(mock_datetime):
    mock_datetime.now.return_value = datetime(2023, 11, 15)
    mock_datetime.strptime = datetime.strptime

    # Test Month Step (M) with negative offset
    start, end = h_alis_date.dw_date_gib_zeitraum(-1, "M", "YYYYMMDD")
    assert start == "20231101"  # First of current month
    assert end == "20231031"    # Last day of October (current - 1 month)

    # Test Year Step (Y) with positive offset
    start, end = h_alis_date.dw_date_gib_zeitraum(1, "Y", "YYYYMMDD")
    assert start == "20230101"  # New Year of current year
    assert end == "20241231"    # New Year's Eve of next year
```

---

## Section 2: Unit & Behavioral Parity Tests for `h_alis_parameter.py`

### Test Case 2.1: Parameter Presence Check (`pruefeParameterGesetzt`)
*   **Purpose**: Verify that `pruefeParameterGesetzt` correctly identifies unset environment variables and sets the global error state (`ErrNr` and `ErrArg`) in `os.environ`.
*   **Setup**: Clear `os.environ` of `ErrNr` and `ErrArg`.
*   **Action**: 
    1.  Call with a set environment variable.
    2.  Call with an unset environment variable.
*   **Pass/Fail Criterion**: 
    *   **Pass**: If the variable is unset, `os.environ["ErrNr"]` is set to `"194"` and `os.environ["ErrArg"]` is set to the parameter name. If set, error state remains unchanged.
    *   **Fail**: Error state is not updated, or incorrect error codes are set.

```python
import os
import h_alis_parameter

def test_pruefe_parameter_gesetzt():
    # Reset error state
    h_alis_parameter.set_error(0, "")
    
    # Case 1: Parameter is set
    os.environ["TEST_PARAM_VAR"] = "value"
    h_alis_parameter.pruefeParameterGesetzt("TestParam", "TEST_PARAM_VAR")
    err_nr, _ = h_alis_parameter.get_error()
    assert err_nr == 0

    # Case 2: Parameter is missing
    if "TEST_MISSING_VAR" in os.environ:
        del os.environ["TEST_MISSING_VAR"]
    h_alis_parameter.pruefeParameterGesetzt("MissingParam", "TEST_MISSING_VAR")
    err_nr, err_arg = h_alis_parameter.get_error()
    assert err_nr == 194
    assert err_arg == "MissingParam"
```

### Test Case 2.2: KPI Code Conversion (`konvertiereKennzahl`)
*   **Purpose**: Verify that `konvertiereKennzahl` maps legacy German KPI names to their standardized 3-letter shorthand codes and handles unknown values.
*   **Setup**: Reset error state.
*   **Action**: 
    1.  Convert `"zugang"`, `"abgang"`, `"bestand"`, `"restguthaben"`.
    2.  Convert an invalid KPI `"invalid_kpi"`.
*   **Pass/Fail Criterion**: 
    *   **Pass**: Valid KPIs are mapped to `"zug"`, `"abg"`, `"bst"`, `"rst"`. Invalid KPIs set `ErrNr` to `198` and the variable to `"???"`.
    *   **Fail**: Incorrect mappings or failure to set error state on invalid inputs.

```python
def test_konvertiere_kennzahl():
    h_alis_parameter.set_error(0, "")
    
    # Test valid mappings
    os.environ["KPI_VAR"] = "zugang"
    h_alis_parameter.konvertiereKennzahl("KPI_VAR")
    assert os.environ["KPI_VAR"] == "zug"

    os.environ["KPI_VAR"] = "restguthaben"
    h_alis_parameter.konvertiereKennzahl("KPI_VAR")
    assert os.environ["KPI_VAR"] == "rst"

    # Test invalid mapping
    h_alis_parameter.set_error(0, "")
    os.environ["KPI_VAR"] = "invalid_kpi"
    h_alis_parameter.konvertiereKennzahl("KPI_VAR")
    assert os.environ["KPI_VAR"] == "???"
    err_nr, err_arg = h_alis_parameter.get_error()
    assert err_nr == 198
    assert err_arg == "invalid_kpi"
```

### Test Case 2.3: System-KPI Compatibility Matrix (`pruefeSystemKennzahl`)
*   **Purpose**: Validate that `pruefeSystemKennzahl` enforces the compatibility matrix between source systems and KPIs, setting `ErrNr = 195` for invalid combinations.
*   **Setup**: Reset error state.
*   **Action**: 
    1.  Test allowed combination: `("sap", "srs")`.
    2.  Test forbidden combination: `("sap", "zug")`.
*   **Pass/Fail Criterion**: 
    *   **Pass**: Allowed combinations do not alter the error state. Forbidden combinations set `ErrNr = 195` and `ErrArg = "Ungueltige Kombination sap zug"`.
    *   **Fail**: Forbidden combinations are allowed to pass, or allowed combinations are blocked.

```python
def test_pruefe_system_kennzahl():
    # Allowed combination
    h_alis_parameter.set_error(0, "")
    h_alis_parameter.pruefeSystemKennzahl("sap", "srs")
    err_nr, _ = h_alis_parameter.get_error()
    assert err_nr == 0

    # Forbidden combination
    h_alis_parameter.set_error(0, "")
    h_alis_parameter.pruefeSystemKennzahl("sap", "zug")
    err_nr, err_arg = h_alis_parameter.get_error()
    assert err_nr == 195
    assert err_arg == "Ungueltige Kombination sap zug"
```

---

## Section 3: Integration & Database Mocking Tests for `f_alis_msgerr.py`

### Test Case 3.1: Database State Transitions (`setze_status_ok` / `setze_status_abbruch`)
*   **Purpose**: Verify that `setze_status_ok` and `setze_status_abbruch` execute the correct Oracle PL/SQL stored procedures with the provided entry ID.
*   **Setup**: Mock `oracledb.connect` and its cursor context manager.
*   **Action**: 
    1.  Call `setze_status_ok(12345)`.
    2.  Call `setze_status_abbruch(12345)`.
*   **Pass/Fail Criterion**: 
    *   **Pass**: The mock cursor executes `"BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;"` and `"BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;"` respectively, passing `[12345]` as a bind parameter.
    *   **Fail**: Stored procedures are called with incorrect SQL, incorrect parameters, or connection is not committed.

```python
from unittest.mock import MagicMock, patch
import f_alis_msgerr

@patch('f_alis_msgerr.oracledb.connect')
def test_database_status_transitions(mock_connect):
    # Setup mock connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Action 1: Set Status OK
    f_alis_msgerr.setze_status_ok(12345)
    mock_cursor.execute.assert_called_with(
        "BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", [12345]
    )
    assert mock_conn.commit.call_count == 1

    # Action 2: Set Status Abbruch
    f_alis_msgerr.setze_status_abbruch(12345)
    mock_cursor.execute.assert_called_with(
        "BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [12345]
    )
    assert mock_conn.commit.call_count == 2
```

### Test Case 3.2: Detection of Truncation Gaps (Missing Functions)
*   **Purpose**: Explicitly assert the existence and signature of all legacy functions to prevent deployment of truncated Python code.
*   **Setup**: Import `f_alis_msgerr`.
*   **Action**: Inspect the module for the presence of all legacy functions.
*   **Pass/Fail Criterion**: 
    *   **Pass**: All functions (`fehlerbehandlung`, `setze_status_ok`, `setze_status_abbruch`, `ermittle_nr`, `erzeuge_eintrag`, `melde_fehler`, `logdateiname`, `setze_stichtag_info`, `append_timing_infos`) are defined.
    *   **Fail**: Any of the listed functions are missing or truncated.

```python
def test_prevent_truncation_gaps():
    import f_alis_msgerr
    
    # Assert all legacy functions are implemented in the Python module
    assert hasattr(f_alis_msgerr, 'fehlerbehandlung')
    assert hasattr(f_alis_msgerr, 'setze_status_ok')
    assert hasattr(f_alis_msgerr, 'setze_status_abbruch')
    assert hasattr(f_alis_msgerr, 'ermittle_nr')
    assert hasattr(f_alis_msgerr, 'erzeuge_eintrag')
    assert hasattr(f_alis_msgerr, 'melde_fehler')
    assert hasattr(f_alis_msgerr, 'logdateiname')
    assert hasattr(f_alis_msgerr, 'setze_stichtag_info')
    assert hasattr(f_alis_msgerr, 'append_timing_infos')
```

---

## Section 4: Integration & Subprocess Mocking Tests for `h_alis_sqlplus.py`

### Test Case 4.1: SQL*Plus Execution Preconditions and Execution
*   **Purpose**: Verify that `starte_sql_skript` validates file readability and executes SQL*Plus with correct arguments and environment variables.
*   **Setup**: 
    *   Mock `pathlib.Path.is_file` and `os.access` to simulate file readability.
    *   Mock `subprocess.run` to prevent actual process execution.
    *   Set `os.environ["DW_ORAUSER"] = "test_user/test_pass@test_db"`.
*   **Action**: 
    1.  Call with a non-existent/unreadable script.
    2.  Call with a valid script and arguments.
*   **Pass/Fail Criterion**: 
    *   **Pass**: 
        *   Unreadable script returns `201` and calls `dwmsg_melde_fehler`.
        *   Readable script executes `subprocess.run` with `["sqlplus", "test_user/test_pass@test_db", "@/path/to/script.sql", "arg1", "arg2"]` and returns the process exit code.
    *   **Fail**: Subprocess is called when preconditions fail, or arguments are passed incorrectly.

```python
from unittest.mock import patch, MagicMock
import h_alis_sqlplus
import os

@patch('h_alis_sqlplus.pathlib.Path')
@patch('h_alis_sqlplus.os.access')
@patch('h_alis_sqlplus.subprocess.run')
@patch('h_alis_sqlplus.dwmsg_melde_fehler')
def test_starte_sql_skript(mock_melde_fehler, mock_run, mock_access, mock_path):
    os.environ["DW_ORAUSER"] = "test_user/test_pass@test_db"

    # Case 1: Script file does not exist / is unreadable
    mock_path.return_value.is_file.return_value = False
    rc = h_alis_sqlplus.starte_sql_skript("999", "/invalid/path.sql")
    assert rc == 201
    mock_melde_fehler.assert_called_with("999", "E", "201", "/invalid/path.sql")

    # Case 2: Script is valid and readable
    mock_path.return_value.is_file.return_value = True
    mock_access.return_value = True
    
    # Mock successful subprocess execution (exit code 0)
    mock_proc = MagicMock()
    mock_proc.return_code = 0
    mock_run.return_value = mock_proc

    rc = h_alis_sqlplus.starte_sql_skript("123", "/valid/path.sql", "param1", "param2")
    
    assert rc == 0
    mock_run.assert_called_with(
        ["sqlplus", "test_user/test_pass@test_db", "@/valid/path.sql", "param1", "param2"],
        stdin=subprocess.DEVNULL,
        capture_output=False,
        check=False
    )
```