# Migration Validation Test Suite: Shared Files (is/util/bin)

This document defines the migration-validation test suite for the migrated Python utility modules located in `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`. These tests ensure behavioral equivalence between the legacy KornShell (`.ksh`) scripts and the migrated Python (`.py`) implementations.

---

## Test Case 1: `f_alis_msgerr` — Error Handling & Status Updates (`dwmsg_fehlerbehandlung`)

### Purpose
Verify that `dwmsg_fehlerbehandlung` correctly logs a fatal error (code `10`) with the captured exit code and sets the execution log entry status to aborted (`Abbruch`) in the database.

### Setup
- Set the environment variable `DW_ORAUSER` to a mock connection string.
- Mock the `oracledb` connection and cursor objects to capture executed PL/SQL statements.

### Action
Run the following `pytest` test case:

```python
import os
import pytest
from unittest.mock import MagicMock, patch

# Import the migrated module
import f_alis_msgerr

@patch("f_alis_msgerr.get_db_connection")
def test_dwmsg_fehlerbehandlung(mock_get_db):
    # Setup mock database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    os.environ["DW_ORAUSER"] = "mock_user/mock_pass@mock_db"
    
    # Action: Trigger error handler with entry ID "99999" and exit code 2
    f_alis_msgerr.dwmsg_fehlerbehandlung(eintrags_nr="99999", fehler_nr=2)

    # Assertions for MeldeFehler (Fatal, Error Code 10, with custom message)
    # Since MeldeFehler is called first, verify the PL/SQL execution
    mock_cursor.execute.assert_any_call(
        "BEGIN BERT_MELDUNG.Fehler(:1, :2, :3, :4); END;",
        ["F", "99999", 10, "ErrorCode ist: 2"]
    )

    # Assertions for SetzeStatusAbbruch
    mock_cursor.execute.assert_any_call(
        "BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;",
        ["99999"]
    )
    
    # Ensure transactions were committed
    assert mock_conn.commit.call_count == 2
```

### Pass/Fail Criterion
- **Pass**: The database cursor executes `BERT_MELDUNG.Fehler` with parameters `['F', '99999', 10, 'ErrorCode ist: 2']` and `BERT_MELDUNG.SetzeStatusAbbruch` with parameter `['99999']`. Both transactions are committed.
- **Fail**: Any database call is missed, parameters do not match, or the transaction is not committed.

---

## Test Case 2: `f_alis_msgerr` — Sequence Number Retrieval (`dwmsg_ermittle_nr`)

### Purpose
Verify that `dwmsg_ermittle_nr` executes the external SQL script via `sqlplus`, reads the generated sequence number from the temporary file, cleans up the file, and returns the stripped sequence.

### Setup
- Set the environment variables `DW_ORAUSER` and `DW_DIR_ROOT`.
- Mock `subprocess.run` to simulate the creation of the temporary file containing the sequence number.

### Action
Run the following `pytest` test case:

```python
import os
import pytest
from unittest.mock import patch, mock_open

import f_alis_msgerr

@patch("subprocess.run")
@patch("os.remove")
@patch("os.path.exists", return_value=True)
def test_dwmsg_ermittle_nr(mock_exists, mock_remove, mock_sub_run):
    os.environ["DW_ORAUSER"] = "mock_user/mock_pass@mock_db"
    os.environ["DW_DIR_ROOT"] = "/opt/dwh"
    
    # Mock the file read operation to return a sequence number with spaces
    mock_file_content = "   87654321 \n"
    
    with patch("builtins.open", mock_open(read_data=mock_file_content)) as mock_file:
        # Action
        result = f_alis_msgerr.dwmsg_ermittle_nr(var_name="MY_SEQ_VAR")
        
        # Assertions
        assert result == "87654321"
        mock_sub_run.assert_called_once()
        # Verify sqlplus was called with the correct script path
        args = mock_sub_run.call_args[0][0]
        assert "sqlplus" in args
        assert "@/opt/dwh/allgemein/is/util/sql/d_al_is_ermittlenr.sql" in args
        
        # Verify cleanup of the temporary file
        mock_remove.assert_called_once()
```

### Pass/Fail Criterion
- **Pass**: The function returns `"87654321"` (whitespace stripped), calls `sqlplus` with the correct path, and deletes the temporary file.
- **Fail**: The temporary file is not deleted, the sequence number is not stripped, or the `sqlplus` command is malformed.

---

## Test Case 3: `h_alis_date` — Date Addition & Leap Year Rollover (`AddiereDatum`)

### Purpose
Verify that `AddiereDatum` correctly adds days, handles month rollovers, leap years, and year transitions natively without database roundtrips.

### Setup
- No external dependencies or database connections are required (pure Python logic).

### Action
Run the following `pytest` test case:

```python
import pytest
import h_alis_date

def test_addiere_datum_scenarios():
    # Test Case A: Standard addition within the same month
    assert h_alis_date.AddiereDatum("20231015", 5) == "20231020"

    # Test Case B: Month rollover (non-leap year February)
    assert h_alis_date.AddiereDatum("20230228", 1) == "20230301"

    # Test Case C: Month rollover (leap year February)
    assert h_alis_date.AddiereDatum("20200228", 1) == "20200229"
    assert h_alis_date.AddiereDatum("20200228", 2) == "20200301"

    # Test Case D: Year rollover
    assert h_alis_date.AddiereDatum("20231231", 1) == "20240101"

    # Test Case E: Large day addition spanning multiple months
    assert h_alis_date.AddiereDatum("20230101", 100) == "20230411"
```

### Pass/Fail Criterion
- **Pass**: All date additions match the expected Gregorian calendar dates exactly, including leap year and year-end transitions.
- **Fail**: Any calculated date is incorrect or raises an unhandled exception.

---

## Test Case 4: `h_alis_date` — Date Range Calculation (`DWDate_Gib_Zeitraum`)

### Purpose
Verify that `DWDate_Gib_Zeitraum` correctly calculates start and end dates for Days (`D`), Months (`M`), and Years (`Y`) intervals based on a fixed reference date.

### Setup
- Mock `datetime.date.today` to return a fixed date: `2023-10-15`.

### Action
Run the following `pytest` test case:

```python
import datetime
import pytest
from unittest.mock import patch

import h_alis_date

@patch("datetime.date")
def test_dw_date_gib_zeitraum(mock_date):
    # Fix today's date to 2023-10-15
    mock_date.today.return_value = datetime.date(2023, 10, 15)
    mock_date.side_effect = lambda *args, **kwargs: datetime.date(*args, **kwargs)

    # Test Case A: Days basis ('D') with positive offset
    h_alis_date.globals_dict.clear()
    rc = h_alis_date.DWDate_Gib_Zeitraum(Offset=10, Stufe="D", Format="YYYYMMDD", Var_Start="START", Var_Ende="ENDE")
    assert rc == 0
    assert h_alis_date.globals_dict["START"] == "20231015"
    assert h_alis_date.globals_dict["ENDE"] == "20231025"

    # Test Case B: Months basis ('M') with negative offset (Start is 1st of current month, End is Ultimo of target month)
    h_alis_date.globals_dict.clear()
    rc = h_alis_date.DWDate_Gib_Zeitraum(Offset=-2, Stufe="M", Format="YYYYMMDD", Var_Start="START", Var_Ende="ENDE")
    assert rc == 0
    assert h_alis_date.globals_dict["START"] == "20231001"  # First of current month
    assert h_alis_date.globals_dict["ENDE"] == "20230831"    # Ultimo of August (10 - 2 = 8)

    # Test Case C: Years basis ('Y') with positive offset (Start is New Year of current year, End is Sylvester of target year)
    h_alis_date.globals_dict.clear()
    rc = h_alis_date.DWDate_Gib_Zeitraum(Offset=1, Stufe="Y", Format="YYYYMMDD", Var_Start="START", Var_Ende="ENDE")
    assert rc == 0
    assert h_alis_date.globals_dict["START"] == "20230101"  # New Year 2023
    assert h_alis_date.globals_dict["ENDE"] == "20241231"    # Sylvester 2024 (2023 + 1)
```

### Pass/Fail Criterion
- **Pass**: The calculated start and end dates match the legacy business rules:
  - Days: `Start = Today`, `End = Today + Offset`.
  - Months: `Start = First of current month`, `End = Ultimo of target month`.
  - Years: `Start = New Year of current year`, `End = Sylvester of target year`.
- **Fail**: The calculated dates do not match these rules, or the return code is non-zero.

---

## Test Case 5: `h_alis_parameter` — Parameter Normalization & Validation

### Purpose
Verify that metric names are correctly normalized to abbreviations, and invalid system-metric combinations are caught.

### Setup
- Initialize the global variables `ErrNr` and `ErrArg` in `h_alis_parameter`.
- Set environment variables to simulate the calling shell's environment.

### Action
Run the following `pytest` test case:

```python
import os
import pytest
import h_alis_parameter

def test_parameter_normalization_and_validation():
    # Reset global error state
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""

    # Test Case A: Normalize "bestand" to "bst"
    os.environ["MY_KENNZAHL"] = "bestand"
    h_alis_parameter.konvertiereKennzahl("MY_KENNZAHL")
    assert os.environ["MY_KENNZAHL"] == "bst"
    assert h_alis_parameter.ErrNr == 0

    # Test Case B: Normalize invalid metric
    os.environ["MY_KENNZAHL"] = "invalid_metric"
    h_alis_parameter.konvertiereKennzahl("MY_KENNZAHL")
    assert os.environ["MY_KENNZAHL"] == "???"
    assert h_alis_parameter.ErrNr == 198
    assert h_alis_parameter.ErrArg == "invalid_metric"

    # Reset error state for next test
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""

    # Test Case C: Validate invalid combination (sap + zug)
    h_alis_parameter.pruefeSystemKennzahl("sap", "zug")
    assert h_alis_parameter.ErrNr == 195
    assert "Ungueltige Kombination sap zug" in h_alis_parameter.ErrArg

    # Reset error state for next test
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""

    # Test Case D: Validate valid combination (sap + srs)
    h_alis_parameter.pruefeSystemKennzahl("sap", "srs")
    assert h_alis_parameter.ErrNr == 0
```

### Pass/Fail Criterion
- **Pass**: Metric names are correctly mapped, invalid metrics set `ErrNr = 198`, and invalid system-metric combinations set `ErrNr = 195` with the correct German error message.
- **Fail**: Normalization fails, or invalid combinations are permitted without setting the error state.

---

## Test Case 6: `h_alis_sqlplus` — Safe SQL Execution (`starte_sql_skript`)

### Purpose
Verify that `starte_sql_skript` validates script readability, logs parameters, executes `sqlplus` with redirected input, and propagates the exit code.

### Setup
- Set the environment variable `DW_ORAUSER`.
- Mock `pathlib.Path.is_file` and `os.access` to simulate a readable SQL script.
- Mock `subprocess.run` to capture the command execution.

### Action
Run the following `pytest` test case:

```python
import os
import pytest
from unittest.mock import patch, MagicMock

import h_alis_sqlplus

@patch("h_alis_sqlplus.Path.is_file", return_value=True)
@patch("os.access", return_value=True)
@patch("subprocess.run")
def test_starte_sql_skript_success(mock_sub_run, mock_access, mock_is_file):
    os.environ["DW_ORAUSER"] = "test_user/test_pass@test_db"
    
    # Mock successful subprocess execution (exit code 0)
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_sub_run.return_value = mock_result

    # Action: Execute a mock script with parameters
    rc = h_alis_sqlplus.starte_sql_skript(
        "11111", 
        "/opt/dwh/test_script.sql", 
        "param_val1", 
        "param_val2"
    )

    # Assertions
    assert rc == 0
    mock_sub_run.assert_called_once_with(
        ["sqlplus", "test_user/test_pass@test_db", "@/opt/dwh/test_script.sql", "param_val1", "param_val2"],
        stdin=subprocess.DEVNULL,
        check=False
    )
```

### Pass/Fail Criterion
- **Pass**: The function returns `0`, and `subprocess.run` is called with the correct `sqlplus` arguments and `stdin` redirected to `DEVNULL` (equivalent to `</dev/null` in the legacy shell).
- **Fail**: The function returns a non-zero code, fails to validate the file, or executes `sqlplus` with incorrect arguments.