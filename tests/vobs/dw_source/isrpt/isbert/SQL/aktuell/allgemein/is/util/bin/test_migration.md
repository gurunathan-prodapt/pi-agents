# Migration Validation Test Suite: Shared Files (is/util/bin)

This document defines the migration-validation test suite to prove that the migrated Python utility modules (`f_alis_msgerr.py`, `h_alis_date.py`, `h_alis_parameter.py`, and `h_alis_sqlplus.py`) are behaviorally equivalent to their legacy KornShell counterparts.

---

## 1. Test Strategy & Prerequisites

The utility scripts under test are foundational libraries used across the entire data warehouse platform. Because they manage process state, date arithmetic, parameter validation, and database logging, the validation strategy relies on **unit-level parity testing** and **mocked integration testing**.

### Prerequisites
To run these tests, the following Python packages must be installed in the test environment:
```bash
pip install pytest pytest-mock oracledb
```

### Environment Variables
The tests expect the following environment variables to be set or mocked:
- `DW_ORAUSER`: Database connection string (e.g., `test_user/test_pass@test_dsn`).
- `DW_DIR_ROOT`: Root directory for SQL scripts.
- `DW_DIR_PROT`: Target directory for log files.

---

## 2. Test Case 1: `f_alis_msgerr` Validation (Database Logging & Status Tracking)

### Purpose
Verify that the migrated Python logging and status-tracking functions correctly validate inputs, format log filenames, and execute the expected PL/SQL procedures in the Oracle database.

### Setup
- Mock the `oracledb` connection and cursor objects to prevent actual database writes while capturing the exact PL/SQL calls and arguments.
- Set environment variables: `DW_DIR_PROT=/tmp/prot`, `DW_ORAUSER=user/pass@dsn`, `DW_DIR_ROOT=/tmp/root`.

### Action
Execute a series of unit tests targeting each function in `f_alis_msgerr.py`.

### Pass/Fail Criteria
- **Pass**: 
  - Missing mandatory parameters raise `SystemExit` with code `1` or `2` and print the exact legacy German error messages to `stderr`.
  - Database calls invoke the correct PL/SQL procedures (`BERT_MELDUNG.SetzeStatusOk`, `BERT_MELDUNG.SetzeStatusAbbruch`, `BERT_MELDUNG.Erzeuge_Eintrag`, `BERT_MELDUNG.Fehler`, `BERT_MELDUNG.SetzeZusatzInfos`) with correctly typed parameters.
  - Log filenames match the pattern: `{DW_DIR_PROT}/{JobKennung}_{YYYYMMDD_HHMM}_{EintragsNr}.log`.
- **Fail**: Any deviation in procedure names, argument types, exit codes, or error messages.

### Test Code (`test_f_alis_msgerr.py`)

```python
import os
import sys
import pytest
from unittest.mock import MagicMock, patch
import datetime

# Set up environment variables before importing the module
os.environ["DW_ORAUSER"] = "test_user/test_pass@test_dsn"
os.environ["DW_DIR_ROOT"] = "/tmp/root"
os.environ["DW_DIR_PROT"] = "/tmp/prot"

import f_alis_msgerr

@pytest.fixture
def mock_db(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mocker.patch("f_alis_msgerr.get_db_connection", return_value=mock_conn)
    return mock_cursor, mock_conn

def test_dwmsg_setze_status_ok_success(mock_db):
    mock_cursor, mock_conn = mock_db
    f_alis_msgerr.dwmsg_setze_status_ok("12345")
    
    mock_cursor.callproc.assert_called_once_with("BERT_MELDUNG.SetzeStatusOk", [12345])
    mock_conn.commit.assert_called_once()

def test_dwmsg_setze_status_ok_missing_param(capsys):
    with pytest.raises(SystemExit) as exc_info:
        f_alis_msgerr.dwmsg_setze_status_ok("")
    assert exc_info.value.code == 1
    stderr = capsys.readouterr().err
    assert "Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben" in stderr

def test_dwmsg_setze_status_abbruch_success(mock_db):
    mock_cursor, mock_conn = mock_db
    f_alis_msgerr.dwmsg_setze_status_abbruch("12345")
    
    mock_cursor.callproc.assert_called_once_with("BERT_MELDUNG.SetzeStatusAbbruch", [12345])
    mock_conn.commit.assert_called_once()

def test_dwmsg_setze_status_abbruch_missing_param(capsys):
    with pytest.raises(SystemExit) as exc_info:
        f_alis_msgerr.dwmsg_setze_status_abbruch("")
    assert exc_info.value.code == 1
    stderr = capsys.readouterr().err
    assert "Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben" in stderr

def test_dwmsg_erzeuge_eintrag(mock_db):
    mock_cursor, mock_conn = mock_db
    f_alis_msgerr.dwmsg_erzeuge_eintrag("12345", "JOB_TEST", "PROG_TEST", "/tmp/test.log")
    
    mock_cursor.callproc.assert_called_once_with(
        "BERT_MELDUNG.Erzeuge_Eintrag", [12345, "JOB_TEST", "PROG_TEST", "/tmp/test.log"]
    )
    mock_conn.commit.assert_called_once()

def test_dwmsg_melde_fehler_three_params(mock_db):
    mock_cursor, mock_conn = mock_db
    f_alis_msgerr.dwmsg_melde_fehler("12345", "E", "99")
    mock_cursor.callproc.assert_called_once_with("BERT_MELDUNG.Fehler", ["E", 12345, 99])

def test_dwmsg_melde_fehler_five_params(mock_db):
    mock_cursor, mock_conn = mock_db
    f_alis_msgerr.dwmsg_melde_fehler("12345", "F", "10", "ZusatzA", "ZusatzB")
    mock_cursor.callproc.assert_called_once_with("BERT_MELDUNG.Fehler", ["F", 12345, 10, "ZusatzA", "ZusatzB"])

def test_dwmsg_logdateiname():
    res = f_alis_msgerr.dwmsg_logdateiname("MY_LOG_VAR", "JOB_ABC", "98765")
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    expected = f"/tmp/prot/JOB_ABC_{current_time}_98765.log"
    assert res == expected
    assert os.environ["MY_LOG_VAR"] == expected

def test_dwmsg_setze_stichtag_info(mock_db, capsys):
    mock_cursor, mock_conn = mock_db
    f_alis_msgerr.dwmsg_setze_stichtag_info("12345", "20260330", "YYYYMMDD")
    
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args[0]
    assert "BERT_MELDUNG.SetzeZusatzInfos" in call_args[0]
    assert call_args[1] == [12345, "20260330", "YYYYMMDD"]

def test_dwmsg_setze_stichtag_info_missing_fmt(capsys):
    with pytest.raises(SystemExit) as exc_info:
        f_alis_msgerr.dwmsg_setze_stichtag_info("12345", "20260330", "")
    assert exc_info.value.code == 2
    stderr = capsys.readouterr().err
    assert "Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!" in stderr

def test_dwmsg_append_timing_infos(mock_db):
    mock_cursor, mock_conn = mock_db
    f_alis_msgerr.dwmsg_append_timing_infos("12345", "Step 1 Completed", "YYYY-MM-DD HH24:MI:SS")
    
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args[0]
    assert "to_char(SYSDATE, :3)" in call_args[0]
    assert call_args[1] == [12345, "Step 1 Completed", "YYYY-MM-DD HH24:MI:SS"]
```

---

## 3. Test Case 2: `h_alis_date` Validation (Date Calculations & Formatting)

### Purpose
Verify that the migrated Python date utility functions perform leap-year-aware date arithmetic, format conversions, and range calculations identically to the legacy Oracle-backed shell functions.

### Setup
No database connection is required because the migrated module uses native Python `datetime` and `calendar` libraries.

### Action
Execute unit tests covering:
- Oracle-to-Python date format translation.
- Previous month calculation.
- Date validity checking.
- Date comparison (`datum1 <= datum2`).
- Period generation (`DWDate_Gib_Zeitraum`) for Days, Months, and Years.
- Last day of month detection.
- Days in month calculation.
- Date addition with month/year overflow.

### Pass/Fail Criteria
- **Pass**:
  - All date calculations match legacy expectations (including leap years like 2000, 2024, and non-leap years like 1900, 2023).
  - `DWDate_Datum_LE` raises a `ValueError` with the exact legacy German message when `datum1 > datum2`.
  - `DWDate_Gib_Zeitraum` returns the correct start and end dates aligned to month/year boundaries.
- **Fail**: Any calculation mismatch, incorrect format translation, or missing exception propagation.

### Test Code (`test_h_alis_date.py`)

```python
import pytest
import datetime
import h_alis_date

def test_translate_oracle_format():
    assert h_alis_date.translate_oracle_format("YYYYMMDD") == "%Y%m%d"
    assert h_alis_date.translate_oracle_format("DD.MM.YYYY HH24:MI:SS") == "%d.%m.%Y %H:%M:%S"

def test_dwdate_vormonat():
    # Relative to today, verify it returns the previous month formatted correctly
    today = datetime.date.today()
    first_of_this_month = today.replace(day=1)
    expected_date = first_of_this_month - datetime.timedelta(days=1)
    
    res = h_alis_date.DWDate_Vormonat("YYYYMM")
    assert res == expected_date.strftime("%Y%m")

def test_dwdate_datum_check():
    assert h_alis_date.DWDate_Datum_Check("20260330", "YYYYMMDD") is True
    assert h_alis_date.DWDate_Datum_Check("30.03.2026", "DD.MM.YYYY") is True
    assert h_alis_date.DWDate_Datum_Check("20260230", "YYYYMMDD") is False  # Invalid day
    assert h_alis_date.DWDate_Datum_Check("invalid", "YYYYMMDD") is False

def test_dwdate_datum_le_valid():
    assert h_alis_date.DWDate_Datum_LE("20260301", "20260330") is True
    assert h_alis_date.DWDate_Datum_LE("20260330", "20260330") is True

def test_dwdate_datum_le_invalid():
    with pytest.raises(ValueError) as exc_info:
        h_alis_date.DWDate_Datum_LE("20260330", "20260301")
    assert "Datum 20260330 ist groesser als 20260301" in str(exc_info.value)

def test_dwdate_gib_zeitraum_days():
    # Offset of +5 days
    start, ende = h_alis_date.DWDate_Gib_Zeitraum(5, "D", "YYYYMMDD")
    start_dt = datetime.datetime.strptime(start, "%Y%m%d").date()
    ende_dt = datetime.datetime.strptime(ende, "%Y%m%d").date()
    assert start_dt == datetime.date.today()
    assert ende_dt == start_dt + datetime.timedelta(days=5)

def test_dwdate_gib_zeitraum_months():
    # Offset of +2 months. Start must be 1st of current month, end must be last day of target month.
    start, ende = h_alis_date.DWDate_Gib_Zeitraum(2, "M", "YYYYMMDD")
    start_dt = datetime.datetime.strptime(start, "%Y%m%d").date()
    ende_dt = datetime.datetime.strptime(ende, "%Y%m%d").date()
    
    assert start_dt.day == 1
    assert start_dt.month == datetime.date.today().month
    assert ende_dt.day in [28, 29, 30, 31]  # Must be last day of month

def test_letzter_tag_des_monats():
    assert h_alis_date.LetzterTagDesMonats("20260331") == 0  # True (0 in legacy)
    assert h_alis_date.LetzterTagDesMonats("20260330") == 1  # False (1 in legacy)
    assert h_alis_date.LetzterTagDesMonats("20240229") == 0  # Leap year Feb
    assert h_alis_date.LetzterTagDesMonats("20230228") == 0  # Non-leap year Feb

def test_tage_im_monat():
    assert h_alis_date.TageimMonat("2026", "03") == 31
    assert h_alis_date.TageimMonat("2024", "02") == 29  # Leap year
    assert h_alis_date.TageimMonat("2023", "02") == 28  # Non-leap year

def test_addiere_datum():
    assert h_alis_date.AddiereDatum("20260328", 5) == "20260402"
    assert h_alis_date.AddiereDatum("20231230", 5) == "20240104"  # Year overflow
```

---

## 4. Test Case 3: `h_alis_parameter` Validation (Parameter Normalization & Business Rules)

### Purpose
Verify that parameter parsing, metric/system normalization, and business rule validations (e.g., system-metric compatibility) behave identically to the legacy KornShell implementation.

### Setup
- Initialize the global state variables `h_alis_parameter.ErrNr` and `h_alis_parameter.ErrArg` before each test.
- Mock external subprocess calls to `DWDate_Datum_Check` and `DWDate_Datum_LE` inside `pruefeZeitraum` to isolate parameter logic.

### Action
Execute unit tests covering:
- Parameter presence checks (`pruefeParameterGesetzt`).
- Metric normalization (`konvertiereKennzahl`).
- System normalization (`konvertiereSystem`).
- Master data normalization (`konvertiereSDName`).
- System-metric compatibility checks (`pruefeSystemKennzahl`).
- Business area mapping (`gibBereich`).
- Time granularity mapping (`gibIntervall`).
- Relative time parameter validation (`pruefeZeitParameter`).

### Pass/Fail Criteria
- **Pass**:
  - Global state variables `ErrNr` and `ErrArg` are updated with the exact legacy error codes and messages on validation failures.
  - Normalization functions map verbose German strings to correct shortcodes (e.g., `"zugang"` -> `"zug"`, `"sap"` -> `"sap"`).
  - Invalid system-metric combinations (e.g., system `"sap"` with metric `"zug"`) correctly trigger `ErrNr = 195`.
  - Area and interval mappings match the legacy lists exactly.
- **Fail**: Any mismatch in mapped codes, incorrect error numbers, or failure to block execution when `ErrNr != 0`.

### Test Code (`test_h_alis_parameter.py`)

```python
import pytest
import os
import h_alis_parameter

@pytest.fixture(autouse=True)
def run_around_tests():
    # Reset global error state before each test
    h_alis_parameter.reset_errors()
    yield

def test_pruefe_parameter_gesetzt_success():
    os.environ["TEST_VAR"] = "filled_value"
    h_alis_parameter.pruefeParameterGesetzt("Test Parameter", "TEST_VAR")
    assert h_alis_parameter.ErrNr == 0

def test_pruefe_parameter_gesetzt_missing():
    if "TEST_VAR" in os.environ:
        del os.environ["TEST_VAR"]
    h_alis_parameter.pruefeParameterGesetzt("Test Parameter", "TEST_VAR")
    assert h_alis_parameter.ErrNr == 194
    assert h_alis_parameter.ErrArg == "Test Parameter"

def test_konvertiere_kennzahl():
    assert h_alis_parameter.konvertiereKennzahl("zugang") == "zug"
    assert h_alis_parameter.konvertiereKennzahl("bestand") == "bst"
    assert h_alis_parameter.konvertiereKennzahl("standard_rechnung") == "srs"
    assert h_alis_parameter.konvertiereKennzahl("unknown_metric") == "???"
    assert h_alis_parameter.ErrNr == 198

def test_konvertiere_system():
    assert h_alis_parameter.konvertiereSystem("SAP") == "sap"
    assert h_alis_parameter.konvertiereSystem("carmen") == "carmen"
    assert h_alis_parameter.konvertiereSystem("invalid_sys") == "???"
    assert h_alis_parameter.ErrNr == 195

def test_pruefe_system_kennzahl_valid():
    h_alis_parameter.pruefeSystemKennzahl("sap", "srs")
    assert h_alis_parameter.ErrNr == 0

def test_pruefe_system_kennzahl_invalid():
    # SAP is not allowed to load raw 'zug' (zugang) metrics
    h_alis_parameter.pruefeSystemKennzahl("sap", "zug")
    assert h_alis_parameter.ErrNr == 195
    assert "Ungueltige Kombination sap zug" in h_alis_parameter.ErrArg

def test_gib_bereich():
    assert h_alis_parameter.gibBereich("zug") == "tn"
    assert h_alis_parameter.gibBereich("gut") == "us"
    assert h_alis_parameter.gibBereich("tvd") == "gd"
    assert h_alis_parameter.gibBereich("ksd") == "sd"
    assert h_alis_parameter.gibBereich("mds") == "md"
    
    # Unknown metric
    res = h_alis_parameter.gibBereich("invalid")
    assert res == ""
    assert h_alis_parameter.ErrNr == 196

def test_gib_intervall():
    assert h_alis_parameter.gibIntervall("zug") == "t"  # Daily
    assert h_alis_parameter.gibIntervall("bst") == "m"  # Monthly

def test_pruefe_zahl_positiv():
    h_alis_parameter.pruefeZahlPositiv("10", "Offset")
    assert h_alis_parameter.ErrNr == 0
    
    h_alis_parameter.pruefeZahlPositiv("-5", "Offset")
    assert h_alis_parameter.ErrNr == 195
    assert "muss groesser gleich 0 sein" in h_alis_parameter.ErrArg

def test_pruefe_zeit_parameter_exclusivity():
    # Setting both dates AND offset is forbidden
    h_alis_parameter.pruefeZeitParameter("20260301", "20260330", "10")
    assert h_alis_parameter.ErrNr == 195
    assert "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden" in h_alis_parameter.ErrArg
```

---

## 5. Test Case 4: `h_alis_sqlplus` Validation (SQL Script Execution & Error Propagation)

### Purpose
Verify that the SQL script execution wrapper validates parameters, checks file readability, logs execution metadata, and correctly propagates the return code of the executed subprocess.

### Setup
- Set environment variable `DW_ORAUSER=test_user/test_pass@test_dsn`.
- Mock `subprocess.run` to capture the command line execution without launching an actual `sqlplus` process.
- Mock `os.path.exists` and `os.access` to simulate readable and unreadable SQL files.

### Action
Execute unit tests covering:
- Missing parameters (Eintragsnr or Script path).
- Missing or unreadable SQL script files.
- Successful execution of SQL*Plus with arguments.
- Subprocess failure propagation.

### Pass/Fail Criteria
- **Pass**:
  - Missing parameters trigger error code `196` and call `dwmsg_melde_fehler`.
  - Unreadable files trigger error code `201` and call `dwmsg_melde_fehler`.
  - Valid executions call `subprocess.run` with the exact command structure: `['sqlplus', 'test_user/test_pass@test_dsn', '@<script_path>', 'arg1', 'arg2']` and redirect standard input to `DEVNULL`.
  - The function returns the exact exit code of the subprocess.
- **Fail**: Any deviation in command-line arguments, failure to detect unreadable files, or incorrect return code propagation.

### Test Code (`test_h_alis_sqlplus.py`)

```python
import pytest
import os
import subprocess
from unittest.mock import patch, MagicMock

os.environ["DW_ORAUSER"] = "test_user/test_pass@test_dsn"

import h_alis_sqlplus

@patch("h_alis_sqlplus.dwmsg_melde_fehler")
def test_starte_sql_skript_missing_params(mock_melde_fehler):
    rc = h_alis_sqlplus.starte_sql_skript("", "/tmp/test.sql")
    assert rc == 196
    mock_melde_fehler.assert_called_once_with("", "E", 196, "alis_sqlplus V1.1.3 starteSQLSkript")

@patch("h_alis_sqlplus.dwmsg_melde_fehler")
@patch("os.path.exists", return_value=False)
def test_starte_sql_skript_file_not_found(mock_exists, mock_melde_fehler):
    rc = h_alis_sqlplus.starte_sql_skript("12345", "/tmp/nonexistent.sql")
    assert rc == 201
    mock_melde_fehler.assert_called_once_with("12345", "E", 201, "/tmp/nonexistent.sql")

@patch("os.path.exists", return_value=True)
@patch("os.access", return_value=True)
@patch("subprocess.run")
def test_starte_sql_skript_success(mock_run, mock_access, mock_exists):
    # Mock subprocess to return exit code 0
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_run.return_value = mock_proc
    
    rc = h_alis_sqlplus.starte_sql_skript("12345", "/tmp/test.sql", "param1", "param2")
    
    assert rc == 0
    mock_run.assert_called_once_with(
        ["sqlplus", "test_user/test_pass@test_dsn", "@/tmp/test.sql", "param1", "param2"],
        stdin=subprocess.DEVNULL,
        check=False
    )

@patch("os.path.exists", return_value=True)
@patch("os.access", return_value=True)
@patch("subprocess.run")
def test_starte_sql_skript_failure_propagation(mock_run, mock_access, mock_exists):
    # Mock subprocess to return exit code 12
    mock_proc = MagicMock()
    mock_proc.returncode = 12
    mock_run.return_value = mock_proc
    
    rc = h_alis_sqlplus.starte_sql_skript("12345", "/tmp/test.sql")
    
    assert rc == 12
```

---

## 6. Test Case 5: End-to-End Integration & Parity Verification

### Purpose
Prove that the migrated Python modules can be chained together to perform a complete operational sequence (initialize run, validate parameters, calculate dates, and execute a script) with identical behavior and state transitions as the legacy KornShell framework.

### Setup
- Set up a mock database environment using the `mock_db` fixture.
- Create a dummy SQL script file `/tmp/test_run.sql` to satisfy readability checks.
- Set environment variables:
  ```bash
  export DW_ORAUSER="test_user/test_pass@test_dsn"
  export DW_DIR_ROOT="/tmp/root"
  export DW_DIR_PROT="/tmp/prot"
  ```

### Action
Execute a simulated ETL orchestration sequence using the migrated Python modules:
1. Initialize a tracking entry ID (`DWMSG_ErmittleNr` / `dwmsg_ermittle_nr`).
2. Generate a standardized log filename (`DWMSG_Logdateiname` / `dwmsg_logdateiname`).
3. Create the database log entry (`DWMSG_ErzeugeEintrag` / `dwmsg_erzeuge_eintrag`).
4. Normalize and validate incoming parameters:
   - System: `"SAP"`
   - Metric: `"standard_rechnung"`
   - Verify compatibility.
5. Calculate the execution date window:
   - Start: Today
   - End: Today + 5 days
6. Execute the database script `/tmp/test_run.sql` passing the calculated dates as arguments.
7. Update the run status to successful (`DWMSG_SetzeStatusOK` / `dwmsg_setze_status_ok`).

### Pass/Fail Criteria
- **Pass**:
  - The entire sequence executes without raising unhandled exceptions.
  - All intermediate parameters are correctly normalized (`"SAP"` -> `"sap"`, `"standard_rechnung"` -> `"srs"`).
  - The database mock captures the correct sequence of procedure calls:
    1. `BERT_MELDUNG.Erzeuge_Eintrag`
    2. `BERT_MELDUNG.SetzeStatusOk`
  - The subprocess mock captures the execution of `sqlplus` with the correct date arguments.
- **Fail**: Any unhandled exception, validation failure, or incorrect sequence of database/subprocess calls.

### Test Code (`test_integration_parity.py`)

```python
import os
import pytest
import subprocess
from unittest.mock import MagicMock, patch

# Set up environment
os.environ["DW_ORAUSER"] = "test_user/test_pass@test_dsn"
os.environ["DW_DIR_ROOT"] = "/tmp/root"
os.environ["DW_DIR_PROT"] = "/tmp/prot"

import f_alis_msgerr
import h_alis_date
import h_alis_parameter
import h_alis_sqlplus

@patch("f_alis_msgerr.get_db_connection")
@patch("subprocess.run")
@patch("os.path.exists", return_value=True)
@patch("os.access", return_value=True)
def test_e2e_orchestration_flow(mock_access, mock_exists, mock_run, mock_db_conn):
    # 1. Setup DB Mocks
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_db_conn.return_value = mock_conn

    # Mock subprocess.run for h_alis_sqlplus and f_alis_msgerr.dwmsg_ermittle_nr
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = "Anfangsdatum=20260330\nEndedatum=20260404"
    mock_run.return_value = mock_proc

    # 2. Step 1: Initialize tracking ID (Mocking file read for ErmittleNr)
    with patch("builtins.open", pytest.mock.mock_open(read_data="123456")):
        entry_id = f_alis_msgerr.dwmsg_ermittle_nr("MY_RUN_ID")
    assert entry_id == "123456"

    # 3. Step 2: Generate log filename
    log_file = f_alis_msgerr.dwmsg_logdateiname("MY_LOG_FILE", "JOB_INTEGRATION", entry_id)
    assert "JOB_INTEGRATION" in log_file
    assert "123456.log" in log_file

    # 4. Step 3: Create database log entry
    f_alis_msgerr.dwmsg_erzeuge_eintrag(entry_id, "JOB_INTEGRATION", "test_integration_parity", log_file)
    mock_cursor.callproc.assert_any_call(
        "BERT_MELDUNG.Erzeuge_Eintrag", [123456, "JOB_INTEGRATION", "test_integration_parity", log_file]
    )

    # 5. Step 4: Normalize and validate parameters
    sys_normalized = h_alis_parameter.konvertiereSystem("SAP")
    metric_normalized = h_alis_parameter.konvertiereKennzahl("standard_rechnung")
    
    assert sys_normalized == "sap"
    assert metric_normalized == "srs"
    
    h_alis_parameter.pruefeSystemKennzahl(sys_normalized, metric_normalized)
    assert h_alis_parameter.ErrNr == 0

    # 6. Step 5: Calculate execution date window (5 days offset)
    start_date, end_date = h_alis_date.DWDate_Gib_Zeitraum(5, "D", "YYYYMMDD")

    # 7. Step 6: Execute SQL script with calculated dates
    rc = h_alis_sqlplus.starte_sql_skript(entry_id, "/tmp/test_run.sql", start_date, end_date)
    assert rc == 0
    mock_run.assert_called_with(
        ["sqlplus", "test_user/test_pass@test_dsn", "@/tmp/test_run.sql", start_date, end_date],
        stdin=subprocess.DEVNULL,
        check=False
    )

    # 8. Step 7: Update run status to OK
    f_alis_msgerr.dwmsg_setze_status_ok(entry_id)
    mock_cursor.callproc.assert_any_call("BERT_MELDUNG.SetzeStatusOk", [123456])
    assert mock_conn.commit.call_count == 3  # ErzeugeEintrag, SetzeStatusOk, and internal commits
```