# Migration Validation Test Suite: Shared Utility Libraries

This document defines the migration-validation test suite for the migrated Python utility libraries located in `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`. These tests prove that the migrated Python modules are behaviorally equivalent to the legacy KornShell scripts, covering output parity, edge cases, stateful error handling, and external system interactions.

---

## Test Suite 1: `h_alis_date.py` Behavioral Equivalence

This suite validates the date arithmetic, format translation, range calculation, and boundary logic of `h_alis_date.py` without requiring database round-trips.

### Test Case 1.1: `DWDate_Vormonat` Output Parity & Format Translation
* **Purpose**: Verify that `DWDate_Vormonat` correctly calculates the previous month relative to the current system time and formats it according to Oracle-style format masks translated to Python equivalents.
* **Setup**: 
  * System clock frozen or mocked to a specific date (e.g., `2023-10-15`) using `freezegun` or standard unittest mocking.
* **Action**: Call `DWDate_Vormonat` with various Oracle format masks (`YYYYMM`, `YYYY-MM-DD`, `YYYYMMDD`).
* **Pass/Fail Criterion**: The returned string must match the expected formatted previous month (`202309`, `2023-09-30`, `20230930`).

```python
import datetime
import pytest
from unittest.mock import patch
import h_alis_date

@pytest.mark.parametrize("oracle_fmt, expected", [
    ("YYYYMM", "202309"),
    ("YYYY-MM-DD", "2023-09-30"),
    ("YYYYMMDD", "20230930")
])
def test_dw_date_vormonat_parity(oracle_fmt, expected):
    # Freeze time to 2023-10-15
    mocked_now = datetime.datetime(2023, 10, 15, 12, 0, 0)
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = mocked_now
        mock_datetime.strptime = datetime.datetime.strptime
        
        result = h_alis_date.DWDate_Vormonat(oracle_fmt)
        assert result == expected
```

### Test Case 1.2: `DWDate_Datum_Check` and `DWDate_Datum_LE` Edge Cases
* **Purpose**: Verify that date validation and chronological comparison functions handle valid dates, invalid dates, leap years, and chronological violations correctly.
* **Setup**: None.
* **Action**: 
  * Call `DWDate_Datum_Check` with valid/invalid dates and formats.
  * Call `DWDate_Datum_LE` with valid chronological order and chronological violations.
* **Pass/Fail Criterion**: 
  * `DWDate_Datum_Check` must return `0` for valid dates and `1` for invalid dates.
  * `DWDate_Datum_LE` must return `0` if `datum1 <= datum2`, and `1` if `datum1 > datum2`, printing the exact legacy German error message to `stderr`.

```python
import sys
import h_alis_date

def test_dw_date_datum_check():
    # Valid dates
    assert h_alis_date.DWDate_Datum_Check("20240229", "YYYYMMDD") == 0  # Leap year
    assert h_alis_date.DWDate_Datum_Check("31.12.2023", "DD.MM.YYYY") == 0
    
    # Invalid dates
    assert h_alis_date.DWDate_Datum_Check("20230229", "YYYYMMDD") == 1  # Non-leap year
    assert h_alis_date.DWDate_Datum_Check("20231301", "YYYYMMDD") == 1  # Invalid month
    assert h_alis_date.DWDate_Datum_Check("", "YYYYMMDD") == 1

def test_dw_date_datum_le(capsys):
    # Chronologically correct
    assert h_alis_date.DWDate_Datum_LE("20231015", "20231016") == 0
    assert h_alis_date.DWDate_Datum_LE("20231015", "20231015") == 0
    
    # Chronological violation
    rc = h_alis_date.DWDate_Datum_LE("20231016", "20231015")
    assert rc == 1
    
    # Verify exact legacy German error message in stderr
    stderr = capsys.readouterr().err
    assert "Datum 20231016 ist groesser als 20231015" in stderr
```

### Test Case 1.3: `DWDate_Gib_Zeitraum` Range Calculations
* **Purpose**: Verify that `DWDate_Gib_Zeitraum` correctly calculates start and end dates based on offsets and time units ('D', 'M', 'Y').
* **Setup**: Freeze system time to `2023-10-15`.
* **Action**: Call `DWDate_Gib_Zeitraum` with different offsets and units.
* **Pass/Fail Criterion**: 
  * Unit 'D' (Days): Start is today, End is today + offset.
  * Unit 'M' (Months): Start is the 1st of the current month, End is the last day of the offset month.
  * Unit 'Y' (Years): Start is Jan 1st of the current year, End is Dec 31st of the offset year.

```python
import datetime
import pytest
from unittest.mock import patch
import h_alis_date

def test_dw_date_gib_zeitraum():
    mocked_now = datetime.datetime(2023, 10, 15, 12, 0, 0)
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = mocked_now
        mock_datetime.strptime = datetime.datetime.strptime
        
        # Test Days Offset (+5 days)
        start, end = h_alis_date.DWDate_Gib_Zeitraum(5, "D", "YYYYMMDD")
        assert start == "20231015"
        assert end == "20231020"
        
        # Test Months Offset (-2 months)
        # Start should be 1st of current month (2023-10-01)
        # End should be last day of August (2023-08-31)
        start, end = h_alis_date.DWDate_Gib_Zeitraum(-2, "M", "YYYYMMDD")
        assert start == "20231001"
        assert end == "20230831"
        
        # Test Years Offset (+1 year)
        # Start should be Jan 1st of current year (2023-01-01)
        # End should be Dec 31st of next year (2024-12-31)
        start, end = h_alis_date.DWDate_Gib_Zeitraum(1, "Y", "YYYYMMDD")
        assert start == "20230101"
        assert end == "202412-31"
```

### Test Case 1.4: `LetzterTagDesMonats`, `TageimMonat`, and `AddiereDatum` Boundary Tests
* **Purpose**: Verify low-level calendar math functions for leap years, month lengths, and multi-month/multi-year date additions.
* **Setup**: None.
* **Action**: Execute boundary tests for month-end detection, day counts, and date additions.
* **Pass/Fail Criterion**: Calculations must match standard Gregorian calendar rules.

```python
import h_alis_date

def test_letzter_tag_des_monats():
    assert h_alis_date.LetzterTagDesMonats("20240229") == 0  # Leap year Feb end
    assert h_alis_date.LetzterTagDesMonats("20240228") == 1  # Not Feb end in leap year
    assert h_alis_date.LetzterTagDesMonats("20230228") == 0  # Non-leap year Feb end
    assert h_alis_date.LetzterTagDesMonats("20231231") == 0  # Dec end

def test_tage_im_monat():
    assert h_alis_date.TageimMonat("2024", "02") == 29
    assert h_alis_date.TageimMonat("2023", "02") == 28
    assert h_alis_date.TageimMonat("2023", "11") == 30

def test_addiere_datum():
    # Standard addition
    assert h_alis_date.AddiereDatum("20231015", "10") == "20231025"
    # Month rollover
    assert h_alis_date.AddiereDatum("20231025", "10") == "20231104"
    # Year rollover
    assert h_alis_date.AddiereDatum("20231225", "10") == "20240104"
    # Leap year rollover
    assert h_alis_date.AddiereDatum("20240225", "5") == "20240301"
```

---

## Test Suite 2: `h_alis_parameter.py` Validation & Normalization

This suite validates parameter parsing, normalization, and compatibility matrices. It ensures that the stateful environment variables (`ErrNr`, `ErrArg`) are mutated correctly.

### Test Case 2.1: Parameter Normalization
* **Purpose**: Verify that verbose parameter names for metrics (Kennzahlen), source systems, and master data are correctly normalized to their 3-letter codes and written back to the environment.
* **Setup**: Clear `os.environ` of `ErrNr` and `ErrArg`.
* **Action**: Set environment variables and call normalization functions.
* **Pass/Fail Criterion**: Target environment variables must contain the correct normalized codes. Unrecognized values must set `ErrNr` and `ErrArg` appropriately.

```python
import os
import pytest
import h_alis_parameter

@pytest.fixture(autouse=True)
def cleanup_env():
    os.environ.pop("ErrNr", None)
    os.environ.pop("ErrArg", None)
    yield

def test_konvertiere_kennzahl_success():
    os.environ["TEST_VAR"] = "bestand"
    h_alis_parameter.konvertiereKennzahl("TEST_VAR")
    assert os.environ["TEST_VAR"] == "bst"
    assert int(os.environ.get("ErrNr", "0")) == 0

def test_konvertiere_kennzahl_failure():
    os.environ["TEST_VAR"] = "invalid_metric"
    h_alis_parameter.konvertiereKennzahl("TEST_VAR")
    assert os.environ["TEST_VAR"] == "???"
    assert int(os.environ.get("ErrNr")) == 198
    assert os.environ.get("ErrArg") == "invalid_metric"

def test_konvertiere_system():
    os.environ["SYS_VAR"] = "CARMEN"
    h_alis_parameter.konvertiereSystem("SYS_VAR")
    assert os.environ["SYS_VAR"] == "carmen"
    
    os.environ["SYS_VAR"] = "INVALID_SYS"
    h_alis_parameter.konvertiereSystem("SYS_VAR")
    assert os.environ["SYS_VAR"] == "???"
    assert int(os.environ.get("ErrNr")) == 195
    assert "Unbekannte Datenherkunft" in os.environ.get("ErrArg")
```

### Test Case 2.2: System-Metric Compatibility Matrix
* **Purpose**: Verify that the compatibility matrix between source systems and metrics is strictly enforced.
* **Setup**: Clear `os.environ` of `ErrNr` and `ErrArg`.
* **Action**: Call `pruefeSystemKennzahl` with valid and invalid combinations.
* **Pass/Fail Criterion**: Valid combinations must leave `ErrNr` as `0`. Invalid combinations must set `ErrNr` to `195` and populate `ErrArg` with the exact legacy error string.

```python
import os
import pytest
import h_alis_parameter

@pytest.fixture(autouse=True)
def cleanup_env():
    os.environ.pop("ErrNr", None)
    os.environ.pop("ErrArg", None)
    yield

@pytest.mark.parametrize("system, kennzahl, should_pass", [
    ("carmen", "zug", True),
    ("carmen", "twe", False),  # Invalid: Carmen cannot do tarifwechsel (twe)
    ("sap", "srs", True),
    ("sap", "zug", False),     # Invalid: SAP cannot do zugang (zug)
    ("xtra", "rst", True),
    ("xtra", "zug", False)     # Invalid: Xtra can only do restguthaben (rst)
])
def test_pruefe_system_kennzahl(system, kennzahl, should_pass):
    h_alis_parameter.pruefeSystemKennzahl(system, kennzahl)
    if should_pass:
        assert int(os.environ.get("ErrNr", "0")) == 0
    else:
        assert int(os.environ.get("ErrNr", "0")) == 195
        assert os.environ.get("ErrArg") == f"Ungueltige Kombination {system} {kennzahl}"
```

### Test Case 2.3: Domain and Interval Mapping
* **Purpose**: Verify that metrics are correctly mapped to their functional domains (Bereiche: `tn`, `us`, `gd`, `sd`, `md`) and reporting intervals (Intervalle: `t` for daily, `m` for monthly).
* **Setup**: Clear `os.environ` of `ErrNr` and `ErrArg`.
* **Action**: Call `gibBereich` and `gibIntervall` for various metrics.
* **Pass/Fail Criterion**: The mapped domain and interval must match the legacy specification.

```python
import os
import pytest
import h_alis_parameter

@pytest.fixture(autouse=True)
def cleanup_env():
    os.environ.pop("ErrNr", None)
    os.environ.pop("ErrArg", None)
    yield

def test_gib_bereich():
    # 'zug' (zugang) belongs to subscriber domain 'tn'
    h_alis_parameter.gibBereich("zug", "OUT_BEREICH")
    assert os.environ["OUT_BEREICH"] == "tn"
    
    # 'srs' (standard_rechnung) belongs to revenue domain 'us'
    h_alis_parameter.gibBereich("srs", "OUT_BEREICH")
    assert os.environ["OUT_BEREICH"] == "us"

def test_gib_intervall():
    # 'zug' is daily ('t')
    h_alis_parameter.gibIntervall("zug", "OUT_INTERVAL")
    assert os.environ["OUT_INTERVAL"] == "t"
    
    # 'bst' (bestand) is monthly ('m')
    h_alis_parameter.gibIntervall("bst", "OUT_INTERVAL")
    assert os.environ["OUT_INTERVAL"] == "m"
```

### Test Case 2.4: Stateful Error Tracking and Mutually Exclusive Time Parameters
* **Purpose**: Verify that validation routines respect existing error states (early exit) and enforce mutual exclusivity between absolute dates and relative offsets.
* **Setup**: Clear `os.environ` of `ErrNr` and `ErrArg`.
* **Action**: 
  * Call validation with `ErrNr` pre-set to a non-zero value.
  * Call `pruefeZeitParameter` with conflicting or missing parameters.
* **Pass/Fail Criterion**: 
  * If `ErrNr != 0`, functions must exit immediately without executing or overwriting the error.
  * `pruefeZeitParameter` must fail if both offset and dates are set, or if both are missing.

```python
import os
import pytest
import h_alis_parameter

@pytest.fixture(autouse=True)
def cleanup_env():
    os.environ.pop("ErrNr", None)
    os.environ.pop("ErrArg", None)
    yield

def test_early_exit_on_existing_error():
    # Pre-set error state
    os.environ["ErrNr"] = "999"
    os.environ["ErrArg"] = "Pre-existing error"
    
    # This call should normally fail and set ErrNr=195, but must exit early
    h_alis_parameter.pruefeSystemKennzahl("carmen", "twe")
    
    assert os.environ["ErrNr"] == "999"
    assert os.environ["ErrArg"] == "Pre-existing error"

def test_pruefe_zeit_parameter_exclusivity():
    # Error: Both absolute dates and relative offset are set
    h_alis_parameter.pruefeZeitParameter("20231015", "20231020", "5")
    assert int(os.environ.get("ErrNr", "0")) == 195
    assert "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden" in os.environ.get("ErrArg")

def test_pruefe_zeit_parameter_missing():
    # Error: All parameters are empty
    os.environ.pop("ErrNr", None)
    h_alis_parameter.pruefeZeitParameter("", "", "")
    assert int(os.environ.get("ErrNr", "0")) == 195
    assert "Datumswerte oder Zeitspanne fehlen" in os.environ.get("ErrArg")
```

---

## Test Suite 3: `f_alis_msgerr.py` Database-Backed Auditing & Logging

This suite validates the logging, status tracking, and error telemetry dispatch of `f_alis_msgerr.py`. It uses mocks to simulate database interactions via `oracledb`.

### Test Case 3.1: Log Path Generation
* **Purpose**: Verify that `dwmsg_logdateiname` constructs log file paths matching the legacy naming convention and directory structure.
* **Setup**: Set `DW_DIR_PROT` environment variable. Freeze system time to `2023-10-15 12:34`.
* **Action**: Call `dwmsg_logdateiname`.
* **Pass/Fail Criterion**: The returned path must match `{DW_DIR_PROT}/{job_kennung}_{YYYYMMDD_HHMM}_{eintrags_nr}.log`.

```python
import os
import datetime
from unittest.mock import patch
import f_alis_msgerr

def test_dwmsg_logdateiname():
    os.environ["DW_DIR_PROT"] = "/var/log/dwh"
    mocked_now = datetime.datetime(2023, 10, 15, 12, 34, 56)
    
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = mocked_now
        
        log_path = f_alis_msgerr.dwmsg_logdateiname("VAR", "JOB_BERT_01", 12345)
        assert log_path == "/var/log/dwh/JOB_BERT_01_20231015_1234_12345.log"
```

### Test Case 3.2: Database Status Transitions
* **Purpose**: Verify that status transition functions execute the correct PL/SQL stored procedures on the `BERT_MELDUNG` package.
* **Setup**: Mock `oracledb.connect` and the database cursor.
* **Action**: Call `dwmsg_setze_status_ok` and `dwmsg_setze_status_abbruch`.
* **Pass/Fail Criterion**: The database cursor must execute the expected anonymous PL/SQL block with the correct bind variables.

```python
from unittest.mock import MagicMock, patch
import f_alis_msgerr

@patch('f_alis_msgerr.get_db_connection')
def test_dwmsg_status_transitions(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Test OK Status
    f_alis_msgerr.dwmsg_setze_status_ok(98765)
    mock_cursor.execute.assert_called_with(
        "BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", [98765]
    )
    mock_conn.commit.assert_called()
    
    # Test Aborted Status
    f_alis_msgerr.dwmsg_setze_status_abbruch(98765)
    mock_cursor.execute.assert_called_with(
        "BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [98765]
    )
```

### Test Case 3.3: Error Telemetry Dispatch
* **Purpose**: Verify that the error handler (`dwmsg_fehlerbehandlung`) dispatches the correct error code and severity to the database and transitions the job state to aborted.
* **Setup**: Mock `get_db_connection` and standard output.
* **Action**: Call `dwmsg_fehlerbehandlung` with an active error code.
* **Pass/Fail Criterion**: 
  * `BERT_MELDUNG.Fehler` must be called with severity `F` (Fatal), error code `10` (unexpected error), and the original error code in the context string.
  * `BERT_MELDUNG.SetzeStatusAbbruch` must be called.
  * The exact legacy German console output must be printed.

```python
from unittest.mock import MagicMock, patch
import f_alis_msgerr

@patch('f_alis_msgerr.get_db_connection')
def test_dwmsg_fehlerbehandlung(mock_get_conn, capsys):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Trigger error handler for entry 55555 with shell exit code 127
    f_alis_msgerr.dwmsg_fehlerbehandlung(55555, error_code=127)
    
    # Verify error reporting call
    mock_cursor.execute.assert_any_call(
        "BEGIN BERT_MELDUNG.Fehler(:1, :2, :3, :4, :5); END;",
        ["F", 55555, 10, "ErrorCode ist: 127", ""]
    )
    
    # Verify status abort call
    mock_cursor.execute.assert_any_call(
        "BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [55555]
    )
    
    # Verify exact legacy German console output
    stdout = capsys.readouterr().out
    assert "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus" in stdout
```

### Test Case 3.4: Supplemental Metadata Updates
* **Purpose**: Verify that supplemental metadata updates (`dwmsg_setze_stichtag_info` and `dwmsg_append_timing_infos`) correctly bind and format date parameters inside PL/SQL blocks.
* **Setup**: Mock `get_db_connection` and database cursor.
* **Action**: Call metadata update functions.
* **Pass/Fail Criterion**: The PL/SQL blocks must match the legacy inline SQL statements exactly, including the `to_date` and `to_char` conversions.

```python
from unittest.mock import MagicMock, patch
import f_alis_msgerr

@patch('f_alis_msgerr.get_db_connection')
def test_dwmsg_metadata_updates(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Test Stichtag Info Update
    f_alis_msgerr.dwmsg_setze_stichtag_info(44444, "2023-10-15", "YYYY-MM-DD")
    expected_plsql_stichtag = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, to_date(:stichtag, :stichtag_fmt));
                COMMIT;
            END;
            """
    mock_cursor.execute.assert_any_call(
        expected_plsql_stichtag,
        eintrags_nr=44444,
        stichtag="2023-10-15",
        stichtag_fmt="YYYY-MM-DD"
    )
    
    # Test Timing Info Append
    f_alis_msgerr.dwmsg_append_timing_infos(44444, "Step 1 Completed", "YYYY-MM-DD HH24:MI:SS")
    expected_plsql_timing = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, NULL, :info_text || ' ' || to_char(SYSDATE, :date_format) || ' ');
                COMMIT;
            END;
            """
    mock_cursor.execute.assert_any_call(
        expected_plsql_timing,
        eintrags_nr=44444,
        info_text="Step 1 Completed",
        date_format="YYYY-MM-DD HH24:MI:SS"
    )
```

---

## Test Suite 4: `h_alis_sqlplus.py` Execution Wrapper

This suite validates the pre-execution checks, argument validation, and subprocess execution wrapping of `h_alis_sqlplus.py`.

### Test Case 4.1: Pre-execution File and Argument Validations
* **Purpose**: Verify that `starteSQLSkript` performs strict pre-execution checks on arguments and file readability, logging errors and returning correct status codes without invoking the database.
* **Setup**: Mock `subprocess.run` to intercept calls to `DWMSG_MeldeFehler`. Create a temporary unreadable file or mock `os.access`.
* **Action**: 
  * Call `starteSQLSkript` with missing arguments.
  * Call `starteSQLSkript` with a non-existent or unreadable script path.
* **Pass/Fail Criterion**: 
  * Missing arguments must return `196` and call `DWMSG_MeldeFehler` with code `196`.
  * Unreadable files must return `201` and call `DWMSG_MeldeFehler` with code `201`.

```python
import os
from unittest.mock import patch, MagicMock
import h_alis_sqlplus

@patch('h_alis_sqlplus.subprocess.run')
def test_starte_sql_skript_validation_failures(mock_run):
    # Case 1: Missing required arguments
    rc = h_alis_sqlplus.starteSQLSkript("", "")
    assert rc == 196
    mock_run.assert_called_with(
        ["DWMSG_MeldeFehler", "", "E", "196", "alis_sqlplus V1.1.3 starteSQLSkript"],
        check=False
    )
    
    # Case 2: Script file does not exist / is not readable
    rc = h_alis_sqlplus.starteSQLSkript("11111", "/nonexistent/path.sql")
    assert rc == 201
    mock_run.assert_called_with(
        ["DWMSG_MeldeFehler", "11111", "E", "201", "/nonexistent/path.sql"],
        check=False
    )
```

### Test Case 4.2: Subprocess Execution and Exit Code Propagation
* **Purpose**: Verify that `starteSQLSkript` correctly invokes the `sqlplus` binary with the appropriate connection string, script path, and forwarded arguments, and propagates the exit code.
* **Setup**: 
  * Set `DW_ORAUSER` environment variable.
  * Mock `shutil.which` to simulate that `sqlplus` is installed.
  * Mock `os.path.isfile` and `os.access` to return `True` for the script path.
  * Mock `subprocess.run` to return a specific exit code (e.g., `0` for success, `2` for database error).
* **Action**: Call `starteSQLSkript` with valid parameters.
* **Pass/Fail Criterion**: 
  * The subprocess must be called with `sqlplus {DW_ORAUSER} @{script_path} {args}`.
  * Standard input must be redirected to `DEVNULL`.
  * The function must return the exact exit code of the subprocess.

```python
import os
from unittest.mock import patch, MagicMock
import h_alis_sqlplus

@patch('h_alis_sqlplus.subprocess.run')
@patch('h_alis_sqlplus.shutil.which')
@patch('h_alis_sqlplus.os.path.isfile')
@patch('h_alis_sqlplus.os.access')
def test_starte_sql_skript_execution_parity(mock_access, mock_isfile, mock_which, mock_run):
    # Setup environment and file mocks
    os.environ["DW_ORAUSER"] = "ops$dwh/password@dwh_db"
    mock_which.return_value = "/usr/bin/sqlplus"
    mock_isfile.return_value = True
    mock_access.return_value = True
    
    # Mock successful sqlplus execution (exit code 0)
    mock_response = MagicMock()
    mock_response.return_code = 0
    mock_run.return_value = mock_response
    
    rc = h_alis_sqlplus.starteSQLSkript("22222", "/dwh/sql/my_job.sql", "param1", "param2")
    
    # Verify exit code propagation
    assert rc == 0
    
    # Verify exact command line construction and DEVNULL redirection
    mock_run.assert_called_with(
        ["sqlplus", "ops$dwh/password@dwh_db", "@/dwh/sql/my_job.sql", "param1", "param2"],
        stdin=h_alis_sqlplus.subprocess.DEVNULL,
        check=False
    )
    
    # Mock failed sqlplus execution (exit code 2)
    mock_response.return_code = 2
    rc = h_alis_sqlplus.starteSQLSkript("22222", "/dwh/sql/my_job.sql", "param1")
    assert rc == 2
```