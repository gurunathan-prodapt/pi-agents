# Migration Validation Test Suite: Shared Utilities (`is/util/bin`)

This document defines the migration-validation tests to prove that the migrated Python modules (`f_alis_msgerr.py`, `h_alis_date.py`, `h_alis_parameter.py`, and `h_alis_sqlplus.py`) are behaviorally equivalent to their legacy KornShell counterparts.

---

## Group 1: Date Utility Validation (`h_alis_date.py`)

This group validates that the in-memory Python date calculations match the legacy Oracle-based and shell-based date calculations exactly, including leap years, month-end rollovers, and period calculations.

### Test 1.1: Leap Year and Days in Month Calculation
* **Purpose**: Verify that leap year detection and days-in-month calculations are correct, specifically handling century boundaries (e.g., 1900, 2000, 2004).
* **Setup**: 
  * Ensure `h_alis_date.py` is in the Python path.
* **Action**: Execute `tage_im_monat` and `letzter_tag_des_monats` for a matrix of leap and non-leap years.
* **Pass/Fail Criterion**: 
  * Year 2000 (leap year) must return 29 days for February.
  * Year 1900 (non-leap year) must return 28 days for February.
  * `letzter_tag_des_monats("20040229")` must return `True`.
  * `letzter_tag_des_monats("20040228")` must return `False`.

```python
import pytest
from h_alis_date import tage_im_monat, letzter_tag_des_monats, _is_leap_year

def test_leap_year_detection():
    assert _is_leap_year(2000) is True
    assert _is_leap_year(1900) is False
    assert _is_leap_year(2004) is True
    assert _is_leap_year(2023) is False

def test_tage_im_monat():
    assert tage_im_monat(2000, 2) == 29
    assert tage_im_monat(1900, 2) == 28
    assert tage_im_monat(2023, 2) == 28
    assert tage_im_monat(2023, 1) == 31
    assert tage_im_monat(2023, 4) == 30

def test_letzter_tag_des_monats():
    assert letzter_tag_des_monats("20000229") is True
    assert letzter_tag_des_monats("20000228") is False
    assert letzter_tag_des_monats("20230228") is True
    assert letzter_tag_des_monats("20230227") is False
    assert letzter_tag_des_monats("20231231") is True
    assert letzter_tag_des_monats("invalid") is False
```

### Test 1.2: Date Addition and Rollover Correctness
* **Purpose**: Verify that adding positive/negative days to a date correctly rolls over month and year boundaries, matching the legacy `AddiereDatum` logic.
* **Setup**: Import `addiere_datum` from `h_alis_date.py`.
* **Action**: Add offsets that cross month and year boundaries, including leap days.
* **Pass/Fail Criterion**: 
  * Adding 1 day to `20231231` must yield `20240101`.
  * Adding 2 days to `20040228` must yield `20040301`.
  * Adding -1 day (subtracting) via negative offset must yield correct previous dates.

```python
def test_addiere_datum_rollover():
    # Year rollover
    assert addiere_datum("20231231", 1) == "20240101"
    # Leap year February rollover
    assert addiere_datum("20040228", 1) == "20040229"
    assert addiere_datum("20040228", 2) == "20040301"
    # Non-leap year February rollover
    assert addiere_datum("20230228", 1) == "20230301"
    # Negative addition (subtraction)
    assert addiere_datum("20230101", -1) == "20221231"
```

### Test 1.3: Date Chronology and Format Validation
* **Purpose**: Verify that date format checking and chronological comparison behave identically to the legacy Oracle-backed checks.
* **Setup**: Import `dw_date_datum_check` and `dw_date_datum_le` from `h_alis_date.py`.
* **Action**: Validate correct/incorrect formats and compare chronological order.
* **Pass/Fail Criterion**:
  * `dw_date_datum_check` must return `True` only if the string matches the format.
  * `dw_date_datum_le` must return `True` if `date1 <= date2`, and raise `ValueError` if `date1 > date2`.

```python
def test_dw_date_datum_check():
    assert dw_date_datum_check("20231025", "YYYYMMDD") is True
    assert dw_date_datum_check("25.10.2023", "DD.MM.YYYY") is True
    assert dw_date_datum_check("2023-10-25", "YYYYMMDD") is False
    assert dw_date_datum_check("invalid-date", "YYYYMMDD") is False

def test_dw_date_datum_le():
    assert dw_date_datum_le("20231024", "20231025") is True
    assert dw_date_datum_le("20231025", "20231025") is True
    
    with pytest.raises(ValueError, match="ist groesser als"):
        dw_date_datum_le("20231026", "20231025")
```

### Test 1.4: Timeframe Generation (Offsets and Steps)
* **Purpose**: Verify that `dw_date_gib_zeitraum` correctly calculates start and end dates based on step units ('D', 'M', 'Y') and offsets.
* **Setup**: Mock the current system date to a fixed value (e.g., `2023-10-15`) to ensure deterministic outputs.
* **Action**: Call `dw_date_gib_zeitraum` with various offsets and steps.
* **Pass/Fail Criterion**:
  * Step 'D' with offset `5` must return `20231015` and `20231020`.
  * Step 'M' with offset `-1` must return the first and last day of the previous month (`20230901` and `20230930`).
  * Step 'Y' with offset `0` must return the first and last day of the current year (`20230101` and `20231231`).

```python
from unittest.mock import patch

@patch('h_alis_date.datetime')
def test_dw_date_gib_zeitraum(mock_datetime):
    # Fix system time to 2023-10-15
    mock_datetime.now.return_value = datetime(2023, 10, 15)
    mock_datetime.strptime = datetime.strptime
    
    # Day step
    start, end = dw_date_gib_zeitraum(5, 'D', 'YYYYMMDD', 'START', 'ENDE')
    assert start == "20231015"
    assert end == "20231020"

    # Month step (previous month)
    start, end = dw_date_gib_zeitraum(-1, 'M', 'YYYYMMDD', 'START', 'ENDE')
    assert start == "20230901"
    assert end == "20230930"

    # Year step (current year)
    start, end = dw_date_gib_zeitraum(0, 'Y', 'YYYYMMDD', 'START', 'ENDE')
    assert start == "20230101"
    assert end == "20231231"
```

---

## Group 2: Parameter Parsing and Validation (`h_alis_parameter.py`)

This group validates that parameter parsing, system/metric conversions, and compatibility matrices match the legacy business rules exactly.

### Test 2.1: Parameter Presence and Error State Management
* **Purpose**: Verify that `pruefeParameterGesetzt` correctly flags missing environment variables and updates the global `ErrNr` and `ErrArg` state.
* **Setup**: Import `h_alis_parameter` and reset the error state.
* **Action**: Call `pruefeParameterGesetzt` with existing and missing environment variables.
* **Pass/Fail Criterion**:
  * If the environment variable is set, `ErrNr` must remain `0`.
  * If the environment variable is missing, `ErrNr` must be set to `194` and `ErrArg` must contain the parameter name.

```python
import os
import h_alis_parameter

def test_pruefe_parameter_gesetzt():
    h_alis_parameter.reset_error()
    
    # Setup environment variable
    os.environ["TEST_VAR"] = "present_value"
    h_alis_parameter.pruefeParameterGesetzt("Test Parameter", "TEST_VAR")
    assert h_alis_parameter.ErrNr == 0
    
    # Test missing variable
    if "MISSING_VAR" in os.environ:
        del os.environ["MISSING_VAR"]
    h_alis_parameter.pruefeParameterGesetzt("Missing Parameter", "MISSING_VAR")
    assert h_alis_parameter.ErrNr == 194
    assert h_alis_parameter.ErrArg == "Missing Parameter"
```

### Test 2.2: Metric (Kennzahl) and System Abbreviation Mapping
* **Purpose**: Verify that long-form metric and system names are correctly mapped to their standardized short-form abbreviations.
* **Setup**: Set environment variables with mixed-case long-form names.
* **Action**: Invoke `konvertiereKennzahl`, `konvertiereSystem`, and `konvertiereSDName`.
* **Pass/Fail Criterion**:
  * `"zugang"` must map to `"zug"`.
  * `"carmen"` must remain `"carmen"`.
  * Unrecognized metrics must set `ErrNr` to `198` and map to `"???"`.

```python
def test_konvertiere_kennzahl():
    h_alis_parameter.reset_error()
    os.environ["MAPPED_KENNZAHL"] = "ZUGANG"
    h_alis_parameter.konvertiereKennzahl("MAPPED_KENNZAHL")
    assert os.environ["MAPPED_KENNZAHL"] == "zug"
    assert h_alis_parameter.ErrNr == 0

    # Unrecognized metric
    os.environ["BAD_KENNZAHL"] = "UNKNOWN_METRIC"
    h_alis_parameter.konvertiereKennzahl("BAD_KENNZAHL")
    assert os.environ["BAD_KENNZAHL"] == "???"
    assert h_alis_parameter.ErrNr == 198
    assert h_alis_parameter.ErrArg == "UNKNOWN_METRIC"

def test_konvertiere_system():
    h_alis_parameter.reset_error()
    os.environ["MAPPED_SYSTEM"] = "CARMEN"
    h_alis_parameter.konvertiereSystem("MAPPED_SYSTEM")
    assert os.environ["MAPPED_SYSTEM"] == "carmen"
    assert h_alis_parameter.ErrNr == 0
```

### Test 2.3: System-Metric Compatibility Matrix
* **Purpose**: Verify that only valid combinations of source systems and metrics are permitted, matching the legacy `pruefeSystemKennzahl` rules.
* **Setup**: Reset error state.
* **Action**: Call `pruefeSystemKennzahl` with valid and invalid combinations.
* **Pass/Fail Criterion**:
  * `("carmen", "zug")` must be valid (`ErrNr == 0`).
  * `("carmen", "twe")` must be invalid (`ErrNr == 195`).
  * `("xtra", "rst")` must be valid (`ErrNr == 0`).
  * `("xtra", "zug")` must be invalid (`ErrNr == 195`).

```python
@pytest.mark.parametrize("system, kennzahl, expected_valid", [
    ("carmen", "zug", True),
    ("carmen", "twe", False),  # Invalid combination
    ("xtra", "rst", True),
    ("xtra", "zug", False),    # Invalid combination
    ("sap", "srs", True),
    ("sap", "zug", False),     # Invalid combination
    ("nnv", "tvd", True),
    ("nnv", "zug", False)      # Invalid combination
])
def test_pruefe_system_kennzahl(system, kennzahl, expected_valid):
    h_alis_parameter.reset_error()
    h_alis_parameter.pruefeSystemKennzahl(system, kennzahl)
    if expected_valid:
        assert h_alis_parameter.ErrNr == 0
    else:
        assert h_alis_parameter.ErrNr == 195
        assert "Ungueltige Kombination" in h_alis_parameter.ErrArg
```

### Test 2.4: Domain (Bereich) and Interval (Intervall) Resolution
* **Purpose**: Verify that metrics are correctly classified into their respective business domains (`tn`, `us`, `gd`, `sd`, `md`) and reporting intervals (`t`, `m`).
* **Setup**: Reset error state.
* **Action**: Call `gibBereich` and `gibIntervall` for various metrics.
* **Pass/Fail Criterion**:
  * Metric `"zug"` must resolve to domain `"tn"` and interval `"t"`.
  * Metric `"bst"` must resolve to domain `"tn"` and interval `"m"`.
  * Metric `"tvd"` must resolve to domain `"gd"` and interval `"m"`.

```python
def test_gib_bereich_und_intervall():
    h_alis_parameter.reset_error()
    
    # Test 'zug'
    h_alis_parameter.gibBereich("zug", "OUT_BEREICH")
    h_alis_parameter.gibIntervall("zug", "OUT_INTERVAL")
    assert os.environ["OUT_BEREICH"] == "tn"
    assert os.environ["OUT_INTERVAL"] == "t"

    # Test 'bst'
    h_alis_parameter.gibBereich("bst", "OUT_BEREICH")
    h_alis_parameter.gibIntervall("bst", "OUT_INTERVAL")
    assert os.environ["OUT_BEREICH"] == "tn"
    assert os.environ["OUT_INTERVAL"] == "m"

    # Test invalid metric
    h_alis_parameter.reset_error()
    h_alis_parameter.gibBereich("invalid_metric", "OUT_BEREICH")
    assert h_alis_parameter.ErrNr == 196
```

### Test 2.5: Time Parameter and Offset Validation
* **Purpose**: Verify that mutual exclusivity of explicit start/end dates versus relative offsets is enforced, and that offsets are correctly calculated.
* **Setup**: Reset error state.
* **Action**: Call `pruefeZeitParameter` with various parameter combinations.
* **Pass/Fail Criterion**:
  * Providing both dates and an offset must fail (`ErrNr == 195`).
  * Providing only an offset (positive integer) must pass (`ErrNr == 0`).
  * Providing invalid chronological dates must fail (`ErrNr == 195`).

```python
def test_pruefe_zeit_parameter():
    # Case 1: Only offset provided (Valid)
    h_alis_parameter.reset_error()
    h_alis_parameter.pruefeZeitParameter("", "", "10")
    assert h_alis_parameter.ErrNr == 0

    # Case 2: Both offset and dates provided (Invalid)
    h_alis_parameter.reset_error()
    h_alis_parameter.pruefeZeitParameter("20231001", "20231010", "10")
    assert h_alis_parameter.ErrNr == 195
    assert "Es darf nur eine Zeitspanne oder beide" in h_alis_parameter.ErrArg

    # Case 3: Only dates provided in correct order (Valid)
    h_alis_parameter.reset_error()
    h_alis_parameter.pruefeZeitParameter("20231001", "20231010", "")
    assert h_alis_parameter.ErrNr == 0

    # Case 4: Only dates provided in incorrect order (Invalid)
    h_alis_parameter.reset_error()
    h_alis_parameter.pruefeZeitParameter("20231010", "20231001", "")
    assert h_alis_parameter.ErrNr == 195
```

---

## Group 3: SQL*Plus Execution Wrapper (`h_alis_sqlplus.py`)

This group validates that the SQL*Plus execution wrapper correctly handles file system checks, parameter validation, and process execution.

### Test 3.1: Script Readability and Parameter Guards
* **Purpose**: Verify that `starte_sql_skript` fails immediately if required parameters are missing or if the target SQL script is unreadable.
* **Setup**: Set `DW_ORAUSER` in the environment. Create a dummy unreadable file path.
* **Action**: Call `starte_sql_skript` with missing parameters and unreadable file paths.
* **Pass/Fail Criterion**:
  * Missing `p_Eintragsnr` or `p_Skript` must return exit code `196`.
  * An unreadable or non-existent script path must return exit code `201`.

```python
import pytest
import os
from h_alis_sqlplus import starte_sql_skript

def test_starte_sql_skript_guards(tmp_path):
    os.environ["DW_ORAUSER"] = "test_user/test_pass@test_db"
    
    # Missing parameters
    rc = starte_sql_skript("", "")
    assert rc == 196

    # Non-existent script
    rc = starte_sql_skript("12345", "/nonexistent/path/script.sql")
    assert rc == 201
```

### Test 3.2: SQL*Plus Process Execution and Exit Code Propagation
* **Purpose**: Verify that `starte_sql_skript` correctly invokes `sqlplus` via subprocess and propagates its exit code back to the caller.
* **Setup**: Mock `subprocess.run` to simulate successful and failed `sqlplus` executions.
* **Action**: Call `starte_sql_skript` with a valid dummy script path.
* **Pass/Fail Criterion**:
  * The wrapper must return `0` when `sqlplus` succeeds.
  * The wrapper must return the exact non-zero exit code (e.g., `2`) when `sqlplus` fails.

```python
from unittest.mock import patch, MagicMock

@patch('subprocess.run')
def test_starte_sql_skript_execution(mock_run, tmp_path):
    os.environ["DW_ORAUSER"] = "test_user/test_pass@test_db"
    
    # Create a dummy readable script file
    dummy_script = tmp_path / "dummy.sql"
    dummy_script.write_text("SELECT 1 FROM DUAL;")
    
    # Mock successful execution
    mock_response = MagicMock()
    mock_response.returncode = 0
    mock_run.return_value = mock_response
    
    rc = starte_sql_skript("12345", str(dummy_script), "param1", "param2")
    assert rc == 0
    mock_run.assert_called_once()
    
    # Verify command line arguments passed to sqlplus
    called_args = mock_run.call_args[0][0]
    assert called_args[0] == "sqlplus"
    assert called_args[1] == "test_user/test_pass@test_db"
    assert called_args[2] == f"@{str(dummy_script)}"
    assert called_args[3] == "param1"
    assert called_args[4] == "param2"

    # Mock failed execution (exit code 2)
    mock_response.returncode = 2
    mock_run.return_value = mock_response
    
    rc = starte_sql_skript("12345", str(dummy_script))
    assert rc == 2
```

---

## Group 4: Message and Error Logging (`f_alis_msgerr.py`)

This group validates that the unified logging and error management functions behave identically to the legacy database-backed tracking system.

### Test 4.1: Log Filename Generation Parity
* **Purpose**: Verify that `dwmsg_logdateiname` constructs log file paths matching the legacy pattern: `${DW_DIR_PROT}/${JobKennung}_YYYYMMDD_HHMM_${DWMSG_EintragsNr}.log`.
* **Setup**: Set `DW_DIR_PROT` in the environment.
* **Action**: Call `dwmsg_logdateiname` and verify the output format using a regular expression.
* **Pass/Fail Criterion**: The generated filename must match the exact timestamped pattern and resolve to the correct directory.

```python
import os
import re
from f_alis_msgerr import dwmsg_logdateiname

def test_dwmsg_logdateiname():
    os.environ["DW_DIR_PROT"] = "/var/log/dw_prot"
    
    log_path = dwmsg_logdateiname("VAR_OUT", "JOB_BERT_LOAD", "99999")
    
    # Pattern: /var/log/dw_prot/JOB_BERT_LOAD_YYYYMMDD_HHMM_99999.log
    pattern = r"^/var/log/dw_prot/JOB_BERT_LOAD_\d{8}_\d{4}_99999\.log$"
    assert re.match(pattern, log_path) is not None
```

### Test 4.2: Database Logging Procedures
* **Purpose**: Verify that database-backed status updates (`SetzeStatusOk`, `SetzeStatusAbbruch`, `SetzeZusatzInfos`) execute the correct PL/SQL blocks via the database driver.
* **Setup**: Mock the `oracledb` connection and cursor to capture executed SQL and parameters.
* **Action**: Call `dwmsg_setze_stichtag_info` and `dwmsg_append_timing_infos`.
* **Pass/Fail Criterion**:
  * The executed SQL must call `BERT_MELDUNG.SetzeZusatzInfos`.
  * Bind variables must be correctly mapped to prevent SQL injection.

```python
from unittest.mock import patch, MagicMock

@patch('oracledb.connect')
def test_dwmsg_database_procedures(mock_connect):
    os.environ["DW_ORAUSER"] = "user/pass@dsn"
    
    # Mock DB connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Execute Stichtag Info Update
    dwmsg_setze_stichtag_info("12345", "2023-10-15", "YYYY-MM-DD")
    
    # Verify SQL execution and bind parameters
    mock_cursor.execute.assert_called_once()
    executed_sql = mock_cursor.execute.call_args[0][0]
    bind_params = mock_cursor.execute.call_args[0][1]
    
    assert "BERT_MELDUNG.SetzeZusatzInfos" in executed_sql
    assert bind_params["eintrags_nr"] == 12345
    assert bind_params["stichtag"] == "2023-10-15"
    assert bind_params["stichtag_fmt"] == "YYYY-MM-DD"
```

---

## Group 5: End-to-End Integration and Parity Run

This test executes a simulated ETL job lifecycle using the migrated Python modules to prove complete behavioral equivalence under operational conditions.

### Test 5.1: Simulated Job Lifecycle
* **Purpose**: Verify that a calling script can initialize a tracking ID, log progress, append timing info, handle errors, and finalize status successfully.
* **Setup**: 
  * Mock all external database calls (`sqlplus` and `oracledb`).
  * Set required environment variables: `DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`.
* **Action**: Run the following sequence:
  1. Generate a log filename.
  2. Simulate a successful run and call `dwmsg_setze_status_ok`.
  3. Simulate a failure, trigger `dwmsg_fehlerbehandlung`, and verify that the status is set to aborted.
* **Pass/Fail Criterion**: The entire sequence must execute without raising unhandled exceptions, and all mocked database calls must receive the correct state transitions.

```python
@patch('subprocess.run')
@patch('oracledb.connect')
def test_simulated_job_lifecycle(mock_connect, mock_subproc, tmp_path):
    # Setup environment
    os.environ["DW_ORAUSER"] = "user/pass@dsn"
    os.environ["DW_DIR_ROOT"] = str(tmp_path)
    os.environ["DW_DIR_PROT"] = str(tmp_path / "prot")
    os.makedirs(os.environ["DW_DIR_PROT"], exist_ok=True)
    
    # Mock subprocess for sqlplus calls
    mock_response = MagicMock()
    mock_response.returncode = 0
    mock_subproc.return_value = mock_response

    # 1. Generate Log Filename
    log_file = dwmsg_logdateiname("LOG_VAR", "JOB_TEST", "55555")
    assert os.path.dirname(log_file) == os.environ["DW_DIR_PROT"]
    
    # 2. Set Status OK
    dwmsg_setze_status_ok("55555")
    # Verify sqlplus was called with SetzeStatusOk
    assert mock_subproc.call_count == 1
    args = mock_subproc.call_args[0][0]
    assert "BERT_MELDUNG.SetzeStatusOk" in args
    
    # 3. Trigger Error Handler
    mock_subproc.reset_mock()
    dwmsg_fehlerbehandlung("55555", fehler_nr=99)
    # Error handler should:
    # a) Call MeldeFehler (sqlplus BERT_MELDUNG.Fehler)
    # b) Call SetzeStatusAbbruch (sqlplus BERT_MELDUNG.SetzeStatusAbbruch)
    assert mock_subproc.call_count == 2
```