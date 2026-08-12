# Migration Validation Test Suite: Shared Utilities Library
**Target Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`  
**Components Under Test:**
1. `f_alis_msgerr.py` (Migrated from `f_alis_msgerr.ksh`)
2. `h_alis_date.py` (Migrated from `h_alis_date.ksh`)
3. `h_alis_parameter.py` (Migrated from `h_alis_parameter.ksh`)
4. `h_alis_sqlplus.py` (Migrated from `h_alis_sqlplus.ksh`)

---

## Section 1: Output Parity & Date Math Validation (`h_alis_date.py`)

This section validates that the migrated Python date utility library matches the legacy KornShell/Oracle date calculations exactly, covering leap years, date formatting, and relative reporting windows without requiring active database connections.

### Test Case 1.1: Previous Month Calculation (`dw_date_vormonat`)
* **Purpose:** Verify that `dw_date_vormonat` correctly calculates the previous month relative to the current system date and formats it according to Oracle-style format strings.
* **Setup:**
  * Freeze the system time to a specific date (e.g., March 15, 2024, and January 1, 2024) using `freezegun` or standard mocking.
* **Action:**
  * Execute `dw_date_vormonat("YYYYMM")` and `dw_date_vormonat("YYYY-MM-DD")`.
* **Pass/Fail Criterion:**
  * **Pass:** When system time is frozen at `2024-03-15`, the function returns `"202402"` for `"YYYYMM"` and `"2024-02-29"` (leap year leap day) for `"YYYY-MM-DD"`. When frozen at `2024-01-10`, it returns `"202312"`.
  * **Fail:** Any deviation in month subtraction, year-boundary crossing, or formatting.

```python
import pytest
from unittest.mock import patch
import datetime
from h_alis_date import dw_date_vormonat

@pytest.mark.parametrize("frozen_date, format_str, expected", [
    (datetime.date(2024, 3, 15), "YYYYMM", "202402"),
    (datetime.date(2024, 3, 15), "YYYY-MM-DD", "2024-02-29"), # Leap year
    (datetime.date(2023, 3, 15), "YYYY-MM-DD", "2023-02-28"), # Non-leap year
    (datetime.date(2024, 1, 15), "YYYYMM", "202312"),         # Year boundary
])
def test_dw_date_vormonat_parity(frozen_date, format_str, expected):
    with patch('datetime.date') as mock_date:
        mock_date.today.return_return_value = frozen_date
        mock_date.side_effect = lambda *args, **kw: datetime.date(*args, **kw)
        
        # We also need to patch datetime.datetime if used internally
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.today.return_value = datetime.datetime(frozen_date.year, frozen_date.month, frozen_date.day)
            mock_datetime.strptime = datetime.datetime.strptime
            
            result = dw_date_vormonat(format_str)
            assert result == expected
```

---

### Test Case 1.2: Date Format Validation (`dw_date_datum_check`)
* **Purpose:** Verify that `dw_date_datum_check` correctly validates date strings against Oracle-style format patterns, returning `True` for valid dates and `False` for invalid dates or format mismatches.
* **Setup:** None.
* **Action:**
  * Call `dw_date_datum_check` with valid dates, invalid dates (e.g., February 30th), and mismatched formats.
* **Pass/Fail Criterion:**
  * **Pass:** Returns `True` only when the date string strictly conforms to the format and represents a real calendar date.
  * **Fail:** Returns `True` for invalid dates (e.g., `"20230229"`) or raises unhandled exceptions.

```python
from h_alis_date import dw_date_datum_check

@pytest.mark.parametrize("date_str, format_str, expected", [
    ("20231025", "YYYYMMDD", True),
    ("25.10.2023", "DD.MM.YYYY", True),
    ("2023-10-25", "YYYY-MM-DD", True),
    ("20230229", "YYYYMMDD", False), # Non-leap year invalid day
    ("20240229", "YYYYMMDD", True),  # Leap year valid day
    ("20231301", "YYYYMMDD", False), # Invalid month
    ("invalid", "YYYYMMDD", False),
])
def test_dw_date_datum_check(date_str, format_str, expected):
    assert dw_date_datum_check(date_str, format_str) == expected
```

---

### Test Case 1.3: Date Chronology Assertion (`dw_date_datum_le`)
* **Purpose:** Verify that `dw_date_datum_le` asserts that `datum1 <= datum2` and raises a `ValueError` with the exact legacy German error message if `datum1 > datum2`.
* **Setup:** None.
* **Action:**
  * Call `dw_date_datum_le` with `datum1 <= datum2` and `datum1 > datum2`.
* **Pass/Fail Criterion:**
  * **Pass:** Returns `True` when `datum1 <= datum2`. Raises `ValueError` containing `"Datum <datum1> ist groesser als <datum2>"` when `datum1 > datum2`.
  * **Fail:** Does not raise an exception on chronological violation, or raises an exception with incorrect error text.

```python
import pytest
from h_alis_date import dw_date_datum_le

def test_dw_date_datum_le_valid():
    assert dw_date_datum_le("20231010", "20231015") is True
    assert dw_date_datum_le("20231010", "20231010") is True

def test_dw_date_datum_le_invalid():
    d1, d2 = "20231015", "20231010"
    with pytest.raises(ValueError) as exc_info:
        dw_date_datum_le(d1, d2)
    assert f"Datum {d1} ist groesser als {d2}" in str(exc_info.value)
```

---

### Test Case 1.4: Relative Time Window Generation (`dw_date_gib_zeitraum`)
* **Purpose:** Verify that `dw_date_gib_zeitraum` correctly calculates start and end dates based on step units (`'Y'`, `'M'`, `'D'`) and offsets, matching the legacy database-driven logic.
* **Setup:** Freeze system date to `2023-10-15`.
* **Action:**
  * Call `dw_date_gib_zeitraum` with various offsets and step units.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * For `'D'` with offset `-5`: returns `("20231015", "20231010")`.
    * For `'M'` with offset `-1`: returns `("20231001", "20230930")` (start is 1st of current month, end is last day of target month).
    * For `'Y'` with offset `-1`: returns `("20230101", "20221231")` (start is Jan 1st of current year, end is Dec 31st of target year).
  * **Fail:** Incorrect boundary calculations, especially around month-end and year-end transitions.

```python
from h_alis_date import dw_date_gib_zeitraum

def test_dw_date_gib_zeitraum_days():
    with patch('datetime.date') as mock_date:
        mock_date.today.return_value = datetime.date(2023, 10, 15)
        
        # Days offset
        start, end = dw_date_gib_zeitraum(-5, 'D', "YYYYMMDD")
        assert start == "20231015"
        assert end == "20231010"

def test_dw_date_gib_zeitraum_months():
    with patch('datetime.date') as mock_date:
        mock_date.today.return_value = datetime.date(2023, 10, 15)
        
        # Month offset (Start of current month, End of target month)
        start, end = dw_date_gib_zeitraum(-1, 'M', "YYYYMMDD")
        assert start == "20231001"
        assert end == "20230930"

def test_dw_date_gib_zeitraum_years():
    with patch('datetime.date') as mock_date:
        mock_date.today.return_value = datetime.date(2023, 10, 15)
        
        # Year offset (Jan 1 of current year, Dec 31 of target year)
        start, end = dw_date_gib_zeitraum(-1, 'Y', "YYYYMMDD")
        assert start == "20230101"
        assert end == "20221231"
```

---

### Test Case 1.5: Month Boundary & Leap Year Logic
* **Purpose:** Verify calendar math functions (`letzter_tag_des_monats`, `tage_im_monat`, `addiere_datum`) handle leap years (including century leap years like 2000 vs 1900) and date additions correctly.
* **Setup:** None.
* **Action:**
  * Execute functions with leap and non-leap year parameters.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * `letzter_tag_des_monats("20000229")` is `True`, `"19000229"` is `False` (1900 was not a leap year).
    * `tage_im_monat(2024, 2)` returns `29`, `tage_im_monat(2023, 2)` returns `28`.
    * `addiere_datum("20231028", 5)` returns `"20231102"`.
  * **Fail:** Incorrect day counts or leap year evaluations.

```python
from h_alis_date import letzter_tag_des_monats, tage_im_monat, addiere_datum

def test_leap_year_evaluations():
    assert letzter_tag_des_monats("20000229") is True
    assert letzter_tag_des_monats("20040229") is True
    assert letzter_tag_des_monats("19000228") is True # 1900 Feb 28 was last day
    assert tage_im_monat(2000, 2) == 29
    assert tage_im_monat(1900, 2) == 28

def test_addiere_datum_overflow():
    assert addiere_datum("20231230", 3) == "20240102"
    assert addiere_datum("20240228", 2) == "20240301" # Leap year transition
```

---

## Section 2: Parameter Normalization & Validation State (`h_alis_parameter.py`)

This section validates parameter parsing, normalization mappings, system-key figure compatibility rules, and the propagation of the global error state (`ErrNr` and `ErrArg`).

### Test Case 2.1: Key Figure & System Normalization
* **Purpose:** Verify that verbose German parameter values are correctly mapped to their standardized short-form codes in the environment variables, and that invalid values set the appropriate error state.
* **Setup:** Clear and reset global `ErrNr` and `ErrArg` in `h_alis_parameter`.
* **Action:**
  * Set environment variables and call `konvertiereKennzahl` and `konvertiereSystem`.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * `"bestand"` maps to `"bst"`, `"abgang_zukunft"` maps to `"abz"`.
    * `"sap"` remains `"sap"`, `"invalid_sys"` sets `ErrNr = 195` and environment variable to `"???"`.
  * **Fail:** Incorrect mappings, or failure to set the global error state on invalid input.

```python
import os
import h_alis_parameter

def setup_function(function):
    # Reset global error state before each test
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""
    if "TEST_VAR" in os.environ:
        del os.environ["TEST_VAR"]

def test_konvertiere_kennzahl_success():
    os.environ["TEST_VAR"] = "bestand"
    h_alis_parameter.konvertiereKennzahl("TEST_VAR")
    assert os.environ["TEST_VAR"] == "bst"
    assert h_alis_parameter.ErrNr == 0

def test_konvertiere_kennzahl_invalid():
    os.environ["TEST_VAR"] = "unknown_metric"
    h_alis_parameter.konvertiereKennzahl("TEST_VAR")
    assert os.environ["TEST_VAR"] == "???"
    assert h_alis_parameter.ErrNr == 198
    assert h_alis_parameter.ErrArg == "unknown_metric"

def test_konvertiere_system_success():
    os.environ["TEST_VAR"] = "CARMEN"
    h_alis_parameter.konvertiereSystem("TEST_VAR")
    assert os.environ["TEST_VAR"] == "carmen"
    assert h_alis_parameter.ErrNr == 0
```

---

### Test Case 2.2: System-Key Figure Compatibility Matrix (`pruefeSystemKennzahl`)
* **Purpose:** Verify that the compatibility matrix rules between source systems and key figures are strictly enforced.
* **Setup:** Reset global error state.
* **Action:**
  * Call `pruefeSystemKennzahl` with valid and invalid combinations.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * Valid combinations (e.g., `system="sap"`, `kennzahl="srs"`) leave `ErrNr = 0`.
    * Invalid combinations (e.g., `system="sap"`, `kennzahl="zug"`) set `ErrNr = 195` and `ErrArg = "Ungueltige Kombination sap zug"`.
  * **Fail:** Allowed combinations are blocked, or forbidden combinations are permitted without setting the error state.

```python
import h_alis_parameter

@pytest.mark.parametrize("system, kennzahl, should_pass", [
    ("sap", "srs", True),
    ("sap", "zug", False), # Forbidden: SAP cannot deliver 'zug'
    ("carmen", "zug", True),
    ("carmen", "twe", False), # Forbidden
    ("xtra", "rst", True),
    ("xtra", "zug", False), # Forbidden
])
def test_pruefe_system_kennzahl_matrix(system, kennzahl, should_pass):
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""
    
    h_alis_parameter.pruefeSystemKennzahl(system, kennzahl)
    if should_pass:
        assert h_alis_parameter.ErrNr == 0
    else:
        assert h_alis_parameter.ErrNr == 195
        assert f"Ungueltige Kombination {system} {kennzahl}" in h_alis_parameter.ErrArg
```

---

### Test Case 2.3: Domain Area & Interval Mapping (`gibBereich`, `gibIntervall`)
* **Purpose:** Verify that key figures are correctly mapped to their business domain areas (Bereich: `tn`, `us`, `gd`, `sd`, `md`) and reporting intervals (Intervall: `t`, `m`).
* **Setup:** Reset global error state.
* **Action:**
  * Call `gibBereich` and `gibIntervall` for various key figures.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * `gibBereich("bst", "OUT_VAR")` sets `os.environ["OUT_VAR"] = "tn"`.
    * `gibIntervall("bst", "OUT_VAR")` sets `os.environ["OUT_VAR"] = "m"`.
    * `gibIntervall("zug", "OUT_VAR")` sets `os.environ["OUT_VAR"] = "t"`.
  * **Fail:** Incorrect mappings or failure to write to the specified environment variable.

```python
import os
import h_alis_parameter

def test_gib_bereich_and_intervall():
    h_alis_parameter.ErrNr = 0
    
    # Test Bereich mapping
    h_alis_parameter.gibBereich("bst", "TEST_BEREICH")
    assert os.environ["TEST_BEREICH"] == "tn"
    
    # Test Intervall mapping (bst is monthly)
    h_alis_parameter.gibIntervall("bst", "TEST_INTERVAL")
    assert os.environ["TEST_INTERVAL"] == "m"
    
    # Test Intervall mapping (zug is daily)
    h_alis_parameter.gibIntervall("zug", "TEST_INTERVAL")
    assert os.environ["TEST_INTERVAL"] == "t"
```

---

### Test Case 2.4: Mutual Exclusivity & Time Span Conversion
* **Purpose:** Verify that `pruefeZeitParameter` enforces mutual exclusivity (either offset or start/end date, but not both) and that `konvertiereZeitspanne` correctly calculates historical dates relative to `RUN_DATE`.
* **Setup:** Set `os.environ["RUN_DATE"] = "20231015"`. Reset global error state.
* **Action:**
  * Call `pruefeZeitParameter` with conflicting arguments.
  * Call `konvertiereZeitspanne` with offset `5` and key figure `"zug"` (daily) or `"bst"` (monthly).
* **Pass/Fail Criterion:**
  * **Pass:** 
    * Conflicting parameters set `ErrNr = 195`.
    * `konvertiereZeitspanne` sets start date to `"20231010"` and end date to `"20231015"` for daily metric with offset `5`.
  * **Fail:** Allows conflicting parameters, or calculates incorrect date boundaries.

```python
import os
import h_alis_parameter

def test_pruefe_zeit_parameter_conflict():
    h_alis_parameter.ErrNr = 0
    # Conflict: both dates and offset provided
    h_alis_parameter.pruefeZeitParameter("20231001", "20231010", "5")
    assert h_alis_parameter.ErrNr == 195
    assert "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden" in h_alis_parameter.ErrArg

def test_konvertiere_zeitspanne_daily():
    h_alis_parameter.ErrNr = 0
    os.environ["RUN_DATE"] = "20231015"
    
    h_alis_parameter.konvertiereZeitspanne("START_VAR", "ENDE_VAR", "5", "zug")
    assert os.environ["START_VAR"] == "20231010"
    assert os.environ["ENDE_VAR"] == "20231015"
```

---

## Section 3: Database Logging & PL/SQL Execution (`f_alis_msgerr.py`)

This section validates database interactions, ensuring that the Python module correctly formats and executes PL/SQL calls to the `BERT_MELDUNG` package using the `oracledb` library.

### Test Case 3.1: Sequence ID Retrieval (`dwmsg_ermittle_nr`)
* **Purpose:** Verify that `dwmsg_ermittle_nr` correctly calls the database sequence generator function `BERT_MELDUNG.ErmittleNr` and returns the clean string representation of the retrieved ID.
* **Setup:** Mock the `oracledb` connection and cursor objects. Configure the cursor to return a mock sequence number (e.g., `987654`).
* **Action:**
  * Call `dwmsg_ermittle_nr()`.
* **Pass/Fail Criterion:**
  * **Pass:** The function returns `"987654"` and executes the exact PL/SQL block: `BEGIN :1 := BERT_MELDUNG.ErmittleNr; END;` with an out-parameter.
  * **Fail:** Database errors are unhandled, or the PL/SQL block is incorrectly formatted.

```python
import pytest
from unittest.mock import MagicMock, patch
import oracledb
import f_alis_msgerr

@patch('f_alis_msgerr.get_db_connection')
def test_dwmsg_ermittle_nr(mock_get_conn):
    # Setup mocks
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    # Mock the out parameter variable
    mock_var = MagicMock()
    mock_var.getvalue.return_value = 987654.0
    mock_cur.var.return_value = mock_var
    
    # Action
    result = f_alis_msgerr.dwmsg_ermittle_nr()
    
    # Assertions
    assert result == "987654"
    mock_cur.execute.assert_called_once()
    executed_sql = mock_cur.execute.call_args[0][0]
    assert "BERT_MELDUNG.ErmittleNr" in executed_sql
```

---

### Test Case 3.2: Status Updates & Error Logging (`dwmsg_setze_status_ok`, `dwmsg_melde_fehler`)
* **Purpose:** Verify that status updates and error logging calls correctly execute the corresponding PL/SQL procedures with correct bind parameters.
* **Setup:** Mock `oracledb` connection and cursor.
* **Action:**
  * Call `dwmsg_setze_status_ok("12345")` and `dwmsg_melde_fehler("12345", "F", 10, "Zusatz 1", "Zusatz 2")`.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * `SetzeStatusOk` executes `BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;` with bind `["12345"]`.
    * `Fehler` executes `BEGIN BERT_MELDUNG.Fehler(:1, :2, :3, :4, :5); END;` with binds `["F", "12345", 10, "Zusatz 1", "Zusatz 2"]`.
  * **Fail:** Incorrect bind mappings, missing commits, or unhandled database exceptions.

```python
@patch('f_alis_msgerr.get_db_connection')
def test_dwmsg_setze_status_ok(mock_get_conn):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    f_alis_msgerr.dwmsg_setze_status_ok("12345")
    
    mock_cur.execute.assert_called_once_with(
        "BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", ["12345"]
    )
    mock_conn.commit.assert_called_once()

@patch('f_alis_msgerr.get_db_connection')
def test_dwmsg_melde_fehler(mock_get_conn):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    f_alis_msgerr.dwmsg_melde_fehler("12345", "F", 10, "ErrText", "Details")
    
    mock_cur.execute.assert_called_once_with(
        """
                    BEGIN
                        BERT_MELDUNG.Fehler(:1, :2, :3, :4, :5);
                    END;
                """, ["F", "12345", 10, "ErrText", "Details"]
    )
    mock_conn.commit.assert_called_once()
```

---

### Test Case 3.3: Key-Date & Timing Metadata Updates
* **Purpose:** Verify that `dwmsg_setze_stichtag_info` and `dwmsg_append_timing_infos` correctly execute PL/SQL blocks using Oracle date conversion functions (`to_date`, `to_char`, `SYSDATE`).
* **Setup:** Mock `oracledb` connection and cursor.
* **Action:**
  * Call `dwmsg_setze_stichtag_info("12345", "20231015", "YYYYMMDD")`.
  * Call `dwmsg_append_timing_infos("12345", "Step 1 completed", "YYYY-MM-DD HH24:MI:SS")`.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * Stichtag executes `BERT_MELDUNG.SetzeZusatzInfos(:1, to_date(:2, :3))` with binds `["12345", "20231015", "YYYYMMDD"]`.
    * Timing executes `BERT_MELDUNG.SetzeZusatzInfos(:1, null, :2||' '||to_char(SYSDATE,:3)||' ')` with binds `["12345", "Step 1 completed", "YYYY-MM-DD HH24:MI:SS"]`.
  * **Fail:** Incorrect SQL syntax, missing binds, or failure to commit.

```python
@patch('f_alis_msgerr.get_db_connection')
def test_dwmsg_setze_stichtag_info(mock_get_conn):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    f_alis_msgerr.dwmsg_setze_stichtag_info("12345", "20231015", "YYYYMMDD")
    
    mock_cur.execute.assert_called_once()
    sql_called = mock_cur.execute.call_args[0][0]
    binds_called = mock_cur.execute.call_args[0][1]
    
    assert "to_date(:2, :3)" in sql_called
    assert binds_called == ["12345", "20231015", "YYYYMMDD"]
    mock_conn.commit.assert_called_once()
```

---

## Section 4: Process Orchestration & Subprocess Execution (`h_alis_sqlplus.py`)

This section validates the execution wrapper for external SQL*Plus scripts, ensuring parameter validation, file system checks, and exit code propagation behave exactly as in the legacy shell wrapper.

### Test Case 4.1: SQL*Plus Execution Wrapper (`starte_sql_skript`)
* **Purpose:** Verify that `starte_sql_skript` validates script existence, logs errors via `DWMSG_MeldeFehler` if validation fails, and executes the `sqlplus` command-line utility with correct arguments and environment variables.
* **Setup:** 
  * Set `os.environ["DW_ORAUSER"] = "user/pass@dsn"`.
  * Mock `os.path.exists` and `os.access` to simulate file existence and readability.
  * Mock `subprocess.run` to intercept the command execution.
* **Action:**
  * Call `starte_sql_skript("12345", "/path/to/script.sql", "arg1", "arg2")`.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * If the script file does not exist, returns `201` and calls `DWMSG_MeldeFehler` with code `201`.
    * If the script exists, executes `subprocess.run` with command `['sqlplus', 'user/pass@dsn', '@/path/to/script.sql', 'arg1', 'arg2']` and returns the subprocess exit code.
  * **Fail:** Executes `sqlplus` when the script is missing, fails to propagate the exit code, or fails to redirect standard input (`stdin=subprocess.DEVNULL`).

```python
import os
import pytest
from unittest.mock import patch, MagicMock
import subprocess
from h_alis_sqlplus import starte_sql_skript

@patch('os.path.exists')
@patch('os.access')
@patch('h_alis_sqlplus.dwmsg_melde_fehler')
def test_starte_sql_skript_missing_file(mock_melde, mock_access, mock_exists):
    # Simulate missing file
    mock_exists.return_value = False
    
    rc = starte_sql_skript("12345", "/missing/script.sql")
    
    assert rc == 201
    mock_melde.assert_called_once_with("12345", "E", 201, "/missing/script.sql")

@patch('os.path.exists')
@patch('os.access')
@patch('subprocess.run')
def test_starte_sql_skript_success(mock_run, mock_access, mock_exists):
    # Simulate existing and readable file
    mock_exists.return_value = True
    mock_access.return_value = True
    os.environ["DW_ORAUSER"] = "test_user/test_pass@test_db"
    
    # Mock subprocess return code
    mock_res = MagicMock()
    mock_res.returncode = 0
    mock_run.return_value = mock_res
    
    rc = starte_sql_skript("12345", "/valid/script.sql", "param1", "param2")
    
    assert rc == 0
    mock_run.assert_called_once_with(
        ["sqlplus", "test_user/test_pass@test_db", "@/valid/script.sql", "param1", "param2"],
        stdin=subprocess.DEVNULL,
        check=False
    )
```