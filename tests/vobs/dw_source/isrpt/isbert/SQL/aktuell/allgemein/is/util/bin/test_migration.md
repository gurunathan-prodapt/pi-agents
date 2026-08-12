# Migration Validation Test Suite: Shared Utility Binaries

This document defines the migration-validation test suite for the migrated Python utility modules located in `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`. These tests ensure behavioral equivalence between the legacy KornShell (`.ksh`) scripts and the modernized Python (`.py`) implementations.

---

## Section 1: `f_alis_msgerr.py` (Session Log & Telemetry)

This module manages process execution states, logging, and telemetry. In the target architecture, Oracle database package calls (`BERT_MELDUNG`) are replaced by Google Cloud BigQuery DML operations targeting the centralized audit table `bert_meldung`.

### Test Case 1.1: Session Initialization and Status Updates
* **Purpose**: Verify that `dwmsg_erzeuge_eintrag` inserts a new tracking row with status `'LAUFEND'` and that `dwmsg_setze_status_ok` and `dwmsg_setze_status_abbruch` update the status and end times correctly in BigQuery.
* **Setup**:
  * Set environment variables: `GCP_PROJECT="test-project"`, `BQ_DATASET="test_dataset"`.
  * Mock the `google.cloud.bigquery.Client` to capture and assert the structure of the executed queries.
* **Action**:
  1. Call `dwmsg_erzeuge_eintrag("10001", "JOB_TEST", "test_script.py", "gs://logs/test.log")`.
  2. Call `dwmsg_setze_status_ok("10001")`.
  3. Call `dwmsg_setze_status_abbruch("10001")`.
* **Pass/Fail Criterion**:
  * **Pass**: The insert query targets the correct table path `` `test-project.test_dataset.bert_meldung` `` and passes all parameters as scalar query parameters. The update queries correctly modify the `status` column to `'OK'` and `'ABBRUCH'` respectively, and set `end_zeit = CURRENT_TIMESTAMP()`.
  * **Fail**: Any query syntax error, incorrect table pathing, or missing query parameters.

```python
import os
import pytest
from unittest.mock import MagicMock, patch

# Set environment variables before importing the module under test
os.environ["GCP_PROJECT"] = "test-project"
os.environ["BQ_DATASET"] = "test_dataset"

import f_alis_msgerr

@patch("f_alis_msgerr._get_bq_client")
def test_session_lifecycle(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    # 1. Test Erzeuge Eintrag
    f_alis_msgerr.dwmsg_erzeuge_eintrag("10001", "JOB_TEST", "test_script.py", "gs://logs/test.log")
    
    assert mock_client.query.call_count == 1
    call_args = mock_client.query.call_args[0]
    query_str = call_args[0]
    job_config = mock_client.query.call_args[1]["job_config"]
    
    assert "INSERT INTO `test-project.test_dataset.bert_meldung`" in query_str
    assert "VALUES (@eintrags_nr, @job_kennung, @programmname, @log_datei, CURRENT_TIMESTAMP(), 'LAUFEND')" in query_str
    
    params = {p.name: p.value for p in job_config.query_parameters}
    assert params["eintrags_nr"] == "10001"
    assert params["job_kennung"] == "JOB_TEST"
    assert params["programmname"] == "test_script.py"
    assert params["log_datei"] == "gs://logs/test.log"

    # 2. Test Setze Status OK
    mock_client.reset_mock()
    f_alis_msgerr.dwmsg_setze_status_ok("10001")
    
    assert mock_client.query.call_count == 1
    query_str = mock_client.query.call_args[0][0]
    assert "UPDATE `test-project.test_dataset.bert_meldung`" in query_str
    assert "SET status = 'OK', end_zeit = CURRENT_TIMESTAMP()" in query_str

    # 3. Test Setze Status Abbruch
    mock_client.reset_mock()
    f_alis_msgerr.dwmsg_setze_status_abbruch("10001")
    
    assert mock_client.query.call_count == 1
    query_str = mock_client.query.call_args[0][0]
    assert "SET status = 'ABBRUCH', end_zeit = CURRENT_TIMESTAMP()" in query_str
```

### Test Case 1.2: Error Logging and Severity Mapping
* **Purpose**: Verify that `dwmsg_melde_fehler` correctly maps error parameters and updates the BigQuery audit table with the correct severity type, error number, and optional context fields (`zusatz1`, `zusatz2`).
* **Setup**:
  * Mock the BigQuery client.
* **Action**:
  * Call `dwmsg_melde_fehler("10001", "F", "10", "ErrorCode ist: 1", "Optional context")`.
* **Pass/Fail Criterion**:
  * **Pass**: The query updates `fehler_typ`, `fehler_nr`, `zusatz1`, and `zusatz2` using parameterized values. `fehler_nr` must be cast to an integer (`INT64`) to match the target schema.
  * **Fail**: Type mismatch (e.g., passing `fehler_nr` as a string instead of an integer) or incorrect parameter mapping.

```python
@patch("f_alis_msgerr._get_bq_client")
def test_melde_fehler(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    f_alis_msgerr.dwmsg_melde_fehler("10001", "F", "10", "ErrorCode ist: 1", "Optional context")
    
    assert mock_client.query.call_count == 1
    job_config = mock_client.query.call_args[1]["job_config"]
    params = {p.name: p.value for p in job_config.query_parameters}
    
    assert params["typ"] == "F"
    assert params["eintrags_nr"] == "10001"
    assert params["fehler_nr"] == 10  # Must be integer
    assert params["zusatz1"] == "ErrorCode ist: 1"
    assert params["zusatz2"] == "Optional context"
```

### Test Case 1.3: Date Parsing and Format Conversion
* **Purpose**: Verify that `oracle_to_python_fmt` correctly translates Oracle-style date format masks to Python `strftime` format masks, and that `dwmsg_setze_stichtag_info` parses and updates the `stichtag` column as a native BigQuery `DATE`.
* **Setup**:
  * Mock the BigQuery client.
* **Action**:
  * Call `dwmsg_setze_stichtag_info("10001", "20231031", "YYYYMMDD")`.
  * Call `dwmsg_setze_stichtag_info("10001", "2023-10-31", "YYYY-MM-DD")`.
* **Pass/Fail Criterion**:
  * **Pass**: The date string is correctly parsed into a Python `datetime.date` object and passed to BigQuery as a `DATE` parameter type.
  * **Fail**: Parsing raises a `ValueError` or the query parameter is sent as a raw string instead of a structured date.

```python
@patch("f_alis_msgerr._get_bq_client")
def test_setze_stichtag_info(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    # Test YYYYMMDD
    f_alis_msgerr.dwmsg_setze_stichtag_info("10001", "20231031", "YYYYMMDD")
    job_config = mock_client.query.call_args[1]["job_config"]
    params = {p.name: p.value for p in job_config.query_parameters}
    assert params["stichtag"] == datetime.date(2023, 10, 31)

    # Test YYYY-MM-DD
    f_alis_msgerr.dwmsg_setze_stichtag_info("10001", "2023-10-31", "YYYY-MM-DD")
    job_config = mock_client.query.call_args[1]["job_config"]
    params = {p.name: p.value for p in job_config.query_parameters}
    assert params["stichtag"] == datetime.date(2023, 10, 31)
```

### Test Case 1.4: Log Filename Generation (GCS vs Local Path)
* **Purpose**: Verify that `dwmsg_logdateiname` correctly constructs log file paths, supporting both local directories and Google Cloud Storage (`gs://`) URI schemes.
* **Setup**:
  * Set `GCS_LOGS_BUCKET` environment variable.
* **Action**:
  1. Call `dwmsg_logdateiname("JOB_A", "12345")` with `GCS_LOGS_BUCKET="gs://my-logs-bucket"`.
  2. Call `dwmsg_logdateiname("JOB_A", "12345")` with `GCS_LOGS_BUCKET=""` and `DW_DIR_PROT="/var/log/bert"`.
* **Pass/Fail Criterion**:
  * **Pass**: GCS paths are joined using forward slashes without doubling them (e.g., `gs://my-logs-bucket/JOB_A_..._12345.log`). Local paths are joined using the OS-native path separator.
  * **Fail**: Double slashes in GCS URIs (e.g., `gs://my-logs-bucket//JOB_A...`) or incorrect path construction.

```python
def test_logdateiname_generation(monkeypatch):
    # Test GCS Bucket pathing
    monkeypatch.setenv("GCS_LOGS_BUCKET", "gs://my-logs-bucket/")
    monkeypatch.setenv("DW_DIR_PROT", "/var/log/bert")
    f_alis_msgerr.DW_DIR_PROT = "gs://my-logs-bucket/"
    
    log_path = f_alis_msgerr.dwmsg_logdateiname("JOB_A", "12345")
    assert log_path.startswith("gs://my-logs-bucket/")
    assert log_path.endswith("_12345.log")
    assert "//JOB_A" not in log_path

    # Test Local pathing fallback
    monkeypatch.delenv("GCS_LOGS_BUCKET", raising=False)
    f_alis_msgerr.DW_DIR_PROT = "/var/log/bert"
    
    log_path_local = f_alis_msgerr.dwmsg_logdateiname("JOB_A", "12345")
    assert log_path_local.startswith("/var/log/bert")
    assert log_path_local.endswith("_12345.log")
```

---

## Section 2: `h_alis_date.py` (Date Arithmetic & Validation)

This module replaces legacy shell-based date arithmetic and Oracle database queries with native, high-performance Python date calculations.

### Test Case 2.1: Vormonat (Previous Month) Calculation and Formatting
* **Purpose**: Verify that `dw_date_vormonat` correctly calculates the first day of the previous month and formats it according to Oracle-style format strings.
* **Setup**:
  * Mock `datetime.date.today` to return a fixed date (e.g., `2024-03-15`).
* **Action**:
  * Call `dw_date_vormonat("MY_VAR", "YYYYMM")`.
  * Call `dw_date_vormonat("MY_VAR", "YYYY-MM-DD")`.
* **Pass/Fail Criterion**:
  * **Pass**: For a current date of `2024-03-15`, the previous month's start date must be calculated as `2024-02-01`. The output printed to stdout must match the shell-compatible assignment format: `MY_VAR='202402'` and `MY_VAR='2024-02-01'`.
  * **Fail**: Incorrect month calculation (especially across year boundaries, e.g., January to December) or incorrect string formatting.

```python
import h_alis_date

def test_dw_date_vormonat(capsys, monkeypatch):
    # Mock today to be 2024-03-15
    class MockDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2024, 3, 15)
    monkeypatch.setattr(h_alis_date, "date", MockDate)

    # Test YYYYMM format
    res = h_alis_date.dw_date_vormonat("MY_VAR", "YYYYMM")
    assert res == "202402"
    captured = capsys.readouterr()
    assert captured.out.strip() == "MY_VAR='202402'"

    # Test YYYY-MM-DD format
    res = h_alis_date.dw_date_vormonat("MY_VAR", "YYYY-MM-DD")
    assert res == "2024-02-01"
    captured = capsys.readouterr()
    assert captured.out.strip() == "MY_VAR='2024-02-01'"
```

### Test Case 2.2: Date Validation and Relational Checks
* **Purpose**: Verify that `dw_date_datum_check` and `dw_date_datum_le` correctly validate date formats and assert chronological order, raising/logging the exact German error messages on failure.
* **Setup**: None.
* **Action**:
  1. Call `dw_date_datum_check("20231031", "YYYYMMDD")`.
  2. Call `dw_date_datum_check("31.10.2023", "DD.MM.YYYY")`.
  3. Call `dw_date_datum_check("invalid-date", "YYYYMMDD")`.
  4. Call `dw_date_datum_le("20231031", "20231101")`.
  5. Call `dw_date_datum_le("20231101", "20231031")`.
* **Pass/Fail Criterion**:
  * **Pass**: Format checks return `True` for valid dates and `False` for invalid ones. `dw_date_datum_le` returns `True` if `datum1 <= datum2`. If `datum1 > datum2`, it prints the exact legacy German error message to stderr: `"Datum <datum1> ist groesser als <datum2>"` and returns `False`.
  * **Fail**: Incorrect boolean returns, or mismatch in the printed error message.

```python
def test_date_validation_and_comparison(capsys):
    # Format checks
    assert h_alis_date.dw_date_datum_check("20231031", "YYYYMMDD") is True
    assert h_alis_date.dw_date_datum_check("31.10.2023", "DD.MM.YYYY") is True
    assert h_alis_date.dw_date_datum_check("2023-13-01", "YYYY-MM-DD") is False

    # Chronological checks
    assert h_alis_date.dw_date_datum_le("20231031", "20231101") is True
    assert h_alis_date.dw_date_datum_le("20231031", "20231031") is True
    
    # Failure check (must print exact legacy error message)
    assert h_alis_date.dw_date_datum_le("20231101", "20231031") is False
    captured = capsys.readouterr()
    assert "Datum 20231101 ist groesser als 20231031" in captured.err
```

### Test Case 2.3: Date Range Generation (Gib_Zeitraum)
* **Purpose**: Verify that `dw_date_gib_zeitraum` correctly calculates intervals for Days (`'D'`), Months (`'M'`), and Years (`'Y'`) with leap year handling.
* **Setup**:
  * Mock `datetime.date.today` to return `2024-02-15` (leap year).
* **Action**:
  1. Call `dw_date_gib_zeitraum(10, "D", "YYYYMMDD", "START", "ENDE")`.
  2. Call `dw_date_gib_zeitraum(-1, "M", "YYYYMMDD", "START", "ENDE")`.
  3. Call `dw_date_gib_zeitraum(1, "Y", "YYYYMMDD", "START", "ENDE")`.
* **Pass/Fail Criterion**:
  * **Pass**:
    * Day offset `10` from `2024-02-15` returns `START='20240215'` and `ENDE='20240225'`.
    * Month offset `-1` returns the first day of the current month as start (`2024-02-01`) and the last day of the target month as end (`2024-01-31`).
    * Year offset `1` returns the first day of the current year as start (`2024-01-01`) and the last day of the target year as end (`2025-12-31`).
  * **Fail**: Incorrect date boundary calculations or format mismatches.

```python
def test_dw_date_gib_zeitraum(capsys, monkeypatch):
    class MockDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2024, 2, 15)
    monkeypatch.setattr(h_alis_date, "date", MockDate)

    # Test Day level
    start, end = h_alis_date.dw_date_gib_zeitraum(10, "D", "YYYYMMDD", "S", "E")
    assert start == "20240215"
    assert end == "20240225"

    # Test Month level (relative to 2024-02-15, offset -1 month -> Jan 2024)
    start, end = h_alis_date.dw_date_gib_zeitraum(-1, "M", "YYYYMMDD", "S", "E")
    assert start == "20240201"  # Start is first of current month
    assert end == "20240131"    # End is last day of target month (Jan has 31 days)

    # Test Year level (relative to 2024-02-15, offset +1 year -> 2025)
    start, end = h_alis_date.dw_date_gib_zeitraum(1, "Y", "YYYYMMDD", "S", "E")
    assert start == "20240101"  # Start is first day of current year
    assert end == "2025-12-31".replace("-", "")  # End is last day of target year
```

### Test Case 2.4: Date Addition and Month-End Logic
* **Purpose**: Verify that `addiere_datum`, `letzter_tag_des_monats`, and `tage_im_monat` handle leap years (e.g., 2000, 2024 vs 2100) and roll over months/years correctly.
* **Setup**: None.
* **Action**:
  1. Call `letzter_tag_des_monats("20240229")` (leap year) and `letzter_tag_des_monats("20230228")` (non-leap year).
  2. Call `tage_im_monat(2100, 2)` (century year, not leap year).
  3. Call `addiere_datum("20231231", 1)` and `addiere_datum("20240228", 2)`.
* **Pass/Fail Criterion**:
  * **Pass**:
    * `20240229` is identified as the last day of the month (`True`), while `20230228` is also identified as the last day of the month (`True`).
    * `tage_im_monat(2100, 2)` returns `28`.
    * `addiere_datum("20231231", 1)` returns `"20240101"`.
    * `addiere_datum("20240228", 2)` returns `"20240301"`.
  * **Fail**: Incorrect leap year evaluation or rollover logic.

```python
def test_date_addition_and_month_end():
    # Month-end checks
    assert h_alis_date.letzter_tag_des_monats("20240229") is True
    assert h_alis_date.letzter_tag_des_monats("20240228") is False
    assert h_alis_date.letzter_tag_des_monats("20230228") is True

    # Days in month checks
    assert h_alis_date.tage_im_monat(2024, 2) == 29
    assert h_alis_date.tage_im_monat(2023, 2) == 28
    assert h_alis_date.tage_im_monat(2100, 2) == 28  # 2100 is not a leap year

    # Date addition checks
    assert h_alis_date.addiere_datum("20231231", 1) == "20240101"
    assert h_alis_date.addiere_datum("20240228", 2) == "20240301"
```

---

## Section 3: `h_alis_parameter.py` (Parameter Parsing & Normalization)

This module parses, normalizes, and validates business-critical parameters (such as source system names, key figure identifiers/KPIs, and execution date intervals).

### Test Case 3.1: Parameter Presence and Error State Tracking
* **Purpose**: Verify that `pruefeParameterGesetzt` correctly tracks missing parameters using the global `ErrNr` and `ErrArg` variables.
* **Setup**:
  * Reset global error variables: `h_alis_parameter.ErrNr = 0`, `h_alis_parameter.ErrArg = ""`.
* **Action**:
  1. Set `os.environ["TEST_VAR"] = "populated"`. Call `pruefeParameterGesetzt("Test Parameter", "TEST_VAR")`.
  2. Set `os.environ["TEST_VAR"] = ""`. Call `pruefeParameterGesetzt("Test Parameter", "TEST_VAR")`.
* **Pass/Fail Criterion**:
  * **Pass**: When the environment variable is populated, `ErrNr` remains `0`. When the environment variable is empty or missing, `ErrNr` is set to `194` and `ErrArg` is set to the parameter name (`"Test Parameter"`).
  * **Fail**: Failure to detect empty variables or incorrect error code assignment.

```python
import h_alis_parameter

def test_parameter_presence(monkeypatch):
    # Reset error state
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""

    # Case 1: Parameter is set
    monkeypatch.setenv("TEST_VAR", "some_value")
    h_alis_parameter.pruefeParameterGesetzt("MyParam", "TEST_VAR")
    assert h_alis_parameter.ErrNr == 0

    # Case 2: Parameter is empty
    monkeypatch.setenv("TEST_VAR", "")
    h_alis_parameter.pruefeParameterGesetzt("MyParam", "TEST_VAR")
    assert h_alis_parameter.ErrNr == 194
    assert h_alis_parameter.ErrArg == "MyParam"
```

### Test Case 3.2: KPI and System Normalization
* **Purpose**: Verify that `konvertiereKennzahl`, `konvertiereSystem`, and `konvertiereSDName` correctly map long-form names to standard abbreviations and set error states for unknown inputs.
* **Setup**:
  * Reset global error variables.
* **Action**:
  1. Set `os.environ["KPI_VAR"] = "ZUGANG"`. Call `konvertiereKennzahl("KPI_VAR")`.
  2. Set `os.environ["SYS_VAR"] = "CARMEN"`. Call `konvertiereSystem("SYS_VAR")`.
  3. Set `os.environ["SD_VAR"] = "rahmenvertrag"`. Call `konvertiereSDName("SD_VAR")`.
  4. Set `os.environ["KPI_VAR"] = "invalid_kpi"`. Call `konvertiereKennzahl("KPI_VAR")`.
* **Pass/Fail Criterion**:
  * **Pass**:
    * `"ZUGANG"` is normalized to `"zug"`.
    * `"CARMEN"` is normalized to `"carmen"`.
    * `"rahmenvertrag"` is normalized to `"rv"`.
    * `"invalid_kpi"` sets `ErrNr` to `198` and the variable value to `"???"`.
  * **Fail**: Incorrect mapping or failure to set error codes on invalid inputs.

```python
def test_normalization(monkeypatch):
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""

    # Test KPI normalization
    monkeypatch.setenv("KPI_VAR", "ZUGANG")
    h_alis_parameter.konvertiereKennzahl("KPI_VAR")
    assert os.environ["KPI_VAR"] == "zug"
    assert h_alis_parameter.ErrNr == 0

    # Test System normalization
    monkeypatch.setenv("SYS_VAR", "CARMEN")
    h_alis_parameter.konvertiereSystem("SYS_VAR")
    assert os.environ["SYS_VAR"] == "carmen"
    assert h_alis_parameter.ErrNr == 0

    # Test Master Data normalization
    monkeypatch.setenv("SD_VAR", "rahmenvertrag")
    h_alis_parameter.konvertiereSDName("SD_VAR")
    assert os.environ["SD_VAR"] == "rv"
    assert h_alis_parameter.ErrNr == 0

    # Test Invalid KPI
    monkeypatch.setenv("KPI_VAR", "invalid_kpi")
    h_alis_parameter.konvertiereKennzahl("KPI_VAR")
    assert os.environ["KPI_VAR"] == "???"
    assert h_alis_parameter.ErrNr == 198
```

### Test Case 3.3: System-KPI Compatibility Matrix
* **Purpose**: Verify that `pruefeSystemKennzahl` correctly flags invalid combinations of source systems and KPIs (e.g., `sap` with `zug` is invalid, `nnv` with `tvd` is valid).
* **Setup**:
  * Reset global error variables.
* **Action**:
  1. Call `pruefeSystemKennzahl("nnv", "tvd")`.
  2. Call `pruefeSystemKennzahl("sap", "zug")`.
  3. Call `pruefeSystemKennzahl("carmen", "twe")`.
* **Pass/Fail Criterion**:
  * **Pass**: Valid combinations leave `ErrNr` as `0`. Invalid combinations set `ErrNr` to `195` and `ErrArg` to `"Ungueltige Kombination <system> <kennzahl>"`.
  * **Fail**: Allowing an invalid combination or blocking a valid one.

```python
def test_system_kpi_compatibility():
    # Reset error state
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""

    # Valid combination
    h_alis_parameter.pruefeSystemKennzahl("nnv", "tvd")
    assert h_alis_parameter.ErrNr == 0

    # Invalid combination (SAP cannot deliver ZUG)
    h_alis_parameter.pruefeSystemKennzahl("sap", "zug")
    assert h_alis_parameter.ErrNr == 195
    assert "Ungueltige Kombination sap zug" in h_alis_parameter.ErrArg

    # Reset and test another invalid combination
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.pruefeSystemKennzahl("carmen", "twe")
    assert h_alis_parameter.ErrNr == 195
    assert "Ungueltige Kombination carmen twe" in h_alis_parameter.ErrArg
```

### Test Case 3.4: Area and Interval Mapping
* **Purpose**: Verify that `gibBereich` and `gibIntervall` correctly categorize KPIs into business areas (`tn`, `us`, `gd`, `sd`, `md`) and temporal granularities (`t`, `m`).
* **Setup**:
  * Reset global error variables.
* **Action**:
  1. Call `gibBereich("zug")` and `gibIntervall("zug")`.
  2. Call `gibBereich("bst")` and `gibIntervall("bst")`.
  3. Call `gibBereich("tvd")` and `gibIntervall("tvd")`.
* **Pass/Fail Criterion**:
  * **Pass**:
    * `"zug"` maps to area `"tn"` and interval `"t"` (daily).
    * `"bst"` maps to area `"tn"` and interval `"m"` (monthly).
    * `"tvd"` maps to area `"gd"` and interval `"m"` (monthly).
  * **Fail**: Incorrect area or interval mapping.

```python
def test_area_and_interval_mapping(monkeypatch):
    h_alis_parameter.ErrNr = 0
    h_alis_parameter.ErrArg = ""

    # Test ZUG (Zugang) -> Area: tn, Interval: t
    monkeypatch.setenv("AREA_VAR", "")
    h_alis_parameter.gibBereich("zug")
    # Note: gibBereich writes to the environment variable passed as the second argument in legacy,
    # but in Python it can return the value or write to os.environ depending on the implementation.
    # Let's test the environment variable mutation to match the legacy behavior.
    h_alis_parameter.gibBereich("zug", "AREA_VAR")
    assert os.environ["AREA_VAR"] == "tn"

    monkeypatch.setenv("INT_VAR", "")
    h_alis_parameter.gibIntervall("zug", "INT_VAR")
    assert os.environ["INT_VAR"] == "t"

    # Test BST (Bestand) -> Area: tn, Interval: m
    h_alis_parameter.gibBereich("bst", "AREA_VAR")
    assert os.environ["AREA_VAR"] == "tn"
    h_alis_parameter.gibIntervall("bst", "INT_VAR")
    assert os.environ["INT_VAR"] == "m"
```

---

## Section 4: `h_alis_sqlplus.py` (SQL Execution Wrapper)

This module wraps the execution of SQL scripts. In the target architecture, while hybrid phases may still call `sqlplus`, the wrapper must validate parameters and file readability before execution.

### Test Case 4.1: SQL Script Pre-flight Checks and Execution
* **Purpose**: Verify that `starteSQLSkript` validates parameter presence, checks file readability, logs parameters, and executes the script, returning the correct exit code.
* **Setup**:
  * Mock the external `DWMSG_MeldeFehler` command to prevent execution failures during unit testing.
  * Mock `subprocess.run` to simulate SQL*Plus execution.
* **Action**:
  1. Call `starteSQLSkript("", "script.sql")` (missing entry number).
  2. Call `starteSQLSkript("10001", "non_existent_file.sql")` (missing script file).
  3. Call `starteSQLSkript("10001", "valid_script.sql", "param1", "param2")` where `valid_script.sql` exists and is readable.
* **Pass/Fail Criterion**:
  * **Pass**:
    * Missing parameters return exit code `196` and trigger error logging.
    * Unreadable files return exit code `201` and trigger error logging.
    * Valid scripts execute `sqlplus` with the correct arguments and return the exit code of the subprocess.
  * **Fail**: Executing a non-existent script, or failing to propagate the subprocess exit code.

```python
import h_alis_sqlplus
import pathlib

@patch("h_alis_sqlplus.call_dwmsg_meldefehler")
@patch("subprocess.run")
def test_starte_sql_skript(mock_run, mock_melde_fehler, tmp_path):
    # Case 1: Missing parameters
    rc = h_alis_sqlplus.starteSQLSkript("", "script.sql")
    assert rc == 196
    mock_melde_fehler.assert_called_with("", "E", 196, "alis_sqlplus V1.1.3 starteSQLSkript")

    # Case 2: Non-existent script file
    rc = h_alis_sqlplus.starteSQLSkript("10001", "non_existent.sql")
    assert rc == 201
    mock_melde_fehler.assert_called_with("10001", "E", 201, "non_existent.sql")

    # Case 3: Valid script execution
    # Create a dummy script file
    dummy_script = tmp_path / "test_script.sql"
    dummy_script.write_text("SELECT 1 FROM DUAL;")

    # Mock successful subprocess execution (exit code 0)
    mock_process = MagicMock()
    mock_process.return_code = 0
    mock_run.return_value = mock_process

    os.environ["DW_ORAUSER"] = "user/pass@db"
    rc = h_alis_sqlplus.starteSQLSkript("10001", str(dummy_script), "arg1", "arg2")
    
    # Verify subprocess call structure
    assert mock_run.call_count == 1
    executed_args = mock_run.call_args[0][0]
    assert executed_args[0] == "sqlplus"
    assert executed_args[1] == "user/pass@db"
    assert executed_args[2] == f"@{dummy_script}"
    assert executed_args[3:] == ["arg1", "arg2"]
```