The migration of `h_alis_parameter.ksh` to a Python module `alis_parameter.py` requires comprehensive validation to ensure behavioral equivalence. The tests below cover output parity, transformation correctness, external system replacements (specifically `DWDate_*` functions), and data quality assertions for input parameters.

We will use `pytest` for testing and `pytest-mock` (or `unittest.mock.patch`) to control `datetime.date.today()` for deterministic date calculations.

**Test Setup (Conceptual):**

```python
# conftest.py (or similar for pytest fixtures)
import pytest
from freezegun import freeze_time
import datetime

@pytest.fixture
def fixed_datetime():
    """Fixture to freeze datetime.date.today() for consistent test results."""
    with freeze_time("2023-10-26"): # A specific date for deterministic tests
        yield

# test_alis_parameter.py (or similar for test file)
import pytest
from alis_parameter import (
    pruefe_parameter_gesetzt, konvertiere_kennzahl, konvertiere_system,
    konvertiere_sd_name, konvertiere_aufb_stufe_xtra, pruefe_system_kennzahl,
    gib_bereich, gib_intervall, pruefe_zahl_positiv, pruefe_zeit_parameter,
    pruefe_zeitraum, konvertiere_zeitspanne,
    ParameterError, ValidationError,
    _dwdate_datum_check, _dwdate_datum_le, _dwdate_gib_zeitraum # Internal functions for direct testing
)
import datetime
```

---

### Test Case 1: `pruefe_parameter_gesetzt` - Basic Validation

**Purpose:** Verify that the `pruefe_parameter_gesetzt` function correctly identifies unset or empty parameters and raises the appropriate `ParameterError`.

**Setup:**
*   Call the function with various combinations of `param_name` and `param_value`.

**Action:**
*   Call `pruefe_parameter_gesetzt("TestParam", "some_value")`
*   Call `pruefe_parameter_gesetzt("EmptyParam", "")`
*   Call `pruefe_parameter_gesetzt("NoneParam", None)`
*   Call `pruefe_parameter_gesetzt("", "value")` (internal error case)

**Pass/Fail Criterion:**
*   For valid inputs (`"some_value"`), the function should complete without raising an exception.
*   For empty string or `None` values, a `ParameterError` with `error_code=194` and `arg` matching the `param_name` should be raised.
*   For an empty `param_name` itself, a `ParameterError` with `error_code=196` should be raised.

**Runnable Test Code (pytest):**

```python
def test_pruefe_parameter_gesetzt_valid():
    """Test with a valid, non-empty parameter."""
    try:
        pruefe_parameter_gesetzt("TestParam", "some_value")
    except Exception as e:
        pytest.fail(f"Unexpected exception for valid parameter: {e}")

def test_pruefe_parameter_gesetzt_empty_string():
    """Test with an empty string parameter."""
    with pytest.raises(ParameterError) as excinfo:
        pruefe_parameter_gesetzt("EmptyParam", "")
    assert excinfo.value.error_code == 194
    assert excinfo.value.arg == "EmptyParam"
    assert "Parameter 'EmptyParam' is not set." in str(excinfo.value)

def test_pruefe_parameter_gesetzt_none_value():
    """Test with a None parameter."""
    with pytest.raises(ParameterError) as excinfo:
        pruefe_parameter_gesetzt("NoneParam", None)
    assert excinfo.value.error_code == 194
    assert excinfo.value.arg == "NoneParam"
    assert "Parameter 'NoneParam' is not set." in str(excinfo.value)

def test_pruefe_parameter_gesetzt_empty_param_name():
    """Test with an empty param_name (internal error)."""
    with pytest.raises(ParameterError) as excinfo:
        pruefe_parameter_gesetzt("", "value")
    assert excinfo.value.error_code == 196
    assert "Internal error: 'param_name' must be a non-empty string." in str(excinfo.value)
```

---

### Test Case 2: `konvertiere_kennzahl` - Mapping and Error Handling

**Purpose:** Verify that `konvertiere_kennzahl` correctly converts full key figure names to their abbreviations and handles unknown names.

**Setup:**
*   Provide known key figure names (e.g., "zugang", "bestand", "gutschrift").
*   Provide an unknown key figure name (e.g., "unbekannt").
*   Provide empty input.

**Action:**
*   Call `konvertiere_kennzahl` with each input.

**Pass/Fail Criterion:**
*   For known names, the function should return the correct abbreviation (e.g., "zug", "bst", "gut").
*   The conversion should be case-insensitive.
*   For an unknown name, a `ValidationError` with `error_code=198` and `arg` matching the unknown name should be raised.
*   For empty input, a `ParameterError` with `error_code=196` should be raised.

**Runnable Test Code (pytest):**

```python
def test_konvertiere_kennzahl_valid_mappings():
    """Test known key figure conversions (case-insensitive)."""
    assert konvertiere_kennzahl("zugang") == "zug"
    assert konvertiere_kennzahl("BESTAND") == "bst"
    assert konvertiere_kennzahl("Gutschrift") == "gut"
    assert konvertiere_kennzahl("teilnehmerverbindungsdaten") == "tvd"
    assert konvertiere_kennzahl("netznutzungsklassen") == "nnk"
    assert konvertiere_kennzahl("bewegart") == "bwa"

def test_konvertiere_kennzahl_unknown():
    """Test an unknown key figure."""
    with pytest.raises(ValidationError) as excinfo:
        konvertiere_kennzahl("unbekannt")
    assert excinfo.value.error_code == 198
    assert excinfo.value.arg == "unbekannt"
    assert "Unknown key figure: 'unbekannt'" in str(excinfo.value)

def test_konvertiere_kennzahl_empty_input():
    """Test with empty input."""
    with pytest.raises(ParameterError) as excinfo:
        konvertiere_kennzahl("")
    assert excinfo.value.error_code == 196
    assert "Input 'kennzahl' cannot be empty." in str(excinfo.value)
```

---

### Test Case 3: `konvertiere_system` - Mapping and Error Handling

**Purpose:** Verify that `konvertiere_system` correctly converts system names to their abbreviations and handles unknown systems.

**Setup:**
*   Provide known system names (e.g., "sap", "carmen", "d1").
*   Provide an unknown system name (e.g., "unbekannt_system").
*   Provide empty input.

**Action:**
*   Call `konvertiere_system` with each input.

**Pass/Fail Criterion:**
*   For known names, the function should return the correct abbreviation (which is often the lowercase version of the input itself for this function).
*   The conversion should be case-insensitive.
*   For an unknown name, a `ValidationError` with `error_code=195` and `arg` matching the unknown name should be raised.
*   For empty input, a `ParameterError` with `error_code=196` should be raised.

**Runnable Test Code (pytest):**

```python
def test_konvertiere_system_valid_mappings():
    """Test known system conversions (case-insensitive)."""
    assert konvertiere_system("sap") == "sap"
    assert konvertiere_system("CARMEN") == "carmen"
    assert konvertiere_system("D1") == "d1"
    assert konvertiere_system("Brunet") == "brunet"

def test_konvertiere_system_unknown():
    """Test an unknown system."""
    with pytest.raises(ValidationError) as excinfo:
        konvertiere_system("unbekannt_system")
    assert excinfo.value.error_code == 195
    assert excinfo.value.arg == "unbekannt_system"
    assert "Unknown data source system: 'unbekannt_system'!" in str(excinfo.value)

def test_konvertiere_system_empty_input():
    """Test with empty input."""
    with pytest.raises(ParameterError) as excinfo:
        konvertiere_system("")
    assert excinfo.value.error_code == 196
    assert "Input 'system_name' cannot be empty." in str(excinfo.value)
```

---

### Test Case 4: `pruefe_system_kennzahl` - Combination Logic

**Purpose:** Verify the complex conditional logic within `pruefe_system_kennzahl` for valid and invalid system-key figure combinations.

**Setup:**
*   Provide various combinations of (system, kennzahl) pairs, including valid ones and those expected to fail based on the KSH script's `if/elif` logic.
*   Assume `system` and `kennzahl` are already in their abbreviated, lowercase forms.

**Action:**
*   Call `pruefe_system_kennzahl` with each pair.

**Pass/Fail Criterion:**
*   For valid combinations, the function should complete without raising an exception.
*   For invalid combinations, a `ValidationError` with `error_code=195` and an `arg` describing the invalid combination should be raised.
*   For empty inputs, a `ParameterError` with `error_code=196` should be raised.

**Runnable Test Code (pytest):**

```python
def test_pruefe_system_kennzahl_valid_combinations():
    """Test known valid system-kennzahl combinations."""
    try:
        pruefe_system_kennzahl("sap", "srs") # SAP can have SRS
        pruefe_system_kennzahl("carmen", "zug") # Carmen can have ZUG
        pruefe_system_kennzahl("d1", "bst") # D1 can have BST
        pruefe_system_kennzahl("nnv", "tvd") # NNV must have TVD or LKL
        pruefe_system_kennzahl("nnv", "lkl")
        pruefe_system_kennzahl("ctel", "abg") # CTEL must have ABG, BST, ZUG, TWE
        pruefe_system_kennzahl("xtra", "rst") # XTRA must have RST
        pruefe_system_kennzahl("dwh", "mds") # DWH must have MDS
        pruefe_system_kennzahl("brunet", "d1n") # Brunet must have D1N, RUB, LMO
        pruefe_system_kennzahl("sigma", "nnk") # Sigma has many specific ones
    except Exception as e:
        pytest.fail(f"Unexpected exception for valid combination: {e}")

def test_pruefe_system_kennzahl_invalid_combinations():
    """Test known invalid system-kennzahl combinations."""
    # NNV with non-tvd/lkl
    with pytest.raises(ValidationError) as excinfo:
        pruefe_system_kennzahl("nnv", "zug")
    assert excinfo.value.error_code == 195
    assert excinfo.value.arg == "Invalid combination nnv zug"

    # Carmen with TWE (explicitly forbidden)
    with pytest.raises(ValidationError) as excinfo:
        pruefe_system_kennzahl("carmen", "twe")
    assert excinfo.value.error_code == 195
    assert excinfo.value.arg == "Invalid combination carmen twe"

    # SAP with ZUG (explicitly forbidden)
    with pytest.raises(ValidationError) as excinfo:
        pruefe_system_kennzahl("sap", "zug")
    assert excinfo.value.error_code == 195
    assert excinfo.value.arg == "Invalid combination sap zug"

    # DPPS with TWE (explicitly forbidden)
    with pytest.raises(ValidationError) as excinfo:
        pruefe_system_kennzahl("dpps", "twe")
    assert excinfo.value.error_code == 195
    assert excinfo.value.arg == "Invalid combination dpps twe"

    # CTEL with non-allowed (e.g., PLS)
    with pytest.raises(ValidationError) as excinfo:
        pruefe_system_kennzahl("ctel", "pln")
    assert excinfo.value.error_code == 195
    assert excinfo.value.arg == "Invalid combination ctel pln"

    # XTRA with non-RST
    with pytest.raises(ValidationError) as excinfo:
        pruefe_system_kennzahl("xtra", "zug")
    assert excinfo.value.error_code == 195
    assert excinfo.value.arg == "Invalid combination xtra zug"

    # D1 with GUT (explicitly forbidden)
    with pytest.raises(ValidationError) as excinfo:
        pruefe_system_kennzahl("d1", "gut")
    assert excinfo.value.error_code == 195
    assert excinfo.value.arg == "Invalid combination d1 gut"

def test_pruefe_system_kennzahl_empty_inputs():
    """Test with empty system or kennzahl."""
    with pytest.raises(ParameterError) as excinfo:
        pruefe_system_kennzahl("", "zug")
    assert excinfo.value.error_code == 196

    with pytest.raises(ParameterError) as excinfo:
        pruefe_system_kennzahl("sap", "")
    assert excinfo.value.error_code == 196
```

---

### Test Case 5: `_dwdate_datum_check` - Date Format Validation (External System Replacement)

**Purpose:** Verify the `_dwdate_datum_check` helper function (replacement for `DWDate_Datum_Check`) correctly validates date strings against a given format.

**Setup:**
*   Provide valid date strings in `YYYYMMDD` format.
*   Provide invalid date strings (wrong format, non-existent date).

**Action:**
*   Call `_dwdate_datum_check` with each date string.

**Pass/Fail Criterion:**
*   Return `True` for valid dates.
*   Return `False` for invalid dates.

**Runnable Test Code (pytest):**

```python
def test_dwdate_datum_check_valid():
    """Test _dwdate_datum_check with valid dates."""
    assert _dwdate_datum_check("20231026") is True
    assert _dwdate_datum_check("19990101") is True
    assert _dwdate_datum_check("20000229") is True # Leap year

def test_dwdate_datum_check_invalid_format():
    """Test _dwdate_datum_check with invalid date formats."""
    assert _dwdate_datum_check("2023-10-26") is False
    assert _dwdate_datum_check("202310") is False
    assert _dwdate_datum_check("notadate") is False

def test_dwdate_datum_check_non_existent_date():
    """Test _dwdate_datum_check with non-existent dates."""
    assert _dwdate_datum_check("20230230") is False # Feb has only 28/29 days
    assert _dwdate_datum_check("20231301") is False # Invalid month
    assert _dwdate_datum_check("20230132") is False # Invalid day
```

---

### Test Case 6: `_dwdate_gib_zeitraum` - Date Range Calculation (External System Replacement)

**Purpose:** Verify the `_dwdate_gib_zeitraum` helper function (replacement for `DWDate_Gib_Zeitraum`) correctly calculates date ranges based on a span and unit, relative to a fixed "today".

**Setup:**
*   Use the `fixed_datetime` fixture to set `datetime.date.today()` to `2023-10-26`.
*   Provide various `span_value` (negative integers) and `unit` ('D' or 'M') combinations.

**Action:**
*   Call `_dwdate_gib_zeitraum` with each combination.

**Pass/Fail Criterion:**
*   Return the correct `(start_date_str, end_date_str)` tuple in `YYYYMMDD` format.
*   For 'D' unit, start and end dates should be the same.
*   For 'M' unit, start and end dates should be the first and last day of the target month.
*   Raise `ParameterError` for unknown units.

**Runnable Test Code (pytest):**

```python
def test_dwdate_gib_zeitraum_daily(fixed_datetime):
    """Test _dwdate_gib_zeitraum for daily spans."""
    # Today is 2023-10-26
    assert _dwdate_gib_zeitraum(-0, 'D') == ("20231026", "20231026")
    assert _dwdate_gib_zeitraum(-1, 'D') == ("20231025", "20231025")
    assert _dwdate_gib_zeitraum(-7, 'D') == ("20231019", "20231019")
    assert _dwdate_gib_zeitraum(-30, 'D') == ("20230926", "20230926")

def test_dwdate_gib_zeitraum_monthly(fixed_datetime):
    """Test _dwdate_gib_zeitraum for monthly spans."""
    # Today is 2023-10-26
    assert _dwdate_gib_zeitraum(-0, 'M') == ("20231001", "20231031")
    assert _dwdate_gib_zeitraum(-1, 'M') == ("20230901", "20230930")
    assert _dwdate_gib_zeitraum(-2, 'M') == ("20230801", "20230831")
    assert _dwdate_gib_zeitraum(-10, 'M') == ("20230101", "20230131") # Jan 2023
    assert _dwdate_gib_zeitraum(-11, 'M') == ("20221201", "20221231") # Dec 2022
    assert _dwdate_gib_zeitraum(-22, 'M') == ("20220101", "20220131") # Jan 2022
    assert _dwdate_gib_zeitraum(-23, 'M') == ("20211201", "20211231") # Dec 2021

    # Test across year boundary (e.g., from Oct 2023 to Feb 2023)
    assert _dwdate_gib_zeitraum(-8, 'M') == ("20230201", "20230228") # Feb 2023 (not leap year)
    # Test a leap year month (e.g., from Oct 2023 to Feb 2024, if today was 2024-10-26)
    # To test this, we need to adjust fixed_datetime
    with freeze_time("2024-10-26"): # 2024 is a leap year
        assert _dwdate_gib_zeitraum(-8, 'M') == ("20240201", "20240229")

def test_dwdate_gib_zeitraum_unknown_unit():
    """Test _dwdate_gib_zeitraum with an unknown unit."""
    with pytest.raises(ParameterError) as excinfo:
        _dwdate_gib_zeitraum(-1, 'W') # 'W' for week is not supported
    assert "Unknown unit for Zeitraum calculation: 'W'" in str(excinfo.value)
```

---

### Test Case 7: `konvertiere_zeitspanne` - Span to Date Range Conversion

**Purpose:** Verify that `konvertiere_zeitspanne` correctly converts a numeric time span and key figure into a start and end date, leveraging `pruefe_zahl_positiv` and `_dwdate_gib_zeitraum`.

**Setup:**
*   Use the `fixed_datetime` fixture to set `datetime.date.today()` to `2023-10-26`.
*   Provide `span_value_str` (e.g., "1", "30") and `kennzahl` (e.g., "zug" for daily, "bst" for monthly).

**Action:**
*   Call `konvertiere_zeitspanne` with each combination.

**Pass/Fail Criterion:**
*   Return the correct `(start_date_str, end_date_str)` tuple in `YYYYMMDD` format.
*   Raise `ValidationError` if `span_value_str` is not a positive number or if `_dwdate_gib_zeitraum` fails.

**Runnable Test Code (pytest):**

```python
def test_konvertiere_zeitspanne_daily_kennzahl(fixed_datetime):
    """Test konvertiere_zeitspanne for daily kennzahlen."""
    # Today is 2023-10-26
    assert konvertiere_zeitspanne("0", "zug") == ("20231026", "20231026")
    assert konvertiere_zeitspanne("1", "zug") == ("20231025", "20231025")
    assert konvertiere_zeitspanne("7", "abg") == ("20231019", "20231019")

def test_konvertiere_zeitspanne_monthly_kennzahl(fixed_datetime):
    """Test konvertiere_zeitspanne for monthly kennzahlen (bst)."""
    # Today is 2023-10-26
    assert konvertiere_zeitspanne("0", "bst") == ("20231001", "20231031")
    assert konvertiere_zeitspanne("1", "bst") == ("20230901", "20230930")
    assert konvertiere_zeitspanne("3", "bst") == ("20230701", "20230731")
    assert konvertiere_zeitspanne("10", "bst") == ("20230101", "20230131")

def test_konvertiere_zeitspanne_invalid_span_value():
    """Test konvertiere_zeitspanne with invalid span values."""
    with pytest.raises(ValidationError) as excinfo:
        konvertiere_zeitspanne("-1", "zug") # Negative span
    assert "Parameter 'Zeitspanne' must be greater than or equal to 0." in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        konvertiere_zeitspanne("abc", "zug") # Non-numeric span
    assert "Parameter 'Zeitspanne' is not a numeric value." in str(excinfo.value)

    with pytest.raises(ParameterError) as excinfo:
        konvertiere_zeitspanne("", "zug") # Empty span
    assert excinfo.value.error_code == 194
    assert excinfo.value.arg == "Zeitspanne"
```

---

### Test Case 8: `pruefe_zeit_parameter` - Date Parameter Combination Logic

**Purpose:** Verify that `pruefe_zeit_parameter` correctly validates combinations of start date, end date, and time offset, ensuring mutual exclusivity and basic validity.

**Setup:**
*   Provide various combinations of `start_date`, `end_date`, and `time_offset`.
*   This function internally calls `pruefe_zahl_positiv` and `pruefe_zeitraum`, so its error handling will reflect those.

**Action:**
*   Call `pruefe_zeit_parameter` with each combination.

**Pass/Fail Criterion:**
*   For valid combinations (either `time_offset` set OR both `start_date` and `end_date` set and valid), the function should complete without raising an exception.
*   For invalid combinations (e.g., `time_offset` and dates both set, or only one date set), a `ValidationError` with `error_code=195` should be raised.
*   Errors from nested calls (`pruefe_zahl_positiv`, `pruefe_zeitraum`) should propagate.

**Runnable Test Code (pytest):**

```python
def test_pruefe_zeit_parameter_valid_time_offset():
    """Test with a valid time_offset only."""
    try:
        pruefe_zeit_parameter(None, None, "5")
        pruefe_zeit_parameter("", "", "0")
    except Exception as e:
        pytest.fail(f"Unexpected exception for valid time_offset: {e}")

def test_pruefe_zeit_parameter_valid_dates():
    """Test with valid start and end dates only."""
    try:
        pruefe_zeit_parameter("20230101", "20230105", None)
        pruefe_zeit_parameter("20230101", "20230101", "")
    except Exception as e:
        pytest.fail(f"Unexpected exception for valid dates: {e}")

def test_pruefe_zeit_parameter_invalid_mix():
    """Test with an invalid mix of time_offset and dates."""
    with pytest.raises(ValidationError) as excinfo:
        pruefe_zeit_parameter("20230101", "20230105", "5")
    assert excinfo.value.error_code == 195
    assert "Only a time span OR both date values must be set, not a mix." in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        pruefe_zeit_parameter("20230101", None, "5")
    assert excinfo.value.error_code == 195
    assert "Only a time span OR both date values must be set, not a mix." in str(excinfo.value)

def test_pruefe_zeit_parameter_missing_all():
    """Test with all parameters missing."""
    with pytest.raises(ValidationError) as excinfo:
        pruefe_zeit_parameter(None, None, None)
    assert excinfo.value.error_code == 195
    assert "Date values or time span are missing." in str(excinfo.value)

def test_pruefe_zeit_parameter_missing_one_date():
    """Test with only one date provided."""
    with pytest.raises(ValidationError) as excinfo:
        pruefe_zeit_parameter("20230101", None, None)
    assert excinfo.value.error_code == 195
    assert "Both start and end dates must be provided." in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        pruefe_zeit_parameter(None, "20230105", None)
    assert excinfo.value.error_code == 195
    assert "Both start and end dates must be provided." in str(excinfo.value)

def test_pruefe_zeit_parameter_invalid_nested_date_format():
    """Test with invalid date format, propagated from pruefe_zeitraum."""
    with pytest.raises(ValidationError) as excinfo:
        pruefe_zeit_parameter("2023-01-01", "20230105", None)
    assert excinfo.value.error_code == 195
    assert "Start date '2023-01-01' does not match format %Y%m%d." in str(excinfo.value)

def test_pruefe_zeit_parameter_invalid_nested_date_order():
    """Test with invalid date order, propagated from pruefe_zeitraum."""
    with pytest.raises(ValidationError) as excinfo:
        pruefe_zeit_parameter("20230105", "20230101", None)
    assert excinfo.value.error_code == 195
    assert "Start date is not less than or equal to end date." in str(excinfo.value)

def test_pruefe_zeit_parameter_invalid_nested_time_offset_value():
    """Test with invalid time_offset value, propagated from pruefe_zahl_positiv."""
    with pytest.raises(ValidationError) as excinfo:
        pruefe_zeit_parameter(None, None, "-5")
    assert excinfo.value.error_code == 195
    assert "Parameter 'Zeitspanne' must be greater than or equal to 0." in str(excinfo.value)
```

---

### Test Case 9: `gib_bereich` and `gib_intervall` - Derivation Logic

**Purpose:** Verify that `gib_bereich` and `gib_intervall` correctly derive the "Bereich" (area) and "Intervall" (interval) for given key figures.

**Setup:**
*   Provide known key figures (abbreviations) that map to different areas/intervals.
*   Provide an unknown key figure.

**Action:**
*   Call `gib_bereich` and `gib_intervall` with each input.

**Pass/Fail Criterion:**
*   For known key figures, return the correct area/interval string.
*   The lookup should be case-insensitive.
*   For unknown key figures, raise a `ValidationError` with `error_code=196`.
*   For empty input, raise a `ParameterError` with `error_code=196`.

**Runnable Test Code (pytest):**

```python
def test_gib_bereich_valid_mappings():
    """Test gib_bereich with valid kennzahlen."""
    assert gib_bereich("zug") == "tn"
    assert gib_bereich("BST") == "tn"
    assert gib_bereich("gut") == "us"
    assert gib_bereich("srs") == "us"
    assert gib_bereich("tvd") == "gd"
    assert gib_bereich("nnk") == "gd"
    assert gib_bereich("ksd") == "sd"
    assert gib_bereich("mds") == "md"

def test_gib_bereich_unknown_kennzahl():
    """Test gib_bereich with an unknown kennzahl."""
    with pytest.raises(ValidationError) as excinfo:
        gib_bereich("unknown_kz")
    assert excinfo.value.error_code == 196
    assert "Abbreviation 'unknown_kz' unknown." in str(excinfo.value)

def test_gib_bereich_empty_input():
    """Test gib_bereich with empty input."""
    with pytest.raises(ParameterError) as excinfo:
        gib_bereich("")
    assert excinfo.value.error_code == 196
    assert "Input 'kennzahl' cannot be empty." in str(excinfo.value)

def test_gib_intervall_valid_mappings():
    """Test gib_intervall with valid kennzahlen."""
    assert gib_intervall("zug") == "t"
    assert gib_intervall("ABG") == "t"
    assert gib_intervall("bst") == "m"
    assert gib_intervall("tvd") == "m"
    assert gib_intervall("srs") == "t"
    assert gib_intervall("nnk") == "m"

def test_gib_intervall_unknown_kennzahl():
    """Test gib_intervall with an unknown kennzahl."""
    with pytest.raises(ValidationError) as excinfo:
        gib_intervall("unknown_kz")
    assert excinfo.value.error_code == 196
    assert "Abbreviation 'unknown_kz' unknown." in str(excinfo.value)

def test_gib_intervall_empty_input():
    """Test gib_intervall with empty input."""
    with pytest.raises(ParameterError) as excinfo:
        gib_intervall("")
    assert excinfo.value.error_code == 196
    assert "Input 'kennzahl' cannot be empty." in str(excinfo.value)
```