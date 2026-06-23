The migration of `h_alis_parameter.ksh` to `alis_parameter_utils.py` involves re-implementing KornShell utility functions in Python. The validation tests below focus on ensuring behavioral equivalence, covering output parity, transformation correctness, and proper handling of external dependencies and edge cases.

The provided Python code includes placeholder mappings. For the tests to be concrete, these mappings have been populated based on the `case` statements and conditional logic found in the legacy KornShell script snippet. The `ALLOWED_SYSTEM_KENNZAHL_COMBINATIONS` has been inferred by identifying all invalid combinations in the KSH script and assuming all other combinations are valid.

---

## Test Setup: Populated Mappings and Mocking

Before running any tests, the placeholder mappings in `alis_parameter_utils.py` must be populated with the actual values from the legacy KornShell script. Additionally, `datetime.now()` will be mocked for deterministic date calculations.

```python
import pytest
from unittest.mock import patch
from datetime import datetime, date, timedelta
import alis_parameter_utils as apu

# --- Populate Mappings from Legacy KSH Script ---
apu.KENNZAHL_MAP = {
    "ZUGANG": "zug", "ABGANG": "abg", "ABGANG_ZUKUNFT": "abz", "BESTAND": "bst",
    "TARIFWECHSEL": "twe", "PLAN": "pln", "GUTSCHRIFT": "gut", "AUFLADUNG": "auf",
    "RESTGUTHABEN": "rst", "TEILNEHMERVERBINDUNGSDATEN": "tvd", "USKONTO": "usk",
    "USTEILNEHMER": "ust", "LEISTUNGSKLASSE": "lkl", "LOESCHUNG": "loe",
    "REAKTIVIERUNG": "rak", "STANDARD_RECHNUNG": "srs", "STANDARD_GUTSCHRIFT": "sgs",
    "GUTSCHRIFT_RV": "sg_rv", "RECHNUNGEN_RV_DPPS": "sr_rv_dpps", "BEWEGART": "bwa",
    "KUNDENSTAMM": "ksd", "MAHNSTUFE": "mahn", "METADATENSTRUKTUR": "mds",
    "D1NEWS": "d1n", "RUBRIK": "rub", "LIEFERMODUS": "lmo",
    "NETZNUTZUNGSKLASSEN": "nnk", "TAGESVERKEHRSKURVEN": "tvk", "GESPRAECHSZIELE": "gz",
    "GESPRAECHSLAENGENVERTEILUNG": "glv", "ZONENKENNUNG": "zonek", "ZONENTYP": "zonet",
    "NETZNUTZUNGSKLASSENTYP": "nnkt", "TARIFART": "trfa", "GESPRAECHSTYP": "gtyp",
    "BASISDIENST": "basisd", "NATIONALINTERNATIONAL": "natint", "GLAENGENINTERVALL": "glint",
}

apu.SYSTEM_MAP = {
    "SAP": "sap", "CARMEN": "carmen", "DPPS": "dpps", "D1": "d1", "XTRA": "xtra",
    "CTEL": "ctel", "NNV": "nnv", "DWH": "dwh", "BRUNET": "brunet", "SIGMA": "sigma",
}

apu.SD_NAME_MAP = {
    "VO": "vo", "RAHMENVERTRAG": "rv", "TARIF": "trf", "TSTATUS": "ts", "ZAHLMODUS": "zm",
    "KDG_GRUND": "kdg", "GUTSCHRIFT": "gut", "AUFLADUNG": "auf", "LEISTUNG": "l_leist",
    "GUTSCHRIHRT_GRUND": "l_gutgr", "SAP_GUTSCHRIFT_GRUND": "sap_l_gutgr", "PRODUKT": "l_prod",
    "MAHNVERFAHREN_SAPIST": "l_mahnv_ist", "MAHNVERFAHREN_SAPFI": "l_mahnv_fi",
    "MAHNSTUFENTYP_SAPIST": "l_mahnstyp_ist", "BEWEGART": "bwa",
}

apu.AUFB_STUFE_XTRA_MAP = {
    "ZUSAMMENFUEHRUNG": "mrg", "BEFUELLUNG": "fill",
}

# Inferring ALLOWED_SYSTEM_KENNZAHL_COMBINATIONS from the KSH script's *invalid* logic.
# This assumes any combination not explicitly marked as invalid in KSH is valid.
ALL_KENNZAHLEN = set(apu.KENNZAHL_MAP.values())
ALL_SYSTEMS = set(apu.SYSTEM_MAP.values())
_temp_allowed_combinations = set()
for sys in ALL_SYSTEMS:
    for kz in ALL_KENNZAHLEN:
        _temp_allowed_combinations.add((sys, kz))

# Apply KSH invalidation rules (from the provided KSH snippet):
# Rule 1: if [ "$System" != "nnv" -a \( "$Kennzahl" = "tvd" -o "$Kennzahl" = "lkl" \) ]
for sys in ALL_SYSTEMS:
    if sys != "nnv":
        _temp_allowed_combinations.discard((sys, "tvd"))
        _temp_allowed_combinations.discard((sys, "lkl"))

# Rule 2: elif [ "$System" = "carmen" ]
INVALID_CARMEN_KENNZAHLEN = {"twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}
for kz in INVALID_CARMEN_KENNZAHLEN:
    _temp_allowed_combinations.discard(("carmen", kz))

# Rule 3: elif [ "$System" = "sap" ]
INVALID_SAP_KENNZAHLEN = {"zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"}
for kz in INVALID_SAP_KENNZAHLEN:
    _temp_allowed_combinations.discard(("sap", kz))

# Rule 4: elif [ "$System" = "dpps" ]
INVALID_DPPS_KENNZAHLEN = {"twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}
for kz in INVALID_DPPS_KENNZAHLEN:
    _temp_allowed_combinations.discard(("dpps", kz))

# Rule 5: elif [ "$System" = "ctel" ]
VALID_CTEL_KENNZAHLEN = {"abg", "bst", "zug", "twe"}
for kz in ALL_KENNZAHLEN:
    if kz not in VALID_CTEL_KENNZAHLEN:
        _temp_allowed_combinations.discard(("ctel", kz))

# Rule 6: elif [ "$System" = "xtra" ]
VALID_XTRA_KENNZAHLEN = {"rst"}
for kz in ALL_KENNZAHLEN:
    if kz not in VALID_XTRA_KENNZAHLEN:
        _temp_allowed_combinations.discard(("xtra", kz))

# Rule 7: elif [ "$System" = "d1" ] (partial in KSH, assuming the listed are invalid)
INVALID_D1_KENNZAHLEN = {"gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn"}
for kz in INVALID_D1_KENNZAHLEN:
    _temp_allowed_combinations.discard(("d1", kz))

apu.ALLOWED_SYSTEM_KENNZAHL_COMBINATIONS = frozenset(_temp_allowed_combinations)

# Placeholder mappings for gibBereich and gibIntervall (not explicitly in KSH snippet, but needed for tests)
apu.KENNZAHL_TO_BEREICH = {
    "zug": "TEILNEHMER", "abg": "TEILNEHMER", "bst": "TEILNEHMER", "rst": "GUTHABEN",
    "auf": "GUTHABEN", "gut": "GUTHABEN", "tvd": "VERKEHR", "lkl": "VERKEHR",
    "srs": "RECHNUNG", "sgs": "RECHNUNG", "mahn": "FINANZ", "ksd": "STAMMDATEN",
    "rub": "PRODUKT", "lmo": "PRODUKT", "nnk": "NETZ", "tvk": "NETZ", "gz": "NETZ",
    "glv": "NETZ", "zonek": "NETZ", "zonet": "NETZ", "nnkt": "NETZ", "trfa": "NETZ",
    "gtyp": "NETZ", "basisd": "NETZ", "natint": "NETZ", "glint": "NETZ",
}

apu.KENNZAHL_TO_INTERVALL = {
    "zug": "t", "abg": "t", "bst": "m", "rst": "t", "auf": "t", "gut": "t",
    "tvd": "t", "lkl": "t", "srs": "t", "sgs": "t", "mahn": "m", "ksd": "t",
    "rub": "t", "lmo": "t", "nnk": "t", "tvk": "t", "gz": "t", "glv": "t",
    "zonek": "t", "zonet": "t", "nnkt": "t", "trfa": "t", "gtyp": "t",
    "basisd": "t", "natint": "t", "glint": "t",
}

# Mock datetime.now() for deterministic date calculations in konvertiereZeitspanne
MOCK_TODAY = date(2023, 10, 26)
```

---

## Test Case 1: `pruefeParameterGesetzt` - Basic Validation

**Purpose:** Verify that the `pruefeParameterGesetzt` function correctly identifies unset or empty parameters and raises the appropriate exception, mimicking the KornShell `[ -z "$param_wert" ]` check.

**Setup:** No specific setup beyond the module import.

**Action:** Call `apu.pruefeParameterGesetzt` with various string inputs.

**Pass/Fail Criterion:**
*   For valid, non-empty strings, the function should complete without raising an exception.
*   For `None`, empty strings, or strings containing only whitespace, `apu.ParameterNotSetError` should be raised.

```python
def test_pruefeParameterGesetzt():
    # Test 1.1: Valid parameter
    apu.pruefeParameterGesetzt("some_value", "TestParam") # Should pass

    # Test 1.2: Empty string
    with pytest.raises(apu.ParameterNotSetError, match="Required parameter 'EmptyParam' is not set or empty."):
        apu.pruefeParameterGesetzt("", "EmptyParam")

    # Test 1.3: String with only whitespace
    with pytest.raises(apu.ParameterNotSetError, match="Required parameter 'WhitespaceParam' is not set or empty."):
        apu.pruefeParameterGesetzt("   ", "WhitespaceParam")

    # Test 1.4: None value
    with pytest.raises(apu.ParameterNotSetError, match="Required parameter 'NoneParam' is not set or empty."):
        apu.pruefeParameterGesetzt(None, "NoneParam")

    # Test 1.5: Non-string value (Python's type check ensures this is caught)
    with pytest.raises(apu.ParameterNotSetError, match="Required parameter 'NonStringParam' is not set or empty."):
        apu.pruefeParameterGesetzt(123, "NonStringParam")
```

---

## Test Case 2: `konvertiereKennzahl` - Transformation Correctness

**Purpose:** Validate that `konvertiereKennzahl` accurately converts descriptive Kennzahl names to their short codes, handling case sensitivity and unknown values as specified. This tests output parity and transformation correctness.

**Setup:** `apu.KENNZAHL_MAP` is populated.

**Action:** Call `apu.konvertiereKennzahl` with known and unknown Kennzahl descriptions.

**Pass/Fail Criterion:**
*   For valid descriptive names (case-insensitive), the function should return the correct short code.
*   For unknown descriptive names, `apu.InvalidParameterValueError` should be raised.
*   For empty/None input, `apu.ParameterNotSetError` should be raised.

```python
def test_konvertiereKennzahl():
    # Test 2.1: Valid conversion (lowercase input)
    assert apu.konvertiereKennzahl("zugang") == "zug"

    # Test 2.2: Valid conversion (uppercase input)
    assert apu.konvertiereKennzahl("BESTAND") == "bst"

    # Test 2.3: Valid conversion (mixed case input)
    assert apu.konvertiereKennzahl("Tarifwechsel") == "twe"

    # Test 2.4: Unknown Kennzahl
    with pytest.raises(apu.InvalidParameterValueError, match="Unknown Kennzahl description: 'UNKNOWN_KZ'."):
        apu.konvertiereKennzahl("UNKNOWN_KZ")

    # Test 2.5: Empty string
    with pytest.raises(apu.ParameterNotSetError):
        apu.konvertiereKennzahl("")

    # Test 2.6: None value
    with pytest.raises(apu.ParameterNotSetError):
        apu.konvertiereKennzahl(None)
```

---

## Test Case 3: `konvertiereSystem` - Transformation Correctness

**Purpose:** Validate `konvertiereSystem` converts descriptive system names to their normalized short codes, including self-mapping systems, case sensitivity, and error handling for unknown systems.

**Setup:** `apu.SYSTEM_MAP` is populated.

**Action:** Call `apu.konvertiereSystem` with various system descriptions.

**Pass/Fail Criterion:**
*   For valid descriptive names (case-insensitive), the function should return the correct short code.
*   For unknown descriptive names, `apu.InvalidParameterValueError` should be raised.
*   For empty/None input, `apu.ParameterNotSetError` should be raised.

```python
def test_konvertiereSystem():
    # Test 3.1: Valid conversion (self-mapping, lowercase)
    assert apu.konvertiereSystem("sap") == "sap"

    # Test 3.2: Valid conversion (self-mapping, uppercase)
    assert apu.konvertiereSystem("CARMEN") == "carmen"

    # Test 3.3: Valid conversion (self-mapping, mixed case)
    assert apu.konvertiereSystem("Dpps") == "dpps"

    # Test 3.4: Unknown System
    with pytest.raises(apu.InvalidParameterValueError, match="Unknown System description: 'UNKNOWN_SYS'."):
        apu.konvertiereSystem("UNKNOWN_SYS")

    # Test 3.5: Empty string
    with pytest.raises(apu.ParameterNotSetError):
        apu.konvertiereSystem("")

    # Test 3.6: None value
    with pytest.raises(apu.ParameterNotSetError):
        apu.konvertiereSystem(None)
```

---

## Test Case 4: `pruefeSystemKennzahl` - Combination Logic

**Purpose:** Verify that `pruefeSystemKennzahl` correctly validates system-kennzahl combinations based on the complex `if/elif` logic from the legacy KornShell script. This tests transformation correctness and edge cases.

**Setup:** `apu.ALLOWED_SYSTEM_KENNZAHL_COMBINATIONS` is populated based on the KSH logic.

**Action:** Call `apu.pruefeSystemKennzahl` with various valid and invalid system/kennzahl pairs.

**Pass/Fail Criterion:**
*   For allowed combinations, the function should complete without raising an exception.
*   For disallowed combinations, `apu.InvalidCombinationError` should be raised.
*   For empty/None inputs, `apu.ParameterNotSetError` should be raised.

```python
def test_pruefeSystemKennzahl():
    # Test 4.1: Valid combination (general)
    apu.pruefeSystemKennzahl("sap", "srs") # Should pass, as 'srs' is not in SAP's invalid list

    # Test 4.2: Valid combination (specific rule: nnv with tvd)
    apu.pruefeSystemKennzahl("nnv", "tvd") # Should pass

    # Test 4.3: Valid combination (specific rule: ctel with zug)
    apu.pruefeSystemKennzahl("ctel", "zug") # Should pass

    # Test 4.4: Valid combination (specific rule: xtra with rst)
    apu.pruefeSystemKennzahl("xtra", "rst") # Should pass

    # Test 4.5: Invalid combination (general: non-nnv with tvd)
    with pytest.raises(apu.InvalidCombinationError, match="Invalid combination: System 'sap' with Kennzahl 'tvd' is not allowed."):
        apu.pruefeSystemKennzahl("sap", "tvd")

    # Test 4.6: Invalid combination (carmen with twe)
    with pytest.raises(apu.InvalidCombinationError, match="Invalid combination: System 'carmen' with Kennzahl 'twe' is not allowed."):
        apu.pruefeSystemKennzahl("carmen", "twe")

    # Test 4.7: Invalid combination (sap with zug)
    with pytest.raises(apu.InvalidCombinationError, match="Invalid combination: System 'sap' with Kennzahl 'zug' is not allowed."):
        apu.pruefeSystemKennzahl("sap", "zug")

    # Test 4.8: Invalid combination (dpps with loe)
    with pytest.raises(apu.InvalidCombinationError, match="Invalid combination: System 'dpps' with Kennzahl 'loe' is not allowed."):
        apu.pruefeSystemKennzahl("dpps", "loe")

    # Test 4.9: Invalid combination (ctel with non-allowed kennzahl, e.g., rst)
    with pytest.raises(apu.InvalidCombinationError, match="Invalid combination: System 'ctel' with Kennzahl 'rst' is not allowed."):
        apu.pruefeSystemKennzahl("ctel", "rst")

    # Test 4.10: Invalid combination (xtra with non-allowed kennzahl, e.g., zug)
    with pytest.raises(apu.InvalidCombinationError, match="Invalid combination: System 'xtra' with Kennzahl 'zug' is not allowed."):
        apu.pruefeSystemKennzahl("xtra", "zug")

    # Test 4.11: Empty system
    with pytest.raises(apu.ParameterNotSetError):
        apu.pruefeSystemKennzahl("", "zug")

    # Test 4.12: Empty kennzahl
    with pytest.raises(apu.ParameterNotSetError):
        apu.pruefeSystemKennzahl("sap", "")
```

---

## Test Case 5: `pruefeZeitraum` - Date Validation and External Dependency Replacement

**Purpose:** Ensure `pruefeZeitraum` correctly validates date formats and date order, replacing the `DWDate_Datum_Check` and `DWDate_Datum_LE` external utilities with Python's `datetime` functionality. This tests external-system replacements and transformation correctness.

**Setup:** No specific setup beyond the module import.

**Action:** Call `apu.pruefeZeitraum` with various date strings.

**Pass/Fail Criterion:**
*   For valid `YYYYMMDD` dates where `anfang <= ende`, the function should complete without error.
*   For invalid date formats, `apu.InvalidDateError` should be raised.
*   For `anfang > ende`, `apu.InvalidDateRangeError` should be raised.
*   For empty/None inputs, `apu.ParameterNotSetError` should be raised.

```python
def test_pruefeZeitraum():
    # Test 5.1: Valid date range
    apu.pruefeZeitraum("20230101", "20230105") # Should pass

    # Test 5.2: Start date equals end date
    apu.pruefeZeitraum("20230315", "20230315") # Should pass

    # Test 5.3: Invalid start date format
    with pytest.raises(apu.InvalidDateError, match="Date parameter 'Anfang \\(start date\\)' has invalid format '2023-01-01'."):
        apu.pruefeZeitraum("2023-01-01", "20230105")

    # Test 5.4: Invalid end date format
    with pytest.raises(apu.InvalidDateError, match="Date parameter 'Ende \\(end date\\)' has invalid format '20231301'."):
        apu.pruefeZeitraum("20230101", "20231301")

    # Test 5.5: Start date after end date
    with pytest.raises(apu.InvalidDateRangeError, match="Start date '20230105' cannot be after end date '20230101'."):
        apu.pruefeZeitraum("20230105", "20230101")

    # Test 5.6: Empty start date
    with pytest.raises(apu.ParameterNotSetError, match="Date parameter 'Anfang \\(start date\\)' is not set or empty."):
        apu.pruefeZeitraum("", "20230105")

    # Test 5.7: None end date
    with pytest.raises(apu.ParameterNotSetError, match="Date parameter 'Ende \\(end date\\)' is not set or empty."):
        apu.pruefeZeitraum("20230101", None)
```

---

## Test Case 6: `pruefeZahlPositiv` - Numeric Validation

**Purpose:** Confirm `pruefeZahlPositiv` correctly checks if a string represents a positive number (>= 0), mimicking KornShell's numeric checks. This tests transformation correctness and type handling.

**Setup:** No specific setup beyond the module import.

**Action:** Call `apu.pruefeZahlPositiv` with various numeric and non-numeric string inputs.

**Pass/Fail Criterion:**
*   For valid positive numeric strings (integers or floats, including "0"), the function should return the converted number.
*   For negative numeric strings, `apu.InvalidParameterValueError` should be raised.
*   For non-numeric strings, `apu.InvalidParameterValueError` should be raised.
*   For empty/None input, `apu.ParameterNotSetError` should be raised.

```python
def test_pruefeZahlPositiv():
    # Test 6.1: Valid positive integer
    assert apu.pruefeZahlPositiv("100", "Offset") == 100

    # Test 6.2: Valid zero
    assert apu.pruefeZahlPositiv("0", "Offset") == 0

    # Test 6.3: Valid positive float
    assert apu.pruefeZahlPositiv("10.5", "Offset") == 10.5

    # Test 6.4: Negative number
    with pytest.raises(apu.InvalidParameterValueError, match="Parameter 'Offset' must be a positive number, but got '-5'."):
        apu.pruefeZahlPositiv("-5", "Offset")

    # Test 6.5: Non-numeric string
    with pytest.raises(apu.InvalidParameterValueError, match="Parameter 'Offset' must be a valid number, but got 'abc'."):
        apu.pruefeZahlPositiv("abc", "Offset")

    # Test 6.6: Empty string
    with pytest.raises(apu.ParameterNotSetError):
        apu.pruefeZahlPositiv("", "Offset")

    # Test 6.7: None value
    with pytest.raises(apu.ParameterNotSetError):
        apu.pruefeZahlPositiv(None, "Offset")
```

---

## Test Case 7: `pruefeZeitParameter` - Conditional Date/Offset Validation

**Purpose:** Verify `pruefeZeitParameter` correctly implements the conditional logic for validating either a date range or a time offset, but not both, and handles missing parameters.

**Setup:** No specific setup beyond the module import.

**Action:** Call `apu.pruefeZeitParameter` with various combinations of `anfang`, `ende`, and `zeitoffset`.

**Pass/Fail Criterion:**
*   For a valid date range (and no offset), the function should pass.
*   For a valid offset (and no dates), the function should pass.
*   If both date range and offset are provided, `apu.InvalidParameterValueError` should be raised.
*   If neither date range nor offset is provided, `apu.ParameterNotSetError` should be raised.
*   If only one date of the range is provided, `apu.ParameterNotSetError` should be raised.
*   Invalid date formats or non-positive offsets should raise their respective exceptions.

```python
def test_pruefeZeitParameter():
    # Test 7.1: Valid date range, no offset
    apu.pruefeZeitParameter("20230101", "20230105", None) # Should pass

    # Test 7.2: Valid offset, no dates
    apu.pruefeZeitParameter(None, None, "10") # Should pass

    # Test 7.3: Both date range and offset provided
    with pytest.raises(apu.InvalidParameterValueError, match="Cannot provide both a date range \\(Anfang/Ende\\) and a time offset \\(Zeitoffset\\)."):
        apu.pruefeZeitParameter("20230101", "20230105", "10")

    # Test 7.4: Neither date range nor offset provided
    with pytest.raises(apu.ParameterNotSetError, match="Either a date range \\(Anfang/Ende\\) or a time offset \\(Zeitoffset\\) must be provided."):
        apu.pruefeZeitParameter(None, None, None)

    # Test 7.5: Only start date provided
    with pytest.raises(apu.ParameterNotSetError, match="If providing a date range, both 'Anfang' and 'Ende' must be set."):
        apu.pruefeZeitParameter("20230101", None, None)

    # Test 7.6: Invalid date format in range
    with pytest.raises(apu.InvalidDateError):
        apu.pruefeZeitParameter("2023-01-01", "20230105", None)

    # Test 7.7: Invalid offset (negative)
    with pytest.raises(apu.InvalidParameterValueError, match="Parameter 'Zeitoffset' must be a positive number, but got '-5'."):
        apu.pruefeZeitParameter(None, None, "-5")

    # Test 7.8: Invalid offset (non-numeric)
    with pytest.raises(apu.InvalidParameterValueError, match="Parameter 'Zeitoffset' must be a valid number, but got 'abc'."):
        apu.pruefeZeitParameter(None, None, "abc")
```

---

## Test Case 8: `konvertiereZeitspanne` - Date Arithmetic and External Dependency Replacement

**Purpose:** Validate that `konvertiereZeitspanne` correctly calculates start and end dates based on a span and Kennzahl, replacing the `DWDate_Gib_Zeitraum` external utility. Special attention is paid to month arithmetic edge cases. This tests external-system replacements, transformation correctness, and edge cases.

**Setup:**
*   `apu.KENNZAHL_TO_INTERVALL` is populated.
*   `datetime.now()` is mocked to a fixed date (`MOCK_TODAY`) for deterministic results.

**Action:** Call `apu.konvertiereZeitspanne` with various span and Kennzahl inputs.

**Pass/Fail Criterion:**
*   For valid daily spans, the function should return the correct `(start_date, end_date)` tuple.
*   For valid monthly spans, the function should return the correct `(start_date, end_date)` tuple, correctly handling month-end adjustments (e.g., 20231031 - 1 month -> 20230930).
*   For zero or negative spans, `apu.InvalidParameterValueError` should be raised.
*   For non-numeric spans, `apu.InvalidParameterValueError` should be raised.
*   For unknown Kennzahlen (leading to unknown interval unit), `apu.InvalidParameterValueError` should be raised.

```python
@patch('alis_parameter_utils.datetime')
def test_konvertiereZeitspanne(mock_dt):
    mock_dt.now.return_value = datetime(MOCK_TODAY.year, MOCK_TODAY.month, MOCK_TODAY.day)
    mock_dt.strptime = datetime.strptime # Use actual strptime
    mock_dt.date = date # Use actual date
    mock_dt.timedelta = timedelta # Use actual timedelta

    # Test 8.1: Daily span (Kennzahl 'zug' maps to 't')
    start_date_str, end_date_str = apu.konvertiereZeitspanne("7", "zug")
    expected_start = (MOCK_TODAY - timedelta(days=7)).strftime(apu.DATE_FORMAT)
    expected_end = MOCK_TODAY.strftime(apu.DATE_FORMAT)
    assert start_date_str == expected_start
    assert end_date_str == expected_end

    # Test 8.2: Monthly span (Kennzahl 'bst' maps to 'm')
    # MOCK_TODAY is 2023-10-26. Subtract 3 months.
    # 2023-10-26 - 3 months = 2023-07-26
    start_date_str, end_date_str = apu.konvertiereZeitspanne("3", "bst")
    expected_start = date(2023, 7, 26).strftime(apu.DATE_FORMAT)
    expected_end = MOCK_TODAY.strftime(apu.DATE_FORMAT)
    assert start_date_str == expected_start
    assert end_date_str == expected_end

    # Test 8.3: Monthly span - month-end edge case (e.g., 2023-03-31 - 1 month = 2023-02-28)
    mock_dt.now.return_value = datetime(2023, 3, 31)
    start_date_str, end_date_str = apu.konvertiereZeitspanne("1", "bst")
    expected_start = date(2023, 2, 28).strftime(apu.DATE_FORMAT)
    expected_end = date(2023, 3, 31).strftime(apu.DATE_FORMAT)
    assert start_date_str == expected_start
    assert end_date_str == expected_end

    # Test 8.4: Monthly span - month-end edge case (e.g., 2024-03-31 - 1 month = 2024-02-29 for leap year)
    mock_dt.now.return_value = datetime(2024, 3, 31)
    start_date_str, end_date_str = apu.konvertiereZeitspanne("1", "bst")
    expected_start = date(2024, 2, 29).strftime(apu.DATE_FORMAT)
    expected_end = date(2024, 3, 31).strftime(apu.DATE_FORMAT)
    assert start_date_str == expected_start
    assert end_date_str == expected_end

    # Test 8.5: Zero span
    with pytest.raises(apu.InvalidParameterValueError, match="Zeitspanne must be greater than 0."):
        apu.konvertiereZeitspanne("0", "zug")

    # Test 8.6: Negative span (caught by pruefeZahlPositiv)
    with pytest.raises(apu.InvalidParameterValueError, match="Parameter 'Zeitspanne' must be a positive number, but got '-5'."):
        apu.konvertiereZeitspanne("-5", "zug")

    # Test 8.7: Non-numeric span (caught by pruefeZahlPositiv)
    with pytest.raises(apu.InvalidParameterValueError, match="Parameter 'Zeitspanne' must be a valid number, but got 'abc'."):
        apu.konvertiereZeitspanne("abc", "zug")

    # Test 8.8: Unknown Kennzahl (caught by gibIntervall)
    with pytest.raises(apu.InvalidParameterValueError, match="Unknown Kennzahl 'UNKNOWN_KZ' for Intervall determination."):
        apu.konvertiereZeitspanne("1", "UNKNOWN_KZ")
```

---

## Test Case 9: `gibBereich` and `gibIntervall` - Lookup Correctness

**Purpose:** Verify that `gibBereich` and `gibIntervall` correctly retrieve the associated area and interval codes for a given Kennzahl, based on their respective mappings. This tests output parity and transformation correctness.

**Setup:** `apu.KENNZAHL_TO_BEREICH` and `apu.KENNZAHL_TO_INTERVALL` are populated.

**Action:** Call the functions with known and unknown Kennzahl inputs.

**Pass/Fail Criterion:**
*   For valid Kennzahlen, the functions should return the correct associated code.
*   For unknown Kennzahlen, `apu.InvalidParameterValueError` should be raised.
*   For empty/None input, `apu.ParameterNotSetError` should be raised.

```python
def test_gibBereich_gibIntervall():
    # Test 9.1: gibBereich - Valid Kennzahl
    assert apu.gibBereich("zug") == "TEILNEHMER"
    assert apu.gibBereich("bst") == "TEILNEHMER"
    assert apu.gibBereich("mahn") == "FINANZ"

    # Test 9.2: gibBereich - Unknown Kennzahl
    with pytest.raises(apu.InvalidParameterValueError, match="Unknown Kennzahl 'UNKNOWN_KZ' for Bereich determination."):
        apu.gibBereich("UNKNOWN_KZ")

    # Test 9.3: gibBereich - Empty Kennzahl
    with pytest.raises(apu.ParameterNotSetError):
        apu.gibBereich("")

    # Test 9.4: gibIntervall - Valid Kennzahl
    assert apu.gibIntervall("zug") == "t"
    assert apu.gibIntervall("bst") == "m"
    assert apu.gibIntervall("mahn") == "m"

    # Test 9.5: gibIntervall - Unknown Kennzahl
    with pytest.raises(apu.InvalidParameterValueError, match="Unknown Kennzahl 'UNKNOWN_KZ' for Intervall determination."):
        apu.gibIntervall("UNKNOWN_KZ")

    # Test 9.6: gibIntervall - Empty Kennzahl
    with pytest.raises(apu.ParameterNotSetError):
        apu.gibIntervall("")
```