As a senior data-migration QA engineer, I've reviewed the migration design for `h_alis_parameter.ksh` and the generated code. The migration strategy involves a hybrid approach, leveraging BigQuery UDFs for stateless transformations and a Python module for more complex logic, especially date handling.

The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, and handling of internal dependencies. Given the nature of the script as a utility library, traditional data quality, row-count, and schema assertions are not directly applicable; instead, correctness of function outputs and error handling are paramount.

---

## Migration Validation Tests: `h_alis_parameter.ksh`

### 1. Output Parity & Transformation Correctness - BigQuery SQL UDFs

These tests validate the BigQuery SQL UDFs, focusing on their ability to produce the same output or error conditions as the original KornShell script's logic for various inputs.

#### Test Case 1.1: `pruefe_parameter_gesetzt` - Basic Validation

*   **Purpose**: Verify that the `pruefe_parameter_gesetzt` UDF correctly identifies whether a parameter value is set (non-NULL, non-empty, non-whitespace).
*   **Setup**: Ensure the `pruefe_parameter_gesetzt` UDF is deployed to `your_project_id.utility_functions`.
*   **Action**: Execute the UDF with various string inputs.
*   **Pass/Fail Criterion**: The UDF returns the expected `STRUCT<is_valid BOOL, error_message STRING>` based on the input.

```sql
-- Test 1.1.1: Valid parameter
SELECT `your_project_id.utility_functions`.pruefe_parameter_gesetzt('some_value');
-- Expected: {is_valid: TRUE, error_message: ''}

-- Test 1.1.2: NULL parameter
SELECT `your_project_id.utility_functions`.pruefe_parameter_gesetzt(NULL);
-- Expected: {is_valid: FALSE, error_message: 'Parameter is not set or is empty.'}

-- Test 1.1.3: Empty string parameter
SELECT `your_project_id.utility_functions`.pruefe_parameter_gesetzt('');
-- Expected: {is_valid: FALSE, error_message: 'Parameter is not set or is empty.'}

-- Test 1.1.4: Whitespace-only parameter
SELECT `your_project_id.utility_functions`.pruefe_parameter_gesetzt('   ');
-- Expected: {is_valid: FALSE, error_message: 'Parameter is not set or is empty.'}
```

#### Test Case 1.2: `konvertiere_kennzahl` - Key Figure Conversion

*   **Purpose**: Validate that `konvertiere_kennzahl` correctly converts descriptive key figure names to their standardized short codes, handling case insensitivity and unknown values.
*   **Setup**: Ensure the `konvertiere_kennzahl` UDF is deployed. The UDF should contain all mappings from the original KornShell `case` statement.
*   **Action**: Call the UDF with known and unknown key figure descriptions.
*   **Pass/Fail Criterion**: For known inputs, the UDF returns the correct short code. For unknown inputs, the UDF raises an `ERROR()` with a descriptive message.

```sql
-- Test 1.2.1: Known key figure (lowercase)
SELECT `your_project_id.utility_functions`.konvertiere_kennzahl('zugang');
-- Expected: 'zug'

-- Test 1.2.2: Known key figure (uppercase/mixed case)
SELECT `your_project_id.utility_functions`.konvertiere_kennzahl('ABGANG');
-- Expected: 'abg'

-- Test 1.2.3: Another known key figure
SELECT `your_project_id.utility_functions`.konvertiere_kennzahl('glaengenintervall');
-- Expected: 'glint'

-- Test 1.2.4: Unknown key figure
SELECT `your_project_id.utility_functions`.konvertiere_kennzahl('unbekannte_kennzahl');
-- Expected: Query fails with an ERROR: "Invalid Kennzahl description: unbekannte_kennzahl"
```

#### Test Case 1.3: `konvertiere_system` - System Name Conversion

*   **Purpose**: Validate that `konvertiere_system` correctly converts descriptive system names to their standardized short codes, handling case insensitivity and unknown values.
*   **Setup**: Ensure the `konvertiere_system` UDF is deployed with all mappings.
*   **Action**: Call the UDF with known and unknown system descriptions.
*   **Pass/Fail Criterion**: For known inputs, the UDF returns the correct short code. For unknown inputs, the UDF raises an `ERROR()`.

```sql
-- Test 1.3.1: Known system (lowercase)
SELECT `your_project_id.utility_functions`.konvertiere_system('sap');
-- Expected: 'sap'

-- Test 1.3.2: Known system (mixed case)
SELECT `your_project_id.utility_functions`.konvertiere_system('Carmen');
-- Expected: 'carmen'

-- Test 1.3.3: Unknown system
SELECT `your_project_id.utility_functions`.konvertiere_system('unknown_source');
-- Expected: Query fails with an ERROR: "Invalid System description: unknown_source"
```

#### Test Case 1.4: `konvertiere_sdname` - Master Data System Name Conversion

*   **Purpose**: Validate that `konvertiere_sdname` correctly converts descriptive master data system names to short codes.
*   **Setup**: Ensure the `konvertiere_sdname` UDF is deployed with all mappings.
*   **Action**: Call the UDF with known and unknown SDName descriptions.
*   **Pass/Fail Criterion**: For known inputs, the UDF returns the correct short code. For unknown inputs, the UDF raises an `ERROR()`.

```sql
-- Test 1.4.1: Known SDName
SELECT `your_project_id.utility_functions`.konvertiere_sdname('rahmenvertrag');
-- Expected: 'rv'

-- Test 1.4.2: Another known SDName
SELECT `your_project_id.utility_functions`.konvertiere_sdname('produkt');
-- Expected: 'l_prod'

-- Test 1.4.3: Unknown SDName
SELECT `your_project_id.utility_functions`.konvertiere_sdname('unbekanntes_sd');
-- Expected: Query fails with an ERROR: "Invalid SDName description: unbekanntes_sd"
```

#### Test Case 1.5: `konvertiere_aufbstufextra` - Xtra Preparation Stage Conversion

*   **Purpose**: Validate that `konvertiere_aufbstufextra` correctly converts Xtra preparation stage names to short codes.
*   **Setup**: Ensure the `konvertiere_aufbstufextra` UDF is deployed with all mappings.
*   **Action**: Call the UDF with known and unknown stage descriptions.
*   **Pass/Fail Criterion**: For known inputs, the UDF returns the correct short code. For unknown inputs, the UDF raises an `ERROR()`.

```sql
-- Test 1.5.1: Known stage
SELECT `your_project_id.utility_functions`.konvertiere_aufbstufextra('zusammenfuehrung');
-- Expected: 'mrg'

-- Test 1.5.2: Another known stage
SELECT `your_project_id.utility_functions`.konvertiere_aufbstufextra('befuellung');
-- Expected: 'fill'

-- Test 1.5.3: Unknown stage
SELECT `your_project_id.utility_functions`.konvertiere_aufbstufextra('unbekannte_stufe');
-- Expected: Query fails with an ERROR: "Invalid AufbStufeXtra description: unbekannte_stufe"
```

#### Test Case 1.6: `pruefe_system_kennzahl` - System-Key Figure Combination Validation

*   **Purpose**: Verify that `pruefe_system_kennzahl` correctly validates combinations of system and key figure based on the complex business rules from the original KornShell script. This is a critical test for transformation correctness.
*   **Setup**: Ensure the `pruefe_system_kennzahl` UDF is deployed with the complete `if/elif` logic translated into BigQuery SQL `CASE` statements.
*   **Action**: Execute the UDF with a comprehensive set of `(system, kennzahl)` pairs, covering valid, explicitly invalid, and edge cases from the original script.
*   **Pass/Fail Criterion**: The UDF returns `STRUCT(TRUE, '')` for valid combinations and `STRUCT(FALSE, 'error_message')` for invalid ones, with the error message matching the expected output.

```sql
-- Test 1.6.1: Valid combination (e.g., SAP and zug)
SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('sap', 'zug');
-- Expected: {is_valid: TRUE, error_message: ''}

-- Test 1.6.2: Invalid combination (e.g., Carmen and twe)
SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('carmen', 'twe');
-- Expected: {is_valid: FALSE, error_message: 'Ungueltige Kombination carmen twe'}

-- Test 1.6.3: Another invalid combination (e.g., SAP and tvd)
SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('sap', 'tvd');
-- Expected: {is_valid: FALSE, error_message: 'Ungueltige Kombination sap tvd'}

-- Test 1.6.4: Valid combination for CTEL
SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('ctel', 'twe');
-- Expected: {is_valid: TRUE, error_message: ''}

-- Test 1.6.5: Invalid combination for CTEL
SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('ctel', 'tvd');
-- Expected: {is_valid: FALSE, error_message: 'Ungueltige Kombination ctel tvd'}

-- Test 1.6.6: NNV system with tvd (should be invalid based on original logic: `if [ "$System" != "nnv" -a \( "$Kennzahl" = "tvd" -o "$Kennzahl" = "lkl" \) ]`)
SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('nnv', 'tvd');
-- Expected: {is_valid: TRUE, error_message: ''} (because the condition `"$System" != "nnv"` is false)

-- Test 1.6.7: Non-NNV system with tvd (should be invalid)
SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('d1', 'tvd');
-- Expected: {is_valid: FALSE, error_message: 'Ungueltige Kombination d1 tvd'}

-- Test 1.6.8: Empty system or kennzahl (should be handled by the UDF's internal logic or a wrapper)
-- Assuming the UDF handles this as an invalid combination or an explicit error.
SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('', 'zug');
-- Expected: {is_valid: FALSE, error_message: 'Invalid combination of system "" and kennzahl "zug".'} (or similar)
```

#### Test Case 1.7: `gib_bereich` - Determine Area/Group

*   **Purpose**: Verify that `gib_bereich` correctly determines the "Bereich" (area/group) based on the input `kennzahl`.
*   **Setup**: Ensure the `gib_bereich` UDF is deployed with all hardcoded lists translated into `IN` clauses or arrays.
*   **Action**: Call the UDF with various `kennzahl` values.
*   **Pass/Fail Criterion**: For known inputs, the UDF returns the correct "Bereich". For unknown inputs, the UDF raises an `ERROR()`.

```sql
-- Test 1.7.1: Kennzahl in TN_BEREICH list
SELECT `your_project_id.utility_functions`.gib_bereich('zug');
-- Expected: 'TN_BEREICH'

-- Test 1.7.2: Kennzahl in GD_BEREICH list
SELECT `your_project_id.utility_functions`.gib_bereich('glint');
-- Expected: 'GD_BEREICH'

-- Test 1.7.3: Unknown Kennzahl
SELECT `your_project_id.utility_functions`.gib_bereich('unbekannt');
-- Expected: Query fails with an ERROR: "Could not determine Bereich for Kennzahl: unbekannt"
```

#### Test Case 1.8: `gib_intervall` - Determine Interval Type

*   **Purpose**: Verify that `gib_intervall` correctly determines the interval type ('t' for daily, 'm' for monthly) based on the input `kennzahl`.
*   **Setup**: Ensure the `gib_intervall` UDF is deployed with all hardcoded lists.
*   **Action**: Call the UDF with various `kennzahl` values.
*   **Pass/Fail Criterion**: For known inputs, the UDF returns the correct interval type. For unknown inputs, the UDF raises an `ERROR()`.

```sql
-- Test 1.8.1: Kennzahl for daily interval
SELECT `your_project_id.utility_functions`.gib_intervall('zug');
-- Expected: 't'

-- Test 1.8.2: Kennzahl for monthly interval
SELECT `your_project_id.utility_functions`.gib_intervall('bst');
-- Expected: 'm'

-- Test 1.8.3: Unknown Kennzahl
SELECT `your_project_id.utility_functions`.gib_intervall('unbekannt');
-- Expected: Query fails with an ERROR: "Could not determine Intervall for Kennzahl: unbekannt"
```

#### Test Case 1.9: `pruefe_zahl_positiv` - Numeric and Positive Validation (BigQuery)

*   **Purpose**: Verify that the BigQuery `pruefe_zahl_positiv` UDF correctly identifies if a string represents a non-negative number.
*   **Setup**: Ensure the `pruefe_zahl_positiv` UDF is deployed.
*   **Action**: Execute the UDF with various string inputs (positive, zero, negative, non-numeric, empty).
*   **Pass/Fail Criterion**: The UDF returns the expected `STRUCT<is_valid BOOL, error_message STRING>`.

```sql
-- Test 1.9.1: Positive number
SELECT `your_project_id.utility_functions`.pruefe_zahl_positiv('123');
-- Expected: {is_valid: TRUE, error_message: ''}

-- Test 1.9.2: Zero
SELECT `your_project_id.utility_functions`.pruefe_zahl_positiv('0');
-- Expected: {is_valid: TRUE, error_message: ''}

-- Test 1.9.3: Negative number
SELECT `your_project_id.utility_functions`.pruefe_zahl_positiv('-5');
-- Expected: {is_valid: FALSE, error_message: 'Value is negative.'}

-- Test 1.9.4: Non-numeric string
SELECT `your_project_id.utility_functions`.pruefe_zahl_positiv('abc');
-- Expected: {is_valid: FALSE, error_message: 'Value is not a valid number.'}

-- Test 1.9.5: Empty string
SELECT `your_project_id.utility_functions`.pruefe_zahl_positiv('');
-- Expected: {is_valid: FALSE, error_message: 'Value is not a valid number.'}

-- Test 1.9.6: Number with decimal
SELECT `your_project_id.utility_functions`.pruefe_zahl_positiv('10.5');
-- Expected: {is_valid: TRUE, error_message: ''}
```

---

### 2. Output Parity & Transformation Correctness - Python Module

These tests validate the Python functions in `src/python/h_alis_parameter/date_utils.py`, ensuring they replicate the behavior of the original KornShell logic and its external `DWDate_` dependencies.

#### Test Case 2.1: `is_valid_date_format` (DWDate_Datum_Check replacement)

*   **Purpose**: Verify that `is_valid_date_format` correctly checks if a date string matches the specified format.
*   **Setup**: Ensure `date_utils.py` is accessible in the Python environment.
*   **Action**: Call the function with various date strings and formats.
*   **Pass/Fail Criterion**: The function returns `True` for valid dates/formats and `False` otherwise.

```python
# pytest code for date_utils.py
from src.python.h_alis_parameter.date_utils import is_valid_date_format

def test_is_valid_date_format():
    # Valid cases
    assert is_valid_date_format('20231026', '%Y%m%d') == True
    assert is_valid_date_format('26.10.2023', '%d.%m.%Y') == True
    # Invalid format
    assert is_valid_date_format('2023-10-26', '%Y%m%d') == False
    # Invalid date values
    assert is_valid_date_format('20230230', '%Y%m%d') == False # Feb 30th
    assert is_valid_date_format('20231301', '%Y%m%d') == False # Invalid month
    # Empty/None
    assert is_valid_date_format('', '%Y%m%d') == False
    assert is_valid_date_format(None, '%Y%m%d') == False # Python handles None, shell empty string
```

#### Test Case 2.2: `is_date1_le_date2` (DWDate_Datum_LE replacement)

*   **Purpose**: Verify that `is_date1_le_date2` correctly compares two date strings.
*   **Setup**: Ensure `date_utils.py` is accessible.
*   **Action**: Call the function with various date pairs.
*   **Pass/Fail Criterion**: The function returns `True` if `date1 <= date2` and `False` otherwise, including cases with invalid date formats.

```python
# pytest code for date_utils.py
from src.python.h_alis_parameter.date_utils import is_date1_le_date2

def test_is_date1_le_date2():
    # date1 < date2
    assert is_date1_le_date2('20231025', '20231026') == True
    # date1 = date2
    assert is_date1_le_date2('20231026', '20231026') == True
    # date1 > date2
    assert is_date1_le_date2('20231027', '20231026') == False
    # Invalid date format for one
    assert is_date1_le_date2('2023-10-26', '20231026') == False
    # Invalid date values
    assert is_date1_le_date2('20230230', '20230301') == False
    # Empty/None
    assert is_date1_le_date2('', '20231026') == False
    assert is_date1_le_date2('20231026', None) == False
```

#### Test Case 2.3: `get_date_range_from_span` (DWDate_Gib_Zeitraum replacement)

*   **Purpose**: Verify that `get_date_range_from_span` correctly calculates start and end dates based on a span and unit.
*   **Setup**: Ensure `date_utils.py` is accessible.
*   **Action**: Call the function with various `span_value`, `unit`, and `reference_date_str` combinations.
*   **Pass/Fail Criterion**: The function returns the correct `(start_date_str, end_date_str)` tuple or raises a `ValueError` for invalid inputs.

```python
# pytest code for date_utils.py
from src.python.h_alis_parameter.date_utils import get_date_range_from_span
import pytest
import datetime

def test_get_date_range_from_span_daily():
    # 7 days span, reference today (2023-10-26) -> 2023-10-20 to 2023-10-26
    # Mock datetime.date.today() for consistent testing
    with pytest.MonkeyPatch().context() as m:
        m.setattr(datetime.date, 'today', lambda: datetime.date(2023, 10, 26))
        assert get_date_range_from_span(7, 'D') == ('20231020', '20231026')
        assert get_date_range_from_span(1, 'D', '20230115') == ('20230115', '20230115')
        assert get_date_range_from_span(30, 'D', '20230315') == ('20230214', '20230315')

def test_get_date_range_from_span_monthly():
    # 3 months span, reference today (2023-10-26) -> 2023-08-01 to 2023-10-31
    with pytest.MonkeyPatch().context() as m:
        m.setattr(datetime.date, 'today', lambda: datetime.date(2023, 10, 26))
        assert get_date_range_from_span(3, 'M') == ('20230801', '20231031')
        assert get_date_range_from_span(1, 'M', '20230115') == ('20230101', '20230131')
        assert get_date_range_from_span(2, 'M', '20230315') == ('20230201', '20230331')
        assert get_date_range_from_span(1, 'M', '20230215') == ('20230201', '20230228') # Leap year not considered in this example, but should be handled by relativedelta

def test_get_date_range_from_span_invalid_inputs():
    with pytest.raises(ValueError, match="Span value must be positive."):
        get_date_range_from_span(0, 'D')
    with pytest.raises(ValueError, match="Invalid unit for span."):
        get_date_range_from_span(1, 'X')
    with pytest.raises(ValueError, match="Invalid reference date format."):
        get_date_range_from_span(1, 'D', '2023-10-26')
```

#### Test Case 2.4: `pruefeZahlPositiv` - Numeric and Positive Validation (Python)

*   **Purpose**: Verify that the Python `pruefeZahlPositiv` function correctly identifies if a string represents a non-negative number.
*   **Setup**: Ensure `date_utils.py` is accessible.
*   **Action**: Call the function with various string inputs.
*   **Pass/Fail Criterion**: The function returns `True` for non-negative numeric strings and `False` otherwise.

```python
# pytest code for date_utils.py
from src.python.h_alis_parameter.date_utils import pruefeZahlPositiv

def test_pruefe_zahl_positiv():
    assert pruefeZahlPositiv('100', 'param') == True
    assert pruefeZahlPositiv('0', 'param') == True
    assert pruefeZahlPositiv('10.5', 'param') == True
    assert pruefeZahlPositiv('-10', 'param') == False
    assert pruefeZahlPositiv('abc', 'param') == False
    assert pruefeZahlPositiv('', 'param') == False
    assert pruefeZahlPositiv(None, 'param') == False
    assert pruefeZahlPositiv('   5   ', 'param') == True # Whitespace should be handled by float()
```

#### Test Case 2.5: `pruefeZeitraum` - Date Range Validation

*   **Purpose**: Verify that `pruefeZeitraum` correctly validates if a start date is less than or equal to an end date.
*   **Setup**: Ensure `date_utils.py` is accessible.
*   **Action**: Call the function with various start/end date string pairs.
*   **Pass/Fail Criterion**: The function returns `True` for valid ranges and `False` for invalid ranges or invalid date formats.

```python
# pytest code for date_utils.py
from src.python.h_alis_parameter.date_utils import pruefeZeitraum

def test_pruefe_zeitraum():
    # Valid ranges
    assert pruefeZeitraum('20230101', '20230101') == True
    assert pruefeZeitraum('20230101', '20230131') == True
    assert pruefeZeitraum('20230101', '20240101') == True
    # Invalid ranges (start > end)
    assert pruefeZeitraum('20230131', '20230101') == False
    # Invalid date formats
    assert pruefeZeitraum('2023-01-01', '20230131') == False
    assert pruefeZeitraum('20230101', '2023-01-31') == False
    # Empty/None
    assert pruefeZeitraum('', '20230131') == False
    assert pruefeZeitraum('20230101', None) == False
```

#### Test Case 2.6: `pruefeZeitParameter` - Date/Offset Parameter Validation

*   **Purpose**: Verify that `pruefeZeitParameter` correctly validates combinations of start date, end date, and time offset, ensuring mutual exclusivity and validity.
*   **Setup**: Ensure `date_utils.py` is accessible.
*   **Action**: Call the function with various combinations of `p_anfangsdatum`, `p_endedatum`, and `p_zeitoffset`.
*   **Pass/Fail Criterion**: The function returns `(True, '')` for valid combinations and `(False, 'error_message')` for invalid ones.

```python
# pytest code for date_utils.py
from src.python.h_alis_parameter.date_utils import pruefeZeitParameter

def test_pruefe_zeit_parameter():
    # Valid: only dates
    assert pruefeZeitParameter('20230101', '20230131', None) == (True, '')
    # Valid: only offset
    assert pruefeZeitParameter(None, None, '10') == (True, '')

    # Invalid: dates and offset
    assert pruefeZeitParameter('20230101', '20230131', '10') == (False, 'Error: Cannot provide both dates and time offset.')
    assert pruefeZeitParameter('20230101', None, '10') == (False, 'Error: Cannot provide time offset with start or end date.')
    assert pruefeZeitParameter(None, '20230131', '10') == (False, 'Error: Cannot provide time offset with start or end date.')

    # Invalid: only start date
    assert pruefeZeitParameter('20230101', None, None) == (False, 'Error: Either start/end dates or time offset must be provided.')
    # Invalid: only end date
    assert pruefeZeitParameter(None, '20230131', None) == (False, 'Error: Either start/end dates or time offset must be provided.')
    # Invalid: no parameters
    assert pruefeZeitParameter(None, None, None) == (False, 'Error: Either start/end dates or time offset must be provided.')

    # Invalid: invalid date range
    assert pruefeZeitParameter('20230131', '20230101', None) == (False, 'Error: Invalid date range (start date after end date or invalid format).')
    # Invalid: invalid date format
    assert pruefeZeitParameter('2023-01-01', '20230131', None) == (False, 'Error: Invalid date range (start date after end date or invalid format).')

    # Invalid: non-positive offset
    assert pruefeZeitParameter(None, None, '-5') == (False, 'Error: Time offset must be a positive number.')
    assert pruefeZeitParameter(None, None, 'abc') == (False, 'Error: Time offset must be a positive number.')
```

#### Test Case 2.7: `konvertiereZeitspanne` - Convert Span to Dates

*   **Purpose**: Verify that `konvertiereZeitspanne` correctly calculates start and end dates based on a numeric span and `kennzahl` (which determines the unit, 'D' or 'M').
*   **Setup**: Ensure `date_utils.py` is accessible.
*   **Action**: Call the function with various `p_spanne` and `p_kennzahl` values.
*   **Pass/Fail Criterion**: The function returns the correct `(start_date_str, end_date_str)` tuple or `(None, None)` for invalid inputs.

```python
# pytest code for date_utils.py
from src.python.h_alis_parameter.date_utils import konvertiereZeitspanne
import pytest
import datetime

def test_konvertiere_zeitspanne_daily():
    # Mock today's date for consistent results
    with pytest.MonkeyPatch().context() as m:
        m.setattr(datetime.date, 'today', lambda: datetime.date(2023, 10, 26))
        # Default unit 'D' for non-'bst' kennzahl
        assert konvertiereZeitspanne('7', 'zug') == ('20231020', '20231026')
        assert konvertiereZeitspanne('1', 'abg') == ('20231026', '20231026')

def test_konvertiere_zeitspanne_monthly():
    with pytest.MonkeyPatch().context() as m:
        m.setattr(datetime.date, 'today', lambda: datetime.date(2023, 10, 26))
        # Unit 'M' for 'bst' kennzahl
        assert konvertiereZeitspanne('3', 'bst') == ('20230801', '20231031')
        assert konvertiereZeitspanne('1', 'bst') == ('20231001', '20231031')
        assert konvertiereZeitspanne('2', 'bst', date_format='%Y%m%d') == ('20230901', '20231031')

def test_konvertiere_zeitspanne_invalid_span():
    assert konvertiereZeitspanne('-5', 'zug') == (None, None)
    assert konvertiereZeitspanne('abc', 'bst') == (None, None)
    assert konvertiereZeitspanne('', 'zug') == (None, None)
    assert konvertiereZeitspanne(None, 'zug') == (None, None)

def test_konvertiere_zeitspanne_unknown_kennzahl():
    # Unknown kennzahl should default to 'D' unit
    with pytest.MonkeyPatch().context() as m:
        m.setattr(datetime.date, 'today', lambda: datetime.date(2023, 10, 26))
        assert konvertiereZeitspanne('7', 'unknown_kf') == ('20231020', '20231026')
```

---

### 3. External-System Replacements

*   **Purpose**: Verify that any external system interactions (e.g., Oracle reads, SFTP/S3 drops) behave as specified in the design.
*   **Setup**: N/A
*   **Action**: N/A
*   **Pass/Fail Criterion**: The migration design document explicitly states: "No other external systems (like Oracle, SFTP, S3) were identified as direct dependencies of this specific KornShell script." The `DWDate_` functions were internal dependencies (utility functions called by the script itself), and their re-implementation is covered in the Python module tests above. Therefore, no specific tests for external system replacements are required for this particular migration.

---

### 4. Data Quality / Row-Count / Schema Assertions

*   **Purpose**: Ensure data integrity and structural correctness.
*   **Setup**: N/A
*   **Action**: N/A
*   **Pass/Fail Criterion**: As `h_alis_parameter.ksh` is a utility script providing functions for parameter validation and conversion, it does not process or generate data in the traditional sense that would involve row counts or schema changes. Data quality is implicitly covered by the correctness of the validation and conversion logic tested in sections 1 and 2. The functions operate on individual string inputs and return single values or structs, not datasets. Therefore, no specific tests for row-count or schema assertions are applicable.