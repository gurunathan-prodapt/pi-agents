As a senior data-migration QA engineer, I've analyzed the migration design and the provided BigQuery code for `h_alis_date.ksh`. The following test cases are designed to ensure behavioral equivalence, transformation correctness, and proper handling of external system replacements and data quality.

---

## Migration Validation Tests for `h_alis_date.ksh`

### General Testing Principles

*   **Legacy Baseline:** For each test, the expected output from the BigQuery component will be compared against the actual output obtained by executing the original `h_alis_date.ksh` script with the same logical inputs. This often involves running the KornShell script and capturing its standard output or return code.
*   **BigQuery Execution:** BigQuery Stored Procedures will be called using `CALL` statements, and UDFs will be invoked in `SELECT` statements. Results from `OUT` parameters will be captured using `DECLARE` and `SET` statements within an anonymous block or a test procedure.
*   **Date Context:** For functions relying on `CURRENT_DATE()`, tests should ideally be run in a controlled environment where `CURRENT_DATE()` can be mocked or the test cases are designed to be robust to the actual execution date (e.g., testing year boundaries). For simplicity in these examples, `CURRENT_DATE()` is assumed to be the execution date, and specific dates are used for comparison.
*   **Error Handling:** BigQuery's `ASSERT` statements are used to validate expected outcomes and error conditions.

---

### Test Case 1: `DWDate_Vormonat` - Standard Previous Month Calculation

*   **Purpose:** Verify that `DWDate_Vormonat` correctly calculates the previous month for a standard date, using a specified format.
*   **Setup:**
    *   Assume `CURRENT_DATE()` is `2023-03-15`.
    *   Legacy: Set `DW_ORAUSER` and `DW_DIR_ROOT` environment variables as required by the original script.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Vormonat` function, capturing the output variable.
        ```bash
        # Simulate legacy call
        # Assuming DW_ORAUSER and DW_DIR_ROOT are set
        . ./h_alis_date.ksh
        DWDate_Vormonat MY_VAR "%Y%m"
        echo $MY_VAR
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Vormonat` procedure.
        ```sql
        DECLARE result STRING;
        CALL dataset.DWDate_Vormonat('%Y%m', result);
        SELECT result;
        ```
*   **Pass/Fail Criterion:** The `result` from BigQuery must be `202302`.
    *   **Transformation Correctness:** Ensures `DATE_SUB` and `FORMAT_DATE` work as expected.
    *   **Output Parity:** Matches the expected output from the legacy script for this input.

### Test Case 2: `DWDate_Vormonat` - Year Boundary Previous Month Calculation

*   **Purpose:** Verify `DWDate_Vormonat` correctly handles the year boundary when calculating the previous month.
*   **Setup:**
    *   Assume `CURRENT_DATE()` is `2023-01-10`.
    *   Legacy: Set `DW_ORAUSER` and `DW_DIR_ROOT`.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Vormonat`.
        ```bash
        # Simulate legacy call (adjusting CURRENT_DATE for testing)
        # This would typically involve mocking or running on a specific date.
        # For this test, assume the script is run on 2023-01-10.
        . ./h_alis_date.ksh
        DWDate_Vormonat MY_VAR "%Y%m"
        echo $MY_VAR
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Vormonat` procedure.
        ```sql
        -- To simulate CURRENT_DATE, we can use a specific date in a test harness
        -- or temporarily modify the procedure for testing purposes.
        -- For this example, we'll assume a test harness can set the context.
        -- If not, the test would need to be run on Jan 10th.
        DECLARE result STRING;
        -- Assuming a test harness can set CURRENT_DATE() to '2023-01-10'
        CALL dataset.DWDate_Vormonat('%Y%m', result);
        SELECT result;
        ```
*   **Pass/Fail Criterion:** The `result` from BigQuery must be `202212`.
    *   **Transformation Correctness:** Validates `DATE_SUB` across year boundaries.

### Test Case 3: `DWDate_Vormonat` - Default Format Handling

*   **Purpose:** Verify `DWDate_Vormonat` uses the default format (`%Y%m`) when `DWDate_FMT` is empty or NULL.
*   **Setup:**
    *   Assume `CURRENT_DATE()` is `2023-07-20`.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Vormonat` and an empty format string.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Vormonat MY_VAR "" # Empty string for format
        echo $MY_VAR
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Vormonat` procedure with an empty string and NULL.
        ```sql
        DECLARE result_empty STRING;
        DECLARE result_null STRING;
        -- Assuming CURRENT_DATE() is '2023-07-20'
        CALL dataset.DWDate_Vormonat('', result_empty);
        CALL dataset.DWDate_Vormonat(NULL, result_null);
        SELECT result_empty, result_null;
        ```
*   **Pass/Fail Criterion:** Both `result_empty` and `result_null` from BigQuery must be `202306`.
    *   **Transformation Correctness:** Confirms `COALESCE(NULLIF(DWDate_FMT, ''), '%Y%m')` logic.

### Test Case 4: `DWDate_Datum_Check` - Valid Date and Format

*   **Purpose:** Verify `DWDate_Datum_Check` correctly identifies a valid date string with its corresponding format.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Datum_Check`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Datum_Check "20230131" "YYYYMMDD"
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Datum_Check` procedure.
        ```sql
        DECLARE is_valid_date BOOL;
        CALL dataset.DWDate_Datum_Check('20230131', '%Y%m%d', is_valid_date);
        ASSERT is_valid_date IS TRUE AS 'Test Case 4 Failed: Valid date not recognized.';
        ```
*   **Pass/Fail Criterion:** The BigQuery `is_valid_date` must be `TRUE`. The legacy script's return code should be `0`.
    *   **Output Parity:** Matches legacy return code.
    *   **Transformation Correctness:** `PARSE_DATE` correctly parses the date.

### Test Case 5: `DWDate_Datum_Check` - Invalid Date Value

*   **Purpose:** Verify `DWDate_Datum_Check` correctly identifies an invalid date value (e.g., non-existent day).
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Datum_Check`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Datum_Check "20230230" "YYYYMMDD" # Feb 30th does not exist
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Datum_Check` procedure.
        ```sql
        DECLARE is_valid_date BOOL;
        CALL dataset.DWDate_Datum_Check('20230230', '%Y%m%d', is_valid_date);
        ASSERT is_valid_date IS FALSE AS 'Test Case 5 Failed: Invalid date recognized as valid.';
        ```
*   **Pass/Fail Criterion:** The BigQuery `is_valid_date` must be `FALSE`. The legacy script's return code should be non-`0` (e.g., `1` or `255` depending on `sqlplus` error).
    *   **Output Parity:** Matches legacy return code.
    *   **Transformation Correctness:** `PARSE_DATE` correctly throws an error for invalid date values.

### Test Case 6: `DWDate_Datum_Check` - Mismatched Date Format

*   **Purpose:** Verify `DWDate_Datum_Check` correctly identifies a date string that does not match the provided format.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Datum_Check`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Datum_Check "2023-01-01" "YYYYMMDD" # Format mismatch
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Datum_Check` procedure.
        ```sql
        DECLARE is_valid_date BOOL;
        CALL dataset.DWDate_Datum_Check('2023-01-01', '%Y%m%d', is_valid_date);
        ASSERT is_valid_date IS FALSE AS 'Test Case 6 Failed: Mismatched format date recognized as valid.';
        ```
*   **Pass/Fail Criterion:** The BigQuery `is_valid_date` must be `FALSE`. The legacy script's return code should be non-`0`.
    *   **Output Parity:** Matches legacy return code.
    *   **Transformation Correctness:** `PARSE_DATE` correctly throws an error for format mismatches.

### Test Case 7: `DWDate_Datum_LE` - First Date Less Than Second

*   **Purpose:** Verify `DWDate_Datum_LE` passes when the first date is less than the second.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Datum_LE`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Datum_LE "20230101" "20230102"
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Datum_LE` procedure.
        ```sql
        DECLARE is_le BOOL;
        CALL dataset.DWDate_Datum_LE('20230101', '20230102', is_le);
        ASSERT is_le IS TRUE AS 'Test Case 7 Failed: 20230101 < 20230102 should pass.';
        ```
*   **Pass/Fail Criterion:** The BigQuery `is_le` must be `TRUE`. The legacy script's return code should be `0`.
    *   **Output Parity:** Matches legacy return code.
    *   **Transformation Correctness:** Date comparison logic is correct.

### Test Case 8: `DWDate_Datum_LE` - First Date Equal to Second

*   **Purpose:** Verify `DWDate_Datum_LE` passes when the first date is equal to the second.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Datum_LE`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Datum_LE "20230101" "20230101"
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Datum_LE` procedure.
        ```sql
        DECLARE is_le BOOL;
        CALL dataset.DWDate_Datum_LE('20230101', '20230101', is_le);
        ASSERT is_le IS TRUE AS 'Test Case 8 Failed: 20230101 = 20230101 should pass.';
        ```
*   **Pass/Fail Criterion:** The BigQuery `is_le` must be `TRUE`. The legacy script's return code should be `0`.
    *   **Output Parity:** Matches legacy return code.
    *   **Transformation Correctness:** Date comparison logic is correct.

### Test Case 9: `DWDate_Datum_LE` - First Date Greater Than Second (Error Case)

*   **Purpose:** Verify `DWDate_Datum_LE` correctly raises an error when the first date is greater than the second.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Datum_LE`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Datum_LE "20230102" "20230101"
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Datum_LE` procedure within a `BEGIN...EXCEPTION` block to catch the `ASSERT` error.
        ```sql
        DECLARE is_le BOOL;
        DECLARE error_message STRING;
        BEGIN
          CALL dataset.DWDate_Datum_LE('20230102', '20230101', is_le);
          SET error_message = 'No error raised, but expected one.';
        EXCEPTION WHEN ERROR THEN
          SET error_message = @@error.message;
        END;
        ASSERT STARTS_WITH(error_message, 'Datum 20230102 ist groesser als 20230101') IS TRUE
          AS 'Test Case 9 Failed: Expected specific error message for d1 > d2.';
        ```
*   **Pass/Fail Criterion:** The BigQuery execution must raise an error with a message starting with `'Datum 20230102 ist groesser als 20230101'`. The legacy script's return code should be non-`0` (e.g., `255` or `1` depending on `sqlplus` error handling).
    *   **Output Parity:** Matches legacy error condition.
    *   **Transformation Correctness:** `ASSERT FALSE` correctly triggers an error with the specified message.

### Test Case 10: `DWDate_Gib_Zeitraum` - Daily Period (Offset 0)

*   **Purpose:** Verify `DWDate_Gib_Zeitraum` returns the current day for `Stufe='D'` and `Offset=0`.
*   **Setup:**
    *   Assume `CURRENT_DATE()` is `2023-03-15`.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Gib_Zeitraum`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Gib_Zeitraum 0 "D" "YYYYMMDD" START_VAR END_VAR
        echo "Start: $START_VAR, Ende: $END_VAR"
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Gib_Zeitraum` procedure.
        ```sql
        DECLARE start_date STRING;
        DECLARE end_date STRING;
        -- Assuming CURRENT_DATE() is '2023-03-15'
        CALL dataset.DWDate_Gib_Zeitraum(0, 'D', '%Y%m%d', start_date, end_date);
        ASSERT start_date = '20230315' AND end_date = '20230315'
          AS 'Test Case 10 Failed: Daily period (offset 0) incorrect.';
        ```
*   **Pass/Fail Criterion:** BigQuery `start_date` and `end_date` must both be `20230315`. Legacy output should match.
    *   **Output Parity:** Matches legacy output.
    *   **Transformation Correctness:** `DATE_ADD` with `0 DAY` and `FORMAT_DATE` work.

### Test Case 11: `DWDate_Gib_Zeitraum` - Monthly Period (Offset -1, Year Boundary)

*   **Purpose:** Verify `DWDate_Gib_Zeitraum` correctly calculates the start and end of the previous month, crossing a year boundary.
*   **Setup:**
    *   Assume `CURRENT_DATE()` is `2023-01-20`.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Gib_Zeitraum`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Gib_Zeitraum -1 "M" "YYYYMMDD" START_VAR END_VAR
        echo "Start: $START_VAR, Ende: $END_VAR"
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Gib_Zeitraum` procedure.
        ```sql
        DECLARE start_date STRING;
        DECLARE end_date STRING;
        -- Assuming CURRENT_DATE() is '2023-01-20'
        CALL dataset.DWDate_Gib_Zeitraum(-1, 'M', '%Y%m%d', start_date, end_date);
        ASSERT start_date = '20221201' AND end_date = '20221231'
          AS 'Test Case 11 Failed: Monthly period (offset -1, year boundary) incorrect.';
        ```
*   **Pass/Fail Criterion:** BigQuery `start_date` must be `20221201` and `end_date` must be `20221231`. Legacy output should match.
    *   **Output Parity:** Matches legacy output.
    *   **Transformation Correctness:** `DATE_ADD(..., INTERVAL -1 MONTH)`, `DATE_TRUNC(..., MONTH)`, and `LAST_DAY(..., MONTH)` work correctly across year boundaries.

### Test Case 12: `DWDate_Gib_Zeitraum` - Yearly Period (Offset 1)

*   **Purpose:** Verify `DWDate_Gib_Zeitraum` correctly calculates the start and end of the next year.
*   **Setup:**
    *   Assume `CURRENT_DATE()` is `2023-06-01`.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Gib_Zeitraum`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Gib_Zeitraum 1 "Y" "YYYYMMDD" START_VAR END_VAR
        echo "Start: $START_VAR, Ende: $END_VAR"
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Gib_Zeitraum` procedure.
        ```sql
        DECLARE start_date STRING;
        DECLARE end_date STRING;
        -- Assuming CURRENT_DATE() is '2023-06-01'
        CALL dataset.DWDate_Gib_Zeitraum(1, 'Y', '%Y%m%d', start_date, end_date);
        ASSERT start_date = '20240101' AND end_date = '20241231'
          AS 'Test Case 12 Failed: Yearly period (offset 1) incorrect.';
        ```
*   **Pass/Fail Criterion:** BigQuery `start_date` must be `20240101` and `end_date` must be `20241231`. Legacy output should match.
    *   **Output Parity:** Matches legacy output.
    *   **Transformation Correctness:** `DATE_ADD(..., INTERVAL 1 YEAR)`, `DATE_TRUNC(..., YEAR)`, and `LAST_DAY(..., YEAR)` work correctly.

### Test Case 13: `DWDate_Gib_Zeitraum` - Invalid Stufe (Error Case)

*   **Purpose:** Verify `DWDate_Gib_Zeitraum` raises an error for an invalid `Stufe` parameter.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `DWDate_Gib_Zeitraum` and an invalid `Stufe`.
        ```bash
        . ./h_alis_date.ksh
        DWDate_Gib_Zeitraum 0 "X" "YYYYMMDD" START_VAR END_VAR
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.DWDate_Gib_Zeitraum` procedure within a `BEGIN...EXCEPTION` block.
        ```sql
        DECLARE start_date STRING;
        DECLARE end_date STRING;
        DECLARE error_message STRING;
        BEGIN
          CALL dataset.DWDate_Gib_Zeitraum(0, 'X', '%Y%m%d', start_date, end_date);
          SET error_message = 'No error raised, but expected one for invalid Stufe.';
        EXCEPTION WHEN ERROR THEN
          SET error_message = @@error.message;
        END;
        ASSERT error_message = 'Invalid Stufe (Level) provided. Must be D, M, or Y.'
          AS 'Test Case 13 Failed: Expected specific error message for invalid Stufe.';
        ```
*   **Pass/Fail Criterion:** BigQuery execution must raise an error with the exact message `'Invalid Stufe (Level) provided. Must be D, M, or Y.'`. The legacy script should return `1` and print an error message.
    *   **Output Parity:** Matches legacy error condition.
    *   **Transformation Correctness:** `ASSERT` statement correctly handles invalid input.

### Test Case 14: `LetzterTagDesMonats` - Last Day of Month (True)

*   **Purpose:** Verify `LetzterTagDesMonats` correctly identifies a date as the last day of its month.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `LetzterTagDesMonats`.
        ```bash
        . ./h_alis_date.ksh
        LetzterTagDesMonats "20230131"
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.LetzterTagDesMonats` UDF.
        ```sql
        SELECT dataset.LetzterTagDesMonats('20230131');
        ```
*   **Pass/Fail Criterion:** The BigQuery UDF must return `TRUE`. The legacy script's return code should be `0`.
    *   **Output Parity:** Matches legacy return code.
    *   **Transformation Correctness:** `LAST_DAY` comparison works.

### Test Case 15: `LetzterTagDesMonats` - Not Last Day of Month (False)

*   **Purpose:** Verify `LetzterTagDesMonats` correctly identifies a date as not the last day of its month.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `LetzterTagDesMonats`.
        ```bash
        . ./h_alis_date.ksh
        LetzterTagDesMonats "20230115"
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.LetzterTagDesMonats` UDF.
        ```sql
        SELECT dataset.LetzterTagDesMonats('20230115');
        ```
*   **Pass/Fail Criterion:** The BigQuery UDF must return `FALSE`. The legacy script's return code should be `1`.
    *   **Output Parity:** Matches legacy return code.
    *   **Transformation Correctness:** `LAST_DAY` comparison works.

### Test Case 16: `LetzterTagDesMonats` - Leap Year February

*   **Purpose:** Verify `LetzterTagDesMonats` correctly handles leap year February.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `LetzterTagDesMonats`.
        ```bash
        . ./h_alis_date.ksh
        LetzterTagDesMonats "20240229" # Leap year
        echo $? # Capture return code
        LetzterTagDesMonats "20230228" # Non-leap year
        echo $? # Capture return code
        ```
    *   **BigQuery:** Call the `dataset.LetzterTagDesMonats` UDF for both cases.
        ```sql
        SELECT dataset.LetzterTagDesMonats('20240229') AS is_leap_feb_last_day,
               dataset.LetzterTagDesMonats('20230228') AS is_non_leap_feb_last_day;
        ```
*   **Pass/Fail Criterion:** BigQuery `is_leap_feb_last_day` must be `TRUE`. BigQuery `is_non_leap_feb_last_day` must be `TRUE`. Legacy return codes should be `0` for both.
    *   **Output Parity:** Matches legacy return codes.
    *   **Transformation Correctness:** `LAST_DAY` correctly accounts for leap years.

### Test Case 17: `TageimMonat` - Standard Month

*   **Purpose:** Verify `TageimMonat` returns the correct number of days for a standard month.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `TageimMonat`.
        ```bash
        . ./h_alis_date.ksh
        TageimMonat 2023 1 # January
        TageimMonat 2023 4 # April
        ```
    *   **BigQuery:** Call the `dataset.TageimMonat` UDF.
        ```sql
        SELECT dataset.TageimMonat(2023, 1) AS days_jan,
               dataset.TageimMonat(2023, 4) AS days_apr;
        ```
*   **Pass/Fail Criterion:** BigQuery `days_jan` must be `31`, `days_apr` must be `30`. Legacy output should match.
    *   **Output Parity:** Matches legacy output.
    *   **Transformation Correctness:** `EXTRACT(DAY FROM LAST_DAY(...))` works.

### Test Case 18: `TageimMonat` - Leap Year vs. Non-Leap Year February

*   **Purpose:** Verify `TageimMonat` correctly returns days for February in leap and non-leap years.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `TageimMonat`.
        ```bash
        . ./h_alis_date.ksh
        TageimMonat 2024 2 # Leap year
        TageimMonat 2023 2 # Non-leap year
        ```
    *   **BigQuery:** Call the `dataset.TageimMonat` UDF.
        ```sql
        SELECT dataset.TageimMonat(2024, 2) AS days_leap_feb,
               dataset.TageimMonat(2023, 2) AS days_non_leap_feb;
        ```
*   **Pass/Fail Criterion:** BigQuery `days_leap_feb` must be `29`, `days_non_leap_feb` must be `28`. Legacy output should match.
    *   **Output Parity:** Matches legacy output.
    *   **Transformation Correctness:** `LAST_DAY` correctly accounts for leap years.

### Test Case 19: `AddiereDatum` - Positive Days, Month/Year Overflow

*   **Purpose:** Verify `AddiereDatum` correctly adds positive days, handling month and year overflows.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `AddiereDatum`.
        ```bash
        . ./h_alis_date.ksh
        AddiereDatum "20230125" 10 # Month overflow
        AddiereDatum "20231225" 10 # Year overflow
        ```
    *   **BigQuery:** Call the `dataset.AddiereDatum` procedure.
        ```sql
        DECLARE result_month_overflow STRING;
        DECLARE result_year_overflow STRING;
        CALL dataset.AddiereDatum('20230125', 10, result_month_overflow);
        CALL dataset.AddiereDatum('20231225', 10, result_year_overflow);
        ASSERT result_month_overflow = '20230204' AND result_year_overflow = '20240104'
          AS 'Test Case 19 Failed: Positive day addition with overflows incorrect.';
        ```
*   **Pass/Fail Criterion:** BigQuery `result_month_overflow` must be `20230204`, `result_year_overflow` must be `20240104`. Legacy output should match.
    *   **Output Parity:** Matches legacy output.
    *   **Transformation Correctness:** `DATE_ADD` correctly handles date arithmetic.

### Test Case 20: `AddiereDatum` - Negative Days, Month/Year Underflow

*   **Purpose:** Verify `AddiereDatum` correctly adds negative days, handling month and year underflows.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `AddiereDatum`.
        ```bash
        . ./h_alis_date.ksh
        AddiereDatum "20230205" -10 # Month underflow
        AddiereDatum "20230105" -10 # Year underflow
        ```
    *   **BigQuery:** Call the `dataset.AddiereDatum` procedure.
        ```sql
        DECLARE result_month_underflow STRING;
        DECLARE result_year_underflow STRING;
        CALL dataset.AddiereDatum('20230205', -10, result_month_underflow);
        CALL dataset.AddiereDatum('20230105', -10, result_year_underflow);
        ASSERT result_month_underflow = '20230126' AND result_year_underflow = '20221226'
          AS 'Test Case 20 Failed: Negative day addition with underflows incorrect.';
        ```
*   **Pass/Fail Criterion:** BigQuery `result_month_underflow` must be `20230126`, `result_year_underflow` must be `20221226`. Legacy output should match.
    *   **Output Parity:** Matches legacy output.
    *   **Transformation Correctness:** `DATE_ADD` with negative intervals correctly handles date arithmetic.

### Test Case 21: `AddiereDatum` - Adding Zero Days

*   **Purpose:** Verify `AddiereDatum` returns the original date when adding zero days.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** Execute `h_alis_date.ksh` with `AddiereDatum`.
        ```bash
        . ./h_alis_date.ksh
        AddiereDatum "20230510" 0
        ```
    *   **BigQuery:** Call the `dataset.AddiereDatum` procedure.
        ```sql
        DECLARE result_zero_days STRING;
        CALL dataset.AddiereDatum('20230510', 0, result_zero_days);
        ASSERT result_zero_days = '20230510'
          AS 'Test Case 21 Failed: Adding zero days changed the date.';
        ```
*   **Pass/Fail Criterion:** BigQuery `result_zero_days` must be `20230510`. Legacy output should match.
    *   **Output Parity:** Matches legacy output.
    *   **Transformation Correctness:** `DATE_ADD` with `0 DAY` works.

### Test Case 22: `IsLeapYear` - Leap Year Identification

*   **Purpose:** Verify the helper UDF `IsLeapYear` correctly identifies leap years.
*   **Setup:** None.
*   **Action:**
    *   **Legacy:** The `IsLeapYear` logic is embedded in `LetzterTagDesMonats` and `TageimMonat`. Its correctness is implicitly tested by those functions.
    *   **BigQuery:** Call the `dataset.IsLeapYear` UDF for various years.
        ```sql
        SELECT dataset.IsLeapYear(2000) AS y2000, -- Divisible by 400
               dataset.IsLeapYear(2004) AS y2004, -- Divisible by 4, not 100
               dataset.IsLeapYear(1900) AS y1900, -- Divisible by 100, not 400
               dataset.IsLeapYear(2023) AS y2023; -- Not divisible by 4
        ```
*   **Pass/Fail Criterion:** BigQuery results must be `y2000=TRUE`, `y2004=TRUE`, `y1900=FALSE`, `y2023=FALSE`.
    *   **Transformation Correctness:** The boolean logic for leap year calculation is correct.

### Test Case 23: External System Replacement - No Oracle Calls

*   **Purpose:** Verify that the migrated BigQuery components do not make any calls to the legacy Oracle database or rely on external SQL files.
*   **Setup:**
    *   Ensure the BigQuery environment is isolated from the legacy Oracle database.
*   **Action:**
    *   Review the BigQuery code for `dataset.DWDate_Vormonat`, `dataset.DWDate_Datum_Check`, `dataset.DWDate_Datum_LE`, `dataset.DWDate_Gib_Zeitraum`, `dataset.LetzterTagDesMonats`, `dataset.TageimMonat`, `dataset.AddiereDatum`.
    *   Execute any of the BigQuery procedures/UDFs and monitor BigQuery logs for any external connection attempts or references to external resources (though BigQuery SQL itself doesn't directly support external calls in this manner).
*   **Pass/Fail Criterion:**
    *   **Code Review:** The BigQuery SQL code must not contain any `EXTERNAL_QUERY`, `FEDERATED_QUERY`, or similar constructs that would connect to an external database.
    *   **Execution Monitoring:** No network traffic or log entries indicating attempts to connect to the Oracle database should be observed during the execution of the BigQuery components.
    *   **External System Replacement:** This test directly validates the elimination of the Oracle dependency as per the design.

### Test Case 24: Data Quality - NULL Input Handling for UDFs

*   **Purpose:** Verify that UDFs handle `NULL` inputs gracefully, returning `NULL` or an appropriate error as per BigQuery's default `NULL` handling for functions.
*   **Setup:** None.
*   **Action:**
    *   **BigQuery:** Call `LetzterTagDesMonats`, `TageimMonat`, and `IsLeapYear` with `NULL` inputs.
        ```sql
        SELECT dataset.LetzterTagDesMonats(NULL) AS last_day_null_date,
               dataset.TageimMonat(NULL, 1) AS days_null_year,
               dataset.TageimMonat(2023, NULL) AS days_null_month,
               dataset.IsLeapYear(NULL) AS leap_year_null;
        ```
*   **Pass/Fail Criterion:** All results must be `NULL`. BigQuery's native functions generally propagate `NULL` inputs to `NULL` outputs unless explicitly handled.
    *   **Data Quality:** Ensures predictable behavior with missing or invalid data.

### Test Case 25: Data Quality - Invalid Date String for `PARSE_DATE` (Procedures)

*   **Purpose:** Verify that procedures relying on `PARSE_DATE` handle invalid date strings by raising an error, as expected.
*   **Setup:** None.
*   **Action:**
    *   **BigQuery:** Call `DWDate_Datum_LE` and `AddiereDatum` with an unparseable date string.
        ```sql
        DECLARE is_le BOOL;
        DECLARE add_date_result STRING;
        DECLARE error_message_le STRING;
        DECLARE error_message_add STRING;

        BEGIN
          CALL dataset.DWDate_Datum_LE('INVALID_DATE', '20230101', is_le);
          SET error_message_le = 'No error for invalid date in DWDate_Datum_LE.';
        EXCEPTION WHEN ERROR THEN
          SET error_message_le = @@error.message;
        END;

        BEGIN
          CALL dataset.AddiereDatum('INVALID_DATE', 10, add_date_result);
          SET error_message_add = 'No error for invalid date in AddiereDatum.';
        EXCEPTION WHEN ERROR THEN
          SET error_message_add = @@error.message;
        END;

        ASSERT STARTS_WITH(error_message_le, 'Failed to parse date') IS TRUE
          AS 'Test Case 25 Failed: DWDate_Datum_LE did not error for invalid date.';
        ASSERT STARTS_WITH(error_message_add, 'Failed to parse date') IS TRUE
          AS 'Test Case 25 Failed: AddiereDatum did not error for invalid date.';
        ```
*   **Pass/Fail Criterion:** Both `DWDate_Datum_LE` and `AddiereDatum` calls must raise an error, and the error messages should indicate a date parsing failure (e.g., `'Failed to parse date...'`).
    *   **Data Quality:** Ensures robust error handling for malformed inputs.