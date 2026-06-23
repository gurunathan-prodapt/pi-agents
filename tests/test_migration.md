As a senior data-migration QA engineer, I've analyzed the migration design document and the generated BigQuery code for `h_alis_date.ksh`. The following test cases are designed to validate the migration, focusing on behavioral equivalence, transformation correctness, and proper handling of external system replacements and edge cases.

A critical point highlighted in the design document is the unavailability of the source SQL files (`d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`). For the functions `DWDate_Vormonat` and `DWDate_Gib_Zeitraum`, the BigQuery UDFs are based on a *common interpretation* of their likely behavior. Therefore, tests for these functions will validate the BigQuery UDFs against this interpreted logic, rather than strict output parity with an unknown legacy implementation.

---

## Test Environment Assumptions

*   **Legacy Environment:**
    *   A KornShell environment where `h_alis_date.ksh` can be sourced and its functions called.
    *   `sqlplus` is configured and can connect to an Oracle database using `DW_ORAUSER`.
    *   `DW_DIR_ROOT` is set to the path containing `allgemein/is/util/sql/d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`. For testing, these SQL files would ideally be mocked or their actual content used if available.
    *   For `DWDate_Vormonat` and `DWDate_Gib_Zeitraum`, if the original SQL files are found, their logic should be used to derive expected outputs. Otherwise, the tests will validate the BigQuery UDF's *interpreted* logic.
*   **Migrated Environment:**
    *   A Google Cloud BigQuery project with the `dw_utils` dataset created.
    *   All BigQuery UDFs (`is_last_day_of_month`, `days_in_month`, `add_days_to_date`, `is_valid_date`, `is_date_le`, `get_previous_month`, `get_date_range`) are deployed in the `dw_utils` dataset.
    *   Tests will be executed using BigQuery SQL queries.

---

## Test Cases

### Test Case: `LetzterTagDesMonat` / `dw_utils.is_last_day_of_month` - Standard Last Day

*   **Purpose:** Verify that the UDF correctly identifies the last day of a standard month.
*   **Setup:**
    *   Legacy: Ensure `h_alis_date.ksh` is sourced.
    *   Migrated: Ensure `dw_utils.is_last_day_of_month` UDF is deployed.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    LetzterTagDesMonats "20230131"
    echo $? # Capture return code
    ```
*   **Expected Output (Legacy):** `0` (indicating true)
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.is_last_day_of_month('20230131') AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `TRUE`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_last_day_of_month('20230131') AS actual_output,
        TRUE AS expected_output,
        CASE WHEN dw_utils.is_last_day_of_month('20230131') = TRUE THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `LetzterTagDesMonat` / `dw_utils.is_last_day_of_month` - Not Last Day

*   **Purpose:** Verify that the UDF correctly identifies a day that is not the last day of the month.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    LetzterTagDesMonats "20230115"
    echo $? # Capture return code
    ```
*   **Expected Output (Legacy):** `1` (indicating false)
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.is_last_day_of_month('20230115') AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `FALSE`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_last_day_of_month('20230115') AS actual_output,
        FALSE AS expected_output,
        CASE WHEN dw_utils.is_last_day_of_month('20230115') = FALSE THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `LetzterTagDesMonat` / `dw_utils.is_last_day_of_month` - Leap Year February

*   **Purpose:** Verify correct handling of February in a leap year.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    LetzterTagDesMonats "20240229" # 2024 is a leap year
    echo $?
    LetzterTagDesMonats "20240228"
    echo $?
    ```
*   **Expected Output (Legacy):** `0` (for 20240229), `1` (for 20240228)
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.is_last_day_of_month('20240229') AS leap_year_last_day,
        dw_utils.is_last_day_of_month('20240228') AS leap_year_not_last_day;
    ```
*   **Pass/Fail Criterion:** `leap_year_last_day` must be `TRUE`, `leap_year_not_last_day` must be `FALSE`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_last_day_of_month('20240229') AS actual_leap_last_day,
        TRUE AS expected_leap_last_day,
        dw_utils.is_last_day_of_month('20240228') AS actual_leap_not_last_day,
        FALSE AS expected_leap_not_last_day,
        CASE
            WHEN dw_utils.is_last_day_of_month('20240229') = TRUE AND dw_utils.is_last_day_of_month('20240228') = FALSE THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `LetzterTagDesMonat` / `dw_utils.is_last_day_of_month` - Non-Leap Year February

*   **Purpose:** Verify correct handling of February in a non-leap year.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    LetzterTagDesMonats "20230228" # 2023 is not a leap year
    echo $?
    LetzterTagDesMonats "20230227"
    echo $?
    ```
*   **Expected Output (Legacy):** `0` (for 20230228), `1` (for 20230227)
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.is_last_day_of_month('20230228') AS non_leap_year_last_day,
        dw_utils.is_last_day_of_month('20230227') AS non_leap_year_not_last_day;
    ```
*   **Pass/Fail Criterion:** `non_leap_year_last_day` must be `TRUE`, `non_leap_year_not_last_day` must be `FALSE`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_last_day_of_month('20230228') AS actual_non_leap_last_day,
        TRUE AS expected_non_leap_last_day,
        dw_utils.is_last_day_of_month('20230227') AS actual_non_leap_not_last_day,
        FALSE AS expected_non_leap_not_last_day,
        CASE
            WHEN dw_utils.is_last_day_of_month('20230228') = TRUE AND dw_utils.is_last_day_of_month('20230227') = FALSE THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `LetzterTagDesMonat` / `dw_utils.is_last_day_of_month` - Invalid Date Input

*   **Purpose:** Verify NULL handling for invalid date strings.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    LetzterTagDesMonats "20230230" # Invalid date
    echo $?
    LetzterTagDesMonats "INVALID"
    echo $?
    ```
*   **Expected Output (Legacy):** The legacy script's `cut` and arithmetic operations might produce unexpected results or errors, but it won't return 0 or 1 meaningfully for an invalid date. It's likely to fail or produce garbage. The `return 1` is for argument count, not date validity.
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.is_last_day_of_month('20230230') AS invalid_date,
        dw_utils.is_last_day_of_month('INVALID') AS malformed_string,
        dw_utils.is_last_day_of_month(NULL) AS null_input;
    ```
*   **Pass/Fail Criterion:** All BigQuery UDF outputs must be `NULL`. This aligns with `SAFE.PARSE_DATE` behavior and is a robust way to handle invalid inputs in BigQuery.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_last_day_of_month('20230230') IS NULL AS invalid_date_is_null,
        dw_utils.is_last_day_of_month('INVALID') IS NULL AS malformed_string_is_null,
        dw_utils.is_last_day_of_month(NULL) IS NULL AS null_input_is_null,
        CASE
            WHEN dw_utils.is_last_day_of_month('20230230') IS NULL
             AND dw_utils.is_last_day_of_month('INVALID') IS NULL
             AND dw_utils.is_last_day_of_month(NULL) IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `TageimMonat` / `dw_utils.days_in_month` - Standard Months

*   **Purpose:** Verify the UDF correctly returns the number of days for standard months.
*   **Setup:**
    *   Legacy: Ensure `h_alis_date.ksh` is sourced.
    *   Migrated: Ensure `dw_utils.days_in_month` UDF is deployed.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    TageimMonat 2023 1  # January
    TageimMonat 2023 4  # April
    ```
*   **Expected Output (Legacy):** `31` (for Jan), `30` (for Apr)
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.days_in_month(2023, 1) AS days_jan,
        dw_utils.days_in_month(2023, 4) AS days_apr;
    ```
*   **Pass/Fail Criterion:** `days_jan` must be `31`, `days_apr` must be `30`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.days_in_month(2023, 1) AS actual_jan, 31 AS expected_jan,
        dw_utils.days_in_month(2023, 4) AS actual_apr, 30 AS expected_apr,
        CASE
            WHEN dw_utils.days_in_month(2023, 1) = 31 AND dw_utils.days_in_month(2023, 4) = 30 THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `TageimMonat` / `dw_utils.days_in_month` - February (Leap and Non-Leap)

*   **Purpose:** Verify correct handling of February in leap and non-leap years.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    TageimMonat 2024 2 # Leap year
    TageimMonat 2023 2 # Non-leap year
    ```
*   **Expected Output (Legacy):** `29` (for 2024), `28` (for 2023)
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.days_in_month(2024, 2) AS days_feb_leap,
        dw_utils.days_in_month(2023, 2) AS days_feb_non_leap;
    ```
*   **Pass/Fail Criterion:** `days_feb_leap` must be `29`, `days_feb_non_leap` must be `28`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.days_in_month(2024, 2) AS actual_leap_feb, 29 AS expected_leap_feb,
        dw_utils.days_in_month(2023, 2) AS actual_non_leap_feb, 28 AS expected_non_leap_feb,
        CASE
            WHEN dw_utils.days_in_month(2024, 2) = 29 AND dw_utils.days_in_month(2023, 2) = 28 THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `TageimMonat` / `dw_utils.days_in_month` - Invalid Month Input

*   **Purpose:** Verify NULL handling for invalid month inputs.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    TageimMonat 2023 0
    TageimMonat 2023 13
    ```
*   **Expected Output (Legacy):** The legacy script's array lookup `"${LetzterTag[$2]}"` would likely result in an empty string or an error for out-of-bounds indices.
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.days_in_month(2023, 0) AS invalid_month_0,
        dw_utils.days_in_month(2023, 13) AS invalid_month_13,
        dw_utils.days_in_month(NULL, 1) AS null_year,
        dw_utils.days_in_month(2023, NULL) AS null_month;
    ```
*   **Pass/Fail Criterion:** All BigQuery UDF outputs must be `NULL`. This aligns with `SAFE.MAKE_DATE` behavior.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.days_in_month(2023, 0) IS NULL AS invalid_month_0_is_null,
        dw_utils.days_in_month(2023, 13) IS NULL AS invalid_month_13_is_null,
        dw_utils.days_in_month(NULL, 1) IS NULL AS null_year_is_null,
        dw_utils.days_in_month(2023, NULL) IS NULL AS null_month_is_null,
        CASE
            WHEN dw_utils.days_in_month(2023, 0) IS NULL
             AND dw_utils.days_in_month(2023, 13) IS NULL
             AND dw_utils.days_in_month(NULL, 1) IS NULL
             AND dw_utils.days_in_month(2023, NULL) IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `AddiereDatum` / `dw_utils.add_days_to_date` - Add Days (No Overflow)

*   **Purpose:** Verify adding days without month/year overflow.
*   **Setup:**
    *   Legacy: Ensure `h_alis_date.ksh` is sourced.
    *   Migrated: Ensure `dw_utils.add_days_to_date` UDF is deployed.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    AddiereDatum "20230115" 10
    ```
*   **Expected Output (Legacy):** `20230125`
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.add_days_to_date('20230115', 10) AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `20230125`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.add_days_to_date('20230115', 10) AS actual_output,
        '20230125' AS expected_output,
        CASE WHEN dw_utils.add_days_to_date('20230115', 10) = '20230125' THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `AddiereDatum` / `dw_utils.add_days_to_date` - Add Days (Month Overflow)

*   **Purpose:** Verify adding days that cause a month overflow.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    AddiereDatum "20230125" 10
    ```
*   **Expected Output (Legacy):** `20230204`
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.add_days_to_date('20230125', 10) AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `20230204`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.add_days_to_date('20230125', 10) AS actual_output,
        '20230204' AS expected_output,
        CASE WHEN dw_utils.add_days_to_date('20230125', 10) = '20230204' THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `AddiereDatum` / `dw_utils.add_days_to_date` - Add Days (Year Overflow)

*   **Purpose:** Verify adding days that cause a year overflow.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    AddiereDatum "20231225" 10
    ```
*   **Expected Output (Legacy):** `20240104`
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.add_days_to_date('20231225', 10) AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `20240104`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.add_days_to_date('20231225', 10) AS actual_output,
        '20240104' AS expected_output,
        CASE WHEN dw_utils.add_days_to_date('20231225', 10) = '20240104' THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `AddiereDatum` / `dw_utils.add_days_to_date` - Leap Year Boundary

*   **Purpose:** Verify correct handling of leap year boundaries when adding days.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    AddiereDatum "20240228" 1 # Leap year
    AddiereDatum "20230228" 1 # Non-leap year
    ```
*   **Expected Output (Legacy):** `20240229` (for leap), `20230301` (for non-leap)
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.add_days_to_date('20240228', 1) AS leap_year_result,
        dw_utils.add_days_to_date('20230228', 1) AS non_leap_year_result;
    ```
*   **Pass/Fail Criterion:** `leap_year_result` must be `20240229`, `non_leap_year_result` must be `20230301`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.add_days_to_date('20240228', 1) AS actual_leap, '20240229' AS expected_leap,
        dw_utils.add_days_to_date('20230228', 1) AS actual_non_leap, '20230301' AS expected_non_leap,
        CASE
            WHEN dw_utils.add_days_to_date('20240228', 1) = '20240229' AND dw_utils.add_days_to_date('20230228', 1) = '20230301' THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `AddiereDatum` / `dw_utils.add_days_to_date` - Subtract Days

*   **Purpose:** Verify subtracting days (negative `days_to_add`).
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    AddiereDatum "20230115" -10
    ```
*   **Expected Output (Legacy):** `20230105`
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.add_days_to_date('20230115', -10) AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `20230105`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.add_days_to_date('20230115', -10) AS actual_output,
        '20230105' AS expected_output,
        CASE WHEN dw_utils.add_days_to_date('20230115', -10) = '20230105' THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `AddiereDatum` / `dw_utils.add_days_to_date` - Invalid Date Input

*   **Purpose:** Verify NULL handling for invalid date strings.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    AddiereDatum "20230230" 5 # Invalid date
    AddiereDatum "INVALID" 5
    AddiereDatum "" 5
    ```
*   **Expected Output (Legacy):** The legacy script would likely produce garbage or errors due to `cut` and arithmetic on invalid date parts.
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.add_days_to_date('20230230', 5) AS invalid_date,
        dw_utils.add_days_to_date('INVALID', 5) AS malformed_string,
        dw_utils.add_days_to_date(NULL, 5) AS null_input_date,
        dw_utils.add_days_to_date('20230101', NULL) AS null_days_to_add;
    ```
*   **Pass/Fail Criterion:** All BigQuery UDF outputs must be `NULL`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.add_days_to_date('20230230', 5) IS NULL AS invalid_date_is_null,
        dw_utils.add_days_to_date('INVALID', 5) IS NULL AS malformed_string_is_null,
        dw_utils.add_days_to_date(NULL, 5) IS NULL AS null_input_date_is_null,
        dw_utils.add_days_to_date('20230101', NULL) IS NULL AS null_days_to_add_is_null,
        CASE
            WHEN dw_utils.add_days_to_date('20230230', 5) IS NULL
             AND dw_utils.add_days_to_date('INVALID', 5) IS NULL
             AND dw_utils.add_days_to_date(NULL, 5) IS NULL
             AND dw_utils.add_days_to_date('20230101', NULL) IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Datum_Check` / `dw_utils.is_valid_date` - Valid Date

*   **Purpose:** Verify the UDF correctly identifies a valid date string with its format.
*   **Setup:**
    *   Legacy: Ensure `h_alis_date.ksh` is sourced.
    *   Migrated: Ensure `dw_utils.is_valid_date` UDF is deployed.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    DWDate_Datum_Check "20230115" "YYYYMMDD"
    echo $?
    DWDate_Datum_Check "15.01.2023" "DD.MM.YYYY"
    echo $?
    ```
*   **Expected Output (Legacy):** `0` (for both)
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.is_valid_date('20230115', 'YYYYMMDD') AS valid_ymd,
        dw_utils.is_valid_date('15.01.2023', 'DD.MM.YYYY') AS valid_dmy;
    ```
*   **Pass/Fail Criterion:** Both BigQuery UDF outputs must be `TRUE`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_valid_date('20230115', 'YYYYMMDD') AS actual_valid_ymd, TRUE AS expected_valid_ymd,
        dw_utils.is_valid_date('15.01.2023', 'DD.MM.YYYY') AS actual_valid_dmy, TRUE AS expected_valid_dmy,
        CASE
            WHEN dw_utils.is_valid_date('20230115', 'YYYYMMDD') = TRUE AND dw_utils.is_valid_date('15.01.2023', 'DD.MM.YYYY') = TRUE THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Datum_Check` / `dw_utils.is_valid_date` - Invalid Date / Mismatched Format

*   **Purpose:** Verify the UDF correctly identifies invalid dates or format mismatches.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    DWDate_Datum_Check "20230230" "YYYYMMDD" # Invalid date
    echo $?
    DWDate_Datum_Check "2023-01-15" "YYYYMMDD" # Mismatched format
    echo $?
    ```
*   **Expected Output (Legacy):** `1` (for both, as `TO_DATE` would fail)
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.is_valid_date('20230230', 'YYYYMMDD') AS invalid_date,
        dw_utils.is_valid_date('2023-01-15', 'YYYYMMDD') AS mismatched_format;
    ```
*   **Pass/Fail Criterion:** Both BigQuery UDF outputs must be `FALSE`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_valid_date('20230230', 'YYYYMMDD') AS actual_invalid_date, FALSE AS expected_invalid_date,
        dw_utils.is_valid_date('2023-01-15', 'YYYYMMDD') AS actual_mismatched_format, FALSE AS expected_mismatched_format,
        CASE
            WHEN dw_utils.is_valid_date('20230230', 'YYYYMMDD') = FALSE AND dw_utils.is_valid_date('2023-01-15', 'YYYYMMDD') = FALSE THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Datum_Check` / `dw_utils.is_valid_date` - NULL/Empty Inputs

*   **Purpose:** Verify NULL handling for NULL or empty date/format inputs.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    DWDate_Datum_Check "" "YYYYMMDD"
    echo $?
    DWDate_Datum_Check "20230101" ""
    echo $?
    DWDate_Datum_Check "20230101" # Missing format parameter
    echo $?
    ```
*   **Expected Output (Legacy):** `1` (for all, due to `TO_DATE` failure or argument count check).
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.is_valid_date('', 'YYYYMMDD') AS empty_date_string,
        dw_utils.is_valid_date('20230101', '') AS empty_format_string,
        dw_utils.is_valid_date(NULL, 'YYYYMMDD') AS null_date_string,
        dw_utils.is_valid_date('20230101', NULL) AS null_format_string;
    ```
*   **Pass/Fail Criterion:** All BigQuery UDF outputs must be `FALSE` (for empty strings, as `SAFE.PARSE_DATE` would fail) or `NULL` (for explicit `NULL` inputs). The UDF's `CASE WHEN date_string IS NULL OR date_format IS NULL THEN NULL` handles explicit NULLs.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_valid_date('', 'YYYYMMDD') = FALSE AS empty_date_string_is_false,
        dw_utils.is_valid_date('20230101', '') = FALSE AS empty_format_string_is_false,
        dw_utils.is_valid_date(NULL, 'YYYYMMDD') IS NULL AS null_date_string_is_null,
        dw_utils.is_valid_date('20230101', NULL) IS NULL AS null_format_string_is_null,
        CASE
            WHEN dw_utils.is_valid_date('', 'YYYYMMDD') = FALSE
             AND dw_utils.is_valid_date('20230101', '') = FALSE
             AND dw_utils.is_valid_date(NULL, 'YYYYMMDD') IS NULL
             AND dw_utils.is_valid_date('20230101', NULL) IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Datum_LE` / `dw_utils.is_date_le` - Date1 <= Date2

*   **Purpose:** Verify `date1` is less than or equal to `date2`.
*   **Setup:**
    *   Legacy: Ensure `h_alis_date.ksh` is sourced.
    *   Migrated: Ensure `dw_utils.is_date_le` UDF is deployed.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    DWDate_Datum_LE "20230101" "20230101" # Equal
    echo $?
    DWDate_Datum_LE "20230101" "20230102" # Less than
    echo $?
    ```
*   **Expected Output (Legacy):** `0` (for both)
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.is_date_le('20230101', '20230101') AS equal_dates,
        dw_utils.is_date_le('20230101', '20230102') AS less_than_dates;
    ```
*   **Pass/Fail Criterion:** Both BigQuery UDF outputs must be `TRUE`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_date_le('20230101', '20230101') AS actual_equal, TRUE AS expected_equal,
        dw_utils.is_date_le('20230101', '20230102') AS actual_less_than, TRUE AS expected_less_than,
        CASE
            WHEN dw_utils.is_date_le('20230101', '20230101') = TRUE AND dw_utils.is_date_le('20230101', '20230102') = TRUE THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Datum_LE` / `dw_utils.is_date_le` - Date1 > Date2

*   **Purpose:** Verify `date1` is greater than `date2`.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    DWDate_Datum_LE "20230102" "20230101"
    echo $?
    ```
*   **Expected Output (Legacy):** `1` (due to `raise_application_error` causing `sqlplus` to exit with failure)
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.is_date_le('20230102', '20230101') AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `FALSE`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_date_le('20230102', '20230101') AS actual_output,
        FALSE AS expected_output,
        CASE WHEN dw_utils.is_date_le('20230102', '20230101') = FALSE THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `DWDate_Datum_LE` / `dw_utils.is_date_le` - Invalid Date Inputs

*   **Purpose:** Verify NULL handling for invalid date strings.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    DWDate_Datum_LE "20230230" "20230101" # Invalid date1
    echo $?
    DWDate_Datum_LE "20230101" "20230230" # Invalid date2
    echo $?
    DWDate_Datum_LE "INVALID" "20230101"
    echo $?
    ```
*   **Expected Output (Legacy):** `1` (due to `TO_DATE` failure in PL/SQL block).
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.is_date_le('20230230', '20230101') AS invalid_date1,
        dw_utils.is_date_le('20230101', '20230230') AS invalid_date2,
        dw_utils.is_date_le('INVALID', '20230101') AS malformed_date1,
        dw_utils.is_date_le('20230101', NULL) AS null_date2;
    ```
*   **Pass/Fail Criterion:** All BigQuery UDF outputs must be `NULL`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.is_date_le('20230230', '20230101') IS NULL AS invalid_date1_is_null,
        dw_utils.is_date_le('20230101', '20230230') IS NULL AS invalid_date2_is_null,
        dw_utils.is_date_le('INVALID', '20230101') IS NULL AS malformed_date1_is_null,
        dw_utils.is_date_le('20230101', NULL) IS NULL AS null_date2_is_null,
        CASE
            WHEN dw_utils.is_date_le('20230230', '20230101') IS NULL
             AND dw_utils.is_date_le('20230101', '20230230') IS NULL
             AND dw_utils.is_date_le('INVALID', '20230101') IS NULL
             AND dw_utils.is_date_le('20230101', NULL) IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Vormonat` / `dw_utils.get_previous_month` - Standard Case

*   **Purpose:** Verify the UDF returns the previous month's date for a standard input.
*   **Setup:**
    *   Legacy: Requires `d_alis_vormonat.sql`. Assume it returns the date one month prior, formatted.
    *   Migrated: Ensure `dw_utils.get_previous_month` UDF is deployed.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    DWDate_Vormonat MY_VAR "YYYYMMDD"
    echo $MY_VAR
    # To simulate for a specific date, you'd need to modify d_alis_vormonat.sql or mock sqlplus.
    # Assuming for '20230315', it would return '20230215'.
    ```
*   **Expected Output (Legacy - Interpreted):** For input `20230315`, output `20230215`.
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.get_previous_month('20230315', 'YYYYMMDD') AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `20230215`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.get_previous_month('20230315', 'YYYYMMDD') AS actual_output,
        '20230215' AS expected_output,
        CASE WHEN dw_utils.get_previous_month('20230315', 'YYYYMMDD') = '20230215' THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `DWDate_Vormonat` / `dw_utils.get_previous_month` - Month-End Rollover

*   **Purpose:** Verify correct handling of month-end rollovers (e.g., March 31st -> February 28th/29th).
*   **Setup:** Same as above.
*   **Action (Legacy):** (Requires `d_alis_vormonat.sql` logic analysis. Assuming Oracle's `ADD_MONTHS(date, -1)` behavior).
    ```bash
    # Assuming legacy behavior for '20230331' is '20230228'
    # Assuming legacy behavior for '20240331' is '20240229'
    ```
*   **Expected Output (Legacy - Interpreted):** `20230228` (for 20230331), `20240229` (for 20240331).
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.get_previous_month('20230331', 'YYYYMMDD') AS non_leap_rollover,
        dw_utils.get_previous_month('20240331', 'YYYYMMDD') AS leap_rollover;
    ```
*   **Pass/Fail Criterion:** `non_leap_rollover` must be `20230228`, `leap_rollover` must be `20240229`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.get_previous_month('20230331', 'YYYYMMDD') AS actual_non_leap, '20230228' AS expected_non_leap,
        dw_utils.get_previous_month('20240331', 'YYYYMMDD') AS actual_leap, '20240229' AS expected_leap,
        CASE
            WHEN dw_utils.get_previous_month('20230331', 'YYYYMMDD') = '20230228' AND dw_utils.get_previous_month('20240331', 'YYYYMMDD') = '20240229' THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Vormonat` / `dw_utils.get_previous_month` - Year Rollover

*   **Purpose:** Verify correct handling of year rollover (e.g., Jan 15th -> Dec 15th of previous year).
*   **Setup:** Same as above.
*   **Action (Legacy):** (Requires `d_alis_vormonat.sql` logic analysis. Assuming Oracle's `ADD_MONTHS(date, -1)` behavior).
    ```bash
    # Assuming legacy behavior for '20230115' is '20221215'
    ```
*   **Expected Output (Legacy - Interpreted):** `20221215`.
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.get_previous_month('20230115', 'YYYYMMDD') AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `20221215`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.get_previous_month('20230115', 'YYYYMMDD') AS actual_output,
        '20221215' AS expected_output,
        CASE WHEN dw_utils.get_previous_month('20230115', 'YYYYMMDD') = '20221215' THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `DWDate_Vormonat` / `dw_utils.get_previous_month` - Different Output Format

*   **Purpose:** Verify the UDF correctly formats the output date.
*   **Setup:** Same as above.
*   **Action (Legacy):** (Requires `d_alis_vormonat.sql` logic analysis).
    ```bash
    # Assuming legacy behavior for '20230315' with 'DD.MM.YYYY' is '15.02.2023'
    ```
*   **Expected Output (Legacy - Interpreted):** `15.02.2023`.
*   **Action (Migrated):**
    ```sql
    SELECT dw_utils.get_previous_month('20230315', 'DD.MM.YYYY') AS result;
    ```
*   **Pass/Fail Criterion:** The BigQuery UDF output must be `15.02.2023`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.get_previous_month('20230315', 'DD.MM.YYYY') AS actual_output,
        '15.02.2023' AS expected_output,
        CASE WHEN dw_utils.get_previous_month('20230315', 'DD.MM.YYYY') = '15.02.2023' THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

### Test Case: `DWDate_Vormonat` / `dw_utils.get_previous_month` - Invalid Input

*   **Purpose:** Verify NULL handling for invalid input date strings or format.
*   **Setup:** Same as above.
*   **Action (Legacy):** (Requires `d_alis_vormonat.sql` logic analysis. `sqlplus` would likely error).
    ```bash
    # Assuming legacy would fail or return empty/garbage for invalid date or format.
    ```
*   **Expected Output (Legacy - Interpreted):** Error or empty.
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.get_previous_month('20230230', 'YYYYMMDD') AS invalid_date,
        dw_utils.get_previous_month('20230101', 'INVALID_FORMAT') AS invalid_format,
        dw_utils.get_previous_month(NULL, 'YYYYMMDD') AS null_date,
        dw_utils.get_previous_month('20230101', NULL) AS null_format;
    ```
*   **Pass/Fail Criterion:** All BigQuery UDF outputs must be `NULL`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.get_previous_month('20230230', 'YYYYMMDD') IS NULL AS invalid_date_is_null,
        dw_utils.get_previous_month('20230101', 'INVALID_FORMAT') IS NULL AS invalid_format_is_null,
        dw_utils.get_previous_month(NULL, 'YYYYMMDD') IS NULL AS null_date_is_null,
        dw_utils.get_previous_month('20230101', NULL) IS NULL AS null_format_is_null,
        CASE
            WHEN dw_utils.get_previous_month('20230230', 'YYYYMMDD') IS NULL
             AND dw_utils.get_previous_month('20230101', 'INVALID_FORMAT') IS NULL
             AND dw_utils.get_previous_month(NULL, 'YYYYMMDD') IS NULL
             AND dw_utils.get_previous_month('20230101', NULL) IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Gib_Zeitraum` / `dw_utils.get_date_range` - Unit 'D' (Days)

*   **Purpose:** Verify date range calculation for 'D' unit with positive and negative offset.
*   **Setup:**
    *   Legacy: Requires `d_alis_datum_zeitraum.sql`. Assume `SYSDATE` is `2023-01-15`.
    *   Migrated: Ensure `dw_utils.get_date_range` UDF is deployed. For testing, we'll use a fixed `CURRENT_DATE()` for predictability.
*   **Action (Legacy - Interpreted):**
    ```bash
    # Assuming SYSDATE is 20230115
    # DWDate_Gib_Zeitraum 5 "D" "YYYYMMDD" START_VAR END_VAR
    # echo $START_VAR $END_VAR # Expected: 20230115 20230120
    # DWDate_Gib_Zeitraum -5 "D" "YYYYMMDD" START_VAR END_VAR
    # echo $START_VAR $END_VAR # Expected: 20230115 20230110
    ```
*   **Expected Output (Legacy - Interpreted, assuming `CURRENT_DATE()` is '2023-01-15'):**
    *   Offset 5: `start_date='20230115'`, `end_date='20230120'`
    *   Offset -5: `start_date='20230115'`, `end_date='20230110'`
*   **Action (Migrated):**
    ```sql
    -- To simulate CURRENT_DATE() for testing, wrap the UDF call in a SELECT statement
    -- with a specific date, or use a temporary UDF for CURRENT_DATE().
    -- For this test, we'll assume CURRENT_DATE() is '2023-01-15' for expected values.
    SELECT
        dw_utils.get_date_range(5, 'D', 'YYYYMMDD') AS range_plus_5_days,
        dw_utils.get_date_range(-5, 'D', 'YYYYMMDD') AS range_minus_5_days;
    ```
*   **Pass/Fail Criterion:**
    *   `range_plus_5_days.start_date` must be `CURRENT_DATE()` (e.g., '20230115'), `range_plus_5_days.end_date` must be `CURRENT_DATE() + 5 days` (e.g., '20230120').
    *   `range_minus_5_days.start_date` must be `CURRENT_DATE()` (e.g., '20230115'), `range_minus_5_days.end_date` must be `CURRENT_DATE() - 5 days` (e.g., '20230110').
*   **Test Code (SQL Assertion - using a fixed date for `CURRENT_DATE()` for predictability):**
    ```sql
    -- For predictable testing, replace CURRENT_DATE() with a fixed date.
    -- In a real test harness, you might mock CURRENT_DATE() or run on a specific date.
    -- For this example, we'll hardcode the expected values based on a hypothetical CURRENT_DATE() = '2023-01-15'.
    WITH TestData AS (
      SELECT
        dw_utils.get_date_range(5, 'D', 'YYYYMMDD') AS actual_plus_5,
        dw_utils.get_date_range(-5, 'D', 'YYYYMMDD') AS actual_minus_5
    )
    SELECT
        actual_plus_5.start_date AS actual_plus_5_start, '20230115' AS expected_plus_5_start,
        actual_plus_5.end_date AS actual_plus_5_end, '20230120' AS expected_plus_5_end,
        actual_minus_5.start_date AS actual_minus_5_start, '20230115' AS expected_minus_5_start,
        actual_minus_5.end_date AS actual_minus_5_end, '20230110' AS expected_minus_5_end,
        CASE
            WHEN actual_plus_5.start_date = '20230115' AND actual_plus_5.end_date = '20230120'
             AND actual_minus_5.start_date = '20230115' AND actual_minus_5.end_date = '20230110' THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Gib_Zeitraum` / `dw_utils.get_date_range` - Unit 'M' (Months)

*   **Purpose:** Verify date range calculation for 'M' unit, handling month boundaries.
*   **Setup:** Same as above.
*   **Action (Legacy - Interpreted):**
    ```bash
    # Assuming SYSDATE is 20230115
    # DWDate_Gib_Zeitraum 1 "M" "YYYYMMDD" START_VAR END_VAR
    # echo $START_VAR $END_VAR # Expected: 20230101 20230228 (or 29)
    # DWDate_Gib_Zeitraum -1 "M" "YYYYMMDD" START_VAR END_VAR
    # echo $START_VAR $END_VAR # Expected: 20230101 20221231
    ```
*   **Expected Output (Legacy - Interpreted, assuming `CURRENT_DATE()` is '2023-01-15'):**
    *   Offset 1: `start_date='20230101'`, `end_date='20230228'` (2023 is not a leap year)
    *   Offset -1: `start_date='20230101'`, `end_date='20221231'`
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.get_date_range(1, 'M', 'YYYYMMDD') AS range_plus_1_month,
        dw_utils.get_date_range(-1, 'M', 'YYYYMMDD') AS range_minus_1_month;
    ```
*   **Pass/Fail Criterion:**
    *   `range_plus_1_month.start_date` must be `DATE_TRUNC(CURRENT_DATE(), MONTH)` (e.g., '20230101'), `range_plus_1_month.end_date` must be `LAST_DAY(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH))` (e.g., '20230228').
    *   `range_minus_1_month.start_date` must be `DATE_TRUNC(CURRENT_DATE(), MONTH)` (e.g., '20230101'), `range_minus_1_month.end_date` must be `LAST_DAY(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))` (e.g., '20221231').
*   **Test Code (SQL Assertion - using a fixed date for `CURRENT_DATE()` for predictability):**
    ```sql
    WITH TestData AS (
      SELECT
        dw_utils.get_date_range(1, 'M', 'YYYYMMDD') AS actual_plus_1,
        dw_utils.get_date_range(-1, 'M', 'YYYYMMDD') AS actual_minus_1
    )
    SELECT
        actual_plus_1.start_date AS actual_plus_1_start, '20230101' AS expected_plus_1_start,
        actual_plus_1.end_date AS actual_plus_1_end, '20230228' AS expected_plus_1_end,
        actual_minus_1.start_date AS actual_minus_1_start, '20230101' AS expected_minus_1_start,
        actual_minus_1.end_date AS actual_minus_1_end, '20221231' AS expected_minus_1_end,
        CASE
            WHEN actual_plus_1.start_date = '20230101' AND actual_plus_1.end_date = '20230228'
             AND actual_minus_1.start_date = '20230101' AND actual_minus_1.end_date = '20221231' THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Gib_Zeitraum` / `dw_utils.get_date_range` - Unit 'Y' (Years)

*   **Purpose:** Verify date range calculation for 'Y' unit, handling year boundaries.
*   **Setup:** Same as above.
*   **Action (Legacy - Interpreted):**
    ```bash
    # Assuming SYSDATE is 20230115
    # DWDate_Gib_Zeitraum 1 "Y" "YYYYMMDD" START_VAR END_VAR
    # echo $START_VAR $END_VAR # Expected: 20230101 20241231
    # DWDate_Gib_Zeitraum -1 "Y" "YYYYMMDD" START_VAR END_VAR
    # echo $START_VAR $END_VAR # Expected: 20230101 20221231
    ```
*   **Expected Output (Legacy - Interpreted, assuming `CURRENT_DATE()` is '2023-01-15'):**
    *   Offset 1: `start_date='20230101'`, `end_date='20241231'`
    *   Offset -1: `start_date='20230101'`, `end_date='20221231'`
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.get_date_range(1, 'Y', 'YYYYMMDD') AS range_plus_1_year,
        dw_utils.get_date_range(-1, 'Y', 'YYYYMMDD') AS range_minus_1_year;
    ```
*   **Pass/Fail Criterion:**
    *   `range_plus_1_year.start_date` must be `DATE_TRUNC(CURRENT_DATE(), YEAR)` (e.g., '20230101'), `range_plus_1_year.end_date` must be `LAST_DAY(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 YEAR), MONTH)` (e.g., '20241231').
    *   `range_minus_1_year.start_date` must be `DATE_TRUNC(CURRENT_DATE(), YEAR)` (e.g., '20230101'), `range_minus_1_year.end_date` must be `LAST_DAY(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL -1 YEAR), MONTH)` (e.g., '20221231').
*   **Test Code (SQL Assertion - using a fixed date for `CURRENT_DATE()` for predictability):**
    ```sql
    WITH TestData AS (
      SELECT
        dw_utils.get_date_range(1, 'Y', 'YYYYMMDD') AS actual_plus_1,
        dw_utils.get_date_range(-1, 'Y', 'YYYYMMDD') AS actual_minus_1
    )
    SELECT
        actual_plus_1.start_date AS actual_plus_1_start, '20230101' AS expected_plus_1_start,
        actual_plus_1.end_date AS actual_plus_1_end, '20241231' AS expected_plus_1_end,
        actual_minus_1.start_date AS actual_minus_1_start, '20230101' AS expected_minus_1_start,
        actual_minus_1.end_date AS actual_minus_1_end, '20221231' AS expected_minus_1_end,
        CASE
            WHEN actual_plus_1.start_date = '20230101' AND actual_plus_1.end_date = '20241231'
             AND actual_minus_1.start_date = '20230101' AND actual_minus_1.end_date = '20221231' THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```

### Test Case: `DWDate_Gib_Zeitraum` / `dw_utils.get_date_range` - Invalid Inputs

*   **Purpose:** Verify NULL handling for invalid unit or NULL inputs.
*   **Setup:** Same as above.
*   **Action (Legacy):**
    ```bash
    . h_alis_date.ksh
    DWDate_Gib_Zeitraum 1 "X" "YYYYMMDD" START_VAR END_VAR # Invalid unit
    echo $?
    DWDate_Gib_Zeitraum 1 "D" "YYYYMMDD" # Missing output vars
    echo $?
    ```
*   **Expected Output (Legacy):** `1` (for invalid unit or argument count).
*   **Action (Migrated):**
    ```sql
    SELECT
        dw_utils.get_date_range(1, 'X', 'YYYYMMDD') AS invalid_unit,
        dw_utils.get_date_range(NULL, 'D', 'YYYYMMDD') AS null_offset,
        dw_utils.get_date_range(1, NULL, 'YYYYMMDD') AS null_unit,
        dw_utils.get_date_range(1, 'D', NULL) AS null_format;
    ```
*   **Pass/Fail Criterion:** All BigQuery UDF outputs must be `NULL`.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        dw_utils.get_date_range(1, 'X', 'YYYYMMDD') IS NULL AS invalid_unit_is_null,
        dw_utils.get_date_range(NULL, 'D', 'YYYYMMDD') IS NULL AS null_offset_is_null,
        dw_utils.get_date_range(1, NULL, 'YYYYMMDD') IS NULL AS null_unit_is_null,
        dw_utils.get_date_range(1, 'D', NULL) IS NULL AS null_format_is_null,
        CASE
            WHEN dw_utils.get_date_range(1, 'X', 'YYYYMMDD') IS NULL
             AND dw_utils.get_date_range(NULL, 'D', 'YYYYMMDD') IS NULL
             AND dw_utils.get_date_range(1, NULL, 'YYYYMMDD') IS NULL
             AND dw_utils.get_date_range(1, 'D', NULL) IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result;
    ```