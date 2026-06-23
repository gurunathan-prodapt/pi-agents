# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh

## 1. Purpose & Scope
The original KornShell script `h_alis_date.ksh` serves as a utility library for date-related operations within the legacy data warehouse environment. Its primary purpose is to provide helper functions for calculating previous months, validating dates, comparing dates, deriving date periods, identifying month-ends, calculating days in a month, and adding days to a date. These functions were critical for various ETL processes that required precise date manipulation, often leveraging an Oracle database via SQL*Plus for more complex calculations.

The scope of this migration is to re-implement the functionality of `h_alis_date.ksh` in Google BigQuery, ensuring equivalent behavior and providing a set of callable date utility functions for dependent BigQuery ETL processes.

## 2. Source Inventory
The job consists of a single source file:
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
- **Technology:** KornShell script with embedded Oracle SQL*Plus calls and shell arithmetic.
- **Complexity Tier:** medium
- **Automation Bucket:** semi_auto
- **File Purpose:** Utility/Helper functions for date manipulation.

## 3. Target Architecture
The target architecture in BigQuery will involve:
- **BigQuery Stored Procedures:** For functions that require multiple steps, `OUT` parameters, or error handling (e.g., `DWDate_Vormonat`, `DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`, `AddiereDatum`). These will encapsulate the logic and provide a clean interface.
- **BigQuery User-Defined Functions (UDFs):** For simpler, idempotent date calculations that return a single value (e.g., `LetzterTagDesMonats`, `TageimMonat`).
- **Standard BigQuery Date Functions:** Extensive use of native BigQuery functions like `DATE_SUB`, `DATE_ADD`, `PARSE_DATE`, `FORMAT_DATE`, `LAST_DAY`, `DATE_TRUNC`, and `EXTRACT` to replace custom shell and Oracle SQL logic.
- **Error Handling:** BigQuery scripting constructs like `ASSERT` or `BEGIN...EXCEPTION` blocks will replace Oracle's `raise_application_error` and shell's return codes.

## 4. Data Flow & Lineage
The original script's data flow involves:
- **Input:** Function parameters passed via shell arguments, and environment variables (`DW_ORAUSER`, `DW_DIR_ROOT`).
- **Processing:** Execution of shell arithmetic, calls to `sqlplus` with embedded or external SQL, and temporary file operations.
- **Output:** Values assigned to shell variables via `eval`, return codes, and printed output.

In the BigQuery target, the data flow will be streamlined:
- **Input:** Stored procedures and UDFs will accept strongly typed input parameters (e.g., `STRING`, `INT64`).
- **Processing:** Logic will be executed natively within BigQuery using SQL scripting, date functions, and UDFs. Temporary file operations and `eval` are eliminated.
- **Output:** Stored procedures will return results via `OUT` parameters, and UDFs will return values directly, aligning with BigQuery's functional programming paradigm.

## 5. Transformation Logic

**Original Function: `DWDate_Vormonat`**
- **Legacy Logic:** Calls `sqlplus` to execute `d_alis_vormonat.sql`, which likely calculates the previous month in Oracle. The result is read from a temporary file and assigned to a shell variable.
- **Target Logic (BigQuery Stored Procedure):**
  - **Concept:** Direct calculation using BigQuery's `DATE_SUB`.
  - **Implementation:** `CREATE OR REPLACE PROCEDURE dataset.DWDate_Vormonat(IN VarName STRING, IN DWDate_FMT STRING, OUT result_value STRING) ... SET result_value = FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH));` (Adjust `DWDate_FMT` mapping for specific Oracle format requirements).

**Original Function: `DWDate_Datum_Check`**
- **Legacy Logic:** Uses `sqlplus` to attempt an `Oracle TO_DATE` conversion. The success or failure of this conversion determines if the date is valid, indicated by the `sqlplus` exit status.
- **Target Logic (BigQuery Stored Procedure):**
  - **Concept:** Utilize `PARSE_DATE` within a `BEGIN...EXCEPTION` block to check for valid date parsing.
  - **Implementation:** `CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_Check(IN wert STRING, IN format STRING, OUT is_valid BOOL) ... BEGIN SET parsed_date = PARSE_DATE(format, wert); SET is_valid = TRUE; EXCEPTION WHEN ERROR THEN SET is_valid = FALSE; END;`

**Original Function: `DWDate_Datum_LE`**
- **Legacy Logic:** Compares two dates using an embedded Oracle PL/SQL block. If the first date is greater, it raises an `application_error`.
- **Target Logic (BigQuery Stored Procedure):**
  - **Concept:** Parse dates and use BigQuery's `ASSERT` statement for validation.
  - **Implementation:** `CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_LE(IN datum1 STRING, IN datum2 STRING, OUT is_less_or_equal BOOL) ... DECLARE d1 DATE; DECLARE d2 DATE; SET d1 = PARSE_DATE('%Y%m%d', datum1); SET d2 = PARSE_DATE('%Y%m%d', datum2); IF d1 > d2 THEN ASSERT FALSE AS CONCAT('Datum ', datum1, ' ist groesser als ', datum2); END IF; SET is_less_or_equal = TRUE;`

**Original Function: `DWDate_Gib_Zeitraum`**
- **Legacy Logic:** Calls `sqlplus` to execute `d_alis_datum_zeitraum.sql` with offset and level parameters. The script parses the output from a temporary file to extract start and end dates.
- **Target Logic (BigQuery Stored Procedure):**
  - **Concept:** Replicate the period calculation logic directly using BigQuery date arithmetic and functions like `DATE_TRUNC`, `LAST_DAY`, `DATE_ADD`.
  - **Implementation:** `CREATE OR REPLACE PROCEDURE dataset.DWDate_Gib_Zeitraum(IN Offset INT64, IN Stufe STRING, IN Format STRING, OUT Var_Start STRING, OUT Var_Ende STRING) ... (contains IF/ELSEIF blocks for Stufe 'D', 'M', 'Y' with corresponding DATE_ADD, DATE_TRUNC, LAST_DAY operations)`

**Original Function: `LetzterTagDesMonats`**
- **Legacy Logic:** Shell script logic to parse date, determine leap year, and compare the day part with an array of month-end days.
- **Target Logic (BigQuery UDF):**
  - **Concept:** Direct comparison using `LAST_DAY` function.
  - **Implementation:** `CREATE OR REPLACE FUNCTION dataset.LetzterTagDesMonats(date_yyyymmdd STRING) RETURNS BOOL AS (DATE(PARSE_DATE('%Y%m%d', date_yyyymmdd)) = LAST_DAY(PARSE_DATE('%Y%m%d', date_yyyymmdd)));`

**Original Function: `TageimMonat`**
- **Legacy Logic:** Shell script logic to determine leap year and return days in a month from an array.
- **Target Logic (BigQuery UDF):**
  - **Concept:** Use `LAST_DAY` and `EXTRACT(DAY FROM ...)` to get days in month.
  - **Implementation:** `CREATE OR REPLACE FUNCTION dataset.TageimMonat(year_int INT64, month_int INT64) RETURNS INT64 AS (EXTRACT(DAY FROM LAST_DAY(DATE(year_int, month_int, 1))));`

**Original Function: `AddiereDatum`**
- **Legacy Logic:** Shell script performs iterative day, month, and year arithmetic with manual overflow handling for months and years.
- **Target Logic (BigQuery Stored Procedure):**
  - **Concept:** Utilize BigQuery's `DATE_ADD` function for direct date addition.
  - **Implementation:** `CREATE OR REPLACE PROCEDURE dataset.AddiereDatum(IN date_yyyymmdd STRING, IN days_to_add INT64, OUT result_date STRING) ... DECLARE d DATE DEFAULT PARSE_DATE('%Y%m%d', date_yyyymmdd); SET result_date = FORMAT_DATE('%Y%m%d', DATE_ADD(d, INTERVAL days_to_add DAY));`

## 6. External Dependencies
**Original:**
- **Oracle Database:** Accessed via `sqlplus` using the `$DW_ORAUSER` connection string. This is a direct dependency.
- **External SQL Files:** `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` are invoked by the shell script through `sqlplus`. These contain Oracle-specific SQL logic.
- **Temporary Files:** Used for inter-process communication (passing results from `sqlplus` back to the shell script).

**Target Replacement Strategy:**
- The Oracle database dependency will be eliminated. All date calculation logic will be migrated into native BigQuery Stored Procedures and UDFs.
- The external SQL files will be refactored and integrated directly into the BigQuery Stored Procedures. Their Oracle-specific SQL will be translated to BigQuery SQL.
- Temporary files and the parsing of their contents will no longer be necessary, as results will be returned directly via `OUT` parameters or function return values in BigQuery.
- Environment variables like `DW_ORAUSER` and `DW_DIR_ROOT` will become obsolete in the BigQuery context. Any configurable parameters will be handled via BigQuery procedure/function arguments or BigQuery connection/execution parameters.

## 7. Unresolved / Risks
- **`eval`-based variable assignment:** The original script heavily relies on `eval` to dynamically assign values to caller variables. BigQuery Stored Procedures will use `OUT` parameters to return values, which is a different paradigm and requires careful refactoring of the calling code in the target environment.
- **Oracle-specific date formatting and behavior:** While BigQuery offers robust date functions, subtle differences in how Oracle handles specific date formats or edge cases (e.g., date validation behavior) might require detailed mapping and testing to ensure exact functional equivalence. The `DWDate_FMT` parameter might need a dedicated mapping function if the Oracle format strings are complex.
- **Missing `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` code:** The actual content of these external SQL files was not available during analysis. The proposed BigQuery equivalents are based on the presumed functionality. A more precise migration would require reviewing the content of these SQL files.
- **Error handling granularity:** Oracle's `raise_application_error` provides specific error codes. BigQuery's `ASSERT` provides a message, but a more structured error handling mechanism might be needed if downstream systems rely on specific error codes.

## 8. Build Plan
The migration will involve creating the following BigQuery components, likely within a dedicated `dataset`:

1.  **BigQuery Stored Procedure: `dataset.DWDate_Vormonat`**
    - **Language:** BigQuery SQL
    - **Purpose:** Calculates the previous month's date.

2.  **BigQuery Stored Procedure: `dataset.DWDate_Datum_Check`**
    - **Language:** BigQuery SQL
    - **Purpose:** Validates if a given string represents a valid date in a specified format.

3.  **BigQuery Stored Procedure: `dataset.DWDate_Datum_LE`**
    - **Language:** BigQuery SQL
    - **Purpose:** Compares two dates and asserts if the first is not less than or equal to the second.

4.  **BigQuery Stored Procedure: `dataset.DWDate_Gib_Zeitraum`**
    - **Language:** BigQuery SQL
    - **Purpose:** Calculates start and end dates for a period based on an offset and a level (Day, Month, Year).

5.  **BigQuery UDF: `dataset.LetzterTagDesMonats`**
    - **Language:** BigQuery SQL
    - **Purpose:** Checks if a given date is the last day of its month.

6.  **BigQuery UDF: `dataset.TageimMonat`**
    - **Language:** BigQuery SQL
    - **Purpose:** Returns the number of days in a given month and year.

7.  **BigQuery Stored Procedure: `dataset.AddiereDatum`**
    - **Language:** BigQuery SQL
    - **Purpose:** Adds a specified number of days to a given date.

*(Optional helper functions if needed after further analysis):*
8.  **BigQuery UDF: `dataset.IsLeapYear`**
    - **Language:** BigQuery SQL
    - **Purpose:** Determines if a given year is a leap year.

9.  **BigQuery UDF: `dataset.OracleFormatToBigQuery`**
    - **Language:** BigQuery SQL
    - **Purpose:** Maps Oracle date format strings to BigQuery format strings. (To be developed if `DWDate_FMT` values are varied and require dynamic mapping).