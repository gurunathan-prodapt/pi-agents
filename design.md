# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh

## 1. Purpose & Scope
This migration job focuses on `h_alis_parameter.ksh`, a KornShell script providing utility routines for parsing, normalizing, and validating parameters related to data systems, key figures, and timeframes. Its primary purpose is to standardize input values and check for valid combinations and formats, as well as calculate date ranges. This script functions as a library of helper functions to be sourced and utilized by other ETL processes. The job was assembled from 1 component, and its stage distribution is noted as complex.

## 2. Source Inventory
The job consists of a single file:
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
  - **Technology:** KornShell (ksh)
  - **Category:** Shell Script
  - **Summary:** Provides utility functions for parsing, validating, and converting parameters related to data systems, key figures, and timeframes. Standardizes input values and checks for valid combinations and formats.
  - **Complexity Tier:** `complex`
  - **Migration Flags:** `["loc-escalated"]` (Lines of Code escalated the complexity)
  - **Automation Bucket:** `manual` (B3)

## 3. Target Architecture
The target platform is Google BigQuery. The functions within `h_alis_parameter.ksh` will be migrated to BigQuery SQL Stored Procedures and User-Defined Functions (UDFs).
- **BigQuery SQL Stored Procedures:** For functions involving complex logic, error handling, and `OUT` parameters (e.g., `pruefeParameterGesetzt`, `konvertiereKennzahl`, `pruefeSystemKennzahl`).
- **BigQuery SQL UDFs:** For simpler, reusable transformations that return a single value (e.g., `normalize_lower`, `is_empty` helper functions).
- **Auxiliary Tables:** Optional mapping/configuration tables might be created to externalize the `CASE` logic used in conversion functions, improving maintainability (e.g., `parameter_kennzahl_map`, `parameter_system_allowlist`).

## 4. Data Flow & Lineage
The original script `h_alis_parameter.ksh` acts as a library of utility functions. Static analysis of lineage did not identify explicit `INVOKES` or `DEPENDS_ON` relationships, suggesting it is typically `sourced` (included) by other KornShell scripts that then call its functions. In the BigQuery target environment, these migrated BigQuery Stored Procedures and UDFs will be invoked by other migrated ETL processes (e.g., within other BigQuery Stored Procedures or directly in SQL queries) that previously sourced the original KornShell script. There are no direct data sources or targets for this utility script itself; it processes parameters passed to its functions.

## 5. Transformation Logic
The transformation will involve converting KornShell functions into equivalent BigQuery SQL Stored Procedures and UDFs. Key aspects of the transformation include:

**General Principles:**
- **Error Handling:** The original script uses `ErrNr` and `ErrArg` global variables for error propagation. In BigQuery, this will be handled via `OUT` parameters for status codes and messages in stored procedures, or by returning `NULL` or raising exceptions for UDFs.
- **Dynamic Variable Indirection (`eval`):** This KornShell construct will be replaced with explicit procedure parameters (IN/OUT) in BigQuery SQL, as BigQuery does not support dynamic variable names in the same manner.
- **Case Sensitivity:** `typeset -l` for lowercase conversion will be handled by BigQuery's `LOWER()` function.
- **Conditional Logic:** `if` and `case` statements will be translated to `IF` and `CASE` statements in BigQuery SQL.
- **Looping:** The `for` loops used in `gibBereich` and `gibIntervall` will be replaced by `CASE` statements with `IN` clauses for membership checking, or by joins against mapping tables.

**Function-by-Function Breakdown:**

1.  **`pruefeParameterGesetzt`**:
    *   **Logic:** Checks if input parameters are set (not empty).
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with `IN` parameters. Empty check using `IS NULL OR TRIM(s) = ''`. Sets `ErrNr` and `ErrArg` `OUT` parameters.

2.  **`konvertiereKennzahl`**:
    *   **Logic:** Converts descriptive `Kennzahl` names to canonical short codes using a `case` statement.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with an `INOUT` parameter for `Kennzahl`. `CASE` expression for mapping. Sets `ErrNr` and `ErrArg` `OUT` parameters if an unknown value is encountered.

3.  **`konvertiereSystem`**:
    *   **Logic:** Converts descriptive `System` names to canonical short codes or keeps them as is if already canonical.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with an `INOUT` parameter for `System`. `CASE` expression for mapping. Sets `ErrNr` and `ErrArg` `OUT` parameters for unknown systems.

4.  **`konvertiereSDName`**:
    *   **Logic:** Converts descriptive `Stammdaten (SD)` names to abbreviations.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with an `INOUT` parameter for `System`. `CASE` expression for mapping. Sets `ErrNr` and `ErrArg` `OUT` parameters for unknown SD sources.

5.  **`konvertiereAufbStufeXtra`**:
    *   **Logic:** Converts `Aufbereitungsstufen` (processing stages) to normalized abbreviations.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with an `INOUT` parameter for `Stufe`. `CASE` expression for mapping. Sets `ErrNr` and `ErrArg` `OUT` parameters for unknown stages.

6.  **`pruefeSystemKennzahl`**:
    *   **Logic:** Validates whether specific `System` and `Kennzahl` combinations are allowed using complex conditional logic (`if/elif`).
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with `IN` parameters. Nested `IF/ELSEIF` or `CASE` with `IN` clauses for combination checks. Sets `ErrNr` and `ErrArg` `OUT` parameters if an invalid combination is found.

7.  **`gibBereich`**:
    *   **Logic:** Determines a logical `Bereich` (area/category) based on the input `Kennzahl` by checking membership in predefined lists.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with `IN` `Kennzahl` and `OUT` `VarBereich` parameters. `CASE` statement with `IN` clauses for membership. Sets `ErrNr` and `ErrArg` `OUT` parameters for unknown `Kennzahl`s.

8.  **`gibIntervall`**:
    *   **Logic:** Determines a logical `Intervall` (interval, 't' for daily, 'm' for monthly) based on the input `Kennzahl`.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with `IN` `Kennzahl` and `OUT` `VarIntervall` parameters. `CASE` statement with `IN` clauses for membership. Sets `ErrNr` and `ErrArg` `OUT` parameters for unknown `Kennzahl`s.

9.  **`pruefeZeitraum`**:
    *   **Logic:** Validates if two `YYYYMMDD` formatted dates represent a valid period (both present, valid format, start <= end). Depends on external `DWDate_Datum_Check` and `DWDate_Datum_LE`.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with `IN` parameters. Uses `SAFE.PARSE_DATE('%Y%m%d', date_string)` for format validation and conversion. Direct comparison for start <= end. Sets `ErrNr` and `ErrArg` `OUT` parameters.

10. **`pruefeZahlPositiv`**:
    *   **Logic:** Checks if a given parameter is a positive numeric value.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with `IN` parameters. Uses `SAFE_CAST(value AS INT64)` to check for numeric validity and then checks if `n < 0`. Sets `ErrNr` and `ErrArg` `OUT` parameters.

11. **`pruefeZeitParameter`**:
    *   **Logic:** Validates mutually exclusive input patterns: either date range (`Anfangsdatum`, `Endedatum`) or `Zeitspanne` (timespan).
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with `IN` parameters. Uses `IF/ELSE` to handle the conditional logic. Calls `pruefeZahlPositiv` and `pruefeZeitraum` procedures as sub-routines. Sets `ErrNr` and `ErrArg` `OUT` parameters for invalid combinations.

12. **`konvertiereZeitspanne`**:
    *   **Logic:** Calculates `Anfangsdatum` and `Endedatum` based on a numeric `Zeitspanne` and `Kennzahl` (which determines the unit: day 'D' or month 'M'). Depends on external `DWDate_Gib_Zeitraum`.
    *   **BigQuery Equivalent:** `CREATE PROCEDURE` with `INOUT` parameters for `p_VarAnfang`, `p_VarEnde`, and `IN` parameters for `p_Spanne`, `p_Kennzahl`. Uses BigQuery's `CURRENT_DATE()`, `DATE_SUB()` and `FORMAT_DATE()` functions to compute the dates. Sets `ErrNr` and `ErrArg` `OUT` parameters for errors.

## 6. External Dependencies
The `lineage_assembled_jobs` record indicated no external systems. However, the analysis of the script content reveals implicit external dependencies:
- **External Date Helper Functions:** `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum`. These are not defined within `h_alis_parameter.ksh` but are called by `pruefeZeitraum` and `konvertiereZeitspanne`.
  - **Replacement Strategy:** These will be replaced by native BigQuery date and time functions such as `PARSE_DATE`, `FORMAT_DATE`, `DATE_ADD`, `DATE_SUB`, and standard SQL comparison operators. If the exact complex logic of these legacy functions cannot be precisely replicated with standard BigQuery functions, a minimal Python UDF could be considered as a last resort, though BigQuery SQL is preferred for date manipulation.
- **Temporary Filesystem Usage:** The `pruefeZeitraum` and `konvertiereZeitspanne` functions create and delete temporary log files in `/tmp`.
  - **Replacement Strategy:** This shell-specific behavior is not directly transferable. Error messages will be captured via `OUT` parameters or structured logging within BigQuery.

## 7. Unresolved / Risks
- **`manual` Migration Bucket:** The job is categorized for manual migration, indicating significant effort due to its complexity and the use of shell-specific constructs.
- **`loc-escalated` Flag:** The high Lines of Code (LOC) for a utility script implies intricate logic, which contributes to its "complex" tier and manual migration bucket.
- **Dynamic Variable `eval`:** The use of `eval` for dynamic variable access is a core shell feature not directly supported in BigQuery SQL. This requires careful refactoring into explicit procedure parameters.
- **External `DWDate_*` Function Exact Semantics:** While BigQuery has rich date functions, ensuring the exact behavior (e.g., error codes, edge cases) of the undocumented `DWDate_*` external functions may require thorough testing and potential minor adjustments.
- **Global Error State (`ErrNr`, `ErrArg`):** The global nature of these error variables in the shell script needs to be carefully translated into structured error handling within BigQuery stored procedures, likely using `OUT` parameters.

## 8. Build Plan
The build plan involves creating BigQuery SQL DDL statements for the procedures and functions.

1.  **Define Helper UDFs:**
    *   `normalize_lower(s STRING) RETURNS STRING`
    *   `is_empty(s STRING) RETURNS BOOL`

2.  **Create BigQuery Stored Procedures:**
    *   `pruefeParameterGesetzt(IN param_name STRING, IN param_var STRING, IN param_wert STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `konvertiereKennzahl(INOUT Kennzahl STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `konvertiereSystem(INOUT System STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `konvertiereSDName(INOUT System STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `konvertiereAufbStufeXtra(INOUT Stufe STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `pruefeSystemKennzahl(IN System STRING, IN Kennzahl STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `gibBereich(IN Kennzahl STRING, OUT VarBereich STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `gibIntervall(IN Kennzahl STRING, OUT VarIntervall STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `pruefeZeitraum(IN Anfang STRING, IN Ende STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `pruefeZahlPositiv(IN p_Zahl STRING, IN p_ParameterName STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `pruefeZeitParameter(IN p_Anfangsdatum STRING, IN p_Endedatum STRING, IN p_ZeitOffset STRING, OUT ErrNr INT64, OUT ErrArg STRING)`
    *   `konvertiereZeitspanne(INOUT p_VarAnfang STRING, INOUT p_VarEnde STRING, IN p_Spanne STRING, IN p_Kennzahl STRING, OUT ErrNr INT64, OUT ErrArg STRING)`

3.  **Deployment:**
    *   The generated DDL for procedures and UDFs will be deployed to the target BigQuery dataset.
    *   Any calling scripts or jobs that previously sourced `h_alis_parameter.ksh` will be modified to invoke these new BigQuery Stored Procedures and UDFs directly.

**Target Language:** BigQuery SQL (for Stored Procedures and UDFs).