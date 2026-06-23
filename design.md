# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh

## 1. Purpose & Scope
This KornShell script, `h_alis_date.ksh`, serves as a utility library for date calculations and validations within a legacy data warehouse environment. Its primary function is to provide shell functions that perform date arithmetic (e.g., getting the previous month, checking date validity, comparing dates, adding days to a date, calculating month-end, determining days in a month) by leveraging an Oracle database via `sqlplus`. The script is designed to be sourced by other shell scripts, making these date utility functions available for use. This migration aims to port these functionalities to Google Cloud's BigQuery platform.

## 2. Source Inventory
The primary source for this migration is a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
    *   **Technology:** KornShell (KSH), Oracle SQL/PLSQL (embedded and invoked via `sqlplus`)
    *   **Summary:** Provides date utility functions using Oracle `sqlplus` for database interactions.
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Automatic (B2) - due to the mix of shell scripting, embedded SQL, and external SQL file invocations, requiring careful translation to BigQuery-compatible logic and potential Python orchestration.

## 3. Target Architecture
The target platform is Google BigQuery. The functions provided by `h_alis_date.ksh` will be re-implemented as BigQuery SQL user-defined functions (UDFs) or BigQuery stored procedures, depending on their complexity and statefulness. Orchestration of these functions, if necessary (e.g., for sequences of calls or parameter handling), will be managed by Python scripts (e.g., using `apache-beam` or `airflow` with BigQuery operators) or directly within BigQuery scripting.

*   **BigQuery Datasets:**
    *   A dedicated dataset (e.g., `dw_utils`) will be created to house the migrated date utility functions.
*   **BigQuery Routines (UDFs/Stored Procedures):**
    *   Each KornShell function will be translated into an equivalent BigQuery SQL UDF or Stored Procedure.
*   **Orchestration (Optional):**
    *   If any existing job directly calls `h_alis_date.ksh` as a standalone script (which is unlikely given its utility nature), a Python wrapper script (e.g., as an Airflow DAG task) might be required to invoke the new BigQuery routines.

## 4. Data Flow & Lineage
The original script's data flow involves:
1.  **Shell script execution:** The `h_alis_date.ksh` script is sourced or executed.
2.  **Parameter passing:** Input parameters are passed to the shell functions.
3.  **Oracle `sqlplus` calls:** Functions call `sqlplus` to execute inline Oracle SQL/PLSQL or external `.sql` files (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`).
4.  **Oracle Database interaction:** SQL queries read from the `DUAL` table and potentially other system tables for date functions. PL/SQL blocks perform date comparisons and raise application errors.
5.  **Output parsing:** The script parses `sqlplus` output from temporary files or direct command output to capture results.
6.  **Shell variable assignment:** Results are assigned to shell variables for further processing or return.

In the BigQuery target architecture:
1.  **BigQuery Function/Procedure Calls:** External processes or other BigQuery scripts will invoke the migrated BigQuery UDFs/Stored Procedures.
2.  **BigQuery Internal Processing:** The UDFs/Stored Procedures will perform date calculations using BigQuery's native date/time functions.
3.  **Result Return:** Results will be returned directly by the BigQuery routines.

The direct `READS_TABLE` edge to `TABLE:DUAL` confirms the interaction with Oracle for basic date operations. The `sqlplus` invocations of `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` indicate these SQL scripts are also part of the date calculation logic and will need to be identified and migrated as well.

## 5. Transformation Logic

Each function in `h_alis_date.ksh` will be transformed as follows:

*   **`DWDate_Vormonat()`:**
    *   **Legacy:** Calls `sqlplus` to execute `d_alis_vormonat.sql` which likely retrieves the previous month's date from Oracle.
    *   **Target:** A BigQuery UDF (e.g., `DW_UTILS.get_previous_month(input_date DATE, output_format STRING)`) using `DATE_SUB`, `LAST_DAY`, and `FORMAT_DATE` functions to achieve the equivalent logic. The `d_alis_vormonat.sql` script's logic will need to be integrated or converted into a separate BigQuery UDF/Procedure if it contains complex logic.

*   **`DWDate_Datum_Check()`:**
    *   **Legacy:** Uses `sqlplus` to attempt `TO_DATE('$wert','$format') FROM dual` in Oracle. Returns 0 for valid, 1 for invalid.
    *   **Target:** A BigQuery UDF (e.g., `DW_UTILS.is_valid_date(date_string STRING, date_format STRING) RETURNS BOOL`) that attempts to parse the date using `PARSE_DATE` or `SAFE.PARSE_DATE`. If `SAFE.PARSE_DATE` returns `NULL`, the date is invalid.

*   **`DWDate_Datum_LE()`:**
    *   **Legacy:** Uses `sqlplus` with an anonymous PL/SQL block to compare two dates (`datum1`, `datum2` in YYYYMMDD format) and raises an error if `datum1 > datum2`. Returns 0 if `P1 <= P2`.
    *   **Target:** A BigQuery UDF (e.g., `DW_UTILS.is_date_le(date_string1 STRING, date_string2 STRING) RETURNS BOOL`) that parses the date strings (e.g., using `PARSE_DATE('%Y%m%d', date_string1)`) and performs a direct comparison (`parsed_date1 <= parsed_date2`). Error handling can be managed by the calling process or the UDF can return `NULL` for invalid date inputs.

*   **`DWDate_Gib_Zeitraum()`:**
    *   **Legacy:** Calls `sqlplus` to execute `d_alis_datum_zeitraum.sql` with offset, unit (Y/M/D), and format. Parses output from a temporary file.
    *   **Target:** This is the most complex function involving start/end points based on `SYSDATE` and an offset, handling month/year boundaries (first day/last day). This could be a BigQuery Stored Procedure or a more complex UDF (e.g., `DW_UTILS.get_date_range(offset INT64, unit STRING, output_format STRING)`). It will use `CURRENT_DATE()`, `DATE_ADD`, `DATE_SUB`, `LAST_DAY`, `EXTRACT(YEAR/MONTH)`, `MAKE_DATE` to recreate the logic. The logic within `d_alis_datum_zeitraum.sql` needs to be fully analyzed and converted.

*   **`LetzterTagDesMonat()`:**
    *   **Legacy:** Shell-based leap year calculation and array lookup.
    *   **Target:** A BigQuery UDF (e.g., `DW_UTILS.is_last_day_of_month(date_string STRING) RETURNS BOOL`) using `LAST_DAY(PARSE_DATE('%Y%m%d', date_string))` and comparing with the day part of the input date. Leap year logic is implicitly handled by BigQuery's date functions.

*   **`TageimMonat()`:**
    *   **Legacy:** Shell-based leap year calculation and array lookup.
    *   **Target:** A BigQuery UDF (e.g., `DW_UTILS.days_in_month(year INT64, month INT64) RETURNS INT64`) that can calculate the number of days in a given month and year using BigQuery's date functions (e.g., `EXTRACT(DAY FROM LAST_DAY(MAKE_DATE(year, month, 1)))`).

*   **`AddiereDatum()`:**
    *   **Legacy:** Pure shell arithmetic for adding days, with complex loops for month and year overflows, including handling leap years.
    *   **Target:** A BigQuery UDF (e.g., `DW_UTILS.add_days_to_date(date_string STRING, days_to_add INT64) RETURNS STRING`) using `DATE_ADD(PARSE_DATE('%Y%m%d', date_string), INTERVAL days_to_add DAY)` and then `FORMAT_DATE('%Y%m%d', ...)` for output. BigQuery handles date arithmetic and leap years automatically.

All parsing of `sqlplus` output and temporary file handling in the original shell script will be eliminated. Error handling (e.g., parameter validation) in the shell script will be translated to BigQuery's error handling mechanisms or pre-validation in calling scripts/procedures.

## 6. External Dependencies
The script has a significant dependency on an Oracle database.

*   **Oracle Database:**
    *   **Original Role:** Executes SQL and PL/SQL code via `sqlplus` for date calculations and validations. It uses `DUAL` and potentially other implicit system functions/tables. Also executes external SQL scripts like `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`.
    *   **Replacement in BigQuery:**
        *   All Oracle-specific SQL/PLSQL logic will be rewritten using BigQuery's standard SQL and date/time functions.
        *   The `DUAL` table usage is not needed in BigQuery, as expressions can be evaluated directly (e.g., `SELECT CURRENT_DATE()`).
        *   The external SQL scripts (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`) must be located, their content analyzed, and their logic converted into corresponding BigQuery UDFs or integrated directly into the new BigQuery routines derived from `h_alis_date.ksh`.
        *   Environment variables `DW_ORAUSER` and `DW_DIR_ROOT` will become irrelevant in the BigQuery context. Any configuration they provided will be replaced by direct BigQuery dataset/table references or parameters to BigQuery routines.

## 7. Unresolved / Risks
*   **Missing External SQL Content:** The content of `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` was not available in this analysis. Their logic is critical for a complete migration. Without these, the corresponding BigQuery routines cannot be fully implemented.
*   **Usage Patterns:** The current analysis only covers the migration of the utility script itself. How other calling scripts use `h_alis_date.ksh` (e.g., parameters passed, return value expectations, error handling) needs to be understood to ensure seamless integration of the new BigQuery routines into existing data pipelines.
*   **Performance:** Shell scripting with `sqlplus` calls can be slow due to process invocation overhead. Migrating to native BigQuery functions/procedures should improve performance, but careful testing is required.
*   **Error Handling:** The original script uses shell return codes and `sqlplus` exit statuses. This needs to be translated to BigQuery's error handling mechanisms or Python's exception handling for any orchestration layer. The PL/SQL `raise_application_error` in `DWDate_Datum_LE` specifically needs a BigQuery equivalent or a Python-based error mechanism.
*   **Global Variables/Environment:** The script relies on `DW_ORAUSER` and `DW_DIR_ROOT`. The values of these variables and how they're set in the legacy environment are needed to understand the full context of the Oracle connections and file paths.

## 8. Build Plan
1.  **Analyze Dependent SQL Files:**
    *   Locate and analyze the content of `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`. Extract their logic and parameters.
2.  **Define BigQuery Dataset:**
    *   Create a BigQuery dataset, e.g., `dw_utils`, to house the date utility routines.
    *   Language: BigQuery DDL
3.  **Translate `LetzterTagDesMonat` to BigQuery UDF:**
    *   Create a BigQuery SQL UDF `DW_UTILS.is_last_day_of_month(date_string STRING) RETURNS BOOL`.
    *   Language: BigQuery SQL
4.  **Translate `TageimMonat` to BigQuery UDF:**
    *   Create a BigQuery SQL UDF `DW_UTILS.days_in_month(year INT64, month INT64) RETURNS INT64`.
    *   Language: BigQuery SQL
5.  **Translate `AddiereDatum` to BigQuery UDF:**
    *   Create a BigQuery SQL UDF `DW_UTILS.add_days_to_date(date_string STRING, days_to_add INT64) RETURNS STRING`.
    *   Language: BigQuery SQL
6.  **Translate `DWDate_Datum_Check` to BigQuery UDF:**
    *   Create a BigQuery SQL UDF `DW_UTILS.is_valid_date(date_string STRING, date_format STRING) RETURNS BOOL`.
    *   Language: BigQuery SQL
7.  **Translate `DWDate_Datum_LE` to BigQuery UDF:**
    *   Create a BigQuery SQL UDF `DW_UTILS.is_date_le(date_string1 STRING, date_string2 STRING) RETURNS BOOL`.
    *   Language: BigQuery SQL
8.  **Translate `DWDate_Vormonat` to BigQuery UDF/Procedure:**
    *   Create a BigQuery SQL UDF or Stored Procedure `DW_UTILS.get_previous_month(...)`. This will depend on the complexity of `d_alis_vormonat.sql`.
    *   Language: BigQuery SQL
9.  **Translate `DWDate_Gib_Zeitraum` to BigQuery Stored Procedure/UDF:**
    *   Create a BigQuery SQL Stored Procedure or a more complex UDF `DW_UTILS.get_date_range(...)`. This will depend heavily on the logic in `d_alis_datum_zeitraum.sql`.
    *   Language: BigQuery SQL
10. **Refactor Calling Scripts (if any):**
    *   Identify any legacy scripts that `source` or `call` `h_alis_date.ksh` functions.
    *   Rewrite these calling scripts to invoke the new BigQuery routines, potentially using Python (e.g., `google-cloud-bigquery` client library) within an Airflow DAG.
    *   Language: Python (for orchestration), BigQuery SQL (for direct calls if applicable).
11. **Testing:**
    *   Develop comprehensive unit tests for each BigQuery routine, comparing output with the legacy script's behavior.
    *   Develop integration tests for calling scripts to ensure end-to-end functionality.