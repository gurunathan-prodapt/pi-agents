# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `h_alis_parameter.ksh`. This script serves as a utility module containing helper routines for parsing, normalizing, validating, and converting various parameters within a data warehousing environment. Its primary functions include standardizing key figure names (`Kennzahl`), source system names (`System`), master data source names (`SDName`), and Xtra preparation stages (`AufbStufeXtra`). It also provides validation for system-key figure combinations, derivation of metric groups and intervals, and consistency checks for date and time parameters.

The module itself does not perform data processing or generate data; rather, it provides a library of functions to be invoked by other scripts or processes. The migration aims to translate this core business logic into the Google Cloud Platform, specifically targeting BigQuery-compatible components to ensure interoperability with future data pipelines.

## 2. Source Inventory
| File Name | Technology | Complexity Tier | Migration Bucket | Notes |
| :---------------------------------------------------------------------- | :--------- | :-------------- | :--------------- | :---- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | KornShell  | complex         | manual           | This script is a library of functions for parameter validation and conversion. Marked as "loc-escalated". |

## 3. Target Architecture
The target platform is Google BigQuery. Given the nature of the source script as a library of functions for validation and conversion, a direct one-to-one translation to BigQuery SQL may not be the most efficient or idiomatic approach for all functions. Instead, a hybrid approach is recommended:

*   **BigQuery User-Defined Functions (UDFs) (SQL or JavaScript)**: For simpler, stateless conversion or validation logic that operates on individual values (e.g., `konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `konvertiereAufbStufeXtra`, `gibBereich`, `gibIntervall`). SQL UDFs are preferred for performance if the logic is easily expressible in SQL; otherwise, JavaScript UDFs can be used.
*   **Python Module / Cloud Functions**: For more complex logic involving external dependencies, error handling states, or date manipulations that are more naturally expressed in Python (e.g., `pruefeZeitraum`, `pruefeZahlPositiv`, `pruefeZeitParameter`, `konvertiereZeitspanne`). This Python module could then be invoked by other data pipelines (e.g., Dataflow jobs, Cloud Functions, Airflow DAGs) that interact with BigQuery.
*   **Error Handling**: The current `ErrNr`/`ErrArg` global state management in the shell script would be replaced by standard exception handling in Python or error codes/status returns in UDFs.

All migrated components will reside within a designated BigQuery dataset (e.g., `your_project.utility_functions`).

## 4. Data Flow & Lineage
The source script `h_alis_parameter.ksh` is a utility module; it does not have explicit data inputs or outputs in the traditional sense of a data pipeline. No direct `READS` or `WRITES` edges were found in the lineage analysis. Its "data flow" is entirely internal to the calling processes, which would pass parameters to its functions and receive processed/validated parameters in return.

In the target architecture, this logic will be exposed as callable functions/procedures. Other ETL jobs or applications would invoke these BigQuery UDFs or Python functions to perform parameter normalization and validation before or during data processing.

## 5. Transformation Logic
Each function from the original KornShell script will be re-implemented in the target environment.

### 5.1 Parameter Validation and Conversion Functions

*   **`pruefeParameterGesetzt (param_name, param_var)`**
    *   **Logic**: Checks if an environment variable `param_var` (referenced by its name) is set and not empty. If empty, sets an error.
    *   **Target Implementation**: This function's core logic (checking if a value is non-empty) can be implemented as a BigQuery SQL UDF, taking the parameter value directly. The `eval` mechanism would need to be replaced by direct parameter passing. Error signaling would be via returning a `BOOLEAN` (isValid) and an `ERROR_MESSAGE` string, or by raising an error directly within a Python function.

*   **`konvertiereKennzahl (param_var)`**
    *   **Logic**: Converts descriptive key figure names (e.g., "zugang") into standardized short codes (e.g., "zug") using a `case` statement.
    *   **Target Implementation**: Best suited for a **BigQuery SQL UDF**. This can be directly translated into a `CASE` expression in SQL, which is highly efficient.
    ```sql
    CREATE OR REPLACE FUNCTION `your_project.utility_functions`.konvertiereKennzahl(kennzahl_desc STRING) RETURNS STRING AS (
      CASE LOWER(kennzahl_desc)
        WHEN 'zugang' THEN 'zug'
        WHEN 'abgang' THEN 'abg'
        -- ... other mappings ...
        WHEN 'glaengenintervall' THEN 'glint'
        ELSE '???' -- Error handling, or return NULL/raise error as per spec
      END
    );
    ```

*   **`konvertiereSystem (param_var)`**
    *   **Logic**: Converts descriptive system names to standardized short codes, also using a `case` statement. Validates against a list of known systems.
    *   **Target Implementation**: Similar to `konvertiereKennzahl`, this is ideal for a **BigQuery SQL UDF** using a `CASE` expression.
    ```sql
    CREATE OR REPLACE FUNCTION `your_project.utility_functions`.konvertiereSystem(system_desc STRING) RETURNS STRING AS (
      CASE LOWER(system_desc)
        WHEN 'sap' THEN 'sap'
        WHEN 'carmen' THEN 'carmen'
        -- ... other mappings ...
        WHEN 'sigma' THEN 'sigma'
        ELSE '???' -- Error handling, or return NULL/raise error as per spec
      END
    );
    ```

*   **`konvertiereSDName (param_var)`**
    *   **Logic**: Converts descriptive master data system names to short codes.
    *   **Target Implementation**: Another strong candidate for a **BigQuery SQL UDF** with a `CASE` expression.

*   **`konvertiereAufbStufeXtra (param_var)`**
    *   **Logic**: Converts Xtra preparation stage names ("zusammenfuehrung", "befuellung") to short codes ("mrg", "fill").
    *   **Target Implementation**: Again, a straightforward **BigQuery SQL UDF** with a `CASE` expression.

### 5.2 Validation Functions

*   **`pruefeSystemKennzahl (param_system, param_kennzahl)`**
    *   **Logic**: Contains complex `if/elif` logic to validate if a combination of system and key figure is allowed.
    *   **Target Implementation**: This can be implemented as a **BigQuery SQL UDF returning BOOLEAN**. The `if/elif` structure translates directly to a `CASE` expression or nested `IF` statements in SQL. This function is critical and contains significant business rules.

*   **`gibBereich (param_kennzahl, var_bereich)`**
    *   **Logic**: Determines a "Bereich" (area/group) based on the `param_kennzahl` by checking against several hardcoded lists (`list_tn`, `list_us`, `list_gd`, etc.).
    *   **Target Implementation**: Best as a **BigQuery SQL UDF**. It can use `ARRAY_CONTAINS` on predefined arrays (or `UNNEST` with `JOIN` for larger lists) within a `CASE` statement.

*   **`gibIntervall (param_kennzahl, var_intervall)`**
    *   **Logic**: Determines an "Intervall" (interval type, 't' for daily or 'm' for monthly) based on `param_kennzahl` from hardcoded lists.
    *   **Target Implementation**: Best as a **BigQuery SQL UDF** using `ARRAY_CONTAINS` within a `CASE` statement.

### 5.3 Date and Numeric Validation/Conversion Functions

*   **`pruefeZahlPositiv (p_zahl, p_parametername)`**
    *   **Logic**: Checks if a given value is numeric and non-negative.
    *   **Target Implementation**: Can be a simple **BigQuery SQL UDF** using `SAFE_CAST` and comparison operators.
    ```sql
    CREATE OR REPLACE FUNCTION `your_project.utility_functions`.pruefeZahlPositiv(value STRING, param_name STRING) RETURNS BOOLEAN AS (
      IF(SAFE_CAST(value AS NUMERIC) IS NOT NULL AND SAFE_CAST(value AS NUMERIC) >= 0, TRUE, FALSE)
      -- Or raise an error using ERROR() if desired behavior is to fail
    );
    ```

*   **`pruefeZeitraum (anfang, ende)`**
    *   **Logic**: Validates if two `YYYYMMDD` formatted dates form a valid period (start <= end). Relies on external functions `DWDate_Datum_Check` and `DWDate_Datum_LE`.
    *   **Target Implementation**: This function, along with others relying on `DWDate_`, will need to be re-implemented. It can be a **BigQuery JavaScript UDF** for more complex date parsing/validation or a **Python function** within a utility module. BigQuery's native date functions (e.g., `PARSE_DATE`, comparison operators) should be leveraged.

*   **`pruefeZeitParameter (p_anfangsdatum, p_endedatum, p_zeitoffset)`**
    *   **Logic**: Validates combinations of start date, end date, and time span (either dates or span, not mixed). Calls `pruefeZahlPositiv` and `pruefeZeitraum`.
    *   **Target Implementation**: Due to its conditional logic and reliance on other date functions, this is best implemented as a **Python function** in a shared utility module.

*   **`konvertiereZeitspanne (p_varanfang, p_varende, p_spanne, p_kennzahl)`**
    *   **Logic**: Calculates start and end dates based on a numeric span and key figure. The unit of span (`D` for Day, `M` for Month) depends on `p_kennzahl` (e.g., 'bst' uses months). Relies on external `DWDate_Gib_Zeitraum`.
    *   **Target Implementation**: This is also best implemented as a **Python function** in a shared utility module, leveraging Python's `datetime` and `timedelta` objects. The logic for determining `Offset_Unit` based on `p_kennzahl` is straightforward.

## 6. External Dependencies
The original script `h_alis_parameter.ksh` has the following explicit external dependencies:

*   **`DWDate_Datum_Check`**: An external utility function for checking date format validity.
*   **`DWDate_Datum_LE`**: An external utility function for comparing two dates (less than or equal).
*   **`DWDate_Gib_Zeitraum`**: An external utility function for calculating date ranges based on a span.

**Migration Strategy for External Dependencies:**
These `DWDate_` functions are critical and will need to be re-implemented.
*   For BigQuery UDFs, direct BigQuery SQL date functions (e.g., `PARSE_DATE('%Y%m%d', date_string)`, date arithmetic `DATE_ADD`, `DATE_SUB`, comparison operators) should replace `DWDate_Datum_Check` and `DWDate_Datum_LE`.
*   For Python functions, Python's `datetime` module (e.g., `datetime.strptime`, `timedelta`, `relativedelta` from `dateutil` for month/year calculations) will be used to replicate the functionality of all `DWDate_` functions.

No other external systems (like Oracle, SFTP, S3) were identified as direct dependencies of this specific KornShell script.

## 7. Unresolved / Risks

*   **Error Handling Replication**: The original script uses a global `ErrNr` and `ErrArg` for error signaling. This pattern is not directly transferable to BigQuery UDFs or Python functions.
    *   **Resolution**: BigQuery UDFs can either return `NULL` for invalid inputs, return a `STRUCT` containing `(value, error_code, error_message)`, or use the `ERROR()` function to fail the query explicitly. Python functions will use standard Python exception handling (`try-except`). The choice depends on the desired behavior of the calling processes. Given the library nature, returning error flags/messages might be more flexible than always failing.
*   **`eval` Command Usage**: The script heavily uses `eval "param_wert=\\$$param_var"` to access variables by name.
    *   **Resolution**: This pattern should be avoided in the migrated code due to security and complexity. Parameters should be passed directly by value or by reference in Python. BigQuery UDFs receive explicit arguments, removing the need for `eval`.
*   **Temporary Files**: `pruefeZeitraum` and `konvertiereZeitspanne` use temporary files for logging external `DWDate_` command output.
    *   **Resolution**: In BigQuery UDFs, standard logging mechanisms or direct error messages should replace this. In Python, standard `logging` to Cloud Logging or direct `stderr` output will be used.
*   **"loc-escalated" complexity flag**: This flag suggests that the script's logic might be more intricate than initially appears, possibly due to subtle shell-specific behaviors or implicit context.
    *   **Mitigation**: Thorough unit testing for each migrated function is crucial to ensure exact behavior replication.
*   **Manual Migration Bucket**: The "manual" classification implies that automated tools are not sufficient. This confirms the need for careful human analysis and re-implementation.

## 8. Build Plan
The migration will involve creating a suite of new components on Google Cloud Platform:

1.  **Develop `date_utils.py` Python Module**:
    *   Re-implement the functionality of `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum` using Python's `datetime` and potentially `dateutil` libraries.
    *   Include functions for `pruefeZeitraum`, `pruefeZahlPositiv`, `pruefeZeitParameter`, and `konvertiereZeitspanne` within this module, leveraging the re-implemented date utilities.
    *   Implement robust exception handling.
    *   **Language**: Python
    *   **Location**: Source code repository for shared Python utilities.

2.  **Create BigQuery SQL UDFs**:
    *   For `konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `konvertiereAufbStufeXtra`, `pruefeSystemKennzahl`, `gibBereich`, `gibIntervall`, `pruefeParameterGesetzt` (adapting error handling).
    *   These UDFs will reside in `your_project.utility_functions` dataset.
    *   **Language**: BigQuery SQL
    *   **Location**: BigQuery, managed via DDL scripts in version control.

3.  **Unit Testing**:
    *   Develop comprehensive unit tests for each Python function and BigQuery UDF to ensure functional equivalence with the original KornShell logic. This is especially important for the complex `pruefeSystemKennzahl` and date calculation functions.
    *   **Language**: Python (for Python module), SQL (for BigQuery UDFs testing).

4.  **Integration Testing**:
    *   If existing KornShell scripts invoke `h_alis_parameter.ksh`, these calling scripts will need to be migrated first. Then, integration tests will ensure that the new BigQuery UDFs and Python utility functions are correctly invoked and behave as expected within the wider data pipeline context.

This phased approach allows for independent testing and deployment of the utility functions, laying a robust foundation for the migration of consuming jobs.