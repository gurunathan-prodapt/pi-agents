# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh

## 1. Purpose & Scope

This migration job involves a single KornShell utility script, `h_alis_parameter.ksh`, which provides a set of helper routines for parsing, validating, and converting various parameters within a larger ETL ecosystem. Its primary function is to normalize and check data-related input parameters (e.g., Kennzahlen, Liefersysteme, date ranges) for downstream processing. It acts as a foundational library for other scripts, rather than a standalone ETL process.

The scope of this migration is to re-implement the functionality of these parameter handling routines to support a BigQuery-centric data platform. This implies converting the KornShell functions into a suitable BigQuery-compatible language, likely Python, to be integrated into data pipelines orchestrated by tools such as Airflow.

## 2. Source Inventory

The job consists of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   **Technology:** KornShell (KornShell script)
    *   **Complexity Tier:** Undetermined (no data in `file_complexity` table)
    *   **Automation Bucket:** B3 (manual)
    *   **Purpose:** Utility script for parameter validation and conversion.

## 3. Target Architecture

The target architecture will involve replacing the KornShell script with a Python module containing equivalent functions. This Python module will reside within the Google Cloud Platform ecosystem, likely deployed as part of an Airflow DAG's supporting code, or as a set of Cloud Functions if standalone invocation is required.

*   **Core Logic:** Re-implement the KornShell functions (`pruefeParameterGesetzt`, `konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `konvertiereAufbStufeXtra`, `pruefeSystemKennzahl`, `gibBereich`, `gibIntervall`, `pruefeZeitraum`, `pruefeZahlPositiv`, `pruefeZeitParameter`, `konvertiereZeitspanne`) as Python functions within a new Python module (e.g., `alis_parameter_utils.py`).
*   **Error Handling:** The current `ErrNr`/`ErrArg` pattern will be replaced with Pythonic exception handling (e.g., raising custom exceptions or using standard Python error types).
*   **Date Functions:** External date utility calls (`DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`) will be replaced with standard Python `datetime` module functionalities or Google Cloud-specific date/time libraries if available and more suitable for BigQuery integration.
*   **Integration:** This Python module will be imported and used by other Python scripts (e.g., Airflow tasks) that prepare and load data into BigQuery. Direct BigQuery UDF conversion is possible for simpler, stateless functions, but the current script's complexity and use of external file operations (for date utilities) suggest a Python library approach is more appropriate.

## 4. Data Flow & Lineage

The original KornShell script `h_alis_parameter.ksh` does not directly process data or interact with data sources/targets. Instead, it provides utility functions that are invoked by other scripts, which in turn might handle data flow. The lineage analysis confirmed no direct `READS` or `WRITES` edges for this file.

In the target architecture:
*   There is no direct "data flow" from this utility module itself.
*   The Python module `alis_parameter_utils.py` will be a dependency for other Python-based ETL jobs (e.g., Airflow DAGs) that perform data transformations and loading into BigQuery.
*   These dependent ETL jobs will call the functions within `alis_parameter_utils.py` to validate and transform their input parameters before executing BigQuery SQL queries or PySpark transformations.

## 5. Transformation Logic

Each KornShell function will be individually converted to an equivalent Python function.

*   **`pruefeParameterGesetzt(param_name, param_var)`:**
    *   **Legacy:** Checks if an environment variable `param_var` (whose name is given by `param_var`) is set and not empty. Sets `ErrNr`/`ErrArg` on failure.
    *   **Target:** A Python function that takes `param_name` and the *value* of the parameter directly. It will raise a `ValueError` or a custom exception if the parameter is empty.
*   **`konvertiereKennzahl(varname)`:**
    *   **Legacy:** Converts a descriptive `Kennzahl` name (read from an environment variable `varname`) to a short code using a `case` statement.
    *   **Target:** A Python function that takes the `Kennzahl` string directly and returns the corresponding short code. A dictionary lookup will replace the `case` statement. Raise an exception for unknown `Kennzahl` values.
*   **`konvertiereSystem(varname)`:**
    *   **Legacy:** Converts a descriptive `System` name to a normalized short code.
    *   **Target:** Similar to `konvertiereKennzahl`, using a dictionary for mapping and returning the normalized system string, raising an exception for unknown systems.
*   **`konvertiereSDName(varname)`:**
    *   **Legacy:** Converts a descriptive `Stammdaten-Liefersystem` (SD Name) to a short code.
    *   **Target:** Similar Python function with dictionary mapping.
*   **`konvertiereAufbStufeXtra(varname)`:**
    *   **Legacy:** Converts an `Aufbereitungsstufe` name to a short code (`mrg`, `fill`).
    *   **Target:** Similar Python function with dictionary mapping.
*   **`pruefeSystemKennzahl(system, kennzahl)`:**
    *   **Legacy:** Validates if a combination of `system` and `kennzahl` is allowed based on hardcoded `if/elif` logic.
    *   **Target:** A Python function that takes `system` and `kennzahl` as arguments and performs the validation. This could be implemented with nested dictionaries or a series of `if/elif` statements. Raise an exception if the combination is invalid.
*   **`gibBereich(kennzahl, varbereich)`:**
    *   **Legacy:** Determines the "Bereich" (area) for a given `kennzahl` by searching predefined lists.
    *   **Target:** A Python function taking `kennzahl` and returning the area code. This can be implemented using multiple lists or sets of `kennzahl` values, and iterating through them to find a match.
*   **`gibIntervall(kennzahl, varintervall)`:**
    *   **Legacy:** Determines the "Intervall" (interval, 't' or 'm') for a given `kennzahl` by searching predefined lists.
    *   **Target:** Similar Python function taking `kennzahl` and returning the interval code.
*   **`pruefeZeitraum(anfang, ende)`:**
    *   **Legacy:** Validates date format (YYYYMMDD) and order (Anfang <= Ende) using external utilities (`DWDate_Datum_Check`, `DWDate_Datum_LE`).
    *   **Target:** A Python function using `datetime.strptime` to parse dates and standard comparison operators to check the order. Raise exceptions for invalid format or order. The temporary file logic for capturing `stderr` will be replaced by direct error messages in Python exceptions.
*   **`pruefeZahlPositiv(zahl, parametername)`:**
    *   **Legacy:** Checks if a parameter is numeric and positive.
    *   **Target:** A Python function attempting to convert the input to a number and checking if it's non-negative. Raise `ValueError` or custom exception.
*   **`pruefeZeitParameter(anfang, ende, zeitoffset)`:**
    *   **Legacy:** Validates date range parameters (either start/end dates OR a time offset).
    *   **Target:** A Python function that implements the same conditional logic, calling the new `pruefeZeitraum` and `pruefeZahlPositiv` Python functions.
*   **`konvertiereZeitspanne(varanfang, varende, spanne, kennzahl)`:**
    *   **Legacy:** Calculates start and end dates from a time `spanne` and `kennzahl` (which determines unit, D or M) using an external utility (`DWDate_Gib_Zeitraum`).
    *   **Target:** A Python function using `datetime` and `timedelta` objects to perform the date arithmetic. The `Offset_Unit` logic will be directly implemented in Python.

## 6. External Dependencies

The original script has the following external dependencies:

*   **`DWDate_Datum_Check`:** A utility for checking date format.
    *   **Replacement:** Python's `datetime.strptime` with a format string like `"%Y%m%d"`.
*   **`DWDate_Datum_LE`:** A utility for checking if the first date is less than or equal to the second date.
    *   **Replacement:** Python's `datetime` object comparison operators (`<=`).
*   **`DWDate_Gib_Zeitraum`:** A utility for calculating start and end dates based on a span and unit.
    *   **Replacement:** Python's `datetime` and `timedelta` objects. For example, to subtract 'D' days or 'M' months. Note: Month arithmetic can be tricky and might require a specialized library or careful implementation to handle month-end dates correctly.

There are no other external systems (like Oracle, SFTP, S3) directly referenced or interacted with by this specific utility script.

## 7. Unresolved / Risks

*   **`file_complexity` data missing:** The `file_complexity` table returned no rows, so an official complexity tier was not determined. However, given the nature of a utility script with complex nested logic (case statements, conditional checks, external program calls), it can be assessed as at least "Medium" or "Complex" for manual migration. The "manual" automation bucket from `automation_rate` confirms this.
*   **Ambiguity in `DWDate_*` functions:** The exact behavior of `DWDate_Gib_Zeitraum` for month-based offsets (e.g., `Offset_Unit=M`) and its handling of edge cases (e.g., 31st of month minus 1 month resulting in February 28/29) needs to be carefully investigated. This might require access to the source code of `DWDate_Gib_Zeitraum` or detailed documentation to ensure precise replication in Python.
*   **Global Error Variables (`ErrNr`, `ErrArg`):** The KornShell script uses global variables for error reporting. In Python, this will be replaced by exceptions. Ensure all calling contexts are updated to handle these Python exceptions appropriately.
*   **`eval` statements:** The use of `eval` in KornShell for indirect variable manipulation (`eval "param_wert=\\$$param_var"`) needs careful translation to Python. Python offers more direct ways to access variables by name if they are in a dictionary-like context, or simply passing and returning values for function parameters. This conversion should prioritize clarity and maintainability over a literal `eval` translation.

## 8. Build Plan

The migration will involve creating a new Python module.

1.  **Create Python Module (`alis_parameter_utils.py`):**
    *   Initialize an empty Python file named `alis_parameter_utils.py`.
2.  **Implement `pruefeZahlPositiv`:**
    *   Translate the logic of `pruefeZahlPositiv` into a Python function. This is a foundational validation.
3.  **Implement Date Utility Replacements:**
    *   Create Python functions (e.g., `_check_date_format`, `_is_date_le`, `_calculate_date_range`) to replicate the behavior of `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum` using `datetime` and `timedelta`. Special attention to `DWDate_Gib_Zeitraum` for month arithmetic.
4.  **Implement `pruefeZeitraum`:**
    *   Translate `pruefeZeitraum` using the new Python date utility functions.
5.  **Implement `pruefeZeitParameter`:**
    *   Translate `pruefeZeitParameter` using the new Python validation functions.
6.  **Implement `konvertiereZeitspanne`:**
    *   Translate `konvertiereZeitspanne` using the new Python date arithmetic functions.
7.  **Implement Conversion Functions:**
    *   Translate `konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `konvertiereAufbStufeXtra` using Python dictionaries for mappings.
8.  **Implement Validation Functions:**
    *   Translate `pruefeSystemKennzahl`, `gibBereich`, `gibIntervall` into Python functions.
9.  **Implement `pruefeParameterGesetzt`:**
    *   Translate `pruefeParameterGesetzt` into a Python function.
10. **Refactor Error Handling:**
    *   Replace `ErrNr`/`ErrArg` error states with appropriate Python exceptions (e.g., `ValueError`, `KeyError`, or custom exceptions).
11. **Unit Testing:**
    *   Develop comprehensive unit tests for each Python function to ensure functional parity with the original KornShell script.
12. **Integration Testing:**
    *   Ensure that any calling scripts or components that depend on this utility are updated to use the new Python module and its functions.