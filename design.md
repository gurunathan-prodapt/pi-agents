# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh

## 1. Purpose & Scope
This KornShell script, `h_alis_parameter.ksh`, functions as a utility module providing helper routines for parsing, validating, and converting parameters within a legacy system. Its primary purpose is to standardize and validate various input values related to data systems, key figures (Kennzahlen), and timeframes. It ensures that system and metric combinations are allowed, derives logical groupings and intervals for metrics, validates date ranges, and calculates date ranges from given time spans. This script does not directly interact with business data tables but rather provides foundational parameter handling for other processes.

## 2. Source Inventory
The job consists of a single source file:
- **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
- **Technology:** KornShell (ksh)
- **Complexity Tier:** `complex` (`loc-escalated` migration flag)
- **Automation Bucket:** `manual`
- **Summary:** A KornShell script offering utility functions for parameter validation, conversion, and date arithmetic, intended to be sourced or invoked by other shell scripts.

## 3. Target Architecture
The target platform is Google Cloud BigQuery. Given the utility nature of this KornShell script, a direct conversion to BigQuery SQL or stored procedures is not appropriate. The recommended target architecture is a reusable Python module designed for the BigQuery-oriented execution context, potentially leveraging Google Cloud Functions or Dataflow for stateless execution, or integrated into a larger Python-based ETL orchestration framework like Apache Airflow running on Cloud Composer.

The translated module will expose its functionalities as Python functions, mirroring the original shell functions. It should handle parameter validation and conversion, and date calculations. This Python module will be independent of direct BigQuery table dependencies, unless a calling process explicitly supplies such inputs.

## 4. Data Flow & Lineage
The `h_alis_parameter.ksh` script is a utility and does not have explicit data flow edges (READS/WRITES) to or from other files within the analyzed job `5af228f1`. It is expected to be sourced or called by other shell scripts that would then utilize its functions.
The internal "data flow" within the script involves:
- **Inputs:** Parameters passed as arguments to its functions (e.g., `pruefeParameterGesetzt`, `konvertiereKennzahl`, `pruefeZeitraum`), and environment variables (accessed via `eval`).
- **Processing:** Logical operations using `case` statements for conversion, `if/elif/else` for validation, and string manipulation.
- **Outputs:** Updated environment variables (via `eval`), and error status communicated through global variables `ErrNr` and `ErrArg`.

## 5. Transformation Logic
The core transformation logic will involve translating the KornShell functions into equivalent Python functions.

**Key Conversion Areas:**
*   **Parameter Handling:**
    *   Shell `typeset` declarations (local variables) will become local Python variables.
    *   Indirect variable access using `eval "param_wert=\\$$param_var"` will be translated to Python's dictionary-like parameter passing or explicit function arguments.
    *   Conditional checks (`if [ -z "$VarName" ]`, `if [ $ErrNr -ne 0 ]`) will translate to Python `if/else` statements.
*   **String Manipulation & Case Statements:**
    *   Shell `case` statements for converting `Kennzahl` (key figures) and `System` names will be replaced by Python dictionaries or a series of `if/elif/else` statements for mapping. For example, `zugang) Kennzahl="zug";;` would become `{"zugang": "zug", ...}`.
*   **Error Handling:**
    *   The global `ErrNr` and `ErrArg` variables for error state management will need to be re-engineered. A Pythonic approach would involve raising specific exceptions (e.g., `ValueError`, custom exceptions) that can be caught and handled by calling modules.
*   **Date Operations:**
    *   Functions like `pruefeZeitraum`, `pruefeZahlPositiv`, `pruefeZeitParameter`, and `konvertiereZeitspanne` rely on external `DWDate_*` functions and shell date utilities. These will need to be re-implemented using Python's `datetime` module or a dedicated date/time library, ensuring equivalent logic for date format validation, comparison, and interval calculation. The external shell commands (`basename`, `date`, `cat`, `rm`) used for temporary files will be replaced by Python's standard library functions for file I/O or managed by in-memory operations.
*   **List Iteration:**
    *   Loops like `for bk in $listBereich;` involving `IFS` manipulation will be translated to standard Python list iterations.

## 6. External Dependencies
The script has several external dependencies, both system commands and undeclared functions:

*   **System Commands:**
    *   `basename`: Used for temporary file naming. Can be replaced by Python's `os.path.basename`.
    *   `date`: Used for timestamping temporary files. Can be replaced by Python's `datetime` module.
    *   `cat`, `rm`: Used for temporary file handling and debugging output. Can be replaced by Python's file I/O operations and logging.
*   **Undeclared Functions (Critical Missing Information):**
    *   `DWDate_Datum_Check`: Expected to check if a date string conforms to a given format (YYYYMMDD). This will need to be implemented in Python using `datetime.strptime` and error handling.
    *   `DWDate_Datum_LE`: Expected to compare two dates and return true if the first is less than or equal to the second. This will need to be implemented in Python using `datetime` objects comparison.
    *   `DWDate_Gib_Zeitraum`: Expected to calculate a date range based on a start date, unit (Day/Month), and span. This will need to be implemented in Python using `datetime` and `timedelta` objects.

**Replacement Strategy:**
The external `DWDate_` functions are critical and currently undefined. They must be re-implemented in Python based on their expected behavior as inferred from the KornShell script. This requires understanding their precise logic, including how they handle edge cases and error conditions.

## 7. Unresolved / Risks
*   **Undeclared Functions Implementation:** The most significant risk is the lack of implementation details for `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum`. Without their exact logic, a precise migration to Python is impossible. Assumptions will have to be made, or the original source code for these functions must be located and analyzed.
*   **Error Handling (`ErrNr`, `ErrArg`):** The script relies on global `ErrNr` and `ErrArg` variables for error status. The initialization and lifecycle of these variables are not defined within this script, suggesting they are managed by a calling script or global shell environment. The Python migration will need a robust error handling mechanism (e.g., exceptions) that is compatible with how other modules would consume this utility.
*   **Performance implications:** While unlikely for a utility script, `eval` usage in shell scripts can sometimes mask performance-sensitive operations. The Python implementation should be reviewed for efficiency.
*   **Completeness of Kennzahl/System Mappings:** The `case` statements for `konvertiereKennzahl` and `konvertiereSystem` are extensive. While directly translatable, any missing or incorrect mappings could lead to runtime errors.

## 8. Build Plan
The migration will involve creating a Python module (e.g., `alis_parameter.py`) containing the translated functions.

1.  **Module Structure:**
    *   Create a Python module file `alis_parameter.py`.
    *   Define constants for `MODUL_NAME` and `MODUL_VERSION`.
    *   Define custom exception classes (e.g., `ParameterError`, `ValidationError`) to replace the `ErrNr`/`ErrArg` mechanism.

2.  **Function Translation (Iterative):**
    *   **`pruefeParameterGesetzt`:** Translate to a Python function that checks for `None` or empty string inputs, raising a `ParameterError` if validation fails.
    *   **`konvertiereKennzahl`:** Translate the `case` statement into a Python dictionary mapping (e.g., `kennzahl_map = {"zugang": "zug", ...}`) and a function that performs the lookup. Raise an error if a key figure is unknown.
    *   **`konvertiereSystem`:** Similar to `konvertiereKennzahl`, translate the `case` statement to a Python dictionary lookup for system names.
    *   **`konvertiereSDName`:** Similar to `konvertiereKennzahl`, translate the `case` statement to a Python dictionary lookup for SD system names.
    *   **`konvertiereAufbStufeXtra`:** Translate the `case` statement to a Python dictionary lookup for processing stages.
    *   **`pruefeSystemKennzahl`:** Translate the complex `if/elif` logic for valid system-key figure combinations into a Python function with corresponding conditional checks.
    *   **`gibBereich`:** Translate the logic that maps key figures to "Bereich" (area) based on lists (`list_tn`, `list_us`, etc.) into Python lists and a lookup function.
    *   **`gibIntervall`:** Translate the logic that maps key figures to "Intervall" (interval) based on lists (`list_t`, `list_m`) into Python lists and a lookup function.
    *   **`pruefeZahlPositiv`:** Translate to a Python function that checks if an input is a positive number, raising an error if not.
    *   **`pruefeZeitParameter`:** Translate the conditional logic for validating date parameters (start date, end date, or time span) into a Python function.
    *   **`pruefeZeitraum`:** Translate to a Python function that validates date formats and compares start/end dates. This function will be dependent on the re-implementation of `DWDate_Datum_Check` and `DWDate_Datum_LE`.
    *   **`konvertiereZeitspanne`:** Translate to a Python function that calculates a date range from a span, unit, and kennzahl. This function will be dependent on the re-implementation of `DWDate_Gib_Zeitraum`.

3.  **Dependency Resolution:**
    *   **`DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`:** Implement these functions within the `alis_parameter.py` module or a separate `date_utils.py` module, leveraging Python's `datetime` library. This is a critical prerequisite.

4.  **Testing:**
    *   Develop comprehensive unit tests for each translated Python function to ensure functional parity with the original KornShell script.

**Language for Build Output:** Python