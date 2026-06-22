# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell utility script `h_alis_parameter.ksh` from its legacy environment to Google Cloud Platform. The original script served as a library of helper routines for parsing, normalizing, validating, and converting various parameters (e.g., key figures, system names, dates) within a data warehousing context.

The migration translates this core business logic into a hybrid architecture on Google Cloud:
*   **BigQuery User-Defined Functions (UDFs)**: For stateless conversion and validation logic that can be efficiently expressed in SQL.
*   **Python Module**: For more complex logic involving external dependencies, advanced date manipulations, and structured error handling.

This approach ensures interoperability with future data pipelines leveraging BigQuery and other Google Cloud services.

## 2. Generated Artifacts

The migration produced the following artifacts:

*   **`src/python/h_alis_parameter/date_utils.py`**
    *   **Role**: A Python module containing re-implementations of the original `DWDate_` external functions (`DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`) and the more complex date/numeric validation and conversion functions (`pruefeZahlPositiv`, `pruefeZeitraum`, `pruefeZeitParameter`, `konvertiereZeitspanne`). This module is designed to be imported and used by other Python-based data pipelines or Cloud Functions.

*   **`src/sql/bq_udfs/udf_pruefe_parameter_gesetzt.sql`**
    *   **Role**: BigQuery SQL UDF to check if a given parameter value is set and not empty. Returns a `STRUCT` indicating validity and an error message if not valid.

*   **`src/sql/bq_udfs/udf_konvertiere_kennzahl.sql`**
    *   **Role**: BigQuery SQL UDF to convert descriptive key figure names (e.g., 'zugang') into standardized short codes (e.g., 'zug').

*   **`src/sql/bq_udfs/udf_konvertiere_system.sql`**
    *   **Role**: BigQuery SQL UDF to convert descriptive system names into standardized short codes (e.g., 'sap', 'carmen').

*   **`src/sql/bq_udfs/udf_konvertiere_sdname.sql`**
    *   **Role**: BigQuery SQL UDF to convert descriptive master data system names into standardized short codes.

*   **`src/sql/bq_udfs/udf_konvertiere_aufbstufextra.sql`**
    *   **Role**: BigQuery SQL UDF to convert Xtra preparation stage names (ee.g., 'zusammenfuehrung') into standardized short codes (e.g., 'mrg').

*   **`src/sql/bq_udfs/udf_pruefe_system_kennzahl.sql`**
    *   **Role**: BigQuery SQL UDF to validate if a specific combination of system and key figure is allowed based on defined business rules. Returns a `STRUCT` indicating validity and an error message.

*   **`src/sql/bq_udfs/udf_gib_bereich.sql`**
    *   **Role**: BigQuery SQL UDF to derive a "Bereich" (area/group) based on a given key figure.

*   **`src/sql/bq_udfs/udf_gib_intervall.sql`**
    *   **Role**: BigQuery SQL UDF to derive an "Intervall" (interval type, 't' for daily or 'm' for monthly) based on a given key figure.

*   **`src/sql/bq_udfs/udf_pruefe_zahl_positiv.sql`**
    *   **Role**: BigQuery SQL UDF to check if a string value represents a numeric, non-negative number. Returns a `STRUCT` indicating validity and an error message.

## 3. Key Design Decisions

*   **Hybrid Architecture for Functionality Split**:
    *   **Why**: A direct, monolithic translation to a single BigQuery UDF or a single Python module would not leverage the strengths of each platform. Simple, stateless transformations and validations are highly efficient as BigQuery SQL UDFs, executing directly within the BigQuery engine. More complex logic, especially that involving external library dependencies (like `dateutil` for `relativedelta`) or intricate conditional flows, is more naturally and robustly handled in Python.
    *   **Trade-offs**: This introduces two distinct deployment and management paths (SQL DDL for UDFs, Python package/deployment for the module). However, the benefits in performance, maintainability, and idiomatic expression outweigh this complexity.

*   **Modern Error Handling**:
    *   **Why**: The original KornShell script's global `ErrNr`/`ErrArg` pattern is outdated and not suitable for modern, distributed cloud environments.
    *   **Trade-offs**: For BigQuery UDFs, a `STRUCT<is_valid BOOL, error_message STRING>` return type was chosen for validation functions to allow calling queries to handle errors gracefully without immediate failure. For conversion functions, the `ERROR()` function is used to explicitly fail the query on invalid input, aligning with BigQuery's error propagation model. Python functions utilize standard exception handling (`try-except`) for clarity and robustness. This requires consuming applications to adapt to these new error reporting mechanisms.

*   **Elimination of `eval` and Temporary Files**:
    *   **Why**: The KornShell script's heavy reliance on `eval` for dynamic variable access is a security risk and reduces readability. The use of temporary files for logging external command output is inefficient and complicates debugging in a cloud environment.
    *   **Trade-offs**: Parameters are now passed directly by value or reference, and logging is handled via standard Python `logging` (to Cloud Logging) or BigQuery's native error reporting. This improves security and maintainability but requires a complete re-architecture of how parameters are accessed and how diagnostics are collected.

*   **Re-implementation of `DWDate_` Functions**:
    *   **Why**: The original script depended on external `DWDate_` utilities. These were re-implemented using native BigQuery SQL date functions (for UDFs) and Python's `datetime` and `dateutil` libraries (for the Python module). This removes external binary dependencies and ensures portability within the GCP ecosystem.
    *   **Trade-offs**: Requires careful verification that the re-implemented logic precisely matches the behavior of the legacy `DWDate_` utilities, especially concerning edge cases and locale-specific date handling.

*   **BigQuery UDFs in a Dedicated Dataset**:
    *   **Why**: Placing all utility UDFs in a specific dataset (e.g., `your_project_id.utility_functions`) provides clear organization, simplifies permission management, and avoids naming conflicts.

## 4. Manual Steps Before Go-Live

1.  **BigQuery Dataset Creation**:
    *   Create the dedicated BigQuery dataset for utility functions: `your_project_id.utility_functions`.
    *   **Command**: `bq mk --dataset --default_table_expiration 3600 your_project_id:utility_functions` (adjust expiration as needed).

2.  **IAM Permissions**:
    *   Ensure that the service accounts or user identities that will deploy and execute these UDFs have the necessary BigQuery roles (ee.g., `BigQuery Data Editor` for creation, `BigQuery Data Viewer` for execution).
    *   Ensure that any service accounts executing Python code (e.g., Cloud Functions, Dataflow jobs) have permissions to deploy and run Python applications.

3.  **Python Library Installation**:
    *   The `date_utils.py` module depends on the `python-dateutil` library. This must be installed in the Python environment where the module will be used.
    *   **Command**: `pip install python-dateutil` (or include in `requirements.txt` for deployment).

4.  **Deploy BigQuery UDFs**:
    *   Execute each `src/sql/bq_udfs/*.sql` script against your BigQuery project. Replace `your_project_id` with your actual GCP project ID.
    *   **Example Command**: `bq query --use_legacy_sql=false < src/sql/bq_udfs/udf_konvertiere_kennzahl.sql`

5.  **Deploy Python Module**:
    *   The `src/python/h_alis_parameter/date_utils.py` module should be made available to consuming applications. This could involve:
        *   Packaging it as part of a larger Python application.
        *   Deploying it as a Cloud Function.
        *   Including it in a shared library accessible by Dataflow or Airflow (Composer) environments.

6.  **Populate `CASE` Statements**:
    *   Review the generated SQL UDFs (`udf_konvertiere_kennzahl.sql`, `udf_konvertiere_system.sql`, etc.) and ensure all `CASE` statements are fully populated with the complete set of mappings from the original KornShell script. The generated code includes placeholders like `-- Add more mappings from the original KornShell script's case statement here`.

## 5. Known Gaps & Unresolved References

*   **Completeness of `CASE` Mappings**: The generated SQL UDFs for conversion (`konvertiere_kennzahl`, `konvertiere_system`, etc.) contain placeholders. These must be thoroughly reviewed and completed with all mappings present in the original `h_alis_parameter.ksh` script.
*   **`pruefeZahlPositiv` Parameter Name**: In the Python implementation of `pruefeZahlPositiv`, the `parameter_name` argument is currently unused. While not critical for functionality, it could be leveraged for more descriptive error messages if needed by consuming applications.
*   **`konvertiereZeitspanne` Reference Date**: The `konvertiereZeitspanne` Python function, via `get_date_range_from_span`, defaults to `datetime.date.today()` if no `reference_date_str` is provided. The exact behavior of the original `DWDate_Gib_Zeitraum` regarding its reference date when a span is given should be verified to ensure precise functional equivalence.
*   **"loc-escalated" Complexity**: The original script was flagged as "loc-escalated" in complexity. While the migration design addresses known complexities, there might be subtle, undocumented behaviors or implicit context in the original KornShell script that could be missed. Thorough testing is crucial to uncover these.
*   **Error Handling Consistency**: While a strategy for error handling has been implemented (STRUCTs for validation, `ERROR()` for conversion in SQL; exceptions in Python), consuming applications must be designed to consistently interpret and react to these new error patterns.

## 6. Validation

Validation will involve a multi-tiered approach to ensure functional equivalence and correctness.

1.  **Unit Tests (Python)**:
    *   **How to run**: Develop a Python test suite (e.g., using `pytest`) for `src/python/h_alis_parameter/date_utils.py`.
    *   **What "passing" means**:
        *   All functions (`is_valid_date_format`, `is_date1_le_date2`, `get_date_range_from_span`, `pruefeZahlPositiv`, `pruefeZeitraum`, `pruefeZeitParameter`, `konvertiereZeitspanne`) return expected outputs for a comprehensive set of inputs, including valid cases, invalid inputs, and edge cases (e.g., leap years, month boundaries, zero/negative spans).
        *   Error conditions (e.g., invalid date formats, non-positive numbers, conflicting parameters) correctly raise exceptions or return expected error indicators.

2.  **Unit Tests (BigQuery UDFs)**:
    *   **How to run**: Write SQL queries that invoke each UDF with various inputs and assert the expected output. These can be run directly in the BigQuery console or via a CI/CD pipeline.
    *   **Example**:
        ```sql
        -- Test udf_konvertiere_kennzahl
        SELECT `your_project_id.utility_functions`.konvertiere_kennzahl('Zugang') AS result; -- Expected: 'zug'
        SELECT `your_project_id.utility_functions`.konvertiere_kennzahl('Unknown') AS result; -- Expected: ERROR

        -- Test udf_pruefe_system_kennzahl
        SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('sap', 'zug') AS result; -- Expected: {TRUE, ''}
        SELECT `your_project_id.utility_functions`.pruefe_system_kennzahl('carmen', 'kfb') AS result; -- Expected: {FALSE, 'Kennzahl "kfb" not allowed for system "carmen".'}
        ```
    *   **What "passing" means**:
        *   All UDFs return the correct converted values or validation results (`TRUE`/`FALSE` in the `STRUCT`).
        *   Error conditions (e.g., invalid input to conversion UDFs, invalid combinations for validation UDFs) correctly trigger the `ERROR()` function or return the expected error message within the `STRUCT`.

3.  **Integration Tests**:
    *   **How to run**: Once consuming jobs or pipelines are migrated to use these new components, integration tests should be performed. This involves running the end-to-end data pipelines that utilize these utility functions.
    *   **What "passing" means**:
        *   The overall data pipeline executes successfully without unexpected errors related to parameter validation or conversion.
        *   The final data output matches the expected results, indicating that the utility functions processed parameters correctly.
        *   Error scenarios within the pipeline (e.g., invalid input data leading to parameter validation failures) are handled as designed, either by gracefully logging errors or by failing the pipeline with informative messages.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after deployment, the following rollback procedure can be initiated:

1.  **Revert Consuming Applications**:
    *   If any applications or data pipelines were modified to use the new BigQuery UDFs or Python module, revert those changes to point back to the original `h_alis_parameter.ksh` script or its legacy invocation method. This is the primary and most critical step.

2.  **Remove BigQuery UDFs**:
    *   Drop the newly created BigQuery UDFs from the `your_project_id.utility_functions` dataset.
    *   **Command**:
        ```sql
        DROP FUNCTION IF EXISTS `your_project_id.utility_functions`.pruefe_parameter_gesetzt;
        DROP FUNCTION IF EXISTS `your_project_id.utility_functions`.konvertiere_kennzahl;
        -- ... repeat for all other UDFs ...
        ```

3.  **Deactivate/Remove Python Module**:
    *   If the Python module was deployed as a Cloud Function, deactivate or delete the Cloud Function.
    *   If it was part of a shared library, revert the deployment of that library to a previous version or remove the `h_alis_parameter` module.

4.  **Re-enable Legacy Environment**:
    *   Ensure that the original `h_alis_parameter.ksh` script and its dependencies are fully operational and accessible by any applications that have been reverted.

This procedure assumes that the original KornShell script and its execution environment remain available and functional as a fallback during the migration period.