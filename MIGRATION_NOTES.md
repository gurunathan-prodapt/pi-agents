```markdown
# Migration Notes — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh

## 1. Summary

This document outlines the migration of the KornShell utility script `h_alis_parameter.ksh` from a legacy environment to a Google Cloud Platform (GCP) BigQuery-oriented execution context.

The original script provided helper routines for parsing, validating, and converting parameters related to data systems, key figures (Kennzahlen), and timeframes. It ensured parameter validity, derived logical groupings, and performed date arithmetic.

The script has been migrated to a reusable Python module, `alis_parameter.py`, designed to be integrated into Python-based ETL orchestration frameworks (e.g., Apache Airflow on Cloud Composer), Google Cloud Functions, or Dataflow jobs. This Python module exposes equivalent functionalities as Python functions, maintaining the core logic of parameter handling, validation, and date calculations.

## 2. Generated Artifacts

The migration produced the following primary artifact:

*   **`alis_parameter.py`**: This is the core Python module. It contains all the translated functions from the original `h_alis_parameter.ksh` script, including parameter validation, system/key figure conversion, and date calculation logic. It is designed to be imported and used by other Python scripts or applications requiring these utility functions.
*   **`tests/test_alis_parameter.py` (Recommended)**: While not directly generated in the provided output, a comprehensive set of unit tests for `alis_parameter.py` is a crucial artifact for ensuring functional parity and is strongly recommended to be developed alongside the module.

## 3. Key Design Decisions

The following key design decisions guided the migration:

*   **Target Language and Platform**: Python was chosen as the target language due to its suitability for developing reusable utility modules, strong integration with GCP services, and better maintainability compared to KornShell for complex logic. The target platform is a generic Python execution environment within GCP, allowing flexibility for integration into various services like Cloud Composer, Cloud Functions, or Dataflow.
*   **Direct Function-to-Function Translation**: Each significant KornShell function within `h_alis_parameter.ksh` was translated into a corresponding Python function within `alis_parameter.py`. This approach ensures a clear mapping between the original and migrated code, simplifying verification and future maintenance.
*   **Pythonic Error Handling**: The original KornShell script used global variables (`ErrNr`, `ErrArg`) for error reporting. This was re-engineered into a Pythonic exception-based model using custom exceptions (`ParameterError`, `ValidationError`). This provides a more robust, explicit, and localized error handling mechanism, improving code clarity and allowing calling applications to gracefully handle specific error conditions. The original error codes (e.g., 195, 196, 198) are preserved within the exception objects for backward compatibility in error interpretation.
*   **Re-implementation of External Date Functions**: The original script relied on undeclared external `DWDate_*` functions. These critical dependencies (`DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`) were re-implemented directly within `alis_parameter.py` (as private helper functions `_dwdate_datum_check`, `_dwdate_datum_le`, `_dwdate_gib_zeitraum`) using Python's `datetime` and `calendar` modules. This eliminates external shell dependencies and makes the module self-contained.
*   **Use of Python Dictionaries for Mappings**: KornShell `case` statements used for converting system names, key figures, and processing stages were translated into Python dictionaries (e.g., `kennzahl_map`, `system_map`). This provides a more efficient, readable, and maintainable way to manage mappings compared to chained `if/elif` statements.
*   **Explicit Parameter Passing**: The original script used `eval` for indirect variable access, which can be less secure and harder to trace. The Python module uses explicit function arguments, improving code clarity, type safety (with type hints), and security.

**Notable Trade-offs**:

*   **Initial Re-implementation Effort**: The re-implementation of the `DWDate_*` functions required careful analysis of their inferred behavior from the KornShell script, as their original source was unavailable. Any subtle differences in logic could lead to discrepancies.
*   **Adaptation of Calling Scripts**: The migration of this utility script necessitates significant changes in all upstream scripts or applications that previously sourced or invoked `h_alis_parameter.ksh`. These calling components must be updated to import and utilize the new Python module and adapt to its exception-based error handling.

## 4. Manual Steps Before Go-Live

The following manual steps are required before the migrated `alis_parameter.py` module can be fully operational in a production environment:

1.  **Deployment of `alis_parameter.py`**:
    *   **Package Management**: The `alis_parameter.py` module should be deployed to a suitable Python package repository (e.g., Google Artifact Registry, PyPI) or made available in the Python environment where dependent applications will run.
    *   **Environment Setup**: Ensure the target execution environment (e.g., Cloud Composer environment, Cloud Functions runtime, Dataflow worker images) has Python 3.8+ installed and can access the deployed module.

2.  **Integration with Calling Applications**:
    *   **Identify Dependents**: All legacy KornShell scripts or applications that sourced or called `h_alis_parameter.ksh` must be identified.
    *   **Code Modification**: Each identified dependent application needs to be modified to:
        *   Remove references to `h_alis_parameter.ksh`.
        *   Import the `alis_parameter` Python module.
        *   Replace calls to KornShell functions (e.g., `pruefeParameterGesetzt`) with calls to their Python equivalents (e.g., `alis_parameter.pruefe_parameter_gesetzt`).
        *   Adapt error handling logic to catch and process `ParameterError` and `ValidationError` exceptions instead of checking global `ErrNr`/`ErrArg` variables.

3.  **IAM/Permissions**:
    *   While `alis_parameter.py` itself is a self-contained utility and does not directly interact with GCP services like BigQuery or Cloud Storage, the calling applications might. Ensure the service accounts used by these calling applications have all necessary IAM permissions for their respective operations.

4.  **Configuration/Secrets**:
    *   This utility script does not handle connection strings or secrets directly. Any such configurations previously managed by the KornShell environment for calling scripts will need to be re-evaluated and securely managed within the GCP context (e.g., using Secret Manager, environment variables, or configuration files).

5.  **Scheduling**:
    *   If the original KornShell scripts were part of scheduled jobs (e.g., cron jobs), their Python-based replacements will need to be scheduled using appropriate GCP services (e.g., Cloud Composer/Airflow DAGs, Cloud Scheduler triggering Cloud Functions/Dataflow jobs).

## 5. Known Gaps & Unresolved References

*   **Exact `DWDate_*` Logic**: While the `_dwdate_datum_check`, `_dwdate_datum_le`, and `_dwdate_gib_zeitraum` functions have been re-implemented based on their inferred behavior, the precise original logic (especially for edge cases or specific date calculations) of the legacy `DWDate_*` functions was not available. This is a potential gap that might require further validation or adjustment if discrepancies are found.
*   **Global Error State vs. Exceptions**: The original script's reliance on global `ErrNr` and `ErrArg` variables for error state management implies a specific interaction pattern with calling scripts. The Python exception model is fundamentally different. While the error codes are preserved, the overall flow of error detection and handling in calling scripts will need to be redesigned to properly catch and interpret these exceptions. This is a significant architectural change for dependent components.
*   **Comprehensive Calling Script Inventory**: The migration focused solely on `h_alis_parameter.ksh`. A complete inventory and analysis of all scripts that depend on this utility are crucial for a successful end-to-end migration. The effort to adapt these calling scripts is a major follow-up item.
*   **Performance Review**: While unlikely for a utility script, the original KornShell script's use of `eval` could potentially mask performance-sensitive operations. The Python implementation should be monitored for performance, especially if it's integrated into high-volume or latency-sensitive workflows.

## 6. Validation

Validation of the migrated `alis_parameter.py` module involves both unit and integration testing:

*   **Unit Tests**:
    *   **How to run**: Develop a comprehensive suite of unit tests (e.g., using `pytest`) for `alis_parameter.py`. These tests should cover every function within the module.
    *   **What "passing" means**:
        *   All functions return the expected output for valid inputs (e.g., `konvertiere_kennzahl` returns correct abbreviations).
        *   Functions correctly raise `ParameterError` or `ValidationError` for invalid or missing inputs, with the correct error messages and codes.
        *   Date calculation functions (`_dwdate_gib_zeitraum`, `konvertiere_zeitspanne`) produce accurate date ranges for various spans and units.
        *   Validation functions (`pruefe_zeitraum`, `pruefe_system_kennzahl`) correctly identify valid and invalid conditions.
        *   Edge cases (e.g., empty strings, non-numeric inputs where numbers are expected, boundary dates) are handled as expected.

*   **Integration Tests**:
    *   **How to run**: Integrate `alis_parameter.py` into a representative calling application (e.g., a Python script that mimics the behavior of an original KornShell job). Execute this application with a variety of test data and parameters.
    *   **What "passing" means**:
        *   The calling application successfully imports and uses the `alis_parameter` module.
        *   The application's overall execution flow remains correct, with parameters being validated and converted as expected.
        *   Error conditions originating from `alis_parameter.py` are correctly caught and handled by the calling application, leading to appropriate logging or termination behavior.
        *   The final output or behavior of the integrated application matches the output/behavior of the original KornShell-based job.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Halt New Deployments**: Immediately stop any ongoing deployments or updates related to the migrated Python module or its dependent applications.
2.  **Revert Calling Applications**: Revert the code of all dependent applications to their last known stable version that used the original `h_alis_parameter.ksh` script. This involves:
    *   Removing `import alis_parameter` statements.
    *   Restoring calls to the original KornShell functions.
    *   Reverting error handling logic to check `ErrNr`/`ErrArg` as before.
3.  **Undeploy Python Module**: Remove or disable the deployed `alis_parameter.py` module from the Python package repository or execution environments to prevent accidental usage.
4.  **Restore Legacy Environment**: Ensure the original KornShell script `h_alis_parameter.ksh` is available and correctly configured in the legacy execution environment.
5.  **Restart Legacy Jobs**: Restart the original KornShell-based jobs or applications.
6.  **Verify Functionality**: Thoroughly verify that the legacy system is fully operational and performing as expected.

This rollback procedure ensures a quick return to the previous stable state, minimizing disruption. A root cause analysis should then be performed to address the issues before attempting re-migration.
```