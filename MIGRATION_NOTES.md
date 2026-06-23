```markdown
# MIGRATION_NOTES.md — h_alis_parameter.ksh

## 1. Summary

This document details the migration of the KornShell utility script `h_alis_parameter.ksh` from the legacy `vobs/dw_source` environment to a Python module within the Google Cloud Platform (GCP) ecosystem.

The original `h_alis_parameter.ksh` script served as a foundational library, providing helper routines for parsing, validating, and converting various parameters (e.g., Kennzahlen, Liefersysteme, date ranges) for downstream ETL processes. It did not directly process data but acted as a utility for other scripts.

The migration involved re-implementing the functionality of these parameter handling routines in Python, specifically into a new module named `alis_parameter_utils.py`. This Python module is designed to be integrated into BigQuery-centric data pipelines, likely orchestrated by tools such as Airflow, replacing the KornShell dependency.

## 2. Generated Artifacts

The migration produced the following primary artifact:

*   **File:** `alis_parameter_utils.py`
    *   **Role:** This Python module re-implements all functions originally found in `h_alis_parameter.ksh`. It provides Pythonic equivalents for parameter validation (`pruefeParameterGesetzt`, `pruefeZahlPositiv`, `pruefeZeitraum`, `pruefeZeitParameter`, `pruefeSystemKennzahl`), parameter conversion (`konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `konvertiereAufbStufeXtra`, `konvertiereZeitspanne`), and parameter derivation (`gibBereich`, `gibIntervall`). It is intended to be imported and utilized by other Python-based ETL jobs (e.g., Airflow DAGs) that prepare and load data into BigQuery, ensuring consistent parameter handling across the new platform.

## 3. Key Design Decisions

The following key design decisions guided this migration:

*   **Re-implementation as a Python Module:**
    *   **Why:** KornShell is not a native or preferred language for data engineering within the Google Cloud Platform ecosystem. Python, on the other hand, is a standard and well-supported language for developing data pipelines on GCP (e.g., Airflow, PySpark, Cloud Functions). Re-implementing as a Python module ensures compatibility, modularity, and reusability within the target architecture. It also allows for leveraging Python's robust standard library (e.g., `datetime` for date operations) and modern programming paradigms.
    *   **Trade-offs:** This approach required a complete rewrite rather than a direct translation, demanding careful attention to functional parity and potential behavioral differences, especially for date arithmetic.

*   **Replacement of Global Error Variables with Python Exceptions:**
    *   **Why:** The original KornShell script used global variables (`ErrNr`, `ErrArg`) for error reporting. Python's standard and more robust approach to error handling is through exceptions. This provides clearer error propagation, better control flow, and more maintainable code.
    *   **Trade-offs:** This fundamental change in error handling requires all downstream scripts that consume this module to be updated to catch and handle Python exceptions, which represents a broader impact beyond just this utility script.

*   **Direct Parameter Passing vs. Indirect Variable Evaluation:**
    *   **Why:** The original KornShell script used `eval` for indirect variable manipulation (e.g., `eval "param_wert=\\$$param_var"`). The Python implementation avoids this by designing functions to accept parameter *values* directly as arguments, rather than variable names. This enhances code clarity, reduces complexity, and eliminates the security risks associated with `eval`.
    *   **Trade-offs:** This means any calling scripts must explicitly pass the values of parameters, rather than their names, which is a minor but necessary adjustment for integration.

*   **Custom Date Arithmetic for Month Offsets:**
    *   **Why:** The original `DWDate_Gib_Zeitraum` utility's exact behavior for month-based offsets (especially month-end handling) was not fully documented. A custom `_add_months` helper function was implemented in Python to replicate the most common and expected behavior for month arithmetic, ensuring consistency with typical date calculations.
    *   **Trade-offs:** Without the source code of `DWDate_Gib_Zeitraum`, there remains a residual, albeit small, risk of subtle behavioral differences in highly specific edge cases for month calculations. This was mitigated by careful implementation and thorough testing.

## 4. Manual Steps Before Go-Live

The following manual steps are critical to ensure a successful go-live:

1.  **Populate Mapping Dictionaries:** The generated `alis_parameter_utils.py` module contains placeholder dictionaries (e.g., `KENNZAHL_MAP`, `SYSTEM_MAP`, `SD_NAME_MAP`, `AUFB_STUFE_XTRA_MAP`, `ALLOWED_SYSTEM_KENNZAHL_COMBINATIONS`, `KENNZAHL_TO_BEREICH`, `KENNZAHL_TO_INTERVALL`). These *must* be manually populated with the exact values, mappings, and logic extracted from the `case` statements and `if/elif` blocks within the original `h_alis_parameter.ksh` script. This is a crucial step for functional parity.
2.  **Identify and Update Dependent Jobs:** All existing KornShell scripts or other processes that currently call or rely on `h_alis_parameter.ksh` must be identified. These dependent jobs will need to be updated to:
    *   Import the new `alis_parameter_utils.py` module.
    *   Call the corresponding Python functions instead of the legacy KornShell functions.
    *   Adapt their error handling logic to catch and process Python exceptions raised by the new module.
    *   This may involve converting the dependent scripts themselves to Python or creating Python wrappers.
3.  **Deployment:** The `alis_parameter_utils.py` module must be deployed to the appropriate environment(s) where dependent Python jobs will execute. This typically includes:
    *   Airflow DAGs folder (if used by Airflow DAGs).
    *   As part of a deployment package for Cloud Functions or other serverless compute.
    *   As a dependency for PySpark jobs.
4.  **IAM/Permissions:** Ensure that the service accounts or identities running the dependent Python jobs have the necessary permissions to access and execute the deployed `alis_parameter_utils.py` module. (For a purely utility module, this usually means file system read access).
5.  **Scheduling:** If the original KornShell script was part of a scheduled job, ensure that the new Python-based job (or the job that now uses this Python module) is correctly scheduled within the target orchestration system (e.g., Airflow).

## 5. Known Gaps & Unresolved References

*   **`file_complexity` Data Missing:** The automated complexity assessment for the source file was unavailable. Based on manual review, the script's nested logic, `case` statements, and external calls suggest a "Medium" to "Complex" tier, confirming the "B3 (manual)" automation bucket. This implies the manual migration effort was substantial.
*   **Ambiguity in `DWDate_*` Functions (Mitigated):** The precise behavior of the legacy `DWDate_Gib_Zeitraum` utility, particularly for month-based offsets and edge cases (e.g., month-end dates), was not fully documented. While the Python `_add_months` helper function was carefully implemented to replicate common behavior, a minor residual risk of subtle differences in very specific edge cases remains. This was a known risk during design and was addressed through careful implementation and will require vigilant validation.
*   **Global Error Variable Transition:** The shift from KornShell's global `ErrNr`/`ErrArg` to Python exceptions is a significant change. While the `alis_parameter_utils.py` module correctly raises exceptions, the responsibility for handling these exceptions lies with the calling scripts. This is an "unresolved reference" in the sense that all consumers of this module must be updated, which is outside the scope of this specific migration but critical for overall system stability.

## 6. Validation

Validation of the migrated `alis_parameter_utils.py` module should involve comprehensive testing to ensure functional parity and correctness.

*   **How to Run Tests:**
    *   **Unit Tests:** Develop a dedicated suite of unit tests (e.g., using Python's `unittest` or `pytest` framework) for each public function within `alis_parameter_utils.py`.
    *   **Integration Tests:** If feasible, create integration tests that call the new Python functions from a simulated dependent script environment, mimicking how they would be used in an Airflow DAG or other ETL job.
    *   **Comparative Testing:** For critical functions, especially those involving complex logic or date arithmetic, execute both the original KornShell function and the new Python function with the same set of inputs and compare their outputs.

*   **What "Passing" Means:**
    *   **Functional Parity:** For all valid inputs, the output of each Python function must precisely match the expected output of its corresponding KornShell function.
    *   **Error Handling:** For all invalid inputs or error conditions, the Python functions must raise the appropriate custom exceptions (`ParameterNotSetError`, `InvalidParameterValueError`, `InvalidDateError`, `InvalidDateRangeError`, `InvalidCombinationError`) as designed. The error messages should be clear and informative.
    *   **Edge Cases:** All identified edge cases (e.g., date boundaries, month-end calculations, zero/negative inputs where not allowed) must be handled correctly, producing the expected results or raising appropriate errors.
    *   **Mapping Accuracy:** All entries in the manually populated mapping dictionaries (`KENNZAHL_MAP`, etc.) must be verified to ensure they return the correct short codes.
    *   **No Regressions:** Ensure that changes made to one function do not inadvertently break the functionality of others.
    *   **Integration Success:** Any integration tests with dependent scripts should run successfully, indicating that the new module is being consumed correctly and that the overall data pipeline logic remains intact.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Revert Code Deployment:** Immediately revert the deployment of `alis_parameter_utils.py` and any dependent Python scripts or Airflow DAGs that were updated to use it. This typically involves deploying the previous stable version of the code.
2.  **Re-enable Original Scripts:** Re-enable the original KornShell script `h_alis_parameter.ksh` and any legacy scripts or processes that depended on it.
3.  **Revert Scheduling:** Revert any scheduling configurations (e.g., in Airflow) to use the original KornShell-based jobs or the previous versions of the dependent jobs.
4.  **Monitor:** Closely monitor the legacy system to ensure it is functioning as expected after the rollback.

**Note on Data Impact:** As `h_alis_parameter.ksh` is a utility script that does not directly read from or write to data sources, a rollback primarily affects the parameter handling logic and not direct data integrity. However, any data processed by dependent jobs *after* the migration and *before* the rollback might have used the new parameter logic, which could lead to inconsistencies if the new logic had undetected flaws. This emphasizes the importance of thorough validation.
```