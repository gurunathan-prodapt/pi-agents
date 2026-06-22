# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`, a KornShell script providing sophisticated text parsing and templating functionalities.

The original script, classified under the "retire" migration bucket due to its complexity, reliance on `eval`, and heavy use of various Unix utilities (`sed`, `nawk`, `perl`), has been re-engineered. It is now replaced by a **Python application** (`alis_parser.py`) designed to replicate its core functionality. The target platform for this Python application is **Google Cloud**, specifically intended for deployment as a **Google Cloud Function** for on-demand, event-driven execution, or as a **Dataflow job** for batch processing, depending on specific invocation patterns and scalability needs.

## 2. Generated artifacts

The migration has produced the following primary artifact:

*   **`src/python/alis_parser.py`**:
    *   **Role**: This Python file contains the `AlisParser` class, which encapsulates the re-engineered text parsing and templating logic. It serves as a direct functional replacement for the original `h_alis_parser.ksh` script. The class provides methods to process template strings, handle variable assignments, expand lists with conditional formatting (`<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>`), manage file inclusions, and generate timestamps.

## 3. Key design decisions

The re-engineering approach was guided by the following key design decisions:

*   **Re-engineering over Direct Conversion**: Given the "retire" migration bucket, the script's "complex" tier, and its heavy reliance on procedural shell logic, dynamic `eval` statements, and external Unix utilities, a direct lift-and-shift or automated conversion was deemed infeasible and undesirable. A complete re-engineering in a modern language was chosen for maintainability, scalability, and security.
*   **Target Language: Python**: Python was selected as the target language due to its suitability for text processing, strong ecosystem (e.g., `re` module for regex, `datetime` for timestamps), BigQuery compatibility, and ease of deployment within Google Cloud.
*   **Elimination of `eval`**: The original script's extensive use of `eval` for dynamic command generation was a significant security and maintainability concern. The Python solution completely replaces this with explicit programmatic logic, using regular expressions and structured data processing.
*   **Modular Object-Oriented Design**: The functionality is encapsulated within an `AlisParser` Python class. This promotes modularity, reusability, and easier testing compared to the monolithic shell script. Methods within the class (e.g., `_parser_filattrib`, `_process_single_list_item_template`, `parse_template`) mirror the logical functions of the original script.
*   **Two-Pass Parsing Strategy**: The `parse_template` method employs a two-pass approach:
    1.  **First Pass**: Identifies and processes variable assignments (`<VAR=VALUE>`) and `LISTDEF` blocks, populating an internal `self.context` dictionary with global variables and structured list data.
    2.  **Second Pass**: Applies transformations based on the populated context, including variable substitutions, list expansions, file inclusions, and timestamp generations. This ensures that all definitions are available before transformations begin.
*   **Abstracted File Resolution**: The `AlisParser` constructor accepts an optional `file_resolver` callable. This allows the parser to be flexible regarding the source of included files (e.g., local filesystem, Google Cloud Storage, or even an in-memory dictionary), enhancing portability and testability.
*   **Regular Expressions for Pattern Matching**: Python's `re` module is used extensively to identify and extract custom tags and patterns (e.g., `<VAR=VALUE>`, `<LISTDEF>`, `<INCLUDE FILE='...'/>`, `<TIMESTAMP/>`), replacing the `sed`, `nawk`, and `perl` commands of the original script.
*   **Structured Error Reporting**: The Python implementation includes an `self.errors` list to collect and report any parsing issues encountered, improving observability and debugging compared to the original script's less structured error handling.

## 4. Manual steps before go-live

Before the re-engineered Python parser can go live, the following manual steps are required:

1.  **Deployment to Google Cloud**:
    *   Deploy the `alis_parser.py` code (and any dependencies) to the chosen Google Cloud service (e.g., as a **Google Cloud Function** or a **Dataflow job**). This involves creating the service, configuring its runtime environment (Python 3.x), and uploading the code.
2.  **IAM & Permissions Configuration**:
    *   Create or identify a dedicated **Service Account** for the deployed Cloud Function/Dataflow job.
    *   Grant this Service Account the necessary IAM roles and permissions. This typically includes:
        *   `Storage Object Viewer` (roles/storage.objectViewer) for reading files from Google Cloud Storage if `<INCLUDE FILE='...'/>` directives refer to GCS paths.
        *   `Cloud Functions Invoker` (roles/cloudfunctions.invoker) if the function is to be triggered by other services.
        *   `BigQuery Data Editor` (roles/bigquery.dataEditor) or similar, if the processed output is to be written to BigQuery tables.
        *   `Cloud Logging Writer` (roles/logging.logWriter) for logging.
3.  **Input File Accessibility**:
    *   Ensure that any files referenced by `<INCLUDE FILE='...'/>` directives in templates are accessible to the deployed Python service. If these files were previously on local disk, they must be migrated to a cloud-accessible storage solution, such as **Google Cloud Storage (GCS)** buckets. The `file_resolver` in `AlisParser` would need to be configured to read from GCS.
4.  **Scheduling (if applicable)**:
    *   If the original script was executed on a schedule, configure a new scheduler for the Python service. For Cloud Functions, this might involve setting up **Cloud Scheduler** to trigger the function via HTTP. For Dataflow, **Cloud Composer (Airflow)** is a common choice.
5.  **Environment Variables/Configuration**:
    *   If the Python application requires any configuration (e.g., GCS bucket names for includes, default output paths), these should be set up as environment variables in the Cloud Function/Dataflow environment or passed as parameters during invocation.
6.  **BigQuery Schema/Dataset Creation (Conditional)**:
    *   If the processed output from the Python parser is intended to be written to BigQuery, ensure that the target BigQuery datasets and table schemas are pre-created and correctly defined.

## 5. Known gaps & unresolved references

The re-engineering effort addressed the core functionality, but some aspects warrant further consideration or represent potential differences from the original script's behavior:

*   **`parser_getnode` Re-implementation**: The original script included a `parser_getnode` function for XML-like node extraction. While the `parse_template` method handles general templating, a direct equivalent for `parser_getnode` is not explicitly implemented in the generated `alis_parser.py`. If this specific functionality is critical and distinct from general templating, it would need to be added, potentially using Python's `re` module or an XML parsing library.
*   **Complex Chained Variable Definitions**: The current variable substitution in `parse_template` is a single pass. If the original script relied on highly complex, multi-level chained variable definitions where a variable defined later in the template could influence a variable defined earlier, the current single-pass approach might not fully replicate this. A more sophisticated dependency graph or iterative replacement might be needed for such edge cases.
*   **Infinite Include Loop Detection**: The recursive handling of `<INCLUDE FILE='...'/>` directives in `parse_template` does not currently include explicit detection or prevention of infinite include loops. While unlikely in well-formed templates, this could lead to stack overflow errors for malicious or malformed inputs.
*   **Performance Characteristics**: While Python is generally performant, the `nawk` and `sed` operations in the original KornShell script are highly optimized for text processing at the OS level. The Python re-implementation, especially with extensive regular expression usage, needs careful performance monitoring and profiling, particularly for very large templates or high-volume processing.
*   **Error Handling Granularity**: While `self.errors` collects issues, the exact behavior of the original script on encountering an error (e.g., whether it would exit immediately or continue processing) might differ. The Python implementation currently collects errors and continues, but this can be adjusted if an immediate halt is required.
*   **`parser_filmeta` / `parser_filfile` Equivalence**: The original `parser_filmeta` and `parser_filfile` functions dynamically generated shell pipelines using `eval`. The Python `parse_template` method implicitly covers their intent by parsing definitions and applying transformations. However, any subtle, highly dynamic behaviors or side effects of the original `eval`-based approach might not be perfectly replicated. Comprehensive testing is crucial here.

## 6. Validation

Validation of the migrated `alis_parser.py` involves ensuring functional equivalence with the original `h_alis_parser.ksh` script.

1.  **Unit Tests**:
    *   **How to run**: Execute the provided `if __name__ == "__main__":` block in `alis_parser.py` or a dedicated test suite (e.g., using `pytest`).
    *   **What "passing" means**: Individual methods (`_parser_filattrib`, `_process_single_list_item_template`) and core parsing logic within `parse_template` correctly handle their specific responsibilities. This includes:
        *   Correct variable substitution.
        *   Accurate list expansion, including `<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>` logic for various list sizes (empty, single, multiple).
        *   Proper handling of `LISTDEF` parsing and storage.
        *   Correct timestamp generation with various formats and offsets.
        *   Successful file inclusion and recursive parsing of included content.
        *   Error messages are correctly captured in `parser.errors` for invalid inputs.

2.  **Integration Tests (Functional Equivalence)**:
    *   **How to run**:
        *   Create a comprehensive suite of test templates (`.tmpl` files) and associated data files (`.dat` files for includes) that represent all known use cases and edge cases of the original `h_alis_parser.ksh`.
        *   For each test case:
            1.  Execute the original `h_alis_parser.ksh` script with the test template and capture its standard output.
            2.  Execute the new `alis_parser.py` (via its `parse_template` method) with the same test template and capture its output.
            3.  Compare the outputs.
    *   **What "passing" means**:
        *   The output generated by `alis_parser.py` for each test template **matches byte-for-byte** (or semantically, allowing for minor whitespace differences if deemed acceptable) the output of the original `h_alis_parser.ksh`.
        *   No critical errors are reported by `alis_parser.py` (i.e., `parser.errors` list is empty or contains only expected, non-critical warnings).
        *   All variables, lists, includes, and timestamps are correctly processed and rendered as per the original script's behavior.

## 7. Rollback procedure

In the event of issues with the migrated Python parser, the following rollback procedure should be followed:

1.  **Halt New Invocations**: Immediately stop any new invocations of the deployed Python Cloud Function or Dataflow job. This might involve disabling the Cloud Function, pausing the Dataflow job, or stopping any associated Cloud Scheduler/Cloud Composer triggers.
2.  **Revert Invocation Source**: Revert any changes made to upstream systems or scripts that were modified to call the new Python service. Point them back to invoke the original `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh` script.
3.  **Verify Original Script Functionality**: Ensure that the original `h_alis_parser.ksh` script and its execution environment are still available and fully functional. Run a quick smoke test using the original script to confirm it produces expected output.
4.  **Undeploy Python Service**: Undeploy or delete the Python Cloud Function or Dataflow job from the Google Cloud environment.
5.  **Data Reconciliation (Conditional)**: If the Python parser wrote any data to BigQuery or other downstream systems, assess the impact of the rollback. Depending on the nature of the data and the issue, this may require:
    *   Deleting data written by the Python service.
    *   Restoring previous versions of tables from backups.
    *   Running a data reconciliation process to ensure consistency.
    *   Re-processing data using the original KornShell script.
    *   This step is highly dependent on the specific data flow and usage of the parser's output.
6.  **Monitor**: Monitor the systems after rollback to ensure stability and correct operation using the original script.