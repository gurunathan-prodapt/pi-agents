# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`. This script, a generic pattern and template parsing utility, was designated for the `retire` migration bucket, indicating a re-architecture rather than a direct, like-for-like translation.

The functionality of `h_alis_parser.ksh` has been migrated to a hybrid Google Cloud Platform (GCP) architecture:

*   **BigQuery (BQ)**: For storing template definitions, list data, and attributes, and for handling simpler string manipulation tasks via Stored Procedures (SPs) and User-Defined Functions (UDFs).
*   **Cloud Composer (Apache Airflow) / Python**: For orchestrating the overall parsing process, managing complex logic such as recursive file inclusions, dynamic variable substitutions, and interpreting the procedural aspects of the original script that involved dynamic `eval`uation.

The migration aims to replace the legacy KornShell script and its dependencies on local filesystem and standard Unix utilities (`sed`, `nawk`, `perl`, `grep`) with cloud-native, scalable, and maintainable GCP services.

## 2. Generated artifacts

The migration has produced the following artifacts:

*   **`bq/ddl/templates.sql`**
    *   **Role**: BigQuery DDL to create the `templates` table. This table stores the content of template files that were previously read by `h_alis_parser.ksh`, enabling BigQuery and Python components to access template definitions.
*   **`bq/ddl/list_data.sql`**
    *   **Role**: BigQuery DDL to create the `list_data` table. This table stores elements for named lists, replacing the list definitions previously processed by the `parser_fillist` function.
*   **`bq/ddl/attributes.sql`**
    *   **Role**: BigQuery DDL to create the `attributes` table. This table stores scalar attribute key-value pairs, replacing the attribute definitions previously processed by the `parser_filattrib` function.
*   **`bq/stored_procedures/fill_attribute_placeholder.sql`**
    *   **Role**: BigQuery Stored Procedure that replicates the functionality of the `parser_filattrib` function. It performs simple string replacement of a placeholder with a given value within a text block.
*   **`bq/stored_procedures/fill_list_placeholders.sql`**
    *   **Role**: BigQuery Stored Procedure that replicates the functionality of the `parser_fillist` function. It handles the replacement of list-specific placeholders (`<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>`) based on elements retrieved from the `list_data` table.
*   **`bq/stored_procedures/get_node_content.sql`**
    *   **Role**: BigQuery Stored Procedure designed to extract content within `<NODE>...</NODE>` tags and resolve `<NODE INCLUDE file>` directives by looking up content in the `templates` table. This partially replicates the `parser_getnode` function.
*   **`bq/user_defined_functions/format_timestamp_with_offset.sql`**
    *   **Role**: BigQuery User-Defined Function that provides timestamp formatting with optional offsets (e.g., `SYSDATE:-5d:YYYY-MM-DD`), replacing the `date` command and timestamp logic in the original script.
*   **`python/cloud_composer/alis_parser_orchestrator.py`**
    *   **Role**: A Python script intended for deployment on Cloud Composer (or Cloud Functions). This is the core orchestrator that re-implements the complex, dynamic parsing logic of the original `h_alis_parser.ksh`. It interacts with BigQuery tables for data and templates, handles recursive inclusions, applies attribute and list substitutions, and manages timestamp logic programmatically. It replaces the main `parser` function and the dynamic `eval`uation aspects of `parser_filmeta` and `parser_filfile`.

## 3. Key design decisions

The migration strategy for `h_alis_parser.ksh` was heavily influenced by its `retire` migration bucket and its nature as a generic parsing utility.

*   **Hybrid Architecture (BigQuery + Python/Cloud Composer)**:
    *   **Decision**: To split the script's functionality, leveraging BigQuery for data storage and simpler, declarative transformations (DDLs, SPs, UDFs), and Python (orchestrated by Cloud Composer) for complex, procedural, and dynamic logic.
    *   **Rationale**: The original script's dynamic `eval`uation and recursive file processing are not directly translatable to BigQuery SQL. Python offers the necessary programmatic control, string manipulation capabilities, and external library support (e.g., `google-cloud-bigquery`) to re-implement this logic effectively. BigQuery provides a managed, scalable, and cost-effective solution for data storage and simpler transformations.
    *   **Trade-offs**: Introduces a multi-technology solution, which can increase operational complexity compared to a single-language migration. Requires careful management of data flow and state between BigQuery and Python components.

*   **Data Centralization in BigQuery**:
    *   **Decision**: Migrate all template files, list definitions, and attribute values from the legacy filesystem to dedicated BigQuery tables (`templates`, `list_data`, `attributes`).
    *   **Rationale**: This centralizes configuration and template data, making it accessible to all GCP components, improving discoverability, versioning (via BigQuery's time travel), and eliminating dependencies on local filesystems.
    *   **Trade-offs**: Requires an initial data ingestion step to populate these tables. Accessing data from BigQuery might introduce slight latency compared to local file reads, though this is generally negligible for configuration data.

*   **Replacement of Dynamic `eval` with Programmatic Interpretation**:
    *   **Decision**: The `eval` command in the original KornShell script, which allowed for dynamic execution of generated shell commands, is replaced by explicit Python logic in `alis_parser_orchestrator.py`. This Python script programmatically interprets the "commands" (e.g., LIST, ATTRIBUTE definitions) from templates and applies transformations.
    *   **Rationale**: Direct `eval`uation of arbitrary code is a security risk and impossible to replicate in BigQuery SQL. Python provides a safe and controlled environment to achieve similar dynamic behavior by interpreting instructions and applying transformations within the application logic.
    *   **Trade-offs**: Requires a thorough understanding of all possible dynamic behaviors of the original script to ensure complete and accurate re-implementation in Python.

*   **Native BigQuery Functions for String and Timestamp Manipulation**:
    *   **Decision**: Utilize BigQuery's built-in string functions (`REPLACE`, `REGEXP_REPLACE`, `REGEXP_EXTRACT`, `SPLIT`, `ARRAY_AGG`) and timestamp functions (`CURRENT_TIMESTAMP()`, `TIMESTAMP_ADD`, `TIMESTAMP_SUB`, `FORMAT_TIMESTAMP`) where possible.
    *   **Rationale**: Leverages BigQuery's optimized engine for common data manipulation tasks, reducing the need for custom code and improving performance for these specific operations.
    *   **Trade-offs**: Some complex `nawk` or `perl` regex patterns might be challenging to translate directly to BigQuery's `REGEXP` functions, potentially requiring more verbose SQL or offloading to Python. The `format_timestamp_with_offset` UDF is a simplified example; a fully robust solution for all possible legacy date formats/offsets might be more complex.

*   **Handling of Recursive File Includes**:
    *   **Decision**: The recursive file inclusion logic (e.g., `<NODE INCLUDE file>`) is handled by the Python orchestrator, which fetches included content from the BigQuery `templates` table and recursively processes it.
    *   **Rationale**: This procedural logic with state management (e.g., `filelist` stack in the original script) is best handled in a programmatic language like Python.
    *   **Trade-offs**: Requires careful implementation in Python to prevent infinite loops (circular includes) and manage the processing stack.

## 4. Manual steps before go-live

Before the migrated `h_alis_parser.ksh` functionality can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`<DATASET_ID>`) exists within the specified GCP project (`<PROJECT_ID>`). If not, create it:
        ```bash
        bq mk --dataset --default_table_expiration 36000 <PROJECT_ID>:<DATASET_ID>
        ```

2.  **BigQuery DDL Deployment**:
    *   Execute the provided DDL scripts to create the necessary tables, stored procedures, and user-defined functions in BigQuery. Replace `<PROJECT_ID>` and `<DATASET_ID>` placeholders with actual values.
        ```bash
        bq query --use_legacy_sql=false < bq/ddl/templates.sql
        bq query --use_legacy_sql=false < bq/ddl/list_data.sql
        bq query --use_legacy_sql=false < bq/ddl/attributes.sql
        bq query --use_legacy_sql=false < bq/stored_procedures/fill_attribute_placeholder.sql
        bq query --use_legacy_sql=false < bq/stored_procedures/fill_list_placeholders.sql
        bq query --use_legacy_sql=false < bq/stored_procedures/get_node_content.sql
        bq query --use_legacy_sql=false < bq/user_defined_functions/format_timestamp_with_offset.sql
        ```

3.  **Data Ingestion into BigQuery Tables**:
    *   Populate the `templates`, `list_data`, and `attributes` BigQuery tables with the corresponding data extracted from the legacy environment. This involves:
        *   Extracting all template files, meta-block definitions, list elements, and attribute values that `h_alis_parser.ksh` previously processed.
        *   Transforming this data into a format suitable for insertion into the BigQuery tables (e.g., CSV, JSON).
        *   Loading the data using `bq load` commands or BigQuery Data Transfer Service.
        *   **Example for `templates`**:
            ```bash
            # Assuming you have a CSV file with template_name,template_content,description
            bq load --source_format=CSV --autodetect <PROJECT_ID>:<DATASET_ID>.templates gs://your-gcs-bucket/templates.csv
            ```

4.  **IAM / Permissions Setup**:
    *   **Cloud Composer Service Account**: Ensure the service account used by Cloud Composer (or Cloud Functions) has the following BigQuery roles:
        *   `BigQuery Data Editor` (to read/write to `templates`, `list_data`, `attributes` tables)
        *   `BigQuery Job User` (to run queries and stored procedures)
    *   **Other Service Accounts**: Any other service accounts that will trigger or interact with the parsing logic (e.g., Cloud Functions, other Cloud Composer DAGs) must also have appropriate BigQuery permissions.

5.  **Cloud Composer Environment Setup**:
    *   Deploy the `python/cloud_composer/alis_parser_orchestrator.py` script as a DAG in your Cloud Composer environment.
    *   Configure environment variables within the Cloud Composer environment or the DAG definition for `GCP_PROJECT_ID` and `BQ_DATASET_ID`.
    *   Define the DAG's schedule and any necessary parameters (e.g., input template name, specific attributes/lists to override).

6.  **Connection Strings / Secrets**:
    *   The Python script uses the `google.cloud.bigquery` client library, which handles authentication automatically when running in GCP environments (e.g., Cloud Composer, Cloud Functions) using the attached service account. No explicit connection strings or secrets are typically required for BigQuery access in this setup.

## 5. Known gaps & unresolved references

The `retire` migration bucket and the complexity of the original KornShell script introduce several considerations:

*   **Comprehensive `nawk` Logic Translation**: The `nawk` blocks in the original `parser` function are highly procedural and stateful. While the Python orchestrator aims to replicate the core logic, ensuring *every* edge case and subtle behavior of the original `nawk` script is perfectly translated requires extensive testing and might reveal minor discrepancies. The Python implementation provides a programmatic equivalent but might not be a byte-for-byte functional match for all complex `nawk` scenarios.
*   **Dynamic `eval` Coverage**: The original script's use of `eval` allowed for arbitrary shell command execution based on parsed content. While the Python orchestrator replaces this with programmatic interpretation, there's a risk that some highly specific or unusual dynamic behaviors enabled by `eval` might not have been fully anticipated or re-implemented.
*   **Timestamp Offset Parsing Robustness**: The `format_timestamp_with_offset` UDF and the Python `apply_timestamp_substitutions` function provide common timestamp offset capabilities. However, if the original `h_alis_parser.ksh` supported a very wide array of complex or non-standard date/time formats and offsets (e.g., `1m` for 1 month, `1y` for 1 year, or highly specific date arithmetic), these might need further refinement in the Python script or additional BigQuery UDFs.
*   **Nested Node Handling**: The `get_node_content` SP and the Python `resolve_includes` function handle basic node extraction and file inclusions. If the original script supported deeply nested `<NODE>` structures with complex interactions or specific processing order, the current implementation might need enhancements to ensure full fidelity.
*   **`loc-escalated` Flag Implications**: The `loc-escalated` flag on the original script suggests it might have contained highly specific or business-critical logic that was difficult to understand or generalize. While the migration aims for functional equivalence, there's an inherent risk that subtle, undocumented behaviors tied to this complexity might be missed.
*   **`retire` Bucket Justification**: The `retire` status implies that a direct, 1:1 migration was not the primary goal. It's possible that some less critical or obsolete functionalities of the original script were intentionally simplified or omitted during the re-architecture. Any such intentional omissions should be documented and validated with stakeholders.

## 6. Validation

Validation of the migrated `h_alis_parser.ksh` functionality involves a multi-pronged approach:

1.  **Unit Testing BigQuery Components**:
    *   **DDLs**: Verify that tables, SPs, and UDFs are created successfully without errors.
    *   **Stored Procedures (`fill_attribute_placeholder`, `fill_list_placeholders`, `get_node_content`)**:
        *   Create temporary test tables for `templates`, `list_data`, `attributes` with known input data.
        *   Call the SPs with various inputs (e.g., single attribute, multiple list items, empty list, node with include, node without include, node not found).
        *   Assert the output (e.g., modified template string, extracted node content) against expected results.
    *   **UDF (`format_timestamp_with_offset`)**:
        *   Execute `SELECT` statements calling the UDF with different formats and offsets (e.g., `CURRENT_TIMESTAMP()`, `CURRENT_DATE()`, `+1 DAY`, `-5 HOUR`).
        *   Verify the returned formatted timestamp matches the expected value.

2.  **Python Orchestrator Local Testing**:
    *   Run `python/cloud_composer/alis_parser_orchestrator.py` locally with the provided `if __name__ == '__main__':` block.
    *   Ensure it successfully connects to BigQuery, fetches example data, and produces output for the sample templates.
    *   Test various scenarios:
        *   Templates with attributes, lists (single, multiple, empty).
        *   Templates with timestamp substitutions (different formats, positive/negative offsets).
        *   Templates with `<NODE INCLUDE file>` directives (including non-existent includes).
        *   Templates with comments (full line, inline).
        *   Templates with multiple newlines.

3.  **End-to-End Functional Testing**:
    *   **Input Preparation**: Identify a representative set of actual template files and associated input data (attributes, list elements) that were processed by the original `h_alis_parser.ksh` in the legacy environment.
    *   **Legacy Output Capture**: Run the original `h_alis_parser.ksh` script with these inputs and capture its exact output.
    *   **Migrated Output Generation**: Deploy the Python orchestrator to a development Cloud Composer environment. Configure it to process the same set of inputs (now stored in BigQuery tables). Trigger the DAGs and capture their output.
    *   **Comparison**: Perform a byte-for-byte comparison (or a semantically equivalent comparison for whitespace/newlines) between the legacy output and the migrated output.
    *   **Passing Criteria**:
        *   The generated output from the migrated components must be functionally identical to the output of the original `h_alis_parser.ksh` for all tested scenarios.
        *   No errors or warnings should be reported by the BigQuery components or the Python orchestrator during execution.
        *   The execution time and resource consumption should be within acceptable limits for the target GCP environment.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Immediate Action**:
    *   **Revert Downstream Processes**: Immediately reconfigure any downstream processes or jobs that were updated to consume the output of the migrated components to instead use the output from the original `h_alis_parser.ksh` script.
    *   **Disable Migrated Components**:
        *   **Cloud Composer**: Pause or disable the Cloud Composer DAG(s) responsible for running `alis_parser_orchestrator.py`.
        *   **BigQuery**: If any BigQuery SPs/UDFs are directly invoked by other processes, consider revoking execution permissions or renaming them to prevent accidental use.

2.  **Restore Legacy Environment**:
    *   Ensure the original `h_alis_parser.ksh` script and its dependencies (template files, configuration files, local utilities) are fully operational and accessible in the legacy environment.
    *   Verify that the legacy environment can resume processing without issues.

3.  **Cleanup (Optional, Post-Rollback)**:
    *   Once the rollback is confirmed successful and the legacy system is stable, the migrated GCP resources can be cleaned up:
        *   **BigQuery**: Drop the `templates`, `list_data`, `attributes` tables, and the `fill_attribute_placeholder`, `fill_list_placeholders`, `get_node_content` stored procedures, and `format_timestamp_with_offset` UDF.
            ```bash
            bq rm -f <PROJECT_ID>:<DATASET_ID>.templates
            bq rm -f <PROJECT_ID>:<DATASET_ID>.list_data
            bq rm -f <PROJECT_ID>:<DATASET_ID>.attributes
            bq rm -f <PROJECT_ID>:<DATASET_ID>.fill_attribute_placeholder
            bq rm -f <PROJECT_ID>:<DATASET_ID>.fill_list_placeholders
            bq rm -f <PROJECT_ID>:<DATASET_ID>.get_node_content
            bq rm -f <PROJECT_ID>:<DATASET_ID>.format_timestamp_with_offset
            ```
        *   **Cloud Composer**: Delete the deployed DAG(s) from the Cloud Composer environment.

4.  **Root Cause Analysis**:
    *   Investigate the reason for the rollback, identify the issues with the migrated components, and plan for remediation before attempting another migration.