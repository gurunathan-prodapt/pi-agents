# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`. This script, primarily a text-pattern parsing and template-expansion utility, has been migrated to Google Cloud Platform, leveraging **BigQuery SQL Stored Procedures** for core text manipulation logic and **external orchestration components** (e.g., Cloud Functions, Dataflow) for dynamic file handling and complex `eval`-based logic. Cloud Storage will serve as the primary storage for input files.

## 2. Generated artifacts

The migration has produced the following BigQuery SQL stored procedures:

*   **`bigquery/stored_procedures/parser_fillist_bq.sql`**
    *   **Role:** This stored procedure translates the `parser_fillist` function from the original KornShell script. It is responsible for expanding `<LIST>` blocks within a given template text, replacing `<FIRST>`, `<MIDDLE>`, `<END>`, and `<SINGLE>` placeholders based on the provided list of values. It handles cases for empty, single-element, and multi-element lists.
*   **`bigquery/stored_procedures/parser_filattrib_bq.sql`**
    *   **Role:** This stored procedure translates the `parser_filattrib` function. Its purpose is to perform global substitution of a specified placeholder (e.g., `<PLACEHOLDER>`) with a given value within the input text.
*   **`bigquery/stored_procedures/parser_getnode_bq.sql`**
    *   **Role:** This stored procedure translates the `parser_getnode` function. It extracts the content enclosed between specified XML-like opening and closing tags (`<node_name>...</node_name>`) from a larger input text. It includes basic error handling for cases where the node is not found.
*   **`bigquery/stored_procedures/parser_bq.sql`**
    *   **Role:** This is the main orchestrator stored procedure, translating the top-level `parser` function. It manages the overall text transformation process, including iterative substitutions, variable management (via BigQuery script variables), and calling the other `parser_*_bq` sub-procedures. It includes a scaffold for timestamp substitution and a loop structure to mimic the iterative nature of the original script's parsing. Note that the full logic for dynamic attribute/list fetching and inclusion resolution is expected to be integrated here, potentially relying on external staging data.

## 3. Key design decisions

*   **BigQuery SQL Stored Procedures for Core Logic**: The decision to use BigQuery SQL Stored Procedures was driven by BigQuery's robust string manipulation functions (`REGEXP_REPLACE`, `REPLACE`, `SUBSTR`, `STRPOS`, `SPLIT`, `ARRAY_TO_STRING`), array capabilities, and procedural language features (`DECLARE`, `SET`, `IF`, `WHILE`, `CALL`). This allows for direct translation of the text processing logic into a managed, scalable BigQuery environment.
*   **Externalization of File I/O and Dynamic `eval`**:
    *   **File System Access**: The original script's reliance on direct file system access (e.g., `INCLUDE` directives, `cat` commands) cannot be directly replicated in BigQuery SQL. This functionality is externalized to a pre-processing layer (e.g., Cloud Functions or Dataflow). This external component will be responsible for reading files from Cloud Storage, resolving `INCLUDE` directives, and providing the fully materialized text content to BigQuery, either as a string parameter or by populating staging tables.
    *   **Dynamic `eval` Logic**: The KornShell script's heavy use of `eval` for dynamic command construction and execution (`parser_filmeta`, `parser_filfile`) is fundamentally incompatible with BigQuery SQL's static procedural nature. This has been re-architected to use structured BigQuery scripting (e.g., `WHILE` loops, `IF/THEN/ELSE` statements) and explicit `CALL`s to the generated stored procedures. This requires parsing definition files into BigQuery-consumable formats (e.g., staging tables) and then programmatically invoking the appropriate BigQuery procedures based on these definitions.
*   **State Management**: Global shell variables and list definitions are translated into BigQuery script variables (`DECLARE`, `SET`) and potentially temporary tables or arrays within the `parser_bq` procedure to maintain state across iterative transformations.
*   **Addressing "Retire" Status**: The migration proceeds despite the `retire` bucket status, implying a re-evaluation of the script's business value or a specific requirement to port its functionality to BigQuery. This decision acknowledges the potential for misdirected effort if the retirement status is confirmed.
*   **Trade-offs**:
    *   **Increased Orchestration Complexity**: Introducing external components for file I/O and dynamic logic adds an orchestration layer (e.g., Cloud Composer/Workflows) that was not present in the standalone shell script.
    *   **Loss of `eval` Flexibility**: While `eval` offers extreme flexibility, its removal in favor of structured BigQuery code improves maintainability, security, and predictability, but requires a more rigid, pre-defined approach to dynamic logic.
    *   **Performance Characteristics**: BigQuery is optimized for large-scale data warehousing. While its string functions are powerful, extensive, iterative string manipulation on very large single text blocks might have different performance and cost implications compared to a shell script optimized for text streams.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset (e.g., `dataset`) where the stored procedures will reside is created.
    *   `CREATE SCHEMA IF NOT EXISTS \`your_gcp_project.dataset\`;`
2.  **IAM Permissions**:
    *   **BigQuery**: The service account executing the BigQuery procedures (e.g., via Cloud Composer, Cloud Functions, or direct API calls) must have `BigQuery Data Editor` or `BigQuery User` roles on the target dataset, and `BigQuery Job User` for running queries.
    *   **Cloud Storage**: If using Cloud Storage for input files, the service account for the external pre-processor (Cloud Function/Dataflow) needs `Storage Object Viewer` and potentially `Storage Object Creator` roles on the relevant buckets.
    *   **Cloud Functions/Dataflow**: The service account for these services needs appropriate execution roles and permissions to interact with BigQuery and Cloud Storage.
3.  **External Pre-processor Deployment**:
    *   Develop and deploy the external component (e.g., Cloud Function, Dataflow job) responsible for:
        *   Reading initial template files and definition files from Cloud Storage.
        *   Resolving `INCLUDE` directives by fetching content from Cloud Storage.
        *   Flattening the input text and parsing definition files (for lists, attributes) into a structured format suitable for BigQuery.
        *   Loading this pre-processed data into BigQuery staging tables or passing it as parameters to the main `parser_bq` procedure.
4.  **BigQuery Staging Table Creation**:
    *   Create any necessary BigQuery staging tables to hold input templates, definition files (e.g., `parser_definitions_attributes`, `parser_definitions_lists`), and pre-processed content.
    *   Example DDL for a definition table:
        ```sql
        CREATE TABLE IF NOT EXISTS `your_gcp_project.dataset.parser_definitions_attributes` (
            placeholder STRING,
            value STRING
        );
        CREATE TABLE IF NOT EXISTS `your_gcp_project.dataset.parser_definitions_lists` (
            list_name STRING,
            list_values ARRAY<STRING>
        );
        ```
5.  **Initial Data Ingestion**:
    *   Load all initial template files, definition files, and any other static input data into Cloud Storage buckets.
    *   Populate the BigQuery staging tables with the parsed definitions and initial template content.
6.  **Scheduling and Orchestration**:
    *   Set up a scheduling mechanism (e.g., Cloud Composer, Cloud Workflows, Cloud Scheduler triggering Cloud Functions) to orchestrate the end-to-end process:
        *   Trigger the external pre-processor.
        *   Execute the main `dataset.parser_bq` stored procedure.
        *   Handle the output (e.g., write to a BigQuery table, Cloud Storage, or pass to another service).

## 5. Known gaps & unresolved references

*   **"Retire" Migration Bucket**: The most significant unresolved item is the `retire` classification of the original script. Further clarification is needed to confirm if this migration effort aligns with current business strategy. If the script is truly meant to be retired, this migration might be unnecessary.
*   **Full `parser_bq` Implementation**: The provided `parser_bq.sql` is a functional scaffold. The complete logic for dynamically fetching attributes and lists from staging tables (as described in the "Transformation Logic" section of the design document) and iteratively calling `parser_filattrib_bq` and `parser_fillist_bq` needs to be fully implemented. This includes the logic to replace `parser_filmeta` and `parser_filfile`.
*   **External Pre-processor Detail**: The external pre-processing component (for `INCLUDE` resolution and `eval` logic translation) is critical but remains a conceptual design. Its detailed implementation, including error handling, scalability, and integration with BigQuery, needs to be fully specified and built.
*   **Comprehensive Error Handling**: While basic error handling is present (e.g., `SIGNAL SQLSTATE` in `parser_getnode_bq`), a robust error handling strategy for all edge cases, including malformed input, missing definitions, and unexpected data, needs to be designed and implemented across all components.
*   **Performance and Cost Optimization**: For very large input texts or high-frequency execution, the performance and cost of BigQuery's string operations should be monitored. Potential optimizations might include batching inputs or refining regex patterns.
*   **Variable Scope and State**: The original script's use of global shell variables and dynamic state management needs careful mapping to BigQuery script variables and temporary tables to ensure identical behavior. This requires thorough testing.

## 6. Validation

Validation of the migrated `h_alis_parser.ksh` functionality will involve a multi-tiered approach:

1.  **Unit Testing (BigQuery Stored Procedures)**:
    *   **How to run**: Each BigQuery stored procedure (`parser_fillist_bq`, `parser_filattrib_bq`, `parser_getnode_bq`, `parser_bq`) should be tested in isolation. This can be done by executing `CALL` statements directly in the BigQuery console or via the `bq` command-line tool, providing various sample inputs (e.g., different list sizes, placeholder values, node structures).
    *   **What "passing" means**: The output `template_text` or `node_text` from the BigQuery procedure must exactly match the expected output generated by the corresponding function in the original KornShell script for the same inputs. Error conditions (e.g., node not found) should be correctly signaled.

2.  **Integration Testing (End-to-End Pipeline)**:
    *   **How to run**: Execute the full end-to-end pipeline, starting from the external pre-processor (if applicable), through data ingestion into BigQuery staging tables, and finally calling the main `dataset.parser_bq` stored procedure. This should be done with a comprehensive suite of test cases derived from the legacy system's usage patterns.
    *   **What "passing" means**:
        *   The final `output_text` returned by `dataset.parser_bq` for a given input must be byte-for-byte identical to the STDOUT of the original `h_alis_parser.ksh` script when run with the equivalent input.
        *   All intermediate steps (pre-processing, staging table population) complete successfully without errors.
        *   The execution completes within acceptable time and cost limits.

3.  **Regression Testing**:
    *   Utilize existing test cases (if any) from the legacy system to ensure that the migrated solution produces identical results for known scenarios.
    *   Create new test cases for edge scenarios, including empty inputs, malformed templates, and large data volumes.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Rollback**:
    *   **Revert Orchestration**: Immediately disable or reconfigure any new orchestration (e.g., Cloud Composer DAGs, Cloud Workflows, Cloud Scheduler jobs) that triggers the migrated BigQuery solution.
    *   **Re-enable Legacy Job**: Re-enable the original KornShell script (`h_alis_parser.ksh`) in its legacy execution environment. Ensure that any upstream processes are re-pointed to use the legacy script.
    *   **Monitor Legacy System**: Verify that the legacy system is functioning correctly and processing data as expected.

2.  **Post-Rollback Clean-up (if necessary)**:
    *   **Delete BigQuery Procedures**: If the rollback is deemed permanent or requires significant re-development, delete the deployed BigQuery stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `your_gcp_project.dataset.parser_fillist_bq`;
        DROP PROCEDURE IF EXISTS `your_gcp_project.dataset.parser_filattrib_bq`;
        DROP PROCEDURE IF EXISTS `your_gcp_project.dataset.parser_getnode_bq`;
        DROP PROCEDURE IF EXISTS `your_gcp_project.dataset.parser_bq`;
        ```
    *   **Delete Staging Tables**: Drop any BigQuery staging tables created for the migration.
    *   **Remove External Components**: Delete or disable the deployed Cloud Functions, Dataflow jobs, or other external components related to the migration.
    *   **Remove Cloud Storage Data**: Delete any specific Cloud Storage buckets or objects created solely for the migrated job's input/output.

This rollback strategy ensures a quick return to a stable state using the proven legacy system while allowing time for investigation and resolution of the issues encountered with the migrated solution.