# MIGRATION_NOTES.md — vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

## 1. Summary

This document details the migration of the `h_alis_parser.ksh` shell script, a versatile template parser and code generation helper, from its legacy ksh environment to Google BigQuery. The script's core functionality, including scalar attribute replacement, list expansion, node/block extraction, recursive file inclusion, and timestamp substitution, has been re-implemented using BigQuery Scripting, Stored Procedures, and SQL functions.

## 2. Generated Artifacts

The migration process has produced the following BigQuery SQL artifacts:

*   **`project.dataset.ddl.sql`**
    *   **Role**: Defines the necessary staging tables in BigQuery. These tables (`template_files`, `include_files`, `parser_output`) are used to store the content of input template files, included files, and the final generated output, respectively, replacing the original script's direct filesystem access.
*   **`project.dataset.replace_placeholder.sql`**
    *   **Role**: A temporary SQL function that performs simple string replacement. It replicates the functionality of `nawk`'s `gsub` or `sed` for scalar attribute substitution (e.g., `<PLACEHOLDER>` with a value).
*   **`project.dataset.expand_list_template.sql`**
    *   **Role**: A temporary SQL function designed to handle list attribute expansion within templates. It replaces specific placeholders like `<FIRST>`, `<MIDDLE>`, `<END>`, and `<SINGLE>` based on an array of provided list values, mimicking the complex `sed` logic from the original script.
*   **`project.dataset.extract_node.sql`**
    *   **Role**: A temporary SQL function that extracts specific sections (nodes) from a given text block. It identifies content between `<node_name>` and `</node_name>` tags, replicating the `nawk` logic used for node extraction.
*   **`project.dataset.parser_main.sql`**
    *   **Role**: The main BigQuery Stored Procedure that orchestrates the entire parsing and code generation process. It encapsulates the core logic of the original `parser()` function, iteratively processing template lines, managing variables and lists, handling includes, and applying transformations using the helper functions. It replaces the complex `nawk` script and the overall shell script flow.

## 3. Key Design Decisions

The following key design decisions were made to translate the shell script's functionality to BigQuery:

*   **BigQuery Scripting and Stored Procedures**: The procedural nature of the original ksh script, with its loops, conditionals, and function calls, is directly mapped to BigQuery Scripting and Stored Procedures. This allows for state management (variables, lists) and iterative processing within a SQL environment.
*   **Staging Tables for File System Access**: Direct file system operations (`cat`, `getline`) are replaced by pre-ingesting template and include file contents into dedicated BigQuery staging tables (`template_files`, `include_files`). This centralizes data access within BigQuery.
*   **SQL Functions for String Manipulation**: `sed`, `nawk`, and `perl` commands are replaced by BigQuery's rich set of standard SQL string functions (`REGEXP_REPLACE`, `SPLIT`, `STRING_AGG`, `REGEXP_CONTAINS`) and custom temporary functions (`replace_placeholder`, `expand_list_template`, `extract_node`). This leverages BigQuery's native capabilities for text processing.
*   **Procedural Handling of Dynamic `eval`**: The original script's use of `eval` for dynamically constructing and executing shell pipelines (e.g., for `parser_filmeta` and `parser_filfile`) is re-engineered. Instead of dynamic SQL (`EXECUTE IMMEDIATE`), the `parser_main` procedure directly interprets and applies the directives (variable assignments, list definitions) through explicit loops and conditional logic, providing a more structured and secure approach.
*   **In-Memory Array for Recursive Includes**: Recursive file inclusions are handled by dynamically modifying an in-memory array (`v_processed_lines`) within the `parser_main` procedure. When an `<INCLUDE FILE>` directive is encountered, the content of the included file is fetched from the `include_files` staging table and inserted directly into the processing stream, ensuring correct sequential processing.
*   **Temporary Tables for State Management**: Instead of shell variables and arrays, BigQuery Scripting variables (`v_var_map`, `v_list_map`) and potentially temporary tables are used to maintain the state of defined variables and lists throughout the parsing process.

## 4. Manual Steps Before Go-Live

Before the migrated BigQuery solution can be used in production, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it using the Google Cloud Console or `bq mk` command.
2.  **IAM Permissions**: Grant the necessary Identity and Access Management (IAM) roles to the service account or user that will execute the BigQuery procedures. This typically includes `BigQuery Data Editor` for the target dataset and potentially `Storage Object Viewer` if file ingestion is performed via Cloud Storage.
3.  **Data Ingestion**:
    *   **Template Files**: All original template files (`.ksh` or other text files processed by `h_alis_parser.ksh`) must be ingested into the `project.dataset.template_files` table. Each line of a file should be inserted as a separate record, preserving `file_name` and `line_no`.
    *   **Include Files**: Any files referenced by `<INCLUDE FILE>` or `<LISTDEF FILE>` directives must be ingested into the `project.dataset.include_files` table, following the same structure as template files.
    *   This ingestion can be done via `bq load` commands, Cloud Storage transfers, Dataflow jobs, or Cloud Functions.
4.  **Orchestration Setup**: Configure an external orchestrator (e.g., Cloud Composer/Apache Airflow) to manage the end-to-end workflow. This includes:
    *   Triggering the data ingestion steps.
    *   Executing the `project.dataset.parser_main` stored procedure with appropriate parameters (`p_job_id`, `p_input_file`, `p_input_node`, `p_output_mode`).
    *   Handling any subsequent steps that consume the output from `project.dataset.parser_output`.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps or areas requiring further attention/redesign (B4 items):

1.  **Timestamp Arithmetic**: The original script included logic for timestamp manipulation (e.g., `match(cmd,/[0-9][0-9][0-9][0-9][0-9]* *[-+] *[0-9]*[ymdwhis][0-9]*[t]?.*/))`). The current BigQuery implementation only handles the `<TIMESTAMP>` directive for the current timestamp (`FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP())`). Any date/time arithmetic capabilities are not yet migrated and would require custom SQL logic using `DATE_ADD`, `DATE_SUB`, etc.
2.  **List Range Parsing**: The design document noted the original script's ability to handle list ranges (e.g., `[ms]?[n0-9][0-9]*-[n0-9][0-9]*`). This advanced list indexing and slicing functionality is not implemented in the `expand_list_template` function or `parser_main` procedure, which currently only support `<FIRST>`, `<MIDDLE>`, `<END>`, and `<SINGLE>` generic placeholders.
3.  **Named List Placeholders**: The original `parser_fillist` likely supported named list placeholders (e.g., `<MYLIST.FIRST>`). The current `expand_list_template` and `parser_main` only handle generic list placeholders (`<FIRST>`, `<MIDDLE>`, etc.) and apply the first list found in `v_list_map` that matches. If the original script used multiple named lists concurrently within the same template block, this behavior might differ and requires redesign to associate placeholders with specific list names.
4.  **Generic `eval` Replacement**: While the specific `eval` patterns for variable and list definition from `parser_filmeta` and `parser_filfile` are handled procedurally, the BigQuery solution does not provide a generic replacement for arbitrary `eval` calls that might have existed in the original `h_alis_parser.ksh` for other complex shell command pipelines. If such generic `eval` usage is present, it remains an unaddressed gap.
5.  **Error Handling Granularity**: The BigQuery stored procedure uses `RAISE BQ.EXCEPTION` for error conditions. While functional, it may lack the specific error messages or exit codes that the original shell script provided, which might be important for downstream error handling or logging.
6.  **Performance for Large Inputs**: Iterative string manipulation and array operations within BigQuery Scripting, especially for very large templates or deeply nested includes, might introduce performance overhead compared to highly optimized native shell utilities (`sed`, `nawk`, `perl`). Performance testing with representative large datasets is recommended.

## 6. Validation

To validate the migrated `h_alis_parser.ksh` functionality in BigQuery:

1.  **Prepare Test Data**:
    *   Select a comprehensive set of original template files and their corresponding include files that cover all functionalities (scalar replacement, list expansion, node extraction, includes, timestamps, and various edge cases).
    *   Ingest these files into the `project.dataset.template_files` and `project.dataset.include_files` tables as described in Section 4.
2.  **Execute Original Script**: For each test case, run the original `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh` script with the relevant input file and parameters. Capture its standard output.
3.  **Execute BigQuery Procedure**: For each corresponding test case, execute the `project.dataset.parser_main` stored procedure, passing the `file_name` and `node_name` as parameters.
    ```sql
    CALL `project.dataset.parser_main`(
        p_job_id => 'test_run_123',
        p_input_file => 'your_test_template.txt',
        p_input_node => 'YOUR_NODE_NAME', -- Optional, pass NULL or '' if not applicable
        p_output_mode => 'default' -- Placeholder, not used in current implementation
    );
    ```
4.  **Retrieve BigQuery Output**: Query the `project.dataset.parser_output` table for the `job_id` corresponding to your test run, ordering by `output_line_no`.
    ```sql
    SELECT output_text FROM `project.dataset.parser_output` WHERE job_id = 'test_run_123' ORDER BY output_line_no;
    ```
5.  **Comparison**:
    *   Compare the output from the original shell script (Step 2) with the output retrieved from BigQuery (Step 4).
    *   **Passing Criteria**:
        *   The generated output from BigQuery must be functionally identical (line-by-line, character-for-character) to the output of the original `h_alis_parser.ksh` script for all test cases.
        *   Error conditions (e.g., missing files, missing nodes) should be handled gracefully, and BigQuery exceptions should be raised as expected.
        *   The execution time of the BigQuery procedure should be within acceptable performance thresholds for typical workloads.

## 7. Rollback Procedure

In the event that the migrated BigQuery solution encounters critical issues or does not meet requirements after go-live, the following rollback procedure should be followed:

1.  **Halt New Orchestrations**: Immediately stop any new scheduled or triggered executions of the BigQuery `parser_main` procedure via Cloud Composer/Airflow or other orchestrators.
2.  **Revert to Legacy System**: Reconfigure all upstream processes and consumers to revert to using the original `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh` script in its legacy environment.
3.  **Monitor Legacy System**: Verify that the legacy system is functioning correctly and processing workloads as expected.
4.  **Cleanup (Optional)**: Once the legacy system is stable, the BigQuery staging tables (`template_files`, `include_files`, `parser_output`) and the stored procedure/functions can be dropped or archived if they are no longer needed. This step should only be performed after confirming the successful rollback and if there's no need for forensic analysis of the failed migration attempt.