# MIGRATION_NOTES.md: h_alis_parser.ksh

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`.

The original script functions as a sophisticated pattern and template parsing framework for code generation. It processes template text, replacing placeholders for scalar attributes, expanding lists, handling nested list references, integrating content from included files, processing meta-blocks, and performing timestamp substitutions.

The script has been migrated to **Google BigQuery**, leveraging its capabilities for data processing and procedural logic. The new architecture utilizes BigQuery Stored Procedures for orchestration, BigQuery User-Defined Functions (UDFs) for complex string and timestamp manipulations, and Cloud Storage for initial data staging. Orchestration is managed via Cloud Composer.

## 2. Generated artifacts

The migration produced the following BigQuery and orchestration artifacts:

*   **`ddl/template_tables.sql`**
    *   **Role:** Defines the BigQuery Data Definition Language (DDL) for staging tables that hold the input data for the parsing process. This includes `template_lines` (for the main template text), `include_map` (for included file content), `meta_blocks` (for meta-block definitions), `list_values` (for list elements), and `scalar_values` (for scalar attribute key-value pairs). These tables replace the original script's reliance on STDIN and filesystem access.

*   **`udfs/get_node.sql`**
    *   **Role:** A BigQuery SQL User-Defined Function (UDF) designed to extract content within named XML-like nodes (e.g., `<NODE>...</NODE>`). This UDF replaces a part of the functionality found in the original `parser_getnode` function.

*   **`udfs/expand_list_block.sql`**
    *   **Role:** A BigQuery SQL UDF responsible for implementing the list expansion logic. It iterates through list elements and performs replacements for placeholders like `{{LIST_NAME[i]}}`, replicating the core behavior of the original `parser_fillist` function.

*   **`udfs/parse_timestamp_expression.js`**
    *   **Role:** A BigQuery JavaScript UDF that parses and applies custom timestamp expressions. This UDF handles the specific grammar for timestamp arithmetic (e.g., `1d`, `2y-3m`, `5hT`) previously managed by the `date` command and custom shell logic in the original script. It provides the flexibility needed for complex regex parsing.

*   **`stored_procedures/parser_main.sql`**
    *   **Role:** The central BigQuery Stored Procedure that orchestrates the entire template parsing logic. It replicates the core functionality of the original `parser`, `parser_filmeta`, and `parser_filfile` functions. This procedure manages scalar replacements, list expansions, include directives, timestamp processing, and the iterative convergence loop. It uses temporary tables to maintain state during processing.

*   **`orchestration/example_dag.py`**
    *   **Role:** An example Airflow Directed Acyclic Graph (DAG) demonstrating how to trigger the `parser_main` BigQuery stored procedure. This DAG serves as a blueprint for scheduling and managing the execution of the BigQuery parsing process within a Cloud Composer environment.

## 3. Key design decisions

The migration of `h_alis_parser.ksh` involved several key design decisions to translate its complex, dynamic shell-based logic into a BigQuery-compatible architecture:

*   **Target Platform Selection (BigQuery):** BigQuery was chosen to leverage its robust capabilities for large-scale data processing, SQL-based procedural logic, and integration with the Google Cloud ecosystem. This allows for a more scalable, maintainable, and cost-effective solution compared to maintaining complex shell scripts.

*   **Replacement of Dynamic Shell Execution with Static SQL:** The original script heavily relied on dynamic code generation and execution using `eval` with `sed`, `awk`, and other shell utilities. This dynamic behavior is a significant challenge for migration. The design decision was to re-implement the *intent* of these dynamic operations as explicit, static BigQuery SQL procedural logic and UDFs. This means translating the logic that *generates* shell commands into direct BigQuery SQL statements that achieve the same outcome.

*   **Data Staging and Ingestion:** Direct filesystem access for template files, included files, and configuration was replaced by a structured data flow:
    *   All input files are first staged in **Cloud Storage**.
    *   From Cloud Storage, data is loaded into dedicated **BigQuery staging tables** (`template_lines`, `include_map`, `meta_blocks`, `list_values`, `scalar_values`). This provides a structured, queryable source for the BigQuery parsing logic.

*   **Strategic Use of UDFs:**
    *   **JavaScript UDFs (`parse_timestamp_expression.js`):** Employed for highly complex string parsing or regex operations, particularly for the custom timestamp expression grammar, where native SQL functions might be less ergonomic or performant. This allows for more flexible and powerful pattern matching.
    *   **SQL UDFs (`get_node.sql`, `expand_list_block.sql`):** Used for specific, reusable string manipulation or block extraction logic that can be efficiently expressed in standard SQL.

*   **Iterative Processing within Stored Procedures:** The original script's core `parser` function used an iterative `do...while` loop to apply transformations until convergence. This was replicated in the `parser_main` BigQuery Stored Procedure using BigQuery's `LOOP` and `WHILE` statements. Temporary tables are used to hold the `current_text` and track changes, ensuring that transformations are applied sequentially and iteratively until no further changes occur or a maximum iteration limit is reached.

*   **Trade-offs:**
    *   **Fidelity vs. Maintainability:** A direct, byte-for-byte translation of every dynamic shell command was deemed impractical and would result in an unmaintainable BigQuery solution. The chosen approach prioritizes translating the *functional intent* into idiomatic BigQuery SQL, which may lead to minor behavioral differences in edge cases but significantly improves maintainability and scalability.
    *   **Performance of Iterative Updates:** While BigQuery is highly optimized, iterative updates on temporary tables within a stored procedure can be less performant than highly optimized, compiled shell utilities for certain operations. This requires careful monitoring and potential optimization post-migration.
    *   **Complexity of Translation:** The manual translation of dynamic `eval`-based logic into static SQL is inherently complex and requires a deep understanding of the original script's intricate behaviors.

## 4. Manual steps before go-live

Before the migrated `h_alis_parser.ksh` functionality can go live in BigQuery, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` as referenced in the generated code) exists in your GCP project. If not, create it.

2.  **IAM and Permissions Configuration:**
    *   **Service Account for Orchestration:** The service account used by Cloud Composer (or other orchestrator) must have the following BigQuery roles:
        *   `BigQuery Data Editor` (for creating/updating tables and executing stored procedures).
        *   `BigQuery Job User` (for running BigQuery jobs).
        *   `BigQuery Data Viewer` (to read from source tables).
    *   **Cloud Storage Permissions:** The service account must also have `Storage Object Viewer` and `Storage Object Creator` roles for the Cloud Storage buckets used for staging input files and potentially exporting output.

3.  **Cloud Storage Staging:**
    *   **Upload Input Files:** All template files (which were originally passed via STDIN or referenced), include files, meta-block definitions, list value definitions, and scalar value definitions must be uploaded to a designated Cloud Storage bucket. These files will serve as the source for populating the BigQuery staging tables.

4.  **BigQuery Staging Table Population:**
    *   **Ingest Data:** Load the content from the Cloud Storage staged files into the respective BigQuery staging tables defined in `ddl/template_tables.sql`:
        *   `project.dataset.template_lines`
        *   `project.dataset.include_map`
        *   `project.dataset.meta_blocks`
        *   `project.dataset.list_values`
        *   `project.dataset.scalar_values`
    *   This can be done using BigQuery load jobs (e.g., `bq load`), external tables, or Dataflow/Cloud Functions for more complex parsing during ingestion. This step is crucial as it provides the structured input for the `parser_main` stored procedure.

5.  **Airflow GCP Connection:**
    *   Verify that the Airflow environment (Cloud Composer) has a configured GCP connection (e.g., `google_cloud_default`) that uses the service account with the necessary IAM permissions.

6.  **Parameter Configuration:**
    *   Review and update the `template_id_param` and `output_table_name` parameters in the `orchestration/example_dag.py` to match your specific template IDs and desired output table names.

## 5. Known gaps & unresolved references

The migration involved significant re-engineering due to the highly dynamic nature of the original KornShell script. While the core functionality has been translated, some aspects represent known gaps or require further attention:

*   **Dynamic `eval` of Shell Pipelines:** The original script's extensive use of `eval` to execute dynamically constructed `sed`, `awk`, and other shell pipelines has been replaced by explicit BigQuery SQL. While the *intent* is preserved, there's an inherent risk of subtle behavioral differences or edge cases not fully captured by the static SQL translation. Thorough testing is required.

*   **Arbitrary File Inclusion:** The original script could `INCLUDE` arbitrary files from the filesystem. In the BigQuery migration, this is constrained. All files intended for inclusion must be pre-ingested into Cloud Storage and subsequently loaded into the `project.dataset.include_map` table. Any attempt to `INCLUDE` a file not present in this map will result in an error or an unexpanded placeholder.

*   **Complex `sed`/`awk` Script Generation:** The dynamic generation of complex `sed` and `awk` scripts in the original `parser_fillist`, `parser_filmeta`, and `parser_filfile` functions is not directly replicated. Instead, their functional outcomes are achieved using BigQuery's `REGEXP_REPLACE`, `REPLACE`, `SPLIT`, and procedural logic. This translation might not perfectly capture all nuances of the original `awk` state management or `sed`'s multi-line pattern space.

*   **Timestamp Expression Grammar:** While the `parse_timestamp_expression.js` UDF aims to replicate the custom timestamp arithmetic, the exact parsing and truncation logic (e.g., `5hT`) from the original `date` command and custom shell logic needs rigorous validation against all possible input formats.

*   **Performance of Iterative Transformations:** The `parser_main` stored procedure uses an iterative `LOOP` with temporary tables. While designed for convergence, this approach might be less performant for very large templates or a high number of iterations compared to the highly optimized shell utilities. Performance monitoring and potential optimization (e.g., using `MERGE` statements more extensively) will be necessary.

*   **Simplified `LISTDEF` and `PARSEIGNORE` Handling:**
    *   The `parser_main.sql` stored procedure currently handles `LISTDEF` directives by attempting to extract list names and values from a single line (e.g., `LISTDEF MYLIST="val1","val2"`). The original KSH script had more sophisticated block parsing for `LISTDEF` that could span multiple lines. This is a simplification and may require refinement if complex multi-line `LISTDEF`s are in use.
    *   The `PARSEIGNORE` directive, which in the original script would ignore blocks for further processing, is largely ignored in the current BigQuery implementation due to the line-by-line iterative model. If `PARSEIGNORE` functionality is critical, a more complex block-aware parsing mechanism would be required.

*   **`parser_getnode` and `parser_filmeta` Translation:** The `get_node` UDF provides basic regex-based node extraction. The original `parser_filmeta` and `parser_filfile` functions involved dynamic `awk` scripts and `eval` to process meta-blocks and files. The `parser_main` SP aims to achieve the *outcome* of these functions through explicit SQL, but the underlying dynamic generation of processing logic is not directly translated.

## 6. Validation

Validation of the migrated `h_alis_parser.ksh` functionality will involve a multi-faceted approach to ensure correctness and performance.

### How to Run Tests:

1.  **Unit Tests for UDFs:**
    *   Execute each UDF (`project.dataset.get_node`, `project.dataset.expand_list_block`, `project.dataset.parse_timestamp_expression`) directly in BigQuery with a comprehensive set of test cases, including valid inputs, edge cases, and invalid inputs.
    *   Example for `get_node`:
        ```sql
        SELECT project.dataset.get_node('<root><NODE>content</NODE></root>', 'NODE');
        SELECT project.dataset.get_node('no node here', 'NODE');
        ```
    *   Example for `parse_timestamp_expression`:
        ```sql
        SELECT project.dataset.parse_timestamp_expression('1d', '2023-01-01 12:00:00');
        SELECT project.dataset.parse_timestamp_expression('2y-3m', '2023-01-01 12:00:00');
        SELECT project.dataset.parse_timestamp_expression('5hT', '2023-01-01 12:34:56');
        ```

2.  **Unit Tests for `parser_main` Stored Procedure:**
    *   Prepare small, controlled BigQuery staging tables (`template_lines`, `scalar_values`, `list_values`, `include_map`) that represent specific test scenarios (e.g., a template with only scalar replacements, a template with list expansions, a template with includes, a template with timestamp directives, a template with `NOPRINT`).
    *   Execute the `project.dataset.parser_main` stored procedure for each scenario:
        ```sql
        CALL project.dataset.parser_main('test_template_scalar', 'project.dataset.output_scalar_test');
        CALL project.dataset.parser_main('test_template_list', 'project.dataset.output_list_test');
        ```
    *   Inspect the generated output tables (`project.dataset.output_scalar_test`, etc.) to verify correctness.

3.  **Integration Tests (End-to-End):**
    *   **Prepare Production-like Data:** Ingest a representative set of actual production templates, include files, list definitions, and scalar values into the BigQuery staging tables.
    *   **Run via Orchestration:** Deploy the `orchestration/example_dag.py` (or your customized DAG) to Cloud Composer and trigger it. Ensure the DAG successfully executes the `parser_main` stored procedure.
    *   **Compare Outputs:**
        *   For each test case, run the original `h_alis_parser.ksh` script with its corresponding inputs and capture its STDOUT.
        *   Export the output from the BigQuery `parser_main` stored procedure (e.g., to Cloud Storage as a single text file).
        *   Perform a byte-for-byte comparison between the output of the original script and the BigQuery generated output. Tools like `diff` or programmatic comparisons should be used.

### What "Passing" Means:

*   **Functional Equivalence:** The output text generated by `project.dataset.parser_main` must be **byte-for-byte identical** to the output of the original `h_alis_parser.ksh` for a comprehensive suite of test cases covering all known features and edge cases. Any discrepancies must be investigated and resolved.
*   **No Errors:** The BigQuery jobs (UDFs, stored procedures) and the Cloud Composer DAG must execute without any errors or unexpected warnings.
*   **Performance:** The execution time and BigQuery cost for processing typical production templates should be within acceptable limits, ideally matching or improving upon the original script's performance.
*   **Resource Utilization:** BigQuery slot consumption and other resource metrics should be monitored to ensure efficient operation.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after deploying the migrated `h_alis_parser.ksh` functionality, the following rollback procedure should be followed:

1.  **Immediate Action (Orchestration):**
    *   **Cloud Composer:** If the new DAG (`h_alis_parser_bigquery_migration`) was deployed and is causing issues, immediately pause or delete the DAG in Cloud Composer. This will prevent further execution of the BigQuery parsing process.
    *   **Other Orchestrators:** For other orchestration tools, disable or revert the job/workflow that triggers the BigQuery stored procedure.

2.  **Revert BigQuery Code (If Necessary):**
    *   If the issue is traced to the BigQuery UDFs or Stored Procedures, you may need to:
        *   Drop the newly created UDFs: `project.dataset.get_node`, `project.dataset.expand_list_block`, `project.dataset.parse_timestamp_expression`.
        *   Drop the `project.dataset.parser_main` stored procedure.
        *   *(Note: This step is usually only required if the BigQuery code itself is fundamentally flawed and cannot be quickly patched forward.)*

3.  **Data Rollback (Minimal Impact):**
    *   The BigQuery output tables generated by `parser_main` (e.g., `project.dataset.parsed_output_table`) are new and do not overwrite existing production data. Therefore, no data rollback is strictly required for existing datasets.
    *   However, any downstream processes that were configured to consume the output of the new BigQuery parsing process must be reverted to their previous input sources or paused until the issue is resolved.

4.  **Re-enable Original System:**
    *   **Restore Original Script:** Ensure the original `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh` script is fully operational and its associated scheduling or invocation mechanisms are re-enabled.
    *   **Verify Functionality:** Confirm that the original script is producing the expected output and that all dependent systems are functioning correctly.

5.  **Post-Rollback Analysis:**
    *   Thoroughly investigate the root cause of the issue that necessitated the rollback.
    *   Address any identified bugs, design flaws, or configuration errors in the migrated BigQuery solution.
    *   Re-validate the corrected solution in a staging environment before attempting another deployment.