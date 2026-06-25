# Migration Design — vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

## 1. Purpose & Scope
This shell script, `h_alis_parser.ksh`, serves as a versatile parser and code generation helper library. Its primary purpose is to process text templates, replacing various placeholders with dynamic content. This includes:
-   **Scalar attribute replacement**: Substituting specific placeholders like `<PLACEHOLDER>` with concrete values.
-   **List attribute expansion**: Handling list-like structures within templates, allowing for replacement of `<FIRST>`, `<MIDDLE>`, `<END>`, and `<SINGLE>` elements based on the provided list.
-   **Node/block extraction**: Extracting specific sections (`<BEREICH>...</BEREICH>`) from files, including the ability to recursively embed content from other files (`<BEREICH INCLUDE file>`).
-   **Recursive include processing**: Resolving external file inclusions within templates.
-   **Timestamp substitution**: Injecting current timestamps into templates.

The script is fundamental for template-driven generation of code or configuration files, where dynamic content needs to be inserted into static structures.

## 2. Source Inventory
This migration job involves a single source file.

-   **File**: `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`
    -   **Technology**: Shell Script (ksh) utilizing `sed`, `nawk`, `perl`, `grep`, `wc`, `cat`, `printf`, `date`.
    -   **Tier**: Complex (due to dynamic shell `eval`, recursive parsing logic, and intricate string manipulations across multiple tools).
    -   **Automation Bucket**: Semi-Auto (B2) to Manual (B3). While some components can be automated, the dynamic nature of `eval` and recursive file includes will require careful manual design and implementation.

## 3. Target Architecture
The target platform is Google BigQuery. The functionality of `h_alis_parser.ksh` will be replicated using:
-   **BigQuery Scripting**: To orchestrate the parsing logic, handle conditional flows, and manage state.
-   **BigQuery Stored Procedures**: To encapsulate individual parsing functions (e.g., `parser_fillist`, `parser_filattrib`, `parser_getnode`, and the main `parser`).
-   **Temporary Tables**: For intermediate data storage, especially when processing template content, variables, and list elements.
-   **Standard SQL Functions**: For string manipulation (`REGEXP_REPLACE`, `SPLIT`, `STRING_AGG`), conditional logic, and timestamp generation (`CURRENT_TIMESTAMP()`, `FORMAT_TIMESTAMP`).
-   **Cloud Storage / Staging Tables**: To ingest and store template files and include files. The content of these files will be stored in tables (e.g., `project.dataset.template_files`, `project.dataset.include_files`) with columns like `file_name`, `line_no`, and `line_text`.
-   **JavaScript UDFs (conditional)**: Only if extremely complex regex or string manipulation proves unmanageable directly in standard SQL.
-   **External Orchestration (e.g., Cloud Composer/Airflow)**: For managing the overall workflow, especially for recursive file traversal and `eval`-generated shell pipelines, which need to be replaced with procedural SQL loops and dynamic SQL.

## 4. Data Flow & Lineage
The script operates on input text streams (either from STDIN or files) and produces transformed text to STDOUT.
-   **Input Sources**:
    -   `parser_fillist`, `parser_filattrib`, `parser_getnode`: STDIN (text stream).
    -   `parser_filmeta`, `parser_filfile`: File paths containing template/definition content.
    -   `parser`: STDIN (template stream) and recursively included files.
-   **Internal Flow**:
    -   The `parser` function is the core orchestrator, dynamically calling other parsing functions and managing state (variables, lists, include queue).
    -   It iteratively scans lines for directives (`<TAG>`, `<VAR = VALUE>`, `<INCLUDE FILE>`, `<LISTDEF>`), resolves them, and substitutes values.
    -   The functions `parser_fillist`, `parser_filattrib`, `parser_getnode` are modular utilities for specific parsing tasks.
    -   `parser_filmeta` and `parser_filfile` read external files, normalize their content, and then construct and `eval` shell pipelines of `parser_filattrib` and `parser_fillist` calls.
-   **Output Targets**:
    -   All functions primarily write transformed text to STDOUT.
    -   In BigQuery, this output will be stored in designated output tables (e.g., `project.dataset.parser_output`) or returned as the result of a stored procedure.
-   **Execution Order**: The script's execution is sequential, driven by the calls to the various `parser_` functions and the iterative/recursive logic within `parser()`. The `eval` commands in `parser_filmeta` and `parser_filfile` represent dynamic sub-executions.

## 5. Transformation Logic
The core transformation logic involves replacing template placeholders.

-   **`parser_fillist`**:
    -   **Legacy**: Uses `sed` with complex regex and conditional logic based on list length to replace `<FIRST>`, `<MIDDLE>`, `<END>`, and `<SINGLE>` placeholders.
    -   **Target (BQ SQL)**: Replicated by a `CREATE TEMP FUNCTION` (`expand_list_template`) that takes input text, list name, and an array of list values. It will use `REGEXP_REPLACE` and conditional logic (e.g., `CASE` statements based on `ARRAY_LENGTH`) to achieve the same element substitutions. `STRING_AGG` and `UNNEST` will be used for array processing.
-   **`parser_filattrib`**:
    -   **Legacy**: Uses `nawk` with `gsub` for global string replacement.
    -   **Target (BQ SQL)**: Replicated by a `CREATE TEMP FUNCTION` (`replace_placeholder`) using `REGEXP_REPLACE` to substitute the placeholder with the given value.
-   **`parser_getnode`**:
    -   **Legacy**: Uses `nawk` to extract text between `<node_name>` and `</node_name>` tags, handling `INCLUDE` directives via `system("cat ...")`.
    -   **Target (BQ SQL)**: Replicated by a `CREATE TEMP FUNCTION` (`extract_node`) that splits the input text into lines, identifies start/end tags using `REGEXP_CONTAINS`, and `STRING_AGG`s the relevant lines. Include handling will need to fetch content from the `include_source` staging table.
-   **`parser_filmeta` and `parser_filfile`**:
    -   **Legacy**: Read files, preprocess text (strip comments, normalize whitespace), dynamically build shell command pipelines (chains of `parser_filattrib` and `parser_fillist`), and execute them using `eval`.
    -   **Target (BQ SQL)**: This is a key transformation challenge. The `eval` mechanism must be replaced with BigQuery scripting's procedural capabilities. This will involve:
        -   Reading file content from staging tables (`template_files`).
        -   Parsing lines to identify assignments and list definitions.
        -   Using `EXECUTE IMMEDIATE` for dynamic SQL construction within a loop or procedural block to apply the replacements, mimicking the pipeline.
        -   For list definitions, variables representing list values will be populated and passed to the `expand_list_template` function.
-   **`parser` (Main Logic)**:
    -   **Legacy**: A robust `nawk` script with iterative and recursive logic to scan for various directives, manage `varlist`, `listdefmode`, handle `INCLUDE`, `LISTDEF`, `TIMESTAMP`, and range-based list access.
    -   **Target (BQ SQL)**: The main `parser_main` stored procedure will implement an iterative loop (`WHILE repeat_flag DO ... END WHILE`) to process directives.
        -   Variable storage (`varlist`) will be managed using `CREATE TEMP TABLE parser_state` or as procedural variables within the stored procedure.
        -   `REGEXP_EXTRACT_ALL` will be used to identify directives.
        -   Timestamp directives will use `CURRENT_TIMESTAMP()` and `FORMAT_TIMESTAMP()`.
        -   Include directives will trigger lookups in the `include_source` table and append content.
        -   List range parsing (e.g., `[ms]?[n0-9][0-9]*-[n0-9][0-9]*`) will require custom SQL parsing logic or a helper UDF to extract indices and apply them to list arrays.

## 6. External Dependencies
The script has the following external dependencies:

-   **File System Access**:
    -   **Legacy**: Reads template files and include files directly from the filesystem (`cat $1`, `getline < file`).
    -   **Target (BigQuery)**: File content will be pre-ingested into BigQuery tables (e.g., `project.dataset.template_files`, `project.dataset.include_files`) via Cloud Storage or other ETL processes. All file reads will become `SELECT` queries against these tables.
-   **`date` command**:
    -   **Legacy**: Used for `SYSDATE` in `parser()` to generate `YYYYMMDDHHMMSS` timestamps (`"date +%Y%m%d%H%M%S"|getline SYSDATE`).
    -   **Target (BigQuery)**: Replaced with BigQuery's `CURRENT_TIMESTAMP()` and `FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP())`.
-   **Shell Utilities (`sed`, `nawk`, `perl`, `grep`, `wc`, `cat`, `printf`)**:
    -   **Legacy**: Core components of the parsing logic.
    -   **Target (BigQuery)**: Replaced by BigQuery's rich set of SQL string functions (`REGEXP_REPLACE`, `REGEXP_CONTAINS`, `SPLIT`, `STRING_AGG`, `SUBSTR`), array functions, and procedural logic within stored procedures. Regular expressions will be extensively used.

## 7. Unresolved / Risks
-   **Dynamic `eval` statements**: The `eval` command is a significant challenge as it executes dynamically generated shell code. This must be meticulously re-engineered in BigQuery Scripting using `EXECUTE IMMEDIATE` for dynamic SQL or through a more structured, interpreted approach that builds and executes SQL fragments. This is a high-risk area for semantic discrepancies.
-   **Complex `sed` and `nawk` logic**: Some of the `sed` and `nawk` patterns are highly condensed and complex. Translating these precisely to BigQuery `REGEXP_REPLACE` or `REGEXP_EXTRACT` will require careful testing to ensure identical behavior, especially for edge cases.
-   **Recursive File Inclusion**: The `parser()` function handles recursive file includes. In BigQuery, this will require careful design to ensure included content is correctly identified, fetched from a staging table, and inserted into the working text, potentially using iterative loops or recursive CTEs if the depth of inclusion is known and limited.
-   **Error Handling**: The shell script has specific error behaviors (e.g., `exit 1` for missing nodes, printing errors for missing lists). BigQuery stored procedures should replicate these error handling mechanisms using `RAISE ERROR` or by logging messages to a dedicated logging table.
-   **Performance**: Iterative string manipulation on potentially large templates within BigQuery scripting could be less performant than highly optimized `sed`/`nawk`/`perl` in a shell environment. Optimization strategies (e.g., using `ARRAY<STRUCT>` for state, efficient regex) will be crucial.
-   **Timestamp Arithmetic**: The script includes a snippet `match(cmd,/[0-9][0-9][0-9][0-9][0-9]* *[-+] *[0-9]*[ymdwhis][0-9]*[t]?.*/))` for timestamp manipulation. This pattern needs careful translation to BigQuery's date/timestamp functions (`DATE_ADD`, `DATE_SUB`, `TIMESTAMP_ADD`, `TIMESTAMP_SUB`).

## 8. Build Plan
The migration will involve building the following components in BigQuery:

1.  **Staging Tables for Files**:
    -   `project.dataset.template_files` (stores content of original template files)
    -   `project.dataset.include_files` (stores content of recursively included files)
    -   `project.dataset.parser_output` (stores the final generated output)
    -   **Language**: DDL (BigQuery SQL)
2.  **Helper `TEMP FUNCTIONS` (or UDFs)**:
    -   `replace_placeholder(input_text STRING, placeholder STRING, replacement STRING)`: BigQuery SQL
    -   `expand_list_template(input_text STRING, listname STRING, list_values ARRAY<STRING>)`: BigQuery SQL
    -   `extract_node(input_text STRING, node_name STRING)`: BigQuery SQL
    -   Additional helper functions for specific regex patterns or complex date arithmetic.
    -   **Language**: BigQuery SQL (or JavaScript for UDFs if needed)
3.  **Main Stored Procedure**:
    -   `project.dataset.parser_main(IN input_file STRING, IN input_node STRING, IN output_mode STRING)`: This procedure will encapsulate the core `parser()` logic and orchestrate calls to the helper functions. It will manage the processing loop, variable states, and dynamic execution.
    -   **Language**: BigQuery Scripting / BigQuery SQL
4.  **Data Ingestion Jobs**:
    -   ETL processes (e.g., Dataflow, Cloud Functions, or `bq load` commands) to ingest the original `.ksh` template files and any other referenced files into the `template_files` and `include_files` BigQuery staging tables.
    -   **Language**: Python (for Dataflow/Cloud Functions) or shell scripts calling `bq load`.
5.  **Orchestration**:
    -   An Airflow DAG (or Cloud Composer workflow) to sequence the ingestion of files, execution of the `parser_main` stored procedure, and any subsequent steps utilizing the generated output.
    -   **Language**: Python (for Airflow DAG).

This plan systematically breaks down the complex shell script into manageable BigQuery components, addressing the unique challenges posed by dynamic execution and file system interactions.