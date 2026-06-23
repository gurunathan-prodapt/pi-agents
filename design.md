# Migration Design — vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

## 1. Purpose & Scope

This migration design document addresses the KornShell script located at `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`. The script provides a set of text-pattern parsing and template-expansion routines, primarily used for code-generation-style inputs. Its core functionality includes placeholder replacement, list expansion, node extraction from XML-like structures, meta/file-driven substitutions, handling of included files, variable assignment, list definitions, and timestamp substitution. The script acts as a utility library for other processes that require dynamic text manipulation.

The scope of this migration is to translate the functionality of this KornShell script to BigQuery, leveraging BigQuery's capabilities for data transformation and procedural logic, or identifying external components where direct BigQuery translation is not feasible or optimal.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`
*   **Technology:** KornShell script, heavily utilizing `sed`, `nawk`, `perl`, `grep` for text processing, and `eval` for dynamic command execution.
*   **Category:** `shell`
*   **Detected Tool:** `KornShell`
*   **File Purpose:** Utility / Shared Library (implied, as it defines functions)
*   **Complexity Signals:** (None detected in `file_analysis`)
*   **Complexity Tier:** Unknown (No entry found in `file_complexity` table for this file.)
*   **Migration Bucket:** `retire` (This indicates that the file is currently marked for retirement, which is a key consideration for the migration strategy. This migration design assumes a decision to proceed despite the `retire` bucket status.)

## 3. Target Architecture

The target architecture will primarily consist of BigQuery SQL stored procedures and scripting for the core text processing logic. Due to the dynamic nature of file inclusions and command evaluation in the original script, certain components will require external orchestration or pre-processing outside of BigQuery.

*   **BigQuery Stored Procedures:** Core functions (`parser_fillist`, `parser_filattrib`, `parser_getnode`) will be translated into BigQuery SQL stored procedures. The main `parser` orchestrator will also be a BigQuery stored procedure, managing state and calling sub-procedures.
*   **BigQuery Scripting:** Used within the main `parser` stored procedure for control flow, loops, and variable management.
*   **BigQuery Staging Tables:** To hold input templates, definition files, and list data (converted from newline-separated to BigQuery ARRAYs).
*   **Cloud Storage:** For storing raw input files, especially those referenced dynamically via `INCLUDE` statements.
*   **External Pre-processing/Orchestration (e.g., Cloud Functions, Dataflow):** A potential component for resolving dynamic file inclusions and for transforming highly dynamic `eval`-based logic into a structured BigQuery-consumable format if direct SQL translation proves overly complex or inefficient.

## 4. Data Flow & Lineage

The original script functions as a text processing utility. Its data flow involves:

*   **Sources:**
    *   Standard Input (STDIN): The primary mechanism for receiving template text.
    *   Files: Referenced directly by path (`$1` in `parser_filmeta`, `parser_filfile`) or via `INCLUDE` tags within template text.
    *   Command-line arguments: For list names, placeholders, and values.
*   **Transformations:** Each `parser_` function performs specific text manipulations, substitutions, and template expansions as described in the original shell script comments and by the CM MCP tool analysis. The `parser()` function orchestrates these transformations iteratively.
*   **Targets:** Standard Output (STDOUT): The transformed text is printed to STDOUT.

**Migrated Data Flow:**

1.  **Input Ingestion:**
    *   Initial template text will be provided as a string parameter to the main `parser_bq` stored procedure or loaded from a BigQuery staging table.
    *   Files referenced via `INCLUDE` tags or by `parser_filmeta`/`parser_filfile` will need to be ingested into BigQuery staging tables (e.g., as a table with `filename` and `content` columns) or pre-processed externally to resolve inclusions before being passed to BigQuery.
2.  **Transformation Pipeline:**
    *   The `parser_bq` stored procedure will read the input template line by line (or operate on the entire string).
    *   It will manage script variables (equivalent to shell variables) and use temporary tables or arrays to store list definitions (`varlist`).
    *   It will invoke other BigQuery stored procedures (`parser_fillist_bq`, `parser_filattrib_bq`, `parser_getnode_bq`) to perform specific text transformations.
    *   Dynamic execution logic (from `eval` in the original) will be re-engineered into BigQuery control flow (IF/THEN/ELSE, loops) calling the appropriate BQ procedures.
3.  **Output:** The final transformed text will be returned by the `parser_bq` stored procedure as a string or inserted into a BigQuery output table for downstream consumption.

There are no explicit `lineage_edges` records for this file within the provided `run_id`, indicating it likely functions as a standalone utility or is called in a way not captured by the static lineage analysis for this specific run. Its integration points with other systems would be through its STDIN/STDOUT interface, which would need to be re-established in the BigQuery environment (e.g., via Cloud Composer, Cloud Functions orchestrating BigQuery jobs, or direct BigQuery API calls).

## 5. Transformation Logic

The core logic of the shell script revolves around advanced text parsing and manipulation using standard Unix utilities. This will be re-implemented using BigQuery's extensive string functions, array capabilities, and procedural language features.

*   **`parser_fillist` function (Shell) → `dataset.parser_fillist_bq` (BigQuery SQL Stored Procedure):**
    *   **Original Logic:** Replaces `<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>` placeholders within a `<LIST>` block based on the number of elements in the input list. Uses `sed` with advanced regex and flow control.
    *   **BigQuery Logic:**
        *   Will accept `listname` (STRING), `list_values` (ARRAY<STRING>), and `template_text` (INOUT STRING).
        *   Determine list count using `ARRAY_LENGTH(list_values)`.
        *   Use `IF/ELSE` statements to handle single vs. multi-element lists.
        *   `REGEXP_REPLACE` will be used for all placeholder substitutions (`<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>`, `<LIST ...>`, `</LIST>`).
        *   `ARRAY_TO_STRING` will be used to concatenate middle elements with a newline delimiter if `middle_values` is an array.
*   **`parser_filattrib` function (Shell) → `dataset.parser_filattrib_bq` (BigQuery SQL Stored Procedure):**
    *   **Original Logic:** Performs global placeholder substitution (e.g., `<PLACEHOLDER>`) with a given `value` using `nawk`'s `gsub`.
    *   **BigQuery Logic:**
        *   Will accept `placeholder` (STRING), `value` (STRING), and `text_in` (INOUT STRING).
        *   Use `REPLACE` or `REGEXP_REPLACE` to substitute all occurrences of `<placeholder>` with `value`.
*   **`parser_getnode` function (Shell) → `dataset.parser_getnode_bq` (BigQuery SQL Stored Procedure):**
    *   **Original Logic:** Extracts content between XML-like tags (`<node_name> ... </node_name>`) and handles `INCLUDE` directives via `system("cat ...")`. Uses `nawk`.
    *   **BigQuery Logic:**
        *   Will accept `node_name` (STRING), `input_text` (STRING), and return `node_text` (OUT STRING).
        *   Use `SPLIT` to break `input_text` into lines.
        *   Use `REGEXP_CONTAINS` to identify the opening and closing tags.
        *   Use `STRING_AGG` within a subquery to reassemble lines between the identified tags.
        *   **File Inclusion:** The `INCLUDE` functionality (reading external files) cannot be directly replicated in BigQuery SQL. This will require pre-processing (see External Dependencies). The `parser_getnode_bq` procedure will operate on already materialized content.
        *   Error handling for `node not found` will be managed by `SIGNAL SQLSTATE '45000'`.
*   **`parser_filmeta` and `parser_filfile` functions (Shell):**
    *   **Original Logic:** These functions read definition files, extract content (potentially a node), normalize it, and dynamically construct a shell pipeline of `parser_fillist` and `parser_filattrib` calls using `eval`.
    *   **BigQuery Logic:** The dynamic `eval` approach is not transferable. Instead, these functions will be refactored:
        *   The definition files will be parsed (e.g., by loading into a staging table).
        *   The definitions (e.g., lists and attributes) will be extracted and stored in BigQuery script variables or temporary tables.
        *   The processing pipeline will then explicitly call the BigQuery stored procedures (`parser_fillist_bq`, `parser_filattrib_bq`) based on the extracted definitions, rather than dynamically generating SQL statements at runtime.
*   **`parser` function (Shell) → `dataset.parser_bq` (BigQuery SQL Stored Procedure):**
    *   **Original Logic:** The main orchestrator, managing variables, lists, includes, and iteratively applying transformations. Relies on mutable global state and file system operations.
    *   **BigQuery Logic:**
        *   Will accept `input_text` (STRING) and return `output_text` (OUT STRING).
        *   Use BigQuery script variables (`DECLARE`, `SET`) to manage `varlist` (variables), `listdefmode` (list definition state), `repeat_flag` (for iterative processing), etc.
        *   Implement the iterative processing using `WHILE` loops.
        *   Call `parser_fillist_bq`, `parser_filattrib_bq`, and other procedures as needed based on the logic extracted from the shell script.
        *   **File Inclusion:** `INCLUDE` directives will require the input `input_text` to be already resolved by an external pre-processing step, or the procedure would query a staging table of "included files" that were loaded beforehand.
        *   **Timestamp Generation:** Convert shell `date` commands and arithmetic (`date +%Y%m%d%H%M%S`, `printf "xx"cmd""`) to BigQuery's `FORMAT_TIMESTAMP`, `TIMESTAMP_ADD`, `TIMESTAMP_SUB`, and `DATETIME_ADD`/`SUB` functions.

## 6. External Dependencies

The original script has several external dependencies:

*   **Shell Utilities (`sed`, `nawk`, `perl`, `grep`, `wc`, `cat`, `eval`, `printf`, `date`):**
    *   **Replacement in BigQuery:** These will be replaced by BigQuery's native SQL functions and procedural capabilities:
        *   `sed`, `nawk`, `grep`: Primarily `REGEXP_REPLACE`, `REGEXP_EXTRACT`, `SPLIT`, `STRING_AGG`, `REPLACE`.
        *   `wc`: `ARRAY_LENGTH` (after `SPLIT`).
        *   `cat`: Data will be pre-loaded into BigQuery tables or processed externally.
        *   `eval`: Replaced by structured BigQuery scripting (IF/THEN/ELSE, `CALL` to procedures). This is a significant re-architecture.
        *   `printf`, `date`: `FORMAT_TIMESTAMP`, `TIMESTAMP_ADD`, `TIMESTAMP_SUB`.
*   **Filesystem (`INCLUDE` tags, `cat $1`):**
    *   **Replacement in BigQuery:** BigQuery SQL does not have direct filesystem access. Files referenced by `INCLUDE` or as parameters to `parser_filmeta`/`parser_filfile` must be pre-loaded into BigQuery staging tables (e.g., with `filename` and `content` columns) or resolved by an external orchestration layer (e.g., Cloud Function, Dataflow job) which reads from Cloud Storage and provides the materialized content to BigQuery.
*   **No Network APIs:** The original script does not appear to make external API calls, so no BigQuery remote functions or external system integrations are needed for this specific aspect.

## 7. Unresolved / Risks

*   **"Retire" Migration Bucket:** The file is categorized as `retire`. This indicates it may not be actively maintained or considered for migration. Proceeding with migration against this classification introduces a risk that the effort might be misdirected if the job's business value is truly low. Clarification on the `retire` status is crucial.
*   **Dynamic `eval` Logic:** The heavy reliance on `eval` in `parser_filmeta` and `parser_filfile` to construct and execute shell pipelines is the most complex aspect to migrate. A direct, literal translation to BigQuery SQL is not possible. This requires a significant redesign to interpret the definition files and procedurally invoke BigQuery stored procedures based on those definitions. This could be a source of errors or unexpected behavior if not carefully managed.
*   **Dynamic File Inclusion:** The script's ability to `INCLUDE` other files dynamically is not directly supported in BigQuery. This necessitates an external preprocessing step (e.g., a Cloud Function that reads files from Cloud Storage, resolves includes, and provides a flattened text to BigQuery) or a robust BigQuery ingestion strategy for all potential include files.
*   **State Management:** The shell script's use of mutable global variables (`varlist`, `filelist`, etc.) requires careful translation to BigQuery. While BigQuery scripting supports variables, replicating complex, nested state management could be challenging and might require temporary tables or structured data types.
*   **Missing Complexity Data:** The absence of complexity tier and migration flags from the `file_complexity` table means that the inherent complexity and specific migration challenges have not been formally assessed for this file by the automated tools. This increases the risk of underestimating effort.

## 8. Build Plan

This plan outlines the steps to build the migrated components in BigQuery.

*   **Phase 1: Core BigQuery Stored Procedures (BQ SQL)**
    *   **Task 1.1:** Create `dataset.parser_fillist_bq` stored procedure.
    *   **Task 1.2:** Create `dataset.parser_filattrib_bq` stored procedure.
    *   **Task 1.3:** Create `dataset.parser_getnode_bq` stored procedure.
*   **Phase 2: External Pre-processing and Data Ingestion (Python/Cloud Functions/Dataflow & BQ DDL)**
    *   **Task 2.1:** Design and implement the external pre-processor to resolve `INCLUDE` directives from source templates and definition files. This component will read files from Cloud Storage, flatten them, and prepare content for BigQuery.
    *   **Task 2.2:** Create BigQuery DDL for staging tables to hold pre-processed template content, definition data (for lists and attributes), and any other input necessary for the BigQuery procedures.
*   **Phase 3: Re-engineer Dynamic Logic (BQ SQL)**
    *   **Task 3.1:** Design and implement BigQuery SQL stored procedures or scripts to replace `parser_filmeta` and `parser_filfile` logic. These will read from the staging tables (populated in Phase 2) and programmatically call `parser_fillist_bq` and `parser_filattrib_bq`, managing script variables and control flow.
*   **Phase 4: Main Orchestrator (BQ SQL)**
    *   **Task 4.1:** Create `dataset.parser_bq` stored procedure. This procedure will orchestrate the entire parsing process, managing script variables, iterating through input content (from staging tables), and calling the procedures from Phase 1 and Phase 3. It will implement the iterative substitution logic and timestamp generation.
*   **Phase 5: Orchestration and Execution Layer (Cloud Composer/Workflows)**
    *   **Task 5.1:** Implement an orchestration layer (e.g., using Cloud Composer or Cloud Workflows) to manage the end-to-end job flow, including:
        *   Triggering the external pre-processor (Phase 2).
        *   Loading data into BigQuery staging tables.
        *   Executing the main `dataset.parser_bq` stored procedure.
        *   Handling output.
*   **Phase 6: Testing and Validation**
    *   **Task 6.1:** Develop comprehensive unit tests for each BigQuery stored procedure and the external pre-processor.
    *   **Task 6.2:** Develop integration tests to validate the end-to-end migrated solution against various test cases from the legacy system, ensuring identical output.

This design acknowledges the "retire" status but provides a blueprint for migration should that decision be overturned or if a specific need arises to port this functionality to BigQuery.