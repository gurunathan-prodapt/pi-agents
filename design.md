# Migration Design — vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

## 1. Purpose & Scope
The KornShell script `h_alis_parser.ksh` serves as a generic pattern and template parsing utility. Its primary function is to replace placeholders in text templates using various mechanisms: scalar attributes, named lists, list ranges, nested list definitions, file includes, and timestamp substitutions. It contains several functions (`parser_fillist`, `parser_filattrib`, `parser_getnode`, `parser_filmeta`, `parser_filfile`, `parser`) that collectively enable it to process template files and meta blocks, ultimately emitting transformed text. This script is intended for use in code generation and dynamic content creation within the legacy environment.
The migration bucket for this script is `retire`, suggesting that its functionality might be absorbed into other components or replaced by native BigQuery mechanisms or external orchestration rather than a direct, like-for-like migration.

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File Name**: `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`
*   **Technology**: KornShell (uses `sed`, `nawk`, `perl`, `grep`)
*   **Complexity Tier**: Complex
*   **Migration Flags**: `loc-escalated`
*   **Automation Bucket**: `retire`
*   **File Purpose**: Utility script for parsing and code generation.

## 3. Target Architecture
Given the `retire` migration bucket, a direct translation to BigQuery SQL is not necessarily the goal. Instead, the functionalities provided by this shell script should be re-evaluated for their necessity in the BigQuery environment.

*   **Template Storage**: Any templates previously processed by this script should be stored in BigQuery tables or external configuration files in Google Cloud Storage.
*   **Parsing Logic**:
    *   Simple placeholder replacements and list expansions could be handled by BigQuery SQL UDFs (User-Defined Functions) or Stored Procedures utilizing string manipulation functions (e.g., `REGEXP_REPLACE`, `SPLIT`, `ARRAY_AGG`).
    *   More complex logic involving recursive includes, dynamic pipeline generation, and system calls would likely require external orchestration using tools like Cloud Composer (Apache Airflow) running Python-based processors.
    *   The timestamp manipulation could be replaced by BigQuery's `FORMAT_TIMESTAMP` and `TIMESTAMP_ADD`/`TIMESTAMP_SUB` functions.
*   **Data Flow**: The script itself is a processing utility. Its "inputs" are text templates and "outputs" are processed text. In BigQuery, this would translate to:
    *   **Input**: Template definitions (stored in BQ tables), list elements (from BQ tables or parameters), attribute values (from BQ tables or parameters).
    *   **Processing**: BigQuery Stored Procedures, UDFs, or Python scripts in Cloud Functions/Cloud Run orchestrated by Cloud Composer.
    *   **Output**: Generated SQL queries, configuration files, or data that can be used by downstream BigQuery processes.

## 4. Data Flow & Lineage
The `h_alis_parser.ksh` script is a utility and does not appear as a direct source or target of specific tables in the provided lineage edges. It is likely invoked by other scripts or processes to generate dynamic content. The lineage data shows various SQL files reading and writing to tables, and UC4 XML files invoking other jobs and scripts. The current script's role is to generate or transform parts of these other scripts or their inputs.

*   **Execution Flow**: The script's functions (`parser_fillist`, `parser_filattrib`, `parser_getnode`, `parser_filmeta`, `parser_filfile`, `parser`) are internal and invoked sequentially or conditionally based on the logic within the script.
*   **Dynamic Content Generation**: This script acts as a pre-processor for other processes. Its output (transformed text) becomes input for other shell commands or scripts.

## 5. Transformation Logic
The core transformation logic involves string manipulation and pattern matching.

*   **parser_fillist**: Replaces list placeholders (`<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>`) within a text block based on provided list elements.
    *   **Target Logic**: This can be replicated using BigQuery SQL with `REGEXP_REPLACE`, `SPLIT`, `ARRAY_AGG`, and conditional logic within a stored procedure or Python UDF. The handling of single vs. multiple list elements and their positions would be translated into explicit logic.
*   **parser_filattrib**: Replaces a specific placeholder with a given value.
    *   **Target Logic**: Directly translatable with BigQuery's `REGEXP_REPLACE` or simple `REPLACE` functions.
*   **parser_getnode**: Extracts content within specified `<NODE> ... </NODE>` tags, supporting `<NODE INCLUDE file>` directives.
    *   **Target Logic**: Content extraction can be done with `REGEXP_EXTRACT`. File inclusion logic would need a lookup mechanism (e.g., a BigQuery table mapping node names to content) and iterative processing in a stored procedure or external Python script.
*   **parser_filmeta / parser_filfile**: These functions read definition files, parse them, and construct a shell pipeline using `parser_fillist` and `parser_filattrib` calls, which are then `eval`uated. This creates dynamic script execution.
    *   **Target Logic**: This dynamic `eval`uation is a major gap. In BigQuery, this would require a programmatic approach.
        *   The template definitions would be stored in BigQuery tables.
        *   A BigQuery Stored Procedure or an external Python script would read these definitions, interpret the "commands" (LIST, attributes), and programmatically generate the desired output, rather than constructing and evaluating a shell string.
        *   The use of `perl` for `s/\t/ /` and `sed` for comment removal and trimming would be replaced by BigQuery's string functions.
*   **parser**: The main parsing engine, managing variables, list definitions, includes, and timestamp substitutions.
    *   **Target Logic**: This complex `nawk`-based logic would be best re-implemented in a BigQuery Stored Procedure using variables, `SPLIT`, `ARRAY`, `REGEXP_EXTRACT`, and control flow (`LOOP`, `IF/ELSE`). Recursive file inclusion would require careful state management. Timestamp logic (`SYSDATE`, `5d`, `2h`) would map to `CURRENT_TIMESTAMP()`, `TIMESTAMP_ADD`, `TIMESTAMP_SUB` in BigQuery.

## 6. External Dependencies
The script itself does not have external system dependencies beyond the local filesystem and standard shell utilities (`sed`, `nawk`, `perl`, `grep`, `date`).

*   **Legacy**:
    *   **Local Filesystem**: Used for reading template files, meta blocks, and included files.
    *   **Shell Utilities**: `sed`, `nawk`, `perl`, `grep`, `wc`, `cat`, `date`, `printf`.
*   **Target (BigQuery)**:
    *   **Filesystem**: Not applicable. Source files (templates, definitions) would be migrated to BigQuery tables or Google Cloud Storage.
    *   **Shell Utilities**: These functionalities would be replaced by native BigQuery SQL string functions, UDFs (JavaScript UDFs for more complex regex if needed), or re-implemented in Python code orchestrated by Cloud Composer.

## 7. Unresolved / Risks
*   **Dynamic `eval`uation**: The `eval` command is a significant challenge as it represents arbitrary code execution. This cannot be directly replicated in BigQuery SQL. It must be re-architected to use explicit programmatic logic (e.g., Python in Cloud Functions/Composer) that interprets the "commands" rather than executing dynamically generated shell code.
*   **Recursive File Inclusion**: The script's ability to recursively include files (and the `filelist` stack management in `parser`) would need careful re-implementation, potentially using a lookup table for included content and an iterative process.
*   **Complex `nawk` logic**: The `nawk` blocks, especially within the main `parser` function, are highly procedural and stateful. While BigQuery Stored Procedures can handle loops and variables, directly translating this `nawk` logic might be complex and require a different approach for optimal performance in BigQuery (e.g., array-based processing).
*   **Performance**: Shell scripts using `sed`, `awk`, `perl` for text processing are often efficient for line-by-line operations. Migrating this to BigQuery SQL string functions on large datasets needs to be performance-evaluated.
*   **Migration Bucket `retire`**: The fact that this job is slated for retirement indicates a strong possibility that its functionality is either obsolete, can be simplified, or can be absorbed into other existing or newly designed components. A direct translation might not be cost-effective or necessary.

## 8. Build Plan
Given the `retire` status, the build plan focuses on replacing or absorbing the functionality rather than a direct port.

1.  **Assess Current Usage**:
    *   Identify all upstream processes that invoke `h_alis_parser.ksh`.
    *   Analyze the specific inputs (templates, list data, attributes) and outputs (generated text) for each invocation.
    *   Determine if the generated content is still required in the target BigQuery environment, or if the business logic it supports can be simplified/eliminated.

2.  **Migrate Templates and Definitions (BQ Tables)**:
    *   Extract all static template files and meta-block definitions.
    *   Create BigQuery tables to store these templates, potentially with columns for template name, content, placeholder definitions, etc.
    *   Create a BigQuery table to store list data and attribute values that were previously passed to `parser_fillist` and `parser_filattrib`.

3.  **Develop BigQuery SQL Stored Procedures/UDFs (BQSQL/JavaScript)**:
    *   **`parser_filattrib` Equivalent**: Create a BigQuery SQL UDF or Stored Procedure for simple placeholder replacement.
    *   **`parser_fillist` Equivalent**: Create a BigQuery SQL Stored Procedure to handle list expansion logic. This would involve iterating through list elements and replacing placeholders, potentially using `ARRAY_AGG` and `STRING_AGG` for reconstruction.
    *   **`parser_getnode` Equivalent**: Create a BigQuery SQL Stored Procedure to extract text between specified tags and perform lookups for included content from the migrated template tables.
    *   **Timestamp Logic**: Implement timestamp transformations using native BigQuery functions.

4.  **Re-implement Complex Parsing/Inclusion Logic (Python/Cloud Composer)**:
    *   For the complex, dynamic `eval`-based logic in `parser_filmeta`, `parser_filfile`, and the main `parser` function, a Python script orchestrated by Cloud Composer would be necessary.
    *   This Python script would:
        *   Read templates and definitions from BigQuery tables.
        *   Implement the variable management, list definition, and inclusion logic using Python's string manipulation and data structures.
        *   Perform the parsing and content generation programmatically, avoiding dynamic shell execution.
        *   Handle the recursive include resolution by iteratively fetching content from BigQuery tables.

5.  **Integrate with Downstream Processes**:
    *   Modify any processes that previously consumed the output of `h_alis_parser.ksh` to instead consume the output from the new BigQuery Stored Procedures, UDFs, or Cloud Composer-driven Python scripts.

**Build Language**: Primarily BigQuery SQL for simpler transformations and Python for complex parsing, orchestration, and handling of dynamic logic.