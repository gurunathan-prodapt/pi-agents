# Migration Design — vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

## 1. Purpose & Scope
The KornShell script `h_alis_parser.ksh` serves as a core utility for parsing and transforming text-based templates. Its primary function is to substitute various placeholders (e.g., lists, attributes, named nodes) within input content, often read from files or standard input. It acts as a sophisticated text templating and pattern-matching engine, dynamically generating or modifying text based on defined rules and external configuration. This script is classified as `complex` with `loc-escalated` migration flags and is designated for `retire` (B0) in the migration bucket, indicating that a direct 1:1 conversion may not be feasible or desirable, and its functionality might need re-evaluation or re-implementation.

## 2. Source Inventory
The job is composed of a single file:
- **File Path**: `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`
- **Technology**: KornShell (Shell Script)
- **Category**: `shell`
- **Tool**: `KornShell`
- **Summary**: This KornShell script provides a set of utility functions for parsing and transforming text by substituting placeholders (lists, attributes, nodes) within input content, often read from files or standard input. It acts as a text templating and parsing engine.
- **Tier**: `complex`
- **Migration Flags**: `["loc-escalated"]` (suggests high line-of-code complexity or specific patterns making direct migration challenging).
- **Automation Bucket**: `retire` (B0). This means the functionality of this script should be considered for retirement, replacement, or re-engineering rather than direct automated migration.

The script defines the following key functions:
- `parser_fillist`: Replaces list-based placeholders (`<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>`) within `<LIST>` blocks.
- `parser_filattrib`: Replaces single attribute placeholders (`<attribute_name>`) with a given value.
- `parser_getnode`: Extracts content within XML-like tags (`<NODE>` to `</NODE>`) and handles `<NODE INCLUDE ...>` directives.
- `parser_filmeta`: Reads a file, extracts a meta-block using `parser_getnode`, then parses lines within that block to dynamically generate and `eval`uate shell commands for list and attribute substitutions.
- `parser_filfile`: Similar to `parser_filmeta`, processes an entire file (or a specified node within it) for definitions, dynamically generating and `eval`uating substitution commands.
- `parser`: A general-purpose `nawk` script processing input line by line, handling variable assignments and substitutions, file inclusions (`<INCLUDE filename>`), complex list element expansion based on ranges, and timestamp substitution.

## 3. Target Architecture
Given the `retire` migration bucket and the script's nature as a complex text parsing and templating engine rather than a direct data transformation ETL script, a direct BigQuery SQL conversion is inappropriate. The migration should focus on re-engineering its core functionality into a modern, maintainable, and BigQuery-friendly ecosystem.

**Recommended Target Components**:
-   **Cloud Composer (Airflow)**: To orchestrate the execution of the re-engineered parsing logic and integrate it with downstream BigQuery data pipelines.
-   **Python on Cloud Functions / Cloud Run / Dataflow**: The core text parsing and templating logic should be re-implemented in Python.
    -   **Cloud Functions/Cloud Run**: Suitable for isolated, smaller parsing tasks, especially if the script's functions are called independently with small inputs.
    -   **Dataflow (Python)**: Preferred if the parsing operations involve large volumes of text data or require distributed processing capabilities (e.g., if the script processes numerous large template files).
-   **BigQuery User-Defined Functions (UDFs)**: For very simple, idempotent parsing logic that can be expressed in SQL-compatible Python or JavaScript, allowing direct integration within BigQuery queries. However, the complexity of `h_alis_parser.ksh` suggests most of its logic would reside outside direct BigQuery UDFs.
-   **Configuration/Definition Storage**: Custom definition files used by `parser_filmeta` and `parser_filfile` should be migrated to structured formats like YAML, JSON, or Avro, stored in Cloud Storage or a metadata store.

The goal is to replace the shell-based templating with a more robust, secure, and maintainable solution in Python, leveraging GCP's serverless and data processing capabilities.

## 4. Data Flow & Lineage
The `h_alis_parser.ksh` script does not directly interact with traditional data sources or targets (like databases or external systems) in a manner that would be captured as typical ETL data flow within the provided `run_id` lineage. Instead, its "data flow" is primarily text-based processing:

-   **Inputs**:
    -   Text streams (via STDIN).
    -   Files specified as arguments or through `<INCLUDE>` directives within templates. These files contain raw text, template definitions, or configuration values for substitutions.
-   **Internal Processing**: The script processes these inputs using its various `sed`, `nawk`, and `perl` routines to:
    -   Identify and replace placeholders (lists, attributes).
    -   Extract specific text blocks (nodes).
    -   Dynamically generate and execute further parsing commands (`eval` statements).
-   **Outputs**:
    -   Transformed text streams (to STDOUT).
    -   The output of this script is then presumably consumed by other parts of a larger workflow (e.g., as dynamically generated SQL, configuration files, or other scripts). The exact downstream usage is not detailed in the current lineage.

## 5. Transformation Logic
The transformation logic is highly complex, implemented through intertwined `sed`, `nawk`, and `perl` commands, often involving dynamic generation and `eval`uation of shell commands.

**High-Level Transformation Capabilities**:
-   **Templating with Lists**: The `parser_fillist` function enables sophisticated list-based templating. It can replace `<FIRST>`, `<MIDDLE>`, `<END>`, and `<SINGLE>` tags within a `<LIST>` block based on a provided list of elements. This logic is highly dependent on `sed` and conditional shell constructs.
-   **Attribute Substitution**: `parser_filattrib` provides simple key-value substitution, replacing `<KEY>` with `VALUE` using `nawk`.
-   **Node Extraction and Inclusion**: `parser_getnode` acts as a rudimentary XML-like parser, extracting content between `<NODE_NAME>` and `</NODE_NAME>` tags. It also supports file inclusion via `<NODE_NAME INCLUDE /path/to/file>`.
-   **Dynamic Configuration Processing (`parser_filmeta`, `parser_filfile`)**: These functions are central to the script's flexibility. They read configuration from files or meta-blocks, dynamically construct sequences of `parser_fillist` and `parser_filattrib` calls, and then `eval`uate these generated commands. This allows for runtime-defined placeholder substitution rules.
-   **General Pattern Parsing (`parser`)**: This is the most complex function, a large `nawk` script that processes input line by line. It manages internal state for:
    -   **Variable Definitions and Substitutions**: Supports simple `<VAR = VALUE>` assignments and subsequent `<VAR>` substitutions.
    -   **File Inclusion**: `<INCLUDE filename>` directives queue additional files for processing.
    -   **List Definitions and Expansions**: Supports `<LISTDEF name attributes>` for defining lists and `<listname [m|s]N-N>` or `<listname [m|s]N>` for expanding list elements based on numeric ranges or single-element access.
    -   **Timestamp Logic**: `<TIMESTAMP SYSDATE>` or `<TIMESTAMP YYYYMMDD +/- X[ymdwhis] >` for dynamic date/time insertion.

**Challenges in Re-implementation**:
-   **Complexity of `sed`/`nawk`**: The highly optimized, compact, and often cryptic `sed` and `nawk` one-liners are difficult to translate directly and maintain.
-   **Dynamic `eval`**: The use of `eval` in `parser_filmeta` and `parser_filfile` is a major security risk and a source of complexity. It must be replaced with structured, programmatic templating rather than arbitrary command execution.
-   **Stateful `nawk` Logic**: The `parser` function's internal state management (e.g., `listdefmode`, `varlist`, `filelist`, `repeat`) will require careful re-implementation in a procedural language like Python.
-   **Error Handling**: The existing error handling is minimal, and re-implementation offers an opportunity for robust, auditable error management.

## 6. External Dependencies
The script `h_alis_parser.ksh` primarily relies on standard Unix/Linux command-line utilities and the local filesystem.
-   **Operating System Utilities**: `sed`, `nawk` (or `awk`), `perl`, `grep`, `wc`, `cat`, `date`, `printf`.
    -   **Replacement**: These functionalities will be replaced by equivalent Python libraries (e.g., `re` for regular expressions, file I/O for `cat`, `datetime` for date operations, custom parsing logic for `awk`/`sed` patterns).
-   **Filesystem**: The script reads various configuration and template files from the local filesystem.
    -   **Replacement**: File inputs will be managed via Google Cloud Storage (GCS) buckets, allowing for scalable and accessible storage in the GCP environment.
-   **No explicit external system connections**: No direct database connections (e.g., Oracle, SQL Server), SFTP, or other external systems were identified as being directly used *by this script*. Any such interactions would be performed by scripts or jobs that consume the output of this parser.

## 7. Unresolved / Risks
-   **Retirement Rationale**: The most significant unresolved aspect is the `retire` migration bucket. A thorough review with business stakeholders is essential to confirm whether the functionality provided by `h_alis_parser.ksh` is still needed. If not, the easiest and recommended approach is to decommission the script entirely.
-   **Security Vulnerabilities**: The heavy reliance on `eval` and `system("cat $0")` opens the door to severe command injection vulnerabilities. Any re-implementation *must* prioritize secure parsing and templating over direct command execution.
-   **Untracked Dependencies**: The `lineage_edges` output did not show any direct callers or consumers of this script. Understanding its upstream invocations and downstream consumption is critical to ensure a complete migration and prevent breaking dependent processes.
-   **Performance**: The chained pipes and multiple invocations of `sed`, `nawk`, and `perl` can be inefficient for large inputs. A re-implementation in Python could offer performance improvements through optimized libraries.
-   **Maintainability and Debugging**: The current script is inherently difficult to understand, debug, and modify due to its complex shell scripting paradigms. Re-engineering into Python will significantly improve maintainability.
-   **Dynamic Nature**: The script's ability to dynamically interpret and execute logic from configuration files via `eval` is powerful but also fragile. This dynamism needs to be carefully replicated in a structured and safe manner in the target environment.

## 8. Build Plan
The build plan focuses on re-engineering the functionality of `h_alis_parser.ksh` into a Python-based solution orchestrated by Cloud Composer.

**Phase 1: Discovery & Requirements Validation (Critical for 'Retire' bucket)**
1.  **Business Justification Review**: Engage with business owners to confirm the continued necessity of the functionalities provided by `h_alis_parser.ksh`. Document all use cases and requirements. If functionality is no longer needed, retire the script.
2.  **Upstream/Downstream Analysis**: Identify all calling scripts/jobs and all consumers of the output generated by `h_alis_parser.ksh`. This is essential to ensure no dependencies are missed.
3.  **Detailed Functional Specification**: For each function (`parser_fillist`, `parser_filattrib`, `parser_getnode`, `parser_filmeta`, `parser_filfile`, `parser`), create a detailed functional specification, including all input parameters, expected outputs, and edge-case behaviors.

**Phase 2: Architectural Design & Technology Selection**
1.  **Select Core Re-implementation Language**: Python is recommended for its strong text processing libraries, BigQuery integration, and suitability for orchestration.
2.  **Choose Execution Environment**:
    -   For utility functions and general parsing, **Cloud Functions** or **Cloud Run** might be sufficient if calls are event-driven and inputs are manageable.
    -   For large-scale or batch processing, **Cloud Dataflow (Python)** should be considered.
3.  **Templating Engine**: Adopt a robust Python templating engine like **Jinja2** to replace the dynamic `eval` constructs safely.
4.  **Configuration Management**: Define a standard format (e.g., YAML/JSON) for external configuration files, to be stored in GCS.

**Phase 3: Development & Implementation**
1.  **Module Creation**: Create a Python package `alis_parser` containing modules for each of the original shell functions.
2.  **`alis_parser.fillist(list_name, elements, text)`**: Re-implement `parser_fillist` using Python's `re` module for regular expression-based substitutions.
3.  **`alis_parser.filattrib(attr_name, attr_value, text)`**: Re-implement `parser_filattrib` using Python string `replace` or `re.sub`.
4.  **`alis_parser.get_node(node_name, text_stream)`**: Re-implement `parser_getnode` using a combination of `re` or a dedicated XML/HTML parser if the structure is consistently well-formed XML. Secure file reading for `INCLUDE` directives from GCS.
5.  **`alis_parser.filmeta(config_file_path, meta_block_name)` & `alis_parser.filfile(config_file_path, node_name=None)`**: Re-implement these to read structured configuration files (e.g., YAML) from GCS, and then programmatically invoke `alis_parser.fillist` and `alis_parser.filattrib` using Python's object model, entirely eliminating `eval`.
6.  **`alis_parser.parser(input_stream)`**: Re-implement the main `parser` logic in Python, carefully managing state for variables, lists, file inclusions (from GCS), and timestamp logic. Use `re` for pattern matching and `datetime` for date operations.
7.  **Input/Output Handling**: Ensure all functions can handle input from strings, file paths (GCS URIs), or standard input, and produce output as strings or write to GCS.

**Phase 4: Testing**
1.  **Unit Tests**: Develop comprehensive unit tests for each Python function to ensure functional parity with the original script's behavior across all known use cases and edge cases.
2.  **Integration Tests**: Test the integrated Python modules within a simplified workflow.
3.  **Security Testing**: Verify that the re-implemented code is free from command injection or other vulnerabilities.

**Phase 5: Deployment & Orchestration**
1.  **Cloud Storage Integration**: Store all input template files, configuration files, and generated output in designated GCS buckets.
2.  **Cloud Composer DAGs**: Create Cloud Composer (Airflow) DAGs to define the workflow, trigger Python functions (via PythonOperators or KubernetesPodOperators for Cloud Run/Functions), manage dependencies, and handle logging/monitoring.
3.  **Monitoring & Alerting**: Implement Stackdriver (Cloud Monitoring) for logging, metrics, and alerting for the new Python components.

**Deliverables**:
-   Python `alis_parser` package (modules for each re-engineered function).
-   Structured configuration files (e.g., YAML/JSON) stored in GCS.
-   Cloud Composer DAGs (`.py` files) for orchestrating the parsing tasks.
-   Comprehensive unit and integration test suite.
-   Updated documentation for the new `alis_parser` module.