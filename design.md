# Migration Design — vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

## 1. Purpose & Scope
This document outlines the migration design for the `h_alis_parser.ksh` KornShell script.
The script serves as a set of sophisticated text parsing and templating routines, designed to replace placeholders and process code-generator-style patterns within text streams. It includes functions for list expansion, attribute replacement, node extraction (XML-like blocks), and recursive template processing. Its primary purpose is to generate customized output based on input templates and provided data or configurations.

The `h_alis_parser.ksh` file is classified with a "retire" migration bucket, indicating that a direct lift-and-shift migration is not recommended or feasible. Instead, a re-engineering or replacement of its functionality within the target BigQuery ecosystem is required.

## 2. Source Inventory
The migration job consists of a single source file:
- **File Name**: `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh`
- **Technology**: KornShell Script (`.ksh`)
- **Category**: shell
- **Tool**: KornShell
- **Complexity Tier**: complex
- **Migration Flags**: ["loc-escalated"]
- **Automation Bucket**: retire
- **File Purpose**: (not explicitly defined, but inferred as a utility/library script for parsing/templating)

## 3. Target Architecture
Given that `h_alis_parser.ksh` is a complex shell script heavily reliant on procedural logic, dynamic command generation (`eval`), and standard Unix utilities (`sed`, `nawk`, `perl`, `grep`), a direct conversion to BigQuery SQL is not appropriate. The "retire" migration bucket further reinforces this.

The recommended target architecture involves re-implementing the script's core templating and parsing functionality using a modern, BigQuery-compatible programming language, such as **Python**. This Python solution would then be deployed as a **Google Cloud Function** or a **Dataflow job**, depending on invocation patterns and scalability requirements.

Key components of the target architecture:
- **Python Application**: Replicates the parsing and templating logic.
- **Templating Engine**: A Python library like Jinja2 or Mako could be used for the placeholder replacement, potentially abstracting some of the custom logic.
- **Input/Output**: The Python application would accept input text (templates) and data (lists, attributes) via function parameters or API calls, and return the processed text.
- **Deployment**: Google Cloud Functions for on-demand, event-driven execution, or Dataflow for batch processing of larger template sets.

## 4. Data Flow & Lineage
The `h_alis_parser.ksh` script does not exhibit traditional ETL data flow (READS/WRITES) from external systems or databases within the lineage graph for this specific assembled job. Its role is that of a utility that transforms text. Therefore, the "data flow" is primarily textual:
- **Input**: Raw template text (potentially from files or stdin) and dynamic list/attribute data.
- **Transformation**: In-memory string manipulation, pattern matching, and replacement using `sed`, `nawk`, `perl`.
- **Output**: Transformed text (to stdout).

In the target BigQuery environment, this would translate to:
- **Input**: A Python function or service would receive the template string and replacement values.
- **Processing**: The Python code executes the parsing/templating logic.
- **Output**: The transformed string is returned to the calling application or written to a designated BigQuery table/storage as needed.

No direct database tables are read from or written to by this specific script based on the lineage analysis. Its interactions are confined to text processing.

## 5. Transformation Logic
The transformation logic of `h_alis_parser.ksh` is encapsulated in several KornShell functions that utilize `sed`, `nawk`, `perl`, and other shell commands to manipulate text based on custom-defined patterns.

### Core Functions and their proposed re-implementation in Python:

1.  **`parser_fillist` (List Placeholder Replacement)**:
    *   **Legacy Logic**: Replaces `<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>` placeholders within `<LIST name>...</LIST>` blocks with elements from a provided list. Uses complex `sed` scripting, including dynamically generated `sed` commands for multi-element lists.
    *   **Target Logic**: Re-implement using Python string manipulation and a templating engine's loop/conditional constructs. A custom filter or function might be needed to handle the `<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>` semantics within the templating engine.

2.  **`parser_filattrib` (Single Attribute Replacement)**:
    *   **Legacy Logic**: Replaces `<placeholder>` with a given value using `nawk`'s `gsub` function.
    *   **Target Logic**: Direct string replacement using Python's `str.replace()` or the templating engine's variable substitution feature.

3.  **`parser_getnode` (XML-like Node Extraction)**:
    *   **Legacy Logic**: Extracts content between `<BEREICH>` and `</BEREICH>` tags. Handles `<BEREICH INCLUDE file>` by executing `cat` to pull in external content. Uses `nawk` with state tracking.
    *   **Target Logic**: Python code would use regular expressions or an XML/HTML parsing library (if the structure is strictly XML/HTML) to identify and extract nodes. File inclusion would be handled by reading the specified file content programmatically.

4.  **`parser_filmeta` / `parser_filfile` (Meta/File Block Processing)**:
    *   **Legacy Logic**: These functions read a file (or a specific node within a file), normalize its content (remove comments, whitespace), and then dynamically generate a shell pipeline using `eval`. This pipeline calls `parser_fillist` and `parser_filattrib` repeatedly to expand all lists and attributes defined within the file/node.
    *   **Target Logic**: This is the most complex part to re-engineer. The dynamic `eval` of shell commands must be replaced. A Python solution would involve:
        *   Reading the file/node content.
        *   Parsing variable definitions and list definitions from this content.
        *   Storing these definitions in Python dictionaries/lists.
        *   Then, applying these definitions to the template using the chosen templating engine.
        *   The logic to determine whether a line implies a list or an attribute (based on `<LIST ...>` syntax) needs to be mirrored in Python.

5.  **`parser` (General Code-Generator Pattern Parser)**:
    *   **Legacy Logic**: The main parsing engine, implemented in a complex `nawk` script. It handles variable assignments, variable substitutions, `LISTDEF` blocks (defining lists and their attributes), list range expansions, `INCLUDE` directives for external files, and `TIMESTAMP` generation. It manages an internal state for `listdefmode` and `repeat` processing for list expansions.
    *   **Target Logic**: This function essentially orchestrates the entire templating process. A Python function would:
        *   Iterate through the input template line by line.
        *   Use regular expressions to identify custom tags (`<VAR=VALUE>`, `<VAR>`, `<INCLUDE FILE>`, `<LISTDEF>`, `<LIST NAME M/S START-END>`, `<TIMESTAMP>`).
        *   Maintain a symbol table (Python dictionary) for variables and list definitions.
        *   Implement the `LISTDEF` parsing and storage.
        *   Implement the list range expansion logic, dynamically substituting elements.
        *   Handle file inclusions by reading external files and processing their content recursively or iteratively.
        *   Replace `TIMESTAMP` expressions with Python's `datetime` module functionalities.

**Key Transformation Principles:**
- Replace shell command-line tools (`sed`, `nawk`, `perl`, `grep`) with equivalent Python string manipulation methods, regular expressions (`re` module), and potentially a dedicated templating library (e.g., Jinja2).
- Eliminate `eval` for security and maintainability, replacing dynamic script generation with programmatic logic.
- Structure the Python code into functions that mirror the original `parser_*` functions for modularity.

## 6. External Dependencies
The original script does not have explicit external system dependencies (databases, APIs, SFTP, etc.) listed in the `lineage_assembled_jobs` record. All its "external" interactions are internal to the shell environment or filesystem.

- **Operating System Commands**: The script heavily relies on standard Unix utilities: `sed`, `nawk`, `perl`, `grep`, `wc`, `cat`, `date`, `printf`.
    - **Replacement Strategy**: These will be replaced by equivalent functionality within the chosen Python environment.
        - `sed`, `nawk`, `grep`: Python's `re` module (regular expressions) and string methods.
        - `perl`: Python's `re` module, string methods.
        - `wc`: Python's `len(string.splitlines())`.
        - `cat`: Python's file I/O operations (`open().read()`).
        - `date`: Python's `datetime` module.
        - `printf`: Python's f-strings or `str.format()`.

- **Filesystem Access**: The script includes external files using `INCLUDE` directives.
    - **Replacement Strategy**: In Python, this would translate to reading files from Google Cloud Storage (GCS) or other accessible storage, or having the included content passed as a parameter if the files are small and known at runtime.

## 7. Unresolved / Risks
- **Complexity of Dynamic Code Generation (`eval`)**: The original script extensively uses `eval` to execute dynamically constructed shell pipelines. Replicating this dynamic behavior in a safe and maintainable Pythonic way, especially for a templating engine, is challenging. It requires a robust parsing layer to understand the intent of the generated commands.
- **Custom Templating Language**: The custom placeholder syntax (`<LIST ...>`, `<FIRST>`, `<MIDDLE>`, `<END>`, `<LISTDEF>`, etc.) is highly specific. While a general templating engine can handle some, specific logic will need to be written to fully emulate the original behavior, particularly the nuanced list expansion in `parser_fillist` and the stateful processing in `parser`.
- **"loc-escalated" Migration Flag**: This flag, combined with the "complex" tier, indicates that the sheer lines of code and the intricate logic make this a non-trivial re-engineering effort.
- **Lack of Structured Error Handling**: The original script has minimal structured error handling. The re-engineered solution should include comprehensive logging, error reporting, and validation.
- **Performance Characteristics**: The `nawk` and `sed` operations in the shell script are highly optimized for text processing. The Python re-implementation needs to be carefully designed for performance, especially if processing very large templates or data sets.

## 8. Build Plan

**Target Language**: Python

**Phase 1: Analysis and Design Refinement**
1.  **Detailed Logic Mapping**: Create a precise mapping of each `sed`, `nawk`, `perl` command block and control flow within the KornShell script to equivalent Python logic.
2.  **Templating Engine Selection**: Evaluate Python templating engines (e.g., Jinja2) for their suitability to handle the custom placeholder syntax and dynamic features.
3.  **Data Structure Design**: Define Python classes/data structures to represent variables, lists, and their attributes, mirroring the internal state management of the original `nawk` script.

**Phase 2: Core Parsing Logic Implementation**
1.  **`parser_filattrib` Equivalent**: Implement a Python function for simple attribute replacement.
2.  **`parser_getnode` Equivalent**: Implement a Python function to extract content within specified tags, including handling of "INCLUDE" directives.
3.  **`parser_fillist` Equivalent**: Implement a Python function to handle list-based placeholder replacement, carefully replicating the single vs. multi-element list logic and `<FIRST>`, `<MIDDLE>`, `<END>`, `<SINGLE>` semantics. This might involve custom Jinja2 extensions or functions.
4.  **`parser` (Main Parser) Equivalent**: Implement the core Python function that orchestrates the overall parsing:
    *   Reading input templates.
    *   Identifying variable assignments, `LISTDEF` blocks, `INCLUDE` directives, and `TIMESTAMP` calls.
    *   Managing the symbol table (variables, lists).
    *   Invoking the `parser_fillist` and `parser_filattrib` equivalents as needed.
    *   Implementing the `TIMESTAMP` generation using `datetime`.

**Phase 3: Integration and Testing**
1.  **Unit Tests**: Develop comprehensive unit tests for each Python function, covering all identified edge cases and logic branches, especially for complex list expansions and error scenarios.
2.  **Integration Tests**: Test the full parsing pipeline with representative legacy templates to ensure the output matches the original script's behavior.
3.  **Deployment Packaging**: Package the Python application for deployment (e.g., as a Cloud Function or Dataflow job).

**Phase 4: Deployment and Monitoring**
1.  **Deployment**: Deploy the Python application to the chosen Google Cloud environment.
2.  **Monitoring & Alerting**: Implement Cloud Monitoring and Logging for the new service.
3.  **Documentation**: Update documentation for the new Python-based parsing utility.

**Estimated Effort**: This is a significant re-engineering effort due to the script's complexity and "retire" status. It would require an experienced Python developer with strong understanding of text parsing, regular expressions, and potentially templating engines. The "loc-escalated" flag indicates potential for higher effort than a typical "complex" script due to code volume or intricate logic within lines.