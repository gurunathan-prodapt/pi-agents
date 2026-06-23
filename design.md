# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_bpr_evn.ksh`. The script's primary purpose is to orchestrate the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. It prepares parameters such as a processing date (`Stichtag`) and an optional restart value, sets up logging and error handling, and then invokes a core kernel script (`k_ausd_bp_ta_bpr_evn.ksh`) with the resolved parameters to generate a snapshot of the DWH contract cache for demand scoring (FOS-Tabelle).

The scope of this migration is to translate the orchestration logic of this specific wrapper script from KornShell to Horizon Python, with the understanding that the actual data processing (performed by the core kernel script) will be handled separately.

## 2. Source Inventory
The job consists of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh`
*   **Technology:** KornShell Script (`.ksh`)
*   **Summary:** This KSH script orchestrates the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. It prepares parameters and calls a core script to generate a snapshot of the DWH contract cache for demand scoring (FOS-Tabelle).
*   **Complexity Tier:** Medium
*   **Automation Bucket:** Semi-automatic

## 3. Target Architecture
The migrated component will operate within the Google Cloud Platform (GCP) ecosystem.
*   **Orchestration:** The KornShell script's orchestration logic will be translated into a Python script designed to run within the Horizon Python framework. This Python script will handle parameter parsing, date determination, logging, and the invocation of the core processing logic.
*   **Data Processing:** The actual data transformation and loading, currently handled by `k_ausd_bp_ta_bpr_evn.ksh`, will be performed by a separate, migrated component, likely leveraging BigQuery SQL or Python/PySpark for BigQuery.
*   **Logging & Error Handling:** The existing custom logging and error handling mechanisms will be replaced with Python-native logging and potentially integrated with GCP logging services (e.g., Cloud Logging).

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_evn.ksh` script acts as an orchestrator, with the following conceptual data flow:

1.  **Environment Initialization:** The script sources `. $HOME/.dw_init` to set up environment variables.
2.  **Parameter Acquisition:**
    *   Command-line arguments (`-s` for `Stichtag` (processing date), `-l` for `Wiederanlaufwert` (restart value)) are parsed.
    *   If `-s` is not provided, the `Stichtag` defaults to the current system date.
    *   If `-l` is not provided, `Wiederanlaufwert` defaults to `0`.
3.  **Date Determination:** The script uses `DWDate_Gib_Zeitraum` (from a sourced helper script) to obtain the current system date.
4.  **Logging & Error Handling Setup:** Utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, and various `DWMSG_` functions) are sourced and utilized to initialize logging and set up error traps.
5.  **Core Script Invocation:** The script constructs the full path to the core kernel script (`k_ausd_bp_ta_bpr_evn.ksh`) and executes it, passing the determined parameters (`-j`, `-s`, `-f`, `-l`) along with log output redirection.
6.  **Status Reporting:** Upon successful completion of the core script, a success message is logged, and the job status is updated using `DWMSG_SetzeStatusOK`.

No direct `READS` or `WRITES` to tables or files are performed by this wrapper script itself. All data interactions are delegated to the `k_ausd_bp_ta_bpr_evn.ksh` script that it invokes.

## 5. Transformation Logic
The KornShell logic will be transformed into Python as follows:

*   **Script Structure:** The entire shell script will be converted into a Python script (`.py` file).
*   **Parameter Parsing:** The `getopts` logic for `-s`, `-l`, and `-h` will be re-implemented using Python's `argparse` module or manual argument parsing for `sys.argv`.
*   **Environment Initialization:** The sourcing of `. $HOME/.dw_init` will need to be replaced by explicit environment variable settings within the Python environment or through configuration files (e.g., `BERT_DIR_ROOT`).
*   **Utility Script Translation:**
    *   `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` (and the `DWMSG_` functions) will need to be re-implemented in Python or replaced with suitable Python libraries or GCP services. For example, date handling can use Python's `datetime` module, and custom logging can be replaced with Python's `logging` module.
    *   The `DWDate_Gib_Zeitraum` function will be replaced by standard Python date functions to retrieve and format the current date.
    *   The `pruefeParameterGesetzt` function will be replaced by Python validation logic.
*   **Core Script Invocation:** The invocation of `k_ausd_bp_ta_bpr_evn.ksh` will be replaced by calling its migrated equivalent in the target environment. If `k_ausd_bp_ta_bpr_evn.ksh` is also migrated to Python, it will be a direct function call or module import. If it's migrated to BigQuery SQL, the Python script will execute the BigQuery job.
*   **Error Handling:** Shell `trap` mechanisms will be replaced with Python `try-except` blocks to catch and handle exceptions, integrating with the new logging system.
*   **Logging:** `print` statements and redirection to `$LogDatei` will be replaced with Python's `logging` module, writing to a designated log file or standard output, potentially integrating with GCP Cloud Logging.

The pseudocode generated by the CM MCP tool provides a solid starting point for this Python implementation, outlining functions for parameter parsing, date handling, logging, and a placeholder for the core job invocation.

## 6. External Dependencies
The original script has the following external dependencies:

*   **Shell Utilities:** `ksh`, `getopts`, `tee`, `print`, `trap`, `cat`. These will be replaced by native Python constructs and libraries.
*   **Environment File:** `. $HOME/.dw_init`. This file sets critical environment variables (e.g., `BERT_DIR_ROOT`). In the target environment, these variables should be defined explicitly in the execution environment (e.g., Kubernetes Pod environment variables, Airflow variables, or Python configuration files).
*   **Helper Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error handling)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date handling utilities, including `DWDate_Gib_Zeitraum`)
    *   `DWMSG_` functions (Logging and status updates)
    These helper functions will need to be re-implemented in Python or replaced with standard Python libraries and potentially GCP-native services for logging and monitoring.
*   **Core Kernel Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_evn.ksh`. This is the most significant dependency. Its migration is outside the direct scope of this wrapper script's migration but is crucial for the overall job's functionality. The Python wrapper will invoke the migrated version of this core script.

There are no apparent direct database connections, SFTP, or other external system interactions handled directly by this specific wrapper script. All such interactions are presumed to be within the core kernel script.

## 7. Unresolved / Risks
Several aspects need clarification and present potential risks:

*   **Missing Origins for Variables/Functions:**
    *   The exact definition and values of `BERT_DIR_ROOT` must be explicitly defined for the target Python environment.
    *   The implementation details of helper functions like `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, and all `DWMSG_` functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`) are crucial. Their Python equivalents need to be developed or mapped to existing framework utilities.
    *   The `dummy` argument in `DWDate_Gib_Zeitraum` needs to be understood if it has any significance beyond a placeholder.
*   **Core Kernel Script `k_ausd_bp_ta_bpr_evn.ksh`:** The migration of this wrapper script is dependent on the migration of its core component. The logic of `k_ausd_bp_ta_bpr_evn.ksh` will define the actual BigQuery tables and SQL transformations. This core script will need its own dedicated migration design.
*   **Implicit Assumptions:** The original script might rely on implicit environment settings or file paths that are not immediately obvious from the code. A thorough review with legacy system experts is recommended.
*   **Error Codes:** The custom error codes (e.g., `ErrNr=193`, `ErrNr=192`) will need to be mapped to appropriate Python exceptions or a standardized error reporting mechanism in the target environment.

## 8. Build Plan
1.  **Define Environment Variables:** Determine the equivalent of `BERT_DIR_ROOT` and other necessary environment variables in the GCP/Horizon Python environment.
2.  **Python Script Generation:** Create a new Python file, `r_ausd_bp_ta_bpr_evn.py`, based on the provided pseudocode.
3.  **Implement Helper Functions:** Develop Python implementations for `get_current_date_ddmmyyyy`, `get_next_job_entry_number`, `get_log_filename`, `create_log_entry`, `set_stichtag_info`, `log_error`, `handle_error`, `set_status_ok`, and `append_to_log`. These should ideally leverage existing Horizon Python framework utilities or standard Python libraries and integrate with GCP Cloud Logging.
4.  **Integrate Core Logic Invocation:** Replace the `run_core_job` placeholder with the actual invocation mechanism for the migrated `k_ausd_bp_ta_bpr_evn.ksh` (e.g., calling a Python function/module, executing a BigQuery job).
5.  **Unit Testing:** Develop unit tests for the Python script's parameter parsing, date logic, and error handling.
6.  **Integration Testing:** Test the Python wrapper script with the migrated core kernel script (`k_ausd_bp_ta_bpr_evn.py` or its BigQuery equivalent).
7.  **Deployment:** Deploy the Python script to the target orchestration platform (e.g., Airflow, Cloud Composer).

**Build Language:** Python