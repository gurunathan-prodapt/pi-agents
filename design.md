# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `r_ausd_v_ta_action_assoc.ksh` to Google Cloud Platform, specifically targeting BigQuery. The original script serves as a wrapper or orchestration script for a contract data reconciliation job focused on the `ta_action_assoc` table. Its primary functions include:
*   Loading environment variables and utility functions.
*   Parsing command-line parameters.
*   Initializing job-specific logging and error handling.
*   Invoking a core processing script (`k_ausd_v_ta_action_assoc.ksh`) which contains the actual business logic.
*   Managing job status updates and logging the execution flow and any errors.

The scope of this migration focuses on transforming the wrapper functionality into BigQuery-native components, primarily stored procedures and audit tables. The migration of the core script `k_ausd_v_ta_action_assoc.ksh` is considered a subsequent, dependent task.

## 2. Source Inventory
The job is composed of a single main script.

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh`
*   **Technology**: KornShell (ksh)
*   **Complexity Tier**: Medium
*   **Automation Bucket**: Semi-auto (B2)
*   **File Purpose**: ETL wrapper/orchestration script.

## 3. Target Architecture
The migrated solution will primarily leverage BigQuery for its core functionality, supplemented by BigQuery audit/log tables.

*   **BigQuery Stored Procedures**:
    *   `sp_vertragsdatenabgleich_entry`: An entry-point stored procedure to handle initial parameter validation and invocation of the main orchestration procedure.
    *   `sp_vertragsdatenabgleich`: The main orchestration stored procedure, replacing the KornShell wrapper logic. This will manage job initialization, logging, error handling, and the invocation of the migrated core business logic.
    *   `sp_k_ausd_v_ta_action_assoc` (placeholder): A future stored procedure that will contain the migrated logic from the currently external `k_ausd_v_ta_action_assoc.ksh` script.
*   **BigQuery Audit/Log Tables**:
    *   `project.dataset.job_log`: To store main job entry records, status, and timestamps.
    *   `project.dataset.job_control`: To store job-specific control parameters, such as the `Stichtag` (reference date).
    *   `project.dataset.job_log_detail`: For granular log messages and events during execution.
    *   `project.dataset.job_error_log`: To record detailed error information.
*   **External Orchestration (Optional)**: For advanced error handling, retry mechanisms, or interaction with external systems not directly supported by BigQuery SQL, Cloud Workflows or Cloud Composer (Airflow DAG) could be considered.

## 4. Data Flow & Lineage
**Legacy Data Flow:**
1.  The `r_ausd_v_ta_action_assoc.ksh` script starts execution.
2.  It sources environment variables from `$HOME/.dw_init` and utilizes the `BERT_DIR_ROOT` variable to locate other scripts.
3.  Utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are sourced for common functions (e.g., error handling, date formatting, parameter parsing).
4.  Parameters are parsed using `getopts`.
5.  Custom `DWMSG_` functions are used to manage job entry numbers, log file names, create initial log entries, and set a reference date (`Stichtag`).
6.  `trap` commands are set up to catch `INT` (interrupt) and `ERR` (shell command error) signals, directing error handling to `DWMSG_Fehlerbehandlung`.
7.  The core business logic script `k_ausd_v_ta_action_assoc.ksh` is invoked with `JobKennung` and `DW_EintragsNr` as parameters. Its standard output and error are redirected to a generated log file.
8.  Upon successful completion of the core script, a success message is printed to the console and appended to the log file. `DWMSG_SetzeStatusOK` updates the job's status.
9.  If any errors occur, `DWMSG_MeldeFehler` is called, and the script exits with an error code.

**Target BigQuery Data Flow:**
1.  An external scheduler (e.g., Cloud Composer, Cloud Scheduler) or direct invocation triggers `project.dataset.sp_vertragsdatenabgleich_entry`.
2.  `sp_vertragsdatenabgleich_entry` handles initial parameter validation (e.g., for help requests or missing arguments) and then calls `project.dataset.sp_vertragsdatenabgleich`.
3.  `project.dataset.sp_vertragsdatenabgleich` begins execution:
    *   Declares variables for job name, version, identifiers, and dates, potentially initialized from configuration tables or procedure parameters.
    *   Inserts a new record into `project.dataset.job_log` (simulating `DWMSG_ErzeugeEintrag`) and `project.dataset.job_control` (simulating `DWMSG_SetzeStichtagInfo`) for audit trail.
    *   Enters a `BEGIN...EXCEPTION...END` block to handle errors gracefully (replacing shell `trap`).
    *   Calls `project.dataset.sp_k_ausd_v_ta_action_assoc` (the migrated core logic).
    *   On successful completion, updates the status in `project.dataset.job_log` to 'OK' (simulating `DWMSG_SetzeStatusOK`) and inserts a success message into `project.dataset.job_log_detail`.
    *   If an exception occurs, it logs the error details to `project.dataset.job_error_log`, updates the job status in `project.dataset.job_log` to 'ERROR', and raises an error to indicate job failure.

## 5. Transformation Logic
The transformation will focus on converting shell script constructs to their BigQuery SQL equivalents within stored procedures.

*   **Parameter Handling (`getopts`)**: Will be replaced by standard BigQuery stored procedure `IN` parameters.
*   **Environment Sourcing (`. $HOME/.dw_init`)**: Global configuration values and environment variables will be passed as parameters to the stored procedure, retrieved from dedicated BigQuery configuration tables, or declared as `DECLARE` variables within the procedure. Sensitive data should be managed via Google Secret Manager.
*   **Utility Functions (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)**: The functionality provided by these sourced scripts will be re-implemented directly in BigQuery SQL within the stored procedure or as separate BigQuery functions/procedures. The `DWMSG_` functions will be translated into `INSERT` and `UPDATE` statements against the new BigQuery audit/log tables. Date formatting will use BigQuery's `FORMAT_DATE` and `CURRENT_DATE()` functions.
*   **Error Handling (`set -eu`, `trap`)**: The `set -eu` behavior (exit on error/unset variable) will be inherently handled by BigQuery SQL's execution model and explicit error handling within `BEGIN...EXCEPTION...END` blocks. The `trap` commands for `INT` and `ERR` will be replaced by the `EXCEPTION WHEN ERROR` clause in BigQuery, which allows for custom error logging and re-raising exceptions.
*   **Core Script Invocation (`${Name_Kernskript} ...`)**: The call to `k_ausd_v_ta_action_assoc.ksh` will be replaced by a `CALL` statement to the migrated BigQuery stored procedure `project.dataset.sp_k_ausd_v_ta_action_assoc`.
*   **Logging (`print`, `tee -a $LogDatei`)**: All console output and redirections to log files will be replaced by `INSERT` statements into the `project.dataset.job_log_detail` table, providing a structured and queryable log of job execution.

## 6. External Dependencies
**Legacy External Dependencies:**
*   **File-based Configuration**: `$HOME/.dw_init` for system-wide or user-specific environment variables.
*   **Custom Shell Utility Scripts**: Scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` provide shared functions.
*   **Core Business Logic Script**: `k_ausd_v_ta_action_assoc.ksh` which is external to this wrapper and contains the main processing logic. This script is likely to have its own set of dependencies (e.g., database connections, file I/O).

**Target BigQuery Replacement:**
*   **Configuration**: Environment variables from `$HOME/.dw_init` will be converted into BigQuery parameters (for SPs), configuration tables, or explicit `DECLARE` statements. Sensitive information will be managed using Google Secret Manager.
*   **Utility Scripts**: The functionality of the `DWMSG_` utilities will be re-implemented directly within the BigQuery stored procedures or as BigQuery functions/procedures that interact with the audit/log tables.
*   **Core Business Logic**: The `k_ausd_v_ta_action_assoc.ksh` script is a critical dependency. It must be analyzed and migrated separately. If its logic is purely SQL, it will be migrated to a BigQuery stored procedure (`sp_k_ausd_v_ta_action_assoc`). If it involves complex transformations, external data sources, or filesystem operations, other Google Cloud services like Dataflow (for ETL), Cloud Functions (for event-driven tasks), or Cloud Run (for containerized applications) might be necessary, orchestrated by Cloud Workflows or Cloud Composer.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_v_ta_action_assoc.ksh`)**: The actual business logic resides in this script, which has not yet been analyzed. Its complexity, data sources, transformations, and potential external system interactions are currently unknown and pose the main risk. A detailed analysis and migration plan for `k_ausd_v_ta_action_assoc.ksh` is essential.
*   **Missing Lineage Edges**: The `lineage_edges` tool did not identify the invocation of `k_ausd_v_ta_action_assoc.ksh` by the wrapper script. This suggests that dynamic or shell-based invocations might not be fully captured by automated lineage analysis, requiring manual verification of such dependencies.
*   **Undefined Parameters**: The parameters `-s` and `-l` are declared in the `getopts` string but not used within the `r_ausd_v_ta_action_assoc.ksh` wrapper. Their purpose and usage, if any, will need to be clarified through analysis of the `k_ausd_v_ta_action_assoc.ksh` script or other documentation.
*   **Advanced Shell Semantics**: While basic error handling is covered by `BEGIN...EXCEPTION...END`, highly specific shell trap behaviors or complex process management might require more sophisticated orchestration outside of pure BigQuery SQL (e.g., Cloud Workflows).

## 8. Build Plan
1.  **Define BigQuery Audit/Log Table Schemas (DDL)**:
    *   Create tables: `project.dataset.job_log`, `project.dataset.job_control`, `project.dataset.job_log_detail`, `project.dataset.job_error_log`.
    *   Language: BigQuery DDL.
2.  **Develop BigQuery Stored Procedures for Utility Functions**:
    *   Translate the logic of `DWMSG_` functions (e.g., `_ErmittleNr`, `_Logdateiname`, `_ErzeugeEintrag`, `_SetzeStichtagInfo`, `_Fehlerbehandlung`, `_SetzeStatusOK`) into BigQuery SQL procedures/functions that interact with the newly defined audit/log tables.
    *   Language: BigQuery SQL.
3.  **Implement `sp_vertragsdatenabgleich` and `sp_vertragsdatenabgleich_entry` BigQuery Stored Procedures**:
    *   Develop the main wrapper stored procedure, including parameter handling, environment setup from config/parameters, logging to audit tables, and `BEGIN...EXCEPTION...END` blocks for error management.
    *   Implement the entry-point procedure for initial parameter validation.
    *   Language: BigQuery SQL.
4.  **Analyze and Design Migration for `k_ausd_v_ta_action_assoc.ksh` (Dependent Task)**:
    *   Perform a dedicated analysis of the core script to understand its business logic, data sources, transformations, and output targets.
    *   Design its migration to a BigQuery stored procedure (`sp_k_ausd_v_ta_action_assoc`) or other suitable GCP services (e.g., Dataflow, Cloud Run).
    *   Language: Dependent on analysis (likely BigQuery SQL, Python for Dataflow/Cloud Run).
5.  **Integrate Core Logic Invocation**:
    *   Once `sp_k_ausd_v_ta_action_assoc` is developed, update `sp_vertragsdatenabgleich` to call this procedure.
    *   Language: BigQuery SQL.
6.  **Implement External Orchestration (if required)**:
    *   If complex scheduling, retries, or interactions with non-BigQuery services are needed, design and implement a Cloud Composer DAG or Cloud Workflow to orchestrate the BigQuery stored procedures.
    *   Language: Python (for Airflow), YAML (for Cloud Workflows).
7.  **Testing**: Develop unit and integration tests for all migrated components to ensure functional parity and performance.