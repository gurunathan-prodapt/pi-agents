# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh

## 1. Purpose & Scope

The source file `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh` is a KornShell wrapper script. Its primary purpose is to orchestrate the reconciliation of contract data for the `ta_discount` table. It performs essential setup tasks, including environment initialization, command-line parameter validation, and establishing logging and error handling mechanisms. Crucially, this script then invokes a separate, core processing script (`k_ausd_v_ta_discount.ksh`) where the actual business logic for data reconciliation resides. Upon successful completion of the core script, the wrapper ensures proper logging of the job status.

The scope of this migration design is to translate the functionality of this KornShell wrapper script to BigQuery, focusing on maintaining its orchestration, parameter handling, and logging capabilities within the BigQuery ecosystem. The design assumes that the core processing logic within `k_ausd_v_ta_discount.ksh` will be migrated separately into a BigQuery-compatible stored procedure.

## 2. Source Inventory

The job consists of a single primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh`
    *   **Technology:** KornShell
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Complexity Tier:** medium
    *   **Migration Bucket:** semi_auto
    *   **Purpose:** Wrapper/orchestration script for contract data reconciliation (`ta_discount`). It sets up the environment, handles parameters, and calls a core processing script.
    *   **Invokes:** `k_ausd_v_ta_discount.ksh` (core processing script), which contains the main data reconciliation logic.

## 3. Target Architecture

The migration targets BigQuery as the primary platform. The KornShell wrapper's functionality will be replaced with BigQuery scripting and stored procedures, utilizing BigQuery for orchestration, parameter handling, and logging.

The target architecture will consist of:

*   **BigQuery Stored Procedure (Wrapper):** A main BigQuery stored procedure (e.g., `project.dataset.Vertragsdatenabgleich_wrapper`) will replicate the orchestration logic of `r_ausd_v_ta_discount.ksh`. This procedure will handle parameter parsing, initialize job-specific metadata, manage logging, and invoke the core reconciliation logic.
*   **BigQuery Stored Procedure (Core Logic):** The functionality of `k_ausd_v_ta_discount.ksh` (the core processing script) will be migrated into a separate BigQuery stored procedure (e.g., `project.dataset.k_ausd_v_ta_discount`). This procedure will encapsulate the actual data reconciliation and transformation logic.
*   **Logging and Status Tables:** Dedicated BigQuery tables will replace the legacy log files and custom status tracking.
    *   `project.dataset.job_log`: To store detailed job execution logs (e.g., `job_entry_nr`, `job_kennung`, `script_name`, `log_timestamp`, `log_level`, `message`).
    *   `project.dataset.job_status`: To maintain the current status and historical records of job runs (e.g., `job_entry_nr`, `job_kennung`, `stichtag`, `status_code`, `status_text`).
*   **Configuration Tables (Optional):** Environment variables and hardcoded paths could be externalized into BigQuery configuration tables or passed as stored procedure parameters for better modularity and maintainability.
*   **External Orchestration (Cloud Composer/Airflow):** For aspects that BigQuery SQL cannot directly handle (e.g., specific OS-level traps, file system interactions beyond Cloud Storage, or complex scheduling), an external orchestrator like Cloud Composer (Apache Airflow) may be used to schedule the BigQuery stored procedures and manage their execution flow, including error handling and retries.

## 4. Data Flow & Lineage

**Current Inferred Data Flow:**

1.  The `r_ausd_v_ta_discount.ksh` wrapper script is executed.
2.  It loads environment variables from `$HOME/.dw_init` and sources utility scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` for error handling and date manipulation.
3.  Command-line parameters are parsed using `getopts`.
4.  Job-specific identifiers and a log file name are determined using custom `DWMSG_` functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`).
5.  Error traps are set for `INT` and `ERR` signals, directing control to `DWMSG_Fehlerbehandlung`.
6.  A banner and job metadata are printed to console and log file.
7.  The core processing script `k_ausd_v_ta_discount.ksh` is invoked with job-specific parameters, and its output is redirected to the log file.
8.  Upon successful completion, a success message is logged, and the job status is updated via `DWMSG_SetzeStatusOK`.

*(Note: Direct lineage edges for this specific script were not found in the `lineage_edges` table; the above flow is inferred from static code analysis and the file's purpose description.)*

**Target BigQuery Data Flow:**

1.  An external orchestrator (e.g., Cloud Composer) or a scheduled query initiates the BigQuery wrapper stored procedure (`project.dataset.Vertragsdatenabgleich_wrapper`).
2.  The wrapper procedure initializes declared variables and retrieves configuration (e.g., from configuration tables).
3.  Input parameters to the wrapper procedure are validated. If a help parameter is provided, usage information is returned.
4.  Job entry numbers and log identifiers are generated, similar to `DWMSG_ErmittleNr` and `DWMSG_Logdateiname`.
5.  Job start and metadata information are inserted into the `project.dataset.job_log` table.
6.  The job's status is set to 'RUNNING' in the `project.dataset.job_status` table.
7.  The core BigQuery stored procedure (`project.dataset.k_ausd_v_ta_discount`) is invoked with necessary parameters (e.g., `JobKennung`, `DW_EintragsNr`).
8.  Error handling is managed by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. If an error occurs in the core procedure, an error message is logged to `project.dataset.job_log`, and the job status is updated to 'ERROR' in `project.dataset.job_status`.
9.  Upon successful execution of the core procedure, a success message is logged to `project.dataset.job_log`, and the job status is updated to 'OK' in `project.dataset.job_status`.

## 5. Transformation Logic

The KornShell wrapper script itself contains no direct data transformation logic. Its logic is purely for orchestration, environment setup, parameter handling, and error management. The actual data reconciliation logic is presumed to be within the `k_ausd_v_ta_discount.ksh` script.

The migration involves translating the shell script's control flow and utility calls into BigQuery SQL procedural constructs:

*   **Environment Variables (`ProgName`, `ProgVersion`, `BERT_DIR_ROOT`, `HOME`):**
    *   **Legacy:** Shell environment variables.
    *   **Target:** `DECLARE` statements for local variables within BigQuery stored procedures, procedure parameters, or values retrieved from dedicated BigQuery configuration tables.
*   **Parameter Parsing (`getopts`):**
    *   **Legacy:** `while getopts` loop to parse command-line options.
    *   **Target:** For this simple case, the single `-h` option can be handled directly as a procedure parameter. For more complex parameter handling, BigQuery procedural `IF` or `CASE` statements would be used.
*   **Conditionals (`if`, `case`):**
    *   **Legacy:** Shell `if [ ! $ErrNr -eq 0 ]`, `case $param in ...`.
    *   **Target:** BigQuery `IF ... THEN ... ELSE ... END IF` and `CASE` expressions.
*   **Functions and Commands (`usage`, `date`, `tee`, `print`):**
    *   **Legacy:** `usage()` function, `date +%d%m%Y`, `tee -a $LogDatei`, `print` statements.
    *   **Target:**
        *   `usage`: Replicated as a `SELECT` statement returning the help text within the wrapper stored procedure.
        *   `date`: `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
        *   `tee -a`: Replaced by `INSERT` statements into the `job_log` table and potentially `SELECT` statements for console output if executed interactively.
        *   `print`: Replaced by `INSERT` statements into the `job_log` table.
*   **Error Traps (`trap INT`, `trap ERR`):**
    *   **Legacy:** Shell `trap` command to catch signals and execute `DWMSG_Fehlerbehandlung`.
    *   **Target:** BigQuery `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks to catch and handle SQL exceptions. Error details will be logged to `job_log`, and `job_status` will be updated. `RAISE USING MESSAGE` will propagate errors if needed.
*   **File Manipulation (Sourcing scripts, Log file writes):**
    *   **Legacy:** `. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/.../*.ksh`, output redirection to log files.
    *   **Target:** Sourced utility scripts will be converted into BigQuery stored procedures or helper functions. Log file writes will be replaced by `INSERT` statements into the `job_log` table.
*   **Core Script Invocation:**
    *   **Legacy:** `${Name_Kernskript} ...`.
    *   **Target:** `CALL project.dataset.k_ausd_v_ta_discount(...)` to execute the migrated core logic as a BigQuery stored procedure.

## 6. External Dependencies

The original script has the following external dependencies and their proposed replacements:

*   **Legacy Shell Environment (`. $HOME/.dw_init`):**
    *   **Replacement:** Configuration values will be passed as parameters to the BigQuery stored procedure, retrieved from BigQuery configuration tables, or set as session variables.
*   **Sourced Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):**
    *   **Replacement:** The functionalities of these scripts will be re-implemented as BigQuery stored procedures or internal helper SQL functions. For example, error messaging (`DWMSG_MeldeFehler`) and date formatting (`h_alis_date.ksh`) can be directly translated.
*   **Custom Logging/Error Handling Framework (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`):**
    *   **Replacement:** These functions will be implemented as BigQuery stored procedures that interact with the `project.dataset.job_log` and `project.dataset.job_status` tables to record events and update job progress.
*   **Core Processing Script (`k_ausd_v_ta_discount.ksh`):**
    *   **Replacement:** This script will be migrated into a dedicated BigQuery stored procedure (`project.dataset.k_ausd_v_ta_discount`) that contains the actual data reconciliation logic for `ta_discount`. Its interface will be defined to accept necessary parameters from the wrapper.
*   **Standard Unix Utilities (`getopts`, `date`, `tee`, `print`, `trap`):**
    *   **Replacement:** As detailed in Section 5, these will be replaced by BigQuery SQL constructs (`DECLARE`, `IF`, `CASE`, `FORMAT_DATE`, `INSERT`, `BEGIN...EXCEPTION`) or handled by the external orchestration layer.

## 7. Unresolved / Risks

*   **Unknown Core Logic:** The most significant unresolved item is the exact business logic and data transformations within `k_ausd_v_ta_discount.ksh`. This script was not part of the initial `component_files` and its content is unknown. A separate analysis and design will be required for this core script to complete the end-to-end migration. This impacts the `semi_auto` migration bucket.
*   **Shell-Specific Features:** Direct emulation of OS-level traps (signals) and advanced shell process management within BigQuery SQL is not possible. These aspects will rely on BigQuery's `EXCEPTION` blocks and robust external orchestration (e.g., Cloud Composer/Airflow) for retry mechanisms and failure handling.
*   **File System Interactions:** If `k_ausd_v_ta_discount.ksh` performs complex file I/O or interacts with specific legacy file systems, this will require further investigation. These operations might need to be re-engineered using Cloud Storage, Cloud Functions, or Cloud Run in conjunction with BigQuery.
*   **Parameter Complexity of Core Script:** The number and type of parameters passed to `k_ausd_v_ta_discount.ksh` (beyond `-j` and `-f`) are currently unknown. This will influence the design of the target BigQuery stored procedure for the core logic.

## 8. Build Plan

The build plan will proceed in an ordered fashion, starting with foundational BigQuery objects and then implementing the procedural logic.

1.  **Define BigQuery Tables (BQSQL):**
    *   `project.dataset.job_log`: Create this table to capture all logging information.
    *   `project.dataset.job_status`: Create this table to track the status and historical records of job runs.
    *   (Optional) `project.dataset.job_config`: Create a configuration table if environment variables or constants are to be externalized.

2.  **Implement Utility Stored Procedures (BQSQL):**
    *   Create BigQuery stored procedures for the custom `DWMSG_` framework functions, including:
        *   `DWMSG_ErmittleNr_proc()`
        *   `DWMSG_Logdateiname_proc()`
        *   `DWMSG_ErzeugeEintrag_proc()`
        *   `DWMSG_SetzeStichtagInfo_proc()`
        *   `DWMSG_Fehlerbehandlung_proc()`
        *   `DWMSG_SetzeStatusOK_proc()`

3.  **Design and Implement Core Processing Stored Procedure (BQSQL):**
    *   **`project.dataset.k_ausd_v_ta_discount`**: This is a placeholder. A separate design phase is required to analyze the legacy `k_ausd_v_ta_discount.ksh` script and translate its data reconciliation logic into a BigQuery stored procedure. This procedure will take `JobKennung` and `DW_EintragsNr` as input parameters.

4.  **Implement Wrapper Stored Procedure (BQSQL):**
    *   Create the `project.dataset.Vertragsdatenabgleich_wrapper` BigQuery stored procedure. This procedure will implement the logic derived from `r_ausd_v_ta_discount.ksh`, including:
        *   Parameter handling (e.g., for a help flag).
        *   Variable declarations.
        *   Calls to the utility stored procedures for logging and status management.
        *   The `CALL` statement to invoke `project.dataset.k_ausd_v_ta_discount`.
        *   `BEGIN...EXCEPTION` blocks for error handling.

5.  **Develop External Orchestration (Python/Airflow):**
    *   If necessary, create a Cloud Composer (Apache Airflow) DAG to schedule the `project.dataset.Vertragsdatenabgleich_wrapper` stored procedure. This DAG can also incorporate additional error monitoring, retry logic, and integration with other Google Cloud services if required by the core script's dependencies.

This phased approach allows for a modular migration, addressing the wrapper logic first while acknowledging the need for subsequent analysis and migration of the core reconciliation logic.