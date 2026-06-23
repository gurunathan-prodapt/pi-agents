# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh

## 1. Purpose & Scope
This job, `r_ausd_v_ta_vvl_dwh.ksh`, is a KornShell wrapper script designed for orchestrating a contract data reconciliation process for the `ta_vvl_dwh` table. Its primary purpose is to:
- Initialize the runtime environment by sourcing necessary shell libraries.
- Parse command-line parameters, including a help option (`-h`) and two other generic parameters (`-s`, `-l`).
- Implement an error-handling and logging framework, setting traps for interrupts and errors.
- Orchestrate the execution of a core processing script, `k_ausd_v_ta_vvl_dwh.ksh`, passing job-specific identifiers.
- Log job status and outcomes, indicating successful completion or error conditions.

The script itself does not contain business logic for data transformation or reconciliation; it serves purely as an orchestration layer, delegating the core work to the `k_ausd_v_ta_vvl_dwh.ksh` script. The scope of this migration is to convert this KornShell orchestration layer to an equivalent BigQuery-native solution, integrating with a BigQuery-native version of the core processing logic.

## 2. Source Inventory
The job consists of a single source file:

- **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh`
- **Technology:** KornShell
- **Complexity Tier:** Medium
- **Automation Bucket:** semi_auto
- **Summary:** This script acts as a framework for reconciling contract data. It handles parameter parsing, environment setup, logging, error trapping, and orchestrates the execution of a core processing script.

## 3. Target Architecture
The target architecture will leverage BigQuery's native capabilities for scripting, stored procedures, and data warehousing.

- **Orchestration:** The KornShell wrapper will be re-implemented as a BigQuery Script. This script will handle parameter processing, job initialization, logging, and calling the core processing logic. For external scheduling or more complex workflows, Cloud Composer (Airflow) or Cloud Workflows can be considered, but for this specific wrapper, a BigQuery Script is sufficient.
- **Core Processing Logic:** The invoked `k_ausd_v_ta_vvl_dwh.ksh` script, which contains the actual reconciliation business logic, will be migrated to a BigQuery Stored Procedure. This migration needs to be treated as a separate, dependent task.
- **Logging and Status Management:** The custom `DWMSG_*` functions and file-based logging will be replaced by dedicated BigQuery logging and audit tables. These tables will store job execution metadata, status, and any messages or errors generated during runtime.
- **Environment and Configuration:** Shell-sourced environment variables and configuration files (`.dw_init`) will be replaced by BigQuery Script parameters, BigQuery configuration tables, or session variables.
- **Utilities:** Shell utilities like `date` and `tee` will be replaced by their BigQuery SQL function equivalents (e.g., `FORMAT_DATE(CURRENT_DATE())`, `INSERT` statements for logging).

## 4. Data Flow & Lineage
The original job's data flow involves the `r_ausd_v_ta_vvl_dwh.ksh` script orchestrating the execution of `k_ausd_v_ta_vvl_dwh.ksh`.

**Legacy Data Flow:**
1. `r_ausd_v_ta_vvl_dwh.ksh` (Wrapper Script)
   - Initializes environment and error handling.
   - Parses command-line parameters.
   - Generates a `JobKennung` and `DW_EintragsNr`.
   - Sets up file-based logging.
   - INVOKES `k_ausd_v_ta_vvl_dwh.ksh` (Core Script) with `JobKennung` and `DW_EintragsNr` as parameters.
   - Appends core script output to the log file.
   - Updates job status upon completion.

**Target BigQuery Data Flow:**
1. **BigQuery Orchestration Script (`r_ausd_v_ta_vvl_dwh_bq.sql`):**
   - Declares script variables for `ProgName`, `ProgVersion`, `JobKennung`, `v_sysdate`, etc.
   - Accepts parameters for `-s`, `-l` (and optionally `-h` for help emulation).
   - Performs parameter validation.
   - Calls `DWMSG_ErmittleNr_SP` (a BigQuery Stored Procedure) to obtain a job entry number.
   - Calls `DWMSG_Logdateiname_SP` and `DWMSG_ErzeugeEintrag_SP` to record job metadata in a logging table (e.g., `project.dataset.job_log`).
   - Calls `DWMSG_SetzeStichtagInfo_SP` to set date information.
   - Inserts job header information into `project.dataset.job_log`.
   - **CALLS** the core processing BigQuery Stored Procedure: `project.dataset.k_ausd_v_ta_vvl_dwh_sp(JobKennung, DW_EintragsNr)`.
   - Handles exceptions using `BEGIN...EXCEPTION...END` blocks, logging errors via `DWMSG_Fehlerbehandlung_SP` and inserting into `job_log`.
   - On successful completion, inserts a success message into `job_log` and calls `DWMSG_SetzeStatusOK_SP`.

This establishes a clear lineage where the BigQuery Orchestration Script calls the BigQuery Stored Procedure for the core logic, with all actions and states recorded in BigQuery logging tables.

## 5. Transformation Logic

The migration involves translating KornShell constructs and patterns to BigQuery SQL Scripting and Stored Procedures.

**Key Transformations:**

- **Environment Initialization:**
  - **Legacy:** `. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, etc.
  - **Target:**
    - `DECLARE` statements for script-level variables.
    - Configuration values retrieved from dedicated BigQuery configuration tables.
    - Sourced shell functions (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will be re-implemented as BigQuery Stored Procedures or user-defined functions (UDFs) if their logic is purely functional and reusable.
- **Parameter Parsing (`getopts`):**
  - **Legacy:** `while getopts ... do case $param in ... esac done`
  - **Target:** BigQuery Script parameters (e.g., `DECLARE p_s STRING DEFAULT NULL;`), and conditional `IF` statements to handle parameter logic.
- **Error Handling and Traps (`set -eu`, `trap`):**
  - **Legacy:** `set -eu` for strict mode; `trap "..." INT`, `trap "..." ERR`.
  - **Target:** BigQuery `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. Custom error logging will involve `INSERT` statements into the `job_log` table and `SIGNAL SQLSTATE` for explicit error propagation. The `DWMSG_MeldeFehler` and `DWMSG_Fehlerbehandlung` functions will be converted to BigQuery Stored Procedures.
- **Variable Definitions:**
  - **Legacy:** `ProgName="...", typeset -u JobKennung="..."`, `v_sysdate=$(date +%d%m%Y)`
  - **Target:** `DECLARE ProgName STRING DEFAULT '...';`, `DECLARE JobKennung STRING DEFAULT UPPER('...');`, `DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());`
- **Logging (`print`, `tee -a $LogDatei`):**
  - **Legacy:** `print "..."`, `print "..." | tee -a $LogDatei`.
  - **Target:** `INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_message, log_ts, log_level) VALUES (...)`.
- **Script Invocation (`${Name_Kernskript} ...`):**
  - **Legacy:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh -j $JobKennung -f ${DW_EintragsNr}`
  - **Target:** `CALL project.dataset.k_ausd_v_ta_vvl_dwh_sp(JobKennung, DW_EintragsNr);` (assuming the core script is migrated to a BigQuery Stored Procedure).
- **Exit Codes:**
  - **Legacy:** `exit $ErrNr`, `exit 0`.
  - **Target:** Implicit exit on script completion, or `SIGNAL SQLSTATE` to indicate error.

## 6. External Dependencies
The original script has several dependencies, primarily other shell scripts and system utilities.

- **Shell Libraries (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`):**
  - **Replacement Strategy:** These will be re-implemented as BigQuery Stored Procedures, UDFs, or integrated directly into the BigQuery Orchestration Script as part of the setup and utility functions. Configuration values from `.dw_init` will be moved to BigQuery configuration tables.
- **Core Script (`k_ausd_v_ta_vvl_dwh.ksh`):**
  - **Replacement Strategy:** This script contains the primary business logic and will be migrated to a BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_vvl_dwh_sp`. This migration is a separate and critical dependency.
- **System Utilities (`date`, `tee`):**
  - **Replacement Strategy:** `date` will be replaced by BigQuery SQL functions like `CURRENT_DATE()` and `FORMAT_DATE()`. `tee` (for logging to console and file) will be replaced by `INSERT` statements to a BigQuery logging table.
- **`DWMSG_*` Functions:**
  - **Replacement Strategy:** These functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`) are custom error and logging handlers. They will be re-implemented as BigQuery Stored Procedures that interact with new logging/audit tables in BigQuery.

There were no `external_systems` or `unresolved_targets` explicitly identified in the initial analysis, implying that database connections or external system integrations are handled by the `k_ausd_v_ta_vvl_dwh.ksh` script or are not directly part of this wrapper.

## 7. Unresolved / Risks
- **Core Script Migration:** The most significant dependency and potential risk is the migration of `k_ausd_v_ta_vvl_dwh.ksh`. The success of this wrapper's migration heavily relies on `k_ausd_v_ta_vvl_dwh.ksh` being successfully converted to a BigQuery Stored Procedure.
- **`DWMSG_*` Functions Exact Semantics:** The precise logic and persistence mechanisms of the `DWMSG_*` functions (e.g., where `DW_EintragsNr` is sourced from, how `LogDatei` is determined beyond its name) need to be fully understood to accurately replicate their behavior in BigQuery logging tables and stored procedures.
- **Parameter Usage:** The parameters `-s` and `-l` are accepted by `getopts` but not explicitly used within the provided script. Their intended purpose and usage in downstream or sourced components need to be clarified to ensure correct migration and functionality.
- **Error Propagation:** While `BEGIN...EXCEPTION` handles errors, ensuring the exact same error codes and exit behaviors are replicated for external callers might require careful design of error messages and `SIGNAL SQLSTATE`.
- **Complexity of Sourced Libraries:** The contents of the sourced KSH libraries (e.g., `f_alis_msgerr.ksh`) are not fully analyzed. Their migration to BigQuery Stored Procedures might introduce additional complexity.

## 8. Build Plan
The build plan focuses on generating BigQuery-native components to replace the KornShell script.

1.  **Develop Logging & Audit Tables (BigQuery SQL DDL):**
    *   Create `project.dataset.job_log` table to store job execution logs (job number, job kennung, timestamp, message, log level, etc.).
    *   Create any necessary `job_control` or `job_status` tables to replicate the functionality of the `DWMSG_*` functions.

2.  **Develop `DWMSG_*` BigQuery Stored Procedures (BigQuery SQL):**
    *   `DWMSG_ErmittleNr_SP()`: Procedure to generate/retrieve a unique job entry number.
    *   `DWMSG_Logdateiname_SP(IN JobKennung STRING, IN DW_EintragsNr INT64, OUT LogDatei STRING)`: Procedure to determine log file name (or equivalent identifier for log entry grouping).
    *   `DWMSG_ErzeugeEintrag_SP(IN DW_EintragsNr INT64, IN JobKennung STRING, IN ScriptName STRING, IN LogIdentifier STRING)`: Procedure to create initial log entry.
    *   `DWMSG_SetzeStichtagInfo_SP(IN DW_EintragsNr INT64, IN DateValue STRING, IN DateFormat STRING)`: Procedure to record reference date information.
    *   `DWMSG_Fehlerbehandlung_SP(IN DW_EintragsNr INT64)`: Procedure to handle errors, log details, and set status.
    *   `DWMSG_SetzeStatusOK_SP(IN DW_EintragsNr INT64)`: Procedure to mark job as successful.

3.  **Develop BigQuery Orchestration Script (`r_ausd_v_ta_vvl_dwh_bq.sql` - BigQuery Script):**
    *   Based on the provided BigQuery SQL Pseudocode from the design tool.
    *   Implement variable declarations (`DECLARE`).
    *   Implement parameter handling (e.g., `IF p_h IS NOT NULL THEN ... END IF;`).
    *   Integrate calls to the `DWMSG_*` stored procedures for logging and status.
    *   Implement `BEGIN...EXCEPTION WHEN ERROR THEN...END` for robust error handling.
    *   Include the `CALL` statement for the core processing stored procedure (`project.dataset.k_ausd_v_ta_vvl_dwh_sp`).

4.  **Migrate Core Processing Script (`k_ausd_v_ta_vvl_dwh.ksh` to `k_ausd_v_ta_vvl_dwh_sp` - BigQuery Stored Procedure):**
    *   This is a separate, dependent task. Its details are outside the scope of this document but are crucial for end-to-end functionality.

5.  **Configuration Management:**
    *   Define how environment-specific values (`BERT_DIR_ROOT`) or other configuration parameters will be managed in BigQuery (e.g., dedicated configuration tables, BigQuery connections, or external configuration files for Cloud Composer).