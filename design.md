# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_p_discount.ksh`. The original script serves as an orchestration wrapper for a data synchronization process specifically targeting the `ta_p_discount` table. Its primary business purpose is to manage the execution environment, parse parameters, handle error logging, and invoke a core kernel script (`k_ausd_v_ta_p_discount.ksh`) responsible for the actual data synchronization logic. The scope of this migration is to re-implement this orchestration logic and its dependencies within the BigQuery ecosystem.

## 2. Source Inventory
The job consists of a single source file:
*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh`
    *   **Technology**: KornShell Script (`ksh`)
    *   **Summary**: Orchestration script for `ta_p_discount` data synchronization. It handles environment setup, parameter parsing, error handling, and calls a core script.
    *   **Complexity Tier**: Not explicitly available in analysis, but inferred as `medium` given the orchestration logic and external script calls.
    *   **Automation Bucket**: `semi_auto`

## 3. Target Architecture
The migration will re-architect the orchestration to run within BigQuery.
*   **Main Component**: A BigQuery Stored Procedure, `project.dataset.sp_bert_v_ta_p_discount`, will replace the `r_ausd_v_ta_p_discount.ksh` shell script.
*   **Core Logic**: The core data synchronization logic originally in `k_ausd_v_ta_p_discount.ksh` will be migrated into a separate BigQuery Stored Procedure, likely named `project.dataset.sp_k_ausd_v_ta_p_discount`.
*   **Logging**: The existing file-based logging (`LogDatei`) will be replaced by dedicated BigQuery audit/logging tables, e.g., `project.dataset.dw_job_log` for job status and `project.dataset.dw_error_log` for error details.
*   **Environment**: Environment variables and sourced scripts (`.dw_init`, `f_alis_msgerr.ksh`, etc.) will be replaced by stored procedure parameters, BigQuery session variables, or configuration tables as needed.
*   **External Orchestration (Optional)**: If complex scheduling or cross-platform dependencies arise, an external orchestrator like Cloud Composer (Airflow) could be used to call the BigQuery stored procedure. However, for this wrapper script, a direct BigQuery stored procedure is sufficient.

## 4. Data Flow & Lineage
The original script's primary role is orchestration rather than direct data processing.
*   **Invocation**: The `r_ausd_v_ta_p_discount.ksh` script is the entry point, handling initial setup and parameter validation.
*   **Dependency Sourcing**: It sources several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) for environment setup, error handling, and parameter management.
*   **Core Logic Invocation**: It then invokes the `k_ausd_v_ta_p_discount.ksh` script, passing job-specific parameters. This `k_ausd_v_ta_p_discount.ksh` is assumed to contain the actual data synchronization logic for the `ta_p_discount` table.
*   **Logging**: Both the wrapper and the core script's outputs are redirected to a dynamically generated log file (`LogDatei`).
*   **Error Handling**: `trap` mechanisms are used to capture `INT` and `ERR` signals, triggering error handling routines.

**Target Data Flow:**
1.  **`project.dataset.sp_bert_v_ta_p_discount` (BigQuery Stored Procedure)**:
    *   Receives parameters (`p_h`, `p_s`, `p_l`).
    *   Initializes job metadata and determines `DW_EintragsNr` (job entry number) and `LogDatei` (log file equivalent) using `dw_job_log` and `dw_job_context` tables.
    *   Performs parameter validation. If errors, logs to `dw_error_log` and raises an exception.
    *   Inserts job start status into `dw_job_log`.
    *   Inserts system date information into `dw_job_context`.
    *   Calls `project.dataset.sp_k_ausd_v_ta_p_discount` (BigQuery Stored Procedure for core logic), passing `JobKennung` and `DW_EintragsNr`.
    *   Upon successful completion, updates job status to `OK` in `dw_job_log`.
    *   Includes `EXCEPTION WHEN ERROR THEN` blocks to catch and log errors to `dw_error_log` and update `dw_job_log` status to `FAILED`.

## 5. Transformation Logic

**Original Script (`r_ausd_v_ta_p_discount.ksh`) Functions and Their BigQuery Equivalents:**

*   **Initialization (ProgName, ProgVersion, JobKennung, v_sysdate):**
    *   **Legacy**: Shell variables and `date` command.
    *   **Target**: BigQuery `DECLARE` statements and `FORMAT_DATE(CURRENT_DATE())`.
*   **Usage Function (`usage()`):**
    *   **Legacy**: `cat <<EOF ... EOF`.
    *   **Target**: BigQuery `SELECT` statements for displaying help text.
*   **Environment Sourcing (`. $HOME/.dw_init`, etc.):**
    *   **Legacy**: Shell `.` command to include external scripts.
    *   **Target**: Replaced by BigQuery stored procedure parameters, BigQuery session variables, or data from BigQuery configuration tables. Specific functionality of sourced scripts (e.g., parameter parsing, date utilities) will be re-implemented directly in BQSQL or handled by external calls to Cloud Functions if too complex for pure SQL.
*   **Parameter Parsing (`getopts`):**
    *   **Legacy**: `while getopts ... do case ... esac`.
    *   **Target**: BigQuery Stored Procedure `IN` parameters (`p_h`, `p_s`, `p_l`) and conditional `IF` statements for validation.
*   **Error Handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`, `trap`):**
    *   **Legacy**: External error handling script, specific error reporting functions, and `trap` for signal handling.
    *   **Target**: BigQuery `INSERT` statements into `dw_error_log` table, `SIGNAL SQLSTATE` for raising errors, and `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for capturing and handling exceptions.
*   **Job Logging (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`):**
    *   **Legacy**: External logging functions, dynamic log file creation, and status updates.
    *   **Target**: BigQuery `INSERT` and `UPDATE` statements against `dw_job_log` and `dw_job_context` tables.
*   **Core Script Invocation (`${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1`):**
    *   **Legacy**: Direct shell execution of `k_ausd_v_ta_p_discount.ksh`.
    *   **Target**: BigQuery `CALL project.dataset.sp_k_ausd_v_ta_p_discount(JobKennung, DW_EintragsNr)`.
*   **Output Redirection (`>> $LogDatei 2>&1`, `| tee -a $LogDatei`):**
    *   **Legacy**: Shell output redirection and `tee` command.
    *   **Target**: All logging will be handled via explicit `INSERT` statements into BigQuery logging tables. Console output, if required, will be via `SELECT` statements.

## 6. External Dependencies
The original script does not directly interact with external systems like Oracle, SFTP, or S3. Its dependencies are primarily other local shell scripts and the underlying `ksh` environment.

*   **`$HOME/.dw_init` (Environment Initialization):**
    *   **Replacement**: Configuration can be managed via BigQuery stored procedure parameters, BigQuery session variables, or a dedicated BigQuery configuration table. Essential environment settings will be directly incorporated into the BigQuery stored procedure logic or its calling environment.
*   **Utility Shell Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):**
    *   **Replacement**: The functionalities provided by these scripts (error messaging, parameter handling utilities, date utilities) will be re-implemented directly in BigQuery SQL using built-in functions, `CREATE FUNCTION` for UDFs if complex, or incorporated into the stored procedure logic.
*   **`k_ausd_v_ta_p_discount.ksh` (Core Kernel Script):**
    *   **Replacement**: This script, containing the core data synchronization logic for `ta_p_discount`, will be migrated into a separate BigQuery Stored Procedure: `project.dataset.sp_k_ausd_v_ta_p_discount`. This stored procedure will be called by `project.dataset.sp_bert_v_ta_p_discount`.

## 7. Unresolved / Risks
*   **Core Logic Complexity**: The content of `k_ausd_v_ta_p_discount.ksh` is unknown. Its complexity will determine the effort required for its migration to `sp_k_ausd_v_ta_p_discount` (likely BQSQL or BQ Stored Procedures). If it involves complex file I/O or operating system interactions, these parts may require redesign with Cloud Storage, Dataflow, or Cloud Functions.
*   **Exact Logging Schema**: The precise schema of the `DWMSG` logging system is not known, so placeholders for BigQuery logging tables are used (`dw_job_log`, `dw_error_log`, `dw_job_context`). These will need to be defined based on the original system's logging details.
*   **Parameter `s` and `l` usage**: The `getopts` section of the source script declares `-s` and `-l` parameters but does not explicitly use them in the provided code. Their purpose and necessity in the BigQuery migration need to be confirmed. The design assumes they are passed through to the core script if needed.
*   **Data Synchronization Approach**: The summary states "data synchronization process for the 'ta_p_discount' table". The exact method of synchronization (e.g., full load, incremental load, CDC) is not defined, which will impact the design of `sp_k_ausd_v_ta_p_discount`.

## 8. Build Plan
The migration will proceed with the following steps, generating artifacts in BigQuery SQL and potentially Python (for external orchestration if needed).

1.  **Define BigQuery Logging Tables (DDL - BigQuery SQL)**:
    *   `project.dataset.dw_job_log` (for job status and metadata)
    *   `project.dataset.dw_error_log` (for error details)
    *   `project.dataset.dw_job_context` (for job context/date information)
2.  **Migrate Utility Script Functionality (BigQuery SQL - UDFs/Logic in SP)**:
    *   Re-implement relevant logic from `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` into BigQuery UDFs or directly into the stored procedures.
3.  **Design and Implement Core Kernel Stored Procedure (`sp_k_ausd_v_ta_p_discount` - BigQuery Stored Procedure / SQL)**:
    *   Analyze `k_ausd_v_ta_p_discount.ksh` to understand its data synchronization logic for `ta_p_discount`.
    *   Translate this logic into BigQuery SQL, creating `project.dataset.sp_k_ausd_v_ta_p_discount`. This is a critical path item and may be a separate sub-project.
4.  **Implement Orchestration Stored Procedure (`sp_bert_v_ta_p_discount` - BigQuery Stored Procedure)**:
    *   Create the `project.dataset.sp_bert_v_ta_p_discount` stored procedure using the provided BigQuery SQL pseudocode as a starting point.
    *   Integrate parameter handling, logging calls, error handling, and the call to `sp_k_ausd_v_ta_p_discount`.
5.  **Testing**:
    *   Unit tests for each stored procedure.
    *   Integration tests to ensure `sp_bert_v_ta_p_discount` correctly calls `sp_k_ausd_v_ta_p_discount` and logs all statuses.
    *   Data validation tests for the synchronized `ta_p_discount` table.
6.  **Deployment**:
    *   Deploy DDL for logging tables.
    *   Deploy BigQuery stored procedures.
    *   Configure scheduling (e.g., using Cloud Composer or BigQuery scheduled queries) to invoke `project.dataset.sp_bert_v_ta_p_discount`.