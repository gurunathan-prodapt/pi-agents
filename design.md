# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh

## 1. Purpose & Scope
This migration targets the KornShell script `r_ausd_bp_ta_bpr_basis.ksh`, which serves as an orchestration layer for the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. Its primary function is to prepare parameters, manage logging, handle errors, and then execute a core processing script, `k_ausd_bp_ta_bpr_basis.ksh`. The job determines a processing date (`Stichtag`) and can support restart/resume capabilities. The scope of this migration is to re-implement this orchestration logic in Google Cloud's BigQuery ecosystem, leveraging BigQuery Stored Procedures for the procedural logic and potentially Cloud Composer for overall workflow management, while delegating core data transformations to a separate BigQuery component derived from the kernel script.

## 2. Source Inventory
This job consists of a single KornShell script.
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh`
    *   **Technology:** KornShell
    *   **Migration Bucket:** semi_auto (B2)
    *   **Role:** Orchestration script. It parses command-line arguments, sets up environment variables, includes helper scripts for error handling and date management, and then invokes a core kernel script.

## 3. Target Architecture
The target architecture in BigQuery will involve:
*   **BigQuery Stored Procedure:** The orchestration logic of `r_ausd_bp_ta_bpr_basis.ksh` will be converted into a BigQuery Stored Procedure, named `project.dataset.ausd_bp_ta_bpr_basis_wrapper`. This procedure will accept parameters for `Stichtag` and `Wiederanlaufwert`.
*   **Logging Table:** A dedicated BigQuery table (e.g., `project.dataset.job_log`) will capture job status, error messages, and execution details, replacing the shell script's log file output and framework-specific logging functions.
*   **Core Processing Stored Procedure:** The downstream kernel script `k_ausd_bp_ta_bpr_basis.ksh` will be migrated into a separate BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_bp_ta_bpr_basis`). The wrapper procedure will `CALL` this core processing procedure.
*   **Orchestration (Optional):** While the wrapper procedure can be scheduled directly in BigQuery, for more complex workflows or dependencies, Cloud Composer (Airflow) could be used to orchestrate the execution of the BigQuery Stored Procedures.
*   **Configuration Tables:** For environment-driven configuration and potential job numbering mechanisms, auxiliary BigQuery tables (e.g., `dataset/job_control`, `dataset/job_parameters`) may be required.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_basis.ksh` script acts as an entry point and orchestrator.
1.  **Parameter Reception:** It accepts `-s` (Stichtag) and `-l` (Wiederanlaufwert) as command-line arguments.
2.  **Environment Setup:** It sources several helper scripts:
    *   `$HOME/.dw_init` (environment initialization)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error/message framework)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter helper)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date helper)
3.  **Date & Restart Value Initialization:** It defaults the restart value to `0` if not provided and determines the processing date, defaulting to the system date if `Stichtag` is not explicitly set.
4.  **Logging Initialization:** It initializes job logging using framework functions to determine a job number and log file name.
5.  **Core Processing Invocation:** The script's main action is to execute the kernel script:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh`
    *   It passes `JobKennung`, `p_stichtag`, `DW_EintragsNr`, and `p_wiederanlaufWert` to the kernel script.
6.  **Status Reporting:** Upon completion of the kernel script, it updates the job status via framework functions and exits.

In BigQuery, this will translate to the wrapper stored procedure receiving parameters, performing initial setup and logging (inserting into `job_log`), and then calling the core processing stored procedure (`k_ausd_bp_ta_bpr_basis`).

## 5. Transformation Logic

The shell script's logic will be re-implemented in BigQuery SQL as follows:

*   **Parameter Handling:**
    *   Bash `getopts` will be replaced by BigQuery Stored Procedure input parameters (`IN p_stichtag STRING`, `IN p_wiederanlaufWert INT64`).
*   **Defaulting Logic:**
    *   Bash `if [[ -z "$p_wiederanlaufWert" ]]` will become `IF p_wiederanlaufWert IS NULL THEN SET v_wiederanlaufWert = 0; END IF;`.
    *   Date defaulting `if [[ -z "$p_stichtag" ]]` will be `IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN SET v_stichtag = v_sysdate; END IF;`.
*   **System Date Derivation:**
    *   `DWDate_Gib_Zeitraum 1 'D' 'DDMMYYYY' v_sysdate dummy` will be replaced by BigQuery's `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Parameter Validation:**
    *   `pruefeParameterGesetzt Stichtag p_stichtag` and subsequent error handling will be implemented using BigQuery `IF` statements and `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ...` for error propagation.
*   **Logging and Error Handling:**
    *   The `DWMSG_*` framework functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung`) will be replaced by `INSERT` statements into the `project.dataset.job_log` BigQuery table.
    *   Shell `trap` commands will be handled by BigQuery Stored Procedure's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block.
    *   The `set -e` behavior (abort on command failure) is inherent in BigQuery's procedural language within a `BEGIN ... END` block, where any unhandled error will terminate the block.
*   **External Script Invocation:**
    *   `${Name_Kernskript} ...` will be translated to a `CALL project.dataset.k_ausd_bp_ta_bpr_basis(...)` statement within the wrapper stored procedure.
*   **No Direct Data Transformation:** This wrapper script performs no direct data extraction, transformation, or aggregation. All such logic is delegated to the `k_ausd_bp_ta_bpr_basis.ksh` (which will be `k_ausd_bp_ta_bpr_basis` BigQuery Stored Procedure).

## 6. External Dependencies
The original script has no direct external system dependencies (e.g., Oracle, SFTP, S3) based on the lineage analysis. However, it does have internal dependencies on sourced shell scripts and the invoked kernel script.

*   **Sourced Helper Scripts:**
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    These will be re-implemented as part of the BigQuery Stored Procedure (e.g., as procedural logic, BigQuery UDFs, or by replacing their functionality with native BigQuery functions).
*   **Kernel Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh`
    This is the most critical dependency. Its functionality must be separately migrated into a BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_basis`) which will be called by this wrapper.

## 7. Unresolved / Risks
*   **Missing Analysis Data:** `file_purpose`, `complexity_signals`, `tier`, and `migration_flags` were not available from the database. This suggests a potential gap in the initial analysis which could hide unforeseen complexities. The `semi_auto` migration bucket (B2) confirms that manual effort will be required.
*   **Kernel Script Migration:** The core business logic resides in `k_ausd_bp_ta_bpr_basis.ksh`. The successful migration of this wrapper heavily depends on the successful, independent migration of that kernel script into a BigQuery Stored Procedure. Any complex file system operations or shell-specific external calls within the kernel script would need careful redesign, potentially involving external orchestration via Cloud Composer or Python.
*   **Framework-specific functions:** `DWMSG_*` and `DWDate_Gib_Zeitraum` are framework-specific. While BigQuery provides equivalents for date functions, the logging and error messaging framework will require a custom implementation using BigQuery tables and procedural logic. The exact semantics of these functions must be understood for accurate replication.
*   **Exact Log File Semantics:** The script writes to a log file dynamically. While an audit table in BigQuery can capture structured logs, if byte-for-byte fidelity of the original log file is required, Cloud Logging or GCS integration might be necessary.
*   **Global Job Numbering:** If `DW_EintragsNr` needs to be a globally unique and sequential job number across multiple jobs, a more robust mechanism than `SELECT IFNULL(MAX(job_entry_nr), 0) + 1` from a log table would be needed, potentially a sequence table or an atomic counter managed externally.

## 8. Build Plan

The migration will involve the following steps:

1.  **Define Logging Table:** Create the `project.dataset.job_log` BigQuery table to store job execution logs, status, and error details.
    *   **Language:** BigQuery DDL
2.  **Migrate Kernel Script:** Develop and deploy the BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_basis.ksh`. This is a prerequisite.
    *   **Language:** BigQuery SQL (Stored Procedure)
3.  **Develop Wrapper Stored Procedure:** Create the `project.dataset.ausd_bp_ta_bpr_basis_wrapper` BigQuery Stored Procedure.
    *   **Input Parameters:** `p_stichtag STRING`, `p_wiederanlaufWert INT64`.
    *   **Logic:**
        *   Implement parameter defaulting and validation.
        *   Derive system date using `FORMAT_DATE(CURRENT_DATE())`.
        *   Insert job start entry into `project.dataset.job_log`.
        *   Call `project.dataset.k_ausd_bp_ta_bpr_basis` with the derived parameters.
        *   Implement `EXCEPTION WHEN ERROR` block to log failures and `SIGNAL` errors.
        *   Insert success entry into `project.dataset.job_log` upon successful completion.
    *   **Language:** BigQuery SQL (Stored Procedure)
4.  **Automate Scheduling:** Configure a scheduled query in BigQuery or create a Cloud Composer DAG to invoke the `project.dataset.ausd_bp_ta_bpr_basis_wrapper` stored procedure with desired frequency and parameters.
    *   **Language:** BigQuery Scheduled Query definition or Python (for Cloud Composer DAG)
5.  **Configuration Management:** If necessary, establish tables or external mechanisms for managing environment variables or configuration values previously sourced from files like `$HOME/.dw_init`.
    *   **Language:** BigQuery DDL/DML for configuration tables, or YAML/JSON for external configuration.