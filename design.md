# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `r_ausd_v_ta_cntrct_templ.ksh` to Google Cloud Platform, specifically BigQuery.

The primary purpose of this script is to act as a wrapper or orchestration layer for a contract data reconciliation process focusing on the `ta_cntrct_templ` table. It handles environment setup, parses command-line parameters, initializes job and error logging, and ultimately invokes a core processing script (`k_ausd_v_ta_cntrct_templ.ksh`). The script ensures proper error handling and logging throughout its execution.

The scope of this migration covers transforming the shell-based orchestration and its dependencies into equivalent BigQuery-native components and potentially leveraging other Google Cloud services for broader orchestration if necessary.

## 2. Source Inventory
The assembled job consists of one primary source file.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh`
    *   **Technology:** KornShell (KornShell)
    *   **Summary:** This KornShell script serves as a wrapper for the contract data reconciliation process for the `ta_cntrct_templ` table, handling environment setup, parameter parsing, and error logging before invoking a core processing script.
    *   **Complexity Tier:** Undetermined (information not available from `file_complexity`)
    *   **Migration Flags:** Undetermined (information not available from `file_complexity`)
    *   **Automation Bucket:** Semi-automatic (B2)

## 3. Target Architecture
The target architecture will leverage BigQuery for data processing and potentially Cloud Composer (for Airflow DAGs) or Cloud Workflows for orchestration.

*   **BigQuery Stored Procedures:** The wrapper script's orchestration logic will be converted into a BigQuery Stored Procedure, named `project.dataset.Vertragsdatenabgleich` (or similar). This procedure will handle parameter parsing, logging initialization, and the invocation of the core logic.
*   **BigQuery Tables for Logging/Auditing:** The custom logging and error handling functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_MeldeFehler`) will be replaced by direct inserts/updates into dedicated BigQuery audit and error log tables (e.g., `project.dataset.job_audit_log`, `project.dataset.job_error_log`, `project.dataset.job_reference_date`).
*   **Core Logic as BigQuery Stored Procedure:** The invoked core script, `k_ausd_v_ta_cntrct_templ.ksh`, is expected to contain the actual data reconciliation logic. This script will need to be analyzed and converted into a separate BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_cntrct_templ`) that can be called by the main orchestration procedure.
*   **External Orchestration (Optional):** If the overall workflow involves complex scheduling, cross-system dependencies, or external triggers beyond the scope of a single BigQuery stored procedure, Cloud Composer (Airflow) could be used to schedule and manage the execution of the BigQuery stored procedures.

## 4. Data Flow & Lineage
The current lineage information (from `lineage_edges`) did not explicitly capture the invocation or data flow for this specific `run_id`. However, based on the source code analysis and the generated design, the conceptual data flow will be as follows:

1.  **Invocation:** The `project.dataset.Vertragsdatenabgleich` BigQuery Stored Procedure is initiated, potentially via a scheduler (e.g., Cloud Composer, Cloud Scheduler) or manually.
2.  **Parameter Handling & Logging:** The procedure parses input parameters and records job start information, including a unique job entry number and log file details, into BigQuery audit tables.
3.  **Core Logic Invocation:** The main orchestration procedure `project.dataset.Vertragsdatenabgleich` calls the core logic stored procedure, `project.dataset.k_ausd_v_ta_cntrct_templ`, passing relevant job metadata.
4.  **Core Logic Execution:** The `k_ausd_v_ta_cntrct_templ` procedure performs the contract data reconciliation against the `ta_cntrct_templ` table (and potentially other related tables) within BigQuery. The specifics of data reads, transformations, and writes for this step are dependent on the analysis of `k_ausd_v_ta_cntrct_templ.ksh` itself.
5.  **Status Update & Error Handling:** Upon completion (success or failure) of the core logic, the `Vertragsdatenabgleich` procedure updates the job status in the BigQuery audit tables. In case of errors, error details are logged into BigQuery error tables.

## 5. Transformation Logic

The KornShell wrapper script's logic will be translated into a BigQuery Stored Procedure, `project.dataset.Vertragsdatenabgleich`.

*   **Parameter Handling:** `getopts` logic will be replaced by `IN` parameters in the BigQuery Stored Procedure. Error codes (192, 193) will be mapped to `SIGNAL SQLSTATE` messages.
*   **Environment Sourcing (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These external script dependencies will be eliminated. Configuration (if any) can be hardcoded within the BigQuery procedure or stored in configuration tables. Error handling (e.g., `DWMSG_MeldeFehler`) will be replaced by `INSERT` statements into BigQuery error logs. Date functions will use BigQuery's native `FORMAT_DATE` and `CURRENT_DATE()`.
*   **Logging (`DWMSG_*` functions):** All calls to custom `DWMSG_*` functions will be replaced by `INSERT` or `UPDATE` statements to dedicated BigQuery audit and error log tables.
    *   `DWMSG_ErmittleNr DW_EintragsNr` -> `SELECT IFNULL(MAX(eintrags_nr), 0) + 1 INTO DW_EintragsNr FROM job_audit_log`
    *   `DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr` -> `SET LogDatei = CONCAT(JobKennung, '_', CAST(DW_EintragsNr AS STRING), '.log')`
    *   `DWMSG_ErzeugeEintrag ...` -> `INSERT INTO job_audit_log (...)`
    *   `DWMSG_SetzeStichtagInfo ...` -> `INSERT INTO job_reference_date (...)`
    *   `DWMSG_Fehlerbehandlung ...` -> `INSERT INTO job_error_log (...)`
    *   `DWMSG_SetzeStatusOK ...` -> `UPDATE job_audit_log SET status = 'OK' ...`
*   **Core Script Execution (`${Name_Kernskript}`):** The invocation of `k_ausd_v_ta_cntrct_templ.ksh` will be replaced by a `CALL` statement to a corresponding BigQuery Stored Procedure: `CALL project.dataset.k_ausd_v_ta_cntrct_templ(JobKennung, DW_EintragsNr);`.
*   **Traps (`trap - INT ERR`):** Shell traps for error handling will be replaced by `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks within the BigQuery Stored Procedure to catch and handle SQL errors, logging them to the error tables.
*   **`tee -a $LogDatei`:** Direct file output to a log file via `tee` will be replaced by inserts into the BigQuery audit log tables.

## 6. External Dependencies
The current script itself does not have direct external system dependencies (e.g., Oracle, SFTP, S3) based on the `lineage_assembled_jobs` analysis. All dependencies identified are other local shell scripts or internal shell commands.

*   **Sourced KornShell scripts:**
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   **Replacement:** These scripts contain environment variables, utility functions, and error handling logic. Their functionality will be absorbed directly into the BigQuery Stored Procedure logic (for date handling and parameter parsing) or replaced by BigQuery audit/error logging tables. Global configuration will be managed via BigQuery configuration tables or environment variables in the orchestrator (e.g., Cloud Composer).
*   **Core Processing Script (`k_ausd_v_ta_cntrct_templ.ksh`):**
    *   **Replacement:** This script is a crucial dependency and will need its own dedicated migration design. It is anticipated to be converted into a BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_cntrct_templ`) which will be called by the wrapper procedure.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_v_ta_cntrct_templ.ksh`) Content:** The most significant unresolved item is the content and complexity of `k_ausd_v_ta_cntrct_templ.ksh`. This core script likely contains the business logic for data reconciliation and will require a separate, detailed analysis and migration design. Without its content, the full end-to-end data flow and transformation logic cannot be completely defined.
*   **Missing Complexity Data:** The `file_complexity` table returned no rows for `r_ausd_v_ta_cntrct_templ.ksh`, meaning its complexity tier and migration flags are unknown. This could indicate hidden complexities that were not automatically detected.
*   **Detailed `DWMSG_*` Functionality:** While a general approach to replace `DWMSG_*` with BigQuery logging tables has been outlined, the exact schema and detail level required for these tables depend on the full functionality of the `DWMSG_*` suite, which is not fully exposed in this wrapper script.
*   **`-s` and `-l` Parameters:** These parameters are declared in `getopts` but not explicitly used in the visible code. Their purpose in the legacy system (e.g., passed to sourced scripts, or to the core script) needs to be clarified to ensure full functional parity in the BigQuery solution. The current pseudocode includes them but without specific handling.
*   **Hollow Job Fast Path:** The fact that `lineage_edges` returned no rows implies that the automatic dependency detection was not fully effective for this job. This means that the complete ecosystem of files involved beyond the explicit `component_files` needs manual verification.

## 8. Build Plan
The build plan will proceed in two main phases: defining the logging infrastructure and migrating the wrapper script, followed by the migration of the core processing script.

1.  **Define BigQuery Logging & Audit Tables (SQL):**
    *   Create `project.dataset.job_audit_log`
    *   Create `project.dataset.job_error_log`
    *   Create `project.dataset.job_reference_date`
    *   Language: BigQuery SQL

2.  **Migrate `r_ausd_v_ta_cntrct_templ.ksh` to BigQuery Stored Procedure (SQL):**
    *   Develop `project.dataset.Vertragsdatenabgleich` BigQuery Stored Procedure.
    *   Implement parameter handling, call to core script, and logging/error handling logic using the newly created BigQuery tables.
    *   Language: BigQuery SQL

3.  **Analyze and Migrate `k_ausd_v_ta_cntrct_templ.ksh` (TBD - Separate Design Document):**
    *   This step requires a separate detailed analysis of the core script.
    *   Output will likely be `project.dataset.k_ausd_v_ta_cntrct_templ` BigQuery Stored Procedure.
    *   Language: BigQuery SQL (likely, but depends on core script content)

4.  **Develop Orchestration (Optional - Cloud Composer / Workflows):**
    *   If complex scheduling or external dependencies are identified, create an Airflow DAG or Cloud Workflow definition to orchestrate the execution of `project.dataset.Vertragsdatenabgleich`.
    *   Language: Python (for Airflow) or YAML/JSON (for Cloud Workflows)

5.  **Testing:**
    *   Unit tests for `project.dataset.Vertragsdatenabgleich`.
    *   Integration tests for the full workflow including `project.dataset.k_ausd_v_ta_cntrct_templ`.
    *   Data validation to ensure reconciliation logic matches legacy system.

This staged approach allows for addressing the orchestration layer first, while deferring the more complex data transformation logic to a subsequent, dedicated design effort.