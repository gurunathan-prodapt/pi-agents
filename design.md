# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_period.ksh`. This script serves as a wrapper or driver for a contract data reconciliation job specifically for the `ta_period` table. Its primary functions include initializing the runtime environment, validating command-line parameters, setting up a custom logging and error-handling framework, and invoking a core script (`k_ausd_v_ta_period.ksh`) that contains the main business logic. The job is assembled from a single component, this script, and has a medium complexity stage distribution.

## 2. Source Inventory
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh`
*   **Technology:** KornShell (shell script)
*   **Tier:** medium
*   **Automation Bucket:** semi_auto
*   **Description:** This script orchestrates the execution of the `ta_period` contract data reconciliation. It handles environment setup, parameter parsing, robust logging and error management via a custom `DWMSG_` framework, and delegates the core reconciliation tasks to an invoked script.

## 3. Target Architecture
The target platform is Google Cloud's BigQuery. The migration will leverage BigQuery's capabilities for data processing and orchestration.
*   **Core Logic:** The logic currently residing in `k_ausd_v_ta_period.ksh` (and potentially other sourced scripts) will be migrated to BigQuery SQL Stored Procedures.
*   **Orchestration:** The wrapper logic from `r_ausd_v_ta_period.ksh` will be transformed into a BigQuery Stored Procedure (`sp_bert_v_ta_period`). For higher-level scheduling and external interactions, Google Cloud Composer (Apache Airflow), Cloud Workflows, or Cloud Run could be employed, especially for handling non-SQL native shell features like `trap` and external process execution.
*   **Logging and Monitoring:** The existing custom logging framework (`DWMSG_` functions) will be replaced by dedicated BigQuery logging and job control tables (e.g., `project.dataset.job_log`, `project.dataset.job_status`, `project.dataset.job_control`).
*   **Environment Initialization:** Environment variables and sourced utility scripts will be replaced by explicit parameter passing, BigQuery dataset/project configurations, or helper stored procedures.

## 4. Data Flow & Lineage
The `r_ausd_v_ta_period.ksh` script acts primarily as a control-flow orchestrator rather than a direct data transformer.
*   **Inputs:**
    *   Command-line parameters (`-h`, `-s`, `-l` as inferred from `getopts` definition).
    *   Environment configuration from `$HOME/.dw_init`.
    *   Utility functions from `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`.
    *   System date (`date +%d%m%Y`).
*   **Processes:**
    1.  Initialize environment and error handling.
    2.  Parse input parameters.
    3.  If errors in parameters, log and exit.
    4.  Initialize custom logging mechanism (determine job entry number, log file name, create job entry, set stichtag info).
    5.  Set up shell `trap` for `INT` and `ERR` signals to invoke `DWMSG_Fehlerbehandlung`.
    6.  Execute the core script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh` with parameters `-j $JobKennung -f ${DW_EintragsNr}`.
    7.  Direct output from the core script to the log file.
    8.  On successful completion, log success message and set job status.
*   **Outputs:**
    *   Console output (usage, job header, success message).
    *   Log file (dynamically named, capturing job status, messages, and output from the core script).
    *   Exit codes (0 for success, 192/193 for parameter errors, 1 for trapped errors).
*   **Lineage Gaps:** No direct `READS` or `WRITES` edges for `r_ausd_v_ta_period.ksh` were found in the `lineage_edges` table. The data flow within `k_ausd_v_ta_period.ksh` (which is invoked by this script) is currently not defined in the provided lineage and would need to be analyzed separately.

## 5. Transformation Logic
The `r_ausd_v_ta_period.ksh` script does **not** contain any direct data transformation logic. Its role is purely orchestration and error handling. All actual data reconciliation or transformation is delegated to the `k_ausd_v_ta_period.ksh` core script. The migration of this wrapper script will focus on replicating its control flow, parameter handling, logging, and invocation patterns within the BigQuery ecosystem.

## 6. External Dependencies
The script has the following external dependencies:
*   **Shell Utilities:** Standard KornShell (`ksh`) environment, `getopts`, `date`, `print`, `tee`, `trap`.
*   **Initialization Files:** `$HOME/.dw_init` for environment setup.
*   **Custom Framework Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter handling)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities)
    *   These scripts contain functions like `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK`. These functions are central to the script's operational integrity and will need to be re-implemented in BigQuery Stored Procedures or a Python-based orchestration layer.
*   **Core Business Logic Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh`. This is the most critical dependency, as its content dictates the actual data processing. Its migration is essential and must be coordinated.

## 7. Unresolved / Risks
*   **`k_ausd_v_ta_period.ksh` Content:** The details of the core script `k_ausd_v_ta_period.ksh` are unknown. Its migration design is a critical next step and will determine the full end-to-end data processing in BigQuery.
*   **`DWMSG_` Framework:** The exact implementations of the `DWMSG_` functions are not available. Their logic will need to be reverse-engineered or re-specified to create equivalent BigQuery logging and control procedures/tables.
*   **Shell-Specific Features:** Direct translation of shell `trap` commands and the `.` (source) command into pure BigQuery SQL is not feasible. These will require either BigQuery Stored Procedure `EXCEPTION` blocks, or more robust orchestration via Cloud Composer/Workflows/Cloud Run to mimic the error handling and environment sourcing.
*   **Parameter `s:` and `l:`:** While declared in `getopts`, their usage is not evident in the provided wrapper script. This implies they might be passed to the core script or used in other sourced utilities. Their ultimate purpose needs clarification for complete migration.
*   **Lineage Completeness:** The absence of direct `lineage_edges` for this specific script as a source or target node within the provided metadata suggests that the automated lineage discovery might not fully capture the dependencies of orchestrator scripts or their invoked sub-scripts. This could pose a risk for understanding complete data flow if not addressed by manual analysis.

## 8. Build Plan
The migration build plan will consist of the following steps:

1.  **BigQuery Logging and Control Tables DDL:**
    *   Create DDL for `project.dataset.job_log` table (columns: `job_name`, `job_entry_nr`, `log_level`, `error_nr`, `error_arg`, `message`, `log_file_name`, `business_date`, `created_at`, `updated_at`).
    *   Create DDL for `project.dataset.job_status` table (columns: `job_name`, `job_entry_nr`, `status`, `updated_at`).
    *   Create DDL for `project.dataset.job_control` table (columns: `job_name`, `job_entry_nr`, `stichtag`, `stichtag_format`, `created_at`).

2.  **BigQuery Stored Procedure for Wrapper Logic (`sp_bert_v_ta_period`):**
    *   Develop a BigQuery Stored Procedure `project.dataset.sp_bert_v_ta_period` in BQSQL.
    *   This procedure will handle parameter parsing, job initialization, logging to the new BigQuery tables, and error handling via `EXCEPTION` blocks.
    *   It will include a `CALL` statement to invoke the migrated core script, `project.dataset.sp_k_ausd_v_ta_period`.

3.  **Migration of Core Script (`k_ausd_v_ta_period.ksh`):**
    *   Analyze `k_ausd_v_ta_period.ksh` to identify its data transformation logic, sources, and targets.
    *   Design and develop a corresponding BigQuery Stored Procedure, `project.dataset.sp_k_ausd_v_ta_period`, or a series of BQSQL scripts/views if the logic is primarily data transformation.

4.  **Migration of Utility Scripts:**
    *   Re-implement the logic of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` as helper functions within BigQuery Stored Procedures or as separate small BQSQL functions/procedures, integrating them into the logging and parameter handling in `sp_bert_v_ta_period` and `sp_k_ausd_v_ta_period`.
    *   The `$HOME/.dw_init` environment setup will be replaced by explicit BigQuery project/dataset configurations or runtime parameters.

5.  **Orchestration Layer (if needed):**
    *   If complex external processes or advanced `trap`-like error handling beyond BigQuery's native `EXCEPTION` block capabilities are required, develop a Cloud Composer DAG (Python) or Cloud Workflow (YAML) to orchestrate the calls to `sp_bert_v_ta_period`.

6.  **Configuration Files:**
    *   `bq_project_config.json` for BigQuery project/dataset settings.
    *   `workflow_orchestration.yaml` (or similar) for Cloud Workflows or a Python DAG for Cloud Composer.

The build plan emphasizes a phased approach, starting with the supporting infrastructure (logging tables) then the orchestrating stored procedure, followed by the core business logic.