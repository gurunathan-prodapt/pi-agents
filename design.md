# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_bp_ref.ksh`, which acts as a wrapper/orchestration script for a contract data reconciliation job. Its primary purpose is to prepare the runtime environment, validate command-line parameters, initialize logging and error handling, and then invoke a core script, `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh`, for the actual data processing related to the `ta_bp_ref` table. The job was assembled from a single component file.

## 2. Source Inventory
The job consists of one primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh`
    *   **Technology:** KornShell (Ksh)
    *   **Category:** Shell
    *   **Purpose:** ETL (Orchestration/Wrapper)
    *   **Complexity Tier:** Medium
    *   **Migration Flags:** None
    *   **Automation Bucket:** Semi-Auto

## 3. Target Architecture
The target platform is Google BigQuery. The wrapper script will be migrated to a BigQuery Stored Procedure.

*   **BigQuery Stored Procedure:** A stored procedure named `project.dataset.vertragsdatenabgleich_wrapper` will encapsulate the logic of the original KornShell script. This procedure will accept parameters equivalent to the original script's command-line arguments.
*   **Audit/Logging Table:** A dedicated BigQuery table, `project.dataset.job_audit_log`, will be created to record job execution details, status, errors, and logging information, replacing the file-based logging of the original script.
*   **Configuration Table (Optional):** An optional configuration table could store static values like `ProgName`, `ProgVersion`, and core procedure names, if not hardcoded or passed as parameters.
*   **Core Logic Stored Procedure:** The core script `k_ausd_v_ta_bp_ref.ksh` (which is not part of this specific migration scope but is invoked by the wrapper) is expected to be migrated into a separate BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_bp_ref`).

## 4. Data Flow & Lineage
The `r_ausd_v_ta_bp_ref.ksh` script primarily orchestrates the execution of another script.

*   **Execution Flow:**
    1.  The `vertragsdatenabgleich_wrapper` BigQuery Stored Procedure is invoked with optional parameters (`p_s`, `p_l`).
    2.  It initializes environment variables and logging parameters (simulated by BigQuery `DECLARE` statements and audit table inserts).
    3.  It performs parameter validation. If validation fails, it logs the error to `job_audit_log` and exits.
    4.  It logs the job start and metadata to `job_audit_log`.
    5.  It calls the `project.dataset.k_ausd_v_ta_bp_ref` BigQuery Stored Procedure (placeholder for the core script's logic).
    6.  Upon successful completion of the core procedure, it logs a success message and updates the job status in `job_audit_log`.
    7.  In case of an error during the core procedure execution, an exception handler catches the error, logs it to `job_audit_log`, and raises the error.

*   **Lineage:**
    *   This specific wrapper script does not directly read from or write to data sources/targets. Its lineage is primarily defined by its invocation of the core script.
    *   No direct `INVOKES`, `READS`, `WRITES`, or `DEPENDS_ON` edges were found for this file in the `lineage_edges` table, reinforcing its role as an orchestrator rather than a direct data transformer.

## 5. Transformation Logic
The transformation focuses on mapping KornShell constructs and orchestration patterns to BigQuery SQL scripting capabilities.

*   **Environment Variables:** Shell variables like `ProgName`, `ProgVersion`, `JobKennung`, `v_sysdate`, `Name_Kernskript` will be translated to `DECLARE` variables within the BigQuery Stored Procedure or passed as input parameters.
*   **Parameter Parsing:** The `getopts` logic will be replaced by direct input parameters to the BigQuery Stored Procedure (`p_s`, `p_l`). Parameter validation will use BigQuery's conditional logic (`IF...THEN...END IF`).
*   **Error Handling:** The `set -eu` and `trap` mechanisms will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for robust error management and logging to the `job_audit_log` table.
*   **Logging:** The calls to `DWMSG_` functions and `tee -a $LogDatei` for file-based logging will be replaced by `INSERT` and `UPDATE` statements against the `job_audit_log` table.
*   **Script Invocation:** The execution of `${Name_Kernskript}` will be replaced by a `CALL` statement to the corresponding BigQuery Stored Procedure that houses the migrated core logic.
*   **Date Formatting:** `date +%d%m%Y` will be converted to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **`usage` Function:** This will be replicated as a conditional `SELECT` statement returning usage information if a specific help parameter is passed.

## 6. External Dependencies
The original script has several implicit external dependencies:

*   **Sourced Environment Files:**
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   **Replacement:** These will be eliminated. Configuration parameters will be passed directly to the BigQuery stored procedure or managed via configuration tables. Utility functions will be reimplemented within BigQuery UDFs or the stored procedure itself, or replaced by native BigQuery functions.
*   **Core Processing Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh`
    *   **Replacement:** This script will be migrated into its own BigQuery Stored Procedure, which will then be called by the `vertragsdatenabgleich_wrapper` procedure.

No other external systems (like Oracle, SFTP, S3) were identified in `lineage_external_systems`.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_v_ta_bp_ref.ksh`) Logic:** The migration design for this wrapper assumes that the core script, which performs the actual data reconciliation, will also be migrated to BigQuery. The current design includes a placeholder `CALL` to this future BigQuery stored procedure. The complexity and specific logic within `k_ausd_v_ta_bp_ref.ksh` are not analyzed in this document and represent a dependency for full job functionality.
*   **Unsupported Shell Features:** While the wrapper script itself is largely orchestrational, if the core script or any sourced utilities contain complex shell-specific operations (e.g., advanced filesystem manipulation, external system calls not manageable by BigQuery), these will require further analysis.
    *   **Mitigation:** For such unsupported logic, Cloud Functions, Cloud Run, or Dataflow (using Python) could be employed to encapsulate and execute these parts, with results persisted back to BigQuery.
*   **Environmental Variables:** The `BERT_DIR_ROOT` environment variable and its resolution are critical for the original script's operation. Its value needs to be explicitly managed in the BigQuery environment, likely as a parameter or configuration lookup.

## 8. Build Plan
The migration will involve the following steps:

1.  **Create `job_audit_log` Table (BQSQL DDL):** Define the schema for the logging table to capture job execution details.
2.  **Migrate `r_ausd_v_ta_bp_ref.ksh` to `vertragsdatenabgleich_wrapper` Stored Procedure (BQSQL):**
    *   Translate shell variables to `DECLARE` statements or procedure parameters.
    *   Convert parameter parsing (`getopts`) to BigQuery procedure parameters and conditional logic.
    *   Replace shell-specific error handling (`trap`) with BigQuery `EXCEPTION` blocks.
    *   Rewrite logging (`DWMSG_` functions, `tee`) to insert/update `job_audit_log`.
    *   Include a placeholder `CALL` to the future `k_ausd_v_ta_bp_ref` BigQuery Stored Procedure.
3.  **Migrate `k_ausd_v_ta_bp_ref.ksh` (BQSQL or other as determined by its content):** This is a dependent task, not fully detailed here.
4.  **Integrate Orchestration:** If the core script requires non-SQL components, integrate these as separate Cloud Functions/Run/Dataflow jobs orchestrated by a BigQuery workflow or an external orchestrator (e.g., Cloud Composer/Airflow).