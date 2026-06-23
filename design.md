# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

## 1. Purpose & Scope
This job, `r_ausd_bp_ta_bpr_evn.ksh`, is an orchestration shell script. Its primary purpose is the initial provisioning of selected base products for the BERT system. It creates a cutoff-date extraction of contract cache data from the Data Warehouse (DWH) and makes it available for Forderungsscoring (FOS). The script handles parameter parsing, environment setup, error trapping, logging, and then invokes a core business logic script, `k_ausd_bp_ta_bpr_evn.ksh`, to perform the actual data processing. It also manages conditional deletion of existing data in the target table based on whether an active contract cache exists and if the data has been collected by the FOS loader.

## 2. Source Inventory
The job is primarily composed of a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh`
    *   **Technology:** KornShell
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Complexity Tier:** Not available from `file_complexity` table.
    *   **Automation Bucket:** semi_auto (B2)
    *   **Summary:** This script acts as a wrapper for a core data processing script. It handles command-line arguments, sets up the execution environment, incorporates an error handling and logging framework, and orchestrates the execution of the main data preparation logic.

## 3. Target Architecture
The target platform is BigQuery. The migration will involve:
*   A BigQuery Stored Procedure to encapsulate the orchestration logic of `r_ausd_bp_ta_bpr_evn.ksh`.
*   Another BigQuery Stored Procedure to house the core data processing logic expected in `k_ausd_bp_ta_bpr_evn.ksh`.
*   BigQuery tables for source data (e.g., `contract_cache_source`), a target table for FOS provisioning (e.g., `fos_target_table`), and an audit/log table (e.g., `job_audit_log`).
*   External orchestration (e.g., Cloud Composer, Cloud Workflows) to schedule and execute the BigQuery Stored Procedures.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_evn.ksh` script itself doesn't directly read or write to database tables. It performs preparatory steps and then invokes a "core script" (`k_ausd_bp_ta_bpr_evn.ksh`), which is assumed to handle the actual data interactions.

**Current Flow (Legacy):**
1.  **Start:** `r_ausd_bp_ta_bpr_evn.ksh` execution.
2.  **Environment Setup:** Sources `$HOME/.dw_init` and helper scripts for error handling (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) and date utilities (`h_alis_date.ksh`).
3.  **Parameter Processing:** Parses command-line arguments (`-s` for Stichtag, `-l` for Wiederanlaufwert). Initializes `p_wiederanlaufWert` to 0 if not set. Determines `p_stichtag` (cutoff date) by defaulting to system date if not provided.
4.  **Error Handling Initialization:** Sets up logging mechanisms and `trap` commands for signal handling.
5.  **Core Script Invocation:** Executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_evn.ksh` passing the parsed parameters (`-j`, `-s`, `-f`, `-l`).
6.  **Logging & Exit:** Records job status (start, success) to a log file and exits.

**Target Flow (BigQuery):**
1.  **Orchestration Trigger:** An external scheduler (e.g., Cloud Composer) triggers the main BigQuery Stored Procedure, `ausd_bp_ta_bpr_evn_wrapper`.
2.  **Wrapper Procedure (`ausd_bp_ta_bpr_evn_wrapper`):**
    *   Parses and validates input parameters (`p_stichtag`, `p_wiederanlaufWert`).
    *   Determines current date and default cutoff date logic.
    *   Logs job start and parameters into `project.dataset.job_audit_log`.
    *   Calls the core data processing procedure, `project.dataset.ausd_bp_ta_bpr_evn_core`, passing relevant parameters.
    *   Logs job success into `project.dataset.job_audit_log`.
    *   Handles exceptions and logs errors to `project.dataset.job_audit_log`.
3.  **Core Processing Procedure (`ausd_bp_ta_bpr_evn_core`):**
    *   (Placeholder for logic from `k_ausd_bp_ta_bpr_evn.ksh`)
    *   If `p_wiederanlaufWert` > 0, `DELETE` from `project.dataset.fos_target_table` for `DWH_VERTRAG_ID >= p_wiederanlaufWert`.
    *   `INSERT` data from `project.dataset.contract_cache_source` into `project.dataset.fos_target_table` based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID` criteria.
    *   Includes logic to delete the target table if no active cache records are found.

## 5. Transformation Logic
The transformation logic for `r_ausd_bp_ta_bpr_evn.ksh` primarily involves porting shell script constructs and patterns to BigQuery SQL Stored Procedures.

*   **Parameter Handling:** Shell `getopts` for command-line arguments will be mapped to `IN` parameters of the BigQuery Stored Procedure. Default value assignments (`if [[ -z ... ]]`) will be handled using `IFNULL` or `COALESCE` in BigQuery SQL.
*   **Date Calculation:** Shell date utilities (`DWDate_Gib_Zeitraum`) will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()`, `PARSE_DATE()` functions.
*   **Error Handling and Logging:**
    *   The custom shell `DWMSG_*` functions will be replaced by `INSERT` statements into a dedicated `job_audit_log` BigQuery table.
    *   Shell `set -e` and `trap` mechanisms will be mapped to BigQuery's `EXCEPTION WHEN ERROR THEN ... END` blocks and `SIGNAL SQLSTATE` for explicit error raising.
*   **External Script Invocation:** The call to `k_ausd_bp_ta_bpr_evn.ksh` will be replaced by a `CALL` to a separate BigQuery Stored Procedure, `ausd_bp_ta_bpr_evn_core`.
*   **Core Data Logic:** The actual `SELECT`, `INSERT`, `DELETE` statements (currently within `k_ausd_bp_ta_bpr_evn.ksh`) will be directly translated into BigQuery SQL within the `ausd_bp_ta_bpr_evn_core` procedure. This includes the date-based filtering (`Gueltig_von <= Stichtag < Gueltig_bis` and `LADEDATUM < Stichtag`) and `DWH_VERTRAG_ID` filtering for the restart mechanism. The conditional `DELETE` based on active cache existence will also be replicated in SQL.

## 6. External Dependencies
The current script has the following external dependencies:

*   **Environment Initialization:** `$HOME/.dw_init` and various helper scripts under `${BERT_DIR_ROOT}`.
    *   **Target Replacement:** These will be replaced by explicit parameter passing, configuration tables in BigQuery, or environment variables managed by the orchestration layer (e.g., Cloud Composer variables).
*   **Core Script:** `k_ausd_bp_ta_bpr_evn.ksh`.
    *   **Target Replacement:** This will be migrated into a dedicated BigQuery Stored Procedure (`ausd_bp_ta_bpr_evn_core`) that will be called by the main wrapper procedure.
*   **Implicit Databases:** The script interacts with an underlying DWH for contract cache and a target system for Forderungsscoring (FOS).
    *   **Target Replacement:** These will be mapped to specific BigQuery datasets and tables (e.g., `project.dataset.contract_cache_source`, `project.dataset.fos_target_table`).

No explicit external systems like Oracle, SFTP, or S3 were identified from `lineage_assembled_jobs`.

## 7. Unresolved / Risks
*   **Content of `k_ausd_bp_ta_bpr_evn.ksh`:** The design assumes that `k_ausd_bp_ta_bpr_evn.ksh` primarily contains SQL-like data manipulation logic that can be directly translated to BigQuery SQL. If it contains complex shell scripting, file system operations, or calls to other external programs, these parts would require more elaborate migration strategies (e.g., Cloud Functions, Dataflow, custom Python scripts).
*   **`file_complexity` Data:** The `file_complexity` table returned no rows for this file, meaning no specific complexity tier or migration flags were available. This could imply a lack of detailed static analysis for this particular file, potentially masking unforeseen complexities.
*   **Helper Script Logic:** The exact content and side effects of helper scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` were not analyzed in detail beyond their stated purpose. Any non-standard logic within these would need careful review during implementation.
*   **"AL??" Comments:** The source code contains commented-out lines like `#AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum`. It's unclear what "AL??" signifies and if this functionality was ever active or is needed in the target. This should be clarified with the business owner.

## 8. Build Plan
1.  **Define BigQuery Schemas:**
    *   `project.dataset.job_audit_log` table to store logging information (job_name, job_entry_nr, error_nr, error_arg, log_ts, message, stichtag, sysdate_ddmmyyyy, restart_value, status).
    *   Define schemas for `project.dataset.contract_cache_source` (source data) and `project.dataset.fos_target_table` (target data) based on legacy DWH schema.
2.  **Develop `ausd_bp_ta_bpr_evn_core` BigQuery Stored Procedure (SQL):**
    *   Implement the data extraction, filtering, and loading logic based on the requirements described for `k_ausd_bp_ta_bpr_evn.ksh`, including conditional deletes and inserts.
3.  **Develop `ausd_bp_ta_bpr_evn_wrapper` BigQuery Stored Procedure (SQL):**
    *   Implement parameter parsing, validation, and defaulting logic.
    *   Integrate logging to `job_audit_log`.
    *   Implement exception handling.
    *   Include the `CALL` to `ausd_bp_ta_bpr_evn_core`.
4.  **Create Orchestration Layer (e.g., Cloud Composer DAG - Python):**
    *   Develop a Cloud Composer DAG that calls the `ausd_bp_ta_bpr_evn_wrapper` BigQuery Stored Procedure, passing required parameters.
    *   Configure scheduling for the DAG.
5.  **Testing:**
    *   Unit tests for both BigQuery Stored Procedures.
    *   Integration tests for the entire workflow, including the orchestration layer, using sample data.
    *   Data validation to ensure data consistency and correctness between source and target.