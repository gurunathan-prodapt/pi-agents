# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

## 1. Purpose & Scope

This KornShell script (`r_ausd_rechempf.ksh`) serves as an orchestration layer for the initial provisioning (snapshot) of the "Vertrags-Cache" (contract cache) for the "Forderungsscoring" (FOS) system. Its primary purpose is to manage parameters, set up the execution environment, handle logging and error trapping, and then delegate the core data processing to a separate kernel script. The job ensures a date-based extraction and can support restart capabilities based on a contract ID.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh`
    *   **Technology:** KornShell script
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Orchestrates the contract cache provisioning, handles parameters, logging, error trapping, and calls a core script for actual data processing.
    *   **Dependencies (sourced helper scripts):**
        *   `$HOME/.dw_init` (environment initialization)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing utilities)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling utilities)
    *   **Core Processing Script (invoked):**
        *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_rechempf.ksh`

## 3. Target Architecture

The migration targets Google Cloud's BigQuery platform. The current KornShell orchestrator will be re-implemented as a BigQuery Stored Procedure.

*   **Main Orchestration:** A BigQuery Stored Procedure (`sp_initial_befuellung_vertrags_cache_fos`) will replace the `r_ausd_rechempf.ksh` script. This procedure will manage input parameters, define variables, handle conditional logic, and invoke the migrated core processing logic.
*   **Logging and Auditing:** File-based logging will be replaced by dedicated BigQuery audit/log tables (`project.dataset.job_log`, `project.dataset.job_error_log`, `project.dataset.job_log_messages`).
*   **Helper Functions:** Shell helper scripts (e.g., for date handling, parameter validation) will be translated into BigQuery User-Defined Functions (UDFs) or inline SQL logic within the stored procedure, or potentially into separate, smaller BigQuery Stored Procedures if they encapsulate complex reusable logic.
*   **Core Data Processing:** The functionality of the invoked kernel script (`k_ausd_rechempf.ksh`) will need to be translated into a separate BigQuery Stored Procedure (`sp_k_ausd_rechempf`) or a series of SQL statements/views, called by the main orchestration procedure.
*   **External Orchestration (Optional):** For complex dependencies or scheduling, Google Cloud Composer (Apache Airflow) or Cloud Workflows could be used to trigger the BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The migrated job will operate as follows:

1.  **Execution Trigger:** The BigQuery Stored Procedure (`sp_initial_befuellung_vertrags_cache_fos`) is invoked, typically on a schedule, with optional `p_stichtag` (cutoff date) and `p_wiederanlaufWert` (restart value) parameters.
2.  **Parameter Handling:** The procedure parses and validates the input parameters. If `p_wiederanlaufWert` is not provided, it defaults to `0`. If `p_stichtag` is not provided, it defaults to the current system date.
3.  **Logging Initialization:** An entry is created in the BigQuery `job_log` table, recording the job's start time and metadata.
4.  **Core Processing Invocation:** The main orchestration procedure calls the BigQuery Stored Procedure that encapsulates the logic of `k_ausd_rechempf.ksh` (e.g., `sp_k_ausd_rechempf`), passing the relevant parameters (`job_kennung`, `stichtag`, `eintragsnr`, `wiederanlaufWert`).
5.  **Error Handling:** Any errors during the execution of the core processing procedure are caught, logged to the `job_error_log` table, and re-raised to indicate failure.
6.  **Success/Completion:** If the core processing completes successfully, a success message is logged, and the `job_log` entry is updated with a `status = 'OK'` and completion timestamp.

## 5. Transformation Logic

The current script is primarily an orchestrator and performs limited data transformation. The transformation logic will focus on converting shell scripting constructs into BigQuery SQL and procedural logic.

*   **Parameter Processing:**
    *   Shell `getopts` will be replaced by stored procedure input parameters.
    *   Conditional logic (`if [[ -z ... ]]`, `if [ ! $ErrNr -eq 0 ]`) for default values and validation will be translated to `IF...THEN...END IF;` statements in BigQuery.
    *   `p_wiederanlaufWert` default: `IFNULL(p_wiederanlaufWert, 0)`
    *   `p_stichtag` default: `IFNULL(p_stichtag, v_sysdate)`
*   **Date Handling:** The `DWDate_Gib_Zeitraum` and other date utilities will be replaced by BigQuery's built-in date functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE`).
*   **Logging:** Shell `print` and `tee -a` commands will be replaced by `INSERT` statements into dedicated BigQuery logging tables (`job_log`, `job_log_messages`, `job_error_log`).
*   **Error Handling:** Shell `set -e` and `trap` commands will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END;` blocks and `RAISE` statements.
*   **Script Invocation:** The shell execution of `k_ausd_rechempf.ksh` will be replaced by a `CALL` statement to its corresponding BigQuery Stored Procedure.

**Note:** The actual data extraction, transformation, and loading logic is external to this script (residing in `k_ausd_rechempf.ksh`) and requires separate analysis and migration into BigQuery SQL.

## 6. External Dependencies

Based on the provided information, the assembled job itself does not directly declare external system dependencies (e.g., Oracle, SFTP, S3) in `external_systems`. However, the KornShell script utilizes several internal helper scripts, which need to be addressed:

*   **Internal Script Dependencies:**
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    These will be re-implemented either as BigQuery UDFs, inline logic within the stored procedure, or as standalone BigQuery Stored Procedures, depending on their complexity and reusability.
*   **Core Processing Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_rechempf.ksh`
    This script is the most significant dependency. Its functionality will need to be fully migrated to BigQuery, likely as a separate stored procedure (`sp_k_ausd_rechempf`), and any data sources or targets it interacts with (e.g., DWH tables mentioned in comments) must be identified and mapped to BigQuery datasets and tables.

## 7. Unresolved / Risks

*   **`k_ausd_rechempf.ksh` Logic:** The core data processing logic of the `k_ausd_rechempf.ksh` script is currently unresolved. Its content must be analyzed to understand the data sources, transformations, and targets, which are crucial for a complete BigQuery migration. This is the highest risk component.
*   **Legacy Data Sources:** The original "Vertrags-Cache" source system and any DWH tables referenced by `k_ausd_rechempf.ksh` are not fully detailed. These upstream data sources need to be identified and migrated or integrated with BigQuery.
*   **Dynamic Path Resolution:** The script uses `BERT_DIR_ROOT` which is an environment variable. Ensuring correct path resolution for sourced scripts and the invoked kernel script in the BigQuery environment or external orchestration layer is vital.
*   **Historical `MIN(sysdate,maxladedatum)` Logic:** The commented-out logic for deriving `p_stichtag` from `MIN(sysdate, maxladedatum)` (from `FOSHoleLadedatum "DWH\\$TA_C_VERTRAG" v_ladedatum`) suggests a potential requirement for a cutoff date based on the maximum load date of a source table. If this logic was intended but not currently active, it might need to be implemented in BigQuery if it's a new business requirement.

## 8. Build Plan

The build plan outlines the steps to migrate `r_ausd_rechempf.ksh` to BigQuery. This plan assumes that the `k_ausd_rechempf.ksh` script will also be migrated to BigQuery as `sp_k_ausd_rechempf`.

1.  **DDL for Logging and Audit Tables (BigQuery SQL)**
    *   Create `project.dataset.job_log` table: to store job execution metadata (start/end times, status, parameters).
    *   Create `project.dataset.job_error_log` table: to log detailed error information.
    *   Create `project.dataset.job_log_messages` table: for general informational and success messages.

2.  **Helper Function Migration (BigQuery SQL)**
    *   Analyze `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, and other sourced scripts.
    *   Migrate relevant functions to BigQuery UDFs or small Stored Procedures as needed.
    *   Re-implement environment variable lookups (e.g., `BERT_DIR_ROOT`) as BigQuery constants or configuration parameters within the stored procedure.

3.  **`k_ausd_rechempf.ksh` Migration (BigQuery SQL - `sp_k_ausd_rechempf`)**
    *   **CRITICAL STEP:** Analyze the actual `k_ausd_rechempf.ksh` script to understand its data sources, transformation logic (SQL queries, data manipulation), and target tables.
    *   Translate its functionality into one or more BigQuery Stored Procedures or a series of SQL scripts/views. This will involve converting any database-specific SQL (e.g., Oracle) to BigQuery SQL, re-creating tables, and implementing data pipelines.

4.  **Main Orchestration Stored Procedure (BigQuery SQL - `sp_initial_befuellung_vertrags_cache_fos`)**
    *   Translate the `r_ausd_rechempf.ksh` script into the main BigQuery Stored Procedure:
        *   Define input parameters for `p_stichtag` and `p_wiederanlaufWert`.
        *   Implement parameter defaulting and validation logic.
        *   Integrate logging into the `job_log` and `job_log_messages` tables.
        *   Implement error handling using `BEGIN...EXCEPTION...END` blocks and `RAISE` statements, logging to `job_error_log`.
        *   Include the `CALL` statement to invoke the migrated `sp_k_ausd_rechempf`.

5.  **Testing and Validation**
    *   Thoroughly test each migrated component.
    *   Perform end-to-end testing of the `sp_initial_befuellung_vertrags_cache_fos` with various parameter combinations.
    *   Validate data integrity and accuracy in the target BigQuery tables against the legacy system.

6.  **Scheduling (Google Cloud Composer/Workflows)**
    *   If external scheduling is required, create a Cloud Composer DAG or Cloud Workflow to schedule and trigger the `sp_initial_befuellung_vertrags_cache_fos` stored procedure.