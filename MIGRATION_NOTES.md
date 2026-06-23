# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_apn_carmen.ksh` from its legacy environment to Google BigQuery. The original script served as an orchestration layer, handling parameter parsing, date validation, date calculations, execution of a core SQL script (`d_ausd_bp_ta_apn_carmen.sql`), record counting, and job logging.

The migration re-implements this orchestration logic and its underlying data processing using BigQuery Stored Procedures and BigQuery SQL. The main orchestration is now handled by `r_ausd_bp_ta_apn_carmen` BigQuery Stored Procedure, which calls a separate BigQuery Stored Procedure `d_ausd_bp_ta_apn_carmen` for the core data transformation.

## 2. Generated Artifacts

The migration process has generated the following BigQuery artifacts:

*   **`bigquery/ddl/job_log_table.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_log_table`. This table is designed to capture job execution details, replacing the functionality of the `FOSJobErzeugeEintrag` utility from the original environment. It records parameters, status, processed records, and timestamps for each run.
*   **`bigquery/ddl/pool_basisprodukt.sql`**
    *   **Role:** Provides a placeholder DDL for the `PoolBasisprodukt` table. This table is referenced in the original script as `v_TabName`. Its actual schema and role (source, target, or intermediate) need to be fully determined from the content of the original `d_ausd_bp_ta_apn_carmen.sql` script. This DDL serves as a starting point for its BigQuery representation.
*   **`bigquery/stored_procedures/d_ausd_bp_ta_apn_carmen.sql`**
    *   **Role:** This is a stub for the BigQuery Stored Procedure that will encapsulate the core data processing logic originally found in `d_ausd_bp_ta_apn_carmen.sql`. It is designed to accept a key date and restart value, perform data transformations, and return the number of records processed. The actual SQL content from the source file needs to be inserted into this procedure.
*   **`bigquery/stored_procedures/r_ausd_bp_ta_apn_carmen.sql`**
    *   **Role:** This is the main BigQuery Stored Procedure that replaces the `k_ausd_bp_ta_apn_carmen.ksh` KornShell script. It handles parameter validation, date calculations, calls the `d_ausd_bp_ta_apn_carmen` procedure for data processing, and logs the job's outcome to the `job_log_table`. It orchestrates the entire flow in BigQuery.

## 3. Key Design Decisions

The following key design decisions were made during this migration:

*   **BigQuery Stored Procedures for Orchestration and Logic:** Both the control flow (`k_ausd_bp_ta_apn_carmen.ksh`) and the core data processing (`d_ausd_bp_ta_apn_carmen.sql`) are migrated into separate BigQuery Stored Procedures. This centralizes the logic within BigQuery, leveraging its native capabilities for execution, parameter handling, and error management.
*   **Replacement of Shell Utilities with BigQuery SQL:**
    *   **Parameter Handling:** `getopts` and custom parameter validation functions are replaced by `IN` parameters and `IF` conditions with `RAISE USING MESSAGE` within the BigQuery Stored Procedure.
    *   **Date Utilities:** `gestern.ksh` and `h_alis_date.ksh` functionality (e.g., "today" and "yesterday" calculations, date format validation) is replaced by BigQuery's built-in `CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()`, and `REGEXP_CONTAINS()` functions. This eliminates external script dependencies.
    *   **Error Handling:** `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` are replaced by BigQuery's `RAISE USING MESSAGE` for structured error reporting.
*   **Encapsulation of Core SQL:** The logic from `d_ausd_bp_ta_apn_carmen.sql` is encapsulated into its own BigQuery Stored Procedure (`d_ausd_bp_ta_apn_carmen`). This promotes modularity, reusability, and clearer separation of concerns between orchestration and data transformation.
*   **BigQuery Table for Job Logging:** The commented-out `FOSJobErzeugeEintrag` functionality is migrated to an explicit `INSERT` statement into a dedicated `job_log_table` in BigQuery. This provides a structured, queryable log of job executions directly within the data warehouse.
*   **Elimination of Temporary Files:** The use of temporary files (e.g., `$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_carmen.tmp` for record counts) is replaced by BigQuery variables (`DECLARE`) and direct querying of target tables or `OUT` parameters from sub-procedures. This avoids file system operations and keeps the entire process within BigQuery.
*   **Trade-offs:**
    *   **Increased BigQuery SQL Complexity:** The migration requires a deeper understanding of BigQuery SQL and its procedural extensions compared to simple shell scripting.
    *   **Dependency on BigQuery Ecosystem:** The solution is now tightly coupled with BigQuery, requiring BigQuery-specific tools and knowledge for deployment, monitoring, and troubleshooting.
    *   **Initial Development Effort:** Re-implementing shell logic and external utilities in BigQuery SQL requires careful translation and testing.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure the target BigQuery `project` and `dataset` (e.g., `default_project.default_dataset`) exist. If not, create them.
2.  **Deploy DDLs:**
    *   Execute `bigquery/ddl/job_log_table.sql` to create the `job_log_table`.
    *   Execute `bigquery/ddl/pool_basisprodukt.sql` to create the `PoolBasisprodukt` table. **Note:** The schema for `PoolBasisprodukt` is a placeholder; it must be updated with the actual schema derived from the original `d_ausd_bp_ta_apn_carmen.sql` and any associated data models.
3.  **Migrate Core SQL Logic:**
    *   **Crucially, the stub in `bigquery/stored_procedures/d_ausd_bp_ta_apn_carmen.sql` must be replaced with the actual BigQuery-compatible SQL logic from the original `d_ausd_bp_ta_apn_carmen.sql` file.** This is a significant manual effort and requires careful translation of any Oracle/SQL*Plus specific syntax to BigQuery SQL.
4.  **Deploy Stored Procedures:**
    *   Execute `bigquery/stored_procedures/d_ausd_bp_ta_apn_carmen.sql` (after filling in the actual logic) to create the core data processing procedure.
    *   Execute `bigquery/stored_procedures/r_ausd_bp_ta_apn_carmen.sql` to create the main orchestration procedure.
5.  **Data Migration:**
    *   Migrate any source data tables required by `d_ausd_bp_ta_apn_carmen.sql` from their original location to BigQuery.
    *   Perform an initial data load for the `PoolBasisprodukt` table if it serves as a source or target for existing data.
6.  **IAM/Permissions:**
    *   Grant the necessary BigQuery IAM roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to the service account or user that will be executing these stored procedures. This includes permissions to read from source tables, write to target tables (like `PoolBasisprodukt`), and insert into `job_log_table`.
7.  **Scheduling/Orchestration:**
    *   Configure an external orchestrator (e.g., Cloud Composer, Cloud Workflows, Cloud Scheduler) to call the `project.dataset.r_ausd_bp_ta_apn_carmen` stored procedure with the required input parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`, `p_wiederanlauf_wert`).

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps, unresolved references, or areas requiring further attention:

*   **Core SQL Logic (`d_ausd_bp_ta_apn_carmen.sql` content):** This is the most significant gap. The generated `d_ausd_bp_ta_apn_carmen.sql` is a stub. The actual, BigQuery-compatible SQL logic from the original file must be manually migrated and inserted. This is a **B4 (Redesign/Manual Migration)** item.
*   **`PoolBasisprodukt` Table Schema:** The DDL for `PoolBasisprodukt` is a placeholder. Its definitive schema, including column names, data types, and primary keys, must be derived from the original `d_ausd_bp_ta_apn_carmen.sql` and any related data models.
*   **Custom Logic in `gestern.ksh`:** The migration assumes `gestern.ksh` provides standard "today" and "yesterday" dates. If it contains complex, custom calendar logic (e.g., handling holidays, fiscal periods, or specific business day calculations), this logic will need dedicated analysis and re-implementation in BigQuery SQL (potentially using UDFs).
*   **SQL*Plus Specific Features:** If the original `d_ausd_bp_ta_apn_carmen.sql` or the `starteSQLSkript` utility utilized highly specific SQL*Plus features (e.g., `SET SERVEROUTPUT ON`, `PROMPT`, `ACCEPT`) or Oracle-specific functions not directly transferable to BigQuery SQL, these will require custom conversion or alternative BigQuery patterns.
*   **Commented-Out Code (`sed/sort/join` block):** The original script contained a commented-out block of `sed/sort/join` commands. This functionality has been ignored for the initial migration. If this logic is ever required, it would necessitate a separate migration effort to BigQuery table transformations or data manipulation statements.
*   **Environment Variables:** The original script relied heavily on environment variables like `$HOME`, `$BERT_DIR_ROOT`, and `$DW_DIR_UTL`. In the BigQuery environment, these are replaced by explicit project and dataset names, or configuration parameters passed to the stored procedures. Any implicit paths or configurations derived from these variables in the original `d_ausd_bp_ta_apn_carmen.sql` will need to be explicitly defined in the BigQuery version.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job, follow these steps:

1.  **Deployment Verification:**
    *   Confirm that all DDLs and Stored Procedures are successfully deployed to the target BigQuery project and dataset.
    *   Verify that the `job_log_table` and `PoolBasisprodukt` (with its correct schema) exist.
2.  **Test Execution - Success Path:**
    *   Execute the main orchestration procedure `CALL `default_project.default_dataset.r_ausd_bp_ta_apn_carmen`(...)` with a set of valid input parameters (e.g., `p_job_kennung`, `p_eintrags_nr`, `p_stichtag` in `DDMMYYYY` format, and a valid `p_wiederanlauf_wert`).
    *   **Passing Criteria:**
        *   The procedure completes without error.
        *   An entry is successfully inserted into `job_log_table` with the correct `job_identifier`, `entry_number`, `key_date`, `restart_value`, and `records_processed`.
        *   The `records_processed` value in the log table accurately reflects the number of records processed by the `d_ausd_bp_ta_apn_carmen` procedure.
        *   The target table (`PoolBasisprodukt` or other tables modified by `d_ausd_bp_ta_apn_carmen`) shows the expected data transformations and updates.
3.  **Test Execution - Error Paths:**
    *   **Missing Parameters:** Call `r_ausd_bp_ta_apn_carmen` with missing required parameters (e.g., `NULL` or empty string for `p_job_kennung`, `p_eintrags_nr`, `p_stichtag`).
        *   **Passing Criteria:** The procedure `RAISE`s an error with a message indicating the missing parameter (e.g., "FEHLER: Missing parameter p_JobKennung").
    *   **Invalid Date Format:** Call `r_ausd_bp_ta_apn_carmen` with `p_stichtag` in an incorrect format (e.g., `YYYY-MM-DD`, `DD/MM/YYYY`, or non-numeric).
        *   **Passing Criteria:** The procedure `RAISE`s an error with a message indicating an invalid date format (e.g., "FEHLER: Invalid date format for p_Stichtag. Expected DDMMYYYY.").
    *   **Invalid Restart Value:** Call `r_ausd_bp_ta_apn_carmen` with `p_wiederanlauf_wert` as a non-integer string.
        *   **Passing Criteria:** The procedure `RAISE`s an error with a message indicating an invalid restart value (e.g., "FEHLER: Invalid restart value. Expected integer, got ABC").
4.  **Data Integrity Check:**
    *   After successful runs, compare a sample of processed data in BigQuery with the expected output based on the original script's behavior. This is especially critical once the `d_ausd_bp_ta_apn_carmen` stub is filled.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Halt New Executions:** Immediately stop any scheduled or manual executions of the BigQuery stored procedure `r_ausd_bp_ta_apn_carmen`.
2.  **Revert Scheduling:** Reconfigure the external orchestrator (e.g., Cloud Composer, Cloud Scheduler) to disable calls to the BigQuery stored procedure and re-enable the execution of the original `k_ausd_bp_ta_apn_carmen.ksh` script in its legacy environment.
3.  **Data Restoration (if necessary):**
    *   If the BigQuery job made incorrect modifications to target tables (e.g., `PoolBasisprodukt`), restore these tables from the most recent backup taken *before* the BigQuery job's execution.
    *   The `job_log_table` can be truncated or its entries for the problematic runs can be deleted if desired, but typically log tables are kept for audit.
4.  **Delete BigQuery Artifacts:**
    *   Delete the BigQuery stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `default_project.default_dataset.r_ausd_bp_ta_apn_carmen`;
        DROP PROCEDURE IF EXISTS `default_project.default_dataset.d_ausd_bp_ta_apn_carmen`;
        ```
    *   If the `PoolBasisprodukt` table was created specifically for this migration and is not used by other processes, it can be dropped (after ensuring data is restored elsewhere if needed):
        ```sql
        DROP TABLE IF EXISTS `default_project.default_dataset.PoolBasisprodukt`;
        ```
    *   The `job_log_table` can be retained or dropped based on policy.
5.  **Monitor Legacy System:** Ensure the original `k_ausd_bp_ta_apn_carmen.ksh` script is running correctly in the legacy environment.
6.  **Root Cause Analysis:** Investigate the issues that necessitated the rollback and address them before attempting another migration.