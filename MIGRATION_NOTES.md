# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`. The original script served as an orchestrator for data preparation, handling environment setup, parameter parsing and validation, execution of a core SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`), and basic logging.

The migration target platform is Google Cloud BigQuery. The KornShell orchestration logic has been translated into a BigQuery Stored Procedure, leveraging BigQuery's native SQL capabilities for parameter handling, validation, error management, and logging. The core data processing logic, originally in `d_ausd_bp_ta_bcp_msisdn.sql`, is expected to be migrated into BigQuery SQL statements executed within or called by this stored procedure.

## 2. Generated artifacts

The migration process has generated the following BigQuery artifacts:

*   **`your_project_id.your_dataset_id.job_log.sql`**
    *   **Role:** This SQL file defines the Data Definition Language (DDL) for the `job_log` table in BigQuery. This table serves as the centralized logging mechanism for the migrated job, replacing the original script's temporary file-based record counting and any commented-out job management system interactions. It records job name, entry number, reference date, restart value, processed record count, status (SUCCESS/FAILED), timestamp, and any error messages.

*   **`your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn.sql`**
    *   **Role:** This SQL file contains the BigQuery Stored Procedure `r_ausd_bp_ta_bcp_msisdn`. This procedure is the direct replacement for the original `k_ausd_bp_ta_bcp_msisdn.ksh` KornShell script. It encapsulates the orchestration logic, including:
        *   Accepting input parameters (Job ID, entry number, reference date, restart value).
        *   Validating these parameters, especially the reference date format.
        *   Executing the core data processing logic (which needs to be migrated from `d_ausd_bp_ta_bcp_msisdn.sql` and integrated into this procedure or called by it).
        *   Capturing the number of processed records.
        *   Logging the job's status and record count to the `job_log` table.
        *   Implementing BigQuery-native error handling.

## 3. Key design decisions

*   **Orchestration Logic in BigQuery Stored Procedure:** The entire orchestration logic, previously handled by the KornShell script, is now embedded within a BigQuery Stored Procedure. This eliminates external shell dependencies, provides native BigQuery execution, and allows for direct integration with BigQuery's SQL capabilities.
*   **Parameter Handling:** Command-line arguments from the KornShell script are directly mapped to input parameters of the BigQuery Stored Procedure. This provides type safety and simplifies parameter passing within the BigQuery environment.
*   **Native BigQuery Functions for Date Operations:** Shell script date utilities (`gestern.ksh`, `h_alis_date.ksh`) are replaced by BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`). This improves performance, consistency, and removes external script calls.
*   **Centralized BigQuery Logging Table:** The ad-hoc temporary file for record counts and the commented-out job management system interactions are replaced by a dedicated `job_log` table in BigQuery. This provides a structured, queryable, and robust logging mechanism for job status and metrics.
*   **BigQuery-Native Error Handling:** The KornShell script's error handling (`f_alis_msgerr.ksh`, `exit` codes) is replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks and `RAISE` statements. This ensures errors are caught, logged, and propagated within the BigQuery ecosystem.
*   **Direct SQL Execution:** The `starteSQLSkript` wrapper for `sqlplus` is replaced by direct execution of BigQuery SQL statements within the stored procedure. This removes the dependency on an external SQL client and streamlines the data processing flow.

**Notable Trade-offs:**

*   **Increased Complexity in BigQuery SQL:** While beneficial for integration, translating shell orchestration logic into BigQuery SQL can sometimes lead to more verbose or complex SQL code compared to simple shell scripting.
*   **Dependency on Core SQL Migration:** The full functionality of this migrated orchestrator hinges on the successful and accurate migration of the `d_ausd_bp_ta_bcp_msisdn.sql` logic into BigQuery SQL. This is a significant prerequisite.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`your_project_id.your_dataset_id`) exists. If not, create it.
    ```sql
    CREATE SCHEMA `your_project_id.your_dataset_id`;
    ```
2.  **Deploy `job_log` Table DDL:** Execute the `your_project_id.your_dataset_id.job_log.sql` script to create the `job_log` table.
    ```sql
    -- From your_project_id.your_dataset_id.job_log.sql
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_log` (
        job_name STRING,
        entry_nr STRING,
        stichtag STRING,
        restart_value STRING,
        records_processed INT64,
        status STRING,
        created_at TIMESTAMP,
        error_message STRING
    );
    ```
3.  **Migrate Core SQL Logic:** **Crucially, the business logic from `d_ausd_bp_ta_bcp_msisdn.sql` must be migrated to BigQuery SQL.** This involves:
    *   Identifying source and target tables used by `d_ausd_bp_ta_bcp_msisdn.sql`.
    *   Creating the necessary DDL for these source and target tables in BigQuery.
    *   Translating the SQL logic into BigQuery-compatible statements (e.g., `INSERT`, `MERGE`, `CREATE TABLE AS SELECT`).
    *   Integrating this translated logic into the `r_ausd_bp_ta_bcp_msisdn` stored procedure, replacing the placeholder comments. This includes updating the `target_table` and `source_table` references in the generated procedure.
4.  **Deploy Stored Procedure:** Execute the `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn.sql` script to create or replace the stored procedure.
5.  **IAM Permissions:** Grant appropriate BigQuery IAM roles to the service account or user that will execute the stored procedure. This typically includes:
    *   `BigQuery Data Editor` on `your_project_id.your_dataset_id` (to write to `job_log` and target tables).
    *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
6.  **Scheduling (if applicable):** If the job needs to be scheduled, configure a BigQuery Scheduled Query, Cloud Composer DAG, or Google Cloud Workflow to invoke the `r_ausd_bp_ta_bcp_msisdn` stored procedure with the required parameters.
7.  **Replace Placeholders:** Ensure all instances of `your_project_id.your_dataset_id` in the generated code are replaced with the actual BigQuery project ID and dataset ID.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up and require further attention:

*   **Core SQL Logic (`d_ausd_bp_ta_bcp_msisdn.sql`):** The actual data transformation logic from this original SQL file is the primary unresolved reference. Its migration to BigQuery SQL is a prerequisite for this orchestrator to function correctly. The `target_table` and `source_table` references in the generated stored procedure are placeholders that must be updated based on this migration.
*   **Commented-out Code Functionality:** The original KornShell script contains significant commented-out sections (e.g., `AL??` for job management, `sed`, `sort`, `join` for post-processing). It is critical to confirm if these functionalities are still required. If so, they need to be designed and implemented in BigQuery SQL or integrated into the orchestration layer (e.g., Cloud Composer).
*   **`starteSQLSkript` Exact Behavior:** The precise functionality of the `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) in the original script, especially regarding output capture and error handling, was assumed to be replaced by direct BigQuery SQL execution. Any subtle behaviors not captured by this assumption might need adjustment.
*   **Error Number Mapping:** The original script uses specific error codes (e.g., `ErrNr=193`). While BigQuery's `RAISE` and `EXCEPTION` provide robust error handling, a direct mapping of these legacy error numbers to specific BigQuery error messages or custom error codes has not been implemented and might be required for consistency with existing error reporting.

## 6. Validation

To validate the migrated BigQuery stored procedure, perform the following tests:

1.  **Successful Execution (Happy Path):**
    *   **Action:** Call the stored procedure with valid parameters (e.g., `CALL \`your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn\`('TestJob', '1', '01012023', '0');`).
    *   **Passing Criteria:**
        *   The procedure completes without raising an error.
        *   A `SUCCESS` entry is recorded in the `your_project_id.your_dataset_id.job_log` table for the corresponding job run.
        *   The `records_processed` field in the `job_log` entry matches the expected number of records processed by the core SQL logic.
        *   The target BigQuery tables contain the correct, transformed data as expected for the given `Stichtag`.

2.  **Parameter Validation (Error Cases):**
    *   **Action:** Call the stored procedure with missing or invalid parameters:
        *   Missing `p_JobKennung` (e.g., `NULL` or empty string).
        *   Missing `p_EintragsNr` (e.g., `NULL` or empty string).
        *   Missing `p_Stichtag` (e.g., `NULL` or empty string).
        *   Invalid `p_Stichtag` format (e.g., `'2023-01-01'`, `'01/01/2023'`).
    *   **Passing Criteria:**
        *   The procedure raises an error with a descriptive message (e.g., "Jobkennung fehlt", "Stichtag hat kein gueltiges Format DDMMYYYY").
        *   A `FAILED` entry is recorded in the `your_project_id.your_dataset_id.job_log` table, including the error message.

3.  **Core Logic Failure (Error Path):**
    *   **Action:** Simulate a failure within the core data processing logic (e.g., by introducing a syntax error in the integrated SQL, or by ensuring a source table is unavailable).
    *   **Passing Criteria:**
        *   The procedure raises an error.
        *   A `FAILED` entry is recorded in the `your_project_id.your_dataset_id.job_log` table, capturing the specific error message from the core logic failure.

## 7. Rollback procedure

In case of issues or unexpected behavior after deployment, the following steps outline the rollback procedure:

1.  **Halt New BigQuery Executions:** Immediately stop any scheduled BigQuery queries, Cloud Composer DAGs, or other orchestration mechanisms that invoke the `r_ausd_bp_ta_bcp_msisdn` stored procedure.
2.  **Revert to Original System:** Re-enable and restart the execution of the original `k_ausd_bp_ta_bcp_msisdn.ksh` KornShell script in the legacy environment.
3.  **Data Integrity Check:**
    *   Assess the state of the target tables in BigQuery. If the BigQuery procedure made any modifications, determine if these changes need to be reverted. This might involve:
        *   Restoring the target tables from a point-in-time backup (if available).
        *   Running a compensating transaction if the changes were minor and reversible.
        *   If the BigQuery procedure only inserted new data, simply dropping the newly inserted partitions/data might suffice.
    *   Ensure that the original system can pick up processing from a consistent state, potentially requiring a re-run for the affected `Stichtag`.
4.  **Optional: Remove BigQuery Artifacts:** If the rollback is deemed permanent or for a significant period, consider dropping the BigQuery stored procedure and the `job_log` table to avoid confusion and resource consumption.
    ```sql
    DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`;
    DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_log`;
    ```
5.  **Post-Rollback Monitoring:** Monitor the legacy system to ensure it is functioning correctly and processing data as expected.