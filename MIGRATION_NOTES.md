# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell (KSH) wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh`. The script, which orchestrates the preparation of selected base products for downstream systems like BERT and Forderungsscoring (FOS), has been migrated to Google Cloud BigQuery.

The migration involved refactoring the KSH script's parameter handling, logging, and orchestration logic into a BigQuery Stored Procedure. The core business logic, originally residing in `k_ausd_bp_ta_cntrct_dist.ksh`, is expected to be migrated separately into another BigQuery Stored Procedure.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_control.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `project.dataset.job_control` table in BigQuery. This table serves as a central repository for tracking the execution status, parameters, and timestamps of jobs, replacing the implicit job status tracking and some logging aspects of the original KSH script.
*   **`sql/ddl/job_audit_log.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.job_audit_log` table. This table stores detailed operational logs and messages generated during the execution of the BigQuery Stored Procedure, replacing the flat-file log output of the legacy system.
*   **`sql/ddl/job_error_log.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.job_error_log` table. This table is dedicated to recording specific errors encountered during job execution, replacing the error reporting mechanisms (`DWMSG_Fehlerbehandlung`) of the original KSH script.
*   **`sql/procedures/ausd_bp_ta_cntrct_dist_wrapper.sql`**
    *   **Role:** This BigQuery Stored Procedure, `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`, is the direct migration of the `r_ausd_bp_ta_cntrct_dist.ksh` wrapper script. It handles:
        *   Parsing and defaulting of input parameters (`p_stichtag`, `p_wiederanlaufWert`).
        *   Validation of the `Stichtag`.
        *   Structured logging to `job_control`, `job_audit_log`, and `job_error_log` tables.
        *   Orchestration by calling the core business logic stored procedure (`project.dataset.ausd_bp_ta_cntrct_dist_core`).
        *   Robust error handling using BigQuery's `EXCEPTION WHEN ERROR` block.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Wrapper Script to BigQuery Stored Procedure:** The KSH wrapper script's orchestration, parameter handling, and logging responsibilities were directly translated into a BigQuery Stored Procedure (`ausd_bp_ta_cntrct_dist_wrapper`). This centralizes the control flow within BigQuery, leveraging its native capabilities.
*   **Structured Logging in BigQuery Tables:** The legacy flat-file logging (`DWMSG_*` functions, `tee -a $LogDatei`) was replaced with structured logging by inserting records into dedicated BigQuery tables (`job_control`, `job_audit_log`, `job_error_log`). This provides queryable, centralized, and more robust logging and auditing capabilities.
*   **Native BigQuery Parameter and Date Handling:** KSH `getopts` and helper scripts (`h_alis_parameter.ksh`, `h_alis_date.ksh`) were replaced by BigQuery Stored Procedure input parameters, `IFNULL`/`NULLIF` for defaulting, and native BigQuery date functions (`CURRENT_DATE()`, `FORMAT_DATE()`). This simplifies the code and removes external script dependencies.
*   **BigQuery Error Handling:** The KSH `set -e`, `trap` commands, and `DWMSG_Fehlerbehandlung` were replaced by BigQuery's `EXCEPTION WHEN ERROR` block and `SIGNAL SQLSTATE` for error propagation, combined with `INSERT` statements into the `job_error_log` table for detailed error recording.
*   **Orchestration via `CALL` Statement:** The KSH invocation of the core script (`k_ausd_bp_ta_cntrct_dist.ksh`) was replaced by a `CALL` statement to the target BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_cntrct_dist_core`). This maintains the wrapper-core separation within the BigQuery environment.

**Notable Trade-offs:**

*   **Loss of Direct OS-level Signal Handling:** The `trap` command in KSH provides OS-level signal handling. This functionality is not directly replicable within BigQuery SQL. Higher-level orchestration tools (e.g., Cloud Composer) are expected to manage job cancellation, retries, and error notifications.
*   **Increased Logging Complexity:** While structured logging offers significant benefits, it replaces simple file appends with more complex SQL `INSERT` statements, which can be slightly more verbose.
*   **Dependency on Core Logic Migration:** The `ausd_bp_ta_cntrct_dist_wrapper` procedure is dependent on the successful and complete migration of the core business logic into `project.dataset.ausd_bp_ta_cntrct_dist_core`.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **BigQuery Logging/Control Table Creation:** Execute the DDL scripts for the logging and control tables:
    *   `sql/ddl/job_control.sql`
    *   `sql/ddl/job_audit_log.sql`
    *   `sql/ddl/job_error_log`
3.  **Core Logic Migration and Deployment:** The core business logic from `k_ausd_bp_ta_cntrct_dist.ksh` **must be migrated and deployed** as the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_dist_core`. This is a critical prerequisite.
4.  **Deploy Wrapper Stored Procedure:** Execute the DDL for the wrapper stored procedure:
    *   `sql/procedures/ausd_bp_ta_cntrct_dist_wrapper.sql`
5.  **IAM/Permissions Configuration:**
    *   Grant the service account that will execute the BigQuery Stored Procedures (via an orchestrator) the necessary BigQuery roles: `BigQuery Data Editor` (for inserting into log tables and potentially writing to target tables) and `BigQuery Job User` (for running queries and procedures).
    *   Ensure the service account has permissions to read from source DWH tables and write to target FOS tables in BigQuery.
6.  **Orchestration Setup:** Configure the chosen orchestration tool (e.g., Cloud Composer, Cloud Workflows, Cloud Scheduler) to trigger the `project.dataset.ausd_bp_ta_cntrct_dist_wrapper` stored procedure. This includes defining the schedule and passing any required parameters.
7.  **Data Migration:** Ensure that the source DWH Contract Cache tables and the target FOS tables have been successfully migrated to BigQuery and are accessible to the stored procedures.

## 5. Known gaps & unresolved references

*   **Core Script Logic (`k_ausd_bp_ta_cntrct_dist.ksh`):** The detailed logic of this core script was not part of this migration scope. Its migration to `project.dataset.ausd_bp_ta_cntrct_dist_core` is a significant, unresolved item that requires separate design and implementation. The functionality of the wrapper depends entirely on this core procedure being correctly implemented.
*   **Specific Data Sources/Targets:** The exact BigQuery table names and schemas for the "DWH Contract Cache" (source) and "FOS-Tabelle" (target) are not defined in this migration. These need to be identified and mapped.
*   **`DW_EintragsNr` Generation:** The `job_nr` in `job_control` is generated using `MAX(job_nr) + 1`. While generally sufficient for batch processing, this approach is not atomically safe in highly concurrent environments. For scenarios requiring strict sequential or gap-free IDs under high concurrency, a more robust ID generation strategy (e.g., BigQuery sequences if available, or a dedicated ID service) might be considered.
*   **`log_file_name` Placeholder:** The `log_file_name` field in `job_audit_log` currently uses a placeholder value (`'placeholder_log_file.log'`). If this field is intended to carry meaningful information (e.g., a unique identifier for a specific log run), a mechanism to generate and populate it with a relevant value should be implemented.

## 6. Validation

Validation of the migrated component involves unit and integration testing:

1.  **Unit Tests (BigQuery Stored Procedure `ausd_bp_ta_cntrct_dist_wrapper`):**
    *   **Test Cases:**
        *   Call with valid `p_stichtag` and `p_wiederanlaufWert`.
        *   Call with `p_stichtag` as `NULL` or empty string (should default to `CURRENT_DATE`).
        *   Call with `p_wiederanlaufWert` as `NULL` (should default to `0`).
        *   Call with an invalid `p_stichtag` (e.g., non-date format, if validation logic is added beyond just `NULL`/empty check).
        *   Simulate a failure in the `ausd_bp_ta_cntrct_dist_core` procedure (e.g., by creating a dummy core procedure that always `SIGNAL`s an error).
    *   **Passing Criteria:**
        *   For successful calls: `job_control` table shows `status = 'SUCCESS'`, `job_audit_log` contains expected messages, and `job_error_log` is empty.
        *   For calls with missing/invalid `Stichtag`: `job_control` table shows `status = 'FAILED'`, `job_error_log` contains the expected validation error, and `SIGNAL SQLSTATE` is raised.
        *   For simulated core procedure failure: `job_control` table shows `status = 'FAILED'`, `job_error_log` contains the error from the core procedure, and `SIGNAL SQLSTATE` is raised.
        *   The `CALL project.dataset.ausd_bp_ta_cntrct_dist_core` statement is executed with the correct, resolved parameters.

2.  **Integration Tests (End-to-End Workflow):**
    *   **Prerequisites:** The `project.dataset.ausd_bp_ta_cntrct_dist_core` procedure must be fully implemented and deployed, and source/target data tables must be available.
    *   **Test Cases:**
        *   Trigger the `ausd_bp_ta_cntrct_dist_wrapper` via the configured orchestrator (e.g., Cloud Composer DAG) with various valid parameters.
        *   Trigger with parameters that should cause a validation error.
        *   Trigger with parameters that might cause an error in the core procedure.
    *   **Passing Criteria:**
        *   The orchestrator job completes successfully (or fails as expected for error cases).
        *   `job_control` table reflects the final status (`SUCCESS` or `FAILED`).
        *   `job_audit_log` contains a complete sequence of events for the job.
        *   `job_error_log` is empty for successful runs and contains relevant errors for failed runs.
        *   Crucially, the `project.dataset.ausd_bp_ta_cntrct_dist_core` procedure is invoked, and its intended data transformations (extraction from DWH, processing, and loading into FOS target tables) are correctly performed, resulting in the expected data in the target BigQuery tables. Data consistency checks between legacy and new target data should be performed.

## 7. Rollback procedure

In case of issues or unexpected behavior after deployment, the following rollback procedure can be followed:

1.  **Immediate Reversion:**
    *   **Orchestration:** Deactivate or delete the new orchestrator configuration (e.g., Cloud Composer DAG, Cloud Workflow, Cloud Scheduler job) that triggers `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`.
    *   **Legacy System:** Re-enable or revert to using the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh` script and its associated scheduling.

2.  **BigQuery Cleanup (if necessary):**
    *   **Stored Procedures:** Drop the newly deployed BigQuery Stored Procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`;
        -- If core procedure was also deployed as part of this migration, drop it too:
        -- DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_cntrct_dist_core`;
        ```
    *   **Logging/Control Tables:** If these tables were created solely for this migration and are not shared, they can be dropped:
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_control`;
        DROP TABLE IF EXISTS `project.dataset.job_audit_log`;
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        ```
    *   **Data Rollback:** If the `ausd_bp_ta_cntrct_dist_core` procedure (once implemented) writes or modifies data in target tables, a specific data rollback strategy will be required. This might involve:
        *   Restoring target tables from a previous backup.
        *   Utilizing BigQuery's time travel feature to revert tables to a state before the problematic run.
        *   Running a cleanup script to delete or correct data written by the failed job. This strategy depends heavily on the specific data transformations performed by the core logic.

3.  **Re-evaluation:** Analyze the cause of the rollback, address the identified issues, and plan for a re-deployment.