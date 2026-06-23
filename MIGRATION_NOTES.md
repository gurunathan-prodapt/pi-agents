# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`. The original job served as an ETL control script, orchestrating environment setup, parameter validation, date validation, and the execution of an associated SQL script (`d_ausd_bp_ta_bpr_beschr.sql`) primarily interacting with the `PoolBasisprodukt` table.

The job has been migrated to Google Cloud Platform, leveraging **BigQuery** for data processing and storage. The KornShell orchestration logic, including parameter handling, validation, and date derivations, has been re-implemented as a BigQuery Stored Procedure. The underlying business SQL logic (from `d_ausd_bp_ta_bpr_beschr.sql`) is expected to be translated into a separate BigQuery Stored Procedure, which is then called by the main orchestration procedure.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`project/dataset/ddl/pool_basis_produkt.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the target table `PoolBasisprodukt` in BigQuery. This table is the primary data target for the business logic executed by the job. The provided DDL is a placeholder and requires refinement based on the actual schema used by the original `d_ausd_bp_ta_bpr_beschr.sql`.
*   **`project/dataset/ddl/job_audit_table.sql`**
    *   **Role**: Defines the DDL for a new audit table in BigQuery. This table replaces the functionality of the legacy job management system (e.g., `FOSJobErzeugeEintrag`) by logging the status, parameters, record counts, and messages for each job run.
*   **`project/dataset/stored_procedures/d_ausd_bp_ta_bpr_beschr_proc.sql`**
    *   **Role**: This is a BigQuery Stored Procedure designed to encapsulate the core business logic originally found in `d_ausd_bp_ta_bpr_beschr.sql`. It accepts parameters from the orchestration procedure and is expected to perform the actual data extraction, transformation, and loading operations into `PoolBasisprodukt`. *Note: This is currently a placeholder and requires the actual translation of the original SQL script.*
*   **`project/dataset/stored_procedures/r_ausd_bp_ta_bpr_beschr.sql`**
    *   **Role**: This is the main BigQuery Stored Procedure that replaces the original KornShell script. It handles:
        *   Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
        *   Performing date validations and derivations.
        *   Calling the `d_ausd_bp_ta_bpr_beschr_proc` to execute the business logic.
        *   Retrieving record counts after the business logic execution.
        *   Logging job status and details into the `job_audit_table`, including error handling.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration**: The entire KornShell control flow, including parameter validation, date handling, and execution sequencing, is translated into a BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_beschr`). This centralizes the logic within BigQuery, leveraging its native scripting capabilities (DECLARE, SET, IF, RAISE, CALL) and reducing external dependencies.
    *   **Trade-off**: While BigQuery Stored Procedures are powerful for SQL-centric logic, complex external interactions or multi-service orchestrations might eventually necessitate an external orchestrator like Cloud Composer. For this job's scope, BigQuery SPs are sufficient.
*   **Native BigQuery Functions for Utilities**: Instead of replicating shell utility scripts (`h_alis_date.ksh`, `gestern.ksh`), BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB`, `SAFE.PARSE_DATE`) are used for date derivations and validations. Parameter validation uses standard SQL `IF` and `RAISE` statements.
    *   **Benefit**: Simplifies the code, improves performance, and reduces maintenance overhead by relying on BigQuery's optimized functions.
*   **Dedicated BigQuery Stored Procedure for Business Logic**: The original `d_ausd_bp_ta_bpr_beschr.sql` is designed to be migrated into its own BigQuery Stored Procedure (`d_ausd_bp_ta_bpr_beschr_proc`). This promotes modularity and reusability, allowing the orchestration logic to remain clean and focused on control flow.
    *   **Benefit**: Clear separation of concerns between orchestration and business logic.
*   **BigQuery Audit Table for Job Logging**: The legacy job management system (implied by commented `FOSJobErzeugeEintrag`) is replaced by a dedicated `job_audit_table` in BigQuery. This provides a centralized, queryable log of all job executions, including success/failure status, parameters, and record counts.
    *   **Benefit**: Enhanced observability, easier debugging, and historical tracking of job runs within the data platform.
*   **Error Handling with `RAISE` and `EXCEPTION WHEN ERROR`**: BigQuery's procedural error handling is used to catch and log errors to the `job_audit_table` before re-raising them. This ensures that failures are recorded and propagated.
    *   **Benefit**: Robust error reporting and logging, crucial for operational stability.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Creation**:
    *   Ensure the target Google Cloud Project (`project`) exists.
    *   Create the BigQuery dataset (`dataset`) within the project, if it doesn't already exist. This dataset will house all tables and stored procedures.
        *   `bq mk --dataset project:dataset`
2.  **IAM Permissions**:
    *   Grant appropriate IAM roles to the service account or user that will execute the BigQuery stored procedures. This typically includes:
        *   `BigQuery Data Editor` on `project.dataset` (for `INSERT`, `UPDATE`, `DELETE` on `PoolBasisprodukt` and `job_audit_table`).
        *   `BigQuery Job User` on `project` (to run BigQuery jobs, including stored procedures).
        *   `BigQuery Data Viewer` on `project.dataset` (to read data from `PoolBasisprodukt` for record counting).
3.  **Deploy DDLs**:
    *   Execute `project/dataset/ddl/pool_basis_produkt.sql` to create the `PoolBasisprodukt` table. **Crucially, the placeholder schema must be updated with the actual columns from the original `PoolBasisprodukt` table as defined by `d_ausd_bp_ta_bpr_beschr.sql`.**
    *   Execute `project/dataset/ddl/job_audit_table.sql` to create the `job_audit_table`.
4.  **Deploy Stored Procedures**:
    *   Execute `project/dataset/stored_procedures/d_ausd_bp_ta_bpr_beschr_proc.sql` to create the business logic procedure. **This procedure must be fully translated from the original `d_ausd_bp_ta_bpr_beschr.sql` before deployment.**
    *   Execute `project/dataset/stored_procedures/r_ausd_bp_ta_bpr_beschr.sql` to create the main orchestration procedure.
5.  **Connection Strings/Secrets**:
    *   BigQuery stored procedures do not directly use external connection strings or secrets in the same way a KornShell script might. All interactions are within BigQuery. If `d_ausd_bp_ta_bpr_beschr_proc` needs to access external data sources (e.g., Cloud Storage, other databases), appropriate external tables or federated queries would need to be configured, along with their respective IAM permissions.
6.  **Scheduling**:
    *   Integrate the execution of `CALL project.dataset.r_ausd_bp_ta_bpr_beschr(...)` into a GCP scheduling service:
        *   **Cloud Scheduler**: For simple, time-based triggers.
        *   **Cloud Composer (Airflow)**: Recommended for more complex workflows, dependency management, retries, and integration with other GCP services. A Python DAG would be created to call the BigQuery stored procedure with the required parameters.
        *   **Cloud Workflows**: For event-driven or sequential task orchestration.
    *   Ensure the scheduler passes the required parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) to the BigQuery stored procedure.

## 5. Known gaps & unresolved references

*   **`d_ausd_bp_ta_bpr_beschr.sql` Translation**: The most significant gap is the actual content of `d_ausd_bp_ta_bpr_beschr_proc.sql`. This procedure is currently a placeholder. A separate, detailed migration effort is required to translate the original `d_ausd_bp_ta_bpr_beschr.sql` into BigQuery SQL, including its DDL for `PoolBasisprodukt` and any other tables it interacts with.
*   **`PoolBasisprodukt` Schema**: The DDL for `project.dataset.PoolBasisprodukt` is a minimal placeholder. Its full schema must be defined based on the original `d_ausd_bp_ta_bpr_beschr.sql` script's requirements.
*   **Commented-out Code in Source**: The original KornShell script contained commented-out sections for file manipulation (`sed`, `sort`, `join`). These are not migrated. If these operations are ever reactivated, their functionality would need to be re-evaluated and implemented using BigQuery SQL transformations (e.g., `REGEXP_REPLACE`, `DISTINCT`, `JOIN`) or `EXPORT DATA` if file output is truly required.
*   **`starteSQLSkript` Functionality**: The exact implementation details of the original `starteSQLSkript` are not fully known. The migration assumes it's a wrapper for SQL execution. If it had advanced features (e.g., specific error handling, retry mechanisms, connection pooling), these would need to be replicated in the BigQuery stored procedure or the orchestration layer (e.g., Cloud Composer).
*   **Job-table Integration (`FOSJobErzeugeEintrag`)**: While `job_audit_table` is a replacement, its specific columns and data types should be reviewed against the original `FOSJobErzeugeEintrag` system to ensure all necessary audit information is captured.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prerequisites**: Ensure all manual steps (DDL deployment, SP deployment, IAM) are completed, and the `d_ausd_bp_ta_bpr_beschr_proc` contains the translated business logic.
2.  **Test Data**: Load a representative set of test data into any source tables that `d_ausd_bp_ta_bpr_beschr_proc` might read from.
3.  **Execute the Main Procedure**:
    *   Call the main orchestration procedure `r_ausd_bp_ta_bpr_beschr` with various parameter combinations, including:
        *   **Valid parameters**: `CALL project.dataset.r_ausd_bp_ta_bpr_beschr('JOB123', 'ENTRY001', '01012023', 0);`
        *   **Missing required parameters**: `CALL project.dataset.r_ausd_bp_ta_bpr_beschr(NULL, 'ENTRY001', '01012023', 0);` (Expected to `RAISE` an error).
        *   **Invalid date format**: `CALL project.dataset.r_ausd_bp_ta_bpr_beschr('JOB123', 'ENTRY001', '2023-01-01', 0);` (Expected to `RAISE` an error).
        *   **Restart value**: `CALL project.dataset.r_ausd_bp_ta_bpr_beschr('JOB123', 'ENTRY001', '01012023', 1);`
4.  **Check `job_audit_table`**:
    *   After each execution, query `project.dataset.job_audit_table` to verify:
        *   A new record is inserted for each run.
        *   `job_status` correctly reflects 'A' for success or 'E' for failure.
        *   `message` contains "Job executed successfully" or the appropriate error message.
        *   `record_count` matches the expected number of records processed/loaded.
        *   All input parameters (`tab_name`, `stichtag`, `run_date`, `restart_flag`, etc.) are correctly logged.
5.  **Check `PoolBasisprodukt`**:
    *   After successful runs, query `project.dataset.PoolBasisprodukt` to ensure:
        *   The data loaded is correct and complete according to the business logic.
        *   The number of rows matches `record_count` in the audit table for the corresponding run.
        *   Data types and values are as expected.
6.  **Performance**: Monitor the execution time of the stored procedures in BigQuery to ensure they meet performance requirements.

**"Passing" means**:
*   The `r_ausd_bp_ta_bpr_beschr` procedure executes without unhandled errors for valid inputs.
*   Parameter validation correctly identifies and raises errors for invalid inputs.
*   The `job_audit_table` accurately records the outcome (success/failure), parameters, and record counts for all executions.
*   The `PoolBasisprodukt` table contains the correct and expected data after successful runs, matching the output of the original KornShell job.
*   Execution times are within acceptable limits.

## 7. Rollback procedure

In case of issues or a decision to revert, the following rollback procedure should be followed:

1.  **Stop Scheduling**: Immediately disable or pause any scheduled executions of the BigQuery stored procedure (`r_ausd_bp_ta_bpr_beschr`) in Cloud Scheduler, Cloud Composer, or any other orchestration service.
2.  **Revert Data (if necessary)**:
    *   If the `PoolBasisprodukt` table was modified by the migrated job and data integrity is compromised, restore the table to its state before the migration. This might involve:
        *   Restoring from a BigQuery snapshot or backup.
        *   Running a data correction script.
        *   If the original job was still running in parallel, ensure its output is correctly populating the original target.
3.  **Drop BigQuery Objects**:
    *   Drop the migrated BigQuery stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_bp_ta_bpr_beschr`;
        DROP PROCEDURE IF EXISTS `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`;
        ```
    *   Drop the newly created tables (if they are not needed for audit history or other purposes):
        ```sql
        DROP TABLE IF EXISTS `project.dataset.PoolBasisprodukt`; -- Only if it was newly created and not replacing an existing one
        DROP TABLE IF EXISTS `project.dataset.job_audit_table`;
        ```
4.  **Reactivate Original Job**:
    *   Ensure the original KornShell script (`k_ausd_bp_ta_bpr_beschr.ksh`) and its dependencies are fully operational in the legacy environment.
    *   Re-enable its original scheduling mechanism.
5.  **Verify Original Job**:
    *   Monitor the original job to confirm it is running correctly and producing the expected output.

This procedure assumes that the original job and its environment remain available and functional as a fallback.