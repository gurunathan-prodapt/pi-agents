# MIGRATION_NOTES.md

## 1. Summary

This migration involved the orchestration logic of the KornShell script `k_ausd_bp_ta_cntrct_dist.ksh`. This script was responsible for parsing parameters, setting up the execution environment, performing date calculations, and orchestrating the execution of a core SQL script (`d_ausd_bp_ta_cntrct_dist.sql`).

The migration target platform is Google Cloud Platform, specifically BigQuery. The KornShell orchestration logic has been translated into a BigQuery stored procedure, and the core SQL logic is intended to be migrated into a separate, nested BigQuery stored procedure.

## 2. Generated Artifacts

The following BigQuery SQL files were generated as part of this migration:

*   **`my_bq_dataset/ddl/job_tracking_table.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `job_tracking_table` in BigQuery. This table is designed to capture metadata about each execution of the migrated process, including job identifiers, key dates, record counts, and execution status. It replaces the functionality of the commented-out `FOSJobErzeugeEintrag` calls and provides a centralized logging mechanism.
*   **`my_bq_dataset/ddl/target_result_table.sql`**
    *   **Role**: Provides a placeholder DDL for the `target_result_table`. This table is expected to be the primary output destination for the data transformations performed by the core SQL logic (originally `d_ausd_bp_ta_cntrct_dist.sql`). Its schema needs to be fully defined based on the actual output of the original SQL script.
*   **`my_bq_dataset/sp/sp_d_ausd_bp_ta_cntrct_dist.sql`**
    *   **Role**: This is a placeholder BigQuery stored procedure intended to encapsulate the core data transformation and loading logic originally found in `d_ausd_bp_ta_cntrct_dist.sql`. It accepts parameters from the orchestrating procedure and is responsible for populating the `target_result_table`. The actual SQL logic from the source file needs to be fully implemented here.
*   **`my_bq_dataset/sp/sp_k_ausd_bp_ta_cntrct_dist.sql`**
    *   **Role**: This is the main BigQuery stored procedure that replicates the orchestration logic of the original `k_ausd_bp_ta_cntrct_dist.ksh` script. It handles parameter validation, date derivation, calls the `sp_d_ausd_bp_ta_cntrct_dist` procedure, retrieves the processed record count, and logs execution details into the `job_tracking_table`. It serves as the primary entry point for the migrated workflow.

## 3. Key Design Decisions

*   **KornShell to BigQuery Stored Procedures**: The entire orchestration logic of the original KornShell script has been directly translated into a BigQuery stored procedure (`sp_k_ausd_bp_ta_cntrct_dist`). This decision leverages BigQuery's native scripting capabilities, allowing for a serverless and scalable execution environment without external compute instances for orchestration.
*   **Nested Stored Procedures for Modularity**: The core SQL logic (`d_ausd_bp_ta_cntrct_dist.sql`) is designed to be migrated into a separate, nested BigQuery stored procedure (`sp_d_ausd_bp_ta_cntrct_dist`). This promotes modularity, reusability, and clearer separation of concerns between orchestration and data transformation.
*   **In-Procedure Logic for Utilities**: The functionality of various KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `gestern.ksh`) has been re-implemented directly within the BigQuery stored procedure using standard SQL functions and scripting constructs (e.g., `SAFE.PARSE_DATE`, `CURRENT_DATE()`, `DATE_SUB()`, `IF`, `ASSERT`, `ERROR()`). This eliminates external dependencies and simplifies deployment.
*   **Parameter Handling via SP Arguments**: All command-line parameters (`Jobkennung`, `EintragsNr`, `Stichtag`, `wiederanlaufWert`) are now passed as explicit arguments to the BigQuery stored procedures. This provides a clear and type-safe interface for invoking the process.
*   **Record Count from BigQuery Table**: The original method of reading a record count from a temporary file has been replaced by a direct `SELECT COUNT(*)` query on the target BigQuery table (`target_result_table`). This is a more robust and BigQuery-native approach.
*   **Dedicated Job Tracking Table**: The commented-out job tracking functionality (`FOSJobErzeugeEintrag`) has been replaced by explicit `INSERT` statements into a new `job_tracking_table`. This provides a structured, queryable log of all job executions within BigQuery.
*   **Error Handling with `RAISE` and `EXCEPTION`**: BigQuery's `EXCEPTION WHEN ERROR` block is used to catch and log errors, providing a structured way to manage failures and record error messages in the `job_tracking_table`.

**Notable Trade-offs**:
*   **Loss of Shell Scripting Flexibility**: Direct file system operations, complex text processing (like `sed`, `sort`, `join` for the commented-out post-processing), and arbitrary external command execution are no longer directly available within BigQuery SQL. If such functionality is required, it would need to be re-architected using BigQuery-native SQL transformations, Cloud Functions, or external orchestration tools like Cloud Composer.
*   **Dependency on BigQuery Ecosystem**: The solution is now tightly coupled with BigQuery, which is beneficial for data processing but might require re-evaluation if the core data platform changes.

## 4. Manual Steps Before Go-Live

Before the migrated process can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `my_bq_dataset` exists in your GCP project (`my_gcp_project`). If not, create it:
        ```bash
        bq mk --location=US my_gcp_project:my_bq_dataset
        ```
        (Adjust location as necessary).
2.  **Schema/Table Creation**:
    *   Execute the DDL scripts to create the necessary tables:
        *   `my_bq_dataset/ddl/job_tracking_table.sql`
        *   `my_bq_dataset/ddl/target_result_table.sql` (Ensure this DDL is fully defined based on the actual output of the core SQL logic).
3.  **IAM/Permissions**:
    *   The service account or user identity that will execute the `sp_k_ausd_bp_ta_cntrct_dist` stored procedure must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on `my_bq_dataset` (to insert into `job_tracking_table` and for `sp_d_ausd_bp_ta_cntrct_dist` to write to `target_result_table`).
        *   `BigQuery Data Viewer` on any source tables that `sp_d_ausd_bp_ta_cntrct_dist` reads from.
        *   `BigQuery Job User` (to run queries and stored procedures).
4.  **Connection Strings/Secrets**:
    *   No explicit connection strings or secrets are required for BigQuery stored procedures themselves, as they run natively within BigQuery.
    *   If an external orchestrator (e.g., Cloud Composer, Dataform) is used, ensure its service account has the necessary BigQuery permissions as listed above.
5.  **Scheduling**:
    *   If using Cloud Composer (Apache Airflow), a DAG needs to be developed and deployed to schedule and invoke `my_gcp_project.my_bq_dataset.sp_k_ausd_bp_ta_cntrct_dist` with the appropriate parameters.
    *   If using Dataform, a Dataform job definition needs to be created to call the stored procedure.
    *   Alternatively, the stored procedure can be scheduled directly via Cloud Scheduler and a Cloud Function trigger, or manually executed.
6.  **Core SQL Logic Implementation**:
    *   **Crucially**, the placeholder logic within `my_bq_dataset/sp/sp_d_ausd_bp_ta_cntrct_dist.sql` must be replaced with the actual, fully migrated SQL logic from the original `d_ausd_bp_ta_cntrct_dist.sql` file. This includes defining the correct source tables, transformations, and `INSERT` statements into `target_result_table`.
7.  **Source Data Availability**:
    *   Ensure all source tables required by `sp_d_ausd_bp_ta_cntrct_dist` are available in BigQuery and contain the necessary data.

## 5. Known Gaps & Unresolved References

*   **Core SQL Logic (B4 Item)**: The `sp_d_ausd_bp_ta_cntrct_dist.sql` procedure currently contains placeholder logic. The complete and validated SQL transformation logic from the original `d_ausd_bp_ta_cntrct_dist.sql` needs to be fully implemented and tested. This is a critical B4 item.
*   **`target_result_table` DDL (B4 Item)**: The DDL for `target_result_table` is generic. Its schema must be precisely defined based on the output columns and data types of the fully migrated `sp_d_ausd_bp_ta_cntrct_dist` procedure.
*   **Commented-Out Code Review**: The original KornShell script contained commented-out sections for file-based post-processing (`sed`, `sort`, `join`) and job deactivation (`FOSJobDeaktivate`). A stakeholder review is required to confirm if this functionality is truly obsolete or if it needs to be re-implemented in BigQuery (e.g., via SQL transformations on staging tables or external processing).
*   **`p_wiederanlaufWert` Usage**: The `p_wiederanlaufWert` parameter is passed through but its specific usage within the original `d_ausd_bp_ta_cntrct_dist.sql` or any other part of the workflow was not clear from the provided script. Its intended purpose needs to be clarified and implemented if relevant.
*   **`starteSQLSkript` Semantics**: While `CALL` replaces the execution, any complex logic within the original `h_alis_sqlplus.ksh`'s `starteSQLSkript` function (e.g., specific error handling, transaction management, retry mechanisms beyond simple execution) would need to be explicitly replicated or handled by the orchestrator.
*   **Helper Script Details**: The full implementation details of the original helper scripts (e.g., specific error codes, logging formats, date calculations beyond simple yesterday/today) were not fully analyzed. Any subtle business logic embedded in these scripts should be reviewed and replicated if necessary.

## 6. Validation

To validate the migrated BigQuery stored procedures:

1.  **Execute the Main Stored Procedure**:
    *   Open the BigQuery console or use the `bq query` command-line tool.
    *   Call `sp_k_ausd_bp_ta_cntrct_dist` with example parameters.
    *   **Example Call**:
        ```sql
        CALL `my_gcp_project.my_bq_dataset.sp_k_ausd_bp_ta_cntrct_dist`(
            'JOB_AUSD_BP_TA_CNTRCT',
            'ENTRY_001',
            '01012023', -- Stichtag in DDMMYYYY format
            'N'         -- Example wiederanlaufWert
        );
        ```

2.  **Verify `job_tracking_table`**:
    *   Query the `my_gcp_project.my_bq_dataset.job_tracking_table` to check the latest entry.
    *   **Passing Criteria**:
        *   A new row should exist for the executed job.
        *   `status` should be 'SUCCESS'.
        *   `record_count` should reflect the number of records processed/inserted by `sp_d_ausd_bp_ta_cntrct_dist` for the given `Stichtag`.
        *   `start_timestamp` and `end_timestamp` should be populated.
        *   `error_message` should be NULL.

3.  **Verify `target_result_table`**:
    *   Query `my_gcp_project.my_bq_dataset.target_result_table` to inspect the data generated.
    *   **Passing Criteria**:
        *   The table should contain the expected transformed data for the `Stichtag` provided.
        *   The data should match the output of the original `d_ausd_bp_ta_cntrct_dist.sql` script when run with the same inputs.
        *   The number of records should match the `record_count` logged in `job_tracking_table`.

4.  **Error Scenario Testing**:
    *   Call the stored procedure with invalid parameters (e.g., missing `Jobkennung`, invalid `Stichtag` format).
    *   **Passing Criteria**:
        *   The `job_tracking_table` should record an entry with `status` as 'FAILED'.
        *   `error_message` should contain a descriptive error message indicating the validation failure.
        *   No unexpected data should be written to `target_result_table`.

## 7. Rollback Procedure

In case of issues or a decision to revert the migration, follow these steps:

1.  **Stop New Executions**:
    *   If using Cloud Composer or Dataform, disable or delete the DAG/job that calls `sp_k_ausd_bp_ta_cntrct_dist`.
    *   Ensure no manual executions of the BigQuery stored procedure are initiated.
2.  **Revert BigQuery Objects**:
    *   **Delete Stored Procedures**:
        ```sql
        DROP PROCEDURE IF EXISTS `my_gcp_project.my_bq_dataset.sp_k_ausd_bp_ta_cntrct_dist`;
        DROP PROCEDURE IF EXISTS `my_gcp_project.my_bq_dataset.sp_d_ausd_bp_ta_cntrct_dist`;
        ```
    *   **Revert Tables (Optional, depending on impact)**:
        *   If the `target_result_table` was exclusively populated by the new process and its data is not needed, it can be truncated or dropped. If it's shared or contains historical data, a point-in-time recovery might be necessary if data was corrupted.
        *   The `job_tracking_table` can be retained for audit purposes or dropped if not needed.
        ```sql
        -- TRUNCATE TABLE `my_gcp_project.my_bq_dataset.target_result_table`;
        -- DROP TABLE IF EXISTS `my_gcp_project.my_bq_dataset.target_result_table`;
        -- DROP TABLE IF EXISTS `my_gcp_project.my_bq_dataset.job_tracking_table`;
        ```
3.  **Reactivate Original Process**:
    *   Ensure the original `k_ausd_bp_ta_cntrct_dist.ksh` script and its dependencies are in place and functional.
    *   Re-enable any legacy scheduling mechanisms (e.g., cron jobs) for the KornShell script.
4.  **Data Reconciliation**:
    *   Verify that the original process can resume data processing correctly.
    *   If any data was written by the BigQuery process, ensure it is either cleaned up or reconciled with the data produced by the original system to maintain data consistency.