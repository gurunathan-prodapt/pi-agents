# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_bp_ref.ksh` KornShell script and its associated Oracle SQL script `d_ausd_v_ta_bp_ref.sql`. The original job processes and updates business partner reference data (`ta_bp_ref`) by reading from `dwtk_meldungen` and `cds$ta_bp_ref` to populate `sof$ta_bp_ref` and perform a merge operation on `VIA`.

The migration involved converting the KornShell orchestration logic and Oracle SQL data processing logic into Google BigQuery Stored Procedures and BigQuery SQL. The target platform is Google BigQuery, leveraging its native capabilities for data storage, processing, and orchestration.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`ddl/dwtk_meldungen.sql`**:
    *   **Role**: Defines the Data Definition Language (DDL) for the `dwtk_meldungen` table in BigQuery. This table stores metadata about job executions, including timestamps used for cutoff date determination.
*   **`ddl/cds_ta_bp_ref.sql`**:
    *   **Role**: Defines the DDL for the `cds_ta_bp_ref` table in BigQuery. This table holds the source business partner reference data. Note: The `$` in the original Oracle table name `CDS$TA_BP_REF` has been replaced with `_` for BigQuery compatibility.
*   **`ddl/sof_ta_bp_ref.sql`**:
    *   **Role**: Defines the DDL for the `sof_ta_bp_ref` table in BigQuery. This table serves as an intermediate or target table for processed business partner reference data. Note: The `$` in the original Oracle table name `SOF$TA_BP_REF` has been replaced with `_` for BigQuery compatibility.
*   **`ddl/VIA.sql`**:
    *   **Role**: Defines the DDL for the `VIA` table in BigQuery. This table is the ultimate target for a `MERGE` operation, integrating the processed business partner reference data. The DDL is a placeholder and should be refined based on the actual schema of the original `VIA` table.
*   **`ddl/job_control_table.sql`**:
    *   **Role**: Defines the DDL for the `job_control_table` in BigQuery. This table replaces the legacy shell script's job registration and status tracking mechanism, storing job execution details, status, and metrics.
*   **`ddl/job_error_log.sql`**:
    *   **Role**: Defines the DDL for the `job_error_log` table in BigQuery. This table centralizes error and informational logging, replacing the shell script's file-based logging and `f_alis_msgerr.ksh` utility.
*   **`stored_procedures/sp_d_ausd_v_ta_bp_ref.sql`**:
    *   **Role**: Contains the BigQuery Stored Procedure `sp_d_ausd_v_ta_bp_ref`. This procedure encapsulates the core data processing logic, including truncating `sof_ta_bp_ref`, inserting filtered data from `cds_ta_bp_ref`, and performing the `MERGE` operation on `VIA`. It replaces the `d_ausd_v_ta_bp_ref.sql` Oracle script.
*   **`stored_procedures/sp_ausd_v_ta_bp_ref.sql`**:
    *   **Role**: Contains the BigQuery Stored Procedure `sp_ausd_v_ta_bp_ref`. This procedure acts as the main orchestrator, replacing the `k_ausd_v_ta_bp_ref.ksh` KornShell script. It handles parameter validation, job control table updates, cutoff date determination, invocation of the data processing procedure, and comprehensive error handling and logging.

## 3. Key design decisions

*   **Orchestration via BigQuery Stored Procedures**: The KornShell script's role in parameter parsing, job registration, error handling, and sequential execution was fully migrated to a BigQuery Stored Procedure (`sp_ausd_v_ta_bp_ref`). This centralizes job control within BigQuery, eliminating external script dependencies and leveraging BigQuery's native procedural capabilities.
*   **Data Processing within BigQuery Stored Procedures**: The Oracle SQL logic from `d_ausd_v_ta_bp_ref.sql` was translated into BigQuery SQL and encapsulated within a dedicated BigQuery Stored Procedure (`sp_d_ausd_v_ta_bp_ref`). This promotes modularity, reusability, and allows for fine-grained error handling and transaction management.
*   **Dedicated Job Control and Error Logging Tables**: Instead of relying on filesystem-based logging and custom shell utilities, two BigQuery tables (`job_control_table` and `job_error_log`) were introduced. These tables provide a structured, queryable, and centralized mechanism for tracking job status, metrics, and errors, aligning with cloud-native best practices.
*   **BigQuery-Native Syntax and Functions**:
    *   Oracle-specific date functions (`TO_CHAR`, `TO_DATE`) were replaced with BigQuery equivalents (`FORMAT_DATE`, `PARSE_DATE`).
    *   `NVL` was replaced with `COALESCE`.
    *   `TRUNCATE TABLE` was directly translated.
    *   `SQL*Plus` directives (`DEFINE`, `SPOOL`, `WHENEVER SQLERROR`, `SET SERVEROUTPUT ON`, etc.) were removed as they are client-side commands irrelevant in a BigQuery Stored Procedure context. Their functionalities were absorbed by BigQuery's procedural language (e.g., `DECLARE`, `BEGIN...END`, `RAISE`, `EXCEPTION WHEN ERROR`).
*   **Handling of Database Links**: The Oracle database link (`&v_carmen`) was rendered obsolete. All source and target tables (`dwtk_meldungen`, `cds_ta_bp_ref`, `sof_ta_bp_ref`, `VIA`) are assumed to be co-located within the same BigQuery project and dataset, simplifying data access and improving performance.
*   **Transaction Management**: The BigQuery Stored Procedures implement explicit `BEGIN TRANSACTION`, `COMMIT TRANSACTION`, and `ROLLBACK TRANSACTION` blocks. This ensures atomicity for job control table updates and data modifications, maintaining data integrity even in case of failures.
*   **Error Handling and Re-raising**: Comprehensive `EXCEPTION WHEN ERROR` blocks are used within the stored procedures to catch errors, log them to `job_error_log`, update the `job_control_table` with a `FAILED` status, and re-raise the error to the caller for external monitoring.
*   **Placeholder for `DWPA_UTIL_SKRIPT.runstatement` and `VIA` MERGE**: The exact logic for `DWPA_UTIL_SKRIPT.runstatement` (assumed to be `TRUNCATE TABLE`) and the full `MERGE` statement for the `VIA` table were not fully detailed in the source. Placeholders have been included, requiring further refinement based on the original Oracle implementation.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Setup**:
    *   Ensure a Google Cloud Project is set up.
    *   Create the target BigQuery dataset (e.g., `my_dataset`) where all tables and stored procedures will reside.
2.  **Schema Creation**:
    *   Execute the DDL scripts for all tables in the specified BigQuery dataset:
        *   `ddl/dwtk_meldungen.sql`
        *   `ddl/cds_ta_bp_ref.sql`
        *   `ddl/sof_ta_bp_ref.sql`
        *   `ddl/VIA.sql` (Review and refine the DDL for `VIA` based on its actual schema in Oracle)
        *   `ddl/job_control_table.sql`
        *   `ddl/job_error_log.sql`
3.  **Initial Data Loading**:
    *   Perform a one-time historical data load from the Oracle source tables (`dwtk_meldungen`, `cds$ta_bp_ref`, `sof$ta_bp_ref`, `VIA`) into their respective BigQuery tables. This can be done using:
        *   BigQuery Data Transfer Service (for supported sources).
        *   Cloud Data Fusion or other ETL tools.
        *   Exporting data from Oracle to Cloud Storage and then loading into BigQuery.
    *   Establish an incremental data loading strategy for `dwtk_meldungen` and `cds_ta_bp_ref` if they are continuously updated in the source Oracle system and need to be synchronized with BigQuery.
4.  **IAM and Permissions**:
    *   Grant appropriate BigQuery IAM roles to the service account or user that will execute the stored procedures and scheduled queries. This typically includes `BigQuery Data Editor` for the dataset containing the tables and `BigQuery Job User` for running queries.
5.  **Stored Procedure Deployment**:
    *   Execute the `CREATE OR REPLACE PROCEDURE` statements for both stored procedures in the target BigQuery dataset:
        *   `stored_procedures/sp_d_ausd_v_ta_bp_ref.sql`
        *   `stored_procedures/sp_ausd_v_ta_bp_ref.sql`
6.  **Scheduling Configuration**:
    *   **BigQuery Scheduled Queries**: If using BigQuery's native scheduler, configure a scheduled query to execute `CALL \`my_project.my_dataset.sp_ausd_v_ta_bp_ref\`(p_JobKennung => 'YOUR_JOB_ID', p_EintragsNr => YOUR_ENTRY_NUMBER);`
        *   Define the schedule (e.g., daily, hourly).
        *   Specify the destination dataset for query results (can be temporary).
        *   Ensure the service account running the scheduled query has the necessary BigQuery permissions.
    *   **Cloud Composer (Airflow)**: If more complex orchestration or external dependencies are required, create a Cloud Composer DAG that calls the BigQuery Stored Procedure.
7.  **Parameter Configuration**:
    *   Determine the appropriate values for `p_JobKennung` and `p_EintragsNr` for the scheduled execution. These parameters are crucial for job tracking and logging.

## 5. Known gaps & unresolved references

The following items were identified during the migration design and require further attention or confirmation:

*   **`r_ausd_vertrag.ksh` reference**: The original KornShell script's comments indicate it might be a control script for `r_ausd_vertrag.ksh`. While no direct invocation was found, a complete understanding of `r_ausd_vertrag.ksh`'s role in the broader workflow is recommended to ensure no implicit dependencies are missed.
*   **`DWPA_UTIL_SKRIPT.runstatement` functionality**: The exact behavior and parameters of this Oracle package procedure, particularly its side effects, need thorough verification. The migration assumes its primary action in this context is equivalent to `TRUNCATE TABLE`, but this should be confirmed.
*   **`VIA` table `MERGE` operation details**: The full `MERGE` statement for the `VIA` table was not available in the provided `d_ausd_v_ta_bp_ref.sql`. The generated `sp_d_ausd_v_ta_bp_ref.sql` contains a placeholder `MERGE` statement. The precise `ON` clause, `WHEN MATCHED` (UPDATE) logic, and `WHEN NOT MATCHED` (INSERT) logic must be extracted from the original Oracle implementation and incorporated into the BigQuery Stored Procedure.
*   **"Deactivate older active jobs" logic**: The business rules defining what constitutes an "active job" and the exact conditions for deactivation (e.g., based on `job_kennung` only, or also `entry_nr` or other criteria) should be explicitly confirmed to ensure the `UPDATE` statement in `sp_ausd_v_ta_bp_ref` accurately reflects the desired behavior.
*   **`v_carmen` DB Link**: While the migration consolidates data into BigQuery, the original use of `&v_carmen` for `cds$ta_bp_ref` suggests `cds$ta_bp_ref` might have been on a separate Oracle instance. This is a non-issue for BigQuery once data is consolidated, but it's a historical context point.

## 6. Validation

Validation of the migrated job involves ensuring functional equivalence and data integrity.

1.  **Unit Testing `sp_d_ausd_v_ta_bp_ref`**:
    *   **Setup**: Populate `my_project.my_dataset.dwtk_meldungen` and `my_project.my_dataset.cds_ta_bp_ref` with representative test data, including edge cases (e.g., `NULL` values for `modified_at`, `valid_to`, various date ranges).
    *   **Execution**: Call the stored procedure directly:
        ```sql
        CALL `my_project.my_dataset.sp_d_ausd_v_ta_bp_ref`(
          v_datum => DATE '2023-01-01', -- Use a specific test date
          p_EintragsNr => 999,
          p_JobKennung => 'TEST_DP_JOB',
          processed_records => @record_count
        );
        SELECT @record_count;
        ```
    *   **Verification**:
        *   Query `my_project.my_dataset.sof_ta_bp_ref` to verify that the correct data has been inserted according to the filtering logic.
        *   Query `my_project.my_dataset.VIA` to verify the `MERGE` operation's outcome (inserts and updates).
        *   Compare the record counts and data content with the expected output from the legacy Oracle SQL script run against the same test data.
        *   Verify error logging in `job_error_log` for negative test cases.

2.  **Unit Testing `sp_ausd_v_ta_bp_ref`**:
    *   **Setup**: Ensure `job_control_table` is empty or contains known states.
    *   **Execution**: Call the main orchestration procedure with various parameters:
        ```sql
        -- Valid execution
        CALL `my_project.my_dataset.sp_ausd_v_ta_bp_ref`(p_JobKennung => 'VALID_JOB_01', p_EintragsNr => 1);
        -- Invalid parameters (expect failure)
        -- CALL `my_project.my_dataset.sp_ausd_v_ta_bp_ref`(p_JobKennung => NULL, p_EintragsNr => 2);
        -- CALL `my_project.my_dataset.sp_ausd_v_ta_bp_ref`(p_JobKennung => 'JOB_02', p_EintragsNr => NULL);
        ```
    *   **Verification**:
        *   Query `my_project.my_dataset.job_control_table` to verify:
            *   Job registration (`RUNNING` status at start, `SUCCESS` or `FAILED` at end).
            *   Correct `start_time`, `end_time`, `records_processed`.
            *   Older active jobs are correctly `DEACTIVATED`.
        *   Query `my_project.my_dataset.job_error_log` to verify error messages are logged for failed executions.
        *   Verify the `v_datum` (cutoff date) is correctly determined from `dwtk_meldungen`.

3.  **End-to-End Integration Testing**:
    *   **Setup**: Use a dedicated test environment with representative data in all BigQuery tables.
    *   **Execution**: Run the `sp_ausd_v_ta_bp_ref` procedure as it would be scheduled.
    *   **Verification**:
        *   Compare the final state of `my_project.my_dataset.sof_ta_bp_ref` and `my_project.my_dataset.VIA` with the output of the legacy system for the same input data and execution parameters.
        *   Check `job_control_table` and `job_error_log` for complete and accurate logging.
        *   Monitor BigQuery job history for successful completion and resource usage.

**"Passing" means**:
*   The `sp_ausd_v_ta_bp_ref` procedure completes without unhandled errors.
*   The `job_control_table` accurately reflects the job's status (`SUCCESS`), start/end times, and `records_processed`.
*   The data in `my_project.my_dataset.sof_ta_bp_ref` and `my_project.my_dataset.VIA` is identical to the output produced by the legacy Oracle job when executed with the same input data.
*   Error conditions are correctly caught, logged to `job_error_log`, and result in a `FAILED` status in `job_control_table`.

## 7. Rollback procedure

In case of critical issues during or after go-live, the following steps outline the procedure to roll back to the legacy system:

1.  **Stop BigQuery Scheduled Queries/Cloud Composer DAGs**: Immediately disable or delete the BigQuery Scheduled Query or Cloud Composer DAG that invokes `sp_ausd_v_ta_bp_ref` to prevent further execution of the migrated job.
2.  **Re-enable Legacy Scheduling**: Reactivate the original scheduling mechanism for `k_ausd_v_ta_bp_ref.ksh` (e.g., cron job, enterprise scheduler).
3.  **Data Restoration (if necessary)**:
    *   If the migrated job made irreversible changes to shared target tables (e.g., `VIA`) that cannot be easily undone, restore these tables in the Oracle environment from a recent backup taken *before* the BigQuery job's first production run.
    *   For `sof$ta_bp_ref`, as it's truncated and re-inserted, the impact might be limited to the current run.
4.  **Verify Legacy System Operation**: Monitor the re-enabled legacy job to ensure it runs successfully and produces the expected output.
5.  **Post-Rollback Analysis**: Investigate the root cause of the rollback. This may involve reviewing BigQuery job logs, `job_error_log` entries, and comparing data states to identify the discrepancy or error.
6.  **Cleanup (Optional, after successful rollback)**:
    *   The BigQuery tables and stored procedures can be retained for debugging and future re-migration attempts.
    *   If a clean slate is desired, the BigQuery stored procedures and tables created during the migration can be dropped.

**Note**: A robust rollback strategy heavily relies on having recent, reliable backups of the target Oracle tables and a clear understanding of the impact of the migrated job's operations.