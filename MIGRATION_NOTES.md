# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh` has been migrated. This script, originally responsible for job control, parameter parsing, and orchestrating the execution of an underlying SQL script (`d_ausd_v_ta_cntrct_crs3.sql`), has been re-implemented in Google BigQuery.

The target platform is Google BigQuery, leveraging BigQuery Stored Procedures for orchestration and control logic, and BigQuery Standard SQL for the core business logic. The migration replaces shell-based job management, error handling, and parameter parsing with native BigQuery constructs and dedicated logging tables.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL files:

*   **`ddl/job_table.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `job_table`. This table is central to managing the state (active/inactive) and history of ETL jobs within BigQuery, replacing the implicit job management logic of the original KornShell script.
*   **`ddl/error_log.sql`**
    *   **Role**: Defines the DDL for the `error_log` table. This table is used to capture and store detailed error messages, error numbers, and contextual information from BigQuery stored procedure executions, replacing the functionality of `f_alis_msgerr.ksh`.
*   **`ddl/execution_log.sql`**
    *   **Role**: Defines the DDL for the `execution_log` table. This table records key details of each stored procedure execution, including job identifiers, processed record counts, and final status, replacing the temporary file-based record counting and providing comprehensive execution history.
*   **`ddl/ta_cntrct_crs3.sql`**
    *   **Role**: Provides a placeholder DDL for the `ta_cntrct_crs3` table. This table is identified as the primary target for the core business logic originally contained in `d_ausd_v_ta_cntrct_crs3.sql`. **Note: This DDL is a placeholder and MUST be updated with the actual schema of your `ta_cntrct_crs3` table.**
*   **`sprocs/log_error.sql`**
    *   **Role**: Defines a helper BigQuery Stored Procedure, `log_error`. This procedure encapsulates the logic for inserting error details into the `error_log` table, promoting reusability and consistent error reporting.
*   **`sprocs/log_execution.sql`**
    *   **Role**: Defines a helper BigQuery Stored Procedure, `log_execution`. This procedure encapsulates the logic for inserting execution details into the `execution_log` table, ensuring consistent logging of job status and metrics.
*   **`sql/d_ausd_v_ta_cntrct_crs3_placeholder.sql`**
    *   **Role**: This file is a **placeholder** for the core business logic. It represents the content that was originally in `d_ausd_v_ta_cntrct_crs3.sql`. **This file MUST be replaced with the actual BigQuery Standard SQL translation of your business logic.** The main stored procedure expects to capture the total number of records processed from this logic.
*   **`sprocs/sp_ausd_v_ta_cntrct_crs3.sql`**
    *   **Role**: This is the main BigQuery Stored Procedure, `sp_ausd_v_ta_cntrct_crs3`. It re-implements the entire control and orchestration logic of the original `k_ausd_v_ta_cntrct_crs3.ksh` script. This includes parameter validation, job activation/deactivation using `job_table`, calling the core business SQL logic (from the placeholder), and comprehensive error and execution logging.

## 3. Key Design Decisions

*   **Target Platform: Google BigQuery Stored Procedures**: The decision to migrate to BigQuery Stored Procedures was made to leverage BigQuery's native capabilities for data processing and orchestration. This consolidates the logic within the data warehouse environment, eliminating external shell script dependencies and benefiting from BigQuery's scalability and performance.
*   **Dedicated Job Control, Error, and Execution Logging Tables**: Instead of relying on implicit shell script logic, temporary files, or external utility scripts, dedicated BigQuery tables (`job_table`, `error_log`, `execution_log`) were designed. This provides structured, queryable, and persistent records of job status, errors, and execution metrics, significantly improving observability and maintainability.
*   **Direct Parameter Passing**: The original `getopts` shell parsing is replaced by direct parameter passing to the BigQuery Stored Procedure (`p_JobKennung`, `p_EintragsNr`). This simplifies the interface and leverages BigQuery's native parameter handling.
*   **Elimination of Temporary Files**: The shell script's use of temporary files (e.g., for `v_records`) is replaced by BigQuery `DECLARE` variables and direct `COUNT(*)` operations within the stored procedure, streamlining the data flow and reducing I/O overhead.
*   **Core SQL Logic as an Inline/Called Component**: The business logic from `d_ausd_v_ta_cntrct_crs3.sql` is intended to be translated into BigQuery Standard SQL and either inlined directly into `sp_ausd_v_ta_cntrct_crs3` or called as a separate BigQuery routine. This approach keeps the core data transformation logic close to the orchestration.
*   **Standardized Error Handling**: The legacy `f_alis_msgerr.ksh` is replaced by a structured `EXCEPTION WHEN ERROR` block within the BigQuery Stored Procedure, which logs details to `error_log` and re-raises the error, providing a consistent and robust error management mechanism.

**Notable Trade-offs:**

*   **Dependency on BigQuery Ecosystem**: The solution is now tightly coupled with BigQuery, which might limit portability to other data warehousing platforms.
*   **Manual Core SQL Translation**: The most significant trade-off is the necessity for manual translation of the `d_ausd_v_ta_cntrct_crs3.sql` content to BigQuery Standard SQL, as the original SQL was not provided. This requires careful review and testing.
*   **Debugging Complexity**: While BigQuery Stored Procedures offer robust logging, debugging complex SQL logic within a stored procedure can sometimes be more involved than debugging a simple shell script or standalone SQL file.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Project and Dataset Setup**:
    *   Ensure the target BigQuery project (`your_project_id`) and dataset (`your_dataset_id`) exist. If not, create them.
2.  **Deploy DDLs for Control Tables**:
    *   Execute `ddl/job_table.sql` to create the `job_table`.
    *   Execute `ddl/error_log.sql` to create the `error_log` table.
    *   Execute `ddl/execution_log.sql` to create the `execution_log` table.
3.  **Define `ta_cntrct_crs3` Table Schema**:
    *   **CRITICAL**: Review and update `ddl/ta_cntrct_crs3.sql` with the **actual and complete schema** of your `ta_cntrct_crs3` table. This placeholder DDL must be replaced with the correct column definitions, data types, and constraints. Then, execute the updated DDL.
4.  **Translate and Integrate Core SQL Logic**:
    *   **CRITICAL**: Manually translate the content of the original `d_ausd_v_ta_cntrct_crs3.sql` script into BigQuery Standard SQL.
    *   Replace the placeholder content within `sql/d_ausd_v_ta_cntrct_crs3_placeholder.sql` with your translated BigQuery SQL.
    *   Decide whether to inline this SQL directly into `sprocs/sp_ausd_v_ta_cntrct_crs3.sql` or to create a separate BigQuery routine (e.g., another stored procedure or script) and call it from the main procedure. The current `sp_ausd_v_ta_cntrct_crs3.sql` is structured to easily accept inlined SQL. Ensure that the logic correctly calculates and returns the `v_records_processed_total`.
5.  **Deploy Helper Stored Procedures**:
    *   Execute `sprocs/log_error.sql` to create or replace the `log_error` procedure.
    *   Execute `sprocs/log_execution.sql` to create or replace the `log_execution` procedure.
6.  **Deploy Main Stored Procedure**:
    *   Execute `sprocs/sp_ausd_v_ta_cntrct_crs3.sql` to create or replace the main stored procedure.
7.  **IAM/Permissions**:
    *   Ensure the service account or user identity that will execute the BigQuery stored procedure has appropriate IAM roles. At a minimum, it will need `BigQuery Data Editor` on the dataset containing the tables and stored procedures, and potentially `BigQuery Job User` to run queries.
8.  **Connection Strings/Secrets**:
    *   No direct connection strings or secrets are managed within the BigQuery stored procedure itself. However, if an external orchestrator (e.g., Cloud Composer/Airflow) is used to call the stored procedure, ensure it is configured with the necessary BigQuery connection details and credentials.
9.  **Scheduling**:
    *   Configure your chosen orchestrator (e.g., Cloud Composer, Cloud Scheduler, Dataform, or a custom solution) to call the `sp_ausd_v_ta_cntrct_crs3` stored procedure with the required `p_JobKennung` and `p_EintragsNr` parameters.
    *   Example call: `CALL your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3('YOUR_JOB_ID', 'YOUR_ENTRY_NR');`
10. **Initial `job_table` State (Optional)**:
    *   If there are existing job states from the legacy system that need to be carried over, populate the `your_project_id.your_dataset_id.job_table` with initial records.

## 5. Known Gaps & Unresolved References

*   **Core SQL Logic (`d_ausd_v_ta_cntrct_crs3.sql`)**: The content of this critical SQL script was not provided in the migration design. Its translation to BigQuery Standard SQL and integration into `sp_ausd_v_ta_cntrct_crs3` is the most significant outstanding task. The complexity and specific dialect of the original SQL will dictate the effort required.
*   **`ta_cntrct_crs3` Table Schema**: The DDL for `ta_cntrct_crs3` is a placeholder. Its actual schema needs to be defined and deployed.
*   **Legacy Error Number Mapping**: The error numbers (192, 193) used in the BigQuery stored procedure are based on the legacy script's examples. A comprehensive mapping of all potential legacy error codes to BigQuery-compatible error numbers or a standardized error classification might be beneficial.
*   **`r_ausd_vertrag.ksh` Context**: The design document notes that `k_ausd_v_ta_cntrct_crs3.ksh` is a control script *for* `r_ausd_vertrag.ksh`. The full context and interaction with `r_ausd_vertrag.ksh` are not fully detailed. If `r_ausd_vertrag.ksh` is part of the same job stream, its migration or integration with the new BigQuery stored procedure will need to be addressed.
*   **Missing Complexity/Automation Data**: The original script's complexity tier and automation rate were unavailable. This means the migration effort was based on manual analysis rather than automated assessment.
*   **Data Migration for `job_table`**: If the legacy system maintained a persistent job state, a data migration strategy for populating the new `job_table` with historical or current job statuses might be required.

## 6. Validation

Validation involves ensuring the migrated BigQuery stored procedure functions correctly and produces accurate results.

**How to Run Tests:**

1.  **Unit Testing (BigQuery Console/Client)**:
    *   Execute the main stored procedure directly from the BigQuery console or a BigQuery client (e.g., `bq` CLI, Python client library).
    *   **Valid Parameters**: `CALL your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3('TEST_JOB_1', 'ENTRY_001');`
    *   **Invalid Parameters**: Test with `NULL` or empty strings for `p_JobKennung` and `p_EintragsNr` to verify error handling.
    *   **Already Active Job**: Manually set `active_flag = TRUE` for a job in `job_table` and then attempt to run the SP with the same parameters to verify the "SKIPPED_ALREADY_ACTIVE" path.
2.  **Integration Testing (Orchestrator)**:
    *   If an orchestrator (e.g., Airflow) is in place, configure a test DAG/job to call the BigQuery stored procedure. This tests the end-to-end flow, including parameter passing from the orchestrator.
3.  **Data Validation**:
    *   Run the original `k_ausd_v_ta_cntrct_crs3.ksh` script in the legacy environment for a specific set of input data.
    *   Run the migrated `sp_ausd_v_ta_cntrct_crs3` with the equivalent input parameters.
    *   Compare the resulting data in the target tables (e.g., `ta_cntrct_crs3`) between the legacy and BigQuery environments. This is crucial for ensuring data accuracy.

**What "Passing" Means:**

*   **Successful Execution**: The `sp_ausd_v_ta_cntrct_crs3` stored procedure completes without unhandled errors.
*   **Correct Job Status in `job_table`**:
    *   Before execution, if the job is not present, it should be inserted with `active_flag = TRUE`.
    *   If the job exists, `active_flag` should be updated to `TRUE`.
    *   Upon successful completion, `active_flag` should be `FALSE`, `updated_ts` and `completed_ts` should be updated.
*   **Accurate `execution_log` Entries**:
    *   A `SUCCESS` entry should be present in `execution_log` with the correct `job_name`, `entry_nr`, `tab_name`, and `records_processed`.
    *   For skipped runs, a `SKIPPED_ALREADY_ACTIVE` entry should be present.
*   **Correct `error_log` Entries (for error scenarios)**:
    *   When invalid parameters are provided or an error occurs during core logic execution, an entry should be logged in `error_log` with relevant `procedure_name`, `err_nr`, `err_arg`, and `message`.
    *   The stored procedure should `RAISE` an error, indicating failure to the caller.
*   **Data Consistency**: The data in the target tables (e.g., `ta_cntrct_crs3`) after the BigQuery stored procedure runs must be identical to the data produced by the legacy script for the same input. This includes record counts, specific column values, and overall data integrity.
*   **Performance**: The BigQuery stored procedure should complete within acceptable performance thresholds, ideally matching or exceeding the legacy script's performance.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Halt New Executions**: Immediately stop any scheduled or manual executions of the `sp_ausd_v_ta_cntrct_crs3` stored procedure from your orchestrator (e.g., disable the Airflow DAG, remove Cloud Scheduler jobs).
2.  **Reactivate Legacy Job**: Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh` script in the legacy environment.
3.  **Revert BigQuery Objects (Optional, but Recommended for Cleanliness)**:
    *   **Drop Stored Procedures**:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`;
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.log_error`;
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.log_execution`;
        ```
    *   **Drop Control Tables**:
        ```sql
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_table`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.error_log`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.execution_log`;
        ```
    *   **Data Rollback for `ta_cntrct_crs3`**: This is the most critical and complex part.
        *   If the core SQL logic performed `INSERT` operations, you might need to delete the newly inserted records.
        *   If it performed `UPDATE` or `DELETE` operations, you might need to restore the table to a previous state.
        *   **BigQuery Time Travel**: For recent changes (within 7 days by default), you can use BigQuery's time travel feature to query the table as it was at a specific timestamp before the problematic execution.
            ```sql
            -- Example: Restore table to a state before a specific timestamp
            CREATE OR REPLACE TABLE `your_project_id.your_dataset_id.ta_cntrct_crs3` AS
            SELECT * FROM `your_project_id.your_dataset_id.ta_cntrct_crs3` FOR SYSTEM_TIME AS OF 'YYYY-MM-DD HH:MM:SS UTC';
            ```
        *   **Backup/Snapshot Restore**: If you have a robust backup strategy (e.g., daily snapshots), restore the `ta_cntrct_crs3` table from a backup taken before the migration.
        *   **Reverse Process**: In some cases, a specific "undo" script might be required if time travel or backups are not feasible or sufficient.
4.  **Monitor Legacy System**: Closely monitor the re-activated legacy job to ensure it functions as expected.

**Note**: The data rollback strategy for `ta_cntrct_crs3` is highly dependent on the specific DML operations performed by the core SQL logic. A detailed plan for data recovery should be established during the testing phase.