```markdown
# MIGRATION_NOTES.md for k_ausd_v_ta_action_assoc.ksh

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`. The original script served as a control mechanism, orchestrating the execution of a SQL script responsible for managing `ta_action_assoc` data, handling parameters, error logging, and job status tracking.

The job has been migrated to Google Cloud BigQuery. The KornShell control script's functionality has been refactored into a BigQuery Stored Procedure (`project.dataset.sp_ausd_v_ta_action_assoc`), which now encapsulates the control flow, parameter handling, and orchestration logic. The legacy job tracking and error logging mechanisms have been replaced by dedicated BigQuery control tables.

## 2. Generated Artifacts

The migration process generated the following BigQuery-specific artifacts:

*   **`project/dataset/ddl/job_control.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_control` table in BigQuery. This table is used to track the status, start/end timestamps, and parameters of job executions, replacing the implicit job tracking of the legacy KornShell script.
*   **`project/dataset/ddl/job_result.sql`**
    *   **Role:** Defines the DDL for the `job_result` table. This table stores the outcomes of job executions, such as record counts, which were previously captured via temporary files in the legacy system.
*   **`project/dataset/ddl/error_log.sql`**
    *   **Role:** Defines the DDL for the `error_log` table. This table centralizes error messages, codes, and context, replacing the console output and potentially file-based error logging of the original script.
*   **`project/dataset/ddl/ta_action_assoc.sql`**
    *   **Role:** Provides a placeholder DDL for the `ta_action_assoc` table. This table is the primary data entity manipulated by the core SQL logic. Its full schema needs to be finalized based on the analysis of the original `d_ausd_v_ta_action_assoc.sql` content.
*   **`project/dataset/sp_ausd_v_ta_action_assoc.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure that is the direct replacement for the `k_ausd_v_ta_action_assoc.ksh` script. It handles parameter validation, updates control tables, orchestrates the core data manipulation logic (derived from `d_ausd_v_ta_action_assoc.sql`), captures record counts, and manages error handling within the BigQuery environment.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Refactoring KornShell to BigQuery Stored Procedure**: The original KornShell script, acting as an orchestrator, was refactored into a BigQuery Stored Procedure. This decision centralizes the job's logic within the BigQuery environment, eliminating external shell dependencies, `sqlplus` calls, and simplifying deployment and execution management. It leverages BigQuery's native capabilities for procedural logic and data manipulation.
*   **Dedicated BigQuery Control Tables for Job Metadata**: Instead of relying on file-based logging, console output, and implicit job tracking, dedicated BigQuery tables (`job_control`, `job_result`, `error_log`) were introduced. This provides structured, queryable, and persistent storage for job status, execution results, and error details, significantly improving observability, auditing, and maintainability.
*   **Integration of Core SQL Logic via `EXECUTE IMMEDIATE`**: The downstream SQL script (`d_ausd_v_ta_action_assoc.sql`) invoked by the original KornShell script is intended to be either inlined or called as a separate BigQuery Stored Procedure within the main `sp_ausd_v_ta_action_assoc`. This approach integrates the data transformation directly into the BigQuery procedure, removing external file dependencies and streamlining the execution flow.
*   **Native BigQuery Parameter Handling and Error Reporting**: Shell script parameter validation and error handling (e.g., `getopts`, `echo`, `exit` codes) are replaced by BigQuery's native procedural constructs (`IF` statements, `ASSERT`, `RAISE`, `EXCEPTION WHEN ERROR`). This ensures robust validation and error propagation consistent with BigQuery's SQL dialect.
*   **Direct Record Count Capture**: The legacy method of writing record counts to a temporary file and then reading it back is replaced by directly querying the affected tables within the BigQuery Stored Procedure and storing the result in the `job_result` table. This eliminates temporary file management and ensures data consistency.
*   **Placeholder `ta_action_assoc` DDL**: Given the absence of the `d_ausd_v_ta_action_assoc.sql` content during the design phase, a placeholder DDL for `ta_action_assoc` was created. This acknowledges the dependency and allows for the overall structure to be defined, with the understanding that the full schema will be derived and implemented once the core SQL is analyzed.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **IAM Permissions Configuration**: Grant appropriate Identity and Access Management (IAM) roles to the service account or user that will deploy and execute the BigQuery DDLs and stored procedures. This typically includes `BigQuery Data Editor` or `BigQuery Admin` roles for the target dataset.
3.  **Finalize `ta_action_assoc` Schema**:
    *   **Crucial Step**: Analyze the content of the original `d_ausd_v_ta_action_assoc.sql` file.
    *   Derive the complete and accurate schema for the `project.dataset.ta_action_assoc` table, including all columns, data types, and any primary/partitioning keys.
    *   Update and deploy the `project/dataset/ddl/ta_action_assoc.sql` with the finalized schema.
4.  **Data Ingestion**: Ensure all source tables referenced by the original `d_ausd_v_ta_action_assoc.sql` (including `ta_action_assoc` itself, if it's a source for further processing) are ingested and available in BigQuery with their respective data.
5.  **Integrate Core SQL Logic**:
    *   Translate the entire content of `d_ausd_v_ta_action_assoc.sql` into BigQuery SQL.
    *   Integrate this translated logic into the `project/dataset/sp_ausd_v_ta_action_assoc.sql` file, replacing the placeholder `EXECUTE IMMEDIATE` block. This might involve inlining the SQL, creating a separate BigQuery Stored Procedure, or using BigQuery Scripting.
6.  **Deploy DDLs**: Execute the DDL scripts (`job_control.sql`, `job_result.sql`, `error_log.sql`, and the finalized `ta_action_assoc.sql`) to create the necessary tables in BigQuery.
7.  **Deploy Stored Procedure**: Execute the `project/dataset/sp_ausd_v_ta_action_assoc.sql` script to create or replace the stored procedure in BigQuery.
8.  **Scheduling Configuration**: Set up a scheduler (e.g., Google Cloud Composer, Cloud Scheduler, Dataform) to invoke the `project.dataset.sp_ausd_v_ta_action_assoc` stored procedure. Configure it to pass the required `p_JobKennung` and `p_EintragsNr` parameters.
9.  **Secrets Management**: If any new sensitive parameters or connection strings are introduced, ensure they are securely managed using Google Cloud Secret Manager and accessed appropriately by the scheduler or stored procedure.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps or unresolved references that require follow-up:

*   **Content of `d_ausd_v_ta_action_assoc.sql`**: The most critical unresolved item is the actual SQL logic contained within the original `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_action_assoc.sql` file. This content is essential for completing the `sp_ausd_v_ta_action_assoc` stored procedure and defining the final `ta_action_assoc` table schema.
*   **Full Logic of Sourced Utility Scripts**: While basic replacements for `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` have been assumed, any complex or unique logic within these legacy KornShell utility scripts needs to be thoroughly analyzed and translated into BigQuery SQL or UDFs if not covered by standard BigQuery functions.
*   **Job Activation/Deactivation Logic**: The original script's summary mentions "ignoring active jobs" and "deactivating old active jobs." The specific implementation details of this logic were not fully available. While the `job_control` table provides the framework, the precise BigQuery SQL logic to manage these states (e.g., checking for existing active jobs before starting, updating old jobs) needs to be explicitly defined and integrated into the stored procedure or its orchestration.
*   **Complete `ta_action_assoc` Schema**: The DDL for `ta_action_assoc` is currently a placeholder. Its final structure (all columns, data types, partitioning, clustering) must be accurately derived from the source system and the `d_ausd_v_ta_action_assoc.sql` script.

## 6. Validation

Validation of the migrated job involves several stages to ensure functional equivalence and data integrity.

*   **Unit Testing**:
    *   **Parameter Validation**: Execute `sp_ausd_v_ta_action_assoc` with various combinations of valid and invalid `p_JobKennung` and `p_EintragsNr` parameters. Verify that the procedure correctly raises errors for invalid inputs and logs them to `error_log`.
    *   **Control Table Updates**: For successful runs, verify that `job_control` is updated with `STARTED` and `FINISHED` statuses, and `job_result` contains the correct record count. For failed runs, ensure `job_control` shows `FAILED` and `error_log` contains relevant details.
    *   **Core Logic (once integrated)**: Test the translated BigQuery SQL logic (from `d_ausd_v_ta_action_assoc.sql`) in isolation with sample data to ensure it performs the expected transformations on `ta_action_assoc` and other affected tables.
*   **Integration Testing**:
    *   Execute the `sp_ausd_v_ta_action_assoc` in an environment that closely mirrors production (e.g., a staging BigQuery project).
    *   Verify the end-to-end data flow, from the invocation of the stored procedure with parameters to the final state of `ta_action_assoc` and the control tables.
*   **Data Validation**:
    *   Run the migrated job with a representative dataset.
    *   Compare the record counts and data samples in the target `ta_action_assoc` table (and any other tables modified by the core SQL logic) against the output of the original legacy job.
    *   Ensure all data transformations, filters, and aggregations match the legacy system's behavior.

**"Passing" Criteria**:

A successful migration is validated when:

1.  The `sp_ausd_v_ta_action_assoc` stored procedure executes without unhandled exceptions.
2.  For successful runs, the `job_control` table shows a `status` of 'FINISHED', and the `record_count` in `job_control` and `job_result` matches the expected number of processed records.
3.  For expected error conditions (e.g., invalid parameters), the `job_control` table shows a `status` of 'FAILED', and corresponding detailed error messages are present in the `error_log` table.
4.  The `ta_action_assoc` table (and any other tables modified by the core SQL logic) contains data that is functionally identical to the output produced by the legacy `k_ausd_v_ta_action_assoc.ksh` job.
5.  No unexpected errors are reported in BigQuery logs or the `error_log` table.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Halt**:
    *   Immediately pause or disable any scheduled executions of the `sp_ausd_v_ta_action_assoc` stored procedure in the BigQuery environment (e.g., disable the Cloud Composer DAG or Cloud Scheduler job).
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh` job in the legacy environment to resume normal operations.
2.  **BigQuery Stored Procedure Rollback**:
    *   If a previous stable version of `sp_ausd_v_ta_action_assoc.sql` exists, deploy that version using `CREATE OR REPLACE PROCEDURE`.
    *   Alternatively, to completely remove the migrated procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_ausd_v_ta_action_assoc`;
        ```
3.  **BigQuery Table Rollback (Data Tables)**:
    *   For data tables like `ta_action_assoc` that were modified by the stored procedure, BigQuery's time travel feature can be used to restore the table to a state before the problematic execution.
    *   Identify the timestamp before the erroneous execution.
    *   Restore the table:
        ```sql
        CREATE TABLE `project.dataset.ta_action_assoc_restored` AS
        SELECT * FROM `project.dataset.ta_action_assoc`
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X MINUTE); -- Adjust X to the time before the issue
        -- Or, if you want to overwrite the existing table (use with extreme caution):
        -- CREATE OR REPLACE TABLE `project.dataset.ta_action_assoc` AS
        -- SELECT * FROM `project.dataset.ta_action_assoc`
        -- FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X MINUTE);
        ```
    *   **Note**: BigQuery's default time travel window is 7 days. Ensure the rollback is performed within this window.
4.  **BigQuery Table Rollback (Control Tables)**:
    *   For control tables (`job_control`, `job_result`, `error_log`), if their data is not critical for historical auditing during rollback, they can be truncated or dropped:
        ```sql
        TRUNCATE TABLE `project.dataset.job_control`;
        TRUNCATE TABLE `project.dataset.job_result`;
        TRUNCATE TABLE `project.dataset.error_log`;
        -- Or to drop completely:
        -- DROP TABLE IF EXISTS `project.dataset.job_control`;
        -- DROP TABLE IF EXISTS `project.dataset.job_result`;
        -- DROP TABLE IF EXISTS `project.dataset.error_log`;
        ```
5.  **Data Reconciliation**: Depending on the nature of the data modifications, a data reconciliation process might be necessary to ensure consistency between the legacy system (now reactivated) and any data that might have been partially or incorrectly processed in BigQuery. This step is highly dependent on the specific data flow and transformation logic.