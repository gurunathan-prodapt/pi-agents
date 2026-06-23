# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_vvl_dwh.ksh` from its legacy environment to Google Cloud Platform (GCP).

The original script served as an orchestration and control mechanism, responsible for:
*   Ignoring currently active jobs to prevent conflicts.
*   Parsing and validating input parameters (`Jobkennung`, `EintragsNr`).
*   Executing a core SQL script (`d_ausd_v_ta_vvl_dwh.sql`) for data transformation.
*   Recording job status and processed record counts.
*   Deactivating old active jobs (logic not fully detailed in source).

The migration targets Google BigQuery for data processing and storage, with the orchestration logic encapsulated within a BigQuery Stored Procedure. This approach leverages BigQuery's scalability and managed services, replacing the custom shell scripting and `sqlplus` execution.

## 2. Generated artifacts

The migration process generated the following BigQuery-compatible artifacts:

*   **`job_table_ddl.sql`**:
    *   **Role**: Defines the Data Definition Language (DDL) for the `job_table` in BigQuery. This table is used to track the status, parameters, and record counts of each job execution, replacing the shell script's custom job tracking and temporary file mechanisms.
*   **`job_error_log_ddl.sql`**:
    *   **Role**: Defines the DDL for the `job_error_log` table in BigQuery. This table captures detailed error messages and context, replacing the custom `DWMSG_MeldeFehler` function and shell-based error logging.
*   **`target_table_ddl.sql`**:
    *   **Role**: Defines the DDL for the `target_table` (representing `ta_vvl_dwh`) in BigQuery. This is the destination table for the transformed data. *Note: This is a placeholder; the actual schema needs to be populated based on the original `ta_vvl_dwh` structure.*
*   **`d_ausd_v_ta_vvl_dwh_migrated.sql`**:
    *   **Role**: This file is a placeholder for the core data transformation logic, which was originally contained within `d_ausd_v_ta_vvl_dwh.sql`. Its content must be fully translated from the original SQL dialect (likely Oracle) to BigQuery SQL. This script will perform the actual data manipulation (INSERT/MERGE) into `project.dataset.target_table`.
*   **`r_ausd_vertrag_control_sp.sql`**:
    *   **Role**: This BigQuery Stored Procedure encapsulates the orchestration and control logic of the original `k_ausd_v_ta_vvl_dwh.ksh` script. It handles parameter validation, job status logging, execution of the core transformation logic (via `d_ausd_v_ta_vvl_dwh_migrated.sql`), and error handling. It also includes a helper procedure `log_error` to centralize error logging.

## 3. Key design decisions

*   **Target Platform: BigQuery Stored Procedures**: The primary orchestration logic of the KornShell script was migrated to a BigQuery Stored Procedure (`r_ausd_vertrag_control_sp`). This decision was made to keep the entire data processing pipeline within BigQuery, leveraging its native capabilities for SQL execution, parameter handling, and procedural logic. This avoids the overhead and complexity of introducing an external orchestrator like Cloud Composer for this specific job, given its relatively contained scope.
*   **Parameter Handling**: Command-line parameters (`Jobkennung`, `EintragsNr`) from the original shell script are now directly passed as input parameters to the BigQuery Stored Procedure. This provides a clear and type-safe interface for job invocation.
*   **Centralized Logging and Job Tracking**: Custom shell-based error reporting (`DWMSG_MeldeFehler`) and job status tracking (using temporary files and implicit database updates) have been replaced by dedicated BigQuery tables (`job_table`, `job_error_log`). This provides a structured, queryable, and scalable mechanism for monitoring job execution and debugging failures.
*   **Elimination of Temporary Files**: The shell script's reliance on temporary files (e.g., `$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_dwh_$$.tmp`) for passing record counts has been eliminated. Record counts are now captured directly within the BigQuery Stored Procedure, either from DML statement results or by querying the target table, and then stored in `job_table`.
*   **SQL Dialect Conversion**: The core business transformation logic, originally in `d_ausd_v_ta_vvl_dwh.sql` (likely Oracle SQL), requires full conversion to BigQuery SQL. This involves adapting syntax, functions, and potentially rewriting procedural constructs to BigQuery's set-based or procedural SQL capabilities. This is a critical step to ensure optimal performance and compatibility within BigQuery.
*   **Replacement of Utility Scripts**: The functionality provided by various sourced utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) has been re-implemented using BigQuery's native functions, procedural statements (e.g., `IF`, `RAISE`), and the new logging tables.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Setup**:
    *   Ensure the target GCP project is correctly configured.
    *   Create the BigQuery dataset (e.g., `project.dataset`) where the tables and stored procedures will reside.
2.  **Schema Creation**:
    *   Execute `job_table_ddl.sql` to create the `job_table`.
    *   Execute `job_error_log_ddl.sql` to create the `job_error_log` table.
    *   **Crucially, update and execute `target_table_ddl.sql` with the actual schema of `ta_vvl_dwh`**. The provided file is a placeholder.
3.  **Core SQL Logic Migration**:
    *   **Manually translate the full content of the original `d_ausd_v_ta_vvl_dwh.sql` into BigQuery SQL.** This is the most significant manual step.
    *   Replace the placeholder content in `d_ausd_v_ta_vvl_dwh_migrated.sql` with the fully translated BigQuery SQL. This SQL should perform the necessary data transformations and write to `project.dataset.target_table`.
    *   Ensure the migrated SQL can return or allow for the capture of the number of processed records.
4.  **Stored Procedure Deployment**:
    *   Execute `r_ausd_vertrag_control_sp.sql` to create the main control stored procedure and the `log_error` helper procedure in BigQuery.
    *   **Update the `EXECUTE IMMEDIATE` block within `r_ausd_vertrag_control_sp.sql`** to correctly invoke or embed the migrated `d_ausd_v_ta_vvl_dwh_migrated.sql` logic and capture the `v_records_processed` count.
5.  **IAM Permissions**:
    *   Grant appropriate BigQuery IAM roles to the service account or user that will invoke the stored procedure. This typically includes `BigQuery Data Editor` on the target dataset and `BigQuery Job User` for running queries.
6.  **Source Data Migration**:
    *   If the original `d_ausd_v_ta_vvl_dwh.sql` reads from an external database (e.g., Oracle), ensure that the necessary source tables are migrated or continuously synchronized to BigQuery. This might involve one-time data transfers or setting up CDC pipelines (e.g., using Datastream).
7.  **Scheduling**:
    *   Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer, or a custom application) to invoke the `project.dataset.r_ausd_vertrag_control_sp` with the required `p_JobKennung` and `p_EintragsNr` parameters at the desired frequency.

## 5. Known gaps & unresolved references

The following items were identified as gaps or risks during the migration design and require further attention:

*   **`d_ausd_v_ta_vvl_dwh.sql` Content**: The actual content and complexity of the core SQL transformation script (`d_ausd_v_ta_vvl_dwh.sql`) were not available. Its migration to BigQuery SQL is the most critical and potentially complex part of this job. Without its content, the full scope of transformation logic, specific BigQuery syntax conversions, and potential performance optimizations cannot be fully assessed.
*   **Job Deactivation Logic**: The original script's purpose mentions "alte aktive Jobs werden einfach dekativiert" (old active jobs are simply deactivated). The explicit code for this deactivation was not visible in the provided shell script. This logic needs to be identified and re-implemented within the BigQuery Stored Procedure or an external orchestrator if it's a critical business requirement.
*   **Dependency on `r_ausd_vertrag.ksh`**: The design document notes "Kontrollscript zu r_ausd_vertrag.ksh". The exact relationship (e.g., parent/child, sibling) between `k_ausd_v_ta_vvl_dwh.ksh` and `r_ausd_vertrag.ksh` is unclear. Understanding the full job hierarchy is crucial for proper scheduling and execution order in the new environment.
*   **Unidentified Utility Scripts**: The full functionality of the sourced utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) needs to be thoroughly understood to ensure all their capabilities are accurately replicated or replaced by BigQuery equivalents.
*   **Original `ta_vvl_dwh` Schema**: The DDL for the target table (`target_table_ddl.sql`) is a placeholder. The actual schema of `ta_vvl_dwh` must be obtained and used to define the BigQuery table correctly.
*   **Record Count Capture in SP**: The `r_ausd_vertrag_control_sp.sql` currently uses `@@row_count` as a placeholder for capturing processed records. This needs to be adjusted based on the actual DML in `d_ausd_v_ta_vvl_dwh_migrated.sql`. For complex DML or multiple statements, a `SELECT COUNT(*)` after the DML or a return value from a sub-procedure might be necessary.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prerequisites**: Ensure all manual steps (Section 4) have been completed, including the full migration of `d_ausd_v_ta_vvl_dwh.sql` content and the deployment of all DDLs and stored procedures.
2.  **Prepare Test Data**: Load a representative set of test data into the BigQuery source tables that `d_ausd_v_ta_vvl_dwh_migrated.sql` will read from. Ensure this data covers various scenarios, including edge cases and potential error conditions.
3.  **Clear Target Table**: Truncate or delete data from `project.dataset.target_table` and `project.dataset.job_table`, `project.dataset.job_error_log` to ensure a clean test run.
4.  **Execute the Stored Procedure**:
    *   Open the BigQuery console.
    *   Execute the stored procedure with sample parameters:
        ```sql
        CALL project.dataset.r_ausd_vertrag_control_sp(
            p_JobKennung => 'TEST_JOB_001',
            p_EintragsNr => 'ENTRY_001'
        );
        ```
    *   Test with invalid parameters (e.g., `NULL` or empty strings for `p_JobKennung`) to verify error handling.
5.  **Verify Execution**:
    *   **Check `job_table`**: Query `project.dataset.job_table` to ensure a record for `TEST_JOB_001` exists with `status = 'COMPLETED'`, a non-zero `record_count` (if data was processed), and valid `created_ts`/`finished_ts`.
    *   **Check `target_table`**: Query `project.dataset.target_table` to verify that the expected data has been inserted/updated correctly according to the logic in `d_ausd_v_ta_vvl_dwh_migrated.sql`. Compare the output with expected results from the original system if possible.
    *   **Check `job_error_log`**: If an error was intentionally triggered (e.g., invalid parameters, or if the core SQL fails), query `project.dataset.job_error_log` to ensure the error was logged correctly with relevant details.
    *   **BigQuery Job History**: Review the BigQuery job history for the executed stored procedure to check for any BigQuery-level errors or warnings.

**"Passing" means**:
*   The `r_ausd_vertrag_control_sp` executes without BigQuery-level errors.
*   A record is successfully inserted into `project.dataset.job_table` with `status = 'COMPLETED'` and an accurate `record_count`.
*   The `project.dataset.target_table` contains the correct, transformed data as expected.
*   No unexpected errors are logged in `project.dataset.job_error_log`.
*   Parameter validation correctly identifies and logs invalid inputs.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Job Execution**: Immediately halt any scheduled invocations of the `project.dataset.r_ausd_vertrag_control_sp` (e.g., disable Cloud Scheduler jobs, pause Airflow DAGs).
2.  **Revert Scheduling**: Re-enable or restart the original scheduling mechanism for the `k_ausd_v_ta_vvl_dwh.ksh` script in the legacy environment.
3.  **Verify Legacy Job**: Monitor the execution of the original `k_ausd_v_ta_vvl_dwh.ksh` script to ensure it is running as expected and processing data correctly.
4.  **Data Consistency (Optional, if needed)**: If the new BigQuery job made irreversible changes or corrupted data in `project.dataset.target_table`, a data recovery step might be necessary. This could involve:
    *   Restoring `project.dataset.target_table` from a previous snapshot or backup (if available).
    *   Running a data reconciliation script to correct any discrepancies.
    *   *Note: This step is highly dependent on the nature of the data transformation and the impact of the failure.*
5.  **Analyze and Rectify**: Investigate the root cause of the failure in the migrated BigQuery job, make necessary corrections, and re-test thoroughly in a non-production environment before attempting another deployment.