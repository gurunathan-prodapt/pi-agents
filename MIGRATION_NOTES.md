# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `r_ausd_v_ta_disc_zusgf.ksh` from a legacy Unix/Linux environment to Google BigQuery. The original script orchestrated a data reconciliation process for the `ta_disc_zusgf` table, handling environment setup, parameter parsing, logging, and error handling before invoking a core processing script (`k_ausd_v_ta_disc_zusgf.ksh`).

The migration translates this orchestration and control-flow logic into a BigQuery Stored Procedure named `Vertragsdatenabgleich_wrapper_sp`. Shared utility functions for logging and error handling are also migrated to dedicated BigQuery Stored Procedures. Logging, previously file-based, is now managed through a BigQuery audit/log table (`job_log_table`). The core business logic, originally in `k_ausd_v_ta_disc_zusgf.ksh`, is represented by a placeholder BigQuery Stored Procedure (`k_ausd_v_ta_disc_zusgf_sp`) which requires further implementation.

## 2. Generated artifacts

The migration process generated the following BigQuery DDL and Stored Procedure scripts:

*   **`ddl/job_log_table.sql`**
    *   **Role**: Defines the schema for the `job_log_table`. This table serves as the central repository for all job execution logs, status updates, and error messages, replacing the filesystem-based log files of the original KornShell script.
*   **`sp/DWMSG_ErmittleNr_sp.sql`**
    *   **Role**: A utility BigQuery Stored Procedure that generates a unique job entry number (`job_nr`) for each execution. This replaces the shell script's method of generating unique identifiers.
*   **`sp/DWMSG_Logdateiname_sp.sql`**
    *   **Role**: A utility BigQuery Stored Procedure that constructs a simulated log file name. While actual logs are in `job_log_table`, this SP provides a consistent identifier that can be stored in the log table for traceability, mimicking the original script's log file naming convention.
*   **`sp/DWMSG_ErzeugeEintrag_sp.sql`**
    *   **Role**: A utility BigQuery Stored Procedure responsible for creating the initial log entry for a job in the `job_log_table` when the wrapper script starts.
*   **`sp/DWMSG_SetzeStichtagInfo_sp.sql`**
    *   **Role**: A utility BigQuery Stored Procedure to log reference date information into the `job_log_table`, capturing important runtime context.
*   **`sp/DWMSG_Fehlerbehandlung_sp.sql`**
    *   **Role**: A utility BigQuery Stored Procedure for centralized error handling. It updates the status of a failed job in `job_log_table` to 'FAILED' and inserts an error message. This replaces the `trap ERR` logic in the original shell script.
*   **`sp/DWMSG_SetzeStatusOK_sp.sql`**
    *   **Role**: A utility BigQuery Stored Procedure to mark a job as 'SUCCESS' in the `job_log_table` upon successful completion.
*   **`ddl/ta_disc_zusgf.sql`**
    *   **Role**: A placeholder DDL for the `ta_disc_zusgf` table in BigQuery. The actual schema for this table needs to be defined based on the source system's schema during the migration of the core logic.
*   **`sp/k_ausd_v_ta_disc_zusgf_sp.sql`**
    *   **Role**: A placeholder BigQuery Stored Procedure for the core data reconciliation logic. This SP is intended to contain the translated business logic from the original `k_ausd_v_ta_disc_zusgf.ksh` script. Its full implementation is a subsequent task.
*   **`sp/Vertragsdatenabgleich_wrapper_sp.sql`**
    *   **Role**: The main BigQuery Stored Procedure that encapsulates the orchestration logic of the original `r_ausd_v_ta_disc_zusgf.ksh` script. It handles parameter parsing, calls the utility SPs for logging and error handling, and invokes the core processing SP (`k_ausd_v_ta_disc_zusgf_sp`).

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration**: The KornShell wrapper's control flow, parameter handling, and script invocation logic were directly translated into a BigQuery Stored Procedure (`Vertragsdatenabgleich_wrapper_sp`). This leverages BigQuery's native scripting capabilities for ETL orchestration, keeping the logic within the data warehouse environment.
*   **Centralized BigQuery Logging**: Instead of disparate filesystem log files, all job execution details, status updates, and error messages are consolidated into a single `job_log_table` in BigQuery. This provides a structured, queryable, and scalable logging solution.
*   **Modular Utility Stored Procedures**: Common functionalities like generating job IDs, constructing log names, and handling status updates/errors (e.g., `DWMSG_ErmittleNr_sp`, `DWMSG_Fehlerbehandlung_sp`) were extracted into separate, reusable BigQuery Stored Procedures. This mirrors the modularity of the original shell utility scripts.
*   **Separation of Wrapper and Core Logic**: The wrapper script's role (orchestration, logging, error handling) is distinct from the core business logic (data reconciliation). This separation is maintained in BigQuery, with `Vertragsdatenabgleich_wrapper_sp` calling `k_ausd_v_ta_disc_zusgf_sp`. This allows for independent development and testing of the core logic.
*   **Parameter Translation**: Shell script arguments (`getopts`) are directly mapped to input parameters of the `Vertragsdatenabgleich_wrapper_sp`. This simplifies invocation and ensures type safety.
*   **Structured Error Handling**: The `trap ERR` mechanism of KornShell is replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, providing robust and structured error management within the SQL context.
*   **Trade-offs**:
    *   **Loss of Direct Filesystem Interaction**: BigQuery Stored Procedures do not have direct access to the filesystem. This necessitates the shift to table-based logging and requires alternative solutions (e.g., Cloud Storage, Cloud Functions) if file I/O is critical for the core logic.
    *   **Shell Environment Variables**: The concept of global shell environment variables (e.g., `$HOME`, `$BERT_DIR_ROOT`) is replaced by explicit parameters, `DECLARE` variables, or configuration tables within BigQuery.
    *   **External Orchestration**: While the wrapper logic is in BigQuery, if complex scheduling or inter-job dependencies exist, an external orchestrator like Cloud Workflows or Cloud Composer might be required to trigger the BigQuery Stored Procedure.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project_id.dataset_id`) exists. If not, create it.
2.  **`ta_disc_zusgf` Table Schema Definition**: The `ddl/ta_disc_zusgf.sql` file currently contains a placeholder. The actual schema for the `ta_disc_zusgf` table in BigQuery must be accurately defined based on the source system's schema. This involves identifying all columns, their data types, and any partitioning/clustering requirements.
3.  **Core Logic Implementation (`k_ausd_v_ta_disc_zusgf_sp`)**: The `sp/k_ausd_v_ta_disc_zusgf_sp.sql` file is a placeholder. The complete data reconciliation logic from the original `k_ausd_v_ta_disc_zusgf.ksh` script must be translated into BigQuery SQL and implemented within this stored procedure. This is a critical and potentially complex task.
4.  **IAM Permissions**:
    *   The service account or user executing the BigQuery Stored Procedures must have appropriate IAM roles, including:
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) on the target dataset to create/update tables and stored procedures.
        *   `BigQuery Job User` to run queries and stored procedures.
        *   Permissions to read/write to `job_log_table` and `ta_disc_zusgf`.
5.  **Initial Data Load for `ta_disc_zusgf`**: If `ta_disc_zusgf` is not already populated in BigQuery, an initial data load from the source system must be performed.
6.  **Scheduling Configuration**: Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer, or a custom Cloud Function) to invoke the `Vertragsdatenabgleich_wrapper_sp` at the desired frequency and with the necessary parameters.
7.  **Parameter Mapping**: Review and confirm the mapping of original KornShell script parameters to the `Vertragsdatenabgleich_wrapper_sp` parameters (`p_param_s`, `p_param_l`). Adjust default values or add more parameters as needed based on the original script's behavior.

## 5. Known gaps & unresolved references

*   **Core Script Logic (`k_ausd_v_ta_disc_zusgf.ksh`)**: The most significant gap is the full implementation of `k_ausd_v_ta_disc_zusgf_sp`. This document provides the wrapper, but the core business logic remains a placeholder. Its migration complexity, specific data transformations, and potential external dependencies (e.g., other tables, external files) need a dedicated analysis.
*   **`ta_disc_zusgf` Schema**: The DDL for `ta_disc_zusgf` is generic. The precise column definitions, data types, and table options (e.g., partitioning, clustering) must be determined and applied.
*   **Migration Bucket `retire` (B0)**: The original job was marked for retirement. A clear decision is needed on whether this migration should proceed or if the functionality is indeed obsolete. If retired, this migration effort is unnecessary.
*   **Missing `file_complexity` Data**: The absence of complexity data for the source script means the effort estimation for the core logic migration might be underestimated.
*   **Implicit Dependencies**: There might be other implicit dependencies in the original KornShell environment (e.g., specific environment variables, system commands, or other sourced scripts not explicitly listed) that were not fully captured and might require further investigation during the core script migration.
*   **Configuration Tables**: While `job_log_table` is created, the design document mentioned "Configuration tables" for job identifiers, log settings, and runtime parameters. These have not been explicitly generated and might be needed for more dynamic configurations.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Execute the Wrapper Stored Procedure**:
    *   Open the BigQuery console.
    *   Navigate to the `project_id.dataset_id` dataset.
    *   Execute the `Vertragsdatenabgleich_wrapper_sp` using a `CALL` statement, providing any necessary parameters.
        ```sql
        CALL `project_id.dataset_id.Vertragsdatenabgleich_wrapper_sp`(
          p_param_s => 'some_value_s',
          p_param_l => 'some_value_l',
          p_display_help => FALSE
        );
        ```
    *   Initially, for testing the wrapper itself, the `k_ausd_v_ta_disc_zusgf_sp` can contain minimal logic or simply log its invocation.

2.  **What "passing" means**:
    *   **Successful Execution**: The `CALL` statement completes without raising an unhandled BigQuery error.
    *   **Log Table Verification**: Query the `job_log_table` for the `job_nr` corresponding to the execution.
        ```sql
        SELECT * FROM `project_id.dataset_id.job_log_table` WHERE job_nr = <your_job_nr> ORDER BY created_at;
        ```
        *   Verify that the `job_log_table` contains entries for the job start, banner, core script invocation, and a final entry with `status = 'SUCCESS'`.
        *   In case of an error, verify that `DWMSG_Fehlerbehandlung_sp` was called, and the `job_log_table` shows `status = 'FAILED'` and relevant error messages.
    *   **Data Reconciliation (after core SP implementation)**: Once `k_ausd_v_ta_disc_zusgf_sp` is fully implemented, verify that:
        *   The `ta_disc_zusgf` table (and any other affected tables) reflects the expected data changes according to the business logic.
        *   Data integrity is maintained.
        *   Performance metrics are within acceptable limits.
    *   **Parameter Handling**: Test with different parameter values (including edge cases or missing parameters if applicable) to ensure correct parsing and behavior.
    *   **Error Scenarios**: Intentionally introduce errors (e.g., by modifying `k_ausd_v_ta_disc_zusgf_sp` to `RAISE` an error) to confirm that the error handling (`DWMSG_Fehlerbehandlung_sp`) correctly logs the failure and updates the job status.

## 7. Rollback procedure

In case of issues or a decision to revert, the following rollback procedure should be followed:

1.  **Disable New Orchestration**: If any new scheduling or orchestration (e.g., Cloud Scheduler job, Cloud Composer DAG) was set up to trigger `Vertragsdatenabgleich_wrapper_sp`, disable or delete it immediately.
2.  **Drop BigQuery Objects**:
    *   Drop the `Vertragsdatenabgleich_wrapper_sp` stored procedure.
    *   Drop the `k_ausd_v_ta_disc_zusgf_sp` stored procedure.
    *   Drop all utility stored procedures (`DWMSG_ErmittleNr_sp`, `DWMSG_Logdateiname_sp`, `DWMSG_ErzeugeEintrag_sp`, `DWMSG_SetzeStichtagInfo_sp`, `DWMSG_Fehlerbehandlung_sp`, `DWMSG_SetzeStatusOK_sp`).
    *   If the `ta_disc_zusgf` table was created specifically for this migration and no other processes depend on it, consider dropping it. If it's a shared table, revert any schema changes made during the migration.
    *   The `job_log_table` can be retained for historical audit, or dropped if no longer needed.
    ```sql
    DROP PROCEDURE IF EXISTS `project_id.dataset_id.Vertragsdatenabgleich_wrapper_sp`;
    DROP PROCEDURE IF EXISTS `project_id.dataset_id.k_ausd_v_ta_disc_zusgf_sp`;
    DROP PROCEDURE IF EXISTS `project_id.dataset_id.DWMSG_ErmittleNr_sp`;
    -- ... (drop other utility SPs)
    DROP TABLE IF EXISTS `project_id.dataset_id.ta_disc_zusgf`; -- Use with caution if data exists
    -- DROP TABLE IF EXISTS `project_id.dataset_id.job_log_table`; -- Optional, for full cleanup
    ```
3.  **Revert Data Changes (if necessary)**: If the `k_ausd_v_ta_disc_zusgf_sp` had already processed data in `ta_disc_zusgf` and those changes need to be undone, restore the `ta_disc_zusgf` table from a backup taken before the migration or execute specific undo scripts.
4.  **Re-enable Original Job**: Re-enable the original `r_ausd_v_ta_disc_zusgf.ksh` script in its legacy environment and ensure its scheduling is restored.