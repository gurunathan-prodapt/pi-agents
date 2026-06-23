# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell wrapper script `r_ausd_v_ta_action_assoc.ksh`. The script, originally responsible for orchestrating a contract data reconciliation job, has been migrated from a file-based KornShell environment to Google Cloud Platform, specifically leveraging **BigQuery Stored Procedures** and **BigQuery Audit/Log Tables**.

The migration focused on transforming the wrapper's core functionalities, including:
*   **Job Orchestration**: Managing the overall execution flow.
*   **Parameter Handling**: Processing command-line arguments.
*   **Logging**: Recording job events and status.
*   **Error Handling**: Gracefully managing and reporting execution failures.

The original script's core business logic, contained within `k_ausd_v_ta_action_assoc.ksh`, has been identified as a separate, dependent migration task and is represented by a placeholder stored procedure in this migration.

## 2. Generated artifacts

The migration produced the following BigQuery SQL artifacts:

*   **`sql/ddl/job_log.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `project.dataset.job_log` table. This table serves as the primary record for each job run, storing its unique ID, name, start/end times, status, version, and input parameters. It replaces the high-level logging and status tracking previously managed by `DWMSG_ErzeugeEintrag` and `DWMSG_SetzeStatusOK` in the legacy shell script.

*   **`sql/ddl/job_control.sql`**
    *   **Role**: Defines the DDL for the `project.dataset.job_control` table. This table stores job-specific control parameters, such as the `Stichtag` (reference date), which was previously managed by `DWMSG_SetzeStichtagInfo`.

*   **`sql/ddl/job_log_detail.sql`**
    *   **Role**: Defines the DDL for the `project.dataset.job_log_detail` table. This table captures granular log messages (INFO, WARNING, ERROR) during job execution, replacing the file-based log output (`tee -a $LogDatei`) of the legacy script.

*   **`sql/ddl/job_error_log.sql`**
    *   **Role**: Defines the DDL for the `project.dataset.job_error_log` table. This table records detailed error information, including messages, stack traces, and severity, replacing the error reporting functionality of `DWMSG_Fehlerbehandlung` and `DWMSG_MeldeFehler`.

*   **`sql/procedures/job_logging/util_create_job_entry.sql`**
    *   **Role**: Creates the `project.dataset.util_create_job_entry` stored procedure. This utility procedure is responsible for initiating a new job entry in `job_log` and generating a unique `job_id`, mirroring the functionality of `DWMSG_ErzeugeEintrag`.

*   **`sql/procedures/job_logging/util_log_detail.sql`**
    *   **Role**: Creates the `project.dataset.util_log_detail` stored procedure. This utility procedure inserts detailed log messages into the `job_log_detail` table, providing structured logging for various events during job execution.

*   **`sql/procedures/job_logging/util_set_stichtag_info.sql`**
    *   **Role**: Creates the `project.dataset.util_set_stichtag_info` stored procedure. This utility procedure records the `Stichtag` (reference date) in the `job_control` table, replicating the `DWMSG_SetzeStichtagInfo` functionality.

*   **`sql/procedures/job_logging/util_set_status_ok.sql`**
    *   **Role**: Creates the `project.dataset.util_set_status_ok` stored procedure. This utility procedure updates the `job_log` entry to mark a job as successfully completed ('OK'), analogous to `DWMSG_SetzeStatusOK`.

*   **`sql/procedures/job_logging/util_handle_error.sql`**
    *   **Role**: Creates the `project.dataset.util_handle_error` stored procedure. This utility procedure logs detailed error information to `job_error_log` and updates the `job_log` entry to 'ERROR', replacing the error handling logic of `DWMSG_Fehlerbehandlung` and `DWMSG_MeldeFehler`.

*   **`sql/procedures/sp_vertragsdatenabgleich.sql`**
    *   **Role**: Creates the `project.dataset.sp_vertragsdatenabgleich` stored procedure. This is the main orchestration procedure, directly replacing the `r_ausd_v_ta_action_assoc.ksh` wrapper script. It handles job initialization, parameter passing, logging, error management (`BEGIN...EXCEPTION...END`), and invokes the core business logic (via `sp_k_ausd_v_ta_action_assoc`).

*   **`sql/procedures/sp_vertragsdatenabgleich_entry.sql`**
    *   **Role**: Creates the `project.dataset.sp_vertragsdatenabgleich_entry` stored procedure. This serves as an entry point for external schedulers, handling initial parameter validation (e.g., mandatory arguments) before calling the main orchestration procedure.

*   **`sql/procedures/sp_k_ausd_v_ta_action_assoc.sql`**
    *   **Role**: Creates a placeholder stored procedure `project.dataset.sp_k_ausd_v_ta_action_assoc`. This procedure is intended to house the migrated core business logic from the `k_ausd_v_ta_action_assoc.ksh` script, which is a dependent task for future migration.

## 3. Key design decisions

1.  **Migration to BigQuery Stored Procedures for Orchestration**:
    *   **Why**: BigQuery stored procedures provide a native, scalable, and performant environment for executing SQL-based logic directly within the data warehouse. This eliminates the need for external compute resources (like VMs running shell scripts) for orchestration, reducing operational overhead and improving integration with BigQuery data. It allows for direct manipulation of BigQuery tables for logging and control.
    *   **Trade-offs**: While powerful for SQL-centric tasks, BigQuery stored procedures have limitations for complex file system operations, external API calls, or highly dynamic process management that shell scripts might offer. For this wrapper, the benefits of native integration outweighed these limitations.

2.  **Structured Logging and Audit Trails in BigQuery Tables**:
    *   **Why**: Replacing file-based logs with dedicated BigQuery tables (`job_log`, `job_control`, `job_log_detail`, `job_error_log`) provides significant advantages. Logs become immediately queryable, enabling easier monitoring, analysis, and auditing of job executions. This centralizes operational data alongside business data.
    *   **Trade-offs**: Requires explicit `INSERT` statements for logging, which adds verbosity compared to simple `echo` or `print` in shell. However, the structured nature and queryability far outweigh this.

3.  **Modular Utility Procedures for Reusability**:
    *   **Why**: The common `DWMSG_` functions in the legacy script were translated into distinct BigQuery utility procedures (e.g., `util_create_job_entry`, `util_handle_error`). This promotes code reusability, maintainability, and consistency across different jobs that might require similar logging or control functionalities.

4.  **`BEGIN...EXCEPTION...END` for Robust Error Handling**:
    *   **Why**: BigQuery's `BEGIN...EXCEPTION...END` block provides a structured and robust mechanism for error handling, directly replacing the shell `trap` commands. This allows for specific error logging, status updates, and controlled re-raising of exceptions, ensuring job failures are properly recorded and communicated.

5.  **Placeholder for Core Business Logic (`sp_k_ausd_v_ta_action_assoc`)**:
    *   **Why**: The `k_ausd_v_ta_action_assoc.ksh` script contains the actual business logic and was identified as a separate, dependent migration task. Creating a placeholder allows the wrapper's migration to proceed independently, establishing the new orchestration framework, while deferring the more complex analysis and transformation of the core logic.
    *   **Trade-offs**: This introduces a critical dependency. The full functionality of the job will not be realized until the placeholder is fully implemented. It also means the current migration only covers the "shell" of the original job.

6.  **Parameter Handling via Stored Procedure Arguments**:
    *   **Why**: Command-line `getopts` parsing is replaced by direct `IN` parameters in BigQuery stored procedures. This provides type safety, clear definition of inputs, and simplifies invocation from schedulers.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` as used in the generated code) exists. If not, create it:
        ```sql
        CREATE SCHEMA project.dataset;
        ```

2.  **IAM Permissions**:
    *   The Google Cloud service account or user identity that will execute these stored procedures must have appropriate BigQuery permissions. This typically includes:
        *   `BigQuery Data Editor` role on the `project.dataset` to create/update tables and insert/update data.
        *   `BigQuery Job User` role to run jobs and execute stored procedures.
        *   `BigQuery Data Viewer` role on any source tables that `sp_k_ausd_v_ta_action_assoc` will eventually read from.

3.  **Deploy Generated Artifacts**:
    *   Execute all generated DDL (`sql/ddl/*.sql`) to create the audit and control tables.
    *   Execute all generated stored procedure DDL (`sql/procedures/**/*.sql`) to create the procedures. Ensure they are deployed in the correct order (e.g., utility procedures before the main orchestration procedure).

4.  **Implement Core Business Logic**:
    *   **CRITICAL**: The `project.dataset.sp_k_ausd_v_ta_action_assoc` stored procedure is currently a placeholder. Its full implementation, translating the logic from `k_ausd_v_ta_action_assoc.ksh`, is required before the job can perform its intended function. This will involve detailed analysis of the original script's SQL queries, data transformations, and any external interactions.

5.  **Scheduling Configuration**:
    *   Configure a scheduler (e.g., Cloud Scheduler, Cloud Composer/Airflow, or a custom application) to invoke the `project.dataset.sp_vertragsdatenabgleich_entry` stored procedure.
    *   The scheduler must pass the required parameters, primarily `p_stichtag_date` (e.g., `CALL project.dataset.sp_vertragsdatenabgleich_entry(CURRENT_DATE(), '1.0', FALSE);`).

6.  **Configuration Management**:
    *   If any global configuration values (from `$HOME/.dw_init` in the legacy system) are needed by the fully implemented `sp_k_ausd_v_ta_action_assoc`, decide on their management strategy (e.g., BigQuery configuration tables, Google Secret Manager for sensitive values, or passed as parameters).

## 5. Known gaps & unresolved references

1.  **Core Business Logic (`k_ausd_v_ta_action_assoc.ksh`)**:
    *   **Status**: This is the most significant gap. The actual data processing and business logic from the original `k_ausd_v_ta_action_assoc.ksh` script has not been migrated and is represented by a placeholder procedure (`sp_k_ausd_v_ta_action_assoc`).
    *   **Impact**: The migrated wrapper can execute, log, and handle errors, but it will not perform the intended contract data reconciliation until `sp_k_ausd_v_ta_action_assoc` is fully implemented.
    *   **Follow-up**: A dedicated analysis and migration effort for `k_ausd_v_ta_action_assoc.ksh` is required.

2.  **Undefined Parameters (`-s`, `-l`)**:
    *   **Status**: The original `r_ausd_v_ta_action_assoc.ksh` script's `getopts` string included options `-s` and `-l`, but these parameters were not used within the wrapper script itself.
    *   **Impact**: Their purpose, if any, is unclear without analyzing `k_ausd_v_ta_action_assoc.ksh`. They are not currently mapped to parameters in the migrated BigQuery procedures.
    *   **Follow-up**: During the migration of `k_ausd_v_ta_action_assoc.ksh`, clarify if these parameters are required and, if so, integrate them into the `sp_k_ausd_v_ta_action_assoc` signature and the main orchestration procedure.

3.  **Lineage Edges for Dynamic Invocations**:
    *   **Status**: The automated `lineage_edges` tool did not detect the invocation of `k_ausd_v_ta_action_assoc.ksh` by the wrapper.
    *   **Impact**: This highlights a limitation in automated lineage analysis for dynamic or shell-based script invocations. Manual verification of such dependencies is crucial.
    *   **Follow-up**: Ensure all dependencies of `k_ausd_v_ta_action_assoc.ksh` are thoroughly identified during its dedicated migration.

4.  **Advanced Shell Semantics**:
    *   **Status**: While basic error handling (`set -eu`, `trap`) is covered by BigQuery's `BEGIN...EXCEPTION...END`, highly specific shell behaviors (e.g., complex process management, inter-process communication, or very specific signal handling) might not be fully replicated by pure BigQuery SQL.
    *   **Impact**: If `k_ausd_v_ta_action_assoc.ksh` relies on such advanced shell features, its migration might require more than just BigQuery SQL (e.g., Cloud Functions, Cloud Run, or Cloud Composer for orchestration).
    *   **Follow-up**: Analyze `k_ausd_v_ta_action_assoc.ksh` for any such advanced shell semantics.

## 6. Validation

To validate the migrated wrapper functionality, follow these steps:

1.  **Prerequisites**: Ensure all DDL and stored procedures (including the placeholder `sp_k_ausd_v_ta_action_assoc`) have been deployed to BigQuery.

2.  **Test Scenarios**:

    *   **Successful Execution**:
        *   Execute the entry-point procedure with valid parameters:
            ```sql
            CALL project.dataset.sp_vertragsdatenabgleich_entry(CURRENT_DATE(), '1.0', FALSE);
            ```
        *   **Expected "Passing" Criteria**:
            *   A new record appears in `project.dataset.job_log` with `status = 'OK'` and `end_time` populated.
            *   A record for `Stichtag` appears in `project.dataset.job_control`.
            *   `project.dataset.job_log_detail` contains a sequence of `INFO` messages, including "Job started", "Stichtag set", "Placeholder for core logic called", and "Job completed successfully."
            *   `project.dataset.job_error_log` should be empty for this run.

    *   **Execution with Debug Mode**:
        *   Execute with debug flag set to TRUE:
            ```sql
            CALL project.dataset.sp_vertragsdatenabgleich_entry(CURRENT_DATE(), '1.0', TRUE);
            ```
        *   **Expected "Passing" Criteria**:
            *   Same as successful execution, but `project.dataset.job_log_detail` should also contain a `DEBUG` message: "Debug mode is ON."

    *   **Error Handling (Simulated Core Logic Failure)**:
        *   **Step 1**: Modify the `sp_k_ausd_v_ta_action_assoc` placeholder to simulate an error. For example, uncomment the `SELECT 1 / 0;` line:
            ```sql
            -- In sql/procedures/sp_k_ausd_v_ta_action_assoc.sql
            CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_action_assoc(
                IN p_job_id STRING,
                IN p_stichtag_date DATE
            )
            BEGIN
                -- ... existing placeholder code ...
                SELECT 1 / 0; -- Simulate an error
                -- ...
            END;
            ```
        *   **Step 2**: Re-deploy the modified `sp_k_ausd_v_ta_action_assoc`.
        *   **Step 3**: Execute the entry-point procedure:
            ```sql
            CALL project.dataset.sp_vertragsdatenabgleich_entry(CURRENT_DATE(), '1.0', FALSE);
            ```
        *   **Expected "Passing" Criteria**:
            *   The `CALL` statement should terminate with an error message indicating job failure (e.g., "Job failed for job_id ...: Division by zero").
            *   A new record appears in `project.dataset.job_log` with `status = 'ERROR'` and `end_time` populated.
            *   `project.dataset.job_log_detail` contains `ERROR` messages related to the failure.
            *   `project.dataset.job_error_log` contains a detailed entry for the error, including the error message and stack trace.

    *   **Missing Mandatory Parameter**:
        *   Attempt to call the entry point without `p_stichtag_date`:
            ```sql
            -- This will fail at the SQL parsing level if called directly without a date literal
            -- or if the scheduler doesn't provide it.
            -- Example of how it might be called incorrectly:
            -- CALL project.dataset.sp_vertragsdatenabgleich_entry(NULL, '1.0', FALSE);
            ```
        *   **Expected "Passing" Criteria**:
            *   The procedure call should immediately raise an error with the message: "Parameter p_stichtag_date must be provided."
            *   No entries should be created in `job_log`, `job_control`, `job_log_detail`, or `job_error_log` as the validation occurs before job initialization.

3.  **Functional Parity (Post-Core Logic Migration)**:
    *   Once `sp_k_ausd_v_ta_action_assoc` is fully implemented, comprehensive integration tests will be required to ensure that the data transformations and outputs match the legacy `k_ausd_v_ta_action_assoc.ksh` script. This will involve comparing output datasets, record counts, and specific data values.

## 7. Rollback procedure

In case of critical issues or if the migrated job does not meet requirements, the following rollback procedure can be executed:

1.  **Disable New Job Scheduling**:
    *   Immediately disable or remove any scheduled triggers (e.g., Cloud Scheduler job, Cloud Composer DAG) that invoke `project.dataset.sp_vertragsdatenabgleich_entry`.

2.  **Re-enable Legacy Job**:
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh` script and its associated scheduling are fully functional and re-enabled. Verify that it can run successfully.

3.  **Delete BigQuery Objects (Optional, but Recommended for Clean-up)**:
    *   **Stored Procedures**: Drop all generated stored procedures from the BigQuery dataset:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.sp_vertragsdatenabgleich_entry;
        DROP PROCEDURE IF EXISTS project.dataset.sp_vertragsdatenabgleich;
        DROP PROCEDURE IF EXISTS project.dataset.sp_k_ausd_v_ta_action_assoc;
        DROP PROCEDURE IF EXISTS project.dataset.util_create_job_entry;
        DROP PROCEDURE IF EXISTS project.dataset.util_log_detail;
        DROP PROCEDURE IF EXISTS project.dataset.util_set_stichtag_info;
        DROP PROCEDURE IF EXISTS project.dataset.util_set_status_ok;
        DROP PROCEDURE IF EXISTS project.dataset.util_handle_error;
        ```
    *   **Tables**: Drop the audit and control tables. **WARNING**: This will delete all historical log data. If retaining logs is desired, skip this step or archive the data first.
        ```sql
        DROP TABLE IF EXISTS project.dataset.job_log;
        DROP TABLE IF EXISTS project.dataset.job_control;
        DROP TABLE IF EXISTS project.dataset.job_log_detail;
        DROP TABLE IF EXISTS project.dataset.job_error_log;
        ```

4.  **Data Consistency Check**:
    *   If the (fully implemented) `sp_k_ausd_v_ta_action_assoc` had written any data to target tables, verify the state of those tables. Depending on the nature of the data, a clean-up or restoration from a backup might be necessary to ensure data consistency. (This step is more relevant once the core logic is migrated).

This rollback procedure ensures a quick return to the previous operational state while providing options for cleaning up the partially migrated components.