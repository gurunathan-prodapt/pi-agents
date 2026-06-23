# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh`. This script, originally responsible for orchestrating the `ta_cntrct_templ` contract data reconciliation process, including environment setup, parameter parsing, and logging, has been migrated to Google Cloud's BigQuery platform.

The target architecture consists of:
*   A BigQuery Stored Procedure, `project.dataset.sp_vertragsdatenabgleich`, which serves as the new orchestration layer, handling parameter input, job registration, and logging.
*   Three BigQuery tables (`project.dataset.job_registry`, `project.dataset.job_log`, `project.dataset.job_error_log`) for centralized auditing and logging.
*   An assumed BigQuery Stored Procedure, `project.dataset.sp_k_ausd_v_ta_cntrct_templ`, which will contain the core reconciliation logic, replacing the original `k_ausd_v_ta_cntrct_templ.ksh` script.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`project/dataset/table_ddl/job_registry.sql`**
    *   **Role**: Defines the schema for the `job_registry` table. This table is used to store metadata for each job run, including a unique job number (`job_nr`), job identifier (`job_kennung`), script name, current status (e.g., `RUNNING`, `SUCCESS`, `ERROR`), and timestamps for start, end, and last update. It serves as the central control table for job execution status.

*   **`project/dataset/table_ddl/job_log.sql`**
    *   **Role**: Defines the schema for the `job_log` table. This table captures general informational messages, warnings, and other non-critical events during a job's execution. Each entry is linked to a specific job run via `job_nr` and includes a log level, message, and timestamp.

*   **`project/dataset/table_ddl/job_error_log.sql`**
    *   **Role**: Defines the schema for the `job_error_log` table. This table is dedicated to storing detailed error information when a job fails. It includes the `job_nr`, job identifier, error number/code, additional error arguments, the detailed error message, and the timestamp of the error.

*   **`project/dataset/sp_vertragsdatenabgleich.sql`**
    *   **Role**: This BigQuery Stored Procedure replaces the original `r_ausd_v_ta_cntrct_templ.ksh` wrapper script. It is responsible for:
        *   Accepting input parameters (`p_show_help`, `p_param_s`, `p_param_l`).
        *   Generating a unique job number and registering the job in `job_registry`.
        *   Logging job start, progress, and completion messages to `job_log`.
        *   Handling parameter validation, including displaying a help message.
        *   Invoking the core processing logic via `CALL project.dataset.sp_k_ausd_v_ta_cntrct_templ`.
        *   Implementing robust error handling using `EXCEPTION WHEN ERROR THEN` blocks, logging detailed errors to `job_error_log`, and updating `job_registry` with the final status.
        *   Updating the job status in `job_registry` upon successful completion.

## 3. Key design decisions

1.  **BigQuery Stored Procedures for Orchestration**: The original KornShell wrapper script's orchestration logic (parameter parsing, environment setup, logging, core script invocation) was directly translated into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This leverages BigQuery's native capabilities for procedural logic and eliminates the need for external compute for this layer.
2.  **Centralized BigQuery Logging and Auditing**: Instead of file-based logging and custom shell functions (`DWMSG_*`), a structured logging framework using dedicated BigQuery tables (`job_registry`, `job_log`, `job_error_log`) was implemented. This provides a unified, queryable, and scalable solution for tracking job execution, status, and errors.
3.  **Replacement of Shell Features with BigQuery SQL Equivalents**:
    *   `getopts` for parameter parsing was replaced by direct input parameters to the Stored Procedure.
    *   `trap` for error handling was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks.
    *   `print` and `tee` for output were replaced by `INSERT` statements into the `job_log` table.
    *   Date utilities were replaced by BigQuery's `CURRENT_TIMESTAMP()` and `FORMAT_DATE()` functions.
4.  **Assumption of Core Logic Migration**: A key decision was to assume that the core business logic, originally in `k_ausd_v_ta_cntrct_templ.ksh`, would also be migrated to a separate BigQuery Stored Procedure (`sp_k_ausd_v_ta_cntrct_templ`). This allows for a fully BigQuery-native solution for the entire workflow.
5.  **Trade-offs**:
    *   **Performance of Logging**: Frequent `INSERT` statements into logging tables might introduce minor overhead compared to simple file appends. However, BigQuery's optimized ingestion and query capabilities mitigate this for most scenarios. For very high-volume logging, streaming inserts or batching could be considered.
    *   **Operational Paradigm Shift**: Moving from shell scripting to BigQuery SQL Stored Procedures requires a shift in operational mindset, debugging techniques, and monitoring tools. While BigQuery provides robust error handling, the asynchronous nature of shell `trap` commands is replaced by synchronous `EXCEPTION` blocks.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
    ```sql
    CREATE SCHEMA project.dataset;
    ```
2.  **IAM Permissions**:
    *   Grant the service account or user executing the BigQuery Stored Procedures the necessary IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` to create tables and insert/update data in `job_registry`, `job_log`, and `job_error_log`.
        *   `BigQuery Job User` to run queries and procedures.
        *   `BigQuery Data Viewer` on any source/target tables that `sp_k_ausd_v_ta_cntrct_templ` will read from or write to.
3.  **Deploy Logging Tables**: Execute the DDL for the logging tables:
    *   `project/dataset/table_ddl/job_registry.sql`
    *   `project/dataset/table_ddl/job_log.sql`
    *   `project/dataset/table_ddl/job_error_log.sql`
4.  **Migrate and Deploy Core Logic (`sp_k_ausd_v_ta_cntrct_templ`)**:
    *   **Crucially**, the core processing script `k_ausd_v_ta_cntrct_templ.ksh` must be fully migrated to `project.dataset.sp_k_ausd_v_ta_cntrct_templ` and deployed to BigQuery *before* `sp_vertragsdatenabgleich` can be executed successfully. This is a prerequisite and is considered a separate migration effort (B4 item).
5.  **Deploy Wrapper Stored Procedure**: Execute the DDL for `project/dataset/sp_vertragsdatenabgleich.sql`.
6.  **Orchestration Setup**:
    *   Configure a Cloud Composer (Airflow) DAG or a BigQuery Scheduled Query to invoke `project.dataset.sp_vertragsdatenabgleich` with the appropriate parameters. This includes defining the schedule, any required environment variables, and parameter values for `p_param_s` and `p_param_l`.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up or represent known gaps:

*   **Core Script Migration (B4 Item)**: The content and complexity of `k_ausd_v_ta_cntrct_templ.ksh` are currently unknown. Its migration to `project.dataset.sp_k_ausd_v_ta_cntrct_templ` is a critical, separate, and potentially complex task. Depending on its logic (e.g., file I/O, external system interactions, non-SQL logic), it might require migration to other GCP services like Dataflow, Dataproc, or Cloud Functions, orchestrated by Cloud Composer. This is the biggest unknown and risk.
*   **Detailed `s` and `l` Parameter Usage**: The specific business logic and impact of the `-s` and `-l` parameters (now `p_param_s` and `p_param_l`) within the original `k_ausd_v_ta_cntrct_templ.ksh` are not fully detailed in the wrapper's design. Their exact usage needs to be understood and replicated correctly in `sp_k_ausd_v_ta_cntrct_templ`.
*   **`$HOME/.dw_init` Content**: The exact environment variables and configurations set by the original `.dw_init` script need to be identified. These might need to be replicated as BigQuery project/dataset IDs, runtime configuration parameters, or environment variables within the orchestration layer (e.g., Cloud Composer).
*   **Shell-specific Behavior**: While functional equivalents exist, the exact behavior and timing of shell features like `tee` (for simultaneous console/file output) and `trap` (for asynchronous error handling) might differ in the BigQuery SQL environment. Thorough testing is required to ensure no subtle behavioral changes impact the overall process.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prerequisites**: Ensure all manual steps (Section 4) are completed, especially the deployment of `sp_k_ausd_v_ta_cntrct_templ` (even if it's a stub for initial testing).
2.  **Execute the Stored Procedure**:
    *   **Help Message**: Call the procedure with `p_show_help = TRUE`:
        ```sql
        CALL project.dataset.sp_vertragsdatenabgleich(TRUE, NULL, NULL);
        ```
    *   **Successful Run**: Call the procedure with valid parameters (replace `param_s_value` and `param_l_value` with actual expected values):
        ```sql
        CALL project.dataset.sp_vertragsdatenabgleich(FALSE, 'param_s_value', 'param_l_value');
        ```
    *   **Error Scenario**: Simulate an error in `sp_k_ausd_v_ta_cntrct_templ` (e.g., by making it intentionally raise an error or fail a data operation) and then call `sp_vertragsdatenabgleich`.
3.  **Verify Logging Tables**:
    *   Query `project.dataset.job_registry` to check the job status, start/end times, and `job_nr`.
    *   Query `project.dataset.job_log` for informational messages, ensuring all expected log entries are present.
    *   Query `project.dataset.job_error_log` to verify error details for failed runs.
4.  **Data Reconciliation Verification (Post-Core Script Migration)**: Once `sp_k_ausd_v_ta_cntrct_templ` is fully migrated, verify that the data reconciliation performed by the BigQuery procedures yields identical results to the legacy system. This involves comparing output tables or data states.

**"Passing" means**:
*   The `sp_vertragsdatenabgleich` procedure completes without raising unhandled errors.
*   For successful runs, `job_registry.status` is `SUCCESS` and `job_log` contains expected completion messages.
*   For runs with `p_show_help = TRUE`, `job_registry.status` is `COMPLETED_WITH_HELP` and the help message is logged.
*   For error scenarios, `job_registry.status` is `ERROR`, `job_error_log` contains detailed error information, and `job_log` contains an error message.
*   All parameters (`p_param_s`, `p_param_l`) are correctly passed to and utilized by `sp_k_ausd_v_ta_cntrct_templ`.
*   The data reconciliation logic (once `sp_k_ausd_v_ta_cntrct_templ` is complete) produces the expected and correct output, matching the legacy system's behavior.

## 7. Rollback procedure

In case of critical issues or if the migration needs to be reverted, follow these steps:

1.  **Stop Orchestration**: Immediately disable or delete any Cloud Composer DAGs or BigQuery Scheduled Queries that invoke `project.dataset.sp_vertragsdatenabgleich`.
2.  **Delete BigQuery Stored Procedures**:
    ```sql
    DROP PROCEDURE IF EXISTS project.dataset.sp_vertragsdatenabgleich;
    DROP PROCEDURE IF EXISTS project.dataset.sp_k_ausd_v_ta_cntrct_templ; -- If it was deployed
    ```
3.  **Delete BigQuery Logging Tables**:
    ```sql
    DROP TABLE IF EXISTS project.dataset.job_registry;
    DROP TABLE IF EXISTS project.dataset.job_log;
    DROP TABLE IF EXISTS project.dataset.job_error_log;
    ```
    *Note: Consider archiving these tables before dropping if historical logging data is required.*
4.  **Revert to Legacy System**: Re-enable or restart the original `r_ausd_v_ta_cntrct_templ.ksh` script and its associated scheduling mechanisms in the legacy environment.
5.  **Verify Legacy System**: Confirm that the original job is running correctly and producing expected results.