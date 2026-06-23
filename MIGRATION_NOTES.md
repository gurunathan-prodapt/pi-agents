# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `r_ausd_v_ta_p_discount_rr.ksh` from its legacy environment to Google BigQuery. The original script served as an orchestration layer, handling environment setup, parameter parsing, logging, error trapping, and invoking a core data processing script (`k_ausd_v_ta_p_discount_rr.ksh`) for the `ta_p_discount_rr` data reconciliation process.

The migration involved transforming the shell-based orchestration and custom logging framework into BigQuery stored procedures and audit tables. The core data processing logic, originally in `k_ausd_v_ta_p_discount_rr.ksh`, is represented by a placeholder BigQuery stored procedure, signifying that its detailed migration is a subsequent, independent effort.

**Key outcomes of this migration phase:**
*   The shell wrapper script is replaced by a BigQuery stored procedure (`project.dataset.vertragsdatenabgleich_wrapper`).
*   File-based logging and job status tracking are replaced by dedicated BigQuery audit tables and helper stored procedures.
*   Parameter handling and error management are re-implemented using BigQuery SQL constructs.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL files:

*   **`project.dataset.job_error_log_ddl.sql`**
    *   **Role:** DDL for creating the `job_error_log` table. This table stores detailed error messages for job runs, replacing the file-based error logging of the original system.
*   **`project.dataset.job_log_ddl.sql`**
    *   **Role:** DDL for creating the `job_log` table. This table stores general informational log messages for job runs, replacing the standard output and file-based logging.
*   **`project.dataset.job_status_ddl.sql`**
    *   **Role:** DDL for creating the `job_status` table. This table tracks the lifecycle status (e.g., STARTED, COMPLETED, FAILED) of job executions.
*   **`project.dataset.job_stichtag_ddl.sql`**
    *   **Role:** DDL for creating the `job_stichtag` table. This table records the specific "Stichtag" (key date) associated with each job run.
*   **`project.dataset.job_entry_sequence_ddl.sql`**
    *   **Role:** DDL for creating the `job_entry_sequence` table. This table is used to atomically manage and generate unique, sequential entry numbers for each job run, replacing the custom shell-based sequence generation.
*   **`project.dataset.dwmsg_ermittlenr_proc.sql`**
    *   **Role:** BigQuery stored procedure that implements the `DWMSG_ErmittleNr` functionality. It atomically retrieves and increments the job entry number for a given `job_kennung`.
*   **`project.dataset.dwmsg_erzeugeeintrag_proc.sql`**
    *   **Role:** BigQuery stored procedure that implements the `DWMSG_ErzeugeEintrag` functionality. It inserts general log messages into the `job_log` table.
*   **`project.dataset.dwmsg_setzestichtaginfo_proc.sql`**
    *   **Role:** BigQuery stored procedure that implements the `DWMSG_SetzeStichtagInfo` functionality. It records the `Stichtag` for a job run in the `job_stichtag` table.
*   **`project.dataset.dwmsg_meldefehler_proc.sql`**
    *   **Role:** BigQuery stored procedure that implements the `DWMSG_MeldeFehler` functionality. It logs error details into the `job_error_log` table.
*   **`project.dataset.dwmsg_setzestatusok_proc.sql`**
    *   **Role:** BigQuery stored procedure that implements the `DWMSG_SetzeStatusOK` functionality. It updates the job status in the `job_status` table.
*   **`project.dataset.k_ausd_v_ta_p_discount_rr_proc.sql`**
    *   **Role:** Placeholder BigQuery stored procedure for the core business logic. This procedure will eventually contain the full migration of the original `k_ausd_v_ta_p_discount_rr.ksh` script's data processing logic.
*   **`project.dataset.vertragsdatenabgleich_wrapper_proc.sql`**
    *   **Role:** The main BigQuery stored procedure that replaces the `r_ausd_v_ta_p_discount_rr.ksh` wrapper script. It orchestrates the job execution, handles parameters, calls the `DWMSG` logging procedures, and invokes the core logic procedure.

## 3. Key Design Decisions

1.  **Orchestration Layer Replacement**: The KornShell wrapper script's primary function of orchestrating the job, handling parameters, and managing execution flow was directly translated into a BigQuery stored procedure (`vertragsdatenabgleich_wrapper`). This centralizes the job's control within the data platform.
2.  **Centralized Logging and Auditing**: The custom, file-based `DWMSG` logging framework was replaced by a structured, queryable logging system in BigQuery. This involves:
    *   **Dedicated Audit Tables**: `job_error_log`, `job_log`, `job_status`, `job_stichtag`, and `job_entry_sequence` provide a robust, scalable, and queryable repository for all job metadata and logs.
    *   **Helper Stored Procedures**: Each significant `DWMSG` function (e.g., `DWMSG_ErmittleNr`, `DWMSG_MeldeFehler`) was re-implemented as a BigQuery stored procedure. This encapsulates logging logic and promotes reusability.
3.  **Parameter Handling**: Command-line arguments of the original script (e.g., `-s` for `Stichtag`, `-h` for help) are mapped to `IN` parameters of the BigQuery wrapper stored procedure. This provides a clear, type-safe interface for job invocation.
4.  **Error Management**: The shell script's `trap` mechanism for error handling is replaced by BigQuery's `EXCEPTION WHEN ERROR` blocks. This allows for structured error logging into `job_error_log` and graceful failure handling within the BigQuery environment.
5.  **Atomic Job Entry Number Generation**: The original script's method for generating sequential job entry numbers (`DWMSG_ErmittleNr`) was re-implemented using a `MERGE` statement against the `job_entry_sequence` table. This ensures atomic incrementation and avoids race conditions for concurrent job runs of the same `job_kennung`.
6.  **Core Logic Decoupling**: The actual data processing logic from `k_ausd_v_ta_p_discount_rr.ksh` is represented as a separate, placeholder BigQuery stored procedure (`k_ausd_v_ta_p_discount_rr`). This decision acknowledges that the core logic is a substantial migration effort in itself and allows for the wrapper's migration to proceed independently.
7.  **Trade-offs**:
    *   **Increased BigQuery Resource Usage**: Storing logs in BigQuery tables consumes storage and incurs query costs, which is generally more expensive than simple file I/O. However, this is offset by the benefits of centralized observability, scalability, and the ability to perform analytics on job metadata.
    *   **Re-implementation Complexity**: Custom shell utilities and environment variable management had to be re-implemented using BigQuery SQL constructs, which can sometimes be more verbose or require different paradigms than shell scripting.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS project.dataset;
        ```
2.  **IAM Permissions**:
    *   Grant the necessary BigQuery roles to the service account or user that will be executing these procedures. This typically includes:
        *   `BigQuery Data Editor` (for `INSERT` into log tables and potential data modifications by the core script).
        *   `BigQuery Job User` (to run jobs and procedures).
        *   `BigQuery Data Viewer` (to read from source tables, if applicable to the core script).
3.  **Deploy DDLs**:
    *   Execute all `_ddl.sql` files to create the audit and sequence tables:
        *   `project.dataset.job_error_log_ddl.sql`
        *   `project.dataset.job_log_ddl.sql`
        *   `project.dataset.job_status_ddl.sql`
        *   `project.dataset.job_stichtag_ddl.sql`
        *   `project.dataset.job_entry_sequence_ddl.sql`
4.  **Deploy Stored Procedures**:
    *   Execute all `_proc.sql` files to create the helper logging procedures, the core logic placeholder, and the main wrapper procedure:
        *   `project.dataset.dwmsg_ermittlenr_proc.sql`
        *   `project.dataset.dwmsg_erzeugeeintrag_proc.sql`
        *   `project.dataset.dwmsg_setzestichtaginfo_proc.sql`
        *   `project.dataset.dwmsg_meldefehler_proc.sql`
        *   `project.dataset.dwmsg_setzestatusok_proc.sql`
        *   `project.dataset.k_ausd_v_ta_p_discount_rr_proc.sql` (placeholder)
        *   `project.dataset.vertragsdatenabgleich_wrapper_proc.sql`
5.  **Orchestration Update**:
    *   Update the existing job scheduler (e.g., UC4, Cloud Composer/Airflow, Dataform) to invoke the BigQuery stored procedure `project.dataset.vertragsdatenabgleich_wrapper` instead of the original KornShell script.
    *   Ensure the orchestrator passes any required parameters (e.g., `p_stichtag`) correctly.

## 5. Known Gaps & Unresolved References

1.  **Core Script Migration (B4 Item)**: The most significant gap is the full migration of the core business logic contained within `k_ausd_v_ta_p_discount_rr.ksh`. The current `project.dataset.k_ausd_v_ta_p_discount_rr` is a placeholder. This requires a dedicated migration design and implementation effort, as it contains the actual data reconciliation logic for `ta_p_discount_rr`.
2.  **Comprehensive Utility Script Analysis**: While key `DWMSG` functions have been mapped, a thorough analysis of all functionalities provided by the sourced KornShell utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) is still needed. This ensures that any other critical environment variables, helper functions, or specific behaviors are replicated or accounted for in BigQuery.
3.  **DWMSG Framework Completeness**: The migration covers the most apparent `DWMSG` functions. A complete audit of all `DWMSG` functions and their exact behavior (e.g., specific error codes, log file naming conventions, detailed status messages) is recommended to ensure full functional parity.
4.  **Error Trapping Nuances**: BigQuery's `EXCEPTION WHEN ERROR` handles SQL errors. However, specific KornShell `trap` signals (e.g., `INT` for interrupt) might not have direct BigQuery equivalents. If such signal handling was critical for graceful shutdown or specific recovery scenarios, this might need to be addressed at the orchestration layer (e.g., by Cloud Composer's task retry/timeout mechanisms).
5.  **Configuration Management**: The original script might have relied on environment variables or configuration files. While some parameters are now procedure inputs, a comprehensive strategy for managing all configuration (e.g., using BigQuery configuration tables, environment variables in Cloud Composer, or Secret Manager for sensitive data) should be established.

## 6. Validation

To validate the migrated wrapper and logging framework:

1.  **Unit Test Logging Procedures**:
    *   Call each `dwmsg_` helper procedure directly with sample data.
    *   Query the respective audit tables (`job_log`, `job_error_log`, `job_status`, `job_stichtag`, `job_entry_sequence`) to verify that entries are correctly inserted and data types are as expected.
    *   Verify `dwmsg_ermittlenr` correctly increments `entry_nr` for a given `job_kennung` across multiple calls.
2.  **Test Wrapper Execution (Success Path)**:
    *   Execute the main wrapper procedure:
        ```sql
        CALL project.dataset.vertragsdatenabgleich_wrapper(p_job_kennung => 'TEST_JOB', p_stichtag => '2023-01-01');
        ```
    *   **Passing Criteria**:
        *   The procedure completes without BigQuery errors.
        *   Verify entries in `job_log`, `job_status`, `job_stichtag` for `job_kennung = 'TEST_JOB'` and the generated `entry_nr`.
        *   The `job_status` table should show `STARTED` and `COMPLETED` entries.
        *   The `job_entry_sequence` table should show the `entry_nr` incremented for `TEST_JOB`.
        *   The `k_ausd_v_ta_p_discount_rr` placeholder procedure should have been called, evidenced by its log entry in `job_log`.
3.  **Test Wrapper Execution (Help Path)**:
    *   Execute the wrapper with the help flag:
        ```sql
        CALL project.dataset.vertragsdatenabgleich_wrapper(p_help => TRUE);
        ```
    *   **Passing Criteria**:
        *   The procedure should output the usage message and exit without performing any logging or calling the core script.
4.  **Test Wrapper Execution (Error Path)**:
    *   Temporarily modify the `k_ausd_v_ta_p_discount_rr` placeholder procedure to explicitly `RAISE` an error (e.g., `RAISE 'Simulated error in core script';`).
    *   Execute the main wrapper procedure again:
        ```sql
        CALL project.dataset.vertragsdatenabgleich_wrapper(p_job_kennung => 'TEST_JOB_ERROR', p_stichtag => '2023-01-02');
        ```
    *   **Passing Criteria**:
        *   The wrapper procedure should terminate with an error.
        *   Verify entries in `job_log`, `job_status`, `job_error_log` for `job_kennung = 'TEST_JOB_ERROR'`.
        *   The `job_status` table should show `STARTED` and `FAILED` entries.
        *   The `job_error_log` table should contain the simulated error message.

## 7. Rollback Procedure

In case of issues or a decision to revert, follow these steps:

1.  **Revert Orchestration**:
    *   Immediately update the job scheduler (e.g., UC4, Cloud Composer) to stop calling `project.dataset.vertragsdatenabgleich_wrapper` and revert to executing the original KornShell script `r_ausd_v_ta_p_discount_rr.ksh`.
2.  **Monitor Original Job**:
    *   Verify that the original KornShell script is running as expected in the legacy environment.
3.  **Drop BigQuery Objects (Optional but Recommended for Cleanup)**:
    *   If the rollback is permanent, drop the created BigQuery stored procedures and tables:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.vertragsdatenabgleich_wrapper;
        DROP PROCEDURE IF EXISTS project.dataset.k_ausd_v_ta_p_discount_rr;
        DROP PROCEDURE IF EXISTS project.dataset.dwmsg_setzestatusok;
        DROP PROCEDURE IF EXISTS project.dataset.dwmsg_meldefehler;
        DROP PROCEDURE IF EXISTS project.dataset.dwmsg_setzestichtaginfo;
        DROP PROCEDURE IF EXISTS project.dataset.dwmsg_erzeugeeintrag;
        DROP PROCEDURE IF EXISTS project.dataset.dwmsg_ermittlenr;

        DROP TABLE IF EXISTS project.dataset.job_error_log;
        DROP TABLE IF EXISTS project.dataset.job_log;
        DROP TABLE IF EXISTS project.dataset.job_status;
        DROP TABLE IF EXISTS project.dataset.job_stichtag;
        DROP TABLE IF EXISTS project.dataset.job_entry_sequence;
        ```
4.  **Data Recovery (if core script was migrated and ran)**:
    *   **IMPORTANT**: Since the core script `k_ausd_v_ta_p_discount_rr` is currently a placeholder, this wrapper migration itself does not directly modify business data. However, if the core script had been fully migrated and executed, any data modifications it performed would need a specific data recovery plan (e.g., restoring from a snapshot, running an undo script, or re-processing from a known good state). This aspect must be thoroughly addressed during the core script's migration.