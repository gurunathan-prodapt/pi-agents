# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh`. This script, responsible for preparing cutoff-date-based contract cache data for "Forderungsscoring" (FOS) in the BERT system, has been migrated from a legacy Unix environment to Google Cloud Platform (GCP).

The migration targets BigQuery, where the original KornShell wrapper logic has been transformed into a BigQuery Stored Procedure. This new procedure handles parameter parsing, validation, logging, and orchestrates the execution of the core business logic, which is expected to be migrated into a separate BigQuery Stored Procedure.

## 2. Generated artifacts

The migration process has generated the following BigQuery SQL artifacts:

*   **`project.dataset.job_run_log.sql`**
    *   **Role**: DDL for the `job_run_log` table. This table serves as the primary audit log for each execution instance of the migrated job, capturing start/end times, overall status, and a summary message. It replaces the high-level job status tracking previously managed by the `DWMSG` framework.
*   **`project.dataset.job_error_log.sql`**
    *   **Role**: DDL for the `job_error_log` table. This table stores detailed information about any errors encountered during job execution, including timestamps, error codes, messages, and stack traces. It replaces the error reporting aspects of the `DWMSG` framework and shell `trap` error handling.
*   **`project.dataset.job_metadata_log.sql`**
    *   **Role**: DDL for the `job_metadata_log` table. This table captures key metadata related to each job run, such as processed parameters (`Stichtag`, `Wiederanlaufwert`) and simulated log file names. It replaces the `DWMSG_SetzeStichtagInfo` and similar metadata logging functions.
*   **`project.dataset.job_status_log.sql`**
    *   **Role**: DDL for the `job_status_log` table. This table provides granular, step-by-step status updates throughout the job's execution, offering a detailed timeline of its progress. It replaces the various `DWMSG_ErzeugeEintrag` calls for progress reporting.
*   **`project.dataset.bereitstellung_basisprodukte_bert.sql`**
    *   **Role**: BigQuery Stored Procedure. This is the core migrated component, directly replacing the `r_ausd_bp_ta_iccid_einzeln.ksh` wrapper script. It accepts `p_stichtag` and `p_wiederanlaufWert` as input, performs parameter defaulting and validation, logs its activities to the new audit tables, and orchestrates the call to the kernel logic (expected to be `project.dataset.k_ausd_bp_ta_iccid_einzeln`). It includes robust error handling using `BEGIN...EXCEPTION` blocks.

## 3. Key design decisions

*   **Wrapper to BigQuery Stored Procedure**: The KornShell wrapper script's orchestration logic (parameter handling, logging, kernel invocation) was directly translated into a BigQuery Stored Procedure (`bereitstellung_basisprodukte_bert`). This leverages BigQuery's native capabilities for procedural logic and allows for direct integration with BigQuery data processing.
*   **Dedicated Logging Tables**: The proprietary `DWMSG` framework's logging and status management were replaced by a set of purpose-built BigQuery tables (`job_run_log`, `job_error_log`, `job_metadata_log`, `job_status_log`). This provides a structured, queryable, and scalable logging solution within BigQuery.
*   **Delegation of Kernel Logic**: The original script's primary function was to invoke a "kernel script" (`k_ausd_bp_ta_iccid_einzeln.ksh`). This design decision was maintained in the migration, with the BigQuery wrapper Stored Procedure calling a *separate* BigQuery Stored Procedure (`k_ausd_bp_ta_iccid_einzeln`) that will encapsulate the kernel's data processing logic. This promotes modularity and separation of concerns.
*   **BigQuery Native Constructs for Shell Functions**: Shell-specific functionalities like date handling (`date +%d%m%Y`), parameter parsing, and environment variable management were replaced with BigQuery SQL equivalents (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`, `COALESCE`, `IF` statements, and Stored Procedure parameters).
*   **Robust Error Handling**: Shell `trap` mechanisms were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks within the Stored Procedure. This ensures that errors are caught, logged to `job_error_log`, and the overall job status in `job_run_log` is updated appropriately, before re-raising the error to the caller.
*   **Trade-offs**:
    *   **Loss of direct OS interaction**: The migration to BigQuery Stored Procedures means losing direct access to the underlying operating system for file system operations, external commands, or complex environment variable management. This is mitigated by BigQuery's powerful SQL capabilities and the structured logging approach.
    *   **Dependency on Kernel Migration**: The full functionality of this migrated wrapper is dependent on the successful and accurate migration of the `k_ausd_bp_ta_iccid_einzeln.ksh` kernel script into its corresponding BigQuery Stored Procedure.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **IAM Permissions**:
    *   Grant appropriate BigQuery IAM roles to the service account or user that will execute the `bereitstellung_basisprodukte_bert` Stored Procedure. This includes:
        *   `BigQuery Data Editor` on the `project.dataset` for inserting into logging tables and calling other Stored Procedures.
        *   `BigQuery Job User` for running BigQuery jobs.
        *   Permissions to read from source DWH tables and write to target FOS tables (these will be defined during the kernel script migration).
3.  **Deploy Logging Tables**: Execute the DDL scripts for `project.dataset.job_run_log.sql`, `project.dataset.job_error_log.sql`, `project.dataset.job_metadata_log.sql`, and `project.dataset.job_status_log.sql` to create these tables in the target BigQuery dataset.
4.  **Deploy Wrapper Stored Procedure**: Execute the `project.dataset.bereitstellung_basisprodukte_bert.sql` script to create the main wrapper Stored Procedure in the target BigQuery dataset.
5.  **Migrate and Deploy Kernel Stored Procedure**: **Crucially**, the `k_ausd_bp_ta_iccid_einzeln.ksh` kernel script *must be migrated* into its own BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_iccid_einzeln`) and deployed to the same dataset. The wrapper will fail without this dependency.
6.  **Data Migration**:
    *   Migrate all necessary source data from the legacy DWH tables (e.g., `DWH_VERTRAG_ID`) to their corresponding BigQuery tables.
    *   Migrate any existing data from the legacy "FOS-Tabelle" to its BigQuery counterpart, if historical data needs to be preserved.
7.  **Scheduling**: Configure a scheduling mechanism (e.g., Cloud Composer DAG, Cloud Scheduler, or a custom application) to invoke the `project.dataset.bereitstellung_basisprodukte_bert` Stored Procedure with the required `p_stichtag` and `p_wiederanlaufWert` parameters at the desired frequency.

## 5. Known gaps & unresolved references

The following items are identified as known gaps or require further follow-up:

*   **`k_ausd_bp_ta_iccid_einzeln.ksh` (Kernel Script) Migration**: This is the most significant unresolved item. The content, complexity, data sources, and target of this script are currently unknown. Its migration into `project.dataset.k_ausd_bp_ta_iccid_einzeln` is a prerequisite for the wrapper to function correctly and represents the bulk of the actual data transformation logic.
*   **`DWMSG_*` Framework Parity**: While logging tables replace the `DWMSG` framework, a thorough analysis is needed to ensure all nuances of the original framework's logging, error handling, and metadata management (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_Fehlerbehandlung`) are fully replicated in BigQuery.
*   **`DWDate_Gib_Zeitraum` Function**: The exact logic of this date helper function needs to be precisely mapped to BigQuery's date functions to guarantee identical date calculations, especially for cutoff date determination.
*   **`Wiederanlaufwert` Logic**: The "delete entries >= this value" logic, which resides in the kernel script, needs careful migration. This will likely involve BigQuery `MERGE` statements or a combination of `DELETE` and `INSERT` to ensure the correct upsert/re-processing behavior for the FOS table.
*   **Error Handling Parity (Signals)**: The shell `trap` commands for various signals (`INT`, `STOP`, `CONT`, `ERR`) provide process-level robustness. While BigQuery Stored Procedures offer `EXCEPTION` blocks, direct replication of OS signal handling is not possible. External orchestration (e.g., Cloud Composer) might be required for comprehensive process monitoring and recovery.
*   **`Stichtag` Default Logic Discrepancy**: The original script's comments suggest `MIN(sysdate, maxladedatum)` for `Stichtag` defaulting, but the active code uses `sysdate`. The current BigQuery migration reflects the *active code's* `sysdate` logic. A business decision is needed to confirm if the `MIN(sysdate, maxladedatum)` logic should be implemented instead.

## 6. Validation

Validation ensures the migrated BigQuery Stored Procedure functions as expected and produces correct results.

**How to run tests:**

1.  **Prerequisites**: Ensure all logging tables and the `bereitstellung_basisprodukte_bert` Stored Procedure are deployed. The `k_ausd_bp_ta_iccid_einzeln` Stored Procedure (kernel) must also be deployed, even if it's a stub for initial wrapper testing.
2.  **Execute the Stored Procedure**: Call the `project.dataset.bereitstellung_basisprodukte_bert` Stored Procedure from the BigQuery console, a client library, or an orchestration tool (e.g., Cloud Composer).
    *   **Successful Scenario**:
        ```sql
        CALL project.dataset.bereitstellung_basisprodukte_bert('01012023', 0);
        CALL project.dataset.bereitstellung_basisprodukte_bert(NULL, 100); -- Test Stichtag defaulting
        CALL project.dataset.bereitstellung_basisprodukte_bert('31122022', NULL); -- Test Wiederanlaufwert defaulting
        ```
    *   **Failure Scenario (e.g., missing Stichtag)**:
        ```sql
        CALL project.dataset.bereitstellung_basisprodukte_bert(NULL, NULL); -- Should trigger Stichtag validation error
        ```
3.  **Query Logging Tables**: After each execution, query the `job_run_log`, `job_error_log`, `job_metadata_log`, and `job_status_log` tables using the `run_id` to inspect the job's execution details.
4.  **Verify Kernel Execution**: If the kernel SP is fully migrated, verify its impact on the target FOS table.

**What "passing" means:**

*   **Successful Execution**:
    *   The `CALL` statement completes without raising an unhandled error.
    *   The corresponding entry in `project.dataset.job_run_log` shows `status = 'SUCCESS'` and `end_timestamp` is populated.
    *   `project.dataset.job_error_log` contains no entries for the `run_id` of the successful execution.
    *   `project.dataset.job_status_log` shows a logical progression of status messages, including "Job started", "Parameters processed", "Stichtag validated", "Calling kernel stored procedure", "Kernel stored procedure completed successfully", and "Job completed successfully".
    *   `project.dataset.job_metadata_log` accurately reflects the input and processed parameters (`stichtag_processed`, `wiederanlaufWert_processed`) and the simulated `log_file_name`.
    *   **Functional Equivalence**: The data generated or modified by the `project.dataset.k_ausd_bp_ta_iccid_einzeln` Stored Procedure (the kernel) in the target FOS table must be identical to the output produced by the original `k_ausd_bp_ta_iccid_einzeln.ksh` script when run with the same inputs. This is the ultimate measure of success for the end-to-end process.
*   **Expected Failure (e.g., invalid parameters)**:
    *   The `CALL` statement raises an error (e.g., `MESSAGE_TEXT = 'Stichtag parameter is required...'`).
    *   The corresponding entry in `project.dataset.job_run_log` shows `status = 'FAILED'` and `end_timestamp` is populated.
    *   `project.dataset.job_error_log` contains an entry for the `run_id` with the expected error message and details.
    *   `project.dataset.job_status_log` shows "Job failed" and the error message.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Halt New Executions**: Immediately stop any scheduled or manual invocations of the `project.dataset.bereitstellung_basisprodukte_bert` BigQuery Stored Procedure.
2.  **Revert Orchestration**: If an orchestrator (e.g., Cloud Composer) was configured to call the BigQuery Stored Procedure, revert its configuration to call the original `r_ausd_bp_ta_iccid_einzeln.ksh` script (or its legacy scheduling mechanism).
3.  **Delete BigQuery Stored Procedures**:
    ```sql
    DROP PROCEDURE IF EXISTS project.dataset.bereitstellung_basisprodukte_bert;
    DROP PROCEDURE IF EXISTS project.dataset.k_ausd_bp_ta_iccid_einzeln; -- If migrated
    ```
4.  **Delete BigQuery Logging Tables (Optional but Recommended for Cleanliness)**:
    ```sql
    DROP TABLE IF EXISTS project.dataset.job_run_log;
    DROP TABLE IF EXISTS project.dataset.job_error_log;
    DROP TABLE IF EXISTS project.dataset.job_metadata_log;
    DROP TABLE IF EXISTS project.dataset.job_status_log;
    ```
    *Note*: If these tables contain valuable audit history, consider renaming them or archiving their data instead of dropping them.
5.  **Restore Data (if necessary)**: If the migrated kernel Stored Procedure (`k_ausd_bp_ta_iccid_einzeln`) modified any target data (e.g., the FOS table) in a way that is incompatible with the legacy system, restore the affected target tables from a backup taken *before* the migration or from the last successful run of the legacy job. This step is critical and depends heavily on the kernel's logic and the data retention strategy.
6.  **Verify Legacy System**: Confirm that the original `r_ausd_bp_ta_iccid_einzeln.ksh` script can execute successfully and produce correct results in the legacy environment.