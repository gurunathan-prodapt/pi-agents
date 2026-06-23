# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh`. This script, an orchestration wrapper for a core data reconciliation process related to the `ta_vvl_upgrade` table, has been re-implemented within Google Cloud's BigQuery environment.

The migration involved transforming the shell script's orchestration logic, parameter handling, logging, and error management into BigQuery Stored Procedures and dedicated BigQuery tables. The core data reconciliation logic, previously invoked by the shell script, is also migrated into a BigQuery Stored Procedure.

**Target Platform:** Google Cloud BigQuery.

## 2. Generated artifacts

The migration produced the following BigQuery-specific artifacts:

*   **`job_audit_log.sql`**:
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_audit_log` BigQuery table. This table serves as a centralized, persistent, and queryable repository for all job execution events, including start, finish, and error messages. It replaces the file-based logging mechanism of the legacy KornShell script.
*   **`job_control.sql`**:
    *   **Role:** Defines the DDL for the `job_control` BigQuery table. This table stores the current status and key parameters (like `stichtag`) for each job, enabling centralized job monitoring and control. It replaces internal shell variables and potentially other control mechanisms used in the legacy environment.
*   **`sp_k_ausd_v_ta_vvl_upgrade.sql`**:
    *   **Role:** Defines the BigQuery Stored Procedure `sp_k_ausd_v_ta_vvl_upgrade`. This procedure encapsulates the core data reconciliation logic that was originally contained within the `k_ausd_v_ta_vvl_upgrade.ksh` kernel script. It performs the actual data manipulation (e.g., INSERT, UPDATE, DELETE) on tables like `ta_vvl_upgrade` based on the provided `stichtag`.
*   **`sp_vertragsdatenabgleich.sql`**:
    *   **Role:** Defines the main BigQuery Stored Procedure `sp_vertragsdatenabgleich`. This procedure is the direct replacement for the `r_ausd_v_ta_vvl_upgrade.ksh` KornShell script. It handles:
        *   Parsing input parameters (e.g., `stichtag`, log level).
        *   Initializing job-specific metadata (job entry number, log file name concept).
        *   Logging job start and end events to `job_audit_log`.
        *   Updating job status in `job_control`.
        *   Invoking the core reconciliation logic via `sp_k_ausd_v_ta_vvl_upgrade`.
        *   Implementing robust error handling using BigQuery's `EXCEPTION WHEN ERROR` blocks.

## 3. Key design decisions

The migration approach was guided by the following key design decisions:

*   **BigQuery Stored Procedures for Orchestration**: The entire orchestration logic of `r_ausd_v_ta_vvl_upgrade.ksh` was translated into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This decision centralizes the execution within BigQuery, leveraging its native capabilities for data processing and reducing the need for external compute resources for simple orchestration. It also simplifies deployment and management by keeping the logic close to the data.
*   **Centralized BigQuery Tables for Logging and Control**: File-based logging and internal shell variables were replaced by dedicated BigQuery tables (`job_audit_log` and `job_control`). This provides a persistent, queryable, and scalable logging solution, significantly improving observability, auditability, and ease of debugging compared to scattered log files. Job status and control parameters are now consistently managed within the data warehouse.
*   **Native BigQuery Error Handling**: The KornShell script's `trap INT ERR` and `set -e` mechanisms were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR...END` blocks. This ensures robust error management that is native to the BigQuery SQL environment, allowing for structured error logging and graceful failure handling.
*   **Parameterization via Stored Procedure Inputs**: Command-line argument parsing (`getopts`) from the shell script was directly mapped to `IN` parameters of the BigQuery Stored Procedure. This provides a clear, type-safe interface for invoking the job and simplifies parameter validation.
*   **Core Logic as a Separate Stored Procedure**: The core data reconciliation logic from `k_ausd_v_ta_vvl_upgrade.ksh` was encapsulated in its own BigQuery Stored Procedure (`sp_k_ausd_v_ta_vvl_upgrade`). This promotes modularity, reusability, and allows the core logic to benefit from BigQuery's optimized SQL execution.
*   **Trade-off: Potential External Orchestration**: A notable trade-off is the assumption that `k_ausd_v_ta_vvl_upgrade.ksh` is fully translatable to BigQuery SQL. If the kernel script contains complex file I/O, external system calls, or non-SQL compatible logic, an external orchestrator like Cloud Composer or Cloud Run might be required to execute Python scripts for those specific parts, adding complexity to the overall architecture.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be performed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`DATASET_ID` in `PROJECT_ID.DATASET_ID`) exists. If not, create it:
        ```bash
        bq mk --dataset --default_location=US PROJECT_ID:DATASET_ID
        ```
2.  **IAM Permissions Configuration**:
    *   Grant the service account or user executing the BigQuery procedures the necessary IAM roles. At a minimum, this includes:
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) on the `PROJECT_ID.DATASET_ID` dataset to create tables, stored procedures, and insert/update data.
        *   `BigQuery Job User` on the `PROJECT_ID` to run BigQuery jobs.
3.  **Deploy Schema for Logging and Control Tables**:
    *   Execute the DDL scripts to create the `job_audit_log` and `job_control` tables:
        ```bash
        bq query --use_legacy_sql=false < job_audit_log.sql
        bq query --use_legacy_sql=false < job_control.sql
        ```
4.  **Deploy Stored Procedures**:
    *   Execute the DDL scripts to create the `sp_k_ausd_v_ta_vvl_upgrade` and `sp_vertragsdatenabgleich` stored procedures:
        ```bash
        bq query --use_legacy_sql=false < sp_k_ausd_v_ta_vvl_upgrade.sql
        bq query --use_legacy_sql=false < sp_vertragsdatenabgleich.sql
        ```
5.  **Data Table DDL (if applicable)**:
    *   If `sp_k_ausd_v_ta_vvl_upgrade` manipulates specific data tables (e.g., `ta_vvl_upgrade`), ensure their DDLs are deployed and the tables exist in the target dataset.
6.  **Scheduling Configuration**:
    *   Set up a scheduling mechanism to invoke `sp_vertragsdatenabgleich`. Options include:
        *   **Cloud Scheduler**: For simple, time-based triggers, calling `bq query --use_legacy_sql=false "CALL PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich(...)"`.
        *   **Cloud Composer (Airflow)**: For complex workflows, dependency management, and integration with other GCP services. A DAG would be created to call the stored procedure.
        *   **Cloud Functions/Run**: For event-driven triggers or custom logic to invoke the procedure.
7.  **Parameter Configuration**:
    *   Determine the default or required values for `p_stichtag_in` and `p_log_level_in` when scheduling the procedure.

## 5. Known gaps & unresolved references

The following items were identified as potential gaps or require further analysis:

*   **Content of `k_ausd_v_ta_vvl_upgrade.ksh`**: The detailed implementation of `sp_k_ausd_v_ta_vvl_upgrade` is a placeholder. The actual content of the legacy `k_ausd_v_ta_vvl_upgrade.ksh` script needs thorough analysis. If it contains non-SQL compatible logic (e.g., complex file system operations, external API calls, or specific shell utilities), `sp_k_ausd_v_ta_vvl_upgrade` will need to be augmented or replaced by Python scripts executed via Cloud Run or Cloud Composer. This is a **B4 (Redesign)** item if non-SQL logic is found.
*   **Framework Functions (`DWMSG_...`)**: The exact functionality of the `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` utility functions was inferred as basic logging and status updates. If these functions perform more complex operations (e.g., interacting with external systems, complex data lookups beyond simple logging), the corresponding logic in `sp_vertragsdatenabgleich` might need further refinement.
*   **Performance Considerations**: While BigQuery is highly performant for large-scale data processing, the translation of shell script constructs to SQL might introduce performance differences, especially for operations that were very efficient in the shell (e.g., small file manipulations). Performance testing and optimization may be required.
*   **`set -eu` Equivalence**: The strict error handling provided by `set -eu` in KornShell has been translated to BigQuery's `EXCEPTION WHEN ERROR` blocks. While robust, thorough testing is needed to ensure all edge cases and error conditions are handled with equivalent strictness and desired behavior.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job, perform the following steps:

1.  **Deployment Verification**:
    *   Confirm that `job_audit_log`, `job_control` tables, and `sp_k_ausd_v_ta_vvl_upgrade`, `sp_vertragsdatenabgleich` stored procedures are successfully deployed in the target BigQuery dataset.
    *   Check BigQuery console for their presence and correct schema/definition.
2.  **Functional Testing**:
    *   **Help Functionality**: Execute the main stored procedure with the help flag:
        ```sql
        CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(NULL, NULL, TRUE);
        ```
        Verify that the help message is displayed correctly.
    *   **Successful Execution**: Call the main stored procedure with valid parameters (e.g., current date for `stichtag`):
        ```sql
        CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`('20231026', 'INFO', FALSE);
        ```
    *   **Error Handling**: Introduce a deliberate error within `sp_k_ausd_v_ta_vvl_upgrade` (e.g., by attempting to query a non-existent table) and execute `sp_vertragsdatenabgleich` to trigger the error path.
3.  **Logging and Control Table Verification**:
    *   **`job_audit_log`**: After each execution (success and failure), query the `job_audit_log` table:
        ```sql
        SELECT * FROM `PROJECT_ID.DATASET_ID.job_audit_log` WHERE job_name = 'sp_vertragsdatenabgleich' ORDER BY event_ts DESC LIMIT 10;
        ```
        *   **"Passing" Criteria**: For a successful run, expect `START` and `FINISH` events. For a failed run, expect `START` and `ERROR` events with relevant error messages and stack traces.
    *   **`job_control`**: Query the `job_control` table:
        ```sql
        SELECT * FROM `PROJECT_ID.DATASET_ID.job_control` WHERE job_name = 'sp_vertragsdatenabgleich';
        ```
        *   **"Passing" Criteria**: For a successful run, `job_status` should be 'OK'. For a failed run, `job_status` should be 'FAILED'. `job_entry_no`, `stichtag`, and `updated_ts` should reflect the latest execution.
4.  **Data Reconciliation Validation**:
    *   **"Passing" Criteria**: The most critical validation is to compare the data state or output generated by the BigQuery job (specifically `sp_k_ausd_v_ta_vvl_upgrade`) with the results produced by the legacy `r_ausd_v_ta_vvl_upgrade.ksh` script for the same `stichtag`. This involves:
        *   Running both the legacy and migrated jobs with identical input parameters.
        *   Extracting relevant data from `ta_vvl_upgrade` (or other affected tables) in both environments.
        *   Performing a data comparison (e.g., row counts, checksums, specific column value comparisons) to ensure functional equivalence.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions**: Immediately halt any scheduled or manual executions of the `sp_vertragsdatenabgleich` BigQuery stored procedure.
2.  **Re-enable Legacy Job**: Re-activate the original `r_ausd_v_ta_vvl_upgrade.ksh` KornShell script in the legacy environment. Ensure its scheduling and dependencies are fully restored.
3.  **Data Rollback (if necessary)**:
    *   If the BigQuery job modified data in `ta_vvl_upgrade` or other tables, and these modifications are deemed incorrect or harmful, a data rollback strategy must be executed. This could involve:
        *   Restoring the affected BigQuery tables from a point-in-time backup (if available).
        *   Utilizing BigQuery's time travel feature to revert tables to a state before the problematic job execution.
        *   Executing inverse DML statements (e.g., `DELETE` for `INSERT`s, `UPDATE` to previous values) if the changes are well-understood and reversible.
        *   **Note**: A robust data rollback plan should be established *before* go-live, especially for critical data.
4.  **Revert BigQuery Objects (Optional)**:
    *   If the deployed BigQuery stored procedures or tables are causing issues, they can be dropped or reverted to a previous version.
        *   To drop a stored procedure: `DROP PROCEDURE IF EXISTS PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich;`
        *   To drop a table: `DROP TABLE IF EXISTS PROJECT_ID.DATASET_ID.job_audit_log;`
    *   It is recommended to keep the deployed BigQuery objects for post-mortem analysis unless they are actively causing system instability.
5.  **Root Cause Analysis**: Investigate the reason for the rollback using the `job_audit_log` and other monitoring tools to identify and resolve the underlying issue before attempting re-deployment.