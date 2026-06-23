# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `r_ausd_bp_ta_cntrct_evn.ksh` from its legacy environment to Google BigQuery. The original script was responsible for orchestrating the initial provisioning of selected base products for the BERT system, handling parameter parsing, date determination, logging, error handling, and delegating core business logic to a kernel script.

The migration involved converting the KornShell wrapper's logic into a BigQuery Stored Procedure. The target platform is Google BigQuery, leveraging its native scripting capabilities for control flow, parameter management, and structured logging. The core business logic, originally in `k_ausd_bp_ta_cntrct_evn.ksh`, is represented by a placeholder BigQuery Stored Procedure in this phase, awaiting a separate, detailed migration.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`bigquery/project/dataset/job_log.sql`**
    *   **Role:** Defines the `job_log` table in BigQuery. This table serves as the central repository for all operational logs, informational messages, warnings, and errors generated during the execution of the migrated job and its sub-components. It replaces the legacy file-based logging mechanism.
*   **`bigquery/project/dataset/job_control.sql`**
    *   **Role:** Defines the `job_control` table in BigQuery. This table tracks the overall status and metadata of job executions, including job number, name, script name, log file reference, `stichtag` information, and start/end timestamps. It replaces the legacy job status tracking.
*   **`bigquery/project/dataset/k_ausd_bp_ta_cntrct_evn_core.sql`**
    *   **Role:** Defines a placeholder BigQuery Stored Procedure named `k_ausd_bp_ta_cntrct_evn_core`. This procedure is intended to encapsulate the core business logic originally found in `k_ausd_bp_ta_cntrct_evn.ksh`. For this migration phase, it only logs its invocation, serving as an interface for the wrapper procedure. Its detailed implementation requires a separate migration effort.
*   **`bigquery/project/dataset/ausd_bp_ta_cntrct_evn_wrapper.sql`**
    *   **Role:** Defines the `ausd_bp_ta_cntrct_evn_wrapper` BigQuery Stored Procedure. This is the direct migration of the `r_ausd_bp_ta_cntrct_evn.ksh` KornShell script. It handles input parameters (`p_stichtag`, `p_wiederanlaufWert`), determines the effective cutoff date, manages job logging and control table updates, and orchestrates the call to the `k_ausd_bp_ta_cntrct_evn_core` procedure. It also implements BigQuery-native error handling.

## 3. Key design decisions

*   **Wrapper to BigQuery Stored Procedure:** The KornShell wrapper script, which primarily handles orchestration, parameter validation, and logging, was migrated to a BigQuery Stored Procedure (`ausd_bp_ta_cntrct_evn_wrapper`). This decision leverages BigQuery's native scripting capabilities for control flow, variable management, and direct interaction with BigQuery tables for logging and job control, eliminating the need for external shell environments.
*   **Structured Logging and Job Control:** The legacy file-based logging and custom `DWMSG_*` functions were replaced by dedicated BigQuery tables (`job_log` and `job_control`). This provides a structured, queryable, and scalable logging solution, enabling easier monitoring, auditing, and debugging within the BigQuery ecosystem.
*   **Parameter Handling Translation:** KornShell's `getopts` and environment variable handling for parameters (`-s`, `-l`) were directly translated to `IN` parameters of the BigQuery Stored Procedure. Defaulting logic (e.g., for `p_wiederanlaufWert` and `p_stichtag`) was implemented using BigQuery SQL functions like `IFNULL` and `COALESCE`, and `IF...THEN...END IF` blocks.
*   **Date Logic Modernization:** Legacy shell commands for date determination (`DWDate_Gib_Zeitraum`, `v_sysdate`) were replaced with BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()` functions, providing native and efficient date manipulation.
*   **BigQuery-Native Error Handling:** The KornShell's `set -e` and `trap` mechanisms were re-engineered using BigQuery scripting's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, `ASSERT` statements for validation, and `RAISE` for custom error signaling. This ensures robust error management integrated with BigQuery's execution model.
*   **Modular Core Logic Invocation:** The core business logic, originally in `k_ausd_bp_ta_cntrct_evn.ksh`, was designed as a separate BigQuery Stored Procedure (`k_ausd_bp_ta_cntrct_evn_core`) to be invoked by the wrapper using a `CALL` statement. This promotes modularity, separation of concerns, and allows for a phased migration approach where the complex core logic can be developed and tested independently.
*   **No Direct Data Transformation in Wrapper:** Consistent with its original role, the migrated wrapper procedure (`ausd_bp_ta_cntrct_evn_wrapper`) performs no direct data transformations or aggregations. Its function remains purely orchestration and context preparation.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure a dedicated BigQuery project and dataset (e.g., `project.dataset`) are created and configured for the migrated assets.
2.  **IAM and Permissions Configuration:**
    *   Set up appropriate Google Cloud IAM roles and service accounts. The service account used for executing the BigQuery Stored Procedures (e.g., via an orchestrator) must have:
        *   `BigQuery Data Editor` role on `project.dataset` to create/update tables and procedures.
        *   `BigQuery Job User` role to run queries and procedures.
        *   Permissions to read/write to any source/target tables that `k_ausd_bp_ta_cntrct_evn_core` will eventually interact with.
3.  **Deployment of BigQuery Artifacts:**
    *   Execute the `CREATE TABLE` statements for `job_log.sql` and `job_control.sql` in the target BigQuery dataset.
    *   Execute the `CREATE OR REPLACE PROCEDURE` statements for `k_ausd_bp_ta_cntrct_evn_core.sql` and `ausd_bp_ta_cntrct_evn_wrapper.sql` in the target BigQuery dataset.
4.  **Core Logic Implementation:**
    *   **Crucially, the `k_ausd_bp_ta_cntrct_evn_core` procedure is currently a placeholder.** A separate, detailed migration and implementation of the actual data extraction and transformation logic from the original `k_ausd_bp_ta_cntrct_evn.ksh` script must be completed and deployed as `project.dataset.k_ausd_bp_ta_cntrct_evn_core`. This includes defining any source (DWH) and target (FOS-Tabelle) BigQuery tables.
5.  **Orchestration Setup:**
    *   Configure a Google Cloud orchestration service (e.g., Cloud Composer/Apache Airflow, Cloud Workflows, Cloud Run) to schedule and trigger the `project.dataset.ausd_bp_ta_cntrct_evn_wrapper` BigQuery Stored Procedure.
    *   Define the parameters (`p_stichtag`, `p_wiederanlaufWert`) to be passed to the wrapper procedure via the orchestrator.
    *   Ensure the orchestrator has the necessary IAM permissions to invoke BigQuery procedures.
6.  **Configuration Management:**
    *   If the legacy `.dw_init` or other utility scripts contained global configurations, these should be re-evaluated. If still needed, they might be managed via BigQuery configuration tables, environment variables in the orchestrator, or directly embedded in the procedures.

## 5. Known gaps & unresolved references

The following items are identified as known gaps or require further follow-up:

*   **Core Logic Migration (`k_ausd_bp_ta_cntrct_evn.ksh`):** This is the most significant unresolved item. The `k_ausd_bp_ta_cntrct_evn_core` BigQuery Stored Procedure is currently a placeholder. A dedicated, in-depth analysis and migration design for the original `k_ausd_bp_ta_cntrct_evn.ksh` script is required to implement its actual data extraction, transformation, and loading logic. This includes identifying its data sources (DWH) and target tables (FOS-Tabelle) and translating their SQL/shell logic to BigQuery.
*   **Detailed Logic of Sourced Utilities:** The exact functionalities of legacy utility scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` need to be fully understood. While general BigQuery equivalents have been used, a thorough review is needed to ensure no critical business logic or specific error handling from these utilities is missed. Complex or reusable logic might warrant dedicated BigQuery UDFs.
*   **Legacy Logging Framework Fidelity:** The custom `DWMSG_*` logging framework in the original KornShell script might have specific features (e.g., integration with legacy monitoring, specific alert triggers) that are not directly replicated by simple inserts into `job_log`. A review is needed to ensure that all critical aspects of the legacy logging and alerting are covered, potentially requiring integration with Google Cloud Logging and Monitoring.
*   **Performance Considerations for Core Logic:** While the wrapper itself is lightweight, the performance of the future `k_ausd_bp_ta_cntrct_evn_core` procedure will be critical. Its design must consider BigQuery best practices for query optimization, partitioning, clustering, and efficient data loading.
*   **Orchestration Design Details:** The specific choice and detailed design of the orchestration mechanism (e.g., Cloud Composer DAG structure, Cloud Workflows definition, Cloud Run service configuration) need to be finalized. This includes defining retry policies, dependency management, and monitoring alerts.
*   **Target Data Table Definition:** The schema for the target table(s) where the contract event data will be made available for demand scoring (e.g., `project.dataset.fos_contract_events`) needs to be defined and created. This is dependent on the analysis of `k_ausd_bp_ta_cntrct_evn.ksh`.

## 6. Validation

Validation of the migrated wrapper script involves ensuring that it correctly handles parameters, manages job status, logs appropriately, and successfully invokes the placeholder core logic.

**How to run the tests:**

1.  **Direct BigQuery Procedure Execution:**
    *   Use the BigQuery UI, `bq` command-line tool, or a client library to directly `CALL` the `project.dataset.ausd_bp_ta_cntrct_evn_wrapper` procedure.
    *   **Test Case 1: All parameters provided.**
        ```sql
        CALL `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`('01012023', 100);
        ```
    *   **Test Case 2: Only `stichtag` provided.**
        ```sql
        CALL `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`('15032023', NULL);
        ```
    *   **Test Case 3: No parameters provided (defaults to current date, restart 0).**
        ```sql
        CALL `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`(NULL, NULL);
        ```
    *   **Test Case 4: Invalid `stichtag` (e.g., empty string, though `NULLIF` handles this).**
        ```sql
        -- This should trigger the error handling for missing Stichtag
        CALL `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`('', NULL);
        ```
2.  **Query `job_log` and `job_control` tables:** After each execution, query these tables to verify entries.
    ```sql
    SELECT * FROM `project.dataset.job_control` ORDER BY created_at DESC LIMIT 5;
    SELECT * FROM `project.dataset.job_log` ORDER BY created_at DESC LIMIT 10;
    ```
3.  **Orchestration Trigger (End-to-End):**
    *   Once the orchestration (e.g., Cloud Composer DAG) is set up, trigger it manually.
    *   Monitor the orchestrator's logs for successful execution.
    *   Verify the BigQuery `job_log` and `job_control` tables for entries generated by the orchestrator-triggered run.

**What "passing" means:**

*   **Successful Execution:** The `ausd_bp_ta_cntrct_evn_wrapper` procedure completes without raising unhandled BigQuery errors.
*   **Correct Parameter Handling:**
    *   `v_effective_stichtag` is correctly derived (either from input `p_stichtag` or `v_sysdate`).
    *   `v_restart_value` is correctly derived (from input `p_wiederanlaufWert` or defaults to `0`).
*   **Accurate Logging:**
    *   The `job_control` table contains a new entry for each execution, with `status` transitioning from `RUNNING` to `OK` upon successful completion.
    *   The `job_log` table contains informational messages, including the job start message with correct `Stichtag` and `RestartValue`, and the success message ("Die Abarbeitung wurde ohne erkennbare Fehler beendet").
    *   The `k_ausd_bp_ta_cntrct_evn_core` placeholder procedure is successfully invoked, and its invocation message appears in `job_log`.
*   **Robust Error Handling:**
    *   When an expected error condition is met (e.g., missing `stichtag` parameter), the procedure `RAISE`s an error, logs an `E` (Error) level message in `job_log`, and updates the `job_control` status to `ERROR`.
*   **No Unexpected Costs/Performance:** The execution of the wrapper procedure should be fast and incur minimal BigQuery costs, as it primarily involves control flow and metadata operations.

## 7. Rollback procedure

In case of issues with the migrated job, the following rollback procedure can be followed:

1.  **Deactivate Orchestration:**
    *   Immediately pause or delete the schedule/DAG/workflow in the Google Cloud orchestration service (e.g., Cloud Composer, Cloud Workflows) that triggers the `ausd_bp_ta_cntrct_evn_wrapper` BigQuery Stored Procedure. This prevents further execution of the migrated job.
2.  **Revert to Legacy Scheduler:**
    *   Re-enable the original `r_ausd_bp_ta_cntrct_evn.ksh` KornShell script in its legacy scheduler (e.g., cron, Autosys, etc.). Ensure it is configured to run as per its original schedule and parameters.
3.  **BigQuery Artifact Cleanup (Optional, but Recommended):**
    *   **Drop BigQuery Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_bp_ta_cntrct_evn_core`;
        ```
    *   **Drop BigQuery Tables:**
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        DROP TABLE IF EXISTS `project.dataset.job_control`;
        ```
        *Note: Be cautious when dropping tables, especially `job_log` and `job_control`, as they contain historical execution data. Consider archiving them first if historical data is critical.*
4.  **Data Impact Assessment:**
    *   Since the `ausd_bp_ta_cntrct_evn_wrapper` itself does not perform data transformations, its direct impact on business data is minimal. However, if the placeholder `k_ausd_bp_ta_cntrct_evn_core` was partially or incorrectly implemented and had written data, a separate data rollback or correction strategy would be required for the target tables (e.g., `fos_contract_events`). This is outside the scope of the wrapper's direct rollback.
5.  **Review and Root Cause Analysis:**
    *   Analyze the `job_log` table (if not dropped) and orchestrator logs to identify the root cause of the failure before attempting re-migration.