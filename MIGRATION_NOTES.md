# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell wrapper script `r_ausd_bp_ta_msisdn_his.ksh` from its legacy environment to Google BigQuery. The original script was responsible for orchestrating the execution of a core ETL process, handling parameter parsing, validation, date determination, and logging, before invoking a kernel script (`k_ausd_bp_ta_msisdn_his.ksh`).

The migration re-implements this wrapper's orchestration, parameter handling, and logging functionalities as a BigQuery Stored Procedure. The core business logic, originally in `k_ausd_bp_ta_msisdn_his.ksh`, is represented by a placeholder BigQuery Stored Procedure and is subject to its own separate migration. The execution of the new BigQuery wrapper will be managed by a GCP orchestrator (e.g., Cloud Composer/Airflow or Cloud Workflows).

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/my_project.my_dataset.job_registry.sql`**
    *   **Role**: This DDL script creates the `job_registry` table in BigQuery. This table serves as a central repository for tracking the execution status, start/end times, and key metadata (job name, script name, cutoff date) of each job run. It replaces the status tracking mechanisms of the legacy system.
*   **`sql/ddl/my_project.my_dataset.job_message_log.sql`**
    *   **Role**: This DDL script creates the `job_message_log` table. It stores operational messages, informational logs, and success indicators generated during the execution of the wrapper and kernel (stub) stored procedures. This replaces the file-based logging of the original KornShell script.
*   **`sql/ddl/my_project.my_dataset.job_error_log.sql`**
    *   **Role**: This DDL script creates the `job_error_log` table. It captures detailed error information, including error codes, arguments, and messages, for any failed job executions. This centralizes error reporting and replaces the error handling and logging functions (`f_alis_msgerr.ksh`) of the legacy script.
*   **`sql/sp/my_project.my_dataset.k_ausd_bp_ta_msisdn_his.sql`**
    *   **Role**: This is a BigQuery Stored Procedure stub. It acts as a placeholder for the actual core business logic that was contained within the original `k_ausd_bp_ta_msisdn_his.ksh` script. For the purpose of this wrapper migration, it simply logs its invocation. The full implementation of this procedure will be part of a subsequent migration effort.
*   **`sql/sp/my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper.sql`**
    *   **Role**: This is the main BigQuery Stored Procedure that re-implements the `r_ausd_bp_ta_msisdn_his.ksh` wrapper logic. It handles input parameters (`p_stichtag`, `p_wiederanlaufWert`), defaults, validation, job registration, logging to the audit tables, and orchestrates the call to the `k_ausd_bp_ta_msisdn_his` kernel stub. It also incorporates robust error handling using BigQuery's `EXCEPTION WHEN ERROR` blocks.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration**: The decision to re-implement the wrapper as a BigQuery Stored Procedure (`ausd_bp_ta_msisdn_his_wrapper`) leverages BigQuery's native procedural capabilities. This centralizes the orchestration logic within the target data platform, reducing external dependencies and simplifying deployment compared to maintaining separate shell scripts or external services for simple orchestration.
*   **Dedicated BigQuery Audit Tables for Logging**: Instead of file-based logs or custom logging frameworks, a structured logging approach using dedicated BigQuery tables (`job_registry`, `job_message_log`, `job_error_log`) was chosen. This provides a queryable, centralized, and scalable logging solution, making it easier to monitor job status, debug issues, and analyze historical execution data.
*   **Native BigQuery SQL Functions for Utilities**: Legacy shell utility scripts (e.g., `h_alis_parameter.ksh`, `h_alis_date.ksh`) were replaced by native BigQuery SQL functions (e.g., `IFNULL()`, `CURRENT_DATE()`, `FORMAT_DATE()`) and procedural logic. This eliminates external script dependencies and integrates the functionality directly into the BigQuery environment.
*   **Robust Error Handling with `EXCEPTION WHEN ERROR`**: BigQuery's `EXCEPTION WHEN ERROR` blocks and `RAISE` statements were used to replace the shell script's `set -e` and `trap` mechanisms. This provides structured error capture, logging to `job_error_log`, status updates in `job_registry`, and controlled error propagation.
*   **Stub for Kernel Logic (`k_ausd_bp_ta_msisdn_his`)**: Recognizing that the core business logic is complex and requires its own migration, a stub stored procedure was created. This allows the wrapper to be migrated and tested independently, establishing the orchestration framework while deferring the detailed data transformation logic.
*   **Unique Job ID Generation**: Since BigQuery tables do not have an auto-incrementing `INT64` primary key, `UNIX_MICROS(CURRENT_TIMESTAMP())` was used to generate a unique `DW_EintragsNr` for each job run. This ensures distinct entries in the audit tables.
*   **Orchestrator Integration**: The design explicitly calls for an external GCP orchestrator (Cloud Composer/Airflow or Cloud Workflows) to manage scheduling, dependency management, and invocation of the BigQuery Stored Procedure. This offloads complex scheduling logic from the BigQuery environment itself.

## 4. Manual Steps Before Go-Live

Before the migrated wrapper can be put into production, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`my_project.my_dataset` or the chosen equivalent) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS my_project.my_dataset;
        ```
2.  **Deploy DDLs**:
    *   Execute the DDL scripts to create the audit tables:
        *   `sql/ddl/my_project.my_dataset.job_registry.sql`
        *   `sql/ddl/my_project.my_dataset.job_message_log.sql`
        *   `sql/ddl/my_project.my_dataset.job_error_log.sql`
3.  **Deploy Stored Procedures**:
    *   Execute the SQL scripts to create or replace the stored procedures:
        *   `sql/sp/my_project.my_dataset.k_ausd_bp_ta_msisdn_his.sql` (the stub)
        *   `sql/sp/my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper.sql`
4.  **IAM/Permissions**:
    *   Ensure the service account or user identity that will execute the BigQuery Stored Procedures has the necessary IAM roles:
        *   `BigQuery Data Editor` on `my_project.my_dataset` to insert/update data in the audit tables and call stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   If the kernel script (`k_ausd_bp_ta_msisdn_his`) interacts with other tables, appropriate `BigQuery Data Viewer`/`Editor` roles for those tables will be needed.
5.  **Orchestrator Setup**:
    *   Configure a Cloud Composer DAG (Airflow) or Cloud Workflow to schedule and invoke the `my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper` stored procedure.
    *   Define parameters (`p_stichtag`, `p_wiederanlaufWert`) within the orchestrator, potentially using Airflow variables or Workflow inputs.
    *   Ensure the orchestrator's service account has the necessary BigQuery permissions as described above.
6.  **Configuration (if applicable)**:
    *   If any global environment variables (like `BERT_DIR_ROOT` from the legacy system) are needed by the *actual* kernel script (once migrated), consider storing them in a BigQuery configuration table or as Airflow variables, and pass them to the kernel SP. For this wrapper, direct environment sourcing is no longer applicable.

## 5. Known Gaps & Unresolved References

*   **Core Business Logic (`k_ausd_bp_ta_msisdn_his`)**: The most significant gap. The `k_ausd_bp_ta_msisdn_his.sql` is currently a stub. The actual data transformation logic from the original `k_ausd_bp_ta_msisdn_his.ksh` needs to be fully migrated and implemented into this BigQuery Stored Procedure. This will be a separate, substantial migration effort.
*   **Dynamic Source/Target Identification**: The original design document noted a potential for dynamic table referencing (`AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG"`). If the `k_ausd_bp_ta_msisdn_his` script indeed uses dynamic SQL or dynamically determines source/target tables, this complexity must be addressed during its migration.
*   **`BERT_DIR_ROOT` and other Environment Variables**: The legacy script sourced `$HOME/.dw_init` and used `BERT_DIR_ROOT`. While the wrapper itself no longer directly uses these, if the kernel script's logic depends on such configurations, a BigQuery-native approach (e.g., configuration tables, BigQuery parameters, or Airflow variables) must be designed for the kernel migration.
*   **Date Logic Nuances**: The design document highlighted a discrepancy between `MIN(sysdate, maxladedatum)` in comments and `p_stichtag=$v_sysdate` in active code. The migration followed the active code's logic (defaulting `v_stichtag` to `CURRENT_DATE()` if `p_stichtag` is not provided). This should be confirmed with business users if the `maxladedatum` logic was ever intended to be active.
*   **Orchestration Details**: While Cloud Composer/Workflows is identified as the target orchestrator, the specific DAG/Workflow definition (e.g., scheduling, retries, alerts) is not part of this migration and needs to be developed.

## 6. Validation

To validate the successful migration and functionality of the BigQuery wrapper:

1.  **Deploy All Artifacts**: Ensure all DDLs and Stored Procedures listed in Section 2 are successfully deployed to the target BigQuery dataset.

2.  **Test `ausd_bp_ta_msisdn_his_wrapper` with Valid Parameters**:
    *   **Scenario 1: All parameters provided.**
        ```sql
        CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(DATE '2023-10-26', 1);
        ```
        *   **Passing Criteria**:
            *   The call completes without error.
            *   `my_project.my_dataset.job_registry` contains one entry for `AUSD_BP_TA_MSISDN_HIS` with `status = 'OK'`, `stichtag = '2023-10-26'`, and `finished_at` populated.
            *   `my_project.my_dataset.job_message_log` contains entries indicating job start, kernel call, and job completion for the corresponding `job_entry_nr`.
            *   `my_project.my_dataset.job_error_log` is empty for this `job_entry_nr`.
    *   **Scenario 2: `p_stichtag` omitted (defaults to `CURRENT_DATE()`).**
        ```sql
        CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(NULL, 1);
        ```
        *   **Passing Criteria**: Same as Scenario 1, but `stichtag` in `job_registry` should be `CURRENT_DATE()`.
    *   **Scenario 3: `p_wiederanlaufWert` omitted (defaults to 0).**
        ```sql
        CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(DATE '2023-10-26', NULL);
        ```
        *   **Passing Criteria**: Same as Scenario 1, and the message in `job_message_log` for kernel call should show `Wiederanlaufwert: 0`.
    *   **Scenario 4: Both `p_stichtag` and `p_wiederanlaufWert` omitted.**
        ```sql
        CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(NULL, NULL);
        ```
        *   **Passing Criteria**: Same as Scenario 1, with `stichtag = CURRENT_DATE()` and `Wiederanlaufwert: 0`.

3.  **Test `ausd_bp_ta_msisdn_his_wrapper` with Error Scenarios**:
    *   **Scenario 5: Simulate an error in the kernel script.**
        *   Modify `sql/sp/my_project.my_dataset.k_ausd_bp_ta_msisdn_his.sql` to uncomment the `RAISE EXCEPTION 'Simulated error in kernel script.';` line and redeploy the stub.
        *   Execute the wrapper:
            ```sql
            CALL my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(DATE '2023-10-27', 1);
            ```
        *   **Passing Criteria**:
            *   The call should fail and raise an exception.
            *   `my_project.my_dataset.job_registry` contains one entry for `AUSD_BP_TA_MSISDN_HIS` with `status = 'ERROR'` and `finished_at` populated.
            *   `my_project.my_dataset.job_error_log` contains an entry for the corresponding `job_entry_nr` with `error_message` related to the simulated kernel error.
            *   `my_project.my_dataset.job_message_log` should contain the job start message, but not the job completion message.
        *   **IMPORTANT**: After testing, revert the `k_ausd_bp_ta_msisdn_his` stub to its original state (comment out the `RAISE` statement) and redeploy.

4.  **End-to-End Orchestration Test**:
    *   Once the orchestrator (Cloud Composer DAG or Cloud Workflow) is configured, trigger it manually.
    *   **Passing Criteria**:
        *   The orchestrator job completes successfully (or fails as expected in error scenarios).
        *   The BigQuery audit tables (`job_registry`, `job_message_log`, `job_error_log`) reflect the execution status and logs as per the above criteria.

## 7. Rollback Procedure

In case of critical issues or if the migration needs to be reverted, follow these steps:

1.  **Stop New Executions**:
    *   Disable or pause the Cloud Composer DAG or Cloud Workflow that invokes the new BigQuery wrapper.
2.  **Re-enable Legacy Scheduling**:
    *   Re-enable the original scheduling mechanism for `r_ausd_bp_ta_msisdn_his.ksh` in the legacy environment.
3.  **Verify Legacy System Functionality**:
    *   Monitor the legacy script's execution to ensure it is running correctly and processing data as expected.
4.  **Optional: Clean Up BigQuery Artifacts**:
    *   If the rollback is deemed permanent, the BigQuery stored procedures and audit tables can be dropped. **Caution**: Ensure no other processes or future migrations depend on these tables before dropping them.
    *   Drop the wrapper stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper;
        ```
    *   Drop the kernel stub stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS my_project.my_dataset.k_ausd_bp_ta_msisdn_his;
        ```
    *   Drop the audit tables:
        ```sql
        DROP TABLE IF EXISTS my_project.my_dataset.job_registry;
        DROP TABLE IF EXISTS my_project.my_dataset.job_message_log;
        DROP TABLE IF EXISTS my_project.my_dataset.job_error_log;
        ```