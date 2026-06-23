# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`. This script, originally responsible for orchestrating a data preparation job related to the `ta_action_assoc` entity, including parameter handling, job status management, and execution of a core SQL script, has been migrated to Google Cloud's BigQuery platform.

The core orchestration logic, parameter parsing, and job management functionality of the KornShell script have been translated into a BigQuery Stored Procedure. Auxiliary functions like error logging and job status tracking, previously handled by shell utilities and temporary files, are now managed by dedicated BigQuery tables. The underlying business logic from `d_ausd_v_ta_action_assoc.sql` is expected to be migrated separately into a BigQuery-compatible format, likely another BigQuery Stored Procedure, which will be invoked by the newly created orchestration procedure.

**Source:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh` (KornShell script)
**Target:** Google BigQuery (Stored Procedure and Tables)

## 2. Generated Artifacts

The migration process generated the following BigQuery-compatible artifacts:

*   **`sql/ddl/job_table.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_table`. This table replaces the implicit job management logic within the original KornShell script, providing a structured way to track active and inactive jobs, including their start and end timestamps.
*   **`sql/ddl/error_log.sql`**
    *   **Role:** Defines the DDL for the `error_log` table. This table centralizes error reporting, replacing the shell script's `echo`/`print` statements and custom error handling functions (`f_alis_msgerr.ksh`). It captures error details such as timestamp, code, arguments, procedure name, and message.
*   **`sql/ddl/job_log.sql`**
    *   **Role:** Defines the DDL for the `job_log` table. This table serves as an audit trail for job executions, storing details like job identifier, entry number, table name, status (STARTED/FINISHED/FAILED), record counts, and execution timestamps. It replaces the temporary file mechanism used in the original script to store processed record counts.
*   **`sql/sp/sp_ausd_v_ta_action_assoc.sql`**
    *   **Role:** This BigQuery Stored Procedure (`sp_ausd_v_ta_action_assoc`) is the primary migrated artifact. It encapsulates the orchestration logic of the original KornShell script. It handles parameter validation, updates job status in `job_table` and `job_log`, invokes the core business logic (expected to be in `sp_d_ausd_v_ta_action_assoc`), and manages error handling and logging.

## 3. Key Design Decisions

The migration approach was guided by the following key design decisions:

*   **BigQuery Stored Procedure for Orchestration:** The KornShell script's role as an orchestrator, handling parameters, control flow, and job management, naturally translates to a BigQuery Stored Procedure. This allows for native BigQuery scripting capabilities, direct interaction with BigQuery tables, and integration within the BigQuery ecosystem.
*   **Dedicated BigQuery Tables for Metadata:** Instead of relying on temporary files, shell variables, or implicit state management, explicit BigQuery tables (`job_table`, `error_log`, `job_log`) were introduced. This provides:
    *   **Persistence:** Job status and error logs are durably stored.
    *   **Queryability:** Metadata can be easily queried for monitoring, auditing, and debugging.
    *   **Scalability:** BigQuery tables are designed for large-scale data, suitable for extensive logging.
*   **Parameter Handling via SP Arguments:** The `getopts` mechanism for command-line arguments in KornShell is replaced by direct input parameters to the BigQuery Stored Procedure. This simplifies invocation and type safety.
*   **Separation of Orchestration and Business Logic:** The migration focuses on the wrapper script's functionality. The core business logic from `d_ausd_v_ta_action_assoc.sql` is identified as a separate migration effort (`sp_d_ausd_v_ta_action_assoc`). This modular approach allows for independent development and testing of the data transformation logic.
*   **BigQuery Scripting for Control Flow:** Shell conditional logic (`if`, `case`) and loops are replaced by BigQuery's `IF...THEN...END IF` statements and other scripting constructs, leveraging the platform's native capabilities.
*   **Standardized Error Handling:** Custom shell error functions are replaced by `INSERT` statements into a centralized `error_log` table and `RAISE USING MESSAGE` for immediate procedure termination, providing a consistent error reporting mechanism.

**Notable Trade-offs:**

*   **Dependency on `sp_d_ausd_v_ta_action_assoc`:** The `sp_ausd_v_ta_action_assoc` procedure is dependent on the prior migration and deployment of the core SQL logic from `d_ausd_v_ta_action_assoc.sql` into a BigQuery Stored Procedure (e.g., `sp_d_ausd_v_ta_action_assoc`). Without this, the orchestration procedure will not perform its primary data processing function.
*   **Increased BigQuery Resource Usage for Metadata:** While beneficial for persistence and queryability, maintaining dedicated tables for job control and logging will incur BigQuery storage and query costs, which were not directly present in the shell script's temporary file approach.

## 4. Manual Steps Before Go-Live

Before the migrated BigQuery Stored Procedure can be used in a production environment, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`my_project.my_dataset` as referenced in the generated code) exists. If not, create it:
        ```sql
        CREATE SCHEMA `my_project.my_dataset`
        OPTIONS(
            location="US" -- or your desired region
        );
        ```
2.  **DDL Deployment for Metadata Tables:**
    *   Execute the DDL scripts to create the necessary metadata tables in the target dataset:
        *   `sql/ddl/job_table.sql`
        *   `sql/ddl/error_log.sql`
        *   `sql/ddl/job_log.sql`
    *   Example for `job_table`:
        ```sql
        CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_table` (
            job_kennung STRING NOT NULL,
            eintrags_nr STRING NOT NULL,
            active_flag BOOL NOT NULL,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP
        );
        ```
3.  **IAM Permissions Configuration:**
    *   The service account or user invoking `sp_ausd_v_ta_action_assoc` must have appropriate BigQuery IAM roles:
        *   `BigQuery Data Editor` on `my_project.my_dataset` to `INSERT`, `UPDATE`, `MERGE` into `job_table`, `error_log`, and `job_log`.
        *   `BigQuery Job User` to execute stored procedures.
        *   Permissions to `SELECT`, `INSERT`, `UPDATE`, `DELETE` on any tables that `sp_d_ausd_v_ta_action_assoc` (the core business logic) interacts with.
4.  **Migration and Deployment of Core SQL Logic:**
    *   The SQL logic from `d_ausd_v_ta_action_assoc.sql` **must be migrated to BigQuery Standard SQL and deployed as a separate BigQuery Stored Procedure**, e.g., `my_project.my_dataset.sp_d_ausd_v_ta_action_assoc`. This procedure must accept `p_JobKennung` and `p_EintragsNr` as parameters.
    *   The placeholder `CALL \`my_project.my_dataset.sp_d_ausd_v_ta_action_assoc\`(p_JobKennung, p_EintragsNr);` in `sp_ausd_v_ta_action_assoc.sql` assumes this procedure exists.
    *   The `v_records` calculation in `sp_ausd_v_ta_action_assoc` currently uses a placeholder table (`some_target_table_after_sql_execution`). This must be updated to accurately reflect the record count processed by `sp_d_ausd_v_ta_action_assoc` (e.g., by having `sp_d_ausd_v_ta_action_assoc` return the count as an `OUT` parameter or by querying the actual target table).
5.  **Deployment of `sp_ausd_v_ta_action_assoc`:**
    *   Execute the `sql/sp/sp_ausd_v_ta_action_assoc.sql` script to create the stored procedure in the target dataset.
6.  **Scheduling Configuration:**
    *   If the original KornShell script was part of a scheduled job (e.g., cron, UC4), configure a new scheduler (e.g., Cloud Composer DAG, Cloud Scheduler, Cloud Functions) to invoke `my_project.my_dataset.sp_ausd_v_ta_action_assoc` with the required `p_JobKennung` and `p_EintragsNr` parameters.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration design and remain as known gaps or require further follow-up:

*   **Core SQL Logic Migration (`d_ausd_v_ta_action_assoc.sql`):** This is the most critical unresolved item. The content of `d_ausd_v_ta_action_assoc.sql` was not part of this migration scope and must be analyzed, translated to BigQuery Standard SQL, and deployed as `sp_d_ausd_v_ta_action_assoc` before the orchestration procedure can function fully. This may involve Teradata-to-BigQuery SQL conversion if the source database was Teradata.
*   **`job_table` Schema and Data:** The exact schema and existing data within the legacy `job_table` (or its conceptual equivalent) and its `active_flag` logic are unknown. The DDL provided is a generic representation; detailed analysis of the original system's job control mechanism is required to ensure faithful re-implementation in BigQuery.
*   **Error Logging Details (`DWMSG_MeldeFehler`):** The full functionality of the original `DWMSG_MeldeFehler` function and the specific nuances of its `ErrNr` codes are not fully detailed. The current `error_log` table and `RAISE` statements provide a basic equivalent, but a deeper investigation might reveal specific error categories or actions that need to be replicated.
*   **`pruefeParameterGesetzt` Details:** The precise implementation of `pruefeParameterGesetzt` in `h_alis_parameter.ksh` is not fully known. The current BigQuery SP implements basic `NULL` or empty string checks. If the original function had more complex validation rules (e.g., regex, lookup against a list of valid values), these would need to be incorporated.
*   **Orchestration Context:** The broader orchestration context of `k_ausd_v_ta_action_assoc.ksh` (e.g., if it's part of a larger workflow managed by UC4 or another scheduler) needs to be fully understood to design the appropriate BigQuery-native scheduling solution (e.g., Cloud Composer, Cloud Scheduler).
*   **Data Consistency and Transactional Integrity:** The original system's transactional guarantees during job status updates and data processing need careful consideration. BigQuery's transactional capabilities (e.g., multi-statement transactions) should be leveraged if the original system had specific atomicity requirements that go beyond individual statement execution.
*   **`some_target_table_after_sql_execution`:** This is a placeholder in the generated code for calculating `v_records`. It must be replaced with the actual target table(s) that `sp_d_ausd_v_ta_action_assoc` writes to, or `sp_d_ausd_v_ta_action_assoc` should be modified to return the record count directly.

## 6. Validation

To validate the successful migration and functionality of `sp_ausd_v_ta_action_assoc`, perform the following tests:

1.  **Prerequisites:**
    *   Ensure all manual steps (Section 4) have been completed, especially the deployment of `sp_d_ausd_v_ta_action_assoc` (even if it's a dummy version for initial testing).
    *   Verify the `job_table`, `error_log`, and `job_log` tables are empty or contain only test data.

2.  **Test Cases:**

    *   **Successful Execution (Valid Parameters):**
        *   **Action:** Call `sp_ausd_v_ta_action_assoc` with valid `p_JobKennung` and `p_EintragsNr` values.
            ```sql
            CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_1', 'ENTRY_001');
            ```
        *   **Expected Outcome ("Passing" Criteria):**
            *   The procedure completes without raising an unhandled exception.
            *   `job_log` table contains two entries for `('TEST_JOB_1', 'ENTRY_001')`: one with `status = 'STARTED'` and a later one with `status = 'FINISHED'`, including a non-zero `record_count` (if `sp_d_ausd_v_ta_action_assoc` processed data).
            *   `job_table` contains an entry for `('TEST_JOB_1', 'ENTRY_001')` with `active_flag = FALSE` and `end_ts` populated.
            *   `error_log` table remains empty.
            *   The `sp_d_ausd_v_ta_action_assoc` (core logic) is successfully invoked and performs its intended data transformations. Verify the target tables of `sp_d_ausd_v_ta_action_assoc` contain the expected data.

    *   **Missing `p_JobKennung` (Error Case):**
        *   **Action:** Call `sp_ausd_v_ta_action_assoc` with `NULL` or empty `p_JobKennung`.
            ```sql
            CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`(NULL, 'ENTRY_002');
            -- or
            CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('', 'ENTRY_002');
            ```
        *   **Expected Outcome ("Passing" Criteria):**
            *   The procedure terminates with a `RAISE` error message: `FEHLER: 0 E 1 Jobkennung`.
            *   `error_log` table contains an entry with `error_code = 1`, `error_arg = 'Jobkennung'`, and `message = 'Bitte ueber Rahmenscript aufrufen'`.
            *   `job_log` table does *not* contain any `STARTED` or `FINISHED` entries for this invocation (as validation occurs before job logging).

    *   **Missing `p_EintragsNr` (Error Case):**
        *   **Action:** Call `sp_ausd_v_ta_action_assoc` with `NULL` or empty `p_EintragsNr` and a valid `p_JobKennung`.
            ```sql
            CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_2', NULL);
            -- or
            CALL `my_project.my_dataset.sp_ausd_v_ta_action_assoc`('TEST_JOB_2', '');
            ```
        *   **Expected Outcome ("Passing" Criteria):**
            *   The procedure terminates with a `RAISE` error message: `FEHLER: 0 E 1 EintragsNr`.
            *   `error_log` table contains an entry with `error_code = 1`, `error_arg = 'EintragsNr'`, and `message = 'Bitte ueber Rahmenscript aufrufen'`.
            *   `job_log` table does *not* contain any `STARTED` or `FINISHED` entries for this invocation.

    *   **Concurrent Job Handling (if applicable):**
        *   **Action:** Simulate a scenario where an "old" job with the same `p_JobKennung` but different `p_EintragsNr` is marked as active in `job_table`, then run a new job.
            1.  Manually `INSERT` an entry into `job_table`: `('TEST_JOB_3', 'OLD_ENTRY', TRUE, CURRENT_TIMESTAMP(), NULL)`.
            2.  Call `sp_ausd_v_ta_action_assoc` with `('TEST_JOB_3', 'NEW_ENTRY')`.
        *   **Expected Outcome ("Passing" Criteria):**
            *   The `job_table` entry for `('TEST_JOB_3', 'OLD_ENTRY')` is updated to `active_flag = FALSE` and `end_ts` populated.
            *   The new job `('TEST_JOB_3', 'NEW_ENTRY')` executes successfully and its status is correctly reflected in `job_table` and `job_log`.

    *   **Error during `sp_d_ausd_v_ta_action_assoc` (Core Logic Failure):**
        *   **Action:** Modify `sp_d_ausd_v_ta_action_assoc` (for testing purposes) to intentionally `RAISE` an error, then call `sp_ausd_v_ta_action_assoc` with valid parameters.
        *   **Expected Outcome ("Passing" Criteria):**
            *   `sp_ausd_v_ta_action_assoc` catches the error from the sub-procedure and terminates with a `RAISE` message indicating failure.
            *   `error_log` table contains an entry for the unhandled exception within `sp_ausd_v_ta_action_assoc` (error_code -1).
            *   `job_log` table contains an entry for the job with `status = 'FAILED'` and `end_ts` populated.
            *   `job_table` entry for the job is updated to `active_flag = FALSE` and `end_ts` populated.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior with the migrated BigQuery solution, the following steps outline the procedure to roll back to the original KornShell script:

1.  **Stop New Invocations:**
    *   Immediately disable or remove any new scheduling mechanisms (e.g., Cloud Composer DAGs, Cloud Scheduler jobs, Cloud Functions) that are invoking `my_project.my_dataset.sp_ausd_v_ta_action_assoc`.
2.  **Reactivate Original Scheduling:**
    *   Re-enable or reconfigure the original scheduling mechanism (e.g., cron job, UC4 scheduler) that was responsible for invoking `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`.
3.  **Verify Original Script Functionality:**
    *   Monitor the execution of the original KornShell script to ensure it is running as expected and processing data correctly. Check its logs and output.
4.  **Data State Assessment (if necessary):**
    *   If the BigQuery Stored Procedure made any data modifications before the rollback, assess the state of the target tables. Depending on the nature of the changes and the business requirements, a data recovery or reconciliation process might be necessary. This is especially critical if `sp_d_ausd_v_ta_action_assoc` performed `DELETE` or `UPDATE` operations.
5.  **Clean Up BigQuery Artifacts (Optional, Post-Rollback Analysis):**
    *   Once the rollback is confirmed successful and the original system is stable, the BigQuery stored procedure and tables can be dropped if they are no longer needed or if a re-migration is planned.
        ```sql
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.sp_ausd_v_ta_action_assoc`;
        DROP TABLE IF EXISTS `my_project.my_dataset.job_table`;
        DROP TABLE IF EXISTS `my_project.my_dataset.error_log`;
        DROP TABLE IF EXISTS `my_project.my_dataset.job_log`;
        -- Also drop sp_d_ausd_v_ta_action_assoc if it was created
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.sp_d_ausd_v_ta_action_assoc`;
        ```
    *   Analyze the `error_log` and `job_log` tables in BigQuery to understand the root cause of the rollback.