# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh`. The original script was responsible for orchestrating the cleanup of temporary intermediate tables by invoking a core cleanup script (`k_drop_temp_table.ksh`), handling parameters, logging, and error management.

The job has been migrated to Google Cloud BigQuery. The wrapper logic, parameter handling, logging, and error management are now implemented as a BigQuery Stored Procedure. The core cleanup logic, which was previously in `k_drop_temp_table.ksh`, is also expected to be migrated into a separate BigQuery Stored Procedure. Logging and status updates, previously file-based, are now directed to dedicated BigQuery tables.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`project.dataset.create_log_tables.sql`**
    *   **Role:** This script is responsible for creating the necessary BigQuery tables (`job_log` and `job_status`) that will store execution logs, error messages, and job status updates for the migrated process. These tables replace the file-based logging mechanism of the original KornShell script.
*   **`project.dataset.k_drop_temp_table.sql`**
    *   **Role:** This is a placeholder BigQuery Stored Procedure that represents the migrated logic from the original `k_drop_temp_table.ksh` script. Its primary role, once fully implemented, will be to identify and drop temporary tables based on provided parameters like `p_stichtag` and `p_wiederanlaufwert`. The current version includes logging capabilities but requires the actual table dropping logic to be added.
*   **`project.dataset.bert_drop_temp_table_wrapper.sql`**
    *   **Role:** This BigQuery Stored Procedure is the direct equivalent of the original `r_drop_temp_table.ksh` wrapper script. It handles input parameter parsing (`p_stichtag_in`, `p_wiederanlaufWert_in`), applies default values, validates input, manages logging to the `job_log` table, and orchestrates the call to the core cleanup procedure (`project.dataset.k_drop_temp_table`). It also incorporates robust error handling using BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures**: The entire logic, including the wrapper and the core cleanup, is reimplemented as BigQuery Stored Procedures. This leverages BigQuery's native capabilities for data processing, eliminates the need for external compute environments for the core logic, and simplifies deployment within the Google Cloud ecosystem.
*   **Centralized BigQuery Logging**: File-based logging (`print`, `echo`, `tee`) from the original KornShell script is replaced by structured logging to dedicated BigQuery tables (`job_log`, `job_status`). This provides a centralized, queryable, and scalable logging solution, improving observability and debugging.
*   **Native BigQuery Control Flow and Error Handling**: Shell-specific constructs like `getopts` for parameter parsing, `if/case` statements for conditional logic, and `trap` commands for error handling are replaced by BigQuery SQL's `IN` parameters, `IF...THEN...ELSE`, `ASSERT`, and `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This ensures robust and idiomatic BigQuery SQL implementation.
*   **Decoupling Wrapper and Core Logic**: The wrapper (`BERT_DROP_TEMP_TABLE`) and core cleanup (`k_drop_temp_table`) are kept as separate BigQuery Stored Procedures. This maintains modularity, allowing independent development, testing, and potential reuse of the core cleanup logic.
*   **Cloud-Native Orchestration**: The original UC4 scheduler dependency is replaced by a recommendation for Google Cloud's native scheduling services (Cloud Composer, Cloud Workflows, or Cloud Scheduler). This aligns with cloud best practices and provides flexible, scalable orchestration.
*   **Parameter Translation**: KornShell command-line arguments (`-s`, `-l`) are directly translated into `IN` parameters for the BigQuery Stored Procedures, maintaining the original job's configurability.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it in your Google Cloud project.
2.  **IAM Permissions**:
    *   Grant the necessary BigQuery IAM roles to the service account that will execute these stored procedures and the orchestrating service (e.g., Cloud Composer service account, Cloud Workflows service account). Required roles typically include:
        *   `BigQuery Data Editor` (for `INSERT` into log/status tables, and `DROP TABLE` in `k_drop_temp_table`).
        *   `BigQuery Job User` (to run queries and stored procedures).
3.  **Deploy Logging and Status Tables**:
    *   Execute the `project.dataset.create_log_tables.sql` script to create the `job_log` and `job_status` tables in your BigQuery dataset.
4.  **Implement Core Cleanup Logic (`k_drop_temp_table`)**:
    *   **CRITICAL**: The `project.dataset.k_drop_temp_table.sql` script is currently a placeholder. The actual logic from the original `k_drop_temp_table.ksh` script must be thoroughly analyzed and translated into BigQuery SQL `DROP TABLE` or `DELETE` statements. This may involve:
        *   Identifying the temporary tables to be dropped.
        *   Determining how `p_stichtag` and `p_wiederanlaufwert` influence the selection of tables.
        *   Potentially querying BigQuery's `INFORMATION_SCHEMA` or a custom metadata table (e.g., `temp_table_registry`) to find tables to drop.
        *   Using `EXECUTE IMMEDIATE` for dynamic `DROP TABLE` statements.
5.  **Deploy BigQuery Stored Procedures**:
    *   Execute the `project.dataset.k_drop_temp_table.sql` (after full implementation) and `project.dataset.bert_drop_temp_table_wrapper.sql` scripts to create or replace these stored procedures in your BigQuery dataset.
6.  **Configure Orchestration/Scheduling**:
    *   Replace the UC4 scheduler by configuring a Google Cloud scheduling mechanism (e.g., Cloud Composer DAG, Cloud Workflow, or Cloud Scheduler job) to invoke the `project.dataset.BERT_DROP_TEMP_TABLE` stored procedure. This configuration will also be responsible for passing the `p_stichtag_in` and `p_wiederanlaufWert_in` parameters.
7.  **`temp_table_registry` (if applicable)**:
    *   If the `k_drop_temp_table` implementation relies on a `temp_table_registry` table (as hinted in the pseudocode), this table must be created and maintained with the metadata of temporary tables to be managed.

## 5. Known gaps & unresolved references

*   **`k_drop_temp_table.ksh` Content (B4 Item)**: The most significant gap is the actual implementation of the core cleanup logic within `project.dataset.k_drop_temp_table.sql`. The provided migration code includes a placeholder, but the detailed analysis and translation of the original `k_drop_temp_table.ksh` script's logic (which was not available in the design document) is a critical follow-up item. This includes understanding how it identifies temporary tables and uses `p_wiederanlaufwert`.
*   **`p_wiederanlaufWert` Usage**: The exact purpose and filtering logic of `p_wiederanlaufWert` within the original `k_drop_temp_table.ksh` are assumed. Its precise role in the BigQuery implementation needs to be confirmed and accurately translated.
*   **Error Code Mapping**: The original script used specific `ErrNr` values (e.g., `192`, `193`). While the BigQuery procedure handles errors, a consistent mapping or replacement strategy for these specific error codes in the `job_log` table should be defined if required for downstream systems.
*   **`DWMSG_ErmittleNr` Replacement**: The `eintragsnr` in the BigQuery logging currently uses a timestamp-based integer. For a truly unique and sequential entry number, a more robust mechanism (e.g., a BigQuery sequence table or a dedicated logging service) might be considered if strict sequentiality is a requirement.
*   **`temp_table_registry` Design**: If the `k_drop_temp_table` procedure is to manage temporary tables effectively, a `temp_table_registry` table (as suggested in the pseudocode) would need a formal design, creation, and a process for populating and updating it.

## 6. Validation

To validate the successful migration and functionality of the `BERT_DROP_TEMP_TABLE` job, follow these steps:

1.  **Unit Testing `k_drop_temp_table` (after implementation)**:
    *   **Setup**: Create a few dummy temporary tables in BigQuery that would typically be targeted by the cleanup logic. Populate a mock `temp_table_registry` if used.
    *   **Execution**: Call `CALL project.dataset.k_drop_temp_table('TEST_JOB', '01012023', 1, 0);` with various `p_stichtag` and `p_wiederanlaufwert` values.
    *   **Passing**:
        *   Verify that the expected temporary tables are dropped.
        *   Check `project.dataset.job_log` for `INFO` messages indicating successful drops and no `ERROR` messages.
        *   If a `temp_table_registry` is used, verify its status updates.
2.  **Unit Testing `BERT_DROP_TEMP_TABLE`**:
    *   **Execution (Success Case)**: Call `CALL project.dataset.BERT_DROP_TEMP_TABLE('01012023', '0');` (or with `NULL` for defaults).
    *   **Passing (Success Case)**:
        *   Query `project.dataset.job_log` for the `v_eintragsnr` generated during the call. Expect `INFO` messages for job start, parameters, core logic execution, and successful completion.
        *   Query `project.dataset.job_status` for the same `v_eintragsnr`. Expect a single entry with `status = 'OK'`.
        *   Verify that the `k_drop_temp_table` was indeed called and performed its cleanup (as per its own validation).
    *   **Execution (Failure Case - Invalid Stichtag)**: Call `CALL project.dataset.BERT_DROP_TEMP_TABLE('INVALID_DATE', '0');`.
    *   **Passing (Failure Case - Invalid Stichtag)**:
        *   The procedure call should raise an error.
        *   Query `project.dataset.job_log`. Expect an `ERROR` entry with `err_nr = 193` and a message indicating invalid `Stichtag` format.
        *   Query `project.dataset.job_status`. Expect an entry with `status = 'ERROR'`.
    *   **Execution (Failure Case - Core Logic Error)**: Simulate an error within `k_drop_temp_table` (e.g., by temporarily modifying it to `RAISE` an error). Then call `CALL project.dataset.BERT_DROP_TEMP_TABLE('01012023', '0');`.
    *   **Passing (Failure Case - Core Logic Error)**:
        *   The wrapper procedure should catch the error and re-raise it.
        *   Query `project.dataset.job_log`. Expect an `ERROR` entry with `err_nr = 999` (or specific error from `k_drop_temp_table`) and a message reflecting the core logic failure.
        *   Query `project.dataset.job_status`. Expect an entry with `status = 'ERROR'`.
3.  **Integration Testing (with Orchestrator)**:
    *   **Setup**: Deploy the Cloud Composer DAG, Cloud Workflow, or Cloud Scheduler job.
    *   **Execution**: Trigger the orchestrator manually or wait for its scheduled run.
    *   **Passing**:
        *   Monitor the orchestrator logs for successful invocation and completion.
        *   Perform the same checks on `project.dataset.job_log` and `project.dataset.job_status` as in the `BERT_DROP_TEMP_TABLE` unit tests.
        *   Confirm that the orchestrator correctly passes parameters to the BigQuery Stored Procedure.

## 7. Rollback procedure

In case of issues or a decision to revert the migration, follow these steps:

1.  **Deactivate/Delete Orchestration**:
    *   Immediately deactivate or delete the Cloud Composer DAG, Cloud Workflow, or Cloud Scheduler job that invokes `project.dataset.BERT_DROP_TEMP_TABLE`. This prevents further execution of the migrated job.
2.  **Re-enable Original Scheduler**:
    *   Re-enable the original UC4 job (`DW.BERT_DROP_TEMP_TABLE.xml`) to resume the legacy process.
3.  **Drop BigQuery Stored Procedures**:
    *   Execute the following commands in BigQuery to remove the migrated stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.BERT_DROP_TEMP_TABLE`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_drop_temp_table`;
        ```
4.  **Drop Logging and Status Tables**:
    *   Execute the following commands to remove the logging and status tables. **Note**: If these tables are used by other processes, consider archiving their data before dropping.
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        DROP TABLE IF EXISTS `project.dataset.job_status`;
        ```
5.  **Data Rollback (if applicable)**:
    *   Since this job primarily drops temporary tables, a data rollback is generally not required for the tables it manages. However, if the `k_drop_temp_table` implementation inadvertently dropped non-temporary tables or modified persistent data, a specific data recovery plan would be needed (e.g., restoring from backups or snapshots).
6.  **Review and Clean Up**:
    *   Ensure all cloud resources related to the migration (e.g., specific IAM roles, any temporary datasets) are cleaned up if no longer needed.
    *   Review logs from both the migrated and original systems to understand the cause of the rollback.