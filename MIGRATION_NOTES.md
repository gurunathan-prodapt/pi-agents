# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_inv_acc.ksh` job, originally a KornShell script orchestrating an Oracle SQL script, to Google Cloud's BigQuery platform. The migration re-platforms the job's control flow, parameter handling, and data transformation logic into BigQuery Stored Procedures and dedicated BigQuery tables for job control and error logging.

The original KornShell script's responsibilities included:
*   Ignoring active jobs to prevent concurrent execution.
*   Invoking a core SQL script (`d_ausd_v_ta_inv_acc.sql`) for data manipulation.
*   Registering job status in an implicit job control mechanism.
*   Deactivating old active jobs.

The target platform is Google BigQuery, utilizing:
*   **BigQuery Stored Procedures**: To encapsulate the orchestration logic (replacing the KornShell script) and the core data transformation logic (replacing the Oracle SQL script).
*   **BigQuery Tables**: For job control, error logging, and all source/target data.

## 2. Generated artifacts

The migration produced the following BigQuery SQL scripts, which define the necessary database objects and procedures:

*   **`my_project.my_dataset.job_table.sql`**
    *   **Role**: Defines the schema for the `job_table` in BigQuery. This table centralizes job status tracking, replacing the implicit and file-based job management of the original KornShell script. It stores information such as `job_kennung`, `eintrags_nr`, `tab_name`, `active_flag`, timestamps, record counts, and error details.

*   **`my_project.my_dataset.error_log.sql`**
    *   **Role**: Defines the schema for the `error_log` table in BigQuery. This table serves as a centralized repository for capturing error messages and operational logs, replacing the `DWMSG_MeldeFehler` calls and standard error output of the original KornShell script.

*   **`my_project.my_dataset.d_ausd_v_ta_inv_acc.sql`**
    *   **Role**: Creates a BigQuery Stored Procedure named `d_ausd_v_ta_inv_acc`. This procedure encapsulates the core data transformation logic originally found in the `d_ausd_v_ta_inv_acc.sql` Oracle script. It performs a `TRUNCATE` and `INSERT` operation, populating `my_project.sof_dataset.sof$ta_inv_acc` from `my_project.sof_dataset.sof$ta_inv_assign`, `sof$ta_inv_def`, and `sof$ta_acc_ref` tables.

*   **`my_project.my_dataset.r_ausd_vertrag_control.sql`**
    *   **Role**: Creates the main BigQuery Stored Procedure named `r_ausd_vertrag_control`. This procedure is the direct replacement for the `k_ausd_v_ta_inv_acc.ksh` KornShell script. It handles:
        *   Parameter validation (`p_JobKennung`, `p_EintragsNr`).
        *   Checking for and ignoring currently active jobs.
        *   Deactivating old active jobs for the same `tab_name`.
        *   Inserting new job entries into `job_table`.
        *   Calling the `d_ausd_v_ta_inv_acc` data transformation procedure.
        *   Capturing and updating record counts.
        *   Comprehensive error handling and logging to `error_log` and `job_table`.

## 3. Key design decisions

*   **Orchestration Re-platforming to BigQuery Stored Procedures**: The entire control flow, parameter handling, and job management logic of the original KornShell script were translated into a BigQuery Stored Procedure (`r_ausd_vertrag_control`). This eliminates the need for external shell environments and leverages BigQuery's native procedural capabilities for a more integrated and scalable solution.
*   **Data Transformation Re-platforming to BigQuery Standard SQL**: The core data manipulation logic from the Oracle SQL script (`d_ausd_v_ta_inv_acc.sql`) was converted to BigQuery Standard SQL and encapsulated within its own BigQuery Stored Procedure (`d_ausd_v_ta_inv_acc`). This promotes modularity and allows for independent testing and maintenance of the transformation logic.
*   **Centralized Job Control and Error Logging Tables**: Instead of relying on temporary files, implicit job states, and shell-based error reporting, dedicated BigQuery tables (`job_table` and `error_log`) were introduced. This provides a structured, queryable, and persistent mechanism for monitoring job status, history, and errors, significantly improving observability and debugging.
*   **Explicit Parameter Handling**: The `getopts` and parameter validation logic from the KornShell script were directly translated into `IN` parameters and `IF` conditions within the BigQuery Stored Procedure, ensuring robust input validation.
*   **Replacement of Temporary Files**: The KornShell script's use of temporary files for inter-process communication (e.g., capturing record counts) was replaced by BigQuery's native variable handling (`DECLARE`, `SET`) and direct querying of target tables within the stored procedure.
*   **Handling of "Ignoring Active Jobs" and "Deactivating Old Jobs"**: This critical control flow logic was implemented using `SELECT COUNT(*)` and `UPDATE` statements against the new `job_table`, ensuring that only one instance of a job for a given `tab_name` is actively running, and previous runs are properly marked as completed or deactivated.
*   **Error Handling and Reporting**: The `DWMSG_MeldeFehler` calls were replaced by `INSERT` statements into the `error_log` table and `SIGNAL SQLSTATE` for controlled error propagation within the BigQuery environment, providing structured error reporting.
*   **Trade-off: Utility Script Reimplementation**: Instead of directly migrating the utility KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.), their relevant functionalities (e.g., error logging, parameter validation) were reimplemented directly within the BigQuery Stored Procedures. This avoids creating BigQuery UDFs for simple shell logic and keeps the solution self-contained.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the `my_project.my_dataset` dataset exists in BigQuery. This dataset will host the `job_table`, `error_log`, and the stored procedures.
    *   Ensure the `my_project.sof_dataset` dataset exists in BigQuery. This dataset is expected to host the source and target tables (`sof$ta_inv_assign`, `sof$ta_inv_def`, `sof$ta_acc_ref`, `sof$ta_inv_acc`).

2.  **Table Schema Creation**:
    *   Execute `my_project.my_dataset.job_table.sql` to create the `job_table`.
    *   Execute `my_project.my_dataset.error_log.sql` to create the `error_log` table.
    *   **Crucially, ensure all source and target tables referenced in `d_ausd_v_ta_inv_acc.sql` are created in `my_project.sof_dataset` with their correct schemas and data types.** These include:
        *   `my_project.sof_dataset.sof$ta_inv_assign`
        *   `my_project.sof_dataset.sof$ta_inv_def`
        *   `my_project.sof_dataset.sof$ta_acc_ref`
        *   `my_project.sof_dataset.sof$ta_inv_acc` (target table)
        *   `my_project.dwtk_dataset.dwtk_meldungen` (if `d_ausd_v_ta_inv_acc.sql` had used it, though the generated code does not)

3.  **BigQuery Stored Procedure Deployment**:
    *   Execute `my_project.my_dataset.d_ausd_v_ta_inv_acc.sql` to create the data transformation procedure.
    *   Execute `my_project.my_dataset.r_ausd_vertrag_control.sql` to create the main control procedure.

4.  **IAM Permissions**:
    *   The service account or user identity that will execute the `r_ausd_vertrag_control` stored procedure must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on `my_project.my_dataset` (for `job_table`, `error_log`, and procedure execution).
        *   `BigQuery Data Viewer` on `my_project.sof_dataset` (for reading source tables).
        *   `BigQuery Data Editor` on `my_project.sof_dataset` (for writing to `sof$ta_inv_acc`).
        *   `BigQuery Job User` (to run BigQuery jobs/procedures).

5.  **Connection Strings / Secrets**:
    *   No explicit connection strings or secrets are required for BigQuery native operations, as authentication is handled via IAM. Ensure the execution environment (e.g., Cloud Composer, Cloud Functions, or direct BigQuery console) is configured with the correct service account.

6.  **Scheduling**:
    *   If this job is part of a larger workflow or requires scheduled execution, configure a scheduler (e.g., Google Cloud Composer/Apache Airflow, Cloud Scheduler, or Workflows) to invoke the `my_project.my_dataset.r_ausd_vertrag_control` stored procedure with the necessary `p_JobKennung` and `p_EintragsNr` parameters.

## 5. Known gaps & unresolved references

The following items were identified as risks or require further analysis and potential follow-up:

*   **Detailed SQL Script Analysis (B4 Item)**: While the `d_ausd_v_ta_inv_acc.sql` was migrated based on the provided snippet, a full, detailed analysis of the original Oracle SQL script's complete DML, functions, and any Oracle-specific constructs (e.g., `PACKAGE:DWPA_UTIL_SKRIPT`) was not performed. Any complex Oracle-specific SQL or PL/SQL logic not covered by the provided snippet might require further adaptation or re-implementation in BigQuery Standard SQL or UDFs. The `DWPA_UTIL_SKRIPT` package's functionality remains unknown and needs to be reverse-engineered if it contains critical logic.
*   **Original `d_ausd_v_ta_inv_acc.sql` Content**: The provided `GENERATED MIGRATION CODE` for `d_ausd_v_ta_inv_acc.sql` only includes a `TRUNCATE` and `INSERT` statement. If the original Oracle SQL script contained `MERGE` statements (as hinted in the design document's "Writes To: `TABLE:VIA` (via `merge via`)") or other complex DML, this logic is currently missing from the migrated procedure and needs to be added. The current migration only handles `sof$ta_inv_acc`.
*   **Environmental Variables**: The original KornShell script relied on environment variables like `$BERT_DIR_ROOT` and `$DW_DIR_UTL`. These have been abstracted away in the BigQuery solution, but if they contained configuration values that need to be dynamic, these should be managed via BigQuery procedure parameters, configuration tables, or external orchestration tools.
*   **Orchestration Context**: The design document mentioned the ksh script being "called by a wrapper script." The broader orchestration context and dependencies of this job within the original environment need to be fully understood to ensure the BigQuery solution integrates seamlessly into the new data pipeline. The current solution assumes direct invocation of `r_ausd_vertrag_control`.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prepare Test Data**: Populate the source tables (`my_project.sof_dataset.sof$ta_inv_assign`, `sof$ta_inv_def`, `sof$ta_acc_ref`) with representative test data, including edge cases and data that would trigger both successful and error conditions.

2.  **Execute the Control Procedure**:
    *   Open the BigQuery console or use a BigQuery client.
    *   Execute the main control stored procedure:
        ```sql
        CALL `my_project.my_dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG_1', 'TEST_ENTRY_NR_1');
        ```
    *   **Test Parameter Validation**: Execute with `NULL` or empty parameters to verify error handling:
        ```sql
        CALL `my_project.my_dataset.r_ausd_vertrag_control`(NULL, 'TEST_ENTRY_NR_2');
        CALL `my_project.my_dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG_3', NULL);
        ```
    *   **Test "Ignoring Active Jobs"**: Run the same job kennung and entry number twice in quick succession. The second run should log a message in `error_log` and `RETURN` without processing.
        ```sql
        CALL `my_project.my_dataset.r_ausd_vertrag_control`('CONCURRENT_JOB', 'CONCURRENT_ENTRY');
        -- Immediately run again
        CALL `my_project.my_dataset.r_ausd_vertrag_control`('CONCURRENT_JOB', 'CONCURRENT_ENTRY');
        ```
    *   **Test "Deactivating Old Jobs"**: Run a job for `ta_inv_acc`, then run another job for `ta_inv_acc` with a different `JobKennung`. The first job's `active_flag` should be set to `FALSE` by the second run.

3.  **Verify Job Status in `job_table`**:
    *   Query the `job_table` to check the status of executed jobs:
        ```sql
        SELECT * FROM `my_project.my_dataset.job_table` ORDER BY created_ts DESC;
        ```
    *   **Passing Criteria**:
        *   Successful runs should have `active_flag = FALSE`, `completed_ts` populated, `record_count` reflecting the number of rows inserted, `error_code = 0`, and `error_message = 'Successfully completed'`.
        *   Runs that were ignored due to an active job should have an entry in `error_log` indicating the ignore, and no corresponding entry in `job_table` for the ignored run (as it returned early).
        *   Runs that encountered validation errors should have `active_flag = FALSE`, `completed_ts` populated, `error_code` matching the validation error (e.g., 193), and `error_message` describing the error.
        *   Runs that caused other SQL errors should have `active_flag = FALSE`, `completed_ts` populated, `error_code = -1` (or specific SQL error code), and `error_message` detailing the SQL error.

4.  **Verify Error Logging in `error_log`**:
    *   Query the `error_log` table:
        ```sql
        SELECT * FROM `my_project.my_dataset.error_log` ORDER BY error_ts DESC;
        ```
    *   **Passing Criteria**: All expected error conditions (parameter validation, SQL errors, ignored active jobs) should have corresponding entries in this table with relevant timestamps, error numbers, and messages.

5.  **Verify Data in Target Table**:
    *   Query the target table `my_project.sof_dataset.sof$ta_inv_acc`:
        ```sql
        SELECT * FROM `my_project.sof_dataset.sof$ta_inv_acc`;
        ```
    *   **Passing Criteria**: The data in `sof$ta_inv_acc` should accurately reflect the transformation logic applied by `d_ausd_v_ta_inv_acc`, matching the expected output based on the source data. The `record_count` in `job_table` should match `COUNT(*)` from `sof$ta_inv_acc`.

## 7. Rollback procedure

In case of issues or a decision to revert, follow these steps to roll back to the original KornShell-based job:

1.  **Stop New BigQuery Job Executions**:
    *   Immediately halt any scheduled or manual executions of the `my_project.my_dataset.r_ausd_vertrag_control` BigQuery stored procedure.

2.  **Reactivate Original Scheduling**:
    *   If the original KornShell job (`k_ausd_v_ta_inv_acc.ksh`) was deactivated, reactivate its scheduler (e.g., cron job, enterprise scheduler) to resume its normal operation.

3.  **Data Rollback (if necessary)**:
    *   **Assess Data Impact**: Determine if any data written by the BigQuery job (`my_project.sof_dataset.sof$ta_inv_acc`) needs to be reverted. Since the `d_ausd_v_ta_inv_acc` procedure performs a `TRUNCATE` before `INSERT`, a simple rollback might involve:
        *   If a backup of `sof$ta_inv_acc` was taken before the BigQuery job ran, restore it.
        *   If no backup, and the data is critical, the table might need to be truncated or specific rows deleted, depending on the data retention policy and the impact of the BigQuery job's run.
    *   **Important**: If the original Oracle job also performs a `TRUNCATE` and `INSERT`, then the data in `sof$ta_inv_acc` would be overwritten by the next successful run of the original job, potentially mitigating the need for a manual data rollback.

4.  **Decommission BigQuery Objects (Optional, for clean-up)**:
    *   Once the rollback is confirmed and the original job is stable, the migrated BigQuery objects can be dropped to clean up the environment. This step should only be performed after a successful and stable rollback.
        ```sql
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.r_ausd_vertrag_control`;
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.d_ausd_v_ta_inv_acc`;
        DROP TABLE IF EXISTS `my_project.my_dataset.job_table`;
        DROP TABLE IF EXISTS `my_project.my_dataset.error_log`;
        -- If the target table was exclusively created for the migration and is not used by the original job, it can also be dropped.
        -- DROP TABLE IF EXISTS `my_project.sof_dataset.sof$ta_inv_acc`;
        ```

5.  **Monitor Original Job**:
    *   Closely monitor the original KornShell job to ensure it is running correctly and processing data as expected after the rollback.