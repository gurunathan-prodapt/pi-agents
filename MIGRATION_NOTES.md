# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `k_ausd_v_ta_discount.ksh`, which serves as an orchestration wrapper for a core SQL script (`d_ausd_v_ta_discount.sql`), has been migrated.

**What was migrated:**
The wrapper logic of `k_ausd_v_ta_discount.ksh`, including its parameter handling, job control mechanisms (activating/deactivating job entries), and the invocation of the main SQL processing logic. This script's primary function is to manage job execution state and orchestrate the call to the data transformation script.

**Target Platform:**
The migration target is Google BigQuery. The wrapper logic has been transformed into a BigQuery Stored Procedure (`r_ausd_v_ta_discount`), which interacts with dedicated BigQuery tables for job status (`job_table`), error logging (`job_error_log`), and run control (`job_run_control`). The core SQL processing script (`d_ausd_v_ta_discount.sql`) is represented by a placeholder BigQuery Stored Procedure (`d_ausd_v_ta_discount`) and will be migrated in a subsequent, dependent effort.

## 2. Generated artifacts

The following files were generated as part of this migration:

*   **`your_project/your_dataset/ddl/job_table.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_table` in BigQuery. This table is used to track the active status, start/end times, and other metadata for job executions, mirroring the functionality of the legacy job table.
*   **`your_project/your_dataset/ddl/job_error_log.sql`**
    *   **Role:** Defines the DDL for the `job_error_log` table in BigQuery. This table captures details of any errors encountered during the execution of the migrated procedures, replacing the legacy `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` mechanisms.
*   **`your_project/your_dataset/ddl/job_run_control.sql`**
    *   **Role:** Defines the DDL for the `job_run_control` table in BigQuery. This table is intended to store control information, specifically the number of records processed by the core SQL logic, replacing the temporary file (`tmpFile`) approach used in the original script.
*   **`your_project/your_dataset/procedures/d_ausd_v_ta_discount.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure for the `d_ausd_v_ta_discount.sql` script. It accepts `p_JobKennung` and `p_EintragsNr` as input and returns `records_processed` as an `OUT` parameter. This procedure currently contains simulated processing logic and will be updated with the actual business transformation logic in a future migration phase.
*   **`your_project/your_dataset/procedures/r_ausd_v_ta_discount.sql`**
    *   **Role:** The main migrated BigQuery Stored Procedure. This procedure encapsulates the wrapper logic of the original `k_ausd_v_ta_discount.ksh` script. It handles parameter validation, updates the `job_table` for job status management, calls the `d_ausd_v_ta_discount` procedure for core processing, and manages error logging and transaction control.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Wrapper Logic:** The KornShell script's orchestration and control flow logic were directly translated into a BigQuery Stored Procedure (`r_ausd_v_ta_discount`).
    *   **Why:** This approach leverages BigQuery's native capabilities for complex SQL logic, parameter handling, and transaction management, reducing external dependencies and simplifying deployment within the BigQuery ecosystem. It aligns with the target architecture of centralizing ETL logic within BigQuery.
    *   **Trade-offs:** Requires rewriting shell-specific constructs (e.g., `getopts`, `if/then/else`, external utility calls) into BQSQL. This can sometimes be more verbose for simple control flow but offers better integration with BigQuery data operations.
*   **Dedicated BigQuery Tables for Job Control:** Separate tables (`job_table`, `job_error_log`, `job_run_control`) were created to manage job status, log errors, and capture run metrics.
    *   **Why:** This provides a structured, queryable, and scalable mechanism for job metadata management, replacing disparate shell-based logging and temporary file approaches. It allows for easier monitoring and auditing within BigQuery.
    *   **Trade-offs:** Requires explicit DDL creation and DML operations within the stored procedures, rather than relying on implicit file system operations or external database connections.
*   **Parameter Handling via Procedure Arguments:** Command-line arguments (`p_JobKennung`, `p_EintragsNr`) are directly mapped to input parameters of the BigQuery Stored Procedure.
    *   **Why:** This is the standard and most robust way to pass inputs to BigQuery procedures, ensuring type safety and clear interface definition.
*   **Transaction Management:** The `r_ausd_v_ta_discount` procedure uses `BEGIN TRANSACTION`, `COMMIT TRANSACTION`, and `ROLLBACK TRANSACTION` blocks.
    *   **Why:** To ensure atomicity of job state updates (deactivating old jobs, activating current job, and calling the core logic). If any step within the transaction fails, all changes are rolled back, maintaining data consistency in the `job_table`.
    *   **Trade-offs:** Adds complexity to the procedure logic compared to simple sequential shell commands, but significantly improves data integrity.
*   **BigQuery Native Error Handling:** `RAISE` statements and `EXCEPTION WHEN ERROR` blocks are used for error management, with errors logged to `job_error_log`.
    *   **Why:** Replaces legacy shell error reporting (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`) with BigQuery's built-in exception handling, providing structured error messages and codes that are easily queryable.
*   **Placeholder for Core SQL Logic:** The `d_ausd_v_ta_discount.sql` content is represented by a placeholder procedure.
    *   **Why:** This allows for the independent migration and testing of the wrapper logic, decoupling it from the potentially complex transformation logic of the core SQL script.
    *   **Trade-offs:** The full end-to-end functionality cannot be validated until the core SQL script is fully migrated and integrated. The placeholder procedure simulates behavior, which might not perfectly reflect the final implementation.
*   **Record Count Capture via `OUT` Parameter:** The `d_ausd_v_ta_discount` procedure returns `records_processed` as an `OUT` parameter.
    *   **Why:** This is a clean and efficient way for a called procedure to return a single value to its caller, replacing the legacy temporary file mechanism.

## 4. Manual steps before go-live

Before the migrated solution can be fully operational, the following manual steps must be performed:

1.  **BigQuery Project and Dataset Creation:**
    *   Ensure the BigQuery project (`your_project`) and dataset (`your_dataset`) exist. If not, create them.
    *   `your_project` should be replaced with the actual Google Cloud Project ID.
    *   `your_dataset` should be replaced with the actual BigQuery Dataset ID.
2.  **Deploy DDL for Control Tables:**
    *   Execute the DDL scripts to create the necessary control tables:
        *   `your_project/your_dataset/ddl/job_table.sql`
        *   `your_project/your_dataset/ddl/job_error_log.sql`
        *   `your_project/your_dataset/ddl/job_run_control.sql`
    *   This can be done via the BigQuery UI, `bq` command-line tool, or a CI/CD pipeline.
3.  **Deploy Placeholder Core SQL Procedure:**
    *   Execute the DDL script for the placeholder core SQL procedure:
        *   `your_project/your_dataset/procedures/d_ausd_v_ta_discount.sql`
    *   This procedure must exist before the wrapper procedure can be deployed.
4.  **Deploy Wrapper Stored Procedure:**
    *   Execute the DDL script for the main wrapper stored procedure:
        *   `your_project/your_dataset/procedures/r_ausd_v_ta_discount.sql`
5.  **IAM / Permissions:**
    *   The service account or user identity that will execute the `r_ausd_v_ta_discount` stored procedure must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on `your_project.your_dataset` to `INSERT`, `UPDATE`, and `SELECT` from `job_table`, `job_error_log`, `job_run_control`.
        *   `BigQuery Job User` on `your_project` to run BigQuery jobs (including stored procedures).
        *   Permissions to `CALL` the `d_ausd_v_ta_discount` procedure.
6.  **Scheduling Configuration:**
    *   Configure a scheduler (e.g., Cloud Composer/Airflow, Cloud Scheduler, Cloud Workflows) to invoke the `your_project.your_dataset.r_ausd_v_ta_discount` stored procedure.
    *   The scheduler must pass the required input parameters: `p_JobKennung` and `p_EintragsNr`.
    *   Example Airflow DAG snippet (conceptual):
        ```python
        from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
        # ...
        call_proc = BigQueryExecuteStoredProcedureOperator(
            task_id="call_r_ausd_v_ta_discount",
            project_id="your_project",
            dataset_id="your_dataset",
            procedure_id="r_ausd_v_ta_discount",
            parameters=[
                {"name": "p_JobKennung", "parameterType": {"type": "STRING"}, "value": "YOUR_JOB_KENNUNG"},
                {"name": "p_EintragsNr", "parameterType": {"type": "STRING"}, "value": "YOUR_EINTRAGS_NR"}
            ]
        )
        ```
7.  **Connection Strings / Secrets:**
    *   No explicit connection strings or secrets are required for the BigQuery procedures themselves, as they operate within the BigQuery environment.
    *   If the core `d_ausd_v_ta_discount` procedure (once fully migrated) requires access to external data sources, those connection details would need to be managed separately (e.g., via Cloud Secret Manager).

## 5. Known gaps & unresolved references

*   **Full Migration of `d_ausd_v_ta_discount.sql`**: The most significant gap is that the core business logic contained within `d_ausd_v_ta_discount.sql` has not yet been migrated. The current `d_ausd_v_ta_discount` BigQuery procedure is a placeholder with simulated processing. A dedicated analysis and migration effort is required for this SQL script.
*   **Detailed `starteSQLSkript` Logic**: The original `starteSQLSkript` function's full behavior (e.g., specific connection handling, retry mechanisms, advanced error handling beyond simple exit codes) is not fully known. The BigQuery migration assumes a direct `CALL` to the SQL procedure and standard transaction management. Any complex features of `starteSQLSkript` would need to be explicitly re-implemented or handled by an external orchestrator.
*   **`DWMSG_MeldeFehler` Functionality**: The full scope of the legacy `DWMSG_MeldeFehler` error reporting system is not replicated. The current solution logs errors to `job_error_log`. If `DWMSG_MeldeFehler` included features like email notifications, PagerDuty alerts, or integration with other monitoring systems, these would need to be implemented separately (e.g., via Cloud Logging sinks, Cloud Functions, or Cloud Monitoring alerts based on `job_error_log` entries).
*   **Complexity Signals**: The original `file_complexity` analysis did not provide specific signals, meaning potential hidden complexities or edge cases in the `k_ausd_v_ta_discount.ksh` script might not have been fully identified and addressed in the migration design.
*   **Environment Initialization (`.dw_init`)**: The specific environment variables or configurations set by `.dw_init` are not fully known. The BigQuery procedures operate within their own environment. If `.dw_init` set critical parameters or paths that influence the *logic* rather than just the execution environment, these might need to be explicitly passed or configured within the BigQuery procedures.

## 6. Validation

Validation of the migrated `r_ausd_v_ta_discount` wrapper procedure should focus on its control flow, parameter handling, and interaction with the job control tables.

**How to run the tests:**

1.  **Prerequisites:** Ensure all DDLs and the placeholder `d_ausd_v_ta_discount` procedure are deployed as per Section 4.
2.  **Test Cases:**
    *   **Successful Run:**
        ```sql
        CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_SUCCESS', 'ENTRY_001');
        ```
    *   **Invalid `p_JobKennung`:**
        ```sql
        CALL `your_project.your_dataset.r_ausd_v_ta_discount`(NULL, 'ENTRY_002');
        -- or
        CALL `your_project.your_dataset.r_ausd_v_ta_discount`('', 'ENTRY_003');
        ```
    *   **Invalid `p_EintragsNr`:**
        ```sql
        CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_INVALID_ENTRY', NULL);
        -- or
        CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_INVALID_ENTRY', '');
        ```
    *   **Consecutive Runs (Job Deactivation):**
        ```sql
        CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_CONSECUTIVE', 'ENTRY_A');
        CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_CONSECUTIVE', 'ENTRY_B'); -- Should deactivate ENTRY_A
        ```
    *   **Simulated Failure in `d_ausd_v_ta_discount` (Manual Test):**
        *   Temporarily modify `d_ausd_v_ta_discount` to `RAISE` an error (e.g., `RAISE USING MESSAGE 'Simulated error in core logic';`).
        *   Then call `r_ausd_v_ta_discount`:
            ```sql
            CALL `your_project.your_dataset.r_ausd_v_ta_discount`('TEST_JOB_FAIL', 'ENTRY_FAIL');
            ```
        *   Remember to revert the change in `d_ausd_v_ta_discount` after testing.

**What "passing" means:**

*   **Successful Runs:**
    *   The `CALL` statement completes without raising an error.
    *   In `your_project.your_dataset.job_table`:
        *   For the `p_JobKennung` and `p_EintragsNr` of the current run, there should be one entry with `active_flag = FALSE`, `start_ts` populated, and `end_ts` populated.
        *   For consecutive runs with the same `p_JobKennung` but different `p_EintragsNr`, the *previous* entry for that `p_JobKennung` should have `active_flag = FALSE` and `end_ts` populated.
    *   In `your_project.your_dataset.job_run_control`:
        *   An entry should exist for the `p_JobKennung` and `p_EintragsNr` with `script_name` as `k_ausd_v_ta_discount.ksh_wrapper` and `records_processed` reflecting the value returned by `d_ausd_v_ta_discount`.
    *   In `your_project.your_dataset.job_error_log`: No new entries should be present for the successful run.
*   **Invalid Parameter Runs:**
    *   The `CALL` statement should `RAISE` an error with a message indicating the invalid parameter.
    *   In `your_project.your_dataset.job_error_log`: An entry should be present with the relevant `job_kennung`, `eintrags_nr`, `err_nr` (1001 or 1002), and `err_arg` matching the validation error message.
    *   In `your_project.your_dataset.job_table`: No new active entries should be created, and no existing entries should be modified by the wrapper logic itself (though the `RAISE` will prevent the transaction from committing).
*   **Simulated Failure Run:**
    *   The `CALL` statement should `RAISE` an error.
    *   In `your_project.your_dataset.job_error_log`: An entry should be present for the failed run, detailing the error message from `d_ausd_v_ta_discount`.
    *   In `your_project.your_dataset.job_table`: The transaction should have been rolled back. This means the initial `INSERT` for the current job run should *not* be present, or if it was, it should be `active_flag = FALSE` and `end_ts` populated if the error occurred *after* the initial insert but *before* the final update. The key is that the `job_table` should reflect a consistent state (no hanging `active_flag = TRUE` entries for a failed run).

## 7. Rollback procedure

In the event that the migrated BigQuery solution needs to be rolled back, follow these steps:

1.  **Halt New Invocations:**
    *   Immediately disable or remove the scheduler (e.g., Cloud Composer DAG, Cloud Scheduler job) that invokes the `your_project.your_dataset.r_ausd_v_ta_discount` BigQuery Stored Procedure. This prevents any new runs of the migrated job.
2.  **Reactivate Legacy Script:**
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh` script is available and correctly configured in its legacy environment.
    *   Re-enable or re-schedule the legacy job execution mechanism.
3.  **Data Consistency Check (if applicable):**
    *   Review the `your_project.your_dataset.job_table` for any `active_flag = TRUE` entries corresponding to the migrated job that might have been left in an inconsistent state if a failure occurred mid-transaction. Manually update these to `active_flag = FALSE` and populate `end_ts` if necessary to reflect their actual (failed) completion.
    *   If the `d_ausd_v_ta_discount` procedure (even the placeholder) performed any data modifications that need to be reverted, execute appropriate DML statements to restore the data to its state before the migrated job ran. This is highly dependent on the actual logic of `d_ausd_v_ta_discount.sql`.
4.  **Decommission BigQuery Artifacts:**
    *   **Drop Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `your_project.your_dataset.r_ausd_v_ta_discount`;
        DROP PROCEDURE IF EXISTS `your_project.your_dataset.d_ausd_v_ta_discount`;
        ```
    *   **Drop Control Tables:**
        ```sql
        DROP TABLE IF EXISTS `your_project.your_dataset.job_table`;
        DROP TABLE IF EXISTS `your_project.your_dataset.job_error_log`;
        DROP TABLE IF EXISTS `your_project.your_dataset.job_run_control`;
        ```
    *   These steps remove the migrated components from the BigQuery environment.
5.  **Monitor Legacy System:**
    *   Verify that the legacy `k_ausd_v_ta_discount.ksh` script is running as expected and processing data correctly.