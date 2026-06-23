# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_c_bfc.ksh` from a legacy Unix/KornShell environment to Google Cloud Platform, primarily leveraging Google BigQuery.

The original script acted as an orchestrator and wrapper for a core data processing script (`k_ausd_v_ta_c_bfc.ksh`), handling environment setup, parameter parsing, job logging, and error management for updating a "Bindefristcache" table (`ta_c_bfc`).

The migrated solution re-engineers this orchestration logic into a BigQuery Stored Procedure (`sp_bindefristcache_update`). Shell-based logging is replaced by inserts into dedicated BigQuery audit and error log tables. The core data processing logic, originally in `k_ausd_v_ta_c_bfc.ksh`, is represented by a placeholder BigQuery Stored Procedure (`sp_ausd_v_ta_c_bfc`), whose final implementation depends on further analysis of its internal logic.

## 2. Generated artifacts

The migration process generated the following BigQuery artifacts:

*   **`your_project_id.your_dataset_id.job_audit_log` (BigQuery Table DDL)**
    *   **Role:** This table serves as the central repository for job execution logs, replacing the file-based logging (`$LogDatei`) of the original KornShell script. It captures job start/end times, status, messages, and key identifiers like `job_kennung` and `entry_nr`.
*   **`your_project_id.your_dataset_id.job_error_log` (BigQuery Table DDL)**
    *   **Role:** This table stores detailed error information for failed job executions, replacing the error reporting mechanisms (`DWMSG_MeldeFehler`) of the original script. It includes BigQuery-specific error details like `sqlstate`, `message`, and `stack_trace`.
*   **`your_project_id.your_dataset_id.sp_ausd_v_ta_c_bfc` (BigQuery Stored Procedure)**
    *   **Role:** This is a placeholder stored procedure intended to house the core data processing logic originally found in `k_ausd_v_ta_c_bfc.ksh`. It accepts `p_job_kennung` and `p_entry_nr` as parameters, mirroring the original script's invocation. Its actual implementation will be determined by a separate migration effort for the core logic.
*   **`your_project_id.your_dataset_id.sp_bindefristcache_update` (BigQuery Stored Procedure)**
    *   **Role:** This is the main orchestration stored procedure, directly replacing `r_ausd_v_ta_c_bfc.ksh`. It handles parameter validation (e.g., for help), generates job metadata, logs job status to `job_audit_log`, calls the core logic (`sp_ausd_v_ta_c_bfc`), and manages error handling by logging to `job_error_log` and re-raising exceptions.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration:** The wrapper script's control flow, parameter handling, and logging initiation were directly translated into a BigQuery Stored Procedure (`sp_bindefristcache_update`). This leverages BigQuery's native SQL capabilities for procedural logic, keeping the orchestration close to the data and minimizing external dependencies.
*   **Table-based Logging:** All shell-based logging (`DWMSG_...` functions, `LogDatei`) was replaced by `INSERT` statements into dedicated BigQuery tables (`job_audit_log`, `job_error_log`). This centralizes logging within BigQuery, making it queryable, scalable, and integrated with the data platform.
*   **BigQuery Native Error Handling:** The shell script's `trap` commands and error checking (`if [ ! $ErrNr -eq 0 ]`) were replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks. This provides robust, transactional error management within the stored procedure context.
*   **Placeholder for Core Logic:** The core processing script (`k_ausd_v_ta_c_bfc.ksh`) was migrated as a placeholder BigQuery Stored Procedure (`sp_ausd_v_ta_c_bfc`). This decision acknowledges the unknown complexity of the core logic and defers its detailed migration path (e.g., pure SQL SP, Python Cloud Run, Dataflow) to a separate, focused effort. This allows the orchestration layer to be migrated independently.
*   **Parameter Mapping:** Command-line parameters from the original script (e.g., `-h`) were mapped to BigQuery Stored Procedure input parameters, maintaining functional parity where applicable.
*   **Dynamic Metadata Generation:** The dynamic generation of job identifiers (`JobKennung`, `DW_EintragsNr`) was replicated using BigQuery functions like `FORMAT_TIMESTAMP` and `MAX() + 1` for sequence generation.

**Trade-offs:**
*   **`trap` handling:** BigQuery's `EXCEPTION` blocks provide robust error handling but do not directly replicate the asynchronous signal handling capabilities of shell `trap` commands. This is an acceptable trade-off as BigQuery stored procedures operate in a more controlled, transactional environment.
*   **Core Logic Unknown:** The decision to use a placeholder for `k_ausd_v_ta_c_bfc.ksh` introduces a dependency on future work. However, it allows for incremental migration and avoids blocking the wrapper script's migration.

## 4. Manual steps before go-live

Before the migrated job can be run in a production environment, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_dataset_id` in `your_project_id`) exists. If not, create it:
        ```bash
        bq mk --dataset your_project_id:your_dataset_id
        ```
2.  **IAM Permissions:**
    *   The service account or user executing the BigQuery Stored Procedure must have the following IAM roles:
        *   `BigQuery Data Editor` on `your_project_id.your_dataset_id` to create/update tables and stored procedures, and insert into log tables.
        *   `BigQuery Job User` on `your_project_id` to run BigQuery jobs.
        *   If the core logic (`sp_ausd_v_ta_c_bfc`) interacts with other BigQuery tables (e.g., `ta_c_bfc`), ensure appropriate `BigQuery Data Viewer` and `BigQuery Data Editor` permissions on those tables.
        *   If `sp_ausd_v_ta_c_bfc` is migrated to an external service (e.g., Cloud Run, Dataflow), the BigQuery SP's service account will need `roles/run.invoker` or `roles/dataflow.admin` respectively, and the external service's service account will need permissions to access BigQuery.
3.  **Deploy Generated Artifacts:**
    *   Execute the DDL for `job_audit_log` and `job_error_log` to create these tables in your target dataset.
    *   Execute the DDL for `sp_ausd_v_ta_c_bfc` to create the core logic placeholder stored procedure.
    *   Execute the DDL for `sp_bindefristcache_update` to create the main orchestration stored procedure.
    *   **Important:** Replace `your_project_id.your_dataset_id` placeholders in all generated SQL with your actual project ID and dataset ID before deployment.
4.  **Core Logic Implementation (`sp_ausd_v_ta_c_bfc`):**
    *   The `sp_ausd_v_ta_c_bfc` procedure is currently a placeholder. Its actual implementation, based on the analysis of `k_ausd_v_ta_c_bfc.ksh`, must be developed and deployed. This might involve:
        *   Replacing the placeholder `SELECT FORMAT(...)` with actual SQL DML/DDL.
        *   If the core logic is external (e.g., Cloud Run), the `CALL` statement within `sp_bindefristcache_update` might need to be adjusted to invoke that external service (e.g., via a BigQuery `EXTERNAL_QUERY` or a Cloud Composer DAG).
5.  **Scheduling:**
    *   The original script was likely scheduled via `cron` or a similar job scheduler. The migrated BigQuery Stored Procedure will need to be scheduled using a cloud-native scheduler, such as:
        *   **Cloud Composer (Apache Airflow):** Recommended for complex workflows, dependency management, and monitoring.
        *   **Cloud Scheduler + Cloud Functions:** For simpler, time-based triggers.
        *   **BigQuery Scheduled Queries:** If the job is purely SQL and doesn't require external orchestration.
    *   The scheduler must be configured to `CALL` `your_project_id.your_dataset_id.sp_bindefristcache_update`.
6.  **Connection Strings/Secrets:**
    *   If `sp_ausd_v_ta_c_bfc` (or any part of the migrated solution) requires access to external databases or APIs, ensure connection strings and secrets are securely managed (e.g., using Secret Manager) and accessible to the executing service account.

## 5. Known gaps & unresolved references

*   **Core Script `k_ausd_v_ta_c_bfc.ksh` Logic (B4 Item):** The most significant gap is the actual implementation of the core data processing logic within `sp_ausd_v_ta_c_bfc`. Its migration path (BigQuery SQL SP, PySpark on Dataproc, Python on Cloud Run, Dataflow) is dependent on a detailed analysis of its internal logic, which is currently unknown. This is flagged as a B4 (Redesign) item.
*   **Shell `trap` Handling:** While BigQuery's `EXCEPTION` blocks provide robust error handling, they do not offer a direct equivalent to the asynchronous signal handling of shell `trap` commands (e.g., for `INT` or `ERR` signals). The current design relies on BigQuery's native exception mechanisms, which are sufficient for most data processing scenarios but may behave differently under external signal interruptions.
*   **Undefined CLI Options (`-s`, `-l`):** The original `getopts` `ParamList` included `-s:` and `-l:`, but no explicit handling for these parameters was found in `r_ausd_v_ta_c_bfc.ksh`. It's assumed they are either unused or implicitly handled by sourced scripts. In the migrated solution, these parameters are not supported. If they are critical, further investigation into the original sourced scripts is required.
*   **Dynamic Source Path (`BERT_DIR_ROOT`):** The original script used `$BERT_DIR_ROOT` for dynamic path resolution. This environment-dependent concept is replaced by explicit BigQuery object references (`your_project_id.your_dataset_id.object_name`). If `BERT_DIR_ROOT` implied a more complex, configurable environment, this aspect might need further parameterization in the BigQuery solution (e.g., using BigQuery constants or external configuration).

## 6. Validation

To validate the migrated solution, follow these steps:

1.  **Prerequisites:**
    *   Ensure all manual steps from Section 4 are completed, including the deployment of all generated artifacts and the initial implementation of `sp_ausd_v_ta_c_bfc` (even if it's just a placeholder).
    *   Ensure the `ta_c_bfc` table (or its BigQuery equivalent) exists and is accessible if the core logic interacts with it.
2.  **Run the Help Option:**
    *   Execute the stored procedure with the help flag:
        ```sql
        CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`(p_help => TRUE);
        ```
    *   **Passing Criteria:** The output should display the usage information as defined in the procedure.
3.  **Run a Successful Execution:**
    *   Execute the main stored procedure without any parameters (or with `p_help => FALSE`):
        ```sql
        CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`();
        ```
    *   **Passing Criteria:**
        *   The call should complete without raising an unhandled BigQuery exception.
        *   Query `your_project_id.your_dataset_id.job_audit_log` for the `job_kennung` generated during the run. There should be an entry with `status = 'COMPLETED'` and `message = 'Job execution completed successfully.'`.
        *   The `end_ts` column for this entry should be populated.
        *   If `sp_ausd_v_ta_c_bfc` has a functional implementation, verify that the `ta_c_bfc` table (or its target equivalent) has been updated as expected by the core logic.
4.  **Run an Error Scenario (if `sp_ausd_v_ta_c_bfc` can be made to fail):**
    *   Modify `sp_ausd_v_ta_c_bfc` temporarily to `RAISE BQ EXCEPTION 'Simulated error in core logic';` to test error handling.
    *   Execute the main stored procedure:
        ```sql
        CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`();
        ```
    *   **Passing Criteria:**
        *   The call should raise a BigQuery exception indicating job failure.
        *   Query `your_project_id.your_dataset_id.job_audit_log` for the `job_kennung`. There should be an entry with `status = 'FAILED'`.
        *   Query `your_project_id.your_dataset_id.job_error_log` for the same `job_kennung`. There should be an entry containing the simulated error message and BigQuery error details (`error_message`, `error_stack`).

## 7. Rollback procedure

In case of issues or a need to revert the migration, follow these steps:

1.  **Stop Scheduling:** Immediately disable or remove any scheduled jobs (e.g., Cloud Composer DAGs, Cloud Scheduler jobs) that invoke `your_project_id.your_dataset_id.sp_bindefristcache_update`.
2.  **Drop BigQuery Stored Procedures:**
    *   Drop the main orchestration procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.sp_bindefristcache_update`;
        ```
    *   Drop the core logic placeholder procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.sp_ausd_v_ta_c_bfc`;
        ```
3.  **Drop BigQuery Log Tables (Optional, but recommended for full rollback):**
    *   If the log tables were created solely for this migration and contain no other critical data, drop them:
        ```sql
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_audit_log`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_error_log`;
        ```
    *   **Caution:** If these log tables are shared or contain data from other processes, do NOT drop them. Instead, consider archiving or clearing only the relevant entries.
4.  **Re-enable Original Job:** Re-enable the original `r_ausd_v_ta_c_bfc.ksh` script in its legacy environment, ensuring its `cron` job or scheduler is active.
5.  **Verify Original Job Functionality:** Run the original `r_ausd_v_ta_c_bfc.ksh` script and verify that it executes successfully and updates the `ta_c_bfc` table as expected in the legacy environment.