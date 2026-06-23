```markdown
# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh`. The script, primarily responsible for job orchestration, parameter handling, and custom logging for a contract data reconciliation process, has been migrated to Google BigQuery. The target platform for this migration is a suite of BigQuery Stored Procedures and tables, encapsulating the original script's functionalities within BigQuery's native environment.

## 2. Generated artifacts

The migration process has generated the following BigQuery artifacts:

*   **`ddl/dw_job_entries.sql`**
    *   **Role:** Defines the DDL for the `dw_job_entries` BigQuery table. This table serves as a central repository for logging job execution details, including start/end times, status, and messages, replacing the file-based logging of the original KornShell script.
*   **`ddl/dw_error_log.sql`**
    *   **Role:** Defines the DDL for the `dw_error_log` BigQuery table. This table captures detailed error information, such as error codes, messages, source components, and stack traces, replacing the custom error logging mechanisms (`DWMSG_MeldeFehler`) in the original script.
*   **`ddl/dw_job_status.sql`**
    *   **Role:** Defines the DDL for the `dw_job_status` BigQuery table. This table maintains the current status of ongoing or recently completed jobs, providing a quick overview of job health, replacing the status tracking logic in the original script.
*   **`sprocs/sp_dwmsg_erzeuge_eintrag.sql`**
    *   **Role:** BigQuery Stored Procedure that creates a new entry in `dw_job_entries` and updates/inserts a record into `dw_job_status`. It centralizes the logic for initiating job tracking, replacing the `DWMSG_erzeuge_Eintrag` function.
*   **`sprocs/sp_dwmsg_setze_status_ok.sql`**
    *   **Role:** BigQuery Stored Procedure that updates the `dw_job_entries` and `dw_job_status` tables to mark a job as successfully completed. It replaces the `DWMSG_setze_Status_OK` function.
*   **`sprocs/sp_dwmsg_meldefehler.sql`**
    *   **Role:** BigQuery Stored Procedure responsible for logging detailed error information into `dw_error_log` and updating the status of the corresponding job in `dw_job_entries` and `dw_job_status` to 'FAILED'. It replaces the `DWMSG_MeldeFehler` function.
*   **`sprocs/sp_dwmsg_fehlerbehandlung.sql`**
    *   **Role:** BigQuery Stored Procedure that orchestrates error handling. It calls `sp_dwmsg_meldefehler` to log the error and then raises a BigQuery error, effectively stopping execution and propagating the failure. It replaces the `DWMSG_FehlerBehandlung` function.
*   **`sprocs/sp_k_ausd_v_ta_inv_acc.sql`**
    *   **Role:** This is a **placeholder** BigQuery Stored Procedure. It is intended to host the migrated core reconciliation logic originally found in `k_ausd_v_ta_inv_acc.ksh`. For now, it contains a `SELECT` statement to indicate its invocation. Its full implementation is a separate migration effort.
*   **`sprocs/sp_vertragsdatenabgleich.sql`**
    *   **Role:** The main BigQuery Stored Procedure that replaces the `r_ausd_v_ta_inv_acc.ksh` script. It handles parameter validation, orchestrates the job flow, calls the core reconciliation logic (`sp_k_ausd_v_ta_inv_acc`), and integrates with the new BigQuery-based logging and error handling procedures.

## 3. Key design decisions

The following key design decisions guided the migration of the KornShell script to BigQuery:

*   **Migration to BigQuery Stored Procedures for Orchestration:** The entire orchestration logic, including parameter parsing, validation, and job flow control, has been re-implemented as a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This leverages BigQuery's native capabilities for procedural logic, ensuring execution within the data warehouse environment.
*   **Centralized Logging and Status Management:** The custom `DWMSG_*` functions and file-based logging of the original script have been replaced by dedicated BigQuery tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`) and helper stored procedures (`sp_dwmsg_erzeuge_eintrag`, `sp_dwmsg_setze_status_ok`, `sp_dwmsg_meldefehler`, `sp_dwmsg_fehlerbehandlung`). This provides structured, queryable, and scalable logging within BigQuery.
*   **Parameter Handling via Stored Procedure Arguments:** Command-line argument parsing (`getopts`) from the KornShell script is replaced by explicit, strongly typed parameters in the BigQuery Stored Procedure. This improves clarity, type safety, and simplifies invocation.
*   **BigQuery `EXCEPTION` Blocks for Error Handling:** The `trap INT/ERR` mechanism of KornShell is replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides robust, structured error handling that integrates seamlessly with the BigQuery logging tables.
*   **Placeholder for Core Logic:** The invocation of the core reconciliation script (`k_ausd_v_ta_inv_acc.ksh`) is replaced by a `CALL` to a placeholder BigQuery Stored Procedure (`sp_k_ausd_v_ta_inv_acc`). This decouples the orchestration migration from the potentially more complex core business logic migration, allowing for an iterative approach.
*   **Configuration Management:** Environment variables and sourced configuration files (e.g., `$HOME/.dw_init`) are intended to be replaced by stored procedure parameters or, if more complex, by dedicated BigQuery configuration tables. For this wrapper, key variables like `JobKennung` are handled as parameters.

**Notable Trade-offs:**

*   **Fidelity of Shell Signal Trapping:** While BigQuery's `EXCEPTION` blocks provide robust error handling, the exact behavior of `trap INT/ERR` in KornShell, particularly concerning external process interruption signals, might not be perfectly replicated. This is generally acceptable as BigQuery jobs are typically managed by orchestrators that handle retries and failures.
*   **Dependency on Core Script Migration:** The `sp_k_ausd_v_ta_inv_acc` is currently a placeholder. The full functionality of the migrated wrapper depends on the successful and accurate migration of the core reconciliation logic, which is a separate, potentially complex, effort.

## 4. Manual steps before go-live

Before the migrated BigQuery stored procedures can be used in a production environment, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `your_gcp_project_id.your_dataset_name`;
        ```
    *   **Action:** Replace `your_gcp_project_id` and `your_dataset_name` with actual values in all generated SQL files before deployment.
2.  **IAM Permissions:**
    *   The service account or user executing these BigQuery stored procedures must have the necessary IAM roles:
        *   `BigQuery Data Editor` (or equivalent) on `your_gcp_project_id.your_dataset_name` to create and modify tables and stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
        *   Permissions to read/write to any other BigQuery tables that `sp_k_ausd_v_ta_inv_acc` (once implemented) might interact with.
    *   **Action:** Verify and grant appropriate IAM roles.
3.  **Configuration Review and Extraction:**
    *   Analyze the contents of `$HOME/.dw_init` and any other utility scripts (e.g., `h_alis_parameter.ksh`, `h_alis_date.ksh`) that were sourced by the original KornShell script.
    *   Identify any critical environment variables, configuration parameters, or static values that are not explicitly passed as parameters to `sp_vertragsdatenabgleich`.
    *   **Action:** Decide how these configurations will be managed in BigQuery (e.g., hardcoded defaults in the SP, passed as additional parameters, or stored in a BigQuery configuration table). Update the stored procedures accordingly.
4.  **Secrets Management (if applicable):**
    *   If any configuration extracted from the original environment contains sensitive information (e.g., API keys, database credentials for external systems), these should be managed securely.
    *   **Action:** Consider using Google Secret Manager and integrating it with Cloud Functions or other services that can securely pass these secrets to BigQuery jobs if needed (though direct secret access from BigQuery SPs is limited).
5.  **Scheduling and Orchestration:**
    *   The original KornShell script was likely scheduled via a cron job or an existing orchestrator. The BigQuery stored procedure needs a new scheduling mechanism.
    *   **Action:** Configure a scheduler (e.g., Cloud Composer/Apache Airflow, Cloud Scheduler, Dataform) to invoke `CALL `your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich`('DDMMYYYY', 'MODE');` with the required parameters.
6.  **Core Logic Implementation:**
    *   The `sprocs/sp_k_ausd_v_ta_inv_acc.sql` is a placeholder.
    *   **Action:** The actual business logic from `k_ausd_v_ta_inv_acc.ksh` must be migrated and implemented within this stored procedure (or a series of SQL queries orchestrated by Dataform/Airflow, which `sp_k_ausd_v_ta_inv_acc` would then call). This is a critical prerequisite for the full functionality of `sp_vertragsdatenabgleich`.

## 5. Known gaps & unresolved references

*   **Core Script Migration (`k_ausd_v_ta_inv_acc.ksh`):** The most significant gap is the full implementation of `project.dataset.sp_k_ausd_v_ta_inv_acc`. This procedure currently serves as a placeholder. Its migration is a separate, complex task that needs to be completed before the end-to-end reconciliation process can function.
*   **Full `DWMSG_*` Functionality:** While the core logging and error reporting functions (`DWMSG_erzeuge_Eintrag`, `DWMSG_MeldeFehler`, `DWMSG_FehlerBehandlung`, `DWMSG_setze_Status_OK`) have been migrated, other utility functions from `f_alis_msgerr.ksh` (e.g., `DWMSG_ermittle_Nr`, `DWMSG_LogDateiname`, `DWMSG_setze_Stichtag_Info`) have not been explicitly migrated as separate BigQuery procedures. Their functionality is either absorbed into the main `sp_vertragsdatenabgleich` or deemed unnecessary in the BigQuery context. A review of their original usage might be needed to ensure no critical functionality is missed.
*   **Environmental Context (`$HOME/.dw_init`):** The precise contents and implications of `$HOME/.dw_init` and other sourced utility scripts are not fully known without deeper analysis. While common variables are assumed to be handled as parameters, there might be subtle configurations or dependencies that need to be explicitly identified and migrated.
*   **Signal Handling Fidelity:** The `trap INT/ERR` mechanism in KornShell has specific behaviors related to process signals. BigQuery's `EXCEPTION` handling is robust for SQL errors but might not perfectly replicate all nuances of OS-level signal trapping. This is generally acceptable for data warehouse jobs.
*   **`project.dataset` Placeholder:** All generated SQL files use `project.dataset` as a placeholder. This must be replaced with the actual GCP project ID and BigQuery dataset name before deployment.

## 6. Validation

To validate the migrated BigQuery stored procedures, follow these steps:

1.  **Deployment:** Deploy all DDL and stored procedure SQL files to your target BigQuery project and dataset.
2.  **Test Invocation:** Execute the main orchestration stored procedure `sp_vertragsdatenabgleich` with various parameter combinations.

    *   **Successful Run (Example):**
        ```sql
        CALL `your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich`('26102023', 'PROD');
        ```
    *   **Test Mode Run (Example):**
        ```sql
        CALL `your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich`('27102023', 'TEST');
        ```
    *   **Invalid Date Format (Error Scenario):**
        ```sql
        CALL `your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich`('2023-10-26', 'PROD');
        ```
    *   **Missing Mode (Error Scenario):**
        ```sql
        CALL `your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich`('26102023', NULL);
        ```
    *   **Invalid Mode (Error Scenario):**
        ```sql
        CALL `your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich`('26102023', 'DEV');
        ```
    *   **Simulate Core Logic Failure:** (Requires modifying `sp_k_ausd_v_ta_inv_acc` temporarily to `RAISE` an error).

3.  **Verification of "Passing" Criteria:**

    *   **Successful Execution:**
        *   The `sp_vertragsdatenabgleich` procedure completes without raising an unhandled error.
        *   Query `your_gcp_project_id.your_dataset_name.dw_job_entries` for the `job_id` and `run_id` of the execution. The `status` should be 'SUCCESS', and `end_timestamp` should be populated.
        *   Query `your_gcp_project_id.your_dataset_name.dw_job_status`. The entry for the `job_id`/`run_id` should show `status = 'SUCCESS'`.
        *   No new entries should appear in `your_gcp_project_id.your_dataset_name.dw_error_log` for this run.
        *   The placeholder `sp_k_ausd_v_ta_inv_acc` should have been called (its internal `SELECT` statement would be visible in job logs if executed interactively).

    *   **Error Handling Verification:**
        *   When an invalid parameter is passed or an error is simulated in `sp_k_ausd_v_ta_inv_acc`, the `sp_vertragsdatenabgleich` procedure should terminate with a raised error.
        *   Query `your_gcp_project_id.your_dataset_name.dw_job_entries`. The `status` for the corresponding `job_id`/`run_id` should be 'FAILED', and `end_timestamp` should be populated.
        *   Query `your_gcp_project_id.your_dataset_name.dw_job_status`. The entry for the `job_id`/`run_id` should show `status = 'FAILED'`.
        *   A new entry should appear in `your_gcp_project_id.your_dataset_name.dw_error_log` for the failed run, containing the error message, code, and stack trace.
        *   The error message returned by the `RAISE` statement in `sp_dwmsg_fehlerbehandlung` should be descriptive and match the expected error.

## 7. Rollback procedure

In case of issues or if the migration needs to be reverted, follow these steps to roll back to the original KornShell script:

1.  **Stop New Invocations:**
    *   Immediately disable or remove any new scheduling configurations (e.g., Cloud Composer DAGs, Cloud Scheduler jobs) that invoke the BigQuery stored procedure `sp_vertragsdatenabgleich`.
2.  **Reactivate Original Scheduling:**
    *   Re-enable the original scheduling mechanism (e.g., cron job) that was responsible for executing `r_ausd_v_ta_inv_acc.ksh`.
3.  **Delete BigQuery Artifacts (Optional but Recommended for Clean-up):**
    *   **Delete Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich`;
        DROP PROCEDURE IF EXISTS `your_gcp_project_id.your_dataset_name.sp_k_ausd_v_ta_inv_acc`;
        DROP PROCEDURE IF EXISTS `your_gcp_project_id.your_dataset_name.sp_dwmsg_fehlerbehandlung`;
        DROP PROCEDURE IF EXISTS `your_gcp_project_id.your_dataset_name.sp_dwmsg_meldefehler`;
        DROP PROCEDURE IF EXISTS `your_gcp_project_id.your_dataset_name.sp_dwmsg_setze_status_ok`;
        DROP PROCEDURE IF EXISTS `your_gcp_project_id.your_dataset_name.sp_dwmsg_erzeuge_eintrag`;
        ```
    *   **Delete Tables:**
        ```sql
        DROP TABLE IF EXISTS `your_gcp_project_id.your_dataset_name.dw_job_entries`;
        DROP TABLE IF EXISTS `your_gcp_project_id.your_dataset_name.dw_error_log`;
        DROP TABLE IF EXISTS `your_gcp_project_id.your_dataset_name.dw_job_status`;
        ```
    *   **Note:** Ensure you replace `your_gcp_project_id.your_dataset_name` with the actual project and dataset names used during deployment.
4.  **Verify Original Script Functionality:**
    *   Monitor the execution of the original `r_ausd_v_ta_inv_acc.ksh` script to ensure it is running as expected and producing correct outputs and logs.

This rollback procedure ensures a quick return to the previous operational state while providing steps for cleaning up the partially migrated BigQuery components.