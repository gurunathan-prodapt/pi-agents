# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh`. This script, originally responsible for orchestrating a contract data reconciliation process, environment setup, parameter parsing, custom logging, and invoking a core business logic script (`k_ausd_v_ta_cntrct_valid.ksh`), has been re-platformed to Google Cloud Platform (GCP).

The target platform leverages:
*   **BigQuery Stored Procedures** for the orchestration logic, parameter handling, and custom logging.
*   **BigQuery Tables** for centralized job logging (`job_log`).
*   **Google Cloud Scheduler** for triggering the BigQuery Stored Procedure on a schedule.
*   **Google Cloud Secret Manager** for secure storage of sensitive configuration.
*   **Google Cloud IAM** for managing permissions.

The core business logic, originally in `k_ausd_v_ta_cntrct_valid.ksh`, has been migrated as a placeholder BigQuery Stored Procedure, awaiting full implementation.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`project.dataset.job_log_table_ddl.sql`**
    *   **Role**: Defines the BigQuery table schema for `job_log`. This table centralizes job execution metadata, status, error messages, and parameters, replacing the legacy custom logging framework (`DWMSG_`). It is partitioned by `start_timestamp` and clustered by `job_name` and `status` for optimized querying.

*   **`project.dataset.log_utils_sp.sql`**
    *   **Role**: Contains BigQuery Stored Procedures that provide logging utilities. These procedures (`generate_job_run_id`, `create_job_log_entry`, `update_job_log_status`) are used by the main wrapper procedure to manage entries in the `job_log` table, mimicking the functionality of the legacy `DWMSG_` functions.

*   **`project.dataset.r_ausd_v_ta_cntrct_valid_wrapper_sp.sql`**
    *   **Role**: This is the main BigQuery Stored Procedure that encapsulates the wrapper logic of the original `r_ausd_v_ta_cntrct_valid.ksh` script. It handles input parameters, initializes and updates job log entries, calls the core business logic procedure (`BERT_K_TA_CNTRCT_VALID`), and manages error handling using BigQuery's `EXCEPTION WHEN ERROR THEN` blocks.

*   **`project.dataset.k_ausd_v_ta_cntrct_valid_placeholder_sp.sql`**
    *   **Role**: A placeholder BigQuery Stored Procedure for the core business logic originally found in `k_ausd_v_ta_cntrct_valid.ksh`. This procedure currently logs a success message but is intended to be fully implemented with the actual contract validation and reconciliation logic using BigQuery SQL.

*   **`scheduler_config.sh`**
    *   **Role**: A shell script to configure and deploy a Google Cloud Scheduler job. This job is responsible for triggering the `project.dataset.BERT_V_TA_CNTRCT_VALID` BigQuery Stored Procedure on a defined schedule, replacing the legacy scheduling mechanism.

*   **`secret_manager_config.sh`**
    *   **Role**: A shell script demonstrating how to create and manage secrets in Google Secret Manager. This is intended for securely storing sensitive environment variables or configuration values that were previously sourced from files like `$HOME/.dw_init`.

*   **`iam_config.sh`**
    *   **Role**: A shell script to configure the necessary IAM roles and create a dedicated service account for the Cloud Scheduler. It grants the service account `BigQuery Job User` and `BigQuery Data Editor` roles, ensuring it has the permissions required to execute BigQuery jobs and stored procedures.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration**: The wrapper logic was re-implemented as a BigQuery Stored Procedure (`BERT_V_TA_CNTRCT_VALID`). This choice leverages BigQuery's native capabilities for SQL-based orchestration, reducing the need for external compute (like Cloud Functions or Cloud Run) for pure SQL workflows. It simplifies deployment and management within the BigQuery ecosystem.
*   **Dedicated BigQuery `job_log` Table for Logging**: The custom `DWMSG_` logging framework was replaced with a structured `job_log` table in BigQuery. This provides a centralized, queryable, and auditable record of job executions, statuses, and errors, aligning with modern data warehousing practices and integrating seamlessly with BigQuery's data processing. Standard output/error from BigQuery jobs is also captured by Google Cloud Logging.
*   **Parameter Handling via Stored Procedure Arguments**: Command-line arguments from the legacy KSH script are now passed as explicit input parameters to the BigQuery Stored Procedure. This provides clear input contracts and type safety, improving maintainability and reducing parsing overhead.
*   **GCP Secret Manager for Sensitive Configuration**: Environment variables from `$HOME/.dw_init` that contain sensitive information are intended to be managed via Secret Manager. This provides a secure, auditable, and centralized way to handle secrets, adhering to GCP security best practices. Non-sensitive configuration can be passed as parameters or stored in BigQuery configuration tables.
*   **BigQuery-Native Error Handling**: The `trap` commands from the KSH script are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides robust, structured error handling within the SQL context, allowing for graceful failure management and detailed error logging.
*   **Decoupling Wrapper from Core Logic**: The core business logic (`k_ausd_v_ta_cntrct_valid.ksh`) was explicitly separated into its own BigQuery Stored Procedure (`BERT_K_TA_CNTRCT_VALID`). This design decision allows for independent development, testing, and deployment of the orchestration layer and the data transformation logic, facilitating a phased migration approach.
*   **Cloud Scheduler for External Triggering**: For simple, time-based scheduling, Cloud Scheduler was chosen to trigger the BigQuery Stored Procedure. This is a cost-effective and fully managed service for cron-like job scheduling. Cloud Composer (Airflow) remains an alternative for more complex orchestration needs involving multiple steps or external system interactions.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure a GCP project is active and billing is enabled.
    *   Create the target BigQuery dataset (e.g., `project.dataset`) if it does not already exist. Replace `project` and `dataset` placeholders in the generated scripts with your actual values.

2.  **Deploy BigQuery DDL and Stored Procedures**:
    *   Execute `project.dataset.job_log_table_ddl.sql` to create the `job_log` table.
    *   Execute `project.dataset.log_utils_sp.sql` to create the logging utility stored procedures.
    *   Execute `project.dataset.k_ausd_v_ta_cntrct_valid_placeholder_sp.sql` to create the placeholder for the core logic.
    *   Execute `project.dataset.r_ausd_v_ta_cntrct_valid_wrapper_sp.sql` to create the main wrapper stored procedure.
    *   **Crucially, the placeholder `project.dataset.BERT_K_TA_CNTRCT_VALID` must be fully implemented with the actual business logic from `k_ausd_v_ta_cntrct_valid.ksh` before production use.**

3.  **Configure IAM and Service Accounts**:
    *   Run `iam_config.sh` after replacing `PROJECT_ID` and `SCHEDULER_SERVICE_ACCOUNT_NAME` with your values. This script will:
        *   Create a dedicated service account for Cloud Scheduler (e.g., `bert-scheduler-sa`).
        *   Grant `roles/bigquery.jobUser` and `roles/bigquery.dataEditor` to this service account on your project. These roles are essential for the service account to execute BigQuery jobs and modify data.

4.  **Manage Secrets (if applicable)**:
    *   Review the contents of the legacy `$HOME/.dw_init` file.
    *   For any sensitive environment variables (e.g., database credentials, API keys), use `secret_manager_config.sh` (after replacing placeholders) to store them in Google Secret Manager.
    *   If the core logic (once implemented) needs to access these secrets, an intermediary (e.g., Cloud Function, Cloud Workflow) might be required to retrieve them and pass them as parameters to the BigQuery Stored Procedure, as BigQuery Stored Procedures cannot directly access Secret Manager.

5.  **Configure Cloud Scheduler Job**:
    *   Run `scheduler_config.sh` after replacing all placeholder variables (`PROJECT_ID`, `REGION`, `BIGQUERY_DATASET`, `SERVICE_ACCOUNT_EMAIL`, `CRON_SCHEDULE`, etc.).
    *   Ensure the `SERVICE_ACCOUNT_EMAIL` matches the one created in step 3.
    *   Adjust `CRON_SCHEDULE` and the parameters (`DYNAMIC_JOB_KENNUNG`, `EINTRAEG_NR`) as per your scheduling requirements.

## 5. Known gaps & unresolved references

The following items were identified during the migration design and remain as known gaps or require further follow-up:

*   **Core Script Logic (`k_ausd_v_ta_cntrct_valid.ksh`)**: The detailed business logic within the original `k_ausd_v_ta_cntrct_valid.ksh` script is currently unknown. Its complexity (e.g., shell commands, external calls, specific data manipulation) will heavily influence the effort required to fully implement the `project.dataset.BERT_K_TA_CNTRCT_VALID` BigQuery Stored Procedure. This is the **primary unresolved item (B4 - Redesign/Separate Assessment)**. If it involves non-SQL operations, a different migration path (e.g., Python/PySpark on Dataproc) might be necessary.
*   **Full `DWMSG_` Framework Scope**: While the wrapper script used specific `DWMSG_` functions for logging, the full extent and complexity of the legacy `DWMSG_` framework are not entirely known. There might be additional functionalities or reporting mechanisms that need to be replicated or replaced beyond basic job logging.
*   **Environment Variables from `.dw_init`**: The specific content and criticality of all variables defined in `$HOME/.dw_init` are unknown. A thorough analysis is required to determine which variables are still needed, if they are sensitive, and how they should be managed in the GCP environment (Secret Manager, BigQuery config tables, or direct parameters).
*   **Parameter Usage (`-s`, `-l`)**: The original KSH script parsed `-s` (source system) and `-l` (log level) parameters but did not directly use them within the wrapper. Their intended use within the core `k_ausd_v_ta_cntrct_valid.ksh` script needs to be understood to ensure they are correctly passed and utilized by the migrated `BERT_K_TA_CNTRCT_VALID` procedure.

## 6. Validation

To validate the successful migration and functionality of the new BigQuery-based orchestration:

1.  **Deployment Verification**:
    *   Confirm that all BigQuery DDL and Stored Procedures (`job_log` table, `log_utils_sp`, `BERT_K_TA_CNTRCT_VALID` placeholder, `BERT_V_TA_CNTRCT_VALID` wrapper) are successfully deployed in the target BigQuery dataset.
    *   Verify the Cloud Scheduler job is created and enabled in the GCP Console.
    *   Confirm the dedicated service account and its IAM roles are correctly configured.

2.  **Manual Execution Test (Success Scenario)**:
    *   Manually trigger the `project.dataset.BERT_V_TA_CNTRCT_VALID` stored procedure from the BigQuery UI or `bq query` command-line tool with valid test parameters (e.g., `CALL project.dataset.BERT_V_TA_CNTRCT_VALID('TEST_JOB', 1, 'BERT_V_TA_CNTRCT_VALID', 'Manual Test');`).
    *   **Passing Criteria**:
        *   The procedure executes without error.
        *   A new entry appears in `project.dataset.job_log` with `status = 'SUCCESS'`, `start_timestamp`, `end_timestamp`, and correct `parameters_json`.
        *   Informational messages from the procedure (including the placeholder's message) appear in Cloud Logging.

3.  **Manual Execution Test (Failure Scenario)**:
    *   Modify the `project.dataset.BERT_K_TA_CNTRCT_VALID` placeholder to intentionally cause an error (e.g., uncomment `SELECT 1 / 0;`).
    *   Manually trigger the `project.dataset.BERT_V_TA_CNTRCT_VALID` stored procedure again.
    *   **Passing Criteria**:
        *   The wrapper procedure should catch the error and terminate with a `RAISE` statement.
        *   A new entry appears in `project.dataset.job_log` with `status = 'FAILED'`, `start_timestamp`, `end_timestamp`, and a detailed `error_message`.
        *   Error messages from the procedure appear in Cloud Logging.

4.  **Scheduled Execution Test**:
    *   Wait for the Cloud Scheduler job to trigger the BigQuery Stored Procedure at its scheduled time, or manually trigger the Cloud Scheduler job.
    *   **Passing Criteria**:
        *   The job executes successfully (or fails as expected if the placeholder is still configured to fail).
        *   Corresponding entries appear in `project.dataset.job_log` with `caller_process = 'Cloud Scheduler'`.
        *   Logs are visible in Cloud Logging.

5.  **Data Validation (Post-Core Logic Implementation)**:
    *   Once the `project.dataset.BERT_K_TA_CNTRCT_VALID` procedure is fully implemented, execute the wrapper and verify that the data transformations and reconciliation logic produce the expected output in the target BigQuery tables. This will involve comparing output data with the legacy system's results.

## 7. Rollback procedure

In case of issues or a need to revert to the legacy system, follow these steps:

1.  **Disable/Delete Cloud Scheduler Job**:
    *   In the GCP Console, navigate to Cloud Scheduler, find the job `bert-v-ta-cntrct-valid-scheduler`, and either disable it or delete it.
    *   Alternatively, use `gcloud scheduler jobs delete bert-v-ta-cntrct-valid-scheduler --location=<your-region>`.

2.  **Delete BigQuery Stored Procedures**:
    *   Execute `DROP PROCEDURE IF EXISTS project.dataset.BERT_V_TA_CNTRCT_VALID;`
    *   Execute `DROP PROCEDURE IF EXISTS project.dataset.BERT_K_TA_CNTRCT_VALID;`
    *   Execute `DROP PROCEDURE IF EXISTS project.dataset.create_job_log_entry;`
    *   Execute `DROP PROCEDURE IF EXISTS project.dataset.update_job_log_status;`
    *   Execute `DROP PROCEDURE IF EXISTS project.dataset.generate_job_run_id;`
    *   The `job_log` table can be retained for historical logging or dropped if no longer needed.

3.  **Revert IAM Changes (Optional)**:
    *   If the dedicated service account or its roles are no longer needed, they can be removed.
    *   `gcloud projects remove-iam-policy-binding <PROJECT_ID> --member="serviceAccount:<SCHEDULER_SERVICE_ACCOUNT_EMAIL>" --role="roles/bigquery.jobUser"`
    *   `gcloud projects remove-iam-policy-binding <PROJECT_ID> --member="serviceAccount:<SCHEDULER_SERVICE_ACCOUNT_EMAIL>" --role="roles/bigquery.dataEditor"`
    *   `gcloud iam service-accounts delete <SCHEDULER_SERVICE_ACCOUNT_EMAIL>`

4.  **Re-enable Legacy Job**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh` script in its legacy scheduling system.

5.  **Data Rollback (if applicable)**:
    *   If the core logic (`BERT_K_TA_CNTRCT_VALID`) was fully implemented and modified data, a data rollback strategy (e.g., restoring from backups, using BigQuery time travel, or running a reverse transformation) might be necessary. This is outside the scope of this wrapper migration but is a critical consideration for any data-modifying job.