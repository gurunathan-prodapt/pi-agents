# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_bp_ta_bpr_instance.ksh` from its legacy environment to Google Cloud Platform (GCP), specifically BigQuery.

The original script served as an orchestration wrapper for the initial provisioning of selected basic products for the BERT system, generating a snapshot of contract caches for "Forderungsscoring". It handled parameter parsing, environment setup, error handling, and delegated core processing to a kernel script (`k_ausd_bp_ta_bpr_instance.ksh`).

The migrated solution transforms this orchestration logic into a BigQuery Stored Procedure, leveraging BigQuery's native capabilities for parameter handling, logging, error management, and calling downstream BigQuery procedures.

## 2. Generated Artifacts

The migration process has generated the following artifacts:

*   **`project.dataset.ausd_bp_ta_bpr_instance` (BigQuery Stored Procedure)**
    *   **Role:** This is the primary migrated component, directly replacing the `r_ausd_bp_ta_bpr_instance.ksh` KornShell script. It handles parameter validation, defaulting, job logging, and orchestrates the call to the `k_ausd_bp_ta_bpr_instance` BigQuery Stored Procedure.
    *   **File:** `r_ausd_bp_ta_bpr_instance.sql`

*   **`project.dataset.sp_write_job_log` (BigQuery Stored Procedure)**
    *   **Role:** A reusable helper procedure designed to standardize and centralize logging operations into the `job_log` table.
    *   **File:** `sp_write_job_log.sql`

*   **`project.dataset.sp_set_job_status` (BigQuery Stored Procedure)**
    *   **Role:** A reusable helper procedure to manage and update the status of jobs in the `job_status` table, supporting both inserts and updates.
    *   **File:** `sp_set_job_status.sql`

*   **`project.dataset.job_log` (BigQuery Table)**
    *   **Role:** Stores detailed log messages, including job name, number, log level, message content, and relevant parameters (`stichtag`, `restart_value`), along with a timestamp. This replaces the custom file-based logging of the original script.

*   **`project.dataset.job_metadata` (BigQuery Table)**
    *   **Role:** Stores metadata specific to each job run, such as the generated log file name (for conceptual consistency), system date, cutoff date, and restart value. This helps in auditing and tracking job executions.

*   **`project.dataset.job_status` (BigQuery Table)**
    *   **Role:** Maintains the current status (e.g., 'RUNNING', 'OK', 'ERROR') for each job instance, providing a quick overview of job health.

## 3. Key Design Decisions

1.  **Orchestration Script to BigQuery Stored Procedure:**
    *   **Why:** The original script's primary function was orchestration, parameter handling, and error management, with the core business logic delegated to a kernel script. BigQuery Stored Procedures are well-suited for this, allowing native SQL-based control flow, parameter passing, and integration with other BigQuery components. This approach leverages BigQuery's scalability and managed service benefits.
    *   **Trade-offs:** Loss of direct access to the underlying operating system for shell-specific commands (e.g., `trap`, `getopts` directly). These functionalities are replaced by BigQuery SQL equivalents (e.g., `EXCEPTION WHEN ERROR`, `IFNULL`, `REGEXP_CONTAINS`).

2.  **Centralized Logging and Status Management:**
    *   **Why:** The original script used a custom `DWMSG` framework and file-based logging. Migrating to dedicated BigQuery tables (`job_log`, `job_metadata`, `job_status`) provides a structured, queryable, and centralized logging solution. This improves observability, auditing, and simplifies error diagnosis within the GCP ecosystem.
    *   **Trade-offs:** Requires defining and maintaining BigQuery table schemas. The custom `DWMSG` error numbering (`ErrNr`) is mapped to BigQuery's `RAISE USING MESSAGE` or custom error messages.

3.  **Parameter Handling and Defaulting:**
    *   **Why:** The `getopts` and conditional logic in KornShell for `Stichtag` and `Wiederanlaufwert` are directly translated into `IN` parameters for the BigQuery Stored Procedure, using `IFNULL` and `CURRENT_DATE()` for defaulting. This maintains functional parity while adhering to BigQuery SQL syntax.
    *   **Trade-offs:** The date format validation (`DDMMYYYY`) is now handled via `REGEXP_CONTAINS` instead of external helper scripts.

4.  **Delegation to Kernel Stored Procedure:**
    *   **Why:** The original script explicitly called `k_ausd_bp_ta_bpr_instance.ksh`. The migrated design maintains this clear separation of concerns by having the `ausd_bp_ta_bpr_instance` BigQuery Stored Procedure `CALL` its corresponding `k_ausd_bp_ta_bpr_instance` BigQuery Stored Procedure. This ensures modularity and simplifies the migration of the kernel script as a separate unit.
    *   **Trade-offs:** The full end-to-end functionality is dependent on the successful migration and deployment of the kernel script.

5.  **Helper Script Integration:**
    *   **Why:** KornShell helper scripts (e.g., for error handling, parameter parsing, date handling) are replaced by native BigQuery SQL functions and constructs. This reduces external dependencies and consolidates logic within BigQuery, improving performance and maintainability.
    *   **Trade-offs:** Requires careful translation of helper script logic into BigQuery SQL, ensuring functional equivalence.

## 4. Manual Steps Before Go-Live

The following steps must be performed manually before the migrated job can be put into production:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `project.dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `project.dataset`;
        ```

2.  **BigQuery Table Creation:**
    *   Deploy the `job_log`, `job_metadata`, and `job_status` tables in the target dataset.
        ```sql
        CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
          job_name STRING,
          job_nr INT64,
          log_level STRING,
          message STRING,
          stichtag STRING,
          restart_value INT64,
          created_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS `project.dataset.job_metadata` (
          job_name STRING,
          job_nr INT64,
          log_file_name STRING,
          sysdate_ddmmyyyy STRING,
          stichtag_ddmmyyyy STRING,
          restart_value INT64,
          created_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS `project.dataset.job_status` (
          job_name STRING,
          job_nr INT64,
          status STRING,
          updated_at TIMESTAMP
        );
        ```

3.  **BigQuery Stored Procedure Deployment:**
    *   Deploy the generated BigQuery Stored Procedures (`r_ausd_bp_ta_bpr_instance.sql`, `sp_write_job_log.sql`, `sp_set_job_status.sql`) to the target dataset. This involves executing the `CREATE OR REPLACE PROCEDURE` statements.

4.  **IAM / Permissions:**
    *   Configure appropriate IAM roles and permissions for the service account that will execute these BigQuery procedures. This service account will need:
        *   `BigQuery Data Editor` on the `project.dataset` to create/update/insert into the `job_log`, `job_metadata`, and `job_status` tables.
        *   `BigQuery Job User` to run BigQuery jobs and stored procedures.
        *   Permissions to call the `k_ausd_bp_ta_bpr_instance` procedure (once it's migrated and deployed).

5.  **Connection Strings / Secrets:**
    *   No direct connection strings or secrets are managed by this specific orchestration procedure, as it operates entirely within BigQuery. Any downstream dependencies of `k_ausd_bp_ta_bpr_instance` will need their own secret management strategy (e.g., Secret Manager).

6.  **Scheduling:**
    *   Integrate the execution of the `CALL project.dataset.ausd_bp_ta_bpr_instance(...)` statement into your chosen GCP scheduler.
        *   **Cloud Composer (Airflow):** Create a DAG that uses the `BigQueryOperator` to execute the stored procedure.
        *   **Cloud Workflows:** Define a workflow that includes a BigQuery step to call the procedure.
        *   **Cloud Scheduler + Cloud Functions/Run:** A Cloud Scheduler job could trigger a Cloud Function or Cloud Run service, which then executes the BigQuery stored procedure.

7.  **Kernel Script Migration:**
    *   **Crucially, the `k_ausd_bp_ta_bpr_instance.ksh` script must be migrated to a corresponding BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_instance`) and deployed before this orchestrator can function end-to-end.**

## 5. Known Gaps & Unresolved References

1.  **Downstream Logic in `k_ausd_bp_ta_bpr_instance.ksh`:** The migration of the main `r_ausd_bp_ta_bpr_instance.ksh` script is straightforward as it's an orchestrator. The critical unknown is the content and dependencies of `k_ausd_bp_ta_bpr_instance.ksh`. This kernel script likely contains the actual data manipulation (SQL, potentially other shell commands) and needs its own detailed analysis and migration plan. Without this, the overall job migration is incomplete.
2.  **`BERT_DIR_ROOT` variable:** This environment variable's resolution is critical for locating dependent scripts in the original environment. Its value needs to be configured in the target GCP environment (e.g., as a BigQuery constant, a parameter in Cloud Composer, or an environment variable for Cloud Run/Functions) if any non-BigQuery components still rely on it. For the current BigQuery-only migration, this variable is no longer directly relevant.
3.  **Date Logic Discrepancy:** The script's comment `AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum` suggests an original intent to derive `Stichtag` from `MIN(sysdate, maxladedatum)` from a table. The current implementation defaults `Stichtag` to `v_sysdate`. This potential discrepancy should be clarified with business users to ensure the correct date logic is implemented in BigQuery. The current migration defaults to `CURRENT_DATE()` if `p_stichtag` is not provided.
4.  **`Wiederanlaufwert` Semantics:** The "restart threshold" implies a specific interaction with target data (delete records >= threshold). The exact BigQuery implementation will depend on the business requirements for idempotency and restartability. The current orchestrator passes this value to the kernel script, but its interpretation and action are deferred to `k_ausd_bp_ta_bpr_instance`.
5.  **`semi_auto` Automation Bucket:** This indicates that while automated translation is possible, some manual intervention or review will be required, likely due to the shell scripting patterns and the need to define the BigQuery environment setup. This has been addressed by the manual steps outlined above.

## 6. Validation

To validate the migrated `ausd_bp_ta_bpr_instance` BigQuery Stored Procedure, perform the following steps:

1.  **Prerequisites:**
    *   Ensure all BigQuery tables (`job_log`, `job_metadata`, `job_status`) and the helper procedures (`sp_write_job_log`, `sp_set_job_status`) are deployed.
    *   **Crucially, a stub or fully migrated `project.dataset.k_ausd_bp_ta_bpr_instance` procedure must exist.** For initial testing of the orchestrator, a simple stub that logs its parameters and returns successfully is sufficient.

2.  **Test Cases:**

    *   **Case 1: Successful Execution (with defaults)**
        *   **Action:** Call the procedure without parameters:
            ```sql
            CALL `project.dataset.ausd_bp_ta_bpr_instance`(NULL, NULL);
            ```
        *   **Expected "Passing" Criteria:**
            *   The procedure completes successfully without raising an error.
            *   `job_log` table contains entries for job start, successful completion, and the call to `k_ausd_bp_ta_bpr_instance`.
            *   `job_metadata` table contains an entry with `stichtag_ddmmyyyy` set to `CURRENT_DATE()` (DDMMYYYY format) and `restart_value` as `0`.
            *   `job_status` table shows the job as 'OK'.
            *   The stub `k_ausd_bp_ta_bpr_instance` procedure should have been called with `job_name`, `CURRENT_DATE()`, a valid `job_nr`, and `0` for `restart_value`.

    *   **Case 2: Successful Execution (with explicit parameters)**
        *   **Action:** Call the procedure with specific parameters:
            ```sql
            CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 100);
            ```
        *   **Expected "Passing" Criteria:**
            *   The procedure completes successfully.
            *   `job_log` and `job_metadata` entries reflect `stichtag` as '01012023' and `restart_value` as `100`.
            *   `job_status` shows 'OK'.
            *   The stub `k_ausd_bp_ta_bpr_instance` procedure should have been called with '01012023' and `100`.

    *   **Case 3: Invalid `Stichtag` Format**
        *   **Action:** Call the procedure with an incorrectly formatted `Stichtag`:
            ```sql
            CALL `project.dataset.ausd_bp_ta_bpr_instance`('2023-01-01', NULL);
            ```
        *   **Expected "Passing" Criteria:**
            *   The procedure raises an error with a message similar to "AppError: Abbruch. Error Message: Stichtag must be in DDMMYYYY format".
            *   `job_log` table contains an 'E' (Error) level entry indicating parameter validation failure.
            *   `job_status` table shows the job as 'ERROR'.

    *   **Case 4: Downstream Kernel Procedure Failure**
        *   **Action:** Modify the stub `k_ausd_bp_ta_bpr_instance` to `RAISE USING MESSAGE = 'Simulated kernel error';` and then call the orchestrator:
            ```sql
            CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', NULL);
            ```
        *   **Expected "Passing" Criteria:**
            *   The `ausd_bp_ta_bpr_instance` procedure catches the error from the kernel and raises its own error message.
            *   `job_log` table contains an 'E' (Error) level entry indicating the failure of the kernel procedure.
            *   `job_status` table shows the job as 'ERROR'.

## 7. Rollback Procedure

In case of critical issues or if the migrated job does not perform as expected, the following rollback procedure can be executed to revert to the original state:

1.  **Stop New Executions:**
    *   Immediately disable or remove the scheduling mechanism (e.g., Cloud Composer DAG, Cloud Workflow, Cloud Scheduler job) that triggers the `project.dataset.ausd_bp_ta_bpr_instance` BigQuery Stored Procedure.

2.  **Re-enable Original Job:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh` KornShell script in its legacy environment. Ensure its original scheduler is reactivated.

3.  **Remove Migrated BigQuery Components (Optional but Recommended for Clean-up):**
    *   **Delete BigQuery Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_bpr_instance`;
        DROP PROCEDURE IF EXISTS `project.dataset.sp_write_job_log`;
        DROP PROCEDURE IF EXISTS `project.dataset.sp_set_job_status`;
        -- Also drop the k_ausd_bp_ta_bpr_instance procedure if it was deployed
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_bp_ta_bpr_instance`;
        ```
    *   **Delete BigQuery Tables:**
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        DROP TABLE IF EXISTS `project.dataset.job_metadata`;
        DROP TABLE IF EXISTS `project.dataset.job_status`;
        ```
    *   **Delete BigQuery Dataset (if created solely for this migration):**
        ```sql
        DROP SCHEMA IF EXISTS `project.dataset`;
        ```
    *   **Remove IAM Bindings:** Revoke any specific IAM permissions granted to service accounts for this migrated job.

4.  **Verify Original Job Functionality:**
    *   Confirm that the original KornShell script is running as expected and processing data correctly in the legacy environment.

This rollback procedure ensures a quick return to the previous stable state, minimizing disruption.