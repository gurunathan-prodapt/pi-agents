# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh` has been migrated. This script, originally responsible for orchestrating the initial provisioning of selected basic products for BERT by handling command-line parameters, date determination, error handling, logging, and invoking a "kernel" script, has been re-platformed to Google Cloud Platform (GCP).

The core orchestration logic, parameter handling, and logging have been translated into a BigQuery Stored Procedure, leveraging BigQuery's native SQL capabilities. The overall job scheduling and execution will be managed by Cloud Composer (Airflow), aligning with GCP's recommended practices for data pipeline orchestration. The invoked kernel script (`k_ausd_bp_ta_bcp_msisdn.ksh`) is expected to be migrated into a separate BigQuery Stored Procedure, which this wrapper SP will call.

## 2. Generated artifacts

The migration process has generated the following BigQuery DDL and Stored Procedure files:

*   **`ddl/job_control.sql`**
    *   **Role**: Defines a BigQuery table named `job_control`. This table serves as a centralized repository for tracking the overall status, start/end times, and key parameters of each execution instance of the migrated job. It replaces the file-based job status tracking of the legacy system.
*   **`ddl/job_log.sql`**
    *   **Role**: Defines a BigQuery table named `job_log`. This table stores detailed informational, warning, and error messages generated during the job's execution. It replaces the file-based log output, providing structured and queryable logs.
*   **`ddl/job_error_log.sql`**
    *   **Role**: Defines a BigQuery table named `job_error_log`. This table is specifically designed to capture detailed error information, including error type, message, and stack trace, when a job encounters a failure. It enhances error visibility and debugging capabilities compared to unstructured log files.
*   **`stored_procedures/sp_r_ausd_bp_ta_bcp_msisdn.sql`**
    *   **Role**: Contains the BigQuery Stored Procedure `project.dataset.sp_r_ausd_bp_ta_bcp_msisdn`. This is the primary migrated component, replacing the original KornShell wrapper script. It handles:
        *   Receiving input parameters (`p_stichtag`, `p_wiederanlaufWert`).
        *   Defaulting logic for parameters (e.g., `p_stichtag` to current date, `p_wiederanlaufWert` to 0).
        *   Date determination using BigQuery's native date functions.
        *   Parameter validation (e.g., `stichtag` format).
        *   Logging job progress and status to the `job_control` and `job_log` tables.
        *   Invoking the (future) kernel stored procedure `project.dataset.sp_ausd_bp_ta_bcp_msisdn_kernel`.
        *   Robust error handling using `EXCEPTION WHEN ERROR` blocks and logging errors to `job_error_log`.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures**: The entire orchestration logic, including parameter parsing, defaulting, validation, and error handling, has been re-implemented as a BigQuery Stored Procedure. This leverages BigQuery's scalability, performance, and native SQL capabilities, moving away from shell scripting for core logic.
*   **Structured Logging in BigQuery Tables**: File-based logging and status tracking (`job_control`, `job_log`, `job_error_log`) have been replaced by dedicated BigQuery tables. This provides a centralized, structured, and queryable logging mechanism, significantly improving observability and debugging.
*   **Cloud Composer (Airflow) for Orchestration**: The `Cloud Composer (Airflow)` hint from the source analysis was adopted. Airflow will be used to schedule, trigger, and monitor the BigQuery Stored Procedure, providing robust workflow management, retries, and alerting.
*   **Direct Parameter Passing**: Command-line arguments (`getopts`) from the original script are now direct `IN` parameters to the BigQuery Stored Procedure, simplifying parameter handling and type safety.
*   **Native BigQuery Date Functions**: Legacy date calculations and utilities have been replaced with BigQuery's built-in `CURRENT_DATE()`, `FORMAT_DATE()`, and other date/time functions, ensuring consistency and efficiency.
*   **Robust Error Handling**: The `trap` commands and custom error functions of the KornShell script are replaced by BigQuery's `EXCEPTION WHEN ERROR` blocks for runtime errors and `SIGNAL SQLSTATE` for explicit error signaling (e.g., parameter validation failures). This integrates error handling directly into the SQL logic.
*   **Modularization of Kernel Logic**: The core business logic, originally in `k_ausd_bp_ta_bcp_msisdn.ksh`, is explicitly designed to be migrated into a separate BigQuery Stored Procedure (`sp_ausd_bp_ta_bcp_msisdn_kernel`). This maintains a clear separation of concerns between orchestration and data transformation, allowing for independent development and testing of the kernel.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset` as referenced in the DDL and SP) exists. If not, create it.
2.  **Deploy DDLs**: Execute the `ddl/job_control.sql`, `ddl/job_log.sql`, and `ddl/job_error_log.sql` scripts in BigQuery to create the necessary logging and control tables.
3.  **Deploy Stored Procedure**: Deploy the `stored_procedures/sp_r_ausd_bp_ta_bcp_msisdn.sql` to the target BigQuery dataset.
4.  **Migrate and Deploy Kernel Stored Procedure**: **Crucially**, the `k_ausd_bp_ta_bcp_msisdn.ksh` script must be fully migrated into its own BigQuery Stored Procedure, `project.dataset.sp_ausd_bp_ta_bcp_msisdn_kernel`, and deployed. The wrapper SP will fail if this kernel SP does not exist or is not functional.
5.  **IAM Permissions Configuration**:
    *   The service account used by Cloud Composer/Airflow to trigger the BigQuery Stored Procedure must have `BigQuery Job User` and `BigQuery Data Editor` roles on the target dataset (`project.dataset`) to execute the SP and write to the logging tables.
    *   The BigQuery service account executing the stored procedure (implicitly, the one associated with the Airflow job) must have `BigQuery Data Editor` permissions on the `job_control`, `job_log`, and `job_error_log` tables.
    *   It also needs appropriate `BigQuery Data Viewer` and `BigQuery Data Editor` permissions on any source and target tables that the `sp_ausd_bp_ta_bcp_msisdn_kernel` will interact with.
6.  **Cloud Composer (Airflow) DAG Creation**: Develop and deploy an Airflow DAG that:
    *   Schedules the job according to the required frequency.
    *   Uses the `BigQueryOperator` or `BigQueryExecuteQueryOperator` to call `project.dataset.sp_r_ausd_bp_ta_bcp_msisdn`.
    *   Passes the `p_stichtag` and `p_wiederanlaufWert` parameters to the SP, potentially deriving them from Airflow variables or execution context.
    *   Configures appropriate retries and alerts.
7.  **Environment Variable Translation**: Identify and translate any critical environment variables or configurations set by the legacy `$HOME/.dw_init` script into Airflow Variables, GCP Secret Manager, or other appropriate GCP configuration mechanisms.

## 5. Known gaps & unresolved references

*   **Kernel Script (`k_ausd_bp_ta_bcp_msisdn.ksh`) Migration**: The most significant gap is that the actual business logic contained within `k_ausd_bp_ta_bcp_msisdn.ksh` has not yet been migrated. The `sp_r_ausd_bp_ta_bcp_msisdn` procedure includes a placeholder `CALL` to `sp_ausd_bp_ta_bcp_msisdn_kernel`, which must be developed and deployed separately. Its complexity, data sources, and targets are critical for the complete solution.
*   **`$HOME/.dw_init` Contents**: The exact environment variables and configurations set by `$HOME/.dw_init` need to be fully identified and translated into GCP-native configuration (e.g., Airflow variables, Cloud Secret Manager, BigQuery connection properties).
*   **`DWDate_Gib_Zeitraum` Logic**: While basic date determination is handled, the full logic of `DWDate_Gib_Zeitraum`, especially if it involves `maxladedatum` from a database table, needs to be confirmed and implemented in BigQuery SQL if not already covered. The current implementation assumes `v_sysdate` as the default.
*   **Missing Complexity/Automation Data**: The original analysis lacked complexity and automation rate data for the source script. This means the actual effort and potential for further automation were not fully assessed, which could impact future planning.
*   **Shell-specific `trap` commands**: While BigQuery's `EXCEPTION WHEN ERROR` and Airflow's error handling cover most scenarios, any highly specific `trap` logic that goes beyond simple error catching might require careful consideration for an equivalent GCP implementation.

## 6. Validation

To ensure the successful migration and functionality of the `sp_r_ausd_bp_ta_bcp_msisdn` job, the following validation steps should be performed:

1.  **Unit Testing of `sp_r_ausd_bp_ta_bcp_msisdn`**:
    *   **How to run**: Execute the stored procedure directly in BigQuery using `CALL project.dataset.sp_r_ausd_bp_ta_bcp_msisdn(p_stichtag => '...', p_wiederanlaufWert => ...);`
    *   **Test Cases**:
        *   **Valid parameters**: Provide a valid `stichtag` (e.g., '01012023') and `wiederanlaufWert` (e.g., 1).
        *   **Missing `stichtag`**: Call with `p_stichtag => NULL` or `p_stichtag => ''`. Verify it defaults to the current system date.
        *   **Missing `wiederanlaufWert`**: Call with `p_wiederanlaufWert => NULL`. Verify it defaults to 0.
        *   **Invalid `stichtag` format**: Provide a `stichtag` that does not match `DDMMYYYY` (e.g., '2023-01-01', 'ABC').
        *   **Kernel SP failure**: (Once `sp_ausd_bp_ta_bcp_msisdn_kernel` is implemented) Simulate a failure within the kernel SP to ensure the wrapper SP correctly catches and logs the error.
    *   **What "passing" means**:
        *   For successful runs: The procedure completes without error. The `job_control` table shows `status = 'SUCCESS'`, and `job_log` contains expected informational messages.
        *   For parameter validation failures: The procedure raises an error (`SIGNAL SQLSTATE '45000'`). The `job_control` table shows `status = 'FAILED'` with an `error_message`, and `job_error_log` contains the detailed error.
        *   For kernel SP failures: The procedure raises an error. The `job_control` table shows `status = 'FAILED'` with an `error_message`, and `job_error_log` contains the detailed error, including a stack trace.
        *   All logging tables (`job_control`, `job_log`, `job_error_log`) are populated correctly for each test case.

2.  **Integration Testing with Cloud Composer (Airflow)**:
    *   **How to run**: Trigger the Airflow DAG that calls `sp_r_ausd_bp_ta_bcp_msisdn`.
    *   **Test Cases**:
        *   Run the DAG with default parameters.
        *   Run the DAG with specific `stichtag` and `wiederanlaufWert` passed via Airflow variables or configuration.
        *   Simulate a failure (e.g., by temporarily making the kernel SP non-existent or introducing an error).
    *   **What "passing" means**:
        *   The Airflow DAG completes successfully for valid runs.
        *   The BigQuery job triggered by Airflow completes successfully, as verified in the BigQuery UI and the `job_control` table.
        *   Airflow correctly captures and reports failures from the BigQuery SP.
        *   The `job_control`, `job_log`, and `job_error_log` tables are populated as expected, reflecting the Airflow-triggered executions.

3.  **Data Validation (Post-Kernel Migration)**:
    *   Once `sp_ausd_bp_ta_bcp_msisdn_kernel` is implemented, a full end-to-end data validation should be performed.
    *   **How to run**: Execute the migrated job (via Airflow).
    *   **What "passing" means**: The output data generated by the `sp_ausd_bp_ta_bcp_msisdn_kernel` in BigQuery matches the output data produced by the legacy `k_ausd_bp_ta_bcp_msisdn.ksh` for the same input parameters and source data. This typically involves row counts, checksums, and detailed data comparisons.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after deploying the migrated job, the following rollback procedure should be followed:

1.  **Immediate Action (Orchestration Rollback)**:
    *   **Disable New Orchestration**: Immediately disable or pause the newly deployed Cloud Composer (Airflow) DAG responsible for triggering `sp_r_ausd_bp_ta_bcp_msisdn`.
    *   **Re-enable Legacy Orchestration**: Re-enable the original scheduling mechanism (e.g., cron job, legacy scheduler) that was responsible for executing `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh`.
    *   **Verify Legacy Job Execution**: Monitor the legacy job to ensure it resumes normal operation and produces expected outputs.

2.  **Data Rollback (If Kernel SP Modified Data)**:
    *   This wrapper script itself does not modify business data. However, if the `sp_ausd_bp_ta_bcp_msisdn_kernel` (the migrated kernel logic) has already run and modified target tables, a data rollback strategy for those specific tables must be executed. This typically involves:
        *   Restoring target tables from a backup taken just before the migration.
        *   Running a reverse transformation or data correction script.
        *   (Ideally) The kernel SP should have a mechanism for idempotent execution or a clear recovery path.

3.  **Cleanup (Optional)**:
    *   If the rollback is deemed permanent or long-term, the deployed BigQuery Stored Procedure (`sp_r_ausd_bp_ta_bcp_msisdn`) and the associated DDL tables (`job_control`, `job_log`, `job_error_log`) can be dropped from BigQuery. However, these artifacts are generally benign and can be left in place if they do not interfere with other processes.
    *   The Airflow DAG can be deleted or marked as inactive.

4.  **Root Cause Analysis**:
    *   Once the legacy system is stable, perform a thorough root cause analysis of the issues encountered with the migrated job. This will inform necessary corrections before attempting re-deployment.