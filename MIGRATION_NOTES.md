# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `r_ausd_v_ta_cntrct_crs.ksh`, an orchestration layer for a contract data reconciliation process, has been migrated. Its primary function, which includes parameter parsing, environment setup, robust logging, error handling, and orchestrating a core data processing script, has been re-implemented.

The migration target platform is **Google BigQuery**, where the orchestration logic is encapsulated within a BigQuery Stored Procedure. External scheduling and invocation will be handled by **Cloud Composer (Airflow DAG)** or **Cloud Workflows**. Logging and auditing functionalities have been transitioned from filesystem-based logs to dedicated BigQuery audit tables.

## 2. Generated Artifacts

The migration process results in the following artifacts:

*   **`sp_vertragsdatenabgleich`** (BigQuery Stored Procedure): This is the primary migrated artifact, containing the orchestration logic, parameter handling, logging, and error management previously found in `r_ausd_v_ta_cntrct_crs.ksh`.
*   **`sp_ausd_v_ta_cntrct_crs`** (BigQuery Stored Procedure): This procedure will encapsulate the core data reconciliation logic, migrated from the original `k_ausd_v_ta_cntrct_crs.ksh` script. (Note: This procedure's content is dependent on the analysis of the core script, which is a known gap).
*   **`project.dataset.job_error_log`** (BigQuery Table): Stores detailed error messages and context for job failures.
*   **`project.dataset.job_control`** (BigQuery Table): Manages job entry numbers and metadata, providing a central point for job identification.
*   **`project.dataset.job_audit_log`** (BigQuery Table): Records detailed operational logs, including job start, progress, and completion messages.
*   **`project.dataset.job_status`** (BigQuery Table): Tracks the overall status (e.g., running, success, failed) of each job execution.
*   **Airflow DAG / Cloud Workflow** (Python): An external orchestration script responsible for scheduling, parameter passing, and invoking the `sp_vertragsdatenabgleich` BigQuery Stored Procedure.

## 3. Key Design Decisions

*   **Orchestration to BigQuery Stored Procedure**: The shell script's orchestration logic (parameter handling, logging, error handling) was directly translated into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This leverages BigQuery's native capabilities for procedural logic and keeps the control flow within the data warehouse environment.
*   **Structured Logging in BigQuery**: Instead of writing to filesystem log files, all job-related messages, errors, and audit trails are now inserted into dedicated BigQuery tables (`job_error_log`, `job_control`, `job_audit_log`, `job_status`). This provides structured, queryable, and centralized logging for easier monitoring and analysis.
*   **External Orchestration**: The invocation of the BigQuery Stored Procedure is delegated to an external orchestrator (Cloud Composer/Airflow or Cloud Workflows). This separates scheduling and workflow management from the data processing logic, aligning with cloud-native best practices.
*   **Modularization of Core Logic**: The core data processing logic (originally in `k_ausd_v_ta_cntrct_crs.ksh`) is designed to be migrated into a separate BigQuery Stored Procedure (`sp_ausd_v_ta_cntrct_crs`), which is then called by the main orchestration procedure. This promotes reusability and maintainability.
*   **Translation of Shell Constructs**: KornShell-specific features like parameter parsing (`getopts`), error handling (`trap`, `exit`), and environment variable management are replaced with BigQuery SQL equivalents (e.g., `DECLARE` statements, `IF` conditions, `BEGIN...EXCEPTION` blocks, `SIGNAL SQLSTATE`).
*   **Trade-offs**:
    *   **Shift from File-based to Database Logging**: While offering better queryability and centralization, it requires a different approach to log viewing and alerting compared to traditional file tailing.
    *   **Dependency on External Orchestrator**: Introduces an additional component (Airflow/Workflows) that needs to be managed and monitored, but provides robust scheduling and dependency management.
    *   **Loss of Direct Shell Environment**: Environment variables and shell utilities are replaced by BigQuery-specific configurations and procedures, requiring careful mapping.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **BigQuery Audit/Log Table Creation**:
    *   Create the `job_error_log`, `job_control`, `job_audit_log`, and `job_status` tables within the target dataset. The schemas for these tables must be defined based on the logging requirements (e.g., `job_id`, `timestamp`, `message`, `error_code`, `status`).
3.  **IAM & Permissions**:
    *   Grant the service account used by Cloud Composer/Workflows (or any other invoking entity) appropriate BigQuery roles:
        *   `BigQuery Data Editor` or `BigQuery Admin` on the target dataset for creating/executing stored procedures and inserting into log tables.
        *   `BigQuery User` or `BigQuery Data Viewer` for monitoring and querying log tables.
    *   Ensure the service account has necessary permissions for Cloud Composer/Workflows execution.
4.  **Connection Strings/Configuration**:
    *   The BigQuery project ID and dataset ID must be correctly configured within the Airflow DAG or Cloud Workflow that invokes the BigQuery Stored Procedure.
5.  **Secrets Management**:
    *   If the core script (`k_ausd_v_ta_cntrct_crs.ksh`) or any of its dependencies involve sensitive credentials (e.g., for external APIs, databases), these must be securely stored in Google Secret Manager and accessed by the BigQuery Stored Procedure or the orchestrator.
6.  **Scheduling Setup**:
    *   Deploy the Airflow DAG to Cloud Composer or configure the Cloud Workflow.
    *   Define the schedule for the job, ensuring it aligns with the original job's execution frequency.
    *   Configure any necessary alerts or monitoring for the new orchestration.

## 5. Known Gaps & Unresolved References

The following items have been identified as gaps or require further analysis and resolution:

*   **Content of Core Script (`k_ausd_v_ta_cntrct_crs.ksh`)**: The actual data processing and reconciliation logic residing in `k_ausd_v_ta_cntrct_crs.ksh` was not part of the initial analysis. Its content must be thoroughly analyzed and migrated into `sp_ausd_v_ta_cntrct_crs`. This is a critical dependency for full functionality.
*   **Sourced Utility Scripts**: The functionality of shell utility scripts sourced by `r_ausd_v_ta_cntrct_crs.ksh` (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) needs detailed analysis. Their functions (error messaging, parameter handling, date operations) must be translated into BigQuery Stored Procedures or UDFs, or their logic absorbed into the main orchestration procedure.
*   **Environment Variable Mapping**: The original script relies on environment variables like `$HOME` and `${BERT_DIR_ROOT}`. These need to be explicitly mapped to BigQuery project/dataset names, configuration parameters, or hardcoded values in the target environment.
*   **Shell-specific Behavior Parity**: While conceptual equivalents exist, achieving exact behavioral parity for shell-specific features like `set -eu` (strict error handling) and `trap` (signal management) in BigQuery SQL's `BEGIN...EXCEPTION` blocks requires careful implementation and thorough testing. Edge cases might behave differently.
*   **B4 Items (Redesign)**: Any specific redesign items identified during the analysis of the core script or utility scripts will need to be addressed as part of their respective migrations.

## 6. Validation

Validation of the migrated job involves a multi-stage testing approach:

1.  **Unit Testing BigQuery Stored Procedures**:
    *   **`sp_vertragsdatenabgleich`**: Test independently by calling it with various valid and invalid parameters. Verify that:
        *   Parameters are parsed and validated correctly.
        *   Audit/log tables (`job_control`, `job_audit_log`, `job_status`) are populated with correct job metadata and status updates.
        *   Error conditions (e.g., invalid parameters) are caught, logged in `job_error_log`, and appropriate signals are raised.
        *   The call to `sp_ausd_v_ta_cntrct_crs` is correctly initiated.
    *   **`sp_ausd_v_ta_cntrct_crs`**: Once developed, unit test this procedure to ensure its core data reconciliation logic functions as expected, performing correct reads, transformations, and writes to target tables.
2.  **Integration Testing via External Orchestrator**:
    *   Execute the Airflow DAG or Cloud Workflow that invokes `sp_vertragsdatenabgleich`.
    *   **"Passing" means**:
        *   The external orchestrator successfully triggers the BigQuery Stored Procedure.
        *   The `sp_vertragsdatenabgleich` procedure executes without unhandled errors.
        *   All audit and log tables (`job_control`, `job_audit_log`, `job_status`, `job_error_log`) are populated accurately, reflecting the job's lifecycle and outcome.
        *   The `sp_ausd_v_ta_cntrct_crs` (core logic) is successfully invoked and completes its data processing.
        *   The expected data changes, transformations, or reconciliations (as defined by the original `k_ausd_v_ta_cntrct_crs.ksh`) are observed in the target BigQuery tables.
        *   The overall job status reported in `job_status` is `SUCCESS` for successful runs and `FAILED` with appropriate error details for failed runs.
        *   Performance metrics are within acceptable thresholds.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Deactivate New Orchestration**: Immediately pause or disable the Airflow DAG in Cloud Composer or the Cloud Workflow that invokes `sp_vertragsdatenabgleich`. This stops any further execution of the migrated job.
2.  **Re-enable Legacy Scheduler**: Re-enable the original scheduler (e.g., UC4) responsible for invoking `r_ausd_v_ta_cntrct_crs.ksh` in the legacy environment.
3.  **Verify Legacy Job Execution**: Monitor the legacy job to ensure it resumes normal operation and processes data correctly.
4.  **Data Integrity Check**: If the migrated job performed any data writes or modifications before rollback, assess the state of the affected data. Depending on the nature of the core logic (`sp_ausd_v_ta_cntrct_crs`), a data rollback or correction might be necessary. This should be part of the core script's migration design.
5.  **Optional: Clean Up Migrated Artifacts**: If the rollback is deemed permanent or a significant re-design is required, the BigQuery Stored Procedures (`sp_vertragsdatenabgleich`, `sp_ausd_v_ta_cntrct_crs`) and associated audit tables can be dropped from BigQuery.
6.  **Root Cause Analysis**: Investigate the reason for the rollback using the BigQuery audit/error logs to identify and resolve the issues before attempting re-migration or re-deployment.