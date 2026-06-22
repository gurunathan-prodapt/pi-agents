# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `r_ausd_bp_ta_cntrct_dist.ksh`, responsible for the initial provisioning of selected base products for the BERT system and providing contract cache data to the Forderungsscoring (FOS) system, has been migrated.

The migration target platform is Google Cloud Platform (GCP), specifically utilizing:
*   **BigQuery** for data processing, storage, and implementing the core logic via Stored Procedures.
*   **Cloud Composer (Apache Airflow) or Cloud Workflows** for job orchestration and scheduling.
*   **Cloud Logging and Cloud Monitoring** for platform-level visibility and operational insights.

The migrated component acts as a wrapper, handling parameter processing, defaulting, validation, and logging, while orchestrating a call to a separate BigQuery Stored Procedure that will contain the core business logic (kernel).

## 2. Generated artifacts

The following artifacts have been generated as part of this migration:

*   **`ddl/logging_tables.sql`**
    *   **Role:** This SQL script defines the Data Definition Language (DDL) for four BigQuery tables:
        *   `job_control`: Tracks overall job status, metadata, and parameters.
        *   `job_log`: Stores detailed execution logs for each job step.
        *   `job_error_log`: Captures specific error details, including stack traces.
        *   `job_message_log`: Records general job messages or business events.
    *   **Purpose:** These tables replace the legacy `DWMSG_*` custom logging framework, providing structured, queryable logs within BigQuery, enhancing observability and integration with GCP monitoring tools.

*   **`sprocs/ausd_bp_ta_cntrct_dist_wrapper.sql`**
    *   **Role:** This BigQuery Stored Procedure (`ausd_bp_ta_cntrct_dist_wrapper`) is the direct migration of the original `r_ausd_bp_ta_cntrct_dist.ksh` wrapper script.
    *   **Purpose:** It handles:
        *   Accepting input parameters (`p_stichtag` and `p_wiederanlaufWert`).
        *   Defaulting parameters (e.g., `p_stichtag` to current date, `p_wiederanlaufWert` to 0).
        *   Validating parameter formats and presence.
        *   Logging job lifecycle events, parameters, and errors into the new BigQuery logging tables.
        *   Orchestrating the call to the core business logic, which will reside in a separate BigQuery Stored Procedure named `ausd_bp_ta_cntrct_dist_kernel`.
        *   Implementing robust error handling using BigQuery's `EXCEPTION WHEN ERROR THEN` blocks.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Logic Implementation**: The KornShell script's logic (wrapper and future kernel) is re-implemented as BigQuery Stored Procedures. This leverages BigQuery's scalability, performance, and native SQL capabilities for data processing, eliminating the need for external compute for the core ETL.
*   **Structured Logging in BigQuery**: The custom `DWMSG_*` logging framework is replaced by dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`, `job_message_log`). This provides a standardized, queryable, and centralized logging solution, improving monitoring and debugging capabilities within GCP.
*   **Orchestration with Cloud Composer/Workflows**: The scheduling and orchestration of the job will transition from a legacy scheduler to Cloud Composer (Apache Airflow) or Cloud Workflows. This provides robust workflow management, dependency handling, retries, and integration with other GCP services.
*   **Separation of Wrapper and Kernel Logic**: The original script's modularity (wrapper calling a kernel script) is preserved. The `ausd_bp_ta_cntrct_dist_wrapper` SP handles orchestration and parameter management, while the core business logic will be migrated into a separate `ausd_bp_ta_cntrct_dist_kernel` SP. This allows for phased migration and clearer separation of concerns.
*   **Leveraging BigQuery Native Functions**: Shell-specific functionalities like date calculations (`DWDate_Gib_Zeitraum`), parameter parsing (`getopts`), and conditional logic are replaced with BigQuery's native SQL functions (`CURRENT_DATE()`, `FORMAT_DATE()`, `PARSE_DATE()`, `IFNULL`, `NULLIF`, `TRIM`, `IF` statements).
*   **BigQuery's `EXCEPTION WHEN ERROR` for Error Handling**: The legacy `trap` mechanism for error handling is replaced by BigQuery's structured `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks, providing more robust and granular error management within the stored procedures.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the target BigQuery dataset (e.g., `your_project_id.your_dataset_id`) where the logging tables and stored procedures will reside.
    *   **Action:** `bq mk --dataset your_project_id:your_dataset_id`

2.  **IAM/Permissions Configuration**:
    *   Ensure the service account that will execute the BigQuery stored procedures (e.g., via Cloud Composer or directly) has the necessary BigQuery roles:
        *   `BigQuery Data Editor` (or `BigQuery Admin`) on the target dataset to create tables, insert logs, and execute stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
    *   If the `ausd_bp_ta_cntrct_dist_kernel` interacts with other GCP resources (e.g., Cloud Storage, other BigQuery datasets), ensure the service account has appropriate permissions for those resources.

3.  **Deploy Logging Tables DDL**:
    *   Execute the `ddl/logging_tables.sql` script in BigQuery to create the `job_control`, `job_log`, `job_error_log`, and `job_message_log` tables.
    *   **Action:** `bq query --use_legacy_sql=false < ddl/logging_tables.sql` (after replacing `project_id.dataset_id`).

4.  **Deploy Wrapper Stored Procedure**:
    *   Execute the `sprocs/ausd_bp_ta_cntrct_dist_wrapper.sql` script in BigQuery to create the `ausd_bp_ta_cntrct_dist_wrapper` stored procedure.
    *   **Action:** `bq query --use_legacy_sql=false < sprocs/ausd_bp_ta_cntrct_dist_wrapper.sql` (after replacing `project_id.dataset_id`).

5.  **Implement and Deploy Kernel Stored Procedure**:
    *   **Crucial Step:** The `ausd_bp_ta_cntrct_dist_kernel` stored procedure, containing the core business logic from `k_ausd_bp_ta_cntrct_dist.ksh`, must be fully implemented and deployed to BigQuery. This is a placeholder in the current generated code.

6.  **Orchestration Setup (Cloud Composer/Workflows)**:
    *   Create and deploy a Cloud Composer DAG or Cloud Workflow definition to schedule and execute the `project_id.dataset_id.ausd_bp_ta_cntrct_dist_wrapper` stored procedure.
    *   Configure parameters, scheduling frequency, retries, and dependencies within the orchestrator.

## 5. Known gaps & unresolved references

*   **Kernel Script Logic (`k_ausd_bp_ta_cntrct_dist.ksh`)**: The most significant gap is the unmigrated core business logic residing in `k_ausd_bp_ta_cntrct_dist.ksh`. Its content, data sources, data targets, and detailed transformation logic are currently unknown. The `ausd_bp_ta_cntrct_dist_kernel` stored procedure is a placeholder and needs to be fully designed and implemented. This is a **B4 item** requiring further analysis and development.
*   **External Dependencies of Kernel**: Any external systems or specific data sources/sinks accessed by `k_ausd_bp_ta_cntrct_dist.ksh` are not yet identified. Their migration strategy (e.g., Cloud Functions, Cloud Run, Dataproc) will depend on the kernel's analysis.
*   **Missing Complexity/Automation Rate**: The original script's complexity and automation potential were not available from the analysis database. This means the migration effort estimation was based purely on manual analysis, and there might be unforeseen complexities in the kernel.
*   **Full Parity of Custom Logging Framework**: While a new BigQuery logging solution is in place, ensuring complete functional parity with all nuances of the legacy `DWMSG_*` framework and potentially migrating historical logs might require additional effort.
*   **Performance Optimization Notes**: The `AL??` comments in the original script regarding `FOSHoleLadedatum` suggest specific performance considerations or alternative data sources. These need to be understood and addressed during the migration of the kernel logic to ensure optimal performance in BigQuery.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to run the tests:**

1.  **Unit Testing (Wrapper SP)**:
    *   Execute the `ausd_bp_ta_cntrct_dist_wrapper` stored procedure directly in the BigQuery console with various parameter combinations:
        *   **Valid parameters**: Provide a valid `p_stichtag` (e.g., '31122023') and `p_wiederanlaufWert` (e.g., 0 or 100).
        *   **Defaulting `p_stichtag`**: Call with `p_stichtag = NULL` or `p_stichtag = ''`.
        *   **Defaulting `p_wiederanlaufWert`**: Call with `p_wiederanlaufWert = NULL`.
        *   **Invalid `p_stichtag` format**: Provide a malformed date string (e.g., '2023-12-31', '31/12/2023').
        *   **Missing `p_stichtag` (after defaulting)**: (This scenario should be prevented by the SP's validation, but can be tested by manipulating the SP to ensure the error is caught).
    *   Monitor the BigQuery job history and query the `job_control`, `job_log`, `job_error_log`, and `job_message_log` tables to verify correct logging.

2.  **Integration Testing (Wrapper + Kernel SP)**:
    *   Once `ausd_bp_ta_cntrct_dist_kernel` is implemented and deployed, execute the `ausd_bp_ta_cntrct_dist_wrapper` SP.
    *   Verify that the `ausd_bp_ta_cntrct_dist_kernel` is called with the correct, validated parameters.
    *   Monitor the execution of the kernel SP and its impact on target tables.

3.  **End-to-End Testing (Orchestrator + Wrapper + Kernel)**:
    *   Trigger the Cloud Composer DAG or Cloud Workflow.
    *   Monitor the orchestrator's logs and status.
    *   Verify the entire flow, from scheduling to final data output.

4.  **Data Validation**:
    *   Run the legacy `r_ausd_bp_ta_cntrct_dist.ksh` script with a specific set of input parameters.
    *   Run the migrated BigQuery job with the *exact same* input parameters.
    *   Compare the output data (e.g., target tables for FOS) from both systems to ensure byte-for-byte or record-by-record equivalence. This is the most critical validation step once the kernel is migrated.

**What "passing" means:**

*   **Successful Execution**: The `ausd_bp_ta_cntrct_dist_wrapper` (and subsequently the `ausd_bp_ta_cntrct_dist_kernel`) completes without errors, and the `job_control` table shows `status = 'SUCCESS'`.
*   **Correct Parameter Handling**: Parameters are parsed, defaulted, and validated as expected. Invalid inputs result in appropriate error messages and job failure (e.g., `SQLSTATE '45000'`).
*   **Accurate Logging**: All relevant steps, parameters, and status updates are correctly recorded in the `job_log` and `job_control` tables. Errors are logged in `job_error_log`.
*   **Kernel Invocation**: The `ausd_bp_ta_cntrct_dist_kernel` stored procedure is successfully called with the correct `stichtag` (as a `DATE` type) and `wiederanlaufWert`.
*   **Data Equivalence (Post-Kernel Migration)**: The data produced by the migrated job in BigQuery (e.g., in the tables provided to FOS) is identical to the data produced by the legacy KornShell script for the same input parameters and source data state.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after deploying the migrated job, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Disable New Job**: Immediately disable or pause the Cloud Composer DAG or Cloud Workflow that orchestrates the `ausd_bp_ta_cntrct_dist_wrapper` stored procedure. This prevents further execution of the new job.

2.  **Revert to Legacy System**:
    *   **Re-enable Legacy Job**: Re-enable the original `r_ausd_bp_ta_cntrct_dist.ksh` script and its associated legacy scheduling mechanism.
    *   **Verify Legacy Operation**: Confirm that the legacy job is running as expected and producing correct output.

3.  **Cleanup (if necessary and migration is abandoned)**:
    *   If the migration is deemed unsuccessful and a full revert is required, the following BigQuery resources can be dropped:
        *   The `ausd_bp_ta_cntrct_dist_wrapper` stored procedure.
        *   The `ausd_bp_ta_cntrct_dist_kernel` stored procedure (if it was deployed).
        *   The logging tables: `job_control`, `job_log`, `job_error_log`, `job_message_log`.
    *   **Action (example):**
        ```sql
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.ausd_bp_ta_cntrct_dist_wrapper`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.ausd_bp_ta_cntrct_dist_kernel`; -- If deployed
        DROP TABLE IF EXISTS `project_id.dataset_id.job_control`;
        DROP TABLE IF EXISTS `project_id.dataset_id.job_log`;
        DROP TABLE IF EXISTS `project_id.dataset_id.job_error_log`;
        DROP TABLE IF EXISTS `project_id.dataset_id.job_message_log`;
        ```
    *   **Note**: Dropping tables will delete all historical log data. Consider backing up critical log data if needed before dropping.

4.  **Post-Rollback Analysis**:
    *   Investigate the root cause of the issues that necessitated the rollback. Address the identified problems in the migrated code or design before attempting another deployment.