# MIGRATION_NOTES.md

## 1. Summary

This migration addresses the KornShell script `r_ausd_bp_ta_apn_carmen.ksh`, which acts as an orchestration and parameter-handling wrapper for the initial provisioning of selected basic products for BERT. Its primary function involves preparing parameters, determining the processing date (`Stichtag`), handling restart logic, and invoking a core processing script (`k_ausd_bp_ta_apn_carmen.ksh`). It also integrates with a custom messaging and error logging framework.

The job has been migrated to Google Cloud Platform, primarily utilizing **BigQuery Stored Procedures** for the orchestration logic and **BigQuery Tables** for logging and auditing. The original UC4 scheduler will be replaced by **Cloud Composer (Airflow)** for workflow orchestration.

## 2. Generated artifacts

The migration process has generated the following BigQuery DDL and Stored Procedure:

*   **`bigquery_ddl/job_audit_log.sql`**
    *   **Role:** This DDL script creates the `job_audit_log` BigQuery table. This table serves as the central repository for tracking the lifecycle of job executions, including start times, end times, parameters, and overall status (STARTED, SUCCESS, ERROR). It replaces the custom shell-based logging framework (`DWMSG_*` functions) for audit purposes.
*   **`bigquery_ddl/job_error_log.sql`**
    *   **Role:** This DDL script creates the `job_error_log` BigQuery table. It is designed to capture detailed information about errors encountered during job execution, including error codes, arguments, and messages. This replaces the error reporting aspects of the `DWMSG_*` framework.
*   **`bigquery_ddl/job_status.sql`**
    *   **Role:** This DDL script creates the `job_status` BigQuery table. This table is intended to track the current, real-time status of ongoing or last-run jobs, providing a quick overview of job health.
*   **`bigquery_stored_procedures/sp_ausd_bp_ta_apn_carmen.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the entire orchestration and parameter-handling logic of the original `r_ausd_bp_ta_apn_carmen.ksh` script. It handles input parameters (`p_stichtag`, `p_wiederanlaufWert`), determines the processing date, performs validation, manages logging to the `job_audit_log` and `job_error_log` tables, and ultimately invokes the core processing logic (expected to be `sp_k_ausd_bp_ta_apn_carmen`).

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures for Orchestration:**
    *   **Why:** BigQuery Stored Procedures offer a serverless, scalable, and cost-effective way to execute complex SQL logic and orchestrate data pipelines directly within BigQuery. This aligns with the target architecture's focus on BigQuery as the primary data platform. It allows for direct parameter passing, robust error handling (`BEGIN...EXCEPTION...END`), and integration with BigQuery's native date functions, effectively replacing the KornShell script's logic.
    *   **Trade-offs:** Requires rewriting shell-specific constructs (e.g., `getopts`, `trap`, `source` commands) into BigQuery SQL. The `DWMSG_*` logging framework needed a complete re-implementation using BigQuery tables.
*   **Dedicated BigQuery Tables for Logging and Auditing:**
    *   **Why:** Replacing the filesystem-based `DWMSG_*` logging framework with BigQuery tables (`job_audit_log`, `job_error_log`, `job_status`) centralizes logging, makes it queryable, and integrates seamlessly with GCP's monitoring and alerting services (Cloud Logging, Cloud Monitoring). This provides a more robust, scalable, and observable logging solution.
    *   **Trade-offs:** Requires explicit `INSERT` and `UPDATE` statements within the stored procedure for every log event, which adds verbosity compared to a shell function call.
*   **Cloud Composer (Airflow) for Scheduling:**
    *   **Why:** Airflow is the standard GCP solution for orchestrating complex data pipelines, offering superior capabilities compared to UC4 for dependency management, retries, monitoring, and integration with other GCP services. It provides a programmatic way to define workflows.
    *   **Trade-offs:** Introduces a new technology stack (Python, Airflow concepts) and requires defining DAGs to trigger the BigQuery Stored Procedures.
*   **Parameter Handling via Stored Procedure Inputs:**
    *   **Why:** Directly passing `p_stichtag` and `p_wiederanlaufWert` as `IN` parameters to the BigQuery Stored Procedure is the idiomatic way to handle inputs in SQL procedures, replacing the shell's `getopts` mechanism. This ensures type safety and clear interface definition.
    *   **Trade-offs:** Requires the calling Airflow DAG to correctly format and pass these parameters.
*   **Dependency on Core Script Migration (`sp_k_ausd_bp_ta_apn_carmen`):**
    *   **Why:** The wrapper script's primary function is to invoke the core logic. To maintain a fully BigQuery-native pipeline, the core script (`k_ausd_bp_ta_apn_carmen.ksh`) must also be migrated, ideally into another BigQuery Stored Procedure. This ensures end-to-end execution within BigQuery.
    *   **Trade-offs:** This introduces a significant dependency and an unresolved item, as the core script's complexity is unknown and its migration is a separate effort.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `your_project_id.your_dataset_id`) exists. If not, create it.
2.  **BigQuery Table Creation:**
    *   Execute the DDL scripts for the logging tables:
        *   `bigquery_ddl/job_audit_log.sql`
        *   `bigquery_ddl/job_error_log.sql`
        *   `bigquery_ddl/job_status.sql`
    *   Replace `your_project_id.your_dataset_id` placeholders with the actual project and dataset IDs.
3.  **BigQuery Stored Procedure Creation:**
    *   Execute the DDL script for the wrapper stored procedure:
        *   `bigquery_stored_procedures/sp_ausd_bp_ta_apn_carmen.sql`
    *   Replace `your_project_id.your_dataset_id` placeholders with the actual project and dataset IDs.
4.  **Core Stored Procedure Creation (Prerequisite):**
    *   The core processing logic from `k_ausd_bp_ta_apn_carmen.ksh` must be migrated and deployed as `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_apn_carmen` (or equivalent). This is a critical dependency.
5.  **IAM / Permissions:**
    *   The service account used by Cloud Composer (or any other orchestrator) to invoke the BigQuery Stored Procedure must have the following BigQuery roles:
        *   `BigQuery Data Editor` on the target dataset (to `INSERT` into logging tables and `UPDATE` `job_status`).
        *   `BigQuery Job User` on the project (to run BigQuery jobs, including stored procedures).
        *   `BigQuery Data Viewer` on any source tables accessed by `sp_k_ausd_bp_ta_apn_carmen`.
6.  **Cloud Composer (Airflow) DAG Deployment:**
    *   Develop and deploy an Airflow DAG that:
        *   Triggers `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`.
        *   Passes `p_stichtag` (e.g., `{{ ds_nodash }}` for current date or a specific date) and `p_wiederanlaufWert` (e.g., `0` or a configurable value) as parameters.
        *   Handles scheduling (e.g., daily execution).
        *   Configures appropriate retries and alerts.
7.  **Connection Strings / Secrets:**
    *   No explicit connection strings are required for BigQuery operations when executed from within GCP (e.g., Airflow using default service accounts). Ensure the Airflow environment has the necessary GCP connection configured.
    *   If any secrets were handled by `$HOME/.dw_init` in the original script, these must be securely managed in GCP (e.g., Secret Manager) and passed to the Airflow DAG or BigQuery Stored Procedure if needed by the core logic.

## 5. Known gaps & unresolved references

*   **Core Script Logic (`k_ausd_bp_ta_apn_carmen.ksh`):** The internal logic, data sources, and targets of the invoked core script are not part of this migration design. Its migration design and complexity are currently unresolved and represent the largest risk. It's assumed to contain the actual data processing logic, which could involve complex SQL, file operations, or other shell commands that require careful translation to BigQuery SQL, Python, or other GCP services. The placeholder `CALL your_project_id.your_dataset_id.sp_k_ausd_bp_ta_apn_carmen(...)` will fail until this procedure is implemented.
*   **`DWMSG_*` Framework Full Replacement:** While audit tables are proposed for logging, the full scope of the `DWMSG_*` framework (e.g., specific alert mechanisms, integration with monitoring systems, email notifications) needs to be mapped and implemented on GCP (e.g., Cloud Logging, Cloud Monitoring, Pub/Sub for alerts, Cloud Functions for notifications). The current solution only covers the data persistence of logs.
*   **"MAX(ladedatum)" logic in Stichtag determination:** The original `usage` description mentioned `MIN(sysdate,maxladedatum)` for synchronization, but the script defaults to `sysdate` if `-s` is not provided. This discrepancy might indicate a hidden business rule or a potential for data integrity issues if `maxladedatum` is critical for correct `Stichtag` calculation. This requires clarification and potential adjustment of the `v_stichtag` derivation logic.
*   **`trap` functionality nuances:** The direct translation of `trap` (for `INT`, `STOP`, `CONT`, `ERR`) to BigQuery `EXCEPTION` blocks handles errors, but nuances like graceful shutdowns or specific signal handling might need to be addressed at the orchestration level (Cloud Composer) if they were critical in the original script.
*   **Complexity Tier and Migration Flags:** The `file_complexity` data was not available, meaning a detailed, pre-assessed complexity and specific migration challenges for this script were unknown. This could introduce unforeseen complexities during implementation of the core logic.

## 6. Validation

To validate the migrated `sp_ausd_bp_ta_apn_carmen` stored procedure:

1.  **Prerequisites:**
    *   Ensure all BigQuery DDLs (`job_audit_log`, `job_error_log`, `job_status`) have been executed.
    *   Ensure `sp_ausd_bp_ta_apn_carmen` has been created.
    *   **Crucially, a dummy or fully implemented `sp_k_ausd_bp_ta_apn_carmen` must exist.** For initial testing of the wrapper, `sp_k_ausd_bp_ta_apn_carmen` can be a simple procedure that just logs its parameters and returns, or even raises a controlled error to test the error handling of the wrapper.

2.  **Running Tests:**
    *   **Manual Execution (BigQuery Console/CLI):**
        *   **Successful Run:**
            ```sql
            CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('01012023', 0);
            -- Or for default Stichtag:
            CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`(NULL, 0);
            ```
        *   **Parameter Validation Error (e.g., missing Stichtag if logic was stricter):**
            ```sql
            -- The current SP defaults Stichtag, so this might not trigger the specific v_errnr=193.
            -- To test this path, you might temporarily modify the SP to enforce non-NULL/empty Stichtag.
            -- Example if Stichtag was strictly required:
            -- CALL `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`('', 0);
            ```
        *   **Core Logic Error (simulated):** If `sp_k_ausd_bp_ta_apn_carmen` is designed to raise an error under certain conditions, call `sp_ausd_bp_ta_apn_carmen` with parameters that trigger that error.
    *   **Cloud Composer (Airflow) Execution:**
        *   Deploy a simple Airflow DAG that calls `sp_ausd_bp_ta_apn_carmen` with various parameters (valid, invalid, triggering core errors).

3.  **"Passing" Criteria:**
    *   **Successful Run:**
        *   The `CALL` statement completes without raising an unhandled BigQuery error.
        *   A `SUCCESS` entry is found in `your_project_id.your_dataset_id.job_audit_log` for the corresponding `job_kennung` and `job_nr`.
        *   The `job_status` table shows `status = 'SUCCESS'` for the latest run of this `job_kennung`.
        *   The `sp_k_ausd_bp_ta_apn_carmen` (or its dummy) is successfully invoked and its expected side effects (e.g., data processing, internal logging) are observed.
    *   **Parameter Validation Error:**
        *   The `CALL` statement raises a `SIGNAL SQLSTATE '45000'` error with a message indicating a parameter issue.
        *   An `ERROR` entry is found in `your_project_id.your_dataset_id.job_error_log` with `error_nr = 193` and `error_arg = 'Stichtag'`.
        *   An `ERROR` entry is found in `your_project_id.your_dataset_id.job_audit_log` with `status = 'ERROR'`.
        *   The `job_status` table shows `status = 'ERROR'`.
    *   **Core Logic Error:**
        *   The `CALL` statement raises an error originating from `sp_k_ausd_bp_ta_apn_carmen`.
        *   An `ERROR` entry is found in `your_project_id.your_dataset_id.job_error_log` with details of the error from the core procedure.
        *   An `ERROR` entry is found in `your_project_id.your_dataset_id.job_audit_log` with `status = 'ERROR'`.
        *   The `job_status` table shows `status = 'ERROR'`.

## 7. Rollback procedure

The rollback procedure involves reverting to the original KornShell script and UC4 scheduling.

1.  **Stop New Deployments:** Halt any further deployments of the migrated BigQuery components or Airflow DAGs.
2.  **Disable Cloud Composer DAG:**
    *   In the Cloud Composer UI, disable the Airflow DAG responsible for invoking `sp_ausd_bp_ta_apn_carmen`.
3.  **Re-enable UC4 Job:**
    *   Re-enable the original UC4 job definition (`DW.BERT_AUSD_BP_TA_APN_CARMEN.xml`) to resume execution of `r_ausd_bp_ta_apn_carmen.ksh`.
4.  **Monitor Original Job:**
    *   Verify that the original KornShell script is executing correctly via UC4 and producing expected outputs.
5.  **Optional: Clean up Migrated Resources (if necessary):**
    *   If the rollback is permanent or for a significant period, consider dropping the BigQuery Stored Procedure (`sp_ausd_bp_ta_apn_carmen`) and the logging tables (`job_audit_log`, `job_error_log`, `job_status`) to avoid incurring costs or confusion.
    *   `DROP PROCEDURE IF EXISTS your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen;`
    *   `DROP TABLE IF EXISTS your_project_id.your_dataset_id.job_audit_log;`
    *   `DROP TABLE IF EXISTS your_project_id.your_dataset_id.job_error_log;`
    *   `DROP TABLE IF EXISTS your_project_id.your_dataset_id.job_status;`
    *   Remove the Airflow DAG from the Cloud Composer environment.

**Note:** A full rollback of the data processed by the core script (`k_ausd_bp_ta_apn_carmen.ksh` or `sp_k_ausd_bp_ta_apn_carmen`) would depend on the specific data transformation and loading logic within that script and is outside the scope of this wrapper migration. It would typically involve restoring data from backups or running reverse ETL processes.