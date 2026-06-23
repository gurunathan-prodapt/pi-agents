# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `r_ausd_v_ta_barrier.ksh` from a legacy Unix/Linux environment to Google Cloud Platform. The script, which orchestrates a contract data reconciliation job against the `ta_barrier` table, has been re-engineered to leverage Google Cloud's serverless data warehousing and orchestration capabilities.

The migration targets:
*   **Core Logic & Utilities:** BigQuery Stored Procedures for all business logic, parameter handling, logging, and error management.
*   **Data Persistence:** BigQuery tables for logging and job status tracking.
*   **Orchestration:** Google Cloud Composer (Apache Airflow) for scheduling, execution, and monitoring.

The original script's primary function was to initialize the runtime environment, parse parameters, set up logging, invoke a kernel script (`k_ausd_v_ta_barrier.ksh`), and record job status. This functionality is now fully encapsulated within BigQuery Stored Procedures and orchestrated by Airflow.

## 2. Generated Artifacts

The migration process generated the following artifacts:

*   **`sql/ddl/job_log_table_ddl.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `job_log_table`. This table stores detailed log messages, job numbers, job identifiers, timestamps, and severity levels for all migrated jobs, replacing the legacy filesystem-based log files.
*   **`sql/ddl/job_status_table_ddl.sql`**
    *   **Role:** BigQuery DDL script to create the `job_status_table`. This table tracks the current status (e.g., 'RUNNING', 'SUCCESS', 'FAILED') and last update timestamp for each job, providing a centralized status repository.
*   **`sql/procedures/DWMSG_ErmittleNr_SP.sql`**
    *   **Role:** BigQuery Stored Procedure that generates a unique job entry number (`DW_EintragsNr`). It replaces the `DWMSG_ErmittleNr` shell function, ensuring unique identifiers for job runs within BigQuery.
*   **`sql/procedures/DWMSG_Logdateiname_SP.sql`**
    *   **Role:** BigQuery Stored Procedure that constructs a logical "log filename" string. This replaces the `DWMSG_Logdateiname` shell function, providing a consistent identifier for log entries even though physical files are no longer used.
*   **`sql/procedures/DWMSG_ErzeugeEintrag_SP.sql`**
    *   **Role:** BigQuery Stored Procedure that creates an initial entry in both the `job_log_table` and `job_status_table` when a job starts. It mimics the `DWMSG_ErzeugeEintrag` shell function.
*   **`sql/procedures/DWMSG_SetzeStichtagInfo_SP.sql`**
    *   **Role:** BigQuery Stored Procedure to log reference date (Stichtag) information. It replaces the `DWMSG_SetzeStichtagInfo` shell function.
*   **`sql/procedures/DWMSG_MeldeFehler_SP.sql`**
    *   **Role:** BigQuery Stored Procedure for logging specific error messages, including error codes and arguments, and updating the job status to 'FAILED'. It replaces the `DWMSG_MeldeFehler` shell function.
*   **`sql/procedures/DWMSG_Fehlerbehandlung_SP.sql`**
    *   **Role:** BigQuery Stored Procedure for generic error handling, logging unhandled exceptions, and setting the job status to 'FAILED'. It replaces the `trap ERR/INT` mechanisms of the shell script.
*   **`sql/procedures/DWMSG_SetzeStatusOK_SP.sql`**
    *   **Role:** BigQuery Stored Procedure to log a success message and update the job status to 'SUCCESS'. It replaces the `DWMSG_SetzeStatusOK` shell function.
*   **`sql/procedures/k_ausd_v_ta_barrier_sp.sql`**
    *   **Role:** BigQuery Stored Procedure acting as a placeholder for the core kernel logic of `k_ausd_v_ta_barrier.ksh`. This procedure will contain the actual contract data reconciliation logic once fully migrated.
*   **`sql/procedures/r_ausd_v_ta_barrier_sp.sql`**
    *   **Role:** The main BigQuery Stored Procedure, migrated from `r_ausd_v_ta_barrier.ksh`. This procedure orchestrates the entire job, handling parameter validation, calling utility procedures, invoking the kernel procedure, and managing overall job status and logging.
*   **`dags/r_ausd_v_ta_barrier_dag.py`**
    *   **Role:** An Apache Airflow DAG (Python script) for Google Cloud Composer. This DAG is responsible for scheduling, triggering, and monitoring the execution of the `r_ausd_v_ta_barrier_sp` BigQuery Stored Procedure, replacing the legacy cron-based scheduling and manual execution.

## 3. Key Design Decisions

*   **Migration to BigQuery Stored Procedures:** The core decision was to translate KornShell logic directly into BigQuery SQL Stored Procedures. This leverages BigQuery's serverless, scalable, and cost-effective execution environment, eliminating the need for managing compute instances for shell scripts. It also allows for direct interaction with BigQuery data.
*   **Consolidation of Utility Functions:** Reusable shell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were refactored into dedicated BigQuery Stored Procedures or UDFs. This promotes modularity, reusability within the BigQuery ecosystem, and simplifies maintenance by centralizing common logic.
*   **Structured Logging to BigQuery Tables:** Instead of filesystem-based log files, all logging and status updates are directed to BigQuery tables (`job_log_table`, `job_status_table`). This provides structured, queryable logs, enhances observability, and allows for easy integration with Google Cloud Logging and Monitoring.
*   **BigQuery Native Error Handling:** The shell's `trap` mechanisms for error handling were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END;` blocks within stored procedures. This provides robust, native error management within the SQL context.
*   **Cloud Composer (Airflow) for Orchestration:** Airflow was chosen for scheduling and orchestration due to its powerful DAG capabilities, rich set of operators for Google Cloud services, and robust monitoring features. This replaces simple cron jobs and provides a more resilient and observable workflow.
*   **Parameterization:** Command-line parameters from the original script are now passed as input parameters to the BigQuery Stored Procedures and managed by the Airflow DAG. This ensures flexibility and configurability of the job.

**Notable Trade-offs:**
*   **Re-implementation Effort:** Direct translation from KornShell to BigQuery SQL scripting requires a complete re-implementation, as the paradigms are significantly different.
*   **Loss of Direct Filesystem Access:** Operations that relied on local filesystem access (e.g., reading/writing temporary files, specific directory structures) had to be re-architected to use BigQuery tables or other cloud storage solutions.
*   **Learning Curve:** Developers familiar with shell scripting may need to adapt to BigQuery SQL scripting's procedural constructs and best practices.

## 4. Manual Steps before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **Google Cloud Project and Dataset Setup:**
    *   Ensure a Google Cloud Project (`my_project_id`) is active and billing is enabled.
    *   Create the target BigQuery Dataset (`my_dataset_id`) within the project.
        ```bash
        bq mk --dataset my_project_id:my_dataset_id
        ```
2.  **BigQuery Table Creation:**
    *   Execute the DDL scripts to create the logging and status tables:
        ```bash
        bq query --use_legacy_sql=false < sql/ddl/job_log_table_ddl.sql
        bq query --use_legacy_sql=false < sql/ddl/job_status_table_ddl.sql
        ```
3.  **BigQuery Stored Procedure Deployment:**
    *   Deploy all generated BigQuery Stored Procedures to the `my_dataset_id` dataset. This can be done via the BigQuery UI, `bq query` command, or a CI/CD pipeline.
        ```bash
        bq query --use_legacy_sql=false < sql/procedures/DWMSG_ErmittleNr_SP.sql
        bq query --use_legacy_sql=false < sql/procedures/DWMSG_Logdateiname_SP.sql
        bq query --use_legacy_sql=false < sql/procedures/DWMSG_ErzeugeEintrag_SP.sql
        bq query --use_legacy_sql=false < sql/procedures/DWMSG_SetzeStichtagInfo_SP.sql
        bq query --use_legacy_sql=false < sql/procedures/DWMSG_MeldeFehler_SP.sql
        bq query --use_legacy_sql=false < sql/procedures/DWMSG_Fehlerbehandlung_SP.sql
        bq query --use_legacy_sql=false < sql/procedures/DWMSG_SetzeStatusOK_SP.sql
        bq query --use_legacy_sql=false < sql/procedures/k_ausd_v_ta_barrier_sp.sql
        bq query --use_legacy_sql=false < sql/procedures/r_ausd_v_ta_barrier_sp.sql
        ```
4.  **IAM Permissions:**
    *   The Google Cloud service account used by Cloud Composer (Airflow) must have the following BigQuery roles:
        *   `BigQuery Data Editor` (or `BigQuery Admin`) on `my_project_id.my_dataset_id` to execute stored procedures and insert/update data in log/status tables.
        *   `BigQuery Job User` to run BigQuery jobs.
    *   Ensure any other service accounts or users interacting with these resources have appropriate permissions.
5.  **Airflow Connection Configuration:**
    *   Verify that the `google_cloud_default` connection is correctly configured in your Cloud Composer environment, pointing to the correct Google Cloud Project.
6.  **Airflow DAG Deployment and Scheduling:**
    *   Upload the `dags/r_ausd_v_ta_barrier_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Crucially, update the `schedule` parameter in the DAG definition from `None` to the desired production schedule** (e.g., `"@daily"`, `"0 0 * * *"`, or a specific cron expression).
    *   Review and set appropriate default parameter values (`JOB_KENNUNG_PARAM`, `S_PARAM`, `L_PARAM`) within the DAG or configure them to be dynamically pulled from Airflow Variables or XComs.
7.  **Source Data Availability:**
    *   Ensure that the `ta_barrier` table and any other source tables required by the `k_ausd_v_ta_barrier_sp` (once fully implemented) are present in BigQuery and accessible to the service account.

**Important:** Replace all placeholders (`my_project_id`, `my_dataset_id`) with your actual project and dataset identifiers before deployment.

## 5. Known Gaps & Unresolved References

The following items are identified as gaps or require further follow-up:

*   **Kernel Script (`k_ausd_v_ta_barrier.ksh`) Full Migration:** The `k_ausd_v_ta_barrier_sp` in BigQuery is currently a placeholder. The detailed logic, data sources, transformations, and dependencies of the original `k_ausd_v_ta_barrier.ksh` are critical and must be fully analyzed and migrated. This is the most significant dependency for the complete functionality of the job.
*   **Undocumented Parameter Usage (`-s`, `-l`):** The exact purpose and expected values of the `-s` and `-l` command-line parameters from the original KornShell script are not fully documented in the provided design. Their usage within the `k_ausd_v_ta_barrier.ksh` (or other components) needs to be clarified to ensure the BigQuery stored procedure (`r_ausd_v_ta_barrier_sp`) receives and processes them correctly.
*   **`DWMSG_*` Utility Implementation Details:** While the `DWMSG_*` functions have been migrated to BigQuery Stored Procedures, the full internal logic of their original shell counterparts (e.g., how `DW_EintragsNr` was generated in a multi-user environment, specific persistence mechanisms for status) was assumed based on common patterns. A deeper dive into the original `f_alis_msgerr.ksh` and related scripts might be necessary to ensure complete functional parity.
*   **`$HOME/.dw_init` Content:** The contents of the `$HOME/.dw_init` environment initialization script are unknown. If it contains complex environment variables, paths, or specific configurations crucial for the legacy job, these need to be identified and translated into BigQuery procedure parameters, Airflow variables, or other appropriate cloud configurations.
*   **Error Handling Granularity:** While BigQuery's `EXCEPTION WHEN ERROR THEN` provides robust error handling, the exact nuances of shell `trap` commands (e.g., handling specific signals like `INT`, `TERM`, or custom exit codes) might not be perfectly replicated. For highly sensitive operations, a review of specific error scenarios and their handling might be warranted.

## 6. Validation

To validate the successful migration and functionality of the `r_ausd_v_ta_barrier` job:

1.  **BigQuery Stored Procedure Execution (Manual Test):**
    *   Execute the main wrapper stored procedure directly in BigQuery:
        ```sql
        CALL `my_project_id.my_dataset_id.r_ausd_v_ta_barrier_sp`(
            p_job_kennung => 'TEST_TA_BARRIER_MANUAL',
            p_s => 'test_s_value',
            p_l => 'test_l_value'
        );
        ```
    *   **Passing Criteria:**
        *   The BigQuery job completes without unhandled errors.
        *   Query the `job_log_table` and `job_status_table` to confirm entries for `TEST_TA_BARRIER_MANUAL` are created and the final status is 'SUCCESS'.
        *   (Once `k_ausd_v_ta_barrier_sp` is fully implemented) Verify that the core reconciliation logic has executed correctly by inspecting the target data or reconciliation reports.

2.  **Airflow DAG Execution (Orchestration Test):**
    *   Trigger the `r_ausd_v_ta_barrier_orchestration` DAG manually from the Airflow UI.
    *   **Passing Criteria:**
        *   The Airflow DAG run completes successfully (green status).
        *   The `execute_r_ausd_v_ta_barrier_sp` task completes successfully.
        *   Query the `job_log_table` and `job_status_table` in BigQuery to confirm entries for the job (using the `JOB_KENNUNG_PARAM` defined in the DAG) are created and the final status is 'SUCCESS'.
        *   (Once `k_ausd_v_ta_barrier_sp` is fully implemented) Verify the business outcome as per the manual test.

3.  **Error Scenario Testing:**
    *   Introduce invalid parameters or simulate an error within `k_ausd_v_ta_barrier_sp` (e.g., by raising an explicit error in the placeholder).
    *   **Passing Criteria:**
        *   The BigQuery stored procedure (and Airflow DAG) should gracefully fail.
        *   Error messages should be logged in `job_log_table` with 'ERROR' severity.
        *   The `job_status_table` should show 'FAILED' for the corresponding job.
        *   The Airflow DAG run should show a 'failed' status.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions:**
    *   In the Airflow UI, un-schedule or pause the `r_ausd_v_ta_barrier_orchestration` DAG to prevent any new runs.
    *   If any manual triggers are in place, ensure they are also disabled.
2.  **Revert to Legacy System:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh` script in its legacy scheduling system (e.g., cron).
    *   Verify that the legacy script can execute successfully and produce expected results.
3.  **BigQuery Artifact Cleanup (Optional, based on severity):**
    *   If the migrated BigQuery stored procedures or tables are causing conflicts or are deemed unstable, they can be dropped.
        *   Drop the main stored procedure:
            ```sql
            DROP PROCEDURE IF EXISTS `my_project_id.my_dataset_id.r_ausd_v_ta_barrier_sp`;
            ```
        *   Drop the utility stored procedures (e.g., `DWMSG_ErmittleNr_SP`, `DWMSG_MeldeFehler_SP`, etc.).
        *   Drop the logging and status tables:
            ```sql
            DROP TABLE IF EXISTS `my_project_id.my_dataset_id.job_log_table`;
            DROP TABLE IF EXISTS `my_project_id.my_dataset_id.job_status_table`;
            ```
    *   **Data Integrity Note:** If the `k_ausd_v_ta_barrier_sp` (once implemented) performs data modifications, a data rollback strategy (e.g., restoring from a snapshot, running a reverse ETL job) might be necessary depending on the extent of data corruption or incorrect processing. This should be part of the kernel script's specific rollback plan.
4.  **Airflow DAG Removal (Optional):**
    *   Delete the `dags/r_ausd_v_ta_barrier_dag.py` file from the Cloud Composer DAGs folder.

This procedure ensures a quick return to the previous operational state while allowing for investigation and re-planning of the migration.