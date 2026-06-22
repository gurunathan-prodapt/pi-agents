# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_cntrct_templ.ksh` from its legacy environment to Google Cloud Platform (GCP). The original script served as an orchestration component, handling environment setup, parameter validation, error handling, job registration, and the execution of an associated SQL script (`d_ausd_v_ta_cntrct_templ.sql`) to update the `ta_cntrct_templ` table.

The migrated solution leverages **BigQuery** for data storage and core logic execution, and **Cloud Composer (Apache Airflow)** for workflow orchestration and scheduling.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`d_ausd_v_ta_cntrct_templ.bqsql`**
    *   **Role:** This file contains the core data manipulation logic, translated from the original `d_ausd_v_ta_cntrct_templ.sql` script, into BigQuery Standard SQL. It performs a `TRUNCATE` and `INSERT` operation on the `project.dataset.ta_cntrct_templ` table based on data from `cds_ta_cntrct_template` and `cds_ta_care_description`. This script is embedded directly within the BigQuery Stored Procedure.
*   **`ddl_job_error_log.bqsql`**
    *   **Role:** This DDL script defines the `project.dataset.job_error_log` table in BigQuery. This table is used to capture detailed error messages and context whenever an error occurs during the execution of the BigQuery Stored Procedure.
*   **`ddl_job_table.bqsql`**
    *   **Role:** This DDL script defines the `project.dataset.job_table` in BigQuery. This table serves as the central repository for tracking the status (STARTED, FINISHED, DEACTIVATED, ERROR), start/end times, and records processed for each job execution, replacing the implicit job management of the legacy system.
*   **`r_ausd_v_ta_cntrct_templ.bqsql`**
    *   **Role:** This file contains the BigQuery Stored Procedure `project.dataset.r_ausd_v_ta_cntrct_templ`. It encapsulates the orchestration logic of the original KornShell script, including parameter validation, job status updates in `job_table`, error logging to `job_error_log`, deactivation of older jobs, and the execution of the core data manipulation logic from `d_ausd_v_ta_cntrct_templ.bqsql`.
*   **`k_ausd_v_ta_cntrct_templ_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG for Cloud Composer. Its primary function is to trigger the `project.dataset.r_ausd_v_ta_cntrct_templ` BigQuery Stored Procedure, passing `p_JobKennung` and `p_EintragsNr` as parameters. It acts as the scheduler and orchestrator for the migrated job.

## 3. Key Design Decisions

*   **Migration to BigQuery Stored Procedure for Core Logic and Orchestration:**
    *   **Why:** This approach centralizes the business logic, parameter handling, error management, and job status updates within a single, serverless, and scalable BigQuery component. It leverages BigQuery's native procedural capabilities, reducing the need for external scripting to manage database interactions. This aligns with a "data-centric" processing model on GCP.
    *   **Notable Trade-offs:** This decision required a complete re-implementation of shell script logic (e.g., `getopts`, `if` conditions, temporary file handling) using BigQuery SQL's procedural extensions (`DECLARE`, `IF`, `EXCEPTION WHEN ERROR`, `SIGNAL SQLSTATE`). This can increase the complexity of the BigQuery Stored Procedure itself compared to a simple SQL script.
*   **Cloud Composer for Scheduling and External Triggering:**
    *   **Why:** Cloud Composer (managed Apache Airflow) provides robust scheduling, monitoring, and workflow management capabilities, replacing the ad-hoc scheduling and execution of the legacy KornShell script. It offers a standardized way to trigger BigQuery operations and integrate with other GCP services.
    *   **Notable Trade-offs:** Introduces a new dependency on a managed Airflow environment, requiring familiarity with DAG development and Airflow concepts.
*   **Elimination of Temporary Files and Shell Utilities:**
    *   **Why:** The legacy script's reliance on temporary files for inter-process communication (e.g., record counts) and various sourced utility shell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.) was replaced by BigQuery's internal variables (`DECLARE`), native SQL functions, and direct error logging tables. This simplifies the architecture, removes file system dependencies, and improves performance by keeping operations within BigQuery.
    *   **Notable Trade-offs:** Required careful analysis and re-implementation of the functionality provided by these legacy components using BigQuery SQL constructs.
*   **Dedicated BigQuery Tables for Job Management:**
    *   **Why:** Explicit `project.dataset.job_table` and `project.dataset.job_error_log` tables were created in BigQuery to manage job status and error reporting. This provides a structured, queryable, and centralized mechanism for monitoring job executions, replacing potentially disparate logging and status tracking in the legacy system.

## 4. Manual Steps Before Go-Live

Before the migrated job can be deployed and run, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure the target GCP `project` and BigQuery `dataset` (e.g., `project.dataset`) exist. Replace all placeholders in the generated code with the actual project and dataset IDs.
2.  **BigQuery Table Creation:**
    *   Execute the DDL scripts to create the necessary control tables:
        *   `ddl_job_table.bqsql`
        *   `ddl_job_error_log.bqsql`
    *   Ensure the target data tables exist and are migrated to BigQuery:
        *   `project.dataset.ta_cntrct_templ` (target table)
        *   `project.dataset.cds_ta_cntrct_template` (source table)
        *   `project.dataset.cds_ta_care_description` (source table)
        *   `project.dataset.dwtk_meldungen` (source table for `v_datum_bq` calculation)
    *   Verify that the schema of these tables matches the expectations of the BigQuery Stored Procedure.
3.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `r_ausd_v_ta_cntrct_templ.bqsql` script in BigQuery to create or replace the stored procedure.
4.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   **IAM/Permissions:** The Cloud Composer Service Account (typically `service-<project-number>@cloudcomposer.gserviceaccount.com`) must have sufficient permissions to:
        *   Execute BigQuery Stored Procedures (`bigquery.routines.call`).
        *   Read and write data to BigQuery tables (`bigquery.dataEditor` or specific `bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.insertData`, `bigquery.tables.truncate` on `project.dataset.*`).
    *   **Airflow Connection:** Verify or create an Airflow connection named `google_cloud_default` (or the name specified in the DAG) that points to your GCP project. This is typically pre-configured in Cloud Composer.
5.  **Cloud Composer DAG Deployment:**
    *   Upload the `k_ausd_v_ta_cntrct_templ_dag.py` file to the DAGs folder of your Cloud Composer environment.
6.  **Scheduling:**
    *   The DAG is currently configured with `schedule=None`, indicating it's designed for manual or external triggering. If automatic scheduling is required, update the `schedule` parameter in `k_ausd_v_ta_cntrct_templ_dag.py` (e.g., `schedule="@daily"`).

## 5. Known Gaps & Unresolved References

*   **Missing Source Code for Legacy Dependencies:** The migration design and generated code for `d_ausd_v_ta_cntrct_templ.bqsql` were based on assumptions about the original `d_ausd_v_ta_cntrct_templ.sql` and utility scripts. A thorough review against the actual source code of `d_ausd_v_ta_cntrct_templ.sql` and the logic within `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` is critical to ensure functional equivalence.
*   **Parent Script `r_ausd_vertrag.ksh` Migration:** This migration focuses solely on `k_ausd_v_ta_cntrct_templ.ksh`. The calling script, `r_ausd_vertrag.ksh`, will require its own migration design to ensure end-to-end functionality in the new GCP environment. The current DAG is designed to be triggered, likely by the migrated version of `r_ausd_vertrag.ksh`.
*   **`starteSQLSkript` Functionality:** The exact behavior of the `starteSQLSkript` function (e.g., specific error handling, connection pooling, parameter binding) from `h_alis_sqlplus.ksh` was not fully detailed. The BigQuery Stored Procedure's error handling and parameter passing are designed to be robust, but a direct comparison to the original function's nuances is a follow-up item.
*   **`dwtk_meldungen` Table:** The `v_datum_bq` calculation relies on a migrated `project.dataset.dwtk_meldungen` table. The existence and schema of this table, and the accuracy of the `MAX(m.timecreated)` logic, need to be confirmed.
*   **Placeholder Values:** The generated code uses `project` and `dataset` as placeholders. These must be replaced with the actual GCP Project ID and BigQuery Dataset ID before deployment.

## 6. Validation

To validate the successful migration and functionality of the new solution:

1.  **Trigger the DAG:**
    *   Navigate to the Cloud Composer UI.
    *   Find the `k_ausd_v_ta_cntrct_templ_dag` DAG.
    *   Manually trigger the DAG, providing test values for `job_kennung` and `eintragsnr` in the trigger UI (e.g., `{"job_kennung": "TEST_CONTRACT_TEMPLATE", "eintragsnr": 999}`).
2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. The `call_r_ausd_v_ta_cntrct_templ_sp` task should complete successfully.
3.  **Check BigQuery Job Table:**
    *   Query `project.dataset.job_table` for the `job_kennung` and `eintragsnr` used in the test run.
    *   **Passing Criteria:** The entry should show `status = 'FINISHED'`, `end_time` populated, and `records_processed` reflecting the number of rows inserted into `ta_cntrct_templ`.
4.  **Check BigQuery Error Log:**
    *   Query `project.dataset.job_error_log`.
    *   **Passing Criteria:** There should be *no* new entries related to the test run. If entries exist, investigate the `error_message` and `sql_state`.
5.  **Verify Target Data:**
    *   Query `project.dataset.ta_cntrct_templ` to confirm that data has been inserted correctly and matches expectations based on the source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`) and the `v_datum_bq` logic.
    *   The number of rows in `ta_cntrct_templ` should match the `records_processed` value in `job_table`.
6.  **Edge Case Testing:**
    *   Test with missing parameters (e.g., trigger DAG without `job_kennung` or `eintragsnr` in params) to ensure the validation logic in the Stored Procedure correctly logs errors and signals SQLSTATE.
    *   Test with data that would cause the core SQL logic to fail (if possible) to verify error handling.

## 7. Rollback Procedure

In case of issues or critical failures after go-live, the following rollback procedure can be executed:

1.  **Deactivate Cloud Composer DAG:**
    *   In the Cloud Composer UI, toggle off the `k_ausd_v_ta_cntrct_templ_dag` to prevent further executions.
    *   Alternatively, delete the `k_ausd_v_ta_cntrct_templ_dag.py` file from the DAGs folder.
2.  **Revert BigQuery Stored Procedure (Optional but Recommended):**
    *   If the BigQuery Stored Procedure `r_ausd_v_ta_cntrct_templ` introduced breaking changes or is found to be faulty, it can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.r_ausd_v_ta_cntrct_templ;
        ```
    *   If a previous version of the stored procedure existed, it can be redeployed.
3.  **Revert BigQuery Tables (If DDL Changes were Made):**
    *   If the DDL for `job_table` or `job_error_log` caused issues, they can be dropped and recreated with their previous schema (if applicable).
    *   **Data Restoration for `ta_cntrct_templ`:** If the data in `project.dataset.ta_cntrct_templ` was corrupted or incorrectly updated by the migrated job, restore the table from a recent backup or snapshot. BigQuery's time travel feature can also be used to query data from a previous point in time.
4.  **Re-enable Legacy System:**
    *   Ensure the original `k_ausd_v_ta_cntrct_templ.ksh` script and its dependencies are fully operational in the legacy environment.
    *   Re-enable any scheduling or triggering mechanisms for the legacy script (e.g., cron jobs, parent scripts like `r_ausd_vertrag.ksh`).
5.  **Verify Legacy Functionality:**
    *   Run the original `k_ausd_v_ta_cntrct_templ.ksh` script and verify that it executes successfully and produces the expected results in the legacy database.