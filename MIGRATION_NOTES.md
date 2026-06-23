# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell wrapper script `r_ausd_v_ta_cntrct_crs2.ksh`, which orchestrates a data reconciliation process for the `ta_cntrct_crs2` table. The migration targets Google Cloud Platform (GCP), leveraging BigQuery for data processing and storage, and Cloud Composer (Apache Airflow) for workflow orchestration. The original script's operational framework, including parameter handling, logging, and error management, has been re-engineered into BigQuery Stored Procedures and an Airflow DAG. The core data reconciliation logic, originally in `k_ausd_v_ta_cntrct_crs2.ksh`, is represented by a placeholder BigQuery Stored Procedure, awaiting detailed implementation.

## 2. Generated artifacts

The migration process has generated the following artifacts:

*   **`sql/audit/create_audit_tables.sql`**
    *   **Role:** This SQL script defines and creates the necessary BigQuery tables for centralized job control, logging, and error tracking. These tables (`job_control`, `job_log`, `job_error_log`) replace the file-based logging and status tracking of the original KornShell script.
*   **`sql/stored_procedures/k_ausd_v_ta_cntrct_crs2_sp.sql`**
    *   **Role:** This BigQuery Stored Procedure serves as a placeholder for the core data reconciliation logic. It is intended to encapsulate the business transformations and data manipulation that were originally present in `k_ausd_v_ta_cntrct_crs2.ksh`. Its current implementation includes basic logging entries but requires full development based on the original script's detailed logic.
*   **`sql/stored_procedures/vertraegsdatenabgleich_sp.sql`**
    *   **Role:** This BigQuery Stored Procedure is the direct migration of the `r_ausd_v_ta_cntrct_crs2.ksh` wrapper script. It handles input parameters, initializes logging, manages job status in the `job_control` table, implements error trapping using BigQuery Scripting's `EXCEPTION` blocks, and orchestrates the call to the core processing logic (`k_ausd_v_ta_cntrct_crs2`).
*   **`dags/vertraegsdatenabgleich_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG for Cloud Composer. It is responsible for scheduling and orchestrating the execution of the `Vertragsdatenabgleich` BigQuery Stored Procedure. It replaces the cron-based or manual execution of the original KornShell script, providing robust scheduling, monitoring, and integration with GCP services.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures for Logic:** The operational wrapper logic (from `r_ausd_v_ta_cntrct_crs2.ksh`) and the core processing logic (from `k_ausd_v_ta_cntrct_crs2.ksh`) are migrated to BigQuery Stored Procedures. This decision centralizes data processing within BigQuery, leveraging its scalability and performance for SQL-based transformations. It replaces shell scripting's control flow with BigQuery's native SQL scripting capabilities, including variable declarations, conditional logic, and robust error handling (`BEGIN...EXCEPTION...END`).
*   **Cloud Composer (Airflow) for Orchestration:** Cloud Composer was chosen to replace the legacy scheduling mechanism (likely cron or manual execution). Airflow provides a managed, scalable, and feature-rich platform for defining, scheduling, and monitoring complex data workflows. It offers better visibility, retry mechanisms, and integration with other GCP services compared to shell scripts.
*   **Centralized BigQuery Tables for Logging and Auditing:** The original script's file-based logging and status tracking (`DWMSG_*` functions) are replaced by dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`). This provides a centralized, queryable, and scalable solution for auditing job executions, tracking status, and analyzing errors, significantly improving observability.
*   **Separation of Wrapper and Core Logic:** The design maintains the separation between the wrapper (orchestration, logging, error handling) and the core business logic. This modularity allows for independent development, testing, and maintenance of the core data reconciliation logic within `k_ausd_v_ta_cntrct_crs2_sp.sql`, while `vertraegsdatenabgleich_sp.sql` focuses on the operational framework.
*   **Parameter Handling Translation:** The `getopts`-based command-line parameter parsing in the original script is translated into explicit input parameters for the BigQuery Stored Procedure and subsequently managed by the Airflow DAG, ensuring clear input definition and validation.
*   **Error Handling Modernization:** The `trap` mechanisms in KornShell are replaced by BigQuery Scripting's `BEGIN...EXCEPTION...END` blocks, providing structured and robust error handling directly within the SQL context, allowing for precise error logging and status updates.

## 4. Manual steps before go-live

Before deploying and running the migrated job, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a GCP project (`your_gcp_project_id`) is provisioned.
    *   Create the target BigQuery dataset (`your_bigquery_dataset_id`) where the audit tables and stored procedures will reside.
2.  **IAM Permissions Configuration:**
    *   **Cloud Composer Service Account:** Grant the Cloud Composer environment's service account the necessary BigQuery roles (e.g., `BigQuery Data Editor` or custom roles) on `your_gcp_project_id.your_bigquery_dataset_id` to allow it to create tables, stored procedures, and insert/update data.
    *   **Deployment User/Service Account:** Ensure the user or service account deploying the DAG and BigQuery objects has appropriate permissions (e.g., `Composer User`, `BigQuery Admin` or `BigQuery Data Editor` for deployment).
3.  **Deploy Audit Tables:**
    *   Execute the `sql/audit/create_audit_tables.sql` script in BigQuery to create the `job_control`, `job_log`, and `job_error_log` tables.
4.  **Implement Core Logic:**
    *   **CRITICAL:** Fully implement the business logic within `sql/stored_procedures/k_ausd_v_ta_cntrct_crs2_sp.sql`. This placeholder currently only contains basic logging and *does not* perform any data reconciliation. Detailed analysis of the original `k_ausd_v_ta_cntrct_crs2.ksh` is required for this step.
5.  **Deploy Stored Procedures:**
    *   Execute `sql/stored_procedures/k_ausd_v_ta_cntrct_crs2_sp.sql` (after implementation) and `sql/stored_procedures/vertraegsdatenabgleich_sp.sql` in BigQuery to create or replace the stored procedures.
6.  **Update DAG Configuration:**
    *   In `dags/vertraegsdatenabgleich_dag.py`:
        *   Replace `PROJECT_ID = "your-gcp-project-id"` with your actual GCP project ID.
        *   Replace `DATASET_ID = "your_bigquery_dataset_id"` with your actual BigQuery dataset ID.
        *   Replace `LOCATION = "us-central1"` with the correct BigQuery dataset location.
        *   Update `schedule=None` to the desired Airflow schedule (e.g., `"@daily"`, `"0 0 * * *"`, or a specific cron expression).
        *   Replace `'default_s_value'` and `'default_l_value'` with actual parameter values or Airflow variables/macros if these parameters are dynamic.
7.  **Deploy Airflow DAG:**
    *   Upload `dags/vertraegsdatenabgleich_dag.py` to your Cloud Composer environment's DAGs folder.

## 5. Known gaps & unresolved references

*   **Core Logic Implementation (B4 Item):** The most significant gap is the placeholder nature of `sql/stored_procedures/k_ausd_v_ta_cntrct_crs2_sp.sql`. The actual data reconciliation logic from the original `k_ausd_v_ta_cntrct_crs2.ksh` has not been migrated and *must* be fully developed and implemented. This is a critical follow-up item.
*   **Utility Script Functionality:** The full extent of functionality provided by the original sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) needs further analysis. While basic logging and date functions are covered, any complex or unique features from these scripts might require additional BigQuery native functions, UDFs, or Python helper functions within the Airflow DAG.
*   **Purpose of `-s` and `-l` Parameters:** The original script declares `-s` and `-l` parameters via `getopts` but their usage is not explicit in the provided source. Their intended purpose and impact on the job's execution need to be clarified to ensure correct parameter passing and handling in the migrated solution. Currently, they are passed as default string values.
*   **Robust `entry_number` Generation:** The current implementation for `v_entry_number` in `vertraegsdatenabgleich_sp.sql` uses `COALESCE(MAX(entry_number), 0) + 1`. While functional, for high-concurrency scenarios, this might lead to race conditions or non-unique `entry_number` values. A more robust sequence generation strategy (e.g., using a dedicated sequence table with atomic updates or a UUID-based approach) might be required depending on the job's execution frequency and concurrency.
*   **`sysdate_info` Format:** The original script used `date +%d%m%Y`. The migrated `sysdate_info` in `job_control` is stored as a `DATE` type. If downstream systems or reports specifically require the `DDMMYYYY` string format, an explicit `FORMAT_DATE('%d%m%Y', sysdate_info)` conversion will be necessary when querying the `job_control` table.

## 6. Validation

Validation should cover unit testing of individual components and integration testing of the entire workflow.

### How to Run Tests

1.  **BigQuery Audit Tables:**
    *   Execute `sql/audit/create_audit_tables.sql` in a BigQuery query editor. Verify that the three tables (`job_control`, `job_log`, `job_error_log`) are created in the specified dataset.
2.  **BigQuery Stored Procedures:**
    *   **`k_ausd_v_ta_cntrct_crs2_sp.sql` (after implementation):**
        *   Execute the stored procedure directly in BigQuery with test parameters:
            ```sql
            CALL `your_gcp_project_id.your_bigquery_dataset_id.k_ausd_v_ta_cntrct_crs2`('TEST_JOB', 1);
            ```
        *   Verify that the expected data transformations occur and that log entries are written to `job_log`.
    *   **`vertraegsdatenabgleich_sp.sql`:**
        *   Execute the stored procedure directly in BigQuery with various parameter combinations:
            *   Help flag: `CALL `your_gcp_project_id.your_bigquery_dataset_id.Vertragsdatenabgleich`(TRUE, NULL, NULL);` (should return help message).
            *   Successful run: `CALL `your_gcp_project_id.your_bigquery_dataset_id.Vertragsdatenabgleich`(FALSE, 'test_s', 'test_l');`
            *   Error scenario (e.g., by temporarily introducing an error in `k_ausd_v_ta_cntrct_crs2_sp`): `CALL `your_gcp_project_id.your_bigquery_dataset_id.Vertragsdatenabgleich`(FALSE, 'error_s', 'error_l');`
        *   Query `job_control`, `job_log`, and `job_error_log` tables to verify correct entries for each scenario.
3.  **Cloud Composer DAG:**
    *   Ensure the `dags/vertraegsdatenabgleich_dag.py` is deployed to your Cloud Composer environment.
    *   Access the Airflow UI.
    *   Manually trigger the `vertraegsdatenabgleich_daily_job` DAG.
    *   Monitor the DAG run in the Airflow UI for success or failure.
    *   After the DAG completes, query the BigQuery audit tables (`job_control`, `job_log`, `job_error_log`) to confirm the job's status, logs, and any errors.
    *   Verify the state of the `ta_cntrct_crs2` table and any other tables modified by the core logic.

### What "Passing" Means

A successful migration and validation means:

*   The `vertraegsdatenabgleich_daily_job` DAG in Cloud Composer completes successfully without any Airflow task failures.
*   For successful runs, the `job_control` table shows an entry with `status = 'OK'` and appropriate `start_timestamp`, `end_timestamp`, and `parameters`.
*   The `job_log` table contains expected informational messages corresponding to the job's execution flow.
*   For intentional error scenarios, the `job_control` table shows an entry with `status = 'ERROR'`, and the `job_error_log` table contains detailed error messages and stack traces.
*   The `ta_cntrct_crs2` table (and any other tables modified by `k_ausd_v_ta_cntrct_crs2_sp`) reflects the correct data reconciliation results as per the business requirements defined in the original `k_ausd_v_ta_cntrct_crs2.ksh` script.
*   All parameters (`p_s`, `p_l`) are correctly passed and reflected in the `job_control.parameters` JSON.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, follow these steps to roll back to the original system:

1.  **Pause New Workflow:**
    *   Immediately pause the `vertraegsdatenabgleich_daily_job` DAG in the Cloud Composer Airflow UI to prevent further executions of the migrated job.
2.  **Revert Code (if necessary):**
    *   If the issue is due to a recent code change, revert the `dags/vertraegsdatenabgleich_dag.py` to a previous stable version in Cloud Composer.
    *   Similarly, revert `sql/stored_procedures/vertraegsdatenabgleich_sp.sql` and `sql/stored_procedures/k_ausd_v_ta_cntrct_crs2_sp.sql` to their previous versions or drop them if they were newly introduced and causing issues.
3.  **Data Restoration (if data was modified):**
    *   If the migrated job has modified data in `ta_cntrct_crs2` or other critical tables, use BigQuery's Time Travel feature to restore these tables to a state prior to the problematic execution. This requires knowing the exact timestamp or job ID of the last known good state.
    *   Example: `CREATE OR REPLACE TABLE `project.dataset.table` AS SELECT * FROM `project.dataset.table` FOR SYSTEM_TIME AS OF 'YYYY-MM-DD HH:MM:SS UTC';`
4.  **Re-enable Original System:**
    *   Re-enable the original `r_ausd_v_ta_cntrct_crs2.ksh` script in its legacy environment (e.g., re-enable its cron job or scheduled task).
5.  **Monitor Original System:**
    *   Verify that the original system is functioning as expected and processing data correctly.
6.  **Root Cause Analysis:**
    *   Thoroughly investigate the cause of the failure in the migrated system using BigQuery logs (`job_log`, `job_error_log`), Airflow logs, and other GCP monitoring tools before attempting re-deployment.