# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` and its associated core SQL logic (`d_ausd_bp_ta_bpr_apn.sql`). The migration re-platforms this job from a legacy Unix/Oracle environment to Google Cloud Platform (GCP).

The target platform leverages:
*   **Google BigQuery** for data storage, transformation, and procedural logic (replacing Oracle SQL and parts of the KornShell script).
*   **Google Cloud Composer (Apache Airflow)** for workflow orchestration, scheduling, and parameter management (replacing the KornShell script's control flow).

## 2. Generated Artifacts

The migration process has generated the following files:

*   **`sql/ddl/prod_dw_isrpt.PoolBasisprodukt.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the target table `PoolBasisprodukt` in the `prod_dw_isrpt` dataset. This table will store the final processed data, replacing the output of the original Oracle SQL script.
*   **`sql/ddl/prod_dw_logs.error_log.sql`**
    *   **Role:** BigQuery DDL script to create a dedicated `error_log` table in the `prod_dw_logs` dataset. This table will capture structured error messages and details, replacing the basic error handling and logging mechanisms of the original KornShell script.
*   **`sql/ddl/prod_dw_logs.job_tracking.sql`**
    *   **Role:** BigQuery DDL script to create a `job_tracking` table in the `prod_dw_logs` dataset. This table will store metadata about job executions, including status, record counts, and parameters, replacing any optional or commented-out job tracking functionality in the original script.
*   **`sql/procedures/prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp.sql`**
    *   **Role:** BigQuery Stored Procedure that encapsulates the core data transformation logic originally found in `d_ausd_bp_ta_bpr_apn.sql`. It is responsible for reading from source tables and populating `prod_dw_isrpt.PoolBasisprodukt`.
*   **`sql/procedures/prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp.sql`**
    *   **Role:** BigQuery Stored Procedure that replaces the orchestration and control flow of the original `k_ausd_bp_ta_bpr_apn.ksh` script. It handles parameter validation, date calculations, calls the `d_ausd_bp_ta_bpr_apn_sp` procedure, performs record counting, and logs job status.
*   **`dags/k_ausd_bp_ta_bpr_apn_dag.py`**
    *   **Role:** An Apache Airflow DAG (Python script) that serves as the entry point for the migrated job. It defines the workflow, accepts input parameters, and triggers the execution of the `prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp` BigQuery Stored Procedure.

## 3. Key Design Decisions

The following key design decisions were made during this migration:

*   **BigQuery-Centric Logic:** Both the orchestration logic (from `k_ausd_bp_ta_bpr_apn.ksh`) and the core data transformation logic (from `d_ausd_bp_ta_bpr_apn.sql`) have been consolidated into BigQuery Stored Procedures. This centralizes the business logic within the data warehouse, leveraging BigQuery's performance and scalability, and simplifies the Airflow DAG to primarily act as a trigger and parameter passer.
*   **Cloud Composer for Orchestration:** Apache Airflow, managed by Cloud Composer, was chosen to replace the KornShell script's scheduling and execution control. Airflow provides robust scheduling, dependency management, parameterization, and monitoring capabilities native to GCP.
*   **Structured Error Handling:** Instead of ad-hoc shell script error codes and messages, a dedicated `prod_dw_logs.error_log` BigQuery table is used for structured error logging. BigQuery's `SIGNAL SQLSTATE` mechanism is employed to raise and propagate errors, allowing Airflow to catch and react to failures.
*   **Native BigQuery Functionality:** All date calculations, parameter validation, and utility functions previously handled by helper KornShell scripts (`h_alis_date.ksh`, `gestern.ksh`, etc.) are now implemented using BigQuery's native SQL functions and procedural language constructs. This eliminates external script dependencies and improves maintainability.
*   **Explicit Parameterization:** The Airflow DAG and BigQuery Stored Procedures are designed to accept explicit parameters (e.g., `job_kennung`, `stichtag`). This enhances flexibility, testability, and reusability compared to parsing command-line arguments in a shell script.
*   **Direct Record Counting:** The record count, previously obtained from a temporary file, is now directly calculated within the BigQuery Stored Procedure using `SELECT COUNT(*)`, ensuring accuracy and efficiency.
*   **Optional Job Tracking:** A `prod_dw_logs.job_tracking` table has been introduced to provide structured job execution metadata, even though the original script's job tracking was commented out. This provides a foundation for future auditing and monitoring.

**Notable Trade-offs:**

*   **Increased BigQuery Procedural Complexity:** While centralizing logic in BigQuery offers benefits, it means more complex procedural SQL code within BigQuery Stored Procedures compared to simpler shell script logic. This might require a different skill set for maintenance.
*   **Initial Setup Overhead:** Deploying and configuring Cloud Composer, BigQuery datasets, and IAM roles involves more initial setup than simply copying a KornShell script.
*   **Dependency on BigQuery:** The solution is now tightly coupled with BigQuery, making it less portable to other data warehouse platforms.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Setup:**
    *   Ensure a GCP project is available and configured for BigQuery and Cloud Composer.
    *   Replace `your-gcp-project-id` in `dags/k_ausd_bp_ta_bpr_apn_dag.py` with the actual GCP Project ID, or configure an Airflow Variable named `gcp_project_id`.

2.  **BigQuery Dataset Creation:**
    *   Create the following BigQuery datasets in your GCP project:
        *   `prod_dw_isrpt` (for target tables and procedures)
        *   `prod_dw_logs` (for logging and tracking tables)
    *   Ensure appropriate regionality and access controls are set for these datasets.

3.  **BigQuery Table Creation:**
    *   Execute the following DDL scripts in BigQuery to create the necessary tables:
        *   `sql/ddl/prod_dw_isrpt.PoolBasisprodukt.sql`
        *   `sql/ddl/prod_dw_logs.error_log.sql`
        *   `sql/ddl/prod_dw_logs.job_tracking.sql`

4.  **BigQuery Stored Procedure Deployment:**
    *   Execute the following SQL scripts in BigQuery to create the stored procedures:
        *   `sql/procedures/prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp.sql`
        *   `sql/procedures/prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp.sql`

5.  **Source Data Migration & Accessibility:**
    *   Ensure that all source tables referenced in `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp` (e.g., `prod_dw_source.dwtk_meldungen`, `prod_dw_source.sof_ta_bpr_instance`, `prod_dw_source.sof_ta_apn_carmen`) are migrated to BigQuery and accessible from the `prod_dw_isrpt` dataset. The placeholder names in the generated code must be updated to reflect the actual migrated source table names and datasets.

6.  **Cloud Composer (Airflow) Setup:**
    *   **DAG Deployment:** Upload `dags/k_ausd_bp_ta_bpr_apn_dag.py` to your Cloud Composer environment's DAGs folder.
    *   **Airflow Connection:** Verify that the `google_cloud_default` connection is correctly configured in Airflow, providing the necessary credentials for BigQuery access.
    *   **Scheduling:** Configure the desired schedule for the `k_ausd_bp_ta_bpr_apn_dag` in the Airflow UI. The current DAG has `schedule=None`, meaning it will only run manually or via external trigger.

7.  **IAM / Permissions:**
    *   The service account used by your Cloud Composer environment (or the user triggering the DAG) must have the following BigQuery roles for the relevant datasets:
        *   `BigQuery Data Editor` on `prod_dw_isrpt` (to write to `PoolBasisprodukt` and create/call procedures).
        *   `BigQuery Data Editor` on `prod_dw_logs` (to write to `error_log` and `job_tracking`).
        *   `BigQuery Data Viewer` on any source datasets (e.g., `prod_dw_source`).
        *   `BigQuery Job User` (to run BigQuery jobs).

## 5. Known Gaps & Unresolved References

The following items have been identified as potential gaps or require further attention:

*   **Detailed `d_ausd_bp_ta_bpr_apn.sql` Conversion:** The provided `d_ausd_bp_ta_bpr_apn_sp.sql` is a basic conversion. A thorough review of the original `d_ausd_bp_ta_bpr_apn.sql` is required to ensure all Oracle-specific syntax, functions, and complex logic (e.g., specific joins, subqueries, data type conversions) are accurately translated to BigQuery Standard SQL. The placeholder source table names (`prod_dw_source.dwtk_meldungen`, etc.) must be updated.
*   **Commented-out Code Review:** The original KornShell script contained commented-out sections (e.g., FOS job management, `sed`/`sort`/`join` operations). This migration assumes these sections were inactive and did not migrate them. A confirmation is needed if any of this dormant logic should be revived and incorporated.
*   **Error Code Semantics:** The original script might have relied on specific `ErrNr` values for external system integration or specific error handling. While a generic `error_log` table and `SIGNAL SQLSTATE` are implemented, a mapping of original error codes to new BigQuery error messages or custom error numbers might be necessary if external systems depend on these specific codes.
*   **Parameter Validation Granularity:** The `pruefeParameterGesetzt` function in the original script might have had very specific validation rules beyond simple null/empty checks. A detailed comparison is needed to ensure the BigQuery SP's validation logic fully replicates the original behavior.
*   **`gcp_project_id` Placeholder:** The `project_id` in the Airflow DAG (`dags/k_ausd_bp_ta_bpr_apn_dag.py`) uses a placeholder `{{ var.value.get('gcp_project_id', 'your-gcp-project-id') }}`. This needs to be configured either as an Airflow Variable named `gcp_project_id` or hardcoded with the actual project ID.

## 6. Validation

To validate the successful migration and functionality of the job, perform the following steps:

1.  **Unit Testing BigQuery Stored Procedures:**
    *   Execute `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp` directly in BigQuery with sample input parameters.
    *   Execute `prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp` directly in BigQuery with various valid and invalid input parameters (e.g., missing `Stichtag`, invalid `Stichtag` format).
    *   **Passing Criteria:**
        *   Procedures complete without syntax errors.
        *   `prod_dw_isrpt.PoolBasisprodukt` is populated correctly with expected data.
        *   Error scenarios correctly trigger `SIGNAL SQLSTATE` and log entries to `prod_dw_logs.error_log`.
        *   `prod_dw_logs.job_tracking` is updated correctly for successful runs.

2.  **Airflow DAG Execution:**
    *   Trigger the `k_ausd_bp_ta_bpr_apn_dag` manually from the Airflow UI with valid parameters.
    *   Trigger the DAG with parameters designed to cause validation errors (e.g., empty `stichtag`).
    *   **Passing Criteria:**
        *   The Airflow DAG completes successfully for valid inputs.
        *   The `call_main_bigquery_stored_procedure` task shows success in Airflow logs.
        *   For invalid inputs, the `call_main_bigquery_stored_procedure` task fails, and the error is visible in Airflow logs and recorded in `prod_dw_logs.error_log`.

3.  **Data Validation:**
    *   After a successful DAG run, query `prod_dw_isrpt.PoolBasisprodukt`.
    *   Compare the record count and a sample of the data in `PoolBasisprodukt` with the output generated by the original KornShell script for the same input parameters.
    *   **Passing Criteria:**
        *   The total number of records in `prod_dw_isrpt.PoolBasisprodukt` matches the expected count from the legacy system.
        *   A sample of the data (e.g., `CNTRCT_ID`, `BPR_ID`, `ACCESS_POINT_NAME`) matches the legacy output.

4.  **Logging and Tracking Verification:**
    *   Check `prod_dw_logs.error_log` for any unexpected errors.
    *   Check `prod_dw_logs.job_tracking` to ensure a `COMPLETED` entry exists for successful runs, with correct `record_count` and `start_date`.
    *   **Passing Criteria:**
        *   No unexpected errors are present in `prod_dw_logs.error_log`.
        *   Accurate job tracking entries are created for each run.

## 7. Rollback Procedure

In case of issues or critical failures after go-live, the following rollback procedure can be executed:

1.  **Deactivate Airflow DAG:**
    *   In the Cloud Composer Airflow UI, toggle off the `k_ausd_bp_ta_bpr_apn_dag` to prevent further executions. If necessary, delete the DAG file from the DAGs folder.

2.  **Revert BigQuery Stored Procedures:**
    *   If a previous version of the BigQuery Stored Procedures existed, redeploy them. Otherwise, the current procedures can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp;
        DROP PROCEDURE IF EXISTS prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp;
        ```

3.  **Revert BigQuery Tables (Optional/Conditional):**
    *   **`PoolBasisprodukt`:** If the data in `prod_dw_isrpt.PoolBasisprodukt` is critical and cannot be easily regenerated, consider having a snapshot or backup strategy in place before migration. Otherwise, the table can be truncated or dropped if its data is not needed or can be recreated by the original process.
        ```sql
        TRUNCATE TABLE prod_dw_isrpt.PoolBasisprodukt;
        -- OR DROP TABLE IF EXISTS prod_dw_isrpt.PoolBasisprodukt;
        ```
    *   **`error_log` and `job_tracking`:** These tables are for logging and tracking. They can be left as is, or truncated if a clean slate is desired.
        ```sql
        TRUNCATE TABLE prod_dw_logs.error_log;
        TRUNCATE TABLE prod_dw_logs.job_tracking;
        ```

4.  **Resume Original Process:**
    *   Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` job in its legacy environment. Ensure all necessary configurations and dependencies for the original script are restored.