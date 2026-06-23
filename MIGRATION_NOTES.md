# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh` and its core SQL dependency `d_ausd_v_ta_discount.sql`.

The original script served as an orchestration and control component, responsible for parameter parsing, environment setup, executing a core SQL script to process data into the `ta_discount` table, managing job status, and handling error logging.

The migration target is Google BigQuery. The functionality of the KornShell script has been refactored into a BigQuery Stored Procedure, with core data processing logic translated to BigQuery SQL. Job control and logging mechanisms have been replaced with dedicated BigQuery tables. Optional external orchestration is provided via a Cloud Composer (Airflow) DAG.

## 2. Generated artifacts

The following files were generated as part of this migration:

*   **`ddl/ta_discount.ddl`**
    *   **Role:** Defines the BigQuery schema for the `ta_discount` table. This table is the primary data target for the migrated process, replacing the legacy RDBMS table.
*   **`ddl/job_control_table.ddl`**
    *   **Role:** Defines the BigQuery schema for the `job_control` table. This table is used to track the status and lifecycle of job executions, replacing the legacy job management mechanisms and temporary file usage.
*   **`ddl/job_log_table.ddl`**
    *   **Role:** Defines the BigQuery schema for the `job_log` table. This table captures detailed logs, warnings, and errors during job execution, replacing the legacy shell-based error logging and reporting.
*   **`sql/d_ausd_v_ta_discount_bq.sql`**
    *   **Role:** Contains the BigQuery SQL translation of the core data processing logic originally found in `d_ausd_v_ta_discount.sql`. This SQL is embedded and executed within the `r_ausd_vertrag_control` BigQuery Stored Procedure.
*   **`stored_procedures/r_ausd_vertrag_control.bqsql`**
    *   **Role:** This BigQuery Stored Procedure is the central component of the migration. It encapsulates the entire logic of the original `k_ausd_v_ta_discount.ksh` script, including parameter validation, job control, execution of the translated core SQL, error handling, and record count capture.
*   **`dags/airflow_dag_k_ausd_v_ta_discount.py`**
    *   **Role:** An optional Cloud Composer (Airflow) DAG designed to schedule and invoke the `r_ausd_vertrag_control` BigQuery Stored Procedure, passing necessary parameters. This replaces the legacy scheduler.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **BigQuery Stored Procedure for Orchestration**: The entire control flow, parameter handling, and error management logic of the original KornShell script were refactored into a BigQuery Stored Procedure (`r_ausd_vertrag_control`). This centralizes the job's logic within BigQuery, leveraging its native scripting capabilities and reducing external dependencies.
*   **BigQuery Tables for Job Control and Logging**: Legacy job tracking mechanisms (likely RDBMS tables and temporary files) and error logging were replaced with dedicated BigQuery tables (`job_control` and `job_log`). This provides a scalable, queryable, and BigQuery-native audit trail for job executions.
*   **Direct SQL Translation and Embedding**: The core data processing logic from `d_ausd_v_ta_discount.sql` was translated into BigQuery SQL and embedded directly within the `r_ausd_vertrag_control` stored procedure. This simplifies deployment and execution by keeping the processing logic close to the data within BigQuery.
*   **Elimination of Temporary Files**: The original script's reliance on temporary files for capturing processed record counts was replaced by BigQuery scripting variables, streamlining the process and removing file system dependencies.
*   **Cloud Composer (Airflow) for External Scheduling (Optional)**: For robust, cloud-native scheduling and workflow management, an Airflow DAG was provided. This offers a modern, scalable alternative to legacy schedulers and facilitates parameter passing.
*   **Trade-offs**:
    *   **Embedding SQL**: While embedding the core SQL within the stored procedure simplifies deployment and execution, it means the SQL logic is not a standalone file. This might slightly increase maintenance complexity if the SQL needs frequent independent updates, but for this migration, it aligns well with replacing a shell script that *calls* an SQL file.
    *   **New Job Control Infrastructure**: Creating new `job_control` and `job_log` tables requires new DDL and management, but it provides a standardized, BigQuery-native way to manage job state and logging, which is a significant improvement over disparate legacy mechanisms.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project_id.dataset_id`) exists. If not, create it.
2.  **DDL Deployment**:
    *   Execute the DDL scripts to create the necessary tables:
        *   `ddl/ta_discount.ddl`
        *   `ddl/job_control_table.ddl`
        *   `ddl/job_log_table.ddl`
    *   Ensure all source tables referenced in `sql/d_ausd_v_ta_discount_bq.sql` (e.g., `cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `dwtk_meldungen`) are migrated and available in the same `project_id.dataset_id` or a linked dataset.
3.  **Stored Procedure Deployment**:
    *   Execute `stored_procedures/r_ausd_vertrag_control.bqsql` to create the BigQuery Stored Procedure.
4.  **IAM Permissions Configuration**:
    *   The service account used to execute the BigQuery Stored Procedure (e.g., by Airflow or directly) must have:
        *   `BigQuery Data Editor` role on `project_id.dataset_id` to write to `ta_discount`, `job_control`, and `job_log`.
        *   `BigQuery Data Viewer` role on `project_id.dataset_id` (or specific datasets) for all source tables referenced in `d_ausd_v_ta_discount_bq.sql` (e.g., `cds_ta_discount_bc_assoc`, `dwtk_meldungen`).
        *   `BigQuery Job User` role to run BigQuery jobs.
    *   If using Airflow, the Cloud Composer service account requires the necessary BigQuery roles as described above.
5.  **Airflow DAG Configuration and Deployment (if applicable)**:
    *   Update `dags/airflow_dag_k_ausd_v_ta_discount.py` with the correct `PROJECT_ID` and `DATASET_ID`.
    *   Configure the `job_kennung` and `eintrags_nr` parameters within the DAG. These might be sourced from Airflow Variables, XComs, or other dynamic mechanisms.
    *   Ensure the `google_cloud_default` connection is properly configured in Airflow.
    *   Deploy the DAG to your Cloud Composer environment.
6.  **`dwtk_meldungen` table**: Verify the `dwtk_meldungen` table exists in BigQuery and contains relevant data for the `v_run_date_str` calculation as per the original script's logic.

## 5. Known gaps & unresolved references

The following items were identified as gaps or require further analysis and potential follow-up:

*   **`d_ausd_v_ta_discount.sql` Content Analysis**: The full, detailed content and complexity of the original `d_ausd_v_ta_discount.sql` script were not available for a complete translation. The provided BigQuery SQL (`sql/d_ausd_v_ta_discount_bq.sql`) is a best-effort translation based on typical patterns. Any RDBMS-specific syntax, complex PL/SQL, or vendor-specific functions within the original SQL might require further manual translation and testing.
*   **Detailed Job Control Logic**: The precise implementation of "ignoring active jobs" and "deactivating older jobs" from the legacy `starteSQLSkript` function (within `h_alis_sqlplus.ksh`) and `d_ausd_v_ta_discount.sql` was inferred. While the BigQuery Stored Procedure implements a robust job control mechanism, a thorough review against the original logic is needed to ensure exact functional equivalence.
*   **Utility Script Functionality**: The detailed implementations of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` were not fully analyzed. While common functionalities are covered, any highly specific or complex logic within these utilities might not be fully replicated and may require custom BigQuery UDFs or external Cloud Functions.
*   **Parameter Origin**: The exact source and generation mechanism of `p_JobKennung` and `p_EintragsNr` in the legacy system (i.e., how they were supplied to `k_ausd_v_ta_discount.ksh`) need to be identified to ensure correct and consistent parameter passing in the new BigQuery/Airflow environment.
*   **`dwtk_meldungen` table schema**: The schema and content of the `dwtk_meldungen` table, used for determining `v_run_date_str`, are assumed. Its migration and data integrity are critical for the correct functioning of the `r_ausd_vertrag_control` stored procedure.

## 6. Validation

Validation of the migrated job involves both unit and integration testing to ensure functional equivalence and correct operation in the BigQuery environment.

### How to Run Tests

1.  **Unit Tests (BigQuery Stored Procedure)**:
    *   **Parameter Validation**: Execute the `r_ausd_vertrag_control` stored procedure directly in BigQuery with intentionally missing or empty `p_JobKennung` or `p_EintragsNr` parameters.
        ```sql
        CALL `project_id.dataset_id.r_ausd_vertrag_control`('', 'ENTRY_001');
        CALL `project_id.dataset_id.r_ausd_vertrag_control`('JOB_001', '');
        ```
    *   **Job Control Logic**: Execute the stored procedure multiple times in quick succession with the same `p_JobKennung`.
        ```sql
        CALL `project_id.dataset_id.r_ausd_vertrag_control`('TEST_JOB_CONTROL', 'ENTRY_001');
        -- Immediately run again
        CALL `project_id.dataset_id.r_ausd_vertrag_control`('TEST_JOB_CONTROL', 'ENTRY_001');
        ```
    *   **Core SQL Logic**: Populate the source tables (`cds_ta_discount_bc_assoc`, etc.) with representative sample data. Execute the core SQL logic (the `CREATE OR REPLACE TABLE` statement) from `sql/d_ausd_v_ta_discount_bq.sql` directly, ensuring it populates `ta_discount` as expected.
2.  **Integration Tests (End-to-End)**:
    *   **Via Airflow (if deployed)**: Trigger the `k_ausd_v_ta_discount_bq_dag` in your Cloud Composer environment. Monitor the DAG run in the Airflow UI.
    *   **Direct BigQuery Call**: Execute the stored procedure directly with valid parameters.
        ```sql
        CALL `project_id.dataset_id.r_ausd_vertrag_control`('PRODUCTION_JOB', 'PROD_ENTRY_001');
        ```

### What "Passing" Means

A successful migration and validation are indicated by the following criteria:

*   **Data Accuracy**: The `project_id.dataset_id.ta_discount` table is populated with data that is functionally equivalent to the output of the legacy system for the same input data. This should be verified by comparing record counts and a sample of data rows.
*   **Job Control Integrity**:
    *   Queries against `project_id.dataset_id.job_control` show accurate job lifecycle: `start_time`, `end_time`, `status` (`SUCCEEDED`, `FAILED`, `SKIPPED`, `DEACTIVATED`), `processed_records`, and `is_active` flags are correctly updated.
    *   When an active job exists, subsequent runs with the same `p_JobKennung` are correctly `SKIPPED` or older jobs are `DEACTIVATED` before a new one starts.
*   **Logging and Error Handling**:
    *   The `project_id.dataset_id.job_log` table contains relevant informational messages for job start, completion, and any warnings.
    *   In case of errors (e.g., invalid parameters, SQL execution failure), the `job_log` table captures `ERROR` level messages with appropriate `error_details`.
    *   The stored procedure exits gracefully on expected errors, and the `job_control` status reflects `FAILED`.
*   **Performance**: The execution time and BigQuery costs are within acceptable limits, ideally matching or improving upon the legacy system's performance.
*   **No Unexpected Failures**: The job completes without unexpected BigQuery errors, resource exhaustion, or other system-level issues.

## 7. Rollback procedure

In the event of critical issues detected after deployment, the following rollback procedure should be followed to revert to the legacy system:

1.  **Immediate Halt**:
    *   If the Airflow DAG (`k_ausd_v_ta_discount_bq_dag`) was deployed, immediately **pause or disable** the DAG in the Airflow UI to prevent further executions.
    *   If the BigQuery Stored Procedure is being invoked by other means, stop all scheduled or manual invocations.
2.  **Revert BigQuery Objects**:
    *   **Drop Stored Procedure**: Drop the migrated BigQuery Stored Procedure.
        ```sql
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.r_ausd_vertrag_control`;
        ```
    *   **Drop Tables**: Drop the newly created tables associated with this migration.
        ```sql
        DROP TABLE IF EXISTS `project_id.dataset_id.ta_discount`;
        DROP TABLE IF EXISTS `project_id.dataset_id.job_control`;
        DROP TABLE IF EXISTS `project_id.dataset_id.job_log`;
        ```
3.  **Data Restoration (if applicable)**:
    *   If the migration involved destructive changes to the `ta_discount` table (e.g., `CREATE OR REPLACE TABLE` or `TRUNCATE` + `INSERT` without prior backup), restore the `ta_discount` table from the last known good backup or snapshot. BigQuery's time travel feature can be used for this if the table was not explicitly dropped.
4.  **Re-enable Legacy System**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh` script and its associated scheduling mechanism.
5.  **Post-Rollback Analysis**:
    *   Thoroughly analyze the root cause of the failure using BigQuery logs, Airflow logs, and any available monitoring data before attempting any re-deployment or further migration efforts.