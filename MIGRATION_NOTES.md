# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_cntrct_dist.ksh` and its associated SQL script `d_ausd_bp_ta_cntrct_dist.sql`. The original job orchestrated data processing, including parameter validation, environment setup, date derivation, and execution of a core SQL transformation, followed by record count capture.

The job has been migrated to a Google Cloud Platform (GCP) native solution. The orchestration logic of the KornShell script is now encapsulated within a BigQuery Stored Procedure, while the core data transformation logic from the SQL script is translated into BigQuery SQL. Scheduling and execution management are handled by an Apache Airflow DAG deployed on Cloud Composer.

## 2. Generated Artifacts

The migration produced the following artifacts:

*   **`ddl/sof_ta_cntrct_dist.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the target table `project.dataset.sof_ta_cntrct_dist`. This table replaces the legacy `SOF$TA_CNTRCT_DIST` table and will store the distinct contract IDs.
*   **`ddl/job_audit_table.sql`**
    *   **Role**: BigQuery DDL script to create `project.dataset.job_audit_table`. This table serves as a centralized audit log for migrated jobs, capturing execution details, parameters, status, and record counts, replacing the legacy temporary files and job table updates.
*   **`bigquery_sql/d_ausd_bp_ta_cntrct_dist_logic.sql`**
    *   **Role**: Contains the core data transformation logic, translated from the original `d_ausd_bp_ta_cntrct_dist.sql` to BigQuery SQL. This script is designed to be executed within the BigQuery Stored Procedure. It inserts distinct contract IDs from `project.dataset.sof_ta_bpr_basis` into `project.dataset.sof_ta_cntrct_dist`.
*   **`bigquery_sp/sp_k_ausd_bp_ta_cntrct_dist.sql`**
    *   **Role**: A BigQuery Stored Procedure that replaces the orchestration logic of the original `k_ausd_bp_ta_cntrct_dist.ksh` script. It handles parameter validation, date derivation, truncates the target table, executes the core transformation logic, captures record counts, and logs execution details to the `job_audit_table`.
*   **`airflow_dag/k_ausd_bp_ta_cntrct_dist_dag.py`**
    *   **Role**: An Apache Airflow DAG (Python script) for Cloud Composer. This DAG is responsible for scheduling and invoking the `project.dataset.sp_k_ausd_bp_ta_cntrct_dist` BigQuery Stored Procedure, passing the necessary parameters.

## 3. Key Design Decisions

*   **BigQuery Stored Procedure for Orchestration**: The entire control flow, parameter parsing, validation, and environment setup logic from the KornShell script was migrated into a BigQuery Stored Procedure (`sp_k_ausd_bp_ta_cntrct_dist`). This centralizes the job's logic within BigQuery, leveraging its native scripting capabilities and reducing dependencies on external shell environments.
*   **Direct BigQuery SQL for Data Transformation**: The business logic from `d_ausd_bp_ta_cntrct_dist.sql` was directly translated into BigQuery SQL and embedded within the stored procedure. This avoids the need for intermediate processing layers or external SQL execution tools.
*   **Cloud Composer (Apache Airflow) for Scheduling**: Cloud Composer was chosen as the primary orchestration tool. It provides robust scheduling, monitoring, and error handling capabilities, replacing the legacy scheduling mechanism that triggered the KornShell script.
*   **BigQuery Tables for Audit and Logging**: Instead of temporary files (`.tmp`) and legacy job tables, a dedicated `job_audit_table` in BigQuery is used to log all execution details, including start/end times, status, error messages, input parameters, and output record counts. This provides a centralized, queryable audit trail.
*   **Native BigQuery Functions for Utilities**: Legacy utility scripts (e.g., `gestern.ksh` for date derivation, `h_alis_parameter.ksh` for validation) were replaced by equivalent BigQuery SQL functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`, `IF/RAISE` statements). This minimizes external dependencies and keeps the solution BigQuery-native.
*   **TRUNCATE and INSERT Pattern**: The stored procedure explicitly `TRUNCATE`s the target table `sof_ta_cntrct_dist` before performing the `INSERT` operation. This aligns with the likely behavior of the original `TRUNCATE TABLE sof$ta_cntrct_dist REUSE STORAGE` statement implied by the `DWPA_UTIL_SKRIPT` package.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **GCP Project and Dataset Setup**:
    *   Ensure the target GCP project and BigQuery dataset (`project.dataset`) exist.
2.  **BigQuery Table Creation**:
    *   Execute `ddl/sof_ta_cntrct_dist.sql` to create the `project.dataset.sof_ta_cntrct_dist` table.
    *   Execute `ddl/job_audit_table.sql` to create the `project.dataset.job_audit_table`.
3.  **Source Data Availability**:
    *   Verify that the source tables `project.dataset.sof_ta_bpr_basis` and `project.dataset.DWTK_MELDUNGEN` (if used by the full `d_ausd_bp_ta_cntrct_dist.sql` logic) exist and are populated with the necessary data.
4.  **BigQuery Stored Procedure Deployment**:
    *   Execute `bigquery_sp/sp_k_ausd_bp_ta_cntrct_dist.sql` to create or replace the stored procedure in `project.dataset`.
5.  **Cloud Composer Environment Setup**:
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Configure the `google_cloud_default` Airflow connection to point to the correct GCP project and service account with BigQuery access.
6.  **Airflow DAG Deployment**:
    *   Upload `airflow_dag/k_ausd_bp_ta_cntrct_dist_dag.py` to the DAGs folder of your Cloud Composer environment.
7.  **Airflow DAG Configuration**:
    *   **Scheduling**: Update the `schedule` parameter in `k_ausd_bp_ta_cntrct_dist_dag.py` from `None` to the desired cron expression or timedelta (e.g., `'0 0 * * *'` for daily at midnight UTC).
    *   **Parameters**: Review and update the hardcoded parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`, `p_wiederanlauf_wert`) within the `BigQueryInsertJobOperator` in the DAG. These should ideally be dynamic, using Airflow variables, macros (e.g., `{{ ds_nodash }}` for `p_stichtag`), or XComs for production readiness.
8.  **IAM Permissions**:
    *   Grant the service account associated with the Cloud Composer environment (and thus the Airflow DAG) the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on `project.dataset` to read source tables, write to target tables, and execute stored procedures.
        *   `BigQuery Data Viewer` on any other datasets containing lookup tables.
        *   `BigQuery Job User` to run BigQuery jobs.

## 5. Known Gaps & Unresolved References

*   **`DWTK_MELDUNGEN` Table Usage**: The design document indicates `DWTK_MELDUNGEN` as a `READS_TABLE` for `d_ausd_bp_ta_cntrct_dist.sql`. However, the generated `bigquery_sql/d_ausd_bp_ta_cntrct_dist_logic.sql` and the embedded SQL in the stored procedure do not explicitly reference this table. It needs to be confirmed if this table is truly not required for the core logic or if the migration of the SQL logic is incomplete.
*   **`PACKAGE:DWPA_UTIL_SKRIPT`**: While the `TRUNCATE` statement (likely from this package) has been directly implemented, other functions or procedures within the Oracle `DWPA_UTIL_SKRIPT` package might contain additional business logic or utility functions that were not fully analyzed or migrated. A detailed review of this package is recommended.
*   **`p_wiederanlauf_wert` Parameter**: This parameter is passed to the stored procedure but is currently not utilized within its logic. Its original purpose in the legacy script (e.g., for restart logic or specific processing modes) needs to be confirmed and implemented in the BigQuery Stored Procedure if required.
*   **Commented-Out Legacy Logic**: The original KornShell script contained commented-out sections related to job deactivation and complex file post-processing (`sed`, `sort`, `join` for `cibasis_data*.dat`). These have not been migrated. If these functionalities are ever reactivated, they represent additional scope and would require new BigQuery ETL logic.
*   **Dynamic Airflow Parameters**: The example Airflow DAG hardcodes parameters for the BigQuery Stored Procedure call. For a production environment, these parameters should be dynamically sourced (e.g., from Airflow Variables, XComs, or templated values) to ensure flexibility and reusability.
*   **Exact Behavior of Sourced KSH Utilities**: The migration assumed standard functionalities for sourced KornShell utilities (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`). Any subtle or non-standard behaviors of these scripts might not be fully replicated.

## 6. Validation

To validate the successful migration and functionality of the new BigQuery job:

1.  **Trigger the Airflow DAG**:
    *   Manually trigger the `k_ausd_bp_ta_cntrct_dist_dag` from the Cloud Composer UI.
2.  **Monitor Airflow Task Status**:
    *   Verify that the `call_bigquery_stored_procedure` task completes successfully in the Airflow UI.
3.  **Check BigQuery Job History**:
    *   In the BigQuery UI, navigate to "Query history" and confirm that the stored procedure execution (`CALL project.dataset.sp_k_ausd_bp_ta_cntrct_dist(...)`) completed without errors.
4.  **Validate Target Data**:
    *   Query `SELECT * FROM project.dataset.sof_ta_cntrct_dist;`
    *   Verify that the table contains the expected distinct `cntrct_id` values, matching the data from `project.dataset.sof_ta_bpr_basis`.
    *   Confirm that the record count in `sof_ta_cntrct_dist` is as expected (e.g., `SELECT COUNT(DISTINCT cntrct_id) FROM project.dataset.sof_ta_bpr_basis;`).
5.  **Review Audit Log**:
    *   Query `SELECT * FROM project.dataset.job_audit_table ORDER BY start_timestamp DESC LIMIT 1;`
    *   Verify that the latest entry for `job_name = 'k_ausd_bp_ta_cntrct_dist'` has:
        *   `status = 'SUCCESS'`
        *   `output_records` matching the count from step 4.
        *   `input_params` reflecting the parameters passed to the stored procedure.
        *   `start_timestamp` and `end_timestamp` are accurate.
6.  **Error Handling Test (Optional but Recommended)**:
    *   Modify the Airflow DAG or manually call the stored procedure with an intentionally invalid `p_stichtag` (e.g., `'32132023'`).
    *   Verify that the stored procedure `RAISE`s an error, the BigQuery job fails, the Airflow task fails, and an entry in `job_audit_table` is recorded with `status = 'FAILED'` and an appropriate `error_message`.

**"Passing" Criteria**:
A successful validation means:
*   The Airflow DAG runs to completion without errors.
*   The BigQuery Stored Procedure executes successfully.
*   The `project.dataset.sof_ta_cntrct_dist` table is populated correctly with the expected distinct contract IDs.
*   The record count in the target table matches the source logic.
*   A `SUCCESS` entry is recorded in `project.dataset.job_audit_table` with accurate metadata.
*   Error conditions are correctly handled and logged.

## 7. Rollback Procedure

In case of issues or critical failures after go-live, the following rollback procedure can be executed:

1.  **Deactivate New Job**:
    *   In the Cloud Composer UI, pause or delete the `k_ausd_bp_ta_cntrct_dist_dag` to prevent further executions of the migrated job.
2.  **Revert BigQuery Stored Procedure**:
    *   If necessary, drop the `project.dataset.sp_k_ausd_bp_ta_cntrct_dist` stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`;
        ```
3.  **Restore Target Table Data (if necessary)**:
    *   If the `project.dataset.sof_ta_cntrct_dist` table was truncated and data needs to be restored to a previous state (e.g., if the migration introduced bad data), restore it from a BigQuery snapshot, time travel, or a backup. If the table is only populated by this job and no other dependencies, simply dropping and recreating it might suffice.
4.  **Reactivate Legacy Job**:
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh` script and its associated scheduling in the legacy environment are reactivated and running as before.
5.  **Clean Up (Optional)**:
    *   If the rollback is permanent, the `project.dataset.job_audit_table` and `project.dataset.sof_ta_cntrct_dist` tables (if not used by other migrated jobs) can be dropped.