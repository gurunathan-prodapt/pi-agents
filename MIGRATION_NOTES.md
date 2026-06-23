# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh` and its associated SQL script `d_ausd_v_ta_c_bfc.sql`. The original job orchestrated data processing, including parameter validation, job state management, SQL execution, and record count capture.

The job has been migrated to **Google Cloud BigQuery**, leveraging BigQuery Stored Procedures for orchestration and data transformation, BigQuery tables for logging and job control, and a BigQuery User-Defined Function (UDF) for specific business logic. Orchestration and scheduling will be handled by **Cloud Composer (Apache Airflow)**.

## 2. Generated Artifacts

The following artifacts were generated as part of this migration:

*   **`sql/ddl/job_error_log.sql`**:
    *   **Role**: BigQuery DDL for creating the `job_error_log` table. This table centralizes error reporting, replacing the custom error handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`) of the legacy KornShell script.
*   **`sql/ddl/job_run_log.sql`**:
    *   **Role**: BigQuery DDL for creating the `job_run_log` table. This table captures general job events, including start/end times and processed record counts, replacing temporary file usage for record counts and implicit logging.
*   **`sql/ddl/job_control_table.sql`**:
    *   **Role**: BigQuery DDL for creating the `job_control_table`. This table manages the state and concurrency of jobs, replicating the "deactivate old active jobs" logic from the legacy script.
*   **`sql/ddl/ta_c_bfc_table.sql`**:
    *   **Role**: BigQuery DDL for creating the target data table `ta_c_bfc`. This is the table where the core data processing results are stored. Column definitions are placeholders and should be refined based on the source system's schema.
*   **`sql/ddl/source_tables_placeholder.sql`**:
    *   **Role**: BigQuery DDL for creating placeholder source tables (`sof_ta_cntrct_crs`, `sof_ta_barrier`, `sof_ta_cntrct_valid`, `sof_ta_period`). These schemas are minimal and must be fully defined based on the actual source Oracle database schemas.
*   **`sql/udf/bfc_get_bindefrist_udf.sql`**:
    *   **Role**: BigQuery User-Defined Function (UDF) named `bfc_get_bindefrist`. This UDF is a placeholder for the business logic originally encapsulated in the Oracle package `Cds$vr_Bindefrist.GetBindeFrist`. Its logic needs to be fully re-implemented in BigQuery SQL or JavaScript.
*   **`sql/procedure/r_ausd_ta_c_bfc_procedure.sql`**:
    *   **Role**: The core BigQuery Stored Procedure, `r_ausd_ta_c_bfc`. This procedure re-implements the entire orchestration logic of `k_ausd_v_ta_c_bfc.ksh` and the data transformation logic of `d_ausd_v_ta_c_bfc.sql`. It handles parameter validation, job state management, core data processing, and logging.
*   **`dags/airflow_k_ausd_v_ta_c_bfc.py`**:
    *   **Role**: An Apache Airflow DAG definition. This DAG is responsible for scheduling and invoking the `r_ausd_ta_c_bfc` BigQuery Stored Procedure, passing the necessary parameters.

## 3. Key Design Decisions

*   **BigQuery Stored Procedure for Orchestration**: The entire KornShell script's orchestration logic (parameter validation, job state management, error handling) has been translated into a BigQuery Stored Procedure (`r_ausd_ta_c_bfc`). This centralizes the job logic within BigQuery, leveraging its native capabilities and reducing external script dependencies.
*   **Dedicated BigQuery Tables for Logging and Job Control**: Instead of temporary files and implicit job state, explicit BigQuery tables (`job_error_log`, `job_run_log`, `job_control_table`) are used. This provides persistent, queryable, and centralized logging and job state management, improving observability and debugging.
*   **BigQuery UDF for Complex Business Logic**: The Oracle package function `Cds$vr_Bindefrist.GetBindeFrist` has been isolated into a BigQuery UDF (`bfc_get_bindefrist`). This encapsulates the specific business logic, making it reusable and easier to manage, although its re-implementation is a critical follow-up item.
*   **Cloud Composer (Airflow) for Scheduling**: The job's scheduling and invocation are managed by an Airflow DAG. This provides robust, scalable, and observable orchestration capabilities, replacing traditional cron-based scheduling.
*   **Direct DML/DDL Execution within BigQuery Stored Procedure**: The `starteSQLSkript` functionality and the external `d_ausd_v_ta_c_bfc.sql` file are replaced by direct DML/DDL statements embedded within the BigQuery Stored Procedure. This simplifies execution, eliminates external file management, and optimizes performance within BigQuery.
*   **Handling of Oracle `ROWNUM` for Update Limits**: The original Oracle `UPDATE` statement included `ROWNUM <= &v_max_update` to limit the number of rows updated. BigQuery Standard SQL does not have a direct equivalent for limiting `UPDATE` statements in this manner. The migrated BigQuery `UPDATE` will apply to *all* matching rows. This is a functional change that assumes the `ROWNUM` limit was not critical for the business logic or that the performance impact of updating all rows is acceptable. If the limit was essential, a redesign (e.g., using a temporary table with `QUALIFY ROW_NUMBER()`) would be required.
*   **`v_bfc_procedure_date` Derivation**: The original script derived `v_bfc_procedure_date` from `all_objects.created` for an Oracle package. In the BigQuery migration, this has been simplified to `CURRENT_DATE()`. This assumes that the procedure's logic should always be considered "current" for the purpose of `bfc_procedure` date. If a specific historical or configuration-driven date is required, this logic needs to be adjusted.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`your_dataset` in `your_project.your_dataset`) exists.
2.  **IAM Permissions**: Grant the necessary Identity and Access Management (IAM) roles to the service account that will execute the BigQuery Stored Procedure and the Airflow DAG. This typically includes `BigQuery Data Editor` for the dataset containing the tables and `BigQuery Job User` for running queries.
3.  **Source Data Migration**:
    *   Migrate all source tables referenced in `d_ausd_v_ta_c_bfc.sql` (e.g., `sof_ta_cntrct_crs`, `sof_ta_barrier`, `sof_ta_cntrct_valid`, `sof_ta_period`) from their original Oracle environment to BigQuery.
    *   **Crucially, refine the placeholder DDLs in `sql/ddl/source_tables_placeholder.sql`** to accurately reflect the full schema (column names, data types, nullability) of the source Oracle tables.
4.  **Target Table Schema Refinement**: Review and refine the DDL for `sql/ddl/ta_c_bfc_table.sql` to ensure all columns have correct BigQuery data types and constraints based on the expected output.
5.  **Re-implement `bfc_get_bindefrist` UDF Logic**: The `sql/udf/bfc_get_bindefrist_udf.sql` currently contains placeholder logic. The actual business logic from the Oracle package `Cds$vr_Bindefrist.GetBindeFrist` must be fully re-implemented in BigQuery SQL or JavaScript within this UDF. This is a critical functional requirement.
6.  **Airflow DAG Configuration**:
    *   Replace `your_project` and `your_dataset` placeholders in `dags/airflow_k_ausd_v_ta_c_bfc.py` with the actual GCP Project ID and BigQuery Dataset ID.
    *   Configure the `gcp_conn_id` (e.g., `google_cloud_default`) in Airflow to ensure proper authentication to BigQuery.
    *   Replace `DEFAULT_JOB_KENNUNG` and `DEFAULT_EINTRAGS_NR` with the actual parameters expected by the BigQuery Stored Procedure. These might need to be dynamic based on the scheduling context.
    *   Define the `schedule_interval` for the Airflow DAG as per the job's operational requirements.
7.  **Job Control Table Logic Review**: Review the `WHERE` clause for deactivating old jobs in `sql/procedure/r_ausd_ta_c_bfc_procedure.sql` (`UPDATE your_project.your_dataset.job_control_table SET status = 'INACTIVE' WHERE job_kenn_ung = p_job_kennung AND status = 'ACTIVE';`). If the original logic considered `p_eintrags_nr` or other criteria for deactivation, this `WHERE` clause needs to be refined.
8.  **`v_bfc_procedure_date` Source**: If `CURRENT_DATE()` is not the desired source for `v_bfc_procedure_date`, adjust the `SET v_bfc_procedure_date = CURRENT_DATE();` line in the stored procedure to retrieve the date from a configuration table or another appropriate source.

## 5. Known Gaps & Unresolved References

*   **Full `d_ausd_v_ta_c_bfc.sql` Content**: The original SQL script `d_ausd_v_ta_c_bfc.sql` was not provided for full inspection. The translation of its logic into the BigQuery Stored Procedure is based on inferred usage and common Oracle patterns. Any highly complex, proprietary, or undocumented Oracle SQL features (e.g., specific PL/SQL blocks, advanced analytical functions, or vendor-specific hints) might not be fully or correctly translated. This is the biggest unknown and potential source of discrepancies.
*   **`bfc_get_bindefrist` UDF Logic (B4 Item)**: The `sql/udf/bfc_get_bindefrist_udf.sql` is explicitly a placeholder. The actual business logic from the Oracle package `Cds$vr_Bindefrist.GetBindeFrist` *must* be re-implemented in BigQuery SQL or JavaScript. This is a critical functional gap and a **Blocker (B4)** item for go-live.
*   **`ROWNUM` Limitation Impact**: The removal of the `ROWNUM` limit in the final `UPDATE` statement of the BigQuery Stored Procedure is a functional change. It needs to be confirmed if the original `ROWNUM` limit was critical for business logic (e.g., processing only a subset of records) or performance. If it was critical, a redesign of this specific `UPDATE` statement is required.
*   **Exact Job Deactivation Logic**: While the `job_control_table` and its update logic are implemented, the precise conditions for "deactivating old active jobs" (e.g., whether `p_eintrags_nr` also played a role in identifying old jobs to deactivate) need to be verified against the legacy system's behavior.
*   **Error Code Mapping**: The error codes `192` and `193` used in the BigQuery Stored Procedure are arbitrary placeholders from the original script's error handling. A comprehensive mapping of legacy error codes to a new BigQuery-specific or standardized error code system is needed.
*   **Source Table Schemas**: The DDLs for source tables (`sql/ddl/source_tables_placeholder.sql`) are minimal and inferred. The complete and accurate schemas must be derived from the source Oracle database.
*   **`v_bfc_procedure_date` Source**: The current implementation uses `CURRENT_DATE()`. If the original `all_objects.created` date for the Oracle package had a specific historical or configuration-driven significance that `CURRENT_DATE()` does not capture, this needs to be addressed.

## 6. Validation

Validation should cover functional correctness, data integrity, performance, and operational aspects.

### How to Run Tests:

1.  **Deploy Artifacts**:
    *   Execute all DDL scripts (`sql/ddl/*.sql`) in the target BigQuery dataset.
    *   Deploy the UDF (`sql/udf/bfc_get_bindefrist_udf.sql`).
    *   Deploy the Stored Procedure (`sql/procedure/r_ausd_ta_c_bfc_procedure.sql`).
    *   Deploy the Airflow DAG (`dags/airflow_k_ausd_v_ta_c_bfc.py`) to your Cloud Composer environment.
2.  **Prepare Test Data**:
    *   Load representative sample data into the BigQuery source tables (`sof_ta_cntrct_crs`, etc.) that mirrors the production data in the legacy system.
    *   Ensure the test data covers various scenarios:
        *   New `cntrct_id` values.
        *   Existing `cntrct_id` values with updated `bfc_age` or `bfc_count`.
        *   Existing `cntrct_id` values with `bfc_procedure` older than `v_bfc_procedure_date`.
        *   Edge cases for `bfc_get_bindefrist` UDF.
3.  **Execute the Job**:
    *   **Manual Execution (for initial testing)**: Directly call the BigQuery Stored Procedure from the BigQuery console:
        ```sql
        CALL `your_project.your_dataset.r_ausd_ta_c_bfc`('TEST_JOB', 'TEST_ENTRY');
        ```
    *   **Airflow Execution (for integrated testing)**: Trigger the `k_ausd_v_ta_c_bfc_bigquery_migration` DAG in Airflow. Ensure the `p_job_kennung` and `p_eintrags_nr` parameters are set correctly in the DAG definition or via Airflow's configuration.
4.  **Monitor Logs**: Observe the `job_run_log` and `job_error_log` tables in BigQuery for entries related to the test runs.
5.  **Data Comparison**:
    *   After successful runs, compare the data in the target `your_project.your_dataset.ta_c_bfc` table with the corresponding output from the legacy system for the same input data.
    *   Focus on `bindefrist`, `bfc_age`, `bfc_count`, `bfc_procedure`, `commitment_reference_date`, and `cntrct_validity_id`.
6.  **Error Scenario Testing**:
    *   Test with missing/invalid `p_job_kennung` or `p_eintrags_nr` to ensure parameter validation and error logging work as expected.
    *   Simulate potential data errors (if possible) to test the error handling.

### What "Passing" Means:

*   **Functional Correctness**:
    *   The BigQuery Stored Procedure completes without unhandled errors.
    *   The `job_run_log` table accurately records job start, end, and `record_count` (total rows affected by the final `UPDATE`).
    *   The `job_error_log` table correctly captures and details any errors, including parameter validation failures.
    *   The `job_control_table` accurately reflects the job's state transitions (ACTIVE, COMPLETED, FAILED) and deactivates old jobs as expected.
    *   The data in `your_project.your_dataset.ta_c_bfc` exactly matches the output generated by the legacy `k_ausd_v_ta_c_bfc.ksh` job for identical input data. This is the most critical criterion.
*   **Performance**: The execution time of the BigQuery Stored Procedure is within acceptable limits, ideally matching or improving upon the legacy system's performance.
*   **Resource Utilization**: BigQuery slot consumption and cost are within expected bounds.
*   **Observability**: Airflow DAG runs are successful, and logs are accessible and informative.

## 7. Rollback Procedure

In case of critical issues identified after go-live, the following rollback procedure can be initiated:

1.  **Immediate Action (Stop New Runs)**:
    *   **Deactivate Airflow DAG**: Pause or delete the `k_ausd_v_ta_c_bfc_bigquery_migration` DAG in Cloud Composer to prevent any further execution of the BigQuery Stored Procedure.
    *   **Revert Scheduling**: Re-enable the scheduling mechanism for the original `k_ausd_v_ta_c_bfc.ksh` script (e.g., re-add its cron entry).
2.  **Data Rollback (if necessary)**:
    *   If the `your_project.your_dataset.ta_c_bfc` table was modified by the migrated job and the changes are deemed incorrect or corrupted, restore the table to its state prior to the migration. This can be done using BigQuery's time travel feature (e.g., `CREATE TABLE ... AS SELECT * FROM your_project.your_dataset.ta_c_bfc FOR SYSTEM_TIME AS OF TIMESTAMP '...'`) or from a previously taken snapshot/backup.
    *   Verify that the restored data is consistent with the legacy system's expectations.
3.  **Code Rollback (Cleanup)**:
    *   **Delete BigQuery Stored Procedure**: Drop the `r_ausd_ta_c_bfc` stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project.your_dataset.r_ausd_ta_c_bfc`;
        ```
    *   **Delete BigQuery UDF**: Drop the `bfc_get_bindefrist` UDF:
        ```sql
        DROP FUNCTION IF EXISTS `your_project.your_dataset.bfc_get_bindefrist`;
        ```
    *   **Delete BigQuery Tables (Optional, if not needed for historical logging)**: If the logging and control tables are not required for post-mortem analysis, they can be dropped:
        ```sql
        DROP TABLE IF EXISTS `your_project.your_dataset.job_error_log`;
        DROP TABLE IF EXISTS `your_project.your_dataset.job_run_log`;
        DROP TABLE IF EXISTS `your_project.your_dataset.job_control_table`;
        ```
    *   **Remove Airflow DAG**: Delete the `airflow_k_ausd_v_ta_c_bfc.py` file from the Airflow DAGs folder.
4.  **Monitor Legacy System**: Ensure the legacy `k_ausd_v_ta_c_bfc.ksh` job is running correctly and producing expected output after the rollback.