# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh`. The original script was responsible for orchestrating the initial provisioning of selected base products for the BERT system, extracting contract cache data from the Data Warehouse (DWH), and making it available for Forderungsscoring (FOS).

The job has been migrated to Google Cloud Platform (GCP), leveraging **BigQuery** for data processing and **Cloud Composer** (managed Apache Airflow) for orchestration and scheduling. The core logic, previously split between `r_ausd_bp_ta_bpr_evn.ksh` and its invoked script `k_ausd_bp_ta_bpr_evn.ksh`, has been refactored into BigQuery Stored Procedures.

## 2. Generated artifacts

The migration process generated the following files:

*   **`ddl/create_job_audit_log_table.sql`**
    *   **Role:** This SQL DDL script defines the schema for the `job_audit_log` table in BigQuery. This table is used by the migrated procedures to record job execution status, parameters, and error messages, replacing the custom shell logging framework.
*   **`ddl/create_contract_cache_source_table.sql`**
    *   **Role:** This SQL DDL script provides a placeholder schema for the `contract_cache_source` table in BigQuery. This table is intended to hold the source contract cache data, mirroring the structure of the legacy DWH source. **Note:** This schema is a placeholder and requires refinement based on the actual source DWH schema.
*   **`ddl/create_fos_target_table.sql`**
    *   **Role:** This SQL DDL script provides a placeholder schema for the `fos_target_table` in BigQuery. This table serves as the target for the processed data, provisioning it for FOS. It includes a `stichtag` column to identify data for a specific cutoff date. **Note:** This schema is a placeholder and requires refinement based on the actual target data requirements.
*   **`bigquery/ausd_bp_ta_bpr_evn_core.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the core data processing logic. It performs conditional deletions from the target table based on restart values, checks for active contract cache records, and inserts data from the source into the target table, applying the necessary date and ID filtering. This procedure replaces the assumed functionality of `k_ausd_bp_ta_bpr_evn.ksh`.
*   **`bigquery/ausd_bp_ta_bpr_evn_wrapper.sql`**
    *   **Role:** This BigQuery Stored Procedure acts as the main entry point and orchestration layer. It handles parameter parsing, validation, defaulting, and integrates logging into the `job_audit_log` table. It then calls the `ausd_bp_ta_bpr_evn_core` procedure to perform the actual data manipulation and includes robust error handling. This procedure replaces the wrapper functionality of `r_ausd_bp_ta_bpr_evn.ksh`.
*   **`dags/ausd_bp_ta_bpr_evn_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG for Cloud Composer. It is responsible for scheduling and orchestrating the execution of the `ausd_bp_ta_bpr_evn_wrapper` BigQuery Stored Procedure, passing dynamic parameters like the execution date.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Logic Encapsulation:** The decision to migrate the shell script logic into BigQuery Stored Procedures (`_wrapper` and `_core`) was made to leverage BigQuery's native capabilities for data processing, scalability, and SQL-based logic. This eliminates the need for external compute environments for data transformation and aligns with a cloud-native data warehousing approach.
*   **Separation of Concerns (Wrapper and Core Procedures):** The original KornShell job was split into an orchestration script (`r_ausd_bp_ta_bpr_evn.ksh`) and a core logic script (`k_ausd_bp_ta_bpr_evn.ksh`). This pattern was preserved by creating `ausd_bp_ta_bpr_evn_wrapper` for parameter handling, logging, and orchestration, and `ausd_bp_ta_bpr_evn_core` for the actual data manipulation. This improves modularity, testability, and maintainability.
*   **Cloud Composer for Orchestration:** Cloud Composer was chosen as the orchestration layer to provide robust scheduling, monitoring, and error handling capabilities. It allows for defining complex workflows, managing dependencies, and integrating seamlessly with other GCP services, replacing the legacy cron-based or custom scheduler.
*   **BigQuery for Logging:** A dedicated `job_audit_log` table in BigQuery replaces the custom shell script logging. This centralizes logging, makes it queryable, and allows for easier monitoring and auditing of job executions.
*   **Trade-offs:**
    *   **Schema Definition:** The initial migration provides placeholder schemas for source and target tables. This requires manual effort to accurately define the schemas based on the legacy DWH, which is a necessary step for a robust BigQuery implementation.
    *   **Shell-specific features:** Any highly procedural shell logic, filesystem operations, or calls to external non-database tools in the original `k_ausd_bp_ta_bpr_evn.ksh` would require more complex translation (e.g., to Python in Dataflow or Cloud Functions), but for this job, the assumption is that the core logic is SQL-like.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a GCP project (`your_gcp_project`) is set up.
    *   Create the target BigQuery dataset (`your_bigquery_dataset`) where the tables and stored procedures will reside.
2.  **BigQuery Table Creation and Schema Refinement:**
    *   Execute the DDL scripts:
        *   `ddl/create_job_audit_log_table.sql`
        *   `ddl/create_contract_cache_source_table.sql`
        *   `ddl/create_fos_target_table.sql`
    *   **Crucially, refine the schemas for `contract_cache_source` and `fos_target_table`** to accurately reflect the column names, data types, and nullability of the actual legacy DWH source and the desired FOS target. The provided DDLs are placeholders.
3.  **BigQuery Stored Procedure Deployment:**
    *   Execute the SQL scripts to create or replace the BigQuery Stored Procedures:
        *   `bigquery/ausd_bp_ta_bpr_evn_core.sql`
        *   `bigquery/ausd_bp_ta_bpr_evn_wrapper.sql`
4.  **IAM Permissions:**
    *   The service account used by Cloud Composer (or any other entity executing the BigQuery procedures) must have the following BigQuery roles:
        *   `BigQuery Data Editor` on `your_gcp_project.your_bigquery_dataset` to write to `job_audit_log` and `fos_target_table`.
        *   `BigQuery Data Viewer` on `your_gcp_project.your_bigquery_dataset` to read from `contract_cache_source`.
        *   `BigQuery Job User` on `your_gcp_project` to run BigQuery jobs.
5.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned.
    *   Update the `dags/ausd_bp_ta_bpr_evn_dag.py` file with your specific `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, and `BIGQUERY_LOCATION`.
6.  **Deploy Airflow DAG:**
    *   Upload the `dags/ausd_bp_ta_bpr_evn_dag.py` file to the DAGs folder of your Cloud Composer environment.
7.  **Data Ingestion for `contract_cache_source`:**
    *   Establish a mechanism to load data from the legacy DWH contract cache into the `your_gcp_project.your_bigquery_dataset.contract_cache_source` table. This could involve other migration jobs, data transfer services, or ETL pipelines.

## 5. Known gaps & unresolved references

*   **Actual Content of `k_ausd_bp_ta_bpr_evn.ksh`:** The migration design assumes that `k_ausd_bp_ta_bpr_evn.ksh` primarily contains SQL-like data manipulation logic. If it contains complex shell scripting, file system operations, or calls to other external programs, these parts would require further analysis and potentially different migration strategies (e.g., Cloud Functions, Dataflow, custom Python scripts).
*   **Placeholder Schemas:** The DDLs for `contract_cache_source` and `fos_target_table` are placeholders. Their actual schemas (column names, types, constraints) must be accurately defined based on the legacy DWH and FOS requirements. The `EXCEPT` clause in `ausd_bp_ta_bpr_evn_core` might need adjustment based on the final `fos_target_table` schema.
*   **Helper Script Logic:** The exact content and side effects of legacy helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were not fully analyzed. The migration assumes their core functionalities (logging, parameter parsing, date calculations) are adequately covered by BigQuery's native functions and the `job_audit_log` table. Any non-standard or complex logic within these helpers might need specific BigQuery SQL or Python implementations.
*   **"AL??" Comments:** The meaning and necessity of commented-out lines like `#AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum` in the original script are unclear. This functionality was not migrated and requires clarification with the business owner if it's still relevant.
*   **`file_complexity` Data:** The absence of `file_complexity` data for the source script means that no specific complexity tier or migration flags were available, potentially masking unforeseen complexities during the migration.

## 6. Validation

To ensure the migrated job functions correctly, the following validation steps should be performed:

1.  **Unit Testing BigQuery Stored Procedures:**
    *   Manually execute `ausd_bp_ta_bpr_evn_core` and `ausd_bp_ta_bpr_evn_wrapper` in BigQuery using `bq query` or the BigQuery UI with various test parameters (e.g., different `p_stichtag` values, `p_wiederanlaufWert` > 0 and = 0).
    *   Verify that the `job_audit_log` table correctly records job starts, successes, and failures.
    *   Verify that the conditional `DELETE` logic (for `p_wiederanlaufWert` and `v_active_cnt = 0`) works as expected.
2.  **Integration Testing via Cloud Composer:**
    *   Trigger the `ausd_bp_ta_bpr_evn_dag` manually from the Airflow UI in Cloud Composer.
    *   Monitor the DAG run for successful completion.
    *   Check Airflow logs for any errors or unexpected behavior.
    *   Verify that the `job_audit_log` table is updated correctly by the Composer-triggered job.
3.  **Data Validation:**
    *   **Pre-migration Baseline:** Capture the output of the legacy `r_ausd_bp_ta_bpr_evn.ksh` job for a specific `Stichtag` and `Wiederanlaufwert` using a representative dataset.
    *   **Post-migration Comparison:** Load the same representative source data into `contract_cache_source` in BigQuery. Run the migrated job with the *exact same parameters*.
    *   **Comparison:** Compare the data in the `fos_target_table` in BigQuery with the baseline output from the legacy system.
    *   **Passing Criteria:**
        *   All BigQuery Stored Procedures and the Airflow DAG execute without errors.
        *   The `job_audit_log` table accurately reflects the execution status (STARTED, SUCCESS, FAILED) and parameters.
        *   The data in `your_gcp_project.your_bigquery_dataset.fos_target_table` is identical to the data produced by the legacy system for the same input conditions. This includes verifying row counts, specific column values, and the correct application of filtering and deletion logic.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be followed:

1.  **Disable/Delete Cloud Composer DAG:**
    *   In the Airflow UI, set the `ausd_bp_ta_bpr_evn_dag` to "Off" or delete it from the DAGs folder to prevent further executions of the migrated job.
2.  **Revert to Legacy Execution:**
    *   Resume the scheduling and execution of the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh` script in the legacy environment.
3.  **Data Recovery (if necessary):**
    *   If the `fos_target_table` in BigQuery was corrupted or incorrectly populated by the migrated job, and if this table is critical for downstream systems, a data recovery strategy might be needed. This could involve:
        *   Restoring the table from a BigQuery snapshot or point-in-time recovery (if enabled).
        *   Truncating the `fos_target_table` and re-running the legacy job to populate it correctly (if the legacy system can write to BigQuery or if the target is also rolled back).
        *   Manually correcting the data if the impact is limited.
4.  **Optional: Clean Up BigQuery Artifacts:**
    *   If the rollback is permanent, consider dropping the BigQuery Stored Procedures (`ausd_bp_ta_bpr_evn_core`, `ausd_bp_ta_bpr_evn_wrapper`) and the associated tables (`job_audit_log`, `fos_target_table`, `contract_cache_source`) from the BigQuery dataset. This should only be done if no other dependencies rely on these objects.