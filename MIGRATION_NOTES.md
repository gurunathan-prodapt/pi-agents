# MIGRATION_NOTES: k_ausd_v_ta_cntrct_valid.ksh

## 1. Summary

This document details the migration of the legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh` to Google BigQuery. The original script orchestrated the processing of contract validity data, including parameter handling, job tracking, and execution of a core SQL script (`d_ausd_v_ta_cntrct_valid.sql`).

The migration targets a Google BigQuery Stored Procedure, `project.dataset.bert_k_ausd_v_ta_cntrct_valid`, which encapsulates the entire logic. Orchestration for this BigQuery Stored Procedure is provided via an Apache Airflow DAG, intended for deployment on Google Cloud Composer.

## 2. Generated Artifacts

The migration produced the following files:

*   **`bigquery/ddl/ta_cntrct_valid.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the target BigQuery table `project.dataset.ta_cntrct_valid`. This table will store the processed contract validity data, serving as the primary output of the migrated job.
*   **`bigquery/ddl/error_log.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.error_log` BigQuery table. This table centralizes error logging for the migrated job, capturing details such as job name, error message, and stack trace, replacing the shell script's error handling mechanisms.
*   **`bigquery/ddl/job_table.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.job_table` BigQuery table. This table tracks the execution status and metadata of the job runs (e.g., `RUNNING`, `SUCCESS`, `FAILED`), mirroring the job tracking functionality inferred from the original KornShell script's comments.
*   **`bigquery/ddl/job_result_log.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.job_result_log` BigQuery table. This table stores key results of job executions, such as the number of records processed, providing a centralized log for job outcomes.
*   **`bigquery/stored_procedures/bert_k_ausd_v_ta_cntrct_valid.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid`. This is the core migrated component, encapsulating the original KornShell script's orchestration logic (parameter validation, job status updates, error handling) and the data processing logic from the external SQL script `d_ausd_v_ta_cntrct_valid.sql`.
*   **`airflow/dags/k_ausd_v_ta_cntrct_valid_dag.py`**
    *   **Role:** An Apache Airflow DAG definition. This Python script orchestrates the execution of the `bert_k_ausd_v_ta_cntrct_valid` BigQuery Stored Procedure, allowing for scheduled runs, dependency management, and integration into a broader data pipeline within Google Cloud Composer.

## 3. Key Design Decisions

*   **Consolidated Logic into BigQuery Stored Procedure:** The entire orchestration logic from the KornShell script (parameter handling, error logging, job tracking) and the core data transformation logic from `d_ausd_v_ta_cntrct_valid.sql` were combined into a single BigQuery Stored Procedure.
    *   **Why:** This approach leverages BigQuery's native capabilities for complex SQL operations, reduces inter-service communication overhead, and simplifies deployment and maintenance by keeping related logic together. It also eliminates the need for separate shell scripting environments.
    *   **Trade-offs:** Requires a complete re-implementation of shell utility functions (e.g., date handling, parameter parsing) in BigQuery SQL. Debugging complex SQL procedures can sometimes be more challenging than step-by-step shell scripts.
*   **Dedicated BigQuery Logging and Job Tracking Tables:** Instead of relying on file-based logs or an external job tracking system, dedicated BigQuery tables (`error_log`, `job_table`, `job_result_log`) were created.
    *   **Why:** Provides centralized, queryable, and scalable logging and job status tracking within the BigQuery ecosystem. This aligns with cloud-native best practices and simplifies monitoring and auditing.
    *   **Trade-offs:** Requires DDL creation and management for these new tables.
*   **Airflow for Orchestration:** The job is designed to be triggered and managed by an Apache Airflow DAG.
    *   **Why:** Airflow (via Cloud Composer) provides robust scheduling, dependency management, retry mechanisms, and monitoring capabilities, which are essential for production ETL workflows. It replaces the basic scheduling and sequential execution of the original KornShell script.
    *   **Trade-offs:** Introduces an additional component (Airflow) to manage and monitor.
*   **Direct BigQuery Table Operations:** The stored procedure directly interacts with BigQuery tables, eliminating the need for external database connections (e.g., SQL*Plus).
    *   **Why:** Simplifies the architecture, removes a dependency on an external database client, and optimizes performance by keeping data processing within BigQuery.
    *   **Trade-offs:** Requires careful translation of any Oracle-specific SQL constructs or functions from the original `d_ausd_v_ta_cntrct_valid.sql` to BigQuery SQL.
*   **Parameter Handling via Stored Procedure Arguments:** Command-line arguments from the KornShell script are mapped directly to `IN` parameters of the BigQuery Stored Procedure.
    *   **Why:** Provides a clear, type-safe interface for passing runtime configuration to the job.
    *   **Trade-offs:** Requires careful mapping and validation logic within the stored procedure.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **DDL Deployment:**
    *   Execute the DDL scripts to create the necessary tables in BigQuery:
        *   `bigquery/ddl/ta_cntrct_valid.sql`
        *   `bigquery/ddl/error_log.sql`
        *   `bigquery/ddl/job_table.sql`
        *   `bigquery/ddl/job_result_log.sql`
    *   *Note:* The `PRIMARY KEY (job_id, entry_number) NOT ENFORCED` in `job_table.sql` is a BigQuery metadata construct and does not enforce uniqueness. Ensure application logic handles potential duplicates if strict uniqueness is required.
3.  **Source Table Availability:**
    *   Verify that the source tables `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_cntrct_validity` exist in BigQuery and contain the expected data. If they are also migrated, ensure their migration is complete and data is loaded.
4.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `bigquery/stored_procedures/bert_k_ausd_v_ta_cntrct_valid.sql` script to create the stored procedure in the target BigQuery dataset.
5.  **IAM Permissions:**
    *   Ensure the service account used by Cloud Composer (or the entity triggering the DAG) has the following BigQuery permissions:
        *   `bigquery.dataEditor` on `project.dataset` (for `ta_cntrct_valid`, `error_log`, `job_table`, `job_result_log`, `dwtk_meldungen`, `cds_ta_cntrct_validity`).
        *   `bigquery.routines.create` and `bigquery.routines.update` (for deploying/updating the stored procedure).
        *   `bigquery.jobs.create` (to run BigQuery jobs).
6.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
7.  **Airflow GCP Connection:**
    *   Verify that the `google_cloud_default` connection is configured correctly in Airflow, pointing to the GCP project where BigQuery resources reside.
8.  **Airflow DAG Deployment:**
    *   Upload `airflow/dags/k_ausd_v_ta_cntrct_valid_dag.py` to the DAGs folder of your Cloud Composer environment.
9.  **Scheduling Configuration:**
    *   Review and set the `schedule_interval` in `k_ausd_v_ta_cntrct_valid_dag.py` to match the desired production schedule.
    *   Adjust the `p_job_kennung` and `p_eintrags_nr` parameters in the `BigQueryExecuteQueryOperator` to appropriate values for your environment (e.g., using Airflow macros for dynamic values).

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or risks during the migration design and require further attention:

*   **Full `d_ausd_v_ta_cntrct_valid.sql` Analysis:** The exact SQL logic within the original `d_ausd_v_ta_cntrct_valid.sql` was not fully available during the design phase. The generated BigQuery Stored Procedure assumes a basic `INSERT INTO ... SELECT` pattern and a `TRUNCATE TABLE` operation. Any complex Oracle-specific features (e.g., PL/SQL blocks, specific functions, hints, DDL operations beyond `TRUNCATE`) would require further translation and testing.
*   **Job Table Schema Details:** The precise schema and usage of the "job table" in the source system were inferred from comments. The `job_table` DDL is a generic representation. It may need adjustments to align with any specific columns or logic present in the original job tracking system.
*   **"Ignoring Active Jobs" and "Deactivating Old Active Jobs" Logic:** The design document noted that the original script's comments suggested logic for ignoring active jobs and deactivating old ones. The generated BigQuery Stored Procedure does *not* explicitly implement this logic, assuming it was handled by the `d_ausd_v_ta_cntrct_valid.sql` or external mechanisms. If this logic is critical and was part of the shell script's orchestration, it needs to be added to the BigQuery Stored Procedure or handled by the Airflow DAG.
*   **`dwtk_meldungen` and `cds_ta_cntrct_validity` Table Schemas:** The DDL for these source tables was not provided. The generated BigQuery Stored Procedure assumes their existence and compatible column names/types (e.g., `timecreated`, `job_kennung`, `cntrct_validity_id`, `insert_at`, `modified_at`). These assumptions must be validated against the actual source schemas.
*   **`file_complexity` Data:** The absence of `file_complexity` data for the original script meant that specific migration challenges or "B4" items (redesign) were not pre-identified. This could imply hidden complexities that may surface during testing.

## 6. Validation

To validate the successful migration and functionality of the `k_ausd_v_ta_cntrct_valid.ksh` job:

1.  **Deploy and Trigger:**
    *   Ensure all DDLs and the BigQuery Stored Procedure are deployed.
    *   Deploy the `k_ausd_v_ta_cntrct_valid_dag.py` to your Cloud Composer environment.
    *   Manually trigger the `k_ausd_v_ta_cntrct_valid_dag` from the Airflow UI.
2.  **Monitor Airflow DAG Run:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully without retries or failures.
3.  **Check BigQuery Job Status:**
    *   Query `project.dataset.job_table` for the `job_id` and `entry_number` corresponding to the triggered run.
    *   **Passing Criteria:** The `status` column should be 'SUCCESS', and `end_timestamp` should be populated.
4.  **Verify Error Logging:**
    *   Query `project.dataset.error_log` for any entries related to the `job_name` 'k_ausd_v_ta_cntrct_valid' for the specific run.
    *   **Passing Criteria:** No new error entries should be present for the successful run.
5.  **Validate Processed Records:**
    *   Query `project.dataset.job_result_log` for the `job_id` and `entry_number`.
    *   **Passing Criteria:** The `records_processed` count should match the expected number of records based on the source system's output for the same processing period.
6.  **Data Integrity Check:**
    *   Query `project.dataset.ta_cntrct_valid`.
    *   **Passing Criteria:**
        *   The table should contain data.
        *   Perform a row count comparison with the source system's output for the same data set.
        *   Sample data from `project.dataset.ta_cntrct_valid` and compare it against the expected output from the original system (if available) to ensure data accuracy and transformation correctness. This may involve comparing specific columns or a checksum of the data.
7.  **Parameter Validation Test:**
    *   Trigger the Airflow DAG with intentionally missing or invalid parameters (if the DAG allows overriding them) to ensure the stored procedure's parameter validation and error logging mechanisms function correctly.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Deactivate New Airflow DAG:**
    *   In the Airflow UI, toggle off the `k_ausd_v_ta_cntrct_valid_dag` to prevent further executions.
    *   Alternatively, remove the DAG file from the Cloud Composer DAGs folder.
2.  **Re-enable Original KornShell Script:**
    *   Revert any changes made to the original scheduling mechanism (e.g., cron job) that disabled `k_ausd_v_ta_cntrct_valid.ksh`.
    *   Ensure the original script is fully operational and can resume processing.
3.  **BigQuery Stored Procedure Rollback (Optional):**
    *   If the `bert_k_ausd_v_ta_cntrct_valid` stored procedure was created as `CREATE OR REPLACE`, and a previous version existed, you might need to revert to a prior version if the new one introduced breaking changes for other consumers (unlikely for a new migration).
    *   If no other systems depend on this specific stored procedure, simply leaving it in place is usually acceptable, as it will no longer be triggered.
4.  **Data Rollback (If Necessary):**
    *   If the migrated job corrupted or incorrectly processed data in `project.dataset.ta_cntrct_valid`, perform a point-in-time recovery or restore from a backup if available. BigQuery's time travel feature can be used to query data from before the problematic run.
    *   *Note:* The current stored procedure includes a `TRUNCATE TABLE` statement. If this was executed incorrectly, the data in `ta_cntrct_valid` would be lost. A robust data recovery plan should be in place.
5.  **Monitor Original System:**
    *   Verify that the original `k_ausd_v_ta_cntrct_valid.ksh` script is running as expected and producing correct output.