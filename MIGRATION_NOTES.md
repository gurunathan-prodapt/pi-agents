# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `k_ausd_v_ta_discount_rr.ksh` KornShell script and its dependent `d_ausd_v_ta_discount_rr.sql` Oracle SQL script to Google Cloud Platform. The migration targets BigQuery for data processing and a BigQuery Stored Procedure for orchestration, with scheduling managed by Cloud Composer (Apache Airflow).

The original job processed and prepared discount-related data by extracting, transforming, and loading information from various Oracle source tables into aggregated or derived target tables. The migrated solution replicates this functionality entirely within the Google Cloud ecosystem, leveraging BigQuery's scalability and managed services.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/ddl/job_table.sql`**:
    *   **Role**: Defines the Data Definition Language (DDL) for the `job_table` in BigQuery. This table is used to track the active status and metadata of job runs, replacing implicit job control mechanisms from the original KornShell script.
*   **`sql/ddl/job_log.sql`**:
    *   **Role**: Defines the DDL for the `job_log` table in BigQuery. This table stores detailed logs of each job execution, including status, messages, and record counts, replacing the KSH script's logging to temporary files and standard output.
*   **`sql/ddl/sof_ta_discount_rr.sql`**:
    *   **Role**: Defines the DDL for the target table `sof_ta_discount_rr` in BigQuery. This table replaces the Oracle `SOF$TA_DISCOUNT_RR` table and includes a `processing_timestamp` column for efficient partitioning and clustering in BigQuery.
*   **`bqsp/sp_ausd_v_ta_discount_rr.sql`**:
    *   **Role**: Contains the BigQuery Stored Procedure `sp_ausd_v_ta_discount_rr`. This procedure encapsulates the entire logic of the original KornShell script (parameter validation, job control, error handling) and the data transformation logic from the Oracle SQL script. It is the core execution unit of the migrated job.
*   **`composer/dags/k_ausd_v_ta_discount_rr_dag.py`**:
    *   **Role**: An Apache Airflow DAG (Directed Acyclic Graph) for Cloud Composer. This Python script is responsible for scheduling and orchestrating the execution of the `sp_ausd_v_ta_discount_rr` BigQuery Stored Procedure, passing the necessary parameters.

## 3. Key Design Decisions

The following key design decisions were made during this migration:

*   **Consolidated Orchestration and Transformation**: The separate KornShell orchestration and Oracle SQL transformation layers were combined into a single BigQuery Stored Procedure (`sp_ausd_v_ta_discount_rr`). This centralizes the logic, simplifies deployment, and leverages BigQuery's native capabilities for both control flow and data manipulation.
*   **Cloud Composer for Scheduling**: Apache Airflow, via Cloud Composer, was chosen for scheduling. This provides a robust, scalable, and feature-rich environment for managing job dependencies, retries, monitoring, and alerting, replacing the traditional cron-based scheduling.
*   **Dedicated BigQuery Tables for Job Control and Logging**: Instead of relying on temporary files and implicit job table updates, explicit BigQuery tables (`job_table`, `job_log`) were created. This provides structured, queryable, and persistent records of job execution, enhancing observability and debugging.
*   **Direct BigQuery Table References**: Oracle database links (`@pcrs1`) and schema prefixes were replaced by direct references to BigQuery tables within the same project/dataset. This eliminates cross-database communication overhead and simplifies data access.
*   **Native BigQuery DML for Oracle Package Functionality**: Oracle-specific package calls (e.g., `DWPA_UTIL_SKRIPT.runstatement` for `TRUNCATE TABLE`) were replaced with native BigQuery DML statements (`TRUNCATE TABLE`), simplifying the code and removing external dependencies.
*   **BigQuery-Optimized Table Design**: Target tables (`sof_ta_discount_rr`, `job_table`, `job_log`) are designed with BigQuery best practices in mind, including partitioning and clustering, to optimize query performance and reduce costs. A `processing_timestamp` column was added to `sof_ta_discount_rr` specifically for partitioning.
*   **Robust Error Handling**: The BigQuery Stored Procedure includes an `EXCEPTION WHEN ERROR` block to catch and log errors, ensuring that job failures are recorded in `job_log` and the `job_table` is updated appropriately, mirroring the error handling intent of the original KSH script.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Setup**: Ensure that the target Google Cloud Project (`[YOUR_GCP_PROJECT_ID]`) is properly set up and has billing enabled.
2.  **BigQuery Dataset Creation**: Create the BigQuery dataset `isrpt_isbert_stage` within `[YOUR_GCP_PROJECT_ID]`.
    ```bash
    bq mk --dataset [YOUR_GCP_PROJECT_ID]:isrpt_isbert_stage
    ```
3.  **Source Data Ingestion**:
    *   Establish data ingestion pipelines to migrate all required Oracle source tables (e.g., `dwtk_meldungen`, `cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `cds_ta_disc_invoice_item`, `zwischen`) into BigQuery.
    *   Ensure these BigQuery tables reside in the `isrpt_isbert_stage` dataset and have schemas compatible with the BigQuery Stored Procedure's expectations.
    *   Verify that historical data is correctly loaded and that date columns (e.g., `insert_at`, `modified_at`, `valid_from`, `valid_to`) are accurately represented in BigQuery.
4.  **Deploy DDLs**: Execute the DDL scripts to create the necessary tables in BigQuery.
    ```bash
    bq query --use_legacy_sql=false < sql/ddl/job_table.sql
    bq query --use_legacy_sql=false < sql/ddl/job_log.sql
    bq query --use_legacy_sql=false < sql/ddl/sof_ta_discount_rr.sql
    ```
5.  **Deploy BigQuery Stored Procedure**: Deploy the `sp_ausd_v_ta_discount_rr` stored procedure to the `isrpt_isbert_stage` dataset.
    ```bash
    bq query --use_legacy_sql=false < bqsp/sp_ausd_v_ta_discount_rr.sql
    ```
6.  **IAM Permissions**: Grant the necessary BigQuery permissions (e.g., `BigQuery Data Editor`, `BigQuery Job User`, `BigQuery Routine Executor`) to the Google Cloud service account associated with your Cloud Composer environment.
7.  **Cloud Composer Environment**: Ensure a Cloud Composer environment is provisioned and running.
8.  **Airflow Connection**: Verify that the `google_cloud_default` Airflow connection exists and is correctly configured to connect to your GCP project.
9.  **Deploy Airflow DAG**: Upload the `composer/dags/k_ausd_v_ta_discount_rr_dag.py` file to the DAGs folder of your Cloud Composer environment.
10. **Parameter Configuration**: Review and adjust the `p_JobKennung` and `p_EintragsNr` parameters within the Airflow DAG (`k_ausd_v_ta_discount_rr_orchestrator`) to align with business requirements. The `p_EintragsNr` is currently templated to `{{ ds_nodash }}` (current date in YYYYMMDD format), which may need adjustment based on its original meaning.
11. **Secrets Management**: If any sensitive parameters (e.g., connection details for external systems, though none are explicitly identified post-migration) are introduced, ensure they are managed securely using Airflow's Secrets Backend or Google Secret Manager.

## 5. Known Gaps & Unresolved References

*   **`starteSQLSkript` Implicit Logic**: The full complexity of the original `starteSQLSkript` helper function in the KSH script is not entirely known. The migration assumes its primary roles were job table updates and temporary file handling. Any other hidden logic or side effects would represent a gap.
*   **Oracle `VIA` Table**: The migration design document mentions `VIA` as a target table in Oracle, but no DDL for it was generated, nor is it explicitly handled in the BigQuery Stored Procedure. This indicates a potential omission in the migration.
*   **Oracle `ZWISCHEN` Table**: The `ZWISCHEN` table is listed as a source in the design document but is not explicitly referenced in the generated BigQuery SQL. Its role and necessity as a source need to be clarified.
*   **`v_records` Calculation Robustness**: The `v_records` calculation in the BigQuery Stored Procedure (`SELECT COUNT(*) FROM ... WHERE DATE(processing_timestamp) = CURRENT_DATE()`) assumes that all records inserted by a single run will have the current date as their `processing_timestamp` and that no other runs for the same job will occur on the same day. This might need refinement for more robust and unique record counting if these assumptions do not hold.
*   **Oracle `INT664` Typo**: In the generated `bqsp/sp_ausd_v_ta_discount_rr.sql`, the declaration `DECLARE v_records INT664 DEFAULT 0;` contains a typo and should be `INT64`. This needs to be corrected manually.
*   **Oracle Hints (`/*+ parallel(...) full(...) */`)**: These performance hints are Oracle-specific and have no direct BigQuery equivalent. While BigQuery automatically handles parallelism, the performance of the migrated query should be carefully monitored to ensure it meets expectations. BigQuery's internal optimization mechanisms and proper table design (partitioning, clustering) are expected to compensate.
*   **`v_carmen = "@pcrs1"` DB Link Implications**: While the DB link is replaced by direct BigQuery table references, the original `pcrs1` source might have specific data consistency or latency requirements that need to be fully understood and replicated in the BigQuery data ingestion strategy.

## 6. Validation

Validation ensures that the migrated job functions correctly and produces accurate results.

### How to Run Tests:

1.  **Unit Test BigQuery Stored Procedure**:
    *   Manually execute the `sp_ausd_v_ta_discount_rr` stored procedure in BigQuery using the BigQuery UI or `bq query` command.
    *   Test with various parameter combinations:
        *   Valid `p_JobKennung` and `p_EintragsNr`.
        *   Invalid/missing `p_JobKennung` or `p_EintragsNr`.
        *   A scenario where a job with the same `p_EintragsNr` is already marked as active in `job_table`.
        *   A scenario where older active jobs need deactivation.
    *   Example execution:
        ```sql
        CALL `[YOUR_GCP_PROJECT_ID].isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('TEST_JOB_KENNUNG', '20231027');
        ```
2.  **Data Validation**:
    *   After a successful run of the BigQuery Stored Procedure, compare the data in `[YOUR_GCP_PROJECT_ID].isrpt_isbert_stage.sof_ta_discount_rr` with the corresponding data in the original Oracle `SOF$TA_DISCOUNT_RR` table.
    *   Perform record count comparisons.
    *   Execute checksums or aggregate queries (e.g., `SUM`, `AVG` on numeric columns) on both BigQuery and Oracle to ensure data integrity.
    *   For a representative subset of data, perform row-by-row comparisons to verify that the transformation logic yields identical results.
3.  **Integration Test with Cloud Composer**:
    *   Trigger the `k_ausd_v_ta_discount_rr_orchestrator` DAG in your Cloud Composer environment.
    *   Monitor the DAG run status and task logs in the Airflow UI.
    *   Observe BigQuery job history for the execution of the stored procedure.

### What "Passing" Means:

*   **Functional Correctness**:
    *   The Cloud Composer DAG completes successfully without any failed tasks.
    *   The `job_log` table in BigQuery records a 'SUCCESS' status for the corresponding job run.
    *   The `job_table` is updated correctly, reflecting the job's active/inactive status.
    *   The `records_processed` count in the `job_log` matches the actual number of records inserted into `sof_ta_discount_rr`.
    *   For error scenarios, the `job_log` correctly records an 'ERROR' status and the `job_table` is updated to inactive.
*   **Data Accuracy**:
    *   Record counts in `sof_ta_discount_rr` in BigQuery are identical to the original Oracle `SOF$TA_DISCOUNT_RR` for the same processing period.
    *   Data values in `sof_ta_discount_rr` are bit-for-bit identical to the Oracle source after transformation, or any expected differences are documented and justified.
*   **Performance**:
    *   The execution time of the BigQuery Stored Procedure and the overall DAG run are within acceptable Service Level Objectives (SLOs).
    *   BigQuery slot consumption and query costs are within expected limits.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, follow this rollback procedure:

1.  **Disable New Job**: Immediately disable the `k_ausd_v_ta_discount_rr_orchestrator` DAG in Cloud Composer to prevent further executions of the migrated job.
2.  **Revert to Original**: Re-enable the original KornShell script (`k_ausd_v_ta_discount_rr.ksh`) on the legacy platform to resume normal operations using the proven system.
3.  **Data Recovery (if necessary)**:
    *   If the `[YOUR_GCP_PROJECT_ID].isrpt_isbert_stage.sof_ta_discount_rr` table in BigQuery was corrupted or incorrectly updated by the migrated job, use BigQuery's time travel feature to restore the table to a state before the problematic execution.
    *   Alternatively, if time travel is insufficient or data integrity is severely compromised, re-ingest the affected data from the original Oracle source.
4.  **Cleanup (Optional)**: If the migration is deemed a complete failure and a full revert is required, you may choose to:
    *   Drop the BigQuery Stored Procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `[YOUR_GCP_PROJECT_ID].isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`;
        ```
    *   Drop the BigQuery tables created for the job:
        ```sql
        DROP TABLE IF EXISTS `[YOUR_GCP_PROJECT_ID].isrpt_isbert_stage.job_table`;
        DROP TABLE IF EXISTS `[YOUR_GCP_PROJECT_ID].isrpt_isbert_stage.job_log`;
        DROP TABLE IF EXISTS `[YOUR_GCP_PROJECT_ID].isrpt_isbert_stage.sof_ta_discount_rr`;
        ```
5.  **Root Cause Analysis**: Conduct a thorough investigation to identify the root cause of the failure. Address all identified issues, update the migration design and code, and re-test rigorously before attempting another migration.