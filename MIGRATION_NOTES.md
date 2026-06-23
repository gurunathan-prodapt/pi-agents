# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` job. The original job, responsible for enriching discount data with contract and template details, was orchestrated by UC4 and KornShell scripts, executing an Oracle SQL*Plus script to populate the `sof$ta_p_discount_rr` table.

The job has been migrated to a Google Cloud Platform (GCP) centric solution. Orchestration is now handled by Apache Airflow (Cloud Composer), and all data storage and transformation logic are executed within Google BigQuery.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`sql/ddl/sof_ta_p_discount_rr.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the target table `sof_ta_p_discount_rr`. This table stores the enriched discount data.
*   **`sql/ddl/sof_ta_discount_rr.sql`**
    *   **Role:** BigQuery DDL script for the source table `sof_ta_discount_rr`. This table is expected to be populated via an Oracle-to-BigQuery data ingestion pipeline.
*   **`sql/ddl/sof_ta_cntrct_crs.sql`**
    *   **Role:** BigQuery DDL script for the source table `sof_ta_cntrct_crs`. This table is expected to be populated via an Oracle-to-BigQuery data ingestion pipeline.
*   **`sql/ddl/sof_ta_cntrct_templ.sql`**
    *   **Role:** BigQuery DDL script for the source table `sof_ta_cntrct_templ`. This table is expected to be populated via an Oracle-to-BigQuery data ingestion pipeline.
*   **`sql/ddl/dwtk_meldungen.sql`**
    *   **Role:** BigQuery DDL script for a metadata/logging table `dwtk_meldungen`. This table replaces the functionality of the original Oracle `isbert_schema.dwtk_meldungen` table, primarily for tracking job-related information.
*   **`sql/bq_d_ausd_v_ta_p_discount_rr.sql`**
    *   **Role:** BigQuery SQL script containing the core data transformation logic. It truncates the target table and inserts enriched data by joining the source tables, directly translating the logic from the original Oracle SQL*Plus script.
*   **`dags/dw_bert_ausd_v_ta_p_discount_rr.py`**
    *   **Role:** Apache Airflow DAG (Directed Acyclic Graph) definition file. This Python script orchestrates the execution of the BigQuery SQL transformation, replacing the UC4 job and KornShell wrapper scripts.

## 3. Key design decisions

*   **Cloud-Native Orchestration:** Apache Airflow (via Cloud Composer) was chosen to replace UC4 and KornShell scripts. This provides a scalable, managed, and Python-extensible orchestration platform aligned with GCP best practices, offering robust scheduling, monitoring, and error handling capabilities.
*   **BigQuery for Data Transformation:** Google BigQuery was selected as the primary data warehouse and transformation engine. This leverages BigQuery's serverless architecture, high performance for analytical queries, and cost-effectiveness, replacing the Oracle SQL*Plus execution.
*   **Consolidated Transformation Logic:** The multiple KornShell scripts (`r_ausd_v_ta_p_discount_rr.ksh`, `k_ausd_v_ta_p_discount_rr.ksh`) that handled environment setup, parameter passing, and error trapping have been absorbed into the Airflow DAG's structure and Python operators. This streamlines the workflow and reduces script sprawl.
*   **Direct SQL Translation:** The core Oracle SQL*Plus logic (`d_ausd_v_ta_p_discount_rr.sql`) was directly translated into BigQuery SQL. This minimizes changes to the business logic while adapting to BigQuery's syntax and features (e.g., removing Oracle-specific hints like `/*+ parallel(...) */`).
*   **Data Replication Strategy:** It is assumed that all necessary Oracle source tables (`sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `isbert_schema.dwtk_meldungen`) are replicated into BigQuery. This decision simplifies the BigQuery SQL by allowing direct table references and avoids the complexity and potential performance overhead of federated queries to external Oracle databases.
*   **Idempotent Target Table Handling:** The original `TRUNCATE TABLE` followed by `INSERT INTO` pattern for the target table `sof$ta_p_discount_rr` was maintained in the BigQuery SQL. This ensures that each successful run of the job completely refreshes the target table, making the operation idempotent.
*   **Standardized Logging and Metadata:** The `isbert_schema.dwtk_meldungen` table, used for dynamic date calculation and logging, has been replaced by a corresponding BigQuery table (`dwtk_meldungen`). This centralizes metadata within the BigQuery ecosystem.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure the target GCP project (`your-gcp-project-id`) and BigQuery dataset (`your_bigquery_dataset`) are created and configured.
2.  **BigQuery Table Creation:**
    *   Execute all DDL scripts located in `sql/ddl/` (i.e., `sof_ta_p_discount_rr.sql`, `sof_ta_discount_rr.sql`, `sof_ta_cntrct_crs.sql`, `sof_ta_cntrct_templ.sql`, `dwtk_meldungen.sql`) in your target BigQuery dataset.
3.  **Data Ingestion Pipeline Setup:**
    *   Establish and verify the data ingestion pipeline(s) responsible for replicating data from the source Oracle tables (`sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `isbert_schema.dwtk_meldungen`) into their respective BigQuery counterparts. This is a critical prerequisite for the Airflow DAG to function correctly.
4.  **Airflow Environment Configuration:**
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured to allow the Airflow service account to interact with BigQuery.
5.  **IAM Permissions:**
    *   Grant the Cloud Composer service account (or the service account used by the Airflow workers) the necessary IAM roles for BigQuery operations, including at least `BigQuery Data Editor` (for `TRUNCATE` and `INSERT`) and `BigQuery Job User` (to run queries). Additionally, `Storage Object Viewer` is needed for Airflow to read DAG files from the GCS bucket.
6.  **DAG Deployment:**
    *   Upload the `dags/dw_bert_ausd_v_ta_p_discount_rr.py` file to the DAGs folder of your Cloud Composer environment's GCS bucket.
7.  **DAG Configuration Review:**
    *   Review and adjust the following placeholders and configurations within `dags/dw_bert_ausd_v_ta_p_discount_rr.py`:
        *   `PROJECT_ID = 'your-gcp-project-id'`
        *   `DATASET_ID = 'your_bigquery_dataset'`
        *   `email_on_failure = ['your_email@example.com']`
        *   `schedule_interval='@daily'` (adjust to match the original UC4 schedule)
        *   `start_date=datetime(2023, 1, 1)` (adjust to a suitable historical or current date)

## 5. Known gaps & unresolved references

*   **Placeholder Values:** The generated code contains placeholders that *must* be replaced with actual values:
    *   `your-gcp-project-id` (GCP Project ID)
    *   `your_bigquery_dataset` (BigQuery Dataset ID)
    *   `your_email@example.com` (Email address for Airflow alerts)
*   **Scheduling:** The `schedule_interval` in the Airflow DAG is set to `'@daily'` as a default. This needs to be finalized based on the exact scheduling requirements of the original UC4 job.
*   **Start Date:** The `start_date` in the Airflow DAG is set to `datetime(2023, 1, 1)`. This should be adjusted to reflect the actual deployment date or the desired historical start for the DAG.
*   **Data Ingestion Pipeline Implementation:** While the DDLs for source tables are provided, the actual implementation and configuration of the Oracle-to-BigQuery data ingestion pipeline (e.g., using Datastream, Dataflow, or a third-party tool) is outside the scope of this migration and must be established separately.
*   **`v_datum` Logic Discrepancy:** The original Oracle SQL script used a dynamic `v_datum` derived from `isbert_schema.dwtk_meldungen`. The generated BigQuery SQL (`bq_d_ausd_v_ta_p_discount_rr.sql`) does *not* currently incorporate this `v_datum` logic. If this dynamic date filtering is still a requirement for the transformation, it needs to be explicitly added to the BigQuery SQL script.
*   **Missing Metadata:** The original `file_complexity` and `complexity_signals` were not available, which means the "semi_auto" classification is general. Further manual review might be needed for any hidden complexities.
*   **Oracle to BigQuery Type Mapping:** While standard types are generally compatible, subtle differences in data type handling or implicit conversions between Oracle and BigQuery could lead to unexpected behavior. Thorough testing is required.
*   **Performance Optimization:** The original Oracle SQL used `parallel` hints. BigQuery handles parallelism automatically, but initial performance should be monitored, and queries optimized if necessary.

## 6. Validation

To validate the successful migration and operation of the `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` job:

1.  **Verify Source Data Availability:**
    *   Confirm that all required source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`, `dwtk_meldungen`) in BigQuery are populated, up-to-date, and contain the expected data from their Oracle counterparts.
2.  **Trigger Airflow DAG:**
    *   Manually trigger the `dw_bert_ausd_v_ta_p_discount_rr` DAG in the Airflow UI.
3.  **Monitor Execution:**
    *   Observe the DAG run in the Airflow UI, ensuring all tasks (`start_task`, `main_data_processing`, `post_processing_log`, `end_task`) complete successfully without errors.
    *   Check Airflow task logs and Cloud Logging for any warnings or errors.
4.  **Data Validation in BigQuery:**
    *   Query the target table `your-gcp-project-id.your_bigquery_dataset.sof_ta_p_discount_rr` in BigQuery.
    *   **Row Count Comparison:** Compare the row count of the BigQuery target table with the row count of the original Oracle `sof$ta_p_discount_rr` table after a successful run. They should match.
    *   **Data Sample Verification:** Select a sample of records from the BigQuery target table and compare them against the corresponding records in the Oracle source. Pay close attention to the `contract_number` and `std_vertrag` fields to ensure the enrichment logic is correct.
    *   **Data Integrity:** Check for any NULLs in critical fields, unexpected data types, or truncation issues.
    *   **Idempotency Test:** Run the DAG multiple times to ensure that the `TRUNCATE` and `INSERT` logic consistently produces the same correct output without data duplication or corruption.

**"Passing" means:**
*   The Airflow DAG completes successfully without any failed tasks.
*   The target BigQuery table `sof_ta_p_discount_rr` is populated with data.
*   The row count in `sof_ta_p_discount_rr` matches the expected count from the source system.
*   A sample of records in `sof_ta_p_discount_rr` accurately reflects the transformation logic and matches the expected output based on the original system.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after the migration, follow these steps to roll back to the original system:

1.  **Disable New Job:**
    *   Immediately pause or delete the `dw_bert_ausd_v_ta_p_discount_rr` Airflow DAG in the Cloud Composer UI to prevent further execution.
2.  **Re-enable Original Job:**
    *   Re-activate the original UC4 job `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` in the legacy environment.
3.  **Verify Original Job Functionality:**
    *   Monitor the re-enabled UC4 job to ensure it runs successfully and populates the Oracle `sof$ta_p_discount_rr` table as expected.
4.  **Data Restoration (if necessary):**
    *   If the BigQuery migration caused any data inconsistencies or corruption in the target table (`sof$ta_p_discount_rr` in Oracle), restore the table from a backup taken prior to the migration cutover, or re-run the original UC4 job to repopulate it with correct data.
5.  **Post-Rollback Analysis:**
    *   Investigate the root cause of the rollback to address the issues before attempting re-migration.