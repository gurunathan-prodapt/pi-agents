# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `k_ausd_v_ta_discount.ksh` job. This job, originally a KornShell script orchestrating an Oracle SQL script (`d_ausd_v_ta_discount.sql`) to populate the `SOF$TA_DISCOUNT` table, has been re-platformed.

The entire ETL workflow, including the shell orchestration and the core SQL data transformation, has been migrated to a Google Cloud Platform (GCP) BigQuery-centric architecture. The KornShell script's control flow is now managed by **Cloud Composer (Apache Airflow)**, and the Oracle SQL transformation logic has been translated into a **BigQuery Stored Procedure**. The target data is stored in a **BigQuery table**.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`ddl/project.dataset.sof_ta_discount.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the target BigQuery table `project.dataset.sof_ta_discount`. This table will store the processed discount data, replacing the original Oracle `SOF$TA_DISCOUNT` table.
*   **`bigquery/stored_procedures/r_ausd_v_ta_discount.sql`**
    *   **Role:** Contains the BigQuery SQL stored procedure that encapsulates the core data transformation logic. This procedure replaces the functionality of the original `d_ausd_v_ta_discount.sql` script, including parameter validation, cutoff date determination, truncation of the target table, and the `INSERT...SELECT` operation with all joins and filters.
*   **`dags/dag_k_ausd_v_ta_discount.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG for Cloud Composer. It orchestrates the execution of the BigQuery stored procedure, handling job scheduling, parameter passing, and integration with GCP's logging and monitoring services. It replaces the control and orchestration logic previously handled by the KornShell script `k_ausd_v_ta_discount.ksh`.

## 3. Key design decisions

*   **Orchestration from KornShell to Cloud Composer:**
    *   **Why:** Cloud Composer provides a fully managed, scalable, and robust orchestration platform, replacing the custom shell scripting environment. It offers better visibility, error handling, dependency management, and integration with other GCP services.
    *   **Trade-offs:** Requires translation of shell logic (parameter parsing, error handling, utility script calls) into Python and Airflow operators. Introduces a new technology stack (Python/Airflow) for job control.
*   **Data Transformation from Oracle SQL to BigQuery Stored Procedure:**
    *   **Why:** Leveraging BigQuery's native capabilities for data warehousing provides significant performance benefits, scalability, and cost efficiency compared to traditional Oracle databases. Stored procedures encapsulate complex SQL logic, making it reusable and maintainable within BigQuery.
    *   **Trade-offs:** Requires manual translation of Oracle-specific SQL functions (e.g., `TO_DATE`, `NVL`, `TRUNCATE TABLE` via `DWPA_UTIL_SKRIPT`) to their BigQuery equivalents. Potential for subtle differences in data type handling or query optimizer behavior.
*   **Centralized Logging and Monitoring:**
    *   **Why:** Replacing file-based logging and `SPOOL` outputs with Cloud Logging and Monitoring provides a unified, scalable, and searchable logging solution, enhancing operational visibility and alerting capabilities.
    *   **Trade-offs:** Requires adaptation to GCP's logging formats and tools.
*   **Source Data Ingestion:**
    *   **Why:** To enable BigQuery to process the data, all source Oracle tables (`dwtk_meldungen`, `cds$ta_discount_bc_assoc`, `cds$ta_discount`, `cds$ta_care_description`, `cds$ta_disc_vector`) must first be ingested into BigQuery. This is a prerequisite for the migration.
    *   **Trade-offs:** Requires establishing separate data pipelines for initial and ongoing ingestion of source data into BigQuery.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery datasets `project.dataset`, `project.isbert_schema`, and `project.source` exist in your GCP project. If not, create them.
2.  **Target Table Creation:**
    *   Execute the DDL script `ddl/project.dataset.sof_ta_discount.sql` in BigQuery to create the `sof_ta_discount` table.
3.  **Source Data Ingestion:**
    *   Verify that all source Oracle tables (`isbert_schema.dwtk_meldungen`, `cds$ta_discount_bc_assoc`, `cds$ta_discount`, `cds$ta_care_description`, `cds$ta_disc_vector`) have been successfully ingested and are available in BigQuery (e.g., in `project.isbert_schema` and `project.source` datasets) with correct schemas and data types.
4.  **BigQuery Stored Procedure Deployment:**
    *   Execute the script `bigquery/stored_procedures/r_ausd_v_ta_discount.sql` in BigQuery to create the stored procedure.
5.  **IAM/Permissions:**
    *   Ensure the Cloud Composer service account has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` (for `project.dataset` to `TRUNCATE` and `INSERT` into `sof_ta_discount`).
        *   `BigQuery Data Viewer` (for `project.isbert_schema` and `project.source` to read source tables).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
6.  **Cloud Composer Environment Setup:**
    *   Upload the `dags/dag_k_ausd_v_ta_discount.py` file to the DAGs folder of your Cloud Composer environment.
7.  **Parameter Configuration:**
    *   Update the placeholder values for `job_kennung` and `eintrags_nr` in `dags/dag_k_ausd_v_ta_discount.py` with the actual required values for the job. These can be configured as Airflow Variables for easier management.
8.  **Scheduling:**
    *   Configure the `schedule_interval` in `dags/dag_k_ausd_v_ta_discount.py` to match the original job's execution frequency.

## 5. Known gaps & unresolved references

The following items are known gaps or require further follow-up:

*   **Job Control Logic ("aktive Jobs werden ignoriert"):** The original KornShell script mentioned ignoring active jobs. The specific implementation of this logic was not fully detailed. The current Cloud Composer DAG does not explicitly replicate this. If this is a critical concurrency control mechanism, it needs to be analyzed and potentially implemented in the DAG (e.g., using Airflow sensors or external state management in BigQuery).
*   **Data Type Mismatches:** While common Oracle functions have BigQuery equivalents, subtle differences in implicit data type conversions between Oracle and BigQuery might exist. Thorough data validation is required to ensure data integrity.
*   **Performance Tuning:** BigQuery automatically optimizes queries, making Oracle-specific hints (`/*+ parallel(...) */`) obsolete. However, the performance characteristics will differ. Monitoring and potential BigQuery-specific optimizations (e.g., partitioning, clustering) might be necessary post-migration.
*   **Error Handling Fidelity:** The original script's error handling (`f_alis_msgerr.ksh`, `WHENEVER SQLERROR EXIT FAILURE`) has been translated to BigQuery's `RAISE` and Airflow's error handling. A comprehensive review is needed to ensure equivalent robustness and alerting.
*   **Source Data Availability:** The migration assumes that all referenced source Oracle tables are fully ingested and kept up-to-date in BigQuery. The reliability and latency of these ingestion pipelines are critical dependencies.
*   **Parameter Values:** The `p_JobKennung` and `p_EintragsNr` parameters in the Airflow DAG are currently placeholders (`"BERT_DISCOUNT_JOB_ID"`, `"12345"`). These must be replaced with the correct, dynamic, or configured values for the production environment.

## 6. Validation

To validate the successful migration and operation of the `k_ausd_v_ta_discount` job:

1.  **Unit Test BigQuery Stored Procedure:**
    *   Execute the `r_ausd_v_ta_discount` stored procedure directly in BigQuery with sample parameters:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_discount`('TEST_JOB', '99999');
        ```
    *   Verify that the `project.dataset.sof_ta_discount` table is truncated and repopulated correctly.
    *   Check the output of the `SELECT` statement at the end of the procedure for `status`, `cutoff_date`, and `records_loaded`.
2.  **Trigger Cloud Composer DAG:**
    *   Manually trigger the `k_ausd_v_ta_discount_migration` DAG from the Airflow UI.
    *   Monitor the DAG run for successful completion of all tasks.
3.  **Data Validation:**
    *   **Record Count:** Compare the number of rows in `project.dataset.sof_ta_discount` after a successful run with the record count from the original Oracle `SOF$TA_DISCOUNT` table for the same data period.
    *   **Data Integrity:** Perform spot checks or full data comparisons between the BigQuery `sof_ta_discount` table and the original Oracle `SOF$TA_DISCOUNT` table to ensure data accuracy and consistency.
    *   **Cutoff Date Logic:** Verify that the `v_datum_date` derived from `project.isbert_schema.dwtk_meldungen` is correctly applied in the filtering conditions.
4.  **Logging and Monitoring:**
    *   Check Cloud Logging for the BigQuery job execution logs and Airflow task logs. Ensure no errors are reported and the final status message (`VERARBEITUNG_FERTIG`) is present.
    *   Confirm that the `records_loaded` count in the logs matches the actual count in the target table.

**"Passing" Criteria:**
*   The `k_ausd_v_ta_discount_migration` DAG completes successfully without any failed tasks.
*   The BigQuery stored procedure `r_ausd_v_ta_discount` executes without errors.
*   The `project.dataset.sof_ta_discount` table is populated with data.
*   The record count in `project.dataset.sof_ta_discount` is consistent with the expected volume from the source.
*   Data samples from `project.dataset.sof_ta_discount` match the corresponding data in the original Oracle `SOF$TA_DISCOUNT` table, confirming transformation accuracy.
*   All relevant logs in Cloud Logging indicate successful completion and expected output.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Deactivate Cloud Composer DAG:**
    *   Immediately pause or unschedule the `k_ausd_v_ta_discount_migration` DAG in the Airflow UI to prevent further executions.
2.  **Re-enable Original Job:**
    *   Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh` job in the legacy environment.
3.  **Data Restoration (if necessary):**
    *   If the `project.dataset.sof_ta_discount` table in BigQuery was corrupted or populated incorrectly, use BigQuery's time travel feature to restore the table to a state before the problematic migration run.
    *   Alternatively, if time travel is insufficient or not configured, restore the table from a backup if available.
4.  **Cleanup (Optional):**
    *   If the rollback is deemed permanent, the BigQuery stored procedure `r_ausd_v_ta_discount` and the `project.dataset.sof_ta_discount` table can be dropped. The DAG file can be removed from the Composer environment.

**Note:** The success of the rollback procedure relies on the continued availability and operational status of the legacy environment and its data.