# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `r_ausd_v_ta_p_discount.ksh` job, originally a KornShell-orchestrated Oracle SQL process, to Google Cloud's BigQuery platform. The job's primary function is to reconcile and update contract discount data into the `sof$ta_p_discount` table, deriving processing dates from a control table.

The migration involved re-platforming the entire ETL workflow:
*   **Source Platform:** KornShell scripts (`r_ausd_v_ta_p_discount.ksh`, `k_ausd_v_ta_p_discount.ksh`), Oracle SQL (`d_ausd_v_ta_p_discount.sql`), and Oracle Database.
*   **Target Platform:** Google BigQuery, leveraging BigQuery Stored Procedures for core logic and data transformation, with orchestration managed by Google Cloud Composer (Airflow). All source and target tables are migrated to BigQuery.

## 2. Generated Artifacts

The following artifacts were generated as part of this migration:

*   **`create_table_sof_ta_p_discount.sql`**
    *   **Role:** BigQuery DDL (Data Definition Language) script to create the target table `project.dataset.sof_ta_p_discount`. This table replaces the Oracle `sof$ta_p_discount` table.
*   **`create_table_sof_ta_disc_zusgf.sql`**
    *   **Role:** BigQuery DDL script to create the source table `project.dataset.sof_ta_disc_zusgf`. This table replaces the Oracle `sof$ta_disc_zusgf` table.
*   **`create_table_sof_ta_cntrct_crs.sql`**
    *   **Role:** BigQuery DDL script to create the source table `project.dataset.sof_ta_cntrct_crs`. This table replaces the Oracle `sof$ta_cntrct_crs` table.
*   **`create_table_dwtk_meldungen.sql`**
    *   **Role:** BigQuery DDL script to create the control table `project.dataset.dwtk_meldungen`. This table replaces the Oracle `dwtk_meldungen` table, used for deriving processing dates.
*   **`create_table_job_log.sql`**
    *   **Role:** BigQuery DDL script to create a dedicated logging table `project.dataset.job_log`. This table captures job start/end times, status, messages, and records processed, replacing the file-based logging of the original KornShell scripts.
*   **`create_table_job_error_log.sql`**
    *   **Role:** BigQuery DDL script to create a dedicated error logging table `project.dataset.job_error_log`. This table captures detailed error information, replacing the error handling and logging mechanisms of the original KornShell utilities.
*   **`r_ausd_v_ta_p_discount.sql`**
    *   **Role:** BigQuery Stored Procedure. This is the core migrated component, encapsulating the orchestration logic from `r_ausd_v_ta_p_discount.ksh` and `k_ausd_v_ta_p_discount.ksh`, and the data transformation logic from `d_ausd_v_ta_p_discount.sql`. It handles parameter processing, date derivation, target table truncation, data insertion, and integrated logging/error handling.
*   **`airflow_dag_r_ausd_v_ta_p_discount.py`**
    *   **Role:** Python script defining an Apache Airflow DAG (for Cloud Composer). This DAG is responsible for scheduling and invoking the `r_ausd_v_ta_p_discount` BigQuery Stored Procedure, passing necessary parameters.

## 3. Key Design Decisions

### 3.1. Orchestration Consolidation
*   **Decision:** The multiple KornShell scripts (`r_ausd_v_ta_p_discount.ksh`, `k_ausd_v_ta_p_discount.ksh`) were consolidated into a single BigQuery Stored Procedure (`r_ausd_v_ta_p_discount.sql`). External scheduling is handled by an Airflow DAG.
*   **Rationale:** This approach leverages BigQuery's native capabilities for sequential logic and error handling, reducing the overhead of managing shell scripts and external SQL*Plus calls. It simplifies deployment and monitoring within the Google Cloud ecosystem.
*   **Trade-offs:** While simplifying the core execution, it shifts the orchestration complexity to Airflow for scheduling and parameter management. It also means less granular control over individual steps from an external shell perspective.

### 3.2. Native BigQuery SQL Transformation
*   **Decision:** The Oracle SQL logic from `d_ausd_v_ta_p_discount.sql` was directly translated to BigQuery Standard SQL and embedded within the BigQuery Stored Procedure.
*   **Rationale:** BigQuery Standard SQL offers robust features for data transformation, and direct translation minimizes the need for intermediate processing layers. BigQuery's automatic parallelism eliminates the need for Oracle-specific hints (`/*+ parallel(...) */`).
*   **Trade-offs:** Requires careful review of Oracle-specific functions (`NVL`, `TO_CHAR`) to ensure accurate BigQuery equivalents (`COALESCE`, `FORMAT_DATE`) are used and produce identical results.

### 3.3. Replacement of KornShell Utilities
*   **Decision:** The custom KornShell utility framework (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) was replaced by BigQuery's native features and Cloud services.
    *   Error handling and logging are now managed by `EXCEPTION WHEN ERROR` blocks within the Stored Procedure, writing to dedicated BigQuery logging tables (`job_log`, `job_error_log`), and integrating with Cloud Logging.
    *   Parameter handling is done via Stored Procedure input parameters.
    *   Date utilities are replaced by BigQuery's extensive date/time functions.
    *   SQL*Plus execution is no longer required as SQL runs natively.
*   **Rationale:** This eliminates a significant dependency on a legacy shell environment, modernizes the logging and error reporting, and aligns with cloud-native best practices.
*   **Trade-offs:** Requires a complete re-implementation of these functionalities, ensuring all original behaviors (e.g., specific error codes, log formats) are replicated or adequately replaced.

### 3.4. Data Storage Migration
*   **Decision:** All Oracle source and target tables (`sof$ta_disc_zusgf`, `sof$ta_cntrct_crs`, `dwtk_meldungen`, `sof$ta_p_discount`) are migrated to BigQuery tables.
*   **Rationale:** This provides a fully integrated, high-performance data warehousing solution within Google Cloud, eliminating cross-platform data access complexities and potential performance bottlenecks.
*   **Trade-offs:** Requires a robust data ingestion strategy (one-time historical load and ongoing CDC/replication) from Oracle to BigQuery, which is a significant undertaking outside the scope of this specific job migration.

### 3.5. Handling Oracle-Specific Constructs
*   **Decision:** Oracle-specific constructs like `TRUNCATE` via `DWPA_UTIL_SKRIPT.runstatement`, `NVL`, `TO_CHAR`, `/*+ parallel */` hints, `@@row_count` capture, `spool`, and `trace.sql.cfg` were replaced with their BigQuery equivalents or cloud-native solutions.
*   **Rationale:** Ensures the migrated code is idiomatic BigQuery Standard SQL and leverages BigQuery's optimized execution engine.
*   **Trade-offs:** Requires careful mapping and testing to ensure functional equivalence, especially for data type conversions and date formatting.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the environment and deploy the migrated job:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_bigquery_dataset` in `project.dataset`) exists in your GCP project (`your-gcp-project-id`). If not, create it.
2.  **BigQuery Table Creation (DDLs):**
    *   Execute the following DDL scripts in BigQuery to create the necessary tables:
        *   `create_table_sof_ta_p_discount.sql`
        *   `create_table_sof_ta_disc_zusgf.sql`
        *   `create_table_sof_ta_cntrct_crs.sql`
        *   `create_table_dwtk_meldungen.sql`
        *   `create_table_job_log.sql`
        *   `create_table_job_error_log.sql`
    *   **Note on Data Types:** The DDLs use `STRING` for all columns as a generic starting point. Review the actual Oracle data types and adjust the BigQuery DDLs (e.g., to `INT64`, `BIGNUMERIC`, `TIMESTAMP`, `DATE`) for optimal storage and performance *before* data ingestion.
3.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `r_ausd_v_ta_p_discount.sql` script in BigQuery to create the Stored Procedure.
4.  **Data Ingestion:**
    *   **Critical Step:** Establish and execute a data ingestion pipeline to migrate historical and ongoing data from the Oracle source tables (`sof$ta_disc_zusgf`, `sof$ta_cntrct_crs`, `dwtk_meldungen`) to their respective BigQuery tables (`project.dataset.sof_ta_disc_zusgf`, `project.dataset.sof_ta_cntrct_crs`, `project.dataset.dwtk_meldungen`). This pipeline must be operational and synchronized before the migrated job can run correctly.
5.  **IAM/Permissions:**
    *   Grant the necessary BigQuery permissions to the service account that will execute the Airflow DAG (or directly call the Stored Procedure). This typically includes:
        *   `BigQuery Data Editor` (for `project.dataset.sof_ta_p_discount`, `project.dataset.job_log`, `project.dataset.job_error_log`)
        *   `BigQuery Data Viewer` (for `project.dataset.sof_ta_disc_zusgf`, `project.dataset.sof_ta_cntrct_crs`, `project.dataset.dwtk_meldungen`)
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures)
6.  **Cloud Composer/Airflow Configuration:**
    *   Deploy the `airflow_dag_r_ausd_v_ta_p_discount.py` file to your Cloud Composer environment's DAGs folder.
    *   Ensure the `google_cloud_default` connection is correctly configured in Airflow.
    *   **Parameter Values:** Update `PROJECT_ID` and `DATASET_ID` in the DAG file. Determine the correct values for `p_job_kennung` and `p_eintrags_nr` parameters based on the original job's context and update the `parameters` dictionary in the DAG.
    *   **Scheduling:** Configure the `schedule` parameter in the DAG to match the desired execution frequency of the original job.
7.  **Secrets Management (if applicable):**
    *   If any sensitive parameters or configuration values were previously stored in environment variables or files, ensure they are securely managed in Google Cloud (e.g., using Secret Manager) and passed to the Airflow DAG or BigQuery Stored Procedure as needed.

## 5. Known Gaps & Unresolved References

1.  **Oracle DB-Link (`@pcrs1`) Context:** The original design document mentions `DEFINE v_carmen = "@pcrs1"`. While the generated BigQuery code assumes `sof_ta_disc_zusgf` and `sof_ta_cntrct_crs` are local BigQuery tables, it's crucial to confirm if `pcrs1` was a source for *these specific tables* or other data. If data for these tables originated from `pcrs1`, then the data ingestion strategy must include `pcrs1` as a source. This is a potential data lineage gap that needs explicit confirmation.
2.  **Data Type Precision:** The generated DDLs for BigQuery tables use `STRING` for all columns. This is a placeholder. A thorough review of the original Oracle table schemas is required to map precise data types (e.g., `NUMBER(p,s)` to `BIGNUMERIC`, `DATE` to `DATE`, `TIMESTAMP` to `TIMESTAMP`) to BigQuery for optimal storage, performance, and data integrity. This was flagged as a B4 item in the design.
3.  **`file_complexity` and `automation_rate`:** As noted in the design document, these attributes were inferred. While unlikely to impact functionality, any future analysis relying on these metrics should re-evaluate them based on the migrated solution.
4.  **`trace.sql.cfg` Equivalence:** While BigQuery's query history and Cloud Logging provide extensive tracing capabilities, if `trace.sql.cfg` enabled very specific, custom tracing behaviors, a direct functional equivalent might require further investigation or custom logging within the Stored Procedure.
5.  **Environment Variable Mapping:** The original KornShell scripts relied heavily on environment variables (`$HOME/.dw_init`, `BERT_DIR_ROOT`). While the core logic is now in BigQuery, any remaining configuration or path resolution that was dependent on these variables in the broader ecosystem needs to be identified and mapped to BigQuery parameters, Airflow variables, or GCP environment variables/secrets.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

### 6.1. How to Run Tests

1.  **Data Preparation:**
    *   Ensure the BigQuery source tables (`sof_ta_disc_zusgf`, `sof_ta_cntrct_crs`, `dwtk_meldungen`) are populated with representative test data, ideally a snapshot of production data from the Oracle system.
    *   Ensure the target table `sof_ta_p_discount` is empty or contains known test data that will be truncated.
2.  **Manual Stored Procedure Execution (Unit Test):**
    *   Execute the BigQuery Stored Procedure directly in the BigQuery console or via the `bq` command-line tool:
        ```sql
        CALL `project.dataset.r_ausd_v_ta_p_discount`('TEST_JOB_KENNUNG', '123');
        ```
    *   Replace `'TEST_JOB_KENNUNG'` and `'123'` with appropriate test values.
3.  **Airflow DAG Execution (Integration Test):**
    *   Trigger the `r_ausd_v_ta_p_discount_dag` manually from the Airflow UI in Cloud Composer.
    *   Monitor the DAG run in the Airflow UI for task success/failure.
4.  **Logging and Monitoring:**
    *   After each execution, query the `project.dataset.job_log` and `project.dataset.job_error_log` tables to check for job status and any recorded errors.
    *   Review Cloud Logging for BigQuery job logs and any Airflow worker logs.

### 6.2. What "Passing" Means

A successful validation indicates the migrated job is functioning as expected:

1.  **Successful Execution:**
    *   The BigQuery Stored Procedure completes without raising unhandled exceptions.
    *   The Airflow DAG run completes successfully, with all tasks marked as "success."
2.  **Accurate Logging:**
    *   The `project.dataset.job_log` table contains an entry for the job run with `status = 'COMPLETED'`.
    *   The `records_processed` count in `job_log` matches the expected number of rows inserted.
    *   There are no new entries in the `project.dataset.job_error_log` table for the job run.
3.  **Data Integrity and Accuracy:**
    *   **Row Count Comparison:** The number of rows in `project.dataset.sof_ta_p_discount` after the job run matches the number of rows in the original Oracle `sof$ta_p_discount` table when run with identical source data.
    *   **Data Content Comparison:** A row-by-row comparison (e.g., using checksums, hash values, or direct `SELECT *` comparisons) confirms that the data in `project.dataset.sof_ta_p_discount` is identical to the output of the legacy Oracle job for the same input. This is the most critical validation step.
    *   **Processing Date:** Verify that the `v_datum` derived from `dwtk_meldungen` is correct and matches the expected date from the legacy system.
4.  **Performance:**
    *   The BigQuery job completes within acceptable timeframes and consumes resources (slot usage) efficiently.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Immediate Action:**
    *   **Stop New Job:** Immediately pause or disable the `r_ausd_v_ta_p_discount_dag` in Cloud Composer/Airflow to prevent further execution of the migrated job.
    *   **Re-enable Legacy Job:** Re-enable the original KornShell job (`r_ausd_v_ta_p_discount.ksh`) in its legacy environment.
2.  **Data Recovery (if necessary):**
    *   The migrated job performs a `TRUNCATE TABLE` on `project.dataset.sof_ta_p_discount` before inserting data. If the rollback is due to data corruption or incorrect data in this target table, a full reload of `project.dataset.sof_ta_p_discount` from a known good backup or by re-running the legacy Oracle job (if its output can be ingested) might be required to restore data integrity in BigQuery.
    *   If the legacy Oracle `sof$ta_p_discount` table was also affected (e.g., by a data ingestion issue), restore it from the most recent valid backup.
3.  **Code Reversion:**
    *   **BigQuery:** Delete the `project.dataset.r_ausd_v_ta_p_discount` Stored Procedure.
    *   **Airflow:** Remove the `airflow_dag_r_ausd_v_ta_p_discount.py` file from the Cloud Composer DAGs folder.
    *   **BigQuery Tables (Optional):** If the BigQuery tables were created solely for this migration and are not used by other processes, they can be dropped. However, if data ingestion is ongoing, these tables might need to remain for other purposes.
4.  **Data Ingestion Re-evaluation:**
    *   Review the Oracle-to-BigQuery data ingestion pipeline. If the issue was related to data quality or synchronization, this pipeline may need to be paused, reconfigured, or restarted.
5.  **Post-Rollback Verification:**
    *   Confirm that the legacy job is running successfully and producing correct results in the Oracle environment.
    *   Verify the state of the BigQuery tables, ensuring no unintended data changes persist.