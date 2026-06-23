# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_cntrct_valid.ksh` job. The original job, consisting of a KornShell script (`k_ausd_v_ta_cntrct_valid.ksh`) and an Oracle SQL script (`d_ausd_v_ta_cntrct_valid.sql`), was responsible for orchestrating the extraction and loading of contract validity data.

The job has been migrated to the Google Cloud Platform, leveraging **BigQuery** for data storage and processing, and **Cloud Composer (Apache Airflow)** for orchestration. The core ETL logic, previously split between shell and Oracle SQL, is now consolidated into a single BigQuery SQL stored procedure.

## 2. Generated Artifacts

The migration produced the following files:

*   **`bq_ddl_sof_ta_cntrct_valid.sql`**
    *   **Role:** This SQL script defines the Data Definition Language (DDL) for the target BigQuery tables. It creates the `project.dataset.sof_ta_cntrct_valid` table, which serves as the final destination for the processed contract validity data. Optionally, it also creates `project.dataset.job_audit_log` for custom auditing purposes. It ensures the necessary BigQuery datasets (`project.dataset`, `project.isbert_schema`, `project.source_dataset`) exist.
*   **`bq_sp_r_ausd_vertrag.sql`**
    *   **Role:** This BigQuery SQL script defines the `project.dataset.r_ausd_vertrag` stored procedure. This procedure encapsulates the entire ETL logic:
        *   Determining the cutoff date (`v_datum`) by querying `project.isbert_schema.dwtk_meldungen`.
        *   Truncating the target table `project.dataset.sof_ta_cntrct_valid`.
        *   Extracting and loading data from `project.source_dataset.cds_ta_cntrct_validity` into the target table, applying the specified date filters and column mappings.
        *   Counting loaded records and logging audit information into `project.dataset.job_audit_log`.
        *   Implementing error handling and logging.
*   **`composer_dag_k_ausd_v_ta_cntrct_valid.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG (Directed Acyclic Graph) for Cloud Composer. Its purpose is to orchestrate the execution of the BigQuery stored procedure. It defines the DAG's schedule, parameters (`p_JobKennung`, `p_EintragsNr`), and uses the `BigQueryExecuteStoredProcedureOperator` to invoke `project.dataset.r_ausd_vertrag` in BigQuery.

## 3. Key Design Decisions

*   **Consolidation into BigQuery Stored Procedure:**
    *   **Decision:** The orchestration logic from the KornShell script and the data manipulation logic from the Oracle SQL script were combined into a single BigQuery stored procedure (`r_ausd_vertrag`).
    *   **Rationale:** This approach simplifies deployment, reduces the number of components, and leverages BigQuery's native capabilities for data processing. It eliminates the need for external shell scripts and their associated environment management.
    *   **Trade-offs:** For extremely complex shell logic, this might lead to a less readable or maintainable stored procedure. However, for this job's scope (primarily orchestration and SQL execution), it's an efficient solution.
*   **Cloud Composer (Apache Airflow) for Orchestration:**
    *   **Decision:** Cloud Composer was chosen to replace the KornShell script's role in scheduling, parameter passing, and error handling.
    *   **Rationale:** Provides a managed, robust, and scalable orchestration platform with built-in features for scheduling, dependency management, monitoring, and logging, integrating seamlessly with other GCP services.
*   **Native BigQuery Tables for Source Data:**
    *   **Decision:** The Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_validity`) are assumed to be ingested into native BigQuery tables (`project.isbert_schema.dwtk_meldungen`, `project.source_dataset.cds_ta_cntrct_validity`).
    *   **Rationale:** This ensures optimal query performance and cost-efficiency within BigQuery, avoiding the overhead and potential performance issues of federated queries to external databases.
    *   **Trade-offs:** Requires establishing and maintaining separate data ingestion pipelines from Oracle to BigQuery.
*   **Direct `TRUNCATE TABLE`:**
    *   **Decision:** The Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` procedure for truncating the table was replaced with a direct `TRUNCATE TABLE` statement within the BigQuery stored procedure.
    *   **Rationale:** Utilizes BigQuery's native DDL capabilities, simplifying the code and removing an external dependency.
*   **`job_audit_log` Table for Auditing:**
    *   **Decision:** An optional `job_audit_log` table was introduced in BigQuery to capture execution details, record counts, and status.
    *   **Rationale:** Provides a structured and queryable audit trail, replacing the basic logging and spool file outputs of the original shell script.
*   **Parameterization via Airflow Variables:**
    *   **Decision:** Job-specific parameters (`p_JobKennung`, `p_EintragsNr`) are passed to the BigQuery stored procedure via Airflow Variables.
    *   **Rationale:** Centralizes configuration, allows for easy modification without code changes, and provides flexibility for different environments or job instances.

## 4. Manual Steps Before Go-Live

The following manual steps are required before the migrated job can be put into production:

1.  **BigQuery Dataset Creation:**
    *   Ensure the following BigQuery datasets exist in your GCP project:
        *   `project.dataset` (e.g., `your-gcp-project-id.dataset`)
        *   `project.isbert_schema` (e.g., `your-gcp-project-id.isbert_schema`)
        *   `project.source_dataset` (e.g., `your-gcp-project-id.source_dataset`)
    *   *Note: The `bq_ddl_sof_ta_cntrct_valid.sql` script includes `CREATE SCHEMA IF NOT EXISTS` statements, but it's good practice to ensure these are set up with appropriate permissions beforehand.*
2.  **IAM Permissions:**
    *   The service account associated with your Cloud Composer environment must have the following BigQuery roles:
        *   `BigQuery Data Editor` on `project.dataset` (to create/truncate/insert into `sof_ta_cntrct_valid` and `job_audit_log`).
        *   `BigQuery Data Viewer` on `project.isbert_schema` (to read `dwtk_meldungen`).
        *   `BigQuery Data Viewer` on `project.source_dataset` (to read `cds_ta_cntrct_validity`).
        *   `BigQuery Job User` (to run BigQuery jobs).
3.  **Source Data Ingestion Pipelines:**
    *   **Crucial Step:** Establish and configure batch data transfer pipelines to ingest data from the Oracle source system into BigQuery:
        *   Oracle `isbert_schema.dwtk_meldungen` -> BigQuery `project.isbert_schema.dwtk_meldungen`
        *   Oracle `cds$ta_cntrct_validity` -> BigQuery `project.source_dataset.cds_ta_cntrct_validity`
    *   The frequency and method (e.g., Cloud Data Fusion, Dataflow, custom ETL) for these pipelines must be determined and implemented.
4.  **BigQuery Table and Stored Procedure Deployment:**
    *   Execute `bq_ddl_sof_ta_cntrct_valid.sql` to create the target tables.
    *   Execute `bq_sp_r_ausd_vertrag.sql` to create the BigQuery stored procedure.
5.  **Airflow Connection Configuration:**
    *   Ensure the `google_cloud_default` connection is properly configured in your Cloud Composer environment. This connection is used by the `BigQueryExecuteStoredProcedureOperator`.
6.  **Airflow Variable Configuration:**
    *   Create the following Airflow Variables in the Airflow UI (Admin -> Variables):
        *   `bq_project_id`: Your GCP project ID (e.g., `your-gcp-project-id`).
        *   `bq_dataset_id`: The BigQuery dataset ID where the stored procedure resides (e.g., `dataset`).
        *   `job_kennung_param`: The actual value for `p_JobKennung` (e.g., `BERT_CONTRACT_VALIDITY`).
        *   `eintrags_nr_param`: The actual value for `p_EintragsNr` (e.g., `CONTRACT_VALIDITY_RUN_1`).
7.  **Airflow DAG Deployment and Scheduling:**
    *   Upload `composer_dag_k_ausd_v_ta_cntrct_valid.py` to your Cloud Composer DAGs folder.
    *   Configure the desired schedule for the DAG within the Airflow UI (e.g., `@daily`, `0 0 * * *`).

## 5. Known Gaps & Unresolved References

*   **Source Data Ingestion Pipeline Details:** While the need for data ingestion from Oracle to BigQuery is identified, the specific tools (e.g., Data Fusion, Dataflow, DMS) and their implementation details are not part of this migration package. This is a critical prerequisite that must be designed and implemented separately.
*   **`p_JobKennung` and `p_EintragsNr` Values:** The Airflow DAG uses default values from Airflow Variables. The exact production values for these parameters need to be confirmed and configured in the Airflow UI.
*   **Oracle `TO_DATE` Function Equivalence:** The original Oracle SQL uses `TO_DATE(..., 'YYYYMMDD')`. The BigQuery stored procedure assumes `cv.insert_at` and `cv.modified_at` are already `DATETIME` or `TIMESTAMP` types in BigQuery, allowing direct use of `DATE()` for comparison. If the source columns are `STRING` in BigQuery, `PARSE_DATE()` or `PARSE_DATETIME()` with the correct format string (`%Y%m%d`) would be required. This should be verified based on the actual schema of the ingested source tables.
*   **Granular Error Handling:** The BigQuery stored procedure includes a generic `EXCEPTION WHEN ERROR` block. While functional, the original KornShell script might have had more specific error messages or handling for different failure points. If more granular error reporting is required, the stored procedure's error handling can be enhanced.
*   **`BERT_DROP_TEMP_TABLE` Constant:** The `job_kennung` value `'BERT_DROP_TEMP_TABLE'` used to derive `v_datum` is hardcoded in the stored procedure. This should be confirmed if it's a static, unchanging value, or if it needs to be parameterized.
*   **Data Latency Impact:** If the original Oracle job ran with a very high frequency, and the new BigQuery source data ingestion pipeline runs less frequently (e.g., daily batch), there might be an impact on data freshness. This should be assessed and communicated to stakeholders.

## 6. Validation

To validate the successful migration and functionality of the new BigQuery job:

1.  **Trigger the DAG:**
    *   In the Cloud Composer Airflow UI, navigate to the `k_ausd_v_ta_cntrct_valid_bigquery_dag` and manually trigger a run.
2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully (green status). Check the logs of the `call_r_ausd_vertrag_sp` task for any BigQuery-specific messages or errors.
3.  **Check BigQuery Job History:**
    *   In the BigQuery UI, go to "SQL Workspace" -> "Query history" to confirm the stored procedure execution and its status.
4.  **Verify Target Table Content:**
    *   Query `project.dataset.sof_ta_cntrct_valid` in BigQuery.
    *   **Passing Criteria:**
        *   The table should be populated with data.
        *   The number of records loaded should match expectations (e.g., compare with the last successful run of the original job, or with a direct query on the source data applying the same logic).
        *   The `bfc_age` column should correctly reflect the `insert_at` value from the source.
        *   The data content (e.g., `cntrct_validity_id`, `first_period_id`, etc.) should be accurate and consistent with the source data after applying the filtering logic based on `v_datum`.
5.  **Verify Cutoff Date Logic:**
    *   Manually query `project.isbert_schema.dwtk_meldungen` to determine the `MAX(DATE(timecreated))` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This derived date should correspond to the `v_datum` used by the stored procedure.
6.  **Check Audit Log (Optional but Recommended):**
    *   Query `project.dataset.job_audit_log` to confirm an entry was made for the job run, indicating `status = 'SUCCESS'` and the correct `records_loaded` count.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, follow these steps to roll back to the original system:

1.  **Immediate Action:**
    *   **Deactivate the new Airflow DAG:** In the Cloud Composer Airflow UI, toggle off the `k_ausd_v_ta_cntrct_valid_bigquery_dag` to prevent further execution.
2.  **Re-enable Original Job:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh` job in its legacy scheduling system.
3.  **Data Recovery (if necessary):**
    *   Since the BigQuery job performs a `TRUNCATE TABLE` followed by an `INSERT`, the impact of a failed run is generally limited to the current state of `project.dataset.sof_ta_cntrct_valid`.
    *   If the data in `project.dataset.sof_ta_cntrct_valid` is deemed corrupted or incorrect and needs to be reverted to a previous state, and if the original job *also* targets this BigQuery table (unlikely, as it's a migration), then:
        *   If BigQuery's Time Travel feature is enabled for the table, you might be able to query data from a previous point in time.
        *   Otherwise, the most straightforward approach for this truncate-and-load pattern is to simply re-run the original job (if it can populate the BigQuery table) or re-run the BigQuery job after fixing the issue.
    *   If `project.dataset.sof_ta_cntrct_valid` is a staging table and downstream processes rely on it, ensure those processes are paused or reverted as well.
4.  **Investigation:**
    *   Analyze the logs from Cloud Composer and BigQuery to identify the root cause of the issue before attempting to re-deploy or re-enable the migrated job.