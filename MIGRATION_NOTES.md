# MIGRATION_NOTES.md: r_ausd_v_ta_cntrct_crs3.ksh

## 1. Summary

This document details the migration of the `r_ausd_v_ta_cntrct_crs3.ksh` job. This job, originally an Oracle-based ETL workflow orchestrated by KornShell scripts and scheduled by UC4, was responsible for synchronizing contract data into the `ta_cntrct_crs3` table.

The entire workflow has been migrated to the Google Cloud Platform.
*   **Target Platform**: Google Cloud Platform
    *   **Orchestration**: Google Cloud Composer (Airflow)
    *   **Data Storage & Transformation**: Google BigQuery

The core logic, which performs a full refresh of the target table by truncating existing data and inserting new, transformed records based on contract information, has been translated into a BigQuery Stored Procedure. The original KornShell wrapper and control scripts, along with the UC4 scheduling, have been replaced by an Airflow DAG.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`bigquery/stored_procedures/r_ausd_v_ta_cntrct_crs3.sql`**
    *   **Role**: This BigQuery Stored Procedure encapsulates the core data transformation logic. It replaces the original `d_ausd_v_ta_cntrct_crs3.sql` Oracle script. It handles date determination, truncates the target table (`sof_ta_cntrct_crs3`), inserts transformed data from `sof_ta_cntrct_crs2`, and includes built-in logging to `job_audit_log` for job status and error handling.

*   **`airflow/dags/dag_r_ausd_v_ta_cntrct_crs3.py`**
    *   **Role**: This Airflow DAG (Directed Acyclic Graph) orchestrates the execution of the BigQuery Stored Procedure. It replaces the UC4 scheduler and the KornShell wrapper/control scripts (`r_ausd_v_ta_cntrct_crs3.ksh`, `k_ausd_v_ta_cntrct_crs3.ksh`). The DAG defines a single task to call the `r_ausd_v_ta_cntrct_crs3` stored procedure, passing necessary parameters.

## 3. Key Design Decisions

The migration strategy focused on leveraging cloud-native services for scalability, maintainability, and cost-efficiency.

*   **Orchestration: Airflow (Google Cloud Composer)**
    *   **Decision**: Replaced UC4 and KornShell scripts with an Airflow DAG.
    *   **Rationale**: Airflow provides a robust, scalable, and managed orchestration platform in Google Cloud. It allows for clear definition of workflows, dependency management, scheduling, and monitoring, surpassing the capabilities of shell scripts and integrating seamlessly with other GCP services.
    *   **Trade-offs**: Requires familiarity with Python and Airflow concepts; initial setup and configuration of Composer environment.

*   **Data Transformation: BigQuery Stored Procedures**
    *   **Decision**: Translated Oracle SQL into a BigQuery Stored Procedure.
    *   **Rationale**: BigQuery is a highly scalable, serverless data warehouse ideal for analytical workloads. Stored procedures allow encapsulating complex SQL logic directly within BigQuery, leveraging its performance for large datasets and reducing data movement. This approach aligns with modern data warehousing practices.
    *   **Trade-offs**: Requires adapting Oracle-specific SQL syntax and functions to BigQuery SQL; potential cost implications for large queries (though generally efficient for this type of workload).

*   **Logging and Auditing: BigQuery `job_audit_log` Table**
    *   **Decision**: Replaced custom shell logging and `dwtk_meldungen` interactions with a dedicated BigQuery audit log table.
    *   **Rationale**: Provides centralized, structured, and queryable logging for all job executions directly within BigQuery. This simplifies monitoring, debugging, and historical analysis of job runs, integrating well with the BigQuery stored procedure's error handling.
    *   **Trade-offs**: Requires defining and maintaining the `job_audit_log` schema.

*   **Data Sourcing: BigQuery Tables**
    *   **Decision**: Source tables (`sof$ta_cntrct_crs2`, `isbert_schema.dwtk_meldungen`) and target table (`sof$ta_cntrct_crs3`) are migrated to BigQuery.
    *   **Rationale**: Consolidates all data assets within BigQuery, eliminating cross-database dependencies and simplifying the ETL process. This enables BigQuery to perform all transformations natively.
    *   **Trade-offs**: Requires a separate data ingestion strategy for bringing data from Oracle to BigQuery (e.g., Datastream, batch loads).

*   **Full Refresh Strategy**:
    *   **Decision**: Maintained the `TRUNCATE TABLE` followed by `INSERT INTO` approach.
    *   **Rationale**: The original job performed a full refresh, and this strategy is directly supported and efficient in BigQuery for smaller to medium-sized tables where historical data is not required within the target table itself.
    *   **Trade-offs**: Not suitable for incremental updates or very large tables where a full refresh becomes inefficient.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the BigQuery dataset `my_dataset` (or your designated dataset ID) within your GCP project (`my-project`). This dataset will host all source, target, and audit tables.
    *   `gcloud bq mk --dataset my-project:my_dataset`

2.  **Source Data Ingestion**:
    *   Ensure that the `sof_ta_cntrct_crs2` table is populated in `my-project.my_dataset.sof_ta_cntrct_crs2` with data from the original Oracle `sof$ta_cntrct_crs2`. This may involve a one-time batch load and/or setting up a continuous data ingestion pipeline (e.g., Datastream, Dataflow).
    *   Ensure that the `dwtk_meldungen` table is populated in `my-project.my_dataset.dwtk_meldungen` with relevant data from the original Oracle `isbert_schema.dwtk_meldungen`. This table is crucial for the `v_datum` derivation logic.

3.  **Target Table Schema Creation**:
    *   Create the empty target table `my-project.my_dataset.sof_ta_cntrct_crs3` in BigQuery with a schema that precisely matches the output of the stored procedure and the original Oracle `sof$ta_cntrct_crs3` table.
    *   Example DDL (adjust column types and nullability as per source):
        ```sql
        CREATE TABLE `my-project.my_dataset.sof_ta_cntrct_crs3` (
            cntrct_id INT64,
            obj_version INT64,
            contract_number STRING,
            cntrct_template_id INT64,
            cntrct_validity_id INT64,
            valid_from DATE,
            com_per_ext_rea_cv STRING,
            billcycle_id INT64,
            vo_code STRING,
            cntrct_start_date DATE,
            cntrct_st STRING,
            cntrct_parent INT64,
            cntrct_ty INT64,
            cost_centre STRING,
            cost_centre_user STRING,
            commitment_reference_date DATE,
            order_number STRING,
            rv_num STRING,
            twinbill STRING,
            twin_vertrag_id INT64
        );
        ```

4.  **Audit Log Table Schema Creation**:
    *   Create the `my-project.my_dataset.job_audit_log` table in BigQuery. This table is used by the stored procedure for logging job execution status.
    *   Example DDL:
        ```sql
        CREATE TABLE `my-project.my_dataset.job_audit_log` (
            job_kennung STRING,
            eintrags_nr INT64,
            start_timestamp TIMESTAMP,
            end_timestamp TIMESTAMP,
            status STRING,
            message STRING,
            records_processed INT64,
            error_message STRING,
            process_date DATE
        );
        ```

5.  **IAM Permissions**:
    *   Ensure the Google Cloud Composer service account has the necessary IAM roles to:
        *   Execute BigQuery jobs (`roles/bigquery.jobUser`).
        *   Read/write data in `my-project.my_dataset` (`roles/bigquery.dataEditor`).
        *   Create/replace stored procedures (`roles/bigquery.admin` or `roles/bigquery.metadataEditor` + `roles/bigquery.dataEditor`).
    *   Ensure the `google_cloud_default` Airflow connection is correctly configured for your GCP project.

6.  **Airflow DAG Scheduling**:
    *   Update the `schedule` parameter in `airflow/dags/dag_r_ausd_v_ta_cntrct_crs3.py` from `None` to the desired cron expression or timedelta that matches the original UC4 schedule.
    *   Example for daily at midnight UTC: `schedule='0 0 * * *'`

7.  **Airflow DAG Parameterization**:
    *   Review and update the `p_JobKennung` and `p_EintragsNr` parameters in the Airflow DAG. The current values are static placeholders.
        *   `p_JobKennung`: Should reflect the unique identifier for this job, e.g., `BERT_AUSD_V_TA_CNTRCT_CRS3`.
        *   `p_EintragsNr`: Consider using Airflow's dynamic templating (e.g., `{{ run_id }}` or a custom sequence generator) to ensure a unique entry number for each job run, if required for auditing.

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps or areas requiring further attention:

*   **Missing File Complexity Data**: The original `file_complexity` data was unavailable, leading to an estimated "Medium" complexity for the source files. This means specific migration challenges or nuances might not have been explicitly flagged.
*   **Oracle `DWPA_UTIL_SKRIPT.runstatement`**: The original Oracle script used `DWPA_UTIL_SKRIPT.runstatement('TRUNCATE TABLE sof$ta_cntrct_crs3')`. The migration assumes `DWPA_UTIL_SKRIPT.runstatement` solely performs a `TRUNCATE TABLE`. If it contained additional complex logic, that logic would need to be identified and replicated in BigQuery.
*   **Dynamic `v_datum` Dependency**: The processing date (`v_datum`) is derived from `dwtk_meldungen` based on `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This implies a dependency on another job (`BERT_DROP_TEMP_TABLE`) completing successfully and updating `dwtk_meldungen`. Ensure this upstream dependency is correctly managed in the new GCP environment, or that the `dwtk_meldungen` table in BigQuery accurately reflects the necessary state.
*   **DB-Link to Carmen DB (`@pcrs1`)**: The original SQL script contained `DEFINE v_carmen = "@pcrs1"`. While `v_carmen` was not directly used in the main `INSERT...SELECT` statement, it suggests a potential dependency on an external Oracle system (Carmen DB) as the ultimate source for `sof$ta_cntrct_crs2`. If `sof$ta_cntrct_crs2` originates from Carmen, a dedicated and robust data ingestion pipeline from Carmen DB to BigQuery is a critical prerequisite and might represent a significant effort.
*   **Shell-Specific Features**: Original KornShell scripts utilized features like `trap` commands for error handling, `getopts` for parameter parsing, `print`/`tee` for logging, and temporary files for record counts. These have been replaced by Airflow's native error handling, BigQuery Stored Procedure parameters, BigQuery's `job_audit_log`, and `@@row_count` functionality. Any subtle behaviors or side effects of the original shell scripts beyond the core ETL logic might need re-evaluation if issues arise.
*   **Static DAG Parameters**: The `p_JobKennung` and `p_EintragsNr` parameters in the Airflow DAG are currently static placeholders. For production, `p_EintragsNr` should ideally be dynamically generated (e.g., using Airflow's `{{ run_id }}` or a custom sequence) to ensure unique audit entries.

## 6. Validation

To ensure the successful migration and correct functioning of the job, the following validation steps should be performed:

1.  **BigQuery Stored Procedure Unit Testing**:
    *   Execute the `r_ausd_v_ta_cntrct_crs3` stored procedure directly in BigQuery with representative sample data in `sof_ta_cntrct_crs2` and `dwtk_meldungen`.
    *   Verify that the `sof_ta_cntrct_crs3` table is truncated and populated correctly.
    *   Check the `job_audit_log` table for `STARTED`, `SUCCESS`, or `FAILED` entries, ensuring `records_processed` and `process_date` are accurate.
    *   Test edge cases, including scenarios where `dwtk_meldungen` might be empty or not contain the expected `job_kennung`.

2.  **Airflow DAG End-to-End Testing**:
    *   Deploy the `dag_r_ausd_v_ta_cntrct_crs3.py` DAG to a development or staging Google Cloud Composer environment.
    *   Trigger the DAG manually and observe its execution in the Airflow UI.
    *   **"Passing" Criteria**:
        *   The Airflow DAG run completes successfully (green status in Airflow UI).
        *   The `call_r_ausd_v_ta_cntrct_crs3_sp` task completes successfully.
        *   The BigQuery `job_audit_log` table contains a `SUCCESS` entry for the corresponding job run, with accurate `start_timestamp`, `end_timestamp`, `records_processed`, and `process_date`.
        *   The `my-project.my_dataset.sof_ta_cntrct_crs3` table in BigQuery is populated with data.
        *   **Data Validation**: Compare the row count and a sample of data (e.g., using checksums or specific queries) in the BigQuery `sof_ta_cntrct_crs3` table against the expected output from the original Oracle job (if a baseline is available). Ensure the "Twinbill" logic correctly identifies and populates `twinbill` and `twin_vertrag_id`.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Deactivate Airflow DAG**:
    *   In the Google Cloud Composer Airflow UI, toggle off the `dag_r_ausd_v_ta_cntrct_crs3` DAG to prevent further executions.

2.  **Revert to Original UC4 Job**:
    *   Re-enable the original UC4 job (`DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml`) in the legacy environment. Ensure its schedule is reactivated and it can connect to the Oracle database.

3.  **Data State in BigQuery**:
    *   The `my-project.my_dataset.sof_ta_cntrct_crs3` table in BigQuery will contain the data from the last successful run of the migrated job. Since the original job performs a full refresh, the BigQuery table can be left as is, or if desired, it can be truncated or dropped. No specific data restoration is typically required for this target table as it's fully refreshed by the source system.
    *   The `my-project.my_dataset.job_audit_log` will retain all audit entries from the migrated job runs, which can be useful for post-mortem analysis.

4.  **Investigation**:
    *   Analyze the logs in Cloud Logging, Airflow UI, and the BigQuery `job_audit_log` to identify the root cause of the rollback. Rectify the issues in the BigQuery stored procedure or Airflow DAG before attempting re-deployment.