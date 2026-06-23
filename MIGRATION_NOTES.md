# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job originating from the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh`.

The original job orchestrated the execution of an Oracle PL/SQL script (`d_ausd_v_ta_barrier_zusgf.sql`) to process and aggregate barrier-related information from `sof$ta_barrier` into `sof$ta_barrier_zusgf`.

The job has been migrated to Google Cloud Platform (GCP) with the following target architecture:
*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Warehouse:** BigQuery.
*   **Data Transformation:** BigQuery Standard SQL.

The migration involved converting the KornShell orchestration logic into an Airflow DAG and translating the Oracle PL/SQL data transformation logic into BigQuery SQL.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`create_sof_ta_barrier_zusgf.sql`**
    *   **Role:** This SQL script contains the BigQuery Data Definition Language (DDL) to create the target table `sof_ta_barrier_zusgf` in BigQuery. It ensures the table exists with the correct schema (`cntrct_id` INT64, `sperrart_alle` STRING, `sperrgrund_alle` STRING, `stilllegungszeitraum_alle` STRING, `sperrgrund_zusgf` INT64) before data is loaded.
*   **`transform_sof_ta_barrier_zusgf.sql`**
    *   **Role:** This SQL script contains the core data transformation logic, translated from the original Oracle PL/SQL (`d_ausd_v_ta_barrier_zusgf.sql`) to BigQuery Standard SQL. It first truncates the target table `sof_ta_barrier_zusgf` and then inserts the processed data, reading from `sof_ta_barrier` and performing the necessary aggregations and string manipulations.
*   **`k_ausd_v_ta_barrier_zusgf_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG. It orchestrates the execution of the BigQuery DDL and transformation SQL. It includes two main tasks: `create_target_table` (to ensure the target table exists) and `load_transformed_data` (to execute the `transform_sof_ta_barrier_zusgf.sql` logic). It replaces the original KornShell script's orchestration responsibilities.

## 3. Key Design Decisions

*   **Orchestration Migration (KornShell to Airflow):** The original KornShell script's role as a job controller, handling environment setup, parameter parsing, error handling, and SQL script invocation, was replaced by an Apache Airflow DAG. Airflow provides robust scheduling, monitoring, logging, and error handling capabilities native to GCP's Cloud Composer.
*   **Data Transformation Migration (Oracle PL/SQL to BigQuery SQL):** The core data processing logic, originally in an Oracle PL/SQL package with a pipelined table function, was translated into BigQuery Standard SQL. This involved:
    *   Replacing Oracle's pipelined function and explicit looping with BigQuery's `STRING_AGG` function and Common Table Expressions (CTEs) for efficient set-based operations.
    *   Converting Oracle-specific functions (e.g., `TO_CHAR`, `DECODE`) to their BigQuery equivalents (`FORMAT_DATE`, `CASE WHEN`).
    *   Removing Oracle performance hints (e.g., `PARALLEL`) as BigQuery automatically manages parallelism.
    *   The `TRUNCATE TABLE` followed by `INSERT` pattern was preserved, as it aligns with the original script's behavior.
*   **Source and Target Data Platform:** All source and target tables (e.g., `sof$ta_barrier`, `dwtk_meldungen`, `sof$ta_barrier_zusgf`) are assumed to be migrated or replicated to BigQuery, leveraging BigQuery's scalability and analytical capabilities.
*   **Handling of `v_datum`:** The original script's logic to derive `v_datum` from `dwtk_meldungen` was noted to be indirectly used or commented out in the final `INSERT` statement for table naming. For this migration, it was decided to simplify this by not including it in the core transformation SQL, assuming it's either not critical for the main data flow or can be handled as an Airflow parameter if needed for auditing/logging.
*   **`sperrgrund_zusgf` Aggregation:** The complex conditional aggregation for `sperrgrund_zusgf` (where `3` is chosen if any `sperrgrund_zusgf` is not `2`, otherwise `2`) was accurately translated using `CASE WHEN COUNTIF(sperrgrund_zusgf != 2) > 0 THEN 3 ELSE 2` in BigQuery SQL, ensuring business logic consistency.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the `source_dataset` (e.g., `raw_zone`) and `target_dataset` (e.g., `transformed_zone`) exist in your GCP project.
    *   `gcloud bq mk --dataset --project_id=<YOUR_GCP_PROJECT_ID> <SOURCE_DATASET_NAME>`
    *   `gcloud bq mk --dataset --project_id=<YOUR_GCP_PROJECT_ID> <TARGET_DATASET_NAME>`

2.  **Source Data Ingestion:**
    *   The source tables `sof_ta_barrier` and `dwtk_meldungen` must be ingested and available in the specified `source_dataset` within BigQuery. This typically involves setting up a separate data ingestion pipeline (e.g., Datastream for CDC, batch loads using Dataflow or `bq load` commands) from the original Oracle source.

3.  **IAM Permissions:**
    *   The Airflow service account (used by Cloud Composer) must have the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on the `target_dataset` to create, truncate, and insert data into `sof_ta_barrier_zusgf`.
        *   `BigQuery Data Viewer` on the `source_dataset` to read from `sof_ta_barrier` and `dwtk_meldungen`.
        *   `BigQuery Job User` to run BigQuery jobs.

4.  **Airflow GCP Connection:**
    *   Verify that the `google_cloud_default` connection is correctly configured in your Airflow environment and linked to the appropriate GCP project and service account.

5.  **Configure Airflow DAG Variables:**
    *   Edit the `k_ausd_v_ta_barrier_zusgf_dag.py` file to replace the placeholder values for:
        *   `PROJECT_ID = "your-gcp-project-id"`
        *   `SOURCE_DATASET = "source_dataset"`
        *   `TARGET_DATASET = "target_dataset"`

6.  **Airflow DAG Deployment:**
    *   Upload the `k_ausd_v_ta_barrier_zusgf_dag.py` file to your Cloud Composer environment's DAGs folder.

7.  **Scheduling:**
    *   Define the appropriate `schedule` for the DAG within `k_ausd_v_ta_barrier_zusgf_dag.py` (e.g., `@daily`, `0 0 * * *`). The current default is `None`, meaning it will only run manually.

## 5. Known Gaps & Unresolved References

*   **`v_datum` Usage Confirmation:** The original script's derivation of `v_datum` from `isbert_schema.dwtk_meldungen` was not directly used in the final `INSERT` statement for table names in the provided Oracle SQL. A detailed review with business stakeholders is required to confirm if this date parameter is still relevant and, if so, how it should be incorporated (e.g., as an Airflow parameter, a filter in the SQL, or for logging/auditing purposes).
*   **Source Data Latency Requirements:** The migration assumes `sof_ta_barrier` and `dwtk_meldungen` are available and up-to-date in BigQuery. The chosen data ingestion method for these tables must meet the job's data freshness requirements.
*   **Performance Tuning:** While BigQuery handles parallelism automatically, the `STRING_AGG` function, especially with `ORDER BY` over a large number of grouped records, might require performance monitoring and potential tuning (e.g., clustering or partitioning the source table `sof_ta_barrier` by `cntrct_id`) if performance issues arise with very large datasets.

## 6. Validation

To validate the successful migration and correct functionality of the job:

1.  **Trigger the Airflow DAG:**
    *   In the Airflow UI, locate the `k_ausd_v_ta_barrier_zusgf_dag` and manually trigger a run.

2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks (`create_target_table`, `load_transformed_data`) complete successfully without errors. Check task logs for any warnings or errors.

3.  **Verify Target Table Population:**
    *   After successful DAG execution, query the target table in BigQuery:
        ```sql
        SELECT * FROM `<YOUR_GCP_PROJECT_ID>.<TARGET_DATASET_NAME>.sof_ta_barrier_zusgf` LIMIT 100;
        ```
    *   Confirm that data has been inserted and the table is not empty.

4.  **Data Comparison (Passing Criteria):**
    *   **Row Count:** Compare the row count of `sof_ta_barrier_zusgf` in BigQuery with the row count of the original `sof$ta_barrier_zusgf` table in Oracle after a successful run of the original job. The counts should match.
    *   **Data Integrity:**
        *   Select a representative sample of `cntrct_id` values from the BigQuery target table.
        *   For these `cntrct_id`s, compare the aggregated values (`sperrart_alle`, `sperrgrund_alle`, `stilllegungszeitraum_alle`, `sperrgrund_zusgf`) against the corresponding values in the original Oracle `sof$ta_barrier_zusgf` table.
        *   Pay close attention to the concatenation logic (`STRING_AGG`) and the conditional `sperrgrund_zusgf` calculation to ensure they match the expected output from the Oracle job.
    *   **Null Handling:** Verify that `NULL` values are handled consistently, especially for `stilllegungszeitraum_alle` when `ist_stillegung` is not `1`.

## 7. Rollback Procedure

In case of issues or critical failures after go-live, the following rollback procedure can be followed:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, toggle off the `k_ausd_v_ta_barrier_zusgf_dag` to prevent further runs.

2.  **Revert to Original Job:**
    *   Re-enable or restart the original KornShell script (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh`) in the legacy environment. Ensure its scheduling is restored.

3.  **Data Remediation (if necessary):**
    *   If the BigQuery target table `sof_ta_barrier_zusgf` was populated with incorrect data, it can be truncated or dropped:
        ```sql
        TRUNCATE TABLE `<YOUR_GCP_PROJECT_ID>.<TARGET_DATASET_NAME>.sof_ta_barrier_zusgf`;
        -- OR
        DROP TABLE `<YOUR_GCP_PROJECT_ID>.<TARGET_DATASET_NAME>.sof_ta_barrier_zusgf`;
        ```
    *   If the original Oracle job relies on the state of `sof$ta_barrier_zusgf` and it was affected by the migration attempt, a data restore from a backup might be necessary for the Oracle table. However, since the original job truncates and re-inserts, simply re-running the original job should overwrite any potentially bad data.

4.  **Investigate and Resolve:**
    *   Analyze the root cause of the failure in the migrated job using Airflow logs, BigQuery job history, and Cloud Logging. Address the identified issues before attempting re-deployment.