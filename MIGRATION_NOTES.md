# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the batch job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh`. The original job, comprising KornShell scripts and an Oracle SQL script, was responsible for calculating, refreshing, and persisting binding-period values for contracts into a cache table named `ta_c_bfc`.

The migration targets the Google Cloud Platform (GCP), leveraging:
*   **BigQuery** for all data transformation logic, replacing Oracle SQL*Plus, PL/SQL, and proprietary Oracle features like DB Links.
*   **Cloud Composer (Apache Airflow)** for workflow orchestration, replacing the KornShell wrapper and control scripts.

This transition aims to modernize the data pipeline, improve scalability, reduce operational overhead, and align with GCP's managed services ecosystem.

## 2. Generated artifacts

The migration process generated the following files, each serving a specific role in the new GCP-based architecture:

*   **`bigquery/ddl/create_ta_c_bfc_table.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the target cache table `ta_c_bfc` in BigQuery. This script ensures the table structure is correctly set up before data processing begins.
*   **`bigquery/ddl/create_ta_c_bfc_akt_table.sql`**
    *   **Role:** Defines the DDL for the staging table `ta_c_bfc_akt` in BigQuery. This temporary table is used to build and aggregate data before merging into the main cache table.
*   **`bigquery/udf/create_bfc_get_bindefrist_udf.sql`**
    *   **Role:** Creates or replaces a BigQuery SQL User-Defined Function (UDF) named `bfc_get_bindefrist`. This UDF is intended to re-implement the complex business logic originally found in the Oracle PL/SQL function `Cds$vr_Bindefrist.GetBindeFrist`. **Note:** This is currently a placeholder and requires full implementation of the original logic.
*   **`bigquery/sql/step1_build_staging.sql`**
    *   **Role:** Contains the BigQuery SQL statements for the first data processing step. It truncates the `ta_c_bfc_akt` staging table and inserts aggregated data from various source tables, mirroring the initial data preparation in the original Oracle script.
*   **`bigquery/sql/step2_initial_load.sql`**
    *   **Role:** Provides the BigQuery SQL for the second step, which conditionally performs an initial population of the `ta_c_bfc` target table if it is currently empty.
*   **`bigquery/sql/step3_merge_changed_rows.sql`**
    *   **Role:** Implements the core merge logic using BigQuery's `MERGE` statement. This script updates existing rows in `ta_c_bfc` where `bfc_age` or `bfc_count` have changed, and inserts new contract IDs from the staging table.
*   **`bigquery/sql/step4_recalculate_stale_rows.sql`**
    *   **Role:** Contains BigQuery SQL to update rows in `ta_c_bfc` where the `bfc_procedure` date is older than the current processing date. It uses `QUALIFY ROW_NUMBER()` to limit the number of updated rows, mimicking the Oracle `ROWNUM` behavior.
*   **`bigquery/sql/cleanup_staging_table.sql`**
    *   **Role:** Provides the BigQuery SQL to truncate the `ta_c_bfc_akt` staging table, cleaning up temporary data after the main processing is complete.
*   **`airflow/dags/r_ausd_v_ta_c_bfc_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG orchestrates the execution of all BigQuery SQL scripts in the correct sequence, replacing the original KornShell wrapper and control scripts. It manages task dependencies, logging, and error handling within the Cloud Composer environment.

## 3. Key design decisions

The following key design decisions guided the migration to GCP:

*   **BigQuery as the Primary Data Processing Engine:** BigQuery was chosen to replace Oracle SQL for all data transformation logic due to its serverless architecture, petabyte-scale analytics capabilities, and cost-effectiveness. This eliminates the need for managing traditional database instances and proprietary Oracle features.
*   **Cloud Composer (Apache Airflow) for Orchestration:** Cloud Composer was selected to manage the workflow, replacing the KornShell scripts. Airflow provides a robust, scalable, and managed orchestration platform with built-in features for scheduling, monitoring, logging, and error handling, which are superior to custom shell scripting.
*   **Re-implementation of PL/SQL Logic as BigQuery UDF:** The complex business logic encapsulated in the Oracle `Cds$vr_Bindefrist.GetBindeFrist` package (wrapped by `bfc_get_bindefrist`) is critical. To maintain this logic within the data processing layer and avoid external calls, it was decided to re-implement it as a BigQuery SQL UDF. This allows the function to be called directly within BigQuery SQL statements.
*   **Leveraging BigQuery `MERGE` Statement:** The original Oracle script used `MERGE` for upsert operations. BigQuery's native `MERGE` statement was directly adopted to achieve the same efficient update-or-insert functionality for the `ta_c_bfc` cache table.
*   **Translating `ROWNUM` with `QUALIFY ROW_NUMBER()`:** The Oracle `ROWNUM <= &v_max_update` clause, used for throttling updates to stale records, was translated to BigQuery's `QUALIFY ROW_NUMBER() OVER(...) <= {{ v_max_update }}`. This preserves the original behavior of processing a limited batch of records per run, which might be a performance or resource management strategy.
*   **External Data Ingestion for Oracle Sources:** To address the dependency on Oracle DB Links and local Oracle tables, a strategy for replicating all necessary source data into BigQuery was adopted. This will likely involve Google Cloud Datastream for Change Data Capture (CDC) for transactional tables or batch ingestion pipelines for slower-changing data, ensuring data availability in BigQuery.
*   **Modular SQL Scripts within Airflow:** The monolithic Oracle SQL script was broken down into smaller, modular BigQuery SQL files, each representing a distinct step in the transformation process. These are then orchestrated as individual tasks within the Airflow DAG, improving readability, maintainability, and error isolation.

## 4. Manual steps before go-live

Before the migrated job can go live, several manual steps are required to set up the GCP environment and ensure proper functionality:

1.  **GCP Project and BigQuery Dataset Creation:**
    *   Ensure a dedicated GCP project is provisioned.
    *   Create the target BigQuery dataset (e.g., `{{ dataset_id }}`) where `ta_c_bfc`, `ta_c_bfc_akt`, and the UDF will reside.
2.  **IAM Permissions Configuration:**
    *   Grant the service account associated with the Cloud Composer environment (or the user deploying the DAG) the necessary IAM roles:
        *   `BigQuery Data Editor` (or more granular permissions) for the target dataset.
        *   `BigQuery Job User` to run BigQuery queries.
        *   `Composer Worker` and `Composer User` roles for Airflow operations.
3.  **Source Data Ingestion Setup:**
    *   **Crucially**, configure data ingestion pipelines to replicate all required Oracle source tables into the BigQuery dataset. This includes:
        *   `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `isbert_schema.dwtk_meldungen`.
        *   Data from remote Oracle instances accessed via DB Link `@pcrs1` (e.g., `all_objects`, `spr_schema.cds$vr_Bindefrist`, `spr_schema.spr$pa_types`, `spr_schema.cds$ta_cntrct`).
    *   This might involve setting up Datastream, Dataflow jobs, or batch exports/imports.
4.  **Cloud Composer Environment Provisioning:**
    *   Ensure a Cloud Composer environment is provisioned and running.
5.  **Airflow Connection Configuration:**
    *   Verify or create the `google_cloud_default` Airflow connection, ensuring it correctly points to your GCP project and uses the appropriate service account for BigQuery interactions.
6.  **UDF Business Logic Implementation:**
    *   **This is a critical step.** The placeholder logic in `bigquery/udf/create_bfc_get_bindefrist_udf.sql` must be replaced with the actual, fully translated business logic from the Oracle `Cds$vr_Bindefrist.GetBindeFrist` PL/SQL package. This requires detailed analysis of the original Oracle code.
7.  **Airflow DAG Deployment:**
    *   Upload the `airflow/dags/r_ausd_v_ta_c_bfc_dag.py` file to the DAGs folder of your Cloud Composer environment.
8.  **Airflow DAG Configuration and Scheduling:**
    *   In the Airflow UI, enable the `r_ausd_v_ta_c_bfc_dag`.
    *   Configure the desired schedule for the DAG (e.g., daily, hourly) by modifying the `schedule` parameter in the DAG definition.
    *   Set any required Airflow Variables for `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET_ID` if they are not hardcoded in the DAG. The `v_max_update` parameter can be configured via DAG parameters.

## 5. Known gaps & unresolved references

The following items have been identified as gaps, risks, or require further follow-up:

*   **`bfc_get_bindefrist` UDF Implementation (B4 Item):** The most significant gap is the placeholder logic within `bigquery/udf/create_bfc_get_bindefrist_udf.sql`. The complex business logic from the original Oracle `Cds$vr_BindeFrist.GetBindeFrist` PL/SQL package needs to be thoroughly analyzed, understood, and accurately re-implemented in BigQuery SQL. This is a critical B4 item requiring dedicated development and testing.
*   **Oracle-Specific Syntax & Semantics:** While efforts have been made to translate, subtle behavioral differences in Oracle's date/null handling, implicit type conversions, and other proprietary features might exist in BigQuery. Comprehensive testing is needed to identify and address these.
*   **Complete DB Link Data Replication:** Ensuring that *all* data accessed via the Oracle DB Link `@pcrs1` (including metadata tables like `all_objects` and package definitions like `spr_schema.cds$vr_Bindefrist`) is accurately and consistently replicated to BigQuery. Any missing or outdated data could impact the job's correctness.
*   **Purpose of `v_max_update` Throttling:** The exact business or technical reason behind the `ROWNUM <= &v_max_update` throttling in the original Oracle script needs to be confirmed. While `QUALIFY ROW_NUMBER()` provides an equivalent, understanding the original intent (e.g., resource management, avoiding long transactions, batch processing) is important to ensure the BigQuery implementation aligns with requirements.
*   **Full Utility Script Logic Review:** The original KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) were replaced by Airflow's native capabilities. A thorough review of their full logic is needed to ensure no critical business logic, complex error handling, or specific operational procedures were inadvertently lost or require explicit re-implementation within the Airflow DAG or BigQuery SQL.
*   **File Complexity Data:** The absence of `file_complexity` data for the source components makes it challenging to accurately estimate the effort required for the `bfc_get_bindefrist` re-implementation and other manual analysis tasks.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results compared to the legacy system.

**How to Run Tests:**

1.  **Ensure Data Ingestion:** Verify that all required source Oracle tables are fully replicated and up-to-date in the target BigQuery dataset.
2.  **Deploy DAG:** Ensure the `r_ausd_v_ta_c_bfc_dag.py` is deployed to the Cloud Composer environment and enabled.
3.  **Trigger DAG:** Manually trigger the `r_ausd_v_ta_c_bfc_dag` from the Airflow UI.
4.  **Monitor Execution:** Observe the DAG run in the Airflow UI, checking task statuses and logs in Cloud Logging for any errors or warnings.
5.  **Query BigQuery:** Once the DAG completes successfully, query the target `{{ project_id }}.{{ dataset_id }}.ta_c_bfc` table in BigQuery.
6.  **Compare with Source:**
    *   **Row Counts:** Compare the total row count in BigQuery's `ta_c_bfc` with the `sof$ta_c_bfc` table in the Oracle source system for the same data snapshot.
    *   **Data Parity (Sampled):** Select a statistically significant sample of `cntrct_id`s. For these IDs, compare all relevant columns (`bindefrist`, `bfc_age`, `bfc_count`, `bfc_procedure`, `commitment_reference_date`, `cntrct_validity_id`) between the BigQuery and Oracle tables.
    *   **Edge Cases:** Test specific contract IDs known to have complex binding period calculations or historical data changes to validate the `bfc_get_bindefrist` UDF and merge logic.
    *   **Performance:** Monitor the BigQuery job execution times and resource consumption via Cloud Monitoring and BigQuery's job history. Compare these against the performance of the original Oracle job.

**What "Passing" Means:**

*   The `r_ausd_v_ta_c_bfc_dag` completes successfully without any failed tasks.
*   All BigQuery jobs executed by the DAG complete without errors.
*   The row count in `{{ project_id }}.{{ dataset_id }}.ta_c_bfc` is identical to the `sof$ta_c_bfc` table in the Oracle source system for the same data snapshot.
*   For the sampled data, all key data points (`bindefrist`, `bfc_age`, `bfc_count`, `bfc_procedure`, `commitment_reference_date`, `cntrct_validity_id`) in BigQuery's `ta_c_bfc` are identical to their counterparts in Oracle's `sof$ta_c_bfc`.
*   The `bindefrist` values, specifically, are correctly calculated by the re-implemented UDF.
*   The overall execution time of the Airflow DAG is within acceptable performance thresholds, ideally matching or improving upon the original Oracle job's runtime.
*   No unexpected errors or warnings are observed in Cloud Logging.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated to revert to the original system:

1.  **Immediate Action - Stop New Job:**
    *   **Disable Airflow DAG:** Immediately pause or delete the `r_ausd_v_ta_c_bfc_dag` in the Cloud Composer Airflow UI to prevent any further execution or updates to the BigQuery `ta_c_bfc` table.
2.  **Reactivate Original Job:**
    *   Ensure the original Oracle KornShell job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh` is reactivated and scheduled in the legacy environment. Verify its successful execution.
3.  **Data Reversion (if necessary):**
    *   **BigQuery `ta_c_bfc`:** If the BigQuery `ta_c_bfc` table was updated incorrectly and this impacts any downstream systems consuming data from BigQuery, consider reverting the table to a previous state. BigQuery's Time Travel feature allows querying data up to 7 days in the past. Alternatively, if backups were taken, restore from a known good state.
    *   **Oracle Source System:** Given that this job primarily updates a cache table and does not modify core source data, direct impact on the Oracle source system is unlikely. However, if any unforeseen issues arise, follow standard Oracle database recovery procedures.
4.  **Communication:**
    *   Inform all relevant stakeholders about the rollback and the status of the data pipelines.
5.  **Cleanup (Optional):**
    *   If a clean re-deployment is planned, or if the migration is permanently aborted, the BigQuery tables (`ta_c_bfc`, `ta_c_bfc_akt`) and the UDF (`bfc_get_bindefrist`) can be dropped.
    *   The DAG file can be removed from the Cloud Composer DAGs folder.

This procedure ensures a quick return to a stable state using the legacy system while allowing for investigation and resolution of the issues encountered with the migrated job.