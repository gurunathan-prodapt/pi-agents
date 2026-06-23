# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job. This job, originally responsible for updating contract information in the `sof$ta_cntrct_crs2` table by excluding frame contract parents, has been migrated from its legacy UC4/KornShell/Oracle environment to Google Cloud Platform. The target platform utilizes Apache Airflow on Cloud Composer for orchestration and Google BigQuery for all data storage and processing.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dags/dw_bert_ausd_v_ta_cntrct_crs2_dag.py`**
    *   **Role:** This is the Apache Airflow DAG (Directed Acyclic Graph) definition. It orchestrates the entire job flow on Cloud Composer. It defines the sequence of tasks, including creating the target BigQuery table if it doesn't exist and executing the core data transformation logic. It replaces the UC4 job definition and the KornShell wrapper/core scripts.
*   **`sql/d_ausd_v_ta_cntrct_crs2.bqsql`**
    *   **Role:** This file contains the core BigQuery SQL statement responsible for inserting data into the `sof_ta_cntrct_crs2` table. It is a direct translation of the original Oracle SQL script (`d_ausd_v_ta_cntrct_crs2.sql`) into BigQuery-compliant SQL. While the DAG embeds the SQL directly for simplicity in this case, this file represents the standalone BigQuery SQL logic.
*   **`ddl/dw_bert_staging/sof_ta_cntrct_crs2.sql`**
    *   **Role:** This file provides the Data Definition Language (DDL) for creating the target BigQuery table `sof_ta_cntrct_crs2` within the `dw_bert_staging` dataset. It defines the table schema based on the columns populated by the `INSERT` statement. This DDL is also embedded within the Airflow DAG's `create_target_table_if_not_exists` task.

## 3. Key Design Decisions

*   **Orchestration Shift to Airflow:** The legacy UC4 job definition and KornShell scripts (`r_ausd_v_ta_cntrct_crs2.ksh`, `k_ausd_v_ta_cntrct_crs2.ksh`) were replaced by a single Python-based Airflow DAG. This centralizes scheduling, dependency management, and logging within a cloud-native, scalable framework.
*   **Data Processing to BigQuery:** All Oracle SQL operations were translated to BigQuery SQL. This leverages BigQuery's serverless, highly scalable, and cost-effective data warehousing capabilities, eliminating the need for managing traditional relational databases.
*   **SQL Translation:** Oracle-specific SQL syntax, such as the `(+)` outer join, was converted to standard SQL `LEFT OUTER JOIN` for BigQuery compatibility. Oracle functions like `NVL` and `TO_CHAR` were replaced with BigQuery equivalents (`COALESCE`, `FORMAT_DATE`).
*   **KornShell Logic Absorption:** The parameter parsing, environment setup, and basic error handling from the KornShell scripts were either re-implemented directly in the Airflow DAG's Python code or replaced by Airflow's native features (e.g., Airflow variables for parameters, built-in logging, `on_failure_callback` for error handling). Complex utility functions were deemed unnecessary or simplified for the BigQuery context.
*   **Implicit Schema Derivation:** The DDL for the target BigQuery table (`sof_ta_cntrct_crs2`) was derived directly from the `INSERT` statement in the original Oracle SQL, as no explicit DDL was provided in the source inventory. This assumes the column types can be inferred or are standard.
*   **Separation of DDL and DML:** The Airflow DAG explicitly separates the table creation (DDL) from the data loading (DML) into distinct BigQueryOperator tasks. This ensures the target table exists before data manipulation begins and improves task granularity.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the `dw_bert_staging` BigQuery dataset exists in your GCP project. If not, create it:
        ```bash
        bq mk --dataset --default_table_expiration 36500 dw_bert_staging
        ```
        (Adjust `default_table_expiration` as needed).
2.  **IAM Permissions:**
    *   The service account used by your Cloud Composer environment's worker nodes must have the necessary BigQuery permissions. This typically includes:
        *   `BigQuery Data Editor` on the `dw_bert_staging` dataset.
        *   `BigQuery Job User` at the project level.
        *   `Storage Object Viewer` for accessing DAGs and SQL files from Cloud Storage.
3.  **Airflow Connection Strings:**
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured to point to your GCP project. This connection is used by the `BigQueryOperator`.
4.  **Airflow Variables/Secrets:**
    *   Set the Airflow Variable `gcp_project` to your Google Cloud Project ID. This is referenced in the DAG using `{{ var.value.gcp_project }}`.
    *   If any sensitive data (e.g., API keys for external systems, though not directly apparent here) were part of the KornShell environment, they must be securely managed in Airflow (e.g., using Airflow Connections, Secrets Backend, or GCP Secret Manager).
5.  **Scheduling:**
    *   The `schedule=None` in the generated DAG needs to be updated to reflect the actual production schedule of the legacy UC4 job (e.g., `@daily`, `0 0 * * *`). This will require investigating the original UC4 job's scheduling configuration.
6.  **Source Data Migration/Federation:**
    *   **`dw_bert_staging.sof_ta_cntrct_crs` and `dw_bert_staging.dwtk_meldungen`:** These source tables must be present in BigQuery within the `dw_bert_staging` dataset. This requires a prior data migration or continuous replication setup from their Oracle source.
    *   **Carmen DB (`@pcrs1`) Data:** The original job referenced an external Oracle database (Carmen DB) via a DB link. Data required from Carmen DB must be made available in BigQuery. This could involve:
        *   **Full Migration:** Migrating the relevant Carmen DB tables to BigQuery.
        *   **Federated Query:** Setting up a BigQuery federated query to Cloud SQL for Oracle (if Carmen DB is migrated to Cloud SQL).
        *   **Data Transfer Service:** Regularly ingesting necessary data from Carmen DB into BigQuery using a data transfer service.
        *   **Manual Review:** The current BigQuery SQL translation does *not* include the `dwtk_meldungen` or Carmen DB logic as it was not directly part of the main `INSERT` statement. If `dwtk_meldungen` or Carmen DB data was critical for the *main* `INSERT` logic (e.g., for filtering or joining), this needs to be explicitly added to the BigQuery SQL. The current translation only uses `sof_ta_cntrct_crs`.

## 5. Known Gaps & Unresolved References

*   **Incomplete UC4 Workflow Details:** The provided UC4 XML was a single job definition. The full scheduling details (e.g., cron schedule, dependencies on other jobs) within the broader UC4 ecosystem are not fully captured. Manual investigation of the UC4 environment is required to ensure the migrated Airflow DAG has the correct schedule and external dependencies.
*   **Carmen DB Integration:** The reliance on `Carmen DB` via an Oracle DB link requires a clear strategy. The current BigQuery SQL translation *does not* include any explicit joins or lookups to `dwtk_meldungen` or Carmen DB. If the `dwtk_meldungen` table was used to derive a date variable (`s_datum`) that *influenced* the main `INSERT` statement (e.g., in a `WHERE` clause), this logic is currently missing from the BigQuery SQL. A thorough review of the original `d_ausd_v_ta_cntrct_crs2.sql` and its execution context is needed to confirm if `s_datum` or Carmen DB data was implicitly used in the main data population.
*   **Oracle-Specific SQL Nuances:** While the primary `INSERT` statement was translated, subtle differences in data types, function behavior, and optimization hints between Oracle and BigQuery might exist. Thorough testing with production-like data is crucial.
*   **KornShell Script Environment:** The `. $HOME/.dw_init` and other sourced scripts might contain complex environment variable settings or custom functions that are not immediately apparent. While the core logic was absorbed, a comprehensive review of these helper scripts is needed to ensure no critical logic was missed.
*   **UC4 Host/Login Details:** The UC4 job referenced `HostDst>|DWHDWH1P|HOST` and `Login>DW.UNIX.ISBERT`. These represent the execution environment and credentials. These have been replaced by Airflow's execution environment and `google_cloud_default` connection. Ensure the Airflow environment has equivalent access and resources.

## 6. Validation

To validate the migrated job:

1.  **Run the Airflow DAG:**
    *   Upload `dags/dw_bert_ausd_v_ta_cntrct_crs2_dag.py` to your Cloud Composer DAGs folder.
    *   Manually trigger the `dw_bert_ausd_v_ta_cntrct_crs2_dag` from the Airflow UI.
2.  **Monitor Task Execution:**
    *   Observe the Airflow UI for successful completion of all tasks (`create_sof_ta_cntrct_crs2_table_if_not_exists` and `load_sof_ta_cntrct_crs2_data`). Check logs for any errors or warnings.
3.  **Data Validation (What "passing" means):**
    *   **Successful DAG Run:** The Airflow DAG completes without any failed tasks.
    *   **Row Count Comparison:** Compare the row count of the `dw_bert_staging.sof_ta_cntrct_crs2` table in BigQuery with the row count of the `sof$ta_cntrct_crs2` table in the legacy Oracle environment *after* a successful run of both the legacy and migrated jobs with the same source data. The counts should match.
    *   **Data Sample Comparison:** Select a random sample of records from both the BigQuery target table and the legacy Oracle target table. Verify that the data in corresponding columns is identical. Pay special attention to `RV_NUM` and any columns derived from joins.
    *   **Schema Verification:** Confirm that the schema of `dw_bert_staging.sof_ta_cntrct_crs2` in BigQuery matches the expected schema and data types.
    *   **Performance:** Compare the execution time of the migrated job in Airflow/BigQuery against the legacy job. While not a strict "pass/fail," significant performance degradation should be investigated.

## 7. Rollback Procedure

In case of issues with the migrated job, the following rollback procedure can be followed:

1.  **Disable New Job:**
    *   In the Airflow UI, pause or delete the `dw_bert_ausd_v_ta_cntrct_crs2_dag` to prevent further execution.
2.  **Verify Legacy Job Status:**
    *   Confirm that the original `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job in UC4 is still active and can be triggered.
3.  **Trigger Legacy Job:**
    *   Manually trigger the legacy UC4 job to ensure it can run successfully and update the Oracle `sof$ta_cntrct_crs2` table.
4.  **Data Restoration (if necessary):**
    *   If the migrated job corrupted or incorrectly updated the `dw_bert_staging.sof_ta_cntrct_crs2` table in BigQuery, you may need to:
        *   Restore the BigQuery table from a previous snapshot or backup (if enabled).
        *   Alternatively, if the source data (`sof_ta_cntrct_crs`) is still intact, you can re-run the legacy job and then re-ingest the correct data into BigQuery using the established data migration/replication process.
5.  **Monitor Legacy System:**
    *   Monitor the legacy UC4/Oracle job to ensure it continues to operate as expected.
6.  **Root Cause Analysis:**
    *   Investigate the cause of the failure in the migrated job, rectify the issues, and re-test in a non-production environment before attempting another go-live.