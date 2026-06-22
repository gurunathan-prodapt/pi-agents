```markdown
# MIGRATION_NOTES: DW.BERT_AUSD_BP_TA_TARIFOPTION

## 1. Summary

The `DW.BERT_AUSD_BP_TA_TARIFOPTION` job, originally an Oracle-based ETL process orchestrated by UC4, has been migrated to Google Cloud Platform. This job is responsible for preparing and provisioning selected basic product (tarifoption) data for the BERT system, generating a snapshot of contract cache from the Data Warehouse (DWH) for demand scoring.

The migration involved:
*   **Source Platform**: Oracle Database (SQL/PLSQL), KornShell scripts, UC4 scheduler.
*   **Target Platform**: Google Cloud Platform, specifically BigQuery for data storage and transformation, and Apache Airflow (Cloud Composer) for orchestration.

The migrated job now processes and transforms tariff option data within BigQuery, categorizing and aggregating it based on business, GPRS, and other criteria, and populating intermediate and final BigQuery tables.

## 2. Generated Artifacts

The migration produced the following artifacts:

*   **`sql/bigquery/d_ausd_bp_ta_tarifoption.sql`**
    *   **Role**: This BigQuery SQL script contains the core data transformation logic. It is a direct migration of the original `d_ausd_bp_ta_tarifoption.sql` Oracle script. It performs the following actions:
        1.  Declares `v_datum` as a parameter, supplied by the Airflow DAG.
        2.  Drops the existing target tables (`bert_staging.bpr_opt_filter` and `bert_reporting.tarifoption`) to ensure idempotency.
        3.  Creates the intermediate table `bert_staging.bpr_opt_filter` by joining `bert_master.sof_l_bpr_optionen_filter` with the dynamically named source table `bert_raw.sof_ta_bpr_opt_<v_datum>`.
        4.  Creates the final output table `bert_reporting.tarifoption` by aggregating data from `bert_staging.bpr_opt_filter`, categorizing and concatenating product descriptions.

*   **`dags/dw_bert_ausd_bp_ta_tarifoption.py`**
    *   **Role**: This is the Apache Airflow DAG definition file. It orchestrates the entire workflow on Google Cloud Composer. Its responsibilities include:
        1.  Defining the DAG's metadata (ID, schedule, start date, tags, documentation).
        2.  Implementing a `PythonOperator` (`get_v_datum_task`) to query `bert_staging.dwtk_meldungen` and determine the dynamic `v_datum` variable, pushing it to XCom.
        3.  Implementing a `BigQueryExecuteQueryOperator` (`full_sql_transformation_task`) to execute the `d_ausd_bp_ta_tarifoption.sql` script, passing the `v_datum` obtained from XCom as a parameter.
        4.  Defining the task dependencies to ensure correct execution order.

## 3. Key Design Decisions

*   **Orchestration Migration (UC4 to Airflow)**:
    *   **Decision**: Replaced the legacy UC4 scheduler with Apache Airflow (Cloud Composer).
    *   **Rationale**: Leverages a fully managed, cloud-native orchestration service, providing scalability, reliability, and integration with other Google Cloud services. This aligns with the broader cloud migration strategy.
    *   **Trade-offs**: Requires rewriting KornShell script logic into Python for parameter handling and date determination, and adapting error handling to Airflow's mechanisms.

*   **Data Processing Migration (Oracle to BigQuery)**:
    *   **Decision**: Transformed Oracle SQL/PLSQL logic into BigQuery SQL.
    *   **Rationale**: Utilizes BigQuery's serverless, highly scalable, and cost-effective data warehousing capabilities for data storage and transformation, significantly improving query performance and reducing operational overhead compared to a traditional Oracle database.
    *   **Trade-offs**: Required careful translation of Oracle-specific functions (`NVL`, `TO_CHAR`, `LEAD` with `ORDER BY NULL`) and syntax to BigQuery equivalents.

*   **Handling Dynamic Table Naming (`sof$ta_bpr_opt_text_<dynamic_date_variable>`)**:
    *   **Decision**: The dynamic date variable (`v_datum`) is now determined by a PythonOperator in the Airflow DAG and passed as a parameter to the BigQuery SQL script. The SQL script then constructs the table name using string concatenation (`bert_raw.sof_ta_bpr_opt_` || `v_datum`).
    *   **Rationale**: Maintains the original dynamic source table selection mechanism while integrating seamlessly with Airflow's parameter passing capabilities and BigQuery's flexible SQL.
    *   **Trade-offs**: Requires a robust upstream process to ensure the `bert_raw.sof_ta_bpr_opt_YYYYMMDD` tables are consistently ingested and available in BigQuery for each processing date.

*   **Reimplementation of Custom Oracle Concatenation Functions (`sof$ab_con.concatX`)**:
    *   **Decision**: Instead of reimplementing these as BigQuery UDFs (as initially considered in the design document), the logic was simplified and replaced by BigQuery's native `STRING_AGG` function combined with `SUBSTR` and `TRIM`.
    *   **Rationale**: `STRING_AGG` provides a direct and efficient way to concatenate strings within groups, which was determined to be functionally equivalent to the original custom Oracle functions for this specific use case. This avoids the complexity and potential performance overhead of custom UDFs.
    *   **Trade-offs**: This decision assumes the `concatX` functions primarily performed simple string aggregation. If they contained more complex, non-aggregating logic, this simplification might lead to functional discrepancies (though none were identified during the migration).

*   **Intermediate and Final Tables**:
    *   **Decision**: `sof$ta_bpr_opt_filter` and `sof$ta_tarifoption` are migrated to permanent BigQuery tables (`bert_staging.bpr_opt_filter` and `bert_reporting.tarifoption`).
    *   **Rationale**: BigQuery's architecture makes creating and managing permanent tables cost-effective and performant, eliminating the need for explicit temporary table management. The `DROP TABLE IF EXISTS` and `CREATE OR REPLACE TABLE` pattern ensures idempotency for each DAG run.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the following BigQuery datasets exist in your GCP project:
        *   `bert_staging`
        *   `bert_reporting`
        *   `bert_master`
        *   `bert_raw`

2.  **IAM Permissions**:
    *   The service account used by your Cloud Composer environment (Airflow worker service account) must have the following BigQuery roles for the GCP project and relevant datasets:
        *   `BigQuery Data Editor` (for `bert_staging`, `bert_reporting`)
        *   `BigQuery Data Viewer` (for `bert_staging`, `bert_master`, `bert_raw`)
        *   `BigQuery Job User` (for running BigQuery queries)
    *   Ensure the `google_cloud_default` Airflow connection is properly configured and has the necessary permissions.

3.  **Source Data Ingestion**:
    *   **`bert_staging.dwtk_meldungen`**: This table must be populated and kept up-to-date with data from the legacy `isbert_schema.dwtk_meldungen`.
    *   **`bert_master.sof_l_bpr_optionen_filter`**: This table must be populated with data from the legacy `isbert_schema.sof$ta_l_bpr_optionen_filter`.
    *   **`bert_raw.sof_ta_bpr_opt_YYYYMMDD`**: A daily process must be in place to ingest data into BigQuery tables named `bert_raw.sof_ta_bpr_opt_YYYYMMDD` (where `YYYYMMDD` is the date). This is critical for the dynamic table lookup.

4.  **Airflow DAG Deployment**:
    *   Upload the `dags/dw_bert_ausd_bp_ta_tarifoption.py` file to your Cloud Composer environment's DAGs folder.
    *   Upload the `sql/bigquery/d_ausd_bp_ta_tarifoption.sql` file to the `dags/sql/bigquery/` subdirectory within your DAGs folder (or adjust the path in the DAG).

5.  **Scheduling Configuration**:
    *   Update the `schedule` parameter in `dags/dw_bert_ausd_bp_ta_tarifoption.py` from `None` to the desired cron expression (e.g., `'0 0 * * *'` for daily execution at midnight UTC) to match the original UC4 schedule.

## 5. Known Gaps & Unresolved References

*   **Full KornShell Script Logic Reimplementation**: While the core date determination and SQL execution logic from `r_ausd_bp_ta_tarifoption.ksh` and `k_ausd_bp_ta_tarifoption.ksh` has been migrated to Python/Airflow, the full extent of all utility script invocations (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) and their potential side effects or complex environment setups has not been explicitly replicated. It is assumed that Airflow's native logging, error handling, and parameter management suffice. If any of these utilities contained critical business logic beyond basic orchestration, this could be a gap.
*   **Performance Tuning**: BigQuery handles parallelism automatically, but further performance optimization (e.g., partitioning and clustering of target tables `bert_staging.bpr_opt_filter` and `bert_reporting.tarifoption`) should be considered based on query patterns and data volume to optimize cost and performance.
*   **Error Handling Parity**: The original KornShell scripts included specific error handling (`WHENEVER SQLERROR EXIT FAILURE`, `f_alis_msgerr.ksh`). While Airflow provides robust retry mechanisms and integrates with Cloud Logging, a detailed comparison of the exact error handling and notification logic might be required if specific legacy behaviors are critical.
*   **`EVNT_TIME` Information**: The design document noted that `EVNT_TIME` information was not available. If this was a critical scheduling or data dependency, its absence might represent a gap in the overall workflow understanding.

## 6. Validation

To validate the successful migration and operation of the `DW.BERT_AUSD_BP_TA_TARIFOPTION` job:

1.  **Trigger the Airflow DAG**:
    *   From the Airflow UI, manually trigger the `dw_bert_ausd_bp_ta_tarifoption` DAG.
    *   Alternatively, use the `gcloud` CLI:
        ```bash
        gcloud composer environments run <YOUR_COMPOSER_ENV_NAME> \
            --location <YOUR_COMPOSER_LOCATION> \
            dags trigger dw_bert_ausd_bp_ta_tarifoption
        ```

2.  **Monitor DAG Execution**:
    *   Observe the DAG run in the Airflow UI. Ensure all tasks (`get_v_datum` and `execute_full_sql_transformation`) complete successfully without errors. Check task logs for any warnings or unexpected output.

3.  **Verify BigQuery Table Creation/Update**:
    *   After successful DAG execution, verify the existence and content of the following tables in BigQuery:
        *   `bert_staging.bpr_opt_filter`
        *   `bert_reporting.tarifoption`
    *   Check the table schemas to ensure they match expectations.

4.  **Data Validation (What "passing" means)**:
    *   **Row Counts**: Compare the row counts of `bert_reporting.tarifoption` with the corresponding output table from the legacy Oracle system for the same processing date.
    *   **Data Accuracy**:
        *   Perform sample queries on `bert_reporting.tarifoption` and compare specific `business_option`, `sonstige_option`, and `gprs_option` values for a selection of `cntrct_id`s against the legacy system's output.
        *   Pay close attention to the aggregated string columns to ensure the `STRING_AGG` function correctly replicates the original concatenation logic, including ordering and handling of `NULL` values.
    *   **Performance**: Monitor the execution time of the `full_sql_transformation_task` in BigQuery to ensure it meets performance requirements.

A "passing" validation means the Airflow DAG completes successfully, the target BigQuery tables are created/updated with the correct schema, and the data within `bert_reporting.tarifoption` is functionally identical to the output produced by the legacy system for the same input data.

## 7. Rollback Procedure

In case of critical issues or data discrepancies after go-live, the following rollback procedure can be followed:

1.  **Disable the Airflow DAG**:
    *   In the Airflow UI, toggle off the `dw_bert_ausd_bp_ta_tarifoption` DAG to prevent further runs.

2.  **Re-enable Legacy UC4 Job**:
    *   Reactivate the original `DW.BERT_AUSD_BP_TA_TARIFOPTION` job in the UC4 scheduler.

3.  **Data State**:
    *   The BigQuery tables (`bert_staging.bpr_opt_filter`, `bert_reporting.tarifoption`) created by the Airflow DAG will remain in BigQuery. If data corruption is suspected, these tables can be dropped or renamed, but the legacy system will continue to generate its output in Oracle.
    *   If the legacy system needs to overwrite or recreate the data in BigQuery (e.g., if the BigQuery tables were also targets for the legacy system), a separate process would be needed to facilitate that. However, based on the migration design, the BigQuery tables are new targets.

4.  **Investigation and Remediation**:
    *   Analyze the root cause of the issue in the migrated pipeline.
    *   Apply necessary fixes to the Airflow DAG, BigQuery SQL, or source data ingestion processes.
    *   Once fixes are validated in a lower environment, the migration can be re-attempted.
```