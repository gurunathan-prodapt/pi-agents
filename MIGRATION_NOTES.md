# MIGRATION_NOTES for DW.BERT_AUSD_V_TA_CNTRCT_CRS3

## 1. Summary

The ETL job `DW.BERT_AUSD_V_TA_CNTRCT_CRS3`, originally orchestrated by UC4, utilizing KornShell scripts for control, and executing Oracle SQL for data transformation, has been migrated to Google Cloud Platform (GCP). The target platform leverages Apache Airflow on Cloud Composer for orchestration, Python scripts for job control and execution, and Google BigQuery as the target data warehouse. The primary function of this job is to update contract data, including twin-bill information, into the `sof_ta_cntrct_crs3` table.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`ddl/bigquery/isbert_schema.dwtk_meldungen.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `dwtk_meldungen` table within the `isbert_schema` dataset. This table is a migration of the Oracle `isbert_schema.dwtk_meldungen` table, used for retrieving a processing date.
*   **`ddl/bigquery/sof_schema.sof_ta_cntrct_crs2.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_cntrct_crs2` table within the `sof_schema` dataset. This table is a migration of the Oracle `sof$ta_cntrct_crs2` table, which serves as a primary source for contract data.
*   **`ddl/bigquery/sof_schema.sof_ta_cntrct_crs3.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_cntrct_crs3` table within the `sof_schema` dataset. This table is a migration of the Oracle `sof$ta_cntrct_crs3` table, which is the target table for the transformed contract data.
*   **`scripts/dw_bert_ausd_v_ta_cntrct_crs3/d_ausd_v_ta_cntrct_crs3_bq.sql`**
    *   **Role:** The core BigQuery SQL script containing the data transformation logic. It is a direct conversion of the original Oracle `d_ausd_v_ta_cntrct_crs3.sql`. This script reads from `isbert_schema.dwtk_meldungen` and `sof_schema.sof_ta_cntrct_crs2`, truncates `sof_schema.sof_ta_cntrct_crs3`, and inserts processed contract data, including twin-bill information.
*   **`scripts/dw_bert_ausd_v_ta_cntrct_crs3/contract_data_updater.py`**
    *   **Role:** A Python script that replaces the functionality of the legacy KornShell wrapper (`r_ausd_v_ta_cntrct_crs3.ksh`) and control (`k_ausd_v_ta_cntrct_crs3.ksh`) scripts. It handles parameter parsing (e.g., GCP project ID, SQL file path) and executes the `d_ausd_v_ta_cntrct_crs3_bq.sql` script against BigQuery using the `google-cloud-bigquery` client library. It also incorporates basic logging.
*   **`dags/dw_bert_ausd_v_ta_cntrct_crs3_dag.py`**
    *   **Role:** The Apache Airflow DAG definition file. This DAG orchestrates the execution of the `contract_data_updater.py` script. It defines the job's schedule (currently `None` pending further analysis), dependencies, and metadata for Airflow.

## 3. Key Design Decisions

*   **Orchestration Migration (UC4 to Airflow):** The legacy UC4 job definition was replaced by an Apache Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs3_dag.py`). Airflow on Cloud Composer provides a managed, scalable, and robust orchestration platform native to GCP, offering better visibility, monitoring, and integration with other GCP services compared to UC4.
*   **Transformation Logic Migration (Oracle SQL to BigQuery SQL):** The core data transformation logic from `d_ausd_v_ta_cntrct_crs3.sql` was directly converted to BigQuery SQL (`d_ausd_v_ta_cntrct_crs3_bq.sql`). This approach minimizes changes to the business logic, leveraging BigQuery's powerful SQL engine and columnar storage for efficient processing.
*   **Control Script Re-implementation (KornShell to Python):** The KornShell wrapper and control scripts (`r_ausd_v_ta_cntrct_crs3.ksh`, `k_ausd_v_ta_cntrct_crs3.ksh`) were re-implemented in Python (`contract_data_updater.py`). Python is the preferred language for Airflow tasks and GCP interactions, offering better maintainability, error handling, and access to GCP client libraries compared to shell scripting.
*   **Data Warehouse Migration (Oracle to BigQuery):** All Oracle tables (`sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`, `isbert_schema.dwtk_meldungen`) were migrated to BigQuery. BigQuery offers serverless, highly scalable, and cost-effective data warehousing, aligning with GCP's data analytics ecosystem.
*   **Handling Oracle Specifics:**
    *   **`TRUNCATE` Procedure:** The legacy use of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for `TRUNCATE TABLE` was replaced by a direct `TRUNCATE TABLE` statement in BigQuery SQL, as BigQuery supports this as a standard DDL.
    *   **`PARALLEL` Hints:** Oracle's `PARALLEL` hints were removed as BigQuery automatically handles query parallelism and optimization, making such hints unnecessary and potentially counterproductive.
    *   **Schema Mapping:** Oracle schema prefixes (`isbert_schema`, `sof$`) were mapped to BigQuery datasets (`isbert_schema`, `sof_schema`) for clear organization.
*   **Logging and Error Handling:** Cloud Logging is implicitly used by Python scripts running on GCP, replacing custom KornShell logging mechanisms. The Python script includes basic logging for execution status.

**Notable Trade-offs:**

*   **Provisional Scheduling:** The Airflow DAG's schedule is currently set to `None` because the full UC4 job plan (JOBP) and schedule (JSCH) details, including `EVNT_TIME`, were not available. This requires a manual step to define the final schedule.
*   **`v_datum` Variable Usage:** The `v_datum` variable is declared and set in the BigQuery SQL, mirroring the Oracle script. However, its value is not explicitly used in the main `INSERT` statement within the provided SQL. This might indicate it was used for logging or other conditional logic in the original context not fully captured, or it's simply unused. The current migration preserves its declaration.
*   **KornShell Utility Script Re-implementation:** The detailed logic of several sourced KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) was not fully analyzed. The Python script provides a functional equivalent for parameter handling and BigQuery execution, but any subtle behaviors or environment setups from these utilities might need further review if issues arise.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job in a production environment, the following manual steps are required:

1.  **GCP Project Setup:**
    *   Ensure the GCP project specified by `my-gcp-project` (or your actual project ID) is correctly configured and has billing enabled.
2.  **BigQuery Dataset Creation:**
    *   Create the BigQuery datasets `isbert_schema` and `sof_schema` in your GCP project.
        ```bash
        bq mk --dataset --default_location=US my-gcp-project:isbert_schema
        bq mk --dataset --default_location=US my-gcp-project:sof_schema
        ```
3.  **BigQuery Table Creation:**
    *   Execute the generated DDL scripts to create the necessary tables in BigQuery:
        ```bash
        bq query --use_legacy_sql=false < ddl/bigquery/isbert_schema.dwtk_meldungen.sql
        bq query --use_legacy_sql=false < ddl/bigquery/sof_schema.sof_ta_cntrct_crs2.sql
        bq query --use_legacy_sql=false < ddl/bigquery/sof_schema.sof_ta_cntrct_crs3.sql
        ```
4.  **Initial Data Ingestion:**
    *   Ingest historical data from the legacy Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`) into their corresponding BigQuery tables. This can be done using:
        *   GCP Data Transfer Service for Oracle.
        *   Datastream for continuous replication.
        *   Exporting data from Oracle to CSV/Parquet and loading into BigQuery.
        *   Third-party tools like Fivetran.
5.  **IAM Permissions:**
    *   Grant the service account used by your Cloud Composer environment (or the execution environment for the Python script) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on the `isbert_schema` and `sof_schema` datasets (or specific tables) to allow reading from source tables and truncating/inserting into the target table.
        *   `BigQuery Job User` to run BigQuery queries.
6.  **Code Deployment:**
    *   Upload the `contract_data_updater.py` Python script and `d_ausd_v_ta_cntrct_crs3_bq.sql` BigQuery SQL file to a Google Cloud Storage (GCS) bucket accessible by your Airflow environment. A common practice is to place them in a subdirectory within the Airflow DAGs bucket, e.g., `gs://your-airflow-bucket/dags/scripts/dw_bert_ausd_v_ta_cntrct_crs3/`.
    *   Upload the `dw_bert_ausd_v_ta_cntrct_crs3_dag.py` file to the `dags` folder of your Cloud Composer environment's GCS bucket (e.g., `gs://your-airflow-bucket/dags/`).
7.  **Airflow Configuration:**
    *   **`gcp_project_id` Placeholder:** Update the `gcp_project_id` variable in `dags/dw_bert_ausd_v_ta_cntrct_crs3_dag.py` with your actual GCP Project ID.
    *   **Script Path:** Ensure the `bash_command` in the Airflow DAG correctly references the GCS path where `contract_data_updater.py` and `d_ausd_v_ta_cntrct_crs3_bq.sql` are stored. If they are in the same GCS bucket as the DAGs, relative paths might work, or you might need to use `gsutil cp` to copy them to the Airflow worker's local filesystem before execution. For simplicity, the current DAG assumes they are locally accessible relative to the DAG file.
    *   **Scheduling:** Based on the analysis of the legacy UC4 `EVNT_TIME` and any other dependencies, update the `schedule` parameter in the Airflow DAG from `None` to the appropriate cron expression or timedelta.
8.  **Secrets Management (if applicable):**
    *   While BigQuery client libraries typically use service account credentials, if any other sensitive information (e.g., API keys for external systems, though none are identified here) were to be introduced, ensure they are securely managed using Secret Manager.

## 5. Known Gaps & Unresolved References

*   **UC4 Workflow Context:** The Airflow DAG's `schedule` is currently `None`. A thorough analysis of the legacy UC4 `DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml` (and any parent JOBP/JSCH) is required to determine the exact scheduling frequency and any upstream/downstream job dependencies. This will inform the final `schedule` parameter and potential `external_task_sensor` configurations in Airflow.
*   **KornShell Utility Scripts:** The full functionality of the sourced KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) was not exhaustively re-implemented. While the core logic is covered, any subtle environment setups, custom error handling, or parameter processing from these scripts might need further investigation if discrepancies arise.
*   **`v_datum` Variable Usage:** The `v_datum` variable is declared and populated in the BigQuery SQL but not explicitly used in the `INSERT` statement. Its original purpose in the Oracle script (e.g., for logging, auditing, or conditional logic not present in the provided snippet) should be confirmed.
*   **Data Type Mismatches:** While the DDLs provide a reasonable mapping, a comprehensive data type and precision/scale comparison between the Oracle source and BigQuery target tables is recommended to ensure no data loss or unexpected behavior.
*   **DB-Link to CARMEN DB (`@pcrs1`):** The design document mentions a `DB-Link on CARMEN DB (@pcrs1)` as an external dependency, although it's not explicitly used in the provided Oracle SQL. If the CARMEN database is a source for other related processes or if this job implicitly relies on it, its migration or integration into GCP (e.g., via Datastream, Fivetran, or direct ingestion) needs to be addressed.
*   **`my-gcp-project` Placeholder:** The generated code uses `my-gcp-project` as a placeholder for the GCP Project ID. This must be replaced with the actual project ID before deployment.

## 6. Validation

Validation should cover data integrity, functional correctness, and performance.

1.  **Data Validation:**
    *   **Row Counts:** Compare row counts of `sof_ta_cntrct_crs3` in BigQuery with `sof$ta_cntrct_crs3` in Oracle after a full run.
    *   **Checksums/Hashes:** Calculate checksums or hashes of key columns or entire rows for a representative sample of data in both source and target tables to ensure data integrity.
    *   **Sample Data Comparison:** Select a random sample of records from the Oracle `sof$ta_cntrct_crs3` table and manually verify that the corresponding records in BigQuery `sof_ta_cntrct_crs3` match exactly. Pay close attention to `twinbill` and `twin_vertrag_id` columns.
    *   **Edge Cases:** Test with data that includes known twin-bill scenarios, contracts with `cntrct_ty` 10 or 20, and contracts without parents to ensure the `UNION ALL` logic is correctly applied.
2.  **Functional Validation:**
    *   **Airflow DAG Execution:** Trigger the `dw_bert_ausd_v_ta_cntrct_crs3` DAG in Airflow. Verify that all tasks complete successfully without errors.
    *   **BigQuery Job Status:** Check the BigQuery job history to confirm the SQL execution was successful.
    *   **Logging:** Review Cloud Logging for the Airflow tasks and the Python script to ensure no unexpected errors or warnings.
3.  **Performance Validation:**
    *   Compare the execution time of the Airflow DAG (specifically the BigQuery SQL execution) with the historical execution time of the legacy Oracle job. BigQuery is expected to be faster, but monitor for any regressions.

**"Passing" Criteria:**

*   The Airflow DAG completes successfully without any failed tasks.
*   The BigQuery job for `d_ausd_v_ta_cntrct_crs3_bq.sql` completes successfully.
*   Row counts in `sof_ta_cntrct_crs3` in BigQuery match the expected counts from the Oracle source.
*   A statistically significant sample of data in `sof_ta_cntrct_crs3` in BigQuery matches the corresponding data in the legacy Oracle table.
*   The job completes within the defined Service Level Agreement (SLA).
*   Cloud Logging shows no critical errors or unexpected warnings.

## 7. Rollback Procedure

In case of critical issues during or after go-live, the following rollback procedure can be initiated:

1.  **Stop New Runs:**
    *   In the Airflow UI, pause the `dw_bert_ausd_v_ta_cntrct_crs3` DAG to prevent any further executions.
2.  **Revert to Legacy System:**
    *   Re-enable the original `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` job in UC4.
3.  **Data Restoration (if necessary):**
    *   If the BigQuery `sof_ta_cntrct_crs3` table was corrupted or incorrectly updated, use BigQuery's time travel feature to restore the table to a state before the problematic run.
        ```sql
        CREATE OR REPLACE TABLE `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` AS
        SELECT * FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X MINUTE);
        ```
        (Replace `X` with the appropriate number of minutes/hours to revert to a known good state).
    *   Alternatively, if backups were taken, restore from the most recent good backup.
4.  **Investigate and Remediate:**
    *   Analyze logs (Cloud Logging, Airflow logs) to identify the root cause of the issue.
    *   Address the identified problem in the BigQuery SQL, Python script, or Airflow DAG.
    *   Perform thorough testing in a non-production environment before attempting re-deployment.
5.  **Clean Up (Optional):**
    *   If the rollback is permanent, remove the Airflow DAG and associated scripts from GCS.