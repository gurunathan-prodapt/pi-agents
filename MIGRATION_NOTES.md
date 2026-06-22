# MIGRATION_NOTES.md: DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Summary

The `DW.BERT_AUSD_BP_TA_BCP_ICCID` job, originally orchestrated by UC4 and KornShell scripts executing Oracle SQL*Plus, has been migrated to Google Cloud Platform.

The job's purpose is to prepare "Basisprodukte" (basic products) for BERT's demand scoring system by extracting and enriching contract and ICCID-related data into a target table.

The migration involved:
*   **Orchestration**: Translating the UC4 job definition and KornShell scripting logic to an **Apache Airflow DAG** running on Google Cloud Composer.
*   **Data Storage & Transformation**: Migrating Oracle tables to **BigQuery** and translating the core Oracle SQL transformation logic to **Standard BigQuery SQL**.

The core SQL transformation was initially flagged with a `retire` migration bucket, indicating a strong recommendation for re-evaluation of its business value. For the purpose of this migration, the logic has been translated, but the recommendation for re-evaluation stands.

## 2. Generated Artifacts

The following artifacts were generated as part of this migration:

*   **`ddl/bigquery/dwtk_meldungen.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the `dwtk_meldungen` table. This table serves as a source for determining the `v_datum` variable, which is derived from the `timecreated` column.
*   **`ddl/bigquery/sof_ta_bpr_bcp.sql`**
    *   **Role**: BigQuery DDL script to create the `sof_ta_bpr_bcp` table. This table is a primary source for "Basisprodukt" data, used in the main transformation query.
*   **`ddl/bigquery/sof_ta_iccid_vertrag.sql`**
    *   **Role**: BigQuery DDL script to create the `sof_ta_iccid_vertrag` table. This table provides ICCID contract data, joined with `sof_ta_bpr_bcp` in the transformation.
*   **`ddl/bigquery/sof_ta_bcp_iccid.sql`**
    *   **Role**: BigQuery DDL script to create the `sof_ta_bcp_iccid` table. This is the target table where the transformed and enriched data is loaded.
*   **`dags/dw_bert_ausd_bp_ta_bcp_iccid_dag.py`**
    *   **Role**: The main Apache Airflow DAG responsible for orchestrating the entire job. It replaces the legacy UC4 job and KornShell scripts. It includes tasks for extracting the `v_datum` and executing the BigQuery transformation.

## 3. Key Design Decisions

*   **Orchestration Migration to Airflow (Composer)**: The legacy UC4 job and KornShell scripts were migrated to an Airflow DAG. This decision leverages Google Cloud Composer's managed Airflow service, providing a robust, scalable, and cloud-native orchestration platform with Python's flexibility for custom logic and native GCP service integration.
*   **Data Platform Migration to BigQuery**: All Oracle source and target tables were migrated to BigQuery. BigQuery was chosen for its serverless architecture, petabyte-scale analytics capabilities, high performance, and cost-effectiveness, aligning with GCP's data warehousing strategy.
*   **SQL Transformation to Standard BigQuery SQL**: The Oracle SQL*Plus script was translated into Standard BigQuery SQL. This ensures compatibility with the target data platform and allows BigQuery's optimized query engine to handle the data processing efficiently. Oracle-specific syntax (e.g., `NVL`, `TO_CHAR`, hints) was converted to BigQuery equivalents (`IFNULL`, `FORMAT_DATE`).
*   **Truncate-and-Load Strategy**: The original Oracle `TRUNCATE TABLE` followed by `INSERT INTO` was replaced with BigQuery's `INSERT OVERWRITE` statement for the main transformation. This is a common and efficient pattern in BigQuery for full table refreshes, ensuring idempotency and simplifying the operation into a single atomic statement.
*   **Parameter Handling via Airflow**: Legacy KornShell parameter parsing (`Stichtag`, `Wiederanlaufwert`) is now handled by Airflow DAG parameters and XComs. This integrates parameter management directly into the orchestration layer, making it more transparent and manageable within the Airflow UI.
*   **`retire` Bucket Acknowledgment**: Despite the `retire` classification for the core SQL, the decision was made to translate and implement the existing logic in BigQuery. This ensures functional parity in the interim, while strongly recommending a re-evaluation of the job's necessity or a potential redesign (B4 item) by business stakeholders.
*   **Replacement of Utility Scripts**: The various KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, etc.) were not directly migrated. Their functionalities (environment setup, error handling, date formatting) are now handled by Airflow's native features, Python's standard library, or BigQuery's SQL functions, streamlining the codebase and removing shell script dependencies.

## 4. Manual Steps Before Go-Live

Before the Airflow DAG can be deployed and run in production, the following manual steps must be completed:

1.  **Google Cloud Project and BigQuery Dataset Setup**:
    *   Ensure the target Google Cloud Project (`<project>`) is provisioned.
    *   Create the BigQuery dataset (`<dataset>`) where all tables will reside. This can be done via the GCP Console, `bq` CLI, or Terraform.

2.  **BigQuery Table Creation**:
    *   Execute the DDL scripts provided in `ddl/bigquery/` to create all necessary tables in the target BigQuery dataset:
        *   `ddl/bigquery/dwtk_meldungen.sql`
        *   `ddl/bigquery/sof_ta_bpr_bcp.sql`
        *   `ddl/bigquery/sof_ta_iccid_vertrag.sql`
        *   `ddl/bigquery/sof_ta_bcp_iccid.sql`
    *   **Important**: Review the `sof_ta_bcp_iccid.sql` DDL. The design document recommends considering partitioning and clustering strategies (e.g., `PARTITION BY`, `CLUSTER BY CNTRCT_ID_REF`). These should be uncommented and configured based on expected data volume and query patterns before table creation.

3.  **Initial Data Load (Backfill)**:
    *   Load historical data from the source Oracle tables (`dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`) into their respective BigQuery counterparts. This is typically done using a one-time data transfer service (e.g., BigQuery Data Transfer Service, custom ETL, or `gsutil cp` from Cloud Storage).

4.  **IAM Permissions**:
    *   The Service Account associated with the Cloud Composer environment (which runs the Airflow DAG) must have the following BigQuery permissions:
        *   `BigQuery Data Editor` role on the target BigQuery dataset (`<project>.<dataset>`) to create, write, and truncate tables.
        *   `BigQuery Data Viewer` role on the source BigQuery dataset(s) containing `dwtk_meldungen`, `sof_ta_bpr_bcp`, and `sof_ta_iccid_vertrag` to read data.

5.  **Airflow Variables Configuration**:
    *   Set the following Airflow Variables in your Composer environment:
        *   `gcp_project_id`: The ID of your Google Cloud Project (e.g., `your-gcp-project-id`).
        *   `bigquery_dataset_id`: The ID of your BigQuery dataset (e.g., `your_dataset_name`).
    *   Ensure the `google_cloud_default` connection is correctly configured in Airflow, pointing to the appropriate GCP project.

6.  **Scheduling Configuration**:
    *   The `schedule_interval` in the `dw_bert_ausd_bp_ta_bcp_iccid_dag.py` is currently set to `None`. This must be updated to the desired production schedule (e.g., `'@daily'`, `timedelta(days=1)`, or a specific cron expression) before deployment.

## 5. Known Gaps & Unresolved References

*   **`retire` Migration Bucket for Core SQL**: The core SQL transformation (`d_ausd_bp_ta_bcp_iccid.sql`) was categorized under the `retire` migration bucket. While the logic has been translated for functional parity, a thorough business analysis is still required to confirm if this job can be truly retired, if its functionality needs to be absorbed into another process, or if it requires a significant redesign (B4 item).
*   **Parameter `p_wiederanlaufWert` (Restart Value)**: The legacy KornShell scripts handled a `wiederanlaufwert` (restart value) parameter. While the Airflow DAG defines this parameter, the current BigQuery SQL transformation does not explicitly incorporate it into its filtering logic. If the original job used this parameter to perform incremental loads or restart from a specific point, the BigQuery SQL will need modification to utilize `params.wiederanlaufwert` (e.g., in a `WHERE` clause).
*   **`v_datum` Usage in Main Query**: The `extract_v_datum_task` successfully retrieves `v_datum` (max `timecreated` from `dwtk_meldungen`) and pushes it to XCom. However, the `transform_load_data` BigQuery query does not currently utilize this `v_datum`. If the original Oracle SQL intended to use `v_datum` for filtering or other logic within the main `INSERT` statement, the BigQuery SQL will need to be updated to retrieve this value from XCom and incorporate it.
*   **Target Table Partitioning/Clustering**: The DDL for `sof_ta_bcp_iccid` includes commented-out recommendations for partitioning and clustering. These are crucial for BigQuery performance and cost optimization, especially for large tables. A decision on the appropriate strategy (e.g., `CNTRCT_ID_REF` for clustering) needs to be made and implemented in the DDL.
*   **Error Handling and Alerting**: While Airflow provides native error handling and Cloud Logging captures task logs, specific custom alerting mechanisms (e.g., email, PagerDuty) that existed in the legacy UC4/KornShell environment might need to be configured within Airflow or Cloud Monitoring.

## 6. Validation

To ensure the migrated job functions correctly, perform the following validation steps:

1.  **Run the Airflow DAG**:
    *   Manually trigger the `dw_bert_ausd_bp_ta_bcp_iccid` DAG from the Airflow UI.
    *   Monitor the task execution in the Airflow UI and Cloud Logging for any errors or warnings.

2.  **Verify Data in BigQuery**:
    *   After a successful DAG run, query the target table:
        ```sql
        SELECT * FROM `<project>.<dataset>.sof_ta_bcp_iccid` LIMIT 100;
        ```
    *   Check the table details in the BigQuery UI to confirm the last modified time and row count.

3.  **"Passing" Criteria**:
    *   **DAG Completion**: The Airflow DAG completes successfully without any failed tasks.
    *   **Target Table Population**: The `sof_ta_bcp_iccid` table in BigQuery is populated with data.
    *   **Row Count Match**: The number of rows in the BigQuery `sof_ta_bcp_iccid` table matches the row count from the source Oracle `sof$ta_bcp_iccid` table (or is within an acceptable variance, if applicable).
    *   **Data Accuracy**: A sample of data (e.g., 100-1000 rows) from the BigQuery target table should be compared against the corresponding data in the Oracle source table to ensure values for `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, and `TN_IMSI_HLR` are identical.
    *   **Performance**: The DAG completes within an acceptable time frame, meeting any defined SLA.
    *   **No Errors in Logs**: Cloud Logging for the DAG run shows no critical errors or unexpected warnings.

## 7. Rollback Procedure

In case of critical failure or data integrity issues after go-live, follow these steps to roll back:

1.  **Disable Airflow DAG**:
    *   Immediately disable the `dw_bert_ausd_bp_ta_bcp_iccid` DAG in the Airflow UI to prevent further execution.

2.  **Re-enable Legacy Job**:
    *   Re-enable the original `DW.BERT_AUSD_BP_TA_BCP_ICCID` job in UC4 to resume production processing on the Oracle platform.

3.  **Data Rollback (if necessary)**:
    *   If the `sof_ta_bcp_iccid` table in BigQuery was corrupted or incorrectly loaded:
        *   **BigQuery Time Travel**: Utilize BigQuery's time travel feature to restore the table to a state before the problematic run (e.g., `CREATE OR REPLACE TABLE ... AS SELECT * FROM <table_name> FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`).
        *   Alternatively, if a backup strategy is in place, restore the table from the last known good state.
        *   If the issue is with source data, coordinate with data owners to address the upstream problem.

4.  **Revert Code (if necessary)**:
    *   If the issue is identified as a bug in the Airflow DAG code, revert the `dags/dw_bert_ausd_bp_ta_bcp_iccid_dag.py` file to a previous, stable version in your version control system and redeploy it to Composer.

5.  **Investigation**:
    *   Thoroughly investigate the root cause of the failure using Airflow logs, Cloud Logging, and BigQuery job history. Rectify the issue before attempting another migration or re-enabling the new DAG.