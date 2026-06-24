# MIGRATION_NOTES: DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Summary

The `DW.BERT_AUSD_BP_TA_BCP_ICCID` job, originally an Oracle-based data preparation and extraction process orchestrated by UC4 and KornShell scripts, has been migrated to Google Cloud Platform. This job is responsible for populating the `SOF$TA_BCP_ICCID` table with enriched ICCID-related basic product data for the BERT demand scoring system.

The migration involved:
*   **Orchestration**: From UC4/Automic to Google Cloud Composer (Apache Airflow).
*   **Scripting**: From KornShell (`.ksh`) to Python (`.py`) scripts.
*   **Database & SQL**: From Oracle PL/SQL (SQL*Plus) to Google BigQuery SQL.
*   **Data Storage**: All referenced Oracle tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`, `SOF$TA_ICCID_VERTRAG`, `SOF$TA_BCP_ICCID`) are now hosted in BigQuery.

The migrated solution leverages cloud-native services for improved scalability, maintainability, and integration within the Google Cloud ecosystem.

## 2. Generated artifacts

The migration process generated the following files and BigQuery tables:

*   **`d_ausd_bp_ta_bcp_iccid_bq.sql`**
    *   **Role**: Contains the core BigQuery SQL logic for data transformation. This script truncates the target table `sof_schema.ta_bcp_iccid_bq` and then inserts enriched data by joining `sof_schema.ta_bpr_bcp_bq` and `sof_schema.ta_iccid_vertrag_bq`, incorporating date variables derived from `isbert_schema.dwtk_meldungen_bq`.
*   **`k_ausd_bp_ta_bcp_iccid.py`**
    *   **Role**: A Python control script that replaces the original `k_ausd_bp_ta_bcp_iccid.ksh`. It handles parameter parsing, date validation, and orchestrates the execution of `d_ausd_bp_ta_bcp_iccid_bq.sql` via the BigQuery client.
*   **`r_ausd_bp_ta_bcp_iccid.py`**
    *   **Role**: The main Python orchestration script, replacing `r_ausd_bp_ta_bcp_iccid.ksh`. It manages high-level parameters (snapshot date, restart value), sets up the environment, and invokes `k_ausd_bp_ta_bcp_iccid.py` to perform the data processing.
*   **`dw_bert_ausd_bp_ta_bcp_iccid_dag.py`**
    *   **Role**: The Apache Airflow DAG definition file. This Python script defines the workflow, scheduling, and task dependencies, replacing the UC4 job definition. It orchestrates the execution of `r_ausd_bp_ta_bcp_iccid.py`.
*   **BigQuery Tables**:
    *   `isbert_schema.dwtk_meldungen_bq`: Migrated metadata table.
    *   `sof_schema.ta_bpr_bcp_bq`: Migrated source table.
    *   `sof_schema.ta_iccid_vertrag_bq`: Migrated source table.
    *   `sof_schema.ta_bcp_iccid_bq`: Migrated target table for the processed data.

## 3. Key design decisions

*   **Cloud-Native Orchestration with Airflow**: The UC4 job was migrated to Google Cloud Composer (Apache Airflow) to leverage its cloud-native capabilities, Python-based DAGs for flexible workflow definition, robust scheduling, monitoring, and seamless integration with other Google Cloud services. This provides a scalable and modern orchestration platform.
*   **Python for Scripting Logic**: KornShell scripts were translated into Python. This decision was driven by Python's widespread adoption, rich ecosystem of libraries (including Google Cloud client libraries), improved readability, maintainability, and better error handling compared to shell scripting. It also facilitates easier integration within the Airflow environment.
*   **BigQuery for Data Warehousing**: Oracle database tables and SQL logic were migrated to Google BigQuery. BigQuery was chosen for its serverless architecture, petabyte-scale analytics capabilities, high performance for analytical queries, and cost-effectiveness, aligning with the target cloud data warehousing strategy.
*   **Direct SQL Translation**: The core Oracle SQL logic was directly translated to BigQuery SQL. This approach minimizes changes to the business logic, ensuring functional equivalence. Oracle-specific hints (e.g., `/*+ parallel(...) */`) were removed as BigQuery's query optimizer automatically handles parallelism. The `TRUNCATE` and `INSERT` pattern was adapted to BigQuery's DML, and explicit `COMMIT` statements were removed due to BigQuery's auto-commit model.
*   **Centralized Configuration and Logging**: Shell-based environment setup (`.dw_init`, utility scripts) was replaced by Airflow variables, connections, and Python modules. Logging was standardized using Python's logging module, integrating with Google Cloud Logging for centralized log management and monitoring.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset and Table Creation**:
    *   Ensure the BigQuery datasets `isbert_schema` and `sof_schema` exist in the target Google Cloud project.
    *   Create the target BigQuery tables:
        *   `isbert_schema.dwtk_meldungen_bq`
        *   `sof_schema.ta_bpr_bcp_bq`
        *   `sof_schema.ta_iccid_vertrag_bq`
        *   `sof_schema.ta_bcp_iccid_bq`
    *   **Data Migration**: Ensure that the source data from the original Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`) has been successfully migrated and loaded into their respective BigQuery counterparts (`_bq` suffix). This typically involves a separate data migration effort (e.g., using Data Transfer Service, custom ETL, or batch loads).

2.  **IAM Permissions**:
    *   The Google Cloud service account associated with the Cloud Composer environment (Airflow worker nodes) must have the necessary IAM roles and permissions:
        *   `BigQuery Data Editor` on the `sof_schema` dataset (for `ta_bcp_iccid_bq`).
        *   `BigQuery Data Viewer` on the `isbert_schema` and `sof_schema` datasets (for `dwtk_meldungen_bq`, `ta_bpr_bcp_bq`, `ta_iccid_vertrag_bq`).
        *   `BigQuery Job User` to run BigQuery queries.
        *   `Cloud Logging Writer` to write logs to Cloud Logging.
        *   Permissions to execute Python scripts within the Airflow environment.

3.  **Airflow Configuration**:
    *   **Upload DAG**: Upload the `dw_bert_ausd_bp_ta_bcp_iccid_dag.py` file, along with the Python scripts (`k_ausd_bp_ta_bcp_iccid.py`, `r_ausd_bp_ta_bcp_iccid.py`) and the BigQuery SQL file (`d_ausd_bp_ta_bcp_iccid_bq.sql`), to the designated DAGs folder in the Cloud Composer environment's GCS bucket.
    *   **Airflow Variables/Connections**: If any parameters or sensitive information (e.g., specific project IDs, dataset names, or non-default BigQuery connections) are not hardcoded or derived, they should be configured as Airflow Variables or Connections.

4.  **Scheduling**:
    *   Once uploaded, the DAG will appear in the Airflow UI. Configure its schedule (e.g., daily, hourly) to match the original UC4 job's execution frequency. Ensure the DAG is unpaused.

## 5. Known gaps & unresolved references

The following items were identified as gaps or require further resolution:

*   **`d_ausd_bp_ta_bcp_iccid.sql` in `retire` bucket**: The core SQL script was flagged for `retire` in the source inventory. This implies a recommendation to decommission or significantly redesign this component rather than a direct migration. A definitive decision is needed:
    *   If the functionality is no longer required, the entire migrated job should be decommissioned.
    *   If the functionality is still required, the current migration assumes a direct re-implementation to BigQuery. However, a deeper analysis might reveal opportunities for redesign (B4 item) to better align with current business needs or BigQuery best practices.
*   **Missing `file_purpose` for source files**: The original inventory lacked explicit `file_purpose` values for most files. While summaries provided context, clearer roles (e.g., `etl`, `utility`, `orchestrator`) would have aided in more precise target component mapping.
*   **Commented-out FOS Job Management**: The original KornShell scripts contain commented-out references to `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`, and `FOSHoleLadedatum`. These suggest interaction with an external job management or metadata system. It is unclear if this functionality is still active or required. If needed, its migration or replacement with a BigQuery-native metadata management solution or another Google Cloud service must be addressed.
*   **SQL*Plus Specifics**: While the core SQL was translated, any remaining `SQL*Plus` specific commands (e.g., `prompt`, `start`, `spool`, `whenever sqlerror continue/exit failure`) in the original script must be confirmed as fully removed or appropriately handled in the Python execution context. Error handling is now managed at the Python script and Airflow operator level.

## 6. Validation

To validate the successful migration and operation of the `DW.BERT_AUSD_BP_TA_BCP_ICCID` job:

1.  **Run the Airflow DAG**:
    *   Access the Airflow UI for your Cloud Composer environment.
    *   Locate the `dw_bert_ausd_bp_ta_bcp_iccid_dag`.
    *   Trigger a manual run of the DAG. If the job accepts parameters (e.g., snapshot date), ensure they are provided correctly via the Airflow UI or as DAG run configuration.
    *   Monitor the DAG run in the Airflow UI, observing the status of individual tasks. Review task logs for any errors or warnings.

2.  **What "passing" means**:
    *   **Successful DAG Completion**: The `dw_bert_ausd_bp_ta_bcp_iccid_dag` completes successfully in Airflow, with all tasks marked as "success".
    *   **Target Table Population**: The target BigQuery table `sof_schema.ta_bcp_iccid_bq` is populated with data. Verify that the table is not empty (unless expected for the given run parameters).
    *   **Row Count Validation**: Compare the row count of `sof_schema.ta_bcp_iccid_bq` with the expected row count based on the source data and transformation logic. Ideally, this should match the row count produced by the original Oracle job for the same input data.
    *   **Data Quality & Content Validation**:
        *   Perform spot checks on a sample of records in `sof_schema.ta_bcp_iccid_bq` to ensure data integrity and correctness.
        *   Verify that the join condition (`bp.cntrct_id_ref = ic.cntrct_id`) is correctly applied and that the `TN_ICCID` and `TN_IMSI_HLR` columns are populated as expected from `sof_schema.ta_iccid_vertrag_bq`.
        *   Confirm that the `v_datum` logic (derived from `dwtk_meldungen_bq`) is correctly applied if it influences the data selection.
    *   **Output Comparison**: For a specific run date, compare a representative sample of the output data in `sof_schema.ta_bcp_iccid_bq` with the output generated by the original Oracle job in `sof$ta_bcp_iccid`. This is the most robust validation step.

## 7. Rollback procedure

In the event of critical failure or incorrect data processing by the migrated job, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Pause Airflow DAG**: Immediately pause or disable the `dw_bert_ausd_bp_ta_bcp_iccid_dag` in the Airflow UI to prevent further execution.
    *   **Notify Stakeholders**: Inform relevant teams and stakeholders about the issue and the initiation of the rollback.

2.  **Revert to Original System**:
    *   **Re-enable UC4 Job**: Re-enable and, if necessary, manually trigger the original UC4 job `DW.BERT_AUSD_BP_TA_BCP_ICCID` to resume normal operations using the legacy system.

3.  **Data Restoration (if necessary)**:
    *   **BigQuery Target Table**: If the `sof_schema.ta_bcp_iccid_bq` table was corrupted or incorrectly populated by the new job, restore it to a known good state. This can be done by:
        *   Using BigQuery's time travel feature to query data from before the problematic job run.
        *   If time travel is insufficient, restoring from a BigQuery table snapshot or a previous backup.
        *   Alternatively, if the source data is stable, truncating `sof_schema.ta_bcp_iccid_bq` and re-running the original Oracle job (if still available) to populate the Oracle table, then re-migrating that specific table's data to BigQuery.

4.  **Analysis and Remediation**:
    *   **Investigate Failure**: Analyze the Airflow task logs, Cloud Logging, and BigQuery job history to identify the root cause of the failure or data discrepancy in the migrated job.
    *   **Corrective Actions**: Implement necessary fixes in the Python scripts, BigQuery SQL, or Airflow DAG definition.
    *   **Re-test**: Thoroughly re-test the corrected migrated job in a staging environment before attempting another go-live.