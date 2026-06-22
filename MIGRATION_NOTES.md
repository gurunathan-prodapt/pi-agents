# MIGRATION_NOTES.md: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

## 1. Summary

The `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job, originally orchestrated by UC4 and executing KornShell scripts and Oracle SQL, has been migrated to Google Cloud Platform.

The original job was responsible for updating contract data by truncating the `sof$ta_cntrct_crs2` table and then re-populating it with reconciled and enriched data from `sof$ta_cntrct_crs`. Key logic included filtering out parent contracts (Rahmenverträge) and associating child contracts with their parent contract numbers. It also dynamically retrieved a date parameter (`v_datum`) from `isbert_schema.dwtk_meldungen`.

The migrated solution now leverages:
*   **Google Cloud Composer (Airflow)** for job orchestration, replacing UC4 and KornShell scripts.
*   **Google BigQuery** for data storage and processing, replacing the Oracle database.
*   **Python** for scripting logic, environment setup, and interaction with BigQuery, replacing KornShell.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`d_ausd_v_ta_cntrct_crs2.bqsql`**
    *   **Role**: This file contains the core data transformation logic, translated from Oracle SQL to BigQuery Standard SQL. It performs the `INSERT INTO ... SELECT FROM` operation to populate the `ta_cntrct_crs2` table in BigQuery, including the self-join for parent contract identification and filtering. This script is designed to be executed by an Airflow `BigQueryExecuteQueryOperator`.

*   **`dw_bert_ausd_v_ta_cntrct_crs2_dag.py`**
    *   **Role**: This is the Airflow DAG (Directed Acyclic Graph) definition file. It orchestrates the entire job flow, replacing the UC4 job definition and the KornShell wrapper/core scripts. It defines the sequence of tasks:
        *   `start_job`: A dummy start task.
        *   `truncate_target_table`: A Python task that truncates the BigQuery `ta_cntrct_crs2` table.
        *   `get_v_datum`: A Python task that queries BigQuery to fetch the dynamic `v_datum` parameter and pushes it to Airflow XCom.
        *   `execute_main_sql`: A BigQuery operator task that executes the `d_ausd_v_ta_cntrct_crs2.bqsql` script.
        *   `end_job`: A dummy end task.

## 3. Key Design Decisions

*   **Cloud-Native Orchestration with Airflow**: Google Cloud Composer (Airflow) was chosen to replace UC4 and KornShell for its cloud-native capabilities, Python-based extensibility, robust scheduling, monitoring, and error handling features. This consolidates orchestration into a single, modern framework.
*   **BigQuery for Data Processing**: BigQuery was selected as the target data warehouse due to its scalability, performance, and cost-effectiveness for analytical workloads, replacing the Oracle database.
*   **Python for Scripting Logic**: All KornShell script logic (environment setup, parameter handling, logging, error trapping, and database interaction) has been re-implemented in Python within the Airflow DAG. This eliminates dependencies on legacy shell scripts and integrates seamlessly with Airflow.
*   **Explicit BigQuery DDL for Truncation**: The Oracle procedure `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for truncating tables was replaced by a direct `TRUNCATE TABLE` DDL statement executed via a `BigQueryHook` in a PythonOperator. This leverages BigQuery's native DDL capabilities.
*   **Dynamic `v_datum` via XCom**: The retrieval of the `v_datum` parameter, originally an Oracle SQL query, is now performed by a PythonOperator executing a BigQuery query. The result is pushed to Airflow's XCom, allowing other tasks to access this dynamic value if needed.
*   **ANSI SQL for Joins**: The Oracle-specific `(+)` outer join syntax was explicitly translated to `LEFT OUTER JOIN` in BigQuery Standard SQL. This ensures compatibility and adheres to modern SQL standards. The `WHERE` clause logic for `cr.cntrct_ty` was carefully adjusted to maintain equivalent behavior.
*   **Removal of Oracle Hints**: Oracle `parallel` hints were removed as BigQuery automatically handles parallelism and query optimization efficiently.
*   **Jinja Templating for Configuration**: The BigQuery SQL script utilizes Jinja templating (`{{ params.dwh_project_id }}`) to dynamically inject project and dataset IDs, making the SQL portable and configurable via Airflow DAG parameters.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run successfully in production, the following manual steps and configurations are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the `source_dataset` (e.g., `your-gcp-source-project-id.source_dataset`) exists in BigQuery.
    *   Ensure the `dwh_dataset` (e.g., `your-gcp-dwh-project-id.dwh_dataset`) exists in BigQuery.

2.  **BigQuery Table Creation and Data Ingestion**:
    *   **`ta_cntrct_crs`**: The schema for this source table must be created in `source_dataset`, and it must be populated with data migrated from the Oracle `sof$ta_cntrct_crs` table.
    *   **`dwtk_meldungen`**: The schema for this lookup table must be created in `source_dataset`, and it must be populated with data migrated from the Oracle `isbert_schema.dwtk_meldungen` table.
    *   **`ta_cntrct_crs2`**: The schema for this target table must be created in `dwh_dataset`. The job will truncate and re-populate its data.

3.  **IAM Permissions**:
    *   The Google Cloud service account associated with the Cloud Composer environment (or the specific Airflow connection used) must have the following BigQuery roles:
        *   `BigQuery Data Editor` on `projects/your-gcp-dwh-project-id/datasets/dwh_dataset` (for `TRUNCATE` and `INSERT`).
        *   `BigQuery Data Viewer` on `projects/your-gcp-source-project-id/datasets/source_dataset` (for `SELECT`).
        *   `BigQuery Job User` for running BigQuery queries.

4.  **Airflow Connection Configuration**:
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured in your Cloud Composer environment, pointing to the appropriate GCP project.

5.  **Airflow DAG Parameters Update**:
    *   The placeholder values for `SOURCE_PROJECT_ID`, `SOURCE_DATASET`, `DWH_PROJECT_ID`, and `DWH_DATASET` within the `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` file must be updated to reflect your actual GCP project and dataset IDs. Alternatively, these can be managed via Airflow Variables or other dynamic configuration methods.

6.  **Scheduling Configuration**:
    *   The `schedule_interval` in `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` is currently `None`. It must be updated to the desired cron expression (e.g., `'0 0 * * *'` for daily at midnight UTC) to match the original UC4 schedule.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration design and code generation as requiring further analysis, re-implementation, or follow-up:

*   **UC4 Includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`)**: The exact content and purpose of these UC4 includes remain unanalyzed. It is crucial to determine if they contain critical logic, variables, or environment settings that need to be replicated in Airflow.
*   **KornShell Utility Re-implementation**: While core logic for truncation and `v_datum` retrieval has been migrated, the full functionality of other sourced KornShell utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) needs thorough analysis and robust re-implementation in Python. This includes environment variable management, custom parameter parsing, date calculations, and the `starteSQLSkript` function.
*   **Comprehensive Error Handling and Logging**: The legacy KornShell scripts had extensive error handling (`DWMSG_MeldeFehler`, `trap`). A comprehensive cloud-native error handling and alerting strategy using Airflow's native capabilities (e.g., email alerts, Slack notifications) and Cloud Logging must be designed and implemented.
*   **Oracle-Specific Commands**: Oracle-specific tracing and spooling commands (`START ../trace.sql.cfg`, `SPOOL ./tmp/trace_d_ausd_v_ta_cntrct_crs2`) have been removed. Equivalent BigQuery job monitoring and Cloud Logging will replace this functionality, but their configuration and verification are a follow-up item.
*   **Data Type and Schema Fidelity**: Ongoing verification is required to ensure that the data types and nullability constraints of the migrated BigQuery tables precisely match the Oracle source to prevent data integrity issues.
*   **DB-Link (`@pcrs1`) Investigation**: The design document noted the Oracle DB-Link `@pcrs1` potentially connecting to an external "Carmen DB". While it was assumed `sof$ta_cntrct_crs` consolidates this data, this assumption needs to be validated. If the Carmen DB is a live operational system requiring direct access, a separate BigQuery ingestion pipeline for its data will be necessary.
*   **Performance Optimization**: While BigQuery handles parallelism automatically, the performance of the migrated SQL should be thoroughly tested and optimized for large datasets.
*   **Testing Coverage**: The migration introduces new components (Airflow DAG, Python operators). Robust unit and integration testing for these components is a critical follow-up.

## 6. Validation

To ensure the successful migration and functional equivalence of the `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job, the following validation steps should be performed:

1.  **DAG Integrity Check**:
    *   Upload the `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` to the Cloud Composer environment.
    *   Verify that the DAG parses successfully and appears in the Airflow UI without errors.

2.  **Unit Testing (Python Operators)**:
    *   Implement unit tests for the `truncate_target_table_func` and `get_v_datum_func` Python functions to ensure they perform their intended actions correctly (e.g., mock BigQueryHook calls).

3.  **Integration Testing (Staging Environment)**:
    *   **Prerequisites**: Ensure all manual steps (datasets, tables, IAM, Airflow parameters) are completed in a dedicated staging GCP project.
    *   **Execution**: Trigger the `dw_bert_ausd_v_ta_cntrct_crs2` DAG manually in the Airflow UI.
    *   **Task Success**: Verify that all tasks within the DAG (`start_job`, `truncate_target_table`, `get_v_datum`, `execute_main_sql`, `end_job`) complete successfully without errors.
    *   **Log Review**: Examine Airflow task logs and BigQuery job history for any warnings or errors.
    *   **`v_datum` Verification**: Check the XCom value for `v_datum` pushed by the `get_v_datum` task to ensure it matches the expected value from the `dwtk_meldungen` table.
    *   **Target Table Truncation**: Confirm that the `ta_cntrct_crs2` table was empty before the `execute_main_sql` task started (this can be observed by checking BigQuery table details or running a `SELECT COUNT(*)` query immediately after `truncate_target_table` completes).

4.  **Data Validation (Functional Equivalence)**:
    *   **Baseline**: Run the original `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job in the Oracle environment with a known set of source data. Record the exact state of the `sof$ta_cntrct_crs2` table (e.g., row count, checksums of key columns, sample data).
    *   **Migrated Run**: Ensure the BigQuery source tables (`ta_cntrct_crs`, `dwtk_meldungen`) contain the *exact same data* as their Oracle counterparts used for the baseline. Then, run the migrated `dw_bert_ausd_v_ta_cntrct_crs2` DAG.
    *   **Comparison**:
        *   **Row Count**: Compare the total row count of `project_id.dwh_dataset.ta_cntrct_crs2` in BigQuery with `sof$ta_cntrct_crs2` in Oracle. They must be identical.
        *   **Data Integrity**: Perform checksums or hash comparisons on key columns (e.g., `cntrct_id`, `contract_number`, `rv_num`) to ensure data values are identical.
        *   **Sample Data**: Select random samples of data from both the Oracle and BigQuery target tables and manually compare values, paying close attention to `RV_NUM` and filtered `cntrct_ty` values.
        *   **Edge Cases**: Specifically validate contracts with `cntrct_parent` values that do and do not correspond to `cr.cntrct_ty = 10` (Rahmenverträge) to ensure the `LEFT OUTER JOIN` and `WHERE` clause logic is correct.

**"Passing" Criteria**:
A successful migration is confirmed when:
*   The Airflow DAG completes successfully without any task failures.
*   All Airflow logs and BigQuery job logs indicate successful operations.
*   The `ta_cntrct_crs2` table in BigQuery is populated.
*   All data validation checks (row counts, checksums, and functional comparisons) confirm that the data in `project_id.dwh_dataset.ta_cntrct_crs2` is an exact, byte-for-byte match to the data produced by the original Oracle job under identical source conditions.

## 7. Rollback Procedure

In the event of critical failure, data corruption, or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Disable Airflow DAG**: Immediately pause or disable the `dw_bert_ausd_v_ta_cntrct_crs2` DAG in the Airflow UI to prevent further execution.
    *   **Re-enable Original Job**: Re-enable the original UC4 job (`DW.BERT_AUSD_V_TA_CNTRCT_CRS2`) to ensure business continuity and data updates continue via the proven legacy system.

2.  **Data Recovery (if necessary)**:
    *   **BigQuery Target Table**: If the `project_id.dwh_dataset.ta_cntrct_crs2` table was corrupted or incorrectly populated by the migrated job, utilize BigQuery's time travel capabilities to restore the table to a state before the problematic run. For example:
        ```sql
        CREATE OR REPLACE TABLE `project_id.dwh_dataset.ta_cntrct_crs2` AS
        SELECT * FROM `project_id.dwh_dataset.ta_cntrct_crs2` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        ```
        (Adjust the `INTERVAL` as needed to a known good state).
    *   Since this job truncates and re-inserts, the impact is generally confined to the target table.

3.  **Investigation**:
    *   Analyze Airflow task logs, BigQuery job history, and Cloud Logging for the `dw_bert_ausd_v_ta_cntrct_crs2` DAG to identify the root cause of the failure.
    *   Compare the BigQuery data with the Oracle source data (and potentially the original Oracle target data) to pinpoint discrepancies and understand the nature of the issue.

4.  **Resolution**:
    *   Address the identified issue in the Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs2_dag.py`) or the BigQuery SQL (`d_ausd_v_ta_cntrct_crs2.bqsql`).
    *   Thoroughly retest the corrected DAG in a staging environment, following the validation steps outlined above.

5.  **Re-deployment**:
    *   Once the fix is validated and confidence is restored, deploy the updated Airflow DAG to production.
    *   Monitor the first few runs closely.
    *   Once confirmed stable, disable the original UC4 job permanently.