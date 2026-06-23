```markdown
# MIGRATION_NOTES: BERT_V_TA_DISC_ZUSGF

## 1. Summary

The `BERT_V_TA_DISC_ZUSGF` job, originally an Oracle PL/SQL process orchestrated by UC4 and KornShell scripts, has been migrated to the Google Cloud Platform. This job is responsible for processing and concatenating discount descriptions related to contracts.

The migration involved:
*   **Source Platform**: Oracle Database (PL/SQL), UC4/Automic, KornShell scripts.
*   **Target Platform**: Google Cloud Platform, utilizing BigQuery for data storage and transformation, and Cloud Composer (Apache Airflow) for workflow orchestration.

The core functionality of aggregating and concatenating discount information into the `sof$ta_disc_zusgf` table has been re-implemented using BigQuery SQL and orchestrated by an Airflow DAG.

## 2. Generated Artifacts

The migration produced the following files:

*   **`sql/d_ausd_v_ta_disc_zusgf.sql`**
    *   **Role**: This BigQuery SQL script contains the core data transformation logic. It replaces the original Oracle PL/SQL script (`d_ausd_v_ta_disc_zusgf.sql`). Its primary function is to read discount data from the `sof$ta_discount` table, concatenate discount descriptions using `STRING_AGG`, and populate the target `sof$ta_disc_zusgf` table.
*   **`dags/dw_bert_ausd_v_ta_disc_zusgf.py`**
    *   **Role**: This Python file defines an Apache Airflow DAG. It orchestrates the entire workflow, replacing the UC4 job definition (`DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`) and the KornShell wrapper/control scripts (`r_ausd_v_ta_disc_zusgf.ksh`, `k_ausd_v_ta_disc_zusgf.ksh`). The DAG includes tasks for determining the processing date and executing the BigQuery transformation.

## 3. Key Design Decisions

*   **BigQuery for Data Transformation**:
    *   **Why**: BigQuery was chosen to leverage its serverless architecture, scalability, high performance for analytical queries, and cost-effectiveness as the primary data warehouse and transformation engine. It replaces the Oracle PL/SQL environment.
    *   **Trade-offs**: Required a complete re-implementation of Oracle-specific features such as custom object types, pipelined table functions, `(+)` outer join syntax, `NVL`, and `TO_CHAR` functions into standard BigQuery SQL (`STRING_AGG`, `LEFT JOIN`, `COALESCE`, `FORMAT_TIMESTAMP`). This conversion demands careful validation to ensure functional parity, especially for complex aggregation logic.
*   **Cloud Composer (Apache Airflow) for Orchestration**:
    *   **Why**: Airflow provides a managed, robust, and scalable platform for defining, scheduling, and monitoring complex data pipelines on GCP. It replaces the legacy UC4 scheduler and KornShell scripts, offering Python-based DAGs for flexible workflow definition, dependency management, and improved observability.
    *   **Trade-offs**: The environment setup, parameter parsing, and error handling logic previously embedded in KornShell scripts needed to be re-engineered and implemented using Python Operators or Airflow's native features within the DAG.
*   **`PythonOperator` for `determine_processing_date`**:
    *   **Why**: The original Oracle job derived a processing date from the `dwtk_meldungen` table. This dynamic date derivation is best handled by a dedicated `PythonOperator` task in Airflow. This task queries BigQuery, extracts the relevant date, and makes it available to subsequent tasks via XComs, ensuring flexibility and clear separation of concerns.
    *   **Trade-offs**: Introduces an additional task in the DAG, but enhances modularity and maintainability.
*   **`BigQueryExecuteQueryOperator` for SQL Execution**:
    *   **Why**: This operator provides a direct and efficient way to execute BigQuery SQL scripts from within an Airflow DAG, leveraging Airflow's native integration with BigQuery.
    *   **Trade-offs**: The SQL script must be self-contained or designed to accept parameters, which can be passed via Jinja templating or XComs from preceding tasks.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps and configurations are required:

1.  **BigQuery Dataset and Table Availability**:
    *   **Source Tables**: Ensure that the `isbert_schema.dwtk_meldungen` and `sof$ta_discount` tables are successfully migrated to BigQuery and populated with up-to-date data. These are critical prerequisites for the job's execution.
    *   **Target Table**: Verify that the target `sof$ta_disc_zusgf` table exists in BigQuery within the appropriate dataset, or that the service account has permissions to `CREATE OR REPLACE TABLE` in the target dataset.
2.  **IAM Permissions**:
    *   The Google Cloud service account associated with the Cloud Composer environment must have the necessary BigQuery roles:
        *   `BigQuery Data Editor` for the dataset containing `sof$ta_disc_zusgf` (to write/overwrite data).
        *   `BigQuery Data Viewer` for the datasets containing `isbert_schema.dwtk_meldungen` and `sof$ta_discount` (to read source data).
        *   `Storage Object Viewer` for the Cloud Composer bucket where the DAG and SQL files are stored.
3.  **Airflow Connection Configuration**:
    *   Ensure the `google_cloud_default` connection in the Airflow environment is correctly configured to point to the GCP project where BigQuery resources reside.
4.  **Scheduling**:
    *   Confirm that the `@daily` schedule interval defined in the `dw_bert_ausd_v_ta_disc_zusgf.py` DAG matches the desired production schedule of the original UC4 job.
    *   Deploy the DAG to the designated Cloud Composer environment.
5.  **Secrets Management**:
    *   Review the original KornShell scripts for any hardcoded credentials or sensitive parameters. If found, these must be securely managed in GCP Secret Manager and integrated into the Airflow DAG using appropriate Airflow hooks or operators. (No explicit secrets were identified in the provided design, but this is a general best practice).

## 5. Known Gaps & Unresolved References

*   **`dwtk_meldungen` and `sof$ta_discount` Migration**: As highlighted in the design document, the successful operation of this job is contingent on the prior migration and availability of these source tables in BigQuery. The mechanism and schedule for populating these BigQuery tables need to be confirmed and established.
*   **Oracle PL/SQL Pipelined Function Parity**: The original Oracle script used a complex pipelined table function. While `STRING_AGG` in BigQuery SQL provides a functional equivalent for concatenation, thorough testing is required to ensure complete functional parity, especially concerning edge cases, `NULL` handling, and the `ORDER BY` clause within `STRING_AGG`.
*   **Dynamic `v_datum` Usage**: The `determine_processing_date` Airflow task correctly derives the `s_datum` from `dwtk_meldungen` and pushes it to XCom. However, the generated `sql/d_ausd_v_ta_disc_zusgf.sql` does not currently incorporate or use this `s_datum` value for filtering or other logic. If the original Oracle job used this date for data selection (e.g., filtering `sof$ta_discount` by date), the BigQuery SQL needs to be updated to consume this parameter, potentially via Airflow's Jinja templating or by passing it as a query parameter.
*   **KornShell Logic Absorption**: While the core orchestration is handled by Airflow, any remaining complex parameter parsing, environment setup, or advanced error handling logic from the original KornShell scripts (`r_ausd_v_ta_disc_zusgf.ksh`, `k_ausd_v_ta_disc_zusgf.ksh`) that was not explicitly re-implemented in the DAG should be reviewed.
*   **`rabatt_alle` Length Constraint**: The original Oracle PL/SQL might have implicitly or explicitly handled a maximum length for the concatenated `rabatt_alle` field. The generated BigQuery SQL does not include any explicit length constraints (e.g., `SUBSTRING`). If a maximum length is critical for downstream systems, this needs to be added to the BigQuery SQL.
*   **Performance Tuning**: The original Oracle script might have used performance hints (e.g., `PARALLEL`). While BigQuery automatically manages parallelism, the BigQuery SQL should be reviewed and potentially optimized (e.g., using partitioning, clustering, or specific join strategies) to ensure comparable or improved performance.

## 6. Validation

Validation should cover unit testing of individual components and end-to-end testing of the entire pipeline.

### How to Run Tests:

1.  **BigQuery SQL Unit Test**:
    *   Execute the `sql/d_ausd_v_ta_disc_zusgf.sql` script directly in the BigQuery console.
    *   Use a representative sample of data in the `sof$ta_discount` table that mimics production scenarios, including edge cases for discount descriptions.
    *   Manually inspect the output in the `sof$ta_disc_zusgf` table.
2.  **Airflow DAG Unit Test**:
    *   Deploy the `dags/dw_bert_ausd_v_ta_disc_zusgf.py` DAG to a development Cloud Composer environment.
    *   Manually trigger the DAG from the Airflow UI.
    *   Monitor the Airflow logs for each task:
        *   Verify that the `determine_processing_date` task successfully retrieves and pushes the `s_datum` to XCom.
        *   Verify that the `execute_bq_transformation` task starts and completes the BigQuery job without errors.
3.  **End-to-End Integration Test**:
    *   Ensure that the BigQuery source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_discount`) are populated with production-like data.
    *   Run the `dw_bert_ausd_v_ta_disc_zusgf` DAG in a staging environment.
    *   Compare the data in the target `sof$ta_disc_zusgf` table in BigQuery with the corresponding output from the legacy Oracle job for the same processing period.

### What "Passing" Means:

*   **DAG Execution**: All tasks within the `dw_bert_ausd_v_ta_disc_zusgf` Airflow DAG complete successfully without any errors or retries.
*   **Data Population**: The `sof$ta_disc_zusgf` table in BigQuery is populated with data after the DAG run.
*   **Data Accuracy**:
    *   **Row Count**: The number of rows in the BigQuery `sof$ta_disc_zusgf` table matches the row count in the Oracle `sof$ta_disc_zusgf` table for the same processing period.
    *   **Content Verification**: A statistically significant sample of records (e.g., 1-5% or specific high-impact records) from the BigQuery `sof$ta_disc_zusgf` table is compared against the Oracle `sof$ta_disc_zusgf` table. Key fields, especially the concatenated `rabatt_alle`, `cntrct_id`, `cntrct_obj_version`, and `disc_vector_ty`, must match exactly.
*   **Performance**: The end-to-end execution time of the Airflow DAG should be comparable to or ideally faster than the legacy Oracle job.
*   **Logging**: All relevant logs are captured in Cloud Logging, providing sufficient detail for debugging and monitoring.

## 7. Rollback Procedure

In case of critical failure or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Rollback (Orchestration)**:
    *   **Disable Airflow DAG**: Immediately pause or disable the `dw_bert_ausd_v_ta_disc_zusgf` DAG in the Cloud Composer Airflow UI to prevent further execution.
    *   **Re-enable Legacy Job**: Re-enable the original UC4 job definition (`DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`) to resume production processing using the legacy system.
2.  **Data Rollback (if necessary)**:
    *   If the `sof$ta_disc_zusgf` table in BigQuery was overwritten by the failed migration run, restore it to its state prior to the migration attempt. This can be done using BigQuery's time travel feature (if within the time window) or by restoring from a previously taken table snapshot/backup.
    *   Alternatively, if the legacy job is designed to fully truncate and reload the target table, allowing the re-enabled legacy job to run might naturally correct the data in the target table.
3.  **Investigation**:
    *   Analyze the logs in Cloud Logging and Airflow UI to identify the root cause of the failure.
    *   Address the identified issues in the BigQuery SQL, Airflow DAG, or underlying data dependencies.
    *   Plan for re-migration once the issues are resolved and thoroughly tested in a staging environment.

```