# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `DW.BERT_AUSD_V_TA_C_BFC` job. This ETL process, originally orchestrated by UC4 and implemented using KornShell scripts and Oracle SQL, was responsible for updating contract extension period caching in the `sof$ta_c_bfc` table.

The job has been migrated to Google Cloud Platform (GCP), leveraging:
*   **Google BigQuery** as the target data warehouse for all source and target tables.
*   **Apache Airflow (or Cloud Composer)** for job orchestration.
*   **BigQuery Standard SQL** for data transformation logic.

## 2. Generated artifacts

The migration process generated the following files:

*   **`ddl/sof_ta_cntrct_crs.ddl`**: BigQuery DDL script to create the `sof$ta_cntrct_crs` table. This table serves as a source for contract-related data.
*   **`ddl/sof_ta_barrier.ddl`**: BigQuery DDL script to create the `sof$ta_barrier` table, another source for contract data.
*   **`ddl/sof_ta_cntrct_valid.ddl`**: BigQuery DDL script to create the `sof$ta_cntrct_valid` table, providing contract validity information.
*   **`ddl/sof_ta_period.ddl`**: BigQuery DDL script to create the `sof$ta_period` table, used for period-related data.
*   **`ddl/dwtk_meldungen.ddl`**: BigQuery DDL script to create the `dwtk_meldungen` table, used to determine the `Stichtag` (reference date).
*   **`ddl/all_objects.ddl`**: BigQuery DDL script to create a metadata table `all_objects`, used to track the procedure creation date.
*   **`ddl/sof_ta_c_bfc_akt.ddl`**: BigQuery DDL script to create the `sof$ta_c_bfc_akt` table, which acts as a temporary staging table for the caching process.
*   **`ddl/sof_ta_c_bfc.ddl`**: BigQuery DDL script to create the `sof$ta_c_bfc` table, the final target table for contract extension period caching.
*   **`udf/bfc_get_bindefrist.sql`**: BigQuery DDL script to create the `bfc_get_bindefrist` User-Defined Function (UDF). This UDF is a placeholder for the re-implemented Oracle PL/SQL `Cds$vr_Bindefrist.GetBindeFrist` logic.
*   **`d_ausd_v_ta_c_bfc.bqsql`**: The core BigQuery Standard SQL script. This file contains the translated transformation logic from the original `d_ausd_v_ta_c_bfc.sql` Oracle script, including variable declarations, data aggregation, conditional initial load, incremental merge, and batch updates.
*   **`dags/dw_bert_ausd_v_ta_c_bfc.py`**: The Airflow DAG definition. This Python script orchestrates the execution of the `d_ausd_v_ta_c_bfc.bqsql` script using a `BigQueryInsertJobOperator`, replacing the UC4 job and KornShell wrapper scripts.

## 3. Key design decisions

*   **Target Platform Selection**: Google Cloud Platform (GCP) was chosen for its managed services, scalability, and cost-effectiveness. BigQuery provides a fully managed, serverless data warehouse, and Airflow (via Cloud Composer) offers robust workflow orchestration.
*   **Direct SQL Translation**: The complex Oracle SQL logic from `d_ausd_v_ta_c_bfc.sql` was directly translated into BigQuery Standard SQL. This minimizes changes to the core business logic, reducing the risk of introducing new bugs and simplifying validation.
*   **UDF for PL/SQL Function**: The Oracle PL/SQL function `Cds$vr_Bindefrist.GetBindeFrist` (called via `bfc_get_bindefrist`) was identified as a critical piece of logic requiring re-implementation as a BigQuery UDF. This approach keeps the function logic within the BigQuery environment, allowing it to be called directly from the main SQL script.
*   **Airflow for Orchestration**: Apache Airflow was selected to replace UC4 and the KornShell wrapper scripts. A single Airflow DAG encapsulates the entire job, providing clear visibility, scheduling capabilities, and robust error handling mechanisms inherent to Airflow.
*   **Jinja Templating for Environment Variables**: Airflow's `BigQueryInsertJobOperator` uses Jinja templating for the SQL script. This allows for dynamic injection of `project` and `dataset` IDs, making the SQL script portable across different GCP environments (e.g., dev, staging, prod) without modification.
*   **Replacement of Oracle-Specific Features**:
    *   Oracle `(+)` outer join syntax was converted to explicit `LEFT JOIN`.
    *   `NVL` was replaced with `COALESCE`.
    *   `TO_DATE`, `TO_CHAR`, `TRUNC` date functions were converted to BigQuery's `PARSE_DATE`, `FORMAT_DATE`, and `DATE()` functions.
    *   The `ROWNUM <= &v_max_update` for batch processing was replaced with `QUALIFY ROW_NUMBER() OVER (ORDER BY cntrct_id) <= v_max_update`.
    *   The `TRUNCATE TABLE ... REUSE STORAGE` was simplified to `TRUNCATE TABLE`.
    *   References to `all_objects` and `dwtk_meldungen` were mapped to corresponding BigQuery tables, assuming their data is migrated.
    *   The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` calls were replaced by direct BigQuery DDL/DML statements where applicable (e.g., `TRUNCATE TABLE`).
*   **Trade-offs**:
    *   **UDF Complexity**: The `bfc_get_bindefrist` UDF is currently a placeholder. Its full re-implementation requires detailed analysis of the original Oracle PL/SQL, which might be complex and could potentially require a JavaScript UDF or an external function if the logic is too intricate for a pure SQL UDF. This is a significant B4 item.
    *   **Initial Unscheduled DAG**: The DAG is initially unscheduled (`schedule_interval=None`). This requires a manual review of the legacy UC4 schedule and dependencies to determine the appropriate production schedule, which is a manual step.
    *   **Error Handling/Retries**: The `retries=0` in the Airflow DAG is a conservative default. A more robust retry strategy and error handling callbacks need to be defined based on the job's criticality and the nature of potential failures, requiring further analysis of the legacy job's behavior.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`your_bigquery_dataset` in the generated code) exists in your GCP project (`your-gcp-project-id`). If not, create it.
    *   `bq mk --dataset your-gcp-project-id:your_bigquery_dataset`

2.  **BigQuery Table Creation (DDL Execution)**:
    *   Execute all DDL scripts (`ddl/*.ddl`) in the target BigQuery dataset to create the necessary source, staging, and target tables.
    *   `bq query --use_legacy_sql=false --project_id=your-gcp-project-id --dataset_id=your_bigquery_dataset < ddl/sof_ta_cntrct_crs.ddl`
    *   Repeat for `sof_ta_barrier.ddl`, `sof_ta_cntrct_valid.ddl`, `sof_ta_period.ddl`, `dwtk_meldungen.ddl`, `all_objects.ddl`, `sof_ta_c_bfc_akt.ddl`, `sof_ta_c_bfc.ddl`.

3.  **BigQuery UDF Implementation and Deployment**:
    *   **Crucial Step**: The `udf/bfc_get_bindefrist.sql` file currently contains a placeholder (`NULL`). The actual logic from the Oracle `Cds$vr_Bindefrist.GetBindeFrist` PL/SQL function **must be fully re-implemented and deployed** as a BigQuery UDF. This may involve a SQL UDF, a JavaScript UDF, or an external function.
    *   Execute the completed UDF DDL:
        `bq query --use_legacy_sql=false --project_id=your-gcp-project-id --dataset_id=your_bigquery_dataset < udf/bfc_get_bindefrist.sql`

4.  **Initial Data Ingestion**:
    *   Populate the source tables (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `dwtk_meldungen`, `all_objects`) in BigQuery with data from their respective Oracle sources. This can be done via BigQuery Data Transfer Service, `bq load` commands, or other ETL processes.
    *   Ensure `dwtk_meldungen` contains relevant entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'` to correctly determine `v_datum`.
    *   Ensure `all_objects` contains an entry for `object_name = 'CDS$VR_BINDEFRIST'` and `object_type = 'PACKAGE'` with a valid `created` timestamp to correctly determine `v_bfc_procedure`.

5.  **IAM/Permissions**:
    *   Ensure the Airflow service account (or Cloud Composer service account) has the necessary IAM roles for BigQuery:
        *   `BigQuery Data Editor` (for writing to `sof$ta_c_bfc`, `sof$ta_c_bfc_akt`)
        *   `BigQuery Data Viewer` (for reading from all source tables)
        *   `BigQuery Job User` (for running BigQuery jobs)
    *   Verify the `google_cloud_default` Airflow connection is correctly configured and points to the appropriate GCP project.

6.  **Airflow Configuration**:
    *   Update the `params` in the `dags/dw_bert_ausd_v_ta_c_bfc.py` DAG with the correct GCP Project ID and BigQuery Dataset ID:
        ```python
        params={
            'project': 'your-gcp-project-id',
            'dataset': 'your_bigquery_dataset'
        },
        ```
    *   Deploy the `dags/dw_bert_ausd_v_ta_c_bfc.py` DAG to your Airflow environment (e.g., Cloud Composer DAGs folder).
    *   Ensure the `d_ausd_v_ta_c_bfc.bqsql` file is placed in the `template_searchpath` directory (e.g., `/opt/airflow/dags/sql` if using the default setup).

7.  **Scheduling**:
    *   The DAG is currently unscheduled (`schedule_interval=None`). Review the legacy UC4 job's schedule and dependencies to determine the appropriate `schedule_interval` (e.g., `timedelta(days=1)` for daily, or a cron expression) and update the DAG accordingly.

## 5. Known gaps & unresolved references

The following items were flagged during migration and require further attention or are known limitations:

*   **B4 Item: `bfc_get_bindefrist` UDF Logic**: The most significant gap is the placeholder implementation of the `bfc_get_bindefrist` UDF (`udf/bfc_get_bindefrist.sql`). The exact logic of the original Oracle `Cds$vr_Bindefrist.GetBindeFrist` PL/SQL function is not available in the provided source. This UDF *must* be fully re-implemented and validated against the original Oracle logic before go-live. This may require detailed analysis of the Oracle source code or functional specifications.
*   **Scheduler Detail**: The precise scheduling and inter-job dependencies of the legacy UC4 job are unknown. The Airflow DAG is currently unscheduled and requires manual definition of its `schedule_interval` and potential upstream/downstream dependencies.
*   **Oracle `all_objects` Table Equivalent**: The `all_objects` table in BigQuery is a simplified representation. If the original Oracle `all_objects` contained more complex metadata or versioning information that `v_bfc_procedure` implicitly relied upon beyond just the creation date, this might be a subtle gap. For now, it assumes `created` is sufficient.
*   **Error Handling and Retries**: The Airflow DAG is configured with `retries=0`. This is a conservative approach. A more robust retry strategy (e.g., exponential backoff) and specific error handling callbacks (e.g., Slack notifications, PagerDuty alerts) should be implemented based on the job's operational requirements and the nature of expected failures.
*   **Utility Script Functionality**: The KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) were assumed to be either generic or their functionality absorbed by Airflow/BigQuery. If any specific, non-generic functionality was present in these scripts that is critical to the job's operation (e.g., custom logging formats, specific parameter validations), it needs to be re-evaluated and potentially re-implemented in Python within the Airflow DAG.
*   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`**: The original Oracle script used `DWPA_UTIL_SKRIPT.runstatement` for executing DDL/DML. This has been replaced by direct BigQuery DDL/DML. If `runstatement` had any complex side effects or logging mechanisms beyond simple execution, those have not been replicated.
*   **`v_carmen` (DB-Link)**: The Oracle DB-link `&v_carmen` was removed as BigQuery tables are directly referenced. This assumes no hidden logic or specific connection properties were tied to this DB-link that are not covered by direct table access in BigQuery.

## 6. Validation

To validate the migrated `DW.BERT_AUSD_V_TA_C_BFC` job, follow these steps:

1.  **Prerequisites**:
    *   All manual steps before go-live (Section 4) must be completed, especially the full implementation and deployment of the `bfc_get_bindefrist` UDF and initial data ingestion into source tables.
    *   The Airflow DAG `dw_bert_ausd_v_ta_c_bfc.py` must be deployed and unpaused in the Airflow UI.

2.  **Run the Job**:
    *   From the Airflow UI, manually trigger the `dw_bert_ausd_v_ta_c_bfc` DAG.
    *   Monitor the DAG run in the Airflow UI to ensure all tasks complete successfully without errors.

3.  **Check BigQuery Job History**:
    *   In the GCP Console, navigate to BigQuery and check the "Job history" for the project. Verify that the BigQuery job initiated by Airflow completed successfully.

4.  **Data Validation (What "passing" means)**:
    *   **DAG Success**: The Airflow DAG run completes with a "success" status.
    *   **Target Table Update**: Query the `sof$ta_c_bfc` table in BigQuery.
        *   Verify that the `bindefrist`, `bfc_age`, `bfc_count`, `bfc_procedure`, `commitment_reference_date`, and `cntrct_validity_id` columns are populated as expected.
        *   Check the `bfc_procedure` column to ensure it reflects the `v_bfc_procedure` derived from `all_objects`.
    *   **Functional Equivalence**:
        *   **Record Count**: Compare the number of rows in the BigQuery `sof$ta_c_bfc` table with the corresponding Oracle `sof$ta_c_bfc` table after a successful run of both jobs (legacy and migrated) with identical source data.
        *   **Data Comparison (Sample)**: Select a representative sample of `cntrct_id`s. For these IDs, compare the `bindefrist`, `bfc_age`, `bfc_count`, and other relevant columns in the BigQuery `sof$ta_c_bfc` table against the Oracle `sof$ta_c_bfc` table. This is crucial for validating the `bfc_get_bindefrist` UDF.
        *   **Edge Cases**: Test with data that covers known edge cases or specific scenarios that the `Cds$vr_Bindefrist.GetBindeFrist` function handles (e.g., different contract types, validity periods).
    *   **Performance**: Monitor the execution time of the BigQuery job. While not a "passing" criterion, significant performance degradation should be investigated.

A "passing" validation means the Airflow DAG completes successfully, and the data in the BigQuery `sof$ta_c_bfc` table is functionally equivalent to the data produced by the legacy Oracle job, both in terms of content and structure, for a given set of input data.

## 7. Rollback procedure

In case of issues or critical failures after go-live, the following rollback procedure can be executed:

1.  **Stop New Runs**:
    *   In the Airflow UI, unpause the `dw_bert_ausd_v_ta_c_bfc` DAG.
    *   Set `schedule_interval=None` in the DAG definition and redeploy to prevent any further scheduled runs.

2.  **Revert BigQuery Target Table**:
    *   **Option A (Snapshot/Backup)**: If BigQuery table snapshots or backups were taken before the migration or before the problematic run, restore the `sof$ta_c_bfc` table from the last known good state.
    *   **Option B (Time Travel)**: Utilize BigQuery's time travel feature to revert the `sof$ta_c_bfc` table to a state before the problematic run.
        ```sql
        CREATE OR REPLACE TABLE `your-gcp-project-id.your_bigquery_dataset.sof$ta_c_bfc` AS
        SELECT * FROM `your-gcp-project-id.your_bigquery_dataset.sof$ta_c_bfc` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR); -- Adjust interval as needed
        ```
    *   **Option C (Re-ingest from Source)**: If the `sof$ta_c_bfc` table can be fully rebuilt from its source tables, trigger a full reload process (if one exists) or re-ingest data from the Oracle source.

3.  **Re-enable Legacy Job**:
    *   Re-enable the original `DW.BERT_AUSD_V_TA_C_BFC` job in the UC4 environment.
    *   Verify that the legacy job runs successfully and continues to update the Oracle `sof$ta_c_bfc` table as expected.

4.  **Investigate and Remediate**:
    *   Analyze the logs from Airflow and BigQuery to identify the root cause of the failure.
    *   Address the identified issues in the BigQuery SQL, UDF, or Airflow DAG.
    *   Once the issues are resolved and thoroughly tested in a non-production environment, the migration process can be re-attempted.