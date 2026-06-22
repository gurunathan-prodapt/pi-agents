# MIGRATION_NOTES.md: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

## 1. Summary

This migration involved the ETL workflow `DW.BERT_AUSD_V_TA_CNTRCT_CRS2`, which is responsible for updating contract data, specifically reconciling the `ta_cntrct_crs2` table. The original job, orchestrated by UC4 and executing KornShell scripts that in turn ran an Oracle SQL script, has been migrated to Google Cloud Platform.

The target platform utilizes:
*   **Google Cloud Composer (Airflow)** for job orchestration.
*   **Google BigQuery** for data storage and transformation.
*   **Google Cloud Storage** for storing job artifacts.
*   **Google Cloud Dataproc** for executing the PySpark wrapper script.

The core logic, which populates `sof_ta_cntrct_crs2` with enriched contract information while excluding frame contract parent entries, has been translated from Oracle SQL to BigQuery SQL.

## 2. Generated Artifacts

The migration produced the following files:

*   **`d_ausd_v_ta_cntrct_crs2_bq.sql`**
    *   **Role**: This file contains the core data transformation logic, translated from the original Oracle SQL script (`d_ausd_v_ta_cntrct_crs2.sql`) to BigQuery Standard SQL. It includes a `DECLARE` statement for date derivation, a `TRUNCATE TABLE` statement for the target table, and an `INSERT INTO` statement to populate `sof_ta_cntrct_crs2` from `sof_ta_cntrct_crs` with the specified filtering and joining logic. This script is executed by the PySpark wrapper.

*   **`r_ausd_v_ta_cntrct_crs2.py`**
    *   **Role**: This PySpark script acts as a wrapper, replacing the functionality of the original KornShell scripts (`r_ausd_v_ta_cntrct_crs2.ksh` and `k_ausd_v_ta_cntrct_crs2.ksh`). Its primary responsibility is to execute the `d_ausd_v_ta_cntrct_crs2_bq.sql` script against Google BigQuery. It handles environment setup (e.g., `GCP_PROJECT_ID`), logging, and error handling, similar to the original ksh scripts. This script is submitted to a Dataproc cluster by the Airflow DAG.

*   **`dw_bert_ausd_v_ta_cntrct_crs2_dag.py`**
    *   **Role**: This is the Airflow DAG definition file, replacing the UC4 job (`DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml`). It orchestrates the execution of the data transformation. It defines a `DataprocSubmitJobOperator` task that submits the `r_ausd_v_ta_cntrct_crs2.py` PySpark script to a Dataproc cluster, passing necessary parameters like the BigQuery SQL script path.

## 3. Key Design Decisions

*   **Orchestration Layer Migration**: The UC4 job was migrated to **Google Cloud Composer (Airflow)**. Airflow provides robust scheduling, monitoring, and dependency management capabilities native to GCP, replacing the external UC4 system.
*   **Core Logic Translation**: The Oracle SQL script (`d_ausd_v_ta_cntrct_crs2.sql`) was directly translated to **BigQuery Standard SQL**. This approach leverages BigQuery's powerful analytical capabilities and scalability for data transformation, eliminating the need for an Oracle database.
    *   **Trade-off**: Direct SQL translation is efficient but requires careful handling of Oracle-specific syntax (e.g., `(+)` for outer joins, `TO_CHAR` date formatting, `NVL` function, and procedural calls like `DWPA_UTIL_SKRIPT.runstatement`). These were converted to their BigQuery equivalents (`LEFT JOIN`, `FORMAT_DATE`, `COALESCE`, `TRUNCATE TABLE`).
*   **KornShell Script Replacement**: The multiple KornShell scripts were consolidated and replaced by a single **PySpark wrapper script** (`r_ausd_v_ta_cntrct_crs2.py`). This script is designed to execute the BigQuery SQL.
    *   **Trade-off**: While the core logic is in BigQuery SQL, a PySpark wrapper was chosen to maintain a similar execution pattern to the original ksh scripts (i.e., a script invoking the SQL logic) and to provide a flexible environment for future enhancements or integration with other Spark-based processing if needed. It also allows for centralized logging and parameter passing via Dataproc.
*   **Data Storage Migration**: All source and target Oracle tables (`sof$ta_cntrct_crs`, `sof$ta_cntrct_crs2`, `isbert_schema.dwtk_meldungen`) are migrated to **Google BigQuery**. This centralizes data storage within the GCP ecosystem and allows for direct BigQuery SQL operations.
*   **Performance Hint Removal**: The Oracle `/*+ parallel(c,4) parallel (cr, 4) */` hint was removed. BigQuery automatically handles parallelism and query optimization, making such hints unnecessary and potentially counterproductive. Performance will be validated post-migration.
*   **Utility Script Abstraction**: The numerous ksh utility scripts (e.g., for logging, parameter handling) were replaced by standard Python logging within the PySpark script and BigQuery's native capabilities for data operations.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **BigQuery Dataset and Table Creation**:
    *   Ensure the BigQuery dataset `isbert_schema` exists.
    *   Create the source tables `sof_ta_cntrct_crs` and `isbert_schema.dwtk_meldungen` in BigQuery, ensuring their schemas match the Oracle originals and data is loaded.
    *   Create the target table `sof_ta_cntrct_crs2` in BigQuery with the correct schema, matching the output of the `INSERT` statement.
2.  **IAM Permissions**:
    *   Grant the Cloud Composer service account (or the service account used by the Airflow DAG) the necessary permissions to submit Dataproc jobs and interact with Google Cloud Storage.
    *   Grant the Dataproc cluster's service account (or the service account used by the PySpark job) the necessary permissions to read from and write to BigQuery tables (`sof_ta_cntrct_crs`, `isbert_schema.dwtk_meldungen`, `sof_ta_cntrct_crs2`) and to read files from the specified GCS bucket.
3.  **GCP Configuration Placeholders**:
    *   Update the `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` file with actual values for:
        *   `GCP_PROJECT_ID`
        *   `GCP_DATAPROC_REGION`
        *   `GCP_DATAPROC_CLUSTER_NAME` (ensure the Dataproc cluster exists and is running or configured for auto-scaling)
        *   `GCS_BUCKET_NAME`
4.  **GCS Deployment**:
    *   Upload the `r_ausd_v_ta_cntrct_crs2.py` PySpark script to the specified GCS bucket path (e.g., `gs://YOUR_BUCKET_NAME/dataproc_jobs/r_ausd_v_ta_cntrct_crs2.py`).
    *   Upload the `d_ausd_v_ta_cntrct_crs2_bq.sql` BigQuery SQL script to the specified GCS bucket path (e.g., `gs://YOUR_BUCKET_NAME/dataproc_jobs/d_ausd_v_ta_cntrct_crs2_bq.sql`).
5.  **Airflow DAG Deployment**:
    *   Deploy the `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` file to your Cloud Composer environment's DAGs folder.
6.  **Scheduling**:
    *   The Airflow DAG is currently set with `schedule=None`. Based on business requirements, define and apply the appropriate schedule (e.g., `@daily`, `0 5 * * *`) within the DAG definition.

## 5. Known Gaps & Unresolved References

*   **GCP Placeholders**: As noted in section 4, `GCP_PROJECT_ID`, `GCP_DATAPROC_REGION`, `GCP_DATAPROC_CLUSTER_NAME`, and `GCS_BUCKET_NAME` must be configured manually.
*   **No Explicit Schedule**: The original UC4 job XML did not contain explicit scheduling information. The Airflow DAG is generated without a schedule (`schedule=None`). A schedule must be defined based on business requirements.
*   **UC4 Login/Host Mapping**: The original UC4 job used `DW.UNIX.ISBERT` login and `DWHDWH1P` host. These security and execution context details need to be mapped to appropriate GCP IAM roles and Dataproc cluster configurations to ensure secure and authorized execution.
*   **Utility Scripts Review**: The numerous ksh utility scripts sourced by the original ksh scripts (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) were assumed to be primarily for environment setup and logging. Their functionalities have been replaced by Python logging and BigQuery native features. A thorough review of these original utility scripts is recommended to ensure no critical business logic or environment-specific configurations were missed.
*   **Table Schema Accuracy**: The SQL conversion assumes that the column names and data types inferred from the Oracle SQL script are accurate. A full schema migration and validation process is required to confirm the exact BigQuery schemas for `sof_ta_cntrct_crs`, `isbert_schema.dwtk_meldungen`, and `sof_ta_cntrct_crs2`.
*   **Dataproc Cluster Management**: The current design assumes a pre-existing Dataproc cluster. For production, consider using Dataproc Serverless or ephemeral clusters managed by Airflow for cost optimization and simplified management.

## 6. Validation

To validate the successful migration and execution of the `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job:

1.  **Trigger the Airflow DAG**:
    *   Access the Cloud Composer UI.
    *   Find the `dw_bert_ausd_v_ta_cntrct_crs2` DAG.
    *   Manually trigger a run.
2.  **Monitor Airflow Task Status**:
    *   Observe the DAG run in the Airflow UI. All tasks (`start_task`, `run_bert_ausd_v_ta_cntrct_crs2`, `end_task`) should complete successfully (green status).
    *   Check the logs for the `run_bert_ausd_v_ta_cntrct_crs2` task for any errors or warnings from the PySpark script or BigQuery execution.
3.  **Verify BigQuery Job History**:
    *   Navigate to the BigQuery UI in the GCP Console.
    *   Check the "Job history" for successful BigQuery jobs initiated by the Dataproc cluster's service account. This should include the `TRUNCATE TABLE` and `INSERT INTO` operations on `sof_ta_cntrct_crs2`.
4.  **Data Validation**:
    *   **Row Count Comparison**: Compare the row count of the `sof_ta_cntrct_crs2` table in BigQuery with the expected row count from the original Oracle `sof$ta_cntrct_crs2` table after a successful run.
    *   **Data Sample Comparison**: Select a representative sample of records from `sof_ta_cntrct_crs2` in BigQuery and compare them against the corresponding records in the Oracle source to ensure data integrity and correctness of transformations (especially `rv_num` and filtering logic).
    *   **Schema Verification**: Confirm that the schema of `sof_ta_cntrct_crs2` in BigQuery matches the expected schema and data types.

**"Passing" Criteria**:
*   All Airflow DAG tasks complete successfully without errors.
*   BigQuery jobs for truncation and insertion complete successfully.
*   Row counts in the target `sof_ta_cntrct_crs2` table match expectations.
*   A sample of transformed data in `sof_ta_cntrct_crs2` is accurate and consistent with the original Oracle output.

## 7. Rollback Procedure

In case of critical issues or validation failures, the following rollback procedure can be initiated:

1.  **Disable Airflow DAG**:
    *   In the Cloud Composer UI, toggle off the `dw_bert_ausd_v_ta_cntrct_crs2` DAG to prevent further executions.
2.  **Revert to Original UC4 Job**:
    *   Re-enable the original `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job in UC4.
    *   Ensure all necessary Oracle database connections and permissions are active for the UC4 job.
3.  **Data Restoration (if necessary)**:
    *   Since the BigQuery job performs a `TRUNCATE TABLE` followed by an `INSERT`, if data corruption or incorrect transformation is detected, the `sof_ta_cntrct_crs2` table in BigQuery can be restored from a previous snapshot or by re-running the original Oracle job to populate a temporary BigQuery table, then copying it over.
    *   For `sof_ta_cntrct_crs` and `isbert_schema.dwtk_meldungen`, ensure their data remains consistent with the Oracle source.
4.  **Troubleshooting and Redesign**:
    *   Analyze the logs from Airflow, Dataproc, and BigQuery to identify the root cause of the failure.
    *   Address the identified issues in the BigQuery SQL, PySpark script, or Airflow DAG.
    *   Repeat the build and validation steps once fixes are implemented.