# MIGRATION_NOTES: DW.BERT_AUSD_V_TA_CNTRCT_CRS3

## 1. Summary

The `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` job, originally defined in UC4 and executing KornShell scripts with Oracle SQL, has been migrated to Google Cloud Platform (GCP). This job is responsible for synchronizing and updating contract data, specifically populating the `ta_cntrct_crs3` table with contract and "twin-bill" information.

The migration involved:
*   **Orchestration**: From UC4 to Apache Airflow (Cloud Composer).
*   **Script Execution**: From KornShell scripts on a Unix host to a Python script executed on Google Cloud Dataproc.
*   **Data Warehousing**: From Oracle Database to Google BigQuery.
*   **Data Transformation**: From Oracle SQL to BigQuery SQL.

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`ddl/isbert_schema.dwtk_meldungen.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the `isbert_schema.dwtk_meldungen` table. This table is a source for the transformation.
*   **`ddl/sof.ta_cntrct_crs2.sql`**
    *   **Role**: BigQuery DDL script to create the `sof.ta_cntrct_crs2` table. This table is a primary source for the transformation.
*   **`ddl/sof.ta_cntrct_crs3.sql`**
    *   **Role**: BigQuery DDL script to create the `sof.ta_cntrct_crs3` table. This is the target table where the transformed contract data will be loaded.
*   **`sql/d_ausd_v_ta_cntrct_crs3.bqsql`**
    *   **Role**: The core data transformation logic, converted from Oracle SQL to BigQuery SQL. It truncates `sof.ta_cntrct_crs3` and inserts data by joining and unioning from `sof.ta_cntrct_crs2` and `isbert_schema.dwtk_meldungen`.
*   **`python/r_ausd_v_ta_cntrct_crs3.py`**
    *   **Role**: A Python script that encapsulates the logic previously handled by the KornShell wrapper (`r_ausd_v_ta_cntrct_crs3.ksh`) and control (`k_ausd_v_ta_cntrct_crs3.ksh`) scripts. It parses arguments, handles logging, and executes the `d_ausd_v_ta_cntrct_crs3.bqsql` script in BigQuery.
*   **`airflow/dw_bert_ausd_v_ta_cntrct_crs3.py`**
    *   **Role**: The Airflow Directed Acyclic Graph (DAG) definition. This DAG orchestrates the job by submitting the `r_ausd_v_ta_cntrct_crs3.py` Python script to a Dataproc cluster using a `DataprocSubmitJobOperator`.

## 3. Key Design Decisions

*   **Cloud Composer for Orchestration**: Apache Airflow (managed by Cloud Composer) was chosen to replace UC4 due to its native Python support, robust scheduling capabilities, and seamless integration with other GCP services. This provides a modern, scalable, and observable orchestration layer.
*   **BigQuery for Data Warehousing**: Oracle tables were migrated to BigQuery to leverage its serverless, highly scalable, and cost-effective data warehousing capabilities. BigQuery's performance for analytical queries is a significant advantage over traditional relational databases for this type of workload.
*   **Python on Dataproc for Script Logic**: The KornShell scripts' logic (environment setup, parameter parsing, error handling, SQL execution) was re-implemented in Python. Executing this Python script on Dataproc via `DataprocSubmitJobOperator` provides a managed execution environment that can scale resources as needed, replacing the fixed Unix host. While the generated script is plain Python, Dataproc's `pyspark_job` type can execute it, providing a consistent execution model.
*   **Direct BigQuery SQL Conversion**: The core Oracle SQL transformation was directly translated to BigQuery SQL. This approach maximizes BigQuery's native query optimization and performance, removing Oracle-specific syntax (e.g., `(+)` outer joins, `PARALLEL` hints, `NVL`, `TO_CHAR`) and replacing them with BigQuery equivalents. The `TRUNCATE/INSERT` pattern was maintained.
*   **Simplified Error Handling and Logging**: Custom KornShell error handling and logging mechanisms were replaced with standard Python logging, which integrates directly with GCP Cloud Logging for centralized monitoring and debugging.
*   **Manual Trigger and No Retries**: Based on the analysis of the original UC4 job, the Airflow DAG is configured for manual triggering (`schedule_interval=None`) and no automatic retries (`retries=0`) by default, aiming for functional equivalence with the legacy scheduling behavior.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the BigQuery datasets `isbert_schema` and `sof` in your GCP project if they do not already exist.
    *   Command example: `bq mk --dataset your-gcp-project-id:isbert_schema`
    *   Command example: `bq mk --dataset your-gcp-project-id:sof`

2.  **BigQuery Table Creation**:
    *   Execute the DDL scripts (`ddl/isbert_schema.dwtk_meldungen.sql`, `ddl/sof.ta_cntrct_crs2.sql`, `ddl/sof.ta_cntrct_crs3.sql`) to create the necessary tables in BigQuery.
    *   Ensure the schemas accurately reflect the source Oracle tables, including all relevant columns and their correct data types.

3.  **Initial Data Ingestion**:
    *   Migrate historical and initial data from the Oracle tables `isbert_schema.dwtk_meldungen` and `sof$ta_cntrct_crs2` into their respective BigQuery tables (`isbert_schema.dwtk_meldungen`, `sof.ta_cntrct_crs2`). This can be done using GCP Data Transfer Service, Dataflow, or manual export/import.
    *   Establish an ongoing data synchronization mechanism if these tables are continuously updated in Oracle.

4.  **IAM Permissions**:
    *   Ensure the Service Account used by your Cloud Composer environment (Airflow worker) and the Dataproc cluster has the following minimum IAM roles:
        *   `BigQuery Data Editor` (for `sof.ta_cntrct_crs3` and `isbert_schema.dwtk_meldungen` if it's updated by other jobs)
        *   `BigQuery Data Viewer` (for `sof.ta_cntrct_crs2` and `isbert_schema.dwtk_meldungen`)
        *   `Dataproc Worker` (for the Dataproc cluster service account)
        *   `Storage Object Viewer` (for the GCS bucket containing scripts)
        *   `Storage Object Creator` (if the Python script writes temporary files to GCS)

5.  **Dataproc Cluster Setup**:
    *   Ensure a Dataproc cluster named `your-dataproc-cluster-name` (as specified in the Airflow DAG) is provisioned and running in the `your-gcp-region` (e.g., `us-central1`) of your GCP project.
    *   Replace the placeholder values `PROJECT_ID`, `REGION`, `CLUSTER_NAME` in `airflow/dw_bert_ausd_v_ta_cntrct_crs3.py` with your actual GCP environment details.

6.  **GCS Bucket for Scripts**:
    *   Create a Google Cloud Storage (GCS) bucket, e.g., `gs://your-gcs-bucket-for-dataproc-scripts`.
    *   Upload the `python/r_ausd_v_ta_cntrct_crs3.py` and `sql/d_ausd_v_ta_cntrct_crs3.bqsql` files to this GCS bucket. The Python script expects the SQL file to be in the same directory on the Dataproc worker.
    *   Update the `GCS_BUCKET_FOR_SCRIPTS` placeholder in `airflow/dw_bert_ausd_v_ta_cntrct_crs3.py` to point to this bucket.

7.  **Airflow DAG Deployment**:
    *   Upload the `airflow/dw_bert_ausd_v_ta_cntrct_crs3.py` file to the DAGs folder of your Cloud Composer environment.

## 5. Known Gaps & Unresolved References

*   **`v_carmen` DB Link**: The original Oracle SQL contained `DEFINE v_carmen = "@pcrs1"`. While not directly used in the provided SQL, if `sof$ta_cntrct_crs2` or `isbert_schema.dwtk_meldungen` are views or tables that rely on this DB link to the Carmen database, then Carmen is an upstream dependency. Its migration or connectivity to GCP needs to be established and validated. This remains an unresolved external dependency.
*   **UC4 Include Directives (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`)**: The exact functionality of these UC4 `:inc` directives was not fully retrieved. It is assumed their environment setup or logging functions have been adequately addressed by the Python script's refactoring and integration with Cloud Logging. Further investigation might be needed if unexpected environment or logging issues arise.
*   **Data Type Mismatches**: While a best effort was made to map Oracle data types to BigQuery, subtle differences in precision, scale, or date/timestamp handling might exist. Comprehensive data validation is required to confirm type compatibility and prevent data loss or corruption.
*   **Helper Script Complexity**: The original KornShell scripts sourced several helper scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`). The Python script `r_ausd_v_ta_cntrct_crs3.py` re-implements their assumed core functionalities (parameter parsing, logging, SQL execution). If these helper scripts contained complex business logic not captured, it could be a gap.
*   **`pyspark_job` type for plain Python**: The Airflow DAG uses `pyspark_job` to submit the Python script. While Dataproc can execute plain Python scripts this way, it's typically used for PySpark applications. This is a minor point but could be optimized to a `PythonOperator` if the Python script is simple enough to run directly on an Airflow worker, or a `DataprocSubmitJobOperator` with `python_job` if a full Dataproc cluster is still desired for resource isolation/scalability without PySpark. For now, the current setup is functional.

## 6. Validation

To validate the successful migration and operation of `DW.BERT_AUSD_V_TA_CNTRCT_CRS3`:

1.  **Trigger the Airflow DAG**:
    *   Navigate to the Airflow UI for your Cloud Composer environment.
    *   Find the `dw_bert_ausd_v_ta_cntrct_crs3` DAG.
    *   Manually trigger a new DAG run.

2.  **Monitor Airflow Task Execution**:
    *   Observe the DAG run in the Airflow UI. Ensure the `bert_ausd_v_ta_cntrct_crs3` task transitions through `running` to `success`.
    *   Check task logs in the Airflow UI for any errors or warnings. These logs are integrated with Cloud Logging.

3.  **Verify BigQuery Job Completion**:
    *   In the GCP Console, navigate to BigQuery.
    *   Go to "Query history" or "Job history" and confirm that a BigQuery job corresponding to the SQL execution was initiated and completed successfully.
    *   Check BigQuery job details for any errors, warnings, or performance metrics (e.g., duration, bytes processed, slot usage).

4.  **Data Validation**:
    *   Query the target table `sof.ta_cntrct_crs3` in BigQuery to confirm that data has been inserted.
    *   Perform a row count comparison between the legacy Oracle `sof$ta_cntrct_crs3` table and the new BigQuery `sof.ta_cntrct_crs3` table for a specific run.
    *   Execute sample queries on `sof.ta_cntrct_crs3` in BigQuery and compare the results with equivalent queries on the legacy Oracle table to ensure functional equivalence and data accuracy. Pay close attention to `twinbill` and `twin_vertrag_id` columns.

**"Passing" means**:
*   The Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3` completes successfully without any task failures.
*   The underlying BigQuery job completes without errors.
*   The `sof.ta_cntrct_crs3` table in BigQuery is populated with data.
*   The data in `sof.ta_cntrct_crs3` is functionally equivalent and accurate when compared to the output of the legacy Oracle job.
*   The BigQuery job execution time and resource consumption are within acceptable performance thresholds.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable Airflow DAG**:
    *   In the Airflow UI, toggle off the `dw_bert_ausd_v_ta_cntrct_crs3` DAG to prevent further executions.

2.  **Revert to Legacy UC4 Job**:
    *   Re-enable and resume scheduling of the original `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` job in UC4.
    *   Verify that the legacy job is running as expected and populating the Oracle `sof$ta_cntrct_crs3` table.

3.  **Data Integrity Check (Optional but Recommended)**:
    *   If there's any concern about data corruption in the BigQuery `sof.ta_cntrct_crs3` table due to the migrated job, consider restoring it from a previous snapshot or a known good state, or simply truncating it if the legacy system is now the source of truth. Since this job uses `TRUNCATE/INSERT`, the impact is usually limited to the target table itself.

4.  **Cleanup (Post-Rollback)**:
    *   Once the legacy system is confirmed to be fully operational, the deployed GCP resources (Airflow DAG, Python scripts in GCS, Dataproc cluster if dedicated) can be reviewed, debugged, or removed/disabled for further investigation.