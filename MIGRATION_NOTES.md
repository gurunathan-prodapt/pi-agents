# MIGRATION_NOTES.md

## 1. Summary

The `EXIS_SD_APT_BESTANDS` job, originally a UC4 JOBS_UNIX object executing a custom binary (`r_exis_v2`) to export stock data as a gzipped CSV, has been migrated to Google Cloud Platform.

The migration re-platforms the workflow as follows:
*   **Orchestration**: From UC4 scheduler to Apache Airflow on Google Cloud Composer.
*   **Data Processing**: From a custom Unix binary (`r_exis_v2`) to a PySpark script executed on Google Cloud Dataproc.
*   **Source Data**: From direct Oracle database access to BigQuery tables (assuming an upstream migration/replication of Oracle data to BigQuery).
*   **Output Landing Zone**: From a local Unix filesystem to Google Cloud Storage (GCS).

The primary goal is to maintain the existing business logic of extracting stock data and exporting it as a gzipped CSV file (`DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz`), while leveraging GCP's managed services for scalability, reliability, and cost efficiency.

## 2. Generated Artifacts

The migration produced the following key artifacts:

*   **`dags/dw_dwh_exis_sd_apt_bestands.py`**
    *   **Role**: This is the Airflow DAG definition file. It orchestrates the execution of the data export process. It defines the workflow, including the `DataprocSubmitJobOperator` task responsible for launching the PySpark job on a Dataproc cluster. It also manages Airflow-specific configurations like scheduling, retries, and dependencies.
*   **`pyspark_scripts/r_exis_v2.py`**
    *   **Role**: This PySpark script is the re-implementation of the original `r_exis_v2` binary. It connects to BigQuery to read data from the source tables (`SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `RPT$TA_S_D1_VERTRAG`), applies the necessary transformation logic (joins, filters, aggregations), and writes the resulting dataset as a gzipped CSV file to a specified GCS location. It also includes logic to read and interpret the configuration from the `h_exis_apt_bestandsdaten.var` file.

## 3. Key Design Decisions

*   **Orchestration with Airflow on Cloud Composer**: Airflow provides a robust, scalable, and cloud-native solution for workflow orchestration, replacing the legacy UC4 scheduler. Cloud Composer simplifies Airflow deployment and management on GCP.
*   **Data Processing with PySpark on Dataproc**:
    *   **Choice of PySpark**: PySpark is a widely adopted framework for large-scale data processing, offering strong capabilities for ETL (Extract, Transform, Load) operations. It's well-suited for replicating the complex data extraction and transformation logic of the original `r_exis_v2` binary.
    *   **Execution on Dataproc**: Dataproc is GCP's managed Apache Spark service, providing a fully managed, scalable, and cost-effective environment for running PySpark jobs without the overhead of managing underlying infrastructure. This replaces the Unix-based execution environment.
*   **BigQuery as Source Data Platform**: Instead of connecting directly to the legacy Oracle database from Dataproc, the design assumes that the Oracle source tables are already migrated or continuously replicated into BigQuery. This simplifies data access for PySpark, leverages BigQuery's analytical capabilities, and aligns with a modern data warehousing strategy on GCP.
*   **GCS for Output Landing Zone**: Google Cloud Storage is used as the target for the exported gzipped CSV files. This provides a highly durable, scalable, and cost-effective object storage solution, maintaining the file-based output format while integrating seamlessly with other GCP services.
*   **Separation of Concerns (DAG vs. PySpark)**: The Airflow DAG focuses solely on orchestration, while the PySpark script encapsulates the data processing logic. This promotes modularity, reusability, and easier maintenance.
*   **Configuration Management**: Airflow Variables are used for environment-specific GCP configurations (project ID, cluster name, bucket names), providing a centralized and secure way to manage these settings. The legacy `h_exis_apt_bestandsdaten.var` configuration file is stored in GCS and read by the PySpark job, ensuring its parameters are applied to the new implementation.

## 4. Manual Steps Before Go-Live

Before the `dw_dwh_exis_sd_apt_bestands` DAG can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `raw_oracle_data` (or the chosen equivalent) exists in your GCP project.
    *   Verify that the source tables (`SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`, `RPT_TA_S_D1_VERTRAG`) are present within this dataset and are being continuously ingested/replicated from the legacy Oracle source.

2.  **GCS Bucket Provisioning**:
    *   Create three dedicated GCS buckets:
        *   `gs://<GCS_CODE_BUCKET>`: To store the `pyspark_scripts/r_exis_v2.py` file.
        *   `gs://<GCS_OUTPUT_BUCKET>`: To store the generated `DWHM_APT_BESTANDSREPORT_*.csv.gz` output files.
        *   `gs://<GCS_CONFIG_BUCKET>`: To store the `h_exis_apt_bestandsdaten.var` configuration file.

3.  **Upload Files to GCS**:
    *   Upload `pyspark_scripts/r_exis_v2.py` to `gs://<GCS_CODE_BUCKET>/pyspark_scripts/r_exis_v2.py`.
    *   Upload the translated `h_exis_apt_bestandsdaten.var` configuration file to `gs://<GCS_CONFIG_BUCKET>/apt/cfg/h_exis_apt_bestandsdaten.var`.

4.  **Dataproc Cluster Provisioning**:
    *   Ensure a Dataproc cluster named `your-dataproc-cluster` (or the chosen name) is provisioned and running in the specified `DATAPROC_REGION`. This cluster will execute the PySpark job. Configure it with appropriate machine types and worker counts based on expected data volume and processing needs.

5.  **IAM Permissions Configuration**:
    *   **Cloud Composer Service Account**: Grant the service account associated with your Cloud Composer environment the following roles:
        *   `Dataproc Editor` (or `Dataproc Worker` + `Dataproc Viewer` + `Service Account User` on Dataproc worker service account) to submit jobs to Dataproc.
        *   `Storage Object Viewer` on `gs://<GCS_CODE_BUCKET>` to read the PySpark script.
        *   `Storage Object Viewer` on `gs://<GCS_CONFIG_BUCKET>` to read the configuration file.
    *   **Dataproc Worker Service Account**: Grant the service account used by the Dataproc cluster workers the following roles:
        *   `BigQuery Data Viewer` on the BigQuery dataset (`raw_oracle_data`) to read source data.
        *   `Storage Object Viewer` on `gs://<GCS_CODE_BUCKET>` to read the PySpark script.
        *   `Storage Object Viewer` on `gs://<GCS_CONFIG_BUCKET>` to read the configuration file.
        *   `Storage Object Creator` and `Storage Object Viewer` on `gs://<GCS_OUTPUT_BUCKET>` to write the output CSV files.

6.  **Airflow Variables Configuration**:
    *   In your Airflow UI (Admin -> Variables), set the following variables:
        *   `gcp_project_id`: Your Google Cloud Project ID.
        *   `dataproc_region`: The GCP region where your Dataproc cluster is located (e.g., `us-central1`).
        *   `dataproc_cluster_name`: The name of your Dataproc cluster.
        *   `gcs_code_bucket`: The name of the GCS bucket for PySpark scripts.
        *   `gcs_output_bucket`: The name of the GCS bucket for output CSV files.
        *   `gcs_config_bucket`: The name of the GCS bucket for the configuration file.

7.  **Airflow DAG Scheduling**:
    *   Determine the exact `SCHEDULE_INTERVAL` for the DAG based on the legacy UC4 job's schedule. Update the `SCHEDULE_INTERVAL` variable in `dags/dw_dwh_exis_sd_apt_bestands.py` accordingly (e.g., `"0 5 * * *"` for daily at 5 AM UTC).
    *   Set the `START_DATE` in the DAG to a fixed date in the past.

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up and require further attention, including potential redesign (B4 items):

*   **B4: `r_exis_v2` Logic Re-implementation**: The `pyspark_scripts/r_exis_v2.py` currently contains placeholder transformation logic. The exact business logic (joins, filters, aggregations, column selections, data type conversions) of the original `r_exis_v2` binary needs to be thoroughly reverse-engineered or documented and fully implemented in the PySpark script. This is the most critical gap.
*   **B4: `h_exis_apt_bestandsdaten.var` Parsing Logic**: The `read_config_file` function in `r_exis_v2.py` provides a basic key-value parsing example. The actual format and content of `h_exis_apt_bestandsdaten.var` must be fully understood, and the parsing logic in the PySpark script must be robustly implemented to correctly interpret all parameters.
*   **Target System for File Distribution**: The "downstream target system" for the exported CSV file is currently unspecified. The mechanism for securely transferring the file from GCS to this target system needs to be defined and implemented. This might involve Cloud Storage Transfer Service, custom API integration, SFTP, or other methods depending on the target's requirements.
*   **UC4 Schedule Determination**: The `SCHEDULE_INTERVAL` in the Airflow DAG is currently `None`. The precise schedule of the legacy UC4 job needs to be determined and configured in the DAG.
*   **UC4 Variables and Includes Translation**: The UC4 job used includes like `DW.HOLE_PFAD` and variables like `:set &DWH_JOB_KENNUNG`. The exact values, scope, and impact of these on the original job's execution environment and logic need to be fully understood and translated into Airflow variables, Dataproc job arguments, or PySpark logic as appropriate.
*   **BigQuery Source Table Column Names**: The PySpark script uses generic column names (e.g., `some_id`, `another_id`, `column1`). These must be replaced with the actual column names from the migrated Oracle tables in BigQuery.
*   **Dataproc Cluster Type**: The current design assumes a persistent Dataproc cluster. If an ephemeral cluster is preferred (created and deleted with each job run), the Airflow DAG would need to be updated to include `DataprocCreateClusterOperator` and `DataprocDeleteClusterOperator`.

## 6. Validation

To validate the successful migration and functionality of the `dw_dwh_exis_sd_apt_bestands` job:

1.  **Trigger the Airflow DAG**:
    *   Access the Airflow UI in Cloud Composer.
    *   Unpause the `dw_dwh_exis_sd_apt_bestands` DAG.
    *   Manually trigger a run of the DAG.

2.  **Monitor Execution**:
    *   Observe the DAG run in the Airflow UI. Ensure all tasks (`start`, `run_dwh_exis_sd_apt_bestands`, `end`) complete successfully without errors.
    *   Check the logs of the `run_dwh_exis_sd_apt_bestands` task for any PySpark-related errors or warnings.
    *   Monitor the Dataproc cluster for job submission and execution status.

3.  **Verify Output in GCS**:
    *   Navigate to the `gs://<GCS_OUTPUT_BUCKET>` in the Google Cloud Console.
    *   Confirm that a new gzipped CSV file named `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` (where `yyyymmddhhmmss` corresponds to the execution timestamp) has been created.

4.  **"Passing" Criteria**:
    *   **Successful DAG Run**: The Airflow DAG completes successfully with all tasks marked as "success".
    *   **Output File Generation**: A gzipped CSV file is generated in the specified GCS output bucket.
    *   **Data Integrity and Accuracy**: This is the most critical criterion.
        *   **Schema Match**: The generated CSV file's header and column order must exactly match the expected output schema of the legacy `r_exis_v2` binary.
        *   **Record Count**: The number of records in the generated CSV file should match the record count from the legacy job's output for the same input data.
        *   **Data Content Match**: A sample of the data (or the entire dataset if feasible) from the generated CSV must be compared against the output of the legacy `r_exis_v2` binary. This comparison should verify that all data values are identical, accounting for any potential floating-point precision differences or date format variations.
        *   **File Size**: The file size should be comparable to the legacy output, indicating a similar volume of data.
    *   **Performance**: The job should complete within an acceptable time frame, ideally matching or improving upon the legacy job's execution time.

## 7. Rollback Procedure

In case of issues or failure during the go-live or subsequent runs, the following rollback procedure should be followed:

1.  **Disable New Airflow DAG**:
    *   In the Airflow UI, immediately unpause the `dw_dwh_exis_sd_apt_bestands` DAG to prevent further runs.

2.  **Re-enable Legacy UC4 Job**:
    *   Re-enable the original `EXIS_SD_APT_BESTANDS` job in the UC4 scheduler.
    *   Verify that the UC4 job runs successfully according to its original schedule and produces the expected output.

3.  **Verify Legacy Output**:
    *   Confirm that the legacy job generates the `DWHM_APT_BESTANDSREPORT_*.csv.gz` file in its original location and that its content is correct.

4.  **Investigate and Rectify**:
    *   Analyze the logs and errors from the failed Airflow DAG run and Dataproc job.
    *   Address the identified issues in the Airflow DAG, PySpark script, or underlying GCP infrastructure/configuration.
    *   Once the issues are resolved and thoroughly tested in a non-production environment, the migration can be re-attempted.

5.  **Cleanup (Optional)**:
    *   If necessary, delete any erroneous output files generated in `gs://<GCS_OUTPUT_BUCKET>` during the failed migration attempt.