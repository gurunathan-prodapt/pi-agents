# MIGRATION_NOTES: EXIS_SD_APT_BESTANDS

## 1. Summary

The UC4 job `DW.DWH_EXIS_SD_APT_BESTANDS`, responsible for exporting stock data, has been migrated to Google Cloud Platform (GCP).

**Original System:**
*   **Job Name:** `DW.DWH_EXIS_SD_APT_BESTANDS`
*   **Platform:** UC4 (orchestration), UNIX host `DWHDWH5P` (execution)
*   **Core Logic:** External executable `r_exis_v2` with configuration `h_exis_apt_bestandsdaten.var`
*   **Source Data:** `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `RPT$TA_S_D1_VERTRAG`
*   **Output:** Compressed CSV file `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz`

**Target Platform:**
*   **Orchestration:** Airflow (managed by Cloud Composer)
*   **Computation:** PySpark application executed on a Dataproc cluster
*   **Source Data:** BigQuery tables (migrated from original sources)
*   **Output Storage:** Google Cloud Storage (GCS) for the compressed CSV file

This migration re-platforms the data export process to leverage GCP's scalable and managed services.

## 2. Generated Artifacts

The migration produced the following files:

*   **`r_exis_v2.py`**
    *   **Role:** This is a PySpark application that re-implements the core data extraction, transformation, and loading logic previously contained within the `r_exis_v2` executable and its configuration file. It reads data from BigQuery, applies transformations, and writes the resulting gzipped CSV file to a specified GCS bucket.
    *   **Location:** This script should be uploaded to a GCS bucket, e.g., `gs://YOUR_BUCKET_NAME/dags/pyspark/r_exis_v2.py`, to be accessible by Dataproc.

*   **`dw_dwh_exis_sd_apt_bestands.py`**
    *   **Role:** This is an Airflow DAG definition. It orchestrates the execution of the `r_exis_v2.py` PySpark application by submitting it as a job to a Dataproc cluster. It defines the workflow, dependencies, and parameters for the job.
    *   **Location:** This file should be deployed to the DAGs folder of the Cloud Composer environment.

## 3. Key Design Decisions

*   **Cloud-Native Re-platforming:** The decision was made to fully re-platform the job to GCP, leveraging managed services like Cloud Composer (Airflow), Dataproc, BigQuery, and GCS. This provides benefits in terms of scalability, reliability, reduced operational overhead, and integration with the broader GCP ecosystem.
*   **PySpark for Transformation Logic:** The custom `r_exis_v2` executable was re-implemented as a PySpark application. This allows for distributed processing of large datasets, efficient integration with BigQuery (using the Spark-BigQuery connector), and direct output to GCS. It replaces the need for a custom compiled executable and shell scripting.
*   **BigQuery as Source Data Store:** The original source tables were migrated to BigQuery. This provides a highly scalable, performant, and cost-effective data warehouse solution that integrates seamlessly with Dataproc and other GCP services.
*   **GCS for Output Storage:** The output compressed CSV file is stored in GCS. This offers durable, highly available, and scalable object storage, suitable for data exports and integration with other GCP services or external systems.
*   **Airflow for Orchestration:** Airflow on Cloud Composer was chosen for orchestration due to its robust scheduling capabilities, dependency management, monitoring, and extensibility, providing a modern and flexible workflow management system.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the environment and deploy the migrated job:

1.  **BigQuery Setup:**
    *   **Create Dataset:** Create a BigQuery dataset (e.g., `YOUR_BIGQUERY_DATASET`) in your GCP project.
    *   **Migrate Source Tables:** Ensure the original source tables (`SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `RPT$TA_S_D1_VERTRAG`) are migrated to BigQuery with appropriate schemas and data loaded. The BigQuery table names should be `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`, and `RPT_TA_S_D1_VERTRAG` (without the `$` and in uppercase).

2.  **GCS Bucket Setup:**
    *   **Create Bucket:** Create a GCS bucket (e.g., `YOUR_BUCKET_NAME`) to store the PySpark script and the output CSV files.

3.  **Dataproc Cluster Setup:**
    *   **Create Cluster:** Ensure a Dataproc cluster (e.g., `YOUR_DATAPROC_CLUSTER_NAME`) is provisioned and running in the specified `YOUR_DATAPROC_REGION`. The cluster should have network access to BigQuery and GCS.

4.  **IAM Permissions:**
    *   **Cloud Composer Service Account:** Grant the service account used by your Cloud Composer environment the following roles:
        *   `Dataproc Worker` (or custom role with `dataproc.jobs.create`, `dataproc.jobs.get`, `dataproc.jobs.update`, `dataproc.jobs.list`)
        *   `BigQuery Data Viewer` (to read from source tables)
        *   `Storage Object Viewer` (to read the PySpark script from GCS)
        *   `Storage Object Creator` (to write output to GCS)
    *   **Dataproc Cluster Service Account:** Grant the service account associated with the Dataproc cluster the following roles:
        *   `BigQuery Data Viewer` (to read from source tables)
        *   `Storage Object Creator` (to write output to GCS)

5.  **Configuration Updates:**
    *   **Airflow DAG (`dw_dwh_exis_sd_apt_bestands.py`):**
        *   Replace `YOUR_GCP_PROJECT_ID` with your actual GCP project ID.
        *   Replace `YOUR_DATAPROC_REGION` with the region where your Dataproc cluster is located.
        *   Replace `YOUR_DATAPROC_CLUSTER_NAME` with the name of your Dataproc cluster.
        *   Replace `YOUR_BUCKET_NAME` with the name of your GCS bucket.
        *   Replace `YOUR_BIGQUERY_DATASET` with the name of your BigQuery dataset.
        *   **Schedule:** Update `schedule=None` to the desired cron expression or preset (e.g., `"@daily"`, `"0 0 * * *"`) based on the original UC4 job's schedule.
    *   **PySpark Script (`r_exis_v2.py`):**
        *   **Crucially, implement the actual transformation logic** derived from reverse-engineering `r_exis_v2` and `h_exis_apt_bestandsdaten.var` in the designated `TRANSFORMATION LOGIC` section. This is a significant manual effort.

6.  **Deployment:**
    *   **Upload PySpark Script:** Upload the completed `r_exis_v2.py` script to the GCS path specified in the DAG (e.g., `gs://YOUR_BUCKET_NAME/dags/pyspark/r_exis_v2.py`).
    *   **Deploy Airflow DAG:** Upload the updated `dw_dwh_exis_sd_apt_bestands.py` to the DAGs folder of your Cloud Composer environment.

## 5. Known Gaps & Unresolved References

*   **`r_exis_v2` Transformation Logic (B4 Item):** The most significant gap is the placeholder transformation logic in `r_exis_v2.py`. The exact business rules, SQL queries, joins, filtering, and output schema from the original `r_exis_v2` executable and `h_exis_apt_bestandsdaten.var` must be manually reverse-engineered and implemented in PySpark. This is a critical redesign/re-implementation effort.
*   **UC4 Includes (`DW.HOLE_PFAD`, `DW.LESE_LOG`):** The precise functionality of these UC4 includes needs to be fully understood. `DW.HOLE_PFAD` likely retrieves a path, which might be replaced by Airflow variables or GCS paths. `DW.LESE_LOG` suggests logging, which will be handled by Airflow's native logging to Cloud Logging. Confirmation is needed if any specific logic needs to be replicated.
*   **`.dw_init` Script:** The contents and environmental impact of the original `.dw_init` shell script need to be analyzed. Any critical environment variables, paths, or functions set by this script must be replicated in the Dataproc environment or passed as PySpark arguments if necessary.
*   **Original UC4 Schedule:** The specific schedule of the original UC4 job was not provided. This needs to be determined from business requirements and configured in the Airflow DAG.
*   **Output to BigQuery:** The design document mentions the output CSV could "potentially be loaded into a BigQuery table if required for further downstream processing or analytics." This is a potential follow-up item if such a requirement arises.

## 6. Validation

To validate the successful migration and operation of the `EXIS_SD_APT_BESTANDS` job:

1.  **Trigger the DAG:**
    *   Access the Airflow UI for your Cloud Composer environment.
    *   Unpause the `dw_dwh_exis_sd_apt_bestands` DAG.
    *   Manually trigger the DAG, or wait for its scheduled run (once the schedule is configured).

2.  **Monitor Execution:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks (`start`, `dwh_exis_sd_apt_bestands_pyspark_export`, `end`) complete successfully.
    *   Check the logs for the `dwh_exis_sd_apt_bestands_pyspark_export` task for any errors or warnings from the PySpark job. These logs will be available in Cloud Logging.
    *   Verify that a Dataproc job was successfully submitted and completed in the Dataproc console.

3.  **Verify Output:**
    *   **GCS Output:** Navigate to the specified GCS bucket (`gs://YOUR_BUCKET_NAME/exports/`). Confirm that a new gzipped CSV file named `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` has been created.
    *   **Content Validation:**
        *   Download the generated CSV.GZ file and decompress it.
        *   Compare the structure (column headers, data types) and content (row counts, specific data points) of the generated CSV with historical outputs from the original UC4 job or with expected results based on the source data and re-implemented logic.
        *   Perform data quality checks to ensure data integrity and correctness.

**Passing Criteria:**
*   The Airflow DAG completes successfully without any failed tasks.
*   A gzipped CSV file is generated in the correct GCS location with the expected naming convention.
*   The content of the generated CSV file is accurate and complete, matching the expected output based on the fully implemented transformation logic.

## 7. Rollback Procedure

In case of critical failure or unexpected behavior after go-live, follow these steps to roll back to the original system:

1.  **Pause New Job:** Immediately pause the `dw_dwh_exis_sd_apt_bestands` Airflow DAG in the Cloud Composer UI to prevent further execution of the migrated job.

2.  **Re-enable Original Job:** Re-enable and/or manually trigger the original UC4 job `DW.DWH_EXIS_SD_APT_BESTANDS` to ensure business continuity and data export.

3.  **Cleanup (Optional):** If necessary, delete any erroneous or partially generated output files from the GCS bucket that were produced by the failed migrated job run.

4.  **Investigate and Rectify:** Analyze the logs and error messages from the failed Airflow DAG and Dataproc job to identify the root cause of the issue. Make necessary corrections to the `r_exis_v2.py` PySpark script, the `dw_dwh_exis_sd_apt_bestands.py` DAG, or the GCP infrastructure configuration.

5.  **Re-deploy and Re-validate:** Once fixes are implemented, re-deploy the updated artifacts and repeat the validation steps.