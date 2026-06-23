# MIGRATION_NOTES.md

## 1. Summary

The `EXIS_SD_APT_NNA_VOIC` job, originally an Automic (UC4) orchestrated ETL workflow exporting telephone system master data from an Oracle Data Warehouse to a compressed CSV via SFTP, has been migrated to Google Cloud Platform (GCP).

The target platform utilizes:
*   **Google Cloud Composer (Apache Airflow)** for orchestration.
*   **Google BigQuery** for SQL-based data extraction and transformation.
*   **Google Cloud Dataproc** to execute a Python script (`r_exis_v2.py`) for post-processing (header/trailer addition) and gzip compression.
*   **Google Cloud Storage** as a staging area for intermediate and final files.
*   **Google Cloud Run** for secure external SFTP distribution.

## 2. Generated artifacts

The migration process generated the following files:

*   **`d_exis_apt_nna_voice.bqsql`**
    *   **Role:** This file contains the BigQuery SQL query responsible for extracting and transforming voice-related data. It is a direct migration of the original Oracle SQL (`d_exis_apt_nna_voice.sql`), adapted for BigQuery syntax and parameterized for dynamic month selection. It will be executed by the `r_exis_v2.py` script.
*   **`r_exis_v2.py`**
    *   **Role:** This Python script, designed to run on a Dataproc cluster, encapsulates the logic previously handled by the `r_exis_v2` shell script and the `h_exis_apt_nna_voice.var` configuration file. Its responsibilities include:
        1.  Executing the `d_exis_apt_nna_voice.bqsql` query in BigQuery.
        2.  Exporting the BigQuery results to a temporary CSV file in Cloud Storage.
        3.  Downloading the CSV locally.
        4.  Adding a custom header and trailer to the CSV data (mimicking `nawk` functionality).
        5.  Gzip compressing the final CSV file.
        6.  Uploading the compressed file to the designated Cloud Storage output bucket.
*   **`dw_dwh_exis_sd_apt_nna_voic.py`**
    *   **Role:** This is the Apache Airflow DAG definition file. It orchestrates the entire end-to-end workflow on Cloud Composer. It defines tasks to:
        1.  Dynamically calculate the `MONAT_ID` (YYYYMM) parameter.
        2.  Submit the `r_exis_v2.py` script as a PySpark job to a Dataproc cluster.
        3.  Trigger a Cloud Run service (`sftp-transfer-service`) to distribute the final gzipped CSV from Cloud Storage to the external SFTP target.
*   **`cloud_run_sftp_service.py`**
    *   **Role:** This Python Flask application is deployed as a Google Cloud Run service. It replaces the legacy SFTP distribution mechanism. It receives a GCS file path, downloads the file, and then securely uploads it to the configured external SFTP server.

## 3. Key design decisions

*   **Orchestration from UC4 to Cloud Composer (Airflow):** Airflow provides robust scheduling, monitoring, and dependency management capabilities, aligning with modern cloud-native ETL patterns. This replaces the proprietary UC4 scheduler.
*   **Data Transformation from Oracle SQL to BigQuery SQL:** Migrating the core SQL logic to BigQuery leverages its serverless, scalable, and cost-effective data warehousing capabilities. This eliminates the need for a managed Oracle instance for this specific workload.
*   **Post-processing and Compression via Python on Dataproc:** The original `r_exis_v2` shell script and `h_exis_apt_nna_voice.var` config file contained `nawk` and `gzip` logic. This was re-implemented in Python (`r_exis_v2.py`) and deployed on Dataproc. While Cloud Functions or Cloud Run could handle simpler post-processing, Dataproc was chosen to provide flexibility for potential future scaling or more complex data manipulation if the `r_exis_v2` script had hidden complexities.
*   **Cloud Storage as Central Staging:** All intermediate and final files are stored in Cloud Storage, providing a durable, scalable, and highly available object storage solution, replacing local file system operations.
*   **External SFTP Distribution via Cloud Run:** Instead of relying on a generic SFTP client within a VM or a complex Airflow operator, a dedicated Cloud Run service was chosen for SFTP distribution. This provides a serverless, scalable, and secure way to handle external file transfers, isolating SFTP credentials and logic. It also allows for easier auditing and management of external connectivity.
*   **Parameterization of `MONAT_ID`:** The dynamic `MONAT_ID` (YYYYMM) calculation, previously handled by UC4's `SYS_DATE` and `SUBSTR` functions, is now managed within the Airflow DAG using Python's `datetime` module, ensuring consistency and maintainability.
*   **"Retire" Bucket for Config/SQL:** The original configuration (`.var`) and SQL (`.sql`) files were marked for `retire`. This implies that their logic was absorbed and re-engineered into the new GCP components rather than being directly translated, leading to a more cloud-native and maintainable solution.

**Notable Trade-offs:**
*   **Dataproc for `r_exis_v2.py`:** While Dataproc offers powerful processing, for simple header/trailer addition and gzip, a Cloud Function or Cloud Run service might have been a lighter-weight and potentially more cost-effective choice if the `r_exis_v2` script was confirmed to be very simple. The current choice provides headroom for future complexity.
*   **Custom Cloud Run for SFTP:** While Cloud Storage Transfer Service can handle SFTP, a custom Cloud Run service offers greater flexibility for complex SFTP scenarios (e.g., specific key management, pre/post-transfer scripting, dynamic remote paths) and better integration with Airflow for immediate triggering post-file generation. However, it requires more development and maintenance overhead than a managed service.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset (`{{ var.value.bq_dataset_name }}` e.g., `dwh_transformed`) exists in your GCP project.
    *   Verify that all source Oracle DWH tables (`DWH_VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH_VI_C_VERTRAG`, `DWH_VI_F_NNV_TVD_12_MONATE`, `DWH_VI_L_TVD_LEISTUNGSKLASSE`) have been successfully migrated to BigQuery within the appropriate dataset and are accessible.

2.  **Google Cloud Storage (GCS) Bucket Creation:**
    *   Create the GCS bucket for temporary BigQuery exports (`{{ var.value.gcs_temp_bucket_name }}` e.g., `your-bucket-temp`).
    *   Create the GCS bucket for final gzipped output (`{{ var.value.gcs_output_bucket_name }}` e.g., `your-bucket-exports`).
    *   Ensure the specified output prefix (`{{ var.value.gcs_output_prefix }}` e.g., `exis_data/nna_voice`) is understood and correctly configured.

3.  **IAM Permissions Configuration:**
    *   **Cloud Composer Service Account:** Grant the Composer service account (or the service account used by the Airflow worker) the following roles:
        *   `BigQuery Data Editor` (to run queries and create temporary tables).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   `Storage Object Admin` (to read/write to GCS buckets, including temporary and output buckets, and to access DAGs/code).
        *   `Dataproc Editor` (to submit and manage Dataproc jobs).
        *   `Cloud Run Invoker` (to trigger the `sftp-transfer-service` Cloud Run job).
    *   **Dataproc Worker Service Account:** Grant the service account used by the Dataproc cluster workers:
        *   `BigQuery Data Viewer` (to read from source tables).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   `Storage Object Admin` (to read/write to GCS buckets, including temporary and output buckets, and to access `r_exis_v2.py` and `d_exis_apt_nna_voice.bqsql`).
    *   **Cloud Run Service Account (`sftp-transfer-service`):** Grant the Cloud Run service account:
        *   `Storage Object Viewer` (to download the final gzipped CSV from GCS).
        *   Ensure it has network access and any necessary firewall rules to connect to the external SFTP host.

4.  **Airflow Variables Configuration:**
    *   In your Cloud Composer environment, create the following Airflow Variables:
        *   `gcp_project_id`: Your GCP Project ID.
        *   `gcp_region`: The GCP region where your Dataproc cluster and Cloud Run service are located.
        *   `dataproc_cluster_name`: The name of your Dataproc cluster.
        *   `dataproc_code_bucket`: The GCS bucket where `r_exis_v2.py` is uploaded (e.g., `gs://your-dataproc-code-bucket`).
        *   `dags_code_bucket`: The GCS bucket where `d_exis_apt_nna_voice.bqsql` is uploaded (e.g., `gs://your-dags-code-bucket`).
        *   `gcs_temp_bucket_name`: The GCS bucket for temporary BigQuery exports (e.g., `your-bucket-temp/bq_exports`).
        *   `gcs_output_bucket_name`: The GCS bucket for final gzipped output (e.g., `your-bucket-exports`).
        *   `gcs_output_prefix`: The GCS path prefix within the output bucket (e.g., `exis_data/nna_voice`).
        *   `bq_dataset_name`: The BigQuery dataset for temporary tables (e.g., `dwh_transformed`).

5.  **Dataproc Cluster Setup:**
    *   Ensure a Dataproc cluster named `{{ var.value.dataproc_cluster_name }}` is provisioned and running in the specified region. It should have network connectivity to BigQuery and Cloud Storage.

6.  **Cloud Run Service Deployment (`sftp-transfer-service`):**
    *   Deploy the `cloud_run_sftp_service.py` application as a Cloud Run service named `sftp-transfer-service`.
    *   Configure the following environment variables for the Cloud Run service (ideally using Secret Manager for sensitive values):
        *   `SFTP_HOST`: The hostname or IP address of the external SFTP server.
        *   `SFTP_PORT`: The port for SFTP (default 22).
        *   `SFTP_USERNAME`: The username for SFTP authentication.
        *   `SFTP_PASSWORD`: The password for SFTP authentication (consider using SSH keys for production).
        *   `SFTP_REMOTE_PATH`: The target directory on the SFTP server (e.g., `/remote/incoming/`).
    *   Ensure the Cloud Run service is configured to allow internal invocations (e.g., from Airflow).

7.  **Upload Code to GCS:**
    *   Upload `d_exis_apt_nna_voice.bqsql` to the GCS bucket specified by `dags_code_bucket`.
    *   Upload `r_exis_v2.py` to the GCS bucket specified by `dataproc_code_bucket`.
    *   Upload `dw_dwh_exis_sd_apt_nna_voic.py` to your Cloud Composer DAGs folder in GCS.

8.  **Scheduling:**
    *   Update the `schedule` parameter in `dw_dwh_exis_sd_apt_nna_voic.py` to the desired cron schedule or interval (e.g., `"@daily"`, `"0 5 * * *"`) based on the original UC4 job's schedule.

## 5. Known gaps & unresolved references

*   **Missing Complexity Data:** The absence of `file_complexity` data for original source files means the migration effort and potential challenges were estimated without detailed static analysis. Manual review was required to fill this gap.
*   **Incomplete Lineage Edges:** The lack of explicit `lineage_edges` in the source inventory meant that the execution order and data flow had to be inferred from file content, which could lead to misinterpretations if not thoroughly validated.
*   **`r_exis_v2` Executable Functionality:** The exact functionality of the original `r_exis_v2` shell script was not fully known without its source code. The Python implementation (`r_exis_v2.py`) assumes standard data processing, header/trailer addition, and compression. Any hidden complexities or specific business logic within the original script would require further investigation and potential refinement of `r_exis_v2.py`.
*   **Dynamic `MONAT_ID` Derivation:** The Airflow DAG currently calculates `MONAT_ID` for the *previous month*. This aligns with a common ETL pattern but should be explicitly confirmed against the exact business logic of the original UC4 job's `SYS_DATE('YYYYMMDD')` and `SUBSTR(...,1,6)` usage.
*   **SFTP Distribution Confirmation:** The exact mechanism, credentials (password vs. SSH key), and specific remote path requirements for the outgoing SFTP distribution need to be confirmed with the external target system owner and securely configured in the Cloud Run service. Using SSH keys is highly recommended for production environments.
*   **BigQuery Export to Single File:** The `r_exis_v2.py` script includes logic to concatenate multiple CSV files if BigQuery exports them in parts. While this handles the scenario, it's generally more efficient to ensure BigQuery exports to a single file if possible (e.g., by limiting data size or using specific export options).
*   **Error Handling and Retries:** While basic exception handling is present, a production-grade solution would require more robust error handling, retry mechanisms (e.g., Airflow retries, Cloud Run retries), and alerting for each component.

## 6. Validation

To validate the successful migration of the `EXIS_SD_APT_NNA_VOIC` job:

1.  **Trigger the Airflow DAG:**
    *   Access the Cloud Composer UI (Airflow UI).
    *   Find the `dw_dwh_exis_sd_apt_nna_voic` DAG.
    *   Manually trigger the DAG.
    *   Monitor the DAG run in the Airflow UI to ensure all tasks complete successfully.

2.  **Verify Task Execution:**
    *   **`calculate_monat_id` & `get_monat_id_var`:** Check task logs to confirm the correct `MONAT_ID` (YYYYMM) is calculated and passed.
    *   **`submit_dataproc_job`:**
        *   Check Dataproc job logs for successful execution of `r_exis_v2.py`.
        *   Verify that the BigQuery query (`d_exis_apt_nna_voice.bqsql`) ran successfully in BigQuery.
        *   Confirm a temporary CSV file was created in the `gcs_temp_bucket_name` and then cleaned up.
        *   Confirm the final gzipped CSV file (`DWHM_APT_NNA_Daten_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`) is present in the `gcs_output_bucket_name` under the specified `gcs_output_prefix`.
    *   **`distribute_via_sftp`:**
        *   Check Cloud Run service logs for the `sftp-transfer-service` to confirm the file download from GCS and successful upload to the external SFTP server.

3.  **Data Integrity Checks:**
    *   **File Content:** Download the generated gzipped CSV from GCS and decompress it.
        *   Verify the presence and correctness of the custom header and trailer.
        *   Check the record count in the trailer matches the actual number of data rows.
        *   Inspect a sample of data rows for correct formatting and values.
    *   **Data Comparison:**
        *   Compare the record count of the generated file with the record count from a corresponding run of the legacy system.
        *   Perform checksums or row-by-row comparisons on a representative sample of the data against the legacy output to ensure data accuracy and completeness.
        *   Verify that the `DAUER_MIN` and `RBETRAG_VBUD_NETTO_EURO` calculations are correct (e.g., `ROUND(value/60, 2)` and `ROUND(value/100, 2)`).

**"Passing" means:**
*   The Airflow DAG completes with all tasks marked as "success".
*   No errors are reported in the logs of any GCP service involved (Composer, Dataproc, BigQuery, Cloud Storage, Cloud Run).
*   A gzipped CSV file with the correct naming convention is successfully generated in the target GCS output bucket.
*   The generated CSV file contains the expected header and trailer, and its data content is accurate and complete when compared to the legacy system's output.
*   The gzipped CSV file is successfully transferred to the external SFTP target.

## 7. Rollback procedure

In case of critical failure or unexpected issues during or after go-live, the following rollback procedure should be followed:

1.  **Disable New Airflow DAG:**
    *   In the Cloud Composer UI (Airflow UI), locate the `dw_dwh_exis_sd_apt_nna_voic` DAG.
    *   Toggle the DAG to "Off" to prevent any further runs.

2.  **Re-enable Legacy Automic Job:**
    *   Access the Automic (UC4) system.
    *   Locate and re-enable the original `DW.DWH_EXIS_SD_APT_NNA_VOIC` job.
    *   Ensure its original schedule and dependencies are restored.

3.  **Verify Legacy Job Execution:**
    *   Monitor the re-enabled Automic job to confirm it runs successfully and produces the expected output in the legacy environment.
    *   Verify that the generated output file is correctly distributed via the original SFTP mechanism.

4.  **Clean Up (Optional, if rollback is permanent):**
    *   If the rollback is deemed permanent, consider deleting the migrated Airflow DAG, Dataproc cluster, Cloud Run service, and associated GCS buckets/objects to avoid incurring unnecessary costs. This step should only be performed after a thorough assessment that the legacy system is stable and the migration attempt is being abandoned or significantly re-planned.