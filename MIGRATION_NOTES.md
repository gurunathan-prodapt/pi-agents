# MIGRATION_NOTES.md for EXIS_SD_APT_BESTANDS

## 1. Summary

The `EXIS_SD_APT_BESTANDS` job, originally orchestrated by UC4 and utilizing Oracle PL/SQL for data extraction, a custom exporter framework for post-processing, and SFTP for distribution, has been migrated.

The job's functionality has been re-platformed to **Google Cloud Composer (Airflow)** for orchestration. The core data extraction and transformation logic, previously in Oracle PL/SQL, has been translated to **BigQuery SQL**. The custom exporter framework's responsibilities (file generation, compression, dynamic footer addition, and SFTP distribution) are now handled by **BigQuery's export capabilities**, **Google Cloud Storage** for intermediate staging, and **Python Operators within Airflow** for custom post-processing and SFTP transfer.

The target platform leverages Google Cloud Platform services for a scalable, managed, and cloud-native solution.

## 2. Generated artifacts

The migration produced the following files:

*   **`sql/d_exis_apt_bestandsdaten.bqsql`**
    *   **Role:** Contains the BigQuery SQL query responsible for extracting and transforming data from the migrated source tables. This SQL is a direct translation of the original Oracle PL/SQL script (`d_exis_apt_bestandsdaten.sql`), adapted for BigQuery syntax and functions. It is designed to be executed by an Airflow task.

*   **`dags/dw_dwh_exis_sd_apt_bestands_dag.py`**
    *   **Role:** This is the Airflow Directed Acyclic Graph (DAG) that orchestrates the entire `EXIS_SD_APT_BESTANDS` job. It defines the sequence of tasks, including:
        *   Executing the BigQuery SQL query.
        *   Exporting the query results to a compressed CSV file in Google Cloud Storage.
        *   Adding a custom footer line to the generated CSV file (replicating the original `nawk` logic).
        *   Transferring the final file from Google Cloud Storage to the external SFTP target.
    *   This DAG replaces the original UC4 `JOBS_UNIX` object (`DW.DWH_EXIS_SD_APT_BESTANDS.xml`) and integrates the logic previously handled by the custom exporter framework (`h_exis_apt_bestandsdaten.var`).

## 3. Key design decisions

*   **Orchestration Re-platforming to Airflow:** The UC4 `JOBS_UNIX` object was migrated to an Airflow DAG (`dw_dwh_exis_sd_apt_bestands_dag.py`). This decision aligns with the strategy of adopting a cloud-native, managed orchestration service (Cloud Composer) for improved scalability, monitoring, and integration with other GCP services.
*   **Data Extraction & Transformation with BigQuery:** The Oracle PL/SQL (`d_exis_apt_bestandsdaten.sql`) was translated to BigQuery SQL (`d_exis_apt_bestandsdaten.bqsql`). This leverages BigQuery's analytical capabilities, performance, and cost-effectiveness for large-scale data processing, replacing the dependency on the legacy Oracle database for this specific extraction.
*   **Decomposition of Custom Exporter Framework:** The `h_exis_apt_bestandsdaten.var` configuration, which defined SQL source, output path, post-processing (`nawk`, `gzip`), and SFTP details, was retired. Its functionalities were absorbed by:
    *   **BigQueryToCloudStorageOperator:** Handles CSV export and GZIP compression directly from BigQuery.
    *   **PythonOperator for Custom Footer:** A dedicated Python function (`_add_csv_footer_to_gcs_file`) within the DAG handles the specific `nawk` logic for adding a dynamic footer line, as BigQuery's export does not natively support this.
    *   **PythonOperator for SFTP Transfer:** Another Python function (`_sftp_file_from_gcs`) manages the secure transfer of the final file from GCS to the external SFTP server, leveraging Airflow's `SFTPHook`.
*   **Intermediate Cloud Storage for File Staging:** The generated CSV.gz file is first exported to a Google Cloud Storage bucket. This provides a highly available and durable staging area, decoupling the BigQuery export from the SFTP transfer and allowing for intermediate post-processing (like adding the footer).
*   **`print_header=False` in BigQuery Export:** The `BigQueryToCloudStorageOperator` is configured to export data without a header. This is crucial because the original `nawk` logic adds a custom footer, and the subsequent Python operator is designed to append to the data, not replace or modify an existing header.
*   **Temporary BigQuery Table for Query Results:** The BigQuery query first writes its results to a temporary table (`BIGQUERY_TEMP_TABLE_PREFIX_{{{{ ts_nodash }}}}`). This ensures that the query execution is complete and stable before the export process begins, and provides a clear point for potential debugging or validation of the query output before file generation.

## 4. Manual steps before go-live

Before the `EXIS_SD_APT_BESTANDS` DAG can be run successfully in production, the following manual steps must be completed:

1.  **BigQuery Dataset and Table Creation:**
    *   Ensure the BigQuery dataset specified by `BIGQUERY_DATASET_ID` (e.g., `your_dataset`) exists in `GCP_PROJECT_ID`.
    *   The source Oracle tables (`RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`) must be fully migrated to BigQuery. The corresponding BigQuery tables (e.g., `your_project.your_dataset.RPT_TA_S_D1_VERTRAG`) must exist and contain the necessary data.
    *   **Action:** Create dataset and tables, load initial data.
    *   **Update:** Replace `your_project` and `your_dataset` placeholders in `d_exis_apt_bestandsdaten.bqsql` and `dw_dwh_exis_sd_apt_bestands_dag.py` with actual values.

2.  **Google Cloud Storage Bucket Configuration:**
    *   Create a Google Cloud Storage bucket to serve as the intermediate staging area for the exported CSV.gz files.
    *   **Action:** Create GCS bucket.
    *   **Update:** Replace `your-gcs-bucket-name` in `dw_dwh_exis_sd_apt_bestands_dag.py` with the actual bucket name.

3.  **Airflow SFTP Connection Setup:**
    *   Create an Airflow Connection of type `SFTP` with the ID `sftp_default`.
    *   This connection must contain the correct SFTP server details:
        *   **Host:** The SFTP server's hostname or IP address.
        *   **Port:** The SFTP port (default is 22).
        *   **Username:** The username for SFTP authentication.
        *   **Password/Key:** The password or SSH private key for authentication. It is highly recommended to use SSH keys and store them securely (e.g., in Google Secret Manager, referenced by Airflow).
    *   **Action:** Configure `sftp_default` connection in Airflow UI.
    *   **Update:** Replace `SFTP_REMOTE_PATH` in `dw_dwh_exis_sd_apt_bestands_dag.py` with the actual remote directory on the SFTP server.

4.  **IAM Permissions for Composer Service Account:**
    *   The Google Cloud service account associated with your Cloud Composer environment must have the following IAM roles/permissions:
        *   **BigQuery Data Editor:** To run queries, create temporary tables, and export data (`bigquery.jobs.create`, `bigquery.tables.create`, `bigquery.tables.updateData`, `bigquery.tables.getData`, etc.).
        *   **Storage Object Admin:** To write and read objects from the specified GCS bucket (`storage.objects.create`, `storage.objects.get`, `storage.objects.delete`).
        *   **Secret Manager Secret Accessor (if using Secret Manager for SFTP credentials):** To retrieve SFTP credentials.
    *   **Action:** Grant necessary IAM roles to the Composer service account.

5.  **Determine and Configure DAG Schedule:**
    *   The `schedule_interval` in the DAG is currently set to `None` as the original UC4 schedule was not derivable from the provided XML.
    *   **Action:** Identify the exact schedule of the original `EXIS_SD_APT_BESTANDS` job from UC4 scheduling objects (e.g., `EVNT_TIME`, `JOBP`) and update the `schedule_interval` parameter in `dw_dwh_exis_sd_apt_bestands_dag.py` accordingly (e.g., using a cron expression).

6.  **Update GCP Project ID:**
    *   **Action:** Replace `your_project` in `dw_dwh_exis_sd_apt_bestands_dag.py` with your actual GCP Project ID.

## 5. Known gaps & unresolved references

*   **UC4 Schedule:** The precise execution schedule of the original `EXIS_SD_APT_BESTANDS` job remains undetermined from the provided documentation. This needs to be confirmed and configured in the Airflow DAG's `schedule_interval`.
*   **`r_exis_v2` Functionality:** The design document noted uncertainty regarding the exact behavior of the custom executable `r_exis_v2`. The current migration assumes its core ETL logic is fully covered by the BigQuery SQL and Python operators for file manipulation and SFTP. If `r_exis_v2` had other, uncaptured functionalities (e.g., complex external system interactions, specific logging mechanisms not replicated by Airflow), these would represent a gap.
*   **Oracle Tables to BigQuery Mapping Finalization:** The BigQuery table names in the generated SQL (`your_project.your_dataset.RPT_TA_S_D1_VERTRAG`, etc.) are placeholders. These must be finalized and confirmed to match the actual migrated table names and locations in BigQuery.
*   **SFTP Connection Details:** The `SFTP_CONN_ID` and `SFTP_REMOTE_PATH` in the DAG are placeholders. While `sftp_default` is a common convention, the actual connection ID and remote path must be configured correctly in Airflow and match the external SFTP target.
*   **Error Handling and Retries:** The DAG's `default_args` sets `retries=0`, based on the UC4 analysis. This means tasks will not be retried upon failure. This is a **B4 item** for review; a more robust retry policy might be desirable for production environments to handle transient issues.
*   **Monitoring and Alerting:** While Airflow provides basic monitoring, specific alerts for job failures, long-running tasks, or data quality issues need to be configured in GCP (e.g., Cloud Monitoring, Cloud Logging) to ensure operational visibility.

## 6. Validation

To validate the migrated `EXIS_SD_APT_BESTANDS` job, follow these steps:

1.  **Trigger the Airflow DAG:**
    *   Upload `dags/dw_dwh_exis_sd_apt_bestands_dag.py` and `sql/d_exis_apt_bestandsdaten.bqsql` to your Cloud Composer environment's DAGs folder.
    *   Access the Airflow UI.
    *   Locate the `dw_dwh_exis_sd_apt_bestands` DAG.
    *   Manually trigger a run of the DAG.

2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks (`start`, `run_bestands_query_to_temp_table`, `export_temp_table_to_gcs`, `add_csv_footer_to_gcs_file`, `sftp_file_to_external_target`, `end`) execute successfully without errors.
    *   Check the logs for each task for any warnings or unexpected output.

3.  **Verify BigQuery Output:**
    *   After `run_bestands_query_to_temp_table` completes, verify that the temporary BigQuery table (`BIGQUERY_TEMP_TABLE_PREFIX_<timestamp>`) was created and contains the expected number of rows and data.

4.  **Verify GCS File Content:**
    *   After `export_temp_table_to_gcs` and `add_csv_footer_to_gcs_file` complete, navigate to the configured GCS bucket (`your-gcs-bucket-name`).
    *   Locate the generated file (e.g., `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz`).
    *   Download and decompress the file.
    *   **Passing Criteria:**
        *   The file should be a valid CSV.
        *   The data rows should match the expected output from the BigQuery query.
        *   The file should **not** contain a header row (due to `print_header=False`).
        *   The file **must** contain the custom footer line at the very end, matching the format: `X|<DESTINATION_FILE>|<FROM YYYYMMDD>|<RECORD_COUNT>|V_S_Bestandsreport|<SYSDATE YYYYMMMMDD>`.
        *   The `RECORD_COUNT` in the footer should accurately reflect the number of data rows in the file.

5.  **Verify SFTP Transfer:**
    *   After `sftp_file_to_external_target` completes, log in to the external SFTP server.
    *   Navigate to the `SFTP_REMOTE_PATH` directory.
    *   **Passing Criteria:**
        *   The `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` file should be present in the SFTP target directory.
        *   The file size and content (after download and decompression) should be identical to the file verified in GCS.

6.  **Data Integrity Check:**
    *   Compare the row count and a sample of the data in the final SFTP file with the output generated by the original UC4 job for the same period. This is the ultimate "passing" criterion for data accuracy.

## 7. Rollback procedure

In case of issues with the migrated job, the following rollback procedure should be followed:

1.  **Disable New Airflow DAG:**
    *   In the Airflow UI, toggle off the `dw_dwh_exis_sd_apt_bestands` DAG to prevent further runs.

2.  **Re-enable Original UC4 Job:**
    *   Re-activate the original `DW.DWH_EXIS_SD_APT_BESTANDS.xml` UC4 job in the legacy system.
    *   Ensure its schedule is restored and it can execute successfully.

3.  **Verify Original Job Functionality:**
    *   Monitor the re-enabled UC4 job to confirm it is running as expected and producing the correct output files to the SFTP target.

4.  **Clean Up Migrated Artifacts (Optional but Recommended):**
    *   **BigQuery:** Delete any temporary BigQuery tables created by the failed Airflow runs (e.g., `BIGQUERY_TEMP_TABLE_PREFIX_<timestamp>`).
    *   **Cloud Storage:** Delete any partially or incorrectly generated CSV.gz files from the designated GCS bucket.
    *   **SFTP Target:** If any incorrect files were transferred to the SFTP target, coordinate with the recipient to remove them.

5.  **Root Cause Analysis:**
    *   Investigate the cause of the failure in the migrated Airflow DAG using Airflow logs, BigQuery logs, and Cloud Logging. Address the identified issues before attempting another migration or re-deployment.