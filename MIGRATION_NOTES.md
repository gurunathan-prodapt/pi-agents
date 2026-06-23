# MIGRATION_NOTES.md: EXIS_SD_APT_RABATT

## 1. Summary

The `EXIS_SD_APT_RABATT` job, an ETL workflow for extracting, post-processing, and distributing discount data as a compressed CSV file, has been migrated from its legacy on-premises environment to Google Cloud Platform (GCP).

**Original Platform:**
*   **Orchestration:** UC4/Automic Workload Automation
*   **Data Source:** Oracle Database
*   **Data Processing:** Oracle PL/SQL, custom shell scripts (`nawk`, `gzip`)
*   **Data Distribution:** SFTP, local filesystem archiving

**Target Platform:**
*   **Orchestration:** Google Cloud Composer (Apache Airflow)
*   **Data Source:** Google BigQuery
*   **Data Processing:** Google BigQuery (Standard SQL), Python scripts
*   **Data Distribution:** Google Cloud Storage (GCS), Airflow SFTP operators/custom Python for external SFTP

The migration involved re-platforming the entire workflow, converting SQL dialects, reimplementing shell script logic in Python, and adapting file storage and transfer mechanisms to leverage GCP services.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`d_exis_apt_rabattdaten.bqsql`**
    *   **Role:** This file contains the core data extraction and transformation logic, translated from Oracle PL/SQL to Google Standard SQL. It queries the relevant BigQuery tables (`RPT_TA_S_D1_VERTRAG`, `RPT_TA_S_D1_DISCOUNT_RR`, `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`) to aggregate discount information. This SQL script is executed by the `BigQueryInsertJobOperator` within the Airflow DAG.

*   **`post_process_rabattdaten.py`**
    *   **Role:** This Python script replaces the functionality of the legacy `nawk` and `gzip` commands. It reads the raw CSV output from BigQuery (stored temporarily in GCS), applies specific post-processing (e.g., adding a trailer line with dynamic metadata), compresses the file using `gzip`, and uploads the final processed file to a designated GCS location. It is invoked by a `PythonOperator` in the Airflow DAG.

*   **`exis_sd_apt_rabatt_dag.py`**
    *   **Role:** This is the main Airflow DAG definition file. It orchestrates the entire `EXIS_SD_APT_RABATT` workflow on Google Cloud Composer. It defines the sequence of tasks, including BigQuery execution, Python post-processing, SFTP transfer, GCS archiving, and temporary file cleanup. It manages task dependencies, scheduling, and integrates with Airflow Variables for configuration.

## 3. Key Design Decisions

*   **Orchestration Re-platforming to Cloud Composer:** Apache Airflow on Google Cloud Composer was chosen to replace UC4 due to its managed nature, native integration with GCP services, Python-based DAGs for flexibility, and robust scheduling/monitoring capabilities.
*   **BigQuery for Data Transformation:** Google BigQuery was selected as the target data warehouse and SQL execution engine, replacing Oracle. This decision leverages BigQuery's scalability, performance for analytical queries, and cost-effectiveness.
*   **Python for Post-processing Logic:** Custom shell script logic (including `nawk` for trailer generation and `gzip` for compression) was re-implemented in Python. This provides better maintainability, testability, and integration within the Airflow ecosystem compared to executing shell commands directly.
*   **GCS as Primary Storage:** Google Cloud Storage (GCS) replaced local filesystem storage for intermediate data, final output files, and archiving. GCS offers high durability, availability, scalability, and seamless integration with other GCP services.
*   **SFTP Handling via Airflow Operators:** For external SFTP distribution, the design uses a placeholder `BashOperator` but recommends a dedicated Airflow `SFTPOperator` or a custom Python operator leveraging libraries like `paramiko`. This centralizes external connectivity management within Airflow.
*   **`LISTAGG` to `STRING_AGG` Conversion:** Oracle's `LISTAGG` function was directly translated to BigQuery's `STRING_AGG` for aggregating string values, ensuring functional equivalence.
*   **Explicit Joins in SQL:** Implicit Oracle join syntax was converted to explicit `INNER JOIN` clauses in BigQuery SQL for improved readability and adherence to modern SQL standards.
*   **Dynamic Filename Generation:** The original job's dynamic filename generation (e.g., `DWHM_APT_RABATTREPORT_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`) is replicated in the Python post-processing script using Python's `datetime` module, ensuring consistency.
*   **Configuration Management:** Airflow Variables are used to manage environment-specific configurations (e.g., GCS bucket names, BigQuery project/dataset IDs, SFTP details), promoting reusability and easier deployment across environments.

## 4. Manual Steps Before Go-Live

Before the `EXIS_SD_APT_RABATT` DAG can go live, the following manual steps are required:

1.  **BigQuery Dataset and Tables:**
    *   Ensure the BigQuery dataset (`your-bq-dataset-id`) exists.
    *   Verify that the source tables (`RPT_TA_S_D1_VERTRAG`, `RPT_TA_S_D1_DISCOUNT_RR`, `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`) are created and populated with data, either through migration or continuous replication from the Oracle source.

2.  **GCS Buckets and Directories:**
    *   Create the primary GCS data bucket (`your-gcs-data-bucket`) if it doesn't already exist.
    *   The DAG will create necessary prefixes (`tmp/EXIS_SD_APT_RABATT/`, `DW.DWH_APT_EXPORT_TAEGLICH_JP/work/`, `DW.DWH_APT_EXPORT_TAEGLICH_JP/store/`) within this bucket.

3.  **IAM Permissions:**
    *   The Service Account associated with the Cloud Composer environment must have the following IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery User` for query execution and `BigQuery Data Editor` for creating temp tables)
        *   `Storage Object Admin` (or `Storage Object Creator` and `Storage Object Viewer` for GCS operations)
        *   `Composer Worker` (default for Composer)

4.  **Airflow Connections:**
    *   **`google_cloud_default`**: Ensure the default GCP connection is properly configured in Airflow, typically pointing to the Composer environment's service account.
    *   **`sftp_external_connection`**: Create an Airflow Connection of type `SFTP` with the following details for the external SFTP target:
        *   `Conn Id`: `sftp_external_connection` (or the value set in `SFTP_CONNECTION_ID` Airflow Variable)
        *   `Host`: `$DW_APT_SFTP_SERVER` (e.g., `sftp.example.com`)
        *   `Port`: `$DW_APT_SFTP_PORT` (e.g., `22`)
        *   `Login`: `$DW_APT_SFTP_USER` (e.g., `sftpuser`)
        *   `Password` or `Key File`: Use appropriate secure credentials. Consider storing sensitive credentials in GCP Secret Manager and integrating with Airflow.

5.  **Airflow Variables:**
    *   Set the following Airflow Variables in the Airflow UI (Admin -> Variables) or via `gcloud composer environments run ... variables set`:
        *   `gcs_data_bucket`: Name of your GCS bucket (e.g., `my-project-data-bucket`)
        *   `bq_project_id`: Your GCP project ID (e.g., `my-gcp-project`)
        *   `bq_dataset_id`: Your BigQuery dataset ID (e.g., `dwh_raw`)
        *   `dw_dir_exp_apt`: The base directory for exports (e.g., `DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP`)
        *   `sftp_conn_id`: The Airflow Connection ID for SFTP (e.g., `sftp_external_connection`)
        *   `sftp_remote_path`: The remote directory on the SFTP server (e.g., `/sftp/apt/rabatt`)

6.  **SFTP Host Key (if using BashOperator for SFTP):**
    *   If the `sftp_transfer_file` task remains a `BashOperator` that directly calls the `sftp` client, the SFTP server's host key must be added to the `known_hosts` file on the Cloud Composer worker nodes to avoid interactive prompts. This is generally not recommended for production and should be replaced by a proper `SFTPOperator`.

7.  **Deploy DAG and Python Script:**
    *   Upload `exis_sd_apt_rabatt_dag.py` and `post_process_rabattdaten.py` to the DAGs folder of your Cloud Composer environment.

8.  **Scheduling:**
    *   Verify the `schedule_interval` in `exis_sd_apt_rabatt_dag.py` matches the desired daily execution frequency of the original UC4 job.

## 5. Known Gaps & Unresolved References

*   **Undocumented `r_exis_v2` Logic:** The full functionality of the original `r_exis_v2` script was not entirely clear from the configuration. The Python post-processing script (`post_process_rabattdaten.py`) assumes the `nawk` and `gzip` operations were the primary functions. Any other complex logic, error handling, or specific environment setup within `r_exis_v2` not explicitly defined might be missing. **(B4 Item: Further analysis of `r_exis_v2` source code is recommended if available.)**
*   **Source of `<FROM YYYYMMDD>`:** The exact origin and meaning of the `<FROM YYYYMMDD>` date in the original `nawk` trailer line were not definitively identified. The Python script currently uses Airflow's `ds_nodash` (execution date) for this value. This should be validated with business users.
*   **SFTP Connectivity and Security:** The current `sftp_transfer_file` task uses a `BashOperator` as a placeholder. This approach is not robust for production due to security concerns (credentials, host key management) and error handling. **(B4 Item: Replace `BashOperator` with a dedicated Airflow `SFTPOperator` or a custom Python operator using `paramiko` for secure and reliable SFTP transfer, ensuring proper credential management via Airflow Connections or Secret Manager.)**
*   **Performance of Python Post-processing:** For extremely large datasets, reading an entire CSV into Pandas, processing, and compressing within a single Airflow `PythonOperator` might become a bottleneck or exceed worker memory limits. **(B4 Item: If performance issues arise with very large files, consider re-evaluating this step for potential re-implementation using Google Cloud Dataflow for scalable processing.)**
*   **UC4 Job Dependencies:** This migration focuses solely on `EXIS_SD_APT_RABATT`. If this job had upstream or downstream dependencies within UC4 or external systems, those dependencies need to be identified and managed in Airflow (e.g., using `ExternalTaskSensor` or by coordinating schedules). **(B4 Item: Review UC4 dependency graphs to ensure all inter-job relationships are accounted for in the Airflow ecosystem.)**
*   **GCS Archiving Task Implementation:** The `archive_processed_file` task uses a `PythonOperator` with a lambda function to perform a copy-then-delete operation. While functional, a more direct and idiomatic Airflow operator like `GoogleCloudStorageMoveObjectOperator` (if available and suitable for single object moves) or a dedicated `GCSHook` method for moving objects would be cleaner.

## 6. Validation

To validate the successful migration and operation of the `EXIS_SD_APT_RABATT` job:

1.  **Trigger the DAG:**
    *   In the Cloud Composer UI, navigate to the `EXIS_SD_APT_RABATT_dag`.
    *   Manually trigger a run using the "Trigger DAG" button.

2.  **Monitor Task Execution:**
    *   Observe the DAG run in the Airflow UI. All tasks (`extract_transform_bq_to_gcs`, `post_process_and_compress_data`, `sftp_transfer_file`, `archive_processed_file`, `cleanup_temp_raw_csv`) should complete successfully (green status).
    *   Check task logs for any errors or warnings.

3.  **Verify Output in GCS:**
    *   After `post_process_and_compress_data` completes, verify that a compressed CSV file (e.g., `DWHM_APT_RABATTREPORT_YYYYMMDDHHMMSS.csv.gz`) exists in the GCS `work` directory (`gs://<your-gcs-data-bucket>/<DW_DIR_EXP_APT>/work/`).
    *   After `archive_processed_file` completes, verify that the file has been moved from the `work` directory to the `archive` directory (`gs://<your-gcs-data-bucket>/<DW_DIR_EXP_APT>/store/`).
    *   Confirm that the temporary raw CSV file (`gs://<your-gcs-data-bucket>/tmp/EXIS_SD_APT_RABATT/raw_rabattdaten_YYYYMMDD.csv`) is deleted after `cleanup_temp_raw_csv` completes.

4.  **Verify SFTP Transfer:**
    *   Confirm with the external SFTP target system owner that the `DWHM_APT_RABATTREPORT_YYYYMMDDHHMMSS.csv.gz` file has been successfully received in the specified remote path (`$SFTP_REMOTE_PATH`).

5.  **Data Validation (What "Passing" Means):**
    *   **File Content:**
        *   Download the `.gz` file from GCS and decompress it.
        *   Open the CSV file and verify the separator (`|`) and header row.
        *   Crucially, check the last line of the CSV for the correctly formatted trailer line: `X|DWHM_APT_RABATTREPORT_YYYYMMDDHHMMSS.csv|YYYYMMDD|ROW_COUNT|V_S_Rabattreport|YYYYMMDD`.
        *   Verify `ROW_COUNT` matches the actual number of data rows in the file.
    *   **Data Accuracy:**
        *   Compare a sample of the generated data with the output from the legacy system for the same execution date.
        *   Perform row count comparisons between the legacy output and the new BigQuery/GCS output.
        *   Spot-check key fields and aggregated values (e.g., `BASISPRODUKTE` from `STRING_AGG`) to ensure consistency.
    *   **Performance:**
        *   Monitor the execution time of the DAG and individual tasks to ensure it meets or exceeds the performance of the legacy job.

## 7. Rollback Procedure

In case of critical issues or failure during go-live, the following rollback procedure should be followed:

1.  **Disable New Airflow DAG:**
    *   In the Cloud Composer UI, toggle off the `EXIS_SD_APT_RABATT_dag` to prevent further scheduled or manual runs.

2.  **Re-enable Legacy UC4 Job:**
    *   Re-activate the original `DW.DWH_EXIS_SD_APT_RABATT.xml` job in UC4/Automic Workload Automation.
    *   Verify that the legacy job can run successfully and produce its output as expected.

3.  **Clean Up GCP Artifacts (Optional, but Recommended):**
    *   **GCS:** Delete any partially generated or incorrect files from the GCS `work` and `archive` directories for the affected run date.
        *   `gs://<your-gcs-data-bucket>/<DW_DIR_EXP_APT>/work/DWHM_APT_RABATTREPORT_*.csv.gz`
        *   `gs://<your-gcs-data-bucket>/<DW_DIR_EXP_APT>/store/DWHM_APT_RABATTREPORT_*.csv.gz`
        *   `gs://<your-gcs-data-bucket>/tmp/EXIS_SD_APT_RABATT/raw_rabattdaten_*.csv`
    *   **BigQuery:** If any temporary tables were created by the `BigQueryInsertJobOperator` and not cleaned up, manually delete them from the BigQuery dataset.

4.  **Root Cause Analysis and Remediation:**
    *   Investigate the cause of the failure using Airflow logs, BigQuery logs, and GCS logs.
    *   Address the identified issues (e.g., correct SQL, fix Python logic, adjust IAM permissions, update Airflow Variables/Connections, resolve SFTP connectivity).
    *   Once the issues are resolved and thoroughly tested in a staging environment, the migration process can be re-attempted.