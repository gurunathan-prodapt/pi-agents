# MIGRATION_NOTES.md: EXIS_SD_APT_RABATT

## 1. Summary

The `EXIS_SD_APT_RABATT` job, originally orchestrated by UC4 (Automic Workload Automation) and involving Oracle data extraction, custom `nawk` post-processing, `gzip` compression, and SFTP distribution, has been migrated to Google Cloud Platform (GCP).

The target platform leverages:
*   **Orchestration:** Cloud Composer (Apache Airflow)
*   **Data Processing:** BigQuery (for SQL transformation) and Python (for post-processing)
*   **Data Landing & Archiving:** Cloud Storage
*   **Data Distribution:** SFTP (via Airflow `SFTPOperator`)

The job's primary function remains the extraction of discount-related data, its transformation, and distribution as a compressed CSV file (`DWHM_APT_RABATTREPORT_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`).

## 2. Generated Artifacts

The migration produced the following key artifacts:

*   **`sql/ddl/bq_oracle_source_tables.sql`**
    *   **Role:** Provides placeholder BigQuery DDL (Data Definition Language) for the Oracle source tables (`RPT_TA_S_D1_VERTRAG`, `RPT_TA_S_D1_DISCOUNT_RR`, `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`). These tables are expected to be replicated into a BigQuery dataset named `ORACLE_DATA`. This script is for initial setup and schema definition.
*   **`sql/rabatt_data_extraction.sql`**
    *   **Role:** Contains the BigQuery Standard SQL query that replaces the original Oracle PL/SQL logic (`d_exis_apt_rabattdaten.sql`). It performs the data extraction and transformation, including the `STRING_AGG` equivalent of Oracle's `LISTAGG`, from the replicated Oracle source tables in BigQuery.
*   **`python/post_process_rabatt_data.py`**
    *   **Role:** A Python script that re-implements the `nawk`-like post-processing and `gzip` compression logic previously defined in `h_exis_apt_rabattdaten.var`. It reads a CSV file from Cloud Storage, adds a custom header and footer line, and then compresses the result into a `.csv.gz` file, uploading it back to Cloud Storage.
*   **`dags/exis_sd_apt_rabatt_dag.py`**
    *   **Role:** The Apache Airflow DAG (Directed Acyclic Graph) that orchestrates the entire `EXIS_SD_APT_RABATT` workflow. It defines the sequence of tasks, including BigQuery execution, GCS export, Python post-processing, SFTP distribution, and GCS archiving. This DAG replaces the UC4 UNIX job definition (`DW.DWH_EXIS_SD_APT_RABATT.xml`).

## 3. Key Design Decisions

*   **Orchestration Shift from UC4 to Cloud Composer (Airflow):** Airflow provides a robust, cloud-native solution for scheduling, monitoring, and managing complex data workflows, offering better scalability, observability, and integration with GCP services compared to the legacy UC4 system.
*   **Data Transformation in BigQuery:** The core SQL logic was translated from Oracle PL/SQL to BigQuery Standard SQL. This decision leverages BigQuery's serverless, highly scalable, and cost-effective data warehousing capabilities for efficient data processing. Oracle-specific hints were removed as BigQuery handles query optimization automatically. `LISTAGG` was replaced with `STRING_AGG`.
*   **Python for Post-processing:** The custom `nawk` logic and `gzip` compression were re-implemented in a Python script. This provides flexibility, maintainability, and allows for seamless integration within the Airflow DAG using `PythonOperator`, avoiding reliance on legacy shell scripts or custom binaries.
*   **Cloud Storage as Primary Data Landing and Archiving Zone:** Cloud Storage buckets (`gs://<project_id>-apt-rabatt-export/work/` and `gs://<project_id>-apt-rabatt-export/archive/`) replace the local file system for temporary storage and long-term archiving. This offers high durability, availability, and scalability.
*   **Managed SFTP Distribution:** The `SFTPOperator` in Airflow is used for distributing the final file to the external SFTP target. An intermediate `GCSDownloadOperator` step was introduced to download the file from Cloud Storage to the Airflow worker's local filesystem before SFTP transfer, addressing the `SFTPOperator`'s requirement for a local source path.
*   **Oracle Data Source Strategy:** The design assumes that the necessary Oracle source tables will be replicated into BigQuery (e.g., via DataStream or Fivetran) into a dedicated `ORACLE_DATA` dataset. This minimizes direct dependencies on the legacy Oracle system during job execution and leverages BigQuery's performance.
*   **Configuration Management:** Airflow Variables and Connections are used to manage dynamic parameters (e.g., SFTP paths) and sensitive credentials (e.g., SFTP connection details), replacing the legacy `.var` configuration files.

## 4. Manual Steps Before Go-Live

To ensure a successful deployment and operation of the migrated `EXIS_SD_APT_RABATT` job, the following manual steps must be completed:

1.  **GCP Project Configuration:**
    *   Replace `"your-gcp-project-id"` placeholders in the DAG and scripts with the actual GCP Project ID.
2.  **BigQuery Dataset Creation:**
    *   Create the BigQuery datasets:
        *   `PROJECT_ID.ORACLE_DATA`: To house the replicated Oracle source tables.
        *   `PROJECT_ID.dwh_apt_rabatt`: For temporary staging tables created by the job.
    *   Execute `sql/ddl/bq_oracle_source_tables.sql` to create placeholder table schemas in `PROJECT_ID.ORACLE_DATA`.
3.  **Oracle Data Replication Setup:**
    *   Implement a continuous data replication solution (e.g., Google Cloud DataStream, Fivetran, or a custom CDC process) to replicate the following Oracle tables into `PROJECT_ID.ORACLE_DATA` in BigQuery:
        *   `RPT$TA_S_D1_VERTRAG` -> `RPT_TA_S_D1_VERTRAG`
        *   `RPT$TA_S_D1_DISCOUNT_RR` -> `RPT_TA_S_D1_DISCOUNT_RR`
        *   `SOF$TA_BPR_OPTIONEN` -> `SOF_TA_BPR_OPTIONEN`
        *   `SOF$VI_L_OPTIONZUORDNUNG` -> `SOF_VI_L_OPTIONZUORDNUNG`
    *   Ensure data consistency and low latency for these replicated tables.
4.  **Cloud Storage Bucket Creation:**
    *   Create the following Cloud Storage buckets (or ensure they exist) within your GCP project:
        *   `gs://<project_id>-apt-rabatt-export/work/`: For intermediate and final output files before archiving.
        *   `gs://<project_id>-apt-rabatt-export/archive/`: For long-term storage of processed files.
5.  **IAM Permissions:**
    *   Grant the Airflow Service Account (typically `service-<project-number>@cloudcomposer.gserviceaccount.com`) the necessary IAM roles:
        *   `BigQuery Data Editor` (or more granular roles for specific datasets/tables) for `PROJECT_ID.ORACLE_DATA` and `PROJECT_ID.dwh_apt_rabatt`.
        *   `Storage Object Admin` (or `Storage Object Creator` and `Storage Object Deleter`) for `gs://<project_id>-apt-rabatt-export`.
        *   If the SFTP target is on a GCE instance, ensure the Airflow worker service account has network access and potentially SSH keys configured if not using password-based SFTP.
6.  **Airflow Connections:**
    *   In the Airflow UI, create a new SFTP Connection with `Conn Id`: `sftp_apt_rabatt_conn`.
    *   Configure the connection with the appropriate `Host`, `Port`, `Username`, and `Password` (or `Key File` for SSH key authentication) for the external SFTP server.
7.  **Airflow Variables:**
    *   In the Airflow UI, create a new Airflow Variable with `Key`: `sftp_remote_path`.
    *   Set its `Value` to the absolute path on the SFTP server where the files should be uploaded (e.g., `/path/to/sftp/target/dir/`).
8.  **DAG and Script Deployment:**
    *   Upload the following files to your Cloud Composer environment's DAGs folder (or a designated subfolder, e.g., `dags/exis_sd_apt_rabatt/`):
        *   `dags/exis_sd_apt_rabatt_dag.py`
        *   `python/post_process_rabatt_data.py`
        *   `sql/rabatt_data_extraction.sql`
    *   Ensure the directory structure within the DAGs folder matches the relative paths used in the DAG (e.g., `sql/` and `python/` folders).
9.  **Scheduling:**
    *   Review and adjust the `schedule_interval` in `dags/exis_sd_apt_rabatt_dag.py` to match the original UC4 job's schedule.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration design and require further investigation or follow-up:

*   **`DW.HOLE_PFAD` and `DW.LESE_LOG` UC4 Includes:** The exact functionality of these legacy UC4 includes was not fully determined. While assumed to be generic path setup and logging, their content needs to be reviewed to ensure no critical business logic or unique environment configurations were missed during the migration to Airflow's native logging and environment management. (B4 Item)
*   **Oracle Data Replication Strategy:** The chosen method for replicating Oracle source tables to BigQuery (e.g., DataStream, Fivetran) is critical. The design assumes successful and consistent replication. Any latency, data consistency issues, or operational overhead of the chosen replication method could impact the entire job. This requires careful design, implementation, and monitoring of the replication pipeline.
*   **SFTP Target System Compatibility:** It needs to be confirmed whether the external system consuming the SFTP files can adapt to consuming directly from a Cloud Storage bucket (e.g., via signed URLs or a GCS FUSE mount). If so, the SFTP step could potentially be simplified or removed. Otherwise, the current SFTP approach using `SFTPOperator` is necessary. Coordination with the external system owner is required.
*   **`r_exis_v2` Exporter Logic:** The `r_exis_v2` script was a custom binary. While its configuration (`.var` file) provided insight into its behavior, the full extent of its functionality (beyond the `nawk` and `gzip` steps) needs to be confirmed. Detailed testing and comparison with the legacy output are crucial to ensure the Python re-implementation accurately replicates all aspects of its behavior.

## 6. Validation

Validation involves running the migrated job and verifying its output against the expected results and the legacy system's behavior.

**How to Run Tests:**

1.  **Manual Trigger:** In the Airflow UI, navigate to the `exis_sd_apt_rabatt_dag` and manually trigger a DAG run.
2.  **Scheduled Run:** Allow the DAG to run at its scheduled interval.
3.  **Parameterization (Optional):** If the DAG is parameterized for specific test scenarios (e.g., date ranges), use those parameters during manual triggers.

**What "Passing" Means:**

A successful migration is validated by the following criteria:

1.  **Airflow DAG Success:** All tasks within the `exis_sd_apt_rabatt_dag` complete successfully without errors, as indicated by green task boxes in the Airflow UI.
2.  **Data Integrity and Accuracy:**
    *   **Row Count:** The number of records in the final `.csv.gz` file should match the number of records produced by the legacy Oracle job for the same period.
    *   **Data Content:** Sample the data from the generated `.csv.gz` file and compare it with the output of the legacy job. All columns, values, and data types should match.
    *   **Delimiter:** Verify that the data within the CSV uses the expected `|` delimiter.
3.  **File Format and Content:**
    *   **Header/Footer:** The generated `.csv.gz` file, when decompressed, must contain the custom header and footer lines in the exact format specified (`X|<DESTINATION_FILE>|<FROM YYYYMMDD>|NR|V_S_Rabattreport|<SYSDATE YYYYMMDD>`).
    *   **Compression:** Confirm the file is correctly compressed using `gzip`.
4.  **File Delivery:**
    *   **GCS Work Bucket:** The compressed `.csv.gz` file should appear in `gs://<project_id>-apt-rabatt-export/work/` immediately after the `post_process_and_compress` task.
    *   **SFTP Target:** The file must be successfully transferred to the configured external SFTP server at the `sftp_remote_path`. Verify its presence and accessibility by the downstream system.
    *   **GCS Archive Bucket:** After successful SFTP transfer, the file should be moved from the `work/` bucket to `gs://<project_id>-apt-rabatt-export/archive/`.
5.  **Performance:** The job should complete within an acceptable timeframe, ideally matching or improving upon the legacy job's execution duration.
6.  **Resource Utilization:** Monitor BigQuery query costs and Cloud Composer resource usage to ensure they are within expected limits.

## 7. Rollback Procedure

In the event of critical failure, unexpected behavior, or a decision to revert, follow this rollback procedure:

1.  **Immediate Action (Disable New Job):**
    *   In the Airflow UI, toggle off the `exis_sd_apt_rabatt_dag` to prevent any further scheduled or manual runs.
    *   If any runs are currently in progress, attempt to mark them as failed or clear them to stop execution.
2.  **Re-enable Legacy Job:**
    *   Re-enable the original `EXIS_SD_APT_RABATT` UC4 job definition.
    *   Verify that the legacy job is running as expected and producing its output.
3.  **Cleanup (GCP - Optional but Recommended):**
    *   **Cloud Storage:** Delete any files generated by the Airflow DAG from `gs://<project_id>-apt-rabatt-export/work/` and `gs://<project_id>-apt-rabatt-export/archive/` for the affected run dates.
    *   **BigQuery:** Delete any temporary tables created by the Airflow DAG in `PROJECT_ID.dwh_apt_rabatt` (e.g., `rabatt_report_staging_<date>`).
    *   **Airflow:**
        *   Delete the `exis_sd_apt_rabatt_dag` from the Airflow UI (or remove the DAG file from the DAGs folder).
        *   Delete the Airflow Connection `sftp_apt_rabatt_conn`.
        *   Delete the Airflow Variable `sftp_remote_path`.
4.  **Root Cause Analysis:**
    *   Analyze the Airflow task logs, BigQuery job history, and Cloud Storage logs to identify the root cause of the failure or the reason for the rollback.
    *   Address the identified issues before attempting re-migration or re-deployment.