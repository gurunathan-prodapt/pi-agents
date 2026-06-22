# MIGRATION_NOTES.md: EXIS_SD_APT_NNA_VOIC

## 1. Summary

The `EXIS_SD_APT_NNA_VOIC` job, originally responsible for exporting voice-related telephone system master data from Oracle Data Warehouse (DWH) tables to a gzipped CSV file and distributing it via SFTP, has been migrated to Google Cloud Platform (GCP).

**Original Platform:**
*   **Orchestration:** UC4 job scheduler.
*   **Data Processing:** Oracle PL/SQL script (`d_exis_apt_nna_voice.sql`) configured by a `.var` file (`h_exis_apt_nna_voice.var`).
*   **Distribution:** Shell script (`r_exis_v2`) handling gzipping and SFTP transfer.

**Target Platform:**
*   **Orchestration:** Apache Airflow on Google Cloud Composer.
*   **Data Processing:** Google BigQuery (converted SQL from Oracle PL/SQL).
*   **Data Export & Distribution:** Python application leveraging Google Cloud Storage (GCS) for temporary storage and `paramiko` for SFTP transfer.

The migration aims to leverage GCP's scalability, managed services, and cost-efficiency while maintaining the original job's functionality and output format.

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`d_exis_apt_nna_voice.bq.sql`**
    *   **Role:** Contains the BigQuery Standard SQL query derived from the original Oracle PL/SQL script (`d_exis_apt_nna_voice.sql`). This script is responsible for extracting and transforming the voice-related master data from the migrated BigQuery DWH tables. It includes parameterization for the `MONATS_ID`.

*   **`dwh_exis_sd_apt_nna_voic_dag.py`**
    *   **Role:** This is the Apache Airflow DAG definition. It orchestrates the entire data export process on GCP. It defines the job's schedule, dependencies, and tasks, including:
        *   Executing the BigQuery SQL query.
        *   Exporting the query results to GCS as a gzipped CSV.
        *   Invoking the `sftp_exporter.py` script to transfer the file from GCS to the external SFTP server.
        *   Cleaning up temporary BigQuery tables.

*   **`sftp_exporter.py`**
    *   **Role:** A Python utility script designed to handle the secure transfer of files. It downloads a specified gzipped CSV file from a GCS bucket and uploads it to an external SFTP server using `paramiko`. This script encapsulates the SFTP logic previously handled by the `r_exis_v2` shell script.

*   **`raw_dwh.VI_L_MAP_FA_TARIF.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script for creating the `VI_L_MAP_FA_TARIF` table within the `raw_dwh` dataset. This table corresponds to the source Oracle `DWH$VI_L_MAP_FA_TARIF` table.

*   **`raw_dwh.BL_D_TARIF.sql`**
    *   **Role:** BigQuery DDL script for creating the `BL_D_TARIF` table within the `raw_dwh` dataset. This table corresponds to the source Oracle `BL_D_TARIF` table.

*   **`raw_dwh.VI_C_VERTRAG.sql`**
    *   **Role:** BigQuery DDL script for creating the `VI_C_VERTRAG` table within the `raw_dwh` dataset. This table corresponds to the source Oracle `DWH$VI_C_VERTRAG` table.

*   **`raw_dwh.VI_F_NNV_TVD_12_MONATE.sql`**
    *   **Role:** BigQuery DDL script for creating the `VI_F_NNV_TVD_12_MONATE` table within the `raw_dwh` dataset. This table corresponds to the source Oracle `DWH$VI_F_NNV_TVD_12_MONATE` table.

*   **`raw_dwh.VI_L_TVD_LEISTUNGSKLASSE.sql`**
    *   **Role:** BigQuery DDL script for creating the `VI_L_TVD_LEISTUNGSKLASSE` table within the `raw_dwh` dataset. This table corresponds to the source Oracle `DWH$VI_L_TVD_LEISTUNGSKLASSE` table.

## 3. Key Design Decisions

*   **Cloud-Native Architecture (GCP):** The decision to migrate to GCP was driven by the need for a scalable, resilient, and managed data platform. This aligns with broader enterprise cloud adoption strategies.
*   **Apache Airflow for Orchestration:** Airflow was chosen to replace UC4 due to its native integration with GCP services, Python-based extensibility, robust scheduling capabilities, and improved observability for data pipelines. This provides a modern, cloud-friendly orchestration layer.
*   **BigQuery for Data Processing:** BigQuery was selected as the target data warehouse for its serverless architecture, high-performance query capabilities, and cost-effectiveness for large datasets. The Oracle PL/SQL query was directly translated to BigQuery Standard SQL to minimize logic changes and ensure functional equivalence.
*   **Python for SFTP and Custom Logic:** A dedicated Python script (`sftp_exporter.py`) was developed to handle the SFTP transfer. This decision allows for:
    *   **Enhanced Security:** Integration with Google Secret Manager or Airflow Connections for secure credential storage, moving away from `.var` files.
    *   **Flexibility:** Greater control over the SFTP process, including error handling, logging, and potential pre/post-processing steps, which were previously opaque within the `r_exis_v2` shell script.
    *   **Maintainability:** Python is a widely adopted language, making the code easier to maintain and extend.
*   **Temporary BigQuery Table for Export:** The Airflow DAG first writes the query results to a temporary BigQuery table (`temp_nna_voice_export_table`). This is a best practice for BigQuery exports, as it ensures data consistency for the export operation and leverages BigQuery's optimized export functionality to GCS.
*   **Parameterized Query Execution:** The `MONATS_ID` parameter is dynamically generated from Airflow's execution date using macros (`macros.ds_format`) and passed to the BigQuery query. This ensures the job can process data for the correct period based on its schedule, mimicking the original UC4 behavior.
*   **Gzipped CSV Output:** The export to GCS explicitly specifies `compression="GZIP"` and `export_format="CSV"` to match the original output format, ensuring compatibility with the downstream target system.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the `raw_dwh` BigQuery dataset exists in your GCP project (`your-gcp-project-id`). If not, create it.
    *   `bq mk --dataset your-gcp-project-id:raw_dwh`

2.  **BigQuery Table Creation (DDLs):**
    *   Execute the provided DDL scripts (`raw_dwh.*.sql`) in BigQuery to create the necessary tables (`VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `VI_C_VERTRAG`, `VI_F_NNV_TVD_12_MONATE`, `VI_L_TVD_LEISTUNGSKLASSE`) within the `raw_dwh` dataset.
    *   **Important:** Review the DDLs and ensure the data types accurately reflect the source Oracle tables to prevent data truncation or type mismatches during ingestion.

3.  **Initial Data Ingestion:**
    *   Populate the newly created BigQuery tables (`raw_dwh.*`) with historical and ongoing data from the source Oracle DWH. This is a critical step to ensure the BigQuery tables contain the necessary data for the export job.

4.  **Google Cloud Storage (GCS) Bucket Creation:**
    *   Create the GCS bucket specified in the Airflow DAG (`your-gcs-export-bucket`) for temporary storage of the exported CSV file.
    *   `gsutil mb gs://your-gcs-export-bucket`

5.  **IAM Permissions Configuration:**
    *   **Airflow Service Account (Composer Environment):** Grant the service account associated with your Airflow environment the following roles:
        *   `BigQuery Data Editor` (for creating/dropping temporary tables and running queries)
        *   `BigQuery Job User` (for running BigQuery jobs)
        *   `Storage Object Admin` (for writing/reading objects in the GCS export bucket)
        *   `Secret Manager Secret Accessor` (if SFTP credentials are stored in Secret Manager)
    *   **BigQuery Service Account (if different):** Ensure any service account used by BigQuery operations has appropriate permissions.

6.  **SFTP Credentials Management:**
    *   **Secure Storage:** Store the SFTP server host, port, username, and password/SSH key securely. Recommended options:
        *   **Google Secret Manager:** Create secrets for each credential (e.g., `sftp-host`, `sftp-username`, `sftp-password`).
        *   **Airflow Connections:** Create an Airflow connection of type `SFTP` or `Generic` to store these details.
    *   **Update DAG:** Modify `dwh_exis_sd_apt_nna_voic_dag.py` to retrieve these credentials from Secret Manager or Airflow Connections instead of hardcoded placeholders.

7.  **Airflow Deployment:**
    *   Upload both `dwh_exis_sd_apt_nna_voic_dag.py` and `sftp_exporter.py` to your Airflow DAGs folder in the Cloud Composer environment.
    *   Ensure `paramiko` is available in your Airflow environment (e.g., via PyPI packages in Composer environment configuration).

8.  **Airflow Configuration:**
    *   Verify the `GCS_BUCKET`, `BIGQUERY_PROJECT_ID`, and `BIGQUERY_DATASET` variables in `dwh_exis_sd_apt_nna_voic_dag.py` are correctly set to your environment's values.
    *   Confirm the `schedule` for the DAG (`0 0 1 * *` for the 1st of every month at midnight UTC) is appropriate for the business requirement.

## 5. Known Gaps & Unresolved References

*   **`r_exis_v2` Shell Script Logic:** The full functionality of the original `r_exis_v2` shell script, beyond executing SQL and performing SFTP, was not fully analyzed. If it contained complex error handling, logging, or specific file manipulations (e.g., renaming, archiving) that are critical, these might need to be reverse-engineered from the source and incorporated into the `sftp_exporter.py` or the Airflow DAG.
*   **Full UC4 Workflow Context:** The provided UC4 XML was for a single job. There might be upstream or downstream dependencies within the broader UC4 workflow that are not yet understood or accounted for in this migration scope. A comprehensive analysis of the complete UC4 process flow is recommended.
*   **Data Type Mapping Verification:** While DDLs are provided, the precise mapping of Oracle data types to BigQuery data types needs to be thoroughly verified during the initial data ingestion phase to ensure no data loss, precision issues, or unexpected behavior.
*   **`MONATS_ID` Parameter Source:** The exact mechanism and source for generating the `<FROM YYYYMM>` parameter in the original UC4 job should be confirmed against the Airflow macro implementation (`macros.ds_format(ds, '%Y-%m-%d', '%Y%m')`) to guarantee identical behavior and data selection.
*   **SFTP Authentication Method:** The `sftp_exporter.py` supports both password and SSH key-based authentication. The preferred and most secure method for connecting to the external SFTP server needs to be explicitly chosen and configured (e.g., using SSH keys stored in Secret Manager).

## 6. Validation

To ensure the migrated job functions correctly, perform the following validation steps:

1.  **BigQuery Query Validation:**
    *   **Action:** Manually execute the `d_exis_apt_nna_voice.bq.sql` query in the BigQuery console. Replace `@FROM_YYYYMM` with a relevant `YYYYMM` value (e.g., `202301`).
    *   **Passing Criteria:** The query executes successfully, returns the expected number of rows, and the data in a sample of columns matches the output from the original Oracle query for the same period. The schema of the output should also match.

2.  **GCS Export Validation:**
    *   **Action:** Trigger the `dwh_exis_sd_apt_nna_voic_dag` in Airflow. Monitor the `bigquery_export_to_gcs_task`.
    *   **Passing Criteria:** The task completes successfully. A gzipped CSV file named `dwhm_apt_nna_voice_YYYYMM.csv.gz` (where `YYYYMM` corresponds to the execution month) appears in the specified GCS bucket (`your-gcs-export-bucket`). Download and inspect the file to confirm it's a valid gzipped CSV, contains the expected header, and the data is correctly formatted.

3.  **SFTP Transfer Validation:**
    *   **Action:** After the `sftp_transfer_to_external_system` task completes in Airflow, log in to the target SFTP server (using the credentials provided by the external system owner).
    *   **Passing Criteria:** The `dwhm_apt_nna_voice_YYYYMM.csv.gz` file is present in the specified remote directory (`/path/to/remote/sftp/directory`). Verify file permissions and ownership are correct.

4.  **End-to-End Data Validation:**
    *   **Action:** Compare the final gzipped CSV file on the SFTP server with the output generated by the original Oracle/UC4 job for the same period.
    *   **Passing Criteria:**
        *   Row counts are identical.
        *   File size is comparable (allowing for minor differences due to compression algorithms or data type representations).
        *   A sample of records (e.g., first 100, last 100, and random samples) matches exactly.
        *   Checksums (if applicable) of the uncompressed CSV content are identical.
        *   The file name and format (gzipped CSV) are as expected by the downstream system.

## 7. Rollback Procedure

In case of issues or failure during the go-live or post-migration, follow these steps to roll back to the original system:

1.  **Disable New Airflow DAG:**
    *   Immediately pause or delete the `dwh_exis_sd_apt_nna_voic_dag` in your Airflow environment to prevent further execution of the migrated job.

2.  **Re-enable Original UC4 Job:**
    *   Reactivate the original UC4 job (`DW.DWH_EXIS_SD_APT_NNA_VOIC.xml`) in the legacy UC4 scheduler.
    *   Ensure its schedule is re-enabled and it is configured to run as per its original operational state.

3.  **Verify Original System Functionality:**
    *   Monitor the re-enabled UC4 job to confirm it is running successfully and producing the expected output to the SFTP target.
    *   Perform a quick data validation check on the SFTP server to ensure the files generated by the UC4 job are correct.

4.  **Communicate Rollback:**
    *   Inform all relevant stakeholders (e.g., data consumers, operations teams) about the rollback and the status of the data export.

5.  **Cleanup (Optional, Post-Rollback Analysis):**
    *   Once the original system is confirmed stable, you may choose to clean up any temporary BigQuery tables or GCS files created by the failed Airflow DAG runs.
    *   Analyze the root cause of the rollback using Airflow logs, BigQuery job history, and GCS access logs to address issues before attempting re-migration.