# MIGRATION_NOTES.md for EXIS_SD_APT_NNA_VOIC

## 1. Summary

The `EXIS_SD_APT_NNA_VOIC` job, originally an ETL workflow orchestrated by a UC4 JOBS_UNIX object, has been migrated. Its purpose is to export telephone system master data (voice-related data) from an Oracle Data Warehouse (DWH) to a gzipped CSV file, which is then distributed via SFTP.

The migration re-implements this functionality on Google Cloud Platform. The new target platform utilizes:
*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Storage & Processing:** Google BigQuery, replacing the Oracle DWH for the exported data.
*   **File Storage:** Google Cloud Storage (GCS) for temporary staging of exported CSV files.
*   **External Data Transfer:** A Python-based SFTP client within Airflow, replacing the legacy SFTP process.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`dags/dw_dwh_exis_sd_apt_nna_voic.py`**
    *   **Role:** This is the main Apache Airflow DAG (Directed Acyclic Graph) that orchestrates the entire data export process. It defines the sequence of tasks:
        1.  Executing the core SQL logic in BigQuery to extract and transform data into the `DWHM_APT_NNA_Voice` table.
        2.  Exporting the data from the BigQuery table to a gzipped CSV file in a specified GCS bucket.
        3.  A Python task that downloads the GCS file, applies the custom header/trailer logic (mimicking the original `nawk` functionality), re-compresses the file, and then securely transfers it to the external SFTP target.

*   **`sql/ddl/DWHM_APT_NNA_Voice.sql`**
    *   **Role:** This SQL DDL script defines the schema for the target BigQuery table `DWHM_APT_NNA_Voice`. This table will hold the final transformed voice master data before export.

*   **`sql/ddl/DWH_VI_L_MAP_FA_TARIF.sql`**
    *   **Role:** This SQL DDL script defines the schema for the BigQuery representation of the legacy Oracle table `DWH$VI_L_MAP_FA_TARIF`. This is one of the source tables for the export.

*   **`sql/ddl/BL_D_TARIF.sql`**
    *   **Role:** This SQL DDL script defines the schema for the BigQuery representation of the legacy Oracle table `BL_D_TARIF`. This is one of the source tables for the export.

*   **`sql/ddl/DWH_VI_C_VERTRAG.sql`**
    *   **Role:** This SQL DDL script defines the schema for the BigQuery representation of the legacy Oracle table `DWH$VI_C_VERTRAG`. This is one of the source tables for the export.

*   **`sql/ddl/DWH_VI_F_NNV_TVD_12_MONATE.sql`**
    *   **Role:** This SQL DDL script defines the schema for the BigQuery representation of the legacy Oracle table `DWH$VI_F_NNV_TVD_12_MONATE`. This is one of the source tables for the export.

*   **`sql/ddl/DWH_VI_L_TVD_LEISTUNGSKLASSE.sql`**
    *   **Role:** This SQL DDL script defines the schema for the BigQuery representation of the legacy Oracle table `DWH$VI_L_TVD_LEISTUNGSKLASSE`. This is one of the source tables for the export.

## 3. Key design decisions

*   **Orchestration Shift to Airflow:** Apache Airflow on Cloud Composer was chosen to replace UC4 for its cloud-native capabilities, Python-based extensibility, and robust scheduling features. This aligns with the overall platform modernization strategy.
*   **BigQuery as Data Warehouse:** Google BigQuery is used as the primary data processing engine and storage for both source data (migrated from Oracle DWH) and the intermediate export table. This leverages BigQuery's scalability, performance, and cost-effectiveness for analytical workloads.
*   **Consolidated Logic in Airflow DAG:** The original job's configuration (`.var` file) and SQL script (`.sql` file) were retired. Their logic has been consolidated directly into the Airflow DAG. The core SQL query is embedded within a `BigQueryExecuteQueryOperator`, and the post-processing (`nawk`-like header/trailer addition and SFTP distribution) is handled by a `PythonOperator`. This simplifies deployment and maintenance by having a single point of control for the workflow.
*   **Python for Post-processing and SFTP:** Instead of attempting to translate `nawk` commands directly, a `PythonOperator` was used to implement the header/trailer logic. This provides greater flexibility and maintainability within the Airflow ecosystem. Similarly, SFTP distribution is handled via the `paramiko` Python library, offering programmatic control and better integration with cloud services for credential management.
*   **Dynamic `MONATS_ID` Parameterization:** The `MONATS_ID` filter in the BigQuery SQL query is dynamically set using Airflow's `ds_nodash` macro, which represents the execution date of the DAG in `YYYYMMDD` format. This ensures the job processes data for the month corresponding to its scheduled run, aligning with typical monthly reporting cycles.
*   **Staging in GCS:** Data is first exported from BigQuery to a gzipped CSV in a GCS bucket. This provides a reliable, scalable, and cost-effective staging area before the final SFTP transfer, decoupling the BigQuery export from the SFTP operation.

**Notable Trade-offs:**

*   **`paramiko` dependency:** The SFTP step requires the `paramiko` Python library to be installed in the Airflow environment. This adds a dependency that needs to be managed during environment setup.
*   **SFTP Credential Management:** While `paramiko` enables SFTP, the secure management of SFTP host, port, username, and password/keys is critical and requires careful implementation, ideally using Google Secret Manager, which is currently represented by placeholders in the generated code.
*   **Loss of direct `.var` and `.sql` files:** While consolidating logic into the DAG simplifies deployment, it means the original, separate configuration and SQL files are no longer directly present. This is generally a positive trade-off for cloud-native solutions but might require a shift in how developers interact with the logic.

## 4. Manual steps before go-live

Before the `EXIS_SD_APT_NNA_VOIC` job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset and Table Creation:**
    *   Ensure the target BigQuery dataset (`your_dataset` in `your_project`) exists.
    *   Execute the DDL scripts for all source tables (`DWH_VI_L_MAP_FA_TARIF.sql`, `BL_D_TARIF.sql`, `DWH_VI_C_VERTRAG.sql`, `DWH_VI_F_NNV_TVD_12_MONATE.sql`, `DWH_VI_L_TVD_LEISTUNGSKLASSE.sql`) to create the necessary BigQuery tables. These tables must be populated with the migrated data from the Oracle DWH.
    *   The target table `DWHM_APT_NNA_Voice` will be created by the DAG itself (`CREATE OR REPLACE TABLE`), but its DDL (`DWHM_APT_NNA_Voice.sql`) provides the schema definition for reference.

2.  **IAM/Permissions:**
    *   The Airflow service account (associated with your Cloud Composer environment) must have the following IAM roles:
        *   `BigQuery Data Editor` (or equivalent) for `your_project.your_dataset` to create/write to `DWHM_APT_NNA_Voice` and read from the source tables.
        *   `Storage Object Admin` (or equivalent) for `your-gcs-bucket` to write the exported CSV and read it back for post-processing.
        *   If using Google Secret Manager for SFTP credentials, `Secret Manager Secret Accessor` for the relevant secrets.

3.  **Connection Strings/Airflow Connections:**
    *   Ensure the `google_cloud_default` Airflow connection is properly configured and points to your GCP project. This is typically set up by default in Cloud Composer.

4.  **Secrets Management (SFTP Credentials):**
    *   The generated DAG uses environment variables (`SFTP_HOST`, `SFTP_PORT`, `SFTP_USERNAME`, `SFTP_PASSWORD`) as placeholders for SFTP credentials. **This is not secure for production.**
    *   **Action Required:** Implement a secure method for storing and accessing these credentials. Recommended approaches include:
        *   **Google Secret Manager:** Store SFTP credentials in Secret Manager and modify the DAG to retrieve them at runtime.
        *   **Airflow Connections:** Create a generic SFTP connection in Airflow and use the `SFTPOperator` (if applicable) or retrieve credentials from the connection object in the Python task.

5.  **Airflow Environment Configuration:**
    *   The `add_header_trailer_and_sftp` Python task relies on the `paramiko` library for SFTP. This library must be installed in your Cloud Composer environment.
    *   **Action Required:** Add `paramiko` to your Cloud Composer environment's PyPI packages.

6.  **GCS Bucket Configuration:**
    *   Ensure the GCS bucket specified by `GCS_BUCKET` (`your-gcs-bucket`) exists and is accessible by the Airflow service account.

7.  **Scheduling:**
    *   The DAG is configured with `schedule_interval="@monthly"`. Confirm this schedule aligns with the original UC4 job's execution frequency. If a specific day of the month or a different frequency is required, adjust the `schedule_interval` accordingly.

## 5. Known gaps & unresolved references

*   **SFTP Credential Security:** As noted in section 4, the SFTP credentials (`SFTP_HOST`, `SFTP_PORT`, `SFTP_USERNAME`, `SFTP_PASSWORD`) are currently hardcoded placeholders or rely on environment variables. This is a critical security gap that **must be addressed** before production deployment, preferably by integrating with Google Secret Manager or Airflow Connections.
*   **Project/Dataset/Bucket Placeholders:** The generated code uses `your_project`, `your_dataset`, and `your-gcs-bucket` as placeholders. These must be replaced with the actual GCP project ID, BigQuery dataset ID, and GCS bucket name.
*   **`MONATS_ID` Parameterization:** The current implementation uses `ds_nodash[:6]` (YYYYMM of the DAG's execution date) for `MONATS_ID`. While this aligns with a monthly schedule for current data, if the original job allowed for arbitrary historical month processing via a parameter, this functionality would need to be explicitly added to the Airflow DAG (e.g., via DAG run configuration or Airflow variables).
*   **Error Handling for SFTP:** While basic exception handling is present for SFTP, robust retry mechanisms and detailed logging for SFTP failures might need further refinement depending on the criticality of the external system.
*   **External SFTP Server Whitelisting:** Ensure that the IP addresses of your Cloud Composer environment's workers are whitelisted on the external SFTP server to allow connections.

## 6. Validation

To validate the migrated job:

1.  **Trigger the Airflow DAG:**
    *   Upload `dags/dw_dwh_exis_sd_apt_nna_voic.py` to your Cloud Composer environment's DAGs folder.
    *   Unpause the DAG in the Airflow UI.
    *   Manually trigger a DAG run from the Airflow UI.

2.  **Monitor Task Execution:**
    *   Observe the progress of the `process_voice_export`, `export_to_gcs`, and `add_header_trailer_and_sftp_task` tasks in the Airflow UI. Check task logs for any errors or warnings.

3.  **What "passing" means:**
    *   **All tasks in the DAG complete successfully** without errors.
    *   **BigQuery Table Verification:**
        *   The BigQuery table `your_project.your_dataset.DWHM_APT_NNA_Voice` is created or updated.
        *   Query the table to confirm data presence and correctness, especially the `MONATS_ID` filter and column transformations.
    *   **GCS File Verification:**
        *   A gzipped CSV file named `DWHM_APT_NNA_Voice_YYYYMMDD.csv.gz` (where YYYYMMDD matches the DAG run date) is present in `gs://your-gcs-bucket/exis_sd_apt_nna_voic/`.
    *   **SFTP Target Verification:**
        *   The gzipped CSV file is successfully transferred to the configured external SFTP server at `SFTP_REMOTE_PATH`.
        *   **Content Validation:** Download the file from the SFTP server, decompress it, and verify:
            *   The presence and correct format of the header and trailer lines.
            *   The `NR` (record count) in the trailer matches the actual number of data rows.
            *   The data content (number of rows, column values) matches the expected output from the original Oracle job. A sample comparison with a historical run of the original job is highly recommended.

## 7. Rollback procedure

In case of issues with the migrated job, the following rollback procedure can be executed:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, pause the `dw_dwh_exis_sd_apt_nna_voic` DAG to prevent further runs.
    *   (Optional) Delete the DAG file from the Cloud Composer DAGs folder.

2.  **Re-enable Original UC4 Job:**
    *   Re-activate or re-enable the original `EXIS_SD_APT_NNA_VOIC` UC4 job in the legacy environment.
    *   Verify that the original job can run successfully and produce its output as expected.

3.  **Clean Up (Optional but Recommended):**
    *   Delete the `DWHM_APT_NNA_Voice` table in BigQuery if it was created by the new job and is not needed for debugging or historical reference.
    *   Delete the generated gzipped CSV files from the GCS bucket (`gs://your-gcs-bucket/exis_sd_apt_nna_voic/`) to avoid accumulating unnecessary data.