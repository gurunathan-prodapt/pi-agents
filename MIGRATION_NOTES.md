# MIGRATION_NOTES.md for DW.DWH_APT_EXPORT_MONATLICH_JP

## 1. Summary

The UC4 job `DW.DWH_APT_EXPORT_MONATLICH_JP`, responsible for orchestrating the monthly export of telephone system master data into compressed CSV files for distribution to a target system, has been migrated.

The migration target platform is Google Cloud Platform (GCP), leveraging:
*   **Cloud Composer (Apache Airflow)** for workflow orchestration and scheduling.
*   **BigQuery** for data processing, transformation, and preparation.
*   **Cloud Storage** for storing the exported CSV files.

The migration preserves the original functionality, monthly scheduling, and data delivery mechanism, replacing proprietary UC4 components and a custom binary (`r_exis_v2`) with cloud-native services.

## 2. Generated Artifacts

The migration produced the following artifacts:

*   **`dags/dw_dwh_apt_export_monatlich_jp_dag.py`**
    *   **Role:** This is the main Apache Airflow DAG (Directed Acyclic Graph) that orchestrates the entire monthly export process. It defines the sequence of tasks, including prerequisite checks, BigQuery data preparation, and BigQuery-to-Cloud Storage export operations. It replaces the UC4 Job Plan (`DW.DWH_APT_EXPORT_MONATLICH_JP.xml`) and the UC4 Event (`DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT.xml`).

*   **`dags/sql/dwh_exis_sd_apt_nna_data.sql`**
    *   **Role:** This SQL script, designed for BigQuery, is responsible for extracting, transforming, and formatting the "NNA Data" portion of the export. It replicates the logic previously handled by the `r_exis_v2` binary and its associated `.var` configuration files for `DW.DWH_EXIS_SD_APT_NNA_DATA`. The output of this query forms a temporary BigQuery table that is then exported to Cloud Storage.

*   **`dags/sql/dwh_exis_sd_apt_nna_voic.sql`**
    *   **Role:** Similar to the NNA data script, this BigQuery SQL script handles the extraction, transformation, and formatting of the "VOIC Data" portion of the export. It replicates the logic for `DW.DWH_EXIS_SD_APT_NNA_VOIC`, with its output forming another temporary BigQuery table for subsequent export to Cloud Storage.

## 3. Key Design Decisions

*   **Cloud Composer for Orchestration**: Cloud Composer (managed Airflow) was chosen to replace UC4's job planning and event-driven scheduling. This provides a robust, scalable, and cloud-native orchestrator with built-in dependency management, retry mechanisms, and integration with GCP services.
*   **BigQuery for Data Transformation**: The core data extraction and transformation logic, previously encapsulated in a proprietary `r_exis_v2` binary and `.var` configuration files, is re-implemented entirely in BigQuery SQL. This leverages BigQuery's serverless, scalable analytics capabilities, eliminating the need for dedicated UNIX hosts and custom binaries.
*   **BigQuery Extract to Cloud Storage**: BigQuery's native export functionality is used to generate GZIP-compressed CSV files directly to Cloud Storage. This simplifies the export process, ensures efficient compression, and provides a direct cloud-native path for file delivery, replacing manual file generation and distribution steps.
*   **Parameterization for Monthly Exports**: The monthly identifier (`YYYYMM`) is dynamically derived from Airflow's `execution_date` and passed as a parameter to BigQuery SQL queries. This replaces the UC4 `SYS_DATE` logic, ensuring flexibility and consistency for monthly data filtering.
*   **Separation of Concerns**: The Airflow DAG focuses purely on orchestration, while the BigQuery SQL files encapsulate the data transformation logic. This promotes modularity, reusability, and easier maintenance.
*   **Placeholder for External Prerequisite Checks**: Recognizing that the prerequisite UC4 jobs (`DW.BERT_STAMMDATEN_JP`, `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`) might not be migrated simultaneously, the DAG includes placeholder tasks. This design allows for future integration (e.g., via ExternalTaskSensor or custom operators interacting with UC4 APIs) without blocking the core migration.
*   **Cloud Storage as Central Landing Zone**: Cloud Storage is used as the primary destination for exported files, offering high durability, scalability, and various integration options for downstream systems.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps and configurations are required:

1.  **GCP Project Setup**:
    *   Ensure a Google Cloud Project is provisioned and active.
    *   Enable the necessary APIs: BigQuery API, Cloud Composer API, Cloud Storage API.

2.  **Cloud Composer Environment**:
    *   Provision or identify an existing Cloud Composer environment (Airflow version 2.x recommended).
    *   Ensure the Composer environment's service account has the necessary IAM roles (see step 4).

3.  **BigQuery Dataset Creation**:
    *   Create the BigQuery dataset specified by `BIGQUERY_DATASET` (e.g., `your_bigquery_dataset`) in the target GCP project. This dataset will host the temporary tables generated during the export process.

4.  **Cloud Storage Bucket Creation**:
    *   Create the Cloud Storage bucket specified by `GCS_BUCKET` (e.g., `your-gcs-export-bucket`). This bucket will store the final exported CSV.GZ files.

5.  **IAM Permissions**:
    *   **Cloud Composer Service Account**: The service account associated with your Cloud Composer environment requires the following IAM roles:
        *   `BigQuery Data Editor` (to create/write temporary tables)
        *   `BigQuery Job User` (to run BigQuery queries and extract jobs)
        *   `Storage Object Admin` (to write files to the GCS export bucket)
    *   **BigQuery Data Viewer**: Ensure the Composer service account has `BigQuery Data Viewer` access to all source tables referenced in `dags/sql/dwh_exis_sd_apt_nna_data.sql` and `dags/sql/dwh_exis_sd_apt_nna_voic.sql`.

6.  **Airflow Configuration**:
    *   **GCP Connection**: Verify that the `google_cloud_default` Airflow connection is correctly configured for your GCP project.
    *   **Airflow Variables**: It is highly recommended to replace the hardcoded `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, `GCS_BUCKET`, and `GCP_LOCATION` in `dw_dwh_apt_export_monatlich_jp_dag.py` with Airflow Variables for better management and security.
    *   **SQL Files Upload**: Ensure the `dags/sql/dwh_exis_sd_apt_nna_data.sql` and `dags/sql/dwh_exis_sd_apt_nna_voic.sql` files are uploaded to the `dags/sql/` folder within your Composer environment's DAGs bucket.

7.  **Scheduling**:
    *   The DAG is configured with `schedule_interval="@monthly"`. Set the `start_date` in the DAG to a historical date (e.g., `datetime(2023, 1, 1)`) to allow Airflow to schedule the first run correctly based on the monthly interval. Ensure `catchup=False` is maintained to prevent backfilling.

8.  **Complete SQL Logic**:
    *   **Crucially**, the `TODO` sections within `dags/sql/dwh_exis_sd_apt_nna_data.sql` and `dags/sql/dwh_exis_sd_apt_nna_voic.sql` must be completed. This involves reverse-engineering the exact data extraction, transformation, and formatting logic from the legacy `r_exis_v2` binary and `.var` files, and implementing it in BigQuery SQL. This includes identifying the correct source tables and columns.

9.  **Implement Prerequisite Checks**:
    *   The `check_prereq_dw_bert_stammdaten_jp` and `check_prereq_dw_accessp_sigma_gprs_monatlich_jp` tasks are currently `EmptyOperator` placeholders. Implement the actual logic to check the status of these external UC4 jobs. This might involve:
        *   Using an `ExternalTaskSensor` if these jobs are also migrated to Airflow.
        *   Developing a custom Airflow operator to query UC4 via an API.
        *   Polling a database table where UC4 status is logged.

10. **Target System Integration**:
    *   Finalize and implement the mechanism for the target system to retrieve the exported `.csv.gz` files from the Cloud Storage bucket. This could involve:
        *   Direct Cloud Storage access (IAM-controlled).
        *   A Cloud Storage notification triggering a downstream process.
        *   A managed SFTP service (e.g., Storage Transfer Service with SFTP connector).
        *   Signed URLs for temporary access.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps or require further follow-up (B4 items):

*   **`r_exis_v2` Logic Translation (B4)**: The most significant gap is the complete and accurate translation of the proprietary `r_exis_v2` binary's logic and its `.var` configuration files into BigQuery SQL. The current SQL files contain `TODO` placeholders. This requires detailed reverse-engineering, potentially involving SME interviews, analysis of legacy code/configuration, and comparison of sample outputs.
*   **Prerequisite Job Status Check Implementation (B4)**: The current Airflow DAG uses `EmptyOperator` for `check_prereq_dw_bert_stammdaten_jp` and `check_prereq_dw_accessp_sigma_gprs_monatlich_jp`. The actual mechanism to reliably check the completion status of these external UC4 jobs needs to be designed and implemented.
*   **Source Table Identification (B4)**: The SQL scripts use placeholder source tables (e.g., `your_source_table_for_nna_data`). The actual BigQuery source tables corresponding to the legacy data sources must be identified and referenced.
*   **Target System Integration Finalization (B4)**: While Cloud Storage is the landing zone, the precise method for the target system to consume these files needs to be confirmed and implemented. This might involve additional GCP services or custom development.
*   **Sensitive Information Handling**: If the original `.var` files contained sensitive information (e.g., credentials), these must be securely managed in GCP, ideally using Google Secret Manager, and integrated into the Airflow DAG or BigQuery processes as needed.
*   **Error Handling and Alerting**: While basic Airflow retry mechanisms are in place, a comprehensive alerting strategy (e.g., PagerDuty, email notifications for critical failures) needs to be configured in Cloud Monitoring and integrated with the DAG.

## 6. Validation

Validation of the migrated job involves a multi-faceted approach to ensure data accuracy, operational reliability, and functional equivalence with the legacy system.

**How to Run Tests:**

1.  **Development/Staging Environment Deployment**: Deploy the DAG and SQL files to a non-production Cloud Composer environment.
2.  **Manual DAG Trigger**: Manually trigger the `dw_dwh_apt_export_monatlich_jp_dag` for a specific month (e.g., a past month for which legacy data is available).
3.  **Parallel Run**: For at least one full monthly cycle, run the migrated job in parallel with the legacy UC4 job. This is critical for direct comparison.
4.  **SQL Unit Testing**: Before deploying to Airflow, thoroughly test the BigQuery SQL scripts (`dwh_exis_sd_apt_nna_data.sql`, `dwh_exis_sd_apt_nna_voic.sql`) in BigQuery directly with sample data to verify transformation logic and output schema.

**What "Passing" Means:**

A successful migration and validation are defined by the following criteria:

*   **DAG Execution Success**: The `dw_dwh_apt_export_monatlich_jp_dag` completes successfully in Cloud Composer without any task failures or retries (after initial debugging).
*   **Log Verification**: Cloud Logging shows no errors or unexpected warnings from the Airflow tasks or BigQuery jobs.
*   **File Generation**: Two `.csv.gz` files are generated in the specified Cloud Storage bucket (`gs://your-gcs-export-bucket/exports/`) with the correct naming convention (e.g., `DWHM_APT_NNA_Daten_YYYYMMDDHHMMSS.csv.gz`, `DWHM_APT_NNA_Voic_YYYYMMDDHHMMSS.csv.gz`).
*   **File Integrity**:
    *   Files are correctly GZIP compressed.
    *   Files contain a header row.
    *   Files are not empty (unless expected for the given month).
*   **Data Accuracy (Critical)**:
    *   **Row Count Comparison**: The number of rows in the exported CSV files matches the number of rows in the corresponding legacy exports for the same month.
    *   **Data Content Comparison**: A sample of records (or ideally, a full comparison using checksums/diff tools) from the exported CSVs exactly matches the data content and format of the legacy system's output files. This includes column order, data types, and specific values.
*   **Performance**: The migrated job completes within acceptable timeframes, ideally matching or improving upon the legacy job's execution duration.
*   **Target System Consumption**: The target system successfully retrieves and processes the exported files from Cloud Storage without issues.
*   **Monitoring & Alerting**: Configured Cloud Monitoring dashboards and alerts function as expected, providing visibility into job health and notifying on failures.

## 7. Rollback Procedure

In the event of critical issues or failure to meet validation criteria, the following rollback procedure should be followed:

1.  **Disable/Delete Airflow DAG**:
    *   Immediately disable the `dw_dwh_apt_export_monatlich_jp_dag` in the Cloud Composer UI to prevent further runs.
    *   If necessary, delete the DAG from the Composer environment's DAGs bucket.

2.  **Reactivate Legacy UC4 Job**:
    *   Re-enable and, if necessary, manually trigger the original `DW.DWH_APT_EXPORT_MONATLICH_JP` UC4 job in the legacy system. Ensure its schedule is restored.

3.  **Clean Up GCP Artifacts (Optional but Recommended)**:
    *   Delete any temporary BigQuery tables created by the failed Airflow runs (e.g., `temp_dwh_exis_sd_apt_nna_data_YYYYMM`).
    *   Delete any incomplete or erroneous `.csv.gz` files generated in the Cloud Storage export bucket.

4.  **Communicate**:
    *   Inform all relevant stakeholders (business users, operations, data consumers) about the rollback and the return to the legacy system.

5.  **Root Cause Analysis**:
    *   Initiate a thorough investigation into the root cause of the failure in the migrated system before attempting re-deployment.