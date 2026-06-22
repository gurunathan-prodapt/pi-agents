# MIGRATION_NOTES.md

## 1. Summary

The `DW.DWH_APT_EXPORT_MONATLICH_JP` job, originally an UC4 Job Plan, has been migrated to Google Cloud Platform (GCP). This job is responsible for exporting master data from a telephone system into compressed CSV files on a monthly basis.

The migration re-implements the functionality using:
*   **Google Cloud Composer (Airflow)** for orchestration and scheduling.
*   **Google Cloud Dataproc** for executing PySpark jobs that perform the data extraction and transformation.
*   **Google Cloud Storage (GCS)** for storing the resulting compressed CSV files.

The new Airflow DAG, `dw_dwh_apt_export_monatlich_jp`, now orchestrates two PySpark jobs (`dw_dwh_exis_sd_apt_nna_data.py` and `dw_dwh_exis_sd_apt_nna_voic.py`), which replace the original UNIX jobs (`DW.DWH_EXIS_SD_APT_NNA_DATA` and `DW.DWH_EXIS_SD_APT_NNA_VOIC`) and their underlying `r_exis_v2` script logic. The monthly scheduling and prerequisite checks for `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` are also handled within the Airflow DAG.

## 2. Generated artifacts

The migration process generated the following files:

*   **`dags/dw_dwh_apt_export_monatlich_jp.py`**
    *   **Role**: This is the main Airflow DAG file. It defines the workflow, including the monthly schedule, prerequisite checks using `ExternalTaskSensor` for `dw_bert_stammdaten_jp` and `dw_accessp_sigma_gprs_monatlich_jp` DAGs, and the submission of two PySpark jobs to Dataproc using `DataprocSubmitJobOperator`. It also includes a mechanism to prevent concurrent runs.
*   **`pyspark/dw_dwh_exis_sd_apt_nna_data.py`**
    *   **Role**: This is a PySpark application designed to replace the logic of the original `DW.DWH_EXIS_SD_APT_NNA_DATA` UNIX job. It is intended to extract and transform master data based on the `h_exis_apt_nna_daten.var` configuration. **Note**: This script is currently a placeholder and requires manual implementation of the actual data extraction and transformation logic. It generates dummy data and writes it to GCS as a compressed CSV.
*   **`pyspark/dw_dwh_exis_sd_apt_nna_voic.py`**
    *   **Role**: This is a PySpark application designed to replace the logic of the original `DW.DWH_EXIS_SD_APT_NNA_VOIC` UNIX job. It is intended to extract and transform voice master data based on the `h_exis_apt_nna_voice.var` configuration. **Note**: Similar to the `_data` script, this is currently a placeholder and requires manual implementation of the actual data extraction and transformation logic. It generates dummy data and writes it to GCS as a compressed CSV.

## 3. Key design decisions

*   **Orchestration with Airflow on Cloud Composer**: Airflow provides robust scheduling, dependency management, and monitoring capabilities, making it a natural fit for replacing UC4 job plans. Cloud Composer offers a managed Airflow environment, reducing operational overhead.
*   **Data Processing with PySpark on Dataproc**: The original `r_exis_v2` shell script, likely performing data extraction and transformation, is best re-implemented using a scalable data processing framework. PySpark on ephemeral Dataproc clusters provides flexibility, scalability, and cost-efficiency for batch processing.
*   **Cloud Storage for Output**: GCS is chosen as the target for the exported compressed CSV files due to its high durability, availability, scalability, and seamless integration with other GCP services.
*   **ExternalTaskSensor for Prerequisite Jobs**: To maintain the original UC4 dependency chain, `ExternalTaskSensor` is used to ensure that the prerequisite DAGs (`dw_bert_stammdaten_jp` and `dw_accessp_sigma_gprs_monatlich_jp`) complete successfully before this DAG proceeds. This assumes these prerequisite jobs are also migrated to Airflow.
*   **Monthly Schedule**: The Airflow DAG's `schedule` parameter is set to `0 6 1 * *` (6 AM on the 1st of every month) to replicate the monthly UC4 Event trigger.
*   **Synchronous Execution (`max_active_runs=1` and `skip_if_running`)**: The `max_active_runs=1` DAG parameter combined with a `PythonOperator` (`skip_if_running`) that checks for active runs, ensures that only one instance of the monthly export job can run at a time, mirroring the typical synchronous behavior of UC4 job plans.
*   **`on_failure_callback` for Error Handling**: An `on_failure_alarm` function is included as a placeholder for custom alerting logic (e.g., email, Slack, PagerDuty) to replace UC4's `DW.CALL_STANDARD` and `BLOCK` actions.
*   **Placeholder PySpark Scripts**: Due to the lack of detailed information about the `r_exis_v2` script and its `.var` configuration files, the generated PySpark scripts are placeholders. This was a necessary trade-off to generate a functional DAG structure, but it defers the critical data transformation logic implementation.
*   **Parameterization of PySpark Jobs**: The `month_id` and `output_path` are passed as arguments to the PySpark jobs, allowing for dynamic execution based on Airflow's context and GCS pathing.

## 4. Manual steps before go-live

Before this migrated job can go live, the following manual steps are required:

1.  **GCP Project Setup**:
    *   Ensure a GCP project is active and billing is enabled.
    *   Replace `YOUR_GCP_PROJECT_ID` in the DAG with the actual project ID.
2.  **Cloud Composer Environment**:
    *   Provision a Cloud Composer environment (if not already existing).
    *   Ensure the Airflow version is compatible with the generated DAG.
3.  **Dataproc Cluster/Templates**:
    *   Decide on a Dataproc strategy:
        *   **Ephemeral clusters**: Create a Dataproc cluster template for on-demand cluster creation.
        *   **Persistent cluster**: Create a persistent Dataproc cluster.
    *   Replace `YOUR_DATAPROC_REGION` and `YOUR_DATAPROC_CLUSTER_NAME` in the DAG with actual values.
4.  **GCS Buckets Creation**:
    *   Create a GCS bucket for Airflow DAGs (usually managed by Composer).
    *   Create a GCS bucket for storing PySpark scripts (e.g., `gs://YOUR_BUCKET_NAME/pyspark/`).
    *   Create a GCS bucket for storing the exported CSV data (e.g., `gs://YOUR_BUCKET_NAME/exports/`).
    *   Replace `YOUR_BUCKET_NAME` in the DAG with the actual bucket name.
5.  **IAM Roles and Permissions**:
    *   Grant the Cloud Composer service account (e.g., `service-<project-number>@cloudcomposer.gserviceaccount.com`) necessary roles:
        *   `Composer Worker` (for Composer operations).
        *   `Dataproc Worker` (to submit jobs to Dataproc).
        *   `Storage Object Admin` (to read/write from GCS buckets for scripts and output).
    *   Ensure the Dataproc service account has permissions to access the source database (if applicable) and write to the GCS output bucket.
6.  **Source Database Connectivity (if applicable)**:
    *   If the source data is in an external database (e.g., Oracle), configure network connectivity (VPC Peering, Cloud VPN, etc.).
    *   Ensure Dataproc clusters have the necessary JDBC drivers and network access to connect to the source database.
    *   Store database credentials securely (e.g., Secret Manager) and configure PySpark jobs to retrieve them.
7.  **Implement PySpark Transformation Logic**:
    *   **CRITICAL STEP**: Manually analyze the `r_exis_v2` script and its configuration files (`h_exis_apt_nna_daten.var`, `h_exis_apt_nna_voice.var`).
    *   Re-implement the exact data extraction queries, transformation rules, and output formatting into the placeholder PySpark scripts (`dw_dwh_exis_sd_apt_nna_data.py` and `dw_dwh_exis_sd_apt_nna_voic.py`). This will involve connecting to the source database (e.g., Oracle via JDBC) and performing DataFrame operations.
8.  **Prerequisite DAGs Deployment**:
    *   Ensure the DAGs corresponding to `DW.BERT_STAMMDATEN_JP` (`dw_bert_stammdaten_jp`) and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` (`dw_accessp_sigma_gprs_monatlich_jp`) are migrated, deployed, and functional in the same Airflow environment.
9.  **Airflow Configuration**:
    *   If any Airflow Variables or Connections are required for the PySpark jobs (e.g., database connection details), create them in the Airflow UI.
10. **Upload Artifacts**:
    *   Upload the `dw_dwh_apt_export_monatlich_jp.py` DAG file to the Composer DAGs folder.
    *   Upload the completed PySpark scripts (`dw_dwh_exis_sd_apt_nna_data.py`, `dw_dwh_exis_sd_apt_nna_voic.py`) to the designated GCS bucket (e.g., `gs://YOUR_BUCKET_NAME/pyspark/`).
11. **Enable DAG**:
    *   Once all dependencies are met and scripts are uploaded, unpause the `dw_dwh_apt_export_monatlich_jp` DAG in the Airflow UI.

## 5. Known gaps & unresolved references

The following items were identified during the migration design and remain as gaps or require further follow-up:

*   **`r_exis_v2` and `.var` files analysis**: The most significant gap is the complete understanding and re-implementation of the `r_exis_v2` shell script and its associated configuration files (`h_exis_apt_nna_daten.var`, `h_exis_apt_nna_voice.var`). The generated PySpark scripts are currently placeholders and *must* be manually developed to replicate the original data extraction and transformation logic. This requires reverse-engineering the legacy components.
*   **Source Database Details**: The specific source database (e.g., Oracle schema, table names, connection details) used by `r_exis_v2` is not explicitly defined in the UC4 XML. This information is crucial for implementing the PySpark extraction logic.
*   **File Distribution Mechanism**: The original UC4 documentation mentions "distributes it to a target system." The exact mechanism and target for this distribution are unclear. If it involves external systems (e.g., SFTP, another cloud storage), this needs to be identified and a corresponding GCP-native solution (e.g., Cloud Storage notifications, Cloud Functions, or an Airflow transfer operator) implemented.
*   **Prerequisite Jobs Migration**: The successful migration and deployment of `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` to Airflow DAGs are critical. The current DAG relies on `ExternalTaskSensor` which will fail if these DAGs are not present or not completing successfully.
*   **UC4 `ENDED_SKIPPED` Postcondition**: The exact behavior of the `ENDED_SKIPPED` postcondition in UC4 and its implications for Airflow's `trigger_rule` or branching logic need careful manual review to ensure correct translation.
*   **`DW.CALL_STANDARD` and `BLOCK` actions**: The failure actions in UC4 involving `DW.CALL_STANDARD` and `BLOCK` indicate a specific error handling and alerting mechanism. The `on_failure_alarm` callback in the Airflow DAG is a placeholder and needs to be fully implemented to replicate the original alerting and blocking behavior, potentially integrating with Cloud Logging, Monitoring, and notification services.

## 6. Validation

To validate the successful migration and operation of the `dw_dwh_apt_export_monatlich_jp` DAG, follow these steps:

1.  **Trigger the DAG**:
    *   **Manual Trigger**: In the Airflow UI, navigate to the `dw_dwh_apt_export_monatlich_jp` DAG, click on "Trigger DAG" and select "With config" (if needed, though not for this DAG). This allows immediate testing.
    *   **Scheduled Trigger**: Wait for the next scheduled run (6 AM on the 1st of the month). Ensure the prerequisite DAGs (`dw_bert_stammdaten_jp` and `dw_accessp_sigma_gprs_monatlich_jp`) have completed successfully for the corresponding execution date.
2.  **Monitor Airflow UI**:
    *   Observe the DAG run in the Airflow UI's Graph View or Grid View.
    *   Verify that all tasks (`skip_if_running`, `wait_for_bert_stammdaten_jp`, `wait_for_accessp_sigma_gprs_monatlich_jp`, `dw_dwh_exis_sd_apt_nna_data`, `dw_dwh_exis_sd_apt_nna_voic`) execute in the correct sequence and transition to a "success" state.
    *   Check task logs for any errors or warnings.
3.  **Check Dataproc Jobs**:
    *   Navigate to the Dataproc Jobs page in the GCP Console.
    *   Verify that two PySpark jobs were submitted and completed successfully, corresponding to `dw_dwh_exis_sd_apt_nna_data` and `dw_dwh_exis_sd_apt_nna_voic`.
    *   Review the job logs for any PySpark-specific errors or issues during data processing.
4.  **Verify GCS Output**:
    *   Navigate to the GCS bucket specified for exports (e.g., `gs://YOUR_BUCKET_NAME/exports/apt_nna_data/` and `gs://YOUR_BUCKET_NAME/exports/apt_nna_voic/`).
    *   Confirm that compressed CSV files (`.csv.gz`) are present, following the naming convention `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz` and `DWHM_APT_NNA_Voic_<yyyymmddhhmmss>.csv.gz`.
    *   Download and inspect a sample of the generated CSV files to ensure:
        *   The data format is correct (CSV).
        *   The data content is accurate and complete, matching the expected output from the original UC4 job (once the PySpark scripts are fully implemented).
        *   The `month_id` parameter was correctly applied.

**"Passing" means**:
*   The `dw_dwh_apt_export_monatlich_jp` DAG completes successfully in the Airflow UI.
*   Both `dw_dwh_exis_sd_apt_nna_data` and `dw_dwh_exis_sd_apt_nna_voic` Dataproc jobs complete without errors.
*   The expected compressed CSV files are generated in the correct GCS paths with the correct naming convention.
*   (Crucially, once PySpark scripts are fully implemented) The content and schema of the exported CSV files accurately reflect the data extracted and transformed by the original UC4 job.

## 7. Rollback procedure

In case of critical failure or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Pause the new Airflow DAG**:
    *   In the Airflow UI, locate the `dw_dwh_apt_export_monatlich_jp` DAG and toggle its status to "Paused". This will prevent any further scheduled or manual runs of the migrated job.
2.  **Disable/Delete the new Airflow DAG (Optional but Recommended)**:
    *   If the issue is severe and requires significant rework, consider deleting the DAG from the Composer environment to avoid accidental re-enabling.
3.  **Re-enable the original UC4 Job Plan**:
    *   In the UC4 environment, re-enable the `DW.DWH_APT_EXPORT_MONATLICH_JP` Job Plan and its associated `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` event.
    *   Verify that the original UC4 job runs successfully and produces the expected output.
4.  **Clean up GCP Resources (Optional)**:
    *   If the rollback is permanent or long-term, consider deleting the GCS output files generated by the failed Airflow runs to avoid confusion or stale data.
    *   If dedicated Dataproc clusters were used, they can be shut down.
    *   The PySpark scripts in GCS can be removed.

After a successful rollback, analyze the root cause of the failure in the migrated job, address the identified issues (especially the placeholder PySpark logic), and re-test thoroughly before attempting another go-live.