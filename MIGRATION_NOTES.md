# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `DW.DWH_APT_EXPORT_MONATLICH_JP` job. This job is a monthly export process that extracts telephone system master data, compresses it into CSV files, and distributes it to a target system. Originally orchestrated by Automic (UC4) with two parallel UNIX jobs for data extraction, the process has been re-platformed to Google Cloud Platform (GCP).

The migration targets the following GCP services:
*   **Orchestration:** Cloud Composer (managed Apache Airflow) for scheduling and workflow management.
*   **Data Processing:** PySpark applications running on Dataproc (managed Spark) for data extraction, transformation, and loading.
*   **Data Storage:** Cloud Storage (GCS) for storing the exported compressed CSV files.

## 2. Generated Artifacts

The migration produced the following files:

*   **`dags/dw_dwh_run_apt_export_monatlich_jp_evt.py`**
    *   **Role:** This Airflow DAG replaces the UC4 `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` Event object. It is responsible for scheduling the monthly export, checking for prerequisite job plan completions (`DW.BERT_STAMMDATEN_JP`, `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`), and triggering the main data export DAG (`dw_dwh_apt_export_monatlich_jp`). It also implements the `SYNCREF Else=Skip` logic for concurrency.
*   **`dags/dw_dwh_apt_export_monatlich_jp.py`**
    *   **Role:** This Airflow DAG replaces the UC4 `DW.DWH_APT_EXPORT_MONATLICH_JP` Job Plan. It orchestrates the parallel execution of two PySpark applications (`nna_data_exporter.py` and `nna_voice_exporter.py`) on Dataproc to perform the actual data extraction and export. It enforces `max_active_runs=1` to mimic the `SYNCREF Else=Wait` behavior.
*   **`pyspark_apps/nna_data_exporter.py`**
    *   **Role:** This PySpark application replaces the UNIX job `DW.DWH_EXIS_SD_APT_NNA_DATA`. It connects to the source `EXT:DATABASE` (Oracle), extracts "NNA Data" based on the `MONAT_ID` parameter, applies any necessary transformations, and writes the data as a compressed CSV file to a specified GCS bucket.
*   **`pyspark_apps/nna_voice_exporter.py`**
    *   **Role:** This PySpark application replaces the UNIX job `DW.DWH_EXIS_SD_APT_NNA_VOIC`. Similar to `nna_data_exporter.py`, it connects to the source `EXT:DATABASE` (Oracle), extracts "NNA Voice Data" based on the `MONAT_ID` parameter, transforms it, and writes it as a compressed CSV file to a specified GCS bucket.
*   **`docs/pyspark_config_note.md`**
    *   **Role:** A supplementary documentation file explaining the current placeholder status of PySpark configurations (JDBC details, SQL queries) and recommending future best practices for managing these configurations securely and externally.

## 3. Key Design Decisions

*   **Cloud Composer for Orchestration:** Apache Airflow, managed by Cloud Composer, was chosen to replace UC4's scheduling and orchestration capabilities. This provides a Python-native, scalable, and cloud-managed solution that integrates well with other GCP services.
*   **Dataproc with PySpark for Data Processing:** The existing UNIX shell scripts and custom `r_exis_v2` executable were re-implemented as PySpark applications running on Dataproc. This decision was driven by:
    *   **Scalability:** Dataproc offers managed, scalable Spark clusters capable of handling large data volumes.
    *   **Maintainability:** PySpark provides a robust, Python-based framework for ETL, improving code readability and maintainability compared to complex shell scripts.
    *   **GCP Integration:** Seamless integration with GCS for input/output and other GCP services.
*   **Cloud Storage for Exported Files:** GCS replaces the local filesystem and distribution mechanism for the exported CSV files. GCS offers high durability, availability, scalability, and cost-effectiveness, making files easily accessible for downstream systems.
*   **Mapping UC4 Concurrency to Airflow:**
    *   The UC4 Job Plan's `SYNCREF Else=Wait` was mapped to Airflow's `max_active_runs=1` on the `dw_dwh_apt_export_monatlich_jp` DAG, ensuring only one instance of the export process runs at a time.
    *   The UC4 Event's `SYNCREF Else=Skip` was implemented using a custom `PythonOperator` (`guard_concurrency`) in `dw_dwh_run_apt_export_monatlich_jp_evt.py`. This checks for active DAG runs and raises `AirflowSkipException` if another instance is already running, mimicking the "skip" behavior.
*   **Parameterization via Airflow `conf` and PySpark `argparse`:** UC4 variables like `&MONAT_ID` are passed from the triggering DAG (`dw_dwh_run_apt_export_monatlich_jp_evt`) to the triggered DAG (`dw_dwh_apt_export_monatlich_jp`) via the `conf` dictionary of `TriggerDagRunOperator`. These parameters are then passed to the PySpark applications using command-line arguments parsed by `argparse`, providing a flexible and standard way to manage dynamic inputs.
*   **JDBC for Oracle Connectivity:** PySpark's JDBC connector is used to establish connections to the source `EXT:DATABASE` (Oracle). This is a standard and efficient method for Spark to interact with relational databases.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the environment and deploy the migrated job:

1.  **GCP Project Setup:** Ensure a Google Cloud Project is active and billing is enabled.
2.  **Cloud Composer Environment:**
    *   Provision or identify an existing Cloud Composer environment (Airflow version 2.x recommended).
    *   Note the GCS bucket associated with the Composer environment for DAG deployment.
3.  **Dataproc Cluster:**
    *   Create a Dataproc cluster in the specified `REGION` (e.g., `your-dataproc-cluster-name`). Ensure it has network connectivity to the source Oracle database.
    *   Alternatively, configure Dataproc Serverless for Spark if preferred, adjusting `DataprocSubmitJobOperator` accordingly.
4.  **Cloud Storage Buckets:**
    *   Create a GCS bucket for PySpark application code (e.g., `gs://your-pyspark-code-bucket`). This is referenced as `GCS_PYSPARK_CODE_BUCKET` in the DAG.
    *   Create a GCS bucket for the exported CSV files (e.g., `gs://your-gcs-export-bucket`). This is referenced as `GCS_OUTPUT_BUCKET` in the DAG.
5.  **IAM Permissions:**
    *   **Composer Service Account:** Grant the Composer service account (e.g., `service-<project-number>@cloudcomposer.gserviceaccount.com`) the following roles:
        *   `Dataproc Editor` (or more granular roles like `Dataproc Worker` and `Dataproc Viewer`) to submit and monitor Dataproc jobs.
        *   `Storage Object Admin` (or `Storage Object Creator` and `Storage Object Viewer`) on the `GCS_PYSPARK_CODE_BUCKET` and `GCS_OUTPUT_BUCKET`.
    *   **Dataproc Service Account:** Grant the Dataproc cluster's service account (e.g., `your-dataproc-cluster-sa@your-gcp-project-id.iam.gserviceaccount.com`) the following roles:
        *   Permissions to connect to the source Oracle database (e.g., via a VPC network and firewall rules).
        *   `Secret Manager Secret Accessor` if using Secret Manager for database credentials.
        *   `Storage Object Admin` (or `Storage Object Creator` and `Storage Object Viewer`) on the `GCS_OUTPUT_BUCKET`.
6.  **Oracle JDBC Driver:**
    *   Download the appropriate Oracle JDBC driver JAR file (e.g., `ojdbc8.jar`).
    *   Upload this JAR file to a GCS bucket (e.g., `gs://your-gcs-bucket/drivers/ojdbc8.jar`).
    *   Update the `jar_file_uris` parameter in the `DataprocSubmitJobOperator` tasks within `dw_dwh_apt_export_monatlich_jp.py` to reference this JAR.
7.  **Database Connection Details & SQL Queries:**
    *   **Crucial Step:** Reverse-engineer the exact SQL queries, transformation logic, and Oracle connection details (JDBC URL, username, password, schema, table names) from the original `r_exis_v2` executable and `.var` files (`h_exis_apt_nna_daten.var`, `h_exis_apt_nna_voice.var`, `d_exis_apt_nna_daten.sql`, `d_exis_apt_nna_voice.sql`).
    *   **Secure Credentials:** Store the Oracle database username and password in Google Secret Manager.
    *   **Update PySpark Apps:** Modify `pyspark_apps/nna_data_exporter.py` and `pyspark_apps/nna_voice_exporter.py` to:
        *   Replace placeholder `JDBC_URL`, `JDBC_USER`, `JDBC_PASSWORD` with actual values, preferably by retrieving them from Secret Manager at runtime.
        *   Replace placeholder `ORACLE_TABLE_OR_QUERY` with the actual SQL queries and `WHERE` clauses.
        *   Implement the specific transformation logic identified from `r_exis_v2`.
8.  **Update DAG Configuration:**
    *   Edit `dags/dw_dwh_apt_export_monatlich_jp.py` to replace `PROJECT_ID`, `REGION`, `CLUSTER_NAME`, `GCS_PYSPARK_CODE_BUCKET`, and `GCS_OUTPUT_BUCKET` with your actual GCP resource names.
9.  **Upload PySpark Applications:** Upload the modified `nna_data_exporter.py` and `nna_voice_exporter.py` files to the `GCS_PYSPARK_CODE_BUCKET`.
10. **Upload Airflow DAGs:** Upload `dw_dwh_run_apt_export_monatlich_jp_evt.py` and `dw_dwh_apt_export_monatlich_jp.py` to the DAGs folder of your Cloud Composer environment (usually `gs://<your-composer-bucket>/dags/`).
11. **Prerequisite DAGs:** Ensure that the DAGs corresponding to `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` are either migrated to Airflow (and their `dag_id`s are known for `ExternalTaskSensor` or similar checks) or a custom mechanism is in place to query their status if they remain external. The `check_prerequisites_function` in `dw_dwh_run_apt_export_monatlich_jp_evt.py` needs to be fully implemented.
12. **Schedule Validation:** The `schedule_interval` for `dw_dwh_run_apt_export_monatlich_jp_evt.py` is currently set to daily (`0 7 * * *`). This needs to be manually validated and adjusted to a monthly schedule if the original `TimePeriodTT=0720` implies a specific monthly day/time, not just daily polling.

## 5. Known Gaps & Unresolved References

The following items require further investigation or are currently placeholders:

*   **`r_exis_v2` Executable Logic:** The precise SQL queries, data filtering, and transformation logic embedded within the `r_exis_v2` binary and its `.var` configuration files are unknown. This is the most critical gap and requires manual reverse engineering to accurately re-implement in PySpark.
*   **`.dw_init` Script:** The content and purpose of the `. $HOME/.dw_init` script executed by the original UNIX jobs are unknown. It might contain critical environment setups or utility functions that need to be replicated or replaced in the GCP environment.
*   **`EXT:DATABASE` Specifics:** While identified as Oracle, the exact schema, table names, and full connection details for `EXT:DATABASE` are not yet specified. These are crucial for configuring the PySpark jobs.
*   **Monthly Schedule Clarification:** The UC4 `EVNT_TIME` object's `TimePeriodTT=0720` combined with the "monthly" description needs precise clarification to set the correct Airflow cron schedule. The current DAG is set to daily.
*   **Prerequisite Check Implementation:** The `check_prerequisites_function` in `dw_dwh_run_apt_export_monatlich_jp_evt.py` is a placeholder. It needs to be fully implemented, potentially using `ExternalTaskSensor` if the prerequisite jobs are also Airflow DAGs, or a custom sensor/API call if they remain external.
*   **UC4 External Object Activation/Cancellation:** The `ACTIVATE_UC_OBJECT` and `CANCEL_UC_OBJECT` calls in UC4 have been mapped to `TriggerDagRunOperator` and a logging `PythonOperator` respectively. The full implications of `CANCEL_UC_OBJECT` (e.g., stopping a running process) might require more sophisticated Airflow API interaction if a true "cancellation" is needed beyond just logging.
*   **Postcondition Actions:** The postcondition actions in the main UC4 Job Plan (`EXECUTE OBJECT DW.CALL_STANDARD`, `BLOCK`) need analysis. `DW.CALL_STANDARD` could be an alert or a generic handler, while `BLOCK` implies terminal failure. These should be mapped to Airflow callbacks, alerts (e.g., PagerDuty, Slack), or error handling mechanisms.
*   **`DW.HOLE_PFAD` and `DW.LESE_LOG`:** These UC4 include objects, likely utility scripts for path management or logging, need to be analyzed and re-implemented as Python helper functions or integrated into Cloud Logging.

## 6. Validation

Validation of the migrated job involves several stages:

1.  **Unit Testing (PySpark Applications):**
    *   **Method:** Create mock dataframes for Oracle input and verify the transformation logic in `nna_data_exporter.py` and `nna_voice_exporter.py` produces the expected output structure and content.
    *   **Passing Criteria:** All unit tests pass, ensuring the core data processing logic is correct.
2.  **Integration Testing (Airflow DAGs & Dataproc):**
    *   **Triggering:**
        *   Manually trigger the `dw_dwh_run_apt_export_monatlich_jp_evt` DAG in Airflow.
        *   Verify the `guard_concurrency` task correctly skips subsequent runs if one is active.
        *   Verify the `check_prerequisites` task (once implemented) correctly evaluates prerequisite conditions.
        *   Verify `dw_dwh_apt_export_monatlich_jp` is triggered by the event DAG.
        *   Manually trigger `dw_dwh_apt_export_monatlich_jp` directly to test the Dataproc jobs in isolation.
    *   **Dataproc Job Execution:**
        *   Monitor Dataproc job logs in Cloud Logging for any errors or warnings.
        *   Verify the Dataproc cluster starts (if ephemeral) or the job runs on the specified cluster.
    *   **Data Source Connectivity:** Confirm that the PySpark jobs successfully connect to the source Oracle database.
    *   **Data Extraction & Transformation:** Review Dataproc job logs to confirm data is being read and processed.
    *   **Output Verification:**
        *   Navigate to the `GCS_OUTPUT_BUCKET` in Cloud Storage.
        *   Verify that two compressed CSV files (e.g., `DWHM_APT_NNA_Daten_<timestamp>.csv.gz` and `DWHM_APT_NNA_VOIC_<timestamp>.csv.gz`) are created in the expected `exports/` prefix.
        *   Check the file sizes to ensure they are not empty and are within expected ranges.
        *   Download and decompress a sample file. Compare its structure and a subset of its data against the original export files or directly against the source Oracle data.
    *   **Parameter Passing:** Verify that the `monat_id` parameter is correctly passed from the triggering DAG to the PySpark applications.
    *   **Concurrency Handling:** Test `max_active_runs=1` on `dw_dwh_apt_export_monatlich_jp` by attempting to trigger it multiple times concurrently. Only one should run.
    *   **Schedule:** Once the monthly schedule is confirmed and set, observe a scheduled run to ensure it triggers at the correct time.
    *   **Error Handling:** Test scenarios like database connection failures or invalid data to ensure proper error logging and DAG failure.
3.  **"Passing" Criteria:**
    *   All tasks within both Airflow DAGs complete successfully (green status).
    *   No errors or critical warnings in Dataproc job logs.
    *   Two compressed CSV files are generated in the designated GCS bucket for each successful run.
    *   The naming convention of the output files matches the specification.
    *   The content of the exported CSV files is accurate and complete when compared to the source data or previous exports.
    *   The job adheres to the defined monthly schedule and concurrency rules.

## 7. Rollback Procedure

In the event of a critical failure or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action:**
    *   **Disable Airflow DAGs:** Pause both `dw_dwh_run_apt_export_monatlich_jp_evt` and `dw_dwh_apt_export_monatlich_jp` DAGs in the Airflow UI to prevent further execution of the migrated job.
2.  **Revert to Legacy System:**
    *   **Re-enable UC4 Jobs:** Re-activate the original UC4 Job Plan `DW.DWH_APT_EXPORT_MONATLICH_JP` and its associated Event `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` in the Automic environment.
    *   **Verify Legacy Operation:** Monitor the re-enabled UC4 jobs to ensure they are running as expected and producing correct outputs.
3.  **GCP Cleanup (if necessary):**
    *   **Delete Output Files:** If any corrupted or incomplete files were generated in the `GCS_OUTPUT_BUCKET` by the failed GCP job, delete them to avoid confusion or downstream issues.
4.  **Investigation:**
    *   Analyze Cloud Composer logs, Dataproc job logs, and PySpark application logs to identify the root cause of the failure.
    *   Review any changes made to the PySpark code, DAGs, or infrastructure configuration.
5.  **Data Consistency:**
    *   Assess the impact of the failure on downstream systems. If data was partially exported or corrupted, coordinate with consuming systems to determine if re-processing or manual intervention is required.
    *   Ensure that the re-enabled legacy system can provide the necessary data for any missed periods.