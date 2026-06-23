# MIGRATION_NOTES.md: EXIS_SD_APT_NNA_DATA

## 1. Summary

This document details the migration of the legacy UC4 job `DW.DWH_EXIS_SD_APT_NNA_DATA` to Google Cloud Platform (GCP). The original job, responsible for exporting telephone system master data into a compressed CSV file and distributing it, has been re-platformed.

The migrated solution now runs on:
*   **Google Cloud Composer (Airflow)** for orchestration and scheduling.
*   **Google Cloud Dataproc** for executing the data processing logic.
*   **Google Cloud Storage (GCS)** for storing PySpark scripts, configuration files, and the final compressed CSV output.

## 2. Generated Artifacts

The migration process generated the following key artifacts:

*   **`dags/dw_dwh_exis_sd_apt_nna_data.py`**
    *   **Role:** This is the Airflow DAG definition file. It orchestrates the execution of the data export process. It defines the workflow, including the submission of a PySpark job to a Dataproc cluster. It handles parameters such as the job identifier, configuration paths, output paths, and the execution timestamp, passing them to the PySpark script.
*   **`pyspark/r_exis_v2.py`**
    *   **Role:** This is the PySpark application script. It replaces the core logic of the legacy `r_exis_v2` shell script. It is responsible for:
        *   Parsing command-line arguments (job identifier, config path, source path, output path, execution timestamp).
        *   Deriving the `MONAT_ID` from the Airflow execution date.
        *   (Placeholder for) Reading and parsing the configuration file (equivalent to `h_exis_apt_nna_daten.var`).
        *   (Placeholder for) Extracting "telephone system master data" from its source.
        *   Performing any necessary data transformations.
        *   Writing the final output as a compressed CSV (`.csv.gz`) to a specified GCS location.
        *   Logging execution details.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Airflow for Orchestration:** Airflow (via Cloud Composer) was chosen as the target orchestration platform due to its widespread adoption for data workflows, robust scheduling capabilities, dependency management, and native integration with GCP services. This replaces the UC4 job scheduler.
*   **Dataproc for PySpark Execution:** Dataproc was selected for executing the data processing logic. It provides a managed Apache Spark service, which is ideal for running PySpark applications. This offers scalability, cost-effectiveness (especially with serverless options or ephemeral clusters), and eliminates the need to manage underlying infrastructure, replacing the legacy UNIX host execution environment.
*   **PySpark for Transformation Logic:** The core data export and transformation logic, originally in a shell script (`r_exis_v2`), was re-implemented in PySpark. This decision leverages Spark's distributed processing capabilities for potentially large datasets and provides a more maintainable, testable, and scalable solution compared to shell scripting.
*   **GCS for Storage:** Google Cloud Storage is used as the central repository for all artifacts and data:
    *   PySpark scripts are stored in GCS for easy access by Dataproc.
    *   Configuration files (e.g., `h_exis_apt_nna_daten.var` equivalent) are stored in GCS, making them accessible to the PySpark job.
    *   The final compressed CSV output is written directly to GCS, providing a durable, scalable, and accessible staging area for downstream systems.
*   **Parameterization via Airflow and PySpark Arguments:** Critical parameters like `MONAT_ID`, job identifier, configuration paths, and output paths are passed dynamically from the Airflow DAG to the PySpark script using command-line arguments. This ensures flexibility and reusability of the PySpark script. The `MONAT_ID` is derived from Airflow's execution date (`ds`).
*   **Output File Naming Convention:** The PySpark script aims to produce a file named `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`. However, PySpark's `write.csv` operation typically creates a directory containing `part-XXXXX.csv.gz` files. The current implementation coalesces to a single partition and writes to a temporary directory.
    *   **Trade-off:** While the script logs the *intended* final filename, achieving an *exact single file name* directly from PySpark requires an additional step (e.g., a `GCSObjectRenameOperator` in Airflow or `gsutil mv` command) to move/rename the `part-XXXXX.csv.gz` file from the temporary directory to the desired final name. This was noted as a potential follow-up if strict filename adherence is required by downstream systems.
*   **Initial Retry Policy:** The Airflow DAG is configured with `retries=0`.
    *   **Trade-off:** This is a conservative approach given the unknown retry behavior of the legacy UC4 job. It will need to be reviewed and adjusted based on business requirements and the stability of the migrated solution.

## 4. Manual Steps Before Go-Live

Before the `EXIS_SD_APT_NNA_DATA` job can go live, the following manual steps and configurations are required:

1.  **GCP Project Configuration:**
    *   Update the placeholders in `dags/dw_dwh_exis_sd_apt_nna_data.py`:
        *   `GCP_PROJECT_ID`: Replace `"YOUR_GCP_PROJECT_ID"` with the actual GCP project ID.
        *   `DATAPROC_REGION`: Replace `"YOUR_DATAPROC_REGION"` with the desired GCP region for Dataproc.
        *   `DATAPROC_CLUSTER_NAME`: Replace `"YOUR_DATAPROC_CLUSTER_NAME"` with the name of the Dataproc cluster (or configure for serverless Dataproc).
        *   `GCS_BUCKET`: Replace `"YOUR_BUCKET_NAME"` with the name of the GCS bucket for scripts and output.
        *   `GCS_CONFIG_PATH`: Verify and update the exact GCS path for the configuration file.
        *   `GCS_OUTPUT_PATH`: Verify and update the exact GCS path for the output data.
2.  **IAM/Permissions:**
    *   Ensure the Airflow service account (used by Cloud Composer) has the necessary permissions:
        *   `Dataproc Editor` or `Dataproc Worker` roles to submit jobs.
        *   `Storage Object Admin` or `Storage Object Creator` roles for the GCS bucket where scripts, configs, and output data reside.
    *   Ensure the Dataproc cluster service account has:
        *   `Storage Object Viewer` for reading PySpark scripts and configuration files from GCS.
        *   `Storage Object Creator` or `Storage Object Admin` for writing output data to GCS.
        *   Permissions to access the source data system (e.g., BigQuery Data Viewer, Cloud SQL Client, etc.).
3.  **Connection Strings/Secrets:**
    *   **Source Data System:** Identify the actual source of "telephone system master data." If it's a database, API, or another system, configure the necessary connection details (e.g., host, port, credentials) for the PySpark script. These should be securely managed, potentially using Airflow Connections or Google Secret Manager. The `pyspark/r_exis_v2.py` currently has a placeholder for `source_path`.
4.  **Scheduling:**
    *   **Determine Schedule:** Manually investigate the legacy UC4 job's `EVNT_TIME` or consult with SMEs to determine the exact scheduling frequency (e.g., daily, monthly, specific days/times).
    *   **Update Airflow DAG:** Set the `SCHEDULE` variable in `dags/dw_dwh_exis_sd_apt_nna_data.py` accordingly (e.g., `"@daily"`, `timedelta(days=1)`, `None` for manual trigger).
    *   **Start Date:** Update the `start_date` in `default_args` to an appropriate historical or current date.
5.  **Configuration File Migration:**
    *   **`h_exis_apt_nna_daten.var`:** Migrate the content of this legacy configuration file to a suitable format (e.g., JSON, YAML, or a simple key-value text file) and upload it to the specified `GCS_CONFIG_PATH`. The PySpark script will need to be updated to parse this format correctly.
6.  **PySpark Script Deployment:**
    *   Upload the `pyspark/r_exis_v2.py` script to the designated GCS bucket path (e.g., `gs://YOUR_BUCKET_NAME/pyspark/r_exis_v2.py`).
7.  **Dataproc Cluster Provisioning:**
    *   Ensure a Dataproc cluster (or a serverless Dataproc environment) is provisioned and running in the specified `DATAPROC_REGION` with the name `DATAPROC_CLUSTER_NAME`.
8.  **Target System Distribution:**
    *   The design document notes that the "distribution to a target system" is TBD. This mechanism needs to be defined and implemented. This might involve additional Airflow tasks (e.g., `GCSToSFTPOperator`, `CloudFunctionOperator` triggered by GCS events) or direct ingestion by the target system from GCS.

## 5. Known Gaps & Unresolved References

The following items are known gaps or require further investigation/resolution:

*   **Missing Workflow Context (B4 Item):** The most critical gap is the lack of information regarding the legacy UC4 job's schedule (`EVNT_TIME`) and its position within a larger workflow (`JOBP`).
    *   **Action:** Manual investigation with business users or SMEs is required to determine the exact schedule, upstream dependencies, and any downstream jobs that rely on this export. This will dictate the `schedule` parameter in the Airflow DAG and potentially require `ExternalTaskSensor` or other dependency management.
*   **`r_exis_v2` Internal Logic (B4 Item):** The full internal logic of the original `r_exis_v2` executable is unknown.
    *   **Action:** This needs to be reverse-engineered from the legacy script or re-written based on detailed functional specifications from SMEs. The `pyspark/r_exis_v2.py` currently contains placeholder logic for data extraction and transformation.
*   **Source Data System Identification:** The exact source of "telephone system master data" is not explicitly defined.
    *   **Action:** Identify the source system (e.g., database, API, file system) and design the PySpark script to connect and extract data from it.
*   **Target System Distribution Mechanism:** The method for "distributing it to a target system" after the data is written to GCS is undefined.
    *   **Action:** Define and implement the post-export distribution process. This might involve additional Airflow tasks or external services.
*   **Configuration File Parsing:** The `pyspark/r_exis_v2.py` has placeholder logic for parsing `h_exis_apt_nna_daten.var`.
    *   **Action:** Implement robust parsing logic in the PySpark script based on the actual format of the migrated configuration file.
*   **Exact Output Filename:** As noted in design decisions, PySpark writes to a directory. If the downstream system requires a single file with the exact name `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`, an additional Airflow task (e.g., `GCSObjectRenameOperator`) will be needed to move/rename the `part-XXXXX.csv.gz` file.
*   **Retry Policy Review:** The current `retries=0` in the Airflow DAG is a placeholder.
    *   **Action:** Review and adjust the retry policy based on the stability of the migrated job and business requirements for fault tolerance.

## 6. Validation

Validation of the migrated job involves several steps to ensure functional equivalence and correct operation on GCP.

1.  **PySpark Script Unit/Integration Tests:**
    *   **How to run:** Develop and execute unit tests for `pyspark/r_exis_v2.py` to verify individual functions (e.g., `MONAT_ID` derivation, config parsing, transformation logic) using mock data. Integration tests should be run against a test GCS bucket and potentially a test source data system.
    *   **"Passing" means:** All unit and integration tests pass, demonstrating that the PySpark logic correctly processes data and produces the expected output format.
2.  **Airflow DAG Execution:**
    *   **How to run:**
        *   Deploy the `dags/dw_dwh_exis_sd_apt_nna_data.py` to your Cloud Composer environment.
        *   Manually trigger the DAG from the Airflow UI.
        *   Once the schedule is defined, observe scheduled runs.
    *   **"Passing" means:**
        *   The Airflow DAG completes successfully without any task failures.
        *   All tasks within the DAG (e.g., `start`, `dwh_exis_sd_apt_nna_data`) show a "success" status in the Airflow UI.
        *   Dataproc job logs (accessible via Dataproc UI or Cloud Logging) show successful completion of the PySpark job without errors.
3.  **Output Data Verification:**
    *   **How to run:**
        *   After a successful DAG run, navigate to the specified `GCS_OUTPUT_PATH` in the GCS bucket.
        *   Verify the presence of the output directory (e.g., `temp_YYYYMMDDHHMMSS`) containing the compressed CSV file (`part-XXXXX.csv.gz`).
        *   Download and inspect the content of the `csv.gz` file to ensure:
            *   The data matches the expected output from the legacy system (sample comparison).
            *   The CSV format is correct (headers, delimiters, data types).
            *   The `MONAT_ID` column (if added) contains the correct value.
            *   The file is indeed compressed.
    *   **"Passing" means:** The output file exists in GCS, its content is accurate and complete, and its format matches the requirements of the downstream system.
4.  **Logging and Monitoring:**
    *   **How to run:** Review Airflow task logs and Dataproc driver/executor logs in Cloud Logging for any warnings, errors, or unexpected behavior.
    *   **"Passing" means:** Logs indicate normal operation, without critical errors or exceptions.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Pause/Delete New Airflow DAG:**
    *   Immediately pause or delete the `dw_dwh_exis_sd_apt_nna_data` DAG in the Airflow UI to prevent further executions of the migrated job.
2.  **Re-enable Legacy UC4 Job:**
    *   Re-enable the original `DW.DWH_EXIS_SD_APT_NNA_DATA` UC4 job on the legacy platform. Ensure its schedule and dependencies are restored to their pre-migration state.
3.  **Clean Up Partial Output (If Necessary):**
    *   If the migrated job produced any erroneous or partial output in GCS that could interfere with downstream systems, manually delete or move these files from the `GCS_OUTPUT_PATH`.
4.  **Investigate and Rectify:**
    *   Analyze the root cause of the failure using Airflow logs, Dataproc logs, and any monitoring alerts.
    *   Address the identified issues in the Airflow DAG, PySpark script, or GCP infrastructure.
    *   Once the issues are resolved and thoroughly tested in a non-production environment, the migration can be re-attempted.