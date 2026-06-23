# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy UC4 job `DW.BERT_AUSD_BP_TA_BCP_ICCID` to a Google Cloud Platform (GCP) native solution.

The original UC4 job, responsible for "Aufbereitung der instantiierten Basisprodukte" (preparation of instantiated base products) related to ICCID, was an Automic Unix job executing a ksh script (`r_ausd_bp_ta_bcp_iccid.ksh`).

The job has been migrated to:
*   **Orchestration**: An Airflow Directed Acyclic Graph (DAG) named `dw_bert_ausd_bp_ta_bcp_iccid`.
*   **Execution**: A PySpark script (`r_ausd_bp_ta_bcp_iccid.py`) designed to run on a Google Cloud Dataproc cluster.
*   **Storage**: Google Cloud Storage (GCS) for hosting the PySpark script and potentially for intermediate/output data.

This migration falls under the `semi_auto` automation bucket due to the necessity of manually analyzing and translating the core business logic from the original ksh script into PySpark.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dags/dw_bert_ausd_bp_ta_bcp_iccid.py`**
    *   **Role**: This is the Airflow DAG definition file. It orchestrates the execution of the data processing job. Specifically, it defines a single task that uses the `DataprocSubmitJobOperator` to launch the PySpark script on a specified Dataproc cluster. It includes placeholders for GCP project ID, region, Dataproc cluster name, and GCS bucket.
*   **`pyspark_scripts/r_ausd_bp_ta_bcp_iccid.py`**
    *   **Role**: This is the PySpark script intended to replace the functionality of the original `r_ausd_bp_ta_bcp_iccid.ksh` shell script. It initializes a SparkSession and includes a `TODO` section where the actual business logic, data transformations, and I/O operations from the ksh script must be implemented. It incorporates basic logging using Python's `logging` module.

## 3. Key Design Decisions

The following key design decisions were made for this migration:

*   **Orchestration Layer**: Airflow was chosen as the target orchestration platform due to its flexibility, Python-native DAG definitions, and strong integration with GCP services. This replaces the legacy UC4 job scheduling and execution management.
*   **Data Processing Engine**: PySpark running on Google Cloud Dataproc was selected to replace the shell script logic. This provides a scalable, managed, and robust environment for big data processing, aligning with GCP best practices for data workloads.
*   **Script Storage**: Google Cloud Storage (GCS) is used to host the PySpark script. This provides a highly available and durable storage solution, easily accessible by Dataproc clusters.
*   **Unscheduled DAG**: The Airflow DAG is initially defined with `schedule=None`. This decision was based on the absence of an explicit schedule in the provided UC4 analysis. The DAG is intended for manual triggering or integration into a broader Airflow workflow.
*   **`DataprocSubmitJobOperator`**: This specific Airflow operator was chosen to directly submit the PySpark job to a Dataproc cluster, simplifying the interaction between Airflow and Dataproc.
*   **Placeholder Configuration**: GCP project ID, region, Dataproc cluster name, and GCS bucket name are left as placeholders in the generated code. This allows for flexible deployment across different environments (e.g., dev, test, prod) and requires manual configuration during deployment.
*   **UC4 Include Replacement**: The functionality of UC4 include files (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`) will be replaced by Airflow environment variables, PySpark script logic, or GCP-native logging (e.g., Cloud Logging) as appropriate, rather than direct replication.
*   **Semi-Automated Migration**: The migration was classified as `semi_auto` because the core business logic within the `r_ausd_bp_ta_bcp_iccid.ksh` script was not automatically translated. This requires significant manual effort for analysis and PySpark re-implementation.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **GCP Project Setup**:
    *   Ensure a GCP project exists and is configured for billing.
    *   Enable necessary APIs: Dataproc API, Cloud Storage API, BigQuery API (if used by PySpark script).
2.  **Dataproc Cluster Creation/Configuration**:
    *   Create or identify an existing Dataproc cluster in the specified `YOUR_GCP_REGION` (e.g., `us-central1`).
    *   The cluster name must match `YOUR_DATAPROC_CLUSTER_NAME` in the DAG.
    *   Ensure the cluster's service account has appropriate IAM roles (e.g., `Dataproc Worker`, `Storage Object Viewer`, `Storage Object Creator`, `BigQuery Data Editor` if writing to BigQuery, `BigQuery Data Viewer` if reading from BigQuery).
3.  **GCS Bucket Setup**:
    *   Create or identify an existing GCS bucket for storing PySpark scripts. The bucket name must match `YOUR_GCS_BUCKET_NAME`.
    *   Create the `pyspark_scripts/` prefix within this bucket.
4.  **PySpark Script Implementation (Critical B4 Item)**:
    *   **Analyze `r_ausd_bp_ta_bcp_iccid.ksh`**: Thoroughly analyze the original ksh script to understand its exact business logic, data sources, transformations, and output targets.
    *   **Implement `r_ausd_bp_ta_bcp_iccid.py`**: Translate the identified logic into the `pyspark_scripts/r_ausd_bp_ta_bcp_iccid.py` file. This involves:
        *   Defining Spark DataFrame operations.
        *   Configuring data source/sink connectors (e.g., BigQuery, GCS, other databases).
        *   Implementing error handling and logging.
        *   Replicating or replacing the functionality of `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`.
5.  **Upload PySpark Script to GCS**:
    *   Upload the completed `pyspark_scripts/r_ausd_bp_ta_bcp_iccid.py` file to `gs://YOUR_GCS_BUCKET_NAME/pyspark_scripts/`.
6.  **Airflow DAG Deployment**:
    *   Update the `dags/dw_bert_ausd_bp_ta_bcp_iccid.py` file with the correct `project_id`, `region`, `cluster_name`, and `main_python_file_uri` (GCS path).
    *   Deploy the updated DAG file to your Airflow environment's DAGs folder.
7.  **IAM/Permissions for Airflow**:
    *   Ensure the Airflow service account (or the service account associated with the `DataprocSubmitJobOperator`) has the necessary permissions to submit jobs to Dataproc (e.g., `Dataproc Editor` or `Dataproc Worker` roles).
8.  **Scheduling**:
    *   If the job requires a specific schedule (not `None`), update the `schedule` parameter in the Airflow DAG definition accordingly.
9.  **Connection Strings/Secrets**:
    *   If the PySpark script requires access to external databases or APIs, configure appropriate connection strings, secrets, or service accounts (e.g., using Airflow Connections, Google Secret Manager, or environment variables).

## 5. Known Gaps & Unresolved References

The following items are known gaps or require further follow-up:

*   **Detailed `r_ausd_bp_ta_bcp_iccid.ksh` Analysis (B4 Item)**: The most significant gap. The full business logic, data sources, transformations, and output targets of the original ksh script are *not yet fully analyzed or translated*. This requires a manual deep dive into the ksh script and its complete re-implementation in PySpark.
*   **UC4 Include Functionality**: The precise actions of `DW.HOLE_PFAD` (likely environment setup) and `DW.BERT_LESE_LOG` (custom logging) need to be determined. Their functionality must be mapped to appropriate Airflow environment variables, PySpark code, or GCP Cloud Logging mechanisms.
*   **Scheduling Confirmation**: The original UC4 job's schedule was not provided. The Airflow DAG is currently `schedule=None`. If a specific schedule exists in the broader UC4 ecosystem, it must be identified and applied to the Airflow DAG.
*   **GCP Placeholders**: The `YOUR_GCP_PROJECT_ID`, `YOUR_GCP_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_GCS_BUCKET_NAME` placeholders in the generated DAG must be replaced with actual values for deployment.
*   **PySpark Script Arguments**: If the original ksh script accepted command-line arguments, these need to be identified and passed to the PySpark script via the `args` parameter in the `DataprocSubmitJobOperator`.
*   **Error Handling and Retries**: While basic logging is included, a comprehensive error handling strategy for the PySpark script (e.g., dead-letter queues, specific retry logic) and the Airflow DAG (e.g., `retries`, `retry_delay`) should be reviewed and implemented.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deploy and Trigger Airflow DAG**:
    *   Ensure the `dags/dw_bert_ausd_bp_ta_bcp_iccid.py` is deployed to your Airflow environment.
    *   Manually trigger the `dw_bert_ausd_bp_ta_bcp_iccid` DAG from the Airflow UI.
2.  **Monitor Airflow Task Logs**:
    *   Observe the logs for the `run_dw_bert_ausd_bp_ta_bcp_iccid` task in the Airflow UI.
    *   Verify that the `DataprocSubmitJobOperator` successfully submits the job to Dataproc.
3.  **Monitor Dataproc Job Logs**:
    *   Navigate to the Dataproc Jobs section in the GCP Console.
    *   Find the submitted PySpark job and monitor its progress and logs.
    *   Look for any errors or exceptions reported by the PySpark script.
4.  **Data Validation**:
    *   Once the PySpark job completes, verify the output data (e.g., in BigQuery, GCS, or other target systems).
    *   **"Passing" means**:
        *   The Airflow DAG runs to completion without errors.
        *   The Dataproc PySpark job completes successfully (status `SUCCEEDED`).
        *   The output data generated by the PySpark script is accurate, complete, and matches the expected results from the legacy UC4 job. This is the most critical validation step and requires a fully implemented PySpark script.
        *   All logging and monitoring mechanisms (e.g., Cloud Logging) are functioning as expected.

## 7. Rollback Procedure

In case of issues or failure during the go-live or post-go-live period, the following rollback procedure can be executed:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, toggle off the `dw_bert_ausd_bp_ta_bcp_iccid` DAG to prevent further runs.
    *   Alternatively, remove the DAG file from the Airflow DAGs folder.
2.  **Re-enable Legacy UC4 Job**:
    *   Re-enable or re-schedule the original UC4 job `DW.BERT_AUSD_BP_TA_BCP_ICCID` in the legacy UC4 environment.
3.  **Data Cleanup (if necessary)**:
    *   If the migrated job performed any partial or incorrect data writes, identify and revert or clean up the affected data in the target systems (e.g., BigQuery tables, GCS files). This step is crucial to maintain data integrity.
4.  **Post-Rollback Analysis**:
    *   Investigate the root cause of the failure in the migrated job.
    *   Address the identified issues in the Airflow DAG, PySpark script, or GCP infrastructure before attempting re-deployment.