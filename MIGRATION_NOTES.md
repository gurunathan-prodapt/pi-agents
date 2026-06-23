# MIGRATION_NOTES.md: DW.BERT_AUSD_BP_TA_RN_VERTRAG

## 1. Summary

This migration involved the legacy Automic (UC4) Unix job `DW.BERT_AUSD_BP_TA_RN_VERTRAG`, which was responsible for orchestrating the execution of a shell script named `r_ausd_bp_ta_rn_vertrag.ksh`. The job's purpose is "Aufbereitung der instantiierten Basisprodukte" (preparation of instantiated basic products).

The job has been migrated to Google Cloud Platform, utilizing the following services:
*   **Orchestration**: Apache Airflow (managed by Cloud Composer).
*   **Data Processing**: PySpark, executed on Google Cloud Dataproc.
*   **Data Storage/Target**: Google BigQuery.

## 2. Generated artifacts

The migration process generated the following files:

*   **`dags/dw_bert_ausd_bp_ta_rn_vertrag.py`**
    *   **Role**: This is an Apache Airflow DAG (Directed Acyclic Graph) written in Python. It replaces the UC4 job's orchestration logic. Its primary function is to define the workflow, which includes submitting a PySpark job to a Dataproc cluster. It handles the scheduling (once defined) and overall execution flow of the data processing task.
*   **`pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py`**
    *   **Role**: This is a PySpark script written in Python. It is intended to contain the core data processing and transformation logic that was originally present in the `r_ausd_bp_ta_rn_vertrag.ksh` (and likely `k_ausd_bp_ta_rn_vertrag.ksh`) shell script. This script will be executed on a Dataproc cluster and is responsible for reading source data, applying transformations, and writing results to BigQuery.

## 3. Key design decisions

*   **Orchestration with Airflow (Cloud Composer)**: Airflow was chosen to replace UC4 due to its robust workflow management capabilities, Python-native environment, and Google's managed service offering (Cloud Composer), which reduces operational overhead. This provides a modern, scalable, and observable orchestration layer.
*   **Data Processing with PySpark on Dataproc**: The shell script's data processing logic is re-implemented in PySpark. This decision leverages Spark's distributed processing power for scalability and performance, especially for large datasets. Dataproc provides a fully managed Spark environment, simplifying cluster management and integration with other GCP services.
*   **BigQuery as Target Data Warehouse**: BigQuery is selected as the target for processed data due to its serverless architecture, petabyte-scale analytics capabilities, and seamless integration with Spark (via the BigQuery connector).
*   **`semi_auto` Migration Approach**: The migration was categorized as `semi_auto` primarily because the detailed business logic within the original `r_ausd_bp_ta_rn_vertrag.ksh` (and its likely dependency `k_ausd_bp_ta_rn_vertrag.ksh`) was not available for automated translation. This necessitates a manual analysis and re-implementation of the core logic in PySpark.
*   **Default `retries=0`**: The Airflow DAG is configured with `retries=0` and `retry_delay=timedelta(seconds=0)`. This decision mirrors the absence of explicit retry logic in the original UC4 XML export, ensuring that the migrated job's behavior is consistent with the legacy system's defined robustness (or lack thereof) unless explicitly enhanced.
*   **Parameter Handling**: Key parameters like `stichtag` and `wiederanlaufWert` are passed as arguments to the PySpark script via the `DataprocSubmitJobOperator`. This maintains the configurability of the original shell script.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps are required:

1.  **Complete PySpark Script Logic**:
    *   **Crucially**, the `pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py` file contains placeholder comments. The actual data processing logic from the original `r_ausd_bp_ta_rn_vertrag.ksh` and `k_ausd_bp_ta_rn_vertrag.ksh` shell scripts must be manually analyzed, understood, and translated into PySpark code (using Spark SQL or DataFrames).
2.  **GCP Placeholder Replacement**:
    *   In `dags/dw_bert_ausd_bp_ta_rn_vertrag.py`, replace the placeholder values for:
        *   `GCP_PROJECT_ID`: Your actual Google Cloud Project ID.
        *   `DATAPROC_REGION`: The GCP region where your Dataproc cluster is located (e.g., `us-central1`).
        *   `DATAPROC_CLUSTER_NAME`: The name of the Dataproc cluster to be used for PySpark job execution.
        *   `GCS_BUCKET_NAME`: The name of the Google Cloud Storage bucket where the `pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py` will be uploaded.
3.  **Dataproc Cluster Setup**:
    *   Ensure a Dataproc cluster with the specified `DATAPROC_CLUSTER_NAME` is provisioned and running in the `DATAPROC_REGION`. The cluster should have network access to BigQuery and GCS.
4.  **IAM Permissions**:
    *   Grant the service account used by Cloud Composer (Airflow) and the Dataproc cluster sufficient IAM roles:
        *   `Dataproc Worker` (or equivalent) for the Dataproc cluster service account.
        *   `BigQuery Data Editor` (or more granular roles) for writing to BigQuery.
        *   `BigQuery Data Viewer` (or more granular roles) for reading from BigQuery.
        *   `Storage Object Viewer` and `Storage Object Creator` for accessing and uploading scripts to the GCS bucket.
        *   `Dataproc Editor` (or more granular roles) for the Airflow service account to submit jobs to Dataproc.
5.  **BigQuery Dataset/Table Creation**:
    *   Ensure that all necessary BigQuery datasets and tables (both source and target, as determined by the PySpark script's logic) are pre-created with the correct schemas.
6.  **Upload PySpark Script to GCS**:
    *   Upload the completed `pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py` to the specified GCS bucket path: `gs://YOUR_GCS_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py`.
7.  **Define Airflow DAG Schedule**:
    *   The `schedule` parameter in the Airflow DAG is currently set to `None`. Based on the original UC4 job's scheduling requirements (which were not provided in the export), update this parameter (e.g., `schedule="@daily"`, `timedelta(days=1)`, or a specific cron expression).
8.  **Review `stichtag` and `wiederanlaufWert` Logic**:
    *   The `pyspark_job_arguments` in the DAG currently use `{{ ds_nodash }}` for `stichtag` and a hardcoded `0` for `wiederanlaufWert`. Review how these parameters were determined in the legacy system and implement dynamic calculation or Airflow Variable usage if needed.

## 5. Known gaps & unresolved references

*   **Detailed Shell Script Analysis (B4 Item)**: The most significant gap is the complete analysis and translation of the `r_ausd_bp_ta_rn_vertrag.ksh` and `k_ausd_bp_ta_rn_vertrag.ksh` shell scripts into PySpark. This is a critical manual step that determines the functional correctness of the migrated job.
*   **Missing Schedule Information**: The original UC4 export did not contain scheduling details. The Airflow DAG's schedule must be manually defined based on business requirements.
*   **Incomplete Workflow Context**: The migration was based on a single `JOBS_UNIX` file, without a `JOBP` or `JSCH` container. This means potential upstream or downstream dependencies and the broader context of this job within the legacy UC4 workflow are not fully captured and may require further investigation.
*   **UC4 Includes Translation**: The functionality of UC4 includes like `:inc DW.HOLE_PFAD` and `:inc DW.BERT_LESE_LOG` needs to be understood and translated. `DW.HOLE_PFAD` might relate to environment variable setup, which can be handled by Airflow or Dataproc environment configurations. `DW.BERT_LESE_LOG` likely pertains to custom logging, which should be replaced by standard Python/Spark logging integrated with Cloud Logging.
*   **Error Handling and Retries**: While the current Airflow DAG mirrors the UC4 export's lack of explicit retries, a review of desired operational robustness is recommended. Consider adding appropriate `retries` and `retry_delay` to the `DataprocSubmitJobOperator` for production resilience.
*   **Source System Identification**: The design document implies the shell script interacts with a database. The exact source systems (e.g., Oracle, Teradata) and their connection details need to be identified from the shell script and re-engineered for GCP (e.g., BigQuery external tables, direct connectors, or prior ingestion pipelines).

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deploy and Trigger DAG**:
    *   Upload the completed `dags/dw_bert_ausd_bp_ta_rn_vertrag.py` to your Cloud Composer environment's DAGs folder.
    *   Ensure the DAG is unpaused in the Airflow UI.
    *   Manually trigger the `dw_bert_ausd_bp_ta_rn_vertrag` DAG from the Airflow UI.
2.  **Monitor Execution**:
    *   Monitor the DAG run in the Airflow UI to ensure all tasks complete successfully.
    *   Check Dataproc job logs in the Google Cloud Console for any errors or warnings during the PySpark execution.
    *   Review Cloud Logging for detailed logs from both Airflow and Dataproc.
3.  **Data Validation**:
    *   **Functional Equivalence**: Compare the output data in BigQuery generated by the migrated job with the output produced by the legacy UC4 job for the same input data and parameters. This is the most critical validation step.
    *   **Data Integrity**: Verify that data types, formats, and values are correctly preserved or transformed as expected.
    *   **Completeness**: Ensure all expected records are processed and present in the target BigQuery tables.
4.  **Performance Testing**:
    *   Compare the execution time of the migrated job against the legacy job. Optimize Dataproc cluster size and PySpark configurations if performance is not satisfactory.
5.  **Parameter Validation**:
    *   Test the job with different values for `stichtag` and `wiederanlaufWert` to ensure correct behavior under various conditions.

**"Passing" Criteria**:
*   The Airflow DAG completes successfully without any task failures.
*   The Dataproc job completes successfully, and its logs indicate no errors.
*   The data generated in the target BigQuery tables is functionally identical or meets the specified transformation requirements when compared to the legacy system's output.
*   The job completes within acceptable performance thresholds.
*   All logging and monitoring mechanisms are functioning as expected.

## 7. Rollback procedure

In case of issues with the migrated job, the following rollback procedure can be initiated:

1.  **Disable New Airflow DAG**:
    *   Immediately pause the `dw_bert_ausd_bp_ta_rn_vertrag` DAG in the Airflow UI to prevent further executions.
2.  **Re-enable Legacy UC4 Job**:
    *   Re-enable the original `DW.BERT_AUSD_BP_TA_RN_VERTRAG` job in the Automic (UC4) system.
3.  **Data Reversion (if necessary)**:
    *   If the migrated job made destructive or incorrect changes to BigQuery tables, execute a data rollback strategy. This might involve:
        *   Restoring BigQuery tables from a previous snapshot or backup.
        *   Running a compensating job to correct or delete erroneous data.
        *   **Note**: It is highly recommended to implement the PySpark script to write to temporary or staging tables first, or use BigQuery's time travel capabilities, to minimize the impact of potential data corruption during initial deployments.
4.  **Investigate and Rectify**:
    *   Analyze the logs from Airflow, Dataproc, and BigQuery to identify the root cause of the failure.
    *   Correct the issues in the Airflow DAG or PySpark script.
    *   Re-test thoroughly in a non-production environment before attempting another go-live.