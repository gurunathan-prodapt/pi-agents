# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the UC4 job `DW.BERT_AUSD_BP_TA_P_BASISPROD`. This job, originally responsible for "Preparation of instantiated base products" by executing a shell script (`r_ausd_bp_ta_p_basisprod.ksh`) on a UNIX host, has been re-platformed to Google Cloud Platform (GCP).

The migrated solution leverages:
*   **Google Cloud Composer (Airflow)** for orchestration.
*   **Google Cloud Dataproc** for executing the core business logic.
*   **PySpark** as the language for the re-engineered business logic.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dags/dw_bert_ausd_bp_ta_p_basisprod.py`**
    *   **Role:** This is the Airflow DAG definition file. It orchestrates the execution of the PySpark job on a Dataproc cluster. It defines the DAG's metadata (ID, schedule, owner, retries) and contains a single task (`DataprocSubmitJobOperator`) responsible for submitting the PySpark application.
*   **`pyspark_scripts/r_ausd_bp_ta_p_basisprod.py`**
    *   **Role:** This is a placeholder PySpark application. It is intended to contain the re-engineered business logic originally found in the `r_ausd_bp_ta_p_basisprod.ksh` shell script. It will perform the "preparation of instantiated base products" using Spark's distributed processing capabilities, interacting with GCP data services like BigQuery or GCS as needed.

## 3. Key Design Decisions

*   **Orchestration with Airflow on Composer:** Airflow was chosen for its robust workflow orchestration capabilities, native integration with GCP services, and its ability to manage complex dependencies and scheduling. Composer provides a fully managed Airflow environment, reducing operational overhead.
*   **Business Logic Re-platforming to PySpark on Dataproc:** The original job executed a shell script. To align with modern data processing paradigms on GCP, the shell script's logic is re-engineered into a PySpark application. Dataproc provides a managed Spark environment, offering scalability, performance, and cost-effectiveness for distributed data processing. This approach allows for leveraging Spark's capabilities for large-scale data transformations.
*   **`semi_auto` Automation Bucket:** The source job was categorized as `semi_auto` due to the need for manual re-engineering of the shell script logic into PySpark. This implies that while the orchestration layer (Airflow DAG) can be largely automated, the core business logic requires detailed analysis and manual development.
*   **No Derived Schedule:** The original UC4 XML did not contain `EVNT_TIME` or other scheduling information, indicating that this job might be triggered by other jobs or external events. Consequently, the Airflow DAG is generated with `schedule=None`, meaning it will not run automatically and must be triggered manually or by a parent DAG.
*   **Placeholder PySpark Script:** Given the `semi_auto` classification and the lack of the original shell script's content, a placeholder PySpark script was generated. This highlights the critical need for a detailed analysis of `r_ausd_bp_ta_p_basisprod.ksh` to accurately translate its functionality into PySpark.

## 4. Manual Steps Before Go-Live

Before this migrated job can go live, the following manual steps are required:

1.  **GCP Project Configuration:**
    *   Replace `YOUR_GCP_PROJECT_ID` in `dags/dw_bert_ausd_bp_ta_p_basisprod.py` with the actual GCP Project ID.
2.  **Dataproc Cluster Setup:**
    *   Ensure a Dataproc cluster named `YOUR_DATAPROC_CLUSTER_NAME` exists in `YOUR_DATAPROC_REGION`. If not, create one. This cluster will be used to execute the PySpark job.
    *   Replace `YOUR_DATAPROC_REGION` and `YOUR_DATAPROC_CLUSTER_NAME` in the DAG file.
3.  **Google Cloud Storage (GCS) Bucket:**
    *   Create a GCS bucket named `YOUR_BUCKET_NAME` (e.g., `gs://your-project-dataproc-bucket`). This bucket will store the PySpark application script and potentially any input/output data for the job.
    *   Replace `YOUR_BUCKET_NAME` in the DAG file.
4.  **IAM Permissions:**
    *   **Composer Service Account:** Grant the Composer environment's service account (e.g., `service-<project-number>@cloudcomposer.gserviceaccount.com`) the necessary roles to:
        *   Submit jobs to Dataproc (`roles/dataproc.editor` or `roles/dataproc.worker`).
        *   Read/write from the GCS bucket (`roles/storage.objectViewer`, `roles/storage.objectCreator`).
        *   Access BigQuery datasets/tables if the PySpark script interacts with BigQuery (`roles/bigquery.dataEditor`, `roles/bigquery.jobUser`).
    *   **Dataproc Worker Service Account:** Ensure the service account used by the Dataproc cluster's worker nodes has permissions to:
        *   Read the PySpark script from GCS (`roles/storage.objectViewer`).
        *   Read/write data from/to GCS and BigQuery as required by the PySpark logic (`roles/storage.objectAdmin`, `roles/bigquery.dataEditor`).
5.  **PySpark Script Development & Deployment:**
    *   **Analyze `r_ausd_bp_ta_p_basisprod.ksh`:** Thoroughly analyze the content and logic of the original `r_ausd_bp_ta_p_basisprod.ksh` shell script. This is the most critical step.
    *   **Develop `r_ausd_bp_ta_p_basisprod.py`:** Re-engineer the shell script's logic into the `pyspark_scripts/r_ausd_bp_ta_p_basisprod.py` file. This includes identifying data sources, transformations, and target destinations (e.g., BigQuery tables, GCS paths).
    *   **Upload to GCS:** Upload the completed `r_ausd_bp_ta_p_basisprod.py` to the specified GCS path: `gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_p_basisprod.py`.
6.  **Airflow DAG Deployment:**
    *   Upload the `dags/dw_bert_ausd_bp_ta_p_basisprod.py` file to the `dags/` folder of your Composer environment's GCS bucket. Airflow will automatically detect and parse the DAG.
7.  **Update `start_date`:**
    *   Replace `PLACEHOLDER_START_DATE` in the DAG file with an appropriate historical date (e.g., `datetime(2023, 1, 1)` or earlier) to allow for backfills if needed.
8.  **Scheduling & Dependencies:**
    *   As `schedule=None`, determine how this DAG will be triggered. This might involve manual triggers, integration into a parent Airflow DAG, or an external event.

## 5. Known Gaps & Unresolved References

The following items require further investigation or resolution:

*   **B4 Item: Incomplete Workflow Information:** The migration was based on a single UC4 job definition.
    *   **Impact:** No scheduling information (`EVNT_TIME`), inter-job dependencies (`JOBP`), or calendar/synchronization behaviors could be derived.
    *   **Resolution:** A full UC4 workflow export is required to accurately determine the job's original schedule and its position within a larger workflow. This will inform the final `schedule` parameter and potential `ExternalTaskSensor` configurations in Airflow.
*   **B4 Item: Shell Script to PySpark Mapping:** The `pyspark_scripts/r_ausd_bp_ta_p_basisprod.py` is a placeholder.
    *   **Impact:** The actual business logic for "preparation of instantiated base products" is not yet implemented in PySpark.
    *   **Resolution:** The content of the original `r_ausd_bp_ta_p_basisprod.ksh` shell script must be thoroughly analyzed and re-engineered into the PySpark application. This includes understanding any environment variables, external commands, database interactions, and file operations performed by the shell script.
*   **UC4 `inc` and `set` statements:** The original UC4 job included:
    *   `:inc DW.HOLE_PFAD`
    *   `:set &DWH_JOB_KENNUNG='AUSD_BP_TA_P_BASISPROD'`
    *   `. $HOME/.dw_init`
    *   `:inc DW.BERT_LESE_LOG`
    *   **Impact:** The exact functionality of these includes and environment setup is unknown. They might set critical paths, variables, or perform logging.
    *   **Resolution:** Investigate the content of `DW.HOLE_PFAD`, `.dw_init`, and `DW.BERT_LESE_LOG`. The `&DWH_JOB_KENNUNG` variable might need to be passed as an argument to the PySpark script or configured as an Airflow variable.
*   **Retry Policy:** The UC4 XML did not explicitly define a retry policy.
    *   **Impact:** The default `retries=0` in the Airflow DAG might not align with the original operational requirements.
    *   **Resolution:** Confirm the desired retry behavior for this job. If retries are needed, update `default_args["retries"]` and `retry_delay` in the DAG.
*   **GCP Placeholders:** The DAG contains placeholders (`YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`) that must be replaced with actual environment-specific values.

## 6. Validation

To validate the migrated job:

1.  **Deploy the DAG:** Ensure `dags/dw_bert_ausd_bp_ta_p_basisprod.py` is deployed to your Composer environment and `pyspark_scripts/r_ausd_bp_ta_p_basisprod.py` (after development) is uploaded to the specified GCS path.
2.  **Trigger the DAG:** Manually trigger the `dw_bert_ausd_bp_ta_p_basisprod` DAG from the Airflow UI.
3.  **Monitor Airflow Logs:** Observe the task `run_dw_bert_ausd_bp_ta_p_basisprod` in the Airflow UI. Check its logs for any errors or unexpected behavior.
4.  **Monitor Dataproc Job:** Navigate to the Dataproc Jobs section in the GCP Console. Find the job submitted by Airflow and monitor its progress and logs.
5.  **Data Validation:**
    *   **Inspect Output:** Verify that the PySpark job produces the expected output in the target BigQuery tables or GCS locations.
    *   **Compare with Source:** If possible, compare the output of the migrated job with the output generated by the original UC4 job for a specific run. This is crucial for functional correctness.

**"Passing" means:**
*   The Airflow DAG completes successfully without any task failures.
*   The Dataproc job completes successfully, indicated by a "SUCCEEDED" status in the GCP Console.
*   The PySpark application logs indicate successful execution of the business logic.
*   The data generated or transformed by the PySpark job is accurate, complete, and matches the expected results based on the original shell script's functionality.

## 7. Rollback Procedure

In case of issues during validation or after go-live, the following rollback procedure can be followed:

1.  **Pause/Delete Airflow DAG:** In the Airflow UI, pause or delete the `dw_bert_ausd_bp_ta_p_basisprod` DAG to prevent further executions.
2.  **Revert Data Changes (if applicable):**
    *   If the PySpark job modified BigQuery tables, use BigQuery's time travel feature to revert the tables to their state before the problematic job run.
    *   If the job wrote to GCS, delete or revert the newly created/modified files.
    *   **Crucially, ensure any data changes made by the new job are undone or isolated.**
3.  **Re-enable Original UC4 Job:** Re-enable the original `DW.BERT_AUSD_BP_TA_P_BASISPROD` job in UC4/Automic.
4.  **Remove Deployed Artifacts (Optional):**
    *   Delete the `dags/dw_bert_ausd_bp_ta_p_basisprod.py` file from the Composer DAGs folder.
    *   Delete the `pyspark_scripts/r_ausd_bp_ta_p_basisprod.py` file from GCS.
5.  **Investigate and Rectify:** Analyze the root cause of the failure, fix the PySpark code or Airflow DAG, and re-attempt the migration.