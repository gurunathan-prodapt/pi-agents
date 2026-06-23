# MIGRATION_NOTES.md

## 1. Summary

The legacy UC4 job `DW.BERT_AUSD_V_TA_VERTRAG_TMP`, which orchestrates the execution of a KornShell (KSH) script (`r_ausd_v_ta_vertrag_tmp.ksh`) for contract-related data preparation, has been migrated.

*   **Original System:** UC4/Automic (Orchestration), Unix/KSH (Data Processing)
*   **Target Platform:** Google Cloud Platform (GCP)
    *   **Orchestration:** Cloud Composer (Apache Airflow)
    *   **Data Processing:** Dataproc (PySpark)

The migration re-platforms the job to leverage cloud-native services, providing scalability, managed infrastructure, and improved observability.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dw_bert_ausd_v_ta_vertrag_tmp.py`**
    *   **Role:** This is an Apache Airflow Directed Acyclic Graph (DAG) definition file. It replaces the UC4 `JOBS_UNIX` object and is responsible for orchestrating the data processing workflow. It defines a single task that submits a PySpark job to a Dataproc cluster.
    *   **Location:** This file should be uploaded to your Cloud Composer environment's DAGs folder.
*   **`r_ausd_v_ta_vertrag_tmp.py`**
    *   **Role:** This is a PySpark script designed to encapsulate the data processing logic originally found in the `r_ausd_v_ta_vertrag_tmp.ksh` KornShell script. It is executed on a Dataproc cluster by the Airflow DAG.
    *   **Location:** This file should be uploaded to a designated Google Cloud Storage (GCS) bucket, which is then referenced by the Airflow DAG.

## 3. Key Design Decisions

*   **Cloud Composer for Orchestration:** Airflow on Cloud Composer was chosen to replace UC4 for its robust scheduling capabilities, native GCP integration, and industry-standard workflow management. This provides a managed, scalable, and observable orchestration layer.
*   **Dataproc for Data Processing:** Dataproc was selected to execute the data processing logic (re-implemented in PySpark) due to its fully managed nature, scalability for big data workloads, and compatibility with Spark. This eliminates the need to manage underlying Unix servers and provides a modern, distributed processing framework.
*   **PySpark for Data Transformation:** The original KSH script's logic is re-implemented in PySpark to leverage Spark's distributed processing capabilities, making the solution more scalable and maintainable for data transformations.
*   **`DataprocSubmitJobOperator`:** This Airflow operator is used to directly submit the PySpark job to Dataproc, simplifying the interaction between the orchestrator and the processing engine.
*   **Parameter Passing:** UC4 variables (e.g., `&DWH_JOB_KENNUNG`) are passed as command-line arguments to the PySpark script via the `DataprocSubmitJobOperator`, ensuring continuity of configuration.
*   **Linear DAG Structure:** Given the absence of `EVNT_TIME` or `JOBP` files in the source, the generated Airflow DAG has a simple linear dependency (`start >> pyspark_job >> end`). This is a placeholder, and complex scheduling or inter-job dependencies will require manual definition based on a broader review of the UC4 environment.
*   **Trade-offs:**
    *   **Manual KSH to PySpark Conversion:** The core logic of `r_ausd_v_ta_vertrag_tmp.ksh` was not available for automated analysis, necessitating manual reverse-engineering and re-implementation in PySpark. This introduces a significant manual effort and potential for discrepancies.
    *   **Manual Dependency Mapping:** The lack of UC4 workflow definitions means that any complex scheduling or inter-job dependencies with other systems or jobs must be manually identified and configured in Airflow.
    *   **Placeholder Configuration:** The generated Airflow DAG contains placeholders for GCP project ID, region, Dataproc cluster name, and GCS bucket, which require manual configuration before deployment.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **GCP Project Setup:**
    *   Ensure a GCP project is set up and configured.
    *   Enable the necessary APIs: Cloud Composer API, Dataproc API, Cloud Storage API, BigQuery API (if used by PySpark).
2.  **Dataproc Cluster Creation:**
    *   Create or identify an existing Dataproc cluster in the specified `GCP_REGION` that will execute the PySpark job.
    *   Update the `DATAPROC_CLUSTER_NAME` placeholder in `dw_bert_ausd_v_ta_vertrag_tmp.py` with the actual cluster name.
3.  **GCS Bucket for PySpark Scripts:**
    *   Create a dedicated GCS bucket (e.g., `gs://my-dataproc-scripts`) to store the `r_ausd_v_ta_vertrag_tmp.py` script and any other necessary PySpark dependencies.
    *   Update the `GCS_PYSPARK_BUCKET` placeholder in `dw_bert_ausd_v_ta_vertrag_tmp.py` with the actual bucket path.
4.  **Upload PySpark Script:**
    *   Upload the `r_ausd_v_ta_vertrag_tmp.py` file to the `pyspark_scripts/` subfolder within your designated GCS bucket (e.g., `gs://my-dataproc-scripts/pyspark_scripts/r_ausd_v_ta_vertrag_tmp.py`).
5.  **Upload Airflow DAG:**
    *   Upload the `dw_bert_ausd_v_ta_vertrag_tmp.py` file to the DAGs folder of your Cloud Composer environment.
6.  **IAM and Permissions:**
    *   **Cloud Composer Service Account:** Ensure the service account associated with your Cloud Composer environment has the necessary permissions to:
        *   Submit Dataproc jobs (`dataproc.jobs.create`).
        *   Read from the GCS bucket containing the PySpark script (`storage.objects.get`).
        *   Access any other GCP resources (e.g., BigQuery, other GCS buckets) that the PySpark job interacts with.
    *   **Dataproc Worker Service Account:** Ensure the service account used by the Dataproc cluster workers has permissions to:
        *   Read/write data from/to GCS buckets.
        *   Read/write data from/to BigQuery tables.
        *   Access any other external systems or GCP services required by the PySpark logic.
7.  **Secrets and Connection Strings:**
    *   If the original KSH script or the new PySpark script requires access to databases, APIs, or other external systems, ensure that connection strings, credentials, and secrets are securely managed (e.g., using Google Secret Manager) and passed to the PySpark job.
8.  **Define Airflow DAG Schedule:**
    *   The `schedule_interval` in `dw_bert_ausd_v_ta_vertrag_tmp.py` is currently `None`. Manually set this to the desired cron expression or timedelta based on the original UC4 job's schedule (e.g., `'0 0 * * *'` for daily at midnight).
9.  **Implement PySpark Logic:**
    *   **Crucially**, the `r_ausd_v_ta_vertrag_tmp.py` script contains placeholder comments (`# --- TODO: Implement your data processing logic here ---`). The actual data processing logic from the original `r_ausd_v_ta_vertrag_tmp.ksh` script must be manually reverse-engineered and implemented in PySpark.
10. **Review UC4 Include Files:**
    *   Manually review the content of the UC4 include files `:inc DW.HOLE_PFAD` and `:inc DW.BERT_LESE_LOG`. Any critical logic, configurations, or utilities within these includes must be adapted and integrated into the PySpark script or the Airflow environment as appropriate (e.g., as Python helper modules, Airflow variables, or environment variables).

## 5. Known Gaps & Unresolved References

*   **KSH Script Content Unknown:** The actual logic of `r_ausd_v_ta_vertrag_tmp.ksh` was not available for automated analysis. This is the most significant gap, requiring manual reverse-engineering and re-implementation of the data processing logic in PySpark.
*   **Missing UC4 Workflow Definitions:** The absence of `EVNT_TIME` (scheduling) and `JOBP` (job plan/workflow) objects means that the complete scheduling and inter-job dependency chain of this UC4 job within its broader environment could not be automatically derived. The Airflow DAG's schedule and any upstream/downstream dependencies with other jobs will need manual investigation and definition.
*   **UC4 Include Files:** The content and purpose of `:inc DW.HOLE_PFAD` and `:inc DW.BERT_LESE_LOG` are unknown. Their logic needs to be analyzed and adapted for the GCP environment.
*   **Incomplete Metadata:** `file_complexity` and `automation_rate` for the source UC4 XML file were not available, hindering an accurate assessment of migration effort and automation bucket.
*   **Placeholder Values:** The generated Airflow DAG contains placeholder values for `GCP_PROJECT_ID`, `GCP_REGION`, `DATAPROC_CLUSTER_NAME`, and `GCS_PYSPARK_BUCKET`. These must be manually updated.

## 6. Validation

To ensure the migrated job functions correctly, follow these validation steps:

1.  **Local PySpark Script Testing:**
    *   **How to run:** Execute `r_ausd_v_ta_vertrag_tmp.py` locally using a Spark installation (or a local PySpark environment) with sample input data that mimics the production environment.
    *   `spark-submit r_ausd_v_ta_vertrag_tmp.py --job_kennung AUSD_V_TA_VERTRAG_TMP`
    *   **"Passing" means:**
        *   The script executes without errors.
        *   The output data (if any) is generated in the expected format and location.
        *   The data transformations applied match the expected logic derived from the original KSH script.
2.  **Dataproc Direct Submission Test:**
    *   **How to run:** Manually submit the `r_ausd_v_ta_vertrag_tmp.py` script to your Dataproc cluster using `gcloud dataproc jobs submit pyspark`.
    *   `gcloud dataproc jobs submit pyspark --cluster=your-dataproc-cluster-name --region=your-gcp-region gs://your-gcs-bucket-for-pyspark-scripts/pyspark_scripts/r_ausd_v_ta_vertrag_tmp.py -- --job_kennung AUSD_V_TA_VERTRAG_TMP`
    *   **"Passing" means:**
        *   The Dataproc job completes successfully.
        *   Logs in Dataproc show no errors or unexpected warnings.
        *   Output data (if any) is correctly written to the target destination (e.g., GCS, BigQuery).
        *   Resource utilization on the Dataproc cluster is within expected bounds.
3.  **Airflow DAG Execution Test:**
    *   **How to run:**
        1.  Ensure `dw_bert_ausd_v_ta_vertrag_tmp.py` is uploaded to the Composer DAGs folder and the DAG appears in the Airflow UI.
        2.  Manually trigger the `dw_bert_ausd_v_ta_vertrag_tmp` DAG from the Airflow UI.
    *   **"Passing" means:**
        *   The DAG runs successfully, with all tasks (start, `dw_bert_ausd_v_ta_vertrag_tmp_pyspark_job`, end) turning green.
        *   The `DataprocSubmitJobOperator` task successfully submits the PySpark job to Dataproc.
        *   The Dataproc job completes successfully (as verified in step 2).
        *   Logs in Airflow and Dataproc are clean and indicate successful execution.
        *   The final output data is accurate and consistent with the original job's expected results.

## 7. Rollback Procedure

In case of issues or failure during or after the migration, the following rollback procedure can be followed:

1.  **Disable Airflow DAG:**
    *   In the Cloud Composer Airflow UI, toggle off the `dw_bert_ausd_v_ta_vertrag_tmp` DAG to prevent further executions.
    *   Optionally, delete the DAG file from the Composer DAGs folder.
2.  **Re-enable Original UC4 Job:**
    *   Re-enable the original UC4 job `DW.BERT_AUSD_V_TA_VERTRAG_TMP` in the UC4/Automic environment.
3.  **Verify UC4 Job Execution:**
    *   Monitor the UC4 job to ensure it runs successfully and produces the expected output, confirming that the legacy system is fully operational again.
4.  **Data Restoration (if necessary):**
    *   If the migrated job made any changes to production data, and those changes were incorrect or incomplete, a data restoration might be required. This would involve restoring the target data stores (e.g., BigQuery tables, GCS files) to their state before the migrated job's execution. This step is highly dependent on the specific data impact of the job.