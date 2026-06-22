# MIGRATION_NOTES.md

## 1. Summary

The UC4 job `DW.BERT_AUSD_V_TA_P_VERTRAG` has been migrated to Google Cloud Platform.

**Original Job:**
*   **Platform:** UC4/Automic orchestrating a Korn Shell (KSH) script.
*   **Purpose:** Updates contract information related to twin-bill processing.
*   **Core Logic:** Resides in `r_ausd_v_ta_p_vertrag.ksh`.

**Target Platform:**
*   **Orchestration:** Apache Airflow on Google Cloud Composer.
*   **Compute:** PySpark application executed on Google Cloud Dataproc.
*   **Data Platform:** Google BigQuery for all data storage and processing.

This migration replaces the legacy UC4 orchestration and KSH script execution with a cloud-native Airflow DAG that submits a PySpark job to Dataproc, leveraging BigQuery for data operations.

## 2. Generated Artifacts

The migration process generated the following files:

1.  **`dags/dw_bert_ausd_v_ta_p_vertrag.py`**
    *   **Role:** This is the Airflow DAG definition file. It orchestrates the execution of the PySpark application on a Dataproc cluster. It defines a single task using `DataprocSubmitJobOperator` to submit the `r_ausd_v_ta_p_vertrag.py` script.
    *   **Location:** To be deployed to the Airflow DAGs folder in Cloud Composer.

2.  **`pyspark_scripts/r_ausd_v_ta_p_vertrag.py`**
    *   **Role:** This is the PySpark application that reimplements the core business logic originally found in the `r_ausd_v_ta_p_vertrag.ksh` Korn Shell script. Its purpose is to read, transform, and update contract information, interacting with BigQuery.
    *   **Location:** To be uploaded to a Google Cloud Storage (GCS) bucket, specifically `gs://YOUR_BUCKET_NAME/pyspark_scripts/`.

## 3. Key Design Decisions

*   **Cloud-Native Orchestration:** Apache Airflow on Cloud Composer was chosen to replace UC4 for its robust scheduling capabilities, extensibility, and native integration with GCP services.
*   **Distributed Compute for Business Logic:** Google Cloud Dataproc with PySpark was selected to re-platform the KSH script. This provides a scalable, managed, and performant environment for data processing, leveraging Spark's distributed capabilities for potential future growth and complex transformations.
*   **BigQuery as Target Data Platform:** BigQuery is the chosen data warehouse for its serverless architecture, analytical power, and seamless integration with Dataproc and other GCP services, replacing the likely Oracle database interaction.
*   **Direct Dataproc Job Submission:** The `DataprocSubmitJobOperator` is used in the Airflow DAG to directly submit the PySpark job to Dataproc. This simplifies the DAG definition and leverages Airflow's native GCP integrations.
*   **Parameterization:** Key job identifiers (e.g., `job_kennung`) are passed as arguments from the Airflow DAG to the PySpark script, maintaining flexibility and traceability similar to UC4 variable usage.
*   **Reimplementation in PySpark:** The decision to rewrite the KSH logic in PySpark ensures the solution is cloud-compatible, maintainable, and can benefit from the Spark ecosystem.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Configuration:**
    *   Replace `YOUR_GCP_PROJECT_ID` with the actual Google Cloud Project ID.
    *   Replace `YOUR_DATAPROC_REGION` with the desired GCP region for Dataproc.

2.  **Dataproc Cluster Setup:**
    *   Ensure a Dataproc cluster named `YOUR_DATAPROC_CLUSTER_NAME` exists and is running in `YOUR_DATAPROC_REGION`. This cluster will execute the PySpark job.
    *   Verify that the Dataproc cluster has the necessary BigQuery connector JARs available (e.g., via initialization actions or cluster properties).

3.  **Google Cloud Storage (GCS) Setup:**
    *   Create a GCS bucket named `YOUR_BUCKET_NAME` (if it doesn't already exist).
    *   Upload the PySpark script `pyspark_scripts/r_ausd_v_ta_p_vertrag.py` to the specified GCS path: `gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_v_ta_p_vertrag.py`.

4.  **Airflow DAG Deployment:**
    *   Upload the Airflow DAG file `dags/dw_bert_ausd_v_ta_p_vertrag.py` to the DAGs folder of your Cloud Composer environment.

5.  **IAM Permissions:**
    *   **Service Account:** Identify the service account used by your Dataproc cluster (or the one configured for `DataprocSubmitJobOperator`).
    *   **Required Roles:** Grant this service account the following IAM roles:
        *   `Dataproc Worker` (or a custom role with equivalent permissions for Dataproc job execution).
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) for the target BigQuery datasets/tables where contract information is updated.
        *   `BigQuery Data Viewer` for any source BigQuery datasets/tables the PySpark script reads from.
        *   `Storage Object Viewer` for reading the PySpark script from GCS.
        *   `Storage Object Creator` and `Storage Object Viewer` if the PySpark job writes temporary data to GCS.

6.  **BigQuery Dataset and Table Creation:**
    *   Create any necessary BigQuery datasets and tables that the `r_ausd_v_ta_p_vertrag.py` PySpark script will read from or write to. This includes the target tables for updated contract information. The exact schema will depend on the detailed analysis of the KSH script.

7.  **KSH Script Logic Reimplementation (Critical):**
    *   The placeholder logic in `pyspark_scripts/r_ausd_v_ta_p_vertrag.py` **must be replaced** with the actual business logic derived from a thorough reverse engineering of the original `r_ausd_v_ta_p_vertrag.ksh` script. This involves:
        *   Identifying all source data inputs (tables, files).
        *   Understanding all transformation rules, filters, joins, and aggregations.
        *   Determining the target tables and the exact update/insert logic.

8.  **UC4 Includes Equivalents:**
    *   Analyze the functionality of the UC4 includes `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`. If they contain configuration, path definitions, or common functions, these need to be translated into Airflow Variables, Python modules, or integrated directly into the PySpark script.

9.  **Scheduling Configuration:**
    *   The Airflow DAG is currently configured with `schedule_interval=None`. After deployment, manually configure the desired schedule in the Airflow UI based on the original UC4 job's schedule requirements.

10. **Retry and Error Handling:**
    *   Review the `default_args` in the Airflow DAG (e.g., `retries=0`) and adjust according to business requirements for robustness.
    *   Implement comprehensive error handling and logging within the `r_ausd_v_ta_p_vertrag.py` PySpark script.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent known limitations based on the current analysis:

*   **Incomplete Workflow Context:** The broader UC4 workflow dependencies, scheduling, and error handling of the original process are unknown due to the limited scope of the source inventory. This may impact integration with other migrated jobs.
*   **KSH Script Logic Detail:** The exact transformation logic within `r_ausd_v_ta_p_vertrag.ksh` is not detailed. A manual review and reverse engineering of this script are **required** to accurately reimplement its functionality in PySpark.
*   **UC4 Includes Implementation:** The specific functionality of `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` needs to be fully understood and translated into appropriate Python/PySpark modules or Airflow configurations.
*   **GCP Placeholders:** The placeholders `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME` must be replaced with actual environment values.
*   **Scheduling:** The original schedule cannot be derived from the provided UC4 XML. The `schedule_interval=None` in the Airflow DAG requires manual definition in Cloud Composer.
*   **Retry/Error Handling:** The initial Airflow DAG defaults to `retries=0`. This may need to be adjusted based on business requirements for resilience.
*   **Source Table Identification:** While the PySpark script comments suggest reading from source tables, the specific BigQuery tables that correspond to the original KSH script's inputs are not explicitly identified. This needs to be determined during the KSH script analysis.
*   **BigQuery Connector JAR:** While typically handled by Dataproc, explicit verification that the BigQuery connector JAR is available to the Spark application is recommended.

## 6. Validation

To validate the successful migration and functionality of the `DW.BERT_AUSD_V_TA_P_VERTRAG` job:

1.  **Trigger the DAG:**
    *   Access the Airflow UI for your Cloud Composer environment.
    *   Unpause the `dw_bert_ausd_v_ta_p_vertrags` DAG.
    *   Manually trigger a run of the DAG.

2.  **Monitor Execution:**
    *   **Airflow UI:** Observe the DAG run in the Airflow UI. Ensure the `run_bert_ausd_v_ta_p_vertrag` task transitions through "running" to "success". Check task logs for any Airflow-level errors.
    *   **Dataproc Jobs:** In the GCP Console, navigate to Dataproc -> Jobs. Find the submitted PySpark job (its name will include the DAG ID and run ID). Monitor its status and review the driver logs for any PySpark-specific errors or warnings.
    *   **Spark UI (Optional):** For detailed performance analysis, access the Spark UI linked from the Dataproc job details page.

3.  **Data Verification:**
    *   **BigQuery:** After the Dataproc job completes, query the target BigQuery tables that are expected to be updated by the job.
    *   **Comparison:** Compare the updated contract information in BigQuery with the expected output based on the original KSH script's behavior. This may involve comparing a sample of records or performing aggregate checks.
    *   **Data Integrity:** Verify that no data loss or corruption occurred during the transformation.

**"Passing" Criteria:**

*   The Airflow DAG `dw_bert_ausd_v_ta_p_vertrag` completes successfully without any task failures.
*   The Dataproc PySpark job completes successfully, as indicated in the Dataproc Jobs UI and its logs.
*   All expected data transformations and updates to contract information are correctly applied in the target BigQuery tables.
*   The data in BigQuery is consistent with the business logic of the original KSH script.
*   The job completes within acceptable performance thresholds.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Pause Airflow DAG:**
    *   Immediately pause the `dw_bert_ausd_v_ta_p_vertrag` DAG in the Airflow UI to prevent further execution of the migrated job.

2.  **Re-enable UC4 Job:**
    *   Re-enable the original `DW.BERT_AUSD_V_TA_P_VERTRAG` job in the UC4 system. Ensure its schedule and dependencies are correctly restored.

3.  **Verify UC4 Execution:**
    *   Monitor the UC4 job to confirm it resumes normal operation and continues to update contract information as expected.

4.  **Data State (Critical Consideration):**
    *   If the PySpark job made irreversible updates or deletions to BigQuery tables, a data restore from a previous backup or snapshot might be necessary. This depends heavily on the specific update logic implemented in `r_ausd_v_ta_p_vertrag.py` and the availability of BigQuery table snapshots or backups.
    *   If the PySpark job only appended data or performed soft deletes, the impact might be less severe, but data consistency should still be thoroughly checked.

5.  **Post-Rollback Analysis:**
    *   Investigate the root cause of the issue that necessitated the rollback. Address the identified problems in the Airflow DAG, PySpark script, or GCP configuration before attempting re-migration.