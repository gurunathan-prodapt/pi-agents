```markdown
# MIGRATION_NOTES: DW.BERT_AUSD_V_TA_C_BFC

## 1. Summary

The legacy UC4 job `DW.BERT_AUSD_V_TA_C_BFC`, responsible for updating contract extension period caching, has been migrated from a UNIX-based execution environment orchestrated by UC4/Automic to Google Cloud Platform (GCP). The original job executed a KornShell script (`r_ausd_v_ta_c_bfc.ksh`) on a UNIX host (`DWHDWH1P`).

The new target platform utilizes:
*   **Google Cloud Composer (Apache Airflow)** for workflow orchestration, replacing UC4.
*   **Google Cloud Dataproc** for executing the core business logic, replacing the UNIX host.
*   **PySpark** as the language for the re-implemented business logic, replacing the KornShell script.
*   **Google Cloud Storage (GCS)** for storing the PySpark script and other job assets.

## 2. Generated Artifacts

The migration produced the following files:

*   **`dags/dw_bert_ausd_v_ta_c_bfc.py`**
    *   **Role:** This is an Apache Airflow DAG (Directed Acyclic Graph) written in Python. It serves as the orchestrator for the migrated job, replacing the UC4 job definition. Its primary function is to submit and monitor a PySpark job on a Google Cloud Dataproc cluster.
    *   **Location:** This file must be uploaded to the DAGs folder of the Cloud Composer environment.

*   **`pyspark_scripts/r_ausd_v_ta_c_bfc.py`**
    *   **Role:** This is a PySpark script written in Python. It contains the re-implemented business logic that was originally present in the `r_ausd_v_ta_c_bfc.ksh` KornShell script. Its purpose is to perform the necessary data processing and updates for contract extension period caching.
    *   **Location:** This file must be uploaded to a designated Google Cloud Storage bucket (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/`) from where Dataproc can access it.

## 3. Key Design Decisions

*   **Cloud Composer for Orchestration:** Chosen as a managed Apache Airflow service to replace UC4. This provides a scalable, robust, and cloud-native orchestration platform with strong integration capabilities across GCP services.
*   **Dataproc for Compute:** Selected as the execution engine for the business logic. Dataproc is a managed Spark/Hadoop service, ideal for distributed data processing, offering auto-scaling and cost-efficiency, especially with ephemeral clusters. This replaces the dedicated UNIX host.
*   **PySpark for Business Logic Re-implementation:** The original KornShell script's logic is converted to PySpark. This enables leveraging Spark's distributed processing capabilities on Dataproc, aligning with modern data engineering practices, and utilizing Python's extensive ecosystem.
*   **`max_active_runs=1` for Concurrency Control:** The UC4 `SYNCREF` object with `Else="Wait"` behavior, which prevented concurrent job executions, is directly mapped to the Airflow DAG property `max_active_runs=1`. This ensures only one instance of the DAG runs at a time.
*   **`schedule=None` for Initial Scheduling:** As no explicit `EVNT_TIME` (schedule) was found in the original UC4 job definition, the Airflow DAG is initially configured with `schedule=None`. This allows for manual triggering and defers the scheduling decision to a later stage based on confirmed business requirements.
*   **Google Cloud Storage for Script and Asset Management:** GCS provides a highly durable, available, and cost-effective storage solution for the PySpark script and any other job-related files, making them easily accessible to Dataproc clusters.
*   **GCP Service Accounts for IAM:** Replaces the UNIX login (`DW.UNIX.ISBERT`). Service accounts provide a secure and granular mechanism for managing permissions for GCP resources, ensuring the principle of least privilege.

**Notable Trade-offs:**

*   **KornShell to PySpark Conversion Complexity:** The transformation of the KornShell script to PySpark is a manual effort requiring deep understanding of both the original script's logic and PySpark best practices. This introduces potential for subtle behavioral changes and requires thorough testing.
*   **UC4 Include Objects Re-implementation:** The functionality of UC4 include objects (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`) needs to be fully understood and re-implemented, potentially as custom Python modules, Airflow variables, or integrated logging, adding to the migration effort.
*   **Dataproc Cluster Strategy (Ephemeral vs. Persistent):** While the design suggests considering ephemeral clusters for cost efficiency, the generated DAG uses a fixed `DATAPROC_CLUSTER_NAME`. A decision needs to be made whether to use a persistent cluster (faster startup, higher continuous cost) or implement an ephemeral cluster creation/deletion strategy (slower startup, lower cost for infrequent jobs).

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **GCP Project Configuration:**
    *   Update `GCP_PROJECT_ID` in `dags/dw_bert_ausd_v_ta_c_bfc.py` with your actual GCP project ID.
    *   Update `DATAPROC_REGION` in `dags/dw_bert_ausd_v_ta_c_bfc.py` with the desired GCP region for your Dataproc cluster.

2.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running in your GCP project.
    *   Verify that the `apache-airflow-providers-google` package is installed in your Composer environment.

3.  **Dataproc Cluster Provisioning:**
    *   Provision a Dataproc cluster (or configure an ephemeral cluster setup if preferred).
    *   Update `DATAPROC_CLUSTER_NAME` in `dags/dw_bert_ausd_v_ta_c_bfc.py` with the name of your Dataproc cluster.

4.  **Google Cloud Storage (GCS) Setup:**
    *   Create a GCS bucket (e.g., `your-project-dataproc-scripts`).
    *   Update `GCS_BUCKET_NAME` in `dags/dw_bert_ausd_v_ta_c_bfc.py` with the name of your GCS bucket.
    *   Create a folder named `pyspark_scripts/` within this GCS bucket.
    *   Upload the `pyspark_scripts/r_ausd_v_ta_c_bfc.py` file to `gs://YOUR_BUCKET_NAME/pyspark_scripts/`.

5.  **IAM and Permissions:**
    *   **Cloud Composer Service Account:** Grant the Service Account associated with your Cloud Composer environment the following roles:
        *   `Dataproc Editor` (or custom role with `dataproc.jobs.create`, `dataproc.jobs.get`, `dataproc.jobs.update`, `dataproc.jobs.delete`).
        *   `Storage Object Viewer` (to read the PySpark script from GCS).
    *   **Dataproc Cluster Service Account:** Grant the Service Account used by your Dataproc cluster the necessary permissions to:
        *   Read from source data systems (e.g., `BigQuery Data Viewer`, `Storage Object Viewer`).
        *   Write to target data systems (e.g., `BigQuery Data Editor`, `Storage Object Creator`).
        *   Access any other GCP resources required by the PySpark script.

6.  **Connection Strings and Secrets:**
    *   If the PySpark script requires access to external databases or APIs, ensure that connection details, credentials, or API keys are securely managed (e.g., using Google Secret Manager) and configured to be accessible by the Dataproc job.

7.  **Scheduling:**
    *   The DAG is currently `schedule=None`. If the original UC4 job had a specific schedule, update the `schedule` parameter in `dags/dw_bert_ausd_v_ta_c_bfc.py` accordingly (e.g., `schedule_interval="0 5 * * *" for daily at 5 AM UTC`).

8.  **PySpark Script Completion:**
    *   The `pyspark_scripts/r_ausd_v_ta_c_bfc.py` is a placeholder. The complete business logic from the original `r_ausd_v_ta_c_bfc.ksh` KornShell script must be fully implemented, tested, and optimized within this PySpark file.

## 5. Known Gaps & Unresolved References

*   **KornShell Script Logic Analysis (B3/B4 Item):** The detailed business logic within the original `r_ausd_v_ta_c_bfc.ksh` KornShell script is currently unknown. A thorough analysis and manual re-implementation in PySpark is required. This is the most significant unresolved item and falls into the B3/B4 (manual redesign) category.
*   **UC4 Include Object Functionality:** The exact purpose and functionality of UC4 objects `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` need to be fully understood. Their equivalent functionality (e.g., environment variable setup, common utility functions, logging configuration) must be integrated into the Airflow DAG or PySpark script.
*   **Dataproc Cluster Strategy Finalization:** The decision on whether to use a persistent Dataproc cluster or implement an ephemeral cluster creation/deletion strategy needs to be finalized. The current DAG assumes a pre-existing cluster.
*   **Detailed Production Scheduling:** While `schedule=None` is a temporary measure, the precise production schedule of the original UC4 job (if any) must be identified and configured in the Airflow DAG.
*   **Retry and SLA Policies:** The current DAG has `retries=0`. A comprehensive retry strategy, including backoff and maximum retry attempts, along with Service Level Agreement (SLA) definitions, should be established and configured based on business requirements.
*   **Dataproc Job Specific Parameters:** Any specific Dataproc job configurations (e.g., driver memory, executor cores, custom Spark properties, network tags) required for optimal performance or resource allocation need to be identified and added to the `DataprocSubmitJobOperator`.
*   **External System Interactions:** Any interactions the original KornShell script had with external databases, APIs, or other systems must be re-engineered using appropriate GCP services or client libraries within the PySpark script.

## 6. Validation

To ensure the migrated job functions correctly:

**How to Run Tests:**

1.  **Airflow DAG Syntax Check:**
    *   Before deploying, run `python dags/dw_bert_ausd_v_ta_c_bfc.py` locally to check for basic Python syntax errors.
    *   Use `airflow dags parse <DAG_FILE_PATH>` (if Airflow CLI is configured locally) for a more comprehensive syntax check.
2.  **Deploy and Trigger in Cloud Composer:**
    *   Upload `dags/dw_bert_ausd_v_ta_c_bfc.py` to your Cloud Composer DAGs folder.
    *   In the Airflow UI, unpause the `dw_bert_ausd_v_ta_c_bfc` DAG.
    *   Manually trigger the DAG from the Airflow UI.
3.  **Monitor Dataproc Job:**
    *   Observe the Dataproc job execution in the GCP Console (Dataproc -> Jobs) or via Cloud Logging.
4.  **PySpark Script Local/Dev Testing:**
    *   Thoroughly test the `pyspark_scripts/r_ausd_v_ta_c_bfc.py` script in a development environment (e.g., local Spark installation, small Dataproc cluster) with representative data before deploying to production.

**What "Passing" Means:**

*   **Airflow DAG Success:** The `dw_bert_ausd_v_ta_c_bfc` DAG run completes successfully in the Airflow UI (green status).
*   **Task Success:** The `run_dw_bert_ausd_v_ta_c_bfc` task within the DAG completes successfully.
*   **Dataproc Job Success:** The Dataproc job submitted by Airflow completes successfully without errors or warnings.
*   **Functional Correctness:** The PySpark script executes its business logic as intended, producing the correct output, updating the target systems accurately, and ensuring the "contract extension period caching" is updated to match the expected outcome of the original UC4 job. This requires comparing results with the legacy system.
*   **Logging and Monitoring:** Cloud Logging shows no unexpected errors, exceptions, or critical warnings from either the Airflow task or the Dataproc job. Cloud Monitoring metrics (e.g., Dataproc job duration, resource utilization) are within expected bounds.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, follow this rollback procedure:

1.  **Immediate Action (GCP):**
    *   **Pause DAG:** Immediately pause the `dw_bert_ausd_v_ta_c_bfc` DAG in the Airflow UI to prevent any further executions.
    *   **Stop Running Instances:** If a DAG run is currently in progress, attempt to mark the running task as failed or clear it to stop its execution.
2.  **Revert to Original System:**
    *   **Re-enable UC4 Job:** Re-enable and/or re-schedule the original UC4 job `DW.BERT_AUSD_V_TA_C_BFC` in the UC4/Automic environment.
    *   **Verify Original Functionality:** Confirm that the original UC4 job can execute successfully and perform its intended function without issues.
3.  **Cleanup (GCP - Optional):**
    *   **Remove DAG:** Delete the `dags/dw_bert_ausd_v_ta_c_bfc.py` file from the Cloud Composer DAGs folder.
    *   **Remove PySpark Script:** (Optional, if not needed for future re-migration attempts) Delete the `pyspark_scripts/r_ausd_v_ta_c_bfc.py` from the GCS bucket.
    *   **Decommission Resources:** (Optional, if specifically provisioned for this job) Decommission any Dataproc clusters or other GCP resources that were exclusively set up for this migrated job.
4.  **Root Cause Analysis:**
    *   Thoroughly investigate the reason for the rollback (e.g., data integrity issues, performance degradation, unexpected errors, security concerns).
    *   Address all identified issues in the Airflow DAG, PySpark script, or GCP infrastructure configuration before attempting re-deployment.

```