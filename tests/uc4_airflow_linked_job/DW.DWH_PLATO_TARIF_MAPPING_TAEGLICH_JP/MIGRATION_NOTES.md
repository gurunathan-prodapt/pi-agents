# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Platform (GCP). 

*   **Source System**: UC4 (Automic) UNIX Job (`JOBS_UNIX`), active flag `1` (Active).
*   **Legacy Role**: A utility, control, or marker step within the Plato Tarif Mapping Daily workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It executed a basic print statement (`"Doing nothinig"`) to coordinate execution, synchronize downstream steps, or serve as a restart point.
*   **Target Platform**: Google Cloud Platform (GCP).
*   **Target Orchestrator**: Cloud Composer (Apache Airflow).
*   **Target Execution Engine**: Cloud Dataproc (Serverless or Standard Cluster running PySpark).

---

## 2. Generated Artifacts

The migration process has generated two primary files, maintaining strict folder structure fidelity relative to the source repository:

### 1. Airflow DAG File
*   **Path**: `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
*   **Role**: Defines the Airflow DAG (`dw_dwh_dummy_absd_plato_tarife`), imports environment variables, configures the execution parameters, and orchestrates the task sequence (`start >> dwh_dummy_absd_plato_tarife >> end`).

### 2. PySpark Execution Script
*   **Path**: `gcs/scripts/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dwh_dummy_absd_plato_tarife_job.py`
*   **Role**: Implements the actual operational logic. It instantiates a `SparkSession` to align with target platform execution patterns and reproduces the exact legacy output.

---

## 3. Key Design Decisions

*   **Dataproc PySpark Wrapper for Dummy Logic**: Although the legacy job only executed a shell print statement, it was migrated as a Dataproc PySpark job (`DataprocSubmitJobOperator`) rather than a local Airflow `BashOperator` or `PythonOperator`. This maintains architectural consistency across the Plato Tarif Mapping pipeline, ensuring all tasks run within the same compute boundary (Dataproc) and share identical IAM, logging, and networking configurations.
*   **Dynamic Environment Configuration**: Hardcoded environment variables have been eliminated. The DAG dynamically pulls infrastructure-wide constants (Project ID, Region, Cluster Name, and GCS Bucket) from Airflow Variables.
*   **Output/Print Literal Rule**: The legacy print statement contained a typo (`"Doing nothinig"`). To guarantee absolute code integrity and prevent breaking downstream log-scraping tools or verification scripts, this spelling has been preserved exactly in the PySpark script.
*   **Idempotency & Unique Job IDs**: The `DataprocSubmitJobOperator` uses a dynamic `job_id` string template (`dw_dwh_dummy_absd_plato_tarife_{{ ds_nodash }}_{{ hms_triggered }}`) to prevent job ID collisions during manual retries or backfills.

---

## 4. Manual Steps Before Go-Live

The following setup must be completed in the target environment before enabling or triggering the migrated DAG:

### Schema & Dataset Creation
*   Ensure that the Google Cloud Storage (GCS) bucket defined in your Airflow variables exists.
*   Create the target directory structure in GCS and upload the PySpark script:
    ```bash
    gsutil cp dwh_dummy_absd_plato_tarife_job.py gs://<YOUR_GCS_BUCKET_NAME>/scripts/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dwh_dummy_absd_plato_tarife_job.py
    ```

### IAM & Permissions
*   The Cloud Composer Service Account must have the **Dataproc Editor** (`roles/dataproc.editor`) and **Service Account User** (`roles/iam.serviceAccountUser`) roles.
*   The Cloud Composer Service Account must have **Storage Object Viewer** (`roles/storage.objectViewer`) permissions on the GCS bucket containing the PySpark script.

### Connection Strings & Secrets
*   No external database connection strings or secrets are required for this dummy job.

### Airflow Variables (Scheduling & Environment)
Ensure the following keys are registered in the Cloud Composer/Airflow Variables dashboard:

| Variable Key | Expected Value Example |
| :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` |
| `GCP_REGION` | `europe-west3` |
| `DATAPROC_CLUSTER_NAME` | `my-dataproc-cluster` |
| `GCS_BUCKET_NAME` | `my-composer-environment-bucket` |

### Scheduling & Parent Integration
*   The DAG is configured with `schedule=None`. 
*   To integrate this into the daily schedule, it must be linked to the parent orchestrator DAG `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` using a `TriggerDagRunOperator` or consolidated directly into the parent DAG file.

---

## 5. Known Gaps & Unresolved References

*   **Cross-DAG Dependency Validation**: Because this job is triggered by a parent workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`), its standalone execution succeeds, but the end-to-end orchestration cannot be fully validated until the parent DAG is completely migrated and deployed.
*   **Redesign (B4) Items**: None. The job is a simple control marker and does not require complex architectural redesign.

---

## 6. Validation

### How to Run the Tests
1.  **DAG Parse Test**: Verify that the DAG is syntactically correct and loads in Airflow without errors:
    ```bash
    python3 dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
    ```
2.  **Manual Trigger**: Trigger the DAG manually from the Airflow UI or via the gcloud CLI:
    ```bash
    gcloud composer environments run <COMPOSER_ENV_NAME> \
        --location <COMPOSER_REGION> \
        dags trigger -- dw_dwh_dummy_absd_plato_tarife
    ```

### What "Passing" Means
*   The DAG transitions to a `SUCCESS` state.
*   The Dataproc job completes successfully with an exit code of `0`.
*   The Dataproc driver logs (accessible via Cloud Logging or the Dataproc UI) contain the exact string:
    ```
    Doing nothinig
    ```

---

## 7. Rollback Procedure

In the event of an issue or deployment failure, perform the following steps to roll back:

1.  **Pause the Airflow DAG**:
    *   Navigate to the Airflow UI and toggle the switch for `dw_dwh_dummy_absd_plato_tarife` to **Off**.
    *   Alternatively, run:
        ```bash
        gcloud composer environments run <COMPOSER_ENV_NAME> \
            --location <COMPOSER_REGION> \
            dags pause -- dw_dwh_dummy_absd_plato_tarife
        ```
2.  **Remove Artifacts**:
    *   Delete the DAG file from the Composer DAGs folder.
    *   Delete the PySpark script from the GCS bucket.
3.  **Revert to Legacy**:
    *   Ensure the UC4 active flag for `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is set back to `1` (Active) in the UC4 engine to resume legacy scheduling.