# MIGRATION_NOTES.md

## 1. Summary
This document details the migration of the legacy UC4 Job Plan `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` and its associated Unix job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and Cloud Dataproc.

*   **Source Platform:** UC4 / Automic Workload Automation
*   **Target Platform:** GCP Cloud Composer (Apache Airflow) & Cloud Dataproc (PySpark execution)
*   **Workflow Purpose:** Coordinates the daily setup and maintenance of the Plato Mapping table, linking Plato system tariffs with core Data Warehouse (DWH) base tariffs.

---

## 2. Generated Artifacts
The migration process generated two core files, preserving the original folder structure hierarchy:

1.  **Orchestration DAG:**
    *   **File Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_plato_tarif_mapping_taeglich_jp.py`
    *   **Role:** Defines the Airflow DAG structure, handles environment-specific variable resolution, configures the Dataproc task, and implements the failure alerting callback.
2.  **Workload Execution Script:**
    *   **File Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
    *   **Role:** A PySpark script executed on Dataproc. It acts as the processing boundary/stub, preserving the exact functional output and logging behavior of the legacy Unix job.

---

## 3. Key Design Decisions

### Concurrency Control (Sync Object Simulation)
*   **Decision:** Set `max_active_runs=1` in the Airflow DAG definition.
*   **Reasoning:** The legacy UC4 workflow referenced a sync object `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` with an `Else="Wait"` condition. Restricting the DAG to a single active run safely emulates this mutual exclusion behavior within Airflow without requiring complex external database locks.

### Dynamic Dataproc Job IDs
*   **Decision:** Used a Jinja-templated string for the Dataproc job ID: `"dw_dwh_dummy_absd_plato_tarife_{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"`.
*   **Reasoning:** Dataproc requires job IDs to be unique within a region. Appending a sanitized Airflow `run_id` prevents job ID collisions during manual re-runs or backfills.

### Literal Translation Compliance
*   **Decision:** The PySpark script prints the exact string `"Doing nothinig"` (preserving the original German-English typo).
*   **Reasoning:** Strict adherence to the legacy logic ensures that automated log parsers or verification scripts looking for specific legacy stdout patterns do not fail.

### Environment-Specific Configuration Resolution
*   **Decision:** Implemented a hierarchical fallback mechanism for GCP configuration variables (Project, Region, Cluster, Bucket). The code attempts to resolve variables from Airflow Variables first, then OS Environment variables, and finally falls back to hardcoded placeholders.
*   **Reasoning:** This approach ensures local development flexibility while maintaining compatibility with standard Cloud Composer environment configurations.

---

## 4. Manual Steps Before Go-Live

### Schema and Dataset Creation
1.  Ensure that the target Plato Mapping table structures exist in your target data store (e.g., BigQuery or Cloud Spanner) if downstream processes depend on them.

### IAM & Permissions
1.  The Cloud Composer/Airflow worker Service Account must be granted the following IAM roles:
    *   `roles/dataproc.editor` (to submit jobs to the Dataproc cluster)
    *   `roles/storage.objectViewer` (to read the PySpark script from the GCS bucket)

### Connection Strings & Secrets
1.  Configure the following Airflow Variables in the Cloud Composer UI or via the `gcloud` CLI:
    *   `GCP_PROJECT`: Your target GCP Project ID.
    *   `GCP_REGION`: The GCP region where your Dataproc cluster resides.
    *   `DATAPROC_CLUSTER_NAME`: The name of your active Dataproc cluster.
    *   `GCS_BUCKET_NAME`: The GCS bucket where the PySpark script will be uploaded.

### Script Deployment
1.  Upload the generated PySpark script to your GCS bucket:
    ```bash
    gsutil cp uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py \
      gs://<YOUR_GCS_BUCKET_NAME>/pyspark_scripts/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
    ```

### Scheduling
1.  The DAG is currently configured with `schedule_interval=None` because no `EVNT_TIME` trigger was defined in the source UC4 XML.
2.  **Action Required:** Align with your enterprise scheduling team to define the appropriate cron schedule or upstream dataset trigger, and update the `schedule_interval` parameter in the DAG file accordingly.

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) Items & Cross-Job Sync
*   **Unresolved Sync Object:** The sync object `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` was not provided as a physical file in the source migration package.
*   **Risk:** While `max_active_runs=1` protects this specific DAG from self-concurrency, it does *not* protect against concurrent executions of other, separate DAGs that might have shared the same legacy UC4 Sync Object.
*   **Mitigation:** If other migrated workflows share this synchronization boundary, you must implement a cross-DAG sensor (`ExternalTaskSensor`) or use a shared state lock (e.g., via a BigQuery metadata table or Firestore).

### Alerting Integration
*   The `on_failure_alarm` function currently prints a standardized warning message to stdout. This must be integrated with your organization's enterprise alerting channels (e.g., Slack, email, PagerDuty, or Google Cloud Monitoring).

---

## 6. Validation

### Local/Development Environment Validation
1.  **DAG Syntax Check:** Run a syntax check on the DAG file:
    ```bash
    python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_plato_tarif_mapping_taeglich_jp.py
    ```
    *Passing criteria:* The command exits with code `0` and no syntax or import errors.

2.  **PySpark Script Check:** Run the PySpark script locally or in a test environment:
    ```bash
    python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
    ```
    *Passing criteria:* The console outputs:
    ```text
    [INFO] Starting dw_dwh_dummy_absd_plato_tarife workload processing...
    [INFO] Doing nothinig
    [INFO] Workload processing finished successfully.
    ```

### Airflow Environment Validation
1.  Upload the DAG file to the Composer `/dags` folder.
2.  Trigger the DAG manually from the Airflow UI.
3.  **Passing Criteria:**
    *   The `start` task completes instantly.
    *   The `dw_dwh_dummy_absd_plato_tarife` task successfully submits a Dataproc job, executes, and completes with a `success` status.
    *   The Dataproc job logs show the exact string: `Doing nothinig`.
    *   The `end` task completes successfully.

---

## 7. Rollback Procedure
In the event of a deployment failure or unexpected behavior in production, execute the following rollback steps:

1.  **Pause the Airflow DAG:**
    *   Immediately pause the DAG `dw_dwh_plato_tarif_mapping_taeglich_jp` in the Airflow UI or via CLI:
        ```bash
        gcloud composer environments run <ENVIRONMENT_NAME> \
          --location <LOCATION> \
          dags pause -- dw_dwh_plato_tarif_mapping_taeglich_jp
        ```
2.  **Remove DAG and Script Artifacts:**
    *   Delete the DAG file from the Composer GCS bucket:
        ```bash
        gsutil rm gs://<COMPOSER_DAG_BUCKET>/dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_plato_tarif_mapping_taeglich_jp.py
        ```
    *   Delete the PySpark script from the execution bucket:
        ```bash
        gsutil rm gs://<GCS_BUCKET_NAME>/pyspark_scripts/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
        ```
3.  **Re-enable Legacy UC4 Execution:**
    *   Re-activate the legacy UC4 Job Plan `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` in the Automic UI to resume legacy scheduling.