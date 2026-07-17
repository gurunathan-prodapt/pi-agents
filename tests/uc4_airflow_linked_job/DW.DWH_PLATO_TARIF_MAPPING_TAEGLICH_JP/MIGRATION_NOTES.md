# Migration Notes: `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`

This document provides comprehensive technical details regarding the migration of the UC4 Unix job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Platform (GCP) and Apache Airflow (Cloud Composer).

---

## 1. Summary

The legacy UC4 object `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated from an on-premises UC4 scheduler environment to a modern, cloud-native orchestration pipeline on **Google Cloud Composer (Apache Airflow)** and **Google Cloud Dataproc**.

### Source vs. Target Mapping
* **Source Platform:** UC4 / Automic Engine
* **Source Object Type:** `JOBS_UNIX` (Active)
* **Source Job Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
* **Target Platform:** Google Cloud Composer (Airflow 2.x) & Google Cloud Dataproc
* **Target DAG ID:** `dw_dwh_dummy_absd_plato_tarife`
* **Target Execution Engine:** PySpark on Dataproc Serverless / Managed Cluster

### Functional Purpose
This job serves as an operational utility, synchronization checkpoint, and placeholder step within the larger daily Plato tariff mapping sequence (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It executes a non-operational dummy command and returns a success state immediately to allow downstream tasks to proceed.

---

## 2. Generated Artifacts

The migration process generated two primary files, maintaining the exact directory structure of the source repository:

### 1. Airflow DAG Definition File
* **File Path:** `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`
* **Role:** Defines the Airflow DAG structure, imports environment variables dynamically, configures execution parameters, and schedules the Dataproc job submission.

### 2. PySpark Execution Script
* **File Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
* **Role:** The actual script executed on the Dataproc cluster. It initializes a standard Spark Session and outputs the legacy print statement to preserve execution logs.

---

## 3. Key Design Decisions

### Dataproc PySpark vs. Local Bash Operator
* **Decision:** The job is implemented using `DataprocSubmitJobOperator` pointing to a PySpark script, rather than a lightweight local `BashOperator` or `PythonOperator`.
* **Reasoning:** This preserves architectural consistency across the entire `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` workflow. While a local operator would consume fewer resources, utilizing Dataproc ensures that logging, IAM permissions, and execution patterns remain uniform across all tasks in the pipeline.
* **Trade-off:** Slightly higher latency during task startup (Dataproc job submission overhead) in exchange for strict architectural compliance and unified monitoring.

### Dynamic Environment Configuration
* **Decision:** Hardcoded environment variables (Project IDs, Region, Bucket Names, Cluster Names) have been completely eliminated.
* **Reasoning:** Adheres strictly to the **ENV VARIABLE POLICY**. The DAG dynamically fetches these values at runtime using Airflow Variables (`Variable.get(...)`), allowing the same code to run unmodified across Development, UAT, and Production environments.

### Preservation of Legacy Logging (Output/Print Literal Rule)
* **Decision:** The PySpark script explicitly prints `"Doing nothinig"` (including the original typo).
* **Reasoning:** Adheres to the **OUTPUT/PRINT LITERAL RULE**. Preserving exact legacy log outputs ensures that automated log parsers, verification scripts, or operational runbooks do not fail due to missing or altered log signatures.

### Concurrency and Scheduling
* **Decision:** `schedule=None` and `max_active_runs=1`.
* **Reasoning:** This job is a single step within a larger parent workflow. It must not run on an independent cron schedule. Setting `max_active_runs=1` mirrors the legacy UC4 sync behavior, preventing concurrent overlapping runs of the same task.

---

## 4. Manual Steps Before Go-Live

Before enabling or triggering this DAG in a production environment, the following manual setup steps must be completed:

### 1. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in the target Cloud Composer environment:
* `GCP_PROJECT`: The target GCP Project ID (e.g., `prod-dwh-gcp-1234`).
* `GCP_REGION`: The target GCP region where Dataproc is running (e.g., `europe-west3`).
* `GCS_BUCKET`: The GCS bucket name used for staging scripts (e.g., `prod-dwh-composer-bucket`).
* `DATAPROC_CLUSTER`: The name of the active Dataproc cluster (e.g., `dwh-pyspark-cluster`).

### 2. GCS Artifact Deployment
Upload the PySpark execution script to the designated GCS bucket:
* **Source Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
* **Target GCS URI:** `gs://<YOUR_BUCKET_NAME>/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py`

### 3. IAM & Permissions
Ensure that the Cloud Composer worker service account has the following permissions:
* `roles/dataproc.editor` (To submit jobs to the Dataproc cluster)
* `roles/storage.objectViewer` (To read the PySpark script from the GCS bucket)

### 4. Parent DAG Integration
Because this DAG is configured with `schedule=None`, ensure that the parent DAG (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) is configured to trigger this child DAG using a `TriggerDagRunOperator`:
```python
trigger_dummy_job = TriggerDagRunOperator(
    task_id="trigger_dw_dwh_dummy_absd_plato_tarife",
    trigger_dag_id="dw_dwh_dummy_absd_plato_tarife",
    wait_for_completion=True,
    deferrable=True,
    dag=parent_dag,
)
```

---

## 5. Known Gaps & Unresolved References

* **Unmigrated Parent Pipeline:** This DAG cannot be fully validated in an automated end-to-end integration test until the parent pipeline (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) is completely migrated and deployed to the same environment.
* **Resource Optimization Opportunity (B4 Redesign Item):** If resource consumption on Dataproc becomes an operational concern, this job is an ideal candidate for redesign. It can be safely converted to a local Airflow `BashOperator` running `echo "Doing nothinig"` or a `PythonOperator` executing a simple log statement, completely bypassing Dataproc cluster overhead.

---

## 6. Validation

To validate the migration of this job, execute the following testing steps:

### 1. DAG Syntax and Parsing Test
Run this command within your local development environment or Composer CLI to ensure there are no Python syntax or Airflow DAG import errors:
```bash
python3 dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
```

### 2. Manual Execution Test
1. Navigate to the Airflow Web UI.
2. Locate the DAG `dw_dwh_dummy_absd_plato_tarife`.
3. Unpause the DAG.
4. Click **Trigger DAG**.

### 3. Success Criteria
The test run is considered **passing** if:
* The DAG run transitions to a `SUCCESS` state.
* The `dwh_dummy_absd_plato_tarife` task completes successfully.
* The Dataproc job logs in Google Cloud Logging show the successful initialization of the Spark Session.
* The stdout logs of the Dataproc job contain the exact string:
  ```text
  Doing nothinig
  ```

---

## 7. Rollback Procedure

In the event of an operational failure or unexpected behavior during go-live, follow these rollback steps:

1. **Pause the Airflow DAG:**
   Immediately pause the DAG in the Airflow UI or via the CLI to prevent further executions:
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
       --location <LOCATION> \
       dags pause -- dw_dwh_dummy_absd_plato_tarife
   ```
2. **Disable Parent Trigger:**
   If the parent DAG has already been migrated, temporarily disable or bypass the `TriggerDagRunOperator` pointing to this DAG in the parent pipeline.
3. **Revert to Legacy Scheduler:**
   Re-enable the active flag (`Active="1"`) for the `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` job in the UC4 engine to restore legacy scheduling control.