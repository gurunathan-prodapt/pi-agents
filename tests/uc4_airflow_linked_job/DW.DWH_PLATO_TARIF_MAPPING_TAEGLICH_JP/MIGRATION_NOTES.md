# MIGRATION NOTES: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

---

## 1. Summary

This document details the migration of the legacy UC4 Unix Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Platform (GCP). 

* **Source Component**: UC4 Unix Job (`JOBS_UNIX`) with login `DW.UNIX.ISTNS` on host `|DWHDWH1P|HOST`.
* **Target Platform**: Google Cloud Platform (GCP).
* **Orchestration**: Cloud Composer (Apache Airflow).
* **Execution**: Cloud Dataproc (PySpark/Python execution node).
* **Purpose**: This job is a dummy/placeholder step originally used for synchronization, routing, or checkpointing within the daily mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It performs no actual data processing and prints a placeholder message.

---

## 2. Generated Artifacts

The migration process generated two primary files, located in the target repository structure:

1. **Airflow DAG File**
   * **Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
   * **Role**: Defines the Cloud Composer DAG (`dw_dwh_dummy_absd_plato_tarife`), sets up environment variables, and orchestrates the execution of the dummy task using the `DataprocSubmitJobOperator`.
2. **PySpark Execution Script**
   * **Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py`
   * **Role**: A lightweight Python script executed on the Dataproc cluster that preserves the legacy print statement and exits cleanly.

---

## 3. Key Design Decisions

* **Dataproc Execution Pattern**: 
  * *Decision*: The dummy job is mapped to a `DataprocSubmitJobOperator` running a PySpark script, rather than a simple Airflow `EmptyOperator` or local `PythonOperator`.
  * *Reasoning*: This preserves the architectural pattern of the legacy UNIX-to-Dataproc migration. It ensures that the job runs under the same execution identity (GCP Service Account) and on the same compute infrastructure (Dataproc cluster) as other migrated UNIX jobs.
  * *Trade-off*: Submitting a Dataproc job introduces minor execution overhead (job startup and tracking) for a task that only prints a string. If performance optimization becomes critical, this can be simplified to an `EmptyOperator` in a future refactoring phase.
* **Verbatim Output Preservation**:
  * *Decision*: The PySpark script retains the exact, misspelled print statement from the legacy UC4 script: `"Doing nothinig"`.
  * *Reasoning*: Adheres strictly to the **OUTPUT/PRINT LITERAL RULE** to ensure log-level parity and prevent breaking any legacy log-scraping or validation tools.

---

## 4. Manual Steps Before Go-Live

Before activating this DAG in production, the following environment-specific configurations and deployments must be completed:

### A. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in the Cloud Composer environment:
* `GCP_PROJECT`: The target GCP Project ID.
* `GCP_REGION`: The GCP Region where the Dataproc cluster resides.
* `DATAPROC_CLUSTER`: The name of the active Dataproc cluster.
* `GCS_BUCKET`: The Cloud Storage bucket name used for staging scripts.

### B. GCS Artifact Deployment
Upload the PySpark execution script to the designated Cloud Storage path:
```bash
gsutil cp uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py \
  gs://<YOUR_GCS_BUCKET>/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py
```

### C. IAM & Permissions
* Ensure the Cloud Composer worker service account has the **Dataproc Editor** (`roles/dataproc.editor`) and **Storage Object Viewer** (`roles/storage.objectViewer`) roles.
* Ensure the Dataproc VM Service Account has read access to the GCS bucket path containing the script.

### D. Scheduling & Connection Strings
* **Connection**: The DAG uses the default Airflow GCP connection `google_cloud_default`. Verify this connection is correctly configured in Airflow.
* **Scheduling**: The DAG is currently configured with `schedule=None`. It must be triggered manually or integrated into the parent workflow scheduler once migrated.

---

## 5. Known Gaps & Unresolved References

* **Missing Parent Orchestration (JOBP)**: 
  * *Gap*: Only the `JOBS_UNIX` file was provided. The parent Job Plan (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml`) has not yet been migrated.
  * *Impact*: This DAG is currently standalone. Cross-DAG wiring (e.g., using `TriggerDagRunOperator` or `ExternalTaskSensor`) is required to integrate this step into the daily execution chain once the parent workflow is migrated.
* **Redesign (B4) Opportunity**: 
  * If downstream jobs do not strictly depend on a Dataproc job ID or specific Dataproc logs for this step, this entire DAG can be deprecated and replaced with a simple `EmptyOperator` inside the parent workflow DAG to save compute overhead.

---

## 6. Validation

To validate the migration of this job, perform the following steps:

1. **DAG Parsing Test**:
   Ensure the DAG is parsed by Airflow without syntax errors:
   ```bash
   python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
   ```
2. **Manual Trigger**:
   Trigger the DAG manually from the Airflow UI or via the gcloud CLI:
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
     --location <LOCATION> \
     dags trigger -- dw_dwh_dummy_absd_plato_tarife
   ```
3. **Success Criteria**:
   * The DAG run transitions to `success`.
   * The task `dw_dwh_dummy_absd_plato_tarife` completes successfully.
   * Dataproc driver logs show the exact output:
     ```text
     Doing nothinig
     ```
   * The process exits with code `0`.

---

## 7. Rollback Procedure

In the event of a failure or the need to revert to the legacy system:

1. **Pause the Airflow DAG**:
   Turn off the active toggle for `dw_dwh_dummy_absd_plato_tarife` in the Airflow UI, or run:
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
     --location <LOCATION> \
     dags pause -- dw_dwh_dummy_absd_plato_tarife
   ```
2. **Re-enable UC4 Job**:
   Ensure the active flag for `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` in the UC4 engine is set to active (`<Active>1</Active>`) and the legacy agent/host is online.
3. **Cleanup (Optional)**:
   Remove the staged PySpark script from GCS to prevent accidental execution:
   ```bash
   gsutil rm gs://<YOUR_GCS_BUCKET>/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py
   ```