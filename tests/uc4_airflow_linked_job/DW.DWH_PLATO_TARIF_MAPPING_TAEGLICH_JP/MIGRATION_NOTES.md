# MIGRATION NOTES: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document provides the technical migration details, design decisions, manual setup steps, and validation procedures for migrating the UC4 job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Composer (Airflow).

---

## 1. Summary

The UC4 Unix Job (`JOBS_UNIX`) `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to a Python-based Airflow DAG running on Google Cloud Composer. 

In the legacy UC4 system, this job functioned as a "dummy" synchronization or administrative placeholder step within the larger daily workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`. The original script performed no active system, database, or application operations, containing only a UC4 print directive: `:print Doing nothinig`. 

The migrated Airflow DAG replicates this behavior resource-efficiently using an `EmptyOperator` to act as a zero-compute synchronization barrier.

* **Source Platform:** UC4 (Automic) Engine
* **Target Platform:** Google Cloud Composer (Airflow 2.x)
* **Migration Strategy:** Re-platformed from a Unix dummy job to an Airflow `EmptyOperator` (with an optional `DataprocSubmitJobOperator` configuration preserved in comments for framework compliance).

---

## 2. Generated Artifacts

The migration process generated the following file:

* **`uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`**
  * **Role:** The primary Airflow DAG definition file. It establishes the DAG metadata, imports environment variables, defines the execution flow (`start` -> `dwh_dummy_absd_plato_tarife` -> `end`), and embeds legacy documentation.

---

## 3. Key Design Decisions

### Decision 1: Use of `EmptyOperator` as the Primary Execution Model
* **Reasoning:** The original UC4 script contains no functional code other than a print statement. Executing a full Dataproc cluster job or Kubernetes pod for a no-op task introduces unnecessary latency, resource consumption, and cloud costs. The `EmptyOperator` perfectly mimics the legacy "no-op" behavior with zero execution footprint.
* **Trade-off:** If the target enterprise framework strictly mandates that *every* migrated task must submit a Dataproc job for tracking purposes, this approach bypasses that mechanism. To mitigate this, alternative commented-out code for a `DataprocSubmitJobOperator` has been provided in the DAG file.

### Decision 2: Standalone DAG Modeling due to Missing Context
* **Reasoning:** Only the single `JOBS_UNIX` file was provided for migration; the parent Job Plan (`JOBP`) and Time Event (`EVNT_TIME`) files were unavailable. Consequently, the DAG is configured with `schedule=None` and `start_date=datetime(2026, 3, 30)` (matching the UC4 export metadata timestamp).
* **Trade-off:** The job cannot run on a schedule independently and must be triggered manually or integrated into a parent DAG once the parent workflow is migrated.

### Decision 3: Preservation of Legacy Metadata and Typos
* **Reasoning:** To maintain operational continuity and lineage, the German-language operational note (`Wiederanlauf ohne weitere Maßnahmen möglich`) and the original print statement typo (`Doing nothinig`) have been preserved verbatim within the DAG's markdown documentation (`doc_md`).

---

## 4. Manual Steps Before Go-Live

Before deploying and executing this DAG in a production environment, the following manual steps must be completed:

### 1. Environment Variables Configuration
Ensure the following Airflow Variables are configured in the target Cloud Composer environment:
* `GCP_PROJECT`: The ID of your Google Cloud Project.
* `DATAPROC_REGION`: The GCP region where Dataproc resources are provisioned.
* `DATAPROC_CLUSTER`: The name of the active Dataproc cluster.
* `GCS_BUCKET`: The Cloud Storage bucket used for staging and scripts.

### 2. IAM & Permissions
* Ensure the Cloud Composer environment's service account has the `roles/composer.worker` role.
* If utilizing the alternative Dataproc execution path, the service account must also have `roles/dataproc.editor` and `roles/storage.objectViewer` permissions.

### 3. Alternative Script Deployment (Only if using Option 2)
If your organization requires the alternative `DataprocSubmitJobOperator` path:
1. Create a dummy PySpark/Python script containing:
   ```python
   import sys
   print("Doing nothinig")
   sys.exit(0)
   ```
2. Upload this script to GCS at: `gs://<YOUR_GCS_BUCKET>/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py`
3. Uncomment the Option 2 block in `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` and update the dependency chain to use `dwh_dummy_absd_plato_tarife_alt`.

### 4. Scheduling & Parent Integration
Because this job is a sub-step of `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`, you must decide on the integration pattern:
* **Option A (Consolidated):** Copy the task definition from this file and paste it directly into the parent DAG once migrated.
* **Option B (Modular):** Keep this as a standalone DAG and configure an `ExternalTaskSensor` in the downstream DAG to monitor this DAG's execution state.

---

## 5. Known Gaps & Unresolved References

* **Downstream Pipeline Gap:** The downstream consumer and parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` has not yet been migrated. Running this dummy DAG in isolation will have no functional impact on production data.
* **Redesign (B4) Item:** This standalone DAG should ultimately be deprecated. During the migration of the parent Job Plan (`JOBP`), this task should be absorbed as a node within the master DAG to prevent "DAG bloat" in Cloud Composer.

---

## 6. Validation

To validate the migration, perform the following tests:

### Local/CI Validation
1. Run a syntax and import check on the generated Python file:
   ```bash
   python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
   ```
   * **Passing Criteria:** The command exits with code `0` and no import or syntax errors are output.

### Composer UI Validation
1. Upload the file to the Composer DAGs folder: `gs://<composer-dag-bucket>/dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/`
2. Navigate to the Airflow UI and verify that `dw_dwh_dummy_absd_plato_tarife` appears in the DAG list without any "DAG Import Errors".

### Execution Validation
1. Manually trigger the DAG in the Airflow UI.
2. Monitor the execution of the DAG run.
   * **Passing Criteria:** 
     * The DAG transitions to a `success` state.
     * The `start`, `dwh_dummy_absd_plato_tarife`, and `end` tasks all complete successfully.
     * The execution duration for `dwh_dummy_absd_plato_tarife` is near 0 seconds (if using `EmptyOperator`).
     * Clicking on the `dwh_dummy_absd_plato_tarife` task and viewing the "Details" tab displays the legacy German documentation and metadata correctly.

---

## 7. Rollback Procedure

If issues arise during deployment or integration, execute the following rollback steps:

1. **Pause the DAG:** In the Airflow UI, toggle the active switch for `dw_dwh_dummy_absd_plato_tarife` to **Off** (Paused).
2. **Remove the Artifact:** Delete the Python file from the Composer DAGs bucket:
   ```bash
   gsutil rm gs://<composer-dag-bucket>/dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
   ```
3. **Re-enable UC4 Job:** In the UC4 UI, ensure that the original job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is active (`<Active>1</Active>`) and that the UC4 queue/scheduler is processing tasks for the `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` workflow.