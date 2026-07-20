# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## 1. Summary
The legacy UC4/Automic UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to Google Cloud Platform (GCP). 

* **Source Platform**: UC4/Automic (UNIX Job)
* **Target Platform**: Google Cloud Composer (Apache Airflow) & Cloud Dataproc
* **Description**: This job serves as a "dummy" step within the Plato tariff mapping daily pipeline (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It performs no operational data transformations but prints a legacy status message to preserve the execution trace and structural orchestration of the legacy system.

---

## 2. Generated Artifacts
The migration process generated two primary files located in the target directory mirroring the legacy structure:

* **Target Directory**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/`

### 1. Airflow DAG File
* **File Path**: `dw_dwh_dummy_absd_plato_tarife_dag.py`
* **Role**: Orchestrates the workflow execution. It defines the DAG structure, sets up boundary markers (`start` and `end`), resolves environment variables at runtime, and schedules the execution of the dummy task via the `DataprocSubmitJobOperator`.

### 2. Companion Python Script
* **File Path**: `dw_dwh_dummy_absd_plato_tarife.py`
* **Role**: Executes the actual logic of the legacy job. It prints the exact legacy status message and exits cleanly with code `0`.

---

## 3. Key Design Decisions

### Verbatim Output Preservation
The original UC4 script contained the statement `:print Doing nothinig` (including the typo "nothinig"). To ensure strict behavioral parity and avoid breaking any downstream log-scraping or validation tools, this literal print statement has been carried over verbatim as `print("Doing nothinig")` in the companion Python script.

### Dynamic Environment Resolution
To adhere to strict environment isolation practices, no environment-specific values (such as GCP Project IDs, GCS Buckets, or Dataproc Cluster names) are hardcoded. All configurations are resolved dynamically at runtime using Airflow Variables:
* `GCP_PROJECT`
* `GCP_REGION`
* `DATAPROC_CLUSTER`
* `GCS_BUCKET`

### Execution Model Parity
The legacy job executed on a UNIX host (`|DWHDWH1P|HOST`) using specific credentials (`DW.UNIX.ISTNS`). In the migrated architecture:
* The UNIX host is replaced by a Cloud Dataproc cluster.
* The UNIX login is mapped to the Cloud Composer/Dataproc Service Account.
* The task is executed via `DataprocSubmitJobOperator` to maintain architectural consistency with other operational jobs in the same pipeline.

---

## 4. Manual Steps Before Go-Live

### 1. GCS Artifact Deployment
Upload the companion Python script to your environment's GCS bucket:
```bash
gsutil cp uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py \
  gs://<YOUR_GCS_BUCKET>/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py
```
*(Replace `<YOUR_GCS_BUCKET>` with your target environment bucket name during deployment).*

### 2. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in your Cloud Composer environment:
* `GCP_PROJECT`: The target GCP Project ID.
* `GCP_REGION`: The GCP region where Dataproc is running (e.g., `europe-west3`).
* `DATAPROC_CLUSTER`: The name of the active Dataproc cluster.
* `GCS_BUCKET`: The GCS bucket name where scripts and assets are stored.

### 3. IAM & Permissions
Ensure that the Cloud Composer worker service account has the following permissions:
* `roles/dataproc.editor` (or a custom role allowing job submission to the target Dataproc cluster).
* `roles/storage.objectViewer` on the GCS bucket containing the companion script.

### 4. Scheduling & Activation
* The DAG is configured with `is_paused_upon_creation=False` to match the active status (`1`) of the source UC4 job.
* The schedule is set to daily at 05:00 UTC (`0 5 * * *`). Verify this matches the business requirements for the Plato tariff mapping pipeline.

---

## 5. Known Gaps & Unresolved References

### 1. Wiring Gap (Parent Job Plan Not Migrated)
* **Status**: The downstream parent Job Plan `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` has **not yet been migrated**.
* **Impact**: This DAG currently stands as a single-node pipeline with boundary `start` and `end` nodes. 
* **Resolution**: Once the parent Job Plan is migrated, this DAG should either be integrated as a task node inside the consolidated parent DAG, or triggered via a `TriggerDagRunOperator` from the parent workflow.

### 2. Redesign (B4) Recommendation: Resource Waste
* **Issue**: Submitting a PySpark job to a Dataproc cluster via `DataprocSubmitJobOperator` solely to print a single string is highly inefficient. It incurs cluster startup/job submission latency (typically 1–3 minutes) and wastes compute resources.
* **Recommendation**: Refactor this task to use a simple Airflow `PythonOperator` running directly on the Composer worker:
  ```python
  from airflow.operators.python import PythonOperator

  def print_dummy():
      print("Doing nothinig")

  dw_dwh_dummy_absd_plato_tarife = PythonOperator(
      task_id='dw_dwh_dummy_absd_plato_tarife',
      python_callable=print_dummy
  )
  ```
  This change should be implemented during the integration phase of the parent Job Plan.

---

## 6. Validation

### 1. Local Syntax & Import Validation
Verify that the DAG parses cleanly without import errors:
```bash
python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_dag.py
```

### 2. Companion Script Validation
Run the companion script locally to verify output:
```bash
python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
```
* **Expected Output**: `Doing nothinig` (with exit code `0`).

### 3. End-to-End Airflow Test
1. Upload the DAG file to the Composer `dags/` folder.
2. Trigger the DAG manually from the Airflow UI.
3. Verify that the task `dw_dwh_dummy_absd_plato_tarife` completes with a `SUCCESS` status.
4. Inspect the Dataproc driver logs for the job and confirm the presence of the exact string:
   ```
   Doing nothinig
   ```

---

## 7. Rollback Procedure

In the event of a deployment failure or unexpected behavior:

1. **Pause the Airflow DAG**:
   Disable the DAG in the Airflow UI or via the CLI:
   ```bash
   airflow dags pause dw_dwh_plato_tarif_mapping_taeglich_jp
   ```
2. **Remove Target Artifacts**:
   Delete the DAG file from the Composer `dags/` folder and the companion script from GCS to prevent accidental execution.
3. **Re-enable Legacy Job**:
   Re-activate the legacy `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` job in the Automic/UC4 scheduler to resume legacy operations.