# MIGRATION NOTES: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Composer (Apache Airflow).

---

## 1. Summary

The legacy UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to an Apache Airflow DAG running on Google Cloud Composer. 

In the legacy system, this job functioned as a daily dummy/placeholder task within the broader Plato tariff mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It performed no operational data transformations or Ab Initio graph executions; its sole action was printing a placeholder message to the console. 

To preserve orchestration lineage while minimizing cloud resource consumption, this job has been converted into a lightweight Airflow DAG.

* **Source Platform:** UC4 (Automic) UNIX Job (`JOBS_UNIX`)
* **Target Platform:** Google Cloud Composer (Apache Airflow)
* **Target DAG ID:** `dw_dwh_plato_tarif_mapping_taeglich_dag`
* **Target Task ID:** `dw_dwh_dummy_absd_plato_tarife`

---

## 2. Generated Artifacts

The migration process generated the following file:

| Target File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Python | The Airflow DAG definition file. It defines the DAG structure, environment configuration, and executes the dummy print task using a `PythonOperator`. |

---

## 3. Key Design Decisions

### Lightweight `PythonOperator` vs. `DataprocSubmitJobOperator`
* **Decision:** The initial automated migration design suggested mapping this task to a `DataprocSubmitJobOperator` executing a placeholder PySpark script on Google Cloud Dataproc. This was rejected during the design review.
* **Reasoning:** Submitting a Dataproc job solely to print a log message introduces significant overhead, including cluster execution delays, GCS file management, and unnecessary compute costs. 
* **Trade-off:** Implementing a native Airflow `PythonOperator` executes the print statement instantly within the Airflow worker environment, bypassing Dataproc entirely while achieving identical functional parity.

### Verbatim Log Parity (Output/Print Literal Rule)
* **Decision:** The original UC4 script printed the misspelled string `"Doing nothinig"`. This exact spelling has been preserved in the Python execution block.
* **Reasoning:** Maintaining exact character-for-character log parity prevents breaking any legacy log-scraping, monitoring, or automated validation tools that might scan task outputs for this specific string.

### Global Sourcing of Environment Variables
* **Decision:** Infrastructure-level variables (`GCP_PROJECT` and `GCP_REGION`) are retrieved dynamically from the Airflow Variable store (`Variable.get`) rather than being hardcoded.
* **Reasoning:** This ensures environment portability, allowing the same DAG file to run unmodified across Development, UAT, and Production Composer environments.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this DAG in a production environment, the following manual setup steps must be completed:

### 1. Airflow Variables Configuration
Ensure the following global variables are defined in the target Airflow environment's Variable store:
* `GCP_PROJECT`: The ID of your Google Cloud Project.
* `GCP_REGION`: The target GCP region (e.g., `europe-west3`).

### 2. IAM & Permissions
* The Cloud Composer environment's service account must have standard worker execution permissions. Because this task does not interact with external GCP resources (like BigQuery or Dataproc), no specialized service account roles are required for this specific DAG.

### 3. Scheduling & Integration
* **Current Schedule:** The DAG is currently configured with `schedule=None`.
* **Action Required:** Because the parent workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) has not yet been migrated, this DAG must be triggered manually or integrated into the parent DAG once the parent migration is complete.

---

## 5. Known Gaps & Unresolved References

### 1. Missing Parent Workflow (Unmigrated Upstream/Downstream)
* **Gap:** The parent job chain `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` is not yet migrated. 
* **Resolution:** This DAG is currently configured to run standalone. Once the parent workflow is migrated, this task should either be merged directly into the parent DAG file or triggered via a `TriggerDagRunOperator` from the parent orchestrator.

### 2. Future Operational Redesign (B4 Redesign Item)
* **Gap:** If this dummy task is a placeholder for future business logic (e.g., a future Plato tariff mapping PySpark job), the `PythonOperator` will need to be replaced.
* **Resolution:** If actual processing logic is introduced, developers must redesign this task to use the `DataprocSubmitJobOperator` (or another appropriate operator) and point it to the actual PySpark/SQL execution scripts.

---

## 6. Validation

To validate the migration of this task, perform the following steps:

### Execution Test
1. Upload `dw_dwh_dummy_absd_plato_tarife.py` to the Cloud Composer DAGs folder.
2. Navigate to the Airflow UI and locate `dw_dwh_plato_tarif_mapping_taeglich_dag`.
3. Unpause the DAG.
4. Trigger the DAG manually by clicking the **Play** button.

### Success Criteria
The validation is considered **passing** if:
1. The DAG run completes with a status of `Success`.
2. The task `dw_dwh_dummy_absd_plato_tarife` completes successfully in under 5 seconds.
3. The task logs contain the exact string:
   ```text
   Doing nothinig
   ```

---

## 7. Rollback Procedure

In the event of an issue or deployment failure, execute the following rollback steps:

1. **Pause the DAG:** In the Airflow UI, toggle the DAG switch for `dw_dwh_plato_tarif_mapping_taeglich_dag` to **Off** (Paused).
2. **Remove the Artifact:** Delete the DAG file from the Cloud Composer GCS bucket:
   ```bash
   gcloud storage rm gs://<your-composer-bucket>/dags/dw_dwh_dummy_absd_plato_tarife.py
   ```
3. **Re-enable Legacy Job:** If the legacy UC4 environment is still active, ensure the active flag for `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is set to active (`1`) in the Automic UI to resume legacy scheduling.