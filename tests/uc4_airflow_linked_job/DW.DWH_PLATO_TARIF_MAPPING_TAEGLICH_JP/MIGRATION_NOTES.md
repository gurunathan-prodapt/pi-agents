# MIGRATION NOTES
**Job Name:** DW.DWH_DUMMY_ABSD_PLATO_TARIFE  
**Migration Pattern:** `UC4_ONLY` (Orchestration Migration)  
**Target Platform:** Cloud Composer (Apache Airflow) & Google Cloud Platform (GCP)

---

## 1. Summary
The UC4 Unix Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to a native Apache Airflow DAG on Cloud Composer. 

In the legacy UC4 environment, this job functioned as a dummy task within the daily Plato Tarif Mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It contained no business logic or Ab Initio processing, serving primarily as a synchronization anchor, placeholder, or manual trigger step. The migration preserves this orchestration role while optimizing execution for the cloud environment.

---

## 2. Generated Artifacts
The migration process generated the following file:

* **Target File:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`
  * **Role:** A fully compliant Airflow DAG script defining the workflow structure. It replaces the legacy UC4 XML definition and implements the dummy execution logic using a lightweight Airflow operator.
  * **Folder Integrity:** The target file path strictly mirrors the legacy codebase directory structure to maintain repository organization.

---

## 3. Key Design Decisions

### Lightweight Operator Optimization
* **Decision:** The automated migration tool initially suggested mapping this job to a `DataprocSubmitJobOperator` executing a PySpark script. This was rejected in favor of a native Airflow `PythonOperator`.
* **Reasoning:** Spinning up or submitting a job to a Dataproc cluster to execute a dummy print statement is highly inefficient, introduces unnecessary latency, and incurs avoidable GCP compute costs. The `PythonOperator` executes instantly within the Airflow worker environment.

### Print Literal Compliance
* **Decision:** The legacy UC4 script contained the print statement `:print Doing nothinig` (which includes a spelling mistake). This exact string has been preserved character-for-character in the Python execution block:
  ```python
  print("Doing nothinig")
  logging.info("Doing nothinig")
  ```
* **Reasoning:** Strict compliance with legacy print statements prevents breaking any downstream log-scraping, monitoring, or auditing tools that might key off specific legacy log patterns.

### Dynamic Environment Configuration
* **Decision:** Hardcoded environment variables and prose placeholders (e.g., `YOUR_GCP_PROJECT_ID`) have been completely eliminated.
* **Reasoning:** The DAG dynamically fetches global environment configurations (`GCP_PROJECT`, `GCP_REGION`, `GCS_BUCKET`) at runtime using Airflow Variables (`Variable.get()`), ensuring seamless portability across Development, UAT, and Production environments.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Configuration
Ensure that the following global Airflow variables are defined in the target Cloud Composer environment prior to DAG execution:
* `GCP_PROJECT`: The target Google Cloud Project ID.
* `GCP_REGION`: The target GCP region (e.g., `europe-west3`).
* `GCS_BUCKET`: The GCS bucket associated with the environment.

These can be set via the Airflow UI (**Admin -> Variables**) or via the gcloud CLI:
```bash
gcloud composer environments run <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    variables set GCP_PROJECT <YOUR_PROJECT_ID>
```

### 2. IAM & Permissions
Ensure that the Cloud Composer worker service account has the minimum required IAM roles (`roles/composer.worker`) to execute basic Python tasks and write logs to Cloud Logging.

### 3. Scheduling & Parent Integration
The DAG is currently configured with `schedule=None` because it inherits its execution trigger from its parent workflow. If this DAG is to be run independently during testing, it must be triggered manually or via an external sensor.

---

## 5. Known Gaps & Unresolved References

### Unmigrated Parent Workflow (Dependency Risk)
* **Gap:** The parent Job Plan `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` has **not yet been migrated** to Cloud Composer.
* **Redesign (B4) Item:** Once the parent workflow is migrated, this DAG should be integrated. Depending on the final architecture of the parent workflow, this can be achieved by:
  1. Merging this DAG's tasks directly into a single unified parent DAG.
  2. Triggering this DAG from the parent DAG using the `TriggerDagRunOperator`.
  3. Utilizing an `ExternalTaskSensor` to coordinate execution.

### Synchronization Anchor Verification
* **Action Item:** Coordinate with the business operations team to verify that this dummy task does not represent a manual gate or physical pause point in the legacy system. If a manual gate is required, an `EmptyOperator` with a manual trigger or an `Airflow Webhook/Sensor` must be introduced.

---

## 6. Validation

### 1. DAG Parsing Test
Verify that the DAG is syntactically correct and can be loaded by Airflow without import errors:
```bash
python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
```
*A successful test returns no output/errors.*

### 2. Execution Test
1. Upload the DAG file to the Cloud Composer `dags/` folder, preserving the directory structure: `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`.
2. Trigger the DAG manually via the Airflow UI or CLI:
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
       --location <LOCATION> \
       dags trigger -- dw_dwh_plato_tarif_mapping_taeglich_jp
   ```

### 3. Definition of "Passing"
The validation is successful if:
* The DAG run transitions to a **Success** state.
* The task execution sequence completes: `start` -> `dw_dwh_dummy_absd_plato_tarife` -> `end`.
* The task logs for `dw_dwh_dummy_absd_plato_tarife` contain the exact string:
  ```text
  Doing nothinig
  ```

---

## 7. Rollback Procedure

In the event of a deployment failure or unexpected behavior in production:

1. **Pause the DAG:** Immediately pause the DAG in the Airflow UI or via the CLI to prevent further executions:
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
       --location <LOCATION> \
       dags pause -- dw_dwh_plato_tarif_mapping_taeglich_jp
   ```
2. **Remove the Artifact:** Delete the migrated Python file from the Cloud Composer GCS bucket:
   ```bash
   gsutil rm gs://<COMPOSER_DAG_BUCKET>/dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
   ```
3. **Revert to Legacy Orchestration:** If the legacy UC4 system was deactivated, re-enable the `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` job in the UC4 active queue to resume legacy orchestration.