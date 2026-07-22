# MIGRATION NOTES: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the UC4 Unix job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Composer (Airflow).

---

## 1. Summary
The legacy UC4 Unix job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to a native Google Cloud Composer Airflow DAG. 

* **Source Platform:** UC4 / Automic Workload Automation
* **Source Job Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
* **Target Platform:** Google Cloud Composer (Airflow 2.x)
* **Target DAG Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py`
* **Functional Description:** This job acts as a dummy/utility step within the daily Plato tariff mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It does not execute transactional logic or Ab Initio graphs; its sole function is to print a log output.

---

## 2. Generated Artifacts
The migration process produced a single, optimized orchestration file:

* **`DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py`**
  * **Role:** Airflow DAG definition file.
  * **Location:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/`
  * **Implementation:** Contains the DAG configuration, environment variable resolution, and a native `PythonOperator` that executes the dummy print statement.
  * *Note on Discarded Artifacts:* The generic translation tool suggested generating a secondary PySpark script (`dw_dwh_dummy_absd_plato_tarife_script.py`) to run on a Dataproc cluster. This was discarded during the design phase to optimize performance and cost (see Section 3).

---

## 3. Key Design Decisions

### Native Python Operator vs. Dataproc Execution
* **Decision:** Replaced the proposed `DataprocSubmitJobOperator` with a native Airflow `PythonOperator`.
* **Reasoning:** The legacy UC4 script performs only one operation: `:print Doing nothinig`. Provisioning a Google Cloud Dataproc cluster or submitting a job to an active cluster to run a single print statement introduces significant compute overhead, execution latency (typically 1–3 minutes of startup time), and unnecessary GCP billing costs. Running this directly within the Composer worker context via a `PythonOperator` executes instantly with zero additional infrastructure overhead.

### Output Literal Rule (Typo Preservation)
* **Decision:** Retained the exact spelling of the legacy print statement: `"Doing nothinig"`.
* **Reasoning:** Automated log parsers, monitoring tools, or downstream verification scripts in the legacy environment may rely on this exact string (including the typo) to verify successful execution. Correcting the spelling could break these automated systems.

### Dynamic Environment Configuration
* **Decision:** Avoided hardcoded GCP project IDs, regions, or bucket names.
* **Reasoning:** The DAG dynamically resolves the environment using `os.environ.get("GCP_PROJECT")` and Airflow Variables (`Variable.get("GCS_BUCKET")`). This ensures the code is fully portable across Development, UAT, and Production environments without manual modifications.

---

## 4. Manual Steps Before Go-Live

Before enabling and running this DAG in a production Cloud Composer environment, complete the following configuration steps:

### 1. Airflow Variables Configuration
Ensure the following variable is defined in the target Airflow environment (via Airflow UI -> Admin -> Variables or CLI):
* **Key:** `GCS_BUCKET`
* **Value:** The name of your environment-specific Cloud Storage bucket (e.g., `prd-dwh-composer-storage-bucket`).

### 2. IAM & Permissions
* Ensure the Google Cloud Service Account running the Cloud Composer workers has the necessary permissions to write logs to Cloud Logging. (Standard Composer worker roles include this by default).

### 3. Scheduling & Parent Integration
* **Current State:** The DAG is configured with `schedule=None` because it was migrated from an isolated job definition without its parent schedule context.
* **Action Required:** Once the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is migrated to Airflow, this DAG should be integrated into the parent DAG's control sequence (either as a direct task or triggered via a `TriggerDagRunOperator`).

---

## 5. Known Gaps & Unresolved References

* **Downstream Pipeline Migration Lag:**
  * **Reference:** `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (Parent Job Plan).
  * **Gap:** The parent workflow has not yet been migrated to Cloud Composer. Until the parent workflow is migrated, this DAG will remain isolated and must be triggered manually or via external orchestration.
  * **Resolution:** Once the parent workflow is migrated, wire this DAG's execution task into the parent's task dependency tree.

---

## 6. Validation

To validate the migrated job in the target environment:

### Execution Test
1. Upload `DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` to the Composer DAGs folder (`gs://<your-dag-bucket>/dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/`).
2. Navigate to the Airflow UI and locate the DAG `dw_dwh_dummy_absd_plato_tarife`.
3. Unpause the DAG.
4. Trigger the DAG manually by clicking the **Play** button.

### Success Criteria
The migration is considered successful ("passing") if:
1. The DAG run completes with a status of **Success**.
2. The task `dw_dwh_dummy_absd_plato_tarife` completes successfully.
3. The task logs contain the exact string:
   ```text
   Doing nothinig
   ```

---

## 7. Rollback Procedure

If issues arise during deployment or validation, perform the following steps to roll back:

1. **Disable the Airflow DAG:**
   * Toggle the DAG to **Off** in the Airflow UI to prevent any manual or accidental executions.
2. **Remove the DAG File:**
   * Delete the DAG file from the GCS bucket to clean up the environment:
     ```bash
     gcloud storage rm gs://<your-dag-bucket>/dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py
     ```
3. **Re-enable Legacy Execution:**
   * Ensure the legacy UC4 job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` remains active and enabled within the Automic/UC4 engine to prevent gaps in the legacy daily run.