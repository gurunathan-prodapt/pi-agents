# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Composer (Apache Airflow).

---

## 1. Summary
The legacy UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to an Apache Airflow DAG. 

In the source system, this job functioned as a dummy administrative or synchronization checkpoint within the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`. It contained no functional business logic or data processing, executing only an internal UC4 script command to print a diagnostic message. To preserve the legacy execution hierarchy and downstream dependency chains, it has been translated into an Airflow DAG executing a basic echo command.

* **Source Platform:** UC4 / Automic (UNIX Job)
* **Target Platform:** Google Cloud Composer (Apache Airflow)
* **Parent Workflow Context:** `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`

---

## 2. Generated Artifacts
The migration process generated the following file:

| Generated File Path | Role | Description |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Airflow DAG | Python definition file containing the DAG `dw_dwh_dummy_absd_plato_tarife` and its single execution task. |

---

## 3. Key Design Decisions

### Operator Selection
* **Decision:** Use `BashOperator` executing `echo 'Doing nothinig'` instead of an `EmptyOperator`.
* **Rationale:** The original UC4 script utilized an internal scripting command (`:print Doing nothinig`). Implementing a `BashOperator` ensures that the exact diagnostic logging behavior—including the original spelling mistake (`nothinig`)—is preserved in the Airflow task logs for audit and verification purposes.

### Environment and Infrastructure Retirement
* **Decision:** Retire legacy host (`DWHDWH1P`) and login (`DW.UNIX.ISTNS`) configurations.
* **Rationale:** This dummy task has no executable workload requiring external infrastructure. It runs natively within the Cloud Composer worker environment using the default worker service account, eliminating the need for dedicated VM connections or SSH operators.

### Scheduling and Triggering
* **Decision:** Set `schedule=None` (manual/external trigger).
* **Rationale:** The source job has no independent schedule or calendar triggers. It is designed to be executed as part of a parent workflow.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this DAG in production, complete the following steps:

### 1. Airflow Variables Configuration
Ensure the following standard environment variables are defined in your target Airflow environment (even though this specific dummy task does not actively use them, they are imported for environment uniformity):
* `GCP_PROJECT`
* `DATAPROC_REGION`
* `DATAPROC_CLUSTER`
* `GCS_BUCKET`

### 2. IAM & Permissions
Verify that the Cloud Composer environment's service account has basic execution permissions. No specialized external IAM roles are required for this dummy task.

### 3. Parent Workflow Integration
Because the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated, you must plan how this DAG will be triggered:
* **Option A (Consolidated):** When migrating the parent workflow, copy this task directly into the parent DAG file.
* **Option B (Decoupled):** Use a `TriggerDagRunOperator` or `ExternalTaskSensor` in the parent DAG to orchestrate this independent DAG.

---

## 5. Known Gaps & Unresolved References

### Downstream Integration Gap
* **Description:** The downstream consumer workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is currently unmigrated. 
* **Resolution:** The dependency link cannot be fully automated or verified until the parent workflow is migrated to Airflow.

### Redesign (B4) Recommendation
* **Description:** Maintaining an independent DAG file for a single-task dummy execution introduces unnecessary orchestration overhead.
* **Resolution:** During the migration of the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`, it is highly recommended to consolidate this task directly into the parent DAG as an `EmptyOperator` or inline `BashOperator`, rather than deploying it as a standalone DAG.

---

## 6. Validation

To validate the migrated DAG:

1. **DAG Parsing Test:**
   Upload the DAG file to the Airflow `dags/` folder and verify that no import errors are thrown in the Airflow UI.
2. **Manual Execution:**
   Trigger the DAG manually via the Airflow UI or CLI:
   ```bash
   airflow dags trigger dw_dwh_dummy_absd_plato_tarife
   ```
3. **Success Criteria:**
   * The DAG run transitions to `success`.
   * The task `dummy_execution` completes successfully.
   * The task log contains the following output:
     ```text
     [INFO] Running command: echo 'Doing nothinig'
     [INFO] Output:
     Doing nothinig
     ```

---

## 7. Rollback Procedure

If issues arise post-deployment:

1. **Pause the DAG:**
   Turn off the toggle switch for `dw_dwh_dummy_absd_plato_tarife` in the Airflow UI to prevent any accidental triggers.
2. **Remove the Artifact:**
   Delete the DAG file from the Cloud Storage bucket:
   ```bash
   gcloud storage rm gs://<your-composer-bucket>/dags/dw_dwh_dummy_absd_plato_tarife.py
   ```
3. **Revert to Legacy:**
   Ensure the legacy UC4 job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` remains active and enabled in the UC4 environment to handle downstream dependencies.