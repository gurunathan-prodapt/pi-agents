# MIGRATION NOTES: `DW.DWH_TARIFHIST_SCD_MONATLICH_JP`

This document provides comprehensive migration notes for transitioning the monthly tariff history SCD Type 2 orchestration workflow from UC4 to Google Cloud Platform (GCP) Cloud Composer (Airflow 2.x).

---

## 1. Summary

The legacy UC4 Job Plan (JOBP) `DW.DWH_TARIFHIST_SCD_MONATLICH_JP` has been migrated to a native Airflow DAG on GCP Cloud Composer. 

* **Source Platform:** UC4 / Automic Engine
* **Source Object Type:** JOBP (Job Plan)
* **Target Platform:** GCP Cloud Composer (Airflow 2.x)
* **Target DAG ID:** `dw_dwh_tarifhist_scd_monatlich_jp`
* **Migration Strategy:** Orchestration-Only Mapping. The parent Job Plan is migrated as an orchestration DAG that manages cross-job dependencies via `ExternalTaskSensor` tasks and triggers the actual execution logic (migrated separately as a child DAG) via a `TriggerDagRunOperator`.

---

## 2. Generated Artifacts

The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_TARIFHIST/DW.DWH_TARIFHIST_SCD_MONATLICH_JP.py` | Airflow DAG | Orchestration DAG containing sensors for upstream dependencies, a trigger operator for the child execution DAG, and technical boundary tasks. |

---

## 3. Key Design Decisions

### Orchestration-Only Separation
To avoid duplicating business logic and to maintain clean boundaries between scheduling and execution, the UC4 Job Plan (JOBP) and Job Script (JOBS_UNIX) have been decoupled:
* **This DAG (`dw_dwh_tarifhist_scd_monatlich_jp`)** acts purely as an orchestrator. It does not execute PySpark or SQL code directly.
* **The Child DAG (`dw_dwh_tarifhist_scd_monatlich_js`)** contains the actual Dataproc PySpark execution logic for the SCD Type 2 merge.

### Cross-Job Dependency Management
Legacy UC4 external dependencies are modeled explicitly using Airflow `ExternalTaskSensor` tasks. This ensures that the monthly history tracking does not execute until all required upstream daily, weekly, and monthly datasets are successfully processed.

### Dynamic Environment Resolution
To prevent environment-specific hardcoding, all global infrastructure parameters are resolved programmatically at runtime using Airflow Variables:
* `GCP_PROJECT_ID` is resolved via `Variable.get("GCP_PROJECT")`
* `DATAPROC_REGION` is resolved via `Variable.get("GCP_REGION")`
* `GCS_BUCKET_NAME` is resolved via `Variable.get("GCS_BUCKET")`

### Synchronous Child Execution
The `TriggerDagRunOperator` is configured with `wait_for_completion=True`. This preserves the synchronous execution behavior of the original UC4 Job Plan, ensuring that the parent DAG remains active and only marks itself as successful once the child execution DAG completes successfully.

---

## 4. Manual Steps Before Go-Live

Before enabling this DAG in a production environment, the following manual setup steps must be completed:

### 1. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in the target Cloud Composer environment:
* `GCP_PROJECT`: The GCP Project ID hosting the Dataproc cluster.
* `GCP_REGION`: The GCP region where Dataproc jobs are executed (e.g., `europe-west3`).
* `GCS_BUCKET`: The GCS bucket used for staging PySpark scripts and temporary files.

### 2. IAM & Permissions
The Cloud Composer Service Account must have the following permissions:
* `roles/composer.user` (or equivalent Airflow RBAC role) to trigger the child DAG.
* `roles/dataproc.editor` and `roles/storage.objectViewer` (required for the child execution DAG triggered by this orchestrator).

### 3. Target Schema Verification
Verify that the target schema/dataset `DWH_ZIEL` exists in the target database/data warehouse environment and that the service account has write permissions to perform the SCD Type 2 merge.

### 4. Scheduling Alignment
The DAG is configured to run monthly: `0 3 1 * *` (Every 1st of the month at 03:00 AM). Ensure this schedule aligns with business requirements and does not conflict with maintenance windows.

---

## 5. Known Gaps & Unresolved References

### 1. Unmigrated Upstream Dependencies
The following upstream DAGs referenced by the `ExternalTaskSensor` tasks are flagged as **Not Yet Migrated**:
* `dw_dwh_abrechnung_reformat_js`
* `dw_dwh_kunde_abgl_woechentlich_js`
* `dw_dwh_rechnung_export_taeglich_js`
* `dw_dwh_umsatz_konsolidierung_monatlich_js`

> **Impact:** If this orchestration DAG is enabled before these upstream DAGs are deployed, the sensors will fail to find the target DAGs and will eventually time out.

### 2. Sensor Execution Delta Alignment (B4 Redesign Item)
The sensors currently use `execution_delta=timedelta(0)`. Because the upstream jobs run on different schedules (daily, weekly, monthly), a simple zero-delta match will fail to resolve. 
* **Recommendation:** Replace `execution_delta=timedelta(0)` with a custom `execution_date_fn` that resolves the correct execution date of the latest successful run for each specific upstream frequency, or transition to an event-driven architecture (e.g., Airflow Datasets).

---

## 6. Validation

To validate the migrated DAG, perform the following tests:

### 1. DAG Syntax & Import Test
Run a local syntax check to ensure there are no compilation or import errors:
```bash
python DWH/DWH_KERN/PRODUKTION/DW.DWH_TARIFHIST/DW.DWH_TARIFHIST_SCD_MONATLICH_JP.py
```
* **Passing Criteria:** The command exits with code `0` and outputs no errors.

### 2. Airflow UI Validation
1. Upload the DAG file to the Cloud Composer DAGs bucket.
2. Navigate to the Airflow UI and verify that `dw_dwh_tarifhist_scd_monatlich_jp` appears in the DAGs list without import errors.
3. Verify that the Graph View matches the following dependency structure:
   ```text
   start >> [
       wait_for_abrechnung_reformat, 
       wait_for_kunde_abgl, 
       wait_for_rechnung_export, 
       wait_for_umsatz_konsolidierung
   ] >> dw_dwh_tarifhist_scd_monatlich_js >> end
   ```

### 3. Execution Dry-Run
To test the orchestration flow without waiting for upstream schedules:
1. Temporarily mock the `ExternalTaskSensor` tasks to return `True` (or clear them manually in a development environment).
2. Trigger the DAG manually.
3. Verify that the `TriggerDagRunOperator` successfully triggers `dw_dwh_tarifhist_scd_monatlich_js` and waits for its completion.

---

## 7. Rollback Procedure

In the event of a deployment failure or critical issue post-go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `dw_dwh_tarifhist_scd_monatlich_jp` to **Off** (Paused).
2. **Remove the DAG File:**
   Delete the DAG file from the Cloud Composer GCS bucket:
   ```bash
   gsutil rm gs://<your-composer-bucket>/dags/DW.DWH_TARIFHIST_SCD_MONATLICH_JP.py
   ```
3. **Re-enable Legacy Scheduling:**
   Re-activate the legacy UC4 Job Plan `DW.DWH_TARIFHIST_SCD_MONATLICH_JP` in the Automic/UC4 UI.
4. **Verify Legacy Execution:**
   Confirm that the legacy UC4 queue is active and that the next monthly execution is scheduled correctly.