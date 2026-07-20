# Migration Notes: `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`

This document details the migration of the UC4 Job Plan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP` to Google Cloud Composer (Airflow).

---

## 1. Summary

The UC4 Job Plan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP` has been migrated to Google Cloud Composer as an Airflow DAG. 

Following the **UC4_ONLY** migration pattern, this is a pure orchestration migration. The DAG is responsible for:
*   Enforcing cross-DAG execution dependencies (upstream checks).
*   Managing the weekly execution schedule.
*   Triggering the downstream execution DAG (`dw_dwh_kunde_abgl_woechentlich_js`) which contains the actual PySpark/SQL data reconciliation logic.

*   **Source Platform:** UC4 / Automic Engine (Job Plan `JOBP`)
*   **Target Platform:** Google Cloud Composer (Airflow 2.x)

---

## 2. Generated Artifacts

The migration process generated the following file:

| Target File Path | Language / Tech | Role |
| :--- | :--- | :--- |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/dw_dwh_kunde_abgl_woechentlich_jp.py` | Python / Airflow DAG | Orchestrates the weekly execution flow. It uses `ExternalTaskSensor` operators to verify upstream dependencies and a `TriggerDagRunOperator` to execute the child reconciliation job. |

---

## 3. Key Design Decisions

### Decoupled Orchestration (UC4_ONLY Pattern)
To maintain a clean separation of concerns and avoid monolithic DAG designs, the parent Job Plan (`JOBP`) was migrated as a dedicated orchestrator DAG. It does not execute data processing scripts directly. Instead, it manages dependencies and triggers the child execution DAG (`dw_dwh_kunde_abgl_woechentlich_js`).

### Cross-DAG Dependency Management
In UC4, dependencies between different business domains are often managed implicitly via schedules or manual job plan wiring. In Airflow, these are explicitly modeled using `ExternalTaskSensor` operators. This ensures that the weekly customer address comparison only runs after the following upstream processes have completed successfully:
1.  **Billing Reformat:** `dw_dwh_abrechnung_reformat_js`
2.  **Daily Billing Export:** `dw_dwh_rechnung_export_taeglich_js`
3.  **Monthly Tariff History SCD:** `dw_dwh_tarifhist_scd_monatlich_js`
4.  **Consolidated Revenue Matching:** `dw_dwh_umsatz_konsolidierung_monatlich_js`

### Sensor Performance Optimization
To prevent worker slot starvation (where sensors consume all available Airflow worker slots while waiting for upstream DAGs), all `ExternalTaskSensor` tasks are configured with:
*   `mode='reschedule'`: This releases the worker slot between pokes.
*   `poke_interval`: Set to 300 seconds (5 minutes) or 600 seconds (10 minutes) depending on the expected duration of the upstream task.
*   `timeout`: Set to generous limits (2 to 4 hours) to accommodate typical batch window delays.

### Dynamic Configuration
All environment-specific parameters (GCP Project ID, Dataproc Region, Dataproc Cluster Name, and GCS Bucket Name) are resolved dynamically at runtime using Airflow Variables (`Variable.get()`). This eliminates hardcoded environment values and ensures the same DAG file can be deployed across Development, Test, and Production environments without modification.

---

## 4. Manual Steps Before Go-Live

The following manual setup steps must be completed in the target environment before enabling the DAG:

### 1. Airflow Variables Setup
Ensure the following Airflow Variables are configured in the Cloud Composer environment (via the Airflow UI or CLI):

```json
{
  "GCP_PROJECT": "your-gcp-project-id",
  "DATAPROC_REGION": "europe-west3",
  "DATAPROC_CLUSTER": "dwh-dataproc-cluster",
  "GCS_BUCKET": "your-gcs-artifact-bucket"
}
```

### 2. IAM & Permissions
The Cloud Composer Service Account must have the following permissions:
*   `roles/composer.user` (or equivalent Airflow RBAC role) to allow the `TriggerDagRunOperator` to trigger other DAGs within the same environment.
*   `roles/storage.objectViewer` on the GCS bucket containing any referenced PySpark scripts or configuration files.

### 3. Upstream DAG Deployment
The four upstream DAGs monitored by the sensors must be deployed and active in the Composer environment:
*   `dw_dwh_abrechnung_reformat_js`
*   `dw_dwh_rechnung_export_taeglich_js`
*   `dw_dwh_tarifhist_scd_monatlich_js`
*   `dw_dwh_umsatz_konsolidierung_monatlich_js`

### 4. Child DAG Deployment
The child execution DAG `dw_dwh_kunde_abgl_woechentlich_js` must be deployed to the Composer environment.

---

## 5. Known Gaps & Unresolved References

### Upstream Schedule Alignment
By default, `ExternalTaskSensor` looks for an upstream DAG run with the *exact same* execution date and time. Because the upstream jobs have different frequencies (e.g., daily, monthly) and schedules than this weekly job, the sensors may fail to find matching runs unless execution date mapping is configured.
*   **Follow-up Action:** If upstream DAGs run on different schedules, configure the `execution_delta` or `execution_date_fn` parameters on each `ExternalTaskSensor` to map the weekly execution time to the corresponding upstream run times.

### Unmigrated Dependencies
If any of the upstream DAGs or the child execution DAG are not yet migrated to GCP, the sensors or the trigger operator will fail at runtime.
*   **Mitigation:** During the transitional migration phase, temporary "stub" DAGs can be deployed for unmigrated dependencies, or the corresponding sensors can be temporarily paused/commented out.

---

## 6. Validation

To validate the migrated DAG, perform the following tests:

### 1. DAG Parsing Test
Verify that the DAG file is syntactically correct and can be parsed by Airflow without errors:

```bash
python dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/dw_dwh_kunde_abgl_woechentlich_jp.py
```
*Passing criteria:* The command exits with code `0` and outputs no errors.

### 2. Airflow Import Validation
Verify that the DAG is successfully loaded into the Airflow Metadata Database:

```bash
airflow dags list | grep dw_dwh_kunde_abgl_woechentlich_jp
```
*Passing criteria:* The DAG ID appears in the list of active DAGs.

### 3. Graph Validation
Open the Airflow UI, navigate to the `dw_dwh_kunde_abgl_woechentlich_jp` DAG, and verify the Graph View:
*   The four sensors (`wait_for_...`) run in parallel.
*   All four sensors point to the `start` task.
*   `start` points to `dw_dwh_kunde_abgl_woechentlich_js` (Trigger operator).
*   The trigger operator points to the `end` task.

---

## 7. Rollback Procedure

If issues are encountered during go-live, perform the following steps to roll back the orchestration to the legacy UC4 environment:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch for `dw_dwh_kunde_abgl_woechentlich_jp` to **Off** (Paused).
2.  **Remove the DAG File (Optional):**
    Delete the DAG file from the Composer GCS bucket to prevent accidental execution:
    ```bash
    gsutil rm gs://<composer-dag-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/dw_dwh_kunde_abgl_woechentlich_jp.py
    ```
3.  **Reactivate UC4 Job Plan:**
    In the UC4 Automic UI, locate the Job Plan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP` and set its status to **Active** (ensure the scheduler/calendar is enabled).
4.  **Verify Rollback:**
    Confirm in the UC4 activity window that the job plan is scheduled for its next weekly run and that no duplicate executions are running in GCP.