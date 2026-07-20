# Migration Notes: `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP`

## 1. Summary
The UC4 Job Plan (JOBP) `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` has been migrated to an orchestration DAG on **Google Cloud Composer (Airflow 2.x)**. 

The primary purpose of this workflow is to coordinate the daily export of invoice and billing data ("Rechnungsdaten") from the DWH core layer ("DWH-Kernschicht") to an external reporting directory. In the legacy environment, this JOBP coordinated several upstream dependencies and executed a Unix shell script (`DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`) that performed a SQL extraction via `sqlplus`. 

In the target GCP environment:
*   **Orchestration** is handled by the Airflow DAG `dw_dwh_rechnung_export_taeglich_jp`.
*   **Upstream Dependencies** are synchronized using Airflow `ExternalTaskSensor` operators.
*   **Physical Extraction** is decoupled and executed by a child DAG (`dw_dwh_rechnung_export_taeglich_js`) running a PySpark job on **Google Cloud Dataproc**.

---

## 2. Generated Artifacts

The migration process generated the following orchestration file:

### `dw_dwh_rechnung_export_taeglich_jp.py`
*   **Path:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/dw_dwh_rechnung_export_taeglich_jp.py`
*   **Role:** The master orchestration DAG. It defines the execution schedule (daily at 02:00 UTC), establishes sensors to block execution until all upstream DWH processing DAGs complete successfully, and triggers the downstream PySpark extraction DAG.

---

## 3. Key Design Decisions

### Decoupled Orchestration vs. Monolithic Execution
In UC4, Job Plans (JOBPs) often mix dependency management with direct task execution. To align with Airflow best practices, we decoupled these concerns:
*   `dw_dwh_rechnung_export_taeglich_jp` acts strictly as an **Orchestrator DAG**. It contains no business logic or direct data processing.
*   The actual data extraction logic is delegated to the child DAG `dw_dwh_rechnung_export_taeglich_js` via the `TriggerDagRunOperator`. This keeps the orchestration DAG lightweight and simplifies troubleshooting.

### Cross-DAG Synchronization via Sensors
Because the DWH core layer relies on multiple upstream processes completing successfully, we implemented four `ExternalTaskSensor` tasks. These sensors poll the status of the upstream DAGs. 
*   **Trade-off:** Sensors consume worker slots while poking. To mitigate this, we set `poke_interval=120` (2 minutes) and a generous `timeout=7200` (2 hours) to prevent unnecessary resource utilization while allowing ample time for upstream DWH loads to complete.

### Strict Failure Propagation
The `TriggerDagRunOperator` is configured with `wait_for_completion=True` and `trigger_rule=TriggerRule.ALL_SUCCESS`. This ensures that the orchestration DAG remains in a "running" state while the PySpark export executes, and will only mark itself as "success" if the export completes successfully. Any failure in the child DAG immediately bubbles up to trigger the `on_failure_alarm` callback.

### Environment Isolation
To comply with security and configuration standards, all environment-specific parameters (GCP Project, Region, GCS Bucket) are dynamically resolved at runtime using Airflow Variables (`Variable.get()`). No hardcoded infrastructure references exist in the code.

---

## 4. Manual Steps Before Go-Live

Prior to deploying and enabling this DAG in production, the following manual setup steps must be completed:

### 1. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in your Cloud Composer environment:
*   `GCP_PROJECT`: The GCP Project ID hosting your Dataproc clusters.
*   `GCP_REGION`: The GCP region (e.g., `europe-west3`).
*   `GCS_BUCKET`: The GCS bucket where PySpark scripts and export configurations are stored.

### 2. IAM & Permissions
The Cloud Composer Service Account must possess the following IAM roles:
*   `roles/composer.worker` (to execute the sensors and trigger operators).
*   `roles/composer.user` (required by `TriggerDagRunOperator` to trigger other DAGs in the same environment).

### 3. Upstream DAG Deployment
The following upstream DAGs must be deployed and active in the Airflow environment:
1.  `dw_dwh_abrechnung_reformat_js`
2.  `dw_dwh_kunde_abgl_woechentlich_js`
3.  `dw_dwh_tarifhist_scd_monatlich_js`
4.  `dw_dwh_umsatz_konsolidierung_monatlich_js`

Additionally, the child export DAG `dw_dwh_rechnung_export_taeglich_js` must be deployed.

### 4. Scheduling & Execution Window Alignment
Verify that the daily execution time (`0 2 * * *` / 02:00 UTC) aligns with the schedules of the upstream DAGs. If the upstream DAGs run on a different schedule or timezone, adjust the `execution_delta` or `execution_date_fn` parameters in the sensors.

---

## 5. Known Gaps & Unresolved References

### 1. Missing UC4 Source Files (Critical)
*   **EVNT_TIME:** The original UC4 scheduling event file was missing. The schedule `0 2 * * *` is a fallback placeholder based on standard daily export requirements and must be verified by business operations.
*   **JOBS_UNIX:** The physical SQL/shell script extraction logic (`DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`) was missing during initial design. Ensure that the migrated PySpark script in the child DAG accurately replicates the legacy `sqlplus` logic.

### 2. Cross-Frequency Dependency Alignment (Redesign Item)
Two of the upstream dependencies are non-daily tasks:
*   `dw_dwh_tarifhist_scd_monatlich_js` (Monthly)
*   `dw_dwh_umsatz_konsolidierung_monatlich_js` (Monthly)

**Current Gap:** The `ExternalTaskSensor` is currently configured with `execution_delta=timedelta(hours=0)`. On days when the monthly DAGs do not run, these sensors will fail or wait indefinitely until timing out.
*   **Required Redesign:** Before go-live, implement a custom `execution_date_fn` for the monthly sensors to resolve to the *most recent successful run* of the monthly DAGs, rather than looking for a run on the current logical date.

---

## 6. Validation

To validate the migration of the orchestration DAG, perform the following tests in a lower environment (QA/UAT):

### Step 1: DAG Parsing Test
Verify that the DAG is syntactically correct and can be parsed by Airflow without errors:
```bash
python3 -m unittest dw_dwh_rechnung_export_taeglich_jp.py
```
*(Or verify that the DAG appears in the Airflow UI without import errors).*

### Step 2: Sensor Mocking & Dry Run
1.  Temporarily set `check_existence=False` on the sensors, or trigger mock successful runs of the upstream DAGs for the target execution date.
2.  Trigger a manual run of `dw_dwh_rechnung_export_taeglich_jp` from the Airflow UI.
3.  Verify that the sensors successfully transition from `sensing` to `success` once the upstream conditions are met.

### Step 3: End-to-End Integration Test
1.  Ensure all 4 upstream DAGs have completed successfully for logical date `T`.
2.  Trigger `dw_dwh_rechnung_export_taeglich_jp` for logical date `T`.
3.  Verify that:
    *   All sensors pass immediately.
    *   The `TriggerDagRunOperator` successfully triggers `dw_dwh_rechnung_export_taeglich_js`.
    *   The orchestration DAG remains in `running` state while the child DAG runs.
    *   Upon successful completion of the child DAG, the orchestration DAG transitions to `success`.

---

## 7. Rollback Procedure

In the event of an unrecoverable failure or data discrepancy during go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch for `dw_dwh_rechnung_export_taeglich_jp` to **Off (Paused)**.
2.  **Remove/Archive the DAG File:**
    Delete or move the DAG file from the active Cloud Composer GCS bucket:
    ```bash
    gsutil mv gs://${GCS_BUCKET}/dags/dw_dwh_rechnung_export_taeglich_jp.py gs://${GCS_BUCKET}/dags/archive/
    ```
3.  **Reactivate UC4 Job Plan:**
    In the UC4 UserInterface, locate the Job Plan `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` and set its status to **Active**. Ensure the scheduling event (`EVNT_TIME`) is enabled.
4.  **Target Directory Cleanup:**
    If the child PySpark job partially executed and wrote incomplete files to the reporting directory, manually clean up the target directory to prevent downstream reporting tools from consuming corrupted or duplicate data.