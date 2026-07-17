# MIGRATION_NOTES.md

## 1. Summary
This document details the migration of the daily customer database reconciliation orchestration process `DW.DWH_STAMM_KNZB_ABGL_JP` from its legacy **Automic UC4** environment to **Google Cloud Composer (Airflow)**.

The migrated workflow coordinates the daily reconciliation of customer number and basic access master data (*Kundennummer-/Basiszugangs-Stammdaten* - KNZB) between the source system (ISTNS) and the Core Data Warehouse layer (DWH-Kernschicht). It manages workflow execution states via global variables to safeguard against parallel executions and unauthorized runs.

---

## 2. Generated Artifacts
The migration process generated three distinct Python modules to preserve the modularity and folder structure of the legacy system:

1. **`dags/dwh/dwh_kern/produktion/dw_dwh_stamm/dw_dwh_stamm_knzb_abgl_jp.py`**
   * **Role**: Primary Airflow DAG definition file. It maps the overall workflow structure, implements the guard task, and embeds the logic of the legacy start (`DW.DWH_STAMM_KNZB_ABGL_START_JS`) and end (`DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`) jobs as `PythonOperator` tasks.
2. **`dags/dwh/dwh_kern/produktion/dw_dwh_stamm/includes/dw_hole_pfad_knzb.py`**
   * **Role**: Modularized helper script simulating the legacy include `DW.HOLE_PFAD_KNZB`. It retrieves path variables from the central Airflow Variable store.
3. **`dags/dwh/dwh_kern/produktion/dw_dwh_stamm/includes/dw_lese_log_knzb.py`**
   * **Role**: Modularized helper script simulating the legacy include `DW.LESE_LOG_KNZB`. It outputs standard logging records using the original German terminology.

---

## 3. Key Design Decisions
* **State Management via Airflow Variables**: The legacy UC4 system relied on global variable objects (`DW.VARIABLEN_KNZB`) to maintain state across jobs. This has been mapped to JSON-based Airflow Variables (`dw_variablen_knzb`), allowing dynamic state updates (`GET_VAR` and `PUT_VAR` equivalents) directly within Python tasks.
* **Deadlock Prevention**: If a task fails while holding the execution lock (`ABGLEICH_STATUS = "LAEUFT"`), subsequent runs would be permanently blocked. To mitigate this, an `on_failure_callback` (`on_workflow_failure`) is registered at the DAG level to automatically transition the status to `ERROR_STATE` on failure, alerting operations while preventing silent deadlocks.
* **Concurrency Guard**: To prevent race conditions, the DAG is configured with `max_active_runs=1`. Additionally, a custom `start_guard` task checks for any active running instances of the same DAG and skips execution if one is detected.
* **Verbatim Log Preservation**: All logging text, console outputs, and German terminology have been preserved character-for-character to maintain operational consistency for downstream log parsers and support teams.

---

## 4. Manual Steps Before Go-Live

### 4.1. Airflow Variable Creation
You must define the following Airflow Variables in the Cloud Composer environment before triggering the DAG:

1. **`dw_variablen`** (JSON):
   ```json
   {
     "DWH_HOME": "/opt/dwh",
     "HOME": "/home/dwh_user",
     "ISTNS_HOME": "/opt/istns"
   }
   ```
2. **`dw_variablen_knzb`** (JSON):
   ```json
   {
     "ABGLEICH_STATUS": "FREI",
     "LETZTER_LAUF": ""
   }
   ```

### 4.2. IAM & Permissions
* Ensure the Cloud Composer environment's service account has the **Composer Worker** role and permissions to read/write Airflow Variables.

### 4.3. Scheduling Configuration
* Because no `EVNT_TIME` scheduler file was provided in the source XML, the DAG is currently configured with `schedule_interval=None`. 
* **Action Required**: Update the `schedule_interval` parameter in `dw_dwh_stamm_knzb_abgl_jp.py` to the desired cron expression (e.g., daily execution window) before deploying to production.

---

## 5. Known Gaps & Unresolved References
* **B4 Redesign / Missing Ab Initio Graphs**: No `JOBS_UNIX` objects containing Ab Initio graphs or actual data-load scripts were defined inside this UC4 workflow. This DAG acts purely as an orchestration lock/unlock mechanism. If actual data-loading scripts or PySpark jobs need to be triggered between `knzb_abgl_start` and `knzb_abgl_ende`, they must be integrated as intermediate tasks.
* **Hardcoded Fallbacks**: If the Airflow Variables are missing during execution, the tasks will initialize a local fallback state to prevent immediate execution crashes. This should be monitored via Cloud Logging.

---

## 6. Validation

### 6.1. How to Run the Tests
1. **Syntax & Import Check**:
   Run a local Python compilation check on the DAG file to ensure all imports (including the custom sub-modules) resolve correctly:
   ```bash
   python3 dags/dwh/dwh_kern/produktion/dw_dwh_stamm/dw_dwh_stamm_knzb_abgl_jp.py
   ```
2. **Airflow CLI Task Test**:
   Test individual tasks locally using the Airflow CLI:
   ```bash
   airflow tasks test dw_dwh_stamm_knzb_abgl_jp start_guard 2024-11-04
   airflow tasks test dw_dwh_stamm_knzb_abgl_jp knzb_abgl_start 2024-11-04
   airflow tasks test dw_dwh_stamm_knzb_abgl_jp knzb_abgl_ende 2024-11-04
   ```

### 6.2. What "Passing" Means
* **`start_guard`**: Completes successfully if no other instance of the DAG is running.
* **`knzb_abgl_start`**: 
  * Reads `dw_variablen_knzb`.
  * If `ABGLEICH_STATUS` is `"GESPERRT"`, it must raise an `AirflowFailException` and stop.
  * Otherwise, it updates `ABGLEICH_STATUS` to `"LAEUFT"`, updates `LETZTER_LAUF` to the execution date, and completes successfully.
* **`knzb_abgl_ende`**:
  * Reads `dw_variablen_knzb`.
  * Updates `ABGLEICH_STATUS` back to `"FREI"`.
  * Logs the successful completion message in German.

---

## 7. Rollback Procedure
In the event of a deployment failure or critical runtime issue:

1. **Pause the DAG**: Immediately pause the DAG in the Airflow UI to prevent further scheduled executions.
2. **Reset the State Variable**: Manually reset the `dw_variablen_knzb` Airflow Variable to a safe state:
   ```json
   {
     "ABGLEICH_STATUS": "FREI",
     "LETZTER_LAUF": ""
   }
   ```
3. **Revert Code**: Revert the Git repository to the previous stable commit and redeploy the DAG folder to the Cloud Composer GCS bucket.
4. **Legacy Fallback**: If necessary, resume scheduling on the legacy UC4 engine.