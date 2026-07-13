# MIGRATION_NOTES.md

**Job Name:** `Shared Files — isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_PST_ANALYZE_JP/DW.DWH_ADM_PRUEFE_AB_INITIO`  
**Target Platform:** Google Cloud Platform (BigQuery & Cloud Composer / Apache Airflow)  
**Migration Pattern:** UC4_ONLY (Airflow Orchestration & State Tracking)

---

## 1. Summary

This migration transitions the legacy UC4 JOBI (Job Include) synchronization scripts to native Apache Airflow components running on Cloud Composer. 

In the legacy environment, these scripts acted as synchronization barriers (gatekeepers) between external Ab Initio ETL graph completions and downstream incremental Data Warehouse (DWH) loading jobs. The state was tracked using a global UC4 Variable Object (`DW.ADM_AB_INITIO_VAR`).

In the target GCP environment:
* The gatekeeper polling logic is migrated to an **Airflow PythonSensor**.
* The post-execution state update logic is migrated to an **Airflow PythonOperator**.
* The global state repository is migrated to a JSON-serialized **Airflow Variable** (`dw_adm_ab_initio_var`).

---

## 2. Generated Artifacts

The migration process generated two modular Python DAG files to be deployed into your Cloud Composer environment:

### 1. `dags/tasks/dw_dwh_adm_pruefe_ab_initio_start_inc.py`
* **Role:** Gatekeeper Sensor.
* **Logic:** Periodically polls the Airflow Variable `dw_adm_ab_initio_var` for the key `STATUS_DWH`.
  * If the status is `"go"`, the sensor succeeds and downstream tasks are allowed to run.
  * If the status is `"exit1"`, it raises an `AirflowFailException` to immediately fail the task and stop execution.
  * For any other status (e.g., `"wait"`), it continues to poll.

### 2. `dags/tasks/dw_dwh_adm_pruefe_ab_initio_ende_inc.py`
* **Role:** Post-Execution State Updater.
* **Logic:** Executes after downstream incremental DWH processing completes. It logs diagnostic context details (mimicking UC4 console prints) and updates the `dw_adm_ab_initio_var` JSON payload to record that the run is complete (`"fertig"` with timestamp details).

---

## 3. Key Design Decisions

### Airflow Variables as State Store
* **Decision:** Use Airflow's native metadata store via JSON-serialized Airflow Variables (`dw_adm_ab_initio_var`) to replace the UC4 Variable Object (`DW.ADM_AB_INITIO_VAR`).
* **Reasoning:** This avoids the overhead of managing an external database table or Redis instance for simple state synchronization. It keeps the state local to the orchestration engine, matching the legacy UC4 design.
* **Trade-off:** High-frequency polling of Airflow Variables can cause database lock contention on the Airflow metadata database. To mitigate this, the sensor is configured with a reasonable `poke_interval` (10 seconds) and a safety `timeout` (1 hour).

### Sensor Mode Selection (`poke` vs. `reschedule`)
* **Decision:** The sensor is currently configured in `"poke"` mode to match the low-latency requirements of the legacy UC4 system (which used a 10-second wait loop).
* **Trade-off:** `"poke"` mode keeps an Airflow worker slot occupied while waiting. If worker slot starvation becomes an issue in your Cloud Composer environment, this can be changed to `mode="reschedule"`.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following manual setup steps must be completed in the target environment:

### 4.1 Airflow Variable Creation
You must initialize the state tracking variable in Airflow before running the DAGs.
1. Navigate to the Airflow Web UI.
2. Go to **Admin** -> **Variables**.
3. Click **Add a new record** (+).
4. Set **Key** to: `dw_adm_ab_initio_var`
5. Set **Val** (JSON) to:
   ```json
   {
     "STATUS_DWH": "wait"
   }
   ```

### 4.2 IAM & Permissions
* Ensure the Cloud Composer Service Account has the **Composer Worker** role (which inherently grants read/write access to the Airflow metadata database for Variable updates).
* If external systems (such as Cloud Functions or external ETL pipelines) need to trigger or update this state, ensure their service accounts have the **Composer User** or **Airflow API Admin** roles to modify Airflow Variables via the Airflow REST API.

### 4.3 Scheduling & Integration
* Both generated DAGs are configured with `schedule=None` because they are designed to be triggered dynamically or embedded as tasks within a master parent DAG (e.g., using `TriggerDagRunOperator` or by importing the python callables directly into a unified pipeline).
* Integrate these tasks into your master DWH incremental load DAG:
  ```
  [Start Sensor Task] -> [DWH Incremental Load Tasks] -> [End State Update Task]
  ```

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) Items & High-Frequency Polling
* **Database Contention Risk:** If multiple parallel pipelines poll the Airflow Variable store simultaneously, it may degrade Airflow metadata database performance. 
* **Recommendation:** For a more robust production architecture, consider migrating the state tracking store from Airflow Variables to a lightweight operational metadata table in **BigQuery** or **Cloud SQL**. The Python callables can then be updated to query/update that table instead of `Variable.get()` and `Variable.set()`.

---

## 6. Validation

To validate the migrated tasks, perform the following test cases in a non-production Airflow environment:

### Test Case 1: Sensor Polling & Timeout
1. Set the Airflow Variable `dw_adm_ab_initio_var` to `{"STATUS_DWH": "wait"}`.
2. Trigger the `dw_dwh_adm_pruefe_ab_initio_start_inc` DAG.
3. Verify in the task logs that the sensor is actively polling and printing:
   `Checking status for Application: DWH. Current Status: wait`
4. Let it run to verify it respects the 1-hour timeout safety limit if no state change occurs.

### Test Case 2: Sensor Success Path
1. While the sensor from Test Case 1 is actively polling, manually update the Airflow Variable `dw_adm_ab_initio_var` to:
   ```json
   {
     "STATUS_DWH": "go"
   }
   ```
2. Verify in the logs that the sensor detects the change, prints `Status verification successful! Initiating downstream execution pipeline.`, and completes with a **Success** status.

### Test Case 3: Sensor Terminal Failure Path
1. Reset the Airflow Variable `STATUS_DWH` to `wait`.
2. Trigger the sensor DAG.
3. Update the Airflow Variable `STATUS_DWH` to `exit1`.
4. Verify that the sensor immediately raises an `AirflowFailException` and marks the task run as **Failed** without waiting for the timeout.

### Test Case 4: Post-Execution State Update
1. Trigger the `dw_dwh_adm_pruefe_ab_initio_ende_inc` DAG.
2. Verify that the task completes successfully.
3. Check the Airflow Variable `dw_adm_ab_initio_var` in the UI. It should now contain a new key-value pair logging the completion status, structured as:
   ```json
   {
     "STATUS_DWH": "go",
     "dw_dwh_adm_pruefe_ab_initio_ende_inc -> log_and_update_ab_initio_status": "fertig (HH:MM:SS DD.MM.YYYY)"
   }
   ```

---

## 7. Rollback Procedure

If issues are encountered during go-live, execute the following steps to roll back to the legacy UC4 orchestration:

1. **Pause Airflow DAGs:** Go to the Airflow Web UI and toggle the pause switch to **Off** for both `dw_dwh_adm_pruefe_ab_initio_start_inc` and `dw_dwh_adm_pruefe_ab_initio_ende_inc`.
2. **Re-enable UC4 Jobs:** Un-suspend or re-activate the corresponding UC4 JOBI processing flows (`DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` and `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`) in the UC4 Engine.
3. **Verify Legacy State:** Ensure the legacy UC4 variable object `DW.ADM_AB_INITIO_VAR` is manually synchronized to match the current operational state of the Ab Initio ETL pipelines.