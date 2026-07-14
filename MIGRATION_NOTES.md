# MIGRATION_NOTES.md

**Job Path:** `Shared Files — isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_PST_ANALYZE_JP/DW.DWH_ADM_PRUEFE_AB_INITIO`  
**Source Components:** `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC.xml` (JOBI), `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC.xml` (JOBI)

---

## 1. Summary
This migration transfers the gatekeeper and state-reporting logic of the Ab Initio integration from UC4 (Automic) to **Google Cloud Composer (Apache Airflow)**. 

In the legacy environment, these components existed as UC4 Include Scripts (`JOBI`) that acted as execution guards. They polled a global UC4 Variable (`VARA`) named `DW.ADM_AB_INITIO_VAR` to coordinate execution states with an external Ab Initio application. 

The logic has been fully migrated to Python-based Airflow DAGs and utility modules. The target platform is **Google Cloud Composer**, utilizing **Airflow Variables** backed by Cloud Composer's metadata database to maintain state.

---

## 2. Generated Artifacts
The migration yields three target files, structured to separate reusable logic from DAG orchestration:

### 1. `dags/utils/ab_initio_utils.py`
* **Role:** Shared utility module.
* **Contents:** 
  * `poll_ab_initio_status_fn`: Implements the polling loop, checking the status of the application key (`status_dwh`) inside the Airflow Variable.
  * `update_ab_initio_status_fn`: Implements the completion logic, updating the tracking variable to a finished state (`fertig`).
  * `update_variable_state`: A thread-safe helper function that reads, updates, and writes back JSON-backed Airflow Variables without overwriting adjacent keys.

### 2. `dags/dw_dwh_adm_pruefe_ab_initio_start_inc_guard.py`
* **Role:** Airflow DAG representing the start guard.
* **Contents:** Instantiates the `ab_initio_gatekeeper` task using a `PythonOperator` pointing to the polling utility. It acts as the entry barrier for downstream DWH processing.

### 3. `dags/dw_dwh_adm_pruefe_ab_initio_ende_inc.py`
* **Role:** Airflow DAG representing the end audit step.
* **Contents:** Instantiates the `update_ab_initio_status` task using a `PythonOperator` to mark the Ab Initio run as complete.

---

## 3. Key Design Decisions

### Separation of Concerns (Utility vs. DAGs)
Instead of duplicating the variable-handling code inside separate DAG files, all core logic is consolidated into `dags/utils/ab_initio_utils.py`. This allows other pipelines in the DWH environment to import and reuse these gatekeeper functions.

### JSON-Backed Airflow Variables
UC4 `VARA` objects store multiple key-value pairs. To replicate this behavior without creating dozens of individual Airflow Variables, we use a single JSON-backed Airflow Variable named `dw_adm_ab_initio_var`. 

### Race Condition Prevention
Airflow Variables are stored in the shared metadata database. To prevent concurrent tasks from overwriting each other's updates, `update_variable_state` implements a read-modify-write pattern. Additionally, both DAGs are configured with `max_active_runs=1`.

### Infinite Loop Protection
In UC4, a looping job could run indefinitely without immediate cost implications. In a cloud environment, an infinite loop in Cloud Composer consumes worker resources and increases costs. We have enforced an explicit `execution_timeout=timedelta(hours=1)` on the polling task to automatically fail and release resources if the upstream system hangs.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variable Creation
Before running either DAG, you must initialize the shared Airflow Variable. 
* Navigate to the Airflow UI $\rightarrow$ **Admin** $\rightarrow$ **Variables**.
* Create a new variable with the following details:
  * **Key:** `dw_adm_ab_initio_var`
  * **Val (JSON):**
    ```json
    {
      "status_dwh": "wait"
    }
    ```

### 2. IAM & Permissions
Ensure that the Cloud Composer Service Account has the necessary permissions to read and write to the Airflow Metadata Database (granted by default to Composer worker roles). If external systems need to update this variable, ensure they have the `roles/composer.user` role to interact with the Airflow REST API.

### 3. Connection Strings & Secrets
No external database connections or secret managers are required for these specific helper tasks, as they rely entirely on internal Airflow Variables.

### 4. Scheduling & Integration
These DAGs are configured with `schedule_interval=None`. They should not run on a time-based schedule. Instead:
* Integrate them into your master DWH DAG using `TriggerDagRunOperator`, or
* Import the functions from `utils.ab_initio_utils` directly into your main pipeline DAGs to run as inline tasks.

---

## 5. Known Gaps & Unresolved References

### High-Frequency Polling Cost
The legacy script polled every 10 seconds (`DEFAULT_WAIT_INTERVAL = 10`). In Cloud Composer, querying the Airflow Variable store every 10 seconds generates continuous database reads. 
* **Recommendation:** For production environments, increase `DEFAULT_WAIT_INTERVAL` in `dags/utils/ab_initio_utils.py` to `60` or `120` seconds to reduce database load.

### External State Reset
The external Ab Initio orchestrator must reset the `status_dwh` key back to `"wait"` before triggering a new run cycle. If this reset does not occur, the gatekeeper will read the residual `"go"` status from the previous run and immediately pass without waiting for the new batch.

---

## 6. Validation

### How to Run the Tests

#### 1. Unit Testing the Utility Module
Run a local Python test to verify variable manipulation:
```bash
python -m unittest tests/test_ab_initio_utils.py
```

#### 2. Manual DAG Verification
1. Set `dw_adm_ab_initio_var` to `{"status_dwh": "wait"}` in the Airflow UI.
2. Trigger `dw_dwh_adm_pruefe_ab_initio_start_inc_guard`. Verify in the task logs that it is looping and printing `PRUEFE ...`.
3. In another browser tab, update the Airflow Variable `status_dwh` key to `"go"`.
4. Verify that the polling DAG detects the change, updates the variable state to `ACTIVE in Ab Initio...`, and exits with **Success**.
5. Trigger `dw_dwh_adm_pruefe_ab_initio_ende_inc` and verify that the variable updates to `fertig (HH:MM:SS DD.MM.YYYY)`.

### What "Passing" Means
* **Success Case:** The polling task loops while status is `"wait"`, exits cleanly when status becomes `"go"`, and updates the tracking dictionary.
* **Failure Case:** The polling task aborts immediately and raises an `AirflowException` if the status is set to `"exit1"`.
* **Timeout Case:** The polling task is terminated by the scheduler if it runs longer than 1 hour.

---

## 7. Rollback Procedure

In the event of a deployment failure or unexpected behavior:

1. **Pause the Airflow DAGs:**
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
       --location <LOCATION> \
       dags pause dw_dwh_adm_pruefe_ab_initio_start_inc_guard
   ```
2. **Revert to UC4:**
   Keep the legacy UC4 active-flag enabled or reactivate the corresponding UC4 `JOBI` workflows.
3. **Clean Up Variables:**
   If necessary, delete or reset the `dw_adm_ab_initio_var` Airflow Variable to prevent stale states from affecting future migrations.