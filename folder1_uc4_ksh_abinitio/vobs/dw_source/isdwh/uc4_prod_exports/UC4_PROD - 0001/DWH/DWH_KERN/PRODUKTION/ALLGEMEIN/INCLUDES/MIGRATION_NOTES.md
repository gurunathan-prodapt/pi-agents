# Migration Notes: UC4 PROD INCLUDES

## 1. Summary
The shared include utility jobs `DW.HOLE_PFAD` and `DW.LESE_LOG` have been migrated from the legacy UC4/Automic scheduler to Google Cloud Platform (GCP). 

* **Source Platform:** UC4/Automic (XML-based job definitions and shell-based include scripts)
* **Target Platform:** Google Cloud Composer (Apache Airflow 2.x) & Google Cloud Logging
* **Migration Scope:** 
  * Environment path resolution and dynamic date calculations (`DW.HOLE_PFAD.xml`)
  * Execution status evaluation, return-code simulation, and monitoring hooks (`DW.LESE_LOG.xml`)

These utilities have been refactored into reusable Python modules placed within the Airflow environment's `plugins/` directory, allowing downstream DAGs to import and execute them natively.

---

## 2. Generated Artifacts
The following target files were generated to replace the legacy UC4 XML includes:

### 1. `dwh_env_resolver.py`
* **Target Path:** `plugins/helpers/dwh_env_resolver.py`
* **Role:** Replaces `DW.HOLE_PFAD.xml`. It dynamically fetches environment variables from the Airflow Variable database (with safe defaults) and performs calendar math relative to the DAG's logical execution date (`logical_date`). It pushes these resolved paths and dates to Airflow XComs for downstream task consumption and triggers the start monitoring hook.

### 2. `dwh_monitor_callback.py`
* **Target Path:** `plugins/helpers/dwh_monitor_callback.py`
* **Role:** Replaces `DW.LESE_LOG.xml`. It acts as a reusable Airflow task callback module (`on_success_callback` and `on_failure_callback`). It captures task execution states, prints verbatim legacy German console outputs, handles the missing `SHOWLOG.KSH` utility via a safe fallback stub, and triggers the end monitoring hook.

---

## 3. Key Design Decisions
* **Decoupled Helper Library Approach:** Instead of creating standalone, isolated DAGs for these utilities (which would introduce scheduling overhead and complex cross-DAG execution dependencies), they were refactored into a **shared Python helper library**. Downstream DAGs can import these functions directly.
* **Idempotent Date Arithmetic:** Legacy date calculations relied on the system date of the execution machine (`SYS_DATE`). This has been replaced with Airflow's execution context `logical_date` (formerly `execution_date`). This ensures that backfills and historical reruns calculate dates relative to the target data period rather than the real-world execution time.
* **XCom for Parameter Passing:** Resolved paths and calculated dates are pushed to Airflow's XCom storage under the keys `dwh_paths` and `dwh_dates`. This allows downstream operators to dynamically pull parameters using Jinja templates (e.g., `{{ ti.xcom_pull(key='dwh_dates')['LASTMONTH_YYYYMM'] }}`).
* **Callback-Driven Monitoring:** The return-code checking logic of `DW.LESE_LOG` has been mapped directly to Airflow's native task callback architecture (`on_success_callback` and `on_failure_callback`). This eliminates the need for explicit "check status" tasks after every single execution step.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variable Configuration
The following variables must be populated in the Airflow Metadata Database (via the Airflow UI under **Admin -> Variables** or via the `gcloud composer environments run` CLI):

| Variable Key | Expected Value / Example | Description |
| :--- | :--- | :--- |
| `DWH_HOME` | `/opt/dwh` | Base directory for DWH binaries |
| `HOME` | `/home/dwh` | User home directory |
| `KWS_HOME` | `/opt/dwh/kws` | KWS application home directory |
| `PMS_HOME` | `/opt/dwh/pms` | PMS application home directory |
| `ISTNS_HOME` | `/opt/dwh/istns` | ISTNS application home directory |
| `AKTIV_CARMEN` | `0` or `1` | Activation flag for CARMEN module |
| `AKTIV_CRS` | `0` or `1` | Activation flag for CRS module |
| `AKTIV_CTEL` | `0` or `1` | Activation flag for CTEL module |
| `AKTIV_DPPS` | `0` or `1` | Activation flag for DPPS module |
| `AKTIV_KDS` | `0` or `1` | Activation flag for KDS module |
| `AKTIV_WUERFEL` | `0` or `1` | Activation flag for WUERFEL module |
| `AKTIV_XTRA` | `0` or `1` | Activation flag for XTRA module |
| `AKTUELL_CACHE` | `/var/cache/dwh` | Path to active cache directory |

### 2. IAM & Permissions
Ensure that the Cloud Composer Service Account has the following permissions:
* `roles/logging.logWriter` to write execution logs to Google Cloud Logging.
* `roles/composer.worker` to read and write Airflow Variables and XComs.

### 3. Deployment of Shared Helpers
Copy the generated files to your Cloud Composer environment's DAGs/plugins bucket:
```bash
gcloud composer environments storage plugins import \
    --environment <YOUR_COMPOSER_ENV_NAME> \
    --location <YOUR_GCP_REGION> \
    --source folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/ALLGEMEIN/INCLUDES/dwh_env_resolver.py

gcloud composer environments storage plugins import \
    --environment <YOUR_COMPOSER_ENV_NAME> \
    --location <YOUR_GCP_REGION> \
    --source folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/ALLGEMEIN/INCLUDES/dwh_monitor_callback.py
```

---

## 5. Known Gaps & Unresolved References

### 1. Redesign (B4) Items & Missing Components
* **`SHOWLOG.KSH` (SOURCE NOT FOUND):** The legacy shell utility `SHOWLOG.KSH` was not provided in the source files. A Python stub function (`execute_showlog_stub`) has been implemented in `dwh_monitor_callback.py` to prevent execution failures. This stub outputs a warning to Cloud Logging. If the actual logic of `SHOWLOG.KSH` is required, it must be migrated and placed in the environment's execution path.
* **Monitoring Hooks (`DW.DWH_ADM_JOB_MONITOR_START` & `DW.DWH_ADM_JOB_MONITOR_END`):** These legacy monitoring jobs have not yet been migrated. The current implementation uses functional print stubs (`trigger_job_monitor_start` and `trigger_job_monitor_end`). Once the centralized monitoring framework is established on GCP, these stubs must be updated to write to a centralized BigQuery logging table or trigger Pub/Sub events.
* **Downstream Integration:** The downstream jobs (`DW.BERT_AUSD_BP_TA_TARIFOPTION`, `DW.DWH_ABPZ_KKM_AIL_AGENT`, and `DW.DWH_OAIS_EX_PPES_CUBES`) have not yet been migrated. When migrating these DAGs, they must be configured to import and use these helper modules.

---

## 6. Validation

### 1. How to Run Unit Tests
Create a test DAG (`test_dwh_includes.py`) in your Composer environment to validate the execution of both modules:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.dwh_env_resolver import resolve_hole_pfad_context
from helpers.dwh_monitor_callback import dwh_success_callback, dwh_failure_callback

with DAG(
    dag_id="test_dwh_includes",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    on_success_callback=dwh_success_callback,
    on_failure_callback=dwh_failure_callback
) as dag:

    test_task = PythonOperator(
        task_id="test_resolve_paths_and_dates",
        python_callable=resolve_hole_pfad_context,
        provide_context=True
    )
```

### 2. What "Passing" Means
1. **Successful Execution:** The `test_resolve_paths_and_dates` task completes with a `SUCCESS` status.
2. **XCom Verification:** In the Airflow UI, navigate to **Admin -> XComs** and verify that the task has successfully pushed:
   * `dwh_paths` containing the dictionary of environment variables.
   * `dwh_dates` containing correct date strings (e.g., if `logical_date` is `2023-10-15`, then `LASTMONTH_YYYYMM` must be `202309`, `PRELASTMONTH_YYYYMM` must be `202308`, and `NEXTMONTH_YYYYMM` must be `202311`).
3. **Log Verification:** Inspect the task logs in Cloud Logging. They must contain the following exact console outputs:
   * For successful runs:
     ```text
     ****************************************************************
     Rueckgabewert: '0' ***************************************
     ****************************************************************
     [JOB MONITOR END] Terminated DAG: test_dwh_includes | Task: test_resolve_paths_and_dates | State: SUCCESS
     ```
   * For forced failure runs (if tested with an intentional exception):
     ```text
     Executing: /home/dwh/tools/showlog -uc4 test_dwh_includes.test_resolve_paths_and_dates
     Warning: Legacy showlog execution requested, but no source was found. Defaulting to Cloud Logging.
     ****************************************************************
     Rueckgabewert: '1' (Fehlerfall)***************************
     ****************************************************************
     [JOB MONITOR END] Terminated DAG: test_dwh_includes | Task: test_resolve_paths_and_dates | State: FAILED
     ```

---

## 7. Rollback Procedure
In the event of an issue or unexpected behavior in production:

1. **Revert Downstream DAG Imports:** Remove the imports of `helpers.dwh_env_resolver` and `helpers.dwh_monitor_callback` from downstream DAG files and revert them to their previous parameter resolution methods.
2. **Delete Helper Files from GCS:** Remove the migrated helper files from the Airflow plugins directory to prevent import conflicts:
   ```bash
   gcloud storage rm gs://<YOUR_COMPOSER_BUCKET>/plugins/helpers/dwh_env_resolver.py
   gcloud storage rm gs://<YOUR_COMPOSER_BUCKET>/plugins/helpers/dwh_monitor_callback.py
   ```
3. **Clear Airflow Variables:** If necessary, clean up the variables added to the Airflow database:
   ```bash
   gcloud composer environments run <YOUR_COMPOSER_ENV_NAME> \
       --location <YOUR_GCP_REGION> \
       variables -- delete DWH_HOME
   ```