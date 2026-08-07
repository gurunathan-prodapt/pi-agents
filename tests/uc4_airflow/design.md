=== OBJECT: DW.DWH_ADM_JOB_MONITOR_START (JOBI) ===
active=None
title=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 to Apache Airflow Migration Design Document

---

## 1. Overview
**UNCERTAIN: DW.DWH_ADM_JOB_MONITOR_START (JOBI)**
This extraction bundle contains a single UC4 Include (`JOBI`) object, `DW.DWH_ADM_JOB_MONITOR_START`. In UC4/Automic, a JOBI is not an executable workflow (JOBP) or a standalone job (JOBS); rather, it is a reusable script block or text template that other jobs include dynamically at runtime (often for setup, logging, initialization, or monitoring tasks). Because no calling JOBP, JOBS, or parent workflow was supplied in this extraction, this object's exact calling context and the scripts that execute it are unknown. In Apache Airflow, this logic should be refactored into a reusable helper function, a custom Operator, or a shared Python module imported by other DAGs, rather than maintained as an independent DAG.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ADM_JOB_MONITOR_START` | JOBI | None | Include Script / Reusable utility block (likely used to initialize job monitoring/telemetry) |

---

## 3. Scheduling
* **Schedule Analysis**: This object has no native calendar-based schedule or time-based triggers. 
* **Trigger Mechanism**: As a JOBI (Include) utility, it cannot be scheduled or executed independently. It is triggered inline as part of other UC4 jobs that reference it via the `:INCLUDE` directive.
* **Airflow Schedule**: `schedule=None`

---

## 4. Airflow DAG Properties
*Note: Since no JOBP (Workflow) exists in this bundle, a true DAG is not representable. The properties below are stub placeholders representing how this utility would be defined if encapsulated within an Airflow DAG context or as a shared helper module.*

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_adm_job_monitor_start` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `True` |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_adm_job_monitor_start_helper` | `DW.DWH_ADM_JOB_MONITOR_START` | `PythonOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | `#REVIEW-STRUCT:` This is a JOBI include script and has no standalone execution body. It should be imported as a helper library. |

---

## 6. Task Dependency Map
```
No task chain is definable because the extraction contains only a single, non-executable JOBI object.
```

---

## 7. Sync / Concurrency Analysis
No sync rows, locks, or concurrency restrictions are associated with this JOBI object.

| UC4 Sync Else value | lock_kind | Airflow mapping |
| :--- | :--- | :--- |
| N/A | N/A | N/A |

---

## 8. Error Handling and Retry Strategy
* **Default Setup**: Because this represents an initialization or telemetry action, any failure should be caught, logged, and either retried or handled gracefully so as not to block the downstream jobs importing it.
* **Task Retries**: Retries are set to `1` with a `5-minute` delay.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_ADM_JOB_MONITOR_START` | Include Script | Reusable helper function module (e.g., `plugins/helpers/dwh_adm_job_monitor.py`) |

---

## 10. Developer Notes
* `#REVIEW-STRUCT:` **Missing Context**: This extraction contains only a single `JOBI` (Include) object. It has no runnable workflow or jobs. You must locate the UC4 JOBS or JOBP objects that reference `DW.DWH_ADM_JOB_MONITOR_START` to understand how it is called and what variables it relies on.
* `#REVIEW-STRUCT:` **Missing Script Body**: The actual code/content of the JOBI script block was not provided in this metadata extraction. The developer must extract the script body from the UC4 system and convert it into pythonic logic (e.g., an API call to a metadata database or monitoring system).
* **Migration Recommendation**: Do not compile this into a standalone DAG. Instead, write a shared Python helper function in the Airflow plugins directory (e.g., `plugins/helpers/monitoring.py`) and import it inside other DAGs.

---

# Pseudocode Outline

```python
# ==============================================================================
# ── Imports ───────────────────────────────────────────────────────────────────
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
# # REVIEW-STRUCT: Import custom logging/monitoring helpers once JOBI body is resolved
# from helpers.monitoring import dwh_adm_job_monitor_start_logic 

# ==============================================================================
# ── GCP Configuration ──────────────────────────────────────────────────────────
# ==============================================================================
# No GCP services are referenced in this JOBI metadata stub

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# ── on_failure_callback stubs ─────────────────────────────────────────────────
# ==============================================================================
def on_failure_callback_stub(context):
    """
    Placeholder callback to mimic UC4 error handling behavior if needed.
    """
    task_id = context.get('task_instance').task_id
    print(f"Task {task_id} failed. Executing standard alert/cleanup actions.")

# ==============================================================================
# ── DAG Definition (Utility Stub) ─────────────────────────────────────────────
# ==============================================================================
# # REVIEW-STRUCT: This DAG is a container for representation purposes.
# In production, integrate this logic into parent workflows.
with DAG(
    dag_id='dw_dwh_adm_job_monitor_start',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['utility', 'uc4_migration', 'jobi'],
) as dag:

    # ==========================================================================
    # ── Guard Task ────────────────────────────────────────────────────────────
    # ==========================================================================
    # None required (No Else=Skip sync rows detected)

    # ==========================================================================
    # ── Sensor Task ───────────────────────────────────────────────────────────
    # ==========================================================================
    # None required (No earliest_start_time constraint detected)

    # ==========================================================================
    # ── Calendar Check Task ───────────────────────────────────────────────────
    # ==========================================================================
    # None required (No calendar constraints detected)

    # ==========================================================================
    # ── Task: dwh_adm_job_monitor_start_helper ────────────────────────────────
    # ==========================================================================
    def execute_job_monitor_start(**kwargs):
        """
        Placeholder execution logic representing the JOBI script.
        # REVIEW-STRUCT: Developers must replace this placeholder with the actual 
        UC4 script block contents (e.g., registering job run ID, timestamping).
        """
        print("Executing shared JOBI logic: DW.DWH_ADM_JOB_MONITOR_START")
        # Example target behavior:
        # run_id = kwargs['run_id']
        # dwh_adm_job_monitor_start_logic(run_id=run_id)
        pass

    dwh_adm_job_monitor_start_helper = PythonOperator(
        task_id='dwh_adm_job_monitor_start_helper',
        python_callable=execute_job_monitor_start,
        provide_context=True,
        on_failure_callback=on_failure_callback_stub,
    )

    # ==========================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────
    # ==========================================================================
    # No dependency chain exists as this is a single JOBI utility script stub.
    dwh_adm_job_monitor_start_helper
```

# Migration Design Document - DW.DWH_ADM_JOB_MONITOR_START

## File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow/DW.DWH_ADM_JOB_MONITOR_START.xml` | `uc4_airflow/dw_dwh_adm_job_monitor_start.py` | Migrates the UC4 JOBI include script into a reusable Python helper module containing job registration logic. |

---

## Schedule & Variables — Must Be Retained
This UC4 include script relies on several scheduler-set variables. The migrated Python helper module must dynamically retrieve and map these variables through Airflow’s native runtime context:

### UC4 Scheduler-Set Variables to Airflow Context Mapping
* **`ADMJP`** (`SYS_ACT_JPNAME()`): Retrieves the parent JobPlan name. In Airflow, this maps to the active DAG ID: `context['dag'].dag_id`.
* **`ADMJOB`** (`SYS_ACT_JOBNAME()`): Retrieves the job name. In Airflow, this maps to the specific task ID being monitored: `context['task_instance'].task_id`.
* **`ADMNRJOB`** (`SYS_ACT_JOBNR()`): Retrieves the unique active run number. In Airflow, this maps to the unique DAG Run ID or Task Instance run string: `context['run_id']`.
* **`DWH_JOB_KENNUNG`**: Initialized as `""`.
* **`ADMMONJP`** (`PREP_PROCESS_VAR("DW.DWH_MONITORED_JPS")`): A process variable reading configuration records from `DW.DWH_MONITORED_JPS` to find which JobPlans require monitoring.
* **`ADMGB`** and **`ADMWERT`**: Represent individual key-value pairs (JobPlan Name and Monitoring Flag `"J"`) processed line-by-line from the configuration container.
* **`DW.DWH_RUNNING_JOBS`**: The target variable container where the job name and run number are registered via the `PUT_VAR` command.

---

## Cross-File Dependencies
* **Callable Utility**: As a legacy `JOBI` (Include) script, this logic does not run as a standalone DAG. Instead, it is imported and invoked by other DAGs or task lifecycle callbacks (such as an `on_execute_callback` or a dedicated custom operator) to register task starts.
* **Shared State**: Relies on a shared configuration store (`DW_DWH_MONITORED_JPS`) and writes to a shared active jobs registry (`DW_DWH_RUNNING_JOBS`).

---

## Target File Plan
| Target File Path | Language | Source File | Purpose |
| :--- | :--- | :--- | :--- |
| `uc4_airflow/dw_dwh_adm_job_monitor_start.py` | Python | `uc4_airflow/DW.DWH_ADM_JOB_MONITOR_START.xml` | A reusable Python module containing the utility function `dwh_adm_job_monitor_start(...)` to register active tasks. |

---

## Environment-Specific Values
The environment values are classified by their target role and must be resolved dynamically at runtime:

### 1. GLOBAL (Environment-Wide Configuration)
* **`DW_DWH_MONITORED_JPS`** (Derived from `DW.DWH_MONITORED_JPS`)
  * **Role**: Shared dictionary/table listing monitored workflows and their active flags.
  * **Target Resolution**: Sourced at runtime via `Variable.get("DW_DWH_MONITORED_JPS", deserialize_json=True)` or queried from a shared metadata table in BigQuery.
* **`DW_DWH_RUNNING_JOBS`** (Derived from `DW.DWH_RUNNING_JOBS`)
  * **Role**: Shared active registry tracking currently running task/job execution IDs.
  * **Target Resolution**: Sourced and updated at runtime via Cloud Composer Airflow Variables or registered in a shared BigQuery metadata table.

---

## Risks and Manual Steps
* **High-Frequency Variable Updates**: The legacy script uses `PUT_VAR` on `DW.DWH_RUNNING_JOBS` to record running jobs. If translated directly to Airflow Variables (`Variable.set`), frequent concurrent updates from multiple DAGs can cause database lock contention and overhead in the Airflow Metadata Database.
  * **Recommendation**: Implement `DW_DWH_RUNNING_JOBS` as a central logging table in BigQuery (e.g., `GCP_PROJECT.BQ_DATASET.dw_dwh_running_jobs`) using insert/delete queries rather than utilizing Airflow Variables.
* **Unresolved Parent Context**: This is a standalone include script. Manual coordination is required to identify which migrated DAGs should reference and call this start monitor.