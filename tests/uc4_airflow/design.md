=== OBJECT: DW.DWH_ADM_JOB_MONITOR_START (JOBI) ===
active=None
title=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Migration Design Document: DW.DWH_ADM_JOB_MONITOR_START

## 1. Overview
UNCERTAIN: This extraction contains only a single `JOBI` (Job Include) object (`DW.DWH_ADM_JOB_MONITOR_START`) and no parent `JOBP` (workflow) or `JOBS` (job) objects. In UC4, a JOBI object is not an executable workflow or standalone job; instead, it contains reusable script blocks (typically setup, initialization, or monitoring registration logic) included by reference in other job scripts. Because the parent workflows and execution contexts are missing from this bundle, we design this migration as a **reusable Python helper module** within the Airflow environment, and provide a placeholder template DAG to demonstrate how it would be invoked.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|:---|:---|:---|:---|
| `DW.DWH_ADM_JOB_MONITOR_START` | JOBI | None | Reusable job monitoring start script block |

## 3. Scheduling
- No `EVNT_TIME` or scheduling parameters are present. This object represents a reusable include script, meaning it has no calendar-based schedule of its own.
- It is triggered implicitly as a script dependency whenever a parent job that includes it is run.
- **DAG Schedule:** `schedule=None`

## 4. Airflow DAG Properties
Because a JOBI cannot run standalone, we define a template/placeholder DAG representing how a job incorporating this include logic would be structured.

| Property | Value |
|:---|:---|
| **dag_id** | `dw_dwh_adm_job_monitor_start_template` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `True` |
| **default_args** | `{'owner': 'airflow', 'retries': 0}` |

## 5. Task Inventory
Since JOBI logic is typically embedded, we represent its migration as a shared utility function, or an initialization task inside a target DAG.

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| `dwh_adm_job_monitor_start` | `DW.DWH_ADM_JOB_MONITOR_START` | `PythonOperator` | `dwh_adm_job_monitor.py` | None | 0 | N/A | None | None | `False` | None | # REVIEW-STRUCT: This is a JOBI include object. It should be converted to a Python helper function/module or an Airflow custom listener/callback rather than a standalone task. |

## 6. Task Dependency Map
Since there are no other tasks supplied:
```
dwh_adm_job_monitor_start
```

## 7. Sync / Concurrency Analysis
No sync rows or concurrency locks are defined on this JOBI object.

## 8. Error Handling and Retry Strategy
No postconditions or retries are specified. Default Airflow failure handling will apply.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|:---|:---|:---|
| `dw_dwh_adm_job_monitor_start_template` | Sanitised DAG ID | `dw_dwh_adm_job_monitor_start_template` |

## 10. Developer Notes
* # REVIEW-STRUCT: The extraction contains only a single `JOBI` object. This cannot be executed as a standalone DAG. The recommended migration pattern is to port the shell/SQL code inside this JOBI into a shared Python helper module (`dwh_adm_job_monitor.py`) or a custom Airflow custom operator hook/macro.
* # REVIEW-STRUCT: There are no unresolved references, but only because no parent workflows referencing this include were provided in the bundle.
* All GCP placeholders are left empty as no target platform-specific launch logic was identified in this metadata.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Shared JOBI Implementation ───────────────────────────
# REVIEW-STRUCT: In a production environment, this function should be placed
# in a shared utility library (e.g., plugins/utils/dwh_adm_job_monitor.py)
# so it can be imported by any DAG needing job monitoring.
def dwh_adm_job_monitor_start(**context):
    """
    Python implementation of the DW.DWH_ADM_JOB_MONITOR_START JOBI.
    This function contains the translated script logic from the UC4 JOBI.
    """
    # TODO: Paste and adapt the actual shell script or SQL logic from the JOBI body here.
    # Typically this involves logging task startup or updating a metadata table.
    print("Executing job monitoring start registration logic...")

# ── GCP Configuration ────────────────────────────────────
# No GCP services are referenced in this include's metadata.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 0,
}

# ── on_failure_callback stubs ─────────────────────────────
# None defined for this include

# ── DAG Definition (Template showing JOBI integration) ──
with DAG(
    dag_id='dw_dwh_adm_job_monitor_start_template',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['monitoring', 'template'],
) as dag:

    # ── Task: dwh_adm_job_monitor_start ──────────────────
    # This task executes the ported JOBI logic.
    t_dwh_adm_job_monitor_start = PythonOperator(
        task_id='dwh_adm_job_monitor_start',
        python_callable=dwh_adm_job_monitor_start,
        provide_context=True,
    )

    # ── Dependencies ─────────────────────────────────────────
    # No downstream dependencies exist in this isolated single-object context.
    t_dwh_adm_job_monitor_start
```

### Schedule & Variables — Must Be Retained
The scheduler-set variables listed below must be preserved and dynamically populated at runtime using Airflow's execution context and configuration mechanisms:

*   **`ADMJP` (`SYS_ACT_JPNAME()`)**: Sourced at runtime from the Airflow execution context as the parent DAG ID (e.g., `context['dag'].dag_id`).
*   **`ADMJOB` (`SYS_ACT_JOBNAME()`)**: Sourced at runtime from the Airflow execution context as the Task ID (e.g., `context['task'].task_id`).
*   **`ADMNRJOB` (`SYS_ACT_JOBNR()`)**: Sourced at runtime from the Airflow execution context as the DAG run ID or try number (e.g., `context['dag_run'].run_id` or `context['ti'].try_number`).
*   **`DWH_JOB_KENNUNG`**: Sourced as a job-specific local script variable initialized to an empty string `""`.
*   **`ADMMONJP` (`PREP_PROCESS_VAR("DW.DWH_MONITORED_JPS")`)**: Sourced as a shared runtime configuration lookup. This references a global variable container that lists monitored workflows.
*   **`ADMGB` / `ADMWERT` (`GET_PROCESS_LINE(...)`)**: Dynamically extracted line items parsed from the `DW.DWH_MONITORED_JPS` global variable at runtime.
*   **`&ADMJOB = '&ADMNRJOB'` (updated in `DW.DWH_RUNNING_JOBS`)**: Represents a dynamic state update to a global register tracking currently active processes.

---

### Lineage
*   **Lineage**: No upstream producers or downstream consumers were found for this file.

---

### Cross-File Dependencies
*   **Shared Lookup Container (`DW.DWH_MONITORED_JPS`)**: This script relies on reading a shared UC4 VARA-based lookup dataset to determine which parent job plans require monitoring registration.
*   **Active Monitoring Registry (`DW.DWH_RUNNING_JOBS`)**: This script relies on writing a key-value entry into a shared tracking state to log that the job execution has successfully commenced.

---

### Target File Plan
*   **Target File Path**: `uc4_airflow/DW_DWH_ADM_JOB_MONITOR_START.py`
    *   **Language**: Python
    *   **Source File**: `uc4_airflow/DW.DWH_ADM_JOB_MONITOR_START.xml`
    *   **Literal Output Rule Enforcement**: The following print and log statements contain literal strings from the source script. These must be executed exactly as defined, maintaining original phrasing and casing (including German logs):
        *   Commented log: `Job {admjob} mit RNR {admnrjob} gestartet aus {admjp}`
        *   Active log: `Added {admjob} with {admnrjob}`

---

### Environment-Specific Values
The environment-specific shared data structures and context indicators used by this script are classified below:

1.  **GLOBAL (Environment-Wide)**
    *   **`DW.DWH_MONITORED_JPS`**: A shared dataset indicating monitored workflows. On Cloud Composer, this must be sourced as a global Airflow Variable or queried from a shared metadata table in BigQuery:
        ```python
        from airflow.models import Variable
        DWH_MONITORED_JPS = Variable.get("DW_DWH_MONITORED_JPS", deserialize_json=True)
        ```
    *   **`DW.DWH_RUNNING_JOBS`**: A shared registry tracking active task states. In the target environment, this should be maintained using a global Airflow Variable or via updates to a persistent centralized logging/monitoring table in BigQuery.

2.  **JOB-SPECIFIC**
    *   **`SYS_ACT_JPNAME()` / `SYS_ACT_JOBNAME()` / `SYS_ACT_JOBNR()`**: Instance-specific execution metadata provided dynamically by the Airflow DAG run environment. Sourced via TaskInstance parameters:
        ```python
        admjp = context['dag'].dag_id
        admjob = context['task'].task_id
        admnrjob = context['run_id']
        ```

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow/DW.DWH_ADM_JOB_MONITOR_START.xml` | `uc4_airflow/DW_DWH_ADM_JOB_MONITOR_START.py` | Converts the UC4 Job Include (`JOBI`) script block into a reusable Python helper library module, mirroring the original folder structure. |