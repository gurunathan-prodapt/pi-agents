=== OBJECT: DW.DWH_ADM_JOB_MONITOR_START (JOBI) ===
active=None
title=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: UC4 to Apache Airflow Migration

## 1. Overview
The provided extraction bundle contains a single UC4 Include (`JOBI`) utility object: **`DW.DWH_ADM_JOB_MONITOR_START`**. In UC4, JOBI objects are reusable script blocks designed to be embedded within other executable objects (such as Unix/Windows jobs) using the `:INCLUDE` directive. They cannot be executed independently or scheduled on their own. Because no parent workflows (`JOBP`) or calling jobs (`JOBS`) were supplied in this bundle, this design document establishes a stub DAG wrapper to preserve the asset's existence. In a production migration, the contents of this Include object should be converted into a shared Python helper module, an Airflow plugin, or a custom operator.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ADM_JOB_MONITOR_START` | JOBI | None | None |

## 3. Scheduling
- **Schedule:** `None`
- **Trigger Source:** This is a `JOBI` (Include) object. It has no native scheduling or execution capability. It is triggered only when included dynamically by other jobs at runtime. Since no calling objects were supplied in this extraction, its operational invocation details are externally managed and unknown.

## 4. Airflow DAG Properties
To support structural consistency, the JOBI object is represented conceptually below as a stub DAG:

| Property | Value |
| :--- | :--- |
| `dag_id` | `dw_dwh_adm_job_monitor_start` |
| `schedule` | `None` |
| `start_date` | `datetime(2023, 1, 1)` *(placeholder)* |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `is_paused_upon_creation` | `True` *(derived from active=None/inactive state)* |
| `default_args` | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_adm_job_monitor_start_include` | `DW.DWH_ADM_JOB_MONITOR_START` | `EmptyOperator` | N/A | N/A | 1 | 5m | N/A | N/A | N/A | N/A | **# REVIEW-STRUCT:** This is a UC4 JOBI (Include) object. No executable tasks or script body were provided in this bundle. Represented as a stub task. |

## 6. Task Dependency Map
Since only one stub task is defined for this JOBI asset:
```python
dwh_adm_job_monitor_start_include
```

## 7. Sync / Concurrency Analysis
No sync rows, concurrency constraints, or locks were defined for this object.
| UC4 Sync Else value | lock_kind | Airflow mapping |
| :--- | :--- | :--- |
| N/A | N/A | No concurrency constraints identified. |

## 8. Error Handling and Retry Strategy
- No specific postcondition actions or error-handling blocks were present in the extraction.
- Standard default retries (1 retry, 5-minute delay) are applied to the stub task.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_ADM_JOB_MONITOR_START` | JOBI Include Asset | `dw_dwh_adm_job_monitor_start` (DAG ID) |

## 10. Developer Notes
* **# REVIEW-STRUCT:** The only object supplied is a `JOBI` (Include) object: `DW.DWH_ADM_JOB_MONITOR_START`. In UC4, JOBI objects cannot execute independently. They are script templates merged into other jobs at runtime.
* **# REVIEW-STRUCT:** No parent workflows (`JOBP`) or calling jobs (`JOBS`) were supplied in this bundle. The operational context of this include script is unknown.
* **# REVIEW-STRUCT:** No script body or launcher commands were extracted for this JOBI. Developers must locate the underlying UC4 script lines for `DW.DWH_ADM_JOB_MONITOR_START` and determine if they should be migrated to a shared Python helper module, an Airflow Custom Operator, or embedded as a task/macro.

---

## Pseudocode

```python
# ==============================================================================
# ── Imports ───────────────────────────────────────────────────────────────────
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# ── GCP Configuration ─────────────────────────────────────────────────────────
# ==============================================================================
# # REVIEW-STRUCT: No GCP resources required for this JOBI stub.
# If migrating script logic to GCP, specify project, region, and bucket variables here.

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
# No custom failure callbacks were defined in the source metadata.

# ==============================================================================
# ── DAG Definition (dw_dwh_adm_job_monitor_start) ─────────────────────────────
# ==============================================================================
# # REVIEW-STRUCT: This DAG is a stub representation of a UC4 JOBI (Include) object.
# JOBI objects are typically migrated as shared modules/plugins rather than DAGs.
with DAG(
    dag_id='dw_dwh_adm_job_monitor_start',
    default_args=DEFAULT_ARGS,
    description='Migration stub for UC4 Include asset DW.DWH_ADM_JOB_MONITOR_START',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['migrated_uc4', 'jobi_include'],
) as dag:

    # ── Guard Task ────────────────────────────────────────────────────────────
    # No self-lock Else=Skip sync detected. No Guard task required.

    # ── Sensor Task ───────────────────────────────────────────────────────────
    # No earliest_start_time constraint detected.

    # ── Calendar Check Task ───────────────────────────────────────────────────
    # No calendar constraints detected.

    # ── Task: dwh_adm_job_monitor_start_include ───────────────────────────────
    # # REVIEW-STRUCT: Locate original UC4 script body for DW.DWH_ADM_JOB_MONITOR_START.
    # Convert script logic to a Python module or custom operator instead of this EmptyOperator stub.
    dwh_adm_job_monitor_start_include = EmptyOperator(
        task_id='dwh_adm_job_monitor_start_include',
    )

    # ==============================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────────
    # ==============================================================================
    # Single-node task asset. No dependencies.
    dwh_adm_job_monitor_start_include
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow/DW.DWH_ADM_JOB_MONITOR_START.xml` | `uc4_airflow/dw_dwh_adm_job_monitor_start.py` | Converts the UC4 Job Include (JOBI) script logic into a reusable Python helper module containing the monitoring registration function. |

---

### Schedule & Variables

* **Schedule**: As a JOBI (Include) object, this asset does not have an independent trigger schedule. It is designed to be dynamically invoked at the start of any parent workflow execution.
* **Variables**:
  * `ADMJP` (Parent Job Plan Name): Resolved dynamically via the Airflow context using the DAG ID (`context['dag'].dag_id`).
  * `ADMJOB` (Job Name): Resolved dynamically via the Airflow context using the Task ID or DAG ID (`context['task_instance'].task_id` or `context['dag'].dag_id`).
  * `ADMNRJOB` (Job Run ID): Resolved dynamically via the Airflow context using the unique execution Run ID (`context['run_id']`).
  * `DWH_JOB_KENNUNG`: Initialized to empty string `""`.
  * `ADMMONJP` (Monitored Job Plans): Maps to a query against the BigQuery metadata table representing `DW.DWH_MONITORED_JPS`.
  * `ADMGB` (Monitored Item): Sourced from the first column of the monitored job plans table.
  * `ADMWERT` (Monitoring Status Indicator): Sourced from the second column of the monitored job plans table (value `"J"` represents active monitoring).
  * `DW.DWH_RUNNING_JOBS`: Target registry updated by writing/inserting a key-value pair of the active running job (`&ADMJOB` as key, `&ADMNRJOB` as value).

---

### External System Replacements

* **UC4 Variable Objects (VARA)**: 
  * `DW.DWH_MONITORED_JPS` maps to a BigQuery configuration/metadata table: `dw_metadata.dwh_monitored_jps`.
  * `DW.DWH_RUNNING_JOBS` maps to an active tracking metadata table: `dw_metadata.dwh_running_jobs`.
  * The Python registration function uses the Google Cloud BigQuery client library to read the monitoring configuration and record active runs.

---

### Cross-File Dependencies

* **Upstream / Shared Invocation**: Any Airflow DAG that requires start-monitoring tracking will import the `dwh_adm_job_monitor_start` function from `uc4_airflow.dw_dwh_adm_job_monitor_start` and execute it at the start of its run (either as the first task or within an `on_execute_callback`).

---

### Target File Plan

* **`uc4_airflow/dw_dwh_adm_job_monitor_start.py`**:
  * **Language**: Python (Python 3)
  * **Source**: `uc4_airflow/DW.DWH_ADM_JOB_MONITOR_START.xml`
  * **Purpose**: Houses the Python utility function `dwh_adm_job_monitor_start` which queries `dw_metadata.dwh_monitored_jps` for active parent DAGs and registers the running instance into `dw_metadata.dwh_running_jobs`.
  * **Output/Print Logging Preservation**:
    * If the commented-out statement is restored, it must log character-for-character:
      `f"Job {admjob} mit RNR {admnrjob} gestartet aus {admjp}"`
    * The registration verification logging statement must log character-for-character:
      `f"Added {admjob} with {admnrjob}"`

---

### Environment-Specific Values

1. **GLOBAL**
   * `GCP_PROJECT`: Sourced dynamically using `os.environ.get("GCP_PROJECT")` or Airflow variable `Variable.get("GCP_PROJECT")`. Identifies the GCP project housing the BigQuery metadata.
   * `BQ_DATASET`: Sourced dynamically using `os.environ.get("BQ_DATASET", "dw_metadata")`. Identifies the shared dataset where the monitoring registry tables live.

2. **JOB-SPECIFIC**
   * `MONITORED_JPS_TABLE`: The specific table path in BigQuery: `dw_metadata.dwh_monitored_jps`.
   * `RUNNING_JOBS_TABLE`: The specific table path in BigQuery: `dw_metadata.dwh_running_jobs`.

---

### Risks and Manual Steps

* **Table Schema Creation**: The target BigQuery metadata tables `dw_metadata.dwh_monitored_jps` and `dw_metadata.dwh_running_jobs` must be manually provisioned in the environment before any DAG calling this helper can run.
* **Context Extraction in Airflow**: Developers implementing the DAG-level integrations must ensure that the `context` dictionary is passed appropriately to extract the runtime `dag_id` and `run_id` correctly.