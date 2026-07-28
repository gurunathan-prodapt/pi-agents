=== OBJECT: DW.DWH_DUMMY_ABSD_PLATO_TARIFE (JOBS_UNIX) ===
active=1
title=dummy
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=11
launcher_type=unrecognized
launcher_details={'raw_command': ':print Doing nothinig'}
script_body:
:print Doing nothinig
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: UC4 to Apache Airflow Migration

## 1. Overview
This extraction contains a single standalone UC4 `JOBS_UNIX` object named `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`. Based on the script body and configuration, this object functions as a basic dummy job that prints a hardcoded message and performs no actual system-level actions or data processing. No scheduling definition (`EVNT_TIME`), parent job plan (`JOBP`), or trigger script (`SCRI`) was supplied in this bundle. Consequently, this job is determined to be externally triggered, with its upstream source unknown from this extraction alone.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|:---|:---|:---|:---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | Active (1) | dummy |

## 3. Scheduling
- **Schedule**: `schedule=None`
- **Trigger Source**: This workflow contains no calendar-based schedule of its own, nor is it referenced by any trigger objects (`SCRI` or parent `JOBP`) in this bundle. It is classified as **externally triggered**, with the original orchestration mechanism unknown.

## 4. Airflow DAG Properties
Since no parent `JOBP` workflow was provided, a standalone wrapper DAG is designed to encapsulate this single job to allow for independent scheduling, triggering, and execution in Apache Airflow.

| Property | Value |
|:---|:---|
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife_dag` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Mapped from Active=1) |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| `dw_dwh_dummy_absd_plato_tarife` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 5 min | N/A | N/A | N/A | N/A | #REVIEW-STRUCT: launcher command [:print Doing nothinig] not recognised — confirm target operator/script manually |

## 6. Task Dependency Map
Since there is only a single task inside this wrapper DAG, there are no dependencies to chart:
```
dw_dwh_dummy_absd_plato_tarife
```

## 7. Sync / Concurrency Analysis
No `sync_rows` (locks or mutual exclusions) are defined for this object. No additional guard tasks or concurrency limits are required beyond the standard DAG configuration of `max_active_runs=1`.

## 8. Error Handling and Retry Strategy
- **Retries**: Configured to inherit default DAG arguments (1 retry, with a 5-minute delay).
- **Triggers & Sensoring**: No earliest start time, calendar constraints, or `on_failure_callback` definitions exist for this simple task.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|:---|:---|:---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Object Name | `dw_dwh_dummy_absd_plato_tarife` (Task ID) |
| Standalone Wrapper | Generated Wrapper | `dw_dwh_dummy_absd_plato_tarife_dag` (DAG ID) |

## 10. Developer Notes
* **#REVIEW-STRUCT: Unrecognized Launcher Type**: The command `:print Doing nothinig` is unrecognized and treated as a native UC4 utility print statement rather than an actionable shell or application launcher. The task has been mapped to an `EmptyOperator` as a stub. Developers should verify if this job is intended to be completely deprecated, or if it needs to execute a lightweight shell command (e.g., `echo "Doing nothing"`) via a `BashOperator`.
* **External Triggering**: This job has no native schedule or parent workflow in this extraction. If this job needs to be triggered by an external process, consider exposing it via the Airflow REST API or using a `TriggerDagRunOperator` in an upstream DAG.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP services are targeted by this placeholder workflow.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No callbacks required for this standalone task.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife_dag',
    default_args=DEFAULT_ARGS,
    description='Wrapper DAG for standalone dummy job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # None required.

    # ── Sensor Task ──────────────────────────────────────
    # None required.

    # ── Calendar Check Task ──────────────────────────────
    # None required.

    # ── Task: dw_dwh_dummy_absd_plato_tarife ─────────────
    # #REVIEW-STRUCT: launcher command ':print Doing nothinig' not recognised — confirm target operator/script manually
    dw_dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
    )

    # ── Dependencies ─────────────────────────────────────
    # Standalone task — no dependency definitions required.
    dw_dwh_dummy_absd_plato_tarife
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Migrated to an Airflow DAG containing an `EmptyOperator` placeholder representing this dummy orchestration/synchronization point, maintaining folder structure integrity. |

---

### Job dependencies
* **Downstream**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` is listed as a downstream consumer but is **not yet migrated**. The orchestration/triggering connection between this dummy placeholder and its downstream consumer cannot be fully finalized until that downstream object has been migrated. Once migrated, this can be wired using a `TriggerDagRunOperator` or cross-DAG sensors.

### Lineage
* **Downstream consumers**: 
  * `EXT:DWHDWH1P` (via `CALLS_HTTP` lineage relationship)
  * `PACKAGE:DW.UNIX.ISTNS` (via `USES_PACKAGE` lineage relationship)

### External system replacements
* **EXT:DWHDWH1P** (Host): Represents the target environment host `|DWHDWH1P|HOST`. In Cloud Composer, host references are configured using standardized Airflow Connections (e.g., HTTP or SSH connections) rather than hardcoded environment properties.
* **PACKAGE:DW.UNIX.ISTNS** (Login / Package): Represents the legacy login context. In the target Cloud Composer environment, this maps to GCP Service Accounts, IAM configurations, or Airflow execution security contexts.

### Cross-file dependencies
* **Shared schemas & tables**: None. This is a dummy orchestration-only job.

### Target file plan
* **File Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
  * **Language**: Python (Airflow DAG)
  * **Source File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`

### Environment-specific values
* **DWHDWH1P** (Host): **GLOBAL**. This environment-wide value represents the target infrastructure host. It should be sourced dynamically at runtime using Airflow Connections or Composer Environment variables (e.g., via `Variable.get("GCP_HOST_CONN")`).
* **DW.UNIX.ISTNS** (Login): **GLOBAL**. This environment-wide value identifies the execution/authorization context. In Cloud Composer, it maps to the underlying GCP Service Account or Airflow Execution Connection.

### Risks and manual steps
* **Unmigrated Downstream Dependency**: The downstream consumer `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` is not yet migrated. A manual validation step is required during integration testing to establish the correct triggering sequence once the dependent DAG is in place.
* **Dummy Execution Validation**: The legacy UC4 job only performs a dummy logging action (`:print Doing nothinig`). This has been successfully modeled as an `EmptyOperator` in Airflow. Operators should manually verify if this job is strictly used as a synchronization/placeholder point, or if any external system is expecting a physical exit log output that might need to be simulated via a `BashOperator` (e.g., running `echo "Doing nothing"`).