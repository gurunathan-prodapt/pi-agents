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
This extraction bundle contains a single active UC4 UNIX job (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`). Based on the extraction, this object serves as a dummy utility task that performs no actual workload operations, executing a UC4 script print command (`:print Doing nothinig`) instead of an operational OS-level script. Because no parent workflow (JOBP) or execution schedule (JSCH/EVNT) was provided in this bundle, this job is considered externally triggered or manually run, and will be wrapped in its own standalone Airflow DAG for migration.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | 1 | dummy |

---

## 3. Scheduling
* **Calendar-Based Schedule**: No calendar-based schedules, execution windows, or `EVNT_TIME` objects are defined in this bundle.
* **Trigger Mechanism**: There are no active `SCRI` triggering objects or parent `JOBP` workflow tasks referencing this object within this extraction. It is classified as externally triggered (source unknown from this extraction alone).
* **DAG Schedule**: `schedule=None` (manual/external trigger only).

---

## 4. Airflow DAG Properties
Since this is a standalone `JOBS_UNIX` object with no parent workflow, it is mapped to its own single-task Airflow DAG.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1 in UC4)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_dummy_absd_plato_tarife` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 5 min | N/A | N/A | N/A | None | #REVIEW-STRUCT: launcher command ':print Doing nothinig' not recognised — confirm target operator/script manually |

---

## 6. Task Dependency Map
This is a single-task workflow.
```python
dw_dwh_dummy_absd_plato_tarife
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` or mutual exclusion locks were specified for this object.
* **Airflow Mapping**: Standard DAG concurrency rules apply. `max_active_runs=1` is implemented by default to prevent parallel execution conflicts.

---

## 8. Error Handling and Retry Strategy
* **Retries**: Configured to inherit the default DAG retry configuration of 1 retry with a 5-minute delay.
* **Trigger Rules**: Standard `TriggerRule.ALL_SUCCESS` applies.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | UC4 UNIX Job | DAG: `dw_dwh_dummy_absd_plato_tarife` |

---

## 10. Developer Notes
* **Unrecognized Launcher**: The UC4 job script contains a native UC4 scripting directive (`:print Doing nothinig`) instead of an operating system command. Consequently, this task has been mapped to an `EmptyOperator`. 
* #REVIEW-STRUCT: The launcher command is unrecognized. Confirm if this task is purely diagnostic or if a specific script execution needs to be injected in place of the `:print` statement.
* **External Execution**: Since no orchestration wrapper (JOBP) is present in this bundle, this DAG has no defined schedule. Verify which upstream process or scheduler triggered this job in the legacy system.

---
---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP services or resources required for this dummy execution.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure callbacks required for this workflow.

# ── DAG Definition ────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    description='Converted from UC4 JOBS_UNIX object DW.DWH_DUMMY_ABSD_PLATO_TARIFE (dummy)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: dw_dwh_dummy_absd_plato_tarife ──────────────
    # #REVIEW-STRUCT: launcher command ':print Doing nothinig' not recognised — confirm target operator/script manually
    # Mapped to EmptyOperator as a placeholder representing the legacy dummy/print action.
    dw_dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task DAG; no dependency definitions needed.
    dw_dwh_dummy_absd_plato_tarife
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Migrates the UC4 Unix dummy job into an Apache Airflow DAG. Since the source only prints message text and performs no actual operations, it is represented as an EmptyOperator placeholder/synchronization task. |

---

# Migration Design Document Extra Context

### Job dependencies
* **Downstream**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (not yet migrated). Since this downstream target is not yet migrated, the cross-DAG relationship (such as a sensor or direct trigger) cannot be fully finalized or verified in the target Cloud Composer environment until the downstream job is deployed.

### Lineage
* **Downstream Consumers**:
  * `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (job: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) which acts as the downstream receiver of this dummy synchronization point.
* **Infrastructure Lineage**:
  * Legacy Run Host: `EXT:DWHDWH1P`
  * Legacy Package/Login: `PACKAGE:DW.UNIX.ISTNS`

### External system replacements
* **Host Run Target (`EXT:DWHDWH1P`)**: Since this is a dummy sync-point job running a native UC4 command (`:print Doing nothinig`) instead of an operating system level script, no execution target on a physical host is required. The execution is handled natively within Cloud Composer via the Airflow engine as an `EmptyOperator`.

### Cross-file dependencies
* **Shared Workflow**: This job acts as a placeholder within the parent logical block `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.

### Target file plan
* **Target File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py`
  * **Language**: Python (Airflow DAG)
  * **Source**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`

### Environment-specific values
* **Host (`DWHDWH1P`)** — **GLOBAL**: Identifies the legacy host environment. In the target environment, the physical server is bypassed as execution occurs directly on the Google Cloud Composer / GKE infrastructure.
* **Login Owner (`DW.UNIX.ISTNS`)** — **JOB-SPECIFIC**: Defines the credential owner. In the target environment, this maps to the Airflow task owner configuration or Airflow connection profile, defaulted to `dw.unix.istns`.

### Risks and manual steps
* **Unmigrated Downstream Dependency**: Downstream job `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is marked as "not yet migrated". Manual alignment of the Airflow trigger/sensor dependencies will be required once both DAGs are active on Cloud Composer.
* **Dummy Script Print Action**: The source script contains a native UC4 `:print Doing nothinig` directive rather than an actual shell operation. It has been mapped to an `EmptyOperator`. If downstream logic or monitoring tools in the target environment expect a log trace of this literal print, a `BashOperator` running `echo "Doing nothinig"` must be substituted manually during the build phase.