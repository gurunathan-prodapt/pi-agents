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


# Migration Design Document: UC4 to Apache Airflow

## 1. Overview
This bundle contains a single UC4 UNIX job object (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`) representing a dummy utility or placeholder job. It executes a basic UC4 native print script statement rather than a standard operating system command or database process. Because no parent workflow (JOBP) or schedule triggers (SCRI/EVNT) are present in this extraction, the job is treated as an externally triggered, standalone workflow. In Airflow, this is represented as a single-task DAG utilizing an `EmptyOperator` due to its unrecognized scripting command launcher type.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | Active (1) | dummy |

## 3. Scheduling
- **Trigger Source**: This workflow contains no calendar-based schedule of its own, nor does the extraction bundle contain a `SCRI` or `JOBP` object triggering it. It is classified as **externally triggered** (source unknown from this extraction alone).
- **DAG Schedule**: `schedule=None` (no schedule trigger is defined).

## 4. Airflow DAG Properties
Since no parent `JOBP` wrapper was provided, the standalone `JOBS_UNIX` object is wrapped in a dedicated DAG to allow execution within Airflow.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Active=1 in export) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_dummy_absd_plato_tarife_task` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | launcher command `[:print Doing nothinig]` not recognised — confirm target operator/script manually. <br>`#REVIEW-STRUCT:` |

## 6. Task Dependency Map
```python
# Standalone single-task DAG
dwh_dummy_absd_plato_tarife_task
```

## 7. Sync / Concurrency Analysis
No sync rows or concurrency locks were defined for this object. The DAG-level parameter `max_active_runs=1` is set as a standard migration safety precaution.

## 8. Error Handling and Retry Strategy
- No custom postcondition actions, `on_failure_callback` actions, or complex retry policies are specified in the extraction.
- Standard default retries (1 retry with a 5-minute delay) will be applied via the DAG default arguments.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Object Name | `dw_dwh_dummy_absd_plato_tarife` (DAG ID) |

## 10. Developer Notes
* **Unrecognized Launcher**: The raw UC4 script body contains a native scripting instruction (`:print Doing nothinig`) instead of an executable shell script or binary. It has been mapped to an `EmptyOperator` representing a structural placeholder. Confirm if this task needs to perform actual work or can be safely ignored in the target environment. `#REVIEW-STRUCT:`
* **No Parent Jobplan (JOBP)**: This task was exported as an isolated `JOBS_UNIX` object. It has been wrapped in a dedicated, single-task DAG. Confirm if this task should instead be incorporated as a step inside another existing workflow. `#REVIEW-STRUCT:`

---

## Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP connections or variables required for this structural dummy task.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom error callbacks specified for this workflow.

# ── DAG Definition ───────────────────────────────────────
dag_id = 'dw_dwh_dummy_absd_plato_tarife'

with DAG(
    dag_id=dag_id,
    default_args=DEFAULT_ARGS,
    description='Migration wrapper for standalone UC4 dummy job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'dummy_task'],
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # None required (no lock_kind=self/Else=Skip syncs detected).

    # ── Sensor Task ──────────────────────────────────────
    # None required (no earliest_start_time constraint detected).

    # ── Calendar Check Task ──────────────────────────────
    # None required (no calendar constraints detected).

    # ── Task: dwh_dummy_absd_plato_tarife_task ───────────
    # #REVIEW-STRUCT: launcher command ':print Doing nothinig' not recognised.
    # Mapped to EmptyOperator as a placeholder.
    dwh_dummy_absd_plato_tarife_task = EmptyOperator(
        task_id='dwh_dummy_absd_plato_tarife_task',
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-task DAG; no dependency mapping required.
    dwh_dummy_absd_plato_tarife_task
```

### Job dependencies
- **Downstream Job**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`
  - **Migration Status**: Not yet migrated.
  - **Wiring Strategy**: On Cloud Composer, once the downstream workflow is migrated, the dependency should be wired using an `ExternalTaskSensor` in the downstream DAG or a `TriggerDagRunOperator` in this DAG. Since it is currently unmigrated, the cross-DAG relationship cannot be verified or finalized at this stage.

### Lineage
- **Upstream Producers**: None.
- **Downstream Consumers**:
  - `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (job: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) — This is a cross-job downstream hand-off.
- **Execution Host**: `EXT:dwhdwh1p`
  - **Target Mapping**: The legacy UNIX host `dwhdwh1p` is bypassed. Since the task is translated into an `EmptyOperator`, no actual remote host execution environment is required for this job in GCP.
- **Package Reference**: `PACKAGE:DW.UNIX.ISTNS`
  - **Target Mapping**: The UNIX credentials package `DW.UNIX.ISTNS` is retired for this task because native Airflow execution of the `EmptyOperator` does not require operating system logins.

### Target file plan
- **Target File Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`
  - **Source File Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
  - **Language**: Python (Airflow DAG)
  - **Purpose**: Implements the wrapper DAG containing a single `EmptyOperator` representing the legacy dummy/placeholder task `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.

### Environment-specific values
- **GCP_PROJECT** (GLOBAL): Identifies the target GCP environment project. Sourced dynamically in the DAG using `Variable.get("GCP_PROJECT")`.
- **GCP_REGION** (GLOBAL): Identifies the target GCP region. Sourced dynamically in the DAG using `Variable.get("GCP_REGION")`.
- **Legacy Host `DWHDWH1P`** (GLOBAL / Retired): Legacy Unix host identifier. Retired as no operating system command is executed on GCP.
- **Legacy Login `DW.UNIX.ISTNS`** (GLOBAL / Retired): Legacy login profile. Retired as native execution of `EmptyOperator` does not require a Unix user.
- **Queue `CLIENT_QUEUE`** (GLOBAL / Retired): UC4 execution queue. Retired as this task runs on the default Celery/Kubernetes queue in Airflow.

### Risks and manual steps
- **Downstream Dependency Wiring Gap**: The downstream job `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated. The exact connection mechanism (such as `TriggerDagRunOperator` or `ExternalTaskSensor`) cannot be finalized or verified until the downstream workflow is migrated to Cloud Composer.
- **Dummy Command Verification**: The original task executes `:print Doing nothinig` (retained with original typo). It has been mapped to `EmptyOperator`. Manual confirmation is required to ensure that this task indeed performs no other hidden side-effects or utility operations on the legacy environment.
- **Legacy Documentation Reference**: The legacy documentation indicates: `Wiederanlauf ohne weitere Maßnahmen möglich` (retained verbatim). This means a restart is possible without any manual corrective actions. In Airflow, this translates to setting `depends_on_past: False` and allowing clean task clearing and retries.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Converts the UC4 UNIX job configuration into an Airflow DAG with an `EmptyOperator` task to preserve the orchestration flow. |