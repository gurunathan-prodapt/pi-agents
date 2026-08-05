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


# UC4 to Apache Airflow Migration Design Document

## 1. Overview
UNCERTAIN: This extraction contains only a single `JOBS_UNIX` object without a parent `JOBP` workflow, scheduling configuration (`EVNT_TIME`), or script trigger (`SCRI`). It represents a utility task named `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` with a title of "dummy". Its execution script prints a basic diagnostic text (`:print Doing nothinig`), suggesting it functions as a dummy or placeholder step. Because no wrapping workflow is present in this bundle, it is modeled as a standalone single-task DAG that is externally triggered, with its original source and execution context unknown from this extraction alone.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | 1 | dummy |

## 3. Scheduling
* **Schedule Analysis**: No `EVNT_TIME` scheduling object is present in this extraction. There are also no `SCRI` script triggers or parent `JOBP` workflows referencing this object within this bundle.
* **Trigger Source**: Externally triggered (source unknown from this extraction alone).
* **DAG Schedule**: `schedule=None` (no calendar/time trigger is present in the source extraction).

## 4. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife_task` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 300s | None | None | False | None | #REVIEW-STRUCT: launcher command [`:print Doing nothinig`] not recognised — confirm target operator/script manually. |

## 6. Task Dependency Map
```python
dw_dwh_dummy_absd_plato_tarife_task
```

## 7. Sync / Concurrency Analysis
No `sync_rows` or concurrency exclusions were defined in the extraction for this object. Standard `max_active_runs=1` is configured at the DAG level as a default safeguard.

## 8. Error Handling and Retry Strategy
* **Retries**: Standard 1 retry with a 5-minute delay is defined in the default arguments.
* **Postconditions**: No explicit postcondition actions or alert objects are defined in the extraction.
* **Execution Behavior**: Task is synchronous (not fire-and-forget).

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Primary executable object | DAG ID: `dw_dwh_dummy_absd_plato_tarife` |

## 10. Developer Notes
* **#REVIEW-STRUCT: Unrecognized Launcher**: The job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` uses an unrecognized command pattern (`:print Doing nothinig`). It has been mapped to an `EmptyOperator`. Verify whether this task should execute a real Shell/Bash command, Python script, or remains a dummy placeholder task.
* **Extraction Limitations**: Only a single `JOBS_UNIX` object was supplied. If this job is part of a larger workflow, it must be integrated as a task in that target DAG once the definition becomes available.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP resources required for EmptyOperator dummy task.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No callbacks required.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    description="Converted dummy task from UC4 DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: dw_dwh_dummy_absd_plato_tarife_task ────────
    # #REVIEW-STRUCT: launcher command ':print Doing nothinig' not recognised.
    # Mapped to EmptyOperator as it performs no functional operational work in UC4 script.
    dw_dwh_dummy_absd_plato_tarife_task = EmptyOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife_task",
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task DAG. No dependencies to define.
    dw_dwh_dummy_absd_plato_tarife_task
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
|:---|:---|:---|
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Converted to an Airflow DAG under the same relative folder structure, using an `EmptyOperator` to model this legacy dummy/synchronization task. |

### Job dependencies
* **Downstream Job**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is a downstream consumer of this job's completion state. Once both are migrated, this relationship should be established on Cloud Composer using an `ExternalTaskSensor` or a direct DAG trigger.
* **Not Yet Migrated**: The downstream consumer `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is currently marked as **not yet migrated**. This wiring cannot be fully finalized and verified in production until that target job is deployed.

### Lineage
* **Execution Host**: The legacy lineage indicates the job runs on `EXT:DWHDWH1P`. Since this is a placeholder task doing no physical work, no remote connection or execution on this host is required in the migrated Python file.
* **Package Association**: The legacy job uses `PACKAGE:DW.UNIX.ISTNS`. Any common orchestration variables or environment credentials associated with this package are treated as global Airflow configuration references.

### Cross-file dependencies
* **Shared Package**: The job references `DW.UNIX.ISTNS`. Ensure that any login credentials or path defaults provided by this package are available via Airflow Connections or global Variables if functional logic is added later.

### Target file plan
* **`uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`**:
  * **Source**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
  * **Language**: Python (Airflow DAG)
  * **Target Description**: Contains the Airflow DAG definition representing the dummy task.

### Environment-specific values
* **`GCP_PROJECT`** (GLOBAL): The Google Cloud Project hosting the Cloud Composer environment, retrieved via `Variable.get("GCP_PROJECT")` or the environment context.
* **`GCP_REGION`** (GLOBAL): The target GCP deployment region for the Cloud Composer infrastructure.
* **`DWHDWH1P`** (GLOBAL): Host identifier, represented as a global configuration variable or an Airflow Connection ID if execution tasks are eventually mapped to it.
* **`DW.UNIX.ISTNS`** (GLOBAL): Login metadata, represented in Airflow as a Connection/Credential profile for environment security consistency.

### Risks and manual steps
* **Unmigrated Downstream Workflow**: Because `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` has not been migrated, end-to-end integration testing and orchestration scheduling cannot be fully completed.
* **Functional Validation**: The legacy script executes `:print Doing nothinig`. Verify with operational teams that this job truly functions only as an orchestration synchronization point, and does not trigger manual external procedures or side effects on host `DWHDWH1P` that aren't captured in the XML code.