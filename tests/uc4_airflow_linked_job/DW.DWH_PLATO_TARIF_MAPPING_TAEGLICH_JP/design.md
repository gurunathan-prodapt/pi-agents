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


# UC4 Migration Design Document: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## 1. Overview
*UNCERTAIN: This extraction contains only a single standalone UC4 UNIX Job (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`) with no wrapping workflow (JOBP) or script trigger (SCRI).* 
The job appears to perform a dummy printing operation (`:print Doing nothinig`) using native UC4 scripting syntax. Because there is no workflow container provided in this extraction, this job has been wrapped in a standalone single-task Airflow DAG to represent its execution. It has no internal data dependencies or processing logic and is assumed to be externally triggered.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | 1 (Active) | dummy |

## 3. Scheduling
- **Schedule**: No schedule or time-based execution rules are defined in this extraction.
- **Trigger**: Externally triggered (source unknown from this extraction alone; no JOBP or SCRI was supplied to invoke this job).
- **DAG Rule**: `schedule=None` (no manual cron expressions have been generated).

## 4. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Derived from Active=1) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `dummy_absd_plato_tarife` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | # REVIEW-STRUCT: launcher command [`:print Doing nothinig`] not recognised — this uses native UC4 script syntax instead of a shell execution. Mapped to EmptyOperator. |

## 6. Task Dependency Map
Since this DAG contains only a single task, there are no dependency transitions:
```python
dummy_absd_plato_tarife
```

## 7. Sync / Concurrency Analysis
No sync rows or mutual exclusion locks are defined for this object.

## 8. Error Handling and Retry Strategy
- Default task retries are set to `1` with a `5-minute` retry delay.
- No custom callbacks or complex failure strategies are defined.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Object Name | `dw_dwh_dummy_absd_plato_tarife` (DAG ID) |

## 10. Developer Notes
- # REVIEW-STRUCT: The launcher command `:print Doing nothinig` is a UC4 native script statement, not a standard Unix shell execution script. Because it has been classified as an unrecognized launcher type, it has been mapped to an `EmptyOperator` as a dummy placeholder. Confirm with the business if this job is intended to perform any real execution on migration or if it should remain an operational placeholder.
- # REVIEW: This extraction contains no JOBP (workflow) container. A wrapper DAG (`dw_dwh_dummy_absd_plato_tarife`) has been generated to house this single task. Verify if this task should be integrated into a broader migration DAG instead of running in isolation.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP resources or scripts are referenced by this dummy job.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No callbacks required.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    description='Dummy Unix Job migrated from UC4 object DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # None required (no self-lock Else=Skip or Else=Wait constraints).

    # ── Sensor Task ──────────────────────────────────────
    # None required (no earliest start time constraints).

    # ── Calendar Check Task ──────────────────────────────
    # None required (no calendar constraints).

    # ── Task: dummy_absd_plato_tarife ────────────────────
    # # REVIEW-STRUCT: Mapped to EmptyOperator due to unrecognized UC4 native command ':print Doing nothinig'
    dummy_absd_plato_tarife = EmptyOperator(
        task_id='dummy_absd_plato_tarife',
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-task DAG; no dependency mapping required.
    dummy_absd_plato_tarife
```

### Job Dependencies
* **Downstream**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (Job Plan/DAG) — This downstream container is not yet migrated. The wiring (e.g., cross-DAG trigger or task dependency inside the same DAG) cannot be finalized until this downstream dependency is migrated to Airflow.

### Lineage
* **Infrastructure Host**: `EXT:dwhdwh1p` — The legacy UNIX host where this job was registered to run.
* **Package Dependency**: `PACKAGE:DW.UNIX.ISTNS` — The legacy login package/credential mapping associated with the execution.

### Cross-File Dependencies
* **Logical Parent**: The file path suggests this dummy job belongs logically to the `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` workflow folder. In Airflow, this will be represented as a task (`dummy_absd_plato_tarife`) within the overall `dw_dwh_plato_tarif_mapping_taeglich_jp` DAG, or as a standalone DAG if independent scheduling is verified.

### Target File Plan
* **Target File**: `uc4_airflow_linked_job/dw_dwh_plato_tarif_mapping_taeglich_jp/dw_dwh_dummy_absd_plato_tarife.py`
  * **Language**: Python (Airflow DAG)
  * **Source File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`

### Environment-Specific Values
* **`DWHDWH1P`** (Host) — **GLOBAL**: Identifies the legacy execution environment. In the migrated environment, this maps to target-wide variables or connection profiles if remote execution were needed, but since this is mapped to an `EmptyOperator`, no active execution connection is required.
* **`DW.UNIX.ISTNS`** (Login) — **GLOBAL**: The credentials context. In Cloud Composer, execution roles are managed globally via IAM or Airflow Connections.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/dw_dwh_plato_tarif_mapping_taeglich_jp/dw_dwh_dummy_absd_plato_tarife.py` | Converts the UC4 UNIX dummy job into an Airflow DAG with an EmptyOperator task representing this placeholder step in the workflow. |

### Risks & Manual Actions
* **Unmigrated Downstream Dependency**: The downstream workflow/parent DAG `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated. The cross-job orchestration links cannot be validated or enabled in production until the parent container is established.
* **No Direct Scripting Equivalent**: The UC4-native scripting statement `:print Doing nothinig` has no direct equivalent in Airflow/Python. It has been mapped to an `EmptyOperator` as a dummy task. If output printing is required, confirm with operations; any logging implementation must output the literal string `"Doing nothinig"` exactly.