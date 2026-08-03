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
This bundle consists of a single UC4 Unix job (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`) which performs a placeholder or administrative scripting operation within the UC4 environment. Based on its script body, it prints a dummy diagnostic message and contains no functional business logic or data processing. It is not triggered by any schedule or SCRI object in this bundle and is therefore flagged as externally triggered.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | Active (1) | dummy |

---

## 3. Scheduling
- **Trigger Source**: Externally triggered (no schedule, parent JOBP workflow, or SCRI trigger object was supplied in this extraction bundle).
- **DAG Schedule**: `schedule=None` (No calendar/time triggers detected).

---

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dummy_execution` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | #REVIEW-STRUCT: Launcher command `[:print Doing nothinig]` is unrecognized UC4 script syntax. Confirm target operator/script manually. |

---

## 6. Task Dependency Map
Since this DAG contains only one functional task, there are no dependencies to define.
```python
dummy_execution
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` (locks or mutual exclusions) are defined for this object. No concurrency limitations apply other than the standard single-run restriction defined in the DAG settings.

---

## 8. Error Handling and Retry Strategy
- Default failure behavior: Airflow standard task failure notifications.
- Retries: Defined as `1` with a `5-minute` delay.
- No `on_failure_callback` or postcondition action logic was supplied in this object extraction.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Sanitized Airflow DAG ID | `dw_dwh_dummy_absd_plato_tarife` |

---

## 10. Developer Notes
* **#REVIEW-STRUCT: Unrecognized Launcher**: The Unix job utilizes an internal UC4 scripting command (`:print Doing nothinig`) instead of an executable shell instruction. It has been mapped to an `EmptyOperator`. Verify if this job can be completely retired in Airflow, or if it should execute a logging/shell script.
* **External Trigger Source**: No scheduling elements exist. The DAG must be executed on-demand, or triggered externally via Airflow API or a dataset trigger.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Migration of UC4 DWH Dummy Plato Tarife Job',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated', 'uc4', 'jobs_unix'],
) as dag:

    # ── Task: dummy_execution ────────────────────────────
    # REVIEW-STRUCT: Launcher command [:print Doing nothinig] not recognised.
    # Mapped to EmptyOperator. Confirm target operator or script manually if necessary.
    dummy_execution = EmptyOperator(
        task_id='dummy_execution',
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-task DAG. No dependencies to define.
    dummy_execution
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Migrates the UC4 dummy UNIX job to a Cloud Composer DAG containing an EmptyOperator to preserve the legacy workflow structure and orchestration flow. |

---

### Job dependencies
- **Downstream Job**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (not yet migrated). Once migrated, this downstream plan will consume the completion of this dummy task. This can be wired using a `TriggerDagRunOperator`, an `ExternalTaskSensor`, or by nesting this task directly into the parent DAG if the workflows are consolidated.

### Lineage
- **Downstream Consumer**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (job) - references this dummy task's completion before continuing its daily processing.

### External system replacements
- **Host `EXT:DWHDWH1P` and Package `PACKAGE:DW.UNIX.ISTNS`**: These legacy UNIX execution dependencies are replaced natively by Cloud Composer’s default execution framework. Because this is a dummy synchronization task, no external infrastructure connection or package installation is needed.

### Cross-file dependencies
- **Parent Plan Hierarchy**: This job file resides within the folder structure of the parent plan `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`, representing a direct structural dependency. This dummy task is designed to execute as a node within that broader workflow context.

### Target file plan
- **Target File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
  - **Language**: Python (Airflow DAG)
  - **Source File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`

### Environment-specific values
- `DWHDWH1P` (Host): GLOBAL. Target execution infrastructure. Since this dummy task has no executable workload, this host configuration is retired and requires no GCP resource representation.
- `DW.UNIX.ISTNS` (Login): JOB-SPECIFIC. Represents legacy execution permissions. Retired in GCP/Cloud Composer as tasks execute under Composer's default Service Account permissions.

### Risks and manual steps
- **Downstream Not Yet Migrated**: The downstream consumer `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is marked as "not yet migrated". The orchestration mapping and dependency wiring cannot be finalized until that workflow is migrated to Airflow.
- **Literal Print Rule Verification**: If any logging, echo, or print behavior is reintroduced to this task (instead of using a clean `EmptyOperator`), the literal text `Doing nothinig` (including the original spelling mistake) must be retained character-for-character in the target code.