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


# UC4 Workload Automation Migration: Technical Design Document

This document outlines the migration design and structural patterns for converting the extracted UC4/Automic workload definitions into Apache Airflow DAGs.

---

## 1. Overview

This migration package consists of a standalone, native UC4 UNIX job (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`) with an active status. It functions as a placeholder or dummy task that executes a basic print utility statement on a target UNIX host. Because no parent Workflow (`JOBP`), Schedule (`JSCH`), or native trigger Script (`SCRI`) was supplied within this extraction bundle, this job is represented as an independent, single-task Airflow DAG configured for manual or external triggering. The underlying command launcher is categorized as unrecognized, requiring mapping to an `EmptyOperator` task acting as a functional stub.

---

## 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | 1 (Active) | dummy |

---

## 3. Scheduling

* **Schedule Strategy**: This workflow contains no calendar-based trigger objects (such as `EVNT_TIME` or `JSCH` schedule rules) inside this extraction bundle. No triggering script (`SCRI`) or parent workflow is present.
* **Trigger Source**: Externally triggered (source unknown from this extraction alone).
* **Airflow Schedule**: `schedule=None` (manual/API execution only; no cron schedule is assumed or invented).

---

## 4. Airflow DAG Properties

Since no parent `JOBP` workflow was supplied, the standalone `JOBS_UNIX` object is represented as a single-task DAG.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active flag = 1 maps to False)* |
| **default_args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': duration(minutes=5)}` |

---

## 5. Task Inventory

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_dummy_absd_plato_tarife` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 0 | N/A | N/A | N/A | False | None | #REVIEW-STRUCT: launcher command `[:print Doing nothinig]` not recognised — confirm target operator/script manually. This is a dummy script. |

---

## 6. Task Dependency Map

```
[dw_dwh_dummy_absd_plato_tarife]
```
*(Single-task workflow; no upstream or downstream dependencies exist within this bundle.)*

---

## 7. Sync / Concurrency Analysis

No Sync (exclusion) definitions or resource locks were declared for this object within the extraction. Concurrency control is limited to default DAG settings.

| UC4 Sync Else value | lock_kind | Airflow mapping |
| :--- | :--- | :--- |
| N/A | N/A | `max_active_runs=1` is configured as a standard execution guard. |

---

## 8. Error Handling and Retry Strategy

* No postcondition actions, execution blocks, or auto-retries were declared in the source object. 
* Standard Airflow failure execution applies (task state marked as `failed` immediately upon non-zero execution exit, though as an `EmptyOperator` placeholder, this task will complete successfully by default).

---

## 9. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Target Job Object Name | `dw_dwh_dummy_absd_plato_tarife` (DAG ID / Task ID) |

---

## 10. Developer Notes

* **#REVIEW-STRUCT: Unrecognized Launcher**: The target execution body consists of a native UC4 `:print Doing nothinig` script statement rather than standard Unix execution script syntax. It has been mapped to an `EmptyOperator`. The developer must verify if this dummy task is still required in the production ecosystem, or if it can be safely deprecated.
* **#REVIEW-STRUCT: Standalone Migration**: Since this object was extracted in isolation without a wrapping `JOBP` workflow, it is converted into a standalone DAG. The developer should confirm if this task must be embedded as a child node within a larger orchestration DAG instead of operating independently.

---

# Airflow DAG Pseudocode

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
# No GCP connections or GCS storage paths required for this placeholder task.

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── on_failure_callback stubs ─────────────────────────────────────────────────
# ==============================================================================
# No custom error callbacks or alerts are declared for this job.

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    description="Converted from UC4 standalone JOBS_UNIX: DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule_interval=None,  # No calendar schedule present in extraction
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active=1 in extraction
    tags=["uc4_migration", "jobs_unix"],
) as dag:

    # ==========================================================================
    # ── Task: dw_dwh_dummy_absd_plato_tarife ──────────────────────────────────
    # ==========================================================================
    # #REVIEW-STRUCT: Launcher type "unrecognized" with command ':print Doing nothinig'.
    # Converted to EmptyOperator placeholder pending manual script review/deprecation.
    dw_dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife",
    )

    # ==========================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────
    # ==========================================================================
    # Single standalone task. No dependencies to establish.
    dw_dwh_dummy_absd_plato_tarife
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Converted to an Airflow DAG with an `EmptyOperator` task acting as a structural synchronization point, preserving the original folder structure. |

---

### Job Dependencies

* **Downstream Job**: 
  * `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (Status: *not yet migrated*)
  * **Target Wiring**: This downstream relationship must be wired on BigQuery/Cloud Composer. Once `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is migrated, this can be implemented via an Airflow cross-DAG dependency trigger (`TriggerDagRunOperator` in this DAG or an `ExternalTaskSensor` in the downstream DAG). Because the downstream target is not yet migrated, a manual verification step is logged below.

---

### Lineage

* **Downstream Consumer**: 
  * `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (job: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) — This is a cross-job hand-off to reference and is not part of this conversion group.
* **External Connections / HTTP Call**:
  * `EXT:DWHDWH1P` (confidence: 0.85) — Corresponds to the host environment execution attribute `|DWHDWH1P|HOST`.
* **Package Dependency**:
  * `PACKAGE:DW.UNIX.ISTNS` (confidence: 0.80) — Corresponds to the Login configuration `DW.UNIX.ISTNS` used for job execution context.

---

### External System Replacements

* **UNIX Host (`DWHDWH1P`)**: Since the target environment is Cloud Composer, this host designation is replaced by the Composer environment's native worker context. If remote execution is eventually required on a specific VM, this maps to an Airflow SSH connection.
* **Execution Login (`DW.UNIX.ISTNS`)**: This package/login credential maps to an Airflow connection configuration or service account role in Google Cloud.

---

### Target File Plan

* **File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`
  * **Language**: Python (Airflow DAG)
  * **Source**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
  * **Description**: Contains the Airflow DAG with an `EmptyOperator` representing the dummy synchronization task.

---

### Environment-Specific Values

1. **GLOBAL (Environment-wide)**:
   * `DWHDWH1P` (Host): Identifies the legacy execution server. In the target Airflow environment, this is represented globally via a target environment Airflow Connection ID or connection parameter (e.g., `GCP_CONN_DWHDWH1P`) if remote task invocation is utilized, otherwise retired.

2. **JOB-SPECIFIC**:
   * `DW.UNIX.ISTNS` (Login): Represents the execution credentials/package. It is assigned to the DAG/Task configuration via Airflow task parameter overrides or standard execution service accounts.

---

### Risks and Manual Steps

* **Downstream Connection Risk**: The downstream dependency `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated, so the downstream scheduling linkage cannot be finalized. A post-migration step is required to wire the cross-DAG trigger or sensor once the downstream workflow is deployed.
* **Placeholder Verification**: The source job executes a dummy command (`:print Doing nothinig`). In the Airflow DAG, this has been mapped to an `EmptyOperator`. System architects must verify if this synchronization/checkpoint DAG is still required in the target cloud schedule or if it can be safely retired.
* **Preservation of Typos/Literals**: As per the Output/Print Literal Rule, the original typo in the print command (`Doing nothinig`) is preserved in comments and documentation to maintain character-for-character fidelity with the legacy system.