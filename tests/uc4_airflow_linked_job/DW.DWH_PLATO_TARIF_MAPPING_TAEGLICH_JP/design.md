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
This extraction bundle contains a single standalone UC4 UNIX job, `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`. It is defined as a dummy execution job with no functional data processing logic, merely executing a UC4 script print directive (`:print Doing nothinig`). Because this extraction does not contain a parent workflow (`JOBP`) or schedule object (`EVNT_TIME`/`JSCH`), this job is considered externally triggered or represents a stub/utility job. It is migrated here into a single wrapper Airflow DAG to preserve its definition in the Airflow environment.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | 1 (Active) | dummy |

## 3. Scheduling
- **Schedule**: No `EVNT_TIME` or scheduling configuration is defined in this extraction. 
- **Trigger Source**: This workflow is externally triggered; the direct calling source is unknown from this extraction bundle.
- **Airflow Configuration**: `schedule=None`

## 4. Airflow DAG Properties
Since no parent `JOBP` workflow was provided, a wrapper DAG is created for this single job.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife_dag` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active flag is 1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_dummy_absd_plato_tarife` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | **# REVIEW-STRUCT**: launcher command `:print Doing nothinig` not recognised — confirm target operator/script manually |

## 6. Task Dependency Map
Since there is only a single task, the dependency structure is trivial:

```python
dw_dwh_dummy_absd_plato_tarife
```

## 7. Sync / Concurrency Analysis
No sync rows, mutual exclusion rules, or concurrency structures are declared for this job. Standard single-concurrency configuration applies (`max_active_runs=1`).

## 8. Error Handling and Retry Strategy
- Default failure and retry behavior are inherited from the DAG `default_args` (1 retry, 5-minute retry delay).
- No postcondition actions, alerts, or failure callbacks are configured in the extraction.
- The task execution is synchronous (no fire-and-forget logic).

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| Object Name | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | DAG ID: `dw_dwh_dummy_absd_plato_tarife_dag`<br>Task ID: `dw_dwh_dummy_absd_plato_tarife` |
| Host | `|DWHDWH1P|HOST` | N/A (Standardized to EmptyOperator stub) |
| Login | `DW.UNIX.ISTNS` | N/A (Standardized to EmptyOperator stub) |

## 10. Developer Notes
*   **# REVIEW-STRUCT (Unrecognized Launcher)**: The source object `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is a UNIX job with launcher type `unrecognized` because it contains only a UC4 internal script command (`:print Doing nothinig`) instead of native UNIX shell execution commands. This has been mapped to an `EmptyOperator`. Verify with the business logic team if this task can be entirely retired or if it should print/log output using a Python or Bash operator.
*   **External Integration**: Since this job is not nested inside a UC4 Jobplan (`JOBP`), confirm how this job was triggered in UC4 (e.g., via a master scheduler, external API call, or manual intervention) to integrate it correctly with the broader Airflow orchestration strategy.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP connections required for EmptyOperator tasks

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No callbacks required for this migration

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife_dag',
    default_args=DEFAULT_ARGS,
    description='Wrapper DAG for migrated UC4 object DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'dummy'],
) as dag:

    # ── Task: dw_dwh_dummy_absd_plato_tarife ─────────────
    # # REVIEW-STRUCT: The UC4 job 'DW.DWH_DUMMY_ABSD_PLATO_TARIFE' has an unrecognized 
    # launcher command (':print Doing nothinig'). Implementing as an EmptyOperator stub.
    dw_dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife'
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task workflow. No dependencies defined.
    dw_dwh_dummy_absd_plato_tarife
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Maps to an Airflow DAG file containing an `EmptyOperator` representing this dummy/placeholder UC4 UNIX job. |

# Additional Migration Context

### Job Dependencies
* **Downstream**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is a downstream consumer of this job's output. Because it is marked as **not yet migrated**, the orchestration link (such as via an Airflow `TriggerDagRunOperator` or `ExternalTaskSensor`) cannot be finalized until that workflow is migrated to Cloud Composer.

### Lineage
* **Upstream/Hosts**: The legacy job is defined to run on host `EXT:dwhdwh1p` under the login credential package `PACKAGE:DW.UNIX.ISTNS`.

### External System Replacements
* **Execution Host & Credentials**: The legacy UNIX host `dwhdwh1p` and Unix login credential package `DW.UNIX.ISTNS` are retired for this specific workflow. Since the converted task performs no functional actions and is implemented as an `EmptyOperator`, it executes entirely within Cloud Composer without needing external host connections or SSH credentials.

### Target File Plan
* **`uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`**: A Python-based Airflow DAG file. This file represents the migrated orchestration stub for the dummy job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` and preserves the folder hierarchy of the source XML.

### Environment-Specific Values
* **`|DWHDWH1P|HOST`** (Legacy Host): Classified as **GLOBAL** (environment-wide infrastructure). However, it is retired for this specific job since the workflow translates to an `EmptyOperator`.
* **`DW.UNIX.ISTNS`** (Legacy Login Package): Classified as **JOB-SPECIFIC** (credentials). It is retired for this specific job since no remote execution is performed.

### Risks and Manual Steps
* **Downstream Integration**: The downstream workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated. The connection and execution sequence between this DAG and its downstream target must be manually configured and verified once both workflows are active on Cloud Composer.
* **Redundant Synchronization Point**: Since this job's only action is an internal print statement (`:print Doing nothinig`), it acts purely as a dummy milestone. A manual architectural review is recommended to determine whether this task can be consolidated or retired entirely rather than maintained as a standalone DAG.