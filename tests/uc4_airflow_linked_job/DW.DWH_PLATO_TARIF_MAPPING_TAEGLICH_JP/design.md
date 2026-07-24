# Migration Design Document

## File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Migrates the UC4 UNIX placeholder job definition into a 1:1 equivalent Airflow DAG with an `EmptyOperator` task. |

***

### VERBATIM UC4_DESIGN_AIRFLOW_DAG OUTPUT:

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
This extraction bundle contains a single UC4 `JOBS_UNIX` object named `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`. Based on the extracted metadata, this job serves as a "dummy" or placeholder step (with the title "dummy" and script body `:print Doing nothinig`). Because no workflow (`JOBP`) or script trigger (`SCRI`) was supplied in this bundle, this job is treated as an independent work item. To enable its migration to Apache Airflow, it is represented as a single-task DAG. It has no internal scheduling or parent workflow context within this bundle, making it an externally triggered asset.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | Active (1) | dummy |

---

## 3. Scheduling
* **Schedule Analysis**: No `EVNT_TIME` (calendar/time schedule) object is present in this extraction bundle.
* **Trigger Source**: No parent `JOBP` or calling `SCRI` object was supplied in this bundle. The job is marked as externally triggered, with the precise operational caller unknown from this extraction context alone.
* **Airflow Schedule**: `schedule=None` (manual/external trigger only).

---

## 4. Airflow DAG Properties
Since no parent `JOBP` was supplied, a dedicated DAG is created to wrap this single UNIX job.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Derived from Active=1) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_dummy_absd_plato_tarife` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 5 min | N/A | N/A | N/A | N/A | # REVIEW-STRUCT: launcher command `[:print Doing nothinig]` not recognised — confirm target operator/script manually. |

---

## 6. Task Dependency Map
```
dw_dwh_dummy_absd_plato_tarife (Single-task DAG; no execution dependencies)
```

---

## 7. Sync / Concurrency Analysis
No sync rows or cross-locking definitions were supplied for this object. The DAG-level `max_active_runs=1` is sufficient to prevent parallel execution conflicts.

---

## 8. Error Handling and Retry Strategy
* **Retries**: Standard task retries are set to `1` with a `5-minute` delay via `default_args`.
* **Failure Handling**: No custom postconditions, notification objects, or failure callbacks were declared. Tasks will fail silently or rely on global Airflow failure notification configurations.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Target Job Object | DAG ID: `dw_dwh_dummy_absd_plato_tarife`<br>Task ID: `dw_dwh_dummy_absd_plato_tarife` |

---

## 10. Developer Notes
* **#REVIEW-STRUCT**: The launcher type for `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` was classified as `unrecognized` because of the native UC4 script command `:print Doing nothinig`. It has been mapped to an `EmptyOperator` stub. Verify if this job is a genuine dummy execution step or if it needs to execute an actual script or operational task.
* **#REVIEW**: No parent `JOBP` (Workflow) or `SCRI` (Script) wrapper was supplied in this extraction bundle. The job is wrapped into its own independent DAG. Confirm if this job should instead be incorporated as a task within a larger downstream DAG workflow.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP resources or scripts required for this placeholder task.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure handling or callbacks required.

# ── DAG Definition ───────────────────────────────────────
# # REVIEW: No parent JOBP was supplied; creating a standalone DAG for this single task.
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    description='Converted DAG for UC4 job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Guard Task ──────────────────────────────────────────
    # None required (No self-lock Else=Skip concurrency policy identified)

    # ── Sensor Task ─────────────────────────────────────────
    # None required (No earliest start time constraints defined)

    # ── Calendar Check Task ─────────────────────────────────
    # None required (No calendar constraints defined)

    # ── Task: dw_dwh_dummy_absd_plato_tarife ────────────────
    # # REVIEW-STRUCT: Launcher command ':print Doing nothinig' not recognised — confirm target operator/script manually.
    dw_dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task DAG; no dependency definitions needed.
    dw_dwh_dummy_absd_plato_tarife
```

***

## Context & Target Environment Mapping (Additional Context)

### 1. Job Dependencies
* **Upstream**: None discovered in the job context.
* **Downstream**: 
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` (not yet migrated). 
  * **Target Wiring**: Because the downstream daily workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated, this dummy step cannot be wired into its orchestrator automatically. Once the downstream workflow is converted, this DAG should either be triggered via Airflow's `TriggerDagRunOperator` or integrated directly inside the downstream DAG as a task block to preserve the execution order.

### 2. Execution Order
* No specific execution steps or internal task sequence exist. This is a single-task placeholder job.

### 3. Scheduling
* No scheduling definitions are present in the context. The job is triggered purely by the downstream parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.

### 4. Schedule & Variables
* No environment variables or scheduler-set variables are defined in the XML.

### 5. Lineage
* **Upstream Producers**: None.
* **Downstream Consumers**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (represented as `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml`).
* **Lineage Connections**:
  * `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` `--[USES_PACKAGE]--> PACKAGE:DW.UNIX.ISTNS` (Legacy Execution Package Context)
  * `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` `--[CALLS_HTTP]--> EXT:DWHDWH1P` (Legacy Execution Host Connection)

### 6. External System Replacements
* **Legacy Execution Host (`|DWHDWH1P|HOST` / `EXT:DWHDWH1P`)**: Replaced natively by the Cloud Composer Kubernetes worker execution context. No external host SSH or HTTP triggers are required since the script contains no active commands.
* **Legacy Login Domain (`DW.UNIX.ISTNS`)**: Replaced by the native service account and IAM configurations in Google Cloud Platform (GCP).

### 7. Cross-File Dependencies
* This job is part of the daily Plato Tarif mapping execution group. It has a logical execution tie to the parent mapping job `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.

### 8. Target File Plan
* **Target File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`

### 9. Environment-Specific Values
The legacy variables identify infrastructure domains and logins. Under the Environment Variable Policy, they are classified as follows:
* **`DW.UNIX.ISTNS`** [GLOBAL]: Represents the execution security domain. Managed via Composer/Airflow DAG `default_args` owner configuration or Google Cloud Service Account IAM bindings.
* **`|DWHDWH1P|HOST`** [GLOBAL]: Legacy host designation. Retired in GCP as tasks execute inside Cloud Composer.

### 10. Risks & Manual Actions
* **DOWNSTREAM NOT YET MIGRATED**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` is not yet migrated. The final orchestration sequence cannot be verified or tested in isolation until the parent DAG is built.
* **CONSOLIDATION OPPORTUNITY**: This is an empty dummy synchronization step. It is highly recommended to merge this step directly into the downstream DAG `dw_dwh_plato_tarif_mapping_taeglich_jp` as a starting `EmptyOperator` task instead of deploying it as an independent standalone DAG file to avoid Composer DAG scheduling overhead.
* **OUTPUT/PRINT LITERAL RULE**: The legacy print log `:print Doing nothinig` (including the spelling error `nothinig`) has been preserved in the design comments to match the legacy behavior. Do not attempt to fix or correct this typo in the final deployment logs.