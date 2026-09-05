=== OBJECT: DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE (JOBS_UNIX) ===
active=1
title=DUMMY
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=1
launcher_type=unrecognized
launcher_details={'raw_command': ':print mach nix'}
script_body:
:print mach nix
operational_notes=Wiederanlauf ohne weitere MaÃnahmen mÃ¶glich

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4/Automic to Apache Airflow Migration Design Document

---

## 1. Overview
The extracted bundle contains a single UC4 native Unix job object (`DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE`) designed as a "DUMMY" execution step. It contains a simple diagnostic or placeholder command (`:print mach nix`), which performs no actual operational system tasks. No workflow scheduling (JOBP) or script triggers (SCRI) were supplied within this extraction bundle. Consequently, this object is treated as an orphan step that would be externally triggered or integrated as a no-op step in a wider workflow context.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE` | JOBS_UNIX | Active (1) | DUMMY |

---

## 3. Scheduling
* **Schedule Analysis**: No `EVNT_TIME` or workflow-level schedule objects are present. There are also no companion `SCRI` triggering objects or parent `JOBP` workflows defined in this extraction.
* **Trigger Source**: Externally triggered; the exact source is unknown from this extraction alone.
* **Airflow Schedule**: `schedule=None` (no calendar trigger will be synthetically generated).

---

## 4. Airflow DAG Properties
Since no parent `JOBP` workflow was supplied, a wrapper DAG is defined specifically for this job to facilitate independent deployment or manual execution.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_vdgd_nvr_imvt_pre` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active flag = 1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_dummy_vdgd_nvr_imvt_pre` | `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE` | `EmptyOperator` | N/A | N/A | 1 | 5 mins | None | None | False | None | # REVIEW-STRUCT: launcher command [`:print mach nix`] not recognised — confirm target operator/script manually. Mapped to EmptyOperator as it represents a dummy task. |

---

## 6. Task Dependency Map
As this extraction contains only a single job with no orchestrating parent or defined predecessors, the dependency structure is a single standalone node.

```python
dwh_dummy_vdgd_nvr_imvt_pre
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource lock definitions were extracted for this object.

| UC4 Sync Else value | lock_kind | Airflow mapping |
| :--- | :--- | :--- |
| N/A | N/A | `max_active_runs=1` (sufficient default fallback) |

---

## 8. Error Handling and Retry Strategy
* **Retries**: Configured to use standard default retry parameters (1 retry, 5-minute interval) as no specific retry override configuration was present in the extraction.
* **Recovery**: Based on UC4 operational notes ("Wiederanlauf ohne weitere Maßnahmen möglich" / "Restart possible without further measures"), the task can be safely retried or rerun from scratch in Airflow without prior manual cleanup.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE` | UC4 Job Name | DAG ID: `dw_dwh_dummy_vdgd_nvr_imvt_pre` <br> Task ID: `dwh_dummy_vdgd_nvr_imvt_pre` |

---

## 10. Developer Notes
* **Unresolved References**:
  * None — every referenced object was supplied in this bundle (no references exist).
* **# REVIEW-STRUCT**: The launcher command `:print mach nix` is unrecognized by standard execution mappings. Based on the "DUMMY" title and the script text, it acts as a no-op processing step. This has been explicitly mapped to an `EmptyOperator`.
* **Execution Environment**: The UC4 login information suggests host `|DWHDWH1P|HOST` using credentials `DW.UNIX.ISTNS`. If this job is ever expanded to execute actual shell logic, a SSHOperator, CeleryQueue target, or KubernetesPodOperator target mapping to that execution environment must be configured.

---

# PSEUDOCODE OUTLINE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP resources or Dataproc jobs are utilized by this dummy task.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure callback tasks or notifications required.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_dummy_vdgd_nvr_imvt_pre',
    default_args=DEFAULT_ARGS,
    description='Migration DAG for UC4 dummy job DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'dummy'],
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # None required (no Skip sync conditions exist).

    # ── Sensor Task ──────────────────────────────────────
    # None required (no earliest start time constraints exist).

    # ── Calendar Check Task ──────────────────────────────
    # None required (no calendar constraints exist).

    # ── Task: dwh_dummy_vdgd_nvr_imvt_pre ────────────────
    # # REVIEW-STRUCT: UC4 script uses unrecognized command ':print mach nix'. 
    # Mapped to EmptyOperator due to its function as a dummy placeholder step.
    dwh_dummy_vdgd_nvr_imvt_pre = EmptyOperator(
        task_id='dwh_dummy_vdgd_nvr_imvt_pre',
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-node execution; no explicit task dependency links.
    dwh_dummy_vdgd_nvr_imvt_pre
```

# MIGRATION DESIGN DOCUMENT — ADDITIONAL CONTEXT

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_TVD_AKTIVITAETSRATE_MONATLICH_JP/DW.DWH_TVD_AK2_MONATLICH_JP/DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP/DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE.xml` | `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_TVD_AKTIVITAETSRATE_MONATLICH_JP/DW.DWH_TVD_AK2_MONATLICH_JP/DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP/DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE.py` | Migrate the UC4 dummy UNIX job to a single-task Airflow DAG executing an `EmptyOperator` to act as a placeholder synchronization point. |

---

## Additional Migration Context

### Job Dependencies
* **Downstream**: `DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP` — This downstream job is marked as **not yet migrated**. Consequently, the cross-DAG dependency wiring (e.g., via `TriggerDagRunOperator` or `ExternalTaskSensor`) cannot be finalized in the Airflow environment until it is migrated.

### Scheduling
* This job is not directly triggered by any scheduler. It is designed to execute as an included or shared module inside other workflows.
* **Airflow Target**: The DAG is configured with `schedule=None` (no independent trigger). It must be called programmatically or via upstream triggers/DAG runs.

### Schedule & Variables
* There are no scheduler-set variables or runtime variables defined for this job.

### Lineage
* **Runs on Host**: `EXT:dwhdwh1p` (execution host for the UNIX job).
* **Uses Login/Credentials**: `UNRESOLVED:DW.UNIX.ISTNS` (the UC4 login credentials used by the agent).

### Target File Plan
* **Target File Path**: `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_TVD_AKTIVITAETSRATE_MONATLICH_JP/DW.DWH_TVD_AK2_MONATLICH_JP/DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP/DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE.py`
* **Language**: Python (Airflow DAG)
* **Source File**: `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_TVD_AKTIVITAETSRATE_MONATLICH_JP/DW.DWH_TVD_AK2_MONATLICH_JP/DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP/DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE.xml`

### Environment-Specific Values
* **Host (`|DWHDWH1P|HOST`) & Login (`DW.UNIX.ISTNS`)**: Classified as **JOB-SPECIFIC**. Since the job maps to a no-operation task structure using `EmptyOperator`, no active execution connection is needed in the final Airflow deployment. If physical command logic is added in the future, these connections must be mapped to specific Airflow SSH or execution connections.

### Risks & Manual Actions
* **Downstream Dependency Gaps**: Downstream target `DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP` is not yet migrated; cross-DAG triggers cannot be fully completed.
* **Legacy Command Output**: The UC4 script contains `:print mach nix` which translates to a "do nothing" placeholder task. It is successfully mapped to an `EmptyOperator` task.
* SOURCE: NOT FOUND — DW.UNIX.ISTNS — no candidate