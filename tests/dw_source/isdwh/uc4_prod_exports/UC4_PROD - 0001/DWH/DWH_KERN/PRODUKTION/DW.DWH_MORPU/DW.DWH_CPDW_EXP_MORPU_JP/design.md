=== OBJECT: DW.DWH_EXIS_CPDW_DIRECT (JOBS_UNIX) ===
active=1
title=Exportiert Lookupdaten nach CPDW
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=1
launcher_type=unrecognized
launcher_details={'raw_command': '$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='EXIS_CPDW_DIRECT'
. $HOME/.dw_init
$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp
:inc DW.LESE_LOG
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# DESIGN DOCUMENT: DW.DWH_EXIS_CPDW_DIRECT Migration

## 1. Overview
The `DW.DWH_EXIS_CPDW_DIRECT` UC4 object represents a standalone Unix job designed to export lookup data to the CPDW system using an SFTP direct transfer protocol. The job initializes its environment by executing setup scripts and sourcing variables, then executes a specialized binary launcher `r_exis` with configuration parameters. As this extraction bundle does not contain an orchestrating JOBP (Workflow) or scheduling definition, it is classified as an externally triggered task.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_EXIS_CPDW_DIRECT` | JOBS_UNIX | 1 | Exportiert Lookupdaten nach CPDW |

## 3. Scheduling
- **Schedule Policy**: No calendar-based schedule (such as an `EVNT_TIME` or `JSCH` object) is present in this bundle.
- **Trigger Source**: This workflow is externally triggered; no parent `JOBP` or triggering `SCRI` object was supplied in this extraction.
- **Airflow Schedule**: `schedule=None`

## 4. Airflow DAG Properties
Since no parent JOBP was provided, the standalone JOBS_UNIX object is wrapped into its own single-task DAG to ensure runnability.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_exis_cpdw_direct` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation**| `False` |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_exis_cpdw_direct` | `DW.DWH_EXIS_CPDW_DIRECT` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | **# REVIEW-STRUCT:** launcher command `[$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp]` not recognised — confirm target operator/script manually. |

## 6. Task Dependency Map
```python
dwh_exis_cpdw_direct
```
*(Single standalone task; no dependency chain is defined.)*

## 7. Sync / Concurrency Analysis
No `sync_rows` or mutual exclusion locks were defined in this extraction. Concurrency is limited at the DAG level using `max_active_runs=1`.

| UC4 Sync Else value | lock_kind | Airflow mapping |
| :--- | :--- | :--- |
| N/A | N/A | No concurrency constraints defined in source. Standard `max_active_runs=1` applied. |

## 8. Error Handling and Retry Strategy
- Default failure behavior relies on Airflow's standard scheduler retry mechanism.
- No postconditions or explicit `BLOCK` / `EXECUTE` commands were specified in the source definition.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'EXIS_CPDW_DIRECT'` | Airflow Task Parameter or Environment Variable |
| `dw_dwh_exis_cpdw_direct` | Sanitised Object Name | `dag_id` |

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized Launcher**: The original execution script leverages a custom binary/shell script setup: `$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp`. This cannot be automatically mapped to a specific cloud operator. The developer must determine whether this should be executed via `SSHOperator` on a target VM, migrated to a `BashOperator` in a containerized environment, or refactored into a native Python cloud transfer task.
* **Missing Orchestration**: Since this object is a standalone `JOBS_UNIX` task and no `JOBP` workflow was supplied, it has been wrapped into its own DAG. Confirm whether this task should actually be imported as a sub-task or task group inside a larger workflow DAG.
* **Includes/Includes Logic**: The source script references `:inc DW.HOLE_PFAD` and `:inc DW.LESE_LOG`. These UC4 includes are missing from this bundle. Any path resolution or post-execution log parsing performed by these includes must be manually implemented in the target Airflow environment or task execution logic.

---

# PSEUDOCODE OUTLINE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No explicit GCP resources required for placeholder EmptyOperator.
# If migrated to an SSH or Bash execution model, configure connections here.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No explicit error-handling callbacks defined in the source UC4 object.

# ── DAG Definition ────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_exis_cpdw_direct",
    default_args=DEFAULT_ARGS,
    description="Exportiert Lookupdaten nach CPDW (Migrated from JOBS_UNIX)",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["uc4_migration", "unrecognized_launcher"],
) as dag:

    # ── Task: dwh_exis_cpdw_direct ────────────────────────
    # # REVIEW-STRUCT:
    # Original command was:
    #   :inc DW.HOLE_PFAD
    #   :set &DWH_JOB_KENNUNG='EXIS_CPDW_DIRECT'
    #   . $HOME/.dw_init
    #   $HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp
    #   :inc DW.LESE_LOG
    #
    # Action Required: Replace this EmptyOperator placeholder with an SSHOperator, 
    # BashOperator, or custom PythonOperator once the target runtime environment is resolved.
    dwh_exis_cpdw_direct_task = EmptyOperator(
        task_id="dwh_exis_cpdw_direct",
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single task execution; no dependency mapping required.
    dwh_exis_cpdw_direct_task
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/DW.DWH_EXIS_CPDW_DIRECT.xml` | `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/DW.DWH_EXIS_CPDW_DIRECT.py` | Translate the UC4 JOBS_UNIX object into an Airflow DAG file. |

# Job dependencies
* **Downstream Jobs (not yet migrated):**
  * `DW.DWH_CPDW_EXP_MORPU_JP`
  * `DW.DWH_MORPU_MONATLICH_JP`
  * `DW.DWH_RUN_MORPU_MONATLICH_JP_EVT`
  * `DW.DWH_TVD_AK2_MONATLICH_JP`
  
  These downstream jobs consume this job's output. In the GCP environment, cross-DAG dependencies must be established (e.g., using Airflow `TriggerDagRunOperator` or cross-DAG `ExternalTaskSensor` objects) once they are migrated.

# Scheduling
* **Trigger Policy:** This job is not directly triggered by any scheduler. It runs inside other scheduled parent jobs or workflows.
* **Target Scheduling Construct:** The target Airflow DAG will remain unscheduled (`schedule=None`) to act as a callable or importable workflow.

# Schedule & variables
* **Scheduler-set Variables:**
  * `DWH_JOB_KENNUNG = 'EXIS_CPDW_DIRECT'`
  
  This variable must be passed to the migrated Airflow task using the target's native mechanism (such as DAG `params` or the task's environment dictionary `env`).

# Lineage
* **Upstream/Included References:**
  * `DW.HOLE_PFAD` (UC4 script include)
  * `DW.LESE_LOG` (UC4 script include)
  * `.DW_INIT` (Sourced shell script)
  * `R_EXIS.KSH` (Invoked shell script)
* **Execution Host:** `dwhdwh1p` (Legacy host where the command executes)
* **Login/Credentials Profile:** `DW.UNIX.ISTNS` (UC4 login object mapping to target execution context credentials)

# Target file plan
* **Target File Path:** `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/DW.DWH_EXIS_CPDW_DIRECT.py`
* **Language:** Python (Airflow DAG)
* **Source File:** `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/DW.DWH_EXIS_CPDW_DIRECT.xml`

# Environment-specific values
* **GLOBAL:**
  * `DWHDWH1P` (Legacy Execution Host) -> Map to a global environment execution variable or connections (e.g., `GCP_CONN_ISTNS` or SSH connection profiles in Airflow).
* **JOB-SPECIFIC:**
  * `DWH_JOB_KENNUNG` -> Provided inline or via a task configuration dictionary: `JOB_CONFIG = {"DWH_JOB_KENNUNG": "EXIS_CPDW_DIRECT"}`.
  * `$HOME` -> Resolved dynamically in the execution environment.

# Risks & Manual Steps
* **Custom Binary/Script Execution:** No direct target-platform equivalent exists on GCP for the custom legacy command:
  `$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp`
  The logic must be manually resolved and implemented in the execution runtime.
* **Missing Includes and Sourced Scripts:**
  * `DW.HOLE_PFAD` (Source not found; marked as "NO SOURCE NEEDED" by human-confirmed resolution)
  * `DW.LESE_LOG` (Source not found; marked as "NO SOURCE NEEDED" by human-confirmed resolution)
  * `.DW_INIT` (Source not found; marked as "NO SOURCE NEEDED" by human-confirmed resolution)
  * `R_EXIS.KSH` (Source not found; marked as "NO SOURCE NEEDED" by human-confirmed resolution)
  
  Any environment path setup or post-execution log-reading logic must be verified and manually handled on GCP.
* **Downstream Alignment:** Since downstream jobs (`DW.DWH_CPDW_EXP_MORPU_JP`, `DW.DWH_MORPU_MONATLICH_JP`, `DW.DWH_RUN_MORPU_MONATLICH_JP_EVT`, `DW.DWH_TVD_AK2_MONATLICH_JP`) are not yet migrated, their cross-job dependencies cannot be finalized.