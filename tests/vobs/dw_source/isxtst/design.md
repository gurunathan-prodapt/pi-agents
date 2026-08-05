=== OBJECT: DW.EXTTEST_LEGACY_DWH (JOBS_UNIX) ===
active=1
title=legacy_ksh_dwh
login=DW.UNIX.ISXTST
host=|DWHDWH2P|HOST
ert_seconds=0
launcher_type=unrecognized
launcher_details={'raw_command': '&HOME/scripts/r_legacy_ksh_dwh'}
script_body:
:inc DW.EXTTEST_HOLE_PFAD
:set &DWH_JOB_KENNUNG='EXTTEST_LEGACY_DWH'
&HOME/scripts/r_legacy_ksh_dwh
:inc DW.EXTTEST_LESE_LOG
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: UC4 to Apache Airflow Migration

## 1. Overview
This migration document details the transition of a single UC4 UNIX Job object, `DW.EXTTEST_LEGACY_DWH`, into an Apache Airflow environment. Based on the extraction, this object runs a legacy shell script (`r_legacy_ksh_dwh`) and includes environment-setup scripts. No parent workflow (`JOBP`), schedule (`JSCH`), or triggering script (`SCRI`) was provided in this extraction bundle. Consequently, this migrated task is modeled as a standalone Airflow DAG that is externally triggered, waiting to be integrated into a broader orchestration pipeline if necessary.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.EXTTEST_LEGACY_DWH` | `JOBS_UNIX` | `1` | `legacy_ksh_dwh` |

---

## 3. Scheduling
- **Schedule Policy**: No `EVNT_TIME` or scheduling definitions are present in this extraction bundle.
- **Trigger Source**: This object has no calling `SCRI` or `JOBP` parent workflow within the bundle. It is marked as **externally triggered, source unknown from this extraction alone**.
- **Airflow Schedule**: `schedule=None` (no cron schedule will be invented).

---

## 4. Airflow DAG Properties
Because this `JOBS_UNIX` object is standalone in this extraction, it is wrapped in its own single-task Airflow DAG to allow independent execution.

| Property | Value |
|---|---|
| `dag_id` | `dw_exttest_legacy_dwh` |
| `schedule` | `None` |
| `start_date` | `datetime(2023, 1, 1)` *(placeholder)* |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `is_paused_upon_creation` | `False` *(Derived from Active=1)* |
| `default_args` | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `dw_exttest_legacy_dwh_task` | `DW.EXTTEST_LEGACY_DWH` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | # REVIEW-STRUCT: launcher command `&HOME/scripts/r_legacy_ksh_dwh` not recognised — confirm target operator/script manually |

---

## 6. Task Dependency Map
Since this DAG contains only a single migrated task representing the standalone UC4 job, there are no task dependencies:

```
dw_exttest_legacy_dwh_task
```

---

## 7. Sync / Concurrency Analysis
No sync rows or exclusion parameters were defined for this object.

| UC4 Sync Else value | lock_kind | Airflow mapping |
|---|---|---|
| N/A | N/A | `max_active_runs=1` is applied to the DAG as a standard safe-concurrency setting. |

---

## 8. Error Handling and Retry Strategy
- **Retries**: Configured to retry once (`retries: 1`) with a 5-minute delay (`retry_delay: timedelta(minutes=5)`), mapping standard UC4 operational robustness.
- **Postconditions**: No specific UC4 postconditions or error-handling blocks were defined in this object. Standard Airflow execution failure notifications apply.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&DWH_JOB_KENNUNG` | `'EXTTEST_LEGACY_DWH'` | To be set as an environment variable or Airflow template variable if migrated to a `BashOperator`. |
| Sanitised DAG ID | `DW.EXTTEST_LEGACY_DWH` | `dw_exttest_legacy_dwh` |

---

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized Launcher Command**: The UC4 job script launches `&HOME/scripts/r_legacy_ksh_dwh` directly. Because this is an unrecognized custom shell launcher pattern, it has been mapped to an `EmptyOperator` stub. Developers must replace this stub with a `BashOperator`, `SSHOperator`, or standard container execution operator (e.g., `GKEStartPodOperator`), depending on where the target shell script resides in the target environment.
* **# REVIEW-STRUCT: UC4 Includes (`:inc`)**: The UC4 script references `:inc DW.EXTTEST_HOLE_PFAD` and `:inc DW.EXTTEST_LESE_LOG`. These include blocks typically handle environment path resolution and log analysis. Developers must inspect these legacy UC4 includes and reconstruct any required environmental variables or log-handling behaviors in the Airflow environment.
* **Unresolved References**: None. No unresolved references were declared in this extraction bundle.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP configurations are mapped as the script launcher is unrecognized.

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom error callbacks specified.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_exttest_legacy_dwh',
    default_args=default_args,
    description='Legacy KSH DWH execution migrated from DW.EXTTEST_LEGACY_DWH',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # None required (No self-lock Else=Skip sync detected)

    # ── Sensor Task ──────────────────────────────────────
    # None required (No earliest_start_time constraint)

    # ── Calendar Check Task ──────────────────────────────
    # None required (No calendar constraints detected)

    # ── Task: dw_exttest_legacy_dwh_task ─────────────────
    # # REVIEW-STRUCT: launcher command &HOME/scripts/r_legacy_ksh_dwh not recognised.
    # Convert this EmptyOperator to a BashOperator, SSHOperator, or GKEStartPodOperator 
    # once the target runtime environment for the shell script is resolved.
    # Legacy UC4 Includes to manually migrate:
    #   - :inc DW.EXTTEST_HOLE_PFAD
    #   - :inc DW.EXTTEST_LESE_LOG
    # Environment variable to export: DWH_JOB_KENNUNG='EXTTEST_LEGACY_DWH'
    dw_exttest_legacy_dwh_task = EmptyOperator(
        task_id='dw_exttest_legacy_dwh_task',
    )

    # ── Dependencies ─────────────────────────────────────
    # Standalone task - no dependency chain required.
    dw_exttest_legacy_dwh_task
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.xml` | `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/dw_exttest_legacy_dwh.py` | Migrates the UC4 UNIX Job XML definition to an Airflow DAG that orchestrates the execution of the DWH legacy script. |

***

### Job dependencies
* **Downstream**: `DW.EXTTEST_ABLAUFSTEUERUNG` is a downstream consumer of this job's output. Since it is marked as "not yet migrated" in the source context, this connection cannot be finalized immediately. Once migrated, this dependency must be wired on Cloud Composer using an Airflow DAG-triggering sensor or cross-DAG dependency pattern.

### Execution order
The target Airflow orchestration must preserve the sequential execution of the following components:
1. **Step 1 (This File)**: `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.xml` (Migrated as the DAG `dw_exttest_legacy_dwh.py`).
2. **Step 2**: `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` (Migrated and executed by the task inside the Step 1 DAG).

### Schedule & variables
* **Schedule Policy**: This job is NOT directly triggered by any scheduler. It is designed to execute as an include/shared module within scheduled workflows. In the target Airflow environment, it must remain a callable unit (configured with `schedule=None`).
* **Scheduler-Set Variables**:
  - `DWH_JOB_KENNUNG` = `'EXTTEST_LEGACY_DWH'` — Must be passed to the task executor environment.

### Lineage
* **Upstream Producers (Includes)**:
  - `DW.EXTTEST_HOLE_PFAD` — Confirmed by human review to be **not needed** (retired).
  - `DW.EXTTEST_LESE_LOG` — Confirmed by human review to be **not needed** (retired).
* **Downstream Consumers**:
  - `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` (invoked shell script).
* **Execution Infrastructure**:
  - Host: `dwhdwh2p` (external host).
  - Package/Owner: `DW.UNIX.ISXTST`.

### External system replacements
* **Execution Host (`dwhdwh2p`)**: The legacy UNIX execution environment is replaced by a native containerized environment (e.g., GKE Kubernetes Pod, or a VM via SSH connection if remaining on-premises/hybrid) orchestrated by Cloud Composer.
* **Credentials/Login (`DW.UNIX.ISXTST`)**: Replaced by Google Cloud IAM roles, service accounts, or Airflow SSH Connection IDs.

### Cross-file dependencies
* **Invocation Chain**: This orchestration job directly executes `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` (which wraps database extract processes).
* **Includes**: Legacy scripts `DW.EXTTEST_HOLE_PFAD` and `DW.EXTTEST_LESE_LOG` are included inline in the source but are marked as retired per human-confirmed resolutions.

### Target file plan
* **`vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/dw_exttest_legacy_dwh.py`**
  - **Language**: Python (Apache Airflow)
  - **Source File**: `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.xml`
  - *Note: Implementation and pseudocode details are omitted here; please refer to the automatically attached MCP conversion output for the authoritative DAG script structure.*

### Environment-specific values
The following parameters and variables must be resolved dynamically in the target environment:

1. **`GCP_PROJECT`** (GLOBAL): Identifies the target Google Cloud Project.
   - *Source*: Airflow Variable (`Variable.get("GCP_PROJECT")`) or Environment Variable (`os.environ.get("GCP_PROJECT")`).
2. **`GCP_REGION`** (GLOBAL): Identifies the deployment region for the Cloud Composer/GKE infrastructure.
   - *Source*: Airflow Variable (`Variable.get("GCP_REGION")`) or Environment Variable (`os.environ.get("GCP_REGION")`).
3. **`HOME`** (GLOBAL): Maps to the base directory of the migrated scripts on the execution container/server.
   - *Source*: Environment Variable (`os.environ.get("HOME")`) or Airflow Variable (`Variable.get("HOME_DIR")`).
4. **`DWH_JOB_KENNUNG`** (JOB-SPECIFIC): The tracking identifier specific to this job's context.
   - *Value*: `'EXTTEST_LEGACY_DWH'`
   - *Source*: Declared inline in the DAG task configuration as an environment parameter (`env={'DWH_JOB_KENNUNG': 'EXTTEST_LEGACY_DWH'}`).
5. **`DW.UNIX.ISXTST`** (JOB-SPECIFIC): The UNIX execution login.
   - *Source*: Mapped to a specific Airflow Connection ID or GCP Service Account designated for this job group.

### Risks and manual steps
* **Pending Sibling Migration**: The script `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` is a sibling component belonging to a different migration pass. The stub (`dw_exttest_legacy_dwh_task`) inside the target DAG must be manually updated to point to the correct execution operator (e.g., `BashOperator`, `SSHOperator`, or `GKEStartPodOperator`) executing that migrated script.
* **Unmigrated Downstream Dependency**: `DW.EXTTEST_ABLAUFSTEUERUNG` is currently marked "not yet migrated". Cross-DAG linkages cannot be fully established until that downstream job is migrated.
* **Retired Include Verifications**: The include blocks `DW.EXTTEST_HOLE_PFAD` and `DW.EXTTEST_LESE_LOG` have been flagged as "no source needed / not needed" by human review. Developers should ensure any implicit system pathing or logging assertions performed by these blocks are natively satisfied by the Cloud Composer execution environment.

---

### group 2/2 — DESIGN FAILED

ERROR: NO_MCP_TOOL — design cannot proceed for 'DW.EXTTEST_LEGACY_DWH' — no MCP tool is confirmed for this job's source pattern ('UNKNOWN'). Contact the platform team to add or confirm support for this source type before retrying.
