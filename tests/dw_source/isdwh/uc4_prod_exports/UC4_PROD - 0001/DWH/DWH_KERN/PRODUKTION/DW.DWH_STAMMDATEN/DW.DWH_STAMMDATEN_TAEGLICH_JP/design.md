=== OBJECT: DW.DWH_IPSD_DWH_MORPU_LID (JOBS_UNIX) ===
active=1
title=Import der Rechnungsleistungen fÃŒr MORPU Berechnung.
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=1
launcher_type=unrecognized
launcher_details={'raw_command': '$HOME/aktuell/import/is/bin/r_ipis -s dwh -k morpu_map_lid'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='IPSD_DWH_MORPU_LID'
. $HOME/.dw_init

$HOME/aktuell/import/is/bin/r_ipis -s dwh -k morpu_map_lid
:inc DW.LESE_LOG
operational_notes=Der fehlgeschlagene oder unterbrochene Prozess kann ohne weitere Arbeiten erneut ausgefÃŒhrt werden.

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 to Apache Airflow Migration Design Document

## 1. Overview
This migration document details the transition of the UC4 job `DW.DWH_IPSD_DWH_MORPU_LID` to an Apache Airflow DAG. The primary function of this standalone UNIX job is to import invoice services ("Rechnungsleistungen") to support MORPU calculation processes within the data warehouse. It executes a custom binary script (`r_ipis`) with specific command-line arguments targeting the `dwh` database environment and the `morpu_map_lid` context. Because no parent workflow (JOBP) or schedule (EVNT_TIME) was supplied in this extraction, the job is treated as an externally triggered, standalone workflow.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_IPSD_DWH_MORPU_LID` | JOBS_UNIX | 1 (Active) | Import der Rechnungsleistungen für MORPU Berechnung. |

## 3. Scheduling
* **Calendar Schedule**: No scheduling or calendar objects are defined in this extraction. 
* **Triggering Mechanism**: This object is triggered externally. In a production setting, this job is likely a task inside a larger, unsupplied JOBP workflow or triggered via an external scheduler/event.
* **Airflow Schedule Property**: `schedule=None`

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_ipsd_dwh_morpu_lid` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (Placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Derived from Active=1) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_ipsd_dwh_morpu_lid_task` | `DW.DWH_IPSD_DWH_MORPU_LID` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | **# REVIEW-STRUCT:** launcher command `[$HOME/aktuell/import/is/bin/r_ipis -s dwh -k morpu_map_lid]` not recognised — confirm target operator/script manually. |

## 6. Task Dependency Map
Since this extraction consists of a single standalone UNIX job with no surrounding JOBP orchestration:
```
dw_dwh_ipsd_dwh_morpu_lid_task (Single Node Workflow)
```

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource lock specifications were present in this extraction. To guarantee process integrity for this standalone task, `max_active_runs=1` is applied at the DAG level.

## 8. Error Handling and Retry Strategy
* **Retries**: Configured to 1 retry with a 5-minute delay as a standard baseline.
* **Operational Notes**: Per the UC4 operational metadata, a failed or interrupted run of this process can be safely restarted without requiring manual cleanup steps or state restoration.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'IPSD_DWH_MORPU_LID'` | Airflow task parameter/tag or Environment Variable |
| Sanitised DAG ID | `DW.DWH_IPSD_DWH_MORPU_LID` | `dw_dwh_ipsd_dwh_morpu_lid` |

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized UNIX Launcher**: The command execution `$HOME/aktuell/import/is/bin/r_ipis -s dwh -k morpu_map_lid` is treated as an unrecognized command launcher type. Currently represented as an `EmptyOperator` placeholder task. Action is required to determine if this binary should run inside a Docker container (using `KnativePodOperator`/`GKEStartPodOperator`) or via a secure shell execution (`SSHOperator`/`BashOperator`).
* **# REVIEW-STRUCT: Missing Parent Workflow**: This JOBS_UNIX was supplied without a parent JOBP container. Developers must verify where this task fits within the broader end-to-end data pipeline architecture.
* **Environment Sourcing**: The UC4 script sources `.dw_init` and includes `DW.HOLE_PFAD`. Ensure any environment-specific path resolutions are properly configured via Airflow Connection settings or Environment Variables rather than hardcoded login-home lookups.

---

# Airflow DAG Pseudocode

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# Placeholder for project-level connections if execution is migrated to GCP
# PROJECT_ID = "gcp-project-id"
# REGION = "us-central1"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom error execution routines identified in extraction.

# ── DAG Definition (dw_dwh_ipsd_dwh_morpu_lid) ──────────
with DAG(
    dag_id='dw_dwh_ipsd_dwh_morpu_lid',
    default_args=default_args,
    description='Import der Rechnungsleistungen fuer MORPU Berechnung.',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'dwh', 'morpu'],
) as dag:

    # ── Guard Task (N/A) ─────────────────────────────────

    # ── Sensor Task (N/A) ────────────────────────────────

    # ── Calendar Check Task (N/A) ────────────────────────

    # ── Task: dw_dwh_ipsd_dwh_morpu_lid_task ─────────────────
    # REVIEW-STRUCT: UC4 source command: $HOME/aktuell/import/is/bin/r_ipis -s dwh -k morpu_map_lid
    # Sourced include files: DW.HOLE_PFAD, DW.LESE_LOG, and .dw_init
    # Action required: Map the target binary execution context to an active container executor or SSH operator.
    dw_dwh_ipsd_dwh_morpu_lid_task = EmptyOperator(
        task_id='dw_dwh_ipsd_dwh_morpu_lid_task',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single task execution flow; no dependencies required.
    dw_dwh_ipsd_dwh_morpu_lid_task
```

### Job Dependencies
The following downstream jobs consume this job's output or are executed after it. Because they are not yet migrated, direct cross-DAG execution dependencies (e.g., via Airflow `TriggerDagRunOperator` or `ExternalTaskSensor`) cannot be finalized until these target DAGs are implemented.
* **DW.DWH_IPSD_DWH_MORPU_LID** (Downstream consumer) — Not yet migrated
* **DW.DWH_MORPU_MONATLICH_JP** (Downstream consumer) — Not yet migrated
* **DW.DWH_RUN_MORPU_MONATLICH_JP_EVT** (Downstream consumer) — Not yet migrated
* **DW.DWH_STAMMDATEN_TAEGLICH_JP** (Downstream consumer) — Not yet migrated
* **DW.DWH_START_RUN_MORPU_MONATLICH_JP_EVT** (Downstream consumer) — Not yet migrated
* **DW.DWH_TVD_AK2_MONATLICH_JP** (Downstream consumer) — Not yet migrated

### Scheduling
* **Triggering Mechanism**: This job is NOT directly triggered by any scheduler. It executes as an include or shared module inside scheduled jobs.
* **Target Scheduling**: The migrated Airflow DAG must remain a callable/importable unit with `schedule_interval=None`. It should be triggered dynamically by the upstream calling parent DAGs via the `TriggerDagRunOperator`.

### Schedule & Variables — Must Be Retained
* **Scheduler Linkage**: As an event/parent-triggered task, it does not inherit cron scheduling.
* **Script Variables**: The variable `&DWH_JOB_KENNUNG` set within the script must be made available to the environment at runtime as detailed in the Environment-specific values section.

### Lineage
* **Upstream Includes / Configurations**:
  * Include script: `DW.HOLE_PFAD` (loads path variables)
  * Sourced profile: `.DW_INIT` (initializes the UNIX environment)
  * Logging include: `DW.LESE_LOG` (handles log extraction/parsing)
* **Execution Script**:
  * Shell command: `$HOME/aktuell/import/is/bin/r_ipis` (executed with options `-s dwh -k morpu_map_lid`)
* **Infrastructure Host**:
  * Target host: `dwhdwh1p` (defines the execution environment where the binary script lives)
* **Login Package**:
  * Login user: `DW.UNIX.ISTNS` (defines execution permissions and environment profile)

### Target File Plan
Following the **Folder Integrity Rule**, the target folder structure mirrors the source file path, changing only the file type to Python for Composer execution:
* **Target File Path**: `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP/DW.DWH_IPSD_DWH_MORPU_LID.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source File**: `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP/DW.DWH_IPSD_DWH_MORPU_LID.xml`

### Environment-Specific Values
The environment values are classified by their target operational role below:

#### 1. GLOBAL (Environment-Wide Configuration)
* `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")` at runtime.
* `GCS_BUCKET`: Sourced via `Variable.get("GCS_BUCKET")` at runtime (if input/output files are transferred to GCS).
* `SSH_CONN_ID` (representing execution on `dwhdwh1p` with user `DW.UNIX.ISTNS` if using `SSHOperator`): Sourced via Airflow Connection definitions.
* `HOME`: Sourced from the target execution environment (`os.environ.get("HOME")`) or DAG-level environment parameters.

#### 2. JOB-SPECIFIC (Job-Level Configuration)
* `DWH_JOB_KENNUNG`: Value is `"IPSD_DWH_MORPU_LID"`. Configured as a Python DAG parameter/environment variable.
* `IMPORT_SCRIPT_PATH`: Value is `"$HOME/aktuell/import/is/bin/r_ipis"`. Configured as part of the execution task command string.
* `IMPORT_SCRIPT_ARGS`: Value is `"-s dwh -k morpu_map_lid"`. Configured as part of the execution task command string.

---

### Risks & Manual Actions
* **SOURCE: NOT FOUND** — `DW.HOLE_PFAD` — no candidate
* **SOURCE: NOT FOUND** — `DW.LESE_LOG` — no candidate
* **SOURCE: NOT FOUND** — `.DW_INIT` — no candidate
* **SOURCE: NOT FOUND** — `R_IPIS.KSH` — no candidate
* **SOURCE: NOT FOUND** — `DW.UNIX.ISTNS` — no candidate
* **Risk (Not Yet Migrated Downstreams)**: The downstreams listed below have not been migrated. Hand-offs and dependencies cannot be fully tested or wired in the orchestration layer until these DAGs are deployed:
  * `DW.DWH_IPSD_DWH_MORPU_LID`
  * `DW.DWH_MORPU_MONATLICH_JP`
  * `DW.DWH_RUN_MORPU_MONATLICH_JP_EVT`
  * `DW.DWH_STAMMDATEN_TAEGLICH_JP`
  * `DW.DWH_START_RUN_MORPU_MONATLICH_JP_EVT`
  * `DW.DWH_TVD_AK2_MONATLICH_JP`
* **Risk (Binary Execution)**: The target binary `r_ipis` must either be containerized (e.g., GKE Pod execution) or executed on an on-premises VM via an `SSHOperator` connection. Confirm architecture with infrastructure teams.

---

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP/DW.DWH_IPSD_DWH_MORPU_LID.xml` | `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP/DW.DWH_IPSD_DWH_MORPU_LID.py` | Migrated as a standalone, externally-triggerable Airflow DAG to preserve legacy folder structure and execution context. |