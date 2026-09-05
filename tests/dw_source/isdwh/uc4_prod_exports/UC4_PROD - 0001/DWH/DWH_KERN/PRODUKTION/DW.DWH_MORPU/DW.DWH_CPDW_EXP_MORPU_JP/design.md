=== OBJECT: DW.DWH_EXIS_CPDW_LOC (JOBS_UNIX) ===
active=1
title=Exportiert Lookupdaten nach CPDW
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=2
launcher_type=unrecognized
launcher_details={'raw_command': '$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='EXIS_CPDW_LOC'
. $HOME/.dw_init
$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp
:inc DW.LESE_LOG
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 to Apache Airflow Migration Design Document

## 1. Overview
This design document covers the migration of the UC4 job **DW.DWH_EXIS_CPDW_LOC** to Apache Airflow. This is an isolated, active UNIX job (`JOBS_UNIX`) designed to export lookup data to the CPDW target system via SFTP using an internal command-line exporter utility (`r_exis`). Because no parent workflow (`JOBP`) or schedule (`JSCH`/`EVNT`) was supplied in this extraction bundle, this job is encapsulated in a standalone, externally triggered Airflow DAG. 

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_EXIS_CPDW_LOC` | JOBS_UNIX | 1 (Active) | Exportiert Lookupdaten nach CPDW (Exports lookup data to CPDW) |

---

## 3. Scheduling
* **Calendar/Time Schedule**: None. No schedule-defining objects (`EVNT_TIME`, `JSCH`) were present in this extraction bundle.
* **Trigger Mechanism**: Externally triggered. No parent workflow or script trigger was supplied in this bundle to orchestrate this job.
* **Airflow Schedule**: `schedule=None` (manual or external trigger only).

---

## 4. Airflow DAG Properties
The following properties are defined for the standalone DAG wrapping this job:

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_exis_cpdw_loc` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Derived from Active=1)* |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
Since the launcher type is classified as `unrecognized`, the task is mapped to an `EmptyOperator` wrapper, requiring the developer to replace it with the appropriate execution operator (e.g., `BashOperator` or `SSHOperator`) during build phase.

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_exis_cpdw_loc` | `DW.DWH_EXIS_CPDW_LOC` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | **# REVIEW-STRUCT:** launcher command `$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp` not recognised — confirm target operator/script manually. |

---

## 6. Task Dependency Map
Since this DAG contains only a single task, there is no dependency chain:

```
dwh_exis_cpdw_loc
```

---

## 7. Sync / Concurrency Analysis
* No sync keys, mutual exclusion locks, or concurrency restrictions were defined for this object in the UC4 extraction.

---

## 8. Error Handling and Retry Strategy
* **Retries**: Configured with a default of `1` retry with a 5-minute delay.
* **Postconditions**: No explicit UC4 postconditions or execution blocks were parsed. Standard Airflow task failure mechanics apply (task state is set to `failed` after exhausting retries).

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'EXIS_CPDW_LOC'` | Airflow Task environment variable (`env={'DWH_JOB_KENNUNG': 'EXIS_CPDW_LOC'}`) |
| Sanitised DAG ID | N/A | `dw_dwh_exis_cpdw_loc` |

---

## 10. Developer Notes
* **# REVIEW-STRUCT (Unrecognized Launcher)**: The raw command executed in UC4 is `$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp`. In Airflow, this should be migrated to a `BashOperator` (if running locally on a worker with filesystem access) or an `SSHOperator` (if executing on a remote edge node).
* **# REVIEW-STRUCT (Standalone Migration)**: This job was exported without its parent JOBP workflow. Confirm if this task should be integrated into a larger parent DAG or if it remains triggered independently.
* **# REVIEW (UC4 Inclusions)**: The UC4 script contains inclusions (`:inc DW.HOLE_PFAD` and `:inc DW.LESE_LOG`) and environment initializations (`. $HOME/.dw_init`). These must be manually resolved. Ensure any system paths or log-reading operations they performed are successfully handled by the Airflow host environment or task execution logic.

---

# PSEUDOCODE OUTLINE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# NOTE: Developer should import BashOperator or SSHOperator to replace EmptyOperator 
# once execution environment is finalized.

# ── GCP Configuration ────────────────────────────────────
# No GCP resources utilized directly by this local unix utility.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure handlers required.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_exis_cpdw_loc",
    default_args=DEFAULT_ARGS,
    schedule=None,  # No schedule provided in extraction
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["uc4_migration", "standalone_jobs"],
) as dag:

    # ── Guard Task ───────────────────────────────────────────
    # None required (no Else=Skip self-lock sync rows defined).

    # ── Sensor Task ──────────────────────────────────────────
    # None required (no earliest_start_time constraint defined).

    # ── Calendar Check Task ──────────────────────────────────
    # None required (no calendar constraints defined).

    # ── Task: dwh_exis_cpdw_loc ──────────────────────────────
    # # REVIEW-STRUCT: Launcher command unrecognized. Placeholder EmptyOperator used.
    # To implement actual logic, replace with:
    # BashOperator(
    #     task_id="dwh_exis_cpdw_loc",
    #     bash_command="source $HOME/.dw_init && $HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp",
    #     env={"DWH_JOB_KENNUNG": "EXIS_CPDW_LOC"}
    # )
    dwh_exis_cpdw_loc = EmptyOperator(
        task_id="dwh_exis_cpdw_loc",
    )

    # ── Dependencies ─────────────────────────────────────────
    # Standalone task — no dependencies to declare.
    dwh_exis_cpdw_loc
```

### Job Dependencies
* **Downstream Job**: `DW.DWH_CPDW_EXP_MORPU_JP` (not yet migrated)
  * **Target Platform Wiring**: Since the downstream consumer `DW.DWH_CPDW_EXP_MORPU_JP` has not yet been migrated, the final cross-DAG scheduling or sensor wiring cannot be verified or established in Composer. Once that downstream job is ready for migration, an `ExternalTaskSensor` (or direct triggering mechanism) should be configured in its DAG to detect the successful completion of the `dw_dwh_exis_cpdw_loc` DAG.

### Scheduling
* **Target Scheduling**: This job is not directly triggered by any scheduling object in the source system. It executes inside scheduled jobs (e.g., as an include/shared module). In Apache Airflow, this DAG is configured with `schedule=None` so that it remains a callable, externally triggered, or importable orchestrator unit rather than running on its own independent timeline.

### Schedule & Variables
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG = 'EXIS_CPDW_LOC'`
* **Target Platform Routing**: This variable must reach the execution step dynamically. It will be passed as a task environment variable (`env={'DWH_JOB_KENNUNG': 'EXIS_CPDW_LOC'}`) to the executing SSH or Bash operator in Airflow, ensuring the runtime profile can identify the execution context verbatim.

### Lineage
* **Target Execution Host**: The script runs on host `EXT:dwhdwh1p` using credentials associated with login object `DW.UNIX.ISTNS`.
* **Inclusions & Profile Invocations**:
  * Includes `UNRESOLVED:DW.HOLE_PFAD` (Confirmed by human review: NO SOURCE NEEDED)
  * Includes `UNRESOLVED:DW.LESE_LOG` (Confirmed by human review: NO SOURCE NEEDED)
  * Invokes `UNRESOLVED:.DW_INIT` (Confirmed by human review: NO SOURCE NEEDED)
  * Invokes `UNRESOLVED:R_EXIS.KSH` (Confirmed by human review: NO SOURCE NEEDED)

### Cross-File Dependencies
* **Execution Call Chain**: The core logic executing on the target host is:
  ```bash
  $HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp
  ```
  This command invokes a local shell tool on `dwhdwh1p` that performs the SFTP export of lookup data to the `cpdw` target system. The workflow relies on this script existing on the target machine and having established passwordless SFTP access configured between the target machine and `cpdw`.

### Target File Plan
* **Target File**: `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/dw_dwh_exis_cpdw_loc.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source File**: `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/DW.DWH_EXIS_CPDW_LOC.xml`

### Environment-Specific Values
* **Host Identifier (`EXT:dwhdwh1p`)**
  * **Classification**: GLOBAL
  * **Target Implementation**: Managed through an Airflow SSH Connection ID (e.g., `ssh_dwhdwh1p_default`). The connection string is retrieved at runtime from the Airflow connection store, preventing hardcoded references to physical machines.
* **Login Credentials (`DW.UNIX.ISTNS`)**
  * **Classification**: GLOBAL
  * **Target Implementation**: Associated directly with the credential fields of the SSH Connection ID (such as SSH private keys or user credentials) configured in the secure Cloud Composer metadata store.
* **Export Path (`$HOME/aktuell/exporter/is/bin/r_exis`)**
  * **Classification**: JOB-SPECIFIC
  * **Target Implementation**: Kept as an inline executable script path in the `SSHOperator` command string.
* **Job Key (`DWH_JOB_KENNUNG`)**
  * **Classification**: JOB-SPECIFIC
  * **Target Implementation**: Provided inside the `env` parameter of the Airflow task execution operator as a key-value configuration.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/DW.DWH_EXIS_CPDW_LOC.xml` | `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/dw_dwh_exis_cpdw_loc.py` | Converts the UC4 Unix Job definition into an equivalent standalone, externally-triggerable Airflow DAG. |

### Risks & Manual Actions
* **Unmigrated Downstream Link**: The downstream target `DW.DWH_CPDW_EXP_MORPU_JP` is not yet migrated. Cross-job orchestration cannot be validated in Cloud Composer until the consuming DAG exists.
* **External Host Command Execution**: The execution relies on local file systems and utilities (`r_exis` script, `.dw_init` profile) hosted on the external Unix system `dwhdwh1p`. Since these objects have a resolution of "NO SOURCE NEEDED" and are not part of the source files in this deployment, they must be manually validated on the destination SSH host to ensure they are available, runnable, and have key-based SFTP authorization configured to target system `cpdw`.
* **Credential Setup**: The credentials mapped to the `DW.UNIX.ISTNS` login object must be manually created inside the Airflow Connection `ssh_dwhdwh1p_default` prior to the DAG being deployed to production.