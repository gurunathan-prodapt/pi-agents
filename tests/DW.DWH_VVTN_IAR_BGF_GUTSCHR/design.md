=== OBJECT: DW.DWH_VVTN_IAR_BGF_GUTSCHR (JOBS_UNIX) ===
active=1
title=Transform Gutschrift files to one file CSV
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=1
launcher_type=unrecognized
launcher_details={'raw_command': ': set &Month_ID = &LASTMONTH_YYYYMM'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='VVTN_IAR_BGF_GUTSCHR'
: set &Month_ID = &LASTMONTH_YYYYMM
:print Lastmonth is &Month_ID
. $HOME/.dw_init

$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift

:inc DW.LESE_LOG
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 to Apache Airflow Migration Design Document

## 1. Overview
This design document covers the migration of a single UC4 Unix job (`DW.DWH_VVTN_IAR_BGF_GUTSCHR`) to Apache Airflow. The purpose of this workload is to transform credit ("Gutschrift") files into a unified CSV format by executing a local Unix script (`r_vvtn_iar_bgf_gutschrift`). The UC4 script sets environment configurations including the job identifier (`VVTN_IAR_BGF_GUTSCHR`) and the previous calendar month's identifier (`LASTMONTH_YYYYMM`). 

As this bundle contains only a standalone `JOBS_UNIX` task and no scheduling objects (`JSCH` or `EVNT_TIME`), the resulting Airflow DAG is configured as an externally triggered workflow with no native scheduled intervals.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_VVTN_IAR_BGF_GUTSCHR` | JOBS_UNIX | Active (1) | Transform Gutschrift files to one file CSV |

---

## 3. Scheduling
- **Schedule**: `None`
- **Trigger Source**: This workflow has no self-contained UC4 calendar schedule or `EVNT_TIME` object. It is externally triggered.
- **Airflow Configuration**: `schedule=None` (manual execution or external trigger only).

---

## 4. Airflow DAG Properties
Since this extraction contains only a single standalone `JOBS_UNIX` object, it is wrapped inside its own individual DAG for execution.

| Property | Value |
| :--- | :--- |
| **DAG ID** | `dw_dwh_vvtn_iar_bgf_gutschr` |
| **Schedule** | `None` |
| **Start Date** | `datetime(2023, 1, 1)` (Placeholder) |
| **Catchup** | `False` |
| **Max Active Runs** | `1` |
| **Is Paused Upon Creation** | `False` (Active=1) |
| **Default Args** | `{"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_vvtn_iar_bgf_gutschr_task` | `DW.DWH_VVTN_IAR_BGF_GUTSCHR` | `EmptyOperator` | N/A | N/A | 0 | N/A | None | None | False | None | # REVIEW-STRUCT: launcher command [`: set &Month_ID = &LASTMONTH_YYYYMM`] not recognised — confirm target operator/script manually. |

---

## 6. Task Dependency Map
Since this DAG contains only a single task, there are no dependencies to chart.

```
dw_dwh_vvtn_iar_bgf_gutschr_task
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource lock allocations were found in the extraction metadata for this object.
- **Concurrency Strategy**: `max_active_runs=1` is configured to prevent overlapping parallel runs of the same DAG.

---

## 8. Error Handling and Retry Strategy
- **Failure Notification**: Default task failure behavior applies.
- **Execution Guard**: No `earliest_start_time` constraints or calendar conditions are defined.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `VVTN_IAR_BGF_GUTSCHR` | Airflow Environment Variable or Task-level env parameter. |
| `&LASTMONTH_YYYYMM` | Dynamically calculated previous month | Computed dynamically using Airflow Jinja Macro: `{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}` |

---

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized Launcher & Host execution**: The launcher type was flagged as unrecognized due to UC4 script-based pre-processing assignments (`: set &Month_ID = &LASTMONTH_YYYYMM`). In production Airflow, the `EmptyOperator` stub `dw_dwh_vvtn_iar_bgf_gutschr_task` should be converted into an execution operator. 
  * If executing directly on a target machine, use the `SSHOperator` targeting `|DWHDWH1P|HOST` using the credentials defined in the `DW.UNIX.ISTNS` connection.
  * If migrated to a Kubernetes executor or native Cloud environment, encapsulate the underlying script `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` within a Docker container and execute via the `KubernetesPodOperator`.
* **Execution Environment**: Ensure that `$HOME/.dw_init` and the include utility `DW.HOLE_PFAD` (which maps paths) are replicated or accessible in the execution environment.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# NOTE: If executing via SSH, import SSHOperator
# from airflow.providers.ssh.operators.ssh import SSHOperator

# ── GCP Configuration ────────────────────────────────────
# N/A for this standalone migration (no GCS/BigQuery/Dataproc resources utilized)

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No callbacks or alert systems defined in the UC4 metadata.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_vvtn_iar_bgf_gutschr",
    default_args=DEFAULT_ARGS,
    description="Transform Gutschrift files to one file CSV",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["migrated_uc4", "gutschrift"],
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # N/A - No lock_kind=self/Else=Skip detected

    # ── Sensor Task ──────────────────────────────────────
    # N/A - No earliest_start_time constraint detected

    # ── Calendar Check Task ──────────────────────────────
    # N/A - No calendar constraints detected

    # ── Task: dw_dwh_vvtn_iar_bgf_gutschr_task ───────────
    # # REVIEW-STRUCT: Target script is currently mapped to an EmptyOperator due to unrecognized launcher type in UC4 extraction.
    # Replace with SSHOperator or BashOperator to execute:
    #   $HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
    #
    # To pass &LASTMONTH_YYYYMM dynamically, utilize this Airflow Jinja expression:
    #   LASTMONTH_YYYYMM = "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}"
    
    dw_dwh_vvtn_iar_bgf_gutschr_task = EmptyOperator(
        task_id="dw_dwh_vvtn_iar_bgf_gutschr_task",
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-task DAG. No dependencies to define.
    dw_dwh_vvtn_iar_bgf_gutschr_task
```

An implementation-ready Migration Design Document for **DW.DWH_VVTN_IAR_BGF_GUTSCHR** has been prepared. This document covers the orchestration wrapper migration from UC4 to Apache Airflow (Cloud Composer), mapping the legacy UNIX scheduler features to modern cloud equivalents.

---

### Execution Order
The execution order defined in the legacy metadata must be preserved within the target orchestration environment. 

1. **DAG Trigger / Scheduler**: Initialize the DAG execution context, setting the global and run-specific variables.
2. **Execute Processing Pipeline**: Execute the migrated logic of the child shell script `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` (which is migrated in a sibling design pass).
3. **Validate & Reformat Data**: Execute the migrated AWK steps `k_vvtn_iar_bgf_gutschrift.awk` and `k_vvtn_iar_bgf_gutsch_foot.awk` (which are also migrated in their own sibling design passes and called sequentially or embedded inside the child processing pipeline).

---

### Schedule & Variables
The legacy job's scheduling context and dynamic variables must be maintained in the migrated DAG as follows:

* **Trigger & Schedule**: Since the source extraction contains only a standalone job (`JOBS_UNIX`) without a dedicated parent schedule object, the migrated DAG is configured to be manually or externally triggered (`schedule_interval=None`).
* **Variables**:
  * `DWH_JOB_KENNUNG` (Job Identifier): Sourced at task runtime as a job-specific environment variable with the static value `'VVTN_IAR_BGF_GUTSCHR'`.
  * `Month_ID` (Target Month): Calculated dynamically at execution time using the Airflow Jinja Macro:
    ```python
    "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}"
    ```
    This expression accurately replicates the legacy scheduler's `&LASTMONTH_YYYYMM` variable behavior.

---

### Lineage
The logical lineage edges have been mapped to target Cloud Composer and GCP constructs:

* **Inclusions & Utilities (Upstream)**:
  * `.dw_init`, `DW.HOLE_PFAD`, and `DW.LESE_LOG` are utility configurations and logger inclusions confirmed as **NO SOURCE NEEDED**. Their path lookup and log scanning behaviors are natively replaced by Cloud Composer's environmental setup and integrated Cloud Logging (Stackdriver).
* **Invoked Executable (Downstream)**:
  * `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` is invoked directly by this wrapper. This is a cross-file relationship pointing to a sibling KSH/Python script migrated in a separate group. The DAG tasks will reference and trigger this translated processing code.
* **Target Environment Context**:
  * Host: `dwhdwh1p` (Target platform runs on BigQuery and Composer, removing the physical host dependency).
  * Login / Package: `DW.UNIX.ISTNS` (The target DAG runs within Cloud Composer using service account permissions, replacing legacy UNIX package roles).

---

### Cross-File Dependencies
* **Core processing module integration**: The orchestrating Airflow DAG has a direct execution dependency on the Python modules converted from `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`. The DAG must not be scheduled or executed in production until its invoked sibling modules are fully deployed.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml` | `DWH_IAR_BGF_GUTSCHRIFT_JOB/dw_dwh_vvtn_iar_bgf_gutschr.py` | Migrates the UC4 orchestration wrapper job into a native Airflow DAG that orchestrates the execution of the processing logic. |

---

### Target File Plan

* **Target File Path**: `DWH_IAR_BGF_GUTSCHRIFT_JOB/dw_dwh_vvtn_iar_bgf_gutschr.py`
  * **Language**: Python (Airflow DAG)
  * **Source File**: `DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml`

---

### Environment-Specific Values

The configuration values identified in the source files are classified and mapped according to the target GCP architecture:

#### 1. GLOBAL (Environment-wide Infrastructure)
* **GCP_PROJECT**: The identifier of the GCP project hosting Cloud Composer and BigQuery. Sourced at runtime via `os.environ.get("GCP_PROJECT")`.
* **GCS_BUCKET**: The Cloud Storage bucket acting as the landing zone/working directory for incoming data files (replacing `$HOME/aktuell`). Sourced via `os.environ.get("GCS_BUCKET")`.
* **BQ_DATASET**: The BigQuery dataset where the processed Gutschrift tables will reside. Sourced via `os.environ.get("BQ_DATASET")`.

#### 2. JOB-SPECIFIC (Workflow Variables)
* **DWH_JOB_KENNUNG**: Defined statically within the DAG context as a task-level environment variable:
  ```python
  "DWH_JOB_KENNUNG": "VVTN_IAR_BGF_GUTSCHR"
  ```
* **Month_ID**: Calculated dynamically at execution time within the DAG using Airflow templating (see [Schedule & Variables](#schedule--variables)).

---

### Risks & Manual Steps

* **Dependency on Sibling Migrations**: The target DAG orchestrates `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`. This executable is a sibling shell script belonging to a different group's design pass. The DAG workflow cannot be fully tested or finalized until that script is migrated to its target Python counterpart.
* **Legacy Init Assumptions**: The legacy job relies on `.dw_init` and `DW.HOLE_PFAD` for establishing context paths. A manual audit of the migrated processing script must confirm that all required environment variables are mapped to Airflow DAG configs or Cloud Composer environment variables, eliminating reliance on local VM home directories.
* **Dynamic Date Validation**: Airflow scheduler macros compute the prior month string (`LASTMONTH_YYYYMM`). During user acceptance testing (UAT), verify that manual backfills or specific DAG run dates correctly evaluate to the expected `YYYYMM` format used by downstream validation routines.

---

### group 2/4 — DESIGN FAILED

ERROR: NO_MCP_TOOL — design cannot proceed for 'DW.DWH_VVTN_IAR_BGF_GUTSCHR' — required tool 'awk_design_bqsql_python' is not currently available on the CM/SAT MCP servers. Contact the platform team to add support before retrying.


---

### group 3/4 — DESIGN FAILED

ERROR: NO_MCP_TOOL — design cannot proceed for 'DW.DWH_VVTN_IAR_BGF_GUTSCHR' — required tool 'awk_design_bqsql_python' is not currently available on the CM/SAT MCP servers. Contact the platform team to add support before retrying.


---

### group 4/4 — DESIGN FAILED

ERROR: NO_MCP_TOOL — design cannot proceed for 'DW.DWH_VVTN_IAR_BGF_GUTSCHR' — no MCP tool is confirmed for this job's source pattern ('UNKNOWN'). Contact the platform team to add or confirm support for this source type before retrying.
