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


# Design Document: UC4 to Apache Airflow Migration

## 1. Overview
This migration design covers a single UC4 UNIX job: `DW.DWH_VVTN_IAR_BGF_GUTSCHR`. The job is responsible for transforming raw "Gutschrift" (credit note) files into a consolidated CSV file format. It initializes environment variables (including sourcing the `.dw_init` profile and determining a monthly date identifier) and subsequently runs a shell script: `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`. This job is supplied without an enclosing parent Job Plan (`JOBP`) or calendar trigger, indicating that it is either triggered externally by another agent, scheduled directly via an unsupplied UC4 Schedule object (`JSCH`), or executed as part of an upstream workflow not captured in this extraction.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_VVTN_IAR_BGF_GUTSCHR` | JOBS_UNIX | 1 | Transform Gutschrift files to one file CSV |

---

## 3. Scheduling
* **Calendar Schedule:** No schedule-defining objects (`EVNT_TIME` or `JSCH`) are present in this extraction bundle.
* **Trigger Mechanism:** Externally triggered (source unknown from this extraction alone).
* **Airflow Trigger Rule:** `schedule=None` (no schedule will be defined; the DAG must be triggered externally via API, CLI, or an upstream `TriggerDagRunOperator`).

---

## 4. Airflow DAG Properties
Since no parent `JOBP` workflow was supplied, a dedicated wrapper DAG is established to represent and execute this job.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_vvtn_iar_bgf_gutschr` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Derived from Active=1)* |
| **default_args** | `{'owner': 'dw', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_vvtn_iar_bgf_gutschr` | `DW.DWH_VVTN_IAR_BGF_GUTSCHR` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | #REVIEW-STRUCT: Launcher command `: set &Month_ID = &LASTMONTH_YYYYMM` not recognised — confirm target operator/script manually. See Section 10 for details on Bash migration. |

---

## 6. Task Dependency Map
As this migration contains only a single standalone job, there is no multi-task execution chain.

```
dwh_vvtn_iar_bgf_gutschr
```

---

## 7. Sync / Concurrency Analysis
No sync/lock resources or concurrency rules were defined for this object in the extraction.

---

## 8. Error Handling and Retry Strategy
* **Retries:** Defaulted to 1 retry with a 5-minute delay.
* **Failure Actions:** No postcondition failure actions or custom error callbacks were defined in the UC4 metadata.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'VVTN_IAR_BGF_GUTSCHR'` | Task environment variable: `DWH_JOB_KENNUNG` |
| `&LASTMONTH_YYYYMM` | Dynamic date expression | Airflow Jinja expression: `{{ (data_interval_end.in_timezone('Europe/Berlin') - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}` |

---

## 10. Developer Notes
* **#REVIEW-STRUCT: Unrecognized Launcher Migration:** The UC4 object has been mapped to an `EmptyOperator` to comply with automated transition safety rules. However, the script body executes a native Unix shell script: `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`. 
  * *Recommendation:* In the production DAG, replace the `EmptyOperator` with either a `BashOperator` running on an Airflow Worker with access to the file system, or an `SSHOperator` targeting the appropriate host `|DWHDWH1P|HOST`.
* **Environment Sourcing:** The script sources `$HOME/.dw_init`. This profile setup must be ensured in the Airflow execution environment if migrating to a `BashOperator`.
* **Date Logic:** The script parses `&LASTMONTH_YYYYMM`. This should be injected at runtime using Airflow's logical execution date context.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# NOTE: Recommend importing BashOperator or SSHOperator for final execution:
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.operators.bash import BashOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP-specific targets or Buckets are requested in the source script

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'dw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom error handlers required by the extraction

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_vvtn_iar_bgf_gutschr',
    default_args=DEFAULT_ARGS,
    description='Transform Gutschrift files to one file CSV',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # None required (No Else=Skip self-lock rules)

    # ── Sensor Task ──────────────────────────────────────
    # None required (No earliest start time)

    # ── Calendar Check Task ──────────────────────────────
    # None required

    # ── Task: dwh_vvtn_iar_bgf_gutschr ───────────────────
    # #REVIEW-STRUCT: Launcher command not recognised.
    # The default mapping yields an EmptyOperator.
    # Developer should replace this with a Bash/SSH Operator in production.
    
    dwh_vvtn_iar_bgf_gutschr = EmptyOperator(
        task_id='dwh_vvtn_iar_bgf_gutschr',
    )

    # Production alternative implementation template:
    # dwh_vvtn_iar_bgf_gutschr = BashOperator(
    #     task_id='dwh_vvtn_iar_bgf_gutschr',
    #     bash_command="""
    #         . $HOME/.dw_init
    #         export DWH_JOB_KENNUNG='VVTN_IAR_BGF_GUTSCHR'
    #         export Month_ID="{{ (data_interval_end.in_timezone('Europe/Berlin') - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}"
    #         echo "Lastmonth is $Month_ID"
    #         $HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
    #     """,
    # )

    # ── Dependencies ─────────────────────────────────────────
    # Standalone task. No dependency chain required.
    dwh_vvtn_iar_bgf_gutschr
```

# MIGRATION DESIGN DOCUMENT: DW.DWH_VVTN_IAR_BGF_GUTSCHR

## File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml` | `dags/dw_dwh_vvtn_iar_bgf_gutschr.py` | Migrates the UC4 orchestration job into an Airflow DAG executing on Cloud Composer. |

---

## Job Dependencies
* **Upstream Jobs**: No explicit upstream jobs were discovered in the `JOB DEPENDENCIES` section of the pre-collected context.
* **Downstream Jobs**: No downstream consumer jobs were found in the `JOB DEPENDENCIES` section.
* **Included Components (Inlined Scripts)**:
  * `DW.HOLE_PFAD` — Confirmed by human review to need no target source.
  * `DW.LESE_LOG` — Confirmed by human review to need no target source.
  * `.DW_INIT` — Sourced in the shell context. Confirmed by human review to need no target source.

---

## Execution Order
The execution sequence in the target Airflow DAG task must strictly follow the original UC4 logic:
1. **Initialize Shell Context**: Source `$HOME/.dw_init` (or equivalent target runtime profile setup).
2. **Variable Resolution**:
   * Set `DWH_JOB_KENNUNG='VVTN_IAR_BGF_GUTSCHR'`.
   * Set `Month_ID` to the computed value of the previous month (`&LASTMONTH_YYYYMM`).
3. **Log Run Context (Reviewer Feedback)**: Output the literal message `Lastmonth is <Month_ID>` to stdout.
4. **Execute Target Binary**: Call `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`.

---

## Scheduling
* **Trigger Mechanism**: This job has no calendar schedule or `JSCH` event defined in the source XML context. It will be configured as an unscheduled DAG (`schedule=None`) to be triggered on-demand, externally via API, or by an upstream process in Airflow.

---

## Schedule & Variables — Must Be Retained
### Variables
* **`DWH_JOB_KENNUNG`**: String variable set to `'VVTN_IAR_BGF_GUTSCHR'`.
* **`Month_ID`**: Set dynamically to the last month (`YYYYMM` format). In the target Airflow DAG, this is computed using Jinja macros based on the logical execution execution context (e.g., `{{ (data_interval_end.in_timezone('Europe/Berlin') - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}`).

---

## Lineage
* **Upstream Producers**: None discovered in this job context.
* **Downstream Consumers**: No downstream cross-job hand-offs identified.
* **Execution Host**: Runs on legacy host `DWHDWH1P` under login `DW.UNIX.ISTNS`. In Cloud Composer, this execution must map to an Airflow worker running a `BashOperator` (or an `SSHOperator` or `KubernetesPodOperator` if executing on an external VM or container).

---

## External System Replacements
* **Local Filesystem**: The legacy shell script refers to files located in `$HOME/aktuell/vorverarbeitung/...`. In Cloud Composer, files should reside on a shared mount (e.g., persistent volume, Filestore, or GCS buckets mounted via Cloud Storage FUSE).

---

## Cross-File Dependencies
* Sourcing `.dw_init` is a cross-file dependency. The target Airflow environment must configure equivalent environment variables (such as database credentials and application paths) directly within Airflow variables, connection secrets, or custom task env parameters.

---

## Target File Plan
* **`dags/dw_dwh_vvtn_iar_bgf_gutschr.py`**
  * **Language**: Python (Airflow DAG)
  * **Source File**: `DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml`
  * **Implementation Approach**: Maps the UC4 `JOBS_UNIX` to an Airflow `BashOperator` (or optionally `SSHOperator`). It sets environment variables, dynamically resolves the execution month identifier via Jinja templating, explicitly outputs the required verification message, and executes the core script.

---

## Environment-Specific Values
The following parameters are extracted from the legacy context and classified below:

| Legacy Parameter/Name | Target Environment Parameter Name | Scope | Target Resolution / Sourcing Strategy |
| :--- | :--- | :--- | :--- |
| `DWHDWH1P` | `GCP_PROJECT` / `SSH_CONN_ID` | **GLOBAL** | Maps to the target GCP environment's project ID or Airflow SSH Connection targeting the VM environment. |
| `DW.UNIX.ISTNS` | `AIRFLOW_CONN_USER` | **GLOBAL** | Sourced via Airflow Connection settings / IAM permissions. |
| `DWH_JOB_KENNUNG` | `DWH_JOB_KENNUNG` | **JOB-SPECIFIC** | Hardcoded inside the DAG task runtime environment context as `'VVTN_IAR_BGF_GUTSCHR'`. |
| `Month_ID` | `Month_ID` | **JOB-SPECIFIC** | Resolved dynamically at execution runtime using the Airflow Jinja macro execution context. |

---

## Risks and Manual Steps

### Unresolved Files (Not in Source Folder)
The actual processing shell scripts and AWK routines referenced by this job execution sequence are not present in the scope of this migration pass. They must be resolved and migrated separately:
* `SOURCE: NOT FOUND — isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift — no candidate`
* `SOURCE: NOT FOUND — isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.awk — no candidate`
* `SOURCE: NOT FOUND — isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk — no candidate`

### Critical Reviewer Feedback Correction
* **Preserve Literal Output Message**: The previous migration attempt missed translating the UC4 `:print` command. Ensure that the literal logging text is printed character-for-character to stdout in the target execution environment.
  * **Legacy Command**: `:print Lastmonth is &Month_ID`
  * **Target Command**: `echo "Lastmonth is $Month_ID"` (must be explicitly added to the target `bash_command` block).

### Legacy Profile Compatibility
* The legacy job sources `$HOME/.dw_init`. If target execution is moved from VM-based SSH to Google Cloud Composer GKE pods, this global profile must be refactored into Composer-native configuration variables or container environment settings.

---

### group 2/4 — DESIGN FAILED

ERROR: NO_MCP_TOOL — design cannot proceed for 'DW.DWH_VVTN_IAR_BGF_GUTSCHR' — no MCP tool is confirmed for this job's source pattern ('UNKNOWN'). Contact the platform team to add or confirm support for this source type before retrying.


---

### group 3/4 — DESIGN FAILED

ERROR: NO_MCP_TOOL — design cannot proceed for 'DW.DWH_VVTN_IAR_BGF_GUTSCHR' — no MCP tool is confirmed for this job's source pattern ('UNKNOWN'). Contact the platform team to add or confirm support for this source type before retrying.


---

### group 4/4 — DESIGN FAILED

ERROR: NO_MCP_TOOL — design cannot proceed for 'DW.DWH_VVTN_IAR_BGF_GUTSCHR' — no MCP tool is confirmed for this job's source pattern ('UNKNOWN'). Contact the platform team to add or confirm support for this source type before retrying.
