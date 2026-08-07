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
This extraction contains a single UC4 Unix job (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`) which performs a dummy operation (printing "Doing nothinig"). It does not contain any parent workflow (JOBP) or script triggers (SCRI). As such, this job represents a standalone task with no internal dependencies or complex execution flows. It has no calendar-based schedule and is considered to be triggered externally by an unknown source. For migration purposes, this single job is wrapped into its own dedicated, single-task Airflow DAG.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | 1 | dummy |

---

## 3. Scheduling
* **Schedule Analysis**: No `EVNT_TIME` or scheduling helper objects are present in this bundle. No triggering `SCRI` script or parent `JOBP` workflow was supplied.
* **Trigger Mechanism**: Externally triggered (source unknown from this extraction alone).
* **Airflow Schedule**: `schedule=None` (no calendar or cron-based trigger is defined).

---

## 4. Airflow DAG Properties
Since no parent `JOBP` workflow wrapper was supplied in the extraction, a single-task DAG has been created to represent this job.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1 in UC4)* |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_dummy_absd_plato_tarife` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `EmptyOperator` | N/A | N/A | 1 | 5m | None | None | False | None | **# REVIEW-STRUCT:** launcher command `:print Doing nothinig` not recognised — confirm target operator/script manually. |

---

## 6. Task Dependency Map
Since this DAG contains only one standalone task, there are no dependency chains to map.

```python
dwh_dummy_absd_plato_tarife
```

---

## 7. Sync / Concurrency Analysis
* **Sync Constraints**: No active sync rows or cross-DAG locks were defined for this object.
* **Concurrency Handling**: Standard `max_active_runs=1` is applied to prevent overlapping manual or external executions of this DAG.

---

## 8. Error Handling and Retry Strategy
* **Retries**: Configured with a default of 1 retry and a 5-minute delay.
* **Postconditions**: No specific UC4 postcondition actions or failure notifications were defined for this object.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Sanitised Object Name | `dw_dwh_dummy_absd_plato_tarife` *(DAG ID)* |

---

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized Launcher Type**: The job script contains only a native UC4 command (`:print Doing nothinig`) instead of an executable shell command or script reference. An `EmptyOperator` has been used as a structural placeholder. The developer must confirm what actual workload execution (if any) this dummy task is meant to trigger in the target GCP/Airflow environment.
* **Extraction Gap**: No wrapping `JOBP` workflow was supplied. This design assumes the job is run as an independent entry point. If this job is actually a member of an unsupplied master workflow, it should be integrated into that DAG's task structure once defined.

---

# Pseudocode Outline

```python
# ─── IMPORTS ──────────────────────────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ─── GCP CONFIGURATION ────────────────────────────────────────────────────────
# No GCP configurations required for this placeholder task.

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── ON_FAILURE_CALLBACK STUBS ────────────────────────────────────────────────
# No failure callbacks defined in UC4 extraction.

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    description="Converted from UC4 JOBS_UNIX object DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ─── GUARD TASK ───────────────────────────────────────────────────────────
    # None required (No 'Else=Skip' sync row detected)

    # ─── SENSOR TASK ──────────────────────────────────────────────────────────
    # None required (No earliest start time constraints)

    # ─── CALENDAR CHECK TASK ──────────────────────────────────────────────────
    # None required (No calendar constraints active)

    # ─── TASK: dwh_dummy_absd_plato_tarife ────────────────────────────────────
    # # REVIEW-STRUCT: Launcher command ':print Doing nothinig' is unrecognized.
    # Converted to EmptyOperator placeholder. Replace with target operator (e.g. BashOperator,
    # DataprocSubmitJobOperator) once the underlying target runtime pattern is resolved.
    dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id="dwh_dummy_absd_plato_tarife",
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    # Single task DAG; no dependency relationships to declare.
    dwh_dummy_absd_plato_tarife
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Converts the UC4 JOBS_UNIX dummy job into a Cloud Composer Airflow DAG containing a single `EmptyOperator` task acting as a synchronization/placeholder point in the DAG workflow. |

---

### Job Dependencies
* **Downstream**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (JobPlan / Parent workflow, not yet migrated). 
  * *Target Wiring*: Once `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is migrated to Airflow, this DAG's empty task will be wired as a predecessor task or trigger within that parent DAG framework.

---

### Scheduling
* **Trigger Mechanism**: There is no direct schedule defined on this UC4 JOBS_UNIX object. It is intended to be executed on demand or triggered by its parent/predecessor scheduler framework (historically, UC4 orchestration).
* **Airflow Schedule**: `schedule=None` (run on demand or triggered via cross-DAG triggers/parent DAG tasks).

---

### Schedule & Variables
* **Inherited/Event-Triggered linkage**: Designed to run as part of the `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` workflow sequence.
* **Scheduler-Set Variables**: None identified in this UC4 JOBS_UNIX object.

---

### Lineage
* **Execution Host**: Maps from legacy host `dwhdwh1p` to the Cloud Composer GKE environment.
* **Execution Identity/Package**: UC4 Login `DW.UNIX.ISTNS` maps to the target Google Cloud Service Account used to run the Cloud Composer environment tasks.

---

### Cross-File Dependencies
* Shared dependency on the downstream job plan / workflow wrapper `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.

---

### Target File Plan
* **Target File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
  * *Source File*: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
  * *Language*: Python (Airflow DAG)
  * *Purpose*: Executes an `EmptyOperator` task representing the dummy synchronization point.

---

### Environment-Specific Values

All identified environment values are job-specific configurations rather than global infrastructure variables:

1. **JOB-SPECIFIC**:
   * `DW.UNIX.ISTNS` (Legacy Login Package): Represents the execution service account identity on GCP.
   * `dwhdwh1p` (Legacy Target Host): Obsolete on GCP; tasks will run natively within the Cloud Composer Kubernetes environment.

---

### Risks & Manual Steps
* **Downstream Integration**: The downstream job `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is marked as **not yet migrated**. The dependency chain and parent-child task wiring cannot be finalized or verified until that job is migrated.
* **Output / Print Literal Rule Compliance**: The legacy command `:print Doing nothinig` contains a typo ("nothinig"). Per the Output/Print Literal Rule, if this log statement is ever implemented or emitted within the Python task execution, the exact character-for-character literal `"Doing nothinig"` must be preserved.
* **German Operational Notes**: The UC4 documentation block contains German instructions: *"Wiederanlauf ohne weitere Maßnahmen möglich"* (Restart is possible without further actions). This metadata should be retained in the DAG script's module-level docstring for the operations team.