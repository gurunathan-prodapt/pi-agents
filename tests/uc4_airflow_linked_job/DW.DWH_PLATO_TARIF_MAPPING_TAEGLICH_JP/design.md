An analysis of the source UC4 structures and migration pathways has been completed. Below is the comprehensive, implementation-ready Migration Design Document.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` workflow is a daily UC4 Job Plan designed to coordinate the setup and maintenance of the Plato Mapping table. This mapping links Plato system tariffs with core Data Warehouse (DWH) base tariffs. The execution flow contains a single main Unix job execution which acts as a processing boundary, orchestrating the daily data alignment logic.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` | `JOBP` (Job Plan) | `<Active>1</Active>` (Active) | Daily orchestration of Plato and DWH tariff mapping structures. |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `JOBS_UNIX` (Unix Job) | `<Active>1</Active>` (Active) | Job executing script/mapping routine for base Plato tariffs. |

## 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **DAG ID** | `dw_dwh_plato_tarif_mapping_taeglich_jp` |
| **Schedule (Cron)** | `None` *(Note: No EVNT_TIME object was provided in the source files. Triggering schedule defaults to None or must be manual/externally orchestrated)* |
| **Start Date** | `datetime(2026, 3, 30)` *(Placeholder matching export date context)* |
| **Catchup** | `False` |
| **Max Active Runs** | `1` *(Enforced by Sync Object definition analysis)* |
| **Is Paused Upon Creation** | `False` *(Source UC4 objects are Active=1)* |
| **Default Args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `dw_dwh_dummy_absd_plato_tarife.py` | `cluster_name`: `YOUR_DATAPROC_CLUSTER_NAME`<br>`project_id`: `YOUR_GCP_PROJECT_ID`<br>`region`: `YOUR_DATAPROC_REGION` | 0 | None | None | None (`CaleOn="0"`) | False (`ActFlg="1"`) | `on_failure_alarm` | This job acts as a placeholder or dummy structure in UC4 execution (script contains `:print Doing nothinig`). Maps to a standard execution stub. |

## 5. Task Dependency Map
```
start_node >> dw_dwh_dummy_absd_plato_tarife >> end_node
```
- **Execution Flow**: The workflow begins at `start_node`. Once successfully evaluated, the `dw_dwh_dummy_absd_plato_tarife` task executes. After its completion, the DAG proceeds to its terminal boundary (`end_node`).

## 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` | UC4 Workflow Name | DAG ID: `dw_dwh_plato_tarif_mapping_taeglich_jp` |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | UC4 Task Name | Task ID: `dw_dwh_dummy_absd_plato_tarife` |
| `DW.CALL_STANDARD` | External UC4 Call/Alarm | Custom Python alerting function: `on_failure_alarm` |

## 7. Error Handling and Retry Strategy
- **Task Retry**: No automatic task retries are defined in the UC4 properties for `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.
- **Postcondition Alarm**: The task has a postcondition checking status outcomes:
  - If status is `ENDED_SKIPPED`, the step exits cleanly with no action.
  - If status is `ENDED_OK`, it continues execution normally.
  - For any other state (representing execution failures/abends), it executes `DW.CALL_STANDARD` with parameters `##911011`. This behavior is mapped directly to the `on_failure_alarm` callback.
- **ENDED_SKIPPED Handling**: The pass-through checking logic does not alter the trigger rules for downstream steps. The default `ALL_SUCCESS` pattern is retained to prevent execution if predecessor runs fail unexpectedly.
- **Sync Object Analysis**: The workflow references sync object `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` with an `Else="Wait"` condition. This is cleanly managed by limiting DAG executions via `max_active_runs=1` in Airflow.

## 8. Developer Notes
- **Missing Trigger Profile**: No `EVNT_TIME` or scheduling export was provided in the source files. The developer must align the DAG schedule definition with the overall enterprise orchestration strategy.
- **GCP Placeholders**: GCP resource keys (GCP project ID, cluster names, regions, and object store buckets) must be filled before production deployment.
- **ENDED_SKIPPED pass-through gaps**: The pass-through logic logic for skipped tasks has no direct hazard-free configuration equivalent in basic Airflow configurations when guard structures are added. The trigger rule on downstream steps remains `ALL_SUCCESS` to avoid breaking downstream dependency cascades. Manual check is recommended if skipped signals must be actively propagated.
- **Ab Initio Script Parameters**: No explicit Ab Initio execution flags (`-j`, `-k`, `-t`) were discovered in the Unix source code block (which simply performs a log print `:print Doing nothinig`). A generic template stub is mapped for `dw_dwh_dummy_absd_plato_tarife.py`.

---

# SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule

# ── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
GCP_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ────────────────────────────
def on_failure_alarm(context):
    """
    Standard warning/alarm notification mechanism.
    Maps to the UC4 DW.CALL_STANDARD handler triggered by abend execution status.
    """
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    error_msg = f"Task {task_id} failed on execution date {execution_date}. Triggering standard call alert ##911011."
    print(error_msg)
    # TODO: Implement organization-specific alerting channels (e.g., Slack, email, PagerDuty)

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_plato_tarif_mapping_taeglich_jp',
    default_args=default_args,
    description='Täglicher Aufbau der Plato Mapping Tabelle zur Verbindung der Plato und der DWH Basistarife',
    schedule_interval=None,  # Or set dedicated cron window as required
    catchup=False,
    max_active_runs=1,       # Emulates UC4 Sync Object Else=Wait behavior
    is_paused_upon_creation=False
)

# ── Start and End Boundary Tasks ────────────────────────
start_node = EmptyOperator(
    task_id='start',
    dag=dag
)

end_node = EmptyOperator(
    task_id='end',
    dag=dag
)

# ── Task: dw_dwh_dummy_absd_plato_tarife ─────────────────
# Maps to JOBS_UNIX object DW.DWH_DUMMY_ABSD_PLATO_TARIFE
pyspark_job_config = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
    }
}

dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    job=pyspark_job_config,
    # Dynamic Job ID derivation ensuring unique name per run
    job_id="dw_dwh_dummy_absd_plato_tarife_{{ run_id | replace(':', '_') | replace('+', '_') }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,  # Default safety trigger rule preserved
    dag=dag
)

# ── Dependencies ─────────────────────────────────────────
start_node >> dw_dwh_dummy_absd_plato_tarife >> end_node
```

---

# SECTION 3 — ADDED CONTEXT

### Job Dependencies & Execution Order
- **Upstream / Cross-Job Sync**:
  - `JOB:DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` (not yet migrated): This is a UC4 Sync Object (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC`) evaluated at workflow start. Its "Wait" logic is preserved via the DAG parameter `max_active_runs=1` (concurrency gate).
- **Execution Order**:
  - Task 1: `dw_dwh_dummy_absd_plato_tarife` (corresponds to `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`).

### Scheduling & Variables
- **Schedule**: Not explicitly defined via UC4 time-events inside the XML files.
- **User Login Context**: The legacy job uses host login credential `DW.UNIX.ISTNS` running on host destination `|DWHDWH1P|HOST`. In Cloud Composer, security and execution boundaries are governed by the Composer Service Account.
- **Variables**: No custom variables are declared in `<DYNVALUES>`.

### Target File Plan
Every source file maps directly to a target destination under the same folder structure hierarchy:

| Source File Path | Target File / Action | Purpose / Reason for Action |
|:---|:---|:---|
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` | `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_plato_tarif_mapping_taeglich_jp.py` | Orchestration DAG representing the main Job Plan (`JOBP`). |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `pyspark_scripts/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Executable task code representing the target workload process. |

### Environment-Specific Values
- **GCP_PROJECT** (GLOBAL): Sourced from environment (`Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`).
- **GCP_REGION** (GLOBAL): Sourced from environment (`Variable.get("GCP_REGION")` or `os.environ.get("GCP_REGION")`).
- **DATAPROC_CLUSTER_NAME** (GLOBAL): Sourced from environment (`Variable.get("DATAPROC_CLUSTER_NAME")`).
- **GCS_BUCKET_NAME** (GLOBAL): Sourced from environment (`Variable.get("GCS_BUCKET_NAME")`).
- **DWHDWH1P** (GLOBAL): Legacy host target mapped to GCP target configuration.

### Risks & Manual Actions
- **SOURCE: NOT FOUND — DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC — no candidate**
  - *Risk / Mitigation*: The sync object `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` was referenced in the workflow structure. It does not exist as a physical file. In the target design, its locking capability is simulated via Airflow's `max_active_runs=1` limit. The engineer must verify if there are other external workflows running concurrently that also share this sync object constraint. If yes, a cross-DAG sensor or a BigQuery state locking mechanism must be introduced.
- **Task Postcondition Alert Verification**:
  - The legacy task runs `DW.CALL_STANDARD ##911011` upon abends. Ensure the target alerting framework (`on_failure_alarm`) is connected to the enterprise monitoring system.
- **Literal Translation Compliance**:
  - Original script script contains a literal German print statement: `Doing nothinig` (with the original typo preserved). This literal string must be carried over exactly to the execution logs within `dw_dwh_dummy_absd_plato_tarife.py`.