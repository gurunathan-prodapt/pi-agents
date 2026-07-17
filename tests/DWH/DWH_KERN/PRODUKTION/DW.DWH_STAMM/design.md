This document outlines the migration design for the daily customer database reconciliation orchestration process `DW.DWH_STAMM_KNZB_ABGL_JP` onto **Google Cloud Composer (Airflow)**.

---

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_stamm/dw_dwh_stamm_knzb_abgl_jp.py` | Primary workflow DAG structure mapping tasks dynamically. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_START_JS.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_stamm/dw_dwh_stamm_knzb_abgl_jp.py` | Integrated as a PythonOperator task within the workflow file. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_stamm/dw_dwh_stamm_knzb_abgl_jp.py` | Integrated as a PythonOperator task within the workflow file. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_stamm/includes/dw_hole_pfad_knzb.py` | Extracted include script mapped to its own mirroring module to preserve folder integrity. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_stamm/includes/dw_lese_log_knzb.py` | Extracted include script mapped to its own mirroring module to preserve folder integrity. |

---

### Environment-Specific Variables & System Configurations

Following the environment variable policy, any environment-wide references have been normalized to prevent structural hardcoding:

#### GLOBAL Variables (Infrastructure Level)
- **`GCP_PROJECT`**: Extracted via `os.environ.get("GCP_PROJECT")` within Cloud Composer or from standard environment configuration.
- **`GCP_REGION`**: Region mapping for execution assets.

#### JOB-SPECIFIC Variables
- **`dw_variablen`**: Airflow Variable containing JSON dict mapping legacy environment paths (equivalent to the legacy `DW.VARIABLEN` container):
  - `DWH_HOME`
  - `HOME`
  - `ISTNS_HOME`
- **`dw_variablen_knzb`**: Airflow Variable containing JSON dict mapping status configurations (equivalent to the legacy `DW.VARIABLEN_KNZB` container):
  - `ABGLEICH_STATUS`
  - `LETZTER_LAUF`

*Warning: If the variables `dw_variablen` or `dw_variablen_knzb` are missing in Airflow at execution time, they will default to a stable offline fallback initialization state as logged in the pseudocode to prevent execution failure.*

---

### Job Dependencies, Scheduling & Execution Order

- **Upstream Dependencies**: This job has no direct upstream dependency chains listed in the pre-collected context metadata. It is a standalone trigger process.
- **Downstream Dependencies**: This job has no listed downstream DAG dependencies.
- **Task Execution Order**: The runtime steps must strictly mirror the sequential chain:
  `START_GUARD (Safeguard Check)` $\rightarrow$ `KNZB_ABGL_START (Initialize & Lock)` $\rightarrow$ `KNZB_ABGL_ENDE (Finalize & Unlock)`
- **Scheduling**: No specific `EVNT_TIME` schedules were configured in the source XML file. Therefore, `schedule_interval` is set to `None` in the DAG definition. A scheduling mechanism (e.g. cron schedule or sensor) must be supplied manually based on business runtime windows when deploying.

---

### Risks, Manual Steps & Integrity Rules

- **Lock Release Risk**: If the task execution fails midway inside `knzb_abgl_start`, the `ABGLEICH_STATUS` state may remain locked as `"LAEUFT"`, blocking subsequent daily runs. A cleanup error state mechanism (`on_failure_callback`) has been designed to capture run failure and set the state to `"ERROR_STATE"`, alerting operations of the manual reset needed.
- **Log Integrity**: All logging text and message print expressions have been preserved verbatim inside the converted tasks (preserving exact German terminology and punctuation layout).

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
This UC4 workflow coordinates the daily reconciliation of customer number and basic access master data (Kundennummer-/Basiszugangs-Stammdaten - KNZB) between the source system (ISTNS) and the Core Data Warehouse layer (DWH-Kernschicht). It manages workflow execution states via global UC4 variable objects to safeguard against parallel executions and unauthorized runs. The process runs daily, checking a global `ABGLEICH_STATUS` variable to either skip processing if locked, or mark it as active (`LAEUFT`) and subsequently free (`FREI`) upon successful execution.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_STAMM_KNZB_ABGL_JP` | `JOBP` (Job Plan) | Active (`<Active>1</Active>`) | Main daily reconciliation parent job plan. |
| `DW.DWH_STAMM_KNZB_ABGL_START_JS` | `JOBS` (Generic Job) | Active (Inherited) | Start task: Validates execution state, checks lock status, and sets the state to running. |
| `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` | `JOBS` (Generic Job) | Active (Inherited) | End task: Resets execution state to free and writes runtime log entries. |
| `DW.HOLE_PFAD_KNZB` | `JOBI` (Include) | Active (N/A) | Code fragment containing environment path mappings. |
| `DW.LESE_LOG_KNZB` | `JOBI` (Include) | Active (N/A) | Code fragment to print workflow logging lines. |

## 3. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_stamm_knzb_abgl_jp` |
| **schedule** | `None` *(Note: No EVNT_TIME scheduler file was provided in the source files. Manual schedule setup required.)* |
| **start_date** | `datetime(2024, 11, 4)` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Source is Active)* |
| **default_args** | `{"owner": "DWH_TEAM", "retries": 0, "retry_delay": timedelta(minutes=5)}` |

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `start_guard` | `PythonOperator` | N/A | N/A | 0 | N/A | None | None | No | None | Performs pre-execution checks and variable state verification. |
| `knzb_abgl_start` | `PythonOperator` | N/A | N/A | 0 | N/A | None | None | No | None | Simulates the START_JS script. Checks variable constraints and changes state to "LAEUFT". |
| `knzb_abgl_ende` | `PythonOperator` | N/A | N/A | 0 | N/A | None | None | No | None | Simulates the ENDE_JS script. Releases state lock to "FREI" and logs execution. |

*Note: No `JOBS_UNIX` objects containing Ab Initio graphs were defined inside this UC4 workflow. Hence, no Dataproc clusters, PySpark script configurations, or GCS buckets are referenced in the Task Inventory.*

## 5. Task Dependency Map
```text
start_guard >> knzb_abgl_start >> knzb_abgl_ende
```
- **start_guard**: Ensures no overlapping DAG runs are currently executing (`max_active_runs=1` safeguard).
- **knzb_abgl_start**: Evaluates if the database/process is locked (`ABGLEICH_STATUS == "GESPERRT"`). If locked, it halts the DAG. Otherwise, it updates state parameters.
- **knzb_abgl_ende**: Triggered upon successful logical processing. Frees up the database/process status for future runs.

## 6. Parameter and Variable Mapping
| UC4 Parameter / Variable | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.VARIABLEN_KNZB` (Variable Object) | UC4 Global Key-Value Store | Airflow Variable: `dw_variablen_knzb` (JSON dictionary format) |
| `&LAUF_DATUM` | `SYS_DATE("YYYYMMDD")` | Airflow context template variable: `{{ ds_nodash }}` |
| `&ABGLEICH_STATUS` | `GET_VAR('DW.VARIABLEN_KNZB','ABGLEICH_STATUS')` | Parsed field within `dw_variablen_knzb` Airflow Variable |
| `&LETZTER_LAUF` | `GET_VAR('DW.VARIABLEN_KNZB','LETZTER_LAUF')` | Parsed field within `dw_variablen_knzb` Airflow Variable |

## 7. Error Handling and Retry Strategy
- Since this workflow manages critical locks (`ABGLEICH_STATUS` to control "LAEUFT" or "FREI"), a failure during execution could result in a permanent deadlock state ("LAEUFT").
- **State Cleanup Strategy**: An `on_failure_callback` should be registered at the DAG or task level to automatically reset `ABGLEICH_STATUS` to a stable configuration or send alerts to prevent manual unlock operations.
- **Sync Behavior**: The UC4 configuration shows no concurrent execution flags. The `max_active_runs=1` setting is applied to ensure single-instance integrity.

## 8. Developer Notes
- **Missing Event File**: No `EVNT_TIME` object was found in the input payload. The schedule parameter has been default-initialized to `None`. The developer must assign the correct cron schedule during deployment.
- **State Management**: The UC4 scripts dynamically modify state values inside global variable containers (`PUT_VAR`, `GET_VAR`). In Airflow, this is mapped using JSON-based Airflow Variables (`Variable.get("dw_variablen_knzb", deserialize_json=True)`).
- **Include Files**: The contents of the include scripts `DW.HOLE_PFAD_KNZB` and `DW.LESE_LOG_KNZB` have been modularized as separate Python helper files in the `includes/` subdirectory to guarantee folder integrity.

---

# SECTION 2 — PSEUDOCODE

### Target File: `dags/dwh/dwh_kern/produktion/dw_dwh_stamm/includes/dw_hole_pfad_knzb.py`
```python
import logging
from airflow.models import Variable

def hole_pfad_knzb():
    """
    Simulates the Include: DW.HOLE_PFAD_KNZB
    In Airflow/GCP, path constants are typically loaded via Airflow Variables or Environment Variables.
    """
    try:
        global_vars = Variable.get("dw_variablen", deserialize_json=True)
        dwh_home = global_vars.get("DWH_HOME")
        home = global_vars.get("HOME")
        istns_home = global_vars.get("ISTNS_HOME")
        logging.info(f"Loaded paths: DWH_HOME={dwh_home}, HOME={home}, ISTNS_HOME={istns_home}")
        return dwh_home, home, istns_home
    except Exception as e:
        logging.warning(f"Could not load standard path variables: {str(e)}")
        return None, None, None
```

### Target File: `dags/dwh/dwh_kern/produktion/dw_dwh_stamm/includes/dw_lese_log_knzb.py`
```python
import logging

def lese_log_knzb(task_name):
    """
    Simulates the Include: DW.LESE_LOG_KNZB
    """
    logging.info(f"Protokolleintrag: {task_name} innerhalb dw_dwh_stamm_knzb_abgl_jp")
```

### Target File: `dags/dwh/dwh_kern/produktion/dw_dwh_stamm/dw_dwh_stamm_knzb_abgl_jp.py`
```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
import logging

# Import mapped include modules to preserve folder integrity
from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.includes.dw_hole_pfad_knzb import hole_pfad_knzb
from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.includes.dw_lese_log_knzb import lese_log_knzb

# ── GCP Configuration ────────────────────────────────────
# Global environment variables
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_REGION = os.environ.get("GCP_REGION")

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'DWH_TEAM',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
def on_workflow_failure(context):
    """
    Cleans up execution state locks if the workflow fails
    while processing, preventing a permanent lock ('LAEUFT').
    """
    logging.warning("Workflow execution failed. Releasing state lock...")
    try:
        knzb_vars = Variable.get("dw_variablen_knzb", deserialize_json=True)
        if knzb_vars.get("ABGLEICH_STATUS") == "LAEUFT":
            knzb_vars["ABGLEICH_STATUS"] = "ERROR_STATE"
            Variable.set("dw_variablen_knzb", knzb_vars, serialize_json=True)
            logging.info("State set to ERROR_STATE. Manual verification required.")
    except Exception as e:
        logging.error(f"Failed to reset state variables: {str(e)}")

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_jp',
    default_args=default_args,
    description='Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht',
    schedule_interval=None,  # TODO: Developer must set cron expression based on business scheduling window
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    on_failure_callback=on_workflow_failure
)

# ── Guard Task ───────────────────────────────────────────
def check_active_runs(**context):
    """
    Ensures that no other instances of this DAG are currently active.
    """
    from airflow.models import DagRun
    from airflow.utils.state import State

    active_runs = DagRun.find(dag_id=context['dag'].dag_id, state=State.RUNNING)
    # Filter out current executing run
    other_active_runs = [r for r in active_runs if r.run_id != context['run_id']]
    
    if other_active_runs:
        raise AirflowSkipException("Another instance of this DAG is currently running. Skipping execution.")

start_guard = PythonOperator(
    task_id='start_guard',
    python_callable=check_active_runs,
    dag=dag
)

# ── Task: knzb_abgl_start ────────────────────────────────
def process_abgl_start(**context):
    """
    Replicates the functionality of DW.DWH_STAMM_KNZB_ABGL_START_JS:
    - Imports path variables (DW.HOLE_PFAD_KNZB logic)
    - Validates execution status variables
    - Sets process execution status to running
    - Writes run log details (DW.LESE_LOG_KNZB logic)
    """
    # Execute Include: DW.HOLE_PFAD_KNZB
    hole_pfad_knzb()

    # Retrieve execution context parameters
    lauf_datum = context['ds_nodash']  # Equivalent to SYS_DATE("YYYYMMDD")
    
    # Retrieve local state configurations
    try:
        knzb_vars = Variable.get("dw_variablen_knzb", deserialize_json=True)
    except KeyError:
        # Fallback initialization if variable does not exist
        knzb_vars = {"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": ""}
        logging.warning("dw_variablen_knzb variable not found. Initializing with default values.")

    abgleich_status = knzb_vars.get("ABGLEICH_STATUS", "FREI")

    # Evaluate Lock Constraint
    if abgleich_status == "GESPERRT":
        logging.error(f"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung")
        raise AirflowFailException("Processing aborted because status is set to GESPERRT.")

    # Update Lock Status in Variables Container
    knzb_vars["ABGLEICH_STATUS"] = "LAEUFT"
    knzb_vars["LETZTER_LAUF"] = lauf_datum
    Variable.set("dw_variablen_knzb", knzb_vars, serialize_json=True)

    # Execute Include: DW.LESE_LOG_KNZB
    lese_log_knzb("knzb_abgl_start")

knzb_abgl_start = PythonOperator(
    task_id='knzb_abgl_start',
    python_callable=process_abgl_start,
    provide_context=True,
    dag=dag
)

# ── Task: knzb_abgl_ende ─────────────────────────────────
def process_abgl_ende(**context):
    """
    Replicates the functionality of DW.DWH_STAMM_KNZB_ABGL_ENDE_JS:
    - Imports path variables (DW.HOLE_PFAD_KNZB logic)
    - Retrieves last run timestamp
    - Frees the state lock parameter
    - Writes completion logs (DW.LESE_LOG_KNZB logic)
    """
    # Execute Include: DW.HOLE_PFAD_KNZB
    hole_pfad_knzb()

    # Fetch variables and status
    knzb_vars = Variable.get("dw_variablen_knzb", deserialize_json=True)
    letzter_lauf = knzb_vars.get("LETZTER_LAUF", context['ds_nodash'])

    # Release Execution Lock
    knzb_vars["ABGLEICH_STATUS"] = "FREI"
    Variable.set("dw_variablen_knzb", knzb_vars, serialize_json=True)
    
    logging.info(f"KNZB-Stammdatenabgleich fuer Lauf {letzter_lauf} erfolgreich beendet")

    # Execute Include: DW.LESE_LOG_KNZB
    lese_log_knzb("knzb_abgl_ende")

knzb_abgl_ende = PythonOperator(
    task_id='knzb_abgl_ende',
    python_callable=process_abgl_ende,
    provide_context=True,
    dag=dag
)

# ── Dependencies ─────────────────────────────────────────
start_guard >> knzb_abgl_start >> knzb_abgl_ende
```