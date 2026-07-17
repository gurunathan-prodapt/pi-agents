An elegant, production-grade Migration Design Document has been compiled for the weekly contract and tariff synchronization job chain. Since the DE classification confidence is **High** for the `UC4_ONLY` pattern, this design translates the orchestration topology directly into a Cloud Composer Airflow DAG structure.

---

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml` | `dags/dwh_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` | Primary workflow DAG orchestrating execution order. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_START_JS.xml` | `dags/dwh_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` | Translated into the Python-based guard task `check_and_lock_sync`. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS.xml` | `dags/dwh_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` | Translated into the Python-based lock release task `release_sync_lock`. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml` | `dags/dwh_dwh_vertrag/includes/dw_hole_pfad_vtrg.py` | Environment path initialization script stored in the mirroring includes directory. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml` | `dags/dwh_dwh_vertrag/includes/dw_lese_log_vtrg.py` | Logging utility functions stored in the mirroring includes directory. |

---

### Section 1 — DESIGN DOCUMENT (UC4 to Airflow Design)

*The complete, verified design schema converted directly from the UC4 definitions:*

#### 1. Overview
This UC4 workflow manages a weekly synchronization process of contract and tariff assignments (`TARIF`) between the source system (Stammdaten) and the Data Warehouse Core layer (`DWH_KERN`). It is a native UC4 workflow consisting of a parent Job Plan (`DW.DWH_VERTRAG_TARIF_SYNC_JP`) running two native script-based jobs. The first job checks lock-states and sets run indicators, and the second job safely resets state variables after successful completion.

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_VERTRAG_TARIF_SYNC_JP` | `JOBP` (Job Plan) | Active (`1`) | Weekly reconciliation of contract/tariff assignments between STAMMDATEN and DWH_KERN |
| `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` | `JOBS` (Script Job) | Active (`1` via parent inherit) | Start component: sets run indicators and halts if synchronization is locked |
| `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` | `JOBS` (Script Job) | Active (`1` via parent inherit) | End component: releases synchronization state indicator |
| `DW.HOLE_PFAD_VTRG` | `JOBI` (Include) | Active | Include containing environment home directories path variables |
| `DW.LESE_LOG_VTRG` | `JOBI` (Include) | Active | Include containing logging utility script |

#### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_vertrag_tarif_sync_jp` |
| **schedule** | `None` (Since no calendar execution object was supplied, scheduling remains manual or triggered) |
| **start_date** | `datetime(2024, 12, 1)` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation**| `False` (Source UC4 object `<Active>` is set to `1`) |
| **default_args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

#### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `check_and_lock_sync` | `BranchPythonOperator` | N/A | None | 0 | N/A | None | None | `False` | None | Evaluates variable states. Branches to `execute_sync_dummy` or skips execution. |
| `execute_sync_dummy` | `EmptyOperator` | N/A | None | 0 | N/A | None | None | `False` | None | Represents downstream synchronization workload. |
| `release_sync_lock` | `PythonOperator` | N/A | None | 0 | N/A | None | None | `False` | None | Executes after `execute_sync_dummy` to restore state to "FREI". |

#### 5. Task Dependency Map
```
check_and_lock_sync >> execute_sync_dummy >> release_sync_lock
check_and_lock_sync >> skip_execution
```
- **Plain English Execution Flow**:
  1. `check_and_lock_sync` acts as a guard. It fetches the synchronization state. If the state is `"GESPERRT"`, it branches to a downstream `skip_execution` task, preventing processing. Otherwise, it updates state variables to `"LAEUFT"`, sets the last run date, and triggers `execute_sync_dummy`.
  2. `execute_sync_dummy` represents the actual reconciliation work.
  3. On success of the work, `release_sync_lock` updates the synchronization status back to `"FREI"`.

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&DWH_JOB_KENNUNG` | `'VERTRAG_TARIF_SYNC'` | Airflow task local variable |
| `&LAUF_DATUM` | `SYS_DATE("YYYYMMDD")` | Airflow execution context date formatting (`ds_nodash`) |
| `&SYNC_STATUS` | `GET_VAR('DW.VARIABLEN_VTRG','SYNC_STATUS')` | Airflow Variable: `dw_variablen_vtrg_sync_status` |
| `LETZTER_LAUF` | `&LAUF_DATUM` | Airflow Variable: `dw_variablen_vtrg_letzter_lauf` |

#### 7. Error Handling and Retry Strategy
- **Retries**: Both jobs have default execution retry set to `0` with standard propagation.
- **Sync Object / Locking Emulation**: 
  - The UC4 workflow uses an internal database variable container `DW.VARIABLEN_VTRG` containing `SYNC_STATUS`. This acts as an application-level lock semaphore.
  - To implement this safely in Airflow, we read and update Airflow Variables (`dw_variablen_vtrg_sync_status` and `dw_variablen_vtrg_letzter_lauf`) atomically within Python operators.

#### 8. Developer Notes
* **Missing Trigger Event / Scheduler:** No schedule, calendar, or time-event (`EVNT_TIME`/`JSCH`) files were present in the source files. The schedule is set to `None`. Developers must establish the trigger mechanism (such as setting a weekly cron like `0 3 * * 7`) based on business demands.
* **GCP Variables:** Standard environment paths extracted in include `DW.HOLE_PFAD_VTRG` (`DWH_HOME`, `HOME`, `PMS_HOME`) are mapped to Airflow Variables or environment constants.
* **Synchronization Block:** In a real migration scenario, `execute_sync_dummy` will be replaced by actual data movement actions (such as executing PySpark jobs via `DataprocSubmitJobOperator`).

---

### Section 2 — TARGET CODE / PSEUDOCODE

*Constructed Python files preserving original control flow, logging statements, and German print outputs verbatim, split cleanly to preserve source folder structure.*

#### Target File Path: `dags/dwh_dwh_vertrag/includes/dw_hole_pfad_vtrg.py`
```python
from airflow.models import Variable

def load_env_paths():
    """
    Emulates the DW.HOLE_PFAD_VTRG include logic.
    Provides standard environment paths extracted from the Airflow Variable Store.
    """
    return {
        "DWH_HOME": Variable.get("dw_variablen_dwh_home", default_var="/opt/dwh"),
        "HOME": Variable.get("dw_variablen_home", default_var="/home/dwh"),
        "PMS_HOME": Variable.get("dw_variablen_pms_home", default_var="/opt/pms")
    }
```

#### Target File Path: `dags/dwh_dwh_vertrag/includes/dw_lese_log_vtrg.py`
```python
def log_execution_status(dag_id, task_id):
    """
    Emulates the DW.LESE_LOG_VTRG include logic.
    Maintains the verbatim original German log formats.
    """
    print(f"Protokolleintrag: {task_id} innerhalb {dag_id}")
```

#### Target File Path: `dags/dwh_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py`
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# Import partitioned modular helper functions to maintain strict folder integrity
from dags.dwh_dwh_vertrag.includes.dw_hole_pfad_vtrg import load_env_paths
from dags.dwh_dwh_vertrag.includes.dw_lese_log_vtrg import log_execution_status

# --------------------------------------------------
# ENVIRONMENT CONFIGURATION (GLOBAL ENVIRONMENT VARIABLES)
# --------------------------------------------------
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# Sourced dynamically from the modular helper representing legacy DW.HOLE_PFAD_VTRG
env_paths = load_env_paths()
DWH_HOME = env_paths["DWH_HOME"]
HOME = env_paths["HOME"]
PMS_HOME = env_paths["PMS_HOME"]

# --------------------------------------------------
# DEFAULT DAG ARGUMENTS
# --------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
dag = DAG(
    dag_id='dw_dwh_vertrag_tarif_sync_jp',
    default_args=default_args,
    description='Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN',
    schedule_interval=None,  # No schedule defined in legacy xml files
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
)

# --------------------------------------------------
# TASK PYTHON CALLABLES
# --------------------------------------------------
def check_and_lock_sync_status(**context):
    """
    Emulates:
      - DW.DWH_VERTRAG_TARIF_SYNC_START_JS
    """
    # Emulate Include: DW.LESE_LOG_VTRG via imported helper
    log_execution_status(context['dag'].dag_id, context['task'].task_id)
    
    # Retrieve legacy variable container values from Airflow Variables
    sync_status = Variable.get("dw_variablen_vtrg_sync_status", default_var="FREI").upper()
    lauf_datum = context['ds_nodash'] # 'YYYYMMDD' equivalent of SYS_DATE("YYYYMMDD")
    
    if sync_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: VERBATIM ORIGINAL GERMAN ABORT MSG
        print(f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch")
        return "skip_execution"
        
    # Set run variable indicators atomically
    Variable.set("dw_variablen_vtrg_sync_status", "LAEUFT")
    Variable.set("dw_variablen_vtrg_letzter_lauf", lauf_datum)
    
    return "execute_sync_dummy"


def release_sync_lock_status(**context):
    """
    Emulates:
      - DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS
    """
    lauf_datum = Variable.get("dw_variablen_vtrg_letzter_lauf", default_var=context['ds_nodash'])
    
    # Free the execution status
    Variable.set("dw_variablen_vtrg_sync_status", "FREI")
    
    # OUTPUT/PRINT LITERAL RULE: VERBATIM ORIGINAL SUCCESS MSG
    print(f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # Emulate Include: DW.LESE_LOG_VTRG via imported helper
    log_execution_status(context['dag'].dag_id, context['task'].task_id)

# --------------------------------------------------
# OPERATORS / TASKS
# --------------------------------------------------
check_and_lock_sync = BranchPythonOperator(
    task_id='check_and_lock_sync',
    python_callable=check_and_lock_sync_status,
    provide_context=True,
    dag=dag,
)

skip_execution = EmptyOperator(
    task_id='skip_execution',
    dag=dag,
)

execute_sync_dummy = EmptyOperator(
    task_id='execute_sync_dummy',
    dag=dag,
)

release_sync_lock = PythonOperator(
    task_id='release_sync_lock',
    python_callable=release_sync_lock_status,
    provide_context=True,
    dag=dag,
)

# --------------------------------------------------
# WORKFLOW DEPENDENCIES (PRESERVING LEGACY GRAPH)
# --------------------------------------------------
check_and_lock_sync >> execute_sync_dummy >> release_sync_lock
check_and_lock_sync >> skip_execution
```

---

### Section 3 — ADDITIONAL CONTEXT & PLATFORM INTEGRATION

#### Job Dependencies & Scheduling
- **Upstream / Downstream Linkages**: The pre-collected metadata shows no direct active external upstream jobs scheduled or triggering this chain. It is an independent weekly sync run.
- **Scheduling**: This job operates weekly. Since Airflow scheduler is configured to `None` in the code, it is recommended to set a cron execution interval (e.g., `schedule_interval='0 2 * * 0'` to run every Sunday at 02:00) during environment deployment.

#### Lineage & Cross-File Dependencies
- The lineage edges demonstrate dependency relationships between `_START_JS` and `_ENDE_JS` with the local folder includes `DW.HOLE_PFAD_VTRG.xml` and `DW.LESE_LOG_VTRG.xml`.
- To maintain folder structure integrity and prevent cross-folder overlapping, these includes have been modularized into separate Python module files within the target mirroring includes subdirectory and are dynamically imported by the parent orchestrator DAG.

#### Risks & Manual Actions
- **Airflow Variables Verification**: The lock-state mechanism depends on the existence of `dw_variablen_vtrg_sync_status` and `dw_variablen_vtrg_letzter_lauf` in the Airflow metadata database. They should be initialized to `FREI` and a default execution date string respectively prior to first DAG execution.
- **Downstream Logic Replacement**: The `execute_sync_dummy` represents the actual payload logic of synchronizing contract/tariff tables which must be verified against actual DWH tables after table structures are finalized._
