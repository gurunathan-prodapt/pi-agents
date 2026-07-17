An analysis of the source files, execution plan, and the pre-collected context has been performed. Because BOTH the CM MCP and SAT MCP endpoints returned connection or 404 errors (as indicated in the tool listings), no external MCP tools could be successfully called. 

Per the guardrails provided in the instructions, when both MCP services are unavailable, we proceed by manually writing the complete, production-grade target implementation from the rich pre-collected context. No source files are missing; thus, a complete Cloud Composer (Airflow DAG) migration design document has been designed, fully preserving the logic, structure, variables, and literal output text.

---

# MIGRATION DESIGN DOCUMENT

## 1. Executive Summary
* **Legacy Source Component**: `DW.DWH_STAMM_KNZB_ABGL_JP` (Daily reconciliation workflow for customer numbers and basic access master data).
* **Source Type**: UC4/Automic Job Plan (JOBP) and associated Jobs (JOBS) / Includes (JOBI).
* **Target Platform**: Google Cloud Platform (GCP)
* **Target Orchestrator**: Cloud Composer (Apache Airflow)
* **Migration Strategy**: 1:1 Airflow DAG migration of the pure orchestration logic.
* **Migration Pattern**: `UC4_ONLY` — Pure orchestration and variable maintenance. No direct data-plane transformations exist in this specific job chain.

---

## 2. File Disposition Table

To ensure strict compliance with the **Folder Integrity Rule**, target files are split strictly to mirror their source directories. The includes folder content is compiled into its own separate utility module under a matching target path, preventing any multi-directory mixing inside a single target file.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml` | `dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW_DWH_STAMM_KNZB_ABGL_JP.py` | Primary DAG orchestration file defining workflow structure and tasks. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_START_JS.xml` | `dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW_DWH_STAMM_KNZB_ABGL_JP.py` (Folded) | Processed as the start task in the Airflow DAG (`start_task`). |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.xml` | `dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW_DWH_STAMM_KNZB_ABGL_JP.py` (Folded) | Processed as the end task in the Airflow DAG (`ende_task`). |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml` | `dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW_HOLE_PFAD_KNZB.py` | Shared path retrieval logic, translated to standard python module in the includes subfolder. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml` | `dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW_LESE_LOG_KNZB.py` | Standard logging logic, converted to Python logger module in the includes subfolder. |

---

## 3. Scheduling & Variable Management

### Retained Scheduler Variables
UC4 uses variable containers (`DW.VARIABLEN` and `DW.VARIABLEN_KNZB`) to store run status and context paths. These are mapped to Airflow Variables or runtime DAG context:

1. **`DW.VARIABLEN` (Global Infrastructure Paths)**:
   * `DWH_HOME`: Path of DWH Home directory.
   * `HOME`: Path of user/system Home directory.
   * `ISTNS_HOME`: Path of source system home directory.
   * *Airflow Mapping*: Sourced via Airflow Variables (`Variable.get("DW_VARIABLEN", deserialize_json=True)`).

2. **`DW.VARIABLEN_KNZB` (Job-Specific Execution States)**:
   * `ABGLEICH_STATUS`: Can be `"GESPERRT"`, `"LAEUFT"`, or `"FREI"`. Prevent run if `"GESPERRT"`.
   * `LETZTER_LAUF`: Date of the last successful run (`YYYYMMDD`).
   * *Airflow Mapping*: Managed dynamically using custom Airflow Variable updates via Python operators.

### Scheduling
* **Trigger Event**: Daily run.
* **Target Scheduling**: Configured in Composer with `schedule_interval="0 4 * * *"` (Daily at 04:00 AM UTC, or as desired based on environment setup).

---

## 4. Execution Order and Task Dependency Plan

The target Airflow DAG preserves the legacy sequence perfectly:
1. **`start_task` (derived from `DW.DWH_STAMM_KNZB_ABGL_START_JS`)**:
   * Evaluates if variable `ABGLEICH_STATUS` is set to `"GESPERRT"`.
   * If `"GESPERRT"`, raises an Airflow SkipException or FailException as defined by UC4's `STOP_JOB()`.
   * If free, sets `ABGLEICH_STATUS` to `"LAEUFT"` and updates `LETZTER_LAUF` with today's date (`SYS_DATE`).
   * Prints standard log entries using the include module.
2. **`ende_task` (derived from `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`)**:
   * Resets status variable `ABGLEICH_STATUS` back to `"FREI"`.
   * Logs process completion using the include module.

---

## 5. Environment-Specific Values Classification

1. **GLOBAL (Environment-Wide Constants)**:
   * `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
   * `GCS_BUCKET`: Default target data storage bucket if needed downstream.

2. **JOB-SPECIFIC (Job Parameters)**:
   * `DWH_HOME`: Legacypath `DW.VARIABLEN` mapping. Sourced from `Variable.get("DWH_HOME", default_var="/opt/dwh")`.
   * `HOME`: Legacypath `DW.VARIABLEN` mapping. Sourced from `Variable.get("HOME", default_var="/home/airflow")`.
   * `ISTNS_HOME`: Sourced from `Variable.get("ISTNS_HOME", default_var="/opt/istns")`.

---

## 6. Target Code Plan & Python Orchestration Script

To maintain absolute folder integrity, the shared includes are written to their respective subfolder scripts and imported by the main orchestration DAG.

### File: `dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW_HOLE_PFAD_KNZB.py`
```python
import logging
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

def include_hole_pfad_knzb():
    """
    Translates legacy JOBI 'DW.HOLE_PFAD_KNZB'
    Retrieves variables from 'DW.VARIABLEN' Airflow variables.
    """
    try:
        dw_variablen = Variable.get("DW_VARIABLEN", deserialize_json=True)
    except KeyError:
        # Fallbacks for variables if not defined in the Airflow environment
        dw_variablen = {
            "DWH_HOME": "/opt/dwh",
            "HOME": "/home/airflow",
            "ISTNS_HOME": "/opt/istns"
        }
    
    dwh_home = dw_variablen.get("DWH_HOME")
    home = dw_variablen.get("HOME")
    istns_home = dw_variablen.get("ISTNS_HOME")
    
    logger.info(f"Loaded paths - DWH_HOME: {dwh_home}, HOME: {home}, ISTNS_HOME: {istns_home}")
    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "ISTNS_HOME": istns_home
    }
```

### File: `dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW_LESE_LOG_KNZB.py`
```python
import logging

logger = logging.getLogger("airflow.task")

def include_lese_log_knzb(task_name, dag_name):
    """
    Translates legacy JOBI 'DW.LESE_LOG_KNZB'
    OUTPUT/PRINT LITERAL RULE: Must output exact German logs unchanged.
    """
    # Original: :PRINT "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
    logger.info(f"Protokolleintrag: {task_name} innerhalb {dag_name}")
```

### File: `dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW_DWH_STAMM_KNZB_ABGL_JP.py`
```python
import os
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# Resolve path relative to DAGs root folder to ensure Python can resolve includes cleanly
sys.path.append(os.path.join(os.path.dirname(__file__), 'includes'))

# Import the mapped dependencies from their matching folders
from DW_HOLE_PFAD_KNZB import include_hole_pfad_knzb
from DW_LESE_LOG_KNZB import include_lese_log_knzb

# Initialize logging
logger = logging.getLogger("airflow.task")

# ==========================================
# ENV VARIABLE POLICY: Global Configurations
# ==========================================
GCP_PROJECT = os.environ.get("GCP_PROJECT")

def execute_start_js(**context):
    """
    Translates legacy JOBS 'DW.DWH_STAMM_KNZB_ABGL_START_JS'
    """
    task_id = context['task'].task_id
    dag_id = context['dag'].dag_id
    
    # 1. Execute INCLUDE DW.HOLE_PFAD_KNZB
    paths = include_hole_pfad_knzb()
    
    # 2. Local variables
    dwh_job_kennung = 'STAMM_KNZB_ABGL'
    lauf_datum = datetime.now().strftime("%Y%m%d")
    
    # Get current status from DW.VARIABLEN_KNZB variable store
    try:
        knzb_vars = Variable.get("DW_VARIABLEN_KNZB", deserialize_json=True)
    except KeyError:
        knzb_vars = {"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": ""}

    abgleich_status = knzb_vars.get("ABGLEICH_STATUS", "FREI")
    
    # 3. Check condition: :IF &ABGLEICH_STATUS = "GESPERRT"
    if abgleich_status == "GESPERRT":
        # Original: :PRINT "KNZB-Abgleich fuer &LAUF_DATUM ist gesperrt - Abbruch der Verarbeitung"
        logger.error(f"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung")
        raise AirflowFailException(f"Abrupt stop triggered by legacy logic. Status: {abgleich_status}")
    
    # 4. Set status variables: :PUT_VAR
    knzb_vars["ABGLEICH_STATUS"] = "LAEUFT"
    knzb_vars["LETZTER_LAUF"] = lauf_datum
    Variable.set("DW_VARIABLEN_KNZB", knzb_vars, serialize_json=True)
    
    # 5. Execute INCLUDE DW.LESE_LOG_KNZB
    include_lese_log_knzb(task_name=task_id, dag_name=dag_id)

def execute_ende_js(**context):
    """
    Translates legacy JOBS 'DW.DWH_STAMM_KNZB_ABGL_ENDE_JS'
    """
    task_id = context['task'].task_id
    dag_id = context['dag'].dag_id
    
    # 1. Execute INCLUDE DW.HOLE_PFAD_KNZB
    paths = include_hole_pfad_knzb()
    
    # 2. Retrieve last run date
    try:
        knzb_vars = Variable.get("DW_VARIABLEN_KNZB", deserialize_json=True)
    except KeyError:
        knzb_vars = {"ABGLEICH_STATUS": "LAEUFT", "LETZTER_LAUF": datetime.now().strftime("%Y%m%d")}
        
    lauf_datum = knzb_vars.get("LETZTER_LAUF", datetime.now().strftime("%Y%m%d"))
    
    # 3. Reset status: :PUT_VAR DW.VARIABLEN_KNZB, ABGLEICH_STATUS, "FREI"
    knzb_vars["ABGLEICH_STATUS"] = "FREI"
    Variable.set("DW_VARIABLEN_KNZB", knzb_vars, serialize_json=True)
    
    # 4. Print completion statement in German (verbatim)
    # Original: :PRINT "KNZB-Stammdatenabgleich fuer Lauf &LAUF_DATUM erfolgreich beendet"
    logger.info(f"KNZB-Stammdatenabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # 5. Execute INCLUDE DW.LESE_LOG_KNZB
    include_lese_log_knzb(task_name=task_id, dag_name=dag_id)


# ==========================================
# Airflow DAG Definition
# ==========================================
default_args = {
    'owner': 'DWH_STAMM_TEAM',
    'start_date': datetime(2024, 11, 4),
    'retries': 0,
}

with DAG(
    dag_id='DW_DWH_STAMM_KNZB_ABGL_JP',
    default_args=default_args,
    description='Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht',
    schedule_interval='0 4 * * *',  # Runs daily at 04:00 UTC
    catchup=False,
    tags=['dwh', 'stamm_knzb', 'uc4_migration']
) as dag:

    # Start Task (sets run lock)
    start_task = PythonOperator(
        task_id='DW_DWH_STAMM_KNZB_ABGL_START_JS',
        python_callable=execute_start_js,
        provide_context=True,
    )

    # End Task (releases lock)
    ende_task = PythonOperator(
        task_id='DW_DWH_STAMM_KNZB_ABGL_ENDE_JS',
        python_callable=execute_ende_js,
        provide_context=True,
    )

    # Establish sequence matching UC4 design sequence exactly
    start_task >> ende_task
```

---

## 7. Risks & Manual Actions
* **Variable Initialization**: The target Airflow environment must have the variable key `DW_VARIABLEN_KNZB` (JSON format) pre-configured with keys `{"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": ""}` prior to the first execution of this DAG.
* **Mutual Exclusion**: If there are other UC4 migrated processes modifying `DW.VARIABLEN_KNZB`, the environment should utilize Airflow Pools or DAG locks to prevent concurrent race conditions over status variable updates.