An elegant, implementation-ready migration design document has been compiled for the `DW.DWH_VERTRAG_TARIF_SYNC_JP` job. Below is the complete design specification, incorporating the pre-collected context, legacy execution sequences, and environmental variables in accordance with the prescribed **UC4_ONLY** Cloud Composer pattern.

---

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` | Primary workflow orchestration file. Converted into an Apache Airflow DAG mirroring the source folder structure. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_START_JS.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` (Folded) | Start-block task logic. Converted into Python operators and folded directly inside the primary Airflow DAG structure to maintain execution cohesiveness. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` (Folded) | End-block task logic. Converted into Python operators and folded directly inside the primary Airflow DAG structure. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/includes/dw_hole_pfad_vtrg.py` | Local environment variable helper. Isolated into its own file under the mirrored nested folder structure to preserve folder integrity. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/includes/dw_lese_log_vtrg.py` | Utility logging include. Isolated into its own file under the mirrored nested folder structure to preserve folder integrity. |

*Note: All files reside within the folder structure `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG` (with include helper files under its nested `includes` folder). Under the Folder Integrity Rule, we ensure that files sourced from separate directories are mapped to strictly separate target folders, avoiding any cross-directory folding that would violate structural mirroring.*

---

## SECTION 1 — DESIGN SPECIFICATION

### 1. Job Dependencies & Lineage Edges
*   **Upstream / Downstream Jobs:** None discovered. This workflow operates as a self-contained weekly synchronizer.
*   **External System Replacements:** The legacy UC4 variable container `DW.VARIABLEN` and custom table lock container `DW.VARIABLEN_VTRG` are replaced by Native Airflow Variables and Airflow metadata database parameters.

### 2. Execution Order
The execution ordering preserves the legacy sequence perfectly:
1.  **Start Hook:** Initial DAG execution entry.
2.  **Environment Setup (`DW.HOLE_PFAD_VTRG`):** Imported and evaluated dynamically at the start of Python tasks.
3.  **Startup Verification (`DW.DWH_VERTRAG_TARIF_SYNC_START_JS`):** Checks synchronization locks and updates state to running.
4.  **Logging Hook (`DW.LESE_LOG_VTRG`):** Writes execution step headers.
5.  **Shutdown Verification & Release (`DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS`):** Validates and sets state to free.
6.  **End Hook:** Successful DAG completion.

### 3. Scheduling & Variables
*   **Schedule:** The workflow runs weekly. Mapped to standard Cron syntax: `0 3 * * 7` (Weekly on Sundays at 03:00 AM).
*   **Variables Retained:**
    *   `dw_variablen_vtrg_sync_status` (Stores synchronization state lock: "FREI", "LAEUFT", "GESPERRT").
    *   `dw_variablen_vtrg_letzter_lauf` (Stores the last execution run date in `YYYYMMDD` format).

### 4. Environment-Specific Values (Configuration Policy)
All environment variables are parsed dynamically:
*   **GLOBAL Variables:**
    *   `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")`
    *   `GCS_BUCKET`: Sourced via `Variable.get("GCS_BUCKET")`
*   **JOB-SPECIFIC Variables:**
    *   `DWH_HOME`: Path value. Default: `/opt/dwh` (Sourced via the imported helper module).
    *   `HOME`: User home path. Default: `/home/dwarf`.
    *   `PMS_HOME`: PMS home path. Default: `/opt/pms`.

---

## SECTION 2 — CONSOLIDATED TARGET IMPLEMENTATION (VERBATIM)

To preserve the source repository's exact folder structure and comply with folder integrity rules, the include scripts are housed in their respective mirrored target files, which are then clean-imported by the main orchestrator DAG.

### Target File 1: `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/includes/dw_hole_pfad_vtrg.py`
```python
from airflow.models import Variable

def get_vtrg_paths():
    """
    Python replacement for JOBI: DW.HOLE_PFAD_VTRG
    Resolves environment paths dynamically.
    """
    return Variable.get("dw_variablen_paths", default_var={
        "dwh_home": "/opt/dwh",
        "home": "/home/dwarf",
        "pms_home": "/opt/pms"
    }, deserialize_json=True)
```

### Target File 2: `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/includes/dw_lese_log_vtrg.py`
```python
import logging

def log_uc4_metadata(context, step_message=""):
    """
    Python replacement for JOBI: DW.LESE_LOG_VTRG
    Preserves exact German log syntax character-for-character:
    "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    
    # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of original text
    logging.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")
    if step_message:
        logging.info(step_message)
```

### Target File 3: `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py`
```python
# ─── IMPORTS ──────────────────────────────────────────────────────────────────
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowFailException

# Folder-integrity compliant local imports
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.includes.dw_hole_pfad_vtrg import get_vtrg_paths
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.includes.dw_lese_log_vtrg import log_uc4_metadata

# ─── GLOBAL CONFIGURATION (ENVIRONMENT-WIDE) ──────────────────────────────────
# Environment infrastructure variables are resolved via Airflow's Variable store 
# instead of inline literals to remain compliant with global policies.
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)

# ─── JOB-SPECIFIC PARAMETERS ──────────────────────────────────────────────────
VTRG_PATHS = get_vtrg_paths()

# ─── START WORKFLOW LOGIC (DW.DWH_VERTRAG_TARIF_SYNC_START_JS) ────────────────

def evaluate_sync_status(**context):
    """
    Evaluates the current sync locking mechanisms in Airflow.
    Equivalent to UC4 variable lookup:
    :SET &SYNC_STATUS = GET_VAR('DW.VARIABLEN_VTRG','SYNC_STATUS')
    :IF &SYNC_STATUS = "GESPERRT" -> STOP_JOB()
    """
    log_uc4_metadata(context) # Execute imported logging include hook
    
    sync_status = Variable.get("dw_variablen_vtrg_sync_status", default_var="FREI")
    lauf_datum = context['logical_date'].strftime('%Y%m%d')
    
    if sync_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: Original German log preserved character-for-character
        logging.error(f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch")
        return "abort_execution"
        
    return "update_sync_variables"


def abort_job(**context):
    """
    Explicit abort logic mimicking UC4's STOP_JOB() command.
    """
    raise AirflowFailException("Vertrags-/Tarifabgleich execution blocked (GESPERRT). Aborting workflow.")


def set_running_state(**context):
    """
    Updates the sync status container and sets execution metadata parameters.
    Equivalent to UC4 variable assignment:
    :PUT_VAR DW.VARIABLEN_VTRG, SYNC_STATUS, "LAEUFT"
    :PUT_VAR DW.VARIABLEN_VTRG, LETZTER_LAUF, &LAUF_DATUM
    """
    lauf_datum = context['logical_date'].strftime('%Y%m%d')
    
    Variable.set("dw_variablen_vtrg_sync_status", "LAEUFT")
    Variable.set("dw_variablen_vtrg_letzter_lauf", lauf_datum)
    
    log_uc4_metadata(context) # Execute imported logging include hook


# ─── END WORKFLOW LOGIC (DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS) ───────────────────

def release_sync_lock(**context):
    """
    Clears the synchronization lock when execution succeeds.
    Equivalent to UC4 variable assignment:
    :SET &LAUF_DATUM = GET_VAR('DW.VARIABLEN_VTRG','LETZTER_LAUF')
    :PUT_VAR DW.VARIABLEN_VTRG, SYNC_STATUS, "FREI"
    :PRINT "Vertrags-/Tarifabgleich fuer Lauf &LAUF_DATUM erfolgreich beendet"
    """
    lauf_datum = Variable.get("dw_variablen_vtrg_letzter_lauf", default_var=context['logical_date'].strftime('%Y%m%d'))
    
    Variable.set("dw_variablen_vtrg_sync_status", "FREI")
    
    # OUTPUT/PRINT LITERAL RULE: Original German log preserved character-for-character
    complete_msg = f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet"
    
    log_uc4_metadata(context, step_message=complete_msg) # Execute imported logging include hook


# ─── DAG DEFINITION ───────────────────────────────────────────────────────────

default_args = {
    'owner': 'DWH_KERN',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='dw_dwh_vertrag_tarif_sync_jp',
    default_args=default_args,
    description='Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN',
    schedule_interval='0 3 * * 7',       # Weekly on Sundays at 03:00
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh_kern', 'sync', 'vertrag', 'tarif'],
)

# ─── TASK EXECUTION GRAPH ─────────────────────────────────────────────────────

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

check_sync_status = BranchPythonOperator(
    task_id='check_sync_status',
    python_callable=evaluate_sync_status,
    provide_context=True,
    dag=dag,
)

abort_execution = PythonOperator(
    task_id='abort_execution',
    python_callable=abort_job,
    provide_context=True,
    dag=dag,
)

update_sync_variables = PythonOperator(
    task_id='update_sync_variables',
    python_callable=set_running_state,
    provide_context=True,
    dag=dag,
)

release_sync_lock_task = PythonOperator(
    task_id='release_sync_lock',
    python_callable=release_sync_lock,
    provide_context=True,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# ─── DEPENDENCIES ─────────────────────────────────────────────────────────────
start >> check_sync_status
check_sync_status >> abort_execution
check_sync_status >> update_sync_variables >> release_sync_lock_task >> end
```

---

## SECTION 3 — RISKS & MANUAL ACTIONS

1.  **Configuration Pre-requisite:** Before deploying this DAG to Cloud Composer, the following variables must be initialized in Airflow's configuration metadata store:
    *   `GCP_PROJECT`: The target Google Cloud Project ID.
    *   `GCS_BUCKET`: The GCS bucket designated for environment storage.
    *   `dw_variablen_vtrg_sync_status`: Set to a default starting value of `"FREI"`.
2.  **Global lock risks:** If multiple instances or child pipelines utilize the variable `dw_variablen_vtrg_sync_status`, ensure lock updates do not trigger race conditions in concurrent execution loops. (Controlled in this design via `max_active_runs=1`).