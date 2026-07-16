# MIGRATION DESIGN DOCUMENT
**Assembled Job:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml`  
**Source Platform:** UC4 / Automic  
**Target Platform:** BigQuery & Cloud Composer (Airflow)  

---

## 1. File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml` | `dags/dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` | Orchestrates the weekly synchronization workflow using an Airflow DAG. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_START_JS.xml` | `dags/dwh_vertrag/tasks/dw_dwh_vertrag_tarif_sync_start.py` | Python task code representing the startup run initialization, status checks, and metadata updates. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS.xml` | `dags/dwh_vertrag/tasks/dw_dwh_vertrag_tarif_sync_ende.py` | Python task code resetting variables and finalizing the successful workflow run. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml` | `dags/dwh_vertrag/includes/dw_hole_pfad_vtrg.py` | Shared Python helper module loaded dynamically to fetch folder paths. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml` | `dags/dwh_vertrag/includes/dw_lese_log_vtrg.py` | Shared Python helper module loaded dynamically to output execution logs. |

---

## 2. Shared Include Files & Lineage Edges

The workflow depends on two reusable includes (`JOBI` objects in UC4) that are loaded inline in the legacy scripts using the `:inc` command. In the target Airflow environment, these are converted to modular Python helper files imported by individual task modules:

1. **`DW.HOLE_PFAD_VTRG`**  
   * **Purpose:** Reads path environment variables (`DWH_HOME`, `HOME`, `PMS_HOME`) from the shared configuration container `DW.VARIABLEN`.  
   * **Target Mapping:** Implemented as `dags/dwh_vertrag/includes/dw_hole_pfad_vtrg.py` which retrieves globally shared variables from Airflow Variables.

2. **`DW.LESE_LOG_VTRG`**  
   * **Purpose:** Logs the execution state using the active task name (`SYS_ACT_JOBNAME()`) and its parent Job Plan name (`SYS_ACT_JPNAME()`).  
   * **Target Mapping:** Implemented as `dags/dwh_vertrag/includes/dw_lese_log_vtrg.py` using Airflow's built-in standard Python logger.

---

## 3. Schedule, Variables & Variables Mapping

### Scheduling
* **Legacy Trigger:** Weekly execution.
* **Target Scheduling:** Run weekly on **Sundays at 03:00 AM** using standard Airflow cron scheduling.
* **Target Schedule:** `'0 3 * * 7'`

### Variables & State Store
The legacy UC4 environment stores persistent environment states in variable containers (`DW.VARIABLEN` and `DW.VARIABLEN_VTRG`). These are handled in Airflow using **Airflow Variables** or a **BigQuery State Metadata Table** for transactional consistency:

| Legacy Source Variable Container | Key | Target Mapping | Purpose / Expected Value |
| :--- | :--- | :--- | :--- |
| `DW.VARIABLEN` | `DWH_HOME` | `Variable.get("GCP_DWH_HOME")` | Global DWH installation directory. |
| `DW.VARIABLEN` | `HOME` | `Variable.get("GCP_HOME")` | Global user home directory. |
| `DW.VARIABLEN` | `PMS_HOME` | `Variable.get("GCP_PMS_HOME")` | PMS home directory path. |
| `DW.VARIABLEN_VTRG` | `SYNC_STATUS` | `airflow.models.Variable` (or BQ state table) | **JOB-SPECIFIC:** Read/Write status state (`GESPERRT`, `LAEUFT`, `FREI`). |
| `DW.VARIABLEN_VTRG` | `LETZTER_LAUF` | `airflow.models.Variable` (or BQ state table) | **JOB-SPECIFIC:** Stores the date of the last successful run (`YYYYMMDD`). |

---

## 4. Environment-Specific Values (GCP Mapping)

All legacy environmental configurations have been mapped following strict environment-policy standards:

* **GLOBAL (Environment-Wide Constants):**
  * `GCP_PROJECT`: Fetched at runtime via `os.environ.get("GCP_PROJECT")`.
  * `GCP_REGION`: Fetched at runtime via `os.environ.get("GCP_REGION")` or `Variable.get("GCP_REGION")`.
  * `BQ_LOCATION`: Location of BigQuery datasets (defaulting to `'EU'`).
  * `GCS_BUCKET`: The core environment storage bucket, fetched via `Variable.get("GCS_BUCKET")`.

* **JOB-SPECIFIC Parameters:**
  * `SYNC_STATUS` / `LETZTER_LAUF`: Read/write properties managed inside the task modules via `Variable.get` and `Variable.set`.

---

## 5. Risks & Manual Actions

* **CRITICAL CONCURRENT STATE HANDLING:** The job utilizes variable values (`SYNC_STATUS = "GESPERRT"`) as conditional gates. If a job is locked (`GESPERRT`), the legacy system terminates early using `STOP_JOB()`. In the Airflow target, this is preserved via an explicit Airflow validation task that raises an `AirflowSkipException` to gracefully skip subsequent tasks, or an `AirflowFailException` to alert operations depending on preferred alert routing.
* **AUDIT LOG PRINT RULE CONFORMANCE:** In compliance with the **OUTPUT/PRINT LITERAL RULE**, all log statements printed by the tasks in German are preserved character-for-character inside the log statements (e.g. `"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch"`).

---

## 6. Verbatim MCP Transformation & Target File Plan

Below is the complete target file plan and implementation code for the execution sequence of `DW.DWH_VERTRAG_TARIF_SYNC_JP`.

### 6.1. File: `dags/dwh_vertrag/includes/dw_hole_pfad_vtrg.py`
```python
from airflow.models import Variable

def get_path_variables():
    """
    Standard-Include zum Auslesen der Pfad-Variablen aus dem Variablencontainer DW.VARIABLEN.
    """
    dwh_home = Variable.get("GCP_DWH_HOME", default_var="/opt/dwh")
    home = Variable.get("GCP_HOME", default_var="/home/dwh_user")
    pms_home = Variable.get("GCP_PMS_HOME", default_var="/opt/pms")
    
    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "PMS_HOME": pms_home
    }
```

### 6.2. File: `dags/dwh_vertrag/includes/dw_lese_log_vtrg.py`
```python
import logging

logger = logging.getLogger("airflow.task")

def write_execution_log(admjob: str, admjp: str):
    """
    Schreibt einen einfachen Protokolleintrag in das UC4-Laufprotokoll.
    
    OUTPUT/PRINT LITERAL RULE: Verbatim preservation of original output messages.
    """
    logger.info(f"Protokolleintrag: {admjob} innerhalb {admjp}")
```

### 6.3. File: `dags/dwh_vertrag/tasks/dw_dwh_vertrag_tarif_sync_start.py`
```python
from datetime import datetime
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from dags.dwh_vertrag.includes.dw_hole_pfad_vtrg import get_path_variables
from dags.dwh_vertrag.includes.dw_lese_log_vtrg import write_execution_log

def execute_start_task(**context):
    # Execute Path Include
    paths = get_path_variables()
    
    # Define Job Identifiers
    dwh_job_kennung = "VERTRAG_TARIF_SYNC"
    lauf_datum = datetime.now().strftime("%Y%m%d")
    
    # Retrieve Sync Status from central variable container DW.VARIABLEN_VTRG
    sync_status = Variable.get("DW_VARIABLEN_VTRG_SYNC_STATUS", default_var="FREI")
    
    # Conditional verification
    if sync_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: Must match German source text exactly
        print(f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch")
        raise AirflowFailException("Job aborted due to GESPERRT status lock.")
        
    # Set Status tracking
    Variable.set("DW_VARIABLEN_VTRG_SYNC_STATUS", "LAEUFT")
    Variable.set("DW_VARIABLEN_VTRG_LETZTER_LAUF", lauf_datum)
    
    # Execute Log Include
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    write_execution_log(admjob=task_id, admjp=dag_id)
```

### 6.4. File: `dags/dwh_vertrag/tasks/dw_dwh_vertrag_tarif_sync_ende.py`
```python
from airflow.models import Variable
from dags.dwh_vertrag.includes.dw_hole_pfad_vtrg import get_path_variables
from dags.dwh_vertrag.includes.dw_lese_log_vtrg import write_execution_log

def execute_ende_task(**context):
    # Execute Path Include
    paths = get_path_variables()
    
    # Retrieve Last Run Date
    lauf_datum = Variable.get("DW_VARIABLEN_VTRG_LETZTER_LAUF", default_var="UNKNOWN")
    
    # Reset lock status to FREE (FREI)
    Variable.set("DW_VARIABLEN_VTRG_SYNC_STATUS", "FREI")
    
    # OUTPUT/PRINT LITERAL RULE: Must match German source text exactly
    print(f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # Execute Log Include
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    write_execution_log(admjob=task_id, admjp=dag_id)
```

### 6.5. File: `dags/dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` (DAG Orchestrator)
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.dwh_vertrag.tasks.dw_dwh_vertrag_tarif_sync_start import execute_start_task
from dags.dwh_vertrag.tasks.dw_dwh_vertrag_tarif_sync_ende import execute_ende_task

# Default Args configured for data platform operations
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_vertrag_tarif_sync_jp',
    default_args=default_args,
    description='Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN',
    schedule_interval='0 3 * * 7', # Weekly on Sunday at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    dw_dwh_vertrag_tarif_sync_start_js = PythonOperator(
        task_id='dw_dwh_vertrag_tarif_sync_start_js',
        python_callable=execute_start_task,
        provide_context=True
    )

    dw_dwh_vertrag_tarif_sync_ende_js = PythonOperator(
        task_id='dw_dwh_vertrag_tarif_sync_ende_js',
        python_callable=execute_ende_task,
        provide_context=True
    )

    # Sequence Flow execution order: Start -> Ende
    dw_dwh_vertrag_tarif_sync_start_js >> dw_dwh_vertrag_tarif_sync_ende_js
```