An analysis of the CM and SAT MCP tool listings indicates that both endpoints returned errors (connection failed and 404 Not Found, respectively). However, because all necessary source code, structural, and metadata context have been pre-collected and are fully visible in the prompt, we can proceed with creating a comprehensive, implementation-ready migration design document. 

No files are skipped, and all legacy components are mapped 1:1 or appropriately consolidated in line with the high-confidence **UC4_ONLY** prescription to target **Cloud Composer (Airflow)**.

---

# MIGRATION DESIGN DOCUMENT
**Job Name:** `DW.DWH_VERTRAG_TARIF_SYNC_JP`  
**Source Path:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml`  
**Source Platform:** UC4 / Automic Workflows  
**Target Platform:** BigQuery & Google Cloud Composer (Apache Airflow)  
**Migration Pattern:** UC4_ONLY (Pure orchestration of state variables, paths, and metadata log writing)

---

## 1. File Disposition

The following table summarizes the disposition of each source file. To preserve folder integrity, all target files map to mirrored locations corresponding to their source folders.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml` | `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py` | Primary Airflow DAG file. Recreates the workflow structure, orchestrates task execution order, and manages task dependencies. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_START_JS.xml` | `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py` | Integrated as a PythonOperator (`start_task`) within the main DAG file. Validates status constraints and flags start conditions. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS.xml` | `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py` | Integrated as a PythonOperator (`ende_task`) within the main DAG file. Resets status variables after a successful run. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml` | `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/utils/dw_hole_pfad_vtrg.py` | Shared utility script mapped to a mirrored folder structure to resolve global/environment paths. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml` | `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/utils/dw_lese_log_vtrg.py` | Shared logging utility script mapped to a mirrored folder structure to output standardized execution logs. |

---

## 2. Shared Utilities & Refactored Includes

To respect the **Folder Integrity Rule**, the shared include scripts are refactored into Python helper modules within a mirrored sub-directory structure `utils`. This keeps them reusable across other DAGs in the same sub-folder.

### A. Path Resolution Include
**Target File:** `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/utils/dw_hole_pfad_vtrg.py`

```python
import os
from airflow.models import Variable

def get_vtrg_paths():
    """
    Standard-Include zum Auslesen der Pfad-Variablen aus dem Variablencontainer DW.VARIABLEN.
    Retrieves global paths using Airflow Variables, falling back to OS environment.
    """
    # Try fetching from Airflow Variables (Global Config Store), default to os.environ
    dwh_home = Variable.get("DWH_HOME", default_var=os.environ.get("DWH_HOME"))
    home = Variable.get("HOME", default_var=os.environ.get("HOME"))
    pms_home = Variable.get("PMS_HOME", default_var=os.environ.get("PMS_HOME"))
    
    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "PMS_HOME": pms_home
    }
```

### B. Standard Logging Include
**Target File:** `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/utils/dw_lese_log_vtrg.py`

```python
import logging

def write_execution_log(dag_id, task_id):
    """
    Schreibt einen einfachen Protokolleintrag in das Airflow-Task-Laufprotokoll.
    Preserves literal original-language outputs character-for-character.
    """
    # OUTPUT/PRINT LITERAL RULE: Exact original German logging format preserved
    log_message = f"Protokolleintrag: {task_id} innerhalb {dag_id}"
    logging.info(log_message)
    print(log_message)
```

---

## 3. Main Target File Plan (Airflow DAG)

**Target File:** `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py`  
**Description:** Extracted and translated logic from `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` and `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` consolidated into a standard Airflow DAG. State checking uses Airflow Variables to mimic the legacy `DW.VARIABLEN_VTRG` variable container.

```python
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

# Import our refactored folder-integrity compliant includes
from DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.utils.dw_hole_pfad_vtrg import get_vtrg_paths
from DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.utils.dw_lese_log_vtrg import write_execution_log

# Environment variables classification
# GLOBAL (Environment-Wide Infra Parameters)
# Note: Handled dynamically via `Variable.get` within the includes.

# JOB-SPECIFIC PARAMETERS
DAG_ID = "DW_DWH_VERTRAG_TARIF_SYNC_JP"
JOB_NAME_START = "DW.DWH_VERTRAG_TARIF_SYNC_START_JS"
JOB_NAME_ENDE = "DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS"
DWH_JOB_KENNUNG = "VERTRAG_TARIF_SYNC"

def run_start_js(**context):
    """
    Logic from DW.DWH_VERTRAG_TARIF_SYNC_START_JS.xml
    Validates if sync is locked, and updates sync state status and execution date.
    """
    # 1. Include Path logic
    paths = get_vtrg_paths()
    
    # 2. Variable resolution
    execution_date = context['ds_nodash']  # YYYYMMDD format
    
    # Access state-tracking variable container (using Airflow Variable)
    # Default to "FREI" if not defined yet
    sync_status = Variable.get("DW_VARIABLEN_VTRG__SYNC_STATUS", default_var="FREI")
    
    # 3. Status Gate check
    if sync_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: Literal print text must match the legacy UC4 print statement
        msg = f"Vertrags-/Tarifabgleich fuer {execution_date} ist gesperrt - Abbruch"
        print(msg)
        raise AirflowFailException(msg)
        
    # 4. State Updates
    Variable.set("DW_VARIABLEN_VTRG__SYNC_STATUS", "LAEUFT")
    Variable.set("DW_VARIABLEN_VTRG__LETZTER_LAUF", execution_date)
    
    # 5. Include Log logic
    write_execution_log(DAG_ID, JOB_NAME_START)

def run_ende_js(**context):
    """
    Logic from DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS.xml
    Resets the sync status variable back to 'FREI'.
    """
    # 1. Include Path logic
    paths = get_vtrg_paths()
    
    # 2. Get last run execution date for print statements
    lauf_datum = Variable.get("DW_VARIABLEN_VTRG__LETZTER_LAUF", default_var=context['ds_nodash'])
    
    # 3. Put variable state change
    Variable.set("DW_VARIABLEN_VTRG__SYNC_STATUS", "FREI")
    
    # OUTPUT/PRINT LITERAL RULE: Literal print text must match the legacy UC4 print statement
    print(f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # 4. Include Log logic
    write_execution_log(DAG_ID, JOB_NAME_ENDE)


# Define standard weekly schedule logic as declared in legacy JP metadata
default_args = {
    'owner': 'DWH_VERTRAG_OWNER',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 12, 1),
    'retries': 0
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN",
    schedule_interval="0 6 * * 0",  # Sunday morning weekly schedule
    catchup=False,
    max_active_runs=1
) as dag:

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=run_start_js,
        provide_context=True
    )

    ende_task = PythonOperator(
        task_id="ende_task",
        python_callable=run_ende_js,
        provide_context=True
    )

    # Execution order wiring (task sequence)
    start_task >> ende_task
```

---

## 4. Scheduling & Variable Management

### Retained Scheduler Variables
Legacy workflows utilized central parameter tables/variable containers. Airflow Variables are used to model these tables with job-specific grouping syntax:

1. **`DW_VARIABLEN_VTRG__SYNC_STATUS`** (Job-Specific state)
   * **Equivalent Target Source:** Airflow Admin -> Variable Store.
   * **Logical Initial State:** `"FREI"` (valid states: `"FREI"`, `"LAEUFT"`, `"GESPERRT"`).
   * **Purpose:** Acts as a semaphore variable to prevent overlapping processing or manually block run execution.
2. **`DW_VARIABLEN_VTRG__LETZTER_LAUF`** (Job-Specific state metadata)
   * **Equivalent Target Source:** Airflow Variable updated dynamically.
   * **Purpose:** Stores the last processed execution timestamp in format `YYYYMMDD`.

### Environment Configuration Variables (GLOBAL)
These variables must be registered globally within the Cloud Composer environment context:
* **`DWH_HOME`**: Mapped as a Global Airflow Variable (e.g. `/home/airflow/gcs/dags`).
* **`HOME`**: Mapped as a Global Airflow Variable.
* **`PMS_HOME`**: Mapped as a Global Airflow Variable.

### Orchestration & Scheduling Frequency
* **Legacy Trigger:** Weekly Run (configured here as `0 6 * * 0` weekly Sunday scheduler).
* **Scheduling Target:** Composer Cron Native Schedule.

---

## 5. Risks & Manual Actions

1. **State Semaphore Sync**: Because this pipeline depends on setting variables (`SYNC_STATUS`) to preserve consistency across runs, it relies on Airflow Variables being writable at execution time. Ensure the execution service account has sufficient IAM rights to read and write variables in Cloud Composer.
2. **Sequential Queue Configuration**: Ensure that `max_active_runs=1` remains active on the DAG configuration. This mimics the UC4 active queue lock mechanisms, ensuring multi-week schedule overlaps do not overwrite `LETZTER_LAUF` concurrently.
3. **No Target Schema Changes**: This design acts purely as an orchestration engine (`UC4_ONLY`). Actual data loader operations or synchronization steps executed via DB links outside this specific package must be executed in parallel or coordinated downstream from the output of these tasks. No data tables are modified inside these files.