An implementation-ready **MIGRATION DESIGN DOCUMENT** has been constructed to convert the daily customer relationship data reconciliation process (`DW.DWH_STAMM_KNZB_ABGL_JP`) from UC4 to **Cloud Composer (Airflow)**.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml` | `dags/dw_dwh_stamm_knzb_abgl_jp.py` | Primary DAG orchestrator representing the native UC4 Job Plan. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_START_JS.xml` | `dags/dw_dwh_stamm_knzb_abgl_jp.py` (Folded into Airflow Operator) | Handled directly inside the DAG via custom Python/BigQuery operators. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.xml` | `dags/dw_dwh_stamm_knzb_abgl_jp.py` (Folded into Airflow Operator) | Handled directly inside the DAG via custom Python/BigQuery operators. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml` | `dags/includes/dw_hole_pfad_knzb.py` | Extracted into a helper module within the sub-folder matching the source structure to preserve folder integrity. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml` | `dags/includes/dw_lese_log_knzb.py` | Extracted into a helper module within the sub-folder matching the source structure to preserve folder integrity. |

---

### Folder Integrity Rule
As per the **Folder Integrity Rule**, the target repository's folder structure mirrors the source structure. To prevent folder-integrity violations where files from different source folders contribute to the same target file:
* The core Job Plan and Job Script files residing in `DW.DWH_STAMM` map to `dags/dw_dwh_stamm_knzb_abgl_jp.py`.
* The include files residing in the `DW.DWH_STAMM/includes` sub-folder map to separate target files within a corresponding `dags/includes/` directory.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This UC4 workflow (`DW.DWH_STAMM_KNZB_ABGL_JP`) manages the daily reconciliation of customer number/basic access master data (Kundennummer-/Basiszugangs-Stammdaten, or KNZB) between the source system (ISTNS) and the Data Warehouse Core Layer (DWH-Kernschicht). It executes on a daily schedule, orchestrating a sequence of steps starting with initialization and ending with completion confirmation. The workflow is designed as a native UC4 job plan without direct OS-level shell calls.

### 2. UC4 Object Inventory
* `DW.DWH_STAMM_KNZB_ABGL_JP` (`JOBP`): Daily orchestration workflow.
* `DW.DWH_STAMM_KNZB_ABGL_START_JS` (`JOBS`): Initializer script. Evaluates execution lock, sets global variable status to `LAEUFT`, and records execution timestamp.
* `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` (`JOBS`): Completion script. Frees execution status to `FREI` and logs successful completion.
* `DW.HOLE_PFAD_KNZB` (`JOBI`): Helper include which resolves home directories and paths.
* `DW.LESE_LOG_KNZB` (`JOBI`): Standard logging output capturing context.

### 3. Airflow DAG Properties
* **dag_id**: `dw_dwh_stamm_knzb_abgl_jp`
* **schedule**: `"0 6 * * *"` (Calculated daily reconciliation execution)
* **start_date**: `datetime(2024, 11, 4)` (Derived from the last modified timestamp in the XML header)
* **catchup**: `False`
* **max_active_runs**: `1`
* **is_paused_upon_creation**: `False`

### 4. Task Inventory
Since this is classified as a pure-orchestration workflow without external OS or data execution boundaries (`UC4_ONLY`), the UC4 script logic is implemented using native **Airflow PythonOperators** interacting with **Airflow Variables** or **BigQuery** parameters for state tracking. Path and logging operations are imported from the dedicated include files located in the `includes` folder to strictly adhere to the folder integrity model.

| Task ID | Operator | Description |
|---|---|---|
| `task_start_js` | `PythonOperator` | Runs initialization logic, checking execution lock, setting state variables, and calling helper methods. |
| `task_ende_js` | `PythonOperator` | Cleans up, marks state variables as `FREI`, and outputs execution metrics. |

### 5. Task Dependency Map
The execution flow runs sequentially as a single linear pipeline:
```
[Start] >> task_start_js >> task_ende_js >> [End]
```

### 6. Parameter and Variable Mapping
* **Variables Container:** `DW.VARIABLEN_KNZB` maps to Airflow Variables or a configuration object.
* **Variable Keys:**
  * `ABGLEICH_STATUS`: Maps to Airflow Variable `dw_variablen_knzb_abgleich_status`.
  * `LETZTER_LAUF`: Maps to Airflow Variable `dw_variablen_knzb_letzter_lauf`.

### 7. Environment-Specific Values (GCP Variables)
Classified per **ENV VARIABLE POLICY**:

1. **GLOBAL** (Environment-wide configuration)
   * `GCP_PROJECT`: Fetched at runtime via `os.environ.get("GCP_PROJECT")` or Airflow Config.
   * `GCS_BUCKET`: Fetched via `Variable.get("GCS_BUCKET")`.

2. **JOB-SPECIFIC** (Specific to this orchestration pipeline)
   * `dw_variablen_knzb_abgleich_status`: Orchestration state lock.
   * `dw_variablen_knzb_letzter_lauf`: Tracks execution date.
   * `DWH_HOME`, `HOME`, `ISTNS_HOME` (from `dags/includes/dw_hole_pfad_knzb.py`): Stored as job parameters in the DAG configuration or environment variables.

---

## SECTION 2 — CODE IMPLEMENTATION (AIRFLOW DAG)

### Target File: `dags/includes/dw_hole_pfad_knzb.py`
```python
from airflow.models import Variable

def resolve_paths():
    """
    Resolves legacy environment path variables.
    Derived from: DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml
    """
    return {
        "DWH_HOME": Variable.get("DWH_HOME", default_var="/opt/dwh"),
        "HOME": Variable.get("HOME", default_var="/home/dwh_user"),
        "ISTNS_HOME": Variable.get("ISTNS_HOME", default_var="/opt/istns"),
        "DWH_JOB_KENNUNG": "STAMM_KNZB_ABGL"
    }
```

### Target File: `dags/includes/dw_lese_log_knzb.py`
```python
def log_activity(dag_id, task_id):
    """
    Standard logging output capturing task execution context.
    Derived from: DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml
    """
    # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of German log pattern
    print(f"Protokolleintrag: {task_id} innerhalb {dag_id}")
```

### Target File: `dags/dw_dwh_stamm_knzb_abgl_jp.py`
```python
import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# Import modular helper code to respect the Folder Integrity Rule
from includes.dw_hole_pfad_knzb import resolve_paths
from includes.dw_lese_log_knzb import log_activity

# ── GLOBAL ENV CONFIGURATION ─────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT")

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_jp',
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Daily reconciliation schedule
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md="""
    ### Daily Master Data Reconciliation KNZB
    Reconciles Kundennummer/Basiszugangs-Stammdaten (KNZB) master data from the source systems to the DWH Core Layer.
    Converted from UC4 Job Plan: `DW.DWH_STAMM_KNZB_ABGL_JP`
    """
) as dag:

    # ── Task 1: Start JS Logic ─────────────────────────────
    def run_start_js(**context):
        # 1. Resolve path variables (From separated include module)
        job_config = resolve_paths()
        
        # 2. Extract execution timestamp
        lauf_datum = datetime.now().strftime("%Y%m%dd")
        execution_date_str = context['ds']
        
        # 3. Check status locking variable
        abgleich_status = Variable.get("dw_variablen_knzb_abgleich_status", default_var="FREI")
        
        if abgleich_status == "GESPERRT":
            # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of German log/abort message
            print(f"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung")
            raise AirflowFailException(f"Aborted: KNZB-Abgleich fuer {lauf_datum} ist gesperrt")
            
        # 4. Set running status variables
        Variable.set("dw_variablen_knzb_abgleich_status", "LAEUFT")
        Variable.set("dw_variablen_knzb_letzter_lauf", execution_date_str)
        
        # 5. Log activity (From separated include module)
        log_activity(context['dag'].dag_id, context['task'].task_id)

    task_start_js = PythonOperator(
        task_id="dw_dwh_stamm_knzb_abgl_start_js",
        python_callable=run_start_js,
        provide_context=True,
    )

    # ── Task 2: Ende JS Logic ──────────────────────────────
    def run_ende_js(**context):
        # 1. Resolve path variables (From separated include module)
        job_config = resolve_paths()
        
        # 2. Get Last Execution Date
        lauf_datum = Variable.get("dw_variablen_knzb_letzter_lauf", default_var=datetime.now().strftime("%Y%m%d"))
        
        # 3. Release status variable lock
        Variable.set("dw_variablen_knzb_abgleich_status", "FREI")
        
        # 4. Success Completion printing (OUTPUT/PRINT LITERAL RULE)
        print(f"KNZB-Stammdatenabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
        
        # 5. Log activity (From separated include module)
        log_activity(context['dag'].dag_id, context['task'].task_id)

    task_ende_js = PythonOperator(
        task_id="dw_dwh_stamm_knzb_abgl_ende_js",
        python_callable=run_ende_js,
        provide_context=True,
    )

    # ── Execution Path ─────────────────────────────────────
    task_start_js >> task_ende_js
```

---

## SECTION 3 — RISKS & MANUAL ACTIONS

* **RELIANCE ON GLOBAL VARIABLES:** State coordination (locking mechanism `ABGLEICH_STATUS`) depends on GCP/Airflow global variables. This must be monitored to ensure multiple tasks/instances are not writing concurrently to the Airflow DB during high-load scheduling intervals.
* **SCHEDULING ALIGNMENT:** The target schedule is set to run daily at `06:00 UTC` (`0 6 * * *`). The scheduling timing must be cross-referenced with the source system's (ISTNS) actual data delivery windows in the target environment. Ensure upstream extraction pipelines complete prior to this run.