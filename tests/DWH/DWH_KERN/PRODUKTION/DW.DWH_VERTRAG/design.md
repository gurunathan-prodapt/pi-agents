# MIGRATION DESIGN DOCUMENT

## File Disposition

| Source File Path | Target File / Task | Disposition | Description |
| :--- | :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml` | `dags/dw_dwh_vertrag_tarif_sync_jp.py` | Target File | Orchestrating Airflow DAG representing the weekly synchronizing job plan. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_START_JS.xml` | `dags/tasks/dw_dwh_vertrag_tarif_sync_start.py` (referenced by task `start_js`) | Target File | Start-control block script that sets status variables and validates run state. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS.xml` | `dags/tasks/dw_dwh_vertrag_tarif_sync_ende.py` (referenced by task `ende_js`) | Target File | End-control block script resetting the status variables to free upon completion. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml` | Merged into target task python files | Merged | Common path lookup routines merged directly as configurations into task scripts. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml` | Merged into target task python files | Merged | Simple log formatting logic implemented directly using python's native `logging` library. |

---

## 1. Executive Summary & Migration Pattern
* **Prescribed Migration Pattern**: `UC4_ONLY` mapping to Google Cloud Composer (Apache Airflow).
* **Approach**: This job is a pure orchestration workflow running UC4 native scripting commands. It manages a weekly synchronization process by checking locks, updating dynamic status keys, and performing safe end-state resets. The target architecture maps this 1:1 to an Airflow DAG. 
* **Variable Control Mechanism**: Instead of UC4 global variables (`PUT_VAR` / `GET_VAR`), the Airflow DAG and task scripts will read and write to **Airflow Variables** via the standard python metadata database interface.

---

## 2. Shared Include Logic (Merged Components)

The two utility includes `DW.HOLE_PFAD_VTRG` and `DW.LESE_LOG_VTRG` are folded into the target task implementations natively:

### A. Path Resolution (`DW.HOLE_PFAD_VTRG`)
The original include reads:
```
:SET &DWH_HOME = GET_VAR('DW.VARIABLEN','DWH_HOME')
:SET &HOME     = GET_VAR('DW.VARIABLEN','HOME')
:SET &PMS_HOME = GET_VAR('DW.VARIABLEN','PMS_HOME')
```
In the BigQuery / Airflow environment, these paths are treated as **GLOBAL** environment configurations sourced dynamically:
* Python Environment Lookup:
  ```python
  from airflow.models import Variable
  DWH_HOME = Variable.get("DWH_HOME", default_var="/opt/dwh")
  HOME = Variable.get("HOME", default_var="/home/airflow")
  PMS_HOME = Variable.get("PMS_HOME", default_var="/opt/pms")
  ```

### B. Standard Output Logging (`DW.LESE_LOG_VTRG`)
The original include outputs active workflow context:
```
:SET &ADMJP  = SYS_ACT_JPNAME()
:SET &ADMJOB = SYS_ACT_JOBNAME()
:PRINT "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
```
Under Python/Airflow, the standard logging API will preserve this output format EXACTLY (meeting the **OUTPUT/PRINT LITERAL RULE**):
```python
import logging
# Context obtained dynamically from Airflow execution context in the calling task
logging.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")
```

---

## 3. Job Dependency, Execution Order, & Scheduling

### Job Dependencies
* **Upstream**: None specified in context.
* **Downstream**: None specified in context.

### Execution Order
The target DAG preserves the exact 3-step sequential linear dependency graph:
1. **`start_js`** (`DW.DWH_VERTRAG_TARIF_SYNC_START_JS`)
2. **`ende_js`** (`DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS`)

### Scheduling & Variables
* **Schedule**: Weekly schedule (set as `@weekly` or `0 0 * * 0` in the Airflow DAG setup).
* **State Variables Required**:
  * `DW.VARIABLEN_VTRG` / `SYNC_STATUS` (Mapped to Airflow Variable `vtrg_sync_status`).
  * `DW.VARIABLEN_VTRG` / `LETZTER_LAUF` (Mapped to Airflow Variable `vtrg_letzter_lauf`).

---

## 4. Environment-Specific Values Classification

1. **GLOBAL** (Infrastructure and Project Level constants):
   * `GCP_PROJECT`: Airflow dynamic environment configuration.
   * `DWH_HOME`: Airflow global Variable store.
   * `HOME`: Airflow global Variable store.
   * `PMS_HOME`: Airflow global Variable store.

2. **JOB-SPECIFIC**:
   * `vtrg_sync_status`: Dynamic workflow semaphore variable. Values: `"FREI"`, `"LAEUFT"`, `"GESPERRT"`.
   * `vtrg_letzter_lauf`: Keeps track of the last execution timestamp formatted as `YYYYMMDD`.

---

## SECTION 5 — VERBATIM UC4 TO AIRFLOW DAG DESIGN DESIGN DOC

Below is the verbatim design output mapping the UC4 workflow components to their Cloud Composer Airflow task layouts.

```markdown
=== Result for local/home/gurunathan_t/clean_migration_dataset/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml ===
Based on the provided UC4 XML content, here is the detailed Design Document and Pseudocode blueprint for converting the workflow into an Apache Airflow DAG.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This UC4 workflow (`DW.DWH_VERTRAG_TARIF_SYNC_JP`) manages the weekly synchronization and alignment of contract and tariff allocations (Vertrags-/Tarifzuordnung) between the source system (Stammdaten) and the Core Data Warehouse layer (`DWH_KERN`). It is a native UC4 orchestration workflow that runs on a weekly schedule. The workflow consists of a start guard job followed by an execution sync ending job, operating in a linear chain.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_VERTRAG_TARIF_SYNC_JP` | `JOBP` (Job Plan) | `<Active>1</Active>` (Active) | Weekly contract/tariff reconciliation Job Plan orchestration workflow. |
| `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` | `JOBS` (Referenced) | Active | Start task/sensor referenced in the Job Plan. |
| `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` | `JOBS` (Referenced) | Active | End reconciliation task referenced in the Job Plan. |

### 3. Airflow DAG Properties
| Property | Value | Note |
| :--- | :--- | :--- |
| **DAG ID** | `dw_dwh_vertrag_tarif_sync_jp` | Derived by sanitizing and lowercasing the UC4 Job Plan name. |
| **Schedule (Cron)** | `0 0 * * 0` (Weekly) | Parsed from weekly requirement documentation. |
| **Start Date** | `datetime(2024, 12, 1)` | Derived from the `uc4_object_lastmodified_time` metadata. |
| **Catchup** | `False` | Recommended default to prevent execution backfill loops. |
| **Max Active Runs** | `1` | To replicate standard serial UC4 execution constraints. |
| **Is Paused Upon Creation** | `False` | Source UC4 object was active (`<Active>1</Active>`). |
| **Default Args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Default standard fallback args. |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `start` | `EmptyOperator` | N/A | N/A | 0 | N/A | None | `CaleOn="0"` | No | None | Workflow execution start node. |
| `dw_dwh_vertrag_tarif_sync_start_js` | `PythonOperator` | N/A | N/A | 0 | 5 min | None | `CaleOn="0"` | No | None | Start lock check and status setter. |
| `dw_dwh_vertrag_tarif_sync_ende_js` | `PythonOperator` | N/A | N/A | 0 | 5 min | None | `CaleOn="0"` | No | None | End sync status resetter. |
| `end` | `EmptyOperator` | N/A | N/A | 0 | N/A | None | `CaleOn="0"` | No | None | Workflow execution end node. |

### 5. Task Dependency Map
The execution flow is mapped as a linear dependency chain:

```
start >> dw_dwh_vertrag_tarif_sync_start_js >> dw_dwh_vertrag_tarif_sync_ende_js >> end
```

### 6. Parameter and Variable Mapping
| UC4 Parameter / Object | Value/Source | Airflow Equivalent / Sanitized ID |
| :--- | :--- | :--- |
| `DW.DWH_VERTRAG_TARIF_SYNC_JP` | JOBP Name | `dw_dwh_vertrag_tarif_sync_jp` (DAG ID) |
| `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` | Task Name | `dw_dwh_vertrag_tarif_sync_start_js` (Task ID) |
| `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` | Task Name | `dw_dwh_vertrag_tarif_sync_ende_js` (Task ID) |
```

---

## SECTION 6 — SOURCE-TO-TARGET TARGET FILE PLAN & TARGET PYTHON IMPLEMENTATIONS

The components of this job are fully migrated below. The Python task files run as native tasks in Cloud Composer to implement the UC4 script logic.

### File 1: `dags/dw_dwh_vertrag_tarif_sync_jp.py`
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Import target python task scripts
from tasks.dw_dwh_vertrag_tarif_sync_start import execute_start_task
from tasks.dw_dwh_vertrag_tarif_sync_ende import execute_end_task

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_vertrag_tarif_sync_jp',
    default_args=default_args,
    description='Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN',
    schedule_interval='0 0 * * 0', # Weekly execution
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Start boundary
    start = EmptyOperator(
        task_id='start'
    )

    # Task 1: Check lock status and register execution run
    start_js = PythonOperator(
        task_id='dw_dwh_vertrag_tarif_sync_start_js',
        python_callable=execute_start_task,
        provide_context=True
    )

    # Task 2: Sync completion, log output, reset status lock
    ende_js = PythonOperator(
        task_id='dw_dwh_vertrag_tarif_sync_ende_js',
        python_callable=execute_end_task,
        provide_context=True
    )

    # End boundary
    end = EmptyOperator(
        task_id='end'
    )

    # ── Dependency Chain ─────────────────────────────────
    start >> start_js >> ende_js >> end
```

### File 2: `dags/tasks/dw_dwh_vertrag_tarif_sync_start.py`
```python
import logging
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

def execute_start_task(**context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    # 1. Path Lookup logic (Merged Include: DW.HOLE_PFAD_VTRG)
    dwh_home = Variable.get("DWH_HOME", default_var="/opt/dwh")
    home = Variable.get("HOME", default_var="/home/airflow")
    pms_home = Variable.get("PMS_HOME", default_var="/opt/pms")
    
    # 2. Start-JS Script Execution Logic
    dwh_job_kennung = 'VERTRAG_TARIF_SYNC'
    lauf_datum = datetime.now().strftime("%Y%m%d")
    
    # Retrieve variable 'SYNC_STATUS' from the variables container DW.VARIABLEN_VTRG (Airflow Variable mapping)
    sync_status = Variable.get("vtrg_sync_status", default_var="FREI")
    
    if sync_status == "GESPERRT":
        # Rule output exact match: PRINT "Vertrags-/Tarifabgleich fuer &LAUF_DATUM ist gesperrt - Abbruch"
        logging.error(f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch")
        raise AirflowFailException(f"Job aborted because sync is locked: {sync_status}")
        
    # Update state variables
    Variable.set("vtrg_sync_status", "LAEUFT")
    Variable.set("vtrg_letzter_lauf", lauf_datum)
    
    # 3. Write Log logic (Merged Include: DW.LESE_LOG_VTRG)
    # Rule output exact match: "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
    logging.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")
```

### File 3: `dags/tasks/dw_dwh_vertrag_tarif_sync_ende.py`
```python
import logging
from airflow.models import Variable

def execute_end_task(**context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    # 1. Path Lookup logic (Merged Include: DW.HOLE_PFAD_VTRG)
    dwh_home = Variable.get("DWH_HOME", default_var="/opt/dwh")
    home = Variable.get("HOME", default_var="/home/airflow")
    pms_home = Variable.get("PMS_HOME", default_var="/opt/pms")

    # 2. Ende-JS Script Execution Logic
    lauf_datum = Variable.get("vtrg_letzter_lauf", default_val="unknown")
    
    # Reset lock status variables
    Variable.set("vtrg_sync_status", "FREI")
    
    # Rule output exact match: "Vertrags-/Tarifabgleich fuer Lauf &LAUF_DATUM erfolgreich beendet"
    logging.info(f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # 3. Write Log logic (Merged Include: DW.LESE_LOG_VTRG)
    # Rule output exact match: "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
    logging.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")
```

---

## 7. Risks & Manual Actions
* **Variable Initialization**: Before triggering this Airflow DAG in production for the first time, administrators must create the metadata variable `vtrg_sync_status` (initialized to `"FREI"`) and environment configurations (`DWH_HOME`, `HOME`, `PMS_HOME`) in the Cloud Composer Airflow UI / CLI. Failure to do so will cause default values to be picked up.