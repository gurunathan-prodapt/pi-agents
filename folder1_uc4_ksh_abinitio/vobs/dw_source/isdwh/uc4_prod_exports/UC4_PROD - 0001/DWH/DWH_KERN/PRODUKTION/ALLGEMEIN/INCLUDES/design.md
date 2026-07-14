# MIGRATION DESIGN DOCUMENT: UC4 PROD INCLUDES

This document details the migration plan for the shared include utility jobs `DW.HOLE_PFAD` and `DW.LESE_LOG` from UC4/Automic to Google Cloud Platform using Cloud Composer (Airflow) and BigQuery.

---

## SECTION 1 — VERBATIM MCP DESIGN OUTPUTS

### 1. DW.HOLE_PFAD.xml Migration Design
The utility includes variables resolution and date calculation logic. It has been analyzed to map environment constants and logical date variables into Airflow tasks and parameters.

```python
# ==============================================================================
# VERBATIM MCP OUTPUT FOR: DW.HOLE_PFAD.xml
# ==============================================================================
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dateutil.relativedelta import relativedelta

── GCP Configuration ────────────────────────────────────
# Shared placeholder configuration
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
BUCKET_NAME = "YOUR_BUCKET_NAME"

── Environment Path & Date Resolver Helper ───────────────
def resolve_hole_pfad_context(**context):
    """
    Translates the original UC4 DW.HOLE_PFAD logic into Airflow task instances.
    Dynamically derives dates relative to the DAG's logical execution date.
    """
    # 1. Fetch Variables equivalent to UC4 GET_VAR
    dwh_variables = {
        "DWH_HOME": Variable.get("dwh_home", default_value="/opt/dwh"),
        "HOME": Variable.get("home", default_value="/home/dwh"),
        "KWS_HOME": Variable.get("kws_home", default_value=""),
        "PMS_HOME": Variable.get("pms_home", default_value=""),
        "ISTNS_HOME": Variable.get("istns_home", default_value=""),
        "AKTIV_CARMEN": Variable.get("aktiv_carmen", default_value="0"),
        "AKTIV_CRS": Variable.get("aktiv_crs", default_value="0"),
        "AKTIV_CTEL": Variable.get("aktiv_ctel", default_value="0"),
        "AKTIV_DPPS": Variable.get("aktiv_dpps", default_value="0"),
        "AKTIV_KDS": Variable.get("aktiv_kds", default_value="0"),
        "AKTIV_WUERFEL": Variable.get("aktiv_wuerfel", default_value="0"),
        "AKTIV_XTRA": Variable.get("aktiv_xtra", default_value="0"),
        "AKTUELL_CACHE": Variable.get("aktuell_cache", default_value=""),
    }
    
    # 2. Replicate UC4 Date arithmetic based on execution date (logical_date)
    exec_date = context['logical_date'] # Safe for backfills, replaces local machine SYS_DATE
    
    # Calculate LASTMONTH_YYYYMM
    first_of_current_month = exec_date.replace(day=1)
    last_day_of_last_month = first_of_current_month - timedelta(days=1)
    last_month_yyyymm = last_day_of_last_month.strftime("%Y%m")
    
    # Calculate PRELASTMONTH_YYYYMM (subtract 2 months from the 1st of current month)
    pre_last_month_date = first_of_current_month - relativedelta(months=2)
    pre_last_month_yyyymm = pre_last_month_date.strftime("%Y%m")
    
    # Calculate NEXTMONTH_YYYYMM (add 1 month to current month)
    next_month_date = exec_date + relativedelta(months=1)
    next_month_yyyymm = next_month_date.strftime("%Y%m")
    
    # Push all derived variables to XCom so downstream operators can access them
    derived_params = {
        "LASTMONTH_YYYYMM": last_month_yyyymm,
        "PRELASTMONTH_YYYYMM": pre_last_month_yyyymm,
        "NEXTMONTH_YYYYMM": next_month_yyyymm,
    }
    
    context['ti'].xcom_push(key='dwh_paths', value=dwh_variables)
    context['ti'].xcom_push(key='dwh_dates', value=derived_params)
    
    # Replicate JOBI Call: inc DW.DWH_ADM_JOB_MONITOR_START
    trigger_job_monitor_start(context)

def trigger_job_monitor_start(context):
    # TODO: Implement organizational monitoring logger/database write here
    print(f"Job Monitor: Registered execution starting for DAG: {context['dag'].dag_id}")

── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_hole_pfad_utility",
    schedule_interval=None,  # Intended to be triggered or used as a template
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    ── Task: resolve_paths_and_dates ────────────────────
    resolve_paths_and_dates_task = PythonOperator(
        task_id="resolve_paths_and_dates",
        python_callable=resolve_hole_pfad_context,
        provide_context=True,
    )

    resolve_paths_and_dates_task
```

### 2. DW.LESE_LOG.xml Migration Design
The `DW.LESE_LOG` utility evaluates shell exit statuses and controls process execution. Below is the mapping pattern for Airflow execution models:

*   **Error Logging:** Airflow automatically registers standard streams (stdout/stderr) from target runtime operators (e.g., Python operators, Dataproc submit operators, Cloud Storage operators) and redirects them directly to Google Cloud Logging.
*   **Job Monitor End hook:** Translates `:INC DW.DWH_ADM_JOB_MONITOR_END` and the return code checking blocks into Airflow success/failure task callbacks.

#### Target Translation Pseudocode for `DW.LESE_LOG`:
```python
# ==============================================================================
# VERBATIM MCP OUTPUT FOR: DW.LESE_LOG.xml
# ==============================================================================
def resolve_lese_log_behavior(context):
    """
    Simulates the conditional monitoring output and logging execution of DW.LESE_LOG
    within an Airflow Callback.
    """
    ti = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = ti.task_id
    
    # Mocking equivalent job identification for tracking
    dwh_job_kennung = f"{dag_id}.{task_id}"
    
    # In Airflow callbacks, success or failure is determined by task state
    if context.get('exception'):
        # Log capturing matching showlog utility command block
        # Literal original-language prints must be exact
        print(f"Executing: $HOME/tools/showlog -uc4 {dwh_job_kennung}")
        print("****************************************************************")
        print(f"Rueckgabewert: '1' (Fehlerfall)***************************")
        print("****************************************************************")
        # Trigger job monitor failure hook
        trigger_job_monitor_end(context, status="FAILED")
    else:
        print("****************************************************************")
        print("Rueckgabewert: '0' ***************************************")
        print("****************************************************************")
        # Trigger job monitor success hook
        trigger_job_monitor_end(context, status="SUCCESS")

def trigger_job_monitor_end(context, status):
    # TODO: Implement organizational monitoring logger/database write here
    print(f"Job Monitor: Registered execution termination for DAG: {context['dag'].dag_id} with state: {status}")
```

---

## SECTION 2 — CORE DATA CONTEXT & RELATIONSHIPS

### 1. Job Dependencies & Downstream Wiring
As a shared collection of include files, these scripts do not run in isolation. On the target environment, they are built as dynamic Python helper libraries or imported functions utilized by downstream DAGs:
*   **DW.BERT_AUSD_BP_TA_TARIFOPTION** — not yet migrated
*   **DW.DWH_ABPZ_KKM_AIL_AGENT** — not yet migrated
*   **DW.DWH_OAIS_EX_PPES_CUBES** — not yet migrated

These downstream processes must call the Airflow equivalent functions of `DW.HOLE_PFAD` and `DW.LESE_LOG` at task boundaries to ensure target parameters and monitoring hooks are executed.

### 2. Lineage Edges
*   `DW.HOLE_PFAD.xml` calls include: `DW.DWH_ADM_JOB_MONITOR_START.xml`
*   `DW.LESE_LOG.xml` calls include: `DW.DWH_ADM_JOB_MONITOR_END.xml`
*   `DW.LESE_LOG.xml` invokes external shell utility: `SHOWLOG.KSH` (Unresolved Component)

---

## SECTION 3 — TARGET ENVIRONMENT SPECIFICS

### 1. Target File Plan
| Target File Path | Target Language | Source File Reference | Purpose |
| :--- | :--- | :--- | :--- |
| `plugins/helpers/dwh_env_resolver.py` | Python 3 | `DW.HOLE_PFAD.xml` | Utility module to compute logical date strings and pull variables. |
| `plugins/helpers/dwh_monitor_callback.py` | Python 3 | `DW.LESE_LOG.xml` | Unified failure/success callback with logging simulation. |

### 2. Environment Variables & Global Configs
All configurations must be extracted dynamically from the environment rather than written as literals:

```python
from airflow.models import Variable
import os

# GLOBAL CONFIGURATIONS
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_REGION = os.environ.get("GCP_REGION")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

# JOB-SPECIFIC PARAMETERS (Sourced from Airflow Variable database)
DWH_HOME = Variable.get("DWH_HOME")
HOME = Variable.get("HOME")
KWS_HOME = Variable.get("KWS_HOME")
PMS_HOME = Variable.get("PMS_HOME")
ISTNS_HOME = Variable.get("ISTNS_HOME")
AKTIV_CARMEN = Variable.get("AKTIV_CARMEN")
AKTIV_CRS = Variable.get("AKTIV_CRS")
AKTIV_CTEL = Variable.get("AKTIV_CTEL")
AKTIV_DPPS = Variable.get("AKTIV_DPPS")
AKTIV_KDS = Variable.get("AKTIV_KDS")
AKTIV_WUERFEL = Variable.get("AKTIV_WUERFEL")
AKTIV_XTRA = Variable.get("AKTIV_XTRA")
AKTUELL_CACHE = Variable.get("AKTUELL_CACHE")
```

---

## SECTION 4 — RISKS, MANUAL STEPS & UNRESOLVED ITEMS

### 1. Risks & Manual Actions
*   SOURCE: NOT FOUND — SHOWLOG.KSH — no candidate
*   **Monitoring Hook Dependency:** Since the referenced job monitor endpoints (`DW.DWH_ADM_JOB_MONITOR_START.xml` and `DW.DWH_ADM_JOB_MONITOR_END.xml`) have not yet been migrated, the target implementation triggers local placeholder print logs. Real logging integration must be wired once the monitoring job DAGs are finalized.
*   **Unmigrated Downstreams:** Downstream DAG dependencies (`DW.BERT_AUSD_BP_TA_TARIFOPTION`, `DW.DWH_ABPZ_KKM_AIL_AGENT`, `DW.DWH_OAIS_EX_PPES_CUBES`) must be integrated to import and leverage these Python helpers.

### 2. Target Implementation Stubs
Since `SHOWLOG.KSH` is an unresolved component, its execution must be bypassed or replaced with a stub function in the target logging framework.

```python
def execute_showlog_stub(job_kennung):
    # SOURCE: NOT FOUND — SHOWLOG.KSH — no candidate
    # TODO: No source found for SHOWLOG.KSH. Logging fallback to standard Airflow print streams.
    print(f"Warning: Legacy showlog execution requested for {job_kennung}, but no source was found. Defaulting to Cloud Logging.")
```