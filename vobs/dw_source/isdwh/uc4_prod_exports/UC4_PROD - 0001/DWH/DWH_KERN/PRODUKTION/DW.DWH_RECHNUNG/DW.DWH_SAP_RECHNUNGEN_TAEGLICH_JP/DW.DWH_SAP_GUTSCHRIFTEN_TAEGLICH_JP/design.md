# MIGRATION DESIGN DOCUMENT: DW.DWH_SAP_GUTSCHRIFTEN_TAEGLICH_JP

This migration design document covers the transition of the UC4 job orchestration components `DW.HOLE_PFAD` and `DW.LESE_LOG` from a legacy Automic/UC4 environment to Google Cloud Composer (Apache Airflow) running on Google Cloud Platform (GCP).

These files are legacy Job Include (`JOBI`) objects. They do not constitute a standalone workload but provide shared environment/date calculations and post-execution error checking.

---

## SECTION 1 — VERBATIM MCP TOOL OUTPUTS

### 1. Verification of MCP Output for `DW.HOLE_PFAD.xml`
```markdown
Based on the provided UC4 XML file content, here is the detailed Design Document and Pseudocode to migrate the workflow into Apache Airflow.

---

### INPUT VALIDATION & CLASSIFICATION
- **Detected File**: 1 XML file
- **Root Tag**: `<JOBI` → UC4 Include (Script/Include Template) object.
- **Object Name**: `DW.HOLE_PFAD`
- **Validation Flag**: This file is a **UC4 Include (JOBI)** script used for setting global variables and path structures within UC4 workflows. It is **not** an `EVNT_TIME` (Time Event), `JOBP` (Job Plan), or `JOBS_UNIX` (Unix Job) file. As flagged in the instructions, a complete standalone workflow cannot be constructed from this object alone. It serves as a metadata/parameter provider (specifically for variable definition and initialization).

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The `DW.HOLE_PFAD` object is a UC4 Include script (`JOBI`) designed to calculate variable paths, activation flags (`AKTIV_*`), and dynamic date variables (such as last month, previous month, and next month in `YYYYMM` formats). It ends by calling an admin job monitor script (`DW.DWH_ADM_JOB_MONITOR_START`). In an Airflow environment, this logic is typically migrated into a shared Python utility module, a DAG-level pre-execution block, or Airflow Variables/XComs rather than a standalone running task.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.HOLE_PFAD` | JOBI (Include) | N/A (Inherits from caller) | Initialises paths, system flags, and month-range date variables. |

### 3. Airflow DAG Properties
Because this is an Include script and not a scheduling DAG or Unix Job, we map its variables to a placeholder DAG configuration where these calculations can be run as a startup Python preparation task.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_hole_pfad_variables` |
| **schedule** | `None` (Ad-hoc / Called by other DAGs) |
| **start_date** | `YYYY-MM-DD` (Placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation**| `False` (Typically active) |
| **default_args** | `{'owner': 'airflow', 'retries': 0}` |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `calculate_variables` | `PythonOperator` | N/A | None | 0 | N/A | None | CaleOn="0" | False | None | Translates the UC4 script logic and date math into Python dictionaries passed via XComs. |

### 5. Task Dependency Map
Since this is an initialization routine:
`start >> calculate_variables >> end`

### 6. Parameter and Variable Mapping
The UC4 script performs variable lookups (`GET_VAR`) and date manipulations. Below is the mapping to Airflow variables and runtime Python logic:

| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_HOME` | `GET_VAR('DW.VARIABLEN', 'DWH_HOME')` | `Variable.get("dwh_home")` |
| `&HOME` | `GET_VAR('DW.VARIABLEN', 'HOME')` | `Variable.get("home")` |
| `&KWS_HOME` | `GET_VAR('DW.VARIABLEN', 'KWS_HOME')` | `Variable.get("kws_home")` |
| `&PMS_HOME` | `GET_VAR('DW.VARIABLEN', 'PMS_HOME')` | `Variable.get("pms_home")` |
| `&ISTNS_HOME` | `GET_VAR('DW.VARIABLEN', 'ISTNS_HOME')` | `Variable.get("istns_home")` |
| `&AKTIV_CARMEN` | `GET_VAR('DW.VARIABLEN', 'AKTIV_CARMEN')` | `Variable.get("aktiv_carmen")` |
| `&AKTIV_CRS` | `GET_VAR('DW.VARIABLEN', 'AKTIV_CRS')` | `Variable.get("aktiv_crs")` |
| `&AKTIV_CTEL` | `GET_VAR('DW.VARIABLEN', 'AKTIV_CTEL')` | `Variable.get("aktiv_ctel")` |
| `&AKTIV_DPPS` | `GET_VAR('DW.VARIABLEN', 'AKTIV_DPPS')` | `Variable.get("aktiv_dpps")` |
| `&AKTIV_KDS` | `GET_VAR('DW.VARIABLEN', 'AKTIV_KDS')` | `Variable.get("aktiv_kds")` |
| `&AKTIV_WUERFEL`| `GET_VAR('DW.VARIABLEN', 'AKTIV_WUERFEL')`| `Variable.get("aktiv_wuerfel")` |
| `&AKTIV_XTRA` | `GET_VAR('DW.VARIABLEN', 'AKTIV_XTRA')` | `Variable.get("aktiv_xtra")` |
| `&AKTUELL_CACHE`| `GET_VAR('DW.VARIABLEN_DWK_KKM', 'AKTUELL_CACHE')`| `Variable.get("aktuell_cache")` |
| `&LASTMONTH_YYYYMM`| Logic derived from current date | Calculated in Python (see Pseudocode) |
| `&PRELASTMONTH_YYYYMM`| Logic derived from current date | Calculated in Python (see Pseudocode) |
| `&NEXTMONTH_YYYYMM`| Logic derived from current date | Calculated in Python (see Pseudocode) |

### 7. Error Handling and Retry Strategy
- Since this is a simple configuration-setting script running locally on the Airflow worker, no complex retries are defined (`retries=0`).
- Any failure in variable extraction will stop subsequent downstream dependent tasks immediately.

### 8. Developer Notes
- **Include Script Pattern**: This is an include script (`JOBI`). In Airflow, instead of executing this as a separate task across multiple DAGs, it is highly recommended to package this date calculation logic into a **Python utility module** (e.g., `plugins/utils/dw_date_utils.py`) that downstream DAGs can import.
- **Date Math Adjustments**: The UC4 date calculations execute relative to `SYS_DATE(YYYYMMDD)`. In Airflow, this should be mapped to the DAG's logical execution date (`context['ds']`) to maintain idempotency during backfills.
- **Job Monitor**: The script includes `:inc DW.DWH_ADM_JOB_MONITOR_START`. In Airflow, this monitoring is natively handled by the Airflow UI, metadata DB, or open-telemetry integrations. If custom database logging is required, map this to an `on_execute_callback`.

---

## SECTION 2 — PSEUDOCODE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

── GCP Configuration ────────────────────────────────────
# No Dataproc tasks are run directly by this include file.
# Placeholders kept for downstream compatibility.
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
REGION = "YOUR_DATAPROC_REGION"

── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 29),
    'retries': 0,
}

── Helper Logic (UC4 Date Script Translation) ───────────
def calculate_uc4_variables_logic(logical_date_str):
    # Convert execution date (logical_date_str is YYYY-MM-DD format)
    exec_date = datetime.strptime(logical_date_str, "%Y-%m-%d")
    
    # :set &LASTMONTH_YYYYMM = SYS_DATE(YYYYMMDD) -> YYYYMM
    # :set &first = '01' -> YYYYMM01
    current_month_first = exec_date.replace(day=1)
    
    # :set &PRELASTMONTH_YYYYMM = SUB_PERIOD("...","MM:2","YYYYMMDD") -> subtracts 2 months
    pre_last_month = current_month_first - relativedelta(months=2)
    pre_last_month_yyyymm = pre_last_month.strftime("%Y%m")
    
    # :set &LASTMONTH_YYYYMM = SUB_DAYS(&LASTMONTH_YYYYMM, 1) -> Subtracts 1 day from 1st of current month
    last_month = current_month_first - relativedelta(days=1)
    last_month_yyyymm = last_month.strftime("%Y%m")
    
    # :set &NEXTMONTH_YYYYMM = ADD_PERIOD("...","MM:1","YYYYMMDD") -> adds 1 month to current date
    next_month = exec_date + relativedelta(months=1)
    next_month_yyyymm = next_month.strftime("%Y%m")
    
    # Retrieve UC4 Variables mapped to Airflow Variables
    variables = {
        "DWH_HOME": Variable.get("dwh_home", default_var="/home/dwh"),
        "HOME": Variable.get("home", default_var="/home"),
        "KWS_HOME": Variable.get("kws_home", default_var=None),
        "PMS_HOME": Variable.get("pms_home", default_var=None),
        "ISTNS_HOME": Variable.get("istns_home", default_var=None),
        "AKTIV_CARMEN": Variable.get("aktiv_carmen", default_var="0"),
        "AKTIV_CRS": Variable.get("aktiv_crs", default_var="0"),
        "AKTIV_CTEL": Variable.get("aktiv_ctel", default_var="0"),
        "AKTIV_DPPS": Variable.get("aktiv_dpps", default_var="0"),
        "AKTIV_KDS": Variable.get("aktiv_kds", default_var="0"),
        "AKTIV_WUERFEL": Variable.get("aktiv_wuerfel", default_var="0"),
        "AKTIV_XTRA": Variable.get("aktiv_xtra", default_var="0"),
        "AKTUELL_CACHE": Variable.get("aktuell_cache", default_var=None),
        "LASTMONTH_YYYYMM": last_month_yyyymm,
        "PRELASTMONTH_YYYYMM": pre_last_month_yyyymm,
        "NEXTMONTH_YYYYMM": next_month_yyyymm
    }
    
    return variables

def run_variable_calculation(**context):
    # Airflow context ds is 'YYYY-MM-DD'
    logical_date = context['ds']
    calculated_vars = calculate_uc4_variables_logic(logical_date)
    
    # Push variables to XCom so downstream tasks can access them dynamically
    for key, val in calculated_vars.items():
        context['ti'].xcom_push(key=key, value=val)
        print(f"Set Variable {key} = {val}")

── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_hole_pfad_variables',
    schedule_interval=None,  # Ad-hoc / Used as utility
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
) as dag:

── Task: calculate_variables ────────────────────────────
    calculate_variables_task = PythonOperator(
        task_id='calculate_variables',
        python_callable=run_variable_calculation,
        provide_context=True,
    )

── Dependencies ─────────────────────────────────────────
    # This utility DAG contains a single initialization task
    calculate_variables_task
```
```

### 2. Verification of MCP Output for `DW.LESE_LOG.xml`
```markdown
### INPUT VALIDATION

* **Validation Status:** **FAILED / INCOMPLETE WORKFLOW**
* **Issue:** Only one XML file was detected in the input, which defines a **Job Include (`JOBI`)** helper script object named `DW.LESE_LOG`. 
* **Missing Critical Components:** 
  * A complete workflow requires at least one **Time Event (`EVNT_TIME`)** or **Schedule (`JSCH`)** file for scheduling.
  * A **Job Plan (`JOBP`)** file containing the task execution graph, task-level constraints, and dependencies.
  * At least one **Unix Job (`JOBS_UNIX`)** file containing the Ab Initio execution scripts and runtime configurations.
* **Resolution Action:** To generate a comprehensive, production-ready Airflow DAG Design Document and Pseudocode, please paste the contents of the remaining XML files (the `EVNT_TIME` / `JSCH`, `JOBP`, and `JOBS_UNIX` files) between the caret markers.

---

### PRELIMINARY ANALYSIS OF THE PROVIDED SINGLE FILE (`DW.LESE_LOG`)

Even though the workflow is incomplete, here is the analysis of the provided `JOBI` (Job Include) object. This helper script contains shared logic for post-processing and error handling.

#### 1. Script Logic & Error Logging
The include script `DW.LESE_LOG` captures the return code of an upstream execution (`RETURN=$?`) and implements conditional alerting:
* If the return code is non-zero (failure):
  * It triggers an external tool: `$HOME/tools/showlog -uc4 &DWH_JOB_KENNUNG` (which prints diagnostic logs for the job ID stored in the variable `&DWH_JOB_KENNUNG`).
  * It outputs error messages to standard output and exits with the original non-zero return code (`exit $RETURN`).
* If the return code is zero (success):
  * It outputs a success confirmation and exits cleanly (`exit 0`).
* It includes an external monitoring termination script at the end: `:INC DW.DWH_ADM_JOB_MONITOR_END`

#### 2. Airflow Migration Strategy (Shared Logic)
When the complete workflow is imported:
* **Log Forwarding:** In Google Cloud Dataproc, standard output and error streams are automatically forwarded to Cloud Logging. Therefore, the native Airflow Task Instance logs will replace the manual `$HOME/tools/showlog` command.
* **Alerting Framework:** The conditional logic `if [ $RETURN -ne 0 ]` should be mapped to Airflow's built-in alert mechanism using an `on_failure_callback` configured on the tasks. This eliminates the need to append bash script footers to every job.
```

---

## SECTION 2 — JOB ENVIRONMENT & CONTEXT ANALYSIS

### 1. Lineage & Unresolved References
The metadata scanning identified cross-file relationships and unmapped legacy scripts:
* **`DW.HOLE_PFAD.xml`** invokes the package **`DW.DWH_ADM_JOB_MONITOR_START.xml`** (job monitor registration).
* **`DW.LESE_LOG.xml`** invokes the package **`DW.DWH_ADM_JOB_MONITOR_END.xml`** (job monitor deregistration).
* **`DW.LESE_LOG.xml`** references an unresolved command invocation `SHOWLOG.KSH` (detailed below).

### 2. Upstream & Downstream Dependencies
Downstream consumers dependent on these shared parameters (none of these target tables/jobs have been migrated yet, representing cross-DAG dependencies to be resolved on Composer):
* `DW.DWH_EXIS_ACL_FOS_BONI`
* `DW.DWH_EXIS_ACL_PRODUKTE`
* `DW.DWH_EXIS_ACL_RECHDEF`
* `DW.DWH_EXIS_ACL_VERTRA_MF`
* `DW.DWH_EXIS_DIL4GCP_D_OPTION`
* `DW.DWH_EXIS_DIL4GCP_D_VRS`
* `DW.DWH_EXIS_DIL4GCP_F_BPR_ABG`
* `DW.DWH_EXIS_DIL4GCP_F_BST_AKTIV_M`
* `DW.DWH_EXIS_DIL4GCP_F_CROP`
* `DW.DWH_EXIS_DIL4GCP_F_D1_TWVV_TN`
* `DW.DWH_EXIS_DIL4GCP_F_DETAIL_RPOS_CARM`
* `DW.DWH_EXIS_DIL4GCP_F_LARGEACCOUNTS`
* `DW.DWH_EXIS_DIL4GCP_F_MMS_GESPRAECHSZIEL`
* `DW.DWH_EXIS_DIL4GCP_F_MORPU_M`
* `DW.DWH_EXIS_DIL4GCP_F_MORPU_VERTRAG`
* `DW.DWH_EXIS_DIL4GCP_F_NNV_TVD`
* `DW.DWH_EXIS_DIL4GCP_F_RPOS_CARM`
* `DW.DWH_EXIS_DIL4GCP_F_RRABATT_CARM`
* `DW.DWH_EXIS_DIL4GCP_F_TDEG`
* `DW.DWH_EXIS_DIL4GCP_F_THOME_PROV_BEW`
* *(And 39 other downstream data processes depending on these date calculations)*

These parameters should be loaded into Airflow configurations or derived dynamically in a global Airflow macro package so that downstream DAGs do not need manual sensors to wait for a shared "path/variable setup" task.

---

## SECTION 3 — TARGET SYSTEM CONFIGURATION & ENVIRONMENTAL CLASSIFICATION

The environment variables calculated by `DW.HOLE_PFAD` map to variables to be managed globally or run-time scoped in Google Cloud Composer.

### 1. Global Variables (Environment-Wide)
These values should be stored in the Cloud Composer Airflow Variables store (`airflow.models.Variable`) or supplied via environment variables to every execution node.

* `GCP_PROJECT`: Google Cloud Platform Project ID
* `GCP_REGION`: Target GCP compute/Composer region
* `DWH_HOME` (Mapped from `GET_VAR('DW.VARIABLEN', 'DWH_HOME')`): Normalised to Airflow Variable `dwh_home`
* `HOME` (Mapped from `GET_VAR('DW.VARIABLEN', 'HOME')`): Normalised to Airflow Variable `home`
* `KWS_HOME`: Normalised to Airflow Variable `kws_home`
* `PMS_HOME`: Normalised to Airflow Variable `pms_home`
* `ISTNS_HOME`: Normalised to Airflow Variable `istns_home`
* `AKTIV_CARMEN`, `AKTIV_CRS`, `AKTIV_CTEL`, `AKTIV_DPPS`, `AKTIV_KDS`, `AKTIV_WUERFEL`, `AKTIV_XTRA` (system activation triggers): Normalized as Airflow Variables with prefix `aktiv_`

### 2. Job-Specific Values & Variables
These are variables dynamically parsed or used only within the context of the running job:
* `LASTMONTH_YYYYMM`: Derived from logical date math at run-time.
* `PRELASTMONTH_YYYYMM`: Derived from logical date math at run-time.
* `NEXTMONTH_YYYYMM`: Derived from logical date math at run-time.
* `DWH_JOB_KENNUNG`: The specific identifier of the running execution task, mapped to Airflow task run metadata (`task_instance_key_str`).

---

## SECTION 4 — RISKS, MANUAL STEPS & MITIGATIONS

1. **SOURCE: NOT FOUND — SHOWLOG.KSH — no candidate**
   * **Risk**: The logging/troubleshooting script `SHOWLOG.KSH` does not exist in the source codebase. 
   * **Mitigation**: Airflow natively handles console output capturing (`stdout`/`stderr`) and streams logs to Cloud Logging (Stackdriver). The call to `SHOWLOG.KSH` is redundant in GCP. The target implementation replaces this logic completely with Airflow task status handlers and native alerts.
2. **SOURCE: NOT FOUND — DW.DWH_ADM_JOB_MONITOR_START — no candidate**
   * **Risk**: The UC4 job monitoring initialization include does not exist.
   * **Mitigation**: Airflow monitors and captures state transitions via its native metadata database. This call can be ignored or mapped to standard Composer logger entries.
3. **SOURCE: NOT FOUND — DW.DWH_ADM_JOB_MONITOR_END — no candidate**
   * **Risk**: The UC4 job monitoring conclusion include does not exist.
   * **Mitigation**: Standard task execution callbacks or SLA parameters should handle DAG reporting/monitoring rather than manual registration.
4. **WIRING TO DOWNSTREAM TARGETS**: Since downstream DAGs (e.g., `DW.DWH_EXIS_ACL_FOS_BONI`) are not yet migrated, their direct dependencies on these variables should be prepared by hosting the calculation module (`dw_date_utils.py`) in the shared Airflow `plugins/` directory, allowing subsequent workloads to import this module without inter-DAG locking.

---

## SECTION 5 — RE-USABLE TARGET BASH/PYTHON COMPONENT (PSEUDOCODE)

To prevent copying code or establishing complex DAG links, the logic within these two include files is consolidated into a single reusable helper utility.

### Reusable Utility: `plugins/utils/dw_job_helper.py`
```python
import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import logging

def get_dwh_variables(logical_date_str: str) -> dict:
    """
    Translates the variable setting logic from DW.HOLE_PFAD.
    Takes the logical run date ('YYYY-MM-DD') and produces all historical, 
    current, and future date boundaries dynamically.
    """
    exec_date = datetime.strptime(logical_date_str, "%Y-%m-%d")
    
    # Calculate previous, pre-previous, and next month values relative to run date
    current_month_first = exec_date.replace(day=1)
    
    pre_last_month = current_month_first - relativedelta(months=2)
    pre_last_month_yyyymm = pre_last_month.strftime("%Y%m")
    
    last_month = current_month_first - relativedelta(days=1)
    last_month_yyyymm = last_month.strftime("%Y%m")
    
    next_month = exec_date + relativedelta(months=1)
    next_month_yyyymm = next_month.strftime("%Y%m")
    
    # Environment config lookup
    variables = {
        "DWH_HOME": Variable.get("dwh_home", default_var="/home/dwh"),
        "HOME": Variable.get("home", default_var="/home"),
        "KWS_HOME": Variable.get("kws_home", default_var=None),
        "PMS_HOME": Variable.get("pms_home", default_var=None),
        "ISTNS_HOME": Variable.get("istns_home", default_var=None),
        "AKTIV_CARMEN": Variable.get("aktiv_carmen", default_var="0"),
        "AKTIV_CRS": Variable.get("aktiv_crs", default_var="0"),
        "AKTIV_CTEL": Variable.get("aktiv_ctel", default_var="0"),
        "AKTIV_DPPS": Variable.get("aktiv_dpps", default_var="0"),
        "AKTIV_KDS": Variable.get("aktiv_kds", default_var="0"),
        "AKTIV_WUERFEL": Variable.get("aktiv_wuerfel", default_var="0"),
        "AKTIV_XTRA": Variable.get("aktiv_xtra", default_var="0"),
        "AKTUELL_CACHE": Variable.get("aktuell_cache", default_var=None),
        "LASTMONTH_YYYYMM": last_month_yyyymm,
        "PRELASTMONTH_YYYYMM": pre_last_month_yyyymm,
        "NEXTMONTH_YYYYMM": next_month_yyyymm
    }
    
    return variables

def handle_job_status(return_code: int, job_name: str) -> None:
    """
    Translates DW.LESE_LOG post-execution assessment logic.
    Maintains exact legacy output formatting during runtime logs.
    """
    logger = logging.getLogger("airflow.task")
    
    if return_code != 0:
        # Replaces the legacy $HOME/tools/showlog invocation via native logging routing
        logger.error(f"Airflow Task Execution failure encountered for job context: {job_name}")
        logger.info("****************************************************************")
        logger.info(f"Rueckgabewert: '{return_code}' (Fehlerfall)***************************")
        logger.info("****************************************************************")
        sys.exit(return_code)
    else:
        logger.info("****************************************************************")
        logger.info(f"Rueckgabewert: '{return_code}' ***************************************")
        logger.info("****************************************************************")
        sys.exit(0)
```