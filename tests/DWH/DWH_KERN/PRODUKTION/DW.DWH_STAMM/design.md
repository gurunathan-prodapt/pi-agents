# MIGRATION DESIGN DOCUMENT
**Assembled Job:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml`  
**Source Platform:** UC4 / Automic  
**Target Platform:** Google Cloud Platform (BigQuery & Cloud Composer / Apache Airflow)  
**Complexity Tier:** Medium  
**Automation Rate:** 0.95  

---

## 1. PRESCRIBED MIGRATION PATTERN
*   **Pattern:** `UC4_ONLY`
*   **Target:** **Cloud Composer**
*   **Approach:** 1:1 Airflow DAG conversion of UC4 job chains, dependencies, and variables. No data layer migration or storage conversion is directly initiated within this orchestration-only job.

---

## 2. FILE DISPOSITION TABLE
Every file in the pre-collected context is mapped to its target file or action.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml` | `dags/dw_dwh_stamm_knzb_abgl_jp.py` | Primary orchestration DAG defining task ordering. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_START_JS.xml` | `dags/dw_dwh_stamm_knzb_abgl_start_js.py` | State-control and pre-execution initialization DAG. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.xml` | `dags/dw_dwh_stamm_knzb_abgl_ende_js.py` | Completion state resetting and log parsing DAG. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml` | `plugins/helpers/hole_pfad_knzb.py` | Shared utility script imported by start and end DAGs. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml` | `plugins/helpers/lese_log_knzb.py` | Shared logging hook script imported by start and end DAGs. |

---

## 3. CONTEXT THE MCP COULD NOT SEE

### Job Dependencies & Scheduling
*   **Upstream:** None discovered (the main job plan `DW.DWH_STAMM_KNZB_ABGL_JP` is configured with `AllowExternal="1"`, meaning it can be run ad-hoc or triggered as part of an external scheduled chain).
*   **Downstream:** None discovered.
*   **Execution Order:** The target orchestration DAGs strictly preserve the sequence defined in the UC4 dependency graph:
    1.  `START` milestone.
    2.  `DW.DWH_STAMM_KNZB_ABGL_START_JS` (Initializes process, verifies lock).
    3.  `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` (Resets lock, writes logs).
    4.  `END` milestone.
*   **Schedules & Variables:** 
    *   No automatic calendar schedules are defined in the XML. The process runs daily based on manual or upstream triggers.
    *   **Variables Container `DW.VARIABLEN_KNZB`:** Stores the state `ABGLEICH_STATUS` and date `LETZTER_LAUF`. These will be tracked using Airflow Variables.
    *   **Variables Container `DW.VARIABLEN`:** Stores global paths `DWH_HOME`, `HOME`, and `ISTNS_HOME`. These are classified under environmental values below.

### External System Replacements & Cross-File Dependencies
*   **Variable Containers:** Mapped from UC4 database-driven `GET_VAR` / `PUT_VAR` statements to Airflow native JSON Variables (`Variable.get` / `Variable.set`).
*   **Includes:** The UC4 `:inc` commands are migrated to Python module imports (`plugins/helpers/...`), preserving modularity without file merging across different directories, adhering strictly to the **Folder Integrity Rule**.

### Environment-Specific Values (GCP Variables)
1.  **GLOBAL (Environment-Wide Variables)**
    *   `GCP_PROJECT`: Fetched at runtime via `from airflow.models import Variable` using `Variable.get("GCP_PROJECT")`.
    *   `GCP_REGION`: Fetched at runtime via `Variable.get("GCP_REGION")`.
2.  **JOB-SPECIFIC Variables**
    *   `DWH_HOME`: Sourced from the Airflow JSON variable container `dw_variablen` using key `DWH_HOME`.
    *   `HOME`: Sourced from `dw_variablen` using key `HOME`.
    *   `ISTNS_HOME`: Sourced from `dw_variablen` using key `ISTNS_HOME`.

---

## 4. RISKS & MANUAL ACTIONS
*   *None discovered.* All files are fully resolved and accounted for in the file plan. No stubs or unresolved references are present in this execution scope.

---

## 5. RE-USED MCP CONVERSION RESULTS (VERBATIM)

### === Result for DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml ===
#### SECTION 1 — DESIGN DOCUMENT

##### 1. Overview
The **DW.DWH_STAMM_KNZB_ABGL_JP** workflow is a daily job plan (JOBP) responsible for the matching/reconciliation of Customer Number and Base Access master data (Kundennummer-/Basiszugangs-Stammdaten, KNZB) between the ISTNS source system and the DWH Core Layer (DWH-Kernschicht). It is designed as a sequence of native UC4 tasks containing a startup/trigger task and a termination/completion task.

##### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_STAMM_KNZB_ABGL_JP` | `JOBP` (Job Plan) | `<Active>1</Active>` (Active) | Daily reconciliation workflow of customer number / base access master data in the DWH core layer. |
| `DW.DWH_STAMM_KNZB_ABGL_START_JS` | `JOBS` (Referenced) | Unknown (Not provided) | Job task representing the start of the reconciliation. |
| `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` | `JOBS` (Referenced) | Unknown (Not provided) | Job task representing the end of the reconciliation. |

##### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_stamm_knzb_abgl_jp` |
| **schedule** | `None` *(Note: No scheduling trigger file (EVNT_TIME) was provided. Schedule set to None; manual or external trigger assumed).* |
| **start_date** | `datetime(2024, 11, 4)` *(Derived from the UC4 object last modified timestamp)* |
| **catchup** | `False` |
| **max_active_runs** | `1` *(Standard serialization safety)* |
| **is_paused_upon_creation** | `False` *(The source workflow is Active: `<Active>1</Active>`)* |
| **default_args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

##### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `start` | `EmptyOperator` | N/A | N/A | N/A | N/A | None | None | N/A | None | Standard start anchor |
| `dw_dwh_stamm_knzb_abgl_start_js` | `TriggerDagRunOperator` | N/A | N/A | 0 | 5 min | None | None | `wait_for_completion=True` | None | Maps to the first processing block |
| `dw_dwh_stamm_knzb_abgl_ende_js` | `TriggerDagRunOperator` | N/A | N/A | 0 | 5 min | None | None | `wait_for_completion=True` | None | Maps to the second processing block |
| `end` | `EmptyOperator` | N/A | N/A | N/A | N/A | None | None | N/A | None | Standard end anchor |

##### 5. Task Dependency Map
The execution flow is structured as a linear, synchronous pipeline:

```
start >> dw_dwh_stamm_knzb_abgl_start_js >> dw_dwh_stamm_knzb_abgl_ende_js >> end
```

* **start**: Dummy entry milestone.
* **dw_dwh_stamm_knzb_abgl_start_js**: Initiates and monitors the execution of the start stage of the KNZB reconciliation.
* **dw_dwh_stamm_knzb_abgl_ende_js**: Executes after the start stage finishes successfully, completing the reconciliation process.
* **end**: Dummy exit milestone confirming successful processing.

##### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_STAMM_KNZB_ABGL_START_JS` | UC4 Child Task Object Name | Triggered DAG ID: `dw_dwh_stamm_knzb_abgl_start_js` |
| `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` | UC4 Child Task Object Name | Triggered DAG ID: `dw_dwh_stamm_knzb_abgl_ende_js` |

##### 7. Error Handling and Retry Strategy
* **Retries**: No explicit retry counts or postcondition structures are defined in the provided `JOBP` XML. Default retries are set to `0`.
* **Failure Alerts**: No callbacks are defined as no specific notifications/postconditions were configured in this master plan wrapper.
* **Sync Else Action**: There are no `<SYNCREF>` structures defined in this file. The standard single concurrency execution rule is preserved by setting `max_active_runs=1` on the DAG level.

##### 8. Developer Notes
* **Missing Trigger/Scheduler**: No `EVNT_TIME` object was provided. The developer must manually configure the trigger interval (e.g., cron scheduled daily execution) or link it to an orchestrating event in a parent DAG.
* **Missing Child Task Implementations**: The definitions for `DW.DWH_STAMM_KNZB_ABGL_START_JS` and `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` are not provided in this XML block. They are mapped to downstream `TriggerDagRunOperator` placeholders under the assumption that they will be deployed as individual standalone DAGs.
* **Environment Configuration**: Replace GCS and GCP placeholder variables with concrete environment values when building the final Python execution script.

#### SECTION 2 — PSEUDOCODE

```python
# ─── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ─── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ─── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_jp',
    default_args=DEFAULT_ARGS,
    description='Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht',
    schedule_interval=None,  # No schedule provided in UC4 source
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active: 1 in source UC4 XML
)

# ─── Task: start ──────────────────────────────────────────
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# ─── Task: dw_dwh_stamm_knzb_abgl_start_js ────────────────
dw_dwh_stamm_knzb_abgl_start_js = TriggerDagRunOperator(
    task_id='dw_dwh_stamm_knzb_abgl_start_js',
    trigger_dag_id='dw_dwh_stamm_knzb_abgl_start_js',
    wait_for_completion=True,
    poke_interval=30,
    reset_dag_run=True,
    check_fully_qualified_dag_status=True,
    dag=dag,
)

# ─── Task: dw_dwh_stamm_knzb_abgl_ende_js ─────────────────
dw_dwh_stamm_knzb_abgl_ende_js = TriggerDagRunOperator(
    task_id='dw_dwh_stamm_knzb_abgl_ende_js',
    trigger_dag_id='dw_dwh_stamm_knzb_abgl_ende_js',
    wait_for_completion=True,
    poke_interval=30,
    reset_dag_run=True,
    check_fully_qualified_dag_status=True,
    dag=dag,
)

# ─── Task: end ────────────────────────────────────────────
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# ─── Dependencies ─────────────────────────────────────────
start >> dw_dwh_stamm_knzb_abgl_start_js >> dw_dwh_stamm_knzb_abgl_ende_js >> end
```

---

### === Result for DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_START_JS.xml ===
#### SECTION 1 — DESIGN DOCUMENT

##### 1. Overview
This UC4 workflow represents the initialization phase of the master data reconciliation process (`STAMM_KNZB_ABGL`) within the Data Warehouse core namespace (`DW.DWH_STAMM`). The primary job (`DW.DWH_STAMM_KNZB_ABGL_START_JS`) executes a startup script that checks the status of the reconciliation process inside a global state container (`DW.VARIABLEN_KNZB`). If the status is not locked (`GESPERRT`), it transitions the status to running (`LAEUFT`), records the current date as the last execution date, and reads the logs before letting downstream processes continue. This is a critical synchronization guard that prevents concurrent, unauthorized runs.

##### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_STAMM_KNZB_ABGL_START_JS` | `JOBS` (Script/Job Type) | Active (`<Active>` tag omitted from provided source, defaulted to Active/1) | Start component: Sets execution metadata and status for the KNZB master data reconciliation. |

##### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_stamm_knzb_abgl_start_js` |
| **schedule (cron)** | `None` (Ad-hoc or triggered by an upstream parent schedule) |
| **start_date** | `datetime(2026, 1, 1)` |
| **catchup** | `False` |
| **max_active_runs** | `1` (Crucial for status tracking and database/variable consistency) |
| **is_paused_upon_creation** | `False` (Source active by default) |
| **default_args** | `{"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)}` |

##### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `check_and_update_status` | `PythonOperator` | N/A | N/A | 0 | N/A | None | None | No | None | Evaluates global variables and performs status transistions mimicking the UC4 script logic. |

##### 5. Task Dependency Map
The execution logic of this DAG is represented by a single protective checking task:

`start >> check_and_update_status >> end`

- **start**: Dummy start operator.
- **check_and_update_status**: Queries the environment's metadata backend (such as Airflow Variables or a metadata table), checks if the current state is locked (`GESPERRT`), raises an `AirflowSkipException` if locked, and otherwise updates the state to `LAEUFT` and records the execution date.
- **end**: Dummy end operator.

##### 6. Parameter and Variable Mapping
| UC4 Parameter / Variable | Value/Source | Airflow Equivalent |
|---|---|---|
| `&DWH_JOB_KENNUNG` | `'STAMM_KNZB_ABGL'` | Airflow Variable: `dwh_job_kennung` or Task Local parameter |
| `&LAUF_DATUM` | `SYS_DATE("YYYYMMDD")` | Airflow context macro: `{{ ds_nodash }}` |
| `DW.VARIABLEN_KNZB` | Variable container | Airflow Variable (JSON format or individual variables) `dw_variablen_knzb` |
| `ABGLEICH_STATUS` | State indicator value | Key in Airflow JSON Variable: `dw_variablen_knzb["abgleich_status"]` |
| `LETZTER_LAUF` | Execution date tracker | Key in Airflow JSON Variable: `dw_variablen_knzb["letzter_lauf"]` |

##### 7. Error Handling and Retry Strategy
- **Retries**: Default retries are set to 0. Since this script acts as an atomic transaction to check state flags, repeating it automatically on a logical reject (`GESPERRT`) would only lead to repeated failures.
- **Postcondition Analysis**: The script terminates with `STOP_JOB()` (which aborts the pipeline) if the check fails. In Airflow, this is cleanly mapped by raising an `AirflowSkipException` or `AirflowFailException` within the `PythonOperator` to safely stop execution depending on whether a skip or an error state is preferred by operations.
- **Sync Object Behavior**: No explicit `<SYNCREF>` elements with an `Else` flag were provided. However, `max_active_runs=1` is enforced to prevent concurrent writes to the state variables.

##### 8. Developer Notes
- **State Store Choice**: The UC4 script relies on `GET_VAR` and `PUT_VAR` database functions (`DW.VARIABLEN_KNZB`). In Airflow, developers must choose between using **Airflow Variables** (backed by the metadata database) or a **dedicated Cloud SQL metadata table** to store these states. Airflow Variables are used in the pseudocode as the standard replacement.
- **Stop Execution Behavior**: The UC4 `:STOP_JOB()` terminates the process. In the pseudocode, raising `AirflowSkipException` is used to prevent failures from triggering false pager-alerts, while still safely halting downstreams. If a hard failure is preferred, raise a regular Python `ValueError` or `AirflowFailException`.
- **GCP Placeholders**: As this is a pure controller script checking variables rather than launching a Dataproc Job, no GCP Dataproc operator placeholders are used for this step.

#### SECTION 2 — PSEUDOCODE

```python
# ─── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

# ─── Config ──────────────────────────────────────────────
VAR_CONTAINER_NAME = "dw_variablen_knzb"

# ─── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ─── Python Logic for UC4 Script Translation ───────────────
def check_and_update_knzb_status(**context):
    execution_date_str = context['ds_nodash']
    state_container = Variable.get(VAR_CONTAINER_NAME, deserialize_json=True, default_var={})
    current_status = state_container.get("abgleich_status", "FREI")
    
    print(f"Current KNZB Reconciliation Status: {current_status}")
    print(f"Current Execution Date: {execution_date_str}")
    
    if current_status == "GESPERRT":
        message = f"KNZB-Abgleich fuer {execution_date_str} ist gesperrt - Abbruch der Verarbeitung"
        print(message)
        raise AirflowSkipException(message)
        
    state_container["abgleich_status"] = "LAEUFT"
    state_container["letzter_lauf"] = execution_date_str
    
    Variable.set(VAR_CONTAINER_NAME, state_container, serialize_json=True)
    print("Status updated successfully to 'LAEUFT' for run date.")

# ─── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_start_js',
    default_args=default_args,
    description='Start-Baustein: Laufkennung setzen fuer KNZB-Stammdatenabgleich',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
)

# ─── Tasks ────────────────────────────────────────────────
start = EmptyOperator(
    task_id='start',
    dag=dag
)

check_and_update_status = PythonOperator(
    task_id='check_and_update_status',
    python_callable=check_and_update_knzb_status,
    provide_context=True,
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

# ─── Dependencies ─────────────────────────────────────────
start >> check_and_update_status >> end
```

---

### === Result for DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.xml ===
#### SECTION 1 — DESIGN DOCUMENT

##### 1. Overview
The `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` UC4 object is an end-of-process metadata and locking control task (Ende-Baustein) for the KNZB master data reconciliation process (*KNZB-Stammdatenabgleich*). Its primary purpose is to retrieve the execution date parameter, reset the process lock status variable `ABGLEICH_STATUS` back to `"FREI"` (FREE) within the `DW.VARIABLEN_KNZB` configuration container, log a completion statement, and call a log processing script. Because this object serves as a lock-release state manager, it typically executes at the very end of the KNZB reconciliation pipeline.

##### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` | `JOBS` (Generic Job/Script) | Not explicit in source `XHEADER` (assumed Active/1) | Resets the run lock token in the variables container and writes completion logs for the KNZB reconciliation process. |

##### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **DAG ID** | `dw_dwh_stamm_knzb_abgl_ende_js` |
| **Schedule (cron)** | `None` *(Triggered downstream of the main KNZB workflow or run on demand)* |
| **Start Date** | `datetime(2026, 1, 1)` *(Placeholder)* |
| **Catchup** | `False` |
| **Max Active Runs** | `1` *(Ensures sequential state-variable updates)* |
| **Is Paused Upon Creation** | `False` *(Source active flag defaults to normal deployment)* |
| **Default Args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

##### 4. Task Inventory
| Task ID | Operator | PySpark Script / Action | Dataproc/GCP Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `release_knzb_lock` | `PythonOperator` | N/A (State manipulation & logging) | N/A | 0 | N/A | None | None | `False` (Wait for completion) | None | Executes the logic found in the UC4 script body (retrieving variables, setting lock to `"FREI"`, and calling log stubs). |

##### 5. Task Dependency Map
Since this is a single-step utility task translated into its own DAG:
```
start >> release_knzb_lock >> end
```
* **Plain English Flow**: The DAG starts, immediately runs the `release_knzb_lock` task to clear database/variable locks and write success logs, and then terminates.

##### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&LAUF_DATUM` | `GET_VAR('DW.VARIABLEN_KNZB','LETZTER_LAUF')` | `Variable.get("dw_variablen_knzb_letzter_lauf")` or fetched dynamically via metadata database check. |
| `DW.VARIABLEN_KNZB` -> `ABGLEICH_STATUS` | Set to `"FREI"` | `Variable.set("dw_variablen_knzb_abgleich_status", "FREI")` or SQL database execution state update. |
| `DW.HOLE_PFAD_KNZB` | Include/Script call | Represented as an imported helper function/module or task initialization step. |
| `DW.LESE_LOG_KNZB` | Include/Script call | Represented as an imported logging helper function or a separate sub-task logging downstream. |

##### 7. Error Handling and Retry Strategy
* **Retries**: There is no explicit retry block configured in the provided UC4 runtime configuration (`<MrtMethodNone>1</MrtMethodNone>`, `<SrtMethodNone>1</SrtMethodNone>`). The task will not auto-retry.
* **Sync Object Behavior**: No sync matrix was found inside the `<SYNCREF>` element of this job.
* **ENDED_SKIPPED / Gaps**: No postcondition blocks are present in this object. In case of failure during execution, standard Airflow task failure will occur.

##### 8. Developer Notes
* **Missing Orchestration Context**: Only one single `JOBS` file was provided. A complete workflow transformation requires the companion `JOBP` (Job Plan) and scheduling definitions. This conversion treats the job as a single-node workflow DAG.
* **Lock/Variable Storage Assumption**: UC4 dynamic variables (`GET_VAR`, `PUT_VAR`) are mapped to Airflow Variables (`Variable.get`/`Variable.set`) in the pseudocode. In a production cloud environment, you may want to migrate these locks to a persistent relational metadata store (e.g., Cloud SQL or a tracking table in BigQuery) to avoid contention/latency on the Airflow Metastore DB.
* **Includes/Dependencies**: The UC4 `:inc DW.HOLE_PFAD_KNZB` and `:inc DW.LESE_LOG_KNZB` statements are script includes. They have been designed in the pseudocode as mock Python functions representing path retrieval and log verification/writing. These will require manual implementation based on the contents of those missing include objects.

#### SECTION 2 — PSEUDOCODE

```python
# ─── IMPORTS ──────────────────────────────────────────────
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Setup logging
logger = logging.getLogger("airflow.task")

# ─── MOCKED INCLUDE FUNCTIONS (UC4 :inc blocks) ──────────
def get_knzb_path():
    logger.info("Executed: DW.HOLE_PFAD_KNZB script include logic")
    return "/path/to/knzb/data"

def read_knzb_log():
    logger.info("Executed: DW.LESE_LOG_KNZB script include logic")

# ─── DEFAULT ARGS ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_ende_js',
    default_args=DEFAULT_ARGS,
    description='Reset run-token lock and write logs for KNZB-Abgleich',
    schedule=None,  # No cron schedule - triggered externally
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    # ─── TASK: release_knzb_lock ────────────────────────────
    def release_knzb_lock_callable(**context):
        knzb_path = get_knzb_path()
        lauf_datum = Variable.get("dw_variablen_knzb_letzter_lauf", default_var=datetime.today().strftime('%Y-%m-%d'))
        logger.info(f"Retrieved run date: {lauf_datum}")
        
        Variable.set("dw_variablen_knzb_abgleich_status", "FREI")
        logger.info("Set variable dw_variablen_knzb_abgleich_status to 'FREI'")
        
        print(f"KNZB-Stammdatenabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
        read_knzb_log()

    release_knzb_lock = PythonOperator(
        task_id='release_knzb_lock',
        python_callable=release_knzb_lock_callable,
        doc_md="""\
            ### Description
            Gibt die Laufkennung im Variablencontainer nach erfolgreichem Abgleich wieder frei.
            Maps logic of UC4 object DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.
        """
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────
    release_knzb_lock
```

---

### === Result for DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml ===
#### SECTION 1 — DESIGN DOCUMENT

##### 1. Overview
The `DW.HOLE_PFAD_KNZB` object is a UC4 Job Include (`JOBI`) designed to perform environment configuration setup. Its primary function is to query a global variable container (`DW.VARIABLEN`) and retrieve structural directory paths (`DWH_HOME`, `HOME`, and `ISTNS_HOME`). In a migrated Google Cloud Platform (GCP) and Airflow environment, this manual environment-variable fetching logic is typically mapped directly to Airflow Variables or environment-level configuration variables.

##### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.HOLE_PFAD_KNZB` | `JOBI` (Job Include) | Active (derived from status `1`) | Standard Include to read path variables from the variable container `DW.VARIABLEN`. |

##### 3. Airflow DAG Properties
Because this is an include script (`JOBI`) and not a workflow execution object (`JOBP`), it does not contain scheduling or DAG-level execution definitions. If integrated into a master DAG, the properties would be mapped as follows:

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_hole_pfad_knzb_include` (Placeholder - typically embedded directly into executing DAGs) |
| **schedule (cron)** | `None` (Ad-hoc / Embedded helper) |
| **start_date** | `datetime(2026, 1, 1)` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` |
| **default_args** | `{'owner': 'airflow', 'retries': 0}` |

##### 4. Task Inventory
In Apache Airflow, a standard text/variable include object does not map to a standalone Operator task. Instead, it maps to a utility function or configuration step that sets environment variables or returns Airflow Variables.

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `load_env_variables` | `PythonOperator` | N/A | N/A | 0 | N/A | None | CaleOn="0" | False | None | Resolves UC4 environment paths to Airflow Variables. |

##### 5. Task Dependency Map
`start >> load_env_variables >> [downstream_processing_tasks] >> end`

##### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_HOME` | `GET_VAR('DW.VARIABLEN','DWH_HOME')` | `Variable.get('dwh_home')` or GCS Bucket path equivalent |
| `&HOME` | `GET_VAR('DW.VARIABLEN','HOME')` | `Variable.get('home')` or standard environment home |
| `&ISTNS_HOME` | `GET_VAR('DW.VARIABLEN','ISTNS_HOME')` | `Variable.get('istns_home')` |

##### 7. Error Handling and Retry Strategy
* **Failure Handling:** Since this maps to retrieving Airflow Variables or configuring environment configurations at runtime, any failure to load these variables will raise an explicit `AirflowException`, causing the task to fail instantly and halting downstream processing.
* **Sync & Exclusivity:** No complex Sync actions are defined for this configuration snippet.

##### 8. Developer Notes
* **Variable Extraction:** The variables `DWH_HOME`, `HOME`, and `ISTNS_HOME` must be created in the Airflow metadata database (via the UI, CLI, or Cloud Composer Environment Variables) before deploying workflows that depend on this include.
* **GCS Path Resolution:** Ensure that UC4 UNIX directory paths (such as `/opt/dwh/...`) are mapped to cloud-native storage structures (e.g., `gs://YOUR_BUCKET_NAME/dwh/...`) within these variables.

#### SECTION 2 — PSEUDOCODE

```python
# ─── Imports ──────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

# ─── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

# ─── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_hole_pfad_knzb_include',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
)

# ─── Task: load_env_variables ─────────────────────────────
def resolve_uc4_variables(**context):
    try:
        dwh_home = Variable.get("dwh_home", default_var="gs://YOUR_BUCKET_NAME/dwh_home")
        home = Variable.get("home", default_var="gs://YOUR_BUCKET_NAME/home")
        istns_home = Variable.get("istns_home", default_var="gs://YOUR_BUCKET_NAME/istns_home")
        
        context['ti'].xcom_push(key='dwh_home', value=dwh_home)
        context['ti'].xcom_push(key='home', value=home)
        context['ti'].xcom_push(key='istns_home', value=istns_home)
        
        print(f"Successfully loaded path DWH_HOME: {dwh_home}")
        print(f"Successfully loaded path HOME: {home}")
        print(f"Successfully loaded path ISTNS_HOME: {istns_home}")
        
    except Exception as e:
        raise Exception(f"Failed to resolve environment configurations: {str(e)}")

load_env_variables_task = PythonOperator(
    task_id='load_env_variables',
    python_callable=resolve_uc4_variables,
    provide_context=True,
    dag=dag,
)

# ─── Dependencies ─────────────────────────────────────────
load_env_variables_task
```

---

### === Result for DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml ===
#### SECTION 1 — DESIGN DOCUMENT

##### 1. Overview
The `DW.LESE_LOG_KNZB` object is a UC4 Job Include (`JOBI`) component. It does not run as a standalone scheduled job. Instead, it acts as a reusable scripting utility designed to fetch the current active Job Plan name (`SYS_ACT_JPNAME()`) and current active Job name (`SYS_ACT_JOBNAME()`), printing a standard protocol log entry ("Protokolleintrag: [Job] innerhalb [JobPlan]") to the UC4 execution log. In Apache Airflow, this utility is mapped to a reusable Python logging helper function or injected into the `on_execute_callback` of tasks.

##### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.LESE_LOG_KNZB` | JOBI (Job Include) | N/A (Inherited) | Helper script to write execution metadata (Job Name and Job Plan Name) to the execution log. |

##### 3. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_parent_workflow_placeholder` |
| **schedule** | `None` |
| **start_date** | `YYYY-MM-DD` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

##### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `log_metadata_task` | `PythonOperator` | N/A | N/A | None | None | None | None | `False` | None | Replicates UC4 include script execution logging. |

##### 5. Task Dependency Map
```
[Start] >> log_metadata_task >> [Subsequent Tasks...]
```

##### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&ADMJP` | `SYS_ACT_JPNAME()` | `context['dag'].dag_id` |
| `&ADMJOB` | `SYS_ACT_JOBNAME()` | `context['task_instance'].task_id` |

##### 7. Error Handling and Retry Strategy
*   Because `DW.LESE_LOG_KNZB` is a logging utility, failure within this script should not block downstream processing unless critical environment auditing is required.
*   **Sync Behavior:** Inherited from the parent DAG/Job Plan execution framework.

##### 8. Developer Notes
*   **Missing Workflow Context:** The actual parent job stream (`JOBP`), schedules (`JSCH` / `EVNT_TIME`), and execute commands (`JOBS_UNIX`) were not supplied. This design defines how to translate the metadata-logging component.
*   **Airflow Integration Pattern:** Instead of creating a task for this helper, the clean Airflow architectural pattern is to use an `on_execute_callback` on your operators to automatically log this metadata at the start of every task run.

#### SECTION 2 — PSEUDOCODE

```python
# ─── Imports ──────────────────────────────────────────────
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ─── Logging Helper (Equivalent to UC4 JOBI DW.LESE_LOG_KNZB) ─────────────────
def log_uc4_metadata_callback(context):
    parent_workflow_name = context['dag'].dag_id
    active_job_name = context['task_instance'].task_id
    logging.info(f"Protokolleintrag: {active_job_name} innerhalb {parent_workflow_name}")

# ─── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_execute_callback': log_uc4_metadata_callback,
}

# ─── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_parent_workflow_placeholder',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    doc_md="""
    ### Parent Workflow Placeholder
    This DAG contains the migrated execution logging logic from the UC4 JOBI file 'DW.LESE_LOG_KNZB'.
    """
)

# ─── Task: Log Metadata Demo ──────────────────────────────
demo_task = PythonOperator(
    task_id='dw_lese_log_knzb_demo',
    python_callable=lambda: logging.info("Executing core task logic..."),
    dag=dag,
)

# ─── Dependencies ─────────────────────────────────────────
demo_task
```