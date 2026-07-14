### SECTION 1 — DESIGN DOCUMENT (VERBATIM MCP OUTPUTS)

#### === Result for local/home/gurunathan_t/test_dataset/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_PST_ANALYZE_JP/DW.DWH_ADM_PRUEFE_AB_INITIO/DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC.xml ===

### 1. Overview
The **DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC** object is a UC4 Job Include script (`JOBI`). It acts as a **gatekeeper/guard mechanism** designed to check the status of an external Ab Initio application before permitting dependent workflows to proceed. It polls a UC4 variable (`DW.ADM_AB_INITIO_VAR`) in a loop every 10 seconds. If the status is `'go'`, it updates the status to `'ACTIVE'` and exits successfully. If the status is `'exit1'`, it terminates with an error code, stopping downstream execution.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` | JOBI (Include Script) | N/A (Inherited) | Polls a status variable to guard Ab Initio execution; loops until status is `'go'` or aborts on `'exit1'`. |

### 3. Airflow DAG Properties
Since this is an include script and not a standalone workflow, the properties below represent the parent DAG context where this guard logic will be integrated.

| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_adm_pruefe_ab_initio_start_inc_guard` |
| **schedule** | `None` (Typically triggered as a guard step within a parent DAG) |
| **start_date** | `datetime(2023, 1, 1)` (Placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Deploy normally, Active flag inherits from parent) |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `ab_initio_gatekeeper` | `PythonOperator` | N/A | N/A | 0 | N/A | None | None | `False` | None | Implements the polling loop using an Airflow Variable check. |

### 5. Task Dependency Map
Because this is a singular gatekeeper script, its position in a parent pipeline is:
`start >> ab_initio_gatekeeper >> dependent_pyspark_jobs >> end`

*   **ab_initio_gatekeeper:** Continually polls the state variable. It must succeed before any dependent PySpark jobs are triggered.

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&VAR` ("DW.ADM_AB_INITIO_VAR") | Variable Name | Airflow Variable: `dw_adm_ab_initio_var` (JSON-backed dict) |
| `&APPLIKATION` ("DWH") | Application Name | Key in Airflow Variable JSON: `"status_dwh"` |
| `&WAIT` ('10') | Polling Interval | Loop sleep delay: `10` seconds |
| `&EXIT_CODE` ('exit1') | Abort state | Explicit check raising `AirflowException` |

### 7. Error Handling and Retry Strategy
*   **Loop Abort State:** If the polled status key returns `"exit1"`, the task will explicitly raise an `AirflowException` causing the task to fail immediately.
*   **Infinite Loop Prevention:** In Airflow, we enforce a maximum execution timeout (e.g., 1 hour) on this guard task to prevent infinite resource consumption if the status never changes to `"go"`.

### 8. Developer Notes
*   **Variable Setup:** The developer must instantiate an Airflow Variable named `dw_adm_ab_initio_var` with the following JSON schema prior to execution:
    ```json
    {
      "status_dwh": "wait",
      "DW.DWH_ADM_PST_ANALYZE_JP -> DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC": "WAIT for Ab Initio"
    }
    ```
*   **Timeout Assumption:** An explicit execution timeout of 3600 seconds is added to the task configuration to ensure the loop does not run indefinitely.

---

## SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# ── GCP Configuration ────────────────────────────────────
# Placeholder configurations for parent context if needed
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
REGION = "YOUR_DATAPROC_REGION"
CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'dwh_admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'catchup': False
}

# ── Guard Logic (Equivalent to UC4 Script Body) ──────────
def poll_ab_initio_status(**context):
    """
    Implements the polling loop from the JOBI script:
    - Reads Airflow Variable 'dw_adm_ab_initio_var'
    - Wait interval: 10 seconds
    - Success condition: status_dwh == 'go'
    - Abort condition: status_dwh == 'exit1'
    """
    var_name = "dw_adm_ab_initio_var"
    app_key = "status_dwh"
    exit_code = "exit1"
    wait_interval = 10
    
    # Get current runtime context info to match UC4 logging
    jobplan_name = context['dag'].dag_id
    job_name = context['task'].task_id
    betr_job = f"{jobplan_name} -> {job_name}"
    
    # Set initial checking status in Airflow Variable
    current_time = datetime.now().strftime('%H:%M:%S')
    current_date = datetime.now().strftime('%d.%m.%Y')
    
    state_dict = Variable.get(var_name, deserialize_json=True, default_var={})
    state_dict[betr_job] = f"WAIT for Ab Initio ({current_time} {current_date})"
    Variable.set(var_name, state_dict, serialize_json=True)
    
    print(f"Der Pruefung laeuft fuer {job_name} im Jobplan {jobplan_name}")
    
    while True:
        # Fetch status dynamically during each iteration
        state_dict = Variable.get(var_name, deserialize_json=True, default_var={})
        status_appl = state_dict.get(app_key, "wait")
        current_time = datetime.now().strftime('%H:%M:%S')
        
        print(f"Der Status fuer die Applikation DWH ist: {status_appl} ({current_time})")
        
        if status_appl == "go":
            print(f"Pruefung erfolgreich, starte Ab Initio Job(s) ({current_time})")
            state_dict[betr_job] = f"ACTIVE in Ab Initio {current_time} {current_date}"
            Variable.set(var_name, state_dict, serialize_json=True)
            break
            
        elif status_appl == exit_code:
            print(f"Der Status fuer den Pruefjob wurde auf {exit_code} gesetzt, beende Pruefjob ({current_time})")
            state_dict[betr_job] = f"Pruefjob wurde abgebrochen {current_time}"
            Variable.set(var_name, state_dict, serialize_json=True)
            raise AirflowException("Ab Initio check returned failure state (exit1). Execution halted.")
            
        print(f"PRUEFE ... ({current_time})")
        time.sleep(wait_interval)

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_adm_pruefe_ab_initio_start_inc_guard',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    is_paused_upon_creation=False,
    catchup=False
) as dag:

# ── Task: ab_initio_gatekeeper ────────────────────────────
    ab_initio_gatekeeper = PythonOperator(
        task_id='ab_initio_gatekeeper',
        python_callable=poll_ab_initio_status,
        execution_timeout=timedelta(hours=1) # Prevent infinite looping bills
    )

# ── Dependencies ─────────────────────────────────────────
    # This gatekeeper stands alone or as the entrypoint task of a larger pipeline:
    ab_initio_gatekeeper
```

---

#### === Result for local/home/gurunathan_t/test_dataset/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_PST_ANALYZE_JP/DW.DWH_ADM_PRUEFE_AB_INITIO/DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC.xml ===

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` object is a UC4 Include Script (`JOBI`). In UC4, Include Scripts are reusable blocks of code embedded within job definitions (`JOBS`). This specific script acts as a state-reporting and auditing step. When called, it reads a tracking variable (`DW.ADM_AB_INITIO_VAR`), prints log lines indicating that the Ab Initio batch processing has finished for the `"DWH"` application, and writes a timestamped success status (`fertig (HH:MM:SS DD.MM.YYYY)`) back to the UC4 Variable object (`VARA`). 

In Apache Airflow, this tracking pattern translates to updating an Airflow Variable, writing an audit record to a metadata database, or pushing an execution status to XCom/logging frameworks.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` | `JOBI` (Include Script) | N/A (Inherited from Parent Job) | Helper script block that logs completion and updates the state variable `DW.ADM_AB_INITIO_VAR` to indicate Ab Initio processing has completed. |

## 3. Airflow DAG Properties
Since a `JOBI` object is a script fragment and not a workflow, it does not define a DAG. However, when integrated into a parent Airflow DAG, the following properties represent the metadata environment:

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_adm_pruefe_ab_initio_ende_inc` *(derived if compiled as an independent helper DAG)* |
| **schedule** | `None` (Runs only when triggered or included by parent task execution) |
| **start_date** | `datetime(2023, 6, 10)` *(Placeholder based on export timestamp)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation**| `False` *(No XHEADER Active tag present in JOBI root; defaults to active)* |

## 4. Task Inventory
When mapped to Airflow, this helper script is implemented as a Python task that interacts with the Airflow Variable store.

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `update_ab_initio_status` | `PythonOperator` | None (Direct Python execution) | N/A | 0 | N/A | None | None (`CaleOn="0"`) | `wait_for_completion=True` | None | Updates Airflow Variable `dw_adm_ab_initio_var` with completion status. |

## 5. Task Dependency Map
Because this is an include script, it executes inline at the end of its parent job. If mapped as an explicit task step within a migrated parent DAG:

`[Upstream_Ab_Initio_Tasks] >> update_ab_initio_status >> [Downstream_Tasks]`

- **Upstream Ab Initio Tasks**: The processing tasks that must complete before state logging.
- **update_ab_initio_status**: The Python task executing the logic of this JOBI object.
- **Downstream Tasks**: Subsequent tasks that rely on the variable update being finalized.

## 6. Parameter and Variable Mapping
The UC4 script performs variable substitutions and reads/updates a global variable object. This maps to Airflow Variables and execution context macros as follows:

| UC4 Parameter / Variable | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&APPLIKATION` | `"DWH"` | Local Python Variable / Task Parameter |
| `&Var` | `"DW.ADM_AB_INITIO_VAR"` | Airflow Variable: `dw_adm_ab_initio_var` |
| `&TIME` | `SYS_TIME('HH:MM:SS')` | Jinja macro: `{{ utcnow().strftime('%H:%M:%S') }}` |
| `&DATE` | `SYS_DATE(DD.MM.YYYY)` | Jinja macro: `{{ utcnow().strftime('%d.%m.%Y') }}` |
| `&JOBPLANNAME` | `SYS_ACT_PARENT_NAME()` | Jinja macro: `{{ dag.dag_id }}` |
| `&JOBNAME` | `SYS_ACT_ME_NAME` | Jinja macro: `{{ task.task_id }}` |
| `&BETRJOB` | `&JOBPLANNAME -> &JOBNAME` | `f"{dag_id} -> {task_id}"` |
| `&STATUS_APPL` | `GET_VAR(&VAR, 'STATUS_DWH')` | `Variable.get("dw_adm_ab_initio_var", default_val={}).get("STATUS_DWH")` |
| `&STATUS_FERTIG` | `'fertig (&TIME &DATE)'` | `f"fertig ({current_time} {current_date})"` |
| `PUT_VAR` | Writes `&STATUS_FERTIG` to `&VAR` under key `&BETRJOB` | `Variable.set` updating the JSON dictionary key representing the job path. |

## 7. Error Handling and Retry Strategy
- **Failure Mapping**: If the Python task fails to update the Airflow Variable, it should raise an exception and fail. No retries are defined inside this script block.
- **Sync Object / Concurrency**: The source variable acts as a critical section. To prevent concurrent tasks from overwriting the variable concurrently, the DAG should run with `max_active_runs=1` or use Airflow's built-in transaction-safe Variable updates.

## 8. Developer Notes
* **Missing Parent Context**: Since only a single `JOBI` (Include Script) file was provided, a complete scheduling sequence, cluster configuration, and target execution host are missing.
* **Airflow Variable Handling**: Airflow Variables are stored in the metadata database as JSON strings. Writing key-value pairs (like UC4's `PUT_VAR`) should be implemented by reading the variable as a JSON object, updating/adding the key (`&BETRJOB`), and write the updated JSON object back to ensure other keys in the same variable are not wiped out.
* **No TriggerRule.ALL_DONE**: Default `TriggerRule.ALL_SUCCESS` is kept.

---

# SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ── GCP Configuration ────────────────────────────────────
# Note: Placeholders generated for compatibility when integrated into a parent DAG
YOUR_GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
YOUR_DATAPROC_REGION = "YOUR_DATAPROC_REGION"
YOUR_DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
YOUR_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'dwh_admin',
    'start_date': datetime(2023, 6, 10),
    'retries': 0,
}

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_adm_pruefe_ab_initio_ende_inc',
    schedule=None,  # This is a helper/include process, no independent schedule
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    description='Helper process to update Ab Initio execution state'
)

# ── Task: update_ab_initio_status ────────────────────────
def update_ab_initio_status_fn(**context):
    # 1. Resolve runtime context parameters (equivalent to UC4 system functions)
    applikation = "DWH"
    var_name = "dw_adm_ab_initio_var"
    
    now = datetime.utcnow()
    time_str = now.strftime('%H:%M:%S')
    date_str = now.strftime('%d.%m.%Y')
    
    jobplan_name = context['dag'].dag_id
    job_name = context['task'].task_id
    betr_job = f"{jobplan_name} -> {job_name}"
    
    # 2. Fetch the current state from Airflow Variables (GET_VAR equivalent)
    # Variable is assumed to be stored as a JSON dictionary to hold multiple key-value pairs
    state_dict = Variable.get(var_name, deserialize_json=True, default_var={})
    status_appl = state_dict.get(f"STATUS_{applikation}", "UNKNOWN")
    
    status_fertig = f"fertig ({time_str} {date_str})"
    
    # 3. Log actions matching UC4 PRINT statements
    logging.info(f"Der Prüfjob {job_name} läuft im Jobplan {jobplan_name}")
    logging.info(f"Der Status für die Applikation {applikation} ist: {status_appl} ({time_str} {date_str})")
    logging.info(f"Die Ab Initio Verarbeitung ist fertig. Der Status wird auf {status_fertig} umgesetzt.")
    
    # 4. Update and persist state variable (PUT_VAR equivalent)
    state_dict[betr_job] = status_fertig
    Variable.set(var_name, state_dict, serialize_json=True)

update_ab_initio_status = PythonOperator(
    task_id='update_ab_initio_status',
    python_callable=update_ab_initio_status_fn,
    provide_context=True,
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
# This task executes standalone within this helper DAG wrapper
update_ab_initio_status
```

---

### SECTION 2 — ADDITIONAL CONTEXT & WORKFLOW INTEGRATION

The following sections supply the architecture details, environmental variables, target structures, and mapping choices that the standalone MCP design tool could not see.

#### 1. Lineage & Workflow Structure
* **Upstream Status Producers:** An external Ab Initio orchestration environment must update the key `status_dwh` inside the Airflow Variable `dw_adm_ab_initio_var`.
* **Execution Order:** 
  1. The parent orchestrator triggers.
  2. `ab_initio_gatekeeper` task runs, polling the `status_dwh` state in `dw_adm_ab_initio_var`.
  3. Once `status_dwh == 'go'`, downstream incremental DWH PySpark / BigQuery pipeline tasks are triggered.
  4. Upon successful run of the actual Ab Initio pipeline processing tasks, `update_ab_initio_status` executes to flag processing as complete by updating the state.

#### 2. External System Replacements
* **UC4 Variable Object (`VARA`)** $\rightarrow$ **Airflow Variable (`Variable`)** stored in Cloud Composer’s metadata DB as a JSON object: `dw_adm_ab_initio_var`.
* **UC4 Include Script (`JOBI`)** $\rightarrow$ Reusable Python execution functions or utility tasks packaged within custom Airflow operators or imported as Python module handlers inside Cloud Composer.

#### 3. Cross-File Dependencies
These two files depend on the shared state of the `"DW.ADM_AB_INITIO_VAR"` variable. Because they operate on a shared variable, the operations use transactional get-and-set updates of the Airflow Variable to prevent race conditions during parallel DAG runs.

#### 4. Target File Plan
These components should be migrated to the following file layout in Cloud Composer:

| Target File Path | Target Type | Language | Source File | Description |
|---|---|---|---|---|
| `dags/utils/ab_initio_start_guard.py` | Importable utility task | Python | `DW_DWH_ADM_PRUEFE_AB_INITIO_START_INC.xml` | Implements the polling loop task. |
| `dags/utils/ab_initio_end_guard.py` | Importable utility task | Python | `DW_DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC.xml` | Implements the completion status updater. |

#### 5. Environment-Specific Values
* **Airflow Variable Key:** `dw_adm_ab_initio_var`
* **Composer / GCP Location:** Cloud Composer Environment in target VPC.
* **Scheduling:** These tasks do not have standalone schedules; they are imported and orchestrated directly within the main DWH workflow DAGs.

#### 6. Risks & Manual Actions
* **High Poll Frequency Cost:** Polling every 10 seconds against the Airflow Variable store creates constant database reads. It is highly recommended to increase the polling wait interval (`&WAIT`) to 60 or 120 seconds in production.
* **Variable Cleanliness:** The external trigger (Ab Initio or orchestrator) must reset `status_dwh` back to its initial wait value before launching the next batch cycle, otherwise the gatekeeper will automatically pass on the next run.