An implementation-ready **MIGRATION DESIGN DOCUMENT** has been generated for the `DW.DWH_ADM_JOB_MONITOR` job. 

This job consists of two UC4 JOBI (Include) scripts: **`DW.DWH_ADM_JOB_MONITOR_START`** and **`DW.DWH_ADM_JOB_MONITOR_END`**. In the source system, these act as global preprocessing and postprocessing hook scripts to register job starts/ends and check active monitoring variables. On BigQuery / Cloud Composer, these are migrated into reusable Python tasks or modular operators executing within your pipeline orchestration.

---

### VERBATIM MCP CONVERSION DESIGNS

#### 1. START SCRIPT: `DW.DWH_ADM_JOB_MONITOR_START.xml`
```markdown
=== Result for local/home/pranav_b/migration_analyzer/test_folders/folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_JOB_MONITOR/DW.DWH_ADM_JOB_MONITOR_START.xml ===
### INPUT VALIDATION & CLASSIFICATION

A single XML file has been provided in the input:
* **File 1 Root Tag:** `<uc-export>` containing `<JOBI name="DW.DWH_ADM_JOB_MONITOR_START">`
* **UC4 Object Type:** Include/Script (JOBI)
* **UC4 Object Name:** `DW.DWH_ADM_JOB_MONITOR_START`

#### CRITICAL FLAG & VALIDATION WARNING
This export contains only **one single JOBI (Include Script) file** instead of a complete workflow structure. 
* As stated in the validation rules, a complete executable Airflow pipeline representation requires a workflow definition structure—typically comprising at least one `EVNT_TIME` (Scheduling), one `JOBP` / `JSCH` (Job Plan/Schedule), and one or more `JOBS_UNIX` (Unix Job/Execution) files.
* A JOBI object in UC4 acts as a reusable script block (include) rather than a standalone scheduled job.
* Since you have requested a complete conversion blueprint, the following blueprint treats this JOBI block as a preprocessing metadata/auditing routine. We will model it as an Airflow `PythonOperator` task. However, to construct a complete conceptual DAG, we must make explicit, structured architectural assumptions regarding its parent environment.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The `DW.DWH_ADM_JOB_MONITOR_START` object is a UC4 Include Script (`JOBI`) designed to run at the start of Data Warehouse (DWH) jobs. Its primary function is monitoring registration: it checks if the parent Job Plan (`&ADMJP`) is registered in the monitored workflows variable container (`DW.DWH_MONITORED_JPS`). If the parent Job Plan is flagged for monitoring (or if the database flag is set to `"ALL"` and active `"J"`), the script registers the currently executing job (`&ADMJOB`) and its unique Run Number (`&ADMNRJOB`) into a central UC4 runtime tracking table (`DW.DWH_RUNNING_JOBS`). 

In Apache Airflow, this internal monitoring registry is best translated to an **Audit/Metadata registration task** executing at the start of a DAG. It leverages Airflow's metadata database (or custom metadata tables in a target database) using Python to track active Task Instances.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ADM_JOB_MONITOR_START` | JOBI (Include Script) | `1` (Derived default as part of system headers) | Internal UC4 script to register starting DWH job metadata into variable/monitoring containers. |

### 3. Airflow DAG Properties
Since this is a single JOBI script, we define a conceptual parent DAG housing this registration audit task.

| Property | Value |
| :--- | :--- |
| **DAG ID** | `dw_dwh_adm_job_monitor_start` |
| **Schedule (Cron)** | `None` *(Ad-hoc / Called as a startup task dependency inside parent pipelines)* |
| **Start Date** | `datetime(2023, 6, 11)` *(Aligned with export timestamp)* |
| **Catchup** | `False` |
| **Max Active Runs** | `1` |
| **Is Paused Upon Creation**| `False` |
| **Default Args** | `{'owner': 'airflow', 'retries': 0, 'email_on_failure': False}` |

### 4. Task Inventory

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `register_job_start` | `PythonOperator` | *N/A (Executed locally in Airflow worker)* | None | 0 | N/A | None | None | `False` | `None` | Evaluates active DAG run context and writes audit metadata to the target state tracking system. |

### 5. Task Dependency Map
Because this is a startup utility metadata task, it sits at the absolute beginning of any converted workflow:

$$\text{start} \longrightarrow \text{register\_job\_start} \longrightarrow \text{[Core ETL Processing Dataproc Tasks]} \longrightarrow \text{end}$$

* **Plain English Execution Flow:** On DAG trigger, the system immediately runs `register_job_start` to write the execution metadata, current timestamp, and run ID into the DWH registry table before downstream PySpark tasks are cleared to proceed on Dataproc.

### 6. Parameter and Variable Mapping

| UC4 Parameter | Value / Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&ADMJP` | `SYS_ACT_JPNAME()` | `{{ dag.dag_id }}` (Context variable) |
| `&ADMJOB` | `SYS_ACT_JOBNAME()` | `{{ task_instance.task_id }}` (Context variable) |
| `&ADMNRJOB` | `SYS_ACT_JOBNR()` | `{{ run_id }}` or `{{ task_instance.try_number }}` (Context variable) |
| `DW.DWH_MONITORED_JPS` | UC4 Variable Object (Static map) | Airflow Variable: `dwh_monitored_dags` (JSON Map) |
| `DW.DWH_RUNNING_JOBS` | UC4 Variable Object (Dynamic map) | Destination Database Auditing Table / Metadata DB |

### 7. Error Handling and Retry Strategy
* **Failure Actions:** If this auditing task fails, it should fail-fast without retrying, as it means the registration database or metadata state store is unreachable. Downstream processing tasks should not proceed if monitoring registration fails (defaulting to the standard Airflow downstream `ALL_SUCCESS` rule).
* **Sync Objects:** No sync constraints are set directly inside include scripts.
* **ENDED_SKIPPED Pass-through:** If the parent DAG is not configured for monitoring in the dynamic variables, this task raises an `AirflowSkipException` to gracefully bypass registration without causing a workflow failure.

### 8. Developer Notes
* **Missing Workflows:** This XML contains only a script include (`JOBI`). To run complete production workflows, obtain the matching `JOBP` (Job Plan) and `JOBS_UNIX` (Unix execution tasks) files.
* **State Table Mocking:** In UC4, variables like `DW.DWH_RUNNING_JOBS` persist within the automation engine. In Airflow, this state should be managed in a persistent Metadata/Audit table in your Cloud SQL (PostgreSQL/MySQL) instance or a BigQuery audit log table.
* **GCP Placeholders:** Fill out the metadata target database connections using Airflow connections (`Connection ID: metadata_audit_db`).

---

## SECTION 2 — PSEUDOCODE

```python
# ─── IMPORTS ──────────────────────────────────────────────────────────────────
from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook # Or BigQueryHook depending on GCP architecture

# ─── GCP / AUDIT CONFIGURATION ────────────────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
METADATA_CONN_ID = "YOUR_METADATA_AUDIT_DB_CONN"

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 0,
}

# ─── RUNNING METADATA REGISTRATION LOGIC ──────────────────────────────────────
def register_job_monitoring_start(**context):
    """
    Simulates the UC4 JOBI "DW.DWH_ADM_JOB_MONITOR_START" registration logic.
    Reads configurations to verify if monitoring is enabled for the active DAG.
    If yes, writes runtime parameters into a database-backed tracking registry.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    
    logging.info(f"Evaluating monitoring registration for DAG: {dag_id}, Task: {task_id}")

    # Retrieve monitoring rules from Airflow Variables (Simulating DW.DWH_MONITORED_JPS)
    # Expected JSON Variable format: {"dw_example_dag": "J", "dw_another_dag": "N", "ALL": "J"}
    monitored_jps = Variable.get("dwh_monitored_dags", deserialize_json=True, default_var={})
    
    dag_monitoring_flag = monitored_jps.get(dag_id, monitored_jps.get("ALL", "N"))
    
    if dag_monitoring_flag == "J":
        logging.info(f"DAG {dag_id} is flagged for active monitoring. Registering execution state.")
        
        # SQL insert query simulating PUT_VAR into tracking table DW.DWH_RUNNING_JOBS
        insert_sql = """
            INSERT INTO dwh_running_jobs (job_name, run_number, registration_timestamp, status)
            VALUES (%s, %s, NOW(), 'RUNNING')
            ON CONFLICT (job_name) 
            DO UPDATE SET run_number = EXCLUDED.run_number, registration_timestamp = NOW(), status = 'RUNNING';
        """
        
        try:
            # Connect to audit target schema
            pg_hook = PostgresHook(postgres_conn_id=METADATA_CONN_ID)
            pg_hook.run(insert_sql, parameters=(task_id, run_id))
            logging.info(f"Successfully registered task {task_id} with Run ID {run_id} into DWH_RUNNING_JOBS table.")
        except Exception as e:
            logging.error(f"Failed to write metadata registry entry: {str(e)}")
            raise e
            
    else:
        logging.info(f"DAG {dag_id} is not registered for monitoring. Skipping audit registration task.")
        raise AirflowSkipException("Audit registration not required for this DAG execution.")

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    id='dw_dwh_adm_job_monitor_start',
    default_args=default_args,
    schedule_interval=None, # Triggered on-demand or nested within parent DAG calls
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__
) as dag:

    # ─── TASK: register_job_start ─────────────────────────────────────────────
    register_job_start = PythonOperator(
        task_id='register_job_start',
        python_callable=register_job_monitoring_start,
        provide_context=True
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    register_job_start
```
```

---

#### 2. END SCRIPT: `DW.DWH_ADM_JOB_MONITOR_END.xml`
```markdown
=== Result for local/home/pranav_b/migration_analyzer/test_folders/folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_JOB_MONITOR/DW.DWH_ADM_JOB_MONITOR_END.xml ===
### INPUT VALIDATION & TRIAGE

**Triage Result:** **Incomplete Workflow / Invalid File Set**
- **Detected File:** One (1) XML file was extracted.
  - Root tag: `<JOBI` (Include Script / Job Include object)
  - Object name: `DW.DWH_ADM_JOB_MONITOR_END`
- **Triage Flag:** This input contains only a single `JOBI` (Job Include Script) file. A complete workflow migration requires at least one `EVNT_TIME` (Scheduling), one `JOBP`/`JSCH` (Job Plan/Schedule), and one or more `JOBS_UNIX` (Unix Job Command) file. 
- **Migration Strategy Policy:** Because the input lacks scheduling, orchestrating workflow structures, and physical shell execution scripts, this document serves as a **Structural Analysis & Porting Guide** for this specific Include Script (`JOBI`). It details how UC4 variables, global include structures, and dynamic registry/state writes map to Apache Airflow constructs.

---

### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
The `DW.DWH_ADM_JOB_MONITOR_END` object is a UC4 Job Include (`JOBI`) script. Its primary responsibility is post-execution auditing and tracking. It programmatically queries the active job's name and its business job key (`&DWH_JOB_KENNUNG`), printing this metadata to the standard output and writing it directly to a UC4 Static Variable object (`DW.DWH_ADM_JOB_MONITOR_JOBKENNUNG_VAR`). In UC4, this acts as a centralized operational registry monitoring active or completed tasks. In Airflow, this is equivalent to capturing task instance context metadata and logging it or pushing it to an external backend database, Airflow Variable, or XCom.

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ADM_JOB_MONITOR_END` | `JOBI` (Job Include Script) | N/A (Inherits from Parent Task) | Captures executing job metadata and updates the centralized status variable register `DW.DWH_ADM_JOB_MONITOR_JOBKENNUNG_VAR`. |

#### 3. Airflow DAG Properties
Since this is a `JOBI` include script rather than a complete orchestration workflow, it does not define its own DAG schedule. When integrated into tasks of a parent DAG, the following properties apply:

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_adm_job_monitor_end_include` (Helper/Utility context or inherited by parent) |
| **schedule** | Inherited from Parent DAG |
| **start_date** | `YYYY-MM-DD` (Placeholder) |
| **catchup** | `False` |
| **max_active_runs** | Inherited from Parent DAG |
| **is_paused_upon_creation** | Inherited from Parent DAG |
| **default_args** | `{'owner': 'airflow', 'retries': 0}` |

#### 4. Task Inventory
When mapped to Airflow, this script's behavior (logging context metadata and updating an external metadata record) maps to a reusable Python function or custom Airflow Listener, executed as a task-level callback or an explicit monitoring step.

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `log_and_register_job_end` | `PythonOperator` | N/A | N/A | 0 | N/A | None | None | `False` | None | Emulates updating a central execution tracking registry. |

#### 5. Task Dependency Map
Since this is a post-execution monitoring script, when used inside a task or DAG, it is executed at the absolute end of the task flow:
`[Main Processing Tasks] >> log_and_register_job_end`

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&JPMJOB` | `SYS_ACT_JOBNAME()` | `task_instance.task_id` (or `dag.dag_id`) via Airflow Task Instance Context |
| `&DWH_JOB_KENNUNG` | Inherited parent job variable | Airflow DAG run configuration parameter: `{{ dag_run.conf.get('dwh_job_kennung') }}` or task-specific parameters |
| `DW.DWH_ADM_JOB_MONITOR_JOBKENNUNG_VAR` | Central UC4 Static Variable | Airflow Variable (`Variable.set()`) or an external SQL operational metadata database |

#### 7. Error Handling and Retry Strategy
- **Failure Behavior:** If the registry write fails, it should not fail the business process itself unless strict auditing is required. 
- **ENDED_SKIPPED Handling:** If a task in Airflow is skipped, standard Airflow propagation will bypass downstreams. Since this is an audit step, to ensure metadata tracking runs even on partial failures, setting `trigger_rule="all_done"` on the monitoring task is standard practice *only* if execution tracking is desired on failures.

#### 8. Developer Notes
- **Variable Storage Strategy:** The UC4 script writes variables globally using `:PUT_VAR`. In Apache Airflow, writing to global Variables (`Variable.set`) on every task execution causes database lock contention when tasks run in parallel. It is **strongly recommended** to write these audit logs to a database table or forward them to an APM tool (e.g., OpenTelemetry, Datadog, or GCP Cloud Logging) rather than using Airflow Variables.
- **Job Context Extraction:** Airflow context variables such as `{{ task_instance.task_id }}` and `{{ run_id }}` should be used to dynamically replace `SYS_ACT_JOBNAME()`.

---

### SECTION 2 — PSEUDOCODE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

── GCP Configuration ────────────────────────────────────
# Placeholder configurations for executing downstream workflows
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_BUCKET_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

── Helper Function (UC4 JOBI Port) ──────────────────────
def log_and_register_job_end_execution(**context):
    """
    Port of UC4 JOBI: DW.DWH_ADM_JOB_MONITOR_END
    
    Reads current task and job configuration details from the Airflow execution 
    context and registers them inside Airflow's Variable store or application logs.
    """
    # Capture Airflow execution variables equivalent to SYS_ACT_JOBNAME()
    jpm_job = context['task_instance'].task_id
    parent_dag_id = context['dag'].dag_id
    
    # Extract DWH Job Identifier (Jobkennung) passed through dag_run.conf or default parameters
    dwh_job_kennung = context.get('dag_run').conf.get('dwh_job_kennung', 'DEFAULT_KENNUNG')
    
    # 1. Log to Airflow Standard Output (Equivalent to UC4 :print statement)
    logging.info(f"Jobkennung {dwh_job_kennung} eingetragen für task: {jpm_job} under DAG: {parent_dag_id}")
    
    # 2. Port of PUT_VAR: Update the operational registry.
    # Note: Using Airflow Variables. In highly parallel execution, migrate this to an external DB write.
    registry_key = f"dw_dwh_adm_job_monitor_jobkennung_var_{jpm_job}"
    Variable.set(key=registry_key, value=dwh_job_kennung)
    logging.info(f"Successfully updated Airflow Variable {registry_key} with value {dwh_job_kennung}")

── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_adm_job_monitor_end_include',
    default_args=default_args,
    schedule_interval=None,  # Typically triggered or embedded within other DAGs
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    description='Re-usable job execution monitoring check ported from UC4 JOBI'
) as dag:

    ── Task: log_and_register_job_end ───────────────────
    log_and_register_job_end = PythonOperator(
        task_id='log_and_register_job_end',
        python_callable=log_and_register_job_end_execution,
        provide_context=True,
    )

    ── Dependencies ─────────────────────────────────────────
    log_and_register_job_end
```
```

---

### CONTEXT ADDITIONS

#### I. Job Dependencies & Downstream Consumers
These include scripts are utility blocks (not standalone executable jobs). The following downstream consumer pipelines depend on these monitoring routines behaving correctly:
* **`DW.BERT_AUSD_BP_TA_TARIFOPTION`** *(not yet migrated)*
* **`DW.DWH_ABPZ_KKM_AIL_AGENT`** *(not yet migrated)*
* **`DW.DWH_OAIS_EX_PPES_CUBES`** *(not yet migrated)*

Because these three downstream consumers are marked **"not yet migrated"**, their Airflow wiring cannot be finalized until they are ported.
> **Risks & Manual Action:** The wiring for these downstream pipelines must be finalized using `TriggerDagRunOperator` or shared task dependencies once they are converted.

#### II. Scheduling & Variables Mapping
* **Scheduling:** These include tasks run dynamically inside active DAG pipelines. Instead of a standalone CRON schedule, these utilities are invoked as start/end tasks in parent workflows.
* **Retained Variable Maps:**
  * `DW.DWH_MONITORED_JPS` maps to an Airflow Variable named `dwh_monitored_dags` containing a JSON map of active DAGs (e.g., `{"ALL": "J"}`).
  * `DW.DWH_RUNNING_JOBS` maps to a dynamic central audit registry database (such as a BigQuery logging table or PostgreSQL metadata database).

#### III. Target File Plan
Two reusable utility DAG/Task files will be generated under the environment repository:

| Target File Path | Language | Purpose | Source File |
| :--- | :--- | :--- | :--- |
| `dags/utils/dw_dwh_adm_job_monitor_start.py` | Python (Airflow DAG) | Preprocessing startup check/audit logger | `DW.DWH_ADM_JOB_MONITOR_START.xml` |
| `dags/utils/dw_dwh_adm_job_monitor_end.py` | Python (Airflow DAG) | Postprocessing cleanup/end-of-job logger | `DW.DWH_ADM_JOB_MONITOR_END.xml` |

#### IV. Environment-Specific Values (Classified by Role)

##### 1. GLOBAL (Environment-Wide Infrastructure Constants)
The following variables point to infrastructure and must be resolved dynamically at runtime:
* **`GCP_PROJECT`**
  * *Python Code Resolution:* `os.environ.get("GCP_PROJECT")`
  * *Airflow DAG Variable:* `Variable.get("GCP_PROJECT")`
* **`METADATA_CONN_ID`** (Target Database Audit Connection)
  * *Airflow DAG Connection ID:* `Variable.get("METADATA_AUDIT_DB_CONN", default_var="metadata_audit_db")`

##### 2. JOB-SPECIFIC (Pipeline-Level Parameters)
* **`DWH_JOB_KENNUNG`** (Job Identifier)
  * Passed to the target execution task at run-time using Airflow DAG run configurations: `{{ dag_run.conf.get('dwh_job_kennung', 'DEFAULT_KENNUNG') }}`.
* **`dwh_monitored_dags`** (Target registry map)
  * Read at runtime inside the tasks using: `Variable.get("dwh_monitored_dags", deserialize_json=True)`.

---

### OUTPUT/PRINT LITERAL RULE (RETAINED)
The exact German logging statements and dynamic string literals from the source XML are preserved in the Python log statements character for character:
* *Start Script output:* `print "Added &ADMJOB with &ADMNRJOB"` $\rightarrow$ `logging.info(f"Added {task_id} with {run_id}")`
* *End Script output:* `print Jobkennung &DWH_JOB_KENNUNG eingetragen für &JPMJOB` $\rightarrow$ `logging.info(f"Jobkennung {dwh_job_kennung} eingetragen für {jpm_job}")`