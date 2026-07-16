### SECTION 1 — DESIGN DOCUMENT (VERBATIM MCP OUTPUTS)

Below are the complete, unmodified migration design document sections produced by the UC4-to-Airflow conversion engine for the two included Job Include (`JOBI`) components.

---

#### === Result for /home/gurunathan_t/clean_migration_dataset/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml ===
### INPUT VALIDATION

* **Validation Status:** Failed (Partial/Incomplete Workflow)
* **Reasoning:** Only one XML file was provided in the input, and its root tag is `<uc-export>` containing a `<JOBI>` (Job Include) object rather than an `EVNT_TIME`, `JOBP`, `JSCH`, or `JOBS_UNIX` file. A complete, runnable workflow migration requires at least one Schedule/Event coordinator, one Job Plan container, and the underlying execution Jobs.
* **Resolution:** Because the single file provided is a **Job Include (JOBI)** rather than a complete workflow, this document serves as a specialized structural analysis and mapping blueprint for this shared component (`DW.HOLE_PFAD_VTRG`). To build a complete DAG, please supply the master Job Plan (`JOBP`), Schedule (`JSCH`/`EVNT_TIME`), and Unix Job (`JOBS_UNIX`) XML files that reference or depend on this path-resolution logic.

---

### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
The `DW.HOLE_PFAD_VTRG` object is a UC4 Job Include (`JOBI`) designed to perform central path resolution for the Data Warehouse (DWH) environment. It dynamically retrieves absolute directory paths (`DWH_HOME`, `HOME`, and `PMS_HOME`) from a central variables container named `DW.VARIABLEN` and exports them as variables for downstream jobs. In Apache Airflow, this global configuration pattern is migrated away from inline script injection and instead mapped to **Airflow Variables** or a centralized **GCP Secret Manager / Airflow Connection** configuration.

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.HOLE_PFAD_VTRG` | `JOBI` (Job Include) | N/A (Inherited) | Utility script that extracts global directory paths (`DWH_HOME`, `HOME`, `PMS_HOME`) from the configuration container `DW.VARIABLEN`. |

#### 3. Airflow DAG Properties
Since this is an include utility and not a standalone runnable workflow, these properties represent the global context under which this utility will be deployed when integrated into the parent DAG:

| Property | Value |
|---|---|
| **dag_id** | `dw_hole_pfad_vtrg` *(Utility mapping - typically integrated into parent DAGs)* |
| **schedule** | `None` (Ad-hoc / Inherited from parent workflow) |
| **start_date** | `datetime(2026, 1, 1)` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` |
| **default_args** | `{'owner': 'airflow', 'retries': 0}` |

#### 4. Task Inventory
In Apache Airflow, physical files containing inline include commands are replaced by native environmental variable lookups or dynamic dictionary injections inside the `DataprocSubmitJobOperator` properties.

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `resolve_path_variables` | `PythonOperator` (or Airflow Variable retrieval) | N/A | N/A | 0 | N/A | None | None | `False` | None | Reads variables from Airflow's metadata DB or GCP Secret Manager to replicate the `DW.VARIABLEN` container. |

#### 5. Task Dependency Map
Since this is an inline include component, it runs at the very beginning of any task initialization:
```
resolve_path_variables >> [Downstream PySpark Job Tasks...]
```
* **Plain English Flow:** Before executing any converted Unix / PySpark jobs, the Airflow environment resolves the target GCS bucket paths and script coordinates using the parameters retrieved by this task, ensuring that downstream PySpark execution operators point to the correct environment paths.

#### 6. Parameter and Variable Mapping
The UC4 variable container (`DW.VARIABLEN`) and its resolved keys map directly to Airflow Variables or environment variables:

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `GET_VAR('DW.VARIABLEN','DWH_HOME')` | Source variable value | `Variable.get("dwh_home", default_var="gs://YOUR_BUCKET_NAME/dwh/")` |
| `GET_VAR('DW.VARIABLEN','HOME')` | Source variable value | `Variable.get("home_dir", default_var="gs://YOUR_BUCKET_NAME/home/")` |
| `GET_VAR('DW.VARIABLEN','PMS_HOME')` | Source variable value | `Variable.get("pms_home", default_var="gs://YOUR_BUCKET_NAME/pms/")` |

#### 7. Error Handling and Retry Strategy
* **Failure Actions:** If path resolution fails, any dependent downstream task must immediately halt.
* **Airflow Implementation:** The variable resolution runs in-memory during DAG parsing or as an initial synchronous validation task. Failure to resolve these variables raises a standard Airflow exception, preventing downstream jobs from starting with unresolved paths.

#### 8. Developer Notes
* **GCP Placement:** In UC4, these variables pointed to physical UNIX file systems. In the migrated GCP environment, these must point to Google Cloud Storage (GCS) prefixes (e.g., `gs://YOUR_BUCKET_NAME/dwh/`).
* **Migration Pattern:** Do not attempt to run this script block as a shell task. Instead, reference these variables directly within the DAG's python definition using `var.value.dw_variablen_dwh_home` in template fields, or via Python’s `Variable.get()`.

---

### SECTION 2 — PSEUDOCODE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.exceptions import AirflowException

── GCP Configuration ────────────────────────────────────
# Placeholder variables representing the targets for variable mapping
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'data_warehouse_ops',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

── Variable Resolution Helper ───────────────────────────
def resolve_dwh_paths(**context):
    """
    Simulates the UC4 JOBI 'DW.HOLE_PFAD_VTRG' by fetching keys 
    from Airflow's global Variable store (representing the DW.VARIABLEN container).
    """
    try:
        dwh_home = Variable.get("dw_variablen_dwh_home", default_var=f"gs://{GCS_BUCKET}/dwh_home")
        home = Variable.get("dw_variablen_home", default_var=f"gs://{GCS_BUCKET}/home")
        pms_home = Variable.get("dw_variablen_pms_home", default_var=f"gs://{GCS_BUCKET}/pms_home")
        
        # Push to XCom so downstream tasks can optionally reference them dynamically
        context['ti'].xcom_push(key='DWH_HOME', value=dwh_home)
        context['ti'].xcom_push(key='HOME', value=home)
        context['ti'].xcom_push(key='PMS_HOME', value=pms_home)
        
        print(f"Paths successfully resolved: DWH_HOME={dwh_home}, HOME={home}, PMS_HOME={pms_home}")
    except Exception as e:
        raise AirflowException(f"Failed to resolve path variables from Airflow variables store: {str(e)}")

── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_hole_pfad_vtrg_parent_wf',
    schedule_interval=None,  # Parent workflows should set their own trigger schedules
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args
)

── Tasks ────────────────────────────────────────────────
# 1. Path Resolution Task (Replaces JOBI script)
resolve_paths = PythonOperator(
    task_id='resolve_path_variables',
    python_callable=resolve_dwh_paths,
    provide_context=True,
    dag=dag
)

# 2. Downstream Job Template (Example showing how the include-paths are consumed)
pyspark_job_args = [
    "--dwh_home", "{{ task_instance.xcom_pull(task_ids='resolve_path_variables', key='DWH_HOME') }}",
    "--pms_home", "{{ task_instance.xcom_pull(task_ids='resolve_path_variables', key='PMS_HOME') }}"
]

execute_downstream_pyspark_job = DataprocSubmitJobOperator(
    task_id='execute_migrated_unix_job',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job={
        "reference": {"project_id": GCP_PROJECT_ID, "job_id": "dw_migrated_job_{{ run_id | stable_uuid }}"},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/migrated_job.py",
            "args": pyspark_job_args
        }
    },
    dag=dag
)

── Dependencies ─────────────────────────────────────────
resolve_paths >> execute_downstream_pyspark_job
```

---

#### === Result for /home/gurunathan_t/clean_migration_dataset/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml ===
### INPUT VALIDATION

* **Error / Warning:** The input contains only one file of root type `<JOBI` (Job Include / JOBI object: `DW.LESE_LOG_VTRG`).
* **Flagged Missing Dependencies:** A complete UC4 migration blueprint requires a workflow sequence. The critical control structures (at least one `EVNT_TIME` for schedule configuration, one `JOBP` or `JSCH` for dependency and task node configuration, and one `JOBS_UNIX` for executable commands) are entirely missing from this input.
* **Result:** Since the input contains only a JOBI metadata object, a complete End-to-End Workflow Design cannot be constructed. However, a detailed analysis of this specific object is provided below as a partial blueprint.

---

### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
The provided UC4 object `DW.LESE_LOG_VTRG` is a shared **Job Include (JOBI)** object, which is used as a reusable code block inside other workflow structures. In UC4, JOBI objects do not run as standalone tasks; instead, their scripts are dynamically interpreted inside parent jobs. This specific script fetches parent execution context information (specifically the parent Job Plan and job name) and writes a standardised diagnostic or protocol log into the UC4 execution run log.

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.LESE_LOG_VTRG` | JOBI (Job Include) | Not Specified (Inherited) | Diagnostic logging utility that prints current parent job plan (`&ADMJP`) and parent job name (`&ADMJOB`). |

#### 3. Airflow DAG Properties
Since this object is a utility JOBI block and not a parent DAG/Workflow itself, it does not define standalone DAG-level properties. If it is mapped directly into an Airflow environment, it should be treated as a helper function or Python import block within parent DAGs.

| Property | Value |
|---|---|
| **dag_id** | *N/A (Reusable utility helper)* |
| **schedule** | *N/A (Inherits from caller)* |
| **max_active_runs** | *N/A* |
| **is_paused_upon_creation**| *N/A* |

#### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `write_protocol_log` | `PythonOperator` | N/A | None | Inherited | Inherited | None | None | No | None | Standard Python function task that outputs context metadata to Airflow execution logs. |

#### 5. Task Dependency Map
Because this is a standalone include snippet, there are no structural task sequences or downstream triggers defined in this file. In Airflow, this helper would be placed at the beginning or as an inline log emitter within larger DAG executions:

`start >> write_protocol_log >> downstream_processing_tasks`

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&ADMJP` | `SYS_ACT_JPNAME()` | `context['dag'].dag_id` |
| `&ADMJOB` | `SYS_ACT_JOBNAME()` | `context['task_instance'].task_id` |

#### 7. Error Handling and Retry Strategy
Since this task performs basic Python-level diagnostic printing, it does not require external retry configurations or custom failure callbacks. It should simply leverage default task execution properties.

#### 8. Developer Notes
* **Missing Structural Workflows:** Because only a single JOBI file was supplied, no execution workflows, schedules, calendar rules, or Dataproc PySpark commands are available. To build a complete execution pipeline, you must supply the corresponding parent `JOBP` (Job Plan) and executable `JOBS_UNIX` (Unix Job) configurations.
* **UC4 Variable Substitution:** The UC4 script-level functions `SYS_ACT_JPNAME()` and `SYS_ACT_JOBNAME()` must be mapped to their native Airflow task context equivalents (`dag_id` and `task_id` respectively). This is demonstrated in the Pseudocode section.

---

### SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Logging Helper ─────────────────────────────────────────
def execute_protocol_log(**context):
    """
    Equivalent to UC4 JOBI 'DW.LESE_LOG_VTRG'.
    Fetches the runtime workflow/DAG and Task names from the context
    and writes them to the standard Airflow execution log.
    """
    admjp = context['dag'].dag_id
    admjob = context['task_instance'].task_id
    
    # Original UC4 Print logic mapping
    logging.info(f"Protokolleintrag: {admjob} innerhalb {admjp}")

# ── DAG Definition ───────────────────────────────────────
# Note: This is a placeholder representation showing how the helper is 
# integrated. Actual scheduling and configurations depend on the missing parent XMLs.
with DAG(
    dag_id="dw_lese_log_vtrg_wrapper",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger until parent schedule XML is provided
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True
) as dag:

    # ── Task: write_protocol_log ────────────────────────────
    write_protocol_log = PythonOperator(
        task_id="write_protocol_log",
        python_callable=execute_protocol_log,
        provide_context=True,
    )
```

---

### SECTION 2 — CONTEXT & TARGET ARCHITECTURE

The following sections incorporate critical architectural context from the pre-collected workspace that the standalone MCP analysis could not assess.

#### 1. Job Dependencies & Cross-DAG Wiring
* **Upstream Dependencies:** None discovered in the legacy job dependencies context.
* **Downstream Consumers (Cross-DAG triggers):**
  * `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` (Not yet migrated)
  * `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` (Not yet migrated)
  
  *Wiring on BigQuery/Cloud Composer:* Because these two files are utility includes (`JOBI`) designed to inject code inline at run time, they do not themselves directly trigger or establish direct cross-DAG pipelines. However, downstream jobs that invoke tasks containing these modules must be configured on Cloud Composer to preserve these executions. Since the downstream consumer jobs are "not yet migrated", this connection cannot be fully automated or finalized until those parent structures are defined.

#### 2. Execution Order & Shared Path Handling
The target orchestration must adhere to the execution constraints of these files:
* `DW.HOLE_PFAD_VTRG` must execute or resolve its constants before any other task in the parent workflow begins.
* `DW.LESE_LOG_VTRG` is dynamically invoked inline inside running tasks to print run logs. In Airflow, this is mapped as a standard importable Python helper module, typically called at the entry point of execution operators.

#### 3. Scheduling & Orchestration
* **Scheduling Mapping:** No direct scheduler is attached to these files since they represent JOBI imports. They inherit the schedule, triggers, and calendar rules of whatever parent DAG (`JOBP`/`JSCH`) references them.

#### 4. Shared Files & Common Schemas
These components represent **shared includes**. Under the repository structure, they are placed in a mirrored target sub-folder `includes/` to keep clean separation from executable tasks.

---

### SECTION 3 — TARGET ENVIRONMENT CONFIGURATION

#### 1. Environment-Specific Variables (Global vs. Job-Specific)

##### GLOBAL VARIABLES (Environment-Wide Configuration)
To maintain structural correctness across dev, test, and prod environments, these variables must be retrieved from the Airflow Variables storage (or GCP Secret Manager) at runtime:

* `GCP_PROJECT`: Fetched at run-time.
* `GCP_REGION`: Target GCP deploy region.
* `GCS_BUCKET`: The GCS bucket containing runtime scripts, schemas, and configurations.
* `dw_variablen_dwh_home`: Maps to the legacy UC4 `GET_VAR('DW.VARIABLEN', 'DWH_HOME')` variable container. Retrieved dynamically via:
  ```python
  from airflow.models import Variable
  DWH_HOME = Variable.get("dw_variablen_dwh_home")
  ```
* `dw_variablen_home`: Maps to legacy UC4 `GET_VAR('DW.VARIABLEN', 'HOME')`. Retrieved dynamically via:
  ```python
  HOME = Variable.get("dw_variablen_home")
  ```
* `dw_variablen_pms_home`: Maps to legacy UC4 `GET_VAR('DW.VARIABLEN', 'PMS_HOME')`. Retrieved dynamically via:
  ```python
  PMS_HOME = Variable.get("dw_variablen_pms_home")
  ```

##### JOB-SPECIFIC PARAMETERS
* No job-specific constants are present inside these shared utility functions.

---

### SECTION 4 — FILE DISPOSITION & TARGET PLAN

#### File Disposition Table
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml` | `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/dw_hole_pfad_vtrg.py` | Migrated as an importable helper module in a mirrored directory structure to resolve runtime environment path mappings. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml` | `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/dw_lese_log_vtrg.py` | Migrated as a Python logging utility module to allow structured DAG and Task instance execution reporting. |

#### Folder Integrity Rule Compliance
The target repository files are mapped into `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/` to cleanly mirror the original layout. No files from other source folders are mixed into these outputs.

---

### SECTION 5 — RISKS & MANUAL ACTIONS

1. **WIRING COMPONENT GAPS — DOWNSTREAMS NOT MIGRATED:**
   * Downstream consumers `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` and `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` are currently **not yet migrated**. Airflow cross-DAG triggers, datasets, or execution sensors cannot be fully tested or established until those jobs have been translated.
2. **ENVIRONMENT-SPECIFIC PATH TRANSITION (GCS Prefixes):**
   * The original UC4 variable values in `DW.VARIABLEN` likely pointed to raw POSIX UNIX filesystems (e.g., `/opt/dwh/...`). During environment setup, the Airflow Variables (`dw_variablen_dwh_home`, `dw_variablen_home`, and `dw_variablen_pms_home`) **must be populated with valid Cloud Storage bucket paths** (e.g., `gs://[BUCKET-NAME]/dwh/`) instead of local POSIX strings, so that downstream PySpark or Spark-SQL tasks resolve their data contexts correctly.