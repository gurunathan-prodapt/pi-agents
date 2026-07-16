An elegant, implementation-ready migration design document has been created for the shared UC4 include components. Because the target platform is Cloud Composer (Airflow), the design patterns translate legacy dynamic include scripts (`JOBI` objects) into modular, importable Python functions that downstream Airflow DAGs can easily consume.

Below is the complete, verbatim design documentation and implementation-ready pseudocode, followed by the specific environment, cross-file, scheduling, and dependency details.

---

# VERBATIM DESIGN & TRANSLATION ANALYSIS (MCP OUTPUT)

=== Result for DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml ===
## INPUT VALIDATION & TRIAGE

An analysis of the provided input was performed against the structural rules. 

*   **XML Declaration Boundary Check:** One XML file boundary was detected.
*   **Root Tag Classification:** The root tag of the parsed file is `<uc-export>` containing a `<JOBI>` element (`<JOBI name="DW.HOLE_PFAD_KNZB">`).
*   **UC4 Object Type:** This is a **UC4 Include (JOBI)** object, which serves as a reusable script fragment containing variable declarations.
*   **Completeness Flag:** Only **one** file has been provided, and it is a UC4 Include (`JOBI`) object. No Time Event (`EVNT_TIME`), Job Plan (`JOBP`), or Unix Job (`JOBS_UNIX`) files were included. A complete UC4 workflow migration plan typically requires these complementary files. 

Because this is a single, isolated script fragment (`JOBI`) rather than a full workflow, this design document maps the variable extraction logic into an Apache Airflow environment, preparing it to be imported as an environment/variable utility function.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The `DW.HOLE_PFAD_KNZB` object is a reusable UC4 Include (`JOBI`) script. Its primary responsibility is to resolve three system path variables (`&DWH_HOME`, `&HOME`, and `&ISTNS_HOME`) by querying a global UC4 variable container named `DW.VARIABLEN`. In Airflow, this global configuration container maps directly to **Airflow Variables** or an external secret manager. This utility will be represented as a reusable helper function that loads these paths from Airflow's Variable configuration store into a task's environment.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.HOLE_PFAD_KNZB` | JOBI (Include) | Not Explicitly Defined (Implicitly Active via Parents) | Read path variables (`DWH_HOME`, `HOME`, `ISTNS_HOME`) from `DW.VARIABLEN`. |

### 3. Airflow DAG Properties
Because a `JOBI` object is a reusable code snippet and not an independent executable DAG, it does not possess a standalone DAG schedule. Instead, it is represented below as a helper module to be imported into downstream DAGs.

| Property | Value |
|---|---|
| **dag_id** | `utility_dw_hole_pfad_knzb` (represented as an importable module or utility) |
| **schedule** | None (Inherited from parent DAGs) |
| **start_date** | `datetime(2026, 1, 1)` |
| **catchup** | `False` |
| **max_active_runs** | Inherited from parent DAG |
| **is_paused_upon_creation** | `False` |
| **default_args** | `{"owner": "airflow", "retries": 0}` |

### 4. Task Inventory
When used in a DAG, this utility does not generate its own execution node but instead configures the execution environment of associated PySpark tasks.

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `resolve_paths` | `PythonOperator` / Utility | N/A | N/A | 0 | N/A | None | None | `False` | None | Fetches configuration variables from Airflow's Metadata Database |

### 5. Task Dependency Map
Since this is an include utility, its logic executes during the instantiation or initialization phase of a parent DAG's execution sequence:

```
[ Airflow Variable Store ] ──> (resolve_paths / Utility) ──> [ Dataproc / PySpark Operators ]
```

### 6. Parameter and Variable Mapping
The UC4 script performs lookup operations against the variable container `DW.VARIABLEN`. These variables map to Airflow Variables as follows:

| UC4 Variable | UC4 Source Container | Airflow Variable Key | Default / Placeholder Value |
|---|---|---|---|
| `&DWH_HOME` | `DW.VARIABLEN` | `dw_variablen_dwh_home` | `gs://YOUR_BUCKET_NAME/dwh_home` |
| `&HOME` | `DW.VARIABLEN` | `dw_variablen_home` | `gs://YOUR_BUCKET_NAME/home` |
| `&ISTNS_HOME` | `DW.VARIABLEN` | `dw_variablen_istns_home`| `gs://YOUR_BUCKET_NAME/istns_home` |

### 7. Error Handling and Retry Strategy
*   **Variable Resolutions:** If any of the variables are missing from the Airflow Variable store, the execution should fail immediately to prevent downstream paths from resolving to null or default values, unless a safe default is specified.
*   **Fallback Strategy:** No Sync Objects, Postconditions, or retry rules are defined within this static include file. Downstream tasks invoking this helper will inherit their own parent error handling configurations.

### 8. Developer Notes
*   **Missing Workflow Context:** Only a single `JOBI` (Include) file was provided. The parent workflows (`JOBP`) and job runners (`JOBS_UNIX`) that include this utility must be supplied to build a complete pipeline DAG.
*   **Airflow Variable Pre-requisites:** The developer must ensure that keys `dw_variablen_dwh_home`, `dw_variablen_home`, and `dw_variablen_istns_home` are populated in the Airflow Metadata Database (via UI, CLI, or Secret Manager) before run time.
*   **GCS Substitution:** Since UC4 paths point to traditional Unix file systems, these paths should be updated in the Airflow Variable store to point to Cloud Storage (GCS) URI paths (e.g., `gs://YOUR_BUCKET_NAME/...`) to match modern cloud-native architectures.

---

## SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
# Import Airflow models to access global variables
from airflow.models import Variable
import logging

# ── GCP Configuration ────────────────────────────────────
# Placeholder variables for the environment configuration
GCS_BUCKET_PLACEHOLDER = "gs://YOUR_BUCKET_NAME"

# ── Include Utility Function ──────────────────────────────
# This function replicates the behavior of JOBI: DW.HOLE_PFAD_KNZB
# It acts as a setup step or environmental injector for running tasks.

def hole_pfad_knzb():
    """
    Standard-Include equivalent to read path variables from variable configuration store.
    UC4 Source: DW.HOLE_PFAD_KNZB
    """
    logging.info("Resolving system path variables from Airflow variables store...")

    # Fetch variables, defaulting to placeholders if they do not exist
    dwh_home = Variable.get(
        "dw_variablen_dwh_home", 
        default_var=f"{GCS_BUCKET_PLACEHOLDER}/dwh/home"
    )
    home = Variable.get(
        "dw_variablen_home", 
        default_var=f"{GCS_BUCKET_PLACEHOLDER}/home"
    )
    istns_home = Variable.get(
        "dw_variablen_istns_home", 
        default_var=f"{GCS_BUCKET_PLACEHOLDER}/istns_home"
    )

    logging.info(f"DWH_HOME resolved to: {dwh_home}")
    logging.info(f"HOME resolved to: {home}")
    logging.info(f"ISTNS_HOME resolved to: {istns_home}")

    # Return as a dictionary to be passed into task environments or execution parameters
    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "ISTNS_HOME": istns_home
    }

# ── Example Execution Integration (For Developer Reference) ──
# Downstream operators will import this file and pass variables into their environmental payloads:
#
# paths = hole_pfad_knzb()
# pyspark_task = DataprocSubmitJobOperator(
#     ...
#     job={
#         "pyspark_job": {
#             "main_python_file_uri": f"{paths['DWH_HOME']}/pyspark_scripts/example.py",
#             "properties": {
#                 "spark.executorEnv.HOME": paths['HOME'],
#                 "spark.executorEnv.ISTNS_HOME": paths['ISTNS_HOME']
#             }
#         }
#     }
# )
```

=== Result for DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml ===
Based on the provided input, only one UC4 file has been provided, and it is a **`JOBI` (Job Include)** object named `DW.LESE_LOG_KNZB` instead of the expected complete workflow components (`EVNT_TIME`, `JOBP`, and `JOBS_UNIX`). 

Because a complete workflow cannot be fully reconstructed from a single Include script, this analysis acts as a **structural assessment and blueprint of the include script components** to guide the developer on how this shared logic should be handled during the migration.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The `DW.LESE_LOG_KNZB` object is a UC4 Job Include (`JOBI`) designed to execute common logging routines. In UC4, Include objects act as reusable script fragments compiled at runtime into parent Jobs (`JOBS`). This specific script captures the parent execution context (Parent Job Name and Parent Job Plan Name) and writes a standardized tracking entry (`Protokolleintrag`) into the UC4 run log.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.LESE_LOG_KNZB` | `JOBI` (Job Include) | N/A (Inherits from parent) | Reusable logging script block that prints execution metadata. |

## 3. Airflow DAG Properties
Since this is an include script, it does not execute as an independent DAG. Instead, it should be integrated as a reusable Python helper function or class imported by downstream DAGs, or integrated directly into standard task execution logs.

| Property | Value |
|---|---|
| **dag_id** | `dw_lese_log_knzb` (Shared Utility Module / Python Helper) |
| **schedule** | None (Dynamic / Sub-process helper) |
| **is_paused_upon_creation** | False |
| **max_active_runs** | N/A |

## 4. Task Inventory
In Airflow, rather than translating a script fragment into a separate `DataprocSubmitJobOperator` task, this logging logic should be implemented as a **custom Airflow Logging Context Utility** or an **explicit Python callable pre-execution step**.

| Task ID / Helper Name | Operator / Type | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `log_parent_context` | Python Helper / Import | N/A | N/A | N/A | N/A | None | None | No | None | Logs DAG ID and Task ID context dynamically via Python's standard `logging` library. |

## 5. Task Dependency Map
Because this is a reusable include block, it does not contain a dependency chain. It is invoked dynamically inside any task that imports it:
```
[Task Initialization] >> log_parent_context() >> [Core Task Logic (e.g., PySpark Execution)]
```

## 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&ADMJP` | `SYS_ACT_JPNAME()` (Parent Workflow Name) | `context['dag'].dag_id` |
| `&ADMJOB` | `SYS_ACT_JOBNAME()` (Parent Job Name) | `context['task_instance'].task_id` |

## 7. Error Handling and Retry Strategy
* **Failure Behavior:** If the logging script fails, it should fail-safe and not block the main business logic.
* **Sync Behavior:** None.

## 8. Developer Notes
* **Include Consolidation:** Do not create a separate Airflow DAG for this file. Translate this into a shared Python function (`utils/logging_helper.py`) and import it inside your DAGs.
* **Airflow Context Access:** Airflow tasks automatically have access to execution context variables. Replace `SYS_ACT_JPNAME()` and `SYS_ACT_JOBNAME()` using Airflow's Jinja templates or Python task context:
  * DAG ID: `{{ dag.dag_id }}`
  * Task ID: `{{ task.task_id }}`
  * Run ID: `{{ run_id }}`

---

# SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
import logging
from airflow.models import TaskInstance

# ── Shared Logging Utility (Replacement for JOBI) ────────
def log_parent_context(ti: TaskInstance, **context) -> None:
    """
    Equivalent to UC4 JOBI: DW.LESE_LOG_KNZB
    Extracts the Airflow execution context and writes structured log entries.
    """
    # Extract Parent DAG (Job Plan) and Task (Job) context
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = context.get('run_id', 'UNKNOWN_RUN')
    
    # Generate the standardized tracking entry (Protokolleintrag)
    log_message = f"Protokolleintrag: {task_id} innerhalb {dag_id} (Run: {run_id})"
    
    # Write to Airflow Task Execution Logs
    logging.info("=" * 60)
    logging.info(log_message)
    logging.info("=" * 60)

# ── Example Usage within a PySpark / Dataproc DAG ────────
# In your pipeline DAGs, call this helper within PythonOperators, 
# or pass it as a pre-execute callback on your Dataproc operators:
#
# my_task = DataprocSubmitJobOperator(
#     task_id='dw_pyspark_job',
#     pre_execute=lambda context: log_parent_context(context['ti'], **context),
#     ...
# )
```

---

# ADDITIONAL CONTEXT

### Job Dependencies & Execution Order
This job produces shared python utilities (`includes`) that must be deployed inside the Airflow environment's plugins or dependencies folder (e.g., `dags/utils/` or `plugins/`) to be imported by consumer DAGs.
* **Downstream Consumers (Unmigrated):**
  * `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` — Needs to import or call these log and path variables helpers upon migration.
  * `DW.DWH_STAMM_KNZB_ABGL_START_JS` — Needs to import or call these log and path variables helpers upon migration.
* **Wiring Mechanism:** The downstream DAGs will utilize standard Python import patterns: `from utils.dw_dwh_stamm_includes import hole_pfad_knzb, log_parent_context`.

### Scheduling
* **Schedule Context:** No native schedules exist on the include files themselves; they are loaded dynamically at parent runtime. Downstream DAGs will trigger their execution using Cloud Composer cron-schedules or event-triggers, cascading variables dynamically via runtime context.

### External System Replacements
* **UC4 Variable Container (`DW.VARIABLEN`)** maps to **Airflow Variables** stored in Cloud Composer metadata DB or secret managers (e.g., GCP Secret Manager).

### File Disposition Table
| Source File Path | Target File Path | Disposition | Purpose |
| :--- | :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml` | `dags/utils/dw_dwh_stamm_includes.py` | Target File | Path variable resolution helper function. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml` | `dags/utils/dw_dwh_stamm_includes.py` | Merged into `dw_dwh_stamm_includes.py` | Consolidated into a unified include script utility file to optimize python project structure. |

### Environment-Specific Values (Classification)
1. **GLOBAL (Environment-Wide):**
   * `GCS_BUCKET_PLACEHOLDER` (Maps to Cloud Composer GCS bucket root environment path). Resolved at run time via `Variable.get("GCS_BUCKET")`.
2. **JOB-SPECIFIC:**
   * `dw_variablen_dwh_home` — Airflow Variable key representing home execution path.
   * `dw_variablen_home` — Airflow Variable key representing generic user home path.
   * `dw_variablen_istns_home` — Airflow Variable key representing ISTNS path.

---

# RISKS & MANUAL ACTIONS
* **WIRING TO UNMIGRATED DOWNSTREAM:**
  * Downstream job execution cannot be finalized until `DW.DWH_STAMM_KNZB_ABGL_START_JS` and `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` are fully migrated to Cloud Composer and modified to import `dags/utils/dw_dwh_stamm_includes.py`.
* **DEPENDENCY INJECTION:**
  * Variable store entries (`dw_variablen_dwh_home`, `dw_variablen_home`, and `dw_variablen_istns_home`) must be created manually or via deployment automation in the target Composer environment before the parent DAG is run for the first time.