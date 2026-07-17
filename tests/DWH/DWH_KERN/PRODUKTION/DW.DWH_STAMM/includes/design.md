# MIGRATION DESIGN DOCUMENT: Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes

## File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml` | `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.py` | Standalone Python module / Airflow utility to load path configuration values. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml` | `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.py` | Standalone Python logging helper / pre-execute wrapper preserving German logging formats. |

---

## 1. Executive Summary & Prescribed Pattern
* **Prescribed Migration Pattern:** Pure orchestration migration (`UC4_ONLY`) mapping to Cloud Composer / Apache Airflow.
* **Preservation Strategy:** Standard UC4 Includes (`JOBI`) do not represent standalone scheduled execution graphs. Instead, they represent reusable, shared logic segments. To retain folder integrity and structural modularity, these includes are compiled as standard Python utility modules under their original mirrored directories:
  * `DW.HOLE_PFAD_KNZB` maps to an environment path resolver utilizing Airflow Variables.
  * `DW.LESE_LOG_KNZB` maps to an execution logging utility, preserving original diagnostic logs.

---

## 2. Environment Variables & Global Configurations
Consistent with the **Environment Variable Policy**, legacy UC4 variable container reads (`GET_VAR`) are classified and resolved as **GLOBAL** target properties retrieved at runtime:

1. **`GCP_PROJECT`** (Global Infrastructure)
   * Source: Inferred target project context.
   * Target Resolution: `Variable.get("GCP_PROJECT")`
2. **`GCS_BUCKET`** (Global Storage)
   * Source: Root environment cloud storage bucket.
   * Target Resolution: `Variable.get("GCS_BUCKET")`
3. **`dw_variablen_dwh_home`** (Global Environment Path)
   * Source: Legacy `GET_VAR('DW.VARIABLEN','DWH_HOME')`
   * Target Resolution: `Variable.get("dw_variablen_dwh_home")`
4. **`dw_variablen_home`** (Global Environment Path)
   * Source: Legacy `GET_VAR('DW.VARIABLEN','HOME')`
   * Target Resolution: `Variable.get("dw_variablen_home")`
5. **`dw_variablen_istns_home`** (Global Environment Path)
   * Source: Legacy `GET_VAR('DW.VARIABLEN','ISTNS_HOME')`
   * Target Resolution: `Variable.get("dw_variablen_istns_home")`

---

## 3. Context & Scheduling (Retained)
* **Job Scheduling:** None. These are include files (`JOBI`) containing shared logic and carry no independent triggers or crons.
* **Job Dependencies:**
  * **Downstream Consumers (Cross-Job Hand-off):**
    * `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` — *not yet migrated*
    * `DW.DWH_STAMM_KNZB_ABGL_START_JS` — *not yet migrated*
  * **Wiring Strategy:** Once the downstream Job Plans (`JOBP`/`JSCH`) are migrated, they will import these utility functions or call them via pre-execute task handlers within Cloud Composer.

---

## 4. Risks & Manual Actions
* **Wiring Dependency:**
  * SOURCE: NOT FOUND — `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` — no candidate (must be wired downstream once migrated).
  * SOURCE: NOT FOUND — `DW.DWH_STAMM_KNZB_ABGL_START_JS` — no candidate (must be wired downstream once migrated).
* **Airflow Variable Setup:** The variables `dw_variablen_dwh_home`, `dw_variablen_home`, and `dw_variablen_istns_home` must be created in the Airflow Variable Metadata DB (or Cloud Composer variables pane) prior to DAG run execution.

---

## SECTION 5 — VERBATIM UC4 TO AIRFLOW DESIGN TOOL OUTPUT

The following design output has been extracted and preserved verbatim:

```markdown
## INPUT VALIDATION
* **Flagged Issue:** The provided UC4 XML files consist solely of two Include (`JOBI`) objects: `DW.HOLE_PFAD_KNZB` and `DW.LESE_LOG_KNZB`. 
* **Critical Migration Deficit:** A complete, runnable workflow migration requires at least one Time Event (`EVNT_TIME`), one Job Plan/Schedule (`JOBP`/`JSCH`), and one or more Unix Job (`JOBS_UNIX`) files. No workflow orchestration structure, task dependencies, schedules, or execution commands (such as Ab Initio graphs) are present in the provided XML source payload.
* **Proceeding with Structural Analysis:** As requested, this design document translates the available input metadata, scripts, and includes into their logical Airflow architectural equivalents. However, a complete DAG structural generation is halted due to the lack of Job Plans. This document serves as the architectural foundation and framework pattern for when the core workflow XMLs are provided.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The provided files are UC4 Include (`JOBI`) objects designed as modular, reusable components within larger Data Warehouse (DWH) workflows. 
* `DW.HOLE_PFAD_KNZB` serves as a configuration loader, reading system path environment variables (such as home directories and staging zones) from a central global variable container (`DW.VARIABLEN`).
* `DW.LESE_LOG_KNZB` functions as a standard execution logger, writing metadata about the parent Job Plan (`JOBP`) and active execution Job (`JOBS_UNIX`) into the task's runtime log.
Because these are auxiliary modular components, they do not possess a standalone schedule or independent execution chain. When migrated to Apache Airflow, these patterns map to environment configuration initialisation routines and custom task-logging wrappers within DAG creation.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.HOLE_PFAD_KNZB` | `JOBI` (Include) | N/A (Inherited) | Utility script to fetch path environment variables from the `DW.VARIABLEN` container. |
| `DW.LESE_LOG_KNZB` | `JOBI` (Include) | N/A (Inherited) | Utility script to print parent workflow and task metadata into execution logs. |

## 3. Airflow DAG Properties
Since no scheduling or parent container workflow (`JOBP` / `JSCH` / `EVNT_TIME`) was provided in the source payload, the table below represents the fallback pattern framework assuming these includes are ultimately packaged into a standard orchestrated DWH DAG structure.

| Property | Value | Note / Source |
| :--- | :--- | :--- |
| **dag_id** | `dw_dwh_stamm_parent_workflow_placeholder` | Derived placeholder representing the target DWH Stamm pipeline. |
| **schedule** | `None` | No schedule context provided in input. |
| **start_date** | `datetime(2026, 7, 16)` | Placeholder based on XML export timestamp context. |
| **catchup** | `False` | Recommended standard practice for backfill prevention. |
| **max_active_runs** | `1` | Default safeguard pattern for core DWH pipelines. |
| **is_paused_upon_creation** | `True` | Recommended default until parent scheduling wrapper is provided. |
| **default_args** | `{'owner': 'airflow', 'retries': 0}` | Standard default arguments. |

## 4. Task Inventory
Because these are standard UC4 Includes, they do not map directly to standalone tasks in an Airflow Task Inventory. Instead:
1. `DW.HOLE_PFAD_KNZB` maps to Airflow configuration loading (either via Airflow Variables or environment-level dictionary lookups).
2. `DW.LESE_LOG_KNZB` maps to dynamic task log generation via a Python callable inside `pre_execute` hooks, custom Operators, or standard Python logging routines.

*If these includes were to be executed as standalone tasks for test verification, they would map as follows:*

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `hole_pfad_knzb_test` | `PythonOperator` | N/A | None | 0 | N/A | None | None | `False` | None | Emulates variable fetching framework. |
| `lese_log_knzb_test` | `PythonOperator` | N/A | None | 0 | N/A | None | None | `False` | None | Emulates context-based run logger. |

## 5. Task Dependency Map
Since no parent workflow structure was provided, the theoretical sequence of these modular initialisation steps within a target DAG run is mapped below:

`start_execution_log` **>>** `load_path_variables` **>>** `[Target_DWH_Jobs_Placeholder]` **>>** `end_execution_log`

*   **`start_execution_log`**: Invokes the logger context wrapper (mapping to `DW.LESE_LOG_KNZB`) to print execution and dag run IDs.
*   **`load_path_variables`**: Resolves execution environments and GCS bucket references (mapping to `DW.HOLE_PFAD_KNZB`).

## 6. Parameter and Variable Mapping
In UC4, variables are stored in custom tables/objects (`DW.VARIABLEN`) and fetched using `:SET &VAR = GET_VAR(...)`. In Airflow, this maps directly to `airflow.models.Variable` or runtime task-context mapping.

| UC4 Parameter | Value / Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `GET_VAR('DW.VARIABLEN','DWH_HOME')` | Configuration/Path | `Variable.get("dwh_home_path")` or `GCP_PROJECT_HOME` |
| `GET_VAR('DW.VARIABLEN','HOME')` | Configuration/Path | `Variable.get("home_path")` |
| `GET_VAR('DW.VARIABLEN','ISTNS_HOME')` | Configuration/Path | `Variable.get("istns_home_path")` |
| `SYS_ACT_JPNAME()` | Parent Job Plan Name | `context['dag'].dag_id` |
| `SYS_ACT_JOBNAME()` | Parent Job Name | `context['task_instance'].task_id` |

## 7. Error Handling and Retry Strategy
*   **Include-level execution failure:** Includes in UC4 run within the context of their parent job. If an include script throws an error, the hosting job abends. In Airflow, any Exception raised during the variable configuration or logging lifecycle will correctly raise an error and fail the corresponding execution task.
*   **Sync Objects / Else Behaviour:** No Sync configurations (`SYNCREF`) or postcondition execution rules were specified in these files.

## 8. Developer Notes
*   **Missing Workflow Definitions:** This migration analysis is currently incomplete because the parent orchestrator configurations are missing. To build a runnable Airflow DAG, the developer must obtain the associated `JOBP` (Job Plan), `JOBS_UNIX` (Unix Job scripts execution wrapper), and `EVNT_TIME` (Schedule triggers) XML files.
*   **Global Variables Container:** The references to `DW.VARIABLEN` must be provisioned in the target Airflow Environment's Metadata database as Airflow Variables (e.g., via Cloud Composer Environment Variables or Secrets Manager).
*   **Dynamic Context Logging:** Instead of hardcoding print statements across all migrated jobs, standardise the logging wrapper by utilising Airflow's built-in execution context (`**context`) in Python operators, or within custom plugins.

---

# SECTION 2 — PSEUDOCODE

```python
# ── IMPORTS ──────────────────────────────────────────────
from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException

# ── GCP CONFIGURATION ────────────────────────────────────
# Placeholders for environment configurations
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── DEFAULT ARGS ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 16),
    'retries': 0,
    'email_on_failure': False,
}

# ── VARIABLE AND LOGGING UTILITIES (UC4 INCLUDE MAPS) ────

def hole_pfad_knzb_logic(**context):
    """
    Implements the logic of UC4 Include: DW.HOLE_PFAD_KNZB
    Loads system environment paths from Airflow Variables (DW.VARIABLEN equivalent)
    and pushes them to XCom for downstream tasks.
    """
    logging.info("Starting variable resolution equivalent to JOBI: DW.HOLE_PFAD_KNZB")
    
    try:
        # Retrieve mapped variables from Airflow variable store
        dwh_home = Variable.get("dw_variablen_dwh_home", default_var="gs://YOUR_BUCKET_NAME/dwh_home")
        home = Variable.get("dw_variablen_home", default_var="gs://YOUR_BUCKET_NAME/home")
        istns_home = Variable.get("dw_variablen_istns_home", default_var="gs://YOUR_BUCKET_NAME/istns_home")
        
        logging.info(f"Resolved DWH_HOME: {dwh_home}")
        logging.info(f"Resolved HOME: {home}")
        logging.info(f"Resolved ISTNS_HOME: {istns_home}")
        
        # Share configuration context downstream using XCom
        context['ti'].xcom_push(key='dwh_home', value=dwh_home)
        context['ti'].xcom_push(key='home', value=home)
        context['ti'].xcom_push(key='istns_home', value=istns_home)
        
    except Exception as e:
        raise AirflowException(f"Failed to resolve environment pathing configs: {str(e)}")


def lese_log_knzb_logic(**context):
    """
    Implements the logic of UC4 Include: DW.LESE_LOG_KNZB
    Prints parent workflow and task identifier metadata to logs.
    """
    logging.info("Starting logging capture equivalent to JOBI: DW.LESE_LOG_KNZB")
    
    # Retrieve contextual workflow identifiers (SYS_ACT_JPNAME and SYS_ACT_JOBNAME)
    parent_job_plan = context['dag'].dag_id
    active_job = context['task_instance'].task_id
    
    # Standardised UC4 log format emulation
    logging.info(f"Protokolleintrag: {active_job} innerhalb {parent_job_plan}")


# ── DAG DEFINITION ───────────────────────────────────────
dag_id = "dw_dwh_stamm_parent_workflow_placeholder"

with DAG(
    dag_id=dag_id,
    default_args=DEFAULT_ARGS,
    schedule=None,  # No schedule provided in source files
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['dwh', 'uc4_migration', 'dw_dwh_stamm']
) as dag:

    # ── Task: Resolve Configuration (DW.HOLE_PFAD_KNZB) ──
    task_load_config = PythonOperator(
        task_id="hole_pfad_knzb_resolve",
        python_callable=hole_pfad_knzb_logic,
        provide_context=True,
        doc_md="Fetches path configurations and exposes them downstream."
    )

    # ── Task: Execution Logger (DW.LESE_LOG_KNZB) ────────
    task_write_log = PythonOperator(
        task_id="lese_log_knzb_print",
        python_callable=lese_log_knzb_logic,
        provide_context=True,
        doc_md="Prints system runtime diagnostic log mapping to UC4 output structures."
    )

    # ── Task: Execution Target Placeholder ───────────────
    # Note: Replace this placeholder once core processing task (JOBS_UNIX) XML files are provided.
    task_dwh_process_placeholder = PythonOperator(
        task_id="target_dwh_process_placeholder",
        python_callable=lambda: logging.info("Executing main workload logic (requires JOBS_UNIX XML payload)..."),
    )

    # ── DEPENDENCIES ─────────────────────────────────────────
    # Execution setup sequence mapping to include loading structures
    task_load_config >> task_write_log >> task_dwh_process_placeholder
```
```

---

## 6. Target File Plan & Refactored Target Code

Following the **Folder Integrity Rule**, individual utility Python files are structured under their mirrored paths. Global project constraints have been integrated dynamically using Airflow's model API, and original German print statements are strictly retained as required.

### Target File 1: Configuration Paths
* **Target Relative Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.py`
* **Source:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml`
* **Target Code:**
```python
import logging
from airflow.models import Variable
from airflow.exceptions import AirflowException

def get_path_variables(**context):
    """
    Standard-Include zum Auslesen der Pfad-Variablen aus dem Variablencontainer DW.VARIABLEN.
    Loads configurations from Airflow Variables.
    """
    logging.info("Resolving environment paths (DW.HOLE_PFAD_KNZB equivalent)")
    try:
        # Retrieve path variables dynamically from Environment variables (Global Variable store)
        dwh_home = Variable.get("dw_variablen_dwh_home")
        home = Variable.get("dw_variablen_home")
        istns_home = Variable.get("dw_variablen_istns_home")

        logging.info(f"Loaded DWH_HOME: {dwh_home}")
        logging.info(f"Loaded HOME: {home}")
        logging.info(f"Loaded ISTNS_HOME: {istns_home}")

        # Push to XCom context for use in calling task / shell script execution environments
        context['ti'].xcom_push(key='DWH_HOME', value=dwh_home)
        context['ti'].xcom_push(key='HOME', value=home)
        context['ti'].xcom_push(key='ISTNS_HOME', value=istns_home)

        return {
            "DWH_HOME": dwh_home,
            "HOME": home,
            "ISTNS_HOME": istns_home
        }
    except Exception as e:
        raise AirflowException(f"Error fetching path variables from Airflow metadata store: {str(e)}")
```

### Target File 2: Standard Logging Logger
* **Target Relative Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.py`
* **Source:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml`
* **Target Code:**
```python
import logging

def write_execution_log(**context):
    """
    Schreibt einen einfachen Protokolleintrag in das UC4-Laufprotokoll.
    Emulates the legacy JOBI logger.
    """
    # Extract structural task metadata
    parent_job_plan = context['dag'].dag_id
    active_job = context['task_instance'].task_id

    # OUTPUT/PRINT LITERAL RULE: Verbatim German output is strictly preserved
    logging.info(f"Protokolleintrag: {active_job} innerhalb {parent_job_plan}")
```