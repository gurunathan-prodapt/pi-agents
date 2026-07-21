# MIGRATION DESIGN DOCUMENT
**Job Name**: `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`  
**Target Platform**: Cloud Composer (Airflow)  
**Migration Pattern**: `UC4_ONLY` (Orchestration Migration)

---

## 1. FILE DISPOSITION TABLE

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Migrates the UC4 dummy Unix Job to an Airflow DAG. Folder structure is preserved exactly per the Folder Integrity Rule. |

---

## 2. VERBATIM MCP TOOL OUTPUT

Below is the verbatim output returned by the `uc4_design_airflow_dag` tool:

```markdown
### WARNING: INCOMPLETE WORKFLOW DETECTED
*   **Validation Flag**: Only one file has been provided, and it is a Unix Job file (`JOBS_UNIX`), not an `EVNT_TIME` or `JOBP` workflow file. 
*   **Impact**: A complete UC4 workflow typically requires at least one `EVNT_TIME` (for scheduling), one `JOBP` (for orchestration structure), and one or more `JOBS_UNIX` files (for task actions). 
*   **Action taken**: To allow progress, the design document and pseudocode below have been generated using default and placeholder assumptions for orchestration and scheduling. Please export and attach the corresponding `JOBP` and `EVNT_TIME` files to complete this blueprint.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
This design covers the migration of `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`, which is a standalone UC4 Unix Job. In the source system, this is defined as a dummy job that performs no execution logic (it only prints a log statement: "Doing nothinig"). It has an estimated runtime (ERT) of 11 seconds. Because no parent Job Plan (`JOBP`) or Schedule (`JSCH` / `EVNT_TIME`) files were provided, this DAG is designed as a single-task standalone pipeline.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `JOBS_UNIX` | `<Active>1</Active>` | Dummy Unix task running on host `|DWHDWH1P|HOST` using credentials `DW.UNIX.ISTNS`. |

## 3. Airflow DAG Properties
| Property | Value | Note |
|---|---|---|
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` | Derived by sanitising the UC4 object name to lowercase and replacing dots with underscores. |
| **schedule** | `None` or `'@daily'` | Missing scheduling file (`EVNT_TIME`). Defaulting to manual or basic daily schedule placeholder. |
| **start_date** | `datetime(2026, 3, 30)` | Placeholder set based on export timestamp metadata. |
| **catchup** | `False` | Standard recommendation to prevent historical backfill storms. |
| **max_active_runs** | `1` | Standard concurrency guard. |
| **is_paused_upon_creation** | `False` | Source object had `<Active>1</Active>`, so normal deployment applies. |
| **default_args** | `{'owner': 'airflow', 'retries': 0}` | No retries or failure alerts were specified in the source Unix object. |

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dwh_dummy_absd_plato_tarife` | `EmptyOperator` | N/A | N/A | 0 | N/A | None | None | N/A | None | Designed as an `EmptyOperator` since the source command is simply `:print Doing nothinig`. |

## 5. Task Dependency Map
```
start >> dwh_dummy_absd_plato_tarife >> end
```
*   **Dependency Logic**: This is a standalone single-task DAG with no upstream or downstream dependencies.

## 6. Parameter and Variable Mapping
| UC4 Parameter / Attribute | Value / Source | Airflow Equivalent / Action |
|---|---|---|
| **UC4 Object Name** | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Sanitised DAG ID: `dw_dwh_dummy_absd_plato_tarife` |
| **Login** | `DW.UNIX.ISTNS` | Replaced by GCP IAM/Service Account binding in Airflow. |
| **Host** | `|DWHDWH1P|HOST` | Replaced by Google Cloud infrastructure. |
| **Script body** | `:print Doing nothinig` | Replaced by an `EmptyOperator` (or a `BashOperator` executing `echo "Doing nothing"`). |

## 7. Error Handling and Retry Strategy
*   **Retries**: No retry settings are defined in the source UC4 XML (`<RUNTIME>` has average metrics but no automated retry logic).
*   **Postcondition Rules**: None detected.
*   **Sync Rules**: No Sync resources (`<Syncs/>` empty) or concurrency restrictions exist in the job.

## 8. Developer Notes
*   **Missing Orchestration context**: Because this file was exported as a single job, it has been mapped as a simple standalone DAG. If this job is triggered inside an overarching workflow, its `EmptyOperator` task should instead be incorporated into a larger master DAG, or called via a `TriggerDagRunOperator`.
*   **Future Upgrades to Dataproc**: If this job is ever converted from a dummy task into an actual PySpark computation task, GCP placeholder parameters (`YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME`, etc.) have been provided in the pseudocode to ease the migration path.
*   **Scheduler Gaps**: Since there is no `EVNT_TIME` configuration, the schedule remains a manual execution placeholder.

---

# SECTION 2 — PSEUDOCODE

```python
# ==============================================================================
# Imports
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# Note: If this task is eventually converted into an actual Dataproc PySpark execution, 
# uncomment the import below:
# from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ==============================================================================
# GCP Configuration Placeholders (Prepared for future PySpark extensions)
# ==============================================================================
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ==============================================================================
# Default Arguments
# ==============================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# DAG Definition
# ==============================================================================
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=default_args,
    description="Converted dummy task from UC4 DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule_interval=None,  # Missing scheduling context (EVNT_TIME file not provided)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active in UC4 <Active>1</Active>
    tags=["migrated_uc4", "dummy_task"],
) as dag:

    # ==========================================================================
    # Task Declarations
    # ==========================================================================
    
    start = EmptyOperator(task_id="start")

    # This task is mapped to EmptyOperator because the source XML contained 
    # only a log print command: ":print Doing nothinig"
    dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id="dwh_dummy_absd_plato_tarife",
        # If this is converted to a PySpark execution in the future, use:
        # operator=DataprocSubmitJobOperator,
        # job={
        #     "reference": {"project_id": GCP_PROJECT_ID},
        #     "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        #     "pyspark_job": {
        #         "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
        #     }
        # },
        # region=DATAPROC_REGION,
    )

    end = EmptyOperator(task_id="end")

    # ==========================================================================
    # Task Dependencies
    # ==========================================================================
    start >> dwh_dummy_absd_plato_tarife >> end
```
```

---

## 3. CONTEXT THE MCP COULD NOT SEE

### A. Job Dependencies & Execution Order
*   **Upstream Dependencies**: None discovered. This UC4 Job acts as an independent orchestration node or starting/synchronization point.
*   **Downstream Dependencies**:
    *   `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` (Status: **Not yet migrated**).
    *   *Wiring Plan on GCP*: Because the downstream job `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated, the orchestration link between these jobs cannot be fully finalized. Once migrated, a cross-DAG dependency should be established using an `ExternalTaskSensor` in the downstream DAG, or this task should be consolidated directly as a step inside the parent workflow.

### B. Scheduling & Variables
*   **Scheduling**: No local scheduling rules or `EVNT_TIME` details are defined in this individual file context. The DAG schedule is set to `None` (manual/triggered runs only) to prevent unwanted runs until its parent workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) is migrated.
*   **Variables**: No variables are registered in `<DYNVALUES>`.

### C. Lineage Edges
*   `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml --[CALLS_HTTP]--> EXT:DWHDWH1P (conf=0.85)`: The legacy execution environment logged a connection host target, which is not required for execution on Composer because this is a dummy task.
*   `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml --[USES_PACKAGE]--> PACKAGE:DW.UNIX.ISTNS (conf=0.80)`: Points to the execution credentials.

### D. External System Replacements
*   None required. This is a pure metadata/dummy task.

---

## 4. ENVIRONMENT-SPECIFIC VALUES CLASSIFICATION

Per the **Environment Values Policy**, we classify all environment values based on their functional role in the target architecture. 

To strictly enforce the **HARD BAN** on prose placeholders (such as `"YOUR_GCP_PROJECT_ID"` or `"YOUR_BUCKET_NAME"`), the production target code retrieves these values dynamically using the Airflow variable storage system:

| Variable / Parameter Name | Classification | Sourcing Method | Target GCP Mapping / Purpose |
| :--- | :--- | :--- | :--- |
| `GCP_PROJECT` | **GLOBAL** | `Variable.get("GCP_PROJECT")` | Identifies the hosting Google Cloud Project ID. |
| `GCP_REGION` | **GLOBAL** | `Variable.get("GCP_REGION")` | The target GCP region for Composer/Dataproc resources. |
| `DATAPROC_CLUSTER` | **GLOBAL** | `Variable.get("DATAPROC_CLUSTER")` | The name of the target Dataproc cluster (for potential future extensions). |
| `GCS_BUCKET` | **GLOBAL** | `Variable.get("GCS_BUCKET")` | Shared GCS bucket for staging scripts or operational logs. |

---

## 5. RISKS & MANUAL ACTIONS

*   **WIRING: NOT YET MIGRATED** — `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is the downstream consumer/workflow that references this dummy task. The target connection must be verified once that parent workflow is migrated.
*   **PROSE PLACEHOLDER REPLACEMENT** — The verbatim output from the MCP contained string placeholders (e.g. `"YOUR_GCP_PROJECT_ID"`). We have resolved this by strictly replacing them with compliant dynamic `Variable.get` calls in the Target File Plan.
*   **PRESERVATION OF SOURCE LOGGING (German & Typos)** — The source script specifies `:print Doing nothinig` (with a typo in "nothinig"), and the source documentation contains the text `Wiederanlauf ohne weitere Maßnahmen möglich`. We preserve these character-for-character inside the target DAG implementation to adhere strictly to the **Output/Print Literal Rule**.

---

## 6. TARGET FILE PLAN & PRODUCTION-READY BQ-AIRFLOW CODE

### Target File: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py`

This code implements the logic verbatim from the source system. It converts the `:print Doing nothinig` script into a `BashOperator` to execute the exact log message and preserves the German recovery comments as docstrings.

```python
# ==============================================================================
# UC4-to-Airflow Converted DAG
# Source File: uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml
# Documented recovery note (German): Wiederanlauf ohne weitere Maßnahmen möglich
# ==============================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# ==============================================================================
# Environment Configuration Retrieval (Strictly complying with Placeholder Ban)
# ==============================================================================
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=None)
DATAPROC_REGION = Variable.get("GCP_REGION", default_var=None)
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var=None)
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET", default_var=None)

# ==============================================================================
# Default Arguments
# ==============================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# DAG Definition
# ==============================================================================
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=default_args,
    description="Converted dummy task from UC4 DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule_interval=None,  # No schedule defined in the individual XML component
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migrated_uc4", "dummy_task"],
) as dag:

    dag.doc_md = """
    ### Recovery Documentation
    Wiederanlauf ohne weitere Maßnahmen möglich
    """

    start = EmptyOperator(task_id="start")

    # This task is mapped to a BashOperator executing the exact character-for-character
    # legacy log output message to strictly respect the Output/Print Literal Rule.
    dwh_dummy_absd_plato_tarife = BashOperator(
        task_id="dwh_dummy_absd_plato_tarife",
        bash_command='echo "Doing nothinig"',
    )

    end = EmptyOperator(task_id="end")

    # ==========================================================================
    # Task Dependencies
    # ==========================================================================
    start >> dwh_dummy_absd_plato_tarife >> end
```