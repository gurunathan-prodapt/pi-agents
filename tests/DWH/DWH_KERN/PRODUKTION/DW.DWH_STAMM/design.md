# MIGRATION DESIGN DOCUMENT — JOBP ASSEMBLED WORKFLOW
**Target Platform:** BigQuery & Cloud Composer

---

# SECTION 1 — VERBATIM MCP TOOL OUTPUT

Below is the verbatim design output produced by the UC4-to-Airflow transformation mapping engine for the orchestrating Job Plan. 

*(Note: The custom logical handlers representing start/end operations, state checking, and path calculations within the individual job scripts are detailed in the sections that follow.)*

```
=== Result for DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml ===
Here is the comprehensive Design Document and Pseudocode blueprint for converting the UC4 XML workflow into an Apache Airflow DAG.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The **DW.DWH_STAMM_KNZB_ABGL_JP** workflow is a daily UC4 Job Plan (`JOBP`) responsible for reconciling customer number and basic access master data (Kundennummer-/Basiszugangs-Stammdaten, or **KNZB**) between the `ISTNS` source system and the Core Data Warehouse layer (DWH-Kernschicht). It acts as a processing boundary containing sequence tasks to orchestrate the start and end of this master data alignment process.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_STAMM_KNZB_ABGL_JP` | `JOBP` (Job Plan) | Active (`<Active>1</Active>`) | Daily master data alignment workflow for KNZB |
| `DW.DWH_STAMM_KNZB_ABGL_START_JS` | `JOBS` (Referenced Job)* | Active (Assumed) | Start wrapper job for the alignment process |
| `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` | `JOBS` (Referenced Job)* | Active (Assumed) | End wrapper job for the alignment process |

*\*Note: These referenced Unix/Script jobs were identified from the `<task>` list inside the `<JobpStruct>` of the Job Plan, but their individual XML files were not provided in the source dump. Default configurations are applied.*

## 3. Airflow DAG Properties
| Property | Value | Note |
|---|---|---|
| **DAG ID** | `dw_dwh_stamm_knzb_abgl_jp` | Sanitised from `DW.DWH_STAMM_KNZB_ABGL_JP` |
| **Schedule (cron)** | `None` | No schedule context (e.g., `EVNT_TIME` or `JSCH`) was provided in the input; assuming manual trigger or external orchestration. |
| **Start Date** | `datetime(2026, 7, 16)` | Set dynamically to the export date metadata. |
| **Catchup** | `False` | Recommended standard practice. |
| **Max Active Runs** | `1` | Enforces execution serialization. |
| **Is Paused Upon Creation** | `False` | Maps directly from the UC4 `<Active>1</Active>` status. |
| **Default Args** | `{'owner': 'DWH_KERN', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Defaults derived from workflow parameters. |

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `start` | `EmptyOperator` | N/A | N/A | 0 | N/A | None | None | No | None | Map of UC4 `<START>` |
| `dw_dwh_stamm_knzb_abgl_start_js` | `DataprocSubmitJobOperator` | `dw_dwh_stamm_knzb_abgl_start_js.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME`, etc. | 0 | N/A | None | None | No | None | PySpark job mapping for referenced task |
| `dw_dwh_stamm_knzb_abgl_ende_js` | `DataprocSubmitJobOperator` | `dw_dwh_stamm_knzb_abgl_ende_js.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME`, etc. | 0 | N/A | None | None | No | None | PySpark job mapping for referenced task |
| `end` | `EmptyOperator` | N/A | N/A | 0 | N/A | None | None | No | None | Map of UC4 `<END>` |

## 5. Task Dependency Map
The execution flow is mapped as a strict linear dependency chain matching the structural columns (`Col="1"` through `Col="4"`) in the UC4 Job Plan structure:

```mermaid
graph LR
    start --> dw_dwh_stamm_knzb_abgl_start_js
    dw_dwh_stamm_knzb_abgl_start_js --> dw_dwh_stamm_knzb_abgl_ende_js
    dw_dwh_stamm_knzb_abgl_ende_js --> end
```

**Plain English Flow Description:**
1. The DAG initiates with the dummy `start` node.
2. The start processing script `dw_dwh_stamm_knzb_abgl_start_js` is submitted via Dataproc.
3. Upon success, the end processing and validation script `dw_dwh_stamm_knzb_abgl_ende_js` runs.
4. The workflow concludes at the dummy `end` node.

## 6. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_STAMM_KNZB_ABGL_JP` | Workflow ID | `dw_dwh_stamm_knzb_abgl_jp` (DAG ID) |
| `DW.DWH_STAMM_KNZB_ABGL_START_JS` | Task Lnr 2 | `dw_dwh_stamm_knzb_abgl_start_js` (Task ID) |
| `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` | Task Lnr 3 | `dw_dwh_stamm_knzb_abgl_ende_js` (Task ID) |

## 7. Error Handling and Retry Strategy
- **Retry Strategy:** No explicit retries or postconditions were defined in the root plan XML, mapping defaults of `retries=0` to all tasks.
- **Sync Object Behavior:** No `<SYNCREF>` elements with custom `Else` behaviors were defined. Standard `max_active_runs=1` is applied to avoid overlapping executions.

## 8. Developer Notes
* **Missing XML Definitions:** The individual child job definitions (`DW.DWH_STAMM_KNZB_ABGL_START_JS` and `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`) were not included in the source file. They are mapped to `DataprocSubmitJobOperator` assuming standard PySpark migration logic.
* **GCP Placeholders:** Developers must populate all `YOUR_GCP_*` variables inside the Airflow execution environment (e.g., using Airflow Variables or Environment Variables).
* **Scheduling:** Because no time event/schedule XML was provided, the DAG schedule is set to `None`. This should be updated if an external trigger schedule is discovered.

---

# SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
# TODO: Replace these placeholders with your actual environment variables or Airflow Variables
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "DWH_KERN",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 16),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id="dw_dwh_stamm_knzb_abgl_jp",
    default_args=default_args,
    description="Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht",
    schedule_interval=None,  # Manual or external trigger (No UC4 schedule provided)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False, # Mapped from <Active>1</Active>
)

# ── Task: start ──────────────────────────────────────────
# UC4 Column 1, Line 1 (START)
start = EmptyOperator(
    task_id="start",
    dag=dag,
)

# ── Task: dw_dwh_stamm_knzb_abgl_start_js ────────────────
# UC4 Column 2, Line 2 (JOBS)
# Assumed to map to a migrated PySpark script for KNZB Alignment Start
pyspark_job_start = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_stamm_knzb_abgl_start_js.py"
    },
}

dw_dwh_stamm_knzb_abgl_start_js = DataprocSubmitJobOperator(
    task_id="dw_dwh_stamm_knzb_abgl_start_js",
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_start,
    # Dynamically structure run name: [dag_id]__[task_id]__[sanitised_execution_date]
    job_id="dw_dwh_stamm_knzb_abgl_jp_start_{{ ts_nodash }}",
    dag=dag,
)

# ── Task: dw_dwh_stamm_knzb_abgl_ende_js ─────────────────
# UC4 Column 3, Line 3 (JOBS)
# Assumed to map to a migrated PySpark script for KNZB Alignment End
pyspark_job_ende = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_stamm_knzb_abgl_ende_js.py"
    },
}

dw_dwh_stamm_knzb_abgl_ende_js = DataprocSubmitJobOperator(
    task_id="dw_dwh_stamm_knzb_abgl_ende_js",
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_ende,
    # Dynamically structure run name: [dag_id]__[task_id]__[sanitised_execution_date]
    job_id="dw_dwh_stamm_knzb_abgl_jp_ende_{{ ts_nodash }}",
    dag=dag,
)

# ── Task: end ────────────────────────────────────────────
# UC4 Column 4, Line 4 (END)
end = EmptyOperator(
    task_id="end",
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
start >> dw_dwh_stamm_knzb_abgl_start_js >> dw_dwh_stamm_knzb_abgl_ende_js >> end
```

---

# SECTION 3 — FILE DISPOSITION TABLE

All source files identified in the pre-collected context are preserved. The logical boundaries and script code have been integrated as follows:

| Source File Path | Target File Path / Task ID | Disposition | Purpose / Mapping Justification |
| :--- | :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml` | `dags/dw_dwh_stamm_knzb_abgl_jp.py` | **Target File** | Orchestrates the task flow of the primary job plan. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_START_JS.xml` | `tasks/dw_dwh_stamm_knzb_abgl_start_js.py` (referenced by DAG task) | **Target File** | Translates UC4 `:PUT_VAR`, `:GET_VAR`, checks status locks, and maps script logic directly to Python. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.xml` | `tasks/dw_dwh_stamm_knzb_abgl_ende_js.py` (referenced by DAG task) | **Target File** | Releases processing flags, writes final status logs. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml` | Merged into `tasks/dw_dwh_stamm_knzb_abgl_start_js.py` and `tasks/dw_dwh_stamm_knzb_abgl_ende_js.py` | **Merged** | Imported/Inlined as helper functions to extract global configurations. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml` | Merged into `tasks/dw_dwh_stamm_knzb_abgl_start_js.py` and `tasks/dw_dwh_stamm_knzb_abgl_ende_js.py` | **Merged** | Inlined as a Python logging statement preserving the original output structures. |

---

# SECTION 4 — ADDITIONAL CONTEXT & WORKFLOW INTEGRATION

### 1. Job Dependencies & Lineage
* **Upstream:** None discovered in context.
* **Downstream:** None discovered in context.
* **Lineage Edges:** The start (`START_JS`) and end (`ENDE_JS`) tasks depend on `includes/DW.HOLE_PFAD_KNZB` for path lookup and `includes/DW.LESE_LOG_KNZB` to write run indicators to logs. These are migrated natively in python.

### 2. Execution Order & Scheduling
The execution sequence matches the sequential columns mapped inside `DW.DWH_STAMM_KNZB_ABGL_JP.xml`:
1. `START` -> Dummy task initiating the run.
2. `dw_dwh_stamm_knzb_abgl_start_js` -> Checks if alignment status is `"GESPERRT"` (LOCKED). If locked, cancels run. Otherwise sets status to `"LAEUFT"` (RUNNING).
3. `dw_dwh_stamm_knzb_abgl_ende_js` -> Resets status to `"FREI"` (FREE) and logs successful completion.
4. `END` -> Dummy task indicating completion.

**Scheduling:** No timer events exist in the XML structure; the migrated DAG will be initialized as `schedule_interval=None` (triggered externally or manually via Cloud Composer API).

---

# SECTION 5 — ENVIRONMENT VARIABLES & VARIABLES CLASSIFICATION

As per the environment classification guidelines, the legacy variable containers and environment parameters are mapped as follows:

### 1. Global Variables (Shared Environment Infrastructure)
These variables will be mapped to **Airflow Variables** and used globally across all workflows:

* `GCP_PROJECT`: Target Google Cloud Project ID.
* `GCS_BUCKET`: Dataproc/Airflow shared object storage bucket.
* `BQ_DATASET`: Target BigQuery Core Schema/Dataset (mapped from DWH-Kernschicht concept).
* `DWH_HOME`: Path referencing core environment resources (mapped from variable container lookup `GET_VAR('DW.VARIABLEN','DWH_HOME')`).
* `HOME`: Home directory path for user executions (mapped from `GET_VAR('DW.VARIABLEN','HOME')`).
* `ISTNS_HOME`: Connection/interface path for the ISTNS source system (mapped from `GET_VAR('DW.VARIABLEN','ISTNS_HOME')`).

### 2. Job-Specific Variables (Workflow-State Scope)
These variables track run-time state flags and execution locks for the KNZB process. To maintain state dynamically across independent task processes on Google Cloud, they will be saved as **Airflow Variables** inside the script files:

* `DW.VARIABLEN_KNZB -> ABGLEICH_STATUS`: Tracks execution status (`"GESPERRT"`, `"LAEUFT"`, `"FREI"`). Mapped as Airflow variable `dw_variablen_knzb_abgleich_status`.
* `DW.VARIABLEN_KNZB -> LETZTER_LAUF`: Stores the date of the last successful alignment run. Mapped as Airflow variable `dw_variablen_knzb_letzter_lauf`.

---

# SECTION 6 — TARGET FILE PLAN & RE-ENGINEERED CODE

## 1. Main Airflow DAG Plan
**Target File Path:** `dags/dw_dwh_stamm_knzb_abgl_jp.py`
This DAG orchestrates the logical workflow steps, using the Cloud Composer `PythonOperator` to execute the migrated task scripts locally or on Cloud SDK resources.

```python
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Resolve Global Target Configurations
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "DW_DWH_STAMM")

# Inline execution functions directly as Tasks
from tasks.dw_dwh_stamm_knzb_abgl_start_js import run_start_js
from tasks.dw_dwh_stamm_knzb_abgl_ende_js import run_ende_js

default_args = {
    "owner": "DWH_KERN",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 16),
    "retries": 0,
}

with DAG(
    dag_id="dw_dwh_stamm_knzb_abgl_jp",
    default_args=default_args,
    description="Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    execute_start_js = PythonOperator(
        task_id="dw_dwh_stamm_knzb_abgl_start_js",
        python_callable=run_start_js,
    )

    execute_ende_js = PythonOperator(
        task_id="dw_dwh_stamm_knzb_abgl_ende_js",
        python_callable=run_ende_js,
    )

    end = EmptyOperator(task_id="end")

    # Dependency Flow
    start >> execute_start_js >> execute_ende_js >> end
```

## 2. Re-Engineered Task Code

### Task 1: Start Align Task
**Target File Path:** `tasks/dw_dwh_stamm_knzb_abgl_start_js.py`
This script checks the status of the variable lock container, sets status variables to running, and registers execution. All logging statements match the source vocabulary.

```python
import logging
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

logger = logging.getLogger("airflow.task")

def include_hole_pfad_knzb():
    """
    Simulates the include: :inc DW.HOLE_PFAD_KNZB
    Extracts global paths and home directions.
    """
    dwh_home = Variable.get("DWH_HOME", default_var="/home/gurunathan_t/clean_migration_dataset")
    home = Variable.get("HOME", default_var="/home/gurunathan_t")
    istns_home = Variable.get("ISTNS_HOME", default_var="/home/gurunathan_t/istns")
    return dwh_home, home, istns_home

def include_lese_log_knzb(adm_job, adm_jp):
    """
    Simulates the include: :inc DW.LESE_LOG_KNZB
    Writes log entries using the exact German output translation schema.
    """
    # OUTPUT/PRINT LITERAL RULE: Must output exact German text string from legacy log
    logger.info(f"Protokolleintrag: {adm_job} innerhalb {adm_jp}")

def run_start_js(**context):
    # Load Paths
    dwh_home, home, istns_home = include_hole_pfad_knzb()
    
    # Task specific indicators
    dwh_job_kennung = "STAMM_KNZB_ABGL"
    lauf_datum = datetime.now().strftime("%Y%m%d")
    
    # Retrieve lock status variable
    abgleich_status = Variable.get("dw_variablen_knzb_abgleich_status", default_var="FREI")
    
    # Check execution constraints
    if abgleich_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: Must maintain German log string character for character
        logger.error(f"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung")
        raise AirflowFailException("Processing locked downstream. Stopping execution.")
        
    # Write status indicators back to Variables Store
    Variable.set("dw_variablen_knzb_abgleich_status", "LAEUFT")
    Variable.set("dw_variablen_knzb_letzter_lauf", lauf_datum)
    
    # Log script completion
    include_lese_log_knzb("DW.DWH_STAMM_KNZB_ABGL_START_JS", "DW.DWH_STAMM_KNZB_ABGL_JP")
```

### Task 2: End Align Task
**Target File Path:** `tasks/dw_dwh_stamm_knzb_abgl_ende_js.py`
This script releases the processing variables upon safe run termination and logs the status updates.

```python
import logging
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

def include_hole_pfad_knzb():
    """
    Simulates the include: :inc DW.HOLE_PFAD_KNZB
    """
    dwh_home = Variable.get("DWH_HOME", default_var="/home/gurunathan_t/clean_migration_dataset")
    home = Variable.get("HOME", default_var="/home/gurunathan_t")
    istns_home = Variable.get("ISTNS_HOME", default_var="/home/gurunathan_t/istns")
    return dwh_home, home, istns_home

def include_lese_log_knzb(adm_job, adm_jp):
    """
    Simulates the include: :inc DW.LESE_LOG_KNZB
    """
    # OUTPUT/PRINT LITERAL RULE: Original text string must be printed verbatim
    logger.info(f"Protokolleintrag: {adm_job} innerhalb {adm_jp}")

def run_ende_js(**context):
    # Load paths
    dwh_home, home, istns_home = include_hole_pfad_knzb()
    
    # Retrieve current date representation
    lauf_datum = Variable.get("dw_variablen_knzb_letzter_lauf", default_var="UNKNOWN")
    
    # Release Lock Status Variable
    Variable.set("dw_variablen_knzb_abgleich_status", "FREI")
    
    # OUTPUT/PRINT LITERAL RULE: Verbatim print translation matching original execution logs
    logger.info(f"KNZB-Stammdatenabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # Print execution summary log
    include_lese_log_knzb("DW.DWH_STAMM_KNZB_ABGL_ENDE_JS", "DW.DWH_STAMM_KNZB_ABGL_JP")
```

---

# SECTION 7 — RISKS & MANUAL ACTIONS

1. **Airflow Variables Seed Requirement:** Prior to running this workflow, the following Variable must exist in the target Airflow environment configuration:
   * Key: `dw_variablen_knzb_abgleich_status`, Value: `"FREI"` (Set as `"GESPERRT"` manually if execution needs to be blocked).
2. **Global Paths Seeding:** Ensure that paths mapping `DWH_HOME`, `HOME`, and `ISTNS_HOME` are seeded in Cloud Composer environment variables or Airflow Variables to avoid failing back to defaults.