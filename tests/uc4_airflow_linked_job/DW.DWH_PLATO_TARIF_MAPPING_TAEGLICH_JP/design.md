# MIGRATION DESIGN DOCUMENT

## File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Migrates the UC4 Unix dummy job definition to an Airflow DAG file. This job runs on Cloud Composer and performs a simple log output of the legacy message to serve as a synchronization milestone in the larger workflow. |

---

## SECTION 1 — VERBATIM MCP OUTPUT (DESIGN & PSEUDOCODE)

```markdown
### ⚠️ INPUT VALIDATION WARNING

Only one UC4 file was provided, and it is a `JOBS_UNIX` file (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`), not an `EVNT_TIME` or `JOBP` file. A complete UC4 workflow typically requires at least one `EVNT_TIME` file (defining the schedule) and a `JOBP` file (defining the job plan/dependencies). 

Because of this, the design document and pseudocode below are structured under the assumption that this single job will be migrated as a standalone, single-task Airflow DAG. All missing scheduling, workflow context, and parent container details have been called out as assumptions and placeholders.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` UC4 object is a UNIX-type dummy task within the Data Warehouse environment (related to Plato Tarif mappings). Its internal script body executes a no-op task (`:print Doing nothinig`), functioning as a synchronization step, milestone, or structural placeholder in the source system. 

It is designed to run on the UNIX host `DWHDWH1P` under the login `DW.UNIX.ISTNS`. As no scheduler (`JSCH`) or event (`EVNT_TIME`) file was provided, the schedule defaults to a manual/external trigger placeholder.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | Active (1) | Dummy milestone job for Plato Tariff mappings. Performs no physical operation. |

### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` *(Flagged: Missing scheduler source file; default to manual)* |
| **start_date** | `datetime(2026, 3, 30)` *(Derived from export timestamp)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Source Active flag is 1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `dw_dwh_dummy_absd_plato_tarife.py` | Project, Region, Cluster placeholders | 0 | N/A | None | None | `False` (`wait_for_completion=True`) | None | Executing as a PySpark script placeholder to maintain architectural alignment with target design. |

### 5. Task Dependency Map
Since only a single job file was provided, the dependency map consists of a single execution block:

```
[start] >> dw_dwh_dummy_absd_plato_tarife >> [end]
```

* **Execution description:** The DAG starts, immediately runs the PySpark-wrapped dummy job via Dataproc, and terminates upon success.

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Placeholder |
|---|---|---|
| **Host** | `DWHDWH1P` | `YOUR_DATAPROC_CLUSTER_NAME` |
| **Login** | `DW.UNIX.ISTNS` | Airflow Connection / Service Account configuration |
| **Estimated Runtime (ERT)** | `11` seconds | Used for monitoring baselines (SLA configuration) |
| **UC4 Object Name** | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | **Sanitized DAG ID:** `dw_dwh_dummy_absd_plato_tarife` |

### 7. Error Handling and Retry Strategy
* **Retry Policy:** The source UC4 XML indicates no active retry loops or custom postconditions. Default retry count is set to `0`.
* **Alarm / Callback:** No notification objects were declared in the source object. No `on_failure_callback` is defined.
* **Sync Object / Concurrency:** No sync boundaries or resources are declared in `<SYNCREF>`. `max_active_runs=1` is implemented at the DAG level as a defensive measure.

### 8. Developer Notes
* **Missing Orchestration Structure:** Because a parent Job Plan (`JOBP`) was not provided, this job has been modeled as a standalone DAG. If this job is actually a node within a larger workflow, it must be integrated into the parent DAG using the task dependency map of that parent's conversion design.
* **No-Op Translation:** The original script executes `:print Doing nothinig`. In the target Airflow/GCP architecture, this has been mapped to a standard `DataprocSubmitJobOperator` referencing a Python/PySpark stub script (`dw_dwh_dummy_absd_plato_tarife.py`) to preserve standard patterns. If Dataproc compute cost is a concern, this can be swapped to an `EmptyOperator` during the Build stage.
* **GCP Infrastructure Placeholders:** The GCP Project, Region, Cluster Name, and GCS Bucket must be configured via Airflow variables or environment configs prior to deployment.

---

## SECTION 2 — PSEUDOCODE

```python
# ─── IMPORTS ──────────────────────────────────────────────────────────────────
# Airflow core modules
from datetime import datetime, timedelta
from airflow import DAG

# GCP Dataproc Operators
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ─── GCP CONFIGURATION ────────────────────────────────────────────────────────
# Placeholders to be replaced by the developer during the Build stage
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# Path to the PySpark stub replacing the UC4 dummy script
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
# Default configuration mapped from UC4 runtime preferences
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
dag = DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    schedule=None,  # No schedule provided in source files
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active=1 in UC4 XHEADER
    description="Transformed from UC4 JOBS_UNIX DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
)

# ─── TASK: DW_DWH_DUMMY_ABSD_PLATO_TARIFE ─────────────────────────────────────
# Configuration payload for the Dataproc PySpark job submission
pyspark_job_config = {
    "reference": {
        "project_id": GCP_PROJECT_ID,
    },
    "placement": {
        "cluster_name": DATAPROC_CLUSTER_NAME,
    },
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_SCRIPT_URI,
    },
}

run_pyspark_job = DataprocSubmitJobOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_config,
    # Generate a dynamic unique job ID suffix to prevent collisions on retry
    job_id="{dag_id}_{run_id}_task_execution".format(
        dag_id="dw_dwh_dummy_absd_plato_tarife",
        run_id="{{ run_id | ds_nodash }}"
    ),
    dag=dag,
)

# ─── DEPENDENCIES ─────────────────────────────────────────────────────────────
# Single task implementation due to standalone context
run_pyspark_job
```
```

---

## SECTION 2 — ADDED CONTEXT

### 1. Job Dependencies & Target Orchestration
* **Upstream Dependencies:** No direct upstreams are specified for this job file alone.
* **Downstream Dependencies:** 
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` — **Not yet migrated**.
  * **Orchestration Plan:** This dummy job is part of the broader daily tariff mapping job chain (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). Once that parent workflow is migrated to Cloud Composer, this task must be wired as an internal task inside the parent DAG (or as a dependency of it if implemented as multiple DAGs), rather than triggering it as a standalone.

### 2. Execution Order
The workflow target orchestration must preserve execution order:
1. `dw_dwh_dummy_absd_plato_tarife` (performs print logging step)
2. Downstream tasks (within `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` parent execution chain).

### 3. Scheduling & Lineage
* **Scheduling:** No schedule is specified on this specific job XML. It is intended to run when triggered by the parent job plan.
* **Lineage Edges:**
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` -> `EXT:DWHDWH1P` (Legacy Host Reference)
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` -> `PACKAGE:DW.UNIX.ISTNS` (Legacy Login Profile)

### 4. External System Replacements
* **Host system (`DWHDWH1P`) & Unix Package Login (`DW.UNIX.ISTNS`):** In the target BigQuery / Cloud Composer architecture, there is no physical Unix host execution required for this step. Instead, it runs natively as an in-memory task within GKE / Cloud Composer workers using standard Airflow operators.

### 5. Environment-Specific Values Classification
To comply with the environment variables policy, we avoid hardcoding or using prose placeholders. Instead, all variables are classified below and resolved programmatically:

1. **GLOBAL (Environment-Wide):**
   * **`GCP_PROJECT`**: The target Google Cloud project. Sourced dynamically using `Variable.get("GCP_PROJECT")`.
   * **`GCP_REGION`**: The target region. Sourced dynamically using `Variable.get("GCP_REGION")`.

2. **JOB-SPECIFIC:**
   * **`LOGIN` / `HOST`**: Mapped conceptually to the execution security context in Cloud Composer. Since this task only performs local logging, no connections or credentials are required.

---

## SECTION 3 — REFINED PRODUCTION-READY TARGET FILE PLAN & CODE

### Folder Integrity Rule Enforcement
The target file matches the original folder structure and is saved in the mirrored directory:
* **Target Relative Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`

### Refined Lightweight Orchestration Strategy
Because this is a pure dummy synchronization job (executing only `:print Doing nothinig` in the source), utilizing a full Dataproc cluster to run a dummy script is highly inefficient. We refine the design to use a native, lightweight Airflow `PythonOperator` that logs the literal message to the DAG execution stdout. 

To satisfy the **OUTPUT/PRINT LITERAL RULE**, the original legacy German/English print string `Doing nothinig` (including the exact spelling) has been preserved character-for-character.

```python
# uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ─── GLOBAL CONFIGURATION (RUNTIME SOURCED) ──────────────────────────────────
# Sourced dynamically via Airflow Variables to prevent prose placeholders
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCP_REGION = Variable.get("GCP_REGION", default_var=None)

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ─── PYTHON CALLABLE (LOGGING TASK) ──────────────────────────────────────────
def log_dummy_action():
    # OUTPUT/PRINT LITERAL RULE: Must match character-for-character, including legacy typo
    logging.info("Doing nothinig")

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
dag = DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    schedule=None,  # Handled via parent workflow trigger (manual/external trigger placeholder)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    description="Transformed from UC4 JOBS_UNIX DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
)

# ─── TASK ─────────────────────────────────────────────────────────────────────
execute_dummy = PythonOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    python_callable=log_dummy_action,
    dag=dag,
)

execute_dummy
```

---

## SECTION 4 — RISKS & MANUAL ACTIONS

* **SOURCE: UNMIGRATED DOWNSTREAM** — `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` — **no candidate found in current package**. The parent scheduling/workflow context cannot be fully established until this downstream parent container is migrated. 
* **Manual Step (Scheduler Configuration):** If this job is intended to run stand-alone or on a schedule, a scheduler object (`JSCH` / `EVNT_TIME`) must be identified and mapped to the target DAG's `schedule` parameter. Currently, it is set to `None` for manual/parent triggering.
* **Global Variables Verification:** Ensure the global Airflow Variable `GCP_PROJECT` and `GCP_REGION` are populated in the target Cloud Composer environment before executing this DAG.