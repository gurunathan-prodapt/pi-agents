# MIGRATION DESIGN DOCUMENT: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

---

## FILE DISPOSITION TABLE

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Cloud Composer DAG representing the UC4 Job orchestration structure. |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py` | PySpark/Python job script executed by Dataproc to preserve the legacy execution behavior. |

---

## VERBATIM MCP TOOL OUTPUT

Below is the verbatim output returned by the `uc4_design_airflow_dag` tool:

### INPUT VALIDATION ALERT

**CRITICAL WARNING:** Only one UC4 XML file was provided, and it is a `JOBS_UNIX` file (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`). 
A complete UC4 migration package typically requires:
1. **At least one `EVNT_TIME` or `JSCH` file** to determine scheduling.
2. **At least one `JOBP` (Job Plan) file** to determine workflow structure, task dependencies, execution order, constraints, and postconditions.
3. **One or more `JOBS_UNIX` files** representing the actual execution tasks.

Because the orchestration context (JOBP) and scheduling details (EVNT_TIME) are missing, this document has been prepared assuming the single job behaves as a standalone DAG. The missing orchestration and scheduling parameters are flagged as mandatory open items in the Developer Notes.

---

### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
The provided UC4 object `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is a Unix Job (`JOBS_UNIX`) designed as a dummy step. Its source script contains only a print statement (`:print Doing nothinig`), indicating it performs no actual data processing and is utilized as a placeholder, synchronization, or routing node within a larger workflow. It has an estimated runtime (ERT) of 11 seconds and is configured to run under the login `DW.UNIX.ISTNS` on host `|DWHDWH1P|HOST`.

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | `<Active>1</Active>` (Active) | A dummy Unix job that prints a placeholder message and runs for ~11 seconds. |

#### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` *(Missing EVNT_TIME/JSCH; scheduled trigger must be defined manually)* |
| **start_date** | `datetime(2026, 3, 30)` *(Placeholder based on UC4 export date)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Source UC4 active state is 1)* |
| **default_args** | `owner: 'airflow'`, `retries: 0`, `retry_delay: timedelta(minutes=5)` |

#### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py` | Project, Region, Cluster placeholders | 0 | None | None | None | `wait_for_completion=True` | None | Maps to a dummy Python/PySpark execution node |

#### 5. Task Dependency Map
Since no parent `JOBP` was supplied, this DAG is designed as a standalone task:

```text
start >> dw_dwh_dummy_absd_plato_tarife >> end
```

*   **start**: An `EmptyOperator` representing the DAG start.
*   **dw_dwh_dummy_absd_plato_tarife**: The primary execution task executing the placeholder script.
*   **end**: An `EmptyOperator` representing successful execution.

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| Object Name | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `dw_dwh_dummy_absd_plato_tarife` (Sanitised DAG ID) |
| Host | `|DWHDWH1P|HOST` | `YOUR_DATAPROC_CLUSTER_NAME` (GCP Target) |
| Login | `DW.UNIX.ISTNS` | Service Account running the Dataproc Job |
| Script Command | `:print Doing nothinig` | Handled via a placeholder PySpark job or a direct PySpark script logging a placeholder statement |

#### 7. Error Handling and Retry Strategy
*   **Retries**: The source job contains no automatic retry rules. Thus, `retries` is set to `0`.
*   **Postconditions**: No postconditions, automatic restarts, or alert actions are defined in the source XML.
*   **Sync Objects**: No active sync references (`<SYNCREF>`) were declared, meaning standard `max_active_runs=1` is sufficient.

#### 8. Developer Notes
*   **Missing Orchestration Context**: This is a single, disconnected job. If this job is part of a larger workflow (e.g., `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` as indicated in the source file path comments), the corresponding `.xml` files for the parent Job Plan (JOBP) must be retrieved to establish dependencies, scheduling parameters, and calendar logic.
*   **Dummy Execution Mapping**: The source script is a UC4 internal script `:print Doing nothinig`. Since it contains no real shell execution commands or Ab Initio commands, it has been mapped to a standard `DataprocSubmitJobOperator` targeting a dummy PySpark script to match the required migration pattern. Alternatively, if no physical PySpark job is preferred for dummy steps, this can be safely simplified to an `EmptyOperator` or standard `PythonOperator` in the final build.
*   **GCP Resource Definitions**: The developer must replace all capitalised placeholders (`YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`) with target environment variables or Airflow Connection configurations.

---

### SECTION 2 — PSEUDOCODE

```python
# ── IMPORTS ──────────────────────────────────────────────
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

# ── GCP CONFIGURATION ────────────────────────────────────
# TODO: Developer must replace these placeholder values with environment configurations
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── DEFAULT ARGS ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Migration of UC4 dummy Unix job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule=None,  # No schedule found in source file; must run on-demand or trigger via upstream DAG
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active=1 in source UC4 XML
    tags=['uc4_migration', 'dw_dwh_plato_tarif_mapping_taeglich_jp']
)

# ── TASKS ────────────────────────────────────────────────
start = EmptyOperator(
    task_id='start',
    dag=dag
)

# PySpark job config representing the migrated dummy task
# Script maps to lowercase, underscored representation of the UC4 job name
pyspark_job_config = {
    'reference': {'project_id': GCP_PROJECT_ID},
    'placement': {'cluster_name': DATAPROC_CLUSTER_NAME},
    'pyspark_job': {
        'main_python_file_uri': f'gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py',
        # Source UC4 script notes: "Doing nothinig" (print statement)
        'args': ['--note', 'placeholder_for_dummy_execution']
    }
}

# Standard Dataproc operator representing the original UNIX job
dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_config,
    # Generate unique Job ID using execution date to avoid collisions
    job_id='dw_dwh_dummy_absd_plato_tarife_{{ ts_nodash }}',
    gcp_conn_id='google_cloud_default',
    # wait_for_completion=True corresponds to standard task synchronization (ActFlg=1 behaviour)
    asynchronous=False, 
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

# ── DEPENDENCIES ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end
```

---

## ADDED CONTEXT AND TARGET ARCHITECTURE

### 1. Job Dependencies & Downstream Integration
* **Upstream**: None explicitly specified. The job is a standalone step inside the parent workflow hierarchy.
* **Downstream**: 
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` — **Not yet migrated**.
* **Cloud Composer Cross-DAG Wiring**:
  Since this dummy step acts as a synchronization or checkpoint step, the downstream job (once migrated) should either consume this DAG's completion using an `ExternalTaskSensor` in its DAG, or this DAG can trigger the downstream DAG using a `TriggerDagRunOperator`.

### 2. Execution Order
Within the context of this standalone job:
* Execution sequence: `start` -> `dw_dwh_dummy_absd_plato_tarife` -> `end`.
* When the complete parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is migrated, this DAG or task will be integrated as a node in that sequence.

### 3. Scheduling
The source context indicates that this job is nested inside the daily mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`).
* **Composer Schedule**: This dummy DAG is configured with `schedule=None`. It is intended to run on-demand or be triggered directly by its upstream orchestrator (e.g., the parent workflow's orchestrator DAG).

### 4. Lineage Edges
* `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` --[CALLS_HTTP]--> `EXT:DWHDWH1P` (Represents the host/target node mapping).
* `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` --[USES_PACKAGE]--> `PACKAGE:DW.UNIX.ISTNS` (Represents the legacy execution identity).

---

## ENVIRONMENT-SPECIFIC VALUES

Following the Environment Variable Policy, we classify and extract values based on their targets:

### 1. GLOBAL (Environment-Wide Infrastructure Constants)
The values below identify the underlying GCP infrastructure and must be resolved dynamically at runtime using Airflow's config store (`Variable.get`).

* **`GCP_PROJECT`**: The target GCP project ID hosting Cloud Composer and Dataproc.
  * *Sourcing Method*: `Variable.get("GCP_PROJECT")`
* **`GCP_REGION`**: The target region where computing resources are provisioned.
  * *Sourcing Method*: `Variable.get("GCP_REGION")`
* **`DATAPROC_CLUSTER`**: The name of the Dataproc cluster used to execute PySpark tasks.
  * *Sourcing Method*: `Variable.get("DATAPROC_CLUSTER")`
* **`GCS_BUCKET`**: The target Cloud Storage bucket storing scripts and execution files.
  * *Sourcing Method*: `Variable.get("GCS_BUCKET")`

### 2. JOB-SPECIFIC (Parameters Particular to This Workload)
The values below are unique to this specific migration and are set statically or via local DAG parameters.

* **`dag_id`**: `'dw_dwh_dummy_absd_plato_tarife'`
* **`main_python_file_uri`**: `f'gs://{GCS_BUCKET}/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py'`

---

## TARGET CODE IMPLEMENTATION

### 1. Airflow DAG Script
Below is the revised and integrated target DAG script mapping the global variables per policy and pointing to the correct relative storage paths.

```python
# ── IMPORTS ──────────────────────────────────────────────
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

# ── GLOBAL CONFIGURATION (ENVIRONMENT-WIDE CONSTANTS) ─────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── DEFAULT ARGS ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Migration of UC4 dummy Unix job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule=None,  # Handled daily by parent workflow execution chain
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'dw_dwh_plato_tarif_mapping_taeglich_jp']
)

# ── TASKS ────────────────────────────────────────────────
start = EmptyOperator(
    task_id='start',
    dag=dag
)

pyspark_job_config = {
    'reference': {'project_id': GCP_PROJECT},
    'placement': {'cluster_name': DATAPROC_CLUSTER},
    'pyspark_job': {
        'main_python_file_uri': f'gs://{GCS_BUCKET}/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py',
        'args': ['--note', 'placeholder_for_dummy_execution']
    }
}

dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    project_id=GCP_PROJECT,
    region=GCP_REGION,
    job=pyspark_job_config,
    job_id='dw_dwh_dummy_absd_plato_tarife_{{ ts_nodash }}',
    gcp_conn_id='google_cloud_default',
    asynchronous=False, 
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

# ── DEPENDENCIES ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end
```

### 2. PySpark Execution script
This script preserves the original print output exactly as specified under the **OUTPUT/PRINT LITERAL RULE** (retaining the misspelled literal `"Doing nothinig"`).

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Target PySpark placeholder script representing the Unix dummy task
# Folder Location: uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py
#

import sys

def main():
    # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of the legacy print message
    print("Doing nothinig")
    sys.exit(0)

if __name__ == "__main__":
    main()
```

---

## RISKS & MANUAL ACTIONS

* **WIRING: NOT YET MIGRATED** — Downstream job `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` — The overall orchestration sequence and DAG dependency mapping cannot be finalized on Cloud Composer until the downstream workflow XML configuration is migrated.
* **Orchestration Context (JOBP) Gap** — Since only the `JOBS_UNIX` object was scanned, the parent schedule and execution parameters must be verified during target system deployment. Ensure that the parent DAG triggers `dw_dwh_dummy_absd_plato_tarife` programmatically.