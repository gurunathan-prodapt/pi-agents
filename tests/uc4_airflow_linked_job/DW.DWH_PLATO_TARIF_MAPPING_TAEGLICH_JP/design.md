# MIGRATION DESIGN DOCUMENT: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Migrates the UC4 JOBS_UNIX dummy object into an Airflow DAG file in Cloud Composer. Since this is a placeholder task, it preserves the folder structure and implements execution triggers/logs while printing the original script's literal text. |

---

## SECTION 1 — VERBATIM MCP DESIGN DOCUMENT OUTPUT

The following design analysis has been generated directly from the source code via the `uc4_design_airflow_dag` tool:

### WARNING: INCOMPLETE WORKFLOW DETECTED
*Only one UC4 XML file was provided, and it is a `JOBS_UNIX` object (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`). A complete UC4 workflow typically requires at least one `EVNT_TIME` (for scheduling), one `JOBP` (for workflow orchestration and dependencies), and multiple `JOBS_UNIX` files (for actual execution steps). Because these wrapping files are missing, certain attributes (such as workflow scheduling, parent-child task execution chains, calendar constraints, task-level start times, and sync-object constraints) cannot be definitively extracted and are represented with placeholders/default values in this document.*

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The UC4 object analyzed is a single Unix Job (`JOBS_UNIX`) named `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`. Based on its title ("dummy") and script contents (`:print Doing nothinig`), this is a placeholder or dummy job used to mark milestones, coordinate execution steps, or represent an empty task node in a larger workflow. It has no actual execution payload. In the migrated Airflow environment, it will run as a placeholder PySpark job or `EmptyOperator` depending on architectural preference; for the sake of completeness, it is mapped here as a standardized Google Cloud Dataproc PySpark execution step running a skeleton PySpark script.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `JOBS_UNIX` | Active (`1`) | Dummy Unix job that executes a print statement; used as an execution placeholder. |

### 3. Airflow DAG Properties
| Property | Value | Note |
| :--- | :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` | Sanitized from original UC4 object name. |
| **schedule** | `None` | Set to `None` as no `EVNT_TIME` trigger file was provided. |
| **start_date** | `datetime(2026, 3, 30)` | Placeholder derived from UC4 export timestamp metadata. |
| **catchup** | `False` | Recommended default to prevent backfilling historic dummy runs. |
| **max_active_runs** | `1` | Default safety limit preventing concurrent executions. |
| **is_paused_upon_creation**| `False` | Mapped from UC4 `<Active>1</Active>`. The DAG will deploy active. |
| **default_args** | `{ 'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5) }` | No explicit retries were specified in the source `JOBS_UNIX` definition. |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `dw_dwh_dummy_absd_plato_tarife.py` | Project, Region, Cluster placeholders | `0` | N/A | None | None | `False` (`wait_for_completion=True`) | None | No earliest start time or calendar was defined in the source object (requires JOBP analysis). |

### 5. Task Dependency Map
Since only a single Unix Job was provided without a parent `JOBP` workflow, the dependency map contains only this single task:

```
[start_placeholder] >> dw_dwh_dummy_absd_plato_tarife >> [end_placeholder]
```

*   **Execution Flow:** The DAG starts, immediately runs the dummy PySpark task, and terminates.

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Placeholder |
| :--- | :--- | :--- |
| `UC4 Object Name` | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Sanitized DAG ID: `dw_dwh_dummy_absd_plato_tarife` |
| `HostDst` | `\|DWHDWH1P\|HOST` | Mapped to Dataproc Cluster: `YOUR_DATAPROC_CLUSTER_NAME` |
| `Login` | `DW.UNIX.ISTNS` | Mapped to GCP Service Account/Execution Role credentials |
| `Ert` (Estimated Runtime) | `11` seconds | Represented as metadata for testing timeouts if desired |

### 7. Error Handling and Retry Strategy
*   **Retries:** No postconditions or explicit retry mechanisms are defined in this XML payload. Default retries are set to `0`.
*   **ENDED_SKIPPED Handling:** No custom skip states or postconditions exist on this job object.
*   **Sync Object Analysis:** No `<SYNCREF>` synchronization elements are populated in this object. `max_active_runs=1` on the DAG is sufficient to prevent overlapping runs.

### 8. Developer Notes
*   **GCP Placeholders:** You must replace all `YOUR_` prefixed placeholders in the configuration block (Project ID, Region, Cluster Name, Bucket Name) with environment-specific values in GCP.
*   **Missing Workflow Context:** The source file contains no Ab Initio graph reference (such as `r_ai_start` with `-j` and `-k` parameters) because it is a native UC4 command job executing only `:print Doing nothinig`. Consequently:
    *   The PySpark script `dw_dwh_dummy_absd_plato_tarife.py` will contain only skeleton dummy log execution code.
    *   If this task is intended to trigger another process, that logic must be manually injected.
    *   *Note: Under the Output/Print Literal Rule, the string `Doing nothinig` has been explicitly preserved with its original spelling.*
*   **Trigger and Schedule:** No execution frequency was available. This DAG is configured to run on-demand (`schedule=None`) until an `EVNT_TIME` schedule pattern is provided.

---

## SECTION 2 — PSEUDOCODE

```python
# ==============================================================================
# ── Imports ───────────────────────────────────────────────────────────────────
# ==============================================================================
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# ==============================================================================
# ── GCP Configuration (GLOBAL) ────────────────────────────────────────────────
# ==============================================================================
# Standardized environment variables loaded dynamically at runtime (no literal fallbacks)
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET")

PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
dag = DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=default_args,
    description="Airflow representation of migrated UC4 dummy job DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule=None,  # Handled via downstream orchestration sensors / parent trigger
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False, # Mapped from Active=1
    tags=["migrated_uc4", "dw_dwh_dummy_absd_plato_tarife"],
)

# ==============================================================================
# ── Tasks ─────────────────────────────────────────────────────────────────────
# ==============================================================================

# Start and end boundaries for the DAG run execution
start_run = EmptyOperator(
    task_id="start",
    dag=dag,
)

# Task: dw_dwh_dummy_absd_plato_tarife
# Since this job does nothing other than print, we implement a BashOperator 
# to run the exact legacy print statement, satisfying the Output/Print Literal Rule.
run_dummy_bash_job = BashOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    bash_command="echo 'Doing nothinig'", # Verbatim legacy print statement
    dag=dag,
)

end_run = EmptyOperator(
    task_id="end",
    dag=dag,
)

# ==============================================================================
# ── Dependencies ──────────────────────────────────────────────────────────────
# ==============================================================================
start_run >> run_dummy_bash_job >> end_run
```

---

## SECTION 3 — CONTEXT AND PIPELINE INTEGRATION

### 1. Job Dependencies
Based on the pre-collected metadata, this job contains the following downstream dependency:
*   **Downstream Consumer (Cross-Job Hand-off):** 
    *   `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` — **Not yet migrated**
*   **Wiring Method on BigQuery / Composer:**
    *   Because `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated, the orchestration link cannot be fully compiled. 
    *   When both components are deployed, this should be wired via Airflow's native task chains if they are combined into the same parent DAG (which is highly recommended given the `uc4_airflow_linked_job` folder categorization) or via a `TriggerDagRunOperator` if they remain decoupled.

### 2. Execution Order and Scheduling
*   **Execution sequence:** `start` -> `dw_dwh_dummy_absd_plato_tarife` -> `end`.
*   **Schedule:** This job is triggered as part of the daily workflow of its parent chain `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`. It does not have an independent calendar trigger; its scheduling is inherited.

### 3. Lineage Edges & External Targets
*   `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` -> USES_PACKAGE -> `PACKAGE:DW.UNIX.ISTNS` (Login execution role)
*   `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` -> CALLS_HTTP -> `EXT:DWHDWH1P` (Represented by Host Target `|DWHDWH1P|HOST`)
*   **Target Migration Strategy:** 
    *   The UNIX executor `|DWHDWH1P|HOST` maps to the Cloud Composer environment worker node executing the task.
    *   The package execution login/role `DW.UNIX.ISTNS` maps to the corresponding Google Service Account assigned to the Cloud Composer environment.

---

## SECTION 4 — ENVIRONMENT VALUES CLASSIFICATION

Every variable required by the migrated job is classified below in accordance with the Environment Variable Policy. No prose placeholders or literal fallbacks are permitted.

### 1. GLOBAL (Environment-Wide Infrastructure Constants)
The values below remain the same across all jobs in a given environment (Dev/Test/Prod). These are retrieved at runtime from Airflow's config store:

*   **`GCP_PROJECT`**
    *   *Purpose:* Identifies the target Google Cloud project.
    *   *Resolution Method:* `Variable.get("GCP_PROJECT")`
*   **`GCP_REGION`**
    *   *Purpose:* Target region for execution and resources.
    *   *Resolution Method:* `Variable.get("GCP_REGION")`
*   **`DATAPROC_CLUSTER`**
    *   *Purpose:* Name of the shared Dataproc execution cluster.
    *   *Resolution Method:* `Variable.get("DATAPROC_CLUSTER")`
*   **`GCS_BUCKET`**
    *   *Purpose:* Name of the shared Cloud Storage bucket used for script and log storage.
    *   *Resolution Method:* `Variable.get("GCS_BUCKET")`

### 2. JOB-SPECIFIC Constants
These parameters are specific only to this task and do not vary by environment. They are declared in-line inside the task config definition:

*   **`uc4_object`**
    *   *Value:* `"DW.DWH_DUMMY_ABSD_PLATO_TARIFE"`
*   **`uc4_login`**
    *   *Value:* `"DW.UNIX.ISTNS"`
*   **`estimated_runtime_sec`**
    *   *Value:* `11`

---

## SECTION 5 — RISKS & MANUAL ACTIONS

1. **DOWNSTREAM UNMIGRATED WORKFLOW**
   * *Description:* The downstream job `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` is not yet migrated.
   * *Mitigation:* The end task on this DAG cannot trigger the downstream process automatically until `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is deployed. A human operator must manually coordinate or link these DAGs once the downstream components are ready.
2. **VERBATIM TEXT PRESERVATION**
   * *Description:* The original print statement contains a typo: `Doing nothinig`.
   * *Mitigation:* In compliance with the Output/Print Literal Rule, this exact string has been kept as `"Doing nothinig"`. Do not attempt to fix or alter this spelling during deployment, as downstream log-parsing processes might depend on this exact signature.

---

## SECTION 6 — FOLDER INTEGRITY & COMPLIANCE

*   **Folder Integrity Rule:** In compliance with the Folder Integrity Rule, the target file `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` is allocated to the exact relative folder structure matching its source:
    *   *Source path:* `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
    *   *Target path:* `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`
*   **Output/Print Literal Rule:** The legacy shell script execution logic (`:print Doing nothinig`) was kept character-for-character within the Python `BashOperator` execution block to prevent breaking any automated status checking.