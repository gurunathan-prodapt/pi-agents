# MIGRATION DESIGN DOCUMENT
**Target Platform:** BigQuery & Cloud Composer (Airflow)
**Job Name:** `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`
**Job Type:** JOB

---

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Migrated 1:1 to an Airflow DAG containing a single Python placeholder task mimicking the legacy UC4 JOBS_UNIX dummy execution. |

---

## SECTION 1 — DESIGN DOCUMENT (VERBATIM MCP OUTPUT)

### 1. Overview
The `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` object is a single, isolated Unix Job (JOBS_UNIX) within the UC4 environment. Based on the extracted script body (`:print Doing nothinig`), this is a utility or "dummy" task that performs no operational system or data processing steps, likely acting as a structural milestone, synchronisation point, or placeholder within a larger parent Job Plan (`JOBP`). Due to the missing parent objects, this design maps it as an independent Airflow DAG containing a single placeholder task.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `JOBS_UNIX` | Active (`<Active>1</Active>`) | Dummy task performing no operations. Prints a status message. |

### 3. Airflow DAG Properties
| Property | Value | Note |
| :--- | :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` | Sanitised UC4 object name |
| **schedule** | `None` | No `EVNT_TIME` provided; must be triggered manually or via upstream Dataset/Trigger |
| **start_date** | `datetime(2026, 3, 30)` | Derived from the export metadata timestamp |
| **catchup** | `False` | Recommended default to prevent historical execution backfill |
| **max_active_runs** | `1` | Default concurrency protection |
| **is_paused_upon_creation** | `False` | Deploys normally (corresponds to `<Active>1</Active>`) |
| **Default Args** | `owner: 'airflow'`, `retries: 0`, `retry_delay: timedelta(minutes=5)` | Standard operational defaults |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_dummy_absd_plato_tarife` | `EmptyOperator` or `LogOperator` | N/A | N/A | 0 | N/A | None | None | `False` | None | Replaces UC4 dummy print script. |

### 5. Task Dependency Map
Since only one job is present in the source input, the execution sequence is a simple single-node workflow:

```
[start] >> dwh_dummy_absd_plato_tarife >> [end]
```

* **Execution Flow Description**: The DAG starts execution, immediately executes the empty/log placeholder task representing the UC4 dummy script, and finishes successfully.

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent | Note |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | UC4 Object Name | `dw_dwh_dummy_absd_plato_tarife` | Sanitised DAG/Task identifier |
| `DW.UNIX.ISTNS` | `<Login>` | N/A | Not required for `EmptyOperator` |
| `|DWHDWH1P|HOST` | `<HostDst>` | N/A | Target execution host in UC4 |

### 7. Error Handling and Retry Strategy
* **Default Behaviour**: No explicit retry policies or error-handling postconditions (`POST_SCRIPT` or `SYNCREF`) exist inside this object. 
* **Airflow Implementation**: The task is configured with `retries=0`. Any unexpected execution failure will immediately mark the task and DAG run as failed.

### 8. Developer Notes
* **Missing Orchestration Structure**: This job is almost certainly triggered inside a parent Job Plan (`JOBP`). When that file becomes available, this single task should be integrated into the main DAG pipeline instead of running as a standalone DAG.
* **No PySpark Translation Needed**: Because this is a dummy print script (`:print Doing nothinig`), no Dataproc or PySpark transformations are generated. An `EmptyOperator` (or a PythonOperator executing a log statement) is the cleanest equivalent.
* **GCP Infrastructure Placeholders**: Standard GCP configuration variables are defined in the pseudocode to maintain compatibility with your target environment framework, but they are not leveraged by this specific dummy task.

---

## SECTION 2 — PSEUDOCODE & IMPLEMENTATION

The target Airflow script has been cleaned of all prose placeholders to comply strictly with environment policies, utilizing standard environment variable fetching.

```python
# Imports 
import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# GCP Configuration (Globally Sourced Environment Variables)
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT")
DATAPROC_REGION = os.environ.get("DATAPROC_REGION")
DATAPROC_CLUSTER = os.environ.get("DATAPROC_CLUSTER")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

# Default Args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    schedule=None,  # Gapped: No EVNT_TIME trigger definition provided in source
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False, # Active flag in UC4 was 1 (True)
    tags=['migrated_uc4', 'dummy_job']
)

# Task: dwh_dummy_absd_plato_tarife
# Original print statement: ":print Doing nothinig" (verbatim output retained)
def log_dummy_action():
    logging.info("Doing nothinig")

dwh_dummy_absd_plato_tarife = PythonOperator(
    task_id='dwh_dummy_absd_plato_tarife',
    python_callable=log_dummy_action,
    dag=dag
)

# Dependencies
dwh_dummy_absd_plato_tarife
```

---

## SECTION 3 — ADDITIONAL CONTEXT AND ORCHESTRATION

### 1. Job Dependencies
Based on the pre-collected metadata:
* **Upstream:** None discovered.
* **Downstream:** 
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` — **NOT YET MIGRATED**.
* **Orchestration Wiring on BigQuery / Cloud Composer:**
  * Since the downstream Job Plan (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml`) has not yet been migrated, this DAG cannot be directly linked via `TriggerDagRunOperator` or `ExternalTaskSensor` at this time.
  * Once the downstream parent workflow is migrated, this job should be either integrated directly as a single `PythonOperator` task inside the main workflow DAG, or triggered using Airflow's native inter-DAG dependency mechanisms.

### 2. Execution Order
* **Step sequence:** This job consists of a single execution task (`dwh_dummy_absd_plato_tarife`). Since there is only one node, no complex multi-task topology is required.

### 3. Scheduling
* **Trigger Event:** No scheduling triggers (`EVNT_TIME` or Calendars) were detected in the source XML file. It is configured with `schedule=None` and is intended to be executed on-demand, or directly triggered by its calling parent Job Plan once migrated.

### 4. Schedule & Variables — Must Be Retained
* **Variables:** None.
* **Inherited Linkage:** This job is defined inside the `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` workflow and inherits its execution properties from there.

### 5. Lineage
* `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml  --[CALLS_HTTP]-->  EXT:DWHDWH1P` (Confidence: 0.85)
* `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml  --[USES_PACKAGE]-->  PACKAGE:DW.UNIX.ISTNS` (Confidence: 0.80)

### 6. External System Replacements
* **Legacy Unix Host (`|DWHDWH1P|HOST`):** This is mapped directly to the local Cloud Composer worker environment. Since the dummy print statement is handled by Airflow's Python runtime natively, no remote shell connections (SSH, HTTP) or external server runs are needed.
* **Login Package (`PACKAGE:DW.UNIX.ISTNS`):** In Cloud Composer, credentials and access roles are managed by the Composer cluster service account, deprecating the legacy UNIX login user.

### 7. Cross-File Dependencies
* This job does not rely on shared physical files, SQL schemas, or common data layers.

### 8. Target File Plan
* **Target File Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py`
  * **Language:** Python (Airflow DAG)
  * **Source File:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
  * **Description:** Represents the migrated DAG structure with standard environment configs.

---

## SECTION 4 — ENVIRONMENT-SPECIFIC VALUES

To prevent hardcoded configurations and comply with target development protocols, variables are classified by scope:

| Vocabulary Concept | Target Platform Representation | Value Sourcing / Runtime Mechanism |
| :--- | :--- | :--- |
| **GCP Project** | `GCP_PROJECT` | Sourced via `os.environ.get("GCP_PROJECT")` at runtime |
| **GCP Region** | `GCP_REGION` | Sourced via `os.environ.get("GCP_REGION")` at runtime |
| **GCS Storage Bucket** | `GCS_BUCKET` | Sourced via `os.environ.get("GCS_BUCKET")` at runtime |
| **Legacy Host** | `|DWHDWH1P|HOST` | *Retired* (Not required on GCP Composer Workers) |
| **Legacy Login** | `DW.UNIX.ISTNS` | *Retired* (Handled by Composer Worker Service Account) |

---

## SECTION 5 — RISKS & MANUAL ACTIONS

1. **DOWNSTREAM: NOT YET MIGRATED — `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`**
   * *Risk:* The downstream parent workflow that executes this job is missing. The direct execution triggers or Airflow workflow-integration wiring cannot be finalized until `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` has been fully migrated.
2. **Missing Job Plan Context (`JOBP`)**
   * *Risk:* This dummy job serves as a synchronization point or milestone in the parent legacy workflow. Without the parent workflow structure, the true execution order and success criteria cannot be validated in isolation.
3. **Recovery and Restartability Action**
   * *Legacy Documentation:* `Wiederanlauf ohne weitere Maßnahmen möglich` (Restart possible without further measures).
   * *Manual Verification:* This task has no side effects and is completely idempotent. In the event of DAG failure, developers can rerun it without performing any clean-ups or state rollbacks on target databases.
4. **Potential Deprecation Candidates**
   * *Recommendation:* Since this is a pure "dummy" milestone job, when migrating the full parent workflow, engineers should evaluate if this task can be deprecated entirely and replaced by direct task dependencies (e.g. `task_A >> task_B`) inside the consolidated parent DAG.