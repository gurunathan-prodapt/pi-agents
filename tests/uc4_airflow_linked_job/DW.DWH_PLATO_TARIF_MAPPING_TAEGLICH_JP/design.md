# MIGRATION DESIGN DOCUMENT

**Seed Name:** DW.DWH_DUMMY_ABSD_PLATO_TARIFE  
**Seed Type:** JOB  
**Source Root:** `/home/gurunathan_t/tool_mapping_samples`  
**Target Platform:** BigQuery / Cloud Composer (Airflow)  
**Migration Pattern:** UC4_ONLY (Pure orchestration migration, no data layer migration)

---

## 1. Overview
This document details the migration plan for the UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Cloud Composer (Apache Airflow). 

The legacy component is a dummy/placeholder task in the daily mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It performs no actual database or system actions besides printing a diagnostic message (`Doing nothinig`). It is migrated to Airflow as a `PythonOperator` that logs the equivalent message.

---

## 2. File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Migrates the standalone UC4 Job definition into a Python-based Airflow DAG structure, mirroring the original directory layout. |

---

## 3. Environment-Specific Values & Variables Classification

Consistent with the Environment Values Policy, all runtime and infrastructure attributes have been categorized. There are no hardcoded placeholder strings (such as `"YOUR_X"` or `<PROJECT_ID>`) in the production implementation.

### Global (Environment-Wide Variables)
These represent environment-wide infrastructure. They are sourced dynamically via the operating system environment or Airflow configuration:
* **GCP_PROJECT**: `os.environ.get("GCP_PROJECT")` (Canonical GCP project identifier)
* **GCP_REGION**: `os.environ.get("GCP_REGION")` (Target deployment region)
* **DATAPROC_REGION**: `os.environ.get("DATAPROC_REGION")` (Region for Dataproc execution, if any)
* **DATAPROC_CLUSTER**: `os.environ.get("DATAPROC_CLUSTER")` (Name of the execution cluster)
* **GCS_BUCKET**: `os.environ.get("GCS_BUCKET")` (Primary Cloud Storage bucket)

### Job-Specific Variables
These values are particular to this job/file and are populated with real values from the source configuration:
* **owner**: `"DW.UNIX.ISTNS"` (The execution login identifier mapped directly from the `<Login>` tag)
* **dag_id**: `"dw_dwh_dummy_absd_plato_tarife_parent"` (Derived programmatically from the source job name)
* **schedule**: `None` (Since no schedule or `EVNT_TIME` metadata exists for this specific sub-component)

---

## 4. Context the MCP Could Not See

This section details job relationships and lineages found in the workspace context which are outside the scope of individual file analyzers:

* **Job Dependencies**:
  * **Upstream**: None discovered.
  * **Downstream**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` (The parent job plan/orchestrator sequence). It is marked as **not yet migrated** in the codebase context.
  * **Inter-DAG/Task Wiring**: Since the downstream parent orchestrator DAG is not yet migrated, this DAG should be triggered or integrated as an internal task within the parent Airflow workflow once that file is processed.
* **Lineage & Cross-File Edge Cases**:
  * **Package / Access Role**: Uses UC4 package `DW.UNIX.ISTNS` which aligns with the DAG owner metadata.
  * **Execution Host Target**: The XML specifies `<HostDst>|DWHDWH1P|HOST</HostDst>`. This represents execution on the primary legacy host environment `DWHDWH1P`. Under the Composer target architecture, the execution is executed natively inside the Airflow worker namespace.

---

## 5. VERBATIM MCP ANALYZER OUTPUT
The following section is the exact, unaltered output produced by the `uc4_design_airflow_dag` tool:

```markdown
## INPUT VALIDATION
⚠️ **CRITICAL FLAG:** Only **one** UC4 XML file was provided in the input, and it is a `JOBS_UNIX` file (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`). 

A complete, production-ready workflow migration typically requires at least:
1. **An EVNT_TIME file** (to resolve DAG-level scheduling details).
2. **A JOBP or JSCH file** (to resolve execution order, workflow dependencies, task-level constraints like earliest start times, calendar constraints, and fire-and-forget statuses).

Because these files are missing, this document has been designed based on the single provided Unix job, placing placeholder structures for the missing workflow-level configurations.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` object is a standalone UC4 Unix Job. Its internal script contains only a dummy print statement (`:print Doing nothinig`), suggesting it functions as a "no-op" (no operation) placeholder, synchronization anchor, or dummy task within a larger Job Plan (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`, as inferred from the file path structure). Since there are no business data-processing scripts or Ab Initio/PySpark job executions defined, this task will be migrated as a simple diagnostic logging task.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | `<Active>1</Active>` | Dummy job executing a basic script print statement. No data processing commands present. |

## 3. Airflow DAG Properties
Since no scheduling or workflow-level files (`EVNT_TIME` or `JOBP`) were provided, default placeholders are used for the DAG configurations.

| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife_parent` *(derived fallback)* |
| **schedule** | `None` *(Missing EVNT_TIME; developer must supply the cron expression)* |
| **start_date** | `datetime(2026, 3, 30)` *(Derived from source metadata export time)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Source `<Active>1</Active>`)* |
| **default_args** | `{'owner': 'DW.UNIX.ISTNS', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `PythonOperator` | N/A | N/A | 0 | N/A | None | None | False (`wait_for_completion=True`) | None | Executes a simple print/log statement to match the UC4 dummy script behavior. |

## 5. Task Dependency Map
Since this is a single standalone task migrated from a single XML file:

```
[Start] >> dw_dwh_dummy_absd_plato_tarife >> [End]
```

* **Start:** Triggered manually or by an external DAG/Schedule (due to missing `EVNT_TIME`/`JOBP`).
* **dw_dwh_dummy_absd_plato_tarife:** Executes a Python function printing "Doing nothinig" to match the source script behavior.
* **End:** Terminal DAG state.

## 6. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent |
|---|---|---|
| `Login` | `DW.UNIX.ISTNS` | Mapped to `owner` in DAG parameters / Airflow connection identifier if needed. |
| `HostDst` | `|DWHDWH1P|HOST` | Mapped to execution environment metadata. Not active because no actual shell scripts or PySpark jobs are run. |
| `Ert` | `11` | Documented estimated runtime (11 seconds) for baseline validation. |
| Job Script | `:print Doing nothinig` | Mapped to Python `logging.info("Doing nothinig")`. |

## 7. Error Handling and Retry Strategy
* **Retries:** The source job specifies `<MaxRetCode>0</MaxRetCode>` with no custom retry or automatic postcondition actions configured. Defaulting to `retries=0`.
* **on_failure_callback:** No external notification blocks, alarms, or `EXECUTE OBJECT` commands exist in this object's XML. No custom failure callback is defined.
* **ENDED_SKIPPED Pass-through:** Not applicable as there are no parent Job Plan structures or upstream guard tasks.

## 8. Developer Notes
* ⚠️ **Missing Schedule:** No `EVNT_TIME` file was provided. The DAG schedule is defined as `None`. The developer must consult the scheduling calendar and set the correct cron expression manually.
* ⚠️ **Missing Parent DAG:** The original file path implies this job belongs to `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`. When that parent Job Plan XML is acquired, this DAG should be integrated as a task node or triggered task inside that parent DAG workflow instead of running standalone.
* **Dummy Nature:** Since this job does not contain any Ab Initio command calls (`r_ai_start`), it is a pure dummy task. A `PythonOperator` printing a simple log is used instead of a `DataprocSubmitJobOperator`.
* **Typo Preservation:** The source print statement contains the typo "nothinig" (`Doing nothinig`). This has been preserved in the Airflow logging design to ensure strict parity.

---

# SECTION 2 — PSEUDOCODE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

── GCP Configuration ────────────────────────────────────
# Note: GCP configurations are declared but unused since this job does 
# not contain an Ab Initio graph execution.
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

── Callback Functions ───────────────────────────────────
# None required for this dummy job.

── DAG Definition ───────────────────────────────────────
# Mapped from:
# - Active Flag: 1 -> is_paused_upon_creation=False
# - Login: DW.UNIX.ISTNS -> owner
# - Schedule: Missing -> None (Requires manual specification)
dag_id = "dw_dwh_dummy_absd_plato_tarife_parent"

default_args = {
    "owner": "DW.UNIX.ISTNS",
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=dag_id,
    schedule=None,  # TODO: Fill in scheduling cron from missing EVNT_TIME file if available
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    doc_md="""
    ### Workflow Migration: DW.DWH_DUMMY_ABSD_PLATO_TARIFE
    This DAG is a migration of a UC4 Unix Job that acts as a dummy/placeholder.
    """
)

── Task: dw_dwh_dummy_absd_plato_tarife ─────────────────
# Mapped from: JOBS_UNIX "DW.DWH_DUMMY_ABSD_PLATO_TARIFE"
# Script Logic: ":print Doing nothinig"

def log_dummy_message_callable(**context):
    """
    Simulates the exact behavior of the UC4 job script: printing "Doing nothinig".
    """
    logging.info("Doing nothinig")

dw_dwh_dummy_absd_plato_tarife = PythonOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    python_callable=log_dummy_message_callable,
    dag=dag,
)

── Dependencies ─────────────────────────────────────────
# Single task workflow
dw_dwh_dummy_absd_plato_tarife
```
```

---

## 6. PRODUCTION-READY REFINED IMPLEMENTATION

The following code is the ready-to-run implementation, where all placeholder configurations are resolved according to strict environment guidelines (without `YOUR_...` or `<...>` placeholders):

```python
from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Resolve GCP Environment details dynamically using standard os.environ mapping
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT")
DATAPROC_REGION = os.environ.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = os.environ.get("DATAPROC_CLUSTER")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET")

# 2. Define job metadata parameters
dag_id = "dw_dwh_dummy_absd_plato_tarife_parent"

default_args = {
    "owner": "DW.UNIX.ISTNS",
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# 3. Create Airflow DAG (Schedule set to None explicitly because this is a sub-workflow task)
dag = DAG(
    dag_id=dag_id,
    schedule=None,  
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    doc_md="""
    ### Workflow Migration: DW.DWH_DUMMY_ABSD_PLATO_TARIFE
    This DAG represents a migrated UC4 Unix Job acting as a placeholder/no-op node.
    """
)

# 4. Task execution script (Preserves the exact German-like spelling typo 'Doing nothinig' verbatim)
def log_dummy_message_callable(**context):
    """
    Simulates the exact behavior of the UC4 job script: printing "Doing nothinig".
    """
    logging.info("Doing nothinig")

dw_dwh_dummy_absd_plato_tarife = PythonOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    python_callable=log_dummy_message_callable,
    dag=dag,
)

# 5. Define pipeline layout
dw_dwh_dummy_absd_plato_tarife
```

---

## 7. Risks & Manual Actions

1. **Downstream Parent Job Sequence Not Yet Migrated**: The workflow sequence `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` has not yet been migrated. This dummy job is currently deployed standalone but should eventually be merged or configured with a cross-DAG dependency trigger once the parent orchestrator DAG is implemented.
2. **Missing Scheduling Rules**: Due to the absence of the `EVNT_TIME` configuration file, scheduling is set to `None`. This execution relies on manual execution triggers or external scheduler calls. If this sub-job has a specific cron timeline, it must be declared inside the DAG configuration.