# MIGRATION DESIGN DOCUMENT: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Migrated as a mirrored Airflow DAG mapping the UC4 JOBS_UNIX structure and printing the exact literal message. |

---

## SECTION 1 — VERBATIM MCP TOOL OUTPUT

Below is the verbatim output returned by the `uc4_design_airflow_dag` tool:

```markdown
### ⚠️ INPUT VALIDATION & COMPLIANCE WARNING

* **Validation Flag:** Only one UC4 file was provided, and it is a `JOBS_UNIX` (Unix Job) file. 
* **Missing Components:** A complete, operational UC4 workflow translation typically requires at least one **EVNT_TIME** (Time Event) file, one **JOBP** (Job Plan/Workflow) file, and all associated **JOBS_UNIX** files.
* **Impact of Missing Files:**
  * **No Scheduling Information:** There is no calendar, schedule (`JSCH`), or time event (`EVNT_TIME`) defining when this job is triggered.
  * **No Workflow Context:** There is no parenting `JOBP` defining dependencies, upstream triggers, target runtime constraints, or dynamic variables passed during execution.
  * **No Ab Initio Graph Parameters:** This job is a placeholder/dummy UC4 object containing an internal UC4 print script (`:print Doing nothinig`) instead of an Ab Initio execution command (`r_ai_start`).
* **Resolution Approach:** This design document assumes this job will be generated as a standalone Airflow DAG running a dummy placeholder PySpark script on Google Cloud Dataproc.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The UC4 object `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is a Unix Job that acts as a dummy/no-op task within the DWH Plato Tarif mapping environment. Since the execution script consists solely of a UC4 script directive (`:print Doing nothinig`), it does not execute any command-line process or run an Ab Initio graph. Its primary utility is as a synchronization barrier, phase-completion milestone, or future expansion placeholder. In Apache Airflow, this will be modeled as a single-task DAG executing a basic PySpark script that performs a standard dry-run log write, or it can be replaced with an `EmptyOperator` depending on environmental standards.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | `1` (Active) | A dummy step that logs a "Doing nothing" notification. Contains no external system calls. |

### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` *(No scheduling file provided; must be triggered manually or via parent DAG)* |
| **start_date** | `datetime(2026, 3, 30)` *(Derived from export metadata timestamp)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Source was active `<Active>1</Active>`)* |
| **default_args** | `{"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)}` |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `dw_dwh_dummy_absd_plato_tarife.py` | Project, Region, Cluster | 0 | None | None | None | False | None | Performs placeholder Spark operation to mirror structural UC4 execution. |

### 5. Task Dependency Map
Since no parenting `JOBP` workflow container was provided, the task runs as a standalone sequence:

```
[Start] >> dw_dwh_dummy_absd_plato_tarife >> [End]
```

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Object Name | DAG ID: `dw_dwh_dummy_absd_plato_tarife` |
| Host: `|DWHDWH1P|HOST` | Target Execution Engine | Maps to GCP Dataproc Cluster parameters |
| Login: `DW.UNIX.ISTNS` | Operating User | Service Account executing the Airflow Task |

### 7. Error Handling and Retry Strategy
* **Retry Behavior:** The UC4 XML specifies `<Ert>11</Ert>` (estimated runtime of 11 seconds) and does not specify a recovery sequence, automatic restarts, or postcondition overrides. Consequently, retries are configured to `0` in Airflow.
* **Failure Alerts:** If execution fails, standard task failure notifications will apply via default Airflow configurations. No custom alerting objects or terminal `BLOCK` actions are mapped because they are not present in the source file.

### 8. Developer Notes
* **Dummy Behavior Migration:** The original UC4 job only executes `:print Doing nothinig`. This is an internal UC4 scripting engine command that executes *before* the job shell starts. In Airflow, this means no shell commands would run on a target machine. To preserve the structure of the data platform pipeline, we have configured a dummy PySpark script. If a virtual structural step is preferred, the developer can replace the `DataprocSubmitJobOperator` with a simple `EmptyOperator`.
* **Missing Parent Context:** If this job is triggered inside a parent DAG via a `TriggerDagRunOperator`, ensure that the trigger matches the sanitized target ID: `dw_dwh_dummy_absd_plato_tarife`.
* **No Calendar Constraints:** No `<calendars>` blocks were parsed because this is a job-level XML. If this job is bound to a specific operating schedule, those parameters must be verified from the parent environment manually.

---

## SECTION 2 — PSEUDOCODE

```python
# ==========================================
# Imports
# ==========================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator

# ==========================================
# GCP Configuration Placeholders
# ==========================================
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ==========================================
# Default Arguments
# ==========================================
# Setting retries to 0 to mirror the default failure-halt behavior in UC4 XML
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ==========================================
# DAG Definition
# ==========================================
# schedule is None as no EVNT_TIME or JSCH was supplied in the export
# is_paused_upon_creation is False as UC4 <Active> value was 1
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md="""
    ### Workflow Description
    Migration of UC4 Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.
    This job was identified as a dummy/no-op sync job printing: 'Doing nothinig'.
    Mapped to a simple placeholder PySpark execution on Google Cloud Dataproc.
    """
) as dag:

    # Standard pipeline boundary start marker
    start_boundary = EmptyOperator(
        task_id="start"
    )

    # ==========================================
    # Task: dw_dwh_dummy_absd_plato_tarife
    # ==========================================
    # Mapped from the JOBS_UNIX configuration.
    # Runs a placeholder PySpark script that corresponds to the dummy execution logic.
    # Uses a dynamic job ID structure following airflow rules.
    pyspark_job_config = {
        "reference": {
            "project_id": GCP_PROJECT_ID,
            "job_id": "dw_dwh_dummy_absd_plato_tarife_{{ run_id | ts_nodash | lower }}_task"
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
        }
    }

    run_dummy_pyspark_job = DataprocSubmitJobOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_config,
        asynchronous=False  # Wait for completion (equivalent to wait_for_completion=True)
    )

    # Standard pipeline boundary end marker
    end_boundary = EmptyOperator(
        task_id="end"
    )

    # ==========================================
    # Dependencies
    # ==========================================
    start_boundary >> run_dummy_pyspark_job >> end_boundary
```
```

---

## SECTION 3 — TARGET PLATFORM CONTEXT & PIPELINE ORCHESTRATION

### 1. Job Dependencies
* **Upstream:** None discovered inside this specific job definition.
* **Downstream:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` (Not yet migrated).
  * **Inter-DAG Wiring:** Because this target job is structural, the downstream DAG `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` must be triggered or reference this job as a dependency once migrated. Standard practice on Cloud Composer is to use an `ExternalTaskSensor` in the downstream DAG or a `TriggerDagRunOperator` at the end of this DAG.

### 2. Execution Order
This job consists of a single execution task. When run inside Airflow, the execution flow strictly mirrors UC4:
```
[start] >> [dw_dwh_dummy_absd_plato_tarife] >> [end]
```

### 3. Scheduling & Orchestration
* **Schedule:** No calendar, event (`EVNT_TIME`), or scheduling rule (`JSCH`) was provided in this job-level XML. It will be initialized with `schedule=None`.
* **Execution Trigger:** Handled via Composer-level manual execution, or triggered programmatically as part of the broader daily run by the parent schedule.

### 4. Lineage Edges
* **Outbound Call (`CALLS_HTTP`):** Reference to `EXT:DWHDWH1P` (conf=0.85). In legacy UC4, this represents the target host server `|DWHDWH1P|HOST`. Under Cloud Composer, this is abstracted into the target execution network/credentials.
* **Package Usage (`USES_PACKAGE`):** Reference to `PACKAGE:DW.UNIX.ISTNS` (conf=0.80). In UC4, this represents the execution login credentials (`<Login>DW.UNIX.ISTNS</Login>`). This maps directly to the Service Account used to execute the Airflow DAG workers.

### 5. Cross-File Dependencies
This file is a standalone placeholder with no shared tables, database DDLs, or external script call chains.

---

## SECTION 4 — ENVIRONMENT VARIABLE CLASSIFICATION

Per the project policy, environment and system configurations are strictly classified into **GLOBAL** and **JOB-SPECIFIC** roles, preventing the use of prose placeholders:

### 1. GLOBAL (Environment-Wide Variables)
The following variables remain identical across all deployment environments (Dev/Test/Prod) and are retrieved at runtime via Airflow's native configuration store:
* **`GCP_PROJECT`**: The target GCP Project ID. Sourced via `Variable.get("GCP_PROJECT")`.
* **`GCP_REGION`**: The targeted GCP / Composer region. Sourced via `Variable.get("GCP_REGION")`.

### 2. JOB-SPECIFIC Variables
The parameters specific to this job's context:
* **`dag_id`**: Set to `dw_dwh_dummy_absd_plato_tarife`.
* **`owner`**: Set to `airflow`.
* **`service_account`**: Service Account mapped from legacy `DW.UNIX.ISTNS`.

---

## SECTION 5 — REFINED PRODUCTION-READY TARGET FILE PLAN

While the verbatim MCP output outlines a Dataproc PySpark deployment pattern, doing so for a simple UC4 print statement is cost-inefficient and introduces unnecessary compute overhead. 

The production-ready design implements the **UC4_ONLY** pattern by executing the legacy print logic directly on Cloud Composer via a lightweight `PythonOperator`. This fully respects the **OUTPUT/PRINT LITERAL RULE**, ensuring the print statement typo `"Doing nothinig"` is preserved exactly character-for-character.

### Target Code: `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`

```python
"""
DAG: dw_dwh_dummy_absd_plato_tarife
Description: Migration of UC4 Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.
             Executes the legacy placeholder step by printing the exact verbatim message.
Pattern: UC4_ONLY
"""

import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ==============================================================================
# GLOBAL ENVIRONMENT VARIABLE SOURCING
# ==============================================================================
# All variables are retrieved dynamically from Airflow's variable store.
# No prose placeholders are utilized.
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCP_REGION = Variable.get("GCP_REGION", default_var=None)

# ==============================================================================
# DEFAULT ARGUMENTS
# ==============================================================================
# Retries set to 0 to mirror the legacy behavior where failure is unrecovered.
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
}

# ==============================================================================
# LEGACY PRINT EXECUTION (PRESERVED VERBATIM)
# ==============================================================================
def execute_legacy_print():
    """
    OUTPUT/PRINT LITERAL RULE COMPLIANCE:
    Legacy UC4 object printed: "Doing nothinig" (including typo).
    This text is preserved verbatim without alteration.
    """
    logging.info("Doing nothinig")

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    schedule=None,  # No calendar schedule is provided in legacy file
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md="""
    ### Workflow Description
    Migration of UC4 Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.
    This job is a dummy/no-op sync job which prints "Doing nothinig" (verbatim from legacy).
    """
) as dag:

    start_boundary = EmptyOperator(
        task_id="start"
    )

    dw_dwh_dummy_absd_plato_tarife = PythonOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife",
        python_callable=execute_legacy_print
    )

    end_boundary = EmptyOperator(
        task_id="end"
    )

    # Task Execution Sequence
    start_boundary >> dw_dwh_dummy_absd_plato_tarife >> end_boundary
```

---

## SECTION 6 — RISKS & MANUAL ACTIONS

* **Downstream Dependency Not Migrated:** The downstream file `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` is **not yet migrated**. Triggering mechanism (e.g., `TriggerDagRunOperator` or cross-DAG sensors) cannot be finalized until that workflow is available on Cloud Composer.
* **Loss of Host Execution Context:** Legacy runs were tied to Host `|DWHDWH1P|HOST` and Login `DW.UNIX.ISTNS`. The target Airflow environment must have standard GCP Service Accounts configured with appropriate permissions. Ensure the Cloud Composer worker service account has standard execution access.