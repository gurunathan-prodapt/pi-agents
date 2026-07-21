# MIGRATION DESIGN DOCUMENT
**Job Name:** DW.DWH_DUMMY_ABSD_PLATO_TARIFE  
**Source Root:** `/home/gurunathan_t/tool_mapping_samples`  
**Target Platform:** BigQuery & Cloud Composer (Airflow)  
**Migration Pattern:** `UC4_ONLY` (Orchestration Migration)

---

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Migrates the UC4 dummy Unix job logic to a Cloud Composer Airflow DAG structure. |

---

## SECTION 1 — VERBATIM MCP TOOL OUTPUT
The section below contains the complete, unmodified output from the migration conversion tool `uc4_to_airflow_dag_design`.

```markdown
### WARNING: INCOMPLETE WORKFLOW DETECTED
Only one file was provided, and it is a `JOBS_UNIX` (Unix Job) file. A complete workflow transformation typically requires at least one `EVNT_TIME` (Time Event), one `JOBP` (Job Plan), and one `JOBS_UNIX` (Unix Job) file to extract scheduling, workflow structures, and task-level dependencies. 

This analysis is based strictly on the single provided Unix Job file. Assumptions have been made to construct a functional DAG blueprint around it.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The provided UC4 object `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is a Unix Job that functions as a dummy task (the script body merely outputs `:print Doing nothinig`). It is part of the `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` job plan context (inferred from the source path). Because it contains no business logic or Ab Initio processing, its primary purpose in UC4 is to act as a synchronization anchor, a placeholder, or a manual trigger step in the daily Plato Tarif Mapping workflow.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `JOBS_UNIX` | `1` (Active) | A dummy Unix job that prints a placeholder message and finishes successfully. |

## 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_plato_tarif_mapping_taeglich_jp` |
| **schedule** | `None` (No schedule info; `EVNT_TIME` or `JSCH` files were not provided) |
| **start_date** | `2026-03-30` (Derived from object export/modification date) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Source active value is `<Active>1</Active>`) |
| **default_args** | `{ 'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5) }` |

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` (or `EmptyOperator`) | `dw_dwh_dummy_absd_plato_tarife.py` | Project, Region, Cluster, GCS Bucket placeholders | 0 | 5 min | None | None | N/A | None | This is a dummy job. It is modeled here as a Dataproc PySpark task, but can safely be optimized to an `EmptyOperator`. |

## 5. Task Dependency Map
Since only one task was provided, the dependency map represents a standalone task structure:

`start >> dw_dwh_dummy_absd_plato_tarife >> end`

* **`start`**: Airflow dummy start node.
* **`dw_dwh_dummy_absd_plato_tarife`**: The imported dummy task executing its placeholder script on Dataproc.
* **`end`**: Airflow dummy end node.

## 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | UC4 Job Name | Sanitized Task ID: `dw_dwh_dummy_absd_plato_tarife` |
| Host: `|DWHDWH1P|HOST` | Host executing Unix script | Maps to `YOUR_DATAPROC_CLUSTER_NAME` placeholder |
| Login: `DW.UNIX.ISTNS` | Execution Credentials | Maps to Dataproc Service Account / Execution Context |
| Estimated Runtime (ERT) | `11` seconds | Used for performance baseline tracking |

## 7. Error Handling and Retry Strategy
* **Retries**: There is no retry configuration specified in the UC4 XML (`<RUNTIME>` details basic runtime collection settings, but no automated post-execution recovery loops are set). Default Airflow task-level retries are set to `0`.
* **Sync Object Else behavior**: No sync configuration (`<Syncs/>` is empty) was declared on this object.
* **Postconditions**: No post-scripts, terminal block criteria, or custom failure alert rules are defined in this XML file.

## 8. Developer Notes
* **Missing Workflows**: The surrounding Job Plan (`JOBP`) and Schedule/Event triggers are missing. The scheduling parameter has been defaulted to `None`. The developer must verify the correct parent DAG schedule and structure.
* **Dummy Job Optimization**: `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` does nothing but print a string (`Doing nothinig`). In production Airflow, running a full Dataproc cluster job for this is inefficient. It is highly recommended to replace the `DataprocSubmitJobOperator` for this task with an `EmptyOperator` or a lightweight `PythonOperator` that logs the string.
* **GCP Placeholders**: The developer must replace all GCS bucket, project, region, and cluster configuration variables before deployment.

---

# SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
# TODO: Replace placeholders with target environment values
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_plato_tarif_mapping_taeglich_jp',
    default_args=default_args,
    description='Converted Plato Tarif Mapping Daily DAG from UC4',
    schedule=None,  # Unknown Schedule: No EVNT_TIME/JSCH file provided
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False, # Source <Active>1</Active>
)

# ── Tasks ───────────────────────────────────────────────

# Boundary tasks
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Task: dw_dwh_dummy_absd_plato_tarife
# UC4 Object: DW.DWH_DUMMY_ABSD_PLATO_TARIFE
# Note: Source is a dummy job printing "Doing nothinig".
# Below is the Dataproc mapping. (EmptyOperator can be used alternatively to save cost)
pyspark_job_definition = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
    },
}

dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_definition,
    # Unique execution ID generation
    job_id="dw_dwh_dummy_absd_plato_tarife_{{ run_id | ts_nodash }}_{{ task_instance.try_number }}",
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end
```
```

---

## SECTION 2 — ADDITIONAL TARGET ARCHITECTURE CONTEXT

### 1. Job Dependencies & Target Orchestration
Based on the JOB DEPENDENCIES section of the pre-collected context, this job interacts with other components in the workflow environment as follows:
* **Downstream Workflow Dependency:**
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` (Status: **Not Yet Migrated**)
  * **Target Wiring:** The current job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is a child task inside the Daily Plato Tarif Mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). On Cloud Composer, once the parent workflow is migrated, the tasks should be integrated into a single unified DAG (`dw_dwh_plato_tarif_mapping_taeglich_jp`), or orchestrated via cross-DAG execution using the `TriggerDagRunOperator` or `ExternalTaskSensor` if implemented as separate modules.
  * **Predecessors / Successors:** Since this is a placeholder/synchronization step, its successful execution is a prerequisite for downstream execution steps of the Plato Tarif Mapping workflow.

### 2. Execution Order & Scheduling
* **Execution Order:**
  * In the parent JP (Job Plan), this dummy step executes as a synchronization anchor.
  * In the target platform, the execution sequence must preserve this structure, resolving starting points and downstream triggers cleanly within Cloud Composer.
* **Scheduling:**
  * Since the UC4 job contains no active scheduler of its own, it inherits its execution trigger from its parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.
  * The DAG is configured with `schedule=None` and will trigger dynamically based on parent orchestration or external sensor events.

### 3. External System Replacements & Lineage Edges
* **Lineage Edge 1 (`--[CALLS_HTTP]--> EXT:DWHDWH1P`):**
  * **Legacy context:** Executed on UC4 Host `|DWHDWH1P|HOST`.
  * **GCP Target Architecture:** Replaced entirely by standard Cloud Composer (Airflow) worker execution, eliminating the need for dedicated physical host execution.
* **Lineage Edge 2 (`--[USES_PACKAGE]--> PACKAGE:DW.UNIX.ISTNS`):**
  * **Legacy context:** Execution credentials associated with Login `DW.UNIX.ISTNS`.
  * **GCP Target Architecture:** Replaced with IAM Service Account roles associated with Cloud Composer and GKE/Dataproc execution environment.

### 4. Folder Integrity Rule
To guarantee folder integrity, the target folder layout exactly mirrors the legacy codebase directory structures.
* **Source Folder:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/`
* **Target Folder:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/`
* No files from other source folders have been merged or grouped into this target path.

### 5. Print Literal Rule Compliance
The legacy print statement inside the XML `<SCRIPT>` tag is:
```xml
<MSCRI><![CDATA[:print Doing nothinig]]></MSCRI>
```
The original string contains the spelling mistake `"Doing nothinig"`. In the target code, this string must be preserved **exactly as-is** (character-for-character) inside the logging/output mechanism:
```python
print("Doing nothinig")
```

---

## SECTION 3 — COMPLIANT ENVIRONMENTS & TARGET ENVIRONMENT MAPPING

To comply with the environment variable guidelines, there are no prose placeholders (e.g., `"YOUR_GCP_PROJECT_ID"`) in the target code or default fallbacks. Global GCP values must be fetched dynamically at runtime using Airflow Variables or environment settings.

### 1. Environment Variable Classification
* **GLOBAL (Environment-wide):**
  * `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")`
  * `GCP_REGION`: Sourced via `Variable.get("GCP_REGION")`
  * `DATAPROC_CLUSTER`: Sourced via `Variable.get("DATAPROC_CLUSTER")`
  * `GCS_BUCKET`: Sourced via `Variable.get("GCS_BUCKET")`
* **JOB-SPECIFIC:**
  * `task_id`: `dw_dwh_dummy_absd_plato_tarife`
  * `dag_id`: `dw_dwh_plato_tarif_mapping_taeglich_jp`

---

## SECTION 4 — REFINED COMPLIANT TARGET IMPLEMENTATION (PSEUDOCODE)

Below is the optimized, fully compliant production-ready Python Airflow DAG script. In accordance with Section 1's recommendation to optimize dummy jobs, a `PythonOperator` logging the exact literal statement is used to execute the dummy step efficiently without spinning up a Dataproc cluster.

```python
# ── Imports ──────────────────────────────────────────────
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ── GCP Configuration (GLOBAL Env Variables) ──────────────
# Sourced dynamically at runtime via Airflow Variables.
# No hardcoded placeholders or fake fallback values.
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET")

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_plato_tarif_mapping_taeglich_jp',
    default_args=default_args,
    description='Converted Plato Tarif Mapping Daily DAG from UC4',
    schedule=None,  # Dynamic scheduling managed by parent/external triggers
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Matches legacy active state
)

# ── Executable Logic (Print Literal Compliance) ──────────
def execute_dummy_job():
    # MUST print original-language and original-spelling exactly as-is
    print("Doing nothinig")
    logging.info("Doing nothinig")

# ── Tasks ───────────────────────────────────────────────

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Optimized dummy job utilizing a PythonOperator (preserves execution logic efficiently)
dw_dwh_dummy_absd_plato_tarife = PythonOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    python_callable=execute_dummy_job,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end
```

---

## SECTION 5 — RISKS & MANUAL ACTIONS

### 1. Predecessors & Successors Integration Risk
* **SOURCE: NOT YET MIGRATED — DOWNSTREAM — uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml**
* **Impact:** The downstream workflow parent job plan is currently unmigrated. The task node sequence and integration configurations cannot be fully tested or locked down until `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` has been converted.
* **Mitigation:** Defer DAG scheduling activation and perform final end-to-end task integration testing once the parent workflow configuration XML is fully migrated to Cloud Composer.

### 2. Synchronization Anchor validation
* **Risk:** In UC4, dummy tasks are frequently used to pause execution for manual operator checks or external synchronizations. Replacing this task with an instantaneous `PythonOperator` removes any artificial wait state unless manually introduced.
* **Manual Action:** Coordinate with the business operations team to ensure that `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is purely an execution placeholder and does not require manual pause or gate checks in production.