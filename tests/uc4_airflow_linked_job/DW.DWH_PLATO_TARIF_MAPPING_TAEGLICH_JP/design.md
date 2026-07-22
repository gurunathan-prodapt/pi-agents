# MIGRATION DESIGN DOCUMENT: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## 1. File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
|:---|:---|:---|
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Converts the UC4 job definition into an equivalent Cloud Composer Airflow DAG. |

---

## 2. Verbatim MCP Tool Output

The section below contains the complete and verbatim design output produced by the `uc4_design_airflow_dag` tool:

```markdown
## INPUT VALIDATION & GAPS WARNING

* **CRITICAL FILE INVENTORY WARNING**: Only one UC4 file has been provided, and it is a `JOBS_UNIX` object (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`). A complete UC4 workflow typically requires at least one `EVNT_TIME` (for scheduling), one `JOBP` (for workflow sequence/parent job plan execution context), and one or more `JOBS_UNIX` files.
* **MISSING CONTEXT**: Because the parent Job Plan (`JOBP`) and Event Schedule (`EVNT_TIME`) files are missing, task-level constraints (Earliest Start Time, Calendar Constraints, Task-level Retries, and Task Dependencies) and workflow scheduling cannot be extracted from the source. The design and pseudocode below are constructed for a single-task standalone DAG using default parameters where information is missing.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The UC4 object `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is a Unix Job that functions as a dummy/utility step (indicated by the script command `:print Doing nothinig`). In its original context, it was likely used within a daily DWH Plato tariff mapping process (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) as a placeholder, synchronization anchor, or manual trigger point. Since it does not execute any actual transactional logic or Ab Initio graphs, its Airflow counterpart is designed to run a lightweight placeholder PySpark execution or a direct equivalent on a Google Cloud Dataproc cluster.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | Active (`<Active>1</Active>`) | Dummy Unix utility job that prints log outputs; does not contain processing logic or data manipulation commands. |

## 3. Airflow DAG Properties
| Property | Value | Note |
|---|---|---|
| **DAG ID** | `dw_dwh_dummy_absd_plato_tarife` | Derived by lowercasing and replacing dots/hyphens with underscores. |
| **Schedule (cron)** | `None` | **Flagged Gap**: No `EVNT_TIME` or scheduling source was provided. Set to manual/none by default. |
| **Start Date** | `datetime(2026, 3, 30)` | Placeholder set to the date of UC4 export metadata. |
| **Catchup** | `False` | Safe default configuration to prevent backfilling of dummy runs. |
| **Max Active Runs** | `1` | Defaulting to 1 run to prevent overlapping executions. |
| **Is Paused Upon Creation** | `False` | Matches the UC4 active flag `<Active>1</Active>`. |
| **Default Args** | `{ 'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5) }` | Standard operational defaults; no postcondition retries found in the source. |

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `dw_dwh_dummy_absd_plato_tarife.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME` | 0 | 5 min | None | None | `wait_for_completion=True` | None | Maps UC4 Unix Job. Represents a dummy job. Executes placeholder script. |

## 5. Task Dependency Map
Since only one task is present, the dependency sequence represents a basic single-task flow:

```
start >> dw_dwh_dummy_absd_plato_tarife >> end
```

* **start**: An EmptyOperator acting as a clean workflow entrypoint.
* **dw_dwh_dummy_absd_plato_tarife**: The primary task executing the dummy/utility PySpark job.
* **end**: An EmptyOperator acting as a workflow success anchor.

## 6. Parameter and Variable Mapping
| UC4 Parameter / Attribute | Value/Source | Airflow Equivalent / Placeholder |
|---|---|---|
| `name="DW.DWH_DUMMY_ABSD_PLATO_TARIFE"` | Root attribute | `dag_id="dw_dwh_dummy_absd_plato_tarife"` / `task_id="dw_dwh_dummy_absd_plato_tarife"` |
| `Login="DW.UNIX.ISTNS"` | `<Login>` element | Local Unix login, replaced by Airflow connection/service account in GCP. |
| `HostDst="|DWHDWH1P|HOST"` | `<HostDst>` element | Target execution host, replaced by GCP Dataproc Cluster parameters. |
| `Ert="11"` | `<Ert>` element | Expected runtime (11 seconds) documented for telemetry. |
| Script Command | `:print Doing nothinig` | Handled via a dummy/no-op script `dw_dwh_dummy_absd_plato_tarife.py` inside standard Dataproc execution templates. |

## 7. Error Handling and Retry Strategy
* **Retry Counter**: The UC4 runtime settings specify no special automatic execution retries (e.g. `<MrcExecute/>` and `<MrtCancel>0</MrtCancel>`). Retries are set to `0` in Airflow defaults.
* **Postconditions**: No postconditions or execution actions are defined in the XML. Thus, no custom `on_failure_callback` is registered.
* **Sync Object**: The `<SYNCREF>` block is empty (`<Syncs/>`), meaning no concurrency guard tasks or `Else="Skip"` logic is required. Standard `max_active_runs=1` is sufficient.

## 8. Developer Notes
* **Missing Workflows / Scheduling**: This configuration is generated from an isolated `JOBS_UNIX` file. The developer must manually integrate this task into its parent DAG (`dw_dwh_plato_tarif_mapping_taeglich_jp` if that parent DAG is migrated) and wire its actual execution schedule.
* **GCP Infrastructure Placeholders**: All Dataproc cluster variables (project, region, cluster name, bucket path) are defined as constants at the beginning of the DAG file. The developer must update these placeholders or bind them to Airflow Variables.
* **Dummy Script Requirement**: A placeholder PySpark script named `dw_dwh_dummy_absd_plato_tarife.py` must be deployed to the designated GCS bucket location (`gs://YOUR_BUCKET_NAME/pyspark_scripts/`) containing a simple execution output statement (e.g., `print("Doing nothing - Dummy execution")`) to ensure the Dataproc task succeeds without running operational actions.
* **Trigger Rule Warning**: Standard default `TriggerRule.ALL_SUCCESS` is applied. Do not modify or introduce `ALL_DONE` trigger rules without verifying upstream task requirements.

---

# SECTION 2 — PSEUDOCODE

```python
── Imports ──────────────────────────────────────────────
FROM airflow IMPORT DAG
FROM airflow.providers.google.cloud.operators.dataproc IMPORT DataprocSubmitJobOperator
FROM airflow.operators.empty IMPORT EmptyOperator
FROM datetime IMPORT datetime, timedelta

── GCP Configuration ────────────────────────────────────
# TODO: Developer must configure these GCP parameters before deployment
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

── DAG Definition ───────────────────────────────────────
# UC4 Active Value = 1 -> is_paused_upon_creation=False
# UC4 Schedule File = Missing -> schedule=None
dag = DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'dwh_plato_tarif'],
)

── Tasks ────────────────────────────────────────────────

# Start flow boundary anchor
task_start = EmptyOperator(
    task_id='start',
    dag=dag
)

# Task implementing the UC4 Job: DW.DWH_DUMMY_ABSD_PLATO_TARIFE
# Executes a placeholder PySpark job on GCP Dataproc representing the original dummy job
pyspark_job_config = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
    }
}

task_dummy_plato_tarife = DataprocSubmitJobOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_config,
    # Generate dynamic job ID using the DAG run context to prevent conflicts
    job_id="dw_dwh_dummy_absd_plato_tarife_{{ run_id | ts_nodash | lower }}_task",
    asynchronous=False, # wait_for_completion=True behavior
    dag=dag
)

# End flow boundary anchor
task_end = EmptyOperator(
    task_id='end',
    dag=dag
)

── Dependencies ─────────────────────────────────────────
task_start >> task_dummy_plato_tarife >> task_end
```
```

---

## 3. Additional Migration Context

### Job Dependencies & Execution Chains
* **Upstream**: None discovered within the isolated scope of this job context.
* **Downstream**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml — not yet migrated`
  * **Wiring Strategy on Google Cloud Composer**: The legacy downstream job is a parent Job Plan (`JOBP`). Since that parent workflow is not yet migrated, this DAG will remain isolated initially. Once `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is converted to Airflow, this DAG's logic should be integrated as a sub-task flow or orchestrated via a `TriggerDagRunOperator` or shared dataset trigger within the primary parent Airflow DAG.

### Scheduling
* **Schedule Context**: Inherited from its parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (Daily Plato Tariff Mapping workflow). It does not have an independent cron schedule of its own.

### Lineage Edges
* `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml --[CALLS_HTTP]--> EXT:DWHDWH1P` (Confidence: 0.85)
  * Represents target execution connection host `|DWHDWH1P|HOST`.
* `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml --[USES_PACKAGE]--> PACKAGE:DW.UNIX.ISTNS` (Confidence: 0.80)
  * Represents Unix Login credential environment.

### External System & Architecture Replacements
* **Legacy Host**: Target Host `|DWHDWH1P|HOST` and execution package `DW.UNIX.ISTNS` are retired. The execution is migrated entirely to native Cloud Composer / Google Cloud environment.
* **Execution Optimization**:
  The legacy script performs only one operation: `:print Doing nothinig`.
  * **Dataproc Overhead**: Provisioning or submitting a job to a Google Cloud Dataproc cluster merely to run a "do-nothing" print statement introduces unnecessary compute overhead and billing costs.
  * **Recommended Native Alternative**: Rather than deploying a physical PySpark script to Dataproc as suggested by the generic translation tool, we recommend a lightweight native Airflow `PythonOperator` that performs the printing directly in the Composer worker execution context. This yields identical functional behavior with zero cluster runtime overhead.

---

## 4. Target File Plan

Following the **Folder Integrity Rule**, target files are placed in mirrored repository paths matching their source folders.

### File 1: Airflow Orchestration DAG
* **Target Relative Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py`
* **Language**: Python (Airflow DAG)
* **Source File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`

### File 2: Execution Script (Optional - only required if choosing the heavy Dataproc pattern)
* **Target Relative Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_script.py`
* **Language**: Python
* **Source File**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`

---

## 5. Environment-Specific Values

All environment variables have been classified strictly by their target deployment roles. Under the **Hard Ban** policy, no placeholder strings (e.g., `YOUR_GCP_PROJECT_ID`) are used in our own production code.

### 1. Global (Environment-Wide) Configurations
* **GCP_PROJECT**: Identifies the destination Google Cloud Project.
  * *Resolution*: Retrieved dynamically using `os.environ.get("GCP_PROJECT")`.
* **GCP_REGION**: Specifies the target region for cloud services.
  * *Resolution*: Retrieved dynamically using `os.environ.get("GCP_REGION")`.
* **DATAPROC_CLUSTER_NAME**: The shared cluster executing PySpark workloads.
  * *Resolution*: Retrieved via Airflow Config store: `Variable.get("DATAPROC_CLUSTER_NAME")`.
* **GCS_BUCKET**: The deployment storage bucket.
  * *Resolution*: Retrieved via Airflow Config store: `Variable.get("GCS_BUCKET")`.

### 2. Job-Specific Values
* **`DW.UNIX.ISTNS` (Login Package)**: Maps to the target GCP service account executing the Cloud Composer worker processes or Dataproc jobs.
* **`|DWHDWH1P|HOST` (Target host)**: Represents the legacy host platform, which is decommissioned.

### Optimized Production Code Snippet (No Placeholders)
The following code snippet demonstrates the optimized, native execution approach using environment variable resolution:

```python
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Dynamic Global Variable Resolution
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_REGION = os.environ.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="dwh-composer-storage-bucket")

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def execute_dummy_action():
    # OUTPUT/PRINT LITERAL RULE: Verbatim message with original typo retained character-for-character
    print("Doing nothinig")

with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'dwh_plato_tarif'],
) as dag:

    task_start = EmptyOperator(task_id='start')

    # Native Python execution prevents cluster initialization overheads for a simple print command
    task_dummy_plato_tarife = PythonOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
        python_callable=execute_dummy_action,
    )

    task_end = EmptyOperator(task_id='end')

    task_start >> task_dummy_plato_tarife >> task_end
```

---

## 6. Risks & Manual Steps

1. **Downstream Pipeline Migration Lag**:
   * **Downstream Reference**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml`
   * **Risk**: The downstream workflow has not yet been migrated. This DAG will operate as an isolated unit until the parent workflow XML is fully processed and translated.
   * **Mitigation**: Once the parent DAG (`dw_dwh_plato_tarif_mapping_taeglich_jp`) is generated, integrate this task within its control sequence and remove the standalone DAG definition.
2. **Maintenance of Print Statements (Output Literal Rule)**:
   * **Risk**: The print statement contains a typo: `Doing nothinig`. System operators might attempt to correct this.
   * **Mitigation**: Do not "correct" or translate this print statement in subsequent manual phases. It must remain character-for-character identical to prevent automated log scanners or status scripts expecting that exact string from failing.