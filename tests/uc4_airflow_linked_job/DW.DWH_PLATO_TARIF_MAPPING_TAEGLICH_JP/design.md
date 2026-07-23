# MIGRATION DESIGN DOCUMENT: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Migrates the UC4 UNIX dummy job to a target Airflow task representation. Since this is a pure orchestration task with no operational data transformation logic, it will be mapped into an Airflow DAG file. |

---

## SECTION 1 — VERBATIM MCP DESIGN DOCUMENT

Below is the complete, unaltered output of the `uc4_design_airflow_dag` tool:

```markdown
### WARNING: INCOMPLETE WORKFLOW FILES DETECTED
* **File Completeness Flag:** Only one UC4 file was provided (`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`). This is a **JOBS_UNIX** object. No `EVNT_TIME`, `JOBP` (Job Plan), or `JSCH` (Job Schedule) files were provided. A complete workflow typically requires at least one parent controller/workflow object and scheduler definition.
* **Impact:** 
  * The execution schedule, dependencies, earliest start times, and calendar constraints cannot be determined from the source.
  * The script body of `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` contains only a UC4 print statement (`:print Doing nothinig`). It does not run an Ab Initio graph (`r_ai_start` command is missing), indicating this is a "dummy" or execution-placeholder task.
  * We have proceeded with the design and pseudocode for this single-task DAG, using assumptions and placeholders where metadata is missing.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This migration document covers the conversion of the UC4 UNIX Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` into an Apache Airflow DAG. Based on the file path hint (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`), this job is a daily dummy task belonging to a broader Plato tariff mapping workflow. The UC4 job script performs no actual processing logic (it only prints "Doing nothinig"). It is converted into a standard Dataproc execution pattern to allow future implementation of PySpark logic under the same naming convention, or simple execution as a placeholder task.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | 1 (Active) | Placeholder/dummy job printing "Doing nothinig" |

### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_plato_tarif_mapping_taeglich_dag` *(Assumed based on path hint)* |
| **schedule** | `None` *(Missing scheduling files; developer must set this manually)* |
| **start_date** | `datetime(2026, 3, 30)` *(Derived from export timestamp)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Source active flag `<Active>1</Active>`)* |
| **default_args** | `{ 'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5) }` |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | DataprocSubmitJobOperator | `gs://YOUR_BUCKET_NAME/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py` | GCP region, project, cluster, bucket placeholders | 0 | None | None | None | False | None | Converted from a dummy UNIX task. Runs a placeholder PySpark script. |

### 5. Task Dependency Map
Since only one job file was provided, the dependency map consists of a single execution node wrapped within start and end markers:

```
start_marker >> dw_dwh_dummy_absd_plato_tarife >> end_marker
```

* **start_marker**: Dummy empty operator indicating the start of the DAG run.
* **dw_dwh_dummy_absd_plato_tarife**: Converted dummy task executing a placeholder PySpark job on Google Cloud Dataproc.
* **end_marker**: Dummy empty operator indicating successful completion.

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` *(UC4 Object)* | `dw_dwh_dummy_absd_plato_tarife` | Task ID & Trigger ID equivalent |
| Host: `|DWHDWH1P|HOST` | Target cluster placeholder | `YOUR_DATAPROC_CLUSTER_NAME` |
| Login: `DW.UNIX.ISTNS` | Not applicable to cloud execution | Mapped to GCP credentials via Airflow connections |
| Estimated Runtime (ERT) | `11` seconds | Documentation/metric monitoring reference |

### 7. Error Handling and Retry Strategy
* **Retries:** No explicit retry policies, postconditions, or `<SYNCREF>` structures were found in the provided XML file. The DAG relies on standard default arguments of `retries: 0`.
* **Sync Else Action:** Not defined in source. Standard DAG concurrency is restricted via `max_active_runs=1`.

### 8. Developer Notes
* **Missing Workflows/Schedules:** This migration is highly incomplete due to missing `JOBP` or `EVNT_TIME` files. The schedule is currently set to `None`, and no actual task upstream/downstream relationships are established.
* **Ab Initio / PySpark Code Absence:** The source object does not define an Ab Initio graph. The PySpark script target (`dw_dwh_dummy_absd_plato_tarife.py`) is generated as an empty/log-only placeholder script in GCS.
* **GCP Variables:** The developer must replace all capitalised placeholders (`YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`) in the deployment environment or variable configuration.
* **ENDED_SKIPPED / Skipped task rules:** No special trigger rules (e.g. `ALL_DONE`) are implemented to ensure safe upstream skip propagation.

---

## SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
# Import Airflow DAG core and operators
# Import timedelta for execution settings
# Import EmptyOperator as boundaries
# Import DataprocSubmitJobOperator for standard PySpark job submission

# ── GCP Configuration ────────────────────────────────────
# Define environment-specific placeholders
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# Path to the PySpark script mapped from the dummy UC4 task name
PYSPARK_SCRIPT_PATH = "gs://{bucket}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py".format(bucket=GCS_BUCKET_NAME)

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),  # Based on UC4 last modified date metadata
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
# Define DAG dw_dwh_plato_tarif_mapping_taeglich_dag
# No schedule defined due to missing JSCH/EVNT_TIME XML exports
# is_paused_upon_creation=False since original UC4 object <Active> was 1
DAG = DAG(
    dag_id="dw_dwh_plato_tarif_mapping_taeglich_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Task Definitions ─────────────────────────────────────

# Boundary start task
start = EmptyOperator(
    task_id="start",
    dag=DAG,
)

# Task: dw_dwh_dummy_absd_plato_tarife
# Mapped from JOBS_UNIX "DW.DWH_DUMMY_ABSD_PLATO_TARIFE"
# Originally printed "Doing nothinig" in UC4 script. Mapped to placeholder PySpark job.
pyspark_job_config = {
    "reference": {
        "project_id": GCP_PROJECT_ID
    },
    "placement": {
        "cluster_name": DATAPROC_CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_SCRIPT_PATH,
        "args": []
    }
}

dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    job=pyspark_job_config,
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT_ID,
    # Standard dynamic job_id suffix generation to avoid collisions
    job_id="dw_dwh_dummy_absd_plato_tarife_{{ run_id | ts_nodash | lowercase }}",
    dag=DAG,
)

# Boundary end task
end = EmptyOperator(
    task_id="end",
    dag=DAG,
)

# ── Dependencies ─────────────────────────────────────────
# Execution path of the single dummy workflow:
start >> dw_dwh_dummy_absd_plato_tarife >> end
```
```

---

## SECTION 3 — ADDING CONTEXT THE MCP COULD NOT SEE

### 1. Job Dependencies and Lineage
These details are based strictly on the pre-collected context information:
* **Upstream Dependencies:** None discovered in the provided metadata.
* **Downstream Dependencies:** 
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` — **Not Yet Migrated**.
  * *Wiring Pattern:* Because this downstream parent job chain is not yet migrated, the final task link cannot be fully automated in Airflow. This dummy task must be integrated as a step inside the final DAG that migrates `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` when that parent job undergoes conversion.
* **Lineage Edges:**
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` `--[CALLS_HTTP]-->` `EXT:DWHDWH1P` (confidence 0.85)
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` `--[USES_PACKAGE]-->` `PACKAGE:DW.UNIX.ISTNS` (confidence 0.80)
  * *Cloud Resolution:* The legacy system linked this job to host/endpoint environment configurations. On Google Cloud Composer, these can be managed through Airflow Connections (`dwhdwh1p_connection`) if HTTP actions are ever built out. Currently, since the task script contains only `:print Doing nothinig`, these lineage markers represent unused legacy configuration structures.

### 2. Execution Order and Scheduling
* **Execution Order:** There is no execution order metadata inside this single `JOBS_UNIX` task other than executing its internal inline script.
* **Scheduling:** None discovered. No `JSCH` (Schedule) or `EVNT_TIME` files were found. It is inherited from its parent chain, `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (Daily Plato Tariff Mapping workflow). The target Composer task must trigger within the context of that Daily DAG.

### 3. Environment-Specific Values (GCP Configuration)
According to the **ENVIRONMENT VARIABLE POLICY**, variables must be classified by their target roles in GCP:

#### GLOBAL Variables (Environment-wide configuration)
These represent target GCP infrastructure and are shared across all jobs in the environment. At runtime, they are sourced from Airflow's config store (`Variable.get`) or operating system environment variables (`os.environ.get`).
* **`GCP_PROJECT`**: The target Google Cloud Project ID. Sourced at runtime via `Variable.get("GCP_PROJECT")`.
* **`GCP_REGION`**: The target region (e.g., `europe-west3`). Sourced at runtime via `Variable.get("GCP_REGION")`.
* **`GCS_BUCKET`**: The target deployment Cloud Storage bucket. Sourced at runtime via `Variable.get("GCS_BUCKET")`.
* **`DATAPROC_CLUSTER`**: The target Cloud Dataproc execution cluster. Sourced at runtime via `Variable.get("DATAPROC_CLUSTER")`.

#### JOB-SPECIFIC Variables
These values are particular to this job and are explicitly configured inline:
* **`task_id`**: `"dw_dwh_dummy_absd_plato_tarife"`
* **`dag_id`**: `"dw_dwh_plato_tarif_mapping_taeglich_dag"`
* **`login_connection`**: `DW.UNIX.ISTNS` (Maps to Airflow connection identifier if legacy UNIX access is still simulated; otherwise retired).

### 4. Target File Plan
The source file will map to a Python Airflow DAG representation inside a mirrored target repository folder:

* **Target File Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
* **Language:** `Python`
* **Source:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
* **Folder Integrity:** The target repository path strictly mirrors the legacy folder structures as specified by the **FOLDER INTEGRITY RULE**.

### 5. Risks and Manual Steps
* **Downstream Integration Risk:** The parent workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml`) is marked **not yet migrated**. The task definition in `dw_dwh_dummy_absd_plato_tarife.py` is configured to run standalone for now. Once the parent workflow is migrated, this task must be merged or referenced within that parent DAG.
* **Overhead of Dataproc Execution:** The MCP conversion tool automatically assumed a `DataprocSubmitJobOperator` mapping. Submitting an empty Dataproc job incurs high overhead (cluster execution time, script management in GCS) simply to perform no action. 
* **Mitigation Recommendation (Alternative Implementation):** We recommend using a lightweight `PythonOperator` to execute the original print statement. This avoids any Dataproc resource allocation and executes the identical placeholder action natively inside Airflow.

---

## SECTION 4 — LIGHTWEIGHT ALTERNATIVE TARGET CODE

To respect the **OUTPUT/PRINT LITERAL RULE**, the original print statement (`Doing nothinig`) must be carried over with **exact character-for-character accuracy**, including the typographical spelling error:

```python
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ── GCP Configuration (GLOBAL Sourcing) ──────────────────
# Global infrastructure targets sourced via Airflow Variable store
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCP_REGION = Variable.get("GCP_REGION", default_var=None)

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
# is_paused_upon_creation=False reflects UC4 active state
dag = DAG(
    dag_id="dw_dwh_plato_tarif_mapping_taeglich_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,  # Handled by downstream parent orchestrator once migrated
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Dummy Script Execution ───────────────────────────────
def execute_dummy_task():
    # OUTPUT/PRINT LITERAL RULE: Must match legacy text exactly: "Doing nothinig"
    print("Doing nothinig")

# ── Task Definitions ─────────────────────────────────────
start = EmptyOperator(
    task_id="start",
    dag=dag,
)

dw_dwh_dummy_absd_plato_tarife = PythonOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    python_callable=execute_dummy_task,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end
```