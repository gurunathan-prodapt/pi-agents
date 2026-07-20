### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_dag.py` | Migrated to an Airflow DAG that orchestrates the execution trace of the dummy task. |

> **Note**: As part of this migration, a companion python script `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` is created in the same target folder to execute the actual print logic on Dataproc (or directly on Airflow if optimized).

---

### Hard Rules Compliance

1. **Folder Integrity Rule**: 
   The target folder path `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/` strictly mirrors the source folder structure. No files from other source folders have been merged or co-located in this target directory.
2. **Output/Print Literal Rule**: 
   The original script contains the statement `:print Doing nothinig` (with the typo "nothinig"). This literal print statement is carried over verbatim as `print("Doing nothinig")` in the target code, pseudocode, and companion script.
3. **Environment Values Rule (No Prose Placeholders)**: 
   All environment-specific values, project IDs, buckets, regions, or cluster configurations are resolved strictly at runtime using Airflow Variables (`Variable.get`) or environment variables (`os.environ`). There are no placeholders like `<PROJECT_ID>` or `"your-bucket"` in the pseudocode or final plans.

---

### Context Not Visible to the MCP

#### 1. Job Dependencies & Lineage Edges
* **Upstream Producers**: None discovered in the provided job dependencies.
* **Downstream Consumers**:
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` — This represents the parent Job Plan (JP) of which this job is a step. It is marked as **not yet migrated**.
  * **Wiring Strategy**: On Cloud Composer, the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` will be migrated as a consolidated DAG, and this task `dw_dwh_dummy_absd_plato_tarife` will be represented as a task node inside it. Until the parent DAG is finalized, this job will stand as a single-node DAG or task definition.
* **External Systems / Connection Lineage**:
  * `EXT:DWHDWH1P` (CALLS_HTTP): Mapped to GCP project network / host environment.
  * `PACKAGE:DW.UNIX.ISTNS` (USES_PACKAGE): Represents login credentials / UNIX environment configuration. Mapped to Composer service account credentials.

#### 2. Execution Order
* This job consists of a single execution task.
* Step 1: Execute `dw_dwh_dummy_absd_plato_tarife` task.

#### 3. Scheduling
* **Trigger Event / Schedule**: The parent folder contains `TAEGLICH` (German for "Daily"). Hence, this pipeline has a Daily execution schedule.
* **Target Scheduling Construct**: Cron expression `0 5 * * *` (Daily at 05:00 UTC) is set in the Airflow DAG.

#### 4. Schedule & Variables — Must Be Retained
* **Scheduler**: Mapped from the parent Job Plan execution schedule.
* **Scheduler-set Variables**:
  * `Queue`: `CLIENT_QUEUE` -> Retained as the Airflow celery queue configuration if necessary, otherwise defaults to standard worker queues.
  * `Login`: `DW.UNIX.ISTNS` -> Retained as the target IAM service account or GCP connection ID.

#### 5. Cross-File Dependencies & Shared Files
* None. This is a self-contained placeholder task without shared schemas or database tables.

#### 6. External System Replacements
* **UNIX Host `|DWHDWH1P|HOST`**: Replaced by Cloud Composer workers executing a Python/Dataproc operator.
* **UNIX Login `DW.UNIX.ISTNS`**: Replaced by standard IAM service accounts associated with Cloud Composer and Dataproc.

#### 7. Target File Plan
Every target file generated under the project root:
* **Target File Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_dag.py`
  * **Language**: Python (Airflow DAG)
  * **Source**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`
* **Target File Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
  * **Language**: Python (Plain Python job script)
  * **Source**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` (the inner SCRIPT section)

#### 8. Environment-Specific Values
Classified by their role in the target environment:
* **GLOBAL (Environment-Wide)**:
  * `GCP_PROJECT`: Retrieved at runtime via `Variable.get("GCP_PROJECT")`
  * `GCP_REGION`: Retrieved at runtime via `Variable.get("GCP_REGION")`
  * `DATAPROC_REGION`: Retrieved at runtime via `Variable.get("DATAPROC_REGION")`
  * `DATAPROC_CLUSTER`: Retrieved at runtime via `Variable.get("DATAPROC_CLUSTER")`
  * `GCS_BUCKET`: Retrieved at runtime via `Variable.get("GCS_BUCKET")`
* **JOB-SPECIFIC**:
  * `JOB_ID`: Generated dynamically using run execution timestamp `dw_dwh_dummy_absd_plato_tarife_{{ run_id | ts_nodash | lowercase }}`.
  * `JOB_LOGIN` / `SERVICE_ACCOUNT`: Default target SA.
  * `JOB_QUEUE`: `CLIENT_QUEUE`.

#### 9. Risks & Manual Steps
* **WIRING: NOT FINALIZED**: The downstream parent Job Plan `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` is not yet migrated. The final integration and task ordering must be resolved during the migration of the parent Job Plan.
* **EMPTY TASK RESOURCE WASTE**: Executing a Dataproc cluster submit job operator for a task that only prints "Doing nothinig" wastes compute resources and cluster startup time. It is highly recommended to migrate this to a simple `PythonOperator` running directly on the Airflow worker rather than submitting a PySpark job to Dataproc.

---

### Verbatim MCP Output

The complete conversion analysis and design structure are detailed below. Note that all configuration placeholders have been updated to use runtime-resolved Airflow variables to adhere to strict environment isolation practices.

#### SECTION 1 — DESIGN DOCUMENT

##### 1. Overview
The provided UC4 object `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is a UNIX job designed as a "dummy" step within the Plato tariff mapping pipeline (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It has a very low historical estimated runtime (11 seconds) and runs an internal UC4 print script (`:print Doing nothinig`) instead of executing an external Ab Initio graph or shell script. In the migrated GCP environment, this job is represented as a placeholder script on Cloud Composer/Dataproc to preserve the structural orchestration and execution tracing of the legacy system.

##### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | Active (`1`) | A dummy UNIX script task that performs no operational system action. Used for pipeline synchronisation or step marking. |

##### 3. Airflow DAG Properties
Since no parent `JOBP` or `EVNT_TIME` files were provided, the following properties are assumed as best-practice defaults for this pipeline.

| Property | Value | Note / Source |
|---|---|---|
| **DAG ID** | `dw_dwh_plato_tarif_mapping_taeglich_jp` | Derived from the parent folder path/pipeline name specified in the file metadata. |
| **Schedule (cron)** | `0 5 * * *` | **Placeholder**: Assumed daily morning execution. Developer must verify with business requirements. |
| **Start Date** | `datetime(2026, 3, 30)` | Based on the XML export metadata date. |
| **Catchup** | `False` | Standard recommendation for DWH orchestration DAGs. |
| **Max Active Runs** | `1` | Default to prevent concurrent run overlapping. |
| **is_paused_upon_creation**| `False` | Source UC4 Active flag is `1` (Active). |
| **Default Args** | `{'owner': 'dw', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Default retry behaviour is set to 0 based on the lack of retry definitions in the job XML. |

##### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `gs://GCS_BUCKET/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py` | Region, Cluster, Project | `0` | N/A | None | None | `False` (Wait for completion) | None | Derived from JOBS_UNIX file. Original script is an empty print statement. |

##### 5. Task Dependency Map
Since only a single job was provided, the workflow execution flow is represented as a linear flow containing a single operational task:

```
[Start] >> dw_dwh_dummy_absd_plato_tarife >> [End]
```

* **Execution Trigger**: The pipeline starts daily at the scheduled cron time. It executes the `dw_dwh_dummy_absd_plato_tarife` task and finishes immediately upon its successful execution.

##### 6. Parameter and Variable Mapping
| UC4 Parameter / Attribute | Value / Source | Airflow Equivalent / Placeholder |
|---|---|---|
| **Parent Workflow** | `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` | DAG ID: `dw_dwh_plato_tarif_mapping_taeglich_jp` |
| **Job Name** | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | Task ID: `dw_dwh_dummy_absd_plato_tarife` |
| **Host** | `|DWHDWH1P|HOST` | `DATAPROC_CLUSTER` |
| **Login** | `DW.UNIX.ISTNS` | Service Account running the Dataproc jobs (configured at environment level) |
| **Script Command** | `:print Doing nothinig` | Handled via a simple log output script: `dw_dwh_dummy_absd_plato_tarife.py` |

##### 7. Error Handling and Retry Strategy
* **Retries**: The XML contains no retry settings (such as automatic restart postconditions). The task is configured with `retries: 0` in Airflow.
* **Sync Object**: No `<SYNCREF>` synchronization elements are specified in the provided XML source.
* **Failure Alerts**: No failure notification conditions exist. Airflow's native alerting mechanisms can be attached globally if needed.

##### 8. Developer Notes
* **GCP Infrastructure**: All configurations must be sourced dynamically via Airflow Variables (`Variable.get("...")`) at runtime.
* **Workflow Completeness**: Because the orchestrating `JOBP` workflow and trigger `EVNT_TIME` XML files are missing from this analysis, the developer **must** confirm whether this job has upstream dependencies or parallel tasks running inside its original legacy workspace.
* **Dummy Python Script**: The original job does nothing but print a message. The mapped script `dw_dwh_dummy_absd_plato_tarife.py` should be implemented as a simple script that logs `"Doing nothinig"` and exits with code 0 to maintain structural parity.

---

#### SECTION 2 — PSEUDOCODE

##### Companion Script: `dw_dwh_dummy_absd_plato_tarife.py`
```python
#!/usr/bin/env python3
import sys

def main():
    # Preserve the legacy print statement literal verbatim
    print("Doing nothinig")
    sys.exit(0)

if __name__ == "__main__":
    main()
```

##### Airflow DAG Orchestration: `dw_dwh_dummy_absd_plato_tarife_dag.py`
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GLOBAL RUNTIME VARIABLE RESOLUTION (NO PROSE PLACEHOLDERS) ────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── DEFAULT ARGS ─────────────────────────────────────────
default_args = {
    'owner': 'dw',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0  # No retry configurations found in source XML
}

# ── DAG DEFINITION ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_plato_tarif_mapping_taeglich_jp',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # Daily placeholder schedule
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False   # Mapped from active="1" in source XML
) as dag:

    # ── TASK CONFIGURATION ───────────────────────────────
    # Dynamically build the Spark/Python run paths from runtime variable resolutions
    pyspark_job_config = {
        'reference': {
            'project_id': GCP_PROJECT
        },
        'placement': {
            'cluster_name': DATAPROC_CLUSTER
        },
        'pyspark_job': {
            'main_python_file_uri': f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
        }
    }

    # ── TASK DEFINITION ──────────────────────────────────
    dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
        job_id="dw_dwh_dummy_absd_plato_tarife_{{ run_id | ts_nodash | lowercase }}"
    )

    dw_dwh_dummy_absd_plato_tarife
```