An implementation-ready **MIGRATION DESIGN DOCUMENT** has been prepared for the assembled job `DW.DWH_ABPZ_KKM_AIL_AGENT`. 

This design follows the prescribed **High-confidence** DE classification pattern: migrating UC4 job execution logic to **Cloud Composer (Airflow)** and porting the underlying wrapper scripts and Ab Initio framework checks to a **Dataproc Serverless (PySpark)** execution model.

---

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_ABPZ_KKM_AIL_AGENT.xml` | `dags/dw_dwh_abpz_kkm_ail_agent.py` | Migrated to an Airflow DAG that orchestrates step validation and schedules the Dataproc PySpark pipeline. |
| `vobs/dw_source/isdwh/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg` | Retired (Logic migrated to PySpark arguments) | The Ab Initio environment variable mappings are passed dynamically as arguments/properties to the PySpark operator. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/r_alis_objekt` | Retired (Folded into Airflow Operator & PySpark logic) | Framework utility verifying execution requirements and invoking GDE graphs. Mapped to Airflow workflow tasks and pre-checks. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_objekt.ksh` | Retired (Folded into Airflow Operator & PySpark logic) | Library for object freshness and delete intervals. Equivalent logic is handled directly in PySpark. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/f_alis_msgerr.ksh` | Retired (Standardized GCP Logging) | Standardized Oracle error-logging library. Replaced by native Airflow task loggers and Google Cloud Logging. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_date.ksh` | Retired (Python native datetime) | Date calculation library utilizing Oracle SQLPlus. Replaced by Python's native `datetime` module. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parameter.ksh` | Retired | Mapped metrics and systems. Standardized parameterization is handled natively within the Airflow DAG context. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_sqlplus.ksh` | Retired | Helper functions to run SQL*Plus scripts with error handling. This framework component is retired on GCP. |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC.xml` | Retired (Orchestrated natively via Composer) | UC4 polling include that blocked the workflow until Ab Initio was ready. Managed via Airflow task scheduling. |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC.xml` | Retired (Orchestrated natively via Composer) | Updates Ab Initio status variables at completion. Managed via native Airflow DAG state management. |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.HOLE_PFAD.xml` | Retired | Environment path include. Replaced by Cloud Composer environmental configs. |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.LESE_LOG.xml` | Retired | Script to read execution return codes. Replaced by native Airflow exception bubbling. |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_ADM_JOB_MONITOR_START.xml` | Retired | Job monitoring tracking helper. Replaced by Google Cloud Monitoring and Airflow UI metrics. |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_ADM_JOB_MONITOR_END.xml` | Retired | Job monitoring tracking helper. Replaced by Google Cloud Monitoring and Airflow UI metrics. |

---

## SECTION 1 — DESIGN DOCUMENT (VERBATIM MCP OUTPUT)

### 1. Overview
The **DW.DWH_ABPZ_KKM_AIL_AGENT** workflow represents a single UNIX-based job from the UC4 system. It builds a flat-file lookup (`AgentADSLookup.txt`) supporting the `DWH$VI_S_SDM_AGENT_ADS` database view. The job executes an Ab Initio graph called `ABPZ_KKM_AIL_AGENT` via a custom wrapper script and runs on a daily schedule as part of the broader core data warehouse (`DWH_KERN`) daily load sequence.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABPZ_KKM_AIL_AGENT` | `JOBS_UNIX` | `1` (Active) | Builds the Flat-File Lookup for the view `DWH$VI_S_SDM_AGENT_ADS` |

### 3. Airflow DAG Properties
| Property | Value | Note |
| :--- | :--- | :--- |
| **DAG ID** | `dw_dwh_abpz_kkm_ail_agent` | Sanitized lowercase format |
| **Schedule (Cron)** | `None` | No schedule defined in this single task export; default to manual/external trigger |
| **Start Date** | `datetime(2023, 1, 1)` | Static placeholder date |
| **Catchup** | `False` | Recommended default to prevent historical backfilling |
| **Max Active Runs** | `1` | Prevents concurrent job runs over the same files |
| **Is Paused Upon Creation** | `False` | Mapped from source `<Active>1</Active>` |
| **Default Args (Retries)** | `0` | No retries configured in the UC4 `<RUNTIME>` section |
| **Default Args (Retry Delay)**| `timedelta(minutes=5)` | Default placeholder |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `abpz_kkm_ail_agent` | `DataprocSubmitJobOperator` | `abpz_kkm_ail_agent.py` | Project, Region, Cluster, GCS Bucket placeholders | `0` | `None` | `None` | `None` | `False` (Wait for completion) | `None` | No custom error handling rules found in the XML |

### 5. Task Dependency Map
Since only one job file was provided, the workflow sequence consists of a single linear execution task:

```
[Start] >> abpz_kkm_ail_agent >> [End]
```

* **Execution description:** The DAG starts, immediately runs the PySpark task `abpz_kkm_ail_agent` on Dataproc, and finishes upon successful termination of the Spark session.

### 6. Parameter and Variable Mapping
| UC4 Parameter / Attribute | Value / Source | Airflow Equivalent |
| :--- | :--- | :--- |
| **Object Name** | `DW.DWH_ABPZ_KKM_AIL_AGENT` | **DAG ID:** `dw_dwh_abpz_kkm_ail_agent` |
| **Host** | `\|DWHDWH1P\|HOST` | `YOUR_DATAPROC_CLUSTER_NAME` |
| **Login** | `DW.UNIX.ISTNS` | Service Account running the Dataproc job |
| **Job Key (`-k`)** | Not defined | Passed as job property if required |
| **Job Name (`-j`)** | `ABPZ_KKM_AIL_AGENT` | PySpark file: `abpz_kkm_ail_agent.py` |
| **Config Parameter (`-x`)** | `BHB_CCM_PROC_WriteAgentADSLookup.cfg` | Passed as parameter to PySpark command line if needed |
| **Output File (`-o`)** | `AgentADSLookup.txt` | Target path destination in GCS storage bucket |

### 7. Error Handling and Retry Strategy
* **Retries:** The XML defines `<RUNTIME>` parameters showing no custom retry parameters configured (`<ErtMethodDef>1</ErtMethodDef>`, no retry loops in post-scripts).
* **Sync Object:** No `<SYNCREF>` active elements were provided. Standard `max_active_runs=1` is applied.
* **Failure Actions:** No failure-specific notifications (`EXECUTE OBJECT`) or block policies are present. The DAG will use default Airflow failure mechanisms (marking the task and DAG run as `FAILED`).

### 8. Developer Notes
* **Incomplete Workflow context:** This object is typically triggered within a larger UC4 Job Plan (`JOBP`) parent workflow. If this DAG needs to be triggered by another pipeline, reference its sanitized DAG ID: `dw_dwh_abpz_kkm_ail_agent`.
* **GCP Infrastructure Setup:** Developer must configure variables for:
  * `YOUR_GCP_PROJECT_ID`
  * `YOUR_DATAPROC_REGION`
  * `YOUR_DATAPROC_CLUSTER_NAME`
  * `YOUR_BUCKET_NAME`
* **Ab Initio Logical Porting:** The underlying Ab Initio transformation script (`AgentADSLookup.txt` logic with config file `BHB_CCM_PROC_WriteAgentADSLookup.cfg`) must be rewritten as a PySpark application. This is assumed to reside at `gs://YOUR_BUCKET_NAME/pyspark_scripts/abpz_kkm_ail_agent.py`.
* **Environment Init:** The source shell script loads an environment configuration `. $HOME/.dw_init` and reads a date variable `KKM_Rueckblick_Ladedatum` from UC4 variable sheet `DW.VARIABLEN_DWK_KKM`. This date calculation logic should be handled using Airflow's execution date context templates (`{{ ds }}`) inside the Spark job parameters.

---

## SECTION 2 — PSEUDOCODE (VERBATIM MCP OUTPUT)

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
# UC4 runtime configuration mapping showing no custom retries. 
default_args = {
    'owner': 'dwh_operations',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# ── DAG Definition ───────────────────────────────────────
# Active mapping: UC4 <Active>1</Active> implies is_paused_upon_creation=False
with DAG(
    dag_id='dw_dwh_abpz_kkm_ail_agent',
    default_args=default_args,
    description='Rebuilds flat-file lookup for view DWH$VI_S_SDM_AGENT_ADS',
    schedule_interval=None,  # No calendar schedule defined in source XML task level
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh_kern', 'kkm', 'jobs_unix']
) as dag:

    # ── Task: abpz_kkm_ail_agent ─────────────────────────
    # Map the Ab Initio graph wrapper execution to PySpark running on Dataproc
    pyspark_job_definition = {
        "reference": {
            "project_id": GCP_PROJECT_ID
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/abpz_kkm_ail_agent.py",
            "args": [
                "--config", f"gs://{GCS_BUCKET}/config/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg",
                "--output", f"gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt",
                "--run_date", "{{ ds }}"  # Passing execution context date as parameter replacements
            ]
        }
    }

    # Generate dynamic execution job name with custom suffix safely
    run_pyspark_job = DataprocSubmitJobOperator(
        task_id='abpz_kkm_ail_agent',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_definition,
        job_id="dw_dwh_abpz_kkm_ail_agent_{{ ds_nodash }}_{{ ts_nodash }}"
    )

    # ── Dependencies ─────────────────────────────────────────
    run_pyspark_job
```

---

## SECTION 3 — ENHANCED CONTEXT & IMPLEMENTATION LOGIC

### 1. Execution Order & Orchestration Mapping
To preserve the 14 execution steps extracted from the legacy dependency graph, the target Cloud Composer DAG will resolve the sequence as follows:
* **Pre-Execution Synchronization (Steps 1, 9, 10, 12):** The start and verification routines are mapped native to Airflow's task lifecycle. Task monitoring replaces the UC4 database logging writes.
* **Environment Sourcing & Calculations (Steps 2, 7, 11):** The configurations in `BHB_CCM_PROC_WriteAgentADSLookup.cfg` are passed as command-line configurations directly into Dataproc. Path configurations (`DW.HOLE_PFAD`) are normalized to Cloud Storage (`gs://`) paths.
* **Core Processing (Steps 3, 5, 6, 8):** Managed directly by `r_alis_objekt` inside PySpark. Instead of executing dynamic SQL queries on Oracle database objects, the logic evaluates target dataset tables on BigQuery to compute load freshness metrics.
* **Post-Execution (Steps 4, 13, 14):** Log parsing, error handling (`DW.LESE_LOG`), and job end monitoring are handled automatically via standard Airflow log emission to Cloud Logging.

### 2. Upstream & Downstream Dependencies
* **Upstream Producer:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files` (Shared Environment Configs). These variables are already migrated and merged under Pull Request [PR #645](https://github.com/gurunathan-prodapt/pi-agents/pull/645).
* **Target Integration Integration:** The migrated Python module from PR #645 must be imported within the Airflow environment setup to pull standard corporate configurations and directory maps.

### 3. Environment Variable Policy & Classifications

None of the legacy variables are literal values; they must be resolved at runtime using the following policies:

#### GLOBAL Variables (Environment-wide Constants)
The following keys are mapped to Airflow Variables (`Variable.get("NAME")`) or System Environment variables, resolving to exact target project parameters:
* `GCP_PROJECT` — Target BigQuery processing project ID.
* `GCP_REGION` / `DATAPROC_REGION` — Location constraints for compute clusters.
* `DATAPROC_CLUSTER` — Cluster name executing serverless jobs.
* `GCS_BUCKET` — Standard system bucket containing PySpark runtime logic and the migrated configurations.

#### JOB-SPECIFIC Configurations (Target Execution Context)
These values belong inside the job-level configuration object or parameterized dictionary payload:
```python
JOB_CONFIG = {
    "source_view": "dw_sdm_agent_ads", # Extracted representation of DWH$VI_S_SDM_AGENT_ADS
    "target_table": "dw_lookups.AgentADSLookup", 
    "lookback_days": 84, # Mapped from parameter: -z 84
    "job_identifier": "ABPZ_KKM_AIL_AGENT"
}
```

### 4. OUTPUT/PRINT Literal Integrity
All original legacy logging text is preserved verbatim. If an execution step triggers logging or diagnostic echoes, the output statements must match the source layout exactly (e.g. `"Rueckgabewert: '...'"` and `"Der Status fuer den Pruefjob wurde..."`).

### 5. Risks & Manual Actions
* **SOURCE: UNCONFIRMED CANDIDATE — `showlog.ksh`** — This file is checked as non-blocking but has no real file path. Standard Cloud Logging outputs replace this invocation.
* **BigQuery Logical Rewrite of `DWH$VI_S_SDM_AGENT_ADS`** — The Ab Initio processing logic builds dynamic flat-files that populate a database view context. A manual design validation must confirm the physical BigQuery schemas for the view before finalizing the Spark ingestion pipelines.