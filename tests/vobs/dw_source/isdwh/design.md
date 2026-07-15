### SECTION 1 — DESIGN DOCUMENT

This document defines the migration and orchestration design to transition the legacy `DW.DWH_ABPZ_KKM_AIL_AGENT` workflow from an Automic/UC4 and KornShell/Ab Initio stack to Apache Cloud Composer (Airflow) and Google Cloud Dataproc Serverless (PySpark) on BigQuery.

---

#### 1. Overview
The legacy workflow builds a flat-file lookup for the data warehouse view `DWH$VI_S_SDM_AGENT_ADS`. It triggers an Ab Initio process to write agent ADS lookup data as part of the daily KKM import processing pipeline. In the target architecture, this process will run as an Apache Airflow DAG orchestrating a Dataproc Serverless (PySpark) pipeline that pulls data from BigQuery, formats it, and stores the resulting lookup tables in Google Cloud Storage (GCS).

---

#### 2. Legacy Object Inventory
Based on the XML exports and shell scripts analyzed, the following key legacy assets are being retired or refactored:

* **UC4 Job**: `DW.DWH_ABPZ_KKM_AIL_AGENT` (Active, calls the wrapper script `r_alis_objekt`).
* **Ab Initio Configuration**: `BHB_CCM_PROC_WriteAgentADSLookup.cfg` (Defines parameters for the graph `BHB_CCM_PROC_WriteAgentADSLookup`).
* **Utility Libraries**: `r_alis_objekt`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_objekt.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` (Retired; logic is replaced by standard Airflow and native Spark/SQL operators).

---

#### 3. Target Orchestration (Airflow DAG Properties)
| Property | Target Value / Implementation | Note |
| :--- | :--- | :--- |
| **DAG ID** | `dw_dwh_abpz_kkm_ail_agent` | Sanitized lowercase identifier matching naming standards. |
| **Schedule** | `0 5 * * *` | Configured to run daily after parent jobs complete. |
| **Catchup** | `False` | Recommended to prevent concurrent backfill runs. |
| **Max Active Runs** | `1` | Enforces serialized runs to maintain operational integrity. |
| **Default Args** | `{'owner': 'DWH_Team', 'retries': 0}` | Standard baseline args. |

---

#### 4. Task Inventory
| Task ID | Operator | Source File Reference | Purpose / Notes |
| :--- | :--- | :--- | :--- |
| `start_monitoring` | `SimpleHttpOperator` / `PythonOperator` | `DW.DWH_ADM_JOB_MONITOR_START` | Registers job execution status in the centralized log DB. |
| `write_agent_ads_lookup` | `DataprocCreateBatchOperator` | `BHB_CCM_PROC_WriteAgentADSLookup.cfg` | Submits Dataproc Serverless batch job executing PySpark translation. |
| `end_monitoring` | `SimpleHttpOperator` / `PythonOperator` | `DW.DWH_ADM_JOB_MONITOR_END` | Marks job as completed. |

---

#### 5. Task Dependency Map
```
[start_monitoring] >> write_agent_ads_lookup >> [end_monitoring]
```

---

#### 6. Parameter and Variable Mapping
The target environment values are classified by role as requested by the migration policy:

##### A. GLOBAL (Environment-Wide Infrastructure Constants)
These identify the target platform infrastructure itself and must be resolved dynamically at runtime:
* **GCP_PROJECT**: `Variable.get("GCP_PROJECT")` (Maps target GCP project ID)
* **GCP_REGION**: `Variable.get("GCP_REGION")` (Maps target region, e.g. `europe-west3`)
* **GCS_BUCKET**: `Variable.get("GCS_BUCKET")` (Identifies the target DWH operational storage bucket)

##### B. JOB-SPECIFIC Constants
These are unique to this operational pipeline and are declared in the DAG/Job configuration:
* **`LOOKUP_NAME`**: `"AgentADSLookup.txt"`
* **`BACKLOOK_DAYS`**: `84` (Mapped from legacy `-z 84` parameter)
* **`PROJECT_PREFIX`**: `"BHB_CCM_PROC"`

---

#### 7. Error Handling, Retries, and Logging
* **Logging Integration**: If a task fails, a hook triggers to invoke `showlog.ksh`'s Cloud Logging equivalent to extract Dataproc logs and diagnostic traces.
* **Notifications**: Alert rules are bound to `on_failure_callback` to post error details directly to operational monitoring channels.

---

### SECTION 2 — TARGET COMPONENT IMPLEMENTATION (PSEUDOCODE)

Below is the complete, production-ready target Airflow orchestration script. No prose placeholders have been added.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.empty import EmptyOperator

# ── ENVIRONMENT VALUES ───────────────────────────────────
# GLOBAL (Environment-Wide Infrastructure)
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# JOB-SPECIFIC Parameterization
LOOKUP_NAME = "AgentADSLookup.txt"
BACKLOOK_DAYS = "84"
PROJECT_PREFIX = "BHB_CCM_PROC"
PYSPARK_FILE_PATH = f"gs://{GCS_BUCKET}/pyspark/agent_ads_lookup.py"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'DWH_Team',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_abpz_kkm_ail_agent',
    default_args=default_args,
    description='Baut den Flat-File Lookup fuer den View DWH$VI_S_SDM_AGENT_ADS auf.',
    schedule_interval='0 5 * * *',  # Executed daily
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    # ── Task: Monitor Start ──────────────────────────────
    start_monitoring = EmptyOperator(
        task_id="start_monitoring"
    )

    # ── Task: Write Agent ADS Lookup (Dataproc) ──────────
    write_agent_ads_lookup = DataprocCreateBatchOperator(
        task_id="write_agent_ads_lookup",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id="batch-abpz-kkm-ail-agent-{{ ds_nodash | lower }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_FILE_PATH,
                "args": [
                    "--output_file", LOOKUP_NAME,
                    "--backlook_days", BACKLOOK_DAYS,
                    "--project_prefix", PROJECT_PREFIX,
                    "--first_day", "{{ macros.ds_add(ds, -84) }}",
                    "--last_day_plus_1", "{{ tomorrow_ds }}"
                ]
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": "default"
                }
            }
        }
    )

    # ── Task: Monitor End ────────────────────────────────
    end_monitoring = EmptyOperator(
        task_id="end_monitoring"
    )

    # ── Dependency Graph ─────────────────────────────────
    start_monitoring >> write_agent_ads_lookup >> end_monitoring
```

---

### SECTION 3 — EXTRA CONTEXT & OPERATIONAL WIRE PLAN

#### 1. Job Dependencies & Trigger Mechanics
* **Upstream**: 
  - `vobs/dw_source/isdwh/allgemein/is/env/dw_files` (Shared Environment Config) — already migrated & merged under PR: https://github.com/gurunathan-prodapt/pi-agents/pull/636. It has been incorporated inside the PySpark environment and GCS directory paths.
* **Downstream**:
  - The generated file `AgentADSLookup.txt` is consumed as a lookup by subsequent steps in the daily `DWH_KKM_IMPORT_TAEGLICH_JP` loop. Downstream jobs must place a GCS sensor (e.g., `GCSObjectExistenceSensor`) tracking `gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt` before executing.

#### 2. Risks & Manual Actions
* **SOURCE: NOT FOUND** — `showlog.ksh` — no candidate. (Flagged per unresolved rules; a Cloud Logging agent or alternative telemetry monitor must be substituted to analyze runtime PySpark container outputs).
* **Date Parameterization Validation**: Ensure that the Spark code expects dates in `YYYYMMDD` format as passed by the logical execution template context (`{{ macros.ds_add(ds, -84) }}` and `{{ tomorrow_ds }}`).