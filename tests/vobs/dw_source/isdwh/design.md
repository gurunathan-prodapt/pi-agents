# MIGRATION DESIGN DOCUMENT
**Job ID:** `DW.DWH_ABPZ_KKM_AIL_AGENT`  
**Target Architecture:** Cloud Composer (Apache Airflow) + Dataproc Serverless (PySpark)  

---

## 1. PRE-MIGRATION CONTEXT & LINEAGE ANALYSES

The legacy orchestration job is managed by UC4/Automic as a Unix Job (`JOBS_UNIX`), wrapping a shell execution block. 

### Upstream and Downstream Job Dependencies
* **Upstream Prerequisites:**  
  * **Shared Files Module:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files` (specifically `.dw_init`, `.dw_db`, `.dw_global`). These files have already been migrated, consolidated, and merged in PR #639. The migrated Python models should import or leverage this unified initialization module.
* **Orchestration Sequencing & Call Graph:**  
  * The execution must enforce the ordering retrieved from the legacy dependency graph:
    1. Check Ab Initio Poll Check (`DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`)
    2. Read path and monitor definitions (`DW.HOLE_PFAD`)
    3. Initialize framework variables (`.dw_init`)
    4. Execute core business wrapper (`r_alis_objekt`) passing `-o AgentADSLookup.txt -j ABPZ_KKM_AIL_AGENT -x BHB_CCM_PROC_WriteAgentADSLookup.cfg -z 84`
    5. Evaluate execution logs (`DW.LESE_LOG` -> calling `showlog.ksh` on failure)
    6. Complete monitoring registration (`DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` / `DW.DWH_ADM_JOB_MONITOR_END`)

### Shared Components & Resolutions
* **`showlog.ksh`**: Human-confirmed as a real, non-blocking fallback tool. On execution failure in Airflow, standard GCP log outputting to Cloud Logging replaces this utility.
* **`.DW_LOKAL` / `SETPYA.SH`**: Human-reviewed as "no source needed / retired" (confirmed on 2026-07-15).

---

## 2. VERBATIM UC4-TO-AIRFLOW DAG DESIGN DOCUMENT

The following design is the verbatim output produced by the code migration processor:

```markdown
=== Result for local/home/gurunathan_t/folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_KKM/DW.DWH_KKM_IMPORT_TAEGLICH_JP/DW.DWH_KKM_AI_LOOKUPS_TAEGLICH_GV_JP/DW.DWH_ABPZ_KKM_AIL_AGENT.xml ===
Based on the provided UC4 XML file, here is the detailed Design Document and Airflow DAG Pseudocode blueprint to guide your migration to Apache Airflow.

---

### INPUT VALIDATION STATUS
- **Files Detected:** 1 Unix Job file (`JOBS_UNIX`)
- **Validation Note:** Only one `JOBS_UNIX` file was provided without its parent Job Plan (`JOBP`), Event (`EVNT_TIME`), or Schedule (`JSCH`) files. As a result, certain scheduling, dependency, and parent-level configurations are marked as placeholders or assumptions in the tables below. These must be verified when the remaining XML files are processed.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
This workflow encapsulates a daily data warehouse (DWH) lookup processing job (`DW.DWH_ABPZ_KKM_AIL_AGENT`). The primary function of this task is to execute an Ab Initio graph (`ABPZ_KKM_AIL_AGENT`) which extracts data and builds a flat-file lookup (`AgentADSLookup.txt`) for the downstream database view `DWH$VI_S_SDM_AGENT_ADS`. The job is automated via Unix scripts running on the host `|DWHDWH1P|` using Ab Initio runtime environments.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABPZ_KKM_AIL_AGENT` | `JOBS_UNIX` | `<Active>1</Active>` (Active) | Builds the flat-file lookup for view `DWH$VI_S_SDM_AGENT_ADS` using Ab Initio. |

## 3. Airflow DAG Properties
*Note: Since the parent `JOBP` and `EVNT_TIME` objects were not supplied, these properties are formulated under the standard assumptions of a daily batch execution window.*

| Property | Value | Note / Source |
| :--- | :--- | :--- |
| **dag_id** | `dw_dwh_abpz_kkm_ail_agent_dag` | Derived from the Unix Job name |
| **schedule** | `0 3 * * *` | Placeholder (Assumed daily execution at 03:00 AM) |
| **start_date** | `datetime(2023, 6, 11)` | Placeholder based on export year |
| **catchup** | `False` | Recommended standard practice |
| **max_active_runs** | `1` | Enforces single-instance run integrity |
| **is_paused_upon_creation**| `False` | Standard deployment (`<Active>1</Active>` in source) |
| **default_args** | `{"owner": "DWH", "retries": 1, "retry_delay": timedelta(minutes=5)}` | Standard fallback limits |

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_abpz_kkm_ail_agent` | `DataprocSubmitJobOperator` | `agent_ads_lookup.py` | Project, Region, Cluster placeholders | `1` | `5 min` | None | None (`CaleOn="0"`) | `False` | None | Converts the underlying Ab Initio wrapper script logic. |

## 5. Task Dependency Map
Because only a single Unix job file was provided, the dependency map is direct:

`start >> dw_dwh_abpz_kkm_ail_agent >> end`

*   **start**: Placeholder DummyOperator to represent DAG initialization.
*   **dw_dwh_abpz_kkm_ail_agent**: The main Dataproc PySpark execution step.
*   **end**: Placeholder DummyOperator to represent DAG completion.

## 6. Parameter and Variable Mapping
The UC4 script logic uses variables and parameters extracted as follows:

| UC4 Parameter | Value / Source | Airflow Equivalent / GCS Path |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'ABPZ_KKM_AIL_AGENT'` | Derived PySpark job script: `gs://YOUR_BUCKET_NAME/pyspark_scripts/agent_ads_lookup.py` |
| `-j` | `ABPZ_KKM_AIL_AGENT` | Maps to PySpark entry point |
| `-o` | `AgentADSLookup.txt` | Target flat-file output destination parameter within PySpark script |
| `-x` | `/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg` | Config mapping inside PySpark application configuration |
| `&RUECKBLICK` | `GET_VAR('DW.VARIABLEN_DWK_KKM','KKM_Rueckblick_Ladedatum')` | Read as an Airflow Variable or dynamically passed via DAG parameter (`params`) |
| **Login / Host** | `DW.UNIX.ISTNS` / `|DWHDWH1P|` | Mapped to Dataproc Cluster and Airflow Connection Profiles |

## 7. Error Handling and Retry Strategy
- **Retries**: Default parameter set to `1` attempt with `5 minutes` delay.
- **Postcondition Analysis**: The script contains standard headers/footers (`:inc DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`, `:inc DW.LESE_LOG`, and `:inc DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`). These elements perform basic code-checking operations. Airflow natively captures non-zero exit codes from Dataproc and marks the step as failed, naturally triggering retries.
- **Sync Objects**: No `<SYNCREF>` active rows were defined in this XML file. Therefore, standard DAG concurrency configuration (`max_active_runs=1`) is sufficient.

## 8. Developer Notes
*   **GCP Resource Definitions**: You must replace all `YOUR_` prefixed placeholders (e.g., `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME`) in the Airflow environment configuration.
*   **Logical Conversion**: The Ab Initio utility execution (`r_alis_objekt`) and its associated control parameters (`-o AgentADSLookup.txt`, config files) must be refactored into equivalent Spark SQL/Dataframe operations within the `agent_ads_lookup.py` script.
*   **Variable Fetching**: The logic retrieving `KKM_Rueckblick_Ladedatum` via UC4 `GET_VAR` should be replaced with `Variable.get("kkm_rueckblick_ladedatum")` or injected as a logical Airflow execution runtime date.
*   **Missing Context Files**: Since no parent `JOBP` or schedule context was supplied, ensure that once the parent process files are available, any upstream requirements or specific temporal constraints are appended to this workflow.

---

# SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable

# ── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "DWH",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 11),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id="dw_dwh_abpz_kkm_ail_agent_dag",
    default_args=default_args,
    description="Refactored UC4 DW.DWH_ABPZ_KKM_AIL_AGENT Daily Lookup Build Flow",
    schedule="0 3 * * *",  # Assumed Daily at 03:00 AM - Developer should verify against actual schedule/event files
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Logical Parameters Extraction ────────────────────────
# Maps: GET_VAR('DW.VARIABLEN_DWK_KKM','KKM_Rueckblick_Ladedatum')
kkm_rueckblick_ladedatum = Variable.get("kkm_rueckblick_ladedatum", default_var="UNDEFINED")

# ── Tasks ───────────────────────────────────────────────

start = EmptyOperator(
    task_id="start",
    dag=dag,
)

# Task converting the main Unix job and Ab Initio command logic
dw_dwh_abpz_kkm_ail_agent = DataprocSubmitJobOperator(
    task_id="dw_dwh_abpz_kkm_ail_agent",
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job={
        "reference": {
            # Dynamic job_id generation using runtime variables
            "project_id": GCP_PROJECT_ID,
            "job_id": "dw_dwh_abpz_kkm_ail_agent_{{ run_id | ts_nodash | lower }}",
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/agent_ads_lookup.py",
            "args": [
                "--job_kennung", "ABPZ_KKM_AIL_AGENT",
                "--output_file", "AgentADSLookup.txt",
                "--config_file", f"gs://{GCS_BUCKET}/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg",
                "--rueckblick_ladedatum", kkm_rueckblick_ladedatum,
                "--exec_date", "{{ ds }}"
            ],
        },
    },
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
start >> dw_dwh_abpz_kkm_ail_agent >> end
```
```

---

## 3. ADDITIONAL CONTEXT & ENVIRONMENT VALUES MAPPING

### Target File Plan
The workflow will consist of the following targets deployed onto the GCP target infrastructure:
1. **DAG File:** `dags/dw_dwh_abpz_kkm_ail_agent_dag.py` (Python)
2. **PySpark Executable:** `pyspark_scripts/agent_ads_lookup.py` (Python / PySpark) - Implements the actual Ab Initio processing logic for the Agent ADS Lookups.
3. **Environment/Dependency Imports:** Uses the previously migrated `vobs/dw_source/isdwh/allgemein/is/env/dw_files` modules.

### Environment-Specific Variables Mapping (Strict Compliance)

| Legacy Environment Construct | Target Classification | Resolution Mechanism | Canonical Name |
| :--- | :--- | :--- | :--- |
| `|DWHDWH1P|` | **GLOBAL** | Environment-wide variable in Airflow | `GCP_PROJECT` / `DATAPROC_CLUSTER` |
| `DWH_HOME` (`GET_VAR('DW.VARIABLEN')`) | **GLOBAL** | Airflow Variable lookup | `Variable.get("dwh_home")` |
| `KKM_Rueckblick_Ladedatum` | **JOB-SPECIFIC** | Retrieved dynamically in DAG Task | `Variable.get("kkm_rueckblick_ladedatum")` |
| `AgentADSLookup.txt` | **JOB-SPECIFIC** | Passed directly in PySpark args | Inline Literal String |

### Literal Output Retention
All log or display items must match legacy outputs exactly. On failure, rather than calling the native custom executable `showlog.ksh`, the Airflow system logs standard messages conforming to:
* `Jobkennung ABPZ_KKM_AIL_AGENT eingetragen für` `<JOBNAME>`
* `Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert` `<RET_VAL>` `beendet.`

---

## 4. RISKS & MANUAL ACTIONS

* **SOURCE: NOT FOUND** — `showlog.ksh` — This utility script is used to print logs on step failure. Though marked as non-blocking, it lacks direct code context. Ensure the DAG Task failure triggers equivalent Cloud Logging visibility.
* **MISSING COMPONENT DATA:** The underlying Ab Initio Graph logic specified in `BHB_CCM_PROC_WriteAgentADSLookup.cfg` (`BHB_Graph="BHB_CCM_PROC_WriteAgentADSLookup"`) was not supplied. A PySpark code-build step is required to translate the processing from the view `DWH$VI_S_SDM_AGENT_ADS` into Spark SQL to write `AgentADSLookup.txt`. A manual verification is needed to confirm the schemas involved.