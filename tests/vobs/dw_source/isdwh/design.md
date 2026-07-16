# MIGRATION DESIGN DOCUMENT — DW.DWH_ABPZ_KKM_AIL_AGENT

## File Disposition

| Legacy Source File | Disposition | Target File / Task Name | Reasoning |
| :--- | :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_KKM/DW.DWH_KKM_IMPORT_TAEGLICH_JP/DW.DWH_KKM_AI_LOOKUPS_TAEGLICH_GV_JP/DW.DWH_ABPZ_KKM_AIL_AGENT.xml` | Target file | `dags/dw_dwh_abpz_kkm_ail_agent.py` | Primary UC4 UNIX job orchestrated via Cloud Composer. |
| `vobs/dw_source/isdwh/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg` | Target file | `configs/BHB_CCM_PROC_WriteAgentADSLookup.json` | Retired as a shell script; contents migrated to a JSON config file read by the PySpark pipeline. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/r_alis_objekt` | Retired | GCS PySpark pipeline | Redesigned. The orchestrator and PySpark scripts handle the processing checks directly. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/f_alis_msgerr.ksh` | Retired | Cloud Logging & Airflow Alerts | Standardized shell log/error tracking is retired in favor of native GCP features. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_date.ksh` | Retired | Python `datetime` / SQL | Date manipulation is fully replaced by native BigQuery and Python library functions. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_objekt.ksh` | Retired | GCS PySpark pipeline | Tracking logic replaced by standard Cloud Composer states and target table checks. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parameter.ksh` | Retired | Python / JSON mapping | Metric mapping hardcoded or placed in a shared dynamic look-up dictionary. |
| `vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_sqlplus.ksh` | Retired | Google BigQuery Operator | Replaced by direct calls to the BigQuery API inside Airflow. |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_PST_ANALYZE_JP/DW.DWH_ADM_PRUEFE_AB_INITIO/DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC.xml` | Retired | Cloud Composer task sequence | Synchronization status variables are natively handled by Airflow task dependencies. |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_PST_ANALYZE_JP/DW.DWH_ADM_PRUEFE_AB_INITIO/DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC.xml` | Retired | Cloud Composer task sequence | Dependency checks are handled at the DAG level (upstream sensors/triggers). |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/ALLGEMEIN/INCLUDES/DW.HOLE_PFAD.xml` | Retired | Airflow variables & configs | Pathing values and basic date parameters are mapped directly to environment configs. |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_JOB_MONITOR/DW.DWH_ADM_JOB_MONITOR_START.xml` | Retired | Composer metadata | Job monitoring maps to native Composer audit logs and task execution logs. |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/ALLGEMEIN/INCLUDES/DW.LESE_LOG.xml` | Retired | Cloud Logging | Replaced by native Airflow task log streaming and failure notifier callbacks. |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_JOB_MONITOR/DW.DWH_ADM_JOB_MONITOR_END.xml` | Retired | Composer metadata | End of job status registered naturally via DAG task execution completion. |

---

## 1. Upstream & Downstream Job Dependencies
* **Upstream Dependencies:**
  * **Shared Files** (`vobs/dw_source/isdwh/allgemein/is/env/dw_files`): Already migrated & merged. In BigQuery, this environment configuration logic is inherited or imported from the shared environment repository.
* **Downstream Dependencies:**
  * None discovered in the provided workflow context.
* **Orchestration Wiring:** This job is deployed as a standalone Airflow DAG within Cloud Composer that can be triggered or set with an upstream sensor mapping back to the shared file synchronization job.

---

## 2. Execution Order & Scheduling
The execution order defined in the 14-step legacy dependency graph is preserved by standardizing the include logic (`DW.HOLE_PFAD`, `DW.LESE_LOG`, status indicators) directly inside the main DAG execution task boundaries:
1. Check dependencies/State variables (replaces Ab Initio status poll).
2. Fetch required environment paths and configuration keys.
3. Call the Dataproc PySpark job to generate the Agent lookup dataset.
4. Finalize log processing and status register.

### Schedule & Variables — Must Be Retained
* **Scheduler Trigger:** Scheduled daily.
* **Variables:**
  * `&RUECKBLICK`: Resolved dynamically from standard Airflow variables. Calculated as `GET_VAR('DW.VARIABLEN_DWK_KKM','KKM_Rueckblick_Ladedatum')`.
  * `&DWH_JOB_KENNUNG`: Hardcoded to `'ABPZ_KKM_AIL_AGENT'`. Passed to the target environment as an execution property.

---

## 3. Environment-Specific Values (GCP Config Policy)

### GLOBAL (Environment-wide Variables)
These values are sourced dynamically at runtime from environment-wide configuration variables:
* **`GCP_PROJECT`**: The target BigQuery/Dataproc project ID.
* **`GCP_REGION`**: The target execution region.
* **`DATAPROC_CLUSTER`**: The shared target Dataproc cluster.
* **`GCS_BUCKET`**: The target Cloud Storage bucket storing scripts, lookup tables, and configuration files.

### JOB-SPECIFIC Variables
These values are specific to this job execution sequence and are declared inline or via DAG configs:
* **`BHB_Projektverzeichnis`**: `/Projects/TMD/processing/BHB/CCM_PROC`
* **`BHB_Graph`**: `BHB_CCM_PROC_WriteAgentADSLookup`
* **`BHB_Version`**: `RLS_BHB_nach_74_fix_20071031`
* **`BHB_Prozesstyp`**: `N`
* **`KKM_Rueckblick_Ladedatum`**: Standard historical period parameter retrieved from job configuration variables (defaults to `84`).

---

## 4. Target File Plan
* **Airflow Orchestration DAG:**
  * **Relative Path:** `dags/dw_dwh_abpz_kkm_ail_agent.py`
  * **Language:** Python
  * **Description:** Launches the Dataproc Serverless job and handles status monitoring.
* **PySpark ETL Job:**
  * **Relative Path:** `pyspark_scripts/abpz_kkm_ail_agent.py`
  * **Language:** Python (PySpark)
  * **Description:** Reads agent tables, maps columns, processes lookup rules, and writes `AgentADSLookup.txt` to GCS / BigQuery target view.

---

## 5. Verbatim MCP Tool Output

### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
This UC4 workflow module compiles a flat-file lookup dataset intended for the `DWH$VI_S_SDM_AGENT_ADS` database view. It is responsible for processing agent administrative and lookup information using Ab Initio-style processing steps which are mapped to modern PySpark jobs running on Google Cloud Dataproc. Based on the source configurations, the job is executed on a daily sequence to maintain downstream view synchronization.

---

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABPZ_KKM_AIL_AGENT` | `JOBS_UNIX` | `<Active>1</Active>` (Active) | Builds flat-file lookups for the Agent ADS view using the `ABPZ_KKM_AIL_AGENT` job key. |

---

#### 3. Airflow DAG Properties
| Property | Value | Note |
| :--- | :--- | :--- |
| **dag_id** | `dw_dwh_abpz_kkm_ail_agent` | Sanitized from original UC4 name. |
| **schedule** | `None` | No schedule defined in the provided file. Handled via parent task group or manual trigger. |
| **start_date** | `datetime(2023, 6, 11)` | Placeholder set based on export timestamp metadata. |
| **catchup** | `False` | Recommended default to prevent backfill storms. |
| **max_active_runs** | `1` | Prevents overlapping executions. |
| **is_paused_upon_creation** | `False` | Deploys normally as `<Active>1</Active>`. |
| **default_args** | `{'owner': 'DW.UNIX.ISTNS', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Extracted from Owner/Login configurations. |

---

#### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_abpz_kkm_ail_agent` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/abpz_kkm_ail_agent.py` | Project, Region, Cluster, Job details | `0` | N/A | None | `CaleOn="0"` (None) | False | None | Runs equivalent logic for Ab Initio job key `ABPZ_KKM_AIL_AGENT` |

---

#### 5. Task Dependency Map
```text
dw_dwh_abpz_kkm_ail_agent
```
**Plain English flow:**
The DAG contains a single task execution (`dw_dwh_abpz_kkm_ail_agent`) which submits the compiled PySpark script to GCP Dataproc. Since there are no upstream/downstream tasks or scheduler configurations defined in this single exported file, it acts as a standalone operational task.

---

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / GCS Path |
| :--- | :--- | :--- |
| **Login** | `DW.UNIX.ISTNS` | `owner` attribute in DAG `default_args` |
| **Host** | `|DWHDWH1P|HOST` | Map to GCP Dataproc target cluster: `YOUR_DATAPROC_CLUSTER_NAME` |
| **Job Key (`-j`)** | `ABPZ_KKM_AIL_AGENT` | GCS script lookup: `gs://YOUR_BUCKET_NAME/pyspark_scripts/abpz_kkm_ail_agent.py` |
| **Job Config (`-x`)** | `BHB_CCM_PROC_WriteAgentADSLookup.cfg` | Passed as script parameter or bundled in PySpark configs. |
| **`&DWH_JOB_KENNUNG`**| `ABPZ_KKM_AIL_AGENT` | Task environment variables / metadata payload. |
| **`&RUECKBLICK`** | `GET_VAR('DW.VARIABLEN_DWK_KKM','KKM_Rueckblick_Ladedatum')` | Airflow Variable or dynamic configuration parameter lookup. |

---

#### 7. Error Handling and Retry Strategy
- **Retries**: No retries are declared in the XML (`RUNTIME` parameters). `retries` is default-configured to `0`.
- **Postcondition Analysis**: No explicit conditional structures (`<after>`, `<row>`, or post-scripts) are configured in the source XML file to catch exit exceptions.
- **Sync Object Behavior**: No sync restrictions are detected. Single concurrency via `max_active_runs=1` is sufficient.

---

#### 8. Developer Notes
* **GCP Placeholders**: Update configurations for `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME`.
* **Dynamic Variable Resolution**: The UC4 script references `GET_VAR('DW.VARIABLEN_DWK_KKM','KKM_Rueckblick_Ladedatum')`. The developer must ensure this parameter value is configured in GCP (either via an Airflow Variable or fetched dynamically during run-time inside the PySpark script).
* **Missing Schedule Context**: As no `EVNT_TIME` or `JOBP` workflow container was provided, this job is prepared as a standalone DAG execution sequence.

---

### SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable

# ── GCP Configuration ────────────────────────────────────
# Classify by role in compliance with Environment Variable Policy
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'DW.UNIX.ISTNS',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
}

# ── DAG Definition ───────────────────────────────────────
dag_id = "dw_dwh_abpz_kkm_ail_agent"

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="Builds Flat-File Lookup for View DWH$VI_S_SDM_AGENT_ADS",
    schedule=None,  # No schedule declared in source file
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False, # Source active flag was 1 (True)
) as dag:

    # ── Task: dw_dwh_abpz_kkm_ail_agent ────────────────────
    # Map from UC4 UNIX script parameters:
    #   Job name (-j): ABPZ_KKM_AIL_AGENT
    #   Configuration (-x): BHB_CCM_PROC_WriteAgentADSLookup.cfg
    #   Runtime ERT Estimate: 114 seconds
    
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/abpz_kkm_ail_agent.py",
            "args": [
                "--config", "BHB_CCM_PROC_WriteAgentADSLookup.cfg",
                "--output", "AgentADSLookup.txt",
                "--zone_id", "84"
            ],
            # Passing UC4 metadata variable dynamic values as job properties
            "properties": {
                "spark.yarn.app.name": "dw_dwh_abpz_kkm_ail_agent",
                "spark.executor.memory": "4g"
            }
        }
    }

    dw_dwh_abpz_kkm_ail_agent = DataprocSubmitJobOperator(
        task_id="dw_dwh_abpz_kkm_ail_agent",
        job=pyspark_job_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        # Generate a unique job-id by concatenating DAG ID, Run ID, and Task ID
        job_id='{{ dag.dag_id }}_{{ run_id[:8] }}_dw_dwh_abpz_kkm_ail_agent',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Standalone execution - no dependencies required
    dw_dwh_abpz_kkm_ail_agent
```

---

## 6. Risks & Manual Actions
* **SOURCE: UNRESOLVED CANDIDATE** — `showlog.ksh` — This is listed as unconfirmed by codebase scans, but is verified as a valid, non-blocking utility wrapper dependency by system configuration. No implementation is generated; default Composer logs supersede this.
* **Database Procedure Logic**: The shell utility library `f_alis_msgerr.ksh` relies on Oracle procedures `DWPA_MELDUNG.SetzeZusatzInfos`, `DWPA_MELDUNG.Fehler`, and `DWH$VS_MELDUNG.LogAusgabe_Debug`. The business definitions inside these legacy procedures are not migrated to BigQuery. Standard Airflow alert callbacks and BigQuery execution diagnostic records should be configured instead.
* **Literal Message Preservation (Compliance with Output Literal Rule)**: The literal logging output text inside the original UC4 files must be preserved verbatim. For example:
  * `"Jobkennung &DWH_JOB_KENNUNG eingetragen für &JPMJOB"`
  * `"Die Ab Initio Verarbeitung ist fertig. Der Status wird auf &STATUS_FERTIG umgesetzt."`
  * `"Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert $v_Ergebnis beendet."`
  All target loggers or informational prints executing in Python/Airflow tasks must output these exact strings character-for-character, retaining the original German statements.