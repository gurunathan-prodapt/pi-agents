An elegant, implementation-ready migration design document has been created for the shared include scripts. Since these files are reusable UC4 Include Scripts (`JOBI` objects) that provide centralized environment setup and logging mechanisms, they do not run as standalone DAGs. Instead, they translate into a shared, importable Python helper module (`dwh_uc4_helpers.py`) in your target Cloud Composer/Airflow environment.

The migration design document below includes the verbatim reverse-engineered logic from the UC4 execution models, supplemented with target environment details, dependency wiring, and lineage endpoints.

---

# MIGRATION DESIGN DOCUMENT: Shared Files — isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/ALLGEMEIN/INCLUDES

## SECTION 1 — VERBATIM MCP CONVERSIONS

### 1.1 Convert: `DW.HOLE_PFAD.xml` (Environment and Date Variable Setup)

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

── GCP Configuration ────────────────────────────────────
# Shared GCP Configurations (to be passed to downstream Dataproc tasks)
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

── Helper / Logic Functions ──────────────────────────────
def calculate_dwh_variables(**context):
    """
    Translates the variable setting logic from UC4's DW.HOLE_PFAD into Airflow/Python.
    Pushes results into XCom so downstream tasks can consume these paths and dates.
    """
    # 1. Fetch static environment variables (equivalent to GET_VAR calls)
    variables = {
        "DWH_HOME": Variable.get("dwh_home", default_var="/opt/dwh"),
        "HOME": Variable.get("home", default_var="/home/airflow"),
        "KWS_HOME": Variable.get("kws_home", default_var="/opt/kws"),
        "PMS_HOME": Variable.get("pms_home", default_var="/opt/pms"),
        "ISTNS_HOME": Variable.get("istns_home", default_var="/opt/istns"),
        "AKTIV_CARMEN": Variable.get("aktiv_carmen", default_var="1"),
        "AKTIV_CRS": Variable.get("aktiv_crs", default_var="1"),
        "AKTIV_CTEL": Variable.get("aktiv_ctel", default_var="1"),
        "AKTIV_DPPS": Variable.get("aktiv_dpps", default_var="1"),
        "AKTIV_KDS": Variable.get("aktiv_kds", default_var="1"),
        "AKTIV_WUERFEL": Variable.get("aktiv_wuerfel", default_var="1"),
        "AKTIV_XTRA": Variable.get("aktiv_xtra", default_var="1"),
        "AKTUELL_CACHE": Variable.get("aktuell_cache", default_var="1")
    }

    # 2. Replicate UC4 Date Arithmetic logic
    # UC4 equivalent: :set &LASTMONTH_YYYYMM = SYS_DATE(YYYYMMDD) -> parsed to YYYYMM + '01'
    logical_date = context['logical_date'] # Airflow execution/logical date
    first_of_current_month = logical_date.replace(day=1)

    # UC4 equivalent: :set &PRELASTMONTH_YYYYMM = SUB_PERIOD(..., "MM:2")
    prelast_month_dt = first_of_current_month - relativedelta(months=2)
    variables["PRELASTMONTH_YYYYMM"] = prelast_month_dt.strftime("%Y%m")

    # UC4 equivalent: :set &LASTMONTH_YYYYMM = SUB_DAYS(&LASTMONTH_YYYYMM,1) -> then SUBSTR to 6 chars
    # Subtracting 1 day from the 1st of the current month yields the last day of the previous month
    last_month_dt = first_of_current_month - relativedelta(days=1)
    variables["LASTMONTH_YYYYMM"] = last_month_dt.strftime("%Y%m")

    # UC4 equivalent: :set &NEXTMONTH_YYYYMM = ADD_PERIOD(..., "MM:1")
    next_month_dt = logical_date + relativedelta(months=1)
    variables["NEXTMONTH_YYYYMM"] = next_month_dt.strftime("%Y%m")

    # Push variables dictionary to XCom for downstream steps
    for key, val in variables.items():
        context['ti'].xcom_push(key=key, value=val)
        print(f"Set variable {key} = {val}")

── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_hole_pfad_setup',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md="""
    ### DAG Blueprint for DW.HOLE_PFAD JOBI
    This DAG calculates environment variables and date ranges equivalent to the 
    UC4 include script and exposes them via XCom for pipeline tasks.
    """
) as dag:

── Task: initialize_variables ────────────────────────────
    initialize_variables = PythonOperator(
        task_id='initialize_variables',
        python_callable=calculate_dwh_variables,
        provide_context=True,
    )

── Dependencies ─────────────────────────────────────────
    # Since this is a utility setup script, it is the entry point
    initialize_variables
```

---

### 1.2 Convert: `DW.LESE_LOG.xml` (Execution Status and Job Logging Evaluation)

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
# Hooks or clients to replace the DB logging update logic from DW.DWH_ADM_JOB_MONITOR_END
# from airflow.providers.postgres.hooks.postgres import PostgresHook 

── GCP Configuration ────────────────────────────────────
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
REGION = "YOUR_DATAPROC_REGION"
CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
BUCKET_NAME = "YOUR_BUCKET_NAME"

── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 1)
}

── on_failure_callback stubs ─────────────────────────────
def dwh_on_failure_callback(context):
    """
    Translates the error-trapping logic of DW.LESE_LOG.
    """
    # Extract contextual metadata representing &DWH_JOB_KENNUNG
    ti = context.get("task_instance")
    task_id = ti.task_id
    run_id = context.get("run_id")
    job_kennung = f"{task_id}_{run_id}"
    
    # 1. Simulate the stdout output markers from DW.LESE_LOG
    print("****************************************************************")
    print(f"ERROR DETECTED FOR JOB KENNUNG: {job_kennung}")
    print("Executing fallback diagnostic logs...")
    print("****************************************************************")
    
    # 2. TODO: Replace legacy '$HOME/tools/showlog -uc4' with a Cloud Logging call or Airflow log URL
    log_url = ti.log_url
    print(f"Task failure log location: {log_url}")
    
    # 3. Handle the logic implied by ':INC DW.DWH_ADM_JOB_MONITOR_END' on failure
    print("Registering task failure in DWH Administrative Monitoring database...")
    # Example Database Hook call:
    # pg_hook = PostgresHook(postgres_conn_id="dwh_adm_db")
    # pg_hook.run("UPDATE dwh_job_monitor SET status = 'FAILED' WHERE job_id = %s", parameters=(job_kennung,))


── on_success_callback stubs ─────────────────────────────
def dwh_on_success_callback(context):
    """
    Translates the successful exit path (exit 0) and administrative monitoring termination.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id
    run_id = context.get("run_id")
    job_kennung = f"{task_id}_{run_id}"
    
    print("****************************************************************")
    print(f"JOB COMPLETED SUCCESSFULLY: {job_kennung}")
    print("****************************************************************")
    
    # Handle logic implied by ':INC DW.DWH_ADM_JOB_MONITOR_END' on success
    print("Registering task success in DWH Administrative Monitoring database...")
    # Example Database Hook call:
    # pg_hook = PostgresHook(postgres_conn_id="dwh_adm_db")
    # pg_hook.run("UPDATE dwh_job_monitor SET status = 'SUCCESS' WHERE job_id = %s", parameters=(job_kennung,))


── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="parent_dwh_workflow_placeholder",
    schedule_interval=None, # To be filled based on parent workflow schedule
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS
) as dag:

── Task: dwh_step_placeholder ────────────────────────────
    # Example PySpark Job Configuration utilizing the callbacks derived from DW.LESE_LOG
    pyspark_job_config = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET_NAME}/pyspark_scripts/placeholder.py"
        }
    }
    
    run_pyspark_task = DataprocSubmitJobOperator(
        task_id="dwh_step_placeholder",
        job=pyspark_job_config,
        region=REGION,
        project_id=PROJECT_ID,
        # Bind the error handling/termination scripts
        on_failure_callback=dwh_on_failure_callback,
        on_success_callback=dwh_on_success_callback
    )

── Dependencies ─────────────────────────────────────────
    # Sequence mapping
    run_pyspark_task
```

---

## SECTION 2 — CONTEXTUAL TARGET INTEGRATION

This section supplements the automatic translations by detailing cross-file, runtime environment, and dependency context that the isolated parser could not analyze.

### 2.1 Job Dependencies & Lineage Wiring
These include scripts are called by multiple execution units in the production system. In Airflow, they should not be managed as individual DAGs; they should instead be written to a shared utilities directory (e.g. `plugins/dwh_uc4_helpers.py` or `dags/utils/dwh_uc4_helpers.py`) and imported by parent workflows.

*   **Downstream Consumers (Cross-Job Hand-offs):**
    *   `DW.DWH_EXIS_IKDB_STAMM_R` — **not yet migrated**
    *   `DW.DWH_IPGD_APN_TYP` — **not yet migrated**
    *   `DW.DWH_IPGD_QUELLREC` — **not yet migrated**
    *   *Target Wiring Plan:* Once these three downstreams are migrated, they will import the variables setup and callback hooks from the shared module. Since these upstream components are not yet migrated, placeholder imports or empty utility calls must be retained in the downstream tasks until their structural migrations are complete.

### 2.2 Execution Order & Schedule Retention
*   **Scheduling:** This assembly is a shared-file library (`shared_files` type). It does **not** have its own cron triggers or run schedules. It executes dynamically inside downstream jobs.
*   **Execution Sequence:**
    1.  At the start of any executing DWH job chain, the translated logic of `DW.HOLE_PFAD` runs first to calculate date thresholds and paths.
    2.  The job executes its main payload (e.g. BigQuery SQL or Dataproc PySpark scripts).
    3.  During and at the end of the step's execution, the translated logic of `DW.LESE_LOG` traps either success or failure signals to run job logging.

### 2.3 Environment-Specific Values (Configuration Policy)
To prevent hardcoded placeholders, legacy environment configurations must be categorized and retrieved dynamically at runtime based on their role:

1.  **GLOBAL Constants (Environment Infrastructure):**
    These variables are identical across all running DAGs in a specific deployment target environment (Dev/Test/Prod). They must be sourced from Airflow Variables or Environment variables dynamically:
    *   `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")` or standard environment lookup.
    *   `GCP_REGION`: Sourced via `Variable.get("GCP_REGION")`.
    *   `GCS_BUCKET`: The GCS bucket containing the PySpark packages, sourced via `Variable.get("GCS_BUCKET")`.
    *   `DATAPROC_CLUSTER`: The processing cluster name, sourced via `Variable.get("DATAPROC_CLUSTER")`.

2.  **JOB-SPECIFIC Parameters (Variable Containers):**
    The legacy values stored within `DW.VARIABLEN` are job-specific path and toggle configurations. They must map as follows:
    *   **Variables Container:** `DW.VARIABLEN` maps to individual keys in the Airflow Metadata store (or an environment config JSON stored inside a single Airflow variable like `dwh_variables_config`):
        *   `DWH_HOME` $\rightarrow$ `Variable.get("dwh_home", default_var="/opt/dwh")`
        *   `HOME` $\rightarrow$ `Variable.get("home", default_var="/home/airflow")`
        *   `KWS_HOME` $\rightarrow$ `Variable.get("kws_home", default_var="/opt/kws")`
        *   `PMS_HOME` $\rightarrow$ `Variable.get("pms_home", default_var="/opt/pms")`
        *   `ISTNS_HOME` $\rightarrow$ `Variable.get("istns_home", default_var="/opt/istns")`
        *   `AKTIV_CARMEN` $\rightarrow$ `Variable.get("aktiv_carmen", default_var="1")`
        *   `AKTIV_CRS` $\rightarrow$ `Variable.get("aktiv_crs", default_var="1")`
        *   `AKTIV_CTEL` $\rightarrow$ `Variable.get("aktiv_ctel", default_var="1")`
        *   `AKTIV_DPPS` $\rightarrow$ `Variable.get("aktiv_dpps", default_var="1")`
        *   `AKTIV_KDS` $\rightarrow$ `Variable.get("aktiv_kds", default_var="1")`
        *   `AKTIV_WUERFEL` $\rightarrow$ `Variable.get("aktiv_wuerfel", default_var="1")`
        *   `AKTIV_XTRA` $\rightarrow$ `Variable.get("aktiv_xtra", default_var="1")`
        *   `AKTUELL_CACHE` (from `DW.VARIABLEN_DWK_KKM`) $\rightarrow$ `Variable.get("aktuell_cache", default_var="1")`

### 2.4 External System Replacements & Cross-File Dependencies
*   `DW.DWH_ADM_JOB_MONITOR_START` & `DW.DWH_ADM_JOB_MONITOR_END` (Metadata Tracking): These sub-includes are referenced at the boundary of `DW.HOLE_PFAD` and `DW.LESE_LOG`. Since their code is not in this assembly, they are treated as unresolved dependencies (see Risks below). In the target architecture, they must map to database hooks updating a monitoring schema in BigQuery, or standard GCP Cloud Logging sinks.

---

## SECTION 3 — RISKS & MANUAL ACTIONS

*   **SOURCE: NOT FOUND** — `DW.DWH_ADM_JOB_MONITOR_START` — *no candidate*
    *   *Manual Action:* Write or map this start log mechanism inside Airflow's initialization task using a shared metadata tracking utility database call.
*   **SOURCE: NOT FOUND** — `DW.DWH_ADM_JOB_MONITOR_END` — *no candidate*
    *   *Manual Action:* Replicate the cleanup/termination steps using standard Airflow `on_success_callback` and `on_failure_callback` hooks.
*   **SOURCE: NOT FOUND** — `SHOWLOG.KSH` — *no candidate*
    *   *Manual Action:* Since this is a custom logging visualizer script (`$HOME/tools/showlog -uc4`), it must be retired. The `on_failure_callback` must print the Task Instance log URL (`ti.log_url`) to standard output so that operators can access real-time task logs directly via the Airflow/Cloud Composer UI.
*   **Downstream Pipeline Wiring Hand-off:**
    *   *Risk:* The three downstream workflows (`DW.DWH_EXIS_IKDB_STAMM_R`, `DW.DWH_IPGD_APN_TYP`, `DW.DWH_IPGD_QUELLREC`) are not yet migrated.
    *   *Manual Action:* Ensure that when these three target DAGs are generated, they import `dwh_uc4_helpers` and read date parameters from the Airflow execution environment's logical context instead of trying to pass environment parameters via hardcoded execution directories.