## MIGRATION DESIGN DOCUMENT
### JOB: Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes
### TARGET PLATFORM: BigQuery & Cloud Composer (Apache Airflow)

---

### FILE DISPOSITION TABLE

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml` | `dags/includes/dw_hole_pfad_vtrg.py` | Shared Airflow Python variable loader module. Reads global configuration parameters. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml` | `dags/includes/dw_lese_log_vtrg.py` | Shared Airflow Python logging utility to log task execution context. |

---

### JOB DEPENDENCIES, SCHEDULING & EXECUTION ORDER

* **Job Dependencies & Lineage:**
  * **Upstream:** None
  * **Downstream Consumers:** 
    * `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` — not yet migrated
    * `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` — not yet migrated
  * *Wiring on Target Platform:* These downstreams are currently unmigrated. Once migrated, they will import the Python utility files from the `includes/` folder (either via relative Python import paths or dynamic Airflow package distribution).
* **Scheduling:** These includes do not run as standalone DAGs; they are helper modules invoked inside other DAG pipelines on-demand.
* **Execution Order:** Not applicable as standalone scripts. They are executed at the start (or during execution) of downstream tasks within their calling workflows.

---

### ENVIRONMENT & VARIABLE CLASSIFICATION POLICY

#### GLOBAL (Environment-Wide Configurations)
The variables loaded by `DW.HOLE_PFAD_VTRG` describe global installation paths. In Cloud Composer, they are mapped to **Airflow Variables** stored in JSON format under the key `dw_variablen`:

* `DWH_HOME` $\rightarrow$ Map to JSON key `"dwh_home"` (e.g. `Variable.get("dw_variablen", deserialize_json=True).get("dwh_home")`)
* `HOME` $\rightarrow$ Map to JSON key `"home"` (e.g. `Variable.get("dw_variablen", deserialize_json=True).get("home")`)
* `PMS_HOME` $\rightarrow$ Map to JSON key `"pms_home"` (e.g. `Variable.get("dw_variablen", deserialize_json=True).get("pms_home")`)

#### JOB-SPECIFIC
* `&ADMJP` $\rightarrow$ Dynamic job context parameter resolved natively via Airflow Jinja macro `{{ dag.dag_id }}`.
* `&ADMJOB` $\rightarrow$ Dynamic task context parameter resolved natively via Airflow Jinja macro `{{ task.task_id }}`.

---

### RISKS & MANUAL ACTIONS

* **SOURCE: UNRESOLVED DOWNSTREAM** — `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` — no candidate file found.
* **SOURCE: UNRESOLVED DOWNSTREAM** — `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` — no candidate file found.
* **Manual Step:** Ensure the Airflow Variable `dw_variablen` is created in your target Cloud Composer environment prior to deploying any calling DAGs. It must contain the JSON keys `dwh_home`, `home`, and `pms_home` mapped to real Google Cloud Storage URI paths or directory values.

---

### VERBATIM MCP TOOL OUTPUTS & TARGET FILE PLAN

#### TARGET FILE: `dags/includes/dw_hole_pfad_vtrg.py`
*(Derived from `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml`)*

```python
# ── Imports ──────────────────────────────────────────────
from airflow.models import Variable
from airflow.exceptions import AirflowException

# ── GCP / Airflow Environment Configuration ────────────────────
# The UC4 Variable Container "DW.VARIABLEN" should be stored in Airflow 
# as a JSON Variable under the key 'dw_variablen'.
# Example JSON value: 
# {
#   "dwh_home": "gs://YOUR_BUCKET_NAME/dwh",
#   "home": "/home/airflow",
#   "pms_home": "gs://YOUR_BUCKET_NAME/pms"
# }

# ── Shared Configuration Function (Helper) ───────────────
def load_dw_variables():
    """
    Simulates UC4's JOBI 'DW.HOLE_PFAD_VTRG' by fetching path constants
    from Airflow's metastore and returning them as a dictionary.
    """
    try:
        # Retrieve the variable container
        dw_vars = Variable.get("dw_variablen", deserialize_json=True)
        
        dwh_home = dw_vars.get("dwh_home")
        home = dw_vars.get("home")
        pms_home = dw_vars.get("pms_home")
        
        if not all([dwh_home, home, pms_home]):
            raise AirflowException("One or more key paths (dwh_home, home, pms_home) are missing inside Variable 'dw_variablen'.")
            
        return {
            "DWH_HOME": dwh_home,
            "HOME": home,
            "PMS_HOME": pms_home
        }
    except Exception as e:
        raise AirflowException(f"Failed to load environment path configuration from Airflow Variables: {str(e)}")

# ── Usage in Downstream Tasks (Example Context Only) ────
# When constructing Dataproc/PySpark tasks in the main workflow:
# 
#  env_paths = load_dw_variables()
#  pyspark_job_properties = {
#      "spark.executorEnv.DWH_HOME": env_paths["DWH_HOME"],
#      "spark.executorEnv.PMS_HOME": env_paths["PMS_HOME"]
#  }
```

---

#### TARGET FILE: `dags/includes/dw_lese_log_vtrg.py`
*(Derived from `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml`)*

```python
── Imports ──────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

── GCP Configuration ────────────────────────────────────
# No GCP resources required for this pure logging helper.

── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

── on_failure_callback stubs ─────────────────────────────
# No failure callbacks needed for this helper.

── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_lese_log_vtrg_helper',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
)

── Helper Python Function ────────────────────────────────
def log_vtrg_context_executable(**context):
    """
    Implements the migrated logic of DW.LESE_LOG_VTRG.
    Prints parent workflow (DAG ID) and current execution task name (Task ID).
    """
    # Extracting current DAG ID (equivalent to UC4 &ADMJP via SYS_ACT_JPNAME)
    parent_dag_name = context['dag'].dag_id
    
    # Extracting current Task ID (equivalent to UC4 &ADMJOB via SYS_ACT_JOBNAME)
    current_task_name = context['task'].task_id
    
    # Render and print the log entry
    # Output language rules: Printed string format is retained verbatim in German:
    log_message = f"Protokolleintrag: {current_task_name} innerhalb {parent_dag_name}"
    logging.info(log_message)

── Task: log_vtrg_context ───────────────────────────────
log_vtrg_context = PythonOperator(
    task_id='log_vtrg_context',
    python_callable=log_vtrg_context_executable,
    provide_context=True,
    dag=dag,
)

── Dependencies ─────────────────────────────────────────
# Standalone task execution mapping
log_vtrg_context
```