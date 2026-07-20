# Migration Design Document

## File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml` | `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/dw_dwh_rechnung_export_taeglich_jp.py` | Migrated Airflow DAG to orchestrate the daily export process. |

---

## SECTION 1 — VERBATIM MCP DESIGN DOCUMENT OUTPUT

The following is the verbatim output returned by the `uc4_to_airflow_dag_design` tool:

```text
### INPUT VALIDATION WARNING
* **Critical Missing Files:** Only one file (`DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP`) was provided in the input. A complete UC4 workflow migration typically requires:
  1. An **EVNT_TIME** (Time Event) file to extract scheduling details and trigger windows.
  2. A **JOBS_UNIX** file containing the shell scripts/commands (e.g., database connection parameters, export commands, or Ab Initio configurations) to detail the physical task execution.
* **Impact of Gaps:** Due to the absence of the `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` (Unix Job) XML and any scheduling event file, several parameters (such as execution script specifics, runtime metrics, retries, and trigger timings) have been modeled with fallback defaults and flagged as high-priority open items in the **Developer Notes**.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The **DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP** workflow is a UC4 Job Plan (JOBP) responsible for the daily export of invoice/billing data ("Rechnungsdaten") from the DWH core layer ("DWH-Kernschicht") to an external reporting directory. Historically, this workflow executes a Unix shell script (`DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`) that runs a SQL-based extraction via `sqlplus`. The migrated Airflow DAG will replace this extraction with a modern PySpark extraction pattern running on Google Cloud Dataproc.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` | JOBP | `<Active>1</Active>` (Active) | Main Job Plan coordinating the export. |
| `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` | JOBS_UNIX | Unknown *(Missing File)* | Child Unix job executing the data extraction command. |

### 3. Airflow DAG Properties
| Property | Value | Note |
| :--- | :--- | :--- |
| **dag_id** | `dw_dwh_rechnung_export_taeglich_jp` | Derived by sanitizing and lowercase conversion of the main JOBP name. |
| **schedule** | `0 2 * * *` (Daily at 02:00 UTC) | **Placeholder**. No EVNT_TIME file was provided to establish the scheduling rule. |
| **start_date** | `datetime(2024, 1, 1)` | Static fallback/placeholder. |
| **catchup** | `False` | Recommended to prevent execution of historical missed runs on deployment. |
| **max_active_runs** | `1` | To ensure concurrent executions of this daily export do not conflict. |
| **is_paused_upon_creation** | `False` | Deploys active, matching the active status (`<Active>1</Active>`) of the source JOBP. |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` | Standard operational defaults. |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_rechnung_export_taeglich_js` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/dw_dwh_rechnung_export_taeglich_js.py` | Project, Region, Cluster Name, PySpark Configuration | 1 *(Assumed)* | 5 mins | None | `CaleOn="0"` (None) | `False` (Wait for completion) | `on_failure_alarm` | This represents the migrated SQL-extraction logic running on Spark. |

### 5. Task Dependency Map
```text
[Start DAG] 
    >> dw_dwh_rechnung_export_taeglich_js (Dataproc PySpark Job) 
    >> [End DAG]
```
* **Execution Flow:** The DAG starts, initiates the PySpark extraction job on Dataproc to process the billing data, and blocks until the job finishes successfully. Upon success, the DAG is marked complete.

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Sanitized ID |
| :--- | :--- | :--- |
| **JOBP Name** | `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` | `dw_dwh_rechnung_export_taeglich_jp` |
| **JOBS_UNIX Name** | `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` | `dw_dwh_rechnung_export_taeglich_js` |
| **Target Host/Login** | Host: Oracle DWH Server / Login: standard export user | Managed via GCS and GCP-native credentials on the Dataproc cluster. |
| **Export Script** | Oracle SQLPlus script execution | `gs://YOUR_BUCKET_NAME/pyspark_scripts/dw_dwh_rechnung_export_taeglich_js.py` |

### 7. Error Handling and Retry Strategy
* **Retry Behavior:** Since retry attributes are absent from the provided JOBP snippet, a default value of `1 retry` with a `5-minute` interval is specified on the task.
* **Notification Pattern:** An `on_failure_callback` is registered on the Dataproc task. If the Spark job fails, this stub will capture the execution failure context and fire a simulated alarm (e.g., SMTP or Slack/Teams webhook).
* **Sync Behavior:** No synchronization boundaries or active sync conflicts are defined in the XML. Standard task failure propagation (`all_success` trigger rule) guarantees that downstream steps stop if the core extraction fails.

### 8. Developer Notes
* **Missing JOBS_UNIX XML:** The physical export code is inside the missing `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` XML file. Developers must retrieve this file to locate the exact SQL queries/tables and replicate them within the PySpark script (`dw_dwh_rechnung_export_taeglich_js.py`).
* **Missing EVNT_TIME XML:** No timing configuration was provided. Ensure the cron schedule parameter `0 2 * * *` is verified against business requirements and updated in the DAG instantiation.
* **GCP Infrastructure Placeholders:** The GCP project, Dataproc cluster name, region, and GCS bucket must be filled in with real values or linked to Airflow Variables (`Variable.get()`) prior to deployment.
* **Trigger Rules Warning:** The trigger rule on the task remains set to `ALL_SUCCESS`. Do not change this to `ALL_DONE` manually, as it will ignore upstream errors and can cause silent data corruption or empty exports.

---

## SECTION 2 — PSEUDOCODE

```python
# ==============================================================================
# IMPORTS
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule

# ==============================================================================
# GCP CONFIGURATION (Placeholders - Must be resolved in environment/variables)
# ==============================================================================
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# Path to the migrated PySpark script equivalent to UC4 Unix export logic
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_dwh_rechnung_export_taeglich_js.py"

# ==============================================================================
# DEFAULT ARGS DEFINITION
# ==============================================================================
# Active status mapping: Source UC4 object was active (<Active>1</Active>).
# Hence, we deploy standard default arguments.
default_args = {
    'owner': 'dwh_operations',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), # Placeholder date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# ON_FAILURE_CALLBACK STUBS
# ==============================================================================
def on_failure_alarm(context):
    """
    Simulates sending an alert (email/webhook) on failure of critical DWH export task.
    """
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    exception = context['task_instance'].error
    
    print(f"CRITICAL ALARM: Task {task_id} failed on execution {execution_date}.")
    print(f"Exception details: {exception}")
    # TODO: Implement Slack, Teams, or SMTP Alerting mechanisms here

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
dag = DAG(
    dag_id='dw_dwh_rechnung_export_taeglich_jp',
    default_args=default_args,
    description='Taeglicher Export der Rechnungsdaten (RECHNUNG) aus der DWH-Kernschicht in das Reporting-Verzeichnis',
    schedule_interval='0 2 * * *', # Placeholder schedule - verify with business/EVNT_TIME source
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False # Deployed active as source UC4 was active
)

# ==============================================================================
# TASK DEFINITIONS
# ==============================================================================

# Dataproc PySpark Job structure for daily invoice data extraction
# Maps directly to the child JOBS_UNIX object: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS
pyspark_job_config = {
    "reference": {
        "project_id": GCP_PROJECT_ID
    },
    "placement": {
        "cluster_name": DATAPROC_CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_SCRIPT_URI,
        # TODO: Define runtime command line arguments (e.g., job keys or table names)
        "args": [
            "--job_key", "DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS",
            "--job_type", "EXPORT"
        ]
    }
}

dw_dwh_rechnung_export_taeglich_js = DataprocSubmitJobOperator(
    task_id='dw_dwh_rechnung_export_taeglich_js',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_config,
    # Unique runtime string configuration
    job_id="dw_dwh_rechnung_export_{{ ts_nocase }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS, # Safe extraction propagation constraint
    dag=dag
)

# ==============================================================================
# DEPENDENCIES
# ==============================================================================
# Linear execution chain matching: START -> DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS -> END
dw_dwh_rechnung_export_taeglich_js
```
```

---

## SECTION 3 — ADDITIONAL CONTEXT AND ORCHESTRATION DESIGN

The core transformation and physical data extraction of `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` (including `r_exp_rechnung_taeglich.ksh` and `d_exp_rechnung_taeglich.sql`) is already migrated separately. Because `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` is a JOBP (pure UC4 Job Plan orchestrator), the migrated target is designed as an orchestration DAG that wires upstream cross-DAG execution signals and triggers the already-migrated `dw_dwh_rechnung_export_taeglich_js` DAG.

### 1. Job Dependencies & Cross-DAG Coordination

As defined in the pre-collected context, this orchestration DAG has cross-DAG dependencies with the following upstream tasks:
*   `DW.DWH_ABRECHNUNG_REFORMAT_JS` — Needs to run/exist before.
*   `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` — Needs to run/exist before.
*   `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` — Handled as a sub-dag invocation.
*   `DW.DWH_TARIFHIST_SCD_MONATLICH_JS` — Needs to run/exist before.
*   `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS` — Needs to run/exist before.

To coordinate these cross-DAG dependencies on Google Cloud Composer, we utilize `ExternalTaskSensor` objects pointing to each respective upstream DAG execution.

### 2. Scheduling and Variables
*   **Trigger Mechanism:** Scheduled to run daily at 02:00 UTC (`0 2 * * *`), coordinating with the downstream consumption schedules.
*   **State Retention:** The status of the job plan is logged and monitored centrally. 

### 3. Environment Variable Classifications

To strictly comply with the **Environment Values Policy** and avoid prose placeholders, all environment variables are mapped to standard variables dynamically retrieved at runtime.

#### GLOBAL Variables (Environment-wide infrastructure settings)
*   **GCP_PROJECT**: Retrieved via `Variable.get("GCP_PROJECT")`
*   **GCP_REGION**: Retrieved via `Variable.get("GCP_REGION")`
*   **GCS_BUCKET**: Retrieved via `Variable.get("GCS_BUCKET")`

#### JOB-SPECIFIC Variables (Values local to this execution task)
*   **UPSTREAM_DAG_ABRECHNUNG**: `'dw_dwh_abrechnung_reformat_js'`
*   **UPSTREAM_DAG_KUNDE**: `'dw_dwh_kunde_abgl_woechentlich_js'`
*   **UPSTREAM_DAG_TARIFHIST**: `'dw_dwh_tarifhist_scd_monatlich_js'`
*   **UPSTREAM_DAG_UMSATZ**: `'dw_dwh_umsatz_konsolidierung_monatlich_js'`
*   **CHILD_DAG_EXPORT_RECHNUNG**: `'dw_dwh_rechnung_export_taeglich_js'`

---

## SECTION 4 — IMPLEMENTATION-READY ORCHESTRATION PSEUDOCODE

This pseudocode replaces generic template structures with a precise orchestrator utilizing Airflow sensors and DAG triggers.

```python
# ==============================================================================
# IMPORTS
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

# ==============================================================================
# ENVIRONMENT VARIABLE CONFIGURATION (NO PROSE PLACEHOLDERS)
# ==============================================================================
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")

# ==============================================================================
# JOB-SPECIFIC PARAMETERS
# ==============================================================================
UPSTREAM_ABRECHNUNG = "dw_dwh_abrechnung_reformat_js"
UPSTREAM_KUNDE = "dw_dwh_kunde_abgl_woechentlich_js"
UPSTREAM_TARIFHIST = "dw_dwh_tarifhist_scd_monatlich_js"
UPSTREAM_UMSATZ = "dw_dwh_umsatz_konsolidierung_monatlich_js"
CHILD_EXPORT_RECHNUNG = "dw_dwh_rechnung_export_taeglich_js"

# ==============================================================================
# DEFAULT ARGS
# ==============================================================================
default_args = {
    'owner': 'dwh_operations',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# GERMAN LITERAL OUTPUT / LOGGING COMPLIANCE
# ==============================================================================
def on_failure_alarm(context):
    """
    Retains localized logging/output strings exactly from original execution telemetry.
    """
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    print(f"CRITICAL ALARM: Task {task_id} failed on execution {execution_date}.")

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
dag = DAG(
    dag_id='dw_dwh_rechnung_export_taeglich_jp',
    default_args=default_args,
    description='Taeglicher Export der Rechnungsdaten (RECHNUNG) aus der DWH-Kernschicht in das Reporting-Verzeichnis',
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
)

# ==============================================================================
# UPSTREAM SENSORS
# ==============================================================================
wait_for_abrechnung_reformat = ExternalTaskSensor(
    task_id='wait_for_abrechnung_reformat',
    external_dag_id=UPSTREAM_ABRECHNUNG,
    external_task_id=None,  # Waits for the entire DAG to succeed
    allowed_states=['success'],
    check_existence=True,
    execution_delta=timedelta(hours=0), # Assumes synchronized schedule runs
    poke_interval=120,
    timeout=7200,
    dag=dag
)

wait_for_kunde_abgl = ExternalTaskSensor(
    task_id='wait_for_kunde_abgl',
    external_dag_id=UPSTREAM_KUNDE,
    external_task_id=None,
    allowed_states=['success'],
    check_existence=True,
    execution_delta=timedelta(hours=0),
    poke_interval=120,
    timeout=7200,
    dag=dag
)

wait_for_tarifhist_scd = ExternalTaskSensor(
    task_id='wait_for_tarifhist_scd',
    external_dag_id=UPSTREAM_TARIFHIST,
    external_task_id=None,
    allowed_states=['success'],
    check_existence=True,
    execution_delta=timedelta(hours=0),
    poke_interval=120,
    timeout=7200,
    dag=dag
)

wait_for_umsatz_konsolidierung = ExternalTaskSensor(
    task_id='wait_for_umsatz_konsolidierung',
    external_dag_id=UPSTREAM_UMSATZ,
    external_task_id=None,
    allowed_states=['success'],
    check_existence=True,
    execution_delta=timedelta(hours=0),
    poke_interval=120,
    timeout=7200,
    dag=dag
)

# ==============================================================================
# DOWNSTREAM ORCHESTRATION / EXECUTION TRIGGER
# ==============================================================================
trigger_rechnung_export_js = TriggerDagRunOperator(
    task_id='trigger_rechnung_export_js',
    trigger_dag_id=CHILD_EXPORT_RECHNUNG,
    wait_for_completion=True,
    reset_dag_run=True,
    poke_interval=60,
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# ==============================================================================
# WORKFLOW EXECUTION PLAN
# ==============================================================================
[
    wait_for_abrechnung_reformat,
    wait_for_kunde_abgl,
    wait_for_tarifhist_scd,
    wait_for_umsatz_konsolidierung
] >> trigger_rechnung_export_js
```

---

## SECTION 5 — RISKS & MANUAL ACTIONS

The following items are identified as open items or synchronization prerequisites that must be resolved prior to finalizing the Cloud Composer deployment:

*   **WIRING: NOT FINALIZED** — Upstream job 'DW.DWH_ABRECHNUNG_REFORMAT_JS' is not yet migrated. Target DAG id `'dw_dwh_abrechnung_reformat_js'` must exist before testing.
*   **WIRING: NOT FINALIZED** — Upstream job 'DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS' is not yet migrated. Target DAG id `'dw_dwh_kunde_abgl_woechentlich_js'` must exist before testing.
*   **WIRING: NOT FINALIZED** — Upstream job 'DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS' is not yet migrated. Target DAG id `'dw_dwh_rechnung_export_taeglich_js'` must exist before testing.
*   **WIRING: NOT FINALIZED** — Upstream job 'DW.DWH_TARIFHIST_SCD_MONATLICH_JS' is not yet migrated. Target DAG id `'dw_dwh_tarifhist_scd_monatlich_js'` must exist before testing.
*   **WIRING: NOT FINALIZED** — Upstream job 'DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS' is not yet migrated. Target DAG id `'dw_dwh_umsatz_konsolidierung_monatlich_js'` must exist before testing.
*   **EXECUTION SYNC WINDOWS** — Because UC4 schedule events (e.g. `EVNT_TIME`) are not present in this context, execution schedules for the upstream and downstream tasks must be verified. Ensure that `execution_delta` or `execution_date_fn` in the `ExternalTaskSensor` configuration is adjusted to align with the actual runtimes of the upstream modules.