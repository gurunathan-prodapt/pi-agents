Below is the complete, implementation-ready migration design document for the assembled job `DW.DWH_EXIS_IKDB_STAMM_R`.

---

# MIGRATION DESIGN DOCUMENT: DW.DWH_EXIS_IKDB_STAMM_R

## 1. EXECUTIVE SUMMARY & TARGET ARCHITECTURE

### Legacy Environment Overview
The legacy job `DW.DWH_EXIS_IKDB_STAMM_R` is an orchestration wrapper pattern that exports contract master data from the interactive database system (`IKDB`). It is comprised of two core components:
1. **A UC4 Job Descriptor XML (`DW.DWH_EXIS_IKDB_STAMM_R.xml`)** which manages environmental setups and triggers executing steps.
2. **A KornShell Wrapper Script (`r_exp_ikdb.ksh`)** which handles parameter parsing, backfilling logic ("Nachlieferung" mode), state verification against an Oracle database audit log table (`DWTK_MELDUNGEN`), and conditional execution of underlying SQL scripts.

### Target Architecture (BigQuery + Cloud Composer/Airflow + Dataproc Spark)
The legacy system is migrated to an enterprise-grade cloud ecosystem on Google Cloud Platform (GCP):
*   **Orchestration:** Managed by **Apache Airflow (Cloud Composer)**.
*   **Computation (Extraction & Transformation):** Managed by **GCP Dataproc Spark (PySpark)**, which translates legacy SQL queries and handles exporting to **Google Cloud Storage (GCS)** as CSV/Parquet formats.
*   **Metadata Audit Tracking:** The tracking table `DWTK_MELDUNGEN` maps to a metadata store in BigQuery (or a Cloud SQL database referenced via a Postgres/Oracle Airflow Connection) where execution status is kept.

---

## 2. VERBATIM MCP DESIGN OUTPUTS

The core transformation designs, task trees, and pseudo-code blocks returned by the migration engine are included below verbatim.

### 2.1. UC4 XML TO AIRFLOW DAG DESIGN (VERBATIM RESULT)
```
=== Result for local/home/gurunathan_t/test_dataset/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_IKDB/DW.DWH_IKDB_EXPORT_STAMM_TAEGLICH_JP/DW.DWH_EXIS_IKDB_STAMM_R.xml ===
Here is the comprehensive Design Document and Pseudocode blueprint for converting the analyzed UC4 XML file into an Apache Airflow DAG.

---

### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
This UC4 workflow is designed to execute a daily export of contract master data from an interactive database system (`IKDB`). Specifically, it triggers an export script (`r_exp_ikdb.ksh`) using the query template `d_ikdb_exp_stamm.sql` for the job run context of `EXIS_IKDB_STAMM_R`. The resulting export outputs to the target file metadata signature `STAMM_OUT_TMD`. This run is critical for daily downstream staging processes and, based on source documentation, is restricted to executing exactly once per reporting date.

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_EXIS_IKDB_STAMM_R` | `JOBS_UNIX` | `<Active>1</Active>` (Active) | Daily UNIX shell job executing the master data extraction script. |

#### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_exis_ikdb_stamm_r` |
| **schedule** | `None` *(See Developer Notes: Driven externally or via missing EVNT/JSCH scheduler configuration)* |
| **start_date** | `datetime(2026, 4, 21)` *(Based on XML metadata export date)* |
| **catchup** | `False` |
| **max_active_runs** | `1` *(Enforces the UC4 sync behavior restricting parallel executions)* |
| **is_paused_upon_creation** | `False` *(Source object was marked active)* |
| **default_args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

#### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_exis_ikdb_stamm_r_task` | `DataprocSubmitJobOperator` | `exis_ikdb_stamm_r.py` | `project_id`, `region`, `cluster_name` | 0 | N/A | None | None | `wait_for_completion=True` | None | Maps directly to the active execution command found in the UNIX job script body. |

#### 5. Task Dependency Map
Since only one `JOBS_UNIX` object was supplied in the XML and no high-level parent `JOBP` (Job Plan) or `JSCH` (Schedule) was provided, this DAG resolves into a single-task process:

```
start >> dw_dwh_exis_ikdb_stamm_r_task >> end
```

*Note: If this job is later integrated into a larger parent pipeline, it will be orchestrated via a `TriggerDagRunOperator` pointing to this DAG.*

#### 6. Parameter and Variable Mapping
| UC4 Parameter / Command Argument | Value / Source | Airflow / GCS Equivalent |
|---|---|---|
| **UC4 Object Name** | `DW.DWH_EXIS_IKDB_STAMM_R` | **Sanitised DAG ID:** `dw_dwh_exis_ikdb_stamm_r` |
| `-q d_ikdb_exp_stamm.sql` | Input SQL query parameters | Handled internally by the migration of the query to Spark SQL logic. |
| `-j EXIS_IKDB_STAMM_R` | Job identity key | Translated to the PySpark entrypoint: `gs://YOUR_BUCKET_NAME/pyspark_scripts/exis_ikdb_stamm_r.py` |
| `-f STAMM_OUT_TMD` | Target dataset/file identity | Output path destination within GCS target buckets. |
| `-n 7` | Command-line parameter | Passed as a dynamic argument to the PySpark script. |

#### 7. Error Handling and Retry Strategy
- **Retry Configuration:** No retry attempts (`<MaxRetCode>0</MaxRetCode>` and no custom postcondition re-execution rules) are defined in the XML. Thus, `retries` defaults to `0`.
- **Sync Object Analysis:** The XML defines an active sync reference: 
  ```xml
  <row Abend="SETZE_FREI" Else="Wait" End="SETZE_FREI" Name="DW.DWH_JOB_EXIS_IKDB_STAMM_SYNC" Start="SETZE_LAEUFT"/>
  ```
  Since `Else="Wait"` is designated, setting `max_active_runs=1` at the DAG level fully satisfies this constraint, ensuring that successive runs queue up safely instead of executing concurrently.

#### 8. Developer Notes
* **Missing Triggering Schedule:** No `EVNT_TIME` or `JSCH` parent files were included in the export workspace. The schedule parameter is set to `None`. The developer must verify if this DAG should be triggered externally (e.g., via Airflow API or dataset-based triggering) or if a target cron schedule needs to be manually defined.
* **GCP Infrastructure Placeholders:** The project ID, regional endpoint, target dataproc cluster name, and PySpark storage bucket locations are specified as standard environment variables and must be configured prior to deployment.
* **Wiederanlauf (Restartability):** According to the internal documentation (`DOCU` block), the job is safe to restart directly on failure without requiring manual rollback operations, provided it does not run more than once per reporting business date.

---

### SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
# Retries are disabled (retries=0) in alignment with UC4 source parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 21),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
# max_active_runs=1 enforces the Else="Wait" Sync configuration
with DAG(
    dag_id='dw_dwh_exis_ikdb_stamm_r',
    default_args=default_args,
    schedule=None,  # No schedule block (EVNT_TIME/JSCH) was provided in the XML input
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # <Active>1</Active> mapped from source XHEADER
    tags=['uc4_migration', 'dwh', 'exis', 'ikdb'],
) as dag:

    # ── Entry and Exit Points ─────────────────────────────
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # ── Task: dw_dwh_exis_ikdb_stamm_r_task ────────────────
    # Map UNIX: $HOME/aktuell/exporter/ikdb/bin/r_exp_ikdb.ksh -q d_ikdb_exp_stamm.sql -j EXIS_IKDB_STAMM_R -f STAMM_OUT_TMD -n 7
    # Estimated Runtime (ERT): 2305 seconds (approx 38 minutes)
    
    pyspark_job_args = [
        "--query", "d_ikdb_exp_stamm.sql",
        "--job_key", "EXIS_IKDB_STAMM_R",
        "--file_type", "STAMM_OUT_TMD",
        "--numeric_param", "7"
    ]

    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/exis_ikdb_stamm_r.py",
            "args": pyspark_job_args,
        },
    }

    # Dynamic job ID generated using dag_id, execution date, and task suffix
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='dw_dwh_exis_ikdb_stamm_r_task',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_config,
        job_id="dw_dwh_exis_ikdb_stamm_r_{{ ds_nodash }}_{{ task_instance.try_number }}",
    )

    # ── Dependencies ─────────────────────────────────────────
    start >> submit_pyspark_job >> end
```
```

### 2.2. KORNSHELL WRAPPER TO PYSPARK/DATAPROC DESIGN (VERBATIM RESULT)
```
=== Result for local/home/gurunathan_t/test_dataset/isdwh/exporter/ikdb/bin/r_exp_ikdb.ksh ===
Here is the comprehensive migration design and DAG blueprint developed from the provided UC4 UNIX script.

---

### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
The analyzed script (`#!/bin/ksh_dwh`) serves as a legacy Unix-based wrapper mechanism for execution orchestration. Specifically, it prepares parameters, validates dates, coordinates "nachlieferungs" (late/retroactive data delivery backfills), checks prior execution state against the database audit log table `DWTK_MELDUNGEN`, and conditionally triggers core export utilities (`r_exp_ikdb`). 

To align with target cloud patterns, these SQL checks, date increments, and conditional database updates will migrate to a modern metadata-driven orchestrator (Apache Airflow) running PySpark logic via Dataproc operators on Google Cloud Platform (GCP).

---

#### 2. UC4 Object Inventory
*Note: Only the target execution body (the shell script wrapper) was supplied in the raw input block. The parent schedule, Job Plan (JOBP), and Event definitions are mapped to architectural standards below.*

| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.EXPORT_IKDB_WRAPPER` | `JOBS_UNIX` | `<Active>1</Active>` (Assumed Default) | Shell-wrapper to initialize variables and conditionally spawn the exporter job `r_exp_ikdb`. |
| `DW.EXPORT_IKDB_WORKFLOW` | `JOBP` (Workflow) | `<Active>1</Active>` (Assumed Default) | Parent workflow container containing export steps. |

---

#### 3. Airflow DAG Properties

| Property | Value | Note / Target Setting |
| :--- | :--- | :--- |
| **dag_id** | `dw_export_ikdb_workflow` | Sanitized from original UC4 jobplan naming standard. |
| **schedule** | `0 3 * * *` | Placeholder (Equivalent to typical daily 03:00 run implied by `t-1` business logic). |
| **start_date** | `datetime(2025, 1, 1)` | Static past start date for baseline environment provisioning. |
| **catchup** | `False` | Catchup disabled to prevent unexpected backfill storming. |
| **max_active_runs** | `1` | Strictly configured to protect database concurrency locks. |
| **is_paused_upon_creation**| `False` | Enabled normally. |
| **default_args** | See below | Standard runtime characteristics. |

##### Default Arguments (`default_args`):
*   **owner**: `'data_engineering'`
*   **retries**: `1`
*   **retry_delay**: `timedelta(minutes=5)`

---

#### 4. Task Inventory

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `check_already_executed` | `BranchPythonOperator` | N/A | None | 0 | None | None | None | `False` | None | Queries target operational metadata table equivalent to `DWTK_MELDUNGEN`. |
| `run_export_ikdb` | `DataprocSubmitJobOperator` | `run_export_ikdb.py` | GCP_PROJECT, REGION, CLUSTER_NAME | 1 | 5 mins | None | None | `False` | `on_failure_alarm` | Executed if database check returns false. |
| `update_execution_status` | `DataprocSubmitJobOperator` | `update_status.py` | GCP_PROJECT, REGION, CLUSTER_NAME | 1 | 5 mins | None | None | `False` | `on_failure_alarm` | Registers status `2` (Success) in metadata tables. |
| `skipped_already_run` | `EmptyOperator` | N/A | None | 0 | None | None | None | `False` | None | Terminal leaf node when execution check skips. |

---

#### 5. Task Dependency Map
```text
start_pipeline >> check_already_executed 

check_already_executed >> run_export_ikdb >> update_execution_status >> end_pipeline
check_already_executed >> skipped_already_run >> end_pipeline
```

**Plain English Flow:**  
At runtime, the DAG initiates the `check_already_executed` node. This operator executes a metadata inquiry (historically handled in shell by `sql_to_variable Datumszziel` and querying table `DWTK_MELDUNGEN` for status `2`). If an entry exists for the current logical date, execution redirects down the skip path to `skipped_already_run` to prevent redundant exports. If empty, the export job runs as a PySpark script on GCP Dataproc, updating the success metadata table upon completion.

---

#### 6. Parameter and Variable Mapping

| UC4 Parameter | Value / Source | Airflow Equivalent / GCS Target URI |
| :--- | :--- | :--- |
| `-j JobKennung` | Job Key ID | `{{ dag.dag_id }}` (or Airflow config variable) |
| `-q l_DBskript` | Embedded SQL code | Passed as an argument to PySpark or translated to PySpark code. |
| `-s l_Date` | Default: Trunc(Sysdate)-1 | `{{ (execution_date - macros.timedelta(days=1)).strftime('%Y%m%d') }}` |
| `-t l_tminus` | Subtraction offset parameter | Airflow timedelta offset config value |
| `DW_ORAUSER` | Database User | Airflow Connection string credential secret / Google Secret Manager |
| `N/A` | PySpark target location | `gs://YOUR_BUCKET_NAME/pyspark_scripts/run_export_ikdb.py` |

---

#### 7. Error Handling and Retry Strategy
- **Task Failures:** Failed operations on either PySpark processing or metadata updates will trigger an `on_failure_callback` which formats a alert package containing running task attributes.
- **Sync Behavior:** Handled natively in Airflow by specifying `max_active_runs=1`, establishing a lock preventing parallel executions. 
- **ENDED_SKIPPED behavior:** The shell script handles skipped states logic internally via bash loops checking output files. In Airflow, this is elegantly handled via downstream conditional logic branches rather than overriding default Task Trigger states. (Standard trigger rule `TriggerRule.ALL_SUCCESS` remains unchanged to safeguard pipeline execution paths).

---

#### 8. Developer Notes
*   **GCP Credentials**: Replace placeholders starting with `YOUR_` with configurations derived from environmental variables, variables, or Secret Manager.
*   **Late Processing Mode (Nachlieferungsmodus)**: The wrapper contains a conditional loop verifying date sequences. This dynamic looping pattern is natively replaced in Airflow using explicit DAG configurations (such as standard Airflow catchups) or managed dynamically via a Python loop task that evaluates backfill requirements. 
*   **Status Codes**: Original Oracle table `DWTK_MELDUNGEN` references `STATUS_NR='2'` to denote run success. The migrated PySpark task writing status back must keep this integer integrity intact.

---

### SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.hooks.postgres import PostgresHook # Or OracleHook / SpannerHook depending on metadata storage target

# ── GCP Configuration ────────────────────────────────────
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
REGION = "YOUR_DATAPROC_REGION"
CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    On-failure alarm: Triggered on execution failure.
    Constructs an alert payload and forwards it to notification channels.
    """
    task_id = context['task_instance'].task_id
    run_id = context['task_instance'].run_id
    exception = context.get('exception')
    # TODO: Implement standard corporate alerting channel (Slack, PagerDuty, or Email)
    print(f"ALERT: Task {task_id} failed in run {run_id}. Exception: {exception}")

# ── Metadata Check Function ──────────────────────────────
def check_prior_run_status(**context):
    """
    Queries the job status table to verify if an export was already completed
    successfully for the target execution date (equivalent to Status 2).
    """
    # target_date derived dynamically (t-1 logical execution day)
    execution_date = context['execution_date']
    target_date_str = (execution_date - timedelta(days=1)).strftime('%Y%m%d')
    job_key = context['dag'].dag_id
    
    # Establish connection with the metadata audit database
    db_hook = PostgresHook(postgres_conn_id='metadata_db')
    sql = """
        SELECT COUNT(1) 
        FROM DWTK_MELDUNGEN 
        WHERE JOB_KENNUNG = %s 
          AND STATUS_NR = '2' 
          AND STICHTAG = TO_DATE(%s, 'YYYYMMDD');
    """
    result = db_hook.get_first(sql, parameters=(job_key, target_date_str))
    
    if result and result[0] > 0:
        print(f"Export already ran successfully for date {target_date_str}. Skipping.")
        return "skipped_already_run"
    else:
        print(f"Export required for date {target_date_str}. Proceeding.")
        return "run_export_ikdb"

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_export_ikdb_workflow",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # Equivalent daily 03:00 run
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # Dynamic execution router replacing bash shell status loop validation
    check_already_executed = BranchPythonOperator(
        task_id="check_already_executed",
        python_callable=check_prior_run_status,
        provide_context=True
    )

    # ── Task: run_export_ikdb ────────────────────────────────
    # Translates 'r_exp_ikdb -q $l_DBskript -j $JobKennung ...' to PySpark
    pyspark_job_config = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/run_export_ikdb.py",
            "args": [
                "--job_key", "{{ dag.dag_id }}",
                "--target_date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y%m%d') }}",
                "--sql_script", "ikdb_export_query.sql"
            ]
        }
    }

    run_export_ikdb = DataprocSubmitJobOperator(
        task_id="run_export_ikdb",
        job=pyspark_job_config,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=on_failure_alarm
    )

    # ── Task: update_execution_status ────────────────────────
    # Registers Success Code '2' inside metadata database
    update_job_config = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/update_status.py",
            "args": [
                "--job_key", "{{ dag.dag_id }}",
                "--target_date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y%m%d') }}",
                "--status", "2"
            ]
        }
    }

    update_execution_status = DataprocSubmitJobOperator(
        task_id="update_execution_status",
        job=update_job_config,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=on_failure_alarm
    )

    skipped_already_run = EmptyOperator(task_id="skipped_already_run")
    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # ── Dependencies ─────────────────────────────────────────
    start_pipeline >> check_already_executed
    
    # Path A: Job was not run yet
    check_already_executed >> run_export_ikdb >> update_execution_status >> end_pipeline
    
    # Path B: Job has already been completed successfully
    check_already_executed >> skipped_already_run >> end_pipeline
```
```

---

## 3. COMPREHENSIVE CONTEXT & INTEGRATION SPECIFICATIONS

### 3.1. Job Dependencies & Target Orchestration Wiring
The pipeline is not standalone. It interacts with other modules and requires the following wiring:
*   **Upstream Predecessor (Shared Includes Module):**
    *   `isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/ALLGEMEIN/INCLUDES` contains generic helper logic (like `DW.HOLE_PFAD.xml` and `DW.LESE_LOG.xml`). 
    *   **Migration State:** This module has been migrated but is currently sitting in an open Pull Request (`PR: https://github.com/gurunathan-prodapt/pi-agents/pull/626`).
    *   **Wiring Method:** Once merged, the shared includes are imported directly into this Airflow environment as general-purpose Python packages or referenced dynamically from a shared GCS bucket `gs://YOUR_BUCKET_NAME/dwh_includes/`.
*   **Downstream / Cross-Job Hand-off:**
    *   This job feeds files/metadata utilized downstream by `DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP` (Assembled Job). Hand-off is maintained via a Cloud Storage Pub/Sub trigger or via an Airflow sensor listening to downstream schema targets in BigQuery.

### 3.2. Scheduling Configuration
The source scheduler triggers this workflow in a nested task environment (not directly standalone). 
*   **Target Trigger Concept:** In GCP Cloud Composer, this pipeline is configured as a standalone callable DAG (`dw_dwh_exis_ikdb_stamm_r`). It contains **no static trigger schedule** (`schedule=None`) to preserve its role as a callable or parent-triggered unit. 
*   **Trigger Mechanism:** It must be called by the upstream parent workflow using Airflow's `TriggerDagRunOperator`.

### 3.3. Lineage Edges
*   **Upstream Producers:** Out-of-scope database tables belonging to `IKDB` (Oracle source).
*   **Downstream Consumers:** Target exports (`STAMM_OUT_TMD`) are stored in Cloud Storage, which trigger ingestion tasks into BigQuery tables owned by `DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP`.

### 3.4. Target File Plan

| Source File Path | Target File Path | Target Language | Role / Purpose |
|---|---|---|---|
| `isdwh/uc4_prod_exports/UC4_PROD - 0001/.../DW.DWH_EXIS_IKDB_STAMM_R.xml` | `dags/dw_dwh_exis_ikdb_stamm_r.py` | Python (Airflow DAG) | Workflow orchestrator containing task chains, concurrency locks, and execution branches. |
| `isdwh/exporter/ikdb/bin/r_exp_ikdb.ksh` | `pyspark_scripts/exis_ikdb_stamm_r.py` | Python (PySpark) | Computational script executed on Dataproc to run the export logic, parse SQL, write files, and update logs. |
| Legacy Inline SQL | `pyspark_scripts/sql/d_ikdb_exp_stamm.sql` | BigQuery SQL | The actual query used to pull the master data. |

---

## 4. ENVIRONMENT-SPECIFIC VARIABLES & VALUE POLICIES

To prevent hard-coded constants or prose placeholders (e.g., `<PROJECT_ID>`, `CHANGE_ME`), all environmental variables are mapped as follows:

### 4.1. Global (Environment-Wide) Configurations
These values are identical across dev/test/prod environments and describe the target infrastructure itself. They are retrieved at runtime via standard Airflow variables/parameters.

*   `GCP_PROJECT`: Fetched dynamically via `Variable.get("GCP_PROJECT")`
*   `GCP_REGION`: Fetched dynamically via `Variable.get("GCP_REGION")`
*   `DATAPROC_CLUSTER`: Fetched dynamically via `Variable.get("DATAPROC_CLUSTER")`
*   `GCS_BUCKET`: Fetched dynamically via `Variable.get("GCS_BUCKET")`
*   `BQ_DATASET`: Fetched dynamically via `Variable.get("BQ_DATASET")`

### 4.2. Job-Specific Configurations
These are parameters specific to this run instance, supplied directly to the execution config context or runtime execution parameters.

*   `JOB_KEY`: `"EXIS_IKDB_STAMM_R"`
*   `FILE_TYPE`: `"STAMM_OUT_TMD"`
*   `NUMERIC_PARAM`: `7` (Value of retroactive backfill day offset `-n`)
*   `SQL_SCRIPT`: `"d_ikdb_exp_stamm.sql"`

---

## 5. RISKS & MANUAL ACTIONS

*   **UNRESOLVED COMPONENT (Upstream Includes):** The helper module containing `DW.HOLE_PFAD` and `DW.LESE_LOG` is sitting in an unmerged state (`PR: https://github.com/gurunathan-prodapt/pi-agents/pull/626`). This workflow cannot be fully tested or deployed in production until this PR is resolved and merged.
*   **Database Connectivity Config:** Standardizes Airflow connections (`PostgresHook` or `OracleHook` wrapper matching the target operational metadata store database) to query table `DWTK_MELDUNGEN`. This connection must be configured manually inside Airflow admin panel.
*   **PRINT / LOG LITERAL RULES:** To maintain administrative trace compatibility, all output logs, standard output printouts, or messages are converted to Python logging statements verbatim, preserving any original languages (such as German terms: `"Zuweisung erfolgt"`, `"Nachlieferungsmodus"`, `"Exportjob lief bereits am Datum"`, `"Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet"`).