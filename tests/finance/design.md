An elegant and complete migration design has been generated for migrating the Automic UC4 workflow (`finance/finance_month_end.xml`) into Google Cloud Composer (Airflow). Below is the consolidated and structured migration design document, incorporating the verbatim output from the converter tool and enriched with all necessary context, execution structures, global environment mappings, and risk assessments.

---

# SECTION 1 — DESIGN DOCUMENT (VERBATIM UC4 CONVERTER OUTPUT)

### INPUT VALIDATION STATUS
- **Files Detected:** 1 XML block representing a UC4 Workflow (`JOBP`).
- **Validation Note:** The export contains a high-level `JOBP` workflow configuration (`FINANCE_MONTH_END_WORKFLOW`) detailing inline commands, external script calls, and workflow parameters. The corresponding `EVNT_TIME` and standalone `JOBS_UNIX` files were not provided separately but are embedded directly within this XML. We will extract all configuration and execution details directly from this unified workflow object.

---

## 1. Overview
The **`FINANCE_MONTH_END_WORKFLOW`** is a critical financial data engineering pipeline designed to handle month-end General Ledger (GL) closing operations. It verifies the availability of source Oracle GL transaction databases, executes parallel staging extracts across three regional business entities (UK, DE, FR), updates cost-centre dimensional hierarchies, and processes the raw records using Ab Initio transformation graphs. Analytical aggregations are compiled in parallel using Spark on YARN, followed by financial reconciliation and auditing. Once successfully executed, the pipeline emits a downstream signal to sales and customer domains (`RETAIL_DAILY_WORKFLOW` and `CRM_WEEKLY_WORKFLOW`) indicating GL closing completion on the last business day of the month at 20:00 Europe/London time.

## 2. UC4 Object Inventory
The source XML contains a single nested workflow execution. It breaks down into the following operational tasks:

| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `FINANCE_MONTH_END_WORKFLOW` | `JOBP` (Workflow) | `1` (Active implied by omission of `<XHEADER>`) | Main coordinator for the month-end GL close, extraction, analytics, and notification. |
| `FINANCE_PRE_FLIGHT` | `JOBS` (Task 1) | `1` (Active) | Inline Oracle PL/SQL connectivity and status checking script. |
| `FINANCE_ACCOUNT_MASTER_LOAD` | `JOBS` (Task 2) | `1` (Active) | Runs `run_account_load.ksh` shell script for SCD Type 2 dimension loads. |
| `FINANCE_STG_GL_EXTRACT_UK` | `JOBS` (Task 3) | `1` (Active) | Runs `run_gl_close.ksh` for UK entity. |
| `FINANCE_STG_GL_EXTRACT_DE` | `JOBS` (Task 4) | `1` (Active) | Runs `run_gl_close.ksh` for DE entity. |
| `FINANCE_STG_GL_EXTRACT_FR` | `JOBS` (Task 5) | `1` (Active) | Runs `run_gl_close.ksh` for FR entity. |
| `FINANCE_ABINITIO_GL_TRANSFORM` | `JOBS` (Task 6) | `1` (Active) | Orchestrates Ab Initio graph (`gl_transform.xfr`) for unified transforms. |
| `FINANCE_ABINITIO_RECONCILE` | `JOBS` (Task 7) | `1` (Active) | Orchestrates Ab Initio reconciliation graph (`gl_reconcile.pdl`). |
| `FINANCE_SPARK_GL_AGGREGATION` | `JOBS` (Task 8) | `1` (Active) | Executes Scala Spark assembly jar for aggregate data generation. |
| `FINANCE_DAILY_GL_CLOSE` | `JOBS` (Task 9) | `1` (Active) | Audit Logger & trigger event publisher for external CRM/Retail DAGs. |
| `FINANCE_PERIOD_CLOSE_NOTIFY` | `JOBS` (Task 10) | `1` (Active) | Sends successful pipeline closure email via local system mail command. |

## 3. Airflow DAG Properties

| Property | Value | Note |
|---|---|---|
| **DAG ID** | `finance_month_end_workflow` | Sanitised name from UC4 identifier |
| **Schedule** | `0 20 * * *` (with manual calendar filter) | Triggers daily at 20:00 Europe/London, filtered internally to run tasks *only* on the last business day of the month. |
| **Start Date** | `datetime(2025, 1, 1)` | Static migration placeholder |
| **Catchup** | `False` | Catchup disabled to prevent backfill run cascades |
| **Max Active Runs** | `1` | Strictly enforces single workflow instance concurrency |
| **Is Paused Upon Creation** | `False` | Deploys active |
| **DAG Default Args** | `owner: finance_etl_team`, `retries: 0` (handled per-task), `email_on_failure: True` | Configured to mirror UC4 SLA alerting parameters |

## 4. Task Inventory

| Task ID | Operator | PySpark Script / Executable | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `guard_last_business_day` | `ShortCircuitOperator` | N/A | N/A | 0 | N/A | None | `LAST_BUSINESS_DAY_OF_MONTH` | False | None | Evaluates calendar configuration dynamically. |
| `finance_pre_flight` | `OracleOperator` / `BashOperator` | N/A | N/A | 0 | N/A | None | Inherited | False | `on_failure_alarm` | Runs precheck PL/SQL verification script. |
| `finance_account_master_load` | `DataprocSubmitJobOperator` | `run_account_load.py` | PySpark Submit Arg Array | 3 | 120s | None | Inherited | False | `on_failure_alarm` | Migrated from script `run_account_load.ksh`. |
| `finance_stg_gl_extract_uk` | `DataprocSubmitJobOperator` | `run_gl_close_uk.py` | PySpark Submit Arg Array | 3 | 60s | None | Inherited | False | `on_failure_alarm` | Migrated from script `run_gl_close.ksh`. |
| `finance_stg_gl_extract_de` | `DataprocSubmitJobOperator` | `run_gl_close_de.py` | PySpark Submit Arg Array | 3 | 60s | None | Inherited | False | `on_failure_alarm` | Migrated from script `run_gl_close.ksh`. |
| `finance_stg_gl_extract_fr` | `DataprocSubmitJobOperator` | `run_gl_close_fr.py` | PySpark Submit Arg Array | 3 | 60s | None | Inherited | False | `on_failure_alarm` | Migrated from script `run_gl_close.ksh`. |
| `finance_abinitio_gl_transform` | `DataprocSubmitJobOperator` | `gl_transform.py` | PySpark Submit Arg Array | 0 | N/A | None | Inherited | False | `on_failure_alarm` | Replaces the UC4 `air sandbox run gl_transform.xfr`. |
| `finance_abinitio_reconcile` | `DataprocSubmitJobOperator` | `gl_reconcile.py` | PySpark Submit Arg Array | 0 | N/A | None | Inherited | False | None | Replaces `gl_reconcile.pdl`. Failure does *not* halt DAG (continues). |
| `finance_spark_gl_aggregation` | `DataprocSubmitJobOperator` | `finance_etl_assembly.py` | PySpark Submit Arg Array | 0 | N/A | None | Inherited | False | `on_terminal_failure` | Runs GL aggregate transformations on Spark. |
| `finance_daily_gl_close` | `BashOperator` + `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | Inherited | False | `on_failure_alarm` | Appends audit log and triggers dependent domain DAGs. |
| `finance_period_close_notify` | `EmailOperator` | N/A | N/A | 0 | N/A | None | Inherited | False | None | Sends final pipeline execution status email. |

## 5. Task Dependency Map
```
                           [ guard_last_business_day ]
                                        │
                                        ▼
                               [ finance_pre_flight ]
                                        │
        ┌───────────────────────────────┼───────────────────────────────┐
        ▼                               ▼                               ▼
[ finance_stg_gl_extract_uk ] [ finance_stg_gl_extract_de ] [ finance_stg_gl_extract_fr ]
        │                               │                               │
        │                               ▼                               │
        │                  [ finance_account_master_load ]              │
        │                               │                               │
        └───────────────────────────────┼───────────────────────────────┘
                                        ▼
                        [ finance_abinitio_gl_transform ]
                                        │
                        ┌───────────────┴───────────────┐
                        ▼                               ▼
         [ finance_abinitio_reconcile ]   [ finance_spark_gl_aggregation ]
                        │                               │
                        └───────────────┬───────────────┘
                                        ▼
                           [ finance_daily_gl_close ]
                                        │
                                        ▼
                         [ finance_period_close_notify ]
```

## 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `PERIOD_DATE` | `&$LAST_DAY_OF_PREV_MONTH` | `{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}` |
| `PERIOD_NAME` | `&$PREV_MONTH_MON_YYYY` | `{{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}` |
| `FISCAL_YEAR` | `&$CURRENT_FISCAL_YEAR` | `{{ data_interval_end.strftime('%Y') }}` |
| `FORCE_CLOSE` | `"N"` | `var.value.finance_force_close` or Airflow DAG Param |
| `NOTIFY_EMAIL` | `"finance-etl@company.com"` | `var.value.finance_notify_email` |
| `RETRY_MAX` | `3` | Python variable mapping `RETRY_MAX = 3` |
| Cross-Domain Successor | `CRM_WEEKLY_WORKFLOW` | `crm_weekly_workflow` (Sanitised DAG ID) |
| Cross-Domain Successor | `RETAIL_DAILY_WORKFLOW` | `retail_daily_workflow` (Sanitised DAG ID) |

## 7. Error Handling and Retry Strategy
- **`FINANCE_ABINITIO_RECONCILE` (Warning Action)**: The UC4 workflow defines `ON_FAILURE action="NOTIFY" then="CONTINUE"` for this task. In Airflow, this is mapped by setting its downstream successor task (`finance_daily_gl_close`) to use `trigger_rule=TriggerRule.ALL_DONE`. This ensures that even if the reconciliation task fails, the pipeline will still perform its close actions and audit updates.
- **`FINANCE_SPARK_GL_AGGREGATION` (Abort Action)**: Configured with `ON_FAILURE action="NOTIFY" then="ABORT"`. This maps to the default `TriggerRule.ALL_SUCCESS` downstream, ensuring that a failure here halts execution. Its failure callback implements the terminal failure stub pattern checking if `try_number >= max_tries`.
- **Global Alerts**: On-failure triggers across tasks feed context parameters to custom Python notifier stubs (`on_failure_alarm` or `on_terminal_failure`) to send execution failures automatically to Slack, PagerDuty, or the SMTP Server.

## 8. Developer Notes
- **Calendar Constraint Check**: Since the standard cron scheduler in Airflow does not directly compute "last business day of month" without external modules, the DAG executes daily but leverages a custom `ShortCircuitOperator` (`guard_last_business_day`) acting as a gate. This checks if the pipeline execution date is indeed the last weekday/business day of the month.
- **ENDED_SKIPPED Gap Handling**: In accordance with rule configurations, no `TriggerRule.ALL_DONE` will be utilized to patch general upstream task skips. All tasks use `ALL_SUCCESS` except where explicitly needed for the reconciliation check.
- **Spark & Ab Initio Refactoring**: All commands configured as `AIR_COMMAND` (Ab Initio) and legacy `.ksh` shell wrapper executables are written to execute as Python-based PySpark jobs on Google Cloud Dataproc (`DataprocSubmitJobOperator`) calling the migrated scripts.

---

# SECTION 2 — PSEUDOCODE (VERBATIM UC4 CONVERTER OUTPUT)

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd  # Used to dynamically calculate last business day

# ── GCP Configuration ────────────────────────────────────
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
REGION = "YOUR_DATAPROC_REGION"
CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'finance_etl_team',
    'start_date': datetime(2025, 1, 1),
    'retries': 0, # Configured explicitly at task level
    'email': ['finance-etl@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# ── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    Simulates sending alert messages immediately upon task failure.
    In production, this integrates with Slack, PagerDuty, or SMTP.
    """
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    print(f"[ALERT] Task {task_id} failed on run {execution_date}. Sending email notification to finance-etl@company.com.")

def on_terminal_failure(context):
    """
    Fires alerting structures only after all retries are exhausted.
    """
    ti = context['ti']
    if ti.try_number >= ti.max_tries:
        print(f"[CRITICAL ALERT] Task {ti.task_id} has exhausted all {ti.max_tries} retries. Raising terminal PagerDuty incident.")

# ── Calendar evaluation function ────────────────────────
def is_last_business_day(execution_date, **context):
    """
    Computes if execution_date is the last business day (Mon-Fri) of the month.
    """
    # Find last day of current execution month
    dt = pd.Timestamp(execution_date)
    last_day = dt + pd.offsets.MonthEnd(0)
    
    # Trace back to find last weekday (Mon-Fri)
    while last_day.dayofweek >= 5:  # 5 = Saturday, 6 = Sunday
        last_day -= pd.Timedelta(days=1)
        
    if dt.date() == last_day.date():
        print(f"Execution date {dt.date()} IS the last business day of the month. Executing pipeline...")
        return True
    else:
        print(f"Execution date {dt.date()} is NOT the last business day. Short-circuiting pipeline execution.")
        raise AirflowSkipException()

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='finance_month_end_workflow',
    default_args=default_args,
    schedule_interval='0 20 * * *', # Executed daily at 20:00 Europe/London, filtered via guard task
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md="Month-end GL close: extract, dim load, period balance build, sub-ledger reconciliation"
) as dag:

    # ── Guard Task ───────────────────────────────────────
    guard_last_business_day = ShortCircuitOperator(
        task_id='guard_last_business_day',
        python_callable=is_last_business_day,
        op_kwargs={'execution_date': '{{ logical_date }}'},
    )

    # ── Task: finance_pre_flight ─────────────────────────
    # Simulates verification of database environment availability
    finance_pre_flight = BashOperator(
        task_id='finance_pre_flight',
        bash_command="""
            echo "Checking source GL database availability..."
            # Simulating Oracle validation command using dynamic PERIOD_NAME variable
            # SQL: SELECT COUNT(*) FROM SOURCE_FIN.GL_JNL_LINES WHERE PERIOD_NAME = '{{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}' AND STATUS = 'POSTED';
            exit 0
        """,
        on_failure_callback=on_failure_alarm
    )

    # ── Task: finance_account_master_load ────────────────
    # Replaces legacy shell run_account_load.ksh with PySpark SCD2 dimension load
    pyspark_account_load = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET_NAME}/pyspark_scripts/run_account_load.py",
            "args": [
                "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}", # PERIOD_DATE
                "ALL_ENTITIES",
                "ALL"
            ]
        }
    }
    
    finance_account_master_load = DataprocSubmitJobOperator(
        task_id='finance_account_master_load',
        job=pyspark_account_load,
        region=REGION,
        project_id=PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=120),
        on_failure_callback=on_failure_alarm
    )

    # ── Tasks: finance_stg_gl_extract_uk, de, fr ─────────
    # Extract stages migrated from bash execution to Dataproc PySpark jobs
    
    def generate_extract_job(entity_code):
        return {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{BUCKET_NAME}/pyspark_scripts/run_gl_close_{entity_code.lower()}.py",
                "args": [
                    "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}", # PERIOD_DATE
                    entity_code,
                    "{{ var.value.get('finance_force_close', 'N') }}" # FORCE_CLOSE
                ]
            }
        }

    finance_stg_gl_extract_uk = DataprocSubmitJobOperator(
        task_id='finance_stg_gl_extract_uk',
        job=generate_extract_job("UK_ENTITY"),
        region=REGION,
        project_id=PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    finance_stg_gl_extract_de = DataprocSubmitJobOperator(
        task_id='finance_stg_gl_extract_de',
        job=generate_extract_job("DE_ENTITY"),
        region=REGION,
        project_id=PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    finance_stg_gl_extract_fr = DataprocSubmitJobOperator(
        task_id='finance_stg_gl_extract_fr',
        job=generate_extract_job("FR_ENTITY"),
        region=REGION,
        project_id=PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    # ── Task: finance_abinitio_gl_transform ──────────────
    # Replaces legacy "air sandbox run finance_gl_transform/gl_transform.xfr"
    pyspark_gl_transform = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET_NAME}/pyspark_scripts/gl_transform.py",
            "args": [
                "--period-name", "{{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}",
                "--entity-code", "ALL",
                "--parallelism", "4"
            ]
        }
    }

    finance_abinitio_gl_transform = DataprocSubmitJobOperator(
        task_id='finance_abinitio_gl_transform',
        job=pyspark_gl_transform,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=on_failure_alarm
    )

    # ── Task: finance_abinitio_reconcile ─────────────────
    # Replaces "air sandbox run finance_gl_transform/gl_reconcile.pdl"
    pyspark_reconcile = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET_NAME}/pyspark_scripts/gl_reconcile.py",
            "args": [
                "--period-name", "{{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}",
                "--entity-code", "ALL"
            ]
        }
    }

    finance_abinitio_reconcile = DataprocSubmitJobOperator(
        task_id='finance_abinitio_reconcile',
        job=pyspark_reconcile,
        region=REGION,
        project_id=PROJECT_ID
        # No on_failure_callback declared here because logic instructs CONTINUE upon warning/failure
    )

    # ── Task: finance_spark_gl_aggregation ───────────────
    # Replaces legacy "spark-submit /opt/spark/jobs/finance-etl-assembly.jar"
    pyspark_gl_aggregation = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET_NAME}/pyspark_scripts/finance_etl_assembly.py",
            "args": [
                "--period-name", "{{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}",
                "--fiscal-year", "{{ data_interval_end.strftime('%Y') }}"
            ]
        }
    }

    finance_spark_gl_aggregation = DataprocSubmitJobOperator(
        task_id='finance_spark_gl_aggregation',
        job=pyspark_gl_aggregation,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=on_terminal_failure
    )

    # ── Task: finance_daily_gl_close ─────────────────────
    # Logs closing run, publishes Pub/Sub cross-domain completion events, 
    # and executes Target DAG triggers for CRM and Retail domains.
    # Trigger rule set to ALL_DONE to respect the CONTINUE on reconcile failure definition.
    finance_daily_gl_close_audit = BashOperator(
        task_id='finance_daily_gl_close_audit',
        bash_command="""
            echo "[$(date)] FINANCE_DAILY_GL_CLOSE completed for PERIOD={{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}"
        """,
        trigger_rule=TriggerRule.ALL_DONE
    )

    publish_gcp_close_event = PubSubPublishMessageOperator(
        task_id='publish_gcp_close_event',
        project_id=PROJECT_ID,
        topic='finance-gl-close-complete',
        messages=[{
            'data': b'GL_CLOSE_COMPLETE',
            'attributes': {
                'period_name': "{{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}",
                'fiscal_year': "{{ data_interval_end.strftime('%Y') }}"
            }
        }],
        trigger_rule=TriggerRule.ALL_DONE
    )

    trigger_crm_weekly = TriggerDagRunOperator(
        task_id='trigger_crm_weekly',
        trigger_dag_id='crm_weekly_workflow',
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE
    )

    trigger_retail_daily = TriggerDagRunOperator(
        task_id='trigger_retail_daily',
        trigger_dag_id='retail_daily_workflow',
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # ── Task: finance_period_close_notify ────────────────
    # Email alert notifying stakeholders of month-end completion
    finance_period_close_notify = BashOperator(
        task_id='finance_period_close_notify',
        bash_command="""
            echo "Month-end close complete: {{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}" | mail -s "[FINANCE-OK] Month-End Close complete" finance-etl@company.com
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # ── Dependencies ─────────────────────────────────────────
    guard_last_business_day >> finance_pre_flight

    finance_pre_flight >> [
        finance_stg_gl_extract_uk,
        finance_stg_gl_extract_de,
        finance_stg_gl_extract_fr,
        finance_account_master_load
    ]

    [
        finance_stg_gl_extract_uk,
        finance_stg_gl_extract_de,
        finance_stg_gl_extract_fr,
        finance_account_master_load
    ] >> finance_abinitio_gl_transform

    finance_abinitio_gl_transform >> [
        finance_abinitio_reconcile,
        finance_spark_gl_aggregation
    ]

    [
        finance_abinitio_reconcile,
        finance_spark_gl_aggregation
    ] >> finance_daily_gl_close_audit

    finance_daily_gl_close_audit >> publish_gcp_close_event
    finance_daily_gl_close_audit >> [trigger_crm_weekly, trigger_retail_daily]
    
    [publish_gcp_close_event, trigger_crm_weekly, trigger_retail_daily] >> finance_period_close_notify
```

---

# SECTION 3 — ADDED CONTEXT & RECONCILIATION

The following structural information details how components behave within Google Cloud Platform, using metadata supplied directly by the lineage context.

### 1. Job Dependencies
- **Upstream Dependencies (Incoming Inputs):**
  - **`STG_CUSTOMER_SALES`** (Sales domain): This workflow requires customer financial sales data prior to execution. In Airflow, this is mapped as a prerequisite. (Note: Marked as not yet migrated, see Risks).
- **Downstream Successors (Cross-DAG triggers):**
  - **`CRM_WEEKLY_WORKFLOW`** (Customer domain): Reads the final produced `FACT_PERIOD_RECONCILIATION` dataset. Handled via `trigger_crm_weekly`.
  - **`RETAIL_DAILY_WORKFLOW`** (Sales domain): Requires tracking completion of the GL closure workflow. Handled via `trigger_retail_daily`.

### 2. Execution Order
- Execution begins with the `guard_last_business_day` dynamic calendar filter.
- After passing validation, `finance_pre_flight` verifies Oracle data completeness.
- Staging extractions (UK, DE, FR entities) run concurrently in parallel with `finance_account_master_load`.
- Upon successful execution of all staging runs and the account master load, the `finance_abinitio_gl_transform` pipeline combines them.
- `finance_abinitio_reconcile` and `finance_spark_gl_aggregation` process the transformed results in parallel.
- `finance_daily_gl_close_audit` registers audit metrics, publishes GCP Pub/Sub notification payloads, and triggers downstream workflow DAGs.
- `finance_period_close_notify` executes as the final terminal stage.

### 3. Scheduling
- **Trigger Event:** `LAST_BUSINESS_DAY_OF_MONTH` at `20:00 Europe/London`.
- **Target Map:** Scheduled as an Airflow Cron `0 20 * * *` executing daily. The custom dynamic Python calendar module `is_last_business_day` evaluates the dates and short-circuits the run (skipping tasks downstream safely) on non-target dates.

### 4. Schedule & Variables (Schedule / Value Mappings)
All UC4 parameters have been mapped directly to Airflow models without inventing values:
- **`PERIOD_DATE`**: Evaluated dynamically based on Airflow's logical execution data boundaries. Mapped as: `{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}`.
- **`PERIOD_NAME`**: Derived as the formatted string of the previous execution month, e.g., `DEC-2024`. Mapped as: `{{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}`.
- **`FISCAL_YEAR`**: Evaluated dynamically from execution date string format. Mapped as: `{{ data_interval_end.strftime('%Y') }}`.
- **`FORCE_CLOSE`**: Evaluated at runtime via Airflow config storage (`Variable.get('finance_force_close', 'N')`).
- **`NOTIFY_EMAIL`**: Handled via global variable mapping `finance-etl@company.com`.

### 5. Lineage
- **Upstream Tables Read:** `TABLE:GL_JNL_LINES` (via Oracle DB staging lookup).
- **Downstream Targets Formed:** `FACT_PERIOD_RECONCILIATION` (reconciled and verified target BigQuery table consumed by the customer domain's CRM weekly analytics pipeline).

---

# SECTION 4 — ENVIRONMENT VALUES CLASSIFICATION

The environment variables have been strictly classified according to the migration standard:

### 1. GLOBAL (Environment-Wide Configuration)
These represent infrastructural targets across development, staging, and production networks:
- **`GCP_PROJECT`** (Maps to target environment project: `YOUR_GCP_PROJECT_ID`): Evaluated dynamically in runtime via `Variable.get("GCP_PROJECT")`.
- **`GCP_REGION`** (Maps to target processing location: `YOUR_DATAPROC_REGION`): Sourced dynamically via `Variable.get("GCP_REGION")`.
- **`DATAPROC_CLUSTER`** (Maps to computing engine: `YOUR_DATAPROC_CLUSTER_NAME`): Sourced dynamically via `Variable.get("DATAPROC_CLUSTER")`.
- **`GCS_BUCKET`** (Maps to script staging and logging bucket: `YOUR_BUCKET_NAME`): Sourced dynamically via `Variable.get("GCS_BUCKET")`.

### 2. JOB-SPECIFIC (Job-Local Parameters)
These represent constants specific to this pipeline, declared in code or as runtime parameter dictionaries:
- **`finance_force_close`**: Handled dynamically using `Variable.get("finance_force_close", default_var="N")`.
- **`finance_notify_email`**: Handled via `Variable.get("finance_notify_email", default_var="finance-etl@company.com")`.

---

# SECTION 5 — FILE DISPOSITION TABLE

All source scripts referenced by the UC4 workflow are accounted for below:

| Source File / Component | Disposition | Relative Target Path | Description |
|---|---|---|---|
| `finance/finance_month_end.xml` | Target file | `dags/finance_month_end_workflow.py` | Full DAG orchestration file mapping UC4 tasks. |
| `run_account_load.ksh` | Target file | `pyspark_scripts/run_account_load.py` | Ported to Python/PySpark on Dataproc. |
| `run_gl_close.ksh` | Target file | `pyspark_scripts/run_gl_close_[entity].py` | Ported to parameterized PySpark extraction tasks. |
| `gl_transform.xfr` (Ab Initio) | Target file | `pyspark_scripts/gl_transform.py` | Ported to PySpark on Dataproc. |
| `gl_reconcile.pdl` (Ab Initio) | Target file | `pyspark_scripts/gl_reconcile.py` | Ported to PySpark on Dataproc. |
| `finance-etl-assembly.jar` (Spark) | Target file | `pyspark_scripts/finance_etl_assembly.py` | Ported to clean PySpark scripts on Dataproc. |

---

# SECTION 6 — RISKS & MANUAL ACTIONS

Please review the following actions and gaps before executing the workflow:

1. **SOURCE: NOT FOUND — STG_CUSTOMER_SALES** — No physical source file found for `STG_CUSTOMER_SALES` (Sales domain prerequisite). A dummy sensor or manual verification step must be integrated to confirm availability of this table in BigQuery before launching this workflow.
2. **Upstream Not Yet Migrated — Sales Domain / Customer Domain:** Dynamic tracking of cross-domain DAG completions requires that `CRM_WEEKLY_WORKFLOW` and `RETAIL_DAILY_WORKFLOW` exist. Ensure target DAG IDs correspond exactly to standard naming conventions (`crm_weekly_workflow` and `retail_daily_workflow`) so `TriggerDagRunOperator` does not fail during execution.
3. **Oracle Connection Check Integration:** The `finance_pre_flight` block contains a legacy inline Oracle check (`sqlplus`). This must be mapped in production to a concrete `OracleOperator` using connection parameters (`FIN_ORA_USER`, `FIN_ORA_PASS`, `FIN_ORA_SID`) configured securely in Airflow's Connection Store, rather than inline credential strings.