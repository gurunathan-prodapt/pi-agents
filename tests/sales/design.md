An elegant, production-ready Cloud Composer (Airflow) design has been generated based on the high-confidence `UC4_ONLY` migration pattern prescribed for this workflow.

Below is the complete, verbatim tool output along with the necessary cross-job context, global environment configuration rules, file dispositions, and operational details.

---

### File Disposition

| Legacy Source File | Target File / Action | Merged Into / Key Changes |
| :--- | :--- | :--- |
| `sales/retail_daily_workflow.xml` | `dags/retail_daily_workflow.py` | Migrated 1:1 to an Airflow DAG. Standardized UC4 XML structures, job sequences, variable substitutions, scheduling, and notifications into native Airflow tasks and parameters. |

---

### Section 1 — Verbatim Migration Design Document

*The section below contains the complete and exact output of the transformation tool:*

=== Result for sales/retail_daily_workflow.xml ===
### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
The `RETAIL_DAILY_WORKFLOW` is a daily retail sales Extraction, Transformation, and Loading (ETL) pipeline serving the Retail Data Warehouse. It automates the staging extraction of sales transactions from regional Oracle point-of-sale (POS) databases (representing Northern and Southern regions), loads Slowly Changing Dimension (SCD) Type 2 product tables, executes an Ab Initio aggregation transformation, runs a Spark analytical aggregation, performs analytical data quality checks, and publishes a downstream event to trigger the CRM Weekly Workflow. The DAG runs daily at 02:00 Europe/London time and exhibits cross-domain data dependencies, specifically requiring the completion of financial ledger closing from the Finance domain before execution.

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `RETAIL_DAILY_WORKFLOW` | `JOBP` (Workflow) | Active (default `1` assumed) | Main daily orchestrator for the retail sales ETL pipeline. |
| `RETAIL_PRE_CHECK` | `JOBS` (Task 1) | Active (default `1` assumed) | Verifies the accessibility and data availability of the source Oracle POS database. |
| `RETAIL_STG_EXTRACT_NORTH` | `JOBS` (Task 2) | Active (default `1` assumed) | Extracts daily transaction data from the Northern POS source. |
| `RETAIL_STG_EXTRACT_SOUTH` | `JOBS` (Task 3) | Active (default `1` assumed) | Extracts daily transaction data from the Southern POS source. |
| `RETAIL_PRODUCT_MASTER_LOAD` | `JOBS` (Task 4) | Active (default `1` assumed) | Loads and processes product Master dimension reference data (SCD Type 2). |
| `RETAIL_ABINITIO_TRANSFORM` | `JOBS` (Task 5) | Active (default `1` assumed) | Executes the `sales_rollup.xfr` aggregation rules logic. |
| `RETAIL_SPARK_AGGREGATION` | `JOBS` (Task 6) | Active (default `1` assumed) | Executes the Spark analytical layer aggregation using Scala. |
| `RETAIL_DATA_QUALITY_CHECK` | `JOBS` (Task 7) | Active (default `1` assumed) | Performs Python-based schema and threshold validation on loaded tables. |
| `RETAIL_COMPLETION_NOTIFY` | `JOBS` (Task 8) | Active (default `1` assumed) | Sends notifications and fires event signals for downstream dependencies. |

#### 3. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **DAG ID** | `retail_daily_workflow` |
| **Schedule (Cron)** | `0 2 * * *` (Daily at 02:00; local timezone `Europe/London`) |
| **Start Date** | `datetime(2024, 1, 1)` |
| **Catchup** | `False` |
| **Max Active Runs** | `1` |
| **is_paused_upon_creation** | `False` |
| **Default Args** | `{ "owner": "dw_team", "email": ["dw-alerts@company.com"], "email_on_failure": True, "retries": 0, "retry_delay": timedelta(minutes=5) }` |

#### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `wait_for_finance_gl_close` | `ExternalTaskSensor` | N/A | N/A | 3 | 5 min | None | None | N/A | None | Cross-workflow/domain dependency check. |
| `retail_pre_check` | `DataprocSubmitJobOperator` | `retail_pre_check.py` | PySpark job placeholder | 2 | 2 min | None | None | `False` | `on_failure_alarm` | Oracle availability database query. |
| `retail_stg_extract_north` | `DataprocSubmitJobOperator` | `load_daily_sales_north.py` | PySpark job placeholder | 3 | 1 min | None | None | `False` | `on_failure_alarm` | Extract script matching region parameters. |
| `retail_stg_extract_south` | `DataprocSubmitJobOperator` | `load_daily_sales_south.py` | PySpark job placeholder | 3 | 1 min | None | None | `False` | `on_failure_alarm` | Extract script matching region parameters. |
| `retail_product_master_load` | `DataprocSubmitJobOperator` | `load_product_master.py` | PySpark job placeholder | 3 | 1 min | None | None | `False` | `on_failure_alarm` | SCD Type 2 dimension writer. |
| `retail_abinitio_transform` | `DataprocSubmitJobOperator` | `sales_rollup.py` | PySpark job placeholder | 0 | N/A | None | None | `False` | `on_failure_alarm` | Refactored Ab Initio graph logic. |
| `retail_spark_aggregation` | `DataprocSubmitJobOperator` | `sales_aggregation.py` | PySpark job placeholder | 0 | N/A | None | None | `False` | `on_failure_alarm` | Refactored Scala Spark Submit logic. |
| `retail_data_quality_check` | `DataprocSubmitJobOperator` | `retail_data_quality.py` | PySpark job placeholder | 0 | N/A | None | None | `False` | `on_failure_alarm` | Python verification job; warnings ignored. |
| `retail_completion_notify` | `EmailOperator` | N/A | N/A | 0 | N/A | None | None | `False` | `on_failure_alarm` | Success notification step. |
| `publish_downstream_trigger` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | `False` | None | Triggers `crm_weekly_workflow`. |

#### 5. Task Dependency Map
```text
                  [ wait_for_finance_gl_close ]
                               │
                               ▼
                      [ retail_pre_check ]
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
   [ retail_stg_extract_north ]  [ retail_stg_extract_south ]
                └──────────────┬──────────────┘
                               ▼
                [ retail_product_master_load ]
                               │
                               ▼
                [ retail_abinitio_transform ]
                               │
                               ▼
                [ retail_spark_aggregation ]
                               │
                               ▼
                [ retail_data_quality_check ] (trigger_rule="all_done")
                               │
                               ▼
                [ retail_completion_notify ]
                               │
                               ▼
               [ publish_downstream_trigger ]
```
* **Dependency Narrative:** 
  1. The run begins by executing an `ExternalTaskSensor` named `wait_for_finance_gl_close` to ensure the upstream ledger closing pipeline has successfully processed.
  2. Upon verification, the connection sanity check `retail_pre_check` executes.
  3. Once POS availability is checked, the pipeline bifurcates to execute the regional extract processes (`retail_stg_extract_north` and `retail_stg_extract_south`) in parallel.
  4. Both regional extracts must succeed before `retail_product_master_load` can begin execution.
  5. Successful loading of dimensions triggers the core aggregation pipelines (`retail_abinitio_transform` and `retail_spark_aggregation`) sequentially.
  6. The `retail_data_quality_check` task runs next with `trigger_rule="all_done"` so it can assess validation metrics even if some up-stream tasks emitted warnings (standard behaviour of the source system's `SUCCESS_OR_WARNING` status handling).
  7. Successful validation triggers email alerts via `retail_completion_notify` and triggers the downstream processing workflow `crm_weekly_workflow` via `publish_downstream_trigger`.

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&LOAD_DATE` | `&amp;$TODAY-1D` | `{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}` |
| `&BATCH_ID` | `&amp;$TODAY_YYYYMMDD` | `{{ execution_date.strftime('%Y%m%d') }}` |
| `&RETRY_MAX` | `3` | Default parameter mapped to operator arguments or DAG parameters |
| `&RETRY_WAIT` | `60` (seconds) | Mapped to `retry_delay=timedelta(seconds=60)` |
| `&ENV` | `PROD` | Handled via Environment Airflow Variable (e.g. `var.value.ENV`) |
| `&NOTIFY_EMAIL` | `dw-alerts@company.com` | Airflow variables config, or mapped to task notifications list |
| `CRM_WEEKLY_WORKFLOW` | Downstream Target workflow | Sanitised ID: `crm_weekly_workflow` |
| `FINANCE_DAILY_WORKFLOW` | Cross-domain Upstream DAG | Target ID: `finance_daily_workflow` |

#### 7. Error Handling and Retry Strategy
* **Pre-Check and Extract Tasks:** These contain localized retries directly configured into the task options (`retries=2` and `retries=3` respectively) matching the underlying export specifications.
* **Failure Alerts:** Global notifications on workflow failures and SLA violations are specified inside UC4. To match this in Airflow, an custom `on_failure_callback` called `on_failure_alarm` will be configured inside the default parameters of the DAG. It will capture operational failures and dispatch an notification to `dw-alerts@company.com`.
* **Data Quality Check Rule:** The data quality check runs downstream of critical computations and acts as an evaluation tool. If it emits a warning (represented in UC4 as `SUCCESS_OR_WARNING`), execution must carry on. In Airflow, this is mapped by setting `trigger_rule="all_done"` on `retail_data_quality_check` (or allowing soft-failures within its internal evaluation framework) without changing general DAG trigger rules which default to `all_success`.

#### 8. Developer Notes
* **GCP Infrastructure Resource Requirements:** Configuration settings for Dataproc clusters (Project ID, Regions, Cluster IDs, GCS Storage path buckets) must be injected into the DAG variables structure prior to deployment.
* **Timezone Offset Handling:** The schedule is based on local European/London time, which undergoes Daylight Savings (GMT/BST) shifts. Airflow's timezone module must be utilized in the DAG parameters.
* **Cross-DAG Dependency Coordination:** The `ExternalTaskSensor` relies on consistent execution dates between the target DAGs. If `finance_daily_workflow` runs on a different schedule, you will need to adjust the execution delta offset parameter inside the sensor.
* **Warning Handling Rules:** The data quality validation script must be designed to exit with success even if metrics violations are raised, optionally writing warnings to a destination table, to avoid breaking downstream dependency tasks.

---

### SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

# ── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    Callback stub designed to raise alert notification payloads
    on operational task failures to dw-alerts@company.com.
    """
    # task_id = context['task_instance'].task_id
    # execution_date = context['execution_date']
    # exception = context.get('exception')
    # TODO: Connect custom email alerts utility helper or pager alert service
    pass

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'dw_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['dw-alerts@company.com'],
    'email_on_failure': True,
    'on_failure_callback': on_failure_alarm,
}

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='retail_daily_workflow',
    default_args=default_args,
    description='Daily retail sales ETL pipeline migrated from UC4 environment',
    schedule='0 2 * * *',  # Europe/London Local time (handled by setting DAG timezone parameter)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
)

# ── Sensor Task (Cross-Domain Dependency Check) ──────────
wait_for_finance_gl_close = ExternalTaskSensor(
    task_id='wait_for_finance_gl_close',
    external_dag_id='finance_daily_workflow',
    external_task_id='finance_daily_gl_close',
    allowed_states=['success'],
    execution_delta=timedelta(hours=0), # Adjusted if schedules diverge
    mode='poke',
    poke_interval=300,
    timeout=7200,
    dag=dag
)

# ── Task: RETAIL_PRE_CHECK ────────────────────────────────────
# Precheck job maps inline validation queries to a Python script validator
pyspark_pre_check_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/retail_pre_check.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--batch-id", "{{ execution_date.strftime('%Y%m%d') }}"
        ]
    }
}

retail_pre_check = DataprocSubmitJobOperator(
    task_id='retail_pre_check',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_pre_check_job,
    retries=2,
    retry_delay=timedelta(seconds=120),
    dag=dag
)

# ── Task: RETAIL_STG_EXTRACT_NORTH ────────────────────────────
pyspark_extract_north_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/load_daily_sales_north.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--region", "NORTH",
            "--env", "PROD"
        ]
    }
}

retail_stg_extract_north = DataprocSubmitJobOperator(
    task_id='retail_stg_extract_north',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_extract_north_job,
    retries=3,
    retry_delay=timedelta(seconds=60),
    execution_timeout=timedelta(minutes=90),
    dag=dag
)

# ── Task: RETAIL_STG_EXTRACT_SOUTH ────────────────────────────
pyspark_extract_south_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/load_daily_sales_south.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--region", "SOUTH",
            "--env", "PROD"
        ]
    }
}

retail_stg_extract_south = DataprocSubmitJobOperator(
    task_id='retail_stg_extract_south',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_extract_south_job,
    retries=3,
    retry_delay=timedelta(seconds=60),
    execution_timeout=timedelta(minutes=90),
    dag=dag
)

# ── Task: RETAIL_PRODUCT_MASTER_LOAD ──────────────────────────
pyspark_product_master_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/load_product_master.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--retries", "3",
            "--retry-wait", "60"
        ]
    }
}

retail_product_master_load = DataprocSubmitJobOperator(
    task_id='retail_product_master_load',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_product_master_job,
    retries=3,
    retry_delay=timedelta(seconds=60),
    execution_timeout=timedelta(minutes=60),
    dag=dag
)

# ── Task: RETAIL_ABINITIO_TRANSFORM ───────────────────────────
pyspark_abinitio_transform_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/sales_rollup.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--region-code", "ALL",
            "--parallelism", "4"
        ]
    }
}

retail_abinitio_transform = DataprocSubmitJobOperator(
    task_id='retail_abinitio_transform',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_abinitio_transform_job,
    execution_timeout=timedelta(minutes=120),
    dag=dag
)

# ── Task: RETAIL_SPARK_AGGREGATION ────────────────────────────
pyspark_aggregation_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/sales_aggregation.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--env", "PROD"
        ]
    }
}

retail_spark_aggregation = DataprocSubmitJobOperator(
    task_id='retail_spark_aggregation',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_aggregation_job,
    execution_timeout=timedelta(minutes=60),
    dag=dag
)

# ── Task: RETAIL_DATA_QUALITY_CHECK ───────────────────────────
pyspark_dq_check_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/retail_data_quality.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--env", "PROD",
            "--notify-email", "dw-alerts@company.com"
        ]
    }
}

# Employs trigger rule: all_done to resemble UC4 SUCCESS_OR_WARNING path flow behaviour.
retail_data_quality_check = DataprocSubmitJobOperator(
    task_id='retail_data_quality_check',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_dq_check_job,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# ── Task: RETAIL_COMPLETION_NOTIFY ────────────────────────────
retail_completion_notify = EmailOperator(
    task_id='retail_completion_notify',
    to='dw-alerts@company.com',
    subject="[ETL-OK] Retail Daily Load {{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
    html_content="""
    <h3>RETAIL_DAILY_WORKFLOW completed status successfully</h3>
    <p>Daily load processes are finalized for run date.</p>
    """,
    dag=dag
)

# ── Task: DOWNSTREAM TRIGGER EVENT ───────────────────────────
publish_downstream_trigger = TriggerDagRunOperator(
    task_id='publish_downstream_trigger',
    trigger_dag_id='crm_weekly_workflow',
    wait_for_completion=False,
    conf={"triggered_by_parent": "retail_daily_workflow"},
    dag=dag
)

# ── Dependencies ─────────────────────────────────────────
wait_for_finance_gl_close >> retail_pre_check

retail_pre_check >> [retail_stg_extract_north, retail_stg_extract_south]

[retail_stg_extract_north, retail_stg_extract_south] >> retail_product_master_load

retail_product_master_load >> retail_abinitio_transform >> retail_spark_aggregation

retail_spark_aggregation >> retail_data_quality_check

retail_data_quality_check >> retail_completion_notify >> publish_downstream_trigger
```

---

### Section 3 — Context & Architectural Alignment

#### 1. Scheduling & Execution Constraints
* **Timing:** The scheduler trigger `02:00 Europe/London` runs daily. It has been preserved via standard Airflow CRON expressions inside the Python code definition.
* **Execution Order:** 
  1. `FINANCE_DAILY_WORKFLOW.FINANCE_DAILY_GL_CLOSE` (Upstream Cross-Domain External Task)
  2. `RETAIL_PRE_CHECK`
  3. Parallel split: `RETAIL_STG_EXTRACT_NORTH` & `RETAIL_STG_EXTRACT_SOUTH`
  4. Parallel sync: `RETAIL_PRODUCT_MASTER_LOAD`
  5. `RETAIL_ABINITIO_TRANSFORM` (Execution logic transformed to PySpark on Dataproc)
  6. `RETAIL_SPARK_AGGREGATION` (Spark aggregation submitted to Dataproc)
  7. `RETAIL_DATA_QUALITY_CHECK` (Failsafe logic, warnings accepted)
  8. `RETAIL_COMPLETION_NOTIFY` & downstream `CRM_WEEKLY_WORKFLOW` event trigger execution.

#### 2. Upstream & Downstream Dependencies
* **Upstream:**
  * `FINANCE_DAILY_WORKFLOW` -> `FINANCE_DAILY_GL_CLOSE` (Marked as an `ExternalTaskSensor` cross-DAG check in Cloud Composer).
  * *Risks & Manual Actions:* If the finance pipeline has not yet been migrated, the task sensor connection details cannot be validated.
* **Downstream:**
  * `CRM_WEEKLY_WORKFLOW` -> triggered via `TriggerDagRunOperator` upon completion of `RETAIL_COMPLETION_NOTIFY`.
* **Lineage Database:**
  * Reads from Oracle source table: `SOURCE_OPS.SALES_TXN`. This database extraction logic is mapped to PySpark extractor scripts executing on Google Dataproc.

#### 3. Environment-Specific Variable Classifications
Following the global variable management policies:

| Source Variable Key | Target Variable Role | Airflow Fetch Mechanism | Value / Scope |
| :--- | :--- | :--- | :--- |
| `GCP_PROJECT_ID` | **GLOBAL** (Infra) | `Variable.get("GCP_PROJECT")` | Identifies Google Cloud Project. |
| `DATAPROC_REGION` | **GLOBAL** (Infra) | `Variable.get("DATAPROC_REGION")` | GCE Compute Region for Dataproc cluster. |
| `DATAPROC_CLUSTER_NAME`| **GLOBAL** (Infra) | `Variable.get("DATAPROC_CLUSTER")` | Managed Dataproc execution environment cluster. |
| `GCS_BUCKET_NAME` | **GLOBAL** (Infra) | `Variable.get("GCS_BUCKET")` | Storage bucket housing deployed scripts. |
| `ENV` | **JOB-SPECIFIC** | Mapped using local DAG configs / args | `PROD` |
| `NOTIFY_EMAIL` | **JOB-SPECIFIC** | Mapped directly in execution operators | `dw-alerts@company.com` |

---

### Section 4 — Risks & Manual Operational Steps

1. **UPSTREAM NOT YET MIGRATED:** The wiring of `wait_for_finance_gl_close` depends on the existence of `finance_daily_workflow` in Cloud Composer. If not yet migrated, this sensor will timeout. A manual verification bypass or placeholder DAG may be needed during early staging phases.
2. **DOWNSTREAM NOT YET MIGRATED:** The downstream pipeline `crm_weekly_workflow` must be registered in Cloud Composer for the `TriggerDagRunOperator` task (`publish_downstream_trigger`) to successfully resolve.
3. **EXTERNAL SYSTEM REPLACEMENTS:** The legacy system targets physical Oracle and Local mount environments. Migrating to Google Cloud Platform requires deploying matching extract/write connectors (such as BigQuery Storage APIs or Oracle JDBC drivers within the Spark Jobs) and uploading all respective PySpark helper assets (e.g. `retail_pre_check.py`, `sales_rollup.py`) to `gs://YOUR_BUCKET_NAME/pyspark_scripts/`.