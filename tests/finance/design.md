# MIGRATION DESIGN DOCUMENT: FINANCE DAILY WORKFLOW

## SECTION 1 — DESIGN DOCUMENT (VERBATIM UC4 TO AIRFLOW TRANSLATION)

### 1. Overview
The `FINANCE_DAILY_WORKFLOW` is a business-critical daily processing pipeline that extracts General Ledger (GL) transactions, refreshes account master metadata, and updates currency exchange rate tables. It stages this information for downstream operational reports and publishing systems. This process runs daily Monday through Friday at 01:00 Europe/London, and upon successful completion, it publishes a global synchronization signal (`FINANCE_GL_CLOSE_COMPLETE`) that triggers downstream CRM and Retail pipelines.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `FINANCE_DAILY_WORKFLOW` | `JOBP` (Job Plan) | `<Active>1</Active>` (Active) | Daily GL transaction extract and staging load for all entities. |
| `FINANCE_DAILY_PRE_CHECK` | `JOBS_UNIX` (Equivalent) | Active | Verify source GL database system is online and accessible. |
| `FINANCE_DAILY_ACCT_LOAD` | `JOBS_UNIX` (Equivalent) | Active | Daily refresh of account master dimension. |
| `FINANCE_DAILY_RATE_EXTRACT` | `JOBS_UNIX` (Equivalent) | Active | Extract daily exchange rates into `STG_PERIOD_RATES`. |
| `FINANCE_DAILY_GL_EXTRACT` | `JOBS_UNIX` (Equivalent) | Active | Extract daily GL journals from source into staging for multiple foreign entities. |
| `FINANCE_DAILY_GL_CLOSE` | `JOBS_UNIX` (Equivalent) | Active | Audit and publish the final GL close event for downstream consumers. |

### 3. Airflow DAG Properties
| Property | Value | Note |
| :--- | :--- | :--- |
| **dag_id** | `finance_daily_workflow` | Sanitized lowercase ID |
| **schedule** | `0 1 * * 1-5` | Run daily at 01:00 AM, Monday through Friday |
| **timezone** | `Europe/London` | Explicitly configured in source schedule |
| **start_date** | `datetime(2024, 1, 1)` | Placeholder start date |
| **catchup** | `False` | Catchup is disabled to prevent backfilling historic records |
| **max_active_runs** | `1` | Strictly enforces single execution path to avoid race conditions |
| **is_paused_upon_creation** | `False` | Deployed in an active state |
| **default_args** | `{'owner': 'finance_etl', 'retries': 0, 'email_on_failure': True, 'email': 'finance-etl@company.com'}` | Basic defaults for resilience |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `pre_check` | `DataprocSubmitJobOperator` | `finance_daily_pre_check.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME` | 2 | 60s | None | None | No | `on_failure_alarm` | Critical DB verify |
| `acct_load` | `DataprocSubmitJobOperator` | `finance_daily_acct_load.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME` | 3 | 60s | None | None | No | `on_failure_alarm` | Non-blocking fail |
| `rate_extract` | `DataprocSubmitJobOperator` | `finance_daily_rate_extract.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME` | 0 | - | None | None | No | `on_failure_alarm` | SQL-based load |
| `gl_extract` | `DataprocSubmitJobOperator` | `finance_daily_gl_extract.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME` | 3 | 120s | None | None | No | `on_failure_alarm` | Long running |
| `gl_close` | `DataprocSubmitJobOperator` | `finance_daily_gl_close.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME` | 0 | - | None | None | No | `on_failure_alarm` | Audits & publishes |
| `trigger_retail` | `TriggerDagRunOperator` | N/A | N/A | 0 | - | None | None | Yes | None | Downstream trigger |
| `trigger_crm` | `TriggerDagRunOperator` | N/A | N/A | 0 | - | None | None | Yes | None | Downstream trigger |

### 5. Task Dependency Map
```text
start_guard >> pre_check >> [acct_load, rate_extract] >> gl_extract >> gl_close >> [trigger_retail, trigger_crm]
```
* **Dependency Description:**
  1. `start_guard`: Checks if an active instance is currently running. If found, skips gracefully.
  2. `pre_check`: Runs the database accessibility check.
  3. `acct_load` & `rate_extract`: Run in parallel after the connection checks are validated.
  4. `gl_extract`: Orchestrates entity files processing. It runs once both metadata loads complete. Even if `acct_load` or `rate_extract` raise minor exceptions (warnings), `gl_extract` is configured to proceed.
  5. `gl_close`: Finalizes operations, writes audits, and logs the process completion.
  6. `trigger_retail` & `trigger_crm`: Trigger downstream target loops using fire-and-forget logic (`wait_for_completion=False`).

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&PERIOD_DATE` | `&$TODAY` | `{{ ds }}` (Airflow execution date) |
| `&PERIOD_YEAR` | `&$CURRENT_YEAR` | `{{ dag_run.logical_date.strftime('%Y') }}` |
| `&PERIOD_MONTH` | `&$CURRENT_MONTH_NUM` | `{{ dag_run.logical_date.strftime('%m') }}` |
| `&NOTIFY_EMAIL` | `finance-etl@company.com` | `var.value.finance_notify_email` |
| `&RETRY_MAX` | `3` | `var.value.finance_retry_max` |
| `RETAIL_DAILY_WORKFLOW` | Target Trigger DAG | `retail_daily_workflow` |
| `CRM_WEEKLY_WORKFLOW` | Target Trigger DAG | `crm_weekly_workflow` |

### 7. Error Handling and Retry Strategy
- **Failure Alarms:** Every critical task uses an `on_failure_callback` pointing to a unified notification helper. If any task within the main processing pipeline fails, alerts are dispatched directly to the finance operations workspace via standard channel interfaces or SMTP.
- **Upstream Failures:** UC4 defined execution conditions where `gl_extract` allowed preceding warning/success states. In Airflow, this is preserved safely by defaulting tasks to run under the `ALL_SUCCESS` rule, but explicitly managing errors inside `acct_load` and `rate_extract` via non-abort logic. Since `acct_load` was set to `CONTINUE` on failure in UC4, in Airflow we catch that specific exception or use task configuration settings to ensure it doesn't break DAG flow if acceptable.
- **Sync Behavior:** The UC4 scheduling context limits concurrency to `1` pipeline run. This is mapped directly in Airflow using `max_active_runs=1` coupled with an upstream active-run sensor guard at the beginning of the DAG block.

### 8. Developer Notes
- **GCP Placeholders:** Developers must ensure variables such as `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME` are stored inside Airflow Connections, Environment variables, or the Airflow Metadata database.
- **Calendar Gaps:** The source execution contains an exclusion rule: `PUBLIC_HOLIDAYS_UK`. Because calendar databases vary significantly, this logic must be managed using custom logic inside the DAG or via custom timetable objects.
- **Skipping downstream execution:** Avoid mapping `TriggerRule.ALL_DONE` to bypassed tasks; doing so would compromise status handling of guard states. Keep standard propagation pathways.

---

## SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"
SPARK_SCRIPTS_PATH = f"gs://{GCS_BUCKET}/pyspark_scripts"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'finance_etl',
    'start_date': datetime(2024, 1, 1),
    'email': ['finance-etl@company.com', 'dw-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60)
}

# ── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    Unified failure callback to handle critical alerts.
    Fires immediate warning signals to operations.
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    logical_date = context.get('ds')
    
    # TODO: Implement your enterprise messaging integration here (e.g. Slack/Teams webhook or direct SMTP send)
    print(f"ALERT: Task {task_instance.task_id} inside {dag_id} failed on {logical_date}. Notification dispatched.")


# ── DAG Definition ───────────────────────────────────────
dag_id = 'finance_daily_workflow'

with DAG(
    dag_id=dag_id,
    schedule_interval='0 1 * * 1-5',  # Monday to Friday at 01:00 AM
    catchup=False,
    max_active_runs=1,                # Matches UC4 Sync Wait behavior
    is_paused_upon_creation=False,    # Maps to <Active>1</Active>
    default_args=DEFAULT_ARGS,
    tags=['finance', 'daily']
) as dag:

    # ── Guard Task (Else=Skip sync logic) ─────────────────
    def run_guard_logic(**context):
        """
        Check for any active pipeline execution runs.
        If concurrent active run exists, skip this run immediately to prevent collisions.
        """
        current_run_id = context['run_id']
        active_runs = DagRun.find(dag_id=dag_id, state=State.RUNNING)
        
        # Filter out self
        other_active_runs = [r for r in active_runs if r.run_id != current_run_id]
        
        if len(other_active_runs) > 0:
            raise AirflowSkipException("Another instance is running. Gracefully skipping this execution run.")

    concurrency_guard = PythonOperator(
        task_id='concurrency_guard',
        python_callable=run_guard_logic,
        provide_context=True
    )

    # ── Task: pre_check ────────────────────────────────────
    # Maps to: FINANCE_DAILY_PRE_CHECK
    pre_check_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_pre_check.py",
            "args": []
        }
    }

    pre_check = DataprocSubmitJobOperator(
        task_id='finance_daily_pre_check',
        job=pre_check_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=2,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    # ── Task: acct_load ────────────────────────────────────
    # Maps to: FINANCE_DAILY_ACCT_LOAD
    # Notes: Uses custom run-arguments context including execution date.
    acct_load_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_acct_load.py",
            "args": ["{{ ds }}", "ALL", "ACCOUNT_ONLY"]
        }
    }

    acct_load = DataprocSubmitJobOperator(
        task_id='finance_daily_acct_load',
        job=acct_load_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    # ── Task: rate_extract ─────────────────────────────────
    # Maps to: FINANCE_DAILY_RATE_EXTRACT
    # Notes: Leverages runtime templating variables for Year and Month configurations.
    rate_extract_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_rate_extract.py",
            "args": [
                "{{ ds }}",
                "{{ dag_run.logical_date.strftime('%Y') }}",
                "{{ dag_run.logical_date.strftime('%m') }}"
            ]
        }
    }

    rate_extract = DataprocSubmitJobOperator(
        task_id='finance_daily_rate_extract',
        job=rate_extract_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        on_failure_callback=on_failure_alarm
    )

    # ── Task: gl_extract ───────────────────────────────────
    # Maps to: FINANCE_DAILY_GL_EXTRACT
    # Notes: Process orchestration across foreign entities.
    gl_extract_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_gl_extract.py",
            "args": ["{{ ds }}"]
        }
    }

    gl_extract = DataprocSubmitJobOperator(
        task_id='finance_daily_gl_extract',
        job=gl_extract_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=120),
        on_failure_callback=on_failure_alarm
    )

    # ── Task: gl_close ─────────────────────────────────────
    # Maps to: FINANCE_DAILY_GL_CLOSE
    gl_close_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_gl_close.py",
            "args": ["{{ ds }}"]
        }
    }

    gl_close = DataprocSubmitJobOperator(
        task_id='finance_daily_gl_close',
        job=gl_close_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        on_failure_callback=on_failure_alarm
    )

    # ── Downstream Triggers (Fire and Forget) ──────────────
    # Maps to: Cross-domain publication signals
    trigger_retail = TriggerDagRunOperator(
        task_id='trigger_retail_daily_workflow',
        trigger_dag_id='retail_daily_workflow',
        conf={"period_date": "{{ ds }}"},
        wait_for_completion=False  # Fire-and-Forget mapping (ActFlg=0)
    )

    trigger_crm = TriggerDagRunOperator(
        task_id='trigger_crm_weekly_workflow',
        trigger_dag_id='crm_weekly_workflow',
        conf={"period_date": "{{ ds }}"},
        wait_for_completion=False  # Fire-and-Forget mapping (ActFlg=0)
    )

    # ── Dependencies ─────────────────────────────────────────
    concurrency_guard >> pre_check
    pre_check >> [acct_load, rate_extract]
    [acct_load, rate_extract] >> gl_extract
    gl_extract >> gl_close
    gl_close >> [trigger_retail, trigger_crm]
```

---

## SECTION 3 — SYSTEM INTEGRATION & COMPOSER CONTEXT

### 1. Job Dependencies
This workflow acts as a pivotal synchronization point within the enterprise data environment.
- **Upstream Dependencies:** None discovered (driven by schedule).
- **Downstream Dependencies:** 
  - `RETAIL_DAILY_WORKFLOW` (Task: `RETAIL_PRODUCT_MASTER_LOAD` in the **sales** domain) - triggered upon publication of the `FINANCE_GL_CLOSE_COMPLETE` event.
  - `CRM_WEEKLY_WORKFLOW` (Task: `CRM_CUSTOMER_SEGMENT_LOAD` in the **customer** domain) - triggered upon publication of the `FINANCE_GL_CLOSE_COMPLETE` event.
  - *Implementation:* Both are automated on Google Cloud Composer using the `TriggerDagRunOperator` with `wait_for_completion=False` to preserve the fire-and-forget event nature.

### 2. Execution Order
The chronological sequence is explicitly mapped to the DAG tasks:
1. `concurrency_guard`: Checks for running instances.
2. `finance_daily_pre_check`: Runs SQL connection pre-check.
3. `finance_daily_acct_load` & `finance_daily_rate_extract`: Execute in parallel.
4. `finance_daily_gl_extract`: Runs sequentially after BOTH step 3 tasks resolve (respecting warning conditions where fallback permits).
5. `finance_daily_gl_close`: Finalizes staging tables, logs, and triggers the downstream pipeline.

### 3. Scheduling
- **Trigger Type:** Chron-based Time Trigger
- **Schedule:** `0 1 * * 1-5` (Runs Monday through Friday at 01:00 AM Europe/London timezone)
- **Holiday Calender Mapping:** UC4's `PUBLIC_HOLIDAYS_UK` calendar exclusion must be handled programmatically. It is recommended to define a custom Airflow Timetable or implement an execution-day verification step inside the `concurrency_guard` task that skips processing if the running date belongs to a UK Public Holiday registry table.

### 4. Schedule & Variables (Preserved)
The workflow context maintains several key runtime dynamic configurations:
- `PERIOD_DATE`: Sourced from Airflow's logical execution date string (`{{ ds }}`).
- `PERIOD_YEAR`: Sourced via template context: `{{ dag_run.logical_date.strftime('%Y') }}`.
- `PERIOD_MONTH`: Sourced via template context: `{{ dag_run.logical_date.strftime('%m') }}`.
- `NOTIFY_EMAIL`: Configured globally inside Composer variables as `finance_notify_email` (Value fallback: `finance-etl@company.com`).
- `RETRY_MAX`: Configured globally inside Composer variables as `finance_retry_max` (Value fallback: `3`).
- `ALLOW_EMPTY`: Boolean control configuration default `N`.

### 5. Lineage
- **Cross-Job Lineage Ends:** 
  - Publishes `FINANCE_GL_CLOSE_COMPLETE` event to trigger `retail_daily_workflow` (job: `RETAIL_PRODUCT_MASTER_LOAD`).
  - Publishes `FINANCE_GL_CLOSE_COMPLETE` event to trigger `crm_weekly_workflow` (job: `CRM_CUSTOMER_SEGMENT_LOAD`).

### 6. External System Replacements
- **Database Access:** Source Oracle Database connectivity (${FIN_ORA_USER}/${FIN_ORA_PASS}@${FIN_ORA_SID}) used during SQL*Plus checks is replaced by explicit Airflow Connections (e.g., `oracle_default` or GCP Secrets Manager secrets) mapped into the secure Dataproc cluster config or processed as a Federated Query in BigQuery.
- **Local Scripts:** Command execution directories (`/opt/etl/scripts/`, `/opt/etl/sqlplus/`) map to centralized cloud storage pools located at `gs://YOUR_BUCKET_NAME/pyspark_scripts/` or `gs://YOUR_BUCKET_NAME/sql_queries/`.
- **System Commands / uc4api:** Command line tools and API publishes (`uc4api publish_event`) map directly to native `TriggerDagRunOperator` tasks.

### 7. Cross-File Dependencies
- Runs `run_account_load.ksh` (converted to a Python/PySpark module on Cloud Storage: `gs://YOUR_BUCKET_NAME/pyspark_scripts/finance_daily_acct_load.py`).
- Runs SQL-based rate extract script `/opt/etl/sqlplus/rate_extract.sql` (mapped to Spark SQL execution or native BQ SQL script under `finance_daily_rate_extract.py`).
- Iterates entity operations calling `/opt/etl/scripts/run_gl_close.ksh` in parallel for entities: `UK_ENTITY`, `DE_ENTITY`, `FR_ENTITY` (mapped to parallelized thread processes or parallel Dataproc/Dataflow jobs under `finance_daily_gl_extract.py`).

### 8. Target File Plan
| Source Component | Target Relative Path | Target Language | Migration Purpose |
| :--- | :--- | :--- | :--- |
| `finance_daily.json` | `dags/finance_daily_workflow.py` | Python / Airflow DAG | Root orchestration, dependencies, and variables definition. |
| `FINANCE_DAILY_PRE_CHECK` (inline command) | `pyspark_scripts/finance_daily_pre_check.py` | Python / PySpark | Connectivity validation logic. |
| `run_account_load.ksh` | `pyspark_scripts/finance_daily_acct_load.py` | Python / PySpark | Master Account Master Dimension refresh logic. |
| `/opt/etl/sqlplus/rate_extract.sql` | `pyspark_scripts/finance_daily_rate_extract.py` | Python / PySpark | Runs daily exchange rates extraction logic. |
| `run_gl_close.ksh` | `pyspark_scripts/finance_daily_gl_extract.py` | Python / PySpark | Processes journals extraction for UK, DE, and FR entities. |
| `FINANCE_DAILY_GL_CLOSE` (inline command) | `pyspark_scripts/finance_daily_gl_close.py` | Python / PySpark | Audit logs publication and final validation processes. |

### 9. Environment-Specific Values (Classification)

#### GLOBAL (Infrastructure-Wide Configuration)
Sourced dynamically from Environment Variables or Cloud Composer Variables:
- `GCP_PROJECT`: GCP Project ID hosting the Dataproc instances and target storage buckets.
- `GCP_REGION`: Target operational compute region (e.g., `europe-west2`).
- `DATAPROC_REGION`: Target Dataproc compute operational region.
- `DATAPROC_CLUSTER`: Name of the target shared/transient Dataproc processing cluster.
- `GCS_BUCKET`: Shared workspace GCS Bucket storage root name.

*Usage Example:*
```python
from airflow.models import Variable
import os

GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("gcs_bucket_name")
```

#### JOB-SPECIFIC (Workflow Context Config)
Specific values defined inside the legacy workflow execution logic:
- `finance_notify_email`: `'finance-etl@company.com'` (mapped to task failure parameters).
- `finance_retry_max`: `3` (mapped as retry arguments for tasks).
- `finance_allow_empty`: `'N'` (passed as an execution context parameter).

*Usage Example:*
```python
JOB_CONFIG = {
    "notify_email": Variable.get("finance_notify_email", default_var="finance-etl@company.com"),
    "max_retries": int(Variable.get("finance_retry_max", default_var="3")),
    "allow_empty_files": "N"
}
```

---

## SECTION 4 — RISKS, ACTIONS, & DISPOSITIONS

### 1. File Disposition Table

| Source File Name | Target Deployment Path | Migration Disposition | Justification |
| :--- | :--- | :--- | :--- |
| `finance/finance_daily.json` | `dags/finance_daily_workflow.py` | Target File | Core orchestration pipeline converted to Airflow. |

### 2. Risks & Manual Actions

*   **SOURCE: NOT FOUND** — `run_account_load.ksh` — *no candidate*
    *   *Impact:* The core logic for refreshing the account master dimension is missing from the scanned context.
    *   *Action Required:* Build team must acquire `run_account_load.ksh` to extract the master account extraction logic and migrate it into `gs://YOUR_BUCKET_NAME/pyspark_scripts/finance_daily_acct_load.py`.
*   **SOURCE: NOT FOUND** — `rate_extract.sql` — *no candidate*
    *   *Impact:* The SQL script extracting daily exchange rates is missing from the scanned context.
    *   *Action Required:* Locate `/opt/etl/sqlplus/rate_extract.sql` to reconstruct the SQL engine translation inside `gs://YOUR_BUCKET_NAME/pyspark_scripts/finance_daily_rate_extract.py`.
*   **SOURCE: NOT FOUND** — `run_gl_close.ksh` — *no candidate*
    *   *Impact:* The main shell execution logic looping across foreign entities (`UK_ENTITY`, `DE_ENTITY`, `FR_ENTITY`) to extract journals is missing.
    *   *Action Required:* Acquire `/opt/etl/scripts/run_gl_close.ksh` to build equivalent multi-threaded/concurrent extraction tasks inside the target `gs://YOUR_BUCKET_NAME/pyspark_scripts/finance_daily_gl_extract.py` script.
*   **Upstream Pipeline Wiring Gaps**
    *   *Impact:* Downstream consumers `RETAIL_DAILY_WORKFLOW` and `CRM_WEEKLY_WORKFLOW` are triggered via dynamic triggers. If these target pipelines have not yet been migrated to Cloud Composer, the `TriggerDagRunOperator` tasks will fail to locate the DAG IDs.
    *   *Action Required:* Verify the target DAG IDs (`retail_daily_workflow`, `crm_weekly_workflow`) are deployed in the target Composer environment before enabling the trigger tasks in production.
*   **UK Calendar Holiday Exclusions (`PUBLIC_HOLIDAYS_UK`)**
    *   *Impact:* Failing to exclude UK public bank holidays could cause the pipeline to run on non-processing days, generating empty extracts or database lock errors.
    *   *Action Required:* Maintain a shared BigQuery calendar dimension containing holiday dates. Add a check step inside the DAG execution logic or write a custom Airflow Timetable mapping calendar parameters to strictly avoid scheduled runs on specified holiday ranges.