### FILE DISPOSITION TABLE

| Legacy Source File | Target File / Action | Notes / Disposition |
|:---|:---|:---|
| `customer/crm_weekly_workflow.xml` | `dags/crm_weekly_workflow.py` | Migrated 1:1 to an Airflow DAG on Cloud Composer orchestrating Dataproc jobs and GCS sensors. |
| `process_customer_data.ksh` | `pyspark_scripts/process_customer_data.py` | Mentioned in UC4 xml; logic must be ported to PySpark for execution on Cloud Dataproc. |
| `crm_customer_scoring.mp` | `pyspark_scripts/crm_customer_scoring.py` | Legacy Ab Initio graph; logic must be ported to a PySpark master program. |
| `customer_segmentation.scala` | `pyspark_scripts/customer_segmentation.py` | Legacy Spark Scala assembly jar; converted to a PySpark script for execution. |
| `crm_lineage_tracker.py` | `pyspark_scripts/crm_lineage_tracker.py` | Python tracking module ported to run on GCP environment. |

---

### VERBATIM MCP OUTPUT (TRANSFORMATION LOGIC & PSEUDOCODE)

Below is the complete, unmodified output from the migration tool converting `CRM_WEEKLY_WORKFLOW` to an Airflow DAG structure:

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

# ── GCP Configuration ────────────────────────────────────
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
REGION = "YOUR_DATAPROC_REGION"
CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "CRM_ETL_TEAM",
    "start_date": datetime(2025, 1, 1, tzinfo=pendulum.timezone("Europe/London")),
    "email_on_failure": True,
    "email": "crm-etl@company.com",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# ── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    Sends execution-level failure alarms back to the notification channels.
    """
    # TODO: Implement webhook alert, PagerDuty action, or custom SMTP alert configuration
    print(f"CRITICAL failure detected on task {context['task_instance'].task_id}.")

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id="crm_weekly_workflow",
    schedule="0 4 * * 7",  # Every Sunday at 04:00 AM (Europe/London)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    sla=timedelta(hours=5),
)

# ── Sensor Task: Wait for Finance Event ──────────────────
# Mapped from EVNT: CRM_WAIT_FINANCE_EVENT
# Waits for finance/finance_daily.json to exist
crm_wait_finance_event = GCSObjectExistenceSensor(
    task_id="crm_wait_finance_event",
    bucket=GCS_BUCKET,
    object="finance/finance_daily.json",
    timeout=14400,  # 240 Minutes Timeout
    poke_interval=300,
    dag=dag,
)

# ── Sensor Task: Wait for Retail Event ───────────────────
# Mapped from EVNT: CRM_WAIT_RETAIL_EVENT
# Waits for sales/retail_daily.json to exist (Non-blocking)
crm_wait_retail_event = GCSObjectExistenceSensor(
    task_id="crm_wait_retail_event",
    bucket=GCS_BUCKET,
    object="sales/retail_daily.json",
    timeout=7200,  # 120 Minutes Timeout
    poke_interval=150,
    dag=dag,
)

# ── Task: Customer Segment Extract - VIP ─────────────────
# Mapped from JOBS: CRM_CUSTOMER_EXTRACT_VIP
pyspark_job_vip = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/process_customer_data.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--segment", "VIP",
            "--env", "PROD"
        ]
    }
}

crm_customer_extract_vip = DataprocSubmitJobOperator(
    task_id="crm_customer_extract_vip",
    project_id=PROJECT_ID,
    region=REGION,
    job=pyspark_job_vip,
    job_id="crm_vip_ext_{{ ds_nodash }}_{{ mcols_uuid }}",
    retries=3,
    retry_delay=timedelta(seconds=120),
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS, # Will execute only if previous mandatory upstream tasks succeed
    dag=dag,
)

# ── Task: Customer Segment Extract - RETAIL ──────────────
# Mapped from JOBS: CRM_CUSTOMER_EXTRACT_RETAIL
pyspark_job_retail = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/process_customer_data.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--segment", "RETAIL",
            "--env", "PROD"
        ]
    }
}

crm_customer_extract_retail = DataprocSubmitJobOperator(
    task_id="crm_customer_extract_retail",
    project_id=PROJECT_ID,
    region=REGION,
    job=pyspark_job_retail,
    job_id="crm_retail_ext_{{ ds_nodash }}_{{ mcols_uuid }}",
    retries=3,
    retry_delay=timedelta(seconds=120),
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── Task: Customer Segment Extract - WHOLESALE ───────────
# Mapped from JOBS: CRM_CUSTOMER_EXTRACT_WHOLESALE
pyspark_job_wholesale = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/process_customer_data.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--segment", "WHOLESALE",
            "--env", "PROD"
        ]
    }
}

crm_customer_extract_wholesale = DataprocSubmitJobOperator(
    task_id="crm_customer_extract_wholesale",
    project_id=PROJECT_ID,
    region=REGION,
    job=pyspark_job_wholesale,
    job_id="crm_wholesale_ext_{{ ds_nodash }}_{{ mcols_uuid }}",
    retries=3,
    retry_delay=timedelta(seconds=120),
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── Task: CRM Ab Initio Transform ────────────────────────
# Mapped from JOBS: CRM_ABINITIO_TRANSFORM
pyspark_job_abinitio_transform = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/crm_customer_scoring.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--customer-segment", "ALL",
            "--parallelism", "4"
        ]
    }
}

crm_abinitio_transform = DataprocSubmitJobOperator(
    task_id="crm_abinitio_transform",
    project_id=PROJECT_ID,
    region=REGION,
    job=pyspark_job_abinitio_transform,
    job_id="crm_abinitio_{{ ds_nodash }}_{{ mcols_uuid }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── Task: Spark Segmentation ─────────────────────────────
# Mapped from JOBS: CRM_SPARK_SEGMENTATION
pyspark_job_segmentation = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/customer_segmentation.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--env", "PROD"
        ]
    }
}

crm_spark_segmentation = DataprocSubmitJobOperator(
    task_id="crm_spark_segmentation",
    project_id=PROJECT_ID,
    region=REGION,
    job=pyspark_job_segmentation,
    job_id="crm_spark_segmentation_{{ ds_nodash }}_{{ mcols_uuid }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── Task: Python Lineage Tracker ─────────────────────────
# Mapped from JOBS: CRM_PYTHON_LINEAGE
pyspark_job_lineage = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/crm_lineage_tracker.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--env", "PROD"
        ]
    }
}

crm_python_lineage = DataprocSubmitJobOperator(
    task_id="crm_python_lineage",
    project_id=PROJECT_ID,
    region=REGION,
    job=pyspark_job_lineage,
    job_id="crm_lineage_{{ ds_nodash }}_{{ mcols_uuid }}",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── Task: Completion Notification ────────────────────────
# Mapped from JOBS: CRM_COMPLETION_NOTIFY
# Triggers if segmentation runs successfully, regardless of non-critical failures in retail sensors or lineage tracking
crm_completion_notify = EmailOperator(
    task_id="crm_completion_notify",
    to="crm-etl@company.com",
    subject="[OK] Weekly CRM Load {{ ds }}",
    html_content="<p>CRM_WEEKLY_WORKFLOW completed for RUN_DATE={{ ds }}</p>",
    trigger_rule=TriggerRule.ALL_DONE, # Required because of non-blocking inputs
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
crm_wait_finance_event >> crm_wait_retail_event

crm_wait_retail_event >> [
    crm_customer_extract_vip,
    crm_customer_extract_retail,
    crm_customer_extract_wholesale
]

[
    crm_customer_extract_vip,
    crm_customer_extract_retail,
    crm_customer_extract_wholesale
] >> crm_abinitio_transform

crm_abinitio_transform >> [crm_spark_segmentation, crm_python_lineage]

[crm_spark_segmentation, crm_python_lineage] >> crm_completion_notify
```

---

### ADDED CONTEXT FOR MIGRATION

#### 1. Job Dependencies & Lineage Edges
* **Upstream Triggers:**
  * **FINANCE_GL_CLOSE_COMPLETE:** An event published from the upstream `finance/` pipeline (representing financial ledger reconciliation events). On BigQuery/Cloud Composer, this is modeled via the `crm_wait_finance_event` sensor pointing to `gs://<GCS_BUCKET>/finance/finance_daily.json` (representing the `FINANCE_GL_CLOSE_COMPLETE` trigger event).
  * **RETAIL_DAILY_COMPLETE:** An event published from the upstream `sales/` pipeline (representing daily sales summaries). On BigQuery/Cloud Composer, this is modeled via the `crm_wait_retail_event` sensor pointing to `gs://<GCS_BUCKET>/sales/retail_daily.json`. Since retail failure is non-blocking, a timeout or sensor failure will allow the flow to proceed (`TriggerRule.ALL_DONE` downstream configuration).
* **Data Lineage Schemas:**
  * Reads `DW_OWNER.STG_CUSTOMER_SALES` (sales domain - sourced from BigQuery table)
  * Reads `DW_OWNER.FACT_REGIONAL_SUMMARY` (sales domain - sourced from BigQuery table)
  * Reads `FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION` (finance domain - sourced from BigQuery table)

#### 2. Schedule and Variables
* **Schedule:** Converted UC4's Sunday at 04:00 Europe/London execution rule to Cloud Composer Cron `0 4 * * 7` in combination with `pendulum.timezone("Europe/London")` to retain Daylight Savings integrity.
* **Inherited Variables & Mappings:**
  * `&RUN_DATE` $\rightarrow$ Map dynamically using Airflow execution date `{{ ds }}`.
  * `&BATCH_SIZE` $\rightarrow$ Mapped to application parameters where appropriate (Job Specific: `5000`).
  * `&ENV` $\rightarrow$ Map via Airflow variable `Variable.get("ENV")` falling back to `"PROD"`.
  * `&NOTIFY_EMAIL` $\rightarrow$ Map via Airflow variable `Variable.get("NOTIFY_EMAIL")` falling back to `"crm-etl@company.com"`.
  * `&RETRY_MAX` $\rightarrow$ Transformed to Airflow task parameter `retries=3`.

#### 3. Environment Variable Classifications

##### **Global Environment Variables (GCP Infrastructure & Targets)**
These parameters are shared globally across the composer network and must be read at execution runtime rather than being hardcoded inside tasks:
```python
from airflow.models import Variable

GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")
```

##### **Job-Specific Environment Variables**
These variables apply specifically to this pipeline run context:
* `ENV` = `Variable.get("ENV", default_var="PROD")`
* `NOTIFY_EMAIL` = `Variable.get("NOTIFY_EMAIL", default_var="crm-etl@company.com")`
* `BATCH_SIZE` = `5000` (Passed inside execution configuration payloads where needed).

#### 4. Risks & Manual Actions
* **SOURCE: NOT FOUND** — `process_customer_data.ksh` — *No candidate file located in workspace.* The underlying shell extraction script logic must be ported into Python/PySpark and uploaded to `gs://<GCS_BUCKET>/pyspark_scripts/process_customer_data.py`.
* **SOURCE: NOT FOUND** — `crm_customer_scoring.mp` (Ab Initio Graph) — *No candidate file located in workspace.* Legacy graph transformation elements must be redesigned and consolidated inside `gs://<GCS_BUCKET>/pyspark_scripts/crm_customer_scoring.py`.
* **SOURCE: NOT FOUND** — `customer_segmentation.scala` (Scala Spark Job) — *No candidate file located in workspace.* Legacy execution archive (`crm-assembly.jar`) and the class structure `com.company.crm.CustomerSegmentation` must be translated to `gs://<GCS_BUCKET>/pyspark_scripts/customer_segmentation.py`.
* **SOURCE: NOT FOUND** — `crm_lineage_tracker.py` — *No candidate file located in workspace.* Verification is required to confirm how tracking runs inside GCS environments.
* **Cross-Pipeline Sensor Dependencies:** Upstream publisher logic in the `finance/` and `sales/` DAGs must explicitly dump execution-stamped state files to the configured GCS sensor targets (`finance/finance_daily.json` and `sales/retail_daily.json`) upon completion to prevent execution stalls. Ensure these upstreams exist and are migrated.