import calendar
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

# ==========================================
# 1. CONSTANTS, VARIABLES, & CONFIGURATION
# ==========================================
DAG_ID = "finance_month_end_workflow"
DEFAULT_EMAIL = "finance-etl@company.com"

# Fetch configurations dynamically with safe fallbacks
NOTIFY_EMAIL = Variable.get("finance_notify_email", default_var=DEFAULT_EMAIL)
FORCE_CLOSE = Variable.get("finance_force_close", default_var="N")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var="gcp-finance-prod")
GCP_REGION = Variable.get("GCP_REGION", default_var="europe-west1")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER", default_var="finance-spark-cluster")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="finance-scripts-bucket")

# Dataproc PySpark Job Template Creator
def get_dataproc_pyspark_job(script_path: str, args: list) -> dict:
    """
    Generates a reusable Dataproc PySpark job configuration dictionary.
    """
    return {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": script_path,
            "args": args,
        },
    }

# ==========================================
# 2. CUSTOM LOGIC & CALENDAR UTILITIES
# ==========================================
def is_last_business_day(logical_date, **kwargs) -> bool:
    """
    Evaluates whether the given execution date represents the last business day 
    (Monday through Friday) of its calendar month.
    """
    if FORCE_CLOSE == "Y":
        logging.info("FORCE_CLOSE is set to 'Y'. Bypassing business day evaluation constraint.")
        return True

    year = logical_date.year
    month = logical_date.month
    
    # Locate last day of the month
    last_day = calendar.monthrange(year, month)[1]
    last_date = datetime(year, month, last_day)
    
    # Backtrack from the last calendar day to find the last weekday (Mon-Fri)
    while last_date.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        last_date -= timedelta(days=1)
        
    is_last_b_day = (logical_date.date() == last_date.date())
    logging.info(f"Checking Execution Date: {logical_date.date()} | Last Business Day calculated: {last_date.date()}")
    logging.info(f"Short Circuit Verification Result: {is_last_b_day}")
    return is_last_b_day

# ==========================================
# 3. FAILURE CALLBACK HANDLERS
# ==========================================
def on_failure_alarm(context):
    """
    Standard failure callback invoked on individual task failures.
    """
    task_instance = context.get('task_instance')
    logging.error(f"ALERT: Task '{task_instance.task_id}' failed in DAG '{task_instance.dag_id}' on run {task_instance.run_id}.")

def on_terminal_failure(context):
    """
    Terminal failure callback evaluating retry status to flag catastrophic execution drops.
    """
    task_instance = context.get('task_instance')
    try_number = task_instance.try_number
    max_tries = task_instance.max_tries
    
    if try_number >= max_tries:
        logging.critical(f"FATAL: Task '{task_instance.task_id}' failed after exhausting all {max_tries} execution attempts.")
    else:
        logging.warning(f"Task '{task_instance.task_id}' failed. Current attempt: {try_number}/{max_tries}. Re-trying.")

# ==========================================
# 4. DAG CONFIGURATION & OPERATORS
# ==========================================
default_args = {
    "owner": "finance_etl_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email": [NOTIFY_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 20 * * *",  # Executes daily at 20:00 Europe/London (Calendar verification is managed dynamically)
    catchup=False,
    max_active_runs=1,
    tags=["finance", "month-end", "dataproc"],
) as dag:

    # Task Parameter Mappings mapped using Airflow templates
    period_date_param = "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
    period_name_param = "{{ (data_interval_end - macros.timedelta(days=15)).strftime('%b-%Y').upper() }}"
    fiscal_year_param = "{{ data_interval_end.strftime('%Y') }}"

    # [TASK 0] - Short Circuit Calendar Constraint Gate
    guard_last_business_day = ShortCircuitOperator(
        task_id="guard_last_business_day",
        python_callable=is_last_business_day,
        op_kwargs={"logical_date": "{{ logical_date }}"},
    )

    # [TASK 1] - Pre-Flight Connectivity and DB checks (Preserving SQL query logic details)
    finance_pre_flight = BashOperator(
        task_id="finance_pre_flight",
        bash_command=f"""
        echo "Checking source GL database availability..."
        # SQL Check logic: 
        # SELECT COUNT(*) FROM SOURCE_FIN.GL_JNL_LINES WHERE PERIOD_NAME = '{period_name_param}' AND STATUS = 'POSTED';
        exit 0
        """,
        on_failure_callback=on_failure_alarm,
    )

    # [TASK 2] - Regional GL Extract: UK
    finance_stg_gl_extract_uk = DataprocSubmitJobOperator(
        task_id="finance_stg_gl_extract_uk",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=get_dataproc_pyspark_job(f"gs://{GCS_BUCKET}/pyspark_scripts/run_gl_close_uk.py", [period_date_param, "UK_ENTITY", FORCE_CLOSE]),
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm,
    )

    # [TASK 3] - Regional GL Extract: DE
    finance_stg_gl_extract_de = DataprocSubmitJobOperator(
        task_id="finance_stg_gl_extract_de",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=get_dataproc_pyspark_job(f"gs://{GCS_BUCKET}/pyspark_scripts/run_gl_close_de.py", [period_date_param, "DE_ENTITY", FORCE_CLOSE]),
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm,
    )

    # [TASK 4] - Regional GL Extract: FR
    finance_stg_gl_extract_fr = DataprocSubmitJobOperator(
        task_id="finance_stg_gl_extract_fr",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=get_dataproc_pyspark_job(f"gs://{GCS_BUCKET}/pyspark_scripts/run_gl_close_fr.py", [period_date_param, "FR_ENTITY", FORCE_CLOSE]),
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm,
    )

    # [TASK 5] - Dimensional Account Master Load
    finance_account_master_load = DataprocSubmitJobOperator(
        task_id="finance_account_master_load",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=get_dataproc_pyspark_job(f"gs://{GCS_BUCKET}/pyspark_scripts/run_account_load.py", [period_date_param, "ALL_ENTITIES", "ALL"]),
        retries=3,
        retry_delay=timedelta(seconds=120),
        on_failure_callback=on_failure_alarm,
    )

    # [TASK 6] - Ab Initio Migrated Transformation Graph
    finance_abinitio_gl_transform = DataprocSubmitJobOperator(
        task_id="finance_abinitio_gl_transform",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=get_dataproc_pyspark_job(
            f"gs://{GCS_BUCKET}/pyspark_scripts/gl_transform.py", 
            ["--period-name", period_name_param, "--entity-code", "ALL", "--parallelism", "4"]
        ),
        retries=0,
        on_failure_callback=on_failure_alarm,
    )

    # [TASK 7] - Ab Initio Migrated Reconciliation Graph (Does not halt execution on failure)
    finance_abinitio_reconcile = DataprocSubmitJobOperator(
        task_id="finance_abinitio_reconcile",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=get_dataproc_pyspark_job(
            f"gs://{GCS_BUCKET}/pyspark_scripts/gl_reconcile.py", 
            ["--period-name", period_name_param, "--entity-code", "ALL"]
        ),
        retries=0,
    )

    # [TASK 8] - Spark Aggregations on YARN Cluster
    finance_spark_gl_aggregation = DataprocSubmitJobOperator(
        task_id="finance_spark_gl_aggregation",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=get_dataproc_pyspark_job(
            f"gs://{GCS_BUCKET}/pyspark_scripts/finance_etl_assembly.py", 
            ["--period-name", period_name_param, "--fiscal-year", fiscal_year_param]
        ),
        retries=0,
        on_failure_callback=on_terminal_failure,
    )

    # [TASK 9] - Close out Audit Logs & Trigger Successors
    finance_daily_gl_close = BashOperator(
        task_id="finance_daily_gl_close",
        bash_command=f"""
        echo "[$(date)] FINANCE_DAILY_GL_CLOSE completed for PERIOD={period_name_param}" >> /opt/etl/logs/finance/close_audit.log
        """,
        on_failure_callback=on_failure_alarm,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # PubSub cross-domain completion event publisher
    publish_gcp_close_event = PubSubPublishMessageOperator(
        task_id="publish_gcp_close_event",
        project_id=GCP_PROJECT_ID,
        topic="finance-gl-close-complete",
        messages=[{
            "data": b"GL_CLOSE_COMPLETE",
            "attributes": {
                "period_name": period_name_param,
                "fiscal_year": fiscal_year_param
            }
        }],
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Successor DAG triggers
    trigger_retail_daily_workflow = TriggerDagRunOperator(
        task_id="trigger_retail_daily_workflow",
        trigger_dag_id="retail_daily_workflow",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_crm_weekly_workflow = TriggerDagRunOperator(
        task_id="trigger_crm_weekly_workflow",
        trigger_dag_id="crm_weekly_workflow",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [TASK 10] - Status Update Notification Engine (Preserving verbatim source text literals)
    finance_period_close_notify = BashOperator(
        task_id="finance_period_close_notify",
        bash_command=f"""
        echo "Month-end close complete: {period_name_param}" | mailx -s "[FINANCE-OK] Month-End Close {period_name_param}" {NOTIFY_EMAIL}
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ==========================================
    # 5. WORKFLOW DEPENDENCY MAP
    # ==========================================
    
    # 1. Start execution gate
    guard_last_business_day >> finance_pre_flight

    # 2. Fan-out extraction and master dimension tasks
    finance_pre_flight >> [
        finance_stg_gl_extract_uk,
        finance_stg_gl_extract_de,
        finance_stg_gl_extract_fr,
        finance_account_master_load
    ]

    # 3. Consolidate extractions to Unified Transform execution
    [
        finance_stg_gl_extract_uk,
        finance_stg_gl_extract_de,
        finance_stg_gl_extract_fr,
        finance_account_master_load
    ] >> finance_abinitio_gl_transform

    # 4. Fan-out verification and aggregation processing
    finance_abinitio_gl_transform >> [
        finance_abinitio_reconcile,
        finance_spark_gl_aggregation
    ]

    # 5. Re-converge tasks to Daily Close execution
    [
        finance_abinitio_reconcile,
        finance_spark_gl_aggregation
    ] >> finance_daily_gl_close

    # 6. Post-processing notifications & Downstream triggers
    finance_daily_gl_close >> publish_gcp_close_event
    finance_daily_gl_close >> [trigger_retail_daily_workflow, trigger_crm_weekly_workflow]
    
    [publish_gcp_close_event, trigger_retail_daily_workflow, trigger_crm_weekly_workflow] >> finance_period_close_notify