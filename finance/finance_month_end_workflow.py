# ── Imports ──────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd

# ── GCP Configuration ────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")
PYSPARK_PATH = f"{GCS_BUCKET}/pyspark_scripts"

# ── default_args ─────────────────────────────────────────
default_args = {
    'owner': 'finance_etl_team',
    'depends_on_past': False, 
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    Standard Alert Callback: Triggers immediate alert on task failure.
    """
    task_id = context['task_instance'].task_id
    error_msg = f"[CRITICAL] task {task_id} failed in month-end execution."
    print(error_msg)

def on_failure_alarm_warning(context):
    """
    Warning Alert Callback: Sends alerts but does not block execution.
    """
    task_id = context['task_instance'].task_id
    print(f"[WARNING] Task {task_id} failed, but workflow configured to bypass block.")

def on_terminal_failure(context):
    """
    Terminal Failure Callback: Executes when all retries are completely exhausted.
    """
    if context['ti'].try_number >= context['ti'].max_tries:
        print("[CRITICAL] Task completely exhausted retries. Aborting run.")


# ── DAG Definition ───────────────────────────────────────
dag_id = "finance_month_end_workflow"

with DAG(
    dag_id=dag_id,
    schedule="0 20 28-31 * *",  # Triggers late-month days; filtered via ShortCircuit
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    template_searchpath="/opt/airflow/sql"
) as dag:

    # ── Guard / Calendar Check Task ──────────────────────────
    def check_last_business_day(**context):
        """
        Determines whether the execution date is the last business day of the month.
        """
        exec_date = context['execution_date']
        end_of_month_range = pd.date_range(start=exec_date, end=exec_date + timedelta(days=5), freq='B')
        
        if end_of_month_range[1].month != exec_date.month:
            print("Today is verified as the last business day of the month. Proceeding.")
            return True
        else:
            raise AirflowSkipException("Skipping: Today is not the last business day of the month.")

    is_last_business_day = ShortCircuitOperator(
        task_id="is_last_business_day",
        python_callable=check_last_business_day,
        provide_context=True,
    )

    # ── Task: pre_flight ─────────────────────────────────────
    pre_flight = OracleOperator(
        task_id="pre_flight",
        oracle_conn_id="oracle_finance_conn",
        sql="""
            SELECT COUNT(*) FROM SOURCE_FIN.GL_JNL_LINES
            WHERE PERIOD_NAME = '{{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%b-%Y').upper() }}' 
            AND STATUS = 'POSTED';
        """,
        on_failure_callback=on_failure_alarm
    )

    # ── Task: account_master_load ────────────────────────────
    account_load_args = [
        "{{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
        "ALL_ENTITIES",
        "ALL"
    ]
    
    account_master_load = DataprocSubmitJobOperator(
        task_id="account_master_load",
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        job={
            "reference": {"project_id": GCP_PROJECT},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/run_account_load.py",
                "args": account_load_args,
            },
        },
        retries=3,
        retry_delay=timedelta(seconds=120),
        on_failure_callback=on_failure_alarm
    )

    # ── Tasks: STG_GL_EXTRACTS (UK, DE, FR Parallel Processing) ──
    force_close_val = Variable.get("finance_force_close", default_var="N")
    period_date_val = "{{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

    # UK Extract
    stg_gl_extract_uk = DataprocSubmitJobOperator(
        task_id="stg_gl_extract_uk",
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        job={
            "reference": {"project_id": GCP_PROJECT},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/run_gl_close_uk.py",
                "args": [period_date_val, "UK_ENTITY", force_close_val],
            },
        },
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    # DE Extract
    stg_gl_extract_de = DataprocSubmitJobOperator(
        task_id="stg_gl_extract_de",
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        job={
            "reference": {"project_id": GCP_PROJECT},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/run_gl_close_de.py",
                "args": [period_date_val, "DE_ENTITY", force_close_val],
            },
        },
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    # FR Extract
    stg_gl_extract_fr = DataprocSubmitJobOperator(
        task_id="stg_gl_extract_fr",
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        job={
            "reference": {"project_id": GCP_PROJECT},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/run_gl_close_fr.py",
                "args": [period_date_val, "FR_ENTITY", force_close_val],
            },
        },
        retries=3,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    # ── Task: abinitio_gl_transform ──────────────────────────
    abinitio_gl_transform = DataprocSubmitJobOperator(
        task_id="abinitio_gl_transform",
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        job={
            "reference": {"project_id": GCP_PROJECT},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/gl_transform.py",
                "args": [
                    "--period-name", "{{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%b-%Y').upper() }}",
                    "--entity-code", "ALL",
                    "--parallelism", "4"
                ],
            },
        },
        on_failure_callback=on_failure_alarm
    )

    # ── Task: abinitio_reconcile (Handles warning non-blocking branch) ──
    abinitio_reconcile = DataprocSubmitJobOperator(
        task_id="abinitio_reconcile",
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        job={
            "reference": {"project_id": GCP_PROJECT},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/gl_reconcile.py",
                "args": [
                    "--period-name", "{{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%b-%Y').upper() }}",
                    "--entity-code", "ALL"
                ],
            },
        },
        on_failure_callback=on_failure_alarm_warning  
    )

    # ── Task: spark_gl_aggregation ───────────────────────────
    spark_gl_aggregation = DataprocSubmitJobOperator(
        task_id="spark_gl_aggregation",
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        job={
            "reference": {"project_id": GCP_PROJECT},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/finance_etl_assembly.py",
                "args": [
                    "--period-name", "{{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%b-%Y').upper() }}",
                    "--fiscal-year", "{{ data_interval_end.year }}"
                ],
            },
        },
        on_failure_callback=on_terminal_failure
    )

    # ── Task: daily_gl_close ─────────────────────────────────
    def write_close_audit_log(**context):
        period_name = (context['data_interval_end'].replace(day=1) - timedelta(days=1)).strftime('%b-%Y').upper()
        log_entry = f"[{datetime.now()}] FINANCE_DAILY_GL_CLOSE completed for PERIOD={period_name}\n"
        print(f"Audit log published: {log_entry}")

    daily_gl_close_audit = PythonOperator(
        task_id="daily_gl_close_audit",
        python_callable=write_close_audit_log,
        trigger_rule="all_done"  
    )

    trigger_crm_workflow = TriggerDagRunOperator(
        task_id="trigger_crm_weekly_workflow",
        trigger_dag_id="crm_weekly_workflow",
        wait_for_completion=False,
    )

    trigger_retail_workflow = TriggerDagRunOperator(
        task_id="trigger_retail_daily_workflow",
        trigger_dag_id="retail_daily_workflow",
        wait_for_completion=False,
    )

    # ── Task: period_close_notify ────────────────────────────
    period_close_notify = EmailOperator(
        task_id="period_close_notify",
        to=Variable.get("finance_notify_email", default_var="finance-etl@company.com"),
        subject="[FINANCE-OK] Month-End Close {{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%b-%Y').upper() }}",
        html_content="Month-end close complete: {{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%b-%Y').upper() }}"
    )

    # ── Dependency Declarations ──────────────────────────────
    is_last_business_day >> pre_flight
    
    pre_flight >> [
        account_master_load,
        stg_gl_extract_uk,
        stg_gl_extract_de,
        stg_gl_extract_fr
    ]

    [
        account_master_load,
        stg_gl_extract_uk,
        stg_gl_extract_de,
        stg_gl_extract_fr
    ] >> abinitio_gl_transform

    abinitio_gl_transform >> [abinitio_reconcile, spark_gl_aggregation]

    [abinitio_reconcile, spark_gl_aggregation] >> daily_gl_close_audit
    
    daily_gl_close_audit >> [trigger_crm_workflow, trigger_retail_workflow, period_close_notify]