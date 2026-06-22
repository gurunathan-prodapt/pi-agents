# Legacy source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml
# Job: DW.BERT_ABLAUFSTEUERUNG
#
# This Airflow DAG orchestrates the "Bert" data processing workflows,
# replacing the legacy UC4 Job Scheduler DW.BERT_ABLAUFSTEUERUNG.
# It includes calendar-based and time-based triggers for child DAGs.

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.date_time import TimeSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun
from datetime import datetime, timedelta

# --- Calendar Logic Placeholders ---
# These functions need to be manually implemented based on the actual UC4 calendar
# definitions (DW.NEW_CALENDAR and DW.KALENDER with BERT_NICHT constraint).
# The current implementation provides a basic example for demonstration.

def _check_new_calendar_branch(**kwargs):
    """
    Checks the DW.NEW_CALENDAR for DAY_OF_MONTH_25 and DAY_OF_MONTH_05.
    If applicable, returns the task_id to trigger the monthly BERT workflow.
    Otherwise, returns the task_id of the next main flow anchor, effectively skipping.
    """
    execution_date = kwargs["logical_date"]
    day_of_month = execution_date.day
    # Placeholder logic: True if day is 5th or 25th, False otherwise.
    # This must be replaced with the exact logic of UC4's DW.NEW_CALENDAR.
    is_applicable = day_of_month in [5, 25]
    
    print(f"Checking DW.NEW_CALENDAR for {execution_date.strftime('%Y-%m-%d')}. Day of month: {day_of_month}. Is applicable: {is_applicable}")
    
    if is_applicable:
        return 'wait_for_monthly_bert_start_time'
    else:
        print("DW.BERT_MONATLICH_JP will be skipped as per calendar conditions (DW.NEW_CALENDAR).")
        return 'adm_check_event_start_anchor' # Skip monthly BERT tasks and proceed to the next main block

def _check_bert_nicht_calendar_branch(**kwargs):
    """
    Checks the DW.KALENDER with BERT_NICHT constraint.
    If applicable, returns the task_id to trigger the monthly DWH export event.
    Otherwise, returns the task_id of the DAG end anchor, effectively skipping.
    """
    execution_date = kwargs["logical_date"]
    day_of_month = execution_date.day
    # Placeholder logic: True if day is NOT 15th, False if it is 15th.
    # This must be replaced with the exact logic of UC4's DW.KALENDER and BERT_NICHT constraint.
    is_applicable = day_of_month != 15 # Example: BERT_NICHT could mean "not on the 15th"
    
    print(f"Checking DW.KALENDER with BERT_NICHT for {execution_date.strftime('%Y-%m-%d')}. Day of month: {day_of_month}. Is applicable: {is_applicable}")
    
    if is_applicable:
        return 'trigger_dw_dwh_run_apt_export_monatlich_jp_evt'
    else:
        print("DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT will be skipped as per calendar conditions (DW.KALENDER BERT_NICHT).")
        return 'end_of_dag_anchor' # Skip monthly DWH export and proceed to the end of the DAG

# --- Guard Task for Concurrency ---
def _guard_active_run(**kwargs):
    """
    Checks for any currently running instances of this DAG.
    If another run is active, this task raises AirflowSkipException.
    Mimics UC4's SYNCREF with Else=Skip behavior.
    """
    dag_id = kwargs["dag"].dag_id
    current_run_id = kwargs["dag_run"].run_id
    
    active_runs = DagRun.find(dag_id=dag_id, state='running')
    
    # Filter out the current run itself
    other_active_runs = [run for run in active_runs if run.run_id != current_run_id]

    if other_active_runs:
        print(f"Found active runs for DAG '{dag_id}': {[run.run_id for run in other_active_runs]}.")
        print(f"Skipping current run '{current_run_id}' to prevent concurrency issues.")
        raise AirflowSkipException(f"Skipping DAG run {current_run_id} due to active concurrent runs.")
    print(f"No other active runs found for DAG '{dag_id}'. Proceeding with run '{current_run_id}'.")


with DAG(
    dag_id='dw_bert_ablaufsteuerung',
    # Placeholder for start_date. It is recommended to use a fixed date in production.
    start_date=days_ago(1), 
    schedule_interval='0 0 * * *', # Daily at midnight, reflecting UC4's StartTime=00:00 and Period=1
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={
        'owner': 'data-engineering',
        'retries': 0, # Unless specified in child job migration designs
        'retry_delay': timedelta(minutes=0), # Unless specified in child job migration designs
    },
    tags=['uc4', 'bert', 'scheduler', 'migration'],
    description='Airflow DAG for DW.BERT_ABLAUFSTEUERUNG (UC4 JSCH migration). Orchestrates various Bert-related processes.',
) as dag:
    # 1. Guard task for concurrency control (SYNCREF with Else=Skip)
    guard_active_run = PythonOperator(
        task_id='guard_active_run',
        python_callable=_guard_active_run,
        provide_context=True,
    )

    # --- Anchor tasks for linear flow convergence after conditional branches ---
    # These EmptyOperators ensure the DAG flow continues correctly after conditional skips.
    adm_check_event_start_anchor = EmptyOperator(task_id='adm_check_event_start_anchor')
    housekeeping_start_anchor = EmptyOperator(task_id='housekeeping_start_anchor')
    daily_apt_export_start_anchor = EmptyOperator(task_id='daily_apt_export_start_anchor')
    master_data_start_anchor = EmptyOperator(task_id='master_data_start_anchor')
    dwh_export_calendar_check_anchor = EmptyOperator(task_id='dwh_export_calendar_check_anchor')
    end_of_dag_anchor = EmptyOperator(task_id='end_of_dag_anchor')

    # 2. Calendar check for DW.BERT_MONATLICH_JP (DAY_OF_MONTH_25 and DAY_OF_MONTH_05)
    # Uses BranchPythonOperator to conditionally trigger monthly BERT tasks or skip them.
    check_monthly_bert_calendar = BranchPythonOperator(
        task_id='check_monthly_bert_calendar',
        python_callable=_check_new_calendar_branch,
        provide_context=True,
    )

    # 3. Conditional Monthly BERT Workflow: Wait for 20:00 and trigger DW.BERT_MONATLICH_JP
    # (ActFlg=1 -> wait_for_completion=True)
    wait_for_monthly_bert_start_time = TimeSensor(
        task_id='wait_for_monthly_bert_start_time',
        target_time="20:00:00",
    )

    trigger_dw_bert_monatlich_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_monatlich_jp',
        trigger_dag_id='dw_bert_monatlich_jp', # Name of the child DAG for DW.BERT_MONATLICH_JP
        wait_for_completion=True,
        # Placeholder for GCP environment settings if child DAGs require them
        # conf={
        #     'gcp_project_id': 'YOUR_GCP_PROJECT_ID',
        #     'dataproc_region': 'YOUR_DATAPROC_REGION',
        #     'dataproc_cluster_name': 'YOUR_DATAPROC_CLUSTER_NAME',
        #     'bucket_name': 'YOUR_BUCKET_NAME',
        # }
    )

    # 4. Admin Check Event: Wait for 07:00 and trigger DW.BERT_RUN_ADM_CHECK_JP_EVT
    # (ActFlg=0 -> wait_for_completion=False)
    wait_for_adm_check_event_start_time = TimeSensor(
        task_id='wait_for_adm_check_event_start_time',
        target_time="07:00:00",
    )

    trigger_dw_bert_run_adm_check_jp_evt = TriggerDagRunOperator(
        task_id='trigger_dw_bert_run_adm_check_jp_evt',
        trigger_dag_id='dw_bert_run_adm_check_jp_evt', # Name of the child DAG for DW.BERT_RUN_ADM_CHECK_JP_EVT
        wait_for_completion=False,
    )

    # 5. Housekeeping Job: Wait for 04:03 and trigger DW.BERT_ADM_HOUSEKEEPING_JP
    # (ActFlg=1 -> wait_for_completion=True)
    wait_for_housekeeping_start_time = TimeSensor(
        task_id='wait_for_housekeeping_start_time',
        target_time="04:03:00",
    )

    trigger_dw_bert_adm_housekeeping_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_adm_housekeeping_jp',
        trigger_dag_id='dw_bert_adm_housekeeping_jp', # Name of the child DAG for DW.BERT_ADM_HOUSEKEEPING_JP
        wait_for_completion=True,
    )

    # 6. Daily APT Export Job: Wait for 01:30 and trigger DW.DWH_APT_EXPORT_TAEGLICH_JP
    # (ActFlg=0 -> wait_for_completion=False)
    wait_for_daily_apt_export_start_time = TimeSensor(
        task_id='wait_for_daily_apt_export_start_time',
        target_time="01:30:00",
    )

    trigger_dw_dwh_apt_export_taeglich_jp = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_apt_export_taeglich_jp',
        trigger_dag_id='dw_dwh_apt_export_taeglich_jp', # Name of the child DAG for DW.DWH_APT_EXPORT_TAEGLICH_JP
        wait_for_completion=False,
    )

    # 7. Master Data Job: Wait for 01:00 and trigger DW.BERT_STAMMDATEN_JP
    # (ActFlg=1 -> wait_for_completion=True)
    wait_for_master_data_start_time = TimeSensor(
        task_id='wait_for_master_data_start_time',
        target_time="01:00:00",
    )

    trigger_dw_bert_stammdaten_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_stammdaten_jp',
        trigger_dag_id='dw_bert_stammdaten_jp', # Name of the child DAG for DW.BERT_STAMMDATEN_JP
        wait_for_completion=True,
    )

    # 8. Calendar check for DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT (BERT_NICHT constraint)
    check_dwh_export_monthly_calendar = BranchPythonOperator(
        task_id='check_dwh_export_monthly_calendar',
        python_callable=_check_bert_nicht_calendar_branch,
        provide_context=True,
    )

    # 9. Conditional Monthly DWH Export Event: Trigger DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT
    # (ActFlg=1 -> wait_for_completion=True)
    trigger_dw_dwh_run_apt_export_monatlich_jp_evt = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_run_apt_export_monatlich_jp_evt',
        trigger_dag_id='dw_dwh_run_apt_export_monatlich_jp_evt', # Name of the child DAG for DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT
        wait_for_completion=True,
    )

    # --- Task Dependencies ---
    # Initial guard task
    guard_active_run >> check_monthly_bert_calendar

    # Monthly BERT workflow branch
    check_monthly_bert_calendar >> wait_for_monthly_bert_start_time >> trigger_dw_bert_monatlich_jp >> adm_check_event_start_anchor
    
    # If monthly BERT is skipped by calendar, proceed directly to the next main anchor
    check_monthly_bert_calendar >> adm_check_event_start_anchor

    # Main linear flow (Admin Check, Housekeeping, Daily APT Export, Master Data)
    adm_check_event_start_anchor >> wait_for_adm_check_event_start_time >> trigger_dw_bert_run_adm_check_jp_evt >> housekeeping_start_anchor
    
    housekeeping_start_anchor >> wait_for_housekeeping_start_time >> trigger_dw_bert_adm_housekeeping_jp >> daily_apt_export_start_anchor

    daily_apt_export_start_anchor >> wait_for_daily_apt_export_start_time >> trigger_dw_dwh_apt_export_taeglich_jp >> master_data_start_anchor

    master_data_start_anchor >> wait_for_master_data_start_time >> trigger_dw_bert_stammdaten_jp >> dwh_export_calendar_check_anchor

    # Monthly DWH Export event branch
    dwh_export_calendar_check_anchor >> check_dwh_export_monthly_calendar

    check_dwh_export_monthly_calendar >> trigger_dw_dwh_run_apt_export_monatlich_jp_evt >> end_of_dag_anchor
    
    # If monthly DWH Export is skipped by calendar, proceed directly to the end of DAG anchor
    check_dwh_export_monthly_calendar >> end_of_dag_anchor