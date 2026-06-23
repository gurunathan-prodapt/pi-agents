# This file replaces the legacy UC4 job DW.BERT_ABLAUFSTEUERUNG.xml
# Job: DW.BERT_ABLAUFSTEUERUNG
#
# This Airflow DAG orchestrates various productive data processing workflows
# related to "Bert", mirroring the logic of the original UC4 Job Scheduler.

# Standard library imports
from datetime import datetime, timedelta

# Apache Airflow imports
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.date_time import DateTimeSensor
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun
from airflow.utils.state import State
from sqlalchemy import and_

# --- Python Callable Definitions for Operators ---

def _sync_guard_check(**context):
    """
    Mimics the UC4 SYNCREF object's Else=Skip behavior.
    Checks for other running instances of this DAG and raises an AirflowSkipException
    if another active run is found, preventing concurrent execution.
    Note: The DAG-level `max_active_runs=1` also contributes to this behavior,
    but this PythonOperator provides explicit skipping logic.
    """
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    
    session = DagRun.get_session()
    
    # Query for other running instances of the same DAG, excluding the current one
    running_dag_runs = session.query(DagRun).filter(
        and_(
            DagRun.dag_id == dag_id,
            DagRun.state == State.RUNNING,
            DagRun.run_id != dag_run.run_id # Exclude the current dag_run
        )
    ).count()
    
    session.close()

    if running_dag_runs > 0:
        raise AirflowSkipException(
            f"Another instance of DAG {dag_id} is already running. Skipping this run."
        )
    print(f"No other active instances of DAG {dag_id} found. Proceeding with current run.")

def _check_dw_new_calendar(**context):
    """
    Implements the calendar logic for DW.NEW_CALENDAR.
    This calendar triggers on the 5th or 25th day of the month.
    """
    execution_date = context['execution_date']
    day_of_month = execution_date.day
    
    if day_of_month == 5 or day_of_month == 25:
        print(f"DW.NEW_CALENDAR condition met for {execution_date.strftime('%Y-%m-%d')}. Proceeding.")
        return True
    
    print(f"DW.NEW_CALENDAR condition not met for {execution_date.strftime('%Y-%m-%d')}. Skipping downstream tasks.")
    return False

def _check_dw_kalender(**context):
    """
    Placeholder for the DW.KALENDER logic (BERT_NICHT).
    The exact definition of BERT_NICHT is unknown and requires manual implementation.
    Currently, this function always returns True.
    TODO: Implement actual DW.KALENDER (BERT_NICHT) logic.
    """
    print("DW.KALENDER (BERT_NICHT) check. Placeholder - always returning True for now.")
    return True

# --- DAG Definition ---

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # As per design, no explicit retry logic found in UC4
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_bert_ablaufsteuerung',
    start_date=datetime(2023, 1, 1), # Arbitrary past start date
    schedule_interval='0 0 * * *', # Daily at midnight UTC, mimicking UC4 StartTime=00:00
    catchup=False,
    max_active_runs=1, # Ensures only one instance runs at a time, similar to UC4 SYNCREF
    default_args=default_args,
    tags=['bert', 'uc4_migration'],
    doc_md="""
    ### DW.BERT_ABLAUFSTEUERUNG Airflow DAG
    This DAG orchestrates "Bert" related productive data processing workflows,
    migrated from UC4/Automic Job Scheduler.
    It triggers various child DAGs based on time and calendar conditions.
    """
) as dag:
    
    # 1. Synchronization Guard: Prevents concurrent runs
    sync_guard = PythonOperator(
        task_id='sync_guard',
        python_callable=_sync_guard_check,
        provide_context=True,
    )

    # 2. DW.BERT_MONATLICH_JP - Monthly Job Plan
    time_sensor_monatlich = DateTimeSensor(
        task_id='wait_until_20_00_for_monatlich',
        target_date_time="{{ ds }}T20:00:00", # Earliest start 20:00
        poke_interval=timedelta(minutes=5), # Check every 5 minutes
        timeout=timedelta(hours=24), # Timeout after 24 hours
    )

    dw_new_calendar_check = ShortCircuitOperator(
        task_id='dw_new_calendar_check',
        python_callable=_check_dw_new_calendar,
        provide_context=True,
    )

    trigger_dw_bert_monatlich_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_monatlich_jp',
        trigger_dag_id='dw_bert_monatlich_jp',
        wait_for_completion=True, # ActFlg=1
        conf={"scheduled_by": dag.dag_id},
    )

    # 3. DW.BERT_RUN_ADM_CHECK_JP_EVT - Admin Check Event (Fire-and-Forget)
    time_sensor_adm_check = DateTimeSensor(
        task_id='wait_until_07_00_for_adm_check',
        target_date_time="{{ ds }}T07:00:00", # Earliest start 07:00
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(hours=24),
    )

    trigger_dw_bert_run_adm_check_jp_evt = TriggerDagRunOperator(
        task_id='trigger_dw_bert_run_adm_check_jp_evt',
        trigger_dag_id='dw_bert_run_adm_check_jp_evt',
        wait_for_completion=False, # ActFlg=0 (fire-and-forget)
        conf={"scheduled_by": dag.dag_id},
    )

    # 4. DW.BERT_ADM_HOUSEKEEPING_JP - Admin Housekeeping Job Plan
    time_sensor_housekeeping = DateTimeSensor(
        task_id='wait_until_04_03_for_housekeeping',
        target_date_time="{{ ds }}T04:03:00", # Earliest start 04:03
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(hours=24),
    )

    trigger_dw_bert_adm_housekeeping_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_adm_housekeeping_jp',
        trigger_dag_id='dw_bert_adm_housekeeping_jp',
        wait_for_completion=True, # ActFlg=1
        conf={"scheduled_by": dag.dag_id},
    )

    # 5. DW.DWH_APT_EXPORT_TAEGLICH_JP - Daily APT Export Job Plan (Fire-and-Forget)
    time_sensor_taeglich_export = DateTimeSensor(
        task_id='wait_until_01_30_for_taeglich_export',
        target_date_time="{{ ds }}T01:30:00", # Earliest start 01:30
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(hours=24),
    )

    trigger_dw_dwh_apt_export_taeglich_jp = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_apt_export_taeglich_jp',
        trigger_dag_id='dw_dwh_apt_export_taeglich_jp',
        wait_for_completion=False, # ActFlg=0 (fire-and-forget)
        conf={"scheduled_by": dag.dag_id},
    )

    # 6. DW.BERT_STAMMDATEN_JP - Master Data Job Plan
    time_sensor_stammdaten = DateTimeSensor(
        task_id='wait_until_01_00_for_stammdaten',
        target_date_time="{{ ds }}T01:00:00", # Earliest start 01:00
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(hours=24),
    )

    trigger_dw_bert_stammdaten_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_stammdaten_jp',
        trigger_dag_id='dw_bert_stammdaten_jp',
        wait_for_completion=True, # ActFlg=1
        conf={"scheduled_by": dag.dag_id},
    )

    # 7. DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT - Monthly Export Event
    time_sensor_monatlich_export = DateTimeSensor(
        task_id='wait_until_01_00_for_monatlich_export',
        target_date_time="{{ ds }}T01:00:00", # Earliest start 01:00
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(hours=24),
    )

    dw_kalender_check = ShortCircuitOperator(
        task_id='dw_kalender_check',
        python_callable=_check_dw_kalender,
        provide_context=True,
    )

    trigger_dw_dwh_run_apt_export_monatlich_jp_evt = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_run_apt_export_monatlich_jp_evt',
        trigger_dag_id='dw_dwh_run_apt_export_monatlich_jp_evt',
        wait_for_completion=True, # ActFlg=1
        conf={"scheduled_by": dag.dag_id},
    )

    # --- Task Dependencies ---
    sync_guard >> [
        time_sensor_monatlich,
        time_sensor_adm_check,
        time_sensor_housekeeping,
        time_sensor_taeglich_export,
        time_sensor_stammdaten,
        time_sensor_monatlich_export,
    ]

    time_sensor_monatlich >> dw_new_calendar_check >> trigger_dw_bert_monatlich_jp
    time_sensor_adm_check >> trigger_dw_bert_run_adm_check_jp_evt
    time_sensor_housekeeping >> trigger_dw_bert_adm_housekeeping_jp
    time_sensor_taeglich_export >> trigger_dw_dwh_apt_export_taeglich_jp
    time_sensor_stammdaten >> trigger_dw_bert_stammdaten_jp
    time_sensor_monatlich_export >> dw_kalender_check >> trigger_dw_dwh_run_apt_export_monatlich_jp_evt