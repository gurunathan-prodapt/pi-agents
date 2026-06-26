# This Airflow DAG replaces the legacy UC4 Job DW.BERT_ABLAUFSTEUERUNG.
# It orchestrates various sub-jobs and events based on time and calendar conditions.

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time import TimeSensor
from airflow.utils.dates import days_ago
import pendulum
from datetime import datetime

# --- Placeholder functions for custom calendar logic ---
# IMPORTANT: These functions are placeholders.
# They need to be updated with the precise calendar logic from the legacy UC4 system.
# Refer to section 7 ("Unresolved / Risks") and section 8 ("Build Plan - Analyze Calendar Definitions")
# of the migration design document.

def _check_if_monthly_run_day_callable(ds: str, days_of_month: list) -> bool:
    """
    Placeholder: Checks if the current day (from execution date 'ds') is one of the specified days of the month.
    This function should be replaced with logic derived from UC4 calendar definitions like DAY_OF_MONTH_25 and DAY_OF_MONTH_05.
    """
    dag_run_date = datetime.strptime(ds, '%Y-%m-%d')
    print(f"Checking if {dag_run_date.day} is in {days_of_month} for monthly run.")
    return dag_run_date.day in days_of_month

def _check_if_bert_nicht_day_callable(ds: str) -> bool:
    """
    Placeholder: Checks if the current day (from execution date 'ds') is a 'BERT_NICHT' day.
    This logic needs to be fully defined based on the UC4 'BERT_NICHT' calendar object.
    For demonstration, assuming BERT_NICHT days are the 10th and 20th of the month.
    """
    dag_run_date = datetime.strptime(ds, '%Y-%m-%d')
    bert_nicht_days = [10, 20] # This list needs to be derived from actual UC4 'BERT_NICHT' calendar.
    print(f"Checking if {dag_run_date.day} is a BERT_NICHT day ({bert_nicht_days}).")
    return dag_run_date.day in bert_nicht_days

# --- Main DAG Definition ---
with DAG(
    dag_id='dw_bert_ablaufsteuerung',
    start_date=days_ago(1),
    schedule_interval='@daily',  # The main DAG runs daily; internal tasks handle specific timings/conditions.
    catchup=False,
    tags=['bert', 'uc4', 'scheduler', 'orchestration'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': pendulum.duration(minutes=5),
    }
) as dag:
    start = DummyOperator(task_id='start_orchestration')

    # 1. DW.BERT_STAMMDATEN_JP - Daily, 01:00
    # This task is responsible for triggering the 'dw_bert_stammdaten_jp' sub-DAG at 01:00 daily.
    wait_for_01am_stammdaten = TimeSensor(
        task_id='wait_for_01am_bert_stammdaten_jp',
        target_time=pendulum.time(1, 0, 0),
    )
    trigger_bert_stammdaten_jp = TriggerDagRunOperator(
        task_id='trigger_bert_stammdaten_jp',
        trigger_dag_id='dw_bert_stammdaten_jp', # This DAG_ID refers to a separate Airflow DAG for DW.BERT_STAMMDATEN_JP.
        wait_for_completion=True,
        poke_interval=5,
    )
    start >> wait_for_01am_stammdaten >> trigger_bert_stammdaten_jp

    # 2. DW.DWH_APT_EXPORT_TAEGLICH_JP - Daily, 01:30
    # This task triggers the 'dw_dwh_apt_export_taeglich_jp' sub-DAG at 01:30 daily.
    wait_for_0130am_apt_export_taeglich = TimeSensor(
        task_id='wait_for_0130am_dwh_apt_export_taeglich_jp',
        target_time=pendulum.time(1, 30, 0),
    )
    trigger_dwh_apt_export_taeglich_jp = TriggerDagRunOperator(
        task_id='trigger_dwh_apt_export_taeglich_jp',
        trigger_dag_id='dw_dwh_apt_export_taeglich_jp', # This DAG_ID refers to a separate Airflow DAG.
        wait_for_completion=True,
        poke_interval=5,
    )
    start >> wait_for_0130am_apt_export_taeglich >> trigger_dwh_apt_export_taeglich_jp

    # 3. DW.BERT_ADM_HOUSEKEEPING_JP - Daily, 04:03
    # This task triggers the 'dw_bert_adm_housekeeping_jp' sub-DAG at 04:03 daily.
    wait_for_0403am_adm_housekeeping = TimeSensor(
        task_id='wait_for_0403am_bert_adm_housekeeping_jp',
        target_time=pendulum.time(4, 3, 0),
    )
    trigger_bert_adm_housekeeping_jp = TriggerDagRunOperator(
        task_id='trigger_bert_adm_housekeeping_jp',
        trigger_dag_id='dw_bert_adm_housekeeping_jp', # This DAG_ID refers to a separate Airflow DAG.
        wait_for_completion=True,
        poke_interval=5,
    )
    start >> wait_for_0403am_adm_housekeeping >> trigger_bert_adm_housekeeping_jp

    # 4. DW.BERT_RUN_ADM_CHECK_JP_EVT - Daily, 07:00
    # This event is executed daily at 07:00. The actual logic should replace the bash command.
    wait_for_07am_adm_check = TimeSensor(
        task_id='wait_for_07am_bert_run_adm_check_jp_evt',
        target_time=pendulum.time(7, 0, 0),
    )
    run_bert_adm_check_jp_evt = BashOperator(
        task_id='run_bert_adm_check_jp_evt',
        bash_command='echo "Executing DW.BERT_RUN_ADM_CHECK_JP_EVT logic. '
                     'This placeholder should be replaced with actual event handling, '
                     'e.g., calling a Python function or another sub-DAG."',
    )
    start >> wait_for_07am_adm_check >> run_bert_adm_check_jp_evt

    # 5. DW.BERT_MONATLICH_JP - Monthly, 20:00 (on 5th and 25th of month)
    # This task checks for specific days of the month and triggers the sub-DAG at 20:00.
    check_monthly_run_day_bert_monatlich = ShortCircuitOperator(
        task_id='check_monthly_run_day_bert_monatlich',
        python_callable=_check_if_monthly_run_day_callable,
        op_kwargs={'ds': "{{ ds }}", 'days_of_month': [5, 25]},
    )
    wait_for_08pm_bert_monatlich = TimeSensor(
        task_id='wait_for_08pm_bert_monatlich_jp',
        target_time=pendulum.time(20, 0, 0),
    )
    trigger_bert_monatlich_jp = TriggerDagRunOperator(
        task_id='trigger_bert_monatlich_jp',
        trigger_dag_id='dw_bert_monatlich_jp', # This DAG_ID refers to a separate Airflow DAG.
        wait_for_completion=True,
        poke_interval=5,
    )
    start >> check_monthly_run_day_bert_monatlich >> wait_for_08pm_bert_monatlich >> trigger_bert_monatlich_jp

    # 6. DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT - Monthly, 01:00 (excluding BERT_NICHT days)
    # This event runs monthly at 01:00, but excludes 'BERT_NICHT' days.
    def _check_monthly_apt_export_day_callable(ds: str) -> bool:
        """
        Combined logic for DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT:
        Checks if it's a designated monthly export day AND not a BERT_NICHT day.
        The monthly cadence needs to be accurately determined from UC4.
        """
        dag_run_date = datetime.strptime(ds, '%Y-%m-%d')
        # Placeholder for monthly days for APT export. This needs to be precisely defined from UC4.
        # Assuming for example, it should run on the 1st of the month for this demonstration.
        is_designated_monthly_day = dag_run_date.day == 1
        is_bert_nicht_day = _check_if_bert_nicht_day_callable(ds)

        if is_designated_monthly_day and not is_bert_nicht_day:
            print(f"Condition met for DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT for {ds}")
            return True
        else:
            print(f"Condition NOT met for DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT for {ds}. "
                  f"(is_designated_monthly_day: {is_designated_monthly_day}, is_bert_nicht_day: {is_bert_nicht_day})")
            return False

    check_monthly_apt_export_day = ShortCircuitOperator(
        task_id='check_monthly_apt_export_day',
        python_callable=_check_monthly_apt_export_day_callable,
        op_kwargs={'ds': "{{ ds }}"},
    )
    wait_for_01am_apt_export_monatlich = TimeSensor(
        task_id='wait_for_01am_dwh_run_apt_export_monatlich_jp_evt',
        target_time=pendulum.time(1, 0, 0),
    )
    run_dwh_apt_export_monatlich_jp_evt = BashOperator(
        task_id='run_dwh_apt_export_monatlich_jp_evt',
        bash_command='echo "Executing DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT logic. '
                     'This placeholder should be replaced with actual event handling, '
                     'e.g., calling a Python function or another sub-DAG."',
    )
    start >> check_monthly_apt_export_day >> wait_for_01am_apt_export_monatlich >> run_dwh_apt_export_monatlich_jp_evt