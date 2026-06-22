# Airflow DAG for DW.BERT_ABLAUFSTEUERUNG
# Replaces legacy UC4 Job Scheduler DW.BERT_ABLAUFSTEUERUNG

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id='bert_ablaufsteuerung_dag',
    description='Scheduler für alle produktiven Abläufe von Bert',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily', # Derived from UC4 Period: 1 (daily)
    catchup=False,
    tags=['bert', 'uc4_migration'],
) as dag:
    start_task = DummyOperator(
        task_id='start',
    )

    # Placeholder tasks for each UC4 Job Plan (JOBP) and Event (EVNT)
    # The actual logic for these tasks will be implemented in a later phase (P3).
    # Calendar and Earliest Start Time logic will be added in P2/P3.

    bert_monthly_jp_task = DummyOperator(
        task_id='bert_monthly_jp_task',
        # Replaces DW.BERT_MONATLICH_JP
        # Expected to run monthly on 25th or 5th, earliest start 20:00 (P2/P3)
    )

    bert_adm_check_evt_task = DummyOperator(
        task_id='bert_adm_check_evt_task',
        # Replaces DW.BERT_RUN_ADM_CHECK_JP_EVT
        # Expected earliest start 07:00 (P2/P3)
    )

    bert_adm_housekeeping_jp_task = DummyOperator(
        task_id='bert_adm_housekeeping_jp_task',
        # Replaces DW.BERT_ADM_HOUSEKEEPING_JP
        # Expected earliest start 04:03 (P2/P3)
    )

    dwh_apt_export_daily_jp_task = DummyOperator(
        task_id='dwh_apt_export_daily_jp_task',
        # Replaces DW.DWH_APT_EXPORT_TAEGLICH_JP
        # Expected earliest start 01:30 (P2/P3)
    )

    bert_master_data_jp_task = DummyOperator(
        task_id='bert_master_data_jp_task',
        # Replaces DW.BERT_STAMMDATEN_JP
        # Expected earliest start 01:00 (P2/P3)
    )

    dwh_run_apt_export_monthly_evt_task = DummyOperator(
        task_id='dwh_run_apt_export_monthly_evt_task',
        # Replaces DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT
        # Expected to run based on DW.KALENDER (excluding 'BERT_NICHT'), earliest start 01:00 (P2/P3)
    )

    end_task = DummyOperator(
        task_id='end',
    )

    # All child tasks are initially scheduled by the main scheduler and do not have
    # explicit sequential dependencies among themselves in the original UC4 JSCH definition.
    # Therefore, they can run in parallel from the start_task.
    start_task >> [
        bert_monthly_jp_task,
        bert_adm_check_evt_task,
        bert_adm_housekeeping_jp_task,
        dwh_apt_export_daily_jp_task,
        bert_master_data_jp_task,
        dwh_run_apt_export_monthly_evt_task,
    ] >> end_task