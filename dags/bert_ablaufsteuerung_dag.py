# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml
# Job: DW.BERT_ABLAUFSTEUERUNG
# Airflow DAG for orchestrating various productive ETL processes related to "Bert".

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor # Potentially for EVNTs and SYNC
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime

# Import sub-DAGs/TaskGroups (these will be defined in separate files)
from sub_dags.bert_monatlich_jp import create_bert_monatlich_jp_taskgroup
from sub_dags.bert_adm_housekeeping_jp import create_bert_adm_housekeeping_jp_taskgroup
from sub_dags.dwh_apt_export_taeglich_jp import create_dwh_apt_export_taeglich_jp_taskgroup
from sub_dags.bert_stammdaten_jp import create_bert_stammdaten_jp_taskgroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # As per typical Airflow migration for new jobs
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bert_ablaufsteuerung_dag',
    default_args=default_args,
    description='Orchestrates various productive ETL processes related to "Bert".',
    schedule_interval='0 0 * * *', # Daily at 00:00 as per UC4 Period: 1, StartTime: 00:00
    start_date=datetime(2023, 1, 1), # A fixed historical start date
    catchup=False,
    tags=['bert', 'orchestration', 'daily'],
    max_active_runs=1, # Ensures only one run is active at a time, mimicking UC4 single instance behavior
) as dag:

    start = DummyOperator(task_id='start')

    # EVNT:DW.BERT_RUN_ADM_CHECK_JP_EVT - Placeholder for a sensor or external trigger
    # If this event waits for another DAG to complete, ExternalTaskSensor should be used.
    # Otherwise, custom Python logic to check a condition.
    bert_run_adm_check_jp_evt = PythonOperator(
        task_id='bert_run_adm_check_jp_evt',
        python_callable=lambda: print("Checking for BERT_RUN_ADM_CHECK_JP_EVT event..."),
    )

    # JOBP:DW.BERT_MONATLICH_JP as a TaskGroup
    bert_monatlich_jp_group = create_bert_monatlich_jp_taskgroup(dag)

    # JOBP:DW.BERT_ADM_HOUSEKEEPING_JP as a TaskGroup
    bert_adm_housekeeping_jp_group = create_bert_adm_housekeeping_jp_taskgroup(dag)

    # JOBP:DW.DWH_APT_EXPORT_TAEGLICH_JP as a TaskGroup
    dwh_apt_export_taeglich_jp_group = create_dwh_apt_export_taeglich_jp_taskgroup(dag)

    # JOBP:DW.BERT_STAMMDATEN_JP as a TaskGroup
    bert_stammdaten_jp_group = create_bert_stammdaten_jp_taskgroup(dag)

    # EVNT:DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT - Placeholder for a sensor or external trigger
    # Similar to bert_run_adm_check_jp_evt, this might be an ExternalTaskSensor.
    dwh_run_apt_export_monatlich_jp_evt = PythonOperator(
        task_id='dwh_run_apt_export_monatlich_jp_evt',
        python_callable=lambda: print("Checking for DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT event..."),
    )

    end = DummyOperator(task_id='end')

    # Define task dependencies to sequence the workflow
    start >> bert_run_adm_check_jp_evt
    bert_run_adm_check_jp_evt >> bert_monatlich_jp_group
    bert_monatlich_jp_group >> bert_adm_housekeeping_jp_group
    bert_adm_housekeeping_jp_group >> dwh_apt_export_taeglich_jp_group
    dwh_apt_export_taeglich_jp_group >> bert_stammdaten_jp_group
    bert_stammdaten_jp_group >> dwh_run_apt_export_monatlich_jp_evt
    dwh_run_apt_export_monatlich_jp_evt >> end

    # UC4 synchronization DW.BERT_ABLAUFSTEUERUNG_SYNC (Abend="SETZE_FREI", End="SETZE_FREI", Start="SETZE_LAEUFT")
    # This behavior is primarily handled by Airflow's native DAG run management:
    # `max_active_runs=1` ensures only one instance runs at a time.
    # Task dependencies manage the "Start" and "End" conditions.
    # "SETZE_FREI" (release) upon Abend/End implies that other dependent processes
    # could be triggered or allowed to proceed. If external systems or other Airflow DAGs
    # depend on the completion of this DAG, `ExternalTaskSensor` in those DAGs or
    # a `TriggerDagRunOperator` from this DAG would explicitly manage that.