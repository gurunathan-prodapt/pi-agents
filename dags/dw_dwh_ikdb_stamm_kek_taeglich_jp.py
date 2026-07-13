from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def create_trigger_task(task_id: str, dag_ref: DAG) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=task_id,
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
        dag=dag_ref
    )

with DAG(
    dag_id='dw_dwh_ikdb_stamm_kek_taeglich_jp',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,  # Corresponds to Else="Wait" in Sync properties
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    description='Jobplan for coordination of KEK and master data related IN / OUT interfaces',
    tags=['dwh', 'ikdb', 'orchestrator']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Downstream trigger-based tasks
    dw_dwh_ikdb_info_import_taeglich_jp = create_trigger_task('dw_dwh_ikdb_info_import_taeglich_jp', dag)
    dw_dwh_ikdb_export_stamm_taeglich_jp = create_trigger_task('dw_dwh_ikdb_export_stamm_taeglich_jp', dag)
    dw_dwh_ikdb_stamm_nachlieferung_export_jp = create_trigger_task('dw_dwh_ikdb_stamm_nachlieferung_export_jp', dag)
    dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp = create_trigger_task('dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp', dag)
    dw_dwh_ikdb_pseudo_nachlieferung_export_jp = create_trigger_task('dw_dwh_ikdb_pseudo_nachlieferung_export_jp', dag)
    dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp = create_trigger_task('dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp', dag)
    dw_dwh_ikdb_kek_export_taeglich_jp = create_trigger_task('dw_dwh_ikdb_kek_export_taeglich_jp', dag)
    dw_dwh_ikdb_kek_nachlieferung_export_jp = create_trigger_task('dw_dwh_ikdb_kek_nachlieferung_export_jp', dag)
    dw_dwh_ikdb_kek_konsolidierung_taeglich_jp = create_trigger_task('dw_dwh_ikdb_kek_konsolidierung_taeglich_jp', dag)

    # Downstream transfer branches triggered from various consolidation steps
    dw_dwh_ikdb_kek_out_tmd_sftp_jp = create_trigger_task('dw_dwh_ikdb_kek_out_tmd_sftp_jp', dag)
    dw_dwh_ikdb_stamm_out_tmd_sftp_jp = create_trigger_task('dw_dwh_ikdb_stamm_out_tmd_sftp_jp', dag)
    dw_dwh_ikdb_pseudo_out_tmd_sftp_jp = create_trigger_task('dw_dwh_ikdb_pseudo_out_tmd_sftp_jp', dag)

    # Dependencies
    start >> dw_dwh_ikdb_info_import_taeglich_jp
    dw_dwh_ikdb_info_import_taeglich_jp >> dw_dwh_ikdb_export_stamm_taeglich_jp
    dw_dwh_ikdb_export_stamm_taeglich_jp >> dw_dwh_ikdb_stamm_nachlieferung_export_jp
    dw_dwh_ikdb_stamm_nachlieferung_export_jp >> dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp
    dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp >> dw_dwh_ikdb_pseudo_nachlieferung_export_jp
    dw_dwh_ikdb_pseudo_nachlieferung_export_jp >> dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp
    dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp >> dw_dwh_ikdb_kek_export_taeglich_jp
    dw_dwh_ikdb_kek_export_taeglich_jp >> dw_dwh_ikdb_kek_nachlieferung_export_jp
    dw_dwh_ikdb_kek_nachlieferung_export_jp >> dw_dwh_ikdb_kek_konsolidierung_taeglich_jp

    # Parallel downstream branches mapped to their respective predecessor steps
    dw_dwh_ikdb_kek_konsolidierung_taeglich_jp >> dw_dwh_ikdb_kek_out_tmd_sftp_jp
    dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp >> dw_dwh_ikdb_stamm_out_tmd_sftp_jp
    dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp >> dw_dwh_ikdb_pseudo_out_tmd_sftp_jp

    # Synchronized completion at End node
    [dw_dwh_ikdb_kek_out_tmd_sftp_jp, dw_dwh_ikdb_stamm_out_tmd_sftp_jp, dw_dwh_ikdb_pseudo_out_tmd_sftp_jp] >> end