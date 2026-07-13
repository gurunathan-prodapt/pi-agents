from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from scripts.r_exp_ikdb import execute_ikdb_export

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_ikdb_export_stamm_taeglich_jp',
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    description='Downstream orchestrator triggering the BigQuery Master Data (Stamm) export jobs.',
    tags=['dwh', 'ikdb', 'export']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    run_export_stamm = execute_ikdb_export(
        task_id='dw_exis_ikdb_stamm_r',
        job_name='EXIS_IKDB_STAMM_R',
        sql_file_path='sql/d_ikdb_exp_stamm.sql',
        export_target_gcs='gs://dwh-export-ikdb-work/STAMM_OUT_TMD_{{ ds_nodash }}.csv',
        gcp_conn_id='google_cloud_default'
    )

    start >> run_export_stamm >> end