import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator, DataformRunOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime.datetime(2026, 4, 21),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    'dw_bert_ausd_bp_ta_rn_vertrag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    doc_md="""
    ### BERT Base Product Contract Orchestration
    Migrated from UC4 (DW.BERT_AUSD_BP_TA_RN_VERTRAG) and KornShell scripts.
    Triggers the BigQuery data model via Dataform to aggregate and pivot contract phone number fields.
    """
) as dag:

    # 1. Parse Parameters Task (Replacement for Shell Argument Parsing & Date Logic)
    def parse_runtime_parameters(**context):
        conf = context.get('dag_run').conf or {}
        stichtag = conf.get('stichtag', datetime.datetime.now().strftime('%Y%m%d'))
        wiederanlauf_wert = conf.get('wiederanlauf_wert', 0)
        
        print(f"Executing for Stichtag: {stichtag}")
        print(f"Restart boundary value: {wiederanlauf_wert}")
        
        # Push to XComs to make accessible to downstream Dataform compilations if required
        context['ti'].xcom_push(key='stichtag', value=stichtag)

    parse_parameters = PythonOperator(
        task_id='parse_parameters',
        python_callable=parse_runtime_parameters,
    )

    # 2. Trigger Dataform compilation and execution
    # Points to Dataform repository containing definitions/sof_ta_rn_vertrag.sqlx
    run_dataform_model = DataformRunOperator(
        task_id='run_sof_ta_rn_vertrag_model',
        project_id='gcp-bigquery-dwh-prod',
        region='europe-west3',
        repository_id='bert_dataform_repo',
        tags=['bert_stammdaten'],
    )

    parse_parameters >> run_dataform_model