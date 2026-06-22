# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

# Define your GCP project and BigQuery dataset
GCP_PROJECT_ID = 'your_gcp_project'
BQ_DATASET = 'your_bq_dataset'

with DAG(
    dag_id='bert_austausch_ksh_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Set your desired schedule, e.g., '0 5 * * *' for 5 AM daily
    catchup=False,
    tags=['bigquery', 'etl'],
    params={
        'stichtag_in': {'type': 'string', 'default': None, 'description': 'Optional Stichtag in DDMMYYYY format (e.g., "01012023")'},
        'wiederanlaufwert_in': {'type': 'integer', 'default': 0, 'description': 'Optional Wiederanlaufwert (integer)'},
    },
) as dag:
    call_bert_austausch_ksh_sp = BigQueryInsertJobOperator(
        task_id='call_bert_austausch_ksh_sp',
        configuration={
            "query": {
                "query": f"CALL `{GCP_PROJECT_ID}.{BQ_DATASET}.BERT_AUSTAUSCH_KSH`("
                         f"p_stichtag_in => COALESCE('{{{{ dag_run.conf.stichtag_in }}}}', NULL),"
                         f"p_wiederanlaufwert_in => COALESCE('{{{{ dag_run.conf.wiederanlaufwert_in }}}}', 0)::BIGNUMERIC"
                         f");",
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default', # Ensure you have a valid GCP connection configured in Airflow
    )