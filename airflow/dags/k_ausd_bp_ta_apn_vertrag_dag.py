# Migrated from: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
import pendulum

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define your BigQuery project and dataset
BIGQUERY_PROJECT = 'your-gcp-project-id'  # Replace with your GCP Project ID
BIGQUERY_DATASET = 'dataset'              # Replace with your BigQuery Dataset Name (e.g., isbert_dataset)
ISBERT_SCHEMA_DATASET = 'isbert_schema'   # Replace with the dataset where isbert_schema.dwtk_meldungen resides if different

with DAG(
    dag_id='k_ausd_bp_ta_apn_vertrag_workflow',
    default_args=default_args,
    description='Orchestrates APN contract processing in BigQuery, migrated from k_ausd_bp_ta_apn_vertrag.ksh',
    schedule_interval='0 1 * * *', # Example: daily at 1 AM UTC
    tags=['bigquery', 'etl', 'migration'],
    catchup=False,
    params={
        'job_kennung': 'DEFAULT_JOB',
        'eintrags_nr': 'DEFAULT_ENTRY',
        'stichtag': '{{ ds_nodash }}', # Default to today's date in DDMMYYYY format
        'wiederanlauf_wert': 0
    }
) as dag:
    # Task to call the main orchestration BigQuery Stored Procedure
    call_main_sp = BigQueryInsertJobOperator(
        task_id='call_k_ausd_bp_ta_apn_vertrag_sp',
        project_id=BIGQUERY_PROJECT,
        configuration={
            "query": {
                "query": """
                    CALL `{0}.{1}.k_ausd_bp_ta_apn_vertrag_sp`(
                        p_JobKennung => @job_kennung,
                        p_EintragsNr => @eintrags_nr,
                        p_Stichtag => @stichtag,
                        p_wiederanlaufWert => @wiederanlauf_wert,
                        p_dataset_name => '{1}',
                        p_isbert_schema_dataset => '{2}'
                    );
                """.format(BIGQUERY_PROJECT, BIGQUERY_DATASET, ISBERT_SCHEMA_DATASET),
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "job_kennung",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ params.job_kennung }}"}
                    },
                    {
                        "name": "eintrags_nr",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ params.eintrags_nr }}"}
                    },
                    {
                        "name": "stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ params.stichtag }}"}
                    },
                    {
                        "name": "wiederanlauf_wert",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "{{ params.wiederanlauf_wert }}"}
                    }
                ]
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    # No further tasks needed as the BQ SP handles internal orchestration and logging.