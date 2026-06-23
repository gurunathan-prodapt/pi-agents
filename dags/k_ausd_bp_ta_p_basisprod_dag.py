# Apache Airflow DAG for k_ausd_bp_ta_p_basisprod.ksh
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='k_ausd_bp_ta_p_basisprod_workflow',
    default_args=default_args,
    description='Orchestrates BigQuery Stored Procedure for PoolBasisprodukt data preparation (migrated from k_ausd_bp_ta_p_basisprod.ksh)',
    schedule_interval=None, # Define your schedule, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'data_preparation'],
) as dag:
    # Set your BigQuery project and dataset
    BIGQUERY_PROJECT = 'project'
    BIGQUERY_DATASET = 'dataset'
    JOB_KENNUNG = 'POOL_BASISPROD_PREP' # Example job kennung
    EINTRAGS_NR = '1' # Example entry number

    # The Stichtag should ideally be dynamic, e.g., from Airflow macros or a sensor
    # For now, let's use execution_date as Stichtag in DDMMYYYY format
    # Note: airflow.macros.ds_nodash gives YYYYMMDD. We need DDMMYYYY.
    # We'll parse and reformat it.
    stichtag_formatted = "{{ ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4] }}"

    execute_bigquery_sp = BigQueryInsertJobOperator(
        task_id='call_poolbasisprodukt_sp',
        configuration={
            "query": {
                "query": f"""
                    CALL `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.r_ausd_bp_ta_p_basisprod`(
                        p_job_kennung => '{JOB_KENNUNG}',
                        p_eintrags_nr => '{EINTRAGS_NR}',
                        p_stichtag => '{stichtag_formatted}',
                        p_wiederanlauf_wert => 0,
                        p_job_id => '{{{{ dag.dag_id }}}}',
                        p_run_id => '{{{{ run_id }}}}'
                    );
                """,
                "useLegacySql": False,
                "queryParameters": [] # Parameters are embedded via f-string for now, could use queryParameters for better safety
            }
        },
        project_id=BIGQUERY_PROJECT,
        location='US', # Specify your BigQuery location
    )