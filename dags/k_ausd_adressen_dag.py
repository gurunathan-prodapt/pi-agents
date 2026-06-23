# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException

from k_ausd_adressen_utils import validate_date, calculate_yesterday_today, pruefe_parameter_gesetzt, log_error

def _parse_and_validate_params(**kwargs):
    """
    Parses and validates input parameters.
    Replicates getopts and pruefeParameterGesetzt logic.
    """
    params = kwargs['params']
    p_job_kennung = params.get('p_jobkennung')
    p_eintrags_nr = params.get('p_eintragsnr')
    p_stichtag = params.get('p_stichtag')
    p_wiederanlauf_wert = params.get('p_wiederanlaufwert', '0') # Default value as in original script

    try:
        pruefe_parameter_gesetzt('Jobkennung', p_job_kennung)
        pruefe_parameter_gesetzt('EintragsNr', p_eintrags_nr)
        pruefe_parameter_gesetzt('Stichtag', p_stichtag)

        if not validate_date(p_stichtag, '%Y%m%d'):
            log_error(193, f"Stichtag: {p_stichtag}", "Invalid date format for Stichtag. Expected YYYYMMDD.")

    except AirflowFailException as e:
        raise e
    except Exception as e:
        log_error(192, "Parameter validation", f"An unexpected error occurred during parameter validation: {e}")

    kwargs['ti'].xcom_push(key='p_jobkennung', value=p_job_kennung)
    kwargs['ti'].xcom_push(key='p_eintragsnr', value=p_eintrags_nr)
    kwargs['ti'].xcom_push(key='p_stichtag', value=p_stichtag)
    kwargs['ti'].xcom_push(key='p_wiederanlaufwert', value=p_wiederanlauf_wert)


def _calculate_dates(**kwargs):
    """
    Calculates yesterday's and today's dates based on p_stichtag.
    Replicates gestern.ksh functionality.
    """
    p_stichtag = kwargs['ti'].xcom_pull(key='p_stichtag', task_ids='parse_and_validate_parameters_task')
    
    today_date_str, yesterday_date_str = calculate_yesterday_today(p_stichtag, '%Y%m%d')

    kwargs['ti'].xcom_push(key='p_datum_heute', value=today_date_str)
    kwargs['ti'].xcom_push(key='p_datum_gestern', value=yesterday_date_str)


def _log_record_count(**kwargs):
    """
    Logs the number of records processed.
    In the original script, this was read from a temporary file.
    Here, it queries the target BigQuery table `sof_ta_e_reach_gp`.
    """
    target_dataset = kwargs['params']['target_dataset']
    # Assuming sof_ta_e_reach_gp is a primary output table to count
    table_name = f"{target_dataset}.sof_ta_e_reach_gp" 

    hook = BigQueryHook(gcp_conn_id=kwargs['params']['gcp_conn_id'])
    sql = f"SELECT COUNT(*) FROM `{table_name}`"
    
    try:
        df = hook.get_pandas_df(sql=sql)
        v_records = df.iloc[0, 0]
        print(f"Number of records processed and inserted into {table_name}: {v_records}")
        kwargs['ti'].xcom_push(key='v_records', value=v_records)
    except Exception as e:
        log_error(999, "Record Count", f"Failed to get record count from {table_name}: {e}")

with DAG(
    dag_id='k_ausd_adressen_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['isbert', 'addresses', 'bigquery'],
    params={
        'p_jobkennung': {'type': 'string', 'title': 'Job Identifier', 'description': 'Identifier for the job', 'default': 'DEFAULT_JOB'},
        'p_eintragsnr': {'type': 'string', 'title': 'Entry Number', 'description': 'Entry number for processing', 'default': '1'},
        'p_stichtag': {'type': 'string', 'title': 'Effective Date', 'description': 'Effective date in YYYYMMDD format', 'default': datetime.date.today().strftime('%Y%m%d')},
        'p_wiederanlaufwert': {'type': 'string', 'title': 'Restart Value', 'description': 'Value for restart logic', 'default': '0'},
        'gcp_conn_id': {'type': 'string', 'title': 'GCP Connection ID', 'description': 'Airflow GCP Connection ID', 'default': 'google_cloud_default'},
        'source_dataset': {'type': 'string', 'title': 'BigQuery Source Dataset', 'description': 'BigQuery Dataset for source tables (e.g., project.dataset_name)', 'default': 'your-gcp-project.your_source_dataset'},
        'target_dataset': {'type': 'string', 'title': 'BigQuery Target Dataset', 'description': 'BigQuery Dataset for target tables (e.g., project.dataset_name)', 'default': 'your-gcp-project.your_target_dataset'},
    },
) as dag:
    
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: print("Starting k_ausd_adressen_dag..."),
    )

    parse_and_validate_parameters_task = PythonOperator(
        task_id='parse_and_validate_parameters_task',
        python_callable=_parse_and_validate_params,
    )

    calculate_dates_task = PythonOperator(
        task_id='calculate_dates_task',
        python_callable=_calculate_dates,
    )

    execute_d_ausd_adressen_sql_task = BigQueryExecuteQueryOperator(
        task_id='execute_d_ausd_adressen_sql_task',
        sql='d_ausd_adressen.sql.bq',
        use_legacy_sql=False,
        gcp_conn_id='{{ params.gcp_conn_id }}',
        params={
            'source_dataset': '{{ params.source_dataset }}',
            'target_dataset': '{{ params.target_dataset }}',
        }
        # The SQL script uses Jinja templating for v_stichtag, so it implicitly gets dag_run.conf.p_stichtag or params.p_stichtag.
    )

    log_record_count_task = PythonOperator(
        task_id='log_record_count_task',
        python_callable=_log_record_count,
    )

    # Optional: If job table management is re-enabled, add a BigQueryOperator here.
    # For now, it's commented out as per the design document.
    # The original script had:
    # FOSJobDeaktivate $v_TabName
    # FOSJobErzeugeEintrag $v_TabName 'A' 'I' $p_Stichtag $p_Stichtag 'J' 'N' $v_records 'Initialbefuellung'

    start_task >> parse_and_validate_parameters_task >> calculate_dates_task >> execute_d_ausd_adressen_sql_task >> log_record_count_task