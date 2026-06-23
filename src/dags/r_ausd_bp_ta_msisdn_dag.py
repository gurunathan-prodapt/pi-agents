# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
# Migrated Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='r_ausd_bp_ta_msisdn_dag',
    default_args=default_args,
    schedule_interval=None,  # This DAG is designed to be triggered externally or manually
    catchup=False,
    tags=['isbert', 'msisdn', 'bigquery'],
    params={
        'stichtag': '{{ ds }}',  # Stichtag (reference date), defaults to execution date
        'wiederanlaufwert': None, # Wiederanlaufwert (restart value)
    }
) as dag:
    start_task = DummyOperator(
        task_id='start_task',
    )

    def calculate_dates_function(**kwargs):
        """
        Replaces KornShell date calculations (e.g., gestern.ksh) and potential
        date determination from dwtk_meldungen.
        For this specific transformation, v_datum (from dwtk_meldungen) is not
        directly used in the main BigQuery INSERT statement.
        """
        stichtag = kwargs['params'].get('stichtag', kwargs['ds'])
        wiederanlaufwert = kwargs['params'].get('wiederanlaufwert')

        print(f"Stichtag (Reference Date): {stichtag}")
        print(f"Wiederanlaufwert (Restart Value): {wiederanlaufwert}")

        # Example: If 'yesterday's date' was needed (from gestern.ksh equivalent)
        # yesterday_date = datetime.strptime(stichtag, '%Y-%m-%d').date() - timedelta(days=1)
        # kwargs['ti'].xcom_push(key='yesterday', value=yesterday_date.strftime('%Y%m%d'))

        # If v_datum from dwtk_meldungen was required as a templated parameter for
        # the BigQuery SQL, it would be fetched here using BigQuery client.
        # Example (uncomment if needed):
        # from google.cloud import bigquery
        # client = bigquery.Client()
        # project_id = kwargs['dag'].connections['google_cloud_default'].extra_dejson.get('project')
        # if not project_id:
        #     raise ValueError("Google Cloud Project ID not found in 'google_cloud_default' connection.")
        #
        # query_v_datum = f"""
        #   SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        #   FROM `{project_id}.isbert_raw.dwtk_meldungen` AS m
        #   WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        # """
        # query_job = client.query(query_v_datum)
        # for row in query_job:
        #     v_datum = row[0]
        #     kwargs['ti'].xcom_push(key='v_datum', value=v_datum)
        #     print(f"Determined v_datum: {v_datum}")


    calculate_dates_task = PythonOperator(
        task_id='calculate_dates_task',
        python_callable=calculate_dates_function,
        provide_context=True,
    )

    truncate_target_table_task = BigQueryOperator(
        task_id='truncate_target_table_task',
        sql='TRUNCATE TABLE `{{ project_id }}.isbert_curated.sof_ta_msisdn`;',
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',  # Assumes 'google_cloud_default' connection is configured
    )

    run_transformation_task = BigQueryOperator(
        task_id='run_transformation_task',
        sql='{% include "bigquery/d_ausd_bp_ta_msisdn.bqsql" %}',
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
        params={
            # Any parameters needed by the SQL could be passed here, e.g.,
            # 'v_datum': "{{ ti.xcom_pull(key='v_datum', task_ids='calculate_dates_task') }}"
        }
    )

    def log_metrics_function(**kwargs):
        """
        Replaces temporary file usage and KSH logging functions (DWMSG_SetzeStatusOK).
        This task can be expanded to query BigQuery for row counts and log them.
        """
        target_table_path = f"`{kwargs['dag'].connections['google_cloud_default'].extra_dejson.get('project')}.isbert_curated.sof_ta_msisdn`"
        print(f"Transformation to {target_table_path} completed.")

        # Example: Query BigQuery for inserted row count
        # from google.cloud import bigquery
        # client = bigquery.Client()
        # count_query = f"SELECT COUNT(*) FROM {target_table_path}"
        # query_job = client.query(count_query)
        # for row in query_job:
        #     row_count = row[0]
        #     print(f"Number of rows in {target_table_path}: {row_count}")
        #     kwargs['ti'].xcom_push(key='rows_inserted', value=row_count)
        #
        # You can then use this 'rows_inserted' XCom value for further checks or logging.

    log_metrics_task = PythonOperator(
        task_id='log_metrics_task',
        python_callable=log_metrics_function,
        provide_context=True,
    )

    # Define task dependencies
    start_task >> calculate_dates_task >> truncate_target_table_task >> run_transformation_task >> log_metrics_task