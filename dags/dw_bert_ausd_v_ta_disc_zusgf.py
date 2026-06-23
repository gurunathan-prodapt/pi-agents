# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml
# Job: BERT_V_TA_DISC_ZUSGF
# This Airflow DAG orchestrates the process of concatenating discount descriptions
# and populating the `sof$ta_disc_zusgf` table in BigQuery.
# It replaces a legacy UC4 job, KornShell scripts, and Oracle PL/SQL.

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import bigquery

def _determine_processing_date(**context):
    """
    Retrieves the s_datum (processing date) from BigQuery's
    isbert_schema.dwtk_meldungen table and pushes it to XCom.
    """
    client = bigquery.Client()
    query = """
    SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
    FROM `isbert_schema.dwtk_meldungen` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    query_job = client.query(query)
    results = query_job.result()
    s_datum = '19000101' # Default value if no result
    for row in results:
        s_datum = row.s_datum
        break
    context['ti'].xcom_push(key='s_datum', value=s_datum)
    print(f"Processing date (s_datum): {s_datum}")

with DAG(
    dag_id='dw_bert_ausd_v_ta_disc_zusgf',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['bert', 'bigquery', 'data_transformation'],
    description='Airflow DAG for BERT_V_TA_DISC_ZUSGF job to concatenate discount descriptions',
) as dag:
    start = DummyOperator(
        task_id='start',
    )

    determine_processing_date = PythonOperator(
        task_id='determine_processing_date',
        python_callable=_determine_processing_date,
        provide_context=True,
    )

    execute_bq_transformation = BigQueryExecuteQueryOperator(
        task_id='execute_bq_transformation',
        sql='sql/d_ausd_v_ta_disc_zusgf.sql',
        use_legacy_sql=False,
        bigquery_conn_id='google_cloud_default',
        gcp_conn_id='google_cloud_default',
        write_disposition='WRITE_TRUNCATE',
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> determine_processing_date >> execute_bq_transformation >> end