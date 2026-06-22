from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# This import assumes google-cloud-bigquery is available in the Airflow environment
from google.cloud import bigquery


def _get_v_datum(**kwargs):
    """
    Queries `bert_staging.dwtk_meldungen` to determine the maximum timecreated
    for job_kennung 'BERT_DROP_TEMP_TABLE' and pushes it as v_datum to XCom.
    """
    client = bigquery.Client()
    query = """
    SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM
        `bert_staging.dwtk_meldungen` AS m
    WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    query_job = client.query(query)
    result = query_job.result()
    v_datum_val = '19000101' # Default fallback
    for row in result:
        v_datum_val = row[0]
        break
    kwargs['ti'].xcom_push(key='v_datum', value=v_datum_val)


with DAG(
    dag_id='dw_bert_ausd_bp_ta_tarifoption',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # Define your schedule, e.g., '0 0 * * *' for daily execution
    catchup=False,
    tags=['bert', 'dwh', 'migration'],
    doc_md="""
    ### DW.BERT_AUSD_BP_TA_TARIFOPTION Airflow DAG
    This DAG migrates the legacy Oracle ETL job DW.BERT_AUSD_BP_TA_TARIFOPTION
    to BigQuery. It processes tariff option data, categorizing and aggregating
    it based on business, GPRS, and other criteria.

    **Legacy Source:**
    - `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_TARIFOPTION.xml`
    - `vobs/dw_source/isrpt/isbert/install_save/r_ausd_bp_ta_tarifoption.ksh`
    - `vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh`
    - `vobs/dw_source/isrpt/isbert/install_save/d_ausd_bp_ta_tarifoption.sql`
    """,
) as dag:
    # Task to determine the dynamic date variable (v_datum)
    get_v_datum_task = PythonOperator(
        task_id='get_v_datum',
        python_callable=_get_v_datum,
        provide_context=True,
    )

    # Task to execute the main BigQuery SQL transformation script.
    # It passes the dynamically determined v_datum as a BigQuery query parameter.
    # The SQL script handles dropping tables and creating the two target tables.
    full_sql_transformation_task = BigQueryExecuteQueryOperator(
        task_id='execute_full_sql_transformation',
        sql=os.path.join(os.path.dirname(__file__), '../sql/bigquery/d_ausd_bp_ta_tarifoption.sql'),
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default', # Assumes a default GCP connection in Airflow
        params={
            "v_datum_param": "{{ ti.xcom_pull(task_ids='get_v_datum', key='v_datum') }}"
        },
    )

    # Define task dependencies
    get_v_datum_task >> full_sql_transformation_task