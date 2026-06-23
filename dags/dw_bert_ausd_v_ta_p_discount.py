# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_DISCOUNT.xml
# Job: DW.BERT_AUSD_V_TA_P_DISCOUNT

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='dw_bert_ausd_v_ta_p_discount',
    start_date=datetime(2023, 1, 1), # Placeholder: Replace with actual start date
    schedule_interval=None, # Placeholder: Replace with actual schedule interval (e.g., '0 0 * * *' for daily)
    catchup=False,
    tags=['bert', 'discount', 'bigquery'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    call_sp_r_ausd_v_ta_p_discount = BigQueryExecuteStoredProcedureOperator(
        task_id='call_sp_r_ausd_v_ta_p_discount',
        project_id='your-gcp-project', # Replace with your GCP project ID
        dataset_id='your_dataset',     # Replace with your BigQuery dataset ID
        procedure_id='sp_r_ausd_v_ta_p_discount',
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
        # Pass the execution date as p_processing_date parameter
        # ds_nodash provides 'YYYYMMDD', which can be parsed by the SP as a DATE
        parameters=[
            {'name': 'p_processing_date', 'parameterType': {'type': 'DATE'}, 'defaultValue': '{{ ds }}'}
        ]
    )