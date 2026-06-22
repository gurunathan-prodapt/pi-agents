# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='bert_v_ta_c_bfc_orchestration',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Define your schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'isrpt', 'bert'],
    description='Orchestrates the BigQuery Stored Procedure for Bindefristcache (BERT_V_TA_C_BFC)'
) as dag:
    call_bert_v_ta_c_bfc = BigQueryExecuteQueryOperator(
        task_id='call_bert_v_ta_c_bfc_sp',
        sql="""
        CALL `isrpt.BERT_V_TA_C_BFC`(
            p_h => NULL,             -- Set to 'h' for help message, NULL otherwise for normal execution
            p_s => 'default_s_param', -- Replace with actual parameter value or Airflow dynamic values
            p_l => 'default_l_param'  -- Replace with actual parameter value or Airflow dynamic values
        );
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default' # Ensure you have a BigQuery connection configured in Airflow
    )