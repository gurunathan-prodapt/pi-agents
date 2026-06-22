#
# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml
# Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG
#
# Airflow DAG for orchestrating the DW.BERT_AUSD_BP_TA_APN_VERTRAG job.
# This DAG calls a BigQuery Stored Procedure that encapsulates the legacy KornShell and Oracle PL/SQL logic.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

# Define project and dataset information
GCP_PROJECT_ID = "project"  # Replace with your GCP project ID
BIGQUERY_DATASET = "sof"    # Dataset where the stored procedures reside

with DAG(
    dag_id='dw_bert_ausd_bp_ta_apn_vertrag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(days=1),  # Example: daily schedule
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'project_id': GCP_PROJECT_ID,
    },
    tags=['bert', 'bigquery', 'data_ingestion'],
    description='Prepares instantiated base products (APN and contract reference data) for the BERT process.',
) as dag:
    # Generate a job entry number (e.g., a timestamp or sequence from a metadata table)
    # For simplicity, using a static value here, but ideally this would come from a dynamic source
    # or BigQuery sequence if available, or a timestamp in a real scenario.
    current_datetime_str = "{{ ds_nodash }}" # Example for date, could be more granular for unique IDs
    p_eintragsnr_value = 123456789 # Placeholder, consider a dynamic ID for production

    call_orchestration_sp = BigQueryExecuteQueryOperator(
        task_id='call_sp_r_k_ausd_bp_ta_apn_vertrag',
        sql=f"""
            CALL `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sp_r_k_ausd_bp_ta_apn_vertrag`(
                p_jobkennung => 'AUSD_BP_TA_APN_VERTRAG',
                p_eintragsnr => {p_eintragsnr_value}, -- Unique ID for job run
                p_stichtag => CURRENT_DATE(),        -- Stichtag (DDMMYYYY equivalent)
                p_wiederanlaufwert => 0               -- Default restart value
            );
        """,
        use_legacy_sql=False,
        location='US',  # Specify your BigQuery dataset location
    )

    # Define task dependencies
    call_orchestration_sp```