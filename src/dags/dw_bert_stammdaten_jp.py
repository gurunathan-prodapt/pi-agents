# Legacy Source: r_ausd_bp_ta_apn_vertrag.ksh, k_ausd_bp_ta_apn_vertrag.ksh, DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml
# Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG
# Description: Cloud Composer (Apache Airflow) DAG orchestrating the BERT Stammdaten pipeline tasks.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'DW_BERT_STAMMDATEN_JP',
    default_args=default_args,
    description='BERT Stammdaten pipeline containing APN and Contract Reference aggregation',
    schedule_interval=None,
    catchup=False,
    template_searchpath=['/home/airflow/gcs/dags/sql'],
) as dag:

    # 1. Predecessor Task: Load raw APN data into sof$ta_bpr_apn
    load_bpr_apn = BigQueryInsertJobOperator(
        task_id='DW_BERT_AUSD_BP_TA_BPR_APN',
        configuration={
            "query": {
                "query": "SELECT 1; -- Placeholder representing predecessor loading logic",
                "useLegacySql": False,
            }
        },
    )

    # 2. Current Task: Aggregate strings and populate sof$ta_apn_vertrag
    process_apn_vertrag = BigQueryInsertJobOperator(
        task_id='DW_BERT_AUSD_BP_TA_APN_VERTRAG',
        configuration={
            "query": {
                "query": "{% include 'd_ausd_bp_ta_apn_vertrag.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # 3. Successor Task: Join sof$ta_apn_vertrag with other basic product tables
    process_basisprod = BigQueryInsertJobOperator(
        task_id='DW_BERT_AUSD_BP_TA_P_BASISPROD',
        configuration={
            "query": {
                "query": "SELECT 1; -- Placeholder representing successor downstream logic",
                "useLegacySql": False,
            }
        },
    )

    # Orchestration / Lineage Dependency Definitions
    load_bpr_apn >> process_apn_vertrag >> process_basisprod