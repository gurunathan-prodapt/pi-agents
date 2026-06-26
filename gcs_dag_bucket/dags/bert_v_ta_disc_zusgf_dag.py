# ===================================================================
# File:  bert_v_ta_disc_zusgf_dag.py
# Job:   BERT_V_TA_DISC_ZUSGF
# Target: Cloud Composer (Apache Airflow)
# Replaces: DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml, r_ausd_v_ta_disc_zusgf.ksh
# ===================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Default configuration parameters
DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bert_v_ta_disc_zusgf',
    default_args=DEFAULT_ARGS,
    description='Reconciliation & concatenation of discount descriptions per contract',
    schedule_interval=None,  # Triggered by parent monthly/daily orchestration DAG
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['bert', 'contract', 'reporting'],
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    # Executes the high-performance declarative SQL rewrite
    load_sof_ta_disc_zusgf = BigQueryInsertJobOperator(
        task_id='load_sof_ta_disc_zusgf',
        configuration={
            "query": {
                # Load external SQL file stored in GCS / environment path
                "query": "{% include 'sql/sof_ta_disc_zusgf_load.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> load_sof_ta_disc_zusgf >> end