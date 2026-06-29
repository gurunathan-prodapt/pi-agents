# Legacy Sources: DW.BERT_AUSD_BP_TA_BPR_BASIS_HIS.xml, DW.BERT_AUSD_BP_TA_BPR_BASIS.xml
# Job: ausd_bp_ta_bpr_basis
# Platform: Cloud Composer (Airflow)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 21),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_ausd_bp_ta_bpr_basis',
    default_args=default_args,
    description='Prepares instantiated base products for BERT scoring',
    schedule_interval='0 2 * * *',  # Runs daily at 2:00 AM
    catchup=False,
    template_searchpath=['/home/airflow/gcs/dags/sql'],
) as dag:

    # Task 1: Historical Data Processing 
    run_historical_sql = BigQueryInsertJobOperator(
        task_id='run_historical_sql',
        configuration={
            "query": {
                "query": "d_ausd_bp_ta_bpr_basis_his.sql",
                "useLegacySql": False,
            }
        }
    )

    # Task 2: Consolidated Run (Includes SIM and final Basis Product load)
    run_consolidation_sql = BigQueryInsertJobOperator(
        task_id='run_consolidation_sql',
        configuration={
            "query": {
                "query": "d_ausd_bp_ta_bpr_basis.sql",
                "useLegacySql": False,
            }
        }
    )

    run_historical_sql >> run_consolidation_sql