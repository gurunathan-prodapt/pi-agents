# ===================================================================
# Legacy Source: r_ausd_bp_ta_bpr_apn.ksh, k_ausd_bp_ta_bpr_apn.ksh
# Job: ausd_bp_ta_bpr_apn
# Purpose: Orchestrates BERT basis product mapping pipeline in Cloud Composer
# ===================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator
)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

# Standard configurations aligned with Enterprise Modernization policies
default_args = {
    'owner': 'dw_bert',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['bert_alerts@gcp-enterprise-dwh.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_bert_ausd_bp_ta_bpr_apn',
    default_args=default_args,
    description='Modernized pipeline for ausd_bp_ta_bpr_apn BERT basic product mappings',
    schedule_interval='0 6 * * *',  # Daily execution at 06:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    template_searchpath=['/home/airflow/gcs/dags/sql', '/home/airflow/gcs/dags'],
    tags=['bert', 'ausd_bp_ta_bpr_apn'],
) as dag:

    # Step 1: Upstream dependency checks
    wait_for_bpr_instance = BigQueryTableExistenceSensor(
        task_id='wait_for_bpr_instance',
        project_id='gcp-enterprise-dwh',
        dataset_id='dw_bert',
        table_id='sof_ta_bpr_instance',
        poke_interval=120,
        timeout=3600,
    )

    wait_for_apn_carmen = BigQueryTableExistenceSensor(
        task_id='wait_for_apn_carmen',
        project_id='gcp-enterprise-dwh',
        dataset_id='dw_bert',
        table_id='sof_ta_apn_carmen',
        poke_interval=120,
        timeout=3600,
    )

    # Step 2: Main BigQuery DML execution
    run_dml_job = BigQueryInsertJobOperator(
        task_id='run_dml_job',
        configuration={
            "query": {
                "query": "{% include 'd_ausd_bp_ta_bpr_apn.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # Step 3: Data Quality Validation
    validate_output = BigQueryCheckOperator(
        task_id='validate_output',
        sql='SELECT COUNT(1) > 0 FROM `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`',
        use_legacy_sql=False,
    )

    # Graph Execution Layout
    [wait_for_bpr_instance, wait_for_apn_carmen] >> run_dml_job >> validate_output