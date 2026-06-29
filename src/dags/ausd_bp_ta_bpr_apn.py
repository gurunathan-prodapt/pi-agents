# Legacy source: DW.BERT_AUSD_BP_TA_BPR_APN.xml, r_ausd_bp_ta_bpr_apn.ksh, k_ausd_bp_ta_bpr_apn.ksh
# Job: ausd_bp_ta_bpr_apn
# Purpose: Airflow Orchestration DAG processing instantiated basic products and mapping APNs

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.empty import EmptyOperator

# Default arguments for the DAG pipeline execution
default_args = {
    "owner": "data_migration_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define Orchestration Pipeline
with DAG(
    dag_id="ausd_bp_ta_bpr_apn_dag",
    default_args=default_args,
    description="Processes instantiated basic products and maps Access Point Names",
    schedule_interval="0 4 * * *",  # Set to run daily at 04:00 AM
    catchup=False,
    tags=["bert", "dwh", "basisprodukt", "bigquery"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # Single-operator task executing the clean-up and reload transformation query
    transform_basisprodukte_apn = BigQueryExecuteQueryOperator(
        task_id="transform_basisprodukte_apn",
        sql="""
            -- Truncate target table to guarantee daily clean load
            TRUNCATE TABLE `{{ var.value.gcp_project }}.isbert_schema.sof_ta_bpr_apn`;

            -- Insert filtered, unique base product instances mapped to APN
            INSERT INTO `{{ var.value.gcp_project }}.isbert_schema.sof_ta_bpr_apn` (
              cntrct_id,
              bpr_id,
              cntrct_id_ref,
              access_point_name
            )
            SELECT DISTINCT
              bp.cntrct_id,
              bp.bpr_id,
              bp.cntrct_id_ref,
              ap.access_point_name
            FROM `{{ var.value.gcp_project }}.isbert_schema.sof_ta_bpr_instance` bp
            INNER JOIN `{{ var.value.gcp_project }}.isbert_schema.sof_ta_apn_carmen` ap
               ON bp.cntrct_id_ref = ap.cntrct_id
            WHERE bp.bpr_id IN (
              2828, -- vpn
              2829, -- iv_vpn
              2830, -- wap-intranet
              2831, -- telemetrie
              2925, -- mobile ip vpn (50% discount)
              2926, -- mobile ip vpn (100% discount)
              2998, -- blackberry solution
              2999, -- blackberry solution (10% discount)
              3000  -- blackberry solution (20% discount)
            );
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Define DAG tasks execution order
    start_pipeline >> transform_basisprodukte_apn >> end_pipeline