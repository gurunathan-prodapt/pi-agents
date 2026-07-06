"""
Airflow DAG Orchestrator for DW.BERT_P_VERTRAG_JP
Generated to replace UC4 Workflow and KornShell Wrappers.
Uses native BigQuery Operators to run target transformation scripts.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# Default configurations
DEFAULT_ARGS = {
    'owner': 'BERT_DWH_Team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['dwh-alerts@tinternal.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Resolve environments dynamically via variables
GCP_PROJECT = Variable.get("gcp_project", default_var="prod-bert-dwh")
ENV_PREFIX = Variable.get("env_prefix", default_var="prod")
STAGING_DATASET = f"{ENV_PREFIX}_staging"
SQL_BASE_PATH = "/home/gurunathan_t/migrated_composer/sql/bert_p_vertrag_jp"

with DAG(
    dag_id='DW.BERT_P_VERTRAG_JP',
    default_args=DEFAULT_ARGS,
    description='Orchestrates BERT Vertrag Master data preparation and mirroring in BigQuery',
    schedule_interval='0 2 * * *',  # Daily at 02:00 AM
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['BERT', 'VERTRAG', 'BIGQUERY'],
) as dag: 

    def build_bq_task(task_id, sql_file_name):
        return BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                "query": {
                    "query": f"{{% include '{SQL_BASE_PATH}/{sql_file_name}' %}}",
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='google_cloud_default'
        )

    # 1. Base Extract Layers
    period = build_bq_task('d_ausd_v_ta_period', 'd_ausd_v_ta_period.sql')
    discount_rr = build_bq_task('d_ausd_v_ta_discount_rr', 'd_ausd_v_ta_discount_rr.sql')
    cntrct_valid = build_bq_task('d_ausd_v_ta_cntrct_valid', 'd_ausd_v_ta_cntrct_valid.sql')
    barrier = build_bq_task('d_ausd_v_ta_barrier', 'd_ausd_v_ta_barrier.sql')
    vvl_dwh = build_bq_task('d_ausd_v_ta_vvl_dwh', 'd_ausd_v_ta_vvl_dwh.sql')
    inv_assign = build_bq_task('d_ausd_v_ta_inv_assign', 'd_ausd_v_ta_inv_assign.sql')
    inv_def = build_bq_task('d_ausd_v_ta_inv_def', 'd_ausd_v_ta_inv_def.sql')
    acc_ref = build_bq_task('d_ausd_v_ta_acc_ref', 'd_ausd_v_ta_acc_ref.sql')
    action_assoc = build_bq_task('d_ausd_v_ta_action_assoc', 'd_ausd_v_ta_action_assoc.sql')
    discount = build_bq_task('d_ausd_v_ta_discount', 'd_ausd_v_ta_discount.sql')
    apn_ve = build_bq_task('d_ausd_v_ta_apn_ve', 'd_ausd_v_ta_apn_ve.sql')
    bp_ref = build_bq_task('d_ausd_v_ta_bp_ref', 'd_ausd_v_ta_bp_ref.sql')

    # 2. Aggregations & Secondary Layers
    barrier_zusgf = build_bq_task('d_ausd_v_ta_barrier_zusgf', 'd_ausd_v_ta_barrier_zusgf.sql')
    vvl_upgrade = build_bq_task('d_ausd_v_ta_vvl_upgrade', 'd_ausd_v_ta_vvl_upgrade.sql')
    inv_acc = build_bq_task('d_ausd_v_ta_inv_acc', 'd_ausd_v_ta_inv_acc.sql')
    disc_zusgf = build_bq_task('d_ausd_v_ta_disc_zusgf', 'd_ausd_v_ta_disc_zusgf.sql')
    cntrct_crs = build_bq_task('d_ausd_v_ta_cntrct_crs', 'd_ausd_v_ta_cntrct_crs.sql')

    # 3. Aggregation Dependencies
    barrier >> barrier_zusgf
    vvl_dwh >> vvl_upgrade
    
    [inv_assign, inv_def, acc_ref] >> inv_acc
    discount >> disc_zusgf
    
    [cntrct_valid, period] >> cntrct_crs