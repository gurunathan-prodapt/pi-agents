# ===================================================================
# Legacy Source: DW.BERT_AUSD_BP_TA_BPR_BCP.xml, r_ausd_bp_ta_bpr_bcp.ksh
# Job          : ausd_bp_ta_bpr_bcp
# Description  : Airflow DAG orchestrating BCP base products provisioning
# ===================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default arguments config
default_args = {
    "owner": "dwh_migration_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_bcp",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Provisioning of instantiated BCP base products in BigQuery",
    schedule_interval="0 4 * * *",  # Runs daily at 04:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "bert", "bigquery"],
) as dag:

    # Execute conversion SQL script using BigQuery Operator
    process_bpr_bcp = BigQueryExecuteQueryOperator(
        task_id="execute_bpr_bcp_processing",
        sql="""
            DECLARE v_datum STRING;

            SET v_datum = (
              SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
              FROM `isbert_schema.dwtk_meldungen` m
              WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
            );

            TRUNCATE TABLE `isbert_schema.sof_ta_bpr_bcp`;

            INSERT INTO `isbert_schema.sof_ta_bpr_bcp` (
              CNTRCT_ID,
              BPR_ID,
              CNTRCT_ID_REF
            )
            SELECT DISTINCT
              bp.cntrct_id,
              bp.bpr_id,
              bp.cntrct_id_ref
            FROM `isbert_schema.sof_ta_bpr_instance` bp
            WHERE bp.bpr_id = '3142';
        """,
        use_legacy_sql=False,
        location="EU",  # Set to the appropriate BigQuery dataset location (US/EU)
    )

    process_bpr_bcp