"""
DAG: dw_bert_ausd_bp_ta_cntrct_dist
Description: Replaces UC4, ksh wrappers, and Oracle d_ausd_bp_ta_cntrct_dist.sql.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# 1. Fetch Variables or Set Defaults
GCP_PROJECT = Variable.get("gcp_project_id", default_var="gcp-project-placeholder")
SOF_DATASET = Variable.get("bq_sof_dataset", default_var="sof")
ISBERT_DATASET = Variable.get("bq_isbert_dataset", default_var="isbert_schema")
BQ_LOCATION = Variable.get("bq_location", default_var="EU")

default_args = {
    "owner": "airflow",
    "depends_on_past": False, 
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Unified BigQuery SQL logic using Jinja templates to allow dynamic runtime parameter interpolation
# from dag_run.conf without parse-time evaluation issues.
UNIFIED_SQL = """
-- Step 00: Determine v_datum from the metadata audit table
DECLARE v_datum STRING;
DECLARE v_stichtag STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.get('gcp_project_id', 'gcp-project-placeholder') }}.{{ var.value.get('bq_isbert_dataset', 'isbert_schema') }}.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Retrieve stichtag from conf if provided, otherwise default to execution date
SET v_stichtag = COALESCE(
  NULLIF('{{ dag_run.conf.get("stichtag", "") }}', ''),
  FORMAT_DATE('%d%m%Y', CURRENT_DATE())
);

-- Log executing parameter
SELECT FORMAT("Executing for stichtag: %s and audit datum: %s", v_stichtag, v_datum);

-- Step 01: Truncate Target Table
TRUNCATE TABLE `{{ var.value.get('gcp_project_id', 'gcp-project-placeholder') }}.{{ var.value.get('bq_sof_dataset', 'sof') }}.ta_cntrct_dist`;

-- Step 02: Insert distinct contracts
INSERT INTO `{{ var.value.get('gcp_project_id', 'gcp-project-placeholder') }}.{{ var.value.get('bq_sof_dataset', 'sof') }}.ta_cntrct_dist` (CNTRCT_ID)
SELECT DISTINCT cntrct_id
FROM `{{ var.value.get('gcp_project_id', 'gcp-project-placeholder') }}.{{ var.value.get('bq_sof_dataset', 'sof') }}.ta_bpr_basis`
WHERE cntrct_id IS NOT NULL;
"""

with DAG(
    dag_id="dw_bert_ausd_bp_ta_cntrct_dist",
    default_args=default_args,
    description="Consolidated Airflow migration of ausd_bp_ta_cntrct_dist pipelines to BQ",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "dwh", "bert", "sof"],
) as dag: 

    # Compile the query in-memory and execute via the standard Operator
    run_dist_provisioning = BigQueryExecuteQueryOperator(
        task_id="run_dist_provisioning",
        sql=UNIFIED_SQL,
        use_legacy_sql=False,
        location=BQ_LOCATION,
    )

    run_dist_provisioning