import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Dynamic and configurable variables representing the migration design definitions
DAG_ID = "dw_bert_ausd_bp_ta_msisdn_his"
GCP_PROJECT = "gcp-prod-dwh-project"
SOURCE_DATASET = "isbert_schema_prod"
TARGET_DATASET = "sof_dataset"

# SQL script parsed from the transformation translation specifications
SQL_SCRIPT = f"""
DECLARE v_carmen STRING DEFAULT '@pcrs1';
DECLARE v_datum STRING;

SET v_datum = (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `{GCP_PROJECT}.{SOURCE_DATASET}.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `{GCP_PROJECT}.{TARGET_DATASET}.sof$ta_msisdn_his`;

INSERT INTO `{GCP_PROJECT}.{TARGET_DATASET}.sof$ta_msisdn_his`
  (BPRI_COM_ID, MSISDN, CALLNUMBER_ROLE_ID, VALID_TO)
SELECT
  cn1.bpri_com_id,
  CONCAT(CAST(cn1.cc AS STRING), CAST(cn1.ndc AS STRING), CAST(cn1.sn AS STRING)) AS msisdn,
  cn1.callnumber_role_id,
  cn1.valid_to
FROM `{GCP_PROJECT}.{SOURCE_DATASET}.pds$ta_callnumber` cn1
WHERE cn1.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (cn1.modified_at IS NULL OR cn1.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND cn1.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND cn1.is_production = 1;
"""

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Migration pipeline orchestration replacing legacy UC4 job BERT_AUSD_BP_TA_MSISDN_HIS",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    tags=["migration", "bigquery", "msisdn", "historical"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    execute_msisdn_his_logic = BigQueryInsertJobOperator(
        task_id="execute_msisdn_his_logic",
        configuration={
            "query": {
                "query": SQL_SCRIPT,
                "useLegacySql": False,
            }
        },
        location=os.getenv("BQ_LOCATION", "EU"),
        gcp_conn_id=os.getenv("GCP_CONN_ID", "google_cloud_default"),
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> execute_msisdn_his_logic >> end_task