"""
DAG: ausd_bp_ta_bpr_beschr
Description: Bereitstellung Basisprodukte BERT. 
             Prepares instantiated base products descriptions in BigQuery.
Source Files: 
  - DW.BERT_AUSD_BP_TA_BPR_BESCHR.xml (UC4 Job)
  - r_ausd_bp_ta_bpr_beschr.ksh (Shell Wrapper)
  - k_ausd_bp_ta_bpr_beschr.ksh (Controller)
  - d_ausd_bp_ta_bpr_beschr.sql (Oracle SQL)
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default configuration settings
DEFAULT_ARGS = {
    "owner": "isbert_etl",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Fetch GCP configurations from Airflow Variables
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="gcp-bert-prod")
BQ_DATASET = Variable.get("bq_dataset_isbert", default_var="isbert_schema")
GCP_CONN_ID = "google_cloud_default"


def build_transform_sql(project_id: str, dataset: str) -> str:
    """
    Returns the complete transformation logic utilizing parameters.
    """
    return f"""
    -- 1. Declare and extract the audit/drop date metadata from dwtk_meldungen
    DECLARE v_datum STRING;

    SET v_datum = (
      SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
      FROM `{project_id}.{dataset}.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- 2. Clear target table (Equivalent to Oracle TRUNCATE TABLE)
    TRUNCATE TABLE `{project_id}.{dataset}.sof_ta_bpr_beschr`;

    -- 3. Extract, Join, and Load valid active base products
    INSERT INTO `{project_id}.{dataset}.sof_ta_bpr_beschr`
    (
      BPR_ID,
      PDS_DESCRIPTION
    )
    SELECT
      bp.bpr_id,
      dbp.pds_description
    FROM
      `{project_id}.{dataset}.pds_ta_bpr` bp
    INNER JOIN
      `{project_id}.{dataset}.pds_ta_care_description` dbp
    ON
      bp.pds_description_id = dbp.pds_description_id
    WHERE
      bp.modified_at IS NULL
      AND bp.is_production = 1;
    """


# DAG declaration
with DAG(
    dag_id="ausd_bp_ta_bpr_beschr",
    default_args=DEFAULT_ARGS,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval="0 4 * * *",  # Scheduled daily at 04:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["bert", "bigquery", "basisprodukte", "master_data"],
) as dag:

    start_process = EmptyOperator(
        task_id="start_process"
    )

    # Main data transformation block executing BigQuery processing
    execute_bpr_transform = BigQueryExecuteQueryOperator(
        task_id="execute_bpr_transform",
        sql=build_transform_sql(project_id=GCP_PROJECT_ID, dataset=BQ_DATASET),
        use_legacy_sql=False,
        gcp_conn_id=GCP_CONN_ID,
        write_disposition="WRITE_APPEND",  # Controlled manually inside DML script via Truncate
        create_disposition="CREATE_IF_NEEDED",
    )

    end_process = EmptyOperator(
        task_id="end_process"
    )

    # DAG Task Dependency Chain
    start_process >> execute_bpr_transform >> end_process