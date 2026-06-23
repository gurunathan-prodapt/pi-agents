# Airflow DAG generated from legacy source vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
# This DAG orchestrates the BigQuery processing for ta_rn_da_vda_tk.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),  # Set an appropriate start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def build_bigquery_sql():
    """
    Returns the BigQuery SQL script for processing ta_rn_da_vda_tk data.
    """
    return """
TRUNCATE TABLE `sof$ta_rn_da_vda_tk`;

INSERT INTO `sof$ta_rn_da_vda_tk`
(
  CNTRCT_ID,
  DA_RN_MSISDN,
  DA_RN_STATUS,
  DA_RN_VALID_TO,
  VDA_RN_MSISDN,
  VDA_RN_STATUS,
  VDA_RN_VALID_TO,
  TK_RN_MSISDN,
  TK_RN_STATUS,
  TK_RN_VALID_TO
)
SELECT
  cntrct_id,
  DA_RN_msisdn,
  DA_RN_status,
  DA_RN_valid_to,
  VDA_RN_msisdn,
  VDA_RN_status,
  VDA_RN_valid_to,
  TK_RN_msisdn,
  TK_RN_status,
  TK_RN_valid_to
FROM `sof$ta_rn_einzeln` rp
WHERE DA_RN_msisdn IS NOT NULL
   OR VDA_RN_msisdn IS NOT NULL
   OR TK_RN_msisdn IS NOT NULL;
"""

with DAG(
    dag_id='d_ausd_bp_ta_rn_da_vda_tk',
    default_args=default_args,
    schedule_interval=None,  # Define according to the original job's cadence, e.g., '0 0 * * *' for daily
    catchup=False,
    description='BigQuery DAG for ta_rn_da_vda_tk processing',
    tags=['bigquery', 'dw', 'isbert'],
) as dag:
    process_ta_rn_da_vda_tk = BigQueryExecuteQueryOperator(
        task_id='process_ta_rn_da_vda_tk',
        sql=build_bigquery_sql(),
        use_legacy_sql=False,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND', # As specified in design. Note that SQL handles TRUNCATE.
        location='EU',  # Specify the correct BigQuery dataset location (e.g., 'US', 'EU')
    )