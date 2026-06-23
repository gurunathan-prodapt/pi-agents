# Legacy source: k_ausd_bp_ta_iccid_einzeln.ksh
# BigQuery migration for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
# Apache Airflow DAG to orchestrate the BigQuery stored procedure.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define your GCP project and BigQuery dataset
GCP_PROJECT_ID = 'your_gcp_project_id'
BIGQUERY_DATASET = 'your_bigquery_dataset'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='k_ausd_bp_ta_iccid_einzeln_dag',
    default_args=default_args,
    description='Migrated DAG for k_ausd_bp_ta_iccid_einzeln.ksh',
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1), # Example daily schedule
    catchup=False,
    tags=['bigquery', 'data_pipeline'],
) as dag:
    # Example parameters - these would typically be dynamic or configured in Airflow variables
    # Stichtag can be passed as None to default to yesterday's date in the SP
    JOB_KENNUNG_PARAM = 'K_AUSD_BP_TA_ICCID_EINZELN'
    EINTRAGSNR_PARAM = '001'
    # STICHTAG_PARAM = '23072023' # Example: DDMMYYYY
    STICHTAG_PARAM = '' # Pass empty string to use yesterday's date as per legacy logic
    WIEDERANLAUFWERT_PARAM = ''

    call_bigquery_sp = BigQueryExecuteQueryOperator(
        task_id='call_r_ausd_bp_ta_iccid_einzeln_sp',
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
        sql=f"""
            CALL `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.r_ausd_bp_ta_iccid_einzeln`(
                '{JOB_KENNUNG_PARAM}',
                '{EINTRAGSNR_PARAM}',
                '{STICHTAG_PARAM}',
                '{WIEDERANLAUFWERT_PARAM}'
            );
        """,
        use_legacy_sql=False,
    )

    # Future tasks could include:
    # - Data quality checks after SP execution
    # - Notifications (e.g., Slack, Email)
    # - Downstream job triggers