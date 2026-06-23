# Apache Airflow DAG for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='k_ausd_v_ta_discount_rr_orchestrator',
    start_date=days_ago(1),
    schedule_interval='@daily', # Or as per business requirement
    default_args=default_args,
    catchup=False,
    tags=['bigquery', 'discount', 'isbert'],
    description='Orchestrates the BigQuery Stored Procedure for TA Discount RR processing.',
) as dag:
    # You might want to dynamically generate p_JobKennung and p_EintragsNr
    # For simplicity, using static values or a templated approach here.
    # For example, using Airflow's {{ ds_nodash }} for p_EintragsNr if it's date-based.
    # The design document indicates p_EintragsNr is passed, suggesting it could be dynamic.
    # Let's use a combination of static for JobKennung and templated for EintragsNr.

    call_sp_ausd_v_ta_discount_rr = BigQueryExecuteStoredProcedureOperator(
        task_id='call_sp_ausd_v_ta_discount_rr',
        project_id='your_gcp_project', # Replace with your GCP Project ID
        dataset_id='isrpt_isbert_stage',
        procedure_id='sp_ausd_v_ta_discount_rr',
        gcp_conn_id='google_cloud_default', # Ensure this connection exists in Airflow
        parameters={
            'p_JobKennung': 'TA_DISCOUNT_RR', # Example JobKennung, align with business logic
            'p_EintragsNr': '{{ ds_nodash }}' # Example EintragsNr, e.g., current date 'YYYYMMDD'
        }
    )

    # Further tasks can be added here, e.g.,
    # - Data validation checks
    # - Notifications on success/failure
    # - Dependencies on upstream DAGs or data availability

    # Define task dependencies if any
    # call_sp_ausd_v_ta_discount_rr