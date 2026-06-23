# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='k_ausd_bp_ta_iccid_einzeln_migration',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['bigquery', 'etl'],
    description='Migrated KornShell script k_ausd_bp_ta_iccid_einzeln.ksh to BigQuery Stored Procedure.',
) as dag:
    call_main_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id='call_r_ausd_bp_ta_iccid_einzeln',
        project_id='your-gcp-project-id', # IMPORTANT: Replace with your GCP project ID
        dataset_id='dataset',             # IMPORTANT: Replace with your BigQuery dataset ID
        procedure_id='r_ausd_bp_ta_iccid_einzeln',
        parameters=[
            {"name": "p_JobKennung", "parameter_type": {"type": "STRING"}, "value": "DEFAULT_JOB"},
            {"name": "p_EintragsNr", "parameter_type": {"type": "STRING"}, "value": "001"},
            # Convert Airflow's ds (YYYY-MM-DD) to DDMMYYYY format expected by the Stored Procedure
            {"name": "p_Stichtag", "parameter_type": {"type": "STRING"}, "value": "{{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%d%m%Y') }}"},
            {"name": "p_wiederanlaufWert", "parameter_type": {"type": "INT64"}, "value": "0"}
        ]
    )

# Note on Data Ingestion (Build Plan Step 5):
# This is a prerequisite for the BigQuery stored procedures to function.
# Ensure that all source data, especially from 'project.dataset.sof_ta_bpr_basis',
# is ingested and available in BigQuery. This typically involves services like
# Google Cloud Data Transfer Service, Dataflow, or custom ingestion solutions.
# Specific ingestion code is not generated as part of this job migration.