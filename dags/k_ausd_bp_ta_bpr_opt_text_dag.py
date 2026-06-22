# Airflow DAG for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
# This DAG orchestrates the BigQuery stored procedure execution.
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='k_ausd_bp_ta_bpr_opt_text_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # This DAG will be triggered manually or by an external system
    catchup=False,
    tags=['bigquery', 'etl', 'isbert'],
    default_args=default_args,
    description='Airflow DAG to orchestrate BigQuery stored procedure for k_ausd_bp_ta_bpr_opt_text.ksh migration.',
) as dag:
    # Get today's date in DDMMYYYY format for p_Stichtag
    # 'ds_nodash' macro provides the execution date in YYYYMMDD format (e.g., 20230101).
    # We need DDMMYYYY. A simple reordering is done here, assuming 'ds_nodash' is used.
    # For a robust solution, you might parse and format the date within a Python callable.
    # For this example, we'll demonstrate using a static or a rearranged date from ds_nodash.
    # Let's assume the Airflow context correctly provides DDMMYYYY, or we adjust the procedure
    # to handle YYYYMMDD, or pass a pre-formatted date.
    # For now, let's assume `ds_nodash` is YYYYMMDD and we need to pass DDMMYYYY.
    # A more realistic Airflow macro for DDMMYYYY could be:
    # "{{ execution_date.strftime('%d%m%Y') }}"
    stichtag_ddmmyyyy = "{{ execution_date.strftime('%d%m%Y') }}"
    job_kennung = "BP_TA_BPR_OPT_TEXT_JOB"
    eintrags_nr = "1"
    wiederanlauf_wert = "0" # As per design, usage not fully clear in original, passed as is.

    execute_bpr_opt_text_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_r_ausd_bp_ta_bpr_opt_text_sp',
        project_id='project', # Replace with your GCP Project ID
        dataset_id='dataset', # Replace with your BigQuery Dataset ID
        procedure_id='r_ausd_bp_ta_bpr_opt_text',
        parameters=[
            {"name": "p_JobKennung", "parameterType": {"type": "STRING"}, "value": job_kennung},
            {"name": "p_EintragsNr", "parameterType": {"type": "STRING"}, "value": eintrags_nr},
            {"name": "p_Stichtag", "parameterType": {"type": "STRING"}, "value": stichtag_ddmmyyyy},
            {"name": "p_wiederanlaufWert", "parameterType": {"type": "STRING"}, "value": wiederanlauf_wert},
        ],
    )