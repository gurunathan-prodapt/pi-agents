# Airflow DAG for k_ausd_bp_ta_bpr_basis_his.ksh
# This DAG orchestrates the execution of a BigQuery Stored Procedure
# to process data for the PoolBasisprodukt table.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

with DAG(
    dag_id="k_ausd_bp_ta_bpr_basis_his_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # This DAG is meant to be triggered manually or by an external system
    catchup=False,
    tags=["isbert", "basis_his", "bigquery"],
    params={
        "p_job_kennung": "BERT_BASIS_HIS", # Default job identifier
        "p_eintrags_nr": "1",              # Default entry number
        "p_stichtag": "{{ ds }}",          # Default to execution date in YYYY-MM-DD format
        "p_wiederanlauf_wert": "0",        # Default restart value
    },
) as dag:
    call_basis_his_procedure = BigQueryExecuteQueryOperator(
        task_id="call_basis_his_procedure",
        sql="""
        CALL project.dataset.r_ausd_bp_ta_bpr_basis_his(
          p_job_kennung => '{{ params.p_job_kennung }}',
          p_eintrags_nr => '{{ params.p_eintrags_nr }}',
          p_stichtag => PARSE_DATE('%Y-%m-%d', '{{ params.p_stichtag }}'),
          p_wiederanlauf_wert => '{{ params.p_wiederanlauf_wert }}'
        );
        """,
        use_legacy_sql=False,
        location="us-central1", # IMPORTANT: Replace with your actual BigQuery dataset location (e.g., 'europe-west1')
        project_id="your_gcp_project_id", # IMPORTANT: Replace with your actual GCP project ID
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
    )