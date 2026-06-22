# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

with DAG(
    dag_id="k_ausd_geschaeftspartner_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., "@daily"
    catchup=False,
    tags=["bigquery", "etl"],
    doc_md="""
    ### k_ausd_geschaeftspartner_dag
    This DAG orchestrates the BigQuery Stored Procedure `k_ausd_geschaeftspartner_main`
    which replaces the legacy ksh script `k_ausd_geschaeftspartner.ksh` and its
    dependent Oracle SQL*Plus script `d_ausd_geschaeftspartner.sql`.

    **Purpose:**
    Replicates the data preparation task for 'Geschäftspartner' (Business Partners)
    in BigQuery.
    """,
) as dag:
    execute_main_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id="execute_k_ausd_geschaeftspartner_main",
        project_id="your_project",  # Replace with your GCP project ID
        dataset_id="your_dataset_procs",  # Replace with the dataset where the stored procedure is deployed
        procedure_id="k_ausd_geschaeftspartner_main",
        parameters=[
            {"name": "p_JobKennung", "parameter_type": {"type": "STRING"}, "value": "ISBERT_GP_DAILY"}, # Example value
            {"name": "p_EintragsNr", "parameter_type": {"type": "STRING"}, "value": "GP_001"},       # Example value
            {"name": "p_Stichtag", "parameter_type": {"type": "STRING"}, "value": "{{ ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4] }}"},  # Converts YYYYMMDD to DDMMYYYY
            {"name": "p_wiederanlaufWert", "parameter_type": {"type": "INT64"}, "value": 0},
        ],
        gcp_conn_id="google_cloud_default", # Ensure this connection exists in Airflow
    )