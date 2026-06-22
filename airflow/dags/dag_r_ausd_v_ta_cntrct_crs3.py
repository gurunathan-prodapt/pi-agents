# Migrated from legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# This Airflow DAG orchestrates the BigQuery stored procedure for contract data synchronization.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

with DAG(
    dag_id='r_ausd_v_ta_cntrct_crs3',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set to a cron expression (e.g., '0 0 * * *') or timedelta if daily/hourly
    catchup=False,
    tags=['bigquery', 'etl', 'contract'],
    doc_md="""
    ### DAG for `r_ausd_v_ta_cntrct_crs3` BigQuery Stored Procedure
    This DAG orchestrates the execution of the `r_ausd_v_ta_cntrct_crs3` BigQuery stored procedure.
    The stored procedure performs a full refresh of the `sof_ta_cntrct_crs3` table
    based on data from `sof_ta_cntrct_crs2` and `dwtk_meldungen`,
    including logic to identify "Twinbill" contracts.
    """,
) as dag:
    call_bq_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='call_r_ausd_v_ta_cntrct_crs3_sp',
        project_id='my-project',  # Replace with your GCP project ID
        dataset_id='my_dataset',    # Replace with your BigQuery dataset ID
        procedure_id='r_ausd_v_ta_cntrct_crs3',
        gcp_conn_id='google_cloud_default',  # Ensure this connection is configured in Airflow
        parameters=[
            # p_JobKennung should ideally come from an Airflow variable or be dynamically generated
            # For this example, a static string is used.
            {"name": "p_JobKennung", "value": "BERT_AUSD_V_TA_CNTRCT_CRS3", "paramType": "STRING"},
            # p_EintragsNr could be a unique run ID, e.g., {{ run_id }} or a sequence.
            # For this example, a static integer is used.
            {"name": "p_EintragsNr", "value": 1, "paramType": "INT64"}
        ],
    )