# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
# This Airflow DAG orchestrates the execution of the BigQuery Stored Procedure.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
import pendulum

with DAG(
    dag_id='k_ausd_bp_ta_rn_da_vda_tk_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # Arbitrary start date
    schedule=None, # This job is likely triggered externally or on specific dates
    catchup=False,
    tags=['isbert', 'etl', 'bigquery'],
    params={
        "p_JobKennung": Param(
            type="string",
            title="Job Identifier",
            description="Unique identifier for the job run.",
            default="k_ausd_bp_ta_rn_da_vda_tk"
        ),
        "p_EintragsNr": Param(
            type="string",
            title="Entry Number",
            description="Entry number for logging purposes.",
            default="1" # Default to '1' as per original script
        ),
        "p_Stichtag": Param(
            type="string",
            title="Key Date (DDMMYYYY)",
            description="The key date for processing in DDMMYYYY format. E.g., '28022023'",
            pattern="^(0[1-9]|[12][0-9]|3[01])(0[1-9]|1[0-2])(19|20)\d{2}$"
        ),
        "p_wiederanlaufWert": Param(
            type="string",
            title="Restart Value",
            description="Value for restart logic (0 for normal run).",
            default="0"
        ),
        "gcp_project_id": Param(
            type="string",
            title="GCP Project ID",
            description="The Google Cloud Project ID where BigQuery resources reside.",
            default="your_project_id" # Placeholder
        ),
        "bigquery_dataset_id": Param(
            type="string",
            title="BigQuery Dataset ID",
            description="The BigQuery Dataset ID where tables and procedures are located.",
            default="your_dataset_id" # Placeholder
        ),
    }
) as dag:
    call_bigquery_sp = BigQueryExecuteOperator(
        task_id='call_k_ausd_bp_ta_rn_da_vda_tk_sp',
        sql="""
        CALL `{{ params.gcp_project_id }}.{{ params.bigquery_dataset_id }}.k_ausd_bp_ta_rn_da_vda_tk`(
            p_JobKennung => '{{ params.p_JobKennung }}',
            p_EintragsNr => '{{ params.p_EintragsNr }}',
            p_Stichtag => '{{ params.p_Stichtag }}',
            p_wiederanlaufWert => '{{ params.p_wiederanlaufWert }}'
        );
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
    )