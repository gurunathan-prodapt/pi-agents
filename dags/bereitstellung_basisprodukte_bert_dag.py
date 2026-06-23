#
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
#
# Purpose: Cloud Composer DAG to schedule and execute the BigQuery Stored Procedure
# `bereitstellung_basisprodukte_bert`, which orchestrates the BERT base products provisioning.
#
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id="bereitstellung_basisprodukte_bert_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your schedule here, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bert", "bigquery", "etl"],
    description="Orchestrates the execution of BigQuery stored procedure for provisioning BERT base products.",
) as dag:
    # Define the Stichtag parameter.
    # If set to None, the BigQuery Stored Procedure will default it to the current system date (DDMMYYYY).
    # To pass a specific date (e.g., Airflow's execution date formatted as DDMMYYYY):
    # stichtag_param_value = "{{ ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4] }}"
    # (ds_nodash provides YYYYMMDD, so reformat to DDMMYYYY)
    stichtag_param_value = None  # Example: None to use SP default, or '31122023' for a fixed date

    # Define the Wiederanlaufwert parameter.
    # If set to None, the BigQuery Stored Procedure will default it to 0.
    wiederanlaufwert_param_value = 0  # Default to 0 as per the original script's logic

    call_bert_provisioning_sp = BigQueryInsertJobOperator(
        task_id="call_bereitstellung_basisprodukte_bert",
        configuration={
            "query": {
                "query": """
                    CALL `project.dataset.bereitstellung_basisprodukte_bert`(
                        p_stichtag => @stichtag,
                        p_wiederanlaufWert => @wiederanlaufWert
                    );
                """,
                "useLegacySql": False,  # Required for standard SQL and stored procedures
                "queryParameters": [
                    {
                        "name": "stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": stichtag_param_value},
                    },
                    {
                        "name": "wiederanlaufWert",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": wiederanlaufwert_param_value},
                    },
                ],
            }
        },
        gcp_conn_id="google_cloud_default",  # Ensure your GCP connection is configured in Airflow
    )