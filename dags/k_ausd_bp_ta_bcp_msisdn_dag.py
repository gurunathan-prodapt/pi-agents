# Airflow DAG for BigQuery migration of job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "my-gcp-project"
DATASET_ID = "my_dataset"
BIGQUERY_CONN_ID = "google_cloud_default" # Assuming a BigQuery connection is configured in Airflow

with DAG(
    dag_id="k_ausd_bp_ta_bcp_msisdn_bq",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # Define your schedule here, e.g., "@daily"
    catchup=False,
    tags=["bigquery", "migration"],
    description="Orchestrates BigQuery Stored Procedure for PoolBasisprodukt data processing, migrated from k_ausd_bp_ta_bcp_msisdn.ksh",
) as dag:
    # Example parameters - these would typically be dynamically generated or configured
    # For Stichtag, ensure it's in DDMMYYYY format.
    # To use current date for Stichtag in DDMMYYYY format:
    # STICHTAG = "{{ ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4] }}" # Airflow macro for YYYYMMDD, reformatted to DDMMYYYY
    JOB_KENNUNG = "BP_TA_BCP_MSISDN_MIGRATION"
    EINTRAGS_NR = "12345"
    STICHTAG = "01012023" # Example fixed date: 01.01.2023
    WIEDERANLAUF_WERT = "0"

    call_bigquery_procedure = BigQueryInsertJobOperator(
        task_id="call_r_ausd_bp_ta_bcp_msisdn_procedure",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
                        p_JobKennung => '{JOB_KENNUNG}',
                        p_EintragsNr => '{EINTRAGS_NR}',
                        p_Stichtag => '{STICHTAG}',
                        p_wiederanlaufWert => '{WIEDERANLAUF_WERT}'
                    );
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=BIGQUERY_CONN_ID,
    )