# Apache Airflow DAG for k_ausd_bp_ta_cntrct_dist
# Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
# Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id="k_ausd_bp_ta_cntrct_dist_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set to a cron expression or timedelta for scheduling, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=["bigquery", "etl"],
    description="Orchestrates the k_ausd_bp_ta_cntrct_dist process in BigQuery.",
) as dag:
    # Example for how to call the BigQuery Stored Procedure
    # Parameters should ideally come from Airflow variables, XComs, or dynamic sources
    call_bigquery_sp = BigQueryInsertJobOperator(
        task_id="call_bigquery_stored_procedure",
        configuration={
            "query": {
                "query": """
                    CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
                        p_job_kennung => 'DAILY_DIST_JOB',
                        p_eintrags_nr => '001',
                        p_stichtag => FORMAT_DATE('%d%m%Y', CURRENT_DATE()), -- Example: current date as DDMMYYYY
                        p_wiederanlauf_wert => NULL -- Or a specific value if required
                    );
                """,
                "useLegacySql": False,
                "queryParameters": [
                    # Example of passing parameters dynamically from Airflow, if needed
                    # {
                    #     "name": "job_kennung_param",
                    #     "parameterType": {"type": "STRING"},
                    #     "parameterValue": {"value": "DAILY_DIST_JOB"},
                    # }
                ],
            }
        },
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
    )