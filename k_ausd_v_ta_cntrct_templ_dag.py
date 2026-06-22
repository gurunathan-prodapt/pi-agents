# Apache Airflow DAG for k_ausd_v_ta_cntrct_templ.ksh migration
# Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

with DAG(
    dag_id="k_ausd_v_ta_cntrct_templ_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # This DAG is likely triggered externally by its parent, set to None for manual/external trigger
    catchup=False,
    tags=["isbert", "bigquery", "orchestration"],
    params={
        "job_kennung": "DEFAULT_JOB_KENNUNG",  # Default value, to be overridden by trigger
        "eintragsnr": 1,                      # Default value, to be overridden by trigger
    }
) as dag:
    # Task to execute the BigQuery Stored Procedure
    execute_bigquery_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id="call_r_ausd_v_ta_cntrct_templ_sp",
        project_id="project",  # Replace with your GCP Project ID
        dataset_id="dataset",  # Replace with your BigQuery Dataset ID
        procedure_id="r_ausd_v_ta_cntrct_templ",
        gcp_conn_id="google_cloud_default",  # Ensure this connection is configured in Airflow
        parameters={
            "p_JobKennung": "{{ params.job_kennung }}",
            "p_EintragsNr": "{{ params.eintragsnr }}",
        },
    )

    # No explicit dependencies needed within this simple DAG, as the procedure handles internal steps.
    # Further tasks (e.g., notifications, downstream processes) can be added here.