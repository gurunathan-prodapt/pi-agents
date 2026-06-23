# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
# Description: Airflow DAG to orchestrate the BigQuery stored procedure call.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="r_ausd_bp_ta_rn_einzeln_bq_orchestration",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily"
    catchup=False,
    tags=["bigquery", "etl", "bert"],
    default_args=default_args,
    description="Orchestrates the BigQuery stored procedure for BERT contract data provisioning.",
) as dag:
    # Define the BigQuery stored procedure call
    call_main_sp = BigQueryExecuteQueryOperator(
        task_id="call_ausd_bp_ta_rn_einzeln_sp",
        gcp_conn_id="google_cloud_default",  # Your GCP connection ID
        sql="""
        CALL `project.dataset.ausd_bp_ta_rn_einzeln`(
            p_stichtag => '{{ ds_nodash }}', -- Example: Pass execution date as Stichtag (YYYYMMDD)
            p_wiederanlaufwert => 0           -- Example: Default to 0 for full run, or get from config/XCom
        );
        """,
        use_legacy_sql=False,
        params={
            "project_id": "project",  # Replace with your actual project ID
            "dataset_id": "dataset",  # Replace with your actual dataset ID
        },
    )

    # You can add more tasks here, like:
    # - Data validation after SP execution
    # - Notifications (e.g., Slack, email)
    # - Dependency checks on other tables/jobs

    # Define task dependencies
    # In this simple case, just one task
    # call_main_sp