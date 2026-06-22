"""
Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh
Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "project"  # Replace with your GCP Project ID
DATASET_ID = "dataset"  # Replace with your BigQuery Dataset ID
BIGQUERY_CONNECTION_ID = "google_cloud_default" # Or your specific BigQuery connection ID

with DAG(
    dag_id="dag_vertragsdatenabgleich",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Define your schedule (e.g., "@daily", "0 5 * * *")
    catchup=False,
    tags=["bigquery", "etl"],
    description="Airflow DAG to trigger BigQuery Stored Procedure for contract data reconciliation.",
) as dag:
    
    # Task to call the BigQuery wrapper stored procedure
    call_reconciliation_sp = BigQueryInsertJobOperator(
        task_id="call_sp_vertragsdatenabgleich",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": (
                    f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`("
                    f"p_s => '{ "{{ ds }}" }',"  # Pass execution date (YYYY-MM-DD) as Stichtag
                    f"p_l => '{ "{{ ts_nodash }}" }'" # Pass timestamp without dashes as Laufnummer
                    f");"
                ),
                "useLegacySql": False,
                "queryParameters": [],
            }
        },
        gcp_conn_id=BIGQUERY_CONNECTION_ID,
    )

    # You can add more tasks here, e.g.,
    # - Data quality checks after the stored procedure runs
    # - Notifications (e.g., Slack, Email) for success or failure
    # - Triggering dependent DAGs or tasks.
```