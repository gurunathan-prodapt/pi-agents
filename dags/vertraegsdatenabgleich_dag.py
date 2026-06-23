# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
# This Airflow DAG orchestrates the BigQuery stored procedure for Vertragsdatenabgleich.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP project ID
DATASET_ID = "your_bigquery_dataset_id"  # Replace with your BigQuery dataset ID
LOCATION = "us-central1" # Replace with your BigQuery dataset location (e.g., us-central1, europe-west1)

with DAG(
    dag_id="vertraegsdatenabgleich_daily_job",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "data_reconciliation"],
    doc_md="""
    ### Vertragsdatenabgleich (Contract Data Reconciliation) DAG

    This DAG orchestrates the BigQuery stored procedure `Vertragsdatenabgleich`,
    which is responsible for reconciling contract data in the `ta_cntrct_crs2` table.
    It replaces the legacy `r_ausd_v_ta_cntrct_crs2.ksh` KornShell script.

    The DAG calls a main BigQuery stored procedure that handles:
    - Parameter passing (simulating -s and -l from the original script)
    - Logging to `job_control`, `job_log`, and `job_error_log` tables
    - Error handling
    - Invoking the core data reconciliation logic (`k_ausd_v_ta_cntrct_crs2`)

    **Original Source:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh`
    **Target:** BigQuery Stored Procedures and Cloud Composer (Airflow)
    """,
) as dag:
    call_vertraegsdatenabgleich_sp = BigQueryInsertJobOperator(
        task_id="call_vertraegsdatenabgleich_sp",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CALL `{PROJECT_ID}.{DATASET_ID}.Vertragsdatenabgleich`(
                        p_help => FALSE,
                        p_s => 'default_s_value', -- Replace with actual value from Airflow variables or context
                        p_l => 'default_l_value'  -- Replace with actual value from Airflow variables or context
                    );
                """,
                "useLegacySql": False,
                "priority": "INTERACTIVE",
            }
        },
        location=LOCATION,
    )