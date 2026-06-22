# Cloud Composer DAG for k_ausd_v_ta_bp_ref.ksh migration
# Legacy Job: k_ausd_v_ta_bp_ref.ksh
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "project"  # Replace with your GCP project ID
DATASET_ID = "dataset"  # Replace with your BigQuery dataset ID
CONTROL_PROCEDURE_NAME = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control"

with DAG(
    dag_id="k_ausd_v_ta_bp_ref_migration",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., "@daily"
    catchup=False,
    tags=["bigquery", "migration"],
    doc_md="""
    ### k_ausd_v_ta_bp_ref_migration
    This DAG migrates the functionality of the legacy KornShell script `k_ausd_v_ta_bp_ref.ksh`
    to BigQuery. It orchestrates the execution of the BigQuery Stored Procedure
    `r_ausd_vertrag_control` which handles parameter validation, calls the core
    business logic, and logs job execution details.
    """,
) as dag:
    call_control_procedure = BigQueryInsertJobOperator(
        task_id="call_r_ausd_vertrag_control",
        configuration={
            "query": {
                "query": f"""
                CALL {CONTROL_PROCEDURE_NAME}(
                    p_job_kennung => 'YOUR_JOB_KENNUNG', -- Replace with actual job identifier (e.g., 'BP_REF_DAILY')
                    p_eintrags_nr => 'YOUR_EINTRAGS_NR', -- Replace with actual entry number (e.g., '12345')
                    p_stichtag => CURRENT_DATE()          -- Or specify a static date: 'YYYY-MM-DD'
                );
                """,
                "useLegacySql": False,
            }
        },
    )